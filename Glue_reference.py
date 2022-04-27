import datetime
import logging

import boto3
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

SOURCE_S3_BUCKET = 'csai-dse-product-usage-user-login-ingestion'
TARGET_DAU_S3_BUCKET = 'dse-product-usage-dau-prod'
TARGET_DAU_AGG_S3_BUCKET = 'dse-product-usage-dau-agg-prod'
SOURCE_S3_PATH = "s3a://"+SOURCE_S3_BUCKET
TARGET_DAU_S3_PATH = "s3a://"+TARGET_DAU_S3_BUCKET+"/"
TARGET_DAU_AGG_S3_PATH = "s3a://"+TARGET_DAU_AGG_S3_BUCKET+"/"
# END_DATE = datetime.datetime.now()
CURRENT_DATE = datetime.datetime.now()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_credentials():
    """Uses boto3 to create a client session, sends a connection request to AWS using an assumed role dse-spark-s3-role-prod. 

    Raises:
        ce: Exception handled when the boto3 client connection fails to be returned.

    Returns:
        boto3_sts_response['Credentials']: An STS boto3 connection with the Credentials for connecting to AWS.
    """    
    try:
        session = boto3.session.Session()
        logger.info(f'Boto3 session: {session}')

        sts_connection = session.client('sts')

        logger.info('Contacting AWS through the boto3 SDK.')
        boto3_sts_response = sts_connection.assume_role(RoleArn='arn:aws:iam::<accountid>:role/dse-spark-s3-role-prod', RoleSessionName='Spark_Sessionee',DurationSeconds=3600)
        logger.info('Response recevied from AWS')
    except ClientError as ce:
        raise ce


    logger.info('Returning credentials')
    return boto3_sts_response['Credentials']

# def get_s3_dir_list(bucket_name):
#     try:
#         s3_session = boto3.session.Session()
#         logger.info(f'Boto3 session for S3 directory check: {s3_session}')

#         s3_connection = s3_session.client('s3')
#         logger.info('Session created.')

#         s3_objects = s3_connection.list_objects_v2(Bucket=bucket_name)
#         date_set = set()
#         if 'Contents' in s3_objects.keys():
#             for object in s3_objects['Contents']:
#                 date_val = object['Key']
#                 date_set.add(date_val.split('/')[0])
#             date_list = list(date_set)
#         else:
#             logger.info('Empty S3 bucket or no directories to list')
#             return []
#     except ClientError as ce:
#         raise ce
#     return date_list
    
def configure_spark_session(aws_access_key, aws_secret_access_key, aws_session_token):
    spark_session = SparkSession.builder \
    .appName("DAU_Ingestion") \
    .config('spark.jars.packages','org.apache.hadoop:hadoop-aws:3.2.0') \
    .getOrCreate()

    logger.info(f'Spark Version: {spark_session.version}')
    sc = SparkContext.getOrCreate()
    logger.info(f'Hadoop version: {sc._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()}')

    logger.info('Setting Spark session configuration for S3')
    # spark s3 settings
    spark_session._jsc.hadoopConfiguration().set('fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
    spark_session._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
    spark_session._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_access_key)
    spark_session._jsc.hadoopConfiguration().set("fs.s3a.session.token", aws_session_token)

    return spark_session

# def generate_date_range(end_date, days):
#     date_format = '%Y%m%d'
#     date_range = []
#     for day in range(days+1, -1, -1):
#         date_range.append((end_date - datetime.timedelta(day)).strftime(date_format))
#     return date_range
    
def process_files(spark_session_obj):
    logger.info('Reading Parquet files from the source S3 bucket.')
    date_value = CURRENT_DATE.strftime('%Y%m%d')
    # logger.info(f'Reading Parquet files from S3 for the following dates: {date_values}')
    # dau_dir_list = get_s3_dir_list(TARGET_DAU_S3_BUCKET+'/dau_data/')
    # dau_agg_dir_list = get_s3_dir_list(TARGET_DAU_AGG_S3_BUCKET+'/dau_agg_data/')
    # source_partition_list = get_s3_dir_list(SOURCE_S3_BUCKET)
    dau_parquet_df = spark_session_obj.read.parquet(SOURCE_S3_PATH+'/'+date_value)
    if not dau_parquet_df.rdd.isEmpty():
        dau_parquet_df = spark_session_obj.read.json(dau_parquet_df.rdd.map(lambda r:r._COL_0))
        logger.info('Addin a literal column for DAU count')
        dau_parquet_df = dau_parquet_df.withColumn('DAU_COUNT', lit(1))
        dau_agg_parquet_df = dau_parquet_df.groupBy(['ACCOUNTID', 'ACCOUNTKEY', 'ANID', 'ANID_CALC_CRITERIA',\
                                                    'CLIENT_PRODUCT_NAME', 'DAU_DATE', 'DEPLOYMENT_ID',\
                                                    'DEPLOYMENT_KEY', 'END_TIME', 'SERVER_OS_NAME',\
                                                    'SERVER_OS_VERSION', 'SERVER_PRODUCT_NAME',\
                                                    'SERVER_PRODUCT_VERSION', 'SITE_LUID', 'START_TIME',\
                                                    'TSM_CLUSTER_ID']).sum('DAU_COUNT')
        dau_agg_parquet_df = dau_agg_parquet_df.withColumnRenamed('sum(DAU_COUNT)','DAU')                                                    
        logger.info('Writing Parquet files for granular DAU records.')
        dau_parquet_df.coalesce(1).write.option('compression', 'snappy').parquet(TARGET_DAU_S3_PATH+'dau_data/'+date_value)
        logger.info('Writing Parquet files for aggregated DAU records.')
        dau_agg_parquet_df.coalesce(1).write.option('compression', 'snappy').parquet(TARGET_DAU_AGG_S3_PATH+'dau_agg_data/'+date_value)

    # for date_value in date_values:
    #     if date_value in source_partition_list:
    #         dau_parquet_df = spark_session_obj.read.parquet(SOURCE_S3_PATH+'/'+date_value)
    #         if not dau_parquet_df.rdd.isEmpty():
    #             dau_parquet_df = spark_session_obj.read.json(dau_parquet_df.rdd.map(lambda r:r._COL_0))
    #             logger.info('Addin a literal column for DAU count')
    #             dau_parquet_df = dau_parquet_df.withColumn('DAU_COUNT', lit(1))
    #             dau_agg_parquet_df = dau_parquet_df.groupBy(['ACCOUNTID', 'ACCOUNTKEY', 'ANID', 'ANID_CALC_CRITERIA',\
    #                                                      'CLIENT_PRODUCT_NAME', 'DAU_DATE', 'DEPLOYMENT_ID',\
    #                                                      'DEPLOYMENT_KEY', 'END_TIME', 'SERVER_OS_NAME',\
    #                                                      'SERVER_OS_VERSION', 'SERVER_PRODUCT_NAME',\
    #                                                      'SERVER_PRODUCT_VERSION', 'SITE_LUID', 'START_TIME',\
    #                                                      'TSM_CLUSTER_ID']).sum('DAU_COUNT')
    #             dau_agg_parquet_df = dau_agg_parquet_df.withColumnRenamed('sum(DAU_COUNT)','DAU')                                                    
    #             logger.info('Writing Parquet files for granular DAU records.')
    #             if date_value not in dau_dir_list:
    #                 dau_parquet_df.coalesce(1).write.option('compression', 'snappy').parquet(TARGET_DAU_S3_PATH+'dau_data/'+date_value)
    #             else:
    #                 logger.info('Partition exists, skipping write operation.')
    #             logger.info('Writing Parquet files for aggregated DAU records.')
    #             if date_value not in dau_agg_dir_list:
    #                 dau_agg_parquet_df.coalesce(1).write.option('compression', 'snappy').parquet(TARGET_DAU_AGG_S3_PATH+'dau_agg_data'/+date_value)
    #             else:
    #                 logger.info('Partition exists, skipping write operation.')
    #         else:
    #             logger.info('Empty dataframe, skipping write operation.')
    #     else:
    #         logger.info('Partition not found, skipping execution for {date_value}')
    logger.info('Completed writing Parquet files to S3.')

def main():
    aws_cred = get_credentials()

    AWS_ACCESS_KEY_ID = aws_cred['AccessKeyId']
    AWS_SECRET_ACCESS_KEY=aws_cred['SecretAccessKey']
    AWS_SESSION_TOKEN=aws_cred['SessionToken']

    spark = configure_spark_session(aws_access_key=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, aws_session_token=AWS_SESSION_TOKEN)

    process_files(spark_session_obj=spark)


if __name__ == '__main__':
    main()
