'''
This is the whole ETL process for this code using IAM role instead of IAM user credentials
snowflake -> S3 (partition by date save as parquet file) -> snowflake
'''

'''
Pre-condition
1. Create an IAM role

Create a role from IAM page.
Role name: Spark_POC_Role
Role ARN: arn:aws:iam::<aws account number>:role/Spark_POC_ROLE

2. Create an IAM policy and attach to this role
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Action": [
                "s3:ListAllMyBuckets"
            ],
            "Resource": "*"
        },
        {
            "Sid": "",
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Resource": "arn:aws:iam::<aws account number>:role/Spark_POC_ROLE"
        },
        {
            "Sid": "",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:ListBucket",
                "s3:GetObjectVersion",
                "s3:GetObjectAcl",
                "s3:GetObject",
                "s3:GetBucketLocation",
                "s3:GetBucketAcl",
                "s3:DeleteObjectVersion",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::<s3 bucket name>/*",
                "arn:aws:s3:::<s3 bucket name>"
            ]
        }
    ]
}

3. Edit Role Trust Relationship

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<aws account number>:root"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}

4. Open your terminal and add your profile name into the aws credentials file.

file path: /Users/{username}/.aws/credentials

[saml]
...
...

[spark]
role_arn = arn:aws:iam::<aws account number>:role/Spark_POC_ROLE
source_profile = saml

5. Run export AWS_PROFILE=spark in your terminal
6. Run this command to test in terminal: aws s3 ls --profile spark
'''

# Start your Python code to list objects in S3 bucket
import boto3
session = boto3.session.Session(profile_name='spark')
sts_connection = session.client('sts')
response = sts_connection.assume_role(RoleArn='arn:aws:iam::<aws account number>:role/Spark_POC_ROLE', RoleSessionName='Spark_Session',DurationSeconds=3600)
credentials = response['Credentials']

# list temp aws credentials
aws_access_key_id=credentials['AccessKeyId']
aws_secret_access_key=credentials['SecretAccessKey']
aws_session_token=credentials['SessionToken']

# List all objects in target S3 bucket
def get_all_s3_objects(s3, **base_kwargs):
    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3.list_objects_v2(**list_kwargs)
        
        yield from response.get('Contents', [])
        if not response.get('IsTruncated'):  # At the end of the list?
            break
        continuation_token = response.get('NextContinuationToken')
        
get_latest_modified = lambda obj: int(obj['LastModified'].strftime('%s'))

bucket_name = '<s3 bucket name>'

objects=get_all_s3_objects( \
    boto3.client('s3', \
    aws_access_key_id=aws_access_key_id, \
    aws_secret_access_key=aws_secret_access_key, \
    aws_session_token=aws_session_token), Bucket=bucket_name)

for files in sorted(objects,key=get_latest_modified,reverse=True):
        print('object_name:', files['Key'])

'''
Before launching spark instance, you need to make sure all jars has been downloaded properly.
Here's the spark and hadoop version I'm using. 
- Spark Version 3.0.1
- Hadoop version: 3.2.0
aws-java-sdk-bundle-1.11.375.jar can be found from https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle/1.11.375 by search aws-hadoop
'''

# spark init
spark = SparkSession.builder \
    .master("local[4]") \
    .appName("Test_IAM_ROLE") \
    .config("spark.executorEnv.PYTHONPATH", "pyspark.zip:py4j-0.10.9-src.zip")\
    .config('spark.jars','/Users/simon.zhao/Documents/Spark-Jars-Packages/snowflake-jdbc-3.12.8.jar,/Users/simon.zhao/Documents/Spark-Jars-Packages/spark-snowflake_2.12-2.8.1-spark_3.0.jar,/Users/simon.zhao/Documents/Spark-Jars-Packages/aws-java-sdk-bundle-1.11.375.jar') \
    .config('spark.jars.packages','org.apache.hadoop:hadoop-aws:3.2.0') \
    .config('spark.sql.warehouse.dir', 'file:///Users/simon.zhao/Documents/Gainsight_2021/Download/') \
    .getOrCreate()

# sc init & check hadoop version:  Hadoop version: 3.2.0
print('Spark Version',spark.version)
sc = SparkContext.getOrCreate()
print("Hadoop version: " + sc._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion())

# !! since token may expire, so it's better to restart your spark instance everytime
spark._jsc.hadoopConfiguration().set('fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key_id)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.session.token", aws_session_token)

# Load Snowflake credentials settings:

config.read(os.path.expanduser("/Users/{0}/Documents/snowflake_credentials".format(username)))
snowflake_profile = "snowflake"
snowflake_account = config.get(snowflake_profile, "account")
snowflake_user = config.get(snowflake_profile, "user")
snowflake_password = config.get(snowflake_profile, "password")
snowflake_db = config.get(snowflake_profile, "database")
snowflake_schema = config.get(snowflake_profile, "schema")
snowflake_warehouse = config.get(snowflake_profile, "warehouse")
snowflake_role = config.get(snowflake_profile, "role")

sfOptions = {
  "sfURL" : snowflake_account + ".snowflakecomputing.com",
  "sfUser" : snowflake_user,
  "sfPassword" : snowflake_password,
  "sfDatabase" : snowflake_db,
  "sfSchema" : snowflake_schema,
  "sfWarehouse" : snowflake_warehouse,
  "sfRole": snowflake_role
}
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

# Load data from Snowflake using Spark

query = '''
          select *
          from <db>.<schema>.<table>
          where CAST(CREATED_AT AS DATE) = date '2020-12-25'
        '''

spark_df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .option("query",  query) \
        .load()

spark_df.createOrReplaceTempView("snow_table")
res = spark.sql('select *, CAST(CREATED_AT AS DATE) as CREATED_AT_DATE from snow_table')
res.show()

# Spark write to S3 ->  partitioned by created date and save as parquet file
# mode: append VS overwrite
start = timeit.default_timer()
parquet_s3_path = "s3a://" + bucket_name + "/spark/parquet"
res.write.mode('append').partitionBy("CREATED_AT_DATE").parquet(parquet_s3_path)
stop = timeit.default_timer()
print('Time consumption in seconds: ', stop - start)

# spark read from parquet
parquet_s3_path = "s3a://" + bucket_name + "/spark/parquet"
spark_read_df = spark.read.parquet(parquet_s3_path)
spark_read_df.createOrReplaceTempView("read_from_parquet")
read_res = spark.sql("select CREATED_AT_DATE, count(*) as num_rows from read_from_parquet group by 1 order by 2")
read_res.show()

# spark read from parquet and write to snowflake
start = timeit.default_timer()
read_res.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "SPARK_TEST").mode("Overwrite").save()
stop = timeit.default_timer()
print('Time consumption in seconds: ', stop - start)

# stop Spark instance
spark.stop()


