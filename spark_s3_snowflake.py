# import required python modules

import snowflake.connector
import time
import csv
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import getpass
from boto3.session import Session
import boto3
from botocore.client import Config
import timeit
import collections
import warnings
warnings.filterwarnings('ignore')
username = getpass.getuser()
#os.environ['PYSPARK_PYTHON'] = '/Users/username/opt/anaconda3/bin/python3'.format(username)

# spark init

spark = SparkSession.builder \
    .master("local") \
    .appName("Test") \
    .config("spark.executorEnv.PYTHONPATH", "pyspark.zip:py4j-0.10.9-src.zip")\
    .config('spark.jars','/Users/<username>/Documents/Spark-Jars-Packages/snowflake-jdbc-3.12.8.jar,/Users/<username>/Documents/Spark-Jars-Packages/spark-snowflake_2.12-2.8.1-spark_3.0.jar') \
    .config('spark.jars.packages','org.apache.hadoop:hadoop-aws:3.2.0') \
    .config('spark.sql.warehouse.dir', 'file:///Users/<username>/Documents/Gainsight/Download/') \
    .getOrCreate()
    
# Show Spark version v3.0.1 & view Spark_UI 

spark

# sc init & check hadoop version:  Hadoop version: 3.2.0

sc = SparkContext.getOrCreate()
print("Hadoop version: " + sc._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion())


# import the aws credentials from a local file, you can update aws_profile with different name to different bucket

import configparser
config = configparser.ConfigParser()
config.read(os.path.expanduser("/Users/{0}/Documents/Gainsight/PySpark_Scripts/aws_credentials".format(username)))
aws_profile = "aws_s3_prod_bucket"
bucket_name = config.get(aws_profile, "bucket_name")
access_key = config.get(aws_profile, "aws_access_key_id") 
secret_key = config.get(aws_profile, "aws_secret_access_key")
kms_key_arn = config.get(aws_profile, "kms_key_arn")

print('bucket_name: ', bucket_name)
print('access_key: ', access_key)
print('secret_key: ', secret_key)
print('kms_key_arn: ', kms_key_arn)


# Spark access_key, secret_key, KMS settings 

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
spark._jsc.hadoopConfiguration().set("fs.s3a.enableServerSideEncryption", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.server-side-encryption.key", kms_key_arn)
spark.sparkContext.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.server-side-encryption-algorithm", "SSE-KMS")


# Snowflake credentials settings:

sfOptions = {
  "sfURL" : "<domain.snowflakecomputing.com>",
  "sfUser" : "<sowflake username>",
  "sfPassword" : "<snowflake password>",
  "sfDatabase" : "<snowflake database name>",
  "sfSchema" : "<snowflake schema name>",
  "sfWarehouse" : "<snowflake warehouse name>",
  "sfRole": '<snowflake role name>'
}
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

# List all objects in a specific S3 bucket
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
        
most_recent_files=[]

get_latest_modified = lambda obj: int(obj['LastModified'].strftime('%s'))

objects=get_all_s3_objects(boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key), Bucket=bucket_name)

for files in sorted(objects,key=get_latest_modified,reverse=True):
    if files['Key'][:9]=='gainsight':
        #print(files['Key'],files['LastModified'])
        print(files['Key'])
        if len(most_recent_files) > 20:
            continue
        else:
            most_recent_files.append(files['Key'])
print(most_recent_files) 
print("\n")

if aws_profile == "aws_s3_prod_bucket":
    for i in range(len(most_recent_files)):
        most_recent_files[i] = most_recent_files[i][14:len(most_recent_files[i])]
print(most_recent_files)


# Automate to match the file name & get most recent files

visited={'Customer': False, 
         'CTA': False,
         'Task': False,
         'Health': False,
         'Timeline': False,
         'TimelineNotes': False,
         'SuccessPlan': False,
         'Activity': False,
         'Relationship': False,
         'Scorecard': False,
         'Users': False }

row_num=1
for file_name in most_recent_files:
    if file_name[9:].startswith('Health') and not visited['Health']:
        GainsightHealthScore_Test = file_name
        visited['Health'] = True
        print(row_num,'GainsightHealthScore_Test => ', GainsightHealthScore_Test)
        row_num+=1
    elif file_name[9:].startswith('Task') and not visited['Task']:
        TaskData_Test = file_name
        visited['Task'] = True
        print(row_num,'TaskData_Test => ', TaskData_Test)
        row_num+=1
    elif file_name[9:].startswith('CTA') and not visited['CTA']:
        CTAData_Test = file_name
        visited['CTA'] = True
        print(row_num,'CTAData_Test => ', CTAData_Test)
        row_num+=1
    elif file_name[9:].startswith('Customer') and not visited['Customer']:
        CustomerInfo_Test = file_name
        visited['Customer'] = True
        print(row_num,'CustomerInfo_Test => ', CustomerInfo_Test)
        row_num+=1
    elif file_name[9:].startswith('Timeline-') and not visited['Timeline']:
        Timeline_Test = file_name
        visited['Timeline'] = True
        print(row_num,'Timeline_Test => ', Timeline_Test)
        row_num+=1
    elif file_name[9:].startswith('TimelineNotes') and not visited['TimelineNotes']:
        GainsightTimeLineNotes_Test = file_name
        visited['TimelineNotes'] = True
        print(row_num,'GainsightTimeLineNotes_Test => ', GainsightTimeLineNotes_Test)
        row_num+=1
    elif file_name[9:].startswith('Success') and not visited['SuccessPlan']:
        SuccessPlan_Test = file_name
        visited['SuccessPlan'] = True
        print(row_num,'SuccessPlan_Test => ', SuccessPlan_Test)
        row_num+=1
    elif file_name[9:].startswith('Activity') and not visited['Activity']:
        GainsightActivityAttendee_Test = file_name
        visited['Activity'] = True
        print(row_num,'GainsightActivityAttendee_Test => ', GainsightActivityAttendee_Test)
        row_num+=1
    elif file_name[9:].startswith('RelationshipData') and not visited['Relationship']:
        GainsightRelationshipData_Test = file_name
        visited['Relationship'] = True
        print(row_num,'GainsightRelationshipData_Test => ', GainsightRelationshipData_Test)
        row_num+=1
    elif file_name[9:].startswith('RelationshipScore') and not visited['Scorecard']:
        GainsightRelationshipScorecard_Test = file_name
        visited['Scorecard'] = True
        print(row_num,'GainsightRelationshipScorecard_Test => ', GainsightRelationshipScorecard_Test)
        row_num+=1
    elif file_name[9:].startswith('Users') and not visited['Users']:
        GainsightUsers_Test = file_name
        visited['Users'] = True
        print(row_num,'GainsightUsers_Test => ', GainsightUsers_Test)
        row_num+=1

# Give a s3 path to read the selected key as a spark dataframe
# select which key you want to read as a dataframe

key_name = ''
key_path = "s3a://"+ bucket_name + "/gainsight_1.0/"+ key_name
spark_df=spark.read.format("csv").options(header="true").option("delimiter", "\t").load(key_path)

# print the schema for spark dataframe
spark_df.printSchema()

# spark register a temp table and execute query
spark_df.createOrReplaceTempView("test")
spark.sql('<sql query>').show()

# Write query result into Snowflake table
start = timeit.default_timer()
spark_query_res.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "SPARK_TEST").mode("Overwrite").save()
stop = timeit.default_timer()
print('Time in seconds: ', stop - start) 


# spark read local csv file
input_file_path = '<local input file path>'
output_file_path = '<local output file path>'
spark_df_local = spark.read.format("csv").options(header="true").option("delimiter", "\t").load(input_file_path)
spark_df_local.createOrReplaceTempView("test")
spark_query_local=spark.sql('select * from test limit 1')
spark_query_local.coalesce(1).write.mode("overwrite").csv(output_file_path, header=True)


# download objects from S3 bucket

s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

def download_new_s3(Download_File_Name,Download_File_To_Path):
    print("Download file name is", Download_File_Name)
    print("Downloading to",Download_File_To_Path)
    s3.download_file(bucket_name, Download_File_Name, Download_File_To_Path)
    print('Successfully downloaded the', Download_File_Name)

Download_File_Name = <download_file_name>
Download_File_To_Path='/Users/{0}/Desktop/{1}'.format(username,Download_File_Name)
download_new_s3(Download_File_Name, Download_File_To_Path)


# spark-snowflake
df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("query",  'select * from <snowflake schema>.<snowflake table name>') \
  .load()
sdf.createOrReplaceTempView("mytable")
spark.sql('select * from mytable limit 1').show()
start = timeit.default_timer()
#df_new.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "SPARK_TEST").mode("Overwrite").save()
stop = timeit.default_timer()
print('Time in seconds: ', stop - start) 

# List all current fields name in each s3 object 

Name_Dict=collections.defaultdict(list)

s3_file_names=[ CustomerInfo_Test, CTAData_Test, TaskData_Test, Timeline_Test, SuccessPlan_Test, 
GainsightHealthScore_Test, GainsightTimeLineNotes_Test, GainsightUsers_Test, GainsightActivityAttendee_Test, 
            GainsightRelationshipScorecard_Test, GainsightRelationshipData_Test]

csv_column_names=['CustomerInfo','CTA','Task','Timeline','SuccessPlan','HealthScore','TimelineNotes', 'Users', 
                  'ActivityAttendee', 'RelationshipScorecard', 'RelationshipData']

dev_s3_path = ''
prod_s3_path = ''

for i in range(len(s3_file_names)):
	path='s3a://'+ prod_s3_path +'/'+ s3_file_names[i]
	sdf=spark.read.format("csv").options(header="true").option("delimiter", "\t").load(path)
	sdf.createOrReplaceTempView("mytable")
	sdf=spark.sql('select * from mytable limit 1')
	pdf=sdf.toPandas()
	Name_Dict[csv_column_names[i]] = list(pdf.columns)
	print(csv_column_names[i]+ ' has been loaded')
res=pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in Name_Dict.items()]))

res.to_csv('/Users/<username>/Documents/Files_Columns.csv',index=False)
print('Done :)')


# Row count sanity test using PySpark:

snowflake_table_list=['GAINSIGHTCUSTOMERINFO','TEST_GAINSIGHTCUSTOMERINFO',
                      'GAINSIGHTCTAANDTASKS','TEST_GAINSIGHTCTAANDTASKS',
                      'GAINSIGHTHISTORICALHEALTHSCORE','TEST_GAINSIGHTHISTORICALHEALTHSCORE',
                      'GAINSIGHTTIMELINEACTIVITY', 'TEST_GAINSIGHTTIMELINEACTIVITY',
                      'GAINSIGHTSUCCESSPLANS', 'TEST_GAINSIGHTSUCCESSPLANS',
                      'GAINSIGHTENGAGEMENTSCORE','TEST_GAINSIGHTENGAGEMENTSCORE',
                      'TEST_GAINSIGHTRELATIONSHIP', 'TEST_GAINSIGHTRELATIONSHIP',
                      'TEST_GAINSIGHTRELATIONSHIPSCORECARD', 'TEST_GAINSIGHTRELATIONSHIPSCORECARD',
                      'TEST_GAINSIGHTACTIVITYATTENDEE', 'TEST_GAINSIGHTACTIVITYATTENDEE',
                      'TEST_GAINSIGHTUSERS', 'TEST_GAINSIGHTUSERS']
table_names = ['CustomerInfo', 'CTA&Task', 'HealthScore', 'TimelineActivity', 'SuccessPlan','EngagementScore',
               'RELATIONSHIPData', 'RELATIONSHIPSCORECARD', 'ACTIVITYATTENDEE', 'USERS']
temp_lst=[]
Diff_Dict=collections.OrderedDict()
Diff_Dict['Name']=['Prod_Table','Test_Table']
k=0

while len(table_names)>0:
    for i in range(0,2):
        run_query = 'select count(*) as cnt from '+ snowflake_table_list[i]
        res1 = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("query", run_query).load()
        res2= res1.toPandas()
        res2['CNT']=res2.CNT.astype(float)
        temp_lst.append(res2.iloc[0]['CNT'])
    print('The number of rows of',table_names[k],'is calculating ...')
    Diff_Dict[table_names[k]]=temp_lst
    temp_lst=[]
    snowflake_table_list.pop(0)
    snowflake_table_list.pop(0)
    table_names.pop(0)
    

print('Start to write into the CSV file')
res2=pd.DataFrame(dict([(k,pd.Series(v)) for k,v in Diff_Dict.items()]))
res2.to_csv('/Users/<username>/Documents/Row_Count_Comparison.csv',index=False)
print('Done :)')

# spark and sparkContext stop
spark.stop()
sc.stop()







