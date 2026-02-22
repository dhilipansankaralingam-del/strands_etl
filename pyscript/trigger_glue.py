from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import date, datetime, timedelta
from datetime import datetime, timezone, timedelta
import pytz
import zipfile
import io
import os
import re
import time
import csv
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.types import StructType, StructField, TimestampType, LongType
from pyspark.sql.functions import *
import json
from pyspark.sql.session import SparkSession
from awsglue.dynamicframe import DynamicFrame
import boto3
import pandas as pd
import io
from io import StringIO
import logging
from boto3.s3.transfer import TransferConfig

logger = logging.getLogger()
args = getResolvedOptions(sys.argv, ["JOB_NAME", "json_file_name","bucket_name"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
logger = glueContext.get_logger()
job_run_id=args['JOB_RUN_ID']
job_name=args['JOB_NAME']
now = datetime.now()
job_startTime=str(datetime.now())

spark = SparkSession.builder.config('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.iceberg.handle-timestamp-without-timezone','true')\
    .config('spark.sql.catalog.glue_catalog.warehouse', 'ace-da-mem-qlik-landing-us-west-2-847515726144')\
    .config('spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')\
    .config('spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')\
    .config('spark.sql.extensions','org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')\
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic").getOrCreate()
    

bucket_name=args['bucket_name']
json_file_name=args['json_file_name']
s3_client = boto3.client('s3')
response = s3_client.get_object(Bucket=bucket_name, Key=json_file_name)

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DecimalType

def json_to_structtype(dtype):
    type_mapping = {
        'str': StringType(),
        'int': IntegerType()
    }
    
    fields = []
    for col_name, col_type in dtype.items():
        if col_type in type_mapping:
            fields.append(StructField(col_name, type_mapping[col_type], True))
        else:
            raise ValueError(f"Unsupported data type: {col_type}")
    return StructType(fields)


# Read the file content
file_content = response['Body'].read().decode('utf-8')
#print("File content:", file_content)
jobs = json.loads(file_content)
    
for job in jobs:
    #print(job)
    print("*" * 50)
    bucket_name1 = job['s3_bucket']
    landing_loc =  job['landing_loc']
    file_pattern_name = job['file_pattern_name']
    database_name = job['database_name']
    table_name = job['landing_table']
    dtype = job['dtype']
    archive_path = job['archive_s3_path']
    temp_path= job['temp_s3_path']
    
    print(bucket_name1)
    print(landing_loc)
    print(file_pattern_name)
    print(database_name)
    print(table_name)
    print(dtype)
    print(archive_path)
    print(temp_path)

schema = json_to_structtype(dtype)
print("************************")
print(schema)

# List all CSV files in the source folder and its subfolders
paginator = s3_client.get_paginator('list_objects_v2')
response_iterator = paginator.paginate(Bucket=bucket_name, Prefix=landing_loc)

csv_files = []
for page in response_iterator:
    for obj in page.get('Contents', []):
        key = obj['Key']
        if key.endswith('.csv'):
            print('csv file',key)
            csv_files.append(f"s3://{bucket_name}/{key}")



df = spark.read.option("header", "true").schema(schema).csv(csv_files)

# Add new columns with default values
df = df.withColumn('hourly_processed_ind', lit('N'))
df = df.withColumn('daily_processed_ind', lit('N'))

# Get current timestamps
pst = pytz.timezone('US/Pacific')
current_timestamp = datetime.now(pst).strftime('%Y-%m-%d %H:%M:%S')
current_timestamp_utc = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

# Add timestamp columns
df = df.withColumn('insert_timestamp', lit(current_timestamp))
df = df.withColumn('inserted_by', lit('Qlik Staging Lambda'))
df = df.withColumn('updated_timestamp', lit(current_timestamp))
df = df.withColumn('updated_by', lit('Qlik Staging Lambda'))
df = df.withColumn('load_timestamp', lit(current_timestamp_utc))

df.show()
df.cache()
df.createOrReplaceTempView("mem_staging")

print(df.count())

# spark.sql("""
# SELECT count(*), header__partition_name
# FROM mem_staging
# group by 2 
# """).show()

query="""      
        insert into glue_catalog.{0}.{1} SELECT * FROM mem_staging
    """.format(database_name,table_name)
print(query)    
spark.sql(query)

