"""
Enhanced Config-Driven Glue ETL Job with Comprehensive Validations

Features:
- Schema derived from staging table
- Config-driven validation queries
- Age-based folder filtering (> 59 minutes)
- File count validation
- Comprehensive audit logging
- FAIL file checkpoint system
- Archive files only after validation passes
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, timezone, timedelta
import pytz
import json
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
import boto3
from botocore.exceptions import ClientError
import logging
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import base64
from io import BytesIO

# Try to import matplotlib for health check charts
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    CHARTS_AVAILABLE = True
except ImportError:
    CHARTS_AVAILABLE = False
    logger.warning("matplotlib not available - health check charts will be disabled")

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "json_file_name", "bucket_name"])

# Initialize Spark/Glue context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Job metadata
job_run_id = args['JOB_RUN_ID']
job_name = args['JOB_NAME']
job_start_time = datetime.now()

# Configure Spark with Iceberg support
spark = SparkSession.builder \
    .config('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.SparkCatalog') \
    .config('spark.sql.iceberg.handle-timestamp-without-timezone', 'true') \
    .config('spark.sql.catalog.glue_catalog.warehouse', 'ace-da-mem-qlik-landing-us-west-2-847515726144') \
    .config('spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog') \
    .config('spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO') \
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

# S3 client
s3_client = boto3.client('s3')
bucket_name = args['bucket_name']
json_file_name = args['json_file_name']

# Audit data
audit_records = []


def check_fail_file_exists(bucket, table_name):
    """
    Check if a FAIL file exists for the given table.
    Aborts job if FAIL file is present.
    """
    fail_file_key = f"status/{table_name}.FAIL"

    try:
        s3_client.head_object(Bucket=bucket, Key=fail_file_key)

        # If we get here, the file exists - this is a failure condition
        logger.error(f"❌ FAIL file detected: s3://{bucket}/{fail_file_key}")
        logger.error(f"❌ Job aborted. Please investigate and remove the FAIL file to retry.")

        # Read fail file content if it exists
        try:
            response = s3_client.get_object(Bucket=bucket, Key=fail_file_key)
            fail_content = response['Body'].read().decode('utf-8')
            logger.error(f"FAIL file content:\n{fail_content}")
        except:
            pass

        raise Exception(f"FAIL file exists for table {table_name}. Job aborted.")

    except ClientError as e:
        # Check if error is 404 (file not found) - this is the expected/good case
        if e.response['Error']['Code'] == '404':
            logger.info(f"✓ No FAIL file found for {table_name}. Proceeding...")
            return False
        else:
            # Some other S3 error occurred
            logger.error(f"Error checking FAIL file: {str(e)}")
            raise

    except Exception as e:
        if "FAIL file exists" in str(e):
            raise
        logger.error(f"Unexpected error checking FAIL file: {str(e)}")
        raise


def create_fail_file(bucket, table_name, failure_reason):
    """
    Create a FAIL file with failure details.
    """
    fail_file_key = f"status/{table_name}.FAIL"

    fail_content = f"""
FAILURE DETAILS
===============
Table: {table_name}
Timestamp: {datetime.now().isoformat()}
Job Name: {job_name}
Job Run ID: {job_run_id}

Failure Reason:
{failure_reason}

Action Required:
1. Investigate the failure reason above
2. Fix the data quality issues
3. Delete this FAIL file: s3://{bucket}/{fail_file_key}
4. Re-run the job
"""

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=fail_file_key,
            Body=fail_content.encode('utf-8')
        )
        logger.error(f"❌ Created FAIL file: s3://{bucket}/{fail_file_key}")
    except Exception as e:
        logger.error(f"Error creating FAIL file: {str(e)}")


def delete_fail_file(bucket, table_name):
    """
    Delete FAIL file after successful run.
    """
    fail_file_key = f"status/{table_name}.FAIL"

    try:
        s3_client.delete_object(Bucket=bucket, Key=fail_file_key)
        logger.info(f"✓ Deleted old FAIL file (if existed): s3://{bucket}/{fail_file_key}")
    except Exception as e:
        logger.warning(f"Could not delete FAIL file (may not exist): {str(e)}")


def get_folders_older_than_59_minutes(bucket, prefix):
    """
    Get subfolders that are older than 59 minutes.
    Returns list of folder paths.
    """
    logger.info(f"Scanning for folders older than 59 minutes in: s3://{bucket}/{prefix}")

    current_time = datetime.now(timezone.utc)
    age_threshold = timedelta(minutes=59)

    # List all objects
    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')

    eligible_folders = []

    for page in response_iterator:
        # Get subfolders (CommonPrefixes)
        for prefix_info in page.get('CommonPrefixes', []):
            folder_path = prefix_info['Prefix']

            # Get the oldest file in this folder to determine folder age
            folder_objects = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=folder_path,
                MaxKeys=1
            )

            if 'Contents' in folder_objects and len(folder_objects['Contents']) > 0:
                oldest_file = folder_objects['Contents'][0]
                last_modified = oldest_file['LastModified']

                # Calculate age
                age = current_time - last_modified

                logger.info(f"  Folder: {folder_path}")
                logger.info(f"    Last modified: {last_modified}")
                logger.info(f"    Age: {age.total_seconds() / 60:.2f} minutes")

                if age > age_threshold:
                    eligible_folders.append(folder_path)
                    logger.info(f"    ✓ Eligible (> 59 minutes old)")
                else:
                    logger.info(f"    ✗ Too recent (< 59 minutes old)")

    logger.info(f"Found {len(eligible_folders)} eligible folders")
    return eligible_folders


def get_csv_files_from_folders(bucket, folders):
    """
    Get all CSV files from the specified folders.
    Returns dict: {folder_path: [file_paths]}
    """
    files_by_folder = {}
    total_files = 0

    for folder in folders:
        logger.info(f"Scanning CSV files in: {folder}")

        paginator = s3_client.get_paginator('list_objects_v2')
        response_iterator = paginator.paginate(Bucket=bucket, Prefix=folder)

        csv_files = []
        for page in response_iterator:
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.csv'):
                    csv_files.append(f"s3://{bucket}/{key}")

        files_by_folder[folder] = csv_files
        total_files += len(csv_files)
        logger.info(f"  Found {len(csv_files)} CSV files")

    logger.info(f"Total CSV files across all folders: {total_files}")
    return files_by_folder


def count_rows_in_csv_files(bucket, files_by_folder):
    """
    Count total rows in CSV files (excluding headers).
    Returns total row count.
    """
    total_rows = 0

    logger.info("Counting rows in CSV files...")

    for folder, files in files_by_folder.items():
        folder_rows = 0
        for file_path in files:
            # Extract bucket and key from s3:// path
            key = file_path.replace(f"s3://{bucket}/", "")

            try:
                response = s3_client.get_object(Bucket=bucket, Key=key)
                content = response['Body'].read().decode('utf-8')

                # Count lines (subtract 1 for header)
                line_count = len(content.strip().split('\n'))
                # Use conditional instead of max() to avoid conflict with PySpark's max
                row_count = line_count - 1 if line_count > 0 else 0

                folder_rows += row_count
                logger.info(f"    {key}: {row_count} rows")
            except Exception as e:
                logger.error(f"Error counting rows in {file_path}: {str(e)}")

        logger.info(f"  {folder}: {folder_rows} rows")
        total_rows += folder_rows

    logger.info(f"Total rows in all CSV files: {total_rows}")
    return total_rows


def get_table_schema(database_name, table_name):
    """
    Get schema from existing Glue table.
    Returns StructType schema.
    """
    logger.info(f"Fetching schema from table: {database_name}.{table_name}")

    try:
        # Read one row from the table to get schema
        sample_df = spark.sql(f"SELECT * FROM glue_catalog.{database_name}.{table_name} LIMIT 1")
        schema = sample_df.schema

        # Filter out audit columns that we'll add ourselves
        audit_columns = [
            'hourly_processed_ind', 'daily_processed_ind',
            'insert_timestamp', 'inserted_by',
            'updated_timestamp', 'updated_by', 'load_timestamp'
        ]

        from pyspark.sql.types import StructType
        business_fields = [field for field in schema.fields
                          if field.name not in audit_columns]

        business_schema = StructType(business_fields)

        logger.info(f"✓ Schema fetched successfully")
        logger.info(f"  Business columns: {len(business_fields)}")

        return business_schema
    except Exception as e:
        logger.error(f"Error fetching table schema: {str(e)}")
        raise


def run_validation_queries(validation_queries, database_name, table_name):
    """
    Run validation queries from config.
    Returns tuple: (all_passed, failed_validations)
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"RUNNING VALIDATION QUERIES")
    logger.info(f"{'='*60}")

    all_passed = True
    failed_validations = []

    for i, validation in enumerate(validation_queries, 1):
        query_name = validation.get('name', f'Validation_{i}')
        query_sql = validation['query']
        max_allowed = validation.get('max_allowed', 0)  # Default: count should be 0

        logger.info(f"\n[{i}] {query_name}")
        logger.info(f"Query: {query_sql}")
        logger.info(f"Max allowed count: {max_allowed}")

        try:
            # Replace placeholder with actual table reference
            formatted_query = query_sql.format(
                database=database_name,
                table=table_name,
                full_table=f"glue_catalog.{database_name}.{table_name}"
            )

            result = spark.sql(formatted_query)
            count = result.collect()[0][0]

            logger.info(f"Result count: {count}")

            if count > max_allowed:
                all_passed = False
                failed_validations.append({
                    'name': query_name,
                    'query': query_sql,
                    'expected_max': max_allowed,
                    'actual_count': count
                })
                logger.error(f"❌ FAILED: Count {count} exceeds maximum {max_allowed}")
            else:
                logger.info(f"✓ PASSED: Count {count} within limit {max_allowed}")
        except Exception as e:
            all_passed = False
            failed_validations.append({
                'name': query_name,
                'query': query_sql,
                'error': str(e)
            })
            logger.error(f"❌ ERROR running validation: {str(e)}")

    logger.info(f"\n{'='*60}")
    logger.info(f"VALIDATION SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"Total validations: {len(validation_queries)}")
    logger.info(f"Passed: {len(validation_queries) - len(failed_validations)}")
    logger.info(f"Failed: {len(failed_validations)}")

    if all_passed:
        logger.info("✓ ALL VALIDATIONS PASSED")
    else:
        logger.error("❌ VALIDATION FAILURES DETECTED")

    return all_passed, failed_validations


def move_folders_to_archive(bucket, folders, archive_path):
    """
    Move processed folders to archive location.
    Maintains folder structure.
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"MOVING FOLDERS TO ARCHIVE")
    logger.info(f"{'='*60}")

    for folder in folders:
        logger.info(f"Processing folder: {folder}")

        # List all objects in the folder
        paginator = s3_client.get_paginator('list_objects_v2')
        response_iterator = paginator.paginate(Bucket=bucket, Prefix=folder)

        objects_moved = 0

        for page in response_iterator:
            for obj in page.get('Contents', []):
                source_key = obj['Key']

                # Construct destination key in archive
                # Extract the subfolder name from source
                relative_path = source_key.replace(folder, '', 1).lstrip('/')
                dest_key = f"{archive_path.rstrip('/')}/{folder.rstrip('/')}/{relative_path}"

                try:
                    # Copy to archive
                    s3_client.copy_object(
                        Bucket=bucket,
                        CopySource={'Bucket': bucket, 'Key': source_key},
                        Key=dest_key
                    )

                    # Delete original
                    s3_client.delete_object(Bucket=bucket, Key=source_key)

                    objects_moved += 1
                except Exception as e:
                    logger.error(f"Error moving {source_key} to {dest_key}: {str(e)}")

        logger.info(f"  ✓ Moved {objects_moved} objects from {folder}")

    logger.info(f"✓ All folders archived successfully")


def write_audit_record(bucket, table_name, audit_data, database_name):
    """
    Write audit record to S3 in two formats:
    1. Human-readable text format (for manual review)
    2. Parquet format (for querying via Athena/Glue)

    Then runs MSCK REPAIR TABLE to discover new partitions
    """
    current_time = datetime.now()
    timestamp_str = current_time.strftime('%Y%m%d_%H%M%S')

    # 1. Write human-readable audit record
    audit_key = f"audit/{table_name}/audit_{timestamp_str}.txt"

    audit_content = f"""
AUDIT RECORD
============
Staging Table Name: {audit_data['table_name']}
Job Name: {job_name}
Job Run ID: {job_run_id}

Subfolders Considered for Load:
{chr(10).join(['  - ' + f for f in audit_data['folders']])}

Number of Files Used: {audit_data['file_count']}
File Row Count: {audit_data['file_row_count']}
Loaded Row Count: {audit_data['loaded_count']}
Count Match: {'PASS' if audit_data['file_row_count'] == audit_data['loaded_count'] else 'FAIL'}

Validation Checks: {audit_data['validation_status']}
{audit_data.get('validation_details', '')}

Load Timestamp: {audit_data['load_timestamp']}
Duration: {audit_data.get('duration', 'N/A')}

Status: {audit_data['status']}
"""

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=audit_key,
            Body=audit_content.encode('utf-8')
        )
        logger.info(f"✓ Human-readable audit written: s3://{bucket}/{audit_key}")
    except Exception as e:
        logger.error(f"Error writing text audit record: {str(e)}")

    # 2. Write Parquet audit record for table
    # Partitioned by year/month/day for MSCK REPAIR support
    year = current_time.strftime('%Y')
    month = current_time.strftime('%m')
    day = current_time.strftime('%d')

    # Convert folders list to comma-separated string
    folders_str = ','.join(audit_data['folders'])

    # Count match
    count_match = 'PASS' if audit_data['file_row_count'] == audit_data['loaded_count'] else 'FAIL'

    # Parse duration to seconds (handle both "N/A" and numeric values)
    duration_str = str(audit_data.get('duration', 'N/A'))
    if duration_str == 'N/A':
        duration_seconds = 0.0
    else:
        # Extract numeric part (e.g., "123.45 seconds" -> 123.45)
        try:
            duration_seconds = float(duration_str.split()[0])
        except:
            duration_seconds = 0.0

    # Create DataFrame with audit data
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType

    audit_schema = StructType([
        StructField("table_name", StringType(), False),
        StructField("job_name", StringType(), False),
        StructField("job_run_id", StringType(), False),
        StructField("folders", StringType(), True),
        StructField("file_count", IntegerType(), True),
        StructField("file_row_count", LongType(), True),
        StructField("loaded_count", LongType(), True),
        StructField("count_match", StringType(), True),
        StructField("validation_status", StringType(), True),
        StructField("validation_details", StringType(), True),
        StructField("load_timestamp", StringType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("status", StringType(), True)
    ])

    audit_row = [(
        str(audit_data['table_name']),
        str(job_name),
        str(job_run_id),
        folders_str,
        int(audit_data['file_count']),
        int(audit_data['file_row_count']),
        int(audit_data['loaded_count']),
        count_match,
        str(audit_data['validation_status']),
        str(audit_data.get('validation_details', '')).replace('\n', ' ').replace('|', '_'),
        str(audit_data['load_timestamp']),
        duration_seconds,
        str(audit_data['status'])
    )]

    audit_df = spark.createDataFrame(audit_row, schema=audit_schema)

    # Add partition columns
    audit_df = audit_df.withColumn('year', lit(year))
    audit_df = audit_df.withColumn('month', lit(month))
    audit_df = audit_df.withColumn('day', lit(day))

    # Write as Parquet with partitioning
    partition_path = f"s3://{bucket}/audit_table/"

    try:
        audit_df.write \
            .mode('append') \
            .partitionBy('year', 'month', 'day') \
            .parquet(partition_path)

        logger.info(f"✓ Parquet audit written: {partition_path}")
        logger.info(f"  Partition: year={year}, month={month}, day={day}")
    except Exception as e:
        logger.error(f"Error writing Parquet audit: {str(e)}")
        import traceback
        traceback.print_exc()

    # 3. Run MSCK REPAIR TABLE to discover new partitions
    try:
        logger.info("Running MSCK REPAIR TABLE to discover new partitions...")
        repair_query = f"MSCK REPAIR TABLE glue_catalog.{database_name}.etl_audit_log"
        spark.sql(repair_query)
        logger.info(f"✓ MSCK REPAIR completed successfully")
    except Exception as e:
        logger.warning(f"MSCK REPAIR failed (table may not exist yet): {str(e)}")
        logger.warning(f"  Create table using: sql/create_audit_table_parquet.sql")


################################################################################
# HEALTH CHECK MODULE
################################################################################

def check_if_health_check_ran_today(bucket):
    """Check if health check already ran today"""
    current_date = datetime.now().strftime('%Y-%m-%d')
    marker_key = f"health_check_markers/{current_date}.ran"

    try:
        s3_client.head_object(Bucket=bucket, Key=marker_key)
        return True  # Marker exists, already ran today
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False  # Marker doesn't exist
        raise


def create_health_check_marker(bucket):
    """Create marker file indicating health check ran today"""
    current_date = datetime.now().strftime('%Y-%m-%d')
    marker_key = f"health_check_markers/{current_date}.ran"

    content = f"Health check completed at {datetime.now().isoformat()}"
    s3_client.put_object(Bucket=bucket, Key=marker_key, Body=content.encode('utf-8'))
    logger.info(f"✓ Created health check marker: {marker_key}")


def should_run_health_check(health_check_config):
    """Determine if health check should run based on time and configuration"""
    if not health_check_config.get('enabled', False):
        return False

    # Check if it's the right time
    trigger_hour = health_check_config.get('trigger_hour', 21)  # Default 9 PM
    current_hour = datetime.now().hour

    # Only run if current hour matches or is after trigger hour
    if current_hour < trigger_hour:
        logger.info(f"Health check scheduled for {trigger_hour}:00, current hour is {current_hour}:00")
        return False

    return True


def run_athena_query_for_health_check(query, database, output_location):
    """Execute Athena query for health check"""
    try:
        athena_client = boto3.client('athena')

        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )

        query_execution_id = response['QueryExecutionId']

        # Wait for completion
        max_attempts = 60
        for attempt in range(max_attempts):
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']

            if status == 'SUCCEEDED':
                break
            elif status in ['FAILED', 'CANCELLED']:
                reason = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                raise Exception(f"Query {status}: {reason}")

            time.sleep(2)

        # Get results
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        return results

    except Exception as e:
        logger.error(f"Athena query error: {str(e)}")
        raise


def parse_athena_results(results):
    """Parse Athena results into list of dicts"""
    rows = results['ResultSet']['Rows']
    if len(rows) == 0:
        return []

    headers = [col['VarCharValue'] for col in rows[0]['Data']]

    data = []
    for row in rows[1:]:
        row_data = {}
        for i, col in enumerate(row['Data']):
            row_data[headers[i]] = col.get('VarCharValue', '')
        data.append(row_data)

    return data


def get_daily_audit_summary_for_health_check(health_check_config, check_date):
    """Query audit table for daily summary"""
    database = health_check_config['database']
    table = health_check_config['audit_table']
    output_location = health_check_config['athena_output_location']

    year = check_date.strftime('%Y')
    month = check_date.strftime('%m')
    day = check_date.strftime('%d')

    query = f"""
    SELECT
        table_name,
        COUNT(*) as run_count,
        SUM(CAST(file_count AS BIGINT)) as total_file_count,
        SUM(CAST(file_row_count AS BIGINT)) as total_file_rows,
        SUM(CAST(loaded_count AS BIGINT)) as total_loaded_rows,
        COUNT(CASE WHEN count_match = 'PASS' THEN 1 END) as count_match_pass,
        COUNT(CASE WHEN count_match = 'FAIL' THEN 1 END) as count_match_fail,
        COUNT(CASE WHEN validation_status = 'PASS' THEN 1 END) as validation_pass,
        COUNT(CASE WHEN validation_status = 'FAIL' THEN 1 END) as validation_fail,
        COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as success_count,
        COUNT(CASE WHEN status = 'FAIL' THEN 1 END) as fail_count
    FROM {database}.{table}
    WHERE year = '{year}' AND month = '{month}' AND day = '{day}'
    GROUP BY table_name
    """

    results = run_athena_query_for_health_check(query, database, output_location)
    return parse_athena_results(results)


def check_health_status(summary):
    """Check overall health and return issues"""
    issues = []

    for table_data in summary:
        table_name = table_data['table_name']

        # Check file count vs loaded count
        total_file_rows = int(table_data.get('total_file_rows', 0))
        total_loaded_rows = int(table_data.get('total_loaded_rows', 0))

        if total_file_rows != total_loaded_rows:
            issues.append({
                'table': table_name,
                'type': 'COUNT_MISMATCH',
                'message': f"File rows ({total_file_rows}) != Loaded rows ({total_loaded_rows})"
            })

        # Check count match failures
        if int(table_data.get('count_match_fail', 0)) > 0:
            issues.append({
                'table': table_name,
                'type': 'COUNT_MATCH_FAIL',
                'message': f"{table_data['count_match_fail']} runs had count failures"
            })

        # Check validation failures
        if int(table_data.get('validation_fail', 0)) > 0:
            issues.append({
                'table': table_name,
                'type': 'VALIDATION_FAIL',
                'message': f"{table_data['validation_fail']} validation failures"
            })

        # Check job failures
        if int(table_data.get('fail_count', 0)) > 0:
            issues.append({
                'table': table_name,
                'type': 'JOB_FAIL',
                'message': f"{table_data['fail_count']} job runs failed"
            })

    is_healthy = len(issues) == 0
    return is_healthy, issues


def create_donut_chart(passed, failed):
    """Create donut chart for pass/fail"""
    if not CHARTS_AVAILABLE:
        return None

    try:
        fig, ax = plt.subplots(figsize=(4, 4))
        sizes = [passed, failed]
        colors = ['#10B981', '#EF4444']
        labels = [f'Passed\n{passed}', f'Failed\n{failed}']

        wedges, texts = ax.pie(sizes, colors=colors, startangle=90,
                                wedgeprops=dict(width=0.5))

        for i, text in enumerate(texts):
            text.set_text(labels[i])
            text.set_fontsize(12)
            text.set_weight('bold')

        ax.axis('equal')

        buffer = BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight', dpi=100, transparent=True)
        buffer.seek(0)
        image_base64 = base64.b64encode(buffer.read()).decode()
        plt.close()

        return image_base64
    except Exception as e:
        logger.error(f"Error creating donut chart: {str(e)}")
        return None


def create_bar_chart(issues):
    """Create bar chart for issues"""
    if not CHARTS_AVAILABLE or not issues:
        return None

    try:
        issue_counts = {}
        for issue in issues:
            issue_type = issue['type']
            issue_counts[issue_type] = issue_counts.get(issue_type, 0) + 1

        if not issue_counts:
            return None

        fig, ax = plt.subplots(figsize=(6, 3))
        categories = list(issue_counts.keys())
        counts = list(issue_counts.values())
        colors = ['#EF4444', '#F59E0B', '#8B5CF6', '#EC4899']

        bars = ax.barh(categories, counts, color=colors[:len(categories)])
        ax.set_xlabel('Count', fontsize=10, weight='bold')
        ax.set_title('Issues by Category', fontsize=12, weight='bold', pad=10)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax.text(width, bar.get_y() + bar.get_height()/2,
                   f' {int(width)}', ha='left', va='center',
                   fontsize=10, weight='bold')

        plt.tight_layout()

        buffer = BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight', dpi=100, transparent=True)
        buffer.seek(0)
        image_base64 = base64.b64encode(buffer.read()).decode()
        plt.close()

        return image_base64
    except Exception as e:
        logger.error(f"Error creating bar chart: {str(e)}")
        return None


def generate_html_email(summary, issues, check_date, health_check_config):
    """Generate HTML email with dashboard"""
    current_date_str = check_date.strftime('%B %d, %Y')
    current_time_str = datetime.now().strftime('%I:%M %p')

    # Calculate metrics
    total_passed = sum(int(s.get('validation_pass', 0)) + int(s.get('count_match_pass', 0)) for s in summary)
    total_failed = sum(int(s.get('validation_fail', 0)) + int(s.get('count_match_fail', 0)) for s in summary)

    passed_count = total_passed
    failed_count = total_failed

    # Health score
    health_score = (passed_count / (passed_count + failed_count) * 100) if (passed_count + failed_count) > 0 else 0
    health_color = "#10B981" if health_score >= 80 else "#F59E0B" if health_score >= 60 else "#EF4444"
    health_emoji = "🎉" if health_score >= 90 else "👍" if health_score >= 70 else "⚠️" if health_score >= 50 else "🚨"

    # Generate charts
    donut_chart = create_donut_chart(passed_count, failed_count)
    bar_chart = create_bar_chart(issues)

    # KPI cards
    total_files = sum(int(s.get('total_file_count', 0)) for s in summary)
    total_rows = sum(int(s.get('total_file_rows', 0)) for s in summary)
    total_loaded = sum(int(s.get('total_loaded_rows', 0)) for s in summary)
    total_tables = len(summary)

    kpi_cards_html = ""
    for kpi in [
        {"icon": "📁", "value": f"{total_files:,}", "label": "Files Processed", "color": "#3B82F6"},
        {"icon": "📊", "value": f"{total_rows:,}", "label": "Total Rows", "color": "#8B5CF6"},
        {"icon": "✅", "value": f"{total_loaded:,}", "label": "Rows Loaded", "color": "#10B981"},
        {"icon": "🗃️", "value": f"{total_tables}", "label": "Tables", "color": "#F59E0B"},
    ]:
        kpi_cards_html += f'''
            <td style="width: 25%; padding: 10px; vertical-align: top;">
                <div style="background: white; border-radius: 16px; padding: 20px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); text-align: center; border-left: 4px solid {kpi['color']};">
                    <div style="font-size: 32px; margin-bottom: 8px;">{kpi['icon']}</div>
                    <div style="font-size: 24px; font-weight: 700; color: #1F2937; margin-bottom: 4px;">{kpi['value']}</div>
                    <div style="font-size: 12px; color: #6B7280; font-weight: 500;">{kpi['label']}</div>
                </div>
            </td>
        '''

    # Validation table rows
    validation_rows_html = ""
    for idx, table_data in enumerate(summary):
        bg_color = "#F9FAFB" if idx % 2 == 0 else "white"
        val_pass = int(table_data.get('validation_pass', 0))
        val_fail = int(table_data.get('validation_fail', 0))
        count_pass = int(table_data.get('count_match_pass', 0))
        count_fail = int(table_data.get('count_match_fail', 0))

        total_checks = val_pass + val_fail + count_pass + count_fail
        passed_checks = val_pass + count_pass

        status_emoji = "✅" if count_fail == 0 and val_fail == 0 else "❌"
        status_color = "#10B981" if count_fail == 0 and val_fail == 0 else "#EF4444"

        validation_rows_html += f'''
            <tr style="background: {bg_color};">
                <td style="padding: 12px 16px; border-bottom: 1px solid #E5E7EB;">
                    <span style="font-weight: 600; color: #1F2937;">{table_data['table_name']}</span>
                </td>
                <td style="padding: 12px 16px; border-bottom: 1px solid #E5E7EB; color: #4B5563; font-size: 13px;">
                    Count Match: {count_pass} pass, {count_fail} fail<br/>
                    Validations: {val_pass} pass, {val_fail} fail
                </td>
                <td style="padding: 12px 16px; text-align: center; border-bottom: 1px solid #E5E7EB;">
                    <span style="font-weight: 600; color: #1F2937;">{passed_checks}/{total_checks}</span>
                </td>
                <td style="padding: 12px 16px; text-align: center; border-bottom: 1px solid #E5E7EB;">
                    <span style="font-size: 20px;">{status_emoji}</span>
                    <div style="font-size: 11px; color: {status_color}; font-weight: 600; margin-top: 2px;">
                        {"PASS" if count_fail == 0 and val_fail == 0 else "FAIL"}
                    </div>
                </td>
            </tr>
        '''

    # Build HTML
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #F3F4F6;">
    <table width="100%" cellpadding="0" cellspacing="0" style="background: linear-gradient(135deg, #0F172A 0%, #1E293B 50%, #334155 100%);">
        <tr>
            <td style="padding: 40px 20px; text-align: center;">
                <div style="font-size: 48px; margin-bottom: 10px;">📊</div>
                <h1 style="color: #F8FAFC; margin: 0; font-size: 28px; font-weight: 700;">Data Quality Dashboard</h1>
                <p style="color: #94A3B8; margin: 10px 0 0 0; font-size: 14px;">ETL Health Check • {current_date_str} • {current_time_str}</p>
            </td>
        </tr>
    </table>
    <table width="100%" cellpadding="0" cellspacing="0" style="background: linear-gradient(180deg, #334155 0%, #F3F4F6 100%);">
        <tr>
            <td style="padding: 30px 20px;">
                <table width="600" align="center" cellpadding="0" cellspacing="0" style="background: white; border-radius: 24px; box-shadow: 0 25px 50px rgba(0,0,0,0.15);">
                    <tr>
                        <td style="padding: 40px; text-align: center;">
                            <div style="font-size: 64px; margin-bottom: 10px;">{health_emoji}</div>
                            <div style="font-size: 72px; font-weight: 800; color: {health_color}; line-height: 1;">{health_score:.0f}%</div>
                            <div style="font-size: 18px; color: #6B7280; margin-top: 8px; font-weight: 500;">Overall Data Health Score</div>
                            <div style="margin-top: 20px;">
                                <span style="display: inline-block; background: #10B981; color: white; padding: 8px 20px; border-radius: 20px; margin: 4px; font-weight: 600;">✅ {passed_count} Passed</span>
                                <span style="display: inline-block; background: #EF4444; color: white; padding: 8px 20px; border-radius: 20px; margin: 4px; font-weight: 600;">❌ {failed_count} Failed</span>
                            </div>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
    {"" if not donut_chart else f'''
    <table width="100%" cellpadding="0" cellspacing="0" style="background: #F3F4F6;">
        <tr>
            <td style="padding: 20px;">
                <table width="800" align="center" cellpadding="0" cellspacing="0">
                    <tr>
                        <td style="width: 35%; padding: 10px; vertical-align: top;">
                            <div style="background: white; border-radius: 20px; padding: 20px; box-shadow: 0 4px 20px rgba(0,0,0,0.08); text-align: center;">
                                <h3 style="color: #1F2937; margin: 0 0 15px 0; font-size: 16px;">Pass/Fail Distribution</h3>
                                <img src="data:image/png;base64,{donut_chart}" style="max-width: 200px;" />
                            </div>
                        </td>
                        <td style="width: 65%; padding: 10px; vertical-align: top;">
                            <div style="background: white; border-radius: 20px; padding: 20px; box-shadow: 0 4px 20px rgba(0,0,0,0.08);">
                                <h3 style="color: #1F2937; margin: 0 0 15px 0; font-size: 16px; text-align: center;">Issues by Category</h3>
                                {f'<img src="data:image/png;base64,{bar_chart}" style="max-width: 100%;" />' if bar_chart else '<p style="color: #10B981; text-align: center; font-size: 18px;">🎉 No issues found!</p>'}
                            </div>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
    '''}
    <table width="100%" cellpadding="0" cellspacing="0" style="background: #F3F4F6;">
        <tr>
            <td style="padding: 0 20px 20px 20px;">
                <table width="800" align="center" cellpadding="0" cellspacing="0">
                    <tr>
                        <td colspan="4" style="padding-bottom: 15px;"><h2 style="color: #1F2937; margin: 0; font-size: 20px;">📈 Key Metrics</h2></td>
                    </tr>
                    <tr>{kpi_cards_html}</tr>
                </table>
            </td>
        </tr>
    </table>
    <table width="100%" cellpadding="0" cellspacing="0" style="background: #F3F4F6;">
        <tr>
            <td style="padding: 0 20px 30px 20px;">
                <table width="800" align="center" cellpadding="0" cellspacing="0" style="background: white; border-radius: 20px; box-shadow: 0 4px 20px rgba(0,0,0,0.08); overflow: hidden;">
                    <tr>
                        <td style="padding: 25px 25px 15px 25px;">
                            <h2 style="color: #1F2937; margin: 0; font-size: 20px;">🔍 Detailed Validation Results</h2>
                            <p style="color: #6B7280; margin: 5px 0 0 0; font-size: 13px;">Summary for {current_date_str}</p>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 0 25px 25px 25px;">
                            <table width="100%" cellpadding="0" cellspacing="0" style="border-radius: 12px; overflow: hidden; border: 1px solid #E5E7EB;">
                                <tr style="background: linear-gradient(135deg, #1e1b4b 0%, #4c1d95 100%);">
                                    <th style="padding: 14px 16px; text-align: left; color: white; font-weight: 600; font-size: 12px;">Table Name</th>
                                    <th style="padding: 14px 16px; text-align: left; color: white; font-weight: 600; font-size: 12px;">Validation Details</th>
                                    <th style="padding: 14px 16px; text-align: center; color: white; font-weight: 600; font-size: 12px;">Passed</th>
                                    <th style="padding: 14px 16px; text-align: center; color: white; font-weight: 600; font-size: 12px;">Status</th>
                                </tr>
                                {validation_rows_html}
                            </table>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
    <table width="100%" cellpadding="0" cellspacing="0" style="background: linear-gradient(135deg, #1e1b4b 0%, #312e81 100%);">
        <tr>
            <td style="padding: 30px 20px; text-align: center;">
                <div style="font-size: 24px; margin-bottom: 10px;">
                    {"🏆 Stellar data quality today!" if health_score >= 90 else "💪 Good progress!" if health_score >= 70 else "🔧 Issues need attention" if health_score >= 50 else "🚨 Critical issues detected!"}
                </div>
                <p style="color: rgba(255,255,255,0.7); margin: 0; font-size: 12px;">Generated by ETL Health Check • Automated DQA Pipeline</p>
            </td>
        </tr>
    </table>
</body>
</html>
"""

    return html, health_score


def send_health_check_email(html, health_score, check_date, email_config):
    """Send health check email via SMTP"""
    try:
        sender = email_config['sender']
        recipients = email_config['recipients']
        smtp_host = email_config['smtp_host']
        smtp_port = email_config['smtp_port']

        msg = MIMEMultipart('alternative')
        msg['From'] = sender
        msg['To'] = ", ".join(recipients)
        msg.add_header('Content-Type', 'text/html')

        emoji = '🎉' if health_score >= 90 else '📊' if health_score >= 70 else '⚠️' if health_score >= 50 else '🚨'
        msg['Subject'] = f"{emoji} Data Quality Summary - {check_date.strftime('%Y-%m-%d')} | {health_score:.0f}% Health Score"

        part = MIMEText(html, 'html')
        msg.attach(part)

        server = smtplib.SMTP(smtp_host, smtp_port)
        if email_config.get('use_tls', False):
            server.starttls()

        if 'username' in email_config and email_config['username']:
            server.login(email_config['username'], email_config['password'])

        server.sendmail(sender, recipients, msg.as_string())
        server.quit()

        logger.info(f"✓ Health check email sent to {len(recipients)} recipient(s)")
        return True

    except Exception as e:
        logger.error(f"Error sending health check email: {str(e)}")
        return False


def run_health_check(health_check_config, s3_bucket):
    """Run daily health check if conditions are met"""
    logger.info("\n" + "=" * 80)
    logger.info("DAILY HEALTH CHECK")
    logger.info("=" * 80)

    try:
        # Check if already ran today
        if check_if_health_check_ran_today(s3_bucket):
            logger.info("⏭️  Health check already ran today - skipping")
            return

        check_date = datetime.now().date()
        logger.info(f"Running health check for {check_date.strftime('%Y-%m-%d')}")

        # Get audit summary
        summary = get_daily_audit_summary_for_health_check(health_check_config, check_date)

        if not summary:
            logger.info("No audit data found for today - skipping health check")
            return

        logger.info(f"✓ Found audit data for {len(summary)} table(s)")

        # Check health
        is_healthy, issues = check_health_status(summary)

        if is_healthy:
            logger.info("✅ HEALTH CHECK PASSED")
        else:
            logger.warning(f"⚠️  HEALTH CHECK: {len(issues)} issue(s) detected")
            for issue in issues:
                logger.warning(f"  • {issue['table']} [{issue['type']}]: {issue['message']}")

        # Generate and send email
        html, health_score = generate_html_email(summary, issues, check_date, health_check_config)
        send_health_check_email(html, health_score, check_date, health_check_config['email'])

        # Create marker so we don't run again today
        create_health_check_marker(s3_bucket)

        logger.info(f"✓ Health check completed (Score: {health_score:.0f}%)")

    except Exception as e:
        logger.error(f"Error running health check: {str(e)}")
        # Don't fail the main job if health check fails
        import traceback
        traceback.print_exc()


################################################################################
# MAIN ETL EXECUTION
################################################################################

def main():
    """
    Main ETL execution flow
    """
    logger.info("=" * 80)
    logger.info("CONFIG-DRIVEN GLUE ETL JOB - ENHANCED VERSION")
    logger.info("=" * 80)

    try:
        # Read config file
        logger.info(f"\nReading config from: s3://{bucket_name}/{json_file_name}")
        response = s3_client.get_object(Bucket=bucket_name, Key=json_file_name)
        file_content = response['Body'].read().decode('utf-8')
        config = json.loads(file_content)

        # Extract jobs array from config
        jobs = config.get('jobs', [])
        global_settings = config.get('global_settings', {})

        logger.info(f"✓ Config loaded successfully")
        logger.info(f"  Jobs in config: {len(jobs)}")
        logger.info(f"  Global settings: {list(global_settings.keys())}")

        if not jobs:
            error_msg = "No jobs found in configuration file"
            logger.error(f"❌ {error_msg}")
            raise Exception(error_msg)

        # Process each job
        for job_config in jobs:
            logger.info("\n" + "=" * 80)
            logger.info(f"PROCESSING JOB: {job_config.get('job_name', 'Unnamed')}")
            logger.info("=" * 80)

            # Validate required fields
            required_fields = ['s3_bucket', 'landing_loc', 'database_name', 'landing_table', 'archive_s3_path']
            missing_fields = [field for field in required_fields if field not in job_config]

            if missing_fields:
                error_msg = f"Missing required fields in job config: {', '.join(missing_fields)}"
                logger.error(f"❌ {error_msg}")
                logger.error(f"Job config keys found: {list(job_config.keys())}")
                raise Exception(error_msg)

            # Extract config
            s3_bucket = job_config['s3_bucket']
            landing_loc = job_config['landing_loc']
            database_name = job_config['database_name']
            table_name = job_config['landing_table']
            archive_path = job_config['archive_s3_path']
            validation_queries = job_config.get('validation_queries', [])

            logger.info(f"Database: {database_name}")
            logger.info(f"Table: {table_name}")
            logger.info(f"Landing location: s3://{s3_bucket}/{landing_loc}")
            logger.info(f"Archive path: s3://{s3_bucket}/{archive_path}")

            # STEP 1: Check for FAIL file
            logger.info("\n[STEP 1] Checking for FAIL file...")
            check_fail_file_exists(s3_bucket, table_name)

            # STEP 2: Get folders older than 59 minutes
            logger.info("\n[STEP 2] Finding eligible folders (> 59 minutes old)...")
            eligible_folders = get_folders_older_than_59_minutes(s3_bucket, landing_loc)

            if not eligible_folders:
                error_msg = f"No folders older than 59 minutes found in s3://{s3_bucket}/{landing_loc}"
                logger.error(f"❌ {error_msg}")
                create_fail_file(s3_bucket, table_name, error_msg)
                raise Exception(error_msg)

            # STEP 3: Get CSV files from eligible folders
            logger.info("\n[STEP 3] Collecting CSV files from eligible folders...")
            files_by_folder = get_csv_files_from_folders(s3_bucket, eligible_folders)

            all_csv_files = []
            for folder, files in files_by_folder.items():
                all_csv_files.extend(files)

            if not all_csv_files:
                error_msg = f"No CSV files found in eligible folders"
                logger.error(f"❌ {error_msg}")
                create_fail_file(s3_bucket, table_name, error_msg)
                raise Exception(error_msg)

            # STEP 4: Count rows in source CSV files
            logger.info("\n[STEP 4] Counting rows in source CSV files...")
            file_row_count = count_rows_in_csv_files(s3_bucket, files_by_folder)

            # STEP 5: Get schema from target table
            logger.info("\n[STEP 5] Fetching schema from target table...")
            schema = get_table_schema(database_name, table_name)

            # STEP 6: Load CSV files
            logger.info("\n[STEP 6] Loading CSV files into DataFrame...")
            df = spark.read.option("header", "true").schema(schema).csv(all_csv_files)

            # STEP 7: Add audit columns
            logger.info("\n[STEP 7] Adding audit columns...")
            df = df.withColumn('hourly_processed_ind', lit('N'))
            df = df.withColumn('daily_processed_ind', lit('N'))

            pst = pytz.timezone('US/Pacific')
            current_timestamp = datetime.now(pst).strftime('%Y-%m-%d %H:%M:%S')
            current_timestamp_utc = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

            df = df.withColumn('insert_timestamp', lit(current_timestamp))
            df = df.withColumn('inserted_by', lit('Config-Driven Glue Job'))
            df = df.withColumn('updated_timestamp', lit(current_timestamp))
            df = df.withColumn('updated_by', lit('Config-Driven Glue Job'))
            df = df.withColumn('load_timestamp', lit(current_timestamp_utc))

            df.cache()
            df.createOrReplaceTempView("staging_temp")

            loaded_count = df.count()
            logger.info(f"✓ DataFrame created with {loaded_count} rows")

            # STEP 8: Validate row counts
            logger.info("\n[STEP 8] Validating row counts...")
            logger.info(f"  File row count: {file_row_count}")
            logger.info(f"  Loaded row count: {loaded_count}")

            count_match = (file_row_count == loaded_count)

            if not count_match:
                error_msg = f"Row count mismatch: Files={file_row_count}, Loaded={loaded_count}"
                logger.error(f"❌ {error_msg}")
                create_fail_file(s3_bucket, table_name, error_msg)
                raise Exception(error_msg)
            else:
                logger.info("✓ Row counts match")

            # STEP 9: Run validation queries
            logger.info("\n[STEP 9] Running validation queries...")
            all_validations_passed, failed_validations = run_validation_queries(
                validation_queries,
                database_name,
                table_name
            )

            if not all_validations_passed:
                error_msg = "Validation queries failed:\n"
                for failed in failed_validations:
                    error_msg += f"\n  - {failed['name']}: "
                    if 'error' in failed:
                        error_msg += f"Error: {failed['error']}"
                    else:
                        error_msg += f"Count {failed['actual_count']} > {failed['expected_max']}"

                logger.error(f"❌ {error_msg}")
                create_fail_file(s3_bucket, table_name, error_msg)
                raise Exception("Validation failures detected")

            # STEP 10: Insert into target table
            logger.info("\n[STEP 10] Inserting data into target table...")
            insert_query = f"""
                INSERT INTO glue_catalog.{database_name}.{table_name}
                SELECT * FROM staging_temp
            """
            logger.info(f"Query: {insert_query}")
            spark.sql(insert_query)
            logger.info(f"✓ Inserted {loaded_count} rows into {database_name}.{table_name}")

            # STEP 11: Write audit record
            logger.info("\n[STEP 11] Writing audit record...")
            job_end_time = datetime.now()
            duration = (job_end_time - job_start_time).total_seconds()

            audit_data = {
                'table_name': f"{database_name}.{table_name}",
                'folders': eligible_folders,
                'file_count': len(all_csv_files),
                'file_row_count': file_row_count,
                'loaded_count': loaded_count,
                'validation_status': 'PASS',
                'validation_details': f"All {len(validation_queries)} validations passed",
                'load_timestamp': current_timestamp_utc,
                'duration': f"{duration:.2f} seconds",
                'status': 'SUCCESS'
            }

            write_audit_record(s3_bucket, table_name, audit_data, database_name)

            # STEP 12: Move files to archive
            logger.info("\n[STEP 12] Moving files to archive...")
            move_folders_to_archive(s3_bucket, eligible_folders, archive_path)

            # STEP 13: Delete FAIL file if exists
            logger.info("\n[STEP 13] Cleaning up FAIL file...")
            delete_fail_file(s3_bucket, table_name)

            logger.info("\n" + "=" * 80)
            logger.info(f"✓ JOB COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            logger.info(f"  Files processed: {len(all_csv_files)}")
            logger.info(f"  Rows loaded: {loaded_count}")
            logger.info(f"  Validations passed: {len(validation_queries)}")
            logger.info(f"  Duration: {duration:.2f} seconds")

        # After all jobs complete, check if we should run health check
        health_check_config = global_settings.get('health_check')
        if health_check_config and should_run_health_check(health_check_config):
            logger.info("\n" + "=" * 80)
            logger.info("Conditions met for daily health check")
            run_health_check(health_check_config, bucket_name)

    except Exception as e:
        logger.error(f"\n{'='*80}")
        logger.error(f"❌ JOB FAILED")
        logger.error(f"{'='*80}")
        logger.error(f"Error: {str(e)}")
        logger.error(f"{'='*80}")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()
