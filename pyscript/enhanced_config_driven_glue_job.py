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
                row_count = max(0, line_count - 1)  # Exclude header

                folder_rows += row_count
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


def write_audit_record(bucket, table_name, audit_data):
    """
    Write audit record to S3.
    Format: Table | Folders | Files | File Count | Loaded Count | Validations | Timestamp
    """
    audit_key = f"audit/{table_name}/audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

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
        logger.info(f"✓ Audit record written: s3://{bucket}/{audit_key}")
    except Exception as e:
        logger.error(f"Error writing audit record: {str(e)}")


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

            write_audit_record(s3_bucket, table_name, audit_data)

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
