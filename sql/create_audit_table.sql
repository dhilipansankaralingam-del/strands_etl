-- Create Audit Table for ETL Job Monitoring
-- This table can be queried to track all ETL job executions

-- Create external table on top of pipe-delimited audit files
CREATE EXTERNAL TABLE IF NOT EXISTS etl_demo_db.etl_audit_log (
    table_name STRING COMMENT 'Staging table name',
    job_name STRING COMMENT 'Glue job name',
    job_run_id STRING COMMENT 'Glue job run ID',
    folders STRING COMMENT 'Comma-separated list of folders processed',
    file_count INT COMMENT 'Number of CSV files loaded',
    file_row_count BIGINT COMMENT 'Total rows in source CSV files',
    loaded_count BIGINT COMMENT 'Total rows loaded into table',
    count_match STRING COMMENT 'PASS if counts match, FAIL otherwise',
    validation_status STRING COMMENT 'Overall validation status (PASS/FAIL)',
    validation_details STRING COMMENT 'Details of validation results',
    load_timestamp STRING COMMENT 'When the load completed',
    duration STRING COMMENT 'Job duration in seconds',
    status STRING COMMENT 'Job status (SUCCESS/FAIL)'
)
PARTITIONED BY (
    year STRING,
    month STRING,
    day STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://your-bucket-name/audit_table/'
TBLPROPERTIES (
    'skip.header.line.count'='1',
    'classification'='csv'
);

-- After creating the table, run MSCK REPAIR to discover partitions
-- MSCK REPAIR TABLE etl_demo_db.etl_audit_log;

-- Query examples:

-- 1. View all audit records
-- SELECT * FROM etl_demo_db.etl_audit_log
-- ORDER BY load_timestamp DESC
-- LIMIT 100;

-- 2. Check for failed jobs
-- SELECT table_name, job_name, status, load_timestamp, duration
-- FROM etl_demo_db.etl_audit_log
-- WHERE status = 'FAIL'
-- ORDER BY load_timestamp DESC;

-- 3. Check for validation failures
-- SELECT table_name, validation_status, validation_details, load_timestamp
-- FROM etl_demo_db.etl_audit_log
-- WHERE validation_status = 'FAIL'
-- ORDER BY load_timestamp DESC;

-- 4. Check for count mismatches
-- SELECT table_name, file_row_count, loaded_count, count_match, load_timestamp
-- FROM etl_demo_db.etl_audit_log
-- WHERE count_match = 'FAIL'
-- ORDER BY load_timestamp DESC;

-- 5. Job performance by table
-- SELECT
--     table_name,
--     COUNT(*) as run_count,
--     AVG(CAST(REGEXP_REPLACE(duration, ' seconds', '') AS DOUBLE)) as avg_duration_seconds,
--     SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
--     SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as fail_count
-- FROM etl_demo_db.etl_audit_log
-- WHERE year = '2026' AND month = '01'
-- GROUP BY table_name
-- ORDER BY run_count DESC;

-- 6. Daily job statistics
-- SELECT
--     year,
--     month,
--     day,
--     COUNT(*) as total_jobs,
--     SUM(file_count) as total_files_processed,
--     SUM(loaded_count) as total_rows_loaded,
--     SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_jobs,
--     SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed_jobs
-- FROM etl_demo_db.etl_audit_log
-- GROUP BY year, month, day
-- ORDER BY year DESC, month DESC, day DESC
-- LIMIT 30;

-- 7. Find jobs with longest duration
-- SELECT
--     table_name,
--     job_name,
--     job_run_id,
--     file_count,
--     loaded_count,
--     CAST(REGEXP_REPLACE(duration, ' seconds', '') AS DOUBLE) as duration_seconds,
--     load_timestamp
-- FROM etl_demo_db.etl_audit_log
-- WHERE status = 'SUCCESS'
-- ORDER BY duration_seconds DESC
-- LIMIT 20;

-- 8. Check data quality trends
-- SELECT
--     table_name,
--     DATE(load_timestamp) as load_date,
--     COUNT(*) as run_count,
--     SUM(CASE WHEN count_match = 'PASS' THEN 1 ELSE 0 END) as count_match_pass,
--     SUM(CASE WHEN validation_status = 'PASS' THEN 1 ELSE 0 END) as validation_pass
-- FROM etl_demo_db.etl_audit_log
-- WHERE year = '2026' AND month = '02'
-- GROUP BY table_name, DATE(load_timestamp)
-- ORDER BY load_date DESC, table_name;
