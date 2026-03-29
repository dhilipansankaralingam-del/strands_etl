-- ============================================================================
-- ETL Audit Log Table - Parquet Format
-- ============================================================================
-- This table stores audit records for all ETL job executions in Parquet format
-- for efficient querying and analysis.
--
-- Partitioning: year/month/day for query performance
-- Format: Parquet (compressed, columnar storage)
-- Auto-discovery: Use MSCK REPAIR TABLE to discover new partitions
--
-- Usage:
--   1. Create table (run once)
--   2. ETL job writes Parquet files to partitioned location
--   3. MSCK REPAIR TABLE discovers new partitions automatically
--   4. Query with Athena/Spark SQL
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS aew_membership.etl_audit_log (
    table_name STRING COMMENT 'Fully qualified table name (database.table)',
    job_name STRING COMMENT 'Glue job name that processed the data',
    job_run_id STRING COMMENT 'Unique Glue job run identifier',
    folders STRING COMMENT 'Comma-separated list of S3 folders processed',
    file_count INT COMMENT 'Number of files processed',
    file_row_count BIGINT COMMENT 'Total rows in source files (excluding headers)',
    loaded_count BIGINT COMMENT 'Total rows loaded to staging table',
    count_match STRING COMMENT 'PASS if file_row_count = loaded_count, else FAIL',
    validation_status STRING COMMENT 'Overall validation status (PASS/FAIL)',
    validation_details STRING COMMENT 'Detailed validation results',
    load_timestamp STRING COMMENT 'Timestamp when data was loaded (UTC)',
    duration_seconds DOUBLE COMMENT 'Job execution duration in seconds',
    status STRING COMMENT 'Job execution status (SUCCESS/FAIL)'
)
COMMENT 'ETL audit log tracking all data loads and validations'
PARTITIONED BY (
    year STRING COMMENT 'Year of execution (YYYY)',
    month STRING COMMENT 'Month of execution (MM)',
    day STRING COMMENT 'Day of execution (DD)'
)
STORED AS PARQUET
LOCATION 's3://staging/audit_table/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'classification'='parquet',
    'has_encrypted_data'='false',
    'projection.enabled'='true',
    'projection.year.type'='integer',
    'projection.year.range'='2024,2030',
    'projection.year.digits'='4',
    'projection.month.type'='integer',
    'projection.month.range'='1,12',
    'projection.month.digits'='2',
    'projection.day.type'='integer',
    'projection.day.range'='1,31',
    'projection.day.digits'='2',
    'storage.location.template'='s3://staging/audit_table/year=${year}/month=${month}/day=${day}'
);

-- ============================================================================
-- Partition Discovery
-- ============================================================================
-- Run this after ETL jobs complete to discover new partitions
-- (The ETL job will run this automatically after each execution)

MSCK REPAIR TABLE aew_membership.etl_audit_log;

-- ============================================================================
-- Useful Queries
-- ============================================================================

-- 1. View today's audit records
SELECT
    table_name,
    job_run_id,
    file_count,
    file_row_count,
    loaded_count,
    count_match,
    validation_status,
    status,
    load_timestamp
FROM aew_membership.etl_audit_log
WHERE year = CAST(year(CURRENT_DATE) AS VARCHAR)
  AND month = LPAD(CAST(month(CURRENT_DATE) AS VARCHAR), 2, '0')
  AND day = LPAD(CAST(day(CURRENT_DATE) AS VARCHAR), 2, '0')
ORDER BY load_timestamp DESC;

-- 2. View failed jobs in last 7 days
SELECT
    year,
    month,
    day,
    table_name,
    status,
    count_match,
    validation_status,
    validation_details,
    load_timestamp
FROM aew_membership.etl_audit_log
WHERE status = 'FAIL'
  AND CAST(CONCAT(year, '-', month, '-', day) AS DATE) >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY year DESC, month DESC, day DESC, load_timestamp DESC;

-- 3. Count match failures in last 30 days
SELECT
    table_name,
    COUNT(*) as failure_count,
    SUM(file_row_count) as total_file_rows,
    SUM(loaded_count) as total_loaded_rows,
    SUM(file_row_count) - SUM(loaded_count) as row_difference
FROM aew_membership.etl_audit_log
WHERE count_match = 'FAIL'
  AND CAST(CONCAT(year, '-', month, '-', day) AS DATE) >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY table_name
ORDER BY failure_count DESC;

-- 4. Daily load volume trend (last 30 days)
SELECT
    CONCAT(year, '-', month, '-', day) as load_date,
    table_name,
    COUNT(*) as run_count,
    SUM(file_count) as total_files,
    SUM(loaded_count) as total_rows_loaded,
    AVG(duration_seconds) as avg_duration_seconds
FROM aew_membership.etl_audit_log
WHERE CAST(CONCAT(year, '-', month, '-', day) AS DATE) >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY year, month, day, table_name
ORDER BY year DESC, month DESC, day DESC, table_name;

-- 5. Validation failure analysis
SELECT
    table_name,
    validation_status,
    COUNT(*) as occurrence_count,
    validation_details
FROM aew_membership.etl_audit_log
WHERE validation_status = 'FAIL'
  AND CAST(CONCAT(year, '-', month, '-', day) AS DATE) >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY table_name, validation_status, validation_details
ORDER BY occurrence_count DESC;

-- 6. Job performance metrics
SELECT
    table_name,
    COUNT(*) as total_runs,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as successful_runs,
    COUNT(CASE WHEN status = 'FAIL' THEN 1 END) as failed_runs,
    ROUND(COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*), 2) as success_rate_pct,
    AVG(duration_seconds) as avg_duration_seconds,
    MAX(duration_seconds) as max_duration_seconds,
    AVG(loaded_count) as avg_rows_loaded,
    MAX(loaded_count) as max_rows_loaded
FROM aew_membership.etl_audit_log
WHERE CAST(CONCAT(year, '-', month, '-', day) AS DATE) >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY table_name
ORDER BY total_runs DESC;

-- 7. Hourly load pattern analysis
SELECT
    SUBSTRING(load_timestamp, 12, 2) as load_hour,
    table_name,
    COUNT(*) as run_count,
    AVG(loaded_count) as avg_rows_loaded,
    AVG(duration_seconds) as avg_duration_seconds
FROM aew_membership.etl_audit_log
WHERE CAST(CONCAT(year, '-', month, '-', day) AS DATE) >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY SUBSTRING(load_timestamp, 12, 2), table_name
ORDER BY load_hour, table_name;

-- 8. Most recent execution per table
SELECT
    table_name,
    job_run_id,
    file_count,
    file_row_count,
    loaded_count,
    count_match,
    validation_status,
    status,
    load_timestamp,
    duration_seconds
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY load_timestamp DESC) as rn
    FROM aew_membership.etl_audit_log
) ranked
WHERE rn = 1
ORDER BY table_name;

-- 9. Data quality health score (last 24 hours)
SELECT
    ROUND(
        COUNT(CASE WHEN count_match = 'PASS' AND validation_status = 'PASS' AND status = 'SUCCESS' THEN 1 END) * 100.0 /
        COUNT(*),
        2
    ) as health_score_pct,
    COUNT(*) as total_runs,
    COUNT(CASE WHEN count_match = 'PASS' AND validation_status = 'PASS' AND status = 'SUCCESS' THEN 1 END) as healthy_runs,
    COUNT(CASE WHEN count_match = 'FAIL' OR validation_status = 'FAIL' OR status = 'FAIL' THEN 1 END) as unhealthy_runs
FROM aew_membership.etl_audit_log
WHERE CAST(CONCAT(year, '-', month, '-', day, ' ', SUBSTRING(load_timestamp, 12, 8)) AS TIMESTAMP)
      >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR;

-- 10. Partition summary
SELECT
    year,
    month,
    day,
    COUNT(*) as record_count,
    COUNT(DISTINCT table_name) as unique_tables,
    SUM(loaded_count) as total_rows_loaded,
    SUM(duration_seconds) as total_duration_seconds
FROM aew_membership.etl_audit_log
GROUP BY year, month, day
ORDER BY year DESC, month DESC, day DESC
LIMIT 30;

-- ============================================================================
-- Maintenance Queries
-- ============================================================================

-- Drop old partitions (keep last 90 days)
-- Note: Run this periodically to manage storage costs
ALTER TABLE aew_membership.etl_audit_log
DROP IF EXISTS PARTITION (year='2024', month='01', day='01');

-- Optimize table (compaction)
-- Run this in Spark/Glue to compact small files
-- OPTIMIZE aew_membership.etl_audit_log;

-- Vacuum old files (if using Iceberg or Delta Lake)
-- VACUUM aew_membership.etl_audit_log RETAIN 168 HOURS;

-- ============================================================================
-- Table Statistics (for query optimization)
-- ============================================================================

ANALYZE TABLE aew_membership.etl_audit_log COMPUTE STATISTICS;
ANALYZE TABLE aew_membership.etl_audit_log COMPUTE STATISTICS FOR ALL COLUMNS;
