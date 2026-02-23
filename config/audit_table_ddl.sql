-- =============================================================================
-- DDL: etl_orchestrator_audit
-- Description: Audit table for Strands ETL Orchestrator.
--              Stores stale-check events, Glue trigger events, and
--              validation query results per orchestrator run.
--
-- Format     : Pipe-delimited CSV files on S3
-- Partitioned: run_date (string, YYYY-MM-DD)
-- Repair     : MSCK REPAIR TABLE audit_db.etl_orchestrator_audit;
-- =============================================================================

CREATE DATABASE IF NOT EXISTS audit_db
  COMMENT 'Strands ETL Audit Database'
  LOCATION 's3://strands-etl-audit/';

CREATE EXTERNAL TABLE IF NOT EXISTS audit_db.etl_orchestrator_audit (
    run_id                STRING    COMMENT 'Unique orchestrator run identifier',
    event_timestamp       STRING    COMMENT 'ISO-8601 UTC timestamp of the event',
    event_type            STRING    COMMENT 'STALE_CHECK | GLUE_TRIGGER | VALIDATION',
    source_label          STRING    COMMENT 'Friendly name of the monitored S3 source',
    source_bucket         STRING    COMMENT 'S3 bucket of the monitored source',
    source_prefix         STRING    COMMENT 'S3 prefix of the monitored source',
    subfolder_count       STRING    COMMENT 'Number of subfolders found under the prefix',
    status                STRING    COMMENT 'OK | Stale | Check with Source | PASS | FAIL | SKIPPED | ABORTED',
    detail                STRING    COMMENT 'Human-readable detail message',
    glue_job_name         STRING    COMMENT 'Name of the triggered Glue job (if applicable)',
    glue_job_run_id       STRING    COMMENT 'Glue job run ID (if applicable)',
    glue_duration_sec     STRING    COMMENT 'Glue job duration in seconds (if applicable)',
    validation_name       STRING    COMMENT 'Validation query name (if applicable)',
    check_type            STRING    COMMENT 'Validation check type: row_count | no_rows | threshold',
    actual_value          STRING    COMMENT 'Actual value returned by the validation query',
    query                 STRING    COMMENT 'The Athena SQL query executed (if applicable)',
    overall_status        STRING    COMMENT 'Overall orchestrator run status at time of logging'
)
PARTITIONED BY (
    run_date              STRING    COMMENT 'Partition key: YYYY-MM-DD date of the run'
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    ESCAPED BY '\\'
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://strands-etl-audit/audit_logs/orchestrator/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'classification'         = 'csv',
    'has_encrypted_data'     = 'false'
);

-- After each orchestrator run writes a new partition, run:
-- MSCK REPAIR TABLE audit_db.etl_orchestrator_audit;

-- ============================================================================
-- Useful queries
-- ============================================================================

-- Latest run summary
-- SELECT run_id, event_type, status, detail
-- FROM   audit_db.etl_orchestrator_audit
-- WHERE  run_date = cast(current_date AS varchar)
-- ORDER  BY event_timestamp;

-- Failure history (last 7 days)
-- SELECT run_date, run_id, event_type, source_label, validation_name, status, detail
-- FROM   audit_db.etl_orchestrator_audit
-- WHERE  run_date >= cast(date_add('day', -7, current_date) AS varchar)
--   AND  status IN ('FAIL', 'ABORTED', 'Stale', 'Check with Source')
-- ORDER  BY run_date DESC, event_timestamp;
