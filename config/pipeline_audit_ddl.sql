-- =============================================================================
-- DDL: etl_pipeline_audit
-- Description: Audit table for Strands ETL Pipeline.
--              Stores step execution events, validation results, rollback
--              actions, and cost tracking for each pipeline run.
--
-- Format     : Pipe-delimited CSV files on S3
-- Partitioned: run_date (string, YYYY-MM-DD)
-- Repair     : MSCK REPAIR TABLE audit_db.etl_pipeline_audit;
-- =============================================================================

CREATE DATABASE IF NOT EXISTS audit_db
  COMMENT 'Strands ETL Audit Database'
  LOCATION 's3://strands-etl-audit/';

CREATE EXTERNAL TABLE IF NOT EXISTS audit_db.etl_pipeline_audit (
    run_id                  STRING    COMMENT 'Unique pipeline run identifier',
    event_timestamp         STRING    COMMENT 'ISO-8601 UTC timestamp of the event',
    pipeline_name           STRING    COMMENT 'Name of the pipeline (from config)',
    layer                   STRING    COMMENT 'Pipeline layer: STAGING | DATALAKE | BASE | MASTER | SUMMARY',
    step_name               STRING    COMMENT 'Name of the step being executed',
    step_type               STRING    COMMENT 'Step type: sql | ctas | insert | merge | drop | validation | optimize | snapshot',
    step_sequence           STRING    COMMENT 'Step execution order (1-based)',
    description             STRING    COMMENT 'Human-readable step description',
    database_name           STRING    COMMENT 'Athena database context for the step',
    sql_hash                STRING    COMMENT 'MD5 hash of the SQL statement (for tracking)',
    status                  STRING    COMMENT 'PASS | FAIL | SKIPPED | ABORTED | WARN | ROLLBACK',
    detail                  STRING    COMMENT 'Human-readable detail / result message',
    attempt_number          STRING    COMMENT 'Attempt number for this step (1, 2, or 3)',
    max_attempts            STRING    COMMENT 'Maximum attempts configured',
    query_execution_id      STRING    COMMENT 'Athena QueryExecutionId',
    data_scanned_bytes      STRING    COMMENT 'Bytes scanned by the query',
    exec_time_ms            STRING    COMMENT 'Athena engine execution time in milliseconds',
    cost_usd                STRING    COMMENT 'Query cost in USD ($5/TB scanned)',
    target_table            STRING    COMMENT 'Target table for write operations (if applicable)',
    snapshot_id_before      STRING    COMMENT 'Iceberg snapshot ID captured before the step',
    snapshot_id_after       STRING    COMMENT 'Iceberg snapshot ID after the step',
    rollback_executed       STRING    COMMENT 'Whether rollback was executed (true/false)',
    rollback_status         STRING    COMMENT 'Rollback result: PASS | FAIL | N/A',
    overall_status          STRING    COMMENT 'Overall pipeline run status at time of logging',
    failure_reason          STRING    COMMENT 'Error message on failure',
    rows_affected           STRING    COMMENT 'Number of rows affected (if available)',
    duration_sec            STRING    COMMENT 'Wall-clock duration of this step in seconds'
)
PARTITIONED BY (
    run_date                STRING    COMMENT 'Partition key: YYYY-MM-DD date of the run'
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    ESCAPED BY '\\'
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 's3://strands-etl-audit/audit_logs/pipeline/'
TBLPROPERTIES (
    'skip.header.line.count' = '1',
    'classification'         = 'csv',
    'has_encrypted_data'     = 'false'
);

-- After each pipeline run writes a new partition, run:
-- MSCK REPAIR TABLE audit_db.etl_pipeline_audit;

-- ============================================================================
-- Useful queries
-- ============================================================================

-- Latest pipeline run summary
-- SELECT run_id, pipeline_name, step_name, step_type, status, detail,
--        cost_usd, duration_sec
-- FROM   audit_db.etl_pipeline_audit
-- WHERE  run_date = cast(current_date AS varchar)
-- ORDER  BY event_timestamp;

-- Failed steps (last 7 days)
-- SELECT run_date, run_id, pipeline_name, layer, step_name, status,
--        failure_reason, rollback_executed, rollback_status
-- FROM   audit_db.etl_pipeline_audit
-- WHERE  run_date >= cast(date_add('day', -7, current_date) AS varchar)
--   AND  status IN ('FAIL', 'ABORTED')
-- ORDER  BY run_date DESC, event_timestamp;

-- Pipeline cost history
-- SELECT run_date, pipeline_name,
--        SUM(CAST(COALESCE(NULLIF(cost_usd, ''), '0') AS DOUBLE)) AS total_cost,
--        COUNT(DISTINCT run_id) AS runs,
--        COUNT(*) AS total_steps
-- FROM   audit_db.etl_pipeline_audit
-- WHERE  run_date >= cast(date_add('day', -30, current_date) AS varchar)
-- GROUP  BY run_date, pipeline_name
-- ORDER  BY run_date DESC;

-- Rollback history
-- SELECT run_date, run_id, pipeline_name, step_name, target_table,
--        snapshot_id_before, rollback_status, failure_reason
-- FROM   audit_db.etl_pipeline_audit
-- WHERE  rollback_executed = 'true'
--   AND  run_date >= cast(date_add('day', -30, current_date) AS varchar)
-- ORDER  BY run_date DESC, event_timestamp;

-- Step duration trends
-- SELECT pipeline_name, step_name,
--        AVG(CAST(COALESCE(NULLIF(duration_sec, ''), '0') AS DOUBLE)) AS avg_duration,
--        MAX(CAST(COALESCE(NULLIF(duration_sec, ''), '0') AS DOUBLE)) AS max_duration,
--        COUNT(*) AS executions
-- FROM   audit_db.etl_pipeline_audit
-- WHERE  run_date >= cast(date_add('day', -30, current_date) AS varchar)
--   AND  status = 'PASS'
-- GROUP  BY pipeline_name, step_name
-- ORDER  BY pipeline_name, avg_duration DESC;
