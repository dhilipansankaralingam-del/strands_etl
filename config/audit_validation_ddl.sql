-- =============================================================================
-- DDL: audit_db.audit_validation
-- Description: Iceberg table that stores ETL validation rule results.
--              The five AI columns (ai_*) are filled by the Strands
--              validation_agent after each run.
--
-- Format     : Apache Iceberg (supports UPDATE / MERGE DML)
-- Engine     : Athena v3 + AWS Glue Catalog
-- Location   : s3://strands-etl-audit/audit_validation/
--
-- Usage
-- -----
-- 1. Run this DDL once to create the table.
-- 2. The ETL validation framework INSERTs rows with ai_* columns as NULL.
-- 3. The validation_agent UPDATEs the ai_* columns via write_ai_enrichment.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS audit_db
    COMMENT 'Strands ETL Audit Database'
    LOCATION 's3://strands-etl-audit/';

CREATE TABLE IF NOT EXISTS audit_db.audit_validation (

    -- -----------------------------------------------------------------------
    -- Identity
    -- -----------------------------------------------------------------------
    record_id               VARCHAR     COMMENT 'Unique validation record identifier (UUID)',
    run_id                  VARCHAR     COMMENT 'Orchestrator run that produced this record',
    pipeline_name           VARCHAR     COMMENT 'ETL pipeline name',
    database_name           VARCHAR     COMMENT 'Source database (Glue Catalog)',

    -- -----------------------------------------------------------------------
    -- Rule definition
    -- -----------------------------------------------------------------------
    rule_name               VARCHAR     COMMENT 'Validation rule identifier',
    table_name              VARCHAR     COMMENT 'Target table that was validated',
    column_name             VARCHAR     COMMENT 'Column under test (NULL for table-level rules)',
    rule_type               VARCHAR     COMMENT 'NOT_NULL | RANGE | REGEX | REFERENTIAL | BUSINESS',
    expected_constraint     VARCHAR     COMMENT 'Human-readable constraint description',

    -- -----------------------------------------------------------------------
    -- Result
    -- -----------------------------------------------------------------------
    status                  VARCHAR     COMMENT 'PASS | FAIL | WARN',
    severity                VARCHAR     COMMENT 'LOW | MEDIUM | HIGH | CRITICAL',
    failed_value            VARCHAR     COMMENT 'Sample value that triggered the failure',
    row_count_failed        BIGINT      COMMENT 'Number of rows that violated the rule',
    total_row_count         BIGINT      COMMENT 'Total rows scanned by the rule',
    failure_timestamp       TIMESTAMP(6) COMMENT 'UTC timestamp when the failure was recorded',

    -- -----------------------------------------------------------------------
    -- AI enrichment columns — written by validation_agent
    -- -----------------------------------------------------------------------
    ai_classification       VARCHAR     COMMENT 'TRUE_FAILURE | FALSE_POSITIVE | NEEDS_INVESTIGATION',
    ai_confidence           DOUBLE      COMMENT 'Classification confidence 0.0 (low) – 1.0 (high)',
    ai_recommended_action   VARCHAR     COMMENT 'IGNORE | RERUN | FIX_LOGIC | DATA_CORRECTION | ESCALATE | MONITOR',
    ai_explanation          VARCHAR     COMMENT 'Agent reasoning for the classification (1–3 sentences)',
    ai_analysed_at          TIMESTAMP(6) COMMENT 'UTC timestamp when the agent classified this record'

)
LOCATION 's3://strands-etl-audit/audit_validation/'
TBLPROPERTIES (
    'table_type'       = 'ICEBERG',
    'format'           = 'parquet',
    'write_compression'= 'snappy'
);

-- =============================================================================
-- Useful queries
-- =============================================================================

-- Unassessed failures from the last 24 hours (agent input query)
-- SELECT *
-- FROM   audit_db.audit_validation
-- WHERE  status = 'FAIL'
--   AND  ai_classification IS NULL
--   AND  failure_timestamp >= NOW() - INTERVAL '24' HOUR
-- ORDER  BY severity DESC
-- LIMIT  500;

-- Classification breakdown for today
-- SELECT ai_classification,
--        ai_recommended_action,
--        COUNT(*)                          AS record_count,
--        ROUND(AVG(ai_confidence), 3)      AS avg_confidence
-- FROM   audit_db.audit_validation
-- WHERE  ai_analysed_at >= CAST(CURRENT_DATE AS TIMESTAMP)
-- GROUP  BY ai_classification, ai_recommended_action
-- ORDER  BY record_count DESC;

-- CRITICAL TRUE_FAILURE records still open (action = ESCALATE)
-- SELECT record_id, pipeline_name, rule_name, table_name, column_name,
--        ai_confidence, ai_explanation, failure_timestamp
-- FROM   audit_db.audit_validation
-- WHERE  ai_classification      = 'TRUE_FAILURE'
--   AND  ai_recommended_action  = 'ESCALATE'
--   AND  severity               IN ('CRITICAL', 'HIGH')
--   AND  failure_timestamp      >= NOW() - INTERVAL '24' HOUR
-- ORDER  BY failure_timestamp DESC;

-- False-positive rate by rule (last 7 days)
-- SELECT rule_name,
--        table_name,
--        COUNT(*)                                                                AS total,
--        SUM(CASE WHEN ai_classification = 'FALSE_POSITIVE' THEN 1 ELSE 0 END) AS fp_count,
--        ROUND(100.0 * SUM(CASE WHEN ai_classification = 'FALSE_POSITIVE' THEN 1 ELSE 0 END)
--              / NULLIF(COUNT(*), 0), 1)                                        AS fp_pct
-- FROM   audit_db.audit_validation
-- WHERE  status         = 'FAIL'
--   AND  ai_analysed_at >= NOW() - INTERVAL '7' DAY
-- GROUP  BY rule_name, table_name
-- ORDER  BY fp_pct DESC
-- LIMIT  20;
