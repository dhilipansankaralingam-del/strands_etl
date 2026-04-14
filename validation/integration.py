"""
validation/integration.py
==========================
Integration bridge between the existing **table_validator** pipeline and the
**Strands Validation Intelligence Framework**.

Responsibilities
----------------
1.  **AuditBridger**
        Reads FAIL rows from `audit_validation` (via Athena) and converts each
        row into a `ValidationRecord` that the Strands agent can analyse.

2.  **StrandsEnricher**
        Runs `ValidationAnalysisAgent` over a batch of `ValidationRecord`s and
        writes AI-generated columns (classification, confidence, recommended
        action, explanation) back into `audit_validation` on S3.

3.  **IntegratedValidationRunner**
        End-to-end orchestrator.  Call `run()` to:
          a. Execute the table_validator pipeline
          b. Forward every FAIL record to the Strands agent
          c. Persist enriched results back to S3 / Glue
          d. Return the combined report (validation + AI analysis)

Typical integration patterns
------------------------------
Pattern A — Run-after (most common)
    The table_validator finishes first, then the Strands layer reads from
    audit_validation and enriches only the FAIL rows.

        runner = IntegratedValidationRunner(...)
        report = runner.run()

Pattern B — Inline (real-time enrichment)
    Feed each FAIL record to the Strands agent immediately during validation,
    before writing to audit_validation.

        enricher = StrandsEnricher(agent, writer)
        for record in validator_fail_records:
            enricher.enrich_single(record)

Pattern C — On-demand CLI
    Use the validation_cli.py `analyze` / `batch` commands to query existing
    audit_validation records and trigger AI analysis interactively.

`audit_validation` enrichment columns written by this module
-------------------------------------------------------------
    ai_classification       TRUE_FAILURE | FALSE_POSITIVE | NEEDS_INVESTIGATION
    ai_confidence           0.0 – 1.0
    ai_recommended_action   IGNORE | RERUN | FIX_LOGIC | DATA_CORRECTION | ESCALATE | MONITOR
    ai_explanation          Free-text explanation from the agent
    ai_analysed_at          ISO-8601 timestamp
"""

import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import boto3

from validation.models import (
    ValidationRecord,
    AnalysisResult,
    ValidationClassification,
    RecommendedAction,
)
from validation.validation_agent import ValidationAnalysisAgent

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: thin Athena runner (mirrors the one in table_validator.py)
# ---------------------------------------------------------------------------

class _AthenaRunner:
    POLL_INTERVAL = 5
    MAX_POLLS     = 60

    def __init__(self, s3_output: str, workgroup: str, region: str):
        self.s3_output = s3_output
        self.workgroup = workgroup
        self.athena    = boto3.client("athena", region_name=region)

    def run(self, sql: str, database: str) -> Tuple[List[str], List[Dict[str, str]]]:
        exec_id = self.athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": self.s3_output},
            WorkGroup=self.workgroup,
        )["QueryExecutionId"]

        for _ in range(self.MAX_POLLS):
            resp  = self.athena.get_query_execution(QueryExecutionId=exec_id)
            state = resp["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                break
            if state in ("FAILED", "CANCELLED"):
                reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
                raise RuntimeError(f"Athena {state}: {reason}")
            time.sleep(self.POLL_INTERVAL)
        else:
            raise RuntimeError("Athena query timed out")

        result = self.athena.get_query_results(QueryExecutionId=exec_id)
        cols   = [c["Label"] for c in result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
        rows   = [
            {cols[i]: d.get("VarCharValue", "") for i, d in enumerate(row.get("Data", []))}
            for row in result["ResultSet"]["Rows"][1:]
        ]
        return cols, rows


# ---------------------------------------------------------------------------
# 1. AuditBridger
# ---------------------------------------------------------------------------

class AuditBridger:
    """
    Reads failed rows from `audit_validation` in Athena and converts each row
    into a `ValidationRecord` ready for the Strands agent.

    Parameters
    ----------
    database : str
        Glue/Athena database that contains the `audit_validation` table.
    s3_output : str
        S3 path for Athena query results (e.g. ``s3://my-bucket/athena/``).
    workgroup : str
        Athena workgroup (default ``"primary"``).
    region : str
        AWS region.
    """

    AUDIT_TABLE = "audit_validation"

    def __init__(
        self,
        database:   str,
        s3_output:  str,
        workgroup:  str = "primary",
        region:     str = "us-east-1",
    ):
        self.database = database
        self._athena  = _AthenaRunner(s3_output, workgroup, region)

    # ------------------------------------------------------------------

    def fetch_failed_records(
        self,
        run_id:      Optional[str] = None,
        pipeline:    Optional[str] = None,
        limit:       int           = 500,
        since_hours: int           = 24,
    ) -> List[ValidationRecord]:
        """
        Query audit_validation for FAIL rows and convert to ValidationRecords.

        Parameters
        ----------
        run_id : str, optional
            Filter to a specific pipeline run.
        pipeline : str, optional
            Filter by pipeline_name.
        limit : int
            Maximum rows to fetch (default 500).
        since_hours : int
            Only include failures in the last N hours (default 24).

        Returns
        -------
        List[ValidationRecord]
        """
        filters = [
            "status = 'FAIL'",
            f"failure_timestamp >= NOW() - INTERVAL '{since_hours}' HOUR",
        ]
        if run_id:
            filters.append(f"run_id = '{run_id}'")
        if pipeline:
            filters.append(f"pipeline_name = '{pipeline}'")

        where_clause = " AND ".join(filters)
        sql = f"""
            SELECT *
            FROM   {self.database}.{self.AUDIT_TABLE}
            WHERE  {where_clause}
            ORDER  BY failure_timestamp DESC
            LIMIT  {limit}
        """

        try:
            _, rows = self._athena.run(sql, self.database)
        except RuntimeError as exc:
            logger.error(f"Could not fetch failed audit records: {exc}")
            return []

        records = []
        for row in rows:
            try:
                records.append(ValidationRecord.from_athena_row(row))
            except Exception as exc:
                logger.warning(f"Skipping malformed audit row: {exc}")
        logger.info(f"AuditBridger: fetched {len(records)} FAIL records")
        return records

    def fetch_by_record_id(self, record_id: str) -> Optional[ValidationRecord]:
        """Fetch one specific record from audit_validation by its primary key."""
        sql = f"""
            SELECT *
            FROM   {self.database}.{self.AUDIT_TABLE}
            WHERE  record_id = '{record_id}'
            LIMIT  1
        """
        try:
            _, rows = self._athena.run(sql, self.database)
            if rows:
                return ValidationRecord.from_athena_row(rows[0])
        except RuntimeError as exc:
            logger.error(f"Could not fetch record {record_id}: {exc}")
        return None


# ---------------------------------------------------------------------------
# 2. StrandsEnricher
# ---------------------------------------------------------------------------

class StrandsEnricher:
    """
    Runs the Strands `ValidationAnalysisAgent` over a list of `ValidationRecord`s
    and writes the AI-generated columns back to S3 (in the audit_validation
    partition for the current enrichment batch).

    Parameters
    ----------
    agent : ValidationAnalysisAgent
        Pre-constructed Strands agent.
    s3_output_bucket : str
        Bucket name (without s3://) where enriched results are persisted.
    s3_prefix : str
        Key prefix inside the bucket (default ``"audit_validation/enriched"``).
    region : str
        AWS region.
    """

    DEFAULT_S3_PREFIX = "audit_validation/enriched"

    def __init__(
        self,
        agent:             ValidationAnalysisAgent,
        s3_output_bucket:  str,
        s3_prefix:         str = DEFAULT_S3_PREFIX,
        region:            str = "us-east-1",
    ):
        self.agent  = agent
        self.bucket = s3_output_bucket
        self.prefix = s3_prefix.rstrip("/")
        self.s3     = boto3.client("s3", region_name=region)

    # ------------------------------------------------------------------

    def enrich_batch(
        self,
        records:  List[ValidationRecord],
        run_id:   str,
    ) -> List[Dict[str, Any]]:
        """
        Analyse every ValidationRecord and return a list of enriched audit dicts.

        Each dict is the original audit_validation row with AI columns merged in.
        The enriched rows are also persisted to S3.

        Parameters
        ----------
        records : list of ValidationRecord
        run_id : str
            Used to partition the enriched output on S3.

        Returns
        -------
        list of dict  (one per record, with ai_* columns populated)
        """
        enriched: List[Dict[str, Any]] = []

        for record in records:
            ai_cols = self._analyse_one(record)
            row     = record.to_dict()
            row.update(ai_cols)
            enriched.append(row)

        self._persist_enriched(enriched, run_id)
        return enriched

    def enrich_single(self, record: ValidationRecord) -> Dict[str, Any]:
        """
        Analyse a single record inline (Pattern B — real-time enrichment).
        Returns the original row dict with AI columns merged in.
        Does NOT persist to S3 (caller is responsible).
        """
        ai_cols = self._analyse_one(record)
        row = record.to_dict()
        row.update(ai_cols)
        return row

    # ------------------------------------------------------------------

    def _analyse_one(self, record: ValidationRecord) -> Dict[str, Any]:
        """Run the Strands decision agent and extract AI columns."""
        try:
            result: AnalysisResult = self.agent.analyze(record)
            return {
                "ai_classification":     result.classification.value,
                "ai_confidence":         round(result.confidence, 4),
                "ai_recommended_action": result.recommended_action.value,
                "ai_explanation":        result.explanation,
                "ai_analysed_at":        result.timestamp,
                "ai_root_causes":        json.dumps(result.root_causes),
                "ai_next_steps":         json.dumps(result.suggested_next_steps),
                "ai_validation_sql":     result.validation_sql or "",
            }
        except Exception as exc:
            logger.error(f"Agent failed for record {record.record_id}: {exc}")
            return {
                "ai_classification":     ValidationClassification.NEEDS_INVESTIGATION.value,
                "ai_confidence":         0.0,
                "ai_recommended_action": RecommendedAction.ESCALATE.value,
                "ai_explanation":        f"Agent error: {exc}",
                "ai_analysed_at":        datetime.utcnow().isoformat(),
                "ai_root_causes":        "[]",
                "ai_next_steps":         "[]",
                "ai_validation_sql":     "",
            }

    def _persist_enriched(self, enriched: List[Dict[str, Any]], run_id: str) -> str:
        """Write enriched rows to S3 as newline-delimited JSON."""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        key   = f"{self.prefix}/dt={today}/run_id={run_id}/enriched.json"
        body  = "\n".join(json.dumps(row, default=str) for row in enriched)

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )
        uri = f"s3://{self.bucket}/{key}"
        logger.info(f"Enriched audit records written → {uri}")
        return uri


# ---------------------------------------------------------------------------
# 3. IntegratedValidationRunner
# ---------------------------------------------------------------------------

class IntegratedValidationRunner:
    """
    Full end-to-end orchestrator:

        Step 1  TableValidator.run()         → raw audit_validation rows
        Step 2  AuditBridger.fetch_*()       → ValidationRecord objects
        Step 3  StrandsEnricher.enrich_batch() → AI-enriched rows
        Step 4  (optional) pattern extraction  → learning insights
        Step 5  Return consolidated report

    Parameters
    ----------
    config_path : str
        Validation rules config (local path or s3://…).
    s3_output : str
        S3 URI for Athena outputs AND audit_validation writes
        (e.g. ``s3://my-bucket/athena/``).
    audit_database : str
        Glue/Athena database containing the audit_validation table.
    learning_bucket : str
        S3 bucket for Strands learning vectors.
    pipeline_name : str
        Human-readable pipeline tag.
    workgroup : str
        Athena workgroup.
    region : str
        AWS region.
    """

    def __init__(
        self,
        config_path:     str,
        s3_output:       str,
        audit_database:  str,
        learning_bucket: str = "strands-etl-learning",
        pipeline_name:   str = "default_pipeline",
        workgroup:       str = "primary",
        region:          str = "us-east-1",
        extract_patterns: bool = False,
    ):
        from pyscript.table_validator import TableValidator  # lazy import to avoid circular

        self.config_path      = config_path
        self.s3_output        = s3_output
        self.audit_database   = audit_database
        self.pipeline_name    = pipeline_name
        self.extract_patterns = extract_patterns
        self.region           = region

        # Parse bucket from s3_output
        s3_parts = s3_output[5:].split("/", 1)
        s3_bucket = s3_parts[0]

        self.validator = TableValidator(
            config_path=config_path,
            s3_output=s3_output,
            pipeline_name=pipeline_name,
            database=audit_database,
            workgroup=workgroup,
            region=region,
        )
        self.bridger = AuditBridger(
            database=audit_database,
            s3_output=s3_output,
            workgroup=workgroup,
            region=region,
        )
        self.agent = ValidationAnalysisAgent(
            learning_bucket=learning_bucket,
            aws_region=region,
        )
        self.enricher = StrandsEnricher(
            agent=self.agent,
            s3_output_bucket=s3_bucket,
            region=region,
        )

    # ------------------------------------------------------------------

    def run(self) -> Dict[str, Any]:
        """
        Execute the integrated pipeline.

        Returns a consolidated report dict::

            {
              "run_id": "...",
              "pipeline_name": "...",
              "validation_summary": { passed, failed, errors, total_rules },
              "ai_analysis": {
                "true_failures": <int>,
                "false_positives": <int>,
                "needs_investigation": <int>,
                "data_quality_score": <int>,
                "top_issues": [...],
                "executive_summary": "..."
              },
              "enriched_records": [...],
              "patterns": {...},   # present only if extract_patterns=True
              "timestamp": "..."
            }
        """
        logger.info(f"=== IntegratedValidationRunner START (pipeline={self.pipeline_name}) ===")

        # ── Step 1: Run the table validator ──────────────────────────────
        logger.info("Step 1: Running table validator …")
        val_summary = self.validator.run()
        run_id = val_summary["run_id"]
        logger.info(f"  Validation complete. PASS={val_summary['passed']}  "
                    f"FAIL={val_summary['failed']}  ERROR={val_summary['errors']}")

        # ── Step 2: Bridge FAIL rows to ValidationRecords ─────────────────
        logger.info("Step 2: Bridging failed records to Strands ValidationRecords …")
        if val_summary["failed"] == 0:
            logger.info("  No failures detected. Skipping AI analysis.")
            return self._report(run_id, val_summary, enriched=[], batch_result={})

        # Use in-memory records from the validator output to avoid an extra Athena round-trip
        fail_rows = val_summary.get("failed_records", [])
        records   = [ValidationRecord.from_athena_row(row) for row in fail_rows]
        logger.info(f"  Bridged {len(records)} FAIL records")

        # ── Step 3: AI enrichment ─────────────────────────────────────────
        logger.info("Step 3: Running Strands AI enrichment …")
        enriched = self.enricher.enrich_batch(records, run_id)

        # ── Step 4: Batch summary via the agent ───────────────────────────
        logger.info("Step 4: Generating batch AI summary …")
        analysis_results = [self.agent.analyze(r) for r in records]
        batch_result     = self.agent.batch_agent(analysis_results)

        # ── Step 5 (optional): Pattern extraction ─────────────────────────
        patterns = {}
        if self.extract_patterns:
            logger.info("Step 5: Extracting learning patterns …")
            patterns = self.agent.extract_patterns(limit=100)

        logger.info("=== IntegratedValidationRunner COMPLETE ===")
        return self._report(run_id, val_summary, enriched, batch_result, patterns)

    # ------------------------------------------------------------------

    def run_on_existing(
        self,
        run_id:      Optional[str] = None,
        pipeline:    Optional[str] = None,
        since_hours: int = 24,
    ) -> Dict[str, Any]:
        """
        Alternative entry point: skip the validator step and read directly
        from audit_validation in Athena.  Useful for re-analysing old runs
        or running ad-hoc AI enrichment via the CLI.

        Parameters
        ----------
        run_id : str, optional
            Re-analyse a specific historical run.
        pipeline : str, optional
            Filter by pipeline name.
        since_hours : int
            Look back N hours (default 24).
        """
        logger.info("Step 1: Fetching existing FAIL records from audit_validation …")
        records = self.bridger.fetch_failed_records(
            run_id=run_id,
            pipeline=pipeline,
            since_hours=since_hours,
        )
        if not records:
            return {"message": "No failed records found.", "run_id": run_id}

        enrichment_run_id = run_id or str(uuid.uuid4())

        logger.info(f"Step 2: Enriching {len(records)} records …")
        enriched = self.enricher.enrich_batch(records, enrichment_run_id)

        analysis_results = [self.agent.analyze(r) for r in records]
        batch_result     = self.agent.batch_agent(analysis_results)

        return self._report(enrichment_run_id, {}, enriched, batch_result)

    # ------------------------------------------------------------------

    @staticmethod
    def _report(
        run_id:       str,
        val_summary:  Dict[str, Any],
        enriched:     List[Dict[str, Any]],
        batch_result: Dict[str, Any],
        patterns:     Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        report: Dict[str, Any] = {
            "run_id":      run_id,
            "pipeline_name": val_summary.get("pipeline_name", ""),
            "validation_summary": {
                "total_rules": val_summary.get("total_rules", 0),
                "passed":      val_summary.get("passed", 0),
                "failed":      val_summary.get("failed", 0),
                "errors":      val_summary.get("errors", 0),
                "s3_audit_path": val_summary.get("s3_audit_path", ""),
            },
            "ai_analysis":      batch_result,
            "enriched_records": enriched,
            "timestamp":        datetime.utcnow().isoformat(),
        }
        if patterns:
            report["patterns"] = patterns
        return report


# ---------------------------------------------------------------------------
# Convenience factory functions
# ---------------------------------------------------------------------------

def create_runner(
    config_path:     str,
    s3_output:       str,
    audit_database:  str,
    pipeline_name:   str  = "default_pipeline",
    learning_bucket: str  = "strands-etl-learning",
    workgroup:       str  = "primary",
    region:          str  = "us-east-1",
    extract_patterns: bool = False,
) -> IntegratedValidationRunner:
    """
    Factory that wires up all components and returns an IntegratedValidationRunner.

    Example::

        runner = create_runner(
            config_path     = "s3://my-bucket/config/rules.json",
            s3_output       = "s3://my-bucket/athena/",
            audit_database  = "data_quality_db",
            pipeline_name   = "customer_order_etl",
        )
        report = runner.run()
    """
    return IntegratedValidationRunner(
        config_path=config_path,
        s3_output=s3_output,
        audit_database=audit_database,
        learning_bucket=learning_bucket,
        pipeline_name=pipeline_name,
        workgroup=workgroup,
        region=region,
        extract_patterns=extract_patterns,
    )


def analyse_existing_run(
    run_id:         str,
    s3_output:      str,
    audit_database: str,
    learning_bucket: str = "strands-etl-learning",
    workgroup:       str = "primary",
    region:          str = "us-east-1",
) -> Dict[str, Any]:
    """
    Shortcut: read a previous run from audit_validation and enrich with AI.

    Example::

        report = analyse_existing_run(
            run_id         = "3f8a1b2c-...",
            s3_output      = "s3://my-bucket/athena/",
            audit_database = "data_quality_db",
        )
    """
    runner = IntegratedValidationRunner(
        config_path="",        # not used
        s3_output=s3_output,
        audit_database=audit_database,
        learning_bucket=learning_bucket,
        workgroup=workgroup,
        region=region,
    )
    return runner.run_on_existing(run_id=run_id)
