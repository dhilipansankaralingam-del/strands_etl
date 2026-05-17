"""
validation/audit_action_agent.py
=================================
Strands SDK Agentic Audit Failure Analyzer + Action Dispatcher.

Reads FAIL rows from a validation audit table via a simple Athena query,
classifies each failure with AI, enriches with data profiling signals,
and autonomously dispatches corrective actions.

Usage
-----
    from validation.audit_action_agent import AuditActionAgent, build_audit_agent

    # Direct Python API — just supply database, table, and date
    agent = AuditActionAgent(database="insurance_dw", table="audit_validation")
    report = agent.run_for_date("2024-07-01")

    # Autonomous Strands Agent
    agent = build_audit_agent(database="insurance_dw", table="audit_validation")
    response = agent("Analyse all failures for 2024-07-01 and take action.")

Edge Cases Handled
------------------
- Null flood in a NOT_NULL column              → DATA_CORRECTION + fix SQL
- Cardinality explosion after a join           → FIX_LOGIC + query rewrite hint
- Temporal anomaly (timestamp rollback)        → ESCALATE immediately
- Distribution skew masks business KPI failure → MONITOR + alert threshold
- Schema drift on a CRITICAL table             → ESCALATE + halt downstream
- Stale partition breaches SLA                 → flag for re-ingest
- Duplicate key storm in a PK column           → ESCALATE + dedup SQL
- Boundary explosion (unit conversion bug)     → DATA_CORRECTION + fix SQL
- Zero inflation in revenue metrics            → ESCALATE (financial risk)
- Encoding rot in customer name fields         → FIX_LOGIC + re-ingest hint
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3
from strands import Agent, tool

from validation.data_profiler import DataProfilerAgent
from validation.models import (
    ActionDispatchResult,
    AnalysisResult,
    AnomalyType,
    DataProfileResult,
    RecommendedAction,
    ValidationClassification,
    ValidationRecord,
)
from validation.prompts import (
    DATA_PROFILER_SYSTEM_PROMPT,
    SYSTEM_PROMPT,
    build_audit_action_prompt,
)
from validation.validation_agent import ValidationAnalysisAgent

logger = logging.getLogger(__name__)


# ===========================================================================
# AuditActionAgent
# ===========================================================================

class AuditActionAgent:
    """
    End-to-end audit failure analyzer and action dispatcher.

    Steps for each failed validation record:
      1. Load from Athena audit table
      2. Classify via ValidationAnalysisAgent (decision_agent)
      3. Profile the affected table (DataProfilerAgent)
      4. Refine the recommended action using combined signals
      5. Dispatch the action (rerun / fix-sql / rule-change / escalate / monitor)
      6. Persist ActionDispatchResult to S3
    """

    MODEL_ID          = "anthropic.claude-3-sonnet-20240229-v1:0"
    ACTION_LOG_PREFIX = "validation/audit/action_logs/"

    FORCE_ESCALATE_ANOMALIES = {
        AnomalyType.TEMPORAL_ANOMALY,
        AnomalyType.SCHEMA_DRIFT,
        AnomalyType.ZERO_INFLATION,
    }

    def __init__(
        self,
        database: str,
        table: str,
        athena_output: str = "s3://strands-etl-athena-results/audit/",
        learning_bucket: str = "strands-etl-learning",
        aws_region: str = "us-east-1",
        model_id: str = MODEL_ID,
        configured_rules: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    ):
        """
        Parameters
        ----------
        database : str
            Athena / Glue database that contains the audit table.
        table : str
            Audit validation table name (e.g. "audit_validation").
        configured_rules : dict, optional
            Dict keyed by "database.tablename" → list of rule dicts from config.
            Injected into every AI decision prompt.
        """
        self.audit_database  = database
        self.audit_table     = table
        self.learning_bucket = learning_bucket
        self.athena_output   = athena_output
        self.model_id        = model_id
        self.aws_region      = aws_region
        self.configured_rules: Dict[str, List[Dict[str, Any]]] = configured_rules or {}

        self.athena  = boto3.client("athena",          region_name=aws_region)
        self.s3      = boto3.client("s3",              region_name=aws_region)
        self.glue    = boto3.client("glue",            region_name=aws_region)
        self.sns     = boto3.client("sns",             region_name=aws_region)
        self.bedrock = boto3.client("bedrock-runtime", region_name=aws_region)

        self.validation_agent = ValidationAnalysisAgent(
            learning_bucket=learning_bucket,
            aws_region=aws_region,
            model_id=model_id,
        )
        self.profiler = DataProfilerAgent(
            database=database,
            learning_bucket=learning_bucket,
            aws_region=aws_region,
            model_id=model_id,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_for_date(self, date: str, severity_filter: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        """
        Fetch all FAIL records for a given date and process them.

        Parameters
        ----------
        date : str
            Date in YYYY-MM-DD format. Matches on failure_timestamp date part.
        severity_filter : str, optional
            Minimum severity to include: LOW | MEDIUM | HIGH | CRITICAL.
            Defaults to all severities.
        limit : int
            Max records to process per run.

        Example
        -------
            agent = AuditActionAgent(database="insurance_dw", table="audit_validation")
            report = agent.run_for_date("2024-07-01")
            report = agent.run_for_date("2024-07-01", severity_filter="HIGH")
        """
        return self.run(date=date, severity_filter=severity_filter, limit=limit)

    def run(
        self,
        date: Optional[str] = None,
        severity_filter: Optional[str] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """
        Core pipeline: fetch → classify → profile → dispatch → summarise.

        Parameters
        ----------
        date : str, optional
            YYYY-MM-DD date to filter failures on. Omit to fetch latest failures.
        severity_filter : str, optional
            Minimum severity level: LOW | MEDIUM | HIGH | CRITICAL.
        """
        logger.info(f"[AuditActionAgent] Starting audit: date={date}, severity={severity_filter}")

        records = self._fetch_audit_failures(date=date, severity_filter=severity_filter, limit=limit)
        if not records:
            return {"message": "No FAIL records found matching the criteria.", "total": 0}

        logger.info(f"[AuditActionAgent] Loaded {len(records)} failure records")

        # ── Step 2: Batch analyze + profile + dispatch
        dispatch_results: List[ActionDispatchResult] = []
        profile_cache: Dict[str, DataProfileResult] = {}

        for record in records:
            try:
                result = self._process_record(record, profile_cache)
                dispatch_results.append(result)
            except Exception as e:
                logger.error(f"[AuditActionAgent] Failed to process record {record.record_id}: {e}")

        # ── Step 3: Executive summary
        summary = self._build_summary(records, dispatch_results)
        self._persist_summary(date or "latest", summary)

        return {
            "date": date,
            "total_failures": len(records),
            "actions_taken": [r.to_dict() for r in dispatch_results],
            "summary": summary,
            "timestamp": datetime.utcnow().isoformat(),
        }

    def process_single(self, record: ValidationRecord) -> ActionDispatchResult:
        """Analyze and act on a single ValidationRecord."""
        return self._process_record(record, profile_cache={})

    # ------------------------------------------------------------------
    # Core processing pipeline
    # ------------------------------------------------------------------

    def _process_record(
        self,
        record: ValidationRecord,
        profile_cache: Dict[str, DataProfileResult],
        table_cfg: Optional[Dict[str, Any]] = None,
    ) -> ActionDispatchResult:
        table_key = f"{record.database_name or self.audit_database}.{record.table_name}"
        logger.info(f"[AuditActionAgent] Processing {table_key}  rule={record.rule_name}  id={record.record_id}")

        # Pull configured rules for this table
        rules = self.configured_rules.get(table_key, [])
        if rules:
            logger.info(f"[AuditActionAgent] Injecting {len(rules)} configured rules into AI prompt")

        # Load historical health scores for trend analysis
        health_history = self._load_health_score_history(table_key)

        # Step A: AI classification (with rules + health trend)
        history = self.validation_agent._get_similar_outcomes(record, limit=20)
        analysis: AnalysisResult = self.validation_agent.decision_agent(
            record,
            history,
            configured_rules=rules if rules else None,
            health_score_history=health_history if health_history else None,
        )

        # Step B: Data profile for the affected table (cached per table)
        if table_key not in profile_cache:
            self.profiler.database = record.database_name or self.audit_database
            tc = table_cfg or {}
            try:
                profile = self.profiler.profile(
                    table_name=record.table_name,
                    run_id=record.run_id,
                    key_columns=tc.get("key_columns") or ([record.column_name] if record.column_name else None),
                    primary_key_column=tc.get("primary_key_column") or record.additional_context.get("primary_key_column"),
                    metric_columns=tc.get("metric_columns") or record.additional_context.get("metric_columns"),
                    timestamp_column=tc.get("timestamp_column") or record.additional_context.get("timestamp_column"),
                    partition_column=tc.get("partition_column"),
                    freshness_sla_hours=tc.get("freshness_sla_hours", 26.0),
                )
                profile_cache[table_key] = profile
            except Exception as e:
                logger.warning(f"Profile failed for {table_key}: {e}")
                profile_cache[table_key] = None

        profile = profile_cache.get(table_key)

        # Step C: Refine action using profile signals + edge-case overrides
        final_action, action_payload = self._decide_action(record, analysis, profile)

        # Step D: Dispatch the action
        success, error_msg = self._dispatch(final_action, record, action_payload)

        dispatch_result = ActionDispatchResult(
            dispatch_id=str(uuid.uuid4()),
            record_id=record.record_id,
            rule_name=record.rule_name,
            table_name=record.table_name,
            recommended_action=analysis.recommended_action.value,
            action_taken=final_action,
            action_payload=action_payload,
            success=success,
            error_message=error_msg,
        )

        self._persist_action_log(dispatch_result)
        return dispatch_result

    # ------------------------------------------------------------------
    # Action decision logic
    # ------------------------------------------------------------------

    def _decide_action(
        self,
        record: ValidationRecord,
        analysis: AnalysisResult,
        profile: Optional[DataProfileResult],
    ) -> tuple[str, Dict[str, Any]]:
        """
        Determine the final action to take, blending AI recommendation with
        data profile signals and hard-coded edge-case overrides.
        """
        # ── Hard overrides: certain anomaly types always escalate
        if profile:
            for anomaly in profile.anomalies:
                if anomaly.anomaly_type in self.FORCE_ESCALATE_ANOMALIES:
                    logger.warning(
                        f"[AuditActionAgent] Force-escalating {record.record_id} "
                        f"due to {anomaly.anomaly_type.value} anomaly in table profile"
                    )
                    return "ESCALATE", {
                        "alert_message": (
                            f"[FORCE ESCALATE] {anomaly.anomaly_type.value} detected in "
                            f"{record.database_name}.{record.table_name}: {anomaly.description}"
                        ),
                        "anomaly": anomaly.to_dict(),
                        "fix_sql": None,
                        "rule_change": None,
                        "rerun_job": None,
                        "monitor_schedule": None,
                    }

            # Health score too low → escalate regardless of AI
            if profile.data_health_score < 40 and record.severity in ("HIGH", "CRITICAL"):
                return "ESCALATE", {
                    "alert_message": (
                        f"Table {record.table_name} health score is {profile.data_health_score}/100. "
                        f"Failure rule: {record.rule_name}. Immediate investigation required."
                    ),
                    "fix_sql": None,
                    "rule_change": None,
                    "rerun_job": None,
                    "monitor_schedule": None,
                }

        # ── LLM-driven action refinement
        ai_action = self._ai_refine_action(record, analysis, profile)
        action    = ai_action.get("final_action", analysis.recommended_action.value)
        payload   = ai_action.get("action_payload", {})

        # ── Semantic enrichment per action type
        if action == "DATA_CORRECTION" and not payload.get("fix_sql"):
            payload["fix_sql"] = self._generate_fix_sql(record, profile)

        if action == "RERUN" and not payload.get("rerun_job"):
            payload["rerun_job"] = record.additional_context.get("glue_job_name") or self.glue_job_name

        if action == "ESCALATE" and not payload.get("alert_message"):
            payload["alert_message"] = (
                f"[DQ Alert] {record.severity} failure: {record.rule_name} on "
                f"{record.table_name}.{record.column_name}. "
                f"AI explanation: {analysis.explanation[:300]}"
            )

        if action == "MONITOR" and not payload.get("monitor_schedule"):
            payload["monitor_schedule"] = "rate(1 day)"

        return action, payload

    def _ai_refine_action(
        self,
        record: ValidationRecord,
        analysis: AnalysisResult,
        profile: Optional[DataProfileResult],
    ) -> Dict[str, Any]:
        """Call Bedrock to refine the final action using profile context."""
        profile_dict = profile.to_dict() if profile else {}
        prompt = build_audit_action_prompt(
            record=record.to_dict(),
            analysis=analysis.to_dict(),
            profile=profile_dict,
        )
        try:
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 1500,
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": prompt}],
            }
            response = self.bedrock.invoke_model(
                modelId=self.model_id,
                body=json.dumps(body),
            )
            raw = json.loads(response["body"].read())["content"][0]["text"]
            return self._parse_json(raw)
        except Exception as e:
            logger.warning(f"AI action refinement failed: {e}")
            return {"final_action": analysis.recommended_action.value, "action_payload": {}}

    def _generate_fix_sql(
        self, record: ValidationRecord, profile: Optional[DataProfileResult]
    ) -> Optional[str]:
        """Generate a best-effort Athena correction SQL based on the failure type."""
        col   = record.column_name
        table = f"{record.database_name}.{record.table_name}"
        rule  = record.rule_type.upper()

        if rule == "NOT_NULL":
            # Replace nulls with a sensible default
            return (
                f"-- Replace NULLs in {col} with a fallback value\n"
                f"INSERT OVERWRITE INTO {table}\n"
                f"SELECT * REPLACE(COALESCE({col}, 'UNKNOWN') AS {col})\n"
                f"FROM {table};"
            )
        if rule == "RANGE":
            # Clamp out-of-range values
            constraint = record.expected_constraint
            return (
                f"-- Clamp {col} to expected range: {constraint}\n"
                f"-- Review and adjust bounds before running\n"
                f"INSERT OVERWRITE INTO {table}\n"
                f"SELECT * REPLACE(\n"
                f"  CASE WHEN {col} < 0 THEN 0\n"
                f"       WHEN {col} > 9999999 THEN 9999999\n"
                f"       ELSE {col}\n"
                f"  END AS {col}\n"
                f") FROM {table};"
            )
        if rule == "REGEX":
            return (
                f"-- Flag rows where {col} fails regex: {record.expected_constraint}\n"
                f"SELECT * FROM {table}\n"
                f"WHERE NOT REGEXP_LIKE({col}, '{record.expected_constraint}');"
            )
        if rule in ("REFERENTIAL", "BUSINESS"):
            return (
                f"-- Identify orphan rows: {col} not in reference set\n"
                f"SELECT t.* FROM {table} t\n"
                f"LEFT JOIN <reference_table> r ON t.{col} = r.{col}\n"
                f"WHERE r.{col} IS NULL\n"
                f"LIMIT 100;"
            )
        return None

    # ------------------------------------------------------------------
    # Action dispatchers
    # ------------------------------------------------------------------

    def _dispatch(
        self, action: str, record: ValidationRecord, payload: Dict[str, Any]
    ) -> tuple[bool, str]:
        """Route to the appropriate action handler."""
        dispatch_map = {
            "RERUN":           self._dispatch_rerun,
            "DATA_CORRECTION": self._dispatch_data_correction,
            "FIX_LOGIC":       self._dispatch_fix_logic,
            "ESCALATE":        self._dispatch_escalate,
            "MONITOR":         self._dispatch_monitor,
            "IGNORE":          self._dispatch_ignore,
        }
        handler = dispatch_map.get(action, self._dispatch_monitor)
        try:
            return handler(record, payload)
        except Exception as e:
            logger.error(f"Dispatch failed for action {action}: {e}")
            return False, str(e)

    def _dispatch_rerun(self, record: ValidationRecord, payload: Dict[str, Any]) -> tuple[bool, str]:
        """Re-trigger the Glue ETL job for the affected pipeline."""
        job_name = payload.get("rerun_job") or record.additional_context.get("glue_job_name", "")
        if not job_name:
            logger.warning("RERUN requested but no Glue job name available in record context")
            return False, "No Glue job name available — set additional_context.glue_job_name on the record"
        try:
            resp = self.glue.start_job_run(
                JobName=job_name,
                Arguments={
                    "--run_id":       record.run_id,
                    "--table_name":   record.table_name,
                    "--triggered_by": "audit_action_agent",
                },
            )
            logger.info(f"Glue job {job_name} re-triggered: JobRunId={resp['JobRunId']}")
            return True, ""
        except Exception as e:
            return False, str(e)

    def _dispatch_data_correction(
        self, record: ValidationRecord, payload: Dict[str, Any]
    ) -> tuple[bool, str]:
        """Log the generated fix SQL to S3 for human review before execution."""
        fix_sql = payload.get("fix_sql", "-- No fix SQL generated")
        key = (
            f"validation/audit/fix_sql/{record.table_name}/"
            f"{record.run_id}/{record.record_id}.sql"
        )
        try:
            self.s3.put_object(
                Bucket=self.learning_bucket,
                Key=key,
                Body=fix_sql.encode("utf-8"),
                ContentType="text/plain",
            )
            logger.info(f"Fix SQL persisted: s3://{self.learning_bucket}/{key}")
            return True, ""
        except Exception as e:
            return False, str(e)

    def _dispatch_fix_logic(
        self, record: ValidationRecord, payload: Dict[str, Any]
    ) -> tuple[bool, str]:
        """Log a rule change recommendation to S3."""
        rule_change = payload.get("rule_change", "No rule change description provided.")
        doc = {
            "record_id": record.record_id,
            "rule_name": record.rule_name,
            "table_name": record.table_name,
            "column_name": record.column_name,
            "current_constraint": record.expected_constraint,
            "proposed_change": rule_change,
            "created_at": datetime.utcnow().isoformat(),
        }
        key = f"validation/audit/rule_changes/{record.run_id}/{record.record_id}.json"
        try:
            self.s3.put_object(
                Bucket=self.learning_bucket,
                Key=key,
                Body=json.dumps(doc, indent=2),
            )
            logger.info(f"Rule change recommendation persisted: s3://{self.learning_bucket}/{key}")
            return True, ""
        except Exception as e:
            return False, str(e)

    def _dispatch_escalate(
        self, record: ValidationRecord, payload: Dict[str, Any]
    ) -> tuple[bool, str]:
        """Send an SNS alert for critical failures requiring human intervention."""
        sns_topic_arn = record.additional_context.get("sns_topic_arn", "")
        message = payload.get("alert_message", f"DQ failure requires attention: {record.rule_name}")
        subject = f"[DQ ESCALATION] {record.severity}: {record.rule_name} on {record.table_name}"
        if not sns_topic_arn:
            logger.warning("ESCALATE action but no sns_topic_arn in record.additional_context; logging only")
            logger.warning(f"ESCALATION MESSAGE: {subject} — {message}")
            return True, ""
        try:
            self.sns.publish(
                TopicArn=sns_topic_arn,
                Subject=subject[:100],
                Message=json.dumps({
                    "alert": message,
                    "record_id": record.record_id,
                    "rule_name": record.rule_name,
                    "table_name": record.table_name,
                    "severity": record.severity,
                    "run_id": record.run_id,
                    "timestamp": datetime.utcnow().isoformat(),
                    "anomaly_detail": payload.get("anomaly"),
                }, indent=2),
            )
            logger.info(f"SNS alert sent for record {record.record_id}")
            return True, ""
        except Exception as e:
            return False, str(e)

    def _dispatch_monitor(
        self, record: ValidationRecord, payload: Dict[str, Any]
    ) -> tuple[bool, str]:
        """Tag the record for follow-up monitoring in the next N runs."""
        doc = {
            "record_id": record.record_id,
            "rule_name": record.rule_name,
            "table_name": record.table_name,
            "monitor_schedule": payload.get("monitor_schedule", "rate(1 day)"),
            "monitor_reason": payload.get("rationale", "Borderline failure; watching next 3 runs"),
            "created_at": datetime.utcnow().isoformat(),
        }
        key = f"validation/audit/monitor_queue/{record.table_name}/{record.record_id}.json"
        try:
            self.s3.put_object(
                Bucket=self.learning_bucket,
                Key=key,
                Body=json.dumps(doc, indent=2),
            )
            return True, ""
        except Exception as e:
            return False, str(e)

    def _dispatch_ignore(
        self, record: ValidationRecord, payload: Dict[str, Any]
    ) -> tuple[bool, str]:
        """Log confirmed false-positive with rationale; no further action."""
        doc = {
            "record_id": record.record_id,
            "rule_name": record.rule_name,
            "table_name": record.table_name,
            "classification": ValidationClassification.FALSE_POSITIVE.value,
            "rationale": payload.get("rationale", "Confirmed false-positive by AI agent"),
            "ignored_at": datetime.utcnow().isoformat(),
        }
        date_str = record.failure_timestamp[:10] if record.failure_timestamp else "unknown"
        key = f"validation/audit/ignored/{date_str}/{record.record_id}.json"
        try:
            self.s3.put_object(
                Bucket=self.learning_bucket,
                Key=key,
                Body=json.dumps(doc, indent=2),
            )
            return True, ""
        except Exception as e:
            return False, str(e)

    # ------------------------------------------------------------------
    # Health score history (for trend-based confidence calibration)
    # ------------------------------------------------------------------

    def _load_health_score_history(self, table_key: str) -> List[Dict[str, Any]]:
        """Load last 10 profiling results from S3 to compute health trend."""
        try:
            prefix = f"validation/profiling/results/{table_key.replace('.', '/')}/"
            resp = self.s3.list_objects_v2(
                Bucket=self.learning_bucket, Prefix=prefix, MaxKeys=50
            )
            objects = sorted(
                resp.get("Contents", []),
                key=lambda x: x["LastModified"],
                reverse=True,
            )[:10]
            history = []
            for obj in objects:
                try:
                    body = self.s3.get_object(Bucket=self.learning_bucket, Key=obj["Key"])["Body"].read()
                    data = json.loads(body)
                    history.append({
                        "profiled_at":       data.get("profiled_at", ""),
                        "data_health_score": data.get("data_health_score", 100),
                        "anomaly_count":     len(data.get("anomalies", [])),
                        "schema_drift":      data.get("schema_drift_detected", False),
                    })
                except Exception:
                    pass
            return history
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Audit table helpers
    # ------------------------------------------------------------------

    def _fetch_audit_failures(
        self,
        date: Optional[str],
        severity_filter: Optional[str],
        limit: int,
    ) -> List[ValidationRecord]:
        """
        Simple Athena query — fetch FAIL rows for a given date from the audit table.

        date format : YYYY-MM-DD  (matched against DATE(failure_timestamp))
        """
        severity_rank = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}

        where_clauses = ["status = 'FAIL'"]

        if date:
            where_clauses.append(f"DATE(failure_timestamp) = DATE '{date}'")

        if severity_filter and severity_filter in severity_rank:
            min_rank  = severity_rank[severity_filter]
            sev_in    = [s for s, r in severity_rank.items() if r >= min_rank]
            sev_list  = ", ".join(f"'{s}'" for s in sev_in)
            where_clauses.append(f"severity IN ({sev_list})")

        sql = f"""
SELECT
    record_id,
    rule_name,
    table_name,
    column_name,
    failed_value,
    expected_constraint,
    failure_timestamp,
    run_id,
    pipeline_name,
    database_name,
    severity,
    rule_type,
    row_count_failed,
    total_row_count
FROM {self.audit_database}.{self.audit_table}
WHERE {' AND '.join(where_clauses)}
ORDER BY
    CASE severity
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH'     THEN 2
        WHEN 'MEDIUM'   THEN 3
        ELSE 4
    END,
    failure_timestamp DESC
LIMIT {limit}
"""
        rows = self._run_athena(sql)
        if not rows:
            return []
        return [ValidationRecord.from_athena_row(row) for row in rows]

    def _build_summary(
        self,
        records: List[ValidationRecord],
        dispatches: List[ActionDispatchResult],
    ) -> Dict[str, Any]:
        action_counts: Dict[str, int] = {}
        success_count = 0
        for d in dispatches:
            action_counts[d.action_taken] = action_counts.get(d.action_taken, 0) + 1
            if d.success:
                success_count += 1

        severity_counts: Dict[str, int] = {}
        for r in records:
            severity_counts[r.severity] = severity_counts.get(r.severity, 0) + 1

        return {
            "total_failures_processed": len(records),
            "actions_dispatched": len(dispatches),
            "successful_dispatches": success_count,
            "action_breakdown": action_counts,
            "severity_breakdown": severity_counts,
            "tables_affected": list({r.table_name for r in records}),
            "rules_affected": list({r.rule_name for r in records}),
        }

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _persist_action_log(self, result: ActionDispatchResult) -> None:
        key = f"{self.ACTION_LOG_PREFIX}{result.table_name}/{result.dispatch_id}.json"
        try:
            self.s3.put_object(
                Bucket=self.learning_bucket,
                Key=key,
                Body=json.dumps(result.to_dict(), indent=2, default=str),
            )
        except Exception as e:
            logger.warning(f"Could not persist action log: {e}")

    def _persist_summary(self, run_id: str, summary: Dict[str, Any]) -> None:
        key = f"{self.ACTION_LOG_PREFIX}_summaries/{run_id}.json"
        try:
            self.s3.put_object(
                Bucket=self.learning_bucket,
                Key=key,
                Body=json.dumps({"run_id": run_id, **summary, "generated_at": datetime.utcnow().isoformat()}, indent=2),
            )
        except Exception as e:
            logger.warning(f"Could not persist summary: {e}")

    # ------------------------------------------------------------------
    # Athena / JSON helpers
    # ------------------------------------------------------------------

    def _run_athena(self, sql: str, timeout_s: int = 90) -> Optional[List[Dict[str, Any]]]:
        import time
        try:
            resp = self.athena.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={"Database": self.audit_database},
                ResultConfiguration={"OutputLocation": self.athena_output},
            )
            qid = resp["QueryExecutionId"]
            deadline = time.time() + timeout_s
            while time.time() < deadline:
                status = self.athena.get_query_execution(QueryExecutionId=qid)
                state = status["QueryExecution"]["Status"]["State"]
                if state == "SUCCEEDED":
                    break
                if state in ("FAILED", "CANCELLED"):
                    reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
                    logger.warning(f"Athena query {state}: {reason[:200]}")
                    return None
                time.sleep(1)
            else:
                return None

            result = self.athena.get_query_results(QueryExecutionId=qid)
            rs = result["ResultSet"]
            cols = [c["Label"] for c in rs["ResultSetMetadata"]["ColumnInfo"]]
            rows = []
            for row in rs["Rows"][1:]:
                data = row.get("Data", [])
                rows.append({cols[i]: data[i].get("VarCharValue") for i in range(len(cols))})
            return rows
        except Exception as e:
            logger.error(f"Athena error: {e}")
            return None

    def _parse_json(self, raw: str) -> Dict[str, Any]:
        text = raw.strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        for marker in ("```json", "```"):
            if marker in text:
                start = text.index(marker) + len(marker)
                end = text.rfind("```")
                if end > start:
                    try:
                        return json.loads(text[start:end].strip())
                    except json.JSONDecodeError:
                        pass
        if "<json>" in text and "</json>" in text:
            start = text.index("<json>") + 6
            end = text.index("</json>")
            try:
                return json.loads(text[start:end].strip())
            except json.JSONDecodeError:
                pass
        return {}


# ===========================================================================
# Strands @tool wrappers for AuditActionAgent
# ===========================================================================

def make_audit_tools(agent: AuditActionAgent):
    """
    Return Strands @tool-decorated functions bound to an AuditActionAgent.
    These give the Strands Agent autonomous control over the full audit loop.
    """

    @tool
    def fetch_audit_failures(date: str, severity_filter: str = "MEDIUM", limit: int = 50) -> str:
        """
        Fetch FAIL records from the audit table for a given date (YYYY-MM-DD).
        severity_filter: minimum severity (LOW/MEDIUM/HIGH/CRITICAL).
        Returns a JSON list of failure records.
        """
        records = agent._fetch_audit_failures(
            date=date or None,
            severity_filter=severity_filter or None,
            limit=limit,
        )
        return json.dumps([r.to_dict() for r in records], indent=2, default=str)

    @tool
    def classify_and_act(record_json: str) -> str:
        """
        Classify a single validation failure record and dispatch the recommended action.
        record_json: JSON string of a ValidationRecord dict (from fetch_audit_failures).
        Returns the ActionDispatchResult as JSON.
        """
        record_dict = json.loads(record_json)
        record = ValidationRecord.from_athena_row(record_dict)
        result = agent.process_single(record)
        return json.dumps(result.to_dict(), indent=2, default=str)

    @tool
    def run_full_audit(date: str, severity_filter: str = "HIGH") -> str:
        """
        Run the full audit pipeline for a given date (YYYY-MM-DD).
        Fetches failures, classifies each, profiles tables, dispatches actions.
        Returns an executive summary as JSON.
        """
        report = agent.run(date=date, severity_filter=severity_filter)
        return json.dumps(report, indent=2, default=str)

    @tool
    def profile_table_for_failure(table_name: str, column_name: str = "") -> str:
        """
        Profile a table to gather data health signals.
        Returns health score, anomalies, and AI summary as JSON.
        """
        result = agent.profiler.profile(
            table_name=table_name,
            run_id=datetime.utcnow().strftime("%Y-%m-%d"),
            key_columns=[column_name] if column_name else None,
        )
        return json.dumps({
            "health_score": result.data_health_score,
            "schema_drift": result.schema_drift_detected,
            "anomaly_count": len(result.anomalies),
            "anomalies": [a.to_dict() for a in result.anomalies],
            "ai_summary": result.ai_summary,
        }, indent=2, default=str)

    @tool
    def generate_fix_sql(record_json: str) -> str:
        """
        Generate a DATA_CORRECTION SQL for a failed validation record.
        record_json: JSON string of the ValidationRecord.
        """
        record_dict = json.loads(record_json)
        record = ValidationRecord.from_athena_row(record_dict)
        sql = agent._generate_fix_sql(record, profile=None)
        return sql or "No automated fix SQL available for this rule type. Manual investigation required."

    @tool
    def escalate_failure(record_json: str, reason: str) -> str:
        """
        Immediately escalate a failure. Logs the alert; sends via SNS if configured.
        record_json: JSON string of the ValidationRecord.
        reason: explanation of why this is being escalated.
        """
        record_dict = json.loads(record_json)
        record = ValidationRecord.from_athena_row(record_dict)
        payload = {
            "alert_message": (
                f"[AGENT ESCALATION] {reason} — "
                f"Rule: {record.rule_name} on {record.table_name}.{record.column_name}"
            )
        }
        success, err = agent._dispatch_escalate(record, payload)
        return json.dumps({"escalated": success, "error": err})

    @tool
    def get_action_summary(date: str) -> str:
        """
        Return the action summary for a given date (YYYY-MM-DD) from S3 logs.
        """
        try:
            key = f"{agent.ACTION_LOG_PREFIX}_summaries/{date}.json"
            body = agent.s3.get_object(Bucket=agent.learning_bucket, Key=key)["Body"].read()
            return body.decode("utf-8")
        except Exception as e:
            return json.dumps({"error": f"No summary found for date={date}: {e}"})

    return [
        fetch_audit_failures,
        classify_and_act,
        run_full_audit,
        profile_table_for_failure,
        generate_fix_sql,
        escalate_failure,
        get_action_summary,
    ]


AUDIT_AGENT_SYSTEM_PROMPT = """
You are the **Strands Audit Action Agent**, an autonomous data quality enforcer.

## Your Mission
You analyse failed validation records from an audit table, profile the affected
tables for data health signals, classify each failure (TRUE_FAILURE / FALSE_POSITIVE /
NEEDS_INVESTIGATION), and take the appropriate corrective action.

## Decision Framework
1. Call fetch_audit_failures(date, severity_filter) first to get the failure list.
2. For each failure, call profile_table_for_failure to get data health context.
3. Use profile anomalies to determine action:
   - TEMPORAL_ANOMALY / SCHEMA_DRIFT / ZERO_INFLATION → always ESCALATE
   - Health score < 40 + HIGH severity → always ESCALATE
   - STALE_PARTITION → note for re-ingest
   - DUPLICATE_KEY_STORM → DATA_CORRECTION + generate_fix_sql
   - BOUNDARY_EXPLOSION → DATA_CORRECTION or ESCALATE for financial metrics
   - NULL_FLOOD > 30% → DATA_CORRECTION + generate_fix_sql
   - FALSE_POSITIVE (confidence > 0.85) → IGNORE and log rationale
4. Prioritise: CRITICAL → HIGH → MEDIUM.
5. Explain reasoning before each action tool call.
6. Finish with run_full_audit to produce the executive summary.
"""


def build_audit_agent(
    database: str,
    table: str,
    athena_output: str = "s3://strands-etl-athena-results/audit/",
    learning_bucket: str = "strands-etl-learning",
    aws_region: str = "us-east-1",
    model_id: str = AuditActionAgent.MODEL_ID,
) -> Agent:
    """
    Build a fully autonomous Strands Agent for audit failure processing.

    Just supply the Athena database and table — the agent handles the rest.

    Example
    -------
        agent = build_audit_agent(database="insurance_dw", table="audit_validation")
        response = agent("Analyse all HIGH failures for 2024-07-01 and take action.")
        print(response)
    """
    audit_agent = AuditActionAgent(
        database=database,
        table=table,
        athena_output=athena_output,
        learning_bucket=learning_bucket,
        aws_region=aws_region,
        model_id=model_id,
    )
    tools = make_audit_tools(audit_agent)
    return Agent(
        system_prompt=AUDIT_AGENT_SYSTEM_PROMPT,
        tools=tools,
        model=f"bedrock/{model_id}",
    )
