"""
validation/audit_action_agent.py
=================================
Strands SDK Agentic Audit Failure Analyzer + Action Dispatcher.

This module reads FAIL rows from the validation audit table (via Athena),
feeds each record through the ValidationAnalysisAgent for classification,
enriches the result with data profiling signals, then autonomously acts on
the AI recommendation using Strands @tool-decorated actions.

Architecture
------------
AuditActionAgent (Strands Agent)
  │
  ├── @tool fetch_audit_failures()      — read FAIL rows from Athena audit table
  ├── @tool classify_failure()          — run decision_agent on one record
  ├── @tool profile_failed_table()      — trigger DataProfilerAgent for context
  ├── @tool decide_final_action()       — LLM picks action using profile signals
  │
  ├── @tool action_rerun_glue_job()     — re-trigger the pipeline
  ├── @tool action_generate_fix_sql()   — produce DATA_CORRECTION SQL
  ├── @tool action_suggest_rule_fix()   — propose FIX_LOGIC rule change
  ├── @tool action_escalate()           — send SNS alert
  ├── @tool action_schedule_monitor()   — mark for follow-up
  ├── @tool action_ignore()             — log false-positive with rationale
  │
  └── @tool write_action_log()         — persist ActionDispatchResult to S3

Edge Cases Handled
------------------
- Null flood in a NOT_NULL column              → DATA_CORRECTION + fix SQL
- Cardinality explosion after a join           → FIX_LOGIC + query rewrite hint
- Temporal anomaly (timestamp rollback)        → ESCALATE immediately
- Distribution skew masks business KPI failure → MONITOR + alert threshold
- Schema drift on a CRITICAL table             → ESCALATE + halt downstream
- Stale partition breaches SLA                 → RERUN Glue job
- Duplicate key storm in a PK column           → ESCALATE + dedup SQL
- Boundary explosion (unit conversion bug)     → DATA_CORRECTION + fix SQL
- Zero inflation in revenue metrics            → ESCALATE (financial risk)
- Encoding rot in customer name fields         → FIX_LOGIC + re-ingest hint

Usage
-----
    from validation.audit_action_agent import AuditActionAgent, build_audit_agent

    # Option A — direct Python API
    agent = AuditActionAgent(
        audit_database="dq_db",
        audit_table="audit_validation",
        glue_job_name="etl_policy_master",
        sns_topic_arn="arn:aws:sns:us-east-1:123:dq-alerts",
    )
    report = agent.run(run_id="run-2024-07-01", severity_filter="HIGH")

    # Option B — Strands agentic loop (autonomous)
    strands_agent = build_audit_agent(audit_database="dq_db", audit_table="audit_validation")
    response = strands_agent(
        "Analyse all CRITICAL and HIGH failures from run-2024-07-01. "
        "Profile the affected tables, classify each failure, and take action. "
        "Escalate anything touching the policy_master table."
    )
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

    MODEL_ID         = "anthropic.claude-3-sonnet-20240229-v1:0"
    ACTION_LOG_PREFIX = "validation/audit/action_logs/"

    # Edge-case → escalation overrides (anomaly type forces ESCALATE regardless of AI)
    FORCE_ESCALATE_ANOMALIES = {
        AnomalyType.TEMPORAL_ANOMALY,
        AnomalyType.SCHEMA_DRIFT,
        AnomalyType.ZERO_INFLATION,
    }

    def __init__(
        self,
        audit_database: str = "default",
        audit_table: str = "audit_validation",
        glue_job_name: str = "",
        sns_topic_arn: str = "",
        learning_bucket: str = "strands-etl-learning",
        athena_output: str = "s3://strands-etl-athena-results/audit/",
        aws_region: str = "us-east-1",
        model_id: str = MODEL_ID,
    ):
        self.audit_database  = audit_database
        self.audit_table     = audit_table
        self.glue_job_name   = glue_job_name
        self.sns_topic_arn   = sns_topic_arn
        self.learning_bucket = learning_bucket
        self.athena_output   = athena_output
        self.model_id        = model_id
        self.aws_region      = aws_region

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
            learning_bucket=learning_bucket,
            aws_region=aws_region,
            model_id=model_id,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        run_id: Optional[str] = None,
        severity_filter: Optional[str] = None,   # e.g. "HIGH" → HIGH + CRITICAL
        limit: int = 100,
    ) -> Dict[str, Any]:
        """
        Load audit failures, analyze each, dispatch actions, and return a summary.
        """
        logger.info(f"[AuditActionAgent] Starting audit run: run_id={run_id}, severity={severity_filter}")

        # ── Step 1: Load failures from audit table
        records = self._fetch_audit_failures(run_id=run_id, severity_filter=severity_filter, limit=limit)
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
        self._persist_summary(run_id or "unknown", summary)

        return {
            "run_id": run_id,
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
    ) -> ActionDispatchResult:
        logger.info(f"[AuditActionAgent] Processing record {record.record_id} ({record.rule_name})")

        # Step A: AI classification
        analysis: AnalysisResult = self.validation_agent.analyze(record)

        # Step B: Data profile for the affected table (cached per table)
        table_key = f"{record.database_name}.{record.table_name}"
        if table_key not in profile_cache:
            self.profiler.database = record.database_name or self.audit_database
            try:
                profile = self.profiler.profile(
                    table_name=record.table_name,
                    run_id=record.run_id,
                    key_columns=[record.column_name] if record.column_name else None,
                    primary_key_column=record.additional_context.get("primary_key_column"),
                    metric_columns=record.additional_context.get("metric_columns"),
                    timestamp_column=record.additional_context.get("timestamp_column"),
                    freshness_sla_hours=record.additional_context.get("freshness_sla_hours", 26.0),
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
        job_name = payload.get("rerun_job") or self.glue_job_name
        if not job_name:
            logger.warning("RERUN requested but no Glue job name configured")
            return False, "No Glue job name configured"
        try:
            resp = self.glue.start_job_run(
                JobName=job_name,
                Arguments={
                    "--run_id":     record.run_id,
                    "--table_name": record.table_name,
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
        if not self.sns_topic_arn:
            logger.warning("ESCALATE action but no SNS topic ARN configured; logging only")
            return True, ""
        message = payload.get("alert_message", f"DQ failure requires attention: {record.rule_name}")
        subject = f"[DQ ESCALATION] {record.severity}: {record.rule_name} on {record.table_name}"
        try:
            self.sns.publish(
                TopicArn=self.sns_topic_arn,
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
        key = f"validation/audit/ignored/{record.run_id}/{record.record_id}.json"
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
    # Audit table helpers
    # ------------------------------------------------------------------

    def _fetch_audit_failures(
        self,
        run_id: Optional[str],
        severity_filter: Optional[str],
        limit: int,
    ) -> List[ValidationRecord]:
        """Query the Athena audit_validation table for FAIL records."""
        severity_rank = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
        min_rank = severity_rank.get(severity_filter, 1) if severity_filter else 1

        where_clauses = ["status = 'FAIL'"]
        if run_id:
            where_clauses.append(f"run_id = '{run_id}'")

        # Filter by minimum severity (Athena doesn't support CASE in WHERE without CTE)
        if min_rank > 1:
            sev_values = [s for s, r in severity_rank.items() if r >= min_rank]
            sev_list = ", ".join(f"'{s}'" for s in sev_values)
            where_clauses.append(f"severity IN ({sev_list})")

        sql = f"""
SELECT
    record_id, rule_name, table_name, column_name,
    failed_value, expected_constraint, failure_timestamp,
    run_id, pipeline_name, database_name,
    severity, rule_type,
    row_count_failed, total_row_count
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
    def fetch_audit_failures(run_id: str = "", severity_filter: str = "MEDIUM", limit: int = 50) -> str:
        """
        Fetch FAIL records from the validation audit table.
        severity_filter: minimum severity level (LOW/MEDIUM/HIGH/CRITICAL).
        Returns JSON list of failure records.
        """
        records = agent._fetch_audit_failures(
            run_id=run_id or None,
            severity_filter=severity_filter or None,
            limit=limit,
        )
        return json.dumps([r.to_dict() for r in records], indent=2, default=str)

    @tool
    def classify_and_act(record_json: str) -> str:
        """
        Classify a single validation failure record and dispatch the recommended action.
        record_json: JSON string of a ValidationRecord dict (from fetch_audit_failures output).
        Returns the ActionDispatchResult.
        """
        record_dict = json.loads(record_json)
        record = ValidationRecord.from_athena_row(record_dict)
        result = agent.process_single(record)
        return json.dumps(result.to_dict(), indent=2, default=str)

    @tool
    def run_full_audit(run_id: str, severity_filter: str = "HIGH") -> str:
        """
        Run the complete audit analysis pipeline for a given ETL run.
        Fetches failures, classifies each, profiles affected tables, dispatches actions.
        Returns an executive summary.
        """
        report = agent.run(run_id=run_id, severity_filter=severity_filter)
        return json.dumps(report, indent=2, default=str)

    @tool
    def profile_table_for_failure(table_name: str, run_id: str, column_name: str = "") -> str:
        """
        Profile a specific table to gather data health signals for a failure context.
        Returns DataProfileResult summary with anomalies and health score.
        """
        agent.profiler.database = agent.audit_database
        result = agent.profiler.profile(
            table_name=table_name,
            run_id=run_id,
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
        Generate a DATA_CORRECTION SQL statement for a failed validation record.
        record_json: JSON string of the ValidationRecord.
        Returns the fix SQL or an explanation of why no SQL was generated.
        """
        record_dict = json.loads(record_json)
        record = ValidationRecord.from_athena_row(record_dict)
        sql = agent._generate_fix_sql(record, profile=None)
        return sql or "No automated fix SQL available for this rule type. Manual investigation required."

    @tool
    def escalate_failure(record_json: str, reason: str) -> str:
        """
        Immediately escalate a failure via SNS. Use when the situation is critical.
        record_json: JSON string of the ValidationRecord.
        reason: why this is being escalated.
        """
        record_dict = json.loads(record_json)
        record = ValidationRecord.from_athena_row(record_dict)
        payload = {"alert_message": f"[AGENT ESCALATION] {reason} — Record: {record.rule_name} on {record.table_name}"}
        success, err = agent._dispatch_escalate(record, payload)
        return json.dumps({"escalated": success, "error": err})

    @tool
    def get_action_summary(run_id: str) -> str:
        """
        Return a summary of all actions taken for a specific ETL run from S3 logs.
        """
        try:
            key = f"{agent.ACTION_LOG_PREFIX}_summaries/{run_id}.json"
            body = agent.s3.get_object(Bucket=agent.learning_bucket, Key=key)["Body"].read()
            return body.decode("utf-8")
        except Exception as e:
            return json.dumps({"error": f"No summary found for run_id={run_id}: {e}"})

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
You analyse failed validation records from the audit_validation table, profile the
affected tables for data health signals, classify each failure (TRUE_FAILURE /
FALSE_POSITIVE / NEEDS_INVESTIGATION), and take the appropriate corrective action.

## Decision Framework
1. Always fetch failures first, then profile the affected table.
2. Use profile anomalies to inform severity upgrades:
   - TEMPORAL_ANOMALY / SCHEMA_DRIFT / ZERO_INFLATION → always ESCALATE
   - Health score < 40 + HIGH severity → always ESCALATE
   - STALE_PARTITION → RERUN the Glue job
   - DUPLICATE_KEY_STORM → DATA_CORRECTION + generate fix SQL
   - BOUNDARY_EXPLOSION → DATA_CORRECTION or ESCALATE if financial metric
   - NULL_FLOOD > 30% → DATA_CORRECTION
   - FALSE_POSITIVE (confidence > 0.85) → IGNORE and log rationale
3. Prioritise by: CRITICAL → HIGH → MEDIUM.
4. Always explain your reasoning before calling a dispatch tool.
5. After acting on all failures, call run_full_audit to produce a summary.
"""


def build_audit_agent(
    audit_database: str = "default",
    audit_table: str = "audit_validation",
    glue_job_name: str = "",
    sns_topic_arn: str = "",
    learning_bucket: str = "strands-etl-learning",
    aws_region: str = "us-east-1",
    model_id: str = AuditActionAgent.MODEL_ID,
) -> Agent:
    """
    Build and return a fully autonomous Strands Agent for audit failure processing.

    The agent uses the Strands agentic loop — it can call multiple tools in
    sequence, reason over results, and decide what to do next without any
    hardcoded orchestration.

    Example
    -------
        agent = build_audit_agent(
            audit_database="insurance_dw",
            audit_table="audit_validation",
            glue_job_name="etl_policy_master",
            sns_topic_arn="arn:aws:sns:us-east-1:123:dq-alerts",
        )
        response = agent(
            "Analyse all HIGH and CRITICAL failures from run-2024-07-01. "
            "Profile the affected tables. Escalate anything on policy_master or "
            "claims_summary. Generate fix SQL for any DATA_CORRECTION actions."
        )
        print(response)
    """
    audit_agent = AuditActionAgent(
        audit_database=audit_database,
        audit_table=audit_table,
        glue_job_name=glue_job_name,
        sns_topic_arn=sns_topic_arn,
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
