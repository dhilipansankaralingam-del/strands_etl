###################################################################################################
# SCRIPT        : etl_agent_analyzer.py
# PURPOSE       : Strands ETL Agent Analyzer
#                   1) Read unanalysed FAIL rows from audit_validation (Athena)
#                   2) Enrich each record with AI classification via ValidationAnalysisAgent
#                      (Amazon Bedrock / Claude 3 Sonnet) — populates ai_classification,
#                      ai_confidence, ai_recommended_action, ai_explanation, ai_analysed_at
#                   3) Write enriched records back to S3 (audit_validation/enriched/)
#                   4) Generate batch executive summary and (optionally) extract learning patterns
#                   5) Write pipe-delimited audit log to S3
#                   6) Send HTML dashboard email
#
# RUN MODES     : analyze_existing  – read FAIL rows from audit_validation and enrich with AI
#                 full_pipeline     – run table_validator first then enrich failures
#
# USAGE (local) : python pyscript/etl_agent_analyzer.py \
#                     --config config/agent_analyzer_config.json
#
# USAGE (S3)    : python pyscript/etl_agent_analyzer.py \
#                     --config s3://strands-etl-configs/agent_analyzer_config.json
#
# USAGE (Glue)  : python pyscript/etl_agent_analyzer.py --glue-mode
#
#=================================================================================================
# DATE          BY              MODIFICATION LOG
# ----------    -----------     ------------------------------------------
# 04/2026       Dhilipan        Initial version
#=================================================================================================
###################################################################################################

import argparse
import base64
import io
import json
import logging
import os
import smtplib
import sys
import time
import uuid
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional, Tuple

import boto3

# Optional matplotlib for confidence distribution chart
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("etl_agent_analyzer")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
VERSION = "1.1.0"

CLASSIFICATION_COLORS = {
    "TRUE_FAILURE":         "#dc3545",
    "FALSE_POSITIVE":       "#28a745",
    "NEEDS_INVESTIGATION":  "#ffc107",
    "":                     "#6c757d",
}

SEVERITY_RANK = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def _load_config(path: str) -> Dict[str, Any]:
    """Load JSON config from a local path or s3://bucket/key."""
    if path.startswith("s3://"):
        bucket, key = path[5:].split("/", 1)
        s3 = boto3.client("s3")
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        return json.loads(body)
    with open(path) as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Thin Athena runner (standalone — no dependency on validation package internals)
# ---------------------------------------------------------------------------

class _AthenaRunner:
    def __init__(self, s3_output: str, workgroup: str, region: str,
                 poll_interval: int = 5, max_polls: int = 60):
        self.s3_output     = s3_output
        self.workgroup     = workgroup
        self.poll_interval = poll_interval
        self.max_polls     = max_polls
        self.athena        = boto3.client("athena", region_name=region)

    def run(self, sql: str, database: str) -> Tuple[List[str], List[Dict[str, str]]]:
        exec_id = self.athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": self.s3_output},
            WorkGroup=self.workgroup,
        )["QueryExecutionId"]

        for _ in range(self.max_polls):
            resp  = self.athena.get_query_execution(QueryExecutionId=exec_id)
            state = resp["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                break
            if state in ("FAILED", "CANCELLED"):
                reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
                raise RuntimeError(f"Athena query {state}: {reason}\nSQL: {sql[:300]}")
            time.sleep(self.poll_interval)
        else:
            raise RuntimeError(
                f"Athena query timed out after {self.max_polls * self.poll_interval}s"
            )

        result = self.athena.get_query_results(QueryExecutionId=exec_id)
        cols   = [c["Label"] for c in result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
        rows   = [
            {cols[i]: d.get("VarCharValue", "") for i, d in enumerate(row.get("Data", []))}
            for row in result["ResultSet"]["Rows"][1:]
        ]
        return cols, rows


# ---------------------------------------------------------------------------
# Core analyzer class
# ---------------------------------------------------------------------------

class AgentAnalyzer:
    """
    End-to-end Strands ETL Agent Analyzer.

    Reads FAIL records from audit_validation, enriches them with AI analysis
    (Bedrock / Claude 3 Sonnet via ValidationAnalysisAgent), writes results to
    S3, and sends an HTML report email.
    """

    def __init__(self, config: Dict[str, Any]):
        self.cfg        = config
        self.run_id     = str(uuid.uuid4())
        self.run_start  = datetime.utcnow()
        self.region     = config.get("aws", {}).get("region", "us-east-1")

        ath_cfg  = config.get("athena", {})
        self.athena = _AthenaRunner(
            s3_output     = ath_cfg.get("s3_output", "s3://strands-etl-athena-results/agent_analyzer/"),
            workgroup     = ath_cfg.get("workgroup", "primary"),
            region        = self.region,
            poll_interval = ath_cfg.get("poll_interval_seconds", 5),
            max_polls     = ath_cfg.get("max_polls", 60),
        )
        self.audit_database = ath_cfg.get("database", "data_quality_db")
        self.audit_table    = ath_cfg.get("audit_table", "audit_validation")

        out_cfg         = config.get("output", {})
        self.out_bucket = out_cfg.get("s3_bucket", "strands-etl-audit")

        self.s3 = boto3.client("s3", region_name=self.region)

        # Cost tracker — shared with the agent so all Bedrock calls roll up here
        try:
            sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
            from validation.cost_tracker import BedrockCostTracker
            self.cost_tracker = BedrockCostTracker(run_id=self.run_id)
        except ImportError:
            self.cost_tracker = None

        # Holiday calendar — loaded once, used to annotate Z-score anomalies
        self.holiday_calendar = self._load_holiday_calendar(
            config.get("holiday_calendar_path", "config/holiday_calendar.json")
        )

        # Lazy-import the Strands agent
        self._agent = None
        # Per-step cost snapshots for the HTML report
        self._step_costs: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> Dict[str, Any]:
        """
        Execute the full analysis pipeline.

        Returns a report dict with validation_summary, ai_analysis,
        enriched_records, patterns (optional), and audit metadata.
        """
        analysis_cfg  = self.cfg.get("analysis", {})
        run_mode      = self.cfg.get("analyzer", {}).get("run_mode", "analyze_existing")

        logger.info("=" * 70)
        logger.info("Strands ETL Agent Analyzer v%s  run_id=%s", VERSION, self.run_id)
        logger.info("run_mode=%s", run_mode)
        logger.info("=" * 70)

        val_summary: Dict[str, Any] = {}

        # ── Step 1: optionally run table_validator first ─────────────────
        if run_mode == "full_pipeline":
            val_summary = self._run_table_validator()

        # ── Step 2: fetch unanalysed FAIL records from Athena ────────────
        logger.info("Step 2: Fetching unanalysed FAIL records from audit_validation …")
        records = self._fetch_unanalyzed_records(analysis_cfg)
        logger.info("  Found %d records to analyse", len(records))

        if not records:
            logger.info("  No records to analyse – exiting cleanly.")
            report = self._build_report(val_summary, [], {}, {})
            self._write_audit_log(report)
            self._maybe_send_email(report)
            return report

        # ── Step 3: AI enrichment (batched) ──────────────────────────────
        logger.info("Step 3: Running Strands AI enrichment …")
        self._get_agent().set_step_label("Step 3 – AI enrichment")
        enriched, analysis_results = self._analyze_in_batches(
            records, analysis_cfg.get("batch_size", 50)
        )
        self._snapshot_step_cost("Step 3 – AI enrichment (decision agent)")

        # ── Step 4: Write enriched records to S3 ─────────────────────────
        logger.info("Step 4: Writing enriched records to S3 …")
        enriched_s3_uri = self._write_enriched(enriched)
        logger.info("  Written → %s", enriched_s3_uri)

        # ── Step 5: Batch executive summary ──────────────────────────────
        logger.info("Step 5: Generating batch executive summary …")
        self._get_agent().set_step_label("Step 5 – Batch summary")
        batch_summary = self._batch_summary(analysis_results)
        self._snapshot_step_cost("Step 5 – Batch summary (batch agent)")

        # ── Step 6: (optional) Pattern extraction ─────────────────────────
        patterns: Dict[str, Any] = {}
        if analysis_cfg.get("extract_patterns", False):
            logger.info("Step 6: Extracting learning patterns …")
            self._get_agent().set_step_label("Step 6 – Pattern extraction")
            patterns = self._extract_patterns()
            self._snapshot_step_cost("Step 6 – Pattern extraction (learning agent)")

        # ── Step 7: Write audit log ───────────────────────────────────────
        logger.info("Step 7: Writing audit log …")
        report = self._build_report(val_summary, enriched, batch_summary, patterns)
        self._write_audit_log(report)

        # ── Step 8: Send email report ─────────────────────────────────────
        logger.info("Step 8: Sending HTML email report …")
        self._maybe_send_email(report)

        # ── Final cost summary ────────────────────────────────────────────
        if self.cost_tracker:
            logger.info("\n%s", self.cost_tracker.render_table())

        logger.info("=" * 70)
        logger.info("AgentAnalyzer COMPLETE  run_id=%s", self.run_id)
        logger.info("  analysed=%d  true_failures=%d  false_positives=%d  needs_investigation=%d",
                    len(enriched),
                    sum(1 for r in enriched if r.get("ai_classification") == "TRUE_FAILURE"),
                    sum(1 for r in enriched if r.get("ai_classification") == "FALSE_POSITIVE"),
                    sum(1 for r in enriched if r.get("ai_classification") == "NEEDS_INVESTIGATION"),
                    )
        if self.cost_tracker:
            cs = self.cost_tracker.get_summary()
            logger.info("  total_tokens=%d  total_cost_usd=$%.6f",
                        cs.get("total_tokens", 0), cs.get("total_cost_usd", 0.0))
        logger.info("=" * 70)
        return report

    # ------------------------------------------------------------------
    # Step 1 helper: full-pipeline mode
    # ------------------------------------------------------------------

    def _run_table_validator(self) -> Dict[str, Any]:
        """Invoke TableValidator.run() and return its summary dict."""
        try:
            sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
            from pyscript.table_validator import TableValidator
        except ImportError:
            from table_validator import TableValidator

        fp_cfg   = self.cfg.get("full_pipeline", {})
        ath_cfg  = self.cfg.get("athena", {})
        validator = TableValidator(
            config_path   = fp_cfg.get("validation_config_path", ""),
            s3_output     = ath_cfg.get("s3_output", ""),
            pipeline_name = fp_cfg.get("pipeline_name", "default_pipeline"),
            database      = ath_cfg.get("database", "data_quality_db"),
            workgroup     = ath_cfg.get("workgroup", "primary"),
            region        = self.region,
        )
        logger.info("Step 1: Running table_validator …")
        summary = validator.run()
        logger.info(
            "  Validation complete. PASS=%d FAIL=%d ERROR=%d",
            summary.get("passed", 0), summary.get("failed", 0), summary.get("errors", 0),
        )
        return summary

    # ------------------------------------------------------------------
    # Step 2: fetch records
    # ------------------------------------------------------------------

    def _fetch_unanalyzed_records(self, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Query audit_validation for FAIL rows that have not yet been AI-analysed.
        Applies look_back_hours, pipeline_filters, severity_filters, run_id_filter,
        and skip_already_analysed settings from the config.
        """
        look_back   = cfg.get("look_back_hours", 24)
        max_records = cfg.get("max_records", 500)
        skip_done   = cfg.get("skip_already_analysed", True)
        run_id_flt  = cfg.get("run_id_filter", "")
        pipelines   = cfg.get("pipeline_filters", [])
        severities  = cfg.get("severity_filters", [])

        conditions = [
            "status = 'FAIL'",
            f"failure_timestamp >= NOW() - INTERVAL '{look_back}' HOUR",
        ]
        if skip_done:
            conditions.append("(ai_classification IS NULL OR ai_classification = '')")
        if run_id_flt:
            conditions.append(f"run_id = '{run_id_flt}'")
        if pipelines:
            quoted = ", ".join(f"'{p}'" for p in pipelines)
            conditions.append(f"pipeline_name IN ({quoted})")
        if severities:
            quoted = ", ".join(f"'{s}'" for s in severities)
            conditions.append(f"severity IN ({quoted})")

        where = " AND ".join(conditions)
        sql = f"""
            SELECT *
            FROM   {self.audit_database}.{self.audit_table}
            WHERE  {where}
            ORDER  BY
                   CASE severity
                     WHEN 'CRITICAL' THEN 0
                     WHEN 'HIGH'     THEN 1
                     WHEN 'MEDIUM'   THEN 2
                     ELSE 3
                   END,
                   failure_timestamp DESC
            LIMIT  {max_records}
        """

        try:
            _, rows = self.athena.run(sql, self.audit_database)
            return rows
        except RuntimeError as exc:
            logger.error("Failed to fetch audit records: %s", exc)
            return []

    # ------------------------------------------------------------------
    # Step 3: AI enrichment
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Holiday calendar helpers
    # ------------------------------------------------------------------

    def _load_holiday_calendar(self, path: str) -> Dict[str, Any]:
        """Load holiday_calendar.json from local path or S3."""
        try:
            if path.startswith("s3://"):
                bucket, key = path[5:].split("/", 1)
                body = self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()
                return json.loads(body)
            if os.path.exists(path):
                with open(path) as fh:
                    return json.load(fh)
        except Exception as exc:
            logger.warning("Could not load holiday calendar from %s: %s", path, exc)
        return {}

    def get_holiday_context(self, date_str: str) -> Optional[Dict[str, Any]]:
        """
        Return the holiday entry for a given YYYY-MM-DD date string, or None.
        Checks the current year and previous year explicit_dates maps.
        """
        if not self.holiday_calendar or not date_str:
            return None
        date_part = str(date_str)[:10]
        year = date_part[:4]
        for year_key in (f"explicit_dates_{year}", f"explicit_dates_{int(year)-1}"):
            entry = self.holiday_calendar.get(year_key, {}).get(date_part)
            if entry:
                return entry
        return None

    # ------------------------------------------------------------------
    # Cost snapshot helper
    # ------------------------------------------------------------------

    def _snapshot_step_cost(self, label: str) -> None:
        """Record a cumulative cost snapshot after each pipeline step."""
        if not self.cost_tracker:
            return
        cs = self.cost_tracker.get_summary()
        self._step_costs.append({
            "step":           label,
            "cumulative_cost_usd": cs.get("total_cost_usd", 0.0),
            "cumulative_tokens":   cs.get("total_tokens", 0),
            "calls_so_far":        cs.get("total_calls", 0),
        })

    # ------------------------------------------------------------------

    def _get_agent(self):
        """Lazy-initialise the ValidationAnalysisAgent, sharing the cost tracker."""
        if self._agent is None:
            try:
                from validation.validation_agent import ValidationAnalysisAgent
            except ImportError:
                sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
                from validation.validation_agent import ValidationAnalysisAgent

            ai_cfg = self.cfg.get("ai", {})
            self._agent = ValidationAnalysisAgent(
                learning_bucket = ai_cfg.get("learning_bucket", "strands-etl-learning"),
                aws_region      = self.region,
                model_id        = ai_cfg.get("model_id", "anthropic.claude-3-sonnet-20240229-v1:0"),
                cost_tracker    = self.cost_tracker,
            )
        return self._agent

    def _analyze_in_batches(
        self,
        rows: List[Dict[str, Any]],
        batch_size: int,
    ) -> Tuple[List[Dict[str, Any]], List[Any]]:
        """
        Process rows through ValidationAnalysisAgent in batches of *batch_size*.
        Returns (enriched_rows, analysis_result_objects).
        """
        try:
            from validation.models import ValidationRecord
        except ImportError:
            sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
            from validation.models import ValidationRecord

        agent          = self._get_agent()
        enriched_rows  = []
        result_objects = []

        total  = len(rows)
        done   = 0

        for i in range(0, total, batch_size):
            batch = rows[i : i + batch_size]
            logger.info(
                "  Batch %d/%d: processing %d records …",
                i // batch_size + 1,
                (total + batch_size - 1) // batch_size,
                len(batch),
            )
            for raw_row in batch:
                record = ValidationRecord.from_athena_row(raw_row)
                # Inject holiday context so the agent can factor it into classification
                holiday_ctx = self.get_holiday_context(
                    str(raw_row.get("failure_timestamp", ""))[:10]
                )
                if holiday_ctx:
                    record.additional_context["holiday"] = holiday_ctx
                    logger.debug(
                        "  Holiday context injected for %s: %s",
                        raw_row.get("record_id", "?"), holiday_ctx.get("holiday", ""),
                    )
                try:
                    result = agent.analyze(record)
                    ai_cols = {
                        "ai_classification":     result.classification.value,
                        "ai_confidence":         round(result.confidence, 4),
                        "ai_recommended_action": result.recommended_action.value,
                        "ai_explanation":        result.explanation,
                        "ai_analysed_at":        result.timestamp,
                        "ai_root_causes":        json.dumps(result.root_causes),
                        "ai_next_steps":         json.dumps(result.suggested_next_steps),
                        "ai_validation_sql":     result.validation_sql or "",
                    }
                    result_objects.append(result)
                except Exception as exc:
                    logger.error(
                        "  Agent failed for record %s: %s",
                        raw_row.get("record_id", "?"), exc,
                    )
                    ai_cols = {
                        "ai_classification":     "NEEDS_INVESTIGATION",
                        "ai_confidence":         0.0,
                        "ai_recommended_action": "ESCALATE",
                        "ai_explanation":        f"Agent error: {exc}",
                        "ai_analysed_at":        datetime.utcnow().isoformat(),
                        "ai_root_causes":        "[]",
                        "ai_next_steps":         "[]",
                        "ai_validation_sql":     "",
                    }

                enriched = dict(raw_row)
                enriched.update(ai_cols)
                enriched_rows.append(enriched)
                done += 1

            logger.info("  Progress: %d/%d records analysed", done, total)

        return enriched_rows, result_objects

    # ------------------------------------------------------------------
    # Step 4: write enriched records
    # ------------------------------------------------------------------

    def _write_enriched(self, enriched: List[Dict[str, Any]]) -> str:
        """Persist enriched records to S3 as newline-delimited JSON."""
        out_cfg = self.cfg.get("output", {})
        prefix  = out_cfg.get("enriched_prefix", "audit_validation/enriched/").rstrip("/")
        today   = self.run_start.strftime("%Y-%m-%d")
        key     = f"{prefix}/dt={today}/run_id={self.run_id}/enriched.json"
        body    = "\n".join(json.dumps(r, default=str) for r in enriched)

        self.s3.put_object(
            Bucket      = self.out_bucket,
            Key         = key,
            Body        = body.encode("utf-8"),
            ContentType = "application/json",
        )
        return f"s3://{self.out_bucket}/{key}"

    # ------------------------------------------------------------------
    # Step 5: batch summary
    # ------------------------------------------------------------------

    def _batch_summary(self, results: List[Any]) -> Dict[str, Any]:
        """Use the agent's batch_agent to generate an executive summary."""
        if not results:
            return {}
        try:
            return self._get_agent().batch_agent(results)
        except Exception as exc:
            logger.warning("Batch summary failed: %s", exc)
            return {"error": str(exc)}

    # ------------------------------------------------------------------
    # Step 6: pattern extraction
    # ------------------------------------------------------------------

    def _extract_patterns(self) -> Dict[str, Any]:
        """Run the learning_agent over recent resolved outcomes."""
        try:
            patterns = self._get_agent().extract_patterns(limit=100)
            # Persist patterns to S3
            out_cfg  = self.cfg.get("output", {})
            prefix   = out_cfg.get("patterns_prefix", "agent_analyzer/patterns/").rstrip("/")
            today    = self.run_start.strftime("%Y-%m-%d")
            key      = f"{prefix}/dt={today}/{self.run_id}.json"
            self.s3.put_object(
                Bucket      = self.out_bucket,
                Key         = key,
                Body        = json.dumps(patterns, indent=2, default=str).encode("utf-8"),
                ContentType = "application/json",
            )
            logger.info("Patterns persisted → s3://%s/%s", self.out_bucket, key)
            return patterns
        except Exception as exc:
            logger.warning("Pattern extraction failed: %s", exc)
            return {"error": str(exc)}

    # ------------------------------------------------------------------
    # Step 7: build report dict
    # ------------------------------------------------------------------

    def _build_report(
        self,
        val_summary:  Dict[str, Any],
        enriched:     List[Dict[str, Any]],
        batch_result: Dict[str, Any],
        patterns:     Dict[str, Any],
    ) -> Dict[str, Any]:
        true_failures  = sum(1 for r in enriched if r.get("ai_classification") == "TRUE_FAILURE")
        false_positives = sum(1 for r in enriched if r.get("ai_classification") == "FALSE_POSITIVE")
        needs_inv      = sum(1 for r in enriched if r.get("ai_classification") == "NEEDS_INVESTIGATION")
        analysed       = len(enriched)

        cost_summary = self.cost_tracker.get_summary() if self.cost_tracker else {}

        report: Dict[str, Any] = {
            "run_id":           self.run_id,
            "analyzer_version": VERSION,
            "run_start":        self.run_start.isoformat(),
            "run_end":          datetime.utcnow().isoformat(),
            "validation_summary": val_summary,
            "ai_analysis": {
                "total_analysed":      analysed,
                "true_failures":       true_failures,
                "false_positives":     false_positives,
                "needs_investigation": needs_inv,
                "avg_confidence":      (
                    round(
                        sum(r.get("ai_confidence", 0.0) for r in enriched) / analysed, 4
                    ) if analysed else 0.0
                ),
                "executive_summary":   batch_result,
            },
            "cost_summary":     cost_summary,
            "step_costs":       self._step_costs,
            "enriched_records": enriched,
            "patterns":         patterns,
            "timestamp":        datetime.utcnow().isoformat(),
        }
        return report

    # ------------------------------------------------------------------
    # Step 7: audit log
    # ------------------------------------------------------------------

    def _write_audit_log(self, report: Dict[str, Any]) -> None:
        """Write a pipe-delimited audit record to S3."""
        audit_cfg = self.cfg.get("audit", {})
        bucket    = audit_cfg.get("s3_bucket", "strands-etl-audit")
        prefix    = audit_cfg.get("s3_prefix", "audit_logs/agent_analyzer/").rstrip("/")
        today     = self.run_start.strftime("%Y-%m-%d")

        ai   = report.get("ai_analysis", {})
        cost = report.get("cost_summary", {})
        fields = [
            self.run_id,
            self.run_start.strftime("%Y-%m-%d %H:%M:%S"),
            str(ai.get("total_analysed", 0)),
            str(ai.get("true_failures", 0)),
            str(ai.get("false_positives", 0)),
            str(ai.get("needs_investigation", 0)),
            str(ai.get("avg_confidence", 0.0)),
            str(cost.get("total_input_tokens", 0)),
            str(cost.get("total_output_tokens", 0)),
            str(cost.get("total_cost_usd", 0.0)),
            VERSION,
        ]
        header = ("run_id|run_start|total_analysed|true_failures|false_positives"
                  "|needs_investigation|avg_confidence"
                  "|input_tokens|output_tokens|cost_usd|version\n")
        line   = "|".join(fields) + "\n"

        key = f"{prefix}/run_date={today}/{self.run_id}.txt"
        try:
            self.s3.put_object(
                Bucket      = bucket,
                Key         = key,
                Body        = (header + line).encode("utf-8"),
                ContentType = "text/plain",
            )
            logger.info("Audit log written → s3://%s/%s", bucket, key)
        except Exception as exc:
            logger.warning("Could not write audit log: %s", exc)

    # ------------------------------------------------------------------
    # Step 8: email
    # ------------------------------------------------------------------

    def _maybe_send_email(self, report: Dict[str, Any]) -> None:
        email_cfg = self.cfg.get("email", {})
        if not email_cfg.get("enabled", True):
            logger.info("Email disabled in config – skipping.")
            return
        recipients = email_cfg.get("recipients", [])
        if not recipients:
            logger.info("No email recipients configured – skipping.")
            return

        ai     = report.get("ai_analysis", {})
        tf     = ai.get("true_failures", 0)
        fp     = ai.get("false_positives", 0)
        ni     = ai.get("needs_investigation", 0)
        total  = ai.get("total_analysed", 0)

        status_tag = "✅ CLEAN" if tf == 0 else f"⚠ {tf} TRUE FAILURE(S)"
        subject = (
            f"{email_cfg.get('subject_prefix', 'Strands ETL Agent Analyzer')} | "
            f"{status_tag} | {self.run_start.strftime('%Y-%m-%d %H:%M')} UTC"
        )

        html  = self._build_html_report(report)
        msg   = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = email_cfg.get("sender", "")
        msg["To"]      = ", ".join(recipients)
        msg.attach(MIMEText(html, "html"))

        try:
            with smtplib.SMTP(
                email_cfg.get("smtp_host", "localhost"),
                int(email_cfg.get("smtp_port", 25)),
            ) as server:
                server.sendmail(email_cfg["sender"], recipients, msg.as_string())
            logger.info("Email sent to %s", recipients)
        except Exception as exc:
            logger.warning("Email delivery failed: %s", exc)

    # ------------------------------------------------------------------
    # HTML report builder
    # ------------------------------------------------------------------

    def _build_html_report(self, report: Dict[str, Any]) -> str:  # noqa: C901
        ai      = report.get("ai_analysis", {})
        tf      = ai.get("true_failures", 0)
        fp      = ai.get("false_positives", 0)
        ni      = ai.get("needs_investigation", 0)
        total   = ai.get("total_analysed", 0)
        avg_conf = ai.get("avg_confidence", 0.0)
        exec_sum = ai.get("executive_summary", {})
        enriched = report.get("enriched_records", [])

        # ── confidence distribution chart ────────────────────────────────
        chart_html = ""
        if HAS_MATPLOTLIB and enriched:
            try:
                confidences = [r.get("ai_confidence", 0.0) for r in enriched if r.get("ai_confidence") is not None]
                fig, ax = plt.subplots(figsize=(6, 3))
                ax.hist(confidences, bins=10, range=(0, 1), color="#4e73df", edgecolor="white")
                ax.set_xlabel("AI Confidence Score")
                ax.set_ylabel("Record Count")
                ax.set_title("Confidence Score Distribution")
                ax.set_xlim(0, 1)
                plt.tight_layout()
                buf = io.BytesIO()
                plt.savefig(buf, format="png", dpi=100)
                plt.close(fig)
                buf.seek(0)
                img_b64 = base64.b64encode(buf.read()).decode()
                chart_html = f'<img src="data:image/png;base64,{img_b64}" style="max-width:100%;border-radius:6px;margin:10px 0;" />'
            except Exception as exc:
                logger.debug("Chart generation failed: %s", exc)

        # ── records table (top 50 sorted by severity + classification) ───
        sorted_records = sorted(
            enriched,
            key=lambda r: (
                SEVERITY_RANK.get(r.get("severity", "MEDIUM"), 2),
                0 if r.get("ai_classification") == "TRUE_FAILURE" else 1,
            ),
        )[:50]

        rows_html = ""
        for r in sorted_records:
            cls   = r.get("ai_classification", "")
            color = CLASSIFICATION_COLORS.get(cls, "#6c757d")
            conf  = r.get("ai_confidence", "")
            conf_pct = f"{float(conf)*100:.0f}%" if conf != "" else "—"
            rows_html += f"""
            <tr>
              <td style="font-family:monospace;font-size:11px;">{r.get('record_id','')[:18]}…</td>
              <td>{r.get('pipeline_name','')}</td>
              <td>{r.get('table_name','')}</td>
              <td>{r.get('column_name','')}</td>
              <td>{r.get('rule_type','')}</td>
              <td><span style="background:{_severity_color(r.get('severity',''))};color:#fff;padding:2px 7px;border-radius:3px;font-size:11px;">{r.get('severity','')}</span></td>
              <td><span style="background:{color};color:#fff;padding:2px 7px;border-radius:3px;font-size:11px;">{cls}</span></td>
              <td style="text-align:center;">{conf_pct}</td>
              <td style="font-size:11px;">{r.get('ai_recommended_action','')}</td>
              <td style="font-size:11px;max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="{r.get('ai_explanation','').replace(chr(34),'')}">
                {r.get('ai_explanation','')[:120]}{'…' if len(r.get('ai_explanation','')) > 120 else ''}
              </td>
            </tr>"""

        exec_text = ""
        if isinstance(exec_sum, dict):
            exec_text = exec_sum.get("executive_summary", exec_sum.get("summary", ""))
        elif isinstance(exec_sum, str):
            exec_text = exec_sum

        run_end      = report.get("run_end", "")
        cost_summary = report.get("cost_summary", {})
        step_costs   = report.get("step_costs", [])

        # ── Cost per step table ───────────────────────────────────────────
        cost_rows_html = ""
        by_step = cost_summary.get("by_step_labeled", {})
        for label, data in by_step.items():
            cost_rows_html += (
                f"<tr><td>{label}</td>"
                f"<td style='text-align:right'>{data['input_tokens']:,}</td>"
                f"<td style='text-align:right'>{data['output_tokens']:,}</td>"
                f"<td style='text-align:right'>${data['cost_usd']:.6f}</td></tr>"
            )
        total_tokens = cost_summary.get("total_tokens", 0)
        total_cost   = cost_summary.get("total_cost_usd", 0.0)
        total_calls  = cost_summary.get("total_calls", 0)

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<style>
  body  {{ font-family:'Segoe UI',Arial,sans-serif; background:#f0f2f5; margin:0; padding:20px; color:#333; }}
  .card {{ background:#fff; border-radius:8px; box-shadow:0 2px 8px rgba(0,0,0,.10); padding:20px; margin-bottom:18px; }}
  h1    {{ color:#2c3e50; font-size:20px; margin:0 0 4px; }}
  h2    {{ color:#34495e; font-size:15px; margin:10px 0 8px; border-bottom:2px solid #e9ecef; padding-bottom:4px; }}
  .meta {{ color:#666; font-size:12px; }}
  .kpi  {{ display:inline-block; width:140px; background:#f8f9fa; border-radius:8px; padding:14px 18px; margin:6px; text-align:center; border:1px solid #e9ecef; vertical-align:top; }}
  .kpi .val  {{ font-size:28px; font-weight:700; }}
  .kpi .lbl  {{ font-size:12px; color:#666; margin-top:4px; }}
  .red   {{ color:#dc3545; }}
  .green {{ color:#28a745; }}
  .yellow{{ color:#e67e22; }}
  .blue  {{ color:#4e73df; }}
  table  {{ border-collapse:collapse; width:100%; font-size:12px; }}
  th     {{ background:#4e73df; color:#fff; padding:7px 10px; text-align:left; font-weight:600; }}
  td     {{ padding:6px 10px; border-bottom:1px solid #f0f2f5; vertical-align:top; }}
  tr:hover td {{ background:#f8f9fb; }}
  .exec-box {{ background:#f8f9fa; border-left:4px solid #4e73df; padding:10px 16px; border-radius:4px; font-size:13px; white-space:pre-wrap; }}
</style>
</head>
<body>

<div class="card">
  <h1>Strands ETL Agent Analyzer — Run Report</h1>
  <div class="meta">
    Run ID: <b>{self.run_id}</b> &nbsp;|&nbsp;
    Start: <b>{self.run_start.strftime('%Y-%m-%d %H:%M:%S')} UTC</b> &nbsp;|&nbsp;
    End: <b>{run_end}</b> &nbsp;|&nbsp;
    Version: {VERSION}
  </div>
</div>

<div class="card">
  <h2>AI Analysis Summary</h2>
  <div>
    <div class="kpi"><div class="val blue">{total}</div><div class="lbl">Records Analysed</div></div>
    <div class="kpi"><div class="val red">{tf}</div><div class="lbl">True Failures</div></div>
    <div class="kpi"><div class="val green">{fp}</div><div class="lbl">False Positives</div></div>
    <div class="kpi"><div class="val yellow">{ni}</div><div class="lbl">Needs Investigation</div></div>
    <div class="kpi"><div class="val blue">{avg_conf:.0%}</div><div class="lbl">Avg Confidence</div></div>
    <div class="kpi"><div class="val blue">{total_calls}</div><div class="lbl">Bedrock Calls</div></div>
    <div class="kpi"><div class="val blue">${total_cost:.4f}</div><div class="lbl">Total AI Cost</div></div>
  </div>
  {chart_html}
</div>

{f'<div class="card"><h2>Executive Summary</h2><div class="exec-box">{exec_text}</div></div>' if exec_text else ''}

<div class="card">
  <h2>Bedrock Token Usage &amp; Cost by Step</h2>
  <table>
    <tr><th>Step / Agent</th><th style="text-align:right">Input Tokens</th><th style="text-align:right">Output Tokens</th><th style="text-align:right">Cost (USD)</th></tr>
    {cost_rows_html}
    <tr style="font-weight:700;background:#e9ecef;">
      <td>TOTAL</td>
      <td style="text-align:right">{cost_summary.get('total_input_tokens',0):,}</td>
      <td style="text-align:right">{cost_summary.get('total_output_tokens',0):,}</td>
      <td style="text-align:right">${total_cost:.6f}</td>
    </tr>
  </table>
  <p style="font-size:11px;color:#888;margin-top:8px;">
    Pricing: Claude 3 Sonnet — $3.00/1M input tokens, $15.00/1M output tokens (Bedrock on-demand, us-east-1).<br>
    Total tokens this run: <b>{total_tokens:,}</b> &nbsp;|&nbsp; Bedrock API calls: <b>{total_calls}</b>
  </p>
</div>

<div class="card">
  <h2>Analysed Records{' (top 50 by severity)' if len(enriched) > 50 else ''}</h2>
  <table>
    <tr>
      <th>Record ID</th><th>Pipeline</th><th>Table</th><th>Column</th>
      <th>Rule Type</th><th>Severity</th><th>AI Classification</th>
      <th>Confidence</th><th>Action</th><th>Explanation</th>
    </tr>
    {rows_html}
  </table>
</div>

</body>
</html>"""
        return html


def _severity_color(sev: str) -> str:
    return {
        "CRITICAL": "#721c24",
        "HIGH":     "#dc3545",
        "MEDIUM":   "#ffc107",
        "LOW":      "#28a745",
    }.get(sev.upper() if sev else "", "#6c757d")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Strands ETL Agent Analyzer — AI-powered audit_validation enrichment"
    )
    p.add_argument("--config",      default="config/agent_analyzer_config.json",
                   help="Path to config JSON (local or s3://…)")
    p.add_argument("--run-id",      default="",
                   help="Limit analysis to a specific validation run_id")
    p.add_argument("--pipeline",    default="",
                   help="Filter by pipeline name")
    p.add_argument("--look-back",   type=int, default=0,
                   help="Override look_back_hours from config")
    p.add_argument("--no-email",    action="store_true",
                   help="Suppress email even if enabled in config")
    p.add_argument("--glue-mode",   action="store_true",
                   help="Read parameters from Glue job arguments")
    return p.parse_args()


def main() -> int:
    args = _parse_args()

    config_path = args.config

    if args.glue_mode:
        try:
            from awsglue.utils import getResolvedOptions
            glue_args  = getResolvedOptions(sys.argv, ["config"])
            config_path = glue_args.get("config", config_path)
        except Exception as exc:
            logger.warning("Could not read Glue args: %s. Using defaults.", exc)

    config = _load_config(config_path)

    # CLI overrides
    if args.run_id:
        config.setdefault("analysis", {})["run_id_filter"] = args.run_id
    if args.pipeline:
        config.setdefault("analysis", {}).setdefault("pipeline_filters", []).append(args.pipeline)
    if args.look_back:
        config.setdefault("analysis", {})["look_back_hours"] = args.look_back
    if args.no_email:
        config.setdefault("email", {})["enabled"] = False

    analyzer = AgentAnalyzer(config)
    report   = analyzer.run()

    print(json.dumps(
        {k: v for k, v in report.items() if k not in ("enriched_records", "cost_summary")},
        indent=2,
        default=str,
    ))

    if analyzer.cost_tracker:
        print("\n" + analyzer.cost_tracker.render_table())

    ai = report.get("ai_analysis", {})
    if ai.get("true_failures", 0) > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
