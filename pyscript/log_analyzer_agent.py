###################################################################################################
# SCRIPT        : log_analyzer_agent.py
# PURPOSE       : Dedicated Strands Log Analyzer Agent
#                 Reads logs from S3 paths or CloudWatch Log Groups, auto-detects
#                 the AWS service (Glue / Athena / Lambda / EMR / Step Functions /
#                 Fargate / generic CloudWatch), extracts error signals, and sends
#                 each failure cluster to Amazon Bedrock (Claude 3 Sonnet) for
#                 root-cause analysis and concrete fix recommendations.
#
# SUPPORTED SOURCES:
#   S3          : s3://bucket/prefix/  or  s3://bucket/path/to/file.log
#   CloudWatch  : /aws/glue/jobs/<job-name>
#                 /aws/lambda/<function-name>
#                 /aws/emr-serverless/…
#                 /aws/athena/…
#                 custom log groups
#
# USAGE (local) : python pyscript/log_analyzer_agent.py \
#                     --log-paths s3://my-bucket/logs/glue/my_job/2026-04-18/ \
#                     --config config/log_analyzer_config.json
#
# USAGE (multi) : python pyscript/log_analyzer_agent.py \
#                     --log-paths s3://b/glue/ /aws/lambda/etl-fn \
#                     --config config/log_analyzer_config.json \
#                     --since-hours 12
#
# USAGE (Glue)  : python pyscript/log_analyzer_agent.py --glue-mode
#
#=================================================================================================
# DATE          BY              MODIFICATION LOG
# ----------    -----------     -------------------------------------------------------
# 04/2026       Dhilipan        Initial version
#=================================================================================================
###################################################################################################

import argparse
import json
import logging
import os
import re
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("log_analyzer_agent")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
VERSION = "1.0.0"
MAX_LOG_CHARS_PER_CLUSTER = 8_000   # chars sent to Bedrock per error cluster

# ---------------------------------------------------------------------------
# Service detection patterns
# ---------------------------------------------------------------------------
SERVICE_PATTERNS = {
    "glue": [
        r"/aws/glue/",
        r"GlueJobRun",
        r"com\.amazonaws\.services\.glue",
        r"pyspark\.sql",
        r"py4j\.protocol",
        r"GlueContext",
        r"DynamicFrame",
    ],
    "athena": [
        r"/aws/athena/",
        r"AmazonAthena",
        r"HIVE_",
        r"SYNTAX_ERROR",
        r"COLUMN_NOT_FOUND",
        r"TABLE_NOT_FOUND",
        r"com\.amazonaws\.services\.athena",
    ],
    "lambda": [
        r"/aws/lambda/",
        r"START RequestId",
        r"END RequestId",
        r"REPORT RequestId",
        r"Task timed out",
        r"Runtime\.ExitError",
        r"errorType.*Exception",
    ],
    "emr": [
        r"/aws/emr",
        r"YARN",
        r"JobFlow",
        r"HadoopCluster",
        r"hadoop\.mapreduce",
        r"spark\.executor",
        r"Container killed",
        r"ApplicationMaster",
    ],
    "step_functions": [
        r"arn:aws:states",
        r"ExecutionFailed",
        r"StateMachine",
        r"States\.TaskFailed",
        r"States\.HeartbeatTimeout",
    ],
    "fargate": [
        r"/ecs/",
        r"ECS",
        r"DockerContainer",
        r"ContainerExit",
        r"OutOfMemoryError",
    ],
}

# ---------------------------------------------------------------------------
# Error extraction patterns per service
# ---------------------------------------------------------------------------
ERROR_PATTERNS = {
    "glue": [
        r"(?i)(ERROR|FATAL|Exception|Traceback|py4j\.protocol\.Py4JJavaError|JobRunException)",
        r"(?i)(OutOfMemoryError|GC overhead|heap space)",
        r"(?i)(AccessDenied|NoSuchKey|NoSuchBucket)",
        r"(?i)(Job run failed|job timed out|worker.*failed)",
        r"(?i)(AnalysisException|ParseException|SparkException)",
        r"(?i)(duplicate key|constraint violation)",
    ],
    "athena": [
        r"(?i)(FAILED|Error|Exception)",
        r"(?i)(HIVE_\w+|SYNTAX_ERROR|INTERNAL_ERROR)",
        r"(?i)(S3 Exception|Access Denied|NoSuchKey)",
        r"(?i)(Query exhausted resources|Athena query timed out)",
        r"(?i)(COLUMN_NOT_FOUND|TABLE_NOT_FOUND|DATABASE_NOT_FOUND)",
        r"(?i)(GENERIC_INTERNAL_ERROR|EXCEEDED_TIME_LIMIT)",
    ],
    "lambda": [
        r"(?i)(ERROR|Exception|Traceback|Task timed out|Runtime\.ExitError)",
        r"(?i)(OutOfMemoryError|Memory Size|Max Memory Used)",
        r"(?i)(AccessDeniedException|ThrottlingException|TooManyRequestsException)",
        r"(?i)(Connection refused|timeout|Read timed out)",
        r"(?i)(errorMessage|errorType)",
    ],
    "emr": [
        r"(?i)(ERROR|FATAL|Exception|Container killed|ApplicationMaster failed)",
        r"(?i)(YARN.*failed|step failed|MapReduce.*failed)",
        r"(?i)(OutOfMemoryError|GC overhead|Java heap)",
        r"(?i)(Spot Instance|Instance terminated|EC2 capacity)",
        r"(?i)(HDFS.*exception|S3.*exception|Permission denied)",
        r"(?i)(Job.*failed|Stage.*failed|Task.*failed)",
    ],
    "step_functions": [
        r"(?i)(ExecutionFailed|TaskFailed|HeartbeatTimeout|States\.)",
        r"(?i)(Error|Cause|errorMessage)",
        r"(?i)(ResourceNotReady|TaskTimedOut)",
    ],
    "generic": [
        r"(?i)(ERROR|FATAL|CRITICAL|Exception|Traceback|FAILED|failure)",
        r"(?i)(timeout|timed out|connection refused|refused)",
        r"(?i)(AccessDenied|Permission denied|Unauthorized)",
        r"(?i)(OutOfMemory|OOM|heap space|memory)",
    ],
}

# Known root-cause signatures for fast local classification (pre-Bedrock)
FAST_DIAGNOSE = {
    r"OutOfMemoryError|GC overhead|heap space":
        "JVM heap exhausted — increase --executor-memory or scale up worker type",
    r"Task timed out":
        "Lambda timeout — increase function timeout or break into smaller chunks",
    r"Spot Instance.*terminated|EC2.*capacity":
        "Spot capacity reclaimed — retry with On-Demand or increase bid price",
    r"AccessDenied|Permission denied":
        "IAM permission missing — check role policies for S3/Glue/Athena access",
    r"NoSuchKey|NoSuchBucket":
        "S3 path does not exist — verify source file was delivered",
    r"COLUMN_NOT_FOUND|TABLE_NOT_FOUND|DATABASE_NOT_FOUND":
        "Schema drift — table or column removed/renamed in Glue Catalog",
    r"HIVE_PARTITION_SCHEMA_MISMATCH":
        "Partition schema mismatch — run MSCK REPAIR TABLE or recreate partition",
    r"SYNTAX_ERROR":
        "SQL syntax error in Athena query — review generated SQL",
    r"ThrottlingException|TooManyRequests":
        "API rate limit hit — add exponential back-off or request limit increase",
    r"py4j\.protocol\.Py4JJavaError":
        "PySpark Java bridge error — check Spark driver logs for the root Java exception",
    r"duplicate key|ConstraintViolationException":
        "Duplicate primary key — check for double-load or missing dedup logic",
}


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def _load_config(path: str) -> Dict[str, Any]:
    if path.startswith("s3://"):
        bucket, key = path[5:].split("/", 1)
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        return json.loads(body)
    if os.path.exists(path):
        with open(path) as fh:
            return json.load(fh)
    return {}


# ---------------------------------------------------------------------------
# Log fetchers
# ---------------------------------------------------------------------------

class S3LogFetcher:
    """Downloads log content from S3 paths (files or prefixes)."""

    def __init__(self, region: str):
        self.s3 = boto3.client("s3", region_name=region)

    def fetch(self, path: str, since_hours: int = 24) -> List[Tuple[str, str]]:
        """
        Returns list of (s3_key, log_content) tuples.
        path may be s3://bucket/prefix/ or s3://bucket/path/to/file.log
        """
        bucket, prefix = path[5:].split("/", 1)
        cutoff = datetime.utcnow() - timedelta(hours=since_hours)
        results: List[Tuple[str, str]] = []

        if prefix.endswith(".log") or prefix.endswith(".txt") or "." in prefix.split("/")[-1]:
            # Single file
            try:
                obj = self.s3.get_object(Bucket=bucket, Key=prefix)
                results.append((f"s3://{bucket}/{prefix}", obj["Body"].read().decode("utf-8", errors="replace")))
            except Exception as exc:
                logger.warning("Could not fetch s3://%s/%s: %s", bucket, prefix, exc)
        else:
            # Prefix — list and filter by time
            paginator = self.s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if obj["LastModified"].replace(tzinfo=None) < cutoff:
                        continue
                    key = obj["Key"]
                    try:
                        content = self.s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8", errors="replace")
                        results.append((f"s3://{bucket}/{key}", content))
                    except Exception as exc:
                        logger.warning("Skipping s3://%s/%s: %s", bucket, key, exc)
        logger.info("S3LogFetcher: fetched %d log file(s) from %s", len(results), path)
        return results


class CloudWatchLogFetcher:
    """Fetches recent log events from a CloudWatch Log Group."""

    def __init__(self, region: str):
        self.cw = boto3.client("logs", region_name=region)

    def fetch(self, log_group: str, since_hours: int = 24) -> List[Tuple[str, str]]:
        """
        Returns list of (stream_name, log_content) for all streams modified
        in the last since_hours hours.
        """
        cutoff_ms = int((datetime.utcnow() - timedelta(hours=since_hours)).timestamp() * 1000)
        results: List[Tuple[str, str]] = []

        try:
            streams_resp = self.cw.describe_log_streams(
                logGroupName=log_group,
                orderBy="LastEventTime",
                descending=True,
                limit=20,
            )
        except Exception as exc:
            logger.error("Cannot describe log streams for %s: %s", log_group, exc)
            return []

        for stream in streams_resp.get("logStreams", []):
            last_evt = stream.get("lastEventTimestamp", 0)
            if last_evt < cutoff_ms:
                continue
            stream_name = stream["logStreamName"]
            try:
                events_resp = self.cw.get_log_events(
                    logGroupName=log_group,
                    logStreamName=stream_name,
                    startTime=cutoff_ms,
                    limit=2000,
                )
                lines = [e["message"] for e in events_resp.get("events", [])]
                results.append((f"{log_group}/{stream_name}", "\n".join(lines)))
            except Exception as exc:
                logger.warning("Could not read stream %s/%s: %s", log_group, stream_name, exc)

        logger.info("CloudWatchLogFetcher: fetched %d stream(s) from %s", len(results), log_group)
        return results


# ---------------------------------------------------------------------------
# Service detector
# ---------------------------------------------------------------------------

def detect_service(path: str, content: str) -> str:
    """Return the AWS service name for this log source."""
    combined = path + "\n" + content[:2000]
    scores: Dict[str, int] = defaultdict(int)
    for svc, patterns in SERVICE_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, combined):
                scores[svc] += 1
    if scores:
        return max(scores, key=scores.__get__)
    return "generic"


# ---------------------------------------------------------------------------
# Error extractor
# ---------------------------------------------------------------------------

def extract_errors(content: str, service: str) -> List[str]:
    """
    Extract error lines/blocks from log content.
    Returns list of error snippets (each up to ~500 chars).
    """
    patterns = ERROR_PATTERNS.get(service, ERROR_PATTERNS["generic"])
    error_lines: List[str] = []
    lines = content.splitlines()

    i = 0
    while i < len(lines):
        line = lines[i]
        matched = any(re.search(p, line) for p in patterns)
        if matched:
            # Grab 5 lines of context around the match
            snippet_lines = lines[max(0, i - 1): min(len(lines), i + 6)]
            error_lines.append("\n".join(snippet_lines))
            i += 6
        else:
            i += 1

    # Deduplicate near-identical errors (same first 120 chars)
    seen: set = set()
    unique: List[str] = []
    for err in error_lines:
        key = err[:120].strip()
        if key not in seen:
            seen.add(key)
            unique.append(err)

    return unique[:50]  # cap at 50 distinct error blocks


# ---------------------------------------------------------------------------
# Fast local diagnosis
# ---------------------------------------------------------------------------

def fast_diagnose(error_text: str) -> Optional[str]:
    """Return a quick fix suggestion for well-known error patterns (no Bedrock call)."""
    for pattern, suggestion in FAST_DIAGNOSE.items():
        if re.search(pattern, error_text, re.IGNORECASE):
            return suggestion
    return None


# ---------------------------------------------------------------------------
# Bedrock AI agent
# ---------------------------------------------------------------------------

SYSTEM_PROMPT_LOG = """You are a senior AWS data-engineering expert specialising in debugging
Glue, Athena, Lambda, EMR, and Step Functions failures.

Given error log snippets and the AWS service name, you:
1. Identify the root cause(s) clearly and concisely.
2. Provide step-by-step fix instructions that a data engineer can act on immediately.
3. Include specific AWS CLI / Python / Spark code snippets where helpful.
4. Flag if the issue is transient (retry may resolve) vs structural (code change needed).

Always respond in strict JSON matching this schema:
{
  "service":        "<aws-service>",
  "error_category": "<category — e.g. OOM | IAM | Schema Drift | Timeout | Network | Config>",
  "severity":       "<CRITICAL | HIGH | MEDIUM | LOW>",
  "root_causes":    ["<string>", ...],
  "is_transient":   true | false,
  "fix_steps": [
    {"step": 1, "action": "<description>", "code": "<optional shell/python/sql snippet>"}
  ],
  "preventive_measures": ["<string>", ...],
  "estimated_fix_time_minutes": <int>,
  "references": ["<AWS doc URL or KB article>"]
}"""


class LogAnalyzerAgent:
    """
    Strands-style AI agent that analyses AWS service logs and returns
    structured root-cause + fix recommendations via Amazon Bedrock.
    """

    DEFAULT_MODEL = "anthropic.claude-3-sonnet-20240229-v1:0"

    def __init__(
        self,
        region:    str = "us-east-1",
        model_id:  str = DEFAULT_MODEL,
        run_id:    str = "",
    ):
        self.region   = region
        self.model_id = model_id
        self.run_id   = run_id or str(uuid.uuid4())
        self.bedrock  = boto3.client("bedrock-runtime", region_name=region)

        # Cost tracking
        try:
            sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
            from validation.cost_tracker import BedrockCostTracker
            self.cost_tracker = BedrockCostTracker(run_id=self.run_id)
        except ImportError:
            self.cost_tracker = None

    # ------------------------------------------------------------------

    def analyze_logs(
        self,
        log_sources:  List[str],
        since_hours:  int  = 24,
        config:       Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """
        Main entry point. Accepts a list of S3 paths or CloudWatch log group names.

        Returns a structured report dict with per-source analyses, overall summary,
        and token/cost breakdown.
        """
        config = config or {}
        logger.info("=" * 70)
        logger.info("Log Analyzer Agent v%s  run_id=%s", VERSION, self.run_id)
        logger.info("Sources: %s", log_sources)
        logger.info("=" * 70)

        source_results: List[Dict[str, Any]] = []

        for source in log_sources:
            result = self._analyze_source(source, since_hours)
            source_results.append(result)

        overall = self._overall_summary(source_results)

        cost_summary = self.cost_tracker.get_summary() if self.cost_tracker else {}

        report = {
            "run_id":        self.run_id,
            "analyzed_at":   datetime.utcnow().isoformat(),
            "since_hours":   since_hours,
            "sources":       source_results,
            "overall":       overall,
            "cost_summary":  cost_summary,
        }

        if self.cost_tracker:
            logger.info("\n%s", self.cost_tracker.render_table())

        return report

    # ------------------------------------------------------------------

    def _analyze_source(self, source: str, since_hours: int) -> Dict[str, Any]:
        """Download + analyze one log source (S3 prefix or CW log group)."""
        is_s3 = source.startswith("s3://")

        if is_s3:
            fetcher = S3LogFetcher(self.region)
            log_files = fetcher.fetch(source, since_hours)
        else:
            fetcher = CloudWatchLogFetcher(self.region)
            log_files = fetcher.fetch(source, since_hours)

        if not log_files:
            return {
                "source": source, "status": "NO_LOGS",
                "service": "unknown", "findings": [],
                "message": f"No log files found in the last {since_hours}h",
            }

        findings: List[Dict[str, Any]] = []

        for (file_id, content) in log_files:
            service = detect_service(file_id, content)
            errors  = extract_errors(content, service)

            if not errors:
                findings.append({
                    "file": file_id, "service": service,
                    "status": "CLEAN", "errors_found": 0,
                })
                continue

            logger.info("  [%s] %d error block(s) in %s", service, len(errors), file_id)

            # Check for fast local diagnosis first
            combined_errors = "\n---\n".join(errors)
            fast_fix = fast_diagnose(combined_errors)

            if fast_fix and len(errors) <= 2:
                # Confidence is high — skip Bedrock to save cost
                findings.append({
                    "file":          file_id,
                    "service":       service,
                    "status":        "FAST_DIAGNOSED",
                    "errors_found":  len(errors),
                    "error_samples": errors[:3],
                    "root_causes":   [fast_fix],
                    "fix_steps":     [{"step": 1, "action": fast_fix}],
                    "is_transient":  False,
                    "bedrock_used":  False,
                })
            else:
                ai_result = self._invoke_bedrock_analysis(service, errors, file_id)
                ai_result["file"]         = file_id
                ai_result["errors_found"] = len(errors)
                ai_result["error_samples"] = errors[:3]
                ai_result["bedrock_used"] = True
                findings.append(ai_result)

        critical_count = sum(1 for f in findings if f.get("severity") in ("CRITICAL", "HIGH"))

        return {
            "source":         source,
            "service":        findings[0].get("service", "unknown") if findings else "unknown",
            "files_analyzed": len(log_files),
            "status":         "CRITICAL" if critical_count > 0 else "OK",
            "findings":       findings,
        }

    # ------------------------------------------------------------------

    def _invoke_bedrock_analysis(
        self,
        service: str,
        errors:  List[str],
        file_id: str,
    ) -> Dict[str, Any]:
        """Send error clusters to Bedrock and return structured analysis."""
        # Truncate to MAX_LOG_CHARS_PER_CLUSTER to keep prompt size reasonable
        error_text = "\n---\n".join(errors)[:MAX_LOG_CHARS_PER_CLUSTER]

        prompt = f"""AWS Service: {service}
Log Source: {file_id}
Number of distinct error blocks: {len(errors)}

=== ERROR LOG EXCERPTS ===
{error_text}
=== END OF LOGS ===

Analyse these errors and return the JSON response as specified."""

        try:
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 2048,
                "system": SYSTEM_PROMPT_LOG,
                "messages": [{"role": "user", "content": prompt}],
            }
            t0 = time.time()
            response = self.bedrock.invoke_model(
                modelId=self.model_id,
                body=json.dumps(body),
            )
            latency_ms = (time.time() - t0) * 1000
            result     = json.loads(response["body"].read())
            text       = result["content"][0]["text"]

            # Track cost
            if self.cost_tracker:
                usage = result.get("usage", {})
                self.cost_tracker.record_call(
                    model_id      = self.model_id,
                    input_tokens  = usage.get("input_tokens", 0),
                    output_tokens = usage.get("output_tokens", 0),
                    agent_type    = "log_analyzer",
                    step_label    = f"Log: {service}/{os.path.basename(file_id)}",
                    latency_ms    = latency_ms,
                )

            return self._parse_json(text)

        except Exception as exc:
            logger.error("Bedrock invocation failed for %s: %s", file_id, exc)
            return {
                "service":       service,
                "error_category": "AGENT_ERROR",
                "severity":      "HIGH",
                "root_causes":   [f"Log analyzer agent error: {exc}"],
                "is_transient":  False,
                "fix_steps":     [{"step": 1, "action": "Check Bedrock connectivity and IAM."}],
                "preventive_measures": [],
                "estimated_fix_time_minutes": 0,
            }

    # ------------------------------------------------------------------

    def _overall_summary(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        all_findings = [f for r in results for f in r.get("findings", [])]
        critical = [f for f in all_findings if f.get("severity") == "CRITICAL"]
        high     = [f for f in all_findings if f.get("severity") == "HIGH"]
        clean    = [f for f in all_findings if f.get("status") == "CLEAN"]

        all_root_causes = []
        for f in all_findings:
            all_root_causes.extend(f.get("root_causes", []))

        return {
            "total_sources":    len(results),
            "total_files":      sum(r.get("files_analyzed", 0) for r in results),
            "critical_issues":  len(critical),
            "high_issues":      len(high),
            "clean_files":      len(clean),
            "needs_attention":  len(critical) + len(high) > 0,
            "root_cause_summary": list(dict.fromkeys(all_root_causes))[:10],
        }

    # ------------------------------------------------------------------

    def _parse_json(self, raw: str) -> Dict[str, Any]:
        text = raw.strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        for marker in ("```json", "```"):
            if marker in text:
                start = text.index(marker) + len(marker)
                end   = text.rfind("```")
                if end > start:
                    try:
                        return json.loads(text[start:end].strip())
                    except json.JSONDecodeError:
                        pass
        return {
            "service":        "unknown",
            "error_category": "PARSE_ERROR",
            "severity":       "MEDIUM",
            "root_causes":    [text[:500]],
            "is_transient":   False,
            "fix_steps":      [],
            "preventive_measures": [],
        }


# ---------------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------------

def _write_report(report: Dict[str, Any], config: Dict[str, Any], s3_client) -> Optional[str]:
    out_cfg = config.get("output", {})
    bucket  = out_cfg.get("s3_bucket", "")
    prefix  = out_cfg.get("report_prefix", "log_analyzer/reports/").rstrip("/")
    if not bucket:
        return None
    today = datetime.utcnow().strftime("%Y-%m-%d")
    key   = f"{prefix}/dt={today}/{report['run_id']}.json"
    try:
        s3_client.put_object(
            Bucket=bucket, Key=key,
            Body=json.dumps(report, indent=2, default=str).encode(),
            ContentType="application/json",
        )
        uri = f"s3://{bucket}/{key}"
        logger.info("Report written → %s", uri)
        return uri
    except Exception as exc:
        logger.warning("Could not write report: %s", exc)
        return None


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Strands Log Analyzer Agent — AI root-cause analysis for AWS service logs"
    )
    p.add_argument("--log-paths",   nargs="+", default=[],
                   help="Space-separated list of S3 prefixes (s3://…) or CloudWatch log group names (/aws/glue/…)")
    p.add_argument("--config",      default="config/log_analyzer_config.json",
                   help="Config JSON path (local or s3://…)")
    p.add_argument("--since-hours", type=int, default=24,
                   help="Analyse logs from the last N hours (default: 24)")
    p.add_argument("--output",      default="",
                   help="Optional local file path to write the JSON report")
    p.add_argument("--glue-mode",   action="store_true",
                   help="Read parameters from Glue job arguments")
    return p.parse_args()


def main() -> int:
    args = _parse_args()

    config_path = args.config
    log_paths   = args.log_paths
    since_hours = args.since_hours

    if args.glue_mode:
        try:
            from awsglue.utils import getResolvedOptions
            ga          = getResolvedOptions(sys.argv, ["config", "log_paths", "since_hours"])
            config_path = ga.get("config", config_path)
            log_paths   = ga.get("log_paths", "").split(",")
            since_hours = int(ga.get("since_hours", since_hours))
        except Exception as exc:
            logger.warning("Could not read Glue args: %s", exc)

    config = _load_config(config_path)

    if not log_paths:
        log_paths = config.get("log_sources", {}).get("paths", [])

    if not log_paths:
        logger.error("No log paths provided. Use --log-paths or set log_sources.paths in config.")
        return 2

    region   = config.get("aws", {}).get("region", "us-east-1")
    model_id = config.get("ai", {}).get("model_id", LogAnalyzerAgent.DEFAULT_MODEL)

    agent  = LogAnalyzerAgent(region=region, model_id=model_id)
    report = agent.analyze_logs(log_paths, since_hours=since_hours, config=config)

    # Write to S3
    s3 = boto3.client("s3", region_name=region)
    _write_report(report, config, s3)

    # Write locally if requested
    if args.output:
        with open(args.output, "w") as fh:
            json.dump(report, fh, indent=2, default=str)
        logger.info("Report saved locally → %s", args.output)

    # Print summary to stdout
    print(json.dumps(report["overall"], indent=2))
    print("\n--- Cost Summary ---")
    if agent.cost_tracker:
        print(agent.cost_tracker.render_table())

    return 1 if report["overall"].get("needs_attention") else 0


if __name__ == "__main__":
    sys.exit(main())
