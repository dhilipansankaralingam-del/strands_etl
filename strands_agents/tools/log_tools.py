"""
Log tools — S3 / CloudWatch log fetching and fast-diagnose pattern matching.

Tools
-----
fetch_s3_logs          Download log files from S3 and return their text.
fetch_cloudwatch_logs  Pull recent log events from a CloudWatch log group.
extract_error_blocks   Apply 12 fast-diagnose patterns to raw log text and
                       return structured error blocks.

Fast-diagnose patterns (12)
---------------------------
 1  GLUE_OOM               Glue / Spark out-of-memory
 2  GLUE_SCHEMA_MISMATCH   Schema change or column mismatch in Glue
 3  GLUE_CRAWLER_FAIL      Glue Crawler did not complete successfully
 4  ATHENA_TIMEOUT         Athena query resource exhaustion / timeout
 5  ATHENA_PERMISSION      S3 or Glue access-denied for Athena
 6  ATHENA_PARTITION_MISS  Partition referenced but not found
 7  LAMBDA_TIMEOUT         Lambda function exceeded its time limit
 8  LAMBDA_OOM             Lambda function exceeded its memory limit
 9  EMR_STEP_FAILED        EMR step exited with non-zero status
10  EMR_CLUSTER_TERM       EMR cluster terminated unexpectedly
11  NETWORK_TIMEOUT        TCP / socket / SSL connection timeout
12  DATA_CORRUPTION        NullPointerException, bad-data format, cast failure
"""

from __future__ import annotations

import gzip
import io
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
from strands import tool

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AWS_REGION: str = os.environ.get("AWS_REGION", "us-east-1")
CW_MAX_EVENTS: int = int(os.environ.get("CW_MAX_EVENTS", "500"))
S3_MAX_FILES: int = int(os.environ.get("S3_MAX_FILES", "5"))
S3_MAX_BYTES: int = int(os.environ.get("S3_MAX_BYTES", str(512 * 1024)))  # 512 KB

_s3_client: Optional[object] = None
_logs_client: Optional[object] = None


def _s3():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", region_name=AWS_REGION)
    return _s3_client


def _logs():
    global _logs_client
    if _logs_client is None:
        _logs_client = boto3.client("logs", region_name=AWS_REGION)
    return _logs_client


# ---------------------------------------------------------------------------
# 12 fast-diagnose patterns
# ---------------------------------------------------------------------------
_PATTERNS: list[dict] = [
    {
        "id": "GLUE_OOM",
        "service": "glue",
        "severity": "HIGH",
        "regex": re.compile(
            r"(OutOfMemoryError|GC overhead limit exceeded"
            r"|Container killed by YARN for exceeding memory limits"
            r"|java\.lang\.OutOfMemoryError"
            r"|ExecutorLostFailure.*exit status 137)",
            re.IGNORECASE,
        ),
        "summary": "Glue / Spark executor ran out of memory.",
        "remediation": "Increase DPU count, tune spark.executor.memory, or partition the input.",
    },
    {
        "id": "GLUE_SCHEMA_MISMATCH",
        "service": "glue",
        "severity": "HIGH",
        "regex": re.compile(
            r"(SchemaChangeNotSupportedException"
            r"|Cannot resolve column name"
            r"|AnalysisException.*cannot resolve"
            r"|Table or view not found"
            r"|schema mismatch"
            r"|incompatible schema)",
            re.IGNORECASE,
        ),
        "summary": "Glue job encountered a schema change or column mismatch.",
        "remediation": "Re-crawl the table, check recent DDL changes, verify column names.",
    },
    {
        "id": "GLUE_CRAWLER_FAIL",
        "service": "glue",
        "severity": "MEDIUM",
        "regex": re.compile(
            r"(Crawler.*FAILED"
            r"|CrawlerNotRunningException"
            r"|EntityNotFoundException.*crawler"
            r"|crawler.*did not succeed)",
            re.IGNORECASE,
        ),
        "summary": "Glue Crawler failed or did not complete.",
        "remediation": "Check Crawler IAM role, S3 path, and CloudWatch Crawler logs.",
    },
    {
        "id": "ATHENA_TIMEOUT",
        "service": "athena",
        "severity": "HIGH",
        "regex": re.compile(
            r"(Query exhausted resources"
            r"|TIMEOUT_ON_RESOURCE"
            r"|TooManyRequestsException"
            r"|query timed out"
            r"|execution timed out)",
            re.IGNORECASE,
        ),
        "summary": "Athena query timed out or exhausted cluster resources.",
        "remediation": "Break query into smaller chunks, add partition filters, increase timeout.",
    },
    {
        "id": "ATHENA_PERMISSION",
        "service": "athena",
        "severity": "HIGH",
        "regex": re.compile(
            r"(AccessDeniedException"
            r"|403 Forbidden"
            r"|S3ServiceException.*403"
            r"|is not authorized to perform"
            r"|Access Denied)",
            re.IGNORECASE,
        ),
        "summary": "Athena lacks permission to access S3 or Glue Catalog.",
        "remediation": "Review IAM role attached to the Athena workgroup; add s3:GetObject / glue:GetTable.",
    },
    {
        "id": "ATHENA_PARTITION_MISS",
        "service": "athena",
        "severity": "MEDIUM",
        "regex": re.compile(
            r"(PartitionNotFoundException"
            r"|partition not found"
            r"|MSCK REPAIR"
            r"|no partition found"
            r"|Table partition.*does not exist)",
            re.IGNORECASE,
        ),
        "summary": "Athena referenced a partition that does not exist.",
        "remediation": "Run MSCK REPAIR TABLE or add partition manually with ALTER TABLE ADD PARTITION.",
    },
    {
        "id": "LAMBDA_TIMEOUT",
        "service": "lambda",
        "severity": "HIGH",
        "regex": re.compile(
            r"(Task timed out after \d+\.\d+ seconds"
            r"|Lambda function timed out"
            r"|ETIMEDOUT"
            r"|RequestTimeout)",
            re.IGNORECASE,
        ),
        "summary": "Lambda function exceeded its configured timeout.",
        "remediation": "Increase Lambda timeout, optimise the function, or split the workload.",
    },
    {
        "id": "LAMBDA_OOM",
        "service": "lambda",
        "severity": "HIGH",
        "regex": re.compile(
            r"(Runtime exited with error: signal: killed"
            r"|Lambda function out of memory"
            r"|MemorySize.*exceeded"
            r"|Process exited before completing request)",
            re.IGNORECASE,
        ),
        "summary": "Lambda function ran out of memory.",
        "remediation": "Increase Lambda memory allocation (Powers-of-2 MB up to 10240).",
    },
    {
        "id": "EMR_STEP_FAILED",
        "service": "emr",
        "severity": "HIGH",
        "regex": re.compile(
            r"(Step.*FAILED"
            r"|exit status: [1-9]"
            r"|YARN application.*FAILED"
            r"|SparkContext.*stopApplication"
            r"|Application.*finished.*FAILED)",
            re.IGNORECASE,
        ),
        "summary": "An EMR step exited with a non-zero status.",
        "remediation": "Check EMR step stderr log on S3 (s3://<logs-bucket>/elasticmapreduce/<cluster-id>/steps/).",
    },
    {
        "id": "EMR_CLUSTER_TERM",
        "service": "emr",
        "severity": "CRITICAL",
        "regex": re.compile(
            r"(Cluster terminated"
            r"|TERMINATING.*BOOTSTRAP_FAILURE"
            r"|TERMINATING.*INSTANCE_FAILURE"
            r"|BootstrapFailure"
            r"|ec2.*InstanceLimitExceeded)",
            re.IGNORECASE,
        ),
        "summary": "EMR cluster terminated unexpectedly.",
        "remediation": "Check bootstrap actions, EC2 limits, and EMR cluster event log.",
    },
    {
        "id": "NETWORK_TIMEOUT",
        "service": "any",
        "severity": "MEDIUM",
        "regex": re.compile(
            r"(Connection timed out"
            r"|SocketTimeoutException"
            r"|ConnectTimeoutError"
            r"|Read timed out"
            r"|SSLError.*timed out"
            r"|javax\.net\.ssl.*timeout)",
            re.IGNORECASE,
        ),
        "summary": "Network connection timed out.",
        "remediation": "Check VPC endpoints, security groups, NAT gateway, and retry logic.",
    },
    {
        "id": "DATA_CORRUPTION",
        "service": "any",
        "severity": "HIGH",
        "regex": re.compile(
            r"(NullPointerException"
            r"|HIVE_BAD_DATA"
            r"|ClassCastException"
            r"|NumberFormatException"
            r"|corrupt record"
            r"|invalid UTF-8"
            r"|Malformed.*JSON"
            r"|java\.io\.IOException.*EOF)",
            re.IGNORECASE,
        ),
        "summary": "Bad or corrupt data encountered during processing.",
        "remediation": "Inspect the source file, add null / type guards, or quarantine the bad records.",
    },
]


def _read_s3_object(bucket: str, key: str) -> str:
    """Download one S3 object (text or gzip) and return its text content."""
    resp = _s3().get_object(Bucket=bucket, Key=key)
    raw = resp["Body"].read(S3_MAX_BYTES)
    if key.endswith(".gz"):
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
            raw = gz.read(S3_MAX_BYTES)
    return raw.decode("utf-8", errors="replace")


# ---------------------------------------------------------------------------
# Public tools
# ---------------------------------------------------------------------------


@tool
def fetch_s3_logs(bucket: str, prefix: str, max_files: int = S3_MAX_FILES) -> str:
    """
    Download log files from an S3 prefix and return their concatenated text.

    Supports plain-text and gzip-compressed (.gz) log files.  At most
    *max_files* files are read, and each is capped at 512 KB to protect
    context length.

    Parameters
    ----------
    bucket    : S3 bucket name (without s3:// scheme).
    prefix    : S3 key prefix (folder path, e.g. 'elasticmapreduce/j-XXX/steps/').
    max_files : Maximum number of files to read (default 5).

    Returns
    -------
    JSON string with keys:
        files_read  list  – [{key, size_bytes, content_preview (first 200 chars)}]
        full_text   str   – Concatenated log content (use for extract_error_blocks).
        truncated   bool  – True if more files exist beyond max_files.
    """
    try:
        resp = _s3().list_objects_v2(
            Bucket=bucket, Prefix=prefix, MaxKeys=int(max_files) + 1
        )
    except Exception as exc:
        return json.dumps({"error": str(exc)})

    objects = resp.get("Contents", [])
    truncated = len(objects) > int(max_files)
    objects = objects[: int(max_files)]

    files_read: list[dict] = []
    full_text_parts: list[str] = []

    for obj in objects:
        try:
            text = _read_s3_object(bucket, obj["Key"])
            files_read.append(
                {
                    "key": obj["Key"],
                    "size_bytes": obj["Size"],
                    "content_preview": text[:200],
                }
            )
            full_text_parts.append(f"=== {obj['Key']} ===\n{text}\n")
        except Exception as exc:
            files_read.append({"key": obj["Key"], "error": str(exc)})

    return json.dumps(
        {
            "files_read": files_read,
            "full_text": "\n".join(full_text_parts),
            "truncated": truncated,
        }
    )


@tool
def fetch_cloudwatch_logs(
    log_group: str,
    log_stream: Optional[str] = None,
    minutes_back: int = 60,
) -> str:
    """
    Fetch recent log events from a CloudWatch Logs log group.

    Parameters
    ----------
    log_group   : Full CloudWatch log group name
                  (e.g. '/aws-glue/jobs/error' or '/aws/lambda/my-function').
    log_stream  : Optional specific log stream.  If omitted, the most recent
                  stream in the group is used.
    minutes_back: How far back to look (default 60 minutes).

    Returns
    -------
    JSON string with keys:
        log_group   str
        log_stream  str
        event_count int
        events      list  – [{timestamp, message}]
        full_text   str   – All messages joined by newline (use for extract_error_blocks).
    """
    client = _logs()
    start_ms = int(
        (
            datetime.now(timezone.utc) - timedelta(minutes=int(minutes_back))
        ).timestamp()
        * 1000
    )

    # Resolve log stream
    stream_name = log_stream
    if not stream_name:
        try:
            streams = client.describe_log_streams(
                logGroupName=log_group,
                orderBy="LastEventTime",
                descending=True,
                limit=1,
            )
            items = streams.get("logStreams", [])
            stream_name = items[0]["logStreamName"] if items else None
        except Exception as exc:
            return json.dumps({"error": f"Could not list log streams: {exc}"})

    if not stream_name:
        return json.dumps({"error": f"No log streams found in group '{log_group}'"})

    # Fetch events
    events: list[dict] = []
    kwargs = {
        "logGroupName": log_group,
        "logStreamName": stream_name,
        "startTime": start_ms,
        "limit": CW_MAX_EVENTS,
    }
    try:
        resp = client.get_log_events(**kwargs)
        for ev in resp.get("events", []):
            ts = datetime.fromtimestamp(
                ev["timestamp"] / 1000, tz=timezone.utc
            ).isoformat()
            events.append({"timestamp": ts, "message": ev["message"].rstrip()})
    except Exception as exc:
        return json.dumps({"error": str(exc)})

    full_text = "\n".join(e["message"] for e in events)
    return json.dumps(
        {
            "log_group": log_group,
            "log_stream": stream_name,
            "event_count": len(events),
            "events": events,
            "full_text": full_text,
        }
    )


@tool
def extract_error_blocks(log_text: str, service: str = "any") -> str:
    """
    Apply 12 fast-diagnose patterns to raw log text and return structured
    error blocks grouped by pattern type.

    Patterns cover: GLUE_OOM, GLUE_SCHEMA_MISMATCH, GLUE_CRAWLER_FAIL,
    ATHENA_TIMEOUT, ATHENA_PERMISSION, ATHENA_PARTITION_MISS, LAMBDA_TIMEOUT,
    LAMBDA_OOM, EMR_STEP_FAILED, EMR_CLUSTER_TERM, NETWORK_TIMEOUT,
    DATA_CORRUPTION.

    Parameters
    ----------
    log_text : Raw log content (from fetch_s3_logs or fetch_cloudwatch_logs).
    service  : Optional service filter — 'glue' | 'athena' | 'lambda' | 'emr' | 'any'.
               When 'any', all 12 patterns are applied.

    Returns
    -------
    JSON string with keys:
        matched_patterns  list  – [{id, service, severity, summary, remediation,
                                    match_count, sample_lines}]
        unmatched_lines   list  – Lines containing 'error' or 'exception' but
                                  not caught by any pattern.
        diagnosis         str   – Plain-English overall diagnosis.
    """
    lines = log_text.splitlines()
    svc_lower = service.lower()

    matched: list[dict] = []
    matched_line_indices: set[int] = set()

    for pattern in _PATTERNS:
        if svc_lower != "any" and pattern["service"] not in (svc_lower, "any"):
            continue
        hits: list[str] = []
        for idx, line in enumerate(lines):
            if pattern["regex"].search(line):
                hits.append(line.strip())
                matched_line_indices.add(idx)
        if hits:
            matched.append(
                {
                    "id": pattern["id"],
                    "service": pattern["service"],
                    "severity": pattern["severity"],
                    "summary": pattern["summary"],
                    "remediation": pattern["remediation"],
                    "match_count": len(hits),
                    "sample_lines": hits[:5],
                }
            )

    # Collect error/exception lines not already matched
    error_re = re.compile(r"(error|exception|traceback|fatal)", re.IGNORECASE)
    unmatched: list[str] = []
    for idx, line in enumerate(lines):
        if idx not in matched_line_indices and error_re.search(line):
            unmatched.append(line.strip())

    # Build a plain-English diagnosis
    if not matched and not unmatched:
        diagnosis = "No known error patterns detected in the provided log text."
    elif matched:
        top = max(matched, key=lambda m: (m["severity"] == "CRITICAL", m["match_count"]))
        diagnosis = (
            f"Detected {len(matched)} known error pattern(s).  "
            f"Most prominent: [{top['id']}] {top['summary']}  "
            f"Remediation: {top['remediation']}"
        )
        if unmatched:
            diagnosis += f"  Additionally, {len(unmatched)} unrecognised error lines found."
    else:
        diagnosis = (
            f"No known patterns matched, but {len(unmatched)} unrecognised error "
            "lines were found.  Manual inspection recommended."
        )

    return json.dumps(
        {
            "matched_patterns": matched,
            "unmatched_lines": unmatched[:20],
            "diagnosis": diagnosis,
        }
    )
