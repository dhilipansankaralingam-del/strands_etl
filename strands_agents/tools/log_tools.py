"""
strands_agents/tools/log_tools.py
===================================
Strands @tool functions for fetching and pre-processing log content
from S3 and CloudWatch Logs before the log agent performs RCA.
"""

import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from strands import tool

AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# Service detection patterns — ordered by specificity
_SERVICE_HINTS: dict[str, list[str]] = {
    "glue":    ["GlueJobRun", "JobName", "glue", "PythonShell", "DPU", "GlueContext"],
    "athena":  ["QueryExecutionId", "AmazonAthena", "athena", "HIVE_", "SYNTAX_ERROR"],
    "lambda":  ["START RequestId", "END RequestId", "REPORT RequestId", "errorType"],
    "emr":     ["YARN", "ApplicationMaster", "container_", "hadoop", "mapreduce"],
    "generic": [],
}

# Fast local diagnosis (no Bedrock) — pattern → recommended fix
_FAST_FIXES: dict[str, str] = {
    r"Job run attempt has failed.*exceeded.*retries": "Increase Glue MaxRetries or fix root cause below.",
    r"ResourceNotFoundException.*JobRun":             "Glue job name mismatch — verify trigger configuration.",
    r"S3 Access Denied|AccessDeniedException":        "Add s3:GetObject / s3:PutObject to the IAM role.",
    r"HIVE_METASTORE_ERROR":                          "Run `MSCK REPAIR TABLE` in Athena or re-crawl with Glue.",
    r"SYNTAX_ERROR.*line \d+":                        "Fix Athena SQL syntax at the indicated line.",
    r"Task failed.*OutOfMemoryError":                 "Increase Glue DPU count or partition input data.",
    r"Connection refused.*jdbc":                      "Check RDS/Redshift security group allows Glue NAT.",
    r"Rate exceeded.*ThrottlingException":            "Add exponential backoff or request AWS limit increase.",
    r"No space left on device":                       "Increase Glue shuffle spill disk or partition writes.",
    r"Container killed.*exceeded memory":             "Increase EMR instance type or tune spark.executor.memory.",
    r"errorMessage.*timeout":                         "Increase Lambda timeout or move work to Glue/Step Functions.",
    r"AnalysisException.*cannot resolve":             "Column name missing in source — check schema evolution.",
}


def _detect_service(path: str, content: str) -> str:
    scores: dict[str, int] = {svc: 0 for svc in _SERVICE_HINTS}
    for svc, hints in _SERVICE_HINTS.items():
        for h in hints:
            scores[svc] += content.count(h)
    if path.startswith("/aws/glue") or "glue" in path:
        scores["glue"] += 5
    if path.startswith("/aws/lambda") or "lambda" in path:
        scores["lambda"] += 5
    best = max(scores, key=lambda s: scores[s])
    return best if scores[best] > 0 else "generic"


def _fast_diagnose(content: str) -> list[dict[str, str]]:
    findings = []
    for pattern, fix in _FAST_FIXES.items():
        m = re.search(pattern, content, re.IGNORECASE)
        if m:
            findings.append({"matched_pattern": pattern,
                              "excerpt": m.group(0)[:200],
                              "recommended_fix": fix})
    return findings


@tool
def fetch_s3_logs(s3_path: str, since_hours: int = 24,
                  max_bytes: int = 200_000) -> str:
    """
    Fetch log content from an S3 path (single file or prefix).

    Args:
        s3_path:    s3://bucket/key or s3://bucket/prefix/.
        since_hours: Only include files modified within the last N hours.
        max_bytes:   Cap on total content returned (default 200 KB).

    Returns:
        JSON with service, content_sample, fast_diagnosis, char_count.
    """
    if not s3_path.startswith("s3://"):
        return json.dumps({"error": "s3_path must start with s3://"})

    s3     = boto3.client("s3", region_name=AWS_REGION)
    parts  = s3_path[5:].split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)

    try:
        paginator = s3.get_paginator("list_objects_v2")
        files: list[dict] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["LastModified"] >= cutoff:
                    files.append(obj)

        if not files:
            # Treat prefix as a direct key
            files = [{"Key": prefix, "LastModified": datetime.now(timezone.utc)}]

        content_parts: list[str] = []
        total = 0
        for f in sorted(files, key=lambda x: x["LastModified"], reverse=True):
            if total >= max_bytes:
                break
            try:
                body = s3.get_object(Bucket=bucket, Key=f["Key"])["Body"].read()
                chunk = body.decode("utf-8", errors="replace")[:max_bytes - total]
                content_parts.append(chunk)
                total += len(chunk)
            except Exception:
                continue

        content = "\n".join(content_parts)
        service = _detect_service(s3_path, content)
        fast_dx = _fast_diagnose(content)

        return json.dumps({
            "source":         s3_path,
            "service":        service,
            "files_found":    len(files),
            "content_sample": content[:10_000],
            "fast_diagnosis": fast_dx,
            "char_count":     len(content),
        })

    except Exception as exc:
        return json.dumps({"error": str(exc), "source": s3_path})


@tool
def fetch_cloudwatch_logs(log_group: str, since_hours: int = 24,
                          max_events: int = 500) -> str:
    """
    Fetch recent log events from a CloudWatch Logs group.

    Args:
        log_group:   Log group name (e.g. /aws/glue/jobs/trigger_glue_job).
        since_hours: How many hours back to fetch (default 24).
        max_events:  Maximum log events to return (default 500).

    Returns:
        JSON with service, content_sample, fast_diagnosis, event_count.
    """
    logs    = boto3.client("logs", region_name=AWS_REGION)
    cutoff  = int((datetime.now(timezone.utc) - timedelta(hours=since_hours)).timestamp() * 1000)

    try:
        streams_resp = logs.describe_log_streams(
            logGroupName=log_group,
            orderBy="LastEventTime",
            descending=True,
            limit=5,
        )
        streams = streams_resp.get("logStreams", [])

        events: list[str] = []
        for stream in streams:
            if len(events) >= max_events:
                break
            resp = logs.get_log_events(
                logGroupName=log_group,
                logStreamName=stream["logStreamName"],
                startTime=cutoff,
                limit=min(max_events - len(events), 100),
            )
            for ev in resp.get("events", []):
                events.append(ev.get("message", ""))

        content  = "\n".join(events)
        service  = _detect_service(log_group, content)
        fast_dx  = _fast_diagnose(content)

        return json.dumps({
            "source":         log_group,
            "service":        service,
            "event_count":    len(events),
            "content_sample": content[:10_000],
            "fast_diagnosis": fast_dx,
        })

    except logs.exceptions.ResourceNotFoundException:
        return json.dumps({"error": f"Log group not found: {log_group}", "source": log_group})
    except Exception as exc:
        return json.dumps({"error": str(exc), "source": log_group})


@tool
def extract_error_blocks(content: str, service: str = "generic",
                          max_blocks: int = 20) -> str:
    """
    Extract de-duplicated error blocks from raw log content.

    Args:
        content:    Raw log text (from fetch_s3_logs or fetch_cloudwatch_logs).
        service:    Service hint for targeted pattern matching.
        max_blocks: Maximum unique error blocks to return (default 20).

    Returns:
        JSON list of unique error strings.
    """
    error_patterns = [
        r"(?i)(ERROR|CRITICAL|FATAL|Exception|Traceback|Failed|FAILED)[^\n]*\n?(?:  [^\n]*\n?){0,5}",
        r"errorMessage['\": ]+[^\n\"']+",
        r"(?i)caused by[^\n]+",
    ]
    seen: set[str] = set()
    blocks: list[str] = []

    for pat in error_patterns:
        for m in re.finditer(pat, content):
            block = m.group(0).strip()[:500]
            key   = re.sub(r"\s+", " ", block)[:100]
            if key not in seen:
                seen.add(key)
                blocks.append(block)
            if len(blocks) >= max_blocks:
                break

    return json.dumps({
        "service":      service,
        "error_blocks": blocks[:max_blocks],
        "count":        len(blocks),
    })
