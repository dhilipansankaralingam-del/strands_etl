"""
Spark Event Log Agent
=====================
Parses a Spark event log (JSONL) from a local path or S3 URI and extracts
stage durations, shuffle I/O, task skew, GC overhead, and bottleneck stages.

Based on SparkEventLogParser from PR-11 (table-optimizer-analysis).
"""

import gzip
import json
import logging
import statistics
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a **Spark Performance Analyst** specialising in post-execution diagnostics.

Given a parsed Spark event log, identify:
1. Bottleneck stages (high skew, long duration, high shuffle, failed tasks)
2. GC overhead (> 10% → memory pressure)
3. Shuffle hot-spots (> 10 GB total shuffle write)
4. Task skew ratio (max_task_ms / median_task_ms > 5× → severe skew)
5. Actionable Spark configs and code fixes to resolve each issue

Return structured JSON:
{
  "app_name": "...",
  "app_duration_ms": N,
  "shuffle_read_gb": F,
  "shuffle_write_gb": F,
  "gc_overhead_pct": F,
  "peak_executor_mb": F,
  "bottleneck_stages": [],
  "findings": [],
  "recommendations": []
}
Return ONLY valid JSON.
"""

_SKEW_RATIO_HIGH   = 5.0
_GC_OVERHEAD_HIGH  = 0.10
_SHUFFLE_WRITE_HIGH = 10.0
_STAGE_SLOW_MS     = 300_000
_FAILED_TASKS_WARN = 3


def _read_log_lines(log_path: str, region: str = "us-west-2") -> List[bytes]:
    if log_path.startswith(("s3://", "s3a://", "s3n://")):
        try:
            import boto3
            uri    = log_path.replace("s3a://", "s3://").replace("s3n://", "s3://")
            bucket = uri[5:].split("/", 1)[0]
            key    = uri[5:].split("/", 1)[1]
            body   = boto3.client("s3", region_name=region).get_object(Bucket=bucket, Key=key)["Body"].read()
            if body[:2] == b"\x1f\x8b":
                body = gzip.decompress(body)
            return body.splitlines()
        except Exception as exc:
            raise RuntimeError(f"S3 read failed for {log_path}: {exc}")
    p = Path(log_path)
    if not p.exists():
        raise FileNotFoundError(f"Event log not found: {log_path}")
    raw = p.read_bytes()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw.splitlines()


def _extract_accum(accums: List, name_fragment: str) -> Optional[float]:
    for a in accums:
        if isinstance(a, dict) and name_fragment in a.get("Name", ""):
            v = a.get("Value", a.get("Update", None))
            if v is not None:
                return float(v)
    return None


def _process_events(lines: List[bytes]) -> Dict[str, Any]:
    app_name  = "unknown"
    app_start = app_end = 0
    stages:     Dict[int, Dict] = {}
    task_times: Dict[int, List[int]] = {}
    total_gc_ms = total_exec_ms = 0
    peak_exec_mb = 0.0
    total_sr = total_sw = 0

    for raw in lines:
        if not raw.strip():
            continue
        try:
            ev = json.loads(raw)
        except Exception:
            continue
        etype = ev.get("Event", "")

        if etype == "SparkListenerApplicationStart":
            app_name  = ev.get("App Name", "unknown")
            app_start = ev.get("Timestamp", 0)
        elif etype == "SparkListenerApplicationEnd":
            app_end = ev.get("Timestamp", 0)
        elif etype == "SparkListenerStageSubmitted":
            info = ev.get("Stage Info", {})
            sid  = info.get("Stage ID", -1)
            stages[sid] = {
                "stage_id":   sid,
                "name":       info.get("Stage Name", ""),
                "task_count": info.get("Number of Tasks", 0),
                "duration_ms": 0,
                "failed_tasks": 0,
                "shuffle_read_bytes": 0,
                "shuffle_write_bytes": 0,
            }
            task_times[sid] = []
        elif etype == "SparkListenerStageCompleted":
            info   = ev.get("Stage Info", {})
            sid    = info.get("Stage ID", -1)
            sub_ms = info.get("Submission Time", 0)
            cmp_ms = info.get("Completion Time", 0)
            dur_ms = max(0, cmp_ms - sub_ms)
            accums = info.get("Accumulables", [])
            sr     = _extract_accum(accums, "shuffle.read.fetchWaitTime") or 0
            sw     = _extract_accum(accums, "shuffle.write.bytesWritten") or 0
            total_sr += int(sr)
            total_sw += int(sw)
            if sid in stages:
                stages[sid].update({"duration_ms": dur_ms,
                                    "shuffle_read_bytes": int(sr),
                                    "shuffle_write_bytes": int(sw)})
        elif etype == "SparkListenerTaskEnd":
            tm      = ev.get("Task Metrics", {})
            sid     = ev.get("Stage ID", -1)
            exec_ms = tm.get("Executor Run Time", 0)
            gc_ms   = tm.get("JVM GC Time", 0)
            peak_mb = tm.get("Peak Execution Memory", 0) / (1024 ** 2)
            failed  = ev.get("Task End Reason", {}).get("Reason", "") not in ("Success", "")
            total_exec_ms  += exec_ms
            total_gc_ms    += gc_ms
            peak_exec_mb    = max(peak_exec_mb, peak_mb)
            if sid in task_times:
                task_times[sid].append(exec_ms)
            if failed and sid in stages:
                stages[sid]["failed_tasks"] += 1

    # Compute skew per stage
    skew: Dict[int, float] = {}
    for sid, times in task_times.items():
        if len(times) > 1:
            median = statistics.median(times)
            skew[sid] = round(max(times) / max(median, 1), 2)

    gc_pct = (total_gc_ms / max(total_exec_ms, 1)) * 100

    return {
        "app_name":        app_name,
        "app_duration_ms": max(0, app_end - app_start),
        "stage_durations": stages,
        "shuffle_read_gb": round(total_sr / (1024 ** 3), 3),
        "shuffle_write_gb": round(total_sw / (1024 ** 3), 3),
        "task_skew":       skew,
        "gc_overhead_pct": round(gc_pct, 2),
        "peak_executor_mb": round(peak_exec_mb, 1),
    }


def _build_findings_and_recs(metrics: Dict) -> Tuple[List[str], List[Dict], List[Dict]]:
    findings: List[str] = []
    recs:     List[Dict] = []
    bottlenecks: List[Dict] = []

    stages = metrics.get("stage_durations", {})
    skew   = metrics.get("task_skew", {})

    for sid, stage in stages.items():
        issues: List[str] = []
        skew_r = skew.get(int(sid), 0)

        if skew_r >= _SKEW_RATIO_HIGH:
            issues.append(f"skew ratio {skew_r:.1f}×")
            findings.append(f"Stage {sid} ({stage.get('name','')[:40]}): severe task skew ({skew_r:.1f}×)")
            recs.append({"issue": f"Data skew in stage {sid}", "stage": sid,
                         "recommendation": "Enable AQE skewJoin; consider key salting",
                         "spark_config": "spark.sql.adaptive.skewJoin.enabled=true"})

        dur = stage.get("duration_ms", 0)
        if dur > _STAGE_SLOW_MS:
            issues.append(f"duration {dur/1000:.0f}s")
            findings.append(f"Stage {sid}: long duration ({dur/1000:.0f}s). Check for shuffle spill or skew.")

        sw_gb = stage.get("shuffle_write_bytes", 0) / (1024 ** 3)
        if sw_gb > 5:
            issues.append(f"shuffle write {sw_gb:.1f} GB")
            findings.append(f"Stage {sid}: high shuffle write ({sw_gb:.1f} GB). Consider repartitioning or AQE coalesce.")
            recs.append({"issue": f"High shuffle in stage {sid}", "stage": sid,
                         "recommendation": "Enable AQE coalesce; tune spark.sql.shuffle.partitions",
                         "spark_config": "spark.sql.adaptive.coalescePartitions.enabled=true"})

        failed = stage.get("failed_tasks", 0)
        if failed >= _FAILED_TASKS_WARN:
            issues.append(f"{failed} failed tasks")
            findings.append(f"Stage {sid}: {failed} failed tasks — check for OOM or bad records.")
            recs.append({"issue": f"Failed tasks in stage {sid}", "stage": sid,
                         "recommendation": "Check executor memory; add .repartition() before the failing stage"})

        if issues:
            bottlenecks.append({"stage_id": sid, "stage_name": stage.get("name",""),
                                 "duration_ms": dur, "issues": issues, "skew_ratio": skew_r})

    gc_pct = metrics.get("gc_overhead_pct", 0)
    if gc_pct > _GC_OVERHEAD_HIGH * 100:
        findings.append(f"High GC overhead ({gc_pct:.1f}%). Executors are under memory pressure.")
        recs.append({"issue": "GC overhead", "recommendation": "Increase executor memory or enable G1GC",
                     "spark_config": "-XX:+UseG1GC -XX:G1HeapRegionSize=32M"})

    sw_gb = metrics.get("shuffle_write_gb", 0)
    if sw_gb > _SHUFFLE_WRITE_HIGH:
        findings.append(f"Total shuffle write {sw_gb:.1f} GB. Consider caching and broadcast joins.")
        recs.append({"issue": "High total shuffle", "recommendation": "Cache reused DataFrames; broadcast small tables",
                     "spark_config": "spark.sql.autoBroadcastJoinThreshold=104857600"})

    if not findings:
        findings.append("Spark event log shows no critical performance issues.")

    return findings, recs, bottlenecks


# ── Tools ──────────────────────────────────────────────────────────────────────

@tool
def parse_spark_event_log(log_path: str, region: str = "us-west-2") -> str:
    """
    Parse a Spark event log file (local or S3) and extract performance metrics.

    Args:
        log_path: Local path or s3://bucket/key URI to the Spark event log.
        region:   AWS region for S3 access (default us-west-2).

    Returns:
        JSON with app_name, shuffle_read_gb, shuffle_write_gb, gc_overhead_pct,
        peak_executor_mb, stage_durations, task_skew, findings, recommendations.
    """
    try:
        lines   = _read_log_lines(log_path, region)
        metrics = _process_events(lines)
        findings, recs, bottlenecks = _build_findings_and_recs(metrics)
        metrics["findings"]          = findings
        metrics["recommendations"]   = recs
        metrics["bottleneck_stages"] = bottlenecks
        metrics["success"]           = True
        # Convert stage_durations keys to str for JSON serialisation
        metrics["stage_durations"] = {str(k): v for k, v in metrics["stage_durations"].items()}
        metrics["task_skew"]       = {str(k): v for k, v in metrics["task_skew"].items()}
        return json.dumps(metrics)
    except Exception as exc:
        logger.error("Event log parse failed: %s", exc)
        return json.dumps({"success": False, "error": str(exc), "findings": [], "recommendations": []})


@tool
def get_bottleneck_stages(log_path: str, region: str = "us-west-2") -> str:
    """
    Return only the bottleneck stages from a Spark event log (skew, long duration, failures).

    Args:
        log_path: Local path or s3://bucket/key to the event log.
        region:   AWS region.

    Returns:
        JSON list of bottleneck stage dicts with stage_id, issues, skew_ratio.
    """
    try:
        result = json.loads(parse_spark_event_log.__wrapped__(log_path, region))
        return json.dumps(result.get("bottleneck_stages", []))
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def analyse_task_skew(log_path: str, skew_threshold: float = 5.0, region: str = "us-west-2") -> str:
    """
    Identify stages where max_task_time / median_task_time exceeds the threshold.

    Args:
        log_path:       Local path or S3 URI.
        skew_threshold: Ratio above which a stage is considered skewed (default 5.0).
        region:         AWS region.

    Returns:
        JSON dict of skewed stage IDs → skew ratio and remediation suggestions.
    """
    try:
        lines   = _read_log_lines(log_path, region)
        metrics = _process_events(lines)
        skew    = metrics.get("task_skew", {})
        severe  = {sid: {"skew_ratio": r,
                         "remediation": "Key salting or AQE skewJoin hint"}
                   for sid, r in skew.items() if r >= skew_threshold}
        return json.dumps({"skewed_stages": severe, "total_stages_checked": len(skew)})
    except Exception as exc:
        return json.dumps({"error": str(exc)})


def create_spark_event_log_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                                  region: str = "us-west-2") -> Agent:
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(model=model, system_prompt=SYSTEM_PROMPT,
                 tools=[parse_spark_event_log, get_bottleneck_stages, analyse_task_skew])
