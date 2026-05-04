"""
Spark Event Log Parser
=======================
Parses a Spark event log (JSONL format) from a local path or S3 URI and
extracts actionable performance metrics.

Spark writes one JSON event per line to its event log.  Relevant events:

  SparkListenerApplicationStart   → application name, start time
  SparkListenerJobStart           → job ID, submission time
  SparkListenerJobEnd             → job completion time
  SparkListenerStageSubmitted     → stage info (task count, shuffle read)
  SparkListenerStageCompleted     → stage duration, shuffle bytes, failed tasks
  SparkListenerTaskEnd            → per-task: executor time, GC time, shuffle R/W

Metrics extracted
-----------------
  stage_durations   – {stageId: {name, duration_ms, task_count, failed_tasks}}
  shuffle_read_gb   – total shuffle read bytes → GB
  shuffle_write_gb  – total shuffle write bytes → GB
  task_skew         – per stage: max_task_ms / median_task_ms
  gc_overhead_pct   – total GC time / total executor CPU time × 100
  peak_executor_mb  – max peak execution memory seen across all tasks
  bottleneck_stages – stages with: high skew, long duration, high GC, or high shuffle
  findings          – human-readable list of detected performance issues
  recommendations   – list of {issue, recommendation, spark_config} dicts

Usage
-----
  from cost_optimizer.agents.spark_event_log_parser import SparkEventLogParser

  parser = SparkEventLogParser()
  result = parser.parse("s3://my-bucket/spark-logs/app-20240101-123456")
  # or
  result = parser.parse("/tmp/event_log.json")
"""
from __future__ import annotations

import gzip
import io
import json
import re
import statistics
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple


# ─── Thresholds ────────────────────────────────────────────────────────────────
_SKEW_RATIO_HIGH    = 5.0    # max_task / median_task time
_GC_OVERHEAD_HIGH   = 0.10   # 10% GC overhead is a concern
_SHUFFLE_WRITE_HIGH = 10.0   # GB – high shuffle indicates shuffle-heavy job
_STAGE_SLOW_MS      = 300_000  # 5 minutes – long-running stage warning
_FAILED_TASKS_WARN  = 3      # more than this many failed tasks = warning


class SparkEventLogParser:
    """Parse a Spark event log and extract performance signals."""

    # ─── Public entry point ────────────────────────────────────────────────────

    def parse(
        self,
        log_path: str,
        region: str = "us-east-1",
    ) -> Dict[str, Any]:
        """
        Parse a Spark event log from a local path or S3 URI.

        Args
        ----
        log_path : Local filesystem path or s3://bucket/key URI.
        region   : AWS region for S3 access.

        Returns
        -------
        success              – bool
        app_name             – str
        app_duration_ms      – int
        stage_durations      – dict stageId → stage metrics
        shuffle_read_gb      – float
        shuffle_write_gb     – float
        task_skew            – dict stageId → skew_ratio
        gc_overhead_pct      – float
        peak_executor_mb     – float
        bottleneck_stages    – list[dict]
        findings             – list[str]
        recommendations      – list[dict]
        errors               – list[str]
        """
        try:
            lines = list(self._read_log(log_path, region))
        except Exception as exc:
            return {"success": False, "errors": [str(exc)]}

        try:
            metrics = self._process_events(lines)
            findings, recs = _analyze_metrics(metrics)
            metrics["findings"]       = findings
            metrics["recommendations"] = recs
            metrics["success"]        = True
            metrics["errors"]         = []
            return metrics
        except Exception as exc:
            return {"success": False, "errors": [str(exc)]}

    # ─── Log reader ───────────────────────────────────────────────────────────

    def _read_log(self, log_path: str, region: str) -> Iterator[bytes]:
        """Yield raw bytes lines from a local file or S3 object."""
        if log_path.startswith("s3://") or log_path.startswith("s3a://") or \
           log_path.startswith("s3n://"):
            yield from self._read_s3(log_path, region)
        else:
            p = Path(log_path)
            if not p.exists():
                raise FileNotFoundError(f"Event log not found: {log_path}")
            raw = p.read_bytes()
            if raw[:2] == b'\x1f\x8b':  # gzip magic
                raw = gzip.decompress(raw)
            yield from raw.splitlines()

    def _read_s3(self, s3_uri: str, region: str) -> Iterator[bytes]:
        try:
            import boto3
            uri    = s3_uri.replace("s3n://", "s3://").replace("s3a://", "s3://")
            bucket = uri[5:].split("/", 1)[0]
            key    = uri[5:].split("/", 1)[1]
            s3     = boto3.client("s3", region_name=region)
            resp   = s3.get_object(Bucket=bucket, Key=key)
            body   = resp["Body"].read()
            if body[:2] == b'\x1f\x8b':
                body = gzip.decompress(body)
            yield from body.splitlines()
        except Exception as exc:
            raise RuntimeError(f"Failed to read S3 event log {s3_uri}: {exc}")

    # ─── Event processor ─────────────────────────────────────────────────────

    def _process_events(self, lines: List[bytes]) -> Dict[str, Any]:
        """Parse each event and accumulate metrics."""
        # Accumulators
        app_name            = "unknown"
        app_start_ms: int   = 0
        app_end_ms: int     = 0
        stages:  Dict[int, Dict]  = {}  # stageId → stage info
        task_times: Dict[int, List[int]] = {}  # stageId → [executor_ms]
        total_gc_ms:    int = 0
        total_exec_ms:  int = 0
        peak_exec_mb: float = 0.0
        total_shuffle_read:  int = 0
        total_shuffle_write: int = 0

        for raw in lines:
            if not raw.strip():
                continue
            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                continue

            etype = event.get("Event", "")

            if etype == "SparkListenerApplicationStart":
                app_name     = event.get("App Name", "unknown")
                app_start_ms = event.get("Timestamp", 0)

            elif etype == "SparkListenerApplicationEnd":
                app_end_ms = event.get("Timestamp", 0)

            elif etype == "SparkListenerStageSubmitted":
                info = event.get("Stage Info", {})
                sid  = info.get("Stage ID", -1)
                stages[sid] = {
                    "stage_id":   sid,
                    "name":       info.get("Stage Name", ""),
                    "task_count": info.get("Number of Tasks", 0),
                    "submit_ms":  0,
                    "end_ms":     0,
                    "duration_ms": 0,
                    "failed_tasks": 0,
                    "shuffle_read_bytes":  0,
                    "shuffle_write_bytes": 0,
                }
                task_times[sid] = []

            elif etype == "SparkListenerStageCompleted":
                info   = event.get("Stage Info", {})
                sid    = info.get("Stage ID", -1)
                sub_ms = info.get("Submission Time", 0)
                cmp_ms = info.get("Completion Time", 0)
                dur_ms = cmp_ms - sub_ms if cmp_ms > sub_ms else 0

                # Shuffle metrics from stage-level accumulator
                task_metrics = info.get("Task Metrics", info.get("Accumulables", []))
                sr = _extract_accumulator(task_metrics, "internal.metrics.shuffle.read.fetchWaitTime") or 0
                sw = _extract_accumulator(task_metrics, "internal.metrics.shuffle.write.bytesWritten") or 0

                if sid in stages:
                    stages[sid].update({
                        "submit_ms":           sub_ms,
                        "end_ms":              cmp_ms,
                        "duration_ms":         dur_ms,
                        "failed_tasks":        info.get("Number of Failed Tasks", 0),
                        "shuffle_read_bytes":  sr,
                        "shuffle_write_bytes": sw,
                    })

            elif etype == "SparkListenerTaskEnd":
                task_info    = event.get("Task Info", {})
                task_metrics = event.get("Task Metrics", {})
                sid          = event.get("Stage ID", -1)

                exec_ms = task_metrics.get("Executor Run Time", 0)
                gc_ms   = task_metrics.get("JVM GC Time", 0)
                peak_mb = task_metrics.get("Peak Execution Memory", 0) / (1024 * 1024)

                sr_bytes = (task_metrics.get("Shuffle Read Metrics", {})
                            .get("Total Bytes Read", 0))
                sw_bytes = (task_metrics.get("Shuffle Write Metrics", {})
                            .get("Bytes Written", 0))

                total_exec_ms         += exec_ms
                total_gc_ms           += gc_ms
                peak_exec_mb           = max(peak_exec_mb, peak_mb)
                total_shuffle_read    += sr_bytes
                total_shuffle_write   += sw_bytes

                if sid not in task_times:
                    task_times[sid] = []
                if exec_ms > 0:
                    task_times[sid].append(exec_ms)

                # Update stage-level failed task count from task end
                if task_info.get("Failed", False) and sid in stages:
                    stages[sid]["failed_tasks"] = stages[sid].get("failed_tasks", 0) + 1

        # ── Compute per-stage skew ─────────────────────────────────────────────
        task_skew: Dict[int, float] = {}
        for sid, times in task_times.items():
            if len(times) >= 2:
                median_t = statistics.median(times)
                max_t    = max(times)
                task_skew[sid] = round(max_t / max(median_t, 1), 2)
            elif len(times) == 1:
                task_skew[sid] = 1.0

        gc_overhead_pct = (
            round(total_gc_ms / total_exec_ms * 100, 2)
            if total_exec_ms > 0 else 0.0
        )

        bottleneck_stages = _find_bottleneck_stages(stages, task_skew)

        return {
            "app_name":           app_name,
            "app_duration_ms":    (app_end_ms - app_start_ms) if app_end_ms > app_start_ms else 0,
            "stage_count":        len(stages),
            "stage_durations":    stages,
            "shuffle_read_gb":    round(total_shuffle_read  / (1024**3), 3),
            "shuffle_write_gb":   round(total_shuffle_write / (1024**3), 3),
            "task_skew":          task_skew,
            "gc_overhead_pct":    gc_overhead_pct,
            "peak_executor_mb":   round(peak_exec_mb, 1),
            "bottleneck_stages":  bottleneck_stages,
        }


# ─── Helpers ───────────────────────────────────────────────────────────────────

def _extract_accumulator(accumulables: Any, name: str) -> Optional[int]:
    """Extract an accumulator value by name from a list of stage accumulators."""
    if isinstance(accumulables, list):
        for acc in accumulables:
            if isinstance(acc, dict) and name in acc.get("Name", ""):
                try:
                    return int(acc.get("Value", 0))
                except (ValueError, TypeError):
                    pass
    return None


def _find_bottleneck_stages(
    stages: Dict[int, Dict], task_skew: Dict[int, float]
) -> List[Dict]:
    bottlenecks = []
    for sid, stage in stages.items():
        issues = []
        skew   = task_skew.get(sid, 1.0)

        if skew > _SKEW_RATIO_HIGH:
            issues.append(f"task skew: max/median ratio = {skew:.1f}×")
        if stage.get("duration_ms", 0) > _STAGE_SLOW_MS:
            dur_min = stage["duration_ms"] / 60000
            issues.append(f"long duration: {dur_min:.1f} min")
        if stage.get("failed_tasks", 0) > _FAILED_TASKS_WARN:
            issues.append(f"failed tasks: {stage['failed_tasks']}")
        sw_gb = stage.get("shuffle_write_bytes", 0) / (1024**3)
        if sw_gb > _SHUFFLE_WRITE_HIGH:
            issues.append(f"high shuffle write: {sw_gb:.1f} GB")

        if issues:
            bottlenecks.append({
                "stage_id":   sid,
                "name":       stage.get("name", ""),
                "duration_ms": stage.get("duration_ms", 0),
                "task_count": stage.get("task_count", 0),
                "skew_ratio": skew,
                "issues":     issues,
            })
    return sorted(bottlenecks, key=lambda x: x["duration_ms"], reverse=True)


# ─── Analysis & recommendations ───────────────────────────────────────────────

def _analyze_metrics(m: Dict) -> Tuple[List[str], List[Dict]]:
    """Derive findings and recommendations from parsed metrics."""
    findings: List[str] = []
    recs:     List[Dict] = []

    gc_pct    = m.get("gc_overhead_pct", 0.0)
    shuf_w_gb = m.get("shuffle_write_gb", 0.0)
    shuf_r_gb = m.get("shuffle_read_gb", 0.0)
    peak_mb   = m.get("peak_executor_mb", 0.0)
    bottlenecks = m.get("bottleneck_stages", [])

    # ── GC overhead ───────────────────────────────────────────────────────────
    if gc_pct > _GC_OVERHEAD_HIGH * 100:
        findings.append(
            f"High GC overhead: {gc_pct:.1f}% of executor CPU time spent in GC. "
            "Consider increasing executor memory or enabling G1GC."
        )
        recs.append({
            "issue":           "high_gc_overhead",
            "recommendation":  (
                f"GC is consuming {gc_pct:.1f}% of executor runtime. "
                "Increase executor memory or switch to G1GC."
            ),
            "spark_configs": {
                "spark.executor.extraJavaOptions": (
                    "-XX:+UseG1GC -XX:G1HeapRegionSize=32M "
                    "-XX:InitiatingHeapOccupancyPercent=35"
                ),
                "spark.executor.memoryOverhead": "2g",
            },
        })

    # ── Shuffle ───────────────────────────────────────────────────────────────
    if shuf_w_gb > _SHUFFLE_WRITE_HIGH:
        findings.append(
            f"Heavy shuffle: {shuf_w_gb:.1f} GB written, {shuf_r_gb:.1f} GB read. "
            "This indicates expensive sort-merge joins or wide aggregations."
        )
        recs.append({
            "issue":          "heavy_shuffle",
            "recommendation": (
                "Reduce shuffle by: (1) using broadcast joins for small tables, "
                "(2) enabling AQE to coalesce shuffle partitions, "
                "(3) pre-filtering data before joins."
            ),
            "spark_configs": {
                "spark.sql.adaptive.enabled":                    "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.autoBroadcastJoinThreshold":          "104857600",  # 100 MB
                "spark.shuffle.compress":                        "true",
                "spark.reducer.maxSizeInFlight":                 "96m",
            },
        })

    # ── Task skew ─────────────────────────────────────────────────────────────
    skewed = [b for b in bottlenecks if b["skew_ratio"] > _SKEW_RATIO_HIGH]
    if skewed:
        top = skewed[0]
        findings.append(
            f"Data skew detected in stage {top['stage_id']} "
            f"({top['name']}): max/median task ratio = {top['skew_ratio']:.1f}×. "
            "Some tasks process significantly more data than others."
        )
        recs.append({
            "issue":          "data_skew",
            "stage_id":       top["stage_id"],
            "skew_ratio":     top["skew_ratio"],
            "recommendation": (
                "Enable AQE skewJoin handling or apply key salting. "
                "Identify the join/groupBy key causing skew and distribute it."
            ),
            "spark_configs": {
                "spark.sql.adaptive.skewJoin.enabled":              "true",
                "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "3",
                "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "67108864",
            },
        })

    # ── Peak memory ───────────────────────────────────────────────────────────
    if peak_mb > 0:
        peak_gb = peak_mb / 1024
        findings.append(f"Peak executor memory: {peak_gb:.2f} GB per task.")
        if peak_mb > 4 * 1024:  # > 4 GB
            recs.append({
                "issue":          "high_peak_memory",
                "recommendation": (
                    f"Peak task memory is {peak_gb:.1f} GB. "
                    "Consider increasing executor memory or reducing partition size "
                    "with spark.sql.files.maxPartitionBytes."
                ),
                "spark_configs": {
                    "spark.executor.memory":                     f"{max(4, int(peak_gb * 1.5))}g",
                    "spark.executor.memoryOverhead":             "2g",
                    "spark.sql.files.maxPartitionBytes":         "134217728",
                    "spark.memory.fraction":                     "0.8",
                },
            })

    # ── Long stages ───────────────────────────────────────────────────────────
    slow_stages = [b for b in bottlenecks if b["duration_ms"] > _STAGE_SLOW_MS]
    if slow_stages:
        top = slow_stages[0]
        dur_min = top["duration_ms"] / 60000
        findings.append(
            f"Stage {top['stage_id']} ({top['name']}) took {dur_min:.1f} minutes "
            f"with {top['task_count']} tasks. This is the slowest stage."
        )
        recs.append({
            "issue":          "slow_stage",
            "stage_id":       top["stage_id"],
            "duration_min":   round(dur_min, 1),
            "recommendation": (
                "Investigate this stage for: (1) data skew, (2) insufficient parallelism, "
                "(3) large partition sizes. Consider increasing spark.sql.shuffle.partitions "
                "or adding more workers."
            ),
        })

    if not findings:
        findings.append("No major performance issues detected in this Spark event log.")

    return findings, recs
