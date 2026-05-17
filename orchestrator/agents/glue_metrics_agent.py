"""
Glue Metrics Agent
==================
Fetches AWS Glue CloudWatch metrics for a running/completed job and translates
JVM heap, CPU, and worker-utilisation signals into actionable Spark config
overrides and worker-count recommendations.

Incorporates GlueMetricsAnalyzer from PR-11 (table-optimizer-analysis).
"""

import json
import logging
import statistics
from typing import Any, Dict, List, Optional

from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a **Glue Performance Specialist** who interprets AWS CloudWatch metrics
from Glue jobs to produce concrete Spark configuration overrides and worker
right-sizing recommendations.

Thresholds:
  JVM Heap  > 80% p90 → HIGH memory pressure → increase executor memory / upgrade worker type
  JVM Heap  < 40% p90 → LOW  → over-provisioned on memory → downgrade worker type
  Worker CPU> 80% avg → overloaded → add workers or upgrade type
  Worker CPU< 30% avg → idle     → reduce workers or downgrade type
  Workers utilised / allocated < 70% → over-provisioned workers
  Workers utilised / allocated > 95% → under-provisioned workers

Return structured JSON with:
{
  "heap_pressure": "none|low|medium|high|critical",
  "cpu_worker_load": "idle|normal|high|overloaded",
  "worker_utilisation": "under|optimal|over|unknown",
  "findings": [],
  "spark_config_overrides": {},
  "worker_recommendation": { "recommended_workers": N, "recommended_type": "...", "changed": bool }
}
Return ONLY valid JSON.
"""

# ── Threshold constants ───────────────────────────────────────────────────────
_HEAP_HIGH = 0.80
_HEAP_LOW  = 0.40
_CPU_HIGH  = 0.80
_CPU_LOW   = 0.30
_UTIL_LOW  = 0.70
_UTIL_HIGH = 0.95

_TYPE_UPGRADE   = {"G.1X": "G.2X", "G.2X": "G.4X", "G.4X": "G.8X"}
_TYPE_DOWNGRADE = {"G.8X": "G.4X", "G.4X": "G.2X", "G.2X": "G.1X"}


def _normalize(raw: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    """Normalize raw metric dict to {key: {avg, max, min, p90, last}}."""
    import re
    parsed: Dict[str, Dict[str, float]] = {}
    for key, val in raw.items():
        # canonicalize per-worker CPU keys
        canon = re.sub(r'^glue\.\d+\.system\.', 'glue.ALL.system.', key)
        if isinstance(val, dict) and "Datapoints" in val:
            series = [dp.get("Average", dp.get("Maximum", 0.0)) for dp in val["Datapoints"]]
        elif isinstance(val, list):
            series = [float(v) for v in val if v is not None]
        else:
            series = [float(val)]
        if not series:
            continue
        parsed[canon] = {
            "avg": statistics.mean(series),
            "max": max(series),
            "min": min(series),
            "p90": sorted(series)[int(len(series) * 0.90)],
            "last": series[-1],
        }
    return parsed


def _analyse(metrics: Dict[str, Dict[str, float]], current_workers: int,
             current_type: str, exec_mem_gb: float) -> Dict[str, Any]:
    findings: List[str] = []

    # ── Heap ──────────────────────────────────────────────────────────────────
    heap_p90 = metrics.get("glue.ALL.jvm.heap.usage", {}).get("p90", 0.0)
    heap_max = metrics.get("glue.ALL.jvm.heap.usage", {}).get("max", 0.0)
    if heap_max >= 0.95:
        heap_pressure = "critical"
        findings.append(f"CRITICAL: JVM heap peaked at {heap_max:.0%} — OOM risk. Upgrade worker type immediately.")
    elif heap_p90 >= _HEAP_HIGH:
        heap_pressure = "high"
        findings.append(f"HIGH heap pressure (p90={heap_p90:.0%}). Increase executor memory 50% or upgrade worker type.")
    elif heap_p90 >= 0.65:
        heap_pressure = "medium"
        findings.append(f"Moderate heap pressure (p90={heap_p90:.0%}). Consider MEMORY_AND_DISK_SER storage level.")
    elif 0 < heap_p90 < _HEAP_LOW:
        heap_pressure = "low"
        findings.append(f"JVM heap only {heap_p90:.0%} — over-provisioned on memory; downgrade worker type to save cost.")
    else:
        heap_pressure = "none"

    # ── CPU – workers ─────────────────────────────────────────────────────────
    cpu_avg = metrics.get("glue.ALL.system.cpuSystemLoad", {}).get("avg", 0.0)
    if cpu_avg >= _CPU_HIGH:
        cpu_load = "overloaded"
        findings.append(f"Worker CPU overloaded ({cpu_avg:.0%} avg). Add workers or upgrade type.")
    elif 0 < cpu_avg < _CPU_LOW:
        cpu_load = "idle"
        findings.append(f"Worker CPUs idle ({cpu_avg:.0%} avg). Reduce worker count or increase spark.executor.cores.")
    else:
        cpu_load = "normal"

    # ── Worker utilisation ────────────────────────────────────────────────────
    wu_avg  = metrics.get("glue.driver.workerutilized", {}).get("avg", 0.0)
    nae_max = metrics.get(
        "glue.driver.ExecutorAllocationManager.executors.numberAllExecutors", {}
    ).get("max", wu_avg or float(current_workers))
    if nae_max > 0 and wu_avg > 0:
        ratio = wu_avg / nae_max
        if ratio < _UTIL_LOW:
            w_util = "under"
            findings.append(f"Workers under-utilised ({ratio:.0%}). Reduce number_of_workers by ~{int((1 - ratio)*100)}%.")
        elif ratio >= _UTIL_HIGH:
            w_util = "over"
            findings.append(f"Workers maxed out ({ratio:.0%}). Increase number_of_workers by 20–30%.")
        else:
            w_util = "optimal"
    else:
        w_util = "unknown"

    if not findings:
        findings.append("All Glue metrics within healthy ranges — no urgent tuning needed.")

    # ── Spark config overrides ────────────────────────────────────────────────
    overrides: Dict[str, str] = {}
    if heap_pressure in ("high", "critical"):
        new_mem = max(8, int(exec_mem_gb * 1.5))
        overrides.update({
            "spark.executor.memory":         f"{new_mem}g",
            "spark.executor.memoryOverhead": f"{max(2, new_mem // 4)}g",
            "spark.memory.fraction":         "0.80",
            "spark.rdd.compress":            "true",
            "spark.shuffle.compress":        "true",
            "spark.executor.extraJavaOptions": (
                "-XX:+UseG1GC -XX:G1HeapRegionSize=32M "
                "-XX:InitiatingHeapOccupancyPercent=35"
            ),
        })
    elif heap_pressure == "medium":
        overrides["spark.memory.fraction"] = "0.75"
        overrides["spark.rdd.compress"]    = "true"
    elif heap_pressure == "low":
        safe_mem = max(4, int(exec_mem_gb * 0.75))
        overrides["spark.executor.memory"]  = f"{safe_mem}g"
        overrides["spark.memory.fraction"]  = "0.60"

    if cpu_load == "overloaded":
        overrides["spark.executor.cores"] = "2"
    elif cpu_load == "idle":
        overrides["spark.executor.cores"] = "4"

    if w_util in ("under", "over"):
        overrides["spark.dynamicAllocation.enabled"]                  = "true"
        overrides["spark.dynamicAllocation.shuffleTracking.enabled"]  = "true"
        if w_util == "under":
            ideal = max(2, int(wu_avg * 1.1))
            overrides["spark.dynamicAllocation.maxExecutors"] = str(ideal)
        else:
            ideal = int(nae_max * 1.3)
            overrides["spark.dynamicAllocation.maxExecutors"] = str(ideal)

    # ── Worker recommendation ─────────────────────────────────────────────────
    new_workers = current_workers
    new_type    = current_type
    reasons: List[str] = []

    if w_util == "under":
        new_workers = max(2, int(wu_avg * 1.1))
        reasons.append(f"Under-utilised: avg {wu_avg:.1f}/{current_workers} workers used")
    elif w_util == "over":
        new_workers = int(current_workers * 1.25)
        reasons.append("Workers maxed out — increase to reduce task queuing")

    if heap_pressure in ("high", "critical"):
        upgraded = _TYPE_UPGRADE.get(current_type)
        if upgraded:
            new_type = upgraded
            reasons.append(f"High heap pressure → upgrade {current_type} → {upgraded}")
    elif heap_pressure == "low" and cpu_load == "idle" and w_util == "under":
        downgraded = _TYPE_DOWNGRADE.get(current_type)
        if downgraded:
            new_type = downgraded
            reasons.append(f"All metrics low → downgrade {current_type} → {downgraded} to save cost")

    return {
        "heap_pressure":        heap_pressure,
        "cpu_worker_load":      cpu_load,
        "worker_utilisation":   w_util,
        "findings":             findings,
        "spark_config_overrides": overrides,
        "worker_recommendation": {
            "current_workers":     current_workers,
            "recommended_workers": new_workers,
            "current_type":        current_type,
            "recommended_type":    new_type,
            "changed":             new_workers != current_workers or new_type != current_type,
            "reasons":             reasons,
        },
        "raw_values": {
            "heap_p90":      round(heap_p90, 3),
            "heap_max":      round(heap_max, 3),
            "cpu_avg":       round(cpu_avg, 3),
            "workers_avg":   round(wu_avg, 2),
            "executors_max": round(nae_max, 2),
        },
    }


# ── Tools ─────────────────────────────────────────────────────────────────────

@tool
def fetch_glue_metrics_from_cloudwatch(
    job_name: str,
    run_id: str,
    region: str = "us-west-2",
) -> str:
    """
    Fetch Glue CloudWatch metrics for a completed job run.

    Args:
        job_name: AWS Glue job name.
        run_id:   Glue job run ID.
        region:   AWS region (default us-west-2).

    Returns:
        JSON dict of metric name → list of Average datapoints.
    """
    try:
        import boto3
        from datetime import datetime, timezone, timedelta
        cw         = boto3.client("cloudwatch", region_name=region)
        dimensions = [
            {"Name": "JobName",  "Value": job_name},
            {"Name": "JobRunId", "Value": run_id},
            {"Name": "Type",     "Value": "gauge"},
        ]
        metric_names = [
            "glue.ALL.jvm.heap.usage",
            "glue.driver.system.cpuSystemLoad",
            "glue.ALL.system.cpuSystemLoad",
            "glue.driver.workerutilized",
            "glue.driver.aggregate.numCompletedStages",
            "glue.driver.ExecutorAllocationManager.executors.numberAllExecutors",
        ]
        queries = [
            {
                "Id": f"m{i}",
                "MetricStat": {
                    "Metric": {"Namespace": "Glue", "MetricName": name, "Dimensions": dimensions},
                    "Period": 60,
                    "Stat":   "Average",
                },
                "ReturnData": True,
            }
            for i, name in enumerate(metric_names)
        ]
        end   = datetime.now(tz=timezone.utc)
        start = end - timedelta(hours=12)
        resp  = cw.get_metric_data(MetricDataQueries=queries, StartTime=start, EndTime=end)
        raw: Dict[str, Any] = {}
        for i, name in enumerate(metric_names):
            vals = resp["MetricDataResults"][i].get("Values", [])
            if vals:
                raw[name] = vals
        return json.dumps(raw)
    except Exception as exc:
        logger.warning("CloudWatch fetch failed: %s", exc)
        return json.dumps({"error": str(exc)})


@tool
def analyse_glue_metrics(
    raw_metrics_json: str,
    current_workers: int = 10,
    current_worker_type: str = "G.2X",
    executor_memory_gb: float = 4.0,
) -> str:
    """
    Analyse raw Glue CloudWatch metrics and derive Spark config overrides.

    Args:
        raw_metrics_json:    JSON dict returned by fetch_glue_metrics_from_cloudwatch.
        current_workers:     Current number_of_workers setting.
        current_worker_type: Current Glue worker type (G.1X / G.2X / G.4X / G.8X).
        executor_memory_gb:  Current executor memory in GB.

    Returns:
        JSON analysis with heap_pressure, cpu_worker_load, spark_config_overrides,
        worker_recommendation, and human-readable findings.
    """
    try:
        raw     = json.loads(raw_metrics_json)
        if "error" in raw:
            return json.dumps({"error": raw["error"], "findings": ["Metrics unavailable"]})
        metrics = _normalize(raw)
        result  = _analyse(metrics, current_workers, current_worker_type, executor_memory_gb)
        return json.dumps(result)
    except Exception as exc:
        logger.error("Metrics analysis failed: %s", exc)
        return json.dumps({"error": str(exc)})


@tool
def get_spark_configs_from_metrics(
    raw_metrics_json: str,
    current_workers: int = 10,
    current_worker_type: str = "G.2X",
    executor_memory_gb: float = 4.0,
) -> str:
    """
    One-shot: fetch + analyse → return only the Spark config overrides dict.

    Args:
        raw_metrics_json:    JSON dict of raw metric time-series.
        current_workers:     Current worker count.
        current_worker_type: Current worker type.
        executor_memory_gb:  Current executor memory in GB.

    Returns:
        JSON dict of Spark config key → recommended value.
    """
    analysis_json = analyse_glue_metrics.__wrapped__(
        raw_metrics_json, current_workers, current_worker_type, executor_memory_gb
    )
    analysis = json.loads(analysis_json)
    return json.dumps(analysis.get("spark_config_overrides", {}))


def create_glue_metrics_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                               region: str = "us-west-2") -> Agent:
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(model=model, system_prompt=SYSTEM_PROMPT,
                 tools=[fetch_glue_metrics_from_cloudwatch, analyse_glue_metrics,
                        get_spark_configs_from_metrics])
