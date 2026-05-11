"""
Glue Metrics Analyzer
======================
Parses AWS Glue CloudWatch metrics from a job run and translates them into
actionable Spark config overrides and worker-count recommendations.

Supported metrics
-----------------
  glue.ALL.jvm.heap.usage                                    — JVM heap pressure across all nodes
  glue.driver.system.cpuSystemLoad                           — Driver CPU utilization
  glue.ALL.system.cpuSystemLoad  (or glue.<N>.*)             — Worker CPU utilization
  glue.driver.workerutilized                                 — Active workers at each checkpoint
  glue.driver.aggregate.numCompletedStages                   — Stage completion velocity
  glue.driver.ExecutorAllocationManager.executors.numberAllExecutors — Dynamic allocation behavior

Input formats accepted
----------------------
  1. Time series (list of floats):
       {"glue.ALL.jvm.heap.usage": [0.72, 0.78, 0.85, 0.91]}
  2. Pre-aggregated scalar:
       {"glue.ALL.jvm.heap.usage": 0.85}
  3. CloudWatch GetMetricStatistics shape:
       {"glue.ALL.jvm.heap.usage": {"Datapoints": [{"Average": 0.85}, ...]}}

Recommended usage
-----------------
    analyzer  = GlueMetricsAnalyzer()
    metrics   = analyzer.parse_metrics(raw_json)
    analysis  = analyzer.analyze(metrics)
    configs   = analyzer.get_spark_config_overrides(analysis)
    workers   = analyzer.get_worker_recommendation(analysis, current_workers=10)
"""
from __future__ import annotations

import statistics
from typing import Any, Dict, List, Optional, Tuple, Union

# ─── Threshold constants ──────────────────────────────────────────────────────

_HEAP_HIGH       = 0.80   # JVM heap > 80 % → memory pressure
_HEAP_MEDIUM     = 0.65   # 65–80 %         → caution
_HEAP_LOW        = 0.40   # < 40 %          → over-provisioned memory

_CPU_HIGH        = 0.80   # CPU  > 80 %     → CPU bottleneck
_CPU_MEDIUM      = 0.50   # 50–80 %         → healthy
_CPU_LOW         = 0.30   # < 30 %          → CPU under-utilised

_UTIL_LOW_RATIO  = 0.70   # workers_utilised / allocated < 70 % → over-provisioned
_UTIL_HIGH_RATIO = 0.95   # workers_utilised / allocated > 95 % → under-provisioned


class GlueMetricsAnalyzer:
    """Analyzes Glue CloudWatch metrics and produces Spark config recommendations."""

    # ─── Metric name aliases ──────────────────────────────────────────────────

    METRIC_JVM_HEAP       = "glue.ALL.jvm.heap.usage"
    METRIC_CPU_DRIVER     = "glue.driver.system.cpuSystemLoad"
    METRIC_CPU_WORKERS    = "glue.ALL.system.cpuSystemLoad"   # also accepts glue.N.system.cpuSystemLoad
    METRIC_WORKERS_USED   = "glue.driver.workerutilized"
    METRIC_STAGES         = "glue.driver.aggregate.numCompletedStages"
    METRIC_ALL_EXECUTORS  = "glue.driver.ExecutorAllocationManager.executors.numberAllExecutors"

    # ─── Public API ───────────────────────────────────────────────────────────

    def parse_metrics(self, raw: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
        """
        Normalize raw metric input into {metric_name: {avg, max, min, p90, last}}.

        Accepts the three input formats described in the module docstring.
        Also canonicalises `glue.N.system.cpuSystemLoad` → METRIC_CPU_WORKERS.
        """
        parsed: Dict[str, Dict[str, float]] = {}

        for key, value in raw.items():
            canonical = self._canonicalize(key)

            # CloudWatch GetMetricStatistics shape
            if isinstance(value, dict) and "Datapoints" in value:
                series = [dp.get("Average", dp.get("Maximum", 0.0))
                          for dp in value["Datapoints"]]
            elif isinstance(value, list):
                series = [float(v) for v in value if v is not None]
            else:
                series = [float(value)]

            if not series:
                continue

            parsed[canonical] = {
                "avg":  statistics.mean(series),
                "max":  max(series),
                "min":  min(series),
                "p90":  sorted(series)[int(len(series) * 0.90)],
                "last": series[-1],
                "n":    len(series),
            }

        return parsed

    def analyze(self, metrics: Dict[str, Dict[str, float]]) -> Dict[str, Any]:
        """
        Derive findings from parsed metrics.

        Returns a structured analysis dict with:
          heap_pressure     — none | low | medium | high | critical
          cpu_driver_load   — idle | normal | high | overloaded
          cpu_worker_load   — idle | normal | high | overloaded
          worker_utilisation — under | optimal | over
          dynamic_alloc_scaling — shrinking | stable | growing
          stage_velocity    — slow | normal | fast   (relative estimate)
          findings          — list of human-readable insight strings
          raw               — the parsed metrics dict
        """
        findings: List[str] = []

        # ── JVM heap ─────────────────────────────────────────────────────────
        heap = metrics.get(self.METRIC_JVM_HEAP, {})
        heap_p90 = heap.get("p90", 0.0)
        heap_max = heap.get("max", 0.0)

        if heap_max >= 0.95:
            heap_pressure = "critical"
            findings.append(
                f"CRITICAL: JVM heap peaked at {heap_max:.0%} – OOM risk. "
                "Increase executor memory or upgrade worker type immediately."
            )
        elif heap_p90 >= _HEAP_HIGH:
            heap_pressure = "high"
            findings.append(
                f"HIGH heap pressure (p90={heap_p90:.0%}). "
                "Increase executor memory by 50 % or upgrade worker type."
            )
        elif heap_p90 >= _HEAP_MEDIUM:
            heap_pressure = "medium"
            findings.append(
                f"Moderate heap pressure (p90={heap_p90:.0%}). "
                "Monitor; consider enabling off-heap storage (MEMORY_AND_DISK_SER)."
            )
        elif heap_p90 > 0 and heap_p90 < _HEAP_LOW:
            heap_pressure = "low"
            findings.append(
                f"JVM heap only {heap_p90:.0%} utilised – workers are over-provisioned "
                "on memory; consider downgrading worker type to reduce cost."
            )
        else:
            heap_pressure = "none"

        # ── CPU – driver ──────────────────────────────────────────────────────
        cpu_d = metrics.get(self.METRIC_CPU_DRIVER, {})
        cpu_d_avg = cpu_d.get("avg", 0.0)
        if cpu_d_avg >= _CPU_HIGH:
            cpu_driver = "overloaded"
            findings.append(
                f"Driver CPU overloaded ({cpu_d_avg:.0%} avg). "
                "Increase driver memory/cores or reduce collect()/toPandas() calls on driver."
            )
        elif cpu_d_avg >= _CPU_MEDIUM:
            cpu_driver = "high"
        elif cpu_d_avg > 0 and cpu_d_avg < _CPU_LOW:
            cpu_driver = "idle"
            findings.append(
                f"Driver CPU idle ({cpu_d_avg:.0%} avg) – driver is mostly waiting; "
                "parallelism may be under-configured."
            )
        else:
            cpu_driver = "normal"

        # ── CPU – workers ─────────────────────────────────────────────────────
        cpu_w = metrics.get(self.METRIC_CPU_WORKERS, {})
        cpu_w_avg = cpu_w.get("avg", 0.0)
        if cpu_w_avg >= _CPU_HIGH:
            cpu_worker = "overloaded"
            findings.append(
                f"Worker CPU overloaded ({cpu_w_avg:.0%} avg). "
                "Increase worker count or upgrade to a CPU-optimised type."
            )
        elif cpu_w_avg > 0 and cpu_w_avg < _CPU_LOW:
            cpu_worker = "idle"
            findings.append(
                f"Worker CPUs under-utilised ({cpu_w_avg:.0%} avg). "
                "Increase spark.executor.cores or reduce worker count."
            )
        else:
            cpu_worker = "normal"

        # ── Worker utilisation ─────────────────────────────────────────────────
        wu  = metrics.get(self.METRIC_WORKERS_USED, {})
        nae = metrics.get(self.METRIC_ALL_EXECUTORS, {})
        wu_avg  = wu.get("avg", 0.0)
        nae_max = nae.get("max", wu_avg or 1.0)

        if nae_max > 0 and wu_avg > 0:
            ratio = wu_avg / nae_max
            if ratio < _UTIL_LOW_RATIO:
                worker_util = "under"
                findings.append(
                    f"Workers under-utilised: avg {wu_avg:.1f} / {nae_max:.0f} allocated "
                    f"({ratio:.0%}). Reduce number_of_workers by ~{int((1 - ratio) * 100)}%."
                )
            elif ratio >= _UTIL_HIGH_RATIO:
                worker_util = "over"
                findings.append(
                    f"Workers maxed out: {wu_avg:.1f}/{nae_max:.0f} used ({ratio:.0%}). "
                    "Increase number_of_workers by 20–30% to prevent task queuing."
                )
            else:
                worker_util = "optimal"
        else:
            worker_util = "unknown"

        # ── Dynamic allocation trend ───────────────────────────────────────────
        nae_series_first = nae.get("min", 0)
        nae_series_last  = nae.get("last", 0)
        if nae_series_last < nae_series_first * 0.8:
            dyn_scaling = "shrinking"
        elif nae_series_last > nae_series_first * 1.2:
            dyn_scaling = "growing"
        else:
            dyn_scaling = "stable"

        # ── Stage velocity ─────────────────────────────────────────────────────
        stages = metrics.get(self.METRIC_STAGES, {})
        stages_max = stages.get("max", 0)
        # Rough heuristic: < 50 stages for a substantial job suggests slow execution
        if stages_max > 0:
            stage_velocity = "fast" if stages_max > 200 else "normal" if stages_max > 50 else "slow"
            if stage_velocity == "slow":
                findings.append(
                    f"Only {stages_max:.0f} stages completed – job may have wide shuffle "
                    "dependencies or be stuck. Check for data skew."
                )
        else:
            stage_velocity = "unknown"

        if not findings:
            findings.append("All Glue metrics are within healthy ranges – no urgent tuning needed.")

        return {
            "heap_pressure":         heap_pressure,
            "cpu_driver_load":       cpu_driver,
            "cpu_worker_load":       cpu_worker,
            "worker_utilisation":    worker_util,
            "dynamic_alloc_scaling": dyn_scaling,
            "stage_velocity":        stage_velocity,
            "findings":              findings,
            "raw_values": {
                "heap_p90":        round(heap_p90, 3),
                "heap_max":        round(heap_max, 3),
                "cpu_driver_avg":  round(cpu_d_avg, 3),
                "cpu_worker_avg":  round(cpu_w_avg, 3),
                "workers_avg":     round(wu_avg, 2),
                "executors_max":   round(nae_max, 2),
            },
            "raw": metrics,
        }

    def get_spark_config_overrides(
        self,
        analysis: Dict[str, Any],
        current_executor_memory_gb: float = 4.0,
    ) -> Dict[str, str]:
        """
        Return Spark config key→value overrides derived from the metric analysis.
        These are *merged on top of* the base AQE defaults in RecommendationApplierAgent.
        """
        overrides: Dict[str, str] = {}

        heap     = analysis.get("heap_pressure",      "none")
        cpu_w    = analysis.get("cpu_worker_load",    "normal")
        w_util   = analysis.get("worker_utilisation", "unknown")

        # ── Memory tuning ─────────────────────────────────────────────────────
        if heap in ("high", "critical"):
            new_mem_gb = max(8, int(current_executor_memory_gb * 1.5))
            overrides["spark.executor.memory"]          = f"{new_mem_gb}g"
            overrides["spark.executor.memoryOverhead"]  = f"{max(2, new_mem_gb // 4)}g"
            overrides["spark.memory.fraction"]          = "0.80"
            overrides["spark.memory.storageFraction"]   = "0.25"
            overrides["spark.rdd.compress"]             = "true"
            overrides["spark.shuffle.compress"]         = "true"
            overrides["spark.shuffle.spill.compress"]   = "true"
        elif heap == "medium":
            overrides["spark.memory.fraction"]          = "0.75"
            overrides["spark.memory.storageFraction"]   = "0.30"
            overrides["spark.rdd.compress"]             = "true"
        elif heap == "low":
            # Over-provisioned on memory – tighten to save cost
            safe_mem_gb = max(4, int(current_executor_memory_gb * 0.75))
            overrides["spark.executor.memory"]          = f"{safe_mem_gb}g"
            overrides["spark.memory.fraction"]          = "0.60"

        # ── CPU / parallelism tuning ──────────────────────────────────────────
        if cpu_w == "overloaded":
            # More parallelism headroom: reduce per-executor cores so tasks get more GC time
            overrides["spark.executor.cores"]           = "2"
            overrides["spark.task.cpus"]                = "1"
        elif cpu_w == "idle":
            # Use more cores per executor to pack more tasks
            overrides["spark.executor.cores"]           = "4"

        # ── Dynamic allocation ────────────────────────────────────────────────
        if w_util in ("under", "over"):
            overrides["spark.dynamicAllocation.enabled"]             = "true"
            overrides["spark.dynamicAllocation.shuffleTracking.enabled"] = "true"
            if w_util == "under":
                raw   = analysis.get("raw_values", {})
                ideal = max(2, int(raw.get("workers_avg", 4)))
                overrides["spark.dynamicAllocation.maxExecutors"]    = str(ideal)
                overrides["spark.dynamicAllocation.minExecutors"]    = str(max(1, ideal // 2))
            else:  # over
                raw   = analysis.get("raw_values", {})
                ideal = int(raw.get("executors_max", 10) * 1.3)
                overrides["spark.dynamicAllocation.maxExecutors"]    = str(ideal)

        return overrides

    def get_worker_recommendation(
        self,
        analysis: Dict[str, Any],
        current_workers: int,
        current_worker_type: str = "G.2X",
    ) -> Dict[str, Any]:
        """
        Return recommended worker count and optionally a different worker type.
        """
        heap       = analysis.get("heap_pressure",      "none")
        cpu_w      = analysis.get("cpu_worker_load",    "normal")
        w_util     = analysis.get("worker_utilisation", "unknown")
        raw        = analysis.get("raw_values",         {})

        reason: List[str] = []
        new_workers = current_workers
        new_type    = current_worker_type

        # Worker count adjustments
        if w_util == "under":
            workers_avg = raw.get("workers_avg", current_workers)
            new_workers = max(2, int(workers_avg * 1.10))   # 10% headroom above avg utilised
            reason.append(f"Workers under-utilised (avg {workers_avg:.1f}/{current_workers})")
        elif w_util == "over":
            new_workers = max(current_workers + 2, int(current_workers * 1.25))
            reason.append("Workers consistently maxed out – increase to reduce queuing")

        # CPU overload → more workers too
        if cpu_w == "overloaded" and new_workers == current_workers:
            new_workers = max(current_workers + 2, int(current_workers * 1.20))
            reason.append("Worker CPUs overloaded – add workers to spread the load")

        # Worker type upgrade for memory pressure
        TYPE_UPGRADE = {"G.1X": "G.2X", "G.2X": "G.4X", "G.4X": "G.8X"}
        TYPE_DOWNGRADE = {"G.8X": "G.4X", "G.4X": "G.2X", "G.2X": "G.1X"}

        if heap in ("high", "critical"):
            upgraded = TYPE_UPGRADE.get(current_worker_type)
            if upgraded:
                new_type = upgraded
                reason.append(f"High JVM heap → upgrade worker type {current_worker_type}→{upgraded}")
        elif heap == "low" and cpu_w == "idle" and w_util == "under":
            downgraded = TYPE_DOWNGRADE.get(current_worker_type)
            if downgraded:
                new_type = downgraded
                reason.append(
                    f"Memory & CPU under-utilised → downgrade {current_worker_type}→{downgraded} to save cost"
                )

        return {
            "current_workers":      current_workers,
            "recommended_workers":  new_workers,
            "current_worker_type":  current_worker_type,
            "recommended_type":     new_type,
            "changed":              (new_workers != current_workers or new_type != current_worker_type),
            "reason":               reason,
        }

    def fetch_from_cloudwatch(
        self,
        job_name: str,
        run_id: str,
        region: str = "us-west-2",
        period_seconds: int = 60,
        stat: str = "Average",
    ) -> Dict[str, Any]:
        """
        Fetch all supported Glue metrics for a job run from CloudWatch.

        Requires boto3 + CloudWatch read permissions (cloudwatch:GetMetricData).
        Returns raw dict in time-series format suitable for parse_metrics().
        """
        try:
            import boto3
            from datetime import datetime, timezone, timedelta
            from botocore.exceptions import ClientError, NoCredentialsError
        except ImportError:
            return {"error": "boto3 not installed"}

        metric_names = [
            self.METRIC_JVM_HEAP,
            self.METRIC_CPU_DRIVER,
            self.METRIC_CPU_WORKERS,
            self.METRIC_WORKERS_USED,
            self.METRIC_STAGES,
            self.METRIC_ALL_EXECUTORS,
        ]

        cw = boto3.client("cloudwatch", region_name=region)
        dimensions = [
            {"Name": "JobName",   "Value": job_name},
            {"Name": "JobRunId",  "Value": run_id},
            {"Name": "Type",      "Value": "gauge"},
        ]

        # Build metric data queries
        queries = [
            {
                "Id":         f"m{i}",
                "MetricStat": {
                    "Metric": {
                        "Namespace":  "Glue",
                        "MetricName": name,
                        "Dimensions": dimensions,
                    },
                    "Period": period_seconds,
                    "Stat":   stat,
                },
                "ReturnData": True,
            }
            for i, name in enumerate(metric_names)
        ]

        end   = datetime.now(tz=timezone.utc)
        start = end - timedelta(hours=12)   # last 12 h; adjust if job is longer

        try:
            resp   = cw.get_metric_data(
                MetricDataQueries = queries,
                StartTime         = start,
                EndTime           = end,
            )
            raw: Dict[str, Any] = {}
            for i, name in enumerate(metric_names):
                values = resp["MetricDataResults"][i].get("Values", [])
                if values:
                    raw[name] = values
            return raw
        except (ClientError, NoCredentialsError) as exc:
            return {"error": str(exc)}

    # ─── Private ──────────────────────────────────────────────────────────────

    def _canonicalize(self, key: str) -> str:
        """Map glue.<N>.system.cpuSystemLoad → METRIC_CPU_WORKERS."""
        import re
        if re.match(r'^glue\.\d+\.system\.cpuSystemLoad$', key):
            return self.METRIC_CPU_WORKERS
        return key
