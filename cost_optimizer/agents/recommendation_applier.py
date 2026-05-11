"""
Recommendation Applier Agent
==============================
Applies optimization recommendations to existing PySpark scripts.

Rule-based mode
  - Injects recommended Spark configs (AQE, KryoSerializer, shuffle tuning)
    into the SparkSession builder chain
  - Merges Glue CloudWatch metric-derived config overrides when glue_metrics
    are supplied:
      * Heap pressure → executor memory, memory.fraction, G1GC JVM options
      * CPU idle       → reduce executor.cores (scale down)
      * CPU saturated  → add workers, dynamic allocation
      * Worker skew    → skewJoin thresholds, salting comment
      * Shuffle heavy  → shuffle service, reducer buffers
      * S3 reads       → locality.wait=0, ETL memory ratio
  - Replaces .repartition(1) with .coalesce(1)
  - Adds .coalesce() hint comment before write operations lacking one
  - Adds broadcast() hints for small / flagged tables inside .join() calls
  - Annotates .collect() calls on potentially large DataFrames
  - Annotates UDF definitions with built-in function suggestions
  - Inserts .cache() before DataFrames used by multiple actions

LLM mode
  - Sends full script + analysis results + metric findings to Claude; receives
    a complete rewritten script with every recommendation applied and a changelog.
  - Robust application of ALL LLM output types:
      * code_refactoring   – original_code / improved_code (fuzzy-matched)
      * anti_patterns      – fix_code applied at detected locations
      * spark_configs      – injected via SparkSession config block
      * optimizations      – narrative → TODO comment injected near relevant code
"""
from __future__ import annotations

import difflib
import json
import re
import textwrap
from typing import Any, Dict, List, Optional, Tuple

from .base import CostOptimizerAgent, AnalysisInput, AnalysisResult


# ─── Base Spark configuration set ─────────────────────────────────────────────

_SPARK_CONFIGS: Dict[str, str] = {
    "spark.sql.adaptive.enabled":                       "true",
    "spark.sql.adaptive.coalescePartitions.enabled":    "true",
    "spark.sql.adaptive.skewJoin.enabled":              "true",
    "spark.sql.adaptive.localShuffleReader.enabled":    "true",
    "spark.sql.autoBroadcastJoinThreshold":             "10485760",   # 10 MB
    "spark.sql.broadcastTimeout":                       "300",
    "spark.sql.shuffle.partitions":                     "auto",
    "spark.sql.files.maxPartitionBytes":                "134217728",  # 128 MB
    "spark.sql.adaptive.advisoryPartitionSizeInBytes":  "134217728",
    "spark.serializer":     "org.apache.spark.serializer.KryoSerializer",
    "spark.kryoserializer.buffer.max":                  "512m",
}

# GC tuning injected when heap pressure > 0.80
_GC_CONFIGS: Dict[str, str] = {
    "spark.executor.extraJavaOptions": (
        "-XX:+UseG1GC -XX:G1HeapRegionSize=32M "
        "-XX:InitiatingHeapOccupancyPercent=35 "
        "-XX:+G1SummarizeConcMark -XX:+PrintGCDetails"
    ),
    "spark.driver.extraJavaOptions": (
        "-XX:+UseG1GC -XX:G1HeapRegionSize=32M "
        "-XX:InitiatingHeapOccupancyPercent=35"
    ),
}

# Shuffle optimization: injected when CPU driver load is high or shuffle I/O detected
_SHUFFLE_CONFIGS: Dict[str, str] = {
    "spark.shuffle.service.enabled":                    "true",
    "spark.reducer.maxSizeInFlight":                    "96m",
    "spark.shuffle.compress":                           "true",
    "spark.shuffle.spill.compress":                     "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "268435456",  # 256 MB
}

# Skew-specific AQE overrides (stricter thresholds)
_SKEW_CONFIGS: Dict[str, str] = {
    "spark.sql.adaptive.skewJoin.enabled":              "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "3",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "67108864",   # 64 MB
    "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
}

# Data locality: for S3-backed datasets, don't wait for local data
_LOCALITY_CONFIGS: Dict[str, str] = {
    "spark.locality.wait":        "0",
    "spark.locality.wait.node":   "0",
    "spark.locality.wait.rack":   "0",
    "spark.locality.wait.process":"0",
}

# ETL memory hierarchy: favour execution memory over storage when doing heavy ETL
_ETL_MEMORY_CONFIGS: Dict[str, str] = {
    "spark.memory.fraction":        "0.8",
    "spark.memory.storageFraction": "0.2",
}

# Dynamic allocation (paired with shuffle service)
_DYN_ALLOC_CONFIGS: Dict[str, str] = {
    "spark.dynamicAllocation.enabled":              "true",
    "spark.dynamicAllocation.initialExecutors":     "2",
    "spark.dynamicAllocation.minExecutors":         "1",
    "spark.dynamicAllocation.maxExecutors":         "20",
    "spark.dynamicAllocation.executorIdleTimeout":  "60s",
    "spark.dynamicAllocation.schedulerBacklogTimeout": "5s",
}

# Broadcast threshold for small tables
_BROADCAST_ROW_THRESHOLD = 500_000


class RecommendationApplierAgent(CostOptimizerAgent):
    """Apply analysis recommendations to a PySpark script."""

    AGENT_NAME = "recommendation_applier"

    # ─── Public entry point ────────────────────────────────────────────────────

    def apply(
        self,
        script_path: str,
        script_content: str,
        analysis_result: Dict[str, Any],
        source_tables: Optional[List[Dict]] = None,
        glue_metrics: Optional[Dict[str, Any]] = None,
        current_workers: int = 10,
        current_worker_type: str = "G.2X",
        current_executor_memory_gb: float = 4.0,
        size_analyzer_full: Optional[Dict[str, Any]] = None,
        code_analyzer_full: Optional[Dict[str, Any]] = None,
        all_recommendations: Optional[List[Dict]] = None,
    ) -> Dict[str, Any]:
        """
        Apply recommendations to *script_content*.

        Returns
        -------
        modified_script       – transformed source code (str)
        original_script       – unchanged original (str)
        changelog             – list of fix dicts
        fixes_applied         – total number of fixes (int)
        worker_recommendation – dict from GlueMetricsAnalyzer (may be None)
        metric_findings       – human-readable metric insights (list[str])
        success               – bool
        errors                – list[str]
        """
        input_data = AnalysisInput(
            script_path     = script_path,
            script_content  = script_content,
            source_tables   = source_tables or [],
            processing_mode = "full",
            current_config  = {
                "number_of_workers":    current_workers,
                "worker_type":          current_worker_type,
                "executor_memory_gb":   current_executor_memory_gb,
            },
        )
        result = self.analyze(
            input_data,
            context={
                "analysis_result":            analysis_result,
                "glue_metrics":               glue_metrics or {},
                "current_workers":            current_workers,
                "current_worker_type":        current_worker_type,
                "current_executor_memory_gb": current_executor_memory_gb,
                "size_analyzer_full":         size_analyzer_full or {},
                "code_analyzer_full":         code_analyzer_full or {},
                "all_recommendations":        all_recommendations or [],
            },
        )
        return {
            "success":               result.success,
            "modified_script":       result.analysis.get("modified_script", script_content),
            "original_script":       script_content,
            "changelog":             result.recommendations,
            "fixes_applied":         result.analysis.get("fixes_count", 0),
            "worker_recommendation": result.analysis.get("worker_recommendation"),
            "metric_findings":       result.analysis.get("metric_findings", []),
            "errors":                result.errors,
        }

    # ─── Rule-based core ──────────────────────────────────────────────────────

    def _analyze_rule_based(
        self, input_data: AnalysisInput, context: Dict
    ) -> AnalysisResult:
        code      = input_data.script_content
        tables    = input_data.source_tables
        analysis  = context.get("analysis_result", {})
        changelog: List[Dict] = []

        # ── Step 1: Glue metrics → config overrides + worker rec ──────────────
        metric_overrides: Dict[str, str] = {}
        worker_rec: Optional[Dict]       = None
        metric_findings: List[str]       = []
        metric_analysis: Dict            = {}
        raw_metrics = context.get("glue_metrics", {})

        if raw_metrics:
            try:
                from .glue_metrics_analyzer import GlueMetricsAnalyzer
                gma             = GlueMetricsAnalyzer()
                parsed          = gma.parse_metrics(raw_metrics)
                metric_analysis = gma.analyze(parsed)
                metric_findings = metric_analysis.get("findings", [])

                cur_mem_gb      = float(context.get("current_executor_memory_gb", 4.0))
                metric_overrides = gma.get_spark_config_overrides(
                    metric_analysis, current_executor_memory_gb=cur_mem_gb
                )
                worker_rec = gma.get_worker_recommendation(
                    metric_analysis,
                    current_workers     = int(context.get("current_workers", 10)),
                    current_worker_type = context.get("current_worker_type", "G.2X"),
                )
                if metric_overrides:
                    changelog.append({
                        "fix":         "glue_metric_configs",
                        "description": (
                            f"Derived {len(metric_overrides)} Spark config(s) from Glue "
                            "CloudWatch metrics (heap, CPU, worker utilisation)"
                        ),
                        "configs":  metric_overrides,
                        "findings": metric_findings,
                    })
                if worker_rec and worker_rec.get("changed"):
                    changelog.append({
                        "fix":         "worker_recommendation",
                        "description": "; ".join(worker_rec.get("reason", [])),
                        "current": {
                            "workers":     worker_rec["current_workers"],
                            "worker_type": worker_rec["current_worker_type"],
                        },
                        "recommended": {
                            "workers":     worker_rec["recommended_workers"],
                            "worker_type": worker_rec["recommended_type"],
                        },
                    })
            except Exception as exc:
                changelog.append({
                    "fix":         "glue_metrics_error",
                    "description": f"Metrics analysis skipped: {exc}",
                })

        # ── Step 2: Inject base + metric-derived Spark configs ─────────────────
        code = self._inject_spark_configs(code, changelog, metric_overrides)

        # ── Step 3: Metric-driven advanced tuning ─────────────────────────────
        code = self._inject_gc_tuning(code, changelog, metric_analysis)
        code = self._inject_skew_handling(code, changelog, metric_analysis, analysis)
        code = self._inject_shuffle_optimization(code, changelog, metric_analysis)
        code = self._inject_data_locality(code, changelog, metric_analysis)
        code = self._inject_etl_memory_hierarchy(code, changelog, metric_analysis)

        # ── Step 4: Pattern-based fixes ───────────────────────────────────────
        code = self._fix_repartition_1(code, changelog)
        code = self._add_coalesce_before_write(code, changelog)
        code = self._add_broadcast_hints(code, tables, changelog)
        code = self._annotate_collect_calls(code, changelog)
        code = self._annotate_udf_usage(code, changelog)
        code = self._add_cache_before_multi_action(code, changelog)

        # ── Step 5: Robust LLM analysis refactoring ───────────────────────────
        code = self._apply_analysis_refactoring(code, analysis, changelog)

        return AnalysisResult(
            agent_name      = self.AGENT_NAME,
            success         = True,
            analysis        = {
                "modified_script":       code,
                "original_script":       input_data.script_content,
                "changelog":             changelog,
                "fixes_count":           len(changelog),
                "worker_recommendation": worker_rec,
                "metric_findings":       metric_findings,
            },
            recommendations = changelog,
        )

    # ─── LLM prompt override ──────────────────────────────────────────────────

    def _build_llm_prompt(self, input_data: AnalysisInput, context: Dict) -> str:
        analysis       = context.get("analysis_result",    {})
        raw_metrics    = context.get("glue_metrics",       {})
        size_full      = context.get("size_analyzer_full", {})
        code_full      = context.get("code_analyzer_full", {})
        all_recs       = context.get("all_recommendations", [])
        cur_workers    = context.get("current_workers",    10)
        cur_type       = context.get("current_worker_type", "G.2X")
        cur_mem_gb     = context.get("current_executor_memory_gb", 4.0)

        # ── Per-table Iceberg section ─────────────────────────────────────────
        table_findings = size_full.get("table_findings", {})
        iceberg_lines: List[str] = []
        iceberg_action_lines: List[str] = []
        for tbl, findings in table_findings.items():
            fs   = findings.get("file_stats",              {})
            ih   = findings.get("iceberg_health",          {})
            cold = findings.get("cold_partition_analysis", {})
            sc   = findings.get("storage_cost",            {})

            tiny     = fs.get("tiny_file_count",   0)
            avg_mb   = fs.get("avg_file_mb",       0)
            snaps    = ih.get("snapshot_count",    0)
            write_amp = ih.get("write_amplification", 1.0)
            cold_gb  = cold.get("cold_data_estimate_gb", 0)
            waste_pct = sc.get("waste_pct",         0)

            iceberg_lines.append(
                f"  {tbl}: tiny_files={tiny:,} avg={avg_mb:.1f}MB "
                f"snapshots={snaps} write_amp={write_amp:.1f}x "
                f"cold={cold_gb:.0f}GB waste={waste_pct:.0f}%"
            )
            # Derive code-level actions from Iceberg health
            if tiny > 10_000:
                iceberg_action_lines.append(
                    f"  - Before reading {tbl}: add comment "
                    f"'# PRE-CONDITION: run OPTIMIZE {tbl} REWRITE DATA USING bin-pack in Athena "
                    f"({tiny:,} tiny files, avg {avg_mb:.1f} MB — expect {tiny:,} tasks without OPTIMIZE)'"
                )
            if snaps > 30:
                iceberg_action_lines.append(
                    f"  - Add comment near {tbl} read: "
                    f"'# PRE-CONDITION: run CALL system.expire_snapshots(\\'{tbl}\\', "
                    f"TIMESTAMP \\'{{NOW - 7d}}\\') — {snaps} snapshots cause {write_amp:.1f}x I/O overhead'"
                )
            if cold_gb > 50:
                iceberg_action_lines.append(
                    f"  - Add date pushdown filter on {tbl} read: "
                    f"filter out partitions older than 90 days "
                    f"({cold_gb:.0f} GB cold data scanned unnecessarily)"
                )
            if write_amp > 5:
                iceberg_action_lines.append(
                    f"  - Remove any coalesce(1)/repartition(1) before writing to {tbl}: "
                    f"write_amplification={write_amp:.1f}x means Iceberg creates 1 manifest "
                    f"per write-file; bin-pack OPTIMIZE should handle compaction instead"
                )

        iceberg_ctx = "\n".join(iceberg_lines)   or "  (no Iceberg telemetry)"
        action_ctx  = "\n".join(iceberg_action_lines) or "  (none)"

        # ── Code line hotspots ────────────────────────────────────────────────
        hotspots = code_full.get("line_hotspots", [])
        top_line = code_full.get("top_expensive_line", {})
        iceberg_specific = code_full.get("iceberg_specific", [])

        hotspot_ctx = json.dumps(hotspots[:10], indent=2) if hotspots else "  (none)"
        top_ctx     = json.dumps(top_line, indent=2) if top_line else "  (none)"
        ice_code_ctx = json.dumps(iceberg_specific, indent=2) if iceberg_specific else "  (none)"

        # ── P0/P1 recommendations (highest priority only for the applier) ─────
        p01_recs = [r for r in all_recs if r.get("priority") in ("P0", "P1")]
        recs_ctx = json.dumps(p01_recs[:15], indent=2) if p01_recs else "  (none)"

        # ── Glue metrics section ──────────────────────────────────────────────
        metric_section = ""
        if raw_metrics:
            metric_rows = []
            for m, vals in raw_metrics.items():
                if vals:
                    metric_rows.append(
                        f"  {m}: min={min(vals):.2f} max={max(vals):.2f} "
                        f"last={vals[-1]:.2f} "
                        f"trend={'↑' if vals[-1] > vals[0] else '↓'}"
                    )
            metric_section = f"""
══════════════════════════════════════════════════════
GLUE RUNTIME METRICS  ({cur_workers} × {cur_type}  {cur_mem_gb} GB executor memory)
══════════════════════════════════════════════════════
{chr(10).join(metric_rows)}

Metric-to-code mapping rules (apply ALL that match):
  heap > 0.80  → increase executor.memory; inject G1GC JVM options; add cache.unpersist()
  heap < 0.50  → reduce executor.memory; add comment "# executor memory can be halved"
  cpu_workers < 0.30 mid-job → executor CPU idle = SKEW; add .repartition() before heaviest join
  cpu_workers drops after peak → late-stage skew; add AQE skewJoin configs + salting comment
  workerutilized peak < 70%  → add comment "# Job config: reduce workers to {{peak_utilized}}"
  workerutilized > 95%       → add comment "# Job config: increase workers — all executors busy"
  shuffle heavy              → inject shuffle service + reducer buffers
"""

        return f"""You are a Senior PySpark Engineer performing a COMPLETE, HOLISTIC rewrite of the script below.
Your job is to produce a single, runnable optimized Python file with ALL issues fixed.
You can see findings from EVERY analysis layer — fix ALL of them.

══════════════════════════════════════════════════════
ORIGINAL SCRIPT  ({input_data.script_path})
══════════════════════════════════════════════════════
```python
{input_data.script_content}
```

══════════════════════════════════════════════════════
ICEBERG TABLE HEALTH  (from SizeAnalyzerAgent — actual $files data)
══════════════════════════════════════════════════════
Per-table summary:
{iceberg_ctx}

Required Iceberg-driven code changes:
{action_ctx}
{metric_section}
══════════════════════════════════════════════════════
CODE HOTSPOTS  (from CodeAnalyzerAgent — line-level analysis)
══════════════════════════════════════════════════════
Most expensive line:
{top_ctx}

Line hotspots (fix these first):
{hotspot_ctx}

Iceberg-specific code patterns detected:
{ice_code_ctx}

══════════════════════════════════════════════════════
P0/P1 RECOMMENDATIONS  (must fix — highest priority)
══════════════════════════════════════════════════════
{recs_ctx}

══════════════════════════════════════════════════════
FULL ANALYSIS (fallback / additional context)
══════════════════════════════════════════════════════
```json
{json.dumps(analysis, indent=2, default=str)[:3000]}
```

══════════════════════════════════════════════════════
REQUIRED CHANGES — apply ALL in this order
══════════════════════════════════════════════════════

LAYER 1 — Spark configuration (inject into SparkSession builder)
  1. AQE enabled + coalescePartitions + skewJoin + localShuffleReader
  2. KryoSerializer + kryoserializer.buffer.max=512m
  3. shuffle.partitions calibrated to data size (see Iceberg table GB above)
  4. autoBroadcastJoinThreshold tuned to 10% of executor memory
  5. locality.wait=0 for S3-backed jobs
  If Glue metrics supplied: tune executor.memory, memoryOverhead, memory.fraction,
  dynamicAllocation, executor.cores, G1GC JVM options based on metric rules above.

LAYER 2 — Iceberg table pre-conditions (add comments/code BEFORE each table read)
  6. If tiny_file_count > 10k: add # PRE-CONDITION: OPTIMIZE comment above the read
  7. If snapshot_count > 30: add # PRE-CONDITION: expire_snapshots comment above the read
  8. If cold_gb > 50 and no date filter: add .filter("partition_col >= date_sub(current_date(),90)")
  9. If write_amplification > 5: remove coalesce(1)/repartition(1) before Iceberg writes

LAYER 3 — Code pattern fixes (at specific lines from hotspot analysis)
  10. Fix each line_hotspot: apply the "fix" field exactly at the referenced line
  11. Apply iceberg_specific[] actions (OPTIMIZE comment, expire_snapshots, incremental reads)
  12. Replace .repartition(1) → .coalesce(1)
  13. Add broadcast() for small tables (< 500k rows or flagged in source_tables)
  14. Add .cache() before DataFrames used by ≥ 2 actions; add .unpersist() after last use
  15. Add .coalesce(N) before write operations — N = ceil(total_output_gb * 4)
  16. Replace UDFs with pyspark.sql.functions equivalents; add SUGGESTION comment if not possible
  17. Add WARNING comment above every .collect() / .toPandas() call

LAYER 4 — Skew mitigation
  18. If skew_ratio > 5 on join key: add key-salting comment + tighter AQE skewJoin thresholds
  19. If workerutilized drops mid-job: add .repartition(n, skewed_col) before the join

LAYER 5 — Long-running job specific
  20. If same DataFrame reused in a loop: add .cache() before loop, .unpersist() after
  21. If window function on high-skew column: add comment about OOM risk + recommend AQE
  22. Add worker count comment based on workerutilized metric:
      "# Glue job config: set --number-of-workers to <recommended>"

Return ONLY a JSON object — no markdown, no explanation outside JSON:
{{
  "modified_script": "<COMPLETE Python source — every line — as a single string. Must be runnable.>",
  "changelog": [
    {{
      "line": <int or null>,
      "fix": "<type: spark_config|iceberg_precheck|hotspot_fix|broadcast|cache|coalesce|skew|loop_cache|udf|worker_sizing>",
      "description": "<exactly what changed and WHY — reference the metric or telemetry that triggered it>"
    }}
  ],
  "worker_recommendation": {{
    "recommended_workers": <int>,
    "recommended_type": "<G.1X|G.2X|G.4X|G.8X>",
    "monthly_saving_usd": <float>,
    "reason": "<one sentence citing the specific metric>"
  }},
  "iceberg_precheck_commands": [
    "<Athena SQL: OPTIMIZE / VACUUM / expire_snapshots to run before this Glue job>"
  ]
}}
"""

    # ─── Metric-driven advanced injectors ─────────────────────────────────────

    def _inject_gc_tuning(
        self, code: str, log: List[Dict], metric_analysis: Dict
    ) -> str:
        """Inject G1GC JVM options when heap pressure is elevated (medium/high/critical)."""
        # GlueMetricsAnalyzer.analyze() returns string categories
        heap = metric_analysis.get("heap_pressure", "none")
        if heap not in ("medium", "high", "critical"):
            return code

        # Check if already set
        if "UseG1GC" in code:
            return code

        gc_overrides = {}
        # Merge without overwriting existing extraJavaOptions if already present
        existing_exec_opts = re.search(
            r'spark\.executor\.extraJavaOptions["\s:,]+([^\n"]+)', code
        )
        if existing_exec_opts:
            existing_val = existing_exec_opts.group(1).strip().strip('"')
            if "UseG1GC" not in existing_val:
                gc_overrides["spark.executor.extraJavaOptions"] = (
                    existing_val + " " + _GC_CONFIGS["spark.executor.extraJavaOptions"]
                ).strip()
        else:
            gc_overrides.update(_GC_CONFIGS)

        code = self._inject_spark_configs(code, [], gc_overrides)

        log.append({
            "fix":         "gc_tuning_injected",
            "description": (
                f"Injected G1GC JVM options (heap pressure={heap}): "
                "G1HeapRegionSize=32M, InitiatingHeapOccupancyPercent=35"
            ),
        })
        return code

    def _inject_skew_handling(
        self,
        code: str,
        log: List[Dict],
        metric_analysis: Dict,
        analysis_result: Dict,
    ) -> str:
        """
        Inject AQE skewJoin tuning and optionally a key-salting comment near joins.

        Triggers when:
        - metric_analysis.dynamic_alloc_scaling shows executor thrashing, OR
        - analysis_result contains skew-related findings, OR
        - code has join operations and the analysis mentions skew
        """
        # Check if skew handling already present
        if "skewedPartitionFactor" in code and 'salting' in code.lower():
            return code

        # Determine whether skew is suspected
        skew_suspected = False
        for key in ("code_analysis", "analysis", "recommendations"):
            block = analysis_result.get(key, {})
            text  = str(block).lower()
            if "skew" in text or "salt" in text or "hotspot" in text:
                skew_suspected = True
                break
        # Also check metric findings (GlueMetricsAnalyzer returns string findings)
        for finding in metric_analysis.get("findings", []):
            if "skew" in str(finding).lower() or "thrashing" in str(finding).lower():
                skew_suspected = True
                break

        # Inject AQE skewJoin overrides
        skew_configs = dict(_SKEW_CONFIGS)
        code = self._inject_spark_configs(code, [], skew_configs)

        # Insert a salting-hint comment near the first .join( call
        if skew_suspected and ".join(" in code:
            lines  = code.splitlines()
            result = []
            inserted = False
            for i, line in enumerate(lines):
                if not inserted and ".join(" in line:
                    indent = re.match(r'^(\s*)', line).group(1)
                    result += [
                        f"{indent}# ─── Skew-handling note ──────────────────────────────────────",
                        f"{indent}# Data skew suspected on this join key. If AQE skewJoin does  ",
                        f"{indent}# not fully resolve it, apply key salting:                    ",
                        f"{indent}#   from pyspark.sql import functions as F                    ",
                        f"{indent}#   SALT_FACTOR = 10                                          ",
                        f"{indent}#   large_df = large_df.withColumn(                           ",
                        f'{indent}#       "salt", (F.rand() * SALT_FACTOR).cast("int"))         ',
                        f"{indent}#   small_df = small_df.withColumn(                           ",
                        f'{indent}#       "salt", F.explode(F.array([F.lit(i) for i in range(SALT_FACTOR)]))',
                        f"{indent}#   ).join(large_df, on=[join_key, \"salt\"], how=\"left\")   ",
                        f"{indent}# ─────────────────────────────────────────────────────────────",
                    ]
                    inserted = True
                result.append(line)
            code = "\n".join(result)

        log.append({
            "fix":         "skew_handling_injected",
            "description": (
                "Tightened AQE skewJoin thresholds (factor=3, threshold=64 MB)"
                + ("; added key-salting comment near join" if skew_suspected else "")
            ),
        })
        return code

    def _inject_shuffle_optimization(
        self, code: str, log: List[Dict], metric_analysis: Dict
    ) -> str:
        """
        Inject shuffle service + dynamic allocation configs when CPU driver load
        is high or when shuffle I/O is heavy.
        GlueMetricsAnalyzer returns string categories: idle|normal|high|overloaded.
        """
        cpu_driver  = metric_analysis.get("cpu_driver_load", "normal")
        cpu_worker  = metric_analysis.get("cpu_worker_load", "normal")
        dyn_scaling = metric_analysis.get("dynamic_alloc_scaling", "stable")

        # Always inject as best practice if not already present
        if "shuffle.service.enabled" in code:
            return code

        shuffle_cfgs = dict(_SHUFFLE_CONFIGS)
        if cpu_driver == "idle":
            # CPU idle → enable dynamic allocation to free idle executors
            shuffle_cfgs.update(_DYN_ALLOC_CONFIGS)
            log.append({
                "fix":         "dynamic_allocation_injected",
                "description": (
                    f"CPU driver load is idle – enabled dynamic allocation "
                    "to release idle executors and reduce cost"
                ),
            })
        code = self._inject_spark_configs(code, [], shuffle_cfgs)
        log.append({
            "fix":         "shuffle_optimization_injected",
            "description": (
                "Injected shuffle service + reducer buffer configs "
                "(shuffle.compress, reducer.maxSizeInFlight=96m)"
            ),
        })
        return code

    def _inject_data_locality(
        self, code: str, log: List[Dict], metric_analysis: Dict
    ) -> str:
        """
        Set locality.wait=0 for S3-backed jobs.  S3 data is always remote so
        Spark should never wait for local placement – it wastes scheduling time.
        """
        # Only inject if the script reads from S3 or uses Glue catalog
        has_s3 = bool(re.search(r's3[na]?://', code) or "from_catalog" in code
                      or "GlueContext" in code or "glueContext" in code)
        if not has_s3 or "locality.wait" in code:
            return code

        code = self._inject_spark_configs(code, [], _LOCALITY_CONFIGS)
        log.append({
            "fix":         "data_locality_injected",
            "description": (
                "Set spark.locality.wait=0: S3/Glue data is always remote; "
                "waiting for local placement wastes task scheduling time"
            ),
        })
        return code

    def _inject_etl_memory_hierarchy(
        self, code: str, log: List[Dict], metric_analysis: Dict
    ) -> str:
        """
        Tune memory.fraction / storageFraction for heavy ETL (shuffle-intensive
        jobs with limited caching).  Only applied when heap pressure is high.
        GlueMetricsAnalyzer returns string categories: none|low|medium|high|critical.
        """
        heap = metric_analysis.get("heap_pressure", "none")
        if heap not in ("high", "critical") or "memory.fraction" in code:
            return code

        # Heavy ETL: favour execution over storage
        etl_cfgs: Dict[str, str] = dict(_ETL_MEMORY_CONFIGS)

        # Add memory overhead for off-heap + OS
        cur_mem_gb = 4.0  # default; metric_analysis doesn't carry this directly
        overhead_gb = max(1.0, round(cur_mem_gb * 0.15, 1))
        etl_cfgs["spark.executor.memoryOverhead"] = f"{overhead_gb}g"

        code = self._inject_spark_configs(code, [], etl_cfgs)
        log.append({
            "fix":         "etl_memory_hierarchy_injected",
            "description": (
                f"Tuned memory hierarchy (heap_pressure={heap}): "
                f"memory.fraction=0.8, storageFraction=0.2, "
                f"memoryOverhead={overhead_gb}g"
            ),
        })
        return code

    # ─── Base pattern-based fix helpers ───────────────────────────────────────

    def _inject_spark_configs(
        self,
        code: str,
        log: List[Dict],
        extra_configs: Optional[Dict[str, str]] = None,
        use_base: bool = True,
    ) -> str:
        """
        Insert Spark configs after the SparkSession builder chain.
        When *use_base* is True, base _SPARK_CONFIGS are included.
        *extra_configs* takes priority over both base and existing values.
        """
        effective_configs: Dict[str, str] = {}
        if use_base:
            effective_configs.update(_SPARK_CONFIGS)
        effective_configs.update(extra_configs or {})
        if not effective_configs:
            return code

        lines = code.splitlines()
        builder_end_re = re.compile(r'\.(getOrCreate|enableHiveSupport)\s*\(')
        insert_after   = -1
        for i, line in enumerate(lines):
            if builder_end_re.search(line):
                insert_after = i

        if insert_after == -1:
            if log is not None:
                log.append({
                    "line":        0,
                    "fix":         "spark_config_manual",
                    "description": (
                        "SparkSession builder not found – add these configs manually:\n"
                        + "\n".join(
                            f'spark.conf.set("{k}", "{v}")'
                            for k, v in effective_configs.items()
                        )
                    ),
                })
            return code

        existing = set(re.findall(r'spark\.conf\.set\(["\']([^"\']+)', code))
        missing  = {k: v for k, v in effective_configs.items() if k not in existing}
        if not missing:
            return code

        indent_m = re.match(r'^(\s+)', lines[insert_after])
        indent   = indent_m.group(1) if indent_m else "    "

        config_block = ["", f"{indent}# ── Optimizer-injected Spark configs ──────────────────────"]
        for k, v in missing.items():
            config_block.append(f'{indent}spark.conf.set("{k}", "{v}")')

        new_lines = lines[: insert_after + 1] + config_block + lines[insert_after + 1 :]
        if log is not None:
            log.append({
                "line":        insert_after + 1,
                "fix":         "spark_configs_injected",
                "description": f"Injected {len(missing)} Spark config(s)",
                "configs":     list(missing.keys()),
            })
        return "\n".join(new_lines)

    def _fix_repartition_1(self, code: str, log: List[Dict]) -> str:
        """Replace .repartition(1) → .coalesce(1) to avoid a full shuffle."""
        result = []
        for i, line in enumerate(code.splitlines(), 1):
            fixed = re.sub(r'\.repartition\(\s*1\s*\)', '.coalesce(1)', line)
            if fixed != line:
                log.append({
                    "line":        i,
                    "fix":         "repartition_to_coalesce",
                    "description": "Replaced .repartition(1) with .coalesce(1) – avoids full shuffle",
                    "original":    line.strip(),
                    "fixed":       fixed.strip(),
                })
            result.append(fixed)
        return "\n".join(result)

    def _add_coalesce_before_write(self, code: str, log: List[Dict]) -> str:
        """Add a TODO comment before write calls that lack coalesce/repartition."""
        lines  = code.splitlines()
        result = []
        for i, line in enumerate(lines):
            if re.search(
                r'\.write\s*\.\s*(parquet|orc|csv|json|save|saveAsTable|format)\s*\(', line
            ):
                lookback = "\n".join(lines[max(0, i - 4): i])
                if not re.search(r'\.(coalesce|repartition)\s*\(', lookback):
                    indent = re.match(r'^(\s*)', line).group(1)
                    result.append(
                        f"{indent}"
                        "# TODO [optimizer]: add .coalesce(N) before .write to control "
                        "output file count and prevent the small-file problem"
                    )
                    log.append({
                        "line":        i + 1,
                        "fix":         "coalesce_before_write",
                        "description": "Missing coalesce/repartition before write – may produce many small files",
                    })
            result.append(line)
        return "\n".join(result)

    def _add_broadcast_hints(
        self, code: str, tables: List[Dict], log: List[Dict]
    ) -> str:
        """Add broadcast() hints around small-table references inside .join() calls."""
        broadcast_names = {
            t.get("table", "").lower()
            for t in tables
            if t.get("broadcast") or int(t.get("record_count", 9_999_999)) < _BROADCAST_ROW_THRESHOLD
        }
        if not broadcast_names:
            return code

        lines  = code.splitlines()
        result = []
        for i, line in enumerate(lines, 1):
            fixed = line
            if ".join(" in line:
                for tbl in broadcast_names:
                    pat = rf'(?<!\bbroadcast\()(\b(?:{re.escape(tbl)}(?:_df|_data)?)\b)'
                    if re.search(pat, fixed, re.IGNORECASE):
                        new = re.sub(
                            pat, r'broadcast(\1)', fixed, count=1, flags=re.IGNORECASE
                        )
                        if new != fixed:
                            log.append({
                                "line":        i,
                                "fix":         "broadcast_hint",
                                "description": f"Added broadcast() hint for small table '{tbl}'",
                                "original":    line.strip(),
                                "fixed":       new.strip(),
                            })
                            fixed = new
            result.append(fixed)
        return "\n".join(result)

    def _annotate_collect_calls(self, code: str, log: List[Dict]) -> str:
        """Prepend a warning comment to every .collect() call."""
        lines  = code.splitlines()
        result = []
        for i, line in enumerate(lines, 1):
            if re.search(r'\.collect\(\)', line) and not line.strip().startswith("#"):
                indent = re.match(r'^(\s*)', line).group(1)
                result.append(
                    f"{indent}# WARNING [optimizer]: .collect() pulls ALL data to the driver; "
                    "ensure the DataFrame is pre-filtered / aggregated to a small size"
                )
                log.append({
                    "line":        i,
                    "fix":         "collect_warning",
                    "description": ".collect() may cause driver OOM on large DataFrames",
                })
            result.append(line)
        return "\n".join(result)

    def _annotate_udf_usage(self, code: str, log: List[Dict]) -> str:
        """Prepend a suggestion comment above UDF definitions/decorators."""
        lines  = code.splitlines()
        result = []
        for i, line in enumerate(lines, 1):
            if re.search(r'@udf\b|@pandas_udf\b|= udf\s*\(|=udf\s*\(', line) and \
               not line.strip().startswith("#"):
                indent = re.match(r'^(\s*)', line).group(1)
                result.append(
                    f"{indent}# SUGGESTION [optimizer]: Replace this UDF with a "
                    "pyspark.sql.functions built-in where possible – UDFs are "
                    "10-100× slower (Catalyst cannot optimize them)"
                )
                log.append({
                    "line":        i,
                    "fix":         "udf_suggestion",
                    "description": "UDF detected – consider pyspark.sql.functions for 10-100× speedup",
                })
            result.append(line)
        return "\n".join(result)

    def _add_cache_before_multi_action(self, code: str, log: List[Dict]) -> str:
        """Insert .cache() before the first action on a DataFrame used ≥ 2× by actions."""
        lines     = code.splitlines()
        action_re = re.compile(r'\.(count|show|collect|write|toPandas|first|take|head)\s*\(')
        df_var_re = re.compile(r'^[a-z][a-zA-Z0-9_]*$')

        assigned: Dict[str, int] = {}
        for i, line in enumerate(lines):
            m = re.match(r'^\s*([a-z_][a-zA-Z0-9_]*)\s*=\s*[^=]', line)
            if m:
                var = m.group(1)
                if ("df" in var or var.endswith("_data") or var.endswith("_result")) \
                   and df_var_re.match(var):
                    assigned.setdefault(var, i)

        candidates: List[Tuple[str, int]] = []
        for var, assign_line in assigned.items():
            action_lines = [
                i for i, l in enumerate(lines)
                if var in l and action_re.search(l) and i > assign_line
            ]
            if len(action_lines) >= 2:
                candidates.append((var, min(action_lines)))

        if not candidates:
            return code

        out    = list(lines)
        offset = 0
        for var, first_use in sorted(candidates, key=lambda x: x[1]):
            context_window = "\n".join(lines[max(0, first_use - 5): first_use + 2])
            if f"{var}.cache()" in context_window or f"{var}.persist(" in context_window:
                continue
            insert_at = first_use + offset
            indent    = re.match(r'^(\s*)', out[insert_at]).group(1)
            out.insert(
                insert_at,
                f"{indent}{var}.cache()  # [optimizer] cached: used by multiple actions",
            )
            offset += 1
            log.append({
                "line":        first_use + 1,
                "fix":         "cache_multi_action",
                "description": f"'{var}' referenced by ≥2 actions – .cache() added to avoid recomputation",
            })
        return "\n".join(out)

    # ─── Robust LLM refactoring application ──────────────────────────────────

    def _apply_analysis_refactoring(
        self, code: str, analysis: Dict, log: List[Dict]
    ) -> str:
        """
        Apply ALL refactoring-related entries from the LLM analysis result:

        1. code_refactoring[*] – original_code / improved_code replacement
           Uses fuzzy matching when exact match fails (normalized whitespace).
        2. anti_patterns[*].fix_code – applied at detected line numbers.
        3. spark_configs[*] – injected into SparkSession config block.
        4. optimizations[*] – narrative items added as TODO comments.
        """
        # ── Collect all blocks that may contain these lists ───────────────────
        all_code_refactors: List[Dict] = []
        all_anti_patterns: List[Dict]  = []
        all_spark_configs: List[Dict]  = []
        all_optimizations: List[str]   = []

        def _harvest(block: Any) -> None:
            if isinstance(block, dict):
                all_code_refactors.extend(block.get("code_refactoring", []))
                all_anti_patterns.extend(block.get("anti_patterns", []))
                all_spark_configs.extend(block.get("spark_configs", []))
                raw_opts = block.get("optimizations", [])
                if isinstance(raw_opts, list):
                    for o in raw_opts:
                        all_optimizations.append(str(o))
                elif isinstance(raw_opts, dict):
                    all_optimizations.extend(str(v) for v in raw_opts.values())
            elif isinstance(block, list):
                for item in block:
                    _harvest(item)

        for key in ("code_analysis", "analysis", "recommendations"):
            _harvest(analysis.get(key, {}))
        # Also harvest top-level keys in case the LLM returns a flat structure
        _harvest(analysis)

        # ── 1. code_refactoring ───────────────────────────────────────────────
        for rf in all_code_refactors:
            orig     = (rf.get("original_code") or "").strip()
            improved = (rf.get("improved_code") or "").strip()
            benefit  = rf.get("benefit", "")
            if not orig or not improved or orig == improved:
                continue
            result = self._apply_code_block_replacement(code, orig, improved, benefit, log)
            if result != code:
                code = result

        # ── 2. anti_patterns with fix_code ────────────────────────────────────
        for ap in all_anti_patterns:
            fix_code  = (ap.get("fix_code") or "").strip()
            line_nums = ap.get("line_numbers", [])
            pattern   = ap.get("pattern", "")
            fix_msg   = ap.get("fix", "")
            if not fix_code or not line_nums:
                continue
            lines      = code.splitlines()
            changed    = False
            for ln in line_nums:
                idx = ln - 1
                if 0 <= idx < len(lines):
                    old_line = lines[idx]
                    # Only replace if the fix_code is a proper substitution
                    if old_line.strip() and fix_code not in old_line:
                        indent = re.match(r'^(\s*)', old_line).group(1)
                        lines[idx] = f"{indent}{fix_code}"
                        changed = True
            if changed:
                code = "\n".join(lines)
                log.append({
                    "fix":         "anti_pattern_fix_code",
                    "description": f"Applied fix_code for anti-pattern '{pattern}': {fix_msg}",
                    "pattern":     pattern,
                    "fix_code":    fix_code[:200],
                })

        # ── 3. spark_configs ──────────────────────────────────────────────────
        if all_spark_configs:
            cfg_dict: Dict[str, str] = {}
            for sc in all_spark_configs:
                k = sc.get("config_key") or sc.get("key") or sc.get("name", "")
                v = sc.get("value", "")
                if k and v is not None:
                    cfg_dict[str(k)] = str(v)
            if cfg_dict:
                code = self._inject_spark_configs(code, log, cfg_dict, use_base=False)

        # ── 4. optimizations (narrative) ──────────────────────────────────────
        if all_optimizations:
            opt_comments: List[str] = []
            for opt in all_optimizations:
                opt = opt.strip()
                if opt and len(opt) > 5:
                    opt_comments.append(f"# TODO [LLM optimization]: {opt}")
            if opt_comments:
                # Find the first non-import, non-docstring, non-comment line to insert after
                lines  = code.splitlines()
                insert = 0
                for i, line in enumerate(lines):
                    stripped = line.strip()
                    if stripped and not stripped.startswith(("#", "import ", "from ", '"""', "'''")):
                        insert = i
                        break
                # Insert as a block after the first real code line
                indent = re.match(r'^(\s*)', lines[insert]).group(1) if lines else ""
                injected = [f"{indent}{c}" for c in opt_comments]
                new_lines = lines[:insert] + injected + [""] + lines[insert:]
                code = "\n".join(new_lines)
                log.append({
                    "fix":         "optimization_todos",
                    "description": f"Injected {len(opt_comments)} LLM optimization TODO comment(s)",
                    "items":       [c[:120] for c in opt_comments],
                })

        return code

    # ─── Fuzzy code-block replacement ─────────────────────────────────────────

    def _normalize_code(self, s: str) -> str:
        """Normalize code string for fuzzy matching: collapse whitespace, unify quotes."""
        # Collapse runs of whitespace to single space, strip leading/trailing per line
        lines = [re.sub(r'\s+', ' ', ln.strip()) for ln in s.splitlines()]
        # Unify quotes
        normalized = "\n".join(lines)
        normalized = normalized.replace("'", '"')
        return normalized

    def _apply_code_block_replacement(
        self,
        code: str,
        original: str,
        improved: str,
        benefit: str,
        log: List[Dict],
    ) -> str:
        """
        Replace *original* with *improved* in *code* using a three-tier strategy:

        1. Exact match (fastest).
        2. Normalized whitespace/quote match.
        3. Similarity-based: look for the first ~40 chars of *original* in *code*
           and replace the entire matching block.
        """
        # Tier 1: exact
        if original in code:
            new_code = code.replace(original, improved, 1)
            log.append({
                "fix":         "analysis_refactor",
                "description": f"Applied refactoring (exact): {benefit}",
                "original":    original[:200],
                "fixed":       improved[:200],
            })
            return new_code

        # Tier 2: normalize both sides
        norm_code     = self._normalize_code(code)
        norm_original = self._normalize_code(original)
        if norm_original in norm_code:
            # Find the region in the original code that corresponds
            start_idx = norm_code.find(norm_original)
            end_idx   = start_idx + len(norm_original)
            # Map back to original code lines via character offset
            # Simpler: split on line boundaries and replace matching block
            orig_lines = original.splitlines()
            code_lines = code.splitlines()
            for i in range(len(code_lines) - len(orig_lines) + 1):
                block = code_lines[i: i + len(orig_lines)]
                if self._normalize_code("\n".join(block)) == norm_original:
                    # Preserve the indentation of the first matched line
                    indent = re.match(r'^(\s*)', code_lines[i]).group(1)
                    improved_lines = improved.splitlines()
                    reindented = []
                    for j, il in enumerate(improved_lines):
                        if j == 0:
                            reindented.append(indent + il.lstrip())
                        else:
                            # Relative indentation preserved
                            reindented.append(il)
                    new_lines = code_lines[:i] + reindented + code_lines[i + len(orig_lines):]
                    log.append({
                        "fix":         "analysis_refactor",
                        "description": f"Applied refactoring (normalized match): {benefit}",
                        "original":    original[:200],
                        "fixed":       improved[:200],
                    })
                    return "\n".join(new_lines)

        # Tier 3: similarity anchor – use first non-whitespace 50 chars as anchor
        anchor = original.strip()[:50]
        if anchor and len(anchor) > 10:
            anchor_norm = self._normalize_code(anchor)
            norm_lines  = [self._normalize_code(l) for l in code.splitlines()]
            for i, nl in enumerate(norm_lines):
                if anchor_norm in nl or difflib.SequenceMatcher(
                    None, anchor_norm, nl
                ).ratio() > 0.85:
                    # Replace from this line through the block length
                    n = len(original.splitlines())
                    code_lines = code.splitlines()
                    indent = re.match(r'^(\s*)', code_lines[i]).group(1)
                    reindented = [(indent + ln.lstrip() if j == 0 else ln)
                                  for j, ln in enumerate(improved.splitlines())]
                    new_lines = code_lines[:i] + reindented + code_lines[i + n:]
                    log.append({
                        "fix":         "analysis_refactor",
                        "description": f"Applied refactoring (similarity anchor): {benefit}",
                        "original":    original[:200],
                        "fixed":       improved[:200],
                    })
                    return "\n".join(new_lines)

        # No match found – log a warning comment
        log.append({
            "fix":         "analysis_refactor_skipped",
            "description": (
                f"Could not locate original code block for refactoring '{benefit}' "
                "(exact, normalized, and similarity-anchor matching all failed)"
            ),
            "original":    original[:200],
        })
        return code
