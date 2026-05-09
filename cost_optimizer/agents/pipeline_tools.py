"""
Pipeline Agent Tools
====================
Bounded @strands_tool sets for the 4 pipeline agents (SizeAnalyzer,
CodeAnalyzer, ResourceAllocator, RecommendationsAgent).

Design rules:
  - Each tool is stateless and self-contained (no _state cache dependency).
  - No tool does an AWS call — they wrap pure-math functions from scientific_tools.py.
  - Each agent's tool set is small (2–4 tools) with clear docstrings so Claude
    knows exactly WHEN to call each one.
  - If strands is not installed, all decorators become no-ops and the lists are
    populated with the plain functions (graceful degradation).
"""

from __future__ import annotations

from typing import Dict, List, Optional

# ── Strands decorator (graceful no-op when package absent) ───────────────────
try:
    try:
        from strands.tools import tool as strands_tool
    except ImportError:
        from strands import tool as strands_tool          # type: ignore[attr-defined]
    _HAS_STRANDS = True
except ImportError:
    def strands_tool(fn):                                 # type: ignore[misc]
        return fn
    _HAS_STRANDS = False


# =============================================================================
# SHARED SCIENTIFIC TOOLS  (used by multiple agents)
# =============================================================================

@strands_tool
def compute_amdahls_ceiling(serial_fraction_pct: float, current_workers: int) -> Dict:
    """
    Amdahl's Law: S(N) = 1 / (s + (1-s)/N).

    CALL WHEN: collect(), toPandas(), show(), or iterate_collect() anti-patterns
    are detected. These operations run on the driver only (serial), which hard-caps
    the benefit of adding more workers.

    serial_fraction_pct: estimated % of job that is serial (0–99).
    current_workers:     current or proposed Glue / EMR worker count.

    Returns: amdahl_speedup, theoretical_max_speedup, diminishing_returns_elbow,
             efficiency_score, recommendation.
    """
    from .scientific_tools import amdahls_law
    if not (0 < serial_fraction_pct < 100):
        return {"error": "serial_fraction_pct must be between 1 and 99"}
    return amdahls_law(
        serial_fraction=serial_fraction_pct / 100.0,
        num_workers=max(1, int(current_workers)),
    )


@strands_tool
def compute_spot_risk(
    job_duration_hours: float,
    instance_type: str = "m5.2xlarge",
    checkpoint_interval_hours: float = 0.0,
) -> Dict:
    """
    Spot interruption risk: P(survive t hours) = (1 − p_hourly)^t.

    CALL WHEN: recommending EMR Spot or EKS Spot to quantify the real net
    savings after expected restart/rework cost.

    job_duration_hours:        estimated wall-clock duration.
    instance_type:             EC2 instance type (family used for rate lookup).
    checkpoint_interval_hours: 0 = no checkpointing (full restart on interrupt).

    Returns: p_survive_full_job, p_interrupted, expected_rework_hours,
             net_savings_pct, recommendation.
    """
    from .scientific_tools import spot_interruption_risk
    _rates = {
        "m5": 0.05, "m5a": 0.05, "m5n": 0.06,
        "r5": 0.07, "r5a": 0.07, "c5": 0.04,
        "m6i": 0.04, "r6i": 0.06, "r6g": 0.04,
        "g4dn": 0.12, "p3": 0.15,
    }
    family = instance_type.split(".")[0].lower()
    rate   = _rates.get(family, 0.07)
    result = spot_interruption_risk(
        job_duration_hours=max(0.1, job_duration_hours),
        hourly_interruption_rate=rate,
        checkpoint_interval_hours=checkpoint_interval_hours,
    )
    result["instance_type"]   = instance_type
    result["instance_family"] = family
    return result


# =============================================================================
# SIZE ANALYZER TOOLS
# =============================================================================

@strands_tool
def compute_skew_model(partition_sizes: List, table_name: str = "") -> Dict:
    """
    Zipf / power-law skew model using the Hill estimator.

    CALL WHEN: skew_ratio (max_partition / avg_partition) > 3.
    Derives the Zipf exponent α and the mathematically optimal salt factor
    so Claude can recommend salting with a justified number (not a guess).

    partition_sizes: list of partition sizes (record counts or bytes).
                     Pass the raw sizes if available; if only skew_ratio and
                     partition_count are known, synthesise:
                       [avg * skew_ratio^((n-k)/(n-1)) for k in 1..n]
    table_name:      display label.

    Returns: zipf_alpha, skew_severity (MILD/MODERATE/HIGH/EXTREME),
             recommended_salt_factor, spark_salting_snippet.
    """
    from .scientific_tools import zipf_skew_model
    sizes = [float(x) for x in partition_sizes]
    if len(sizes) < 2:
        return {"error": "Need at least 2 partition sizes"}
    result = zipf_skew_model(partition_sizes=sizes)
    if table_name:
        result["table_name"] = table_name
    return result


@strands_tool
def compute_growth_forecast(
    table_name: str,
    current_size_gb: float,
    daily_growth_gb: float,
    forecast_days: int = 90,
) -> Dict:
    """
    Euler's exponential growth: N(t) = N₀ · e^(r·t).

    CALL WHEN: a table has a measurable daily growth rate (from snapshot
    timestamps or CloudWatch). More accurate than linear for tables with
    compounding write patterns (each write adds to a growing base).

    current_size_gb:  table size today in GB.
    daily_growth_gb:  measured GB added per day.
    forecast_days:    projection horizon (default 90).

    Returns: projected_gb, doubling_time_days, milestones (2×/5×/10×),
             monthly_storage_cost_increase_usd.
    """
    from .scientific_tools import exponential_growth
    if current_size_gb <= 0 or daily_growth_gb <= 0:
        return {"error": "current_size_gb and daily_growth_gb must be > 0"}
    daily_rate = daily_growth_gb / current_size_gb
    result     = exponential_growth(
        initial_gb=current_size_gb,
        daily_rate=daily_rate,
        days=forecast_days,
    )
    result["table_name"] = table_name
    return result


@strands_tool
def compute_bloom_filter_value(
    table_name: str,
    table_size_gb: float,
    join_selectivity_pct: float = 5.0,
    num_distinct_join_keys: int = 1_000_000,
) -> Dict:
    """
    Bloom filter I/O savings: P(FP) = (1 − e^(−k·n/m))^k, k_opt = (m/n)·ln2.

    CALL WHEN: a table > 1 GB participates in a join AND is NOT a broadcast
    candidate (i.e., it cannot fit in driver memory). Lower join_selectivity
    means more rows are skipped = bigger savings.

    table_name:             the probed (larger) table in the join.
    table_size_gb:          current live size in GB.
    join_selectivity_pct:   % of rows that match (5 = 5% match → 95% skipped).
    num_distinct_join_keys: distinct values of the join key in the build table.

    Returns: false_positive_rate, io_saved_gb, worth_enabling, iceberg_ddl.
    """
    from .scientific_tools import bloom_filter_savings
    result = bloom_filter_savings(
        table_size_gb=table_size_gb,
        join_selectivity=join_selectivity_pct / 100.0,
        num_distinct_keys=max(1, num_distinct_join_keys),
        bits_per_key=10,
    )
    result["table_name"] = table_name
    return result


# =============================================================================
# CODE ANALYZER TOOLS
# =============================================================================

@strands_tool
def compute_shuffle_partitions(
    avg_task_duration_sec: float,
    num_executors: int,
    total_tasks: Optional[int] = None,
) -> Dict:
    """
    Little's Law: L = λ · W → optimal spark.sql.shuffle.partitions.

    CALL WHEN: shuffle stage is the bottleneck, tasks are too short/long,
    or AQE is overriding partition counts and producing tiny tasks.

    avg_task_duration_sec: mean Spark task duration (from Glue metrics or
                           Spark UI → Stages → Task metrics).
    num_executors:         total executor cores = workers × vCPU_per_worker.
    total_tasks:           total tasks in the slowest stage (optional).

    Returns: optimal_shuffle_partitions, task_throughput_per_sec,
             executor_utilisation, spark_config dict.
    """
    from .scientific_tools import littles_law_parallelism
    if avg_task_duration_sec <= 0:
        return {"error": "avg_task_duration_sec must be > 0"}
    return littles_law_parallelism(
        avg_task_sec=avg_task_duration_sec,
        num_executors=max(1, int(num_executors)),
        total_tasks=total_tasks,
    )


# =============================================================================
# RECOMMENDATIONS AGENT TOOLS
# =============================================================================

@strands_tool
def rank_recommendations_by_impact(recommendations: List) -> Dict:
    """
    Pareto 80/20: score = estimated_savings_percent / √effort_hours.

    CALL ALWAYS at the start of synthesis — identifies which 20% of fixes
    deliver 80% of the total savings so the implementation roadmap is
    impact-ordered, not arbitrary.

    recommendations: list of dicts; each must have at least:
      { "title": "...", "estimated_savings_percent": N, "effort_hours": H }
      (missing fields default to 0).

    Returns: pareto_front (top quick-wins), ranked_recommendations,
             pareto_count, interpretation.
    """
    from .scientific_tools import pareto_rank_recommendations
    recs = [dict(r) if not isinstance(r, dict) else r for r in recommendations]
    if not recs:
        return {"error": "recommendations list is empty"}
    return pareto_rank_recommendations(recs)


@strands_tool
def detect_cost_anomaly(
    historical_values: List,
    current_value: float,
    metric_label: str = "cost_or_duration",
) -> Dict:
    """
    Shewhart X-bar 3-sigma control chart + Western Electric rules.

    CALL WHEN: glue_metrics contains ≥ 5 historical data points for any
    metric (cost, duration, heap peak). Detects statistically significant
    regressions before they become incidents.

    historical_values: previous run measurements (oldest → newest, excl. current).
    current_value:     the new observation to test.
    metric_label:      display label (e.g. "heap_peak", "cost_usd").

    Returns: z_score, zone (normal/1σ/2σ/3σ), is_anomaly,
             ucl_3sigma, lcl_3sigma, western_electric_signals.
    """
    from .scientific_tools import shewhart_control_chart
    hist = [float(v) for v in historical_values]
    if len(hist) < 4:
        return {"error": "Need at least 4 historical values for control chart"}
    return shewhart_control_chart(
        history=hist,
        current_value=float(current_value),
        label=metric_label,
    )


@strands_tool
def detect_metric_periodicity(
    metric_time_series: List,
    sample_interval_minutes: int = 5,
    metric_name: str = "",
) -> Dict:
    """
    Discrete Fourier Transform: dominant period in a CloudWatch time series.

    CALL WHEN: glue_metrics has ≥ 8 data points. Identifies whether cost/heap
    spikes are periodic (daily batch, weekly ETL) vs structural (skew, OOM).
    Use the dominant period to schedule OPTIMIZE/VACUUM at the trough.

    metric_time_series:      ordered metric values (cost, heap%, CPU%).
    sample_interval_minutes: CloudWatch default = 5 min.
    metric_name:             display label.

    Returns: dominant_period_hr, dominant_label (hourly/daily/weekly),
             top_frequencies, interpretation.
    """
    from .scientific_tools import fourier_periodicity
    series = [float(v) for v in metric_time_series]
    if len(series) < 8:
        return {"error": "Need at least 8 data points for DFT"}
    result = fourier_periodicity(
        time_series=series,
        sample_interval_minutes=sample_interval_minutes,
    )
    if metric_name:
        result["metric_name"] = metric_name
    return result


# =============================================================================
# TOOL SETS — one list per pipeline agent
# These are imported by each agent class as AGENT_TOOLS.
# =============================================================================

SIZE_AGENT_TOOLS: list = [
    compute_skew_model,           # when skew_ratio > 3
    compute_growth_forecast,      # when daily growth rate is known
    compute_bloom_filter_value,   # for every large join table
]

CODE_AGENT_TOOLS: list = [
    compute_amdahls_ceiling,      # when serial ops (collect/toPandas) detected
    compute_shuffle_partitions,   # when shuffle bottleneck or task duration known
]

RESOURCE_AGENT_TOOLS: list = [
    compute_amdahls_ceiling,      # validate worker ceiling from code findings
    compute_spot_risk,            # before recommending EMR Spot
]

RECOMMENDATIONS_AGENT_TOOLS: list = [
    rank_recommendations_by_impact,  # always — Pareto ordering
    detect_cost_anomaly,             # when ≥5 historical metric points
    detect_metric_periodicity,       # when ≥8 metric time-series points
]
