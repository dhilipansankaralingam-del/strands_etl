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

import json as _json
from typing import Any, Dict, List, Optional

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

    print(f"\n  ┌─ Tool: compute_amdahls_ceiling ─────────────────────────────────")
    print(f"  │  Algorithm : Amdahl's Law  S(N) = 1 / (s + (1-s)/N)")
    print(f"  │  Purpose   : Finds the hard ceiling on parallel speedup.")
    print(f"  │              Even with infinite workers, a job with {serial_fraction_pct:.0f}% serial")
    print(f"  │              code can never go faster than {100/serial_fraction_pct:.1f}× — adding")
    print(f"  │              workers beyond the elbow point wastes money.")
    print(f"  │  Inputs    : serial={serial_fraction_pct}%  workers={current_workers}")

    result = amdahls_law(
        serial_fraction=serial_fraction_pct / 100.0,
        num_workers=max(1, int(current_workers)),
    )

    speedup   = result.get("amdahl_speedup", 0)
    max_spdup = result.get("theoretical_max_speedup", 0)
    elbow     = result.get("diminishing_returns_elbow", 0)
    print(f"  │  Result    : current speedup={speedup:.2f}×  max possible={max_spdup:.1f}×")
    print(f"  │              diminishing-returns elbow at {elbow} workers")
    print(f"  │  Benefit   : Prevents over-provisioning workers past the point")
    print(f"  │              where each extra worker costs more than it saves.")
    print(f"  └─────────────────────────────────────────────────────────────────\n")
    return result


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

    print(f"\n  ┌─ Tool: compute_spot_risk ───────────────────────────────────────")
    print(f"  │  Algorithm : Survival probability  P(survive t hrs) = (1-p)^t")
    print(f"  │  Purpose   : Spot instances are ~70% cheaper but can be reclaimed")
    print(f"  │              at any time. This computes whether the expected")
    print(f"  │              restart cost erodes the discount below the break-even.")
    print(f"  │  Inputs    : duration={job_duration_hours}h  instance={instance_type}")
    print(f"  │              hourly_interrupt_rate={rate:.0%}  checkpoint={checkpoint_interval_hours}h")

    result = spot_interruption_risk(
        job_duration_hours=max(0.1, job_duration_hours),
        hourly_interruption_rate=rate,
        checkpoint_interval_hours=checkpoint_interval_hours,
    )
    result["instance_type"]   = instance_type
    result["instance_family"] = family

    p_survive = result.get("p_survive_full_job", 0)
    net_save  = result.get("net_savings_pct", 0)
    print(f"  │  Result    : P(survive)={p_survive:.1%}  net savings={net_save:.1f}%")
    print(f"  │  Benefit   : Only recommend Spot if net_savings > 30% AND")
    print(f"  │              P(survive) > 75% — avoids false economy.")
    print(f"  └─────────────────────────────────────────────────────────────────\n")
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

    label = table_name or "table"
    print(f"\n  ┌─ Tool: compute_skew_model  [{label}] ───────────────────────────")
    print(f"  │  Algorithm : Zipf / power-law  size(k) ∝ k^(-α)")
    print(f"  │              α estimated via Hill estimator on {len(sizes)} partition sizes.")
    print(f"  │  What it means:")
    print(f"  │    α ≈ 0   → uniform (no skew, good)")
    print(f"  │    α ≈ 1   → standard Zipf (rank-2 partition has half of rank-1's data)")
    print(f"  │    α > 2   → extreme skew (one partition dominates → stragglers / OOM)")
    print(f"  │  Purpose   : Derives the SALT FACTOR mathematically — instead of")
    print(f"  │              guessing 'try 8 buckets', gives the exact N that balances")
    print(f"  │              the distribution given the measured α.")
    print(f"  │  Benefit   : Salting eliminates join stragglers. The salt factor N")
    print(f"  │              tells Spark to explode the skewed key into N sub-keys,")
    print(f"  │              spreading load evenly across executors.")
    print(f"  │  Inputs    : {len(sizes)} partition sizes, max={max(sizes):,.0f}  min={min(sizes):,.0f}")

    result = zipf_skew_model(partition_sizes=sizes)
    if table_name:
        result["table_name"] = table_name

    alpha    = result.get("zipf_alpha", 0)
    severity = result.get("skew_severity", "UNKNOWN")
    salt     = result.get("recommended_salt_factor", 1)
    print(f"  │  Result    : α={alpha:.3f}  severity={severity}  salt_factor={salt}")
    print(f"  └─────────────────────────────────────────────────────────────────\n")
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
    print(f"\n  ┌─ Tool: compute_growth_forecast  [{table_name}] ─────────────────")
    print(f"  │  Algorithm : Euler exponential growth  N(t) = N₀ · e^(r·t)")
    print(f"  │  Why not linear? Each day's new data is added to a LARGER base,")
    print(f"  │  so the absolute GB added tomorrow is slightly more than today.")
    print(f"  │  Exponential captures this compounding; linear under-estimates.")
    print(f"  │  Inputs    : current={current_size_gb:.2f} GB  daily_growth={daily_growth_gb:.4f} GB/day")
    print(f"  │              daily_rate r={daily_rate:.6f}  horizon={forecast_days} days")
    print(f"  │  Benefit   : Tells you WHEN the table will hit cost thresholds,")
    print(f"  │              doubling time, and whether current workers will cope")
    print(f"  │              in 90 days — so you plan capacity before the breach.")

    result = exponential_growth(
        initial_gb=current_size_gb,
        daily_rate=daily_rate,
        days=forecast_days,
    )
    result["table_name"] = table_name

    proj      = result.get("projected_gb", 0)
    doubling  = result.get("doubling_time_days", 0)
    cost_inc  = result.get("monthly_storage_cost_increase_usd", 0)
    print(f"  │  Result    : {forecast_days}d projection={proj:.2f} GB  doubling={doubling:.0f} days")
    print(f"  │              monthly cost increase ≈ ${cost_inc:.2f}")
    print(f"  └─────────────────────────────────────────────────────────────────\n")
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

    print(f"\n  ┌─ Tool: compute_bloom_filter_value  [{table_name}] ──────────────")
    print(f"  │  Algorithm : Bloom filter  P(FP) = (1 - e^(-k·n/m))^k")
    print(f"  │              k_optimal = (m/n)·ln2")
    print(f"  │  What is a Bloom filter?")
    print(f"  │    A probabilistic bit-array that answers 'is this join key in")
    print(f"  │    the build side?' in O(1) — before reading any data from S3.")
    print(f"  │    If the answer is NO (definitely not), Spark skips that row")
    print(f"  │    entirely.  If YES (probably yes), it reads and checks.")
    print(f"  │    False positives cause unnecessary reads; false negatives are")
    print(f"  │    impossible — correctness is guaranteed.")
    print(f"  │  Benefit   : With {join_selectivity_pct:.0f}% selectivity, {100-join_selectivity_pct:.0f}% of rows")
    print(f"  │              CAN be skipped. Bloom filters capture most of that")
    print(f"  │              savings with only a few MB of memory overhead.")
    print(f"  │  Inputs    : table={table_size_gb:.2f} GB  selectivity={join_selectivity_pct}%")
    print(f"  │              distinct_keys={num_distinct_join_keys:,}")

    result = bloom_filter_savings(
        table_size_gb=table_size_gb,
        join_selectivity=join_selectivity_pct / 100.0,
        num_distinct_keys=max(1, num_distinct_join_keys),
        bits_per_key=10,
    )
    result["table_name"] = table_name

    io_saved = result.get("io_saved_gb", 0)
    fpr      = result.get("false_positive_rate", 0)
    worth    = result.get("worth_enabling", False)
    print(f"  │  Result    : io_saved={io_saved:.2f} GB  false_positive_rate={fpr:.4f}")
    print(f"  │              worth_enabling={worth}")
    print(f"  └─────────────────────────────────────────────────────────────────\n")
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

    print(f"\n  ┌─ Tool: compute_shuffle_partitions ──────────────────────────────")
    print(f"  │  Algorithm : Little's Law  L = λ · W")
    print(f"  │              L = tasks in flight  λ = throughput  W = task duration")
    print(f"  │  What it solves:")
    print(f"  │    Too few shuffle partitions → huge tasks, executor OOM.")
    print(f"  │    Too many partitions → thousands of tiny 1-sec tasks,")
    print(f"  │    scheduler overhead dominates, executors sit idle between tasks.")
    print(f"  │    Little's Law finds the sweet spot where all {num_executors} cores")
    print(f"  │    stay busy with tasks of the right size (~2-4 min each).")
    print(f"  │  Inputs    : avg_task={avg_task_duration_sec}s  executors={num_executors}")

    result = littles_law_parallelism(
        avg_task_sec=avg_task_duration_sec,
        num_executors=max(1, int(num_executors)),
        total_tasks=total_tasks,
    )

    opt_parts = result.get("optimal_shuffle_partitions", 0)
    util      = result.get("executor_utilisation", 0)
    print(f"  │  Result    : optimal_shuffle_partitions={opt_parts}")
    print(f"  │              executor_utilisation={util:.1%}")
    print(f"  │  Benefit   : Setting spark.sql.shuffle.partitions={opt_parts} ensures")
    print(f"  │              tasks are right-sized — no OOM, no scheduling waste.")
    print(f"  └─────────────────────────────────────────────────────────────────\n")
    return result


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

    print(f"\n  ┌─ Tool: rank_recommendations_by_impact ──────────────────────────")
    print(f"  │  Algorithm : Pareto 80/20  score = savings% / √effort_hours")
    print(f"  │  What it does:")
    print(f"  │    Ranks {len(recs)} recommendations by impact-per-effort.")
    print(f"  │    High savings + low effort = highest score (quick wins).")
    print(f"  │    √effort penalises large efforts gently — a 4-hour fix with")
    print(f"  │    40% savings still beats a 1-hour fix with 5% savings.")
    print(f"  │  Benefit   : Ensures the implementation roadmap leads with the")
    print(f"  │              20% of changes that deliver 80% of the cost reduction.")

    result = pareto_rank_recommendations(recs)

    pareto_n = result.get("pareto_count", 0)
    print(f"  │  Result    : {pareto_n} recommendations in Pareto front (quick wins)")
    print(f"  └─────────────────────────────────────────────────────────────────\n")
    return result


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

    print(f"\n  ┌─ Tool: detect_cost_anomaly  [{metric_label}] ───────────────────")
    print(f"  │  Algorithm : Shewhart X-bar 3σ control chart")
    print(f"  │              + Western Electric alarm rules")
    print(f"  │  What it does:")
    print(f"  │    Computes mean (μ) and std (σ) of {len(hist)} historical runs.")
    print(f"  │    UCL = μ + 3σ,  LCL = μ - 3σ  (99.7% confidence band).")
    print(f"  │    A point outside UCL/LCL is a statistically significant spike,")
    print(f"  │    not just noise — something structurally changed.")
    print(f"  │    Western Electric rules catch subtler patterns: 2 of 3 points")
    print(f"  │    in 2σ zone, 4 of 5 in 1σ zone, 8 consecutive on one side.")
    print(f"  │  Benefit   : Catches regressions (skew introduced, new anti-pattern)")
    print(f"  │              before they compound into incidents or cost overruns.")
    print(f"  │  Inputs    : {len(hist)} historical values  current={current_value}")

    result = shewhart_control_chart(
        history=hist,
        current_value=float(current_value),
        label=metric_label,
    )

    z     = result.get("z_score", 0)
    zone  = result.get("zone", "?")
    anom  = result.get("is_anomaly", False)
    print(f"  │  Result    : z_score={z:.2f}  zone={zone}  is_anomaly={anom}")
    print(f"  └─────────────────────────────────────────────────────────────────\n")
    return result


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

    label = metric_name or "metric"
    print(f"\n  ┌─ Tool: detect_metric_periodicity  [{label}] ────────────────────")
    print(f"  │  Algorithm : Discrete Fourier Transform (DFT)")
    print(f"  │              Converts time-domain signal → frequency-domain.")
    print(f"  │  What it does:")
    print(f"  │    Decomposes {len(series)} data points into frequency components.")
    print(f"  │    The dominant frequency tells you the repeating cycle length")
    print(f"  │    (e.g. 24h = daily batch, 168h = weekly ETL).")
    print(f"  │  Why it matters:")
    print(f"  │    A spike every 24h = expected batch load (schedule OPTIMIZE")
    print(f"  │    in the trough window to avoid overlap).")
    print(f"  │    A spike with no period = structural issue (skew, OOM, bad join)")
    print(f"  │    that won't resolve itself — needs a code fix.")
    print(f"  │  Benefit   : Distinguishes 'schedule this differently' from")
    print(f"  │              'fix the code' — very different remediation paths.")
    print(f"  │  Inputs    : {len(series)} points  interval={sample_interval_minutes}min")

    result = fourier_periodicity(
        time_series=series,
        sample_interval_minutes=sample_interval_minutes,
    )
    if metric_name:
        result["metric_name"] = metric_name

    period = result.get("dominant_period_hr", 0)
    dlabel = result.get("dominant_label", "?")
    print(f"  │  Result    : dominant_period={period:.1f}h  ({dlabel})")
    print(f"  └─────────────────────────────────────────────────────────────────\n")
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


# =============================================================================
# BOTO3 NATIVE TOOL-CALLING SUPPORT
# Used when strands-agents is NOT installed.  Provides the same agentic loop
# via the Anthropic tool_use protocol directly over Bedrock bedrock-runtime.
# =============================================================================

_BOTO3_TOOL_SCHEMAS: Dict[str, Dict] = {
    "compute_amdahls_ceiling": {
        "name": "compute_amdahls_ceiling",
        "description": (
            "Amdahl's Law: maximum parallel speedup S(N)=1/(s+(1-s)/N). "
            "Call when collect(), toPandas(), show() or iterate_collect() are detected — "
            "these run on the driver only and hard-cap the benefit of extra workers. "
            "Returns amdahl_speedup, theoretical_max_speedup, diminishing_returns_elbow, recommendation."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "serial_fraction_pct": {
                    "type": "number",
                    "description": "Estimated % of job that is serial (0–99). Each collect/toPandas counts ~8%.",
                },
                "current_workers": {
                    "type": "integer",
                    "description": "Current or proposed Glue / EMR worker count.",
                },
            },
            "required": ["serial_fraction_pct", "current_workers"],
        },
    },
    "compute_spot_risk": {
        "name": "compute_spot_risk",
        "description": (
            "Spot interruption risk: P(survive t hours) = (1 − p_hourly)^t. "
            "Call before recommending EMR Spot or EKS Spot to quantify net savings "
            "after expected restart cost. Only recommend Spot if net_savings_pct > 30 "
            "AND p_survive > 0.75. "
            "Returns p_survive_full_job, p_interrupted, net_savings_pct, recommendation."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "job_duration_hours": {
                    "type": "number",
                    "description": "Estimated wall-clock duration in hours.",
                },
                "instance_type": {
                    "type": "string",
                    "description": "EC2 instance type, e.g. r5.4xlarge. Family sets interruption rate.",
                },
                "checkpoint_interval_hours": {
                    "type": "number",
                    "description": "Checkpoint interval in hours. 0 = no checkpointing (full restart).",
                },
            },
            "required": ["job_duration_hours"],
        },
    },
    "compute_skew_model": {
        "name": "compute_skew_model",
        "description": (
            "Zipf / power-law skew model using the Hill estimator. "
            "Call when skew_ratio (max_partition / avg_partition) > 3. "
            "Derives the optimal salt_factor mathematically (not a guess). "
            "Returns zipf_alpha, skew_severity, recommended_salt_factor, spark_salting_snippet."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "partition_sizes": {
                    "type": "array",
                    "items": {"type": "number"},
                    "description": "List of partition sizes (record counts or bytes). Minimum 2 values.",
                },
                "table_name": {
                    "type": "string",
                    "description": "Display label for the table.",
                },
            },
            "required": ["partition_sizes"],
        },
    },
    "compute_growth_forecast": {
        "name": "compute_growth_forecast",
        "description": (
            "Euler exponential growth: N(t) = N0 * e^(r*t). "
            "Call when a table has a measurable daily growth rate. "
            "More accurate than linear for compounding write patterns. "
            "Returns projected_gb, doubling_time_days, milestones, monthly_storage_cost_increase_usd."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name":       {"type": "string", "description": "Table label."},
                "current_size_gb":  {"type": "number", "description": "Table size today in GB."},
                "daily_growth_gb":  {"type": "number", "description": "GB added per day."},
                "forecast_days":    {"type": "integer", "description": "Forecast horizon (default 90 days)."},
            },
            "required": ["table_name", "current_size_gb", "daily_growth_gb"],
        },
    },
    "compute_bloom_filter_value": {
        "name": "compute_bloom_filter_value",
        "description": (
            "Bloom filter I/O savings: P(FP)=(1-e^(-kn/m))^k. "
            "Call for every table > 1 GB in a JOIN that is not a broadcast candidate. "
            "Returns false_positive_rate, io_saved_gb, worth_enabling, iceberg_ddl."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name":             {"type": "string", "description": "The probed (larger) table."},
                "table_size_gb":          {"type": "number", "description": "Current live size in GB."},
                "join_selectivity_pct":   {"type": "number", "description": "% of rows that match (default 5)."},
                "num_distinct_join_keys": {"type": "integer", "description": "Distinct join key values in the build table."},
            },
            "required": ["table_name", "table_size_gb"],
        },
    },
    "compute_shuffle_partitions": {
        "name": "compute_shuffle_partitions",
        "description": (
            "Little's Law: L = λ·W → optimal spark.sql.shuffle.partitions. "
            "Call when shuffle stage is the bottleneck or AQE is producing tiny tasks. "
            "Returns optimal_shuffle_partitions, task_throughput_per_sec, executor_utilisation, spark_config."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "avg_task_duration_sec": {"type": "number",  "description": "Mean Spark task duration in seconds."},
                "num_executors":         {"type": "integer", "description": "Total executor cores = workers × vCPU_per_worker."},
                "total_tasks":           {"type": "integer", "description": "Total tasks in the slowest stage (optional)."},
            },
            "required": ["avg_task_duration_sec", "num_executors"],
        },
    },
    "rank_recommendations_by_impact": {
        "name": "rank_recommendations_by_impact",
        "description": (
            "Pareto 80/20: score = estimated_savings_percent / sqrt(effort_hours). "
            "Call FIRST, ALWAYS — identifies which 20% of fixes deliver 80% of savings. "
            "Returns pareto_front (quick-wins), ranked_recommendations, pareto_count, interpretation."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "recommendations": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": (
                        "List of dicts, each with at minimum: "
                        "{title: str, estimated_savings_percent: float, effort_hours: float}."
                    ),
                },
            },
            "required": ["recommendations"],
        },
    },
    "detect_cost_anomaly": {
        "name": "detect_cost_anomaly",
        "description": (
            "Shewhart X-bar 3-sigma control chart + Western Electric rules. "
            "Call when glue_metrics has ≥ 5 historical data points for any metric. "
            "Returns z_score, zone (normal/1σ/2σ/3σ), is_anomaly, ucl_3sigma, lcl_3sigma, signals."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "historical_values": {
                    "type": "array",
                    "items": {"type": "number"},
                    "description": "Previous measurements oldest→newest, NOT including current value.",
                },
                "current_value": {"type": "number", "description": "The new observation to test."},
                "metric_label":  {"type": "string", "description": "Label e.g. heap_peak or cost_usd."},
            },
            "required": ["historical_values", "current_value"],
        },
    },
    "detect_metric_periodicity": {
        "name": "detect_metric_periodicity",
        "description": (
            "Discrete Fourier Transform: dominant period in a CloudWatch time series. "
            "Call when glue_metrics has ≥ 8 data points to distinguish periodic vs structural issues. "
            "Returns dominant_period_hr, dominant_label (hourly/daily/weekly), top_frequencies, interpretation."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "metric_time_series": {
                    "type": "array",
                    "items": {"type": "number"},
                    "description": "Ordered metric values (cost, heap%, CPU%).",
                },
                "sample_interval_minutes": {
                    "type": "integer",
                    "description": "Sampling cadence in minutes (CloudWatch default = 5).",
                },
                "metric_name": {"type": "string", "description": "Display label."},
            },
            "required": ["metric_time_series"],
        },
    },
}

# Function name → callable mapping (works whether strands decorator applied or not)
_TOOL_FN_MAP: Dict[str, Any] = {
    "compute_amdahls_ceiling":        compute_amdahls_ceiling,
    "compute_spot_risk":              compute_spot_risk,
    "compute_skew_model":             compute_skew_model,
    "compute_growth_forecast":        compute_growth_forecast,
    "compute_bloom_filter_value":     compute_bloom_filter_value,
    "compute_shuffle_partitions":     compute_shuffle_partitions,
    "rank_recommendations_by_impact": rank_recommendations_by_impact,
    "detect_cost_anomaly":            detect_cost_anomaly,
    "detect_metric_periodicity":      detect_metric_periodicity,
}


def get_boto3_tool_schemas(tools: list) -> List[Dict]:
    """Return Bedrock-compatible tool schema dicts for the given tool list."""
    schemas = []
    for fn in tools:
        name = getattr(fn, "__name__", None) or getattr(fn, "name", str(fn))
        schema = _BOTO3_TOOL_SCHEMAS.get(name)
        if schema:
            schemas.append(schema)
    return schemas


def execute_tool_call(tool_name: str, tool_input: Dict) -> Dict:
    """Execute a pipeline tool by name and return its result dict."""
    fn = _TOOL_FN_MAP.get(tool_name)
    if fn is None:
        msg = f"Unknown tool: {tool_name}"
        print(f"  [TOOL ERROR] {msg}")
        return {"error": msg}
    try:
        result = fn(**tool_input)
        return result if isinstance(result, dict) else {"result": result}
    except Exception as exc:
        import traceback
        print(f"  [TOOL ERROR] {tool_name} raised {type(exc).__name__}: {exc}")
        print(f"  {traceback.format_exc().strip()}")
        return {"error": str(exc), "tool": tool_name, "input": tool_input}
