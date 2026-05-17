"""
Scientific Algorithm Agent
===========================
Pure-Python implementations of 14 mathematical models for PySpark / ETL
cost optimisation.  No external dependencies — uses only stdlib (math,
statistics, random).

Algorithms ported from cost_optimizer/agents/scientific_tools.py
(Branch: claude/table-optimizer-analysis-KEYJa) plus three new additions:

  Ported (11):
    1.  amdahls_law               – speedup ceiling → optimal worker count
    2.  euler_exponential_growth  – N₀·e^(rt) table-size & cost forecast
    3.  zipf_skew_model           – power-law partition skew → salting factor
    4.  shannon_entropy           – column compressibility → codec selection
    5.  gaussian_file_distribution – bimodality coefficient → tiny-file detection
    6.  littles_law_parallelism   – queueing theory → shuffle.partitions
    7.  shewhart_control_chart    – 3-sigma anomaly detection for job cost
    8.  fourier_periodicity       – DFT dominant cycle in CloudWatch series
    9.  bloom_filter_savings      – FPP & I/O savings for join push-down
   10.  spot_interruption_risk    – geometric-distribution spot risk model
   11.  pareto_rank_recommendations – 80/20 impact-per-effort ranking

  New (3):
   12.  planck_partition_temperature – Planck blackbody analogy: hot-partition
                                       detection via energy-per-frequency model
   13.  newtons_cooling_worker_decay – Newton's law of cooling: worker
                                       utilisation decay after peak load
   14.  monte_carlo_cost_simulation  – Monte Carlo job-cost uncertainty bounds
"""

import json
import logging
import math
import random
import statistics
from typing import Any, Dict, List, Optional

from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a **Computational Data-Science Architect** applying rigorous mathematical
models to ETL resource-optimisation problems.

Use the scientific tools to:
1. Determine the provably-optimal worker count via Amdahl's Law
2. Forecast data growth and storage cost with Euler's exponential model
3. Quantify partition skew with the Zipf/Hill estimator and recommend salting
4. Detect tiny-file pathologies via the Gaussian bimodality coefficient
5. Derive shuffle.partitions from Little's Law queueing theory
6. Flag cost anomalies using Shewhart 3-sigma control charts
7. Identify periodic CloudWatch signals via the Discrete Fourier Transform
8. Estimate Bloom-filter I/O savings for selective joins
9. Model Spot interruption probability with the geometric distribution
10. Rank recommendations by the Pareto 80/20 impact-per-effort score
11. Detect "hot" partitions with the Planck blackbody temperature analogy
12. Model worker utilisation decay using Newton's Law of Cooling
13. Bound job cost uncertainty with Monte Carlo simulation

Always show the formula used and cite which signal triggered the recommendation.
Return structured JSON only.
"""


# ─────────────────────────────────────────────────────────────────────────────
# Internal implementations (pure Python, no external deps)
# ─────────────────────────────────────────────────────────────────────────────

def _amdahls_law(serial_fraction: float, num_workers: int) -> Dict:
    if not (0 < serial_fraction < 1):
        return {"error": "serial_fraction must be strictly between 0 and 1"}
    speedup_n   = 1.0 / (serial_fraction + (1.0 - serial_fraction) / num_workers)
    max_speedup = 1.0 / serial_fraction
    elbow_n     = 1
    prev        = 1.0
    for n in range(2, 1001):
        s = 1.0 / (serial_fraction + (1.0 - serial_fraction) / n)
        if (s - prev) / max_speedup < 0.01:
            elbow_n = n - 1
            break
        prev = s
    efficiency = speedup_n / num_workers
    return {
        "model": "Amdahl: S(N) = 1 / (s + (1-s)/N)",
        "serial_fraction": serial_fraction, "num_workers": num_workers,
        "amdahl_speedup": round(speedup_n, 3),
        "theoretical_max_speedup": round(max_speedup, 2),
        "efficiency_score": round(efficiency, 3),
        "diminishing_returns_elbow": elbow_n,
        "workers_past_elbow": max(0, num_workers - elbow_n),
        "recommendation": (
            f"Reduce to {elbow_n} workers (99% of achievable speedup)"
            if num_workers > elbow_n * 1.3
            else f"Worker count near-optimal (elbow={elbow_n})"
        ),
        "over_provisioned": num_workers > elbow_n * 1.5,
    }


def _exponential_growth(initial_gb: float, daily_rate: float,
                        days: int, cost_per_gb_month: float = 0.023) -> Dict:
    if initial_gb <= 0 or days <= 0:
        return {"error": "initial_gb and days must be positive"}
    projected  = initial_gb * math.exp(daily_rate * days)
    dbl        = math.log(2) / daily_rate if daily_rate > 0 else float("inf")
    curr_cost  = initial_gb  * cost_per_gb_month
    fcst_cost  = projected   * cost_per_gb_month
    milestones = []
    for x in [2, 5, 10]:
        if daily_rate > 0:
            milestones.append({"multiplier": x,
                                "days": round(math.log(x) / daily_rate),
                                "projected_gb": round(initial_gb * x, 1)})
    return {
        "model": "Euler: N(t) = N₀·e^(r·t)",
        "initial_gb": round(initial_gb, 3), "daily_rate": round(daily_rate, 6),
        "forecast_days": days, "projected_gb": round(projected, 2),
        "growth_multiplier": round(projected / initial_gb, 2),
        "doubling_time_days": round(dbl, 1) if math.isfinite(dbl) else None,
        "current_monthly_storage_usd": round(curr_cost, 2),
        "forecast_monthly_storage_usd": round(fcst_cost, 2),
        "additional_monthly_cost_usd": round(fcst_cost - curr_cost, 2),
        "growth_milestones": milestones,
    }


def _zipf_skew_model(partition_sizes: List[float]) -> Dict:
    if len(partition_sizes) < 2:
        return {"error": "Need at least 2 partition sizes"}
    sizes = sorted([s for s in partition_sizes if s > 0], reverse=True)
    n = len(sizes)
    if n == 0:
        return {"error": "All partition sizes are zero"}
    xmin     = sizes[-1]
    log_rats = [math.log(s / xmin) for s in sizes if s > xmin]
    if not log_rats:
        return {"zipf_alpha": None, "skew_severity": "none",
                "recommended_salt": 1,
                "interpretation": "All partitions identical — no skew"}
    alpha       = len(log_rats) / sum(log_rats)
    median_size = sizes[n // 2]
    max_size    = sizes[0]
    skew_ratio  = max_size / median_size if median_size > 0 else 1.0
    salt        = max(1, math.ceil(math.sqrt(skew_ratio)))
    severity    = ("EXTREME" if alpha < 0.5 else "HIGH" if alpha < 1.0
                   else "MODERATE" if alpha < 2.0 else "MILD")
    return {
        "model": "Zipf: f(k) ∝ k^(-α) — Hill estimator",
        "zipf_alpha": round(alpha, 4), "skew_severity": severity,
        "skew_ratio_max_median": round(skew_ratio, 1),
        "recommended_salt_factor": salt,
        "spark_salting_snippet": (
            f"from pyspark.sql.functions import floor, rand\n"
            f"SALT = {salt}\n"
            f"df = df.withColumn('_salt', (floor(rand()*SALT)).cast('int'))"
        ),
    }


def _shannon_entropy(value_counts: Dict[str, int]) -> Dict:
    total = sum(value_counts.values())
    if total == 0:
        return {"error": "Empty value_counts"}
    n_distinct = len(value_counts)
    H = -sum((c / total) * math.log2(c / total) for c in value_counts.values() if c > 0)
    Hmax = math.log2(n_distinct) if n_distinct > 1 else 0.0
    norm = H / Hmax if Hmax > 0 else 0.0
    if   norm < 0.20: codec, ratio = "dictionary+gzip",   10.0
    elif norm < 0.40: codec, ratio = "dictionary+snappy",  6.0
    elif norm < 0.60: codec, ratio = "snappy",             3.0
    elif norm < 0.80: codec, ratio = "zstd",               2.0
    else:             codec, ratio = "zstd (marginal)",    1.3
    return {
        "model": "Shannon: H = -Σ p(x)·log₂(p(x))",
        "entropy_bits": round(H, 4), "max_entropy_bits": round(Hmax, 4),
        "normalized_entropy": round(norm, 4), "distinct_values": n_distinct,
        "recommended_codec": codec, "estimated_compression_ratio": ratio,
        "parquet_encoding": "DICTIONARY" if norm < 0.50 else "PLAIN_DICTIONARY",
    }


def _gaussian_file_distribution(file_sizes_mb: List[float]) -> Dict:
    sizes = [s for s in file_sizes_mb if s > 0]
    n = len(sizes)
    if n < 3:
        return {"error": "Need at least 3 positive file sizes"}
    mu, std = statistics.mean(sizes), statistics.stdev(sizes)
    if std == 0:
        return {"mean_mb": round(mu, 2), "std_mb": 0, "is_bimodal": False,
                "interpretation": "All files identical size"}
    skew   = sum(((x - mu) / std) ** 3 for x in sizes) / n
    kurt4  = sum(((x - mu) / std) ** 4 for x in sizes) / n
    bc     = (skew ** 2 + 1) / (kurt4 + (3 * (n - 1) ** 2) / max(1, (n - 2) * (n - 3)))
    bimod  = bc > 0.555
    cv     = std / mu
    tiny   = sum(1 for x in sizes if x  <  10) / n
    small  = sum(1 for x in sizes if 10 <= x < 128) / n
    ideal  = sum(1 for x in sizes if 128 <= x <= 512) / n
    large  = sum(1 for x in sizes if x  > 512) / n
    return {
        "model": "Gaussian + BC = (γ₁²+1)/κ  (Pfister 2013)",
        "n_files": n, "mean_mb": round(mu, 2), "std_mb": round(std, 2),
        "cv": round(cv, 3), "skewness": round(skew, 3),
        "bimodality_coefficient": round(bc, 4), "is_bimodal": bimod,
        "size_bands": {"tiny_pct": round(tiny*100,1), "small_pct": round(small*100,1),
                       "ideal_pct": round(ideal*100,1), "large_pct": round(large*100,1)},
        "interpretation": (
            f"BIMODAL (BC={bc:.3f}>0.555): {tiny*100:.0f}% tiny + {large*100:.0f}% large. "
            "Run OPTIMIZE bin-pack." if bimod
            else f"Unimodal (BC={bc:.3f}≤0.555). Mean={mu:.1f} MB, σ={std:.1f} MB."
        ),
        "action": ("OPTIMIZE REWRITE DATA USING bin-pack WHERE file_size_in_bytes < 134217728"
                   if bimod else "No compaction needed"),
    }


def _littles_law(avg_task_sec: float, num_executors: int,
                 total_tasks: Optional[int] = None,
                 target_min: Optional[float] = None) -> Dict:
    if avg_task_sec <= 0 or num_executors < 1:
        return {"error": "avg_task_sec must be >0 and num_executors >=1"}
    lam   = num_executors / avg_task_sec
    opt_p = max(200, int(num_executors * 3))
    result: Dict[str, Any] = {
        "model": "Little's Law: L = λ·W",
        "avg_task_sec": avg_task_sec, "num_executors": num_executors,
        "task_throughput_per_sec": round(lam, 3),
        "optimal_shuffle_partitions": opt_p,
        "optimal_parallelism": num_executors * 2,
        "spark_config": {"spark.sql.shuffle.partitions": str(opt_p),
                         "spark.default.parallelism":    str(num_executors * 2)},
    }
    if total_tasks and target_min:
        req = math.ceil((total_tasks / (target_min * 60)) * avg_task_sec)
        result["executors_needed_for_target"] = req
        result["scale_factor"] = round(req / num_executors, 2)
    return result


def _shewhart_control_chart(history: List[float], current_value: float,
                            label: str = "cost_usd") -> Dict:
    if len(history) < 5:
        return {"error": "Need at least 5 historical data points"}
    mu, sigma = statistics.mean(history), statistics.stdev(history)
    z         = (current_value - mu) / sigma if sigma > 0 else 0.0
    zone      = ("Zone A (>3σ — CRITICAL)" if abs(z) > 3 else
                 "Zone B (>2σ — WARNING)"  if abs(z) > 2 else
                 "Zone C (>1σ — WATCH)"    if abs(z) > 1 else "In Control")
    we: List[str] = []
    if len(history) >= 8:
        above2 = [(v - mu) / sigma > 2 for v in history[-3:] + [current_value]]
        if sum(above2[-3:]) >= 2:
            we.append("2-of-3 points beyond 2σ (process drift)")
    return {
        "model": "Shewhart: UCL/LCL = μ ± 3σ",
        "label": label, "mean": round(mu, 4), "std": round(sigma, 4),
        "ucl_3sigma": round(mu + 3 * sigma, 4),
        "lcl_3sigma": round(max(0, mu - 3 * sigma), 4),
        "current_value": round(current_value, 4),
        "z_score": round(z, 3), "zone": zone,
        "is_anomaly": abs(z) > 3,
        "western_electric_signals": we,
        "pct_above_mean": round((current_value / mu - 1) * 100, 1) if mu > 0 else 0,
    }


def _fourier_periodicity(series: List[float], interval_min: int = 5) -> Dict:
    n = len(series)
    if n < 8:
        return {"error": "Need at least 8 data points"}
    mu  = sum(series) / n
    x   = [v - mu for v in series]
    amps: List[tuple] = []
    for k in range(1, n // 2 + 1):
        re = sum(x[t] * math.cos(2 * math.pi * k * t / n) for t in range(n))
        im = sum(-x[t] * math.sin(2 * math.pi * k * t / n) for t in range(n))
        amp = math.sqrt(re**2 + im**2) / n
        period_min = (n / k) * interval_min
        amps.append((k, round(amp, 4), round(period_min, 0)))
    amps.sort(key=lambda a: -a[1])
    top = amps[:3]
    dom_hr  = top[0][2] / 60
    dom_day = top[0][2] / 1440
    label   = ("~daily"  if 20 <= dom_hr <= 28 else
               "~weekly" if 6  <= dom_day <= 8 else
               f"{dom_hr:.1f}-hour cycle")
    return {
        "model": "DFT: X[k] = Σ x[n]·e^(−j·2π·kn/N)",
        "n_samples": n, "sample_interval_min": interval_min,
        "dominant_period_min": top[0][2], "dominant_period_hr": round(dom_hr, 2),
        "dominant_amplitude": top[0][1], "dominant_label": label,
        "top_frequencies": [{"rank": i+1, "amplitude": t[1],
                              "period_min": t[2], "period_hr": round(t[2]/60, 2)}
                             for i, t in enumerate(top)],
        "interpretation": (
            f"Dominant: {label} (period={dom_hr:.1f}h, amp={top[0][1]:.4f}). "
            "Schedule OPTIMIZE at the trough of this cycle."
            if top[0][1] > 0.1 else "No strong periodic pattern."
        ),
    }


def _bloom_filter_savings(table_gb: float, selectivity: float,
                          n_distinct: int, bits_per_key: int = 10) -> Dict:
    if not (0 < selectivity <= 1) or n_distinct < 1:
        return {"error": "invalid selectivity or n_distinct"}
    k_opt  = bits_per_key * math.log(2)
    inner  = 1 - math.exp(-k_opt * n_distinct / (bits_per_key * n_distinct))
    fpp    = inner ** k_opt
    filter_mb = (bits_per_key * n_distinct) / 8 / (1024**2)
    scanned = selectivity + (1 - selectivity) * fpp
    io_saved_pct = 1.0 - scanned
    return {
        "model": "Bloom: P(FP) = (1−e^(−kn/m))^k, k_opt=(m/n)·ln2",
        "false_positive_rate": round(fpp, 6),
        "filter_size_mb": round(filter_mb, 3),
        "io_saved_pct": round(io_saved_pct * 100, 2),
        "io_saved_gb": round(table_gb * io_saved_pct, 3),
        "worth_enabling": io_saved_pct > 0.10,
        "iceberg_ddl": (
            "ALTER TABLE <t> SET TBLPROPERTIES "
            "('write.parquet.bloom-filter-enabled.column.<col>'='true');"
        ),
    }


def _spot_risk(job_hours: float, hourly_rate: float = 0.05,
               checkpoint_hours: float = 0.0) -> Dict:
    if not (0 < hourly_rate < 1) or job_hours <= 0:
        return {"error": "invalid parameters"}
    p_survive   = (1 - hourly_rate) ** job_hours
    p_interrupt = 1.0 - p_survive
    mtti        = 1.0 / hourly_rate
    rework      = job_hours if checkpoint_hours == 0 else checkpoint_hours
    expected_rw = p_interrupt * rework
    spot_factor = 0.30   # pay 30% of on-demand
    eff_factor  = spot_factor + p_interrupt * rework / job_hours
    return {
        "model": "Geometric: P(survive) = (1−p)^t",
        "job_hours": job_hours, "hourly_interruption_rate": hourly_rate,
        "p_survive_full_job": round(p_survive, 4),
        "p_interrupted": round(p_interrupt, 4),
        "mean_time_to_interrupt_hr": round(mtti, 1),
        "expected_rework_hours": round(expected_rw, 3),
        "effective_spot_cost_factor": round(eff_factor, 3),
        "net_savings_pct": round((1 - eff_factor) * 100, 1),
        "recommendation": (
            f"Spot viable — net ~{(1-eff_factor)*100:.0f}% savings. Add checkpointing every "
            f"{checkpoint_hours or 0.5}h."
            if eff_factor < 0.8 else "Spot risk too high — use on-demand or add checkpointing."
        ),
    }


def _pareto_rank(recommendations: List[Dict]) -> Dict:
    if not recommendations:
        return {"error": "empty recommendations list"}
    effort_map = {"low": 2, "medium": 8, "high": 24, "very_high": 80}
    scored = []
    for r in recommendations:
        sav  = float(r.get("estimated_savings_percent", 0))
        hrs  = max(0.5, float(effort_map.get(str(r.get("effort","medium")).lower(), 8)))
        score = sav / math.sqrt(hrs)
        scored.append({**r, "_pareto_score": round(score, 3), "_effort_hours": hrs})
    scored.sort(key=lambda x: -x["_pareto_score"])
    total   = sum(r.get("estimated_savings_percent", 0) for r in scored)
    cumul   = 0.0
    cutoff  = len(scored)
    for i, r in enumerate(scored):
        cumul += r.get("estimated_savings_percent", 0)
        if cumul >= 0.80 * total:
            cutoff = i + 1
            break
    return {
        "model": "Pareto 80/20: score = savings_pct / √effort_hours",
        "total_recommendations": len(scored),
        "pareto_count": cutoff,
        "pareto_savings_pct": round(cumul, 1),
        "ranked_recommendations": scored,
        "pareto_front": scored[:cutoff],
        "interpretation": (
            f"Top {cutoff}/{len(scored)} recs deliver {cumul:.0f}% of savings. Focus there first."
        ),
    }


# ── 3 New algorithms ──────────────────────────────────────────────────────────

def _planck_partition_temperature(partition_sizes: List[float],
                                  temperature_scale: float = 1.0) -> Dict:
    """
    Apply Planck's blackbody radiation analogy to partition sizes.

    In Planck's law B(ν,T) ∝ ν³ / (e^(hν/kT) − 1), each frequency bin maps
    to a partition-size bucket.  'Hot' partitions (high relative energy) are
    likely stragglers.  'Cold' partitions waste executor slots.

    partition_sizes: record counts or byte sizes per partition.
    temperature_scale: normalisation factor; set to mean partition size for
                       auto-calibration (default 1.0 → raw values).
    """
    if len(partition_sizes) < 2:
        return {"error": "Need at least 2 partition sizes"}
    sizes = [max(0.001, s) for s in partition_sizes]
    T     = temperature_scale if temperature_scale > 0 else statistics.mean(sizes)
    # Planck energy per partition i: E_i ∝ size_i³ / (exp(size_i / T) − 1)
    energies = []
    for s in sizes:
        nu    = s / T
        denom = math.exp(min(nu, 700)) - 1  # clamp to avoid overflow
        if denom <= 0:
            denom = 1e-9
        energies.append((s ** 3) / denom)

    total_energy   = sum(energies)
    norm_energies  = [e / total_energy for e in energies] if total_energy > 0 else [0]*len(sizes)

    # Hot = top 10% energy; cold = bottom 10%
    sorted_idx     = sorted(range(len(sizes)), key=lambda i: -norm_energies[i])
    hot_threshold  = 0.10
    cold_threshold = 0.10
    hot_count      = max(1, int(len(sizes) * hot_threshold))
    cold_count     = max(1, int(len(sizes) * cold_threshold))
    hot_indices    = sorted_idx[:hot_count]
    cold_indices   = sorted_idx[-cold_count:]

    peak_idx    = sorted_idx[0]
    peak_size   = sizes[peak_idx]
    median_size = sorted(sizes)[len(sizes) // 2]
    temp_ratio  = peak_size / median_size if median_size > 0 else 1.0

    return {
        "model": "Planck blackbody analogy: E ∝ ν³/(e^(ν/T)−1)",
        "temperature_scale": T,
        "partition_count":   len(sizes),
        "peak_partition_size":    round(peak_size, 2),
        "median_partition_size":  round(median_size, 2),
        "temperature_ratio":      round(temp_ratio, 2),
        "hot_partition_indices":  hot_indices,
        "cold_partition_indices": cold_indices,
        "hot_partition_sizes":    [round(sizes[i], 2) for i in hot_indices],
        "cold_partition_sizes":   [round(sizes[i], 2) for i in cold_indices],
        "normalized_energies":    [round(e, 6) for e in norm_energies],
        "interpretation": (
            f"Planck temperature ratio {temp_ratio:.1f}×. "
            f"{hot_count} 'hot' partition(s) contain disproportionate energy — straggler risk. "
            f"Salt hot key or enable AQE skewJoin. "
            f"{cold_count} 'cold' partition(s) are near-empty — coalesce to reduce task overhead."
        ),
        "recommendation": (
            "Apply key salting + AQE skewJoin (skewedPartitionFactor=5)"
            if temp_ratio > 5 else
            "Partition distribution acceptable; monitor periodically"
        ),
    }


def _newtons_cooling_worker_decay(
    initial_utilisation: float,
    ambient_utilisation: float,
    cooling_constant_k: float,
    time_steps: int,
    step_minutes: int = 5,
) -> Dict:
    """
    Apply Newton's Law of Cooling to model worker utilisation decay.

    Newton's Cooling: U(t) = U_ambient + (U₀ − U_ambient)·e^(−k·t)

    After a peak load phase, worker utilisation decays exponentially toward an
    ambient (idle) level.  This identifies the optimal scale-in time — the
    point where adding more workers yields no throughput benefit.

    initial_utilisation:  peak worker utilisation fraction (0–1).
    ambient_utilisation:  steady-state idle fraction (0–1).
    cooling_constant_k:   decay rate per time step (empirical; typical 0.1–0.5).
    time_steps:           number of steps to forecast.
    step_minutes:         minutes per step (default 5 — CloudWatch granularity).
    """
    if not (0 <= initial_utilisation <= 1 and 0 <= ambient_utilisation <= 1):
        return {"error": "utilisation values must be in [0, 1]"}
    if cooling_constant_k <= 0:
        return {"error": "cooling_constant_k must be positive"}

    delta0   = initial_utilisation - ambient_utilisation
    series   = []
    scaleins = []
    for t in range(time_steps + 1):
        u = ambient_utilisation + delta0 * math.exp(-cooling_constant_k * t)
        series.append({"t_min": t * step_minutes, "utilisation": round(u, 4)})
        if u < 0.50 and not scaleins:
            scaleins.append({"t_min": t * step_minutes, "utilisation": round(u, 4),
                             "action": "Scale in: utilisation < 50%"})

    half_life_steps  = math.log(2) / cooling_constant_k
    half_life_min    = half_life_steps * step_minutes
    target_util      = ambient_utilisation + delta0 * 0.10
    t_10pct          = math.log(10) / cooling_constant_k if delta0 > 0 else 0
    t_10pct_min      = t_10pct * step_minutes

    return {
        "model": "Newton's Cooling: U(t) = U_amb + (U₀−U_amb)·e^(−k·t)",
        "initial_utilisation":  initial_utilisation,
        "ambient_utilisation":  ambient_utilisation,
        "cooling_constant_k":   cooling_constant_k,
        "half_life_minutes":    round(half_life_min, 1),
        "time_to_10pct_delta_minutes": round(t_10pct_min, 1),
        "utilisation_series":   series,
        "recommended_scale_in_events": scaleins,
        "interpretation": (
            f"Utilisation decays from {initial_utilisation:.0%} → {ambient_utilisation:.0%} "
            f"with half-life {half_life_min:.0f} min. "
            f"{'Scale in at t=' + str(scaleins[0]['t_min']) + ' min when utilisation < 50%.' if scaleins else 'Utilisation stays high — no scale-in opportunity detected.'}"
        ),
        "glue_flex_recommendation": (
            "Consider Glue Flex execution: peak + idle pattern suits flex warm-up window"
            if half_life_min < 30 else
            "Sustained utilisation — standard execution more predictable than Flex"
        ),
    }


def _monte_carlo_cost_simulation(
    base_cost_usd: float,
    std_dev_pct: float,
    simulations: int = 10_000,
    seed: int = 42,
) -> Dict:
    """
    Monte Carlo simulation for job cost uncertainty bounds.

    Samples job cost from a log-normal distribution (log-normal because costs
    are strictly positive and right-skewed in practice) to produce p5, p50,
    p95, p99 confidence intervals.

    base_cost_usd: expected cost per run (mean of log-normal).
    std_dev_pct:   coefficient of variation as a fraction (e.g. 0.20 = 20%).
    simulations:   number of Monte Carlo draws (default 10,000).
    seed:          random seed for reproducibility.
    """
    if base_cost_usd <= 0:
        return {"error": "base_cost_usd must be positive"}
    if not (0 < std_dev_pct < 5):
        return {"error": "std_dev_pct must be in (0, 5)"}

    random.seed(seed)
    # Log-normal parameters: μ_ln, σ_ln from mean and CV
    cv     = std_dev_pct
    mu_ln  = math.log(base_cost_usd) - 0.5 * math.log(1 + cv**2)
    sig_ln = math.sqrt(math.log(1 + cv**2))

    samples = [math.exp(random.gauss(mu_ln, sig_ln)) for _ in range(simulations)]
    samples.sort()

    def pct(p: float) -> float:
        idx = int(p / 100 * simulations)
        return round(samples[min(idx, simulations - 1)], 4)

    mean_sim = sum(samples) / simulations
    annual_p50  = pct(50)  * 365
    annual_p95  = pct(95)  * 365
    annual_p99  = pct(99)  * 365

    return {
        "model": "Monte Carlo log-normal cost simulation",
        "base_cost_usd": base_cost_usd,
        "coefficient_of_variation": std_dev_pct,
        "simulations": simulations,
        "percentiles": {
            "p5":  pct(5),  "p25": pct(25), "p50": pct(50),
            "p75": pct(75), "p95": pct(95), "p99": pct(99),
        },
        "simulated_mean_usd": round(mean_sim, 4),
        "annual_cost_p50_usd": round(annual_p50, 2),
        "annual_cost_p95_usd": round(annual_p95, 2),
        "annual_cost_p99_usd": round(annual_p99, 2),
        "cost_volatility_band_usd": round(pct(95) - pct(5), 4),
        "interpretation": (
            f"At {std_dev_pct*100:.0f}% cost variability: "
            f"50th pct=${pct(50):.2f}, 95th pct=${pct(95):.2f} per run. "
            f"Annual budget at p95: ${annual_p95:,.0f}. "
            f"{'High volatility — consider reserved capacity.' if std_dev_pct > 0.40 else 'Cost is stable — on-demand is fine.'}"
        ),
    }


# ── Strands @tool wrappers ────────────────────────────────────────────────────

@tool
def run_amdahls_law(serial_fraction: float, num_workers: int) -> str:
    """
    Apply Amdahl's Law to compute speedup ceiling and optimal worker count.

    Args:
        serial_fraction: Fraction of code that is inherently serial (0–1).
                         Estimate from driver-only ops (collect, broadcast setup).
        num_workers:     Current or proposed number of workers.

    Returns:
        JSON with amdahl_speedup, efficiency_score, diminishing_returns_elbow,
        over_provisioned flag, and recommendation.
    """
    try:
        return json.dumps(_amdahls_law(serial_fraction, num_workers))
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def run_euler_growth_forecast(
    initial_gb: float,
    daily_growth_rate: float,
    forecast_days: int,
    storage_cost_per_gb_month: float = 0.023,
) -> str:
    """
    Forecast table size and storage cost using Euler's exponential growth model.

    Args:
        initial_gb:                Current table size in GB.
        daily_growth_rate:         Continuous growth rate per day (e.g. 0.02 = 2%/day).
                                   Compute as: ln(current_gb / old_gb) / days_elapsed.
        forecast_days:             Horizon in days (e.g. 90, 180, 365).
        storage_cost_per_gb_month: S3 storage cost per GB/month (default $0.023).

    Returns:
        JSON with projected_gb, doubling_time_days, additional_monthly_cost_usd,
        growth_milestones.
    """
    try:
        return json.dumps(_exponential_growth(initial_gb, daily_growth_rate,
                                              forecast_days, storage_cost_per_gb_month))
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def run_zipf_skew_model(partition_sizes_json: str) -> str:
    """
    Fit a Zipf/power-law distribution to partition sizes to quantify skew.

    Args:
        partition_sizes_json: JSON list of partition record counts or byte sizes.

    Returns:
        JSON with zipf_alpha, skew_severity, skew_ratio, recommended_salt_factor,
        and a ready-to-use PySpark salting code snippet.
    """
    try:
        sizes = json.loads(partition_sizes_json)
        return json.dumps(_zipf_skew_model(sizes))
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def run_shannon_entropy(value_counts_json: str) -> str:
    """
    Compute Shannon entropy to determine column compressibility and codec choice.

    Args:
        value_counts_json: JSON object mapping value → count
                           e.g. '{"US": 5000, "UK": 2000, "DE": 800}'.

    Returns:
        JSON with entropy_bits, normalized_entropy, recommended_codec,
        estimated_compression_ratio, parquet_encoding.
    """
    try:
        vc = json.loads(value_counts_json)
        return json.dumps(_shannon_entropy(vc))
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def run_gaussian_file_distribution(file_sizes_mb_json: str) -> str:
    """
    Apply the Gaussian bimodality coefficient to detect tiny-file pathologies.

    Args:
        file_sizes_mb_json: JSON list of file sizes in MB.

    Returns:
        JSON with bimodality_coefficient, is_bimodal, size_bands (tiny/small/ideal/large
        percentages), interpretation, and recommended OPTIMIZE action.
    """
    try:
        sizes = json.loads(file_sizes_mb_json)
        return json.dumps(_gaussian_file_distribution(sizes))
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def run_littles_law(
    avg_task_sec: float,
    num_executors: int,
    total_tasks: int = 0,
    target_duration_min: float = 0.0,
) -> str:
    """
    Apply Little's Law (queueing theory) to derive optimal shuffle.partitions.

    Args:
        avg_task_sec:         Average Spark task duration in seconds.
        num_executors:        Current executor count.
        total_tasks:          Total tasks in the job (optional; used with target).
        target_duration_min:  Desired total job duration in minutes (optional).

    Returns:
        JSON with optimal_shuffle_partitions, optimal_parallelism, Spark config
        recommendations, and (if total_tasks + target given) executors_needed.
    """
    try:
        tt = total_tasks if total_tasks > 0 else None
        td = target_duration_min if target_duration_min > 0 else None
        return json.dumps(_littles_law(avg_task_sec, num_executors, tt, td))
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def run_shewhart_control_chart(
    history_json: str,
    current_value: float,
    label: str = "cost_usd",
) -> str:
    """
    Run a Shewhart 3-sigma control chart to detect job cost or duration anomalies.

    Args:
        history_json:   JSON list of historical observations (minimum 5).
        current_value:  The new observation to evaluate.
        label:          Metric name (e.g. "cost_usd", "duration_min").

    Returns:
        JSON with zone (In Control / Zone A/B/C), z_score, is_anomaly,
        UCL/LCL bounds, and Western Electric rule signals.
    """
    try:
        hist = json.loads(history_json)
        return json.dumps(_shewhart_control_chart(hist, current_value, label))
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def run_fourier_periodicity(
    time_series_json: str,
    sample_interval_minutes: int = 5,
) -> str:
    """
    Run a Discrete Fourier Transform to identify dominant cycles in a CloudWatch metric.

    Args:
        time_series_json:        JSON list of metric values in chronological order.
        sample_interval_minutes: Interval between samples in minutes (default 5).

    Returns:
        JSON with dominant_period_hr, dominant_label (daily/weekly/hourly), amplitude,
        and top 3 frequency components.
    """
    try:
        series = json.loads(time_series_json)
        return json.dumps(_fourier_periodicity(series, sample_interval_minutes))
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def run_bloom_filter_savings(
    table_size_gb: float,
    join_selectivity: float,
    num_distinct_keys: int,
    bits_per_key: int = 10,
) -> str:
    """
    Estimate Bloom filter false-positive rate and I/O savings for a join.

    Args:
        table_size_gb:      Size of the probed (larger) table in GB.
        join_selectivity:   Fraction of rows that match (0–1). Low = fewer matches.
        num_distinct_keys:  Distinct join-key values in the build (smaller) table.
        bits_per_key:       Bits per key in the filter (default 10 → FPP ≈ 0.8%).

    Returns:
        JSON with false_positive_rate, filter_size_mb, io_saved_pct, io_saved_gb,
        worth_enabling flag, and Iceberg DDL to enable it.
    """
    try:
        return json.dumps(_bloom_filter_savings(table_size_gb, join_selectivity,
                                                num_distinct_keys, bits_per_key))
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def run_spot_interruption_risk(
    job_duration_hours: float,
    hourly_interruption_rate: float = 0.05,
    checkpoint_interval_hours: float = 0.0,
) -> str:
    """
    Model AWS Spot interruption risk using the geometric distribution.

    Args:
        job_duration_hours:        Expected job duration in hours.
        hourly_interruption_rate:  Probability of interruption per hour (default 5%).
                                   Typical AWS spot: 2–10% depending on instance type.
        checkpoint_interval_hours: Checkpointing interval (0 = no checkpoint).

    Returns:
        JSON with p_survive_full_job, p_interrupted, expected_rework_hours,
        effective_spot_cost_factor, net_savings_pct, and recommendation.
    """
    try:
        return json.dumps(_spot_risk(job_duration_hours, hourly_interruption_rate,
                                     checkpoint_interval_hours))
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def run_pareto_rank_recommendations(recommendations_json: str) -> str:
    """
    Rank recommendations by Pareto 80/20 impact-per-effort score.

    Score = estimated_savings_percent / √effort_hours.
    Identifies the smallest set of recommendations that delivers 80% of savings.

    Args:
        recommendations_json: JSON list of recommendation objects, each with
                              estimated_savings_percent (int) and effort
                              ("low"|"medium"|"high"|"very_high").

    Returns:
        JSON with ranked_recommendations, pareto_front (the critical 20%),
        pareto_count, and interpretation.
    """
    try:
        recs = json.loads(recommendations_json)
        return json.dumps(_pareto_rank(recs))
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def run_planck_partition_temperature(
    partition_sizes_json: str,
    temperature_scale: float = 0.0,
) -> str:
    """
    Apply the Planck blackbody radiation analogy to identify 'hot' (straggler)
    and 'cold' (underutilised) partitions.

    Args:
        partition_sizes_json: JSON list of partition sizes (record counts or bytes).
        temperature_scale:    Normalisation scale T; 0 = auto-set to mean size.

    Returns:
        JSON with hot_partition_indices, cold_partition_indices, temperature_ratio,
        normalized_energies, and recommendation for salting or coalescing.
    """
    try:
        sizes = json.loads(partition_sizes_json)
        T     = temperature_scale if temperature_scale > 0 else (
            statistics.mean([s for s in sizes if s > 0]) if sizes else 1.0
        )
        return json.dumps(_planck_partition_temperature(sizes, T))
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def run_newtons_cooling_worker_decay(
    initial_utilisation: float,
    ambient_utilisation: float,
    cooling_constant_k: float = 0.15,
    time_steps: int = 24,
    step_minutes: int = 5,
) -> str:
    """
    Model worker utilisation decay after peak load using Newton's Law of Cooling.

    Identifies the optimal scale-in moment and whether Glue Flex execution
    is appropriate for the workload shape.

    Args:
        initial_utilisation:  Peak worker utilisation fraction (0–1).
        ambient_utilisation:  Steady-state idle fraction (0–1).
        cooling_constant_k:   Decay rate per time step (typical 0.1–0.5).
        time_steps:           Number of forecast steps (default 24 = 2 hours @ 5 min).
        step_minutes:         Minutes per step (default 5 — CloudWatch granularity).

    Returns:
        JSON with half_life_minutes, utilisation_series, recommended_scale_in_events,
        and glue_flex_recommendation.
    """
    try:
        return json.dumps(_newtons_cooling_worker_decay(
            initial_utilisation, ambient_utilisation,
            cooling_constant_k, time_steps, step_minutes))
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def run_monte_carlo_cost_simulation(
    base_cost_usd: float,
    std_dev_pct: float = 0.20,
    simulations: int = 10000,
) -> str:
    """
    Run a Monte Carlo simulation to bound job cost uncertainty.

    Samples from a log-normal distribution (right-skewed, strictly positive)
    to produce p5–p99 confidence intervals and annual budget estimates.

    Args:
        base_cost_usd: Expected cost per run in USD.
        std_dev_pct:   Coefficient of variation as a fraction (e.g. 0.20 = 20%).
        simulations:   Number of Monte Carlo draws (default 10,000).

    Returns:
        JSON with percentiles (p5/p25/p50/p75/p95/p99), annual_cost_p95_usd,
        cost_volatility_band_usd, and interpretation.
    """
    try:
        return json.dumps(_monte_carlo_cost_simulation(base_cost_usd, std_dev_pct,
                                                       simulations))
    except Exception as exc:
        return json.dumps({"error": str(exc)})


def create_scientific_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                            region: str = "us-west-2") -> Agent:
    """Return a Strands Agent with all 14 scientific algorithm tools."""
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(model=model, system_prompt=SYSTEM_PROMPT, tools=[
        run_amdahls_law,
        run_euler_growth_forecast,
        run_zipf_skew_model,
        run_shannon_entropy,
        run_gaussian_file_distribution,
        run_littles_law,
        run_shewhart_control_chart,
        run_fourier_periodicity,
        run_bloom_filter_savings,
        run_spot_interruption_risk,
        run_pareto_rank_recommendations,
        run_planck_partition_temperature,
        run_newtons_cooling_worker_decay,
        run_monte_carlo_cost_simulation,
    ])
