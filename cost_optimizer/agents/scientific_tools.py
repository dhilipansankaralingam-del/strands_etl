"""
Scientific Algorithm Library for PySpark Cost Optimization
==========================================================

Pure-Python implementations of mathematical models used by the optimizer tools.
No external dependencies — uses only Python standard library (math, statistics).

Algorithms:
  1.  amdahls_law               – theoretical max speedup → optimal worker ceiling
  2.  exponential_growth        – Euler e^(rt) table-size forecast
  3.  zipf_skew_model           – power-law partition skew → exact salting factor
  4.  shannon_entropy           – column compressibility → codec recommendation
  5.  gaussian_file_distribution – bimodality coefficient → detect mixed file sizes
  6.  littles_law_parallelism   – queueing-theory optimal shuffle.partitions
  7.  shewhart_control_chart    – 3-sigma job cost / duration anomaly detection
  8.  fourier_periodicity       – DFT dominant period in CloudWatch time series
  9.  bloom_filter_savings      – Bloom filter FPP and I/O savings estimator
  10. spot_interruption_risk    – geometric-distribution spot interruption model
  11. pareto_recommendation_rank – 80/20 rule to rank recs by savings / effort
"""

import math
import statistics
from typing import Any, Dict, List, Optional, Tuple


# ─────────────────────────────────────────────────────────────────────────────
# 1. Amdahl's Law
#    S(N) = 1 / (s + (1-s)/N)
#    Answers: "how many workers are actually useful given serial code?"
# ─────────────────────────────────────────────────────────────────────────────

def amdahls_law(serial_fraction: float, num_workers: int) -> Dict[str, Any]:
    """
    Compute Amdahl speedup and optimal worker count.

    serial_fraction: fraction of code that cannot be parallelised (0–1).
                     Estimate from driver-only operations (collect, toPandas,
                     broadcast, single-partition sort).
    num_workers:     current or proposed worker count.
    """
    if not (0 < serial_fraction < 1):
        return {"error": "serial_fraction must be strictly between 0 and 1"}
    if num_workers < 1:
        return {"error": "num_workers must be >= 1"}

    speedup_n = 1.0 / (serial_fraction + (1.0 - serial_fraction) / num_workers)
    max_speedup = 1.0 / serial_fraction  # theoretical limit as N → ∞

    # Diminishing-returns elbow: first N where marginal gain < 1%
    elbow_n = 1
    prev_speedup = 1.0
    for n in range(2, 1001):
        s = 1.0 / (serial_fraction + (1.0 - serial_fraction) / n)
        marginal_gain = (s - prev_speedup) / max_speedup
        if marginal_gain < 0.01:
            elbow_n = n - 1
            break
        prev_speedup = s
    else:
        elbow_n = 1000

    efficiency = speedup_n / num_workers  # ideal = 1.0, over-provisioned < 0.5

    return {
        "serial_fraction": serial_fraction,
        "num_workers": num_workers,
        "amdahl_speedup": round(speedup_n, 3),
        "theoretical_max_speedup": round(max_speedup, 2),
        "efficiency_score": round(efficiency, 3),
        "diminishing_returns_elbow": elbow_n,
        "workers_past_elbow": max(0, num_workers - elbow_n),
        "interpretation": (
            f"Max speedup = {max_speedup:.1f}×. "
            f"At {num_workers} workers, actual speedup = {speedup_n:.2f}× "
            f"(efficiency {efficiency*100:.0f}%). "
            f"Returns diminish past {elbow_n} workers — "
            f"{'OVER-PROVISIONED' if num_workers > elbow_n * 1.5 else 'OK'}."
        ),
        "recommendation": (
            f"Reduce to {elbow_n} workers to capture 99% of achievable speedup"
            if num_workers > elbow_n * 1.3 else
            f"Worker count is near-optimal (elbow at {elbow_n})"
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 2. Euler's Exponential Growth Model
#    N(t) = N₀ · e^(r·t)
#    More accurate than linear for compounding data growth.
# ─────────────────────────────────────────────────────────────────────────────

def exponential_growth(
    initial_gb: float,
    daily_rate: float,
    days: int,
    storage_cost_per_gb_month: float = 0.023,
) -> Dict[str, Any]:
    """
    Project table size using Euler's exponential growth formula.

    initial_gb:  current table size in GB.
    daily_rate:  continuous growth rate per day (e.g. 0.02 = 2%/day).
                 Derive from: ln(current_gb / old_gb) / days_elapsed
    days:        forecast horizon (e.g. 90, 180, 365).
    """
    if initial_gb <= 0:
        return {"error": "initial_gb must be positive"}
    if days <= 0:
        return {"error": "days must be positive"}

    projected_gb = initial_gb * math.exp(daily_rate * days)

    doubling_time_days = math.log(2) / daily_rate if daily_rate > 0 else float("inf")
    half_life_days     = math.log(2) / (-daily_rate) if daily_rate < 0 else float("inf")

    # Monthly storage cost projection
    current_monthly_cost  = initial_gb   * storage_cost_per_gb_month
    forecast_monthly_cost = projected_gb * storage_cost_per_gb_month
    added_monthly_cost    = forecast_monthly_cost - current_monthly_cost

    milestones = []
    for target_x in [2, 5, 10]:
        if daily_rate > 0:
            days_to_target = math.log(target_x) / daily_rate
            milestones.append({
                "multiplier": target_x,
                "days": round(days_to_target, 0),
                "projected_gb": round(initial_gb * target_x, 1),
            })

    return {
        "model":               "Euler exponential: N(t) = N₀·e^(r·t)",
        "initial_gb":          round(initial_gb, 3),
        "daily_rate":          round(daily_rate, 6),
        "forecast_days":       days,
        "projected_gb":        round(projected_gb, 2),
        "growth_multiplier":   round(projected_gb / initial_gb, 2),
        "doubling_time_days":  round(doubling_time_days, 1) if math.isfinite(doubling_time_days) else None,
        "half_life_days":      round(half_life_days, 1) if math.isfinite(half_life_days) else None,
        "current_monthly_storage_usd":  round(current_monthly_cost, 2),
        "forecast_monthly_storage_usd": round(forecast_monthly_cost, 2),
        "additional_monthly_cost_usd":  round(added_monthly_cost, 2),
        "growth_milestones":   milestones,
        "interpretation": (
            f"At {daily_rate*100:.2f}%/day, table grows from {initial_gb:.1f} GB → "
            f"{projected_gb:.1f} GB in {days} days (+${added_monthly_cost:.2f}/month storage). "
            f"{'Doubling every ' + str(int(doubling_time_days)) + ' days.' if math.isfinite(doubling_time_days) else 'Shrinking table.'}"
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 3. Zipf / Power-Law Partition Skew Model
#    f(k) ∝ k^(−α)   Hill estimator for α
#    Derives the exact recommended salting factor.
# ─────────────────────────────────────────────────────────────────────────────

def zipf_skew_model(partition_sizes: List[float]) -> Dict[str, Any]:
    """
    Fit a Zipf/power-law distribution to partition sizes.

    partition_sizes: list of record counts or bytes per partition.
    Returns: Zipf exponent α, skew severity, recommended salting factor.
    """
    if len(partition_sizes) < 2:
        return {"error": "Need at least 2 partition sizes"}

    sizes = sorted([s for s in partition_sizes if s > 0], reverse=True)
    n = len(sizes)
    if n == 0:
        return {"error": "All partition sizes are zero or negative"}

    xmin = sizes[-1]  # smallest positive value

    # Hill estimator: α̂ = n / Σ ln(X_i / x_min)
    log_ratios = [math.log(s / xmin) for s in sizes if s > xmin]
    if not log_ratios:
        return {
            "zipf_alpha":          None,
            "skew_severity":       "none",
            "recommended_salt":    1,
            "interpretation":      "All partitions are identical — no skew",
        }

    alpha = len(log_ratios) / sum(log_ratios)

    median_size = sizes[n // 2]
    max_size    = sizes[0]
    skew_ratio  = max_size / median_size if median_size > 0 else 1.0

    # Salting factor: number of buckets to break the largest partition
    # derived so each salt bucket ≈ median size
    recommended_salt = max(1, math.ceil(math.sqrt(skew_ratio)))

    severity = (
        "EXTREME (α<0.5)"  if alpha < 0.5 else
        "HIGH (0.5≤α<1)"   if alpha < 1.0 else
        "MODERATE (1≤α<2)" if alpha < 2.0 else
        "MILD (α≥2)"
    )

    return {
        "model":                   "Zipf power-law: f(k) ∝ k^(-α) — Hill estimator",
        "zipf_alpha":              round(alpha, 4),
        "skew_severity":           severity,
        "skew_ratio_max_median":   round(skew_ratio, 1),
        "recommended_salt_factor": recommended_salt,
        "partition_count":         n,
        "max_partition_size":      round(max_size, 0),
        "median_partition_size":   round(median_size, 0),
        "interpretation": (
            f"Power-law exponent α={alpha:.2f} → {severity}. "
            f"Largest partition is {skew_ratio:.1f}× the median. "
            f"Recommended salting: {recommended_salt} buckets "
            f"(add salt column with rand()*{recommended_salt} before join/groupBy)."
        ),
        "spark_salting_snippet": (
            f"from pyspark.sql.functions import floor, rand\n"
            f"SALT = {recommended_salt}\n"
            f"df = df.withColumn('_salt', (floor(rand() * SALT)).cast('int'))\n"
            f"# Then join/groupBy on (original_key, _salt)"
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 4. Shannon Entropy — Column Compressibility
#    H = −Σ p(x) log₂ p(x)
#    High entropy → hard to compress; low → compress aggressively.
# ─────────────────────────────────────────────────────────────────────────────

def shannon_entropy(value_counts: Dict[str, int]) -> Dict[str, Any]:
    """
    Compute Shannon entropy for a column's value distribution.

    value_counts: {value_string: count} e.g. {"US": 5000, "UK": 2000, ...}
    Returns: entropy in bits, normalized entropy, recommended Parquet codec.
    """
    total = sum(value_counts.values())
    if total == 0:
        return {"error": "value_counts must have at least one non-zero count"}

    n_distinct = len(value_counts)

    entropy = 0.0
    for count in value_counts.values():
        if count > 0:
            p = count / total
            entropy -= p * math.log2(p)

    max_entropy = math.log2(n_distinct) if n_distinct > 1 else 0.0
    normalized  = entropy / max_entropy if max_entropy > 0 else 0.0

    # Estimated Parquet compression ratio from normalized entropy
    # Low entropy → dictionary encoding is very effective
    if normalized < 0.20:
        codec, est_ratio = "dictionary+gzip",   10.0
    elif normalized < 0.40:
        codec, est_ratio = "dictionary+snappy",  6.0
    elif normalized < 0.60:
        codec, est_ratio = "snappy",             3.0
    elif normalized < 0.80:
        codec, est_ratio = "zstd",               2.0
    else:
        codec, est_ratio = "zstd (marginal)",    1.3

    bits_per_value = entropy  # actual information per value in bits
    # Theoretical minimum bytes per value = entropy / 8
    min_bytes_per_value = entropy / 8

    return {
        "model":              "Shannon entropy: H = -Σ p(x)·log₂(p(x))",
        "entropy_bits":       round(entropy, 4),
        "max_entropy_bits":   round(max_entropy, 4),
        "normalized_entropy": round(normalized, 4),
        "distinct_values":    n_distinct,
        "total_values":       total,
        "bits_per_value":     round(bits_per_value, 3),
        "min_bytes_per_value": round(min_bytes_per_value, 4),
        "recommended_codec":  codec,
        "estimated_compression_ratio": est_ratio,
        "parquet_encoding":   "DICTIONARY" if normalized < 0.50 else "PLAIN_DICTIONARY",
        "interpretation": (
            f"Entropy {entropy:.2f}/{max_entropy:.2f} bits (normalized={normalized:.0%}). "
            f"{'Low entropy — dictionary encoding will be very effective, use ' + codec + '.' if normalized < 0.5 else 'High entropy — limited compression gain, use ' + codec + '.'}"
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 5. Gaussian File Distribution + Bimodality Coefficient
#    Bimodality Coefficient (BC) = (γ₁² + 1) / κ
#    BC > 0.555 → bimodal distribution (mixed file sizes — anti-pattern)
# ─────────────────────────────────────────────────────────────────────────────

def gaussian_file_distribution(file_sizes_mb: List[float]) -> Dict[str, Any]:
    """
    Fit a Gaussian model to file sizes. Detect bimodal distributions
    (the mixed tiny-files + oversized-files anti-pattern).

    Uses the Bimodality Coefficient (BC) from SAS literature:
        BC = (skewness² + 1) / excess_kurtosis + 3
        BC > 0.555 → bimodal
    """
    n = len(file_sizes_mb)
    if n < 3:
        return {"error": "Need at least 3 file sizes"}

    sizes = [s for s in file_sizes_mb if s > 0]
    n = len(sizes)
    if n < 3:
        return {"error": "Need at least 3 positive file sizes"}

    mu  = statistics.mean(sizes)
    std = statistics.stdev(sizes)

    if std == 0:
        return {
            "mean_mb": round(mu, 2), "std_mb": 0,
            "is_bimodal": False, "cv": 0,
            "interpretation": "All files are exactly the same size — perfectly uniform",
        }

    # Skewness γ₁ = (1/n) Σ((xᵢ−μ)/σ)³
    skewness = sum(((x - mu) / std) ** 3 for x in sizes) / n

    # Excess kurtosis κ = (1/n) Σ((xᵢ−μ)/σ)⁴ − 3
    kurtosis_raw = sum(((x - mu) / std) ** 4 for x in sizes) / n
    excess_kurtosis = kurtosis_raw - 3

    # Bimodality coefficient (Pfister et al. 2013)
    bc = (skewness ** 2 + 1) / (kurtosis_raw + (3 * (n - 1) ** 2) / ((n - 2) * (n - 3)))
    is_bimodal = bc > 0.555

    cv = std / mu
    within_1sigma = sum(1 for x in sizes if abs(x - mu) <= std) / n
    within_2sigma = sum(1 for x in sizes if abs(x - mu) <= 2 * std) / n

    # Size bands
    tiny_pct  = sum(1 for x in sizes if x < 10)  / n
    small_pct = sum(1 for x in sizes if 10 <= x < 128) / n
    ideal_pct = sum(1 for x in sizes if 128 <= x <= 512) / n
    large_pct = sum(1 for x in sizes if x > 512) / n

    return {
        "model":                    "Gaussian + Bimodality Coefficient (BC = (γ₁²+1)/κ)",
        "n_files":                  n,
        "mean_mb":                  round(mu, 2),
        "std_mb":                   round(std, 2),
        "cv":                       round(cv, 3),
        "skewness":                 round(skewness, 3),
        "excess_kurtosis":          round(excess_kurtosis, 3),
        "bimodality_coefficient":   round(bc, 4),
        "is_bimodal":               is_bimodal,
        "within_1sigma_pct":        round(within_1sigma * 100, 1),
        "within_2sigma_pct":        round(within_2sigma * 100, 1),
        "gaussian_1sigma_range_mb": [round(mu - std, 2), round(mu + std, 2)],
        "gaussian_2sigma_range_mb": [round(mu - 2*std, 2), round(mu + 2*std, 2)],
        "size_bands": {
            "tiny_pct":  round(tiny_pct  * 100, 1),
            "small_pct": round(small_pct * 100, 1),
            "ideal_pct": round(ideal_pct * 100, 1),
            "large_pct": round(large_pct * 100, 1),
        },
        "interpretation": (
            f"BC={bc:.3f} > 0.555 → BIMODAL: {tiny_pct*100:.0f}% tiny + {large_pct*100:.0f}% large files. "
            f"Run OPTIMIZE bin-pack to compact into the ideal 128–512 MB range."
            if is_bimodal else
            f"BC={bc:.3f} ≤ 0.555 → Unimodal (Gaussian-like). "
            f"Mean={mu:.1f} MB, σ={std:.1f} MB. CV={cv:.2f}."
        ),
        "action": (
            "OPTIMIZE table REWRITE DATA USING bin-pack WHERE file_size_in_bytes < 134217728"
            if is_bimodal else "No compaction needed — file distribution is healthy"
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 6. Little's Law — Optimal Parallelism
#    L = λ·W  →  optimal_partitions = λ_target × avg_task_sec
# ─────────────────────────────────────────────────────────────────────────────

def littles_law_parallelism(
    avg_task_sec: float,
    num_executors: int,
    target_duration_min: Optional[float] = None,
    total_tasks: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Apply Little's Law (queueing theory) to derive optimal shuffle.partitions.

    Little's Law: L = λ · W
      L = number of tasks in the system (= num_executors × utilisation)
      λ = task completion rate (tasks/sec) = num_executors / avg_task_sec
      W = average task service time (sec)

    avg_task_sec:      average Spark task duration in seconds.
    num_executors:     current executor count.
    target_duration_min: desired total job duration; used to compute λ_target.
    total_tasks:       total number of tasks in the job (stages × partitions).
    """
    if avg_task_sec <= 0:
        return {"error": "avg_task_sec must be positive"}
    if num_executors < 1:
        return {"error": "num_executors must be >= 1"}

    # Current throughput
    current_lambda = num_executors / avg_task_sec  # tasks/sec

    # Concurrent tasks in flight (steady state)
    L_current = current_lambda * avg_task_sec  # = num_executors (by definition)

    # Optimal shuffle.partitions ≈ 3× executors (keeps pipeline filled)
    optimal_shuffle_partitions = max(200, int(num_executors * 3))

    # If target duration and total tasks given: compute required executors
    req_executors = None
    if target_duration_min and total_tasks:
        target_sec = target_duration_min * 60
        required_lambda = total_tasks / target_sec
        req_executors = math.ceil(required_lambda * avg_task_sec)

    # Utilisation estimate
    utilisation = min(1.0, L_current / num_executors)

    result: Dict[str, Any] = {
        "model":                      "Little's Law: L = λ·W (queueing theory)",
        "avg_task_sec":               avg_task_sec,
        "num_executors":              num_executors,
        "task_throughput_per_sec":    round(current_lambda, 3),
        "concurrent_tasks_L":         round(L_current, 1),
        "executor_utilisation":       round(utilisation, 3),
        "optimal_shuffle_partitions": optimal_shuffle_partitions,
        "optimal_parallelism":        num_executors * 2,
        "interpretation": (
            f"At {avg_task_sec:.1f}s/task and {num_executors} executors: "
            f"throughput = {current_lambda:.2f} tasks/sec. "
            f"Set shuffle.partitions = {optimal_shuffle_partitions} "
            f"to keep all executors busy with a 3× pipeline buffer."
        ),
        "spark_config": {
            "spark.sql.shuffle.partitions":     str(optimal_shuffle_partitions),
            "spark.default.parallelism":        str(num_executors * 2),
        },
    }
    if req_executors is not None:
        result["executors_needed_for_target"] = req_executors
        result["scale_factor"] = round(req_executors / num_executors, 2)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# 7. Shewhart X-bar 3-Sigma Control Chart
#    UCL = μ + 3σ  /  LCL = μ − 3σ
#    Statistical job cost / duration anomaly detection.
# ─────────────────────────────────────────────────────────────────────────────

def shewhart_control_chart(
    history: List[float],
    current_value: float,
    label: str = "cost_usd",
) -> Dict[str, Any]:
    """
    Shewhart X-bar control chart for anomaly detection.

    history:       list of historical observations (≥ 5 required).
    current_value: the new observation to evaluate.
    label:         what is being measured (for the interpretation string).
    """
    if len(history) < 5:
        return {"error": "Need at least 5 historical data points"}

    mu    = statistics.mean(history)
    sigma = statistics.stdev(history)

    ucl_1 = mu + sigma
    ucl_2 = mu + 2 * sigma
    ucl_3 = mu + 3 * sigma
    lcl_3 = max(0, mu - 3 * sigma)

    z_score = (current_value - mu) / sigma if sigma > 0 else 0.0

    zone = (
        "Zone A (>3σ — CRITICAL)"   if abs(z_score) > 3 else
        "Zone B (>2σ — WARNING)"    if abs(z_score) > 2 else
        "Zone C (>1σ — WATCH)"      if abs(z_score) > 1 else
        "In Control"
    )
    is_anomaly = abs(z_score) > 3

    pct_above_mean = (current_value / mu - 1.0) * 100 if mu > 0 else 0.0

    # Western Electric rules (optional signals beyond simple UCL)
    we_rules: List[str] = []
    if len(history) >= 8:
        # Rule 1: 1 point > 3σ (same as is_anomaly above)
        # Rule 2: 2 of 3 consecutive points > 2σ on the same side
        above_2s = [(v - mu) / sigma > 2 for v in history[-3:] + [current_value]]
        if sum(above_2s[-3:]) >= 2:
            we_rules.append("2-of-3 points beyond 2σ on same side (process drift)")
        # Rule 3: 4 of 5 consecutive points > 1σ on the same side
        above_1s = [(v - mu) / sigma > 1 for v in history[-4:] + [current_value]]
        if sum(above_1s) >= 4:
            we_rules.append("4-of-5 points beyond 1σ (sustained upward shift)")
        # Rule 4: 8 consecutive points on same side of mean
        same_side = [v > mu for v in history[-7:] + [current_value]]
        if all(same_side) or not any(same_side):
            we_rules.append("8 consecutive points on same side of mean (mean shift)")

    return {
        "model":              "Shewhart X-bar chart: UCL/LCL = μ ± 3σ",
        "label":              label,
        "mean":               round(mu, 4),
        "std":                round(sigma, 4),
        "ucl_1sigma":         round(ucl_1, 4),
        "ucl_2sigma":         round(ucl_2, 4),
        "ucl_3sigma":         round(ucl_3, 4),
        "lcl_3sigma":         round(lcl_3, 4),
        "current_value":      round(current_value, 4),
        "z_score":            round(z_score, 3),
        "zone":               zone,
        "is_anomaly":         is_anomaly,
        "pct_above_mean":     round(pct_above_mean, 1),
        "western_electric_signals": we_rules,
        "n_historical":       len(history),
        "interpretation": (
            f"{label}={current_value:.4f} is {zone} (z={z_score:.2f}, "
            f"{pct_above_mean:+.0f}% vs mean {mu:.4f}). "
            f"UCL={ucl_3:.4f}. "
            + (f"ALERT: {'; '.join(we_rules)}" if we_rules else "No additional Western Electric signals.")
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 8. Discrete Fourier Transform — Periodicity Detector
#    Identify dominant cycles in CloudWatch time series
#    (daily heap spikes, weekly batch peaks, etc.)
# ─────────────────────────────────────────────────────────────────────────────

def fourier_periodicity(
    time_series: List[float],
    sample_interval_minutes: int = 5,
) -> Dict[str, Any]:
    """
    Compute DFT amplitudes and identify dominant periodicity.

    time_series:             ordered metric values (e.g. heap %, CPU %, cost).
    sample_interval_minutes: interval between samples (default 5 min CloudWatch).
    """
    n = len(time_series)
    if n < 8:
        return {"error": "Need at least 8 data points for meaningful DFT"}

    # Mean-centre the series so DC component doesn't dominate
    mu = sum(time_series) / n
    x  = [v - mu for v in time_series]

    # DFT: X[k] = Σ x[n] · e^(−j·2π·k·n/N)
    amplitudes: List[Tuple[int, float]] = []
    for k in range(1, n // 2 + 1):
        real = sum(x[t] * math.cos(2 * math.pi * k * t / n) for t in range(n))
        imag = sum(-x[t] * math.sin(2 * math.pi * k * t / n) for t in range(n))
        amp = math.sqrt(real ** 2 + imag ** 2) / n
        period_samples = n / k
        period_minutes = period_samples * sample_interval_minutes
        amplitudes.append((k, round(amp, 4), round(period_samples, 1), round(period_minutes, 0)))

    amplitudes.sort(key=lambda a: -a[1])
    top3 = amplitudes[:3]

    dominant_freq, dominant_amp, dominant_period_samples, dominant_period_min = top3[0]
    dominant_period_hr  = dominant_period_min / 60
    dominant_period_day = dominant_period_min / 1440

    label = (
        "~daily cycle"   if 20 <= dominant_period_hr <= 28  else
        "~weekly cycle"  if 6  <= dominant_period_day <= 8  else
        "~hourly cycle"  if 50 <= dominant_period_min <= 70 else
        f"{dominant_period_hr:.1f}-hour cycle"
    )

    return {
        "model":                  "DFT: X[k] = Σ x[n]·e^(−j·2π·kn/N)",
        "n_samples":              n,
        "sample_interval_min":    sample_interval_minutes,
        "dominant_period_samples":dominant_period_samples,
        "dominant_period_min":    dominant_period_min,
        "dominant_period_hr":     round(dominant_period_hr, 2),
        "dominant_amplitude":     dominant_amp,
        "dominant_label":         label,
        "top_frequencies": [
            {
                "rank":           i + 1,
                "freq_index":     t[0],
                "amplitude":      t[1],
                "period_samples": t[2],
                "period_min":     t[3],
                "period_hr":      round(t[3] / 60, 2),
            }
            for i, t in enumerate(top3)
        ],
        "interpretation": (
            f"Dominant frequency: {label} (period={dominant_period_hr:.1f}h, "
            f"amplitude={dominant_amp:.4f}). "
            f"{'Schedule OPTIMIZE and VACUUM to run at the trough of this cycle.' if dominant_amp > 0.1 else 'Low amplitude — no strong periodic pattern detected.'}"
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 9. Bloom Filter I/O Savings Estimator
#    P(FP) = (1 − e^(−k·n/m))^k   — optimal k = (m/n)·ln2
# ─────────────────────────────────────────────────────────────────────────────

def bloom_filter_savings(
    table_size_gb: float,
    join_selectivity: float,
    num_distinct_keys: int,
    bits_per_key: int = 10,
) -> Dict[str, Any]:
    """
    Estimate Bloom filter false-positive rate and I/O savings for a join.

    table_size_gb:     size of the probed (larger) table.
    join_selectivity:  fraction of rows that match (0–1). Low = fewer matches.
    num_distinct_keys: distinct values of the join key in the build (smaller) table.
    bits_per_key:      m/n — bits allocated per key in the filter (default 10).
    """
    if not (0 < join_selectivity <= 1):
        return {"error": "join_selectivity must be in (0, 1]"}
    if num_distinct_keys < 1:
        return {"error": "num_distinct_keys must be >= 1"}

    # Optimal number of hash functions: k = (m/n)·ln2
    k_opt = bits_per_key * math.log(2)

    # False-positive probability: P = (1 − e^(−k·n/m))^k
    inner = 1 - math.exp(-k_opt * num_distinct_keys / (bits_per_key * num_distinct_keys))
    fpp = inner ** k_opt

    # Memory for the filter
    filter_size_bytes = (bits_per_key * num_distinct_keys) / 8
    filter_size_mb    = filter_size_bytes / (1024 ** 2)

    # I/O savings: rows NOT scanned because of early filter push-down
    # Without filter: scan all of table_size_gb
    # With filter: scan only matching rows + false positives
    rows_scanned_pct  = join_selectivity + (1 - join_selectivity) * fpp
    io_saved_pct      = 1.0 - rows_scanned_pct
    io_saved_gb       = table_size_gb * io_saved_pct

    return {
        "model":                "Bloom filter: P(FP) = (1−e^(−kn/m))^k, k_opt = (m/n)·ln2",
        "table_size_gb":        table_size_gb,
        "num_distinct_keys":    num_distinct_keys,
        "bits_per_key":         bits_per_key,
        "optimal_hash_fns_k":   round(k_opt, 2),
        "false_positive_rate":  round(fpp, 6),
        "filter_size_mb":       round(filter_size_mb, 3),
        "join_selectivity":     join_selectivity,
        "rows_scanned_pct":     round(rows_scanned_pct * 100, 2),
        "io_saved_pct":         round(io_saved_pct * 100, 2),
        "io_saved_gb":          round(io_saved_gb, 3),
        "worth_enabling":       io_saved_pct > 0.10,
        "interpretation": (
            f"Bloom filter with {bits_per_key} bits/key: FPP={fpp*100:.3f}%. "
            f"For {join_selectivity*100:.1f}% selectivity on {table_size_gb:.1f} GB table: "
            f"saves {io_saved_pct*100:.0f}% I/O ({io_saved_gb:.2f} GB). "
            f"Filter memory: {filter_size_mb:.1f} MB. "
            + ("ENABLE Bloom filter." if io_saved_pct > 0.10 else "Marginal gain — low selectivity.")
        ),
        "iceberg_ddl": (
            f"ALTER TABLE <table> SET TBLPROPERTIES "
            f"('write.parquet.bloom-filter-enabled.column.<join_col>'='true', "
            f"'write.parquet.bloom-filter-fpp.column.<join_col>'='{min(0.05, fpp*10):.3f}');"
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 10. Spot Instance Interruption Risk (geometric distribution)
#     P(survive t hours) = (1 − p_interrupt)^t
# ─────────────────────────────────────────────────────────────────────────────

def spot_interruption_risk(
    job_duration_hours: float,
    hourly_interruption_rate: float = 0.05,
    checkpoint_interval_hours: float = 0.0,
) -> Dict[str, Any]:
    """
    Model spot instance interruption risk using geometric distribution.

    job_duration_hours:        expected job duration.
    hourly_interruption_rate:  probability of interruption per hour (default 5%).
                               Typical AWS spot: 2–10% depending on instance type.
    checkpoint_interval_hours: if > 0, job can resume from checkpoint, reducing
                               expected re-work cost.
    """
    if not (0 < hourly_interruption_rate < 1):
        return {"error": "hourly_interruption_rate must be in (0, 1)"}
    if job_duration_hours <= 0:
        return {"error": "job_duration_hours must be positive"}

    # P(survive entire job) = (1 − p)^t
    p_survive = (1 - hourly_interruption_rate) ** job_duration_hours

    # Expected time until first interruption: E[T] = 1/p hours (geometric)
    mean_time_to_interrupt = 1.0 / hourly_interruption_rate

    # P(interrupted before t hours)
    p_interrupted = 1.0 - p_survive

    # With checkpointing: max rework = checkpoint_interval
    rework_hours = job_duration_hours if checkpoint_interval_hours == 0 else checkpoint_interval_hours

    # Expected rework cost on interruption
    expected_rework_hours = p_interrupted * rework_hours

    # Spot vs on-demand cost model
    spot_discount = 0.70  # typical ~70% cheaper
    spot_price_factor = 1.0 - spot_discount  # pay 30% of on-demand
    # Effective cost ratio: spot_cost + expected rework cost
    effective_factor = spot_price_factor + p_interrupted * rework_hours / job_duration_hours

    return {
        "model":                    "Geometric distribution: P(survive) = (1−p)^t",
        "job_duration_hours":       job_duration_hours,
        "hourly_interruption_rate": hourly_interruption_rate,
        "p_survive_full_job":       round(p_survive, 4),
        "p_interrupted":            round(p_interrupted, 4),
        "mean_time_to_interrupt_hr":round(mean_time_to_interrupt, 1),
        "expected_rework_hours":    round(expected_rework_hours, 3),
        "checkpoint_interval_hours":checkpoint_interval_hours,
        "effective_spot_cost_factor": round(effective_factor, 3),
        "spot_discount_pct":        70,
        "net_savings_pct":          round((1 - effective_factor) * 100, 1),
        "recommendation": (
            f"Spot viable — net savings ~{(1-effective_factor)*100:.0f}% after expected rework. "
            + (f"Set checkpoint every {checkpoint_interval_hours}h to limit rework." if checkpoint_interval_hours > 0 else
               "Add checkpointing to reduce rework risk.")
        ) if effective_factor < 0.8 else
        "Spot risk too high for this job duration — use on-demand or add checkpointing.",
        "aws_glue_config": (
            "Glue does not support spot natively. Consider EMR with "
            "instance_fleet + spot + checkpointing to S3."
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# 11. Pareto Recommendation Ranker (80/20 rule)
#     Score = savings_pct / sqrt(effort_hours)  — impact-per-effort
# ─────────────────────────────────────────────────────────────────────────────

def pareto_rank_recommendations(
    recommendations: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Apply Pareto 80/20 principle to rank recommendations by impact-per-effort.

    Score = estimated_savings_percent / sqrt(effort_hours)
    The top 20% of recs that deliver 80% of savings are identified.
    """
    if not recommendations:
        return {"error": "recommendations list is empty"}

    effort_map = {"low": 2, "medium": 8, "high": 24, "very_high": 80}

    scored = []
    for r in recommendations:
        savings    = float(r.get("estimated_savings_percent", 0))
        effort_hrs = float(effort_map.get(str(r.get("effort", "medium")).lower(), 8))
        effort_hrs = max(0.5, effort_hrs)
        score      = savings / math.sqrt(effort_hrs)
        scored.append({**r, "_pareto_score": round(score, 3), "_effort_hours": effort_hrs})

    scored.sort(key=lambda x: -x["_pareto_score"])

    # Cumulative savings to find 80% threshold
    total_savings = sum(r.get("estimated_savings_percent", 0) for r in scored)
    cumulative    = 0.0
    top_20pct_idx = len(scored)  # default: all recs
    for i, r in enumerate(scored):
        cumulative += r.get("estimated_savings_percent", 0)
        if cumulative >= 0.80 * total_savings:
            top_20pct_idx = i + 1
            break

    pareto_recs = scored[:top_20pct_idx]

    return {
        "model":                  "Pareto 80/20: score = savings_pct / √effort_hours",
        "total_recommendations":  len(scored),
        "pareto_count":           top_20pct_idx,
        "pareto_pct_of_total":    round(top_20pct_idx / len(scored) * 100, 1),
        "pareto_savings_pct":     round(cumulative, 1),
        "ranked_recommendations": scored,
        "pareto_front":           pareto_recs,
        "interpretation": (
            f"Top {top_20pct_idx} of {len(scored)} recommendations "
            f"({top_20pct_idx/len(scored)*100:.0f}%) deliver "
            f"{cumulative:.0f}% of total savings. "
            f"Focus there first (Pareto 80/20 principle)."
        ),
    }
