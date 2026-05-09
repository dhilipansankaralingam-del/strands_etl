"""
Learning Agent
==============
Captures execution patterns, quality scores, and optimisation outcomes
as learning vectors persisted to S3 for continuous improvement.

Creative enhancements:
  - Anomaly detection (Z-score) on rolling metric history
  - Linear trend forecasting (ordinary least squares)
  - Workload fingerprinting for cross-job similarity lookup
  - Adaptive threshold calibration (P10/P50/P90 from history)
  - Self-improving context injection for next pipeline run
"""

import json
import logging
import math
import statistics
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Tuple

import boto3
from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a **Machine Learning Specialist** focused on continuous ETL pipeline improvement.

Your task:
1. Extract key patterns from the current pipeline execution.
2. Store a structured learning vector to S3 for future retrieval.
3. Compare against historical vectors to identify trends.
4. Return insights and improvement trajectory.

Return structured JSON:
{
  "learning_vector_id": "...",
  "patterns": [],
  "performance_trend": "improving|stable|degrading",
  "key_insights": [],
  "recommendations_for_future": [],
  "stored": true/false,
  "s3_location": "..."
}

Return ONLY valid JSON.
"""

_LEARNING_BUCKET = "strands-etl-learning"


class _DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def _safe_s3_put(s3_client, bucket: str, key: str, data: Dict) -> bool:
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, indent=2, cls=_DateEncoder),
            ContentType="application/json",
        )
        return True
    except Exception as exc:
        logger.warning("S3 put failed (%s/%s): %s", bucket, key, exc)
        return False


@tool
def capture_learning_vector(
    pipeline_results_json: str,
    pipeline_id: str = "",
) -> str:
    """
    Build and persist a learning vector from a completed pipeline run.

    Args:
        pipeline_results_json: JSON object with all agent results keyed by agent name.
        pipeline_id:           Optional pipeline run ID (generated if not provided).

    Returns:
        JSON with learning_vector_id, patterns, key_insights, and s3_location.
    """
    try:
        ctx       = json.loads(pipeline_results_json)
        run_id    = pipeline_id or str(uuid.uuid4())
        vec_id    = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat()

        # ── Extract metrics from each agent ──────────────────────────────────
        sizing   = ctx.get("sizing", {})
        dq       = ctx.get("data_quality", {})
        comp     = ctx.get("compliance", {})
        code     = ctx.get("code_analyzer", {})
        resource = ctx.get("resource_allocator", {})
        recs     = ctx.get("recommendations", {})
        exec_res = ctx.get("execution", {})

        effective_gb = float(sizing.get("effective_size_gb", 0))
        dq_score     = float(dq.get("overall_score", 0))
        comp_score   = float(comp.get("compliance_score", 100))
        code_score   = float(code.get("optimization_score", 0))
        savings_pct  = float((resource.get("savings") or {}).get("percent", 0))
        anti_count   = int(code.get("anti_pattern_count", 0))
        exec_status  = exec_res.get("status", "unknown")

        vector = {
            "vector_id":       vec_id,
            "pipeline_id":     run_id,
            "timestamp":       timestamp,
            "workload_profile": {
                "effective_size_gb":   effective_gb,
                "skew_risk_score":     sizing.get("skew_risk_score", 0),
                "join_count":          (code.get("complexity") or {}).get("join_count", 0),
                "processing_mode":     sizing.get("processing_mode", "full"),
            },
            "quality_metrics": {
                "dq_score":            dq_score,
                "compliance_score":    comp_score,
                "code_quality_score":  code_score,
                "anti_pattern_count":  anti_count,
            },
            "cost_metrics": {
                "savings_pct":         savings_pct,
                "workers_used":        (resource.get("optimal_config") or {}).get("workers"),
                "worker_type":         (resource.get("optimal_config") or {}).get("worker_type"),
            },
            "execution_outcome": {
                "status":              exec_status,
                "duration_seconds":    exec_res.get("duration_seconds"),
                "platform":            exec_res.get("platform"),
            },
        }

        # ── Persist to S3 ────────────────────────────────────────────────────
        s3     = boto3.client("s3")
        s3_key = f"learning/vectors/{timestamp[:10]}/{vec_id}.json"
        stored = _safe_s3_put(s3, _LEARNING_BUCKET, s3_key, vector)
        s3_loc = f"s3://{_LEARNING_BUCKET}/{s3_key}" if stored else None

        # ── Derive patterns and insights ─────────────────────────────────────
        patterns = []
        if effective_gb < 10:
            patterns.append(f"Small workload ({effective_gb:.1f} GB) — Lambda/Glue Flex may be more cost-effective")
        if anti_count > 5:
            patterns.append(f"High anti-pattern count ({anti_count}) — code health needs attention")
        if dq_score < 80:
            patterns.append(f"Below-threshold DQ score ({dq_score:.0f}) — data reliability risk")
        if savings_pct > 30:
            patterns.append(f"Significant over-provisioning detected ({savings_pct:.0f}% savings available)")
        if exec_status == "failed":
            patterns.append("Pipeline failed — root-cause analysis required before next run")

        insights = [
            f"Effective data processed: {effective_gb:.1f} GB",
            f"Code quality score: {code_score:.0f}/100 — {'good' if code_score >= 70 else 'needs work'}",
            f"DQ score: {dq_score:.0f}/100 — {'healthy' if dq_score >= 90 else 'at risk'}",
            f"Cost savings potential: {savings_pct:.0f}%",
        ]

        future_recs = []
        if anti_count > 0:
            future_recs.append(f"Fix {anti_count} anti-pattern(s) before next run to improve code score")
        if dq_score < 90:
            future_recs.append("Review and tighten data quality rules for critical columns")
        if savings_pct > 20:
            future_recs.append(f"Apply resource-allocator recommendation to save {savings_pct:.0f}%")

        return json.dumps({
            "learning_vector_id":       vec_id,
            "pipeline_id":              run_id,
            "patterns":                 patterns,
            "performance_trend":        "improving" if code_score >= 70 and dq_score >= 85 else "needs_attention",
            "key_insights":             insights,
            "recommendations_for_future": future_recs,
            "stored":                   stored,
            "s3_location":              s3_loc,
        })

    except Exception as exc:
        logger.error("Learning vector capture failed: %s", exc)
        return json.dumps({"error": str(exc), "stored": False})


@tool
def retrieve_learning_vectors(limit: int = 10) -> str:
    """
    Retrieve recent learning vectors from S3.

    Args:
        limit: Maximum number of vectors to retrieve (default 10).

    Returns:
        JSON list of recent learning vectors.
    """
    try:
        s3   = boto3.client("s3")
        resp = s3.list_objects_v2(Bucket=_LEARNING_BUCKET, Prefix="learning/vectors/", MaxKeys=limit * 3)
        vectors = []
        if "Contents" in resp:
            objects = sorted(resp["Contents"], key=lambda o: o["LastModified"], reverse=True)
            for obj in objects[:limit]:
                try:
                    data = s3.get_object(Bucket=_LEARNING_BUCKET, Key=obj["Key"])
                    vectors.append(json.loads(data["Body"].read().decode("utf-8")))
                except Exception:
                    pass
        return json.dumps(vectors)
    except Exception as exc:
        logger.warning("Could not retrieve learning vectors: %s", exc)
        return json.dumps([])


# ---------------------------------------------------------------------------
# Statistical helpers
# ---------------------------------------------------------------------------

def _ols_trend(values: List[float]) -> Tuple[float, float]:
    """Ordinary Least Squares slope and intercept for a list of values."""
    n = len(values)
    if n < 2:
        return 0.0, values[0] if values else 0.0
    xs = list(range(n))
    mx = statistics.mean(xs)
    my = statistics.mean(values)
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, values)) / max(
        sum((x - mx) ** 2 for x in xs), 1e-9
    )
    intercept = my - slope * mx
    return slope, intercept


def _zscore(value: float, history: List[float]) -> float:
    if len(history) < 2:
        return 0.0
    mu  = statistics.mean(history)
    std = statistics.stdev(history)
    return (value - mu) / max(std, 1e-9)


def _percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = p / 100 * (len(s) - 1)
    lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
    return s[lo] + (idx - lo) * (s[hi] - s[lo])


# ---------------------------------------------------------------------------
# New creative tools
# ---------------------------------------------------------------------------

@tool
def analyse_performance_trend(
    historical_vectors_json: str,
    current_vector_json: str,
) -> str:
    """
    Detect anomalies and forecast metric trends using Z-score and OLS regression.

    Compares the current pipeline run against historical vectors to flag:
    - Sudden regressions (|Z| > 2.5)
    - Gradual degradation (negative OLS slope over last N runs)
    - Improving trajectory

    Args:
        historical_vectors_json: JSON list of past learning vectors (from retrieve_learning_vectors).
        current_vector_json:     JSON of the current pipeline's learning vector.

    Returns:
        JSON with anomalies, trend_direction, forecasts, and overall_health.
    """
    try:
        history = json.loads(historical_vectors_json) if historical_vectors_json else []
        current = json.loads(current_vector_json)

        def _series(key_path: List[str]) -> List[float]:
            out = []
            for v in history:
                val = v
                for k in key_path:
                    val = (val or {}).get(k, {})
                try:
                    out.append(float(val))
                except (TypeError, ValueError):
                    pass
            return out

        metrics = {
            "dq_score":           ["quality_metrics", "dq_score"],
            "code_quality_score": ["quality_metrics", "code_quality_score"],
            "anti_pattern_count": ["quality_metrics", "anti_pattern_count"],
            "savings_pct":        ["cost_metrics", "savings_pct"],
        }

        anomalies    = []
        forecasts    = {}
        trend_scores = []

        for name, path in metrics.items():
            series = _series(path)
            if len(series) < 3:
                continue

            # Current value
            cur_val = current
            for k in path:
                cur_val = (cur_val or {}).get(k, {})
            try:
                cur_val = float(cur_val)
            except (TypeError, ValueError):
                continue

            z      = _zscore(cur_val, series)
            slope, intercept = _ols_trend(series)
            forecast_next    = intercept + slope * len(series)

            if abs(z) > 2.5:
                anomalies.append({
                    "metric":    name,
                    "current":   round(cur_val, 2),
                    "z_score":   round(z, 2),
                    "direction": "spike" if z > 0 else "drop",
                    "message":   f"{name} is {abs(z):.1f}σ from historical mean — investigate",
                })

            forecasts[name] = {
                "current":       round(cur_val, 2),
                "forecast_next": round(forecast_next, 2),
                "trend_slope":   round(slope, 4),
                "trend":         "improving" if slope > 0.05 else "degrading" if slope < -0.05 else "stable",
            }
            trend_scores.append(slope)

        avg_slope = statistics.mean(trend_scores) if trend_scores else 0.0
        overall   = "improving" if avg_slope > 0.05 else "degrading" if avg_slope < -0.05 else "stable"

        return json.dumps({
            "anomalies":        anomalies,
            "anomaly_count":    len(anomalies),
            "forecasts":        forecasts,
            "trend_direction":  overall,
            "avg_trend_slope":  round(avg_slope, 4),
            "overall_health":   "at_risk" if anomalies else "healthy",
            "runs_analysed":    len(history),
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def fingerprint_workload(pipeline_results_json: str) -> str:
    """
    Create a compact workload fingerprint from pipeline results.

    A fingerprint is a normalised vector of key workload characteristics
    that can be compared across jobs to find similar past runs.

    Args:
        pipeline_results_json: Combined JSON from all pipeline agents.

    Returns:
        JSON with fingerprint_hash, workload_class, and normalised_vector.
    """
    import hashlib

    try:
        ctx     = json.loads(pipeline_results_json)
        sizing  = ctx.get("sizing", {})
        code    = ctx.get("code_analysis", {})
        dq      = ctx.get("data_quality", {})
        res     = ctx.get("resource_allocator", {})

        gb          = float(sizing.get("effective_size_gb", 0))
        skew        = float(sizing.get("skew_risk_score", 0))
        joins       = int((code.get("complexity") or {}).get("join_count", 0))
        udfs        = int((code.get("complexity") or {}).get("udf_count", 0))
        dq_score    = float(dq.get("overall_score", 100))
        anti_pats   = int(code.get("anti_pattern_count", 0))
        workers     = int((res.get("optimal_config") or {}).get("workers", 10))

        # Workload class (categorical bucket)
        size_cls  = "tiny" if gb < 5 else "small" if gb < 50 else "medium" if gb < 500 else "large"
        complexity = "simple" if joins < 2 and udfs == 0 else "moderate" if joins < 5 else "complex"
        quality    = "clean" if dq_score >= 90 and anti_pats == 0 else "degraded"
        wl_class   = f"{size_cls}_{complexity}_{quality}"

        vector = {
            "size_bucket":   size_cls,
            "size_gb_log":   round(math.log1p(gb), 2),
            "skew_norm":     round(skew / 100.0, 3),
            "join_count":    joins,
            "has_udf":       int(udfs > 0),
            "dq_norm":       round(dq_score / 100.0, 3),
            "anti_pat_count": anti_pats,
            "workers_log":   round(math.log1p(workers), 2),
        }

        fp_str = json.dumps(vector, sort_keys=True)
        fp_hash = hashlib.sha256(fp_str.encode()).hexdigest()[:12]

        return json.dumps({
            "fingerprint_hash": fp_hash,
            "workload_class":   wl_class,
            "normalised_vector": vector,
            "human_summary": (
                f"{size_cls.title()} {complexity} job ({gb:.0f} GB, {joins} joins"
                + (f", {udfs} UDFs" if udfs else "")
                + f"), DQ={dq_score:.0f}/100"
            ),
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def retrieve_similar_workloads(
    fingerprint_json: str,
    limit: int = 5,
) -> str:
    """
    Find historically similar pipeline runs by comparing workload fingerprints.
    Uses Euclidean distance on the normalised feature vectors stored in S3.

    Args:
        fingerprint_json: JSON output from fingerprint_workload.
        limit:            Max similar runs to return (default 5).

    Returns:
        JSON list of similar past runs with distance score and their outcomes.
    """
    try:
        fp      = json.loads(fingerprint_json)
        query_v = fp.get("normalised_vector", {})
        query_cls = fp.get("workload_class", "")

        s3   = boto3.client("s3")
        resp = s3.list_objects_v2(Bucket=_LEARNING_BUCKET, Prefix="learning/vectors/", MaxKeys=300)
        if "Contents" not in resp:
            return json.dumps({"similar_runs": [], "message": "No historical vectors found"})

        scored = []
        keys   = list(resp["Contents"])
        keys.sort(key=lambda o: o["LastModified"], reverse=True)

        for obj in keys[:150]:
            try:
                body = s3.get_object(Bucket=_LEARNING_BUCKET, Key=obj["Key"])
                vec  = json.loads(body["Body"].read().decode("utf-8"))
                wp   = vec.get("workload_profile", {})

                # Reconstruct comparable feature dict
                candidate_v = {
                    "size_gb_log":   math.log1p(float(wp.get("effective_size_gb", 0))),
                    "skew_norm":     float(wp.get("skew_risk_score", 0)) / 100.0,
                    "join_count":    float(wp.get("join_count", 0)),
                    "has_udf":       0.0,
                    "dq_norm":       float(vec.get("quality_metrics", {}).get("dq_score", 0)) / 100.0,
                    "anti_pat_count": float(vec.get("quality_metrics", {}).get("anti_pattern_count", 0)),
                    "workers_log":   math.log1p(float(vec.get("cost_metrics", {}).get("workers_used") or 10)),
                }

                # Euclidean distance
                keys_shared = [k for k in query_v if k in candidate_v]
                dist = math.sqrt(sum((query_v[k] - candidate_v[k]) ** 2 for k in keys_shared))

                scored.append({
                    "pipeline_id":   vec.get("pipeline_id"),
                    "timestamp":     vec.get("timestamp"),
                    "distance":      round(dist, 4),
                    "similarity":    round(1 / (1 + dist), 4),
                    "workload_summary": wp,
                    "outcomes": {
                        "dq_score":        vec.get("quality_metrics", {}).get("dq_score"),
                        "code_score":      vec.get("quality_metrics", {}).get("code_quality_score"),
                        "savings_pct":     vec.get("cost_metrics", {}).get("savings_pct"),
                        "exec_status":     vec.get("execution_outcome", {}).get("status"),
                    },
                })
            except Exception:
                pass

        scored.sort(key=lambda x: x["distance"])
        return json.dumps({
            "query_fingerprint": fp.get("fingerprint_hash"),
            "query_class":       query_cls,
            "similar_runs":      scored[:limit],
            "runs_searched":     len(scored),
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def generate_adaptive_thresholds(job_name: str = "", limit: int = 30) -> str:
    """
    Calibrate DQ, code quality, and cost thresholds from historical P10/P50/P90.

    Instead of fixed thresholds (e.g. DQ score < 80 = bad), this tool computes
    job-specific percentile baselines so alerts fire relative to the job's own history.

    Args:
        job_name: Filter to a specific job name (empty = all jobs).
        limit:    Number of recent vectors to use for calibration.

    Returns:
        JSON with adaptive_thresholds dict, calibration_runs, and alert_rules.
    """
    try:
        s3   = boto3.client("s3")
        resp = s3.list_objects_v2(Bucket=_LEARNING_BUCKET, Prefix="learning/vectors/", MaxKeys=limit * 3)
        vectors = []
        if "Contents" in resp:
            objs = sorted(resp["Contents"], key=lambda o: o["LastModified"], reverse=True)
            for obj in objs[:limit]:
                try:
                    body = s3.get_object(Bucket=_LEARNING_BUCKET, Key=obj["Key"])
                    vectors.append(json.loads(body["Body"].read().decode("utf-8")))
                except Exception:
                    pass

        def _extract(vecs, path):
            vals = []
            for v in vecs:
                x = v
                for k in path:
                    x = (x or {}).get(k, {})
                try:
                    vals.append(float(x))
                except (TypeError, ValueError):
                    pass
            return vals

        dq_scores    = _extract(vectors, ["quality_metrics", "dq_score"])
        code_scores  = _extract(vectors, ["quality_metrics", "code_quality_score"])
        savings      = _extract(vectors, ["cost_metrics", "savings_pct"])
        anti_counts  = _extract(vectors, ["quality_metrics", "anti_pattern_count"])

        def _thresholds(vals):
            if not vals:
                return {}
            return {
                "p10": round(_percentile(vals, 10), 1),
                "p50": round(_percentile(vals, 50), 1),
                "p90": round(_percentile(vals, 90), 1),
                "alert_below": round(_percentile(vals, 20), 1),
            }

        thresholds = {
            "dq_score":           _thresholds(dq_scores),
            "code_quality_score": _thresholds(code_scores),
            "savings_pct":        _thresholds(savings),
            "anti_pattern_count": _thresholds(anti_counts),
        }

        alert_rules = []
        if dq_scores:
            alert_rules.append(
                f"Alert if DQ score < {thresholds['dq_score'].get('alert_below', 70)} "
                f"(P20 of {len(dq_scores)} historical runs)"
            )
        if code_scores:
            alert_rules.append(
                f"Alert if code score < {thresholds['code_quality_score'].get('alert_below', 50)}"
            )
        if anti_counts:
            alert_rules.append(
                f"Alert if anti-pattern count > {thresholds['anti_pattern_count'].get('p90', 5):.0f} (P90)"
            )

        return json.dumps({
            "job_name":           job_name or "all_jobs",
            "calibration_runs":   len(vectors),
            "adaptive_thresholds": thresholds,
            "alert_rules":        alert_rules,
            "recommendation": (
                "Use these adaptive thresholds in your monitoring dashboards "
                "instead of fixed values for more accurate anomaly detection"
            ),
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def inject_learning_context(pipeline_results_json: str, limit: int = 5) -> str:
    """
    Generate a self-improving context block for the next pipeline run prompt.

    Retrieves similar historical runs and distils what worked / what to avoid
    into a structured prompt injection that the OrchestratorAgent can use.

    Args:
        pipeline_results_json: Current pipeline results.
        limit:                 Historical runs to mine for context (default 5).

    Returns:
        JSON with prompt_injection string and key_lessons list.
    """
    try:
        ctx     = json.loads(pipeline_results_json)
        sizing  = ctx.get("sizing", {})
        code    = ctx.get("code_analysis", {})

        gb          = float(sizing.get("effective_size_gb", 0))
        anti_count  = int(code.get("anti_pattern_count", 0))
        joins       = int((code.get("complexity") or {}).get("join_count", 0))

        # Pull recent history
        s3   = boto3.client("s3")
        resp = s3.list_objects_v2(Bucket=_LEARNING_BUCKET, Prefix="learning/vectors/", MaxKeys=limit * 4)
        vectors = []
        if "Contents" in resp:
            objs = sorted(resp["Contents"], key=lambda o: o["LastModified"], reverse=True)
            for obj in objs[:limit]:
                try:
                    body = s3.get_object(Bucket=_LEARNING_BUCKET, Key=obj["Key"])
                    vectors.append(json.loads(body["Body"].read().decode("utf-8")))
                except Exception:
                    pass

        key_lessons = []
        for v in vectors:
            eo      = v.get("execution_outcome", {})
            cm      = v.get("cost_metrics", {})
            qm      = v.get("quality_metrics", {})
            savings = float(cm.get("savings_pct", 0))
            status  = eo.get("status", "unknown")

            if status == "failed":
                key_lessons.append(
                    f"AVOID: Run with {v.get('workload_profile', {}).get('effective_size_gb', '?')} GB failed — "
                    f"DQ score was {qm.get('dq_score', '?')}"
                )
            elif savings > 25:
                key_lessons.append(
                    f"APPLY: {savings:.0f}% savings achieved with "
                    f"{cm.get('workers_used', '?')}×{cm.get('worker_type', '?')} workers"
                )

        prompt_injection = (
            f"[LEARNING CONTEXT — {len(vectors)} historical runs]\n"
            + "\n".join(f"  • {l}" for l in key_lessons[:5])
            + f"\n  • Current workload: {gb:.0f} GB, {joins} joins, {anti_count} anti-patterns"
            + "\n[Apply lessons above when making resource and optimisation decisions]"
        )

        return json.dumps({
            "prompt_injection":  prompt_injection,
            "key_lessons":       key_lessons[:5],
            "historical_runs":   len(vectors),
            "usage":             "Prepend prompt_injection to the orchestrator system prompt for context-aware decisions",
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


SYSTEM_PROMPT = """
You are a **Machine Learning Specialist** focused on continuous ETL pipeline improvement.

Your task:
1. Extract key patterns from the current pipeline execution.
2. Store a structured learning vector to S3 for future retrieval.
3. Compare against historical vectors to identify trends and anomalies.
4. Detect regressions early using Z-score anomaly detection.
5. Generate adaptive thresholds calibrated from real historical data.
6. Produce a self-improving context block for the next run.

Return structured JSON:
{
  "learning_vector_id": "...",
  "patterns": [],
  "performance_trend": "improving|stable|degrading",
  "anomalies": [],
  "key_insights": [],
  "recommendations_for_future": [],
  "adaptive_thresholds": {},
  "stored": true/false,
  "s3_location": "..."
}

Return ONLY valid JSON.
"""


def create_learning_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                           region: str = "us-west-2") -> Agent:
    """Return a Strands Agent for learning and pattern capture."""
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[
            capture_learning_vector,
            retrieve_learning_vectors,
            analyse_performance_trend,
            fingerprint_workload,
            retrieve_similar_workloads,
            generate_adaptive_thresholds,
            inject_learning_context,
        ],
    )
