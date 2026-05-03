"""
Learning Agent
==============
Captures execution patterns, quality scores, and optimisation outcomes
as learning vectors persisted to S3 for continuous improvement.
"""

import json
import logging
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict

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


def create_learning_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                           region: str = "us-west-2") -> Agent:
    """Return a Strands Agent for learning and pattern capture."""
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[capture_learning_vector, retrieve_learning_vectors],
    )
