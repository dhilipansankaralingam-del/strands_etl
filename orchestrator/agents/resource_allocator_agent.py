"""
Resource Allocator Agent
========================
Determines optimal compute resources based on effective data size and code complexity.
Compares costs across AWS Glue, EMR, EKS, Azure HDInsight/Synapse, GCP Dataproc, Databricks.
Based on ResourceAllocatorAgent from PR-11 (table-optimizer-analysis).
"""

import json
import logging
import math
from typing import Any, Dict, List

from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a **Principal Cloud Architect** specialising in Spark resource optimisation and cost management.

Given sizing and code-complexity results from upstream agents, determine:
1. Optimal worker count and worker type for AWS Glue
2. Cost comparison across platforms (Glue, EMR, EKS, Azure, GCP, Databricks)
3. Spot/preemptible savings opportunities
4. Resource efficiency analysis (is the job over/under-provisioned?)
5. Estimated cost reduction vs current configuration

Return structured JSON:
{
  "optimal_config": { "platform": "glue", "worker_type": "G.2X", "workers": N, "estimated_duration_hours": F },
  "current_config": { "cost_per_run": F, "annual_cost": F },
  "cost_comparison": [ { "platform": "...", "config": "...", "cost_per_run": F, "monthly_cost": F } ],
  "savings": { "percent": F, "monthly_usd": F, "annual_usd": F },
  "resource_efficiency": { "over_provisioned": true/false, "memory_utilization_estimate": "..." },
  "recommendations": []
}

Return ONLY valid JSON.
"""

# ---------------------------------------------------------------------------
# Pricing tables (mirrors PR-11 ResourceAllocatorAgent)
# ---------------------------------------------------------------------------
_GLUE_PRICING = {
    "G.1X": {"cost": 0.44, "memory_gb": 16,  "vcpu": 4},
    "G.2X": {"cost": 0.88, "memory_gb": 32,  "vcpu": 8},
    "G.4X": {"cost": 1.76, "memory_gb": 64,  "vcpu": 16},
    "G.8X": {"cost": 3.52, "memory_gb": 128, "vcpu": 32},
}
_EMR_PRICING = {
    "m5.xlarge":  {"cost": 0.230, "memory_gb": 16,  "vcpu": 4},
    "m5.2xlarge": {"cost": 0.461, "memory_gb": 32,  "vcpu": 8},
    "m5.4xlarge": {"cost": 0.922, "memory_gb": 64,  "vcpu": 16},
    "r5.xlarge":  {"cost": 0.302, "memory_gb": 32,  "vcpu": 4},
    "r5.2xlarge": {"cost": 0.605, "memory_gb": 64,  "vcpu": 8},
}
_AZURE_HDI = {
    "D4s_v3":  {"cost": 0.192, "memory_gb": 16, "vcpu": 4},
    "D8s_v3":  {"cost": 0.384, "memory_gb": 32, "vcpu": 8},
    "D16s_v3": {"cost": 0.768, "memory_gb": 64, "vcpu": 16},
}
_GCP_DATAPROC = {
    "n2-standard-4":  {"cost": 0.243, "memory_gb": 16, "vcpu": 4},
    "n2-standard-8":  {"cost": 0.485, "memory_gb": 32, "vcpu": 8},
    "n2-highmem-8":   {"cost": 0.580, "memory_gb": 64, "vcpu": 8},
}
_SPOT_DISCOUNT = 0.30  # pay ~30% of on-demand price

# ---------------------------------------------------------------------------
# Logic
# ---------------------------------------------------------------------------

def _calc_optimal_glue(size_gb: float, complexity: int, joins: int, skew: int) -> Dict:
    """Select optimal Glue worker type and count."""
    # Memory per worker × workers should handle data + shuffle overhead
    if size_gb < 10:
        wtype, workers = "G.1X", max(2, math.ceil(size_gb / 5))
    elif size_gb < 50:
        wtype, workers = "G.2X", max(5, math.ceil(size_gb / 10))
    elif size_gb < 200:
        wtype, workers = "G.2X", max(10, math.ceil(size_gb / 15))
    elif size_gb < 500:
        wtype, workers = "G.4X", max(15, math.ceil(size_gb / 20))
    else:
        wtype, workers = "G.4X", max(20, math.ceil(size_gb / 25))

    if joins > 3:
        workers = math.ceil(workers * 1.25)
    if skew > 60:
        workers = math.ceil(workers * 1.20)
    if complexity > 70:
        workers = math.ceil(workers * 1.15)

    # Estimated duration: larger data / more workers = faster
    throughput_gbh = workers * {"G.1X": 8, "G.2X": 15, "G.4X": 25, "G.8X": 40}[wtype]
    duration_h     = max(0.1, size_gb / max(throughput_gbh, 1))

    return {
        "platform":                "glue",
        "worker_type":             wtype,
        "workers":                 workers,
        "estimated_duration_hours": round(duration_h, 2),
    }


def _glue_cost(workers: int, wtype: str, hours: float) -> float:
    price = _GLUE_PRICING.get(wtype, _GLUE_PRICING["G.2X"])
    return round(workers * price["cost"] * hours, 4)


def _build_cost_comparison(optimal: Dict, size_gb: float) -> List[Dict]:
    """Build multi-platform cost comparison."""
    hours      = optimal["estimated_duration_hours"]
    opt_w      = optimal["workers"]
    comparison = []

    # Glue options
    for wtype, price in _GLUE_PRICING.items():
        workers = max(2, math.ceil(opt_w * (16 / price["memory_gb"]) * 0.5))
        cost    = round(workers * price["cost"] * hours, 2)
        comparison.append({
            "platform":     "AWS Glue",
            "config":       f"{workers}× {wtype}",
            "cost_per_run": cost,
            "monthly_cost": round(cost * 30, 2),
        })

    # EMR on-demand vs spot
    for itype, price in list(_EMR_PRICING.items())[:3]:
        nodes = max(3, math.ceil(opt_w * 0.7))
        od    = round(nodes * price["cost"] * hours, 2)
        spot  = round(od * _SPOT_DISCOUNT, 2)
        comparison.append({"platform": "AWS EMR (on-demand)",   "config": f"{nodes}× {itype}", "cost_per_run": od,   "monthly_cost": round(od   * 30, 2)})
        comparison.append({"platform": "AWS EMR (spot 70% off)", "config": f"{nodes}× {itype}", "cost_per_run": spot, "monthly_cost": round(spot * 30, 2)})

    # Azure HDInsight
    for itype, price in list(_AZURE_HDI.items())[:2]:
        nodes = max(3, math.ceil(opt_w * 0.8))
        cost  = round(nodes * price["cost"] * hours, 2)
        comparison.append({"platform": "Azure HDInsight", "config": f"{nodes}× {itype}", "cost_per_run": cost, "monthly_cost": round(cost * 30, 2)})

    # GCP Dataproc
    for itype, price in list(_GCP_DATAPROC.items())[:2]:
        nodes = max(3, math.ceil(opt_w * 0.7))
        cost  = round(nodes * price["cost"] * hours, 2)
        comparison.append({"platform": "GCP Dataproc", "config": f"{nodes}× {itype}", "cost_per_run": cost, "monthly_cost": round(cost * 30, 2)})

    # Databricks (DBU + EC2)
    dbu_hourly  = 0.07  # jobs compute
    ec2_hourly  = _EMR_PRICING["m5.2xlarge"]["cost"]
    dbu_workers = max(3, math.ceil(opt_w * 0.6))
    db_cost     = round(dbu_workers * (dbu_hourly + ec2_hourly) * hours, 2)
    comparison.append({"platform": "Databricks (AWS jobs)", "config": f"{dbu_workers}× m5.2xlarge", "cost_per_run": db_cost, "monthly_cost": round(db_cost * 30, 2)})

    return sorted(comparison, key=lambda x: x["cost_per_run"])


# ---------------------------------------------------------------------------
# Tool
# ---------------------------------------------------------------------------
@tool
def allocate_resources(
    sizing_result_json: str,
    code_analysis_json: str = "{}",
    current_config_json: str = '{"number_of_workers": 10, "worker_type": "G.2X", "platform": "glue"}',
    runs_per_day: int = 1,
) -> str:
    """
    Calculate optimal resource allocation and multi-platform cost comparison.

    Args:
        sizing_result_json:  JSON from Sizing Agent (effective_size_gb, skew_risk_score, …).
        code_analysis_json:  JSON from Code Analyzer Agent (complexity.complexity_score, …).
        current_config_json: Current job configuration (number_of_workers, worker_type, …).
        runs_per_day:        How many times the pipeline runs per day (for monthly cost calc).

    Returns:
        JSON with optimal_config, cost_comparison, savings, resource_efficiency, recommendations.
    """
    try:
        sizing   = json.loads(sizing_result_json)
        code     = json.loads(code_analysis_json) if code_analysis_json else {}
        current  = json.loads(current_config_json)

        size_gb     = float(sizing.get("effective_size_gb", 100))
        complexity  = int((code.get("complexity") or {}).get("complexity_score", 50))
        joins       = int((code.get("complexity") or {}).get("join_count", 0))
        skew        = int(sizing.get("skew_risk_score", 20))

        optimal  = _calc_optimal_glue(size_gb, complexity, joins, skew)
        hours    = optimal["estimated_duration_hours"]

        # Current cost
        cur_w    = int(current.get("number_of_workers", 10))
        cur_type = current.get("worker_type", "G.2X")
        cur_cost = _glue_cost(cur_w, cur_type, hours)

        # Optimal Glue cost
        opt_cost = _glue_cost(optimal["workers"], optimal["worker_type"], hours)
        opt_cost = min(opt_cost, cur_cost)   # can't be more than current

        savings_pct = round((1 - opt_cost / max(cur_cost, 0.01)) * 100, 1)
        monthly_cur = round(cur_cost * runs_per_day * 30, 2)
        monthly_opt = round(opt_cost * runs_per_day * 30, 2)

        comparison  = _build_cost_comparison(optimal, size_gb)

        # Resource efficiency
        memory_gb_needed = size_gb * 3  # rough 3× data for shuffle headroom
        cur_mem_total    = cur_w * _GLUE_PRICING.get(cur_type, {"memory_gb": 32})["memory_gb"]
        over_provisioned = cur_mem_total > memory_gb_needed * 1.5
        util_pct         = round(min(100, memory_gb_needed / max(cur_mem_total, 1) * 100), 1)

        recs = []
        if savings_pct > 10:
            recs.append({
                "priority": "P0",
                "title":    f"Reduce workers from {cur_w} to {optimal['workers']} (save {savings_pct:.0f}%)",
                "savings_pct": savings_pct,
                "effort":   "low",
            })
        if not current.get("flex_mode") and size_gb < 100:
            recs.append({
                "priority": "P1",
                "title":    "Enable Glue Flex execution for 34% discount (non-SLA workloads)",
                "savings_pct": 34,
                "effort":   "low",
            })
        if over_provisioned:
            recs.append({
                "priority": "P1",
                "title":    "Cluster is over-provisioned; right-size to improve cost efficiency",
                "savings_pct": savings_pct,
                "effort":   "low",
            })

        return json.dumps({
            "optimal_config": optimal,
            "current_config": {
                "workers":       cur_w,
                "worker_type":   cur_type,
                "cost_per_run":  round(cur_cost, 2),
                "annual_cost":   round(cur_cost * runs_per_day * 365, 2),
            },
            "cost_comparison": comparison[:10],  # top 10
            "savings": {
                "percent":     savings_pct,
                "monthly_usd": round(monthly_cur - monthly_opt, 2),
                "annual_usd":  round((cur_cost - opt_cost) * runs_per_day * 365, 2),
            },
            "resource_efficiency": {
                "over_provisioned":         over_provisioned,
                "memory_utilization_estimate": f"{util_pct}%",
                "current_total_memory_gb":  cur_mem_total,
                "required_memory_gb":       round(memory_gb_needed, 1),
            },
            "recommendations": recs,
        })

    except Exception as exc:
        logger.error("Resource allocation failed: %s", exc)
        return json.dumps({"error": str(exc)})


@tool
def recommend_flex_execution(
    effective_size_gb: float,
    job_sla_minutes: int = 60,
    runs_per_day: int = 1,
) -> str:
    """
    Recommend AWS Glue Flex vs Standard execution mode based on SLA and volume.

    Args:
        effective_size_gb: Effective processing volume in GB.
        job_sla_minutes:   Maximum acceptable job duration in minutes (default 60).
        runs_per_day:      Number of daily job runs.

    Returns:
        JSON with mode recommendation, estimated savings, and constraints.
    """
    try:
        # Flex reduces cost by ~34% but can start with up to 10 min delay
        flex_eligible = job_sla_minutes >= 30 and effective_size_gb < 500
        # Estimate duration: ~2 GB/min per G.2X worker at 10 workers
        est_duration = max(5, int(effective_size_gb / 20))
        standard_cost_hr = 0.88 * 10  # G.2X x 10 workers
        flex_cost_hr     = standard_cost_hr * 0.66
        saving_per_run   = round((standard_cost_hr - flex_cost_hr) * (est_duration / 60), 2)
        annual_savings   = round(saving_per_run * runs_per_day * 365, 0)

        return json.dumps({
            "recommended_mode":         "FLEX" if flex_eligible else "STANDARD",
            "flex_eligible":            flex_eligible,
            "reason": (
                "SLA allows flex warm-up delay and volume is manageable"
                if flex_eligible else
                f"SLA is tight ({job_sla_minutes} min) or volume too large for Flex"
            ),
            "estimated_duration_min":   est_duration,
            "standard_cost_per_run":    round(standard_cost_hr * (est_duration / 60), 2),
            "flex_cost_per_run":        round(flex_cost_hr     * (est_duration / 60), 2),
            "saving_per_run_usd":       saving_per_run,
            "annual_savings_usd":       annual_savings,
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def compare_spot_savings(
    worker_type: str = "G.2X",
    num_workers: int = 10,
    estimated_duration_hours: float = 1.0,
    runs_per_month: int = 30,
) -> str:
    """
    Compare On-Demand vs Spot/Preemptible pricing for a given configuration.

    Args:
        worker_type:               Glue worker type (G.1X / G.2X / G.4X / G.8X).
        num_workers:               Number of workers.
        estimated_duration_hours:  Expected job duration in hours.
        runs_per_month:            Monthly job run frequency.

    Returns:
        JSON with on_demand_cost, spot_cost, monthly_savings, risk_assessment.
    """
    try:
        price     = _GLUE_PRICING.get(worker_type, _GLUE_PRICING["G.2X"])["cost"]
        od_run    = round(price * num_workers * estimated_duration_hours, 2)
        spot_run  = round(od_run * (1 - _SPOT_DISCOUNT), 2)
        monthly_savings = round((od_run - spot_run) * runs_per_month, 2)
        annual_savings  = round(monthly_savings * 12, 0)

        return json.dumps({
            "worker_type":            worker_type,
            "num_workers":            num_workers,
            "on_demand_cost_per_run": od_run,
            "spot_cost_per_run":      spot_run,
            "spot_discount_pct":      int(_SPOT_DISCOUNT * 100),
            "monthly_savings_usd":    monthly_savings,
            "annual_savings_usd":     annual_savings,
            "risk_assessment":        "Spot instances may be interrupted — use checkpointing",
            "recommendation":         (
                "Use Spot for non-SLA-critical jobs; keep On-Demand for production SLAs"
            ),
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


def create_resource_allocator_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                                     region: str = "us-west-2") -> Agent:
    """Return a Strands Agent for resource allocation."""
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[allocate_resources, recommend_flex_execution, compare_spot_savings],
    )
