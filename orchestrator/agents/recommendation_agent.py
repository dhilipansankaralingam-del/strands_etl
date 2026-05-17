"""
Recommendation Agent
====================
Synthesises findings from all Phase-1 and Phase-2 agents into a prioritised,
ROI-driven recommendation report with an implementation roadmap.
Based on the RecommendationsAgent from PR-11 (table-optimizer-analysis).
"""

import json
import logging
from typing import Any, Dict, List

from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a **Principal Data Engineering Consultant** specialising in ETL optimisation and cost reduction.

Your task is to synthesise findings from multiple specialist agents and produce:
1. Executive summary with headline savings
2. Prioritised recommendation list (P0–P3) with ROI and effort
3. 4-phase implementation roadmap
4. Risk assessment
5. Success metrics

Return structured JSON:
{
  "executive_summary": { "headline": "...", "current_annual_cost": "...", "potential_annual_savings": "..." },
  "cost_analysis": { "current_monthly_cost": F, "potential_monthly_savings": F, "savings_percent": F },
  "recommendations": [{ "priority": "P0", "title": "...", "description": "...", "effort_hours": N, "annual_savings_usd": F, "roi_percent": F, "quick_win": true/false }],
  "implementation_roadmap": { "phase_1": {}, "phase_2": {}, "phase_3": {}, "phase_4": {} },
  "risks": [],
  "success_metrics": []
}

Return ONLY valid JSON.
"""

# ---------------------------------------------------------------------------
_ENG_RATE  = 150  # USD/hour
_EFFORT_H  = {"low": 2, "medium": 8, "high": 24, "very_high": 80}
_PRIORITY_ORDER = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}


def _consolidate(context: Dict) -> List[Dict]:
    """Pull recommendations from all upstream agents."""
    all_recs: List[Dict] = []
    for agent_key in ("sizing", "data_quality", "compliance", "code_analyzer", "resource_allocator"):
        agent_data = context.get(agent_key, {})
        for rec in agent_data.get("recommendations", []):
            rec.setdefault("source", agent_key)
            all_recs.append(rec)
    return all_recs


def _prioritise(recs: List[Dict]) -> List[Dict]:
    sorted_recs = sorted(
        recs,
        key=lambda r: (
            _PRIORITY_ORDER.get(r.get("priority", "P3"), 3),
            -r.get("savings_pct", r.get("estimated_savings_percent", 0)),
        ),
    )
    seen: set = set()
    unique: List[Dict] = []
    for r in sorted_recs:
        key = r.get("title", "")[:30].lower()
        if key not in seen:
            seen.add(key)
            unique.append(r)
    return unique


def _add_roi(recs: List[Dict], annual_cost: float) -> List[Dict]:
    out = []
    for rec in recs:
        pct   = float(rec.get("savings_pct", rec.get("estimated_savings_percent", 5)))
        eff   = rec.get("effort", "medium")
        hours = _EFFORT_H.get(eff, 8)
        impl  = hours * _ENG_RATE
        saves = annual_cost * (pct / 100)
        roi   = ((saves - impl) / max(impl, 1)) * 100
        pb    = impl / max(saves / 12, 0.01)
        out.append({
            **rec,
            "effort_hours":        hours,
            "implementation_cost": round(impl, 0),
            "annual_savings_usd":  round(saves, 0),
            "roi_percent":         round(roi, 1),
            "payback_months":      round(pb, 1),
            "quick_win":           pb < 2 and hours < 8,
        })
    return out


def _roadmap(recs: List[Dict]) -> Dict:
    p0 = [r for r in recs if r.get("priority") == "P0"]
    p1 = [r for r in recs if r.get("priority") == "P1"]
    p2 = [r for r in recs if r.get("priority") == "P2"]
    p3 = [r for r in recs if r.get("priority") in ("P3", None)]

    def phase(name, dur, items):
        return {
            "name":                    name,
            "duration":                dur,
            "actions":                 [r.get("title") for r in items[:5]],
            "expected_savings_percent": sum(r.get("savings_pct", r.get("estimated_savings_percent", 0)) for r in items[:5]),
            "effort_hours":            sum(_EFFORT_H.get(r.get("effort", "medium"), 8) for r in items[:5]),
        }

    return {
        "phase_1": phase("Quick Wins (Week 1–2)",            "2 weeks",   p0),
        "phase_2": phase("Code Optimisation (Week 3–4)",     "2 weeks",   p1),
        "phase_3": phase("Architecture Changes (Month 2–3)", "4–8 weeks", p2),
        "phase_4": phase("Long-term Improvements (Month 3+)","Ongoing",   p3),
    }


def _risks(recs: List[Dict]) -> List[Dict]:
    risks = []
    if any("migrat" in r.get("title", "").lower() for r in recs):
        risks.append({
            "risk":        "Platform migration may cause temporary instability",
            "probability": "medium",
            "impact":      "medium",
            "mitigation":  "Run parallel systems; maintain rollback plan",
        })
    if sum(1 for r in recs if r.get("category") == "code") > 5:
        risks.append({
            "risk":        "Multiple simultaneous code changes may introduce regressions",
            "probability": "medium",
            "impact":      "high",
            "mitigation":  "Implement incrementally with comprehensive integration tests",
        })
    if any("reduc" in r.get("title", "").lower() for r in recs):
        risks.append({
            "risk":        "Resource reduction may impact job SLAs under peak load",
            "probability": "low",
            "impact":      "medium",
            "mitigation":  "Monitor job duration after changes; enable Glue auto-scaling",
        })
    return risks


# ---------------------------------------------------------------------------
@tool
def synthesise_recommendations(all_agent_results_json: str, runs_per_day: int = 1) -> str:
    """
    Synthesise findings from all ETL agents into a prioritised recommendation report.

    Args:
        all_agent_results_json: JSON object keyed by agent name (sizing, data_quality,
                                compliance, code_analyzer, resource_allocator, execution).
        runs_per_day:           Pipeline runs per day (for cost annualisation).

    Returns:
        JSON recommendation report with executive summary, ROI analysis, roadmap, and risks.
    """
    try:
        ctx      = json.loads(all_agent_results_json)
        resource = ctx.get("resource_allocator", {})
        cur_cfg  = resource.get("current_config", {})
        annual   = float(cur_cfg.get("annual_cost", 20_000))
        monthly  = annual / 12

        all_recs  = _consolidate(ctx)
        prioritised = _prioritise(all_recs)
        with_roi  = _add_roi(prioritised, annual)

        # Aggregate savings
        savings_ctx  = resource.get("savings", {})
        savings_pct  = float(savings_ctx.get("percent", 30))
        monthly_save = monthly * (savings_pct / 100)

        exec_summary = {
            "headline":               f"Potential {savings_pct:.0f}% cost reduction identified",
            "current_annual_cost":    f"${annual:,.0f}",
            "potential_annual_savings": f"${annual * savings_pct / 100:,.0f}",
            "critical_issues_count":  sum(1 for r in with_roi if r.get("priority") == "P0"),
            "total_recommendations":  len(with_roi),
            "quick_wins_count":       sum(1 for r in with_roi if r.get("quick_win")),
            "top_findings": [
                f"Effective data size: {(ctx.get('sizing') or {}).get('effective_size_gb', 'N/A')} GB",
                f"Code quality score: {(ctx.get('code_analyzer') or {}).get('optimization_score', 'N/A')}/100",
                f"Anti-patterns found: {(ctx.get('code_analyzer') or {}).get('anti_pattern_count', 0)}",
                f"PII columns detected: {len((ctx.get('compliance') or {}).get('pii_columns_found', []))}",
                f"DQ score: {(ctx.get('data_quality') or {}).get('overall_score', 'N/A')}/100",
                f"Over-provisioned: {(resource.get('resource_efficiency') or {}).get('over_provisioned', False)}",
            ],
        }

        success_metrics = [
            {"metric": "Monthly Compute Cost",  "current": f"${monthly:,.0f}",       "target": f"${monthly - monthly_save:,.0f}", "how": "AWS Cost Explorer"},
            {"metric": "Job Duration (p50)",     "current": "Baseline",               "target": "-20%",                           "how": "CloudWatch + Glue metrics"},
            {"metric": "Resource Utilisation",   "current": "Unknown",                "target": "60–80%",                         "how": "Spark UI / CloudWatch"},
            {"metric": "Anti-pattern Count",     "current": str((ctx.get("code_analyzer") or {}).get("anti_pattern_count", 0)), "target": "0", "how": "Code analysis"},
            {"metric": "DQ Score",               "current": str((ctx.get("data_quality") or {}).get("overall_score", 0)),        "target": "95+", "how": "DQ agent"},
        ]

        return json.dumps({
            "executive_summary":    exec_summary,
            "cost_analysis": {
                "current_monthly_cost":      round(monthly, 2),
                "potential_monthly_savings": round(monthly_save, 2),
                "savings_percent":           savings_pct,
                "confidence":                "high" if savings_pct < 40 else "medium",
            },
            "recommendations":              with_roi,
            "implementation_roadmap":       _roadmap(with_roi),
            "risks":                        _risks(with_roi),
            "success_metrics":              success_metrics,
            "total_implementation_effort_h": sum(r.get("effort_hours", 0) for r in with_roi),
            "expected_payback_months":       round(
                sum(r.get("implementation_cost", 0) for r in with_roi)
                / max(monthly_save, 1), 1
            ),
        })

    except Exception as exc:
        logger.error("Recommendation synthesis failed: %s", exc)
        return json.dumps({"error": str(exc)})


@tool
def generate_implementation_plan(all_agent_results_json: str) -> str:
    """
    Generate a phased implementation plan with effort estimates and success criteria.

    Args:
        all_agent_results_json: Combined JSON from all pipeline agents.

    Returns:
        JSON with phases, tasks, effort_days, owners, success_criteria, and timeline_weeks.
    """
    try:
        results = json.loads(all_agent_results_json) if all_agent_results_json else {}

        code_issues = len(results.get("code_analysis", {}).get("anti_patterns", []))
        pii_issues  = len(results.get("compliance", {}).get("pii_columns", []))
        dq_issues   = len(results.get("data_quality", {}).get("issues", []))
        has_delta   = results.get("delta_iceberg", {}).get("delta_detected", False)

        phases = [
            {
                "phase": 1,
                "name": "Quick Wins (Week 1)",
                "tasks": [
                    {"task": "Enable AQE and KryoSerializer", "effort_days": 0.5,
                     "owner": "Data Engineer", "impact": "high"},
                    {"task": f"Fix {code_issues} anti-patterns (repartition→coalesce, remove UDFs)",
                     "effort_days": max(1, code_issues * 0.25), "owner": "Data Engineer",
                     "impact": "high"},
                ],
                "success_criteria": ["All AQE configs active", "Anti-patterns resolved"],
            },
            {
                "phase": 2,
                "name": "Compliance & Quality (Week 2-3)",
                "tasks": [
                    {"task": f"Mask {pii_issues} PII columns", "effort_days": max(1, pii_issues * 0.5),
                     "owner": "Data Engineer + Legal", "impact": "critical"},
                    {"task": f"Fix {dq_issues} data quality rules", "effort_days": max(1, dq_issues * 0.3),
                     "owner": "Data Engineer", "impact": "high"},
                ],
                "success_criteria": ["PII masked before sink", "DQ score > 90"],
            },
            {
                "phase": 3,
                "name": "Infrastructure (Week 3-4)",
                "tasks": [
                    {"task": "Right-size Glue workers based on allocation report",
                     "effort_days": 0.5, "owner": "Platform Engineer", "impact": "medium"},
                    {"task": "Schedule Delta/Iceberg maintenance jobs" if has_delta else "Add table partitioning",
                     "effort_days": 1.0, "owner": "Platform Engineer", "impact": "medium"},
                ],
                "success_criteria": ["Job cost reduced ≥ 20%", "Maintenance scheduled"],
            },
        ]

        total_days = sum(t["effort_days"] for p in phases for t in p["tasks"])
        return json.dumps({
            "phases":            phases,
            "total_effort_days": round(total_days, 1),
            "timeline_weeks":    4,
            "recommended_start": "Immediately — Phase 1 has no dependencies",
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def prioritise_findings(all_agent_results_json: str) -> str:
    """
    Aggregate findings from all agents and sort by business impact and effort.

    Args:
        all_agent_results_json: Combined JSON from all pipeline agents.

    Returns:
        JSON with prioritised_findings list sorted by priority score.
    """
    try:
        results  = json.loads(all_agent_results_json) if all_agent_results_json else {}
        findings = []

        # Code anti-patterns
        for ap in results.get("code_analysis", {}).get("anti_patterns", []):
            findings.append({
                "source": "CodeAnalyzer", "category": "performance",
                "title": ap.get("pattern", ap.get("type", "anti-pattern")),
                "severity": ap.get("severity", "medium"),
                "effort": "low",
                "priority_score": {"critical": 90, "high": 70, "medium": 50, "low": 30}.get(
                    ap.get("severity", "medium").lower(), 50),
            })

        # PII compliance
        for col in results.get("compliance", {}).get("pii_columns", []):
            findings.append({
                "source": "Compliance", "category": "compliance",
                "title": f"PII column unmasked: {col.get('column', col) if isinstance(col, dict) else col}",
                "severity": "critical", "effort": "medium", "priority_score": 95,
            })

        # Data quality
        for issue in results.get("data_quality", {}).get("issues", []):
            findings.append({
                "source": "DataQuality", "category": "quality",
                "title": issue.get("rule", str(issue)),
                "severity": issue.get("severity", "medium") if isinstance(issue, dict) else "medium",
                "effort": "low", "priority_score": 60,
            })

        findings.sort(key=lambda x: x["priority_score"], reverse=True)
        return json.dumps({
            "prioritised_findings": findings,
            "total_findings":       len(findings),
            "critical_count":       sum(1 for f in findings if f["severity"] == "critical"),
            "high_count":           sum(1 for f in findings if f["severity"] == "high"),
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


def create_recommendation_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                                  region: str = "us-west-2") -> Agent:
    """Return a Strands Agent for recommendation synthesis."""
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[synthesise_recommendations, generate_implementation_plan, prioritise_findings],
    )
