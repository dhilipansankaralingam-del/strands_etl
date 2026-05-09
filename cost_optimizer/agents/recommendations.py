"""
Recommendations Agent
=====================

Synthesizes all agent findings into prioritized, actionable recommendations
with clear ROI and implementation roadmap.
"""

import json
from typing import Dict, List, Any
from datetime import datetime
from .base import CostOptimizerAgent, AnalysisInput, AnalysisResult


class RecommendationsAgent(CostOptimizerAgent):
    """Synthesizes findings into prioritized recommendations."""

    AGENT_NAME = "recommendations"

    # Engineering cost for ROI calculation
    ENGINEERING_HOURLY_RATE = 150  # USD

    # Effort estimates in hours
    EFFORT_ESTIMATES = {
        'low': 2,
        'medium': 8,
        'high': 24,
        'very_high': 80
    }

    def _build_llm_prompt(self, input_data: AnalysisInput, context: Dict) -> str:
        """Holistic synthesis prompt — connects findings from all 4 agents."""

        size_full     = context.get('size_analyzer_full',  {})
        code_full     = context.get('code_analyzer_full',  {})
        resource_full = context.get('resource_allocator',  {})
        glue_metrics  = context.get('glue_metrics',        {})

        # Compact size summary for prompt efficiency
        table_findings = size_full.get('table_findings', {})
        size_summary: List[str] = []
        for tbl, findings in table_findings.items():
            fs   = findings.get('file_stats', {})
            ih   = findings.get('iceberg_health', {})
            gf   = findings.get('growth_forecast', {})
            sc   = findings.get('storage_cost', {})
            cold = findings.get('cold_partition_analysis', {})
            size_summary.append(
                f"  {tbl}: {fs.get('total_size_gb',0):.2f} GB | "
                f"files={fs.get('file_count',0):,} avg={fs.get('avg_file_mb',0):.1f} MB | "
                f"tiny={fs.get('tiny_file_count',0):,} skew={fs.get('skew_ratio',1):.1f}x | "
                f"snapshots={ih.get('snapshot_count',0)} write_amp={ih.get('write_amplification',1):.1f}x | "
                f"growth={gf.get('gb_per_day',0):.2f} GB/day 90d={gf.get('projected_90d_gb',0):.0f} GB | "
                f"live=${sc.get('live_monthly_usd',0):.2f} dead=${sc.get('dead_snapshot_monthly_usd',0):.2f}/mo | "
                f"cold={cold.get('cold_data_estimate_gb',0):.0f} GB "
                f"savings=${cold.get('glacier_monthly_savings_usd',0):.2f}/mo"
            )
        size_ctx = "\n".join(size_summary) if size_summary else "  (none)"

        # Code findings summary
        anti_patterns = code_full.get('anti_patterns', [])
        code_summary = {
            "critical_issues": code_full.get('critical_issues', 0),
            "optimization_score": code_full.get('optimization_score', 0),
            "anti_patterns": [{"pattern": p["pattern"], "severity": p["severity"],
                                "lines": p.get("line_numbers", [])}
                               for p in anti_patterns],
            "iceberg_specific": code_full.get('iceberg_specific', []),
            "top_expensive_line": code_full.get('top_expensive_line', {}),
            "line_hotspots": code_full.get('line_hotspots', []),
        }

        # Glue metrics summary
        metrics_summary: List[str] = []
        if glue_metrics:
            for metric, vals in glue_metrics.items():
                if vals:
                    metrics_summary.append(
                        f"  {metric}: min={min(vals):.2f} max={max(vals):.2f} "
                        f"trend={'↑' if vals[-1] > vals[0] else '↓'} last={vals[-1]:.2f}"
                    )
        metrics_ctx = "\n".join(metrics_summary) if metrics_summary else "  (none)"

        # Worker config
        workers      = input_data.current_config.get('workers', '?')
        worker_type  = input_data.current_config.get('worker_type', 'G.2X')
        runs_per_day = input_data.additional_context.get('runs_per_day', 1)

        # Rule-based pre-computed recs as starting point
        rule_result = self._analyze_rule_based(input_data, context)
        rule_recs_brief = [
            {"priority": r.get("priority"), "title": r.get("title"),
             "savings_pct": r.get("estimated_savings_percent", 0),
             "source": r.get("source", "rule")}
            for r in rule_result.recommendations[:20]
        ]

        return f"""You are a senior AWS cost-optimization architect.
Synthesize findings from 4 analysis agents into a **holistic, prioritized** optimization plan.
Your unique value is detecting COMPOUND issues that no single agent sees alone.

══════════════════════════════════════════════════════════════
JOB METADATA
══════════════════════════════════════════════════════════════
Script : {input_data.script_path}
Job    : {input_data.job_name}
Mode   : {input_data.processing_mode}
Workers: {workers} × {worker_type}  |  runs/day: {runs_per_day}

══════════════════════════════════════════════════════════════
AGENT 1 — SIZE ANALYZER  (Iceberg $files telemetry)
══════════════════════════════════════════════════════════════
{size_ctx}

Cross-table redundancy:
{json.dumps(size_full.get('cross_table_redundancy', {}), indent=2)[:1000]}

Aggregate storage:
{json.dumps(size_full.get('aggregate_storage_costs', {}), indent=2)}

══════════════════════════════════════════════════════════════
AGENT 2 — CODE ANALYZER  (PySpark line-by-line analysis)
══════════════════════════════════════════════════════════════
{json.dumps(code_summary, indent=2)[:4000]}

══════════════════════════════════════════════════════════════
AGENT 3 — GLUE RUNTIME METRICS  (CloudWatch)
══════════════════════════════════════════════════════════════
{metrics_ctx}

══════════════════════════════════════════════════════════════
AGENT 4 — RESOURCE ALLOCATOR
══════════════════════════════════════════════════════════════
{json.dumps(resource_full, indent=2)[:2000]}

══════════════════════════════════════════════════════════════
RULE-BASED PRE-COMPUTED RECOMMENDATIONS  (starting point)
══════════════════════════════════════════════════════════════
{json.dumps(rule_recs_brief, indent=2)}

══════════════════════════════════════════════════════════════
SYNTHESIS GUIDELINES — COMPOUND ISSUES TO DETECT
══════════════════════════════════════════════════════════════

Connect signals ACROSS agents. Each compound issue below amplifies the impact:

1. [SIZE + CODE] Tiny files + broadcast join
   → code has broadcast hint on a table with 48k tiny files
   → driver loads 48k× small network fetches instead of one file stream
   → Fix: OPTIMIZE table FIRST, then broadcast is valid

2. [METRICS + SIZE] Heap peak > 90% + write_amplification > 5×
   → job is loading historical snapshot data not yet expired
   → executor holds dead snapshot rows in shuffle memory
   → Fix: expire_snapshots() + VACUUM before job run

3. [METRICS + CODE] Worker utilization drops mid-job + skew_ratio > 5×
   → partition skew leaves 90% of workers idle waiting for 1 straggler
   → Fix: df.repartition(n, skewed_col) + AQE skewJoin enabled

4. [SIZE + CODE] Snapshot bloat (>30) + no incremental read in code
   → every job run re-scans all historical data
   → Fix: use startSnapshotId for incremental reads = 80-95% I/O reduction

5. [SIZE + SIZE] Cross-table redundancy + code reads both tables
   → paying double storage + double compute for same data
   → Fix: deprecate one table, update all downstream consumers

6. [SIZE + CODE] Cold partitions > 90 days + no date pushdown filter in code
   → full scan includes cold data that never changes
   → Fix: add WHERE partition_col >= current_date - 90 AND archive to Glacier

7. [CODE + METRICS] Multiple collect()/toPandas() + heap > 80%
   → driver aggregates all data locally while also running shuffle
   → compound OOM: shuffle buffer + collected rows contest same memory

8. [SIZE + CODE + METRICS] Write amplification > 8× + coalesce(1) in code + disk spill in metrics
   → code writes all output to 1 file, Iceberg creates 1 manifest per write
   → Fix: remove coalesce(1), add OPTIMIZE after job

PRIORITY RULES:
  P0 = job will fail or OOM without this fix
  P1 = wastes > 20% monthly cost, fix this sprint
  P2 = 10–20% savings, schedule within quarter
  P3 = < 10% savings or architectural, longer term
  P4 = informational / monitoring

AWS PRICING (for ROI):
  G.1X=$0.44/hr  G.2X=$0.88/hr  G.4X=$1.76/hr  G.8X=$3.52/hr
  S3 standard=$0.023/GB/month   Glacier Deep Archive=$0.004/GB/month

Respond ONLY with a JSON object:
{{
  "executive_summary": {{
    "headline": "...",
    "current_monthly_cost_usd": <float>,
    "potential_monthly_savings_usd": <float>,
    "savings_percent": <float>,
    "critical_issues_count": <int>,
    "compound_issues_found": ["brief description of each compound issue detected"],
    "top_savings_opportunity": "single sentence"
  }},
  "cost_analysis": {{
    "current_monthly_cost": <float>,
    "potential_monthly_savings": <float>,
    "potential_annual_savings": <float>,
    "savings_percent": <float>,
    "confidence_level": "high|medium|low"
  }},
  "recommendations": [
    {{
      "priority": "P0|P1|P2|P3|P4",
      "category": "code|config|iceberg|storage|workers|architecture",
      "title": "...",
      "description": "...",
      "root_cause": "which agent signal(s) triggered this — be specific",
      "implementation": "exact SQL / PySpark / config value",
      "lines": [<int>],
      "estimated_savings_percent": <int>,
      "effort": "low|medium|high",
      "quick_win": <bool>,
      "compound_with": ["other rec titles that amplify this fix when combined"]
    }}
  ],
  "recommendation_count_by_priority": {{"P0": 0, "P1": 0, "P2": 0, "P3": 0, "P4": 0}},
  "implementation_roadmap": {{
    "phase_1": {{
      "name": "Quick Wins (Week 1–2)",
      "actions": ["..."],
      "expected_savings_percent": <int>,
      "effort_hours": <int>
    }},
    "phase_2": {{
      "name": "Code + Iceberg Fixes (Week 3–4)",
      "actions": ["..."],
      "expected_savings_percent": <int>,
      "effort_hours": <int>
    }},
    "phase_3": {{
      "name": "Architecture & Storage (Month 2–3)",
      "actions": ["..."],
      "expected_savings_percent": <int>,
      "effort_hours": <int>
    }}
  }},
  "risks": [
    {{"risk": "...", "probability": "low|medium|high", "mitigation": "..."}}
  ],
  "success_metrics": [
    {{"metric": "...", "current_value": "...", "target_value": "...", "measurement": "..."}}
  ]
}}
"""

    def _analyze_rule_based(self, input_data: AnalysisInput, context: Dict) -> AnalysisResult:
        """Synthesize all agent findings into recommendations."""

        # Extract findings from other agents
        size_analysis = context.get('size_analyzer', {})
        code_analysis = context.get('code_analyzer', {})
        resource_analysis = context.get('resource_allocator', {})

        # Calculate current and potential costs
        current_cost = self._calculate_current_cost(context, input_data)
        potential_savings = self._calculate_potential_savings(context)

        # Consolidate all recommendations
        all_recommendations = self._consolidate_recommendations(context)

        # Prioritize and deduplicate
        prioritized = self._prioritize_recommendations(all_recommendations)

        # Create implementation roadmap
        roadmap = self._create_roadmap(prioritized)

        # Calculate ROI for each recommendation
        recommendations_with_roi = self._calculate_roi(prioritized, current_cost, input_data)

        # Generate executive summary
        executive_summary = self._generate_executive_summary(
            context, current_cost, potential_savings, prioritized
        )

        # Risk assessment
        risks = self._assess_risks(prioritized)

        # Success metrics
        success_metrics = self._define_success_metrics(context, potential_savings)

        analysis = {
            'executive_summary': executive_summary,
            'cost_analysis': {
                'current_monthly_cost': round(current_cost['monthly'], 2),
                'current_annual_cost': round(current_cost['annual'], 2),
                'potential_monthly_savings': round(potential_savings['monthly'], 2),
                'potential_annual_savings': round(potential_savings['annual'], 2),
                'savings_percent': round(potential_savings['percent'], 1),
                'confidence_level': potential_savings['confidence']
            },
            'recommendations': recommendations_with_roi,
            'recommendation_count_by_priority': self._count_by_priority(prioritized),
            'implementation_roadmap': roadmap,
            'risks': risks,
            'success_metrics': success_metrics,
            'total_implementation_effort_hours': sum(
                r.get('effort_hours', 0) for r in recommendations_with_roi
            ),
            'total_implementation_cost': sum(
                r.get('implementation_cost', 0) for r in recommendations_with_roi
            ),
            'expected_payback_months': self._calculate_payback(
                recommendations_with_roi, potential_savings
            )
        }

        return AnalysisResult(
            agent_name=self.AGENT_NAME,
            success=True,
            analysis=analysis,
            recommendations=recommendations_with_roi,
            metrics={
                'total_recommendations': len(recommendations_with_roi),
                'p0_count': analysis['recommendation_count_by_priority'].get('P0', 0),
                'potential_annual_savings': potential_savings['annual'],
                'savings_percent': potential_savings['percent']
            }
        )

    def _calculate_current_cost(self, context: Dict, input_data: AnalysisInput) -> Dict:
        """Calculate current running cost."""
        resource = context.get('resource_allocator', {})
        current_config = resource.get('current_config', {})

        cost_per_run = current_config.get('cost_per_run', 50)
        runs_per_day = input_data.additional_context.get('runs_per_day', 1)

        return {
            'per_run': cost_per_run,
            'daily': cost_per_run * runs_per_day,
            'monthly': cost_per_run * runs_per_day * 30,
            'annual': cost_per_run * runs_per_day * 365
        }

    def _calculate_potential_savings(self, context: Dict) -> Dict:
        """Calculate potential savings from all optimizations."""
        resource = context.get('resource_allocator', {})
        code = context.get('code_analyzer', {})

        # Resource savings
        resource_savings_pct = resource.get('savings', {}).get('percent', 0)

        # Code optimization savings
        code_savings_pct = code.get('estimated_cost_reduction_percent', 0)

        # Combined savings (not additive - use diminishing returns)
        combined_pct = resource_savings_pct + (code_savings_pct * (1 - resource_savings_pct / 100))
        combined_pct = min(80, combined_pct)  # Cap at 80%

        # Get current costs
        current = resource.get('current_config', {})
        annual_current = current.get('annual_cost', 20000)

        annual_savings = annual_current * (combined_pct / 100)

        return {
            'percent': combined_pct,
            'monthly': annual_savings / 12,
            'annual': annual_savings,
            'confidence': 'high' if combined_pct < 40 else 'medium' if combined_pct < 60 else 'low'
        }

    def _consolidate_recommendations(self, context: Dict) -> List[Dict]:
        """Consolidate recommendations from all agents."""
        all_recs = []

        # Size analyzer recommendations
        size_recs = context.get('size_analyzer', {}).get('recommendations', [])
        for rec in size_recs:
            rec['source'] = 'size_analyzer'
            all_recs.append(rec)

        # Code analyzer recommendations
        code_recs = context.get('code_analyzer', {}).get('recommendations', [])
        for rec in code_recs:
            rec['source'] = 'code_analyzer'
            all_recs.append(rec)

        # Resource allocator recommendations
        resource_recs = context.get('resource_allocator', {}).get('recommendations', [])
        for rec in resource_recs:
            rec['source'] = 'resource_allocator'
            all_recs.append(rec)

        return all_recs

    def _prioritize_recommendations(self, recommendations: List[Dict]) -> List[Dict]:
        """Prioritize and deduplicate recommendations."""
        # Define priority order
        priority_order = {'P0': 0, 'P1': 1, 'P2': 2, 'P3': 3}

        # Sort by priority, then by estimated savings
        sorted_recs = sorted(
            recommendations,
            key=lambda x: (
                priority_order.get(x.get('priority', 'P3'), 3),
                -x.get('estimated_savings_percent', 0)
            )
        )

        # Deduplicate similar recommendations
        seen_titles = set()
        unique_recs = []
        for rec in sorted_recs:
            title_key = rec.get('title', '').lower()[:30]
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                unique_recs.append(rec)

        return unique_recs

    def _calculate_roi(
        self, recommendations: List[Dict], current_cost: Dict, input_data: AnalysisInput
    ) -> List[Dict]:
        """Calculate ROI for each recommendation."""
        enhanced_recs = []

        for rec in recommendations:
            savings_pct = rec.get('estimated_savings_percent', 5)
            effort = rec.get('effort', 'medium')

            # Calculate effort hours
            effort_hours = self.EFFORT_ESTIMATES.get(effort, 8)

            # Implementation cost
            impl_cost = effort_hours * self.ENGINEERING_HOURLY_RATE

            # Annual savings
            annual_savings = current_cost['annual'] * (savings_pct / 100)

            # ROI percentage
            roi_pct = ((annual_savings - impl_cost) / max(impl_cost, 1)) * 100

            # Payback period in months
            monthly_savings = annual_savings / 12
            payback_months = impl_cost / max(monthly_savings, 1)

            enhanced_rec = {
                **rec,
                'effort_hours': effort_hours,
                'implementation_cost': round(impl_cost, 2),
                'annual_savings_usd': round(annual_savings, 2),
                'roi_percent': round(roi_pct, 1),
                'payback_months': round(payback_months, 1),
                'quick_win': payback_months < 2 and effort_hours < 8
            }

            enhanced_recs.append(enhanced_rec)

        return enhanced_recs

    def _create_roadmap(self, recommendations: List[Dict]) -> Dict:
        """Create implementation roadmap."""
        p0_recs = [r for r in recommendations if r.get('priority') == 'P0']
        p1_recs = [r for r in recommendations if r.get('priority') == 'P1']
        p2_recs = [r for r in recommendations if r.get('priority') == 'P2']
        p3_recs = [r for r in recommendations if r.get('priority') == 'P3']

        return {
            'phase_1': {
                'name': 'Quick Wins (Week 1-2)',
                'duration': '2 weeks',
                'actions': [r.get('title') for r in p0_recs[:5]],
                'expected_savings_percent': sum(r.get('estimated_savings_percent', 0) for r in p0_recs[:5]),
                'effort_hours': sum(self.EFFORT_ESTIMATES.get(r.get('effort', 'low'), 2) for r in p0_recs[:5])
            },
            'phase_2': {
                'name': 'Code Optimization (Week 3-4)',
                'duration': '2 weeks',
                'actions': [r.get('title') for r in p1_recs[:5]],
                'expected_savings_percent': sum(r.get('estimated_savings_percent', 0) for r in p1_recs[:5]),
                'effort_hours': sum(self.EFFORT_ESTIMATES.get(r.get('effort', 'medium'), 8) for r in p1_recs[:5])
            },
            'phase_3': {
                'name': 'Architecture Changes (Month 2-3)',
                'duration': '4-8 weeks',
                'actions': [r.get('title') for r in p2_recs[:5]],
                'expected_savings_percent': sum(r.get('estimated_savings_percent', 0) for r in p2_recs[:5]),
                'effort_hours': sum(self.EFFORT_ESTIMATES.get(r.get('effort', 'high'), 24) for r in p2_recs[:5])
            },
            'phase_4': {
                'name': 'Long-term Improvements (Month 3+)',
                'duration': 'Ongoing',
                'actions': [r.get('title') for r in p3_recs[:5]],
                'expected_savings_percent': sum(r.get('estimated_savings_percent', 0) for r in p3_recs[:5]),
                'effort_hours': sum(self.EFFORT_ESTIMATES.get(r.get('effort', 'high'), 24) for r in p3_recs[:5])
            }
        }

    def _generate_executive_summary(
        self, context: Dict, current_cost: Dict, potential_savings: Dict, recommendations: List[Dict]
    ) -> Dict:
        """Generate executive summary."""
        # Top 3 findings
        critical_issues = sum(
            1 for r in recommendations if r.get('priority') == 'P0'
        )

        # Get key metrics
        size = context.get('size_analyzer', {})
        code = context.get('code_analyzer', {})
        resource = context.get('resource_allocator', {})

        return {
            'headline': f"Potential {potential_savings['percent']:.0f}% cost reduction identified",
            'current_annual_spend': f"${current_cost['annual']:,.0f}",
            'potential_annual_savings': f"${potential_savings['annual']:,.0f}",
            'critical_issues_count': critical_issues,
            'total_recommendations': len(recommendations),
            'quick_wins_count': sum(1 for r in recommendations if r.get('effort') == 'low'),
            'top_findings': [
                f"Data size: {size.get('effective_size_gb', 0):.0f} GB effective",
                f"Code optimization score: {code.get('optimization_score', 0)}/100",
                f"Resource efficiency: {resource.get('savings', {}).get('percent', 0):.0f}% over-provisioned",
                f"Anti-patterns found: {code.get('anti_pattern_count', 0)}",
                f"Best platform: {resource.get('optimal_config', {}).get('platform', 'unknown')}"
            ][:5]
        }

    def _assess_risks(self, recommendations: List[Dict]) -> List[Dict]:
        """Assess risks of implementing recommendations."""
        risks = []

        # Check for platform migration
        platform_recs = [r for r in recommendations if 'migrate' in r.get('title', '').lower()]
        if platform_recs:
            risks.append({
                'risk': 'Platform migration may cause temporary instability',
                'probability': 'medium',
                'impact': 'medium',
                'mitigation': 'Run parallel systems during migration, maintain rollback plan'
            })

        # Check for code changes
        code_recs = [r for r in recommendations if r.get('category') == 'code']
        if len(code_recs) > 5:
            risks.append({
                'risk': 'Multiple code changes may introduce bugs',
                'probability': 'medium',
                'impact': 'high',
                'mitigation': 'Implement changes incrementally, comprehensive testing'
            })

        # Resource reduction risk
        resource_recs = [r for r in recommendations if 'reduce' in r.get('title', '').lower()]
        if resource_recs:
            risks.append({
                'risk': 'Resource reduction may impact job stability',
                'probability': 'low',
                'impact': 'medium',
                'mitigation': 'Monitor job metrics closely after changes, have auto-scaling ready'
            })

        return risks

    def _define_success_metrics(self, context: Dict, potential_savings: Dict) -> List[Dict]:
        """Define success metrics for optimization initiative."""
        resource = context.get('resource_allocator', {})
        current = resource.get('current_config', {})

        return [
            {
                'metric': 'Monthly Compute Cost',
                'current_value': f"${current.get('annual_cost', 0) / 12:,.0f}",
                'target_value': f"${(current.get('annual_cost', 0) * (1 - potential_savings['percent'] / 100)) / 12:,.0f}",
                'measurement': 'AWS Cost Explorer, Glue job metrics'
            },
            {
                'metric': 'Job Duration (p50)',
                'current_value': 'Baseline TBD',
                'target_value': '-20% from baseline',
                'measurement': 'CloudWatch metrics, Glue job history'
            },
            {
                'metric': 'Resource Utilization',
                'current_value': resource.get('resource_efficiency', {}).get('memory_utilization_estimate', 'unknown'),
                'target_value': '60-80% utilization',
                'measurement': 'Spark UI, CloudWatch metrics'
            },
            {
                'metric': 'Anti-patterns Count',
                'current_value': str(context.get('code_analyzer', {}).get('anti_pattern_count', 0)),
                'target_value': '0',
                'measurement': 'Code analysis tool'
            }
        ]

    def _count_by_priority(self, recommendations: List[Dict]) -> Dict:
        """Count recommendations by priority."""
        counts = {'P0': 0, 'P1': 0, 'P2': 0, 'P3': 0}
        for rec in recommendations:
            priority = rec.get('priority', 'P3')
            counts[priority] = counts.get(priority, 0) + 1
        return counts

    def _calculate_payback(self, recommendations: List[Dict], savings: Dict) -> float:
        """Calculate overall payback period in months."""
        total_impl_cost = sum(r.get('implementation_cost', 0) for r in recommendations)
        monthly_savings = savings.get('monthly', 1)
        return round(total_impl_cost / max(monthly_savings, 1), 1)
