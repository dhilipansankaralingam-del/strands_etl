#!/usr/bin/env python3
"""
Multi-Config ETL Optimizer - Agent-based batch analysis.

Uses framework agents:
- code_analysis_agent: Analyzes ETL code patterns
- sizing_agent: Resource sizing recommendations
- resource_allocator_agent: Platform cost analysis
- platform_conversion_agent: Cross-platform recommendations
- recommendation_agent: Aggregates all recommendations

Usage:
    python scripts/multi_config_optimizer.py --source ./demo_configs/ --dest ./reports/
"""

import argparse
import json
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from framework.strands.orchestrator import StrandsOrchestrator
from framework.strands.base_agent import AgentContext, AgentStatus


# Cross-platform cost reference ($/hr per compute unit)
PLATFORM_COSTS = {
    'aws_glue': {'cost': 0.44, 'provider': 'aws'},
    'aws_emr': {'cost': 0.25, 'provider': 'aws'},
    'aws_emr_serverless': {'cost': 0.36, 'provider': 'aws'},
    'gcp_dataproc': {'cost': 0.20, 'provider': 'gcp'},
    'gcp_dataproc_serverless': {'cost': 0.30, 'provider': 'gcp'},
    'gcp_dataflow': {'cost': 0.28, 'provider': 'gcp'},
    'azure_synapse': {'cost': 0.38, 'provider': 'azure'},
    'azure_databricks': {'cost': 0.40, 'provider': 'azure'},
    'databricks': {'cost': 0.45, 'provider': 'independent'},
    'snowflake': {'cost': 0.50, 'provider': 'independent'},
    'spark_k8s': {'cost': 0.15, 'provider': 'independent'},
}


class MultiConfigOptimizer:
    """Agent-based multi-config optimizer."""

    def __init__(self, source_dir: str, dest_dir: str):
        self.source_path = Path(source_dir)
        self.dest_path = Path(dest_dir)
        self.dest_path.mkdir(parents=True, exist_ok=True)
        self.results: List[Dict] = []

    def run(self) -> Dict[str, Any]:
        """Run agent-based analysis on all configs."""
        configs = list(self.source_path.glob('*.json'))
        print(f"\n{'='*60}")
        print(f" Multi-Config ETL Optimizer (Agent-Based)")
        print(f" Found {len(configs)} config(s)")
        print(f"{'='*60}\n")

        for cfg_path in configs:
            print(f"  Processing: {cfg_path.name}")
            result = self._analyze_with_agents(cfg_path)
            self.results.append(result)

        summary = self._build_summary()
        self._save_reports(summary)
        return summary

    def _analyze_with_agents(self, cfg_path: Path) -> Dict[str, Any]:
        """Run framework agents on a single config."""
        with open(cfg_path) as f:
            config = json.load(f)

        job_name = config.get('job_name', cfg_path.stem)
        execution_id = str(uuid.uuid4())[:8]

        # Build agent config from input config
        agent_config = self._build_agent_config(config)

        # Initialize orchestrator with agents enabled
        orchestrator = StrandsOrchestrator(config=agent_config)

        # Execute agents
        try:
            orch_result = orchestrator.execute(
                job_name=job_name,
                run_date=datetime.utcnow(),
                platform=config.get('platform', 'glue'),
                use_llm=False
            )

            # Extract agent outputs
            agent_outputs = {}
            recommendations = []

            for agent_name, agent_result in orch_result.agent_results.items():
                agent_outputs[agent_name] = {
                    'status': agent_result.status.value,
                    'output': agent_result.output,
                    'metrics': agent_result.metrics
                }
                recommendations.extend(agent_result.recommendations)

            # Add cross-platform analysis
            platform_analysis = self._analyze_platforms(config, agent_outputs)

            return {
                'job_name': job_name,
                'config_path': str(cfg_path),
                'execution_id': execution_id,
                'timestamp': datetime.utcnow().isoformat(),
                'status': orch_result.status,
                'agents_run': list(agent_outputs.keys()),
                'agent_outputs': agent_outputs,
                'recommendations': recommendations,
                'platform_analysis': platform_analysis,
                'total_time_ms': orch_result.total_time_ms
            }

        except Exception as e:
            print(f"    Error: {e}")
            return {
                'job_name': job_name,
                'config_path': str(cfg_path),
                'execution_id': execution_id,
                'status': 'error',
                'error': str(e),
                'platform_analysis': self._analyze_platforms(config, {})
            }

    def _build_agent_config(self, input_config: Dict) -> Dict:
        """Convert input config to agent config format."""
        current = input_config.get('current_config', {})
        tables = input_config.get('source_tables', [])

        # Calculate data metrics
        total_size_gb = sum(
            t.get('size_gb', t.get('estimated_rows', 1_000_000) / 2_000_000)
            for t in tables
        )
        total_rows = sum(t.get('estimated_rows', 1_000_000) for t in tables)

        return {
            'job_name': input_config.get('job_name', 'unknown'),
            'script_path': input_config.get('script_path', ''),

            # Sizing agent config
            'sizing': {
                'enabled': 'Y',
                'total_size_gb': total_size_gb,
                'row_count': total_rows,
                'current_workers': current.get('NumberOfWorkers', 10),
                'worker_type': current.get('WorkerType', 'G.1X'),
            },

            # Code analysis config
            'code_analysis': {
                'enabled': 'Y',
                'script_path': input_config.get('script_path', ''),
                'check_anti_patterns': 'Y',
                'check_optimizations': 'Y',
            },

            # Resource allocator config
            'resource_allocator': {
                'enabled': 'Y',
                'monthly_runs': input_config.get('monthly_runs', 30),
                'avg_runtime_hours': input_config.get('avg_runtime_hours', 0.5),
            },

            # Platform conversion config
            'platform_conversion': {
                'enabled': 'Y',
                'source_platform': input_config.get('platform', 'glue'),
                'analyze_alternatives': 'Y',
            },

            # Compliance config
            'compliance': {
                'enabled': 'Y',
            },

            # Recommendation aggregation
            'recommendation': {
                'enabled': 'Y',
                'prioritize_by_impact': 'Y',
                'generate_implementation_plan': 'Y',
            },

            # Pass through source tables
            'source_tables': tables,
            'current_config': current,
        }

    def _analyze_platforms(self, config: Dict, agent_outputs: Dict) -> Dict:
        """Analyze cross-platform options with cost estimates."""
        current = config.get('current_config', {})
        workers = current.get('NumberOfWorkers', 10)
        monthly_runs = config.get('monthly_runs', 30)
        runtime_hrs = config.get('avg_runtime_hours', 0.5)

        # Current cost (assume Glue)
        current_cost = workers * 0.44 * runtime_hrs * monthly_runs

        # Get sizing recommendations from agents if available
        sizing_output = agent_outputs.get('sizing_agent', {}).get('output', {})
        recommended_workers = sizing_output.get('recommended_workers', workers)

        platforms = []
        for name, info in PLATFORM_COSTS.items():
            # Efficiency factor by platform
            efficiency = {
                'databricks': 0.7, 'snowflake': 0.5, 'gcp_dataproc': 0.85,
                'spark_k8s': 0.8, 'aws_emr': 0.9
            }.get(name, 1.0)

            adj_workers = max(2, int(recommended_workers * efficiency))
            cost = adj_workers * info['cost'] * runtime_hrs * monthly_runs
            savings = ((current_cost - cost) / current_cost * 100) if current_cost else 0

            platforms.append({
                'platform': name,
                'provider': info['provider'],
                'monthly_cost': round(cost, 2),
                'savings_percent': round(savings, 1),
                'workers_needed': adj_workers
            })

        # Sort by savings
        platforms.sort(key=lambda x: x['savings_percent'], reverse=True)

        return {
            'current_monthly_cost': round(current_cost, 2),
            'current_platform': config.get('platform', 'aws_glue'),
            'alternatives': platforms[:5],  # Top 5
            'best_option': platforms[0] if platforms else None
        }

    def _build_summary(self) -> Dict[str, Any]:
        """Build overall summary from all results."""
        total_current = sum(
            r.get('platform_analysis', {}).get('current_monthly_cost', 0)
            for r in self.results
        )

        potential_savings = sum(
            r.get('platform_analysis', {}).get('current_monthly_cost', 0) -
            r.get('platform_analysis', {}).get('best_option', {}).get('monthly_cost', 0)
            for r in self.results
            if r.get('platform_analysis', {}).get('best_option')
        )

        # Aggregate recommendations by priority
        all_recs = []
        for r in self.results:
            for rec in r.get('recommendations', []):
                all_recs.append({'job': r['job_name'], 'recommendation': rec})

        # Count platform recommendations
        platform_counts = {}
        for r in self.results:
            best = r.get('platform_analysis', {}).get('best_option', {}).get('platform')
            if best:
                platform_counts[best] = platform_counts.get(best, 0) + 1

        return {
            'generated_at': datetime.utcnow().isoformat(),
            'total_jobs': len(self.results),
            'successful_analyses': sum(1 for r in self.results if r.get('status') != 'error'),
            'total_current_monthly_cost': round(total_current, 2),
            'potential_monthly_savings': round(potential_savings, 2),
            'savings_percent': round(potential_savings / total_current * 100, 1) if total_current else 0,
            'jobs': self.results,
            'all_recommendations': all_recs[:20],  # Top 20
            'platform_recommendation_counts': platform_counts
        }

    def _save_reports(self, summary: Dict):
        """Save JSON and HTML reports."""
        ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

        # JSON
        json_path = self.dest_path / f"agent_optimization_{ts}.json"
        with open(json_path, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"\n  JSON: {json_path}")

        # HTML
        html_path = self.dest_path / f"agent_optimization_{ts}.html"
        html = self._generate_html(summary)
        with open(html_path, 'w') as f:
            f.write(html)
        print(f"  HTML: {html_path}")

    def _generate_html(self, summary: Dict) -> str:
        """Generate HTML report."""
        html = f"""<!DOCTYPE html>
<html><head><title>Agent-Based ETL Optimization</title>
<style>
body {{ font-family: -apple-system, sans-serif; margin: 20px; background: #f5f5f5; }}
.container {{ max-width: 1200px; margin: 0 auto; }}
.card {{ background: white; border-radius: 8px; padding: 20px; margin: 15px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
h1 {{ color: #1a1a1a; }}
.metrics {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; }}
.metric {{ text-align: center; padding: 20px; background: linear-gradient(135deg, #667eea, #764ba2); color: white; border-radius: 8px; }}
.metric .value {{ font-size: 2em; font-weight: bold; }}
table {{ width: 100%; border-collapse: collapse; }}
th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #eee; }}
th {{ background: #f8f9fa; }}
.savings {{ color: #28a745; font-weight: bold; }}
.tag {{ display: inline-block; padding: 3px 8px; border-radius: 4px; font-size: 0.8em; }}
.tag-aws {{ background: #ff9900; color: white; }}
.tag-gcp {{ background: #4285f4; color: white; }}
.tag-azure {{ background: #0078d4; color: white; }}
.tag-independent {{ background: #6c757d; color: white; }}
</style></head>
<body><div class="container">
<h1>Agent-Based ETL Optimization Report</h1>
<p>Generated: {summary['generated_at']} | Agents: sizing, code_analysis, resource_allocator, platform_conversion, recommendation</p>

<div class="metrics">
  <div class="metric"><div class="value">{summary['total_jobs']}</div><div>Jobs Analyzed</div></div>
  <div class="metric"><div class="value">${summary['total_current_monthly_cost']:,.0f}</div><div>Current Monthly</div></div>
  <div class="metric"><div class="value">${summary['potential_monthly_savings']:,.0f}</div><div>Potential Savings</div></div>
  <div class="metric"><div class="value">{summary['savings_percent']:.0f}%</div><div>Savings Rate</div></div>
</div>

<div class="card">
<h2>Job Analysis Summary</h2>
<table>
<tr><th>Job</th><th>Agents Run</th><th>Current Cost</th><th>Best Platform</th><th>Savings</th></tr>
"""
        for job in summary['jobs']:
            pa = job.get('platform_analysis', {})
            best = pa.get('best_option', {})
            provider = best.get('provider', 'aws')
            agents = len(job.get('agents_run', []))
            html += f"""<tr>
<td><strong>{job['job_name']}</strong></td>
<td>{agents} agents</td>
<td>${pa.get('current_monthly_cost', 0):,.0f}</td>
<td><span class="tag tag-{provider}">{best.get('platform', '-')}</span></td>
<td class="savings">{best.get('savings_percent', 0):.0f}%</td>
</tr>"""

        html += """</table></div>

<div class="card">
<h2>Top Recommendations from Agents</h2>
<ul>"""
        for rec in summary.get('all_recommendations', [])[:15]:
            html += f"<li><strong>{rec['job']}</strong>: {rec['recommendation']}</li>"

        html += """</ul></div>

<div class="card">
<h2>Platform Recommendation Distribution</h2>
<table><tr><th>Platform</th><th>Jobs Recommended</th></tr>"""
        for plat, count in summary.get('platform_recommendation_counts', {}).items():
            html += f"<tr><td>{plat}</td><td>{count}</td></tr>"

        html += "</table></div></div></body></html>"
        return html


def main():
    parser = argparse.ArgumentParser(description='Agent-Based Multi-Config Optimizer')
    parser.add_argument('--source', '-s', required=True, help='Directory with config JSONs')
    parser.add_argument('--dest', '-d', required=True, help='Output directory')
    args = parser.parse_args()

    optimizer = MultiConfigOptimizer(args.source, args.dest)
    summary = optimizer.run()

    print(f"\n{'='*60}")
    print(f" {summary['total_jobs']} jobs | ${summary['total_current_monthly_cost']:,.0f}/mo current")
    print(f" Potential: ${summary['potential_monthly_savings']:,.0f}/mo savings ({summary['savings_percent']:.0f}%)")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
