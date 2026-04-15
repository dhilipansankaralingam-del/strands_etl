#!/usr/bin/env python3
"""
Multi-Config ETL Optimizer - Batch analysis with cross-platform recommendations.

Usage:
    python scripts/multi_config_optimizer.py --source ./demo_configs/ --dest ./reports/
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
from dataclasses import dataclass, field, asdict

sys.path.insert(0, str(Path(__file__).parent.parent))


@dataclass
class PlatformOption:
    name: str
    provider: str  # aws, gcp, azure, independent
    cost_per_dpu_hour: float
    min_workers: int
    max_workers: int
    strengths: List[str]
    best_for: List[str]
    migration_effort: str  # low, medium, high


# Cross-platform catalog
PLATFORMS = {
    # AWS
    'aws_glue': PlatformOption('AWS Glue', 'aws', 0.44, 2, 100,
        ['Serverless', 'Native S3', 'Catalog integration'],
        ['S3-centric workloads', 'Infrequent jobs'], 'low'),
    'aws_emr': PlatformOption('AWS EMR', 'aws', 0.25, 3, 500,
        ['Cost-effective at scale', 'Full Spark control', 'Spot instances'],
        ['Large datasets', 'Long-running clusters'], 'medium'),
    'aws_emr_serverless': PlatformOption('AWS EMR Serverless', 'aws', 0.36, 1, 200,
        ['Serverless EMR', 'Auto-scaling', 'No cluster management'],
        ['Variable workloads', 'Cost optimization'], 'low'),

    # GCP
    'gcp_dataproc': PlatformOption('GCP Dataproc', 'gcp', 0.20, 2, 500,
        ['Lowest Spark cost', 'Preemptible VMs', 'BigQuery native'],
        ['Cost-sensitive', 'GCS workloads', 'BigQuery integration'], 'high'),
    'gcp_dataproc_serverless': PlatformOption('GCP Dataproc Serverless', 'gcp', 0.30, 1, 100,
        ['Serverless Spark', 'Auto-scaling', 'No ops'],
        ['Batch Spark jobs', 'Variable workloads'], 'high'),
    'gcp_dataflow': PlatformOption('GCP Dataflow', 'gcp', 0.28, 1, 1000,
        ['Streaming + batch', 'Auto-tuning', 'Unified model'],
        ['Streaming ETL', 'Real-time pipelines'], 'high'),

    # Azure
    'azure_synapse': PlatformOption('Azure Synapse Spark', 'azure', 0.38, 3, 200,
        ['Unified analytics', 'Power BI native', 'SQL + Spark'],
        ['Microsoft ecosystem', 'BI workloads'], 'high'),
    'azure_databricks': PlatformOption('Azure Databricks', 'azure', 0.40, 2, 500,
        ['Delta Lake native', 'Collaborative', 'ML integration'],
        ['Data science teams', 'Delta Lake users'], 'medium'),
    'azure_hdinsight': PlatformOption('Azure HDInsight', 'azure', 0.22, 3, 500,
        ['Open source Hadoop/Spark', 'Lower cost', 'Flexible'],
        ['Hadoop migrations', 'Cost optimization'], 'medium'),

    # Independent / Multi-cloud
    'databricks': PlatformOption('Databricks (Multi-cloud)', 'independent', 0.45, 2, 1000,
        ['Best Spark runtime', 'Delta Lake', 'Photon engine', 'Unity Catalog'],
        ['Performance-critical', 'Multi-cloud', 'Data mesh'], 'medium'),
    'snowflake': PlatformOption('Snowflake', 'independent', 0.50, 1, 1000,
        ['Zero-ops', 'Time-travel', 'Data sharing', 'Near-zero scaling'],
        ['SQL-heavy ETL', 'Data warehousing', 'BI workloads'], 'high'),
    'dbt_cloud': PlatformOption('dbt Cloud + Warehouse', 'independent', 0.10, 1, 100,
        ['SQL transforms', 'Version control', 'Lineage', 'Testing'],
        ['SQL-based ETL', 'Analytics engineering'], 'medium'),
    'apache_spark_k8s': PlatformOption('Spark on Kubernetes', 'independent', 0.15, 1, 1000,
        ['Portable', 'Cloud-agnostic', 'Cost control', 'Spot/preempt'],
        ['Multi-cloud', 'Existing K8s', 'Cost optimization'], 'high'),
    'trino': PlatformOption('Trino/Starburst', 'independent', 0.12, 3, 500,
        ['Federated queries', 'Fast OLAP', 'Multi-source'],
        ['Query federation', 'Ad-hoc analytics'], 'medium'),
}


@dataclass
class JobAnalysis:
    job_name: str
    config_path: str
    current_platform: str
    estimated_monthly_cost: float
    data_size_gb: float
    complexity: str
    recommendations: List[Dict] = field(default_factory=list)
    platform_scores: Dict[str, Dict] = field(default_factory=dict)
    optimizations: List[Dict] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class MultiConfigOptimizer:
    """Analyze multiple ETL configs with cross-platform recommendations."""

    def __init__(self, source_dir: str, dest_dir: str):
        self.source_path = Path(source_dir)
        self.dest_path = Path(dest_dir)
        self.dest_path.mkdir(parents=True, exist_ok=True)
        self.analyses: List[JobAnalysis] = []

    def run(self) -> Dict[str, Any]:
        """Run full analysis on all configs."""
        configs = list(self.source_path.glob('*.json'))
        print(f"\n{'='*60}")
        print(f" Multi-Config ETL Optimizer")
        print(f" Found {len(configs)} config(s) in {self.source_path}")
        print(f"{'='*60}\n")

        for cfg_path in configs:
            print(f"  Analyzing: {cfg_path.name}")
            try:
                analysis = self._analyze_config(cfg_path)
                self.analyses.append(analysis)
            except Exception as e:
                print(f"    Error: {e}")

        # Generate reports
        summary = self._generate_summary()
        self._save_json_report(summary)
        self._save_html_report(summary)

        return summary

    def _analyze_config(self, cfg_path: Path) -> JobAnalysis:
        """Analyze a single config file."""
        with open(cfg_path) as f:
            config = json.load(f)

        job_name = config.get('job_name', cfg_path.stem)

        # Extract metrics from config
        current_cfg = config.get('current_config', {})
        workers = current_cfg.get('NumberOfWorkers', current_cfg.get('workers', 10))
        worker_type = current_cfg.get('WorkerType', 'G.1X')

        # Estimate data size from source tables
        tables = config.get('source_tables', [])
        data_size_gb = sum(t.get('size_gb', t.get('estimated_rows', 1000000) / 1000000 * 0.5)
                          for t in tables)

        # Detect current platform
        current_platform = self._detect_platform(config)

        # Estimate current cost
        monthly_runs = config.get('monthly_runs', 30)
        avg_runtime_hrs = config.get('avg_runtime_hours', 0.5)
        monthly_cost = self._estimate_cost(current_platform, workers, avg_runtime_hrs, monthly_runs)

        # Determine complexity
        complexity = self._assess_complexity(config, data_size_gb)

        analysis = JobAnalysis(
            job_name=job_name,
            config_path=str(cfg_path),
            current_platform=current_platform,
            estimated_monthly_cost=monthly_cost,
            data_size_gb=data_size_gb,
            complexity=complexity
        )

        # Score all platforms
        analysis.platform_scores = self._score_platforms(config, data_size_gb, monthly_runs, avg_runtime_hrs, workers)

        # Generate optimizations
        analysis.optimizations = self._generate_optimizations(config, analysis)

        # Generate recommendations
        analysis.recommendations = self._generate_recommendations(analysis)

        # Check for warnings
        analysis.warnings = self._check_warnings(config, analysis)

        return analysis

    def _detect_platform(self, config: Dict) -> str:
        """Detect current platform from config."""
        cfg = config.get('current_config', {})
        if 'GlueVersion' in cfg or 'NumberOfWorkers' in cfg:
            return 'aws_glue'
        if 'EmrCluster' in cfg or 'ReleaseLabel' in cfg:
            return 'aws_emr'
        if 'databricks' in str(config).lower():
            return 'databricks'
        return 'aws_glue'  # default

    def _estimate_cost(self, platform: str, workers: int, runtime_hrs: float, monthly_runs: int) -> float:
        """Estimate monthly cost for a platform."""
        plat = PLATFORMS.get(platform, PLATFORMS['aws_glue'])
        return workers * plat.cost_per_dpu_hour * runtime_hrs * monthly_runs

    def _assess_complexity(self, config: Dict, data_size_gb: float) -> str:
        """Assess job complexity."""
        tables = config.get('source_tables', [])
        joins = sum(1 for t in tables if t.get('join_type'))

        if data_size_gb > 500 or joins > 5:
            return 'high'
        elif data_size_gb > 50 or joins > 2:
            return 'medium'
        return 'low'

    def _score_platforms(self, config: Dict, data_size_gb: float,
                         monthly_runs: int, runtime_hrs: float, workers: int) -> Dict:
        """Score all platforms for this workload."""
        scores = {}
        current = self._detect_platform(config)
        current_cost = self._estimate_cost(current, workers, runtime_hrs, monthly_runs)

        for name, plat in PLATFORMS.items():
            # Estimate workers needed (adjust for platform efficiency)
            efficiency = {'databricks': 0.7, 'gcp_dataproc': 0.85, 'snowflake': 0.5}.get(name, 1.0)
            adj_workers = max(plat.min_workers, int(workers * efficiency))

            # Runtime adjustment
            runtime_adj = {'databricks': 0.6, 'snowflake': 0.4, 'trino': 0.7}.get(name, 1.0)
            adj_runtime = runtime_hrs * runtime_adj

            cost = adj_workers * plat.cost_per_dpu_hour * adj_runtime * monthly_runs
            savings = ((current_cost - cost) / current_cost * 100) if current_cost > 0 else 0

            # Fit score (0-100)
            fit_score = self._calculate_fit_score(plat, config, data_size_gb)

            scores[name] = {
                'platform': plat.name,
                'provider': plat.provider,
                'monthly_cost': round(cost, 2),
                'savings_percent': round(savings, 1),
                'fit_score': fit_score,
                'migration_effort': plat.migration_effort,
                'strengths': plat.strengths,
                'best_for': plat.best_for
            }

        return scores

    def _calculate_fit_score(self, plat: PlatformOption, config: Dict, data_size_gb: float) -> int:
        """Calculate how well a platform fits the workload (0-100)."""
        score = 50  # baseline

        # Size fit
        if data_size_gb > 100 and plat.max_workers >= 100:
            score += 15
        elif data_size_gb < 10 and 'serverless' in plat.name.lower():
            score += 15

        # Workload type fit
        processing = config.get('processing_mode', 'batch')
        if processing == 'streaming' and 'dataflow' in plat.name.lower():
            score += 20
        if processing == 'sql' and plat.name in ['Snowflake', 'dbt Cloud + Warehouse', 'Trino/Starburst']:
            score += 25

        # Migration effort penalty
        effort_penalty = {'low': 0, 'medium': -10, 'high': -20}
        score += effort_penalty.get(plat.migration_effort, 0)

        # Provider bonus if already on that cloud
        current_cloud = config.get('cloud_provider', 'aws')
        if plat.provider == current_cloud:
            score += 10

        return min(100, max(0, score))

    def _generate_optimizations(self, config: Dict, analysis: JobAnalysis) -> List[Dict]:
        """Generate specific optimizations for current platform."""
        opts = []
        cfg = config.get('current_config', {})

        # Worker optimization
        workers = cfg.get('NumberOfWorkers', 10)
        if analysis.data_size_gb < 5 and workers > 5:
            opts.append({
                'type': 'right_sizing',
                'title': 'Reduce Workers',
                'impact': 'high',
                'savings_percent': 40,
                'detail': f'Data size ({analysis.data_size_gb:.1f}GB) suggests {max(2, int(workers * 0.5))} workers sufficient'
            })

        # Worker type optimization
        if cfg.get('WorkerType') == 'G.2X' and analysis.data_size_gb < 50:
            opts.append({
                'type': 'right_sizing',
                'title': 'Downgrade Worker Type',
                'impact': 'medium',
                'savings_percent': 50,
                'detail': 'G.1X workers sufficient for this data volume'
            })

        # Scheduling optimization
        if config.get('monthly_runs', 30) > 60:
            opts.append({
                'type': 'scheduling',
                'title': 'Consolidate Runs',
                'impact': 'medium',
                'savings_percent': 30,
                'detail': 'Consider batching multiple runs to reduce cold-start overhead'
            })

        # Partitioning
        tables = config.get('source_tables', [])
        for t in tables:
            if t.get('estimated_rows', 0) > 10_000_000 and not t.get('partition_key'):
                opts.append({
                    'type': 'data_optimization',
                    'title': f"Partition {t.get('name', 'table')}",
                    'impact': 'high',
                    'savings_percent': 25,
                    'detail': 'Add partition pruning to reduce data scanned'
                })

        # Compression
        for t in tables:
            fmt = t.get('format', '').lower()
            if fmt in ['csv', 'json', 'text']:
                opts.append({
                    'type': 'data_optimization',
                    'title': f"Convert {t.get('name', 'table')} to Parquet",
                    'impact': 'high',
                    'savings_percent': 60,
                    'detail': 'Parquet reduces storage and improves query performance'
                })

        return opts

    def _generate_recommendations(self, analysis: JobAnalysis) -> List[Dict]:
        """Generate top platform recommendations."""
        recs = []

        # Sort by savings
        sorted_plats = sorted(
            analysis.platform_scores.items(),
            key=lambda x: (x[1]['savings_percent'], x[1]['fit_score']),
            reverse=True
        )

        # Top 3 cost savers
        for name, score in sorted_plats[:3]:
            if score['savings_percent'] > 5:
                recs.append({
                    'type': 'platform_switch',
                    'platform': score['platform'],
                    'provider': score['provider'],
                    'monthly_cost': score['monthly_cost'],
                    'savings_percent': score['savings_percent'],
                    'fit_score': score['fit_score'],
                    'effort': score['migration_effort'],
                    'reason': f"Save {score['savings_percent']:.0f}% - {', '.join(score['strengths'][:2])}"
                })

        # Best fit regardless of cost
        best_fit = max(sorted_plats, key=lambda x: x[1]['fit_score'])
        if best_fit[1]['fit_score'] > 70 and best_fit[0] not in [r['platform'] for r in recs[:3]]:
            recs.append({
                'type': 'best_fit',
                'platform': best_fit[1]['platform'],
                'provider': best_fit[1]['provider'],
                'fit_score': best_fit[1]['fit_score'],
                'reason': f"Best workload fit - {', '.join(best_fit[1]['best_for'][:2])}"
            })

        return recs

    def _check_warnings(self, config: Dict, analysis: JobAnalysis) -> List[str]:
        """Check for potential issues."""
        warnings = []

        if analysis.data_size_gb > 1000:
            warnings.append("Very large dataset - consider incremental processing")

        if analysis.estimated_monthly_cost > 5000:
            warnings.append("High monthly cost - review platform alternatives")

        tables = config.get('source_tables', [])
        if len(tables) > 10:
            warnings.append("Many source tables - consider staging layer")

        return warnings

    def _generate_summary(self) -> Dict[str, Any]:
        """Generate overall summary."""
        total_current_cost = sum(a.estimated_monthly_cost for a in self.analyses)

        # Best savings per job
        potential_savings = 0
        for a in self.analyses:
            if a.platform_scores:
                best = min(a.platform_scores.values(), key=lambda x: x['monthly_cost'])
                potential_savings += a.estimated_monthly_cost - best['monthly_cost']

        return {
            'generated_at': datetime.utcnow().isoformat(),
            'total_jobs': len(self.analyses),
            'total_current_monthly_cost': round(total_current_cost, 2),
            'potential_monthly_savings': round(potential_savings, 2),
            'savings_percent': round(potential_savings / total_current_cost * 100, 1) if total_current_cost else 0,
            'jobs': [asdict(a) for a in self.analyses],
            'platform_summary': self._platform_summary(),
            'top_optimizations': self._top_optimizations()
        }

    def _platform_summary(self) -> Dict[str, int]:
        """Count jobs by recommended platform."""
        counts = {}
        for a in self.analyses:
            if a.recommendations:
                plat = a.recommendations[0].get('platform', 'unknown')
                counts[plat] = counts.get(plat, 0) + 1
        return counts

    def _top_optimizations(self) -> List[Dict]:
        """Get top optimizations across all jobs."""
        all_opts = []
        for a in self.analyses:
            for opt in a.optimizations:
                opt['job'] = a.job_name
                all_opts.append(opt)
        return sorted(all_opts, key=lambda x: x.get('savings_percent', 0), reverse=True)[:10]

    def _save_json_report(self, summary: Dict):
        """Save detailed JSON report."""
        ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        path = self.dest_path / f"optimization_report_{ts}.json"
        with open(path, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"\n  JSON: {path}")

    def _save_html_report(self, summary: Dict):
        """Save HTML summary report."""
        ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        path = self.dest_path / f"optimization_report_{ts}.html"

        html = f"""<!DOCTYPE html>
<html><head><title>ETL Optimization Report</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }}
.container {{ max-width: 1200px; margin: 0 auto; }}
.card {{ background: white; border-radius: 8px; padding: 20px; margin: 15px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
h1 {{ color: #1a1a1a; border-bottom: 3px solid #0066cc; padding-bottom: 10px; }}
h2 {{ color: #333; margin-top: 0; }}
.summary-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; }}
.metric {{ text-align: center; padding: 15px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border-radius: 8px; }}
.metric .value {{ font-size: 2em; font-weight: bold; }}
.metric .label {{ opacity: 0.9; font-size: 0.9em; }}
table {{ width: 100%; border-collapse: collapse; }}
th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #eee; }}
th {{ background: #f8f9fa; font-weight: 600; }}
.savings {{ color: #28a745; font-weight: bold; }}
.warning {{ color: #dc3545; }}
.tag {{ display: inline-block; padding: 3px 8px; border-radius: 4px; font-size: 0.8em; margin: 2px; }}
.tag-aws {{ background: #ff9900; color: white; }}
.tag-gcp {{ background: #4285f4; color: white; }}
.tag-azure {{ background: #0078d4; color: white; }}
.tag-independent {{ background: #6c757d; color: white; }}
.effort-low {{ color: #28a745; }}
.effort-medium {{ color: #ffc107; }}
.effort-high {{ color: #dc3545; }}
</style></head>
<body><div class="container">
<h1>ETL Multi-Config Optimization Report</h1>
<p>Generated: {summary['generated_at']}</p>

<div class="summary-grid">
  <div class="metric"><div class="value">{summary['total_jobs']}</div><div class="label">Jobs Analyzed</div></div>
  <div class="metric"><div class="value">${summary['total_current_monthly_cost']:,.0f}</div><div class="label">Current Monthly</div></div>
  <div class="metric"><div class="value">${summary['potential_monthly_savings']:,.0f}</div><div class="label">Potential Savings</div></div>
  <div class="metric"><div class="value">{summary['savings_percent']:.0f}%</div><div class="label">Savings Rate</div></div>
</div>

<div class="card">
<h2>Job Analysis</h2>
<table>
<tr><th>Job</th><th>Current</th><th>Monthly Cost</th><th>Top Recommendation</th><th>Savings</th><th>Effort</th></tr>
"""
        for job in summary['jobs']:
            rec = job['recommendations'][0] if job['recommendations'] else {}
            plat_tag = f"tag-{rec.get('provider', 'aws')}"
            effort_class = f"effort-{rec.get('effort', 'medium')}"
            html += f"""<tr>
<td><strong>{job['job_name']}</strong><br><small>{job['complexity']} complexity, {job['data_size_gb']:.1f}GB</small></td>
<td>{job['current_platform']}</td>
<td>${job['estimated_monthly_cost']:.0f}</td>
<td><span class="tag {plat_tag}">{rec.get('platform', '-')}</span></td>
<td class="savings">{rec.get('savings_percent', 0):.0f}%</td>
<td class="{effort_class}">{rec.get('effort', '-')}</td>
</tr>"""

        html += """</table></div>

<div class="card">
<h2>Platform Recommendations Summary</h2>
<table>
<tr><th>Platform</th><th>Provider</th><th>Jobs Recommended</th><th>Best For</th></tr>
"""
        for plat, count in summary.get('platform_summary', {}).items():
            p = next((v for k, v in PLATFORMS.items() if v.name == plat), None)
            if p:
                html += f"""<tr>
<td><strong>{plat}</strong></td>
<td><span class="tag tag-{p.provider}">{p.provider.upper()}</span></td>
<td>{count}</td>
<td>{', '.join(p.best_for[:2])}</td>
</tr>"""

        html += """</table></div>

<div class="card">
<h2>Top Optimizations</h2>
<table>
<tr><th>Job</th><th>Optimization</th><th>Impact</th><th>Savings</th></tr>
"""
        for opt in summary.get('top_optimizations', [])[:10]:
            html += f"""<tr>
<td>{opt.get('job', '-')}</td>
<td><strong>{opt.get('title', '-')}</strong><br><small>{opt.get('detail', '')}</small></td>
<td>{opt.get('impact', '-')}</td>
<td class="savings">{opt.get('savings_percent', 0)}%</td>
</tr>"""

        html += """</table></div>
</div></body></html>"""

        with open(path, 'w') as f:
            f.write(html)
        print(f"  HTML: {path}")


def main():
    parser = argparse.ArgumentParser(description='Multi-Config ETL Optimizer')
    parser.add_argument('--source', '-s', required=True, help='Directory with config JSONs')
    parser.add_argument('--dest', '-d', required=True, help='Output directory for reports')
    args = parser.parse_args()

    optimizer = MultiConfigOptimizer(args.source, args.dest)
    summary = optimizer.run()

    print(f"\n{'='*60}")
    print(f" Summary: {summary['total_jobs']} jobs, ${summary['total_current_monthly_cost']:,.0f}/mo")
    print(f" Potential Savings: ${summary['potential_monthly_savings']:,.0f}/mo ({summary['savings_percent']:.0f}%)")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
