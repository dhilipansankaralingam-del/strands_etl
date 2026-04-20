#!/usr/bin/env python3
"""
Multi-Config ETL Optimizer using REAL Strands SDK.

Uses actual LLM-powered agents with tool calling for:
- Code analysis with line-by-line improvements
- Table sizing and S3 scanning
- Platform recommendations
- Cost optimization

Usage:
    export AWS_PROFILE=your-profile  # or set AWS credentials
    python scripts/strands_optimizer.py --source ./demo_configs/ --dest ./reports/
"""

import argparse
import json
import os
import re
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

# Real Strands SDK
from strands import Agent
from strands.tools import tool

# Optional AWS imports
try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


# =============================================================================
# TOOLS - These are called by the Strands Agent
# =============================================================================

@tool
def read_code_file(file_path: str) -> str:
    """Read a Python/PySpark script file and return its contents.

    Args:
        file_path: Path to the script file

    Returns:
        The file contents as a string, or error message if file not found
    """
    print(f"\n  [TOOL] read_code_file({file_path})")
    try:
        path = Path(file_path)
        if not path.exists():
            return f"ERROR: File not found: {file_path}"
        content = path.read_text()
        print(f"  [TOOL] Read {len(content)} chars, {len(content.splitlines())} lines")
        return content
    except Exception as e:
        return f"ERROR reading file: {e}"


@tool
def analyze_pyspark_code(code: str) -> Dict[str, Any]:
    """Analyze PySpark code for anti-patterns and optimization opportunities.

    Args:
        code: The PySpark code to analyze

    Returns:
        Dictionary with findings, anti-patterns, and line-by-line suggestions
    """
    print(f"\n  [TOOL] analyze_pyspark_code({len(code)} chars)")

    lines = code.split('\n')
    findings = []

    anti_patterns = {
        r'\.collect\(\)': ('collect()', 'Loads all data to driver - can cause OOM', 'high'),
        r'\.toPandas\(\)': ('toPandas()', 'Converts to Pandas - memory intensive', 'high'),
        r'\.crossJoin\(': ('crossJoin()', 'Cartesian product - very expensive', 'critical'),
        r'@udf\s*\n': ('UDF without type', 'UDF without return type is slow', 'medium'),
        r'\.repartition\(\d+\)': ('repartition()', 'Causes full shuffle', 'medium'),
        r'for\s+.*\s+in\s+.*\.collect': ('Loop over collect', 'Iterating over collected data', 'high'),
        r'spark\.read\.csv': ('CSV read', 'Consider Parquet for better performance', 'low'),
    }

    good_patterns = {
        r'broadcast\(': ('broadcast()', 'Good: Using broadcast for small tables'),
        r'\.(cache|persist)\(\)': ('cache/persist()', 'Good: Caching intermediate results'),
        r'\.coalesce\(': ('coalesce()', 'Good: Reducing partitions without shuffle'),
        r'\.repartition\(.*col\(': ('repartition by column', 'Good: Partition by key for joins'),
    }

    for i, line in enumerate(lines, 1):
        for pattern, (name, msg, severity) in anti_patterns.items():
            if re.search(pattern, line):
                findings.append({
                    'line': i,
                    'type': 'anti_pattern',
                    'severity': severity,
                    'pattern': name,
                    'message': msg,
                    'code': line.strip()[:80]
                })

        for pattern, (name, msg) in good_patterns.items():
            if re.search(pattern, line):
                findings.append({
                    'line': i,
                    'type': 'good_practice',
                    'severity': 'info',
                    'pattern': name,
                    'message': msg,
                    'code': line.strip()[:80]
                })

    print(f"  [TOOL] Found {len(findings)} patterns")
    return {
        'total_lines': len(lines),
        'findings_count': len(findings),
        'findings': findings,
        'anti_pattern_count': sum(1 for f in findings if f['type'] == 'anti_pattern'),
        'good_practice_count': sum(1 for f in findings if f['type'] == 'good_practice')
    }


@tool
def scan_s3_path(s3_path: str) -> Dict[str, Any]:
    """Scan an S3 path to get file count and total size.

    Args:
        s3_path: S3 URI like s3://bucket/prefix/

    Returns:
        Dictionary with file count, total size, and file types
    """
    print(f"\n  [TOOL] scan_s3_path({s3_path})")

    if not HAS_BOTO3:
        return {'error': 'boto3 not installed', 's3_path': s3_path}

    try:
        if not s3_path.startswith('s3://'):
            return {'error': 'Invalid S3 path', 's3_path': s3_path}

        path = s3_path.replace('s3://', '')
        parts = path.split('/', 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''

        s3 = boto3.client('s3')
        paginator = s3.get_paginator('list_objects_v2')

        total_size = 0
        file_count = 0
        extensions = {}

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={'MaxItems': 1000}):
            for obj in page.get('Contents', []):
                file_count += 1
                total_size += obj['Size']
                ext = Path(obj['Key']).suffix.lower() or 'none'
                extensions[ext] = extensions.get(ext, 0) + 1

        result = {
            's3_path': s3_path,
            'file_count': file_count,
            'total_size_bytes': total_size,
            'total_size_gb': round(total_size / (1024**3), 2),
            'extensions': extensions
        }
        print(f"  [TOOL] Found {file_count} files, {result['total_size_gb']} GB")
        return result

    except Exception as e:
        print(f"  [TOOL] S3 scan failed: {e}")
        return {'error': str(e), 's3_path': s3_path}


@tool
def get_glue_job_info(job_name: str) -> Dict[str, Any]:
    """Get AWS Glue job configuration and recent runs.

    Args:
        job_name: Name of the Glue job

    Returns:
        Dictionary with job config and recent run metrics
    """
    print(f"\n  [TOOL] get_glue_job_info({job_name})")

    if not HAS_BOTO3:
        return {'error': 'boto3 not installed', 'job_name': job_name}

    try:
        glue = boto3.client('glue')

        # Get job definition
        job = glue.get_job(JobName=job_name)['Job']

        # Get recent runs
        runs = glue.get_job_runs(JobName=job_name, MaxResults=5)['JobRuns']

        recent_runs = []
        for run in runs:
            recent_runs.append({
                'run_id': run['Id'],
                'status': run['JobRunState'],
                'started': run.get('StartedOn', '').isoformat() if run.get('StartedOn') else None,
                'duration_sec': run.get('ExecutionTime', 0),
                'dpu_seconds': run.get('DPUSeconds', 0)
            })

        result = {
            'job_name': job_name,
            'worker_type': job.get('WorkerType', 'Standard'),
            'num_workers': job.get('NumberOfWorkers', 0),
            'glue_version': job.get('GlueVersion', ''),
            'timeout_minutes': job.get('Timeout', 0),
            'recent_runs': recent_runs
        }
        print(f"  [TOOL] Got job info: {result['num_workers']} workers, {len(recent_runs)} recent runs")
        return result

    except glue.exceptions.EntityNotFoundException:
        print(f"  [TOOL] Glue job not found: {job_name}")
        return {'error': f'Job not found: {job_name}', 'job_name': job_name}
    except Exception as e:
        print(f"  [TOOL] Glue API failed: {e}")
        return {'error': str(e), 'job_name': job_name}


@tool
def estimate_platform_costs(
    current_platform: str,
    workers: int,
    runtime_hours: float,
    monthly_runs: int,
    data_size_gb: float
) -> Dict[str, Any]:
    """Estimate costs across different data platforms.

    Args:
        current_platform: Current platform (glue, emr, etc.)
        workers: Number of workers/DPUs
        runtime_hours: Average job runtime in hours
        monthly_runs: Number of runs per month
        data_size_gb: Data size in GB

    Returns:
        Dictionary with cost estimates for each platform
    """
    print(f"\n  [TOOL] estimate_platform_costs(workers={workers}, runtime={runtime_hours}h, runs={monthly_runs}/mo)")

    platforms = {
        'aws_glue': {'cost_per_dpu_hr': 0.44, 'efficiency': 1.0, 'provider': 'AWS'},
        'aws_emr': {'cost_per_dpu_hr': 0.25, 'efficiency': 0.9, 'provider': 'AWS'},
        'aws_emr_serverless': {'cost_per_dpu_hr': 0.36, 'efficiency': 0.95, 'provider': 'AWS'},
        'gcp_dataproc': {'cost_per_dpu_hr': 0.20, 'efficiency': 0.85, 'provider': 'GCP'},
        'gcp_dataproc_serverless': {'cost_per_dpu_hr': 0.30, 'efficiency': 0.9, 'provider': 'GCP'},
        'azure_synapse': {'cost_per_dpu_hr': 0.38, 'efficiency': 0.95, 'provider': 'Azure'},
        'azure_databricks': {'cost_per_dpu_hr': 0.40, 'efficiency': 0.7, 'provider': 'Azure'},
        'databricks': {'cost_per_dpu_hr': 0.45, 'efficiency': 0.65, 'provider': 'Multi-cloud'},
        'snowflake': {'cost_per_dpu_hr': 0.50, 'efficiency': 0.5, 'provider': 'Multi-cloud'},
        'spark_on_k8s': {'cost_per_dpu_hr': 0.15, 'efficiency': 0.8, 'provider': 'Self-managed'},
    }

    current_info = platforms.get(current_platform, platforms['aws_glue'])
    current_cost = workers * current_info['cost_per_dpu_hr'] * runtime_hours * monthly_runs

    estimates = []
    for name, info in platforms.items():
        adj_workers = max(2, int(workers * info['efficiency']))
        adj_runtime = runtime_hours * info['efficiency']
        cost = adj_workers * info['cost_per_dpu_hr'] * adj_runtime * monthly_runs
        savings = ((current_cost - cost) / current_cost * 100) if current_cost > 0 else 0

        estimates.append({
            'platform': name,
            'provider': info['provider'],
            'monthly_cost': round(cost, 2),
            'savings_percent': round(savings, 1),
            'adjusted_workers': adj_workers
        })

    estimates.sort(key=lambda x: x['monthly_cost'])

    result = {
        'current_platform': current_platform,
        'current_monthly_cost': round(current_cost, 2),
        'estimates': estimates,
        'best_option': estimates[0],
        'max_savings_percent': estimates[0]['savings_percent']
    }
    print(f"  [TOOL] Current: ${current_cost:.0f}/mo, Best: {estimates[0]['platform']} at ${estimates[0]['monthly_cost']:.0f}/mo")
    return result


# =============================================================================
# MAIN OPTIMIZER
# =============================================================================

class StrandsOptimizer:
    """Multi-config optimizer using real Strands SDK agents."""

    SYSTEM_PROMPT = """You are an expert ETL and data engineering optimization assistant.

Your task is to analyze ETL job configurations and provide detailed recommendations for:
1. Code improvements (line-by-line suggestions)
2. Resource optimization (workers, memory)
3. Cost reduction opportunities
4. Platform migration options

For each job config you receive:
1. Read and analyze the script file if provided
2. Scan S3 paths to understand data sizes
3. Check Glue job configurations if applicable
4. Estimate costs across platforms
5. Provide specific, actionable recommendations

Always be specific with line numbers when suggesting code changes.
Always show your reasoning and calculations.
Format your response clearly with sections for each analysis area."""

    def __init__(self, source_dir: str, dest_dir: str, model: str = None):
        self.source_path = Path(source_dir)
        self.dest_path = Path(dest_dir)
        self.dest_path.mkdir(parents=True, exist_ok=True)
        self.model = model or os.environ.get('STRANDS_MODEL', 'us.anthropic.claude-sonnet-4-6-20250514')
        self.results = []
        self.total_tokens = {'input': 0, 'output': 0}

    def run(self):
        """Run optimization on all configs."""
        configs = list(self.source_path.glob('*.json'))

        print(f"\n{'='*70}")
        print(f"  STRANDS SDK ETL OPTIMIZER")
        print(f"  Model: {self.model}")
        print(f"  Source: {self.source_path}")
        print(f"  Configs: {len(configs)}")
        print(f"{'='*70}")

        if not configs:
            print("\n  ERROR: No config files found!")
            return

        for i, cfg_path in enumerate(configs, 1):
            print(f"\n{'─'*70}")
            print(f"  [{i}/{len(configs)}] {cfg_path.name}")
            print(f"{'─'*70}")

            result = self._analyze_config(cfg_path)
            self.results.append(result)

        self._save_reports()
        self._print_summary()

    def _analyze_config(self, cfg_path: Path) -> Dict[str, Any]:
        """Analyze a single config using Strands agent."""
        start_time = time.time()

        # Load config
        try:
            with open(cfg_path) as f:
                config = json.load(f)
            print(f"  Config loaded: {len(config)} keys")
        except Exception as e:
            return {'error': f'Failed to load config: {e}', 'config_path': str(cfg_path)}

        job_name = config.get('job_name', cfg_path.stem)

        # Build prompt for agent
        prompt = self._build_prompt(config)
        print(f"\n  Prompt size: {len(prompt)} chars")

        # Create Strands agent with tools
        print(f"\n  Creating Strands Agent...")
        print(f"  [AGENT] Model: {self.model}")
        print(f"  [AGENT] Tools: read_code_file, analyze_pyspark_code, scan_s3_path, get_glue_job_info, estimate_platform_costs")

        try:
            agent = Agent(
                model=self.model,
                system_prompt=self.SYSTEM_PROMPT,
                tools=[
                    read_code_file,
                    analyze_pyspark_code,
                    scan_s3_path,
                    get_glue_job_info,
                    estimate_platform_costs
                ]
            )

            print(f"\n  [AGENT] Sending prompt to agent...")
            print(f"  {'─'*50}")

            # Execute agent
            response = agent(prompt)

            print(f"  {'─'*50}")
            print(f"\n  [AGENT] Response received")

            # Extract metrics
            elapsed = time.time() - start_time

            # Get token usage if available
            tokens = {'input': 0, 'output': 0}
            if hasattr(response, 'metrics'):
                tokens = {
                    'input': getattr(response.metrics, 'input_tokens', 0),
                    'output': getattr(response.metrics, 'output_tokens', 0)
                }

            self.total_tokens['input'] += tokens['input']
            self.total_tokens['output'] += tokens['output']

            print(f"  [METRICS] Time: {elapsed:.1f}s, Tokens: {tokens['input']}in/{tokens['output']}out")

            return {
                'job_name': job_name,
                'config_path': str(cfg_path),
                'config': config,
                'analysis': str(response),
                'elapsed_seconds': round(elapsed, 2),
                'tokens': tokens,
                'status': 'success'
            }

        except Exception as e:
            print(f"\n  [ERROR] Agent failed: {e}")
            traceback.print_exc()
            return {
                'job_name': job_name,
                'config_path': str(cfg_path),
                'error': str(e),
                'status': 'error'
            }

    def _build_prompt(self, config: Dict) -> str:
        """Build analysis prompt from config."""
        parts = [f"Analyze this ETL job configuration:\n"]
        parts.append(f"Job Name: {config.get('job_name', 'unknown')}")

        if config.get('script_path'):
            parts.append(f"\nScript Path: {config['script_path']}")
            parts.append("Please read and analyze this script file for anti-patterns and improvements.")

        if config.get('source_tables'):
            parts.append(f"\nSource Tables ({len(config['source_tables'])}):")
            for t in config['source_tables']:
                parts.append(f"  - {t.get('database', 'db')}.{t.get('table', t.get('name', '?'))}")
                if t.get('location') or t.get('s3_path'):
                    s3 = t.get('location') or t.get('s3_path')
                    parts.append(f"    Location: {s3}")
                    parts.append(f"    Please scan this S3 path to determine actual data size.")
                if t.get('estimated_rows'):
                    parts.append(f"    Est. rows: {t['estimated_rows']:,}")

        if config.get('current_config'):
            cc = config['current_config']
            parts.append(f"\nCurrent Configuration:")
            parts.append(f"  Workers: {cc.get('NumberOfWorkers', cc.get('workers', 'unknown'))}")
            parts.append(f"  Worker Type: {cc.get('WorkerType', 'unknown')}")
            if cc.get('GlueVersion'):
                parts.append(f"  Glue Version: {cc['GlueVersion']}")

        if config.get('glue_job_name'):
            parts.append(f"\nGlue Job: {config['glue_job_name']}")
            parts.append("Please fetch the job configuration and recent run metrics.")

        parts.append(f"\nMonthly Runs: {config.get('monthly_runs', 30)}")
        parts.append(f"Avg Runtime: {config.get('avg_runtime_hours', 0.5)} hours")

        parts.append("\n\nPlease provide:")
        parts.append("1. Code analysis with specific line-by-line improvements")
        parts.append("2. Data size assessment from S3 scans")
        parts.append("3. Resource optimization recommendations")
        parts.append("4. Cost comparison across platforms (AWS, GCP, Azure, Databricks, etc.)")
        parts.append("5. Migration recommendations with effort estimates")

        return '\n'.join(parts)

    def _save_reports(self):
        """Save JSON and HTML reports."""
        ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

        # JSON
        json_path = self.dest_path / f"strands_analysis_{ts}.json"
        with open(json_path, 'w') as f:
            json.dump({
                'generated_at': datetime.utcnow().isoformat(),
                'model': self.model,
                'total_tokens': self.total_tokens,
                'results': self.results
            }, f, indent=2, default=str)
        print(f"\n  Saved: {json_path}")

        # HTML
        html_path = self.dest_path / f"strands_analysis_{ts}.html"
        html = self._generate_html()
        with open(html_path, 'w') as f:
            f.write(html)
        print(f"  Saved: {html_path}")

    def _generate_html(self) -> str:
        """Generate HTML report."""
        html = f"""<!DOCTYPE html>
<html><head><title>Strands ETL Analysis</title>
<style>
body {{ font-family: -apple-system, sans-serif; margin: 20px; background: #1a1a2e; color: #eee; }}
.container {{ max-width: 1200px; margin: 0 auto; }}
h1 {{ color: #00d9ff; }}
h2 {{ color: #00ff88; border-bottom: 1px solid #333; padding-bottom: 10px; }}
.card {{ background: #16213e; border-radius: 8px; padding: 20px; margin: 15px 0; }}
.metrics {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin-bottom: 20px; }}
.metric {{ text-align: center; padding: 20px; background: linear-gradient(135deg, #0f3460, #16213e); border-radius: 8px; border: 1px solid #0f3460; }}
.metric .value {{ font-size: 2em; font-weight: bold; color: #00d9ff; }}
pre {{ background: #0f0f23; padding: 15px; border-radius: 5px; overflow-x: auto; white-space: pre-wrap; }}
.success {{ color: #00ff88; }}
.error {{ color: #ff6b6b; }}
</style></head>
<body><div class="container">
<h1>Strands SDK ETL Analysis Report</h1>
<p>Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
<p>Model: {self.model}</p>

<div class="metrics">
  <div class="metric"><div class="value">{len(self.results)}</div><div>Jobs Analyzed</div></div>
  <div class="metric"><div class="value">{sum(1 for r in self.results if r.get('status')=='success')}</div><div>Successful</div></div>
  <div class="metric"><div class="value">{self.total_tokens['input']:,}</div><div>Input Tokens</div></div>
  <div class="metric"><div class="value">{self.total_tokens['output']:,}</div><div>Output Tokens</div></div>
</div>
"""
        for r in self.results:
            status_class = 'success' if r.get('status') == 'success' else 'error'
            html += f"""
<div class="card">
<h2>{r.get('job_name', 'Unknown')}</h2>
<p>Status: <span class="{status_class}">{r.get('status', 'unknown')}</span> |
   Time: {r.get('elapsed_seconds', 0):.1f}s |
   Tokens: {r.get('tokens', {}).get('input', 0)}in / {r.get('tokens', {}).get('output', 0)}out</p>
<pre>{r.get('analysis', r.get('error', 'No analysis'))}</pre>
</div>
"""

        html += "</div></body></html>"
        return html

    def _print_summary(self):
        """Print final summary."""
        print(f"\n{'='*70}")
        print(f"  ANALYSIS COMPLETE")
        print(f"{'='*70}")
        print(f"  Jobs Analyzed: {len(self.results)}")
        print(f"  Successful:    {sum(1 for r in self.results if r.get('status')=='success')}")
        print(f"  Failed:        {sum(1 for r in self.results if r.get('status')!='success')}")
        print(f"{'─'*70}")
        print(f"  Total Tokens:  {self.total_tokens['input']:,} input / {self.total_tokens['output']:,} output")
        print(f"  Est. Cost:     ${(self.total_tokens['input']*0.003 + self.total_tokens['output']*0.015)/1000:.4f}")
        print(f"{'='*70}\n")


def main():
    parser = argparse.ArgumentParser(
        description='Strands SDK ETL Optimizer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/strands_optimizer.py -s ./demo_configs/ -d ./reports/

Environment:
  AWS_PROFILE or AWS credentials for S3/Glue access
  STRANDS_MODEL to override model (default: claude-sonnet-4-6)
"""
    )
    parser.add_argument('--source', '-s', required=True, help='Directory with config JSONs')
    parser.add_argument('--dest', '-d', required=True, help='Output directory')
    parser.add_argument('--model', '-m', help='Model to use (default: claude-sonnet-4-6)')
    args = parser.parse_args()

    optimizer = StrandsOptimizer(args.source, args.dest, args.model)
    optimizer.run()


if __name__ == '__main__':
    main()
