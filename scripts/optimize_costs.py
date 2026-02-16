#!/usr/bin/env python3
"""
PySpark Cost Optimizer CLI
==========================

Multi-agent system for analyzing PySpark scripts and recommending cost optimizations.

Single Script Analysis:
    python scripts/optimize_costs.py --script scripts/pyspark/my_job.py \
        --tables tables.json --mode delta

Batch Analysis (100-500 scripts):
    python scripts/optimize_costs.py --batch batch_config.json

    # Or scan a directory
    python scripts/optimize_costs.py --scan-dir scripts/pyspark/ --output reports/

Generate Batch Config:
    python scripts/optimize_costs.py --generate-batch --scan-dir scripts/pyspark/ \
        --output batch_config.json

Use LLM (Bedrock):
    python scripts/optimize_costs.py --script my_job.py --use-llm --model claude-sonnet
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from cost_optimizer import CostOptimizationOrchestrator, BatchAnalyzer


def analyze_single_script(args):
    """Analyze a single PySpark script."""
    print(f"\n{'='*70}")
    print(" PySpark Cost Optimization Analysis")
    print(f"{'='*70}")
    print(f" Script: {args.script}")
    print(f" Mode:   {args.mode}")
    print(f" LLM:    {'Yes' if args.use_llm else 'No (rule-based)'}")
    print(f"{'='*70}\n")

    # Load source tables
    source_tables = []
    if args.tables:
        with open(args.tables) as f:
            source_tables = json.load(f)
    elif args.tables_json:
        source_tables = json.loads(args.tables_json)

    # Load current config
    current_config = {}
    if args.config:
        with open(args.config) as f:
            current_config = json.load(f)

    # Create orchestrator
    orchestrator = CostOptimizationOrchestrator(
        use_llm=args.use_llm,
        model_id=args.model
    )

    # Run analysis
    result = orchestrator.analyze_script(
        script_path=args.script,
        source_tables=source_tables,
        processing_mode=args.mode,
        current_config=current_config,
        additional_context={
            'runs_per_day': args.runs_per_day,
            'runs_per_year': args.runs_per_day * 365
        }
    )

    # Print results
    if result['success']:
        print_analysis_result(result)

        # Save JSON report
        if args.output:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            print(f"\n Report saved to: {output_path}")
    else:
        print(f"Error: {result.get('error')}")
        sys.exit(1)

    return result


def analyze_batch(args):
    """Analyze multiple scripts from batch config."""

    # Load batch config
    with open(args.batch) as f:
        batch_config = json.load(f)

    scripts = batch_config.get('scripts', [])
    default_config = batch_config.get('default_config', {})

    if not scripts:
        print("Error: No scripts found in batch config")
        sys.exit(1)

    print(f"\nLoaded {len(scripts)} scripts from {args.batch}")

    # Create batch analyzer
    analyzer = BatchAnalyzer(
        use_llm=args.use_llm,
        model_id=args.model,
        max_workers=args.workers,
        output_dir=args.output or 'cost_optimizer/reports'
    )

    # Run analysis
    report = analyzer.analyze_batch(
        scripts=scripts,
        default_config=default_config
    )

    print(f"\n Report saved to: {report['report_path']}")

    # Generate HTML report if requested
    if args.html:
        html_path = generate_html_report(report, args.output or 'cost_optimizer/reports')
        print(f" HTML report: {html_path}")

    return report


def scan_directory(args):
    """Scan directory for PySpark scripts and analyze."""

    scan_dir = Path(args.scan_dir)
    if not scan_dir.exists():
        print(f"Error: Directory not found: {scan_dir}")
        sys.exit(1)

    # Find all Python files
    scripts = list(scan_dir.glob('**/*.py'))
    scripts = [s for s in scripts if not s.name.startswith('__')]

    print(f"Found {len(scripts)} Python scripts in {scan_dir}")

    if args.generate_batch:
        # Generate batch config
        batch_config = generate_batch_config(scripts, args)
        output_path = Path(args.output or 'batch_config.json')
        with open(output_path, 'w') as f:
            json.dump(batch_config, f, indent=2)
        print(f"Batch config saved to: {output_path}")
        print(f"\nTo run analysis:")
        print(f"  python scripts/optimize_costs.py --batch {output_path}")
        return

    # Build script configs
    script_configs = []
    for script in scripts:
        script_configs.append({
            'script_path': str(script),
            'source_tables': [],  # Will be detected from code
            'processing_mode': args.mode,
            'current_config': {
                'platform': 'glue',
                'number_of_workers': 10,
                'worker_type': 'G.2X'
            }
        })

    # Create batch analyzer
    analyzer = BatchAnalyzer(
        use_llm=args.use_llm,
        model_id=args.model,
        max_workers=args.workers,
        output_dir=args.output or 'cost_optimizer/reports'
    )

    # Run analysis
    report = analyzer.analyze_batch(scripts=script_configs)

    print(f"\n Report saved to: {report['report_path']}")

    return report


def generate_batch_config(scripts: list, args) -> dict:
    """Generate batch config from list of scripts."""
    script_configs = []

    for script in scripts:
        script_configs.append({
            'script_path': str(script),
            'source_tables': [],
            'processing_mode': args.mode,
            'current_config': {
                'platform': 'glue',
                'number_of_workers': 10,
                'worker_type': 'G.2X',
                'timeout_minutes': 120
            },
            'additional_context': {
                'runs_per_day': 1
            }
        })

    return {
        '_description': 'Batch cost optimization config',
        '_generated': datetime.utcnow().isoformat(),
        '_instructions': 'Edit source_tables and current_config for each script',
        'default_config': {
            'processing_mode': args.mode,
            'current_config': {
                'platform': 'glue',
                'number_of_workers': 10,
                'worker_type': 'G.2X'
            }
        },
        'scripts': script_configs
    }


def print_analysis_result(result: dict):
    """Print formatted analysis results."""
    summary = result.get('summary', {})
    exec_summary = result.get('executive_summary', {})
    roadmap = result.get('implementation_roadmap', {})
    recommendations = result.get('all_recommendations', [])

    print("\n" + "="*70)
    print(" EXECUTIVE SUMMARY")
    print("="*70)
    print(f"  {exec_summary.get('headline', 'Analysis complete')}")
    print()
    print(f"  Current Annual Spend:    {exec_summary.get('current_annual_spend', 'N/A')}")
    print(f"  Potential Annual Savings: {exec_summary.get('potential_annual_savings', 'N/A')}")
    print(f"  Critical Issues:          {exec_summary.get('critical_issues_count', 0)}")
    print(f"  Total Recommendations:    {exec_summary.get('total_recommendations', 0)}")
    print(f"  Quick Wins:               {exec_summary.get('quick_wins_count', 0)}")

    print("\n" + "-"*70)
    print(" KEY FINDINGS")
    print("-"*70)
    for finding in exec_summary.get('top_findings', []):
        print(f"  • {finding}")

    print("\n" + "-"*70)
    print(" COST BREAKDOWN")
    print("-"*70)
    print(f"  Effective Data Size:      {summary.get('effective_data_size_gb', 0):.0f} GB")
    print(f"  Current Cost/Run:         ${summary.get('current_cost_per_run', 0):.2f}")
    print(f"  Optimal Cost/Run:         ${summary.get('optimal_cost_per_run', 0):.2f}")
    print(f"  Savings Potential:        {summary.get('potential_savings_percent', 0):.0f}%")
    print(f"  Annual Savings:           ${summary.get('potential_annual_savings', 0):,.0f}")

    print("\n" + "-"*70)
    print(" IMPLEMENTATION ROADMAP")
    print("-"*70)

    for phase_key in ['phase_1', 'phase_2', 'phase_3', 'phase_4']:
        phase = roadmap.get(phase_key, {})
        if phase.get('actions'):
            print(f"\n  {phase.get('name', phase_key)}:")
            for action in phase.get('actions', [])[:3]:
                print(f"    → {action}")
            savings = phase.get('expected_savings_percent', 0)
            if savings:
                print(f"    Expected savings: {savings:.0f}%")

    print("\n" + "-"*70)
    print(" TOP RECOMMENDATIONS")
    print("-"*70)

    for i, rec in enumerate(recommendations[:10], 1):
        priority = rec.get('priority', 'P3')
        title = rec.get('title', 'Unknown')
        savings = rec.get('annual_savings_usd', 0)
        quick = " ⚡" if rec.get('quick_win') else ""

        print(f"\n  {i}. [{priority}] {title}{quick}")
        if rec.get('description'):
            print(f"     {rec['description'][:80]}...")
        if savings > 0:
            print(f"     Savings: ${savings:,.0f}/year | ROI: {rec.get('roi_percent', 0):.0f}%")

    print("\n" + "="*70 + "\n")


def generate_html_report(report: dict, output_dir: str) -> str:
    """Generate HTML report from batch results."""
    from pathlib import Path

    output_path = Path(output_dir) / f"cost_optimization_report_{report['batch_id']}.html"

    aggregated = report.get('aggregated_metrics', {})

    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Cost Optimization Report - {report['batch_id']}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 40px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; }}
        .card {{ background: white; border-radius: 8px; padding: 24px; margin-bottom: 24px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #1a1a1a; }}
        h2 {{ color: #333; border-bottom: 2px solid #4CAF50; padding-bottom: 8px; }}
        .metric {{ display: inline-block; margin: 16px 24px 16px 0; }}
        .metric-value {{ font-size: 32px; font-weight: bold; color: #4CAF50; }}
        .metric-label {{ font-size: 14px; color: #666; }}
        .savings {{ color: #4CAF50; }}
        .warning {{ color: #ff9800; }}
        .critical {{ color: #f44336; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background: #f8f9fa; }}
        .progress {{ background: #e0e0e0; border-radius: 4px; height: 8px; }}
        .progress-bar {{ background: #4CAF50; height: 8px; border-radius: 4px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 PySpark Cost Optimization Report</h1>
        <p>Generated: {report['analysis_timestamp']}</p>

        <div class="card">
            <h2>Executive Summary</h2>
            <div class="metric">
                <div class="metric-value">{aggregated.get('total_scripts_analyzed', 0)}</div>
                <div class="metric-label">Scripts Analyzed</div>
            </div>
            <div class="metric">
                <div class="metric-value savings">${aggregated.get('total_annual_savings', 0):,.0f}</div>
                <div class="metric-label">Potential Annual Savings</div>
            </div>
            <div class="metric">
                <div class="metric-value">{aggregated.get('average_savings_percent', 0):.0f}%</div>
                <div class="metric-label">Average Savings</div>
            </div>
            <div class="metric">
                <div class="metric-value critical">{aggregated.get('total_critical_issues', 0)}</div>
                <div class="metric-label">Critical Issues</div>
            </div>
            <div class="metric">
                <div class="metric-value">{aggregated.get('total_quick_wins', 0)}</div>
                <div class="metric-label">Quick Wins</div>
            </div>
        </div>

        <div class="card">
            <h2>Cost Analysis</h2>
            <table>
                <tr>
                    <td>Current Total Cost (per run)</td>
                    <td><strong>${aggregated.get('total_current_cost_per_run', 0):,.2f}</strong></td>
                </tr>
                <tr>
                    <td>Optimal Total Cost (per run)</td>
                    <td><strong>${aggregated.get('total_optimal_cost_per_run', 0):,.2f}</strong></td>
                </tr>
                <tr>
                    <td>Savings per Run</td>
                    <td class="savings"><strong>${aggregated.get('total_savings_per_run', 0):,.2f}</strong></td>
                </tr>
                <tr>
                    <td>Annual Savings (365 runs)</td>
                    <td class="savings"><strong>${aggregated.get('total_annual_savings', 0):,.0f}</strong></td>
                </tr>
            </table>
        </div>

        <div class="card">
            <h2>Top Anti-Patterns ({aggregated.get('total_anti_patterns', 0)} total)</h2>
            <table>
                <tr><th>Pattern</th><th>Occurrences</th><th>Impact</th></tr>
                {''.join(f"<tr><td>{p['pattern']}</td><td>{p['count']}</td><td class='warning'>Medium-High</td></tr>" for p in aggregated.get('top_anti_patterns', [])[:10])}
            </table>
        </div>

        <div class="card">
            <h2>Top Savings Opportunities</h2>
            <table>
                <tr><th>Script</th><th>Savings %</th><th>Annual Savings</th></tr>
                {''.join(f"<tr><td>{s['script']}</td><td>{s['savings_percent']:.0f}%</td><td class='savings'>${s['annual_savings']:,.0f}</td></tr>" for s in aggregated.get('scripts_by_savings', [])[:20])}
            </table>
        </div>

        <div class="card">
            <h2>Analysis Details</h2>
            <p>Total Analysis Time: {report.get('total_analysis_time_seconds', 0):.1f} seconds</p>
            <p>Successful: {report.get('successful', 0)} | Failed: {report.get('failed', 0)}</p>
        </div>
    </div>
</body>
</html>
"""

    with open(output_path, 'w') as f:
        f.write(html)

    return str(output_path)


def main():
    parser = argparse.ArgumentParser(
        description='PySpark Cost Optimization Analyzer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze single script
  python scripts/optimize_costs.py --script my_job.py --tables tables.json

  # Analyze with delta mode
  python scripts/optimize_costs.py --script my_job.py --mode delta

  # Batch analysis
  python scripts/optimize_costs.py --batch batch_config.json

  # Scan directory
  python scripts/optimize_costs.py --scan-dir scripts/pyspark/

  # Generate batch config
  python scripts/optimize_costs.py --generate-batch --scan-dir scripts/pyspark/

  # Use LLM (Bedrock)
  python scripts/optimize_costs.py --script my_job.py --use-llm
"""
    )

    # Input modes
    parser.add_argument('--script', '-s', help='Path to PySpark script')
    parser.add_argument('--batch', '-b', help='Path to batch config JSON')
    parser.add_argument('--scan-dir', help='Directory to scan for scripts')
    parser.add_argument('--generate-batch', action='store_true',
                        help='Generate batch config from scanned scripts')

    # Analysis options
    parser.add_argument('--tables', '-t', help='Path to source tables JSON')
    parser.add_argument('--tables-json', help='Source tables as JSON string')
    parser.add_argument('--config', '-c', help='Path to current job config JSON')
    parser.add_argument('--mode', '-m', default='full', choices=['full', 'delta'],
                        help='Processing mode (default: full)')
    parser.add_argument('--runs-per-day', type=int, default=1,
                        help='Number of runs per day for cost calculation')

    # LLM options
    parser.add_argument('--use-llm', action='store_true',
                        help='Use LLM for analysis (requires Bedrock)')
    parser.add_argument('--model', default='us.anthropic.claude-sonnet-4-20250514-v1:0',
                        help='Bedrock model ID')

    # Batch options
    parser.add_argument('--workers', '-w', type=int, default=4,
                        help='Number of parallel workers for batch')

    # Output options
    parser.add_argument('--output', '-o', help='Output path for report')
    parser.add_argument('--html', action='store_true', help='Generate HTML report')

    args = parser.parse_args()

    # Determine mode and run
    if args.script:
        analyze_single_script(args)
    elif args.batch:
        analyze_batch(args)
    elif args.scan_dir:
        scan_directory(args)
    else:
        parser.print_help()
        print("\nError: Provide --script, --batch, or --scan-dir")
        sys.exit(1)


if __name__ == '__main__':
    main()
