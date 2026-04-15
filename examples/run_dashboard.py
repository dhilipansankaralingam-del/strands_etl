#!/usr/bin/env python3
"""
Dashboard Generator for Strands ETL Framework
Generates an HTML dashboard with visualizations of ETL performance.
"""

import json
import os
import sys
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from learning_module import LearningModule


def generate_dashboard():
    """Generate HTML dashboard from learning data."""
    print("Strands ETL Dashboard Generator")
    print("=" * 40)

    # Initialize learning module
    learning = LearningModule()

    # Get data
    print("\nFetching data...")
    summary_report = learning.generate_report('summary')
    cost_report = learning.generate_report('cost')
    performance_report = learning.generate_report('performance')
    vectors = learning.get_all_vectors(limit=50)

    # Generate HTML
    html = generate_html(summary_report, cost_report, performance_report, vectors)

    # Save dashboard
    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'output')
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, 'dashboard.html')
    with open(output_path, 'w') as f:
        f.write(html)

    print(f"\nDashboard generated: {output_path}")

    # Also print summary to console
    print("\n" + "=" * 40)
    print("DASHBOARD SUMMARY")
    print("=" * 40)

    summary = summary_report.get('summary', {})
    print(f"\nTotal Runs: {summary.get('total_runs', 0)}")
    print(f"Success Rate: {summary.get('success_rate', 0):.1%}")
    print(f"Avg Execution Time: {summary.get('avg_execution_time_seconds', 0):.1f}s")
    print(f"Total Cost: ${summary.get('total_estimated_cost_usd', 0):.2f}")

    print("\nPlatform Usage:")
    for platform, data in summary_report.get('platform_usage', {}).items():
        print(f"  - {platform}: {data.get('count', 0)} runs ({data.get('success_rate', 0):.1%} success)")

    return output_path


def generate_html(summary_report, cost_report, performance_report, vectors):
    """Generate HTML dashboard content."""
    summary = summary_report.get('summary', {})
    platform_usage = summary_report.get('platform_usage', {})
    cost_analysis = cost_report.get('cost_analysis', {})
    performance_trends = performance_report.get('performance_trends', {})

    # Generate recent executions table
    recent_executions = ""
    for v in vectors[:10]:
        status = v.get('execution', {}).get('status', 'unknown')
        status_class = 'success' if status == 'completed' else 'error'
        recent_executions += f"""
        <tr>
            <td>{v.get('timestamp', 'N/A')[:19]}</td>
            <td>{v.get('workload', {}).get('name', 'N/A')}</td>
            <td>{v.get('execution', {}).get('platform', 'N/A')}</td>
            <td class="{status_class}">{status}</td>
            <td>{v.get('metrics', {}).get('execution_time_seconds', 'N/A')}s</td>
            <td>${v.get('metrics', {}).get('estimated_cost_usd', 0):.4f}</td>
        </tr>
        """

    # Generate platform stats
    platform_stats = ""
    for platform, data in platform_usage.items():
        platform_stats += f"""
        <div class="stat-card">
            <h3>{platform.upper()}</h3>
            <p class="big-number">{data.get('count', 0)}</p>
            <p>runs</p>
            <p>Success: {data.get('success_rate', 0):.1%}</p>
            <p>Avg Time: {data.get('avg_execution_time', 0):.1f}s</p>
        </div>
        """

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Strands ETL Dashboard</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f7fa;
            color: #333;
            line-height: 1.6;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }}
        header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
        }}
        header h1 {{
            font-size: 2rem;
            margin-bottom: 10px;
        }}
        header p {{
            opacity: 0.9;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .summary-card {{
            background: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
            text-align: center;
        }}
        .summary-card h3 {{
            color: #666;
            font-size: 0.9rem;
            text-transform: uppercase;
            letter-spacing: 1px;
            margin-bottom: 10px;
        }}
        .summary-card .value {{
            font-size: 2.5rem;
            font-weight: bold;
            color: #333;
        }}
        .summary-card .unit {{
            color: #888;
            font-size: 0.9rem;
        }}
        .summary-card.success .value {{ color: #10b981; }}
        .summary-card.cost .value {{ color: #f59e0b; }}
        .summary-card.time .value {{ color: #3b82f6; }}

        .section {{
            background: white;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.08);
            margin-bottom: 30px;
            overflow: hidden;
        }}
        .section-header {{
            padding: 20px 25px;
            border-bottom: 1px solid #eee;
        }}
        .section-header h2 {{
            font-size: 1.2rem;
            color: #333;
        }}
        .section-content {{
            padding: 25px;
        }}

        .platform-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 20px;
        }}
        .stat-card {{
            background: #f8fafc;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }}
        .stat-card h3 {{
            color: #667eea;
            margin-bottom: 10px;
        }}
        .stat-card .big-number {{
            font-size: 2rem;
            font-weight: bold;
            color: #333;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        th, td {{
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }}
        th {{
            background: #f8fafc;
            font-weight: 600;
            color: #666;
            text-transform: uppercase;
            font-size: 0.8rem;
            letter-spacing: 0.5px;
        }}
        tr:hover {{
            background: #f8fafc;
        }}
        .success {{ color: #10b981; font-weight: 600; }}
        .error {{ color: #ef4444; font-weight: 600; }}

        .trends {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
        }}
        .trend-card {{
            background: #f8fafc;
            padding: 20px;
            border-radius: 8px;
        }}
        .trend-card h4 {{
            color: #666;
            margin-bottom: 15px;
        }}
        .trend-improving {{ color: #10b981; }}
        .trend-declining {{ color: #ef4444; }}

        .cost-breakdown {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
        }}
        .cost-item {{
            background: #f8fafc;
            padding: 20px;
            border-radius: 8px;
        }}
        .cost-item h4 {{
            color: #666;
            margin-bottom: 10px;
        }}
        .cost-item .amount {{
            font-size: 1.5rem;
            font-weight: bold;
            color: #f59e0b;
        }}

        footer {{
            text-align: center;
            padding: 20px;
            color: #888;
            font-size: 0.9rem;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>Strands ETL Dashboard</h1>
            <p>Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</p>
        </header>

        <div class="summary-grid">
            <div class="summary-card">
                <h3>Total Runs</h3>
                <div class="value">{summary.get('total_runs', 0)}</div>
                <div class="unit">executions</div>
            </div>
            <div class="summary-card success">
                <h3>Success Rate</h3>
                <div class="value">{summary.get('success_rate', 0):.0%}</div>
                <div class="unit">{summary.get('successful_runs', 0)} / {summary.get('total_runs', 0)}</div>
            </div>
            <div class="summary-card time">
                <h3>Avg Execution Time</h3>
                <div class="value">{summary.get('avg_execution_time_seconds', 0):.0f}</div>
                <div class="unit">seconds</div>
            </div>
            <div class="summary-card cost">
                <h3>Total Cost</h3>
                <div class="value">${summary.get('total_estimated_cost_usd', 0):.2f}</div>
                <div class="unit">USD</div>
            </div>
        </div>

        <div class="section">
            <div class="section-header">
                <h2>Platform Usage</h2>
            </div>
            <div class="section-content">
                <div class="platform-grid">
                    {platform_stats if platform_stats else '<p>No platform data available</p>'}
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-header">
                <h2>Performance Trends</h2>
            </div>
            <div class="section-content">
                <div class="trends">
                    <div class="trend-card">
                        <h4>Execution Time Trend</h4>
                        <p class="{('trend-improving' if performance_trends.get('trends', {}).get('execution_time_trend') == 'improving' else 'trend-declining')}">
                            {performance_trends.get('trends', {}).get('execution_time_trend', 'N/A').upper()}
                        </p>
                    </div>
                    <div class="trend-card">
                        <h4>Cost Trend</h4>
                        <p class="{('trend-improving' if performance_trends.get('trends', {}).get('cost_trend') == 'improving' else 'trend-declining')}">
                            {performance_trends.get('trends', {}).get('cost_trend', 'N/A').upper()}
                        </p>
                    </div>
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-header">
                <h2>Cost Breakdown by Platform</h2>
            </div>
            <div class="section-content">
                <div class="cost-breakdown">
                    {''.join([f'''
                    <div class="cost-item">
                        <h4>{platform.upper()}</h4>
                        <div class="amount">${data.get('total_cost', 0):.2f}</div>
                        <p>Avg: ${data.get('avg_cost', 0):.2f} / run</p>
                    </div>
                    ''' for platform, data in cost_analysis.get('by_platform', {}).items()]) or '<p>No cost data available</p>'}
                </div>
            </div>
        </div>

        <div class="section">
            <div class="section-header">
                <h2>Recent Executions</h2>
            </div>
            <div class="section-content">
                <table>
                    <thead>
                        <tr>
                            <th>Timestamp</th>
                            <th>Workload</th>
                            <th>Platform</th>
                            <th>Status</th>
                            <th>Duration</th>
                            <th>Cost</th>
                        </tr>
                    </thead>
                    <tbody>
                        {recent_executions if recent_executions else '<tr><td colspan="6">No executions recorded</td></tr>'}
                    </tbody>
                </table>
            </div>
        </div>

        <footer>
            <p>Strands ETL Framework - Learning Module Dashboard</p>
        </footer>
    </div>
</body>
</html>"""

    return html


if __name__ == '__main__':
    generate_dashboard()
