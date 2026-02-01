#!/usr/bin/env python3
"""
Full Pipeline Analysis Script for Strands ETL Framework
Runs comprehensive analysis including patterns, costs, performance, and recommendations.
"""

import json
import os
import sys
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from learning_module import LearningModule, DateTimeEncoder


def run_full_analysis():
    """Run comprehensive pipeline analysis."""
    print("=" * 70)
    print("STRANDS ETL FRAMEWORK - FULL PIPELINE ANALYSIS")
    print("=" * 70)
    print(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 70)

    # Initialize learning module
    learning = LearningModule()

    # Get all data
    print("\n[1/6] Fetching execution data...")
    vectors = learning.get_all_vectors(limit=100)

    if not vectors:
        print("\nNo execution data available.")
        print("Run some ETL jobs first to generate data for analysis.")
        return

    print(f"      Found {len(vectors)} execution records")

    # Pattern Analysis
    print("\n[2/6] Analyzing patterns...")
    patterns = learning.analyze_patterns()

    # Cost Analysis
    print("\n[3/6] Analyzing costs...")
    cost_report = learning.generate_report('cost')

    # Performance Analysis
    print("\n[4/6] Analyzing performance...")
    performance_report = learning.generate_report('performance')

    # Train predictor
    print("\n[5/6] Training platform predictor...")
    training_result = learning.train_platform_predictor()

    # Get AI insights
    print("\n[6/6] Getting AI insights...")
    try:
        insights = learning.get_insights(
            "Provide a comprehensive analysis of my ETL pipeline performance, "
            "including key findings, areas of concern, and actionable recommendations."
        )
    except Exception as e:
        insights = {'error': str(e), 'message': 'AI insights unavailable'}

    # Print Results
    print("\n" + "=" * 70)
    print("ANALYSIS RESULTS")
    print("=" * 70)

    # Summary
    print("\n" + "-" * 70)
    print("1. EXECUTIVE SUMMARY")
    print("-" * 70)

    summary = patterns.get('summary', {})
    print(f"""
    Total Executions:    {summary.get('total_runs', 0)}
    Success Rate:        {summary.get('success_rate', 0):.1%}
    Failed Runs:         {summary.get('failed_runs', 0)}
    Avg Execution Time:  {summary.get('avg_execution_time_seconds', 0):.1f} seconds
    Total Cost:          ${summary.get('total_estimated_cost_usd', 0):.2f}
    Avg Cost per Run:    ${summary.get('avg_cost_per_run_usd', 0):.4f}
    """)

    # Platform Analysis
    print("-" * 70)
    print("2. PLATFORM ANALYSIS")
    print("-" * 70)

    platform_analysis = patterns.get('platform_analysis', {})
    for platform, data in platform_analysis.items():
        print(f"""
    {platform.upper()}:
        Runs:           {data.get('count', 0)}
        Success Rate:   {data.get('success_rate', 0):.1%}
        Avg Time:       {data.get('avg_execution_time', 0):.1f}s
        Avg Cost:       ${data.get('avg_cost', 0):.4f}
        """)

    # Workload Analysis
    print("-" * 70)
    print("3. WORKLOAD ANALYSIS")
    print("-" * 70)

    workload_analysis = patterns.get('workload_analysis', {})
    for workload, data in workload_analysis.items():
        print(f"""
    {workload}:
        Runs:            {data.get('count', 0)}
        Most Used:       {data.get('most_used_platform', 'N/A')}
        Avg Time:        {data.get('avg_execution_time', 0):.1f}s
        """)

    # Performance Trends
    print("-" * 70)
    print("4. PERFORMANCE TRENDS")
    print("-" * 70)

    trends = patterns.get('performance_trends', {})
    if 'message' not in trends:
        print(f"""
    Period 1 (Earlier):
        Executions:     {trends.get('period_1', {}).get('count', 0)}
        Avg Time:       {trends.get('period_1', {}).get('avg_execution_time', 0):.1f}s
        Avg Cost:       ${trends.get('period_1', {}).get('avg_cost', 0):.4f}

    Period 2 (Recent):
        Executions:     {trends.get('period_2', {}).get('count', 0)}
        Avg Time:       {trends.get('period_2', {}).get('avg_execution_time', 0):.1f}s
        Avg Cost:       ${trends.get('period_2', {}).get('avg_cost', 0):.4f}

    Trends:
        Execution Time: {trends.get('trends', {}).get('execution_time_trend', 'unknown').upper()}
        Cost:           {trends.get('trends', {}).get('cost_trend', 'unknown').upper()}
    """)
    else:
        print(f"    {trends.get('message')}")

    # Failure Analysis
    print("-" * 70)
    print("5. FAILURE ANALYSIS")
    print("-" * 70)

    failures = patterns.get('failure_analysis', {})
    if 'message' not in failures:
        print(f"""
    Total Failures:     {failures.get('total_failures', 0)}
    Failure Rate:       {failures.get('failure_rate', 0):.1%}

    Top Failure Reasons:""")
        for reason, count in failures.get('top_failure_reasons', {}).items():
            print(f"        - {reason[:60]}... ({count} occurrences)")

        print("\n    Failures by Platform:")
        for platform, count in failures.get('failures_by_platform', {}).items():
            print(f"        - {platform}: {count}")
    else:
        print(f"    {failures.get('message')}")

    # Cost Breakdown
    print("\n" + "-" * 70)
    print("6. COST BREAKDOWN")
    print("-" * 70)

    cost_analysis = cost_report.get('cost_analysis', {})
    by_platform = cost_analysis.get('by_platform', {})

    print("\n    By Platform:")
    for platform, data in by_platform.items():
        print(f"""
        {platform.upper()}:
            Total:  ${data.get('total_cost', 0):.2f}
            Avg:    ${data.get('avg_cost', 0):.4f}
            Range:  ${data.get('min_cost', 0):.4f} - ${data.get('max_cost', 0):.4f}
        """)

    print("    By Data Volume:")
    for volume, cost in cost_analysis.get('by_data_volume', {}).items():
        if cost:
            print(f"        {volume}: ${cost:.4f} avg")

    # Trained Model
    print("-" * 70)
    print("7. PLATFORM PREDICTOR MODEL")
    print("-" * 70)

    if 'error' not in training_result:
        print(f"""
    Model ID:           {training_result.get('model_id', 'N/A')}
    Training Samples:   {training_result.get('training_samples', 0)}
    Trained At:         {training_result.get('trained_at', 'N/A')}

    Learned Rules:""")
        for rule, data in training_result.get('rules', {}).items():
            print(f"        {rule}: {data.get('recommended_platform', 'N/A')} "
                  f"(confidence: {data.get('confidence', 0):.0%})")
    else:
        print(f"    {training_result.get('error')}")

    # Recommendations
    print("\n" + "-" * 70)
    print("8. RECOMMENDATIONS")
    print("-" * 70)

    recommendations = patterns.get('recommendations', [])
    for i, rec in enumerate(recommendations, 1):
        print(f"""
    {i}. [{rec.get('type', 'general').upper()}]
       {rec.get('recommendation', 'N/A')}
       (Confidence: {rec.get('confidence', 0):.0%})
    """)

    # AI Insights
    print("-" * 70)
    print("9. AI-POWERED INSIGHTS")
    print("-" * 70)

    if 'error' not in insights:
        print(f"\n    {insights.get('answer', 'No insights available')[:500]}...")

        if insights.get('key_findings'):
            print("\n    Key Findings:")
            for finding in insights.get('key_findings', [])[:5]:
                print(f"        - {finding}")

        if insights.get('recommendations'):
            print("\n    AI Recommendations:")
            for rec in insights.get('recommendations', [])[:5]:
                print(f"        - {rec}")
    else:
        print(f"    AI insights unavailable: {insights.get('error', 'Unknown error')}")

    # Platform Conversion Opportunities
    print("\n" + "-" * 70)
    print("10. PLATFORM CONVERSION OPPORTUNITIES")
    print("-" * 70)

    conversion_opportunities = find_conversion_opportunities(vectors)
    if conversion_opportunities:
        for opp in conversion_opportunities[:5]:
            print(f"""
    Workload: {opp['workload']}
        Current Platform:    {opp['current']}
        Suggested Platform:  {opp['suggested']}
        Reason:              {opp['reason']}
        Est. Savings:        {opp['savings']}
            """)
    else:
        print("    No conversion opportunities identified.")

    # Save full report
    print("\n" + "=" * 70)
    print("SAVING FULL REPORT")
    print("=" * 70)

    full_report = {
        'generated_at': datetime.utcnow().isoformat(),
        'summary': summary,
        'platform_analysis': platform_analysis,
        'workload_analysis': workload_analysis,
        'performance_trends': trends,
        'failure_analysis': failures,
        'cost_analysis': cost_analysis,
        'trained_model': training_result,
        'recommendations': recommendations,
        'ai_insights': insights,
        'conversion_opportunities': conversion_opportunities
    }

    output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'output')
    os.makedirs(output_dir, exist_ok=True)

    report_path = os.path.join(output_dir, 'full_analysis_report.json')
    with open(report_path, 'w') as f:
        json.dump(full_report, f, indent=2, cls=DateTimeEncoder)

    print(f"\nFull report saved to: {report_path}")
    print("\n" + "=" * 70)


def find_conversion_opportunities(vectors):
    """Find opportunities to convert between platforms."""
    opportunities = []

    for v in vectors:
        workload = v.get('workload', {})
        execution = v.get('execution', {})
        metrics = v.get('metrics', {})

        current_platform = execution.get('platform')
        exec_time = metrics.get('execution_time_seconds', 0)
        data_volume = workload.get('data_volume')
        complexity = workload.get('complexity')

        # Glue to Lambda opportunity
        if current_platform == 'glue':
            if data_volume == 'low' and complexity == 'low' and exec_time < 300:
                opportunities.append({
                    'workload': workload.get('name', 'Unknown'),
                    'current': 'glue',
                    'suggested': 'lambda',
                    'reason': 'Low volume, low complexity, short runtime',
                    'savings': '50-70%'
                })

        # Lambda to Glue opportunity
        if current_platform == 'lambda':
            if data_volume in ['high', 'medium'] or complexity == 'high':
                opportunities.append({
                    'workload': workload.get('name', 'Unknown'),
                    'current': 'lambda',
                    'suggested': 'glue',
                    'reason': 'Higher volume/complexity may benefit from Spark',
                    'savings': 'Better scalability'
                })

        # Glue to EMR opportunity
        if current_platform == 'glue':
            if data_volume == 'high' and exec_time > 3600:
                opportunities.append({
                    'workload': workload.get('name', 'Unknown'),
                    'current': 'glue',
                    'suggested': 'emr',
                    'reason': 'Long-running high-volume job',
                    'savings': '20-40% with custom EMR cluster'
                })

    # Remove duplicates by workload name
    seen = set()
    unique = []
    for opp in opportunities:
        if opp['workload'] not in seen:
            seen.add(opp['workload'])
            unique.append(opp)

    return unique


if __name__ == '__main__':
    run_full_analysis()
