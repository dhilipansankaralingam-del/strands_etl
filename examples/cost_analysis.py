#!/usr/bin/env python3
"""
Cost Analysis Script for Strands ETL Framework
Provides detailed cost analysis and optimization recommendations.
"""

import json
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from learning_module import LearningModule, DateTimeEncoder


def run_cost_analysis():
    """Run comprehensive cost analysis."""
    print("=" * 60)
    print("STRANDS ETL COST ANALYSIS")
    print("=" * 60)

    # Initialize learning module
    learning = LearningModule()

    # Get cost report
    print("\nFetching cost data...")
    cost_report = learning.generate_report('cost')
    vectors = learning.get_all_vectors(limit=100)

    if not vectors:
        print("\nNo execution data available for cost analysis.")
        print("Run some ETL jobs first to generate data.")
        return

    # Summary
    summary = cost_report.get('summary', {})
    cost_analysis = cost_report.get('cost_analysis', {})

    print("\n" + "-" * 60)
    print("COST SUMMARY")
    print("-" * 60)

    total_cost = summary.get('total_estimated_cost_usd', 0)
    total_runs = summary.get('total_runs', 0)
    avg_cost = summary.get('avg_cost_per_run_usd', 0)

    print(f"Total Cost:        ${total_cost:.2f}")
    print(f"Total Runs:        {total_runs}")
    print(f"Avg Cost per Run:  ${avg_cost:.4f}")

    # Cost by platform
    print("\n" + "-" * 60)
    print("COST BY PLATFORM")
    print("-" * 60)

    by_platform = cost_analysis.get('by_platform', {})
    for platform, data in by_platform.items():
        print(f"\n{platform.upper()}:")
        print(f"  Total Cost:  ${data.get('total_cost', 0):.2f}")
        print(f"  Avg Cost:    ${data.get('avg_cost', 0):.4f}")
        print(f"  Min Cost:    ${data.get('min_cost', 0):.4f}")
        print(f"  Max Cost:    ${data.get('max_cost', 0):.4f}")

    # Cost by data volume
    print("\n" + "-" * 60)
    print("COST BY DATA VOLUME")
    print("-" * 60)

    by_volume = cost_analysis.get('by_data_volume', {})
    for volume, avg in by_volume.items():
        if avg:
            print(f"  {volume.upper():10s}: ${avg:.4f} avg/run")

    # Cost optimization recommendations
    print("\n" + "-" * 60)
    print("COST OPTIMIZATION RECOMMENDATIONS")
    print("-" * 60)

    recommendations = generate_cost_recommendations(vectors, by_platform)
    for i, rec in enumerate(recommendations, 1):
        print(f"\n{i}. {rec['title']}")
        print(f"   {rec['description']}")
        print(f"   Potential Savings: {rec['savings']}")

    # Platform comparison
    print("\n" + "-" * 60)
    print("PLATFORM COST COMPARISON")
    print("-" * 60)

    if len(by_platform) > 1:
        sorted_platforms = sorted(
            by_platform.items(),
            key=lambda x: x[1].get('avg_cost', float('inf'))
        )
        cheapest = sorted_platforms[0]
        most_expensive = sorted_platforms[-1]

        print(f"\nMost Cost-Effective: {cheapest[0].upper()}")
        print(f"  Average cost: ${cheapest[1].get('avg_cost', 0):.4f}/run")

        print(f"\nMost Expensive: {most_expensive[0].upper()}")
        print(f"  Average cost: ${most_expensive[1].get('avg_cost', 0):.4f}/run")

        if cheapest[1].get('avg_cost', 0) > 0:
            savings_percent = (1 - cheapest[1]['avg_cost'] / most_expensive[1].get('avg_cost', 1)) * 100
            print(f"\nPotential savings by switching to {cheapest[0]}: {savings_percent:.1f}%")
    else:
        print("Not enough platform data for comparison.")

    # Monthly projection
    print("\n" + "-" * 60)
    print("MONTHLY COST PROJECTION")
    print("-" * 60)

    if total_runs > 0 and avg_cost > 0:
        # Assume average runs per day
        avg_daily = total_runs / 7  # Assume data is from 1 week
        monthly_runs = avg_daily * 30
        monthly_cost = monthly_runs * avg_cost

        print(f"Estimated monthly runs: {monthly_runs:.0f}")
        print(f"Projected monthly cost: ${monthly_cost:.2f}")

    print("\n" + "=" * 60)


def generate_cost_recommendations(vectors, by_platform):
    """Generate cost optimization recommendations."""
    recommendations = []

    # Check for low-volume jobs on expensive platforms
    low_volume_glue = [
        v for v in vectors
        if v.get('workload', {}).get('data_volume') == 'low'
        and v.get('execution', {}).get('platform') == 'glue'
    ]

    if low_volume_glue:
        recommendations.append({
            'title': 'Move Low-Volume Jobs to Lambda',
            'description': f'{len(low_volume_glue)} low-volume jobs are running on Glue. '
                          'Consider migrating to Lambda for cost savings.',
            'savings': 'Up to 50-70% per job'
        })

    # Check worker over-provisioning
    over_provisioned = [
        v for v in vectors
        if v.get('metrics', {}).get('execution_time_seconds', 0) < 120
        and v.get('execution', {}).get('platform') == 'glue'
    ]

    if over_provisioned:
        recommendations.append({
            'title': 'Reduce Worker Count for Short Jobs',
            'description': f'{len(over_provisioned)} Glue jobs completed in under 2 minutes. '
                          'Consider reducing worker count.',
            'savings': 'Up to 40% per job'
        })

    # Check for failed jobs (wasted cost)
    failed_jobs = [v for v in vectors if v.get('execution', {}).get('status') == 'failed']
    if failed_jobs:
        wasted_cost = sum(
            v.get('metrics', {}).get('estimated_cost_usd', 0)
            for v in failed_jobs
        )
        recommendations.append({
            'title': 'Address Job Failures',
            'description': f'{len(failed_jobs)} jobs failed, wasting ${wasted_cost:.2f}. '
                          'Investigate root causes.',
            'savings': f'${wasted_cost:.2f} potential recovery'
        })

    # Scheduling recommendation
    recommendations.append({
        'title': 'Schedule Non-Urgent Jobs Off-Peak',
        'description': 'Run non-critical ETL jobs during off-peak hours (nights/weekends) '
                      'for potential spot instance savings.',
        'savings': 'Up to 60-70% with spot instances'
    })

    if not recommendations:
        recommendations.append({
            'title': 'Continue Current Optimization',
            'description': 'Your current setup appears well-optimized. '
                          'Continue monitoring for opportunities.',
            'savings': 'N/A'
        })

    return recommendations


if __name__ == '__main__':
    run_cost_analysis()
