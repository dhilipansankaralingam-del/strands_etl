"""
Cost Tracking Agent Tools - Cost analysis and optimization
"""
from typing import Dict, Any, List
from strands import tool

@tool
def calculate_total_execution_cost(
    platform_cost: float,
    storage_cost: float,
    network_cost: float,
    ai_cost: float = 0.0
) -> Dict[str, Any]:
    """
    Calculate total execution cost from all components.

    Args:
        platform_cost: Compute platform cost (Glue/EMR/Lambda)
        storage_cost: S3 storage and I/O cost
        network_cost: Data transfer cost
        ai_cost: AI/ML service cost (Bedrock, SageMaker)

    Returns:
        Complete cost breakdown
    """
    total_cost = platform_cost + storage_cost + network_cost + ai_cost

    breakdown = {
        'platform_cost_usd': round(platform_cost, 4),
        'storage_cost_usd': round(storage_cost, 4),
        'network_cost_usd': round(network_cost, 4),
        'ai_cost_usd': round(ai_cost, 4),
        'total_cost_usd': round(total_cost, 4),
        'cost_percentages': {
            'platform': round((platform_cost / total_cost * 100) if total_cost > 0 else 0, 1),
            'storage': round((storage_cost / total_cost * 100) if total_cost > 0 else 0, 1),
            'network': round((network_cost / total_cost * 100) if total_cost > 0 else 0, 1),
            'ai': round((ai_cost / total_cost * 100) if total_cost > 0 else 0, 1)
        }
    }

    return breakdown


@tool
def analyze_cost_trends(
    current_cost: float,
    previous_costs: List[float],
    time_period_days: int
) -> Dict[str, Any]:
    """
    Analyze cost trends over time.

    Args:
        current_cost: Current execution cost
        previous_costs: List of previous execution costs
        time_period_days: Time period covered by previous_costs

    Returns:
        Cost trend analysis with projections
    """
    if not previous_costs:
        return {
            'trend': 'insufficient_data',
            'message': 'Need historical data for trend analysis'
        }

    avg_previous = sum(previous_costs) / len(previous_costs)
    cost_change = current_cost - avg_previous
    cost_change_pct = (cost_change / avg_previous * 100) if avg_previous > 0 else 0

    # Determine trend
    if abs(cost_change_pct) < 5:
        trend = 'stable'
    elif cost_change_pct > 0:
        trend = 'increasing'
    else:
        trend = 'decreasing'

    # Project monthly cost
    daily_cost = current_cost
    monthly_projection = daily_cost * 30

    return {
        'current_cost_usd': round(current_cost, 4),
        'average_previous_cost_usd': round(avg_previous, 4),
        'cost_change_usd': round(cost_change, 4),
        'cost_change_percentage': round(cost_change_pct, 2),
        'trend': trend,
        'monthly_projection_usd': round(monthly_projection, 2),
        'alert_level': 'high' if cost_change_pct > 20 else 'medium' if cost_change_pct > 10 else 'low'
    }


@tool
def identify_cost_optimization_opportunities(
    execution_metrics: Dict[str, Any],
    cost_breakdown: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Identify opportunities to reduce costs.

    Args:
        execution_metrics: Metrics like duration, data volume, resource utilization
        cost_breakdown: Current cost breakdown

    Returns:
        List of optimization opportunities
    """
    opportunities = []

    # Check CPU/memory utilization
    if execution_metrics.get('cpu_utilization_avg', 100) < 50:
        potential_savings = cost_breakdown.get('platform_cost_usd', 0) * 0.3
        opportunities.append({
            'category': 'resource_sizing',
            'finding': f'Low CPU utilization ({execution_metrics.get("cpu_utilization_avg")}%)',
            'recommendation': 'Reduce DPU/node count or instance size',
            'potential_savings_usd': round(potential_savings, 2),
            'effort': 'low'
        })

    # Check for small files
    if execution_metrics.get('output_file_count', 0) > 1000:
        potential_savings = cost_breakdown.get('storage_cost_usd', 0) * 0.5
        opportunities.append({
            'category': 'small_files',
            'finding': f'Writing {execution_metrics.get("output_file_count")} small files',
            'recommendation': 'Use .coalesce(N) to reduce file count',
            'potential_savings_usd': round(potential_savings, 2),
            'effort': 'low'
        })

    # Check for full scans
    if execution_metrics.get('data_read_gb', 0) / execution_metrics.get('data_written_gb', 1) > 10:
        potential_savings = cost_breakdown.get('platform_cost_usd', 0) * 0.6
        opportunities.append({
            'category': 'data_scanning',
            'finding': 'Reading much more data than writing (possible full table scan)',
            'recommendation': 'Implement incremental/delta loading',
            'potential_savings_usd': round(potential_savings, 2),
            'effort': 'medium'
        })

    # Check platform cost percentage
    if cost_breakdown.get('cost_percentages', {}).get('platform', 0) > 70:
        opportunities.append({
            'category': 'platform_selection',
            'finding': 'Platform cost is >70% of total',
            'recommendation': 'Evaluate alternative platforms (Glue vs EMR vs Lambda)',
            'potential_savings_usd': round(cost_breakdown.get('platform_cost_usd', 0) * 0.2, 2),
            'effort': 'high'
        })

    total_potential_savings = sum(o['potential_savings_usd'] for o in opportunities)

    return {
        'opportunities_identified': len(opportunities),
        'total_potential_savings_usd': round(total_potential_savings, 2),
        'opportunities': sorted(opportunities, key=lambda x: x['potential_savings_usd'], reverse=True),
        'priority_actions': [o for o in opportunities if o['effort'] == 'low'][:3]
    }


@tool
def forecast_monthly_costs(
    daily_costs: List[float],
    growth_rate: float = 0.0
) -> Dict[str, Any]:
    """
    Forecast monthly costs based on historical data.

    Args:
        daily_costs: List of daily costs for past N days
        growth_rate: Expected growth rate (0.1 = 10% growth)

    Returns:
        Monthly cost forecast
    """
    if not daily_costs:
        return {'error': 'No cost data provided'}

    avg_daily_cost = sum(daily_costs) / len(daily_costs)

    # Base monthly projection (30 days)
    base_monthly = avg_daily_cost * 30

    # Adjusted for growth
    projected_monthly = base_monthly * (1 + growth_rate)

    # Calculate confidence based on data variance
    if len(daily_costs) > 1:
        variance = sum((c - avg_daily_cost) ** 2 for c in daily_costs) / len(daily_costs)
        std_dev = variance ** 0.5
        confidence = max(0, min(100, 100 - (std_dev / avg_daily_cost * 100))) if avg_daily_cost > 0 else 0
    else:
        confidence = 50

    return {
        'average_daily_cost_usd': round(avg_daily_cost, 2),
        'base_monthly_projection_usd': round(base_monthly, 2),
        'growth_adjusted_monthly_usd': round(projected_monthly, 2),
        'growth_rate_applied': growth_rate,
        'confidence_percentage': round(confidence, 1),
        'forecast_range': {
            'low_estimate_usd': round(projected_monthly * 0.85, 2),
            'high_estimate_usd': round(projected_monthly * 1.15, 2)
        }
    }
