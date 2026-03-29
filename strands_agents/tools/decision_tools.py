"""
Decision Agent Tools - Platform selection and workload analysis
"""
import json
from typing import Dict, Any, List
from strands import tool
import boto3

@tool
def analyze_workload_characteristics(
    data_volume_gb: float,
    file_count: int,
    transformation_complexity: str,
    query_pattern: str
) -> Dict[str, Any]:
    """
    Analyze workload characteristics to determine optimal processing platform.

    Args:
        data_volume_gb: Size of data in gigabytes
        file_count: Number of files to process
        transformation_complexity: One of 'simple', 'moderate', 'complex'
        query_pattern: One of 'batch', 'streaming', 'interactive'

    Returns:
        Workload analysis with recommendations
    """
    complexity_scores = {'simple': 1, 'moderate': 2, 'complex': 3}
    complexity_score = complexity_scores.get(transformation_complexity, 2)

    # Small files problem detection
    small_files_issue = file_count > 1000 and (data_volume_gb / file_count) < 0.005

    # Calculate workload intensity
    intensity = (data_volume_gb / 100) * complexity_score

    analysis = {
        'data_volume_gb': data_volume_gb,
        'file_count': file_count,
        'avg_file_size_mb': (data_volume_gb * 1024) / file_count if file_count > 0 else 0,
        'transformation_complexity': transformation_complexity,
        'complexity_score': complexity_score,
        'query_pattern': query_pattern,
        'workload_intensity': round(intensity, 2),
        'small_files_detected': small_files_issue,
        'recommendations': []
    }

    # Add recommendations
    if small_files_issue:
        analysis['recommendations'].append({
            'type': 'small_files',
            'severity': 'high',
            'message': f'Detected {file_count} small files (avg {analysis["avg_file_size_mb"]:.2f}MB). Consider coalescing.'
        })

    return analysis


@tool
def calculate_platform_costs(
    platform: str,
    data_volume_gb: float,
    duration_minutes: float,
    dpu_count: int = 2
) -> Dict[str, Any]:
    """
    Calculate estimated cost for running job on a specific platform.

    Args:
        platform: One of 'glue', 'emr', 'lambda', 'batch'
        data_volume_gb: Size of data to process
        duration_minutes: Estimated processing time
        dpu_count: Number of DPUs (for Glue) or nodes (for EMR)

    Returns:
        Cost breakdown and estimates
    """
    costs = {
        'glue': {
            'compute_per_dpu_hour': 0.44,
            'min_duration_minutes': 1,
            'storage_per_gb': 0.023
        },
        'emr': {
            'compute_per_node_hour': 0.27,  # m5.xlarge
            'min_duration_minutes': 1,
            'storage_per_gb': 0.023
        },
        'lambda': {
            'compute_per_gb_second': 0.0000166667,
            'requests_per_million': 0.20,
            'max_duration_minutes': 15
        },
        'batch': {
            'compute_per_vcpu_hour': 0.04048,
            'storage_per_gb': 0.023
        }
    }

    if platform not in costs:
        return {'error': f'Unknown platform: {platform}'}

    config = costs[platform]
    duration_hours = duration_minutes / 60

    if platform == 'glue':
        compute_cost = config['compute_per_dpu_hour'] * dpu_count * duration_hours
        storage_cost = config['storage_per_gb'] * data_volume_gb
        total_cost = compute_cost + storage_cost

        return {
            'platform': platform,
            'compute_cost_usd': round(compute_cost, 4),
            'storage_cost_usd': round(storage_cost, 4),
            'total_cost_usd': round(total_cost, 4),
            'cost_per_gb_usd': round(total_cost / data_volume_gb, 4) if data_volume_gb > 0 else 0,
            'dpu_count': dpu_count,
            'duration_hours': round(duration_hours, 2)
        }

    elif platform == 'emr':
        compute_cost = config['compute_per_node_hour'] * dpu_count * duration_hours
        storage_cost = config['storage_per_gb'] * data_volume_gb
        total_cost = compute_cost + storage_cost

        return {
            'platform': platform,
            'compute_cost_usd': round(compute_cost, 4),
            'storage_cost_usd': round(storage_cost, 4),
            'total_cost_usd': round(total_cost, 4),
            'cost_per_gb_usd': round(total_cost / data_volume_gb, 4) if data_volume_gb > 0 else 0,
            'node_count': dpu_count,
            'duration_hours': round(duration_hours, 2)
        }

    elif platform == 'lambda':
        if duration_minutes > config['max_duration_minutes']:
            return {
                'error': f'Lambda max duration is {config["max_duration_minutes"]} minutes',
                'platform': platform
            }

        # Assume 3GB memory
        memory_gb = 3
        duration_seconds = duration_minutes * 60
        compute_cost = config['compute_per_gb_second'] * memory_gb * duration_seconds
        request_cost = config['requests_per_million'] / 1_000_000
        total_cost = compute_cost + request_cost

        return {
            'platform': platform,
            'compute_cost_usd': round(compute_cost, 4),
            'request_cost_usd': round(request_cost, 6),
            'total_cost_usd': round(total_cost, 4),
            'cost_per_gb_usd': round(total_cost / data_volume_gb, 4) if data_volume_gb > 0 else 0,
            'memory_gb': memory_gb,
            'duration_minutes': duration_minutes
        }

    return {'error': 'Cost calculation not implemented for this platform'}


@tool
def recommend_platform(
    workload_analysis: Dict[str, Any],
    priority: str = 'balanced'
) -> Dict[str, Any]:
    """
    Recommend optimal platform based on workload analysis.

    Args:
        workload_analysis: Output from analyze_workload_characteristics
        priority: One of 'cost', 'speed', 'balanced'

    Returns:
        Platform recommendation with reasoning
    """
    data_volume = workload_analysis['data_volume_gb']
    complexity = workload_analysis['complexity_score']
    intensity = workload_analysis['workload_intensity']
    query_pattern = workload_analysis['query_pattern']

    # Decision logic
    recommendations = []

    # Lambda: Best for small, simple workloads
    if data_volume < 1 and complexity == 1:
        recommendations.append({
            'platform': 'lambda',
            'score': 95,
            'reasoning': 'Small data volume (<1GB) with simple transformations - Lambda is most cost-effective',
            'estimated_duration_minutes': 2,
            'suitability': 'excellent'
        })

    # Glue: Good for medium workloads, serverless
    if 1 <= data_volume <= 500:
        score = 85 if priority == 'balanced' else 75
        recommendations.append({
            'platform': 'glue',
            'score': score,
            'reasoning': 'Medium data volume with serverless benefits - Glue provides good balance',
            'estimated_duration_minutes': max(10, data_volume / 10),
            'suitability': 'good'
        })

    # EMR: Best for large, complex workloads
    if data_volume > 100 or complexity >= 2:
        score = 90 if priority == 'speed' else 80
        recommendations.append({
            'platform': 'emr',
            'score': score,
            'reasoning': 'Large data volume or complex transformations - EMR provides best performance',
            'estimated_duration_minutes': max(15, data_volume / 20),
            'suitability': 'excellent' if data_volume > 500 else 'good'
        })

    # Sort by score
    recommendations.sort(key=lambda x: x['score'], reverse=True)

    if not recommendations:
        recommendations.append({
            'platform': 'glue',
            'score': 70,
            'reasoning': 'Default recommendation for general ETL workloads',
            'estimated_duration_minutes': max(10, data_volume / 10),
            'suitability': 'good'
        })

    return {
        'recommended_platform': recommendations[0]['platform'],
        'confidence_score': recommendations[0]['score'],
        'reasoning': recommendations[0]['reasoning'],
        'estimated_duration_minutes': recommendations[0]['estimated_duration_minutes'],
        'all_options': recommendations,
        'priority_used': priority
    }


@tool
def query_execution_history(
    job_name: str,
    limit: int = 10
) -> Dict[str, Any]:
    """
    Query historical execution data for similar jobs from DynamoDB.

    Args:
        job_name: Name of the job to search for
        limit: Maximum number of records to return

    Returns:
        Historical execution data
    """
    # This would connect to DynamoDB in production
    # For now, return mock data structure
    return {
        'job_name': job_name,
        'total_executions': 0,
        'recent_executions': [],
        'avg_duration_minutes': 0,
        'avg_cost_usd': 0,
        'success_rate': 0.0,
        'note': 'Connect to DynamoDB table StrandsJobExecutions in production'
    }
