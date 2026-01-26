"""
Optimization Agent Tools - Performance tuning and resource optimization
"""
from typing import Dict, Any, List
from strands import tool

@tool
def suggest_spark_optimizations(
    execution_metrics: Dict[str, Any],
    script_issues: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Suggest Spark configuration and code optimizations.

    Args:
        execution_metrics: Execution metrics (memory, CPU, shuffle, etc)
        script_issues: Issues detected from script analysis

    Returns:
        Optimization suggestions
    """
    suggestions = []

    # Memory optimization
    memory_utilization = execution_metrics.get('memory_utilization_pct', 0)
    if memory_utilization > 90:
        suggestions.append({
            'category': 'memory',
            'priority': 'high',
            'suggestion': 'Increase executor memory or reduce partition size',
            'config': 'spark.executor.memory = 8g (increase from current)',
            'expected_impact': '30% faster, fewer OOM errors'
        })
    elif memory_utilization < 40:
        suggestions.append({
            'category': 'memory',
            'priority': 'medium',
            'suggestion': 'Reduce allocated memory to save costs',
            'config': 'spark.executor.memory = 4g (decrease from current)',
            'expected_impact': '20-30% cost reduction'
        })

    # Shuffle optimization
    shuffle_gb = execution_metrics.get('shuffle_read_gb', 0) + execution_metrics.get('shuffle_write_gb', 0)
    data_processed = execution_metrics.get('data_processed_gb', 1)

    if shuffle_gb / data_processed > 2:
        suggestions.append({
            'category': 'shuffle',
            'priority': 'high',
            'suggestion': 'Excessive shuffle detected - optimize joins and group-by operations',
            'config': 'spark.sql.shuffle.partitions = 200 (tune based on data size)',
            'expected_impact': '40-60% performance improvement'
        })

    # Script-based optimizations
    for issue in script_issues:
        if issue.get('type') == 'multiple_counts':
            suggestions.append({
                'category': 'code',
                'priority': 'high',
                'suggestion': 'Cache DataFrame before multiple count operations',
                'code_change': 'df.cache(); count = df.count(); # reuse count variable',
                'expected_impact': '40% faster execution'
            })

        if issue.get('type') == 'missing_broadcast':
            suggestions.append({
                'category': 'code',
                'priority': 'medium',
                'suggestion': 'Use broadcast hint for small dimension tables',
                'code_change': 'from pyspark.sql.functions import broadcast; df.join(broadcast(dim_df), "key")',
                'expected_impact': '30% faster joins'
            })

    return {
        'total_suggestions': len(suggestions),
        'high_priority': len([s for s in suggestions if s['priority'] == 'high']),
        'suggestions': suggestions,
        'estimated_total_improvement': '50-70%' if len(suggestions) > 2 else '20-30%'
    }


@tool
def optimize_resource_allocation(
    workload_size_gb: float,
    complexity: str,
    current_resources: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Recommend optimal resource allocation (DPUs, memory, cores).

    Args:
        workload_size_gb: Size of data to process
        complexity: Workload complexity ('simple', 'moderate', 'complex')
        current_resources: Current resource allocation

    Returns:
        Optimized resource recommendations
    """
    complexity_multiplier = {'simple': 1, 'moderate': 1.5, 'complex': 2}
    multiplier = complexity_multiplier.get(complexity, 1.5)

    # Calculate optimal DPUs (Glue) or nodes (EMR)
    base_dpus = max(2, int(workload_size_gb / 50))  # 1 DPU per 50GB
    recommended_dpus = int(base_dpus * multiplier)

    # Memory per executor
    if workload_size_gb < 10:
        executor_memory_gb = 4
    elif workload_size_gb < 100:
        executor_memory_gb = 8
    else:
        executor_memory_gb = 16

    # Calculate expected performance impact
    current_dpus = current_resources.get('dpu_count', 2)
    performance_change = ((recommended_dpus - current_dpus) / current_dpus * 100) if current_dpus > 0 else 0

    return {
        'recommended_dpus': recommended_dpus,
        'current_dpus': current_dpus,
        'dpu_change': recommended_dpus - current_dpus,
        'executor_memory_gb': executor_memory_gb,
        'executor_cores': 4,
        'performance_impact': f'{abs(performance_change):.0f}% {"faster" if performance_change > 0 else "slower"}',
        'cost_impact': f'{abs(performance_change):.0f}% {"higher" if performance_change > 0 else "lower"}',
        'reasoning': f'Based on {workload_size_gb}GB workload with {complexity} complexity'
    }


@tool
def suggest_partitioning_strategy(
    table_size_gb: float,
    query_patterns: List[str],
    cardinality_info: Dict[str, int]
) -> Dict[str, Any]:
    """
    Recommend partitioning strategy for tables.

    Args:
        table_size_gb: Size of table in GB
        query_patterns: Common query patterns (e.g., ['date_range', 'customer_filter'])
        cardinality_info: Cardinality of candidate partition columns

    Returns:
        Partitioning recommendations
    """
    recommendations = []

    # Determine if partitioning is needed
    if table_size_gb < 1:
        return {
            'partition_recommended': False,
            'reasoning': 'Table too small (<1GB) - partitioning overhead not worth it'
        }

    # Analyze query patterns
    date_queries = any('date' in p.lower() for p in query_patterns)
    customer_queries = any('customer' in p.lower() or 'user' in p.lower() for p in query_patterns)

    # Recommend partition columns
    if date_queries and 'date' in cardinality_info:
        date_cardinality = cardinality_info['date']
        if 30 < date_cardinality < 1000:  # Good partition column
            recommendations.append({
                'column': 'date',
                'strategy': 'daily' if date_cardinality > 365 else 'monthly',
                'reasoning': 'Common filter in queries with good cardinality',
                'expected_benefit': '60-80% faster queries with date filters'
            })

    if customer_queries and 'customer_id' in cardinality_info:
        customer_cardinality = cardinality_info['customer_id']
        if customer_cardinality < 1000:  # Too high cardinality = too many partitions
            recommendations.append({
                'column': 'customer_id',
                'strategy': 'hash_bucketing',
                'buckets': min(100, customer_cardinality // 10),
                'reasoning': 'Customer filtering but high cardinality - use bucketing',
                'expected_benefit': '40-50% faster customer queries'
            })

    # Default recommendation
    if not recommendations and table_size_gb > 10:
        recommendations.append({
            'column': 'ingestion_date',
            'strategy': 'daily',
            'reasoning': 'Default time-based partitioning for large tables',
            'expected_benefit': '50% faster for recent data queries'
        })

    return {
        'partition_recommended': True,
        'table_size_gb': table_size_gb,
        'recommendations': recommendations,
        'implementation': f"PARTITIONED BY ({recommendations[0]['column']})" if recommendations else None
    }


@tool
def recommend_file_format(
    use_case: str,
    read_write_ratio: float,
    compression_priority: str
) -> Dict[str, Any]:
    """
    Recommend optimal file format (Parquet, ORC, Avro, etc).

    Args:
        use_case: One of 'analytics', 'streaming', 'archival'
        read_write_ratio: Ratio of reads to writes (e.g., 10 = 10 reads per write)
        compression_priority: One of 'speed', 'size', 'balanced'

    Returns:
        File format recommendation
    """
    recommendations = {
        'analytics': {
            'format': 'parquet',
            'compression': 'snappy' if compression_priority == 'speed' else 'gzip',
            'reasoning': 'Columnar format ideal for analytical queries',
            'benefits': ['70% storage savings', 'Fast column-level reads', 'Schema evolution']
        },
        'streaming': {
            'format': 'avro',
            'compression': 'snappy',
            'reasoning': 'Row-based format better for streaming workloads',
            'benefits': ['Fast write performance', 'Schema evolution', 'Compact binary format']
        },
        'archival': {
            'format': 'parquet',
            'compression': 'gzip',
            'reasoning': 'Maximum compression for long-term storage',
            'benefits': ['80% storage savings', 'Excellent compression', 'Wide tool support']
        }
    }

    recommended = recommendations.get(use_case, recommendations['analytics'])

    # Adjust for read/write ratio
    if read_write_ratio > 20:  # Read-heavy
        recommended['optimization'] = 'Optimize for read performance - use aggressive compression'
    elif read_write_ratio < 2:  # Write-heavy
        recommended['optimization'] = 'Optimize for write performance - use fast compression'
    else:
        recommended['optimization'] = 'Balanced read/write workload'

    return {
        'use_case': use_case,
        'recommended_format': recommended['format'],
        'recommended_compression': recommended['compression'],
        'reasoning': recommended['reasoning'],
        'benefits': recommended['benefits'],
        'optimization_note': recommended.get('optimization', ''),
        'implementation': f"df.write.format('{recommended['format']}').option('compression', '{recommended['compression']}').save(path)"
    }
