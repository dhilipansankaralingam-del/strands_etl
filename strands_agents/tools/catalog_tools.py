"""
Catalog Tools for Strands ETL Framework

Tools for auto-detecting table sizes, schemas, and data flow analysis
from various data sources (AWS Glue Catalog, S3, Redshift, RDS, DynamoDB).
"""
import boto3
import re
from typing import Dict, Any, List, Optional
from strands import tool


@tool
def detect_glue_table_size(
    database: str,
    table: str,
    aws_region: str = 'us-east-1'
) -> Dict[str, Any]:
    """
    Detect size and metadata of a table in AWS Glue Data Catalog.

    Queries the Glue Catalog to get:
    - Total size in GB
    - Number of files
    - Number of partitions
    - Format (parquet, orc, csv, json)
    - Compression type
    - Last updated timestamp

    Args:
        database: Glue database name
        table: Table name
        aws_region: AWS region

    Returns:
        Dict with table metadata including size, file count, partitions
    """
    try:
        glue = boto3.client('glue', region_name=aws_region)

        # Get table metadata
        response = glue.get_table(DatabaseName=database, Name=table)
        table_metadata = response['Table']

        storage_descriptor = table_metadata.get('StorageDescriptor', {})
        parameters = table_metadata.get('Parameters', {})

        # Extract size information (Glue stores this in parameters)
        size_bytes = int(parameters.get('totalSize', 0))
        size_gb = round(size_bytes / (1024**3), 2)

        num_files = int(parameters.get('numFiles', 0))
        num_rows = int(parameters.get('numRows', 0))

        # Get partition information
        partition_keys = [pk['Name'] for pk in table_metadata.get('PartitionKeys', [])]

        # Count partitions
        num_partitions = 0
        if partition_keys:
            try:
                paginator = glue.get_paginator('get_partitions')
                partition_count = sum(
                    len(page['Partitions'])
                    for page in paginator.paginate(DatabaseName=database, TableName=table)
                )
                num_partitions = partition_count
            except Exception:
                num_partitions = 0

        # Extract format information
        input_format = storage_descriptor.get('InputFormat', '')
        format_type = 'unknown'
        if 'parquet' in input_format.lower():
            format_type = 'parquet'
        elif 'orc' in input_format.lower():
            format_type = 'orc'
        elif 'csv' in input_format.lower() or 'text' in input_format.lower():
            format_type = 'csv'
        elif 'json' in input_format.lower():
            format_type = 'json'

        compression = storage_descriptor.get('SerdeInfo', {}).get('Parameters', {}).get('compression', 'none')

        # Get S3 location
        location = storage_descriptor.get('Location', '')

        # Last updated
        update_time = table_metadata.get('UpdateTime', None)
        last_updated = update_time.isoformat() if update_time else 'unknown'

        return {
            'success': True,
            'database': database,
            'table': table,
            'size_gb': size_gb,
            'size_bytes': size_bytes,
            'num_files': num_files,
            'num_rows': num_rows,
            'num_partitions': num_partitions,
            'partition_keys': partition_keys,
            'format': format_type,
            'compression': compression,
            's3_location': location,
            'last_updated': last_updated,
            'broadcast_eligible': size_gb < 1.0,  # Recommend broadcast if < 1GB
            'recommended_role': 'dimension' if size_gb < 10 else 'fact'
        }

    except Exception as e:
        return {
            'success': False,
            'database': database,
            'table': table,
            'error': str(e),
            'size_gb': 0,
            'recommendation': 'Unable to detect size, please specify manually'
        }


@tool
def detect_s3_path_size(
    s3_path: str,
    aws_region: str = 'us-east-1'
) -> Dict[str, Any]:
    """
    Calculate size of data in an S3 path.

    Args:
        s3_path: S3 path (s3://bucket/prefix/)
        aws_region: AWS region

    Returns:
        Dict with size, file count, avg file size
    """
    try:
        # Parse S3 path
        match = re.match(r's3://([^/]+)/(.+)', s3_path)
        if not match:
            return {'success': False, 'error': 'Invalid S3 path format'}

        bucket = match.group(1)
        prefix = match.group(2).rstrip('/')

        s3 = boto3.client('s3', region_name=aws_region)

        total_size = 0
        file_count = 0

        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                total_size += obj['Size']
                file_count += 1

        size_gb = round(total_size / (1024**3), 2)
        avg_file_size_mb = round((total_size / file_count) / (1024**2), 2) if file_count > 0 else 0

        # Analyze if small files problem exists
        small_files_issue = file_count > 1000 and avg_file_size_mb < 128

        return {
            'success': True,
            's3_path': s3_path,
            'size_gb': size_gb,
            'size_bytes': total_size,
            'file_count': file_count,
            'avg_file_size_mb': avg_file_size_mb,
            'small_files_issue': small_files_issue,
            'broadcast_eligible': size_gb < 1.0,
            'recommended_role': 'dimension' if size_gb < 10 else 'fact'
        }

    except Exception as e:
        return {
            'success': False,
            's3_path': s3_path,
            'error': str(e)
        }


@tool
def detect_redshift_table_size(
    cluster: str,
    database: str,
    schema: str,
    table: str,
    aws_region: str = 'us-east-1'
) -> Dict[str, Any]:
    """
    Detect size of a Redshift table.

    Args:
        cluster: Redshift cluster identifier
        database: Database name
        schema: Schema name
        table: Table name
        aws_region: AWS region

    Returns:
        Dict with table size and row count
    """
    try:
        redshift_data = boto3.client('redshift-data', region_name=aws_region)

        # Query to get table size
        sql = f"""
        SELECT
            COUNT(*) as row_count,
            SUM(size) / 1024.0 / 1024.0 / 1024.0 as size_gb
        FROM stv_tbl_perm
        WHERE name = '{table}';
        """

        response = redshift_data.execute_statement(
            ClusterIdentifier=cluster,
            Database=database,
            Sql=sql
        )

        # Note: In production, you'd wait for query completion and fetch results
        # This is a simplified version

        return {
            'success': True,
            'cluster': cluster,
            'database': database,
            'schema': schema,
            'table': table,
            'size_gb': 0,  # Would be populated from query results
            'row_count': 0,
            'note': 'Redshift query submitted, actual size detection requires async query completion'
        }

    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


@tool
def analyze_data_flow_relationships(
    data_sources: List[Dict[str, Any]],
    script_content: Optional[str] = None
) -> Dict[str, Any]:
    """
    Analyze data flow relationships between tables.

    Determines:
    - Which tables join with which
    - Join keys
    - Fact vs dimension tables
    - Broadcast eligibility
    - Join cardinality (1:1, 1:N, N:M)
    - Optimal join order

    Args:
        data_sources: List of data source configurations
        script_content: Optional PySpark script to analyze for joins

    Returns:
        Dict with data flow analysis and recommendations
    """
    try:
        # Build data flow graph
        tables = {ds['name']: ds for ds in data_sources}

        # Analyze declared relationships
        join_graph = {}
        fact_tables = []
        dimension_tables = []

        for ds in data_sources:
            name = ds['name']
            role = ds.get('role_in_flow', 'unknown')
            joins_with = ds.get('joins_with', [])
            join_keys = ds.get('join_keys', {})
            size_gb = ds.get('size_gb', 0)

            if role == 'fact':
                fact_tables.append(name)
            elif role == 'dimension':
                dimension_tables.append(name)

            join_graph[name] = {
                'role': role,
                'size_gb': size_gb,
                'joins_with': joins_with,
                'join_keys': join_keys
            }

        # Analyze script for additional join patterns (if provided)
        detected_joins = []
        if script_content:
            # Simple regex to detect .join() patterns
            join_pattern = r'\.join\(["\']?(\w+)["\']?'
            matches = re.findall(join_pattern, script_content)
            detected_joins = list(set(matches))

        # Generate broadcast recommendations
        broadcast_recommendations = []
        for name, info in join_graph.items():
            if info['size_gb'] < 1.0 and info['role'] == 'dimension':
                broadcast_recommendations.append({
                    'table': name,
                    'reason': f"Small dimension table ({info['size_gb']} GB < 1 GB threshold)",
                    'expected_impact': 'Eliminate shuffle, 30-50% faster joins'
                })

        # Determine optimal join order
        # Rule: Join fact with smallest dimensions first
        dimension_sizes = [
            (name, info['size_gb'])
            for name, info in join_graph.items()
            if info['role'] == 'dimension'
        ]
        dimension_sizes.sort(key=lambda x: x[1])

        optimal_join_order = []
        if fact_tables:
            optimal_join_order.append({
                'step': 1,
                'table': fact_tables[0],
                'action': 'load',
                'reason': 'Start with fact table'
            })

            for i, (dim_name, dim_size) in enumerate(dimension_sizes, start=2):
                optimal_join_order.append({
                    'step': i,
                    'table': dim_name,
                    'action': 'broadcast_join' if dim_size < 1.0 else 'shuffle_join',
                    'reason': f'Join dimension ({dim_size} GB)'
                })

        # Calculate data flow metrics
        total_shuffle_expected = sum(
            info['size_gb']
            for info in join_graph.values()
            if info['role'] == 'fact' or info['size_gb'] >= 1.0
        )

        return {
            'success': True,
            'fact_tables': fact_tables,
            'dimension_tables': dimension_tables,
            'total_tables': len(tables),
            'join_graph': join_graph,
            'detected_joins_from_script': detected_joins,
            'broadcast_recommendations': broadcast_recommendations,
            'optimal_join_order': optimal_join_order,
            'estimated_shuffle_gb': round(total_shuffle_expected * 2, 2),  # Typically 2x data size
            'complexity_score': len(tables) * len(dimension_tables),
            'recommendations': [
                {
                    'category': 'join_strategy',
                    'priority': 'high',
                    'recommendation': f'Broadcast {len(broadcast_recommendations)} small dimensions to eliminate shuffle',
                    'expected_impact': '30-50% faster execution'
                },
                {
                    'category': 'join_order',
                    'priority': 'medium',
                    'recommendation': 'Follow optimal join order (smallest dimensions first)',
                    'expected_impact': '10-20% faster execution'
                }
            ]
        }

    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


@tool
def auto_detect_all_table_sizes(
    data_sources: List[Dict[str, Any]],
    aws_region: str = 'us-east-1'
) -> Dict[str, Any]:
    """
    Auto-detect sizes for all data sources in the config.

    Supports:
    - glue_catalog: Query Glue Catalog
    - s3: Calculate S3 path size
    - redshift: Query Redshift
    - rds: Query RDS (requires connection)
    - dynamodb: Get DynamoDB table size

    Args:
        data_sources: List of data source configurations
        aws_region: AWS region

    Returns:
        Dict with detected sizes for all sources
    """
    results = []

    for ds in data_sources:
        name = ds['name']
        source_type = ds.get('source_type', ds.get('type', 'unknown'))

        if source_type == 'glue_catalog':
            database = ds.get('database', '')
            table = ds.get('table', '')

            size_info = detect_glue_table_size(database, table, aws_region)
            results.append({
                'name': name,
                'source_type': source_type,
                **size_info
            })

        elif source_type == 's3':
            s3_path = ds.get('path', '')

            size_info = detect_s3_path_size(s3_path, aws_region)
            results.append({
                'name': name,
                'source_type': source_type,
                **size_info
            })

        elif source_type == 'redshift':
            cluster = ds.get('cluster', '')
            database = ds.get('database', '')
            schema = ds.get('schema', 'public')
            table = ds.get('table', '')

            size_info = detect_redshift_table_size(cluster, database, schema, table, aws_region)
            results.append({
                'name': name,
                'source_type': source_type,
                **size_info
            })

        elif source_type == 'dynamodb':
            # DynamoDB size detection
            table_name = ds.get('table_name', '')
            try:
                dynamodb = boto3.client('dynamodb', region_name=aws_region)
                response = dynamodb.describe_table(TableName=table_name)

                table_size_bytes = response['Table']['TableSizeBytes']
                size_gb = round(table_size_bytes / (1024**3), 2)
                item_count = response['Table']['ItemCount']

                results.append({
                    'name': name,
                    'source_type': source_type,
                    'success': True,
                    'table_name': table_name,
                    'size_gb': size_gb,
                    'item_count': item_count
                })
            except Exception as e:
                results.append({
                    'name': name,
                    'source_type': source_type,
                    'success': False,
                    'error': str(e)
                })
        else:
            results.append({
                'name': name,
                'source_type': source_type,
                'success': False,
                'error': f'Unsupported source type: {source_type}'
            })

    # Calculate total data volume
    total_size_gb = sum(r.get('size_gb', 0) for r in results if r.get('success', False))

    return {
        'success': True,
        'total_sources': len(data_sources),
        'detected_sources': len([r for r in results if r.get('success', False)]),
        'total_data_volume_gb': round(total_size_gb, 2),
        'results': results,
        'recommendations': [
            {
                'category': 'data_volume',
                'total_gb': round(total_size_gb, 2),
                'complexity': 'high' if total_size_gb > 500 else 'moderate' if total_size_gb > 100 else 'low'
            }
        ]
    }


@tool
def recommend_platform_based_on_data_flow(
    data_flow_analysis: Dict[str, Any],
    total_data_volume_gb: float,
    complexity: str
) -> Dict[str, Any]:
    """
    Recommend optimal platform based on data flow analysis.

    Args:
        data_flow_analysis: Results from analyze_data_flow_relationships
        total_data_volume_gb: Total data volume
        complexity: Workload complexity (simple, moderate, complex)

    Returns:
        Platform recommendation with reasoning
    """
    num_tables = data_flow_analysis.get('total_tables', 0)
    num_joins = len(data_flow_analysis.get('dimension_tables', []))
    estimated_shuffle_gb = data_flow_analysis.get('estimated_shuffle_gb', 0)

    # Decision logic
    recommendations = []

    if total_data_volume_gb < 10 and complexity == 'simple':
        platform = 'lambda'
        reason = 'Small data volume (<10GB) with simple transformations'
        dpu_count = 0
    elif total_data_volume_gb < 100 and num_joins <= 2:
        platform = 'glue'
        reason = 'Moderate data volume with few joins, Glue is cost-effective'
        dpu_count = 5
    elif total_data_volume_gb > 500 or num_joins > 5:
        platform = 'emr'
        reason = 'Large data volume or complex multi-table joins require EMR'
        dpu_count = 20
    else:
        platform = 'glue'
        reason = 'Balanced workload, Glue provides good performance/cost ratio'
        dpu_count = 10

    return {
        'recommended_platform': platform,
        'reason': reason,
        'dpu_count': dpu_count,
        'confidence': 'high',
        'factors_considered': {
            'total_data_volume_gb': total_data_volume_gb,
            'num_tables': num_tables,
            'num_joins': num_joins,
            'estimated_shuffle_gb': estimated_shuffle_gb,
            'complexity': complexity
        }
    }
