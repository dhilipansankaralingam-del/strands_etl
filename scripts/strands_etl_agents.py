#!/usr/bin/env python3
"""
Strands SDK ETL Optimizer - Complete End-to-End Agent Pipeline

Agents:
  1. SizingAgent - Scans S3/Glue to determine data sizes
  2. CodeAnalysisAgent - Analyzes PySpark code line-by-line
  3. ComplianceAgent - Checks security and best practices
  4. LearningAgent - Learns from historical runs
  5. RecommendationAgent - Aggregates and prioritizes all recommendations

Usage:
    export AWS_REGION=us-west-2
    export AWS_PROFILE=your-profile  # or use IAM role

    # Single config
    python scripts/strands_etl_agents.py --config demo_configs/job.json

    # Multiple configs
    python scripts/strands_etl_agents.py --source demo_configs/ --dest reports/
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field, asdict

# Set region before importing boto3
os.environ.setdefault('AWS_DEFAULT_REGION', 'us-west-2')
os.environ.setdefault('AWS_REGION', 'us-west-2')

# Strands SDK
from strands import Agent
from strands.tools import tool

# AWS clients
try:
    import boto3
    HAS_AWS = True
except ImportError:
    HAS_AWS = False
    print("[WARN] boto3 not installed - AWS features disabled")


# =============================================================================
# TOKEN TRACKER
# =============================================================================

class TokenTracker:
    """Track tokens and timing for each agent."""

    def __init__(self):
        self.agents = {}
        self.total_input = 0
        self.total_output = 0
        self.start = time.time()

    def log_agent(self, name: str, input_tokens: int, output_tokens: int, duration: float):
        self.agents[name] = {
            'input_tokens': input_tokens,
            'output_tokens': output_tokens,
            'duration_sec': round(duration, 2)
        }
        self.total_input += input_tokens
        self.total_output += output_tokens
        print(f"    Tokens: {input_tokens:,} in / {output_tokens:,} out | Time: {duration:.1f}s")

    def summary(self) -> Dict:
        return {
            'agents': self.agents,
            'total_input_tokens': self.total_input,
            'total_output_tokens': self.total_output,
            'total_duration_sec': round(time.time() - self.start, 2),
            'estimated_cost_usd': round((self.total_input * 0.003 + self.total_output * 0.015) / 1000, 4)
        }


# =============================================================================
# TOOLS FOR AGENTS
# =============================================================================

@tool
def read_file(file_path: str) -> str:
    """Read contents of a local file.

    Args:
        file_path: Path to the file to read

    Returns:
        File contents or error message
    """
    print(f"      [TOOL] read_file: {file_path}")
    try:
        content = Path(file_path).read_text()
        print(f"      [TOOL] Read {len(content):,} chars, {len(content.splitlines())} lines")
        return content
    except FileNotFoundError:
        print(f"      [TOOL] ERROR: File not found")
        return f"ERROR: File not found: {file_path}"
    except Exception as e:
        print(f"      [TOOL] ERROR: {e}")
        return f"ERROR: {e}"


@tool
def scan_s3_location(s3_uri: str, max_files: int = 500) -> Dict[str, Any]:
    """Scan S3 location to get file count and total size.

    Args:
        s3_uri: S3 URI like s3://bucket/prefix/
        max_files: Maximum files to scan (default 500)

    Returns:
        Dict with file_count, total_size_gb, file_types
    """
    print(f"      [TOOL] scan_s3_location: {s3_uri}")

    if not HAS_AWS:
        print(f"      [TOOL] ERROR: boto3 not available")
        return {'error': 'boto3 not installed', 's3_uri': s3_uri}

    try:
        path = s3_uri.replace('s3://', '').replace('s3a://', '')
        bucket, *prefix_parts = path.split('/')
        prefix = '/'.join(prefix_parts)

        s3 = boto3.client('s3', region_name='us-west-2')
        paginator = s3.get_paginator('list_objects_v2')

        total_size = 0
        file_count = 0
        extensions = {}

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={'MaxItems': max_files}):
            for obj in page.get('Contents', []):
                file_count += 1
                total_size += obj.get('Size', 0)
                ext = Path(obj['Key']).suffix.lower() or '.none'
                extensions[ext] = extensions.get(ext, 0) + 1

        size_gb = round(total_size / (1024**3), 3)
        print(f"      [TOOL] Found {file_count} files, {size_gb} GB")

        return {
            's3_uri': s3_uri,
            'file_count': file_count,
            'total_size_bytes': total_size,
            'total_size_gb': size_gb,
            'file_types': extensions,
            'scanned_limit': max_files
        }

    except Exception as e:
        print(f"      [TOOL] ERROR: {e}")
        return {'error': str(e), 's3_uri': s3_uri}


@tool
def get_glue_table_info(database: str, table: str) -> Dict[str, Any]:
    """Get Glue Data Catalog table metadata.

    Args:
        database: Glue database name
        table: Table name

    Returns:
        Dict with table location, columns, partitions, size estimate
    """
    print(f"      [TOOL] get_glue_table_info: {database}.{table}")

    if not HAS_AWS:
        return {'error': 'boto3 not installed'}

    try:
        glue = boto3.client('glue', region_name='us-west-2')
        resp = glue.get_table(DatabaseName=database, Name=table)
        tbl = resp['Table']

        location = tbl.get('StorageDescriptor', {}).get('Location', '')
        columns = [c['Name'] for c in tbl.get('StorageDescriptor', {}).get('Columns', [])]
        partitions = [p['Name'] for p in tbl.get('PartitionKeys', [])]

        print(f"      [TOOL] Location: {location}, Columns: {len(columns)}, Partitions: {len(partitions)}")

        return {
            'database': database,
            'table': table,
            'location': location,
            'columns': columns,
            'column_count': len(columns),
            'partition_keys': partitions,
            'format': tbl.get('StorageDescriptor', {}).get('InputFormat', ''),
            'parameters': tbl.get('Parameters', {})
        }

    except glue.exceptions.EntityNotFoundException:
        print(f"      [TOOL] ERROR: Table not found")
        return {'error': f'Table not found: {database}.{table}'}
    except Exception as e:
        print(f"      [TOOL] ERROR: {e}")
        return {'error': str(e)}


@tool
def get_glue_job_runs(job_name: str, max_runs: int = 10) -> Dict[str, Any]:
    """Get recent Glue job runs for learning.

    Args:
        job_name: Name of the Glue job
        max_runs: Maximum runs to retrieve

    Returns:
        Dict with job config and recent run metrics
    """
    print(f"      [TOOL] get_glue_job_runs: {job_name}")

    if not HAS_AWS:
        return {'error': 'boto3 not installed'}

    try:
        glue = boto3.client('glue', region_name='us-west-2')

        # Get job config
        job = glue.get_job(JobName=job_name)['Job']

        # Get runs
        runs_resp = glue.get_job_runs(JobName=job_name, MaxResults=max_runs)
        runs = []

        for r in runs_resp.get('JobRuns', []):
            runs.append({
                'run_id': r['Id'],
                'status': r['JobRunState'],
                'started': r.get('StartedOn').isoformat() if r.get('StartedOn') else None,
                'duration_sec': r.get('ExecutionTime', 0),
                'dpu_seconds': r.get('DPUSeconds', 0),
                'error': r.get('ErrorMessage', '')[:200] if r.get('ErrorMessage') else None
            })

        print(f"      [TOOL] Found {len(runs)} runs, workers: {job.get('NumberOfWorkers', 0)}")

        return {
            'job_name': job_name,
            'worker_type': job.get('WorkerType', 'Standard'),
            'num_workers': job.get('NumberOfWorkers', 0),
            'glue_version': job.get('GlueVersion', ''),
            'timeout_min': job.get('Timeout', 0),
            'runs': runs,
            'avg_duration_sec': sum(r['duration_sec'] for r in runs) / len(runs) if runs else 0
        }

    except glue.exceptions.EntityNotFoundException:
        print(f"      [TOOL] ERROR: Job not found")
        return {'error': f'Job not found: {job_name}'}
    except Exception as e:
        print(f"      [TOOL] ERROR: {e}")
        return {'error': str(e)}


@tool
def start_glue_job(job_name: str, workers: int = None, worker_type: str = None, dry_run: bool = True) -> Dict[str, Any]:
    """Start a Glue job execution (or simulate in dry_run mode).

    Args:
        job_name: Name of the Glue job to run
        workers: Override number of workers (optional)
        worker_type: Override worker type G.1X/G.2X (optional)
        dry_run: If True, simulate execution without actually running

    Returns:
        Dict with run_id, status, and execution details
    """
    print(f"      [TOOL] start_glue_job: {job_name} (dry_run={dry_run})")

    if not HAS_AWS:
        return {'error': 'boto3 not installed'}

    if dry_run:
        print(f"      [TOOL] DRY RUN - simulating execution")
        return {
            'job_name': job_name,
            'run_id': f'jr_simulated_{int(time.time())}',
            'status': 'SIMULATED',
            'dry_run': True,
            'workers': workers or 10,
            'worker_type': worker_type or 'G.1X',
            'estimated_duration_sec': 300,
            'estimated_cost_usd': round((workers or 10) * 0.44 * 0.5, 2)
        }

    try:
        glue = boto3.client('glue', region_name='us-west-2')

        args = {}
        if workers:
            args['NumberOfWorkers'] = workers
        if worker_type:
            args['WorkerType'] = worker_type

        response = glue.start_job_run(JobName=job_name, Arguments=args)
        run_id = response['JobRunId']

        print(f"      [TOOL] Started run: {run_id}")

        return {
            'job_name': job_name,
            'run_id': run_id,
            'status': 'STARTING',
            'dry_run': False
        }

    except Exception as e:
        print(f"      [TOOL] ERROR: {e}")
        return {'error': str(e), 'job_name': job_name}


@tool
def get_job_run_status(job_name: str, run_id: str) -> Dict[str, Any]:
    """Get current status and metrics of a Glue job run.

    Args:
        job_name: Name of the Glue job
        run_id: Job run ID to check

    Returns:
        Dict with status, duration, metrics, and any errors
    """
    print(f"      [TOOL] get_job_run_status: {job_name}/{run_id}")

    if not HAS_AWS:
        return {'error': 'boto3 not installed'}

    try:
        glue = boto3.client('glue', region_name='us-west-2')
        run = glue.get_job_run(JobName=job_name, RunId=run_id)['JobRun']

        result = {
            'job_name': job_name,
            'run_id': run_id,
            'status': run['JobRunState'],
            'started': run.get('StartedOn').isoformat() if run.get('StartedOn') else None,
            'completed': run.get('CompletedOn').isoformat() if run.get('CompletedOn') else None,
            'duration_sec': run.get('ExecutionTime', 0),
            'dpu_seconds': run.get('DPUSeconds', 0),
            'workers': run.get('NumberOfWorkers', 0),
            'worker_type': run.get('WorkerType', ''),
            'error': run.get('ErrorMessage', '')[:500] if run.get('ErrorMessage') else None
        }

        # Calculate cost
        dpu_hours = result['dpu_seconds'] / 3600
        cost_per_dpu = {'G.1X': 0.44, 'G.2X': 0.88, 'G.4X': 1.76}.get(result['worker_type'], 0.44)
        result['cost_usd'] = round(dpu_hours * cost_per_dpu, 2)

        print(f"      [TOOL] Status: {result['status']}, Duration: {result['duration_sec']}s")

        return result

    except Exception as e:
        print(f"      [TOOL] ERROR: {e}")
        return {'error': str(e)}


@tool
def run_athena_query(sql: str, database: str = 'default') -> Dict[str, Any]:
    """Run a SQL query via Athena for data quality validation.

    Args:
        sql: SQL query to execute
        database: Athena database (default: 'default')

    Returns:
        Dict with query results or error
    """
    print(f"      [TOOL] run_athena_query: {sql[:60]}...")

    if not HAS_AWS:
        return {'error': 'boto3 not installed'}

    try:
        athena = boto3.client('athena', region_name='us-west-2')

        # Need output location - check environment
        output_loc = os.environ.get('ATHENA_OUTPUT_LOCATION', 's3://aws-athena-query-results-us-west-2/')

        response = athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_loc}
        )

        query_id = response['QueryExecutionId']
        print(f"      [TOOL] Query started: {query_id}")

        # Poll for completion (max 60 seconds)
        for _ in range(30):
            status = athena.get_query_execution(QueryExecutionId=query_id)
            state = status['QueryExecution']['Status']['State']

            if state == 'SUCCEEDED':
                # Get results
                results = athena.get_query_results(QueryExecutionId=query_id)
                rows = []
                columns = []

                for i, row in enumerate(results['ResultSet']['Rows']):
                    values = [col.get('VarCharValue', '') for col in row['Data']]
                    if i == 0:
                        columns = values
                    else:
                        rows.append(dict(zip(columns, values)))

                print(f"      [TOOL] Query returned {len(rows)} rows")
                return {
                    'success': True,
                    'query_id': query_id,
                    'columns': columns,
                    'rows': rows[:100],  # Limit to 100 rows
                    'row_count': len(rows)
                }

            elif state in ['FAILED', 'CANCELLED']:
                error = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                print(f"      [TOOL] Query failed: {error}")
                return {'success': False, 'error': error, 'query_id': query_id}

            time.sleep(2)

        return {'success': False, 'error': 'Query timeout', 'query_id': query_id}

    except Exception as e:
        print(f"      [TOOL] ERROR: {e}")
        return {'error': str(e)}


@tool
def validate_data_quality_rule(database: str, table: str, rule_type: str, column: str = None, threshold: float = None) -> Dict[str, Any]:
    """Validate a data quality rule against a table.

    Args:
        database: Database name
        table: Table name
        rule_type: Type of rule (not_null, unique, positive, row_count, completeness)
        column: Column to validate (required for column-level rules)
        threshold: Threshold value (for completeness, row_count rules)

    Returns:
        Dict with validation result, records scanned, outliers found
    """
    print(f"      [TOOL] validate_data_quality_rule: {database}.{table} - {rule_type}")

    # Build validation SQL based on rule type
    if rule_type == 'not_null':
        sql = f"SELECT COUNT(*) as total, SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as nulls FROM {database}.{table}"
    elif rule_type == 'unique':
        sql = f"SELECT COUNT(*) as total, COUNT(*) - COUNT(DISTINCT {column}) as duplicates FROM {database}.{table}"
    elif rule_type == 'positive':
        sql = f"SELECT COUNT(*) as total, SUM(CASE WHEN {column} <= 0 THEN 1 ELSE 0 END) as negatives FROM {database}.{table}"
    elif rule_type == 'row_count':
        sql = f"SELECT COUNT(*) as total FROM {database}.{table}"
    elif rule_type == 'completeness':
        sql = f"SELECT COUNT(*) as total, SUM(CASE WHEN {column} IS NULL OR TRIM({column}) = '' THEN 1 ELSE 0 END) as incomplete FROM {database}.{table}"
    else:
        return {'error': f'Unknown rule type: {rule_type}'}

    # Execute via Athena
    result = run_athena_query(sql, database)

    if not result.get('success'):
        return {
            'rule_type': rule_type,
            'table': f'{database}.{table}',
            'status': 'ERROR',
            'error': result.get('error')
        }

    # Parse results
    if result['rows']:
        row = result['rows'][0]
        total = int(row.get('total', 0))

        if rule_type == 'row_count':
            passed = total >= (threshold or 0)
            return {
                'rule_type': rule_type,
                'table': f'{database}.{table}',
                'status': 'PASS' if passed else 'FAIL',
                'records_scanned': total,
                'threshold': threshold,
                'actual_value': total
            }

        outliers = int(row.get('nulls', row.get('duplicates', row.get('negatives', row.get('incomplete', 0)))))
        pct = (total - outliers) / total if total > 0 else 0

        if rule_type == 'completeness':
            passed = pct >= (threshold or 0.95)
        else:
            passed = outliers == 0

        return {
            'rule_type': rule_type,
            'column': column,
            'table': f'{database}.{table}',
            'status': 'PASS' if passed else 'FAIL',
            'records_scanned': total,
            'outliers_found': outliers,
            'pass_rate': round(pct, 4),
            'threshold': threshold
        }

    return {'rule_type': rule_type, 'status': 'NO_DATA'}


@tool
def store_execution_history(job_name: str, execution_data: Dict) -> Dict[str, Any]:
    """Store execution data for learning agent to use later.

    Args:
        job_name: Name of the job
        execution_data: Dict with run metrics (duration, cost, workers, etc.)

    Returns:
        Confirmation of storage
    """
    print(f"      [TOOL] store_execution_history: {job_name}")

    history_dir = Path('data/execution_history')
    history_dir.mkdir(parents=True, exist_ok=True)

    history_file = history_dir / f"{job_name}.jsonl"

    record = {
        'timestamp': datetime.utcnow().isoformat(),
        'job_name': job_name,
        **execution_data
    }

    with open(history_file, 'a') as f:
        f.write(json.dumps(record) + '\n')

    # Count total records
    with open(history_file, 'r') as f:
        total = sum(1 for _ in f)

    print(f"      [TOOL] Stored record #{total} for {job_name}")

    return {
        'stored': True,
        'job_name': job_name,
        'history_file': str(history_file),
        'total_records': total
    }


@tool
def load_execution_history(job_name: str, limit: int = 20) -> Dict[str, Any]:
    """Load historical execution data for learning.

    Args:
        job_name: Name of the job
        limit: Max records to return (default 20)

    Returns:
        Dict with historical runs and computed statistics
    """
    print(f"      [TOOL] load_execution_history: {job_name} (limit={limit})")

    history_file = Path(f'data/execution_history/{job_name}.jsonl')

    if not history_file.exists():
        print(f"      [TOOL] No history found")
        return {'job_name': job_name, 'records': [], 'count': 0}

    records = []
    with open(history_file, 'r') as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    # Take last N records
    records = records[-limit:]

    # Compute statistics
    if records:
        durations = [r.get('duration_sec', 0) for r in records if r.get('duration_sec')]
        costs = [r.get('cost_usd', 0) for r in records if r.get('cost_usd')]

        stats = {
            'avg_duration_sec': sum(durations) / len(durations) if durations else 0,
            'avg_cost_usd': sum(costs) / len(costs) if costs else 0,
            'min_duration_sec': min(durations) if durations else 0,
            'max_duration_sec': max(durations) if durations else 0,
            'success_rate': sum(1 for r in records if r.get('status') == 'SUCCEEDED') / len(records)
        }
    else:
        stats = {}

    print(f"      [TOOL] Loaded {len(records)} records")

    return {
        'job_name': job_name,
        'records': records,
        'count': len(records),
        'statistics': stats
    }


@tool
def analyze_table_schema(database: str, table: str) -> Dict[str, Any]:
    """Analyze table schema: column count, types, width estimate, and partition layout.

    Args:
        database: Glue database name
        table: Table name

    Returns:
        Dict with column breakdown, estimated row width bytes, wide-table flags
    """
    print(f"      [TOOL] analyze_table_schema: {database}.{table}")

    if not HAS_AWS:
        return {'error': 'boto3 not installed'}

    try:
        glue = boto3.client('glue', region_name='us-west-2')
        resp = glue.get_table(DatabaseName=database, Name=table)
        tbl = resp['Table']

        sd = tbl.get('StorageDescriptor', {})
        columns = sd.get('Columns', [])
        partitions = tbl.get('PartitionKeys', [])

        type_map = {}
        estimated_row_bytes = 0
        type_widths = {
            'string': 50, 'varchar': 50, 'char': 20,
            'int': 4, 'bigint': 8, 'long': 8, 'double': 8, 'float': 4,
            'boolean': 1, 'date': 4, 'timestamp': 8,
            'decimal': 16, 'array': 100, 'map': 200, 'struct': 150, 'binary': 100
        }

        complex_cols = []
        string_cols = []
        numeric_cols = []

        for col in columns:
            col_type = col.get('Type', 'string').lower().split('<')[0]
            width = type_widths.get(col_type, 50)
            estimated_row_bytes += width
            type_map[col_type] = type_map.get(col_type, 0) + 1

            if col_type in ('array', 'map', 'struct'):
                complex_cols.append(col['Name'])
            elif col_type in ('string', 'varchar', 'char'):
                string_cols.append(col['Name'])
            elif col_type in ('int', 'bigint', 'double', 'float', 'decimal', 'long'):
                numeric_cols.append(col['Name'])

        is_wide = len(columns) > 50
        has_complex = len(complex_cols) > 0

        print(f"      [TOOL] {len(columns)} columns, ~{estimated_row_bytes}B/row, wide={is_wide}")

        return {
            'database': database,
            'table': table,
            'total_columns': len(columns),
            'partition_keys': [p['Name'] for p in partitions],
            'partition_count': len(partitions),
            'estimated_row_bytes': estimated_row_bytes,
            'estimated_row_kb': round(estimated_row_bytes / 1024, 3),
            'type_breakdown': type_map,
            'string_columns': string_cols[:10],
            'numeric_columns': numeric_cols[:10],
            'complex_columns': complex_cols,
            'is_wide_table': is_wide,
            'has_complex_types': has_complex,
            'warnings': (
                (['Wide table (>50 cols): consider columnar pruning'] if is_wide else []) +
                (['Complex types (array/map/struct): UDFs may be slow'] if has_complex else [])
            )
        }

    except Exception as e:
        print(f"      [TOOL] ERROR: {e}")
        return {'error': str(e), 'database': database, 'table': table}


@tool
def detect_data_skew(database: str, table: str, partition_column: str, sample_limit: int = 20) -> Dict[str, Any]:
    """Detect data skew by analyzing partition value distribution via Athena.

    Args:
        database: Database name
        table: Table name
        partition_column: Column to check skew on (partition key or join key)
        sample_limit: Top N partition values to sample

    Returns:
        Dict with skew score, hot partitions, and recommended salting strategy
    """
    print(f"      [TOOL] detect_data_skew: {database}.{table} on {partition_column}")

    sql = f"""
        SELECT {partition_column},
               COUNT(*) as record_count,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as pct_of_total
        FROM {database}.{table}
        GROUP BY {partition_column}
        ORDER BY record_count DESC
        LIMIT {sample_limit}
    """

    result = run_athena_query(sql, database)

    if not result.get('success'):
        return {'error': result.get('error'), 'database': database, 'table': table}

    rows = result.get('rows', [])
    if not rows:
        return {'skew_detected': False, 'message': 'No data returned'}

    counts = [int(r.get('record_count', 0)) for r in rows]
    pcts = [float(r.get('pct_of_total', 0)) for r in rows]

    max_pct = max(pcts) if pcts else 0
    min_count = min(counts) if counts else 0
    max_count = max(counts) if counts else 0
    skew_ratio = max_count / min_count if min_count > 0 else 999

    skew_detected = max_pct > 30 or skew_ratio > 10

    hot_partitions = [
        {'value': r.get(partition_column, ''), 'count': int(r.get('record_count', 0)), 'pct': float(r.get('pct_of_total', 0))}
        for r in rows[:5]
    ]

    recommendations = []
    if skew_detected:
        recommendations.append(f"Salt {partition_column} with random suffix (e.g. CONCAT({partition_column}, '_', CAST(FLOOR(RAND()*10) AS STRING)))")
        recommendations.append("Use skewHint: df.hint('skew', '{partition_column}')")
        recommendations.append("Enable AQE: spark.sql.adaptive.enabled=true, spark.sql.adaptive.skewJoin.enabled=true")
        if max_pct > 50:
            recommendations.append(f"Consider pre-aggregating hot partition before join")

    print(f"      [TOOL] Skew detected={skew_detected}, max_pct={max_pct}%, ratio={skew_ratio:.1f}x")

    return {
        'database': database,
        'table': table,
        'partition_column': partition_column,
        'skew_detected': skew_detected,
        'skew_ratio': round(skew_ratio, 2),
        'max_partition_pct': max_pct,
        'hot_partitions': hot_partitions,
        'total_partitions_sampled': len(rows),
        'recommendations': recommendations
    }


@tool
def analyze_s3_object_timestamps(s3_uri: str, lookback_hours: int = 24) -> Dict[str, Any]:
    """Analyze S3 object LastModified timestamps to detect incremental data patterns.

    Args:
        s3_uri: S3 URI to scan
        lookback_hours: Hours to look back for recent objects

    Returns:
        Dict with incremental patterns, new file counts, and CDC recommendations
    """
    print(f"      [TOOL] analyze_s3_object_timestamps: {s3_uri} (last {lookback_hours}h)")

    if not HAS_AWS:
        return {'error': 'boto3 not installed'}

    try:
        from datetime import timezone
        path = s3_uri.replace('s3://', '').replace('s3a://', '')
        bucket, *prefix_parts = path.split('/')
        prefix = '/'.join(prefix_parts)

        s3 = boto3.client('s3', region_name='us-west-2')
        paginator = s3.get_paginator('list_objects_v2')

        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        cutoff = now.replace(hour=now.hour - lookback_hours % 24,
                             day=now.day - lookback_hours // 24) if lookback_hours < 24 * 30 else None

        total_files = 0
        recent_files = 0
        recent_size = 0
        total_size = 0
        oldest = None
        newest = None
        hourly_buckets = {}

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={'MaxItems': 2000}):
            for obj in page.get('Contents', []):
                total_files += 1
                total_size += obj.get('Size', 0)
                lm = obj.get('LastModified')

                if lm:
                    if oldest is None or lm < oldest:
                        oldest = lm
                    if newest is None or lm > newest:
                        newest = lm

                    hour_key = lm.strftime('%Y-%m-%d %H:00')
                    hourly_buckets[hour_key] = hourly_buckets.get(hour_key, 0) + 1

                    if cutoff and lm >= cutoff:
                        recent_files += 1
                        recent_size += obj.get('Size', 0)

        recent_pct = round(recent_files / total_files * 100, 1) if total_files else 0
        recent_gb = round(recent_size / (1024**3), 3)
        total_gb = round(total_size / (1024**3), 3)

        # Detect pattern
        if recent_pct < 5:
            pattern = 'FULL_LOAD'
            cdc_suitable = False
        elif recent_pct < 30:
            pattern = 'INCREMENTAL'
            cdc_suitable = True
        else:
            pattern = 'APPEND_HEAVY'
            cdc_suitable = True

        recommendations = []
        if cdc_suitable:
            recommendations.append(f"Use incremental load: filter WHERE last_modified >= '{lookback_hours}h ago'")
            recommendations.append("Add --job-bookmark-option=job-bookmark-enable in Glue for auto-incrementals")
            recommendations.append(f"Only {recent_gb:.2f} GB changed in last {lookback_hours}h vs {total_gb:.2f} GB total - process delta only")
        else:
            recommendations.append("Data appears static/full-load - consider scheduling less frequently")

        print(f"      [TOOL] Pattern={pattern}, recent={recent_pct}%, recent={recent_gb}GB/{total_gb}GB")

        return {
            's3_uri': s3_uri,
            'total_files': total_files,
            'total_size_gb': total_gb,
            'recent_files_last_Nh': recent_files,
            'recent_size_gb': recent_gb,
            'recent_pct': recent_pct,
            'data_pattern': pattern,
            'cdc_suitable': cdc_suitable,
            'oldest_object': oldest.isoformat() if oldest else None,
            'newest_object': newest.isoformat() if newest else None,
            'top_active_hours': sorted(hourly_buckets.items(), key=lambda x: x[1], reverse=True)[:5],
            'recommendations': recommendations
        }

    except Exception as e:
        print(f"      [TOOL] ERROR: {e}")
        return {'error': str(e), 's3_uri': s3_uri}


@tool
def analyze_executor_sizing(total_data_gb: float, num_columns: int, worker_type: str = 'G.1X',
                             num_workers: int = 10) -> Dict[str, Any]:
    """Calculate optimal executor sizing based on data volume and table width.

    Args:
        total_data_gb: Total input data in GB
        num_columns: Number of columns in the widest table
        worker_type: Glue worker type (G.1X, G.2X, G.4X, G.8X)
        num_workers: Current number of workers

    Returns:
        Dict with per-executor data, shuffle estimates, and recommended executor count
    """
    print(f"      [TOOL] analyze_executor_sizing: {total_data_gb}GB, {num_columns} cols, {worker_type}x{num_workers}")

    worker_specs = {
        'G.1X':  {'vcpu': 4,  'mem_gb': 16,  'cost_dpu': 0.44},
        'G.2X':  {'vcpu': 8,  'mem_gb': 32,  'cost_dpu': 0.88},
        'G.4X':  {'vcpu': 16, 'mem_gb': 64,  'cost_dpu': 1.76},
        'G.8X':  {'vcpu': 32, 'mem_gb': 128, 'cost_dpu': 3.52},
        'Standard': {'vcpu': 4, 'mem_gb': 16, 'cost_dpu': 0.44},
    }
    spec = worker_specs.get(worker_type, worker_specs['G.1X'])

    executor_mem = spec['mem_gb']
    driver_overhead = 0.25
    usable_mem_gb = executor_mem * (1 - driver_overhead)

    data_per_executor_gb = total_data_gb / max(num_workers - 1, 1)
    width_multiplier = 1 + (num_columns / 100)
    effective_data_per_exec = data_per_executor_gb * width_multiplier

    shuffle_estimate_gb = total_data_gb * 2.5
    shuffle_per_executor = shuffle_estimate_gb / max(num_workers - 1, 1)

    mem_ok = effective_data_per_exec < usable_mem_gb * 0.6
    optimal_workers = max(2, int((total_data_gb * width_multiplier) / (usable_mem_gb * 0.5)) + 1)

    issues = []
    if not mem_ok:
        issues.append(f"Data per executor ({effective_data_per_exec:.1f}GB) exceeds safe threshold ({usable_mem_gb*0.6:.1f}GB)")
    if shuffle_per_executor > usable_mem_gb * 0.4:
        issues.append(f"Shuffle ({shuffle_per_executor:.1f}GB/executor) may cause spill to disk")
    if num_columns > 100:
        issues.append(f"Wide table ({num_columns} cols): consider SELECT only needed columns early")

    spark_configs = {
        'spark.executor.memory': f'{int(executor_mem * 0.75)}g',
        'spark.executor.memoryOverhead': f'{int(executor_mem * 0.25 * 1024)}m',
        'spark.sql.shuffle.partitions': str(max(200, optimal_workers * 4)),
        'spark.default.parallelism': str(optimal_workers * 2),
    }

    print(f"      [TOOL] Per-executor: {data_per_executor_gb:.2f}GB raw, optimal_workers={optimal_workers}")

    return {
        'worker_type': worker_type,
        'worker_mem_gb': executor_mem,
        'usable_mem_gb': round(usable_mem_gb, 2),
        'data_per_executor_gb': round(data_per_executor_gb, 3),
        'effective_data_per_executor_gb': round(effective_data_per_exec, 3),
        'shuffle_estimate_gb': round(shuffle_estimate_gb, 2),
        'shuffle_per_executor_gb': round(shuffle_per_executor, 3),
        'current_workers': num_workers,
        'optimal_workers': optimal_workers,
        'mem_sufficient': mem_ok,
        'issues': issues,
        'recommended_spark_configs': spark_configs,
        'upgrade_needed': optimal_workers > num_workers or not mem_ok
    }


@tool
def get_spark_config_recommendations(data_size_gb: float, num_tables: int, has_skew: bool,
                                      has_wide_tables: bool, has_complex_types: bool,
                                      worker_type: str = 'G.1X', num_workers: int = 10) -> Dict[str, Any]:
    """Generate dynamic Spark configuration recommendations based on workload characteristics.

    Args:
        data_size_gb: Total data size in GB
        num_tables: Number of tables being joined
        has_skew: Whether data skew was detected
        has_wide_tables: Whether any table has >50 columns
        has_complex_types: Whether any table has array/map/struct columns
        worker_type: Glue worker type
        num_workers: Number of workers

    Returns:
        Dict with categorized Spark configs to apply dynamically
    """
    print(f"      [TOOL] get_spark_config_recommendations: {data_size_gb}GB, {num_tables} tables, skew={has_skew}")

    worker_mem = {'G.1X': 16, 'G.2X': 32, 'G.4X': 64, 'G.8X': 128, 'Standard': 16}.get(worker_type, 16)

    shuffle_partitions = max(200, num_workers * max(4, int(data_size_gb / 2)))
    broadcast_threshold = '100MB' if data_size_gb > 100 else '256MB'

    configs = {
        'adaptive_query_execution': {
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.minPartitionNum': str(num_workers),
            'spark.sql.adaptive.advisoryPartitionSizeInBytes': '128MB',
            'spark.sql.adaptive.skewJoin.enabled': 'true' if has_skew else 'false',
            'spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes': '256MB',
            'spark.sql.adaptive.skewJoin.skewedPartitionFactor': '5',
        },
        'memory_management': {
            'spark.executor.memory': f'{int(worker_mem * 0.75)}g',
            'spark.executor.memoryOverhead': f'{int(worker_mem * 0.25 * 1024)}m',
            'spark.memory.fraction': '0.8',
            'spark.memory.storageFraction': '0.3',
            'spark.sql.execution.arrow.pyspark.enabled': 'true',
        },
        'shuffle_tuning': {
            'spark.sql.shuffle.partitions': str(shuffle_partitions),
            'spark.default.parallelism': str(num_workers * 2),
            'spark.shuffle.compress': 'true',
            'spark.shuffle.spill.compress': 'true',
            'spark.io.compression.codec': 'lz4',
        },
        'join_optimization': {
            'spark.sql.autoBroadcastJoinThreshold': broadcast_threshold,
            'spark.sql.join.preferSortMergeJoin': 'false' if data_size_gb < 50 else 'true',
            'spark.sql.broadcastTimeout': '600',
        },
        'io_optimization': {
            'spark.sql.parquet.filterPushdown': 'true',
            'spark.sql.parquet.mergeSchema': 'false',
            'spark.hadoop.fs.s3a.fast.upload': 'true',
            'spark.hadoop.fs.s3a.multipart.size': '128MB',
            'spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version': '2',
        },
        'wide_table_specific': {
            'spark.sql.columnVector.offheap.enabled': 'true',
            'spark.sql.execution.columnar.scan.enabled': 'true',
        } if has_wide_tables else {},
        'complex_type_specific': {
            'spark.sql.legacy.timeParserPolicy': 'LEGACY',
            'spark.sql.mapKeyDedupPolicy': 'LAST_WIN',
        } if has_complex_types else {},
    }

    glue_job_params = {
        '--conf': ' '.join([
            f"spark.sql.adaptive.enabled=true",
            f"spark.sql.shuffle.partitions={shuffle_partitions}",
            f"spark.sql.adaptive.skewJoin.enabled={'true' if has_skew else 'false'}",
        ])
    }

    highlights = []
    if has_skew:
        highlights.append("AQE skew join enabled - critical for your skewed partitions")
    if data_size_gb > 100:
        highlights.append(f"shuffle.partitions={shuffle_partitions} tuned for {data_size_gb:.0f}GB data")
    if has_wide_tables:
        highlights.append("Off-heap columnar vectors enabled for wide table performance")
    highlights.append(f"Broadcast threshold={broadcast_threshold} based on data size")

    print(f"      [TOOL] Generated {sum(len(v) for v in configs.values())} spark configs")

    return {
        'configs': configs,
        'glue_job_parameters': glue_job_params,
        'key_highlights': highlights,
        'how_to_apply': "Pass via Glue job --conf parameter or spark.sparkContext.setConf() at runtime"
    }


@tool
def get_alternative_tools_analysis(data_size_gb: float, monthly_runs: int,
                                    avg_runtime_hours: float, has_streaming: bool = False,
                                    has_ml: bool = False) -> Dict[str, Any]:
    """Analyze alternative AWS and cross-platform tools that could improve cost or performance.

    Args:
        data_size_gb: Total data size in GB
        monthly_runs: Number of monthly job runs
        avg_runtime_hours: Average runtime in hours
        has_streaming: Whether workload has streaming requirements
        has_ml: Whether workload includes ML/feature engineering

    Returns:
        Dict with ranked alternative tools and innovative recommendations
    """
    print(f"      [TOOL] get_alternative_tools_analysis: {data_size_gb}GB, {monthly_runs} runs/mo, stream={has_streaming}")

    glue_monthly = monthly_runs * avg_runtime_hours * 10 * 0.44  # ~10 DPUs

    aws_alternatives = []

    # AWS EMR Serverless
    emr_cost = monthly_runs * avg_runtime_hours * 8 * 0.26
    aws_alternatives.append({
        'tool': 'AWS EMR Serverless',
        'category': 'AWS Native',
        'monthly_cost_est': round(emr_cost, 2),
        'savings_pct': round((glue_monthly - emr_cost) / glue_monthly * 100, 1),
        'best_for': 'Large Spark jobs, full Spark API control, no cluster mgmt',
        'migration_effort': 'Low - same PySpark code, change entrypoint',
        'pros': ['40% cheaper than Glue', 'Full Spark config control', 'No DPU limits', 'Spot instance support'],
        'cons': ['No visual ETL', 'Manual IAM setup', 'Cold start latency'],
        'innovative_feature': 'Pre-initialized capacity pools for zero cold-start'
    })

    # AWS Athena CTAS
    if data_size_gb < 50:
        athena_cost = monthly_runs * data_size_gb * 0.005
        aws_alternatives.append({
            'tool': 'AWS Athena (CTAS)',
            'category': 'AWS Native - Serverless SQL',
            'monthly_cost_est': round(athena_cost, 2),
            'savings_pct': round((glue_monthly - athena_cost) / glue_monthly * 100, 1),
            'best_for': 'SQL-based transformations on data <100GB, ad-hoc ETL',
            'migration_effort': 'Medium - rewrite PySpark as SQL',
            'pros': ['Pay per query ($5/TB scanned)', 'Zero infrastructure', 'Instant start', 'Federated queries'],
            'cons': ['No complex UDFs', 'Limited window functions', 'No iterative processing'],
            'innovative_feature': 'Athena ACID transactions with Iceberg tables'
        })

    # AWS Glue with Iceberg
    aws_alternatives.append({
        'tool': 'AWS Glue + Apache Iceberg',
        'category': 'AWS Native - Enhancement',
        'monthly_cost_est': round(glue_monthly * 0.7, 2),
        'savings_pct': 30.0,
        'best_for': 'Incremental/CDC workloads, time-travel, schema evolution',
        'migration_effort': 'Low - add Iceberg config to existing Glue job',
        'pros': ['30% less data scanned via partition pruning', 'Row-level deletes (MERGE INTO)', 'Time travel queries', 'Schema evolution without rewrite'],
        'cons': ['Iceberg overhead for small datasets', 'Compaction jobs needed'],
        'innovative_feature': 'Automatic compaction + partition evolution based on query patterns'
    })

    # AWS Step Functions + Lambda
    if avg_runtime_hours < 0.25 and data_size_gb < 5:
        lambda_cost = monthly_runs * 0.002
        aws_alternatives.append({
            'tool': 'AWS Lambda + Step Functions',
            'category': 'AWS Serverless',
            'monthly_cost_est': round(lambda_cost, 2),
            'savings_pct': round((glue_monthly - lambda_cost) / glue_monthly * 100, 1),
            'best_for': 'Lightweight ETL <15min, event-driven pipelines',
            'migration_effort': 'High - rewrite logic, no Spark',
            'pros': ['Near-zero cost', 'Event-driven', 'No cold start with provisioned concurrency'],
            'cons': ['15min max runtime', '10GB memory limit', 'No Spark DataFrame API'],
            'innovative_feature': 'Lambda SnapStart for JVM-based ETL (sub-second init)'
        })

    # Kinesis + Flink for streaming
    if has_streaming:
        aws_alternatives.append({
            'tool': 'Amazon Kinesis + Managed Flink (Zeppelin)',
            'category': 'AWS Streaming',
            'monthly_cost_est': round(monthly_runs * 0.11 * avg_runtime_hours * 24, 2),
            'savings_pct': None,
            'best_for': 'Real-time streaming ETL, sub-second latency',
            'migration_effort': 'High - rewrite as streaming pipeline',
            'pros': ['Sub-second latency', 'Exactly-once semantics', 'Auto-scaling', 'Serverless'],
            'cons': ['Higher complexity', 'Stateful processing overhead'],
            'innovative_feature': 'Flink SQL + Iceberg Streaming Sink for real-time lakehouse'
        })

    # Cross-platform alternatives
    cross_platform = [
        {
            'platform': 'Databricks (AWS)',
            'monthly_cost_est': round(glue_monthly * 0.6, 2),
            'savings_pct': 40.0,
            'best_for': 'ML + ETL, Unity Catalog governance, Delta Lake',
            'innovative_features': ['Photon engine (3-5x faster than Spark)', 'Delta Live Tables for CDC', 'Liquid clustering replaces partitioning', 'AI/BI dashboards built-in']
        },
        {
            'platform': 'Snowflake (Snowpark)',
            'monthly_cost_est': round(glue_monthly * 0.9, 2),
            'savings_pct': 10.0,
            'best_for': 'SQL-heavy workloads, data sharing, governed access',
            'innovative_features': ['Snowpark Python for Spark-like DataFrames', 'Automatic clustering', 'Zero-copy cloning for dev/test', 'Dynamic tables (CDC built-in)']
        },
        {
            'platform': 'GCP Dataproc Serverless',
            'monthly_cost_est': round(glue_monthly * 0.55, 2),
            'savings_pct': 45.0,
            'best_for': 'Pure Spark workloads, BigQuery integration',
            'innovative_features': ['Dataproc Metastore for shared catalog', 'BigQuery Storage Read API (10x faster)', 'Persistent Spark History Server', 'Spot VMs for 60-91% savings']
        },
        {
            'platform': 'Azure Synapse Spark',
            'monthly_cost_est': round(glue_monthly * 0.75, 2),
            'savings_pct': 25.0,
            'best_for': 'Microsoft ecosystem, Power BI integration',
            'innovative_features': ['Synapse Link for zero-ETL from CosmosDB/SQL', 'Intelligent query acceleration', 'Dedicated SQL pool for BI workloads']
        },
    ]

    innovative_patterns = [
        {
            'pattern': 'Apache Iceberg + Incremental Processing',
            'description': 'Use Iceberg snapshot diff to process only new/changed rows since last run',
            'impact': 'Reduce data scanned by 70-90% for slowly changing datasets',
            'implementation': "spark.read.format('iceberg').option('start-snapshot-id', last_snapshot).load(table)"
        },
        {
            'pattern': 'Z-Order Clustering',
            'description': 'Co-locate related data using Z-order on frequently filtered columns',
            'impact': '50-80% reduction in files scanned for selective queries',
            'implementation': "ALTER TABLE t REORG WHERE ... ORDER BY ZORDER(col1, col2)"
        },
        {
            'pattern': 'Bloom Filter Indexes',
            'description': 'Add Parquet bloom filters on high-cardinality join/filter columns',
            'impact': '40-60% I/O reduction for point lookups and selective joins',
            'implementation': "df.write.option('parquet.bloom.filter.enabled#col', 'true').parquet(path)"
        },
        {
            'pattern': 'Dynamic Partition Pruning',
            'description': 'Let Spark broadcast dimension table filters to prune fact table partitions',
            'impact': '60-80% reduction in fact table scans for star schema joins',
            'implementation': "spark.conf.set('spark.sql.optimizer.dynamicPartitionPruning.enabled', 'true')"
        },
        {
            'pattern': 'Graviton3 Workers (EMR on EC2)',
            'description': 'Use ARM-based Graviton3 instances for 20-40% price-performance improvement',
            'impact': '20-40% better performance per dollar vs x86',
            'implementation': "emr_master: r7g.xlarge, core: r7g.2xlarge"
        },
    ]

    aws_alternatives.sort(key=lambda x: x.get('monthly_cost_est', 9999))

    print(f"      [TOOL] Found {len(aws_alternatives)} AWS alternatives, {len(cross_platform)} cross-platform options")

    return {
        'current_glue_monthly_est': round(glue_monthly, 2),
        'aws_alternatives': aws_alternatives,
        'cross_platform_options': cross_platform,
        'innovative_patterns': innovative_patterns,
        'top_recommendation': aws_alternatives[0] if aws_alternatives else None
    }


@tool
def calculate_platform_costs(workers: int, runtime_hours: float, runs_per_month: int) -> Dict[str, Any]:
    """Calculate and compare costs across cloud platforms.

    Args:
        workers: Number of workers/DPUs
        runtime_hours: Average runtime in hours
        runs_per_month: Monthly job runs

    Returns:
        Dict with cost comparison across platforms
    """
    print(f"      [TOOL] calculate_platform_costs: {workers} workers, {runtime_hours}h, {runs_per_month}/mo")

    platforms = {
        'AWS Glue': {'cost': 0.44, 'eff': 1.0},
        'AWS EMR': {'cost': 0.25, 'eff': 0.9},
        'AWS EMR Serverless': {'cost': 0.36, 'eff': 0.95},
        'GCP Dataproc': {'cost': 0.20, 'eff': 0.85},
        'GCP Dataproc Serverless': {'cost': 0.30, 'eff': 0.9},
        'Azure Synapse Spark': {'cost': 0.38, 'eff': 0.95},
        'Databricks': {'cost': 0.45, 'eff': 0.65},
        'Snowflake': {'cost': 0.50, 'eff': 0.5},
        'Spark on K8s': {'cost': 0.15, 'eff': 0.8},
    }

    results = []
    glue_cost = workers * 0.44 * runtime_hours * runs_per_month

    for name, info in platforms.items():
        adj_workers = max(2, int(workers * info['eff']))
        cost = adj_workers * info['cost'] * runtime_hours * runs_per_month
        savings = round((glue_cost - cost) / glue_cost * 100, 1) if glue_cost > 0 else 0

        results.append({
            'platform': name,
            'monthly_cost': round(cost, 2),
            'savings_vs_glue_pct': savings,
            'effective_workers': adj_workers
        })

    results.sort(key=lambda x: x['monthly_cost'])
    print(f"      [TOOL] Best: {results[0]['platform']} at ${results[0]['monthly_cost']}/mo")

    return {
        'current_glue_cost': round(glue_cost, 2),
        'platforms': results,
        'best_option': results[0],
        'max_savings_pct': results[0]['savings_vs_glue_pct']
    }


# =============================================================================
# COMPLIANCE TOOLS - PII / GDPR / HIPAA
# =============================================================================

# Canonical sensitive-column name patterns
_PII_PATTERNS = {
    'DIRECT_IDENTIFIER': [
        'ssn', 'social_security', 'passport', 'national_id', 'tax_id', 'ein',
        'drivers_license', 'license_number', 'voter_id',
    ],
    'CONTACT': [
        'email', 'phone', 'mobile', 'fax', 'address', 'street', 'city', 'zip',
        'postal', 'latitude', 'longitude', 'geo',
    ],
    'FINANCIAL': [
        'credit_card', 'card_number', 'cvv', 'iban', 'account_number', 'routing',
        'bank_account', 'salary', 'income', 'wage', 'payment',
    ],
    'HEALTH_PHI': [
        'diagnosis', 'icd', 'medication', 'prescription', 'treatment', 'condition',
        'patient', 'mrn', 'health_plan', 'insurance_id', 'npi', 'dea_number',
        'discharge', 'admission', 'procedure_code', 'lab_result', 'vital',
    ],
    'BIOMETRIC': [
        'fingerprint', 'retina', 'face_id', 'voiceprint', 'dna', 'biometric',
    ],
    'DEMOGRAPHIC': [
        'dob', 'birth_date', 'age', 'gender', 'race', 'ethnicity', 'religion',
        'nationality', 'marital_status', 'sexual_orientation',
    ],
    'CREDENTIAL': [
        'password', 'passwd', 'secret', 'token', 'api_key', 'private_key',
        'access_key', 'credential', 'auth_token',
    ],
    'NAME': [
        'first_name', 'last_name', 'full_name', 'middle_name', 'maiden_name',
        'legal_name',
    ],
}

_GDPR_CATEGORIES = {'DIRECT_IDENTIFIER', 'CONTACT', 'FINANCIAL', 'DEMOGRAPHIC', 'NAME', 'BIOMETRIC'}
_HIPAA_CATEGORIES = {'DIRECT_IDENTIFIER', 'CONTACT', 'HEALTH_PHI', 'BIOMETRIC'}
_PCI_CATEGORIES = {'FINANCIAL', 'CREDENTIAL'}


@tool
def scan_columns_for_pii(database: str, table: str) -> Dict[str, Any]:
    """Scan Glue table columns for PII, GDPR, HIPAA, and PCI sensitive data patterns.

    Args:
        database: Glue database name
        table: Table name

    Returns:
        Dict with flagged columns, regulation applicability, and masking recommendations
    """
    print(f"      [TOOL] scan_columns_for_pii: {database}.{table}")

    if not HAS_AWS:
        return {'error': 'boto3 not installed'}

    try:
        glue = boto3.client('glue', region_name='us-west-2')
        resp = glue.get_table(DatabaseName=database, Name=table)
        tbl = resp['Table']
        sd = tbl.get('StorageDescriptor', {})
        all_cols = sd.get('Columns', []) + tbl.get('PartitionKeys', [])
    except Exception as e:
        print(f"      [TOOL] ERROR: {e}")
        return {'error': str(e)}

    flagged = {}
    for col in all_cols:
        col_name = col['Name'].lower()
        col_type = col.get('Type', 'string')
        for category, keywords in _PII_PATTERNS.items():
            for kw in keywords:
                if kw in col_name:
                    flagged[col['Name']] = {
                        'category': category,
                        'matched_keyword': kw,
                        'col_type': col_type,
                        'gdpr': category in _GDPR_CATEGORIES,
                        'hipaa': category in _HIPAA_CATEGORIES,
                        'pci': category in _PCI_CATEGORIES,
                    }
                    break
            if col['Name'] in flagged:
                break

    gdpr_cols = [c for c, v in flagged.items() if v['gdpr']]
    hipaa_cols = [c for c, v in flagged.items() if v['hipaa']]
    pci_cols = [c for c, v in flagged.items() if v['pci']]

    masking_recs = []
    for col_name, info in flagged.items():
        cat = info['category']
        if cat in ('DIRECT_IDENTIFIER', 'CREDENTIAL'):
            masking_recs.append(f"{col_name}: TOKENIZE or HASH (SHA-256)")
        elif cat == 'FINANCIAL':
            masking_recs.append(f"{col_name}: MASK last 4 digits only (PCI DSS req)")
        elif cat in ('CONTACT', 'NAME'):
            masking_recs.append(f"{col_name}: PSEUDONYMIZE via lookup table (GDPR Art.4)")
        elif cat == 'HEALTH_PHI':
            masking_recs.append(f"{col_name}: ENCRYPT at-rest + RBAC (HIPAA §164.312)")
        elif cat == 'DEMOGRAPHIC':
            masking_recs.append(f"{col_name}: GENERALIZE (age range, region instead of exact)")
        elif cat == 'BIOMETRIC':
            masking_recs.append(f"{col_name}: IRREVERSIBLE HASH + separate storage required")

    print(f"      [TOOL] {len(flagged)} sensitive cols: GDPR={len(gdpr_cols)}, HIPAA={len(hipaa_cols)}, PCI={len(pci_cols)}")

    return {
        'database': database,
        'table': table,
        'total_columns_scanned': len(all_cols),
        'sensitive_columns_found': len(flagged),
        'flagged_columns': flagged,
        'gdpr_applicable': len(gdpr_cols) > 0,
        'hipaa_applicable': len(hipaa_cols) > 0,
        'pci_applicable': len(pci_cols) > 0,
        'gdpr_columns': gdpr_cols,
        'hipaa_columns': hipaa_cols,
        'pci_columns': pci_cols,
        'masking_recommendations': masking_recs,
        'compliance_risk': 'HIGH' if hipaa_cols or (len(gdpr_cols) > 3) else 'MEDIUM' if gdpr_cols or pci_cols else 'LOW',
    }


@tool
def derive_sensitive_data_via_query(database: str, table: str, sample_size: int = 1000) -> Dict[str, Any]:
    """Run Athena sample queries to derive sensitive data by analyzing actual values (regex-based).

    Args:
        database: Database name
        table: Table name
        sample_size: Number of rows to sample

    Returns:
        Dict with value-level PII detection results
    """
    print(f"      [TOOL] derive_sensitive_data_via_query: {database}.{table} (sample={sample_size})")

    if not HAS_AWS:
        return {'error': 'boto3 not installed'}

    try:
        glue = boto3.client('glue', region_name='us-west-2')
        resp = glue.get_table(DatabaseName=database, Name=table)
        cols = resp['Table'].get('StorageDescriptor', {}).get('Columns', [])
        string_cols = [c['Name'] for c in cols if 'string' in c.get('Type', '').lower() or 'varchar' in c.get('Type', '').lower()][:8]
    except Exception as e:
        return {'error': str(e)}

    if not string_cols:
        return {'message': 'No string columns to sample', 'table': f'{database}.{table}'}

    # Build regex detection SQL for each string column
    detections = {}
    patterns = {
        'EMAIL':        r"REGEXP_LIKE({col}, '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{{2,}}')",
        'US_PHONE':     r"REGEXP_LIKE({col}, '\d{{3}}[-.\s]?\d{{3}}[-.\s]?\d{{4}}')",
        'SSN':          r"REGEXP_LIKE({col}, '\d{{3}}-\d{{2}}-\d{{4}}')",
        'CREDIT_CARD':  r"REGEXP_LIKE({col}, '\d{{4}}[- ]?\d{{4}}[- ]?\d{{4}}[- ]?\d{{4}}')",
        'IP_ADDRESS':   r"REGEXP_LIKE({col}, '\d{{1,3}}\.\d{{1,3}}\.\d{{1,3}}\.\d{{1,3}}')",
        'DATE_OF_BIRTH':r"REGEXP_LIKE({col}, '\d{{4}}-\d{{2}}-\d{{2}}') AND LOWER({col}) LIKE '%birth%'",
    }

    for col in string_cols:
        col_flags = []
        for pii_type, pattern_tpl in patterns.items():
            pattern = pattern_tpl.format(col=col)
            sql = f"""
                SELECT COUNT(*) as matches
                FROM (SELECT {col} FROM {database}.{table} LIMIT {sample_size})
                WHERE {pattern}
            """
            result = run_athena_query(sql, database)
            if result.get('success') and result.get('rows'):
                count = int(result['rows'][0].get('matches', 0))
                if count > 0:
                    col_flags.append({'type': pii_type, 'sample_matches': count})

        if col_flags:
            detections[col] = col_flags

    gdpr_hit = any(
        any(f['type'] in ('EMAIL', 'US_PHONE', 'DATE_OF_BIRTH') for f in flags)
        for flags in detections.values()
    )
    hipaa_hit = any(
        any(f['type'] in ('SSN',) for f in flags)
        for flags in detections.values()
    )
    pci_hit = any(
        any(f['type'] in ('CREDIT_CARD',) for f in flags)
        for flags in detections.values()
    )

    print(f"      [TOOL] Value-level PII in {len(detections)} cols: GDPR={gdpr_hit}, HIPAA={hipaa_hit}, PCI={pci_hit}")

    return {
        'database': database,
        'table': table,
        'sample_size': sample_size,
        'columns_with_pii_values': detections,
        'gdpr_triggered': gdpr_hit,
        'hipaa_triggered': hipaa_hit,
        'pci_triggered': pci_hit,
        'action_required': bool(detections),
    }


@tool
def check_data_encryption_compliance(database: str, table: str) -> Dict[str, Any]:
    """Check S3 encryption, Glue encryption settings, and KMS key usage for a table.

    Args:
        database: Glue database name
        table: Table name

    Returns:
        Dict with encryption status and compliance gaps
    """
    print(f"      [TOOL] check_data_encryption_compliance: {database}.{table}")

    if not HAS_AWS:
        return {'error': 'boto3 not installed'}

    findings = {}

    try:
        glue = boto3.client('glue', region_name='us-west-2')
        resp = glue.get_table(DatabaseName=database, Name=table)
        tbl = resp['Table']
        location = tbl.get('StorageDescriptor', {}).get('Location', '')
        params = tbl.get('Parameters', {})
        findings['glue_table_location'] = location
        findings['glue_table_params'] = params

        # Check S3 bucket encryption
        if location.startswith('s3://'):
            bucket = location.replace('s3://', '').split('/')[0]
            s3 = boto3.client('s3', region_name='us-west-2')
            try:
                enc = s3.get_bucket_encryption(Bucket=bucket)
                rules = enc.get('ServerSideEncryptionConfiguration', {}).get('Rules', [])
                enc_type = rules[0].get('ApplyServerSideEncryptionByDefault', {}).get('SSEAlgorithm', 'NONE') if rules else 'NONE'
                kms_key = rules[0].get('ApplyServerSideEncryptionByDefault', {}).get('KMSMasterKeyID', None) if rules else None
                findings['s3_encryption'] = enc_type
                findings['kms_key_id'] = kms_key
                findings['s3_encrypted'] = enc_type in ('aws:kms', 'AES256')
                findings['uses_cmk'] = kms_key is not None and 'alias/aws/' not in str(kms_key)
            except Exception as e:
                findings['s3_encryption'] = f'ERROR: {e}'
                findings['s3_encrypted'] = False

        # Check Glue Data Catalog encryption
        try:
            dc_enc = glue.get_data_catalog_encryption_settings()
            catalog_enc = dc_enc.get('DataCatalogEncryptionSettings', {})
            findings['catalog_password_encrypted'] = catalog_enc.get('ConnectionPasswordEncryption', {}).get('ReturnConnectionPasswordEncrypted', False)
            findings['catalog_encrypted_at_rest'] = catalog_enc.get('EncryptionAtRest', {}).get('CatalogEncryptionMode', 'DISABLED') != 'DISABLED'
        except Exception as e:
            findings['catalog_encryption_check'] = f'ERROR: {e}'

        gaps = []
        if not findings.get('s3_encrypted'):
            gaps.append('CRITICAL: S3 bucket not encrypted - required for HIPAA/PCI/GDPR')
        if not findings.get('uses_cmk'):
            gaps.append('HIGH: Using AWS-managed key - use Customer Managed Key (CMK) for full control')
        if not findings.get('catalog_encrypted_at_rest'):
            gaps.append('MEDIUM: Glue Data Catalog not encrypted at rest')

        findings['compliance_gaps'] = gaps
        findings['overall_status'] = 'COMPLIANT' if not gaps else ('NON_COMPLIANT' if any('CRITICAL' in g for g in gaps) else 'PARTIAL')

        print(f"      [TOOL] Encryption: S3={findings.get('s3_encrypted')}, CMK={findings.get('uses_cmk')}, gaps={len(gaps)}")
        return findings

    except Exception as e:
        print(f"      [TOOL] ERROR: {e}")
        return {'error': str(e)}


# =============================================================================
# LEARNING TOOLS - DETAILED GLUE RUN METRICS
# =============================================================================

@tool
def get_glue_run_metrics(job_name: str, run_id: str) -> Dict[str, Any]:
    """Fetch detailed Spark metrics for a specific Glue job run from CloudWatch.

    Args:
        job_name: Name of the Glue job
        run_id: Specific run ID to fetch metrics for

    Returns:
        Dict with executor memory, driver memory, data shuffled, skew indicators, spark configs
    """
    print(f"      [TOOL] get_glue_run_metrics: {job_name}/{run_id}")

    if not HAS_AWS:
        return {'error': 'boto3 not installed'}

    metrics_data = {}

    try:
        cw = boto3.client('cloudwatch', region_name='us-west-2')
        glue = boto3.client('glue', region_name='us-west-2')

        # Get job run details
        run = glue.get_job_run(JobName=job_name, RunId=run_id)['JobRun']
        started = run.get('StartedOn')
        completed = run.get('CompletedOn')
        num_workers = run.get('NumberOfWorkers', 10)
        worker_type = run.get('WorkerType', 'G.1X')
        dpu_seconds = run.get('DPUSeconds', 0)

        metrics_data['run_id'] = run_id
        metrics_data['job_name'] = job_name
        metrics_data['status'] = run.get('JobRunState')
        metrics_data['num_workers'] = num_workers
        metrics_data['worker_type'] = worker_type
        metrics_data['duration_sec'] = run.get('ExecutionTime', 0)
        metrics_data['dpu_seconds'] = dpu_seconds
        metrics_data['cost_usd'] = round(dpu_seconds / 3600 * {'G.1X': 0.44, 'G.2X': 0.88, 'G.4X': 1.76}.get(worker_type, 0.44), 4)

        # Parse Spark configs from job arguments
        glue_job = glue.get_job(JobName=job_name)['Job']
        spark_configs = {}
        for k, v in run.get('Arguments', {}).items():
            if '--conf' in k or 'spark.' in v:
                spark_configs[k] = v
        metrics_data['spark_configs'] = spark_configs or glue_job.get('DefaultArguments', {})

        if started and completed:
            end_time = completed
            start_time = started

            # Metric names from Glue CloudWatch namespace
            cw_metrics = [
                ('glue.driver.aggregate.bytesRead',    'bytes_read'),
                ('glue.driver.aggregate.bytesWritten', 'bytes_written'),
                ('glue.driver.aggregate.recordsRead',  'records_read'),
                ('glue.driver.aggregate.shuffleLocalBytesRead',  'shuffle_local_bytes'),
                ('glue.driver.aggregate.shuffleRemoteBytesRead', 'shuffle_remote_bytes'),
                ('glue.driver.jvm.heap.usage',         'driver_heap_usage_pct'),
                ('glue.driver.jvm.heap.used',          'driver_heap_used_bytes'),
                ('glue.ALL.jvm.heap.usage',            'executor_heap_usage_avg'),
                ('glue.ALL.jvm.heap.used',             'executor_heap_used_bytes'),
                ('glue.driver.ExecutorAllocationManager.executors.numberAllExecutors', 'executors_total'),
                ('glue.driver.ExecutorAllocationManager.executors.numberMaxNeededExecutors', 'executors_max_needed'),
            ]

            for metric_name, key in cw_metrics:
                try:
                    resp = cw.get_metric_statistics(
                        Namespace='Glue',
                        MetricName=metric_name,
                        Dimensions=[
                            {'Name': 'JobName', 'Value': job_name},
                            {'Name': 'JobRunId', 'Value': run_id},
                            {'Name': 'Type', 'Value': 'gauge'},
                        ],
                        StartTime=start_time,
                        EndTime=end_time,
                        Period=3600,
                        Statistics=['Maximum', 'Average'],
                    )
                    pts = resp.get('Datapoints', [])
                    if pts:
                        metrics_data[key] = {
                            'max': max(p['Maximum'] for p in pts),
                            'avg': round(sum(p['Average'] for p in pts) / len(pts), 2),
                        }
                except Exception:
                    pass

        # Compute derived insights
        br = metrics_data.get('bytes_read', {}).get('max', 0)
        bw = metrics_data.get('bytes_written', {}).get('max', 0)
        shuffle = (metrics_data.get('shuffle_local_bytes', {}).get('max', 0) +
                   metrics_data.get('shuffle_remote_bytes', {}).get('max', 0))

        metrics_data['data_read_gb']      = round(br / (1024**3), 3) if br else None
        metrics_data['data_written_gb']   = round(bw / (1024**3), 3) if bw else None
        metrics_data['shuffle_total_gb']  = round(shuffle / (1024**3), 3) if shuffle else None
        metrics_data['shuffle_to_read_ratio'] = round(shuffle / br, 2) if br and br > 0 else None
        metrics_data['driver_heap_pct']   = metrics_data.get('driver_heap_usage_pct', {}).get('max')
        metrics_data['executor_heap_pct'] = metrics_data.get('executor_heap_usage_avg', {}).get('avg')

        # Skew indicators
        skew_flags = []
        if metrics_data['shuffle_to_read_ratio'] and metrics_data['shuffle_to_read_ratio'] > 5:
            skew_flags.append(f"High shuffle ratio ({metrics_data['shuffle_to_read_ratio']}x) - likely data skew")
        if metrics_data['driver_heap_pct'] and metrics_data['driver_heap_pct'] > 0.85:
            skew_flags.append(f"Driver heap {metrics_data['driver_heap_pct']*100:.0f}% - check for collect() or driver OOM")
        if metrics_data['executor_heap_pct'] and metrics_data['executor_heap_pct'] > 0.80:
            skew_flags.append(f"Executor heap {metrics_data['executor_heap_pct']*100:.0f}% - memory pressure, consider G.2X")

        metrics_data['skew_indicators'] = skew_flags
        metrics_data['health_status'] = 'CONCERNING' if skew_flags else 'HEALTHY'

        print(f"      [TOOL] Read={metrics_data.get('data_read_gb')}GB, Shuffle={metrics_data.get('shuffle_total_gb')}GB, "
              f"DriverHeap={metrics_data.get('driver_heap_pct')}, skew_flags={len(skew_flags)}")

        return metrics_data

    except Exception as e:
        print(f"      [TOOL] ERROR: {e}")
        return {'error': str(e), 'job_name': job_name, 'run_id': run_id}


@tool
def get_glue_job_run_history(job_name: str, max_runs: int = 20) -> Dict[str, Any]:
    """Get full historical metrics for all recent Glue job runs with trend analysis.

    Args:
        job_name: Name of the Glue job
        max_runs: Max runs to analyze

    Returns:
        Dict with per-run metrics, trend analysis, anomalies, and optimal config learnings
    """
    print(f"      [TOOL] get_glue_job_run_history: {job_name} (max={max_runs})")

    if not HAS_AWS:
        return {'error': 'boto3 not installed'}

    try:
        glue = boto3.client('glue', region_name='us-west-2')
        cw = boto3.client('cloudwatch', region_name='us-west-2')

        runs_resp = glue.get_job_runs(JobName=job_name, MaxResults=max_runs)
        runs = runs_resp.get('JobRuns', [])

        run_summaries = []
        for r in runs:
            dpu_sec = r.get('DPUSeconds', 0)
            wtype = r.get('WorkerType', 'G.1X')
            cost = round(dpu_sec / 3600 * {'G.1X': 0.44, 'G.2X': 0.88, 'G.4X': 1.76}.get(wtype, 0.44), 4)
            run_summaries.append({
                'run_id':       r['Id'],
                'status':       r['JobRunState'],
                'started':      r.get('StartedOn').isoformat() if r.get('StartedOn') else None,
                'duration_sec': r.get('ExecutionTime', 0),
                'dpu_seconds':  dpu_sec,
                'num_workers':  r.get('NumberOfWorkers', 0),
                'worker_type':  wtype,
                'cost_usd':     cost,
                'error':        (r.get('ErrorMessage', '')[:300] if r.get('ErrorMessage') else None),
            })

        # Fetch CloudWatch aggregate metrics across all runs via most recent run
        aggregate_insights = {}
        if runs:
            latest_run = runs[0]
            run_id = latest_run['Id']
            started = latest_run.get('StartedOn')
            completed = latest_run.get('CompletedOn')
            if started and completed:
                for metric_name, key in [
                    ('glue.driver.aggregate.bytesRead',   'latest_bytes_read'),
                    ('glue.driver.aggregate.recordsRead', 'latest_records_read'),
                    ('glue.driver.aggregate.shuffleRemoteBytesRead', 'latest_shuffle_bytes'),
                    ('glue.driver.jvm.heap.usage',        'latest_driver_heap_pct'),
                    ('glue.ALL.jvm.heap.usage',           'latest_executor_heap_pct'),
                    ('glue.driver.ExecutorAllocationManager.executors.numberMaxNeededExecutors', 'max_executors_needed'),
                ]:
                    try:
                        resp = cw.get_metric_statistics(
                            Namespace='Glue',
                            MetricName=metric_name,
                            Dimensions=[
                                {'Name': 'JobName',    'Value': job_name},
                                {'Name': 'JobRunId',   'Value': run_id},
                                {'Name': 'Type',       'Value': 'gauge'},
                            ],
                            StartTime=started,
                            EndTime=completed,
                            Period=3600,
                            Statistics=['Maximum', 'Average'],
                        )
                        pts = resp.get('Datapoints', [])
                        if pts:
                            aggregate_insights[key] = round(max(p['Maximum'] for p in pts), 4)
                    except Exception:
                        pass

        # Trend analysis
        succeeded = [r for r in run_summaries if r['status'] == 'SUCCEEDED']
        failed    = [r for r in run_summaries if r['status'] == 'FAILED']
        durations = [r['duration_sec'] for r in succeeded]
        costs     = [r['cost_usd'] for r in succeeded]

        trends = {}
        if len(durations) >= 2:
            first_half  = sum(durations[:len(durations)//2]) / max(1, len(durations)//2)
            second_half = sum(durations[len(durations)//2:]) / max(1, len(durations) - len(durations)//2)
            trends['duration_trend'] = 'INCREASING' if second_half > first_half * 1.1 else 'DECREASING' if second_half < first_half * 0.9 else 'STABLE'
            trends['duration_change_pct'] = round((second_half - first_half) / first_half * 100, 1) if first_half else 0

        # Anomalies: runs more than 2x average duration
        avg_dur = sum(durations) / len(durations) if durations else 0
        anomalies = [r for r in succeeded if r['duration_sec'] > avg_dur * 2]

        # Optimal config learning
        if succeeded:
            best_run = min(succeeded, key=lambda r: r['cost_usd'])
        else:
            best_run = None

        print(f"      [TOOL] {len(runs)} runs: {len(succeeded)} success, {len(failed)} failed, "
              f"avg_dur={avg_dur:.0f}s, trend={trends.get('duration_trend','N/A')}")

        return {
            'job_name':          job_name,
            'total_runs':        len(runs),
            'succeeded':         len(succeeded),
            'failed':            len(failed),
            'success_rate_pct':  round(len(succeeded) / len(runs) * 100, 1) if runs else 0,
            'avg_duration_sec':  round(avg_dur, 1),
            'avg_cost_usd':      round(sum(costs) / len(costs), 4) if costs else 0,
            'total_cost_usd':    round(sum(r['cost_usd'] for r in run_summaries), 2),
            'trends':            trends,
            'anomaly_runs':      anomalies,
            'best_run':          best_run,
            'latest_run_metrics': aggregate_insights,
            'run_history':       run_summaries,
            'common_errors':     list({r['error'] for r in failed if r['error']})[:5],
        }

    except glue.exceptions.EntityNotFoundException:
        print(f"      [TOOL] ERROR: Job not found: {job_name}")
        return {'error': f'Job not found: {job_name}'}
    except Exception as e:
        print(f"      [TOOL] ERROR: {e}")
        return {'error': str(e)}


# =============================================================================
# RESOURCE ALLOCATOR TOOLS
# =============================================================================

@tool
def compute_resource_recommendation(
    total_data_gb: float,
    num_tables: int,
    has_skew: bool,
    has_complex_joins: bool,
    code_issues: int,
    historical_avg_duration_sec: float,
    historical_avg_workers: int,
    historical_success_rate: float,
    day_of_week: str,
    hour_of_day: int,
    worker_type: str = 'G.1X'
) -> Dict[str, Any]:
    """Compute optimal resource allocation combining sizing, history, code analysis, and time patterns.

    Args:
        total_data_gb: Total input data in GB from Sizing Agent
        num_tables: Number of source tables (join complexity)
        has_skew: Whether data skew was detected
        has_complex_joins: Whether code has broadcast-missing or cross-join patterns
        code_issues: Number of anti-patterns found by Code Analysis Agent
        historical_avg_duration_sec: Average duration from Learning Agent
        historical_avg_workers: Average workers used historically
        historical_success_rate: Historical success rate (0.0-1.0)
        day_of_week: e.g. 'Monday', 'Saturday'
        hour_of_day: 0-23
        worker_type: Current worker type
    Returns:
        Dict with recommended workers, worker_type, spark configs, and reasoning
    """
    print(f"      [TOOL] compute_resource_recommendation: {total_data_gb}GB, dow={day_of_week}, hour={hour_of_day}")

    worker_mem = {'G.1X': 16, 'G.2X': 32, 'G.4X': 64, 'G.8X': 128}.get(worker_type, 16)

    # Base workers from data volume
    base_workers = max(2, int(total_data_gb / 5))

    # Skew multiplier
    skew_mult = 1.4 if has_skew else 1.0

    # Join complexity
    join_mult = 1.0 + (num_tables - 1) * 0.1

    # Code issue penalty (anti-patterns mean we need more headroom)
    code_mult = 1.0 + min(code_issues * 0.05, 0.3)

    # Historical learning weight
    if historical_avg_workers > 0 and historical_avg_duration_sec > 0:
        # If historically fast with fewer workers, trust history
        hist_weight = 0.4
        data_weight = 0.6
        hist_workers = historical_avg_workers
        computed = int(base_workers * skew_mult * join_mult * code_mult)
        blended = int(computed * data_weight + hist_workers * hist_weight)
    else:
        blended = int(base_workers * skew_mult * join_mult * code_mult)

    # Weekend/off-peak: reduce workers (less contention, can run leaner)
    is_weekend = day_of_week in ('Saturday', 'Sunday')
    is_off_peak = hour_of_day < 8 or hour_of_day > 20
    if is_weekend or is_off_peak:
        blended = max(2, int(blended * 0.85))
        schedule_note = 'Off-peak/weekend: reduced workers by 15%'
    else:
        schedule_note = 'Business hours: standard allocation'

    # Low success rate → add buffer
    if historical_success_rate < 0.85:
        blended = int(blended * 1.2)
        schedule_note += ' | Low success rate: +20% buffer'

    recommended_workers = max(2, min(blended, 50))

    # Recommend worker type upgrade if data per worker is high
    data_per_worker = total_data_gb / max(recommended_workers - 1, 1)
    rec_worker_type = worker_type
    upgrade_reason = None
    if data_per_worker > worker_mem * 0.6:
        upgrade_map = {'G.1X': 'G.2X', 'G.2X': 'G.4X', 'G.4X': 'G.8X', 'G.8X': 'G.8X'}
        rec_worker_type = upgrade_map.get(worker_type, 'G.2X')
        upgrade_reason = f"Data/executor ({data_per_worker:.1f}GB) > 60% of {worker_mem}GB RAM"

    shuffle_partitions = max(200, recommended_workers * 4)
    spark_configs = {
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.skewJoin.enabled': 'true' if has_skew else 'false',
        'spark.sql.shuffle.partitions': str(shuffle_partitions),
        'spark.sql.autoBroadcastJoinThreshold': '256MB' if total_data_gb < 100 else '64MB',
        'spark.memory.fraction': '0.8',
        'spark.executor.memory': f"{int({'G.1X':16,'G.2X':32,'G.4X':64,'G.8X':128}.get(rec_worker_type,16)*0.75)}g",
    }

    reasoning = [
        f"Base workers from data volume ({total_data_gb:.1f}GB / 5GB per worker): {base_workers}",
        f"Skew multiplier: {skew_mult}x" if has_skew else "No skew detected",
        f"Join complexity ({num_tables} tables): {join_mult:.2f}x",
        f"Code issues ({code_issues} anti-patterns): {code_mult:.2f}x",
        f"Historical blend (avg {historical_avg_workers} workers): applied" if historical_avg_workers else "No history",
        schedule_note,
    ]
    if upgrade_reason:
        reasoning.append(f"Worker type upgrade {worker_type}→{rec_worker_type}: {upgrade_reason}")

    print(f"      [TOOL] Recommended: {recommended_workers}x {rec_worker_type}, shuffle_partitions={shuffle_partitions}")

    return {
        'recommended_workers': recommended_workers,
        'recommended_worker_type': rec_worker_type,
        'current_worker_type': worker_type,
        'upgrade_needed': rec_worker_type != worker_type,
        'spark_configs': spark_configs,
        'data_per_worker_gb': round(data_per_worker, 2),
        'schedule_context': {'day_of_week': day_of_week, 'hour_of_day': hour_of_day, 'is_weekend': is_weekend, 'is_off_peak': is_off_peak},
        'reasoning': reasoning,
        'estimated_duration_min': round(historical_avg_duration_sec / 60 * (historical_avg_workers / max(recommended_workers, 1)), 1) if historical_avg_duration_sec else None,
        'estimated_cost_usd': round(recommended_workers * {'G.1X': 0.44, 'G.2X': 0.88, 'G.4X': 1.76}.get(rec_worker_type, 0.44) * (historical_avg_duration_sec / 3600 if historical_avg_duration_sec else 0.5), 2),
    }


@tool
def get_weekday_volume_pattern(job_name: str) -> Dict[str, Any]:
    """Analyze historical execution history to find weekday vs weekend volume and duration patterns.

    Args:
        job_name: Glue job name

    Returns:
        Dict with per-weekday avg duration, cost, and volume trends
    """
    print(f"      [TOOL] get_weekday_volume_pattern: {job_name}")

    history_file = Path(f'data/execution_history/{job_name}.jsonl')
    if not history_file.exists():
        # Fall back to Glue runs
        if HAS_AWS:
            try:
                glue = boto3.client('glue', region_name='us-west-2')
                runs = glue.get_job_runs(JobName=job_name, MaxResults=50).get('JobRuns', [])
            except Exception:
                runs = []
        else:
            runs = []
        records = [{'timestamp': r.get('StartedOn').isoformat() if r.get('StartedOn') else None,
                    'duration_sec': r.get('ExecutionTime', 0),
                    'status': r.get('JobRunState')} for r in runs]
    else:
        with open(history_file) as f:
            records = [json.loads(l) for l in f if l.strip()]

    from collections import defaultdict
    day_stats: Dict[str, list] = defaultdict(list)
    for rec in records:
        ts = rec.get('timestamp') or rec.get('started')
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                day_name = dt.strftime('%A')
                day_stats[day_name].append(rec.get('duration_sec', 0))
            except Exception:
                pass

    pattern = {}
    for day, durations in day_stats.items():
        pattern[day] = {
            'avg_duration_sec': round(sum(durations) / len(durations), 1),
            'run_count': len(durations),
            'max_duration_sec': max(durations),
        }

    weekday_avg = sum(v['avg_duration_sec'] for k, v in pattern.items() if k not in ('Saturday', 'Sunday')) / max(1, sum(1 for k in pattern if k not in ('Saturday', 'Sunday')))
    weekend_avg = sum(v['avg_duration_sec'] for k, v in pattern.items() if k in ('Saturday', 'Sunday')) / max(1, sum(1 for k in pattern if k in ('Saturday', 'Sunday')))

    print(f"      [TOOL] Weekday avg={weekday_avg:.0f}s, Weekend avg={weekend_avg:.0f}s")

    return {
        'job_name': job_name,
        'per_day_stats': pattern,
        'weekday_avg_duration_sec': round(weekday_avg, 1),
        'weekend_avg_duration_sec': round(weekend_avg, 1),
        'weekend_is_lighter': weekend_avg < weekday_avg * 0.9,
        'peak_day': max(pattern, key=lambda d: pattern[d]['avg_duration_sec']) if pattern else None,
        'lightest_day': min(pattern, key=lambda d: pattern[d]['avg_duration_sec']) if pattern else None,
    }


# =============================================================================
# EXECUTION METRICS + AUDIT TOOLS
# =============================================================================

@tool
def collect_post_execution_metrics(job_name: str, run_id: str) -> Dict[str, Any]:
    """Collect comprehensive post-execution Spark metrics from CloudWatch for Learning Agent.

    Covers: data skew, shuffle, driver/executor memory, idle executors,
    data movement between nodes, stage-level memory hotspots.

    Args:
        job_name: Glue job name
        run_id: Completed run ID

    Returns:
        Dict with full Spark metric breakdown ready for Learning Agent ingestion
    """
    print(f"      [TOOL] collect_post_execution_metrics: {job_name}/{run_id}")

    if not HAS_AWS:
        return {'error': 'boto3 not installed'}

    try:
        glue = boto3.client('glue', region_name='us-west-2')
        cw   = boto3.client('cloudwatch', region_name='us-west-2')

        run = glue.get_job_run(JobName=job_name, RunId=run_id)['JobRun']
        started   = run.get('StartedOn')
        completed = run.get('CompletedOn')
        if not started or not completed:
            return {'error': 'Run not completed yet', 'run_id': run_id}

        worker_type = run.get('WorkerType', 'G.1X')
        num_workers = run.get('NumberOfWorkers', 10)
        duration    = run.get('ExecutionTime', 0)
        dpu_sec     = run.get('DPUSeconds', 0)

        def _cw_stat(metric, stat='Maximum'):
            try:
                r = cw.get_metric_statistics(
                    Namespace='Glue',
                    MetricName=metric,
                    Dimensions=[
                        {'Name': 'JobName',   'Value': job_name},
                        {'Name': 'JobRunId',  'Value': run_id},
                        {'Name': 'Type',      'Value': 'gauge'},
                    ],
                    StartTime=started, EndTime=completed,
                    Period=int(duration) or 3600,
                    Statistics=[stat],
                )
                pts = r.get('Datapoints', [])
                return pts[0].get(stat) if pts else None
            except Exception:
                return None

        # Raw metrics
        bytes_read          = _cw_stat('glue.driver.aggregate.bytesRead')
        bytes_written       = _cw_stat('glue.driver.aggregate.bytesWritten')
        records_read        = _cw_stat('glue.driver.aggregate.recordsRead')
        shuffle_local       = _cw_stat('glue.driver.aggregate.shuffleLocalBytesRead')
        shuffle_remote      = _cw_stat('glue.driver.aggregate.shuffleRemoteBytesRead')
        driver_heap_used    = _cw_stat('glue.driver.jvm.heap.used')
        driver_heap_usage   = _cw_stat('glue.driver.jvm.heap.usage')
        exec_heap_usage     = _cw_stat('glue.ALL.jvm.heap.usage', 'Average')
        exec_heap_used      = _cw_stat('glue.ALL.jvm.heap.used', 'Average')
        num_exec_alloc      = _cw_stat('glue.driver.ExecutorAllocationManager.executors.numberAllExecutors')
        num_exec_needed     = _cw_stat('glue.driver.ExecutorAllocationManager.executors.numberMaxNeededExecutors')
        tasks_failed        = _cw_stat('glue.driver.aggregate.numFailedTasks')
        tasks_completed     = _cw_stat('glue.driver.aggregate.numCompletedTasks')
        gc_time             = _cw_stat('glue.driver.aggregate.elapsedTime')  # proxy for GC

        # Derived
        shuffle_total   = (shuffle_local or 0) + (shuffle_remote or 0)
        shuffle_ratio   = round(shuffle_total / bytes_read, 2) if bytes_read else None
        read_gb         = round(bytes_read / 1024**3, 3)    if bytes_read    else None
        written_gb      = round(bytes_written / 1024**3, 3) if bytes_written else None
        shuffle_gb      = round(shuffle_total / 1024**3, 3) if shuffle_total else None
        driver_heap_gb  = round(driver_heap_used / 1024**3, 2) if driver_heap_used else None
        exec_heap_pct   = round(exec_heap_usage * 100, 1)   if exec_heap_usage  else None
        driver_heap_pct = round(driver_heap_usage * 100, 1) if driver_heap_usage else None
        idle_executors  = round((num_exec_alloc or 0) - (num_exec_needed or 0), 1)

        # Stage memory hotspot proxy: tasks_failed / tasks_completed ratio
        task_failure_pct = round(tasks_failed / max(tasks_completed or 1, 1) * 100, 2) if tasks_failed else 0.0

        # Skew detection
        skew_flags = []
        if shuffle_ratio and shuffle_ratio > 5:
            skew_flags.append(f"HIGH SHUFFLE RATIO {shuffle_ratio}x - data skew likely")
        if driver_heap_pct and driver_heap_pct > 85:
            skew_flags.append(f"DRIVER HEAP {driver_heap_pct}% - check collect()/broadcast OOM")
        if exec_heap_pct and exec_heap_pct > 80:
            skew_flags.append(f"EXECUTOR HEAP AVG {exec_heap_pct}% - memory pressure, upgrade worker type")
        if idle_executors > num_workers * 0.3:
            skew_flags.append(f"{idle_executors:.0f} idle executors (>{num_workers*0.3:.0f}) - over-provisioned")
        if task_failure_pct > 5:
            skew_flags.append(f"Task failure rate {task_failure_pct}% - check OOM / skew in stages")

        cost = round(dpu_sec / 3600 * {'G.1X': 0.44, 'G.2X': 0.88, 'G.4X': 1.76, 'G.8X': 3.52}.get(worker_type, 0.44), 4)

        result = {
            'job_name':             job_name,
            'run_id':               run_id,
            'status':               run.get('JobRunState'),
            'worker_type':          worker_type,
            'num_workers':          num_workers,
            'duration_sec':         duration,
            'cost_usd':             cost,
            # Data volumes
            'data_read_gb':         read_gb,
            'data_written_gb':      written_gb,
            'records_read':         int(records_read) if records_read else None,
            # Shuffle / data movement between nodes
            'shuffle_local_gb':     round(shuffle_local / 1024**3, 3) if shuffle_local else None,
            'shuffle_remote_gb':    round(shuffle_remote / 1024**3, 3) if shuffle_remote else None,
            'shuffle_total_gb':     shuffle_gb,
            'shuffle_to_read_ratio':shuffle_ratio,
            # Memory
            'driver_heap_pct':      driver_heap_pct,
            'driver_heap_used_gb':  driver_heap_gb,
            'executor_heap_pct_avg':exec_heap_pct,
            # Executor utilization
            'executors_allocated':  int(num_exec_alloc) if num_exec_alloc else None,
            'executors_max_needed': int(num_exec_needed) if num_exec_needed else None,
            'idle_executors':       max(0, idle_executors),
            # Task health
            'tasks_completed':      int(tasks_completed) if tasks_completed else None,
            'tasks_failed':         int(tasks_failed) if tasks_failed else None,
            'task_failure_pct':     task_failure_pct,
            # Analysis
            'skew_flags':           skew_flags,
            'health_status':        'CONCERNING' if skew_flags else 'HEALTHY',
            'collected_at':         datetime.utcnow().isoformat(),
        }

        print(f"      [TOOL] Metrics collected: read={read_gb}GB, shuffle={shuffle_gb}GB, "
              f"driver_heap={driver_heap_pct}%, exec_heap={exec_heap_pct}%, idle={idle_executors}, skew_flags={len(skew_flags)}")
        return result

    except Exception as e:
        print(f"      [TOOL] ERROR: {e}")
        return {'error': str(e), 'job_name': job_name, 'run_id': run_id}


@tool
def write_audit_record(job_name: str, run_id: str, event_type: str,
                        details: Dict, table_name: str = 'etl_audit_log',
                        region: str = 'us-west-2') -> Dict[str, Any]:
    """Write an audit record to DynamoDB for compliance and operational tracking.

    Args:
        job_name: ETL job name
        run_id: Execution run ID
        event_type: e.g. 'JOB_START', 'JOB_COMPLETE', 'RESOURCE_ALLOCATED', 'COMPLIANCE_SCAN', 'DQ_CHECK'
        details: Dict of event-specific details to store
        table_name: DynamoDB table name (default: etl_audit_log)
        region: AWS region

    Returns:
        Confirmation dict
    """
    print(f"      [TOOL] write_audit_record: {table_name} | {job_name} | {event_type}")

    if not HAS_AWS:
        return {'error': 'boto3 not installed'}

    try:
        import uuid
        dynamodb = boto3.resource('dynamodb', region_name=region)
        table    = dynamodb.Table(table_name)

        record = {
            'audit_id':   str(uuid.uuid4()),
            'job_name':   job_name,
            'run_id':     run_id,
            'event_type': event_type,
            'timestamp':  datetime.utcnow().isoformat(),
            'details':    json.dumps(details, default=str),
            'ttl':        int(time.time()) + 90 * 86400,  # 90-day TTL
        }

        table.put_item(Item=record)
        print(f"      [TOOL] Written audit_id={record['audit_id']}")
        return {'success': True, 'audit_id': record['audit_id'], 'table': table_name}

    except Exception as e:
        print(f"      [TOOL] ERROR writing audit: {e}")
        # Fallback: write to local JSONL
        local_path = Path('data/audit_log.jsonl')
        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, 'a') as f:
            f.write(json.dumps({
                'job_name': job_name, 'run_id': run_id,
                'event_type': event_type, 'timestamp': datetime.utcnow().isoformat(),
                'details': details,
            }, default=str) + '\n')
        return {'success': True, 'fallback': 'local', 'path': str(local_path), 'error': str(e)}


@tool
def store_metrics_for_learning(job_name: str, run_metrics: Dict) -> Dict[str, Any]:
    """Persist post-execution Spark metrics to the learning store for future Resource Allocator use.

    Args:
        job_name: ETL job name
        run_metrics: Full metrics dict from collect_post_execution_metrics

    Returns:
        Confirmation dict
    """
    print(f"      [TOOL] store_metrics_for_learning: {job_name}")

    history_dir  = Path('data/execution_history')
    metrics_dir  = Path('data/spark_metrics')
    history_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    record = {'timestamp': datetime.utcnow().isoformat(), 'job_name': job_name, **run_metrics}

    # Append to execution history (used by learning agent)
    with open(history_dir / f"{job_name}.jsonl", 'a') as f:
        f.write(json.dumps(record, default=str) + '\n')

    # Separate detailed spark metrics store
    with open(metrics_dir / f"{job_name}.jsonl", 'a') as f:
        f.write(json.dumps(record, default=str) + '\n')

    print(f"      [TOOL] Stored to execution_history and spark_metrics")
    return {'stored': True, 'job_name': job_name, 'files': [str(history_dir / f"{job_name}.jsonl"), str(metrics_dir / f"{job_name}.jsonl")]}


# =============================================================================
# DASHBOARD TOOLS
# =============================================================================

@tool
def generate_cloudwatch_dashboard(job_name: str, region: str = 'us-west-2') -> Dict[str, Any]:
    """Create or update a CloudWatch dashboard for an ETL job with Spark metrics widgets.

    Args:
        job_name: Glue job name (used as dashboard name)
        region: AWS region

    Returns:
        Dict with dashboard URL and widget summary
    """
    print(f"      [TOOL] generate_cloudwatch_dashboard: {job_name}")

    dashboard_name = f"ETL-{job_name.replace('_', '-').replace(' ', '-')}"

    def _metric(name, label, stat='Maximum', color='#1f77b4'):
        return {
            'type': 'metric',
            'properties': {
                'metrics': [['Glue', name, 'JobName', job_name, 'Type', 'gauge']],
                'view': 'timeSeries', 'stat': stat, 'period': 300,
                'title': label, 'region': region,
                'annotations': {'horizontal': []},
            },
        }

    widgets = [
        # Row 1: Data Volume
        {**_metric('glue.driver.aggregate.bytesRead',    'Data Read (bytes)'),    'x': 0,  'y': 0,  'width': 8, 'height': 6},
        {**_metric('glue.driver.aggregate.bytesWritten', 'Data Written (bytes)'), 'x': 8,  'y': 0,  'width': 8, 'height': 6},
        {**_metric('glue.driver.aggregate.recordsRead',  'Records Read'),         'x': 16, 'y': 0,  'width': 8, 'height': 6},
        # Row 2: Shuffle / Data Movement
        {**_metric('glue.driver.aggregate.shuffleLocalBytesRead',  'Shuffle Local (bytes)'),  'x': 0,  'y': 6,  'width': 8, 'height': 6},
        {**_metric('glue.driver.aggregate.shuffleRemoteBytesRead', 'Shuffle Remote (bytes) = Data Movement', 'Maximum', '#d62728'), 'x': 8, 'y': 6, 'width': 8, 'height': 6},
        # Row 3: Memory
        {**_metric('glue.driver.jvm.heap.usage',  'Driver Heap Usage %',    'Maximum', '#ff7f0e'), 'x': 0,  'y': 12, 'width': 8, 'height': 6},
        {**_metric('glue.ALL.jvm.heap.usage',     'Executor Heap Usage % (avg)', 'Average', '#2ca02c'), 'x': 8, 'y': 12, 'width': 8, 'height': 6},
        {**_metric('glue.driver.jvm.heap.used',   'Driver Heap Used (bytes)', 'Maximum', '#9467bd'), 'x': 16, 'y': 12, 'width': 8, 'height': 6},
        # Row 4: Executor Utilization
        {**_metric('glue.driver.ExecutorAllocationManager.executors.numberAllExecutors',       'Executors Allocated'), 'x': 0,  'y': 18, 'width': 8, 'height': 6},
        {**_metric('glue.driver.ExecutorAllocationManager.executors.numberMaxNeededExecutors', 'Executors Needed'),    'x': 8,  'y': 18, 'width': 8, 'height': 6},
        # Row 5: Tasks
        {**_metric('glue.driver.aggregate.numCompletedTasks', 'Tasks Completed'), 'x': 0,  'y': 24, 'width': 8, 'height': 6},
        {**_metric('glue.driver.aggregate.numFailedTasks',    'Tasks Failed', 'Maximum', '#d62728'), 'x': 8, 'y': 24, 'width': 8, 'height': 6},
    ]

    # Embed x/y into each widget properties
    for i, w in enumerate(widgets):
        w.setdefault('x', (i % 3) * 8)
        w.setdefault('y', (i // 3) * 6)
        w.setdefault('width', 8)
        w.setdefault('height', 6)

    dashboard_body = json.dumps({'widgets': widgets})
    dashboard_url  = f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}#dashboards:name={dashboard_name}"

    if HAS_AWS:
        try:
            cw = boto3.client('cloudwatch', region_name=region)
            cw.put_dashboard(DashboardName=dashboard_name, DashboardBody=dashboard_body)
            print(f"      [TOOL] Dashboard created/updated: {dashboard_name}")
            return {'created': True, 'dashboard_name': dashboard_name, 'url': dashboard_url, 'widgets': len(widgets)}
        except Exception as e:
            print(f"      [TOOL] ERROR: {e}")
            return {'created': False, 'error': str(e), 'dashboard_name': dashboard_name, 'body': dashboard_body}

    # No AWS: return body for manual creation
    local = Path(f'reports/{dashboard_name}.json')
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_text(dashboard_body)
    print(f"      [TOOL] Saved dashboard JSON to {local}")
    return {'created': False, 'local_file': str(local), 'dashboard_name': dashboard_name, 'url': dashboard_url}


# =============================================================================
# AGENT DEFINITIONS
# =============================================================================

SIZING_PROMPT = """You are a Data Sizing Agent. Your job is to determine the actual data sizes for ETL jobs.

For each source table:
1. If an S3 location is provided, use scan_s3_location to get actual size
2. If database/table provided, use get_glue_table_info to get metadata
3. Calculate total data volume and recommend appropriate resources

Always report what you found and any errors encountered."""


CODE_ANALYSIS_PROMPT = """You are a Code Analysis Agent. Your job is to analyze PySpark/Python ETL code.

When given a script path:
1. Use read_file to get the code
2. Analyze for anti-patterns:
   - collect() calls (causes OOM)
   - toPandas() (memory intensive)
   - crossJoin() (expensive)
   - UDFs without type hints (slow)
   - repartition() without partition key (full shuffle)
3. Identify optimization opportunities
4. Provide LINE-BY-LINE recommendations with specific line numbers

Format findings as:
- Line X: [ISSUE] description - [FIX] recommendation"""


COMPLIANCE_PROMPT = """You are an advanced Compliance Agent responsible for security, data privacy, and regulatory compliance of ETL pipelines.

## Your Responsibilities

### A. PII / Sensitive Data Scanning
For each source and target table:
1. Use scan_columns_for_pii to detect sensitive columns by name patterns
2. Use derive_sensitive_data_via_query to confirm PII by sampling actual values with regex
3. Classify findings under: GDPR, HIPAA, PCI-DSS, or general PII

### B. Encryption Compliance
For each table:
1. Use check_data_encryption_compliance to verify:
   - S3 bucket encryption (SSE-KMS or SSE-AES256)
   - Whether Customer Managed Key (CMK) is used (required for HIPAA/PCI)
   - Glue Data Catalog encryption at rest

### C. Configuration Compliance
Review the job config for:
- Timeout set (prevent runaway jobs)
- Worker count within allowed limits
- VPC config (required for sensitive data environments)
- Glue job security configuration (encryption mode)
- IAM role least-privilege
- CloudWatch logging enabled

### D. Regulatory Risk Summary
After scanning, produce a table:

| Regulation | Triggered | Columns Affected | Risk Level | Required Action |
|------------|-----------|-----------------|------------|-----------------|
| GDPR       | Yes/No    | col1, col2      | HIGH/MED   | Pseudonymize    |
| HIPAA      | Yes/No    | col3            | CRITICAL   | Encrypt + RBAC  |
| PCI-DSS    | Yes/No    | col4            | HIGH       | Mask card data  |

Always conclude with:
- COMPLIANCE VERDICT: COMPLIANT / NON-COMPLIANT / NEEDS-REVIEW
- Top 3 remediation actions ranked by risk"""


LEARNING_PROMPT = """You are an advanced Learning Agent that deeply analyzes Glue job execution history to surface patterns, anomalies, and optimization opportunities.

## Your Analysis Steps

### Step 1: Full Run History
Use get_glue_job_run_history to retrieve all recent runs with:
- Per-run duration, cost, worker config, success/failure
- Trend direction (INCREASING / STABLE / DECREASING)
- Anomaly runs (>2x average duration)
- Best performing run config

### Step 2: Deep Metrics for Latest Run
Use get_glue_run_metrics with the latest run_id to get CloudWatch Spark metrics:
- **Data processed**: bytes_read, bytes_written, records_read
- **Shuffle**: shuffle_local_bytes + shuffle_remote_bytes (high shuffle = skew risk)
- **Driver memory**: driver_heap_usage_pct (>85% = OOM risk, check for collect())
- **Executor memory**: executor_heap_usage_avg (>80% = need bigger workers)
- **Executor allocation**: max executors needed vs allocated (over/under-provisioning)
- **Skew indicators**: shuffle_to_read_ratio > 5x = skew; lopsided executor heap

### Step 3: Local Execution History
Use load_execution_history to get stored simulation/execution records and merge with Glue data.

### Step 4: Synthesize Learnings
Produce a structured report:

**PERFORMANCE TRENDS**
- Duration: {trend} by {pct}% over last N runs
- Cost: avg ${avg}/run, total ${total}/month

**RESOURCE UTILIZATION**
- Data read: X GB, written: Y GB, shuffle: Z GB
- Driver heap peak: X% | Executor heap avg: Y%
- Workers allocated: N, max needed: M (over/under by X)

**SKEW ANALYSIS**
- Shuffle ratio: Xx (threshold: 5x)
- Skew flags detected: [list]

**SPARK CONFIG OBSERVED**
- List actual spark configs from the run

**LEARNED OPTIMAL CONFIG**
- Recommended workers, worker type, spark configs for next run
- Based on best run and current trends"""


DATA_QUALITY_PROMPT = """You are the Data Quality Agent. You validate data against quality rules.

Your responsibilities:
1. Run PRE-LOAD validations on source tables
2. Run POST-LOAD validations on target tables
3. Check for:
   - NULL values in required columns (not_null)
   - Duplicate keys (unique)
   - Negative values where positives expected (positive)
   - Row count thresholds (row_count)
   - Data completeness (completeness)

Use validate_data_quality_rule for each rule, or run_athena_query for custom SQL.

Report:
- Total records scanned
- Outliers/failures found
- Pass/Fail status for each rule
- Overall data quality score"""


RESOURCE_ALLOCATOR_PROMPT = """You are the Resource Allocator Agent. Your job is to determine the optimal compute resources for a Glue ETL job based on ALL available signals.

## Input Signals (use ALL of them)

### 1. Sizing Agent Output  → total data GB, file counts, S3 locations
### 2. Code Analysis Output → anti-pattern count, join complexity, UDF usage
### 3. Learning Agent Output → historical avg duration, avg workers, success rate
### 4. Compliance Output    → encryption requirements (encrypted jobs may need more overhead)
### 5. Time Context         → use get_weekday_volume_pattern + datetime.now()

## Your Steps

**Step 1:** Call get_weekday_volume_pattern to understand if today is a high/low volume day.

**Step 2:** Call compute_resource_recommendation with:
- total_data_gb from Sizing Agent (estimate from text if not exact)
- num_tables = count of source_tables in config
- has_skew = True if Learning/Sizing detected skew
- has_complex_joins = True if Code Analysis found missing broadcasts or cross-joins
- code_issues = count of anti-patterns from Code Analysis
- historical_avg_duration_sec, historical_avg_workers, historical_success_rate from Learning
- day_of_week and hour_of_day from current time
- worker_type from current config

**Step 3:** Write audit record using write_audit_record with event_type='RESOURCE_ALLOCATED'.

**Step 4:** Output a structured RESOURCE ALLOCATION DECISION:

```
RESOURCE ALLOCATION DECISION
==============================
Recommended Workers:    X  (current: Y)
Recommended Type:       G.NX  (current: G.MX)
Estimated Duration:     ~Z min
Estimated Cost:         $W

SPARK CONFIGS TO APPLY:
  spark.sql.adaptive.enabled = true
  spark.sql.shuffle.partitions = N
  ...

REASONING:
  1. Data volume: X GB → base N workers
  2. Skew detected → +40%
  3. Weekend/off-peak → -15%
  4. Historical: avg Y workers, Z min → blended
  5. Code issues (N anti-patterns) → +10% buffer

PASS TO EXECUTION AGENT:
  workers=X, worker_type=G.NX, spark_configs={...}
```"""


EXECUTION_PROMPT = """You are the Execution Agent. You receive a resource allocation decision from the Resource Allocator Agent and execute the Glue job with those exact parameters.

## Your Steps

### Phase 1: PRE-EXECUTION
1. Write audit record: write_audit_record(event_type='JOB_START', details={workers, worker_type, spark_configs, reason})
2. Start Glue job: start_glue_job(job_name, workers=allocated_workers, worker_type=allocated_type, dry_run=False/True)

### Phase 2: MONITORING (if executing)
3. Poll get_job_run_status every 30s until SUCCEEDED/FAILED
4. Log interim status to audit: write_audit_record(event_type='JOB_PROGRESS', details={status, elapsed_sec})

### Phase 3: POST-EXECUTION METRICS (CRITICAL)
After job completes, collect ALL Spark metrics for the Learning Agent:
5. Call collect_post_execution_metrics(job_name, run_id) — gets:
   - Data read/written GB, records processed
   - Shuffle local + remote GB (= data movement between nodes)
   - Driver heap % and GB used
   - Executor heap % avg
   - Executors allocated vs needed (idle detection)
   - Task failure rate
   - Skew flags (shuffle ratio, OOM indicators)

### Phase 4: PERSIST FOR LEARNING
6. Call store_metrics_for_learning(job_name, metrics) to persist ALL metrics
7. Write completion audit: write_audit_record(event_type='JOB_COMPLETE', details=metrics)
8. Generate CloudWatch dashboard: generate_cloudwatch_dashboard(job_name)

### Phase 5: REPORT
Output a full execution summary:
```
EXECUTION SUMMARY
==================
Status:          SUCCEEDED / FAILED
Duration:        Xm Ys
Cost:            $Z

DATA PROCESSED:
  Read:          X GB | Y records
  Written:       Z GB
  Shuffle Local: A GB  (within node)
  Shuffle Remote:B GB  (between nodes = network cost)

MEMORY:
  Driver Heap:   X% (Y GB)
  Executor Heap: Z% avg

EXECUTOR UTILIZATION:
  Allocated: N | Needed: M | Idle: K (K/N = X%)

SKEW / HEALTH FLAGS:
  [list any flags]

→ Metrics stored for Learning Agent ✓
→ Audit record written ✓
→ Dashboard updated ✓
```"""


RECOMMENDATION_PROMPT = """You are the Advanced Recommendation Agent. Perform deep multi-dimensional analysis and synthesize all findings into comprehensive, actionable recommendations.

## Your Analysis Framework

### 1. Data Volume & Executor Sizing Analysis
Use analyze_executor_sizing to determine:
- Per-executor data load (input GB / (workers-1))
- Memory headroom vs shuffle spill risk
- Optimal worker count based on data size and table width
- Whether current worker type (G.1X/G.2X/G.4X) is right-sized

### 2. Table Width & Schema Analysis
Use analyze_table_schema for each source table:
- Wide tables (>50 cols) need columnar pruning early in pipeline
- Complex types (array/map/struct) increase serialization cost
- Estimate row width in bytes to validate partition sizes
- Flag schema issues that slow down joins

### 3. Data Skew Detection
Use detect_data_skew on partition/join keys:
- Identify hot partitions (>30% of data in one value)
- Calculate skew ratio (max_partition / min_partition)
- Recommend salting, AQE skew join hints, or pre-aggregation

### 4. Incremental Processing (Timestamp Analysis)
Use analyze_s3_object_timestamps for each S3 source:
- Detect FULL_LOAD vs INCREMENTAL vs APPEND_HEAVY patterns
- Calculate what % of data changed recently
- Recommend CDC / Glue Bookmarks / Iceberg snapshots to process only deltas

### 5. Dynamic Spark Config Generation
Use get_spark_config_recommendations to generate tuned configs:
- AQE settings calibrated to data size and skew
- Shuffle partition count = f(workers, data_gb)
- Broadcast threshold tuning
- Memory fraction and off-heap for wide/complex tables
- I/O compression and S3 multipart settings

### 6. Alternative Tools & Platforms
Use get_alternative_tools_analysis to identify:
- AWS Native: EMR Serverless, Athena CTAS, Iceberg, Lambda
- Cross-Platform: Databricks Photon, Snowpark, GCP Dataproc, Azure Synapse
- Innovative patterns: Bloom filters, Z-order, Dynamic partition pruning, Graviton3
- Calculate monthly cost savings for each alternative

### 7. Synthesize All Agent Findings
After using all tools, produce a PRIORITIZED report:

**CRITICAL** (data loss / job failure risk):
- Data quality failures on critical columns
- OOM-prone code patterns (collect(), toPandas())
- Skew causing executor failures

**HIGH** (>20% cost or performance impact):
- Wrong executor sizing
- Missing incremental processing (processing full data when 95% unchanged)
- Missing broadcasts on small tables
- No AQE enabled

**MEDIUM** (5-20% impact):
- Suboptimal Spark configs
- Wide-table column pruning opportunities
- Compression codec improvements

**LOW / INNOVATIVE** (future-proofing):
- Platform migration opportunities
- Iceberg/Delta Lake adoption
- Bloom filters, Z-ordering

Always include:
- SPARK CONFIG BLOCK: exact configs to copy-paste
- PLATFORM COMPARISON TABLE: cost vs current Glue
- IMPLEMENTATION ROADMAP: Week 1 / Month 1 / Quarter 1
- ESTIMATED SAVINGS: $ and % for top 3 recommendations"""


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================

class StrandsETLOrchestrator:
    """Orchestrates all Strands agents for ETL analysis."""

    def __init__(self, model: str = None, region: str = 'us-west-2'):
        self.model = model or 'us.anthropic.claude-sonnet-4-6-20250514'
        self.region = region
        self.tracker = TokenTracker()
        self.results = {}

        # Set region
        os.environ['AWS_DEFAULT_REGION'] = region
        os.environ['AWS_REGION'] = region

        print(f"\n{'='*70}")
        print(f"  STRANDS ETL AGENT ORCHESTRATOR")
        print(f"  Model: {self.model}")
        print(f"  Region: {self.region}")
        print(f"{'='*70}")

    def analyze(self, config: Dict, execute: bool = False) -> Dict[str, Any]:
        """Run full 9-agent pipeline on a config.

        Pipeline order:
          1. Sizing         → data volumes
          2. Code Analysis  → anti-patterns
          3. Data Quality   → DQ rule validation
          4. Compliance     → PII/GDPR/HIPAA/encryption
          5. Learning       → historical patterns + Spark metrics
          6. Resource Alloc → compute resources based on all above
          7. Execution      → runs job + collects post-execution metrics
          8. Dashboard      → CloudWatch dashboard generation
          9. Recommendation → final prioritized recommendations

        Args:
            config: Job configuration dict
            execute: If True, actually start the Glue job (requires confirmation)

        Config agent flags (all default to True):
            agents.sizing: true/false
            agents.code_analysis: true/false
            agents.data_quality: true/false
            agents.compliance: true/false
            agents.learning: true/false
            agents.resource_allocator: true/false
            agents.execution: true/false
            agents.dashboard: true/false
            agents.recommendation: true/false
        """
        job_name = config.get('job_name', 'unknown')
        agents_cfg = config.get('agents', {})

        # Helper to check if agent is enabled
        def is_enabled(agent_name: str) -> bool:
            return agents_cfg.get(agent_name, True)

        print(f"\n  Job: {job_name}")
        print(f"  Mode: {'EXECUTE' if execute else 'ANALYZE ONLY'}")
        print(f"  Audit Table: {config.get('audit', {}).get('dynamodb_table', 'etl_audit_log')}")
        print(f"  {'─'*60}")
        print(f"  AGENTS ENABLED:")
        agent_list = ['sizing', 'code_analysis', 'data_quality', 'compliance', 'learning',
                      'resource_allocator', 'execution', 'dashboard', 'recommendation']
        for a in agent_list:
            status = '✓ ON' if is_enabled(a) else '✗ OFF'
            print(f"    {a:20s} {status}")
        print(f"  {'─'*60}")

        # 1. Sizing Agent
        if is_enabled('sizing'):
            print(f"\n  [1/9] SIZING AGENT")
            self.results['sizing'] = self._run_sizing_agent(config)
        else:
            print(f"\n  [1/9] SIZING AGENT  [SKIPPED]")
            self.results['sizing'] = {'analysis': 'Skipped (agents.sizing=false)'}

        # 2. Code Analysis Agent
        if is_enabled('code_analysis') and config.get('script_path'):
            print(f"\n  [2/9] CODE ANALYSIS AGENT")
            self.results['code_analysis'] = self._run_code_agent(config)
        else:
            print(f"\n  [2/9] CODE ANALYSIS AGENT  [SKIPPED]")
            self.results['code_analysis'] = {'analysis': 'Skipped (agents.code_analysis=false or no script_path)'}

        # 3. Data Quality Agent
        if is_enabled('data_quality'):
            print(f"\n  [3/9] DATA QUALITY AGENT")
            self.results['data_quality'] = self._run_data_quality_agent(config)
        else:
            print(f"\n  [3/9] DATA QUALITY AGENT  [SKIPPED]")
            self.results['data_quality'] = {'analysis': 'Skipped (agents.data_quality=false)'}

        # 4. Compliance Agent (PII/GDPR/HIPAA)
        if is_enabled('compliance'):
            print(f"\n  [4/9] COMPLIANCE AGENT")
            self.results['compliance'] = self._run_compliance_agent(config)
        else:
            print(f"\n  [4/9] COMPLIANCE AGENT  [SKIPPED]")
            self.results['compliance'] = {'analysis': 'Skipped (agents.compliance=false)'}

        # 5. Learning Agent (historical patterns)
        if is_enabled('learning'):
            print(f"\n  [5/9] LEARNING AGENT")
            self.results['learning'] = self._run_learning_agent(config)
        else:
            print(f"\n  [5/9] LEARNING AGENT  [SKIPPED]")
            self.results['learning'] = {'analysis': 'Skipped (agents.learning=false)'}

        # 6. Resource Allocator Agent
        if is_enabled('resource_allocator'):
            print(f"\n  [6/9] RESOURCE ALLOCATOR AGENT")
            self.results['resource_allocation'] = self._run_resource_allocator_agent(config)
        else:
            print(f"\n  [6/9] RESOURCE ALLOCATOR AGENT  [SKIPPED]")
            self.results['resource_allocation'] = {'analysis': 'Skipped (agents.resource_allocator=false)'}

        # 7. Execution Agent
        if is_enabled('execution'):
            print(f"\n  [7/9] EXECUTION AGENT")
            self.results['execution'] = self._run_execution_agent(config, execute)
        else:
            print(f"\n  [7/9] EXECUTION AGENT  [SKIPPED]")
            self.results['execution'] = {'analysis': 'Skipped (agents.execution=false)'}

        # 8. Dashboard
        if is_enabled('dashboard'):
            print(f"\n  [8/9] GENERATING CLOUDWATCH DASHBOARD")
            self.results['dashboard'] = self._generate_dashboard(config)
        else:
            print(f"\n  [8/9] DASHBOARD  [SKIPPED]")
            self.results['dashboard'] = {'created': False, 'reason': 'Skipped (agents.dashboard=false)'}

        # 9. Recommendation Agent
        if is_enabled('recommendation'):
            print(f"\n  [9/9] RECOMMENDATION AGENT")
            self.results['recommendations'] = self._run_recommendation_agent(config)
        else:
            print(f"\n  [9/9] RECOMMENDATION AGENT  [SKIPPED]")
            self.results['recommendations'] = {'analysis': 'Skipped (agents.recommendation=false)'}

        # Store this run's recommendations for history
        self._store_recommendation_history(job_name, self.results.get('recommendations', {}))

        return {
            'job_name': job_name,
            'config': config,
            'agents': self.results,
            'token_usage': self.tracker.summary()
        }

    def _store_recommendation_history(self, job_name: str, recommendations: Dict):
        """Store recommendations to history file for CLI retrieval."""
        history_dir = Path('data/recommendation_history')
        history_dir.mkdir(parents=True, exist_ok=True)
        record = {
            'timestamp': datetime.utcnow().isoformat(),
            'job_name': job_name,
            'recommendations': recommendations.get('analysis', str(recommendations)),
        }
        with open(history_dir / f"{job_name}.jsonl", 'a') as f:
            f.write(json.dumps(record, default=str) + '\n')

    def _run_agent(self, name: str, system_prompt: str, user_prompt: str, tools: list) -> str:
        """Run a single agent and track metrics."""
        start = time.time()

        # Print agent context
        print(f"\n    {'─'*55}")
        print(f"    AGENT CONTEXT: {name.upper()}")
        print(f"    {'─'*55}")
        print(f"    Model: {self.model}")
        print(f"    Tools: {[t.__name__ for t in tools]}")
        print(f"    System Prompt: {len(system_prompt)} chars")
        print(f"    {'─'*55}")
        print(f"    SYSTEM PROMPT:")
        print(f"    {'─'*55}")
        for line in system_prompt.split('\n')[:10]:  # First 10 lines
            print(f"    {line[:80]}")
        if system_prompt.count('\n') > 10:
            print(f"    ... ({system_prompt.count(chr(10)) - 10} more lines)")
        print(f"    {'─'*55}")
        print(f"    USER PROMPT:")
        print(f"    {'─'*55}")
        for line in user_prompt.split('\n')[:15]:  # First 15 lines
            print(f"    {line[:80]}")
        if user_prompt.count('\n') > 15:
            print(f"    ... ({user_prompt.count(chr(10)) - 15} more lines)")
        print(f"    {'─'*55}")
        print(f"    EXECUTING AGENT...")
        print(f"    {'─'*55}")

        try:
            agent = Agent(
                model=self.model,
                system_prompt=system_prompt,
                tools=tools
            )

            response = agent(user_prompt)
            duration = time.time() - start

            # Extract tokens from Strands response - try multiple paths
            input_tok = 0
            output_tok = 0

            # Method 1: response.metrics object
            if hasattr(response, 'metrics') and response.metrics:
                metrics = response.metrics
                input_tok = getattr(metrics, 'input_tokens', 0) or getattr(metrics, 'inputTokens', 0) or 0
                output_tok = getattr(metrics, 'output_tokens', 0) or getattr(metrics, 'outputTokens', 0) or 0

            # Method 2: response.usage dict (common pattern)
            if input_tok == 0 and hasattr(response, 'usage') and response.usage:
                usage = response.usage
                if isinstance(usage, dict):
                    input_tok = usage.get('input_tokens', usage.get('prompt_tokens', 0))
                    output_tok = usage.get('output_tokens', usage.get('completion_tokens', 0))

            # Method 3: response._raw or response.raw_response
            if input_tok == 0:
                raw = getattr(response, '_raw', None) or getattr(response, 'raw_response', None)
                if raw and isinstance(raw, dict):
                    usage = raw.get('usage', {})
                    input_tok = usage.get('input_tokens', usage.get('prompt_tokens', 0))
                    output_tok = usage.get('output_tokens', usage.get('completion_tokens', 0))

            # Method 4: For Bedrock responses, check message structure
            if input_tok == 0 and hasattr(response, 'message'):
                msg = response.message
                if hasattr(msg, 'usage'):
                    input_tok = getattr(msg.usage, 'input_tokens', 0)
                    output_tok = getattr(msg.usage, 'output_tokens', 0)

            # Method 5: Estimate from content length if all else fails
            if input_tok == 0:
                # Rough estimate: 1 token ≈ 4 chars
                input_tok = (len(system_prompt) + len(user_prompt)) // 4
                output_tok = len(str(response)) // 4
                print(f"    [TOKEN ESTIMATE - actual metrics unavailable]")

            # Print response summary
            print(f"    {'─'*55}")
            print(f"    AGENT RESPONSE:")
            print(f"    {'─'*55}")
            response_str = str(response)
            for line in response_str.split('\n')[:20]:  # First 20 lines
                print(f"    {line[:80]}")
            if response_str.count('\n') > 20:
                print(f"    ... ({response_str.count(chr(10)) - 20} more lines)")
            print(f"    {'─'*55}")

            self.tracker.log_agent(name, input_tok, output_tok, duration)

            return response_str

        except Exception as e:
            duration = time.time() - start
            print(f"    {'─'*55}")
            print(f"    ERROR: {e}")
            print(f"    {'─'*55}")
            import traceback
            traceback.print_exc()
            # Log minimal tokens for error case
            self.tracker.log_agent(name, len(user_prompt) // 4, 0, duration)
            return f"Agent error: {e}"

    def _run_sizing_agent(self, config: Dict) -> Dict:
        tables = config.get('source_tables', [])
        prompt = f"Analyze data sizes for job '{config.get('job_name')}':\n\n"

        for t in tables:
            prompt += f"Table: {t.get('database', 'db')}.{t.get('table', t.get('name', ''))}\n"
            if t.get('location') or t.get('s3_path'):
                prompt += f"  S3: {t.get('location') or t.get('s3_path')}\n"
            if t.get('estimated_rows'):
                prompt += f"  Est rows: {t.get('estimated_rows'):,}\n"

        prompt += "\nScan each S3 location and Glue table to determine actual sizes."

        result = self._run_agent('sizing', SIZING_PROMPT, prompt, [scan_s3_location, get_glue_table_info])
        return {'analysis': result}

    def _run_code_agent(self, config: Dict) -> Dict:
        script = config.get('script_path', '')
        if not script:
            return {'analysis': 'No script_path provided in config'}

        prompt = f"Analyze the ETL script at: {script}\n\nProvide line-by-line code review with specific improvements."

        result = self._run_agent('code_analysis', CODE_ANALYSIS_PROMPT, prompt, [read_file])
        return {'script_path': script, 'analysis': result}

    def _run_data_quality_agent(self, config: Dict) -> Dict:
        tables = config.get('source_tables', [])
        dq_rules = config.get('data_quality', {}).get('rules', [])

        prompt = f"""Validate data quality for job '{config.get('job_name')}':

Source Tables:
"""
        for t in tables:
            db = t.get('database', 'default')
            tbl = t.get('table', t.get('name', ''))
            prompt += f"  - {db}.{tbl}\n"

        if dq_rules:
            prompt += f"\nConfigured Rules:\n"
            for rule in dq_rules[:5]:  # Limit to 5 rules
                prompt += f"  - {rule}\n"
        else:
            prompt += """
Default validations to run:
1. Check NOT NULL on primary key columns
2. Check row count > 0
3. Check for duplicates on key columns
4. Check data completeness (>95%)
"""

        prompt += "\nUse validate_data_quality_rule for each check. Report pass/fail status and outlier counts."

        result = self._run_agent('data_quality', DATA_QUALITY_PROMPT, prompt,
                                  [validate_data_quality_rule, run_athena_query])
        return {'tables_checked': len(tables), 'analysis': result}

    def _run_compliance_agent(self, config: Dict) -> Dict:
        current = config.get('current_config', {})
        source_tables = config.get('source_tables', [])
        compliance_cfg = config.get('compliance', {})

        table_list = '\n'.join(
            f"  - {t.get('database','default')}.{t.get('table', t.get('name',''))} "
            f"(location: {t.get('location','')})"
            for t in source_tables
        )

        prompt = f"""Run full compliance analysis for job '{config.get('job_name')}'.

## JOB CONFIG
  Workers:      {current.get('NumberOfWorkers', current.get('workers', 'unknown'))}
  Worker Type:  {current.get('WorkerType', 'unknown')}
  Timeout:      {current.get('Timeout', 'not set')} minutes
  Encryption:   {compliance_cfg.get('require_encryption', 'not specified')}
  VPC Required: {compliance_cfg.get('require_vpc', 'not specified')}
  Max Workers:  {compliance_cfg.get('max_workers', 'not specified')}

## SOURCE TABLES TO SCAN
{table_list if table_list else '  (none specified)'}

## STEPS
1. For each source table above, call scan_columns_for_pii to detect sensitive columns by name
2. For each source table, call derive_sensitive_data_via_query to validate with actual data samples
3. For each source table, call check_data_encryption_compliance to verify S3/Catalog encryption
4. Review job config for timeout, worker limits, VPC, logging
5. Produce the REGULATORY RISK TABLE and COMPLIANCE VERDICT"""

        tools = [scan_columns_for_pii, derive_sensitive_data_via_query, check_data_encryption_compliance]
        result = self._run_agent('compliance', COMPLIANCE_PROMPT, prompt, tools)
        return {'tables_scanned': len(source_tables), 'analysis': result}

    def _run_learning_agent(self, config: Dict) -> Dict:
        glue_job = config.get('glue_job_name', config.get('job_name', ''))

        prompt = f"""Perform deep learning analysis for Glue job: '{glue_job}'

## STEP 1 - FULL RUN HISTORY
Call get_glue_job_run_history(job_name='{glue_job}', max_runs=20)
This returns all runs with duration, cost, worker config, trends, anomalies, and latest CloudWatch metrics.

## STEP 2 - DETAILED SPARK METRICS
From the run history result, extract the latest successful run_id.
Call get_glue_run_metrics(job_name='{glue_job}', run_id=<latest_run_id>)
This fetches CloudWatch Spark metrics: bytes read/written, shuffle bytes, driver heap %, executor heap %, executor allocation.

## STEP 3 - LOCAL HISTORY
Call load_execution_history(job_name='{glue_job}', limit=20) to get local simulation records.

## STEP 4 - SYNTHESIZE
After calling all three tools, produce a complete LEARNING REPORT covering:
- Performance trends (duration, cost trajectory)
- Resource utilization (data volumes, memory pressure)
- Skew analysis (shuffle ratio, hot executors)
- Observed Spark configs
- Recommended optimal config for next run"""

        tools = [get_glue_job_run_history, get_glue_run_metrics, get_glue_job_runs, load_execution_history]
        result = self._run_agent('learning', LEARNING_PROMPT, prompt, tools)
        return {'glue_job': glue_job, 'analysis': result}

    def _run_resource_allocator_agent(self, config: Dict) -> Dict:
        glue_job    = config.get('glue_job_name', config.get('job_name', ''))
        current     = config.get('current_config', {})
        workers     = current.get('NumberOfWorkers', current.get('workers', 10))
        worker_type = current.get('WorkerType', 'G.1X')
        audit_table = config.get('audit', {}).get('dynamodb_table', 'etl_audit_log')

        sizing_text   = self.results.get('sizing', {}).get('analysis', 'N/A')
        code_text     = self.results.get('code_analysis', {}).get('analysis', 'N/A')
        learning_text = self.results.get('learning', {}).get('analysis', 'N/A')
        tables        = config.get('source_tables', [])

        now = datetime.utcnow()
        prompt = f"""Determine optimal resource allocation for Glue job '{glue_job}'.

## CURRENT CONFIG
  Workers:     {workers} x {worker_type}
  Tables:      {len(tables)} source tables
  Audit Table: {audit_table}

## SIZING AGENT OUTPUT
{sizing_text[:500]}

## CODE ANALYSIS OUTPUT
{code_text[:500]}

## LEARNING AGENT OUTPUT
{learning_text[:500]}

## CURRENT TIME CONTEXT
  UTC Time:    {now.strftime('%Y-%m-%d %H:%M')}
  Day:         {now.strftime('%A')}
  Hour:        {now.hour}

## STEPS
1. Call get_weekday_volume_pattern(job_name='{glue_job}') to get day-of-week patterns
2. Extract from the agent outputs above:
   - total_data_gb (estimate 10.0 if unknown)
   - code_issues count (count 'ANTI-PATTERN' or 'LINE' mentions, default 0)
   - historical_avg_duration_sec, historical_avg_workers, historical_success_rate (default 0 if no history)
   - has_skew (True if skew mentioned in learning output)
3. Call compute_resource_recommendation with all extracted values, day_of_week='{now.strftime('%A')}', hour_of_day={now.hour}
4. Call write_audit_record(job_name='{glue_job}', run_id='allocation-{now.strftime('%Y%m%d%H%M%S')}', event_type='RESOURCE_ALLOCATED', details=recommendation, table_name='{audit_table}')
5. Output the RESOURCE ALLOCATION DECISION block"""

        tools = [compute_resource_recommendation, get_weekday_volume_pattern, write_audit_record]
        result = self._run_agent('resource_allocator', RESOURCE_ALLOCATOR_PROMPT, prompt, tools)
        return {'glue_job': glue_job, 'analysis': result}

    def _run_execution_agent(self, config: Dict, execute: bool = False) -> Dict:
        glue_job    = config.get('glue_job_name', config.get('job_name', ''))
        current     = config.get('current_config', {})
        workers     = current.get('NumberOfWorkers', current.get('workers', 10))
        worker_type = current.get('WorkerType', 'G.1X')
        audit_table = config.get('audit', {}).get('dynamodb_table', 'etl_audit_log')

        # Pull resource allocation decision if available
        alloc_text = self.results.get('resource_allocation', {}).get('analysis', '')
        alloc_note = f"\n## RESOURCE ALLOCATION DECISION\n{alloc_text[:600]}\nUse the workers/worker_type from the above decision if available.\n" if alloc_text else ''

        mode = 'EXECUTE' if execute else 'DRY_RUN'
        prompt = f"""{'Execute' if execute else 'Simulate execution of'} Glue job '{glue_job}'.
{alloc_note}
## PARAMETERS
  Workers:     {workers} x {worker_type}
  Audit Table: {audit_table}
  Mode:        {mode}

## STEPS

**Phase 1 - Start:**
1. Write audit: write_audit_record(job_name='{glue_job}', run_id='pre-run', event_type='JOB_START', details={{'workers':{workers},'worker_type':'{worker_type}'}}, table_name='{audit_table}')
2. Start job:   start_glue_job(job_name='{glue_job}', workers={workers}, worker_type='{worker_type}', dry_run={'False' if execute else 'True'})

**Phase 2 - Monitor (if executing):**
3. If not dry_run, poll get_job_run_status every 30s until terminal state

**Phase 3 - Post-execution metrics (CRITICAL):**
4. Call collect_post_execution_metrics(job_name='{glue_job}', run_id=<run_id from step 2>)
   This collects: data read/written, shuffle (= data movement between nodes), driver heap %,
   executor heap %, idle executors, task failure rate, skew flags.

**Phase 4 - Persist:**
5. store_metrics_for_learning(job_name='{glue_job}', run_metrics=<metrics from step 4>)
6. write_audit_record(event_type='JOB_COMPLETE', details=<metrics>, table_name='{audit_table}')

**Phase 5 - Output EXECUTION SUMMARY** as specified in your system prompt."""

        tools = [
            start_glue_job, get_job_run_status, store_execution_history,
            collect_post_execution_metrics, store_metrics_for_learning,
            write_audit_record,
        ]
        result = self._run_agent('execution', EXECUTION_PROMPT, prompt, tools)
        return {'glue_job': glue_job, 'executed': execute, 'analysis': result}

    def _generate_dashboard(self, config: Dict) -> Dict:
        glue_job = config.get('glue_job_name', config.get('job_name', ''))
        try:
            return generate_cloudwatch_dashboard(glue_job)
        except Exception as e:
            return {'error': str(e)}

    def _run_recommendation_agent(self, config: Dict) -> Dict:
        current = config.get('current_config', {})
        workers = current.get('NumberOfWorkers', current.get('workers', 10))
        worker_type = current.get('WorkerType', 'G.1X')
        runtime = config.get('avg_runtime_hours', 0.5)
        runs = config.get('monthly_runs', 30)
        source_tables = config.get('source_tables', [])

        # Build table info for schema/skew analysis
        table_list = []
        for t in source_tables:
            db = t.get('database', 'default')
            tbl = t.get('table', t.get('name', ''))
            loc = t.get('location', t.get('s3_path', ''))
            rows_est = t.get('estimated_rows', 0)
            table_list.append(f"  - {db}.{tbl} | location: {loc} | est_rows: {rows_est:,}")

        tables_str = '\n'.join(table_list) if table_list else '  (none specified)'

        # Estimate total data from sizing agent result
        sizing_text = self.results.get('sizing', {}).get('analysis', '')

        prompt = f"""Perform comprehensive deep-dive analysis for job '{config.get('job_name')}' and generate actionable recommendations.

## JOB CONFIGURATION
- Workers: {workers} x {worker_type}
- Avg Runtime: {runtime}h
- Monthly Runs: {runs}
- Script: {config.get('script_path', 'N/A')}
- Glue Job: {config.get('glue_job_name', 'N/A')}

## SOURCE TABLES
{tables_str}

## PREVIOUS AGENT FINDINGS

### SIZING AGENT
{self.results.get('sizing', {}).get('analysis', 'N/A')[:600]}

### CODE ANALYSIS AGENT
{self.results.get('code_analysis', {}).get('analysis', 'N/A')[:600]}

### DATA QUALITY AGENT
{self.results.get('data_quality', {}).get('analysis', 'N/A')[:600]}

### COMPLIANCE AGENT
{self.results.get('compliance', {}).get('analysis', 'N/A')[:400]}

### LEARNING AGENT
{self.results.get('learning', {}).get('analysis', 'N/A')[:400]}

### EXECUTION AGENT
{self.results.get('execution', {}).get('analysis', 'N/A')[:300]}

## YOUR DEEP-DIVE ANALYSIS STEPS

Step 1: Call analyze_executor_sizing with estimated total data size (extract from SIZING results above, default 10.0 if unknown), number of columns from widest table (estimate 30 if unknown), worker_type='{worker_type}', num_workers={workers}

Step 2: For each source table with a database and table name, call analyze_table_schema to get column counts, types, and wide-table warnings.

Step 3: For source tables with S3 locations, call analyze_s3_object_timestamps to detect incremental vs full-load patterns.

Step 4: Call get_spark_config_recommendations using insights from Steps 1-3 to generate tuned Spark configs.

Step 5: Call get_alternative_tools_analysis to identify cheaper/faster alternatives on AWS and other platforms.

Step 6: Call calculate_platform_costs with current config ({workers} workers, {runtime}h, {runs} runs/month).

Step 7: Synthesize ALL findings (from this session AND the 6 steps above) into a comprehensive report with CRITICAL / HIGH / MEDIUM / LOW sections, a copy-paste Spark config block, a platform comparison table, and a phased implementation roadmap.

Be specific: include exact line numbers from code analysis, exact Spark config key=value pairs, exact dollar savings, and specific AWS service names."""

        tools = [
            analyze_executor_sizing,
            analyze_table_schema,
            detect_data_skew,
            analyze_s3_object_timestamps,
            get_spark_config_recommendations,
            get_alternative_tools_analysis,
            calculate_platform_costs,
        ]

        result = self._run_agent('recommendations', RECOMMENDATION_PROMPT, prompt, tools)
        return {'analysis': result}


# =============================================================================
# HISTORY FUNCTIONS
# =============================================================================

def show_learning_history(job_name: str, limit: int = 10):
    """Display learning/execution history for a job."""
    print(f"\n{'='*70}")
    print(f"  LEARNING HISTORY: {job_name}")
    print(f"{'='*70}\n")

    # Execution history
    exec_file = Path(f'data/execution_history/{job_name}.jsonl')
    if exec_file.exists():
        with open(exec_file) as f:
            records = [json.loads(l) for l in f if l.strip()][-limit:]
        print(f"  EXECUTION HISTORY (last {len(records)} runs):")
        print(f"  {'─'*60}")
        for r in records:
            ts = r.get('timestamp', 'N/A')[:19]
            dur = r.get('duration_sec', 0)
            cost = r.get('cost_usd', 0)
            status = r.get('status', 'N/A')
            workers = r.get('num_workers', 'N/A')
            print(f"    {ts} | {status:10s} | {dur:>6.0f}s | ${cost:.4f} | {workers} workers")
        print()
    else:
        print(f"  No execution history found at {exec_file}\n")

    # Spark metrics history
    metrics_file = Path(f'data/spark_metrics/{job_name}.jsonl')
    if metrics_file.exists():
        with open(metrics_file) as f:
            records = [json.loads(l) for l in f if l.strip()][-limit:]
        print(f"  SPARK METRICS HISTORY (last {len(records)} runs):")
        print(f"  {'─'*60}")
        for r in records:
            ts = r.get('timestamp', 'N/A')[:19]
            read_gb = r.get('data_read_gb', 'N/A')
            shuffle_gb = r.get('shuffle_total_gb', 'N/A')
            driver_heap = r.get('driver_heap_pct', 'N/A')
            exec_heap = r.get('executor_heap_pct_avg', 'N/A')
            skew = len(r.get('skew_flags', []))
            print(f"    {ts} | Read:{read_gb}GB | Shuffle:{shuffle_gb}GB | "
                  f"Driver:{driver_heap}% | Exec:{exec_heap}% | Skew flags:{skew}")
        print()
    else:
        print(f"  No spark metrics history found at {metrics_file}\n")


def show_recommendation_history(job_name: str, limit: int = 5):
    """Display past recommendations for a job."""
    print(f"\n{'='*70}")
    print(f"  RECOMMENDATION HISTORY: {job_name}")
    print(f"{'='*70}\n")

    rec_file = Path(f'data/recommendation_history/{job_name}.jsonl')
    if rec_file.exists():
        with open(rec_file) as f:
            records = [json.loads(l) for l in f if l.strip()][-limit:]
        for i, r in enumerate(records, 1):
            ts = r.get('timestamp', 'N/A')[:19]
            print(f"  [{i}] {ts}")
            print(f"  {'─'*60}")
            rec_text = r.get('recommendations', '')
            # Print first 30 lines
            for line in rec_text.split('\n')[:30]:
                print(f"    {line[:100]}")
            if rec_text.count('\n') > 30:
                print(f"    ... ({rec_text.count(chr(10)) - 30} more lines)")
            print()
    else:
        print(f"  No recommendation history found at {rec_file}")
        print(f"  Run an analysis first to generate recommendations.\n")


def show_audit_history(job_name: str = None, limit: int = 20):
    """Display audit log entries."""
    print(f"\n{'='*70}")
    print(f"  AUDIT LOG" + (f" (job: {job_name})" if job_name else " (all jobs)"))
    print(f"{'='*70}\n")

    audit_file = Path('data/audit_log.jsonl')
    if audit_file.exists():
        with open(audit_file) as f:
            records = [json.loads(l) for l in f if l.strip()]
        if job_name:
            records = [r for r in records if r.get('job_name') == job_name]
        records = records[-limit:]

        print(f"  {'TIMESTAMP':<20} {'JOB NAME':<25} {'EVENT TYPE':<20}")
        print(f"  {'─'*65}")
        for r in records:
            ts = r.get('timestamp', 'N/A')[:19]
            job = r.get('job_name', 'N/A')[:24]
            event = r.get('event_type', 'N/A')[:19]
            print(f"  {ts:<20} {job:<25} {event:<20}")
        print()
    else:
        print(f"  No audit log found at {audit_file}\n")


def list_all_jobs():
    """List all jobs with history."""
    print(f"\n{'='*70}")
    print(f"  JOBS WITH HISTORY")
    print(f"{'='*70}\n")

    jobs = set()
    for dir_name in ['data/execution_history', 'data/spark_metrics', 'data/recommendation_history']:
        dir_path = Path(dir_name)
        if dir_path.exists():
            for f in dir_path.glob('*.jsonl'):
                jobs.add(f.stem)

    if jobs:
        print(f"  {'JOB NAME':<40} {'EXEC HISTORY':<15} {'SPARK METRICS':<15} {'RECOMMENDATIONS'}")
        print(f"  {'─'*85}")
        for job in sorted(jobs):
            exec_count = sum(1 for _ in open(f'data/execution_history/{job}.jsonl')) if Path(f'data/execution_history/{job}.jsonl').exists() else 0
            metrics_count = sum(1 for _ in open(f'data/spark_metrics/{job}.jsonl')) if Path(f'data/spark_metrics/{job}.jsonl').exists() else 0
            rec_count = sum(1 for _ in open(f'data/recommendation_history/{job}.jsonl')) if Path(f'data/recommendation_history/{job}.jsonl').exists() else 0
            print(f"  {job:<40} {exec_count:<15} {metrics_count:<15} {rec_count}")
        print()
    else:
        print(f"  No jobs found with history.\n")


# =============================================================================
# CLI
# =============================================================================

def run_single_config(config_path: str, output_dir: str, model: str = None, execute: bool = False):
    """Run analysis on a single config file."""
    print(f"\n  Loading: {config_path}")

    with open(config_path) as f:
        config = json.load(f)

    orchestrator = StrandsETLOrchestrator(model=model)
    result = orchestrator.analyze(config, execute=execute)

    # Save results
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    job_name = config.get('job_name', 'unknown')

    # JSON
    json_path = Path(output_dir) / f"{job_name}_{ts}.json"
    with open(json_path, 'w') as f:
        json.dump(result, f, indent=2, default=str)
    print(f"\n  Saved: {json_path}")

    # Print summary
    usage = result.get('token_usage', {})
    print(f"\n{'='*70}")
    print(f"  COMPLETE")
    print(f"  Total Tokens: {usage.get('total_input_tokens', 0):,} in / {usage.get('total_output_tokens', 0):,} out")
    print(f"  Duration: {usage.get('total_duration_sec', 0):.1f}s")
    print(f"  Est Cost: ${usage.get('estimated_cost_usd', 0):.4f}")
    print(f"{'='*70}\n")

    return result


def run_batch(source_dir: str, output_dir: str, model: str = None, execute: bool = False):
    """Run analysis on all configs in a directory."""
    configs = list(Path(source_dir).glob('*.json'))
    print(f"\n  Found {len(configs)} config files in {source_dir}")

    results = []
    for i, cfg_path in enumerate(configs, 1):
        print(f"\n{'#'*70}")
        print(f"  [{i}/{len(configs)}] {cfg_path.name}")
        print(f"{'#'*70}")

        result = run_single_config(str(cfg_path), output_dir, model, execute)
        results.append(result)

    return results


def main():
    parser = argparse.ArgumentParser(
        description='Strands SDK ETL Agent Orchestrator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze only (default)
  python scripts/strands_etl_agents.py --config demo_configs/sales_etl.json

  # Analyze and execute the Glue job
  python scripts/strands_etl_agents.py --config job.json --execute

  # Batch mode
  python scripts/strands_etl_agents.py --source demo_configs/ --dest reports/

  # View history
  python scripts/strands_etl_agents.py --learning-history my_job_name
  python scripts/strands_etl_agents.py --recommendation-history my_job_name
  python scripts/strands_etl_agents.py --audit-history my_job_name
  python scripts/strands_etl_agents.py --list-jobs

Config agent flags (set in JSON):
  {
    "agents": {
      "sizing": true,
      "code_analysis": true,
      "data_quality": true,
      "compliance": true,
      "learning": true,
      "resource_allocator": true,
      "execution": true,
      "dashboard": true,
      "recommendation": true
    }
  }

Environment:
  AWS_REGION=us-west-2 (default)
  AWS_PROFILE=your-profile
"""
    )
    # Analysis mode
    parser.add_argument('--config', '-c', help='Single config JSON file')
    parser.add_argument('--source', '-s', help='Directory with config JSONs (batch mode)')
    parser.add_argument('--dest', '-d', default='./reports', help='Output directory')
    parser.add_argument('--model', '-m', help='Bedrock model ID')
    parser.add_argument('--execute', '-e', action='store_true', help='Actually execute the Glue job (not just analyze)')

    # History mode
    parser.add_argument('--learning-history', metavar='JOB_NAME', help='Show learning/execution history for a job')
    parser.add_argument('--recommendation-history', metavar='JOB_NAME', help='Show past recommendations for a job')
    parser.add_argument('--audit-history', metavar='JOB_NAME', nargs='?', const='__all__', help='Show audit log (optionally filter by job)')
    parser.add_argument('--list-jobs', action='store_true', help='List all jobs with history')
    parser.add_argument('--limit', type=int, default=10, help='Max records to show in history (default: 10)')

    args = parser.parse_args()

    # History commands
    if args.list_jobs:
        list_all_jobs()
        return

    if args.learning_history:
        show_learning_history(args.learning_history, args.limit)
        return

    if args.recommendation_history:
        show_recommendation_history(args.recommendation_history, args.limit)
        return

    if args.audit_history:
        job_filter = None if args.audit_history == '__all__' else args.audit_history
        show_audit_history(job_filter, args.limit)
        return

    # Analysis mode - need config or source
    if not args.config and not args.source:
        parser.error("Either --config or --source required (or use --list-jobs / --learning-history / --recommendation-history)")

    if args.execute:
        print("\n  WARNING: --execute mode will START ACTUAL GLUE JOBS!")
        confirm = input("  Type 'yes' to confirm: ")
        if confirm.lower() != 'yes':
            print("  Aborted.")
            return

    if args.config:
        run_single_config(args.config, args.dest, args.model, args.execute)
    else:
        run_batch(args.source, args.dest, args.model, args.execute)


if __name__ == '__main__':
    main()
