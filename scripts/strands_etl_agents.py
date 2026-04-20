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


COMPLIANCE_PROMPT = """You are a Compliance Agent. Check ETL configurations for:

1. Security: IAM roles, encryption, VPC settings
2. Cost controls: Timeouts, max workers, auto-scaling
3. Best practices: Logging, monitoring, error handling
4. Data governance: Catalog usage, schema validation

Report any compliance gaps found."""


LEARNING_PROMPT = """You are a Learning Agent. Analyze historical job runs to identify patterns.

Use get_glue_job_runs to fetch recent runs, then:
1. Calculate average runtime and cost
2. Identify failed runs and common errors
3. Detect trends (growing data, increasing runtime)
4. Learn optimal resource configurations

Provide insights based on historical patterns."""


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


EXECUTION_PROMPT = """You are the Execution Agent. You manage ETL job execution and track metrics.

Your responsibilities:
1. Start Glue jobs (use dry_run=True for analysis mode, False for real execution)
2. Monitor job status using get_job_run_status
3. Store execution results for learning using store_execution_history
4. Track costs, duration, and success/failure

When executing:
- Always confirm job parameters before starting
- Store results after completion for future learning
- Report any errors clearly with recommendations"""


RECOMMENDATION_PROMPT = """You are the Recommendation Agent. Synthesize all findings into actionable recommendations.

Based on inputs from other agents:
1. Prioritize by impact (Critical > High > Medium > Low)
2. Group by category (Cost, Performance, Security, Code Quality)
3. Estimate effort for each recommendation
4. Create implementation roadmap

Use calculate_platform_costs to compare platform options.

Format as prioritized list with clear next steps."""


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
        """Run all agents on a config.

        Args:
            config: Job configuration
            execute: If True, run ExecutionAgent to actually start the job
        """
        job_name = config.get('job_name', 'unknown')
        print(f"\n  Job: {job_name}")
        print(f"  Mode: {'EXECUTE' if execute else 'ANALYZE ONLY'}")
        print(f"  {'─'*60}")

        # 1. Sizing Agent
        print(f"\n  [1/7] SIZING AGENT")
        self.results['sizing'] = self._run_sizing_agent(config)

        # 2. Code Analysis Agent
        print(f"\n  [2/7] CODE ANALYSIS AGENT")
        self.results['code_analysis'] = self._run_code_agent(config)

        # 3. Data Quality Agent
        print(f"\n  [3/7] DATA QUALITY AGENT")
        self.results['data_quality'] = self._run_data_quality_agent(config)

        # 4. Compliance Agent
        print(f"\n  [4/7] COMPLIANCE AGENT")
        self.results['compliance'] = self._run_compliance_agent(config)

        # 5. Learning Agent (loads historical data)
        print(f"\n  [5/7] LEARNING AGENT")
        self.results['learning'] = self._run_learning_agent(config)

        # 6. Execution Agent (optionally runs the job)
        print(f"\n  [6/7] EXECUTION AGENT")
        self.results['execution'] = self._run_execution_agent(config, execute)

        # 7. Recommendation Agent
        print(f"\n  [7/7] RECOMMENDATION AGENT")
        self.results['recommendations'] = self._run_recommendation_agent(config)

        return {
            'job_name': job_name,
            'config': config,
            'agents': self.results,
            'token_usage': self.tracker.summary()
        }

    def _run_agent(self, name: str, system_prompt: str, user_prompt: str, tools: list) -> str:
        """Run a single agent and track metrics."""
        start = time.time()

        print(f"    Creating agent...")
        print(f"    Prompt: {len(user_prompt)} chars")

        try:
            agent = Agent(
                model=self.model,
                system_prompt=system_prompt,
                tools=tools
            )

            print(f"    Executing...")
            response = agent(user_prompt)
            duration = time.time() - start

            # Extract tokens
            input_tok = getattr(getattr(response, 'metrics', None), 'input_tokens', 0) or 0
            output_tok = getattr(getattr(response, 'metrics', None), 'output_tokens', 0) or 0

            self.tracker.log_agent(name, input_tok, output_tok, duration)

            return str(response)

        except Exception as e:
            print(f"    ERROR: {e}")
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
        prompt = f"""Check compliance for job '{config.get('job_name')}':

Current Config:
  Workers: {current.get('NumberOfWorkers', current.get('workers', 'unknown'))}
  Worker Type: {current.get('WorkerType', 'unknown')}
  Timeout: {current.get('Timeout', 'not set')} minutes

Review for security, cost controls, and best practices."""

        result = self._run_agent('compliance', COMPLIANCE_PROMPT, prompt, [])
        return {'analysis': result}

    def _run_learning_agent(self, config: Dict) -> Dict:
        glue_job = config.get('glue_job_name', config.get('job_name', ''))
        prompt = f"""Analyze historical data for job: {glue_job}

1. Use load_execution_history to get stored execution records
2. Use get_glue_job_runs to get recent Glue runs
3. Identify patterns: avg duration, cost trends, failure rates
4. Learn optimal resource configurations
5. Detect anomalies or degradation"""

        result = self._run_agent('learning', LEARNING_PROMPT, prompt,
                                  [get_glue_job_runs, load_execution_history])
        return {'glue_job': glue_job, 'analysis': result}

    def _run_execution_agent(self, config: Dict, execute: bool = False) -> Dict:
        glue_job = config.get('glue_job_name', config.get('job_name', ''))
        current = config.get('current_config', {})
        workers = current.get('NumberOfWorkers', current.get('workers', 10))
        worker_type = current.get('WorkerType', 'G.1X')

        if execute:
            prompt = f"""Execute the Glue job: {glue_job}

Configuration:
- Workers: {workers}
- Worker Type: {worker_type}

Steps:
1. Start the job using start_glue_job with dry_run=False
2. Monitor status using get_job_run_status (poll every 30 seconds)
3. Once complete, store results using store_execution_history
4. Report final status, duration, cost, and any errors"""
        else:
            prompt = f"""Simulate execution for job: {glue_job}

Configuration:
- Workers: {workers}
- Worker Type: {worker_type}

Use start_glue_job with dry_run=True to estimate:
- Expected duration based on data size
- Expected cost
- Resource utilization

Store the simulation results for future reference."""

        result = self._run_agent('execution', EXECUTION_PROMPT, prompt,
                                  [start_glue_job, get_job_run_status, store_execution_history])
        return {'glue_job': glue_job, 'executed': execute, 'analysis': result}

    def _run_recommendation_agent(self, config: Dict) -> Dict:
        current = config.get('current_config', {})
        workers = current.get('NumberOfWorkers', current.get('workers', 10))
        runtime = config.get('avg_runtime_hours', 0.5)
        runs = config.get('monthly_runs', 30)

        prompt = f"""Synthesize recommendations for job '{config.get('job_name')}':

Previous Agent Findings:
---
SIZING: {self.results.get('sizing', {}).get('analysis', 'N/A')[:400]}
---
CODE ANALYSIS: {self.results.get('code_analysis', {}).get('analysis', 'N/A')[:400]}
---
DATA QUALITY: {self.results.get('data_quality', {}).get('analysis', 'N/A')[:400]}
---
COMPLIANCE: {self.results.get('compliance', {}).get('analysis', 'N/A')[:400]}
---
LEARNING: {self.results.get('learning', {}).get('analysis', 'N/A')[:400]}
---
EXECUTION: {self.results.get('execution', {}).get('analysis', 'N/A')[:400]}
---

Current: {workers} workers, ~{runtime}h runtime, {runs} runs/month

Provide:
1. CRITICAL issues (must fix - data quality failures, code errors)
2. HIGH priority optimizations (significant cost/performance impact)
3. MEDIUM improvements (nice to have)
4. Platform comparison with cost savings
5. Implementation roadmap with effort estimates"""

        result = self._run_agent('recommendations', RECOMMENDATION_PROMPT, prompt, [calculate_platform_costs])
        return {'analysis': result}


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

Environment:
  AWS_REGION=us-west-2 (default)
  AWS_PROFILE=your-profile
"""
    )
    parser.add_argument('--config', '-c', help='Single config JSON file')
    parser.add_argument('--source', '-s', help='Directory with config JSONs (batch mode)')
    parser.add_argument('--dest', '-d', default='./reports', help='Output directory')
    parser.add_argument('--model', '-m', help='Bedrock model ID')
    parser.add_argument('--execute', '-e', action='store_true', help='Actually execute the Glue job (not just analyze)')

    args = parser.parse_args()

    if not args.config and not args.source:
        parser.error("Either --config or --source required")

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
