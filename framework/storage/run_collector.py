"""
Run Collector
=============

Collects execution metrics from AWS Glue/EMR job runs and stores them
in the local agent store for learning and analysis.

Usage:
    # After a Glue job completes
    collector = RunCollector()
    collector.collect_glue_run('my-job-name', 'jr_xxx')

    # Manual recording
    collector.record_run(
        job_name='sales_analytics',
        records_processed=500000,
        duration_seconds=1200,
        cost_usd=1.50,
        platform='glue'
    )
"""

import os
import json
import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List

try:
    import boto3
    from botocore.exceptions import ClientError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

from .local_agent_store import (
    LocalAgentStore,
    ExecutionRecord,
    ComplianceResult,
    DataQualityResult,
    get_store
)

logger = logging.getLogger(__name__)


class RunCollector:
    """
    Collects and stores execution metrics from job runs.

    Can collect from:
    - AWS Glue job runs (via CloudWatch/Glue API)
    - EMR step executions
    - Manual metric reporting
    """

    # Glue pricing (per DPU-hour)
    GLUE_DPU_HOUR_COST = 0.44
    GLUE_FLEX_DPU_HOUR_COST = 0.29

    def __init__(self, store: LocalAgentStore = None, region: str = None):
        self.store = store or get_store()
        self.region = region or os.environ.get('AWS_REGION', 'us-west-2')

        if HAS_BOTO3:
            self.glue_client = boto3.client('glue', region_name=self.region)
            self.cloudwatch = boto3.client('cloudwatch', region_name=self.region)
        else:
            self.glue_client = None
            self.cloudwatch = None

    def collect_glue_run(self, job_name: str, run_id: str) -> Optional[ExecutionRecord]:
        """
        Collect metrics from a completed Glue job run.

        Args:
            job_name: Name of the Glue job
            run_id: The job run ID (e.g., jr_xxx)

        Returns:
            ExecutionRecord if successful, None otherwise
        """
        if not self.glue_client:
            logger.error("boto3 not available - cannot collect Glue metrics")
            return None

        try:
            # Get job run details
            response = self.glue_client.get_job_run(
                JobName=job_name,
                RunId=run_id
            )
            run = response['JobRun']

            # Calculate duration
            started = run.get('StartedOn')
            completed = run.get('CompletedOn')
            if started and completed:
                duration = (completed - started).total_seconds()
            else:
                duration = run.get('ExecutionTime', 0)

            # Calculate cost
            dpu_seconds = run.get('DPUSeconds', 0)
            dpu_hours = dpu_seconds / 3600
            worker_type = run.get('WorkerType', 'G.1X')

            # Adjust cost based on worker type
            if 'flex' in str(run.get('ExecutionClass', '')).lower():
                cost = dpu_hours * self.GLUE_FLEX_DPU_HOUR_COST
            else:
                cost = dpu_hours * self.GLUE_DPU_HOUR_COST

            # Determine memory based on worker type
            memory_map = {
                'G.1X': 16,
                'G.2X': 32,
                'G.4X': 64,
                'G.8X': 128,
                'Standard': 16
            }
            memory_gb = memory_map.get(worker_type, 16)

            # Get number of workers
            workers = run.get('NumberOfWorkers', run.get('MaxCapacity', 2))

            # Get status
            status_map = {
                'SUCCEEDED': 'SUCCEEDED',
                'FAILED': 'FAILED',
                'TIMEOUT': 'TIMEOUT',
                'STOPPED': 'FAILED',
                'STOPPING': 'FAILED',
                'RUNNING': 'RUNNING'
            }
            status = status_map.get(run.get('JobRunState'), 'UNKNOWN')

            # Try to get records processed from CloudWatch metrics
            records_processed = self._get_records_from_cloudwatch(job_name, run_id, started, completed)

            # Create execution record
            record = ExecutionRecord(
                job_name=job_name,
                run_id=run_id,
                timestamp=started.isoformat() if started else datetime.now().isoformat(),
                records_processed=records_processed,
                duration_seconds=duration,
                dpu_hours=dpu_hours,
                cost_usd=cost,
                memory_gb=memory_gb,
                platform='glue',
                workers=int(workers),
                status=status,
                error_message=run.get('ErrorMessage'),
                spark_metrics=self._extract_spark_metrics(run)
            )

            # Store the record
            self.store.record_execution(record)

            logger.info(f"Collected Glue run: {job_name}/{run_id} - {status}")
            return record

        except ClientError as e:
            logger.error(f"Failed to collect Glue run {job_name}/{run_id}: {e}")
            return None

    def _get_records_from_cloudwatch(self, job_name: str, run_id: str,
                                      start_time, end_time) -> int:
        """Try to get records processed from CloudWatch metrics."""
        if not self.cloudwatch or not start_time or not end_time:
            return 0

        try:
            # Glue publishes some metrics to CloudWatch
            response = self.cloudwatch.get_metric_statistics(
                Namespace='Glue',
                MetricName='glue.driver.aggregate.recordsRead',
                Dimensions=[
                    {'Name': 'JobName', 'Value': job_name},
                    {'Name': 'JobRunId', 'Value': run_id}
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Sum']
            )

            if response['Datapoints']:
                return int(sum(dp['Sum'] for dp in response['Datapoints']))

        except ClientError:
            pass

        return 0

    def _extract_spark_metrics(self, run: Dict) -> Dict[str, Any]:
        """Extract Spark metrics from job run arguments."""
        metrics = {}

        # Get arguments that might contain metrics
        args = run.get('Arguments', {})
        metrics['job_bookmark'] = args.get('--job-bookmark-option', 'disabled')
        metrics['enable_metrics'] = args.get('--enable-metrics', 'false')

        # Get Glue version
        metrics['glue_version'] = run.get('GlueVersion', 'unknown')

        return metrics

    def record_run(self,
                   job_name: str,
                   records_processed: int,
                   duration_seconds: float,
                   cost_usd: float,
                   platform: str = 'glue',
                   workers: int = 5,
                   memory_gb: float = 16.0,
                   status: str = 'SUCCEEDED',
                   run_id: str = None,
                   **kwargs) -> ExecutionRecord:
        """
        Manually record a job run.

        Useful when:
        - Running locally for testing
        - AWS API not available
        - Recording from custom platforms
        """
        record = ExecutionRecord(
            job_name=job_name,
            run_id=run_id or f"manual_{uuid.uuid4().hex[:8]}",
            timestamp=datetime.now().isoformat(),
            records_processed=records_processed,
            duration_seconds=duration_seconds,
            dpu_hours=duration_seconds / 3600 * (workers / 2),  # Estimate
            cost_usd=cost_usd,
            memory_gb=memory_gb,
            platform=platform,
            workers=workers,
            status=status,
            **kwargs
        )

        self.store.record_execution(record)
        return record

    def record_compliance_check(self,
                                 job_name: str,
                                 frameworks: Dict[str, Dict],
                                 pii_detected: List[str] = None,
                                 pii_masked: List[str] = None,
                                 run_id: str = None) -> ComplianceResult:
        """Record a compliance check result."""
        # Determine overall status
        statuses = [fw.get('status', 'UNKNOWN') for fw in frameworks.values()]
        if 'NON_COMPLIANT' in statuses:
            overall = 'NON_COMPLIANT'
        elif 'WARNING' in statuses or any(fw.get('warnings', 0) > 0 for fw in frameworks.values()):
            overall = 'WARNING'
        else:
            overall = 'COMPLIANT'

        result = ComplianceResult(
            job_name=job_name,
            run_id=run_id or f"compliance_{uuid.uuid4().hex[:8]}",
            timestamp=datetime.now().isoformat(),
            overall_status=overall,
            frameworks=frameworks,
            pii_columns_detected=pii_detected or [],
            pii_columns_masked=pii_masked or []
        )

        self.store.record_compliance(result)
        return result

    def record_data_quality_check(self,
                                   table_name: str,
                                   checks: List[Dict],
                                   row_count: int = 0,
                                   null_percentages: Dict[str, float] = None,
                                   run_id: str = None) -> DataQualityResult:
        """Record a data quality check result."""
        passed = sum(1 for c in checks if c.get('status') == 'PASSED')
        failed = sum(1 for c in checks if c.get('status') == 'FAILED')
        warnings = sum(1 for c in checks if c.get('status') == 'WARNING')

        result = DataQualityResult(
            table_name=table_name,
            run_id=run_id or f"dq_{uuid.uuid4().hex[:8]}",
            timestamp=datetime.now().isoformat(),
            total_checks=len(checks),
            passed_checks=passed,
            failed_checks=failed,
            warning_checks=warnings,
            check_details=checks,
            row_count=row_count,
            null_percentages=null_percentages or {}
        )

        self.store.record_data_quality(result)
        return result

    def collect_all_recent_runs(self, job_name: str, days: int = 7) -> List[ExecutionRecord]:
        """
        Collect all runs for a job from the last N days.

        Useful for initial data population.
        """
        if not self.glue_client:
            logger.error("boto3 not available")
            return []

        try:
            # Get job run history
            paginator = self.glue_client.get_paginator('get_job_runs')
            records = []

            for page in paginator.paginate(JobName=job_name):
                for run in page.get('JobRuns', []):
                    # Check if within time window
                    started = run.get('StartedOn')
                    if started:
                        from datetime import timedelta
                        cutoff = datetime.now(started.tzinfo) - timedelta(days=days)
                        if started < cutoff:
                            continue

                    # Only collect completed runs
                    if run.get('JobRunState') in ('SUCCEEDED', 'FAILED', 'TIMEOUT'):
                        record = self.collect_glue_run(job_name, run['Id'])
                        if record:
                            records.append(record)

            logger.info(f"Collected {len(records)} runs for {job_name}")
            return records

        except ClientError as e:
            logger.error(f"Failed to collect runs for {job_name}: {e}")
            return []


def seed_sample_data(store: LocalAgentStore = None):
    """
    Seed the store with sample data for demonstration.

    This creates realistic historical data that the agents can learn from.
    """
    store = store or get_store()
    collector = RunCollector(store)

    print("Seeding sample execution data...")

    # Sample runs with varying characteristics
    sample_runs = [
        # sales_analytics job - varied runs over time
        {'job_name': 'sales_analytics', 'records': 285000, 'duration': 1520, 'cost': 1.05, 'workers': 9, 'memory': 16, 'days_ago': 6},
        {'job_name': 'sales_analytics', 'records': 292000, 'duration': 1580, 'cost': 1.12, 'workers': 9, 'memory': 16, 'days_ago': 5},
        {'job_name': 'sales_analytics', 'records': 278000, 'duration': 1450, 'cost': 0.98, 'workers': 8, 'memory': 16, 'days_ago': 4},
        {'job_name': 'sales_analytics', 'records': 305000, 'duration': 1620, 'cost': 1.18, 'workers': 10, 'memory': 16, 'days_ago': 3},
        {'job_name': 'sales_analytics', 'records': 312000, 'duration': 1710, 'cost': 1.25, 'workers': 10, 'memory': 18, 'days_ago': 2},
        {'job_name': 'sales_analytics', 'records': 298000, 'duration': 1550, 'cost': 1.08, 'workers': 9, 'memory': 16, 'days_ago': 1},
        {'job_name': 'sales_analytics', 'records': 315000, 'duration': 1680, 'cost': 1.22, 'workers': 10, 'memory': 18, 'days_ago': 0},

        # customer_etl job
        {'job_name': 'customer_etl', 'records': 48000, 'duration': 290, 'cost': 0.20, 'workers': 3, 'memory': 8, 'days_ago': 5},
        {'job_name': 'customer_etl', 'records': 52000, 'duration': 310, 'cost': 0.23, 'workers': 3, 'memory': 8, 'days_ago': 3},
        {'job_name': 'customer_etl', 'records': 50000, 'duration': 300, 'cost': 0.22, 'workers': 3, 'memory': 8, 'days_ago': 1},

        # demo_complex_sales_analytics (the actual job name from the demo)
        {'job_name': 'demo_complex_sales_analytics', 'records': 290000, 'duration': 1540, 'cost': 1.08, 'workers': 9, 'memory': 15, 'days_ago': 6},
        {'job_name': 'demo_complex_sales_analytics', 'records': 295000, 'duration': 1590, 'cost': 1.15, 'workers': 9, 'memory': 16, 'days_ago': 4},
        {'job_name': 'demo_complex_sales_analytics', 'records': 302000, 'duration': 1650, 'cost': 1.20, 'workers': 10, 'memory': 16, 'days_ago': 2},
        {'job_name': 'demo_complex_sales_analytics', 'records': 312000, 'duration': 1710, 'cost': 1.25, 'workers': 10, 'memory': 18, 'days_ago': 0},
    ]

    from datetime import timedelta

    for run in sample_runs:
        timestamp = datetime.now() - timedelta(days=run['days_ago'])
        collector.record_run(
            job_name=run['job_name'],
            records_processed=run['records'],
            duration_seconds=run['duration'],
            cost_usd=run['cost'],
            workers=run['workers'],
            memory_gb=run['memory'],
            status='SUCCEEDED',
            run_id=f"seed_{run['job_name']}_{run['days_ago']}"
        )

    # Seed compliance data
    print("Seeding compliance data...")
    collector.record_compliance_check(
        job_name='demo_complex_sales_analytics',
        frameworks={
            'GDPR': {
                'status': 'COMPLIANT',
                'checks_passed': 7,
                'checks_failed': 0,
                'warnings': 1,
                'encryption_status': 'ENABLED',
                'retention_policy': '365 days',
                'findings': [
                    {'check': 'PII Masking', 'status': 'WARNING',
                     'detail': 'customer_name partially masked, consider full masking'}
                ]
            },
            'PCI_DSS': {
                'status': 'COMPLIANT',
                'checks_passed': 6,
                'checks_failed': 0,
                'warnings': 0,
                'card_data_masked': True,
                'encryption_status': 'ENABLED'
            },
            'SOX': {
                'status': 'COMPLIANT',
                'checks_passed': 5,
                'checks_failed': 0,
                'warnings': 0,
                'audit_trail': 'ENABLED',
                'data_lineage': 'TRACKED'
            }
        },
        pii_detected=['customer_email', 'customer_name', 'phone', 'address'],
        pii_masked=['customer_email', 'phone', 'credit_card_masked']
    )

    # Seed data quality data
    print("Seeding data quality data...")
    collector.record_data_quality_check(
        table_name='analytics.sales_fact',
        checks=[
            {'check': 'null_check', 'column': 'order_id', 'status': 'PASSED', 'detail': '0% null'},
            {'check': 'null_check', 'column': 'customer_id', 'status': 'PASSED', 'detail': '0.1% null'},
            {'check': 'null_check', 'column': 'product_id', 'status': 'PASSED', 'detail': '0% null'},
            {'check': 'range_check', 'column': 'quantity', 'status': 'PASSED', 'detail': 'All values in range [1, 100]'},
            {'check': 'range_check', 'column': 'unit_price', 'status': 'PASSED', 'detail': 'All values in range [0.01, 10000]'},
            {'check': 'range_check', 'column': 'total_amount', 'status': 'WARNING', 'detail': '2 values outside expected range'},
            {'check': 'uniqueness', 'column': 'order_id', 'status': 'PASSED', 'detail': '100% unique'},
            {'check': 'referential', 'column': 'customer_id', 'status': 'PASSED', 'detail': 'All values exist in customers'},
        ],
        row_count=315000,
        null_percentages={'customer_id': 0.1, 'product_id': 0, 'quantity': 0}
    )

    print("Sample data seeded successfully!")
    print(f"  - Execution records: {len(sample_runs)}")
    print(f"  - Compliance checks: 1")
    print(f"  - Data quality checks: 1")

    # Show computed baselines
    for job in ['sales_analytics', 'customer_etl', 'demo_complex_sales_analytics']:
        baseline = store.get_baseline(job)
        if baseline:
            print(f"\nBaseline for {job}:")
            print(f"  Samples: {baseline.get('sample_count')}")
            print(f"  Avg Duration: {baseline.get('avg_duration_seconds', 0)/60:.1f} min")
            print(f"  Avg Cost: ${baseline.get('avg_cost', 0):.2f}")
