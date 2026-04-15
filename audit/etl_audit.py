"""
ETL Audit Module - Stores all ETL run information in audit tables
Supports DynamoDB (serverless) or RDS (relational) backends
"""

import json
import logging
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from enum import Enum
import hashlib

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AuditBackend(Enum):
    """Supported audit storage backends."""
    DYNAMODB = "dynamodb"
    RDS_MYSQL = "rds_mysql"
    RDS_POSTGRESQL = "rds_postgresql"
    S3_PARQUET = "s3_parquet"


class RunStatus(Enum):
    """ETL run status."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"


@dataclass
class ETLRunAudit:
    """Complete audit record for an ETL run."""
    # Primary identifiers
    run_id: str
    job_name: str
    job_version: str = "1.0"

    # Execution details
    platform: str = "glue"  # glue, emr, eks, lambda
    status: str = RunStatus.PENDING.value
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_seconds: int = 0

    # Configuration used
    config_snapshot: Dict[str, Any] = field(default_factory=dict)
    spark_config: Dict[str, Any] = field(default_factory=dict)
    worker_type: str = ""
    number_of_workers: int = 0

    # Data metrics
    input_tables: List[str] = field(default_factory=list)
    output_tables: List[str] = field(default_factory=list)
    rows_read: int = 0
    rows_written: int = 0
    bytes_read: int = 0
    bytes_written: int = 0
    partitions_processed: int = 0

    # Performance metrics
    shuffle_bytes: int = 0
    spill_bytes: int = 0
    peak_memory_mb: int = 0
    cpu_utilization_pct: float = 0.0

    # Cost tracking
    dpu_seconds: int = 0
    estimated_cost_usd: float = 0.0

    # Data quality
    dq_score: float = 0.0
    dq_rules_passed: int = 0
    dq_rules_failed: int = 0
    dq_details: Dict[str, Any] = field(default_factory=dict)

    # Error information
    error_message: str = ""
    error_type: str = ""
    error_stack_trace: str = ""

    # Auto-healing
    healing_applied: bool = False
    healing_actions: List[str] = field(default_factory=list)
    retry_count: int = 0

    # Recommendations
    recommendations: List[str] = field(default_factory=list)
    platform_recommendation: str = ""

    # Lineage
    triggered_by: str = ""  # user, schedule, dependency
    trigger_source: str = ""  # username, eventbridge rule, upstream job
    downstream_jobs: List[str] = field(default_factory=list)

    # Metadata
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class DataQualityAudit:
    """Audit record for data quality check."""
    audit_id: str
    run_id: str  # Links to ETLRunAudit
    job_name: str
    table_name: str

    # Results
    overall_score: float = 0.0
    total_rules: int = 0
    passed_rules: int = 0
    failed_rules: int = 0

    # Rule details
    rule_results: List[Dict[str, Any]] = field(default_factory=list)

    # Sample failures
    sample_failures: List[Dict[str, Any]] = field(default_factory=list)

    # Recommendations
    recommendations: List[str] = field(default_factory=list)

    # Timestamps
    executed_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class PlatformRecommendationAudit:
    """Audit record for platform recommendations."""
    audit_id: str
    run_id: str
    job_name: str

    # Current state
    current_platform: str = ""
    current_config: Dict[str, Any] = field(default_factory=dict)

    # Recommendation
    recommended_platform: str = ""
    recommended_config: Dict[str, Any] = field(default_factory=dict)
    confidence_score: float = 0.0

    # Reasoning
    reasoning: List[str] = field(default_factory=list)
    estimated_savings_pct: float = 0.0
    estimated_savings_usd: float = 0.0

    # Action taken
    action_taken: str = ""  # applied, ignored, deferred

    # Timestamps
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())


class ETLAuditManager:
    """
    Manages ETL audit records across different storage backends.

    Supports:
    - DynamoDB (recommended for serverless)
    - S3 Parquet (for analytics)
    - RDS (for SQL queries)
    """

    # DynamoDB table names
    RUNS_TABLE = "etl_audit_runs"
    DQ_TABLE = "etl_audit_data_quality"
    RECOMMENDATIONS_TABLE = "etl_audit_recommendations"

    def __init__(self,
                 backend: AuditBackend = AuditBackend.DYNAMODB,
                 region: str = 'us-east-1',
                 table_prefix: str = ""):
        """
        Initialize audit manager.

        Args:
            backend: Storage backend to use
            region: AWS region
            table_prefix: Prefix for table names (e.g., 'prod_')
        """
        self.backend = backend
        self.region = region
        self.table_prefix = table_prefix

        # Initialize clients based on backend
        if backend == AuditBackend.DYNAMODB:
            self.dynamodb = boto3.resource('dynamodb', region_name=region)
            self.dynamodb_client = boto3.client('dynamodb', region_name=region)
        elif backend == AuditBackend.S3_PARQUET:
            self.s3_client = boto3.client('s3', region_name=region)

    def _get_table_name(self, base_name: str) -> str:
        """Get full table name with prefix."""
        return f"{self.table_prefix}{base_name}"

    def create_tables(self) -> Dict[str, bool]:
        """
        Create audit tables if they don't exist.

        Returns:
            Dict with table names and creation status
        """
        results = {}

        if self.backend == AuditBackend.DYNAMODB:
            results = self._create_dynamodb_tables()

        return results

    def _create_dynamodb_tables(self) -> Dict[str, bool]:
        """Create DynamoDB tables."""
        results = {}

        tables_config = [
            {
                "name": self._get_table_name(self.RUNS_TABLE),
                "key_schema": [
                    {"AttributeName": "job_name", "KeyType": "HASH"},
                    {"AttributeName": "run_id", "KeyType": "RANGE"}
                ],
                "attribute_definitions": [
                    {"AttributeName": "job_name", "AttributeType": "S"},
                    {"AttributeName": "run_id", "AttributeType": "S"},
                    {"AttributeName": "started_at", "AttributeType": "S"},
                    {"AttributeName": "status", "AttributeType": "S"}
                ],
                "gsi": [
                    {
                        "IndexName": "status-started_at-index",
                        "KeySchema": [
                            {"AttributeName": "status", "KeyType": "HASH"},
                            {"AttributeName": "started_at", "KeyType": "RANGE"}
                        ],
                        "Projection": {"ProjectionType": "ALL"}
                    },
                    {
                        "IndexName": "job_name-started_at-index",
                        "KeySchema": [
                            {"AttributeName": "job_name", "KeyType": "HASH"},
                            {"AttributeName": "started_at", "KeyType": "RANGE"}
                        ],
                        "Projection": {"ProjectionType": "ALL"}
                    }
                ]
            },
            {
                "name": self._get_table_name(self.DQ_TABLE),
                "key_schema": [
                    {"AttributeName": "run_id", "KeyType": "HASH"},
                    {"AttributeName": "audit_id", "KeyType": "RANGE"}
                ],
                "attribute_definitions": [
                    {"AttributeName": "run_id", "AttributeType": "S"},
                    {"AttributeName": "audit_id", "AttributeType": "S"},
                    {"AttributeName": "table_name", "AttributeType": "S"},
                    {"AttributeName": "executed_at", "AttributeType": "S"}
                ],
                "gsi": [
                    {
                        "IndexName": "table_name-executed_at-index",
                        "KeySchema": [
                            {"AttributeName": "table_name", "KeyType": "HASH"},
                            {"AttributeName": "executed_at", "KeyType": "RANGE"}
                        ],
                        "Projection": {"ProjectionType": "ALL"}
                    }
                ]
            },
            {
                "name": self._get_table_name(self.RECOMMENDATIONS_TABLE),
                "key_schema": [
                    {"AttributeName": "job_name", "KeyType": "HASH"},
                    {"AttributeName": "audit_id", "KeyType": "RANGE"}
                ],
                "attribute_definitions": [
                    {"AttributeName": "job_name", "AttributeType": "S"},
                    {"AttributeName": "audit_id", "AttributeType": "S"}
                ],
                "gsi": []
            }
        ]

        for table_config in tables_config:
            table_name = table_config["name"]
            try:
                # Check if table exists
                self.dynamodb_client.describe_table(TableName=table_name)
                results[table_name] = True
                logger.info(f"Table {table_name} already exists")
            except self.dynamodb_client.exceptions.ResourceNotFoundException:
                # Create table
                try:
                    create_params = {
                        "TableName": table_name,
                        "KeySchema": table_config["key_schema"],
                        "AttributeDefinitions": table_config["attribute_definitions"],
                        "BillingMode": "PAY_PER_REQUEST"
                    }

                    if table_config.get("gsi"):
                        create_params["GlobalSecondaryIndexes"] = [
                            {**gsi, "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}}
                            if "ProvisionedThroughput" not in gsi else gsi
                            for gsi in table_config["gsi"]
                        ]
                        # For PAY_PER_REQUEST, GSIs don't need ProvisionedThroughput
                        create_params["GlobalSecondaryIndexes"] = table_config["gsi"]

                    self.dynamodb_client.create_table(**create_params)

                    # Wait for table to be active
                    waiter = self.dynamodb_client.get_waiter('table_exists')
                    waiter.wait(TableName=table_name)

                    results[table_name] = True
                    logger.info(f"Created table {table_name}")
                except ClientError as e:
                    results[table_name] = False
                    logger.error(f"Failed to create table {table_name}: {e}")

        return results

    def start_run(self, job_name: str, config: Dict[str, Any] = None) -> ETLRunAudit:
        """
        Start a new ETL run audit.

        Args:
            job_name: Name of the ETL job
            config: Job configuration

        Returns:
            New ETLRunAudit record
        """
        run_id = f"{job_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

        audit = ETLRunAudit(
            run_id=run_id,
            job_name=job_name,
            status=RunStatus.RUNNING.value,
            started_at=datetime.utcnow().isoformat(),
            config_snapshot=config or {},
            platform=config.get('execution', {}).get('platform', 'glue') if config else 'glue',
            worker_type=config.get('execution', {}).get('worker_type', 'G.1X') if config else 'G.1X',
            number_of_workers=config.get('execution', {}).get('number_of_workers', 2) if config else 2,
            input_tables=[
                f"{s.get('database', '')}.{s.get('table', '')}"
                for s in config.get('data_sources', [])
            ] if config else []
        )

        self._save_run_audit(audit)
        logger.info(f"Started audit for run {run_id}")

        return audit

    def update_run(self, audit: ETLRunAudit) -> bool:
        """
        Update an existing run audit.

        Args:
            audit: Updated audit record

        Returns:
            True if successful
        """
        audit.updated_at = datetime.utcnow().isoformat()
        return self._save_run_audit(audit)

    def complete_run(self, audit: ETLRunAudit,
                     status: RunStatus = RunStatus.SUCCEEDED,
                     error: str = None) -> bool:
        """
        Mark a run as complete.

        Args:
            audit: Audit record to complete
            status: Final status
            error: Error message if failed

        Returns:
            True if successful
        """
        audit.status = status.value
        audit.completed_at = datetime.utcnow().isoformat()

        # Calculate duration
        if audit.started_at:
            started = datetime.fromisoformat(audit.started_at)
            completed = datetime.fromisoformat(audit.completed_at)
            audit.duration_seconds = int((completed - started).total_seconds())

        # Calculate cost
        if audit.dpu_seconds > 0:
            dpu_hours = audit.dpu_seconds / 3600
            cost_per_dpu_hour = 0.44 if audit.platform == 'glue' else 0.29  # Flex rate
            audit.estimated_cost_usd = round(dpu_hours * cost_per_dpu_hour * audit.number_of_workers, 4)

        if error:
            audit.error_message = error[:5000]  # Truncate long errors
            audit.status = RunStatus.FAILED.value

        return self._save_run_audit(audit)

    def _save_run_audit(self, audit: ETLRunAudit) -> bool:
        """Save run audit to backend."""
        if self.backend == AuditBackend.DYNAMODB:
            return self._save_to_dynamodb(
                self._get_table_name(self.RUNS_TABLE),
                asdict(audit)
            )
        return False

    def _save_to_dynamodb(self, table_name: str, item: Dict[str, Any]) -> bool:
        """Save item to DynamoDB."""
        try:
            table = self.dynamodb.Table(table_name)

            # Convert empty strings to None (DynamoDB doesn't allow empty strings)
            cleaned_item = self._clean_for_dynamodb(item)

            table.put_item(Item=cleaned_item)
            return True
        except ClientError as e:
            logger.error(f"Failed to save to DynamoDB: {e}")
            return False

    def _clean_for_dynamodb(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Clean item for DynamoDB (remove empty strings, convert types)."""
        cleaned = {}
        for key, value in item.items():
            if value == "":
                continue  # Skip empty strings
            elif isinstance(value, dict):
                cleaned_dict = self._clean_for_dynamodb(value)
                if cleaned_dict:  # Only add non-empty dicts
                    cleaned[key] = cleaned_dict
            elif isinstance(value, list):
                if value:  # Only add non-empty lists
                    cleaned[key] = [
                        self._clean_for_dynamodb(v) if isinstance(v, dict) else v
                        for v in value if v != ""
                    ]
            elif isinstance(value, float):
                # Convert to Decimal for DynamoDB
                from decimal import Decimal
                cleaned[key] = Decimal(str(value))
            else:
                cleaned[key] = value
        return cleaned

    def save_dq_audit(self, audit: DataQualityAudit) -> bool:
        """Save data quality audit."""
        if self.backend == AuditBackend.DYNAMODB:
            return self._save_to_dynamodb(
                self._get_table_name(self.DQ_TABLE),
                asdict(audit)
            )
        return False

    def save_recommendation_audit(self, audit: PlatformRecommendationAudit) -> bool:
        """Save platform recommendation audit."""
        if self.backend == AuditBackend.DYNAMODB:
            return self._save_to_dynamodb(
                self._get_table_name(self.RECOMMENDATIONS_TABLE),
                asdict(audit)
            )
        return False

    def get_run(self, job_name: str, run_id: str) -> Optional[ETLRunAudit]:
        """Get a specific run audit."""
        if self.backend == AuditBackend.DYNAMODB:
            try:
                table = self.dynamodb.Table(self._get_table_name(self.RUNS_TABLE))
                response = table.get_item(
                    Key={"job_name": job_name, "run_id": run_id}
                )
                if 'Item' in response:
                    return self._dict_to_audit(response['Item'])
            except ClientError as e:
                logger.error(f"Failed to get run: {e}")
        return None

    def get_runs_by_job(self, job_name: str,
                        limit: int = 100,
                        start_date: datetime = None) -> List[ETLRunAudit]:
        """Get runs for a specific job."""
        runs = []
        if self.backend == AuditBackend.DYNAMODB:
            try:
                table = self.dynamodb.Table(self._get_table_name(self.RUNS_TABLE))

                query_params = {
                    "IndexName": "job_name-started_at-index",
                    "KeyConditionExpression": "job_name = :jn",
                    "ExpressionAttributeValues": {":jn": job_name},
                    "ScanIndexForward": False,  # Descending order
                    "Limit": limit
                }

                if start_date:
                    query_params["KeyConditionExpression"] += " AND started_at >= :sd"
                    query_params["ExpressionAttributeValues"][":sd"] = start_date.isoformat()

                response = table.query(**query_params)

                for item in response.get('Items', []):
                    runs.append(self._dict_to_audit(item))

            except ClientError as e:
                logger.error(f"Failed to query runs: {e}")

        return runs

    def get_runs_by_status(self, status: RunStatus,
                           limit: int = 100) -> List[ETLRunAudit]:
        """Get runs by status."""
        runs = []
        if self.backend == AuditBackend.DYNAMODB:
            try:
                table = self.dynamodb.Table(self._get_table_name(self.RUNS_TABLE))

                response = table.query(
                    IndexName="status-started_at-index",
                    KeyConditionExpression="status = :s",
                    ExpressionAttributeValues={":s": status.value},
                    ScanIndexForward=False,
                    Limit=limit
                )

                for item in response.get('Items', []):
                    runs.append(self._dict_to_audit(item))

            except ClientError as e:
                logger.error(f"Failed to query runs by status: {e}")

        return runs

    def get_recent_runs(self, hours: int = 24, limit: int = 100) -> List[ETLRunAudit]:
        """Get all recent runs across all jobs."""
        runs = []
        if self.backend == AuditBackend.DYNAMODB:
            try:
                table = self.dynamodb.Table(self._get_table_name(self.RUNS_TABLE))
                cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()

                # Scan with filter (not ideal for large tables, but works for recent data)
                response = table.scan(
                    FilterExpression="started_at >= :cutoff",
                    ExpressionAttributeValues={":cutoff": cutoff},
                    Limit=limit
                )

                for item in response.get('Items', []):
                    runs.append(self._dict_to_audit(item))

                # Sort by started_at descending
                runs.sort(key=lambda x: x.started_at or "", reverse=True)

            except ClientError as e:
                logger.error(f"Failed to scan recent runs: {e}")

        return runs[:limit]

    def get_job_summary(self, job_name: str, days: int = 30) -> Dict[str, Any]:
        """Get summary statistics for a job."""
        start_date = datetime.utcnow() - timedelta(days=days)
        runs = self.get_runs_by_job(job_name, limit=1000, start_date=start_date)

        if not runs:
            return {"job_name": job_name, "total_runs": 0}

        successful = [r for r in runs if r.status == RunStatus.SUCCEEDED.value]
        failed = [r for r in runs if r.status == RunStatus.FAILED.value]

        total_cost = sum(float(r.estimated_cost_usd) for r in runs)
        total_duration = sum(r.duration_seconds for r in runs)
        avg_duration = total_duration / len(runs) if runs else 0

        return {
            "job_name": job_name,
            "period_days": days,
            "total_runs": len(runs),
            "successful_runs": len(successful),
            "failed_runs": len(failed),
            "success_rate": len(successful) / len(runs) if runs else 0,
            "total_cost_usd": round(total_cost, 2),
            "avg_cost_per_run": round(total_cost / len(runs), 2) if runs else 0,
            "avg_duration_seconds": round(avg_duration, 0),
            "total_rows_processed": sum(r.rows_read for r in runs),
            "avg_dq_score": sum(r.dq_score for r in runs) / len(runs) if runs else 0,
            "last_run": runs[0].started_at if runs else None,
            "last_status": runs[0].status if runs else None
        }

    def get_dashboard_data(self, hours: int = 24) -> Dict[str, Any]:
        """Get data for dashboard display."""
        runs = self.get_recent_runs(hours=hours, limit=500)

        # Calculate metrics
        total_runs = len(runs)
        successful = sum(1 for r in runs if r.status == RunStatus.SUCCEEDED.value)
        failed = sum(1 for r in runs if r.status == RunStatus.FAILED.value)
        running = sum(1 for r in runs if r.status == RunStatus.RUNNING.value)

        total_cost = sum(float(r.estimated_cost_usd) for r in runs)

        # Group by job
        jobs_summary = {}
        for run in runs:
            if run.job_name not in jobs_summary:
                jobs_summary[run.job_name] = {
                    "total": 0, "successful": 0, "failed": 0, "cost": 0
                }
            jobs_summary[run.job_name]["total"] += 1
            jobs_summary[run.job_name]["cost"] += float(run.estimated_cost_usd)
            if run.status == RunStatus.SUCCEEDED.value:
                jobs_summary[run.job_name]["successful"] += 1
            elif run.status == RunStatus.FAILED.value:
                jobs_summary[run.job_name]["failed"] += 1

        # Recent failures
        recent_failures = [
            {
                "job_name": r.job_name,
                "run_id": r.run_id,
                "started_at": r.started_at,
                "error": r.error_message[:200] if r.error_message else ""
            }
            for r in runs if r.status == RunStatus.FAILED.value
        ][:10]

        return {
            "period_hours": hours,
            "summary": {
                "total_runs": total_runs,
                "successful": successful,
                "failed": failed,
                "running": running,
                "success_rate": successful / total_runs if total_runs else 0,
                "total_cost_usd": round(total_cost, 2)
            },
            "jobs_summary": jobs_summary,
            "recent_failures": recent_failures,
            "recent_runs": [
                {
                    "job_name": r.job_name,
                    "run_id": r.run_id,
                    "status": r.status,
                    "started_at": r.started_at,
                    "duration_seconds": r.duration_seconds,
                    "cost_usd": float(r.estimated_cost_usd)
                }
                for r in runs[:20]
            ]
        }

    def _dict_to_audit(self, item: Dict[str, Any]) -> ETLRunAudit:
        """Convert DynamoDB item to ETLRunAudit."""
        # Convert Decimal to float
        from decimal import Decimal

        def convert_decimals(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            elif isinstance(obj, dict):
                return {k: convert_decimals(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_decimals(i) for i in obj]
            return obj

        item = convert_decimals(item)

        return ETLRunAudit(
            run_id=item.get('run_id', ''),
            job_name=item.get('job_name', ''),
            job_version=item.get('job_version', '1.0'),
            platform=item.get('platform', 'glue'),
            status=item.get('status', ''),
            started_at=item.get('started_at'),
            completed_at=item.get('completed_at'),
            duration_seconds=item.get('duration_seconds', 0),
            config_snapshot=item.get('config_snapshot', {}),
            spark_config=item.get('spark_config', {}),
            worker_type=item.get('worker_type', ''),
            number_of_workers=item.get('number_of_workers', 0),
            input_tables=item.get('input_tables', []),
            output_tables=item.get('output_tables', []),
            rows_read=item.get('rows_read', 0),
            rows_written=item.get('rows_written', 0),
            bytes_read=item.get('bytes_read', 0),
            bytes_written=item.get('bytes_written', 0),
            partitions_processed=item.get('partitions_processed', 0),
            shuffle_bytes=item.get('shuffle_bytes', 0),
            spill_bytes=item.get('spill_bytes', 0),
            peak_memory_mb=item.get('peak_memory_mb', 0),
            cpu_utilization_pct=item.get('cpu_utilization_pct', 0.0),
            dpu_seconds=item.get('dpu_seconds', 0),
            estimated_cost_usd=item.get('estimated_cost_usd', 0.0),
            dq_score=item.get('dq_score', 0.0),
            dq_rules_passed=item.get('dq_rules_passed', 0),
            dq_rules_failed=item.get('dq_rules_failed', 0),
            dq_details=item.get('dq_details', {}),
            error_message=item.get('error_message', ''),
            error_type=item.get('error_type', ''),
            error_stack_trace=item.get('error_stack_trace', ''),
            healing_applied=item.get('healing_applied', False),
            healing_actions=item.get('healing_actions', []),
            retry_count=item.get('retry_count', 0),
            recommendations=item.get('recommendations', []),
            platform_recommendation=item.get('platform_recommendation', ''),
            triggered_by=item.get('triggered_by', ''),
            trigger_source=item.get('trigger_source', ''),
            downstream_jobs=item.get('downstream_jobs', []),
            created_at=item.get('created_at', ''),
            updated_at=item.get('updated_at', ''),
            tags=item.get('tags', {})
        )


# Convenience functions for integration
def create_audit_manager(backend: str = "dynamodb",
                         region: str = "us-east-1",
                         table_prefix: str = "") -> ETLAuditManager:
    """Create an audit manager instance."""
    backend_enum = AuditBackend(backend)
    return ETLAuditManager(backend=backend_enum, region=region, table_prefix=table_prefix)


# Example usage
if __name__ == '__main__':
    # Initialize audit manager
    audit_manager = ETLAuditManager(
        backend=AuditBackend.DYNAMODB,
        region='us-east-1',
        table_prefix='dev_'
    )

    # Create tables (one-time setup)
    print("Creating audit tables...")
    results = audit_manager.create_tables()
    for table, success in results.items():
        print(f"  {table}: {'Created' if success else 'Failed'}")

    # Example: Start a run
    config = {
        'workload': {'name': 'customer_360_etl'},
        'execution': {'platform': 'glue', 'worker_type': 'G.1X', 'number_of_workers': 5},
        'data_sources': [
            {'database': 'ecommerce_db', 'table': 'orders'},
            {'database': 'ecommerce_db', 'table': 'customers'}
        ]
    }

    audit = audit_manager.start_run('customer_360_etl', config)
    print(f"\nStarted run: {audit.run_id}")

    # Simulate run completion
    audit.rows_read = 1000000
    audit.rows_written = 50000
    audit.dpu_seconds = 1800
    audit.dq_score = 0.95
    audit.dq_rules_passed = 19
    audit.dq_rules_failed = 1

    audit_manager.complete_run(audit, RunStatus.SUCCEEDED)
    print(f"Completed run: {audit.status}")

    # Get dashboard data
    print("\nDashboard Data:")
    dashboard = audit_manager.get_dashboard_data(hours=24)
    print(json.dumps(dashboard['summary'], indent=2))
