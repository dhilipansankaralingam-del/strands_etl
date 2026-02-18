#!/usr/bin/env python3
"""
Unified Audit Logger
====================

Centralized audit logging for all agent events with multi-backend support:
1. DynamoDB - for querying and real-time access
2. S3 - for data lake, Athena queries, and archival
3. Local JSON - for fallback and debugging

All events follow a consistent schema for dashboard generation.
"""

import os
import json
import gzip
import logging
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from enum import Enum
from pathlib import Path
from typing import Dict, List, Any, Optional
from decimal import Decimal

logger = logging.getLogger("strands.unified_audit")


class EventType(Enum):
    """Types of audit events."""
    # Agent events
    AGENT_START = "agent_start"
    AGENT_COMPLETE = "agent_complete"
    AGENT_ERROR = "agent_error"
    AGENT_SKIP = "agent_skip"

    # Job events
    JOB_START = "job_start"
    JOB_COMPLETE = "job_complete"
    JOB_ERROR = "job_error"

    # Data events
    DATA_READ = "data_read"
    DATA_WRITE = "data_write"
    DATA_QUALITY = "data_quality"

    # Platform events
    PLATFORM_CONVERSION = "platform_conversion"
    RESOURCE_ALLOCATION = "resource_allocation"

    # Compliance events
    COMPLIANCE_CHECK = "compliance_check"
    PII_DETECTED = "pii_detected"

    # Healing events
    HEALING_TRIGGERED = "healing_triggered"
    HEALING_APPLIED = "healing_applied"

    # Recommendation events
    RECOMMENDATION = "recommendation"

    # Learning events
    LEARNING_UPDATE = "learning_update"
    PREDICTION = "prediction"


@dataclass
class AuditEvent:
    """Unified audit event schema."""
    # Required fields
    event_id: str
    event_type: str
    timestamp: str
    job_name: str
    execution_id: str

    # Event details
    agent_name: Optional[str] = None
    agent_id: Optional[str] = None
    status: Optional[str] = None
    message: Optional[str] = None

    # Metrics
    duration_ms: Optional[float] = None
    records_read: Optional[int] = None
    records_written: Optional[int] = None
    bytes_processed: Optional[int] = None
    cost_usd: Optional[float] = None

    # Platform info
    platform: Optional[str] = None
    worker_type: Optional[str] = None
    worker_count: Optional[int] = None

    # Data quality
    dq_score: Optional[float] = None
    dq_rules_passed: Optional[int] = None
    dq_rules_failed: Optional[int] = None

    # Compliance
    compliance_status: Optional[str] = None
    pii_columns: Optional[List[str]] = None
    violations: Optional[List[str]] = None

    # Errors and recommendations
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    recommendations: Optional[List[str]] = None

    # Custom metadata
    metadata: Optional[Dict[str, Any]] = None

    # Partition keys for efficient querying
    event_date: Optional[str] = None  # YYYY-MM-DD for partitioning
    event_hour: Optional[str] = None  # HH for hourly analysis

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, removing None values."""
        result = {}
        for k, v in asdict(self).items():
            if v is not None:
                if isinstance(v, Enum):
                    result[k] = v.value
                elif isinstance(v, Decimal):
                    result[k] = float(v)
                else:
                    result[k] = v
        return result

    def to_dynamodb_item(self) -> Dict[str, Any]:
        """Convert to DynamoDB-compatible format."""
        item = self.to_dict()
        # Ensure partition key exists
        item['pk'] = f"JOB#{self.job_name}"
        item['sk'] = f"EVENT#{self.timestamp}#{self.event_id}"
        # GSI for event type queries
        item['gsi1pk'] = f"TYPE#{self.event_type}"
        item['gsi1sk'] = f"DATE#{self.event_date}#{self.timestamp}"
        return item


class UnifiedAuditLogger:
    """
    Centralized audit logger with multi-backend support.

    Writes all events to:
    - DynamoDB (for real-time queries)
    - S3 (for data lake and Athena)
    - Local files (for fallback)
    """

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        storage_config = self.config.get('storage', {})
        audit_config = self.config.get('audit', {})

        # Storage settings
        self.primary_backend = audit_config.get('primary_storage', storage_config.get('primary_backend', 'local'))
        self.s3_bucket = storage_config.get('s3_bucket', audit_config.get('s3_bucket', ''))
        self.s3_prefix = storage_config.get('s3_prefix', 'audit-events/')
        self.dynamo_table = audit_config.get('dynamo_table', 'etl_unified_audit')
        self.dynamo_region = storage_config.get('dynamo_region', 'us-east-1')
        self.local_path = Path(storage_config.get('local_base_path', 'data/agent_store')) / 'unified_audit'

        # Enable/disable backends
        self.enable_dynamodb = audit_config.get('enable_dynamodb', True) and self.primary_backend in ('dynamodb', 's3')
        self.enable_s3 = audit_config.get('enable_s3', True) and self.s3_bucket
        self.enable_local = audit_config.get('enable_local', True)

        # Create local directory
        self.local_path.mkdir(parents=True, exist_ok=True)

        # Batch settings for S3
        self.batch_size = audit_config.get('batch_size', 100)
        self.batch_interval_seconds = audit_config.get('batch_interval_seconds', 60)

        # Event buffer for batching
        self._event_buffer: List[AuditEvent] = []
        self._buffer_lock = threading.Lock()

        # Lazy-loaded clients
        self._dynamodb = None
        self._s3 = None

        logger.info(f"UnifiedAuditLogger initialized: dynamodb={self.enable_dynamodb}, s3={self.enable_s3}, local={self.enable_local}")

    @property
    def dynamodb(self):
        """Lazy-load DynamoDB client."""
        if self._dynamodb is None and self.enable_dynamodb:
            try:
                import boto3
                self._dynamodb = boto3.resource('dynamodb', region_name=self.dynamo_region)
            except Exception as e:
                logger.warning(f"Failed to connect to DynamoDB: {e}")
                self.enable_dynamodb = False
        return self._dynamodb

    @property
    def s3(self):
        """Lazy-load S3 client."""
        if self._s3 is None and self.enable_s3:
            try:
                import boto3
                self._s3 = boto3.client('s3')
            except Exception as e:
                logger.warning(f"Failed to connect to S3: {e}")
                self.enable_s3 = False
        return self._s3

    def log_event(
        self,
        event_type: EventType,
        job_name: str,
        execution_id: str,
        agent_name: str = None,
        agent_id: str = None,
        status: str = None,
        message: str = None,
        metrics: Dict[str, Any] = None,
        metadata: Dict[str, Any] = None,
        **kwargs
    ) -> AuditEvent:
        """
        Log an audit event to all enabled backends.

        Args:
            event_type: Type of event
            job_name: Name of the job
            execution_id: Unique execution ID
            agent_name: Name of the agent (optional)
            agent_id: Agent instance ID (optional)
            status: Event status
            message: Event message
            metrics: Event metrics dict
            metadata: Additional metadata
            **kwargs: Additional event fields

        Returns:
            The created AuditEvent
        """
        import uuid

        now = datetime.utcnow()

        # Build event
        event = AuditEvent(
            event_id=str(uuid.uuid4())[:8],
            event_type=event_type.value if isinstance(event_type, EventType) else event_type,
            timestamp=now.isoformat() + 'Z',
            job_name=job_name,
            execution_id=execution_id,
            agent_name=agent_name,
            agent_id=agent_id,
            status=status,
            message=message,
            event_date=now.strftime('%Y-%m-%d'),
            event_hour=now.strftime('%H'),
            metadata=metadata
        )

        # Apply metrics
        if metrics:
            if 'duration_ms' in metrics:
                event.duration_ms = metrics['duration_ms']
            if 'records_read' in metrics:
                event.records_read = metrics['records_read']
            if 'records_written' in metrics:
                event.records_written = metrics['records_written']
            if 'bytes_processed' in metrics:
                event.bytes_processed = metrics['bytes_processed']
            if 'cost_usd' in metrics:
                event.cost_usd = metrics['cost_usd']

        # Apply kwargs
        for key, value in kwargs.items():
            if hasattr(event, key):
                setattr(event, key, value)

        # Write to backends
        self._write_to_backends(event)

        return event

    def log_agent_start(self, job_name: str, execution_id: str, agent_name: str, agent_id: str, **kwargs) -> AuditEvent:
        """Log agent start event."""
        return self.log_event(
            EventType.AGENT_START,
            job_name=job_name,
            execution_id=execution_id,
            agent_name=agent_name,
            agent_id=agent_id,
            status='started',
            message=f"Agent {agent_name} started",
            **kwargs
        )

    def log_agent_complete(
        self,
        job_name: str,
        execution_id: str,
        agent_name: str,
        agent_id: str,
        duration_ms: float,
        output: Dict[str, Any] = None,
        recommendations: List[str] = None,
        **kwargs
    ) -> AuditEvent:
        """Log agent completion event."""
        return self.log_event(
            EventType.AGENT_COMPLETE,
            job_name=job_name,
            execution_id=execution_id,
            agent_name=agent_name,
            agent_id=agent_id,
            status='completed',
            message=f"Agent {agent_name} completed in {duration_ms:.0f}ms",
            metrics={'duration_ms': duration_ms},
            recommendations=recommendations,
            metadata={'output': output},
            **kwargs
        )

    def log_agent_error(
        self,
        job_name: str,
        execution_id: str,
        agent_name: str,
        agent_id: str,
        error: str,
        error_type: str = None,
        **kwargs
    ) -> AuditEvent:
        """Log agent error event."""
        return self.log_event(
            EventType.AGENT_ERROR,
            job_name=job_name,
            execution_id=execution_id,
            agent_name=agent_name,
            agent_id=agent_id,
            status='failed',
            message=f"Agent {agent_name} failed: {error}",
            error_type=error_type or 'unknown',
            error_message=error,
            **kwargs
        )

    def log_agent_skip(
        self,
        job_name: str,
        execution_id: str,
        agent_name: str,
        agent_id: str,
        reason: str,
        **kwargs
    ) -> AuditEvent:
        """Log agent skip event."""
        return self.log_event(
            EventType.AGENT_SKIP,
            job_name=job_name,
            execution_id=execution_id,
            agent_name=agent_name,
            agent_id=agent_id,
            status='skipped',
            message=f"Agent {agent_name} skipped: {reason}",
            **kwargs
        )

    def log_data_quality(
        self,
        job_name: str,
        execution_id: str,
        score: float,
        rules_passed: int,
        rules_failed: int,
        **kwargs
    ) -> AuditEvent:
        """Log data quality check event."""
        status = 'passed' if rules_failed == 0 else 'warning' if score > 80 else 'failed'
        return self.log_event(
            EventType.DATA_QUALITY,
            job_name=job_name,
            execution_id=execution_id,
            status=status,
            message=f"DQ Score: {score:.1f}% ({rules_passed} passed, {rules_failed} failed)",
            dq_score=score,
            dq_rules_passed=rules_passed,
            dq_rules_failed=rules_failed,
            **kwargs
        )

    def log_platform_conversion(
        self,
        job_name: str,
        execution_id: str,
        source_platform: str,
        target_platform: str,
        reason: str,
        **kwargs
    ) -> AuditEvent:
        """Log platform conversion event."""
        return self.log_event(
            EventType.PLATFORM_CONVERSION,
            job_name=job_name,
            execution_id=execution_id,
            status='converted' if source_platform != target_platform else 'unchanged',
            message=f"Platform: {source_platform} -> {target_platform} ({reason})",
            platform=target_platform,
            metadata={'source_platform': source_platform, 'reason': reason},
            **kwargs
        )

    def log_recommendation(
        self,
        job_name: str,
        execution_id: str,
        agent_name: str,
        recommendation: str,
        priority: str = 'medium',
        **kwargs
    ) -> AuditEvent:
        """Log a recommendation event."""
        return self.log_event(
            EventType.RECOMMENDATION,
            job_name=job_name,
            execution_id=execution_id,
            agent_name=agent_name,
            status=priority,
            message=recommendation,
            recommendations=[recommendation],
            **kwargs
        )

    def log_compliance(
        self,
        job_name: str,
        execution_id: str,
        status: str,
        pii_columns: List[str] = None,
        violations: List[str] = None,
        frameworks: List[str] = None,
        **kwargs
    ) -> AuditEvent:
        """Log compliance check event."""
        return self.log_event(
            EventType.COMPLIANCE_CHECK,
            job_name=job_name,
            execution_id=execution_id,
            status=status,
            message=f"Compliance: {status} ({len(pii_columns or [])} PII columns, {len(violations or [])} violations)",
            compliance_status=status,
            pii_columns=pii_columns,
            violations=violations,
            metadata={'frameworks': frameworks},
            **kwargs
        )

    def _write_to_backends(self, event: AuditEvent) -> None:
        """Write event to all enabled backends."""
        event_dict = event.to_dict()

        # Write to local (always, for fallback)
        if self.enable_local:
            self._write_to_local(event_dict)

        # Write to DynamoDB
        if self.enable_dynamodb:
            try:
                self._write_to_dynamodb(event)
            except Exception as e:
                logger.warning(f"DynamoDB write failed: {e}")

        # Buffer for S3 batch write
        if self.enable_s3:
            with self._buffer_lock:
                self._event_buffer.append(event)
                if len(self._event_buffer) >= self.batch_size:
                    self._flush_to_s3()

    def _write_to_local(self, event_dict: Dict[str, Any]) -> None:
        """Write event to local JSON file."""
        try:
            date_str = event_dict.get('event_date', datetime.utcnow().strftime('%Y-%m-%d'))
            file_path = self.local_path / f"audit_{date_str}.jsonl"

            with open(file_path, 'a') as f:
                f.write(json.dumps(event_dict, default=str) + '\n')
        except Exception as e:
            logger.error(f"Local write failed: {e}")

    def _write_to_dynamodb(self, event: AuditEvent) -> None:
        """Write event to DynamoDB."""
        if not self.dynamodb:
            return

        try:
            table = self.dynamodb.Table(self.dynamo_table)
            table.put_item(Item=event.to_dynamodb_item())
        except Exception as e:
            # Try to create table if it doesn't exist
            if 'ResourceNotFoundException' in str(e):
                self._create_dynamodb_table()
                # Retry
                table = self.dynamodb.Table(self.dynamo_table)
                table.put_item(Item=event.to_dynamodb_item())
            else:
                raise

    def _create_dynamodb_table(self) -> None:
        """Create DynamoDB table if it doesn't exist."""
        if not self.dynamodb:
            return

        try:
            self.dynamodb.create_table(
                TableName=self.dynamo_table,
                KeySchema=[
                    {'AttributeName': 'pk', 'KeyType': 'HASH'},
                    {'AttributeName': 'sk', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'pk', 'AttributeType': 'S'},
                    {'AttributeName': 'sk', 'AttributeType': 'S'},
                    {'AttributeName': 'gsi1pk', 'AttributeType': 'S'},
                    {'AttributeName': 'gsi1sk', 'AttributeType': 'S'}
                ],
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'gsi1',
                        'KeySchema': [
                            {'AttributeName': 'gsi1pk', 'KeyType': 'HASH'},
                            {'AttributeName': 'gsi1sk', 'KeyType': 'RANGE'}
                        ],
                        'Projection': {'ProjectionType': 'ALL'},
                        'ProvisionedThroughput': {
                            'ReadCapacityUnits': 5,
                            'WriteCapacityUnits': 5
                        }
                    }
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            )
            logger.info(f"Created DynamoDB table: {self.dynamo_table}")
        except Exception as e:
            logger.warning(f"Failed to create DynamoDB table: {e}")

    def _flush_to_s3(self) -> None:
        """Flush buffered events to S3."""
        if not self._event_buffer or not self.s3:
            return

        with self._buffer_lock:
            events_to_write = self._event_buffer.copy()
            self._event_buffer.clear()

        if not events_to_write:
            return

        try:
            now = datetime.utcnow()
            # Partition by date and hour for efficient querying
            s3_key = (
                f"{self.s3_prefix}"
                f"year={now.strftime('%Y')}/"
                f"month={now.strftime('%m')}/"
                f"day={now.strftime('%d')}/"
                f"hour={now.strftime('%H')}/"
                f"events_{now.strftime('%Y%m%d_%H%M%S')}.json.gz"
            )

            # Write as newline-delimited JSON (NDJSON) - works with Athena
            content = '\n'.join(json.dumps(e.to_dict(), default=str) for e in events_to_write)
            compressed = gzip.compress(content.encode('utf-8'))

            self.s3.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=compressed,
                ContentType='application/gzip'
            )

            logger.info(f"Wrote {len(events_to_write)} events to s3://{self.s3_bucket}/{s3_key}")
        except Exception as e:
            logger.error(f"S3 write failed: {e}")
            # Re-add events to buffer for retry
            with self._buffer_lock:
                self._event_buffer.extend(events_to_write)

    def flush(self) -> None:
        """Flush all buffered events to S3."""
        if self.enable_s3:
            self._flush_to_s3()

    def get_events(
        self,
        job_name: str = None,
        event_type: str = None,
        start_date: str = None,
        end_date: str = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get events from local storage (for dashboard).

        Args:
            job_name: Filter by job name
            event_type: Filter by event type
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Maximum events to return

        Returns:
            List of events
        """
        events = []

        # Get date range
        if not end_date:
            end_date = datetime.utcnow().strftime('%Y-%m-%d')
        if not start_date:
            start_date = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')

        # Read from local files
        for file_path in sorted(self.local_path.glob('audit_*.jsonl'), reverse=True):
            file_date = file_path.stem.replace('audit_', '')
            if file_date < start_date or file_date > end_date:
                continue

            try:
                with open(file_path, 'r') as f:
                    for line in f:
                        if not line.strip():
                            continue
                        event = json.loads(line)

                        # Apply filters
                        if job_name and event.get('job_name') != job_name:
                            continue
                        if event_type and event.get('event_type') != event_type:
                            continue

                        events.append(event)
                        if len(events) >= limit:
                            return events
            except Exception as e:
                logger.warning(f"Failed to read {file_path}: {e}")

        return events

    def get_dashboard_data(self, days: int = 7) -> Dict[str, Any]:
        """
        Get aggregated data for dashboard generation.

        Returns data suitable for EnterpriseDashboard.
        """
        end_date = datetime.utcnow().strftime('%Y-%m-%d')
        start_date = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')

        events = self.get_events(start_date=start_date, end_date=end_date, limit=10000)

        # Aggregate job history
        job_runs = {}
        for event in events:
            if event.get('event_type') == 'job_complete':
                job_name = event.get('job_name')
                if job_name not in job_runs:
                    job_runs[job_name] = []
                job_runs[job_name].append({
                    'name': job_name,
                    'status': event.get('status', 'unknown'),
                    'duration_seconds': (event.get('duration_ms') or 0) / 1000,
                    'cost': event.get('cost_usd') or 0,
                    'records': event.get('records_written') or 0,
                    'platform': event.get('platform') or 'glue',
                    'timestamp': event.get('timestamp')
                })

        # Flatten job history
        job_history = []
        for runs in job_runs.values():
            job_history.extend(runs)
        job_history.sort(key=lambda x: x.get('timestamp', ''), reverse=True)

        # Aggregate DQ results
        dq_results = []
        for event in events:
            if event.get('event_type') == 'data_quality':
                dq_results.append({
                    'job_name': event.get('job_name'),
                    'score': event.get('dq_score') or 0,
                    'passed': event.get('dq_rules_passed') or 0,
                    'failed': event.get('dq_rules_failed') or 0,
                    'status': event.get('status'),
                    'timestamp': event.get('timestamp')
                })

        # Aggregate compliance results
        compliance_results = []
        for event in events:
            if event.get('event_type') == 'compliance_check':
                compliance_results.append({
                    'job_name': event.get('job_name'),
                    'status': event.get('compliance_status') or event.get('status'),
                    'pii_columns': len(event.get('pii_columns') or []),
                    'violations': len(event.get('violations') or []),
                    'frameworks': (event.get('metadata') or {}).get('frameworks', []),
                    'timestamp': event.get('timestamp')
                })

        # Aggregate recommendations
        recommendations = []
        for event in events:
            if event.get('event_type') == 'recommendation':
                recommendations.append({
                    'title': event.get('message'),
                    'description': event.get('message'),
                    'priority': event.get('status', 'medium'),
                    'source': event.get('agent_name', 'unknown')
                })

        # Aggregate cost data
        cost_data = []
        for event in events:
            if event.get('cost_usd'):
                cost_data.append({
                    'platform': event.get('platform', 'glue'),
                    'cost': event.get('cost_usd')
                })

        # Aggregate predictions
        predictions = []
        for event in events:
            if event.get('event_type') == 'prediction':
                predictions.append({
                    'job_name': event.get('job_name'),
                    'probability': (event.get('metadata') or {}).get('failure_probability', 0),
                    'factors': (event.get('metadata') or {}).get('factors', [])
                })

        return {
            'job_history': job_history[:50],
            'dq_results': dq_results[:20],
            'compliance_results': compliance_results[:20],
            'recommendations': recommendations[:20],
            'cost_data': cost_data,
            'predictions': predictions[:10]
        }


# Global instance (lazy-loaded)
_audit_logger: Optional[UnifiedAuditLogger] = None
_audit_lock = threading.Lock()


def get_audit_logger(config: Dict[str, Any] = None) -> UnifiedAuditLogger:
    """Get or create the global audit logger instance."""
    global _audit_logger

    with _audit_lock:
        if _audit_logger is None:
            _audit_logger = UnifiedAuditLogger(config)
        return _audit_logger


def set_audit_logger(logger: UnifiedAuditLogger) -> None:
    """Set the global audit logger instance."""
    global _audit_logger

    with _audit_lock:
        _audit_logger = logger
