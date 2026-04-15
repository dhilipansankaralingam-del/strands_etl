#!/usr/bin/env python3
"""
Strands SDK Storage
===================

Flexible storage backend with fallback support:
1. DynamoDB (primary)
2. S3 (secondary)
3. Local pipe-delimited files (fallback)

All agent data, audit logs, learning data, and recommendations
are persisted through this unified storage interface.
"""

import os
import json
import csv
import gzip
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime, date
from enum import Enum
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import threading


class StorageBackend(Enum):
    """Available storage backends."""
    DYNAMODB = "dynamodb"
    S3 = "s3"
    LOCAL = "local"


@dataclass
class StorageConfig:
    """Configuration for storage."""
    primary_backend: StorageBackend = StorageBackend.LOCAL
    fallback_backend: StorageBackend = StorageBackend.LOCAL

    # DynamoDB settings
    dynamo_table_prefix: str = "etl_"
    dynamo_region: str = "us-east-1"

    # S3 settings
    s3_bucket: str = ""
    s3_prefix: str = "etl-data/"

    # Local settings
    local_base_path: str = "data/agent_store"
    local_format: str = "pipe"  # "pipe", "json", "csv"

    # Retry settings
    max_retries: int = 3
    retry_delay_seconds: float = 1.0


class StorageError(Exception):
    """Storage operation error."""
    pass


class BaseStorage(ABC):
    """Abstract base class for storage backends."""

    @abstractmethod
    def write(self, table: str, data: Dict[str, Any], key: Dict[str, str]) -> bool:
        """Write a record."""
        pass

    @abstractmethod
    def read(self, table: str, key: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Read a record."""
        pass

    @abstractmethod
    def query(
        self,
        table: str,
        partition_key: str,
        partition_value: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Query records by partition key."""
        pass

    @abstractmethod
    def batch_write(self, table: str, records: List[Dict[str, Any]]) -> int:
        """Write multiple records. Returns count written."""
        pass


class DynamoDBStorage(BaseStorage):
    """DynamoDB storage backend."""

    def __init__(self, config: StorageConfig):
        self.config = config
        self.logger = logging.getLogger("strands.storage.dynamodb")
        self._client = None

    @property
    def client(self):
        if self._client is None:
            try:
                import boto3
                self._client = boto3.resource(
                    'dynamodb',
                    region_name=self.config.dynamo_region
                )
            except Exception as e:
                self.logger.error(f"Failed to connect to DynamoDB: {e}")
                raise StorageError(f"DynamoDB connection failed: {e}")
        return self._client

    def _get_table(self, table_name: str):
        full_name = f"{self.config.dynamo_table_prefix}{table_name}"
        return self.client.Table(full_name)

    def write(self, table: str, data: Dict[str, Any], key: Dict[str, str]) -> bool:
        try:
            tbl = self._get_table(table)
            item = {**key, **data}
            # Convert datetime objects
            item = self._serialize(item)
            tbl.put_item(Item=item)
            return True
        except Exception as e:
            self.logger.error(f"DynamoDB write failed: {e}")
            raise StorageError(f"Write failed: {e}")

    def read(self, table: str, key: Dict[str, str]) -> Optional[Dict[str, Any]]:
        try:
            tbl = self._get_table(table)
            response = tbl.get_item(Key=key)
            return response.get('Item')
        except Exception as e:
            self.logger.error(f"DynamoDB read failed: {e}")
            raise StorageError(f"Read failed: {e}")

    def query(
        self,
        table: str,
        partition_key: str,
        partition_value: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        try:
            from boto3.dynamodb.conditions import Key
            tbl = self._get_table(table)
            response = tbl.query(
                KeyConditionExpression=Key(partition_key).eq(partition_value),
                Limit=limit
            )
            return response.get('Items', [])
        except Exception as e:
            self.logger.error(f"DynamoDB query failed: {e}")
            raise StorageError(f"Query failed: {e}")

    def batch_write(self, table: str, records: List[Dict[str, Any]]) -> int:
        try:
            tbl = self._get_table(table)
            with tbl.batch_writer() as batch:
                for record in records:
                    batch.put_item(Item=self._serialize(record))
            return len(records)
        except Exception as e:
            self.logger.error(f"DynamoDB batch write failed: {e}")
            raise StorageError(f"Batch write failed: {e}")

    def _serialize(self, data: Dict) -> Dict:
        """Convert Python objects to DynamoDB-compatible types."""
        result = {}
        for k, v in data.items():
            if isinstance(v, datetime):
                result[k] = v.isoformat()
            elif isinstance(v, date):
                result[k] = v.isoformat()
            elif isinstance(v, dict):
                result[k] = self._serialize(v)
            elif isinstance(v, list):
                result[k] = [
                    self._serialize(i) if isinstance(i, dict) else i
                    for i in v
                ]
            else:
                result[k] = v
        return result


class S3Storage(BaseStorage):
    """S3 storage backend with pipe-delimited file support."""

    def __init__(self, config: StorageConfig):
        self.config = config
        self.logger = logging.getLogger("strands.storage.s3")
        self._client = None

    @property
    def client(self):
        if self._client is None:
            try:
                import boto3
                self._client = boto3.client('s3')
            except Exception as e:
                self.logger.error(f"Failed to connect to S3: {e}")
                raise StorageError(f"S3 connection failed: {e}")
        return self._client

    def _get_key(self, table: str, key: Dict[str, str]) -> str:
        """Build S3 key from table and key."""
        key_str = "_".join(f"{k}={v}" for k, v in sorted(key.items()))
        return f"{self.config.s3_prefix}{table}/{key_str}.json"

    def _get_partition_prefix(self, table: str, partition_value: str) -> str:
        """Build S3 prefix for partition queries."""
        return f"{self.config.s3_prefix}{table}/{partition_value}/"

    def write(self, table: str, data: Dict[str, Any], key: Dict[str, str]) -> bool:
        try:
            s3_key = self._get_key(table, key)
            body = json.dumps({**key, **data}, default=str)
            self.client.put_object(
                Bucket=self.config.s3_bucket,
                Key=s3_key,
                Body=body.encode('utf-8'),
                ContentType='application/json'
            )
            return True
        except Exception as e:
            self.logger.error(f"S3 write failed: {e}")
            raise StorageError(f"Write failed: {e}")

    def read(self, table: str, key: Dict[str, str]) -> Optional[Dict[str, Any]]:
        try:
            s3_key = self._get_key(table, key)
            response = self.client.get_object(
                Bucket=self.config.s3_bucket,
                Key=s3_key
            )
            body = response['Body'].read().decode('utf-8')
            return json.loads(body)
        except self.client.exceptions.NoSuchKey:
            return None
        except Exception as e:
            self.logger.error(f"S3 read failed: {e}")
            raise StorageError(f"Read failed: {e}")

    def query(
        self,
        table: str,
        partition_key: str,
        partition_value: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        try:
            prefix = f"{self.config.s3_prefix}{table}/"
            results = []

            paginator = self.client.get_paginator('list_objects_v2')
            for page in paginator.paginate(
                Bucket=self.config.s3_bucket,
                Prefix=prefix,
                MaxKeys=limit
            ):
                for obj in page.get('Contents', []):
                    if partition_value in obj['Key']:
                        response = self.client.get_object(
                            Bucket=self.config.s3_bucket,
                            Key=obj['Key']
                        )
                        body = response['Body'].read().decode('utf-8')
                        results.append(json.loads(body))
                        if len(results) >= limit:
                            break
                if len(results) >= limit:
                    break

            return results
        except Exception as e:
            self.logger.error(f"S3 query failed: {e}")
            raise StorageError(f"Query failed: {e}")

    def batch_write(self, table: str, records: List[Dict[str, Any]]) -> int:
        count = 0
        for record in records:
            # Extract key fields (assume first two items)
            key = {}
            data = {}
            for i, (k, v) in enumerate(record.items()):
                if i < 2:
                    key[k] = str(v)
                else:
                    data[k] = v
            if self.write(table, data, key):
                count += 1
        return count

    def write_pipe_delimited(
        self,
        table: str,
        records: List[Dict[str, Any]],
        partition_date: datetime = None
    ) -> str:
        """
        Write records as pipe-delimited file to S3.

        Returns the S3 key of the written file.
        """
        if not records:
            return ""

        partition_date = partition_date or datetime.utcnow()
        date_str = partition_date.strftime('%Y/%m/%d')
        timestamp = partition_date.strftime('%Y%m%d_%H%M%S')

        s3_key = f"{self.config.s3_prefix}{table}/{date_str}/{table}_{timestamp}.psv.gz"

        # Build pipe-delimited content
        headers = list(records[0].keys())
        lines = ["|".join(headers)]

        for record in records:
            values = []
            for h in headers:
                val = record.get(h, "")
                if val is None:
                    val = ""
                elif isinstance(val, (dict, list)):
                    val = json.dumps(val)
                else:
                    val = str(val).replace("|", "\\|").replace("\n", "\\n")
                values.append(val)
            lines.append("|".join(values))

        content = "\n".join(lines)

        # Gzip and upload
        compressed = gzip.compress(content.encode('utf-8'))
        self.client.put_object(
            Bucket=self.config.s3_bucket,
            Key=s3_key,
            Body=compressed,
            ContentType='application/gzip'
        )

        self.logger.info(f"Wrote {len(records)} records to s3://{self.config.s3_bucket}/{s3_key}")
        return s3_key


class LocalStorage(BaseStorage):
    """Local file storage backend with pipe-delimited support."""

    def __init__(self, config: StorageConfig):
        self.config = config
        self.logger = logging.getLogger("strands.storage.local")
        self.base_path = Path(config.local_base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self._locks: Dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()

    def _get_lock(self, table: str) -> threading.Lock:
        with self._global_lock:
            if table not in self._locks:
                self._locks[table] = threading.Lock()
            return self._locks[table]

    def _get_table_path(self, table: str) -> Path:
        path = self.base_path / table
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _get_file_path(self, table: str, key: Dict[str, str]) -> Path:
        key_str = "_".join(f"{k}={v}" for k, v in sorted(key.items()))
        safe_key = "".join(c if c.isalnum() or c in "._=-" else "_" for c in key_str)
        return self._get_table_path(table) / f"{safe_key}.json"

    def write(self, table: str, data: Dict[str, Any], key: Dict[str, str]) -> bool:
        try:
            with self._get_lock(table):
                file_path = self._get_file_path(table, key)
                record = {**key, **data}
                with open(file_path, 'w') as f:
                    json.dump(record, f, default=str, indent=2)
                return True
        except Exception as e:
            self.logger.error(f"Local write failed: {e}")
            raise StorageError(f"Write failed: {e}")

    def read(self, table: str, key: Dict[str, str]) -> Optional[Dict[str, Any]]:
        try:
            file_path = self._get_file_path(table, key)
            if not file_path.exists():
                return None
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Local read failed: {e}")
            raise StorageError(f"Read failed: {e}")

    def query(
        self,
        table: str,
        partition_key: str,
        partition_value: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        try:
            table_path = self._get_table_path(table)
            results = []

            for file_path in table_path.glob("*.json"):
                if len(results) >= limit:
                    break
                try:
                    with open(file_path, 'r') as f:
                        record = json.load(f)
                        if record.get(partition_key) == partition_value:
                            results.append(record)
                except Exception:
                    continue

            return results
        except Exception as e:
            self.logger.error(f"Local query failed: {e}")
            raise StorageError(f"Query failed: {e}")

    def batch_write(self, table: str, records: List[Dict[str, Any]]) -> int:
        count = 0
        for record in records:
            # Extract key from first two fields
            key = {}
            data = {}
            for i, (k, v) in enumerate(record.items()):
                if i < 2:
                    key[k] = str(v)
                else:
                    data[k] = v
            try:
                if self.write(table, data, key):
                    count += 1
            except Exception:
                pass
        return count

    def write_pipe_delimited(
        self,
        table: str,
        records: List[Dict[str, Any]],
        partition_date: datetime = None
    ) -> str:
        """
        Write records as pipe-delimited file locally.

        Returns the path of the written file.
        """
        if not records:
            return ""

        partition_date = partition_date or datetime.utcnow()
        date_str = partition_date.strftime('%Y_%m_%d')
        timestamp = partition_date.strftime('%Y%m%d_%H%M%S')

        table_path = self._get_table_path(table)
        file_path = table_path / f"{table}_{date_str}_{timestamp}.psv"

        # Build pipe-delimited content
        headers = list(records[0].keys())

        with open(file_path, 'w', newline='') as f:
            f.write("|".join(headers) + "\n")
            for record in records:
                values = []
                for h in headers:
                    val = record.get(h, "")
                    if val is None:
                        val = ""
                    elif isinstance(val, (dict, list)):
                        val = json.dumps(val)
                    else:
                        val = str(val).replace("|", "\\|").replace("\n", "\\n")
                    values.append(val)
                f.write("|".join(values) + "\n")

        self.logger.info(f"Wrote {len(records)} records to {file_path}")
        return str(file_path)

    def append_audit_log(
        self,
        job_name: str,
        event_type: str,
        event_data: Dict[str, Any],
        timestamp: datetime = None
    ) -> str:
        """
        Append to audit log file (pipe-delimited).

        Creates daily audit log files.
        """
        timestamp = timestamp or datetime.utcnow()
        date_str = timestamp.strftime('%Y_%m_%d')

        audit_dir = self.base_path / "audit_logs"
        audit_dir.mkdir(parents=True, exist_ok=True)

        file_path = audit_dir / f"audit_{date_str}.psv"

        # Build record
        record = {
            'timestamp': timestamp.isoformat(),
            'job_name': job_name,
            'event_type': event_type,
            'event_data': json.dumps(event_data)
        }

        with self._get_lock('audit_logs'):
            file_exists = file_path.exists()

            with open(file_path, 'a', newline='') as f:
                if not file_exists:
                    f.write("|".join(record.keys()) + "\n")
                values = [str(v).replace("|", "\\|") for v in record.values()]
                f.write("|".join(values) + "\n")

        return str(file_path)


class StrandsStorage:
    """
    Unified storage interface with automatic fallback.

    Tries primary backend first, falls back on error.
    """

    def __init__(self, config: Union[StorageConfig, Dict[str, Any]] = None):
        if isinstance(config, dict):
            storage_config = config.get('storage', {})
            self.config = StorageConfig(
                primary_backend=StorageBackend(storage_config.get('primary_backend', 'local')),
                fallback_backend=StorageBackend(storage_config.get('fallback_backend', 'local')),
                dynamo_table_prefix=storage_config.get('dynamo_table_prefix', 'etl_'),
                dynamo_region=storage_config.get('dynamo_region', 'us-east-1'),
                s3_bucket=storage_config.get('s3_bucket', ''),
                s3_prefix=storage_config.get('s3_prefix', 'etl-data/'),
                local_base_path=storage_config.get('local_base_path', 'data/agent_store'),
                local_format=storage_config.get('local_format', 'pipe')
            )
        elif config:
            self.config = config
        else:
            self.config = StorageConfig()

        self.logger = logging.getLogger("strands.storage")
        self._backends: Dict[StorageBackend, BaseStorage] = {}

    def _get_backend(self, backend_type: StorageBackend) -> BaseStorage:
        """Get or create a storage backend."""
        if backend_type not in self._backends:
            if backend_type == StorageBackend.DYNAMODB:
                self._backends[backend_type] = DynamoDBStorage(self.config)
            elif backend_type == StorageBackend.S3:
                self._backends[backend_type] = S3Storage(self.config)
            else:
                self._backends[backend_type] = LocalStorage(self.config)

        return self._backends[backend_type]

    def _with_fallback(self, operation: str, func, *args, **kwargs) -> Any:
        """Execute operation with fallback."""
        # Try primary
        try:
            backend = self._get_backend(self.config.primary_backend)
            return func(backend, *args, **kwargs)
        except StorageError as e:
            self.logger.warning(f"Primary {operation} failed, trying fallback: {e}")

        # Try fallback
        try:
            backend = self._get_backend(self.config.fallback_backend)
            return func(backend, *args, **kwargs)
        except StorageError as e:
            self.logger.error(f"Fallback {operation} also failed: {e}")
            raise

    def write(self, table: str, data: Dict[str, Any], key: Dict[str, str]) -> bool:
        """Write a record with fallback."""
        return self._with_fallback(
            "write",
            lambda b, *a, **k: b.write(*a, **k),
            table, data, key
        )

    def read(self, table: str, key: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Read a record with fallback."""
        return self._with_fallback(
            "read",
            lambda b, *a, **k: b.read(*a, **k),
            table, key
        )

    def query(
        self,
        table: str,
        partition_key: str,
        partition_value: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Query records with fallback."""
        return self._with_fallback(
            "query",
            lambda b, *a, **k: b.query(*a, **k),
            table, partition_key, partition_value, limit
        )

    def batch_write(self, table: str, records: List[Dict[str, Any]]) -> int:
        """Batch write with fallback."""
        return self._with_fallback(
            "batch_write",
            lambda b, *a, **k: b.batch_write(*a, **k),
            table, records
        )

    def write_audit_log(
        self,
        job_name: str,
        event_type: str,
        event_data: Dict[str, Any]
    ) -> str:
        """
        Write audit log entry.

        Uses pipe-delimited format for better compatibility.
        Falls back to local if S3/DynamoDB unavailable.
        """
        try:
            # Try local first for audit (fastest)
            local = self._get_backend(StorageBackend.LOCAL)
            if isinstance(local, LocalStorage):
                return local.append_audit_log(job_name, event_type, event_data)
        except Exception as e:
            self.logger.warning(f"Local audit failed: {e}")

        # Fallback to standard write
        timestamp = datetime.utcnow()
        key = {
            'job_name': job_name,
            'timestamp': timestamp.isoformat()
        }
        data = {
            'event_type': event_type,
            'event_data': event_data
        }
        self.write('audit_logs', data, key)
        return f"audit_logs/{job_name}/{timestamp.isoformat()}"

    def store_agent_data(
        self,
        agent_name: str,
        data_type: str,
        records: List[Dict[str, Any]],
        use_pipe_delimited: bool = True
    ) -> str:
        """
        Store agent-specific data.

        Args:
            agent_name: Name of the agent
            data_type: Type of data (learning, recommendations, etc.)
            records: Data records to store
            use_pipe_delimited: Use pipe-delimited format

        Returns:
            Path/key where data was stored
        """
        table = f"{agent_name}_{data_type}"

        if use_pipe_delimited:
            try:
                # Try local first
                local = self._get_backend(StorageBackend.LOCAL)
                if isinstance(local, LocalStorage):
                    return local.write_pipe_delimited(table, records)
            except Exception as e:
                self.logger.warning(f"Local pipe-delimited failed: {e}")

            # Try S3
            if self.config.s3_bucket:
                try:
                    s3 = self._get_backend(StorageBackend.S3)
                    if isinstance(s3, S3Storage):
                        return s3.write_pipe_delimited(table, records)
                except Exception as e:
                    self.logger.warning(f"S3 pipe-delimited failed: {e}")

        # Fallback to batch write
        self.batch_write(table, records)
        return table

    def get_local_backend(self) -> LocalStorage:
        """Get local storage backend directly."""
        return self._get_backend(StorageBackend.LOCAL)
