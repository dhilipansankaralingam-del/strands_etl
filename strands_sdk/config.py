"""
ETL Configuration Module
========================

Loads and validates ETL job configuration.
"""

import json
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional


@dataclass
class ETLConfig:
    """ETL Job Configuration."""

    job_name: str
    source_tables: List[Dict[str, Any]]
    platform: Dict[str, Any] = field(default_factory=dict)
    data_quality: Dict[str, Any] = field(default_factory=dict)
    compliance: Dict[str, Any] = field(default_factory=dict)
    learning: Dict[str, Any] = field(default_factory=dict)
    storage: Dict[str, Any] = field(default_factory=dict)
    model_provider: str = "bedrock"
    model_id: str = "us.anthropic.claude-sonnet-4-20250514-v1:0"
    aws_region: str = "us-east-1"
    dry_run: bool = False
    raw_config: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_file(cls, config_path: str) -> 'ETLConfig':
        """Load configuration from JSON file."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(path, 'r') as f:
            data = json.load(f)

        return cls(
            job_name=data.get('job_name', 'unknown_job'),
            source_tables=data.get('source_tables', []),
            platform=data.get('platform', {}),
            data_quality=data.get('data_quality', {}),
            compliance=data.get('compliance', {}),
            learning=data.get('learning', {}),
            storage=data.get('storage', {}),
            model_provider=data.get('model_provider', 'bedrock'),
            model_id=data.get('model_id', 'us.anthropic.claude-sonnet-4-20250514-v1:0'),
            aws_region=data.get('aws_region', 'us-east-1'),
            dry_run=data.get('dry_run', False),
            raw_config=data
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return self.raw_config

    def get_table_names(self) -> List[str]:
        """Get list of source table names."""
        return [t.get('name', '') for t in self.source_tables]

    def get_table_config(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific table."""
        for table in self.source_tables:
            if table.get('name') == table_name:
                return table
        return None
