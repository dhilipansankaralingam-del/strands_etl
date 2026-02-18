#!/usr/bin/env python3
"""
Generate HTML Dashboard from Unified Audit Logs
================================================

Reads audit events from:
1. Local JSONL files (primary)
2. S3 (if configured)
3. DynamoDB (if configured)

Generates a comprehensive HTML dashboard for ETL job monitoring.
"""

import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from framework.strands.unified_audit import UnifiedAuditLogger
from framework.dashboard.enterprise_dashboard import EnterpriseDashboard, DashboardData


def load_config(config_path: str = None) -> dict:
    """Load configuration from file or use defaults."""
    if config_path and os.path.exists(config_path):
        with open(config_path, 'r') as f:
            return json.load(f)

    # Check for default configs
    default_paths = [
        'demo_configs/enterprise_sales_config.json',
        'config/default_config.json',
        'etl_config.json'
    ]

    for path in default_paths:
        if os.path.exists(path):
            with open(path, 'r') as f:
                return json.load(f)

    # Return minimal config
    return {
        'storage': {
            'primary_backend': 'local',
            'local_base_path': 'data/agent_store'
        },
        'audit': {
            'enable_local': True
        }
    }


def generate_dashboard(
    config: dict,
    output_path: str = 'dashboard_output/dashboard.html',
    days: int = 7
) -> str:
    """
    Generate HTML dashboard from audit logs.

    Args:
        config: Configuration dictionary
        output_path: Output HTML file path
        days: Number of days of history to include

    Returns:
        Path to generated HTML file
    """
    print(f"Generating dashboard from last {days} days of audit data...")

    # Initialize audit logger
    audit_logger = UnifiedAuditLogger(config)

    # Get dashboard data
    dashboard_data = audit_logger.get_dashboard_data(days=days)

    # Convert to DashboardData
    data = DashboardData(
        job_history=dashboard_data.get('job_history', []),
        recommendations=dashboard_data.get('recommendations', []),
        dq_results=dashboard_data.get('dq_results', []),
        compliance_results=dashboard_data.get('compliance_results', []),
        cost_data=dashboard_data.get('cost_data', []),
        predictions=dashboard_data.get('predictions', [])
    )

    # Generate dashboard
    dashboard = EnterpriseDashboard(config)

    # Ensure output directory exists
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Generate and save
    html = dashboard.generate_full_dashboard(data)
    with open(output_path, 'w') as f:
        f.write(html)

    print(f"Dashboard generated: {output_path}")
    print(f"  - Job history: {len(data.job_history)} records")
    print(f"  - DQ results: {len(data.dq_results)} records")
    print(f"  - Compliance: {len(data.compliance_results)} records")
    print(f"  - Recommendations: {len(data.recommendations)} records")

    return output_path


def export_to_json(
    config: dict,
    output_path: str = 'dashboard_output/audit_data.json',
    days: int = 7
) -> str:
    """
    Export audit data to JSON for external dashboard tools.

    Args:
        config: Configuration dictionary
        output_path: Output JSON file path
        days: Number of days of history to include

    Returns:
        Path to generated JSON file
    """
    print(f"Exporting audit data from last {days} days...")

    # Initialize audit logger
    audit_logger = UnifiedAuditLogger(config)

    # Get all events
    end_date = datetime.utcnow().strftime('%Y-%m-%d')
    start_date = (datetime.utcnow() - timedelta(days=days)).strftime('%Y-%m-%d')

    events = audit_logger.get_events(
        start_date=start_date,
        end_date=end_date,
        limit=100000
    )

    # Ensure output directory exists
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Export
    with open(output_path, 'w') as f:
        json.dump({
            'exported_at': datetime.utcnow().isoformat(),
            'start_date': start_date,
            'end_date': end_date,
            'event_count': len(events),
            'events': events
        }, f, indent=2, default=str)

    print(f"Exported {len(events)} events to: {output_path}")

    return output_path


def create_athena_table_ddl() -> str:
    """Generate Athena DDL for querying S3 audit data."""
    return '''
-- Athena DDL for ETL Unified Audit Events
-- Run this in Athena to create a table over your S3 audit data

CREATE EXTERNAL TABLE IF NOT EXISTS etl_unified_audit (
    event_id STRING,
    event_type STRING,
    timestamp STRING,
    job_name STRING,
    execution_id STRING,
    agent_name STRING,
    agent_id STRING,
    status STRING,
    message STRING,
    duration_ms DOUBLE,
    records_read BIGINT,
    records_written BIGINT,
    bytes_processed BIGINT,
    cost_usd DOUBLE,
    platform STRING,
    worker_type STRING,
    worker_count INT,
    dq_score DOUBLE,
    dq_rules_passed INT,
    dq_rules_failed INT,
    compliance_status STRING,
    pii_columns ARRAY<STRING>,
    violations ARRAY<STRING>,
    error_type STRING,
    error_message STRING,
    recommendations ARRAY<STRING>,
    metadata STRING,
    event_date STRING,
    event_hour STRING
)
PARTITIONED BY (
    year STRING,
    month STRING,
    day STRING,
    hour STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION 's3://YOUR_BUCKET/audit-events/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- Add partitions (run after data exists)
MSCK REPAIR TABLE etl_unified_audit;

-- Example queries:

-- Success rate by job
SELECT
    job_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as successful,
    ROUND(100.0 * SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
FROM etl_unified_audit
WHERE event_type = 'agent_complete'
  AND event_date >= date_format(date_add('day', -7, current_date), '%Y-%m-%d')
GROUP BY job_name
ORDER BY success_rate;

-- Average duration by agent
SELECT
    agent_name,
    AVG(duration_ms) as avg_duration_ms,
    MIN(duration_ms) as min_duration_ms,
    MAX(duration_ms) as max_duration_ms
FROM etl_unified_audit
WHERE event_type = 'agent_complete'
  AND duration_ms IS NOT NULL
GROUP BY agent_name
ORDER BY avg_duration_ms DESC;

-- Data quality trends
SELECT
    event_date,
    job_name,
    dq_score,
    dq_rules_passed,
    dq_rules_failed
FROM etl_unified_audit
WHERE event_type = 'data_quality'
ORDER BY event_date DESC, job_name;

-- Platform conversion history
SELECT
    event_date,
    job_name,
    JSON_EXTRACT_SCALAR(metadata, '$.source_platform') as source_platform,
    platform as target_platform,
    JSON_EXTRACT_SCALAR(metadata, '$.reason') as reason
FROM etl_unified_audit
WHERE event_type = 'platform_conversion'
ORDER BY timestamp DESC;
'''


def main():
    parser = argparse.ArgumentParser(
        description='Generate HTML dashboard from ETL audit logs'
    )
    parser.add_argument(
        '--config', '-c',
        help='Path to configuration file',
        default=None
    )
    parser.add_argument(
        '--output', '-o',
        help='Output HTML file path',
        default='dashboard_output/dashboard.html'
    )
    parser.add_argument(
        '--days', '-d',
        type=int,
        help='Number of days of history to include',
        default=7
    )
    parser.add_argument(
        '--export-json',
        help='Export raw audit data to JSON file',
        default=None
    )
    parser.add_argument(
        '--athena-ddl',
        action='store_true',
        help='Print Athena DDL for S3 audit data'
    )

    args = parser.parse_args()

    if args.athena_ddl:
        print(create_athena_table_ddl())
        return

    # Load config
    config = load_config(args.config)

    # Generate dashboard
    generate_dashboard(config, args.output, args.days)

    # Export JSON if requested
    if args.export_json:
        export_to_json(config, args.export_json, args.days)


if __name__ == '__main__':
    main()
