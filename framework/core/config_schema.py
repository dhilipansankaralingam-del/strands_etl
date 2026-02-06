#!/usr/bin/env python3
"""
Enterprise ETL Framework - Master Configuration Schema
=======================================================

This module defines the configuration schema that drives the entire framework.
ALL features are config-driven with Y/N flags.

Config Sections:
- job_info: Job metadata
- platform: Platform selection with fallback chain
- sources: Source tables from Glue Catalog
- targets: Target tables
- transformations: ETL transformations
- agents: All agent configurations
- integrations: Slack, Teams, Email, Streamlit
- dashboard: CloudWatch and reporting
- audit: DynamoDB audit tables
- scheduling: Cron and dependencies
- resources: Worker configurations
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from enum import Enum
import json


class Platform(Enum):
    """Supported execution platforms."""
    GLUE = "glue"
    EMR = "emr"
    EKS = "eks"
    LAMBDA = "lambda"


class AgentType(Enum):
    """Available agent types."""
    LEARNING = "learning"
    RECOMMENDATION = "recommendation"
    DATA_QUALITY = "data_quality"
    CODE_ANALYSIS = "code_analysis"
    COMPLIANCE = "compliance"
    WORKLOAD_ASSESSMENT = "workload_assessment"
    AUTO_HEALING = "auto_healing"
    PLATFORM_ADVISOR = "platform_advisor"
    COST_OPTIMIZER = "cost_optimizer"
    CODE_CONVERSION = "code_conversion"


class Severity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


def parse_flag(value: Any, default: bool = False) -> bool:
    """Parse Y/N flag to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.upper() in ('Y', 'YES', 'TRUE', '1', 'ON')
    if isinstance(value, int):
        return value == 1
    return default


@dataclass
class PlatformConfig:
    """Platform execution configuration with fallback chain."""
    primary: str = "glue"
    fallback_chain: List[str] = field(default_factory=lambda: ["emr", "eks"])
    force_platform: bool = False  # If True, no fallback
    auto_fallback_on_error: bool = True

    # Platform-specific configs
    glue: Dict[str, Any] = field(default_factory=dict)
    emr: Dict[str, Any] = field(default_factory=dict)
    eks: Dict[str, Any] = field(default_factory=dict)
    lambda_config: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict) -> 'PlatformConfig':
        return cls(
            primary=data.get('primary', 'glue'),
            fallback_chain=data.get('fallback_chain', ['emr', 'eks']),
            force_platform=parse_flag(data.get('force_platform', 'N')),
            auto_fallback_on_error=parse_flag(data.get('auto_fallback_on_error', 'Y')),
            glue=data.get('glue', {}),
            emr=data.get('emr', {}),
            eks=data.get('eks', {}),
            lambda_config=data.get('lambda', {})
        )


@dataclass
class AutoHealingConfig:
    """Auto-healing agent configuration."""
    enabled: bool = True
    correct_code: bool = True
    auto_rerun: bool = True
    max_retries: int = 3
    retry_delay_seconds: int = 60

    # Healable error types
    heal_memory_errors: bool = True
    heal_shuffle_errors: bool = True
    heal_timeout_errors: bool = True
    heal_connection_errors: bool = True
    heal_data_skew: bool = True
    heal_partition_errors: bool = True

    # Code correction options
    apply_broadcast_hints: bool = True
    apply_repartitioning: bool = True
    apply_caching_fixes: bool = True
    optimize_joins: bool = True

    @classmethod
    def from_dict(cls, data: Dict) -> 'AutoHealingConfig':
        return cls(
            enabled=parse_flag(data.get('enabled', 'Y')),
            correct_code=parse_flag(data.get('correct_code', 'Y')),
            auto_rerun=parse_flag(data.get('auto_rerun', 'Y')),
            max_retries=data.get('max_retries', 3),
            retry_delay_seconds=data.get('retry_delay_seconds', 60),
            heal_memory_errors=parse_flag(data.get('heal_memory_errors', 'Y')),
            heal_shuffle_errors=parse_flag(data.get('heal_shuffle_errors', 'Y')),
            heal_timeout_errors=parse_flag(data.get('heal_timeout_errors', 'Y')),
            heal_connection_errors=parse_flag(data.get('heal_connection_errors', 'Y')),
            heal_data_skew=parse_flag(data.get('heal_data_skew', 'Y')),
            heal_partition_errors=parse_flag(data.get('heal_partition_errors', 'Y')),
            apply_broadcast_hints=parse_flag(data.get('apply_broadcast_hints', 'Y')),
            apply_repartitioning=parse_flag(data.get('apply_repartitioning', 'Y')),
            apply_caching_fixes=parse_flag(data.get('apply_caching_fixes', 'Y')),
            optimize_joins=parse_flag(data.get('optimize_joins', 'Y'))
        )


@dataclass
class ComplianceConfig:
    """Data compliance configuration."""
    enabled: bool = True
    check_sources: bool = True
    check_targets: bool = True

    frameworks: List[str] = field(default_factory=lambda: ["GDPR", "PCI-DSS"])
    pii_columns: List[str] = field(default_factory=list)

    mask_pii: bool = True
    masking_rules: Dict[str, str] = field(default_factory=dict)

    audit_access: bool = True
    data_retention_days: int = 2555  # 7 years

    alert_on_violation: bool = True
    block_on_critical_violation: bool = False

    @classmethod
    def from_dict(cls, data: Dict) -> 'ComplianceConfig':
        return cls(
            enabled=parse_flag(data.get('enabled', 'Y')),
            check_sources=parse_flag(data.get('check_sources', 'Y')),
            check_targets=parse_flag(data.get('check_targets', 'Y')),
            frameworks=data.get('frameworks', ['GDPR', 'PCI-DSS']),
            pii_columns=data.get('pii_columns', []),
            mask_pii=parse_flag(data.get('mask_pii', 'Y')),
            masking_rules=data.get('masking_rules', {}),
            audit_access=parse_flag(data.get('audit_access', 'Y')),
            data_retention_days=data.get('data_retention_days', 2555),
            alert_on_violation=parse_flag(data.get('alert_on_violation', 'Y')),
            block_on_critical_violation=parse_flag(data.get('block_on_critical_violation', 'N'))
        )


@dataclass
class DataQualityConfig:
    """Data quality configuration with NL and SQL rules."""
    enabled: bool = True
    fail_on_error: bool = False
    quarantine_path: str = ""

    # Natural language rules
    natural_language_rules: List[str] = field(default_factory=list)

    # SQL rules
    sql_rules: List[Dict[str, str]] = field(default_factory=list)

    # Template rules
    template_rules: List[Dict[str, Any]] = field(default_factory=list)

    # Thresholds
    overall_pass_rate: float = 0.95
    critical_rules: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict) -> 'DataQualityConfig':
        return cls(
            enabled=parse_flag(data.get('enabled', 'Y')),
            fail_on_error=parse_flag(data.get('fail_on_error', 'N')),
            quarantine_path=data.get('quarantine_path', ''),
            natural_language_rules=data.get('natural_language_rules', []),
            sql_rules=data.get('sql_rules', []),
            template_rules=data.get('template_rules', []),
            overall_pass_rate=data.get('thresholds', {}).get('overall_pass_rate', 0.95),
            critical_rules=data.get('thresholds', {}).get('critical_rules_must_pass', [])
        )


@dataclass
class WorkloadAssessmentConfig:
    """Workload assessment configuration for intelligent resource allocation."""
    enabled: bool = True

    # Data volume analysis
    analyze_input_size: bool = True
    analyze_output_size: bool = True

    # Temporal patterns
    consider_weekday_weekend: bool = True
    use_historical_trends: bool = True
    lookback_days: int = 30

    # Resource metrics
    monitor_cpu_load: bool = True
    monitor_memory_profile: bool = True
    monitor_shuffle_bytes: bool = True
    monitor_s3_io: bool = True
    monitor_executor_usage: bool = True

    # Data characteristics
    detect_data_skew: bool = True
    analyze_join_complexity: bool = True
    analyze_partition_strategy: bool = True

    # Cost analysis
    capture_cost_metrics: bool = True

    # Flex mode
    enable_flex_mode: bool = True

    @classmethod
    def from_dict(cls, data: Dict) -> 'WorkloadAssessmentConfig':
        return cls(
            enabled=parse_flag(data.get('enabled', 'Y')),
            analyze_input_size=parse_flag(data.get('analyze_input_size', 'Y')),
            analyze_output_size=parse_flag(data.get('analyze_output_size', 'Y')),
            consider_weekday_weekend=parse_flag(data.get('consider_weekday_weekend', 'Y')),
            use_historical_trends=parse_flag(data.get('use_historical_trends', 'Y')),
            lookback_days=data.get('lookback_days', 30),
            monitor_cpu_load=parse_flag(data.get('monitor_cpu_load', 'Y')),
            monitor_memory_profile=parse_flag(data.get('monitor_memory_profile', 'Y')),
            monitor_shuffle_bytes=parse_flag(data.get('monitor_shuffle_bytes', 'Y')),
            monitor_s3_io=parse_flag(data.get('monitor_s3_io', 'Y')),
            monitor_executor_usage=parse_flag(data.get('monitor_executor_usage', 'Y')),
            detect_data_skew=parse_flag(data.get('detect_data_skew', 'Y')),
            analyze_join_complexity=parse_flag(data.get('analyze_join_complexity', 'Y')),
            analyze_partition_strategy=parse_flag(data.get('analyze_partition_strategy', 'Y')),
            capture_cost_metrics=parse_flag(data.get('capture_cost_metrics', 'Y')),
            enable_flex_mode=parse_flag(data.get('enable_flex_mode', 'Y'))
        )


@dataclass
class CodeAnalysisConfig:
    """Code analysis configuration for PySpark optimization."""
    enabled: bool = True
    analyze_before_run: bool = True

    # Optimization categories
    check_anti_patterns: bool = True
    check_join_optimizations: bool = True
    check_shuffle_optimizations: bool = True
    check_caching_strategy: bool = True
    check_memory_optimizations: bool = True
    check_catalyst_hints: bool = True
    check_partition_strategy: bool = True
    check_udf_usage: bool = True
    check_delta_operations: bool = True
    check_glue_specific: bool = True

    # Recommendations
    recommend_aws_tools: bool = True
    recommend_data_handling: bool = True
    recommend_performance_tuning: bool = True

    # Thresholds
    min_optimization_score: int = 70
    fail_on_critical: bool = False

    @classmethod
    def from_dict(cls, data: Dict) -> 'CodeAnalysisConfig':
        return cls(
            enabled=parse_flag(data.get('enabled', 'Y')),
            analyze_before_run=parse_flag(data.get('analyze_before_run', 'Y')),
            check_anti_patterns=parse_flag(data.get('check_anti_patterns', 'Y')),
            check_join_optimizations=parse_flag(data.get('check_join_optimizations', 'Y')),
            check_shuffle_optimizations=parse_flag(data.get('check_shuffle_optimizations', 'Y')),
            check_caching_strategy=parse_flag(data.get('check_caching_strategy', 'Y')),
            check_memory_optimizations=parse_flag(data.get('check_memory_optimizations', 'Y')),
            check_catalyst_hints=parse_flag(data.get('check_catalyst_hints', 'Y')),
            check_partition_strategy=parse_flag(data.get('check_partition_strategy', 'Y')),
            check_udf_usage=parse_flag(data.get('check_udf_usage', 'Y')),
            check_delta_operations=parse_flag(data.get('check_delta_operations', 'Y')),
            check_glue_specific=parse_flag(data.get('check_glue_specific', 'Y')),
            recommend_aws_tools=parse_flag(data.get('recommend_aws_tools', 'Y')),
            recommend_data_handling=parse_flag(data.get('recommend_data_handling', 'Y')),
            recommend_performance_tuning=parse_flag(data.get('recommend_performance_tuning', 'Y')),
            min_optimization_score=data.get('min_optimization_score', 70),
            fail_on_critical=parse_flag(data.get('fail_on_critical', 'N'))
        )


@dataclass
class IntegrationConfig:
    """Integration configuration for Slack, Teams, Email, Streamlit."""

    # Slack
    slack_enabled: bool = False
    slack_webhook_secret: str = ""
    slack_channel: str = ""
    slack_bot_enabled: bool = False
    slack_voice_enabled: bool = False
    slack_etl_trigger_enabled: bool = False

    # Teams
    teams_enabled: bool = False
    teams_webhook_secret: str = ""
    teams_channel: str = ""

    # Email
    email_enabled: bool = False
    email_sender: str = ""
    email_recipients: List[str] = field(default_factory=list)
    email_ses_region: str = "us-east-1"
    email_html_template: bool = True

    # Streamlit
    streamlit_enabled: bool = False
    streamlit_port: int = 8501
    streamlit_etl_trigger_enabled: bool = False

    @classmethod
    def from_dict(cls, data: Dict) -> 'IntegrationConfig':
        slack = data.get('slack', {})
        teams = data.get('teams', {})
        email = data.get('email', {})
        streamlit = data.get('streamlit', {})

        return cls(
            slack_enabled=parse_flag(slack.get('enabled', 'N')),
            slack_webhook_secret=slack.get('webhook_secret', ''),
            slack_channel=slack.get('channel', ''),
            slack_bot_enabled=parse_flag(slack.get('bot_enabled', 'N')),
            slack_voice_enabled=parse_flag(slack.get('voice_enabled', 'N')),
            slack_etl_trigger_enabled=parse_flag(slack.get('etl_trigger_enabled', 'N')),
            teams_enabled=parse_flag(teams.get('enabled', 'N')),
            teams_webhook_secret=teams.get('webhook_secret', ''),
            teams_channel=teams.get('channel', ''),
            email_enabled=parse_flag(email.get('enabled', 'N')),
            email_sender=email.get('sender', ''),
            email_recipients=email.get('recipients', []),
            email_ses_region=email.get('ses_region', 'us-east-1'),
            email_html_template=parse_flag(email.get('html_template', 'Y')),
            streamlit_enabled=parse_flag(streamlit.get('enabled', 'N')),
            streamlit_port=streamlit.get('port', 8501),
            streamlit_etl_trigger_enabled=parse_flag(streamlit.get('etl_trigger_enabled', 'N'))
        )


@dataclass
class DashboardConfig:
    """Dashboard and reporting configuration."""
    enabled: bool = True

    # CloudWatch
    cloudwatch_enabled: bool = True
    cloudwatch_log_group: str = "/etl-framework/jobs"
    cloudwatch_metrics_namespace: str = "ETLFramework"

    # Dashboards
    create_cloudwatch_dashboard: bool = True
    dashboard_name: str = "ETL-Framework-Dashboard"

    # Reports
    generate_html_reports: bool = True
    report_s3_bucket: str = ""

    # Enterprise view
    enterprise_summary_enabled: bool = True
    job_history_days: int = 90

    # Metrics
    track_job_runs: bool = True
    track_costs: bool = True
    track_failures: bool = True
    track_compliance: bool = True
    predict_failures: bool = True

    @classmethod
    def from_dict(cls, data: Dict) -> 'DashboardConfig':
        return cls(
            enabled=parse_flag(data.get('enabled', 'Y')),
            cloudwatch_enabled=parse_flag(data.get('cloudwatch_enabled', 'Y')),
            cloudwatch_log_group=data.get('cloudwatch_log_group', '/etl-framework/jobs'),
            cloudwatch_metrics_namespace=data.get('cloudwatch_metrics_namespace', 'ETLFramework'),
            create_cloudwatch_dashboard=parse_flag(data.get('create_cloudwatch_dashboard', 'Y')),
            dashboard_name=data.get('dashboard_name', 'ETL-Framework-Dashboard'),
            generate_html_reports=parse_flag(data.get('generate_html_reports', 'Y')),
            report_s3_bucket=data.get('report_s3_bucket', ''),
            enterprise_summary_enabled=parse_flag(data.get('enterprise_summary_enabled', 'Y')),
            job_history_days=data.get('job_history_days', 90),
            track_job_runs=parse_flag(data.get('track_job_runs', 'Y')),
            track_costs=parse_flag(data.get('track_costs', 'Y')),
            track_failures=parse_flag(data.get('track_failures', 'Y')),
            track_compliance=parse_flag(data.get('track_compliance', 'Y')),
            predict_failures=parse_flag(data.get('predict_failures', 'Y'))
        )


@dataclass
class AuditConfig:
    """DynamoDB audit configuration."""
    enabled: bool = True

    # Tables
    run_audit_table: str = "etl_run_audit"
    stage_audit_table: str = "etl_stage_audit"
    dq_audit_table: str = "etl_dq_audit"
    recommendations_table: str = "etl_recommendations"
    metrics_table: str = "etl_metrics"
    compliance_table: str = "etl_compliance_audit"

    # Audit stages
    audit_on_start: bool = True
    audit_on_read: bool = True
    audit_on_transform: bool = True
    audit_on_write: bool = True
    audit_on_complete: bool = True
    audit_on_error: bool = True

    # Retention
    retention_days: int = 90

    @classmethod
    def from_dict(cls, data: Dict) -> 'AuditConfig':
        tables = data.get('dynamodb_tables', {})
        return cls(
            enabled=parse_flag(data.get('enabled', 'Y')),
            run_audit_table=tables.get('run_audit', 'etl_run_audit'),
            stage_audit_table=tables.get('stage_audit', 'etl_stage_audit'),
            dq_audit_table=tables.get('dq_audit', 'etl_dq_audit'),
            recommendations_table=tables.get('recommendations', 'etl_recommendations'),
            metrics_table=tables.get('metrics', 'etl_metrics'),
            compliance_table=tables.get('compliance', 'etl_compliance_audit'),
            audit_on_start=parse_flag(data.get('audit_on_start', 'Y')),
            audit_on_read=parse_flag(data.get('audit_on_read', 'Y')),
            audit_on_transform=parse_flag(data.get('audit_on_transform', 'Y')),
            audit_on_write=parse_flag(data.get('audit_on_write', 'Y')),
            audit_on_complete=parse_flag(data.get('audit_on_complete', 'Y')),
            audit_on_error=parse_flag(data.get('audit_on_error', 'Y')),
            retention_days=data.get('retention_days', 90)
        )


@dataclass
class EKSKarpenterConfig:
    """EKS with Karpenter configuration."""
    enabled: bool = False

    cluster_name: str = ""
    namespace: str = "spark-workloads"

    # Karpenter
    use_karpenter: bool = True
    provisioner_name: str = "default"

    # Instance optimization
    use_spot: bool = True
    spot_percentage: int = 80
    use_graviton: bool = True

    # Scaling
    min_nodes: int = 0
    max_nodes: int = 100
    consolidation_enabled: bool = True

    @classmethod
    def from_dict(cls, data: Dict) -> 'EKSKarpenterConfig':
        return cls(
            enabled=parse_flag(data.get('enabled', 'N')),
            cluster_name=data.get('cluster_name', ''),
            namespace=data.get('namespace', 'spark-workloads'),
            use_karpenter=parse_flag(data.get('use_karpenter', 'Y')),
            provisioner_name=data.get('provisioner_name', 'default'),
            use_spot=parse_flag(data.get('use_spot', 'Y')),
            spot_percentage=data.get('spot_percentage', 80),
            use_graviton=parse_flag(data.get('use_graviton', 'Y')),
            min_nodes=data.get('min_nodes', 0),
            max_nodes=data.get('max_nodes', 100),
            consolidation_enabled=parse_flag(data.get('consolidation_enabled', 'Y'))
        )


@dataclass
class MasterConfig:
    """
    Master configuration that encompasses all framework settings.
    Everything is config-driven.
    """
    # Job info
    job_name: str = ""
    description: str = ""
    version: str = "1.0.0"
    owner: str = ""

    # ETL Script
    script_path: str = ""
    script_type: str = "pyspark"

    # Platform
    platform: PlatformConfig = field(default_factory=PlatformConfig)

    # Sources and Targets
    sources: List[Dict[str, Any]] = field(default_factory=list)
    targets: List[Dict[str, Any]] = field(default_factory=list)

    # Transformations
    transformations: List[Dict[str, Any]] = field(default_factory=list)

    # Agents
    auto_healing: AutoHealingConfig = field(default_factory=AutoHealingConfig)
    compliance: ComplianceConfig = field(default_factory=ComplianceConfig)
    data_quality: DataQualityConfig = field(default_factory=DataQualityConfig)
    workload_assessment: WorkloadAssessmentConfig = field(default_factory=WorkloadAssessmentConfig)
    code_analysis: CodeAnalysisConfig = field(default_factory=CodeAnalysisConfig)

    # Integrations
    integrations: IntegrationConfig = field(default_factory=IntegrationConfig)

    # Dashboard
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)

    # Audit
    audit: AuditConfig = field(default_factory=AuditConfig)

    # EKS/Karpenter
    eks_karpenter: EKSKarpenterConfig = field(default_factory=EKSKarpenterConfig)

    # Spark configs
    spark_configs: Dict[str, str] = field(default_factory=dict)

    # Scheduling
    scheduling: Dict[str, Any] = field(default_factory=dict)

    # Raw config for agents
    raw_config: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict) -> 'MasterConfig':
        """Create MasterConfig from dictionary."""
        return cls(
            job_name=data.get('job_name', ''),
            description=data.get('description', ''),
            version=data.get('version', '1.0.0'),
            owner=data.get('metadata', {}).get('owner', ''),
            script_path=data.get('etl_script', {}).get('path', ''),
            script_type=data.get('etl_script', {}).get('type', 'pyspark'),
            platform=PlatformConfig.from_dict(data.get('platform', {})),
            sources=data.get('sources', data.get('source', {}).get('tables', [])),
            targets=data.get('targets', [data.get('target', {})]),
            transformations=data.get('transformations', []),
            auto_healing=AutoHealingConfig.from_dict(data.get('agents', {}).get('auto_healing', data.get('auto_healing', {}))),
            compliance=ComplianceConfig.from_dict(data.get('compliance', {})),
            data_quality=DataQualityConfig.from_dict(data.get('data_quality', {})),
            workload_assessment=WorkloadAssessmentConfig.from_dict(data.get('agents', {}).get('workload_assessment', data.get('workload_assessment', {}))),
            code_analysis=CodeAnalysisConfig.from_dict(data.get('agents', {}).get('code_analysis', data.get('code_analysis', {}))),
            integrations=IntegrationConfig.from_dict(data.get('integrations', data.get('notifications', {}))),
            dashboard=DashboardConfig.from_dict(data.get('dashboard', {})),
            audit=AuditConfig.from_dict(data.get('audit', {})),
            eks_karpenter=EKSKarpenterConfig.from_dict(data.get('eks_karpenter', data.get('agents', {}).get('eks_optimizer', {}))),
            spark_configs=data.get('spark_configs', {}),
            scheduling=data.get('scheduling', {}),
            raw_config=data
        )

    @classmethod
    def from_json_file(cls, file_path: str) -> 'MasterConfig':
        """Load config from JSON file."""
        with open(file_path, 'r') as f:
            data = json.load(f)
        return cls.from_dict(data)

    @classmethod
    def from_s3(cls, s3_path: str, s3_client=None) -> 'MasterConfig':
        """Load config from S3."""
        import boto3
        s3 = s3_client or boto3.client('s3')

        bucket, key = s3_path.replace('s3://', '').split('/', 1)
        response = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        return cls.from_dict(data)

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        return self.raw_config

    def validate(self) -> List[str]:
        """Validate configuration and return list of errors."""
        errors = []

        if not self.job_name:
            errors.append("job_name is required")

        if not self.script_path and not self.sources:
            errors.append("Either script_path or sources must be specified")

        return errors
