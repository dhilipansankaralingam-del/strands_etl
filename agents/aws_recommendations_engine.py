"""
AWS Recommendations Engine - Recommends optimal AWS services for ETL workloads
Covers orchestration, storage, compute, monitoring, and governance tools
"""

import json
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AWSService(Enum):
    """AWS Services for ETL workloads."""
    # Compute
    GLUE = "aws_glue"
    GLUE_FLEX = "aws_glue_flex"
    EMR = "amazon_emr"
    EMR_SERVERLESS = "amazon_emr_serverless"
    EMR_ON_EKS = "amazon_emr_on_eks"
    EKS = "amazon_eks"
    LAMBDA = "aws_lambda"
    BATCH = "aws_batch"

    # Orchestration
    STEP_FUNCTIONS = "aws_step_functions"
    MWAA = "amazon_mwaa"  # Managed Airflow
    EVENTBRIDGE = "amazon_eventbridge"
    GLUE_WORKFLOWS = "aws_glue_workflows"

    # Storage
    S3 = "amazon_s3"
    S3_GLACIER = "amazon_s3_glacier"
    REDSHIFT = "amazon_redshift"
    DYNAMODB = "amazon_dynamodb"
    RDS = "amazon_rds"

    # Analytics
    ATHENA = "amazon_athena"
    REDSHIFT_SPECTRUM = "amazon_redshift_spectrum"
    QUICKSIGHT = "amazon_quicksight"
    OPENSEARCH = "amazon_opensearch"

    # Data Catalog & Governance
    GLUE_CATALOG = "aws_glue_catalog"
    LAKE_FORMATION = "aws_lake_formation"

    # Streaming
    KINESIS = "amazon_kinesis"
    KINESIS_FIREHOSE = "amazon_kinesis_firehose"
    MSK = "amazon_msk"  # Managed Kafka

    # ML/AI
    SAGEMAKER = "amazon_sagemaker"
    BEDROCK = "amazon_bedrock"

    # Monitoring
    CLOUDWATCH = "amazon_cloudwatch"
    XRAY = "aws_xray"

    # Security
    SECRETS_MANAGER = "aws_secrets_manager"
    KMS = "aws_kms"
    IAM = "aws_iam"


@dataclass
class ServiceRecommendation:
    """A recommendation for an AWS service."""
    service: AWSService
    reason: str
    priority: str  # 'required', 'recommended', 'optional'
    estimated_cost: Optional[str] = None
    setup_complexity: str = "medium"  # 'low', 'medium', 'high'
    alternatives: List[AWSService] = field(default_factory=list)
    configuration: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ArchitectureRecommendation:
    """Complete architecture recommendation."""
    compute: List[ServiceRecommendation]
    orchestration: List[ServiceRecommendation]
    storage: List[ServiceRecommendation]
    analytics: List[ServiceRecommendation]
    governance: List[ServiceRecommendation]
    monitoring: List[ServiceRecommendation]
    security: List[ServiceRecommendation]
    estimated_monthly_cost: float
    architecture_diagram: str
    migration_steps: List[str]


class AWSRecommendationsEngine:
    """
    Recommends optimal AWS services based on workload characteristics.

    Considers:
    - Data volume and velocity
    - Processing complexity
    - Cost optimization
    - Operational overhead
    - Team expertise
    - Compliance requirements
    """

    # Service characteristics for matching
    SERVICE_PROFILES = {
        AWSService.GLUE: {
            "max_data_gb": 500,
            "complexity": ["low", "medium", "high"],
            "latency": "batch",
            "managed": True,
            "cost_per_dpu_hour": 0.44,
            "setup_time": "minutes",
            "best_for": ["ETL", "data_catalog", "crawlers"]
        },
        AWSService.GLUE_FLEX: {
            "max_data_gb": 500,
            "complexity": ["low", "medium"],
            "latency": "batch_flexible",
            "managed": True,
            "cost_per_dpu_hour": 0.29,
            "setup_time": "minutes",
            "best_for": ["non_urgent_etl", "cost_optimization"]
        },
        AWSService.EMR_SERVERLESS: {
            "max_data_gb": 5000,
            "complexity": ["medium", "high"],
            "latency": "batch",
            "managed": True,
            "cost_per_hour": 0.05,  # varies by resources
            "setup_time": "minutes",
            "best_for": ["large_scale_etl", "spark", "presto"]
        },
        AWSService.EMR: {
            "max_data_gb": float("inf"),
            "complexity": ["high", "very_high"],
            "latency": "batch",
            "managed": False,
            "cost_per_hour": "varies",
            "setup_time": "hours",
            "best_for": ["massive_scale", "custom_clusters", "persistent_clusters"]
        },
        AWSService.EKS: {
            "max_data_gb": float("inf"),
            "complexity": ["high", "very_high"],
            "latency": "any",
            "managed": False,
            "cost_per_hour": "varies",
            "setup_time": "hours",
            "best_for": ["mixed_workloads", "microservices", "ml_pipelines", "custom_containers"]
        },
        AWSService.LAMBDA: {
            "max_data_gb": 10,
            "complexity": ["low"],
            "latency": "real_time",
            "managed": True,
            "cost_per_million": 0.20,
            "setup_time": "minutes",
            "best_for": ["small_transforms", "event_driven", "api_backends"]
        },
        AWSService.STEP_FUNCTIONS: {
            "complexity": ["any"],
            "managed": True,
            "cost_per_transition": 0.000025,
            "setup_time": "hours",
            "best_for": ["orchestration", "error_handling", "parallel_execution"]
        },
        AWSService.MWAA: {
            "complexity": ["high"],
            "managed": True,
            "cost_per_hour": 0.49,  # small environment
            "setup_time": "hours",
            "best_for": ["complex_workflows", "existing_airflow", "scheduling"]
        },
        AWSService.KINESIS: {
            "latency": "real_time",
            "managed": True,
            "cost_per_shard_hour": 0.015,
            "best_for": ["streaming", "real_time_analytics"]
        },
        AWSService.ATHENA: {
            "managed": True,
            "cost_per_tb_scanned": 5.00,
            "best_for": ["ad_hoc_queries", "data_exploration", "serverless_sql"]
        },
        AWSService.REDSHIFT: {
            "managed": True,
            "cost_per_hour": 0.25,  # dc2.large
            "best_for": ["data_warehouse", "bi_reporting", "complex_analytics"]
        }
    }

    def __init__(self):
        """Initialize recommendations engine."""
        pass

    def analyze_workload(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze workload characteristics.

        Args:
            config: Pipeline configuration

        Returns:
            Workload analysis dict
        """
        workload = config.get('workload', {})
        data_sources = config.get('data_sources', [])

        # Estimate data volume
        data_volume_gb = self._estimate_data_volume(config)

        # Determine complexity
        complexity = workload.get('complexity', 'medium')

        # Check for streaming requirements
        is_streaming = workload.get('type') == 'streaming' or \
                       any(s.get('type') == 'kinesis' for s in data_sources)

        # Check for ML requirements
        has_ml = workload.get('ml_enabled', False) or \
                 'sagemaker' in str(config).lower()

        # Check compliance requirements
        compliance = config.get('compliance', {})
        requires_governance = compliance.get('enabled', False)

        # Check orchestration needs
        has_dependencies = len(config.get('dependencies', [])) > 0
        needs_scheduling = workload.get('schedule_type') not in [None, 'manual', 'adhoc']

        return {
            'data_volume_gb': data_volume_gb,
            'complexity': complexity,
            'is_streaming': is_streaming,
            'has_ml': has_ml,
            'requires_governance': requires_governance,
            'has_dependencies': has_dependencies,
            'needs_scheduling': needs_scheduling,
            'criticality': workload.get('criticality', 'medium'),
            'schedule_type': workload.get('schedule_type', 'daily'),
            'team_expertise': config.get('team_expertise', 'intermediate')
        }

    def _estimate_data_volume(self, config: Dict[str, Any]) -> float:
        """Estimate data volume from configuration."""
        # Try explicit volume
        workload = config.get('workload', {})
        volume_mapping = {
            'very_low': 0.1,
            'low': 1,
            'medium': 10,
            'high': 100,
            'very_high': 1000
        }
        return volume_mapping.get(workload.get('data_volume', 'medium'), 10)

    def recommend_architecture(self, config: Dict[str, Any]) -> ArchitectureRecommendation:
        """
        Generate complete architecture recommendation.

        Args:
            config: Pipeline configuration

        Returns:
            ArchitectureRecommendation with all services
        """
        analysis = self.analyze_workload(config)

        # Get recommendations for each category
        compute = self._recommend_compute(analysis)
        orchestration = self._recommend_orchestration(analysis)
        storage = self._recommend_storage(analysis)
        analytics = self._recommend_analytics(analysis)
        governance = self._recommend_governance(analysis)
        monitoring = self._recommend_monitoring(analysis)
        security = self._recommend_security(analysis)

        # Generate architecture diagram
        diagram = self._generate_architecture_diagram(
            compute, orchestration, storage, analytics
        )

        # Generate migration steps
        migration_steps = self._generate_migration_steps(
            config, compute, orchestration
        )

        # Estimate cost
        estimated_cost = self._estimate_monthly_cost(
            analysis, compute, orchestration, storage
        )

        return ArchitectureRecommendation(
            compute=compute,
            orchestration=orchestration,
            storage=storage,
            analytics=analytics,
            governance=governance,
            monitoring=monitoring,
            security=security,
            estimated_monthly_cost=estimated_cost,
            architecture_diagram=diagram,
            migration_steps=migration_steps
        )

    def _recommend_compute(self, analysis: Dict[str, Any]) -> List[ServiceRecommendation]:
        """Recommend compute services."""
        recommendations = []
        data_volume = analysis['data_volume_gb']
        complexity = analysis['complexity']
        is_streaming = analysis['is_streaming']
        criticality = analysis['criticality']

        # Streaming workloads
        if is_streaming:
            recommendations.append(ServiceRecommendation(
                service=AWSService.KINESIS,
                reason="Real-time data ingestion for streaming workload",
                priority="required",
                estimated_cost="$0.015/shard-hour + data",
                setup_complexity="medium",
                alternatives=[AWSService.MSK],
                configuration={
                    "shard_count": max(1, int(data_volume / 10)),
                    "retention_hours": 24
                }
            ))

            recommendations.append(ServiceRecommendation(
                service=AWSService.LAMBDA,
                reason="Process streaming records in real-time",
                priority="recommended",
                estimated_cost="$0.20/million requests",
                setup_complexity="low",
                configuration={
                    "memory_mb": 1024,
                    "timeout_seconds": 60,
                    "batch_size": 100
                }
            ))

        # Batch workloads
        else:
            # Small data, simple transforms
            if data_volume < 10 and complexity == 'low':
                recommendations.append(ServiceRecommendation(
                    service=AWSService.LAMBDA,
                    reason=f"Data volume ({data_volume}GB) is small, Lambda is cost-effective",
                    priority="recommended",
                    estimated_cost="~$0.50/month",
                    setup_complexity="low",
                    alternatives=[AWSService.GLUE_FLEX],
                    configuration={
                        "memory_mb": 1024,
                        "timeout_seconds": 900
                    }
                ))

            # Medium data or complexity
            elif data_volume < 100 or complexity in ['low', 'medium']:
                if criticality in ['low', 'medium']:
                    recommendations.append(ServiceRecommendation(
                        service=AWSService.GLUE_FLEX,
                        reason="Non-critical workload can use Flex for 35% cost savings",
                        priority="recommended",
                        estimated_cost=f"~${data_volume * 0.1:.2f}/run",
                        setup_complexity="low",
                        alternatives=[AWSService.GLUE],
                        configuration={
                            "worker_type": "G.1X",
                            "number_of_workers": max(2, int(data_volume / 10)),
                            "flex_execution": True
                        }
                    ))
                else:
                    recommendations.append(ServiceRecommendation(
                        service=AWSService.GLUE,
                        reason="Standard Glue for reliable, managed ETL",
                        priority="recommended",
                        estimated_cost=f"~${data_volume * 0.15:.2f}/run",
                        setup_complexity="low",
                        alternatives=[AWSService.EMR_SERVERLESS],
                        configuration={
                            "worker_type": "G.1X",
                            "number_of_workers": max(2, int(data_volume / 10))
                        }
                    ))

            # Large data
            elif data_volume < 1000:
                recommendations.append(ServiceRecommendation(
                    service=AWSService.EMR_SERVERLESS,
                    reason=f"Large data volume ({data_volume}GB) needs EMR Serverless scale",
                    priority="recommended",
                    estimated_cost=f"~${data_volume * 0.05:.2f}/run",
                    setup_complexity="medium",
                    alternatives=[AWSService.EMR, AWSService.EKS],
                    configuration={
                        "release_label": "emr-6.15.0",
                        "max_capacity": f"{int(data_volume / 10)}vCPU, {int(data_volume / 5)}GB"
                    }
                ))

            # Massive data
            else:
                recommendations.append(ServiceRecommendation(
                    service=AWSService.EKS,
                    reason=f"Massive data ({data_volume}GB) benefits from EKS + Karpenter + SPOT",
                    priority="recommended",
                    estimated_cost=f"~${data_volume * 0.03:.2f}/run with SPOT",
                    setup_complexity="high",
                    alternatives=[AWSService.EMR],
                    configuration={
                        "use_karpenter": True,
                        "spot_percentage": 80,
                        "graviton_enabled": True,
                        "dynamic_allocation": True
                    }
                ))

        return recommendations

    def _recommend_orchestration(self, analysis: Dict[str, Any]) -> List[ServiceRecommendation]:
        """Recommend orchestration services."""
        recommendations = []
        has_dependencies = analysis['has_dependencies']
        needs_scheduling = analysis['needs_scheduling']
        complexity = analysis['complexity']
        team_expertise = analysis.get('team_expertise', 'intermediate')

        # Simple scheduling
        if needs_scheduling and not has_dependencies:
            recommendations.append(ServiceRecommendation(
                service=AWSService.EVENTBRIDGE,
                reason="Simple scheduling with cron expressions",
                priority="recommended",
                estimated_cost="Free for scheduled rules",
                setup_complexity="low",
                configuration={
                    "schedule_expression": "cron(0 8 * * ? *)",  # Daily at 8 AM
                    "target": "glue_job or lambda"
                }
            ))

        # Complex workflows
        if has_dependencies or complexity in ['high', 'very_high']:
            if team_expertise == 'expert' or 'airflow' in str(analysis).lower():
                recommendations.append(ServiceRecommendation(
                    service=AWSService.MWAA,
                    reason="Complex workflows with Airflow expertise",
                    priority="recommended",
                    estimated_cost="$0.49/hour (small environment)",
                    setup_complexity="high",
                    alternatives=[AWSService.STEP_FUNCTIONS],
                    configuration={
                        "environment_class": "mw1.small",
                        "max_workers": 5
                    }
                ))
            else:
                recommendations.append(ServiceRecommendation(
                    service=AWSService.STEP_FUNCTIONS,
                    reason="Visual workflow orchestration with error handling",
                    priority="recommended",
                    estimated_cost="$0.025/1000 state transitions",
                    setup_complexity="medium",
                    alternatives=[AWSService.GLUE_WORKFLOWS],
                    configuration={
                        "type": "STANDARD",
                        "logging_level": "ALL",
                        "include_execution_data": True
                    }
                ))

        # Glue Workflows for simple Glue-centric pipelines
        if not recommendations and needs_scheduling:
            recommendations.append(ServiceRecommendation(
                service=AWSService.GLUE_WORKFLOWS,
                reason="Native Glue orchestration for Glue jobs",
                priority="optional",
                estimated_cost="Free (only pay for jobs)",
                setup_complexity="low"
            ))

        return recommendations

    def _recommend_storage(self, analysis: Dict[str, Any]) -> List[ServiceRecommendation]:
        """Recommend storage services."""
        recommendations = []
        data_volume = analysis['data_volume_gb']

        # S3 is always recommended for data lake
        recommendations.append(ServiceRecommendation(
            service=AWSService.S3,
            reason="Primary data lake storage",
            priority="required",
            estimated_cost=f"~${data_volume * 0.023:.2f}/month (Standard)",
            setup_complexity="low",
            configuration={
                "storage_class": "STANDARD" if data_volume < 100 else "INTELLIGENT_TIERING",
                "lifecycle_rules": True,
                "versioning": True
            }
        ))

        # Archive for historical data
        if data_volume > 100:
            recommendations.append(ServiceRecommendation(
                service=AWSService.S3_GLACIER,
                reason="Cost-effective archive for historical data",
                priority="optional",
                estimated_cost=f"~${data_volume * 0.004:.2f}/month",
                setup_complexity="low",
                configuration={
                    "transition_days": 90,
                    "storage_class": "DEEP_ARCHIVE"
                }
            ))

        return recommendations

    def _recommend_analytics(self, analysis: Dict[str, Any]) -> List[ServiceRecommendation]:
        """Recommend analytics services."""
        recommendations = []
        data_volume = analysis['data_volume_gb']

        # Athena for ad-hoc queries
        recommendations.append(ServiceRecommendation(
            service=AWSService.ATHENA,
            reason="Serverless SQL queries on S3 data",
            priority="recommended",
            estimated_cost="$5.00/TB scanned",
            setup_complexity="low",
            configuration={
                "workgroup": "primary",
                "output_location": "s3://bucket/athena-results/"
            }
        ))

        # Redshift for BI
        if data_volume > 50 or analysis.get('has_bi', False):
            recommendations.append(ServiceRecommendation(
                service=AWSService.REDSHIFT,
                reason="Data warehouse for BI and complex analytics",
                priority="optional",
                estimated_cost="$0.25/hour (dc2.large)",
                setup_complexity="medium",
                alternatives=[AWSService.REDSHIFT_SPECTRUM],
                configuration={
                    "node_type": "dc2.large",
                    "number_of_nodes": 2
                }
            ))

        # QuickSight for visualization
        recommendations.append(ServiceRecommendation(
            service=AWSService.QUICKSIGHT,
            reason="Business intelligence dashboards",
            priority="optional",
            estimated_cost="$9/user/month",
            setup_complexity="medium"
        ))

        return recommendations

    def _recommend_governance(self, analysis: Dict[str, Any]) -> List[ServiceRecommendation]:
        """Recommend governance services."""
        recommendations = []

        # Glue Catalog is always needed
        recommendations.append(ServiceRecommendation(
            service=AWSService.GLUE_CATALOG,
            reason="Central metadata repository",
            priority="required",
            estimated_cost="$1/100K objects/month",
            setup_complexity="low"
        ))

        # Lake Formation for governance
        if analysis['requires_governance']:
            recommendations.append(ServiceRecommendation(
                service=AWSService.LAKE_FORMATION,
                reason="Fine-grained access control and data governance",
                priority="recommended",
                estimated_cost="Free (pay for underlying services)",
                setup_complexity="high",
                configuration={
                    "enable_fine_grained_access": True,
                    "enable_data_filtering": True
                }
            ))

        return recommendations

    def _recommend_monitoring(self, analysis: Dict[str, Any]) -> List[ServiceRecommendation]:
        """Recommend monitoring services."""
        recommendations = []

        recommendations.append(ServiceRecommendation(
            service=AWSService.CLOUDWATCH,
            reason="Metrics, logs, and alarms",
            priority="required",
            estimated_cost="~$3/dashboard + logs",
            setup_complexity="low",
            configuration={
                "retention_days": 30,
                "create_dashboard": True,
                "create_alarms": True
            }
        ))

        if analysis['complexity'] in ['high', 'very_high']:
            recommendations.append(ServiceRecommendation(
                service=AWSService.XRAY,
                reason="Distributed tracing for complex pipelines",
                priority="optional",
                estimated_cost="$5/million traces",
                setup_complexity="medium"
            ))

        return recommendations

    def _recommend_security(self, analysis: Dict[str, Any]) -> List[ServiceRecommendation]:
        """Recommend security services."""
        recommendations = []

        recommendations.append(ServiceRecommendation(
            service=AWSService.SECRETS_MANAGER,
            reason="Secure credential storage",
            priority="required",
            estimated_cost="$0.40/secret/month",
            setup_complexity="low"
        ))

        recommendations.append(ServiceRecommendation(
            service=AWSService.KMS,
            reason="Encryption key management",
            priority="required",
            estimated_cost="$1/key/month",
            setup_complexity="low"
        ))

        return recommendations

    def _generate_architecture_diagram(self,
                                        compute: List[ServiceRecommendation],
                                        orchestration: List[ServiceRecommendation],
                                        storage: List[ServiceRecommendation],
                                        analytics: List[ServiceRecommendation]) -> str:
        """Generate ASCII architecture diagram."""
        compute_services = [r.service.value for r in compute if r.priority == 'recommended']
        orchestration_services = [r.service.value for r in orchestration if r.priority != 'optional']
        storage_services = [r.service.value for r in storage if r.priority == 'required']
        analytics_services = [r.service.value for r in analytics if r.priority != 'optional']

        return f"""
┌─────────────────────────────────────────────────────────────────────────┐
│                     RECOMMENDED ARCHITECTURE                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ DATA SOURCES                                                      │   │
│  │  S3 │ Databases │ APIs │ Streaming                               │   │
│  └───────────────────────────┬─────────────────────────────────────┘   │
│                               │                                          │
│                               ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ ORCHESTRATION: {', '.join(orchestration_services)[:40]:<40} │   │
│  └───────────────────────────┬─────────────────────────────────────┘   │
│                               │                                          │
│                               ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ COMPUTE: {', '.join(compute_services)[:45]:<45} │   │
│  └───────────────────────────┬─────────────────────────────────────┘   │
│                               │                                          │
│                               ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ STORAGE: {', '.join(storage_services)[:45]:<45} │   │
│  └───────────────────────────┬─────────────────────────────────────┘   │
│                               │                                          │
│                               ▼                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │ ANALYTICS: {', '.join(analytics_services)[:43]:<43} │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
"""

    def _generate_migration_steps(self,
                                   config: Dict[str, Any],
                                   compute: List[ServiceRecommendation],
                                   orchestration: List[ServiceRecommendation]) -> List[str]:
        """Generate migration steps."""
        steps = []
        current_platform = config.get('execution', {}).get('platform', 'glue')
        recommended_compute = compute[0].service if compute else AWSService.GLUE

        steps.append("1. Review current ETL jobs and dependencies")

        # Compute migration
        if recommended_compute == AWSService.EMR_SERVERLESS:
            steps.extend([
                "2. Convert Glue scripts to standard PySpark (use glue_script_converter.py)",
                "3. Create EMR Serverless application",
                "4. Update IAM roles for EMR Serverless",
                "5. Configure Glue Catalog access in Spark configuration",
                "6. Test with small dataset before full migration"
            ])
        elif recommended_compute == AWSService.EKS:
            steps.extend([
                "2. Set up EKS cluster with Karpenter",
                "3. Install Spark Operator",
                "4. Convert Glue scripts to standard PySpark",
                "5. Create SparkApplication manifests",
                "6. Configure IRSA for Glue Catalog and S3 access",
                "7. Set up SPOT instance handling",
                "8. Test with small dataset before full migration"
            ])
        elif recommended_compute == AWSService.LAMBDA:
            steps.extend([
                "2. Break down ETL into smaller functions",
                "3. Create Lambda function with appropriate memory/timeout",
                "4. Configure S3 event triggers or EventBridge schedule",
                "5. Test with sample data"
            ])
        else:
            steps.extend([
                "2. Optimize existing Glue job configuration",
                "3. Enable Flex mode if applicable",
                "4. Review and optimize Spark code"
            ])

        # Orchestration
        if orchestration:
            orch_service = orchestration[0].service
            if orch_service == AWSService.STEP_FUNCTIONS:
                steps.append("7. Create Step Functions state machine for workflow")
            elif orch_service == AWSService.MWAA:
                steps.append("7. Deploy MWAA environment and create DAGs")

        steps.append(f"{len(steps) + 1}. Set up monitoring and alerts")
        steps.append(f"{len(steps) + 1}. Document and train team")

        return steps

    def _estimate_monthly_cost(self,
                                analysis: Dict[str, Any],
                                compute: List[ServiceRecommendation],
                                orchestration: List[ServiceRecommendation],
                                storage: List[ServiceRecommendation]) -> float:
        """Estimate monthly cost."""
        data_volume = analysis['data_volume_gb']
        runs_per_month = 30  # Assume daily

        # Compute cost
        compute_cost = 0
        if compute:
            service = compute[0].service
            if service == AWSService.GLUE:
                compute_cost = data_volume * 0.15 * runs_per_month
            elif service == AWSService.GLUE_FLEX:
                compute_cost = data_volume * 0.10 * runs_per_month
            elif service == AWSService.EMR_SERVERLESS:
                compute_cost = data_volume * 0.05 * runs_per_month
            elif service == AWSService.EKS:
                compute_cost = data_volume * 0.03 * runs_per_month
            elif service == AWSService.LAMBDA:
                compute_cost = 10  # Minimal for small workloads

        # Storage cost
        storage_cost = data_volume * 0.023  # S3 Standard

        # Orchestration cost
        orch_cost = 0
        if orchestration:
            service = orchestration[0].service
            if service == AWSService.STEP_FUNCTIONS:
                orch_cost = 5  # Estimated
            elif service == AWSService.MWAA:
                orch_cost = 0.49 * 24 * 30  # ~$353 for small

        return round(compute_cost + storage_cost + orch_cost, 2)

    def generate_report(self, recommendation: ArchitectureRecommendation) -> str:
        """Generate human-readable recommendation report."""
        report = []
        report.append("=" * 70)
        report.append("AWS ARCHITECTURE RECOMMENDATION REPORT")
        report.append("=" * 70)
        report.append("")

        report.append(recommendation.architecture_diagram)

        report.append("\n--- COMPUTE ---")
        for rec in recommendation.compute:
            report.append(f"  [{rec.priority.upper()}] {rec.service.value}")
            report.append(f"    Reason: {rec.reason}")
            report.append(f"    Cost: {rec.estimated_cost}")
            report.append(f"    Setup: {rec.setup_complexity}")
            if rec.alternatives:
                report.append(f"    Alternatives: {', '.join(a.value for a in rec.alternatives)}")
            report.append("")

        report.append("--- ORCHESTRATION ---")
        for rec in recommendation.orchestration:
            report.append(f"  [{rec.priority.upper()}] {rec.service.value}")
            report.append(f"    Reason: {rec.reason}")
            report.append("")

        report.append("--- STORAGE ---")
        for rec in recommendation.storage:
            report.append(f"  [{rec.priority.upper()}] {rec.service.value}")
            report.append(f"    Reason: {rec.reason}")
            report.append("")

        report.append("--- MIGRATION STEPS ---")
        for step in recommendation.migration_steps:
            report.append(f"  {step}")
        report.append("")

        report.append(f"--- ESTIMATED MONTHLY COST: ${recommendation.estimated_monthly_cost} ---")
        report.append("")
        report.append("=" * 70)

        return "\n".join(report)


# Example usage
if __name__ == '__main__':
    engine = AWSRecommendationsEngine()

    # Test configuration
    config = {
        'workload': {
            'name': 'customer_360_etl',
            'data_volume': 'high',  # ~100GB
            'complexity': 'high',
            'criticality': 'medium',
            'schedule_type': 'daily'
        },
        'data_sources': [
            {'type': 'glue_catalog', 'database': 'ecommerce_db', 'table': 'orders'},
            {'type': 'glue_catalog', 'database': 'ecommerce_db', 'table': 'customers'}
        ],
        'compliance': {
            'enabled': True,
            'frameworks': ['GDPR']
        },
        'dependencies': ['upstream_job_1', 'upstream_job_2']
    }

    recommendation = engine.recommend_architecture(config)
    print(engine.generate_report(recommendation))
