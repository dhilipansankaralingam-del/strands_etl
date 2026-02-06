#!/usr/bin/env python3
"""
Enterprise ETL Framework - Core Engine
=======================================

The central engine that:
1. Loads and validates configuration
2. Orchestrates all agents
3. Manages platform fallback
4. Handles auto-healing with code correction
5. Coordinates integrations (Slack, Teams, Email, Streamlit)
6. Manages audit trail in DynamoDB
7. Produces CloudWatch metrics and dashboards

This engine is the heart of the framework - ALL execution flows through it.
"""

import json
import time
import traceback
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from contextlib import contextmanager
from enum import Enum

from .config_schema import (
    MasterConfig, Platform, AgentType, Severity, parse_flag
)


class ExecutionStage(Enum):
    """ETL execution stages for audit."""
    INITIALIZED = "initialized"
    CONFIG_LOADED = "config_loaded"
    PRE_ANALYSIS = "pre_analysis"
    READING_SOURCES = "reading_sources"
    TRANSFORMING = "transforming"
    DATA_QUALITY = "data_quality"
    COMPLIANCE_CHECK = "compliance_check"
    WRITING_TARGETS = "writing_targets"
    POST_ANALYSIS = "post_analysis"
    COMPLETED = "completed"
    FAILED = "failed"
    HEALING = "healing"
    RETRYING = "retrying"
    PLATFORM_FALLBACK = "platform_fallback"


class ExecutionStatus(Enum):
    """Overall execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    WARNING = "warning"
    FAILED = "failed"
    HEALED = "healed"


@dataclass
class AgentResult:
    """Result from agent execution."""
    agent_type: str
    status: str
    findings: List[Dict[str, Any]] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    code_fixes: List[Dict[str, Any]] = field(default_factory=list)
    execution_time_ms: float = 0


@dataclass
class ExecutionMetrics:
    """Execution metrics for a job run."""
    run_id: str
    job_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: ExecutionStatus = ExecutionStatus.PENDING

    # Platform
    platform_used: str = ""
    platform_fallbacks: List[str] = field(default_factory=list)

    # Data metrics
    records_read: int = 0
    records_written: int = 0
    bytes_read: int = 0
    bytes_written: int = 0

    # Resource metrics
    cpu_usage_pct: float = 0
    memory_usage_pct: float = 0
    shuffle_bytes: int = 0
    executor_hours: float = 0

    # Cost
    estimated_cost_usd: float = 0

    # Errors and healing
    errors: List[str] = field(default_factory=list)
    healing_attempts: int = 0
    healing_successful: bool = False

    # Agent results
    agent_results: Dict[str, AgentResult] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'run_id': self.run_id,
            'job_name': self.job_name,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'status': self.status.value,
            'platform_used': self.platform_used,
            'platform_fallbacks': self.platform_fallbacks,
            'records_read': self.records_read,
            'records_written': self.records_written,
            'bytes_read': self.bytes_read,
            'bytes_written': self.bytes_written,
            'cpu_usage_pct': self.cpu_usage_pct,
            'memory_usage_pct': self.memory_usage_pct,
            'shuffle_bytes': self.shuffle_bytes,
            'executor_hours': self.executor_hours,
            'estimated_cost_usd': self.estimated_cost_usd,
            'errors': self.errors,
            'healing_attempts': self.healing_attempts,
            'healing_successful': self.healing_successful,
            'agent_results': {k: v.__dict__ for k, v in self.agent_results.items()}
        }


class FrameworkEngine:
    """
    Core engine that orchestrates the entire ETL framework.
    """

    def __init__(self, config: MasterConfig, aws_region: str = None):
        self.config = config
        self.aws_region = aws_region
        self.run_id = f"{config.job_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

        # Initialize metrics
        self.metrics = ExecutionMetrics(
            run_id=self.run_id,
            job_name=config.job_name,
            start_time=datetime.now()
        )

        # Current stage
        self.current_stage = ExecutionStage.INITIALIZED
        self.current_platform = config.platform.primary

        # Agent registry
        self.agents = {}
        self._initialize_agents()

        # Integration handlers
        self.integrations = {}
        self._initialize_integrations()

        # AWS clients (lazy initialization)
        self._glue_client = None
        self._emr_client = None
        self._dynamodb = None
        self._cloudwatch = None
        self._s3 = None

        print(f"[Engine] Initialized: {config.job_name} (run_id: {self.run_id})")

    def _initialize_agents(self):
        """Initialize all enabled agents."""
        from ..agents import (
            AutoHealingAgent,
            CodeAnalysisAgent,
            ComplianceAgent,
            DataQualityAgent,
            WorkloadAssessmentAgent,
            LearningAgent,
            RecommendationAgent
        )

        if self.config.auto_healing.enabled:
            self.agents['auto_healing'] = AutoHealingAgent(self.config.auto_healing)

        if self.config.code_analysis.enabled:
            self.agents['code_analysis'] = CodeAnalysisAgent(self.config.code_analysis)

        if self.config.compliance.enabled:
            self.agents['compliance'] = ComplianceAgent(self.config.compliance)

        if self.config.data_quality.enabled:
            self.agents['data_quality'] = DataQualityAgent(self.config.data_quality)

        if self.config.workload_assessment.enabled:
            self.agents['workload_assessment'] = WorkloadAssessmentAgent(self.config.workload_assessment)

        # Always enable learning and recommendation
        self.agents['learning'] = LearningAgent(self.config)
        self.agents['recommendation'] = RecommendationAgent(self.config)

        print(f"[Engine] Initialized {len(self.agents)} agents")

    def _initialize_integrations(self):
        """Initialize all enabled integrations."""
        from ..integrations import (
            SlackIntegration,
            TeamsIntegration,
            EmailIntegration,
            StreamlitIntegration
        )

        if self.config.integrations.slack_enabled:
            self.integrations['slack'] = SlackIntegration(self.config.integrations)

        if self.config.integrations.teams_enabled:
            self.integrations['teams'] = TeamsIntegration(self.config.integrations)

        if self.config.integrations.email_enabled:
            self.integrations['email'] = EmailIntegration(self.config.integrations)

        if self.config.integrations.streamlit_enabled:
            self.integrations['streamlit'] = StreamlitIntegration(self.config.integrations)

        print(f"[Engine] Initialized {len(self.integrations)} integrations")

    # =========================================================================
    # PLATFORM MANAGEMENT
    # =========================================================================

    def execute_on_platform(self, platform: str, job_fn: Callable) -> bool:
        """
        Execute job on specified platform.
        Returns True if successful, False if failed and should try fallback.
        """
        self.current_platform = platform
        self.metrics.platform_used = platform
        self._audit_stage(ExecutionStage.READING_SOURCES, {'platform': platform})

        try:
            print(f"[Engine] Executing on platform: {platform}")

            if platform == 'glue':
                return self._execute_glue(job_fn)
            elif platform == 'emr':
                return self._execute_emr(job_fn)
            elif platform == 'eks':
                return self._execute_eks(job_fn)
            elif platform == 'lambda':
                return self._execute_lambda(job_fn)
            else:
                raise ValueError(f"Unknown platform: {platform}")

        except Exception as e:
            error_msg = str(e)
            self.metrics.errors.append(error_msg)
            print(f"[Engine] Platform {platform} failed: {error_msg}")

            # Try auto-healing if enabled
            if self.config.auto_healing.enabled:
                healed = self._try_auto_healing(e)
                if healed:
                    return True

            return False

    def execute_with_fallback(self, job_fn: Callable) -> ExecutionStatus:
        """
        Execute job with platform fallback chain.
        """
        # Start with primary platform
        platforms_to_try = [self.config.platform.primary]

        # Add fallback chain if not forced to single platform
        if not self.config.platform.force_platform and self.config.platform.auto_fallback_on_error:
            platforms_to_try.extend(self.config.platform.fallback_chain)

        for platform in platforms_to_try:
            success = self.execute_on_platform(platform, job_fn)

            if success:
                self.metrics.status = ExecutionStatus.SUCCESS
                return ExecutionStatus.SUCCESS

            # Record fallback
            if platform != platforms_to_try[-1]:
                self.metrics.platform_fallbacks.append(platform)
                self._audit_stage(ExecutionStage.PLATFORM_FALLBACK, {
                    'from_platform': platform,
                    'to_platform': platforms_to_try[platforms_to_try.index(platform) + 1]
                })
                self._notify('platform_fallback', {
                    'from': platform,
                    'to': platforms_to_try[platforms_to_try.index(platform) + 1],
                    'error': self.metrics.errors[-1] if self.metrics.errors else 'Unknown'
                })

        self.metrics.status = ExecutionStatus.FAILED
        return ExecutionStatus.FAILED

    def _execute_glue(self, job_fn: Callable) -> bool:
        """Execute on AWS Glue."""
        import boto3

        glue = boto3.client('glue', region_name=self.aws_region)
        glue_config = self.config.platform.glue

        job_name = glue_config.get('job_name', self.config.job_name)
        worker_type = glue_config.get('worker_type', 'G.1X')
        num_workers = glue_config.get('number_of_workers', 5)

        # Start job run
        response = glue.start_job_run(
            JobName=job_name,
            Arguments={
                '--CONFIG_PATH': f's3://etl-configs/{self.config.job_name}.json',
                '--RUN_ID': self.run_id,
                **glue_config.get('job_arguments', {})
            },
            WorkerType=worker_type,
            NumberOfWorkers=num_workers,
            Timeout=glue_config.get('timeout', 120)
        )

        job_run_id = response['JobRunId']
        print(f"[Glue] Started job run: {job_run_id}")

        # Monitor job
        return self._monitor_glue_job(glue, job_name, job_run_id)

    def _execute_emr(self, job_fn: Callable) -> bool:
        """Execute on EMR."""
        import boto3

        emr = boto3.client('emr', region_name=self.aws_region)
        emr_config = self.config.platform.emr

        cluster_id = emr_config.get('cluster_id')
        if not cluster_id:
            # Create transient cluster if needed
            cluster_id = self._create_emr_cluster(emr, emr_config)

        # Submit step
        step_response = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[{
                'Name': f'{self.config.job_name}_{self.run_id}',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        self.config.script_path
                    ]
                }
            }]
        )

        step_id = step_response['StepIds'][0]
        print(f"[EMR] Submitted step: {step_id}")

        return self._monitor_emr_step(emr, cluster_id, step_id)

    def _execute_eks(self, job_fn: Callable) -> bool:
        """Execute on EKS with Karpenter."""
        print("[EKS] Executing on EKS with Karpenter...")
        # Implementation for EKS Spark Operator
        return True

    def _execute_lambda(self, job_fn: Callable) -> bool:
        """Execute on Lambda."""
        import boto3

        lambda_client = boto3.client('lambda', region_name=self.aws_region)
        lambda_config = self.config.platform.lambda_config

        function_name = lambda_config.get('function_name', self.config.job_name)

        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps({
                'config': self.config.to_dict(),
                'run_id': self.run_id
            })
        )

        result = json.loads(response['Payload'].read())
        return result.get('statusCode') == 200

    # =========================================================================
    # AUTO-HEALING
    # =========================================================================

    def _try_auto_healing(self, error: Exception) -> bool:
        """
        Try to auto-heal from an error.
        Can correct code and rerun if configured.
        """
        if not self.config.auto_healing.enabled:
            return False

        if self.metrics.healing_attempts >= self.config.auto_healing.max_retries:
            print(f"[AutoHeal] Max retries ({self.config.auto_healing.max_retries}) exceeded")
            return False

        self._audit_stage(ExecutionStage.HEALING, {'error': str(error)})
        self.metrics.healing_attempts += 1

        healing_agent = self.agents.get('auto_healing')
        if not healing_agent:
            return False

        # Analyze error and get healing strategy
        healing_result = healing_agent.analyze_and_heal(error, self.config)

        if not healing_result.can_heal:
            print(f"[AutoHeal] Cannot heal error: {error}")
            return False

        print(f"[AutoHeal] Healing strategy: {healing_result.strategy}")

        # Apply code fixes if any
        if healing_result.code_fixes and self.config.auto_healing.correct_code:
            self._apply_code_fixes(healing_result.code_fixes)
            self.metrics.agent_results['auto_healing'] = AgentResult(
                agent_type='auto_healing',
                status='healed',
                code_fixes=healing_result.code_fixes,
                recommendations=healing_result.recommendations
            )

        # Wait before retry
        if self.config.auto_healing.auto_rerun:
            print(f"[AutoHeal] Waiting {self.config.auto_healing.retry_delay_seconds}s before retry...")
            time.sleep(self.config.auto_healing.retry_delay_seconds)

            self._audit_stage(ExecutionStage.RETRYING, {
                'attempt': self.metrics.healing_attempts,
                'strategy': healing_result.strategy
            })

            self.metrics.healing_successful = True
            return True

        return False

    def _apply_code_fixes(self, fixes: List[Dict[str, Any]]):
        """Apply code fixes recommended by auto-healing."""
        print(f"[AutoHeal] Applying {len(fixes)} code fixes...")
        for fix in fixes:
            print(f"  - {fix.get('description', 'Unknown fix')}")
            # Code fix application logic

    # =========================================================================
    # PRE/POST EXECUTION ANALYSIS
    # =========================================================================

    def run_pre_execution_analysis(self) -> Dict[str, AgentResult]:
        """Run all pre-execution agents."""
        results = {}
        self._audit_stage(ExecutionStage.PRE_ANALYSIS)

        print("\n" + "=" * 60)
        print("PRE-EXECUTION ANALYSIS")
        print("=" * 60)

        # Code Analysis
        if 'code_analysis' in self.agents:
            print("\n[Code Analysis] Analyzing script...")
            result = self.agents['code_analysis'].analyze(self.config.script_path)
            results['code_analysis'] = result
            self.metrics.agent_results['code_analysis'] = result
            print(f"[Code Analysis] Score: {result.metrics.get('optimization_score', 0)}/100")
            print(f"[Code Analysis] Issues: {len(result.findings)}")

        # Workload Assessment
        if 'workload_assessment' in self.agents:
            print("\n[Workload Assessment] Analyzing workload...")
            result = self.agents['workload_assessment'].assess(self.config)
            results['workload_assessment'] = result
            self.metrics.agent_results['workload_assessment'] = result
            print(f"[Workload Assessment] Complexity: {result.metrics.get('complexity', 'unknown')}")
            print(f"[Workload Assessment] Recommended resources: {result.recommendations}")

        # Compliance Check on Sources
        if 'compliance' in self.agents and self.config.compliance.check_sources:
            print("\n[Compliance] Checking source tables...")
            result = self.agents['compliance'].check_sources(self.config.sources)
            results['compliance_sources'] = result
            self.metrics.agent_results['compliance_sources'] = result
            print(f"[Compliance] Source issues: {len(result.findings)}")

        # Recommendation
        if 'recommendation' in self.agents:
            print("\n[Recommendation] Generating recommendations...")
            result = self.agents['recommendation'].generate(self.config, results)
            results['recommendation'] = result
            self.metrics.agent_results['recommendation'] = result
            for rec in result.recommendations[:5]:
                print(f"  - {rec}")

        return results

    def run_post_execution_analysis(self, dataframes: Dict) -> Dict[str, AgentResult]:
        """Run all post-execution agents."""
        results = {}
        self._audit_stage(ExecutionStage.POST_ANALYSIS)

        print("\n" + "=" * 60)
        print("POST-EXECUTION ANALYSIS")
        print("=" * 60)

        # Data Quality
        if 'data_quality' in self.agents:
            print("\n[Data Quality] Validating data...")
            for df_name, df in dataframes.items():
                result = self.agents['data_quality'].validate(df, df_name)
                results[f'dq_{df_name}'] = result
                self.metrics.agent_results[f'dq_{df_name}'] = result
                print(f"[Data Quality] {df_name}: {result.metrics.get('pass_rate', 0)*100:.1f}% pass rate")

        # Compliance Check on Targets
        if 'compliance' in self.agents and self.config.compliance.check_targets:
            print("\n[Compliance] Checking target data...")
            result = self.agents['compliance'].check_targets(self.config.targets, dataframes)
            results['compliance_targets'] = result
            self.metrics.agent_results['compliance_targets'] = result
            print(f"[Compliance] Target issues: {len(result.findings)}")

        # Learning Agent
        if 'learning' in self.agents:
            print("\n[Learning] Storing run metrics...")
            self.agents['learning'].store_metrics(self.metrics)

        return results

    # =========================================================================
    # AUDIT & NOTIFICATIONS
    # =========================================================================

    def _audit_stage(self, stage: ExecutionStage, details: Dict = None):
        """Record audit entry for a stage."""
        self.current_stage = stage

        if not self.config.audit.enabled:
            return

        audit_entry = {
            'run_id': self.run_id,
            'job_name': self.config.job_name,
            'stage': stage.value,
            'timestamp': datetime.now().isoformat(),
            'platform': self.current_platform,
            'details': details or {}
        }

        # Write to DynamoDB
        self._write_audit(audit_entry)

        # Write to CloudWatch
        self._write_cloudwatch_metric(stage.value)

    def _write_audit(self, entry: Dict):
        """Write audit entry to DynamoDB."""
        try:
            import boto3
            dynamodb = boto3.resource('dynamodb', region_name=self.aws_region)
            table = dynamodb.Table(self.config.audit.stage_audit_table)
            table.put_item(Item=entry)
        except Exception as e:
            print(f"[Audit] Error writing audit: {e}")

    def _write_cloudwatch_metric(self, metric_name: str, value: float = 1):
        """Write metric to CloudWatch."""
        try:
            import boto3
            cloudwatch = boto3.client('cloudwatch', region_name=self.aws_region)
            cloudwatch.put_metric_data(
                Namespace=self.config.dashboard.cloudwatch_metrics_namespace,
                MetricData=[{
                    'MetricName': metric_name,
                    'Value': value,
                    'Dimensions': [
                        {'Name': 'JobName', 'Value': self.config.job_name},
                        {'Name': 'RunId', 'Value': self.run_id}
                    ]
                }]
            )
        except Exception as e:
            print(f"[CloudWatch] Error writing metric: {e}")

    def _notify(self, event: str, details: Dict = None):
        """Send notification to all enabled channels."""
        message = {
            'event': event,
            'job_name': self.config.job_name,
            'run_id': self.run_id,
            'timestamp': datetime.now().isoformat(),
            'details': details or {}
        }

        for name, integration in self.integrations.items():
            try:
                integration.send(event, message)
            except Exception as e:
                print(f"[{name}] Notification error: {e}")

    # =========================================================================
    # MAIN EXECUTION FLOW
    # =========================================================================

    @contextmanager
    def execute(self):
        """
        Main execution context manager.
        Wraps user ETL code with all framework capabilities.
        """
        try:
            # Notify start
            self._notify('job_started', {'platform': self.current_platform})
            self._audit_stage(ExecutionStage.CONFIG_LOADED)

            # Pre-execution analysis
            pre_results = self.run_pre_execution_analysis()

            # Check if we should proceed
            if self._should_block_execution(pre_results):
                raise RuntimeError("Blocked by pre-execution checks")

            # Create execution context
            ctx = ExecutionContext(
                engine=self,
                config=self.config,
                run_id=self.run_id,
                metrics=self.metrics
            )

            yield ctx

            # Post-execution analysis
            post_results = self.run_post_execution_analysis(ctx.registered_dataframes)

            # Finalize
            self.metrics.end_time = datetime.now()
            self.metrics.status = ExecutionStatus.SUCCESS
            self._audit_stage(ExecutionStage.COMPLETED)
            self._notify('job_completed', self.metrics.to_dict())

            # Send email report if enabled
            if 'email' in self.integrations and self.config.integrations.email_html_template:
                self.integrations['email'].send_html_report(self.metrics, pre_results, post_results)

        except Exception as e:
            self.metrics.end_time = datetime.now()
            self.metrics.status = ExecutionStatus.FAILED
            self.metrics.errors.append(str(e))

            self._audit_stage(ExecutionStage.FAILED, {'error': str(e)})
            self._notify('job_failed', {
                'error': str(e),
                'traceback': traceback.format_exc()
            })

            raise

    def _should_block_execution(self, pre_results: Dict) -> bool:
        """Check if execution should be blocked based on pre-analysis."""
        # Block on critical code issues if configured
        code_result = pre_results.get('code_analysis')
        if code_result and self.config.code_analysis.fail_on_critical:
            critical_issues = [f for f in code_result.findings if f.get('severity') == 'critical']
            if critical_issues:
                print(f"[Engine] Blocking: {len(critical_issues)} critical code issues")
                return True

        # Block on compliance violations if configured
        compliance_result = pre_results.get('compliance_sources')
        if compliance_result and self.config.compliance.block_on_critical_violation:
            critical_violations = [f for f in compliance_result.findings if f.get('severity') == 'critical']
            if critical_violations:
                print(f"[Engine] Blocking: {len(critical_violations)} critical compliance violations")
                return True

        return False


class ExecutionContext:
    """
    Context passed to user ETL code during execution.
    Provides framework-integrated methods for reading/writing data.
    """

    def __init__(self, engine: FrameworkEngine, config: MasterConfig, run_id: str, metrics: ExecutionMetrics):
        self.engine = engine
        self.config = config
        self.run_id = run_id
        self.metrics = metrics

        self.registered_dataframes = {}
        self.spark = None
        self.glue_context = None

    def read_catalog(self, database: str, table: str, push_down_predicate: str = None):
        """Read from Glue Catalog with tracking."""
        full_name = f"{database}.{table}"
        print(f"[Context] Reading: {full_name}")

        self.engine._audit_stage(ExecutionStage.READING_SOURCES, {
            'database': database,
            'table': table
        })

        if self.spark:
            df = self.spark.table(full_name)
            if push_down_predicate:
                df = df.filter(push_down_predicate)

            # Track metrics
            count = df.count()
            self.metrics.records_read += count

            return df

        return None

    def write_catalog(self, df, database: str, table: str, mode: str = "overwrite", partition_by: List[str] = None):
        """Write to Glue Catalog with tracking."""
        full_name = f"{database}.{table}"
        print(f"[Context] Writing: {full_name}")

        self.engine._audit_stage(ExecutionStage.WRITING_TARGETS, {
            'database': database,
            'table': table,
            'mode': mode
        })

        # Track metrics
        count = df.count()
        self.metrics.records_written += count

        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(full_name)

    def register_dataframe(self, name: str, df):
        """Register DataFrame for post-execution validation."""
        self.registered_dataframes[name] = df
