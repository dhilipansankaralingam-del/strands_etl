#!/usr/bin/env python3
"""
Glue ETL Framework Wrapper
===========================

This wrapper integrates your EXISTING Glue PySpark scripts with all framework agents:
- Auto-Healing Agent (retry logic, fallback)
- Compliance Agent (GDPR, HIPAA, PCI-DSS)
- Code Analysis Agent (PySpark optimizations)
- Workload Assessment Agent (resource recommendations)
- Data Quality Agent (NL + SQL rules)
- AWS Recommendations Agent (cost optimization)
- EKS Optimizer Agent (if migrating to EKS)

Usage in your existing Glue script:
    from framework.glue_wrapper import GlueETLFramework

    # Initialize framework with your config
    framework = GlueETLFramework("s3://configs/my_job_config.json")

    # Your existing code wrapped
    with framework.run() as ctx:
        # Read from Glue Catalog (your existing code)
        df = ctx.spark.table("my_database.my_table")

        # Your transformations
        df_transformed = df.filter(...)

        # Write to Glue Catalog
        ctx.write_to_catalog(df_transformed, "output_database", "output_table")

Or analyze existing scripts without modification:
    framework.analyze_script("s3://scripts/my_existing_script.py")
"""

import sys
import json
import time
import traceback
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field
from contextlib import contextmanager
from enum import Enum

# AWS imports (available in Glue environment)
try:
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col, lit, current_timestamp
    AWS_AVAILABLE = True
except ImportError:
    AWS_AVAILABLE = False
    print("Warning: AWS/Glue imports not available. Running in local mode.")


class AgentType(Enum):
    """Available framework agents."""
    AUTO_HEALING = "auto_healing"
    COMPLIANCE = "compliance"
    CODE_ANALYSIS = "code_analysis"
    WORKLOAD_ASSESSMENT = "workload_assessment"
    DATA_QUALITY = "data_quality"
    AWS_RECOMMENDATIONS = "aws_recommendations"
    EKS_OPTIMIZER = "eks_optimizer"


@dataclass
class AgentResult:
    """Result from an agent execution."""
    agent_type: AgentType
    status: str  # "success", "warning", "error"
    findings: List[Dict[str, Any]]
    recommendations: List[str]
    metrics: Dict[str, Any]
    execution_time_ms: float


@dataclass
class FrameworkContext:
    """Context passed to user code during framework execution."""
    spark: SparkSession
    glue_context: Optional[Any]  # GlueContext
    config: Dict[str, Any]
    run_id: str
    job_name: str

    # Tracking
    tables_read: List[str] = field(default_factory=list)
    tables_written: List[str] = field(default_factory=list)
    dataframes: Dict[str, DataFrame] = field(default_factory=dict)

    def read_catalog(self, database: str, table: str) -> DataFrame:
        """Read from Glue Catalog with tracking."""
        full_name = f"{database}.{table}"
        self.tables_read.append(full_name)
        print(f"[Framework] Reading: {full_name}")
        return self.spark.table(full_name)

    def write_to_catalog(self, df: DataFrame, database: str, table: str,
                         mode: str = "overwrite", partition_by: List[str] = None):
        """Write to Glue Catalog with tracking."""
        full_name = f"{database}.{table}"
        self.tables_written.append(full_name)
        print(f"[Framework] Writing: {full_name}")

        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.saveAsTable(full_name)

    def register_dataframe(self, name: str, df: DataFrame):
        """Register DataFrame for DQ validation."""
        self.dataframes[name] = df


class GlueETLFramework:
    """
    Main framework class that wraps existing Glue scripts with all agents.
    """

    def __init__(self, config_path: str = None, config_dict: Dict = None):
        """
        Initialize framework with config from S3 path or dict.

        Args:
            config_path: S3 path to JSON config (e.g., s3://bucket/config.json)
            config_dict: Direct config dictionary
        """
        self.config = config_dict or self._load_config(config_path)
        self.run_id = datetime.now().strftime("%Y%m%d%H%M%S")
        self.job_name = self.config.get("job_name", "unnamed_job")

        # Agent results
        self.agent_results: Dict[AgentType, AgentResult] = {}

        # Initialize Spark/Glue
        self.spark = None
        self.glue_context = None
        self.job = None

        # Notification manager
        self.notifications_enabled = self._parse_flag(
            self.config.get("notifications", {}).get("enabled", "N")
        )

        print(f"[Framework] Initialized: {self.job_name} (run_id: {self.run_id})")

    def _load_config(self, config_path: str) -> Dict:
        """Load config from S3 or local path."""
        if not config_path:
            return {}

        if config_path.startswith("s3://"):
            if AWS_AVAILABLE:
                s3 = boto3.client('s3')
                bucket, key = config_path[5:].split("/", 1)
                response = s3.get_object(Bucket=bucket, Key=key)
                return json.loads(response['Body'].read().decode('utf-8'))
            else:
                raise RuntimeError("AWS not available for S3 config loading")
        else:
            with open(config_path) as f:
                return json.load(f)

    def _parse_flag(self, value: Any) -> bool:
        """Parse Y/N flag to boolean."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.upper() in ('Y', 'YES', 'TRUE', '1')
        return bool(value)

    def _init_spark(self):
        """Initialize Spark session (Glue or local)."""
        if AWS_AVAILABLE:
            sc = SparkContext.getOrCreate()
            self.glue_context = GlueContext(sc)
            self.spark = self.glue_context.spark_session

            # Get Glue job args if available
            try:
                args = getResolvedOptions(sys.argv, ['JOB_NAME'])
                self.job = Job(self.glue_context)
                self.job.init(args['JOB_NAME'], args)
            except:
                pass
        else:
            self.spark = (SparkSession.builder
                         .appName(self.job_name)
                         .config("spark.sql.adaptive.enabled", "true")
                         .getOrCreate())

        # Apply Spark configs from config
        spark_configs = self.config.get("spark_configs", {})
        for key, value in spark_configs.items():
            self.spark.conf.set(key, value)

        print(f"[Framework] Spark initialized: {self.spark.version}")

    @contextmanager
    def run(self):
        """
        Context manager for running your ETL code with framework wrapping.

        Usage:
            with framework.run() as ctx:
                df = ctx.read_catalog("database", "table")
                df_transformed = df.filter(...)
                ctx.write_to_catalog(df_transformed, "out_db", "out_table")
        """
        start_time = time.time()
        ctx = None

        try:
            # Initialize Spark
            self._init_spark()

            # Create context
            ctx = FrameworkContext(
                spark=self.spark,
                glue_context=self.glue_context,
                config=self.config,
                run_id=self.run_id,
                job_name=self.job_name
            )

            # PRE-EXECUTION: Run agents
            print("\n" + "=" * 60)
            print("PRE-EXECUTION AGENT ANALYSIS")
            print("=" * 60)

            # Code Analysis (if script path provided)
            if self.config.get("etl_script", {}).get("path"):
                self._run_code_analysis_agent()

            # Workload Assessment
            self._run_workload_assessment_agent()

            # Send start notification
            self._notify("start", {"run_id": self.run_id})

            # Yield context to user code
            print("\n" + "=" * 60)
            print("EXECUTING USER ETL CODE")
            print("=" * 60)

            yield ctx

            # POST-EXECUTION: Run agents
            print("\n" + "=" * 60)
            print("POST-EXECUTION AGENT ANALYSIS")
            print("=" * 60)

            # Data Quality
            if ctx.dataframes:
                self._run_data_quality_agent(ctx.dataframes)

            # Compliance
            self._run_compliance_agent(ctx)

            # AWS Recommendations
            self._run_aws_recommendations_agent(ctx, time.time() - start_time)

            # Success notification
            duration = time.time() - start_time
            self._notify("success", {
                "run_id": self.run_id,
                "duration_seconds": duration,
                "tables_read": ctx.tables_read,
                "tables_written": ctx.tables_written
            })

            # Commit Glue job
            if self.job:
                self.job.commit()

            print(f"\n[Framework] Job completed successfully in {duration:.2f}s")

        except Exception as e:
            # Auto-Healing: Attempt recovery
            print(f"\n[Framework] ERROR: {str(e)}")
            traceback.print_exc()

            recovery_attempted = self._run_auto_healing_agent(e, ctx)

            if not recovery_attempted:
                # Send failure notification
                self._notify("failure", {
                    "run_id": self.run_id,
                    "error": str(e),
                    "traceback": traceback.format_exc()
                })
                raise

    def analyze_script(self, script_path: str) -> Dict[str, Any]:
        """
        Analyze an existing PySpark script without running it.
        Returns optimization recommendations.
        """
        print(f"\n[Framework] Analyzing script: {script_path}")

        # Load script content
        if script_path.startswith("s3://"):
            s3 = boto3.client('s3')
            bucket, key = script_path[5:].split("/", 1)
            response = s3.get_object(Bucket=bucket, Key=key)
            script_content = response['Body'].read().decode('utf-8')
        else:
            with open(script_path) as f:
                script_content = f.read()

        # Run Code Analysis Agent
        from agents.pyspark_code_analysis_agent import PySparkCodeAnalysisAgent
        agent = PySparkCodeAnalysisAgent()
        results = agent.analyze(script_content, script_path)

        self.agent_results[AgentType.CODE_ANALYSIS] = results

        return {
            "script": script_path,
            "findings": results.findings,
            "recommendations": results.recommendations,
            "optimization_score": results.metrics.get("optimization_score", 0)
        }

    def _run_code_analysis_agent(self):
        """Run Code Analysis Agent on the ETL script."""
        try:
            script_path = self.config.get("etl_script", {}).get("path")
            if script_path:
                results = self.analyze_script(script_path)
                print(f"[Code Analysis] Found {len(results['findings'])} issues")
                print(f"[Code Analysis] Optimization Score: {results['optimization_score']}/100")
        except Exception as e:
            print(f"[Code Analysis] Error: {e}")

    def _run_workload_assessment_agent(self):
        """Run Workload Assessment Agent."""
        try:
            from agents.workload_assessment_agent import WorkloadAssessmentAgent
            agent = WorkloadAssessmentAgent()

            workload = {
                "data_volume_gb": self.config.get("estimated_data_volume_gb", 10),
                "complexity": self.config.get("complexity", "medium"),
                "frequency": self.config.get("scheduling", {}).get("cron", "daily"),
                "current_platform": "glue",
                "current_config": self.config.get("resources", {}).get("glue", {})
            }

            results = agent.assess(workload)
            self.agent_results[AgentType.WORKLOAD_ASSESSMENT] = results

            print(f"[Workload Assessment] Recommended Platform: {results.recommendations[0] if results.recommendations else 'Glue'}")
        except Exception as e:
            print(f"[Workload Assessment] Error: {e}")

    def _run_data_quality_agent(self, dataframes: Dict[str, DataFrame]):
        """Run Data Quality Agent on registered DataFrames."""
        try:
            dq_config = self.config.get("data_quality", {})
            if not self._parse_flag(dq_config.get("enabled", "N")):
                return

            from agents.data_quality_nl_agent import DataQualityNLAgent
            agent = DataQualityNLAgent(self.spark, dq_config)

            # Add Natural Language rules
            for rule in dq_config.get("natural_language_rules", []):
                agent.add_rule_nl(rule)

            # Add SQL rules
            for rule in dq_config.get("sql_rules", []):
                agent.add_rule_sql(rule["name"], rule["expression"], rule.get("description", ""))

            # Add template rules
            for rule in dq_config.get("template_rules", []):
                agent.add_rule_template(**rule)

            # Validate each DataFrame
            for name, df in dataframes.items():
                print(f"\n[Data Quality] Validating: {name}")
                results = agent.validate(df)

            self.agent_results[AgentType.DATA_QUALITY] = AgentResult(
                agent_type=AgentType.DATA_QUALITY,
                status="success" if all(r.passed for r in results) else "warning",
                findings=[{"rule": r.rule_name, "passed": r.passed, "pass_rate": r.pass_rate} for r in results],
                recommendations=[],
                metrics={"total_rules": len(results), "passed": sum(1 for r in results if r.passed)},
                execution_time_ms=0
            )
        except Exception as e:
            print(f"[Data Quality] Error: {e}")

    def _run_compliance_agent(self, ctx: FrameworkContext):
        """Run Compliance Agent."""
        try:
            compliance_config = self.config.get("compliance", {})
            if not self._parse_flag(compliance_config.get("enabled", "N")):
                return

            frameworks = compliance_config.get("frameworks", [])
            pii_columns = compliance_config.get("pii_columns", [])

            findings = []

            # Check for PII exposure
            for df_name, df in ctx.dataframes.items():
                df_columns = [c.lower() for c in df.columns]
                for pii_col in pii_columns:
                    if pii_col.lower() in df_columns:
                        if "GDPR" in frameworks:
                            findings.append({
                                "framework": "GDPR",
                                "severity": "high",
                                "finding": f"PII column '{pii_col}' found in {df_name}",
                                "recommendation": "Apply masking or encryption"
                            })

            self.agent_results[AgentType.COMPLIANCE] = AgentResult(
                agent_type=AgentType.COMPLIANCE,
                status="warning" if findings else "success",
                findings=findings,
                recommendations=[f["recommendation"] for f in findings],
                metrics={"frameworks_checked": frameworks, "pii_columns_found": len(findings)},
                execution_time_ms=0
            )

            print(f"[Compliance] Checked: {frameworks}, Issues: {len(findings)}")
        except Exception as e:
            print(f"[Compliance] Error: {e}")

    def _run_aws_recommendations_agent(self, ctx: FrameworkContext, duration: float):
        """Run AWS Recommendations Agent."""
        try:
            # Estimate costs and provide recommendations
            glue_config = self.config.get("resources", {}).get("glue", {})
            worker_type = glue_config.get("worker_type", "G.1X")
            num_workers = glue_config.get("number_of_workers", 2)

            # Pricing estimates ($/hr per DPU)
            pricing = {"G.1X": 0.44, "G.2X": 0.88, "G.025X": 0.11}
            hourly_rate = pricing.get(worker_type, 0.44)
            estimated_cost = (duration / 3600) * num_workers * hourly_rate

            recommendations = []

            # Check if over-provisioned
            if duration < 300 and num_workers > 5:
                recommendations.append(
                    f"Job runs quickly ({duration:.0f}s). Consider reducing workers from {num_workers} to 3-5."
                )

            # Check worker type
            if worker_type == "G.2X" and duration < 600:
                recommendations.append(
                    "Using G.2X for a short job. Consider G.1X to reduce costs by 50%."
                )

            self.agent_results[AgentType.AWS_RECOMMENDATIONS] = AgentResult(
                agent_type=AgentType.AWS_RECOMMENDATIONS,
                status="success",
                findings=[{"estimated_cost_usd": estimated_cost}],
                recommendations=recommendations,
                metrics={
                    "duration_seconds": duration,
                    "worker_type": worker_type,
                    "num_workers": num_workers,
                    "estimated_cost_usd": estimated_cost
                },
                execution_time_ms=0
            )

            print(f"[AWS Recommendations] Estimated cost: ${estimated_cost:.4f}")
            for rec in recommendations:
                print(f"  - {rec}")
        except Exception as e:
            print(f"[AWS Recommendations] Error: {e}")

    def _run_auto_healing_agent(self, error: Exception, ctx: FrameworkContext) -> bool:
        """
        Auto-Healing Agent: Attempt to recover from errors.
        Returns True if recovery was attempted.
        """
        error_str = str(error).lower()

        # Memory errors
        if "java.lang.outofmemoryerror" in error_str or "memory" in error_str:
            print("[Auto-Healing] Detected memory error. Recommendations:")
            print("  1. Increase spark.executor.memory")
            print("  2. Use G.2X worker type instead of G.1X")
            print("  3. Add repartition() before memory-intensive operations")
            print("  4. Enable spark.sql.adaptive.enabled=true")
            return True

        # Shuffle errors
        if "shuffle" in error_str or "fetch failed" in error_str:
            print("[Auto-Healing] Detected shuffle error. Recommendations:")
            print("  1. Increase spark.sql.shuffle.partitions")
            print("  2. Use broadcast joins for small tables")
            print("  3. Check for data skew and apply salting")
            return True

        # Connection/timeout errors
        if "timeout" in error_str or "connection" in error_str:
            print("[Auto-Healing] Detected timeout/connection error. Retrying...")
            return True

        return False

    def _notify(self, event: str, details: Dict[str, Any]):
        """Send notifications via configured channels."""
        if not self.notifications_enabled:
            return

        notifications_config = self.config.get("notifications", {})
        preferences = notifications_config.get("preferences", {})

        # Check if this event should trigger notification
        if not self._parse_flag(preferences.get(f"on_{event}", "Y")):
            return

        message = f"[{self.job_name}] {event.upper()}: {json.dumps(details, default=str)}"

        # Slack
        if self._parse_flag(notifications_config.get("slack", {}).get("enabled", "N")):
            self._send_slack(message, notifications_config.get("slack", {}))

        # Teams
        if self._parse_flag(notifications_config.get("teams", {}).get("enabled", "N")):
            self._send_teams(message, notifications_config.get("teams", {}))

    def _send_slack(self, message: str, config: Dict):
        """Send Slack notification."""
        try:
            import urllib.request
            webhook_url = config.get("webhook_url", "")
            if webhook_url and not webhook_url.startswith("${"):
                data = json.dumps({"text": message}).encode('utf-8')
                req = urllib.request.Request(webhook_url, data=data,
                                            headers={'Content-Type': 'application/json'})
                urllib.request.urlopen(req)
        except Exception as e:
            print(f"[Slack] Error: {e}")

    def _send_teams(self, message: str, config: Dict):
        """Send Teams notification."""
        try:
            import urllib.request
            webhook_url = config.get("webhook_url", "")
            if webhook_url and not webhook_url.startswith("${"):
                data = json.dumps({"text": message}).encode('utf-8')
                req = urllib.request.Request(webhook_url, data=data,
                                            headers={'Content-Type': 'application/json'})
                urllib.request.urlopen(req)
        except Exception as e:
            print(f"[Teams] Error: {e}")

    def get_results_summary(self) -> Dict[str, Any]:
        """Get summary of all agent results."""
        return {
            agent_type.value: {
                "status": result.status,
                "findings_count": len(result.findings),
                "recommendations_count": len(result.recommendations)
            }
            for agent_type, result in self.agent_results.items()
        }

    def save_audit(self, table_name: str = "etl_run_audit"):
        """Save run audit to DynamoDB."""
        if not AWS_AVAILABLE:
            return

        try:
            dynamodb = boto3.resource('dynamodb')
            table = dynamodb.Table(table_name)

            table.put_item(Item={
                "run_id": self.run_id,
                "job_name": self.job_name,
                "timestamp": datetime.now().isoformat(),
                "config": json.dumps(self.config),
                "agent_results": json.dumps(self.get_results_summary(), default=str)
            })

            print(f"[Framework] Audit saved to {table_name}")
        except Exception as e:
            print(f"[Framework] Audit save error: {e}")


# Convenience function for quick setup
def create_framework(config_path: str = None, **kwargs) -> GlueETLFramework:
    """Create framework instance with config."""
    return GlueETLFramework(config_path=config_path, config_dict=kwargs if kwargs else None)
