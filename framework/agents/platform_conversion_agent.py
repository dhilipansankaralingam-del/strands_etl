#!/usr/bin/env python3
"""
Platform Conversion Agent
=========================

Converts ETL job configurations between different platforms:
- AWS Glue to EMR
- AWS Glue to EKS Spark
- EMR to AWS Glue
- EMR to EKS Spark

This agent handles infrastructure-level conversion including:
- Resource sizing (DPU to core/memory mapping)
- IAM role mappings
- Storage configuration
- Spark configurations
- Cost estimation for target platform

All conversions are stored locally for learning and optimization.
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum


class Platform(Enum):
    """Supported ETL platforms."""
    GLUE = "glue"
    EMR = "emr"
    EKS = "eks"
    DATABRICKS = "databricks"


@dataclass
class ResourceMapping:
    """Resource mapping between platforms."""
    source_config: Dict[str, Any]
    target_config: Dict[str, Any]
    cost_comparison: Dict[str, float]
    notes: List[str] = field(default_factory=list)


@dataclass
class ConversionRecord:
    """Record of a platform conversion."""
    conversion_id: str
    timestamp: str
    source_platform: str
    target_platform: str
    source_config: Dict[str, Any]
    target_config: Dict[str, Any]
    estimated_cost_savings: float
    recommendations: List[str]
    warnings: List[str]


@dataclass
class PlatformConversionResult:
    """Result of platform conversion."""
    success: bool
    target_config: Dict[str, Any]
    resource_mapping: ResourceMapping
    spark_config: Dict[str, str]
    iam_config: Dict[str, str]
    storage_config: Dict[str, str]
    estimated_monthly_cost: float
    cost_comparison: Dict[str, float]
    recommendations: List[str]
    warnings: List[str]
    conversion_steps: List[str]
    error_message: str = ""


class PlatformConversionAgent:
    """
    Agent that converts ETL job configurations between platforms.

    Key Features:
    - Glue DPU to EMR instance mapping
    - Spark configuration translation
    - IAM role recommendations
    - Cost estimation and comparison
    - Local storage of conversion history for learning
    """

    # Glue DPU specifications
    GLUE_DPU_SPECS = {
        "standard": {"memory_gb": 16, "vcpus": 4, "cost_per_hour": 0.44},
        "flex": {"memory_gb": 16, "vcpus": 4, "cost_per_hour": 0.29},
        "g1x": {"memory_gb": 16, "vcpus": 4, "cost_per_hour": 0.44},
        "g2x": {"memory_gb": 32, "vcpus": 8, "cost_per_hour": 0.88}
    }

    # EMR instance specifications
    EMR_INSTANCE_SPECS = {
        "m5.xlarge": {"memory_gb": 16, "vcpus": 4, "cost_per_hour": 0.192},
        "m5.2xlarge": {"memory_gb": 32, "vcpus": 8, "cost_per_hour": 0.384},
        "m5.4xlarge": {"memory_gb": 64, "vcpus": 16, "cost_per_hour": 0.768},
        "r5.xlarge": {"memory_gb": 32, "vcpus": 4, "cost_per_hour": 0.252},
        "r5.2xlarge": {"memory_gb": 64, "vcpus": 8, "cost_per_hour": 0.504},
        "c5.xlarge": {"memory_gb": 8, "vcpus": 4, "cost_per_hour": 0.17},
        "c5.2xlarge": {"memory_gb": 16, "vcpus": 8, "cost_per_hour": 0.34}
    }

    # EKS node specifications (with Spark)
    EKS_NODE_SPECS = {
        "m5.xlarge": {"memory_gb": 16, "vcpus": 4, "cost_per_hour": 0.192},
        "m5.2xlarge": {"memory_gb": 32, "vcpus": 8, "cost_per_hour": 0.384},
        "r5.xlarge": {"memory_gb": 32, "vcpus": 4, "cost_per_hour": 0.252}
    }

    def __init__(self, config: Dict = None, storage_path: str = None):
        self.config = config or {}
        self.storage_path = storage_path or "data/agent_store/platform_conversions.json"
        self._ensure_storage()

    def _ensure_storage(self):
        """Ensure storage directory exists."""
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        if not os.path.exists(self.storage_path):
            with open(self.storage_path, 'w') as f:
                json.dump({"conversions": [], "statistics": {}}, f)

    def convert(
        self,
        source_config: Dict[str, Any],
        source_platform: Platform,
        target_platform: Platform,
        optimization_goal: str = "cost"  # "cost", "performance", "balanced"
    ) -> PlatformConversionResult:
        """
        Convert job configuration from source to target platform.

        Args:
            source_config: Original platform job configuration
            source_platform: Source platform type
            target_platform: Target platform type
            optimization_goal: What to optimize for

        Returns:
            PlatformConversionResult with complete conversion details
        """
        if source_platform == Platform.GLUE:
            if target_platform == Platform.EMR:
                return self._convert_glue_to_emr(source_config, optimization_goal)
            elif target_platform == Platform.EKS:
                return self._convert_glue_to_eks(source_config, optimization_goal)
        elif source_platform == Platform.EMR:
            if target_platform == Platform.GLUE:
                return self._convert_emr_to_glue(source_config, optimization_goal)
            elif target_platform == Platform.EKS:
                return self._convert_emr_to_eks(source_config, optimization_goal)

        return PlatformConversionResult(
            success=False,
            target_config={},
            resource_mapping=ResourceMapping({}, {}, {}),
            spark_config={},
            iam_config={},
            storage_config={},
            estimated_monthly_cost=0,
            cost_comparison={},
            recommendations=[],
            warnings=[],
            conversion_steps=[],
            error_message=f"Conversion from {source_platform.value} to {target_platform.value} not supported"
        )

    def _convert_glue_to_emr(
        self,
        glue_config: Dict[str, Any],
        optimization_goal: str
    ) -> PlatformConversionResult:
        """Convert Glue job configuration to EMR step configuration."""

        recommendations = []
        warnings = []
        conversion_steps = []

        # Extract Glue configuration
        glue_workers = glue_config.get("NumberOfWorkers", 10)
        glue_worker_type = glue_config.get("WorkerType", "G.1X")
        glue_version = glue_config.get("GlueVersion", "4.0")
        timeout_minutes = glue_config.get("Timeout", 480)
        max_retries = glue_config.get("MaxRetries", 0)
        script_location = glue_config.get("Command", {}).get("ScriptLocation", "")
        default_args = glue_config.get("DefaultArguments", {})

        conversion_steps.append(f"1. Analyzed Glue config: {glue_workers} x {glue_worker_type} workers")

        # Map Glue worker type to EMR instance
        dpu_type = "standard" if glue_worker_type in ["Standard", "G.1X"] else "g2x"
        dpu_specs = self.GLUE_DPU_SPECS.get(dpu_type, self.GLUE_DPU_SPECS["standard"])

        total_memory_gb = glue_workers * dpu_specs["memory_gb"]
        total_vcpus = glue_workers * dpu_specs["vcpus"]

        conversion_steps.append(f"2. Total resources needed: {total_memory_gb}GB RAM, {total_vcpus} vCPUs")

        # Select EMR instance type based on optimization goal
        if optimization_goal == "cost":
            # Use fewer larger instances for cost efficiency
            if total_memory_gb <= 64:
                instance_type = "m5.2xlarge"
                instance_count = max(2, (total_memory_gb // 32) + 1)
            else:
                instance_type = "m5.4xlarge"
                instance_count = max(2, (total_memory_gb // 64) + 1)
            recommendations.append("Using larger instances for cost efficiency")
        elif optimization_goal == "performance":
            # Use more smaller instances for parallelism
            instance_type = "m5.xlarge"
            instance_count = max(3, glue_workers)
            recommendations.append("Using more instances for better parallelism")
        else:  # balanced
            instance_type = "m5.2xlarge"
            instance_count = max(2, (glue_workers + 1) // 2)

        instance_specs = self.EMR_INSTANCE_SPECS[instance_type]
        conversion_steps.append(f"3. Selected EMR instance: {instance_count} x {instance_type}")

        # Calculate costs
        glue_hourly_cost = glue_workers * dpu_specs["cost_per_hour"]
        emr_hourly_cost = instance_count * instance_specs["cost_per_hour"]

        # EMR also has EMR service cost ($0.015/hr for m5.xlarge)
        emr_service_cost = instance_count * 0.015
        emr_total_hourly = emr_hourly_cost + emr_service_cost

        avg_job_hours = timeout_minutes / 60 / 2  # Assume job runs half the timeout
        monthly_runs = 30  # Assume daily runs

        glue_monthly = glue_hourly_cost * avg_job_hours * monthly_runs
        emr_monthly = emr_total_hourly * avg_job_hours * monthly_runs

        cost_savings = glue_monthly - emr_monthly
        cost_savings_pct = (cost_savings / glue_monthly * 100) if glue_monthly > 0 else 0

        conversion_steps.append(f"4. Cost analysis: Glue ${glue_monthly:.2f}/mo vs EMR ${emr_monthly:.2f}/mo")

        if cost_savings > 0:
            recommendations.append(f"Estimated savings: ${cost_savings:.2f}/month ({cost_savings_pct:.1f}%)")
        else:
            warnings.append(f"EMR may cost ${-cost_savings:.2f}/month more. Consider keeping Glue for small workloads.")

        # Build EMR cluster configuration
        emr_config = {
            "Name": f"emr-converted-{glue_config.get('Name', 'job')}",
            "ReleaseLabel": self._get_emr_release(glue_version),
            "Applications": [
                {"Name": "Spark"},
                {"Name": "Hadoop"}
            ],
            "Instances": {
                "MasterInstanceType": instance_type,
                "SlaveInstanceType": instance_type,
                "InstanceCount": instance_count,
                "KeepJobFlowAliveWhenNoSteps": False,
                "TerminationProtected": False
            },
            "Steps": [
                {
                    "Name": "Run ETL Job",
                    "ActionOnFailure": "TERMINATE_CLUSTER" if max_retries == 0 else "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": [
                            "spark-submit",
                            "--deploy-mode", "cluster",
                            "--master", "yarn",
                            script_location.replace("s3://", "s3a://") if script_location else "s3a://bucket/scripts/job.py"
                        ]
                    }
                }
            ],
            "JobFlowRole": "EMR_EC2_DefaultRole",
            "ServiceRole": "EMR_DefaultRole",
            "LogUri": default_args.get("--TempDir", "s3://bucket/logs/").replace("--TempDir", "").strip(),
            "VisibleToAllUsers": True,
            "Tags": [
                {"Key": "ConvertedFrom", "Value": "Glue"},
                {"Key": "OriginalJob", "Value": glue_config.get("Name", "unknown")}
            ]
        }

        conversion_steps.append("5. Generated EMR cluster configuration")

        # Build Spark configuration
        spark_config = {
            "spark.executor.instances": str(instance_count - 1),  # -1 for driver
            "spark.executor.memory": f"{int(instance_specs['memory_gb'] * 0.8)}g",
            "spark.executor.cores": str(instance_specs["vcpus"]),
            "spark.driver.memory": f"{int(instance_specs['memory_gb'] * 0.8)}g",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": "1",
            "spark.dynamicAllocation.maxExecutors": str(instance_count * 2),
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider"
        }

        # Translate Glue default arguments to Spark config
        if "--conf" in default_args:
            for conf in default_args.get("--conf", "").split(","):
                if "=" in conf:
                    key, value = conf.split("=", 1)
                    spark_config[key.strip()] = value.strip()

        conversion_steps.append("6. Generated Spark configuration")

        # IAM configuration
        iam_config = {
            "EMR_EC2_DefaultRole": "Required: EC2 instance profile for EMR nodes",
            "EMR_DefaultRole": "Required: EMR service role",
            "S3AccessPolicy": f"Required: S3 read/write access to {script_location} and data paths",
            "GlueCatalogAccess": "Optional: If using Glue Data Catalog as Hive metastore"
        }

        if "--enable-glue-datacatalog" in default_args:
            emr_config["Configurations"] = [
                {
                    "Classification": "spark-hive-site",
                    "Properties": {
                        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                    }
                }
            ]
            recommendations.append("Configured EMR to use Glue Data Catalog as metastore")

        # Storage configuration
        storage_config = {
            "script_location": script_location.replace("s3://", "s3a://") if script_location else "",
            "log_uri": emr_config.get("LogUri", ""),
            "temp_dir": default_args.get("--TempDir", "").replace("s3://", "s3a://"),
            "output_path": default_args.get("--output-path", "").replace("s3://", "s3a://")
        }

        # Additional recommendations
        if glue_workers < 5:
            warnings.append("Small workloads (< 5 DPUs) may be more cost-effective on Glue due to EMR cluster overhead")

        if "--enable-metrics" in default_args:
            recommendations.append("Enable CloudWatch metrics agent on EMR for monitoring")

        if "--job-bookmark-option" in default_args:
            warnings.append("Glue job bookmarks not available in EMR - implement custom checkpoint logic")

        recommendations.append("Consider EMR Serverless for similar serverless experience")
        recommendations.append("Use Spot instances for core nodes to reduce costs by 60-70%")

        # Store conversion record
        self._store_conversion(
            source_platform="glue",
            target_platform="emr",
            source_config=glue_config,
            target_config=emr_config,
            cost_savings=cost_savings,
            recommendations=recommendations,
            warnings=warnings
        )

        conversion_steps.append("7. Stored conversion record for learning")

        return PlatformConversionResult(
            success=True,
            target_config=emr_config,
            resource_mapping=ResourceMapping(
                source_config={
                    "workers": glue_workers,
                    "worker_type": glue_worker_type,
                    "total_memory_gb": total_memory_gb,
                    "total_vcpus": total_vcpus
                },
                target_config={
                    "instance_type": instance_type,
                    "instance_count": instance_count,
                    "total_memory_gb": instance_count * instance_specs["memory_gb"],
                    "total_vcpus": instance_count * instance_specs["vcpus"]
                },
                cost_comparison={
                    "glue_hourly": glue_hourly_cost,
                    "emr_hourly": emr_total_hourly,
                    "glue_monthly": glue_monthly,
                    "emr_monthly": emr_monthly,
                    "savings_monthly": cost_savings,
                    "savings_percent": cost_savings_pct
                }
            ),
            spark_config=spark_config,
            iam_config=iam_config,
            storage_config=storage_config,
            estimated_monthly_cost=emr_monthly,
            cost_comparison={
                "glue_monthly": glue_monthly,
                "emr_monthly": emr_monthly,
                "savings": cost_savings
            },
            recommendations=recommendations,
            warnings=warnings,
            conversion_steps=conversion_steps
        )

    def _convert_glue_to_eks(
        self,
        glue_config: Dict[str, Any],
        optimization_goal: str
    ) -> PlatformConversionResult:
        """Convert Glue job configuration to EKS Spark configuration."""

        recommendations = []
        warnings = []
        conversion_steps = []

        # Extract Glue configuration
        glue_workers = glue_config.get("NumberOfWorkers", 10)
        glue_worker_type = glue_config.get("WorkerType", "G.1X")
        script_location = glue_config.get("Command", {}).get("ScriptLocation", "")

        conversion_steps.append(f"1. Analyzed Glue config: {glue_workers} x {glue_worker_type}")

        # Map to EKS resources
        dpu_type = "standard" if glue_worker_type in ["Standard", "G.1X"] else "g2x"
        dpu_specs = self.GLUE_DPU_SPECS.get(dpu_type, self.GLUE_DPU_SPECS["standard"])

        total_memory_gb = glue_workers * dpu_specs["memory_gb"]

        # Select EKS node type
        if total_memory_gb <= 32:
            node_type = "m5.xlarge"
            node_count = max(2, glue_workers // 2)
        else:
            node_type = "m5.2xlarge"
            node_count = max(2, glue_workers // 3)

        node_specs = self.EKS_NODE_SPECS[node_type]

        conversion_steps.append(f"2. Selected EKS nodes: {node_count} x {node_type}")

        # Cost calculation
        glue_hourly = glue_workers * dpu_specs["cost_per_hour"]
        eks_hourly = node_count * node_specs["cost_per_hour"]
        eks_cluster_cost = 0.10  # EKS cluster hourly cost
        eks_total_hourly = eks_hourly + eks_cluster_cost

        avg_job_hours = 2
        monthly_runs = 30

        glue_monthly = glue_hourly * avg_job_hours * monthly_runs
        eks_monthly = eks_total_hourly * avg_job_hours * monthly_runs
        cost_savings = glue_monthly - eks_monthly

        # Build Kubernetes manifest
        eks_config = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": f"spark-{glue_config.get('Name', 'job').lower().replace('_', '-')}",
                "namespace": "spark-jobs"
            },
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": "spark:3.4.0-python3",
                "imagePullPolicy": "Always",
                "mainApplicationFile": script_location.replace("s3://", "s3a://") if script_location else "local:///opt/spark/job.py",
                "sparkVersion": "3.4.0",
                "restartPolicy": {
                    "type": "OnFailure",
                    "onFailureRetries": 3,
                    "onFailureRetryInterval": 10,
                    "onSubmissionFailureRetries": 5,
                    "onSubmissionFailureRetryInterval": 20
                },
                "driver": {
                    "cores": 2,
                    "coreLimit": "2000m",
                    "memory": "4g",
                    "serviceAccount": "spark-driver",
                    "labels": {
                        "version": "3.4.0"
                    }
                },
                "executor": {
                    "cores": node_specs["vcpus"],
                    "instances": node_count,
                    "memory": f"{int(node_specs['memory_gb'] * 0.8)}g",
                    "labels": {
                        "version": "3.4.0"
                    }
                },
                "sparkConf": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.kubernetes.authenticate.driver.serviceAccountName": "spark-driver",
                    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
                }
            }
        }

        conversion_steps.append("3. Generated SparkApplication Kubernetes manifest")

        # Spark config for EKS
        spark_config = {
            "spark.kubernetes.container.image": "spark:3.4.0-python3",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark-driver",
            "spark.executor.instances": str(node_count),
            "spark.executor.memory": f"{int(node_specs['memory_gb'] * 0.8)}g",
            "spark.executor.cores": str(node_specs["vcpus"]),
            "spark.kubernetes.executor.request.cores": str(node_specs["vcpus"]),
            "spark.kubernetes.executor.limit.cores": str(node_specs["vcpus"]),
            "spark.kubernetes.driver.pod.name": "spark-driver",
            "spark.kubernetes.node.selector.nodegroup": "spark-workers"
        }

        # IAM config for IRSA
        iam_config = {
            "ServiceAccount": "spark-driver (with IRSA annotation)",
            "IAMRole": "arn:aws:iam::ACCOUNT:role/SparkDriverRole",
            "PolicyRequired": "S3 read/write, Glue catalog access (if needed)",
            "IRSA": "Required: aws-iam-authenticator or EKS Pod Identity"
        }

        storage_config = {
            "script_location": script_location.replace("s3://", "s3a://") if script_location else "",
            "spark_image": "spark:3.4.0-python3",
            "namespace": "spark-jobs"
        }

        recommendations.append("Use Karpenter for efficient node scaling")
        recommendations.append("Use Spot instances for executor nodes (60-70% savings)")
        recommendations.append("Consider AWS Mountpoint for S3 for better S3 performance")
        warnings.append("Glue job bookmarks not available - implement custom checkpointing")
        warnings.append("Ensure EKS cluster has Spark Operator installed")

        # Store conversion
        self._store_conversion(
            source_platform="glue",
            target_platform="eks",
            source_config=glue_config,
            target_config=eks_config,
            cost_savings=cost_savings,
            recommendations=recommendations,
            warnings=warnings
        )

        conversion_steps.append("4. Stored conversion record for learning")

        return PlatformConversionResult(
            success=True,
            target_config=eks_config,
            resource_mapping=ResourceMapping(
                source_config={"workers": glue_workers, "worker_type": glue_worker_type},
                target_config={"node_type": node_type, "node_count": node_count},
                cost_comparison={
                    "glue_monthly": glue_monthly,
                    "eks_monthly": eks_monthly,
                    "savings": cost_savings
                }
            ),
            spark_config=spark_config,
            iam_config=iam_config,
            storage_config=storage_config,
            estimated_monthly_cost=eks_monthly,
            cost_comparison={"glue_monthly": glue_monthly, "eks_monthly": eks_monthly, "savings": cost_savings},
            recommendations=recommendations,
            warnings=warnings,
            conversion_steps=conversion_steps
        )

    def _convert_emr_to_glue(
        self,
        emr_config: Dict[str, Any],
        optimization_goal: str
    ) -> PlatformConversionResult:
        """Convert EMR configuration to Glue job configuration."""

        recommendations = []
        warnings = []
        conversion_steps = []

        # Extract EMR configuration
        instances = emr_config.get("Instances", {})
        instance_type = instances.get("SlaveInstanceType", "m5.xlarge")
        instance_count = instances.get("InstanceCount", 3)

        conversion_steps.append(f"1. Analyzed EMR config: {instance_count} x {instance_type}")

        # Get instance specs
        instance_specs = self.EMR_INSTANCE_SPECS.get(instance_type, self.EMR_INSTANCE_SPECS["m5.xlarge"])
        total_memory = instance_count * instance_specs["memory_gb"]

        # Map to Glue workers
        # G.1X = 16GB, G.2X = 32GB
        if total_memory <= 80:
            worker_type = "G.1X"
            num_workers = max(2, (total_memory + 15) // 16)
        else:
            worker_type = "G.2X"
            num_workers = max(2, (total_memory + 31) // 32)

        conversion_steps.append(f"2. Mapped to Glue: {num_workers} x {worker_type}")

        # Cost calculation
        emr_hourly = instance_count * instance_specs["cost_per_hour"]
        emr_service = instance_count * 0.015
        emr_total = emr_hourly + emr_service

        dpu_cost = 0.44 if worker_type == "G.1X" else 0.88
        glue_hourly = num_workers * dpu_cost

        avg_hours = 2
        monthly_runs = 30

        emr_monthly = emr_total * avg_hours * monthly_runs
        glue_monthly = glue_hourly * avg_hours * monthly_runs
        cost_diff = glue_monthly - emr_monthly

        # Extract script from EMR steps
        steps = emr_config.get("Steps", [])
        script_location = ""
        for step in steps:
            args = step.get("HadoopJarStep", {}).get("Args", [])
            for i, arg in enumerate(args):
                if arg.endswith(".py"):
                    script_location = arg
                    break

        # Build Glue configuration
        glue_config = {
            "Name": emr_config.get("Name", "converted-job").replace("emr-", "glue-"),
            "Role": "arn:aws:iam::ACCOUNT:role/GlueServiceRole",
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": script_location.replace("s3a://", "s3://") if script_location else "s3://bucket/scripts/job.py",
                "PythonVersion": "3"
            },
            "DefaultArguments": {
                "--job-language": "python",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-glue-datacatalog": "true",
                "--TempDir": emr_config.get("LogUri", "s3://bucket/temp/").replace("s3a://", "s3://")
            },
            "GlueVersion": "4.0",
            "WorkerType": worker_type,
            "NumberOfWorkers": num_workers,
            "Timeout": 480,
            "MaxRetries": 1
        }

        conversion_steps.append("3. Generated Glue job configuration")

        # Spark config (embedded in Glue)
        spark_config = {
            "--conf spark.sql.adaptive.enabled": "true",
            "--conf spark.sql.adaptive.coalescePartitions.enabled": "true"
        }

        # IAM config
        iam_config = {
            "GlueServiceRole": "Required: Glue service role with S3 and catalog access",
            "S3Policy": "S3 read/write for script, data, and temp locations"
        }

        storage_config = {
            "script_location": glue_config["Command"]["ScriptLocation"],
            "temp_dir": glue_config["DefaultArguments"]["--TempDir"]
        }

        if cost_diff > 0:
            warnings.append(f"Glue may cost ${cost_diff:.2f}/month more than EMR")
            recommendations.append("Glue is best for serverless, short-running jobs")
        else:
            recommendations.append(f"Potential savings: ${-cost_diff:.2f}/month with Glue")

        recommendations.append("Glue provides automatic scaling and no cluster management")
        recommendations.append("Job bookmarks available for incremental processing")
        warnings.append("Custom JARs and libraries need special handling in Glue")

        # Store conversion
        self._store_conversion(
            source_platform="emr",
            target_platform="glue",
            source_config=emr_config,
            target_config=glue_config,
            cost_savings=-cost_diff,
            recommendations=recommendations,
            warnings=warnings
        )

        return PlatformConversionResult(
            success=True,
            target_config=glue_config,
            resource_mapping=ResourceMapping(
                source_config={"instance_type": instance_type, "instance_count": instance_count},
                target_config={"worker_type": worker_type, "num_workers": num_workers},
                cost_comparison={"emr_monthly": emr_monthly, "glue_monthly": glue_monthly}
            ),
            spark_config=spark_config,
            iam_config=iam_config,
            storage_config=storage_config,
            estimated_monthly_cost=glue_monthly,
            cost_comparison={"emr_monthly": emr_monthly, "glue_monthly": glue_monthly, "difference": cost_diff},
            recommendations=recommendations,
            warnings=warnings,
            conversion_steps=conversion_steps
        )

    def _convert_emr_to_eks(
        self,
        emr_config: Dict[str, Any],
        optimization_goal: str
    ) -> PlatformConversionResult:
        """Convert EMR configuration to EKS Spark configuration."""

        recommendations = []
        warnings = []
        conversion_steps = []

        instances = emr_config.get("Instances", {})
        instance_type = instances.get("SlaveInstanceType", "m5.xlarge")
        instance_count = instances.get("InstanceCount", 3)

        conversion_steps.append(f"1. Analyzed EMR config: {instance_count} x {instance_type}")

        # EKS uses same instance types, so mapping is direct
        node_type = instance_type if instance_type in self.EKS_NODE_SPECS else "m5.xlarge"
        node_count = instance_count

        node_specs = self.EKS_NODE_SPECS.get(node_type, self.EKS_NODE_SPECS["m5.xlarge"])
        instance_specs = self.EMR_INSTANCE_SPECS.get(instance_type, self.EMR_INSTANCE_SPECS["m5.xlarge"])

        # Cost calculation
        emr_hourly = instance_count * instance_specs["cost_per_hour"]
        emr_service = instance_count * 0.015
        emr_total = emr_hourly + emr_service

        eks_hourly = node_count * node_specs["cost_per_hour"]
        eks_cluster = 0.10
        eks_total = eks_hourly + eks_cluster

        avg_hours = 2
        monthly_runs = 30

        emr_monthly = emr_total * avg_hours * monthly_runs
        eks_monthly = eks_total * avg_hours * monthly_runs
        cost_savings = emr_monthly - eks_monthly

        # Extract script from steps
        steps = emr_config.get("Steps", [])
        script_location = ""
        for step in steps:
            args = step.get("HadoopJarStep", {}).get("Args", [])
            for arg in args:
                if arg.endswith(".py"):
                    script_location = arg
                    break

        # Build EKS SparkApplication
        eks_config = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": emr_config.get("Name", "job").lower().replace("_", "-"),
                "namespace": "spark-jobs"
            },
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": "spark:3.4.0-python3",
                "mainApplicationFile": script_location or "local:///opt/spark/job.py",
                "sparkVersion": "3.4.0",
                "driver": {
                    "cores": 2,
                    "memory": "4g",
                    "serviceAccount": "spark-driver"
                },
                "executor": {
                    "cores": node_specs["vcpus"],
                    "instances": node_count,
                    "memory": f"{int(node_specs['memory_gb'] * 0.8)}g"
                }
            }
        }

        conversion_steps.append(f"2. Generated EKS SparkApplication manifest")

        spark_config = {
            "spark.kubernetes.container.image": "spark:3.4.0-python3",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.executor.instances": str(node_count),
            "spark.executor.memory": f"{int(node_specs['memory_gb'] * 0.8)}g"
        }

        iam_config = {
            "ServiceAccount": "spark-driver with IRSA",
            "IAMRole": "SparkDriverRole with S3 access"
        }

        storage_config = {
            "script_location": script_location,
            "namespace": "spark-jobs"
        }

        recommendations.append("Use Karpenter for dynamic node provisioning")
        recommendations.append("Consider Spot instances for executors")
        warnings.append("Ensure Spark Operator is installed on EKS cluster")

        self._store_conversion(
            source_platform="emr",
            target_platform="eks",
            source_config=emr_config,
            target_config=eks_config,
            cost_savings=cost_savings,
            recommendations=recommendations,
            warnings=warnings
        )

        return PlatformConversionResult(
            success=True,
            target_config=eks_config,
            resource_mapping=ResourceMapping(
                source_config={"instance_type": instance_type, "instance_count": instance_count},
                target_config={"node_type": node_type, "node_count": node_count},
                cost_comparison={"emr_monthly": emr_monthly, "eks_monthly": eks_monthly}
            ),
            spark_config=spark_config,
            iam_config=iam_config,
            storage_config=storage_config,
            estimated_monthly_cost=eks_monthly,
            cost_comparison={"emr_monthly": emr_monthly, "eks_monthly": eks_monthly, "savings": cost_savings},
            recommendations=recommendations,
            warnings=warnings,
            conversion_steps=conversion_steps
        )

    def _get_emr_release(self, glue_version: str) -> str:
        """Map Glue version to EMR release."""
        mapping = {
            "4.0": "emr-6.15.0",
            "3.0": "emr-6.10.0",
            "2.0": "emr-6.5.0"
        }
        return mapping.get(glue_version, "emr-6.15.0")

    def _store_conversion(
        self,
        source_platform: str,
        target_platform: str,
        source_config: Dict,
        target_config: Dict,
        cost_savings: float,
        recommendations: List[str],
        warnings: List[str]
    ):
        """Store conversion record for learning."""
        try:
            with open(self.storage_path, 'r') as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            data = {"conversions": [], "statistics": {}}

        record = {
            "conversion_id": f"conv_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "timestamp": datetime.now().isoformat(),
            "source_platform": source_platform,
            "target_platform": target_platform,
            "source_config": source_config,
            "target_config": target_config,
            "estimated_cost_savings": cost_savings,
            "recommendations": recommendations,
            "warnings": warnings
        }

        data["conversions"].append(record)

        # Update statistics
        key = f"{source_platform}_to_{target_platform}"
        if key not in data["statistics"]:
            data["statistics"][key] = {"count": 0, "total_savings": 0}
        data["statistics"][key]["count"] += 1
        data["statistics"][key]["total_savings"] += cost_savings

        with open(self.storage_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)

    def get_conversion_history(self, limit: int = 10) -> List[Dict]:
        """Get recent conversion history."""
        try:
            with open(self.storage_path, 'r') as f:
                data = json.load(f)
            return data.get("conversions", [])[-limit:]
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    def get_statistics(self) -> Dict:
        """Get conversion statistics."""
        try:
            with open(self.storage_path, 'r') as f:
                data = json.load(f)
            return data.get("statistics", {})
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def generate_conversion_report(self, result: PlatformConversionResult) -> str:
        """Generate a human-readable conversion report."""
        report = []
        report.append("=" * 60)
        report.append("PLATFORM CONVERSION REPORT")
        report.append("=" * 60)

        report.append(f"\nStatus: {'SUCCESS' if result.success else 'FAILED'}")

        if result.error_message:
            report.append(f"Error: {result.error_message}")
            return "\n".join(report)

        report.append("\n--- Conversion Steps ---")
        for step in result.conversion_steps:
            report.append(f"  {step}")

        report.append("\n--- Resource Mapping ---")
        report.append(f"  Source: {json.dumps(result.resource_mapping.source_config, indent=4)}")
        report.append(f"  Target: {json.dumps(result.resource_mapping.target_config, indent=4)}")

        report.append("\n--- Cost Comparison ---")
        for key, value in result.cost_comparison.items():
            report.append(f"  {key}: ${value:.2f}")

        report.append("\n--- Spark Configuration ---")
        for key, value in list(result.spark_config.items())[:10]:
            report.append(f"  {key}={value}")

        report.append("\n--- IAM Requirements ---")
        for key, value in result.iam_config.items():
            report.append(f"  {key}: {value}")

        if result.recommendations:
            report.append("\n--- Recommendations ---")
            for rec in result.recommendations:
                report.append(f"  * {rec}")

        if result.warnings:
            report.append("\n--- Warnings ---")
            for warn in result.warnings:
                report.append(f"  ! {warn}")

        report.append("\n" + "=" * 60)

        return "\n".join(report)
