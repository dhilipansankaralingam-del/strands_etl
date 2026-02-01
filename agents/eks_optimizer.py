"""
EKS Optimization Module - Karpenter, SPOT, and Graviton configurations for ETL
Generates optimal Kubernetes manifests for Spark workloads on EKS
"""

import json
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InstanceCategory(Enum):
    """EC2 instance categories."""
    MEMORY_OPTIMIZED = "r"      # r6g, r5, r6i - for memory-heavy ETL
    COMPUTE_OPTIMIZED = "c"     # c6g, c5, c6i - for CPU-heavy transforms
    GENERAL_PURPOSE = "m"       # m6g, m5, m6i - balanced workloads
    STORAGE_OPTIMIZED = "i"     # i3, i4i - for shuffle-heavy jobs


class ProcessorType(Enum):
    """Processor architecture."""
    GRAVITON = "arm64"          # AWS Graviton (40% better price-perf)
    INTEL = "amd64"             # Intel x86
    AMD = "amd64"               # AMD x86


class CapacityType(Enum):
    """Instance capacity type."""
    SPOT = "spot"               # 70-90% savings, can be interrupted
    ON_DEMAND = "on-demand"     # Full price, guaranteed


@dataclass
class SpotConfig:
    """SPOT instance configuration."""
    enabled: bool = True
    max_spot_percentage: int = 80       # Max % of executors on SPOT
    fallback_to_on_demand: bool = True
    interruption_behavior: str = "terminate"
    diversification_strategy: str = "capacity-optimized-prioritized"

    # Instance type diversification for SPOT availability
    instance_pools: List[str] = field(default_factory=lambda: [
        "r6g.2xlarge", "r6g.4xlarge",   # Graviton primary
        "r5.2xlarge", "r5.4xlarge",     # Intel fallback
        "m6g.4xlarge", "m5.4xlarge"     # General purpose fallback
    ])


@dataclass
class GravitonConfig:
    """Graviton processor configuration."""
    enabled: bool = True
    prefer_graviton: bool = True        # Prefer Graviton over x86
    fallback_to_x86: bool = True        # Allow x86 if Graviton unavailable

    # Graviton instance families by category
    memory_optimized: List[str] = field(default_factory=lambda: ["r6g", "r7g"])
    compute_optimized: List[str] = field(default_factory=lambda: ["c6g", "c7g"])
    general_purpose: List[str] = field(default_factory=lambda: ["m6g", "m7g"])


@dataclass
class KarpenterConfig:
    """Karpenter node provisioner configuration."""
    enabled: bool = True
    consolidation_enabled: bool = True
    consolidate_after_seconds: int = 30

    # Resource limits
    max_cpu: int = 1000                 # Max vCPUs across all nodes
    max_memory_gb: int = 4000           # Max memory across all nodes

    # Node constraints
    instance_sizes: List[str] = field(default_factory=lambda: [
        "xlarge", "2xlarge", "4xlarge", "8xlarge"
    ])

    # Taints and tolerations
    spark_node_taint: str = "spark-workload=true:NoSchedule"


@dataclass
class EKSSparkConfig:
    """Complete EKS Spark configuration."""
    # Driver configuration (stable, on-demand)
    driver_cores: int = 2
    driver_memory_gb: int = 4
    driver_on_demand: bool = True       # Driver always on-demand for stability

    # Executor configuration (scalable, SPOT-friendly)
    executor_cores: int = 4
    executor_memory_gb: int = 8
    executor_memory_overhead_gb: int = 2
    min_executors: int = 2
    max_executors: int = 100
    initial_executors: int = 5

    # Dynamic allocation
    dynamic_allocation: bool = True
    scale_up_timeout_seconds: int = 10
    scale_down_timeout_seconds: int = 120

    # Decommissioning for SPOT interruptions
    decommission_enabled: bool = True
    shuffle_tracking_enabled: bool = True


class EKSOptimizer:
    """
    Generates optimized EKS configurations for Spark ETL workloads.

    Features:
    - Karpenter NodePool generation
    - SPOT instance strategies
    - Graviton optimization
    - Dynamic pod scaling
    - Cost estimation
    """

    # Cost per hour by instance type (us-east-1 prices)
    INSTANCE_COSTS = {
        # Graviton SPOT (best value)
        "r6g.xlarge_spot": 0.045,
        "r6g.2xlarge_spot": 0.090,
        "r6g.4xlarge_spot": 0.180,
        "r6g.8xlarge_spot": 0.360,

        # Graviton On-Demand
        "r6g.xlarge_ondemand": 0.151,
        "r6g.2xlarge_ondemand": 0.302,
        "r6g.4xlarge_ondemand": 0.605,

        # Intel SPOT
        "r5.xlarge_spot": 0.060,
        "r5.2xlarge_spot": 0.120,
        "r5.4xlarge_spot": 0.240,

        # Intel On-Demand
        "r5.xlarge_ondemand": 0.192,
        "r5.2xlarge_ondemand": 0.384,
        "r5.4xlarge_ondemand": 0.768,
    }

    def __init__(self):
        """Initialize EKS optimizer."""
        self.spot_config = SpotConfig()
        self.graviton_config = GravitonConfig()
        self.karpenter_config = KarpenterConfig()

    def generate_karpenter_nodepool(self,
                                     workload_name: str,
                                     workload_category: str = "heavy") -> Dict[str, Any]:
        """
        Generate Karpenter NodePool manifest for ETL workload.

        Args:
            workload_name: Name of the ETL workload
            workload_category: lightweight, moderate, heavy, massive

        Returns:
            Kubernetes manifest dict
        """
        # Determine instance requirements based on workload
        if workload_category == "lightweight":
            instance_sizes = ["large", "xlarge"]
            max_cpu = 100
            max_memory = 400
        elif workload_category == "moderate":
            instance_sizes = ["xlarge", "2xlarge"]
            max_cpu = 500
            max_memory = 2000
        elif workload_category == "heavy":
            instance_sizes = ["2xlarge", "4xlarge", "8xlarge"]
            max_cpu = 1000
            max_memory = 4000
        else:  # massive
            instance_sizes = ["4xlarge", "8xlarge", "12xlarge", "16xlarge"]
            max_cpu = 2000
            max_memory = 8000

        nodepool = {
            "apiVersion": "karpenter.sh/v1beta1",
            "kind": "NodePool",
            "metadata": {
                "name": f"spark-{workload_name}",
                "labels": {
                    "workload": workload_name,
                    "type": "spark-executor"
                }
            },
            "spec": {
                "template": {
                    "metadata": {
                        "labels": {
                            "workload": workload_name,
                            "node-type": "spark-worker"
                        }
                    },
                    "spec": {
                        "requirements": [
                            {
                                "key": "karpenter.k8s.aws/instance-category",
                                "operator": "In",
                                "values": ["r", "m"]  # Memory and General purpose
                            },
                            {
                                "key": "karpenter.k8s.aws/instance-size",
                                "operator": "In",
                                "values": instance_sizes
                            },
                            {
                                "key": "kubernetes.io/arch",
                                "operator": "In",
                                "values": ["arm64", "amd64"] if self.graviton_config.fallback_to_x86 else ["arm64"]
                            },
                            {
                                "key": "karpenter.sh/capacity-type",
                                "operator": "In",
                                "values": ["spot", "on-demand"] if self.spot_config.fallback_to_on_demand else ["spot"]
                            },
                            {
                                "key": "topology.kubernetes.io/zone",
                                "operator": "In",
                                "values": ["us-east-1a", "us-east-1b", "us-east-1c"]
                            }
                        ],
                        "nodeClassRef": {
                            "name": f"spark-{workload_name}-nodeclass"
                        },
                        "taints": [
                            {
                                "key": "spark-workload",
                                "value": workload_name,
                                "effect": "NoSchedule"
                            }
                        ]
                    }
                },
                "disruption": {
                    "consolidationPolicy": "WhenUnderutilized",
                    "consolidateAfter": f"{self.karpenter_config.consolidate_after_seconds}s"
                },
                "limits": {
                    "cpu": max_cpu,
                    "memory": f"{max_memory}Gi"
                },
                "weight": 100
            }
        }

        return nodepool

    def generate_ec2_nodeclass(self, workload_name: str) -> Dict[str, Any]:
        """Generate EC2NodeClass for Karpenter."""
        return {
            "apiVersion": "karpenter.k8s.aws/v1beta1",
            "kind": "EC2NodeClass",
            "metadata": {
                "name": f"spark-{workload_name}-nodeclass"
            },
            "spec": {
                "amiFamily": "AL2",
                "subnetSelectorTerms": [
                    {"tags": {"karpenter.sh/discovery": "true"}}
                ],
                "securityGroupSelectorTerms": [
                    {"tags": {"karpenter.sh/discovery": "true"}}
                ],
                "instanceStorePolicy": "RAID0",  # Use instance storage for shuffle
                "blockDeviceMappings": [
                    {
                        "deviceName": "/dev/xvda",
                        "ebs": {
                            "volumeSize": "100Gi",
                            "volumeType": "gp3",
                            "iops": 3000,
                            "throughput": 125,
                            "deleteOnTermination": True
                        }
                    }
                ],
                "userData": self._generate_user_data(),
                "tags": {
                    "Environment": "production",
                    "Workload": workload_name,
                    "ManagedBy": "karpenter"
                }
            }
        }

    def _generate_user_data(self) -> str:
        """Generate node user data for Spark optimization."""
        return """
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="BOUNDARY"

--BOUNDARY
Content-Type: text/x-shellscript; charset="us-ascii"

#!/bin/bash
# Optimize for Spark workloads

# Increase file descriptors
echo "* soft nofile 65536" >> /etc/security/limits.conf
echo "* hard nofile 65536" >> /etc/security/limits.conf

# Disable transparent huge pages (improves Spark performance)
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag

# Optimize network settings for shuffle
sysctl -w net.core.somaxconn=65535
sysctl -w net.ipv4.tcp_max_syn_backlog=65535
sysctl -w net.core.netdev_max_backlog=65535

# Mount instance storage for shuffle (if available)
if [ -b /dev/nvme1n1 ]; then
    mkfs.ext4 /dev/nvme1n1
    mkdir -p /mnt/spark-local
    mount /dev/nvme1n1 /mnt/spark-local
    chmod 777 /mnt/spark-local
fi

--BOUNDARY--
"""

    def generate_spark_application(self,
                                    job_name: str,
                                    script_path: str,
                                    config: EKSSparkConfig = None) -> Dict[str, Any]:
        """
        Generate SparkApplication manifest for Spark Operator.

        Args:
            job_name: Name of the Spark job
            script_path: S3 path to the PySpark script
            config: EKSSparkConfig with resource settings

        Returns:
            SparkApplication manifest dict
        """
        config = config or EKSSparkConfig()

        spark_conf = {
            # Dynamic allocation
            "spark.dynamicAllocation.enabled": str(config.dynamic_allocation).lower(),
            "spark.dynamicAllocation.minExecutors": str(config.min_executors),
            "spark.dynamicAllocation.maxExecutors": str(config.max_executors),
            "spark.dynamicAllocation.initialExecutors": str(config.initial_executors),
            "spark.dynamicAllocation.shuffleTracking.enabled": str(config.shuffle_tracking_enabled).lower(),
            "spark.dynamicAllocation.schedulerBacklogTimeout": f"{config.scale_up_timeout_seconds}s",
            "spark.dynamicAllocation.executorIdleTimeout": f"{config.scale_down_timeout_seconds}s",

            # Decommissioning for SPOT
            "spark.decommission.enabled": str(config.decommission_enabled).lower(),
            "spark.storage.decommission.enabled": "true",
            "spark.storage.decommission.shuffleBlocks.enabled": "true",
            "spark.storage.decommission.rddBlocks.enabled": "true",
            "spark.storage.decommission.fallbackStorage.path": "s3a://your-bucket/spark-recovery/",

            # Performance tuning
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.shuffle.partitions": "auto",

            # S3 optimization
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.multipart.size": "128M",

            # Local storage for shuffle
            "spark.local.dir": "/mnt/spark-local"
        }

        return {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": job_name,
                "namespace": "spark-jobs"
            },
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": "public.ecr.aws/emr-on-eks/spark:3.5.0-java17-arm64",
                "imagePullPolicy": "Always",
                "mainApplicationFile": script_path,
                "sparkVersion": "3.5.0",
                "sparkConf": spark_conf,

                "driver": {
                    "cores": config.driver_cores,
                    "memory": f"{config.driver_memory_gb}g",
                    "serviceAccount": "spark-driver",
                    "nodeSelector": {
                        # Driver on On-Demand for stability
                        "karpenter.sh/capacity-type": "on-demand" if config.driver_on_demand else "spot"
                    },
                    "tolerations": [
                        {
                            "key": "spark-workload",
                            "operator": "Exists",
                            "effect": "NoSchedule"
                        }
                    ],
                    "labels": {
                        "spark-role": "driver"
                    }
                },

                "executor": {
                    "cores": config.executor_cores,
                    "memory": f"{config.executor_memory_gb}g",
                    "memoryOverhead": f"{config.executor_memory_overhead_gb}g",
                    "instances": config.initial_executors,
                    "nodeSelector": {
                        # Executors on SPOT for cost savings
                        "karpenter.sh/capacity-type": "spot",
                        # Prefer Graviton
                        "kubernetes.io/arch": "arm64"
                    },
                    "tolerations": [
                        {
                            "key": "spark-workload",
                            "operator": "Exists",
                            "effect": "NoSchedule"
                        },
                        {
                            "key": "spot",
                            "operator": "Equal",
                            "value": "true",
                            "effect": "NoSchedule"
                        }
                    ],
                    "labels": {
                        "spark-role": "executor"
                    },
                    "volumeMounts": [
                        {
                            "name": "spark-local",
                            "mountPath": "/mnt/spark-local"
                        }
                    ]
                },

                "volumes": [
                    {
                        "name": "spark-local",
                        "hostPath": {
                            "path": "/mnt/spark-local",
                            "type": "DirectoryOrCreate"
                        }
                    }
                ],

                "restartPolicy": {
                    "type": "OnFailure",
                    "onFailureRetries": 3,
                    "onFailureRetryInterval": 10,
                    "onSubmissionFailureRetries": 3
                }
            }
        }

    def estimate_cost(self,
                      data_size_gb: float,
                      complexity: str = "medium",
                      use_graviton: bool = True,
                      spot_percentage: int = 80) -> Dict[str, Any]:
        """
        Estimate cost for running ETL on EKS with SPOT/Graviton.

        Args:
            data_size_gb: Input data size in GB
            complexity: low, medium, high
            use_graviton: Whether to use Graviton instances
            spot_percentage: Percentage of executors on SPOT

        Returns:
            Cost estimation dict
        """
        # Estimate resources needed
        if complexity == "low":
            executors_needed = max(2, int(data_size_gb / 10))
            hours_needed = max(0.1, data_size_gb / 50)
        elif complexity == "medium":
            executors_needed = max(5, int(data_size_gb / 5))
            hours_needed = max(0.25, data_size_gb / 30)
        else:  # high
            executors_needed = max(10, int(data_size_gb / 2))
            hours_needed = max(0.5, data_size_gb / 20)

        # Calculate SPOT vs On-Demand split
        spot_executors = int(executors_needed * spot_percentage / 100)
        on_demand_executors = executors_needed - spot_executors

        # Instance type selection
        if use_graviton:
            spot_instance = "r6g.2xlarge_spot"
            on_demand_instance = "r6g.2xlarge_ondemand"
            driver_instance = "r6g.xlarge_ondemand"
        else:
            spot_instance = "r5.2xlarge_spot"
            on_demand_instance = "r5.2xlarge_ondemand"
            driver_instance = "r5.xlarge_ondemand"

        # Calculate costs
        spot_cost = spot_executors * self.INSTANCE_COSTS[spot_instance] * hours_needed
        on_demand_cost = on_demand_executors * self.INSTANCE_COSTS[on_demand_instance] * hours_needed
        driver_cost = self.INSTANCE_COSTS[driver_instance] * hours_needed

        total_cost = spot_cost + on_demand_cost + driver_cost

        # Compare with Glue
        glue_dpu_cost = 0.44  # per DPU-hour
        glue_dpus = executors_needed * 2  # Approximate DPU equivalent
        glue_cost = glue_dpus * glue_dpu_cost * hours_needed

        savings = glue_cost - total_cost
        savings_percentage = (savings / glue_cost * 100) if glue_cost > 0 else 0

        return {
            "eks_estimate": {
                "total_cost": round(total_cost, 2),
                "breakdown": {
                    "driver_cost": round(driver_cost, 2),
                    "spot_executor_cost": round(spot_cost, 2),
                    "on_demand_executor_cost": round(on_demand_cost, 2)
                },
                "resources": {
                    "executors": executors_needed,
                    "spot_executors": spot_executors,
                    "on_demand_executors": on_demand_executors,
                    "estimated_hours": round(hours_needed, 2)
                },
                "instance_types": {
                    "driver": driver_instance.replace("_ondemand", ""),
                    "executors": spot_instance.replace("_spot", "")
                }
            },
            "glue_estimate": {
                "total_cost": round(glue_cost, 2),
                "dpus": glue_dpus,
                "dpu_hours": round(glue_dpus * hours_needed, 2)
            },
            "savings": {
                "amount": round(savings, 2),
                "percentage": round(savings_percentage, 1)
            },
            "recommendations": self._generate_cost_recommendations(
                data_size_gb, complexity, use_graviton, spot_percentage
            )
        }

    def _generate_cost_recommendations(self,
                                        data_size_gb: float,
                                        complexity: str,
                                        use_graviton: bool,
                                        spot_percentage: int) -> List[str]:
        """Generate cost optimization recommendations."""
        recommendations = []

        if not use_graviton:
            recommendations.append(
                "Enable Graviton instances for 40% better price-performance"
            )

        if spot_percentage < 70:
            recommendations.append(
                f"Increase SPOT percentage from {spot_percentage}% to 80%+ for additional savings"
            )

        if data_size_gb > 100 and complexity != "high":
            recommendations.append(
                "Consider using instance store (NVMe) for shuffle-heavy operations"
            )

        if data_size_gb < 10:
            recommendations.append(
                "For small datasets, consider AWS Lambda or Glue Flex instead of EKS"
            )

        if complexity == "high":
            recommendations.append(
                "Enable Spark AQE (Adaptive Query Execution) for automatic optimization"
            )

        recommendations.append(
            "Use Karpenter consolidation to automatically remove underutilized nodes"
        )

        return recommendations

    def generate_complete_setup(self,
                                 workload_name: str,
                                 script_path: str,
                                 workload_category: str = "heavy") -> Dict[str, Any]:
        """
        Generate complete EKS setup for an ETL workload.

        Returns all manifests needed for deployment.
        """
        return {
            "nodepool": self.generate_karpenter_nodepool(workload_name, workload_category),
            "nodeclass": self.generate_ec2_nodeclass(workload_name),
            "spark_application": self.generate_spark_application(
                workload_name,
                script_path,
                EKSSparkConfig()
            ),
            "instructions": f"""
# Deploy EKS Resources for {workload_name}

## 1. Apply Karpenter NodePool
kubectl apply -f nodepool.yaml

## 2. Apply EC2NodeClass
kubectl apply -f nodeclass.yaml

## 3. Submit Spark Job
kubectl apply -f spark-application.yaml

## 4. Monitor Job
kubectl get sparkapplication {workload_name} -n spark-jobs
kubectl logs -f {workload_name}-driver -n spark-jobs

## 5. View Scaling
kubectl get nodes -l workload={workload_name}
kubectl get pods -n spark-jobs -l spark-role=executor
"""
        }


# Example usage and demo
if __name__ == '__main__':
    optimizer = EKSOptimizer()

    # Generate complete setup
    setup = optimizer.generate_complete_setup(
        workload_name="customer-360-etl",
        script_path="s3://my-bucket/scripts/customer_360_etl.py",
        workload_category="heavy"
    )

    print("=" * 60)
    print("EKS Optimization for ETL Workloads")
    print("=" * 60)

    # Show NodePool
    print("\n--- Karpenter NodePool ---")
    print(json.dumps(setup["nodepool"], indent=2))

    # Cost estimation
    print("\n--- Cost Estimation ---")
    cost = optimizer.estimate_cost(
        data_size_gb=100,
        complexity="high",
        use_graviton=True,
        spot_percentage=80
    )

    print(f"EKS Cost: ${cost['eks_estimate']['total_cost']}")
    print(f"Glue Cost: ${cost['glue_estimate']['total_cost']}")
    print(f"Savings: ${cost['savings']['amount']} ({cost['savings']['percentage']}%)")

    print("\nRecommendations:")
    for rec in cost["recommendations"]:
        print(f"  - {rec}")

    print("\n" + setup["instructions"])
