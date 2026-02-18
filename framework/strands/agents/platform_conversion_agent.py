#!/usr/bin/env python3
"""Strands Platform Conversion Agent - Converts between Glue/EMR/EKS."""

from typing import Dict, List, Any
from ..base_agent import StrandsAgent, AgentResult, AgentStatus, AgentContext, register_agent
from ..storage import StrandsStorage

# Unified audit logger (optional)
try:
    from ..unified_audit import get_audit_logger
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False
    get_audit_logger = None


@register_agent
class StrandsPlatformConversionAgent(StrandsAgent):
    """Converts job configurations between platforms (Glue, EMR, EKS)."""

    AGENT_NAME = "platform_conversion_agent"
    AGENT_VERSION = "2.0.0"
    AGENT_DESCRIPTION = "Converts infrastructure config between Glue, EMR, EKS with Karpenter"

    DEPENDENCIES = ['sizing_agent', 'resource_allocator_agent']
    PARALLEL_SAFE = True

    # Platform thresholds
    EMR_THRESHOLD_GB = 100
    EKS_THRESHOLD_GB = 500

    # Instance mappings
    GLUE_TO_EMR = {
        'G.1X': 'm5.xlarge',
        'G.2X': 'm5.2xlarge',
        'G.4X': 'm5.4xlarge',
        'G.8X': 'm5.8xlarge'
    }

    GLUE_TO_EKS = {
        'G.1X': {'cpu': '4', 'memory': '16Gi'},
        'G.2X': {'cpu': '8', 'memory': '32Gi'},
        'G.4X': {'cpu': '16', 'memory': '64Gi'},
        'G.8X': {'cpu': '32', 'memory': '128Gi'}
    }

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.storage = StrandsStorage(config)

    def execute(self, context: AgentContext) -> AgentResult:
        # Check if agent is enabled via agents.platform_conversion_agent.enabled
        if not self.is_enabled('agents.platform_conversion_agent.enabled'):
            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.COMPLETED,
                output={'skipped': True, 'reason': 'Platform conversion agent disabled'}
            )

        platform_config = context.config.get('platform', {})
        auto_convert = platform_config.get('auto_convert', {})

        # Also check platform.auto_convert.enabled for backwards compatibility
        if not auto_convert.get('enabled') in ('Y', 'y', True):
            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.COMPLETED,
                output={'skipped': True, 'reason': 'Auto-convert disabled in platform config'}
            )

        # Get sizing data
        total_size_gb = context.get_shared('total_size_gb', 0)
        recommended_workers = context.get_shared('recommended_workers', 10)
        recommended_type = context.get_shared('recommended_worker_type', 'G.2X')

        # Determine target platform
        emr_threshold = auto_convert.get('convert_to_emr_threshold_gb', self.EMR_THRESHOLD_GB)
        eks_threshold = auto_convert.get('convert_to_eks_threshold_gb', self.EKS_THRESHOLD_GB)
        force_platform = platform_config.get('force_platform')

        source_platform = 'glue'
        target_platform = 'glue'
        conversion_reason = None

        if force_platform:
            target_platform = force_platform
            conversion_reason = f"Forced to {force_platform}"
        elif total_size_gb > eks_threshold:
            target_platform = 'eks'
            conversion_reason = f"Data size {total_size_gb:.0f} GB > {eks_threshold} GB threshold"
        elif total_size_gb > emr_threshold:
            target_platform = 'emr'
            conversion_reason = f"Data size {total_size_gb:.0f} GB > {emr_threshold} GB threshold"

        converted_config = None
        recommendations = []

        if target_platform != source_platform:
            if target_platform == 'emr':
                converted_config = self._convert_to_emr(
                    recommended_workers, recommended_type, context.config
                )
                recommendations.append(f"Platform conversion: Glue → EMR ({conversion_reason})")
            elif target_platform == 'eks':
                converted_config = self._convert_to_eks(
                    recommended_workers, recommended_type, context.config
                )
                recommendations.append(f"Platform conversion: Glue → EKS with Karpenter ({conversion_reason})")

        # Store conversion
        conversion_data = {
            'job_name': context.job_name,
            'source_platform': source_platform,
            'target_platform': target_platform,
            'total_size_gb': total_size_gb,
            'conversion_reason': conversion_reason,
            'converted_config': converted_config
        }

        self.storage.store_agent_data(
            self.AGENT_NAME,
            'conversions',
            [conversion_data],
            use_pipe_delimited=True
        )

        # Share with other agents
        context.set_shared('target_platform', target_platform)
        context.set_shared('converted_config', converted_config)
        context.set_shared('platform_conversion_needed', target_platform != source_platform)

        # Log to unified audit for dashboard
        if AUDIT_AVAILABLE and get_audit_logger:
            try:
                audit = get_audit_logger(self.config)
                audit.log_platform_conversion(
                    job_name=context.job_name,
                    execution_id=context.execution_id,
                    source_platform=source_platform,
                    target_platform=target_platform,
                    reason=conversion_reason or 'No conversion needed'
                )
            except Exception:
                pass

        return AgentResult(
            agent_name=self.AGENT_NAME,
            agent_id=self.agent_id,
            status=AgentStatus.COMPLETED,
            output={
                'source_platform': source_platform,
                'target_platform': target_platform,
                'conversion_needed': target_platform != source_platform,
                'conversion_reason': conversion_reason,
                'converted_config': converted_config
            },
            metrics={
                'conversion_needed': 1 if target_platform != source_platform else 0
            },
            recommendations=recommendations
        )

    def _convert_to_emr(self, workers: int, worker_type: str, config: Dict) -> Dict:
        """Convert Glue config to EMR."""
        instance_type = self.GLUE_TO_EMR.get(worker_type, 'm5.2xlarge')
        emr_config = config.get('emr_config', {})

        return {
            'ClusterName': config.get('job_name', 'emr-job'),
            'ReleaseLabel': emr_config.get('release_label', 'emr-6.15.0'),
            'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}],
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': 'Master',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': instance_type,
                        'InstanceCount': 1
                    },
                    {
                        'Name': 'Core',
                        'Market': 'SPOT' if emr_config.get('use_spot') in ('Y', 'y', True) else 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': instance_type,
                        'InstanceCount': workers
                    }
                ],
                'KeepJobFlowAliveWhenNoSteps': False
            },
            'Configurations': [
                {
                    'Classification': 'spark-defaults',
                    'Properties': {
                        'spark.dynamicAllocation.enabled': 'true',
                        'spark.sql.adaptive.enabled': 'true'
                    }
                }
            ]
        }

    def _convert_to_eks(self, workers: int, worker_type: str, config: Dict) -> Dict:
        """Convert Glue config to EKS with Karpenter."""
        resources = self.GLUE_TO_EKS.get(worker_type, {'cpu': '8', 'memory': '32Gi'})
        eks_config = config.get('eks_config', {})

        return {
            'apiVersion': 'sparkoperator.k8s.io/v1beta2',
            'kind': 'SparkApplication',
            'metadata': {
                'name': config.get('job_name', 'spark-job'),
                'namespace': eks_config.get('namespace', 'spark-jobs')
            },
            'spec': {
                'type': 'Python',
                'mode': 'cluster',
                'image': 'spark:3.5.0',
                'sparkVersion': '3.5.0',
                'driver': {
                    'cores': 2,
                    'memory': '4g',
                    'serviceAccount': 'spark',
                    'nodeSelector': {
                        'karpenter.sh/provisioner-name': 'spark-driver'
                    }
                },
                'executor': {
                    'cores': int(resources['cpu']),
                    'memory': resources['memory'],
                    'instances': workers,
                    'nodeSelector': {
                        'karpenter.sh/provisioner-name': 'spark-executors'
                    }
                },
                'dynamicAllocation': {
                    'enabled': True,
                    'initialExecutors': workers,
                    'minExecutors': 2,
                    'maxExecutors': workers * 2
                }
            },
            'karpenter': {
                'provisioner': {
                    'name': 'spark-executors',
                    'requirements': [
                        {'key': 'karpenter.sh/capacity-type', 'operator': 'In', 'values': ['spot', 'on-demand']},
                        {'key': 'kubernetes.io/arch', 'operator': 'In', 'values': ['amd64', 'arm64']}
                    ],
                    'limits': {
                        'resources': {'cpu': str(workers * int(resources['cpu'])), 'memory': f"{workers * 32}Gi"}
                    }
                }
            }
        }
