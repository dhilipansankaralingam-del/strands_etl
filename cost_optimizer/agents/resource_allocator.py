"""
Resource Allocator Agent
========================

Determines optimal compute resources based on data size and code complexity.
Calculates cost comparisons and savings potential.
"""

from typing import Dict, List, Any
from .base import CostOptimizerAgent, AnalysisInput, AnalysisResult


class ResourceAllocatorAgent(CostOptimizerAgent):
    """Calculates optimal resource allocation and cost savings."""

    AGENT_NAME = "resource_allocator"

    # AWS Glue pricing (per DPU-hour)
    GLUE_PRICING = {
        'G.1X': {'cost': 0.44,  'memory_gb': 16,  'vcpu': 4},
        'G.2X': {'cost': 0.88,  'memory_gb': 32,  'vcpu': 8},
        'G.4X': {'cost': 1.76,  'memory_gb': 64,  'vcpu': 16},
        'G.8X': {'cost': 3.52,  'memory_gb': 128, 'vcpu': 32},
    }

    # EMR pricing (per instance-hour, on-demand EC2 + EMR fee)
    EMR_PRICING = {
        'm5.xlarge':    {'cost': 0.230, 'memory_gb': 16,  'vcpu': 4},
        'm5.2xlarge':   {'cost': 0.461, 'memory_gb': 32,  'vcpu': 8},
        'm5.4xlarge':   {'cost': 0.922, 'memory_gb': 64,  'vcpu': 16},
        'm5.8xlarge':   {'cost': 1.843, 'memory_gb': 128, 'vcpu': 32},
        'r5.xlarge':    {'cost': 0.302, 'memory_gb': 32,  'vcpu': 4},
        'r5.2xlarge':   {'cost': 0.605, 'memory_gb': 64,  'vcpu': 8},
        'r6g.2xlarge':  {'cost': 0.410, 'memory_gb': 64,  'vcpu': 8},   # Graviton
    }

    # Azure HDInsight pricing (per node-hour, D-series)
    AZURE_HDI_PRICING = {
        'D4s_v3':  {'cost': 0.192, 'memory_gb': 16, 'vcpu': 4},
        'D8s_v3':  {'cost': 0.384, 'memory_gb': 32, 'vcpu': 8},
        'D16s_v3': {'cost': 0.768, 'memory_gb': 64, 'vcpu': 16},
    }

    # Azure Synapse Spark (per node-hour, Medium pool)
    AZURE_SYNAPSE_PRICING = {
        'Small':   {'cost': 0.16,  'memory_gb': 8,  'vcpu': 2},
        'Medium':  {'cost': 0.32,  'memory_gb': 28, 'vcpu': 4},
        'Large':   {'cost': 0.64,  'memory_gb': 56, 'vcpu': 8},
        'XLarge':  {'cost': 1.28,  'memory_gb': 112,'vcpu': 16},
    }

    # Databricks (DBU cost + underlying instance)
    DATABRICKS_AWS_PRICING = {
        'Standard': {'dbu_cost': 0.07, 'ec2_type': 'm5.2xlarge'},  # jobs compute DBU
        'Enhanced': {'dbu_cost': 0.10, 'ec2_type': 'm5.2xlarge'},  # all-purpose DBU
    }

    # GCP Dataproc (per node-hour = Compute Engine + Dataproc fee)
    GCP_DATAPROC_PRICING = {
        'n2-standard-4':  {'cost': 0.243, 'memory_gb': 16, 'vcpu': 4},
        'n2-standard-8':  {'cost': 0.485, 'memory_gb': 32, 'vcpu': 8},
        'n2-standard-16': {'cost': 0.970, 'memory_gb': 64, 'vcpu': 16},
        'n2-highmem-8':   {'cost': 0.580, 'memory_gb': 64, 'vcpu': 8},
    }
    GCP_SERVERLESS_DCU_COST = 0.066  # per DCU-hour

    SPOT_DISCOUNT = 0.70   # ~70% on average for spot/preemptible

    def _analyze_rule_based(self, input_data: AnalysisInput, context: Dict) -> AnalysisResult:
        """Rule-based resource allocation analysis."""

        # Get size and complexity from context
        effective_size_gb = context.get('effective_size_gb', 100)
        complexity_score = context.get('complexity_score', 50)
        join_count = context.get('join_count', 0)
        skew_risk_score = context.get('skew_risk_score', 20)

        # Current configuration
        current_config = input_data.current_config
        current_workers = current_config.get('number_of_workers', 10)
        current_worker_type = current_config.get('worker_type', 'G.2X')
        current_platform = current_config.get('platform', 'glue')

        # Calculate optimal configuration
        optimal_config = self._calculate_optimal_config(
            effective_size_gb, complexity_score, join_count, skew_risk_score
        )

        # Calculate costs
        estimated_duration_hours = self._estimate_duration(
            effective_size_gb, optimal_config['workers'], complexity_score
        )

        current_cost = self._calculate_glue_cost(
            current_workers, current_worker_type, estimated_duration_hours
        )

        optimal_glue_cost = self._calculate_glue_cost(
            optimal_config['workers'],
            optimal_config['worker_type'],
            estimated_duration_hours
        )

        # Calculate EMR alternatives
        emr_ondemand_cost = self._calculate_emr_cost(
            optimal_config['workers'],
            optimal_config['emr_instance_type'],
            estimated_duration_hours,
            use_spot=False
        )

        emr_spot_cost = self._calculate_emr_cost(
            optimal_config['workers'],
            optimal_config['emr_instance_type'],
            estimated_duration_hours,
            use_spot=True
        )

        # EKS with Karpenter (spot) – ~10% better bin-packing vs raw EMR spot
        eks_spot_cost = emr_spot_cost * 0.90

        # Azure / GCP / Databricks alternatives
        azure_hdi_cost = self._calculate_azure_hdi_cost(
            optimal_config['workers'], estimated_duration_hours
        )
        azure_synapse_cost = self._calculate_azure_synapse_cost(
            optimal_config['workers'], estimated_duration_hours
        )
        databricks_aws_cost = self._calculate_databricks_aws_cost(
            optimal_config['workers'], estimated_duration_hours
        )
        databricks_aws_spot_cost = databricks_aws_cost * (1 - self.SPOT_DISCOUNT * 0.5)
        gcp_dataproc_cost = self._calculate_gcp_dataproc_cost(
            optimal_config['workers'], estimated_duration_hours, use_spot=False
        )
        gcp_dataproc_spot_cost = self._calculate_gcp_dataproc_cost(
            optimal_config['workers'], estimated_duration_hours, use_spot=True
        )

        # Full multi-cloud platform comparison
        platform_comparison = [
            {'platform': 'aws_glue',            'label': 'AWS Glue',                    'cost': optimal_glue_cost},
            {'platform': 'aws_emr_ondemand',    'label': 'AWS EMR (On-Demand)',          'cost': emr_ondemand_cost},
            {'platform': 'aws_emr_spot',        'label': 'AWS EMR (Spot)',               'cost': emr_spot_cost},
            {'platform': 'aws_eks_karpenter',   'label': 'AWS EKS + Karpenter (Spot)',   'cost': eks_spot_cost},
            {'platform': 'azure_hdinsight',     'label': 'Azure HDInsight',              'cost': azure_hdi_cost},
            {'platform': 'azure_synapse',       'label': 'Azure Synapse Spark',          'cost': azure_synapse_cost},
            {'platform': 'databricks_aws',      'label': 'Databricks on AWS',            'cost': databricks_aws_cost},
            {'platform': 'databricks_aws_spot', 'label': 'Databricks on AWS (Spot)',     'cost': databricks_aws_spot_cost},
            {'platform': 'gcp_dataproc',        'label': 'GCP Dataproc',                'cost': gcp_dataproc_cost},
            {'platform': 'gcp_dataproc_spot',   'label': 'GCP Dataproc (Spot)',          'cost': gcp_dataproc_spot_cost},
        ]
        platform_comparison.sort(key=lambda x: x['cost'])

        best_platform = platform_comparison[0]

        # Calculate savings
        savings_from_rightsizing = current_cost - optimal_glue_cost
        savings_from_platform = current_cost - best_platform['cost']

        # Annual projections (assuming daily runs)
        runs_per_year = input_data.additional_context.get('runs_per_year', 365)
        annual_current = current_cost * runs_per_year
        annual_optimal = best_platform['cost'] * runs_per_year
        annual_savings = annual_current - annual_optimal

        analysis = {
            'current_config': {
                'platform': current_platform,
                'workers': current_workers,
                'worker_type': current_worker_type,
                'cost_per_run': round(current_cost, 2),
                'annual_cost': round(annual_current, 2)
            },
            'optimal_config': {
                'platform': best_platform['platform'],
                'workers': optimal_config['workers'],
                'worker_type': optimal_config['worker_type'],
                'emr_instance_type': optimal_config['emr_instance_type'],
                'cost_per_run': round(best_platform['cost'], 2),
                'annual_cost': round(annual_optimal, 2)
            },
            'estimated_duration_hours': round(estimated_duration_hours, 2),
            'effective_size_gb': effective_size_gb,
            'complexity_factor': optimal_config['complexity_factor'],
            'platform_comparison': [
                {
                    'platform':                  p['platform'],
                    'label':                     p.get('label', p['platform']),
                    'cost_per_run':              round(p['cost'], 3),
                    'annual_cost':               round(p['cost'] * runs_per_year, 2),
                    'savings_vs_current_percent': round(
                        (current_cost - p['cost']) / current_cost * 100, 1
                    ) if current_cost > 0 else 0,
                }
                for p in platform_comparison
            ],
            'savings': {
                'rightsizing_per_run': round(savings_from_rightsizing, 2),
                'platform_per_run': round(savings_from_platform, 2),
                'total_per_run': round(savings_from_platform, 2),
                'percent': round(savings_from_platform / current_cost * 100, 1) if current_cost > 0 else 0,
                'annual_savings': round(annual_savings, 2)
            },
            'resource_efficiency': {
                'current_gb_per_worker': round(effective_size_gb / current_workers, 1),
                'optimal_gb_per_worker': round(effective_size_gb / optimal_config['workers'], 1),
                'memory_utilization_estimate': self._estimate_memory_utilization(
                    effective_size_gb, optimal_config['workers'], optimal_config['worker_type']
                )
            }
        }

        recommendations = self._generate_recommendations(analysis, input_data)

        return AnalysisResult(
            agent_name=self.AGENT_NAME,
            success=True,
            analysis=analysis,
            recommendations=recommendations,
            metrics={
                'current_cost': current_cost,
                'optimal_cost': best_platform['cost'],
                'savings_percent': analysis['savings']['percent'],
                'annual_savings': annual_savings
            }
        )

    def _calculate_optimal_config(
        self, size_gb: float, complexity: int, joins: int, skew_risk: int
    ) -> Dict:
        """Calculate optimal worker configuration."""

        # Base workers from size
        base_workers = max(2, int(size_gb / 10))

        # Complexity factor
        complexity_factor = 1.0
        if complexity > 70:
            complexity_factor = 1.5
        elif complexity > 50:
            complexity_factor = 1.3
        elif complexity > 30:
            complexity_factor = 1.1

        # Join factor
        join_factor = 1.0 + (joins * 0.05)

        # Skew factor
        skew_factor = 1.0 + (skew_risk / 200)  # Max 1.5x at 100 skew risk

        # Calculate workers
        optimal_workers = int(base_workers * complexity_factor * join_factor * skew_factor)
        optimal_workers = max(2, min(100, optimal_workers))  # Cap at 2-100

        # Determine worker type based on memory needs
        memory_per_worker = (size_gb / optimal_workers) * 2  # 2x headroom

        if memory_per_worker <= 8:
            worker_type = 'G.1X'
            emr_type = 'm5.xlarge'
        elif memory_per_worker <= 16:
            worker_type = 'G.2X'
            emr_type = 'm5.2xlarge'
        elif memory_per_worker <= 32:
            worker_type = 'G.4X'
            emr_type = 'm5.4xlarge'
        else:
            worker_type = 'G.8X'
            emr_type = 'm5.8xlarge'

        return {
            'workers': optimal_workers,
            'worker_type': worker_type,
            'emr_instance_type': emr_type,
            'complexity_factor': round(complexity_factor * join_factor * skew_factor, 2),
            'memory_per_worker_gb': round(memory_per_worker, 1)
        }

    def _estimate_duration(self, size_gb: float, workers: int, complexity: int) -> float:
        """Estimate job duration in hours."""
        # Base: 10 GB per worker per hour
        base_hours = size_gb / (workers * 10)

        # Complexity overhead
        complexity_overhead = 1.0 + (complexity / 200)

        # Minimum 0.1 hours (6 minutes), maximum 6 hours
        duration = base_hours * complexity_overhead
        return max(0.1, min(6.0, duration))

    def _calculate_glue_cost(self, workers: int, worker_type: str, hours: float) -> float:
        """Calculate AWS Glue cost."""
        pricing = self.GLUE_PRICING.get(worker_type, self.GLUE_PRICING['G.2X'])
        return workers * pricing['cost'] * hours

    def _calculate_emr_cost(
        self, workers: int, instance_type: str, hours: float, use_spot: bool = False
    ) -> float:
        """Calculate EMR cost."""
        pricing = self.EMR_PRICING.get(instance_type, self.EMR_PRICING['m5.2xlarge'])
        cost = workers * pricing['cost'] * hours

        # Add 1 master node
        cost += pricing['cost'] * hours

        # EMR service fee (~20% of EC2 cost)
        cost *= 1.2

        if use_spot:
            cost *= (1 - self.SPOT_DISCOUNT)

        return cost

    def _calculate_azure_hdi_cost(self, workers: int, hours: float) -> float:
        """Azure HDInsight cost (D8s_v3 workers + 1 head node)."""
        pricing = self.AZURE_HDI_PRICING.get('D8s_v3', {'cost': 0.384})
        return (workers + 1) * pricing['cost'] * hours

    def _calculate_azure_synapse_cost(self, workers: int, hours: float) -> float:
        """Azure Synapse Analytics Spark cost (Medium pool)."""
        pricing = self.AZURE_SYNAPSE_PRICING.get('Medium', {'cost': 0.32})
        return (workers + 1) * pricing['cost'] * hours

    def _calculate_databricks_aws_cost(self, workers: int, hours: float) -> float:
        """Databricks on AWS (Jobs Compute DBU + m5.2xlarge EC2)."""
        dbu_cost = self.DATABRICKS_AWS_PRICING['Standard']['dbu_cost']
        ec2_type = self.DATABRICKS_AWS_PRICING['Standard']['ec2_type']
        ec2_cost = self.EMR_PRICING.get(ec2_type, {'cost': 0.384})['cost']
        return (workers + 1) * (dbu_cost + ec2_cost) * hours

    def _calculate_gcp_dataproc_cost(
        self, workers: int, hours: float, use_spot: bool = False
    ) -> float:
        """GCP Dataproc (n2-standard-8 + 1 master node)."""
        pricing = self.GCP_DATAPROC_PRICING.get('n2-standard-8', {'cost': 0.485})
        cost = (workers + 1) * pricing['cost'] * hours
        if use_spot:
            cost *= (1 - self.SPOT_DISCOUNT)
        return cost

    def _estimate_memory_utilization(self, size_gb: float, workers: int, worker_type: str) -> str:
        """Estimate memory utilization."""
        pricing = self.GLUE_PRICING.get(worker_type, self.GLUE_PRICING['G.2X'])
        total_memory = workers * pricing['memory_gb']

        # Estimate memory need: data size * 3 (for intermediate results, shuffles)
        estimated_need = size_gb * 3

        utilization = (estimated_need / total_memory) * 100

        if utilization < 30:
            return 'low (under-provisioned)'
        elif utilization < 70:
            return 'optimal'
        elif utilization < 90:
            return 'high'
        else:
            return 'critical (may OOM)'

    def _generate_recommendations(self, analysis: Dict, input_data: AnalysisInput) -> List[Dict]:
        """Generate resource allocation recommendations."""
        recommendations = []

        savings = analysis['savings']
        current = analysis['current_config']
        optimal = analysis['optimal_config']

        # Right-sizing recommendation
        if current['workers'] != optimal['workers']:
            direction = 'Reduce' if current['workers'] > optimal['workers'] else 'Increase'
            recommendations.append({
                'priority': 'P1',
                'category': 'resource',
                'title': f'{direction} Worker Count',
                'description': f"Change from {current['workers']} to {optimal['workers']} workers",
                'estimated_savings_usd': savings['rightsizing_per_run'],
                'implementation': f"Set number_of_workers = {optimal['workers']}"
            })

        # Worker type recommendation
        if current['worker_type'] != optimal['worker_type']:
            recommendations.append({
                'priority': 'P1',
                'category': 'resource',
                'title': 'Change Worker Type',
                'description': f"Change from {current['worker_type']} to {optimal['worker_type']}",
                'implementation': f"Set worker_type = {optimal['worker_type']}"
            })

        # Platform recommendation
        if savings['percent'] > 30:
            best = optimal['platform']
            if 'spot' in best or 'eks' in best:
                recommendations.append({
                    'priority': 'P0',
                    'category': 'architecture',
                    'title': f'Migrate to {best.replace("_", " ").title()}',
                    'description': f"Save {savings['percent']:.0f}% (${savings['annual_savings']:.0f}/year) by switching to {best}",
                    'estimated_savings_usd': savings['annual_savings'],
                    'implementation': f"""
1. Convert Glue script to Spark (use convert_to_eks.py)
2. Deploy to {'EKS with Karpenter' if 'eks' in best else 'EMR with Spot instances'}
3. Configure {'Karpenter NodePool' if 'eks' in best else 'instance fleet'} for spot
"""
                })

        # Memory utilization warning
        util = analysis['resource_efficiency']['memory_utilization_estimate']
        if 'under' in util:
            recommendations.append({
                'priority': 'P2',
                'category': 'resource',
                'title': 'Memory Under-utilized',
                'description': 'Current worker type has more memory than needed',
                'implementation': f"Consider smaller worker type: {optimal['worker_type']}"
            })
        elif 'critical' in util:
            recommendations.append({
                'priority': 'P0',
                'category': 'resource',
                'title': 'Memory at Risk',
                'description': 'Current configuration may cause OOM errors',
                'implementation': 'Increase workers or worker type'
            })

        return recommendations
