"""
Resource Allocator Agent
========================

Determines optimal compute resources based on data size and code complexity.
Calculates cost comparisons and savings potential.
"""

import json
from typing import Dict, List, Any
from .base import CostOptimizerAgent, AnalysisInput, AnalysisResult

try:
    from .scientific_tools import amdahls_law, littles_law_parallelism, spot_interruption_risk
    _HAS_SCIENTIFIC = True
except ImportError:
    _HAS_SCIENTIFIC = False

try:
    from .pipeline_tools import RESOURCE_AGENT_TOOLS as _RESOURCE_AGENT_TOOLS
except ImportError:
    _RESOURCE_AGENT_TOOLS = []


class ResourceAllocatorAgent(CostOptimizerAgent):
    """Calculates optimal resource allocation and cost savings."""

    AGENT_NAME     = "resource_allocator"
    AGENT_TOOLS    = _RESOURCE_AGENT_TOOLS   # compute_amdahls_ceiling, compute_spot_risk
    MAX_ITERATIONS = 3                        # Amdahl (from code findings) → Spot risk → final config

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

    def _build_scientific_section(
        self, context: Dict, total_live_gb: float, cur_workers: int
    ) -> str:
        """Build the SCIENTIFIC ANALYSIS block for the resource-allocator LLM prompt."""
        if not _HAS_SCIENTIFIC:
            return "  (scientific_tools not available)"

        lines: List[str] = []

        # 1. Amdahl's Law — pull serial fraction from CodeAnalyzerAgent's scientific output
        code_sci = context.get('code_analyzer_full', {}).get('scientific_analysis', {})
        amdahl   = code_sci.get('amdahls_law', {})
        if amdahl:
            sf = amdahl.get('estimated_serial_fraction_pct', 0)
            lines.append(
                f"Amdahl's Law (from code analysis):\n"
                f"  serial_fraction={sf:.0f}%  "
                f"speedup@{cur_workers}w={amdahl.get('amdahl_speedup',0):.2f}x  "
                f"theoretical_max={amdahl.get('theoretical_max_speedup',0):.2f}x  "
                f"diminishing_returns_elbow={amdahl.get('diminishing_returns_elbow',0)} workers\n"
                f"  → Do NOT recommend more than {amdahl.get('diminishing_returns_elbow', cur_workers)} "
                f"workers unless serial ops are eliminated first."
            )

        # 2. Little's Law — derive optimal shuffle.partitions from glue_metrics task duration
        glue_metrics = context.get('glue_metrics', {})
        avg_task_sec = 0.0
        for metric, vals in glue_metrics.items():
            if not isinstance(vals, list) or not vals:
                continue
            if 'task' in metric.lower() and 'duration' in metric.lower():
                avg_task_sec = sum(vals) / len(vals)
                break
        if avg_task_sec > 0:
            vcpu_per_worker = self.GLUE_PRICING.get(
                context.get('current_config', {}).get('worker_type', 'G.2X'), {}
            ).get('vcpu', 8)
            num_executors = cur_workers * vcpu_per_worker
            ll = littles_law_parallelism(
                avg_task_sec=avg_task_sec,
                num_executors=num_executors,
            )
            lines.append(
                f"Little's Law (shuffle.partitions):\n"
                f"  avg_task={avg_task_sec:.1f}s  executors={num_executors}  "
                f"→ optimal_shuffle_partitions={ll.get('optimal_shuffle_partitions',200)}\n"
                f"  executor_utilisation={ll.get('executor_utilisation',0):.1%}"
            )

        # 3. Spot interruption risk — estimated from effective data size
        est_hours = max(0.5, total_live_gb / 50.0)  # ~50 GB/hr throughput heuristic
        spot = spot_interruption_risk(
            job_duration_hours=est_hours,
            hourly_interruption_rate=0.05,  # m5-family average
        )
        lines.append(
            f"Spot Interruption Risk (m5-family, est. {est_hours:.1f}h job):\n"
            f"  p_survive={spot.get('p_survive_full_job',0):.1%}  "
            f"p_interrupted={spot.get('p_interrupted',0):.1%}  "
            f"net_savings={spot.get('net_savings_pct',0):.0f}%\n"
            f"  → {spot.get('recommendation','')}"
        )

        return "\n\n".join(lines) if lines else "  (no scientific inputs available)"

    def _build_llm_prompt(self, input_data: AnalysisInput, context: Dict) -> str:
        """LLM prompt: uses actual Iceberg table sizes + Glue runtime metrics for precise right-sizing."""

        # Pull full sizing telemetry
        size_full = context.get('size_analyzer_full', {})
        table_findings = size_full.get('table_findings', {})
        agg_storage = size_full.get('aggregate_storage_costs', {})

        # Compact table-level facts
        table_lines: List[str] = []
        total_live_gb = 0.0
        for tbl, f in table_findings.items():
            fs = f.get('file_stats', {})
            gf = f.get('growth_forecast', {})
            gb = fs.get('total_size_gb', 0)
            total_live_gb += gb
            table_lines.append(
                f"  {tbl}: {gb:.2f} GB | files={fs.get('file_count',0):,} "
                f"avg={fs.get('avg_file_mb',0):.1f} MB | skew={fs.get('skew_ratio',1):.1f}x | "
                f"growth={gf.get('gb_per_day',0):.2f} GB/day"
            )
        table_ctx = "\n".join(table_lines) or "  (no Iceberg telemetry)"

        # Glue runtime metrics summary
        glue_metrics = context.get('glue_metrics', {})
        metrics_lines: List[str] = []
        heap_peak = 0.0
        cpu_worker_min = 1.0
        worker_util_min = 1.0
        if glue_metrics:
            for m, vals in glue_metrics.items():
                if not isinstance(vals, list) or not vals:
                    continue
                metrics_lines.append(
                    f"  {m}: min={min(vals):.2f} max={max(vals):.2f} last={vals[-1]:.2f}"
                )
                if 'jvm.heap' in m:
                    heap_peak = max(heap_peak, max(vals))
                if 'ALL.system.cpu' in m:
                    cpu_worker_min = min(cpu_worker_min, min(vals))
                if 'workerutilized' in m:
                    worker_util_min = min(worker_util_min, min(vals))
        metrics_ctx = "\n".join(metrics_lines) or "  (no runtime metrics)"

        # Current config
        cur_workers     = input_data.current_config.get('number_of_workers', 10)
        cur_type        = input_data.current_config.get('worker_type', 'G.2X')
        runs_per_year   = input_data.additional_context.get('runs_per_year', 365)
        complexity_score = context.get('complexity_score', 50)
        join_count       = context.get('join_count', 0)
        anti_patterns    = context.get('anti_pattern_count', 0)

        # Rule-based pre-calc as starting point
        rule = self._analyze_rule_based(input_data, context)
        rule_optimal = rule.analysis.get('optimal_config', {})
        rule_savings  = rule.analysis.get('savings', {})
        rule_platform = rule.analysis.get('platform_comparison', [])

        tool_guidance = """
TOOLS AVAILABLE (call selectively — use only when evidence justifies it):
  compute_amdahls_ceiling(serial_fraction_pct, current_workers)
      → CALL IF code_analyzer scientific_analysis contains amdahls_law results
        OR if anti_pattern_count > 0 and serial ops (collect/toPandas) are listed.
        The elbow is the hard cap — never recommend more workers than that.
  compute_spot_risk(job_duration_hours, instance_type, checkpoint_interval_hours)
      → CALL IF you are about to recommend EMR Spot or EKS Spot as an alternative.
        Estimate job_duration_hours from: total_live_gb / 50 (≈ 50 GB/hr for G.2X).
        Only recommend Spot if net_savings_pct > 30 AND p_survive > 0.75.
After calling any tools, respond with the JSON object specified below.
""" if self.AGENT_TOOLS else ""

        return f"""You are a Principal Cloud Architect specialising in Spark right-sizing and cost optimisation.
Determine the OPTIMAL compute configuration using the actual runtime evidence below.
{tool_guidance}
Rule-based analysis is provided as a starting point — override it where the evidence justifies.

══════════════════════════════════════════════════════
JOB METADATA
══════════════════════════════════════════════════════
Script      : {input_data.script_path}
Job         : {input_data.job_name}
Mode        : {input_data.processing_mode}
Current     : {cur_workers} × {cur_type}
Complexity  : {complexity_score}/100  |  joins={join_count}  |  anti-patterns={anti_patterns}
Runs/year   : {runs_per_year}

══════════════════════════════════════════════════════
ACTUAL TABLE SIZES  (Athena $files — current snapshot only)
══════════════════════════════════════════════════════
{table_ctx}
Total live data: {total_live_gb:.2f} GB

══════════════════════════════════════════════════════
GLUE RUNTIME METRICS  (CloudWatch)
══════════════════════════════════════════════════════
{metrics_ctx}

Signal interpretation:
  heap_peak={heap_peak:.2f}  → {'OOM risk: upgrade worker type or reduce partition size' if heap_peak > 0.85 else 'heap OK' if heap_peak > 0 else 'unknown'}
  cpu_worker_min={cpu_worker_min:.2f} → {'severe skew: executors idle mid-job' if cpu_worker_min < 0.20 else 'mild idle' if cpu_worker_min < 0.50 else 'CPU healthy'}
  worker_util_min={worker_util_min:.2f} → {'over-provisioned: many workers idle' if worker_util_min < 0.40 else 'utilisation OK'}

══════════════════════════════════════════════════════
RULE-BASED PRE-CALC  (override where runtime evidence differs)
══════════════════════════════════════════════════════
Rule optimal: {rule_optimal.get('workers')} × {rule_optimal.get('worker_type')}
Rule savings: {rule_savings.get('percent',0):.0f}%  (${rule_savings.get('annual_savings',0):,.0f}/year)
Platform comparison (rule-based):
{json.dumps([{{'platform': p['platform'], 'cost_per_run': p['cost_per_run']}} for p in rule_platform[:5]], indent=2)}

══════════════════════════════════════════════════════
AWS PRICING REFERENCE
══════════════════════════════════════════════════════
Glue workers (per worker-hour): G.1X=$0.44  G.2X=$0.88  G.4X=$1.76  G.8X=$3.52
EMR on-demand: m5.xlarge=$0.230  m5.2xlarge=$0.461  m5.4xlarge=$0.922
EMR spot: ~70% discount on above
EKS+Karpenter spot: ~10% better bin-packing than EMR spot
Databricks AWS (Jobs): $0.07 DBU + $0.461 EC2 per worker-hour
GCP Dataproc: n2-standard-8=$0.485  spot ~70% discount

══════════════════════════════════════════════════════
RIGHT-SIZING GUIDELINES
══════════════════════════════════════════════════════
Worker count formula:
  base = ceil(total_live_gb / 10)
  multiply by complexity_factor (1.0–1.5 based on joins/skew)
  cap between 2 and 100

Worker type selection (memory headroom = 2× peak partition size):
  peak_partition_gb = total_live_gb / shuffle_partitions
  G.1X (16 GB executor) if peak_partition_gb < 4
  G.2X (32 GB executor) if peak_partition_gb < 12
  G.4X (64 GB executor) if peak_partition_gb < 28
  G.8X (128 GB executor) otherwise

Metric overrides:
  heap_peak > 0.85 AND worker_type < G.4X  → upgrade worker type one tier
  heap_peak < 0.40                          → downgrade worker type one tier
  worker_util_min < 0.40                   → reduce workers to peak_utilised
  cpu_worker_min < 0.20 (skew)             → SAME workers, fix skew first (salting/AQE)

Platform selection:
  Cost difference > 30% vs Glue → recommend migration
  Prefer EKS+Karpenter for batch, EMR spot for ad-hoc, Glue for serverless simplicity

══════════════════════════════════════════════════════
SCIENTIFIC ANALYSIS  (factor these into your decision)
══════════════════════════════════════════════════════
{self._build_scientific_section(context, total_live_gb, cur_workers)}

Respond ONLY with a JSON object:
{{
  "current_config": {{
    "platform": "glue", "workers": {cur_workers}, "worker_type": "{cur_type}",
    "cost_per_run": <float>, "annual_cost": <float>
  }},
  "optimal_config": {{
    "platform": "<best platform>",
    "workers": <int>,
    "worker_type": "<G.1X|G.2X|G.4X|G.8X>",
    "emr_instance_type": "<m5.Nxlarge>",
    "cost_per_run": <float>,
    "annual_cost": <float>,
    "rationale": "<one sentence citing the specific metric or size evidence>"
  }},
  "estimated_duration_hours": <float>,
  "effective_size_gb": {total_live_gb:.2f},
  "platform_comparison": [
    {{"platform": "...", "label": "...", "cost_per_run": <float>,
      "annual_cost": <float>, "savings_vs_current_percent": <float>}}
  ],
  "savings": {{
    "rightsizing_per_run": <float>,
    "platform_per_run": <float>,
    "percent": <float>,
    "annual_savings": <float>
  }},
  "resource_efficiency": {{
    "current_gb_per_worker": <float>,
    "optimal_gb_per_worker": <float>,
    "memory_utilization_estimate": "<low|optimal|high|critical>"
  }},
  "metric_driven_adjustments": [
    "<what metric triggered what change — one item per adjustment>"
  ],
  "recommendations": [
    {{"priority": "P0|P1|P2", "category": "resource|architecture",
      "title": "...", "description": "...", "implementation": "...",
      "estimated_savings_usd": <float>}}
  ]
}}
"""

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
