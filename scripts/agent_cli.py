#!/usr/bin/env python3
"""
Interactive Agent CLI with Prediction & Extrapolation
======================================================

Allows users to interact with trained agents and get predictions for:
- Cost estimation when scaling
- Memory requirements for different data volumes
- Platform recommendations
- Performance predictions

Usage:
    python scripts/agent_cli.py --config demo_configs/complex_demo_config.json
    python scripts/agent_cli.py --agent workload
    python scripts/agent_cli.py --predict-cost --records 1000000
"""

import os
import sys
import json
import math
import argparse
import readline
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
from datetime import datetime

# Add framework to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from framework.agents.workload_assessment_agent import WorkloadAssessmentAgent
from framework.agents.learning_agent import LearningAgent
from framework.agents.data_quality_agent import DataQualityAgent
from framework.agents.code_analysis_agent import CodeAnalysisAgent
from framework.agents.compliance_agent import ComplianceAgent
from framework.agents.recommendation_agent import RecommendationAgent


@dataclass
class ExecutionHistory:
    """Historical execution data for predictions."""
    job_name: str
    records_processed: int
    duration_seconds: float
    dpu_hours: float
    cost_usd: float
    memory_gb: float
    platform: str
    workers: int
    timestamp: datetime
    status: str


@dataclass
class PredictionResult:
    """Result of a prediction query."""
    metric: str
    current_value: float
    predicted_value: float
    scale_factor: float
    confidence: float
    recommendations: List[str] = field(default_factory=list)
    platform_change_suggested: bool = False
    suggested_platform: str = ""


class AgentDataStore:
    """
    In-memory store for agent training data.
    In production, this would connect to DynamoDB or similar.
    """

    def __init__(self):
        self.execution_history: List[ExecutionHistory] = []
        self.baselines: Dict[str, Any] = {}
        self.anomalies: List[Dict] = []
        self.recommendations: List[Dict] = []

        # Load sample historical data for demo
        self._load_sample_data()

    def _load_sample_data(self):
        """Load sample historical data for prediction capabilities."""
        # Sample execution history for different record counts
        sample_runs = [
            ExecutionHistory(
                job_name="sales_analytics",
                records_processed=10000,
                duration_seconds=120,
                dpu_hours=0.2,
                cost_usd=0.09,
                memory_gb=4.0,
                platform="glue",
                workers=2,
                timestamp=datetime.now(),
                status="SUCCEEDED"
            ),
            ExecutionHistory(
                job_name="sales_analytics",
                records_processed=100000,
                duration_seconds=480,
                dpu_hours=0.8,
                cost_usd=0.35,
                memory_gb=8.0,
                platform="glue",
                workers=5,
                timestamp=datetime.now(),
                status="SUCCEEDED"
            ),
            ExecutionHistory(
                job_name="sales_analytics",
                records_processed=500000,
                duration_seconds=1800,
                dpu_hours=3.0,
                cost_usd=1.32,
                memory_gb=16.0,
                platform="glue",
                workers=10,
                timestamp=datetime.now(),
                status="SUCCEEDED"
            ),
            ExecutionHistory(
                job_name="sales_analytics",
                records_processed=1000000,
                duration_seconds=3600,
                dpu_hours=6.0,
                cost_usd=2.64,
                memory_gb=32.0,
                platform="emr",
                workers=20,
                timestamp=datetime.now(),
                status="SUCCEEDED"
            ),
            ExecutionHistory(
                job_name="customer_etl",
                records_processed=50000,
                duration_seconds=300,
                dpu_hours=0.5,
                cost_usd=0.22,
                memory_gb=8.0,
                platform="glue",
                workers=3,
                timestamp=datetime.now(),
                status="SUCCEEDED"
            ),
        ]
        self.execution_history = sample_runs

        # Sample baselines
        self.baselines = {
            'sales_analytics': {
                'avg_duration': 1500,
                'avg_cost': 1.10,
                'avg_records': 403000,
                'avg_memory_gb': 15.0,
                'typical_workers': 9
            },
            'customer_etl': {
                'avg_duration': 300,
                'avg_cost': 0.22,
                'avg_records': 50000,
                'avg_memory_gb': 8.0,
                'typical_workers': 3
            }
        }

    def add_execution(self, history: ExecutionHistory):
        """Add an execution to history."""
        self.execution_history.append(history)

    def get_history_for_job(self, job_name: str) -> List[ExecutionHistory]:
        """Get execution history for a specific job."""
        return [h for h in self.execution_history if h.job_name == job_name]


class PredictionEngine:
    """
    Prediction and extrapolation engine for ETL metrics.
    Uses historical data to predict cost, memory, duration for different scales.
    """

    # Platform thresholds (records where platform change is recommended)
    GLUE_MAX_EFFICIENT_RECORDS = 500000
    EMR_MIN_RECORDS = 250000
    EMR_MAX_EFFICIENT_RECORDS = 10000000
    EKS_MIN_RECORDS = 5000000

    # Pricing constants
    GLUE_DPU_HOUR_COST = 0.44
    EMR_NORMALIZED_HOUR_COST = 0.10
    EKS_VCPU_HOUR_COST = 0.05

    def __init__(self, data_store: AgentDataStore):
        self.data_store = data_store

    def predict_for_scale(self,
                          job_name: str,
                          current_records: int,
                          target_records: int) -> Dict[str, PredictionResult]:
        """
        Predict metrics when scaling from current to target records.
        Returns predictions for cost, duration, memory, and platform.
        """
        predictions = {}
        scale_factor = target_records / current_records if current_records > 0 else 1.0

        # Get historical data for this job
        history = self.data_store.get_history_for_job(job_name)
        baseline = self.data_store.baselines.get(job_name, {})

        # If we have historical data, use regression-like prediction
        if history:
            predictions['cost'] = self._predict_cost(history, current_records, target_records, scale_factor)
            predictions['duration'] = self._predict_duration(history, current_records, target_records, scale_factor)
            predictions['memory'] = self._predict_memory(history, current_records, target_records, scale_factor)
            predictions['workers'] = self._predict_workers(history, current_records, target_records, scale_factor)
        else:
            # Use baseline estimates
            predictions['cost'] = self._estimate_cost(baseline, target_records, scale_factor)
            predictions['duration'] = self._estimate_duration(baseline, target_records, scale_factor)
            predictions['memory'] = self._estimate_memory(baseline, target_records, scale_factor)
            predictions['workers'] = self._estimate_workers(baseline, target_records, scale_factor)

        # Add platform recommendation
        predictions['platform'] = self._recommend_platform(target_records, predictions)

        return predictions

    def _predict_cost(self, history: List[ExecutionHistory],
                      current: int, target: int, scale: float) -> PredictionResult:
        """Predict cost using historical data."""
        # Find closest historical data points
        sorted_history = sorted(history, key=lambda x: x.records_processed)

        # Use log-linear interpolation (cost typically grows sub-linearly)
        if len(sorted_history) >= 2:
            # Fit a simple log-linear model
            records_list = [h.records_processed for h in sorted_history]
            costs_list = [h.cost_usd for h in sorted_history]

            # Calculate cost per million records at different scales
            cpms = [c / (r / 1000000) if r > 0 else 0 for r, c in zip(records_list, costs_list)]
            avg_cpm = sum(cpms) / len(cpms) if cpms else 2.64

            # Economies of scale factor (cost grows slower than linearly)
            scale_efficiency = 0.85 if scale > 1 else 1.0

            current_cost = self._find_nearest_cost(sorted_history, current)
            predicted_cost = current_cost * (scale ** scale_efficiency)

            confidence = 0.85 if len(history) >= 3 else 0.70
        else:
            current_cost = history[0].cost_usd if history else 0.50
            predicted_cost = current_cost * scale * 0.9  # Assume some efficiency gain
            confidence = 0.60

        recommendations = []
        if predicted_cost > 5.0:
            recommendations.append("Consider reserved capacity for cost savings")
        if predicted_cost > 10.0:
            recommendations.append("Enable Spark adaptive query execution for optimization")

        return PredictionResult(
            metric='cost_usd',
            current_value=current_cost,
            predicted_value=round(predicted_cost, 2),
            scale_factor=scale,
            confidence=confidence,
            recommendations=recommendations
        )

    def _predict_duration(self, history: List[ExecutionHistory],
                          current: int, target: int, scale: float) -> PredictionResult:
        """Predict execution duration."""
        sorted_history = sorted(history, key=lambda x: x.records_processed)

        if len(sorted_history) >= 2:
            # Duration typically grows linearly with records
            durations = [h.duration_seconds for h in sorted_history]
            records = [h.records_processed for h in sorted_history]

            # Calculate throughput (records per second)
            throughputs = [r / d if d > 0 else 0 for r, d in zip(records, durations)]
            avg_throughput = sum(throughputs) / len(throughputs) if throughputs else 1000

            current_duration = self._find_nearest_duration(sorted_history, current)

            # Duration scales with records but improves with parallelism
            # Assuming we can add more workers
            parallelism_factor = min(0.95, 1.0 - (math.log10(scale) * 0.05)) if scale > 1 else 1.0
            predicted_duration = current_duration * scale * parallelism_factor

            confidence = 0.80 if len(history) >= 3 else 0.65
        else:
            current_duration = history[0].duration_seconds if history else 300
            predicted_duration = current_duration * scale * 0.95
            confidence = 0.55

        recommendations = []
        if predicted_duration > 3600:  # > 1 hour
            recommendations.append("Consider partitioning data for parallel processing")
        if predicted_duration > 7200:  # > 2 hours
            recommendations.append("Enable checkpointing for long-running jobs")
            recommendations.append("Consider breaking into multiple stages")

        return PredictionResult(
            metric='duration_seconds',
            current_value=current_duration,
            predicted_value=round(predicted_duration, 0),
            scale_factor=scale,
            confidence=confidence,
            recommendations=recommendations
        )

    def _predict_memory(self, history: List[ExecutionHistory],
                        current: int, target: int, scale: float) -> PredictionResult:
        """Predict memory requirements."""
        sorted_history = sorted(history, key=lambda x: x.records_processed)

        if len(sorted_history) >= 2:
            memories = [h.memory_gb for h in sorted_history]
            records = [h.records_processed for h in sorted_history]

            # Memory per million records
            mpm = [m / (r / 1000000) if r > 0 else 8 for r, m in zip(records, memories)]
            avg_mpm = sum(mpm) / len(mpm) if mpm else 16

            current_memory = self._find_nearest_memory(sorted_history, current)

            # Memory scales with sqrt of records (due to optimized structures)
            memory_scale = math.sqrt(scale) if scale > 1 else scale
            predicted_memory = current_memory * memory_scale

            # Round up to common memory sizes
            predicted_memory = self._round_to_memory_tier(predicted_memory)

            confidence = 0.75 if len(history) >= 3 else 0.60
        else:
            current_memory = history[0].memory_gb if history else 8
            predicted_memory = self._round_to_memory_tier(current_memory * math.sqrt(scale))
            confidence = 0.50

        recommendations = []
        if predicted_memory > 32:
            recommendations.append("Use memory-optimized instance types")
        if predicted_memory > 64:
            recommendations.append("Consider data partitioning to reduce memory pressure")
        if predicted_memory > 128:
            recommendations.append("Implement streaming processing to avoid full data load")

        return PredictionResult(
            metric='memory_gb',
            current_value=current_memory,
            predicted_value=predicted_memory,
            scale_factor=scale,
            confidence=confidence,
            recommendations=recommendations
        )

    def _predict_workers(self, history: List[ExecutionHistory],
                         current: int, target: int, scale: float) -> PredictionResult:
        """Predict worker/executor count."""
        sorted_history = sorted(history, key=lambda x: x.records_processed)

        if len(sorted_history) >= 2:
            workers = [h.workers for h in sorted_history]
            records = [h.records_processed for h in sorted_history]

            current_workers = self._find_nearest_workers(sorted_history, current)

            # Workers scale sub-linearly with records
            worker_scale = math.sqrt(scale) if scale > 1 else 1
            predicted_workers = max(2, int(current_workers * worker_scale))

            # Cap at reasonable limits
            predicted_workers = min(predicted_workers, 100)

            confidence = 0.80
        else:
            current_workers = history[0].workers if history else 5
            predicted_workers = max(2, int(current_workers * math.sqrt(scale)))
            confidence = 0.60

        recommendations = []
        if predicted_workers > 20:
            recommendations.append("Consider using dynamic allocation")
        if predicted_workers > 50:
            recommendations.append("EMR or EKS recommended for better cluster management")

        return PredictionResult(
            metric='workers',
            current_value=current_workers,
            predicted_value=predicted_workers,
            scale_factor=scale,
            confidence=confidence,
            recommendations=recommendations
        )

    def _recommend_platform(self, target_records: int,
                            predictions: Dict[str, PredictionResult]) -> PredictionResult:
        """Recommend appropriate platform based on scale."""
        current_platform = "glue"  # Default
        suggested_platform = "glue"
        platform_change = False
        recommendations = []

        memory_pred = predictions.get('memory')
        workers_pred = predictions.get('workers')

        predicted_memory = memory_pred.predicted_value if memory_pred else 16
        predicted_workers = workers_pred.predicted_value if workers_pred else 5

        if target_records < self.GLUE_MAX_EFFICIENT_RECORDS:
            suggested_platform = "glue"
            recommendations.append("Glue is optimal for this data volume")

            if target_records < 50000:
                recommendations.append("Consider using Glue 4.0 with flex execution for cost savings")

        elif target_records < self.EMR_MAX_EFFICIENT_RECORDS:
            if predicted_memory > 32 or predicted_workers > 15:
                suggested_platform = "emr"
                platform_change = True
                recommendations.append("EMR recommended for better resource flexibility")
                recommendations.append("Use EMR Serverless for variable workloads")
            else:
                suggested_platform = "glue"
                recommendations.append("Glue can handle this with increased workers")
                recommendations.append(f"Set NumberOfWorkers to {predicted_workers}")

        else:
            suggested_platform = "eks"
            platform_change = True
            recommendations.append("EKS with Spark Operator recommended for this scale")
            recommendations.append("Consider Karpenter for auto-scaling")
            recommendations.append("Use spot instances for cost optimization")

        # Add specific Glue recommendations
        if suggested_platform == "glue":
            if predicted_workers > 10:
                recommendations.append("Use G.2X or G.4X worker types for more resources")
            if target_records > 200000:
                recommendations.append("Enable job bookmarking for incremental processing")

        # Add specific EMR recommendations
        if suggested_platform == "emr":
            recommendations.append("Use EMR 6.x with Spark 3.x for best performance")
            if predicted_memory > 64:
                recommendations.append("Use r5 or r6g memory-optimized instances")

        return PredictionResult(
            metric='platform',
            current_value=0,  # N/A for platform
            predicted_value=0,  # N/A for platform
            scale_factor=0,
            confidence=0.90,
            recommendations=recommendations,
            platform_change_suggested=platform_change,
            suggested_platform=suggested_platform
        )

    def _find_nearest_cost(self, history: List[ExecutionHistory], records: int) -> float:
        """Find cost from nearest historical data point."""
        if not history:
            return 0.50

        # Linear interpolation between two nearest points
        for i, h in enumerate(history):
            if h.records_processed >= records:
                if i == 0:
                    return h.cost_usd
                prev = history[i-1]
                ratio = (records - prev.records_processed) / (h.records_processed - prev.records_processed)
                return prev.cost_usd + (h.cost_usd - prev.cost_usd) * ratio

        return history[-1].cost_usd

    def _find_nearest_duration(self, history: List[ExecutionHistory], records: int) -> float:
        for i, h in enumerate(history):
            if h.records_processed >= records:
                if i == 0:
                    return h.duration_seconds
                prev = history[i-1]
                ratio = (records - prev.records_processed) / (h.records_processed - prev.records_processed)
                return prev.duration_seconds + (h.duration_seconds - prev.duration_seconds) * ratio
        return history[-1].duration_seconds

    def _find_nearest_memory(self, history: List[ExecutionHistory], records: int) -> float:
        for i, h in enumerate(history):
            if h.records_processed >= records:
                if i == 0:
                    return h.memory_gb
                prev = history[i-1]
                ratio = (records - prev.records_processed) / (h.records_processed - prev.records_processed)
                return prev.memory_gb + (h.memory_gb - prev.memory_gb) * ratio
        return history[-1].memory_gb

    def _find_nearest_workers(self, history: List[ExecutionHistory], records: int) -> int:
        for i, h in enumerate(history):
            if h.records_processed >= records:
                return h.workers
        return history[-1].workers if history else 5

    def _round_to_memory_tier(self, memory_gb: float) -> float:
        """Round to standard memory tiers."""
        tiers = [4, 8, 16, 32, 64, 128, 256, 512]
        for tier in tiers:
            if memory_gb <= tier:
                return float(tier)
        return 512.0

    def _estimate_cost(self, baseline: Dict, target_records: int, scale: float) -> PredictionResult:
        """Estimate cost when no historical data available."""
        # Use baseline or default values
        base_cost = baseline.get('avg_cost', 0.50)
        base_records = baseline.get('avg_records', 100000)

        # Scale cost (sub-linear scaling)
        actual_scale = target_records / base_records if base_records > 0 else scale
        scale_efficiency = 0.85 if actual_scale > 1 else 1.0
        predicted_cost = base_cost * (actual_scale ** scale_efficiency)

        recommendations = []
        if predicted_cost > 5.0:
            recommendations.append("Consider reserved capacity for cost savings")

        return PredictionResult(
            metric='cost_usd',
            current_value=base_cost,
            predicted_value=round(predicted_cost, 2),
            scale_factor=scale,
            confidence=0.60,
            recommendations=recommendations
        )

    def _estimate_duration(self, baseline: Dict, target_records: int, scale: float) -> PredictionResult:
        """Estimate duration when no historical data available."""
        base_duration = baseline.get('avg_duration', 300)  # 5 min default
        base_records = baseline.get('avg_records', 100000)

        actual_scale = target_records / base_records if base_records > 0 else scale
        # Duration scales mostly linearly but with some efficiency gains
        parallelism_factor = 0.9 if actual_scale > 1 else 1.0
        predicted_duration = base_duration * actual_scale * parallelism_factor

        recommendations = []
        if predicted_duration > 3600:
            recommendations.append("Consider partitioning data for parallel processing")

        return PredictionResult(
            metric='duration_seconds',
            current_value=base_duration,
            predicted_value=round(predicted_duration, 0),
            scale_factor=scale,
            confidence=0.55,
            recommendations=recommendations
        )

    def _estimate_memory(self, baseline: Dict, target_records: int, scale: float) -> PredictionResult:
        """Estimate memory when no historical data available."""
        base_memory = baseline.get('avg_memory_gb', 8.0)
        base_records = baseline.get('avg_records', 100000)

        actual_scale = target_records / base_records if base_records > 0 else scale
        # Memory scales with sqrt of records
        memory_scale = math.sqrt(actual_scale) if actual_scale > 1 else actual_scale
        predicted_memory = self._round_to_memory_tier(base_memory * memory_scale)

        recommendations = []
        if predicted_memory > 32:
            recommendations.append("Use memory-optimized instance types")

        return PredictionResult(
            metric='memory_gb',
            current_value=base_memory,
            predicted_value=predicted_memory,
            scale_factor=scale,
            confidence=0.50,
            recommendations=recommendations
        )

    def _estimate_workers(self, baseline: Dict, target_records: int, scale: float) -> PredictionResult:
        """Estimate workers when no historical data available."""
        base_workers = baseline.get('typical_workers', 5)
        base_records = baseline.get('avg_records', 100000)

        actual_scale = target_records / base_records if base_records > 0 else scale
        # Workers scale with sqrt of records
        worker_scale = math.sqrt(actual_scale) if actual_scale > 1 else 1
        predicted_workers = max(2, int(base_workers * worker_scale))
        predicted_workers = min(predicted_workers, 100)

        recommendations = []
        if predicted_workers > 20:
            recommendations.append("Consider using dynamic allocation")

        return PredictionResult(
            metric='workers',
            current_value=base_workers,
            predicted_value=predicted_workers,
            scale_factor=scale,
            confidence=0.55,
            recommendations=recommendations
        )


class InteractiveAgentCLI:
    """
    Interactive CLI for querying agents with prediction capabilities.
    """

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.data_store = AgentDataStore()
        self.prediction_engine = PredictionEngine(self.data_store)

        # Initialize agents (lazy loading)
        self._agents = {}

        # Command history for interactive mode
        self.history = []

        # Historical compliance data (simulated from last runs)
        self.compliance_history = self._load_compliance_history()

        # Historical learning data (simulated from last runs)
        self.learning_history = self._load_learning_history()

        # Available commands
        self.commands = {
            'help': self.cmd_help,
            'predict': self.cmd_predict,
            'scale': self.cmd_scale,
            'analyze': self.cmd_analyze,
            'quality': self.cmd_quality,
            'compliance': self.cmd_compliance,
            'workload': self.cmd_workload,
            'recommend': self.cmd_recommend,
            'history': self.cmd_history,
            'baseline': self.cmd_baseline,
            'platform': self.cmd_platform,
            'cost': self.cmd_cost,
            'memory': self.cmd_memory,
            'learning': self.cmd_learning,
            'trend': self.cmd_trend,
            'anomaly': self.cmd_anomaly,
            'ask': self.cmd_ask,
            'exit': self.cmd_exit,
            'quit': self.cmd_exit,
        }

    def _load_compliance_history(self) -> Dict:
        """Load simulated compliance history from last runs."""
        return {
            'last_run': {
                'timestamp': '2024-02-14 15:30:00',
                'job_name': 'demo_complex_sales_analytics',
                'overall_status': 'COMPLIANT',
                'frameworks': {
                    'GDPR': {
                        'status': 'COMPLIANT',
                        'checks_passed': 7,
                        'checks_failed': 0,
                        'warnings': 1,
                        'findings': [
                            {'check': 'PII Masking', 'status': 'WARNING',
                             'detail': 'customer_name partially masked, consider full masking'}
                        ],
                        'pii_columns_detected': ['customer_email', 'customer_name', 'phone', 'address'],
                        'pii_columns_masked': ['customer_email', 'phone', 'credit_card_masked'],
                        'encryption_status': 'ENABLED',
                        'retention_policy': '365 days'
                    },
                    'PCI_DSS': {
                        'status': 'COMPLIANT',
                        'checks_passed': 6,
                        'checks_failed': 0,
                        'warnings': 0,
                        'findings': [],
                        'card_data_masked': True,
                        'encryption_status': 'ENABLED'
                    },
                    'SOX': {
                        'status': 'COMPLIANT',
                        'checks_passed': 5,
                        'checks_failed': 0,
                        'warnings': 0,
                        'findings': [],
                        'audit_trail': 'ENABLED',
                        'data_lineage': 'TRACKED'
                    }
                },
                'recommendations': [
                    'Apply full masking to customer_name field',
                    'Consider adding automated PII detection for new columns',
                    'Set up compliance drift alerts'
                ]
            },
            'history': [
                {'date': '2024-02-14', 'status': 'COMPLIANT', 'warnings': 1},
                {'date': '2024-02-13', 'status': 'COMPLIANT', 'warnings': 1},
                {'date': '2024-02-12', 'status': 'COMPLIANT', 'warnings': 2},
                {'date': '2024-02-11', 'status': 'COMPLIANT', 'warnings': 1},
                {'date': '2024-02-10', 'status': 'COMPLIANT', 'warnings': 1},
            ]
        }

    def _load_learning_history(self) -> Dict:
        """Load simulated learning history from last runs."""
        return {
            'job_name': 'demo_complex_sales_analytics',
            'baseline': {
                'avg_duration_seconds': 1544,
                'avg_cost': 1.08,
                'avg_records': 290000,
                'avg_memory_gb': 15.0,
                'typical_workers': 9,
                'success_rate': 98.5,
                'created_at': '2024-01-15',
                'updated_at': '2024-02-14',
                'sample_count': 45
            },
            'last_run': {
                'timestamp': '2024-02-14 15:30:00',
                'duration_seconds': 1710,
                'cost': 1.25,
                'records_processed': 312000,
                'memory_used_gb': 18.0,
                'workers_used': 10,
                'status': 'SUCCESS',
                'platform': 'glue',
                'deviations': {
                    'duration': '+10.7%',
                    'cost': '+15.7%',
                    'records': '+7.6%'
                },
                'anomalies_detected': 0
            },
            'trends': {
                'duration': {'direction': 'stable', 'change': '+2.3%', 'period': '7 days'},
                'cost': {'direction': 'increasing', 'change': '+5.1%', 'period': '7 days'},
                'records': {'direction': 'increasing', 'change': '+3.2%', 'period': '7 days'},
                'failure_rate': {'direction': 'stable', 'change': '0%', 'period': '7 days'}
            },
            'predictions': {
                'next_run_duration': 1580,
                'next_run_cost': 1.12,
                'failure_probability': 0.02,
                'recommended_workers': 10
            },
            'recent_runs': [
                {'date': '2024-02-14', 'duration': 1710, 'cost': 1.25, 'records': 312000, 'status': 'SUCCESS'},
                {'date': '2024-02-13', 'duration': 1620, 'cost': 1.18, 'records': 305000, 'status': 'SUCCESS'},
                {'date': '2024-02-12', 'duration': 1450, 'cost': 0.98, 'records': 278000, 'status': 'SUCCESS'},
                {'date': '2024-02-11', 'duration': 1580, 'cost': 1.12, 'records': 292000, 'status': 'SUCCESS'},
                {'date': '2024-02-10', 'duration': 1520, 'cost': 1.05, 'records': 285000, 'status': 'SUCCESS'},
            ],
            'anomalies_history': [
                {'date': '2024-02-05', 'type': 'DURATION', 'detail': 'Duration 45% above baseline', 'resolved': True},
                {'date': '2024-01-28', 'type': 'COST', 'detail': 'Cost spike due to data skew', 'resolved': True},
            ]
        }

    def get_agent(self, agent_type: str):
        """Get or create an agent instance."""
        if agent_type not in self._agents:
            if agent_type == 'workload':
                self._agents[agent_type] = WorkloadAssessmentAgent()
            elif agent_type == 'learning':
                self._agents[agent_type] = LearningAgent()
            elif agent_type == 'quality':
                self._agents[agent_type] = DataQualityAgent()
            elif agent_type == 'code':
                self._agents[agent_type] = CodeAnalysisAgent()
            elif agent_type == 'compliance':
                self._agents[agent_type] = ComplianceAgent()
            elif agent_type == 'recommendation':
                self._agents[agent_type] = RecommendationAgent()
        return self._agents.get(agent_type)

    def run_interactive(self):
        """Run interactive CLI session."""
        self._print_banner()

        print("\nType 'help' for available commands or 'quit' to exit.\n")

        while True:
            try:
                # Get user input
                user_input = input("\033[1;36m[agent-cli]\033[0m > ").strip()

                if not user_input:
                    continue

                # Add to history
                self.history.append(user_input)

                # Parse and execute command
                self._execute_command(user_input)

            except KeyboardInterrupt:
                print("\n\nUse 'quit' or 'exit' to exit.")
            except EOFError:
                break

        print("\nGoodbye!")

    def _print_banner(self):
        """Print CLI banner."""
        banner = """
╔══════════════════════════════════════════════════════════════════════╗
║                  ETL AGENT INTERACTIVE CLI                           ║
║                                                                      ║
║  Prediction & Scaling:                                               ║
║    scale <from> <to>                              Quick scale predict║
║    cost <job> --records <count>                   Cost estimation    ║
║    memory <job> --records <count>                 Memory prediction  ║
║    platform <records>                             Platform recommend ║
║                                                                      ║
║  Learning Agent (Historical Analysis):                               ║
║    learning [--last|--baseline|--predictions]     Learning insights  ║
║    trend [metric]                                 Trend analysis     ║
║    anomaly                                        Anomaly detection  ║
║    ask <question>                                 Natural language Q ║
║                                                                      ║
║  Compliance Agent:                                                   ║
║    compliance [GDPR|PCI_DSS|SOX] [--pii|--history]                   ║
║                                                                      ║
║  Other Agents:                                                       ║
║    analyze <script_path>                          Code analysis      ║
║    quality <table>                                Data quality check ║
║    workload                                       Workload assessment║
║    recommend                                      Get recommendations║
║                                                                      ║
║  Type 'help' for full command list                                   ║
╚══════════════════════════════════════════════════════════════════════╝
"""
        print(banner)

    def _execute_command(self, input_str: str):
        """Parse and execute a command."""
        parts = input_str.split()
        if not parts:
            return

        cmd = parts[0].lower()
        args = parts[1:]

        if cmd in self.commands:
            try:
                self.commands[cmd](args)
            except Exception as e:
                print(f"\033[0;31mError: {str(e)}\033[0m")
        else:
            # Check for natural language queries
            if any(word in input_str.lower() for word in ['cost', 'memory', 'scale', 'records', 'million']):
                self._handle_natural_query(input_str)
            else:
                print(f"Unknown command: {cmd}. Type 'help' for available commands.")

    def _handle_natural_query(self, query: str):
        """Handle natural language queries about predictions."""
        query_lower = query.lower()

        # Extract numbers from query
        import re
        numbers = re.findall(r'[\d,]+(?:k|m)?', query_lower)

        # Parse numbers (handle k/m suffixes)
        parsed_numbers = []
        for n in numbers:
            n_clean = n.replace(',', '')
            if n_clean.endswith('k'):
                parsed_numbers.append(int(float(n_clean[:-1]) * 1000))
            elif n_clean.endswith('m'):
                parsed_numbers.append(int(float(n_clean[:-1]) * 1000000))
            else:
                parsed_numbers.append(int(n_clean))

        # Default job name
        job_name = self.config.get('job_name', 'sales_analytics')

        if len(parsed_numbers) >= 2:
            # Scale prediction
            from_records = parsed_numbers[0]
            to_records = parsed_numbers[1]

            print(f"\n\033[1;33mPrediction: Scaling from {from_records:,} to {to_records:,} records\033[0m")
            print(f"Job: {job_name}")
            print("-" * 60)

            predictions = self.prediction_engine.predict_for_scale(
                job_name, from_records, to_records
            )

            self._display_predictions(predictions)

        elif len(parsed_numbers) == 1:
            # Single scale prediction from baseline
            to_records = parsed_numbers[0]
            baseline = self.data_store.baselines.get(job_name, {})
            from_records = baseline.get('avg_records', 100000)

            print(f"\n\033[1;33mPrediction: Scaling to {to_records:,} records\033[0m")
            print(f"Job: {job_name} (baseline: {from_records:,} records)")
            print("-" * 60)

            predictions = self.prediction_engine.predict_for_scale(
                job_name, from_records, to_records
            )

            self._display_predictions(predictions)
        else:
            print("Please specify record counts. Example: 'scale from 100000 to 1000000'")

    def _display_predictions(self, predictions: Dict[str, PredictionResult]):
        """Display prediction results in a formatted way."""

        # Cost
        if 'cost' in predictions:
            p = predictions['cost']
            print(f"\n\033[1;32m💰 COST:\033[0m")
            print(f"   Current:   ${p.current_value:.2f}")
            print(f"   Predicted: ${p.predicted_value:.2f}")
            print(f"   Change:    {((p.predicted_value / p.current_value) - 1) * 100:.1f}%" if p.current_value > 0 else "   Change:    N/A")
            print(f"   Confidence: {p.confidence * 100:.0f}%")
            if p.recommendations:
                print(f"   Tips:")
                for rec in p.recommendations:
                    print(f"     • {rec}")

        # Duration
        if 'duration' in predictions:
            p = predictions['duration']
            mins = p.predicted_value / 60
            current_mins = p.current_value / 60
            print(f"\n\033[1;32m⏱️  DURATION:\033[0m")
            print(f"   Current:   {current_mins:.1f} min")
            print(f"   Predicted: {mins:.1f} min")
            print(f"   Confidence: {p.confidence * 100:.0f}%")
            if p.recommendations:
                print(f"   Tips:")
                for rec in p.recommendations:
                    print(f"     • {rec}")

        # Memory
        if 'memory' in predictions:
            p = predictions['memory']
            print(f"\n\033[1;32m🧠 MEMORY:\033[0m")
            print(f"   Current:   {p.current_value:.0f} GB")
            print(f"   Predicted: {p.predicted_value:.0f} GB")
            print(f"   Confidence: {p.confidence * 100:.0f}%")
            if p.recommendations:
                print(f"   Tips:")
                for rec in p.recommendations:
                    print(f"     • {rec}")

        # Workers
        if 'workers' in predictions:
            p = predictions['workers']
            print(f"\n\033[1;32m👷 WORKERS:\033[0m")
            print(f"   Current:   {int(p.current_value)} workers")
            print(f"   Predicted: {int(p.predicted_value)} workers")
            if p.recommendations:
                print(f"   Tips:")
                for rec in p.recommendations:
                    print(f"     • {rec}")

        # Platform
        if 'platform' in predictions:
            p = predictions['platform']
            print(f"\n\033[1;32m🖥️  PLATFORM:\033[0m")
            if p.platform_change_suggested:
                print(f"   ⚠️  CHANGE RECOMMENDED: {p.suggested_platform.upper()}")
            else:
                print(f"   Current platform is optimal: {p.suggested_platform.upper()}")
            print(f"   Recommendations:")
            for rec in p.recommendations:
                print(f"     • {rec}")

        print()

    # Command implementations

    def cmd_help(self, args):
        """Show help."""
        help_text = """
Available Commands:
═══════════════════════════════════════════════════════════════════════

PREDICTION & SCALING:
  predict <job> --from <records> --to <records>
      Predict cost, memory, duration when scaling from X to Y records
      Example: predict sales_analytics --from 100000 --to 1000000

  scale <from> <to>
      Quick scale prediction using default job
      Example: scale 100k 1m

  cost <job> --records <count>
      Estimate cost for specific record count
      Example: cost sales_analytics --records 500000

  memory <job> --records <count>
      Estimate memory requirements
      Example: memory sales_analytics --records 1000000

  platform <records>
      Get platform recommendation for record count
      Example: platform 5000000

AGENTS:
  analyze <script_path>
      Run code analysis on a PySpark script

  quality <table_name>
      Check data quality for a table

  compliance [framework] [--last] [--pii] [--history]
      Run compliance check (GDPR, PCI_DSS, SOX)
      Example: compliance            (show last run status)
      Example: compliance GDPR       (show GDPR details)
      Example: compliance --pii      (show PII analysis)
      Example: compliance --history  (show compliance history)

  workload
      Run workload assessment

  recommend
      Get aggregated recommendations

LEARNING AGENT:
  learning [--last] [--baseline] [--predictions]
      Query learning agent based on historical runs
      Example: learning              (show all)
      Example: learning --last       (show last run analysis)
      Example: learning --baseline   (show baseline metrics)
      Example: learning --predictions (show predictions)

  trend [metric]
      Show trend analysis for metrics
      Example: trend                 (show all trends)
      Example: trend cost            (show cost trend)

  anomaly
      Show anomaly detection results

  ask <question>
      Natural language query to agents
      Example: ask why did cost increase?
      Example: ask is the job GDPR compliant?
      Example: ask what is the failure probability?
      Example: ask how much memory for 1 million records?

HISTORY & BASELINES:
  history
      Show execution history

  baseline <job_name>
      Show baseline metrics for a job

OTHER:
  help          Show this help
  exit/quit     Exit the CLI

NATURAL LANGUAGE:
  You can also ask natural language questions like:
  - "What will be the cost if I scale from 100k to 1m records?"
  - "How much memory do I need for 5 million records?"
  - "Should I switch to EMR for 2 million records?"
"""
        print(help_text)

    def cmd_predict(self, args):
        """Predict metrics for scaling."""
        if len(args) < 5:
            print("Usage: predict <job> --from <records> --to <records>")
            return

        job_name = args[0]

        # Parse arguments
        from_records = 100000
        to_records = 1000000

        for i, arg in enumerate(args):
            if arg == '--from' and i + 1 < len(args):
                from_records = self._parse_number(args[i + 1])
            elif arg == '--to' and i + 1 < len(args):
                to_records = self._parse_number(args[i + 1])

        print(f"\n\033[1;33mPrediction: {job_name}\033[0m")
        print(f"Scaling from {from_records:,} to {to_records:,} records")
        print("-" * 60)

        predictions = self.prediction_engine.predict_for_scale(
            job_name, from_records, to_records
        )

        self._display_predictions(predictions)

    def cmd_scale(self, args):
        """Quick scale prediction."""
        if len(args) < 2:
            print("Usage: scale <from_records> <to_records>")
            return

        from_records = self._parse_number(args[0])
        to_records = self._parse_number(args[1])
        job_name = self.config.get('job_name', 'sales_analytics')

        print(f"\n\033[1;33mScale Prediction: {job_name}\033[0m")
        print(f"From {from_records:,} to {to_records:,} records")
        print("-" * 60)

        predictions = self.prediction_engine.predict_for_scale(
            job_name, from_records, to_records
        )

        self._display_predictions(predictions)

    def cmd_cost(self, args):
        """Cost estimation."""
        job_name = args[0] if args else self.config.get('job_name', 'sales_analytics')
        records = 100000

        for i, arg in enumerate(args):
            if arg == '--records' and i + 1 < len(args):
                records = self._parse_number(args[i + 1])

        baseline = self.data_store.baselines.get(job_name, {})
        from_records = baseline.get('avg_records', 100000)

        predictions = self.prediction_engine.predict_for_scale(
            job_name, from_records, records
        )

        if 'cost' in predictions:
            p = predictions['cost']
            print(f"\n\033[1;32mCost Estimation: {job_name}\033[0m")
            print(f"Records: {records:,}")
            print(f"Estimated Cost: ${p.predicted_value:.2f}")
            print(f"Confidence: {p.confidence * 100:.0f}%")
            if p.recommendations:
                print("Recommendations:")
                for rec in p.recommendations:
                    print(f"  • {rec}")

    def cmd_memory(self, args):
        """Memory estimation."""
        job_name = args[0] if args else self.config.get('job_name', 'sales_analytics')
        records = 100000

        for i, arg in enumerate(args):
            if arg == '--records' and i + 1 < len(args):
                records = self._parse_number(args[i + 1])

        baseline = self.data_store.baselines.get(job_name, {})
        from_records = baseline.get('avg_records', 100000)

        predictions = self.prediction_engine.predict_for_scale(
            job_name, from_records, records
        )

        if 'memory' in predictions:
            p = predictions['memory']
            print(f"\n\033[1;32mMemory Estimation: {job_name}\033[0m")
            print(f"Records: {records:,}")
            print(f"Required Memory: {p.predicted_value:.0f} GB")
            print(f"Confidence: {p.confidence * 100:.0f}%")
            if p.recommendations:
                print("Recommendations:")
                for rec in p.recommendations:
                    print(f"  • {rec}")

    def cmd_platform(self, args):
        """Platform recommendation."""
        records = self._parse_number(args[0]) if args else 1000000
        job_name = self.config.get('job_name', 'sales_analytics')

        baseline = self.data_store.baselines.get(job_name, {})
        from_records = baseline.get('avg_records', 100000)

        predictions = self.prediction_engine.predict_for_scale(
            job_name, from_records, records
        )

        if 'platform' in predictions:
            p = predictions['platform']
            print(f"\n\033[1;32mPlatform Recommendation\033[0m")
            print(f"Records: {records:,}")
            print(f"Recommended Platform: {p.suggested_platform.upper()}")
            if p.platform_change_suggested:
                print(f"⚠️  Platform change is recommended!")
            print("\nRecommendations:")
            for rec in p.recommendations:
                print(f"  • {rec}")

    def cmd_analyze(self, args):
        """Run code analysis."""
        if not args:
            print("Usage: analyze <script_path>")
            return

        script_path = args[0]

        if not os.path.exists(script_path):
            print(f"Script not found: {script_path}")
            return

        print(f"\n\033[1;33mAnalyzing: {script_path}\033[0m")
        print("-" * 60)

        agent = self.get_agent('code')
        if agent:
            with open(script_path, 'r') as f:
                code = f.read()

            from framework.agents.code_analysis_agent import CodeContext
            context = CodeContext(
                source_code=code,
                language="pyspark",
                file_path=script_path
            )

            result = agent.analyze(context)

            print(f"\nAnti-patterns Found: {result.anti_patterns_found}")
            print(f"Overall Score: {result.overall_score:.0f}/100")

            if result.issues:
                print("\nIssues:")
                for issue in result.issues[:5]:
                    print(f"  [{issue.severity}] {issue.pattern}: {issue.description}")

            if result.recommendations:
                print("\nRecommendations:")
                for rec in result.recommendations[:5]:
                    print(f"  • {rec.message}")

    def cmd_quality(self, args):
        """Run data quality check."""
        table_name = args[0] if args else "sample_table"

        print(f"\n\033[1;33mData Quality Check: {table_name}\033[0m")
        print("-" * 60)

        # Simulated quality check results
        print("\nChecks Performed: 10")
        print("Passed: 8")
        print("Failed: 2")
        print("\nFailed Checks:")
        print("  • null_check on customer_id: 0.5% null values")
        print("  • range_check on amount: 3 values outside range")

    def cmd_compliance(self, args):
        """Run compliance check based on historical data."""
        # Parse arguments
        framework = None
        show_last = False
        show_pii = False
        show_history = False

        for i, arg in enumerate(args):
            if arg in ('--last', '-l'):
                show_last = True
            elif arg in ('--pii', '-p'):
                show_pii = True
            elif arg in ('--history', '-h'):
                show_history = True
            elif not arg.startswith('-'):
                framework = arg.upper()

        last_run = self.compliance_history.get('last_run', {})

        if show_last or (not framework and not show_pii and not show_history):
            # Show last run summary
            print(f"\n\033[1;33m📋 COMPLIANCE STATUS (Last Run)\033[0m")
            print("-" * 60)
            print(f"  Job: {last_run.get('job_name', 'unknown')}")
            print(f"  Timestamp: {last_run.get('timestamp', 'unknown')}")

            status = last_run.get('overall_status', 'UNKNOWN')
            status_color = "\033[0;32m" if status == 'COMPLIANT' else "\033[0;31m"
            print(f"  Overall Status: {status_color}{status}\033[0m")

            print(f"\n  Frameworks Checked:")
            for fw_name, fw_data in last_run.get('frameworks', {}).items():
                fw_status = fw_data.get('status', 'UNKNOWN')
                fw_color = "\033[0;32m" if fw_status == 'COMPLIANT' else "\033[0;31m"
                warnings = fw_data.get('warnings', 0)
                warn_str = f" ({warnings} warnings)" if warnings > 0 else ""
                print(f"    • {fw_name}: {fw_color}{fw_status}{warn_str}\033[0m")

        if framework:
            # Show specific framework details
            fw_data = last_run.get('frameworks', {}).get(framework, {})
            if not fw_data:
                print(f"\n\033[0;31mFramework '{framework}' not found in last run.\033[0m")
                print(f"Available: {list(last_run.get('frameworks', {}).keys())}")
                return

            print(f"\n\033[1;33m🔒 {framework} COMPLIANCE DETAILS\033[0m")
            print("-" * 60)

            status = fw_data.get('status', 'UNKNOWN')
            status_color = "\033[0;32m" if status == 'COMPLIANT' else "\033[0;31m"
            print(f"  Status: {status_color}{status}\033[0m")
            print(f"  Checks Passed: {fw_data.get('checks_passed', 0)}")
            print(f"  Checks Failed: {fw_data.get('checks_failed', 0)}")
            print(f"  Warnings: {fw_data.get('warnings', 0)}")

            if framework == 'GDPR':
                print(f"\n  GDPR Specifics:")
                print(f"    Encryption: {fw_data.get('encryption_status', 'UNKNOWN')}")
                print(f"    Retention Policy: {fw_data.get('retention_policy', 'Not set')}")
                print(f"    PII Columns Detected: {len(fw_data.get('pii_columns_detected', []))}")
                print(f"    PII Columns Masked: {len(fw_data.get('pii_columns_masked', []))}")

            elif framework == 'PCI_DSS':
                print(f"\n  PCI-DSS Specifics:")
                print(f"    Card Data Masked: {'Yes' if fw_data.get('card_data_masked') else 'No'}")
                print(f"    Encryption: {fw_data.get('encryption_status', 'UNKNOWN')}")

            elif framework == 'SOX':
                print(f"\n  SOX Specifics:")
                print(f"    Audit Trail: {fw_data.get('audit_trail', 'UNKNOWN')}")
                print(f"    Data Lineage: {fw_data.get('data_lineage', 'UNKNOWN')}")

            if fw_data.get('findings'):
                print(f"\n  Findings:")
                for finding in fw_data['findings']:
                    status_icon = "⚠️" if finding['status'] == 'WARNING' else "❌"
                    print(f"    {status_icon} [{finding['status']}] {finding['check']}")
                    print(f"       {finding['detail']}")

        if show_pii:
            # Show PII details
            gdpr_data = last_run.get('frameworks', {}).get('GDPR', {})
            print(f"\n\033[1;33m🔐 PII ANALYSIS\033[0m")
            print("-" * 60)

            detected = gdpr_data.get('pii_columns_detected', [])
            masked = gdpr_data.get('pii_columns_masked', [])
            unmasked = [col for col in detected if col not in masked]

            print(f"\n  PII Columns Detected ({len(detected)}):")
            for col in detected:
                mask_status = "✓ Masked" if col in masked else "⚠️ NOT Masked"
                print(f"    • {col}: {mask_status}")

            if unmasked:
                print(f"\n  \033[0;33m⚠️ Action Required: {len(unmasked)} columns need masking\033[0m")
                for col in unmasked:
                    print(f"    • {col}")

        if show_history:
            # Show compliance history
            print(f"\n\033[1;33m📊 COMPLIANCE HISTORY\033[0m")
            print("-" * 60)
            print(f"\n  {'Date':<12} {'Status':<12} {'Warnings'}")
            print(f"  {'-'*12} {'-'*12} {'-'*10}")
            for h in self.compliance_history.get('history', []):
                status_color = "\033[0;32m" if h['status'] == 'COMPLIANT' else "\033[0;31m"
                print(f"  {h['date']:<12} {status_color}{h['status']:<12}\033[0m {h['warnings']}")

        # Always show recommendations
        if last_run.get('recommendations'):
            print(f"\n  \033[1;36mRecommendations:\033[0m")
            for rec in last_run['recommendations']:
                print(f"    • {rec}")

    def cmd_workload(self, args):
        """Run workload assessment."""
        job_name = self.config.get('job_name', 'sales_analytics')

        print(f"\n\033[1;33mWorkload Assessment: {job_name}\033[0m")
        print("-" * 60)

        baseline = self.data_store.baselines.get(job_name, {})

        print(f"\nCurrent Configuration:")
        print(f"  Workers: {baseline.get('typical_workers', 5)}")
        print(f"  Memory: {baseline.get('avg_memory_gb', 16)} GB")
        print(f"  Avg Duration: {baseline.get('avg_duration', 0) / 60:.1f} min")
        print(f"  Avg Cost: ${baseline.get('avg_cost', 0):.2f}")

        print(f"\nRecommendations:")
        print("  • Current configuration is optimal for workload")
        print("  • Consider enabling adaptive query execution")

    def cmd_recommend(self, args):
        """Get aggregated recommendations."""
        print(f"\n\033[1;33mAggregated Recommendations\033[0m")
        print("-" * 60)

        print("\nHigh Priority:")
        print("  • Enable broadcast join for small dimension tables")
        print("  • Increase executor memory to 8GB for large aggregations")

        print("\nMedium Priority:")
        print("  • Consider partitioning output by date")
        print("  • Enable adaptive query execution")

        print("\nLow Priority:")
        print("  • Consider caching frequently accessed data")

    def cmd_history(self, args):
        """Show execution history."""
        print(f"\n\033[1;33mExecution History\033[0m")
        print("-" * 60)

        for h in self.data_store.execution_history:
            status_color = "\033[0;32m" if h.status == "SUCCEEDED" else "\033[0;31m"
            print(f"\n{h.job_name}:")
            print(f"  Records: {h.records_processed:,}")
            print(f"  Duration: {h.duration_seconds / 60:.1f} min")
            print(f"  Cost: ${h.cost_usd:.2f}")
            print(f"  Platform: {h.platform}")
            print(f"  Status: {status_color}{h.status}\033[0m")

    def cmd_baseline(self, args):
        """Show baseline for a job."""
        job_name = args[0] if args else self.config.get('job_name', 'sales_analytics')

        baseline = self.data_store.baselines.get(job_name, {})

        if not baseline:
            print(f"No baseline found for: {job_name}")
            return

        print(f"\n\033[1;33mBaseline: {job_name}\033[0m")
        print("-" * 60)
        print(f"  Average Records: {baseline.get('avg_records', 0):,}")
        print(f"  Average Duration: {baseline.get('avg_duration', 0) / 60:.1f} min")
        print(f"  Average Cost: ${baseline.get('avg_cost', 0):.2f}")
        print(f"  Average Memory: {baseline.get('avg_memory_gb', 0)} GB")
        print(f"  Typical Workers: {baseline.get('typical_workers', 0)}")

    def cmd_learning(self, args):
        """Query the learning agent based on historical runs."""
        show_last = False
        show_baseline = False
        show_predictions = False
        show_all = len(args) == 0

        for arg in args:
            if arg in ('--last', '-l'):
                show_last = True
            elif arg in ('--baseline', '-b'):
                show_baseline = True
            elif arg in ('--predictions', '-p'):
                show_predictions = True

        if show_all:
            show_last = show_baseline = show_predictions = True

        lh = self.learning_history

        print(f"\n\033[1;33m📚 LEARNING AGENT - {lh.get('job_name', 'unknown')}\033[0m")
        print("=" * 60)

        if show_last:
            last = lh.get('last_run', {})
            print(f"\n\033[1;36m📊 Last Run Analysis\033[0m")
            print("-" * 50)
            print(f"  Timestamp: {last.get('timestamp', 'unknown')}")
            print(f"  Status: \033[0;32m{last.get('status', 'unknown')}\033[0m")
            print(f"  Platform: {last.get('platform', 'unknown')}")
            print(f"\n  Metrics:")
            print(f"    Duration: {last.get('duration_seconds', 0) / 60:.1f} min")
            print(f"    Cost: ${last.get('cost', 0):.2f}")
            print(f"    Records: {last.get('records_processed', 0):,}")
            print(f"    Memory Used: {last.get('memory_used_gb', 0):.1f} GB")
            print(f"    Workers: {last.get('workers_used', 0)}")

            deviations = last.get('deviations', {})
            if deviations:
                print(f"\n  Deviations from Baseline:")
                for metric, value in deviations.items():
                    color = "\033[0;33m" if value.startswith('+') and float(value.rstrip('%')) > 10 else "\033[0;32m"
                    print(f"    {metric.capitalize()}: {color}{value}\033[0m")

            anomalies = last.get('anomalies_detected', 0)
            if anomalies > 0:
                print(f"\n  \033[0;31m⚠️ Anomalies Detected: {anomalies}\033[0m")
            else:
                print(f"\n  \033[0;32m✓ No anomalies detected\033[0m")

        if show_baseline:
            baseline = lh.get('baseline', {})
            print(f"\n\033[1;36m📏 Baseline Metrics\033[0m")
            print("-" * 50)
            print(f"  Sample Count: {baseline.get('sample_count', 0)} runs")
            print(f"  Created: {baseline.get('created_at', 'unknown')}")
            print(f"  Last Updated: {baseline.get('updated_at', 'unknown')}")
            print(f"\n  Averages:")
            print(f"    Duration: {baseline.get('avg_duration_seconds', 0) / 60:.1f} min")
            print(f"    Cost: ${baseline.get('avg_cost', 0):.2f}")
            print(f"    Records: {baseline.get('avg_records', 0):,}")
            print(f"    Memory: {baseline.get('avg_memory_gb', 0):.1f} GB")
            print(f"    Workers: {baseline.get('typical_workers', 0)}")
            print(f"    Success Rate: {baseline.get('success_rate', 0):.1f}%")

        if show_predictions:
            predictions = lh.get('predictions', {})
            print(f"\n\033[1;36m🔮 Predictions (Next Run)\033[0m")
            print("-" * 50)
            print(f"  Expected Duration: {predictions.get('next_run_duration', 0) / 60:.1f} min")
            print(f"  Expected Cost: ${predictions.get('next_run_cost', 0):.2f}")
            print(f"  Recommended Workers: {predictions.get('recommended_workers', 0)}")
            fail_prob = predictions.get('failure_probability', 0) * 100
            fail_color = "\033[0;32m" if fail_prob < 5 else "\033[0;33m" if fail_prob < 15 else "\033[0;31m"
            print(f"  Failure Probability: {fail_color}{fail_prob:.1f}%\033[0m")

    def cmd_trend(self, args):
        """Show trends from learning agent."""
        metric = args[0].lower() if args else None

        trends = self.learning_history.get('trends', {})

        print(f"\n\033[1;33m📈 TREND ANALYSIS\033[0m")
        print("-" * 60)

        if metric and metric in trends:
            # Show specific metric trend
            t = trends[metric]
            direction_icon = "📈" if t['direction'] == 'increasing' else "📉" if t['direction'] == 'decreasing' else "➡️"
            print(f"\n  {metric.upper()} Trend:")
            print(f"    Direction: {direction_icon} {t['direction'].capitalize()}")
            print(f"    Change: {t['change']}")
            print(f"    Period: {t['period']}")
        else:
            # Show all trends
            for metric_name, t in trends.items():
                direction_icon = "📈" if t['direction'] == 'increasing' else "📉" if t['direction'] == 'decreasing' else "➡️"
                direction_color = "\033[0;33m" if t['direction'] == 'increasing' else "\033[0;32m"
                print(f"\n  {metric_name.upper()}:")
                print(f"    {direction_icon} {direction_color}{t['direction'].capitalize()}\033[0m ({t['change']} over {t['period']})")

        # Show recent runs for context
        print(f"\n  Recent Execution Data:")
        print(f"  {'Date':<12} {'Duration':<12} {'Cost':<10} {'Records':<12}")
        print(f"  {'-'*12} {'-'*12} {'-'*10} {'-'*12}")
        for run in self.learning_history.get('recent_runs', [])[:5]:
            print(f"  {run['date']:<12} {run['duration']/60:.1f} min      ${run['cost']:<8.2f} {run['records']:,}")

    def cmd_anomaly(self, args):
        """Show anomaly detection results."""
        print(f"\n\033[1;33m🚨 ANOMALY DETECTION\033[0m")
        print("-" * 60)

        last_run = self.learning_history.get('last_run', {})
        anomalies_detected = last_run.get('anomalies_detected', 0)

        if anomalies_detected == 0:
            print(f"\n  \033[0;32m✓ Last run: No anomalies detected\033[0m")
            print(f"    All metrics within expected thresholds (±20%)")
        else:
            print(f"\n  \033[0;31m⚠️ Last run: {anomalies_detected} anomalies detected\033[0m")

        # Show deviations
        deviations = last_run.get('deviations', {})
        if deviations:
            print(f"\n  Metric Deviations:")
            for metric, value in deviations.items():
                pct = float(value.rstrip('%'))
                status = "⚠️ ANOMALY" if abs(pct) > 20 else "✓ Normal"
                color = "\033[0;31m" if abs(pct) > 20 else "\033[0;32m"
                print(f"    {metric.capitalize()}: {value} {color}({status})\033[0m")

        # Show historical anomalies
        anomaly_history = self.learning_history.get('anomalies_history', [])
        if anomaly_history:
            print(f"\n  Historical Anomalies (Last 30 days):")
            for a in anomaly_history:
                resolved = "✓ Resolved" if a.get('resolved') else "⚠️ Open"
                print(f"    [{a['date']}] {a['type']}: {a['detail']} - {resolved}")

    def cmd_ask(self, args):
        """Natural language query to agents."""
        if not args:
            print("Usage: ask <question>")
            print("Examples:")
            print("  ask why did cost increase?")
            print("  ask is the job compliant with GDPR?")
            print("  ask what is the failure probability?")
            print("  ask how much memory for 1 million records?")
            return

        question = ' '.join(args).lower()

        print(f"\n\033[1;33m🤖 Agent Response\033[0m")
        print("-" * 60)

        # Pattern matching for common questions
        if any(word in question for word in ['cost', 'expensive', 'price', 'money']):
            if 'increase' in question or 'why' in question or 'high' in question:
                # Cost analysis
                last = self.learning_history.get('last_run', {})
                baseline = self.learning_history.get('baseline', {})
                deviation = last.get('deviations', {}).get('cost', '+0%')

                print(f"\n  📊 Cost Analysis:")
                print(f"  Last run cost: ${last.get('cost', 0):.2f}")
                print(f"  Baseline cost: ${baseline.get('avg_cost', 0):.2f}")
                print(f"  Deviation: {deviation}")

                print(f"\n  Possible Reasons:")
                if float(deviation.rstrip('%')) > 10:
                    print(f"    • Data volume increased by {last.get('deviations', {}).get('records', '0%')}")
                    print(f"    • More workers used ({last.get('workers_used', 0)} vs baseline {baseline.get('typical_workers', 0)})")
                    print(f"    • Longer execution time due to data complexity")
                else:
                    print(f"    • Cost is within normal variance")

            elif any(word in question for word in ['million', 'records', 'scale']):
                # Cost prediction
                import re
                numbers = re.findall(r'[\d,]+(?:k|m)?', question)
                if numbers:
                    records = self._parse_number(numbers[0])
                    self.cmd_cost(['sales_analytics', '--records', str(records)])
                    return

        elif any(word in question for word in ['memory', 'ram', 'gb']):
            if any(word in question for word in ['million', 'records', 'scale', 'need']):
                import re
                numbers = re.findall(r'[\d,]+(?:k|m)?', question)
                if numbers:
                    records = self._parse_number(numbers[0])
                    self.cmd_memory(['sales_analytics', '--records', str(records)])
                    return

            # Memory analysis
            last = self.learning_history.get('last_run', {})
            baseline = self.learning_history.get('baseline', {})
            print(f"\n  🧠 Memory Analysis:")
            print(f"  Last run: {last.get('memory_used_gb', 0):.1f} GB")
            print(f"  Baseline: {baseline.get('avg_memory_gb', 0):.1f} GB")
            print(f"\n  Memory scales with √(records). For 10x data, expect ~3.2x memory.")

        elif any(word in question for word in ['compliant', 'compliance', 'gdpr', 'pci', 'sox', 'hipaa']):
            # Compliance query
            last_compliance = self.compliance_history.get('last_run', {})
            status = last_compliance.get('overall_status', 'UNKNOWN')

            print(f"\n  🔒 Compliance Status:")
            status_color = "\033[0;32m" if status == 'COMPLIANT' else "\033[0;31m"
            print(f"  Overall: {status_color}{status}\033[0m")

            if 'gdpr' in question:
                gdpr = last_compliance.get('frameworks', {}).get('GDPR', {})
                print(f"\n  GDPR Specifics:")
                print(f"    Status: {gdpr.get('status', 'UNKNOWN')}")
                print(f"    PII Detected: {len(gdpr.get('pii_columns_detected', []))} columns")
                print(f"    PII Masked: {len(gdpr.get('pii_columns_masked', []))} columns")
            elif 'pci' in question:
                pci = last_compliance.get('frameworks', {}).get('PCI_DSS', {})
                print(f"\n  PCI-DSS Specifics:")
                print(f"    Status: {pci.get('status', 'UNKNOWN')}")
                print(f"    Card Data Masked: {'Yes' if pci.get('card_data_masked') else 'No'}")

        elif any(word in question for word in ['failure', 'fail', 'probability', 'risk']):
            predictions = self.learning_history.get('predictions', {})
            fail_prob = predictions.get('failure_probability', 0) * 100

            print(f"\n  ⚠️ Failure Risk Analysis:")
            fail_color = "\033[0;32m" if fail_prob < 5 else "\033[0;33m" if fail_prob < 15 else "\033[0;31m"
            print(f"  Failure Probability: {fail_color}{fail_prob:.1f}%\033[0m")

            print(f"\n  Risk Factors:")
            if fail_prob < 5:
                print(f"    ✓ Low risk - job has been stable")
            else:
                print(f"    • Recent trend shows increasing duration")
                print(f"    • Consider monitoring resource usage")

            print(f"\n  Based on {self.learning_history.get('baseline', {}).get('sample_count', 0)} historical runs")

        elif any(word in question for word in ['trend', 'pattern', 'direction']):
            self.cmd_trend([])

        elif any(word in question for word in ['anomaly', 'anomalies', 'unusual', 'strange']):
            self.cmd_anomaly([])

        elif any(word in question for word in ['baseline', 'average', 'normal']):
            self.cmd_learning(['--baseline'])

        elif any(word in question for word in ['predict', 'next', 'expect', 'future']):
            self.cmd_learning(['--predictions'])

        else:
            print(f"\n  I can help you with questions about:")
            print(f"    • Cost analysis (e.g., 'why did cost increase?')")
            print(f"    • Memory requirements (e.g., 'how much memory for 1m records?')")
            print(f"    • Compliance status (e.g., 'is the job GDPR compliant?')")
            print(f"    • Failure probability (e.g., 'what is the failure risk?')")
            print(f"    • Trends (e.g., 'show me trends')")
            print(f"    • Anomalies (e.g., 'any anomalies detected?')")
            print(f"\n  Try: 'ask why did cost increase?'")

    def cmd_exit(self, args):
        """Exit the CLI."""
        print("\nExiting...")
        sys.exit(0)

    def _parse_number(self, s: str) -> int:
        """Parse a number string with k/m suffixes."""
        s = s.lower().replace(',', '')
        if s.endswith('k'):
            return int(float(s[:-1]) * 1000)
        elif s.endswith('m'):
            return int(float(s[:-1]) * 1000000)
        return int(float(s))


def main():
    parser = argparse.ArgumentParser(description='Interactive Agent CLI')
    parser.add_argument('--config', '-c', help='Path to config JSON')
    parser.add_argument('--agent', help='Specific agent to interact with')
    parser.add_argument('--predict-cost', action='store_true', help='Predict cost')
    parser.add_argument('--predict-memory', action='store_true', help='Predict memory')
    parser.add_argument('--records', type=int, help='Target record count')
    parser.add_argument('--from-records', type=int, help='Source record count')

    args = parser.parse_args()

    # Load config if provided
    config = {}
    if args.config and os.path.exists(args.config):
        with open(args.config, 'r') as f:
            config = json.load(f)

    cli = InteractiveAgentCLI(config)

    # Handle non-interactive modes
    if args.predict_cost or args.predict_memory:
        job_name = config.get('job_name', 'sales_analytics')
        from_records = args.from_records or 100000
        to_records = args.records or 1000000

        predictions = cli.prediction_engine.predict_for_scale(
            job_name, from_records, to_records
        )

        if args.predict_cost and 'cost' in predictions:
            p = predictions['cost']
            print(f"Predicted cost for {to_records:,} records: ${p.predicted_value:.2f}")

        if args.predict_memory and 'memory' in predictions:
            p = predictions['memory']
            print(f"Predicted memory for {to_records:,} records: {p.predicted_value:.0f} GB")

        return

    # Run interactive mode
    cli.run_interactive()


if __name__ == "__main__":
    main()
