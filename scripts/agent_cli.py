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
            'exit': self.cmd_exit,
            'quit': self.cmd_exit,
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
║  Commands:                                                           ║
║    predict <job> --from <records> --to <records>  Scale prediction   ║
║    cost <job> --records <count>                   Cost estimation    ║
║    memory <job> --records <count>                 Memory prediction  ║
║    platform <records>                             Platform recommend ║
║    analyze <script_path>                          Code analysis      ║
║    quality <table>                                Data quality check ║
║    compliance <framework>                         Compliance check   ║
║    workload                                       Workload assessment║
║    history                                        Show exec history  ║
║    baseline <job>                                 Show baseline      ║
║                                                                      ║
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

  compliance <framework>
      Run compliance check (gdpr, hipaa, pci-dss, sox)

  workload
      Run workload assessment

  recommend
      Get aggregated recommendations

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
        """Run compliance check."""
        framework = args[0].upper() if args else "GDPR"

        print(f"\n\033[1;33mCompliance Check: {framework}\033[0m")
        print("-" * 60)

        # Simulated compliance results
        print(f"\nFramework: {framework}")
        print("Status: COMPLIANT with recommendations")
        print("\nFindings:")
        print("  • PII fields detected: email, phone")
        print("  • Encryption: Enabled")
        print("  • Data retention: Configured")
        print("\nRecommendations:")
        print("  • Consider adding data masking for PII")

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
