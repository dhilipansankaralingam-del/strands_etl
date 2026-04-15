#!/usr/bin/env python3
"""
Learning Agent
==============

Intelligent agent that learns from historical executions to:
1. Build execution patterns and baselines
2. Predict job failures before they happen
3. Recommend optimal configurations based on past performance
4. Identify anomalies in execution metrics
5. Track improvement trends over time
6. Generate insights from execution history

Uses DynamoDB to store and retrieve historical data.
"""

import json
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import math


class TrendType(Enum):
    """Types of trends detected."""
    IMPROVING = "improving"
    DEGRADING = "degrading"
    STABLE = "stable"
    VOLATILE = "volatile"


class AnomalyType(Enum):
    """Types of anomalies detected."""
    DURATION_SPIKE = "duration_spike"
    MEMORY_SPIKE = "memory_spike"
    COST_SPIKE = "cost_spike"
    FAILURE_CLUSTER = "failure_cluster"
    DATA_VOLUME_SPIKE = "data_volume_spike"
    SHUFFLE_SPIKE = "shuffle_spike"


class PredictionConfidence(Enum):
    """Confidence levels for predictions."""
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass
class ExecutionBaseline:
    """Baseline metrics for a job."""
    job_name: str
    avg_duration_seconds: float = 0.0
    std_duration_seconds: float = 0.0
    avg_cost: float = 0.0
    avg_memory_utilization: float = 0.0
    avg_cpu_utilization: float = 0.0
    avg_input_bytes: int = 0
    avg_output_bytes: int = 0
    avg_shuffle_bytes: int = 0
    typical_worker_count: int = 0
    typical_worker_type: str = ""
    success_rate: float = 1.0
    last_updated: str = ""
    sample_count: int = 0


@dataclass
class Anomaly:
    """Detected anomaly in execution."""
    anomaly_type: AnomalyType
    timestamp: str
    job_name: str
    expected_value: float
    actual_value: float
    deviation_factor: float
    description: str
    severity: str  # low, medium, high


@dataclass
class FailurePrediction:
    """Prediction of potential failure."""
    job_name: str
    probability: float
    confidence: PredictionConfidence
    contributing_factors: List[str]
    recommended_actions: List[str]
    based_on_runs: int


@dataclass
class ExecutionInsight:
    """Insight derived from execution history."""
    category: str
    title: str
    description: str
    impact: str
    action: str
    data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LearningResult:
    """Result from learning agent analysis."""
    baseline: Optional[ExecutionBaseline] = None
    anomalies: List[Anomaly] = field(default_factory=list)
    failure_prediction: Optional[FailurePrediction] = None
    insights: List[ExecutionInsight] = field(default_factory=list)
    trends: Dict[str, TrendType] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)


class LearningAgent:
    """
    Agent that learns from historical executions and provides predictions.
    """

    def __init__(self, config, dynamodb_client=None):
        self.config = config
        self.dynamodb = dynamodb_client
        self.history_table = config.history_table if hasattr(config, 'history_table') else "etl_execution_history"
        self.baseline_table = config.baseline_table if hasattr(config, 'baseline_table') else "etl_job_baselines"

        # Anomaly detection thresholds
        self.anomaly_threshold = 2.5  # Standard deviations
        self.failure_window_days = 7
        self.min_samples_for_baseline = 5

    def learn_from_execution(
        self,
        job_name: str,
        execution_metrics: Dict[str, Any],
        store_history: bool = True
    ) -> LearningResult:
        """
        Learn from a single execution and update baselines.

        Args:
            job_name: Name of the job
            execution_metrics: Metrics from the execution
            store_history: Whether to store in history table

        Returns:
            LearningResult with analysis
        """
        result = LearningResult()

        # Get existing baseline
        baseline = self._get_baseline(job_name)

        # Detect anomalies compared to baseline
        if baseline and baseline.sample_count >= self.min_samples_for_baseline:
            result.anomalies = self._detect_anomalies(baseline, execution_metrics, job_name)

        # Update baseline with new data
        updated_baseline = self._update_baseline(baseline, execution_metrics, job_name)
        result.baseline = updated_baseline

        # Store execution in history
        if store_history:
            self._store_execution(job_name, execution_metrics)

        # Save updated baseline
        self._save_baseline(updated_baseline)

        return result

    def analyze_job_history(
        self,
        job_name: str,
        lookback_days: int = 30
    ) -> LearningResult:
        """
        Analyze historical executions for a job.

        Args:
            job_name: Name of the job to analyze
            lookback_days: Number of days of history to analyze

        Returns:
            LearningResult with comprehensive analysis
        """
        result = LearningResult()

        # Get historical executions
        history = self._get_execution_history(job_name, lookback_days)

        if not history:
            result.recommendations.append(
                f"No execution history found for {job_name}. Run the job to build baseline."
            )
            return result

        # Build/update baseline
        result.baseline = self._build_baseline_from_history(job_name, history)

        # Detect trends
        result.trends = self._detect_trends(history)

        # Predict failures
        result.failure_prediction = self._predict_failure(job_name, history, result.baseline)

        # Generate insights
        result.insights = self._generate_insights(job_name, history, result.baseline)

        # Generate recommendations
        result.recommendations = self._generate_recommendations(result)

        return result

    def predict_optimal_config(
        self,
        job_name: str,
        input_size_bytes: int,
        day_of_week: int
    ) -> Dict[str, Any]:
        """
        Predict optimal configuration based on historical performance.

        Args:
            job_name: Name of the job
            input_size_bytes: Expected input size
            day_of_week: Day of week (0=Monday, 6=Sunday)

        Returns:
            Recommended configuration
        """
        history = self._get_execution_history(job_name, lookback_days=90)

        if not history:
            return {"error": "Insufficient history for prediction"}

        # Filter similar executions
        similar_runs = []
        for run in history:
            run_size = run.get("input_bytes", 0)
            # Within 50% of expected size
            if 0.5 * input_size_bytes <= run_size <= 1.5 * input_size_bytes:
                similar_runs.append(run)

        if not similar_runs:
            similar_runs = history  # Fall back to all history

        # Find best performing configuration
        successful_runs = [r for r in similar_runs if r.get("status") == "SUCCEEDED"]
        if not successful_runs:
            successful_runs = similar_runs

        # Group by configuration
        config_performance = {}
        for run in successful_runs:
            config_key = f"{run.get('platform')}_{run.get('worker_type')}_{run.get('num_workers')}"
            if config_key not in config_performance:
                config_performance[config_key] = {
                    "platform": run.get("platform"),
                    "worker_type": run.get("worker_type"),
                    "num_workers": run.get("num_workers"),
                    "durations": [],
                    "costs": [],
                    "success_count": 0,
                    "total_count": 0
                }

            config_performance[config_key]["durations"].append(run.get("duration_seconds", 0))
            config_performance[config_key]["costs"].append(run.get("cost", 0))
            config_performance[config_key]["total_count"] += 1
            if run.get("status") == "SUCCEEDED":
                config_performance[config_key]["success_count"] += 1

        # Score configurations
        best_config = None
        best_score = float('inf')

        for config_key, perf in config_performance.items():
            avg_duration = sum(perf["durations"]) / len(perf["durations"])
            avg_cost = sum(perf["costs"]) / len(perf["costs"])
            success_rate = perf["success_count"] / perf["total_count"]

            # Score: lower is better (normalized cost + duration penalty + failure penalty)
            score = avg_cost + (avg_duration / 3600) + ((1 - success_rate) * 10)

            if score < best_score:
                best_score = score
                best_config = {
                    "platform": perf["platform"],
                    "worker_type": perf["worker_type"],
                    "num_workers": perf["num_workers"],
                    "expected_duration_seconds": int(avg_duration),
                    "expected_cost": round(avg_cost, 2),
                    "success_rate": success_rate,
                    "sample_count": perf["total_count"],
                    "confidence": "high" if perf["total_count"] >= 10 else "medium"
                }

        return best_config or {"error": "Could not determine optimal config"}

    def _get_baseline(self, job_name: str) -> Optional[ExecutionBaseline]:
        """Get existing baseline from DynamoDB."""
        if not self.dynamodb:
            return None

        try:
            response = self.dynamodb.get_item(
                TableName=self.baseline_table,
                Key={"job_name": {"S": job_name}}
            )
            if "Item" in response:
                item = response["Item"]
                return ExecutionBaseline(
                    job_name=job_name,
                    avg_duration_seconds=float(item.get("avg_duration_seconds", {}).get("N", 0)),
                    std_duration_seconds=float(item.get("std_duration_seconds", {}).get("N", 0)),
                    avg_cost=float(item.get("avg_cost", {}).get("N", 0)),
                    avg_memory_utilization=float(item.get("avg_memory_utilization", {}).get("N", 0)),
                    avg_cpu_utilization=float(item.get("avg_cpu_utilization", {}).get("N", 0)),
                    avg_input_bytes=int(item.get("avg_input_bytes", {}).get("N", 0)),
                    avg_output_bytes=int(item.get("avg_output_bytes", {}).get("N", 0)),
                    avg_shuffle_bytes=int(item.get("avg_shuffle_bytes", {}).get("N", 0)),
                    typical_worker_count=int(item.get("typical_worker_count", {}).get("N", 0)),
                    typical_worker_type=item.get("typical_worker_type", {}).get("S", ""),
                    success_rate=float(item.get("success_rate", {}).get("N", 1.0)),
                    last_updated=item.get("last_updated", {}).get("S", ""),
                    sample_count=int(item.get("sample_count", {}).get("N", 0))
                )
        except Exception:
            pass
        return None

    def _update_baseline(
        self,
        baseline: Optional[ExecutionBaseline],
        metrics: Dict[str, Any],
        job_name: str
    ) -> ExecutionBaseline:
        """Update baseline with new execution data."""
        if not baseline:
            baseline = ExecutionBaseline(job_name=job_name)

        n = baseline.sample_count

        # Running average updates
        def running_avg(old_avg: float, new_val: float, count: int) -> float:
            if count == 0:
                return float(new_val)
            return (old_avg * count + new_val) / (count + 1)

        # Running std deviation (Welford's algorithm simplified)
        def running_std(old_std: float, old_avg: float, new_val: float, count: int) -> float:
            if count < 2:
                return 0.0
            new_avg = running_avg(old_avg, new_val, count)
            variance = old_std ** 2
            new_variance = ((count - 1) * variance + (new_val - old_avg) * (new_val - new_avg)) / count
            return math.sqrt(max(0, new_variance))

        new_duration = metrics.get("duration_seconds", baseline.avg_duration_seconds)
        baseline.std_duration_seconds = running_std(
            baseline.std_duration_seconds,
            baseline.avg_duration_seconds,
            new_duration,
            n
        )
        baseline.avg_duration_seconds = running_avg(baseline.avg_duration_seconds, new_duration, n)

        baseline.avg_cost = running_avg(baseline.avg_cost, metrics.get("cost", 0), n)
        baseline.avg_memory_utilization = running_avg(
            baseline.avg_memory_utilization,
            metrics.get("memory_utilization", 0),
            n
        )
        baseline.avg_cpu_utilization = running_avg(
            baseline.avg_cpu_utilization,
            metrics.get("cpu_utilization", 0),
            n
        )
        baseline.avg_input_bytes = int(running_avg(
            baseline.avg_input_bytes,
            metrics.get("input_bytes", 0),
            n
        ))
        baseline.avg_output_bytes = int(running_avg(
            baseline.avg_output_bytes,
            metrics.get("output_bytes", 0),
            n
        ))
        baseline.avg_shuffle_bytes = int(running_avg(
            baseline.avg_shuffle_bytes,
            metrics.get("shuffle_bytes", 0),
            n
        ))

        # Update success rate
        is_success = 1 if metrics.get("status") == "SUCCEEDED" else 0
        baseline.success_rate = running_avg(baseline.success_rate, is_success, n)

        # Update worker info
        baseline.typical_worker_type = metrics.get("worker_type", baseline.typical_worker_type)
        baseline.typical_worker_count = int(running_avg(
            baseline.typical_worker_count,
            metrics.get("num_workers", baseline.typical_worker_count),
            n
        ))

        baseline.sample_count = n + 1
        baseline.last_updated = datetime.utcnow().isoformat()

        return baseline

    def _save_baseline(self, baseline: ExecutionBaseline) -> None:
        """Save baseline to DynamoDB."""
        if not self.dynamodb:
            return

        try:
            self.dynamodb.put_item(
                TableName=self.baseline_table,
                Item={
                    "job_name": {"S": baseline.job_name},
                    "avg_duration_seconds": {"N": str(baseline.avg_duration_seconds)},
                    "std_duration_seconds": {"N": str(baseline.std_duration_seconds)},
                    "avg_cost": {"N": str(baseline.avg_cost)},
                    "avg_memory_utilization": {"N": str(baseline.avg_memory_utilization)},
                    "avg_cpu_utilization": {"N": str(baseline.avg_cpu_utilization)},
                    "avg_input_bytes": {"N": str(baseline.avg_input_bytes)},
                    "avg_output_bytes": {"N": str(baseline.avg_output_bytes)},
                    "avg_shuffle_bytes": {"N": str(baseline.avg_shuffle_bytes)},
                    "typical_worker_count": {"N": str(baseline.typical_worker_count)},
                    "typical_worker_type": {"S": baseline.typical_worker_type},
                    "success_rate": {"N": str(baseline.success_rate)},
                    "last_updated": {"S": baseline.last_updated},
                    "sample_count": {"N": str(baseline.sample_count)}
                }
            )
        except Exception:
            pass

    def _store_execution(self, job_name: str, metrics: Dict[str, Any]) -> None:
        """Store execution in history table."""
        if not self.dynamodb:
            return

        try:
            timestamp = datetime.utcnow().isoformat()
            self.dynamodb.put_item(
                TableName=self.history_table,
                Item={
                    "job_name": {"S": job_name},
                    "timestamp": {"S": timestamp},
                    "execution_id": {"S": metrics.get("execution_id", timestamp)},
                    "status": {"S": metrics.get("status", "UNKNOWN")},
                    "duration_seconds": {"N": str(metrics.get("duration_seconds", 0))},
                    "cost": {"N": str(metrics.get("cost", 0))},
                    "input_bytes": {"N": str(metrics.get("input_bytes", 0))},
                    "output_bytes": {"N": str(metrics.get("output_bytes", 0))},
                    "shuffle_bytes": {"N": str(metrics.get("shuffle_bytes", 0))},
                    "memory_utilization": {"N": str(metrics.get("memory_utilization", 0))},
                    "cpu_utilization": {"N": str(metrics.get("cpu_utilization", 0))},
                    "platform": {"S": metrics.get("platform", "")},
                    "worker_type": {"S": metrics.get("worker_type", "")},
                    "num_workers": {"N": str(metrics.get("num_workers", 0))},
                    "error_message": {"S": metrics.get("error_message", "")},
                    "day_of_week": {"N": str(datetime.utcnow().weekday())}
                }
            )
        except Exception:
            pass

    def _get_execution_history(self, job_name: str, lookback_days: int) -> List[Dict]:
        """Get execution history from DynamoDB."""
        if not self.dynamodb:
            return []

        try:
            cutoff = (datetime.utcnow() - timedelta(days=lookback_days)).isoformat()

            response = self.dynamodb.query(
                TableName=self.history_table,
                KeyConditionExpression="job_name = :jn AND #ts > :cutoff",
                ExpressionAttributeNames={"#ts": "timestamp"},
                ExpressionAttributeValues={
                    ":jn": {"S": job_name},
                    ":cutoff": {"S": cutoff}
                }
            )

            history = []
            for item in response.get("Items", []):
                history.append({
                    "timestamp": item.get("timestamp", {}).get("S", ""),
                    "execution_id": item.get("execution_id", {}).get("S", ""),
                    "status": item.get("status", {}).get("S", ""),
                    "duration_seconds": float(item.get("duration_seconds", {}).get("N", 0)),
                    "cost": float(item.get("cost", {}).get("N", 0)),
                    "input_bytes": int(item.get("input_bytes", {}).get("N", 0)),
                    "output_bytes": int(item.get("output_bytes", {}).get("N", 0)),
                    "shuffle_bytes": int(item.get("shuffle_bytes", {}).get("N", 0)),
                    "memory_utilization": float(item.get("memory_utilization", {}).get("N", 0)),
                    "cpu_utilization": float(item.get("cpu_utilization", {}).get("N", 0)),
                    "platform": item.get("platform", {}).get("S", ""),
                    "worker_type": item.get("worker_type", {}).get("S", ""),
                    "num_workers": int(item.get("num_workers", {}).get("N", 0)),
                    "error_message": item.get("error_message", {}).get("S", ""),
                    "day_of_week": int(item.get("day_of_week", {}).get("N", 0))
                })

            return sorted(history, key=lambda x: x["timestamp"])
        except Exception:
            return []

    def _build_baseline_from_history(
        self,
        job_name: str,
        history: List[Dict]
    ) -> ExecutionBaseline:
        """Build baseline from historical data."""
        baseline = ExecutionBaseline(job_name=job_name)

        if not history:
            return baseline

        durations = [h["duration_seconds"] for h in history]
        costs = [h["cost"] for h in history]
        memory_utils = [h["memory_utilization"] for h in history]
        cpu_utils = [h["cpu_utilization"] for h in history]
        input_bytes = [h["input_bytes"] for h in history]
        output_bytes = [h["output_bytes"] for h in history]
        shuffle_bytes = [h["shuffle_bytes"] for h in history]
        successes = [1 if h["status"] == "SUCCEEDED" else 0 for h in history]

        n = len(history)
        baseline.sample_count = n
        baseline.avg_duration_seconds = sum(durations) / n
        baseline.std_duration_seconds = math.sqrt(sum((d - baseline.avg_duration_seconds)**2 for d in durations) / n) if n > 1 else 0
        baseline.avg_cost = sum(costs) / n
        baseline.avg_memory_utilization = sum(memory_utils) / n
        baseline.avg_cpu_utilization = sum(cpu_utils) / n
        baseline.avg_input_bytes = int(sum(input_bytes) / n)
        baseline.avg_output_bytes = int(sum(output_bytes) / n)
        baseline.avg_shuffle_bytes = int(sum(shuffle_bytes) / n)
        baseline.success_rate = sum(successes) / n

        # Most common worker config
        worker_configs = [(h["worker_type"], h["num_workers"]) for h in history if h["worker_type"]]
        if worker_configs:
            from collections import Counter
            most_common = Counter(worker_configs).most_common(1)[0][0]
            baseline.typical_worker_type = most_common[0]
            baseline.typical_worker_count = most_common[1]

        baseline.last_updated = datetime.utcnow().isoformat()

        return baseline

    def _detect_anomalies(
        self,
        baseline: ExecutionBaseline,
        metrics: Dict[str, Any],
        job_name: str
    ) -> List[Anomaly]:
        """Detect anomalies in current execution vs baseline."""
        anomalies = []
        timestamp = datetime.utcnow().isoformat()

        # Duration anomaly
        if baseline.std_duration_seconds > 0:
            duration = metrics.get("duration_seconds", 0)
            z_score = (duration - baseline.avg_duration_seconds) / baseline.std_duration_seconds
            if abs(z_score) > self.anomaly_threshold:
                anomalies.append(Anomaly(
                    anomaly_type=AnomalyType.DURATION_SPIKE,
                    timestamp=timestamp,
                    job_name=job_name,
                    expected_value=baseline.avg_duration_seconds,
                    actual_value=duration,
                    deviation_factor=z_score,
                    description=f"Duration {duration:.0f}s is {abs(z_score):.1f} std devs from mean {baseline.avg_duration_seconds:.0f}s",
                    severity="high" if abs(z_score) > 4 else "medium"
                ))

        # Memory anomaly
        memory_util = metrics.get("memory_utilization", 0)
        if baseline.avg_memory_utilization > 0 and memory_util > baseline.avg_memory_utilization * 1.5:
            anomalies.append(Anomaly(
                anomaly_type=AnomalyType.MEMORY_SPIKE,
                timestamp=timestamp,
                job_name=job_name,
                expected_value=baseline.avg_memory_utilization,
                actual_value=memory_util,
                deviation_factor=memory_util / baseline.avg_memory_utilization,
                description=f"Memory utilization {memory_util:.1f}% vs expected {baseline.avg_memory_utilization:.1f}%",
                severity="high" if memory_util > 90 else "medium"
            ))

        # Cost anomaly
        cost = metrics.get("cost", 0)
        if baseline.avg_cost > 0 and cost > baseline.avg_cost * 2:
            anomalies.append(Anomaly(
                anomaly_type=AnomalyType.COST_SPIKE,
                timestamp=timestamp,
                job_name=job_name,
                expected_value=baseline.avg_cost,
                actual_value=cost,
                deviation_factor=cost / baseline.avg_cost,
                description=f"Cost ${cost:.2f} vs expected ${baseline.avg_cost:.2f}",
                severity="high" if cost > baseline.avg_cost * 3 else "medium"
            ))

        # Data volume anomaly
        input_bytes = metrics.get("input_bytes", 0)
        if baseline.avg_input_bytes > 0 and input_bytes > baseline.avg_input_bytes * 2:
            anomalies.append(Anomaly(
                anomaly_type=AnomalyType.DATA_VOLUME_SPIKE,
                timestamp=timestamp,
                job_name=job_name,
                expected_value=baseline.avg_input_bytes,
                actual_value=input_bytes,
                deviation_factor=input_bytes / baseline.avg_input_bytes,
                description=f"Input volume {input_bytes/(1024**3):.1f}GB vs expected {baseline.avg_input_bytes/(1024**3):.1f}GB",
                severity="medium"
            ))

        return anomalies

    def _detect_trends(self, history: List[Dict]) -> Dict[str, TrendType]:
        """Detect trends in historical data."""
        trends = {}

        if len(history) < 5:
            return {"duration": TrendType.STABLE, "cost": TrendType.STABLE}

        # Split into halves
        mid = len(history) // 2
        first_half = history[:mid]
        second_half = history[mid:]

        # Duration trend
        first_avg_duration = sum(h["duration_seconds"] for h in first_half) / len(first_half)
        second_avg_duration = sum(h["duration_seconds"] for h in second_half) / len(second_half)

        if second_avg_duration > first_avg_duration * 1.2:
            trends["duration"] = TrendType.DEGRADING
        elif second_avg_duration < first_avg_duration * 0.8:
            trends["duration"] = TrendType.IMPROVING
        else:
            trends["duration"] = TrendType.STABLE

        # Cost trend
        first_avg_cost = sum(h["cost"] for h in first_half) / len(first_half)
        second_avg_cost = sum(h["cost"] for h in second_half) / len(second_half)

        if second_avg_cost > first_avg_cost * 1.2:
            trends["cost"] = TrendType.DEGRADING
        elif second_avg_cost < first_avg_cost * 0.8:
            trends["cost"] = TrendType.IMPROVING
        else:
            trends["cost"] = TrendType.STABLE

        # Check volatility
        durations = [h["duration_seconds"] for h in history]
        if len(durations) > 3:
            avg = sum(durations) / len(durations)
            std = math.sqrt(sum((d - avg)**2 for d in durations) / len(durations))
            cv = std / avg if avg > 0 else 0
            if cv > 0.5:
                trends["stability"] = TrendType.VOLATILE

        return trends

    def _predict_failure(
        self,
        job_name: str,
        history: List[Dict],
        baseline: ExecutionBaseline
    ) -> Optional[FailurePrediction]:
        """Predict probability of failure based on patterns."""
        if len(history) < 5:
            return None

        # Recent failure rate
        recent = history[-10:] if len(history) >= 10 else history
        recent_failures = sum(1 for h in recent if h["status"] != "SUCCEEDED")
        recent_failure_rate = recent_failures / len(recent)

        # Contributing factors
        factors = []
        probability = 0.0

        # Factor 1: Recent failure rate
        if recent_failure_rate > 0.2:
            probability += 0.3
            factors.append(f"High recent failure rate: {recent_failure_rate:.0%}")

        # Factor 2: Data volume increase
        recent_volumes = [h["input_bytes"] for h in recent]
        older_volumes = [h["input_bytes"] for h in history[:-10]] if len(history) > 10 else []
        if older_volumes and recent_volumes:
            recent_avg = sum(recent_volumes) / len(recent_volumes)
            older_avg = sum(older_volumes) / len(older_volumes)
            if recent_avg > older_avg * 1.5:
                probability += 0.2
                factors.append(f"Data volume increased {(recent_avg/older_avg - 1)*100:.0f}%")

        # Factor 3: Memory pressure
        if baseline.avg_memory_utilization > 80:
            probability += 0.2
            factors.append(f"High memory utilization: {baseline.avg_memory_utilization:.0f}%")

        # Factor 4: Duration trend
        if len(history) >= 5:
            durations = [h["duration_seconds"] for h in history[-5:]]
            if all(durations[i] < durations[i+1] for i in range(len(durations)-1)):
                probability += 0.1
                factors.append("Duration consistently increasing")

        # Determine confidence
        if len(history) >= 20:
            confidence = PredictionConfidence.HIGH
        elif len(history) >= 10:
            confidence = PredictionConfidence.MEDIUM
        else:
            confidence = PredictionConfidence.LOW

        # Generate recommendations
        recommendations = []
        if recent_failure_rate > 0.2:
            recommendations.append("Review recent error logs for common patterns")
        if baseline.avg_memory_utilization > 80:
            recommendations.append("Consider increasing worker memory or count")
        if probability > 0.3:
            recommendations.append("Consider running with increased resources as precaution")

        return FailurePrediction(
            job_name=job_name,
            probability=min(probability, 0.95),
            confidence=confidence,
            contributing_factors=factors,
            recommended_actions=recommendations,
            based_on_runs=len(history)
        )

    def _generate_insights(
        self,
        job_name: str,
        history: List[Dict],
        baseline: ExecutionBaseline
    ) -> List[ExecutionInsight]:
        """Generate insights from execution history."""
        insights = []

        # Cost efficiency insight
        if baseline.avg_cost > 0 and baseline.avg_duration_seconds > 0:
            cost_per_minute = baseline.avg_cost / (baseline.avg_duration_seconds / 60)
            insights.append(ExecutionInsight(
                category="cost",
                title="Cost Efficiency",
                description=f"Average cost is ${baseline.avg_cost:.2f} per run (${cost_per_minute:.3f}/minute)",
                impact="Monitor cost trends for optimization opportunities",
                action="Consider spot instances or flex mode for cost reduction",
                data={"avg_cost": baseline.avg_cost, "cost_per_minute": cost_per_minute}
            ))

        # Data processing rate
        if baseline.avg_input_bytes > 0 and baseline.avg_duration_seconds > 0:
            gb_per_minute = (baseline.avg_input_bytes / (1024**3)) / (baseline.avg_duration_seconds / 60)
            insights.append(ExecutionInsight(
                category="performance",
                title="Processing Rate",
                description=f"Average processing rate: {gb_per_minute:.2f} GB/minute",
                impact="Benchmark for performance optimization",
                action="Compare with similar workloads for optimization",
                data={"gb_per_minute": gb_per_minute}
            ))

        # Reliability insight
        if baseline.sample_count >= 10:
            insights.append(ExecutionInsight(
                category="reliability",
                title="Job Reliability",
                description=f"Success rate: {baseline.success_rate:.1%} over {baseline.sample_count} runs",
                impact="High" if baseline.success_rate < 0.9 else "Low",
                action="Investigate failures" if baseline.success_rate < 0.95 else "Maintain current setup",
                data={"success_rate": baseline.success_rate, "sample_count": baseline.sample_count}
            ))

        # Weekday vs weekend pattern
        weekday_runs = [h for h in history if h["day_of_week"] < 5]
        weekend_runs = [h for h in history if h["day_of_week"] >= 5]

        if weekday_runs and weekend_runs:
            weekday_avg = sum(h["input_bytes"] for h in weekday_runs) / len(weekday_runs)
            weekend_avg = sum(h["input_bytes"] for h in weekend_runs) / len(weekend_runs)

            if abs(weekday_avg - weekend_avg) / max(weekday_avg, weekend_avg) > 0.3:
                insights.append(ExecutionInsight(
                    category="pattern",
                    title="Weekday/Weekend Pattern",
                    description=f"Weekday avg: {weekday_avg/(1024**3):.1f}GB, Weekend avg: {weekend_avg/(1024**3):.1f}GB",
                    impact="Consider different configurations for weekdays vs weekends",
                    action="Implement day-based resource scaling",
                    data={"weekday_avg_gb": weekday_avg/(1024**3), "weekend_avg_gb": weekend_avg/(1024**3)}
                ))

        return insights

    def _generate_recommendations(self, result: LearningResult) -> List[str]:
        """Generate actionable recommendations."""
        recommendations = []

        # Based on trends
        if result.trends.get("duration") == TrendType.DEGRADING:
            recommendations.append("Duration is increasing - review for performance regression")
        if result.trends.get("cost") == TrendType.DEGRADING:
            recommendations.append("Costs are increasing - consider optimization or resource review")
        if result.trends.get("stability") == TrendType.VOLATILE:
            recommendations.append("Execution times are volatile - investigate for consistency")

        # Based on failure prediction
        if result.failure_prediction and result.failure_prediction.probability > 0.3:
            recommendations.extend(result.failure_prediction.recommended_actions)

        # Based on baseline
        if result.baseline:
            if result.baseline.avg_memory_utilization > 85:
                recommendations.append("Memory utilization is high - consider larger workers")
            if result.baseline.avg_cpu_utilization < 30:
                recommendations.append("CPU utilization is low - consider fewer but larger workers")
            if result.baseline.success_rate < 0.9:
                recommendations.append("Success rate below 90% - prioritize stability improvements")

        return recommendations
