"""
Local Agent Data Store
=======================

Persists agent data locally for learning, compliance, and recommendations.
This allows agents to learn from actual job executions.

Storage Structure:
    data/agent_store/
        execution_history.json  - All job execution records
        compliance_results.json - Compliance check results per run
        data_quality_results.json - Data quality check results
        baselines.json          - Computed baselines per job
        recommendations.json    - Generated recommendations
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict, field
from pathlib import Path
import statistics

logger = logging.getLogger(__name__)


@dataclass
class ExecutionRecord:
    """Record of a single job execution."""
    job_name: str
    run_id: str
    timestamp: str
    records_processed: int
    duration_seconds: float
    dpu_hours: float
    cost_usd: float
    memory_gb: float
    platform: str
    workers: int
    status: str  # SUCCEEDED, FAILED, TIMEOUT
    error_message: Optional[str] = None
    input_tables: List[str] = field(default_factory=list)
    output_tables: List[str] = field(default_factory=list)
    spark_metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ComplianceResult:
    """Result of a compliance check."""
    job_name: str
    run_id: str
    timestamp: str
    overall_status: str  # COMPLIANT, NON_COMPLIANT, WARNING
    frameworks: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    pii_columns_detected: List[str] = field(default_factory=list)
    pii_columns_masked: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


@dataclass
class DataQualityResult:
    """Result of a data quality check."""
    table_name: str
    run_id: str
    timestamp: str
    total_checks: int
    passed_checks: int
    failed_checks: int
    warning_checks: int
    check_details: List[Dict[str, Any]] = field(default_factory=list)
    row_count: int = 0
    null_percentages: Dict[str, float] = field(default_factory=dict)


@dataclass
class JobBaseline:
    """Computed baseline metrics for a job."""
    job_name: str
    sample_count: int
    created_at: str
    updated_at: str
    avg_duration_seconds: float
    avg_cost: float
    avg_records: float
    avg_memory_gb: float
    typical_workers: int
    success_rate: float
    std_duration: float = 0.0
    std_cost: float = 0.0
    p95_duration: float = 0.0
    p95_cost: float = 0.0


class LocalAgentStore:
    """
    Local file-based storage for agent learning data.

    Usage:
        store = LocalAgentStore()

        # Record an execution
        store.record_execution(ExecutionRecord(...))

        # Get history for a job
        history = store.get_execution_history('sales_analytics')

        # Get/update baselines
        baseline = store.get_baseline('sales_analytics')
        store.update_baseline('sales_analytics')
    """

    DEFAULT_STORE_PATH = "data/agent_store"

    def __init__(self, store_path: str = None):
        self.store_path = Path(store_path or self.DEFAULT_STORE_PATH)
        self._ensure_store_exists()

    def _ensure_store_exists(self):
        """Create store directory and initialize files if needed."""
        self.store_path.mkdir(parents=True, exist_ok=True)

        # Initialize empty files if they don't exist
        files = [
            'execution_history.json',
            'compliance_results.json',
            'data_quality_results.json',
            'baselines.json',
            'recommendations.json',
            'anomalies.json'
        ]

        for filename in files:
            filepath = self.store_path / filename
            if not filepath.exists():
                with open(filepath, 'w') as f:
                    json.dump([], f) if 'history' in filename or 'results' in filename or 'anomalies' in filename else json.dump({}, f)

    def _read_json(self, filename: str) -> Any:
        """Read a JSON file from the store."""
        filepath = self.store_path / filename
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return [] if 'history' in filename or 'results' in filename else {}

    def _write_json(self, filename: str, data: Any):
        """Write data to a JSON file in the store."""
        filepath = self.store_path / filename
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)

    # ==================== Execution History ====================

    def record_execution(self, record: ExecutionRecord):
        """Record a job execution."""
        history = self._read_json('execution_history.json')
        history.append(asdict(record))
        self._write_json('execution_history.json', history)

        # Auto-update baseline after new execution
        self.update_baseline(record.job_name)

        # Check for anomalies
        self._check_for_anomalies(record)

        logger.info(f"Recorded execution: {record.job_name} - {record.status}")

    def get_execution_history(self, job_name: str = None,
                               limit: int = 100,
                               status_filter: str = None) -> List[Dict]:
        """Get execution history, optionally filtered by job name."""
        history = self._read_json('execution_history.json')

        if job_name:
            history = [h for h in history if h.get('job_name') == job_name]

        if status_filter:
            history = [h for h in history if h.get('status') == status_filter]

        # Sort by timestamp descending (most recent first)
        history.sort(key=lambda x: x.get('timestamp', ''), reverse=True)

        return history[:limit]

    def get_last_execution(self, job_name: str = None) -> Optional[Dict]:
        """Get the most recent execution record."""
        history = self.get_execution_history(job_name, limit=1)
        return history[0] if history else None

    def get_recent_runs(self, job_name: str, days: int = 7) -> List[Dict]:
        """Get runs from the last N days."""
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        history = self.get_execution_history(job_name)
        return [h for h in history if h.get('timestamp', '') >= cutoff]

    # ==================== Baselines ====================

    def get_baseline(self, job_name: str) -> Optional[Dict]:
        """Get baseline metrics for a job."""
        baselines = self._read_json('baselines.json')
        return baselines.get(job_name)

    def get_all_baselines(self) -> Dict[str, Dict]:
        """Get all baselines."""
        return self._read_json('baselines.json')

    def update_baseline(self, job_name: str, min_samples: int = 5):
        """Recompute baseline from execution history."""
        history = self.get_execution_history(job_name, status_filter='SUCCEEDED')

        if len(history) < min_samples:
            logger.debug(f"Not enough samples for baseline: {job_name} ({len(history)}/{min_samples})")
            return

        # Compute statistics
        durations = [h['duration_seconds'] for h in history]
        costs = [h['cost_usd'] for h in history]
        records = [h['records_processed'] for h in history]
        memories = [h['memory_gb'] for h in history]
        workers = [h['workers'] for h in history]

        total_runs = len(self.get_execution_history(job_name))
        succeeded_runs = len(history)

        baseline = JobBaseline(
            job_name=job_name,
            sample_count=len(history),
            created_at=history[-1].get('timestamp', datetime.now().isoformat()),
            updated_at=datetime.now().isoformat(),
            avg_duration_seconds=statistics.mean(durations),
            avg_cost=statistics.mean(costs),
            avg_records=statistics.mean(records),
            avg_memory_gb=statistics.mean(memories),
            typical_workers=int(statistics.median(workers)),
            success_rate=(succeeded_runs / total_runs * 100) if total_runs > 0 else 100.0,
            std_duration=statistics.stdev(durations) if len(durations) > 1 else 0,
            std_cost=statistics.stdev(costs) if len(costs) > 1 else 0,
            p95_duration=sorted(durations)[int(len(durations) * 0.95)] if durations else 0,
            p95_cost=sorted(costs)[int(len(costs) * 0.95)] if costs else 0
        )

        baselines = self._read_json('baselines.json')
        baselines[job_name] = asdict(baseline)
        self._write_json('baselines.json', baselines)

        logger.info(f"Updated baseline for {job_name}: {len(history)} samples")

    # ==================== Compliance ====================

    def record_compliance(self, result: ComplianceResult):
        """Record a compliance check result."""
        results = self._read_json('compliance_results.json')
        results.append(asdict(result))
        self._write_json('compliance_results.json', results)

    def get_compliance_history(self, job_name: str = None, limit: int = 30) -> List[Dict]:
        """Get compliance check history."""
        results = self._read_json('compliance_results.json')

        if job_name:
            results = [r for r in results if r.get('job_name') == job_name]

        results.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        return results[:limit]

    def get_last_compliance(self, job_name: str = None) -> Optional[Dict]:
        """Get the most recent compliance result."""
        history = self.get_compliance_history(job_name, limit=1)
        return history[0] if history else None

    # ==================== Data Quality ====================

    def record_data_quality(self, result: DataQualityResult):
        """Record a data quality check result."""
        results = self._read_json('data_quality_results.json')
        results.append(asdict(result))
        self._write_json('data_quality_results.json', results)

    def get_data_quality_history(self, table_name: str = None, limit: int = 30) -> List[Dict]:
        """Get data quality check history."""
        results = self._read_json('data_quality_results.json')

        if table_name:
            results = [r for r in results if r.get('table_name') == table_name]

        results.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        return results[:limit]

    def get_last_data_quality(self, table_name: str) -> Optional[Dict]:
        """Get the most recent data quality result for a table."""
        history = self.get_data_quality_history(table_name, limit=1)
        return history[0] if history else None

    # ==================== Anomalies ====================

    def _check_for_anomalies(self, record: ExecutionRecord):
        """Check if the execution has anomalies vs baseline."""
        baseline = self.get_baseline(record.job_name)
        if not baseline:
            return

        anomalies = []

        # Check duration (threshold: 2x std dev or 50% deviation)
        avg_duration = baseline.get('avg_duration_seconds', 0)
        std_duration = baseline.get('std_duration', avg_duration * 0.2)
        threshold = max(std_duration * 2, avg_duration * 0.5)

        if abs(record.duration_seconds - avg_duration) > threshold:
            deviation = ((record.duration_seconds - avg_duration) / avg_duration * 100) if avg_duration else 0
            anomalies.append({
                'type': 'DURATION',
                'detail': f'Duration {deviation:+.1f}% from baseline',
                'value': record.duration_seconds,
                'expected': avg_duration,
                'threshold': threshold
            })

        # Check cost
        avg_cost = baseline.get('avg_cost', 0)
        std_cost = baseline.get('std_cost', avg_cost * 0.2)
        cost_threshold = max(std_cost * 2, avg_cost * 0.5)

        if abs(record.cost_usd - avg_cost) > cost_threshold:
            deviation = ((record.cost_usd - avg_cost) / avg_cost * 100) if avg_cost else 0
            anomalies.append({
                'type': 'COST',
                'detail': f'Cost {deviation:+.1f}% from baseline',
                'value': record.cost_usd,
                'expected': avg_cost,
                'threshold': cost_threshold
            })

        # Check records processed (might indicate data issues)
        avg_records = baseline.get('avg_records', 0)
        if avg_records > 0:
            records_deviation = abs(record.records_processed - avg_records) / avg_records
            if records_deviation > 0.5:  # 50% deviation
                anomalies.append({
                    'type': 'RECORDS',
                    'detail': f'Records {records_deviation*100:+.1f}% from baseline',
                    'value': record.records_processed,
                    'expected': avg_records
                })

        if anomalies:
            self._record_anomalies(record, anomalies)

    def _record_anomalies(self, record: ExecutionRecord, anomalies: List[Dict]):
        """Record detected anomalies."""
        all_anomalies = self._read_json('anomalies.json')

        for anomaly in anomalies:
            all_anomalies.append({
                'job_name': record.job_name,
                'run_id': record.run_id,
                'timestamp': record.timestamp,
                'resolved': False,
                **anomaly
            })

        self._write_json('anomalies.json', all_anomalies)
        logger.warning(f"Anomalies detected for {record.job_name}: {len(anomalies)}")

    def get_anomalies(self, job_name: str = None,
                      include_resolved: bool = False,
                      days: int = 30) -> List[Dict]:
        """Get anomaly records."""
        anomalies = self._read_json('anomalies.json')

        if job_name:
            anomalies = [a for a in anomalies if a.get('job_name') == job_name]

        if not include_resolved:
            anomalies = [a for a in anomalies if not a.get('resolved')]

        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        anomalies = [a for a in anomalies if a.get('timestamp', '') >= cutoff]

        anomalies.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        return anomalies

    def resolve_anomaly(self, run_id: str, anomaly_type: str):
        """Mark an anomaly as resolved."""
        anomalies = self._read_json('anomalies.json')

        for a in anomalies:
            if a.get('run_id') == run_id and a.get('type') == anomaly_type:
                a['resolved'] = True
                a['resolved_at'] = datetime.now().isoformat()

        self._write_json('anomalies.json', anomalies)

    # ==================== Trends ====================

    def compute_trends(self, job_name: str, days: int = 7) -> Dict[str, Dict]:
        """Compute trends for metrics over the specified period."""
        recent = self.get_recent_runs(job_name, days)

        if len(recent) < 2:
            return {}

        # Sort by timestamp ascending for trend calculation
        recent.sort(key=lambda x: x.get('timestamp', ''))

        def calc_trend(values):
            if len(values) < 2:
                return {'direction': 'stable', 'change': '0%'}

            first_half = statistics.mean(values[:len(values)//2])
            second_half = statistics.mean(values[len(values)//2:])

            if first_half == 0:
                return {'direction': 'stable', 'change': '0%'}

            change = (second_half - first_half) / first_half * 100

            if change > 5:
                direction = 'increasing'
            elif change < -5:
                direction = 'decreasing'
            else:
                direction = 'stable'

            return {
                'direction': direction,
                'change': f'{change:+.1f}%',
                'period': f'{days} days'
            }

        return {
            'duration': calc_trend([r['duration_seconds'] for r in recent]),
            'cost': calc_trend([r['cost_usd'] for r in recent]),
            'records': calc_trend([r['records_processed'] for r in recent]),
            'memory': calc_trend([r['memory_gb'] for r in recent])
        }

    # ==================== Recommendations ====================

    def add_recommendation(self, job_name: str, category: str,
                           priority: str, message: str, source: str = 'learning'):
        """Add a recommendation."""
        recs = self._read_json('recommendations.json')

        if job_name not in recs:
            recs[job_name] = []

        recs[job_name].append({
            'category': category,
            'priority': priority,
            'message': message,
            'source': source,
            'timestamp': datetime.now().isoformat(),
            'applied': False
        })

        self._write_json('recommendations.json', recs)

    def get_recommendations(self, job_name: str = None) -> Dict[str, List[Dict]]:
        """Get recommendations, optionally filtered by job."""
        recs = self._read_json('recommendations.json')

        if job_name:
            return {job_name: recs.get(job_name, [])}

        return recs

    # ==================== Summary ====================

    def get_learning_summary(self, job_name: str) -> Dict:
        """Get a comprehensive learning summary for a job."""
        last_run = self.get_last_execution(job_name)
        baseline = self.get_baseline(job_name)
        trends = self.compute_trends(job_name)
        recent_runs = self.get_recent_runs(job_name, days=7)
        anomalies = self.get_anomalies(job_name, days=30)

        # Calculate deviations from baseline
        deviations = {}
        if last_run and baseline:
            for metric in ['duration_seconds', 'cost_usd', 'records_processed']:
                base_key = metric.replace('_seconds', '').replace('_usd', '').replace('_processed', '')
                base_key = f'avg_{base_key}' if base_key != 'records' else 'avg_records'
                base_val = baseline.get(base_key, 0)
                if base_val > 0:
                    deviation = (last_run.get(metric, 0) - base_val) / base_val * 100
                    deviations[base_key.replace('avg_', '')] = f'{deviation:+.1f}%'

        # Predictions based on trends
        predictions = {}
        if baseline and trends:
            cost_trend = trends.get('cost', {})
            duration_trend = trends.get('duration', {})

            try:
                cost_change = float(cost_trend.get('change', '0%').rstrip('%')) / 100
                duration_change = float(duration_trend.get('change', '0%').rstrip('%')) / 100
            except:
                cost_change = duration_change = 0

            predictions = {
                'next_run_duration': baseline.get('avg_duration_seconds', 0) * (1 + duration_change * 0.5),
                'next_run_cost': baseline.get('avg_cost', 0) * (1 + cost_change * 0.5),
                'recommended_workers': baseline.get('typical_workers', 5),
                'failure_probability': (100 - baseline.get('success_rate', 100)) / 100
            }

        return {
            'job_name': job_name,
            'last_run': last_run,
            'baseline': baseline,
            'trends': trends,
            'deviations': deviations,
            'predictions': predictions,
            'recent_runs': recent_runs[:10],
            'anomalies': anomalies[:10],
            'anomalies_count': len(anomalies)
        }


# Singleton instance for easy access
_store_instance: Optional[LocalAgentStore] = None

def get_store(store_path: str = None) -> LocalAgentStore:
    """Get the singleton store instance."""
    global _store_instance
    if _store_instance is None:
        _store_instance = LocalAgentStore(store_path)
    return _store_instance
