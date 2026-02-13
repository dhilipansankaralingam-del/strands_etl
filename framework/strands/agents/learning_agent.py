#!/usr/bin/env python3
"""Strands Learning Agent - Stores and learns from execution history."""

from datetime import datetime
from typing import Dict, List, Any
from ..base_agent import StrandsAgent, AgentResult, AgentStatus, AgentContext, register_agent
from ..storage import StrandsStorage


@register_agent
class StrandsLearningAgent(StrandsAgent):
    """Stores execution history and learns patterns for predictions."""

    AGENT_NAME = "learning_agent"
    AGENT_VERSION = "2.0.0"
    AGENT_DESCRIPTION = "Stores history and learns from past executions"

    DEPENDENCIES = []  # Can run in parallel
    PARALLEL_SAFE = True

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.storage = StrandsStorage(config)

    def execute(self, context: AgentContext) -> AgentResult:
        learning_config = context.config.get('learning', {})
        if not self.is_enabled('learning.enabled'):
            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.COMPLETED,
                output={'skipped': True}
            )

        # Collect execution data from all agents
        execution_record = self._collect_execution_data(context)

        # Store execution history
        self.storage.store_agent_data(
            self.AGENT_NAME,
            'execution_history',
            [execution_record],
            use_pipe_delimited=True
        )

        # Analyze historical patterns
        patterns = self._analyze_patterns(context)

        # Detect anomalies
        anomalies = []
        if learning_config.get('detect_anomalies') in ('Y', 'y', True):
            anomalies = self._detect_anomalies(execution_record, context)

        # Store patterns and anomalies
        if patterns:
            self.storage.store_agent_data(
                self.AGENT_NAME,
                'patterns',
                patterns,
                use_pipe_delimited=True
            )

        recommendations = []
        if anomalies:
            for anomaly in anomalies:
                recommendations.append(f"Anomaly detected: {anomaly['description']}")

        context.set_shared('execution_record', execution_record)
        context.set_shared('learned_patterns', patterns)

        return AgentResult(
            agent_name=self.AGENT_NAME,
            agent_id=self.agent_id,
            status=AgentStatus.COMPLETED,
            output={
                'record_stored': True,
                'patterns_found': len(patterns),
                'anomalies_found': len(anomalies),
                'execution_record': execution_record
            },
            metrics={
                'patterns_count': len(patterns),
                'anomalies_count': len(anomalies)
            },
            recommendations=recommendations
        )

    def _collect_execution_data(self, context: AgentContext) -> Dict:
        """Collect execution data from context."""
        return {
            'job_name': context.job_name,
            'execution_id': context.execution_id,
            'run_date': context.run_date.isoformat(),
            'platform': context.platform,
            'total_size_gb': context.get_shared('total_size_gb', 0),
            'recommended_workers': context.get_shared('recommended_workers', 0),
            'recommended_worker_type': context.get_shared('recommended_worker_type', ''),
            'target_platform': context.get_shared('target_platform', 'glue'),
            'dq_summary': context.get_shared('dq_summary', {}),
            'compliance_findings_count': len(context.get_shared('compliance_findings', [])),
            'agent_results_count': len(context.agent_results),
            'timestamp': datetime.utcnow().isoformat()
        }

    def _analyze_patterns(self, context: AgentContext) -> List[Dict]:
        """Analyze patterns from historical data."""
        patterns = []

        # Size patterns
        total_size_gb = context.get_shared('total_size_gb', 0)
        if total_size_gb > 0:
            patterns.append({
                'type': 'size_pattern',
                'job_name': context.job_name,
                'run_date': context.run_date.strftime('%A'),  # Day of week
                'size_gb': total_size_gb,
                'is_weekend': context.run_date.weekday() >= 5
            })

        # Worker patterns
        workers = context.get_shared('recommended_workers', 0)
        if workers > 0:
            patterns.append({
                'type': 'worker_pattern',
                'job_name': context.job_name,
                'workers': workers,
                'size_gb': total_size_gb,
                'ratio': total_size_gb / workers if workers > 0 else 0
            })

        return patterns

    def _detect_anomalies(self, current: Dict, context: AgentContext) -> List[Dict]:
        """Detect anomalies compared to historical data."""
        anomalies = []

        # Would compare against stored history in production
        # For now, detect obvious anomalies
        size_gb = current.get('total_size_gb', 0)

        if size_gb > 1000:
            anomalies.append({
                'type': 'size_anomaly',
                'description': f'Unusually large data size: {size_gb:.0f} GB',
                'severity': 'warning'
            })

        return anomalies
