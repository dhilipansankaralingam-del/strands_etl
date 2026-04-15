#!/usr/bin/env python3
"""Strands Healing Agent - Auto-healing for job failures."""

from typing import Dict, List, Any
from ..base_agent import StrandsAgent, AgentResult, AgentStatus, AgentContext, register_agent
from ..storage import StrandsStorage


@register_agent
class StrandsHealingAgent(StrandsAgent):
    """Auto-healing agent for ETL job failures."""

    AGENT_NAME = "healing_agent"
    AGENT_VERSION = "2.0.0"
    AGENT_DESCRIPTION = "Detects and heals common ETL failures"

    DEPENDENCIES = []
    PARALLEL_SAFE = True

    # Error patterns and remediation
    ERROR_PATTERNS = {
        'oom': {
            'patterns': ['OutOfMemoryError', 'Container killed', 'memory limit'],
            'remediation': 'increase_memory',
            'description': 'Out of memory error'
        },
        'shuffle': {
            'patterns': ['shuffle fetch failed', 'FetchFailedException'],
            'remediation': 'increase_shuffle_partitions',
            'description': 'Shuffle failure'
        },
        'timeout': {
            'patterns': ['timeout', 'timed out', 'deadline exceeded'],
            'remediation': 'increase_timeout',
            'description': 'Timeout error'
        },
        'connection': {
            'patterns': ['connection refused', 'connection reset', 'network error'],
            'remediation': 'retry_with_backoff',
            'description': 'Connection error'
        },
        'data_skew': {
            'patterns': ['skewed', 'single task running', 'straggler'],
            'remediation': 'enable_adaptive_skew',
            'description': 'Data skew detected'
        },
        'partition': {
            'patterns': ['partition not found', 'MSCK REPAIR'],
            'remediation': 'repair_partitions',
            'description': 'Partition error'
        }
    }

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.storage = StrandsStorage(config)

    def execute(self, context: AgentContext) -> AgentResult:
        healing_config = context.config.get('auto_healing', {})
        if not self.is_enabled('auto_healing.enabled'):
            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.COMPLETED,
                output={'skipped': True}
            )

        # Get any error context (would come from actual job execution)
        error_context = context.get_shared('job_error', None)

        healing_strategies = []
        recommendations = []

        if error_context:
            detected_errors = self._detect_errors(error_context)
            for error in detected_errors:
                strategy = self._get_healing_strategy(error, healing_config)
                if strategy:
                    healing_strategies.append(strategy)
                    recommendations.append(f"Auto-heal: {strategy['description']} - {strategy['action']}")
        else:
            # Pre-emptive healing checks
            pre_emptive = self._pre_emptive_checks(context)
            healing_strategies.extend(pre_emptive)

        # Store healing data
        self.storage.store_agent_data(
            self.AGENT_NAME,
            'healing_strategies',
            healing_strategies,
            use_pipe_delimited=True
        )

        context.set_shared('healing_strategies', healing_strategies)

        return AgentResult(
            agent_name=self.AGENT_NAME,
            agent_id=self.agent_id,
            status=AgentStatus.COMPLETED,
            output={
                'strategies_count': len(healing_strategies),
                'strategies': healing_strategies
            },
            metrics={
                'strategies_prepared': len(healing_strategies)
            },
            recommendations=recommendations
        )

    def _detect_errors(self, error_context: Dict) -> List[str]:
        """Detect error types from error context."""
        detected = []
        error_message = str(error_context.get('message', '')).lower()

        for error_type, config in self.ERROR_PATTERNS.items():
            for pattern in config['patterns']:
                if pattern.lower() in error_message:
                    detected.append(error_type)
                    break

        return detected

    def _get_healing_strategy(self, error_type: str, config: Dict) -> Dict:
        """Get healing strategy for error type."""
        error_config = self.ERROR_PATTERNS.get(error_type)
        if not error_config:
            return None

        # Check if healing is enabled for this type
        config_key = f"heal_{error_type}_errors"
        if not config.get(config_key) in ('Y', 'y', True):
            return None

        remediation = error_config['remediation']
        action = None

        if remediation == 'increase_memory':
            action = {'type': 'config_change', 'key': 'worker_type', 'value': 'G.4X'}
        elif remediation == 'increase_shuffle_partitions':
            action = {'type': 'spark_config', 'key': 'spark.sql.shuffle.partitions', 'value': '800'}
        elif remediation == 'increase_timeout':
            action = {'type': 'config_change', 'key': 'timeout_minutes', 'value': 720}
        elif remediation == 'retry_with_backoff':
            action = {'type': 'retry', 'backoff_seconds': [30, 60, 120]}
        elif remediation == 'enable_adaptive_skew':
            action = {'type': 'spark_config', 'key': 'spark.sql.adaptive.skewJoin.enabled', 'value': 'true'}
        elif remediation == 'repair_partitions':
            action = {'type': 'sql_command', 'command': 'MSCK REPAIR TABLE {table}'}

        return {
            'error_type': error_type,
            'description': error_config['description'],
            'remediation': remediation,
            'action': action
        }

    def _pre_emptive_checks(self, context: AgentContext) -> List[Dict]:
        """Pre-emptive healing checks before job runs."""
        strategies = []

        # Check if data size suggests potential OOM
        total_size_gb = context.get_shared('total_size_gb', 0)
        recommended_type = context.get_shared('recommended_worker_type', 'G.2X')

        if total_size_gb > 200 and recommended_type in ['G.1X', 'G.2X']:
            strategies.append({
                'error_type': 'pre_emptive_oom',
                'description': 'Pre-emptive OOM prevention',
                'remediation': 'increase_memory',
                'action': {'type': 'config_change', 'key': 'worker_type', 'value': 'G.4X'}
            })

        return strategies
