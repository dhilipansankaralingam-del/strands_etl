#!/usr/bin/env python3
"""Strands Resource Allocator Agent - Dynamic resource allocation."""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from ..base_agent import StrandsAgent, AgentResult, AgentStatus, AgentContext, register_agent
from ..storage import StrandsStorage


@register_agent
class StrandsResourceAllocatorAgent(StrandsAgent):
    """Dynamically allocates resources based on data size, patterns, and history."""

    AGENT_NAME = "resource_allocator_agent"
    AGENT_VERSION = "2.0.0"
    AGENT_DESCRIPTION = "Smart resource allocation based on patterns and trends"

    DEPENDENCIES = ['sizing_agent']  # Needs sizing data
    PARALLEL_SAFE = True

    # Worker type specifications
    WORKER_SPECS = {
        'G.1X': {'memory_gb': 16, 'vcpus': 4, 'cost_per_hour': 0.44},
        'G.2X': {'memory_gb': 32, 'vcpus': 8, 'cost_per_hour': 0.88},
        'G.4X': {'memory_gb': 64, 'vcpus': 16, 'cost_per_hour': 1.76},
        'G.8X': {'memory_gb': 128, 'vcpus': 32, 'cost_per_hour': 3.52}
    }

    # Scale factors
    WEEKEND_SCALE = 0.6
    MONTH_END_SCALE = 1.5
    QUARTER_END_SCALE = 2.0

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.storage = StrandsStorage(config)

    def execute(self, context: AgentContext) -> AgentResult:
        allocation_config = context.config.get('smart_allocation', {})
        if not self.is_enabled('smart_allocation.enabled'):
            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.COMPLETED,
                output={'skipped': True}
            )

        # Get sizing data from sizing agent
        total_size_gb = context.get_shared('total_size_gb', 0)

        # Get current config
        glue_config = context.config.get('glue_config', {})
        config_workers = glue_config.get('number_of_workers', 10)
        config_worker_type = glue_config.get('worker_type', 'G.2X')

        # Analyze patterns
        run_date = context.run_date
        day_type = self._get_day_type(run_date)
        scale_factor = self._get_scale_factor(run_date, allocation_config)

        # Calculate recommended workers
        base_workers = self._calculate_base_workers(total_size_gb)
        recommended_workers = max(2, int(base_workers * scale_factor))

        # Determine worker type based on memory needs
        recommended_type = self._recommend_worker_type(total_size_gb, recommended_workers)

        # Calculate memory and cost
        worker_spec = self.WORKER_SPECS.get(recommended_type, self.WORKER_SPECS['G.2X'])
        total_memory_gb = recommended_workers * worker_spec['memory_gb']
        estimated_cost_per_hour = recommended_workers * worker_spec['cost_per_hour']

        recommendations = []
        if recommended_workers != config_workers:
            recommendations.append(
                f"Adjust workers: {config_workers} → {recommended_workers} (based on {total_size_gb:.0f} GB data)"
            )
        if recommended_type != config_worker_type:
            recommendations.append(
                f"Adjust worker type: {config_worker_type} → {recommended_type}"
            )
        if day_type == 'weekend':
            recommendations.append("Weekend detected - reduced allocation applied")

        # Store allocation
        allocation_data = {
            'job_name': context.job_name,
            'run_date': run_date.isoformat(),
            'day_type': day_type,
            'total_size_gb': total_size_gb,
            'scale_factor': scale_factor,
            'recommended_workers': recommended_workers,
            'recommended_type': recommended_type,
            'config_workers': config_workers,
            'config_type': config_worker_type
        }

        self.storage.store_agent_data(
            self.AGENT_NAME,
            'allocations',
            [allocation_data],
            use_pipe_delimited=True
        )

        # Share with other agents
        context.set_shared('recommended_workers', recommended_workers)
        context.set_shared('recommended_worker_type', recommended_type)
        context.set_shared('total_memory_gb', total_memory_gb)

        return AgentResult(
            agent_name=self.AGENT_NAME,
            agent_id=self.agent_id,
            status=AgentStatus.COMPLETED,
            output={
                'recommended_workers': recommended_workers,
                'recommended_worker_type': recommended_type,
                'total_memory_gb': total_memory_gb,
                'estimated_cost_per_hour': estimated_cost_per_hour,
                'day_type': day_type,
                'scale_factor': scale_factor,
                'input_size_gb': total_size_gb
            },
            metrics={
                'workers': recommended_workers,
                'memory_gb': total_memory_gb,
                'cost_per_hour': estimated_cost_per_hour
            },
            recommendations=recommendations
        )

    def _get_day_type(self, run_date: datetime) -> str:
        """Determine day type (weekday, weekend, month_end, quarter_end)."""
        if run_date.weekday() >= 5:
            return 'weekend'

        # Check for month end (last 3 days)
        next_month = run_date.replace(day=28) + timedelta(days=4)
        last_day = next_month - timedelta(days=next_month.day)
        if (last_day - run_date).days <= 2:
            if run_date.month in [3, 6, 9, 12]:
                return 'quarter_end'
            return 'month_end'

        return 'weekday'

    def _get_scale_factor(self, run_date: datetime, config: Dict) -> float:
        """Get scale factor based on day type."""
        day_type = self._get_day_type(run_date)

        if day_type == 'weekend':
            return config.get('weekend_scale_factor', self.WEEKEND_SCALE)
        elif day_type == 'month_end':
            return config.get('month_end_scale_factor', self.MONTH_END_SCALE)
        elif day_type == 'quarter_end':
            return config.get('quarter_end_scale_factor', self.QUARTER_END_SCALE)

        return 1.0

    def _calculate_base_workers(self, size_gb: float) -> int:
        """Calculate base worker count from data size."""
        # Rule of thumb: 1 worker per 10-20 GB
        if size_gb < 10:
            return 2
        elif size_gb < 50:
            return 5
        elif size_gb < 100:
            return 10
        elif size_gb < 500:
            return 20
        else:
            return min(50, int(size_gb / 20))

    def _recommend_worker_type(self, size_gb: float, workers: int) -> str:
        """Recommend worker type based on memory needs."""
        memory_per_worker = size_gb / max(workers, 1) * 2  # 2x for processing headroom

        if memory_per_worker > 64:
            return 'G.8X'
        elif memory_per_worker > 32:
            return 'G.4X'
        elif memory_per_worker > 16:
            return 'G.2X'
        else:
            return 'G.1X'
