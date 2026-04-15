#!/usr/bin/env python3
"""Strands Resource Allocator Agent - Dynamic resource allocation."""

import json
import pickle
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from ..base_agent import StrandsAgent, AgentResult, AgentStatus, AgentContext, register_agent
from ..storage import StrandsStorage


@register_agent
class StrandsResourceAllocatorAgent(StrandsAgent):
    """Dynamically allocates resources based on data size, complexity, patterns, and history."""

    AGENT_NAME = "resource_allocator_agent"
    AGENT_VERSION = "2.1.0"
    AGENT_DESCRIPTION = "Smart resource allocation based on size, complexity, and patterns"

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
        self.models_dir = Path(
            config.get('models_dir', 'data/models') if config else 'data/models'
        )

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

        # ============================================================
        # DECISION: force_from_config = Y → user controls resources
        #           force_from_config = N → agent decides
        # ============================================================
        force_from_config = allocation_config.get('force_from_config', 'N') in ('Y', 'y', True)

        if force_from_config:
            self.logger.info("force_from_config=Y: Using glue_config values (user-forced)")
            recommended_workers = config_workers
            recommended_type = config_worker_type
            day_type = self._get_day_type(context.run_date)
            scale_factor = 1.0
            complexity_factor = 1.0

            worker_spec = self.WORKER_SPECS.get(recommended_type, self.WORKER_SPECS['G.2X'])
            total_memory_gb = recommended_workers * worker_spec['memory_gb']
            estimated_cost_per_hour = recommended_workers * worker_spec['cost_per_hour']

            context.set_shared('recommended_workers', recommended_workers)
            context.set_shared('recommended_worker_type', recommended_type)
            context.set_shared('total_memory_gb', total_memory_gb)

            self.storage.store_agent_data(
                self.AGENT_NAME, 'allocations',
                [{'job_name': context.job_name, 'run_date': context.run_date.isoformat(),
                  'force_from_config': True, 'workers': recommended_workers,
                  'worker_type': recommended_type, 'total_size_gb': total_size_gb}],
                use_pipe_delimited=True
            )

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
                    'complexity_factor': complexity_factor,
                    'input_size_gb': total_size_gb,
                    'force_from_config': True
                },
                metrics={
                    'workers': recommended_workers,
                    'memory_gb': total_memory_gb,
                    'cost_per_hour': estimated_cost_per_hour
                },
                recommendations=[
                    f"User-forced: {recommended_workers} x {recommended_type} from glue_config"
                ]
            )

        # ============================================================
        # AGENT-DECIDED ALLOCATION (force_from_config = N)
        # Factors: data volume + complexity + day-type + learning predictions
        # ============================================================
        self.logger.info("force_from_config=N: Agent deciding resources based on size + complexity + day-type + learning")

        run_date = context.run_date
        day_type = self._get_day_type(run_date)
        scale_factor = self._get_scale_factor(run_date, allocation_config)
        complexity_factor = self._calculate_complexity_factor(context)

        # Calculate formula-based workers = base(size) * day_scale * complexity
        base_workers = self._calculate_base_workers(total_size_gb)
        formula_workers = max(2, int(base_workers * scale_factor * complexity_factor))

        # Try to load job-specific learning prediction
        learn_features = {
            'total_size_gb': total_size_gb,
            'source_table_count': len(context.config.get('source_tables', [])),
            'complexity_factor': complexity_factor,
            'scale_factor': scale_factor,
            'day_of_week': run_date.weekday(),
            'is_month_end': 1.0 if day_type in ('month_end', 'quarter_end') else 0.0,
        }
        learned_workers, learn_confidence, learn_model_id = self._load_learning_prediction(
            context.job_name, learn_features
        )

        # Blend formula and learning prediction based on confidence
        # Low confidence (<30%) → use formula; high confidence (>70%) → trust learning
        if learned_workers and learn_confidence > 0.0:
            blend_weight = learn_confidence  # 0..0.9
            recommended_workers = max(2, int(
                blend_weight * learned_workers + (1 - blend_weight) * formula_workers
            ))
        else:
            recommended_workers = formula_workers
            learn_model_id = None

        # Determine worker type based on memory needs
        recommended_type = self._recommend_worker_type(total_size_gb, recommended_workers)

        # Calculate memory and cost
        worker_spec = self.WORKER_SPECS.get(recommended_type, self.WORKER_SPECS['G.2X'])
        total_memory_gb = recommended_workers * worker_spec['memory_gb']
        estimated_cost_per_hour = recommended_workers * worker_spec['cost_per_hour']

        recommendations = []
        if recommended_workers != config_workers:
            recommendations.append(
                f"Adjust workers: {config_workers} → {recommended_workers} "
                f"(size={total_size_gb:.0f}GB, complexity={complexity_factor:.1f}x, day={day_type})"
            )
        if recommended_type != config_worker_type:
            recommendations.append(
                f"Adjust worker type: {config_worker_type} → {recommended_type}"
            )
        if day_type == 'weekend':
            recommendations.append("Weekend detected - reduced allocation applied")
        if complexity_factor > 1.2:
            recommendations.append(f"High complexity detected ({complexity_factor:.1f}x) - extra resources allocated")
        if learned_workers and learn_confidence > 0.0:
            recommendations.append(
                f"Learning model {learn_model_id} prediction: {learned_workers} workers "
                f"(confidence={learn_confidence:.0%}); formula: {formula_workers} workers; "
                f"blended: {recommended_workers} workers"
            )
        else:
            recommendations.append(
                f"No learning model found for job '{context.job_name}' — "
                f"using formula-based allocation ({formula_workers} workers). "
                "Run more jobs to train the model."
            )

        # Store allocation
        allocation_data = {
            'job_name': context.job_name,
            'run_date': run_date.isoformat(),
            'force_from_config': False,
            'day_type': day_type,
            'total_size_gb': total_size_gb,
            'scale_factor': scale_factor,
            'complexity_factor': complexity_factor,
            'formula_workers': formula_workers,
            'learned_workers': learned_workers,
            'learn_confidence': learn_confidence,
            'learn_model_id': learn_model_id,
            'recommended_workers': recommended_workers,
            'recommended_type': recommended_type,
            'config_workers': config_workers,
            'config_type': config_worker_type
        }

        self.storage.store_agent_data(
            self.AGENT_NAME, 'allocations',
            [allocation_data], use_pipe_delimited=True
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
                'complexity_factor': complexity_factor,
                'formula_workers': formula_workers,
                'learned_workers': learned_workers,
                'learn_confidence': learn_confidence,
                'learn_model_id': learn_model_id,
                'input_size_gb': total_size_gb,
                'force_from_config': False
            },
            metrics={
                'workers': recommended_workers,
                'memory_gb': total_memory_gb,
                'cost_per_hour': estimated_cost_per_hour
            },
            recommendations=recommendations
        )

    def _load_learning_prediction(
        self,
        job_name: str,
        features: Dict[str, float]
    ) -> Tuple[Optional[int], float, str]:
        """Load job-specific learning model and return predicted worker count.

        Returns:
            (predicted_workers, confidence, model_id)
            Returns (None, 0.0, '') if no model exists for this job.
        """
        try:
            # Sanitize job name (same logic as learning agent)
            safe_name = "".join(
                c if c.isalnum() or c in "._-" else "_" for c in job_name
            )
            job_dir = self.models_dir / safe_name
            registry_file = job_dir / 'model_registry.json'

            if not registry_file.exists():
                return None, 0.0, ''

            with open(registry_file, 'r') as f:
                registry = json.load(f)

            # Find the most recent resource_predictor model
            resource_models = [
                (mid, meta)
                for mid, meta in registry.items()
                if meta.get('model_type') == 'resource_predictor'
            ]

            if not resource_models:
                return None, 0.0, ''

            # Pick most recent by created_at
            resource_models.sort(key=lambda x: x[1].get('created_at', ''), reverse=True)
            model_id, model_meta = resource_models[0]

            # Load the pickle file
            model_file = job_dir / f'{model_id}.pkl'
            if not model_file.exists():
                return None, 0.0, model_id

            with open(model_file, 'rb') as f:
                model = pickle.load(f)

            # Use only features the model was trained on
            trained_features = model_meta.get('features', [])
            predict_input = {
                k: features.get(k, 0.0)
                for k in trained_features
            }

            predicted = model.predict(predict_input)
            predicted_workers = max(2, int(round(predicted)))

            # Confidence based on training records: 0% below 5, ramps to 90% at 50+
            training_records = model_meta.get('training_records', 0)
            confidence = min(0.9, max(0.0, (training_records - 5) / 50.0))

            self.logger.info(
                f"Learning model {model_id} predicts {predicted_workers} workers "
                f"(confidence={confidence:.0%}, trained on {training_records} runs)"
            )
            return predicted_workers, confidence, model_id

        except Exception as e:
            self.logger.warning(f"Could not load learning prediction for {job_name}: {e}")
            return None, 0.0, ''

    def _calculate_complexity_factor(self, context: AgentContext) -> float:
        """
        Calculate complexity factor from code analysis and config.

        Complexity is determined by:
        - Number of JOINs in the script
        - Window functions
        - Aggregations (GROUP BY, DISTINCT)
        - Number of source tables
        """
        complexity_config = context.config.get('smart_allocation', {}).get('complexity_factors', {})
        code_analysis = context.get_shared('code_analysis', {})

        factor = 1.0

        # Source table count drives complexity
        source_count = len(context.config.get('source_tables', []))
        if source_count > 15:
            factor *= 1.5
        elif source_count > 10:
            factor *= 1.3
        elif source_count > 5:
            factor *= 1.1

        # From code analysis agent output (if available)
        join_count = code_analysis.get('join_count', 0)
        window_count = code_analysis.get('window_function_count', 0)
        agg_count = code_analysis.get('aggregation_count', 0)

        if join_count > 10:
            factor *= complexity_config.get('join_count_weight', 1.5)
        elif join_count > 5:
            factor *= 1.2

        if window_count > 3:
            factor *= complexity_config.get('window_function_weight', 2.0)
        elif window_count > 0:
            factor *= 1.3

        if agg_count > 5:
            factor *= complexity_config.get('aggregation_weight', 1.2)

        return round(min(factor, 3.0), 2)  # Cap at 3x

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
