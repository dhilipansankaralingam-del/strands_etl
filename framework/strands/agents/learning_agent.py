#!/usr/bin/env python3
"""
Strands Learning Agent - Local ML Training and Model Management
===============================================================

Trains local ML models from execution history for:
- Resource prediction (workers, memory)
- Runtime estimation
- Cost prediction
- Anomaly detection

Features:
- Model versioning with unique IDs
- Training cost tracking
- Model persistence (local/S3)
- Model querying and inference
"""

import json
import hashlib
import pickle
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field

from ..base_agent import StrandsAgent, AgentResult, AgentStatus, AgentContext, register_agent
from ..storage import StrandsStorage


@dataclass
class ModelMetadata:
    """Metadata for a trained model."""
    model_id: str
    model_type: str
    version: str
    created_at: datetime
    training_data_size: int
    training_records: int
    training_time_seconds: float
    training_cost_usd: float
    features: List[str]
    target: str
    metrics: Dict[str, float] = field(default_factory=dict)
    hyperparameters: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'model_id': self.model_id,
            'model_type': self.model_type,
            'version': self.version,
            'created_at': self.created_at.isoformat(),
            'training_data_size': self.training_data_size,
            'training_records': self.training_records,
            'training_time_seconds': self.training_time_seconds,
            'training_cost_usd': self.training_cost_usd,
            'features': self.features,
            'target': self.target,
            'metrics': self.metrics,
            'hyperparameters': self.hyperparameters
        }


@dataclass
class SimpleLinearModel:
    """Simple linear regression model for resource prediction."""
    coefficients: Dict[str, float] = field(default_factory=dict)
    intercept: float = 0.0
    feature_means: Dict[str, float] = field(default_factory=dict)
    feature_stds: Dict[str, float] = field(default_factory=dict)

    def predict(self, features: Dict[str, float]) -> float:
        """Make a prediction using the learned coefficients."""
        prediction = self.intercept
        for feature, value in features.items():
            if feature in self.coefficients:
                # Normalize using stored means/stds
                mean = self.feature_means.get(feature, 0)
                std = self.feature_stds.get(feature, 1)
                normalized = (value - mean) / std if std > 0 else 0
                prediction += self.coefficients[feature] * normalized
        return max(0, prediction)


@register_agent
class StrandsLearningAgent(StrandsAgent):
    """
    Local ML training agent with model versioning and cost tracking.

    Trained Models:
    1. Resource Predictor - Predicts workers/memory from data size
    2. Runtime Estimator - Predicts job duration from config
    3. Cost Predictor - Predicts job cost from resources/runtime
    4. Anomaly Detector - Identifies unusual patterns
    """

    AGENT_NAME = "learning_agent"
    AGENT_VERSION = "2.1.0"
    AGENT_DESCRIPTION = "ML training with model versioning and cost tracking"

    DEPENDENCIES = []  # Can run in parallel
    PARALLEL_SAFE = True

    # Cost per training compute unit (simulated)
    TRAINING_COST_PER_RECORD = 0.00001  # $0.01 per 1000 records
    TRAINING_COST_PER_SECOND = 0.0005   # $0.50 per hour compute

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.storage = StrandsStorage(config)
        self.models_dir = Path(config.get('models_dir', 'data/models')) if config else Path('data/models')
        self.models_dir.mkdir(parents=True, exist_ok=True)

        # Model registry
        self._model_registry: Dict[str, ModelMetadata] = {}
        self._loaded_models: Dict[str, Any] = {}
        self._load_model_registry()

    def _load_model_registry(self) -> None:
        """Load model registry from disk."""
        registry_file = self.models_dir / 'model_registry.json'
        if registry_file.exists():
            try:
                with open(registry_file, 'r') as f:
                    data = json.load(f)
                    for model_id, meta in data.items():
                        meta['created_at'] = datetime.fromisoformat(meta['created_at'])
                        self._model_registry[model_id] = ModelMetadata(**meta)
            except Exception as e:
                self.logger.warning(f"Could not load model registry: {e}")

    def _save_model_registry(self) -> None:
        """Save model registry to disk."""
        registry_file = self.models_dir / 'model_registry.json'
        try:
            data = {k: v.to_dict() for k, v in self._model_registry.items()}
            with open(registry_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.warning(f"Could not save model registry: {e}")

    def _generate_model_id(self, model_type: str, features: List[str]) -> str:
        """Generate unique model ID based on type and features."""
        content = f"{model_type}:{':'.join(sorted(features))}:{datetime.utcnow().isoformat()}"
        hash_digest = hashlib.sha256(content.encode()).hexdigest()[:12]
        return f"{model_type[:3].upper()}-{hash_digest}"

    def execute(self, context: AgentContext) -> AgentResult:
        """Execute learning agent - collect data, train models, track costs."""
        learning_config = context.config.get('learning', {})

        if not self.is_enabled('learning.enabled'):
            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.COMPLETED,
                output={'skipped': True}
            )

        # Collect execution data
        execution_record = self._collect_execution_data(context)

        # Store execution history
        self.storage.store_agent_data(
            self.AGENT_NAME,
            'execution_history',
            [execution_record],
            use_pipe_delimited=True
        )

        # Load historical data for training
        historical_data = self._load_historical_data()
        historical_data.append(execution_record)

        # Save updated historical data
        self._save_historical_data(historical_data)

        # Train models if we have enough data
        trained_models: List[ModelMetadata] = []
        total_training_cost = 0.0
        total_training_time = 0.0

        min_records = learning_config.get('min_training_records', 5)

        if len(historical_data) >= min_records:
            # Train Resource Predictor
            resource_model, resource_meta = self._train_resource_predictor(historical_data)
            if resource_model and resource_meta:
                trained_models.append(resource_meta)
                total_training_cost += resource_meta.training_cost_usd
                total_training_time += resource_meta.training_time_seconds
                self._save_model(resource_meta.model_id, resource_model)

            # Train Runtime Estimator
            runtime_model, runtime_meta = self._train_runtime_estimator(historical_data)
            if runtime_model and runtime_meta:
                trained_models.append(runtime_meta)
                total_training_cost += runtime_meta.training_cost_usd
                total_training_time += runtime_meta.training_time_seconds
                self._save_model(runtime_meta.model_id, runtime_model)

            # Train Cost Predictor
            cost_model, cost_meta = self._train_cost_predictor(historical_data)
            if cost_model and cost_meta:
                trained_models.append(cost_meta)
                total_training_cost += cost_meta.training_cost_usd
                total_training_time += cost_meta.training_time_seconds
                self._save_model(cost_meta.model_id, cost_model)

        # Detect anomalies
        anomalies = []
        if learning_config.get('detect_anomalies') in ('Y', 'y', True):
            anomalies = self._detect_anomalies(execution_record, historical_data)

        # Analyze patterns
        patterns = self._analyze_patterns(context, historical_data)

        # Store training results
        training_summary = {
            'job_name': context.job_name,
            'execution_id': context.execution_id,
            'timestamp': datetime.utcnow().isoformat(),
            'models_trained': len(trained_models),
            'model_ids': [m.model_id for m in trained_models],
            'total_training_cost_usd': total_training_cost,
            'total_training_time_seconds': total_training_time,
            'historical_records_used': len(historical_data),
            'anomalies_detected': len(anomalies)
        }

        self.storage.store_agent_data(
            self.AGENT_NAME,
            'training_runs',
            [training_summary],
            use_pipe_delimited=True
        )

        # Store patterns
        if patterns:
            self.storage.store_agent_data(
                self.AGENT_NAME,
                'patterns',
                patterns,
                use_pipe_delimited=True
            )

        # Save registry
        self._save_model_registry()

        # Generate recommendations
        recommendations = []
        if trained_models:
            recommendations.append(
                f"Trained {len(trained_models)} models: {', '.join(m.model_id for m in trained_models)}"
            )
            recommendations.append(
                f"Training cost: ${total_training_cost:.4f} ({total_training_time:.2f}s)"
            )

        if anomalies:
            for anomaly in anomalies:
                recommendations.append(f"Anomaly: {anomaly['description']}")

        # Make predictions using trained models
        predictions = self._make_predictions(execution_record)
        if predictions:
            recommendations.append(f"Model predictions: {predictions}")

        # Share with other agents
        context.set_shared('execution_record', execution_record)
        context.set_shared('learned_patterns', patterns)
        context.set_shared('trained_models', [m.to_dict() for m in trained_models])
        context.set_shared('training_cost_usd', total_training_cost)

        return AgentResult(
            agent_name=self.AGENT_NAME,
            agent_id=self.agent_id,
            status=AgentStatus.COMPLETED,
            output={
                'record_stored': True,
                'models_trained': len(trained_models),
                'model_details': [m.to_dict() for m in trained_models],
                'total_training_cost_usd': total_training_cost,
                'total_training_time_seconds': total_training_time,
                'patterns_found': len(patterns),
                'anomalies_found': len(anomalies),
                'historical_records': len(historical_data),
                'predictions': predictions,
                'model_registry_size': len(self._model_registry)
            },
            metrics={
                'models_trained': len(trained_models),
                'training_cost_usd': total_training_cost,
                'training_time_seconds': total_training_time,
                'patterns_count': len(patterns),
                'anomalies_count': len(anomalies),
                'historical_records': len(historical_data)
            },
            recommendations=recommendations
        )

    def _collect_execution_data(self, context: AgentContext) -> Dict:
        """Collect execution data from context."""
        job_metrics = context.get_shared('job_metrics', {})
        job_execution = context.get_shared('job_execution', {})

        return {
            'job_name': context.job_name,
            'execution_id': context.execution_id,
            'run_date': context.run_date.isoformat(),
            'day_of_week': context.run_date.weekday(),
            'is_weekend': context.run_date.weekday() >= 5,
            'platform': context.platform,
            'total_size_gb': context.get_shared('total_size_gb', 0),
            'recommended_workers': context.get_shared('recommended_workers', 0),
            'recommended_worker_type': context.get_shared('recommended_worker_type', ''),
            'target_platform': context.get_shared('target_platform', 'glue'),
            'duration_seconds': job_metrics.get('duration_seconds', 0),
            'records_read': job_metrics.get('records_read', 0),
            'records_written': job_metrics.get('records_written', 0),
            'bytes_read': job_metrics.get('bytes_read', 0),
            'bytes_written': job_metrics.get('bytes_written', 0),
            'shuffle_bytes': job_metrics.get('shuffle_bytes', 0),
            'cost_usd': job_metrics.get('cost_usd', 0),
            'dq_summary': context.get_shared('dq_summary', {}),
            'compliance_findings_count': len(context.get_shared('compliance_findings', [])),
            'agent_results_count': len(context.agent_results),
            'timestamp': datetime.utcnow().isoformat()
        }

    def _load_historical_data(self) -> List[Dict]:
        """Load historical execution data for training."""
        history_file = self.models_dir / 'execution_history.json'

        if history_file.exists():
            try:
                with open(history_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"Could not load history: {e}")

        return []

    def _save_historical_data(self, data: List[Dict]) -> None:
        """Save historical execution data."""
        history_file = self.models_dir / 'execution_history.json'
        try:
            with open(history_file, 'w') as f:
                json.dump(data[-1000:], f)  # Keep last 1000 records
        except Exception as e:
            self.logger.warning(f"Could not save history: {e}")

    def _train_resource_predictor(self, data: List[Dict]) -> Tuple[Optional[SimpleLinearModel], Optional[ModelMetadata]]:
        """Train model to predict workers from data size."""
        start_time = datetime.utcnow()

        # Extract features and target
        features_list = []
        targets = []

        for record in data:
            size_gb = record.get('total_size_gb', 0)
            workers = record.get('recommended_workers', 0)
            if size_gb > 0 and workers > 0:
                features_list.append({
                    'size_gb': size_gb,
                    'is_weekend': 1 if record.get('is_weekend', False) else 0,
                    'day_of_week': record.get('day_of_week', 0)
                })
                targets.append(workers)

        if len(features_list) < 3:
            return None, None

        # Simple linear regression
        model = self._fit_linear_model(features_list, targets)

        training_time = (datetime.utcnow() - start_time).total_seconds()
        training_cost = (
            len(data) * self.TRAINING_COST_PER_RECORD +
            training_time * self.TRAINING_COST_PER_SECOND
        )

        # Evaluate model
        predictions = [model.predict(f) for f in features_list]
        mse = sum((p - t) ** 2 for p, t in zip(predictions, targets)) / len(targets)
        r2 = 1 - mse / max(1, sum((t - sum(targets)/len(targets)) ** 2 for t in targets) / len(targets))

        model_id = self._generate_model_id('resource_predictor', list(features_list[0].keys()))
        version = f"1.{len(self._model_registry)}.0"

        metadata = ModelMetadata(
            model_id=model_id,
            model_type='resource_predictor',
            version=version,
            created_at=datetime.utcnow(),
            training_data_size=sum(r.get('bytes_read', 0) for r in data),
            training_records=len(features_list),
            training_time_seconds=training_time,
            training_cost_usd=training_cost,
            features=list(features_list[0].keys()),
            target='recommended_workers',
            metrics={'mse': mse, 'r2': r2},
            hyperparameters={'learning_rate': 0.01, 'iterations': 100}
        )

        self._model_registry[model_id] = metadata
        self.logger.info(f"Trained resource predictor: {model_id} (R2={r2:.3f}, cost=${training_cost:.4f})")

        return model, metadata

    def _train_runtime_estimator(self, data: List[Dict]) -> Tuple[Optional[SimpleLinearModel], Optional[ModelMetadata]]:
        """Train model to predict runtime from size and workers."""
        start_time = datetime.utcnow()

        features_list = []
        targets = []

        for record in data:
            size_gb = record.get('total_size_gb', 0)
            workers = record.get('recommended_workers', 0)
            duration = record.get('duration_seconds', 0)
            if size_gb > 0 and workers > 0 and duration > 0:
                features_list.append({
                    'size_gb': size_gb,
                    'workers': workers,
                    'size_per_worker': size_gb / workers
                })
                targets.append(duration)

        if len(features_list) < 3:
            return None, None

        model = self._fit_linear_model(features_list, targets)

        training_time = (datetime.utcnow() - start_time).total_seconds()
        training_cost = (
            len(data) * self.TRAINING_COST_PER_RECORD +
            training_time * self.TRAINING_COST_PER_SECOND
        )

        predictions = [model.predict(f) for f in features_list]
        mse = sum((p - t) ** 2 for p, t in zip(predictions, targets)) / len(targets)
        mae = sum(abs(p - t) for p, t in zip(predictions, targets)) / len(targets)

        model_id = self._generate_model_id('runtime_estimator', list(features_list[0].keys()))
        version = f"1.{len(self._model_registry)}.0"

        metadata = ModelMetadata(
            model_id=model_id,
            model_type='runtime_estimator',
            version=version,
            created_at=datetime.utcnow(),
            training_data_size=sum(r.get('bytes_read', 0) for r in data),
            training_records=len(features_list),
            training_time_seconds=training_time,
            training_cost_usd=training_cost,
            features=list(features_list[0].keys()),
            target='duration_seconds',
            metrics={'mse': mse, 'mae': mae},
            hyperparameters={'learning_rate': 0.01, 'iterations': 100}
        )

        self._model_registry[model_id] = metadata
        self.logger.info(f"Trained runtime estimator: {model_id} (MAE={mae:.1f}s, cost=${training_cost:.4f})")

        return model, metadata

    def _train_cost_predictor(self, data: List[Dict]) -> Tuple[Optional[SimpleLinearModel], Optional[ModelMetadata]]:
        """Train model to predict cost from resources and runtime."""
        start_time = datetime.utcnow()

        features_list = []
        targets = []

        for record in data:
            workers = record.get('recommended_workers', 0)
            duration = record.get('duration_seconds', 0)
            cost = record.get('cost_usd', 0)
            if workers > 0 and duration > 0 and cost > 0:
                features_list.append({
                    'workers': workers,
                    'duration_hours': duration / 3600,
                    'worker_hours': workers * duration / 3600
                })
                targets.append(cost)

        if len(features_list) < 3:
            return None, None

        model = self._fit_linear_model(features_list, targets)

        training_time = (datetime.utcnow() - start_time).total_seconds()
        training_cost = (
            len(data) * self.TRAINING_COST_PER_RECORD +
            training_time * self.TRAINING_COST_PER_SECOND
        )

        predictions = [model.predict(f) for f in features_list]
        mse = sum((p - t) ** 2 for p, t in zip(predictions, targets)) / len(targets)
        mape = sum(abs(p - t) / max(t, 0.01) for p, t in zip(predictions, targets)) / len(targets) * 100

        model_id = self._generate_model_id('cost_predictor', list(features_list[0].keys()))
        version = f"1.{len(self._model_registry)}.0"

        metadata = ModelMetadata(
            model_id=model_id,
            model_type='cost_predictor',
            version=version,
            created_at=datetime.utcnow(),
            training_data_size=sum(r.get('bytes_read', 0) for r in data),
            training_records=len(features_list),
            training_time_seconds=training_time,
            training_cost_usd=training_cost,
            features=list(features_list[0].keys()),
            target='cost_usd',
            metrics={'mse': mse, 'mape_percent': mape},
            hyperparameters={'learning_rate': 0.01, 'iterations': 100}
        )

        self._model_registry[model_id] = metadata
        self.logger.info(f"Trained cost predictor: {model_id} (MAPE={mape:.1f}%, cost=${training_cost:.4f})")

        return model, metadata

    def _fit_linear_model(self, features_list: List[Dict], targets: List[float]) -> SimpleLinearModel:
        """Fit a simple linear regression model using gradient descent."""
        model = SimpleLinearModel()

        if not features_list:
            return model

        # Calculate feature statistics
        feature_names = list(features_list[0].keys())
        for fname in feature_names:
            values = [f[fname] for f in features_list]
            model.feature_means[fname] = sum(values) / len(values)
            variance = sum((v - model.feature_means[fname]) ** 2 for v in values) / len(values)
            model.feature_stds[fname] = variance ** 0.5 if variance > 0 else 1

        # Normalize features
        normalized = []
        for f in features_list:
            norm_f = {}
            for fname in feature_names:
                mean = model.feature_means[fname]
                std = model.feature_stds[fname]
                norm_f[fname] = (f[fname] - mean) / std if std > 0 else 0
            normalized.append(norm_f)

        # Initialize coefficients
        for fname in feature_names:
            model.coefficients[fname] = 0.0
        model.intercept = sum(targets) / len(targets)

        # Gradient descent
        learning_rate = 0.01
        iterations = 100

        for _ in range(iterations):
            # Compute predictions
            predictions = []
            for f in normalized:
                pred = model.intercept
                for fname in feature_names:
                    pred += model.coefficients[fname] * f[fname]
                predictions.append(pred)

            # Compute gradients
            n = len(targets)
            intercept_grad = sum(p - t for p, t in zip(predictions, targets)) / n

            coef_grads = {}
            for fname in feature_names:
                coef_grads[fname] = sum(
                    (p - t) * f[fname]
                    for p, t, f in zip(predictions, targets, normalized)
                ) / n

            # Update
            model.intercept -= learning_rate * intercept_grad
            for fname in feature_names:
                model.coefficients[fname] -= learning_rate * coef_grads[fname]

        return model

    def _save_model(self, model_id: str, model: SimpleLinearModel) -> None:
        """Save model to disk."""
        model_file = self.models_dir / f"{model_id}.pkl"
        try:
            with open(model_file, 'wb') as f:
                pickle.dump(model, f)
        except Exception as e:
            self.logger.warning(f"Could not save model {model_id}: {e}")

    def _load_model(self, model_id: str) -> Optional[SimpleLinearModel]:
        """Load model from disk."""
        if model_id in self._loaded_models:
            return self._loaded_models[model_id]

        model_file = self.models_dir / f"{model_id}.pkl"
        if model_file.exists():
            try:
                with open(model_file, 'rb') as f:
                    model = pickle.load(f)
                    self._loaded_models[model_id] = model
                    return model
            except Exception as e:
                self.logger.warning(f"Could not load model {model_id}: {e}")
        return None

    def _make_predictions(self, current_record: Dict) -> Dict[str, Any]:
        """Make predictions using trained models."""
        predictions = {}

        # Find latest models of each type
        for model_id, meta in self._model_registry.items():
            model = self._load_model(model_id)
            if not model:
                continue

            try:
                if meta.model_type == 'resource_predictor':
                    features = {
                        'size_gb': current_record.get('total_size_gb', 0),
                        'is_weekend': 1 if current_record.get('is_weekend', False) else 0,
                        'day_of_week': current_record.get('day_of_week', 0)
                    }
                    predictions['predicted_workers'] = round(model.predict(features))
                    predictions['resource_model_id'] = model_id

                elif meta.model_type == 'runtime_estimator':
                    size_gb = current_record.get('total_size_gb', 0)
                    workers = current_record.get('recommended_workers', 10)
                    features = {
                        'size_gb': size_gb,
                        'workers': workers,
                        'size_per_worker': size_gb / max(workers, 1)
                    }
                    predictions['predicted_runtime_seconds'] = round(model.predict(features))
                    predictions['runtime_model_id'] = model_id

                elif meta.model_type == 'cost_predictor':
                    workers = current_record.get('recommended_workers', 10)
                    duration = current_record.get('duration_seconds', 0) or predictions.get('predicted_runtime_seconds', 3600)
                    features = {
                        'workers': workers,
                        'duration_hours': duration / 3600,
                        'worker_hours': workers * duration / 3600
                    }
                    predictions['predicted_cost_usd'] = round(model.predict(features), 2)
                    predictions['cost_model_id'] = model_id

            except Exception as e:
                self.logger.warning(f"Prediction failed for {model_id}: {e}")

        return predictions

    def _analyze_patterns(self, context: AgentContext, historical_data: List[Dict]) -> List[Dict]:
        """Analyze patterns from historical data."""
        patterns = []

        total_size_gb = context.get_shared('total_size_gb', 0)
        if total_size_gb > 0:
            patterns.append({
                'type': 'size_pattern',
                'job_name': context.job_name,
                'run_date': context.run_date.strftime('%A'),
                'size_gb': total_size_gb,
                'is_weekend': context.run_date.weekday() >= 5
            })

        workers = context.get_shared('recommended_workers', 0)
        if workers > 0:
            patterns.append({
                'type': 'worker_pattern',
                'job_name': context.job_name,
                'workers': workers,
                'size_gb': total_size_gb,
                'ratio': total_size_gb / workers if workers > 0 else 0
            })

        # Historical trend analysis
        if len(historical_data) >= 5:
            recent_sizes = [r.get('total_size_gb', 0) for r in historical_data[-5:]]
            avg_recent = sum(recent_sizes) / len(recent_sizes)

            if total_size_gb > avg_recent * 1.5:
                patterns.append({
                    'type': 'growth_trend',
                    'job_name': context.job_name,
                    'current_size_gb': total_size_gb,
                    'avg_recent_size_gb': avg_recent,
                    'growth_percent': (total_size_gb - avg_recent) / avg_recent * 100
                })

        return patterns

    def _detect_anomalies(self, current: Dict, historical_data: List[Dict]) -> List[Dict]:
        """Detect anomalies compared to historical data."""
        anomalies = []

        size_gb = current.get('total_size_gb', 0)

        if size_gb > 1000:
            anomalies.append({
                'type': 'size_anomaly',
                'description': f'Unusually large data size: {size_gb:.0f} GB',
                'severity': 'warning'
            })

        if len(historical_data) >= 5:
            recent_sizes = [r.get('total_size_gb', 0) for r in historical_data[-5:]]
            avg = sum(recent_sizes) / len(recent_sizes)
            std = (sum((s - avg) ** 2 for s in recent_sizes) / len(recent_sizes)) ** 0.5

            if std > 0 and abs(size_gb - avg) > 3 * std:
                anomalies.append({
                    'type': 'statistical_anomaly',
                    'description': f'Size {size_gb:.0f} GB is {abs(size_gb - avg) / std:.1f} std devs from mean',
                    'severity': 'high'
                })

        return anomalies

    # Public methods for querying models
    def list_models(self) -> List[Dict]:
        """List all trained models."""
        return [m.to_dict() for m in self._model_registry.values()]

    def get_model(self, model_id: str) -> Optional[Dict]:
        """Get model details by ID."""
        if model_id in self._model_registry:
            return self._model_registry[model_id].to_dict()
        return None

    def get_training_costs(self) -> Dict[str, Any]:
        """Get total training costs across all models."""
        total_cost = sum(m.training_cost_usd for m in self._model_registry.values())
        total_time = sum(m.training_time_seconds for m in self._model_registry.values())
        total_records = sum(m.training_records for m in self._model_registry.values())

        return {
            'total_models': len(self._model_registry),
            'total_training_cost_usd': total_cost,
            'total_training_time_seconds': total_time,
            'total_training_records': total_records,
            'by_model': {
                m.model_id: {
                    'type': m.model_type,
                    'cost_usd': m.training_cost_usd,
                    'time_seconds': m.training_time_seconds
                }
                for m in self._model_registry.values()
            }
        }
