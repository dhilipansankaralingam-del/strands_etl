"""
Strands ETL Learning Module
===========================
A dedicated module for capturing, storing, analyzing, and learning from ETL execution patterns.

Features:
- Store learning vectors from each execution
- Analyze historical patterns
- Provide insights on demand
- Train on historical data for better recommendations
- Query similar workloads
"""

import json
import boto3
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
import statistics
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime and Decimal objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class LearningModule:
    """
    Learning Module for Strands ETL Framework.

    Captures execution patterns, analyzes historical data, and provides
    intelligent insights for future ETL runs.
    """

    def __init__(self, bucket_name: str = 'strands-etl-learning', region: str = None):
        """
        Initialize the Learning Module.

        Args:
            bucket_name: S3 bucket for storing learning data
            region: AWS region
        """
        self.bucket_name = bucket_name
        self.region = region
        client_kwargs = {'region_name': region} if region else {}

        self.s3 = boto3.client('s3', **client_kwargs)
        self.bedrock = boto3.client('bedrock-runtime', **client_kwargs)

        # Prefixes for different data types
        self.VECTORS_PREFIX = 'learning/vectors/'
        self.INSIGHTS_PREFIX = 'learning/insights/'
        self.TRAINED_MODELS_PREFIX = 'learning/trained/'
        self.METRICS_PREFIX = 'learning/metrics/'

    # =========================================================================
    # CAPTURE - Store execution data
    # =========================================================================

    def capture_execution(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Capture and store execution data as a learning vector.

        Args:
            context: Execution context containing all pipeline data

        Returns:
            Learning vector that was stored
        """
        execution_result = context.get('execution_result', {})
        quality_report = context.get('quality_report', {})
        optimization = context.get('optimization', {})

        # Calculate execution duration
        execution_time_seconds = self._calculate_execution_time(context)

        # Extract real metrics from job run details
        job_metrics = self._extract_job_metrics(execution_result)

        # Create comprehensive learning vector
        learning_vector = {
            'vector_id': str(uuid.uuid4()),
            'timestamp': datetime.utcnow().isoformat(),
            'pipeline_id': context.get('pipeline_id'),

            # Workload characteristics
            'workload': {
                'name': context.get('config', {}).get('workload', {}).get('name'),
                'data_volume': context.get('config', {}).get('workload', {}).get('data_volume'),
                'complexity': context.get('config', {}).get('workload', {}).get('complexity'),
                'criticality': context.get('config', {}).get('workload', {}).get('criticality'),
                'time_sensitivity': context.get('config', {}).get('workload', {}).get('time_sensitivity')
            },

            # Execution details
            'execution': {
                'platform': execution_result.get('platform'),
                'execution_type': execution_result.get('execution_type'),
                'job_name': execution_result.get('job_name'),
                'status': execution_result.get('status'),
                'final_status': execution_result.get('final_status'),
                'error': execution_result.get('error')
            },

            # Performance metrics
            'metrics': {
                'execution_time_seconds': execution_time_seconds,
                'dpu_seconds': job_metrics.get('dpu_seconds'),
                'max_capacity': job_metrics.get('max_capacity'),
                'number_of_workers': job_metrics.get('number_of_workers'),
                'estimated_cost_usd': self._estimate_cost(job_metrics),
                'records_processed': job_metrics.get('records_processed', 0)
            },

            # Quality scores
            'quality': {
                'overall_score': quality_report.get('overall_score', 0.0),
                'data_quality_score': quality_report.get('data_quality_score', 0.0),
                'completeness_score': quality_report.get('completeness_score', 0.0)
            },

            # Optimization data
            'optimization': {
                'efficiency_score': optimization.get('efficiency_score', 0.0),
                'cost_efficiency': optimization.get('cost_efficiency', 0.0),
                'recommendations_applied': optimization.get('recommendations_applied', [])
            },

            # Agent decisions
            'agent_decisions': {
                'mode': context.get('agent_mode'),
                'selected_platform': context.get('selected_platform'),
                'platform_recommendations': context.get('platform_decision', {}).get('recommendations', [])
            }
        }

        # Store the learning vector
        self._store_vector(learning_vector)

        logger.info(f"Captured learning vector: {learning_vector['vector_id']}")
        return learning_vector

    def _calculate_execution_time(self, context: Dict[str, Any]) -> Optional[float]:
        """Calculate execution time from context."""
        try:
            start_time = context.get('start_time')
            end_time = context.get('end_time')

            if start_time and end_time:
                start = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                end = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                return (end - start).total_seconds()
        except Exception as e:
            logger.warning(f"Could not calculate execution time: {e}")
        return None

    def _extract_job_metrics(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metrics from job run details."""
        job_run = execution_result.get('job_run_details', {})

        return {
            'dpu_seconds': job_run.get('DPUSeconds'),
            'max_capacity': job_run.get('MaxCapacity'),
            'number_of_workers': job_run.get('NumberOfWorkers'),
            'execution_time': job_run.get('ExecutionTime'),
            'records_processed': job_run.get('RecordsProcessed', 0)
        }

    def _estimate_cost(self, metrics: Dict[str, Any]) -> Optional[float]:
        """Estimate cost based on metrics."""
        dpu_seconds = metrics.get('dpu_seconds')
        if dpu_seconds:
            # Glue pricing: ~$0.44 per DPU-hour
            dpu_hours = dpu_seconds / 3600
            return round(dpu_hours * 0.44, 4)
        return None

    def _store_vector(self, vector: Dict[str, Any]) -> None:
        """Store learning vector to S3."""
        try:
            key = f"{self.VECTORS_PREFIX}{vector['vector_id']}.json"
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(vector, indent=2, cls=DateTimeEncoder),
                ContentType='application/json'
            )
        except Exception as e:
            logger.error(f"Failed to store learning vector: {e}")

    # =========================================================================
    # RETRIEVE - Get historical data
    # =========================================================================

    def get_all_vectors(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieve all learning vectors."""
        vectors = []
        try:
            response = self.s3.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.VECTORS_PREFIX,
                MaxKeys=limit
            )

            for obj in response.get('Contents', []):
                try:
                    data = self.s3.get_object(Bucket=self.bucket_name, Key=obj['Key'])
                    vector = json.loads(data['Body'].read().decode('utf-8'))
                    vectors.append(vector)
                except Exception as e:
                    logger.warning(f"Failed to load vector {obj['Key']}: {e}")

            # Sort by timestamp (most recent first)
            vectors.sort(key=lambda x: x.get('timestamp', ''), reverse=True)

        except Exception as e:
            logger.error(f"Failed to retrieve vectors: {e}")

        return vectors

    def get_vectors_by_workload(self, workload_name: str) -> List[Dict[str, Any]]:
        """Get all vectors for a specific workload."""
        all_vectors = self.get_all_vectors()
        return [v for v in all_vectors if v.get('workload', {}).get('name') == workload_name]

    def get_vectors_by_platform(self, platform: str) -> List[Dict[str, Any]]:
        """Get all vectors for a specific platform."""
        all_vectors = self.get_all_vectors()
        return [v for v in all_vectors if v.get('execution', {}).get('platform') == platform]

    def get_similar_workloads(self, workload: Dict[str, Any], top_n: int = 5) -> List[Dict[str, Any]]:
        """
        Find similar workloads based on characteristics.

        Args:
            workload: Current workload characteristics
            top_n: Number of similar workloads to return

        Returns:
            List of similar historical vectors with similarity scores
        """
        all_vectors = self.get_all_vectors()
        scored_vectors = []

        for vector in all_vectors:
            score = self._calculate_similarity(workload, vector.get('workload', {}))
            if score > 0:
                scored_vectors.append({
                    'vector': vector,
                    'similarity_score': score
                })

        # Sort by similarity score
        scored_vectors.sort(key=lambda x: x['similarity_score'], reverse=True)

        return scored_vectors[:top_n]

    def _calculate_similarity(self, workload1: Dict[str, Any], workload2: Dict[str, Any]) -> float:
        """Calculate similarity score between two workloads."""
        score = 0.0
        total_weight = 0.0

        # Weights for different characteristics
        weights = {
            'data_volume': 0.3,
            'complexity': 0.3,
            'criticality': 0.2,
            'time_sensitivity': 0.2
        }

        for key, weight in weights.items():
            total_weight += weight
            if workload1.get(key) == workload2.get(key):
                score += weight

        return score / total_weight if total_weight > 0 else 0.0

    # =========================================================================
    # ANALYZE - Generate insights from data
    # =========================================================================

    def analyze_patterns(self) -> Dict[str, Any]:
        """
        Analyze all historical data and identify patterns.

        Returns:
            Comprehensive analysis report
        """
        vectors = self.get_all_vectors()

        if not vectors:
            return {'error': 'No historical data available for analysis'}

        analysis = {
            'generated_at': datetime.utcnow().isoformat(),
            'total_executions': len(vectors),
            'summary': self._generate_summary(vectors),
            'platform_analysis': self._analyze_platforms(vectors),
            'workload_analysis': self._analyze_workloads(vectors),
            'performance_trends': self._analyze_performance_trends(vectors),
            'cost_analysis': self._analyze_costs(vectors),
            'failure_analysis': self._analyze_failures(vectors),
            'recommendations': self._generate_recommendations(vectors)
        }

        # Store analysis
        self._store_insight('pattern_analysis', analysis)

        return analysis

    def _generate_summary(self, vectors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate summary statistics."""
        successful = [v for v in vectors if v.get('execution', {}).get('status') == 'completed']
        failed = [v for v in vectors if v.get('execution', {}).get('status') == 'failed']

        execution_times = [
            v.get('metrics', {}).get('execution_time_seconds')
            for v in vectors
            if v.get('metrics', {}).get('execution_time_seconds')
        ]

        costs = [
            v.get('metrics', {}).get('estimated_cost_usd')
            for v in vectors
            if v.get('metrics', {}).get('estimated_cost_usd')
        ]

        return {
            'total_runs': len(vectors),
            'successful_runs': len(successful),
            'failed_runs': len(failed),
            'success_rate': len(successful) / len(vectors) if vectors else 0,
            'avg_execution_time_seconds': statistics.mean(execution_times) if execution_times else None,
            'total_estimated_cost_usd': sum(costs) if costs else None,
            'avg_cost_per_run_usd': statistics.mean(costs) if costs else None
        }

    def _analyze_platforms(self, vectors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze performance by platform."""
        platforms = {}

        for vector in vectors:
            platform = vector.get('execution', {}).get('platform', 'unknown')
            if platform not in platforms:
                platforms[platform] = {
                    'count': 0,
                    'successful': 0,
                    'failed': 0,
                    'execution_times': [],
                    'costs': []
                }

            platforms[platform]['count'] += 1

            status = vector.get('execution', {}).get('status')
            if status == 'completed':
                platforms[platform]['successful'] += 1
            elif status == 'failed':
                platforms[platform]['failed'] += 1

            exec_time = vector.get('metrics', {}).get('execution_time_seconds')
            if exec_time:
                platforms[platform]['execution_times'].append(exec_time)

            cost = vector.get('metrics', {}).get('estimated_cost_usd')
            if cost:
                platforms[platform]['costs'].append(cost)

        # Calculate statistics for each platform
        for platform, data in platforms.items():
            data['success_rate'] = data['successful'] / data['count'] if data['count'] > 0 else 0
            data['avg_execution_time'] = statistics.mean(data['execution_times']) if data['execution_times'] else None
            data['avg_cost'] = statistics.mean(data['costs']) if data['costs'] else None
            # Remove raw lists to clean up output
            del data['execution_times']
            del data['costs']

        return platforms

    def _analyze_workloads(self, vectors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze patterns by workload type."""
        workloads = {}

        for vector in vectors:
            name = vector.get('workload', {}).get('name', 'unknown')
            if name not in workloads:
                workloads[name] = {
                    'count': 0,
                    'platforms_used': {},
                    'avg_execution_time': [],
                    'data_volumes': []
                }

            workloads[name]['count'] += 1

            platform = vector.get('execution', {}).get('platform', 'unknown')
            workloads[name]['platforms_used'][platform] = workloads[name]['platforms_used'].get(platform, 0) + 1

            exec_time = vector.get('metrics', {}).get('execution_time_seconds')
            if exec_time:
                workloads[name]['avg_execution_time'].append(exec_time)

            data_vol = vector.get('workload', {}).get('data_volume')
            if data_vol:
                workloads[name]['data_volumes'].append(data_vol)

        # Calculate statistics
        for name, data in workloads.items():
            data['avg_execution_time'] = statistics.mean(data['avg_execution_time']) if data['avg_execution_time'] else None
            data['most_used_platform'] = max(data['platforms_used'], key=data['platforms_used'].get) if data['platforms_used'] else None
            del data['data_volumes']

        return workloads

    def _analyze_performance_trends(self, vectors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze performance trends over time."""
        # Sort by timestamp
        sorted_vectors = sorted(vectors, key=lambda x: x.get('timestamp', ''))

        if len(sorted_vectors) < 2:
            return {'message': 'Not enough data for trend analysis'}

        # Split into two halves for comparison
        mid = len(sorted_vectors) // 2
        first_half = sorted_vectors[:mid]
        second_half = sorted_vectors[mid:]

        def calc_avg(vecs, key_path):
            values = []
            for v in vecs:
                val = v
                for key in key_path:
                    val = val.get(key, {}) if isinstance(val, dict) else None
                    if val is None:
                        break
                if val is not None:
                    values.append(val)
            return statistics.mean(values) if values else None

        first_avg_time = calc_avg(first_half, ['metrics', 'execution_time_seconds'])
        second_avg_time = calc_avg(second_half, ['metrics', 'execution_time_seconds'])

        first_avg_cost = calc_avg(first_half, ['metrics', 'estimated_cost_usd'])
        second_avg_cost = calc_avg(second_half, ['metrics', 'estimated_cost_usd'])

        return {
            'period_1': {
                'count': len(first_half),
                'avg_execution_time': first_avg_time,
                'avg_cost': first_avg_cost
            },
            'period_2': {
                'count': len(second_half),
                'avg_execution_time': second_avg_time,
                'avg_cost': second_avg_cost
            },
            'trends': {
                'execution_time_trend': 'improving' if second_avg_time and first_avg_time and second_avg_time < first_avg_time else 'declining' if second_avg_time and first_avg_time else 'unknown',
                'cost_trend': 'improving' if second_avg_cost and first_avg_cost and second_avg_cost < first_avg_cost else 'increasing' if second_avg_cost and first_avg_cost else 'unknown'
            }
        }

    def _analyze_costs(self, vectors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze cost patterns."""
        costs_by_platform = {}
        costs_by_volume = {'low': [], 'medium': [], 'high': []}

        for vector in vectors:
            cost = vector.get('metrics', {}).get('estimated_cost_usd')
            if not cost:
                continue

            platform = vector.get('execution', {}).get('platform', 'unknown')
            if platform not in costs_by_platform:
                costs_by_platform[platform] = []
            costs_by_platform[platform].append(cost)

            volume = vector.get('workload', {}).get('data_volume', 'medium')
            if volume in costs_by_volume:
                costs_by_volume[volume].append(cost)

        return {
            'by_platform': {
                platform: {
                    'avg_cost': statistics.mean(costs),
                    'min_cost': min(costs),
                    'max_cost': max(costs),
                    'total_cost': sum(costs)
                }
                for platform, costs in costs_by_platform.items()
                if costs
            },
            'by_data_volume': {
                volume: statistics.mean(costs) if costs else None
                for volume, costs in costs_by_volume.items()
            }
        }

    def _analyze_failures(self, vectors: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze failure patterns."""
        failed = [v for v in vectors if v.get('execution', {}).get('status') == 'failed']

        if not failed:
            return {'message': 'No failures recorded'}

        failure_reasons = {}
        failures_by_platform = {}

        for vector in failed:
            error = vector.get('execution', {}).get('error', 'Unknown error')
            failure_reasons[error] = failure_reasons.get(error, 0) + 1

            platform = vector.get('execution', {}).get('platform', 'unknown')
            failures_by_platform[platform] = failures_by_platform.get(platform, 0) + 1

        return {
            'total_failures': len(failed),
            'failure_rate': len(failed) / len(vectors) if vectors else 0,
            'top_failure_reasons': dict(sorted(failure_reasons.items(), key=lambda x: x[1], reverse=True)[:5]),
            'failures_by_platform': failures_by_platform
        }

    def _generate_recommendations(self, vectors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate recommendations based on analysis."""
        recommendations = []

        platform_analysis = self._analyze_platforms(vectors)
        cost_analysis = self._analyze_costs(vectors)
        failure_analysis = self._analyze_failures(vectors)

        # Platform recommendations
        best_platform = None
        best_success_rate = 0
        for platform, data in platform_analysis.items():
            if data.get('success_rate', 0) > best_success_rate:
                best_success_rate = data['success_rate']
                best_platform = platform

        if best_platform:
            recommendations.append({
                'type': 'platform',
                'recommendation': f"Use {best_platform} for highest success rate ({best_success_rate:.1%})",
                'confidence': best_success_rate
            })

        # Cost recommendations
        if cost_analysis.get('by_platform'):
            cheapest_platform = min(
                cost_analysis['by_platform'].items(),
                key=lambda x: x[1].get('avg_cost', float('inf'))
            )
            recommendations.append({
                'type': 'cost',
                'recommendation': f"{cheapest_platform[0]} has lowest average cost (${cheapest_platform[1]['avg_cost']:.2f})",
                'confidence': 0.8
            })

        # Failure prevention
        if failure_analysis.get('top_failure_reasons'):
            top_reason = list(failure_analysis['top_failure_reasons'].keys())[0]
            recommendations.append({
                'type': 'reliability',
                'recommendation': f"Most common failure: {top_reason[:100]}... - review and address",
                'confidence': 0.9
            })

        return recommendations

    def _store_insight(self, insight_type: str, data: Dict[str, Any]) -> None:
        """Store generated insight."""
        try:
            key = f"{self.INSIGHTS_PREFIX}{insight_type}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(data, indent=2, cls=DateTimeEncoder),
                ContentType='application/json'
            )
        except Exception as e:
            logger.error(f"Failed to store insight: {e}")

    # =========================================================================
    # TRAIN - Build prediction models
    # =========================================================================

    def train_platform_predictor(self) -> Dict[str, Any]:
        """
        Train a simple model to predict optimal platform based on workload characteristics.

        Returns:
            Training results and model statistics
        """
        vectors = self.get_all_vectors()
        successful = [v for v in vectors if v.get('execution', {}).get('status') == 'completed']

        if len(successful) < 5:
            return {'error': 'Not enough successful executions to train (need at least 5)'}

        # Build simple rule-based model from data
        rules = {
            'high_volume': {'glue': 0, 'emr': 0, 'lambda': 0},
            'medium_volume': {'glue': 0, 'emr': 0, 'lambda': 0},
            'low_volume': {'glue': 0, 'emr': 0, 'lambda': 0},
            'high_complexity': {'glue': 0, 'emr': 0, 'lambda': 0},
            'medium_complexity': {'glue': 0, 'emr': 0, 'lambda': 0},
            'low_complexity': {'glue': 0, 'emr': 0, 'lambda': 0}
        }

        # Count successful runs by workload type and platform
        for vector in successful:
            volume = vector.get('workload', {}).get('data_volume', 'medium')
            complexity = vector.get('workload', {}).get('complexity', 'medium')
            platform = vector.get('execution', {}).get('platform', 'glue')

            volume_key = f"{volume}_volume"
            complexity_key = f"{complexity}_complexity"

            if volume_key in rules and platform in rules[volume_key]:
                rules[volume_key][platform] += 1
            if complexity_key in rules and platform in rules[complexity_key]:
                rules[complexity_key][platform] += 1

        # Determine best platform for each category
        trained_model = {
            'model_id': str(uuid.uuid4()),
            'trained_at': datetime.utcnow().isoformat(),
            'training_samples': len(successful),
            'rules': {}
        }

        for category, platforms in rules.items():
            if sum(platforms.values()) > 0:
                best_platform = max(platforms, key=platforms.get)
                confidence = platforms[best_platform] / sum(platforms.values())
                trained_model['rules'][category] = {
                    'recommended_platform': best_platform,
                    'confidence': round(confidence, 2),
                    'sample_count': sum(platforms.values())
                }

        # Store trained model
        self._store_trained_model(trained_model)

        logger.info(f"Trained platform predictor with {len(successful)} samples")
        return trained_model

    def _store_trained_model(self, model: Dict[str, Any]) -> None:
        """Store trained model to S3."""
        try:
            key = f"{self.TRAINED_MODELS_PREFIX}platform_predictor_{model['model_id']}.json"
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(model, indent=2, cls=DateTimeEncoder),
                ContentType='application/json'
            )

            # Also store as "latest"
            latest_key = f"{self.TRAINED_MODELS_PREFIX}platform_predictor_latest.json"
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=latest_key,
                Body=json.dumps(model, indent=2, cls=DateTimeEncoder),
                ContentType='application/json'
            )
        except Exception as e:
            logger.error(f"Failed to store trained model: {e}")

    def predict_platform(self, workload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Predict optimal platform for a workload using trained model.

        Args:
            workload: Workload characteristics

        Returns:
            Prediction with confidence score
        """
        # Load latest trained model
        try:
            key = f"{self.TRAINED_MODELS_PREFIX}platform_predictor_latest.json"
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            model = json.loads(response['Body'].read().decode('utf-8'))
        except Exception as e:
            logger.warning(f"No trained model found, using defaults: {e}")
            return self._default_prediction(workload)

        rules = model.get('rules', {})

        volume = workload.get('data_volume', 'medium')
        complexity = workload.get('complexity', 'medium')

        volume_rule = rules.get(f"{volume}_volume", {})
        complexity_rule = rules.get(f"{complexity}_complexity", {})

        # Combine predictions
        predictions = {}

        if volume_rule:
            platform = volume_rule.get('recommended_platform')
            conf = volume_rule.get('confidence', 0)
            predictions[platform] = predictions.get(platform, 0) + conf * 0.5

        if complexity_rule:
            platform = complexity_rule.get('recommended_platform')
            conf = complexity_rule.get('confidence', 0)
            predictions[platform] = predictions.get(platform, 0) + conf * 0.5

        if not predictions:
            return self._default_prediction(workload)

        best_platform = max(predictions, key=predictions.get)

        return {
            'recommended_platform': best_platform,
            'confidence': round(predictions[best_platform], 2),
            'all_scores': predictions,
            'model_id': model.get('model_id'),
            'based_on_samples': model.get('training_samples')
        }

    def _default_prediction(self, workload: Dict[str, Any]) -> Dict[str, Any]:
        """Default prediction when no trained model available."""
        volume = workload.get('data_volume', 'medium')
        complexity = workload.get('complexity', 'medium')

        # Simple heuristic rules
        if volume == 'low' and complexity == 'low':
            platform = 'lambda'
        elif volume == 'high' or complexity == 'high':
            platform = 'glue'
        else:
            platform = 'glue'

        return {
            'recommended_platform': platform,
            'confidence': 0.5,
            'note': 'Using default rules (no trained model available)'
        }

    # =========================================================================
    # INSIGHTS ON DEMAND - Answer questions about historical data
    # =========================================================================

    def get_insights(self, question: str) -> Dict[str, Any]:
        """
        Get insights by asking a question about historical data.
        Uses Bedrock to analyze data and answer.

        Args:
            question: Natural language question about ETL performance

        Returns:
            AI-generated insights based on historical data
        """
        # Get analysis data
        analysis = self.analyze_patterns()
        vectors = self.get_all_vectors(limit=20)

        prompt = f"""
        You are an ETL performance analyst. Based on the following historical data, answer the user's question.

        HISTORICAL ANALYSIS:
        {json.dumps(analysis, indent=2, cls=DateTimeEncoder)}

        RECENT EXECUTION SAMPLES (last 20):
        {json.dumps(vectors[:5], indent=2, cls=DateTimeEncoder)}
        ... and {len(vectors) - 5} more executions

        USER QUESTION: {question}

        Provide a detailed, data-driven answer. Include specific numbers and recommendations where applicable.
        Format your response as JSON with keys: "answer", "key_findings", "recommendations", "confidence"
        """

        try:
            response = self.bedrock.invoke_model(
                modelId='anthropic.claude-3-sonnet-20240229-v1:0',
                body=json.dumps({
                    'anthropic_version': 'bedrock-2023-05-31',
                    'max_tokens': 2000,
                    'messages': [{'role': 'user', 'content': prompt}]
                })
            )

            result = json.loads(response['body'].read())
            answer_text = result['content'][0]['text']

            # Try to parse as JSON
            try:
                if '{' in answer_text:
                    start = answer_text.find('{')
                    end = answer_text.rfind('}') + 1
                    return json.loads(answer_text[start:end])
            except:
                pass

            return {
                'answer': answer_text,
                'key_findings': [],
                'recommendations': [],
                'confidence': 0.7
            }

        except Exception as e:
            logger.error(f"Failed to get insights: {e}")
            return {
                'error': str(e),
                'fallback_analysis': analysis.get('summary', {})
            }

    # =========================================================================
    # REPORTING - Generate reports
    # =========================================================================

    def generate_report(self, report_type: str = 'summary') -> Dict[str, Any]:
        """
        Generate a report of the specified type.

        Args:
            report_type: 'summary', 'detailed', 'cost', 'performance'

        Returns:
            Generated report
        """
        vectors = self.get_all_vectors()

        if report_type == 'summary':
            return {
                'report_type': 'summary',
                'generated_at': datetime.utcnow().isoformat(),
                'summary': self._generate_summary(vectors),
                'platform_usage': self._analyze_platforms(vectors)
            }

        elif report_type == 'detailed':
            return self.analyze_patterns()

        elif report_type == 'cost':
            return {
                'report_type': 'cost',
                'generated_at': datetime.utcnow().isoformat(),
                'cost_analysis': self._analyze_costs(vectors),
                'summary': self._generate_summary(vectors)
            }

        elif report_type == 'performance':
            return {
                'report_type': 'performance',
                'generated_at': datetime.utcnow().isoformat(),
                'performance_trends': self._analyze_performance_trends(vectors),
                'platform_performance': self._analyze_platforms(vectors)
            }

        else:
            return {'error': f'Unknown report type: {report_type}'}


# =============================================================================
# CLI Interface
# =============================================================================

def main():
    """CLI for Learning Module."""
    import argparse

    parser = argparse.ArgumentParser(description='Strands ETL Learning Module')
    parser.add_argument('command', choices=['analyze', 'train', 'predict', 'insights', 'report'],
                        help='Command to execute')
    parser.add_argument('--question', '-q', help='Question for insights command')
    parser.add_argument('--workload', '-w', help='Workload JSON for predict command')
    parser.add_argument('--report-type', '-r', default='summary',
                        choices=['summary', 'detailed', 'cost', 'performance'])
    parser.add_argument('--bucket', '-b', default='strands-etl-learning', help='S3 bucket name')

    args = parser.parse_args()

    learning = LearningModule(bucket_name=args.bucket)

    if args.command == 'analyze':
        result = learning.analyze_patterns()
        print(json.dumps(result, indent=2, cls=DateTimeEncoder))

    elif args.command == 'train':
        result = learning.train_platform_predictor()
        print(json.dumps(result, indent=2, cls=DateTimeEncoder))

    elif args.command == 'predict':
        if args.workload:
            workload = json.loads(args.workload)
        else:
            workload = {'data_volume': 'medium', 'complexity': 'medium'}
        result = learning.predict_platform(workload)
        print(json.dumps(result, indent=2, cls=DateTimeEncoder))

    elif args.command == 'insights':
        question = args.question or "What are the main patterns in my ETL executions?"
        result = learning.get_insights(question)
        print(json.dumps(result, indent=2, cls=DateTimeEncoder))

    elif args.command == 'report':
        result = learning.generate_report(args.report_type)
        print(json.dumps(result, indent=2, cls=DateTimeEncoder))


if __name__ == '__main__':
    main()
