"""
Strands Decision Agent - Intelligent platform selection using ML-based learning.
Uses historical execution patterns to make optimal platform decisions.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import json
import uuid
import numpy as np

from strands_agent_base import StrandsAgent, AgentState
from strands_message_bus import StrandsMessage, MessageType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StrandsDecisionAgent(StrandsAgent):
    """
    Decision Agent that learns from historical executions to select optimal platforms.
    Uses pattern matching and ML-based scoring to recommend Glue, EMR, or Lambda.
    """

    def __init__(self):
        super().__init__(agent_name="decision_agent", agent_type="decision")
        self.platform_scores = {
            'glue': {'total_runs': 0, 'successful_runs': 0, 'avg_cost': 0, 'avg_time': 0},
            'emr': {'total_runs': 0, 'successful_runs': 0, 'avg_cost': 0, 'avg_time': 0},
            'lambda': {'total_runs': 0, 'successful_runs': 0, 'avg_cost': 0, 'avg_time': 0}
        }
        self.learning_cache = []
        self.cache_ttl = 300  # 5 minutes
        self.last_cache_refresh = None

    def _get_subscribed_message_types(self) -> List[MessageType]:
        """Subscribe to platform decision requests."""
        return [MessageType.AGENT_REQUEST]

    async def on_start(self):
        """Initialize by loading historical learning data."""
        logger.info("Decision Agent: Loading historical learning patterns...")
        await self._refresh_learning_cache()
        self._calculate_platform_scores()

    async def process_message(self, message: StrandsMessage) -> Optional[Dict[str, Any]]:
        """Process platform decision requests."""
        if message.message_type != MessageType.AGENT_REQUEST:
            return None

        request_type = message.payload.get('request_type')
        if request_type != 'platform_decision':
            return None

        logger.info(f"Decision Agent: Processing platform decision request")

        workload = message.payload.get('workload', {})
        config = message.payload.get('config', {})

        # Make decision using learning vectors
        decision = await self._make_platform_decision(workload, config)

        # Broadcast decision
        await self.send_message(
            target_agent=None,  # Broadcast
            message_type=MessageType.DECISION_MADE,
            payload=decision,
            correlation_id=message.correlation_id
        )

        return decision

    async def autonomous_cycle(self):
        """Periodically refresh learning data and recalculate scores."""
        # Refresh learning cache every 5 minutes
        if (self.last_cache_refresh is None or
            (datetime.utcnow() - self.last_cache_refresh).total_seconds() > self.cache_ttl):
            await self._refresh_learning_cache()
            self._calculate_platform_scores()

    async def _refresh_learning_cache(self):
        """Load recent learning vectors into cache."""
        logger.info("Decision Agent: Refreshing learning cache...")
        self.learning_cache = await self.load_learning_vectors(limit=50)
        self.last_cache_refresh = datetime.utcnow()
        logger.info(f"Decision Agent: Loaded {len(self.learning_cache)} learning vectors")

    def _calculate_platform_scores(self):
        """Calculate platform scores from learning vectors."""
        if not self.learning_cache:
            logger.warning("No learning vectors available for scoring")
            return

        # Reset scores
        for platform in self.platform_scores:
            self.platform_scores[platform] = {
                'total_runs': 0,
                'successful_runs': 0,
                'avg_cost': 0,
                'avg_time': 0
            }

        # Aggregate metrics from learning vectors
        platform_metrics = {'glue': [], 'emr': [], 'lambda': []}

        for vector in self.learning_cache:
            platform = vector.get('execution_metrics', {}).get('platform_used')
            if not platform or platform not in platform_metrics:
                continue

            success = vector.get('pipeline_context', {}).get('status') == 'completed'
            exec_time = vector.get('execution_metrics', {}).get('execution_time_seconds', 0)
            quality_score = vector.get('execution_metrics', {}).get('data_quality_score', 0)
            efficiency = vector.get('performance_indicators', {}).get('efficiency_score', 0)

            self.platform_scores[platform]['total_runs'] += 1
            if success:
                self.platform_scores[platform]['successful_runs'] += 1

            platform_metrics[platform].append({
                'time': exec_time,
                'quality': quality_score,
                'efficiency': efficiency
            })

        # Calculate averages
        for platform, metrics in platform_metrics.items():
            if metrics:
                self.platform_scores[platform]['avg_time'] = np.mean([m['time'] for m in metrics])
                self.platform_scores[platform]['avg_efficiency'] = np.mean([m['efficiency'] for m in metrics])
                self.platform_scores[platform]['success_rate'] = (
                    self.platform_scores[platform]['successful_runs'] /
                    self.platform_scores[platform]['total_runs']
                    if self.platform_scores[platform]['total_runs'] > 0 else 0
                )

        logger.info(f"Platform scores updated: {json.dumps(self.platform_scores, indent=2)}")

    async def _make_platform_decision(self,
                                      workload: Dict[str, Any],
                                      config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make intelligent platform decision using ML-based pattern matching.
        """
        data_volume = workload.get('data_volume', 'medium')
        complexity = workload.get('complexity', 'medium')
        criticality = workload.get('criticality', 'medium')

        # Find similar workloads in learning vectors
        similar_workloads = self._find_similar_workloads(workload)

        # Calculate platform scores based on similarity
        platform_recommendations = self._score_platforms(
            workload, similar_workloads
        )

        # Use AI for final recommendation
        ai_recommendation = await self._get_ai_recommendation(
            workload, similar_workloads, platform_recommendations
        )

        # Select best platform
        selected_platform = self._select_platform(
            platform_recommendations, ai_recommendation
        )

        decision = {
            'selected_platform': selected_platform,
            'confidence_score': platform_recommendations[selected_platform]['score'],
            'reasoning': platform_recommendations[selected_platform]['reasoning'],
            'alternative_platforms': [
                {
                    'platform': p,
                    'score': data['score'],
                    'reasoning': data['reasoning']
                }
                for p, data in platform_recommendations.items()
                if p != selected_platform
            ],
            'similar_workloads_count': len(similar_workloads),
            'ai_recommendation': ai_recommendation,
            'platform_statistics': self.platform_scores,
            'decision_id': str(uuid.uuid4()),
            'timestamp': datetime.utcnow().isoformat()
        }

        logger.info(f"Decision Agent: Selected platform '{selected_platform}' "
                   f"with confidence {decision['confidence_score']:.2f}")

        # Store decision as learning vector
        await self._store_decision_vector(workload, decision)

        return decision

    def _find_similar_workloads(self, workload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find similar workloads from learning vectors using feature matching."""
        similar = []

        target_features = self._extract_workload_features(workload)

        for vector in self.learning_cache:
            vector_workload = vector.get('workload_characteristics', {})
            vector_features = self._extract_workload_features(vector_workload)

            # Calculate similarity score
            similarity = self._calculate_similarity(target_features, vector_features)

            if similarity > 0.6:  # Threshold for similarity
                similar.append({
                    'vector': vector,
                    'similarity': similarity
                })

        # Sort by similarity
        similar.sort(key=lambda x: x['similarity'], reverse=True)

        logger.info(f"Found {len(similar)} similar workloads")
        return similar[:10]  # Top 10 most similar

    def _extract_workload_features(self, workload: Dict[str, Any]) -> Dict[str, float]:
        """Extract numeric features from workload for similarity calculation."""
        # Convert categorical features to numeric
        volume_map = {'low': 1, 'medium': 2, 'high': 3, 'very_high': 4}
        complexity_map = {'low': 1, 'medium': 2, 'high': 3, 'very_high': 4}
        criticality_map = {'low': 1, 'medium': 2, 'high': 3, 'critical': 4}

        return {
            'data_volume': volume_map.get(workload.get('data_volume', 'medium'), 2),
            'complexity': complexity_map.get(workload.get('complexity', 'medium'), 2),
            'criticality': criticality_map.get(workload.get('criticality', 'medium'), 2),
            'estimated_runtime': workload.get('estimated_runtime_minutes', 30) / 60.0  # Normalize to hours
        }

    def _calculate_similarity(self,
                             features1: Dict[str, float],
                             features2: Dict[str, float]) -> float:
        """Calculate cosine similarity between two feature vectors."""
        # Extract common features
        common_keys = set(features1.keys()) & set(features2.keys())
        if not common_keys:
            return 0.0

        vec1 = np.array([features1[k] for k in common_keys])
        vec2 = np.array([features2[k] for k in common_keys])

        # Cosine similarity
        dot_product = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return dot_product / (norm1 * norm2)

    def _score_platforms(self,
                        workload: Dict[str, Any],
                        similar_workloads: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Score each platform based on workload and historical patterns."""
        scores = {
            'glue': {'score': 0.0, 'reasoning': []},
            'emr': {'score': 0.0, 'reasoning': []},
            'lambda': {'score': 0.0, 'reasoning': []}
        }

        # Base scores from workload characteristics
        data_volume = workload.get('data_volume', 'medium')
        complexity = workload.get('complexity', 'medium')

        # Rule-based initial scoring
        if data_volume in ['high', 'very_high']:
            scores['glue']['score'] += 0.4
            scores['glue']['reasoning'].append("High data volume favors Glue")
            scores['emr']['score'] += 0.3
            scores['emr']['reasoning'].append("High volume suitable for EMR")
        elif data_volume == 'low':
            scores['lambda']['score'] += 0.5
            scores['lambda']['reasoning'].append("Low data volume ideal for Lambda")

        if complexity in ['high', 'very_high']:
            scores['glue']['score'] += 0.3
            scores['glue']['reasoning'].append("High complexity suits Glue")
            scores['emr']['score'] += 0.4
            scores['emr']['reasoning'].append("Complex transformations work well on EMR")
        elif complexity == 'low':
            scores['lambda']['score'] += 0.3
            scores['lambda']['reasoning'].append("Low complexity fits Lambda")

        # Adjust scores based on similar workloads
        if similar_workloads:
            platform_successes = {'glue': 0, 'emr': 0, 'lambda': 0}
            platform_counts = {'glue': 0, 'emr': 0, 'lambda': 0}

            for item in similar_workloads:
                vector = item['vector']
                similarity = item['similarity']
                platform = vector.get('execution_metrics', {}).get('platform_used')
                success = vector.get('pipeline_context', {}).get('status') == 'completed'

                if platform in platform_counts:
                    platform_counts[platform] += 1
                    if success:
                        platform_successes[platform] += similarity  # Weight by similarity

            # Add learned scores
            for platform in scores:
                if platform_counts[platform] > 0:
                    success_rate = platform_successes[platform] / platform_counts[platform]
                    scores[platform]['score'] += success_rate * 0.3
                    scores[platform]['reasoning'].append(
                        f"Historical success rate: {success_rate:.2%} "
                        f"({platform_counts[platform]} similar workloads)"
                    )

        # Add global platform statistics
        for platform in scores:
            stats = self.platform_scores[platform]
            if stats['total_runs'] > 0:
                global_success = stats.get('success_rate', 0)
                scores[platform]['score'] += global_success * 0.2
                scores[platform]['reasoning'].append(
                    f"Global success rate: {global_success:.2%} "
                    f"({stats['total_runs']} total runs)"
                )

        # Normalize scores
        max_score = max(s['score'] for s in scores.values()) or 1.0
        for platform in scores:
            scores[platform]['score'] = min(scores[platform]['score'] / max_score, 1.0)
            scores[platform]['reasoning'] = ' | '.join(scores[platform]['reasoning'])

        return scores

    async def _get_ai_recommendation(self,
                                     workload: Dict[str, Any],
                                     similar_workloads: List[Dict[str, Any]],
                                     platform_scores: Dict[str, Dict[str, Any]]) -> str:
        """Get AI recommendation using Bedrock."""
        # Prepare context
        similar_summary = []
        for item in similar_workloads[:5]:
            vector = item['vector']
            similar_summary.append({
                'platform': vector.get('execution_metrics', {}).get('platform_used'),
                'success': vector.get('pipeline_context', {}).get('status') == 'completed',
                'similarity': item['similarity'],
                'execution_time': vector.get('execution_metrics', {}).get('execution_time_seconds')
            })

        prompt = f"""
        Based on the workload characteristics and historical learning data, recommend the best platform.

        Workload:
        {json.dumps(workload, indent=2)}

        Platform Scores (ML-based):
        {json.dumps(platform_scores, indent=2)}

        Similar Historical Workloads:
        {json.dumps(similar_summary, indent=2)}

        Provide a brief recommendation (2-3 sentences) explaining which platform is best and why.
        """

        system_prompt = "You are an expert platform selection specialist with deep knowledge of AWS Glue, EMR, and Lambda for ETL workloads."

        recommendation = await self.invoke_bedrock(prompt, system_prompt)
        return recommendation.strip()

    def _select_platform(self,
                        platform_scores: Dict[str, Dict[str, Any]],
                        ai_recommendation: str) -> str:
        """Select the best platform based on scores."""
        # Simple selection: highest score
        best_platform = max(platform_scores.items(), key=lambda x: x[1]['score'])
        return best_platform[0]

    async def _store_decision_vector(self,
                                     workload: Dict[str, Any],
                                     decision: Dict[str, Any]):
        """Store decision as a learning vector for future reference."""
        vector = {
            'vector_id': decision['decision_id'],
            'timestamp': datetime.utcnow().isoformat(),
            'agent_type': 'decision',
            'workload_characteristics': workload,
            'decision': {
                'platform': decision['selected_platform'],
                'confidence': decision['confidence_score'],
                'reasoning': decision['reasoning']
            },
            'platform_statistics': self.platform_scores
        }

        await self.store_learning_vector(vector)


# Standalone execution
async def main():
    """Run Decision Agent as standalone service."""
    from strands_message_bus import start_message_bus
    import asyncio

    # Start message bus
    bus_task = asyncio.create_task(start_message_bus())

    # Create and start agent
    agent = StrandsDecisionAgent()
    agent_task = asyncio.create_task(agent.start())

    # Keep running
    try:
        await asyncio.gather(bus_task, agent_task)
    except KeyboardInterrupt:
        logger.info("Shutting down Decision Agent...")
        await agent.stop()


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
