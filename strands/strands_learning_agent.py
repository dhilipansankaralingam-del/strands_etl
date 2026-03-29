"""
Strands Learning Agent - Captures execution patterns and creates learning vectors.
Implements continuous learning from pipeline executions.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import json
import uuid

from strands_agent_base import StrandsAgent
from strands_message_bus import StrandsMessage, MessageType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StrandsLearningAgent(StrandsAgent):
    """
    Learning Agent that captures execution patterns and stores learning vectors.
    Aggregates insights from all other agents for continuous improvement.
    """

    def __init__(self):
        super().__init__(agent_name="learning_agent", agent_type="learning")
        self.execution_contexts = {}  # Track active executions

    def _get_subscribed_message_types(self) -> List[MessageType]:
        """Subscribe to all major events to learn from them."""
        return [
            MessageType.EXECUTION_STARTED,
            MessageType.EXECUTION_COMPLETED,
            MessageType.DECISION_MADE,
            MessageType.OPTIMIZATION_RECOMMENDATION,
            MessageType.LEARNING_UPDATE
        ]

    async def on_start(self):
        """Initialize learning agent."""
        logger.info("Learning Agent: Ready to capture execution patterns")

    async def process_message(self, message: StrandsMessage) -> Optional[Dict[str, Any]]:
        """Process messages and build learning context."""
        correlation_id = message.correlation_id

        # Track execution lifecycle
        if message.message_type == MessageType.EXECUTION_STARTED:
            self.execution_contexts[correlation_id] = {
                'start_time': message.timestamp,
                'platform': message.payload.get('platform'),
                'config': message.payload.get('config', {})
            }

        elif message.message_type == MessageType.DECISION_MADE:
            if correlation_id in self.execution_contexts:
                self.execution_contexts[correlation_id]['decision'] = message.payload

        elif message.message_type == MessageType.EXECUTION_COMPLETED:
            if correlation_id in self.execution_contexts:
                self.execution_contexts[correlation_id]['completion'] = message.payload
                self.execution_contexts[correlation_id]['end_time'] = message.timestamp

                # Create comprehensive learning vector
                await self._create_learning_vector(correlation_id)

        elif message.message_type == MessageType.OPTIMIZATION_RECOMMENDATION:
            if correlation_id in self.execution_contexts:
                self.execution_contexts[correlation_id]['optimization'] = message.payload

        return None

    async def autonomous_cycle(self):
        """Periodically clean up old contexts and analyze patterns."""
        # Clean up old execution contexts (older than 1 hour)
        current_time = datetime.utcnow()
        to_remove = []

        for corr_id, context in self.execution_contexts.items():
            start_time_str = context.get('start_time')
            if start_time_str:
                try:
                    start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                    age_seconds = (current_time - start_time).total_seconds()
                    if age_seconds > 3600:  # 1 hour
                        to_remove.append(corr_id)
                except Exception:
                    pass

        for corr_id in to_remove:
            del self.execution_contexts[corr_id]

        if to_remove:
            logger.info(f"Cleaned up {len(to_remove)} old execution contexts")

    async def _create_learning_vector(self, correlation_id: str):
        """
        Create comprehensive learning vector from aggregated execution data.
        """
        context = self.execution_contexts.get(correlation_id, {})

        if not context:
            logger.warning(f"No context found for correlation {correlation_id}")
            return

        # Extract all relevant data
        start_time_str = context.get('start_time')
        end_time_str = context.get('end_time')
        config = context.get('config', {})
        decision = context.get('decision', {})
        completion = context.get('completion', {})
        optimization = context.get('optimization', {})

        # Calculate execution time
        execution_time_seconds = None
        if start_time_str and end_time_str:
            try:
                start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                execution_time_seconds = (end_time - start_time).total_seconds()
            except Exception as e:
                logger.warning(f"Could not calculate execution time: {e}")

        # Extract execution result
        execution_result = completion.get('execution_result', {})
        status = execution_result.get('status', 'unknown')
        platform = decision.get('selected_platform', execution_result.get('platform', 'unknown'))

        # Extract quality metrics
        quality_report = completion.get('quality_report', {})
        quality_score = quality_report.get('overall_score', 0.95)

        # Extract optimization metrics
        optimization_report = optimization.get('optimization_report', optimization)
        efficiency_score = optimization_report.get('estimated_improvement', {}).get('estimated_improvement_pct', 0)

        # Build comprehensive learning vector
        learning_vector = {
            'vector_id': str(uuid.uuid4()),
            'correlation_id': correlation_id,
            'timestamp': datetime.utcnow().isoformat(),
            'agent_type': 'learning',

            # Workload characteristics
            'workload_characteristics': config.get('workload', {}),

            # Execution metrics
            'execution_metrics': {
                'platform_used': platform,
                'execution_time_seconds': execution_time_seconds,
                'data_quality_score': quality_score,
                'status': status,
                'success': status == 'completed'
            },

            # Decision context
            'decision_context': {
                'selected_platform': platform,
                'decision_confidence': decision.get('confidence_score', 0),
                'decision_reasoning': decision.get('reasoning', '')
            },

            # Performance indicators
            'performance_indicators': {
                'efficiency_score': efficiency_score / 100 if efficiency_score else 0.85,
                'optimization_opportunities': len(optimization_report.get('optimization_opportunities', [])),
                'recommendations_count': len(optimization_report.get('recommendations', []))
            },

            # Quality metrics
            'quality_metrics': {
                'overall_score': quality_score,
                'quality_report': quality_report
            },

            # Optimization insights
            'optimization_recommendations': optimization_report.get('recommendations', []),

            # Full context for deep learning
            'full_context': {
                'config': config,
                'execution_result': execution_result
            }
        }

        # Generate learning insights using AI
        insights = await self._generate_learning_insights(learning_vector)
        learning_vector['learning_insights'] = insights

        # Store the learning vector
        success = await self.store_learning_vector(learning_vector)

        if success:
            logger.info(f"Learning vector {learning_vector['vector_id']} created and stored for {platform} execution")

            # Broadcast learning update
            await self.send_message(
                target_agent=None,
                message_type=MessageType.LEARNING_UPDATE,
                payload={
                    'vector_id': learning_vector['vector_id'],
                    'platform': platform,
                    'execution_time': execution_time_seconds,
                    'quality_score': quality_score,
                    'success': status == 'completed',
                    'insights': insights
                },
                correlation_id=correlation_id
            )

        # Clean up this context
        if correlation_id in self.execution_contexts:
            del self.execution_contexts[correlation_id]

    async def _generate_learning_insights(self, learning_vector: Dict[str, Any]) -> str:
        """Generate AI-powered learning insights from the execution."""
        # Extract key metrics
        platform = learning_vector['execution_metrics']['platform_used']
        exec_time = learning_vector['execution_metrics']['execution_time_seconds']
        success = learning_vector['execution_metrics']['success']
        quality = learning_vector['execution_metrics']['data_quality_score']
        workload = learning_vector['workload_characteristics']

        prompt = f"""
        Analyze this ETL execution and extract key learning insights:

        Platform: {platform}
        Execution Time: {exec_time} seconds
        Success: {success}
        Quality Score: {quality}
        Workload: {json.dumps(workload, indent=2)}

        Provide insights on:
        1. What worked well in this execution
        2. What could be improved
        3. Patterns or lessons learned
        4. Recommendations for similar future workloads

        Be concise (4-5 sentences).
        """

        insights = await self.invoke_bedrock(prompt, "You are an ML specialist analyzing ETL execution patterns.")

        return insights.strip()

    async def get_learning_summary(self, limit: int = 50) -> Dict[str, Any]:
        """Get a summary of learning from recent executions."""
        vectors = await self.load_learning_vectors(limit=limit)

        if not vectors:
            return {
                'total_executions': 0,
                'message': 'No learning data available yet'
            }

        # Aggregate statistics
        platforms_used = {}
        total_time = 0
        successful = 0
        total = len(vectors)

        for vector in vectors:
            platform = vector.get('execution_metrics', {}).get('platform_used', 'unknown')
            platforms_used[platform] = platforms_used.get(platform, 0) + 1

            exec_time = vector.get('execution_metrics', {}).get('execution_time_seconds', 0)
            if exec_time:
                total_time += exec_time

            if vector.get('execution_metrics', {}).get('success'):
                successful += 1

        summary = {
            'total_executions': total,
            'successful_executions': successful,
            'success_rate': successful / total if total > 0 else 0,
            'avg_execution_time_seconds': total_time / total if total > 0 else 0,
            'platforms_used': platforms_used,
            'recent_insights': [v.get('learning_insights', '') for v in vectors[:5]]
        }

        return summary


# Standalone execution
async def main():
    """Run Learning Agent as standalone service."""
    from strands_message_bus import start_message_bus
    import asyncio

    bus_task = asyncio.create_task(start_message_bus())
    agent = StrandsLearningAgent()
    agent_task = asyncio.create_task(agent.start())

    try:
        await asyncio.gather(bus_task, agent_task)
    except KeyboardInterrupt:
        logger.info("Shutting down Learning Agent...")
        await agent.stop()


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
