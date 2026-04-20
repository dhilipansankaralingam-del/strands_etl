"""
Strands Optimization Agent - Pattern-based performance recommendations.
Learns from historical executions to provide actionable optimization suggestions.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import json
import uuid
import numpy as np
from collections import defaultdict

from strands_agent_base import StrandsAgent
from strands_message_bus import StrandsMessage, MessageType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StrandsOptimizationAgent(StrandsAgent):
    """
    Optimization Agent that analyzes execution patterns and provides recommendations.
    Uses ML-based pattern recognition to identify optimization opportunities.
    """

    def __init__(self):
        super().__init__(agent_name="optimization_agent", agent_type="optimization")
        self.optimization_patterns = []
        self.performance_baselines = {}
        self.recommendation_history = []

    def _get_subscribed_message_types(self) -> List[MessageType]:
        """Subscribe to execution completed events."""
        return [MessageType.EXECUTION_COMPLETED, MessageType.AGENT_REQUEST]

    async def on_start(self):
        """Load historical patterns and establish baselines."""
        logger.info("Optimization Agent: Loading historical optimization data...")
        await self._load_optimization_history()
        self._establish_performance_baselines()

    async def process_message(self, message: StrandsMessage) -> Optional[Dict[str, Any]]:
        """Process optimization requests and execution events."""
        if message.message_type == MessageType.EXECUTION_COMPLETED:
            return await self._analyze_execution_performance(message)
        elif message.message_type == MessageType.AGENT_REQUEST:
            request_type = message.payload.get('request_type')
            if request_type == 'optimization_recommendations':
                return await self._generate_recommendations(message.payload)
        return None

    async def autonomous_cycle(self):
        """Periodically analyze patterns and update baselines."""
        # Every cycle, refresh patterns
        if len(self.optimization_patterns) > 100:
            # Trim old patterns
            self.optimization_patterns = self.optimization_patterns[-100:]

    async def _load_optimization_history(self):
        """Load historical optimization data."""
        vectors = await self.load_learning_vectors(limit=50)

        for vector in vectors:
            if 'optimization_recommendations' in vector:
                self.recommendation_history.append(vector)

            if 'execution_metrics' in vector:
                self.optimization_patterns.append({
                    'platform': vector.get('execution_metrics', {}).get('platform_used'),
                    'execution_time': vector.get('execution_metrics', {}).get('execution_time_seconds'),
                    'quality_score': vector.get('execution_metrics', {}).get('data_quality_score'),
                    'efficiency': vector.get('performance_indicators', {}).get('efficiency_score'),
                    'workload': vector.get('workload_characteristics', {}),
                    'timestamp': vector.get('timestamp')
                })

        logger.info(f"Loaded {len(self.optimization_patterns)} optimization patterns")

    def _establish_performance_baselines(self):
        """Establish performance baselines from historical data."""
        if not self.optimization_patterns:
            logger.warning("No patterns available to establish baselines")
            return

        # Group by platform
        platform_metrics = defaultdict(list)

        for pattern in self.optimization_patterns:
            platform = pattern.get('platform', 'unknown')
            if pattern.get('execution_time'):
                platform_metrics[platform].append({
                    'time': pattern['execution_time'],
                    'efficiency': pattern.get('efficiency', 0.8)
                })

        # Calculate baselines
        for platform, metrics in platform_metrics.items():
            times = [m['time'] for m in metrics if m['time']]
            efficiencies = [m['efficiency'] for m in metrics]

            self.performance_baselines[platform] = {
                'avg_execution_time': np.mean(times) if times else 0,
                'p50_execution_time': np.median(times) if times else 0,
                'p95_execution_time': np.percentile(times, 95) if times else 0,
                'avg_efficiency': np.mean(efficiencies) if efficiencies else 0.8,
                'sample_count': len(metrics)
            }

        logger.info(f"Performance baselines: {json.dumps(self.performance_baselines, indent=2)}")

    async def _analyze_execution_performance(self, message: StrandsMessage) -> Dict[str, Any]:
        """Analyze completed execution and provide recommendations."""
        execution_result = message.payload.get('execution_result', {})
        context = message.payload.get('context', {})

        platform = execution_result.get('platform', 'unknown')
        execution_time = self._extract_execution_time(execution_result)

        # Compare against baseline
        baseline = self.performance_baselines.get(platform, {})
        performance_analysis = self._compare_to_baseline(execution_time, baseline)

        # Identify optimization opportunities
        opportunities = await self._identify_optimization_opportunities(
            execution_result, context, performance_analysis
        )

        # Generate specific recommendations
        recommendations = await self._generate_specific_recommendations(
            execution_result, opportunities
        )

        optimization_report = {
            'execution_id': execution_result.get('job_name', 'unknown'),
            'platform': platform,
            'execution_time_seconds': execution_time,
            'performance_analysis': performance_analysis,
            'optimization_opportunities': opportunities,
            'recommendations': recommendations,
            'priority_actions': self._prioritize_recommendations(recommendations),
            'estimated_improvement': self._estimate_improvement(recommendations),
            'timestamp': datetime.utcnow().isoformat(),
            'report_id': str(uuid.uuid4())
        }

        # Store optimization report
        await self._store_optimization_report(optimization_report, message)

        # Broadcast recommendations
        await self.send_message(
            target_agent=None,
            message_type=MessageType.OPTIMIZATION_RECOMMENDATION,
            payload=optimization_report,
            correlation_id=message.correlation_id
        )

        logger.info(f"Generated {len(recommendations)} optimization recommendations")

        return optimization_report

    async def _generate_recommendations(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Generate optimization recommendations based on request."""
        context = payload.get('context', {})
        config = payload.get('config', {})

        # Analyze current configuration
        config_issues = self._analyze_configuration(config)

        # Find similar successful executions
        similar_patterns = self._find_successful_patterns(context)

        # Generate recommendations using AI
        ai_recommendations = await self._get_ai_recommendations(
            context, config, config_issues, similar_patterns
        )

        recommendations = {
            'configuration_recommendations': config_issues,
            'pattern_based_recommendations': similar_patterns,
            'ai_recommendations': ai_recommendations,
            'timestamp': datetime.utcnow().isoformat()
        }

        return recommendations

    def _extract_execution_time(self, execution_result: Dict[str, Any]) -> float:
        """Extract execution time from result."""
        # Try to get from job run details
        job_run = execution_result.get('job_run_details', {})
        if job_run:
            start = job_run.get('StartedOn')
            end = job_run.get('CompletedOn')
            if start and end:
                # These would be datetime objects in real scenario
                return (end - start).total_seconds() if hasattr(end, 'total_seconds') else 0

        # Fallback to estimated or default
        return execution_result.get('execution_time_seconds', 0)

    def _compare_to_baseline(self,
                            execution_time: float,
                            baseline: Dict[str, Any]) -> Dict[str, Any]:
        """Compare execution to baseline performance."""
        if not baseline or not execution_time:
            return {
                'status': 'no_baseline',
                'message': 'No baseline data available for comparison'
            }

        avg_time = baseline.get('avg_execution_time', 0)
        p95_time = baseline.get('p95_execution_time', 0)

        if execution_time <= avg_time:
            status = 'excellent'
            message = f'Performance {((avg_time - execution_time) / avg_time * 100):.1f}% better than average'
        elif execution_time <= p95_time:
            status = 'good'
            message = 'Performance within acceptable range'
        else:
            status = 'needs_improvement'
            message = f'Performance {((execution_time - avg_time) / avg_time * 100):.1f}% slower than average'

        return {
            'status': status,
            'message': message,
            'execution_time': execution_time,
            'baseline_avg': avg_time,
            'baseline_p95': p95_time,
            'vs_average_pct': ((execution_time - avg_time) / avg_time * 100) if avg_time else 0
        }

    async def _identify_optimization_opportunities(self,
                                                   execution_result: Dict[str, Any],
                                                   context: Dict[str, Any],
                                                   performance_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify specific optimization opportunities."""
        opportunities = []

        # Check if performance is below baseline
        if performance_analysis.get('status') == 'needs_improvement':
            opportunities.append({
                'type': 'performance',
                'severity': 'high',
                'description': 'Execution time significantly above baseline',
                'impact': 'high'
            })

        # Check for common anti-patterns from context
        config = context.get('config', {})

        # Check resource allocation
        resource_opp = self._check_resource_allocation(config)
        if resource_opp:
            opportunities.extend(resource_opp)

        # Check for join optimizations
        transformations = config.get('transformations', [])
        join_opp = self._check_join_patterns(transformations)
        if join_opp:
            opportunities.extend(join_opp)

        return opportunities

    def _check_resource_allocation(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check resource allocation for optimization opportunities."""
        opportunities = []

        resources = config.get('resource_allocation', {})
        workers = resources.get('workers', 10)
        instance_type = resources.get('worker_type', 'G.1X')

        # Check if under-resourced
        workload = config.get('workload', {})
        data_volume = workload.get('data_volume', 'medium')

        if data_volume == 'high' and workers < 20:
            opportunities.append({
                'type': 'resource_allocation',
                'severity': 'medium',
                'description': f'High data volume with only {workers} workers',
                'recommendation': 'Consider increasing workers to 20-30 for high data volume',
                'impact': 'medium'
            })

        if data_volume == 'low' and workers > 5:
            opportunities.append({
                'type': 'resource_optimization',
                'severity': 'low',
                'description': f'Low data volume with {workers} workers may be over-provisioned',
                'recommendation': 'Consider reducing workers to 3-5 for cost savings',
                'impact': 'low'
            })

        return opportunities

    def _check_join_patterns(self, transformations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Check join patterns for optimization."""
        opportunities = []

        joins = [t for t in transformations if t.get('type') in ['inner_join', 'left_join']]

        if len(joins) > 2:
            opportunities.append({
                'type': 'join_optimization',
                'severity': 'high',
                'description': f'{len(joins)} joins detected',
                'recommendation': 'Review join order and consider broadcast joins for dimension tables',
                'impact': 'high'
            })

        return opportunities

    async def _generate_specific_recommendations(self,
                                                 execution_result: Dict[str, Any],
                                                 opportunities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate specific, actionable recommendations."""
        recommendations = []

        for opp in opportunities:
            if 'recommendation' in opp:
                recommendations.append({
                    'title': opp['type'].replace('_', ' ').title(),
                    'description': opp['description'],
                    'action': opp['recommendation'],
                    'priority': self._severity_to_priority(opp['severity']),
                    'estimated_impact': opp['impact']
                })

        # Add AI-generated recommendations
        ai_recs = await self._get_ai_optimization_recommendations(
            execution_result, opportunities
        )

        if ai_recs:
            recommendations.append({
                'title': 'AI-Generated Insights',
                'description': ai_recs,
                'action': 'Review AI recommendations and implement as appropriate',
                'priority': 'medium',
                'estimated_impact': 'medium'
            })

        return recommendations

    def _prioritize_recommendations(self, recommendations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prioritize recommendations by impact and effort."""
        priority_order = {'high': 3, 'medium': 2, 'low': 1}

        sorted_recs = sorted(
            recommendations,
            key=lambda x: priority_order.get(x.get('priority', 'low'), 0),
            reverse=True
        )

        return sorted_recs[:3]  # Top 3 priorities

    def _estimate_improvement(self, recommendations: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Estimate potential improvement from recommendations."""
        if not recommendations:
            return {'estimated_improvement_pct': 0, 'confidence': 'low'}

        # Estimate based on number and priority of recommendations
        high_priority = len([r for r in recommendations if r.get('priority') == 'high'])
        medium_priority = len([r for r in recommendations if r.get('priority') == 'medium'])

        estimated_pct = (high_priority * 15) + (medium_priority * 5)  # Rough estimate

        confidence = 'high' if high_priority > 0 else 'medium' if medium_priority > 0 else 'low'

        return {
            'estimated_improvement_pct': min(estimated_pct, 50),  # Cap at 50%
            'confidence': confidence,
            'high_priority_count': high_priority,
            'medium_priority_count': medium_priority
        }

    def _severity_to_priority(self, severity: str) -> str:
        """Convert severity to priority."""
        mapping = {'high': 'high', 'medium': 'medium', 'low': 'low'}
        return mapping.get(severity, 'medium')

    def _analyze_configuration(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze configuration for issues."""
        issues = []

        # Check timeout settings
        timeout = config.get('resource_allocation', {}).get('timeout_minutes', 60)
        if timeout > 120:
            issues.append({
                'area': 'timeout',
                'issue': f'Timeout set to {timeout} minutes is very high',
                'recommendation': 'Review if such long timeout is necessary'
            })

        return issues

    def _find_successful_patterns(self, context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Find similar successful execution patterns."""
        successful_patterns = []

        for pattern in self.optimization_patterns:
            if pattern.get('efficiency', 0) > 0.85:  # High efficiency threshold
                successful_patterns.append({
                    'platform': pattern.get('platform'),
                    'execution_time': pattern.get('execution_time'),
                    'efficiency': pattern.get('efficiency')
                })

        return successful_patterns[:5]  # Top 5

    async def _get_ai_recommendations(self,
                                     context: Dict[str, Any],
                                     config: Dict[str, Any],
                                     config_issues: List[Dict],
                                     similar_patterns: List[Dict]) -> str:
        """Get AI-powered recommendations."""
        prompt = f"""
        Provide optimization recommendations for this ETL pipeline:

        Configuration: {json.dumps(config, indent=2)[:500]}
        Configuration Issues: {json.dumps(config_issues, indent=2)}
        Similar Successful Patterns: {json.dumps(similar_patterns, indent=2)}

        Provide 2-3 specific, actionable optimization recommendations.
        """

        return await self.invoke_bedrock(prompt, "You are an ETL optimization expert.")

    async def _get_ai_optimization_recommendations(self,
                                                   execution_result: Dict[str, Any],
                                                   opportunities: List[Dict]) -> str:
        """Get AI optimization recommendations based on execution."""
        prompt = f"""
        Based on this execution and identified opportunities, provide specific optimizations:

        Execution Platform: {execution_result.get('platform')}
        Opportunities: {json.dumps(opportunities, indent=2)}

        Provide 2-3 most impactful optimizations.
        """

        return await self.invoke_bedrock(prompt, "You are a performance optimization specialist.")

    async def _store_optimization_report(self,
                                        report: Dict[str, Any],
                                        message: StrandsMessage):
        """Store optimization report as learning vector."""
        vector = {
            'vector_id': report['report_id'],
            'timestamp': datetime.utcnow().isoformat(),
            'agent_type': 'optimization',
            'optimization_report': report,
            'correlation_id': message.correlation_id
        }

        await self.store_learning_vector(vector)


# Standalone execution
async def main():
    """Run Optimization Agent as standalone service."""
    from strands_message_bus import start_message_bus
    import asyncio

    bus_task = asyncio.create_task(start_message_bus())
    agent = StrandsOptimizationAgent()
    agent_task = asyncio.create_task(agent.start())

    try:
        await asyncio.gather(bus_task, agent_task)
    except KeyboardInterrupt:
        logger.info("Shutting down Optimization Agent...")
        await agent.stop()


if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
