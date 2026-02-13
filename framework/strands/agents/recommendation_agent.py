#!/usr/bin/env python3
"""Strands Recommendation Agent - Aggregates and prioritizes recommendations."""

from typing import Dict, List, Any
from ..base_agent import StrandsAgent, AgentResult, AgentStatus, AgentContext, register_agent
from ..storage import StrandsStorage


@register_agent
class StrandsRecommendationAgent(StrandsAgent):
    """Aggregates recommendations from all agents and prioritizes them."""

    AGENT_NAME = "recommendation_agent"
    AGENT_VERSION = "2.0.0"
    AGENT_DESCRIPTION = "Aggregates and prioritizes recommendations from all agents"

    # This agent runs last - depends on all others
    DEPENDENCIES = [
        'sizing_agent',
        'compliance_agent',
        'data_quality_agent',
        'code_analysis_agent',
        'resource_allocator_agent',
        'platform_conversion_agent',
        'code_conversion_agent',
        'healing_agent',
        'learning_agent'
    ]
    PARALLEL_SAFE = True

    PRIORITY_WEIGHTS = {
        'critical': 100,
        'high': 75,
        'medium': 50,
        'low': 25,
        'info': 10
    }

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.storage = StrandsStorage(config)

    def execute(self, context: AgentContext) -> AgentResult:
        rec_config = context.config.get('recommendation', {})
        if not self.is_enabled('recommendation.enabled'):
            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.COMPLETED,
                output={'skipped': True}
            )

        # Collect recommendations from all agents
        all_recommendations = []

        for agent_name, result in context.agent_results.items():
            if result.status == AgentStatus.COMPLETED:
                for rec in result.recommendations:
                    all_recommendations.append({
                        'source_agent': agent_name,
                        'recommendation': rec,
                        'priority': self._determine_priority(rec, result)
                    })

        # Prioritize recommendations
        if rec_config.get('prioritize_by_impact') in ('Y', 'y', True):
            all_recommendations.sort(
                key=lambda x: self.PRIORITY_WEIGHTS.get(x['priority'], 50),
                reverse=True
            )

        # Generate implementation plan
        implementation_plan = None
        if rec_config.get('generate_implementation_plan') in ('Y', 'y', True):
            implementation_plan = self._generate_implementation_plan(all_recommendations)

        # Store recommendations
        self.storage.store_agent_data(
            self.AGENT_NAME,
            'recommendations',
            all_recommendations,
            use_pipe_delimited=True
        )

        if implementation_plan:
            self.storage.store_agent_data(
                self.AGENT_NAME,
                'implementation_plans',
                [implementation_plan],
                use_pipe_delimited=True
            )

        # Summary by category
        by_agent = {}
        for rec in all_recommendations:
            agent = rec['source_agent']
            if agent not in by_agent:
                by_agent[agent] = []
            by_agent[agent].append(rec['recommendation'])

        return AgentResult(
            agent_name=self.AGENT_NAME,
            agent_id=self.agent_id,
            status=AgentStatus.COMPLETED,
            output={
                'total_recommendations': len(all_recommendations),
                'recommendations': all_recommendations,
                'by_agent': by_agent,
                'implementation_plan': implementation_plan
            },
            metrics={
                'total_recommendations': len(all_recommendations),
                'agents_with_recommendations': len(by_agent)
            },
            recommendations=[]  # This agent doesn't add its own
        )

    def _determine_priority(self, recommendation: str, result: AgentResult) -> str:
        """Determine priority of a recommendation."""
        rec_lower = recommendation.lower()

        # Critical indicators
        if any(word in rec_lower for word in ['fail', 'error', 'critical', 'security']):
            return 'critical'

        # High priority
        if any(word in rec_lower for word in ['convert', 'oom', 'memory', 'platform']):
            return 'high'

        # Medium priority
        if any(word in rec_lower for word in ['optimize', 'recommend', 'adjust']):
            return 'medium'

        # Low priority
        if any(word in rec_lower for word in ['consider', 'may', 'could']):
            return 'low'

        return 'info'

    def _generate_implementation_plan(self, recommendations: List[Dict]) -> Dict:
        """Generate implementation plan from recommendations."""
        plan = {
            'generated_at': datetime.utcnow().isoformat() if 'datetime' in dir() else None,
            'phases': []
        }

        # Group by priority
        critical = [r for r in recommendations if r['priority'] == 'critical']
        high = [r for r in recommendations if r['priority'] == 'high']
        medium = [r for r in recommendations if r['priority'] == 'medium']
        low = [r for r in recommendations if r['priority'] in ['low', 'info']]

        if critical:
            plan['phases'].append({
                'phase': 1,
                'name': 'Critical Fixes',
                'items': [r['recommendation'] for r in critical]
            })

        if high:
            plan['phases'].append({
                'phase': 2,
                'name': 'High Priority Optimizations',
                'items': [r['recommendation'] for r in high]
            })

        if medium:
            plan['phases'].append({
                'phase': 3,
                'name': 'Performance Improvements',
                'items': [r['recommendation'] for r in medium]
            })

        if low:
            plan['phases'].append({
                'phase': 4,
                'name': 'Future Considerations',
                'items': [r['recommendation'] for r in low]
            })

        return plan


# Import datetime for the plan generation
from datetime import datetime
