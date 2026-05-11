"""Super Prompts for Cost Optimization Agents"""

from .super_prompts import (
    AGENT_PROMPTS,
    SIZE_ANALYZER_PROMPT,
    RESOURCE_ALLOCATOR_PROMPT,
    CODE_ANALYZER_PROMPT,
    RECOMMENDATIONS_PROMPT,
    ORCHESTRATOR_PROMPT,
    get_prompt,
    customize_prompt
)

__all__ = [
    'AGENT_PROMPTS',
    'SIZE_ANALYZER_PROMPT',
    'RESOURCE_ALLOCATOR_PROMPT',
    'CODE_ANALYZER_PROMPT',
    'RECOMMENDATIONS_PROMPT',
    'ORCHESTRATOR_PROMPT',
    'get_prompt',
    'customize_prompt'
]
