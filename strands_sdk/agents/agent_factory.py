"""
Agent Factory
=============

Creates Strands SDK agents with appropriate tools and system prompts.

Each agent is an LLM-powered entity that can:
- Reason about problems using the system prompt
- Use tools to interact with AWS services
- Collaborate with other agents via Swarm
"""

import logging
from typing import Dict, List, Any, Optional

from strands import Agent

from .prompts import AGENT_PROMPTS, get_prompt
from ..tools.aws_tools import (
    get_table_size,
    list_glue_tables,
    start_glue_job,
    get_glue_job_status,
    get_cloudwatch_metrics,
    start_emr_step,
    submit_eks_job
)
from ..tools.storage_tools import (
    write_audit_log,
    store_recommendations,
    load_execution_history,
    save_execution_history
)

logger = logging.getLogger("strands.agents")

# Tool assignments for each agent
AGENT_TOOLS = {
    "sizing_agent": [
        get_table_size,
        list_glue_tables,
        write_audit_log
    ],

    "resource_allocator_agent": [
        write_audit_log
    ],

    "platform_conversion_agent": [
        write_audit_log
    ],

    "code_conversion_agent": [
        write_audit_log
    ],

    "compliance_agent": [
        write_audit_log
    ],

    "data_quality_agent": [
        write_audit_log
    ],

    "execution_agent": [
        start_glue_job,
        get_glue_job_status,
        get_cloudwatch_metrics,
        start_emr_step,
        submit_eks_job,
        write_audit_log,
        save_execution_history
    ],

    "learning_agent": [
        load_execution_history,
        save_execution_history,
        write_audit_log
    ],

    "recommendation_agent": [
        store_recommendations,
        write_audit_log
    ],

    "healing_agent": [
        write_audit_log
    ]
}


def create_agent(
    agent_name: str,
    model_id: str = "us.anthropic.claude-sonnet-4-20250514-v1:0",
    custom_prompt: Optional[str] = None,
    additional_tools: Optional[List] = None,
    **kwargs
) -> Agent:
    """
    Create a Strands SDK agent with tools and system prompt.

    Args:
        agent_name: Name of the agent (e.g., 'sizing_agent')
        model_id: Bedrock model ID
        custom_prompt: Override the default system prompt
        additional_tools: Extra tools to add to the agent
        **kwargs: Additional arguments for Agent constructor

    Returns:
        Configured Strands Agent instance

    Example:
        # Create with default prompt
        agent = create_agent('sizing_agent')

        # Create with custom prompt
        agent = create_agent('sizing_agent', custom_prompt='Focus only on Parquet files.')

        # Create with additional tools
        agent = create_agent('sizing_agent', additional_tools=[my_custom_tool])
    """
    # Get system prompt
    system_prompt = custom_prompt or get_prompt(agent_name)

    # Get tools for this agent
    tools = list(AGENT_TOOLS.get(agent_name, []))

    # Add additional tools if provided
    if additional_tools:
        tools.extend(additional_tools)

    # Create agent with Bedrock model
    agent = Agent(
        model=model_id,
        system_prompt=system_prompt,
        tools=tools if tools else None,
        **kwargs
    )

    logger.info(f"Created agent: {agent_name} with {len(tools)} tools")
    return agent


def create_all_agents(
    model_id: str = "us.anthropic.claude-sonnet-4-20250514-v1:0",
    agent_names: Optional[List[str]] = None,
    custom_prompts: Optional[Dict[str, str]] = None
) -> Dict[str, Agent]:
    """
    Create all ETL agents.

    Args:
        model_id: Bedrock model ID for all agents
        agent_names: List of agent names to create (default: all)
        custom_prompts: Dictionary of agent_name -> custom_prompt overrides

    Returns:
        Dictionary of agent_name -> Agent instance

    Example:
        # Create all agents with defaults
        agents = create_all_agents()

        # Create specific agents
        agents = create_all_agents(agent_names=['sizing_agent', 'execution_agent'])

        # Create with custom prompts
        agents = create_all_agents(custom_prompts={
            'sizing_agent': 'Focus on Delta Lake tables only.'
        })
    """
    custom_prompts = custom_prompts or {}
    agent_names = agent_names or list(AGENT_PROMPTS.keys())

    agents = {}
    for name in agent_names:
        if name not in AGENT_PROMPTS:
            logger.warning(f"Unknown agent: {name}, skipping")
            continue

        custom_prompt = custom_prompts.get(name)
        agents[name] = create_agent(
            agent_name=name,
            model_id=model_id,
            custom_prompt=custom_prompt
        )

    logger.info(f"Created {len(agents)} agents")
    return agents


def get_available_agents() -> List[str]:
    """Get list of available agent names."""
    return list(AGENT_PROMPTS.keys())


def get_agent_tools(agent_name: str) -> List[str]:
    """Get list of tool names available to an agent."""
    tools = AGENT_TOOLS.get(agent_name, [])
    return [t.__name__ for t in tools]
