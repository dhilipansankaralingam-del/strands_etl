"""
ETL Swarm Orchestrator
======================

Multi-agent orchestration using Strands SDK Swarm.

The Swarm enables:
- Agents to hand off tasks to each other
- Parallel execution of independent tasks
- Shared context between agents
- Quality gates and validation

Example:
    from strands_sdk import ETLSwarm

    swarm = ETLSwarm(config_path="demo_configs/enterprise_sales_config.json")
    result = swarm.run()
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

from strands import Agent
from strands.swarm import Swarm

from .config import ETLConfig
from .agents.agent_factory import create_agent, create_all_agents
from .agents.prompts import AGENT_PROMPTS, set_prompt
from .tools.aws_tools import set_dry_run_mode

logger = logging.getLogger("strands.swarm")


class ETLSwarm:
    """
    Multi-agent ETL orchestrator using Strands SDK.

    Coordinates multiple LLM-powered agents to:
    1. Analyze source data sizes
    2. Allocate optimal resources
    3. Check compliance and data quality
    4. Execute jobs on appropriate platform
    5. Learn from execution history

    Example:
        # Basic usage
        swarm = ETLSwarm(config_path="config.json")
        result = swarm.run()

        # With custom model
        swarm = ETLSwarm(
            config_path="config.json",
            model_id="us.anthropic.claude-sonnet-4-20250514-v1:0"
        )

        # Dry-run mode (no actual AWS calls)
        swarm = ETLSwarm(config_path="config.json", dry_run=True)
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        config: Optional[ETLConfig] = None,
        model_id: str = "us.anthropic.claude-sonnet-4-20250514-v1:0",
        dry_run: bool = False,
        custom_prompts: Optional[Dict[str, str]] = None
    ):
        """
        Initialize the ETL Swarm.

        Args:
            config_path: Path to configuration JSON file
            config: Pre-loaded ETLConfig object
            model_id: Bedrock model ID for agents
            dry_run: If True, simulate AWS operations
            custom_prompts: Dictionary of agent_name -> custom_prompt
        """
        # Load configuration
        if config:
            self.config = config
        elif config_path:
            self.config = ETLConfig.from_file(config_path)
        else:
            raise ValueError("Must provide either config_path or config")

        self.model_id = model_id
        self.dry_run = dry_run or self.config.dry_run
        self.custom_prompts = custom_prompts or {}

        # Set dry-run mode for tools
        set_dry_run_mode(self.dry_run)

        # Create agents
        self.agents = self._create_agents()

        # Create Strands Swarm
        self.swarm = Swarm(agents=list(self.agents.values()))

        # Execution state
        self.execution_id = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        self.results: Dict[str, Any] = {}

        logger.info(f"Initialized ETL Swarm with {len(self.agents)} agents")
        if self.dry_run:
            logger.info("Running in DRY-RUN mode - no actual AWS operations")

    def _create_agents(self) -> Dict[str, Agent]:
        """Create all agents with configuration context."""
        # Apply any custom prompts
        for agent_name, prompt in self.custom_prompts.items():
            set_prompt(agent_name, prompt)

        return create_all_agents(
            model_id=self.model_id,
            custom_prompts=self.custom_prompts
        )

    def run(self) -> Dict[str, Any]:
        """
        Run the complete ETL workflow.

        Returns:
            Dictionary with execution results
        """
        start_time = datetime.utcnow()
        logger.info(f"Starting ETL Swarm execution: {self.execution_id}")

        # Build the task description for the swarm
        task = self._build_task_prompt()

        try:
            # Invoke the swarm
            result = self.swarm.invoke(task)

            # Extract and structure results
            self.results = {
                "execution_id": self.execution_id,
                "status": "completed",
                "job_name": self.config.job_name,
                "dry_run": self.dry_run,
                "started_at": start_time.isoformat(),
                "completed_at": datetime.utcnow().isoformat(),
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
                "swarm_response": str(result),
                "agent_count": len(self.agents)
            }

            logger.info(f"ETL Swarm completed in {self.results['duration_seconds']:.1f}s")

        except Exception as e:
            logger.error(f"ETL Swarm failed: {e}")
            self.results = {
                "execution_id": self.execution_id,
                "status": "failed",
                "error": str(e),
                "started_at": start_time.isoformat(),
                "completed_at": datetime.utcnow().isoformat()
            }

        return self.results

    def _build_task_prompt(self) -> str:
        """Build the task prompt for the swarm."""
        tables_info = json.dumps(self.config.source_tables[:5], indent=2)  # First 5 tables

        return f"""Execute ETL job: {self.config.job_name}

CONFIGURATION:
- Total source tables: {len(self.config.source_tables)}
- Primary platform: {self.config.platform.get('primary', 'glue')}
- Dry-run mode: {self.dry_run}

SAMPLE TABLES:
{tables_info}

WORKFLOW:
1. SIZING: Use sizing_agent to detect table sizes. Call get_table_size for each table.
2. RESOURCE ALLOCATION: Based on total size, recommend workers and type.
3. PLATFORM DECISION: Choose Glue (<100GB), EMR (100-500GB), or EKS (>500GB).
4. COMPLIANCE CHECK: Identify PII columns needing masking.
5. EXECUTION: Start the job on the chosen platform.
6. LEARNING: Save execution record for ML training.
7. RECOMMENDATIONS: Provide final recommendations.

Execute each step and provide a comprehensive summary."""

    def run_agent(self, agent_name: str, task: str) -> Any:
        """
        Run a single agent with a specific task.

        Args:
            agent_name: Name of the agent to run
            task: Task description for the agent

        Returns:
            Agent response
        """
        if agent_name not in self.agents:
            raise ValueError(f"Unknown agent: {agent_name}")

        agent = self.agents[agent_name]
        return agent(task)

    def customize_agent(self, agent_name: str, new_prompt: str) -> None:
        """
        Customize an agent's system prompt.

        Args:
            agent_name: Name of the agent to customize
            new_prompt: New system prompt

        Example:
            swarm.customize_agent('sizing_agent', '''
            You are a sizing agent specialized in Delta Lake tables.
            Focus on Z-ordering and partition pruning.
            ''')
        """
        set_prompt(agent_name, new_prompt)

        # Recreate the agent with new prompt
        self.agents[agent_name] = create_agent(
            agent_name=agent_name,
            model_id=self.model_id,
            custom_prompt=new_prompt
        )

        # Rebuild swarm
        self.swarm = Swarm(agents=list(self.agents.values()))

        logger.info(f"Customized agent: {agent_name}")

    def get_agent(self, agent_name: str) -> Agent:
        """Get an agent by name."""
        if agent_name not in self.agents:
            raise ValueError(f"Unknown agent: {agent_name}")
        return self.agents[agent_name]

    def list_agents(self) -> List[str]:
        """List all agent names."""
        return list(self.agents.keys())


def run_quick_analysis(
    config_path: str,
    model_id: str = "us.anthropic.claude-sonnet-4-20250514-v1:0"
) -> Dict[str, Any]:
    """
    Run a quick analysis without full execution.

    Args:
        config_path: Path to configuration file
        model_id: Bedrock model ID

    Returns:
        Analysis results
    """
    swarm = ETLSwarm(
        config_path=config_path,
        model_id=model_id,
        dry_run=True
    )

    # Run only sizing and resource allocation
    config = ETLConfig.from_file(config_path)

    sizing_task = f"""
    Analyze the following tables and provide size estimates:
    {json.dumps(config.source_tables[:10], indent=2)}

    Use get_table_size for each table and sum the total.
    """

    sizing_result = swarm.run_agent('sizing_agent', sizing_task)

    return {
        "type": "quick_analysis",
        "sizing_result": str(sizing_result),
        "tables_analyzed": len(config.source_tables)
    }
