"""
ETL Multi-Agent Orchestrator
=============================

Multi-agent orchestration using Strands SDK Agent class.

This orchestrator:
- Coordinates multiple LLM-powered agents
- Runs agents in defined phases (parallel when possible)
- Passes context between agents
- Collects and aggregates results

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
from concurrent.futures import ThreadPoolExecutor, as_completed

from strands import Agent

from .config import ETLConfig
from .agents.agent_factory import create_agent, create_all_agents
from .agents.prompts import AGENT_PROMPTS, set_prompt
from .tools.aws_tools import set_dry_run_mode

logger = logging.getLogger("strands.orchestrator")


# Define execution phases - agents in same phase can run in parallel
EXECUTION_PHASES = [
    # Phase 1: Data discovery (parallel)
    ["sizing_agent", "compliance_agent"],

    # Phase 2: Resource planning (depends on sizing)
    ["resource_allocator_agent", "data_quality_agent"],

    # Phase 3: Platform decision (depends on resources)
    ["platform_conversion_agent"],

    # Phase 4: Code preparation
    ["code_conversion_agent", "healing_agent"],

    # Phase 5: Execution
    ["execution_agent"],

    # Phase 6: Learning and recommendations
    ["learning_agent", "recommendation_agent"]
]


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
        dry_run: bool = True,
        custom_prompts: Optional[Dict[str, str]] = None,
        max_workers: int = 4
    ):
        """
        Initialize the ETL Orchestrator.

        Args:
            config_path: Path to configuration JSON file
            config: Pre-loaded ETLConfig object
            model_id: Bedrock model ID for agents
            dry_run: If True, simulate AWS operations (default: True)
            custom_prompts: Dictionary of agent_name -> custom_prompt
            max_workers: Max parallel agents per phase
        """
        # Load configuration
        if config:
            self.config = config
        elif config_path:
            self.config = ETLConfig.from_file(config_path)
        else:
            raise ValueError("Must provide either config_path or config")

        self.model_id = model_id
        self.dry_run = dry_run if dry_run is not None else self.config.dry_run
        self.custom_prompts = custom_prompts or {}
        self.max_workers = max_workers

        # Set dry-run mode for tools
        set_dry_run_mode(self.dry_run)

        # Create agents
        self.agents: Dict[str, Agent] = {}

        # Execution state
        self.execution_id = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        self.results: Dict[str, Any] = {}
        self.agent_outputs: Dict[str, Any] = {}
        self.shared_context: Dict[str, Any] = {}

        logger.info(f"Initialized ETL Orchestrator")
        if self.dry_run:
            logger.info("Running in DRY-RUN mode - no actual AWS operations")

    def _create_agent(self, agent_name: str) -> Agent:
        """Create a single agent on demand."""
        if agent_name not in self.agents:
            custom_prompt = self.custom_prompts.get(agent_name)
            self.agents[agent_name] = create_agent(
                agent_name=agent_name,
                model_id=self.model_id,
                custom_prompt=custom_prompt
            )
            logger.info(f"Created agent: {agent_name}")
        return self.agents[agent_name]

    def _build_agent_task(self, agent_name: str) -> str:
        """Build task prompt for a specific agent with context."""
        tables_info = json.dumps(self.config.source_tables[:5], indent=2)

        # Base context
        context = f"""
JOB: {self.config.job_name}
EXECUTION ID: {self.execution_id}
DRY RUN: {self.dry_run}
TOTAL TABLES: {len(self.config.source_tables)}

SAMPLE TABLES:
{tables_info}

PREVIOUS AGENT OUTPUTS:
{json.dumps(self.shared_context, indent=2, default=str)}
"""

        # Agent-specific tasks
        tasks = {
            "sizing_agent": f"""
{context}

YOUR TASK: Analyze and determine the size of all source tables.

INSTRUCTIONS:
1. For each table in the configuration, call get_table_size(database, table_name)
2. Sum up all table sizes to get total_size_gb
3. Identify any tables larger than 50GB as "large tables"
4. Return a summary with:
   - total_size_gb
   - table_count
   - large_tables list
   - sizing_method used
""",

            "compliance_agent": f"""
{context}

YOUR TASK: Check compliance requirements for all source tables.

INSTRUCTIONS:
1. Review each table's columns for PII patterns
2. Check for: SSN, email, phone, address, credit_card, DOB
3. Verify encryption and masking requirements
4. Return findings with:
   - tables_with_pii
   - required_masking_actions
   - compliance_status
""",

            "resource_allocator_agent": f"""
{context}

YOUR TASK: Recommend optimal resource allocation based on data size.

INSTRUCTIONS:
1. Use the total_size_gb from sizing_agent output
2. Apply the resource guidelines:
   - < 10 GB: 2-5 workers, G.1X
   - 10-50 GB: 5-10 workers, G.1X
   - 50-100 GB: 10-20 workers, G.2X
   - 100-500 GB: 20-40 workers, G.2X/G.4X
   - > 500 GB: 40-100 workers, G.4X/G.8X
3. Return recommendations:
   - recommended_workers
   - recommended_worker_type
   - estimated_cost_per_hour
   - reasoning
""",

            "data_quality_agent": f"""
{context}

YOUR TASK: Define and evaluate data quality rules.

INSTRUCTIONS:
1. Define quality rules for key columns
2. Check for null rates, duplicates, outliers
3. Return:
   - quality_rules defined
   - expected_pass_rate
   - critical_checks
""",

            "platform_conversion_agent": f"""
{context}

YOUR TASK: Recommend the optimal execution platform.

INSTRUCTIONS:
1. Use total_size_gb from sizing output
2. Apply platform selection:
   - < 100 GB: AWS Glue
   - 100-500 GB: EMR
   - > 500 GB: EKS with Karpenter
3. Return:
   - recommended_platform
   - conversion_needed (boolean)
   - reasoning
""",

            "code_conversion_agent": f"""
{context}

YOUR TASK: Determine if code conversion is needed.

INSTRUCTIONS:
1. Check if platform conversion requires code changes
2. If converting from Glue to EMR/EKS:
   - GlueContext → SparkSession
   - DynamicFrame → DataFrame
3. Return:
   - conversion_required
   - conversion_type
   - key_changes
""",

            "healing_agent": f"""
{context}

YOUR TASK: Prepare data healing strategies.

INSTRUCTIONS:
1. Based on data quality findings, prepare healing strategies
2. Define auto-heal rules for common issues
3. Return:
   - healing_strategies
   - auto_heal_enabled
   - manual_review_items
""",

            "execution_agent": f"""
{context}

YOUR TASK: Execute the ETL job on the recommended platform.

INSTRUCTIONS:
1. Get the recommended platform from previous agents
2. Start the job using appropriate tool:
   - Glue: use start_glue_job()
   - EMR: use start_emr_step()
   - EKS: use submit_eks_job()
3. Return:
   - job_id
   - status
   - platform_used
   - execution_details
""",

            "learning_agent": f"""
{context}

YOUR TASK: Save execution data for ML training.

INSTRUCTIONS:
1. Compile execution record from all agent outputs
2. Use save_execution_history() to persist
3. Return:
   - record_saved
   - training_data_points
   - model_update_needed
""",

            "recommendation_agent": f"""
{context}

YOUR TASK: Synthesize recommendations from all agents.

INSTRUCTIONS:
1. Review outputs from all previous agents
2. Prioritize recommendations by impact
3. Return:
   - top_recommendations (max 5)
   - action_items
   - estimated_savings
"""
        }

        return tasks.get(agent_name, f"{context}\n\nExecute your designated task for this ETL job.")

    def _run_agent(self, agent_name: str) -> Dict[str, Any]:
        """Run a single agent and capture output."""
        start_time = datetime.utcnow()

        try:
            agent = self._create_agent(agent_name)
            task = self._build_agent_task(agent_name)

            logger.info(f"Running agent: {agent_name}")

            # Call the agent
            response = agent(task)

            # Extract response text
            response_text = str(response)

            duration = (datetime.utcnow() - start_time).total_seconds()

            result = {
                "agent_name": agent_name,
                "status": "completed",
                "response": response_text[:2000],  # Truncate for storage
                "duration_seconds": duration,
                "timestamp": datetime.utcnow().isoformat()
            }

            logger.info(f"Agent {agent_name} completed in {duration:.1f}s")
            return result

        except Exception as e:
            logger.error(f"Agent {agent_name} failed: {e}")
            return {
                "agent_name": agent_name,
                "status": "failed",
                "error": str(e),
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
                "timestamp": datetime.utcnow().isoformat()
            }

    def _run_phase(self, phase_agents: List[str]) -> Dict[str, Any]:
        """Run a phase of agents (potentially in parallel)."""
        results = {}

        if len(phase_agents) == 1:
            # Single agent - run directly
            agent_name = phase_agents[0]
            results[agent_name] = self._run_agent(agent_name)
        else:
            # Multiple agents - run in parallel
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self._run_agent, name): name
                    for name in phase_agents
                }

                for future in as_completed(futures):
                    agent_name = futures[future]
                    try:
                        results[agent_name] = future.result()
                    except Exception as e:
                        results[agent_name] = {
                            "agent_name": agent_name,
                            "status": "failed",
                            "error": str(e)
                        }

        # Update shared context with results
        for agent_name, result in results.items():
            if result.get("status") == "completed":
                self.shared_context[agent_name] = result.get("response", "")

        return results

    def run(self) -> Dict[str, Any]:
        """
        Run the complete ETL workflow through all phases.

        Returns:
            Dictionary with execution results
        """
        start_time = datetime.utcnow()
        logger.info(f"Starting ETL orchestration: {self.execution_id}")
        logger.info(f"Job: {self.config.job_name}")
        logger.info(f"Phases: {len(EXECUTION_PHASES)}")

        all_results = {}
        completed_count = 0
        failed_count = 0

        try:
            for phase_num, phase_agents in enumerate(EXECUTION_PHASES, 1):
                logger.info(f"\n{'='*50}")
                logger.info(f"PHASE {phase_num}: {phase_agents}")
                logger.info(f"{'='*50}")

                phase_results = self._run_phase(phase_agents)
                all_results.update(phase_results)

                # Count results
                for result in phase_results.values():
                    if result.get("status") == "completed":
                        completed_count += 1
                    else:
                        failed_count += 1

            # Build final result
            duration = (datetime.utcnow() - start_time).total_seconds()

            self.results = {
                "execution_id": self.execution_id,
                "status": "completed" if failed_count == 0 else "partial",
                "job_name": self.config.job_name,
                "dry_run": self.dry_run,
                "started_at": start_time.isoformat(),
                "completed_at": datetime.utcnow().isoformat(),
                "duration_seconds": duration,
                "agents_completed": completed_count,
                "agents_failed": failed_count,
                "agent_results": all_results,
                "shared_context": self.shared_context
            }

            logger.info(f"\nOrchestration completed in {duration:.1f}s")
            logger.info(f"Completed: {completed_count}, Failed: {failed_count}")

        except Exception as e:
            logger.error(f"Orchestration failed: {e}")
            self.results = {
                "execution_id": self.execution_id,
                "status": "failed",
                "error": str(e),
                "started_at": start_time.isoformat(),
                "completed_at": datetime.utcnow().isoformat()
            }

        return self.results

    def run_agent(self, agent_name: str, task: str) -> Any:
        """
        Run a single agent with a specific task.

        Args:
            agent_name: Name of the agent to run
            task: Task description for the agent

        Returns:
            Agent response
        """
        agent = self._create_agent(agent_name)
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
        self.custom_prompts[agent_name] = new_prompt

        # Remove cached agent so it gets recreated with new prompt
        if agent_name in self.agents:
            del self.agents[agent_name]

        logger.info(f"Customized agent: {agent_name}")

    def get_agent(self, agent_name: str) -> Agent:
        """Get an agent by name (creates if needed)."""
        return self._create_agent(agent_name)

    def list_agents(self) -> List[str]:
        """List all available agent names."""
        return list(AGENT_PROMPTS.keys())


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
