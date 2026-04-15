#!/usr/bin/env python3
"""
Strands Enhanced ETL Framework Demo
====================================

This script demonstrates the improved Strands integration using:
- strands-agents-tools package
- Graph-based multi-agent orchestration
- Plugin system for cross-cutting concerns
- Memory management for learning

Run with:
    python scripts/strands_enhanced_demo.py --config demo_configs/enterprise_sales_config.json
"""

import json
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

# ============================================================================
# STRANDS CORE IMPORTS
# ============================================================================

try:
    from strands import Agent, tool
    STRANDS_AVAILABLE = True
except ImportError:
    STRANDS_AVAILABLE = False
    print("WARNING: strands package not installed. Install with: pip install strands-agents")

# ============================================================================
# STRANDS-AGENTS-TOOLS IMPORTS (Enhanced Tools)
# ============================================================================

try:
    from strands_tools import (
        use_aws,      # AWS service integration
        batch,        # Parallel tool execution
        workflow,     # Multi-step workflows
        think,        # Advanced reasoning
        diagram,      # Architecture diagrams
        shell,        # System commands
        # memory tools
        # mem0_memory,  # Persistent memory
    )
    STRANDS_TOOLS_AVAILABLE = True
except ImportError:
    STRANDS_TOOLS_AVAILABLE = False
    print("INFO: strands-agents-tools not installed. Install with: pip install strands-agents-tools")

# ============================================================================
# GRAPH MULTI-AGENT IMPORTS
# ============================================================================

try:
    from strands.multiagent import Graph, Swarm
    from strands.multiagent.graph import GraphNode
    MULTIAGENT_AVAILABLE = True
except ImportError:
    MULTIAGENT_AVAILABLE = False
    print("INFO: strands multiagent module not available in current version")


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("strands.enhanced")


# ============================================================================
# PLUGIN SYSTEM EXAMPLE
# ============================================================================

class ETLAuditPlugin:
    """
    Plugin for cross-cutting audit logging.

    When strands Plugin base class is available, extend from it:

        from strands import Plugin, hook

        class ETLAuditPlugin(Plugin):
            name = "etl_audit"

            @hook
            def before_tool_call(self, tool_name: str, args: dict):
                self.log_audit(tool_name, args)
    """

    name = "etl_audit"

    def __init__(self, audit_dir: str = "data/audit_logs"):
        self.audit_dir = Path(audit_dir)
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        self.events: List[Dict[str, Any]] = []

    def log_event(self, event_type: str, agent_name: str, data: Dict[str, Any]) -> None:
        """Log an audit event."""
        event = {
            "timestamp": datetime.utcnow().isoformat(),
            "event_type": event_type,
            "agent_name": agent_name,
            "data": data
        }
        self.events.append(event)
        logger.info(f"[AUDIT] {event_type}: {agent_name}")

    def before_agent_run(self, agent_name: str, task: str) -> None:
        """Hook called before agent execution."""
        self.log_event("agent_started", agent_name, {"task_preview": task[:200]})

    def after_agent_run(self, agent_name: str, result: Any, duration_ms: float) -> None:
        """Hook called after agent execution."""
        self.log_event("agent_completed", agent_name, {
            "duration_ms": duration_ms,
            "success": True
        })

    def save_audit_log(self, job_name: str) -> str:
        """Save audit events to file."""
        filename = f"{job_name}_audit_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = self.audit_dir / filename

        with open(filepath, 'w') as f:
            json.dump(self.events, f, indent=2)

        return str(filepath)


# ============================================================================
# ENHANCED TOOLS (Using strands-agents-tools pattern)
# ============================================================================

@tool
def analyze_table_sizes_batch(
    tables: List[Dict[str, str]],
    dry_run: bool = True
) -> Dict[str, Any]:
    """
    Analyze multiple tables in parallel using batch processing.

    This demonstrates how strands-agents-tools batch tool works.
    In production, use:
        from strands_tools import batch
        batch([get_table_size(t) for t in tables])

    Args:
        tables: List of {database, table_name} dictionaries
        dry_run: Simulate AWS calls

    Returns:
        Aggregated sizing results
    """
    import random

    results = []
    total_size_gb = 0

    for table in tables:
        # Simulate parallel execution
        size_gb = random.uniform(5, 100)
        results.append({
            "database": table.get("database", "default"),
            "table_name": table.get("table_name", "unknown"),
            "size_gb": round(size_gb, 2),
            "row_count": int(size_gb * 1_000_000)
        })
        total_size_gb += size_gb

    return {
        "tables_analyzed": len(tables),
        "total_size_gb": round(total_size_gb, 2),
        "table_details": results,
        "method": "batch_parallel",
        "dry_run": dry_run
    }


@tool
def recommend_platform_with_reasoning(
    total_size_gb: float,
    job_complexity: str = "medium"
) -> Dict[str, Any]:
    """
    Recommend execution platform with step-by-step reasoning.

    This demonstrates how strands-agents-tools think tool works.
    In production, use:
        from strands_tools import think
        think("Given size X, what platform should I recommend?")

    Args:
        total_size_gb: Total data size in GB
        job_complexity: low/medium/high

    Returns:
        Platform recommendation with reasoning steps
    """
    reasoning_steps = []

    # Step 1: Evaluate size threshold
    reasoning_steps.append({
        "step": 1,
        "thought": f"Total data size is {total_size_gb} GB",
        "conclusion": "Size evaluated"
    })

    # Step 2: Apply platform rules
    if total_size_gb < 100:
        platform = "glue"
        reason = "Data under 100GB - Glue is cost-effective"
    elif total_size_gb < 500:
        platform = "emr"
        reason = "Data 100-500GB - EMR provides better performance"
    else:
        platform = "eks"
        reason = "Data over 500GB - EKS with Karpenter for auto-scaling"

    reasoning_steps.append({
        "step": 2,
        "thought": f"Applying platform selection rules for {total_size_gb}GB",
        "conclusion": reason
    })

    # Step 3: Consider complexity
    if job_complexity == "high" and platform == "glue":
        platform = "emr"
        reasoning_steps.append({
            "step": 3,
            "thought": "High complexity job needs more control",
            "conclusion": "Upgrading to EMR for complex transformations"
        })

    return {
        "recommended_platform": platform,
        "confidence": 0.95,
        "reasoning_steps": reasoning_steps,
        "total_reasoning_steps": len(reasoning_steps)
    }


@tool
def generate_architecture_diagram(
    platform: str,
    source_count: int,
    target_count: int
) -> Dict[str, Any]:
    """
    Generate AWS architecture diagram for ETL pipeline.

    This demonstrates how strands-agents-tools diagram tool works.
    In production, use:
        from strands_tools import diagram
        diagram("AWS ETL architecture with Glue, S3, and Redshift")

    Args:
        platform: Target platform (glue/emr/eks)
        source_count: Number of source systems
        target_count: Number of target systems

    Returns:
        Diagram metadata
    """
    diagram_spec = {
        "type": "aws_architecture",
        "platform": platform,
        "components": []
    }

    # Add source components
    for i in range(source_count):
        diagram_spec["components"].append({
            "type": "S3",
            "name": f"source_{i+1}",
            "role": "data_source"
        })

    # Add processing component
    if platform == "glue":
        diagram_spec["components"].append({
            "type": "Glue",
            "name": "etl_job",
            "role": "processing"
        })
    elif platform == "emr":
        diagram_spec["components"].append({
            "type": "EMR",
            "name": "spark_cluster",
            "role": "processing"
        })
    else:
        diagram_spec["components"].append({
            "type": "EKS",
            "name": "spark_k8s",
            "role": "processing"
        })

    # Add target components
    for i in range(target_count):
        diagram_spec["components"].append({
            "type": "S3",
            "name": f"target_{i+1}",
            "role": "data_target"
        })

    return {
        "diagram_generated": True,
        "component_count": len(diagram_spec["components"]),
        "diagram_spec": diagram_spec,
        "visualization_url": f"https://diagrams.internal/etl/{platform}"
    }


# ============================================================================
# ENHANCED AGENTS WITH TOOLS
# ============================================================================

# System prompts for enhanced agents
ENHANCED_PROMPTS = {
    "sizing_agent": """You are an ETL Sizing Agent using advanced batch processing.

Your capabilities:
1. Analyze multiple tables in PARALLEL using batch processing
2. Aggregate size metrics efficiently
3. Identify outliers and large tables

Use the analyze_table_sizes_batch tool to process multiple tables at once.
Always report total_size_gb and tables_analyzed.""",

    "platform_agent": """You are a Platform Selection Agent with reasoning capabilities.

Your capabilities:
1. Apply multi-step reasoning to platform decisions
2. Consider data size, complexity, and cost factors
3. Provide transparent reasoning steps

Use recommend_platform_with_reasoning to show your decision process.
Always explain WHY you chose a specific platform.""",

    "architect_agent": """You are an AWS Architecture Agent that creates diagrams.

Your capabilities:
1. Design ETL pipeline architectures
2. Generate AWS component diagrams
3. Document data flows

Use generate_architecture_diagram to visualize the pipeline.
Always include source, processing, and target components."""
}


def create_enhanced_agent(
    agent_name: str,
    model_id: str = "us.anthropic.claude-sonnet-4-20250514-v1:0"
) -> Optional[Agent]:
    """Create an agent with enhanced tools from strands-agents-tools."""

    if not STRANDS_AVAILABLE:
        logger.warning("Strands not available - returning mock agent")
        return None

    # Select tools based on agent type
    tools_map = {
        "sizing_agent": [analyze_table_sizes_batch],
        "platform_agent": [recommend_platform_with_reasoning],
        "architect_agent": [generate_architecture_diagram]
    }

    tools = tools_map.get(agent_name, [])
    prompt = ENHANCED_PROMPTS.get(agent_name, "You are an ETL assistant.")

    return Agent(
        model=model_id,
        system_prompt=prompt,
        tools=tools if tools else None
    )


# ============================================================================
# GRAPH-BASED ORCHESTRATION (Enhanced Pattern)
# ============================================================================

class GraphOrchestrator:
    """
    Graph-based multi-agent orchestrator.

    When strands Graph is available, use:

        from strands.multiagent import Graph

        graph = Graph()
        graph.add_node("sizing", sizing_agent)
        graph.add_node("platform", platform_agent, depends_on=["sizing"])
        graph.add_node("architect", architect_agent, depends_on=["platform"])
        result = await graph.execute_async()
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.edges: List[tuple] = []
        self.results: Dict[str, Any] = {}
        self.audit = ETLAuditPlugin()

    def add_node(
        self,
        name: str,
        agent: Any,
        depends_on: List[str] = None
    ) -> 'GraphOrchestrator':
        """Add an agent node to the graph."""
        self.nodes[name] = {
            "agent": agent,
            "depends_on": depends_on or []
        }

        # Add edges for dependencies
        for dep in (depends_on or []):
            self.edges.append((dep, name))

        return self

    def _get_execution_order(self) -> List[List[str]]:
        """Topological sort to get execution phases."""
        from collections import defaultdict

        in_degree = defaultdict(int)
        dependents = defaultdict(list)

        for name, node in self.nodes.items():
            for dep in node["depends_on"]:
                dependents[dep].append(name)
                in_degree[name] += 1

        # Kahn's algorithm
        phases = []
        ready = [n for n in self.nodes if in_degree[n] == 0]

        while ready:
            phases.append(ready[:])
            next_ready = []
            for name in ready:
                for dependent in dependents[name]:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        next_ready.append(dependent)
            ready = next_ready

        return phases

    def execute(self, task: str) -> Dict[str, Any]:
        """Execute the graph with dependency resolution."""
        start_time = datetime.utcnow()

        phases = self._get_execution_order()
        logger.info(f"Execution phases: {phases}")

        for phase_num, phase in enumerate(phases, 1):
            logger.info(f"=== Phase {phase_num}: {phase} ===")

            for agent_name in phase:
                node = self.nodes[agent_name]
                agent = node["agent"]

                # Inject previous results into task
                context_task = f"{task}\n\nPrevious results:\n{json.dumps(self.results, indent=2)}"

                self.audit.before_agent_run(agent_name, context_task)
                agent_start = datetime.utcnow()

                try:
                    if agent:
                        response = agent(context_task)
                        self.results[agent_name] = str(response)[:1000]
                    else:
                        # Mock response for demo
                        self.results[agent_name] = f"Simulated result from {agent_name}"

                    duration_ms = (datetime.utcnow() - agent_start).total_seconds() * 1000
                    self.audit.after_agent_run(agent_name, self.results[agent_name], duration_ms)

                except Exception as e:
                    logger.error(f"Agent {agent_name} failed: {e}")
                    self.results[agent_name] = {"error": str(e)}

        # Save audit log
        audit_file = self.audit.save_audit_log(self.config.get("job_name", "unknown"))

        return {
            "status": "completed",
            "phases_executed": len(phases),
            "agent_results": self.results,
            "audit_log": audit_file,
            "duration_seconds": (datetime.utcnow() - start_time).total_seconds()
        }


# ============================================================================
# SWARM PATTERN EXAMPLE
# ============================================================================

class SwarmOrchestrator:
    """
    Swarm-based orchestrator for collective agent behavior.

    When strands Swarm is available, use:

        from strands.multiagent import Swarm

        swarm = Swarm(
            agents=[agent1, agent2, agent3],
            coordinator=coordinator_agent,
            max_iterations=10
        )
        result = await swarm.run_async(task)
    """

    def __init__(self, agents: List[Any], coordinator: Any = None):
        self.agents = agents
        self.coordinator = coordinator
        self.shared_memory: Dict[str, Any] = {}

    def run(self, task: str) -> Dict[str, Any]:
        """Run agents in swarm mode with shared memory."""
        results = []

        for agent in self.agents:
            if agent:
                # Each agent sees shared memory
                context = f"{task}\n\nShared Memory:\n{json.dumps(self.shared_memory, indent=2)}"
                response = agent(context)
                results.append(str(response)[:500])

                # Update shared memory
                self.shared_memory[f"agent_{len(results)}"] = str(response)[:200]

        # Coordinator synthesizes results
        if self.coordinator:
            synthesis_task = f"Synthesize these agent results:\n{json.dumps(results, indent=2)}"
            final = self.coordinator(synthesis_task)
        else:
            final = {"aggregated_results": results}

        return {
            "mode": "swarm",
            "agents_participated": len(results),
            "final_result": str(final),
            "shared_memory_keys": list(self.shared_memory.keys())
        }


# ============================================================================
# MAIN DEMO
# ============================================================================

def run_enhanced_demo(config_path: str, use_graph: bool = True):
    """Run the enhanced Strands demo."""

    # Load config
    with open(config_path) as f:
        config = json.load(f)

    job_name = config.get("job_name", "demo_job")
    tables = config.get("source_tables", [])[:5]  # Limit for demo

    logger.info(f"Starting enhanced demo for: {job_name}")
    logger.info(f"Tables to analyze: {len(tables)}")
    logger.info(f"Strands available: {STRANDS_AVAILABLE}")
    logger.info(f"Strands tools available: {STRANDS_TOOLS_AVAILABLE}")
    logger.info(f"Multiagent available: {MULTIAGENT_AVAILABLE}")

    # Create enhanced agents
    sizing_agent = create_enhanced_agent("sizing_agent")
    platform_agent = create_enhanced_agent("platform_agent")
    architect_agent = create_enhanced_agent("architect_agent")

    if use_graph:
        # Graph-based orchestration
        logger.info("Using Graph-based orchestration")

        orchestrator = GraphOrchestrator(config)
        orchestrator.add_node("sizing", sizing_agent)
        orchestrator.add_node("platform", platform_agent, depends_on=["sizing"])
        orchestrator.add_node("architect", architect_agent, depends_on=["platform"])

        task = f"""
        Analyze this ETL job:
        Job: {job_name}
        Tables: {json.dumps(tables, indent=2)}

        1. First, analyze table sizes using batch processing
        2. Then recommend the optimal platform with reasoning
        3. Finally, generate the architecture diagram
        """

        result = orchestrator.execute(task)

    else:
        # Swarm-based orchestration
        logger.info("Using Swarm-based orchestration")

        swarm = SwarmOrchestrator([sizing_agent, platform_agent, architect_agent])

        task = f"Analyze ETL job {job_name} with {len(tables)} tables"
        result = swarm.run(task)

    logger.info("=" * 60)
    logger.info("DEMO RESULTS")
    logger.info("=" * 60)
    print(json.dumps(result, indent=2, default=str))

    return result


def main():
    parser = argparse.ArgumentParser(description="Enhanced Strands ETL Demo")
    parser.add_argument("--config", required=True, help="Path to config JSON")
    parser.add_argument("--mode", choices=["graph", "swarm"], default="graph",
                       help="Orchestration mode")
    parser.add_argument("--model", default="us.anthropic.claude-sonnet-4-20250514-v1:0",
                       help="Bedrock model ID")

    args = parser.parse_args()

    run_enhanced_demo(
        config_path=args.config,
        use_graph=(args.mode == "graph")
    )


if __name__ == "__main__":
    main()
