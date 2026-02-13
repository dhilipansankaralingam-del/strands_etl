#!/usr/bin/env python3
"""
Strands SDK Multi-Agent Orchestrator
=====================================

Orchestrates multiple agents with:
- Parallel execution for independent agents
- Dependency resolution
- Shared state management
- Result aggregation
- Error handling and recovery
"""

import uuid
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Any, Optional, Type, Set, Tuple
from collections import defaultdict

from .base_agent import StrandsAgent, AgentResult, AgentStatus, AgentContext, AgentRegistry


class TaskStatus(Enum):
    """Status of an orchestrated task."""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class AgentTask:
    """A task to be executed by an agent."""
    agent_name: str
    priority: int = 0  # Lower = higher priority
    dependencies: List[str] = field(default_factory=list)
    config_override: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    timeout_seconds: int = 300


@dataclass
class OrchestratorResult:
    """Result from orchestrator execution."""
    execution_id: str
    status: str
    total_agents: int
    completed_agents: int
    failed_agents: int
    skipped_agents: int
    agent_results: Dict[str, AgentResult] = field(default_factory=dict)
    recommendations: List[Dict[str, Any]] = field(default_factory=list)
    execution_timeline: List[Dict[str, Any]] = field(default_factory=list)
    total_time_ms: float = 0.0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'execution_id': self.execution_id,
            'status': self.status,
            'total_agents': self.total_agents,
            'completed_agents': self.completed_agents,
            'failed_agents': self.failed_agents,
            'skipped_agents': self.skipped_agents,
            'agent_results': {k: v.to_dict() for k, v in self.agent_results.items()},
            'recommendations': self.recommendations,
            'execution_timeline': self.execution_timeline,
            'total_time_ms': self.total_time_ms,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None
        }


class StrandsOrchestrator:
    """
    Multi-agent orchestrator with parallel execution support.

    Features:
    - Dependency-aware scheduling
    - Parallel execution of independent agents
    - Shared state management between agents
    - Error handling and recovery
    - Execution timeline tracking

    Example workflow:

    Phase 1 (Parallel):
        - SizingAgent (detect source sizes)
        - ComplianceAgent (check compliance)
        - CodeAnalysisAgent (analyze code)

    Phase 2 (Depends on Phase 1):
        - ResourceAllocatorAgent (needs sizing)
        - DataQualityAgent (needs compliance)

    Phase 3 (Depends on Phase 2):
        - PlatformConversionAgent (needs resource recommendation)
        - CodeConversionAgent (needs platform decision)

    Phase 4 (Final):
        - ExecutionAgent (runs the job)
        - LearningAgent (stores results)
        - RecommendationAgent (aggregates recommendations)
    """

    def __init__(
        self,
        config: Dict[str, Any],
        max_workers: int = 10,
        fail_fast: bool = False
    ):
        """
        Initialize the orchestrator.

        Args:
            config: Job configuration
            max_workers: Maximum parallel agents
            fail_fast: Stop on first failure
        """
        self.config = config
        self.max_workers = max_workers
        self.fail_fast = fail_fast
        self.execution_id = str(uuid.uuid4())[:12]
        self.logger = logging.getLogger("strands.orchestrator")

        # Task management
        self.tasks: Dict[str, AgentTask] = {}
        self.agents: Dict[str, StrandsAgent] = {}

        # Execution state
        self.context: Optional[AgentContext] = None
        self.completed: Set[str] = set()
        self.failed: Set[str] = set()
        self.skipped: Set[str] = set()
        self.timeline: List[Dict[str, Any]] = []

        # Thread safety
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

    def add_agent(
        self,
        agent: StrandsAgent,
        priority: int = 0,
        dependencies: List[str] = None,
        enabled: bool = True
    ) -> 'StrandsOrchestrator':
        """
        Add an agent to the orchestrator.

        Args:
            agent: The agent instance
            priority: Execution priority (lower = first)
            dependencies: List of agent names this agent depends on
            enabled: Whether the agent is enabled

        Returns:
            self for chaining
        """
        deps = dependencies or list(agent.DEPENDENCIES)

        self.tasks[agent.AGENT_NAME] = AgentTask(
            agent_name=agent.AGENT_NAME,
            priority=priority,
            dependencies=deps,
            enabled=enabled
        )
        self.agents[agent.AGENT_NAME] = agent

        return self

    def add_agents(self, agents: List[Tuple[StrandsAgent, int, List[str]]]) -> 'StrandsOrchestrator':
        """
        Add multiple agents at once.

        Args:
            agents: List of (agent, priority, dependencies) tuples
        """
        for agent, priority, deps in agents:
            self.add_agent(agent, priority, deps)
        return self

    def _build_execution_phases(self) -> List[List[str]]:
        """
        Build execution phases based on dependencies.

        Returns topologically sorted phases where agents in the same
        phase can run in parallel.
        """
        # Build dependency graph
        in_degree: Dict[str, int] = defaultdict(int)
        dependents: Dict[str, List[str]] = defaultdict(list)

        enabled_tasks = {
            name: task for name, task in self.tasks.items()
            if task.enabled
        }

        for name, task in enabled_tasks.items():
            for dep in task.dependencies:
                if dep in enabled_tasks:
                    dependents[dep].append(name)
                    in_degree[name] += 1

        # Kahn's algorithm for topological sort with phases
        phases: List[List[str]] = []
        ready = [name for name in enabled_tasks if in_degree[name] == 0]

        while ready:
            # Sort by priority within phase
            ready.sort(key=lambda x: enabled_tasks[x].priority)
            phases.append(ready[:])

            next_ready = []
            for name in ready:
                for dependent in dependents[name]:
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        next_ready.append(dependent)

            ready = next_ready

        # Check for cycles
        executed = sum(len(phase) for phase in phases)
        if executed < len(enabled_tasks):
            remaining = set(enabled_tasks.keys()) - set(
                name for phase in phases for name in phase
            )
            self.logger.warning(f"Circular dependencies detected: {remaining}")

        return phases

    def _execute_agent(self, agent_name: str) -> AgentResult:
        """Execute a single agent."""
        if self._stop_event.is_set():
            return AgentResult(
                agent_name=agent_name,
                agent_id="cancelled",
                status=AgentStatus.CANCELLED
            )

        agent = self.agents[agent_name]
        start_time = datetime.utcnow()

        # Log timeline event
        self._add_timeline_event(agent_name, "started")

        try:
            result = agent.run(self.context)

            # Store result in context for other agents
            with self._lock:
                self.context.agent_results[agent_name] = result

                if result.status == AgentStatus.COMPLETED:
                    self.completed.add(agent_name)
                elif result.status == AgentStatus.FAILED:
                    self.failed.add(agent_name)

            self._add_timeline_event(
                agent_name,
                "completed" if result.status == AgentStatus.COMPLETED else "failed",
                result.execution_time_ms
            )

            return result

        except Exception as e:
            self.logger.error(f"Agent {agent_name} crashed: {e}")
            result = AgentResult(
                agent_name=agent_name,
                agent_id=agent.agent_id,
                status=AgentStatus.FAILED,
                errors=[str(e)],
                started_at=start_time,
                completed_at=datetime.utcnow()
            )

            with self._lock:
                self.context.agent_results[agent_name] = result
                self.failed.add(agent_name)

            self._add_timeline_event(agent_name, "crashed")
            return result

    def _add_timeline_event(
        self,
        agent_name: str,
        event: str,
        duration_ms: float = None
    ) -> None:
        """Add event to execution timeline."""
        with self._lock:
            self.timeline.append({
                'timestamp': datetime.utcnow().isoformat(),
                'agent': agent_name,
                'event': event,
                'duration_ms': duration_ms
            })

    def _execute_phase(self, phase: List[str]) -> Dict[str, AgentResult]:
        """Execute a phase of agents in parallel."""
        results: Dict[str, AgentResult] = {}

        if not phase:
            return results

        self.logger.info(f"Executing phase with {len(phase)} agents: {phase}")

        with ThreadPoolExecutor(max_workers=min(len(phase), self.max_workers)) as executor:
            futures: Dict[Future, str] = {
                executor.submit(self._execute_agent, name): name
                for name in phase
            }

            for future in as_completed(futures):
                agent_name = futures[future]
                try:
                    result = future.result()
                    results[agent_name] = result

                    if result.status == AgentStatus.FAILED and self.fail_fast:
                        self.logger.warning(f"Fail-fast triggered by {agent_name}")
                        self._stop_event.set()
                        executor.shutdown(wait=False)
                        break

                except Exception as e:
                    self.logger.error(f"Future failed for {agent_name}: {e}")
                    results[agent_name] = AgentResult(
                        agent_name=agent_name,
                        agent_id="error",
                        status=AgentStatus.FAILED,
                        errors=[str(e)]
                    )

        return results

    def execute(
        self,
        job_name: str,
        run_date: datetime = None,
        platform: str = "glue"
    ) -> OrchestratorResult:
        """
        Execute all agents according to dependency order.

        Args:
            job_name: Name of the job
            run_date: Run date for the job
            platform: Target platform

        Returns:
            OrchestratorResult with all agent results
        """
        start_time = datetime.utcnow()
        run_date = run_date or start_time

        self.logger.info(f"Starting orchestration: {self.execution_id}")
        self.logger.info(f"Job: {job_name}, Platform: {platform}")

        # Initialize context
        self.context = AgentContext(
            job_name=job_name,
            config=self.config,
            execution_id=self.execution_id,
            run_date=run_date,
            platform=platform
        )

        # Reset state
        self.completed.clear()
        self.failed.clear()
        self.skipped.clear()
        self.timeline.clear()
        self._stop_event.clear()

        # Build execution phases
        phases = self._build_execution_phases()
        self.logger.info(f"Execution phases: {len(phases)}")

        for i, phase in enumerate(phases):
            self.logger.info(f"Phase {i+1}/{len(phases)}: {phase}")

        # Execute phases
        all_results: Dict[str, AgentResult] = {}

        for phase_num, phase in enumerate(phases, 1):
            if self._stop_event.is_set():
                # Mark remaining as skipped
                for name in phase:
                    if name not in all_results:
                        self.skipped.add(name)
                continue

            self.logger.info(f"=== Phase {phase_num} ===")
            phase_results = self._execute_phase(phase)
            all_results.update(phase_results)

        # Aggregate recommendations
        all_recommendations: List[Dict[str, Any]] = []
        for agent_name, result in all_results.items():
            for rec in result.recommendations:
                all_recommendations.append({
                    'source_agent': agent_name,
                    'recommendation': rec,
                    'priority': 'high' if result.status == AgentStatus.FAILED else 'normal'
                })

        # Build final result
        end_time = datetime.utcnow()

        orchestrator_result = OrchestratorResult(
            execution_id=self.execution_id,
            status='completed' if not self.failed else 'partial_failure' if self.completed else 'failed',
            total_agents=len(self.tasks),
            completed_agents=len(self.completed),
            failed_agents=len(self.failed),
            skipped_agents=len(self.skipped),
            agent_results=all_results,
            recommendations=all_recommendations,
            execution_timeline=self.timeline,
            total_time_ms=(end_time - start_time).total_seconds() * 1000,
            started_at=start_time,
            completed_at=end_time
        )

        self.logger.info(f"Orchestration complete: {orchestrator_result.status}")
        self.logger.info(f"Completed: {len(self.completed)}, Failed: {len(self.failed)}, Skipped: {len(self.skipped)}")

        return orchestrator_result

    def get_execution_summary(self) -> str:
        """Get human-readable execution summary."""
        lines = [
            f"Execution ID: {self.execution_id}",
            f"Completed: {len(self.completed)}",
            f"Failed: {len(self.failed)}",
            f"Skipped: {len(self.skipped)}",
            "",
            "Timeline:"
        ]

        for event in self.timeline:
            duration = f" ({event['duration_ms']:.0f}ms)" if event.get('duration_ms') else ""
            lines.append(f"  [{event['timestamp']}] {event['agent']}: {event['event']}{duration}")

        return "\n".join(lines)
