#!/usr/bin/env python3
"""
Strands SDK Base Agent
======================

Base class for all ETL agents following Strands SDK patterns.
Provides standardized interfaces for agent execution, tool management,
and inter-agent communication.
"""

import uuid
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Any, Optional, Callable, Type
from concurrent.futures import ThreadPoolExecutor, Future
import threading
import json

# LLM support (optional import)
try:
    from .llm import BedrockClient, AGENT_PROMPTS, ResponseParser
    LLM_AVAILABLE = True
except ImportError:
    LLM_AVAILABLE = False
    BedrockClient = None
    AGENT_PROMPTS = {}
    ResponseParser = None

# Unified audit logger (optional import)
try:
    from .unified_audit import get_audit_logger, EventType
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False
    get_audit_logger = None
    EventType = None


class AgentStatus(Enum):
    """Status of an agent execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    WAITING = "waiting"  # Waiting for other agents


@dataclass
class AgentResult:
    """Standardized result from agent execution."""
    agent_name: str
    agent_id: str
    status: AgentStatus
    output: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    execution_time_ms: float = 0.0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'agent_name': self.agent_name,
            'agent_id': self.agent_id,
            'status': self.status.value,
            'output': self.output,
            'metrics': self.metrics,
            'recommendations': self.recommendations,
            'errors': self.errors,
            'warnings': self.warnings,
            'execution_time_ms': self.execution_time_ms,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None
        }


@dataclass
class AgentContext:
    """Context passed to agents during execution."""
    job_name: str
    config: Dict[str, Any]
    execution_id: str
    run_date: datetime
    platform: str = "glue"
    shared_state: Dict[str, Any] = field(default_factory=dict)
    agent_results: Dict[str, AgentResult] = field(default_factory=dict)
    use_llm: bool = False  # Enable LLM-enhanced mode
    llm_stats: Dict[str, Any] = field(default_factory=dict)  # Track LLM usage

    def get_agent_result(self, agent_name: str) -> Optional[AgentResult]:
        """Get result from another agent."""
        return self.agent_results.get(agent_name)

    def set_shared(self, key: str, value: Any) -> None:
        """Set shared state for other agents."""
        self.shared_state[key] = value

    def get_shared(self, key: str, default: Any = None) -> Any:
        """Get shared state from other agents."""
        return self.shared_state.get(key, default)


class StrandsAgent(ABC):
    """
    Base class for all Strands SDK agents.

    Agents follow a standardized lifecycle:
    1. Initialize with config
    2. Execute with context
    3. Return AgentResult

    Agents can:
    - Define tools using @tool decorator
    - Access shared state from other agents
    - Emit recommendations and warnings
    - Collaborate with other agents
    """

    # Agent metadata - override in subclasses
    AGENT_NAME: str = "base_agent"
    AGENT_VERSION: str = "1.0.0"
    AGENT_DESCRIPTION: str = "Base Strands Agent"

    # Dependencies - list of agent names this agent depends on
    DEPENDENCIES: List[str] = []

    # Can run in parallel with other agents (if no dependencies)
    PARALLEL_SAFE: bool = True

    def __init__(self, config: Dict[str, Any] = None):
        """Initialize the agent."""
        self.config = config or {}
        self.agent_id = str(uuid.uuid4())[:8]
        self.logger = logging.getLogger(f"strands.{self.AGENT_NAME}")
        self.tools: Dict[str, Callable] = {}
        self._register_tools()

        # LLM client (lazy-loaded)
        self._llm_client: Optional['BedrockClient'] = None

    @property
    def llm(self) -> Optional['BedrockClient']:
        """Get or create LLM client for this agent."""
        if not LLM_AVAILABLE:
            return None

        if self._llm_client is None:
            llm_config = self.config.get('llm', {})
            self._llm_client = BedrockClient(
                model_id=llm_config.get('model_id'),
                region=llm_config.get('region', 'us-east-1'),
                max_retries=llm_config.get('max_retries', 3),
                enable_cache=llm_config.get('enable_cache', True)
            )
        return self._llm_client

    def get_system_prompt(self) -> str:
        """Get the expert system prompt for this agent."""
        if not LLM_AVAILABLE:
            return ""
        return AGENT_PROMPTS.get(self.AGENT_NAME, "")

    def is_llm_enabled(self, context: 'AgentContext') -> bool:
        """Check if LLM mode is enabled for this execution."""
        return (
            LLM_AVAILABLE and
            context.use_llm and
            self.llm is not None
        )

    def _register_tools(self) -> None:
        """Register all tools defined with @tool decorator."""
        for attr_name in dir(self):
            try:
                # Skip private attributes and properties that may not be initialized
                if attr_name.startswith('_'):
                    continue
                attr = getattr(self, attr_name, None)
                if attr and hasattr(attr, '_is_strands_tool'):
                    self.tools[attr._tool_name] = attr
            except (AttributeError, TypeError):
                # Skip attributes that can't be accessed during initialization
                continue

    @abstractmethod
    def execute(self, context: AgentContext) -> AgentResult:
        """
        Execute the agent's main logic.

        Args:
            context: Execution context with config and shared state

        Returns:
            AgentResult with output, metrics, and recommendations
        """
        pass

    def run(self, context: AgentContext) -> AgentResult:
        """
        Run the agent with timing and error handling.

        This is the main entry point called by the orchestrator.
        Automatically logs events to unified audit logger.
        """
        start_time = datetime.utcnow()

        # Get audit logger if available
        audit = None
        if AUDIT_AVAILABLE and get_audit_logger:
            try:
                audit = get_audit_logger(self.config)
            except Exception:
                pass

        # FIRST: Check if agent is enabled via agents.<agent_name>.enabled
        agent_enabled_key = f"agents.{self.AGENT_NAME}.enabled"
        if not self.is_enabled(agent_enabled_key):
            self.logger.info(f"Agent {self.AGENT_NAME} is disabled via config '{agent_enabled_key}'")
            result = AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.COMPLETED,
                output={'skipped': True, 'reason': f'Disabled via {agent_enabled_key}'},
                started_at=start_time,
                completed_at=datetime.utcnow()
            )
            if audit:
                audit.log_agent_skip(
                    job_name=context.job_name,
                    execution_id=context.execution_id,
                    agent_name=self.AGENT_NAME,
                    agent_id=self.agent_id,
                    reason=f'Disabled via config: {agent_enabled_key}'
                )
            return result

        try:
            self.logger.info(f"Starting agent: {self.AGENT_NAME} (id={self.agent_id})")

            # Log agent start
            if audit:
                audit.log_agent_start(
                    job_name=context.job_name,
                    execution_id=context.execution_id,
                    agent_name=self.AGENT_NAME,
                    agent_id=self.agent_id,
                    platform=context.platform
                )

            # Check dependencies
            for dep in self.DEPENDENCIES:
                dep_result = context.get_agent_result(dep)
                if not dep_result or dep_result.status != AgentStatus.COMPLETED:
                    result = AgentResult(
                        agent_name=self.AGENT_NAME,
                        agent_id=self.agent_id,
                        status=AgentStatus.WAITING,
                        errors=[f"Dependency not met: {dep}"],
                        started_at=start_time
                    )
                    # Log waiting status
                    if audit:
                        audit.log_agent_skip(
                            job_name=context.job_name,
                            execution_id=context.execution_id,
                            agent_name=self.AGENT_NAME,
                            agent_id=self.agent_id,
                            reason=f"Waiting for dependency: {dep}"
                        )
                    return result

            # Execute the agent
            result = self.execute(context)
            result.started_at = start_time
            result.completed_at = datetime.utcnow()
            result.execution_time_ms = (result.completed_at - start_time).total_seconds() * 1000

            self.logger.info(f"Agent completed: {self.AGENT_NAME} in {result.execution_time_ms:.0f}ms")

            # Log agent completion or skip
            if audit:
                if result.output.get('skipped'):
                    audit.log_agent_skip(
                        job_name=context.job_name,
                        execution_id=context.execution_id,
                        agent_name=self.AGENT_NAME,
                        agent_id=self.agent_id,
                        reason=result.output.get('reason', 'Agent disabled')
                    )
                else:
                    audit.log_agent_complete(
                        job_name=context.job_name,
                        execution_id=context.execution_id,
                        agent_name=self.AGENT_NAME,
                        agent_id=self.agent_id,
                        duration_ms=result.execution_time_ms,
                        output=result.output,
                        recommendations=result.recommendations,
                        platform=context.platform
                    )

                    # Log recommendations as separate events
                    for rec in result.recommendations:
                        audit.log_recommendation(
                            job_name=context.job_name,
                            execution_id=context.execution_id,
                            agent_name=self.AGENT_NAME,
                            recommendation=rec
                        )

            return result

        except Exception as e:
            self.logger.error(f"Agent failed: {self.AGENT_NAME} - {str(e)}")

            # Log agent error
            if audit:
                audit.log_agent_error(
                    job_name=context.job_name,
                    execution_id=context.execution_id,
                    agent_name=self.AGENT_NAME,
                    agent_id=self.agent_id,
                    error=str(e),
                    error_type=type(e).__name__
                )

            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.FAILED,
                errors=[str(e)],
                started_at=start_time,
                completed_at=datetime.utcnow()
            )

    def invoke_tool(self, tool_name: str, **kwargs) -> Any:
        """Invoke a registered tool by name."""
        if tool_name not in self.tools:
            raise ValueError(f"Tool not found: {tool_name}")
        return self.tools[tool_name](**kwargs)

    def get_config_value(self, key: str, default: Any = None) -> Any:
        """Get a configuration value with nested key support."""
        keys = key.split('.')
        value = self.config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
        return value if value is not None else default

    def is_enabled(self, feature_key: str) -> bool:
        """Check if a feature is enabled in config."""
        value = self.get_config_value(feature_key)
        return value in ('Y', 'y', True, 'true', 'True', 'yes', 'YES')


class AgentRegistry:
    """Registry of all available agents."""

    _agents: Dict[str, Type[StrandsAgent]] = {}
    _lock = threading.Lock()

    @classmethod
    def register(cls, agent_class: Type[StrandsAgent]) -> Type[StrandsAgent]:
        """Register an agent class."""
        with cls._lock:
            cls._agents[agent_class.AGENT_NAME] = agent_class
        return agent_class

    @classmethod
    def get(cls, name: str) -> Optional[Type[StrandsAgent]]:
        """Get an agent class by name."""
        return cls._agents.get(name)

    @classmethod
    def list_agents(cls) -> List[str]:
        """List all registered agent names."""
        return list(cls._agents.keys())

    @classmethod
    def create(cls, name: str, config: Dict[str, Any] = None) -> Optional[StrandsAgent]:
        """Create an agent instance by name."""
        agent_class = cls.get(name)
        if agent_class:
            return agent_class(config)
        return None


def register_agent(cls: Type[StrandsAgent]) -> Type[StrandsAgent]:
    """Decorator to register an agent class."""
    return AgentRegistry.register(cls)
