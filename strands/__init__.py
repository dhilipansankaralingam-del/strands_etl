"""
Strands ETL Framework - Intelligent Multi-Agent ETL Orchestration

A production-grade, AI-powered ETL framework that uses independent agents
for intelligent platform selection, quality assessment, optimization, and learning.

Key Components:
- Message Bus: Asynchronous event-driven communication
- Decision Agent: ML-based platform selection
- Quality Agent: SQL/NL parsing and code analysis
- Optimization Agent: Pattern-based recommendations
- Learning Agent: Continuous improvement through execution patterns
- Coordinator: Manages agent lifecycle and orchestration

Example Usage:
    ```python
    from strands import StrandsCoordinator

    coordinator = StrandsCoordinator()
    await coordinator.initialize()

    result = await coordinator.orchestrate_pipeline(
        user_request="Process customer orders with quality checks",
        config_path="s3://bucket/config.json"
    )
    ```
"""

from strands_message_bus import (
    StrandsMessageBus,
    StrandsMessage,
    MessageType,
    get_message_bus
)

from strands_agent_base import StrandsAgent, AgentState

from strands_decision_agent import StrandsDecisionAgent
from strands_quality_agent import StrandsQualityAgent
from strands_optimization_agent import StrandsOptimizationAgent
from strands_learning_agent import StrandsLearningAgent

from strands_coordinator import StrandsCoordinator

__version__ = "1.0.0"
__author__ = "Strands ETL Team"

__all__ = [
    # Core Components
    'StrandsCoordinator',
    'StrandsMessageBus',
    'StrandsMessage',
    'MessageType',
    'StrandsAgent',
    'AgentState',

    # Agents
    'StrandsDecisionAgent',
    'StrandsQualityAgent',
    'StrandsOptimizationAgent',
    'StrandsLearningAgent',

    # Utilities
    'get_message_bus',
]
