"""
Strands SDK Integration for Enterprise ETL Framework
=====================================================

This module provides Strands SDK integration for building and orchestrating
AI-powered ETL agents with multi-agent collaboration capabilities.

Key Components:
- StrandsAgent: Base class for all ETL agents
- StrandsOrchestrator: Multi-agent orchestrator with parallel execution
- StrandsTool: Tool decorator for agent capabilities
- AgentResult: Standardized result format
"""

from .base_agent import StrandsAgent, AgentResult, AgentStatus
from .orchestrator import StrandsOrchestrator, AgentTask, TaskStatus, OrchestratorResult
from .tools import StrandsTool, tool
from .storage import StrandsStorage, StorageBackend

__all__ = [
    'StrandsAgent',
    'AgentResult',
    'AgentStatus',
    'StrandsOrchestrator',
    'AgentTask',
    'TaskStatus',
    'OrchestratorResult',
    'StrandsTool',
    'tool',
    'StrandsStorage',
    'StorageBackend'
]
