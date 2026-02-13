#!/usr/bin/env python3
"""
Strands SDK Tools
=================

Tool decorator and management for Strands agents.
Tools are callable functions that agents can use to perform specific operations.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Callable
from functools import wraps
import inspect


@dataclass
class ToolDefinition:
    """Definition of a tool."""
    name: str
    description: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    returns: str = ""
    examples: List[str] = field(default_factory=list)


class StrandsTool:
    """
    Tool wrapper that provides metadata and validation.
    """

    def __init__(
        self,
        name: str = None,
        description: str = None,
        parameters: Dict[str, Any] = None
    ):
        self.name = name
        self.description = description
        self.parameters = parameters or {}

    def __call__(self, func: Callable) -> Callable:
        """Decorator to mark a method as a tool."""
        tool_name = self.name or func.__name__
        tool_description = self.description or func.__doc__ or ""

        # Extract parameters from function signature
        sig = inspect.signature(func)
        params = {}
        for param_name, param in sig.parameters.items():
            if param_name == 'self':
                continue
            param_info = {
                'type': 'any',
                'required': param.default == inspect.Parameter.empty
            }
            if param.annotation != inspect.Parameter.empty:
                param_info['type'] = str(param.annotation.__name__) if hasattr(param.annotation, '__name__') else str(param.annotation)
            if param.default != inspect.Parameter.empty:
                param_info['default'] = param.default
            params[param_name] = param_info

        # Merge with provided parameters
        params.update(self.parameters)

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        # Mark as a strands tool
        wrapper._is_strands_tool = True
        wrapper._tool_name = tool_name
        wrapper._tool_description = tool_description
        wrapper._tool_parameters = params

        return wrapper


def tool(
    name: str = None,
    description: str = None,
    parameters: Dict[str, Any] = None
) -> Callable:
    """
    Decorator to mark a method as a tool.

    Usage:
        @tool(name="analyze_data", description="Analyze input data")
        def analyze(self, data: Dict) -> Dict:
            ...
    """
    return StrandsTool(name, description, parameters)
