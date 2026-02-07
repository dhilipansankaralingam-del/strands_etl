"""
AWS Job Execution Module
========================

Provides actual job execution for Glue and EMR with:
- Pre-flight validation
- Platform fallback
- Metrics collection
"""

from .aws_job_executor import (
    AWSJobExecutor,
    ExecutionMetrics,
    ValidationResult,
    Platform,
    JobStatus,
    validate_and_execute
)

__all__ = [
    'AWSJobExecutor',
    'ExecutionMetrics',
    'ValidationResult',
    'Platform',
    'JobStatus',
    'validate_and_execute'
]
