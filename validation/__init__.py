"""
Strands Validation Intelligence Framework
==========================================

Sub-packages / modules:
  models              – ValidationRecord, AnalysisResult, AthenaQueryResult, …
  prompts             – Prompt templates (system, decision, learning, SQL)
  athena_query_engine – NL → SQL → Athena execution
  validation_agent    – Multi-agent validation analysis (Bedrock-backed)
  validation_cli      – Interactive CLI entry point

Quick start::

    from validation.validation_agent import ValidationAnalysisAgent
    from validation.athena_query_engine import AthenaQueryEngine
    from validation.models import ValidationRecord

    agent = ValidationAnalysisAgent(learning_bucket="strands-etl-learning")
    engine = AthenaQueryEngine(
        database="validation_db",
        s3_output_location="s3://my-bucket/athena/",
    )

CLI::

    python -m validation.validation_cli \\
        --database validation_db \\
        --s3-output s3://my-bucket/athena/
"""

from validation.models import (
    ValidationRecord,
    AnalysisResult,
    HistoricalOutcome,
    AthenaQueryResult,
    ValidationClassification,
    RecommendedAction,
)
from validation.validation_agent import ValidationAnalysisAgent
from validation.athena_query_engine import AthenaQueryEngine

__all__ = [
    "ValidationRecord",
    "AnalysisResult",
    "HistoricalOutcome",
    "AthenaQueryResult",
    "ValidationClassification",
    "RecommendedAction",
    "ValidationAnalysisAgent",
    "AthenaQueryEngine",
]
