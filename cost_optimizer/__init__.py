"""
Cost Optimizer Package
======================

Multi-agent cost optimization system for PySpark workloads.

Usage:
    # Single script analysis
    from cost_optimizer import CostOptimizationOrchestrator

    orchestrator = CostOptimizationOrchestrator()
    result = orchestrator.analyze_script(
        script_path='my_job.py',
        source_tables=[...],
        processing_mode='delta'
    )

    # Batch analysis
    from cost_optimizer import BatchAnalyzer

    analyzer = BatchAnalyzer()
    report = analyzer.analyze_batch(scripts=[...])
"""

from .orchestrator import CostOptimizationOrchestrator, BatchAnalyzer
from .agents.base import AnalysisInput, AnalysisResult

__all__ = [
    'CostOptimizationOrchestrator',
    'BatchAnalyzer',
    'AnalysisInput',
    'AnalysisResult'
]
