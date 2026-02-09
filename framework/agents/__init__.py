"""
Enterprise ETL Framework - Agent Registry
==========================================

All agents that can run independently or as part of the framework.
"""

from .auto_healing_agent import AutoHealingAgent
from .code_analysis_agent import CodeAnalysisAgent
from .compliance_agent import ComplianceAgent
from .data_quality_agent import DataQualityAgent
from .workload_assessment_agent import WorkloadAssessmentAgent
from .learning_agent import LearningAgent
from .recommendation_agent import RecommendationAgent
from .code_conversion_agent import CodeConversionAgent
from .platform_conversion_agent import PlatformConversionAgent

__all__ = [
    'AutoHealingAgent',
    'CodeAnalysisAgent',
    'ComplianceAgent',
    'DataQualityAgent',
    'WorkloadAssessmentAgent',
    'LearningAgent',
    'RecommendationAgent',
    'CodeConversionAgent',
    'PlatformConversionAgent'
]
