"""
Source Sizing Module
====================

Automatically detects and manages source table sizes for intelligent resource allocation.
"""

from .source_size_detector import SourceSizeDetector, TableSize, SizingResult

__all__ = [
    'SourceSizeDetector',
    'TableSize',
    'SizingResult'
]
