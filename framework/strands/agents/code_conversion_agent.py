#!/usr/bin/env python3
"""Strands Code Conversion Agent - Converts PySpark code between platforms."""

import re
from typing import Dict, List, Any
from pathlib import Path
from ..base_agent import StrandsAgent, AgentResult, AgentStatus, AgentContext, register_agent
from ..storage import StrandsStorage


@register_agent
class StrandsCodeConversionAgent(StrandsAgent):
    """Converts PySpark code from Glue to EMR/EKS format."""

    AGENT_NAME = "code_conversion_agent"
    AGENT_VERSION = "2.0.0"
    AGENT_DESCRIPTION = "Converts GlueContext/DynamicFrame code to SparkSession/DataFrame"

    DEPENDENCIES = ['platform_conversion_agent']  # Needs platform decision
    PARALLEL_SAFE = True

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.storage = StrandsStorage(config)

    def execute(self, context: AgentContext) -> AgentResult:
        code_config = context.config.get('platform', {}).get('code_conversion', {})

        if not code_config.get('enabled') in ('Y', 'y', True):
            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.COMPLETED,
                output={'skipped': True, 'reason': 'Code conversion disabled'}
            )

        # Check if platform conversion is needed
        platform_needed = context.get_shared('platform_conversion_needed', False)
        target_platform = context.get_shared('target_platform', 'glue')

        if not platform_needed or target_platform == 'glue':
            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.COMPLETED,
                output={'skipped': True, 'reason': 'No platform conversion needed'}
            )

        # Read original script
        script_path = context.config.get('script', {}).get('local_path', '')
        original_code = None

        if script_path:
            try:
                original_code = Path(script_path).read_text()
            except Exception as e:
                return AgentResult(
                    agent_name=self.AGENT_NAME,
                    agent_id=self.agent_id,
                    status=AgentStatus.FAILED,
                    errors=[f"Failed to read script: {e}"]
                )

        if not original_code:
            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.FAILED,
                errors=['No script content to convert']
            )

        # Convert code
        if target_platform == 'emr':
            converted_code, changes = self._convert_glue_to_emr(original_code)
        elif target_platform == 'eks':
            converted_code, changes = self._convert_glue_to_eks(original_code)
        else:
            converted_code = original_code
            changes = []

        # Save converted script
        output_path = code_config.get('output_converted_script_path', 'converted/')
        saved_path = None

        if code_config.get('preserve_original') in ('Y', 'y', True):
            try:
                output_dir = Path(output_path.replace('s3://', '').split('/')[0] if 's3://' in output_path else output_path)
                output_dir.mkdir(parents=True, exist_ok=True)
                output_file = output_dir / f"{context.job_name}_{target_platform}.py"
                output_file.write_text(converted_code)
                saved_path = str(output_file)
            except Exception as e:
                self.logger.warning(f"Failed to save converted script: {e}")

        # Store conversion data
        self.storage.store_agent_data(
            self.AGENT_NAME,
            'code_conversions',
            [{
                'job_name': context.job_name,
                'source_platform': 'glue',
                'target_platform': target_platform,
                'changes_count': len(changes),
                'changes': changes[:10],
                'output_path': saved_path
            }],
            use_pipe_delimited=True
        )

        recommendations = [f"Code converted: {len(changes)} changes made"]
        if any('NEEDS_REVIEW' in str(c) for c in changes):
            recommendations.append("Some changes need manual review")

        context.set_shared('converted_code', converted_code)
        context.set_shared('converted_script_path', saved_path)

        return AgentResult(
            agent_name=self.AGENT_NAME,
            agent_id=self.agent_id,
            status=AgentStatus.COMPLETED,
            output={
                'target_platform': target_platform,
                'changes_count': len(changes),
                'changes': changes,
                'output_path': saved_path
            },
            metrics={
                'changes_count': len(changes)
            },
            recommendations=recommendations
        )

    def _convert_glue_to_emr(self, code: str) -> tuple:
        """Convert Glue code to EMR-compatible code."""
        converted = code
        changes = []

        # Remove Glue imports
        glue_imports = [
            (r"from awsglue\.context import GlueContext\n?", ""),
            (r"from awsglue\.job import Job\n?", ""),
            (r"from awsglue\.transforms import \*\n?", ""),
            (r"from awsglue\.utils import getResolvedOptions\n?", ""),
            (r"from awsglue\.dynamicframe import DynamicFrame\n?", ""),
        ]

        for pattern, replacement in glue_imports:
            if re.search(pattern, converted):
                converted = re.sub(pattern, replacement, converted)
                changes.append({'type': 'import_removal', 'pattern': pattern[:30]})

        # Add EMR imports
        emr_imports = """from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys
import argparse

"""
        if "from pyspark.sql import SparkSession" not in converted:
            converted = emr_imports + converted
            changes.append({'type': 'import_addition', 'added': 'EMR standard imports'})

        # Replace GlueContext with SparkSession
        if "GlueContext" in converted:
            glue_context_pattern = r"glueContext\s*=\s*GlueContext\([^)]*\)"
            spark_session = '''spark = SparkSession.builder \\
    .appName("EMR-Job") \\
    .enableHiveSupport() \\
    .getOrCreate()
sc = spark.sparkContext'''
            converted = re.sub(glue_context_pattern, spark_session, converted)
            changes.append({'type': 'context_replacement', 'from': 'GlueContext', 'to': 'SparkSession'})

        # Remove Job init/commit
        converted = re.sub(r"job\s*=\s*Job\(glueContext\)\n?", "", converted)
        converted = re.sub(r"job\.init\([^)]*\)\n?", "", converted)
        converted = re.sub(r"job\.commit\(\)\n?", "print('Job completed')\n", converted)
        changes.append({'type': 'job_handling', 'removed': 'Job bookmarks'})

        # Convert catalog reads
        catalog_read = r"glueContext\.create_dynamic_frame\.from_catalog\(\s*database\s*=\s*['\"]([^'\"]+)['\"]\s*,\s*table_name\s*=\s*['\"]([^'\"]+)['\"]"
        for match in re.finditer(catalog_read, converted):
            db, tbl = match.group(1), match.group(2)
            converted = converted.replace(match.group(0), f'spark.table("{db}.{tbl}"')
            changes.append({'type': 'catalog_read', 'table': f'{db}.{tbl}'})

        # Convert DynamicFrame to DataFrame
        converted = re.sub(r"(\w+)\.toDF\(\)", r"\1", converted)
        converted = re.sub(r"DynamicFrame\.fromDF\([^,]+,\s*glueContext,\s*['\"][^'\"]+['\"]\)", "", converted)

        # Replace glueContext references
        converted = re.sub(r"glueContext\.", "spark.", converted)

        return converted, changes

    def _convert_glue_to_eks(self, code: str) -> tuple:
        """Convert Glue code to EKS-compatible code."""
        # First convert to EMR format
        converted, changes = self._convert_glue_to_emr(code)

        # Add K8s-specific SparkSession config
        spark_builder = r"SparkSession\.builder\s*\\\s*\n\s*\.appName\([^)]+\)"
        eks_builder = '''SparkSession.builder \\
    .appName("EKS-Spark-Job") \\
    .config("spark.kubernetes.container.image", "spark:3.5.0") \\
    .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")'''

        if re.search(spark_builder, converted):
            converted = re.sub(spark_builder, eks_builder, converted)
            changes.append({'type': 'eks_config', 'added': 'Kubernetes Spark configs'})

        return converted, changes
