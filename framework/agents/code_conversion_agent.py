#!/usr/bin/env python3
"""
Code Conversion Agent
=====================

Intelligent agent that converts PySpark code between platforms:
1. Glue to EMR conversion
2. Glue to EKS/Spark conversion
3. EMR to Glue conversion
4. Handle platform-specific APIs
5. Maintain compatibility while optimizing for target platform

Conversion includes:
- GlueContext vs SparkSession handling
- DynamicFrame vs DataFrame conversion
- Job bookmarks handling
- Platform-specific configurations
- S3 path handling
- Catalog table references
"""

import re
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum


class SourcePlatform(Enum):
    """Source platforms for conversion."""
    GLUE = "glue"
    EMR = "emr"
    DATABRICKS = "databricks"
    LOCAL_SPARK = "local_spark"


class TargetPlatform(Enum):
    """Target platforms for conversion."""
    GLUE = "glue"
    EMR = "emr"
    EKS = "eks"
    DATABRICKS = "databricks"


@dataclass
class ConversionChange:
    """A single change made during conversion."""
    line_number: Optional[int]
    change_type: str
    original: str
    converted: str
    description: str
    requires_review: bool = False


@dataclass
class ConversionResult:
    """Result of code conversion."""
    converted_code: str
    changes: List[ConversionChange] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    required_dependencies: List[str] = field(default_factory=list)
    config_changes: Dict[str, str] = field(default_factory=dict)
    success: bool = True
    error_message: str = ""


class CodeConversionAgent:
    """
    Agent that converts PySpark code between platforms.
    """

    def __init__(self, config):
        self.config = config

        # Glue-specific patterns
        self.glue_patterns = {
            "glue_import": r"from awsglue\.(\w+) import",
            "glue_context": r"GlueContext\([^)]*\)",
            "dynamic_frame": r"DynamicFrame\.fromDF|\.fromDF\([^)]*glueContext",
            "to_dynamic_frame": r"\.toDF\(\)|DynamicFrame\.toDF",
            "glue_catalog_read": r"glueContext\.create_dynamic_frame\.from_catalog",
            "glue_catalog_write": r"glueContext\.write_dynamic_frame\.from_catalog",
            "glue_s3_read": r"glueContext\.create_dynamic_frame\.from_options",
            "glue_s3_write": r"glueContext\.write_dynamic_frame\.from_options",
            "job_init": r"Job\(glueContext\)",
            "job_commit": r"job\.commit\(\)",
            "resolve_choice": r"\.resolveChoice\([^)]*\)",
            "apply_mapping": r"\.apply_mapping\([^)]*\)",
            "relationalize": r"\.relationalize\([^)]*\)"
        }

        # EMR-specific patterns
        self.emr_patterns = {
            "spark_session": r"SparkSession\.builder",
            "hadoop_conf": r"spark\.sparkContext\.hadoopConfiguration",
            "dynamic_allocation": r"spark\.dynamicAllocation"
        }

    def convert(
        self,
        code: str,
        source_platform: SourcePlatform,
        target_platform: TargetPlatform,
        preserve_comments: bool = True
    ) -> ConversionResult:
        """
        Convert PySpark code from source to target platform.

        Args:
            code: Original PySpark code
            source_platform: Platform the code was written for
            target_platform: Platform to convert to
            preserve_comments: Whether to preserve existing comments

        Returns:
            ConversionResult with converted code and metadata
        """
        if source_platform == SourcePlatform.GLUE:
            if target_platform == TargetPlatform.EMR:
                return self._convert_glue_to_emr(code)
            elif target_platform == TargetPlatform.EKS:
                return self._convert_glue_to_eks(code)
        elif source_platform == SourcePlatform.EMR:
            if target_platform == TargetPlatform.GLUE:
                return self._convert_emr_to_glue(code)

        return ConversionResult(
            converted_code=code,
            success=False,
            error_message=f"Conversion from {source_platform.value} to {target_platform.value} not supported"
        )

    def _convert_glue_to_emr(self, code: str) -> ConversionResult:
        """Convert AWS Glue code to EMR-compatible code."""
        result = ConversionResult(converted_code=code)
        converted = code
        changes = []

        # Replace Glue imports with standard Spark imports
        glue_imports = re.findall(r"from awsglue[^\n]+\n", converted)
        for imp in glue_imports:
            changes.append(ConversionChange(
                line_number=None,
                change_type="import_removal",
                original=imp.strip(),
                converted="# Removed Glue import (EMR uses standard Spark)",
                description="Removed Glue-specific import"
            ))

        converted = re.sub(r"from awsglue\.context import GlueContext\n?", "", converted)
        converted = re.sub(r"from awsglue\.job import Job\n?", "", converted)
        converted = re.sub(r"from awsglue\.transforms import \*\n?", "", converted)
        converted = re.sub(r"from awsglue\.utils import getResolvedOptions\n?", "", converted)
        converted = re.sub(r"from awsglue\.dynamicframe import DynamicFrame\n?", "", converted)

        # Add EMR-compatible imports
        emr_imports = """from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
"""
        if "from pyspark" not in converted:
            converted = emr_imports + converted
            changes.append(ConversionChange(
                line_number=1,
                change_type="import_addition",
                original="",
                converted=emr_imports.strip(),
                description="Added standard Spark imports for EMR"
            ))

        # Replace GlueContext with SparkSession
        if "GlueContext" in converted:
            glue_context_pattern = r"glueContext\s*=\s*GlueContext\([^)]*\)"
            spark_session_code = """spark = SparkSession.builder \\
    .appName("EMR-Job") \\
    .enableHiveSupport() \\
    .getOrCreate()
sc = spark.sparkContext"""

            converted = re.sub(glue_context_pattern, spark_session_code, converted)
            changes.append(ConversionChange(
                line_number=None,
                change_type="context_replacement",
                original="GlueContext(SparkContext.getOrCreate())",
                converted=spark_session_code,
                description="Replaced GlueContext with SparkSession"
            ))

        # Remove SparkContext creation if separate
        converted = re.sub(r"sc\s*=\s*SparkContext\(\)\n?", "", converted)
        converted = re.sub(r"sc\s*=\s*SparkContext\.getOrCreate\(\)\n?", "", converted)

        # Replace Job initialization and commit
        converted = re.sub(r"job\s*=\s*Job\(glueContext\)\n?", "", converted)
        converted = re.sub(r"job\.init\([^)]*\)\n?", "", converted)
        converted = re.sub(r"job\.commit\(\)\n?", "# Job completed successfully\nprint('ETL job completed')\n", converted)
        changes.append(ConversionChange(
            line_number=None,
            change_type="job_handling",
            original="Job(glueContext).commit()",
            converted="print('ETL job completed')",
            description="Removed Glue job bookmarks (not available in EMR)"
        ))

        # Convert DynamicFrame reads from catalog
        catalog_read_pattern = r"glueContext\.create_dynamic_frame\.from_catalog\(\s*database\s*=\s*['\"]([^'\"]+)['\"]\s*,\s*table_name\s*=\s*['\"]([^'\"]+)['\"]\s*(?:,[^)]*)?)"
        for match in re.finditer(catalog_read_pattern, converted):
            database = match.group(1)
            table = match.group(2)
            spark_read = f'spark.table("{database}.{table}")'
            converted = converted.replace(match.group(0), spark_read)
            changes.append(ConversionChange(
                line_number=None,
                change_type="catalog_read",
                original=match.group(0)[:50] + "...",
                converted=spark_read,
                description=f"Converted Glue catalog read to Spark table read"
            ))

        # Convert DynamicFrame reads from S3
        s3_read_pattern = r"glueContext\.create_dynamic_frame\.from_options\(\s*connection_type\s*=\s*['\"]s3['\"]\s*,\s*connection_options\s*=\s*\{[^}]*['\"]paths['\"]\s*:\s*\[([^\]]+)\][^}]*\}[^)]*format\s*=\s*['\"]([^'\"]+)['\"][^)]*\)"
        for match in re.finditer(s3_read_pattern, converted, re.DOTALL):
            paths = match.group(1)
            format_type = match.group(2)
            spark_read = f'spark.read.format("{format_type}").load({paths})'
            converted = converted.replace(match.group(0), spark_read)
            changes.append(ConversionChange(
                line_number=None,
                change_type="s3_read",
                original="glueContext.create_dynamic_frame.from_options(...)",
                converted=spark_read,
                description="Converted Glue S3 read to Spark read"
            ))

        # Convert DynamicFrame writes
        catalog_write_pattern = r"glueContext\.write_dynamic_frame\.from_catalog\([^)]+\)"
        converted = re.sub(
            catalog_write_pattern,
            '# Write to Hive table\ndf.write.mode("overwrite").saveAsTable("database.table")',
            converted
        )

        s3_write_pattern = r"glueContext\.write_dynamic_frame\.from_options\([^)]+\)"
        converted = re.sub(
            s3_write_pattern,
            'df.write.mode("overwrite").parquet("s3://bucket/output/")',
            converted
        )

        # Convert DynamicFrame to DataFrame and vice versa
        converted = re.sub(r"(\w+)\.toDF\(\)", r"\1", converted)  # DyF.toDF() -> just use DF
        converted = re.sub(r"DynamicFrame\.fromDF\((\w+),\s*glueContext,\s*['\"][^'\"]+['\"]\)", r"\1", converted)

        # Handle getResolvedOptions
        resolved_options_pattern = r"args\s*=\s*getResolvedOptions\(sys\.argv,\s*\[([^\]]+)\]\)"
        match = re.search(resolved_options_pattern, converted)
        if match:
            args_list = match.group(1)
            emr_args = f"""# Parse arguments (EMR step arguments)
import argparse
parser = argparse.ArgumentParser()
for arg in [{args_list}]:
    parser.add_argument(f'--{{arg}}', required=True)
args = vars(parser.parse_args())"""
            converted = re.sub(resolved_options_pattern, emr_args, converted)
            changes.append(ConversionChange(
                line_number=None,
                change_type="argument_parsing",
                original="getResolvedOptions(sys.argv, [...])",
                converted="argparse-based parsing",
                description="Converted Glue argument parsing to argparse"
            ))

        # Replace glueContext references
        converted = re.sub(r"glueContext\.", "spark.", converted)

        # Handle resolveChoice
        resolve_choice_pattern = r"\.resolveChoice\(\s*specs\s*=\s*\[([^\]]+)\]\)"
        for match in re.finditer(resolve_choice_pattern, converted):
            result.warnings.append(
                f"resolveChoice() found - manual review required. "
                f"Consider using cast() or coalesce() for type handling."
            )
            changes.append(ConversionChange(
                line_number=None,
                change_type="resolve_choice",
                original=match.group(0),
                converted="# TODO: Replace with appropriate cast() calls",
                description="resolveChoice needs manual conversion",
                requires_review=True
            ))
        converted = re.sub(resolve_choice_pattern, "", converted)

        # Handle apply_mapping
        apply_mapping_pattern = r"\.apply_mapping\(\s*mappings\s*=\s*\[([^\]]+)\]\)"
        for match in re.finditer(apply_mapping_pattern, converted):
            result.warnings.append(
                "apply_mapping() found - converted to select() with alias(). Verify column names."
            )
        converted = re.sub(apply_mapping_pattern, ".select(*[col(old).alias(new) for old, new in mappings])", converted)

        # Add EMR-specific configurations
        result.config_changes = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.dynamicAllocation.enabled": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.InstanceProfileCredentialsProvider"
        }

        # Add required dependencies
        result.required_dependencies = [
            "pyspark",
            "hadoop-aws",
            "aws-java-sdk-bundle"
        ]

        # Clean up extra blank lines
        converted = re.sub(r"\n{3,}", "\n\n", converted)

        result.converted_code = converted
        result.changes = changes
        result.success = True

        if result.warnings:
            result.warnings.insert(0, "Code converted with some manual review items:")

        return result

    def _convert_glue_to_eks(self, code: str) -> ConversionResult:
        """Convert AWS Glue code to EKS/Spark Kubernetes code."""
        # First convert to EMR-compatible code
        emr_result = self._convert_glue_to_emr(code)

        if not emr_result.success:
            return emr_result

        converted = emr_result.converted_code
        changes = emr_result.changes.copy()

        # Add Kubernetes-specific SparkSession configuration
        spark_builder_pattern = r"SparkSession\.builder\s*\\\s*\n\s*\.appName\([^)]+\)"
        eks_spark_builder = """SparkSession.builder \\
    .appName("EKS-Spark-Job") \\
    .config("spark.kubernetes.container.image", "spark:latest") \\
    .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark") \\
    .config("spark.executor.instances", "3")"""

        if re.search(spark_builder_pattern, converted):
            converted = re.sub(spark_builder_pattern, eks_spark_builder, converted)
            changes.append(ConversionChange(
                line_number=None,
                change_type="eks_config",
                original="Standard SparkSession",
                converted="EKS-configured SparkSession",
                description="Added Kubernetes-specific Spark configurations"
            ))

        # Add EKS-specific configurations
        eks_configs = {
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.driver.pod.name": "spark-driver",
            "spark.kubernetes.executor.request.cores": "1",
            "spark.kubernetes.executor.limit.cores": "2",
            "spark.kubernetes.executor.request.memory": "2g",
            "spark.kubernetes.executor.limit.memory": "4g",
            "spark.kubernetes.node.selector.nodegroup": "spark-workers"
        }

        emr_result.config_changes.update(eks_configs)
        emr_result.converted_code = converted
        emr_result.changes = changes

        emr_result.required_dependencies.extend([
            "kubernetes-client",
            "spark-kubernetes"
        ])

        emr_result.warnings.append(
            "For EKS deployment, ensure Spark image is available in ECR "
            "and appropriate IAM roles are configured."
        )

        return emr_result

    def _convert_emr_to_glue(self, code: str) -> ConversionResult:
        """Convert EMR Spark code to AWS Glue code."""
        result = ConversionResult(converted_code=code)
        converted = code
        changes = []

        # Add Glue imports
        glue_imports = """import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import *
"""
        # Remove existing imports that will be replaced
        converted = re.sub(r"from pyspark\.sql import SparkSession\n?", "", converted)

        if "from awsglue" not in converted:
            converted = glue_imports + converted
            changes.append(ConversionChange(
                line_number=1,
                change_type="import_addition",
                original="",
                converted="Glue imports",
                description="Added AWS Glue imports"
            ))

        # Replace SparkSession with GlueContext
        spark_session_pattern = r"spark\s*=\s*SparkSession\.builder[^.]*(?:\.config\([^)]+\))*(?:\s*\\?\s*)*\.(?:getOrCreate|enableHiveSupport)\(\)[^)]*\)?"
        glue_context_code = """# Initialize Glue context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init('glue-job', args)"""

        if re.search(spark_session_pattern, converted, re.DOTALL):
            converted = re.sub(spark_session_pattern, glue_context_code, converted, flags=re.DOTALL)
            changes.append(ConversionChange(
                line_number=None,
                change_type="context_replacement",
                original="SparkSession.builder...",
                converted="GlueContext",
                description="Replaced SparkSession with GlueContext"
            ))

        # Add argument parsing if not present
        if "getResolvedOptions" not in converted and "args" not in converted:
            args_code = """# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
"""
            # Insert after imports
            import_end = converted.rfind("import ")
            if import_end > 0:
                line_end = converted.find("\n", import_end)
                converted = converted[:line_end+1] + "\n" + args_code + converted[line_end+1:]

        # Convert spark.table() to Glue catalog reads
        table_read_pattern = r"spark\.table\(['\"](\w+)\.(\w+)['\"]\)"
        for match in re.finditer(table_read_pattern, converted):
            database = match.group(1)
            table = match.group(2)
            glue_read = f'''glueContext.create_dynamic_frame.from_catalog(
    database="{database}",
    table_name="{table}"
).toDF()'''
            converted = converted.replace(match.group(0), glue_read)
            changes.append(ConversionChange(
                line_number=None,
                change_type="catalog_read",
                original=match.group(0),
                converted=f"Glue catalog read for {database}.{table}",
                description="Converted Spark table read to Glue catalog"
            ))

        # Convert spark.read to Glue read
        spark_read_pattern = r"spark\.read\.format\(['\"](\w+)['\"]\)\.load\(([^)]+)\)"
        for match in re.finditer(spark_read_pattern, converted):
            format_type = match.group(1)
            paths = match.group(2)
            glue_read = f'''glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={{"paths": [{paths}]}},
    format="{format_type}"
).toDF()'''
            converted = converted.replace(match.group(0), glue_read)
            changes.append(ConversionChange(
                line_number=None,
                change_type="s3_read",
                original=match.group(0),
                converted="Glue S3 read",
                description="Converted Spark read to Glue dynamic frame read"
            ))

        # Convert df.write to Glue write
        write_pattern = r"(\w+)\.write\.mode\(['\"](\w+)['\"]\)\.(\w+)\(['\"]([^'\"]+)['\"]\)"
        for match in re.finditer(write_pattern, converted):
            df_name = match.group(1)
            mode = match.group(2)
            format_type = match.group(3)
            path = match.group(4)

            glue_write = f'''# Convert to DynamicFrame and write
{df_name}_dyf = DynamicFrame.fromDF({df_name}, glueContext, "{df_name}_dyf")
glueContext.write_dynamic_frame.from_options(
    frame={df_name}_dyf,
    connection_type="s3",
    connection_options={{"path": "{path}"}},
    format="{format_type}"
)'''
            converted = converted.replace(match.group(0), glue_write)
            changes.append(ConversionChange(
                line_number=None,
                change_type="s3_write",
                original=match.group(0),
                converted="Glue dynamic frame write",
                description="Converted Spark write to Glue write"
            ))

        # Add job.commit() at the end if not present
        if "job.commit()" not in converted:
            converted = converted.rstrip() + "\n\n# Commit the job\njob.commit()\n"
            changes.append(ConversionChange(
                line_number=None,
                change_type="job_commit",
                original="",
                converted="job.commit()",
                description="Added Glue job commit for bookmarks"
            ))

        # Add Glue-specific configurations
        result.config_changes = {
            "--job-bookmark-option": "job-bookmark-enable",
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true"
        }

        result.required_dependencies = [
            "awsglue",
            "pyspark"
        ]

        result.converted_code = converted
        result.changes = changes
        result.success = True

        result.warnings.append(
            "Glue job parameters should be configured in AWS Console or via CloudFormation"
        )

        return result

    def generate_conversion_report(self, result: ConversionResult) -> str:
        """Generate a detailed conversion report."""
        report = []
        report.append("# Code Conversion Report")
        report.append(f"\n**Status:** {'Success' if result.success else 'Failed'}")

        if result.error_message:
            report.append(f"\n**Error:** {result.error_message}")
            return "\n".join(report)

        # Changes summary
        report.append(f"\n## Changes Made ({len(result.changes)})")
        for change in result.changes:
            review_flag = " ⚠️ NEEDS REVIEW" if change.requires_review else ""
            report.append(f"\n### {change.change_type}{review_flag}")
            report.append(f"- **Description:** {change.description}")
            if change.original:
                report.append(f"- **Original:** `{change.original[:100]}{'...' if len(change.original) > 100 else ''}`")
            if change.converted:
                report.append(f"- **Converted:** `{change.converted[:100]}{'...' if len(change.converted) > 100 else ''}`")

        # Warnings
        if result.warnings:
            report.append("\n## Warnings")
            for warning in result.warnings:
                report.append(f"- ⚠️ {warning}")

        # Configuration changes
        if result.config_changes:
            report.append("\n## Required Configuration Changes")
            report.append("```")
            for key, value in result.config_changes.items():
                report.append(f"{key}={value}")
            report.append("```")

        # Dependencies
        if result.required_dependencies:
            report.append("\n## Required Dependencies")
            for dep in result.required_dependencies:
                report.append(f"- {dep}")

        # Converted code preview
        report.append("\n## Converted Code Preview")
        report.append("```python")
        preview = result.converted_code[:2000]
        if len(result.converted_code) > 2000:
            preview += "\n\n# ... (truncated) ..."
        report.append(preview)
        report.append("```")

        return "\n".join(report)

    def validate_conversion(self, original: str, converted: str) -> Dict[str, Any]:
        """Validate that converted code maintains functionality."""
        validation = {
            "valid": True,
            "issues": [],
            "recommendations": []
        }

        # Check for orphaned references
        original_vars = set(re.findall(r"\b([a-zA-Z_]\w*)\b", original))
        converted_vars = set(re.findall(r"\b([a-zA-Z_]\w*)\b", converted))

        # Check for Glue-specific references in converted code (if converting from Glue)
        glue_refs = ["glueContext", "DynamicFrame", "GlueContext", "Job"]
        for ref in glue_refs:
            if ref in original and ref not in converted:
                # Expected - reference was removed
                pass
            elif ref not in original and ref in converted:
                # New reference added - verify it's properly initialized
                if ref == "glueContext" and "GlueContext" not in converted:
                    validation["issues"].append(f"{ref} used but not initialized")
                    validation["valid"] = False

        # Check for balanced parentheses and brackets
        for char_pair in [("(", ")"), ("[", "]"), ("{", "}")]:
            if converted.count(char_pair[0]) != converted.count(char_pair[1]):
                validation["issues"].append(f"Unbalanced {char_pair[0]}{char_pair[1]} in converted code")
                validation["valid"] = False

        # Check for common patterns that should be preserved
        if "spark.read" in original and "spark.read" not in converted and "create_dynamic_frame" not in converted:
            validation["issues"].append("Data read operation may have been lost in conversion")
            validation["valid"] = False

        if ".write" in original and ".write" not in converted and "write_dynamic_frame" not in converted:
            validation["issues"].append("Data write operation may have been lost in conversion")
            validation["valid"] = False

        # Recommendations
        if "cache()" in original and "cache()" not in converted:
            validation["recommendations"].append("Consider adding caching back for performance")

        return validation
