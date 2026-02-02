"""
Glue to EMR/EKS Script Converter
Converts AWS Glue scripts to run on EMR or EKS with Spark Operator
"""

import re
import logging
from typing import Dict, Any, List, Tuple
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ConversionResult:
    """Result of script conversion."""
    original_code: str
    converted_code: str
    changes_made: List[str]
    warnings: List[str]
    spark_conf: Dict[str, str]


class GlueScriptConverter:
    """
    Converts AWS Glue PySpark scripts to run on EMR or EKS.

    Handles:
    - GlueContext to SparkSession conversion
    - DynamicFrame to DataFrame conversion
    - Glue Catalog access preservation
    - Job bookmarks to checkpointing
    """

    # Patterns to detect and replace
    GLUE_PATTERNS = {
        # Import statements
        'glue_imports': r'from awsglue\.(transforms|utils|context|job|dynamicframe) import \*?.*',
        'glue_context_import': r'from awsglue\.context import GlueContext',

        # Context creation
        'spark_context': r'sc\s*=\s*SparkContext\(\)',
        'glue_context': r'glueContext\s*=\s*GlueContext\(sc\)',
        'spark_session_from_glue': r'spark\s*=\s*glueContext\.spark_session',

        # Job initialization
        'job_init': r'job\s*=\s*Job\(glueContext\).*\n.*job\.init\(.*\)',
        'job_commit': r'job\.commit\(\)',
        'resolved_options': r'args\s*=\s*getResolvedOptions\(sys\.argv,\s*\[.*\]\)',

        # DynamicFrame operations
        'create_dynamic_frame_catalog': r'glueContext\.create_dynamic_frame\.from_catalog\(\s*database\s*=\s*["\'](\w+)["\']\s*,\s*table_name\s*=\s*["\'](\w+)["\']\s*\)',
        'create_dynamic_frame_s3': r'glueContext\.create_dynamic_frame\.from_options\([^)]+\)',
        'to_df': r'\.toDF\(\)',
        'from_df': r'DynamicFrame\.fromDF\([^)]+\)',

        # Write operations
        'write_dynamic_frame_catalog': r'glueContext\.write_dynamic_frame\.from_catalog\([^)]+\)',
        'write_dynamic_frame_s3': r'glueContext\.write_dynamic_frame\.from_options\([^)]+\)',
    }

    # Spark configuration for Glue Catalog access
    GLUE_CATALOG_SPARK_CONF = {
        "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        "spark.sql.catalogImplementation": "hive",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
    }

    def __init__(self):
        """Initialize the converter."""
        pass

    def convert(self, glue_code: str, target: str = "eks") -> ConversionResult:
        """
        Convert Glue script to EMR/EKS compatible script.

        Args:
            glue_code: Original Glue PySpark code
            target: 'emr' or 'eks'

        Returns:
            ConversionResult with converted code and metadata
        """
        changes = []
        warnings = []
        converted = glue_code

        # Step 1: Replace imports
        converted, import_changes = self._convert_imports(converted)
        changes.extend(import_changes)

        # Step 2: Replace context/session creation
        converted, context_changes = self._convert_context(converted, target)
        changes.extend(context_changes)

        # Step 3: Convert DynamicFrame operations to DataFrame
        converted, df_changes, df_warnings = self._convert_dynamic_frames(converted)
        changes.extend(df_changes)
        warnings.extend(df_warnings)

        # Step 4: Remove job bookmarks (replace with checkpointing if needed)
        converted, job_changes = self._convert_job_management(converted)
        changes.extend(job_changes)

        # Step 5: Add main function wrapper if not present
        if 'if __name__' not in converted:
            converted = self._wrap_in_main(converted)
            changes.append("Wrapped code in main() function")

        # Determine required Spark configuration
        spark_conf = self.GLUE_CATALOG_SPARK_CONF.copy()

        return ConversionResult(
            original_code=glue_code,
            converted_code=converted,
            changes_made=changes,
            warnings=warnings,
            spark_conf=spark_conf
        )

    def _convert_imports(self, code: str) -> Tuple[str, List[str]]:
        """Convert Glue imports to standard PySpark imports."""
        changes = []

        # Remove Glue-specific imports
        glue_import_pattern = r'^from awsglue.*$\n?'
        if re.search(glue_import_pattern, code, re.MULTILINE):
            code = re.sub(glue_import_pattern, '', code, flags=re.MULTILINE)
            changes.append("Removed awsglue imports")

        # Remove getResolvedOptions import
        if 'getResolvedOptions' in code:
            code = re.sub(r'^from awsglue\.utils import getResolvedOptions\n?', '', code, flags=re.MULTILINE)

        # Add standard PySpark imports if not present
        standard_imports = """from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sys
"""

        if 'from pyspark.sql import SparkSession' not in code:
            # Find the first import or add at top
            import_match = re.search(r'^(import |from )', code, re.MULTILINE)
            if import_match:
                code = code[:import_match.start()] + standard_imports + code[import_match.start():]
            else:
                code = standard_imports + code
            changes.append("Added standard PySpark imports")

        return code, changes

    def _convert_context(self, code: str, target: str) -> Tuple[str, List[str]]:
        """Convert GlueContext to SparkSession."""
        changes = []

        # Pattern to find and replace context creation block
        context_block_pattern = r'''(sc\s*=\s*SparkContext\(\)\s*\n)?(glueContext\s*=\s*GlueContext\(sc\)\s*\n)?(spark\s*=\s*glueContext\.spark_session\s*\n)?'''

        spark_session_code = '''# Create Spark session with Glue Catalog support
spark = SparkSession.builder \\
    .appName("converted_etl_job") \\
    .enableHiveSupport() \\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
'''

        if re.search(r'glueContext\s*=\s*GlueContext', code):
            # Remove old context creation
            code = re.sub(r'sc\s*=\s*SparkContext\(\)\s*\n?', '', code)
            code = re.sub(r'glueContext\s*=\s*GlueContext\(sc\)\s*\n?', '', code)
            code = re.sub(r'spark\s*=\s*glueContext\.spark_session\s*\n?', '', code)

            # Add new SparkSession creation after imports
            # Find end of imports
            import_end = 0
            for match in re.finditer(r'^(import |from ).*$', code, re.MULTILINE):
                import_end = match.end()

            code = code[:import_end] + '\n\n' + spark_session_code + code[import_end:]
            changes.append("Replaced GlueContext with SparkSession")

        return code, changes

    def _convert_dynamic_frames(self, code: str) -> Tuple[str, List[str], List[str]]:
        """Convert DynamicFrame operations to DataFrame operations."""
        changes = []
        warnings = []

        # Convert create_dynamic_frame.from_catalog to spark.table
        catalog_pattern = r'(\w+)\s*=\s*glueContext\.create_dynamic_frame\.from_catalog\(\s*database\s*=\s*["\'](\w+)["\']\s*,\s*table_name\s*=\s*["\'](\w+)["\']\s*(?:,\s*transformation_ctx\s*=\s*["\'][^"\']*["\']\s*)?\)\.toDF\(\)'

        def replace_catalog_read(match):
            var_name = match.group(1)
            database = match.group(2)
            table = match.group(3)
            return f'{var_name} = spark.table("{database}.{table}")'

        if re.search(catalog_pattern, code):
            code = re.sub(catalog_pattern, replace_catalog_read, code)
            changes.append("Converted DynamicFrame.from_catalog to spark.table")

        # Also handle without .toDF()
        catalog_pattern_no_todf = r'(\w+)\s*=\s*glueContext\.create_dynamic_frame\.from_catalog\(\s*database\s*=\s*["\'](\w+)["\']\s*,\s*table_name\s*=\s*["\'](\w+)["\']\s*(?:,\s*transformation_ctx\s*=\s*["\'][^"\']*["\']\s*)?\)'

        if re.search(catalog_pattern_no_todf, code):
            code = re.sub(catalog_pattern_no_todf, replace_catalog_read, code)
            changes.append("Converted DynamicFrame.from_catalog to spark.table (no toDF)")

        # Convert from_options (S3 reads)
        s3_pattern = r'glueContext\.create_dynamic_frame\.from_options\(\s*connection_type\s*=\s*["\']s3["\']\s*,\s*connection_options\s*=\s*\{[^}]*["\']paths["\']\s*:\s*\[([^\]]+)\][^}]*\}[^)]*\)'

        def replace_s3_read(match):
            paths = match.group(1)
            return f'spark.read.parquet({paths})'

        if re.search(s3_pattern, code):
            code = re.sub(s3_pattern, replace_s3_read, code)
            changes.append("Converted S3 read to spark.read")
            warnings.append("S3 read format assumed to be parquet - verify and adjust if needed")

        # Remove any remaining .toDF() on DataFrames (not needed)
        code = re.sub(r'\.toDF\(\)\s*\.toDF\(\)', '.toDF()', code)

        # Convert write operations
        write_pattern = r'glueContext\.write_dynamic_frame\.from_options\([^)]+\)'
        if re.search(write_pattern, code):
            warnings.append("Manual review needed for write_dynamic_frame operations")

        return code, changes, warnings

    def _convert_job_management(self, code: str) -> Tuple[str, List[str]]:
        """Convert job management (bookmarks, etc.)."""
        changes = []

        # Remove Job initialization
        job_init_pattern = r'job\s*=\s*Job\(glueContext\)\s*\n\s*job\.init\([^)]+\)\s*\n?'
        if re.search(job_init_pattern, code):
            code = re.sub(job_init_pattern, '', code)
            changes.append("Removed Job initialization (bookmarks not supported - use checkpointing)")

        # Remove job.commit()
        if 'job.commit()' in code:
            code = re.sub(r'job\.commit\(\)\s*\n?', '', code)
            changes.append("Removed job.commit()")

        # Remove getResolvedOptions
        resolved_pattern = r'args\s*=\s*getResolvedOptions\(sys\.argv,\s*\[[^\]]+\]\)\s*\n?'
        if re.search(resolved_pattern, code):
            code = re.sub(resolved_pattern, '# Arguments can be passed via spark-submit or environment variables\n', code)
            changes.append("Removed getResolvedOptions - use spark-submit args instead")

        # Add spark.stop() at end if not present
        if 'spark.stop()' not in code:
            code = code.rstrip() + '\n\nspark.stop()\n'
            changes.append("Added spark.stop() at end")

        return code, changes

    def _wrap_in_main(self, code: str) -> str:
        """Wrap code in main function."""
        # Find imports (keep them at top level)
        import_lines = []
        other_lines = []

        for line in code.split('\n'):
            if line.strip().startswith(('import ', 'from ')) and 'spark' not in line.lower():
                import_lines.append(line)
            else:
                other_lines.append(line)

        imports = '\n'.join(import_lines)
        main_body = '\n'.join(other_lines)

        # Indent main body
        indented_body = '\n'.join('    ' + line if line.strip() else line for line in main_body.split('\n'))

        return f'''{imports}


def main():
{indented_body}


if __name__ == "__main__":
    main()
'''

    def generate_spark_submit_command(self, script_path: str,
                                       spark_conf: Dict[str, str],
                                       target: str = "emr") -> str:
        """Generate spark-submit command with proper configuration."""
        conf_args = ' \\\n    '.join([f'--conf "{k}={v}"' for k, v in spark_conf.items()])

        if target == "emr":
            return f'''spark-submit \\
    --master yarn \\
    --deploy-mode cluster \\
    {conf_args} \\
    {script_path}'''
        else:  # EKS
            return f'''# For EKS, configuration is in SparkApplication manifest
# Or use spark-submit with k8s master:
spark-submit \\
    --master k8s://https://<k8s-api-server> \\
    --deploy-mode cluster \\
    --name converted-etl \\
    {conf_args} \\
    {script_path}'''

    def generate_spark_application_yaml(self,
                                         job_name: str,
                                         script_path: str,
                                         spark_conf: Dict[str, str]) -> str:
        """Generate Kubernetes SparkApplication manifest."""
        conf_yaml = '\n    '.join([f'"{k}": "{v}"' for k, v in spark_conf.items()])

        return f'''apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: {job_name}
  namespace: spark-jobs
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: YOUR_ECR_REPO/spark-glue:latest
  imagePullPolicy: Always
  mainApplicationFile: {script_path}
  sparkVersion: "3.5.0"

  sparkConf:
    {conf_yaml}
    "spark.dynamicAllocation.enabled": "true"
    "spark.dynamicAllocation.minExecutors": "2"
    "spark.dynamicAllocation.maxExecutors": "20"

  driver:
    cores: 2
    memory: "4g"
    serviceAccount: spark-sa
    nodeSelector:
      karpenter.sh/capacity-type: on-demand
    tolerations:
      - key: "spark-workload"
        operator: "Exists"
        effect: "NoSchedule"

  executor:
    cores: 4
    instances: 5
    memory: "8g"
    nodeSelector:
      karpenter.sh/capacity-type: spot
      kubernetes.io/arch: arm64
    tolerations:
      - key: "spark-workload"
        operator: "Exists"
        effect: "NoSchedule"

  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
'''


# Example usage
if __name__ == '__main__':
    # Sample Glue script
    sample_glue_script = '''
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from Glue Catalog
orders_df = glueContext.create_dynamic_frame.from_catalog(
    database="ecommerce_db",
    table_name="orders",
    transformation_ctx="orders_ctx"
).toDF()

customers_df = glueContext.create_dynamic_frame.from_catalog(
    database="ecommerce_db",
    table_name="customers",
    transformation_ctx="customers_ctx"
).toDF()

# Join and transform
result = orders_df.join(customers_df, "customer_id")
result = result.groupBy("customer_id").agg({"total_amount": "sum"})

# Write output
result.write.mode("overwrite").parquet("s3://bucket/output/")

job.commit()
'''

    converter = GlueScriptConverter()
    result = converter.convert(sample_glue_script, target="eks")

    print("=" * 60)
    print("GLUE TO EKS SCRIPT CONVERTER")
    print("=" * 60)

    print("\n--- Changes Made ---")
    for change in result.changes_made:
        print(f"  ✓ {change}")

    if result.warnings:
        print("\n--- Warnings ---")
        for warning in result.warnings:
            print(f"  ⚠ {warning}")

    print("\n--- Required Spark Configuration ---")
    for key, value in result.spark_conf.items():
        print(f"  {key}:")
        print(f"    {value}")

    print("\n--- Converted Script ---")
    print(result.converted_code)

    print("\n--- SparkApplication Manifest ---")
    print(converter.generate_spark_application_yaml(
        "customer-360-etl",
        "local:///opt/spark/scripts/customer_360_etl.py",
        result.spark_conf
    ))
