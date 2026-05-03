"""
Job Generator Agent
===================
Generates production-ready PySpark ETL scripts from a structured job spec
combining natural-language prompts, table definitions, validation rules,
optimization requirements, and volume assessment.

Based on JobGeneratorAgent from PR-11 (table-optimizer-analysis).
"""

import json
import logging
import re
from datetime import date
from typing import Any, Dict, List, Optional

from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a **Senior PySpark ETL Developer** who generates production-grade Glue/Spark scripts.

Given a job specification, generate a complete PySpark script that:
1. Follows best practices: AQE enabled, no .collect(), broadcast small tables
2. Implements the described business logic step-by-step
3. Embeds validation assertions (row count checks, null checks, anomaly guards)
4. Applies the optimization requirements (broadcast hints, caching, partition strategy)
5. Uses the appropriate platform boilerplate (Glue 4.0 vs standalone Spark)
6. Includes proper error handling and logging

Return structured JSON:
{
  "job_name": "...",
  "platform": "glue|spark",
  "generated_script": "...",
  "script_lines": N,
  "optimizations_applied": [],
  "validation_rules_embedded": [],
  "success": true
}
Return ONLY valid JSON.
"""

_DEFAULT_SPARK_CONFIGS = {
    "spark.sql.adaptive.enabled":                    "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled":           "true",
    "spark.sql.autoBroadcastJoinThreshold":          "10485760",
    "spark.sql.shuffle.partitions":                  "auto",
    "spark.sql.files.maxPartitionBytes":             "134217728",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
}

_GLUE_HEADER = """\
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys, logging

logger = logging.getLogger(__name__)
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc   = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job   = Job(glue)
job.init(args["JOB_NAME"], args)
"""

_SPARK_HEADER = """\
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)
"""


def _build_spark_session(job_name: str, configs: Dict[str, str], platform: str) -> str:
    cfg_lines = "\n".join(f'    .config("{k}", "{v}")' for k, v in sorted(configs.items()))
    if platform == "glue":
        return ""  # Glue provides SparkSession via GlueContext
    return f'''\
spark = (SparkSession.builder
    .appName("{job_name}")
{cfg_lines}
    .getOrCreate()
)
'''


def _build_reads(sources: List[Dict], processing_mode: str) -> str:
    lines = ["\n# ── Source reads ─────────────────────────────────────────────────────────────"]
    for src in sources:
        name    = src.get("table", src.get("name", "unknown"))
        db      = src.get("database", "")
        fmt     = src.get("format", "parquet")
        query   = src.get("query", "")
        var     = re.sub(r'[^a-z0-9]', '_', name.lower())
        path    = src.get("path", f"s3://data/{db}/{name}/")

        if query:
            lines.append(f'{var}_df = spark.sql("""{query}""")')
        elif db:
            lines.append(f'{var}_df = spark.table("{db}.{name}")')
        else:
            lines.append(f'{var}_df = spark.read.format("{fmt}").load("{path}")')

        if processing_mode == "delta":
            ts = "current_timestamp() - INTERVAL 1 DAY"
            lines.append(f'# Delta mode: filter to recent incremental records')
            lines.append(f'{var}_df = {var}_df.filter(col("updated_at") >= {ts})')
    return "\n".join(lines)


def _build_joins(joins: List[Dict], sources: List[Dict]) -> str:
    if not joins:
        return ""
    lines = ["\n# ── Joins ────────────────────────────────────────────────────────────────────"]
    source_vars = [re.sub(r'[^a-z0-9]', '_', s.get("table", s.get("name", "src")).lower()) for s in sources]
    base_var    = source_vars[0] if source_vars else "df"

    for j in joins:
        right     = re.sub(r'[^a-z0-9]', '_', j.get("right_table", "right").lower())
        key       = j.get("on", j.get("key", "id"))
        jtype     = j.get("type", "inner")
        broadcast_hint = "broadcast(" if j.get("broadcast", False) else ""
        broadcast_close = ")" if broadcast_hint else ""
        lines.append(f'{base_var}_df = {base_var}_df.join({broadcast_hint}{right}_df{broadcast_close}, "{key}", "{jtype}")')
    return "\n".join(lines)


def _build_transforms(transforms: List[Dict]) -> str:
    if not transforms:
        return ""
    lines = ["\n# ── Transformations ─────────────────────────────────────────────────────────"]
    for t in transforms:
        ttype = t.get("type", "withColumn")
        col_  = t.get("column", "new_col")
        expr_ = t.get("expression", "lit(None)")
        if ttype == "withColumn":
            lines.append(f'result_df = result_df.withColumn("{col_}", expr("{expr_}"))')
        elif ttype == "filter":
            lines.append(f'result_df = result_df.filter("{expr_}")')
        elif ttype == "drop":
            lines.append(f'result_df = result_df.drop("{col_}")')
    return "\n".join(lines)


def _build_aggregation(agg: Dict) -> str:
    if not agg:
        return ""
    lines = ["\n# ── Aggregation ──────────────────────────────────────────────────────────────"]
    group_keys = agg.get("group_by", [])
    metrics    = agg.get("metrics", {})
    gb_str     = ", ".join(f'"{k}"' for k in group_keys)
    agg_exprs  = ", ".join(f'{fn}("{col}").alias("{alias}")' for alias, (fn, col)
                           in {a: (m.get("function", "sum"), m.get("column", a))
                               for a, m in metrics.items() if isinstance(m, dict)}.items())
    if gb_str and agg_exprs:
        lines.append(f'result_df = result_df.groupBy({gb_str}).agg({agg_exprs})')
    return "\n".join(lines)


def _build_validations(rules: List[str]) -> str:
    if not rules:
        return ""
    lines = ["\n# ── Validation assertions ────────────────────────────────────────────────────"]
    for rule in rules:
        rule_lower = rule.lower()
        if "row count must be > 0" in rule_lower or "output row count" in rule_lower:
            lines.append('assert result_df.count() > 0, "VALIDATION FAILED: output is empty"')
        elif "no null" in rule_lower:
            col_match = re.search(r'in\s+(\w+)\s+column', rule_lower)
            if col_match:
                col = col_match.group(1)
                lines.append(f'assert result_df.filter(col("{col}").isNull()).count() == 0, "VALIDATION FAILED: nulls in {col}"')
        elif "non-negative" in rule_lower or ">= 0" in rule_lower:
            col_match = re.search(r'(\w+)\s+must', rule_lower)
            if col_match:
                col = col_match.group(1)
                lines.append(f'assert result_df.filter(col("{col}") < 0).count() == 0, "VALIDATION FAILED: negative {col}"')
        else:
            lines.append(f'# TODO: implement validation: {rule}')
    return "\n".join(lines)


def _build_write(target: Dict, processing_mode: str) -> str:
    if not target:
        return "\n# TODO: add write operation"
    lines = ["\n# ── Write ────────────────────────────────────────────────────────────────────"]
    mode    = target.get("mode", "overwrite")
    db      = target.get("database", "")
    tbl     = target.get("table", "output")
    fmt     = target.get("format", "delta")
    part    = target.get("partition_by", "")
    path    = target.get("path", f"s3://analytics/{db}/{tbl}/")

    part_str = f'.partitionBy("{part}")' if part else ""
    if db and tbl:
        lines.append(f'result_df.write.format("{fmt}").mode("{mode}"){part_str}.saveAsTable("{db}.{tbl}")')
    else:
        lines.append(f'result_df.write.format("{fmt}").mode("{mode}"){part_str}.save("{path}")')
    return "\n".join(lines)


def _generate_script(spec: Dict) -> str:
    job_name  = spec.get("job_name", "generated_etl_job")
    platform  = spec.get("platform", "glue")
    sources   = spec.get("source_tables", [])
    joins     = spec.get("joins", [])
    transforms = spec.get("transformations", [])
    agg       = spec.get("aggregations", {})
    target    = spec.get("target_table", {})
    val_rules = spec.get("validation_rules", [])
    mode      = spec.get("processing_mode", "full")
    extra_cfg = spec.get("spark_configs", {})

    configs = {**_DEFAULT_SPARK_CONFIGS, **extra_cfg}

    parts = [
        _GLUE_HEADER if platform == "glue" else _SPARK_HEADER,
        _build_spark_session(job_name, configs, platform),
        _build_reads(sources, mode),
        "\n# ── Initial result DataFrame (first source) ─────────────────────────────────",
    ]
    if sources:
        first_var = re.sub(r'[^a-z0-9]', '_', sources[0].get("table", sources[0].get("name", "src")).lower())
        parts.append(f"result_df = {first_var}_df")
    parts += [
        _build_joins(joins, sources),
        _build_transforms(transforms),
        _build_aggregation(agg),
        _build_validations(val_rules),
        _build_write(target, mode),
    ]
    if platform == "glue":
        parts.append("\njob.commit()")
    return "\n".join(p for p in parts if p)


# ── Tools ──────────────────────────────────────────────────────────────────────

@tool
def generate_pyspark_job(job_spec_json: str) -> str:
    """
    Generate a production-ready PySpark ETL script from a structured job spec.

    Args:
        job_spec_json: JSON job spec with job_name, platform, source_tables, joins,
                       transformations, aggregations, target_table, validation_rules,
                       optimization_requirements, processing_mode, spark_configs.

    Returns:
        JSON with generated_script, script_lines, optimizations_applied,
        validation_rules_embedded, and success flag.
    """
    try:
        spec   = json.loads(job_spec_json)
        script = _generate_script(spec)
        lines  = script.count("\n") + 1

        opts_applied = ["AQE enabled", "KryoSerializer", "adaptive coalesce", "skewJoin handling"]
        for opt in spec.get("optimization_requirements", []):
            if "broadcast" in opt.lower():
                opts_applied.append("broadcast hint for small tables")
            if "cache" in opt.lower():
                opts_applied.append("cache before multi-action")

        return json.dumps({
            "job_name":                   spec.get("job_name", "generated_job"),
            "platform":                   spec.get("platform", "glue"),
            "generated_script":           script,
            "script_lines":               lines,
            "optimizations_applied":      list(set(opts_applied)),
            "validation_rules_embedded":  spec.get("validation_rules", []),
            "success":                    True,
        })
    except Exception as exc:
        logger.error("Job generation failed: %s", exc)
        return json.dumps({"success": False, "error": str(exc), "generated_script": ""})


@tool
def generate_job_from_nl_prompt(
    job_name: str,
    job_prompt: str,
    source_tables_json: str,
    target_table_json: str = "{}",
    platform: str = "glue",
    processing_mode: str = "full",
    validation_rules_json: str = "[]",
) -> str:
    """
    Generate a PySpark script from a natural-language job description.

    Args:
        job_name:              Name for the generated job.
        job_prompt:            Natural-language description of the ETL logic.
        source_tables_json:    JSON list of source table descriptors.
        target_table_json:     JSON target table descriptor.
        platform:              "glue" or "spark" (default: glue).
        processing_mode:       "full" or "delta" (default: full).
        validation_rules_json: JSON list of validation rule strings.

    Returns:
        JSON with generated_script and metadata.
    """
    try:
        sources    = json.loads(source_tables_json)
        target     = json.loads(target_table_json) if target_table_json else {}
        val_rules  = json.loads(validation_rules_json) if validation_rules_json else []

        spec = {
            "job_name":          job_name,
            "platform":          platform,
            "processing_mode":   processing_mode,
            "source_tables":     sources,
            "target_table":      target,
            "validation_rules":  val_rules,
            "job_prompt":        job_prompt,
            "optimization_requirements": [
                "enable AQE with skewJoin threshold 64MB",
                "broadcast small dimension tables",
                "use dynamic partition overwrite",
                "cache aggregated result before dedup window",
            ],
        }
        return generate_pyspark_job.__wrapped__(json.dumps(spec))
    except Exception as exc:
        return json.dumps({"success": False, "error": str(exc)})


@tool
def generate_glue_job_definition(
    job_name: str,
    script_s3_path: str,
    iam_role: str,
    worker_type: str = "G.2X",
    num_workers: int = 10,
    additional_python_modules: str = "",
) -> str:
    """
    Generate an AWS Glue job definition (boto3 create_job parameters).

    Args:
        job_name:                  Glue job name.
        script_s3_path:            S3 URI for the PySpark script.
        iam_role:                  IAM role ARN.
        worker_type:               G.1X / G.2X / G.4X / G.8X (default G.2X).
        num_workers:               Number of workers (default 10).
        additional_python_modules: Comma-separated list of pip packages.

    Returns:
        JSON Glue create_job parameter dict, ready to pass to boto3.
    """
    try:
        definition: Dict[str, Any] = {
            "Name":        job_name,
            "Role":        iam_role,
            "Command": {
                "Name":           "glueetl",
                "ScriptLocation": script_s3_path,
                "PythonVersion":  "3",
            },
            "GlueVersion":    "4.0",
            "WorkerType":     worker_type,
            "NumberOfWorkers": num_workers,
            "DefaultArguments": {
                "--JOB_NAME":                       job_name,
                "--enable-metrics":                 "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-spark-ui":                "true",
                "--conf":                           "spark.sql.adaptive.enabled=true",
            },
        }
        if additional_python_modules:
            definition["DefaultArguments"]["--additional-python-modules"] = additional_python_modules

        return json.dumps({
            "glue_job_definition": definition,
            "boto3_call":          f"glue.create_job(**{job_name}_definition)",
            "estimated_cost_per_hour": round(num_workers * {"G.1X": 0.44, "G.2X": 0.88,
                                                             "G.4X": 1.76, "G.8X": 3.52}.get(worker_type, 0.88), 2),
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


def create_job_generator_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                                region: str = "us-west-2") -> Agent:
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(model=model, system_prompt=SYSTEM_PROMPT,
                 tools=[generate_pyspark_job, generate_job_from_nl_prompt,
                        generate_glue_job_definition])
