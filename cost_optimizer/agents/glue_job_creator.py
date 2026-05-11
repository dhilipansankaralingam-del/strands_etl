"""
Glue Job Creator Agent
========================
Creates or updates an AWS Glue Spark job from a local (optimized) PySpark script.

Workflow
--------
1. Upload the script to S3   → s3://<bucket>/<prefix>/<job_name>.py
2. Create or update the Glue job with:
     - correct worker type + count (from analysis or metric recommendations)
     - Glue 4.0, Python 3, Spark engine
     - default job arguments (AQE flags, --job-bookmark-option, etc.)
     - optional connections, extra JARs, tags
3. Return the Glue job name, console URL, and S3 script URI

Usage
-----
    from cost_optimizer.agents.glue_job_creator import GlueJobCreatorAgent

    agent = GlueJobCreatorAgent(region="us-west-2")
    result = agent.create_or_update(
        script_local_path = "./jobs/customer_orders_optimized.py",
        job_name          = "customer-orders-etl",
        s3_bucket         = "my-glue-scripts",
        iam_role_arn      = "arn:aws:iam::123456789012:role/GlueRole",
        worker_type       = "G.2X",
        number_of_workers = 10,
        worker_recommendation = {...},   # from GlueMetricsAnalyzer
    )
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional


_GLUE_CONSOLE_BASE = "https://console.aws.amazon.com/gluestudio/home?region={region}#/editor/jobs/{job_name}"

# Spark / Glue configuration applied to every created job
_DEFAULT_JOB_ARGS: Dict[str, str] = {
    "--enable-metrics":                              "true",
    "--enable-continuous-cloudwatch-log":            "true",
    "--enable-spark-ui":                             "true",
    "--enable-job-insights":                         "true",
    "--job-bookmark-option":                         "job-bookmark-enable",
    "--conf": (
        "spark.sql.adaptive.enabled=true"
        " --conf spark.sql.adaptive.coalescePartitions.enabled=true"
        " --conf spark.sql.adaptive.skewJoin.enabled=true"
        " --conf spark.serializer=org.apache.spark.serializer.KryoSerializer"
        " --conf spark.sql.shuffle.partitions=auto"
    ),
}

_GLUE_VERSIONS = {"4.0", "3.0", "2.0"}


class GlueJobCreatorAgent:
    """
    Creates or updates an AWS Glue Spark job from a local PySpark script.

    Does not require the CostOptimizerAgent base class because it performs
    a single deterministic action (not dual-mode rule/LLM analysis).
    """

    def __init__(self, region: str = "us-west-2"):
        self.region = region

    # ─── Public entry point ───────────────────────────────────────────────────

    def create_or_update(
        self,
        script_local_path: str,
        job_name: str,
        s3_bucket: str,
        iam_role_arn: str,
        s3_script_prefix: str = "glue-scripts",
        worker_type: str = "G.2X",
        number_of_workers: int = 10,
        glue_version: str = "4.0",
        python_version: str = "3",
        timeout_minutes: int = 2880,
        max_retries: int = 1,
        max_concurrent_runs: int = 1,
        connections: Optional[List[str]] = None,
        extra_jars: Optional[List[str]] = None,
        extra_py_files: Optional[List[str]] = None,
        extra_job_args: Optional[Dict[str, str]] = None,
        tags: Optional[Dict[str, str]] = None,
        worker_recommendation: Optional[Dict[str, Any]] = None,
        description: str = "",
        enable_bookmark: bool = True,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Upload *script_local_path* to S3, then create or update a Glue Spark job.

        If *worker_recommendation* is provided (output from GlueMetricsAnalyzer),
        its recommended_workers and recommended_type take priority over the
        *worker_type* / *number_of_workers* parameters.

        Args
        ----
        script_local_path   Local path to the optimized PySpark script.
        job_name            Glue job name (must be unique per account/region).
        s3_bucket           S3 bucket for the script.
        iam_role_arn        IAM role ARN with Glue + S3 permissions.
        s3_script_prefix    S3 key prefix (default: glue-scripts).
        worker_type         G.1X | G.2X | G.4X | G.8X (default: G.2X).
        number_of_workers   Worker count (default: 10).
        glue_version        Glue version string (default: 4.0).
        timeout_minutes     Job timeout in minutes (default: 2880 = 48 h).
        max_retries         Automatic retry count (default: 1).
        max_concurrent_runs Max parallel runs (default: 1).
        connections         Glue connection names (JDBC, VPC, etc.).
        extra_jars          Comma-separated S3 URIs for extra JARs.
        extra_py_files      Comma-separated S3 URIs for extra Python files.
        extra_job_args      Additional default job arguments.
        tags                AWS resource tags.
        worker_recommendation  Dict from GlueMetricsAnalyzer.get_worker_recommendation().
        description         Human-readable job description.
        enable_bookmark     Enable Glue job bookmarks (default: True).
        dry_run             If True, return the would-be API payload without calling AWS.

        Returns
        -------
        Dict with: success, action (created|updated|dry_run), job_name,
                   s3_script_uri, console_url, job_definition, errors
        """
        # Apply metric-based recommendations if available
        if worker_recommendation and worker_recommendation.get("changed"):
            worker_type        = worker_recommendation.get("recommended_type",  worker_type)
            number_of_workers  = worker_recommendation.get("recommended_workers", number_of_workers)

        if glue_version not in _GLUE_VERSIONS:
            glue_version = "4.0"

        # Build S3 script key
        today       = date.today().isoformat()
        script_key  = f"{s3_script_prefix}/{job_name}/{today}/{Path(script_local_path).name}"
        s3_uri      = f"s3://{s3_bucket}/{script_key}"

        # Build job arguments
        job_args = dict(_DEFAULT_JOB_ARGS)
        if not enable_bookmark:
            job_args["--job-bookmark-option"] = "job-bookmark-disable"
        if extra_job_args:
            job_args.update(extra_job_args)

        # Build job definition
        job_def: Dict[str, Any] = {
            "Name":             job_name,
            "Description":      description or f"Optimized Glue job: {job_name}",
            "Role":             iam_role_arn,
            "ExecutionProperty": {"MaxConcurrentRuns": max_concurrent_runs},
            "Command": {
                "Name":           "glueetl",
                "ScriptLocation": s3_uri,
                "PythonVersion":  python_version,
            },
            "DefaultArguments": job_args,
            "GlueVersion":      glue_version,
            "WorkerType":       worker_type,
            "NumberOfWorkers":  number_of_workers,
            "Timeout":          timeout_minutes,
            "MaxRetries":       max_retries,
            "Tags":             {
                "optimizer":    "strands_optimizer",
                "generated_on": today,
                **(tags or {}),
            },
        }

        if connections:
            job_def["Connections"] = {"Connections": connections}

        if extra_jars:
            job_args["--extra-jars"] = ",".join(extra_jars) if isinstance(extra_jars, list) else extra_jars

        if extra_py_files:
            job_args["--extra-py-files"] = ",".join(extra_py_files) if isinstance(extra_py_files, list) else extra_py_files

        console_url = _GLUE_CONSOLE_BASE.format(region=self.region, job_name=job_name)

        if dry_run:
            return {
                "success":        True,
                "action":         "dry_run",
                "job_name":       job_name,
                "s3_script_uri":  s3_uri,
                "console_url":    console_url,
                "job_definition": job_def,
                "errors":         [],
            }

        # ── Upload script to S3 ───────────────────────────────────────────────
        try:
            import boto3
            from botocore.exceptions import ClientError, NoCredentialsError
        except ImportError:
            return {
                "success": False,
                "errors":  ["boto3 not installed – run: pip install boto3"],
            }

        try:
            script_body = Path(script_local_path).read_bytes()
        except OSError as exc:
            return {"success": False, "errors": [f"Cannot read script: {exc}"]}

        try:
            s3 = boto3.client("s3", region_name=self.region)
            s3.put_object(
                Bucket      = s3_bucket,
                Key         = script_key,
                Body        = script_body,
                ContentType = "text/x-python",
                Metadata    = {
                    "generated-by": "strands_optimizer",
                    "job-name":     job_name,
                },
            )
        except (ClientError, NoCredentialsError) as exc:
            return {"success": False, "errors": [f"S3 upload failed: {exc}"]}

        # ── Create or update Glue job ─────────────────────────────────────────
        glue   = boto3.client("glue", region_name=self.region)
        action = "created"
        errors: List[str] = []

        try:
            glue.get_job(JobName=job_name)
            # Job exists → update it
            update_payload = {k: v for k, v in job_def.items()
                              if k not in ("Name", "Tags")}
            glue.update_job(JobName=job_name, JobUpdate=update_payload)
            action = "updated"
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "EntityNotFoundException":
                try:
                    glue.create_job(**job_def)
                    action = "created"
                except ClientError as create_exc:
                    return {
                        "success": False,
                        "errors":  [f"Glue create_job failed: {create_exc}"],
                    }
            else:
                return {"success": False, "errors": [f"Glue error: {exc}"]}

        return {
            "success":        True,
            "action":         action,
            "job_name":       job_name,
            "s3_script_uri":  s3_uri,
            "console_url":    console_url,
            "worker_type":    worker_type,
            "number_of_workers": number_of_workers,
            "glue_version":   glue_version,
            "job_definition": job_def,
            "errors":         errors,
        }

    def start_job_run(
        self,
        job_name: str,
        job_args: Optional[Dict[str, str]] = None,
        timeout_minutes: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Trigger a single run of the Glue job.

        Returns run_id and CloudWatch log group URL on success.
        """
        try:
            import boto3
            from botocore.exceptions import ClientError, NoCredentialsError
        except ImportError:
            return {"success": False, "errors": ["boto3 not installed"]}

        glue    = boto3.client("glue", region_name=self.region)
        kwargs: Dict[str, Any] = {"JobName": job_name}
        if job_args:
            kwargs["Arguments"]     = job_args
        if timeout_minutes:
            kwargs["Timeout"]       = timeout_minutes

        try:
            resp   = glue.start_job_run(**kwargs)
            run_id = resp["JobRunId"]
            cw_url = (
                f"https://console.aws.amazon.com/cloudwatch/home?region={self.region}"
                f"#logsV2:log-groups/log-group/$252Faws-glue$252Fjobs$252Foutput"
                f"/log-events/{job_name}$252F{run_id}"
            )
            return {
                "success":            True,
                "job_name":           job_name,
                "run_id":             run_id,
                "cloudwatch_url":     cw_url,
            }
        except (ClientError, NoCredentialsError) as exc:
            return {"success": False, "errors": [str(exc)]}

    def generate_job_documentation(
        self,
        job_name: str,
        script_content: str,
        job_definition: Dict[str, Any],
        source_tables: List[Dict],
        analysis_result: Optional[Dict] = None,
        test_file_path: Optional[str] = None,
    ) -> str:
        """
        Generate a Markdown documentation file for the Glue job.

        Covers: purpose, source tables, target, Spark config, worker config,
        how to run locally, how to trigger the Glue job, test instructions.
        """
        today   = date.today().isoformat()
        workers = job_definition.get("NumberOfWorkers", "?")
        w_type  = job_definition.get("WorkerType",       "?")
        version = job_definition.get("GlueVersion",      "4.0")
        s3_uri  = job_definition.get("Command", {}).get("ScriptLocation", "?")
        role    = job_definition.get("Role",              "?")

        # Extract key configs from job args
        args    = job_definition.get("DefaultArguments", {})
        bookmark= args.get("--job-bookmark-option", "disabled")

        # Build table summary
        table_rows = ""
        for t in source_tables:
            db    = t.get("database", "")
            tbl   = t.get("table",    t.get("name", "unknown"))
            fmt   = t.get("format",   "parquet")
            recs  = t.get("record_count", 0)
            size  = f"{t.get('size_gb', 0):.2f} GB" if t.get("size_gb") else f"~{recs:,} rows"
            bcast = "✓" if t.get("broadcast") else ""
            part  = t.get("partition_column", "")
            table_rows += (
                f"| `{db}.{tbl}` | {fmt.upper()} | {size} | {part} | {bcast} |\n"
            )

        # Savings summary from analysis
        savings_section = ""
        if analysis_result:
            s = analysis_result.get("summary", {})
            cur  = s.get("current_cost_per_run",  0)
            opt  = s.get("optimal_cost_per_run",  0)
            pct  = s.get("potential_savings_percent", 0)
            annual = s.get("potential_annual_savings", 0)
            if pct:
                savings_section = f"""
## Cost Optimisation Summary

| Metric | Value |
|--------|-------|
| Current cost / run | ${cur:.3f} |
| Optimised cost / run | ${opt:.3f} |
| Savings per run | {pct:.0f}% |
| Estimated annual savings | ${annual:,.0f} |
"""

        # Test section
        test_section = ""
        if test_file_path:
            test_section = f"""
## Running Tests

Tests were auto-generated for this job.

```bash
# Install dependencies
pip install pytest pyspark

# Run all tests (local Spark session – no AWS needed)
pytest {test_file_path} -v

# Run only static checks (no Spark required)
pytest {test_file_path} -v -k "StaticChecks"

# Run data integrity tests
pytest {test_file_path} -v -k "DataIntegrity"
```
"""

        # Pre-compute values to avoid backslash inside f-string expressions (Python < 3.12)
        first_line = (script_content.splitlines()[0] if script_content else "").strip()
        script_filename = first_line or f"{job_name}.py"
        default_table_row = "| (auto-detected at runtime) | | | | |"
        table_section = table_rows if table_rows else default_table_row

        doc = f"""# {job_name}

> Auto-generated documentation — `strands_optimizer` — {today}

## Overview

| Property | Value |
|----------|-------|
| Glue Job Name | `{job_name}` |
| Glue Version | {version} |
| Worker Type | {w_type} |
| Number of Workers | {workers} |
| IAM Role | `{role}` |
| Script S3 URI | `{s3_uri}` |
| Job Bookmark | {bookmark} |

## Source Tables

| Table | Format | Size / Rows | Partition Column | Broadcast |
|-------|--------|-------------|-----------------|-----------|
{table_section}
{savings_section}
## Spark Configuration (injected by optimizer)

Key configs set in the script:

| Config | Value | Why |
|--------|-------|-----|
| `spark.sql.adaptive.enabled` | `true` | AQE dynamically optimises shuffle partitions, joins, skew |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Splits skewed partitions automatically |
| `spark.sql.shuffle.partitions` | `auto` | AQE auto-tunes partition count |
| `spark.sql.autoBroadcastJoinThreshold` | `10485760` (10 MB) | Auto-broadcasts small tables |
| `spark.sql.files.maxPartitionBytes` | `134217728` (128 MB) | Targets one task ≈ one Parquet file |
| `spark.serializer` | `KryoSerializer` | 3–10× faster than Java serialiser |

## Running Locally (for development / debugging)

```bash
# Install dependencies
pip install pyspark boto3

# Run with a local Spark session (set PYSPARK_SUBMIT_ARGS if needed)
python {script_filename} \\
  --JOB_NAME {job_name} \\
  --job-bookmark-option job-bookmark-disable
```

## Triggering the Glue Job

### AWS Console
1. Open: <https://console.aws.amazon.com/glue>
2. Navigate to **Jobs** → **{job_name}**
3. Click **Run**

### AWS CLI

```bash
aws glue start-job-run \\
  --job-name "{job_name}" \\
  --region {self.region}
```

### Python (boto3)

```python
import boto3
client = boto3.client("glue", region_name="{self.region}")
run = client.start_job_run(JobName="{job_name}")
print(run["JobRunId"])
```

### strands_optimizer CLI

```bash
python -m cost_optimizer.strands_optimizer \\
  --script path/to/{job_name}.py \\
  --apply-fixes \\
  --create-glue-job \\
  --glue-job-name {job_name} \\
  --glue-role-arn <YOUR_ROLE_ARN> \\
  --glue-script-bucket <YOUR_BUCKET> \\
  --run-tests
```
{test_section}
## Monitoring

- **CloudWatch Logs**: `/aws/glue/jobs/output`
- **Glue Job Metrics**: CloudWatch namespace `Glue`
- **Key metrics to watch**:
  - `glue.ALL.jvm.heap.usage` — keep below 0.80
  - `glue.driver.workerutilized` — should match `NumberOfWorkers`
  - `glue.ALL.system.cpuSystemLoad` — healthy range: 0.40–0.75

## Maintenance

Re-run `strands_optimizer` after significant data volume changes or when
`glue.ALL.jvm.heap.usage` p90 exceeds 0.80 in CloudWatch to get updated
worker-type and memory recommendations.
"""
        return doc
