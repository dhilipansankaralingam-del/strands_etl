"""
Execution Agent
===============
Submits and monitors ETL jobs on AWS Glue, EMR, or Lambda.
Applies resource-allocator recommendations before submission.
"""

import json
import logging
import time
import uuid
from typing import Any, Dict

import boto3
from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are an **ETL Execution Specialist** responsible for submitting and monitoring data pipeline jobs.

Your responsibilities:
1. Select the correct execution platform (Glue / EMR / Lambda) based on resource-allocator output.
2. Apply optimal worker configuration before submission.
3. Monitor job progress and detect failures early.
4. Return a structured execution report.

Return structured JSON:
{
  "platform": "glue|emr|lambda",
  "job_name": "...",
  "run_id": "...",
  "status": "running|completed|failed",
  "submitted_at": "ISO8601",
  "completed_at": "ISO8601",
  "duration_seconds": N,
  "error": null | "...",
  "metrics": {}
}

Return ONLY valid JSON.
"""


@tool
def submit_glue_job(
    job_config_json: str,
    resource_allocation_json: str = "{}",
) -> str:
    """
    Create and start an AWS Glue ETL job.

    Args:
        job_config_json:          JSON with script_location, iam_role, and optional arguments.
        resource_allocation_json: JSON from Resource Allocator (workers, worker_type, …).

    Returns:
        JSON with job_name, run_id, status, and submitted_at.
    """
    try:
        config    = json.loads(job_config_json)
        resources = json.loads(resource_allocation_json) if resource_allocation_json else {}

        job_name = f"strands-etl-{uuid.uuid4().hex[:8]}"
        glue     = boto3.client("glue")

        optimal  = resources.get("optimal_config", {})
        workers  = optimal.get("workers",     config.get("workers", 10))
        wtype    = optimal.get("worker_type", config.get("worker_type", "G.2X"))

        glue.create_job(
            Name=job_name,
            Role=config.get("iam_role", "GlueServiceRole"),
            Command={
                "Name":           "glueetl",
                "ScriptLocation": config["script_location"],
                "PythonVersion":  "3",
            },
            DefaultArguments={
                "--config_path": config.get("config_s3_path", ""),
                "--JOB_NAME":    job_name,
            },
            WorkerType=wtype,
            NumberOfWorkers=workers,
            GlueVersion="4.0",
        )

        run   = glue.start_job_run(JobName=job_name)
        run_id = run["JobRunId"]

        return json.dumps({
            "platform":     "glue",
            "job_name":     job_name,
            "run_id":       run_id,
            "status":       "running",
            "workers":      workers,
            "worker_type":  wtype,
            "submitted_at": __import__("datetime").datetime.utcnow().isoformat(),
        })
    except Exception as exc:
        logger.error("Glue job submission failed: %s", exc)
        return json.dumps({"error": str(exc), "status": "failed"})


@tool
def monitor_glue_job(job_name: str, run_id: str, timeout_seconds: int = 3600) -> str:
    """
    Poll an AWS Glue job until it reaches a terminal state.

    Args:
        job_name:        Glue job name.
        run_id:          Job run ID.
        timeout_seconds: Maximum wait time (default 1 hour).

    Returns:
        JSON with final_status, duration_seconds, and job_run_details.
    """
    try:
        glue         = boto3.client("glue")
        poll_interval = 30
        start        = time.time()

        while time.time() - start < timeout_seconds:
            resp   = glue.get_job_run(JobName=job_name, RunId=run_id)
            run    = resp["JobRun"]
            status = run["JobRunState"]

            if status in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"):
                return json.dumps({
                    "job_name":        job_name,
                    "run_id":          run_id,
                    "final_status":    status,
                    "status":          "completed" if status == "SUCCEEDED" else "failed",
                    "duration_seconds": int(time.time() - start),
                    "error":           run.get("ErrorMessage") if status != "SUCCEEDED" else None,
                    "metrics":         {
                        "workers":           run.get("NumberOfWorkers"),
                        "max_capacity":      run.get("MaxCapacity"),
                        "execution_time":    run.get("ExecutionTime"),
                    },
                })

            time.sleep(poll_interval)

        return json.dumps({
            "job_name": job_name,
            "run_id":   run_id,
            "status":   "failed",
            "error":    f"Monitoring timeout after {timeout_seconds}s",
        })
    except Exception as exc:
        logger.error("Job monitoring failed: %s", exc)
        return json.dumps({"error": str(exc), "status": "failed"})


@tool
def submit_emr_job(job_config_json: str, resource_allocation_json: str = "{}") -> str:
    """
    Create an EMR cluster and submit a Spark step.

    Args:
        job_config_json:          JSON with script_location and optional cluster settings.
        resource_allocation_json: JSON from Resource Allocator.

    Returns:
        JSON with cluster_id, step_id, and status.
    """
    try:
        config    = json.loads(job_config_json)
        resources = json.loads(resource_allocation_json) if resource_allocation_json else {}
        emr       = boto3.client("emr")

        cluster_name = f"strands-etl-{uuid.uuid4().hex[:8]}"
        instances    = max(3, resources.get("optimal_config", {}).get("workers", 5) // 2)

        resp = emr.run_job_flow(
            Name=cluster_name,
            ReleaseLabel="emr-6.15.0",
            Instances={
                "MasterInstanceType": "m5.xlarge",
                "SlaveInstanceType":  "m5.2xlarge",
                "InstanceCount":      instances,
            },
            Steps=[{
                "Name":              "ETL Step",
                "ActionOnFailure":   "TERMINATE_CLUSTER",
                "HadoopJarStep": {
                    "Jar":  "command-runner.jar",
                    "Args": ["spark-submit", "--master", "yarn", config["script_location"]],
                },
            }],
            ServiceRole= "EMR_DefaultRole",
            JobFlowRole="EMR_EC2_DefaultRole",
            Applications=[{"Name": "Spark"}],
        )

        return json.dumps({
            "platform":     "emr",
            "cluster_name": cluster_name,
            "cluster_id":   resp["JobFlowId"],
            "status":       "running",
            "submitted_at": __import__("datetime").datetime.utcnow().isoformat(),
        })
    except Exception as exc:
        logger.error("EMR submission failed: %s", exc)
        return json.dumps({"error": str(exc), "status": "failed"})


def create_execution_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                            region: str = "us-west-2") -> Agent:
    """Return a Strands Agent for job execution."""
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[submit_glue_job, monitor_glue_job, submit_emr_job],
    )
