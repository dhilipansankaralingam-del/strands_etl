"""
Strands SDK ETL Framework
=========================

Enterprise ETL framework using Strands SDK for LLM-driven multi-agent orchestration.

This framework uses:
- strands.Agent: LLM-powered agents with system prompts
- strands.tool: Decorator to define tools agents can use
- strands.swarm.Swarm: Multi-agent collaboration and handoffs

Example:
    from strands_sdk import ETLSwarm

    swarm = ETLSwarm(config_path="demo_configs/enterprise_sales_config.json")
    result = swarm.run()
"""

from .config import ETLConfig
from .swarm import ETLSwarm
from .tools import (
    get_table_size,
    list_glue_tables,
    start_glue_job,
    get_glue_job_status,
    get_cloudwatch_metrics,
    start_emr_step,
    submit_eks_job,
    write_audit_log,
    store_recommendations
)

__all__ = [
    'ETLConfig',
    'ETLSwarm',
    'get_table_size',
    'list_glue_tables',
    'start_glue_job',
    'get_glue_job_status',
    'get_cloudwatch_metrics',
    'start_emr_step',
    'submit_eks_job',
    'write_audit_log',
    'store_recommendations'
]
