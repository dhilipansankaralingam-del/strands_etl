#!/usr/bin/env python3
"""
Run Audit Trail & HTML Summary Generator
==========================================

Produces a SINGLE consolidated audit file per job run that captures
every agent step, resource decisions, DQ results, Glue metrics,
compliance findings, learning predictions, and cost breakdown.

Output locations:
  Local:  data/run_audit/<job_name>/<execution_id>.jsonl
  S3:     s3://<bucket>/run-audit/<job_name>/<execution_id>.jsonl
  HTML:   data/run_audit/<job_name>/<execution_id>.html

The JSONL file is Athena-friendly: one JSON object per line, with a
consistent schema so you can CREATE EXTERNAL TABLE on top of it.

Usage in orchestrator:
    tracker = RunAuditTracker(config, job_name, execution_id)
    tracker.log_job_start(...)
    # ... agents run ...
    tracker.log_agent_result(agent_name, result, context)
    # ... at the end ...
    tracker.log_job_complete(orchestrator_result)
    tracker.generate_html()
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional

logger = logging.getLogger("strands.run_audit")


class RunAuditTracker:
    """
    Per-run audit tracker.  One instance per orchestrator.execute() call.

    Collects events into an ordered list, writes JSONL + optional S3,
    and generates an HTML job run summary.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        job_name: str,
        execution_id: str,
        run_date: datetime = None,
    ):
        self.config = config
        self.job_name = job_name
        self.execution_id = execution_id
        self.run_date = run_date or datetime.now(timezone.utc)

        # Ordered event list for this run
        self.events: List[Dict[str, Any]] = []

        # Local output directory
        base_dir = Path(config.get('storage', {}).get('local_base_path', 'data'))
        safe_job = "".join(c if c.isalnum() or c in "._-" else "_" for c in job_name)
        self.run_dir = base_dir / "run_audit" / safe_job
        self.run_dir.mkdir(parents=True, exist_ok=True)

        # File paths
        self.jsonl_path = self.run_dir / f"{execution_id}.jsonl"
        self.html_path = self.run_dir / f"{execution_id}.html"

        # S3
        self.s3_bucket = config.get('storage', {}).get('s3_bucket', '')
        self.s3_prefix = config.get('storage', {}).get('s3_prefix', 'run-audit/')
        self._s3 = None

    # ------------------------------------------------------------------
    # Event logging
    # ------------------------------------------------------------------

    def _emit(self, event: Dict[str, Any]) -> None:
        """Append event and flush to JSONL immediately (append mode)."""
        event.setdefault('event_id', str(uuid.uuid4())[:8])
        event.setdefault('timestamp', datetime.now(timezone.utc).isoformat())
        event['job_name'] = self.job_name
        event['execution_id'] = self.execution_id
        event['run_date'] = self.run_date.strftime('%Y-%m-%d')
        self.events.append(event)

        # Append to JSONL
        try:
            with open(self.jsonl_path, 'a') as f:
                f.write(json.dumps(event, default=str) + '\n')
        except Exception as e:
            logger.warning(f"Could not write audit event: {e}")

    def log_job_start(
        self,
        platform: str,
        agents: List[str],
        phases: List[List[str]],
    ) -> None:
        self._emit({
            'event_type': 'job_start',
            'status': 'started',
            'platform': platform,
            'agents_registered': agents,
            'execution_phases': phases,
            'config_snapshot': {
                k: v for k, v in self.config.items()
                if k not in ('aws_access_key', 'aws_secret_key', 'password')
            },
        })

    def log_phase_start(self, phase_num: int, agents: List[str]) -> None:
        self._emit({
            'event_type': 'phase_start',
            'phase_number': phase_num,
            'agents': agents,
        })

    def log_phase_complete(self, phase_num: int, agents: List[str], duration_ms: float) -> None:
        self._emit({
            'event_type': 'phase_complete',
            'phase_number': phase_num,
            'agents': agents,
            'duration_ms': round(duration_ms, 1),
        })

    def log_agent_result(
        self,
        agent_name: str,
        result: Any,
        context: Any = None,
    ) -> None:
        """Log full agent result with all output, metrics, recommendations."""
        result_dict = result.to_dict() if hasattr(result, 'to_dict') else {}

        event = {
            'event_type': 'agent_result',
            'agent_name': agent_name,
            'agent_id': result_dict.get('agent_id', ''),
            'status': result_dict.get('status', 'unknown'),
            'execution_time_ms': result_dict.get('execution_time_ms', 0),
            'output': result_dict.get('output', {}),
            'metrics': result_dict.get('metrics', {}),
            'recommendations': result_dict.get('recommendations', []),
            'errors': result_dict.get('errors', []),
            'warnings': result_dict.get('warnings', []),
        }

        # Enrich with shared state from specific agents
        if context and hasattr(context, 'get_shared'):
            if agent_name == 'sizing_agent':
                event['sizing'] = {
                    'total_size_gb': context.get_shared('total_size_gb'),
                    'source_record_counts': context.get_shared('source_record_counts'),
                }
            elif agent_name == 'resource_allocator_agent':
                event['resource_allocation'] = {
                    'recommended_workers': context.get_shared('recommended_workers'),
                    'recommended_worker_type': context.get_shared('recommended_worker_type'),
                }
            elif agent_name == 'execution_agent':
                event['job_metrics'] = context.get_shared('job_metrics', {})
                event['job_execution'] = context.get_shared('job_execution', {})
            elif agent_name == 'data_quality_agent':
                for phase in ('pre_load', 'post_load'):
                    summary = context.get_shared(f'dq_summary_{phase}')
                    if summary:
                        event[f'dq_summary_{phase}'] = summary
            elif agent_name == 'learning_agent':
                event['learning'] = {
                    'predictions': context.get_shared('learning_predictions'),
                    'model_info': context.get_shared('learning_model_info'),
                }

        self._emit(event)

    def log_job_complete(
        self,
        orchestrator_result: Any,
        total_cost_usd: float = 0.0,
        potential_savings_usd: float = 0.0,
    ) -> None:
        """Log the final job-complete event with full summary."""
        orch_dict = orchestrator_result.to_dict() if hasattr(orchestrator_result, 'to_dict') else {}

        # Gather LLM token usage from config / context if available
        llm_stats = self.config.get('_llm_stats', {})

        self._emit({
            'event_type': 'job_complete',
            'status': orch_dict.get('status', 'unknown'),
            'total_agents': orch_dict.get('total_agents', 0),
            'completed_agents': orch_dict.get('completed_agents', 0),
            'failed_agents': orch_dict.get('failed_agents', 0),
            'skipped_agents': orch_dict.get('skipped_agents', 0),
            'total_time_ms': orch_dict.get('total_time_ms', 0),
            'total_cost_usd': total_cost_usd,
            'potential_savings_usd': potential_savings_usd,
            'llm_tokens_input': llm_stats.get('input_tokens', 0),
            'llm_tokens_output': llm_stats.get('output_tokens', 0),
            'llm_cost_usd': llm_stats.get('cost_usd', 0.0),
            'recommendations': orch_dict.get('recommendations', []),
            'execution_timeline': orch_dict.get('execution_timeline', []),
        })

        # Also emit a consolidated run_summary event for easy querying
        self._emit_run_summary(orch_dict, total_cost_usd, potential_savings_usd, llm_stats)

    def _emit_run_summary(
        self,
        orch_dict: Dict[str, Any],
        total_cost_usd: float,
        potential_savings_usd: float,
        llm_stats: Dict[str, Any],
    ) -> None:
        """
        Emit a single 'run_summary' event with all KPIs, resource allocation,
        execution metrics, DQ summary, and compliance findings.

        This event is designed for easy Athena/table queries - all key metrics
        in one flat/nested structure.
        """
        # Extract data from agent_result events
        sizing = {}
        resource_allocation = {}
        execution_metrics = {}
        dq_summary = {'pre_load': {}, 'post_load': {}}
        compliance_summary = {}
        learning_summary = {}
        all_recommendations = []

        for e in self.events:
            if e.get('event_type') != 'agent_result':
                continue
            agent = e.get('agent_name', '')
            output = e.get('output', {})
            metrics = e.get('metrics', {})

            # Collect recommendations from all agents
            for rec in e.get('recommendations', []):
                all_recommendations.append({
                    'agent': agent,
                    'recommendation': rec,
                })

            if agent == 'sizing_agent':
                sizing = {
                    'total_size_gb': e.get('sizing', {}).get('total_size_gb') or output.get('total_size_gb', 0),
                    'source_table_count': output.get('source_count', 0),
                    'source_record_counts': e.get('sizing', {}).get('source_record_counts', {}),
                    'detection_method': output.get('detection_method', ''),
                }

            elif agent == 'resource_allocator_agent':
                resource_allocation = {
                    'input_size_gb': output.get('input_size_gb', 0),
                    'day_type': output.get('day_type', ''),
                    'scale_factor': output.get('scale_factor', 1.0),
                    'complexity_factor': output.get('complexity_factor', 1.0),
                    'formula_workers': output.get('formula_workers', 0),
                    'learned_workers': output.get('learned_workers'),
                    'learn_confidence': output.get('learn_confidence', 0),
                    'learn_model_id': output.get('learn_model_id', ''),
                    'recommended_workers': output.get('recommended_workers', 0),
                    'recommended_worker_type': output.get('recommended_worker_type', ''),
                    'config_workers': output.get('config_workers', 0),
                    'config_worker_type': output.get('config_type', ''),
                    'force_from_config': output.get('force_from_config', False),
                    'total_memory_gb': output.get('total_memory_gb', 0),
                    'estimated_cost_per_hour': output.get('estimated_cost_per_hour', 0),
                }

            elif agent == 'execution_agent':
                job_exec = e.get('job_execution', {}) or output
                job_met = e.get('job_metrics', {}) or metrics
                execution_metrics = {
                    'platform': job_exec.get('platform', ''),
                    'status': job_exec.get('status', ''),
                    'duration_seconds': job_exec.get('duration_seconds', 0),
                    'workers': job_exec.get('workers', 0),
                    'worker_type': job_exec.get('worker_type', ''),
                    'records_read': job_exec.get('records_read', 0),
                    'records_written': job_exec.get('records_written', 0),
                    'bytes_read': job_exec.get('bytes_read', 0),
                    'bytes_written': job_exec.get('bytes_written', 0),
                    'shuffle_bytes': job_exec.get('shuffle_bytes', 0),
                    'skewness_ratio': job_exec.get('skewness_ratio', 0),
                    'etl_throughput_mbps': job_exec.get('etl_throughput_mbps', 0),
                    'avg_executor_memory_pct': job_exec.get('avg_executor_memory_pct', 0),
                    'max_executor_memory_pct': job_exec.get('max_executor_memory_pct', 0),
                    'driver_memory_pct': job_exec.get('driver_memory_pct', 0),
                    'completed_stages': job_exec.get('completed_stages', 0),
                    'failed_stages': job_exec.get('failed_stages', 0),
                    'cost_usd': job_exec.get('cost_usd', 0),
                    'error_message': job_exec.get('error_message', ''),
                }

            elif agent == 'data_quality_agent':
                for phase in ('pre_load', 'post_load'):
                    summary = e.get(f'dq_summary_{phase}', {})
                    if summary:
                        dq_summary[phase] = {
                            'total_rules': summary.get('total', 0),
                            'passed': summary.get('passed', 0),
                            'failed': summary.get('failed', 0),
                            'warnings': summary.get('warnings', 0),
                            'score': summary.get('score', 0),
                            'records_scanned': summary.get('records_scanned', 0),
                            'outliers_found': summary.get('outliers_found', 0),
                        }
                # Also capture from output if not in enriched data
                if not dq_summary['pre_load'] and not dq_summary['post_load']:
                    phase = output.get('phase', 'pre_load')
                    dq_summary[phase] = {
                        'total_rules': output.get('total_rules', 0),
                        'passed': output.get('passed', 0),
                        'failed': output.get('failed', 0),
                        'warnings': output.get('warnings', 0),
                        'score': output.get('score', 0),
                        'records_scanned': output.get('records_scanned', 0),
                        'outliers_found': output.get('outliers_found', 0),
                    }

            elif agent == 'compliance_agent':
                compliance_summary = {
                    'tables_scanned': output.get('tables_scanned', []),
                    'total_pii_columns': output.get('total_pii_columns', 0),
                    'frameworks_checked': output.get('frameworks_checked', []),
                    'violations': output.get('violations', []),
                    'compliance_score': output.get('compliance_score', 0),
                }

            elif agent == 'learning_agent':
                learning_summary = {
                    'models_trained': output.get('models_trained', 0),
                    'training_records': output.get('training_records', 0),
                    'total_training_cost_usd': output.get('total_training_cost', 0),
                    'total_training_time_seconds': output.get('total_training_time_seconds', 0),
                    'predictions': e.get('learning', {}).get('predictions', {}),
                }

        # Build the consolidated run_summary event
        self._emit({
            'event_type': 'run_summary',

            # ---- KPIs ----
            'kpi': {
                'overall_status': orch_dict.get('status', 'unknown'),
                'total_duration_ms': orch_dict.get('total_time_ms', 0),
                'total_agents': orch_dict.get('total_agents', 0),
                'completed_agents': orch_dict.get('completed_agents', 0),
                'failed_agents': orch_dict.get('failed_agents', 0),
                'skipped_agents': orch_dict.get('skipped_agents', 0),
                'execution_cost_usd': total_cost_usd,
                'potential_savings_usd': potential_savings_usd,
                'llm_tokens_input': llm_stats.get('input_tokens', 0),
                'llm_tokens_output': llm_stats.get('output_tokens', 0),
                'llm_cost_usd': llm_stats.get('cost_usd', 0.0),
            },

            # ---- Sizing ----
            'sizing': sizing,

            # ---- Resource Allocation ----
            'resource_allocation': resource_allocation,

            # ---- Execution Metrics ----
            'execution': execution_metrics,

            # ---- Data Quality ----
            'data_quality': dq_summary,

            # ---- Compliance ----
            'compliance': compliance_summary,

            # ---- Learning ----
            'learning': learning_summary,

            # ---- All Recommendations ----
            'recommendations': all_recommendations,
        })

    # ------------------------------------------------------------------
    # S3 upload
    # ------------------------------------------------------------------

    def upload_to_s3(self) -> Optional[str]:
        """Upload JSONL and HTML to S3. Returns S3 URI or None."""
        if not self.s3_bucket:
            return None
        try:
            import boto3
            s3 = boto3.client('s3')

            safe_job = "".join(
                c if c.isalnum() or c in "._-/" else "_" for c in self.job_name
            )
            date_prefix = self.run_date.strftime('%Y/%m/%d')
            base_key = f"{self.s3_prefix}{safe_job}/{date_prefix}/{self.execution_id}"

            # Upload JSONL
            if self.jsonl_path.exists():
                s3.upload_file(
                    str(self.jsonl_path),
                    self.s3_bucket,
                    f"{base_key}.jsonl",
                )
                logger.info(f"Uploaded audit JSONL to s3://{self.s3_bucket}/{base_key}.jsonl")

            # Upload HTML
            if self.html_path.exists():
                s3.upload_file(
                    str(self.html_path),
                    self.s3_bucket,
                    f"{base_key}.html",
                    ExtraArgs={'ContentType': 'text/html'},
                )
                logger.info(f"Uploaded audit HTML to s3://{self.s3_bucket}/{base_key}.html")

            return f"s3://{self.s3_bucket}/{base_key}"

        except Exception as e:
            logger.warning(f"S3 upload failed: {e}")
            return None

    # ------------------------------------------------------------------
    # HTML Summary Generator
    # ------------------------------------------------------------------

    def generate_html(self) -> str:
        """Generate a beautiful HTML job run summary and write to disk."""
        # Gather data from events
        job_start = self._find_event('job_start')
        job_complete = self._find_event('job_complete')
        agent_results = [e for e in self.events if e.get('event_type') == 'agent_result']

        # Extract key metrics
        exec_event = self._find_agent('execution_agent')
        sizing_event = self._find_agent('sizing_agent')
        resource_event = self._find_agent('resource_allocator_agent')
        dq_event = self._find_agent('data_quality_agent')
        compliance_event = self._find_agent('compliance_agent')
        learning_event = self._find_agent('learning_agent')

        job_metrics = (exec_event or {}).get('job_metrics', {})
        job_execution = (exec_event or {}).get('job_execution', {})
        resource_output = (resource_event or {}).get('output', {})
        dq_pre = (dq_event or {}).get('dq_summary_pre_load', {})
        dq_post = (dq_event or {}).get('dq_summary_post_load', {})
        sizing_data = (sizing_event or {}).get('sizing', {})
        compliance_output = (compliance_event or {}).get('output', {})
        learning_output = (learning_event or {}).get('output', {})
        learning_data = (learning_event or {}).get('learning', {})

        overall_status = (job_complete or {}).get('status', 'unknown')
        total_time_ms = (job_complete or {}).get('total_time_ms', 0)
        total_cost = (job_complete or {}).get('total_cost_usd', 0)
        potential_savings = (job_complete or {}).get('potential_savings_usd', 0)
        llm_input_tokens = (job_complete or {}).get('llm_tokens_input', 0)
        llm_output_tokens = (job_complete or {}).get('llm_tokens_output', 0)
        llm_cost = (job_complete or {}).get('llm_cost_usd', 0)
        timeline = (job_complete or {}).get('execution_timeline', [])
        recommendations = []
        for ar in agent_results:
            for rec in ar.get('recommendations', []):
                recommendations.append({
                    'agent': ar.get('agent_name', ''),
                    'text': rec,
                })

        status_color = {
            'completed': '#22c55e',
            'partial_failure': '#f59e0b',
            'failed': '#ef4444',
        }.get(overall_status, '#6b7280')

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Job Run Summary — {self._esc(self.job_name)} — {self.execution_id}</title>
<style>
  :root {{
    --bg: #0f172a; --card: #1e293b; --border: #334155;
    --text: #e2e8f0; --muted: #94a3b8; --accent: #38bdf8;
    --green: #22c55e; --red: #ef4444; --amber: #f59e0b;
    --font: 'Segoe UI', system-ui, -apple-system, sans-serif;
    --mono: 'Cascadia Code', 'Fira Code', 'Consolas', monospace;
  }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family:var(--font); background:var(--bg); color:var(--text); padding:24px; }}
  .container {{ max-width:1280px; margin:0 auto; }}
  h1 {{ font-size:1.8rem; margin-bottom:4px; }}
  h2 {{ font-size:1.2rem; color:var(--accent); margin:24px 0 12px; border-bottom:1px solid var(--border); padding-bottom:6px; }}
  h3 {{ font-size:1rem; color:var(--muted); margin:12px 0 6px; }}
  .header {{ display:flex; justify-content:space-between; align-items:flex-start; flex-wrap:wrap; gap:12px; margin-bottom:24px; }}
  .header-left h1 span {{ color:var(--muted); font-weight:400; font-size:1rem; margin-left:8px; }}
  .status-badge {{ display:inline-block; padding:6px 16px; border-radius:6px; font-weight:600; font-size:0.9rem; color:#fff; }}
  .kpi-grid {{ display:grid; grid-template-columns:repeat(auto-fit, minmax(200px, 1fr)); gap:12px; margin-bottom:24px; }}
  .kpi {{ background:var(--card); border:1px solid var(--border); border-radius:8px; padding:16px; }}
  .kpi .label {{ font-size:0.75rem; color:var(--muted); text-transform:uppercase; letter-spacing:0.05em; }}
  .kpi .value {{ font-size:1.6rem; font-weight:700; margin-top:4px; }}
  .kpi .sub   {{ font-size:0.8rem; color:var(--muted); margin-top:2px; }}
  .card {{ background:var(--card); border:1px solid var(--border); border-radius:8px; padding:16px; margin-bottom:16px; }}
  table {{ width:100%; border-collapse:collapse; font-size:0.85rem; }}
  th {{ text-align:left; padding:8px 10px; color:var(--muted); border-bottom:1px solid var(--border); font-weight:600; }}
  td {{ padding:8px 10px; border-bottom:1px solid var(--border); vertical-align:top; }}
  tr:last-child td {{ border-bottom:none; }}
  .pass {{ color:var(--green); font-weight:600; }}
  .fail {{ color:var(--red); font-weight:600; }}
  .warn {{ color:var(--amber); font-weight:600; }}
  .mono {{ font-family:var(--mono); font-size:0.82rem; }}
  .timeline {{ position:relative; padding-left:24px; }}
  .timeline::before {{ content:''; position:absolute; left:8px; top:0; bottom:0; width:2px; background:var(--border); }}
  .tl-item {{ position:relative; margin-bottom:8px; font-size:0.85rem; }}
  .tl-item::before {{ content:''; position:absolute; left:-20px; top:6px; width:10px; height:10px; border-radius:50%; background:var(--accent); }}
  .tl-item.fail::before {{ background:var(--red); }}
  .tl-item .tl-time {{ color:var(--muted); font-family:var(--mono); font-size:0.78rem; }}
  .rec-list {{ list-style:none; }}
  .rec-list li {{ padding:8px 12px; border-left:3px solid var(--accent); margin-bottom:6px; background:rgba(56,189,248,0.06); border-radius:0 4px 4px 0; font-size:0.85rem; }}
  .rec-list li .rec-agent {{ color:var(--muted); font-size:0.75rem; }}
  .two-col {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
  @media (max-width:768px) {{ .two-col {{ grid-template-columns:1fr; }} .kpi-grid {{ grid-template-columns:repeat(2,1fr); }} }}
  .footer {{ text-align:center; color:var(--muted); font-size:0.75rem; margin-top:32px; padding-top:16px; border-top:1px solid var(--border); }}
</style>
</head>
<body>
<div class="container">

<!-- Header -->
<div class="header">
  <div class="header-left">
    <h1>{self._esc(self.job_name)} <span>{self.execution_id}</span></h1>
    <div style="color:var(--muted);font-size:0.85rem;margin-top:4px;">
      Run Date: {self.run_date.strftime('%Y-%m-%d %H:%M:%S UTC')}
    </div>
  </div>
  <div>
    <span class="status-badge" style="background:{status_color}">
      {overall_status.upper().replace('_',' ')}
    </span>
  </div>
</div>

<!-- KPI Cards -->
<div class="kpi-grid">
  <div class="kpi">
    <div class="label">Total Duration</div>
    <div class="value">{self._fmt_duration(total_time_ms)}</div>
  </div>
  <div class="kpi">
    <div class="label">Execution Cost</div>
    <div class="value">${total_cost:.4f}</div>
    <div class="sub">Potential savings: ${potential_savings:.4f}</div>
  </div>
  <div class="kpi">
    <div class="label">Records Read</div>
    <div class="value">{self._fmt_number(job_metrics.get('records_read', job_execution.get('records_read', 0)))}</div>
    <div class="sub">Written: {self._fmt_number(job_metrics.get('records_written', job_execution.get('records_written', 0)))}</div>
  </div>
  <div class="kpi">
    <div class="label">Data Movement</div>
    <div class="value">{self._fmt_bytes(job_metrics.get('bytes_read', job_execution.get('bytes_read', 0)))}</div>
    <div class="sub">Throughput: {job_metrics.get('etl_throughput_mbps', 0):.1f} MB/s</div>
  </div>
  <div class="kpi">
    <div class="label">Workers</div>
    <div class="value">{resource_output.get('recommended_workers', job_execution.get('workers', '-'))}</div>
    <div class="sub">Type: {resource_output.get('recommended_worker_type', job_execution.get('worker_type', '-'))}</div>
  </div>
  <div class="kpi">
    <div class="label">LLM Tokens</div>
    <div class="value">{self._fmt_number(llm_input_tokens + llm_output_tokens)}</div>
    <div class="sub">Cost: ${llm_cost:.4f}</div>
  </div>
</div>

<!-- Resource Allocation -->
<h2>Resource Allocation</h2>
<div class="card">
  <table>
    <tr><th>Metric</th><th>Value</th></tr>
    <tr><td>Input Size</td><td>{resource_output.get('input_size_gb', sizing_data.get('total_size_gb', '-'))} GB</td></tr>
    <tr><td>Day Type</td><td>{resource_output.get('day_type', '-')}</td></tr>
    <tr><td>Complexity Factor</td><td>{resource_output.get('complexity_factor', '-')}x</td></tr>
    <tr><td>Scale Factor</td><td>{resource_output.get('scale_factor', '-')}x</td></tr>
    <tr><td>Formula Workers</td><td>{resource_output.get('formula_workers', '-')}</td></tr>
    <tr><td>Learning Model</td><td>{resource_output.get('learn_model_id', 'None')}</td></tr>
    <tr><td>Learned Workers</td><td>{resource_output.get('learned_workers', '-')}</td></tr>
    <tr><td>Learn Confidence</td><td>{self._fmt_pct(resource_output.get('learn_confidence'))}</td></tr>
    <tr><td>Final (Blended)</td><td><strong>{resource_output.get('recommended_workers', '-')}</strong> &times; {resource_output.get('recommended_worker_type', '-')}</td></tr>
    <tr><td>Force From Config</td><td>{'Yes' if resource_output.get('force_from_config') else 'No'}</td></tr>
  </table>
</div>

<!-- Glue Execution Metrics -->
<h2>Execution Metrics</h2>
<div class="card">
  <table>
    <tr><th>Metric</th><th>Value</th></tr>
    <tr><td>Platform</td><td>{job_execution.get('platform', '-')}</td></tr>
    <tr><td>Status</td><td class="{'pass' if job_execution.get('status')=='SUCCEEDED' else 'fail'}">{job_execution.get('status', '-')}</td></tr>
    <tr><td>Duration</td><td>{self._fmt_duration((job_execution.get('duration_seconds', 0) or 0) * 1000)}</td></tr>
    <tr><td>Records Read / Written</td><td>{self._fmt_number(job_execution.get('records_read', 0))} / {self._fmt_number(job_execution.get('records_written', 0))}</td></tr>
    <tr><td>Bytes Read / Written</td><td>{self._fmt_bytes(job_execution.get('bytes_read', 0))} / {self._fmt_bytes(job_execution.get('bytes_written', 0))}</td></tr>
    <tr><td>Shuffle Bytes</td><td>{self._fmt_bytes(job_execution.get('shuffle_bytes', 0))}</td></tr>
    <tr><td>Skewness Ratio</td><td class="{'warn' if (job_execution.get('skewness_ratio', 0) or 0) > 0.5 else ''}">{job_execution.get('skewness_ratio', 0):.4f}</td></tr>
    <tr><td>ETL Throughput</td><td>{job_execution.get('etl_throughput_mbps', 0):.1f} MB/s</td></tr>
    <tr><td>Avg Executor Memory</td><td>{job_execution.get('avg_executor_memory_pct', 0):.1f}%</td></tr>
    <tr><td>Max Executor Memory</td><td class="{'warn' if (job_execution.get('max_executor_memory_pct', 0) or 0) > 85 else ''}">{job_execution.get('max_executor_memory_pct', 0):.1f}%</td></tr>
    <tr><td>Driver Memory</td><td class="{'warn' if (job_execution.get('driver_memory_pct', 0) or 0) > 80 else ''}">{job_execution.get('driver_memory_pct', 0):.1f}%</td></tr>
    <tr><td>Completed Stages</td><td>{job_execution.get('completed_stages', 0)}</td></tr>
    <tr><td>Failed Stages</td><td class="{'fail' if (job_execution.get('failed_stages', 0) or 0) > 0 else ''}">{job_execution.get('failed_stages', 0)}</td></tr>
    <tr><td>Execution Cost</td><td>${job_execution.get('cost_usd', 0):.4f}</td></tr>
  </table>
</div>

<!-- Data Quality -->
<h2>Data Quality</h2>
<div class="two-col">
  {self._render_dq_card('Pre-Load (Source)', dq_pre, dq_event)}
  {self._render_dq_card('Post-Load (Target)', dq_post, dq_event)}
</div>
{self._render_dq_rules_table(dq_event)}

<!-- Compliance -->
<h2>Compliance</h2>
<div class="card">
{self._render_compliance(compliance_output)}
</div>

<!-- Learning & Predictions -->
<h2>Learning Agent</h2>
<div class="card">
{self._render_learning(learning_output, learning_data)}
</div>

<!-- Agent Timeline -->
<h2>Agent Execution Timeline</h2>
<div class="card">
{self._render_timeline(agent_results, timeline)}
</div>

<!-- Recommendations -->
<h2>Recommendations ({len(recommendations)})</h2>
<div class="card">
{self._render_recommendations(recommendations)}
</div>

<div class="footer">
  Strands ETL Framework &mdash; Run Audit &mdash; Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
</div>

</div>
</body>
</html>"""

        try:
            with open(self.html_path, 'w') as f:
                f.write(html)
            logger.info(f"HTML summary written to {self.html_path}")
        except Exception as e:
            logger.error(f"Failed to write HTML: {e}")

        return str(self.html_path)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_event(self, event_type: str) -> Optional[Dict]:
        for e in self.events:
            if e.get('event_type') == event_type:
                return e
        return None

    def _find_agent(self, agent_name: str) -> Optional[Dict]:
        for e in self.events:
            if e.get('event_type') == 'agent_result' and e.get('agent_name') == agent_name:
                return e
        return None

    @staticmethod
    def _esc(s: str) -> str:
        return (s or '').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')

    @staticmethod
    def _fmt_duration(ms) -> str:
        if not ms:
            return '-'
        ms = float(ms)
        if ms < 1000:
            return f"{ms:.0f}ms"
        secs = ms / 1000
        if secs < 60:
            return f"{secs:.1f}s"
        mins = secs / 60
        if mins < 60:
            return f"{int(mins)}m {int(secs % 60)}s"
        hours = mins / 60
        return f"{int(hours)}h {int(mins % 60)}m"

    @staticmethod
    def _fmt_number(n) -> str:
        if n is None:
            return '-'
        n = int(n)
        if n >= 1_000_000_000:
            return f"{n/1_000_000_000:.1f}B"
        if n >= 1_000_000:
            return f"{n/1_000_000:.1f}M"
        if n >= 1_000:
            return f"{n/1_000:.1f}K"
        return str(n)

    @staticmethod
    def _fmt_bytes(b) -> str:
        if not b:
            return '-'
        b = int(b)
        for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
            if b < 1024:
                return f"{b:.1f} {unit}"
            b /= 1024
        return f"{b:.1f} PB"

    @staticmethod
    def _fmt_pct(v) -> str:
        if v is None:
            return '-'
        return f"{float(v):.0%}"

    def _render_dq_card(self, title: str, summary: Dict, dq_event: Optional[Dict]) -> str:
        if not summary:
            return f'<div class="card"><h3>{title}</h3><p style="color:var(--muted)">No data</p></div>'
        total = summary.get('total', 0)
        passed = summary.get('passed', 0)
        failed = summary.get('failed', 0)
        warnings = summary.get('warnings', 0)
        score = summary.get('score', 0)
        scanned = summary.get('records_scanned', 0)
        outliers = summary.get('outliers_found', 0)
        score_class = 'pass' if score >= 90 else 'warn' if score >= 70 else 'fail'
        return f"""<div class="card">
  <h3>{title}</h3>
  <table>
    <tr><td>Score</td><td class="{score_class}">{score:.1f}%</td></tr>
    <tr><td>Rules Passed / Failed / Warnings</td><td>{passed} / {failed} / {warnings}</td></tr>
    <tr><td>Records Scanned</td><td>{self._fmt_number(scanned)}</td></tr>
    <tr><td>Outliers Found</td><td>{self._fmt_number(outliers)}</td></tr>
  </table>
</div>"""

    def _render_dq_rules_table(self, dq_event: Optional[Dict]) -> str:
        if not dq_event:
            return ''
        rules = (dq_event.get('output') or {}).get('rules', [])
        if not rules:
            return ''
        rows = ''
        for r in rules:
            pf = r.get('pass_fail_status', '')
            pf_class = 'pass' if pf == 'PASS' else 'fail' if pf in ('FAIL', 'ERROR') else 'warn'
            rows += f"""<tr>
  <td>{self._esc(str(r.get('table', '')))}</td>
  <td>{self._esc(str(r.get('column', '-')))}</td>
  <td>{self._esc(str(r.get('rule_type', '')))}</td>
  <td>{r.get('severity', '-')}</td>
  <td>{self._fmt_number(r.get('records_scanned', 0))}</td>
  <td>{self._fmt_number(r.get('outliers_found', 0))}</td>
  <td>{r.get('threshold', '-')}</td>
  <td>{r.get('actual_value', '-')}</td>
  <td class="{pf_class}">{pf}</td>
</tr>"""
        return f"""<div class="card" style="overflow-x:auto;">
<table>
  <tr><th>Table</th><th>Column</th><th>Rule</th><th>Severity</th><th>Scanned</th><th>Outliers</th><th>Threshold</th><th>Actual</th><th>Result</th></tr>
  {rows}
</table>
</div>"""

    def _render_compliance(self, output: Dict) -> str:
        if not output or output.get('skipped'):
            return '<p style="color:var(--muted)">Compliance agent was skipped or no data</p>'
        tables = output.get('tables_scanned', [])
        pii_total = output.get('total_pii_columns', 0)
        frameworks = output.get('frameworks_checked', [])
        violations = output.get('violations', [])
        rows = ''
        for t in (output.get('table_results', []) if isinstance(output.get('table_results'), list) else []):
            rows += f"<tr><td>{self._esc(str(t.get('table','')))}</td><td>{t.get('pii_columns',0)}</td><td>{', '.join(t.get('frameworks',[]))}</td></tr>"
        if not rows:
            rows = f"<tr><td colspan='3'>Tables scanned: {len(tables) if isinstance(tables, list) else tables}, PII columns: {pii_total}, Frameworks: {', '.join(frameworks) if isinstance(frameworks, list) else frameworks}</td></tr>"
        return f"""<table>
  <tr><th>Table</th><th>PII Columns</th><th>Frameworks</th></tr>
  {rows}
</table>
{'<p class="fail" style="margin-top:8px;">Violations: ' + ', '.join(str(v) for v in violations) + '</p>' if violations else ''}"""

    def _render_learning(self, output: Dict, learning_data: Dict) -> str:
        if not output or output.get('skipped'):
            return '<p style="color:var(--muted)">Learning agent was skipped or no data</p>'
        rows = ''
        for key in ('models_trained', 'training_records', 'total_training_cost', 'total_training_time_seconds'):
            val = output.get(key, '-')
            if isinstance(val, float):
                val = f"{val:.4f}" if 'cost' in key else f"{val:.2f}"
            rows += f"<tr><td>{key.replace('_', ' ').title()}</td><td>{val}</td></tr>"
        # Predictions
        predictions = (learning_data or {}).get('predictions') or output.get('predictions', {})
        if predictions and isinstance(predictions, dict):
            for pred_type, pred_val in predictions.items():
                rows += f"<tr><td>Prediction: {self._esc(pred_type)}</td><td>{pred_val}</td></tr>"
        return f"<table><tr><th>Metric</th><th>Value</th></tr>{rows}</table>"

    def _render_timeline(self, agent_results: List[Dict], timeline: List[Dict]) -> str:
        items = ''
        # Use agent_results for richer data, fall back to timeline
        for ar in agent_results:
            name = ar.get('agent_name', '')
            status = ar.get('status', '')
            ms = ar.get('execution_time_ms', 0)
            css = 'fail' if status == 'failed' else ''
            items += f"""<div class="tl-item {css}">
  <strong>{self._esc(name)}</strong>
  <span class="{'pass' if status=='completed' else 'fail' if status=='failed' else 'warn'}"> {status}</span>
  <span class="tl-time"> {self._fmt_duration(ms)}</span>
</div>"""
        if not items:
            for t in timeline:
                items += f"""<div class="tl-item">
  <span class="tl-time">{self._esc(str(t.get('timestamp','')))}</span>
  <strong>{self._esc(str(t.get('agent','')))}</strong> — {t.get('event','')}
</div>"""
        return f'<div class="timeline">{items}</div>' if items else '<p style="color:var(--muted)">No timeline data</p>'

    def _render_recommendations(self, recs: List[Dict]) -> str:
        if not recs:
            return '<p style="color:var(--muted)">No recommendations</p>'
        items = ''
        for r in recs:
            items += f"""<li>
  {self._esc(r.get('text', ''))}
  <div class="rec-agent">{self._esc(r.get('agent', ''))}</div>
</li>"""
        return f'<ul class="rec-list">{items}</ul>'
