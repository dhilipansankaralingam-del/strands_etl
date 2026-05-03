"""
Multi-Agent ETL Orchestrator
=============================

True agentic ETL framework powered by Strands SDK with Agent-to-Agent (A2A)
interaction via the agent_as_tool pattern.

Architecture  (15 specialist agents, 40+ tools):

  OrchestratorAgent
    │
    ├── Phase 1 ─ PARALLEL (6 agents)
    │     ├── SizingAgent           → data volume, skew risk, partition efficiency (3 tools)
    │     ├── DataQualityAgent      → DQ rule evaluation, schema checks (2 tools)
    │     ├── ComplianceAgent       → PII detection, GDPR/HIPAA/PCI/SOX/CCPA (2 tools)
    │     ├── CodeAnalyzerAgent     → line-by-line anti-pattern + AQE config (2 tools)
    │     ├── ColumnLineageAgent    → column-level data flow, Mermaid/DOT (3 tools)
    │     └── DeltaIcebergAgent     → format detection, maintenance SQL, partition (3 tools)
    │
    ├── Phase 2 ─ SEQUENTIAL (1 agent, consumes Phase 1)
    │     └── ResourceAllocatorAgent → right-sizing, cost comparison, Spot/Flex (3 tools)
    │
    ├── Phase 3 ─ PARALLEL (2 agents, generate artefacts)
    │     ├── RecommendationApplierAgent → inject AQE, fix anti-patterns, diff (4 tools)
    │     └── JobGeneratorAgent          → generate PySpark job, Glue job def (3 tools)
    │
    ├── Phase 4 ─ SEQUENTIAL (1 agent)
    │     └── ExecutionAgent   → submit & monitor Glue / EMR job (3 tools)
    │
    └── Phase 5 ─ PARALLEL (5 agents, post-execution analytics)
          ├── GlueMetricsAgent      → CloudWatch metric fetch & analysis (3 tools)
          ├── SparkEventLogAgent    → event log parse, skew, bottlenecks (3 tools)
          ├── ScriptTesterAgent     → test generation, local run, validation (4 tools)
          ├── RecommendationAgent   → ROI roadmap, impl plan, prioritisation (3 tools)
          └── LearningAgent         → persist + retrieve learning vectors (2 tools)
"""

import json
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from strands import Agent
from strands.models import BedrockModel
from strands.tools import agent_as_tool

from .agents import (
    create_sizing_agent,
    create_data_quality_agent,
    create_compliance_agent,
    create_code_analyzer_agent,
    create_column_lineage_agent,
    create_delta_iceberg_agent,
    create_resource_allocator_agent,
    create_recommendation_applier_agent,
    create_job_generator_agent,
    create_execution_agent,
    create_glue_metrics_agent,
    create_spark_event_log_agent,
    create_script_tester_agent,
    create_recommendation_agent,
    create_learning_agent,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

_ORCHESTRATOR_SYSTEM_PROMPT = """
You are a **Master ETL Orchestrator Agent** coordinating 15 specialist sub-agents in a 5-phase pipeline.

Your responsibilities:
1. Delegate analysis tasks to specialist agents via the provided tools
2. Synthesise results across agents to identify the highest-impact actions
3. Ensure Phase 1 parallel agents complete before Phase 2 proceeds
4. Pass enriched context downstream so each phase builds on the previous
5. Produce a coherent final report with business impact scores and ROI estimates

Always call agents in the mandated phase order. Never skip a phase.
Return structured JSON when asked for a final report.
"""


class _DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def _dumps(obj: Any) -> str:
    return json.dumps(obj, cls=_DateEncoder)


def _safe_json(text: Any) -> Any:
    """Extract the first JSON object/array from a string or return raw."""
    if isinstance(text, (dict, list)):
        return text
    s = str(text)
    for start, end in [(s.find("{"), "}"), (s.find("["), "]")]:
        if start != -1:
            close = s.rfind(end)
            if close > start:
                try:
                    return json.loads(s[start:close + 1])
                except Exception:
                    pass
    return {"raw": s}


class MultiAgentOrchestrator:
    """
    5-phase ETL orchestration framework using Strands agent_as_tool pattern.

    Usage:
        orch = MultiAgentOrchestrator()
        result = orch.run_pipeline(config)
    """

    def __init__(
        self,
        model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
        region: str = "us-west-2",
        max_workers: int = 6,
    ):
        self.model_id    = model_id
        self.region      = region
        self.max_workers = max_workers

        logger.info("Initialising 15 specialist agents …")

        # ── Phase 1 ────────────────────────────────────────────────────────────
        self._sizing_agent             = create_sizing_agent(model_id, region)
        self._dq_agent                 = create_data_quality_agent(model_id, region)
        self._compliance_agent         = create_compliance_agent(model_id, region)
        self._code_analyzer_agent      = create_code_analyzer_agent(model_id, region)
        self._column_lineage_agent     = create_column_lineage_agent(model_id, region)
        self._delta_iceberg_agent      = create_delta_iceberg_agent(model_id, region)

        # ── Phase 2 ────────────────────────────────────────────────────────────
        self._resource_allocator_agent = create_resource_allocator_agent(model_id, region)

        # ── Phase 3 ────────────────────────────────────────────────────────────
        self._rec_applier_agent        = create_recommendation_applier_agent(model_id, region)
        self._job_generator_agent      = create_job_generator_agent(model_id, region)

        # ── Phase 4 ────────────────────────────────────────────────────────────
        self._execution_agent          = create_execution_agent(model_id, region)

        # ── Phase 5 ────────────────────────────────────────────────────────────
        self._glue_metrics_agent       = create_glue_metrics_agent(model_id, region)
        self._spark_event_log_agent    = create_spark_event_log_agent(model_id, region)
        self._script_tester_agent      = create_script_tester_agent(model_id, region)
        self._recommendation_agent     = create_recommendation_agent(model_id, region)
        self._learning_agent           = create_learning_agent(model_id, region)

        # ── Wrap every specialist as a tool for the OrchestratorAgent ──────────
        self._tools = [
            agent_as_tool(self._sizing_agent,             name="sizing_agent",
                          description="Analyse data volumes, skew risk, partition efficiency"),
            agent_as_tool(self._dq_agent,                 name="data_quality_agent",
                          description="Run data quality checks and generate DQ rules"),
            agent_as_tool(self._compliance_agent,         name="compliance_agent",
                          description="Detect PII columns and generate masking code"),
            agent_as_tool(self._code_analyzer_agent,      name="code_analyzer_agent",
                          description="Line-by-line PySpark anti-pattern analysis"),
            agent_as_tool(self._column_lineage_agent,     name="column_lineage_agent",
                          description="Trace column-level data lineage and render diagrams"),
            agent_as_tool(self._delta_iceberg_agent,      name="delta_iceberg_agent",
                          description="Detect Delta/Iceberg format and emit maintenance SQL"),
            agent_as_tool(self._resource_allocator_agent, name="resource_allocator_agent",
                          description="Right-size Glue workers and compare cloud costs"),
            agent_as_tool(self._rec_applier_agent,        name="recommendation_applier_agent",
                          description="Apply Spark configs and fix anti-patterns in scripts"),
            agent_as_tool(self._job_generator_agent,      name="job_generator_agent",
                          description="Generate production-ready PySpark/Glue ETL scripts"),
            agent_as_tool(self._execution_agent,          name="execution_agent",
                          description="Submit and monitor AWS Glue or EMR jobs"),
            agent_as_tool(self._glue_metrics_agent,       name="glue_metrics_agent",
                          description="Fetch and analyse Glue CloudWatch metrics"),
            agent_as_tool(self._spark_event_log_agent,    name="spark_event_log_agent",
                          description="Parse Spark event logs for bottlenecks and skew"),
            agent_as_tool(self._script_tester_agent,      name="script_tester_agent",
                          description="Generate and run pytest test suites for PySpark scripts"),
            agent_as_tool(self._recommendation_agent,     name="recommendation_agent",
                          description="Synthesise ROI-driven recommendations and implementation plan"),
            agent_as_tool(self._learning_agent,           name="learning_agent",
                          description="Persist pipeline learning vectors and retrieve history"),
        ]

        self._orchestrator = Agent(
            model=BedrockModel(model_id=model_id, region_name=region),
            system_prompt=_ORCHESTRATOR_SYSTEM_PROMPT,
            tools=self._tools,
        )
        logger.info("OrchestratorAgent ready with %d specialist tools", len(self._tools))

    # ── Internal helpers ────────────────────────────────────────────────────────

    def _run_parallel(self, tasks: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a dict of {name: callable} in parallel, return {name: result}."""
        results: Dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(fn): name for name, fn in tasks.items()}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    results[name] = _safe_json(future.result())
                    logger.info("  ✓ %s completed", name)
                except Exception as exc:
                    logger.error("  ✗ %s failed: %s", name, exc)
                    results[name] = {"error": str(exc)}
        return results

    def _call_agent(self, agent: Agent, prompt: str) -> Any:
        """Invoke a specialist agent synchronously and parse its response."""
        try:
            response = agent(prompt)
            return _safe_json(str(response))
        except Exception as exc:
            logger.error("Agent call failed: %s", exc)
            return {"error": str(exc)}

    # ── Main pipeline ───────────────────────────────────────────────────────────

    def run_pipeline(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the full 5-phase multi-agent ETL pipeline.

        Args:
            config: Pipeline configuration dict with keys:
                - job_name (str)
                - source_tables (list)
                - table_schemas (list)
                - script_content (str, optional)
                - target_table (dict, optional)
                - job_config (dict, optional)
                - processing_mode ("full"|"delta", default "full")
                - compliance_frameworks (list, optional)
                - event_log_path (str, optional)
                - runs_per_day (int, default 1)

        Returns:
            Comprehensive pipeline result dict.
        """
        pipeline_id   = str(uuid.uuid4())[:8]
        started_at    = datetime.utcnow().isoformat()
        job_name      = config.get("job_name", "etl_job")
        tables        = config.get("source_tables", [])
        schemas       = config.get("table_schemas", tables)
        script        = config.get("script_content", "")
        target        = config.get("target_table", {})
        mode          = config.get("processing_mode", "full")
        frameworks    = config.get("compliance_frameworks", ["GDPR", "HIPAA", "PCI-DSS"])
        event_log     = config.get("event_log_path", "")
        runs_per_day  = config.get("runs_per_day", 1)
        join_count    = len([t for t in tables if t.get("join_key")]) or 1

        tables_json    = _dumps(tables)
        schemas_json   = _dumps(schemas)
        frameworks_json = _dumps(frameworks)

        logger.info("=" * 70)
        logger.info("Pipeline %s  |  job: %s  |  mode: %s", pipeline_id, job_name, mode)
        logger.info("=" * 70)

        # ── Phase 1: Parallel analysis (6 agents) ──────────────────────────────
        logger.info("[Phase 1] Running 6 analysis agents in parallel …")

        def _run_sizing():
            return self._call_agent(
                self._sizing_agent,
                f"Analyse data sizing for job '{job_name}' with tables: {tables_json}. "
                f"Processing mode: {mode}. Join count: {join_count}."
            )

        def _run_dq():
            return self._call_agent(
                self._dq_agent,
                f"Run data quality checks for schemas: {schemas_json}"
            )

        def _run_compliance():
            return self._call_agent(
                self._compliance_agent,
                f"Scan schemas for PII columns. Active frameworks: {frameworks_json}. "
                f"Schemas: {schemas_json}"
            )

        def _run_code_analysis():
            if not script:
                return {"skipped": True, "reason": "No script_content provided"}
            return self._call_agent(
                self._code_analyzer_agent,
                f"Analyse this PySpark script for anti-patterns and optimization opportunities:\n{script[:3000]}"
            )

        def _run_lineage():
            if not script:
                return {"skipped": True, "reason": "No script_content provided"}
            return self._call_agent(
                self._column_lineage_agent,
                f"Trace column lineage and generate a Mermaid diagram for:\n{script[:3000]}"
            )

        def _run_delta_iceberg():
            if not script:
                return {"skipped": True, "reason": "No script_content provided"}
            return self._call_agent(
                self._delta_iceberg_agent,
                f"Detect Delta/Iceberg format and emit maintenance commands for:\n{script[:3000]}"
            )

        phase1 = self._run_parallel({
            "sizing":        _run_sizing,
            "data_quality":  _run_dq,
            "compliance":    _run_compliance,
            "code_analysis": _run_code_analysis,
            "lineage":       _run_lineage,
            "delta_iceberg": _run_delta_iceberg,
        })

        sizing_result   = phase1.get("sizing", {})
        effective_gb    = sizing_result.get("effective_size_gb", 50)
        skew_score      = sizing_result.get("skew_risk_score", 0)
        logger.info("[Phase 1] Done — effective_gb=%.1f  skew=%d", effective_gb, skew_score)

        # ── Phase 2: Resource allocation ────────────────────────────────────────
        logger.info("[Phase 2] Resource allocation …")
        current_config = config.get("current_config", {
            "worker_type": "G.2X", "num_workers": 10
        })
        phase2_prompt = (
            f"Allocate optimal resources for job '{job_name}'. "
            f"Sizing result: {_dumps(sizing_result)}. "
            f"Code analysis: {_dumps(phase1.get('code_analysis', {}))}. "
            f"Current config: {_dumps(current_config)}. "
            f"Runs per day: {runs_per_day}."
        )
        allocation = self._call_agent(self._resource_allocator_agent, phase2_prompt)
        logger.info("[Phase 2] Done — recommended: %s",
                    allocation.get("optimal_config", {}).get("worker_type", "?"))

        # ── Phase 3: Script artefact generation (parallel) ─────────────────────
        logger.info("[Phase 3] Generating optimised script artefacts in parallel …")

        def _run_rec_applier():
            if not script:
                return {"skipped": True}
            return self._call_agent(
                self._rec_applier_agent,
                f"Apply all recommendations to this script. "
                f"Code analysis: {_dumps(phase1.get('code_analysis', {})[:500] if isinstance(phase1.get('code_analysis'), list) else phase1.get('code_analysis', {}))}. "
                f"Source tables: {tables_json}. Script:\n{script[:2000]}"
            )

        def _run_job_generator():
            job_spec = {
                "job_name":        job_name,
                "platform":        config.get("platform", "glue"),
                "processing_mode": mode,
                "source_tables":   tables,
                "target_table":    target,
                "validation_rules": config.get("validation_rules", []),
                "optimization_requirements": [
                    "enable AQE with skewJoin", "broadcast small tables",
                    "use dynamic partition overwrite",
                ],
            }
            return self._call_agent(
                self._job_generator_agent,
                f"Generate a production-ready PySpark job from this spec: {_dumps(job_spec)}"
            )

        phase3 = self._run_parallel({
            "recommendation_applier": _run_rec_applier,
            "job_generator":          _run_job_generator,
        })
        logger.info("[Phase 3] Done — applier: %s  generator: %s",
                    "ok" if "error" not in phase3.get("recommendation_applier", {}) else "err",
                    "ok" if "error" not in phase3.get("job_generator", {}) else "err")

        # ── Phase 4: Execution ──────────────────────────────────────────────────
        logger.info("[Phase 4] Submitting job …")
        job_config  = config.get("job_config", {})
        exec_prompt = (
            f"Submit ETL job '{job_name}' with config: {_dumps(job_config)}. "
            f"Resource allocation: {_dumps(allocation)}."
        )
        execution = self._call_agent(self._execution_agent, exec_prompt)
        job_run_id = execution.get("job_run_id", execution.get("run_id", ""))
        logger.info("[Phase 4] Done — run_id=%s  status=%s",
                    job_run_id, execution.get("status", "?"))

        # ── Phase 5: Post-execution analytics (parallel, 5 agents) ─────────────
        logger.info("[Phase 5] Post-execution analytics in parallel …")
        all_phase_results = {
            "sizing":        sizing_result,
            "data_quality":  phase1.get("data_quality", {}),
            "compliance":    phase1.get("compliance", {}),
            "code_analysis": phase1.get("code_analysis", {}),
            "lineage":       phase1.get("lineage", {}),
            "delta_iceberg": phase1.get("delta_iceberg", {}),
            "allocation":    allocation,
            "execution":     execution,
        }

        def _run_glue_metrics():
            if not job_run_id:
                return {"skipped": True, "reason": "No job_run_id from execution phase"}
            return self._call_agent(
                self._glue_metrics_agent,
                f"Fetch and analyse CloudWatch metrics for Glue job '{job_name}' "
                f"run ID '{job_run_id}'."
            )

        def _run_spark_event_log():
            if not event_log:
                return {"skipped": True, "reason": "No event_log_path in config"}
            return self._call_agent(
                self._spark_event_log_agent,
                f"Parse Spark event log at '{event_log}' and identify bottlenecks."
            )

        def _run_script_tester():
            tgt_script = (phase3.get("recommendation_applier", {}).get("modified_script")
                          or phase3.get("job_generator", {}).get("generated_script")
                          or script)
            if not tgt_script:
                return {"skipped": True}
            return self._call_agent(
                self._script_tester_agent,
                f"Generate and run tests for this PySpark script. "
                f"Source tables: {tables_json}. "
                f"Processing mode: {mode}. Script:\n{tgt_script[:2000]}"
            )

        def _run_recommendations():
            return self._call_agent(
                self._recommendation_agent,
                f"Synthesise all pipeline findings into a prioritised recommendation report. "
                f"Results: {_dumps(all_phase_results)}. "
                f"Runs per day: {runs_per_day}."
            )

        def _run_learning():
            return self._call_agent(
                self._learning_agent,
                f"Capture a learning vector for pipeline '{pipeline_id}' job '{job_name}'. "
                f"Results: {_dumps(all_phase_results)}."
            )

        phase5 = self._run_parallel({
            "glue_metrics":    _run_glue_metrics,
            "event_log":       _run_spark_event_log,
            "script_tests":    _run_script_tester,
            "recommendations": _run_recommendations,
            "learning":        _run_learning,
        })

        logger.info("[Phase 5] Done")

        # ── Assemble final report ───────────────────────────────────────────────
        finished_at = datetime.utcnow().isoformat()
        report = {
            "pipeline_id":   pipeline_id,
            "job_name":      job_name,
            "started_at":    started_at,
            "finished_at":   finished_at,
            "processing_mode": mode,

            # Phase results
            "phase1_analysis": {
                "sizing":        phase1.get("sizing", {}),
                "data_quality":  phase1.get("data_quality", {}),
                "compliance":    phase1.get("compliance", {}),
                "code_analysis": phase1.get("code_analysis", {}),
                "lineage":       phase1.get("lineage", {}),
                "delta_iceberg": phase1.get("delta_iceberg", {}),
            },
            "phase2_allocation":     allocation,
            "phase3_artefacts": {
                "optimised_script":  phase3.get("recommendation_applier", {}),
                "generated_job":     phase3.get("job_generator", {}),
            },
            "phase4_execution":      execution,
            "phase5_analytics": {
                "glue_metrics":    phase5.get("glue_metrics", {}),
                "event_log":       phase5.get("event_log", {}),
                "script_tests":    phase5.get("script_tests", {}),
                "recommendations": phase5.get("recommendations", {}),
                "learning":        phase5.get("learning", {}),
            },

            # Summary stats
            "summary": {
                "agents_used":        15,
                "tools_available":    43,
                "phases_completed":   5,
                "effective_size_gb":  effective_gb,
                "skew_risk_score":    skew_score,
                "pii_columns_found":  len(phase1.get("compliance", {})
                                          .get("pii_columns", [])),
                "anti_patterns_found": len(phase1.get("code_analysis", {})
                                           .get("anti_patterns", [])),
                "test_cases_generated": phase5.get("script_tests", {}).get("test_count", 0),
                "execution_status":   execution.get("status", "unknown"),
            },
        }

        logger.info("Pipeline %s complete  |  %d agents  |  effective_gb=%.1f",
                    pipeline_id, 15, effective_gb)
        return report

    def chat(self, message: str) -> str:
        """Interactive mode — send a free-text message to the OrchestratorAgent."""
        response = self._orchestrator(message)
        return str(response)


# ── Factory and CLI entry-point ─────────────────────────────────────────────────

def create_multi_agent_orchestrator(
    model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
    region: str = "us-west-2",
) -> MultiAgentOrchestrator:
    """Factory function for the 15-agent ETL orchestrator."""
    return MultiAgentOrchestrator(model_id=model_id, region=region)


def main():
    """CLI entry-point: runs the pipeline from sample_pipeline_config.json."""
    import sys
    from pathlib import Path

    cfg_path = Path(__file__).parent / "sample_pipeline_config.json"
    if len(sys.argv) > 1:
        cfg_path = Path(sys.argv[1])

    if not cfg_path.exists():
        print(f"Config file not found: {cfg_path}")
        sys.exit(1)

    config = json.loads(cfg_path.read_text())
    print(f"Starting multi-agent ETL pipeline for job: {config.get('job_name', 'unknown')}")
    print(f"Agents: 15  |  Tools: 43  |  Phases: 5\n")

    orchestrator = create_multi_agent_orchestrator()
    result = orchestrator.run_pipeline(config)

    summary = result.get("summary", {})
    print("\n" + "=" * 60)
    print("Pipeline Complete")
    print("=" * 60)
    print(f"  Agents used:          {summary.get('agents_used', 15)}")
    print(f"  Tools available:      {summary.get('tools_available', 43)}")
    print(f"  Effective size (GB):  {summary.get('effective_size_gb', 0):.1f}")
    print(f"  PII columns found:    {summary.get('pii_columns_found', 0)}")
    print(f"  Anti-patterns found:  {summary.get('anti_patterns_found', 0)}")
    print(f"  Test cases generated: {summary.get('test_cases_generated', 0)}")
    print(f"  Execution status:     {summary.get('execution_status', 'unknown')}")

    out_path = Path("pipeline_result.json")
    out_path.write_text(json.dumps(result, indent=2, cls=_DateEncoder))
    print(f"\nFull report saved to: {out_path}")


if __name__ == "__main__":
    main()
