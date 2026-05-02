"""
Multi-Agent ETL Orchestrator
=============================

Architecture (agent-to-agent via Strands agent_as_tool pattern):

  OrchestratorAgent
    │
    ├── Phase 1 — PARALLEL (ThreadPoolExecutor)
    │     ├── SizingAgent           → data volume, skew risk, partition efficiency
    │     ├── DataQualityAgent      → DQ rule evaluation, schema checks
    │     ├── ComplianceAgent       → PII detection, GDPR/HIPAA/PCI-DSS gaps
    │     └── CodeAnalyzerAgent     → line-by-line anti-pattern + optimisation
    │
    ├── Phase 2 — SEQUENTIAL (consumes Phase 1 results)
    │     └── ResourceAllocatorAgent → right-sizing, multi-cloud cost comparison
    │
    ├── Phase 3 — SEQUENTIAL (consumes Phase 2 results)
    │     └── ExecutionAgent         → submit & monitor Glue / EMR / Lambda job
    │
    └── Phase 4 — PARALLEL (consumes all prior results)
          ├── RecommendationAgent   → ROI-driven roadmap synthesis
          └── LearningAgent         → persist learning vector to S3
"""

import json
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from datetime import datetime
from typing import Any, Dict, List, Optional

from strands import Agent
from strands.models import BedrockModel
from strands.tools import agent_as_tool

from .agents import (
    create_sizing_agent,
    create_data_quality_agent,
    create_compliance_agent,
    create_code_analyzer_agent,
    create_resource_allocator_agent,
    create_execution_agent,
    create_recommendation_agent,
    create_learning_agent,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Orchestrator system prompt
# ---------------------------------------------------------------------------
_ORCHESTRATOR_SYSTEM_PROMPT = """
You are the **Master ETL Orchestration Agent** for an enterprise data platform.

You coordinate a team of specialist agents that each handle one aspect of ETL pipeline management.
Your responsibilities:
1. Parse the user request and config to understand what needs to be done.
2. Dispatch Phase-1 agents in parallel (sizing, data_quality, compliance, code_analyzer).
3. Feed Phase-1 results to the resource_allocator_agent.
4. Feed Phase-2 results to the execution_agent.
5. Dispatch Phase-4 agents in parallel (recommendation, learning).
6. Assemble the final pipeline report and return it.

You communicate with each specialist agent as a tool call.
Always pass structured JSON between agents.
Return a complete pipeline execution report as structured JSON.
"""


# ---------------------------------------------------------------------------
class MultiAgentOrchestrator:
    """
    Coordinates multi-phase, multi-agent ETL pipeline execution.

    Agent-to-Agent (A2A) interaction is achieved via Strands agent_as_tool:
    each specialist agent is wrapped as a callable tool that the orchestrator
    can invoke directly.  Phase-1 and Phase-4 agents run in parallel using a
    ThreadPoolExecutor.
    """

    def __init__(
        self,
        model_id: str  = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
        region:   str  = "us-west-2",
        max_workers: int = 4,
    ):
        self.model_id    = model_id
        self.region      = region
        self.max_workers = max_workers

        logger.info("Initialising specialist agents …")

        # ── Specialist agents ─────────────────────────────────────────────
        self._sizing_agent      = create_sizing_agent(model_id, region)
        self._dq_agent          = create_data_quality_agent(model_id, region)
        self._compliance_agent  = create_compliance_agent(model_id, region)
        self._code_agent        = create_code_analyzer_agent(model_id, region)
        self._resource_agent    = create_resource_allocator_agent(model_id, region)
        self._execution_agent   = create_execution_agent(model_id, region)
        self._rec_agent         = create_recommendation_agent(model_id, region)
        self._learning_agent    = create_learning_agent(model_id, region)

        # ── Wrap each specialist as an agent_as_tool for the orchestrator ─
        sizing_tool      = agent_as_tool(self._sizing_agent,     "sizing_agent",     "Estimates data volumes, skew risk, and partition efficiency for source tables.")
        dq_tool          = agent_as_tool(self._dq_agent,         "data_quality_agent","Evaluates DQ rules, detects schema issues, and scores data quality 0-100.")
        compliance_tool  = agent_as_tool(self._compliance_agent, "compliance_agent", "Scans schemas for PII, checks GDPR/HIPAA/PCI-DSS compliance, recommends masking.")
        code_tool        = agent_as_tool(self._code_agent,       "code_analyzer_agent","Line-by-line PySpark anti-pattern detection, complexity scoring, AQE config recs.")
        resource_tool    = agent_as_tool(self._resource_agent,   "resource_allocator_agent","Right-sizes compute resources and compares costs across Glue/EMR/Azure/GCP/Databricks.")
        execution_tool   = agent_as_tool(self._execution_agent,  "execution_agent",  "Submits and monitors ETL jobs on Glue, EMR, or Lambda.")
        rec_tool         = agent_as_tool(self._rec_agent,        "recommendation_agent","Synthesises all agent findings into a prioritised ROI-driven recommendation report.")
        learning_tool    = agent_as_tool(self._learning_agent,   "learning_agent",   "Captures execution patterns as learning vectors to S3 for continuous improvement.")

        # ── Orchestrator Agent (uses all specialist agents as tools) ──────
        model             = BedrockModel(model_id=model_id, region_name=region)
        self.orchestrator = Agent(
            model         = model,
            system_prompt = _ORCHESTRATOR_SYSTEM_PROMPT,
            tools         = [
                sizing_tool, dq_tool, compliance_tool, code_tool,
                resource_tool, execution_tool, rec_tool, learning_tool,
            ],
        )
        logger.info("All agents ready.")

    # -----------------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------------

    def _call_agent(self, agent: Agent, prompt: str, agent_name: str) -> Dict[str, Any]:
        """Call a specialist agent and parse its JSON response."""
        logger.info("→ [%s] Starting …", agent_name)
        try:
            raw = str(agent(prompt))
            # Try to extract JSON from the response
            start = raw.find("{")
            end   = raw.rfind("}") + 1
            if start >= 0 and end > start:
                result = json.loads(raw[start:end])
            else:
                result = {"raw_response": raw}
            logger.info("✓ [%s] Completed.", agent_name)
            return result
        except Exception as exc:
            logger.error("✗ [%s] Failed: %s", agent_name, exc)
            return {"error": str(exc), "agent": agent_name}

    def _run_parallel(self, tasks: Dict[str, tuple]) -> Dict[str, Any]:
        """
        Run multiple (agent, prompt) tasks in parallel.

        Args:
            tasks: {result_key: (agent, prompt, agent_name)}

        Returns:
            {result_key: agent_result_dict}
        """
        results: Dict[str, Any] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            future_map: Dict[Future, str] = {
                pool.submit(self._call_agent, agent, prompt, name): key
                for key, (agent, prompt, name) in tasks.items()
            }
            for fut in as_completed(future_map):
                key = future_map[fut]
                try:
                    results[key] = fut.result()
                except Exception as exc:
                    logger.error("Parallel task '%s' raised: %s", key, exc)
                    results[key] = {"error": str(exc)}
        return results

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def run_pipeline(
        self,
        user_request: str,
        config: Dict[str, Any],
        pipeline_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute the full multi-agent ETL pipeline.

        Execution order:
          Phase 1 (parallel) → Phase 2 (sequential) → Phase 3 (sequential)
          → Phase 4 (parallel)

        Args:
            user_request: Natural-language description of what the pipeline should do.
            config:       Pipeline configuration dict (tables, scripts, platform, compliance, …).
            pipeline_id:  Optional unique run identifier.

        Returns:
            Complete pipeline report dict.
        """
        run_id     = pipeline_id or str(uuid.uuid4())
        start_time = datetime.utcnow().isoformat()
        logger.info("═══ Pipeline %s starting ═══", run_id)

        report: Dict[str, Any] = {
            "pipeline_id": run_id,
            "user_request": user_request,
            "start_time": start_time,
            "status": "running",
            "phases": {},
        }

        cfg_str = json.dumps(config)

        # ── Build shared context strings ───────────────────────────────────
        tables          = config.get("workload", {}).get("data_sources", config.get("tables", []))
        tables_json     = json.dumps(tables)
        schemas_json    = json.dumps(config.get("table_schemas", []))
        script_path     = (config.get("scripts", {}).get("pyspark", "") or
                           config.get("script_path", ""))
        script_content  = config.get("script_content", "")
        processing_mode = config.get("processing_mode", "full")
        frameworks_json = json.dumps(config.get("compliance", {}).get("frameworks", ["gdpr", "pci_dss"]))
        dq_rules_json   = json.dumps(config.get("data_quality", {}).get("rules", []))
        cur_platform    = json.dumps(config.get("platform", {}).get("resource_allocation",
                                                                     {"number_of_workers": 10, "worker_type": "G.2X"}))
        join_count      = config.get("join_count", len(tables))

        # ══════════════════════════════════════════════════════════════════
        # PHASE 1 — Parallel: Sizing, DataQuality, Compliance, CodeAnalyzer
        # ══════════════════════════════════════════════════════════════════
        logger.info("── Phase 1 (parallel): sizing, data_quality, compliance, code_analyzer ──")

        phase1_tasks = {
            "sizing": (
                self._sizing_agent,
                (f"Analyse these source tables and produce a sizing report.\n\n"
                 f"Tables: {tables_json}\n"
                 f"Processing mode: {processing_mode}\n"
                 f"Join count: {join_count}\n\n"
                 f"Call analyse_data_sizing with tables_json='{tables_json}', "
                 f"processing_mode='{processing_mode}', join_count={join_count}."),
                "SizingAgent",
            ),
            "data_quality": (
                self._dq_agent,
                (f"Evaluate data quality for the pipeline.\n\n"
                 f"Table schemas: {schemas_json}\n"
                 f"DQ rules: {dq_rules_json}\n\n"
                 f"First call generate_dq_rules_from_schema if rules are empty, "
                 f"then call run_data_quality_checks."),
                "DataQualityAgent",
            ),
            "compliance": (
                self._compliance_agent,
                (f"Scan for PII and compliance gaps.\n\n"
                 f"Table schemas: {schemas_json}\n"
                 f"Active frameworks: {frameworks_json}\n\n"
                 f"Call scan_schema_for_pii with table_schemas_json='{schemas_json}', "
                 f"active_frameworks_json='{frameworks_json}'."),
                "ComplianceAgent",
            ),
            "code_analyzer": (
                self._code_agent,
                (f"Perform line-by-line analysis of the PySpark script.\n\n"
                 f"Script content (first 4000 chars): {script_content[:4000]}\n"
                 f"Script path: {script_path}\n\n"
                 f"Call analyse_pyspark_code with the full script_content."),
                "CodeAnalyzerAgent",
            ),
        }

        phase1_results = self._run_parallel(phase1_tasks)
        report["phases"]["phase_1"] = {
            "status":     "completed",
            "agents":     list(phase1_tasks.keys()),
            "results_summary": {
                "effective_size_gb":    phase1_results.get("sizing", {}).get("effective_size_gb"),
                "dq_score":             phase1_results.get("data_quality", {}).get("overall_score"),
                "compliance_score":     phase1_results.get("compliance", {}).get("compliance_score"),
                "code_quality_score":   phase1_results.get("code_analyzer", {}).get("optimization_score"),
                "anti_patterns_found":  phase1_results.get("code_analyzer", {}).get("anti_pattern_count"),
                "pii_columns_found":    len(phase1_results.get("compliance", {}).get("pii_columns_found", [])),
            },
        }

        sizing_json   = json.dumps(phase1_results.get("sizing", {}))
        code_json     = json.dumps(phase1_results.get("code_analyzer", {}))
        effective_gb  = float(phase1_results.get("sizing", {}).get("effective_size_gb", 100))

        # ══════════════════════════════════════════════════════════════════
        # PHASE 2 — Sequential: Resource Allocator
        # ══════════════════════════════════════════════════════════════════
        logger.info("── Phase 2 (sequential): resource_allocator ──")

        resource_result = self._call_agent(
            self._resource_agent,
            (f"Calculate optimal resource allocation and cost comparison.\n\n"
             f"Sizing result: {sizing_json}\n"
             f"Code analysis: {code_json}\n"
             f"Current config: {cur_platform}\n\n"
             f"Call allocate_resources with sizing_result_json='{sizing_json}', "
             f"code_analysis_json='{code_json}', "
             f"current_config_json='{cur_platform}'."),
            "ResourceAllocatorAgent",
        )
        report["phases"]["phase_2"] = {
            "status":          "completed",
            "agents":          ["resource_allocator"],
            "results_summary": {
                "optimal_workers":   (resource_result.get("optimal_config") or {}).get("workers"),
                "optimal_type":      (resource_result.get("optimal_config") or {}).get("worker_type"),
                "savings_percent":   (resource_result.get("savings") or {}).get("percent"),
            },
        }

        # ══════════════════════════════════════════════════════════════════
        # PHASE 3 — Sequential: Execution Agent
        # ══════════════════════════════════════════════════════════════════
        logger.info("── Phase 3 (sequential): execution_agent ──")

        job_config = {
            "script_location": script_path,
            "iam_role":        config.get("platform", {}).get("iam_role", "GlueServiceRole"),
            "config_s3_path":  config.get("config_s3_path", ""),
        }
        exec_result = self._call_agent(
            self._execution_agent,
            (f"Submit the ETL job with optimised configuration.\n\n"
             f"Job config: {json.dumps(job_config)}\n"
             f"Resource allocation: {json.dumps(resource_result)}\n\n"
             f"Call submit_glue_job with job_config_json='{json.dumps(job_config)}' "
             f"and resource_allocation_json='{json.dumps(resource_result)}'."),
            "ExecutionAgent",
        )
        report["phases"]["phase_3"] = {
            "status":          "completed",
            "agents":          ["execution"],
            "results_summary": {
                "job_name":  exec_result.get("job_name"),
                "run_id":    exec_result.get("run_id"),
                "exec_status": exec_result.get("status"),
            },
        }

        # ══════════════════════════════════════════════════════════════════
        # PHASE 4 — Parallel: Recommendation + Learning
        # ══════════════════════════════════════════════════════════════════
        logger.info("── Phase 4 (parallel): recommendation, learning ──")

        all_results = {
            **phase1_results,
            "resource_allocator": resource_result,
            "execution": exec_result,
        }
        all_json = json.dumps(all_results)

        phase4_tasks = {
            "recommendations": (
                self._rec_agent,
                (f"Synthesise findings from all agents into a recommendation report.\n\n"
                 f"All agent results: {all_json}\n\n"
                 f"Call synthesise_recommendations with all_agent_results_json='{all_json}'."),
                "RecommendationAgent",
            ),
            "learning": (
                self._learning_agent,
                (f"Capture a learning vector for this pipeline run.\n\n"
                 f"Pipeline results: {all_json}\n"
                 f"Pipeline ID: {run_id}\n\n"
                 f"Call capture_learning_vector with pipeline_results_json='{all_json}', "
                 f"pipeline_id='{run_id}'."),
                "LearningAgent",
            ),
        }

        phase4_results = self._run_parallel(phase4_tasks)
        report["phases"]["phase_4"] = {
            "status":          "completed",
            "agents":          list(phase4_tasks.keys()),
            "results_summary": {
                "total_recommendations": len(
                    (phase4_results.get("recommendations") or {}).get("recommendations", [])
                ),
                "quick_wins": (phase4_results.get("recommendations") or {}).get(
                    "executive_summary", {}
                ).get("quick_wins_count"),
                "learning_stored": (phase4_results.get("learning") or {}).get("stored", False),
                "learning_vector_id": (phase4_results.get("learning") or {}).get("learning_vector_id"),
            },
        }

        # ══════════════════════════════════════════════════════════════════
        # Assemble final report
        # ══════════════════════════════════════════════════════════════════
        end_time = datetime.utcnow().isoformat()
        report.update({
            "status":   "completed" if exec_result.get("status") != "failed" else "failed",
            "end_time": end_time,
            "agent_results": {
                **phase1_results,
                "resource_allocator": resource_result,
                "execution":          exec_result,
                "recommendations":    phase4_results.get("recommendations", {}),
                "learning":           phase4_results.get("learning", {}),
            },
            "executive_summary": (
                phase4_results.get("recommendations", {}).get("executive_summary", {})
            ),
        })

        logger.info("═══ Pipeline %s → %s ═══", run_id, report["status"])
        return report

    # -----------------------------------------------------------------------

    def chat(self, user_message: str) -> str:
        """
        Interactive mode: pass a free-form message to the orchestrator agent
        which can then call any specialist agent tool as needed.

        Args:
            user_message: Free-form question or command.

        Returns:
            Orchestrator response string.
        """
        return str(self.orchestrator(user_message))


# ---------------------------------------------------------------------------
# Convenience factory
# ---------------------------------------------------------------------------

def create_multi_agent_orchestrator(
    model_id: str  = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
    region:   str  = "us-west-2",
    max_workers: int = 4,
) -> MultiAgentOrchestrator:
    """Instantiate and return a MultiAgentOrchestrator."""
    return MultiAgentOrchestrator(model_id=model_id, region=region, max_workers=max_workers)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Multi-Agent ETL Orchestrator")
    parser.add_argument("--config",  required=True, help="Path to JSON config file")
    parser.add_argument("--request", default="Process ETL pipeline with quality checks and compliance",
                        help="Natural-language pipeline request")
    parser.add_argument("--model",   default="us.anthropic.claude-3-7-sonnet-20250219-v1:0")
    parser.add_argument("--region",  default="us-west-2")
    parser.add_argument("--chat",    action="store_true", help="Enter interactive chat mode")
    args = parser.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    orchestrator = create_multi_agent_orchestrator(args.model, args.region)

    if args.chat:
        print("Multi-Agent ETL Orchestrator — interactive mode (Ctrl+C to exit)")
        while True:
            try:
                msg = input("\norchestrator> ").strip()
                if msg:
                    print(orchestrator.chat(msg))
            except KeyboardInterrupt:
                break
    else:
        result = orchestrator.run_pipeline(args.request, config)
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
