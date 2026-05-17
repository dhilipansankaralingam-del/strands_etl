#!/usr/bin/env python3
"""
strands_etl_agents.py
=====================
CLI entry point that triggers the Strands SDK Multi-Agent ETL Pipeline.

All intelligence lives inside the 19 specialist agents (Strands SDK).
This script is purely a bootstrap: parse args → build config → fire pipeline.

Usage:
    python3.11 scripts/strands_etl_agents.py --config job.json [OPTIONS]

Full example:
    python3.11 scripts/strands_etl_agents.py \
        --config         etl_config.json \
        --model          us.anthropic.claude-sonnet-4-6-20250514 \
        --region         us-west-2 \
        --mode           delta \
        --script         jobs/orders_etl.py \
        --athena-output  s3://my-bucket/athena-results/ \
        --glue-metrics   metrics/glue_cloudwatch.json \
        --spark-event-log s3://my-bucket/spark-event-logs/orders_etl/ \
        --runs-per-day   4 \
        --workers        6 \
        --worker-type    G.1X \
        --compliance     GDPR,PCI-DSS \
        --output-file    results/pipeline_out.json
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

# ── allow running from repo root ─────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from orchestrator.multi_agent_orchestrator import MultiAgentOrchestrator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("strands_etl_cli")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_json_arg(value) -> dict:
    """
    Accept a metrics value that is one of:
      - None                  → return {}
      - a dict (from JSON)    → return as-is
      - a file path string    → read and parse the JSON file
      - an inline JSON string → parse directly
    """
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    # Try as file path first
    p = Path(value)
    if p.exists():
        logger.info("Loading metrics from file: %s", p)
        return json.loads(p.read_text())
    # Try inline JSON string
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        logger.warning("Could not parse metrics value as file or JSON: %s", value)
        return {}


def _translate_config(raw: dict, args: argparse.Namespace) -> dict:
    """
    Translate etl_config.json → run_pipeline config dict.
    CLI args always override values in the JSON file.
    """
    workload = raw.get("workload", raw)
    platform = raw.get("platform", {})
    checks   = raw.get("checks", {})
    scripts  = raw.get("scripts", {})

    # ── source_tables ─────────────────────────────────────────────────────────
    source_tables = []
    for ds in workload.get("data_sources", []):
        source_tables.append({
            "database":         ds.get("database", "default"),
            "table":            ds.get("table", ds.get("name", "")),
            "format":           ds.get("format", "parquet"),
            "record_count":     ds.get("record_count", 0),
            "column_count":     ds.get("column_count", 30),
            "size_gb":          ds.get("size_gb", 0),
            "join_key":         ds.get("join_key", ds.get("on", "")),
            "partition_column": ds.get("partition_column", ""),
            "description":      ds.get("description", ""),
        })
    if not source_tables:
        source_tables = raw.get("source_tables", [])

    # ── table_schemas ─────────────────────────────────────────────────────────
    table_schemas = raw.get("table_schemas", [])

    # ── target_table ──────────────────────────────────────────────────────────
    target = workload.get("target", raw.get("target_table", {}))

    # ── compliance frameworks ─────────────────────────────────────────────────
    frameworks = (
        [f.strip() for f in args.compliance.split(",")]
        if args.compliance
        else raw.get("compliance_frameworks", ["GDPR", "HIPAA", "PCI-DSS"])
    )

    # ── DQ rules from checks.data_quality ────────────────────────────────────
    dq_rules = []
    for chk in checks.get("data_quality", []):
        dq_rules.append({
            "rule_id":       chk.get("name", ""),
            "name":          chk.get("description", chk.get("name", "")),
            "rule_type":     chk.get("rule", "completeness"),
            "target_table":  chk.get("table", ""),
            "target_column": chk.get("column", ""),
            "severity":      "error",
        })

    # ── resource config ───────────────────────────────────────────────────────
    res = platform.get("resource_allocation", {})
    worker_type = args.worker_type or res.get("worker_type", "G.2X")
    num_workers = args.workers     or int(res.get("workers", 10))

    # ── script content ────────────────────────────────────────────────────────
    script_content = ""
    script_path = args.script or scripts.get("local_pyspark", "")
    if script_path and Path(script_path).exists():
        script_content = Path(script_path).read_text()
        logger.info("Loaded script: %s (%d bytes)", script_path, len(script_content))
    elif script_path:
        logger.warning("Script not found locally (may be S3 path): %s", script_path)

    # ── glue metrics — pre-collected CloudWatch JSON ──────────────────────────
    # CLI: --glue-metrics metrics/glue_cloudwatch.json  OR  --glue-metrics '{"heap_peak_pct":82}'
    # Config key: "glue_metrics": { ... } or "glue_metrics": "path/to/file.json"
    glue_metrics = _load_json_arg(args.glue_metrics or raw.get("glue_metrics"))

    # ── spark event log — S3 path to Spark UI event log ──────────────────────
    # CLI: --spark-event-log s3://bucket/spark-event-logs/job-name/
    # Config key: "event_log_path": "s3://..."
    # SparkEventLogAgent (Phase 5) reads this to find stage bottlenecks/skew.
    spark_event_log = (args.spark_event_log
                       or args.event_log
                       or raw.get("event_log_path", "")
                       or raw.get("spark_event_log", ""))

    # ── assemble final pipeline config ────────────────────────────────────────
    return {
        "job_name":              args.job_name or workload.get("name", "etl_job"),
        "processing_mode":       args.mode     or raw.get("processing_mode", "full"),
        "source_tables":         source_tables,
        "table_schemas":         table_schemas,
        "target_table":          target,
        "validation_rules":      dq_rules,
        "compliance_frameworks": frameworks,
        "script_content":        script_content,
        "runs_per_day":          args.runs_per_day,
        "platform":              platform.get("preferred", args.platform),

        # Phase 0 — Iceberg telemetry (skipped when empty)
        "athena_output_s3":      args.athena_output or raw.get("athena_output_s3", ""),

        # Phase 1 / compound analysis — pre-collected Glue CloudWatch metrics
        # Accepted as: JSON file path, inline JSON string, or dict in config file
        # Keys used: heap_peak_pct, worker_count, avg_task_time_ms, shuffle_read_gb,
        #            spill_gb, gc_time_pct, num_workers, driver_memory_pct
        "glue_metrics":          glue_metrics,

        # Phase 5 — Spark event log (S3 URI or local path)
        # SparkEventLogAgent will parse stage timelines, find stragglers and skew
        "event_log_path":        spark_event_log,

        "job_config": {
            "worker_type":     worker_type,
            "num_workers":     num_workers,
            "glue_version":    getattr(args, "glue_version", None) or raw.get("glue_version", "5.0"),
            "use_graviton":    getattr(args, "graviton", False) or raw.get("use_graviton", False),
            "timeout_minutes": _parse_timeout(res.get("timeout", 120)),
            "script_location": scripts.get("pyspark", ""),
            "temp_dir":        raw.get("temp_dir", ""),
        },
        "current_config": {
            "worker_type": res.get("worker_type", "G.2X"),
            "num_workers": int(res.get("workers", 10)),
        },
    }


def _parse_timeout(val) -> int:
    """Parse timeout — accepts int (minutes) or string like '2h', '120'."""
    if isinstance(val, int):
        return val
    s = str(val).lower().strip()
    if s.endswith("h"):
        return int(s[:-1]) * 60
    return int(s)


# ── Output helpers ────────────────────────────────────────────────────────────

def _print_summary(result: dict) -> None:
    summary = result.get("summary", {})
    telem   = result.get("phase0_iceberg_telemetry", {})
    alloc   = result.get("phase2_allocation", {})
    code    = result.get("phase1_analysis", {}).get("code_analysis", {})
    dq      = result.get("phase1_analysis", {}).get("data_quality", {})
    glue_m  = result.get("phase5_analytics", {}).get("glue_metrics", {})
    spark_m = result.get("phase5_analytics", {}).get("event_log", {})

    print("\n" + "=" * 65)
    print("  STRANDS MULTI-AGENT ETL PIPELINE — RESULTS")
    print("=" * 65)
    print(f"  Pipeline ID   : {result.get('pipeline_id')}")
    print(f"  Job           : {result.get('job_name')}")
    print(f"  Mode          : {result.get('processing_mode')}")
    print(f"  Agents used   : {summary.get('agents_used', 19)}")
    print(f"  Tools avail   : {summary.get('tools_available', 55)}")
    print(f"  Started       : {result.get('started_at')}")
    print(f"  Finished      : {result.get('finished_at')}")
    print("-" * 65)

    # Phase 0 — Iceberg telemetry
    if telem and not telem.get("error"):
        print(f"\n[Phase 0] Iceberg Telemetry ({telem.get('tables_analyzed', 0)} tables)")
        for t in telem.get("tables", []):
            st = t.get("iceberg_stats", t.get("sizing_analysis", {}))
            print(f"  {t['table']:30s}  health={st.get('health_grade','?')}  "
                  f"size={st.get('total_size_gb',0):.1f}GB  "
                  f"tiny_files={st.get('tiny_file_cnt',0):,}  "
                  f"skew={st.get('skew_ratio',1):.1f}×")
    elif summary.get("iceberg_telemetry_enabled"):
        print("\n[Phase 0] Iceberg Telemetry: ERROR — check athena_output_s3 permissions")
    else:
        print("\n[Phase 0] Iceberg Telemetry: SKIPPED (pass --athena-output to enable)")

    # Phase 1 — analysis
    print(f"\n[Phase 1] Analysis")
    print(f"  Effective data size : {summary.get('effective_size_gb', 0):.1f} GB")
    print(f"  Skew risk score     : {summary.get('skew_risk_score', 0)}/100")
    print(f"  DQ overall score    : {dq.get('overall_score', '?')}/100")
    print(f"  PII columns found   : {summary.get('pii_columns_found', 0)}")
    print(f"  Anti-patterns found : {summary.get('anti_patterns_found', 0)}")
    if code.get("top_expensive_line"):
        tl = code["top_expensive_line"]
        print(f"  Top expensive line  : L{tl.get('line')} — {str(tl.get('issue',''))[:60]}")

    # Phase 2 — resource allocation
    opt = alloc.get("optimal_config", {})
    if opt:
        print(f"\n[Phase 2] Resource Allocation")
        print(f"  Recommended  : {opt.get('worker_type')} × {opt.get('num_workers')} workers")
        print(f"  Cost saving  : {alloc.get('estimated_savings_pct', 0)}%")
        if alloc.get("monthly_savings_usd"):
            print(f"  Monthly save : ${alloc['monthly_savings_usd']:.0f}")

    # Phase 4 — execution
    exec_r = result.get("phase4_execution", {})
    print(f"\n[Phase 4] Execution")
    print(f"  Job run ID  : {exec_r.get('job_run_id', 'N/A')}")
    print(f"  Status      : {exec_r.get('status', 'N/A')}")

    # Phase 5 — post-execution analytics
    if glue_m and not glue_m.get("skipped"):
        print(f"\n[Phase 5] Glue Metrics")
        print(f"  Heap peak  : {glue_m.get('heap_peak_pct', '?')}%")
        print(f"  Shuffle GB : {glue_m.get('shuffle_read_gb', '?')}")
    if spark_m and not spark_m.get("skipped"):
        print(f"\n[Phase 5] Spark Event Log")
        bottlenecks = spark_m.get("bottlenecks", [])
        if bottlenecks:
            print(f"  Bottlenecks : {bottlenecks[0]}")
    tests = result.get("phase5_analytics", {}).get("script_tests", {})
    if tests.get("test_count"):
        print(f"\n[Phase 5] Tests: {tests.get('passed', 0)}/{tests.get('test_count', 0)} passed")

    print("=" * 65 + "\n")


# ── Argument parser ───────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="strands_etl_agents.py",
        description="Trigger the Strands SDK Multi-Agent ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Minimal
  python3.11 scripts/strands_etl_agents.py --config etl_config.json

  # Full run — Iceberg telemetry + pre-collected Glue metrics + Spark event log
  python3.11 scripts/strands_etl_agents.py \\
      --config          etl_config.json \\
      --model           us.anthropic.claude-sonnet-4-6-20250514 \\
      --mode            delta \\
      --script          jobs/orders_etl.py \\
      --athena-output   s3://my-bucket/athena-results/ \\
      --glue-metrics    metrics/glue_cloudwatch.json \\
      --spark-event-log s3://my-bucket/spark-event-logs/orders_etl/ \\
      --glue-version    5.0 \\
      --graviton \\
      --runs-per-day    4 \\
      --workers         6 --worker-type G.2X \\
      --compliance      GDPR,PCI-DSS \\
      --output-file     results/pipeline_out.json

  # Glue 5.2 with Graviton3 workers (16% cost saving)
  python3.11 scripts/strands_etl_agents.py \\
      --config       etl_config.json \\
      --glue-version 5.2 \\
      --graviton \\
      --worker-type  G.2X \\
      --workers      10

  # Pass Glue metrics inline (no file needed)
  python3.11 scripts/strands_etl_agents.py \\
      --config       etl_config.json \\
      --glue-metrics '{"heap_peak_pct": 87, "worker_count": 10, "shuffle_read_gb": 42}'

  # Chat mode — ask the orchestrator a free-form question
  python3.11 scripts/strands_etl_agents.py \\
      --config etl_config.json \\
      --chat "Why is my orders job slow and what should I fix first?"
        """,
    )

    # ── Config ────────────────────────────────────────────────────────────────
    p.add_argument("--config", required=True,
                   help="Path to job config JSON (etl_config.json format)")

    # ── Model / AWS ───────────────────────────────────────────────────────────
    p.add_argument("--model",  default="us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                   help="Bedrock model ID")
    p.add_argument("--region", default="us-west-2",
                   help="AWS region (default: us-west-2)")

    # ── Pipeline overrides ────────────────────────────────────────────────────
    p.add_argument("--job-name",   default=None, dest="job_name",
                   help="Override job name from config")
    p.add_argument("--mode",       choices=["full", "delta"], default=None,
                   help="Processing mode: full | delta")
    p.add_argument("--script",     default=None,
                   help="Path to local PySpark .py script for code analysis")
    p.add_argument("--platform",   choices=["glue", "emr"], default="glue",
                   help="Execution platform (default: glue)")
    p.add_argument("--compliance", default=None,
                   help="Comma-separated frameworks: GDPR,HIPAA,PCI-DSS,SOX,CCPA")

    # ── Metrics inputs ────────────────────────────────────────────────────────
    p.add_argument("--athena-output", default=None, dest="athena_output",
                   help="S3 URI for Athena results — enables Phase 0 Iceberg telemetry\n"
                        "e.g. s3://my-bucket/athena-results/")

    p.add_argument("--glue-metrics", default=None, dest="glue_metrics",
                   metavar="FILE_OR_JSON",
                   help="Pre-collected Glue CloudWatch metrics. Accepted as:\n"
                        "  JSON file path: metrics/glue_cloudwatch.json\n"
                        "  Inline JSON:    '{\"heap_peak_pct\":87,\"worker_count\":10}'\n"
                        "Keys used by agents: heap_peak_pct, worker_count,\n"
                        "  avg_task_time_ms, shuffle_read_gb, spill_gb,\n"
                        "  gc_time_pct, driver_memory_pct")

    p.add_argument("--spark-event-log", default=None, dest="spark_event_log",
                   metavar="S3_URI_OR_PATH",
                   help="S3 URI (or local path) to Spark UI event log directory.\n"
                        "SparkEventLogAgent parses stage timelines, finds bottlenecks\n"
                        "and skewed tasks in Phase 5.\n"
                        "e.g. s3://my-bucket/spark-event-logs/orders_etl/\n"
                        "     or /tmp/spark-events/app-20260512/")

    p.add_argument("--event-log", default=None, dest="event_log",
                   help="Alias for --spark-event-log (backwards compat)")

    # ── Resource overrides ────────────────────────────────────────────────────
    p.add_argument("--workers",      type=int, default=None,
                   help="Number of Glue/EMR workers (overrides config)")
    p.add_argument("--worker-type",  default=None, dest="worker_type",
                   choices=["G.1X", "G.2X", "G.4X", "G.8X", "Z.2X",
                            "G.1X.GRAVITON", "G.2X.GRAVITON", "G.4X.GRAVITON"],
                   help="Glue worker type (overrides config). .GRAVITON variants require Glue 5.0+")
    p.add_argument("--glue-version", default=None, dest="glue_version",
                   choices=["4.0", "5.0", "5.2"],
                   help="Target Glue version: 4.0 | 5.0 | 5.2 (default: from config or 5.0). "
                        "Glue 5.0 = Spark 3.5.2 + Iceberg 1.6.1. Glue 5.2 = Spark 3.5.4 + Iceberg 1.7.1.")
    p.add_argument("--graviton",     action="store_true", default=False,
                   help="Recommend Graviton3 workers (16%% cheaper, requires Glue 5.0+). "
                        "Sets use_graviton=true in job definition.")
    p.add_argument("--runs-per-day", type=int, default=1, dest="runs_per_day",
                   help="How many times per day this job runs (for cost calc, default: 1)")

    # ── Output ────────────────────────────────────────────────────────────────
    p.add_argument("--output-file", default=None, dest="output_file",
                   help="Save full pipeline result JSON to this file\n"
                        "(auto-named pipeline_result_<job>_<ts>.json if omitted)")
    p.add_argument("--verbose",     action="store_true",
                   help="Print full result JSON to stdout")

    # ── Chat mode ─────────────────────────────────────────────────────────────
    p.add_argument("--chat", default=None,
                   help="Skip pipeline, ask the OrchestratorAgent a free-form question")

    return p


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = _build_parser()
    args   = parser.parse_args()

    # Load config file
    config_path = Path(args.config)
    if not config_path.exists():
        parser.error(f"Config file not found: {args.config}")
    raw_config = json.loads(config_path.read_text())
    logger.info("Loaded config: %s", config_path)

    # Build orchestrator
    logger.info("Initialising Strands Multi-Agent Orchestrator  model=%s  region=%s",
                args.model, args.region)
    orch = MultiAgentOrchestrator(
        model_id    = args.model,
        region      = args.region,
        max_workers = 6,
    )

    # Chat mode — bypass full pipeline
    if args.chat:
        logger.info("Chat mode: %s", args.chat)
        response = orch.chat(args.chat)
        print("\n" + "=" * 65)
        print("  ORCHESTRATOR RESPONSE")
        print("=" * 65)
        print(response)
        print("=" * 65)
        return

    # Translate and run
    pipeline_config = _translate_config(raw_config, args)
    logger.info(
        "Pipeline config ready — job=%s  mode=%s  tables=%d  "
        "script=%s  athena=%s  glue_metrics=%s  spark_event_log=%s",
        pipeline_config["job_name"],
        pipeline_config["processing_mode"],
        len(pipeline_config["source_tables"]),
        "YES" if pipeline_config.get("script_content") else "NO",
        "YES" if pipeline_config.get("athena_output_s3") else "NO",
        "YES" if pipeline_config.get("glue_metrics") else "NO",
        "YES" if pipeline_config.get("event_log_path") else "NO",
    )

    result = orch.run_pipeline(pipeline_config)

    _print_summary(result)

    # Save result JSON
    output_path = args.output_file
    if not output_path:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_path = f"pipeline_result_{pipeline_config['job_name']}_{ts}.json"
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2, default=str)
    logger.info("Full result saved → %s", output_path)

    if args.verbose:
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
