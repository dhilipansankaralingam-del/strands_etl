"""
run_audit_analysis.py
=====================
Trigger script for the Strands AI Audit Analysis + Data Profiling pipeline.

How it works
------------
1. Reads config/audit_analysis_config.json
2. Queries Athena for FAIL rows in the audit table for a given date
3. For each failure:
     a. AI classifies it (TRUE_FAILURE / FALSE_POSITIVE / NEEDS_INVESTIGATION)
     b. Data profiler checks the source table for anomalies (nulls, schema drift,
        duplicate keys, stale partitions, boundary explosions, etc.)
     c. AI picks the final action (RERUN / DATA_CORRECTION / FIX_LOGIC /
        ESCALATE / MONITOR / IGNORE)
     d. Action is dispatched and logged to S3
4. Prints a summary report

Usage
-----
    # Analyse today's failures
    python run_audit_analysis.py

    # Analyse a specific date
    python run_audit_analysis.py --date 2024-07-01

    # High-severity failures only
    python run_audit_analysis.py --date 2024-07-01 --severity HIGH

    # Use the autonomous Strands agent (full agentic loop)
    python run_audit_analysis.py --date 2024-07-01 --mode agent

    # Profile a specific table only (no audit table needed)
    python run_audit_analysis.py --profile-only --table etl_orchestrator_audit
"""

import argparse
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("audit_analysis")

CONFIG_PATH = Path(__file__).parent / "config" / "audit_analysis_config.json"


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def load_config(path: Path = CONFIG_PATH) -> dict:
    with open(path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Mode A — Direct Python API (recommended for scripts / scheduled jobs)
# ---------------------------------------------------------------------------

def run_direct(cfg: dict, run_date: str, severity: str) -> dict:
    """
    Fetch failures → classify → profile → dispatch → return report.
    This mode is deterministic and easy to integrate into Airflow / Step Functions.
    """
    from validation.audit_action_agent import AuditActionAgent

    athena = cfg["athena"]
    ai     = cfg["ai"]

    agent = AuditActionAgent(
        database        = athena["database"],
        table           = athena["table"],
        athena_output   = athena["output_location"],
        learning_bucket = ai["learning_bucket"],
        aws_region      = athena["region"],
        model_id        = ai["model_id"],
    )

    logger.info(f"Running audit analysis for date={run_date}, severity≥{severity}")
    report = agent.run_for_date(
        date            = run_date,
        severity_filter = severity,
        limit           = ai["max_records_per_run"],
    )
    return report


# ---------------------------------------------------------------------------
# Mode B — Autonomous Strands Agent (full agentic loop)
# ---------------------------------------------------------------------------

def run_agent(cfg: dict, run_date: str, severity: str) -> str:
    """
    The Strands Agent decides autonomously what tools to call and in what order.
    Better for exploratory / ad-hoc analysis via natural language.
    """
    from validation.audit_action_agent import build_audit_agent

    athena = cfg["athena"]
    ai     = cfg["ai"]

    agent = build_audit_agent(
        database        = athena["database"],
        table           = athena["table"],
        athena_output   = athena["output_location"],
        learning_bucket = ai["learning_bucket"],
        aws_region      = athena["region"],
        model_id        = ai["model_id"],
    )

    prompt = (
        f"Analyse all {severity} and higher failures from the audit table for {run_date}. "
        f"For each failure: profile the affected table, classify the failure, "
        f"and dispatch the recommended action. "
        f"Generate fix SQL for any DATA_CORRECTION actions. "
        f"Escalate anything with TEMPORAL_ANOMALY or SCHEMA_DRIFT. "
        f"End with a full executive summary."
    )

    logger.info(f"Launching Strands agentic loop for date={run_date}")
    return agent(prompt)


# ---------------------------------------------------------------------------
# Mode C — Profile a single table only
# ---------------------------------------------------------------------------

def run_profile_only(cfg: dict, table_name: str) -> dict:
    """
    Run the DataProfilerAgent on a single table and print the health report.
    Useful for ad-hoc table inspection without needing any audit failures.
    """
    from validation.data_profiler import DataProfilerAgent

    athena      = cfg["athena"]
    ai          = cfg["ai"]
    profiling   = cfg["profiling"]

    # Find table-specific config (falls back to defaults)
    table_cfg = next(
        (t for t in cfg.get("tables", []) if t["name"] == table_name),
        {}
    )

    profiler = DataProfilerAgent(
        database                    = athena["database"],
        athena_output_location      = athena["output_location"],
        learning_bucket             = ai["learning_bucket"],
        aws_region                  = athena["region"],
        model_id                    = ai["model_id"],
        null_flood_delta            = profiling["null_flood_delta_pct"] / 100,
        cardinality_explosion_x     = profiling["cardinality_explosion_multiplier"],
        skew_threshold              = profiling["distribution_skew_threshold_pct"] / 100,
        boundary_explosion_x        = profiling["boundary_explosion_multiplier"],
        zero_inflation_pct          = profiling["zero_inflation_threshold_pct"] / 100,
        dup_key_pct                 = profiling["duplicate_key_threshold_pct"] / 100,
        freshness_sla_hours         = profiling["freshness_sla_hours"],
    )

    run_id = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

    logger.info(f"Profiling table: {athena['database']}.{table_name}")
    result = profiler.profile(
        table_name         = table_name,
        run_id             = run_id,
        key_columns        = table_cfg.get("key_columns"),
        primary_key_column = table_cfg.get("primary_key_column"),
        metric_columns     = table_cfg.get("metric_columns"),
        timestamp_column   = table_cfg.get("timestamp_column"),
        partition_column   = table_cfg.get("partition_column"),
        freshness_sla_hours= table_cfg.get("freshness_sla_hours", profiling["freshness_sla_hours"]),
    )
    return result.to_dict()


# ---------------------------------------------------------------------------
# Output / printing
# ---------------------------------------------------------------------------

SEVERITY_COLOURS = {
    "CRITICAL": "\033[91m",   # red
    "HIGH":     "\033[93m",   # yellow
    "MEDIUM":   "\033[94m",   # blue
    "LOW":      "\033[37m",   # grey
}
RESET = "\033[0m"


def colour(text: str, severity: str) -> str:
    return f"{SEVERITY_COLOURS.get(severity, '')}{text}{RESET}"


def print_profile_report(report: dict, cfg: dict) -> None:
    out = cfg.get("output", {})
    print("\n" + "═" * 70)
    print(f"  DATA PROFILE REPORT — {report['database_name']}.{report['table_name']}")
    print("═" * 70)
    score = report["data_health_score"]
    bar   = "█" * (score // 5) + "░" * (20 - score // 5)
    score_colour = "CRITICAL" if score < 40 else ("HIGH" if score < 60 else ("MEDIUM" if score < 80 else "LOW"))
    print(f"\n  Health Score : {colour(f'{score}/100  [{bar}]', score_colour)}")
    print(f"  Run ID       : {report['run_id']}")
    print(f"  Profiled At  : {report['profiled_at']}")
    print(f"  Schema Drift : {'⚠  YES' if report['schema_drift_detected'] else '✓  No'}")

    kpi = report.get("kpi_snapshot", {})
    print(f"\n  {'─'*30} KPIs {'─'*30}")
    print(f"  Row Count      : {kpi.get('row_count', 'n/a'):,}")
    rc_delta = kpi.get("row_count_delta_pct")
    if rc_delta is not None:
        arrow = "▲" if rc_delta >= 0 else "▼"
        print(f"  Row Δ vs prev  : {arrow} {abs(rc_delta):.1%}")
    freshness = kpi.get("freshness_hours")
    if freshness is not None:
        print(f"  Freshness      : {freshness:.1f}h ago  (latest: {kpi.get('latest_partition_value','?')})")
    dup_pct = kpi.get("duplicate_key_pct")
    if dup_pct is not None:
        print(f"  Duplicate Keys : {dup_pct:.2%}")

    anomalies = report.get("anomalies", [])
    if anomalies and out.get("print_anomalies", True):
        print(f"\n  {'─'*30} ANOMALIES ({len(anomalies)}) {'─'*25}")
        for a in anomalies:
            sev  = a["severity"]
            col  = f"[{a['column_name']}]" if a.get("column_name") else "[table]"
            print(f"\n  {colour(f'● {sev}', sev)}  {a['anomaly_type']}  {col}")
            print(f"    {a['description']}")
            print(f"    Observed : {a['observed_value']}   Expected : {a.get('expected_range','?')}")
            print(f"    Action   : {a['recommended_action']}")
            if a.get("detection_sql"):
                print(f"    SQL      : {a['detection_sql'][:120]}...")
    else:
        print(f"\n  ✓ No anomalies detected")

    if report.get("ai_summary"):
        print(f"\n  {'─'*30} AI SUMMARY {'─'*27}")
        print(f"  {report['ai_summary']}")
    print("═" * 70 + "\n")


def print_audit_report(report: dict, cfg: dict) -> None:
    out = cfg.get("output", {})
    print("\n" + "═" * 70)
    print(f"  AUDIT ANALYSIS REPORT — {report.get('date', 'N/A')}")
    print("═" * 70)

    summary = report.get("summary", {})
    print(f"\n  Total failures processed : {report.get('total_failures', 0)}")
    print(f"  Actions dispatched       : {summary.get('actions_dispatched', 0)}")
    print(f"  Successful dispatches    : {summary.get('successful_dispatches', 0)}")

    sev = summary.get("severity_breakdown", {})
    if sev:
        print(f"\n  Severity breakdown:")
        for s, c in sorted(sev.items(), key=lambda x: ["CRITICAL","HIGH","MEDIUM","LOW"].index(x[0]) if x[0] in ["CRITICAL","HIGH","MEDIUM","LOW"] else 99):
            print(f"    {colour(s, s):<20}  {c}")

    actions = summary.get("action_breakdown", {})
    if actions and out.get("print_actions", True):
        print(f"\n  Actions taken:")
        for action, count in actions.items():
            print(f"    {action:<20}  {count}")

    tables = summary.get("tables_affected", [])
    if tables:
        print(f"\n  Tables affected : {', '.join(tables)}")

    if out.get("print_actions", True):
        dispatched = report.get("actions_taken", [])
        if dispatched:
            print(f"\n  {'─'*30} PER-RECORD ACTIONS {'─'*19}")
            for d in dispatched[:20]:   # cap display at 20
                ok = "✓" if d.get("success") else "✗"
                print(f"  {ok}  {d['action_taken']:<20}  {d['rule_name']:<30}  {d['table_name']}")
                if not d.get("success") and d.get("error_message"):
                    print(f"       Error: {d['error_message'][:80]}")

    print("═" * 70 + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Strands Audit AI Analysis + Data Profiling")
    p.add_argument("--date",         default=str(date.today()), help="Date to analyse (YYYY-MM-DD)")
    p.add_argument("--severity",     default=None,              help="Min severity: LOW|MEDIUM|HIGH|CRITICAL")
    p.add_argument("--mode",         default="direct",          choices=["direct","agent"], help="Execution mode")
    p.add_argument("--profile-only", action="store_true",       help="Profile a table without running audit")
    p.add_argument("--table",        default=None,              help="Table name for --profile-only mode")
    p.add_argument("--config",       default=str(CONFIG_PATH),  help="Path to config JSON")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg  = load_config(Path(args.config))

    # Override severity from CLI or fall back to config default
    severity = args.severity or cfg["ai"].get("severity_filter", "MEDIUM")

    if args.profile_only:
        table = args.table or cfg["athena"]["table"]
        report = run_profile_only(cfg, table)
        print_profile_report(report, cfg)

    elif args.mode == "agent":
        response = run_agent(cfg, args.date, severity)
        print("\n" + "═" * 70)
        print("  STRANDS AGENT RESPONSE")
        print("═" * 70)
        print(response)
        print("═" * 70 + "\n")

    else:
        report = run_direct(cfg, args.date, severity)
        print_audit_report(report, cfg)


if __name__ == "__main__":
    main()
