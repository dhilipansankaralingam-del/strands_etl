"""
run_audit_analysis.py
=====================
Trigger script — single entry point for the Strands AI Audit Analysis pipeline.

Reads everything from config/audit_analysis_config.json.

Usage
-----
    # Today's failures (all severities from config)
    python run_audit_analysis.py

    # Specific date, HIGH+ severity
    python run_audit_analysis.py --date 2024-07-01 --severity HIGH

    # Autonomous Strands agent (natural-language loop)
    python run_audit_analysis.py --date 2024-07-01 --mode agent

    # Profile tables only — no audit failures needed
    python run_audit_analysis.py --profile-only

    # Natural-language validations only
    python run_audit_analysis.py --nl-only

    # Everything in one shot
    python run_audit_analysis.py --date 2024-07-01 --full
"""

import argparse
import json
import logging
import os
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

_C = {
    "RESET":  "\033[0m",  "BOLD":   "\033[1m",
    "CYAN":   "\033[96m", "YELLOW": "\033[93m",
    "GREEN":  "\033[92m", "RED":    "\033[91m",
    "GREY":   "\033[37m", "MAGENTA":"\033[95m",
}
def _c(text, colour): return f"{_C.get(colour,'')}{text}{_C['RESET']}"


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config(path: Path = CONFIG_PATH) -> dict:
    with open(path) as f:
        return json.load(f)


def parse_db_table(full_name: str) -> tuple[str, str]:
    """Split 'database.tablename' → ('database', 'tablename')."""
    parts = full_name.split(".", 1)
    if len(parts) != 2:
        raise ValueError(f"Expected 'database.tablename', got: '{full_name}'")
    return parts[0], parts[1]


def get_table_cfg(cfg: dict, full_name: str) -> dict:
    return cfg.get("table_config", {}).get(full_name, {})


# ---------------------------------------------------------------------------
# Mode A — Direct audit analysis
# ---------------------------------------------------------------------------

def run_direct(cfg: dict, run_date: str, severity: str, csv_exporter) -> dict:
    from validation.audit_action_agent import AuditActionAgent
    from validation.prompt_logger import get_logger

    db, tbl       = parse_db_table(cfg["audit_table"])
    ai            = cfg["ai"]
    rules         = cfg.get("table_rules", {})
    athena_output = cfg["athena_output_location"]
    learning_bkt  = cfg["learning_bucket"]
    region        = cfg["aws_region"]

    agent = AuditActionAgent(
        database         = db,
        table            = tbl,
        athena_output    = athena_output,
        learning_bucket  = learning_bkt,
        aws_region       = region,
        model_id         = ai["model_id"],
        configured_rules = rules,
    )

    _banner(f"AUDIT ANALYSIS  {run_date}  ≥{severity}", "CYAN")
    report = agent.run_for_date(
        date            = run_date,
        severity_filter = severity,
        limit           = ai["max_records_per_run"],
    )

    # Write per-record rows to CSV
    plogger = get_logger()
    for action in report.get("actions_taken", []):
        csv_exporter.write_audit_finding(
            record_dict   = {"table_name": action.get("table_name",""), "record_id": action.get("record_id",""),
                             "rule_name": action.get("rule_name",""), "failure_timestamp": ""},
            analysis_dict = {},
            dispatch_dict = action,
        )

    return report


# ---------------------------------------------------------------------------
# Mode B — Autonomous Strands agent
# ---------------------------------------------------------------------------

def run_agent(cfg: dict, run_date: str, severity: str) -> str:
    from validation.audit_action_agent import build_audit_agent

    db, tbl       = parse_db_table(cfg["audit_table"])
    ai            = cfg["ai"]

    agent = build_audit_agent(
        database        = db,
        table           = tbl,
        athena_output   = cfg["athena_output_location"],
        learning_bucket = cfg["learning_bucket"],
        aws_region      = cfg["aws_region"],
        model_id        = ai["model_id"],
    )

    prompt = (
        f"Analyse all {severity} and higher failures from the audit table for {run_date}. "
        f"For each failure: profile the affected table, classify using any configured rules, "
        f"and dispatch the recommended action. "
        f"Escalate any TEMPORAL_ANOMALY, SCHEMA_DRIFT, or ZERO_INFLATION anomalies immediately. "
        f"Generate fix SQL for DATA_CORRECTION actions. "
        f"End with a complete executive summary."
    )

    _banner(f"STRANDS AGENT  {run_date}", "CYAN")
    return agent(prompt)


# ---------------------------------------------------------------------------
# Mode C — Data profiling only
# ---------------------------------------------------------------------------

def run_profiling(cfg: dict, csv_exporter, run_date: str = "") -> list:
    from validation.data_profiler import DataProfilerAgent
    from validation.prompt_logger import get_logger

    profiling = cfg["profiling"]
    ai        = cfg["ai"]
    results   = []

    for full_name in cfg.get("profile_tables", []):
        db, tbl  = parse_db_table(full_name)
        tc       = get_table_cfg(cfg, full_name)

        profiler = DataProfilerAgent(
            database                = db,
            athena_output_location  = cfg["athena_output_location"],
            learning_bucket         = cfg["learning_bucket"],
            aws_region              = cfg["aws_region"],
            model_id                = ai["model_id"],
            null_flood_delta        = profiling["null_flood_delta_pct"] / 100,
            cardinality_explosion_x = profiling["cardinality_explosion_multiplier"],
            skew_threshold          = profiling["distribution_skew_threshold_pct"] / 100,
            boundary_explosion_x    = profiling["boundary_explosion_multiplier"],
            zero_inflation_pct      = profiling["zero_inflation_threshold_pct"] / 100,
            dup_key_pct             = profiling["duplicate_key_threshold_pct"] / 100,
            freshness_sla_hours     = profiling["freshness_sla_hours"],
            ml_config               = cfg.get("ml_anomaly_scoring"),
        )

        run_id = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        _banner(f"PROFILING  {full_name}", "YELLOW")

        result = profiler.profile(
            table_name          = tbl,
            run_id              = run_id,
            key_columns         = tc.get("key_columns"),
            primary_key_column  = tc.get("primary_key_column"),
            metric_columns      = tc.get("metric_columns"),
            timestamp_column    = tc.get("timestamp_column"),
            partition_column    = tc.get("partition_column"),
            freshness_sla_hours = tc.get("freshness_sla_hours", profiling["freshness_sla_hours"]),
            profiling_columns   = tc.get("profiling_columns"),
            date_filter_column  = tc.get("date_filter_column"),
            run_date            = run_date or None,
            lookback_days       = tc.get("profiling_lookback_days",
                                         profiling.get("default_lookback_days", 0)),
        )

        _print_profile(result)

        for anomaly in result.anomalies:
            csv_exporter.write_profile_anomaly(
                table_name     = full_name,
                anomaly_dict   = anomaly.to_dict(),
                health_score   = result.data_health_score,
                schema_drift   = result.schema_drift_detected,
            )

        results.append(result)

    return results


# ---------------------------------------------------------------------------
# Mode D½ — Auto-ruleset generation
# ---------------------------------------------------------------------------

def run_auto_rulesets(cfg: dict, run_date: str) -> list:
    from validation.auto_ruleset_generator import AutoRulesetGenerator

    ruleset_cfg = cfg.get("auto_ruleset_generation", {})
    if not ruleset_cfg.get("enabled", False):
        print(_c("  auto_ruleset_generation disabled in config — skipping.", "GREY"))
        return []

    results_tbl_full = ruleset_cfg.get("results_table", cfg["audit_table"])
    rs_db, rs_tbl    = parse_db_table(results_tbl_full)

    gen = AutoRulesetGenerator(
        database        = rs_db,
        results_table   = rs_tbl,
        athena_output   = cfg["athena_output_location"],
        learning_bucket = cfg["learning_bucket"],
        aws_region      = cfg["aws_region"],
        ruleset_config  = ruleset_cfg,
    )

    _banner("AUTO-RULESET GENERATION", "YELLOW")
    summaries = []
    for full_name in cfg.get("profile_tables", []):
        tc = cfg.get("table_config", {}).get(full_name, {})
        summary = gen.run(target_table=full_name, table_cfg=tc, run_date=run_date)
        summaries.append(summary)
        _print_ruleset_summary(full_name, summary)
    return summaries


# ---------------------------------------------------------------------------
# Mode D — Natural Language validations
# ---------------------------------------------------------------------------

def run_nl_validations(cfg: dict, csv_exporter) -> list:
    from validation.nl_validator import NLValidator

    nl_items = cfg.get("natural_language_validations", [])
    if not nl_items:
        print(_c("  No natural_language_validations configured — skipping.", "GREY"))
        return []

    _banner("NATURAL LANGUAGE VALIDATIONS", "MAGENTA")

    validator = NLValidator(
        athena_output = cfg["athena_output_location"],
        aws_region    = cfg["aws_region"],
        model_id      = cfg["ai"]["model_id"],
    )

    findings = validator.run_all(nl_items, cfg)

    for f in findings:
        csv_exporter.write_nl_finding(f.to_csv_row())

    return findings


# ---------------------------------------------------------------------------
# Console output helpers
# ---------------------------------------------------------------------------

def _banner(title: str, colour: str = "CYAN") -> None:
    width = 72
    print()
    print(_c("╔" + "═" * (width - 2) + "╗", colour))
    print(_c(f"║  {title:<{width - 4}}║", colour))
    print(_c("╚" + "═" * (width - 2) + "╝", colour))


def _print_profile(result) -> None:
    score = result.data_health_score
    bar   = "█" * (score // 5) + "░" * (20 - score // 5)
    sc    = "RED" if score < 40 else ("YELLOW" if score < 70 else "GREEN")
    print(f"\n  {_c(f'Health Score: {score}/100  [{bar}]', sc)}")
    print(f"  Schema Drift : {'⚠  YES' if result.schema_drift_detected else '✓  No'}")
    print(f"  Anomalies    : {len(result.anomalies)}"
          f"  (critical={len(result.critical_anomalies())})")
    if result.ai_summary:
        print(f"  AI Summary   : {result.ai_summary[:180]}")

    SEV_C = {"CRITICAL": "RED", "HIGH": "YELLOW", "MEDIUM": "CYAN", "LOW": "GREY"}
    for a in result.anomalies:
        c = SEV_C.get(a.severity.value, "GREY")
        col = f"[{a.column_name}]" if a.column_name else "[table]"
        print(f"\n    {_c(f'● {a.severity.value}', c)}  {a.anomaly_type.value}  {col}")
        print(f"      {a.description}")
        print(f"      → {a.recommended_action}")


def _print_audit_summary(report: dict) -> None:
    summary = report.get("summary", {})
    _banner("AUDIT SUMMARY", "GREEN")
    print(f"  Total processed  : {report.get('total_failures', 0)}")
    print(f"  Actions taken    : {summary.get('actions_dispatched', 0)}")
    print(f"  Successes        : {summary.get('successful_dispatches', 0)}")

    sev = summary.get("severity_breakdown", {})
    if sev:
        print(f"\n  Severity breakdown:")
        for s in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
            if s in sev:
                c = "RED" if s == "CRITICAL" else ("YELLOW" if s == "HIGH" else "CYAN")
                print(f"    {_c(s, c):<24}  {sev[s]}")

    actions = summary.get("action_breakdown", {})
    if actions:
        print(f"\n  Actions taken:")
        for action, cnt in actions.items():
            print(f"    {action:<22}  {cnt}")

    dispatched = report.get("actions_taken", [])
    if dispatched:
        print(f"\n  Per-record:")
        for d in dispatched[:25]:
            ok  = _c("✓", "GREEN") if d.get("success") else _c("✗", "RED")
            act = _c(d.get("action_taken",""), "YELLOW")
            print(f"    {ok}  {act:<20}  {d.get('rule_name',''):<30}  {d.get('table_name','')}")


def _print_ruleset_summary(table: str, summary: dict) -> None:
    if summary.get("skipped") or not summary.get("enabled", True):
        return
    if summary.get("error"):
        print(_c(f"  ✗  {table}: {summary['error']}", "RED"))
        return
    total = summary.get("rules_generated", 0)
    passed = summary.get("pass", 0)
    failed = summary.get("fail", 0)
    sc = "GREEN" if failed == 0 else ("YELLOW" if failed < total // 2 else "RED")
    print(f"\n  {_c(table, 'CYAN')}  — {total} rules  "
          f"{_c(str(passed)+' PASS', 'GREEN')}  {_c(str(failed)+' FAIL', sc)}")
    for r in summary.get("results", []):
        status_c = "GREEN" if r["status"] == "PASS" else "RED"
        col = f"[{r['column_name']}]" if r.get("column_name") else "[table]"
        print(f"    {_c(r['status'], status_c):<16}  {r['rule_name']:<40}  {col}")


def _print_nl_summary(findings: list) -> None:
    if not findings:
        return
    _banner("NL VALIDATION SUMMARY", "MAGENTA")
    VERDICT_C = {"PASS": "GREEN", "FAIL": "RED", "WARNING": "YELLOW", "INCONCLUSIVE": "GREY"}
    for f in findings:
        c = VERDICT_C.get(f.verdict, "GREY")
        print(f"  {_c(f.verdict, c):<20}  {f.nl_query[:60]}")
        print(f"    {f.explanation[:120]}")


def _print_token_summary(plogger) -> None:
    plogger.print_cost_summary()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Strands Audit AI Analysis + Data Profiling")
    p.add_argument("--date",         default=str(date.today()), help="Date to analyse (YYYY-MM-DD)")
    p.add_argument("--severity",     default=None,  help="Min severity: LOW|MEDIUM|HIGH|CRITICAL")
    p.add_argument("--mode",         default="direct", choices=["direct", "agent"], help="Execution mode")
    p.add_argument("--profile-only",  action="store_true", help="Run data profiling only")
    p.add_argument("--nl-only",       action="store_true", help="Run NL validations only")
    p.add_argument("--ruleset-only",  action="store_true", help="Run auto-ruleset generation only")
    p.add_argument("--full",          action="store_true", help="Run audit + profiling + NL + rulesets")
    p.add_argument("--config",       default=str(CONFIG_PATH), help="Path to config JSON")
    return p.parse_args()


def main() -> None:
    args    = parse_args()
    cfg     = load_config(Path(args.config))
    out_cfg = cfg.get("output", {})
    ai_cfg  = cfg["ai"]

    severity = args.severity or ai_cfg.get("severity_filter", "MEDIUM")

    # Boot the shared prompt logger
    from validation.prompt_logger import reset_logger
    plogger = reset_logger(
        preview_chars  = out_cfg.get("prompt_preview_chars", 800),
        log_responses  = out_cfg.get("log_responses_to_console", True),
    )

    from validation.csv_exporter import CSVExporter
    from validation.html_reporter import HTMLReporter
    from validation.email_sender  import EmailSender

    csv_dir     = out_cfg.get("csv_directory", "output")
    report_cfg  = cfg.get("reporting", {})
    reporter    = HTMLReporter(
        run_date    = args.date,
        config_path = args.config,
        pipeline    = cfg.get("pipeline_name", "Strands ETL Audit"),
    )

    profile_results: list = []
    nl_findings:     list = []
    audit_report:    dict = {}
    ruleset_summaries: list = []

    with CSVExporter(output_dir=csv_dir, run_date=args.date) as csv_exp:

        # ── Profile tables
        if args.profile_only or args.full:
            profile_results = run_profiling(cfg, csv_exp, run_date=args.date)

        # ── Auto-ruleset generation
        if args.ruleset_only or args.full:
            ruleset_summaries = run_auto_rulesets(cfg, args.date)

        # ── NL validations
        if args.nl_only or args.full:
            nl_findings = run_nl_validations(cfg, csv_exp)
            _print_nl_summary(nl_findings)

        # ── Audit analysis (default / --full)
        if not args.profile_only and not args.nl_only and not args.ruleset_only:
            if args.mode == "agent":
                response = run_agent(cfg, args.date, severity)
                _banner("STRANDS AGENT RESPONSE", "GREEN")
                print(response)
            else:
                audit_report = run_direct(cfg, args.date, severity, csv_exp)
                _print_audit_summary(audit_report)

            # Also run NL validations in full mode
            if args.full:
                nl_findings = run_nl_validations(cfg, csv_exp)
                _print_nl_summary(nl_findings)

        # ── Write token usage CSV
        token_usages = plogger.to_dicts()
        for u in token_usages:
            csv_exp.write_token_usage(u)

        # ── Build HTML report (while CSV files are still open so paths exist)
        reporter.add_audit_report(audit_report)
        reporter.add_profile_results(profile_results)
        reporter.add_nl_findings(nl_findings)
        reporter.add_token_usages(token_usages)
        reporter.add_ruleset_summaries(ruleset_summaries)

        html_path = Path(csv_dir) / f"dq_report_{args.date}.html"
        reporter.save(str(html_path))
        _banner("HTML REPORT", "CYAN")
        print(f"  Report saved → {_c(str(html_path), 'GREEN')}")

        # ── Email report
        email_cfg = report_cfg.get("email", {})
        if email_cfg.get("enabled", False):
            _banner("SENDING EMAIL", "MAGENTA")
            sender = EmailSender(email_cfg, aws_region=cfg["aws_region"])
            attach_paths = (
                [str(p) for p in Path(csv_dir).glob(f"*_{args.date}.csv")]
                if email_cfg.get("attach_csvs", False)
                else []
            )
            ok = sender.send(
                html_body        = reporter.render(),
                run_date         = args.date,
                attachment_paths = attach_paths,
            )
            if ok:
                print(_c(f"  Email sent to {email_cfg.get('to_addresses','')}", "GREEN"))
            else:
                print(_c("  Email send failed — check SES config and logs.", "RED"))
        else:
            print(_c("  Email disabled (set reporting.email.enabled=true to enable).", "GREY"))

    # ── Token + cost summary (printed after CSV closes so files are flushed)
    _print_token_summary(plogger)


if __name__ == "__main__":
    main()
