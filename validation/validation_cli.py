"""
Strands Validation CLI — interactive terminal for NL → Athena queries
and failed-validation analysis.

Usage:
    python -m validation.validation_cli [OPTIONS]

Options:
    --database     Athena / Glue database name          (default: validation_db)
    --s3-output    S3 location for Athena query results (required)
    --bucket       S3 bucket for learning data          (default: strands-etl-learning)
    --region       AWS region                           (default: us-east-1)
    --workgroup    Athena workgroup                     (default: primary)
    --no-color     Disable ANSI colour output

Commands available at the interactive prompt:
    query  <natural language>   NL → SQL → Athena, display results
    sql    <sql statement>      Execute raw SQL directly
    analyze <record_id>         Analyse a single failed validation record
    batch                       Analyse all recent failed records
    history [N]                 Show the N most recent Athena queries (default 10)
    schema [table]              Print the Glue Catalog schema
    feedback <record_id>        Submit human feedback on a classification
    patterns                    Extract learning patterns from resolved outcomes
    reset                       Clear conversation history
    help                        Show this help text
    exit / quit                 Exit the CLI
"""

import argparse
import json
import logging
import os
import sys
import textwrap
from datetime import datetime
from typing import List, Dict, Any, Optional

# Optional readline support for arrow-key history
try:
    import readline
    _HAS_READLINE = True
except ImportError:
    _HAS_READLINE = False

from validation.athena_query_engine import AthenaQueryEngine
from validation.validation_agent import ValidationAnalysisAgent
from validation.models import (
    ValidationRecord,
    AthenaQueryResult,
    ValidationClassification,
    RecommendedAction,
)

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ANSI colour helpers
# ---------------------------------------------------------------------------

class _Colors:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    RED     = "\033[31m"
    GREEN   = "\033[32m"
    YELLOW  = "\033[33m"
    CYAN    = "\033[36m"
    MAGENTA = "\033[35m"
    WHITE   = "\033[97m"
    DIM     = "\033[2m"


_USE_COLOR = True  # toggled by --no-color


def _c(text: str, *codes: str) -> str:
    if not _USE_COLOR:
        return text
    return "".join(codes) + text + _Colors.RESET


def _header(text: str) -> str:
    return _c(f"\n{'─' * 60}\n  {text}\n{'─' * 60}", _Colors.BOLD, _Colors.CYAN)


def _ok(text: str) -> str:
    return _c(f"✓ {text}", _Colors.GREEN)


def _warn(text: str) -> str:
    return _c(f"⚠ {text}", _Colors.YELLOW)


def _err(text: str) -> str:
    return _c(f"✗ {text}", _Colors.RED)


def _dim(text: str) -> str:
    return _c(text, _Colors.DIM)


# ---------------------------------------------------------------------------
# Result display helpers
# ---------------------------------------------------------------------------

def _print_athena_result(result: AthenaQueryResult) -> None:
    if result.status != "SUCCEEDED":
        print(_err(f"Query failed: {result.error_message}"))
        return

    scanned_mb = result.data_scanned_bytes / (1024 * 1024)
    elapsed_s  = result.execution_time_ms / 1000

    print(_dim(f"\nQuery ID : {result.query_execution_id}"))
    print(_dim(f"Elapsed  : {elapsed_s:.2f}s  |  Scanned: {scanned_mb:.2f} MB  |  Rows: {result.row_count}"))

    if not result.rows:
        print(_warn("No rows returned."))
        return

    # Simple aligned table
    col_widths = {c: len(c) for c in result.columns}
    for row in result.rows[:200]:
        for c in result.columns:
            col_widths[c] = max(col_widths[c], len(str(row.get(c, ""))))

    header_cells = [c.ljust(col_widths[c]) for c in result.columns]
    sep_cells    = ["-" * col_widths[c]    for c in result.columns]

    print()
    print(_c("  " + "  ".join(header_cells), _Colors.BOLD))
    print("  " + "  ".join(sep_cells))
    for row in result.rows[:200]:
        cells = [str(row.get(c, "")).ljust(col_widths[c]) for c in result.columns]
        print("  " + "  ".join(cells))

    if result.row_count > 200:
        print(_dim(f"  … {result.row_count - 200} more rows not shown."))


def _classification_badge(cls: str) -> str:
    colors = {
        "TRUE_FAILURE":        _Colors.RED,
        "FALSE_POSITIVE":      _Colors.GREEN,
        "NEEDS_INVESTIGATION": _Colors.YELLOW,
    }
    return _c(f" {cls} ", colors.get(cls, _Colors.WHITE), _Colors.BOLD)


def _print_analysis(result: Dict[str, Any]) -> None:
    """Pretty-print a single AnalysisResult dict."""
    cls         = result.get("classification", "?")
    confidence  = result.get("confidence", 0.0)
    conf_label  = "HIGH" if confidence >= 0.85 else "MEDIUM" if confidence >= 0.60 else "LOW"
    action      = result.get("recommended_action", "?")
    explanation = result.get("explanation", "")
    causes      = result.get("root_causes", [])
    steps       = result.get("suggested_next_steps", [])
    sql         = result.get("validation_sql")

    print(_header(f"Analysis: {result.get('record_id', 'unknown')}"))
    print(f"  Classification : {_classification_badge(cls)}")
    print(f"  Confidence     : {_c(conf_label, _Colors.BOLD)}  ({confidence:.0%})")
    print(f"  Action         : {_c(action, _Colors.MAGENTA, _Colors.BOLD)}")
    print(f"\n  Explanation:\n  {textwrap.fill(explanation, width=72, subsequent_indent='  ')}")

    if causes:
        print("\n  Root Causes:")
        for i, c in enumerate(causes, 1):
            print(f"    {i}. {c}")

    if steps:
        print("\n  Suggested Next Steps:")
        for i, s in enumerate(steps, 1):
            print(f"    {i}. {s}")

    if sql:
        print("\n  Validation SQL:")
        for line in sql.strip().splitlines():
            print(f"    {_c(line, _Colors.CYAN)}")

    similar = result.get("similar_historical_failures", [])
    if similar:
        print(f"\n  Similar Historical Failures: {len(similar)} found")


def _print_batch_summary(summary: Dict[str, Any]) -> None:
    print(_header("Batch Analysis Summary"))
    tf  = summary.get("true_failure_count", 0)
    fp  = summary.get("false_positive_count", 0)
    ni  = summary.get("needs_investigation_count", 0)
    dqs = summary.get("data_quality_score", "?")

    print(f"  {_c('TRUE_FAILURE', _Colors.RED, _Colors.BOLD)}         : {tf}")
    print(f"  {_c('FALSE_POSITIVE', _Colors.GREEN, _Colors.BOLD)}       : {fp}")
    print(f"  {_c('NEEDS_INVESTIGATION', _Colors.YELLOW, _Colors.BOLD)}: {ni}")
    print(f"  Data Quality Score   : {_c(str(dqs), _Colors.BOLD)} / 100")

    issues = summary.get("top_issues", [])
    if issues:
        print("\n  Top Issues:")
        for i, issue in enumerate(issues, 1):
            print(f"    {i}. {issue}")

    patterns = summary.get("recurring_patterns", [])
    if patterns:
        print("\n  Recurring Patterns:")
        for p in patterns:
            print(f"    • {p}")

    exec_summary = summary.get("executive_summary", "")
    if exec_summary:
        print(f"\n  Executive Summary:\n  {textwrap.fill(exec_summary, width=72, subsequent_indent='  ')}")


# ---------------------------------------------------------------------------
# CLI session state
# ---------------------------------------------------------------------------

class _Session:
    def __init__(self) -> None:
        self.query_history: List[Dict[str, Any]] = []
        self.last_sql: str = ""


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def _cmd_query(
    args: str,
    engine: AthenaQueryEngine,
    session: _Session,
) -> None:
    nl_query = args.strip()
    if not nl_query:
        print(_warn("Usage: query <natural language question>"))
        return

    print(_dim(f"Translating to SQL…"))
    sql_resp = engine.nl_to_sql(nl_query)

    if "error" in sql_resp:
        print(_err(f"SQL generation failed: {sql_resp['error']}"))
        return

    sql = sql_resp.get("sql", "")
    explanation = sql_resp.get("explanation", "")
    intent = sql_resp.get("assumed_intent", "")

    print(_dim(f"\nSQL: {sql[:300]}"))
    if explanation:
        print(_dim(f"→  {explanation}"))

    print(_dim("\nExecuting…"))
    result = engine.execute_sql(sql, nl_query=nl_query)

    session.query_history.append({
        "timestamp": datetime.utcnow().isoformat(),
        "nl_query": nl_query,
        "sql": sql,
        "rows": result.row_count,
        "status": result.status,
    })
    session.last_sql = sql

    _print_athena_result(result)


def _cmd_sql(args: str, engine: AthenaQueryEngine, session: _Session) -> None:
    sql = args.strip()
    if not sql:
        print(_warn("Usage: sql <SQL statement>"))
        return
    print(_dim("Executing…"))
    result = engine.execute_sql(sql)
    session.query_history.append({
        "timestamp": datetime.utcnow().isoformat(),
        "nl_query": "(direct sql)",
        "sql": sql,
        "rows": result.row_count,
        "status": result.status,
    })
    session.last_sql = sql
    _print_athena_result(result)


def _cmd_analyze(
    args: str,
    engine: AthenaQueryEngine,
    agent: ValidationAnalysisAgent,
) -> None:
    """
    Fetch a failed validation record by ID from Athena, then run the agent.
    Falls back to a prompt-driven flow if the record is not found.
    """
    record_id = args.strip()
    if not record_id:
        print(_warn("Usage: analyze <record_id>"))
        return

    # Try to fetch the record from Athena
    result = engine.execute_sql(
        f"SELECT * FROM {engine.database}.failed_validations "
        f"WHERE record_id = '{record_id}' LIMIT 1"
    )

    if result.status != "SUCCEEDED" or not result.rows:
        print(_warn(f"Record '{record_id}' not found in Athena. Enter details manually."))
        record = _prompt_manual_record(record_id)
    else:
        record = ValidationRecord.from_athena_row(result.rows[0])

    print(_dim("Running validation analysis agent…"))
    analysis = agent.analyze(record)
    _print_analysis(analysis.to_dict())


def _prompt_manual_record(record_id: str) -> ValidationRecord:
    """Interactively collect a ValidationRecord from the user."""
    print(_c("\n  Enter record details (press Enter to skip optional fields):", _Colors.BOLD))

    def _ask(prompt: str, default: str = "") -> str:
        value = input(f"  {prompt} [{default}]: ").strip()
        return value if value else default

    return ValidationRecord(
        record_id=record_id,
        rule_name=_ask("rule_name"),
        table_name=_ask("table_name"),
        column_name=_ask("column_name"),
        failed_value=_ask("failed_value"),
        expected_constraint=_ask("expected_constraint"),
        failure_timestamp=_ask("failure_timestamp", datetime.utcnow().isoformat()),
        run_id=_ask("run_id", "manual"),
        pipeline_name=_ask("pipeline_name (optional)"),
        severity=_ask("severity [LOW|MEDIUM|HIGH|CRITICAL]", "MEDIUM"),
        rule_type=_ask("rule_type [NOT_NULL|RANGE|REGEX|REFERENTIAL|BUSINESS]", "UNKNOWN"),
        row_count_failed=int(_ask("row_count_failed", "1") or "1"),
        total_row_count=int(_ask("total_row_count", "0") or "0"),
    )


def _cmd_batch(
    engine: AthenaQueryEngine,
    agent: ValidationAnalysisAgent,
) -> None:
    """Fetch the last 50 failed validations and run batch analysis."""
    print(_dim("Fetching recent failed validations…"))
    result = engine.execute_sql(
        f"SELECT * FROM {engine.database}.failed_validations "
        f"ORDER BY failure_timestamp DESC LIMIT 50"
    )

    if result.status != "SUCCEEDED":
        print(_err(f"Could not fetch failed validations: {result.error_message}"))
        return

    if not result.rows:
        print(_warn("No failed validations found."))
        return

    records = [ValidationRecord.from_athena_row(row) for row in result.rows]
    print(_dim(f"Analysing {len(records)} records…"))

    batch_output = agent.analyze_batch(records)

    # Print per-record results
    for r in batch_output.get("results", []):
        _print_analysis(r)

    # Print executive summary
    _print_batch_summary(batch_output.get("summary", {}))


def _cmd_history(args: str, session: _Session) -> None:
    n = 10
    if args.strip().isdigit():
        n = int(args.strip())

    recent = session.query_history[-n:]
    if not recent:
        print(_dim("No query history yet."))
        return

    print(_header(f"Last {len(recent)} Queries"))
    for i, entry in enumerate(reversed(recent), 1):
        ts    = entry.get("timestamp", "")[:19]
        nl    = entry.get("nl_query", "")[:60]
        rows  = entry.get("rows", "?")
        status = entry.get("status", "?")
        color = _Colors.GREEN if status == "SUCCEEDED" else _Colors.RED
        print(f"  {_c(str(i).rjust(2), _Colors.DIM)}  {_c(ts, _Colors.DIM)}  "
              f"{_c(status.ljust(9), color)}  rows={str(rows).ljust(6)}  {nl}")


def _cmd_schema(args: str, engine: AthenaQueryEngine) -> None:
    table_filter = args.strip().lower()
    schema = engine.get_schema()

    if not schema:
        print(_warn("Schema is empty or could not be loaded from Glue Catalog."))
        return

    for tname, cols in sorted(schema.items()):
        if table_filter and table_filter not in tname.lower():
            continue
        print(f"\n  {_c(tname, _Colors.BOLD, _Colors.CYAN)}")
        for col in cols:
            partition_tag = _c(" (partition)", _Colors.DIM) if col.get("partition") else ""
            print(f"    {col['name'].ljust(32)} {_c(col['type'], _Colors.MAGENTA)}{partition_tag}")


def _cmd_feedback(args: str, agent: ValidationAnalysisAgent) -> None:
    record_id = args.strip()
    if not record_id:
        print(_warn("Usage: feedback <record_id>"))
        return

    def _ask(prompt: str, choices: Optional[List[str]] = None) -> str:
        suffix = f" ({'/'.join(choices)})" if choices else ""
        while True:
            value = input(f"  {prompt}{suffix}: ").strip()
            if not choices or value.upper() in [c.upper() for c in choices]:
                return value
            print(_warn(f"  Choose one of: {choices}"))

    print(_c(f"\n  Feedback for record: {record_id}", _Colors.BOLD))
    classification = _ask(
        "Classification assigned",
        [c.value for c in ValidationClassification],
    ).upper()
    was_correct_str = _ask("Was the classification correct?", ["yes", "no"])
    was_correct = was_correct_str.lower() == "yes"
    action_taken = _ask("Action taken", [a.value for a in RecommendedAction]).upper()
    notes = input("  Resolution notes (optional): ").strip()

    outcome = agent.submit_feedback(
        record_id=record_id,
        classification=classification,
        was_correct=was_correct,
        actual_action_taken=action_taken,
        resolution_notes=notes,
    )
    print(_ok(f"Feedback saved as outcome {outcome.outcome_id}"))


def _cmd_patterns(agent: ValidationAnalysisAgent) -> None:
    print(_dim("Extracting learning patterns from resolved outcomes…"))
    patterns = agent.extract_patterns(limit=100)

    if "message" in patterns:
        print(_warn(patterns["message"]))
        return

    print(_header("Learning Patterns"))

    fp_patterns = patterns.get("false_positive_patterns", [])
    if fp_patterns:
        print("\n  False-Positive Patterns:")
        for p in fp_patterns:
            print(f"    • [{p.get('rule_type', '?')}] {p.get('table_name', '?')}: {p.get('pattern', '?')} "
                  f"(seen {p.get('frequency', '?')}x)")

    signatures = patterns.get("true_failure_signatures", [])
    if signatures:
        print("\n  True-Failure Signatures:")
        for s in signatures:
            print(f"    • {s}")

    systemic = patterns.get("systemic_issues", [])
    if systemic:
        print("\n  Systemic Issues Detected:")
        for s in systemic:
            print(f"    ⚠ {s}")

    summary = patterns.get("learning_vector_summary", "")
    if summary:
        print(f"\n  Summary:\n  {textwrap.fill(summary, width=72, subsequent_indent='  ')}")


def _print_help() -> None:
    print(_header("Available Commands"))
    commands = [
        ("query <question>",     "Translate NL to SQL and execute against Athena"),
        ("sql <statement>",      "Execute a raw SQL statement against Athena"),
        ("analyze <record_id>",  "Analyse a single failed validation record"),
        ("batch",                "Analyse the 50 most recent failed records"),
        ("history [N]",          "Show last N queries (default 10)"),
        ("schema [table]",       "Print Glue Catalog schema (optional filter)"),
        ("feedback <record_id>", "Submit human feedback on a classification"),
        ("patterns",             "Extract learning patterns from resolved outcomes"),
        ("reset",                "Clear NL-to-SQL conversation history"),
        ("help",                 "Show this help text"),
        ("exit / quit",          "Exit the CLI"),
    ]
    for cmd, desc in commands:
        print(f"  {_c(cmd.ljust(28), _Colors.CYAN)}  {desc}")


# ---------------------------------------------------------------------------
# Main REPL
# ---------------------------------------------------------------------------

def _run_repl(engine: AthenaQueryEngine, agent: ValidationAnalysisAgent) -> None:
    session = _Session()

    banner = f"""
{_c('╔══════════════════════════════════════════════════════════╗', _Colors.CYAN, _Colors.BOLD)}
{_c('║   Strands Validation Intelligence — Interactive CLI      ║', _Colors.CYAN, _Colors.BOLD)}
{_c('╚══════════════════════════════════════════════════════════╝', _Colors.CYAN, _Colors.BOLD)}
  Database : {_c(engine.database, _Colors.YELLOW)}
  Type {_c('help', _Colors.CYAN)} for available commands or {_c('exit', _Colors.CYAN)} to quit.
"""
    print(banner)

    while True:
        try:
            raw = input(_c("strands-val> ", _Colors.BOLD, _Colors.CYAN)).strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            break

        if not raw:
            continue

        parts = raw.split(None, 1)
        cmd   = parts[0].lower()
        rest  = parts[1] if len(parts) > 1 else ""

        try:
            if cmd in ("exit", "quit"):
                print("Bye.")
                break
            elif cmd == "help":
                _print_help()
            elif cmd == "query":
                _cmd_query(rest, engine, session)
            elif cmd == "sql":
                _cmd_sql(rest, engine, session)
            elif cmd == "analyze":
                _cmd_analyze(rest, engine, agent)
            elif cmd == "batch":
                _cmd_batch(engine, agent)
            elif cmd == "history":
                _cmd_history(rest, session)
            elif cmd == "schema":
                _cmd_schema(rest, engine)
            elif cmd == "feedback":
                _cmd_feedback(rest, agent)
            elif cmd == "patterns":
                _cmd_patterns(agent)
            elif cmd == "reset":
                engine.reset_conversation()
                print(_ok("Conversation history cleared."))
            else:
                print(_warn(f"Unknown command '{cmd}'. Type 'help' for usage."))
        except Exception as exc:
            print(_err(f"Error: {exc}"))
            logger.exception("Command error")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    global _USE_COLOR

    parser = argparse.ArgumentParser(
        description="Strands Validation Intelligence CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("VALIDATION_DATABASE", "validation_db"),
        help="Athena/Glue database name (default: validation_db)",
    )
    parser.add_argument(
        "--s3-output",
        default=os.environ.get("ATHENA_S3_OUTPUT", ""),
        help="S3 location for Athena query results (e.g. s3://my-bucket/athena/)",
    )
    parser.add_argument(
        "--bucket",
        default=os.environ.get("LEARNING_BUCKET", "strands-etl-learning"),
        help="S3 bucket for learning data (default: strands-etl-learning)",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        help="AWS region (default: us-east-1)",
    )
    parser.add_argument(
        "--workgroup",
        default=os.environ.get("ATHENA_WORKGROUP", "primary"),
        help="Athena workgroup (default: primary)",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI colour output",
    )
    args = parser.parse_args()

    if args.no_color or not sys.stdout.isatty():
        _USE_COLOR = False

    if not args.s3_output:
        print(_err("--s3-output is required (e.g. s3://my-bucket/athena-results/)"))
        parser.print_help()
        sys.exit(1)

    engine = AthenaQueryEngine(
        database=args.database,
        s3_output_location=args.s3_output,
        aws_region=args.region,
        workgroup=args.workgroup,
    )
    agent = ValidationAnalysisAgent(
        learning_bucket=args.bucket,
        aws_region=args.region,
    )

    _run_repl(engine, agent)


if __name__ == "__main__":
    main()
