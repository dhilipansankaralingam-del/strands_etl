"""
slack/block_kit.py
==================
Slack Block Kit message builders for every ETL agent response type.

Blocks reference: https://api.slack.com/reference/block-kit/blocks
"""

from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Colour palette (for section accessories)
# ---------------------------------------------------------------------------
_COLOR = {
    "critical": "#dc3545",
    "ok":       "#28a745",
    "warning":  "#ffc107",
    "info":     "#4e73df",
    "neutral":  "#6c757d",
}


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def _md(text: str) -> Dict:
    return {"type": "mrkdwn", "text": text}


def _plain(text: str) -> Dict:
    return {"type": "plain_text", "text": text, "emoji": True}


def _divider() -> Dict:
    return {"type": "divider"}


def _header(text: str) -> Dict:
    return {"type": "header", "text": _plain(text)}


def _section(text: str, accessory: Optional[Dict] = None) -> Dict:
    block: Dict[str, Any] = {"type": "section", "text": _md(text)}
    if accessory:
        block["accessory"] = accessory
    return block


def _fields(*texts: str) -> Dict:
    return {"type": "section", "fields": [_md(t) for t in texts]}


def _context(*texts: str) -> Dict:
    return {"type": "context", "elements": [_md(t) for t in texts]}


def _button(text: str, action_id: str, url: str = "", value: str = "") -> Dict:
    btn: Dict[str, Any] = {
        "type": "button",
        "text": _plain(text),
        "action_id": action_id,
    }
    if url:
        btn["url"] = url
    if value:
        btn["value"] = value
    return btn


def _actions(*buttons) -> Dict:
    return {"type": "actions", "elements": list(buttons)}


# ---------------------------------------------------------------------------
# Immediate acknowledgement (sent in <3 s while processor runs async)
# ---------------------------------------------------------------------------

def ack_message(command: str, args: str, user_id: str) -> Dict[str, Any]:
    return {
        "response_type": "ephemeral",
        "blocks": [
            _section(
                f":hourglass_flowing_sand: *Running `{command} {args}`* …\n"
                f"<@{user_id}> I'll post results here when the agent completes."
            ),
        ],
    }


def ack_unknown_command(command: str, text: str) -> Dict[str, Any]:
    return {
        "response_type": "ephemeral",
        "blocks": [
            _section(
                f":question: Unknown command or args: `{command} {text}`\n"
                "Try `/etl-help` to see available commands."
            ),
        ],
    }


def ack_error(error: str) -> Dict[str, Any]:
    return {
        "response_type": "ephemeral",
        "blocks": [
            _section(f":x: *Error:* {error}"),
        ],
    }


# ---------------------------------------------------------------------------
# Help message
# ---------------------------------------------------------------------------

def help_message() -> Dict[str, Any]:
    return {
        "response_type": "ephemeral",
        "blocks": [
            _header("Strands ETL Agent — Slack Commands"),
            _divider(),
            _section(
                "*`/etl-analyze`* — Run the AI agent analyzer on recent audit failures\n"
                "Options: `pipeline=<name>`  `hours=<N>`  `severity=CRITICAL|HIGH`\n"
                "_Example:_ `/etl-analyze pipeline=member_info_etl hours=48`"
            ),
            _divider(),
            _section(
                "*`/etl-logs`* — Analyze AWS service logs for errors\n"
                "Options: `source=<s3://path or /aws/glue/job-name>`  `hours=<N>`\n"
                "_Example:_ `/etl-logs source=/aws/glue/jobs/trigger_glue_job hours=12`"
            ),
            _divider(),
            _section(
                "*`/etl-query`* — Ask any question about your data in plain English\n"
                "_Example:_ `/etl-query which clubs have the most failures this week?`\n"
                "_Example:_ `/etl-query show z-score anomalies for club CA-001 last 60 days`"
            ),
            _divider(),
            _section(
                "*`/etl-status`* — Show the last analyzer run summary\n"
                "_Example:_ `/etl-status`"
            ),
            _divider(),
            _section(
                "*`/etl-holiday`* — Check if a date is a known US holiday\n"
                "_Example:_ `/etl-holiday 2026-12-25`"
            ),
            _context(
                ":bulb: All commands support `help` as the first arg for detailed usage.",
                ":lock: Secrets are read from AWS Secrets Manager — never paste tokens here.",
            ),
        ],
    }


# ---------------------------------------------------------------------------
# etl-analyze result
# ---------------------------------------------------------------------------

def analyze_result(report: Dict[str, Any], run_id: str) -> Dict[str, Any]:
    ai     = report.get("ai_analysis", {})
    total  = ai.get("total_analysed", 0)
    tf     = ai.get("true_failures", 0)
    fp     = ai.get("false_positives", 0)
    ni     = ai.get("needs_investigation", 0)
    conf   = ai.get("avg_confidence", 0.0)
    cost   = report.get("cost_summary", {})
    cost_usd  = cost.get("total_cost_usd", 0.0)
    tokens    = cost.get("total_tokens", 0)

    status_emoji = ":white_check_mark:" if tf == 0 else ":rotating_light:"
    status_text  = "All clear — no true failures" if tf == 0 else f"{tf} true failure(s) detected"

    exec_raw = ai.get("executive_summary", {})
    exec_text = ""
    if isinstance(exec_raw, dict):
        exec_text = exec_raw.get("executive_summary", exec_raw.get("summary", ""))
    elif isinstance(exec_raw, str):
        exec_text = exec_raw
    exec_text = exec_text[:500] + ("…" if len(exec_text) > 500 else "")

    # Top 5 true failures
    top_fails = [
        r for r in report.get("enriched_records", [])
        if r.get("ai_classification") == "TRUE_FAILURE"
    ][:5]

    fail_lines = ""
    for r in top_fails:
        sev   = r.get("severity", "")
        sev_e = {"CRITICAL": ":red_circle:", "HIGH": ":orange_circle:",
                 "MEDIUM": ":yellow_circle:", "LOW": ":white_circle:"}.get(sev, ":black_circle:")
        fail_lines += (
            f"{sev_e} *{r.get('table_name','')}* › `{r.get('rule_name','')}` — "
            f"{r.get('ai_explanation','')[:100]}\n"
        )

    blocks: List[Dict] = [
        _header(f"{status_emoji} ETL Agent Analyzer — {status_text}"),
        _fields(
            f"*Records Analysed*\n{total:,}",
            f"*True Failures*\n{'⚠ ' if tf else ''}{tf}",
            f"*False Positives*\n{fp}",
            f"*Needs Investigation*\n{ni}",
            f"*Avg Confidence*\n{conf:.0%}",
            f"*AI Cost*\n${cost_usd:.4f} ({tokens:,} tokens)",
        ),
        _divider(),
    ]

    if exec_text:
        blocks.append(_section(f"*Executive Summary*\n{exec_text}"))
        blocks.append(_divider())

    if fail_lines:
        blocks.append(_section(f"*Top True Failures*\n{fail_lines}"))
        blocks.append(_divider())

    blocks.append(
        _context(
            f"Run ID: `{run_id}`",
            f"Run time: {report.get('run_start','')[:19]} UTC",
        )
    )

    return {
        "response_type": "in_channel",
        "blocks": blocks,
        "color": _COLOR["critical"] if tf > 0 else _COLOR["ok"],
    }


# ---------------------------------------------------------------------------
# etl-logs result
# ---------------------------------------------------------------------------

def logs_result(report: Dict[str, Any]) -> Dict[str, Any]:
    overall   = report.get("overall", {})
    critical  = overall.get("critical_issues", 0)
    high      = overall.get("high_issues", 0)
    total_f   = overall.get("total_files", 0)
    needs_att = overall.get("needs_attention", False)
    causes    = overall.get("root_cause_summary", [])[:5]

    cost      = report.get("cost_summary", {})
    cost_usd  = cost.get("total_cost_usd", 0.0) if cost else 0.0

    status_e  = ":rotating_light:" if needs_att else ":white_check_mark:"
    status_t  = f"{critical} critical, {high} high issues found" if needs_att else "No issues found"

    causes_text = "\n".join(f"• {c}" for c in causes) if causes else "_None identified_"

    sources = report.get("sources", [])
    source_lines = ""
    for src in sources[:6]:
        svc    = src.get("service", "unknown")
        status = src.get("status", "")
        s_e    = ":red_circle:" if status == "CRITICAL" else ":white_check_mark:"
        source_lines += f"{s_e} `{src.get('source','')[-60:]}` — {svc.upper()} — {src.get('files_analyzed',0)} file(s)\n"

    blocks: List[Dict] = [
        _header(f"{status_e} Log Analyzer — {status_t}"),
        _fields(
            f"*Sources Scanned*\n{len(sources)}",
            f"*Files Analysed*\n{total_f}",
            f"*Critical Issues*\n{critical}",
            f"*High Issues*\n{high}",
            f"*AI Cost*\n${cost_usd:.4f}",
        ),
        _divider(),
        _section(f"*Root Causes Identified*\n{causes_text}"),
        _divider(),
    ]

    if source_lines:
        blocks.append(_section(f"*Sources*\n{source_lines}"))

    blocks.append(
        _context(
            f"Run ID: `{report.get('run_id','')}`",
            f"Analyzed at: {report.get('analyzed_at','')[:19]} UTC",
        )
    )

    return {
        "response_type": "in_channel",
        "blocks": blocks,
        "color": _COLOR["critical"] if needs_att else _COLOR["ok"],
    }


# ---------------------------------------------------------------------------
# etl-query (NL → Athena) result
# ---------------------------------------------------------------------------

def query_result(nl_query: str, sql: str, rows: List[Dict], columns: List[str],
                 row_count: int, cost_usd: float = 0.0) -> Dict[str, Any]:
    # Render top 10 rows as a simple markdown table
    table = ""
    if rows and columns:
        header = " | ".join(f"*{c}*" for c in columns)
        sep    = " | ".join("---" for _ in columns)
        body_lines = []
        for row in rows[:10]:
            body_lines.append(" | ".join(str(row.get(c, "")) for c in columns))
        table = "\n".join([f"```{header}", sep] + body_lines + ["```"])
        if row_count > 10:
            table += f"\n_… {row_count - 10} more rows. Refine your query to narrow results._"
    else:
        table = "_No rows returned._"

    blocks: List[Dict] = [
        _header(":mag: ETL NL Query Result"),
        _section(f"*Query:* _{nl_query}_"),
        _section(f"*Generated SQL:*\n```{sql[:400]}{'…' if len(sql)>400 else ''}```"),
        _divider(),
        _section(f"*Results* ({row_count:,} rows)\n{table}"),
        _context(
            f"AI Cost: ${cost_usd:.4f}",
            "Powered by Amazon Bedrock (Claude 3 Sonnet) + Athena",
        ),
    ]

    return {
        "response_type": "in_channel",
        "blocks": blocks,
    }


# ---------------------------------------------------------------------------
# etl-status (last run summary from S3)
# ---------------------------------------------------------------------------

def status_message(report: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not report:
        return {
            "response_type": "ephemeral",
            "blocks": [_section(":information_source: No recent runs found in S3.")],
        }

    ai      = report.get("ai_analysis", {})
    tf      = ai.get("true_failures", 0)
    total   = ai.get("total_analysed", 0)
    cost    = report.get("cost_summary", {})
    run_id  = report.get("run_id", "")
    started = report.get("run_start", "")[:19]

    status_e = ":white_check_mark:" if tf == 0 else ":rotating_light:"

    return {
        "response_type": "ephemeral",
        "blocks": [
            _header(f"{status_e} Last Run Status"),
            _fields(
                f"*Run ID*\n`{run_id[:18]}…`",
                f"*Started*\n{started} UTC",
                f"*Records Analysed*\n{total}",
                f"*True Failures*\n{tf}",
                f"*AI Cost*\n${cost.get('total_cost_usd', 0):.4f}",
            ),
        ],
    }


# ---------------------------------------------------------------------------
# etl-holiday check
# ---------------------------------------------------------------------------

def holiday_message(date_str: str, entry: Optional[Dict]) -> Dict[str, Any]:
    if not entry:
        return {
            "response_type": "ephemeral",
            "blocks": [
                _section(
                    f":calendar: `{date_str}` is *not* a known US holiday.\n"
                    "Volume anomalies on this date will be analysed at standard Z-score thresholds."
                ),
            ],
        }

    mult      = entry.get("multiplier", 1.0)
    surge     = entry.get("surge_next", 1.0)
    holiday   = entry.get("holiday", "").replace("_", " ").title()

    return {
        "response_type": "ephemeral",
        "blocks": [
            _header(f":calendar: {date_str} — {holiday}"),
            _fields(
                f"*Expected Volume*\n{mult:.0%} of normal",
                f"*Next-Day Surge*\n{surge:.0%} of normal",
            ),
            _section(
                f":information_source: Volume anomaly alerts on this date will be *suppressed* "
                f"or threshold-adjusted. The AI agent will classify low-volume failures as "
                f"`FALSE_POSITIVE` unless the count is below {mult*0.5:.0%} of normal."
            ),
        ],
    }


# ---------------------------------------------------------------------------
# Generic error / timeout
# ---------------------------------------------------------------------------

def processing_error(command: str, error: str) -> Dict[str, Any]:
    return {
        "response_type": "ephemeral",
        "blocks": [
            _header(":x: Agent Processing Error"),
            _section(f"Command `{command}` failed:\n```{error[:400]}```"),
            _context("Check CloudWatch logs for the etl-slack-processor Lambda for details."),
        ],
    }
