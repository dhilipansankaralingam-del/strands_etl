###################################################################################################
# SCRIPT        : etl_tabbed_dashboard.py
# PURPOSE       : Generates a tabbed HTML dashboard email combining:
#                   Tab 1 – Full Pipeline / Validation (today's detailed results)
#                   Tab 2 – Summary Dashboard (7-day / 30-day overview with charts)
#
#                 Reads from the etl_orchestrator_audit Athena table.
#                 If summary has not been executed for today, displays a placeholder message.
#                 If full pipeline has not been executed for today, displays a placeholder.
#
# USAGE         : python etl_tabbed_dashboard.py \
#                     --config /path/to/orchestrator_config.json \
#                     --summary-config /path/to/summary_dashboard_config.json
#
#                 Both configs can be local files or s3:// URIs.
#
#=================================================================================================
# DATE          BY              MODIFICATION LOG
# ----------    -----------     ------------------------------------------
# 03/2026       Dhilipan        Initial version – tabbed dashboard combiner
#
#=================================================================================================
###################################################################################################

import boto3
import json
import sys
import os
import time
import uuid
import argparse
import logging
import base64
import io
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Optional: matplotlib for chart generation
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("etl_tabbed_dashboard")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ATHENA_COST_PER_TB_SCANNED = 5.00

# ---------------------------------------------------------------------------
# AWS Clients (lazy singletons)
# ---------------------------------------------------------------------------
_s3 = None
_athena = None


def s3_client():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def athena_client():
    global _athena
    if _athena is None:
        _athena = boto3.client("athena")
    return _athena


# ---------------------------------------------------------------------------
# Config Loader
# ---------------------------------------------------------------------------
def load_config(config_path):
    """Load config from a local path or S3 URI."""
    if config_path.startswith("s3://"):
        parts = config_path.replace("s3://", "").split("/", 1)
        bucket, key = parts[0], parts[1]
        resp = s3_client().get_object(Bucket=bucket, Key=key)
        return json.loads(resp["Body"].read().decode("utf-8"))
    else:
        with open(config_path, "r") as f:
            return json.load(f)


# ============================================================================
# ATHENA QUERY EXECUTION
# ============================================================================
def run_athena_query(query, database, output_location, workgroup="primary"):
    """Execute an Athena query and return results with cost tracking."""
    client = athena_client()
    try:
        start_resp = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_location},
            WorkGroup=workgroup,
        )
        qid = start_resp["QueryExecutionId"]
    except Exception as exc:
        return {
            "state": "FAILED", "data": [], "headers": [],
            "cost_usd": 0.0, "failure_reason": str(exc),
        }

    # Poll for completion
    for _ in range(120):
        status = client.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    else:
        return {
            "state": "TIMEOUT", "data": [], "headers": [],
            "cost_usd": 0.0, "failure_reason": "Query timed out after 240s",
        }

    # Get cost info
    stats = status["QueryExecution"].get("Statistics", {})
    data_scanned = stats.get("DataScannedInBytes", 0)
    cost_usd = (data_scanned / (1024 ** 4)) * ATHENA_COST_PER_TB_SCANNED

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", state)
        return {
            "state": state, "data": [], "headers": [],
            "cost_usd": cost_usd, "failure_reason": reason,
        }

    # Fetch results
    rows = []
    headers = []
    paginator = client.get_paginator("get_query_results")
    for page in paginator.paginate(QueryExecutionId=qid):
        result_set = page["ResultSet"]
        if not headers:
            headers = [c["Name"] for c in result_set["ResultSetMetadata"]["ColumnInfo"]]
        for row in result_set["Rows"]:
            vals = [d.get("VarCharValue", "") for d in row["Data"]]
            rows.append(dict(zip(headers, vals)))
    # Remove header row (first row is column names)
    if rows and rows[0] == dict(zip(headers, headers)):
        rows = rows[1:]

    return {
        "state": "SUCCEEDED", "data": rows, "headers": headers,
        "cost_usd": cost_usd, "failure_reason": None,
    }


# ============================================================================
# FETCH TODAY'S FULL PIPELINE RESULTS FROM AUDIT TABLE
# ============================================================================
def fetch_todays_pipeline_results(audit_cfg, run_date):
    """Query the audit table for today's full_pipeline/validation_only results."""
    db = audit_cfg["database"]
    table = audit_cfg["table"]
    bucket = audit_cfg.get("athena_output_bucket",
                           audit_cfg.get("s3_bucket", "strands-etl-athena-results"))
    prefix = audit_cfg.get("athena_output_prefix", "tabbed_dashboard_results/")
    output_location = f"s3://{bucket}/{prefix}"
    workgroup = audit_cfg.get("athena_workgroup", "primary")

    # Check if any validation records exist for today
    check_query = f"""
    SELECT COUNT(*) AS rec_count
    FROM {db}.{table}
    WHERE run_date = '{run_date}'
      AND event_type IN ('VALIDATION', 'COMPARISON', 'STALE_CHECK', 'GLUE_TRIGGER')
    """
    result = run_athena_query(check_query, db, output_location, workgroup)
    if result["state"] != "SUCCEEDED" or not result["data"]:
        return None, result.get("cost_usd", 0)

    rec_count = int(result["data"][0].get("rec_count", 0))
    if rec_count == 0:
        return None, result.get("cost_usd", 0)

    # Fetch the actual records
    detail_query = f"""
    SELECT run_id, event_timestamp, event_type, source_label,
           validation_name, check_type, status, actual_value,
           detail, cost_usd, failure_reason, overall_status,
           glue_job_name, glue_job_run_id, glue_duration_sec
    FROM {db}.{table}
    WHERE run_date = '{run_date}'
    ORDER BY event_timestamp
    """
    detail_result = run_athena_query(detail_query, db, output_location, workgroup)
    total_cost = result.get("cost_usd", 0) + detail_result.get("cost_usd", 0)
    if detail_result["state"] != "SUCCEEDED":
        return None, total_cost

    return detail_result["data"], total_cost


# ============================================================================
# RUN SUMMARY QUERIES
# ============================================================================
def run_summary_queries(summary_config):
    """Execute summary queries in parallel and return results with charts."""
    summary_cfg = summary_config.get("summary", {})
    queries = summary_cfg.get("queries", [])
    if not queries:
        return []

    max_parallel = summary_cfg.get("max_parallel", 5)
    bucket = summary_cfg.get("athena_output_bucket", "strands-etl-athena-results")
    prefix = summary_cfg.get("athena_output_prefix", "summary_results/")
    output_location = f"s3://{bucket}/{prefix}"
    workgroup = summary_cfg.get("athena_workgroup", "primary")

    results = []

    def _exec(q):
        name = q["name"]
        database = q.get("database", "audit_db")
        query_sql = q["query"]
        chart_cfg = q.get("chart")
        description = q.get("description", "")

        result = run_athena_query(query_sql, database, output_location, workgroup)
        return {
            "name": name,
            "description": description,
            "status": "PASS" if result["state"] == "SUCCEEDED" else "FAIL",
            "data": result["data"],
            "headers": result["headers"],
            "chart": chart_cfg,
            "cost_usd": result["cost_usd"],
            "detail": result.get("failure_reason") or f"{len(result['data'])} row(s) returned",
        }

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {executor.submit(_exec, q): q["name"] for q in queries}
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                results.append({
                    "name": futures[future], "description": "",
                    "status": "FAIL", "data": [], "headers": [],
                    "chart": None, "cost_usd": 0,
                    "detail": str(exc),
                })

    # Preserve original query order
    order = {q["name"]: i for i, q in enumerate(queries)}
    results.sort(key=lambda r: order.get(r["name"], 999))
    return results


# ============================================================================
# CHART GENERATION
# ============================================================================
def generate_chart_base64(chart_cfg, headers, data):
    """Generate a chart as base64-encoded PNG using matplotlib."""
    if not HAS_MATPLOTLIB or not data:
        return None

    chart_type = chart_cfg.get("type", "bar")
    title = chart_cfg.get("title", "")
    x_col = chart_cfg.get("x_column", headers[0] if headers else "")
    y_col = chart_cfg.get("y_column", headers[1] if len(headers) > 1 else "")

    x_vals = [row.get(x_col, "") for row in data]
    y_vals = []
    for row in data:
        try:
            y_vals.append(float(row.get(y_col, 0)))
        except (ValueError, TypeError):
            y_vals.append(0)

    fig, ax = plt.subplots(figsize=(10, 4))

    if chart_type == "bar":
        colors = ["#3498db" if v >= 0 else "#dc3545" for v in y_vals]
        ax.bar(range(len(x_vals)), y_vals, color=colors, width=0.6)
        ax.set_xticks(range(len(x_vals)))
        ax.set_xticklabels(x_vals, rotation=45, ha="right", fontsize=8)
    elif chart_type == "trend":
        ax.plot(range(len(x_vals)), y_vals, marker="o", color="#3498db",
                linewidth=2, markersize=5)
        ax.fill_between(range(len(x_vals)), y_vals, alpha=0.15, color="#3498db")
        ax.set_xticks(range(len(x_vals)))
        ax.set_xticklabels(x_vals, rotation=45, ha="right", fontsize=8)
    else:
        ax.bar(range(len(x_vals)), y_vals, color="#3498db", width=0.6)
        ax.set_xticks(range(len(x_vals)))
        ax.set_xticklabels(x_vals, rotation=45, ha="right", fontsize=8)

    ax.set_title(title, fontsize=12, fontweight="bold", pad=10)
    ax.set_ylabel(y_col, fontsize=10)
    ax.grid(axis="y", alpha=0.3)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def generate_html_bar_chart(chart_cfg, headers, data):
    """Fallback HTML-based bar chart when matplotlib is unavailable."""
    x_col = chart_cfg.get("x_column", headers[0] if headers else "")
    y_col = chart_cfg.get("y_column", headers[1] if len(headers) > 1 else "")
    title = chart_cfg.get("title", "")

    y_vals = []
    for row in data:
        try:
            y_vals.append(float(row.get(y_col, 0)))
        except (ValueError, TypeError):
            y_vals.append(0)

    max_val = max(y_vals) if y_vals else 1

    html = f'<div style="margin:15px 0;"><strong>{title}</strong>'
    for row, yv in zip(data, y_vals):
        label = row.get(x_col, "")
        pct = (yv / max_val * 100) if max_val > 0 else 0
        html += f"""
        <div style="display:flex;align-items:center;margin:4px 0;font-size:12px;">
            <div style="width:120px;text-align:right;padding-right:8px;
                        overflow:hidden;text-overflow:ellipsis;">{label}</div>
            <div style="flex:1;background:#e9ecef;border-radius:4px;height:18px;">
                <div style="width:{pct:.0f}%;background:#3498db;height:100%;
                            border-radius:4px;min-width:2px;"></div>
            </div>
            <div style="width:60px;padding-left:8px;font-weight:bold;">{yv:g}</div>
        </div>"""
    html += "</div>"
    return html


# ============================================================================
# STATUS COLOR HELPER
# ============================================================================
def _status_color(status):
    return {
        "OK": "#28a745", "Stale": "#dc3545",
        "Check with Source": "#fd7e14", "PASS": "#28a745",
        "FAIL": "#dc3545", "SKIPPED": "#6c757d",
        "ABORTED": "#6c757d", "WARN": "#fd7e14",
        "NOT_EXECUTED": "#6c757d",
    }.get(status, "#6c757d")


# ============================================================================
# GENERATE TABBED HTML DASHBOARD
# ============================================================================
def generate_tabbed_html(
    run_date, pipeline_records, summary_results,
    pipeline_executed, summary_executed, total_cost_usd=0.0
):
    """
    Generate a tabbed HTML email with:
      Tab 1: Full Pipeline / Validation (today's detailed results)
      Tab 2: Summary Dashboard (7-day/30-day overview)
    """
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    html = f"""
    <html><head>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; color: #333;
               background-color: #f5f6fa; }}
        .container {{ max-width: 1000px; margin: 0 auto; background: white;
                      border-radius: 12px; padding: 0; box-shadow: 0 2px 12px rgba(0,0,0,0.08);
                      overflow: hidden; }}

        /* ---- Tab Navigation ---- */
        .tab-nav {{ display: flex; background: #2c3e50; padding: 0; margin: 0; }}
        .tab-btn {{ padding: 14px 30px; color: #bdc3c7; font-size: 14px;
                    font-weight: bold; cursor: pointer; border: none; background: none;
                    border-bottom: 3px solid transparent; transition: all 0.2s;
                    text-transform: uppercase; letter-spacing: 0.5px; }}
        .tab-btn:hover {{ color: white; background: rgba(255,255,255,0.05); }}
        .tab-btn.active {{ color: white; border-bottom: 3px solid #3498db;
                           background: rgba(255,255,255,0.08); }}
        .tab-content {{ display: none; padding: 30px; }}
        .tab-content.active {{ display: block; }}

        /* ---- Header ---- */
        .dash-header {{ background: linear-gradient(135deg, #2c3e50 0%, #3498db 100%);
                        padding: 25px 30px; color: white; }}
        .dash-header h1 {{ margin: 0 0 8px 0; font-size: 22px; color: white; }}
        .dash-header p {{ margin: 0; font-size: 13px; opacity: 0.85; color: #ecf0f1; }}

        /* ---- Common ---- */
        h2 {{ color: #2c3e50; margin-top: 25px; font-size: 17px;
              border-left: 4px solid #3498db; padding-left: 12px; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 10px; font-size: 13px; }}
        th {{ background-color: #4285f4; color: #1a1a2e;
              padding: 10px 12px; text-align: left;
              font-size: 12px; font-weight: bold; letter-spacing: 0.3px; }}
        td {{ padding: 8px 12px; border-bottom: 1px solid #eee; }}
        tr:nth-child(even) {{ background-color: #f8f9fa; }}
        .badge {{ display: inline-block; padding: 4px 10px; border-radius: 4px;
                  color: white; font-weight: bold; font-size: 11px; }}
        .summary-box {{ display: inline-block; padding: 12px 22px; margin: 4px 6px 4px 0;
                        border-radius: 8px; color: white; font-size: 14px;
                        font-weight: bold; text-align: center; min-width: 110px; }}
        .chart-container {{ margin: 20px 0; text-align: center; }}
        .chart-container img {{ max-width: 100%; border-radius: 8px;
                                box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
        .footer {{ padding: 20px 30px; font-size: 11px; color: #888;
                   border-top: 1px solid #eee; }}
        .placeholder {{ text-align: center; padding: 60px 30px; color: #888; }}
        .placeholder h3 {{ color: #bdc3c7; font-size: 20px; margin-bottom: 10px; }}
        .placeholder p {{ font-size: 14px; }}

        /* ---- KPI Cards ---- */
        .kpi-row {{ display: flex; flex-wrap: wrap; gap: 12px; margin: 20px 0; }}
        .kpi-card {{ flex: 1; min-width: 130px; border-radius: 8px; padding: 15px;
                     text-align: center; }}
        .kpi-label {{ color: #666; font-size: 11px; font-weight: bold; }}
        .kpi-value {{ font-size: 28px; font-weight: bold; margin: 4px 0; }}
        .kpi-sub {{ color: #888; font-size: 11px; }}
    </style>

    <script>
    function switchTab(tabId) {{
        // Hide all tabs
        var contents = document.getElementsByClassName('tab-content');
        for (var i = 0; i < contents.length; i++) {{
            contents[i].classList.remove('active');
        }}
        var btns = document.getElementsByClassName('tab-btn');
        for (var i = 0; i < btns.length; i++) {{
            btns[i].classList.remove('active');
        }}
        // Show selected
        document.getElementById(tabId).classList.add('active');
        document.getElementById('btn-' + tabId).classList.add('active');
    }}
    </script>
    </head>
    <body>
    <div class="container">

    <!-- Dashboard Header -->
    <div class="dash-header">
        <h1>Strands ETL Orchestrator Dashboard</h1>
        <p>Date: {run_date} &nbsp;|&nbsp; Generated: {now_str} &nbsp;|&nbsp;
           Athena Cost: ${total_cost_usd:.4f}</p>
    </div>

    <!-- Tab Navigation -->
    <div class="tab-nav">
        <button class="tab-btn active" id="btn-tab-pipeline"
                onclick="switchTab('tab-pipeline')">
            Full Pipeline
        </button>
        <button class="tab-btn" id="btn-tab-summary"
                onclick="switchTab('tab-summary')">
            Summary Dashboard
        </button>
    </div>
    """

    # ==================================================================
    # TAB 1: Full Pipeline
    # ==================================================================
    html += '<div id="tab-pipeline" class="tab-content active">'

    if not pipeline_executed:
        html += """
        <div class="placeholder">
            <h3>&#9888; Pipeline Yet to Be Executed</h3>
            <p>No full pipeline or validation run has been recorded for today.<br>
               Results will appear here once the orchestrator completes today's run.</p>
        </div>"""
    else:
        html += _build_pipeline_tab(pipeline_records, run_date)

    html += "</div>"

    # ==================================================================
    # TAB 2: Summary Dashboard
    # ==================================================================
    html += '<div id="tab-summary" class="tab-content">'

    if not summary_executed:
        html += """
        <div class="placeholder">
            <h3>&#9888; Summary Yet to Be Executed</h3>
            <p>The summary dashboard has not been generated for today.<br>
               Run the orchestrator in <code>summary</code> mode to populate this tab.</p>
        </div>"""
    else:
        html += _build_summary_tab(summary_results)

    html += "</div>"

    # ==================================================================
    # Footer
    # ==================================================================
    html += f"""
    <div class="footer">
        <p>Generated by Strands ETL Tabbed Dashboard v1.0 &nbsp;|&nbsp; BIG DATA Team</p>
    </div>
    </div></body></html>"""

    return html


# ============================================================================
# TAB 1 BUILDER: Full Pipeline
# ============================================================================
def _build_pipeline_tab(records, run_date):
    """Build HTML content for the Full Pipeline tab from audit records."""
    if not records:
        return '<p style="color:#888;">No records found.</p>'

    # Separate records by event_type
    stale_records = [r for r in records if r.get("event_type") == "STALE_CHECK"]
    glue_records = [r for r in records if r.get("event_type") == "GLUE_TRIGGER"]
    validation_records = [r for r in records if r.get("event_type") == "VALIDATION"]
    comparison_records = [r for r in records if r.get("event_type") == "COMPARISON"]

    # Determine overall status from the last record
    overall_status = records[-1].get("overall_status", "UNKNOWN") if records else "UNKNOWN"
    overall_color = "#28a745" if overall_status == "PASS" else "#dc3545"

    # KPI calculations
    all_checks = validation_records + comparison_records
    total_checks = len(all_checks)
    passed = sum(1 for c in all_checks if c.get("status") == "PASS")
    failed = sum(1 for c in all_checks if c.get("status") == "FAIL")
    warned = sum(1 for c in all_checks if c.get("status") == "WARN")
    not_exec = sum(1 for c in all_checks if c.get("status") == "NOT_EXECUTED")
    pass_rate = (passed / total_checks * 100) if total_checks > 0 else 0
    pass_color = "#28a745" if pass_rate >= 80 else ("#fd7e14" if pass_rate >= 60 else "#dc3545")

    # Total cost
    total_cost = 0.0
    for r in all_checks:
        try:
            total_cost += float(r.get("cost_usd") or 0)
        except (ValueError, TypeError):
            pass

    # Run IDs
    run_ids = sorted(set(r.get("run_id", "") for r in records if r.get("run_id")))
    run_id_str = ", ".join(run_ids) if run_ids else "N/A"

    html = ""

    # ---- Status Banner ----
    html += f"""
    <div style="margin-bottom:15px;">
        <span class="summary-box" style="background-color:{overall_color};">
            Overall: {overall_status}</span>
        <span class="summary-box" style="background-color:#17a2b8;">
            Sources: {len(stale_records)}</span>
        <span class="summary-box" style="background-color:{'#28a745' if not failed else '#dc3545'};">
            Validations: {passed}/{total_checks}</span>
    </div>
    <p style="font-size:12px;color:#666;">Run ID: {run_id_str} &nbsp;|&nbsp; Date: {run_date}</p>
    """

    # ---- KPI Scorecard ----
    if total_checks > 0:
        html += f"""
    <div class="kpi-row">
      <div class="kpi-card" style="background:#f0fff0;border-left:4px solid #28a745;">
        <div class="kpi-label">PASSED</div>
        <div class="kpi-value" style="color:#28a745;">{passed}</div>
        <div class="kpi-sub">{pass_rate:.1f}% of total</div>
      </div>
      <div class="kpi-card" style="background:#fff8e1;border-left:4px solid #fd7e14;">
        <div class="kpi-label">WARNINGS</div>
        <div class="kpi-value" style="color:#fd7e14;">{warned}</div>
        <div class="kpi-sub">{(warned/total_checks*100):.1f}% of total</div>
      </div>
      <div class="kpi-card" style="background:#fff0f0;border-left:4px solid #dc3545;">
        <div class="kpi-label">FAILURES</div>
        <div class="kpi-value" style="color:#dc3545;">{failed}</div>
        <div class="kpi-sub">{(failed/total_checks*100):.1f}% of total</div>
      </div>
      <div class="kpi-card" style="background:#f5f5f5;border-left:4px solid #6c757d;">
        <div class="kpi-label">NOT EXECUTED</div>
        <div class="kpi-value" style="color:#6c757d;">{not_exec}</div>
        <div class="kpi-sub">{(not_exec/total_checks*100):.1f}% of total</div>
      </div>
      <div class="kpi-card" style="background:#f0f4ff;border-left:4px solid #3498db;">
        <div class="kpi-label">TOTAL CHECKS</div>
        <div class="kpi-value" style="color:#2c3e50;">{total_checks}</div>
        <div class="kpi-sub">Cost: ${total_cost:.4f}</div>
      </div>
    </div>

    <div style="background:#fafbfc;border:1px solid #eee;border-radius:8px;padding:15px;
                margin-bottom:20px;">
        <div class="kpi-label">PASS RATE</div>
        <div style="margin:8px 0;">
          <div style="background:#e9ecef;border-radius:10px;height:20px;overflow:hidden;">
            <div style="background:{pass_color};height:100%;width:{pass_rate:.0f}%;
                        border-radius:10px;"></div>
          </div>
        </div>
        <div style="color:{pass_color};font-size:22px;font-weight:bold;text-align:center;">
            {pass_rate:.1f}%</div>
    </div>
    """

    # ---- Section 1: Stale Detection ----
    section_num = 1
    if stale_records:
        html += f"<h2>{section_num}. S3 Stale File Detection</h2>"
        html += """<table><thead><tr><th>#</th><th>Source</th><th>Status</th>
            <th>Detail</th></tr></thead><tbody>"""
        for i, sr in enumerate(stale_records, 1):
            color = _status_color(sr.get("status", ""))
            html += f"""<tr><td>{i}</td>
                <td><b>{sr.get('source_label', '-')}</b></td>
                <td><span class="badge" style="background-color:{color};">
                    {sr.get('status', '-')}</span></td>
                <td>{sr.get('detail', '-')}</td></tr>"""
        html += "</tbody></table>"
        section_num += 1

    # ---- Section 2: Glue Jobs ----
    if glue_records:
        html += f"<h2>{section_num}. Glue Job Triggers</h2>"
        html += """<table><thead><tr><th>#</th><th>Source</th><th>Glue Job</th>
            <th>Run ID</th><th>Status</th><th>Duration</th><th>Detail</th>
            </tr></thead><tbody>"""
        for i, gr in enumerate(glue_records, 1):
            color = _status_color(gr.get("status", ""))
            dur = gr.get("glue_duration_sec", "-")
            dur_str = f"{dur}s" if dur and dur != "-" else "-"
            html += f"""<tr><td>{i}</td>
                <td><b>{gr.get('source_label', '-')}</b></td>
                <td>{gr.get('glue_job_name', '-')}</td>
                <td>{gr.get('glue_job_run_id', '-')}</td>
                <td><span class="badge" style="background-color:{color};">
                    {gr.get('status', '-')}</span></td>
                <td>{dur_str}</td><td>{gr.get('detail', '-')}</td></tr>"""
        html += "</tbody></table>"
        section_num += 1

    # ---- Section 3: Validations ----
    if validation_records:
        html += f"<h2>{section_num}. Post-Load Validations</h2>"
        html += """<table><thead><tr><th>#</th><th>Validation</th><th>Check Type</th>
            <th>Status</th><th>Actual</th><th>Cost</th><th>Detail</th>
            </tr></thead><tbody>"""
        for i, vr in enumerate(validation_records, 1):
            color = _status_color(vr.get("status", ""))
            cost_val = vr.get("cost_usd", "")
            cost_str = f"${float(cost_val):.4f}" if cost_val and cost_val != "" else "-"
            row_bg = ' style="background:#f5f5f5;"' if vr.get("status") == "NOT_EXECUTED" else ""
            html += f"""<tr{row_bg}><td>{i}</td>
                <td><b>{vr.get('validation_name', '-')}</b></td>
                <td>{vr.get('check_type', '-')}</td>
                <td><span class="badge" style="background-color:{color};">
                    {vr.get('status', '-')}</span></td>
                <td>{vr.get('actual_value', '-')}</td>
                <td style="font-size:11px;">{cost_str}</td>
                <td>{vr.get('detail', '-')}</td></tr>"""
        html += "</tbody></table>"
        section_num += 1

    # ---- Section 4: Comparisons ----
    if comparison_records:
        html += f"<h2>{section_num}. Cross-Query Comparisons</h2>"
        html += """<table><thead><tr><th>#</th><th>Comparison</th><th>Check Type</th>
            <th>Status</th><th>Actual</th><th>Detail</th></tr></thead><tbody>"""
        for i, cr in enumerate(comparison_records, 1):
            color = _status_color(cr.get("status", ""))
            html += f"""<tr><td>{i}</td>
                <td><b>{cr.get('validation_name', '-')}</b></td>
                <td>{cr.get('check_type', '-')}</td>
                <td><span class="badge" style="background-color:{color};">
                    {cr.get('status', '-')}</span></td>
                <td>{cr.get('actual_value', '-')}</td>
                <td>{cr.get('detail', '-')}</td></tr>"""
        html += "</tbody></table>"

    return html


# ============================================================================
# TAB 2 BUILDER: Summary Dashboard
# ============================================================================
def _build_summary_tab(summary_results):
    """Build HTML content for the Summary Dashboard tab."""
    if not summary_results:
        return '<p style="color:#888;">No summary results available.</p>'

    # Separate 7-day and 30-day queries
    queries_7d = [r for r in summary_results if "7d" in r["name"]]
    queries_30d = [r for r in summary_results if "30d" in r["name"]]
    queries_other = [r for r in summary_results
                     if "7d" not in r["name"] and "30d" not in r["name"]]

    html = ""

    # ---- 7-Day Overview ----
    if queries_7d:
        html += '<h2>7-Day Overview</h2>'
        html += _build_summary_cards(queries_7d)

    # ---- 30-Day Trends ----
    if queries_30d:
        html += '<h2>30-Day Trends</h2>'
        html += _build_summary_cards(queries_30d)

    # ---- Other ----
    if queries_other:
        html += '<h2>Additional Summaries</h2>'
        html += _build_summary_cards(queries_other)

    return html


def _build_summary_cards(results):
    """Build summary cards with data tables and charts."""
    html = ""
    for sr in results:
        html += f"""<div style="margin:20px 0;padding:15px;background:#fafbfc;
                     border-radius:8px;border:1px solid #eee;">
            <h3 style="color:#2c3e50;margin-top:0;">{sr['name'].replace('_', ' ').title()}</h3>
            <p style="color:#666;font-size:12px;">{sr.get('description', '')}</p>"""

        if sr["status"] == "FAIL":
            html += f'<p style="color:#dc3545;">Query failed: {sr["detail"]}</p>'
        elif sr["data"]:
            # Data table
            html += '<table style="margin-bottom:15px;"><thead><tr>'
            for h in sr["headers"]:
                html += f"<th>{h}</th>"
            html += "</tr></thead><tbody>"
            for row in sr["data"][:50]:
                html += "<tr>"
                for h in sr["headers"]:
                    html += f"<td>{row.get(h, '')}</td>"
                html += "</tr>"
            html += "</tbody></table>"

            # Chart
            if sr.get("chart") and sr["data"]:
                chart_b64 = generate_chart_base64(sr["chart"], sr["headers"], sr["data"])
                if chart_b64:
                    html += f"""<div class="chart-container">
                        <img src="data:image/png;base64,{chart_b64}"
                             alt="{sr['chart'].get('title', 'Chart')}" /></div>"""
                elif sr["chart"].get("type") in ("bar", "trend"):
                    html += generate_html_bar_chart(sr["chart"], sr["headers"], sr["data"])
        else:
            html += '<p style="color:#888;">No data returned.</p>'

        html += "</div>"
    return html


# ============================================================================
# EMAIL
# ============================================================================
def send_email(html_content, email_cfg, run_date):
    """Send the tabbed dashboard HTML email via SMTP."""
    sender = email_cfg["sender"]
    recipients = email_cfg["recipients"]
    subject = (f"{email_cfg.get('subject_prefix', 'ETL Dashboard')} – "
               f"TABBED VIEW – {run_date}")

    msg = MIMEMultipart("alternative")
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_content, "html"))

    smtp_host = email_cfg.get("smtp_host", "localhost")
    smtp_port = email_cfg.get("smtp_port", 25)

    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
        logger.info("Tabbed dashboard email sent to %s", ", ".join(recipients))
    except Exception as exc:
        logger.error("Failed to send email: %s", exc)


# ============================================================================
# SAVE HTML TO S3
# ============================================================================
def save_html_to_s3(html_content, audit_cfg, run_date):
    """Optionally save the HTML dashboard to S3 for archival."""
    bucket = audit_cfg.get("s3_bucket", "strands-etl-audit")
    prefix = audit_cfg.get("s3_prefix", "audit_logs/orchestrator/").rstrip("/")
    key = f"{prefix}/dashboards/run_date={run_date}/tabbed_dashboard.html"

    try:
        s3_client().put_object(
            Bucket=bucket, Key=key,
            Body=html_content.encode("utf-8"),
            ContentType="text/html",
        )
        s3_path = f"s3://{bucket}/{key}"
        logger.info("Dashboard HTML saved to %s", s3_path)
        return s3_path
    except Exception as exc:
        logger.error("Failed to save HTML to S3 (non-fatal): %s", exc)
        return None


# ============================================================================
# MAIN
# ============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Strands ETL Tabbed Dashboard – Combines Full Pipeline + Summary in tabs"
    )
    parser.add_argument(
        "--config", required=True,
        help="Path to orchestrator_config.json (local or s3://). "
             "Used for audit table info, email config, and pipeline lookup.",
    )
    parser.add_argument(
        "--summary-config", required=False, default=None,
        help="Path to summary_dashboard_config.json (local or s3://). "
             "If not provided, summary section from --config is used.",
    )
    parser.add_argument(
        "--run-date", required=False, default=None,
        help="Override run date (YYYY-MM-DD). Defaults to today (UTC).",
    )
    parser.add_argument(
        "--skip-email", action="store_true", default=False,
        help="Skip sending email (useful for local testing).",
    )
    parser.add_argument(
        "--save-html", action="store_true", default=False,
        help="Save the generated HTML to S3 for archival.",
    )
    parser.add_argument(
        "--output-file", required=False, default=None,
        help="Write HTML to a local file (for testing/preview).",
    )
    args = parser.parse_args()

    run_date = args.run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logger.info("=" * 70)
    logger.info("STRANDS ETL TABBED DASHBOARD – Date: %s", run_date)
    logger.info("=" * 70)

    # Load configs
    config = load_config(args.config)
    audit_cfg = config.get("audit", {})
    email_cfg = config.get("email", {})

    summary_config = None
    if args.summary_config:
        logger.info("Loading separate summary config: %s", args.summary_config)
        summary_config = load_config(args.summary_config)

    effective_summary_config = summary_config if summary_config else config
    total_cost_usd = 0.0

    # ================================================================
    # TAB 1: Fetch today's pipeline results from audit table
    # ================================================================
    logger.info("-" * 50)
    logger.info("Fetching today's pipeline results from audit table...")
    logger.info("-" * 50)

    pipeline_records = None
    pipeline_executed = False
    try:
        pipeline_records, cost = fetch_todays_pipeline_results(audit_cfg, run_date)
        total_cost_usd += cost
        pipeline_executed = pipeline_records is not None and len(pipeline_records) > 0
        if pipeline_executed:
            logger.info("Found %d pipeline records for %s", len(pipeline_records), run_date)
        else:
            logger.info("No pipeline records found for %s", run_date)
    except Exception as exc:
        logger.error("Failed to fetch pipeline results: %s", exc)

    # ================================================================
    # TAB 2: Run summary queries (or check if already executed)
    # ================================================================
    logger.info("-" * 50)
    logger.info("Running summary dashboard queries...")
    logger.info("-" * 50)

    summary_results = []
    summary_executed = False
    has_summary = bool(
        effective_summary_config.get("summary", {}).get("queries")
    )

    if has_summary:
        try:
            summary_results = run_summary_queries(effective_summary_config)
            summary_executed = len(summary_results) > 0
            total_cost_usd += sum(sr.get("cost_usd", 0) for sr in summary_results)
            logger.info("Executed %d summary queries", len(summary_results))
        except Exception as exc:
            logger.error("Failed to run summary queries: %s", exc)
    else:
        logger.info("No summary queries configured – Summary tab will show placeholder")

    # ================================================================
    # Generate Tabbed HTML
    # ================================================================
    logger.info("-" * 50)
    logger.info("Generating tabbed dashboard HTML...")
    logger.info("-" * 50)

    html = generate_tabbed_html(
        run_date=run_date,
        pipeline_records=pipeline_records or [],
        summary_results=summary_results,
        pipeline_executed=pipeline_executed,
        summary_executed=summary_executed,
        total_cost_usd=total_cost_usd,
    )

    # ================================================================
    # Output
    # ================================================================
    if args.output_file:
        with open(args.output_file, "w") as f:
            f.write(html)
        logger.info("HTML written to %s", args.output_file)

    if args.save_html:
        save_html_to_s3(html, audit_cfg, run_date)

    if not args.skip_email:
        # Use email config from summary config if available, else main config
        effective_email_cfg = (
            summary_config.get("email", email_cfg) if summary_config else email_cfg
        )
        send_email(html, effective_email_cfg, run_date)

    # ---- Final Summary ----
    logger.info("=" * 70)
    logger.info(
        "COMPLETE – Pipeline tab: %s | Summary tab: %s | "
        "Pipeline records: %d | Summary queries: %d | Athena Cost: $%.4f",
        "EXECUTED" if pipeline_executed else "NOT YET",
        "EXECUTED" if summary_executed else "NOT YET",
        len(pipeline_records or []),
        len(summary_results),
        total_cost_usd,
    )
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
