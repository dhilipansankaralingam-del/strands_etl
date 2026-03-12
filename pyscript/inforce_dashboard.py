###################################################################################################
# SCRIPT        : inforce_dashboard.py
# PURPOSE       : Inforce Membership Monthly Movement – Data Quality Dashboard
#                   1) Reads source data from Redshift into Spark temp view
#                   2) Runs config-driven validations (SQL files from S3)
#                   3) Evaluates pass/fail per check with configurable failure detection
#                   4) Runs KPI queries for membership scorecard
#                   5) Runs trend queries with chart generation (trend, bar, grouped bar)
#                   6) Generates a rich dashboard-style HTML email
#                   7) Fails the Glue job (exit 1) if critical checks fail
#
# CONFIG        : config/inforce_dashboard_config.json
# GLUE PARAMS   : --S3_BUCKET, --REDSHIFT_CON_NAME, --TempDir, --CONFIG_PATH (optional)
#
#=================================================================================================
# DATE          BY              MODIFICATION LOG
# ----------    -----------     ------------------------------------------
# 03/2026       Dhilipan        Rewrite of inforce_dash.py with dashboard email, trends, KPIs
#
#=================================================================================================
###################################################################################################

import sys
import os
import json
import traceback
import time
import base64
import io
from datetime import datetime, timedelta
from collections import OrderedDict

import boto3
import botocore
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

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
# Constants
# ---------------------------------------------------------------------------
STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_WARN = "WARN"
STATUS_ERROR = "ERROR"
STATUS_INFO = "INFO"

VIBRANT_COLORS = [
    "#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4", "#FFEAA7",
    "#DDA0DD", "#FF8C42", "#98D8C8", "#F7DC6F", "#BB8FCE",
    "#85C1E9", "#F1948A", "#82E0AA", "#F8C471", "#AED6F1",
]

CATEGORY_COLORS = {
    "balance": "#3498db",
    "anomaly": "#e74c3c",
    "completeness": "#2ecc71",
    "rolling_average": "#9b59b6",
    "statistical": "#f39c12",
    "count_check": "#1abc9c",
    "informational": "#95a5a6",
}


# ============================================================================
# AWS + SPARK SETUP
# ============================================================================
s3_client = boto3.client("s3")


def get_file_from_s3(bucket, key):
    """Read a file from S3 and return its content as a string."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")


def load_config(bucket, key):
    """Load JSON config from S3."""
    content = get_file_from_s3(bucket, key)
    return json.loads(content)


def init_spark():
    """Initialize Spark session with Iceberg catalog support."""
    spark = (
        SparkSession.builder
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog",
    )
    return spark


def read_from_redshift(spark, database_name, sql_query, url, user, password, temp_dir):
    """Read data from Redshift using Spark Redshift connector."""
    full_url = f"{url}/{database_name}"
    df = (
        spark.read.format("io.github.spark_redshift_community.spark.redshift")
        .option("url", full_url)
        .option("query", sql_query)
        .option("user", user)
        .option("password", password)
        .option("tempformat", "AVRO")
        .option("tempdir", temp_dir)
        .option("forward_spark_s3_credentials", "true")
        .load()
    )
    print(f"Extracted {df.count()} rows from Redshift")
    return df


# ============================================================================
# VALIDATION EXECUTION
# ============================================================================
def evaluate_failure(result_df, failure_check):
    """
    Evaluate whether a validation result DataFrame indicates failure.

    Supported failure_check types:
      - column_value: check if any column matching pattern contains fail_value
      - row_count_zero: fail if no rows returned
      - row_count_nonzero: fail if rows ARE returned (e.g., anomaly rows)
      - none: informational only, never fails

    Returns: (status, failure_details_list)
    """
    check_type = failure_check.get("type", "none")

    if check_type == "none":
        return STATUS_INFO, []

    try:
        pdf = result_df.toPandas()
    except Exception as exc:
        return STATUS_ERROR, [f"Error converting to pandas: {exc}"]

    if check_type == "column_value":
        columns_pattern = failure_check.get("columns_pattern", "")
        fail_value = failure_check.get("fail_value", "N")
        failures = []
        matching_cols = [c for c in pdf.columns if columns_pattern.lower() in c.lower()]
        for col in matching_cols:
            fail_rows = pdf[pdf[col].astype(str) == str(fail_value)]
            if len(fail_rows) > 0:
                failures.append(f"Column '{col}': {len(fail_rows)} row(s) with value '{fail_value}'")
        if failures:
            return STATUS_FAIL, failures
        return STATUS_PASS, []

    elif check_type == "row_count_zero":
        if len(pdf) == 0:
            return STATUS_FAIL, ["Query returned 0 rows (expected > 0)"]
        return STATUS_PASS, []

    elif check_type == "row_count_nonzero":
        if len(pdf) > 0:
            return STATUS_FAIL, [f"Query returned {len(pdf)} rows (expected 0)"]
        return STATUS_PASS, []

    return STATUS_INFO, []


def run_validations(spark, s3_bucket, validations_config):
    """Execute all validation SQL files and evaluate pass/fail."""
    results = []

    for i, vdef in enumerate(validations_config):
        name = vdef["name"]
        display_name = vdef.get("display_name", name)
        sql_file = vdef.get("sql_file", "")
        category = vdef.get("category", "general")
        failure_check = vdef.get("failure_check", {"type": "none"})
        start_time = time.time()

        print(f"[{i+1}/{len(validations_config)}] Running: {display_name}")

        result = {
            "name": name,
            "display_name": display_name,
            "category": category,
            "description": vdef.get("description", ""),
            "logic": vdef.get("logic", ""),
            "expected_result": vdef.get("expected_result", ""),
            "failure_implication": vdef.get("failure_implication", ""),
            "abort_on_failure": vdef.get("abort_on_failure", False),
            "sql_file": sql_file,
            "row_count": 0,
            "status": STATUS_ERROR,
            "detail": "",
            "failure_details": [],
            "duration_sec": 0,
            "data_preview": [],
            "columns": [],
        }

        try:
            sql_query = get_file_from_s3(s3_bucket, sql_file)
            result_df = spark.sql(sql_query)
            row_count = result_df.count()
            result["row_count"] = row_count

            # Evaluate failure
            status, failure_details = evaluate_failure(result_df, failure_check)
            result["status"] = status
            result["failure_details"] = failure_details
            if failure_details:
                result["detail"] = "; ".join(failure_details)
            else:
                result["detail"] = f"{row_count} row(s) returned"

            # Capture data preview (first 20 rows) for the email table
            result["columns"] = [f.name for f in result_df.schema.fields]
            preview_rows = result_df.limit(20).toPandas().to_dict("records")
            result["data_preview"] = preview_rows

            print(f"   -> {status}: {result['detail']}")

        except Exception as exc:
            traceback.print_exc()
            result["status"] = STATUS_ERROR
            result["detail"] = str(exc)
            result["failure_details"] = [str(exc)]
            print(f"   -> ERROR: {exc}")

        result["duration_sec"] = round(time.time() - start_time, 1)
        results.append(result)

    return results


# ============================================================================
# KPI EXECUTION
# ============================================================================
def run_kpi_queries(spark, kpi_config):
    """Execute KPI queries and return scalar values."""
    kpis = []
    for kdef in kpi_config.get("queries", []):
        name = kdef["name"]
        display_name = kdef.get("display_name", name)
        query = kdef["query"]
        fmt = kdef.get("format", "number")
        color = kdef.get("color", "#3498db")

        try:
            result_df = spark.sql(query)
            pdf = result_df.toPandas()
            value = pdf.iloc[0, 0] if len(pdf) > 0 else None
            if value is not None:
                value = float(value)
            kpis.append({
                "name": name, "display_name": display_name,
                "value": value, "format": fmt, "color": color,
                "status": "ok",
            })
        except Exception as exc:
            print(f"KPI query failed for {name}: {exc}")
            kpis.append({
                "name": name, "display_name": display_name,
                "value": None, "format": fmt, "color": "#6c757d",
                "status": "error", "error": str(exc),
            })

    return kpis


# ============================================================================
# TREND EXECUTION
# ============================================================================
def run_trend_queries(spark, trends_config):
    """Execute trend queries and return data for charting."""
    trends = []
    for tdef in trends_config.get("queries", []):
        name = tdef["name"]
        display_name = tdef.get("display_name", name)
        query = tdef["query"]
        chart_cfg = tdef.get("chart", {})

        try:
            result_df = spark.sql(query)
            pdf = result_df.toPandas()
            columns = list(pdf.columns)
            data = pdf.to_dict("records")
            trends.append({
                "name": name, "display_name": display_name,
                "columns": columns, "data": data,
                "chart": chart_cfg, "status": "ok",
            })
            print(f"Trend query '{name}': {len(data)} rows")
        except Exception as exc:
            print(f"Trend query failed for {name}: {exc}")
            trends.append({
                "name": name, "display_name": display_name,
                "columns": [], "data": [],
                "chart": chart_cfg, "status": "error", "error": str(exc),
            })

    return trends


# ============================================================================
# CHART GENERATION (matplotlib)
# ============================================================================
def _to_b64_png(fig):
    """Save matplotlib figure to base64 PNG string."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight", facecolor="#FAFBFC")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def generate_trend_chart(chart_cfg, data, columns):
    """Generate a trend/bar/grouped chart as base64 PNG."""
    if not HAS_MATPLOTLIB or not data:
        return None

    chart_type = chart_cfg.get("type", "trend")
    title = chart_cfg.get("title", "")
    x_col = chart_cfg.get("x_column", columns[0] if columns else "")
    y_col = chart_cfg.get("y_column", columns[1] if len(columns) > 1 else "")
    group_by = chart_cfg.get("group_by")
    top_n = chart_cfg.get("top_n")

    import pandas as pd
    df = pd.DataFrame(data)

    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor("#FAFBFC")
    ax.set_facecolor("#FAFBFC")

    if chart_type == "trend" and not group_by:
        x_vals = df[x_col].astype(str).tolist()
        y_vals = pd.to_numeric(df[y_col], errors="coerce").fillna(0).tolist()
        ax.fill_between(range(len(x_vals)), y_vals, alpha=0.15, color="#45B7D1")
        ax.plot(range(len(x_vals)), y_vals, color="#45B7D1", linewidth=2.5,
                marker="o", markersize=6, markerfacecolor="#FF6B6B",
                markeredgecolor="white", markeredgewidth=1.5)
        ax.set_xticks(range(len(x_vals)))
        ax.set_xticklabels(x_vals, rotation=45, ha="right", fontsize=8)
        for i, v in enumerate(y_vals):
            ax.annotate(f"{v:,.0f}", (i, v), textcoords="offset points",
                        xytext=(0, 10), ha="center", fontsize=7, fontweight="bold")

    elif chart_type == "trend" and group_by:
        groups = df[group_by].unique()
        if top_n:
            totals = df.groupby(group_by)[y_col].apply(
                lambda x: pd.to_numeric(x, errors="coerce").sum()
            ).nlargest(top_n)
            groups = totals.index.tolist()
        for idx, grp in enumerate(groups):
            grp_df = df[df[group_by] == grp].sort_values(x_col)
            x_vals = grp_df[x_col].astype(str).tolist()
            y_vals = pd.to_numeric(grp_df[y_col], errors="coerce").fillna(0).tolist()
            color = VIBRANT_COLORS[idx % len(VIBRANT_COLORS)]
            ax.plot(range(len(x_vals)), y_vals, color=color, linewidth=2,
                    marker="o", markersize=4, label=str(grp))
        ax.legend(fontsize=8, loc="upper left", framealpha=0.9)
        x_labels = df[x_col].astype(str).unique()
        ax.set_xticks(range(len(x_labels)))
        ax.set_xticklabels(x_labels, rotation=45, ha="right", fontsize=8)

    elif chart_type == "grouped_bar" and group_by:
        import numpy as np
        groups = df[group_by].unique()
        x_labels = df[x_col].astype(str).unique()
        n_groups = len(groups)
        bar_width = 0.8 / max(n_groups, 1)
        x_pos = np.arange(len(x_labels))
        for idx, grp in enumerate(groups):
            grp_df = df[df[group_by] == grp]
            # Align to x_labels order
            vals = []
            for xl in x_labels:
                match = grp_df[grp_df[x_col].astype(str) == xl]
                vals.append(pd.to_numeric(match[y_col], errors="coerce").sum() if len(match) > 0 else 0)
            color = VIBRANT_COLORS[idx % len(VIBRANT_COLORS)]
            offset = (idx - n_groups / 2 + 0.5) * bar_width
            ax.bar(x_pos + offset, vals, width=bar_width, label=str(grp), color=color)
        ax.set_xticks(x_pos)
        ax.set_xticklabels(x_labels, rotation=45, ha="right", fontsize=8)
        ax.legend(fontsize=8)

    elif chart_type == "stacked_bar" and group_by:
        import numpy as np
        x_labels = sorted(df[x_col].astype(str).unique())
        groups = df.groupby(group_by)[y_col].apply(
            lambda x: pd.to_numeric(x, errors="coerce").sum()
        )
        if top_n:
            groups = groups.nlargest(top_n)
        top_groups = groups.index.tolist()
        x_pos = np.arange(len(x_labels))
        bottom = np.zeros(len(x_labels))
        for idx, grp in enumerate(top_groups):
            grp_df = df[df[group_by] == grp]
            vals = []
            for xl in x_labels:
                match = grp_df[grp_df[x_col].astype(str) == xl]
                vals.append(pd.to_numeric(match[y_col], errors="coerce").sum() if len(match) > 0 else 0)
            color = VIBRANT_COLORS[idx % len(VIBRANT_COLORS)]
            ax.bar(x_pos, vals, bottom=bottom, label=str(grp), color=color, width=0.6)
            bottom += np.array(vals)
        ax.set_xticks(x_pos)
        ax.set_xticklabels(x_labels, rotation=45, ha="right", fontsize=8)
        ax.legend(fontsize=7, loc="upper left", ncol=2)

    elif chart_type == "bar":
        x_vals = df[x_col].astype(str).tolist()
        y_vals = pd.to_numeric(df[y_col], errors="coerce").fillna(0).tolist()
        colors = [VIBRANT_COLORS[i % len(VIBRANT_COLORS)] for i in range(len(x_vals))]
        ax.bar(range(len(x_vals)), y_vals, color=colors, width=0.6)
        ax.set_xticks(range(len(x_vals)))
        ax.set_xticklabels(x_vals, rotation=45, ha="right", fontsize=8)
        for i, v in enumerate(y_vals):
            ax.annotate(f"{v:,.0f}", (i, v), textcoords="offset points",
                        xytext=(0, 5), ha="center", fontsize=8, fontweight="bold")

    else:
        plt.close(fig)
        return None

    ax.set_title(title, fontsize=13, fontweight="bold", color="#2c3e50", pad=12)
    ax.set_ylabel(y_col.replace("_", " ").title(), fontsize=10, color="#2c3e50")
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()

    return _to_b64_png(fig)


def generate_pass_rate_pie(passed, failed, warned, errored, info_count):
    """Generate a donut chart for validation pass rate."""
    if not HAS_MATPLOTLIB:
        return None

    labels = []
    sizes = []
    colors = []
    if passed > 0:
        labels.append(f"PASS ({passed})")
        sizes.append(passed)
        colors.append("#28a745")
    if failed > 0:
        labels.append(f"FAIL ({failed})")
        sizes.append(failed)
        colors.append("#dc3545")
    if warned > 0:
        labels.append(f"WARN ({warned})")
        sizes.append(warned)
        colors.append("#fd7e14")
    if errored > 0:
        labels.append(f"ERROR ({errored})")
        sizes.append(errored)
        colors.append("#6c757d")
    if info_count > 0:
        labels.append(f"INFO ({info_count})")
        sizes.append(info_count)
        colors.append("#17a2b8")

    if not sizes:
        return None

    fig, ax = plt.subplots(figsize=(5, 5))
    fig.patch.set_facecolor("#FAFBFC")
    wedges, texts, autotexts = ax.pie(
        sizes, labels=labels, colors=colors, autopct="%1.0f%%",
        startangle=140, pctdistance=0.75,
        wedgeprops={"edgecolor": "white", "linewidth": 2, "width": 0.4},
    )
    for t in autotexts:
        t.set_fontsize(10)
        t.set_fontweight("bold")
    ax.set_title("Validation Results", fontsize=14, fontweight="bold",
                 color="#2c3e50", pad=15)
    plt.tight_layout()
    return _to_b64_png(fig)


def generate_category_bar_chart(results):
    """Generate a horizontal bar chart of pass/fail counts by category."""
    if not HAS_MATPLOTLIB:
        return None

    from collections import defaultdict
    cats = defaultdict(lambda: {"pass": 0, "fail": 0, "warn": 0, "info": 0, "error": 0})
    for r in results:
        cat = r.get("category", "other")
        s = r["status"]
        if s == STATUS_PASS:
            cats[cat]["pass"] += 1
        elif s == STATUS_FAIL:
            cats[cat]["fail"] += 1
        elif s == STATUS_WARN:
            cats[cat]["warn"] += 1
        elif s == STATUS_ERROR:
            cats[cat]["error"] += 1
        else:
            cats[cat]["info"] += 1

    if not cats:
        return None

    import numpy as np
    cat_names = sorted(cats.keys())
    pass_vals = [cats[c]["pass"] for c in cat_names]
    fail_vals = [cats[c]["fail"] for c in cat_names]
    warn_vals = [cats[c]["warn"] for c in cat_names]
    info_vals = [cats[c]["info"] for c in cat_names]

    fig, ax = plt.subplots(figsize=(8, max(3, len(cat_names) * 0.7)))
    fig.patch.set_facecolor("#FAFBFC")
    ax.set_facecolor("#FAFBFC")

    y_pos = np.arange(len(cat_names))
    ax.barh(y_pos, pass_vals, height=0.5, color="#28a745", label="PASS")
    ax.barh(y_pos, fail_vals, height=0.5, left=pass_vals, color="#dc3545", label="FAIL")
    left2 = [p + f for p, f in zip(pass_vals, fail_vals)]
    ax.barh(y_pos, warn_vals, height=0.5, left=left2, color="#fd7e14", label="WARN")
    left3 = [l + w for l, w in zip(left2, warn_vals)]
    ax.barh(y_pos, info_vals, height=0.5, left=left3, color="#17a2b8", label="INFO")

    ax.set_yticks(y_pos)
    ax.set_yticklabels([c.replace("_", " ").title() for c in cat_names], fontsize=10)
    ax.set_xlabel("Check Count", fontsize=10)
    ax.set_title("Checks by Category", fontsize=13, fontweight="bold", color="#2c3e50")
    ax.legend(fontsize=9)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    return _to_b64_png(fig)


# ============================================================================
# HTML FALLBACK CHARTS (when matplotlib unavailable)
# ============================================================================
def html_bar_chart_fallback(chart_cfg, data, columns):
    """Generate a simple HTML bar chart when matplotlib is unavailable."""
    x_col = chart_cfg.get("x_column", columns[0] if columns else "")
    y_col = chart_cfg.get("y_column", columns[1] if len(columns) > 1 else "")
    title = chart_cfg.get("title", "")

    y_vals = []
    labels = []
    for row in data:
        labels.append(str(row.get(x_col, "")))
        try:
            y_vals.append(float(row.get(y_col, 0)))
        except (ValueError, TypeError):
            y_vals.append(0)

    max_val = max(y_vals) if y_vals else 1
    html = f'<div style="margin:15px 0;"><strong>{title}</strong>'
    for lbl, yv in zip(labels, y_vals):
        pct = (yv / max_val * 100) if max_val > 0 else 0
        html += f"""
        <div style="display:flex;align-items:center;margin:3px 0;font-size:12px;">
          <div style="width:130px;text-align:right;padding-right:8px;overflow:hidden;
                      text-overflow:ellipsis;">{lbl}</div>
          <div style="flex:1;background:#e9ecef;border-radius:4px;height:18px;">
            <div style="width:{pct:.0f}%;background:#3498db;height:100%;
                        border-radius:4px;min-width:2px;"></div>
          </div>
          <div style="width:70px;padding-left:8px;font-weight:bold;">{yv:,.0f}</div>
        </div>"""
    html += "</div>"
    return html


# ============================================================================
# STATUS COLOR HELPER
# ============================================================================
def _status_color(status):
    return {
        STATUS_PASS: "#28a745", STATUS_FAIL: "#dc3545",
        STATUS_WARN: "#fd7e14", STATUS_ERROR: "#6c757d",
        STATUS_INFO: "#17a2b8",
    }.get(status, "#6c757d")


# ============================================================================
# HTML DASHBOARD GENERATION
# ============================================================================
def generate_dashboard_html(
    validation_results, kpi_results, trend_results,
    overall_status, run_duration_sec, config
):
    """Generate a rich dashboard-style HTML email."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    run_date = datetime.now().strftime("%Y-%m-%d")
    job_name = config.get("job", {}).get("name", "INFORCE_DQ")
    layer = config.get("job", {}).get("layer", "membership_mart")
    overall_color = _status_color(overall_status)
    duration_str = f"{int(run_duration_sec // 60)}m {int(run_duration_sec % 60)}s"

    # Counts
    total = len(validation_results)
    passed = sum(1 for r in validation_results if r["status"] == STATUS_PASS)
    failed = sum(1 for r in validation_results if r["status"] == STATUS_FAIL)
    warned = sum(1 for r in validation_results if r["status"] == STATUS_WARN)
    errored = sum(1 for r in validation_results if r["status"] == STATUS_ERROR)
    info_count = sum(1 for r in validation_results if r["status"] == STATUS_INFO)
    actionable = passed + failed + warned
    pass_rate = (passed / actionable * 100) if actionable > 0 else 0
    pass_color = "#28a745" if pass_rate >= 80 else ("#fd7e14" if pass_rate >= 60 else "#dc3545")

    html = f"""
    <html><head><style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 0; color: #333;
               background-color: #f5f6fa; }}
        .container {{ max-width: 1050px; margin: 0 auto; background: white;
                      border-radius: 0; overflow: hidden;
                      box-shadow: 0 2px 12px rgba(0,0,0,0.08); }}
        .header {{ background: linear-gradient(135deg, #2c3e50 0%, #8e44ad 50%, #3498db 100%);
                   padding: 28px 30px; color: white; }}
        .header h1 {{ margin: 0 0 6px 0; font-size: 22px; letter-spacing: 0.5px; }}
        .header p {{ margin: 0; font-size: 12px; opacity: 0.85; }}
        .content {{ padding: 25px 30px; }}
        h2 {{ color: #2c3e50; margin-top: 30px; font-size: 17px;
              border-left: 4px solid #8e44ad; padding-left: 12px; }}
        h3 {{ color: #2c3e50; font-size: 15px; margin-top: 20px; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 10px; font-size: 12px; }}
        th {{ background-color: #4285f4; color: #1a1a2e;
              padding: 9px 10px; text-align: left;
              font-size: 11px; font-weight: bold; letter-spacing: 0.3px; }}
        td {{ padding: 7px 10px; border-bottom: 1px solid #eee; }}
        tr:nth-child(even) {{ background-color: #f8f9fa; }}
        .badge {{ display: inline-block; padding: 3px 9px; border-radius: 4px;
                  color: white; font-weight: bold; font-size: 10px; }}
        .summary-box {{ display: inline-block; padding: 10px 18px; margin: 3px 5px 3px 0;
                        border-radius: 8px; color: white; font-size: 13px;
                        font-weight: bold; text-align: center; min-width: 100px; }}
        .kpi-row {{ display: flex; flex-wrap: wrap; gap: 12px; margin: 18px 0; }}
        .kpi-card {{ flex: 1; min-width: 120px; border-radius: 10px; padding: 18px 12px;
                     text-align: center; color: white; }}
        .kpi-label {{ font-size: 10px; font-weight: bold; opacity: 0.9;
                      text-transform: uppercase; letter-spacing: 0.5px; }}
        .kpi-value {{ font-size: 26px; font-weight: bold; margin: 6px 0 2px 0; }}
        .kpi-sub {{ font-size: 10px; opacity: 0.7; }}
        .chart-container {{ margin: 18px 0; text-align: center; }}
        .chart-container img {{ max-width: 100%; border-radius: 8px;
                                box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
        .check-card {{ margin: 12px 0; border: 1px solid #eee; border-radius: 8px;
                       overflow: hidden; }}
        .check-card-header {{ display: flex; justify-content: space-between;
                              align-items: center; padding: 12px 15px;
                              background: #fafbfc; border-bottom: 1px solid #eee; }}
        .check-card-body {{ padding: 12px 15px; }}
        .cat-badge {{ display: inline-block; padding: 2px 8px; border-radius: 10px;
                      font-size: 10px; font-weight: bold; color: white; margin-left: 8px; }}
        .footer {{ padding: 20px 30px; font-size: 11px; color: #888;
                   border-top: 1px solid #eee; background: #fafbfc; }}
        .dict-box {{ background: #f8f9fa; border: 1px solid #e9ecef; border-radius: 8px;
                     padding: 15px; margin: 10px 0; }}
    </style></head>
    <body>
    <div class="container">

    <!-- Header -->
    <div class="header">
        <h1>Inforce Membership DQ Dashboard</h1>
        <p>{job_name} &nbsp;|&nbsp; Layer: {layer} &nbsp;|&nbsp;
           Date: {run_date} &nbsp;|&nbsp; Generated: {now_str} &nbsp;|&nbsp;
           Duration: {duration_str}</p>
    </div>

    <div class="content">

    <!-- Overall Status Banner -->
    <div style="margin-bottom:18px;">
        <span class="summary-box" style="background-color:{overall_color};">
            Overall: {overall_status}</span>
        <span class="summary-box" style="background-color:#17a2b8;">
            Total Checks: {total}</span>
        <span class="summary-box" style="background-color:#28a745;">
            Passed: {passed}</span>"""

    if failed > 0:
        html += f"""
        <span class="summary-box" style="background-color:#dc3545;">
            Failed: {failed}</span>"""
    if warned > 0:
        html += f"""
        <span class="summary-box" style="background-color:#fd7e14;">
            Warnings: {warned}</span>"""
    if errored > 0:
        html += f"""
        <span class="summary-box" style="background-color:#6c757d;">
            Errors: {errored}</span>"""
    if info_count > 0:
        html += f"""
        <span class="summary-box" style="background-color:#17a2b8;">
            Info: {info_count}</span>"""

    html += "</div>"

    # ---- KPI Scorecard ----
    if kpi_results:
        html += '<h2>1. Membership KPIs</h2>'
        html += '<div class="kpi-row">'
        for kpi in kpi_results:
            val = kpi.get("value")
            color = kpi.get("color", "#3498db")
            if val is None:
                val_str = "N/A"
            elif kpi["format"] == "percentage":
                val_str = f"{val:.1f}%"
            else:
                val_str = f"{val:,.0f}"
            html += f"""
            <div class="kpi-card" style="background:{color};">
                <div class="kpi-label">{kpi['display_name']}</div>
                <div class="kpi-value">{val_str}</div>
            </div>"""
        html += "</div>"

    # ---- Pass Rate + Category Charts ----
    html += '<h2>2. Validation Summary</h2>'

    # Pass rate gauge bar
    if actionable > 0:
        html += f"""
    <div style="display:flex;gap:15px;margin:15px 0;flex-wrap:wrap;">
      <div style="flex:2;min-width:250px;background:#fafbfc;border:1px solid #eee;
                  border-radius:8px;padding:18px;">
        <div style="color:#666;font-size:11px;font-weight:bold;margin-bottom:8px;">
            PASS RATE (excluding informational)</div>
        <div style="background:#e9ecef;border-radius:10px;height:22px;overflow:hidden;">
          <div style="background:{pass_color};height:100%;width:{pass_rate:.0f}%;
                      border-radius:10px;"></div>
        </div>
        <div style="color:{pass_color};font-size:24px;font-weight:bold;
                    text-align:center;margin-top:8px;">{pass_rate:.1f}%</div>
        <div style="text-align:center;color:#888;font-size:11px;">
            {passed} passed / {actionable} actionable checks</div>
      </div>"""

        # Pass rate donut chart
        pie_b64 = generate_pass_rate_pie(passed, failed, warned, errored, info_count)
        if pie_b64:
            html += f"""
      <div style="flex:1;min-width:200px;text-align:center;">
        <img src="data:image/png;base64,{pie_b64}" style="max-width:260px;border-radius:8px;" />
      </div>"""
        html += "</div>"

    # Category bar chart
    cat_chart_b64 = generate_category_bar_chart(validation_results)
    if cat_chart_b64:
        html += f"""
    <div class="chart-container">
        <img src="data:image/png;base64,{cat_chart_b64}" alt="Checks by Category" />
    </div>"""

    # ---- Validation Details ----
    html += '<h2>3. Validation Check Details</h2>'

    for i, r in enumerate(validation_results, 1):
        color = _status_color(r["status"])
        cat_color = CATEGORY_COLORS.get(r["category"], "#95a5a6")
        dur_str = f'{r["duration_sec"]}s'

        html += f"""
    <div class="check-card" style="border-left:4px solid {color};">
      <div class="check-card-header">
        <div>
          <span style="font-weight:bold;color:#2c3e50;">
              {i}. {r['display_name']}</span>
          <span class="cat-badge" style="background:{cat_color};">
              {r['category']}</span>
        </div>
        <div>
          <span style="color:#888;font-size:11px;margin-right:10px;">
              {r['row_count']} rows | {dur_str}</span>
          <span class="badge" style="background-color:{color};">{r['status']}</span>
        </div>
      </div>
      <div class="check-card-body">
        <p style="color:#666;font-size:11px;margin:4px 0;">
          <b>Description:</b> {r['description']}</p>
        <p style="color:#666;font-size:11px;margin:4px 0;">
          <b>Detail:</b> {r['detail']}</p>"""

        # Show failure details if any
        if r["failure_details"]:
            html += '<div style="margin:8px 0;padding:8px;background:#fff0f0;border-radius:4px;font-size:11px;">'
            for fd in r["failure_details"]:
                html += f'<div style="color:#dc3545;">&#9888; {fd}</div>'
            html += "</div>"

        # Data preview table
        if r["data_preview"] and r["columns"]:
            html += '<table style="margin-top:8px;"><thead><tr>'
            for col in r["columns"]:
                html += f"<th>{col.replace('_', ' ').title()}</th>"
            html += "</tr></thead><tbody>"
            for row in r["data_preview"][:15]:
                html += "<tr>"
                for col in r["columns"]:
                    val = row.get(col, "")
                    cell_val = str(val) if val is not None else "NULL"
                    # Highlight fail values
                    cell_style = ""
                    if r["status"] == STATUS_FAIL and cell_val in ("N", "ANOMALY", "FAIL"):
                        cell_style = ' style="color:#dc3545;font-weight:bold;"'
                    html += f"<td{cell_style}>{cell_val}</td>"
                html += "</tr>"
            html += "</tbody></table>"
            if r["row_count"] > 15:
                html += f'<p style="color:#888;font-size:10px;">Showing 15 of {r["row_count"]} rows</p>'

        html += "</div></div>"

    # ---- Trends Section ----
    if trend_results:
        html += '<h2>4. Membership Trends</h2>'
        for tr in trend_results:
            html += f"""
        <div style="margin:18px 0;padding:15px;background:#fafbfc;
                     border-radius:8px;border:1px solid #eee;">
            <h3 style="color:#2c3e50;margin-top:0;">{tr['display_name']}</h3>"""

            if tr["status"] == "error":
                html += f'<p style="color:#dc3545;">Query failed: {tr.get("error", "")}</p>'
            elif tr["data"]:
                chart_b64 = generate_trend_chart(tr["chart"], tr["data"], tr["columns"])
                if chart_b64:
                    html += f"""<div class="chart-container">
                        <img src="data:image/png;base64,{chart_b64}"
                             alt="{tr['chart'].get('title', '')}" /></div>"""
                else:
                    html += html_bar_chart_fallback(tr["chart"], tr["data"], tr["columns"])

                # Also show data table for trend queries (collapsed, first 10 rows)
                if len(tr["data"]) <= 20:
                    html += '<table style="margin-top:10px;"><thead><tr>'
                    for col in tr["columns"]:
                        html += f"<th>{col.replace('_', ' ').title()}</th>"
                    html += "</tr></thead><tbody>"
                    for row in tr["data"]:
                        html += "<tr>"
                        for col in tr["columns"]:
                            html += f"<td>{row.get(col, '')}</td>"
                        html += "</tr>"
                    html += "</tbody></table>"
            else:
                html += '<p style="color:#888;">No data returned.</p>'

            html += "</div>"

    # ---- Checks Dictionary ----
    html += '<h2>5. DQ Checks Dictionary</h2>'
    html += '<div class="dict-box">'
    for r in validation_results:
        if r.get("logic") or r.get("expected_result"):
            html += f"""
            <div style="margin-bottom:12px;padding-bottom:12px;border-bottom:1px solid #eee;">
              <div style="font-weight:bold;color:#2c3e50;font-size:13px;">
                  {r['display_name']}</div>
              <table style="margin:4px 0;border:none;font-size:11px;">
                <tr><td style="border:none;width:140px;color:#888;font-weight:bold;">
                    Purpose</td><td style="border:none;">{r['description']}</td></tr>
                <tr><td style="border:none;color:#888;font-weight:bold;">
                    Logic</td><td style="border:none;">{r.get('logic', '-')}</td></tr>
                <tr><td style="border:none;color:#888;font-weight:bold;">
                    Expected Result</td><td style="border:none;">{r.get('expected_result', '-')}</td></tr>
                <tr><td style="border:none;color:#888;font-weight:bold;">
                    Failure Impact</td><td style="border:none;">{r.get('failure_implication', '-')}</td></tr>
              </table>
            </div>"""
    html += "</div>"

    # ---- Footer ----
    html += f"""
    </div><!-- end content -->
    <div class="footer">
        <p>Generated by Inforce Membership DQ Dashboard v2.0 &nbsp;|&nbsp;
           BIG DATA – Membership Team &nbsp;|&nbsp; {now_str}</p>
    </div>
    </div></body></html>"""

    return html


# ============================================================================
# EMAIL
# ============================================================================
def send_email(html_content, email_cfg, overall_status):
    """Send the dashboard HTML email via SMTP."""
    sender = email_cfg["sender"]
    recipients = email_cfg["recipients"]
    run_date = datetime.now().strftime("%Y-%m-%d")
    status_emoji = "PASSED" if overall_status == STATUS_PASS else "FAILED"

    subject = (f"{email_cfg.get('subject_prefix', 'Inforce DQ')} – "
               f"{status_emoji} – {run_date}")

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
        print(f"Dashboard email sent to {', '.join(recipients)}")
    except Exception as exc:
        print(f"Failed to send email: {exc}")


# ============================================================================
# MAIN
# ============================================================================
def main():
    # Parse Glue job arguments
    args = getResolvedOptions(
        sys.argv, ["S3_BUCKET", "JOB_NAME", "REDSHIFT_CON_NAME", "TempDir"]
    )

    s3_bucket = args["S3_BUCKET"]
    redshift_con = args["REDSHIFT_CON_NAME"]
    temp_dir = args["TempDir"]

    run_start_time = time.time()

    print("=" * 70)
    print(f"INFORCE DQ DASHBOARD – Started at {datetime.now()}")
    print("=" * 70)

    # ---- Load Config ----
    config_key = "membership_mart/config/inforce_dashboard_config.json"
    try:
        config = load_config(s3_bucket, config_key)
        print(f"Config loaded from s3://{s3_bucket}/{config_key}")
    except Exception:
        # Fallback: load from local path
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "config", "inforce_dashboard_config.json",
        )
        with open(config_path, "r") as f:
            config = json.load(f)
        print(f"Config loaded from {config_path}")

    job_cfg = config.get("job", {})
    email_cfg = config.get("email", {})

    # ---- Initialize Spark ----
    spark = init_spark()
    glueContext = GlueContext(spark.sparkContext)

    # ---- Extract Redshift credentials ----
    jdbc_conf = glueContext.extract_jdbc_conf(redshift_con)
    base_url = jdbc_conf.get("url")
    user = jdbc_conf.get("user")
    password = jdbc_conf.get("password")

    # ---- Load source data into temp view ----
    print("-" * 50)
    print("Loading source data from Redshift...")
    print("-" * 50)

    source_query = job_cfg.get("source_query", "")
    src_database = job_cfg.get("source_database", "membership_mart")
    temp_view = job_cfg.get("temp_view_name", "monthly_movement_hist")

    df = read_from_redshift(spark, src_database, source_query, base_url, user, password, temp_dir)
    df.createOrReplaceTempView(temp_view)
    print(f"Created temp view '{temp_view}' with {df.count()} rows")

    # ---- Run KPI Queries ----
    print("-" * 50)
    print("Running KPI queries...")
    print("-" * 50)
    kpi_results = run_kpi_queries(spark, config.get("kpis", {}))
    print(f"Computed {len(kpi_results)} KPIs")

    # ---- Run Validations ----
    print("-" * 50)
    print("Running validations...")
    print("-" * 50)
    validation_results = run_validations(spark, s3_bucket, config.get("validations", []))

    # ---- Run Trend Queries ----
    print("-" * 50)
    print("Running trend queries...")
    print("-" * 50)
    trend_results = run_trend_queries(spark, config.get("trends", {}))

    # ---- Determine Overall Status ----
    has_failures = any(r["status"] == STATUS_FAIL for r in validation_results)
    has_errors = any(r["status"] == STATUS_ERROR for r in validation_results)
    critical_failures = any(
        r["status"] == STATUS_FAIL and r.get("abort_on_failure")
        for r in validation_results
    )

    if has_failures or has_errors:
        overall_status = STATUS_FAIL
    else:
        overall_status = STATUS_PASS

    run_duration_sec = time.time() - run_start_time

    # ---- Generate Dashboard HTML ----
    print("-" * 50)
    print("Generating dashboard HTML...")
    print("-" * 50)

    html = generate_dashboard_html(
        validation_results, kpi_results, trend_results,
        overall_status, run_duration_sec, config,
    )

    # ---- Send Email ----
    send_email(html, email_cfg, overall_status)

    # ---- Final Summary ----
    passed = sum(1 for r in validation_results if r["status"] == STATUS_PASS)
    failed = sum(1 for r in validation_results if r["status"] == STATUS_FAIL)
    print("=" * 70)
    print(
        f"COMPLETE – Overall: {overall_status} | "
        f"Checks: {len(validation_results)} ({passed} pass, {failed} fail) | "
        f"KPIs: {len(kpi_results)} | Trends: {len(trend_results)} | "
        f"Duration: {run_duration_sec:.0f}s"
    )
    print("=" * 70)

    # ---- Fail the job if critical checks failed ----
    if critical_failures:
        failure_names = [
            r["display_name"] for r in validation_results
            if r["status"] == STATUS_FAIL and r.get("abort_on_failure")
        ]
        print(f"CRITICAL FAILURES: {', '.join(failure_names)}")
        spark.stop()
        raise Exception(f"Critical validation failures: {', '.join(failure_names)}")

    spark.stop()
    print("JOB COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    main()
