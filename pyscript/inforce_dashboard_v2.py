###################################################################################################
# SCRIPT        : inforce_dashboard_v2.py
# PURPOSE       : Inforce Membership Monthly Movement – Data Quality Dashboard
#                   1) Reads source data from Redshift into Spark temp view
#                   2) Runs config-driven validations (SQL files from S3)
#                   3) Evaluates pass/fail per check with configurable failure detection
#                   4) Runs KPI queries for membership scorecard
#                   5) Runs trend queries with animated GIF chart generation
#                   6) Generates a rich dashboard-style HTML email with inline data dictionary
#                   7) Fails the Glue job (exit 1) if critical checks fail
#
# CONFIG        : config/inforce_dashboard_config.json
# GLUE PARAMS   : --S3_BUCKET, --REDSHIFT_CON_NAME, --TempDir, --CONFIG_PATH (optional)
#
#=================================================================================================
# DATE          BY              MODIFICATION LOG
# ----------    -----------     ------------------------------------------
# 03/2026       Dhilipan        Rewrite of inforce_dash.py with dashboard email, trends, KPIs
# 03/2026       Dhilipan        v2: Animated GIF charts, inline data dictionary, category ordering
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

# Optional: Pillow for animated GIF generation
try:
    from PIL import Image as PILImage
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

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

# Unicode pictorial icons for sections and KPIs
SECTION_ICONS = {
    "kpi": "&#x1F4CA;",          # bar chart
    "summary": "&#x1F3AF;",      # target / bullseye
    "validations": "&#x1F50D;",  # magnifying glass
    "trends": "&#x1F4C8;",       # chart increasing
    "dictionary": "&#x1F4D6;",   # open book
}

KPI_ICONS = {
    "members": "&#x1F465;",      # busts in silhouette
    "add": "&#x2795;",           # heavy plus sign
    "deduct": "&#x2796;",        # heavy minus sign
    "net": "&#x1F4B9;",          # chart with upwards trend and yen
    "product": "&#x1F4E6;",      # package
    "default": "&#x2B50;",       # star
}

STATUS_ICONS = {
    STATUS_PASS: "&#x2705;",     # white check mark
    STATUS_FAIL: "&#x274C;",     # cross mark
    STATUS_WARN: "&#x26A0;",     # warning sign
    STATUS_ERROR: "&#x1F6A8;",   # rotating light
    STATUS_INFO: "&#x2139;",     # information source
}

CATEGORY_ICONS = {
    "balance": "&#x2696;",       # scales
    "anomaly": "&#x1F50E;",      # magnifying glass right
    "completeness": "&#x2611;",  # ballot box with check
    "rolling_average": "&#x1F4CA;",  # bar chart
    "statistical": "&#x1F4D0;",  # triangular ruler
    "count_check": "&#x1F522;",  # input numbers
    "informational": "&#x1F4AC;", # speech balloon
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

            # Capture all data rows for the email table
            result["columns"] = [f.name for f in result_df.schema.fields]
            preview_rows = result_df.toPandas().to_dict("records")
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
# CHART GENERATION (matplotlib → animated GIF via Pillow)
# ============================================================================
def _fig_to_pil(fig, dpi=100):
    """Convert a matplotlib figure to a PIL Image (RGBA → RGB with white bg)."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight", facecolor="#FAFBFC")
    plt.close(fig)
    buf.seek(0)
    if HAS_PIL:
        img = PILImage.open(buf).convert("RGBA")
        background = PILImage.new("RGBA", img.size, (250, 251, 252, 255))
        background.paste(img, mask=img)
        return background.convert("RGB")
    return None


def _to_b64_png(fig):
    """Save matplotlib figure to base64 PNG string."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight", facecolor="#FAFBFC")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def _to_b64_gif(fig):
    """Save a single matplotlib figure as a static GIF via Pillow, or fall back to PNG."""
    if not HAS_PIL:
        return _to_b64_png(fig), "image/png"
    pil_img = _fig_to_pil(fig, dpi=130)
    if pil_img is None:
        return _to_b64_png(fig), "image/png"
    gif_buf = io.BytesIO()
    pil_img.save(gif_buf, format="GIF")
    gif_buf.seek(0)
    return base64.b64encode(gif_buf.read()).decode("utf-8"), "image/gif"


def _assemble_animated_gif(frames, durations=None):
    """Assemble PIL Image frames into an animated GIF. Returns (b64_str, mime)."""
    if not frames:
        return None, None
    if durations is None:
        durations = [80] * (len(frames) - 1) + [2000]
    gif_buf = io.BytesIO()
    frames[0].save(gif_buf, format="GIF", save_all=True,
                   append_images=frames[1:], duration=durations, loop=0)
    gif_buf.seek(0)
    return base64.b64encode(gif_buf.read()).decode("utf-8"), "image/gif"


def _render_trend_frame(chart_cfg, data, columns, fraction=1.0):
    """Render a single frame of a trend/bar/grouped chart scaled by fraction (0..1).
    Returns (fig, is_pie) where is_pie indicates the chart was a pie (handled differently)."""
    import pandas as pd
    chart_type = chart_cfg.get("type", "trend")
    title = chart_cfg.get("title", "")
    x_col = chart_cfg.get("x_column", columns[0] if columns else "")
    y_col = chart_cfg.get("y_column", columns[1] if len(columns) > 1 else "")
    group_by = chart_cfg.get("group_by")
    top_n = chart_cfg.get("top_n")
    df = pd.DataFrame(data)

    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor("#FAFBFC")
    ax.set_facecolor("#FAFBFC")

    if chart_type == "trend" and not group_by:
        x_vals = df[x_col].astype(str).tolist()
        full_y = pd.to_numeric(df[y_col], errors="coerce").fillna(0).tolist()
        n_points = max(1, int(len(x_vals) * fraction))
        x_draw = list(range(n_points))
        y_draw = full_y[:n_points]

        ax.fill_between(x_draw, y_draw, alpha=0.15, color="#45B7D1")
        ax.plot(x_draw, y_draw, color="#45B7D1", linewidth=2.5,
                marker="o", markersize=6, markerfacecolor="#FF6B6B",
                markeredgecolor="white", markeredgewidth=1.5)
        ax.set_xticks(range(len(x_vals)))
        ax.set_xticklabels(x_vals, rotation=45, ha="right", fontsize=8)
        if full_y:
            ax.set_ylim(0, max(full_y) * 1.2)
        if fraction >= 1.0:
            n_pts = len(full_y)
            label_step = 1 if n_pts <= 8 else (2 if n_pts <= 16 else (3 if n_pts <= 30 else max(1, n_pts // 10)))
            for i, v in enumerate(full_y):
                if i == 0 or i == n_pts - 1 or i % label_step == 0:
                    y_offset = 12 if (i % 2 == 0) else -16
                    va = "bottom" if y_offset > 0 else "top"
                    ax.annotate(f"{v:,.0f}", (i, v), textcoords="offset points",
                                xytext=(0, y_offset), ha="center", va=va,
                                fontsize=7, fontweight="bold", color="#2c3e50")

    elif chart_type == "trend" and group_by:
        groups = df[group_by].unique()
        if top_n:
            totals = df.groupby(group_by)[y_col].apply(
                lambda x: pd.to_numeric(x, errors="coerce").sum()
            ).nlargest(top_n)
            groups = totals.index.tolist()
        x_labels = df[x_col].astype(str).unique()
        n_points = max(1, int(len(x_labels) * fraction))
        for idx, grp in enumerate(groups):
            grp_df = df[df[group_by] == grp].sort_values(x_col)
            x_vals = grp_df[x_col].astype(str).tolist()[:n_points]
            y_vals = pd.to_numeric(grp_df[y_col], errors="coerce").fillna(0).tolist()[:n_points]
            color = VIBRANT_COLORS[idx % len(VIBRANT_COLORS)]
            ax.plot(range(len(x_vals)), y_vals, color=color, linewidth=2,
                    marker="o", markersize=4, label=str(grp))
        ax.legend(fontsize=8, loc="upper left", framealpha=0.9)
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
            vals = []
            for xl in x_labels:
                match = grp_df[grp_df[x_col].astype(str) == xl]
                vals.append(pd.to_numeric(match[y_col], errors="coerce").sum() if len(match) > 0 else 0)
            vals = [v * fraction for v in vals]
            color = VIBRANT_COLORS[idx % len(VIBRANT_COLORS)]
            offset = (idx - n_groups / 2 + 0.5) * bar_width
            ax.bar(x_pos + offset, vals, width=bar_width, label=str(grp), color=color)
        ax.set_xticks(x_pos)
        ax.set_xticklabels(x_labels, rotation=45, ha="right", fontsize=8)
        ax.legend(fontsize=8)
        # Fix y-axis to full data range
        all_vals = pd.to_numeric(df[y_col], errors="coerce").fillna(0)
        ax.set_ylim(0, all_vals.max() * 1.15 if len(all_vals) else 1)

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
            vals = [v * fraction for v in vals]
            color = VIBRANT_COLORS[idx % len(VIBRANT_COLORS)]
            ax.bar(x_pos, vals, bottom=bottom, label=str(grp), color=color, width=0.6)
            bottom += np.array(vals)
        ax.set_xticks(x_pos)
        ax.set_xticklabels(x_labels, rotation=45, ha="right", fontsize=8)
        ax.legend(fontsize=7, loc="upper left", ncol=2)

    elif chart_type == "bar":
        x_vals = df[x_col].astype(str).tolist()
        full_y = pd.to_numeric(df[y_col], errors="coerce").fillna(0).tolist()
        y_vals = [v * fraction for v in full_y]
        max_y = max(full_y) if full_y else 1
        colors = [VIBRANT_COLORS[i % len(VIBRANT_COLORS)] for i in range(len(x_vals))]
        ax.bar(range(len(x_vals)), y_vals, color=colors, width=0.6)
        ax.set_xticks(range(len(x_vals)))
        ax.set_xticklabels(x_vals, rotation=45, ha="right", fontsize=8)
        ax.set_ylim(0, max_y * 1.15)
        if fraction >= 1.0:
            for i, v in enumerate(y_vals):
                ax.annotate(f"{v:,.0f}", (i, v), textcoords="offset points",
                            xytext=(0, 5), ha="center", fontsize=8, fontweight="bold")

    elif chart_type == "pie":
        label_col = chart_cfg.get("label_column", x_col)
        value_col = chart_cfg.get("value_column", y_col)
        labels = df[label_col].astype(str).tolist()
        values = pd.to_numeric(df[value_col], errors="coerce").fillna(0).tolist()
        if top_n and len(labels) > top_n:
            paired = sorted(zip(values, labels), reverse=True)
            top_vals = [v for v, _ in paired[:top_n]]
            top_labels = [l for _, l in paired[:top_n]]
            others_sum = sum(v for v, _ in paired[top_n:])
            if others_sum > 0:
                top_vals.append(others_sum)
                top_labels.append("Others")
            values, labels = top_vals, top_labels
        # Animate by showing N slices progressively
        n_show = max(1, int(len(values) * fraction))
        show_vals = values[:n_show] + ([sum(values[n_show:])] if n_show < len(values) else [])
        show_labels = labels[:n_show] + (["..."] if n_show < len(values) else [])
        show_colors = [VIBRANT_COLORS[i % len(VIBRANT_COLORS)] for i in range(len(show_labels))]
        wedges, texts, autotexts = ax.pie(
            show_vals, labels=show_labels, colors=show_colors,
            autopct="%1.1f%%", startangle=140, pctdistance=0.78,
            wedgeprops={"edgecolor": "white", "linewidth": 2},
        )
        for t in autotexts:
            t.set_fontsize(9)
            t.set_fontweight("bold")
            t.set_color("white")
        for t in texts:
            t.set_fontsize(9)
            t.set_color("#2c3e50")
        ax.set_title(title, fontsize=13, fontweight="bold", color="#2c3e50", pad=15)
        plt.tight_layout()
        return fig
    else:
        plt.close(fig)
        return None

    ax.set_title(title, fontsize=13, fontweight="bold", color="#2c3e50", pad=12)
    ax.set_ylabel(y_col.replace("_", " ").title(), fontsize=10, color="#2c3e50")
    ax.grid(axis="y", alpha=0.3, linestyle="--")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    plt.tight_layout()
    return fig


def generate_trend_chart(chart_cfg, data, columns):
    """Generate an animated GIF trend/bar/grouped chart. Falls back to static PNG.
    Returns (base64_string, mime_type) or (None, None)."""
    if not HAS_MATPLOTLIB or not data:
        return None, None

    n_frames = 10

    # If PIL available, generate animated GIF
    if HAS_PIL:
        frames = []
        for i in range(n_frames + 1):
            frac = i / n_frames
            fig = _render_trend_frame(chart_cfg, data, columns, fraction=frac)
            if fig is None:
                return None, None
            pil_img = _fig_to_pil(fig, dpi=100)
            if pil_img:
                frames.append(pil_img)
        if frames:
            return _assemble_animated_gif(frames)

    # Fallback: static PNG
    fig = _render_trend_frame(chart_cfg, data, columns, fraction=1.0)
    if fig is None:
        return None, None
    return _to_b64_png(fig), "image/png"


def generate_pass_rate_pie(passed, failed, warned, errored, info_count):
    """Generate an animated donut chart for validation pass rate.
    Returns (base64_string, mime_type) or (None, None)."""
    if not HAS_MATPLOTLIB:
        return None, None

    labels = []
    sizes = []
    colors = []
    if passed > 0:
        labels.append(f"PASS ({passed})"); sizes.append(passed); colors.append("#28a745")
    if failed > 0:
        labels.append(f"FAIL ({failed})"); sizes.append(failed); colors.append("#dc3545")
    if warned > 0:
        labels.append(f"WARN ({warned})"); sizes.append(warned); colors.append("#fd7e14")
    if errored > 0:
        labels.append(f"ERROR ({errored})"); sizes.append(errored); colors.append("#6c757d")
    if info_count > 0:
        labels.append(f"INFO ({info_count})"); sizes.append(info_count); colors.append("#17a2b8")
    if not sizes:
        return None, None

    def _make_pie_frame(n_show):
        fig, ax = plt.subplots(figsize=(5, 5))
        fig.patch.set_facecolor("#FAFBFC")
        s_vals = sizes[:n_show] + ([sum(sizes[n_show:])] if n_show < len(sizes) else [])
        s_labels = labels[:n_show] + (["..."] if n_show < len(sizes) else [])
        s_colors = colors[:n_show] + (["#e0e0e0"] if n_show < len(sizes) else [])
        wedges, texts, autotexts = ax.pie(
            s_vals, labels=s_labels, colors=s_colors, autopct="%1.0f%%",
            startangle=140, pctdistance=0.75,
            wedgeprops={"edgecolor": "white", "linewidth": 2, "width": 0.4},
        )
        for t in autotexts:
            t.set_fontsize(10); t.set_fontweight("bold")
        ax.set_title("Validation Results", fontsize=14, fontweight="bold",
                     color="#2c3e50", pad=15)
        plt.tight_layout()
        return fig

    if HAS_PIL:
        frames = []
        for n in range(1, len(sizes) + 1):
            fig = _make_pie_frame(n)
            pil_img = _fig_to_pil(fig, dpi=100)
            if pil_img:
                frames.append(pil_img)
        if frames:
            # Duplicate last frame for hold
            durations = [300] * (len(frames) - 1) + [2000]
            return _assemble_animated_gif(frames, durations)

    # Fallback
    fig = _make_pie_frame(len(sizes))
    return _to_b64_png(fig), "image/png"


def generate_category_bar_chart(results):
    """Generate an animated horizontal bar chart of pass/fail counts by category.
    Balance is 1st, Anomaly is 2nd, then the rest alphabetically.
    Returns (base64_string, mime_type) or (None, None)."""
    if not HAS_MATPLOTLIB:
        return None, None

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
        return None, None

    import numpy as np

    # Force ordering: balance first, anomaly second, rest alphabetically
    priority = {"balance": 0, "anomaly": 1}
    cat_names = sorted(cats.keys(), key=lambda c: (priority.get(c, 99), c))

    pass_vals = [cats[c]["pass"] for c in cat_names]
    fail_vals = [cats[c]["fail"] for c in cat_names]
    warn_vals = [cats[c]["warn"] for c in cat_names]
    info_vals = [cats[c]["info"] for c in cat_names]
    max_total = max(p + f + w + i for p, f, w, i in
                    zip(pass_vals, fail_vals, warn_vals, info_vals)) if cat_names else 1

    def _make_cat_frame(fraction):
        fig, ax = plt.subplots(figsize=(8, max(3, len(cat_names) * 0.7)))
        fig.patch.set_facecolor("#FAFBFC")
        ax.set_facecolor("#FAFBFC")
        y_pos = np.arange(len(cat_names))
        pv = [v * fraction for v in pass_vals]
        fv = [v * fraction for v in fail_vals]
        wv = [v * fraction for v in warn_vals]
        iv = [v * fraction for v in info_vals]
        ax.barh(y_pos, pv, height=0.5, color="#28a745", label="PASS")
        ax.barh(y_pos, fv, height=0.5, left=pv, color="#dc3545", label="FAIL")
        left2 = [p + f for p, f in zip(pv, fv)]
        ax.barh(y_pos, wv, height=0.5, left=left2, color="#fd7e14", label="WARN")
        left3 = [l + w for l, w in zip(left2, wv)]
        ax.barh(y_pos, iv, height=0.5, left=left3, color="#17a2b8", label="INFO")
        ax.set_yticks(y_pos)
        ax.set_yticklabels([c.replace("_", " ").title() for c in cat_names], fontsize=10)
        ax.set_xlabel("Check Count", fontsize=10)
        ax.set_xlim(0, max_total * 1.15)
        ax.set_title("Checks by Category", fontsize=13, fontweight="bold", color="#2c3e50")
        ax.legend(fontsize=9)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        plt.tight_layout()
        return fig

    n_frames = 10
    if HAS_PIL:
        frames = []
        for i in range(n_frames + 1):
            frac = i / n_frames
            fig = _make_cat_frame(frac)
            pil_img = _fig_to_pil(fig, dpi=100)
            if pil_img:
                frames.append(pil_img)
        if frames:
            return _assemble_animated_gif(frames)

    # Fallback
    fig = _make_cat_frame(1.0)
    return _to_b64_png(fig), "image/png"


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

    # Build status pill data for the banner
    status_pills = [
        (overall_color, STATUS_ICONS.get(overall_status, ""), f"Overall: {overall_status}"),
        ("#17a2b8", "&#x1F4CB;", f"Total Checks: {total}"),
        ("#28a745", "&#x2705;", f"Passed: {passed}"),
    ]
    if failed > 0:
        status_pills.append(("#dc3545", "&#x274C;", f"Failed: {failed}"))
    if warned > 0:
        status_pills.append(("#fd7e14", "&#x26A0;", f"Warnings: {warned}"))
    if errored > 0:
        status_pills.append(("#6c757d", "&#x1F6A8;", f"Errors: {errored}"))
    if info_count > 0:
        status_pills.append(("#17a2b8", "&#x2139;", f"Info: {info_count}"))

    html = f"""
    <html><head><style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 0; color: #333;
               background-color: #f0f2f5; line-height: 1.6; }}
        .container {{ max-width: 1080px; margin: 0 auto; background: white;
                      border-radius: 12px; overflow: hidden;
                      box-shadow: 0 4px 20px rgba(0,0,0,0.12); }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 30%, #f093fb 60%, #f5576c 100%);
                   padding: 35px 35px 30px 35px; color: white;
                   position: relative; overflow: hidden; }}
        .header::before {{ content: ''; position: absolute; top: -50%; left: -50%;
                          width: 200%; height: 200%; background: radial-gradient(circle,
                          rgba(255,255,255,0.1) 0%, transparent 60%);
                          animation: shimmer 3s ease-in-out infinite; }}
        .header h1 {{ margin: 0 0 8px 0; font-size: 26px; letter-spacing: 0.5px;
                      text-shadow: 0 2px 4px rgba(0,0,0,0.2); position: relative; }}
        .header p {{ margin: 0; font-size: 12px; opacity: 0.9; position: relative; }}
        .header-icon {{ font-size: 32px; margin-right: 10px; vertical-align: middle; }}
        .content {{ padding: 30px 35px; }}

        h2 {{ color: #2c3e50; margin-top: 40px; margin-bottom: 18px; font-size: 18px;
              padding: 10px 16px; border-radius: 8px;
              background: linear-gradient(90deg, #f8f9ff 0%, #fff 100%);
              border-left: 5px solid #764ba2; }}
        h2 .section-icon {{ font-size: 20px; margin-right: 8px; vertical-align: middle; }}

        h3 {{ color: #2c3e50; font-size: 15px; margin-top: 22px; margin-bottom: 12px; }}

        p {{ margin-bottom: 12px; }}

        table {{ border-collapse: collapse; width: 100%; margin-top: 12px; margin-bottom: 16px;
                 font-size: 12px; border-radius: 8px; overflow: hidden;
                 box-shadow: 0 1px 4px rgba(0,0,0,0.06); table-layout: auto; }}
        th {{ background: linear-gradient(135deg, #eef2ff 0%, #f0e6ff 100%); color: #4a2d7a;
              padding: 10px 12px; text-align: left;
              font-size: 11px; font-weight: bold; letter-spacing: 0.5px;
              text-transform: uppercase; border-bottom: 2px solid #764ba2;
              white-space: nowrap; }}
        td {{ padding: 8px 12px; border-bottom: 1px solid #f0f0f0; white-space: nowrap; }}
        tr:nth-child(even) {{ background-color: #fafbfe; }}
        tr:hover {{ background-color: #f0f3ff; }}
        .wide-table-wrap {{ overflow-x: auto; max-width: 100%; }}

        .badge {{ display: inline-block; padding: 4px 12px; border-radius: 12px;
                  color: white; font-weight: bold; font-size: 10px;
                  letter-spacing: 0.5px; text-transform: uppercase;
                  box-shadow: 0 2px 4px rgba(0,0,0,0.15); }}

        .summary-pill {{ display: inline-flex; align-items: center; gap: 6px;
                        padding: 12px 20px; margin: 4px 6px 4px 0;
                        border-radius: 25px; color: white; font-size: 13px;
                        font-weight: bold; text-align: center;
                        box-shadow: 0 3px 8px rgba(0,0,0,0.15);
                        transition: transform 0.2s; }}
        .summary-pill .pill-icon {{ font-size: 18px; }}

        .kpi-row {{ margin: 22px 0; }}
        .kpi-card {{ border-radius: 16px; padding: 22px 16px;
                     text-align: center; position: relative;
                     overflow: hidden; box-shadow: 0 6px 20px rgba(0,0,0,0.10);
                     border: none; }}
        .kpi-card::before {{
            content: ''; position: absolute; top: -50%; left: -50%;
            width: 200%; height: 200%;
            background: linear-gradient(45deg,
                transparent 30%, rgba(255,255,255,0.15) 50%, transparent 70%);
            animation: kpi-shimmer 3s ease-in-out infinite;
        }}
        @keyframes kpi-shimmer {{
            0% {{ transform: translateX(-100%) rotate(25deg); }}
            100% {{ transform: translateX(100%) rotate(25deg); }}
        }}
        @keyframes kpi-float {{
            0%, 100% {{ transform: translateY(0px); }}
            50% {{ transform: translateY(-6px); }}
        }}
        @keyframes kpi-pulse {{
            0%, 100% {{ transform: scale(1); opacity: 1; }}
            50% {{ transform: scale(1.05); opacity: 0.9; }}
        }}
        .kpi-icon {{ font-size: 34px; margin-bottom: 10px; position: relative;
                     display: inline-block;
                     animation: kpi-float 3s ease-in-out infinite; }}
        .kpi-label {{ font-size: 11px; font-weight: bold;
                      text-transform: uppercase; letter-spacing: 0.8px;
                      position: relative; }}
        .kpi-value {{ font-size: 32px; font-weight: bold; margin: 10px 0 4px 0;
                      position: relative;
                      text-shadow: 0 2px 8px rgba(0,0,0,0.08);
                      animation: kpi-pulse 4s ease-in-out infinite; }}

        .chart-container {{ margin: 22px 0; text-align: center; }}
        .chart-container img {{ max-width: 100%; border-radius: 10px;
                                box-shadow: 0 4px 15px rgba(0,0,0,0.1);
                                border: 1px solid #eef0f5; }}

        .check-card {{ margin: 16px 0; border: 1px solid #e8eaf0; border-radius: 10px;
                       overflow: hidden; box-shadow: 0 2px 6px rgba(0,0,0,0.04); }}
        .check-card-header {{ display: flex; justify-content: space-between;
                              align-items: center; padding: 14px 18px;
                              background: linear-gradient(90deg, #fafbfe 0%, #fff 100%);
                              border-bottom: 1px solid #eee; }}
        .check-card-body {{ padding: 16px 18px; }}
        .check-title-icon {{ font-size: 16px; margin-right: 6px; vertical-align: middle; }}

        .cat-badge {{ display: inline-flex; align-items: center; gap: 4px;
                      padding: 3px 10px; border-radius: 12px;
                      font-size: 10px; font-weight: bold; color: white; margin-left: 8px; }}

        .footer {{ padding: 25px 35px; font-size: 11px; color: #888;
                   border-top: 2px solid #eee;
                   background: linear-gradient(90deg, #fafbfc 0%, #f5f6fa 100%); }}
        .footer-icon {{ font-size: 14px; margin-right: 4px; vertical-align: middle; }}

        .dict-box {{ background: linear-gradient(135deg, #f8f9ff 0%, #fef9ff 100%);
                     border: 1px solid #e8e0f0; border-radius: 10px;
                     padding: 20px; margin: 14px 0; }}
        .dict-item {{ margin-bottom: 16px; padding-bottom: 16px; border-bottom: 1px solid #eee; }}
        .dict-item:last-child {{ border-bottom: none; margin-bottom: 0; padding-bottom: 0; }}
        .dict-icon {{ font-size: 14px; margin-right: 6px; vertical-align: middle; }}

        .trend-card {{ margin: 22px 0; padding: 20px; background: linear-gradient(135deg,
                       #fafbfe 0%, #f8f5ff 100%);
                       border-radius: 12px; border: 1px solid #e8e0f0;
                       box-shadow: 0 2px 8px rgba(0,0,0,0.04); }}
        .trend-title {{ color: #2c3e50; margin-top: 0; margin-bottom: 14px; }}
        .trend-icon {{ font-size: 18px; margin-right: 6px; vertical-align: middle; }}

        .spacer {{ height: 20px; }}
        .spacer-sm {{ height: 12px; }}
    </style></head>
    <body>
    <div class="container">

    <!-- Header with gradient + icon -->
    <div class="header">
        <h1><span class="header-icon">&#x1F4CA;</span>
            Inforce Membership DQ Dashboard</h1>
        <p>&#x1F3E2; {job_name} &nbsp;&bull;&nbsp;
           &#x1F4C2; Layer: {layer} &nbsp;&bull;&nbsp;
           &#x1F4C5; {run_date} &nbsp;&bull;&nbsp;
           &#x23F1; Generated: {now_str} &nbsp;&bull;&nbsp;
           &#x23F3; Duration: {duration_str}</p>
    </div>

    <div class="content">

    <div class="spacer-sm"></div>

    <!-- Overall Status Banner with pill badges -->
    <div style="margin-bottom:24px;text-align:center;">"""

    for pill_color, pill_icon, pill_text in status_pills:
        html += f"""
        <span class="summary-pill" style="background:{pill_color};">
            <span class="pill-icon">{pill_icon}</span> {pill_text}</span>"""

    html += """
    </div>

    <div class="spacer"></div>"""

    # ---- KPI Scorecard (animated parallel tiles via HTML table) ----
    # Each tile: gradient background, dark contrasting value, animated icon
    KPI_TILE_STYLES = [
        # (background_gradient, value_color, label_color, fallback_bg)
        ("linear-gradient(135deg, #e8f0fe 0%, #d4e4ff 100%)", "#1a237e", "#3949ab", "#d4e4ff"),
        ("linear-gradient(135deg, #fce4ec 0%, #f8bbd0 100%)", "#880e4f", "#ad1457", "#f8bbd0"),
        ("linear-gradient(135deg, #e0f7fa 0%, #b2ebf2 100%)", "#006064", "#00838f", "#b2ebf2"),
        ("linear-gradient(135deg, #e8f5e9 0%, #c8e6c9 100%)", "#1b5e20", "#2e7d32", "#c8e6c9"),
        ("linear-gradient(135deg, #fff3e0 0%, #ffe0b2 100%)", "#e65100", "#ef6c00", "#ffe0b2"),
        ("linear-gradient(135deg, #f3e5f5 0%, #e1bee7 100%)", "#4a148c", "#6a1b9a", "#e1bee7"),
        ("linear-gradient(135deg, #fbe9e7 0%, #ffccbc 100%)", "#bf360c", "#d84315", "#ffccbc"),
        ("linear-gradient(135deg, #e8eaf6 0%, #c5cae9 100%)", "#283593", "#3949ab", "#c5cae9"),
    ]

    # Filter out "retention" KPIs
    filtered_kpis = [
        kpi for kpi in kpi_results
        if "retention" not in kpi.get("name", "").lower()
        and "retention" not in kpi.get("display_name", "").lower()
    ]

    if filtered_kpis:
        html += f'<h2><span class="section-icon">{SECTION_ICONS["kpi"]}</span> 1. Membership KPIs</h2>'
        html += '<div class="spacer-sm"></div>'
        # Use HTML table for guaranteed side-by-side rendering in all email clients
        n_kpis = len(filtered_kpis)
        td_width = max(int(100 / n_kpis), 15)
        html += f'<table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:22px 0;"><tr>'

        for idx, kpi in enumerate(filtered_kpis):
            val = kpi.get("value")
            icon_key = kpi.get("name", "default")
            # Map KPI name to icon
            kpi_icon = KPI_ICONS.get("default")
            for key in KPI_ICONS:
                if key in icon_key or key in kpi.get("display_name", "").lower():
                    kpi_icon = KPI_ICONS[key]
                    break
            if val is None:
                val_str = "N/A"
            elif kpi["format"] == "percentage":
                val_str = f"{val:.1f}%"
            else:
                val_str = f"{val:,.0f}"

            # Cycle through tile styles
            gradient, val_color, lbl_color, fallback_bg = KPI_TILE_STYLES[idx % len(KPI_TILE_STYLES)]
            anim_delay = f"{idx * 0.4:.1f}s"

            html += f"""
            <td width="{td_width}%" valign="top" style="padding:0 8px;">
              <div class="kpi-card" style="background:{gradient};
                          background-color:{fallback_bg};">
                <div class="kpi-icon" style="animation-delay:{anim_delay};">{kpi_icon}</div>
                <div class="kpi-label" style="color:{lbl_color};">{kpi['display_name']}</div>
                <div class="kpi-value" style="color:{val_color};animation-delay:{anim_delay};">
                    {val_str}</div>
              </div>
            </td>"""
        html += "</tr></table>"
        html += '<div class="spacer"></div>'

    # ---- Pass Rate + Category Charts ----
    html += f'<h2><span class="section-icon">{SECTION_ICONS["summary"]}</span> 2. Validation Summary</h2>'
    html += '<div class="spacer-sm"></div>'

    # Pass rate gauge bar with colorful gradient
    if actionable > 0:
        # Animated gradient for the pass rate bar
        bar_gradient = (
            f"linear-gradient(90deg, {pass_color} 0%, {pass_color}cc {pass_rate:.0f}%, "
            f"#e9ecef {pass_rate:.0f}%)"
        )
        html += f"""
    <div style="display:flex;gap:18px;margin:18px 0;flex-wrap:wrap;">
      <div style="flex:2;min-width:280px;background:linear-gradient(135deg, #fafbfe 0%, #f5f0ff 100%);
                  border:1px solid #e8e0f0;border-radius:12px;padding:22px;">
        <div style="color:#555;font-size:12px;font-weight:bold;margin-bottom:10px;
                    letter-spacing:0.5px;">
            &#x1F3AF; PASS RATE <span style="color:#aaa;font-weight:normal;">
            (excluding informational)</span></div>

        <div style="background:#e9ecef;border-radius:12px;height:28px;overflow:hidden;
                    box-shadow:inset 0 2px 4px rgba(0,0,0,0.1);">
          <div style="background:linear-gradient(90deg, {pass_color}, {pass_color}bb);
                      height:100%;width:{pass_rate:.0f}%;border-radius:12px;
                      box-shadow:0 2px 6px {pass_color}66;"></div>
        </div>

        <div style="text-align:center;margin-top:12px;">
          <span style="color:{pass_color};font-size:36px;font-weight:bold;
                       text-shadow:0 2px 4px {pass_color}33;">{pass_rate:.1f}%</span>
        </div>

        <div style="text-align:center;margin-top:6px;display:flex;justify-content:center;gap:16px;">
          <span style="color:#28a745;font-size:12px;font-weight:bold;">
              &#x2705; {passed} Passed</span>
          <span style="color:#dc3545;font-size:12px;font-weight:bold;">
              &#x274C; {failed} Failed</span>
          <span style="color:#888;font-size:12px;">
              of {actionable} actionable</span>
        </div>
      </div>"""

        # Pass rate donut chart (animated GIF)
        pie_b64, pie_mime = generate_pass_rate_pie(passed, failed, warned, errored, info_count)
        if pie_b64:
            html += f"""
      <div style="flex:1;min-width:220px;text-align:center;background:linear-gradient(135deg,
                  #fafbfe 0%, #f5f0ff 100%);border:1px solid #e8e0f0;
                  border-radius:12px;padding:15px;">
        <img src="data:{pie_mime};base64,{pie_b64}"
             style="max-width:280px;border-radius:10px;
                    box-shadow:0 3px 10px rgba(0,0,0,0.08);" />
      </div>"""
        html += "</div>"

    html += '<div class="spacer-sm"></div>'

    # Category bar chart (animated GIF, balance 1st, anomaly 2nd)
    cat_chart_b64, cat_mime = generate_category_bar_chart(validation_results)
    if cat_chart_b64:
        html += f"""
    <div class="chart-container" style="background:linear-gradient(135deg, #fafbfe 0%,
                #f5f0ff 100%);border:1px solid #e8e0f0;border-radius:12px;padding:18px;">
        <img src="data:{cat_mime};base64,{cat_chart_b64}" alt="Checks by Category" />
    </div>"""

    html += '<div class="spacer"></div>'

    # ---- Validation Details ----
    html += f'<h2><span class="section-icon">{SECTION_ICONS["validations"]}</span> 3. Validation Check Details</h2>'
    html += '<div class="spacer-sm"></div>'

    for i, r in enumerate(validation_results, 1):
        color = _status_color(r["status"])
        cat_color = CATEGORY_COLORS.get(r["category"], "#95a5a6")
        cat_icon = CATEGORY_ICONS.get(r["category"], "&#x1F4CB;")
        status_icon = STATUS_ICONS.get(r["status"], "")
        dur_str = f'{r["duration_sec"]}s'

        html += f"""
    <div class="check-card" style="border-left:5px solid {color};">
      <div class="check-card-header">
        <div>
          <span class="check-title-icon">{status_icon}</span>
          <span style="font-weight:bold;color:#2c3e50;font-size:13px;">
              {i}. {r['display_name']}</span>
          <span class="cat-badge" style="background:{cat_color};">
              {cat_icon} {r['category']}</span>
        </div>
        <div>
          <span style="color:#888;font-size:11px;margin-right:10px;">
              &#x1F4C4; {r['row_count']} rows &nbsp;|&nbsp; &#x23F1; {dur_str}</span>
          <span class="badge" style="background-color:{color};">{r['status']}</span>
        </div>
      </div>
      <div class="check-card-body">
        <p style="color:#555;font-size:12px;margin:6px 0 10px 0;">
          &#x1F4DD; <b>Description:</b> {r['description']}</p>

        <p style="color:#555;font-size:12px;margin:6px 0 10px 0;">
          &#x1F527; <b>Detail:</b> {r['detail']}</p>"""

        # Show failure details if any
        if r["failure_details"]:
            html += """<div style="margin:12px 0;padding:12px 14px;
                        background:linear-gradient(90deg, #fff0f0 0%, #fff5f5 100%);
                        border-radius:8px;border-left:4px solid #dc3545;font-size:12px;">"""
            for fd in r["failure_details"]:
                html += f'<div style="color:#dc3545;margin:4px 0;">&#x1F6A8; {fd}</div>'
            html += "</div>"

        html += '<div class="spacer-sm"></div>'

        # Data preview table (collapsible if >10 rows)
        if r["data_preview"] and r["columns"]:
            n_rows = r["row_count"]
            is_large = n_rows > 10

            if is_large:
                html += f"""
            <details style="margin-top:10px;">
              <summary style="cursor:pointer;padding:10px 14px;background:linear-gradient(135deg,
                        #f8f9ff 0%, #fef9ff 100%);border:1px solid #e8e0f0;border-radius:8px;
                        font-size:12px;font-weight:bold;color:#764ba2;list-style:none;
                        display:flex;align-items:center;gap:8px;">
                <span style="transition:transform 0.2s;">&#x25B6;</span>
                &#x1F4CB; Data Table ({n_rows} rows) — Click to expand
              </summary>
              <div class="wide-table-wrap">"""
            else:
                html += '<div class="wide-table-wrap">'

            html += '<table style="margin-top:10px;"><thead><tr>'
            for col in r["columns"]:
                html += f"<th>{col.replace('_', ' ').title()}</th>"
            html += "</tr></thead><tbody>"
            for row in r["data_preview"]:
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
            html += f"""<p style="color:#999;font-size:10px;margin-top:8px;
                        font-style:italic;">&#x1F4C4; {n_rows} rows</p>"""

            if is_large:
                html += "</div></details>"
            else:
                html += "</div>"

        # ---- Inline Data Dictionary for this check ----
        if r.get("logic") or r.get("expected_result"):
            cat_color_d = CATEGORY_COLORS.get(r.get("category", ""), "#95a5a6")
            html += f"""
        <div style="margin-top:16px;padding:16px 18px;
                    background:linear-gradient(135deg, #f8f9ff 0%, #fef9ff 100%);
                    border:1px solid #e8e0f0;border-radius:10px;">
          <div style="font-weight:bold;color:#764ba2;font-size:12px;margin-bottom:10px;
                      letter-spacing:0.5px;">
              &#x1F4D6; CHECK DICTIONARY</div>
          <table style="margin:0;border:none;font-size:12px;box-shadow:none;">
            <tr><td style="border:none;width:130px;color:#764ba2;font-weight:bold;
                           vertical-align:top;padding:4px 8px;">
                &#x1F3AF; Purpose</td>
                <td style="border:none;padding:4px 8px;color:#444;">
                {r['description']}</td></tr>
            <tr><td style="border:none;color:#764ba2;font-weight:bold;
                           vertical-align:top;padding:4px 8px;">
                &#x2699; Logic</td>
                <td style="border:none;padding:4px 8px;color:#444;">
                {r.get('logic', '-')}</td></tr>
            <tr><td style="border:none;color:#764ba2;font-weight:bold;
                           vertical-align:top;padding:4px 8px;">
                &#x2705; Expected</td>
                <td style="border:none;padding:4px 8px;color:#444;">
                {r.get('expected_result', '-')}</td></tr>
            <tr><td style="border:none;color:#764ba2;font-weight:bold;
                           vertical-align:top;padding:4px 8px;">
                &#x26A0; Impact</td>
                <td style="border:none;padding:4px 8px;color:#444;">
                {r.get('failure_implication', '-')}</td></tr>
          </table>
        </div>"""

        html += "</div></div>"
        html += '<div class="spacer-sm"></div>'

    # ---- Trends Section ----
    if trend_results:
        html += f'<h2><span class="section-icon">{SECTION_ICONS["trends"]}</span> 4. Membership Trends</h2>'
        html += '<div class="spacer-sm"></div>'

        # Trend type icons
        trend_type_icons = {
            "trend": "&#x1F4C8;",        # chart increasing
            "grouped_bar": "&#x1F4CA;",   # bar chart
            "stacked_bar": "&#x1F4CA;",   # bar chart
            "bar": "&#x1F4CA;",           # bar chart
            "pie": "&#x1F967;",           # pie
        }

        for idx, tr in enumerate(trend_results):
            chart_type = tr.get("chart", {}).get("type", "trend")
            trend_icon = trend_type_icons.get(chart_type, "&#x1F4C8;")
            # Alternate subtle background gradients for visual variety
            bg_colors = [
                "linear-gradient(135deg, #f0f7ff 0%, #f5f0ff 100%)",
                "linear-gradient(135deg, #f0fff7 0%, #f5f0ff 100%)",
                "linear-gradient(135deg, #fff7f0 0%, #fff0f5 100%)",
                "linear-gradient(135deg, #f5f0ff 0%, #f0f7ff 100%)",
            ]
            bg = bg_colors[idx % len(bg_colors)]
            border_colors = ["#d0e0f0", "#c0e8d0", "#f0d0c0", "#d8c0f0"]
            bc = border_colors[idx % len(border_colors)]

            html += f"""
        <div class="trend-card" style="background:{bg};border-color:{bc};">
            <h3 class="trend-title">
                <span class="trend-icon">{trend_icon}</span>
                {tr['display_name']}</h3>"""

            if tr["status"] == "error":
                html += f"""
            <p style="color:#dc3545;font-size:12px;">
                &#x1F6A8; <b>Query failed:</b> {tr.get("error", "")}</p>"""
            elif tr["data"]:
                chart_b64, chart_mime = generate_trend_chart(tr["chart"], tr["data"], tr["columns"])
                if chart_b64:
                    html += f"""
            <div class="chart-container">
                <img src="data:{chart_mime};base64,{chart_b64}"
                     alt="{tr['chart'].get('title', '')}" /></div>"""
                else:
                    html += html_bar_chart_fallback(tr["chart"], tr["data"], tr["columns"])

                html += '<div class="spacer-sm"></div>'

                # Show data table for trend queries (all rows, collapsible if >10 rows)
                t_n_rows = len(tr["data"])
                t_is_large = t_n_rows > 10

                if t_is_large:
                    html += f"""
                <details style="margin-top:12px;">
                  <summary style="cursor:pointer;padding:10px 14px;background:linear-gradient(135deg,
                            #f8f9ff 0%, #fef9ff 100%);border:1px solid #e8e0f0;border-radius:8px;
                            font-size:12px;font-weight:bold;color:#764ba2;list-style:none;
                            display:flex;align-items:center;gap:8px;">
                    <span>&#x25B6;</span>
                    &#x1F4CB; Trend Data ({t_n_rows} rows) — Click to expand
                  </summary>
                  <div class="wide-table-wrap">"""
                else:
                    html += '<div class="wide-table-wrap" style="margin-top:12px;">'

                html += '<table><thead><tr>'
                for col in tr["columns"]:
                    html += f"<th>{col.replace('_', ' ').title()}</th>"
                html += "</tr></thead><tbody>"
                for row in tr["data"]:
                    html += "<tr>"
                    for col in tr["columns"]:
                        html += f"<td>{row.get(col, '')}</td>"
                    html += "</tr>"
                html += "</tbody></table>"

                if t_is_large:
                    html += "</div></details>"
                else:
                    html += "</div>"
            else:
                html += '<p style="color:#888;font-size:12px;">&#x1F4ED; No data returned.</p>'

            html += "</div>"

        html += '<div class="spacer"></div>'

    # ---- Footer ----
    html += f"""
    <div class="spacer"></div>
    </div><!-- end content -->

    <div class="footer" style="text-align:center;">
        <p>
            <span class="footer-icon">&#x1F4E7;</span>
            Generated by <b>Inforce Membership DQ Dashboard v2.0</b>
            &nbsp;&bull;&nbsp;
            <span class="footer-icon">&#x1F46B;</span>
            BIG DATA &ndash; Membership Team
            &nbsp;&bull;&nbsp;
            <span class="footer-icon">&#x1F4C5;</span>
            {now_str}
        </p>
        <p style="margin-top:8px;color:#aaa;font-size:10px;">
            &#x2728; Powered by PySpark + AWS Glue &nbsp;|&nbsp;
            &#x1F512; Internal Use Only</p>
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
    if overall_status == STATUS_PASS:
        status_emoji = "\u2705 PASSED"
    else:
        status_emoji = "\u274C FAILED"

    subject = (f"{email_cfg.get('subject_prefix', 'Inforce DQ')} \u2013 "
               f"{status_emoji} \u2013 {run_date}")

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
