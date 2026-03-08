"""
DQ Audit Dashboard — Streamlit application for the ETL Orchestrator audit table.

Table : audit_db.etl_orchestrator_audit  (AWS Athena)
Format : pipe-delimited on S3, partitioned by run_date

Run:
    streamlit run pyscript/dq_audit_dashboard.py
"""

import time
from datetime import date, datetime, timedelta

import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    layout="wide",
    page_title="DQ Audit Dashboard",
    page_icon="\U0001f4ca",
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ATHENA_DATABASE = "audit_db"
ATHENA_TABLE = "etl_orchestrator_audit"
ATHENA_OUTPUT = "s3://strands-etl-audit/athena-results/"
ATHENA_WORKGROUP = "primary"

EVENT_TYPES = ["STALE_CHECK", "GLUE_TRIGGER", "VALIDATION", "COMPARISON", "SUMMARY_QUERY"]
STATUS_VALUES = ["PASS", "FAIL", "SKIPPED", "ABORTED", "OK", "Stale", "Check with Source"]

# Quality dimension mapping (derived from validation_name / check_type patterns)
QUALITY_DIMENSION_MAP = {
    "row_count": "Completeness",
    "no_rows": "Validity",
    "threshold": "Accuracy",
    "freshness": "Freshness",
    "volume": "Volume",
    "uniqueness": "Uniqueness",
}

PASS_STATUSES = {"PASS", "OK"}
FAIL_STATUSES = {"FAIL", "Stale", "Check with Source", "ABORTED"}

# ---------------------------------------------------------------------------
# Athena query helpers
# ---------------------------------------------------------------------------

def _get_athena_client():
    return boto3.client("athena")


def _run_athena_query(sql: str) -> pd.DataFrame:
    """Execute an Athena query synchronously and return results as a DataFrame."""
    client = _get_athena_client()
    response = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup=ATHENA_WORKGROUP,
    )
    query_execution_id = response["QueryExecutionId"]

    # Poll until complete
    max_wait = 120
    elapsed = 0
    while elapsed < max_wait:
        status_resp = client.get_query_execution(QueryExecutionId=query_execution_id)
        state = status_resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED",):
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status_resp["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown error"
            )
            raise RuntimeError(f"Athena query {state}: {reason}")
        time.sleep(2)
        elapsed += 2

    if elapsed >= max_wait:
        raise TimeoutError("Athena query timed out after 120 seconds.")

    # Paginate results
    paginator = client.get_paginator("get_query_results")
    rows = []
    columns = None
    for page in paginator.paginate(QueryExecutionId=query_execution_id):
        result_set = page["ResultSet"]
        if columns is None:
            columns = [
                col["VarCharValue"]
                for col in result_set["Rows"][0]["Data"]
            ]
            data_rows = result_set["Rows"][1:]
        else:
            data_rows = result_set["Rows"]
        for row in data_rows:
            rows.append(
                [d.get("VarCharValue", "") for d in row["Data"]]
            )

    if not columns:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=300)
def fetch_audit_data(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch audit records for the given date range from Athena."""
    sql = f"""
    SELECT
        run_id,
        run_date,
        event_timestamp,
        event_type,
        source_label,
        source_bucket,
        source_prefix,
        subfolder_count,
        status,
        detail,
        glue_job_name,
        glue_job_run_id,
        glue_duration_sec,
        validation_name,
        check_type,
        actual_value,
        query,
        overall_status
    FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
    WHERE run_date >= '{start_date}'
      AND run_date <= '{end_date}'
    ORDER BY run_date DESC, event_timestamp DESC
    """
    return _run_athena_query(sql)


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _classify_dimension(row: pd.Series) -> str:
    """Map a row to a quality dimension based on check_type and validation_name."""
    ct = str(row.get("check_type", "")).lower().strip()
    vn = str(row.get("validation_name", "")).lower().strip()

    if ct in QUALITY_DIMENSION_MAP:
        return QUALITY_DIMENSION_MAP[ct]

    for keyword, dim in QUALITY_DIMENSION_MAP.items():
        if keyword in vn:
            return dim

    if ct or vn:
        return "Validity"
    return "Other"


def _is_pass(status: str) -> bool:
    return status in PASS_STATUSES


def _is_fail(status: str) -> bool:
    return status in FAIL_STATUSES


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.title("Filters")

today = date.today()
default_start = today - timedelta(days=30)

col_s, col_e = st.sidebar.columns(2)
with col_s:
    start_date = st.date_input("Start Date", value=default_start)
with col_e:
    end_date = st.date_input("End Date", value=today)

if start_date > end_date:
    st.sidebar.error("Start date must be before end date.")
    st.stop()

selected_event_types = st.sidebar.multiselect(
    "Event Type", options=EVENT_TYPES, default=EVENT_TYPES
)
selected_statuses = st.sidebar.multiselect(
    "Status", options=STATUS_VALUES, default=STATUS_VALUES
)
run_id_search = st.sidebar.text_input("Run ID Search", placeholder="Enter run_id...")

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

st.title("DQ Audit Dashboard")

try:
    raw_df = fetch_audit_data(str(start_date), str(end_date))
except Exception as exc:
    st.error(f"Failed to query Athena: {exc}")
    st.info("Ensure AWS credentials are configured and the Athena table exists.")
    st.stop()

if raw_df.empty:
    st.warning("No audit records found for the selected date range.")
    st.stop()

# Apply sidebar filters
df = raw_df.copy()
if selected_event_types:
    df = df[df["event_type"].isin(selected_event_types)]
if selected_statuses:
    df = df[df["status"].isin(selected_statuses)]
if run_id_search:
    df = df[df["run_id"].str.contains(run_id_search, case=False, na=False)]

if df.empty:
    st.warning("No records match the current filters.")
    st.stop()

# Cast numeric columns
for col in ("subfolder_count", "glue_duration_sec", "actual_value"):
    df[col] = pd.to_numeric(df[col], errors="coerce")

df["is_pass"] = df["status"].apply(_is_pass)
df["is_fail"] = df["status"].apply(_is_fail)
df["dimension"] = df.apply(_classify_dimension, axis=1)

# ---------------------------------------------------------------------------
# KPI Scorecard row (always visible)
# ---------------------------------------------------------------------------

total_checks = len(df)
passes = int(df["is_pass"].sum())
failures = int(df["is_fail"].sum())
warnings = int(df["status"].isin(["SKIPPED", "Check with Source"]).sum())
fatal = int(df["status"].isin(["ABORTED"]).sum())
pass_rate = round(passes / total_checks * 100, 1) if total_checks else 0.0

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
kpi1.metric("Total Checks", f"{total_checks:,}")
kpi2.metric("Pass Rate", f"{pass_rate}%")
kpi3.metric("Warnings", f"{warnings:,}")
kpi4.metric("Failures", f"{failures:,}")
kpi5.metric("Fatal", f"{fatal:,}")

st.markdown("---")

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab1, tab2, tab3, tab4 = st.tabs([
    "KPI Scorecard",
    "30-Day Summary",
    "Run History",
    "Trend Analysis",
])

# ========================== TAB 1: KPI Scorecard ==========================
with tab1:
    st.subheader("7-Day KPI View")

    seven_days_ago = str(today - timedelta(days=7))
    df7 = df[df["run_date"] >= seven_days_ago].copy()

    if df7.empty:
        st.info("No data in the last 7 days.")
    else:
        # Daily pass/fail trend
        daily = (
            df7.groupby("run_date")
            .agg(
                passes=("is_pass", "sum"),
                failures=("is_fail", "sum"),
                total=("status", "count"),
            )
            .reset_index()
            .sort_values("run_date")
        )
        daily["pass_rate"] = round(daily["passes"] / daily["total"] * 100, 1)

        fig_daily = go.Figure()
        fig_daily.add_trace(
            go.Bar(x=daily["run_date"], y=daily["passes"], name="Pass", marker_color="#2ecc71")
        )
        fig_daily.add_trace(
            go.Bar(x=daily["run_date"], y=daily["failures"], name="Fail", marker_color="#e74c3c")
        )
        fig_daily.update_layout(
            title="Daily Pass / Fail Counts (7 Days)",
            barmode="stack",
            xaxis_title="Date",
            yaxis_title="Count",
            height=380,
        )
        st.plotly_chart(fig_daily, use_container_width=True)

        # Pass rate by source_label (layer/table proxy)
        st.subheader("Pass Rate by Layer / Table")
        by_label = (
            df7[df7["source_label"].str.strip() != ""]
            .groupby("source_label")
            .agg(passes=("is_pass", "sum"), total=("status", "count"))
            .reset_index()
        )
        if not by_label.empty:
            by_label["pass_rate"] = round(by_label["passes"] / by_label["total"] * 100, 1)
            by_label = by_label.sort_values("pass_rate")
            fig_label = px.bar(
                by_label,
                x="pass_rate",
                y="source_label",
                orientation="h",
                text="pass_rate",
                color="pass_rate",
                color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
                range_color=[0, 100],
                labels={"pass_rate": "Pass Rate (%)", "source_label": "Source"},
            )
            fig_label.update_layout(height=max(300, len(by_label) * 35), showlegend=False)
            st.plotly_chart(fig_label, use_container_width=True)
        else:
            # Fall back to validation_name
            by_vn = (
                df7[df7["validation_name"].str.strip() != ""]
                .groupby("validation_name")
                .agg(passes=("is_pass", "sum"), total=("status", "count"))
                .reset_index()
            )
            if not by_vn.empty:
                by_vn["pass_rate"] = round(by_vn["passes"] / by_vn["total"] * 100, 1)
                by_vn = by_vn.sort_values("pass_rate")
                fig_vn = px.bar(
                    by_vn,
                    x="pass_rate",
                    y="validation_name",
                    orientation="h",
                    text="pass_rate",
                    color="pass_rate",
                    color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
                    range_color=[0, 100],
                    labels={"pass_rate": "Pass Rate (%)", "validation_name": "Validation"},
                )
                fig_vn.update_layout(height=max(300, len(by_vn) * 35), showlegend=False)
                st.plotly_chart(fig_vn, use_container_width=True)
            else:
                st.info("No labelled checks to display pass rate breakdown.")

# ========================== TAB 2: 30-Day Summary ==========================
with tab2:
    st.subheader("30-Day Aggregated Summary")

    # Monthly KPI trend (weekly buckets for granularity within 30 days)
    df_monthly = df.copy()
    df_monthly["run_date_dt"] = pd.to_datetime(df_monthly["run_date"], errors="coerce")
    df_monthly["week"] = df_monthly["run_date_dt"].dt.isocalendar().week.astype(str)
    df_monthly["year_week"] = (
        df_monthly["run_date_dt"].dt.isocalendar().year.astype(str)
        + "-W"
        + df_monthly["week"].str.zfill(2)
    )

    weekly_agg = (
        df_monthly.groupby("year_week")
        .agg(passes=("is_pass", "sum"), failures=("is_fail", "sum"), total=("status", "count"))
        .reset_index()
        .sort_values("year_week")
    )
    weekly_agg["pass_rate"] = round(weekly_agg["passes"] / weekly_agg["total"] * 100, 1)

    fig_weekly = go.Figure()
    fig_weekly.add_trace(
        go.Bar(x=weekly_agg["year_week"], y=weekly_agg["passes"], name="Pass", marker_color="#2ecc71")
    )
    fig_weekly.add_trace(
        go.Bar(x=weekly_agg["year_week"], y=weekly_agg["failures"], name="Fail", marker_color="#e74c3c")
    )
    fig_weekly.add_trace(
        go.Scatter(
            x=weekly_agg["year_week"],
            y=weekly_agg["pass_rate"],
            name="Pass Rate %",
            yaxis="y2",
            mode="lines+markers",
            line=dict(color="#3498db", width=2),
        )
    )
    fig_weekly.update_layout(
        title="Weekly KPI Trend (30 Days)",
        barmode="stack",
        xaxis_title="Week",
        yaxis_title="Count",
        yaxis2=dict(title="Pass Rate %", overlaying="y", side="right", range=[0, 105]),
        height=400,
    )
    st.plotly_chart(fig_weekly, use_container_width=True)

    # Quality dimensions breakdown
    st.subheader("Quality Dimensions Breakdown")
    dim_df = (
        df[df["dimension"] != "Other"]
        .groupby("dimension")
        .agg(passes=("is_pass", "sum"), failures=("is_fail", "sum"), total=("status", "count"))
        .reset_index()
    )
    if not dim_df.empty:
        dim_df["pass_rate"] = round(dim_df["passes"] / dim_df["total"] * 100, 1)

        col_chart, col_table = st.columns([2, 1])
        with col_chart:
            fig_dim = px.bar(
                dim_df,
                x="dimension",
                y=["passes", "failures"],
                barmode="stack",
                color_discrete_map={"passes": "#2ecc71", "failures": "#e74c3c"},
                labels={"value": "Count", "dimension": "Quality Dimension"},
                title="Checks by Quality Dimension",
            )
            fig_dim.update_layout(height=380)
            st.plotly_chart(fig_dim, use_container_width=True)
        with col_table:
            st.dataframe(
                dim_df[["dimension", "total", "passes", "failures", "pass_rate"]]
                .rename(columns={
                    "dimension": "Dimension",
                    "total": "Total",
                    "passes": "Pass",
                    "failures": "Fail",
                    "pass_rate": "Pass Rate %",
                }),
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.info("No quality dimension data available. Dimensions are derived from check_type.")

    # Top failing checks
    st.subheader("Top Failing Checks")
    fail_df = df[df["is_fail"]].copy()
    if not fail_df.empty:
        # Group by a meaningful identifier
        fail_df["check_id"] = fail_df.apply(
            lambda r: r["validation_name"] if r["validation_name"].strip() else (
                r["source_label"] if r["source_label"].strip() else r["event_type"]
            ),
            axis=1,
        )
        top_fails = (
            fail_df.groupby("check_id")
            .agg(failure_count=("status", "count"), last_failure=("run_date", "max"))
            .reset_index()
            .sort_values("failure_count", ascending=False)
            .head(15)
        )
        top_fails.columns = ["Check", "Failure Count", "Last Failure Date"]
        st.dataframe(top_fails, use_container_width=True, hide_index=True)
    else:
        st.success("No failures in the selected period!")

# ========================== TAB 3: Run History ==========================
with tab3:
    st.subheader("Audit Record History")

    display_cols = [
        "run_date", "run_id", "event_type", "status", "source_label",
        "validation_name", "check_type", "actual_value", "detail",
        "glue_job_name", "glue_duration_sec", "overall_status",
    ]
    available_cols = [c for c in display_cols if c in df.columns]
    history_df = df[available_cols].copy()

    # Run-level filter within the tab
    run_ids = sorted(df["run_id"].dropna().unique(), reverse=True)
    selected_run = st.selectbox("Filter by Run ID", options=["All"] + list(run_ids))
    if selected_run != "All":
        history_df = history_df[history_df["run_id"] == selected_run]

    st.dataframe(
        history_df,
        use_container_width=True,
        hide_index=True,
        height=600,
    )

    # Expandable detail for each run_id
    st.subheader("Run Details (expand to view)")
    unique_runs = history_df["run_id"].unique()
    for rid in unique_runs[:25]:  # limit to first 25 to avoid overload
        run_subset = history_df[history_df["run_id"] == rid]
        overall = run_subset["overall_status"].iloc[0] if not run_subset.empty else "N/A"
        run_date_val = run_subset["run_date"].iloc[0] if not run_subset.empty else ""
        status_icon = "PASS" if overall in PASS_STATUSES else "FAIL"
        with st.expander(f"[{status_icon}] {rid}  ({run_date_val})"):
            st.dataframe(run_subset, use_container_width=True, hide_index=True)

# ========================== TAB 4: Trend Analysis ==========================
with tab4:
    st.subheader("Trend Analysis")

    # Pass rate trend over time
    trend_df = (
        df.groupby("run_date")
        .agg(passes=("is_pass", "sum"), total=("status", "count"))
        .reset_index()
        .sort_values("run_date")
    )
    trend_df["pass_rate"] = round(trend_df["passes"] / trend_df["total"] * 100, 1)

    fig_trend = px.line(
        trend_df,
        x="run_date",
        y="pass_rate",
        markers=True,
        title="Daily Pass Rate Trend",
        labels={"pass_rate": "Pass Rate (%)", "run_date": "Date"},
    )
    fig_trend.add_hline(y=95, line_dash="dash", line_color="orange", annotation_text="SLA Target (95%)")
    fig_trend.update_layout(height=400, yaxis_range=[0, 105])
    st.plotly_chart(fig_trend, use_container_width=True)

    # SLA compliance trend
    st.subheader("SLA Compliance Trend")
    sla_threshold = st.slider("SLA Threshold (%)", 50, 100, 95, step=1)
    trend_df["sla_met"] = trend_df["pass_rate"] >= sla_threshold

    fig_sla = go.Figure()
    colors = ["#2ecc71" if met else "#e74c3c" for met in trend_df["sla_met"]]
    fig_sla.add_trace(
        go.Bar(
            x=trend_df["run_date"],
            y=trend_df["pass_rate"],
            marker_color=colors,
            text=trend_df["pass_rate"],
            textposition="auto",
        )
    )
    fig_sla.add_hline(y=sla_threshold, line_dash="dash", line_color="orange",
                      annotation_text=f"SLA Target ({sla_threshold}%)")
    fig_sla.update_layout(
        title=f"SLA Compliance (Target: {sla_threshold}%)",
        xaxis_title="Date",
        yaxis_title="Pass Rate (%)",
        yaxis_range=[0, 105],
        height=400,
    )
    st.plotly_chart(fig_sla, use_container_width=True)

    # Cost trend (Glue duration as proxy)
    st.subheader("Glue Job Duration Trend")
    glue_df = df[df["event_type"] == "GLUE_TRIGGER"].copy()
    if not glue_df.empty and glue_df["glue_duration_sec"].notna().any():
        cost_trend = (
            glue_df.groupby("run_date")
            .agg(total_duration_sec=("glue_duration_sec", "sum"), job_count=("glue_job_name", "count"))
            .reset_index()
            .sort_values("run_date")
        )
        cost_trend["total_duration_min"] = round(cost_trend["total_duration_sec"] / 60, 1)

        fig_cost = px.bar(
            cost_trend,
            x="run_date",
            y="total_duration_min",
            text="total_duration_min",
            title="Daily Glue Job Duration (minutes)",
            labels={"total_duration_min": "Duration (min)", "run_date": "Date"},
            color="total_duration_min",
            color_continuous_scale="Blues",
        )
        fig_cost.update_layout(height=380)
        st.plotly_chart(fig_cost, use_container_width=True)
    else:
        st.info("No Glue job duration data available for cost/duration trend.")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
st.caption(
    f"Data source: `{ATHENA_DATABASE}.{ATHENA_TABLE}` | "
    f"Date range: {start_date} to {end_date} | "
    f"Records: {len(df):,} | "
    f"Last refreshed: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
)
