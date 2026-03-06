###################################################################################################
# SCRIPT        : glue_job_status_report.py
# PURPOSE       : Generate EMAIL with Glue job status report including SLA tracking,
#                 cost analysis, projections, charts, and job health scoring
# INPUT         : List of Glue job configs (name, domain, SLA, worker type)
# OUTPUT        : Rich HTML email with dashboards, charts, and cost projections
#
#  PROCESS    :
#    1) Get list of Glue jobs from JOB_CONFIGS
#    2) For each job, get latest + historical run details
#    3) Calculate SLA compliance (ON TIME / BREACHED), cost in $, times in minutes
#    4) Generate 7-day SLA compliance & cost trend charts (matplotlib / HTML fallback)
#    5) Build cost projection table (weekly, monthly, yearly)
#    6) Calculate job health scores, identify failure streaks
#    7) Generate rich HTML dashboard and send email via SMTP
#
#==================================================================================================#
# DATE          BY              MODIFICATION LOG
# ----------    -----------     ------------------------------------------
# 11/20/2025    AI Assistant    Initial version
# 03/2026       Dhilipan        SLA tracking, cost analysis, projections, charts, health scoring
#
#==================================================================================================#
###################################################################################################

import boto3
import pytz
import smtplib
import base64
import io
from collections import defaultdict
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Optional: matplotlib for chart generation
# ---------------------------------------------------------------------------
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GLUE_COST_PER_DPU_HOUR = 0.44  # AWS Glue standard pricing (USD)

VIBRANT_COLORS = [
    "#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4", "#FFEAA7",
    "#DDA0DD", "#FF8C42", "#98D8C8", "#F7DC6F", "#BB8FCE",
    "#85C1E9", "#F1948A", "#82E0AA", "#F8C471", "#AED6F1",
]

PST = pytz.timezone("US/Pacific")

# Number of historical runs to fetch per job (covers ~7 days for most jobs)
MAX_HISTORY_RUNS = 50

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Each job entry: name, domain, sla_minutes (max allowed execution time)
JOB_CONFIGS = [
    {"name": "job1", "domain": "Customer Base",       "sla_minutes": 30},
    {"name": "job2", "domain": "Customer Master",     "sla_minutes": 45},
    {"name": "job3", "domain": "Customer Master",     "sla_minutes": 45},
    {"name": "job4", "domain": "Membership Base",     "sla_minutes": 60},
    {"name": "job5", "domain": "Membership Master",   "sla_minutes": 45},
    {"name": "job6", "domain": "Life Master",         "sla_minutes": 30},
    {"name": "job7", "domain": "Life Master",         "sla_minutes": 30},
    {"name": "job8", "domain": "Membership Master",   "sla_minutes": 45},
]

SENDER = "Job_Status@company.com"
RECIPIENTS = ["list@company.com"]
SMTP_HOST = "x.x.x.x"
SMTP_PORT = 25

# ---------------------------------------------------------------------------
# AWS Client
# ---------------------------------------------------------------------------
glue_client = boto3.client("glue", region_name="us-west-2")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
appName = "GLUE_JOB_STATUS_REPORT"
print(f"{appName} -- STARTED")
script_start = datetime.now()


def seconds_to_minutes(seconds):
    """Convert seconds to a formatted minutes string."""
    if seconds is None or seconds == "N/A":
        return "N/A"
    try:
        sec = float(seconds)
        minutes = sec / 60.0
        if minutes < 1:
            return f"{minutes:.2f} min"
        return f"{minutes:.1f} min"
    except (ValueError, TypeError):
        return "N/A"


def calc_cost(dpu_seconds):
    """Calculate cost in USD from DPU-seconds."""
    if dpu_seconds is None or dpu_seconds == "N/A":
        return 0.0
    try:
        return (float(dpu_seconds) / 3600.0) * GLUE_COST_PER_DPU_HOUR
    except (ValueError, TypeError):
        return 0.0


def _status_color(status):
    """Return color hex for a status string."""
    return {
        "SUCCEEDED": "#28a745", "FAILED": "#dc3545", "RUNNING": "#007bff",
        "STOPPED": "#6c757d", "TIMEOUT": "#dc3545", "ON TIME": "#28a745",
        "BREACHED": "#dc3545", "NO_RUNS_FOUND": "#6c757d", "ERROR": "#dc3545",
    }.get(status, "#333333")


# ---------------------------------------------------------------------------
# Data Collection
# ---------------------------------------------------------------------------
def get_job_details(job_cfg):
    """
    Get latest run status, historical metrics, SLA compliance, and cost
    for a single Glue job.
    """
    job_name = job_cfg["name"]
    domain = job_cfg["domain"]
    sla_minutes = job_cfg.get("sla_minutes", 60)

    empty = {
        "job_name": job_name, "domain": domain,
        "status": "NO_RUNS_FOUND", "worker_type": "N/A", "num_workers": "N/A",
        "dpu_seconds": 0, "execution_time_sec": 0,
        "execution_time_min": "N/A", "last_run_time": "Never",
        "last_run_pst": "Never", "error_message": "No job runs found",
        "is_recent": False, "is_today": False,
        "sla_minutes": sla_minutes, "sla_status": "N/A",
        "runs_last_24h": 0, "avg_exec_min": "N/A", "success_rate_24h": "N/A",
        "cost_today": 0.0, "cost_7_days": 0.0,
        "history": [],  # list of per-run dicts for 7-day analysis
    }

    try:
        response = glue_client.get_job_runs(
            JobName=job_name, MaxResults=MAX_HISTORY_RUNS
        )
    except Exception as exc:
        empty["status"] = "ERROR"
        empty["error_message"] = str(exc)
        return empty

    if not response.get("JobRuns"):
        return empty

    job_runs = response["JobRuns"]
    latest = job_runs[0]

    # --- Latest run details ---
    status = latest.get("JobRunState", "UNKNOWN")
    dpu_seconds = latest.get("DPUSeconds") or 0
    exec_time_sec = latest.get("ExecutionTime") or 0
    exec_time_min = seconds_to_minutes(exec_time_sec)
    worker_type = latest.get("WorkerType", "Standard")
    num_workers = latest.get("NumberOfWorkers", latest.get("AllocatedCapacity", "N/A"))
    error_message = latest.get("ErrorMessage", "")

    started_on = latest.get("StartedOn")
    now_utc = datetime.now(pytz.UTC)
    is_recent = False
    is_today = False
    last_run_display = "Never"
    last_run_pst = "Never"

    if started_on:
        if started_on.tzinfo is None:
            started_on = pytz.UTC.localize(started_on)
        last_run_display = started_on.strftime("%Y-%m-%d %H:%M:%S UTC")
        last_run_pst = started_on.astimezone(PST).strftime("%Y-%m-%d %H:%M:%S PST")
        time_diff = (now_utc - started_on).total_seconds()
        is_recent = time_diff < 86400
        is_today = started_on.astimezone(PST).date() == now_utc.astimezone(PST).date()

    # --- SLA check (latest run) ---
    sla_status = "N/A"
    if exec_time_sec and exec_time_sec > 0:
        sla_status = "BREACHED" if (exec_time_sec / 60.0) > sla_minutes else "ON TIME"

    # --- Historical metrics (all runs within last 7 days) ---
    runs_24h = 0
    successful_24h = 0
    exec_times_24h = []
    cost_today = 0.0
    cost_7_days = 0.0
    history = []

    for run in job_runs:
        run_start = run.get("StartedOn")
        if not run_start:
            continue
        if run_start.tzinfo is None:
            run_start = pytz.UTC.localize(run_start)

        age_sec = (now_utc - run_start).total_seconds()
        run_dpu_sec = run.get("DPUSeconds") or 0
        run_exec_sec = run.get("ExecutionTime") or 0
        run_status = run.get("JobRunState", "UNKNOWN")
        run_cost = calc_cost(run_dpu_sec)
        run_sla = "BREACHED" if (run_exec_sec / 60.0) > sla_minutes else "ON TIME"

        run_date_pst = run_start.astimezone(PST).strftime("%Y-%m-%d")

        if age_sec < 604800:  # 7 days
            cost_7_days += run_cost
            history.append({
                "date": run_date_pst,
                "status": run_status,
                "exec_sec": run_exec_sec,
                "exec_min": round(run_exec_sec / 60.0, 1),
                "dpu_seconds": run_dpu_sec,
                "cost": run_cost,
                "sla_status": run_sla,
                "worker_type": run.get("WorkerType", "Standard"),
                "num_workers": run.get("NumberOfWorkers",
                                       run.get("AllocatedCapacity", "N/A")),
            })

        if age_sec < 86400:  # 24h
            runs_24h += 1
            cost_today += run_cost
            if run_exec_sec:
                exec_times_24h.append(run_exec_sec)
            if run_status == "SUCCEEDED":
                successful_24h += 1

    avg_exec_min = "N/A"
    if exec_times_24h:
        avg_sec = sum(exec_times_24h) / len(exec_times_24h)
        avg_exec_min = seconds_to_minutes(avg_sec)

    success_rate = "N/A"
    if runs_24h > 0:
        success_rate = f"{(successful_24h / runs_24h) * 100:.0f}%"

    return {
        "job_name": job_name, "domain": domain,
        "status": status, "worker_type": worker_type, "num_workers": num_workers,
        "dpu_seconds": dpu_seconds, "execution_time_sec": exec_time_sec,
        "execution_time_min": exec_time_min,
        "last_run_time": last_run_display, "last_run_pst": last_run_pst,
        "error_message": error_message,
        "is_recent": is_recent, "is_today": is_today,
        "sla_minutes": sla_minutes, "sla_status": sla_status,
        "runs_last_24h": runs_24h, "avg_exec_min": avg_exec_min,
        "success_rate_24h": success_rate,
        "cost_today": round(cost_today, 2), "cost_7_days": round(cost_7_days, 2),
        "history": history,
    }


# ---------------------------------------------------------------------------
# 7-Day Aggregation
# ---------------------------------------------------------------------------
def aggregate_7_day_stats(all_jobs):
    """
    Aggregate daily SLA compliance, cost, and run counts across all jobs
    for the past 7 days.
    Returns dict keyed by date string: {date: {on_time, breached, cost, runs, ...}}
    """
    now_pst = datetime.now(pytz.UTC).astimezone(PST)
    days = []
    for i in range(6, -1, -1):
        d = (now_pst - timedelta(days=i)).strftime("%Y-%m-%d")
        days.append(d)

    daily = {d: {"on_time": 0, "breached": 0, "cost": 0.0,
                 "runs": 0, "succeeded": 0, "failed": 0}
             for d in days}

    for job in all_jobs:
        for h in job.get("history", []):
            d = h["date"]
            if d not in daily:
                continue
            daily[d]["runs"] += 1
            daily[d]["cost"] += h["cost"]
            if h["sla_status"] == "ON TIME":
                daily[d]["on_time"] += 1
            elif h["sla_status"] == "BREACHED":
                daily[d]["breached"] += 1
            if h["status"] == "SUCCEEDED":
                daily[d]["succeeded"] += 1
            elif h["status"] == "FAILED":
                daily[d]["failed"] += 1

    return days, daily


# ---------------------------------------------------------------------------
# Cost Projections
# ---------------------------------------------------------------------------
def build_cost_projections(all_jobs):
    """
    Build cost projection table based on 7-day actual data.
    Returns list of dicts: [{domain, worker_type, cost_7d, weekly, monthly, yearly}]
    """
    domain_costs = defaultdict(lambda: {
        "cost_7d": 0.0, "worker_types": set(), "num_workers": set(),
        "total_exec_min": 0.0, "run_count": 0,
    })

    for job in all_jobs:
        key = job["domain"]
        domain_costs[key]["cost_7d"] += job["cost_7_days"]
        for h in job.get("history", []):
            domain_costs[key]["worker_types"].add(h.get("worker_type", "N/A"))
            domain_costs[key]["num_workers"].add(str(h.get("num_workers", "N/A")))
            domain_costs[key]["total_exec_min"] += h.get("exec_min", 0)
            domain_costs[key]["run_count"] += 1

    projections = []
    for domain in sorted(domain_costs.keys()):
        d = domain_costs[domain]
        cost_7d = d["cost_7d"]
        weekly = cost_7d  # already 7 days
        monthly = cost_7d * (30.0 / 7.0)
        yearly = cost_7d * (365.0 / 7.0)
        projections.append({
            "domain": domain,
            "worker_types": ", ".join(sorted(d["worker_types"])),
            "total_exec_min": round(d["total_exec_min"], 1),
            "run_count": d["run_count"],
            "cost_7d": round(cost_7d, 2),
            "weekly": round(weekly, 2),
            "monthly": round(monthly, 2),
            "yearly": round(yearly, 2),
        })

    # Grand total row
    total_7d = sum(p["cost_7d"] for p in projections)
    projections.append({
        "domain": "TOTAL",
        "worker_types": "",
        "total_exec_min": round(sum(p["total_exec_min"] for p in projections), 1),
        "run_count": sum(p["run_count"] for p in projections),
        "cost_7d": round(total_7d, 2),
        "weekly": round(total_7d, 2),
        "monthly": round(total_7d * (30.0 / 7.0), 2),
        "yearly": round(total_7d * (365.0 / 7.0), 2),
    })

    return projections


# ---------------------------------------------------------------------------
# Job Health Score
# ---------------------------------------------------------------------------
def calc_health_score(job):
    """
    Calculate a 0-100 health score for a job based on:
      - Success rate (40 pts)
      - SLA compliance (30 pts)
      - Recency (15 pts)
      - No errors (15 pts)
    """
    score = 0
    history = job.get("history", [])

    # Success rate (40 pts)
    if history:
        succeeded = sum(1 for h in history if h["status"] == "SUCCEEDED")
        score += int((succeeded / len(history)) * 40)

    # SLA compliance (30 pts)
    if history:
        on_time = sum(1 for h in history if h["sla_status"] == "ON TIME")
        score += int((on_time / len(history)) * 30)

    # Recency — ran today (15 pts)
    if job.get("is_today"):
        score += 15
    elif job.get("is_recent"):
        score += 8

    # No errors on latest run (15 pts)
    if job.get("status") == "SUCCEEDED" and not job.get("error_message"):
        score += 15
    elif job.get("status") == "RUNNING":
        score += 10

    return min(score, 100)


def health_color(score):
    """Return color for health score."""
    if score >= 80:
        return "#28a745"
    if score >= 60:
        return "#fd7e14"
    return "#dc3545"


def health_label(score):
    """Return label for health score."""
    if score >= 80:
        return "Healthy"
    if score >= 60:
        return "Warning"
    return "Critical"


# ---------------------------------------------------------------------------
# Chart Generation
# ---------------------------------------------------------------------------
def generate_sla_pie_chart(days, daily):
    """Generate a pie chart of overall SLA compliance for the past 7 days."""
    total_on_time = sum(daily[d]["on_time"] for d in days)
    total_breached = sum(daily[d]["breached"] for d in days)

    if total_on_time + total_breached == 0:
        return None

    if not HAS_MATPLOTLIB:
        return None

    fig, ax = plt.subplots(figsize=(5, 4))
    fig.patch.set_facecolor("#FAFBFC")

    labels = ["On Time", "Breached"]
    sizes = [total_on_time, total_breached]
    colors = ["#28a745", "#dc3545"]
    explode = (0, 0.05)

    wedges, texts, autotexts = ax.pie(
        sizes, labels=labels, colors=colors, explode=explode,
        autopct="%1.0f%%", startangle=90, pctdistance=0.75,
        wedgeprops={"edgecolor": "white", "linewidth": 2},
    )
    for t in autotexts:
        t.set_fontsize(12)
        t.set_fontweight("bold")
        t.set_color("white")
    for t in texts:
        t.set_fontsize(11)
        t.set_color("#2c3e50")

    ax.set_title("SLA Compliance (Past 7 Days)", fontsize=13,
                 fontweight="bold", color="#2c3e50", pad=15)

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def generate_cost_trend_chart(days, daily):
    """Generate a bar chart of daily cost for the past 7 days."""
    costs = [round(daily[d]["cost"], 2) for d in days]
    if sum(costs) == 0:
        return None
    if not HAS_MATPLOTLIB:
        return None

    fig, ax = plt.subplots(figsize=(8, 4))
    fig.patch.set_facecolor("#FAFBFC")
    ax.set_facecolor("#FAFBFC")

    short_days = [d[5:] for d in days]  # MM-DD
    bar_colors = ["#45B7D1"] * len(days)

    bars = ax.bar(short_days, costs, color=bar_colors, edgecolor="white",
                  linewidth=0.8, width=0.6)
    ax.set_ylabel("Cost (USD)", fontsize=11, fontweight="bold", color="#2c3e50")
    ax.set_title("Daily Glue Cost (Past 7 Days)", fontsize=13,
                 fontweight="bold", color="#2c3e50", pad=15)
    ax.grid(axis="y", color="#E8ECF0", linestyle="--", linewidth=0.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    for bar, val in zip(bars, costs):
        if val > 0:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(costs) * 0.02,
                    f"${val:.2f}", ha="center", fontsize=9, fontweight="bold", color="#2c3e50")

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def generate_sla_trend_chart(days, daily):
    """Generate a stacked bar chart: on-time vs breached per day."""
    on_times = [daily[d]["on_time"] for d in days]
    breached = [daily[d]["breached"] for d in days]
    if sum(on_times) + sum(breached) == 0:
        return None
    if not HAS_MATPLOTLIB:
        return None

    fig, ax = plt.subplots(figsize=(8, 4))
    fig.patch.set_facecolor("#FAFBFC")
    ax.set_facecolor("#FAFBFC")

    short_days = [d[5:] for d in days]
    x = range(len(days))

    ax.bar(x, on_times, color="#28a745", edgecolor="white", label="On Time", width=0.6)
    ax.bar(x, breached, bottom=on_times, color="#dc3545", edgecolor="white",
           label="Breached", width=0.6)

    ax.set_xticks(x)
    ax.set_xticklabels(short_days)
    ax.set_ylabel("Job Runs", fontsize=11, fontweight="bold", color="#2c3e50")
    ax.set_title("Daily SLA Status (Past 7 Days)", fontsize=13,
                 fontweight="bold", color="#2c3e50", pad=15)
    ax.legend(fontsize=10)
    ax.grid(axis="y", color="#E8ECF0", linestyle="--", linewidth=0.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


# ---------------------------------------------------------------------------
# HTML Fallback Charts (when matplotlib unavailable)
# ---------------------------------------------------------------------------
def html_sla_pie_fallback(days, daily):
    """Pure HTML/CSS pie representation for SLA compliance."""
    total_on_time = sum(daily[d]["on_time"] for d in days)
    total_breached = sum(daily[d]["breached"] for d in days)
    total = total_on_time + total_breached
    if total == 0:
        return "<p>No SLA data available.</p>"

    on_pct = (total_on_time / total) * 100
    br_pct = (total_breached / total) * 100

    return f"""
    <div style="margin:15px 0;">
      <h3 style="color:#2c3e50;">SLA Compliance (Past 7 Days)</h3>
      <div style="display:flex;align-items:center;gap:20px;">
        <div style="width:200px;height:20px;border-radius:10px;overflow:hidden;
                    display:flex;border:1px solid #ddd;">
          <div style="width:{on_pct:.0f}%;background:#28a745;"></div>
          <div style="width:{br_pct:.0f}%;background:#dc3545;"></div>
        </div>
        <span style="font-size:13px;">
          <b style="color:#28a745;">On Time: {total_on_time} ({on_pct:.0f}%)</b> &nbsp;|&nbsp;
          <b style="color:#dc3545;">Breached: {total_breached} ({br_pct:.0f}%)</b>
        </span>
      </div>
    </div>"""


def html_cost_bar_fallback(days, daily):
    """Pure HTML/CSS bar chart for daily cost."""
    costs = [daily[d]["cost"] for d in days]
    max_cost = max(costs) if costs else 1

    html = '<h3 style="color:#2c3e50;">Daily Glue Cost (Past 7 Days)</h3>'
    html += '<table style="width:100%;border-collapse:collapse;font-size:13px;">'
    for d, cost in zip(days, costs):
        pct = (cost / max_cost * 100) if max_cost > 0 else 0
        html += f"""<tr>
            <td style="width:80px;padding:5px 8px;font-weight:bold;color:#2c3e50;">{d[5:]}</td>
            <td style="padding:5px 4px;">
                <div style="background:#45B7D1;width:{pct:.0f}%;height:22px;border-radius:4px;
                            display:inline-block;min-width:2px;"></div>
                <span style="margin-left:8px;font-weight:bold;color:#2c3e50;">${cost:.2f}</span>
            </td></tr>"""
    html += "</table>"
    return html


# ---------------------------------------------------------------------------
# HTML Report Generation
# ---------------------------------------------------------------------------
def generate_html(job_statuses, days, daily, projections):
    """Generate the full HTML dashboard email."""

    now_pst = datetime.now(pytz.UTC).astimezone(PST).strftime("%Y-%m-%d %H:%M:%S PST")

    # --- Summary counts ---
    total_jobs = len(job_statuses)
    succeeded = sum(1 for j in job_statuses if j["status"] == "SUCCEEDED")
    failed = sum(1 for j in job_statuses if j["status"] == "FAILED")
    running = sum(1 for j in job_statuses if j["status"] == "RUNNING")
    sla_on_time = sum(1 for j in job_statuses if j["sla_status"] == "ON TIME")
    sla_breached = sum(1 for j in job_statuses if j["sla_status"] == "BREACHED")
    total_cost_today = sum(j["cost_today"] for j in job_statuses)
    total_cost_7d = sum(j["cost_7_days"] for j in job_statuses)
    recent_count = sum(1 for j in job_statuses if j["is_recent"])

    # --- Health scores ---
    for j in job_statuses:
        j["health_score"] = calc_health_score(j)

    html = f"""
    <html><head><style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; color: #333;
               background-color: #f5f6fa; }}
        .container {{ max-width: 1100px; margin: 0 auto; background: white;
                      border-radius: 12px; padding: 30px;
                      box-shadow: 0 2px 12px rgba(0,0,0,0.08); }}
        h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 12px;
              font-size: 22px; }}
        h2 {{ color: #2c3e50; margin-top: 30px; font-size: 17px;
              border-left: 4px solid #3498db; padding-left: 12px; }}
        .summary-box {{ display: inline-block; padding: 10px 18px; margin: 4px 5px;
                        border-radius: 8px; color: white; font-size: 13px;
                        font-weight: bold; text-align: center; min-width: 100px; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 10px; font-size: 12px; }}
        th {{ background: linear-gradient(135deg, #2c3e50, #34495e); color: white;
              padding: 9px 10px; text-align: left; white-space: nowrap; }}
        td {{ padding: 7px 10px; border-bottom: 1px solid #eee; }}
        tr:nth-child(even) {{ background-color: #f8f9fa; }}
        .badge {{ display: inline-block; padding: 3px 8px; border-radius: 4px;
                  color: white; font-weight: bold; font-size: 11px; }}
        .chart-container {{ margin: 20px 0; text-align: center; }}
        .chart-container img {{ max-width: 100%; border-radius: 8px;
                                box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
        .health-bar {{ display: inline-block; height: 14px; border-radius: 7px;
                       vertical-align: middle; }}
        .footer {{ margin-top: 30px; font-size: 11px; color: #888;
                   border-top: 1px solid #eee; padding-top: 15px; }}
        .red-row {{ background-color: #fff0f0 !important; }}
        .projection-total {{ background-color: #eaf4ff !important; font-weight: bold; }}
    </style></head>
    <body><div class="container">

    <h1>Glue Job Status &amp; SLA Dashboard</h1>
    <p><b>Report Generated:</b> {now_pst} &nbsp;|&nbsp;
       <b>Period:</b> Past 7 Days</p>

    <div style="margin: 15px 0;">
        <span class="summary-box" style="background:#17a2b8;">Jobs: {total_jobs}</span>
        <span class="summary-box" style="background:#28a745;">Succeeded: {succeeded}</span>
        <span class="summary-box" style="background:{'#dc3545' if failed else '#6c757d'};">
            Failed: {failed}</span>
        <span class="summary-box" style="background:{'#007bff' if running else '#6c757d'};">
            Running: {running}</span>
        <span class="summary-box" style="background:#28a745;">
            SLA On Time: {sla_on_time}</span>
        <span class="summary-box" style="background:{'#dc3545' if sla_breached else '#28a745'};">
            SLA Breached: {sla_breached}</span>
        <span class="summary-box" style="background:#8e44ad;">
            Cost Today: ${total_cost_today:.2f}</span>
        <span class="summary-box" style="background:#2c3e50;">
            Cost 7-Day: ${total_cost_7d:.2f}</span>
    </div>
    """

    # ================================================================
    # Section 1: Main Job Status Table
    # ================================================================
    html += """
    <h2>1. Job Status &amp; SLA</h2>
    <table>
     <thead><tr>
      <th>#</th><th>Job Name</th><th>Domain</th><th>Status</th>
      <th>SLA Limit</th><th>Exec Time</th><th>SLA Status</th>
      <th>Last Run (PST)</th><th>Worker</th><th>Workers</th>
      <th>Cost (Today)</th><th>Runs (24h)</th><th>Avg Exec (24h)</th>
      <th>Success Rate</th><th>Health</th>
     </tr></thead><tbody>
    """

    for i, j in enumerate(job_statuses, 1):
        sla_color = _status_color(j["sla_status"])
        status_color = _status_color(j["status"])
        h_score = j["health_score"]
        h_color = health_color(h_score)
        row_class = "red-row" if j["sla_status"] == "BREACHED" or j["status"] == "FAILED" else ""

        html += f"""
        <tr class="{row_class}">
         <td>{i}</td>
         <td><b>{j['job_name']}</b></td>
         <td>{j['domain']}</td>
         <td><span class="badge" style="background:{status_color};">{j['status']}</span></td>
         <td>{j['sla_minutes']} min</td>
         <td>{j['execution_time_min']}</td>
         <td><span class="badge" style="background:{sla_color};">{j['sla_status']}</span></td>
         <td style="font-size:11px;">{j['last_run_pst']}</td>
         <td>{j['worker_type']}</td>
         <td>{j['num_workers']}</td>
         <td>${j['cost_today']:.2f}</td>
         <td>{j['runs_last_24h']}</td>
         <td>{j['avg_exec_min']}</td>
         <td>{j['success_rate_24h']}</td>
         <td>
           <div class="health-bar" style="width:{h_score}px;background:{h_color};"></div>
           <span style="font-size:11px;color:{h_color};font-weight:bold;">{h_score}</span>
         </td>
        </tr>"""

        if j["error_message"] and j["status"] in ("FAILED", "ERROR", "TIMEOUT"):
            html += f"""
        <tr class="{row_class}">
         <td colspan="15" style="color:#dc3545;font-size:11px;padding-left:30px;">
           <b>Error:</b> {j['error_message'][:200]}</td>
        </tr>"""

    html += "</tbody></table>"

    # ================================================================
    # Section 2: SLA Charts
    # ================================================================
    html += "<h2>2. SLA &amp; Cost Trends (Past 7 Days)</h2>"

    # -- SLA Pie --
    sla_pie_b64 = generate_sla_pie_chart(days, daily)
    sla_trend_b64 = generate_sla_trend_chart(days, daily)
    cost_trend_b64 = generate_cost_trend_chart(days, daily)

    if sla_pie_b64 or sla_trend_b64:
        html += '<div style="display:flex;flex-wrap:wrap;gap:20px;justify-content:center;">'
        if sla_pie_b64:
            html += f'''<div class="chart-container">
                <img src="data:image/png;base64,{sla_pie_b64}" alt="SLA Pie" /></div>'''
        if sla_trend_b64:
            html += f'''<div class="chart-container">
                <img src="data:image/png;base64,{sla_trend_b64}" alt="SLA Trend" /></div>'''
        html += "</div>"
    else:
        html += html_sla_pie_fallback(days, daily)

    if cost_trend_b64:
        html += f'''<div class="chart-container" style="margin-top:15px;">
            <img src="data:image/png;base64,{cost_trend_b64}" alt="Cost Trend" /></div>'''
    else:
        html += html_cost_bar_fallback(days, daily)

    # ================================================================
    # Section 3: 7-Day Summary Table
    # ================================================================
    html += """
    <h2>3. Daily Summary (Past 7 Days)</h2>
    <table>
     <thead><tr>
      <th>Date</th><th>Total Runs</th><th>Succeeded</th><th>Failed</th>
      <th>On Time</th><th>Breached</th><th>SLA %</th><th>Daily Cost</th>
     </tr></thead><tbody>
    """

    for d in days:
        dd = daily[d]
        total_runs = dd["runs"]
        sla_total = dd["on_time"] + dd["breached"]
        sla_pct = f"{(dd['on_time'] / sla_total) * 100:.0f}%" if sla_total > 0 else "N/A"
        sla_pct_color = "#28a745" if dd["breached"] == 0 else "#dc3545"

        html += f"""
        <tr>
         <td><b>{d}</b></td>
         <td>{total_runs}</td>
         <td style="color:#28a745;font-weight:bold;">{dd['succeeded']}</td>
         <td style="color:{'#dc3545' if dd['failed'] else '#333'};
             font-weight:{'bold' if dd['failed'] else 'normal'};">{dd['failed']}</td>
         <td style="color:#28a745;">{dd['on_time']}</td>
         <td style="color:{'#dc3545' if dd['breached'] else '#333'};
             font-weight:{'bold' if dd['breached'] else 'normal'};">{dd['breached']}</td>
         <td style="color:{sla_pct_color};font-weight:bold;">{sla_pct}</td>
         <td>${dd['cost']:.2f}</td>
        </tr>"""

    html += "</tbody></table>"

    # ================================================================
    # Section 4: Cost Projection
    # ================================================================
    html += """
    <h2>4. Cost Projection (Based on 7-Day Actuals)</h2>
    <p style="color:#666;font-size:12px;">
       Pricing: $0.44 per DPU-hour. Projections extrapolated from the past 7-day spend.</p>
    <table>
     <thead><tr>
      <th>Domain</th><th>Worker Type(s)</th><th>Runs (7d)</th>
      <th>Total Exec (min)</th><th>Actual 7-Day</th>
      <th>Weekly Forecast</th><th>Monthly Forecast</th><th>Yearly Forecast</th>
     </tr></thead><tbody>
    """

    for p in projections:
        row_class = "projection-total" if p["domain"] == "TOTAL" else ""
        html += f"""
        <tr class="{row_class}">
         <td><b>{p['domain']}</b></td>
         <td>{p['worker_types']}</td>
         <td>{p['run_count']}</td>
         <td>{p['total_exec_min']}</td>
         <td>${p['cost_7d']:.2f}</td>
         <td>${p['weekly']:.2f}</td>
         <td>${p['monthly']:.2f}</td>
         <td>${p['yearly']:.2f}</td>
        </tr>"""

    html += "</tbody></table>"

    # ================================================================
    # Section 5: Jobs Needing Attention
    # ================================================================
    attention_jobs = [j for j in job_statuses
                      if j["sla_status"] == "BREACHED"
                      or j["status"] in ("FAILED", "ERROR", "TIMEOUT")
                      or j["health_score"] < 60]

    if attention_jobs:
        html += """
        <h2>5. Jobs Needing Attention</h2>
        <table>
         <thead><tr>
          <th>Job Name</th><th>Domain</th><th>Issue</th><th>Health</th><th>Detail</th>
         </tr></thead><tbody>
        """
        for j in attention_jobs:
            issues = []
            if j["sla_status"] == "BREACHED":
                issues.append(f"SLA Breached (took {j['execution_time_min']}, limit {j['sla_minutes']} min)")
            if j["status"] in ("FAILED", "ERROR", "TIMEOUT"):
                issues.append(f"Status: {j['status']}")
            if j["health_score"] < 60:
                issues.append(f"Low health score: {j['health_score']}/100")
            if not j["is_recent"]:
                issues.append("Not run in last 24h")

            h_score = j["health_score"]
            h_color = health_color(h_score)

            html += f"""
            <tr class="red-row">
             <td><b>{j['job_name']}</b></td>
             <td>{j['domain']}</td>
             <td style="color:#dc3545;font-weight:bold;">{'<br>'.join(issues)}</td>
             <td><span style="color:{h_color};font-weight:bold;">{h_score}/100
                 ({health_label(h_score)})</span></td>
             <td style="font-size:11px;">{j['error_message'][:150] if j['error_message'] else '-'}</td>
            </tr>"""

        html += "</tbody></table>"

    # ================================================================
    # Section 6: Summary Stats
    # ================================================================
    html += f"""
    <h2>{'6' if attention_jobs else '5'}. Summary</h2>
    <ul style="font-size:13px;line-height:1.8;">
      <li><b>Total Jobs Monitored:</b> {total_jobs}</li>
      <li><b>Jobs Run in Last 24h:</b> {recent_count}</li>
      <li><b>Succeeded:</b> {succeeded} &nbsp;|&nbsp;
          <b>Failed:</b> {failed} &nbsp;|&nbsp;
          <b>Running:</b> {running}</li>
      <li><b>SLA On Time:</b> {sla_on_time} &nbsp;|&nbsp;
          <b style="color:{'#dc3545' if sla_breached else '#333'};">
          SLA Breached: {sla_breached}</b></li>
      <li><b>Total Cost Today:</b> ${total_cost_today:.2f}</li>
      <li><b>Total Cost (7 Days):</b> ${total_cost_7d:.2f}</li>
      <li><b>Projected Monthly Cost:</b> ${total_cost_7d * 30.0 / 7.0:.2f}</li>
      <li><b>Total Runs in Last 24h:</b> {sum(j['runs_last_24h'] for j in job_statuses)}</li>
      <li><b>Avg Health Score:</b>
          {sum(j['health_score'] for j in job_statuses) // total_jobs}/100</li>
    </ul>
    """

    # ================================================================
    # Footer
    # ================================================================
    html += f"""
    <div class="footer">
      <p>Generated by Glue Job Status Report v2.0 &nbsp;|&nbsp; Big Data Team</p>
      <p>Script execution time: {(datetime.now() - script_start).total_seconds():.1f}s</p>
    </div>
    </div></body></html>
    """

    return html


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------
def send_email(html_body, sla_breached_count):
    """Send the HTML report via SMTP."""
    msg = MIMEMultipart("alternative")
    msg["From"] = SENDER
    msg["To"] = ", ".join(RECIPIENTS)

    status_tag = "SLA BREACHED" if sla_breached_count > 0 else "ALL OK"
    msg["Subject"] = (f"Glue Job Status & SLA Report - {status_tag} - "
                      f"{datetime.now(pytz.UTC).astimezone(PST).strftime('%Y-%m-%d')}")

    msg.attach(MIMEText(html_body, "html"))

    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    server.starttls()
    server.sendmail(SENDER, RECIPIENTS, msg.as_string())
    server.quit()
    print("Email sent successfully.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # 1. Collect job details
    print(f"Fetching status for {len(JOB_CONFIGS)} jobs...")
    job_statuses = []
    for cfg in JOB_CONFIGS:
        details = get_job_details(cfg)
        job_statuses.append(details)
        sla_flag = f" [SLA {details['sla_status']}]" if details["sla_status"] != "N/A" else ""
        print(f"  {cfg['name']}: {details['status']} | "
              f"{details['execution_time_min']}{sla_flag} | "
              f"${details['cost_today']:.2f} today")

    # 2. Aggregate 7-day stats
    days, daily = aggregate_7_day_stats(job_statuses)

    # 3. Build cost projections
    projections = build_cost_projections(job_statuses)

    # 4. Generate HTML
    print("Generating HTML report...")
    html = generate_html(job_statuses, days, daily, projections)

    # 5. Send email
    sla_breached = sum(1 for j in job_statuses if j["sla_status"] == "BREACHED")
    print(f"SLA Summary: {sum(1 for j in job_statuses if j['sla_status'] == 'ON TIME')} on time, "
          f"{sla_breached} breached")
    send_email(html, sla_breached)

    print(f"{appName} -- FINISHED")
    print(f"Total execution time: {datetime.now() - script_start}")


if __name__ == "__main__":
    main()
