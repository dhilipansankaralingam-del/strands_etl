"""
Daily Health Check Glue Job
============================
Queries the etl_audit_log table, validates data quality, generates dashboard,
and sends email report. Runs once daily at 9 PM.

Health Check Logic:
- For each table, sum(file_row_count) must equal sum(loaded_count)
- All validation_status must be PASS
- count_match must be PASS for all records
- If all checks pass: emit SUCCESS, otherwise FAIL

Usage:
    aws glue start-job-run --job-name daily-health-check \
        --arguments '{"--CONFIG_S3_PATH":"s3://bucket/config/health_check_config.json"}'
"""

import sys
import json
import boto3
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta
import base64
from io import BytesIO
from awsglue.utils import getResolvedOptions

# Try to import matplotlib, if not available, charts will be skipped
try:
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend
    import matplotlib.pyplot as plt
    from matplotlib.patches import Wedge
    import numpy as np
    CHARTS_AVAILABLE = True
except ImportError:
    print("WARNING: matplotlib not available. Charts will be disabled.")
    CHARTS_AVAILABLE = False

# Initialize clients
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')
glue_client = boto3.client('glue')

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'CONFIG_S3_PATH'])
job_name = args['JOB_NAME']
config_s3_path = args['CONFIG_S3_PATH']

# Parse S3 path
config_s3_parts = config_s3_path.replace('s3://', '').split('/', 1)
config_bucket = config_s3_parts[0]
config_key = config_s3_parts[1]

print(f"Starting Daily Health Check Job: {job_name}")
print(f"Config: {config_s3_path}")


def load_config():
    """Load configuration from S3"""
    try:
        response = s3_client.get_object(Bucket=config_bucket, Key=config_key)
        config_content = response['Body'].read().decode('utf-8')
        config = json.loads(config_content)
        print(f"✓ Configuration loaded successfully")
        return config
    except Exception as e:
        print(f"❌ Error loading configuration: {str(e)}")
        raise


def run_athena_query(query, database, output_location):
    """Execute Athena query and return results"""
    try:
        print(f"Executing query in database '{database}'...")

        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': output_location}
        )

        query_execution_id = response['QueryExecutionId']
        print(f"Query execution ID: {query_execution_id}")

        # Wait for query to complete
        max_attempts = 60
        attempt = 0
        while attempt < max_attempts:
            result = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = result['QueryExecution']['Status']['State']

            if status == 'SUCCEEDED':
                print(f"✓ Query completed successfully")
                break
            elif status in ['FAILED', 'CANCELLED']:
                reason = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                raise Exception(f"Query {status}: {reason}")

            attempt += 1
            time.sleep(2)

        if attempt >= max_attempts:
            raise Exception("Query timeout after 120 seconds")

        # Get results
        results = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        return results

    except Exception as e:
        print(f"❌ Athena query error: {str(e)}")
        raise


def parse_athena_results(results):
    """Parse Athena results into list of dicts"""
    rows = results['ResultSet']['Rows']
    if len(rows) == 0:
        return []

    # Extract column names from first row
    headers = [col['VarCharValue'] for col in rows[0]['Data']]

    # Extract data rows
    data = []
    for row in rows[1:]:
        row_data = {}
        for i, col in enumerate(row['Data']):
            row_data[headers[i]] = col.get('VarCharValue', '')
        data.append(row_data)

    return data


def get_daily_audit_summary(config, check_date):
    """Get audit summary for the specified date"""
    database = config['database']
    table = config['audit_table']
    output_location = config['athena_output_location']

    # Parse date
    year = check_date.strftime('%Y')
    month = check_date.strftime('%m')
    day = check_date.strftime('%d')

    # Query to get daily summary
    query = f"""
    SELECT
        table_name,
        COUNT(*) as run_count,
        SUM(CAST(file_count AS BIGINT)) as total_file_count,
        SUM(CAST(file_row_count AS BIGINT)) as total_file_rows,
        SUM(CAST(loaded_count AS BIGINT)) as total_loaded_rows,
        COUNT(CASE WHEN count_match = 'PASS' THEN 1 END) as count_match_pass,
        COUNT(CASE WHEN count_match = 'FAIL' THEN 1 END) as count_match_fail,
        COUNT(CASE WHEN validation_status = 'PASS' THEN 1 END) as validation_pass,
        COUNT(CASE WHEN validation_status = 'FAIL' THEN 1 END) as validation_fail,
        COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as success_count,
        COUNT(CASE WHEN status = 'FAIL' THEN 1 END) as fail_count,
        MAX(load_timestamp) as latest_run
    FROM {database}.{table}
    WHERE year = '{year}'
        AND month = '{month}'
        AND day = '{day}'
    GROUP BY table_name
    """

    print(f"Querying audit data for {check_date.strftime('%Y-%m-%d')}...")
    results = run_athena_query(query, database, output_location)
    summary = parse_athena_results(results)

    return summary


def get_validation_details(config, check_date):
    """Get detailed validation results for the day"""
    database = config['database']
    table = config['audit_table']
    output_location = config['athena_output_location']

    year = check_date.strftime('%Y')
    month = check_date.strftime('%m')
    day = check_date.strftime('%d')

    query = f"""
    SELECT
        table_name,
        validation_status,
        validation_details,
        count_match,
        file_row_count,
        loaded_count,
        load_timestamp
    FROM {database}.{table}
    WHERE year = '{year}'
        AND month = '{month}'
        AND day = '{day}'
    ORDER BY load_timestamp DESC
    """

    results = run_athena_query(query, database, output_location)
    details = parse_athena_results(results)

    return details


def check_health_status(summary):
    """
    Check overall health status based on summary data
    Returns: (is_healthy: bool, issues: list)
    """
    issues = []

    for table_data in summary:
        table_name = table_data['table_name']

        # Check 1: File count vs loaded count
        total_file_rows = int(table_data.get('total_file_rows', 0))
        total_loaded_rows = int(table_data.get('total_loaded_rows', 0))

        if total_file_rows != total_loaded_rows:
            issues.append({
                'table': table_name,
                'type': 'COUNT_MISMATCH',
                'message': f"File rows ({total_file_rows}) != Loaded rows ({total_loaded_rows})"
            })

        # Check 2: Count match failures
        count_match_fail = int(table_data.get('count_match_fail', 0))
        if count_match_fail > 0:
            issues.append({
                'table': table_name,
                'type': 'COUNT_MATCH_FAIL',
                'message': f"{count_match_fail} runs had count match failures"
            })

        # Check 3: Validation failures
        validation_fail = int(table_data.get('validation_fail', 0))
        if validation_fail > 0:
            issues.append({
                'table': table_name,
                'type': 'VALIDATION_FAIL',
                'message': f"{validation_fail} runs had validation failures"
            })

        # Check 4: Job failures
        fail_count = int(table_data.get('fail_count', 0))
        if fail_count > 0:
            issues.append({
                'table': table_name,
                'type': 'JOB_FAIL',
                'message': f"{fail_count} job runs failed"
            })

    is_healthy = len(issues) == 0
    return is_healthy, issues


def create_donut_chart(passed, failed):
    """Create donut chart for pass/fail distribution"""
    if not CHARTS_AVAILABLE:
        return None

    try:
        fig, ax = plt.subplots(figsize=(4, 4))

        sizes = [passed, failed]
        colors = ['#10B981', '#EF4444']
        labels = [f'Passed\n{passed}', f'Failed\n{failed}']

        wedges, texts = ax.pie(sizes, colors=colors, startangle=90,
                                wedgeprops=dict(width=0.5))

        for i, text in enumerate(texts):
            text.set_text(labels[i])
            text.set_fontsize(12)
            text.set_weight('bold')
            text.set_color('white' if i == 1 else '#1F2937')

        ax.axis('equal')

        # Convert to base64
        buffer = BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight', dpi=100, transparent=True)
        buffer.seek(0)
        image_base64 = base64.b64encode(buffer.read()).decode()
        plt.close()

        return image_base64
    except Exception as e:
        print(f"Error creating donut chart: {str(e)}")
        return None


def create_bar_chart(issues):
    """Create bar chart for issues by category"""
    if not CHARTS_AVAILABLE or not issues:
        return None

    try:
        # Count issues by type
        issue_counts = {}
        for issue in issues:
            issue_type = issue['type']
            issue_counts[issue_type] = issue_counts.get(issue_type, 0) + 1

        if not issue_counts:
            return None

        fig, ax = plt.subplots(figsize=(6, 3))

        categories = list(issue_counts.keys())
        counts = list(issue_counts.values())
        colors = ['#EF4444', '#F59E0B', '#8B5CF6', '#EC4899']

        bars = ax.barh(categories, counts, color=colors[:len(categories)])

        ax.set_xlabel('Count', fontsize=10, weight='bold')
        ax.set_title('Issues by Category', fontsize=12, weight='bold', pad=10)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        # Add count labels
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax.text(width, bar.get_y() + bar.get_height()/2,
                   f' {int(width)}', ha='left', va='center',
                   fontsize=10, weight='bold')

        plt.tight_layout()

        buffer = BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight', dpi=100, transparent=True)
        buffer.seek(0)
        image_base64 = base64.b64encode(buffer.read()).decode()
        plt.close()

        return image_base64
    except Exception as e:
        print(f"Error creating bar chart: {str(e)}")
        return None


def create_gauge_chart(error_rate, title):
    """Create gauge chart for error rate"""
    if not CHARTS_AVAILABLE:
        return None

    try:
        fig, ax = plt.subplots(figsize=(2.5, 2))

        # Gauge parameters
        theta = np.linspace(0, np.pi, 100)

        # Background arc
        ax.plot(np.cos(theta), np.sin(theta), 'lightgray', linewidth=8)

        # Value arc
        value_theta = np.linspace(0, np.pi * (error_rate / 100), 100)
        color = '#10B981' if error_rate < 5 else '#F59E0B' if error_rate < 20 else '#EF4444'
        ax.plot(np.cos(value_theta), np.sin(value_theta), color, linewidth=8)

        # Center text
        ax.text(0, -0.3, f'{error_rate:.1f}%', ha='center', va='center',
               fontsize=16, weight='bold', color=color)
        ax.text(0, -0.55, title, ha='center', va='center',
               fontsize=8, color='#6B7280')

        ax.set_xlim(-1.2, 1.2)
        ax.set_ylim(-0.8, 1.2)
        ax.axis('off')

        buffer = BytesIO()
        plt.savefig(buffer, format='png', bbox_inches='tight', dpi=100, transparent=True)
        buffer.seek(0)
        image_base64 = base64.b64encode(buffer.read()).decode()
        plt.close()

        return image_base64
    except Exception as e:
        print(f"Error creating gauge chart: {str(e)}")
        return None


def generate_html_report(config, summary, details, issues, check_date):
    """Generate HTML email report"""

    current_date_str = check_date.strftime('%B %d, %Y')
    current_time_str = datetime.now().strftime('%I:%M %p')

    # Calculate metrics
    total_passed = sum(int(s.get('validation_pass', 0)) + int(s.get('count_match_pass', 0)) for s in summary)
    total_failed = sum(int(s.get('validation_fail', 0)) + int(s.get('count_match_fail', 0)) for s in summary)

    passed_count = total_passed
    failed_count = total_failed

    # Calculate health score
    health_score = (passed_count / (passed_count + failed_count) * 100) if (passed_count + failed_count) > 0 else 0
    health_color = "#10B981" if health_score >= 80 else "#F59E0B" if health_score >= 60 else "#EF4444"
    health_emoji = "🎉" if health_score >= 90 else "👍" if health_score >= 70 else "⚠️" if health_score >= 50 else "🚨"

    # Generate charts
    donut_chart = create_donut_chart(passed_count, failed_count)
    bar_chart = create_bar_chart(issues)

    # Generate gauge charts for each table
    gauge_charts = []
    gauge_html = ""
    for table_data in summary:
        total_runs = int(table_data.get('run_count', 0))
        fail_runs = int(table_data.get('fail_count', 0))
        error_rate = (fail_runs / total_runs * 100) if total_runs > 0 else 0

        gauge = create_gauge_chart(error_rate, table_data['table_name'])
        if gauge:
            gauge_charts.append(gauge)
            gauge_html += f'''
                <td style="width: 25%; padding: 20px; text-align: center; vertical-align: top;">
                    <img src="data:image/png;base64,{gauge}" style="max-width: 150px;" />
                </td>
            '''

    # Generate KPI cards
    total_files = sum(int(s.get('total_file_count', 0)) for s in summary)
    total_rows = sum(int(s.get('total_file_rows', 0)) for s in summary)
    total_loaded = sum(int(s.get('total_loaded_rows', 0)) for s in summary)
    total_tables = len(summary)

    kpi_cards = [
        {"icon": "📁", "value": f"{total_files:,}", "label": "Files Processed", "color": "#3B82F6"},
        {"icon": "📊", "value": f"{total_rows:,}", "label": "Total Rows", "color": "#8B5CF6"},
        {"icon": "✅", "value": f"{total_loaded:,}", "label": "Rows Loaded", "color": "#10B981"},
        {"icon": "🗃️", "value": f"{total_tables}", "label": "Tables", "color": "#F59E0B"},
    ]

    kpi_cards_html = ""
    for kpi in kpi_cards:
        kpi_cards_html += f'''
            <td style="width: 25%; padding: 10px; vertical-align: top;">
                <div style="background: white; border-radius: 16px; padding: 20px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); text-align: center; border-left: 4px solid {kpi['color']};">
                    <div style="font-size: 32px; margin-bottom: 8px;">{kpi['icon']}</div>
                    <div style="font-size: 24px; font-weight: 700; color: #1F2937; margin-bottom: 4px;">{kpi['value']}</div>
                    <div style="font-size: 12px; color: #6B7280; font-weight: 500;">{kpi['label']}</div>
                </div>
            </td>
        '''

    # Generate validation rows
    validation_rows_html = ""
    for idx, table_data in enumerate(summary):
        bg_color = "#F9FAFB" if idx % 2 == 0 else "white"

        val_pass = int(table_data.get('validation_pass', 0))
        val_fail = int(table_data.get('validation_fail', 0))
        count_pass = int(table_data.get('count_match_pass', 0))
        count_fail = int(table_data.get('count_match_fail', 0))

        total_checks = val_pass + val_fail + count_pass + count_fail
        passed_checks = val_pass + count_pass

        status_emoji = "✅" if count_fail == 0 and val_fail == 0 else "❌"
        status_color = "#10B981" if count_fail == 0 and val_fail == 0 else "#EF4444"

        validation_rows_html += f'''
            <tr style="background: {bg_color};">
                <td style="padding: 12px 16px; border-bottom: 1px solid #E5E7EB;">
                    <span style="font-weight: 600; color: #1F2937;">{table_data['table_name']}</span>
                </td>
                <td style="padding: 12px 16px; border-bottom: 1px solid #E5E7EB; color: #4B5563; font-size: 13px;">
                    Count Match: {count_pass} pass, {count_fail} fail<br/>
                    Validations: {val_pass} pass, {val_fail} fail
                </td>
                <td style="padding: 12px 16px; text-align: center; border-bottom: 1px solid #E5E7EB;">
                    <span style="font-weight: 600; color: #1F2937;">{passed_checks}/{total_checks}</span>
                </td>
                <td style="padding: 12px 16px; text-align: center; border-bottom: 1px solid #E5E7EB;">
                    <span style="font-size: 20px;">{status_emoji}</span>
                    <div style="font-size: 11px; color: {status_color}; font-weight: 600; margin-top: 2px;">
                        {"PASS" if count_fail == 0 and val_fail == 0 else "FAIL"}
                    </div>
                </td>
            </tr>
        '''

    # Placeholder for state pie chart (would need actual data)
    state_pie_chart = ""

    # Build complete HTML
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #F3F4F6;">

    <!-- Header Banner -->
    <table width="100%" cellpadding="0" cellspacing="0" style="background: linear-gradient(135deg, #0F172A 0%, #1E293B 50%, #334155 100%);">
        <tr>
            <td style="padding: 40px 20px; text-align: center;">
                <div style="font-size: 48px; margin-bottom: 10px;">📊</div>
                <h1 style="color: #F8FAFC; margin: 0; font-size: 28px; font-weight: 700; letter-spacing: -0.5px; text-shadow: 0 2px 4px rgba(0,0,0,0.3);">
                    Data Quality Dashboard
                </h1>
                <p style="color: #94A3B8; margin: 10px 0 0 0; font-size: 14px;">
                    ETL Health Check • {current_date_str} • {current_time_str}
                </p>
            </td>
        </tr>
    </table>

    <!-- Health Score Hero -->
    <table width="100%" cellpadding="0" cellspacing="0" style="background: linear-gradient(180deg, #334155 0%, #F3F4F6 100%);">
        <tr>
            <td style="padding: 30px 20px;">
                <table width="600" align="center" cellpadding="0" cellspacing="0" style="background: white; border-radius: 24px; box-shadow: 0 25px 50px rgba(0,0,0,0.15);">
                    <tr>
                        <td style="padding: 40px; text-align: center;">
                            <div style="font-size: 64px; margin-bottom: 10px;">{health_emoji}</div>
                            <div style="font-size: 72px; font-weight: 800; color: {health_color}; line-height: 1;">
                                {health_score:.0f}%
                            </div>
                            <div style="font-size: 18px; color: #6B7280; margin-top: 8px; font-weight: 500;">
                                Overall Data Health Score
                            </div>
                            <div style="margin-top: 20px;">
                                <span style="display: inline-block; background: #10B981; color: white; padding: 8px 20px; border-radius: 20px; margin: 4px; font-weight: 600;">
                                    ✅ {passed_count} Passed
                                </span>
                                <span style="display: inline-block; background: #EF4444; color: white; padding: 8px 20px; border-radius: 20px; margin: 4px; font-weight: 600;">
                                    ❌ {failed_count} Failed
                                </span>
                            </div>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>

    <!-- Charts Section -->
    {"" if not donut_chart else f'''
    <table width="100%" cellpadding="0" cellspacing="0" style="background: #F3F4F6;">
        <tr>
            <td style="padding: 20px;">
                <table width="800" align="center" cellpadding="0" cellspacing="0">
                    <tr>
                        <!-- Donut Chart -->
                        <td style="width: 35%; padding: 10px; vertical-align: top;">
                            <div style="background: white; border-radius: 20px; padding: 20px; box-shadow: 0 4px 20px rgba(0,0,0,0.08); text-align: center;">
                                <h3 style="color: #1F2937; margin: 0 0 15px 0; font-size: 16px;">Pass/Fail Distribution</h3>
                                <img src="data:image/png;base64,{donut_chart}" style="max-width: 200px;" />
                            </div>
                        </td>
                        <!-- Bar Chart -->
                        <td style="width: 65%; padding: 10px; vertical-align: top;">
                            <div style="background: white; border-radius: 20px; padding: 20px; box-shadow: 0 4px 20px rgba(0,0,0,0.08);">
                                <h3 style="color: #1F2937; margin: 0 0 15px 0; font-size: 16px; text-align: center;">Issues by Category</h3>
                                {f'<img src="data:image/png;base64,{bar_chart}" style="max-width: 100%;" />' if bar_chart else '<p style="color: #10B981; text-align: center; font-size: 18px;">🎉 No issues found!</p>'}
                            </div>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
    '''}

    <!-- KPI Cards -->
    <table width="100%" cellpadding="0" cellspacing="0" style="background: #F3F4F6;">
        <tr>
            <td style="padding: 0 20px 20px 20px;">
                <table width="800" align="center" cellpadding="0" cellspacing="0">
                    <tr>
                        <td colspan="4" style="padding-bottom: 15px;">
                            <h2 style="color: #1F2937; margin: 0; font-size: 20px;">
                                📈 Key Metrics
                            </h2>
                        </td>
                    </tr>
                    <tr>
                        {kpi_cards_html}
                    </tr>
                </table>
            </td>
        </tr>
    </table>

    <!-- Gauge Charts -->
    {"" if not gauge_charts else f'''
    <table width="100%" cellpadding="0" cellspacing="0" style="background: #F3F4F6;">
        <tr>
            <td style="padding: 0 20px 20px 20px;">
                <table width="800" align="center" cellpadding="0" cellspacing="0" style="background: white; border-radius: 20px; box-shadow: 0 4px 20px rgba(0,0,0,0.08);">
                    <tr>
                        <td colspan="4" style="padding: 20px 20px 10px 20px;">
                            <h3 style="color: #1F2937; margin: 0; font-size: 16px;">📉 Error Rate by Table</h3>
                        </td>
                    </tr>
                    <tr>
                        {gauge_html}
                    </tr>
                </table>
            </td>
        </tr>
    </table>
    '''}

    <!-- Detailed Validation Table -->
    <table width="100%" cellpadding="0" cellspacing="0" style="background: #F3F4F6;">
        <tr>
            <td style="padding: 0 20px 30px 20px;">
                <table width="800" align="center" cellpadding="0" cellspacing="0" style="background: white; border-radius: 20px; box-shadow: 0 4px 20px rgba(0,0,0,0.08); overflow: hidden;">
                    <tr>
                        <td style="padding: 25px 25px 15px 25px;">
                            <h2 style="color: #1F2937; margin: 0; font-size: 20px;">
                                🔍 Detailed Validation Results
                            </h2>
                            <p style="color: #6B7280; margin: 5px 0 0 0; font-size: 13px;">
                                Summary of all ETL job runs for {current_date_str}
                            </p>
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 0 25px 25px 25px;">
                            <table width="100%" cellpadding="0" cellspacing="0" style="border-radius: 12px; overflow: hidden; border: 1px solid #E5E7EB;">
                                <tr style="background: linear-gradient(135deg, #1e1b4b 0%, #4c1d95 100%);">
                                    <th style="padding: 14px 16px; text-align: left; color: white; font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px;">Table Name</th>
                                    <th style="padding: 14px 16px; text-align: left; color: white; font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px;">Validation Details</th>
                                    <th style="padding: 14px 16px; text-align: center; color: white; font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px;">Passed</th>
                                    <th style="padding: 14px 16px; text-align: center; color: white; font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px;">Status</th>
                                </tr>
                                {validation_rows_html}
                            </table>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>

    <!-- Footer -->
    <table width="100%" cellpadding="0" cellspacing="0" style="background: linear-gradient(135deg, #1e1b4b 0%, #312e81 100%);">
        <tr>
            <td style="padding: 30px 20px; text-align: center;">
                <div style="font-size: 24px; margin-bottom: 10px;">
                    {"🏆 Stellar data quality today!" if health_score >= 90 else "💪 Good progress, keep it up!" if health_score >= 70 else "🔧 Some issues need attention" if health_score >= 50 else "🚨 Critical issues detected!"}
                </div>
                <p style="color: rgba(255,255,255,0.7); margin: 0; font-size: 12px;">
                    Generated by ETL Health Check • Automated DQA Pipeline
                </p>
                <p style="color: rgba(255,255,255,0.5); margin: 10px 0 0 0; font-size: 11px;">
                    📧 Questions? Reply to this email or reach out to the data engineering team
                </p>
            </td>
        </tr>
    </table>

</body>
</html>
"""

    return html, health_score


def send_email_report(config, html, health_score, check_date):
    """Send HTML email report via SMTP"""
    try:
        sender = config['email']['sender']
        recipients = config['email']['recipients']
        smtp_host = config['email']['smtp_host']
        smtp_port = config['email']['smtp_port']

        # Build email
        msg = MIMEMultipart('alternative')
        msg['From'] = sender
        msg['To'] = ", ".join(recipients)
        msg.add_header('Content-Type', 'text/html')

        # Subject with emoji based on health score
        emoji = '🎉' if health_score >= 90 else '📊' if health_score >= 70 else '⚠️' if health_score >= 50 else '🚨'
        msg['Subject'] = f"{emoji} Data Quality Summary - {check_date.strftime('%Y-%m-%d')} | {health_score:.0f}% Health Score"

        # Attach HTML
        part = MIMEText(html, 'html')
        msg.attach(part)

        # Send email
        print(f"Connecting to SMTP server {smtp_host}:{smtp_port}...")
        server = smtplib.SMTP(smtp_host, smtp_port)

        # Use TLS if configured
        if config['email'].get('use_tls', True):
            server.starttls()

        # Authenticate if credentials provided
        if 'username' in config['email'] and 'password' in config['email']:
            server.login(config['email']['username'], config['email']['password'])

        text = msg.as_string()
        server.sendmail(sender, recipients, text)
        server.quit()

        print(f"✓ Email sent successfully to {len(recipients)} recipient(s)")
        return True

    except Exception as e:
        print(f"❌ Error sending email: {str(e)}")
        return False


def main():
    """Main execution flow"""
    try:
        start_time = datetime.now()
        print("=" * 80)
        print("DAILY HEALTH CHECK - EXECUTION STARTED")
        print("=" * 80)

        # Load configuration
        config = load_config()

        # Determine check date (yesterday by default, or today if specified)
        check_date = datetime.now().date()
        if config.get('check_previous_day', False):
            check_date = check_date - timedelta(days=1)

        print(f"\nCheck date: {check_date.strftime('%Y-%m-%d')}")

        # Get audit summary
        print("\n" + "=" * 80)
        print("STEP 1: Querying Audit Table")
        print("=" * 80)
        summary = get_daily_audit_summary(config, check_date)

        if not summary:
            print("⚠️  No audit data found for the specified date")
            print(f"This may be normal if no ETL jobs ran on {check_date.strftime('%Y-%m-%d')}")

            # Send notification email about no data
            if config.get('send_email_on_no_data', False):
                html = f"""
                <html>
                <body style="font-family: Arial, sans-serif; padding: 20px;">
                    <h2>⚠️ No ETL Activity Detected</h2>
                    <p>No audit records were found for <strong>{check_date.strftime('%Y-%m-%d')}</strong>.</p>
                    <p>This may indicate:</p>
                    <ul>
                        <li>No ETL jobs were scheduled to run today</li>
                        <li>All jobs were skipped (no data to process)</li>
                        <li>Potential issue with job scheduling</li>
                    </ul>
                    <p>Please verify your ETL job schedule and execution logs.</p>
                </body>
                </html>
                """
                send_email_report(config, html, 100, check_date)

            sys.exit(0)

        print(f"✓ Found audit data for {len(summary)} table(s)")
        for table_data in summary:
            print(f"  - {table_data['table_name']}: {table_data['run_count']} runs")

        # Get validation details
        print("\n" + "=" * 80)
        print("STEP 2: Analyzing Validation Results")
        print("=" * 80)
        details = get_validation_details(config, check_date)
        print(f"✓ Retrieved {len(details)} validation record(s)")

        # Check health status
        print("\n" + "=" * 80)
        print("STEP 3: Health Status Check")
        print("=" * 80)
        is_healthy, issues = check_health_status(summary)

        if is_healthy:
            print("✅ HEALTH CHECK PASSED - All validations successful!")
        else:
            print(f"❌ HEALTH CHECK FAILED - {len(issues)} issue(s) detected:")
            for issue in issues:
                print(f"  • {issue['table']} [{issue['type']}]: {issue['message']}")

        # Generate HTML report
        print("\n" + "=" * 80)
        print("STEP 4: Generating HTML Report")
        print("=" * 80)
        html, health_score = generate_html_report(config, summary, details, issues, check_date)
        print(f"✓ HTML report generated (Health Score: {health_score:.0f}%)")

        # Send email
        print("\n" + "=" * 80)
        print("STEP 5: Sending Email Report")
        print("=" * 80)
        email_sent = send_email_report(config, html, health_score, check_date)

        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print("\n" + "=" * 80)
        print("EXECUTION SUMMARY")
        print("=" * 80)
        print(f"Check Date:     {check_date.strftime('%Y-%m-%d')}")
        print(f"Tables:         {len(summary)}")
        print(f"Health Score:   {health_score:.0f}%")
        print(f"Status:         {'PASS ✅' if is_healthy else 'FAIL ❌'}")
        print(f"Issues Found:   {len(issues)}")
        print(f"Email Sent:     {'Yes ✓' if email_sent else 'No ✗'}")
        print(f"Duration:       {duration:.2f} seconds")
        print("=" * 80)

        # Exit with appropriate status
        if is_healthy:
            print("\n✅ Job completed successfully - All health checks passed!")
            sys.exit(0)
        else:
            print("\n❌ Job completed with failures - Health checks failed!")
            sys.exit(1)

    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
