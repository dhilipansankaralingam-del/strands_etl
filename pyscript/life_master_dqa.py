#####################################################################################################
#                                                                                                   #
#   Life Master Data Quality Assurance (DQA) - Email Dashboard                                      #
#   Generates a modern, styled HTML email with DQ validation results                                #
#                                                                                                   #
#===================================================================================================#
#   Uses same visual styling as inforce_dashboard_v2.py                                             #
#===================================================================================================#
#####################################################################################################

import boto3
from py4j.java_gateway import java_import
import botocore
import json
import pytz
import sys
import io
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.utils import COMMASPACE, formatdate
from email import encoders
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

######################################################################
############## DQA
######################################################################
import pyspark
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from functools import reduce
from pyspark.sql.functions import date_format, current_date

######################################################################
appName = "DQA"
######################################################################

print('{0} -- STARTED'.format(appName.upper()))
start = datetime.now()

spark = SparkSession.builder.config('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.SparkCatalog')\
    .config('spark.sql.catalog.glue_catalog.warehouse', '')\
    .config('spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')\
    .config('spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')\
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions').getOrCreate()

spark.sparkContext.setLogLevel('WARN')
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter")
spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("set hive.optimize.sort.dynamic.partition=true")
spark.conf.set("spark.sql.crossJoin.enabled", "true")

s3_client = boto3.client('s3')


# ---- Helper Functions ----
def get_file_from_s3(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response['Body'].read().decode('utf-8')


def file_exists(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise e


# ---- Get Parameters ----
args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'JOB_NAME', 'LAYER'])
layer = args['LAYER']
general_layer = layer.split("_")[2]

# ---- SQL Files & Table Names ----
sql_file_keys = [
    'quality_1.sql'
]
table_names = [
    "enterprise_life_policy_master"
]

# ---- Delete & Reappear Query ----
another_sql_key = "delete_reappear.sql"
sql_query1 = get_file_from_s3(args['S3_BUCKET'], another_sql_key)
result_to_be_attached = spark.sql(sql_query1)
record_count = result_to_be_attached.count()

# ---- Run DQA Queries ----
all_stats_arrays = []
for i, sql_key in enumerate(sql_file_keys):
    sql_query = get_file_from_s3(args['S3_BUCKET'], sql_key)
    sql_query = (sql_query
                 .replace('AS "Count"', 'AS `Count`')
                 .replace('AS "Rolling_10_day_avg"', 'AS `Rolling_10_day_avg`')
                 .replace('AS "status"', 'AS `status`'))
    todays_dqa = spark.sql(sql_query)
    rows = todays_dqa.toPandas().to_dict("records")
    all_stats_arrays.append((table_names[i], rows))


# ---- Status Color Helper ----
def _status_color(status_val):
    s = str(status_val).strip().upper()
    if s in ("PASS", "OK", "NORMAL"):
        return "#28a745"
    elif s in ("FAIL", "ALERT", "ANOMALY"):
        return "#dc3545"
    elif s in ("WARN", "WARNING"):
        return "#fd7e14"
    return "#6c757d"


def _status_icon(status_val):
    s = str(status_val).strip().upper()
    if s in ("PASS", "OK", "NORMAL"):
        return "&#x2705;"
    elif s in ("FAIL", "ALERT", "ANOMALY"):
        return "&#x274C;"
    elif s in ("WARN", "WARNING"):
        return "&#x26A0;"
    return "&#x2139;"


# ---- Count pass/fail for summary ----
total_checks = 0
passed_checks = 0
failed_checks = 0
for _, rows in all_stats_arrays:
    for row in rows:
        total_checks += 1
        status_val = str(row.get("status", "")).strip().upper()
        if status_val in ("PASS", "OK", "NORMAL"):
            passed_checks += 1
        elif status_val in ("FAIL", "ALERT", "ANOMALY"):
            failed_checks += 1

pass_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0
pass_color = "#28a745" if pass_rate >= 90 else "#fd7e14" if pass_rate >= 70 else "#dc3545"
run_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ---- Validation Descriptions ----
validation_descriptions = {
    "RECORD_VOLUME": "Total number of records received daily, with rolling 10-day average and standard deviation checks.",
    "NULLABILITY_COMBINED": "Count of records with null values in critical fields (application_id, etc.).",
    "AMOUNT_ANOMALIES_COMBINED": "Count of records with invalid amount values (negative/face amounts or excessively high values).",
    "PREMIUM_CONSISTENCY": "Count of records where annualized premium doesn't match modal premium * 12 for annually billed policies.",
    "DUPLICATES_COMBINED": "Count of duplicate records based on business keys (application_id).",
    "DATE_VALIDATIONS_COMBINED": "Count of records with invalid date relationships (effective dates in future, expiry dates too far out, termination before effective, etc.).",
}

# ---- Build HTML Email ----
html = f"""
<html>
<head>
<style>
    body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 0; color: #333;
           background-color: #f0f2f5; line-height: 1.6; }}
    .container {{ max-width: 900px; margin: 0 auto; background: white;
                  border-radius: 12px; overflow: hidden;
                  box-shadow: 0 4px 20px rgba(0,0,0,0.12); }}
    .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 30%, #f093fb 60%, #f5576c 100%);
               padding: 35px 35px 30px 35px; color: white;
               position: relative; overflow: hidden; }}
    .header h1 {{ margin: 0 0 8px 0; font-size: 26px; letter-spacing: 0.5px;
                  text-shadow: 0 2px 4px rgba(0,0,0,0.2); }}
    .header p {{ margin: 0; font-size: 12px; opacity: 0.9; }}
    .content {{ padding: 30px 35px; }}

    h2 {{ color: #2c3e50; margin-top: 32px; margin-bottom: 18px; font-size: 18px;
          padding: 10px 16px; border-radius: 8px;
          background: linear-gradient(90deg, #f8f9ff 0%, #fff 100%);
          border-left: 5px solid #764ba2; }}

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

    .badge {{ display: inline-block; padding: 4px 12px; border-radius: 12px;
              color: white; font-weight: bold; font-size: 10px;
              letter-spacing: 0.5px; text-transform: uppercase;
              box-shadow: 0 2px 4px rgba(0,0,0,0.15); }}

    .summary-pill {{ display: inline-flex; align-items: center; gap: 6px;
                    padding: 12px 20px; margin: 4px 6px 4px 0;
                    border-radius: 25px; color: white; font-size: 13px;
                    font-weight: bold; text-align: center;
                    box-shadow: 0 3px 8px rgba(0,0,0,0.15); }}

    .kpi-card {{ border-radius: 16px; padding: 22px 16px;
                 text-align: center; position: relative;
                 overflow: hidden; box-shadow: 0 6px 20px rgba(0,0,0,0.10); }}
    .kpi-icon {{ font-size: 34px; margin-bottom: 10px; }}
    .kpi-label {{ font-size: 11px; font-weight: bold;
                  text-transform: uppercase; letter-spacing: 0.8px; }}
    .kpi-value {{ font-size: 32px; font-weight: bold; margin: 10px 0 4px 0;
                  text-shadow: 0 2px 8px rgba(0,0,0,0.08); }}

    .check-card {{ margin: 14px 0; border: 1px solid #e8eaf0; border-radius: 10px;
                   overflow: hidden; box-shadow: 0 2px 6px rgba(0,0,0,0.04); }}
    .check-card-header {{ display: flex; justify-content: space-between;
                          align-items: center; padding: 14px 18px;
                          background: linear-gradient(90deg, #fafbfe 0%, #fff 100%);
                          border-bottom: 1px solid #eee; }}
    .check-card-body {{ padding: 16px 18px; }}

    .dict-box {{ background: linear-gradient(135deg, #f8f9ff 0%, #fef9ff 100%);
                 border: 1px solid #e8e0f0; border-radius: 10px;
                 padding: 20px; margin: 14px 0; }}
    .dict-item {{ margin-bottom: 14px; padding-bottom: 14px; border-bottom: 1px solid #eee;
                  font-size: 12px; color: #555; }}
    .dict-item:last-child {{ border-bottom: none; margin-bottom: 0; padding-bottom: 0; }}

    .footer {{ padding: 25px 35px; font-size: 11px; color: #888;
               border-top: 2px solid #eee;
               background: linear-gradient(90deg, #fafbfc 0%, #f5f6fa 100%); }}

    .spacer {{ height: 20px; }}
    .spacer-sm {{ height: 12px; }}
</style>
</head>
<body>
<div class="container">

    <!-- ======== HEADER ======== -->
    <div class="header">
        <h1>&#x1F4CA; Data Quality Validations</h1>
        <p>&#x1F4C5; {run_date} &nbsp;&bull;&nbsp; &#x1F3E2; Life Master &nbsp;&bull;&nbsp;
           &#x1F4C2; {general_layer.upper()} Layer</p>
    </div>

    <div class="content">

    <!-- ======== GREETING ======== -->
    <p style="font-size:13px;color:#555;">
        Hello,<br><br>
        Daily Data Quality Validations for the <strong>{general_layer.upper()}</strong> layer,
        generated by the Life DQ script.
    </p>

    <!-- ======== KPI SUMMARY ======== -->
    <h2>&#x1F3AF; 1. Summary</h2>
    <div class="spacer-sm"></div>

    <table style="width:auto;box-shadow:none;border:none;margin:0;">
      <tr style="background:none;">
        <td style="border:none;padding:6px 0;">
          <div class="summary-pill" style="background:linear-gradient(135deg,{pass_color},{pass_color}cc);">
            <span style="font-size:18px;">&#x2705;</span> Passed: {passed_checks}
          </div>
        </td>
        <td style="border:none;padding:6px 0;">
          <div class="summary-pill" style="background:linear-gradient(135deg,#dc3545,#dc3545cc);">
            <span style="font-size:18px;">&#x274C;</span> Failed: {failed_checks}
          </div>
        </td>
        <td style="border:none;padding:6px 0;">
          <div class="summary-pill" style="background:linear-gradient(135deg,#6c757d,#6c757dcc);">
            <span style="font-size:18px;">&#x1F4CB;</span> Total: {total_checks}
          </div>
        </td>
      </tr>
    </table>

    <div class="spacer-sm"></div>

    <!-- Pass Rate Bar -->
    <div style="background:linear-gradient(135deg, #fafbfe 0%, #f5f0ff 100%);
                border:1px solid #e8e0f0;border-radius:12px;padding:22px;">
        <div style="color:#555;font-size:12px;font-weight:bold;margin-bottom:10px;
                    letter-spacing:0.5px;">
            &#x1F3AF; PASS RATE</div>
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
    </div>

    <div class="spacer-sm"></div>

    <!-- Delete & Reappear KPI card -->
    <div style="display:flex;gap:18px;margin:14px 0;">
      <div class="kpi-card" style="flex:1;background:linear-gradient(135deg,
                  {'#fff3e0' if record_count > 0 else '#e8f5e9'} 0%,
                  {'#ffe0b2' if record_count > 0 else '#c8e6c9'} 100%);">
        <div class="kpi-icon">{'&#x26A0;' if record_count > 0 else '&#x2705;'}</div>
        <div class="kpi-label" style="color:{'#e65100' if record_count > 0 else '#2e7d32'};">
            Delete &amp; Reappear</div>
        <div class="kpi-value" style="color:{'#bf360c' if record_count > 0 else '#1b5e20'};">
            {record_count}</div>
        <div style="font-size:11px;color:#888;margin-top:4px;">records detected</div>
      </div>
    </div>

    <div class="spacer"></div>
"""

# ---- Per-Table DQA Results ----
for tbl_idx, (table_name, rows) in enumerate(all_stats_arrays):
    html += f"""
    <h2>&#x1F50D; {tbl_idx + 2}. Data Quality &mdash; {table_name}</h2>
    <div class="spacer-sm"></div>
"""
    n_rows = len(rows)
    is_large = n_rows > 10

    if is_large:
        html += f"""
    <details style="margin-top:10px;">
      <summary style="cursor:pointer;padding:10px 14px;background:linear-gradient(135deg,
                #f8f9ff 0%, #fef9ff 100%);border:1px solid #e8e0f0;border-radius:8px;
                font-size:12px;font-weight:bold;color:#764ba2;list-style:none;
                display:flex;align-items:center;gap:8px;">
        <span style="transition:transform 0.2s;">&#x25B6;</span>
        &#x1F4CB; {n_rows} validation checks &mdash; Click to expand
      </summary>"""

    # Render as individual check cards
    for row_idx, row in enumerate(rows):
        metric = str(row.get("metric", ""))
        source_date = str(row.get("source_file_date", ""))
        count_val = str(row.get("Count", row.get("count", "")))
        rolling_avg = str(row.get("Rolling_10_day_avg", row.get("rolling_10_day_avg", "")))
        status_val = str(row.get("status", ""))
        color = _status_color(status_val)
        icon = _status_icon(status_val)

        html += f"""
    <div class="check-card" style="border-left:5px solid {color};">
      <div class="check-card-header">
        <div>
          <span style="font-size:16px;margin-right:6px;">{icon}</span>
          <span style="font-weight:bold;color:#2c3e50;font-size:13px;">
              {row_idx + 1}. {metric}</span>
        </div>
        <div>
          <span style="color:#888;font-size:11px;margin-right:10px;">
              &#x1F4C5; {source_date}</span>
          <span class="badge" style="background-color:{color};">{status_val}</span>
        </div>
      </div>
      <div class="check-card-body">
        <table style="width:auto;margin:0;">
          <tr>
            <th>Metric</th><th>Source Date</th><th>Count</th>
            <th>Rolling 10-Day Avg</th><th>Status</th>
          </tr>
          <tr>
            <td style="font-weight:bold;">{metric}</td>
            <td>{source_date}</td>
            <td style="font-weight:bold;">{count_val}</td>
            <td>{rolling_avg}</td>
            <td><span class="badge" style="background-color:{color};">{status_val}</span></td>
          </tr>
        </table>
      </div>
    </div>"""

    if is_large:
        html += "\n    </details>"

# ---- Validation Descriptions ----
html += """
    <div class="spacer"></div>
    <h2>&#x1F4D6; Validation Descriptions</h2>
    <div class="spacer-sm"></div>

    <div class="dict-box">
      <div style="font-weight:bold;color:#764ba2;font-size:12px;margin-bottom:14px;
                  letter-spacing:0.5px;">
          &#x1F4D6; MASTER DATA QUALITY CHECKS</div>
"""

for metric_name, desc in validation_descriptions.items():
    html += f"""
      <div class="dict-item">
        <span style="font-weight:bold;color:#4a2d7a;">&#x1F539; {metric_name}</span><br>
        <span style="color:#666;margin-left:20px;">{desc}</span>
      </div>"""

html += """
    </div>

    <div class="dict-box">
      <div style="font-weight:bold;color:#764ba2;font-size:12px;margin-bottom:14px;
                  letter-spacing:0.5px;">
          &#x1F504; DELETE &amp; REAPPEAR</div>
      <div class="dict-item">
        <span style="color:#666;">
            Identifies records that have been deleted from the system and subsequently reappeared,
            indicating potential data processing issues or recovery scenarios.
            See attached CSV for full details.</span>
      </div>
    </div>
"""

# ---- Footer ----
duration = (datetime.now() - start).total_seconds()
html += f"""
    </div><!-- end content -->

    <div class="footer">
        <p>&#x1F4E7; Generated by <strong>Life DQ Automation</strong>
           &nbsp;&bull;&nbsp; &#x23F1; {duration:.1f}s
           &nbsp;&bull;&nbsp; &#x1F4C5; {run_date}</p>
        <p>Thanks,<br><strong>BIG DATA - Life Team</strong></p>
    </div>

</div><!-- end container -->
</body>
</html>
"""

print(html)

# ---- Send Email ----
sender = "life_master_validations"
recipients = []

msg = MIMEMultipart('alternative')
msg['From'] = sender
msg['To'] = ", ".join(recipients)
msg.add_header('Content-Type', 'text/html')
msg['Subject'] = "Data Quality Life Master Validations - Summary"

part2 = MIMEText(html, 'html')
msg.attach(part2)

# Attach CSV for Delete & Reappear
csv_buffer = io.StringIO()
result_to_be_attached.toPandas().to_csv(csv_buffer, index=False)
csv_content = csv_buffer.getvalue()
part = MIMEBase('text', 'csv')
part.set_payload(csv_content)
encoders.encode_base64(part)
part.add_header('Content-Disposition', 'attachment; filename="delete_reappear.csv"')
msg.attach(part)

server = smtplib.SMTP('10.19.10.29', 25)
text = msg.as_string()
server.starttls()
server.sendmail(sender, recipients, text)
server.quit()

print('{0} -- FINISHED'.format(appName.upper()))
spark.stop()
