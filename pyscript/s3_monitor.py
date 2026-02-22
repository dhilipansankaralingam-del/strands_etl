#####################################################################################################
# SCRIPT        : s3-folder-count-monitor
# PURPOSE       : Monitor S3 buckets/subfolders for folder counts > 8 and send email alerts
# INPUT         : List of S3 buckets/subfolders
# OUTPUT        : HTML email with alerts for folders exceeding count threshold
#
# PROCESS       :
#    1) Iterate through list of buckets/subfolders
#    2) Count subfolders in each path
#    3) If count > 8, add to HTML table
#    4) Send email with results
#
#===================================================================================================
# DATE        BY          MODIFICATION LOG
# ----------  ----------- --------------------------------------
# 10/28/2025  Dhilipan          Initial version
#
#===================================================================================================
##################################################################################################

import boto3
import json
import sys
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# List of buckets/subfolders to monitor
# Format: [{'bucket': 'bucket-name', 'prefix': 'subfolder/', 'stale_hours_threshold': 8}]
buckets_to_monitor = [
    #{'bucket': 'cust-landing-us-west-2-xxxx', 'prefix': 'm_customer/', 'stale_hours_threshold': 8},
    {'bucket': 'cust-landing-us-west-2-xxxx', 'prefix': 'landing/MACP.Mac_ADDRESS_TB__ct/', 'stale_hours_threshold': 4},
    {'bucket': 'cust-landing-us-west-2-xxxx', 'prefix': 'landing/MACP.Mac_CUST_ADDRESS__ct/', 'stale_hours_threshold': 4},
    {'bucket': 'cust-landing-us-west-2-xxxx', 'prefix': 'landing/MACP.Mac_CUSTOMER__ct/', 'stale_hours_threshold': 4},
    {'bucket': 'cust-landing-us-west-2-xxxx', 'prefix': 'landing/MACP.Mac_CUST_CMMCT_PRFNC_CHNNL__ct/', 'stale_hours_threshold': 4},
    {'bucket': 'cust-landing-us-west-2-xxxx', 'prefix': 'landing/MRDP.MRD_MBR_INFO__ct/', 'stale_hours_threshold': 4},
    {'bucket': 'cust-landing-us-west-2-xxxx', 'prefix': 'landing/MRDP.MRD_MBR_PRD_DTL__ct/', 'stale_hours_threshold': 4}

]

def count_subfolders(bucket, prefix):
    """
    Count the number of subfolders (common prefixes) under the given prefix.
    """
    s3_client = boto3.client('s3')
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
        subfolders = response.get('CommonPrefixes', [])
        return len(subfolders)
    except Exception as e:
        print(f"Error counting subfolders in {bucket}/{prefix}: {str(e)}")
        return 0

def check_stale_files(bucket, prefix, stale_hours_threshold):
    """
    Check if the latest file in the folder is older than the stale threshold.
    Returns (is_stale, latest_file_time, hours_since_last_file)
    """
    s3_client = boto3.client('s3')
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' not in response:
            return True, None, None  # No files found, consider stale

        # Find the most recent file
        latest_file = max(response['Contents'], key=lambda x: x['LastModified'])
        latest_time = latest_file['LastModified']
        current_time = datetime.utcnow().replace(tzinfo=latest_time.tzinfo)
        hours_since = (current_time - latest_time).total_seconds() / 3600

        is_stale = hours_since > stale_hours_threshold
        return is_stale, latest_time, hours_since
    except Exception as e:
        print(f"Error checking stale files in {bucket}/{prefix}: {str(e)}")
        return True, None, None  # Consider stale on error

def generate_html_table(folder_alerts, stale_alerts):
    """
    Generate HTML table for both folder count and stale file alerts.
    """
    html = ""

    # Folder Count Alerts
    if folder_alerts:
        html += "<h3>Folder Count Alerts (Count > 8)</h3>"
        html += """
        <table border="1" style="border-collapse: collapse; width: 100%;">
            <thead>
                <tr bgcolor='purple' style="color: white;">
                    <th>Bucket</th>
                    <th>Subfolder Path</th>
                    <th>Subfolder Count</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        """

        for alert in folder_alerts:
            html += f"""
            <tr>
                <td>{alert['bucket']}</td>
                <td>{alert['prefix']}</td>
                <td>{alert['count']}</td>
                <td style="color: red; font-weight: bold;">ALERT: Count > 8</td>
            </tr>
            """

        html += """
            </tbody>
        </table>
        <br>
        """

    # Stale File Alerts
    if stale_alerts:
        html += "<h3>Stale File Alerts (No files received in 8+ hours)</h3>"
        html += """
        <table border="1" style="border-collapse: collapse; width: 100%;">
            <thead>
                <tr bgcolor='orange' style="color: white;">
                    <th>Bucket</th>
                    <th>Subfolder Path</th>
                    <th>Last File Time</th>
                    <th>Hours Since Last File</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
        """

        for alert in stale_alerts:
            html += f"""
            <tr>
                <td>{alert['bucket']}</td>
                <td>{alert['prefix']}</td>
                <td>{alert['last_time'].strftime('%Y-%m-%d %H:%M:%S') if alert['last_time'] else 'N/A'}</td>
                <td>{alert['hours_since']:.1f} hours</td>
                <td style="color: red; font-weight: bold;">ALERT: Stale Files</td>
            </tr>
            """

        html += """
            </tbody>
        </table>
        <br>
        """

    if not folder_alerts and not stale_alerts:
        html = "<p>No alerts found for folder counts or stale files.</p>"

    return html

def send_email(html_content):
    """
    Send HTML email with the alerts.
    """
    sender = "gid02061@aaa-calif.com"
    recipients = ["Ganji.Srinivasu@ace.aaa.com", "Ahmad.Imran@ace.aaa.com", "Suresh.Namitha@ace.aaa.com", "Sankaralingam.DhilipanSomasundaram@ace.aaa.com","ramanan.swaminathan@ace.aaa.com"]
    #recipients = ["Sankaralingam.DhilipanSomasundaram@ace.aaa.com"]

    msg = MIMEMultipart('alternative')
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = "S3 Monitor Alert - Folder Counts & Stale Files"

    html = f"""
    <html>
        <head>
            <style>
                table {{
                    border-collapse: collapse;
                    width: 100%;
                }}
                th, td {{
                    border: 1px solid black;
                    padding: 8px;
                    text-align: left;
                }}
                th {{
                    background-color: purple;
                    color: white;
                }}
                h3 {{
                    color: #333;
                    margin-top: 20px;
                }}
            </style>
        </head>
        <body>
            <p>Hello,</p>
            <p>This email contains alerts for S3 buckets/subfolders monitoring:</p>
            <ul>
                <li>Subfolder counts exceeding 8</li>
                <li>Files not received for 8+ hours (stale files)</li>
            </ul>
            <p>Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            {html_content}
            <p>Thanks,<br>BIG DATA Team</p>
        </body>
    </html>
    """

    part = MIMEText(html, 'html')
    msg.attach(part)

    try:
        server = smtplib.SMTP('10.19.10.29', 25)
        server.starttls()
        text = msg.as_string()
        server.sendmail(sender, recipients, text)
        server.quit()
        print("Email sent successfully.")
    except Exception as e:
        print(f"Error sending email: {str(e)}")

def main():
    print("Starting S3 Folder Count & Stale File Monitor...")
    folder_alerts = []
    stale_alerts = []

    for item in buckets_to_monitor:
        bucket = item['bucket']
        prefix = item['prefix']
        stale_threshold = item.get('stale_hours_threshold', 8)

        # Check folder count
        count = count_subfolders(bucket, prefix)
        print(f"Bucket: {bucket}, Prefix: {prefix}, Subfolder Count: {count}")

        if count > 6:
            folder_alerts.append({
                'bucket': bucket,
                'prefix': prefix,
                'count': count
            })

        # Check for stale files
        is_stale, last_time, hours_since = check_stale_files(bucket, prefix, stale_threshold)
        print(f"Bucket: {bucket}, Prefix: {prefix}, Last File: {last_time}, Hours Since: {hours_since:.1f}")

        if is_stale:
            stale_alerts.append({
                'bucket': bucket,
                'prefix': prefix,
                'last_time': last_time,
                'hours_since': hours_since
            })

    if folder_alerts or stale_alerts:
        html_table = generate_html_table(folder_alerts, stale_alerts)
        send_email(html_table)
        print(f"Found {len(folder_alerts)} folder alerts and {len(stale_alerts)} stale file alerts. Email sent.")
    else:
        print("No alerts found. No email sent.")

    print("S3 Folder Count & Stale File Monitor completed.")

if __name__ == "__main__":
    main()