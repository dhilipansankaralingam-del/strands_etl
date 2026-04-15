"""
Email Reporter - HTML email reports for ETL jobs
Supports SES for sending rich HTML reports with job summaries, data quality, and metrics
"""

import json
import logging
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class EmailRecipient:
    """Email recipient configuration."""
    email: str
    name: Optional[str] = None
    report_types: List[str] = None  # ['daily', 'failure', 'quality']


@dataclass
class EmailReport:
    """Email report to send."""
    subject: str
    html_body: str
    text_body: str
    recipients: List[str]
    sender: str
    cc: Optional[List[str]] = None
    attachments: Optional[List[Dict]] = None


class EmailReporter:
    """
    Email Reporter for sending rich HTML reports.

    Features:
    - Job execution summaries
    - Data quality reports
    - Cost analysis
    - Failure alerts
    - Scheduled digest reports
    """

    # Default email templates
    TEMPLATES = {
        'header': '''
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            padding: 30px;
        }}
        .header {{
            background: linear-gradient(135deg, #1a73e8 0%, #0d47a1 100%);
            color: white;
            padding: 30px;
            border-radius: 8px 8px 0 0;
            margin: -30px -30px 30px -30px;
        }}
        .header h1 {{
            margin: 0;
            font-size: 24px;
        }}
        .header p {{
            margin: 10px 0 0 0;
            opacity: 0.9;
        }}
        .metric-card {{
            display: inline-block;
            background: #f8f9fa;
            padding: 15px 25px;
            border-radius: 8px;
            margin: 10px 10px 10px 0;
            text-align: center;
        }}
        .metric-value {{
            font-size: 32px;
            font-weight: bold;
            color: #1a73e8;
        }}
        .metric-label {{
            font-size: 12px;
            color: #666;
            text-transform: uppercase;
        }}
        .success {{ color: #28a745; }}
        .warning {{ color: #ffc107; }}
        .error {{ color: #dc3545; }}
        .section {{
            margin: 30px 0;
        }}
        .section h2 {{
            color: #1a73e8;
            border-bottom: 2px solid #e0e0e0;
            padding-bottom: 10px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #e0e0e0;
        }}
        th {{
            background-color: #f8f9fa;
            font-weight: 600;
        }}
        tr:hover {{
            background-color: #f8f9fa;
        }}
        .status-badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 500;
        }}
        .status-success {{ background: #d4edda; color: #155724; }}
        .status-failed {{ background: #f8d7da; color: #721c24; }}
        .status-running {{ background: #cce5ff; color: #004085; }}
        .status-warning {{ background: #fff3cd; color: #856404; }}
        .alert {{
            padding: 15px 20px;
            border-radius: 8px;
            margin: 20px 0;
        }}
        .alert-error {{
            background: #f8d7da;
            border-left: 4px solid #dc3545;
        }}
        .alert-warning {{
            background: #fff3cd;
            border-left: 4px solid #ffc107;
        }}
        .alert-info {{
            background: #cce5ff;
            border-left: 4px solid #1a73e8;
        }}
        .footer {{
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #e0e0e0;
            text-align: center;
            color: #666;
            font-size: 12px;
        }}
        .button {{
            display: inline-block;
            background: #1a73e8;
            color: white;
            padding: 12px 24px;
            border-radius: 6px;
            text-decoration: none;
            font-weight: 500;
        }}
        .button:hover {{
            background: #0d47a1;
        }}
    </style>
</head>
<body>
    <div class="container">
''',
        'footer': '''
        <div class="footer">
            <p>Enterprise ETL Framework | Generated at {timestamp}</p>
            <p>
                <a href="{console_url}">AWS Console</a> |
                <a href="{dashboard_url}">Dashboard</a>
            </p>
        </div>
    </div>
</body>
</html>
'''
    }

    def __init__(self, region: str = 'us-east-1',
                 sender_email: str = None,
                 config: Dict[str, Any] = None):
        """
        Initialize Email Reporter.

        Args:
            region: AWS region
            sender_email: Default sender email address
            config: Email configuration from pipeline config
        """
        self.region = region
        self.ses_client = boto3.client('ses', region_name=region)
        self.sender_email = sender_email or os.environ.get('SES_SENDER_EMAIL')
        self.config = config or {}

    def send_email(self, report: EmailReport) -> bool:
        """
        Send an email report via SES.

        Args:
            report: EmailReport to send

        Returns:
            True if successful
        """
        if not self.sender_email and not report.sender:
            logger.error("No sender email configured")
            return False

        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = report.subject
            msg['From'] = report.sender or self.sender_email
            msg['To'] = ', '.join(report.recipients)

            if report.cc:
                msg['Cc'] = ', '.join(report.cc)

            # Attach text and HTML parts
            text_part = MIMEText(report.text_body, 'plain')
            html_part = MIMEText(report.html_body, 'html')
            msg.attach(text_part)
            msg.attach(html_part)

            # Send via SES
            response = self.ses_client.send_raw_email(
                Source=msg['From'],
                Destinations=report.recipients + (report.cc or []),
                RawMessage={'Data': msg.as_string()}
            )

            logger.info(f"Email sent: {response['MessageId']}")
            return True

        except ClientError as e:
            logger.error(f"Failed to send email: {e}")
            return False

    def generate_job_summary_report(self, jobs: List[Dict[str, Any]],
                                     period: str = 'daily') -> str:
        """
        Generate HTML report for job execution summary.

        Args:
            jobs: List of job run details
            period: Report period ('daily', 'weekly', 'monthly')

        Returns:
            HTML report string
        """
        # Calculate metrics
        total_jobs = len(jobs)
        successful = sum(1 for j in jobs if j.get('status') in ['SUCCEEDED', 'COMPLETED'])
        failed = sum(1 for j in jobs if j.get('status') == 'FAILED')
        running = sum(1 for j in jobs if j.get('status') in ['RUNNING', 'STARTING'])
        success_rate = (successful / total_jobs * 100) if total_jobs > 0 else 0
        total_cost = sum(j.get('cost', 0) for j in jobs)

        # Build HTML
        html = self.TEMPLATES['header']

        # Header
        html += f'''
        <div class="header">
            <h1>ETL Job Execution Report</h1>
            <p>{period.title()} Summary - {datetime.now().strftime('%B %d, %Y')}</p>
        </div>
'''

        # Metrics cards
        html += '''
        <div class="section">
            <div class="metric-card">
                <div class="metric-value">{}</div>
                <div class="metric-label">Total Jobs</div>
            </div>
            <div class="metric-card">
                <div class="metric-value success">{}</div>
                <div class="metric-label">Successful</div>
            </div>
            <div class="metric-card">
                <div class="metric-value error">{}</div>
                <div class="metric-label">Failed</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{:.1f}%</div>
                <div class="metric-label">Success Rate</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">${:.2f}</div>
                <div class="metric-label">Total Cost</div>
            </div>
        </div>
'''.format(total_jobs, successful, failed, success_rate, total_cost)

        # Failed jobs alert
        if failed > 0:
            failed_jobs = [j for j in jobs if j.get('status') == 'FAILED']
            html += '''
        <div class="alert alert-error">
            <strong>Attention Required:</strong> {} job(s) failed in the last {} period.
            <ul>
'''.format(failed, period)
            for job in failed_jobs[:5]:
                html += f"                <li>{job.get('name', 'Unknown')} - {job.get('error', 'Unknown error')[:100]}</li>\n"
            html += '''
            </ul>
        </div>
'''

        # Job details table
        html += '''
        <div class="section">
            <h2>Job Execution Details</h2>
            <table>
                <tr>
                    <th>Job Name</th>
                    <th>Status</th>
                    <th>Duration</th>
                    <th>Cost</th>
                    <th>Started</th>
                </tr>
'''
        for job in jobs[:20]:  # Limit to 20 jobs
            status = job.get('status', 'Unknown')
            status_class = 'status-success' if status in ['SUCCEEDED', 'COMPLETED'] else (
                'status-failed' if status == 'FAILED' else 'status-running'
            )
            html += f'''
                <tr>
                    <td>{job.get('name', 'Unknown')}</td>
                    <td><span class="status-badge {status_class}">{status}</span></td>
                    <td>{job.get('duration', 'N/A')}</td>
                    <td>${job.get('cost', 0):.2f}</td>
                    <td>{job.get('started', 'N/A')}</td>
                </tr>
'''

        html += '''
            </table>
        </div>
'''

        # Footer
        html += self.TEMPLATES['footer'].format(
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
            console_url='https://console.aws.amazon.com/glue',
            dashboard_url='#'
        )

        return html

    def generate_data_quality_report(self, dq_report: Dict[str, Any]) -> str:
        """
        Generate HTML report for data quality results.

        Args:
            dq_report: Data quality report dict

        Returns:
            HTML report string
        """
        score = dq_report.get('overall_score', 0)
        passed = dq_report.get('passed_rules', 0)
        failed = dq_report.get('failed_rules', 0)
        total = dq_report.get('total_rules', 0)

        # Score color
        if score >= 0.9:
            score_class = 'success'
        elif score >= 0.7:
            score_class = 'warning'
        else:
            score_class = 'error'

        html = self.TEMPLATES['header']

        # Header
        html += f'''
        <div class="header">
            <h1>Data Quality Report</h1>
            <p>Table: {dq_report.get('table_name', 'N/A')} | {datetime.now().strftime('%B %d, %Y %H:%M')}</p>
        </div>
'''

        # Score display
        html += f'''
        <div class="section" style="text-align: center;">
            <div class="metric-card" style="min-width: 200px;">
                <div class="metric-value {score_class}">{score:.0%}</div>
                <div class="metric-label">Overall Quality Score</div>
            </div>
        </div>

        <div class="section">
            <div class="metric-card">
                <div class="metric-value">{total}</div>
                <div class="metric-label">Total Rules</div>
            </div>
            <div class="metric-card">
                <div class="metric-value success">{passed}</div>
                <div class="metric-label">Passed</div>
            </div>
            <div class="metric-card">
                <div class="metric-value error">{failed}</div>
                <div class="metric-label">Failed</div>
            </div>
        </div>
'''

        # Rules table
        html += '''
        <div class="section">
            <h2>Rule Results</h2>
            <table>
                <tr>
                    <th>Rule Name</th>
                    <th>Type</th>
                    <th>Status</th>
                    <th>Pass Rate</th>
                    <th>Failing Records</th>
                </tr>
'''
        for result in dq_report.get('results', []):
            rule = result.get('rule', {})
            status = 'PASSED' if result.get('passed') else 'FAILED'
            status_class = 'status-success' if result.get('passed') else 'status-failed'

            html += f'''
                <tr>
                    <td>{rule.get('name', 'Unknown')}</td>
                    <td>{rule.get('rule_type', 'N/A')}</td>
                    <td><span class="status-badge {status_class}">{status}</span></td>
                    <td>{result.get('pass_rate', 0):.1%}</td>
                    <td>{result.get('failing_records', 0):,}</td>
                </tr>
'''

        html += '''
            </table>
        </div>
'''

        # Recommendations
        recommendations = dq_report.get('recommendations', [])
        if recommendations:
            html += '''
        <div class="section">
            <h2>Recommendations</h2>
            <div class="alert alert-info">
                <ul>
'''
            for rec in recommendations:
                html += f"                    <li>{rec}</li>\n"
            html += '''
                </ul>
            </div>
        </div>
'''

        html += self.TEMPLATES['footer'].format(
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
            console_url='https://console.aws.amazon.com/athena',
            dashboard_url='#'
        )

        return html

    def generate_failure_alert_email(self, job_name: str, error: str,
                                      context: Dict[str, Any] = None) -> str:
        """
        Generate HTML email for job failure alert.

        Args:
            job_name: Failed job name
            error: Error message
            context: Additional context

        Returns:
            HTML email string
        """
        context = context or {}

        html = self.TEMPLATES['header']

        # Header with error styling
        html += f'''
        <div class="header" style="background: linear-gradient(135deg, #dc3545 0%, #a71d2a 100%);">
            <h1>ETL Job Failed</h1>
            <p>{job_name} | {datetime.now().strftime('%B %d, %Y %H:%M:%S')}</p>
        </div>

        <div class="alert alert-error">
            <strong>Error Details:</strong>
            <pre style="white-space: pre-wrap; font-family: monospace; margin-top: 10px;">{error[:2000]}</pre>
        </div>
'''

        # Context details
        if context:
            html += '''
        <div class="section">
            <h2>Execution Context</h2>
            <table>
'''
            for key, value in context.items():
                if key not in ['suggestions', 'stack_trace']:
                    html += f'''
                <tr>
                    <td style="font-weight: 600;">{key}</td>
                    <td>{value}</td>
                </tr>
'''
            html += '''
            </table>
        </div>
'''

        # Suggestions
        suggestions = context.get('suggestions', [])
        if suggestions:
            html += '''
        <div class="section">
            <h2>Suggested Actions</h2>
            <ul>
'''
            for suggestion in suggestions:
                html += f"                <li>{suggestion}</li>\n"
            html += '''
            </ul>
        </div>
'''

        # Action buttons
        html += f'''
        <div class="section" style="text-align: center;">
            <a href="https://console.aws.amazon.com/cloudwatch" class="button">View Logs</a>
            <a href="https://console.aws.amazon.com/glue" class="button" style="margin-left: 10px;">AWS Glue Console</a>
        </div>
'''

        html += self.TEMPLATES['footer'].format(
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
            console_url='https://console.aws.amazon.com/glue',
            dashboard_url='#'
        )

        return html

    def generate_cost_report(self, cost_data: Dict[str, Any],
                              period: str = 'monthly') -> str:
        """
        Generate HTML cost analysis report.

        Args:
            cost_data: Cost data by job/service
            period: Report period

        Returns:
            HTML report string
        """
        total_cost = cost_data.get('total', 0)
        by_job = cost_data.get('by_job', {})
        by_service = cost_data.get('by_service', {})
        trend = cost_data.get('trend', 0)  # Percentage change

        trend_class = 'error' if trend > 10 else ('warning' if trend > 0 else 'success')
        trend_arrow = '↑' if trend > 0 else '↓'

        html = self.TEMPLATES['header']

        html += f'''
        <div class="header">
            <h1>ETL Cost Analysis Report</h1>
            <p>{period.title()} Summary - {datetime.now().strftime('%B %Y')}</p>
        </div>

        <div class="section" style="text-align: center;">
            <div class="metric-card" style="min-width: 200px;">
                <div class="metric-value">${total_cost:,.2f}</div>
                <div class="metric-label">Total Cost</div>
            </div>
            <div class="metric-card">
                <div class="metric-value {trend_class}">{trend_arrow} {abs(trend):.1f}%</div>
                <div class="metric-label">vs Last Period</div>
            </div>
        </div>

        <div class="section">
            <h2>Cost by Job</h2>
            <table>
                <tr>
                    <th>Job Name</th>
                    <th>Runs</th>
                    <th>Total Cost</th>
                    <th>Avg Cost/Run</th>
                </tr>
'''
        for job_name, data in sorted(by_job.items(), key=lambda x: x[1].get('total', 0), reverse=True)[:10]:
            html += f'''
                <tr>
                    <td>{job_name}</td>
                    <td>{data.get('runs', 0)}</td>
                    <td>${data.get('total', 0):,.2f}</td>
                    <td>${data.get('avg', 0):,.2f}</td>
                </tr>
'''

        html += '''
            </table>
        </div>

        <div class="section">
            <h2>Cost by Service</h2>
            <table>
                <tr>
                    <th>Service</th>
                    <th>Cost</th>
                    <th>% of Total</th>
                </tr>
'''
        for service, cost in sorted(by_service.items(), key=lambda x: x[1], reverse=True):
            pct = (cost / total_cost * 100) if total_cost > 0 else 0
            html += f'''
                <tr>
                    <td>{service}</td>
                    <td>${cost:,.2f}</td>
                    <td>{pct:.1f}%</td>
                </tr>
'''

        html += '''
            </table>
        </div>
'''

        html += self.TEMPLATES['footer'].format(
            timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
            console_url='https://console.aws.amazon.com/cost-management',
            dashboard_url='#'
        )

        return html

    def send_job_summary(self, jobs: List[Dict[str, Any]],
                          recipients: List[str],
                          period: str = 'daily') -> bool:
        """
        Generate and send job summary report.

        Args:
            jobs: List of job execution data
            recipients: Email recipients
            period: Report period

        Returns:
            True if successful
        """
        html = self.generate_job_summary_report(jobs, period)

        # Generate plain text version
        successful = sum(1 for j in jobs if j.get('status') in ['SUCCEEDED', 'COMPLETED'])
        failed = sum(1 for j in jobs if j.get('status') == 'FAILED')

        text = f"""
ETL Job Execution Report - {period.title()} Summary

Total Jobs: {len(jobs)}
Successful: {successful}
Failed: {failed}
Success Rate: {(successful / len(jobs) * 100) if jobs else 0:.1f}%

View the full report in your email client with HTML support.
"""

        report = EmailReport(
            subject=f"ETL {period.title()} Report - {datetime.now().strftime('%Y-%m-%d')}",
            html_body=html,
            text_body=text,
            recipients=recipients,
            sender=self.sender_email
        )

        return self.send_email(report)

    def send_data_quality_report(self, dq_report: Dict[str, Any],
                                   recipients: List[str]) -> bool:
        """
        Generate and send data quality report.

        Args:
            dq_report: Data quality report dict
            recipients: Email recipients

        Returns:
            True if successful
        """
        html = self.generate_data_quality_report(dq_report)

        score = dq_report.get('overall_score', 0)
        text = f"""
Data Quality Report

Table: {dq_report.get('table_name', 'N/A')}
Overall Score: {score:.0%}
Passed Rules: {dq_report.get('passed_rules', 0)}
Failed Rules: {dq_report.get('failed_rules', 0)}

View the full report in your email client with HTML support.
"""

        # Subject based on score
        if score >= 0.9:
            status = "PASSED"
        elif score >= 0.7:
            status = "WARNING"
        else:
            status = "FAILED"

        report = EmailReport(
            subject=f"Data Quality {status}: {dq_report.get('table_name', 'Unknown')} ({score:.0%})",
            html_body=html,
            text_body=text,
            recipients=recipients,
            sender=self.sender_email
        )

        return self.send_email(report)

    def send_failure_alert(self, job_name: str, error: str,
                            recipients: List[str],
                            context: Dict[str, Any] = None) -> bool:
        """
        Send job failure alert email.

        Args:
            job_name: Failed job name
            error: Error message
            recipients: Email recipients
            context: Additional context

        Returns:
            True if successful
        """
        html = self.generate_failure_alert_email(job_name, error, context)

        text = f"""
ETL JOB FAILED: {job_name}

Error: {error[:500]}

Please check the AWS Console for more details.
"""

        report = EmailReport(
            subject=f"[ALERT] ETL Job Failed: {job_name}",
            html_body=html,
            text_body=text,
            recipients=recipients,
            sender=self.sender_email
        )

        return self.send_email(report)


# Example usage
if __name__ == '__main__':
    reporter = EmailReporter()

    # Sample job data
    sample_jobs = [
        {'name': 'customer_360_etl', 'status': 'SUCCEEDED', 'duration': '15m', 'cost': 2.50, 'started': '2024-01-15 08:00'},
        {'name': 'orders_sync', 'status': 'SUCCEEDED', 'duration': '8m', 'cost': 1.20, 'started': '2024-01-15 08:30'},
        {'name': 'inventory_update', 'status': 'FAILED', 'duration': '3m', 'cost': 0.50, 'started': '2024-01-15 09:00', 'error': 'Out of memory'},
        {'name': 'reports_daily', 'status': 'SUCCEEDED', 'duration': '22m', 'cost': 3.80, 'started': '2024-01-15 10:00'},
    ]

    # Generate sample report
    html = reporter.generate_job_summary_report(sample_jobs, 'daily')
    print("Generated job summary report")
    print(f"HTML length: {len(html)} characters")

    # Save for preview
    with open('/tmp/sample_report.html', 'w') as f:
        f.write(html)
    print("Saved to /tmp/sample_report.html")
