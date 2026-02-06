#!/usr/bin/env python3
"""
Email Integration (HTML Notifications)
======================================

Enterprise email integration for ETL framework:
1. HTML formatted email notifications
2. Rich job reports with metrics
3. Data quality reports
4. Compliance reports
5. Daily/weekly summary digests
6. Error alerts with stack traces

Uses AWS SES for email delivery.

Requires:
- AWS SES configured with verified sender
- EMAIL_SENDER: Verified sender email
- EMAIL_RECIPIENTS: Comma-separated list of recipients
"""

import os
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


@dataclass
class EmailConfig:
    """Email configuration."""
    sender: str
    recipients: List[str]
    region: str = "us-east-1"


class EmailIntegration:
    """
    Email integration for HTML notifications.
    """

    def __init__(self, config):
        self.config = config
        self.sender = getattr(config, 'email_sender', os.getenv('EMAIL_SENDER'))
        recipients_str = getattr(config, 'email_recipients', os.getenv('EMAIL_RECIPIENTS', ''))
        self.recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]
        self.region = getattr(config, 'aws_region', os.getenv('AWS_REGION', 'us-east-1'))

        # Try to import boto3
        try:
            import boto3
            self.ses_client = boto3.client('ses', region_name=self.region)
        except ImportError:
            self.ses_client = None

    def send_job_report(
        self,
        job_name: str,
        status: str,
        metrics: Dict[str, Any],
        recommendations: List[str] = None,
        to: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Send a job completion report.

        Args:
            job_name: Name of the job
            status: Job status (success/failed)
            metrics: Job metrics dictionary
            recommendations: List of recommendations
            to: Optional specific recipients

        Returns:
            Send result
        """
        subject = f"{'✅' if status == 'success' else '❌'} ETL Job Report: {job_name}"

        html_content = self._build_job_report_html(job_name, status, metrics, recommendations)
        text_content = self._build_job_report_text(job_name, status, metrics, recommendations)

        return self._send_email(subject, html_content, text_content, to)

    def send_dq_report(
        self,
        job_name: str,
        dq_results: Dict[str, Any],
        to: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Send a data quality report.

        Args:
            job_name: Name of the job
            dq_results: Data quality results
            to: Optional specific recipients

        Returns:
            Send result
        """
        overall_status = dq_results.get('overall_status', 'unknown')
        subject = f"📊 Data Quality Report: {job_name} - {overall_status.upper()}"

        html_content = self._build_dq_report_html(job_name, dq_results)
        text_content = self._build_dq_report_text(job_name, dq_results)

        return self._send_email(subject, html_content, text_content, to)

    def send_compliance_report(
        self,
        job_name: str,
        compliance_results: Dict[str, Any],
        to: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Send a compliance report.

        Args:
            job_name: Name of the job
            compliance_results: Compliance check results
            to: Optional specific recipients

        Returns:
            Send result
        """
        status = compliance_results.get('status', 'unknown')
        subject = f"🔒 Compliance Report: {job_name} - {status.upper()}"

        html_content = self._build_compliance_report_html(job_name, compliance_results)
        text_content = self._build_compliance_report_text(job_name, compliance_results)

        return self._send_email(subject, html_content, text_content, to)

    def send_daily_summary(
        self,
        summary: Dict[str, Any],
        to: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Send a daily summary report.

        Args:
            summary: Summary metrics
            to: Optional specific recipients

        Returns:
            Send result
        """
        date_str = datetime.now().strftime('%Y-%m-%d')
        subject = f"📈 Daily ETL Summary - {date_str}"

        html_content = self._build_summary_html(summary, "Daily")
        text_content = self._build_summary_text(summary, "Daily")

        return self._send_email(subject, html_content, text_content, to)

    def send_error_alert(
        self,
        job_name: str,
        error: str,
        stack_trace: Optional[str] = None,
        to: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Send an error alert.

        Args:
            job_name: Name of the job
            error: Error message
            stack_trace: Optional stack trace
            to: Optional specific recipients

        Returns:
            Send result
        """
        subject = f"🚨 ETL Error Alert: {job_name}"

        html_content = self._build_error_alert_html(job_name, error, stack_trace)
        text_content = self._build_error_alert_text(job_name, error, stack_trace)

        return self._send_email(subject, html_content, text_content, to)

    def _send_email(
        self,
        subject: str,
        html_content: str,
        text_content: str,
        to: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Send an email using SES."""
        recipients = to or self.recipients

        if not self.sender or not recipients:
            return {"success": False, "error": "Missing sender or recipients"}

        if not self.ses_client:
            return {"success": False, "error": "SES client not available (boto3 not installed)"}

        try:
            response = self.ses_client.send_email(
                Source=self.sender,
                Destination={'ToAddresses': recipients},
                Message={
                    'Subject': {'Data': subject, 'Charset': 'UTF-8'},
                    'Body': {
                        'Text': {'Data': text_content, 'Charset': 'UTF-8'},
                        'Html': {'Data': html_content, 'Charset': 'UTF-8'}
                    }
                }
            )
            return {"success": True, "message_id": response.get('MessageId')}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _build_job_report_html(
        self,
        job_name: str,
        status: str,
        metrics: Dict[str, Any],
        recommendations: List[str] = None
    ) -> str:
        """Build HTML content for job report."""
        status_color = "#28a745" if status == "success" else "#dc3545"
        status_text = "COMPLETED SUCCESSFULLY" if status == "success" else "FAILED"

        duration = metrics.get('duration_seconds', 0)
        duration_str = f"{duration // 60}m {duration % 60}s" if duration else "N/A"

        metrics_rows = ""
        for key, value in metrics.items():
            if key != 'duration_seconds':
                metrics_rows += f"""
                <tr>
                    <td style="padding: 10px; border-bottom: 1px solid #eee;"><strong>{key.replace('_', ' ').title()}</strong></td>
                    <td style="padding: 10px; border-bottom: 1px solid #eee;">{value}</td>
                </tr>
                """

        recommendations_html = ""
        if recommendations:
            recommendations_html = """
            <h3 style="color: #333; margin-top: 30px;">💡 Recommendations</h3>
            <ul style="color: #666;">
            """
            for rec in recommendations:
                recommendations_html += f"<li style='margin: 5px 0;'>{rec}</li>"
            recommendations_html += "</ul>"

        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
                .container {{ max-width: 600px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                .header {{ background: {status_color}; color: white; padding: 20px; text-align: center; }}
                .content {{ padding: 20px; }}
                .metric-table {{ width: 100%; border-collapse: collapse; }}
                .footer {{ background: #f8f9fa; padding: 15px; text-align: center; color: #666; font-size: 12px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1 style="margin: 0;">{'✅' if status == 'success' else '❌'} ETL Job Report</h1>
                    <p style="margin: 10px 0 0 0; opacity: 0.9;">{job_name}</p>
                </div>
                <div class="content">
                    <div style="text-align: center; padding: 20px; background: #f8f9fa; border-radius: 8px; margin-bottom: 20px;">
                        <h2 style="color: {status_color}; margin: 0;">{status_text}</h2>
                        <p style="color: #666; margin: 10px 0 0 0;">Duration: {duration_str}</p>
                    </div>

                    <h3 style="color: #333;">📊 Metrics</h3>
                    <table class="metric-table">
                        {metrics_rows}
                    </table>

                    {recommendations_html}
                </div>
                <div class="footer">
                    Generated by ETL Framework | {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}
                </div>
            </div>
        </body>
        </html>
        """

    def _build_job_report_text(
        self,
        job_name: str,
        status: str,
        metrics: Dict[str, Any],
        recommendations: List[str] = None
    ) -> str:
        """Build plain text content for job report."""
        lines = [
            f"ETL Job Report: {job_name}",
            "=" * 50,
            f"Status: {status.upper()}",
            "",
            "Metrics:",
        ]

        for key, value in metrics.items():
            lines.append(f"  - {key.replace('_', ' ').title()}: {value}")

        if recommendations:
            lines.append("")
            lines.append("Recommendations:")
            for rec in recommendations:
                lines.append(f"  - {rec}")

        lines.append("")
        lines.append(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")

        return "\n".join(lines)

    def _build_dq_report_html(self, job_name: str, dq_results: Dict[str, Any]) -> str:
        """Build HTML content for DQ report."""
        overall_status = dq_results.get('overall_status', 'unknown')
        status_color = "#28a745" if overall_status == "passed" else "#dc3545" if overall_status == "failed" else "#ffc107"

        total_rules = dq_results.get('total_rules', 0)
        passed_rules = dq_results.get('passed_rules', 0)
        failed_rules = dq_results.get('failed_rules', 0)

        results_html = ""
        for result in dq_results.get('results', [])[:20]:
            status_icon = "✅" if result.get('status') == 'passed' else "❌"
            results_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #eee;">{status_icon}</td>
                <td style="padding: 8px; border-bottom: 1px solid #eee;">{result.get('rule_id', 'N/A')}</td>
                <td style="padding: 8px; border-bottom: 1px solid #eee;">{result.get('rule_description', 'N/A')[:50]}</td>
                <td style="padding: 8px; border-bottom: 1px solid #eee;">{result.get('pass_rate', 0)*100:.1f}%</td>
            </tr>
            """

        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
                .container {{ max-width: 700px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                .header {{ background: {status_color}; color: white; padding: 20px; text-align: center; }}
                .content {{ padding: 20px; }}
                .summary {{ display: flex; justify-content: space-around; text-align: center; margin: 20px 0; }}
                .summary-item {{ padding: 15px; background: #f8f9fa; border-radius: 8px; flex: 1; margin: 0 5px; }}
                table {{ width: 100%; border-collapse: collapse; }}
                th {{ background: #343a40; color: white; padding: 10px; text-align: left; }}
                .footer {{ background: #f8f9fa; padding: 15px; text-align: center; color: #666; font-size: 12px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1 style="margin: 0;">📊 Data Quality Report</h1>
                    <p style="margin: 10px 0 0 0; opacity: 0.9;">{job_name}</p>
                </div>
                <div class="content">
                    <div class="summary">
                        <div class="summary-item">
                            <h3 style="margin: 0; color: #333;">{total_rules}</h3>
                            <p style="margin: 5px 0 0 0; color: #666;">Total Rules</p>
                        </div>
                        <div class="summary-item">
                            <h3 style="margin: 0; color: #28a745;">{passed_rules}</h3>
                            <p style="margin: 5px 0 0 0; color: #666;">Passed</p>
                        </div>
                        <div class="summary-item">
                            <h3 style="margin: 0; color: #dc3545;">{failed_rules}</h3>
                            <p style="margin: 5px 0 0 0; color: #666;">Failed</p>
                        </div>
                    </div>

                    <h3 style="color: #333;">Rule Results</h3>
                    <table>
                        <tr>
                            <th>Status</th>
                            <th>Rule ID</th>
                            <th>Description</th>
                            <th>Pass Rate</th>
                        </tr>
                        {results_html}
                    </table>
                </div>
                <div class="footer">
                    Generated by ETL Framework | {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}
                </div>
            </div>
        </body>
        </html>
        """

    def _build_dq_report_text(self, job_name: str, dq_results: Dict[str, Any]) -> str:
        """Build plain text content for DQ report."""
        lines = [
            f"Data Quality Report: {job_name}",
            "=" * 50,
            f"Overall Status: {dq_results.get('overall_status', 'unknown').upper()}",
            f"Total Rules: {dq_results.get('total_rules', 0)}",
            f"Passed: {dq_results.get('passed_rules', 0)}",
            f"Failed: {dq_results.get('failed_rules', 0)}",
            "",
            "Rule Results:",
        ]

        for result in dq_results.get('results', []):
            status = "PASS" if result.get('status') == 'passed' else "FAIL"
            lines.append(f"  [{status}] {result.get('rule_id', 'N/A')}: {result.get('rule_description', 'N/A')}")

        return "\n".join(lines)

    def _build_compliance_report_html(self, job_name: str, compliance_results: Dict[str, Any]) -> str:
        """Build HTML content for compliance report."""
        status = compliance_results.get('status', 'unknown')
        status_color = "#28a745" if status == "compliant" else "#dc3545" if status == "non_compliant" else "#ffc107"

        pii_findings = compliance_results.get('pii_findings', [])
        violations = compliance_results.get('violations', [])

        pii_html = ""
        for finding in pii_findings[:10]:
            pii_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #eee;">{finding.get('column_name', 'N/A')}</td>
                <td style="padding: 8px; border-bottom: 1px solid #eee;">{finding.get('pii_type', 'N/A')}</td>
                <td style="padding: 8px; border-bottom: 1px solid #eee;">{finding.get('confidence', 0)*100:.0f}%</td>
            </tr>
            """

        violations_html = ""
        for v in violations[:10]:
            severity_color = "#dc3545" if v.get('severity') == 'critical' else "#ffc107" if v.get('severity') == 'high' else "#17a2b8"
            violations_html += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #eee;">{v.get('rule_id', 'N/A')}</td>
                <td style="padding: 8px; border-bottom: 1px solid #eee;">{v.get('rule_description', 'N/A')[:50]}</td>
                <td style="padding: 8px; border-bottom: 1px solid #eee; color: {severity_color};">{v.get('severity', 'N/A').upper()}</td>
            </tr>
            """

        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }}
                .container {{ max-width: 700px; margin: 0 auto; background: white; border-radius: 8px; overflow: hidden; }}
                .header {{ background: {status_color}; color: white; padding: 20px; text-align: center; }}
                .content {{ padding: 20px; }}
                table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
                th {{ background: #343a40; color: white; padding: 10px; text-align: left; }}
                .footer {{ background: #f8f9fa; padding: 15px; text-align: center; color: #666; font-size: 12px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1 style="margin: 0;">🔒 Compliance Report</h1>
                    <p style="margin: 10px 0 0 0; opacity: 0.9;">{job_name} - {status.upper()}</p>
                </div>
                <div class="content">
                    <h3>PII Detection ({len(pii_findings)} findings)</h3>
                    <table>
                        <tr><th>Column</th><th>PII Type</th><th>Confidence</th></tr>
                        {pii_html if pii_html else '<tr><td colspan="3" style="padding: 10px; text-align: center;">No PII detected</td></tr>'}
                    </table>

                    <h3 style="margin-top: 30px;">Compliance Violations ({len(violations)} found)</h3>
                    <table>
                        <tr><th>Rule</th><th>Description</th><th>Severity</th></tr>
                        {violations_html if violations_html else '<tr><td colspan="3" style="padding: 10px; text-align: center;">No violations</td></tr>'}
                    </table>
                </div>
                <div class="footer">
                    Generated by ETL Framework | {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}
                </div>
            </div>
        </body>
        </html>
        """

    def _build_compliance_report_text(self, job_name: str, compliance_results: Dict[str, Any]) -> str:
        """Build plain text content for compliance report."""
        lines = [
            f"Compliance Report: {job_name}",
            "=" * 50,
            f"Status: {compliance_results.get('status', 'unknown').upper()}",
            "",
            f"PII Findings: {len(compliance_results.get('pii_findings', []))}",
            f"Violations: {len(compliance_results.get('violations', []))}",
        ]
        return "\n".join(lines)

    def _build_summary_html(self, summary: Dict[str, Any], period: str) -> str:
        """Build HTML content for summary report."""
        return f"""
        <!DOCTYPE html>
        <html>
        <head><meta charset="UTF-8"></head>
        <body style="font-family: Arial, sans-serif; padding: 20px;">
            <h1>📈 {period} ETL Summary</h1>
            <table style="width: 100%; border-collapse: collapse;">
                <tr><td style="padding: 10px;"><strong>Total Jobs:</strong></td><td>{summary.get('total_jobs', 0)}</td></tr>
                <tr><td style="padding: 10px;"><strong>Successful:</strong></td><td style="color: green;">{summary.get('successful', 0)}</td></tr>
                <tr><td style="padding: 10px;"><strong>Failed:</strong></td><td style="color: red;">{summary.get('failed', 0)}</td></tr>
                <tr><td style="padding: 10px;"><strong>Success Rate:</strong></td><td>{summary.get('success_rate', 0):.1f}%</td></tr>
                <tr><td style="padding: 10px;"><strong>Total Cost:</strong></td><td>${summary.get('total_cost', 0):.2f}</td></tr>
            </table>
            <p style="color: #666; font-size: 12px;">Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        </body>
        </html>
        """

    def _build_summary_text(self, summary: Dict[str, Any], period: str) -> str:
        """Build plain text content for summary report."""
        return f"""
{period} ETL Summary
{'=' * 40}
Total Jobs: {summary.get('total_jobs', 0)}
Successful: {summary.get('successful', 0)}
Failed: {summary.get('failed', 0)}
Success Rate: {summary.get('success_rate', 0):.1f}%
Total Cost: ${summary.get('total_cost', 0):.2f}
        """

    def _build_error_alert_html(self, job_name: str, error: str, stack_trace: Optional[str]) -> str:
        """Build HTML content for error alert."""
        stack_html = f"<pre style='background: #f8f9fa; padding: 15px; overflow: auto; font-size: 12px;'>{stack_trace}</pre>" if stack_trace else ""

        return f"""
        <!DOCTYPE html>
        <html>
        <head><meta charset="UTF-8"></head>
        <body style="font-family: Arial, sans-serif; padding: 20px;">
            <div style="background: #dc3545; color: white; padding: 20px; border-radius: 8px;">
                <h1 style="margin: 0;">🚨 ETL Error Alert</h1>
                <p style="margin: 10px 0 0 0;">{job_name}</p>
            </div>
            <div style="padding: 20px;">
                <h3>Error Message</h3>
                <p style="color: #dc3545; background: #f8d7da; padding: 15px; border-radius: 4px;">{error}</p>
                {f'<h3>Stack Trace</h3>{stack_html}' if stack_trace else ''}
            </div>
            <p style="color: #666; font-size: 12px;">Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
        </body>
        </html>
        """

    def _build_error_alert_text(self, job_name: str, error: str, stack_trace: Optional[str]) -> str:
        """Build plain text content for error alert."""
        lines = [
            f"ETL Error Alert: {job_name}",
            "=" * 50,
            "",
            f"Error: {error}",
        ]
        if stack_trace:
            lines.extend(["", "Stack Trace:", stack_trace])
        return "\n".join(lines)
