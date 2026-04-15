#!/usr/bin/env python3
"""
Microsoft Teams Integration
===========================

Enterprise Teams integration for ETL framework:
1. Send adaptive card notifications
2. Webhook-based message delivery
3. Rich formatting with status indicators
4. Action buttons for approvals
5. Real-time job updates

Requires:
- TEAMS_WEBHOOK_URL: Incoming webhook URL for the Teams channel
"""

import os
import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import urllib.request
import urllib.error


class TeamsMessageType(Enum):
    """Types of Teams messages."""
    SUCCESS = "success"
    FAILURE = "failure"
    WARNING = "warning"
    INFO = "info"
    APPROVAL = "approval"


@dataclass
class TeamsMessage:
    """A Teams message to send."""
    title: str
    text: str
    sections: List[Dict] = field(default_factory=list)
    actions: List[Dict] = field(default_factory=list)
    theme_color: str = "0078D4"


class TeamsIntegration:
    """
    Microsoft Teams integration for ETL notifications.
    """

    def __init__(self, config):
        self.config = config
        self.webhook_url = config.teams_webhook_url if hasattr(config, 'teams_webhook_url') else os.getenv('TEAMS_WEBHOOK_URL')

        # Color mapping
        self.color_map = {
            TeamsMessageType.SUCCESS: "28a745",
            TeamsMessageType.FAILURE: "dc3545",
            TeamsMessageType.WARNING: "ffc107",
            TeamsMessageType.INFO: "0078D4",
            TeamsMessageType.APPROVAL: "6f42c1"
        }

        # Icon mapping
        self.icon_map = {
            TeamsMessageType.SUCCESS: "✅",
            TeamsMessageType.FAILURE: "❌",
            TeamsMessageType.WARNING: "⚠️",
            TeamsMessageType.INFO: "ℹ️",
            TeamsMessageType.APPROVAL: "🔔"
        }

    def send_notification(
        self,
        message_type: TeamsMessageType,
        title: str,
        details: Dict[str, Any],
        actions: Optional[List[Dict]] = None
    ) -> Dict[str, Any]:
        """
        Send a formatted notification to Teams.

        Args:
            message_type: Type of message
            title: Main message title
            details: Dictionary of details to include
            actions: Optional action buttons

        Returns:
            API response or error details
        """
        if not self.webhook_url:
            return {"ok": False, "error": "No webhook URL configured"}

        icon = self.icon_map.get(message_type, "ℹ️")
        color = self.color_map.get(message_type, "0078D4")

        # Build adaptive card
        card = self._build_adaptive_card(
            title=f"{icon} {title}",
            details=details,
            color=color,
            actions=actions
        )

        return self._send_card(card)

    def send_job_started(self, job_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Send job started notification."""
        return self.send_notification(
            message_type=TeamsMessageType.INFO,
            title=f"ETL Job Started: {job_name}",
            details={
                "Job Name": job_name,
                "Platform": config.get("platform", "N/A"),
                "Workers": str(config.get("num_workers", "N/A")),
                "Started At": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                "Triggered By": config.get("triggered_by", "System")
            }
        )

    def send_job_completed(self, job_name: str, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Send job completed notification."""
        duration = metrics.get("duration_seconds", 0)
        duration_str = f"{duration // 60}m {duration % 60}s" if duration else "N/A"

        return self.send_notification(
            message_type=TeamsMessageType.SUCCESS,
            title=f"ETL Job Completed: {job_name}",
            details={
                "Job Name": job_name,
                "Duration": duration_str,
                "Records Processed": f"{metrics.get('records_processed', 'N/A'):,}" if isinstance(metrics.get('records_processed'), int) else str(metrics.get('records_processed', 'N/A')),
                "Cost": f"${metrics.get('cost', 0):.2f}",
                "Output Location": metrics.get("output_path", "N/A")
            }
        )

    def send_job_failed(
        self,
        job_name: str,
        error: str,
        healing_attempted: bool = False
    ) -> Dict[str, Any]:
        """Send job failed notification."""
        details = {
            "Job Name": job_name,
            "Error": error[:500] + "..." if len(error) > 500 else error,
            "Failed At": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        }

        if healing_attempted:
            details["Auto-Healing"] = "Attempted but unsuccessful"

        return self.send_notification(
            message_type=TeamsMessageType.FAILURE,
            title=f"ETL Job Failed: {job_name}",
            details=details
        )

    def send_dq_alert(self, job_name: str, dq_results: Dict[str, Any]) -> Dict[str, Any]:
        """Send data quality alert."""
        failed_rules = [r for r in dq_results.get("results", []) if r.get("status") == "failed"]

        details = {
            "Job Name": job_name,
            "Total Rules": str(dq_results.get("total_rules", 0)),
            "Failed Rules": str(len(failed_rules)),
            "Pass Rate": f"{dq_results.get('pass_rate', 0) * 100:.1f}%"
        }

        if failed_rules:
            details["Top Failures"] = ", ".join([r.get("rule_id", "Unknown") for r in failed_rules[:3]])

        message_type = TeamsMessageType.FAILURE if failed_rules else TeamsMessageType.SUCCESS

        return self.send_notification(
            message_type=message_type,
            title=f"Data Quality Report: {job_name}",
            details=details
        )

    def send_compliance_alert(self, job_name: str, compliance_results: Dict[str, Any]) -> Dict[str, Any]:
        """Send compliance alert."""
        violations = compliance_results.get("violations", [])
        pii_findings = compliance_results.get("pii_findings", [])

        details = {
            "Job Name": job_name,
            "Status": compliance_results.get("status", "unknown").upper(),
            "PII Detected": str(len(pii_findings)),
            "Compliance Violations": str(len(violations))
        }

        if violations:
            critical = [v for v in violations if v.get("severity") == "critical"]
            if critical:
                details["Critical Violations"] = str(len(critical))

        message_type = TeamsMessageType.FAILURE if violations else (TeamsMessageType.WARNING if pii_findings else TeamsMessageType.SUCCESS)

        return self.send_notification(
            message_type=message_type,
            title=f"Compliance Check: {job_name}",
            details=details
        )

    def send_approval_request(
        self,
        job_name: str,
        action: str,
        details: Dict[str, Any],
        approve_url: str,
        reject_url: str
    ) -> Dict[str, Any]:
        """Send an approval request with action buttons."""
        actions = [
            {
                "@type": "OpenUri",
                "name": "✅ Approve",
                "targets": [{"os": "default", "uri": approve_url}]
            },
            {
                "@type": "OpenUri",
                "name": "❌ Reject",
                "targets": [{"os": "default", "uri": reject_url}]
            }
        ]

        all_details = {"Job Name": job_name, "Action Required": action}
        all_details.update({k: str(v) for k, v in details.items()})

        return self.send_notification(
            message_type=TeamsMessageType.APPROVAL,
            title=f"Approval Required: {action}",
            details=all_details,
            actions=actions
        )

    def send_summary_report(
        self,
        report_title: str,
        metrics: Dict[str, Any],
        period: str = "Daily"
    ) -> Dict[str, Any]:
        """Send a summary report."""
        return self.send_notification(
            message_type=TeamsMessageType.INFO,
            title=f"{period} ETL Summary: {report_title}",
            details={
                "Period": period,
                "Total Jobs": str(metrics.get("total_jobs", 0)),
                "Successful": str(metrics.get("successful", 0)),
                "Failed": str(metrics.get("failed", 0)),
                "Success Rate": f"{metrics.get('success_rate', 0):.1f}%",
                "Total Cost": f"${metrics.get('total_cost', 0):.2f}",
                "Total Duration": f"{metrics.get('total_duration_minutes', 0):.0f} minutes"
            }
        )

    def _build_adaptive_card(
        self,
        title: str,
        details: Dict[str, Any],
        color: str,
        actions: Optional[List[Dict]] = None
    ) -> Dict:
        """Build a Teams adaptive card."""
        # Build facts from details
        facts = [
            {"name": str(k), "value": str(v)}
            for k, v in details.items()
        ]

        card = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": color,
            "summary": title,
            "sections": [{
                "activityTitle": title,
                "facts": facts,
                "markdown": True
            }]
        }

        if actions:
            card["potentialAction"] = actions

        return card

    def _send_card(self, card: Dict) -> Dict[str, Any]:
        """Send an adaptive card to Teams."""
        if not self.webhook_url:
            return {"ok": False, "error": "No webhook URL configured"}

        try:
            data = json.dumps(card).encode('utf-8')
            req = urllib.request.Request(
                self.webhook_url,
                data=data,
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=10) as response:
                return {"ok": True, "status": response.status}
        except urllib.error.URLError as e:
            return {"ok": False, "error": str(e)}
        except Exception as e:
            return {"ok": False, "error": str(e)}
