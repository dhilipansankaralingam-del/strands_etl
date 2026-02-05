"""
Microsoft Teams Integration for ETL Framework
Provides Teams notifications, adaptive cards, and collaboration features
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum

import boto3
import requests

logger = logging.getLogger(__name__)


class TeamsMessageType(Enum):
    """Types of Teams messages"""
    SIMPLE = "simple"
    ADAPTIVE_CARD = "adaptive_card"
    ACTION_CARD = "action_card"


class AlertSeverity(Enum):
    """Alert severity levels with Teams colors"""
    INFO = "default"
    SUCCESS = "good"
    WARNING = "warning"
    ERROR = "attention"
    CRITICAL = "attention"


@dataclass
class TeamsConfig:
    """Teams integration configuration"""
    webhook_url: str
    channel_name: str = "etl-alerts"
    enabled: bool = True
    # Notification preferences
    notify_on_start: bool = False
    notify_on_success: bool = True
    notify_on_failure: bool = True
    notify_on_warning: bool = True
    notify_on_dq_failure: bool = True
    notify_on_cost_alert: bool = True
    # Thresholds
    cost_alert_threshold_usd: float = 100.0
    dq_score_threshold: float = 0.8
    duration_alert_minutes: int = 60
    # Message preferences
    include_recommendations: bool = True
    include_metrics: bool = True
    mention_on_failure: List[str] = field(default_factory=list)  # User emails to mention


@dataclass
class TeamsMessage:
    """Teams message structure"""
    title: str
    text: str
    severity: AlertSeverity = AlertSeverity.INFO
    facts: Dict[str, str] = field(default_factory=dict)
    actions: List[Dict[str, Any]] = field(default_factory=list)
    mentions: List[str] = field(default_factory=list)


class TeamsIntegration:
    """Microsoft Teams integration for ETL notifications"""

    def __init__(self, config: TeamsConfig):
        self.config = config
        self.secrets_client = boto3.client('secretsmanager')

    def _get_webhook_url(self) -> str:
        """Get webhook URL from config or Secrets Manager"""
        if self.config.webhook_url.startswith('arn:aws:secretsmanager'):
            try:
                response = self.secrets_client.get_secret_value(
                    SecretId=self.config.webhook_url
                )
                secret = json.loads(response['SecretString'])
                return secret.get('webhook_url', '')
            except Exception as e:
                logger.error(f"Failed to get webhook from Secrets Manager: {e}")
                return ""
        return self.config.webhook_url

    def _get_severity_color(self, severity: AlertSeverity) -> str:
        """Map severity to Teams color"""
        color_map = {
            AlertSeverity.INFO: "default",
            AlertSeverity.SUCCESS: "good",
            AlertSeverity.WARNING: "warning",
            AlertSeverity.ERROR: "attention",
            AlertSeverity.CRITICAL: "attention"
        }
        return color_map.get(severity, "default")

    def _build_adaptive_card(self, message: TeamsMessage) -> Dict[str, Any]:
        """Build Teams Adaptive Card payload"""

        # Build facts section
        facts = []
        for key, value in message.facts.items():
            facts.append({
                "title": key,
                "value": str(value)
            })

        # Build card body
        body = [
            {
                "type": "TextBlock",
                "size": "Large",
                "weight": "Bolder",
                "text": message.title,
                "color": self._get_severity_color(message.severity)
            },
            {
                "type": "TextBlock",
                "text": message.text,
                "wrap": True
            }
        ]

        # Add facts if present
        if facts:
            body.append({
                "type": "FactSet",
                "facts": facts
            })

        # Add mentions if present
        entities = []
        if message.mentions:
            mention_text = " ".join([f"<at>{email}</at>" for email in message.mentions])
            body.append({
                "type": "TextBlock",
                "text": f"Attention: {mention_text}",
                "wrap": True
            })
            for email in message.mentions:
                entities.append({
                    "type": "mention",
                    "text": f"<at>{email}</at>",
                    "mentioned": {
                        "id": email,
                        "name": email.split('@')[0]
                    }
                })

        # Build actions
        actions = []
        for action in message.actions:
            actions.append({
                "type": "Action.OpenUrl",
                "title": action.get("title", "View"),
                "url": action.get("url", "#")
            })

        # Construct the adaptive card
        card = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "contentUrl": None,
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": body,
                        "actions": actions if actions else None,
                        "msteams": {
                            "entities": entities
                        } if entities else None
                    }
                }
            ]
        }

        return card

    def _build_simple_message(self, message: TeamsMessage) -> Dict[str, Any]:
        """Build simple Teams message payload"""

        sections = [{
            "activityTitle": message.title,
            "activitySubtitle": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "text": message.text,
            "facts": [{"name": k, "value": str(v)} for k, v in message.facts.items()]
        }]

        # Add actions
        potential_actions = []
        for action in message.actions:
            potential_actions.append({
                "@type": "OpenUri",
                "name": action.get("title", "View"),
                "targets": [{"os": "default", "uri": action.get("url", "#")}]
            })

        return {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": self._get_theme_color(message.severity),
            "summary": message.title,
            "sections": sections,
            "potentialAction": potential_actions if potential_actions else None
        }

    def _get_theme_color(self, severity: AlertSeverity) -> str:
        """Get theme color hex for message card"""
        color_map = {
            AlertSeverity.INFO: "0076D7",
            AlertSeverity.SUCCESS: "00FF00",
            AlertSeverity.WARNING: "FFA500",
            AlertSeverity.ERROR: "FF0000",
            AlertSeverity.CRITICAL: "8B0000"
        }
        return color_map.get(severity, "0076D7")

    def send_message(self, message: TeamsMessage,
                     message_type: TeamsMessageType = TeamsMessageType.ADAPTIVE_CARD) -> bool:
        """Send message to Teams channel"""

        if not self.config.enabled:
            logger.info("Teams notifications disabled")
            return True

        webhook_url = self._get_webhook_url()
        if not webhook_url:
            logger.error("No webhook URL configured")
            return False

        try:
            if message_type == TeamsMessageType.ADAPTIVE_CARD:
                payload = self._build_adaptive_card(message)
            else:
                payload = self._build_simple_message(message)

            response = requests.post(
                webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )

            if response.status_code == 200:
                logger.info(f"Teams message sent successfully: {message.title}")
                return True
            else:
                logger.error(f"Teams API error: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Failed to send Teams message: {e}")
            return False

    def send_etl_start_notification(self, job_name: str, run_id: str,
                                    config: Dict[str, Any]) -> bool:
        """Send ETL job start notification"""

        if not self.config.notify_on_start:
            return True

        message = TeamsMessage(
            title=f"ETL Job Started: {job_name}",
            text=f"Job `{job_name}` has started execution.",
            severity=AlertSeverity.INFO,
            facts={
                "Run ID": run_id,
                "Start Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
                "Platform": config.get("platform", "auto"),
                "Source": config.get("source", {}).get("type", "unknown"),
                "Target": config.get("target", {}).get("type", "unknown")
            }
        )

        return self.send_message(message)

    def send_etl_success_notification(self, job_name: str, run_id: str,
                                      metrics: Dict[str, Any]) -> bool:
        """Send ETL job success notification"""

        if not self.config.notify_on_success:
            return True

        facts = {
            "Run ID": run_id,
            "Duration": f"{metrics.get('duration_seconds', 0)} seconds",
            "Rows Processed": f"{metrics.get('rows_read', 0):,}",
            "Rows Written": f"{metrics.get('rows_written', 0):,}"
        }

        if self.config.include_metrics:
            facts["Estimated Cost"] = f"${metrics.get('estimated_cost_usd', 0):.4f}"
            facts["DQ Score"] = f"{metrics.get('dq_score', 1.0):.1%}"

        message = TeamsMessage(
            title=f"ETL Job Completed: {job_name}",
            text=f"Job `{job_name}` completed successfully.",
            severity=AlertSeverity.SUCCESS,
            facts=facts
        )

        return self.send_message(message)

    def send_etl_failure_notification(self, job_name: str, run_id: str,
                                      error: str, metrics: Dict[str, Any] = None) -> bool:
        """Send ETL job failure notification"""

        if not self.config.notify_on_failure:
            return True

        facts = {
            "Run ID": run_id,
            "Failed At": datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "Error": error[:200] + "..." if len(error) > 200 else error
        }

        if metrics:
            facts["Duration Before Failure"] = f"{metrics.get('duration_seconds', 0)} seconds"
            facts["Rows Processed"] = f"{metrics.get('rows_read', 0):,}"

        message = TeamsMessage(
            title=f"ETL Job Failed: {job_name}",
            text=f"Job `{job_name}` has failed and requires attention.",
            severity=AlertSeverity.ERROR,
            facts=facts,
            mentions=self.config.mention_on_failure,
            actions=[
                {"title": "View Logs", "url": f"https://console.aws.amazon.com/cloudwatch/home?#logsV2:log-groups/log-group/etl-jobs/log-events/{run_id}"},
                {"title": "Retry Job", "url": f"https://your-etl-dashboard.com/jobs/{job_name}/retry"}
            ]
        )

        return self.send_message(message)

    def send_dq_alert(self, job_name: str, run_id: str,
                      dq_results: Dict[str, Any]) -> bool:
        """Send data quality alert"""

        if not self.config.notify_on_dq_failure:
            return True

        dq_score = dq_results.get('overall_score', 1.0)
        if dq_score >= self.config.dq_score_threshold:
            return True  # No alert needed

        failed_rules = dq_results.get('failed_rules', [])
        failed_summary = ", ".join(failed_rules[:5])
        if len(failed_rules) > 5:
            failed_summary += f" (+{len(failed_rules) - 5} more)"

        message = TeamsMessage(
            title=f"Data Quality Alert: {job_name}",
            text=f"Job `{job_name}` has data quality issues below threshold.",
            severity=AlertSeverity.WARNING,
            facts={
                "Run ID": run_id,
                "DQ Score": f"{dq_score:.1%}",
                "Threshold": f"{self.config.dq_score_threshold:.1%}",
                "Failed Rules": failed_summary,
                "Total Rules": str(dq_results.get('total_rules', 0)),
                "Passed Rules": str(dq_results.get('passed_rules', 0))
            },
            actions=[
                {"title": "View DQ Report", "url": f"https://your-etl-dashboard.com/jobs/{job_name}/dq/{run_id}"}
            ]
        )

        return self.send_message(message)

    def send_cost_alert(self, job_name: str, run_id: str,
                        estimated_cost: float, details: Dict[str, Any] = None) -> bool:
        """Send cost threshold alert"""

        if not self.config.notify_on_cost_alert:
            return True

        if estimated_cost < self.config.cost_alert_threshold_usd:
            return True  # No alert needed

        facts = {
            "Run ID": run_id,
            "Estimated Cost": f"${estimated_cost:.2f}",
            "Threshold": f"${self.config.cost_alert_threshold_usd:.2f}",
            "Exceeded By": f"${estimated_cost - self.config.cost_alert_threshold_usd:.2f}"
        }

        if details:
            facts["Platform"] = details.get("platform", "unknown")
            facts["Duration"] = f"{details.get('duration_minutes', 0):.1f} minutes"

        message = TeamsMessage(
            title=f"Cost Alert: {job_name}",
            text=f"Job `{job_name}` has exceeded the cost threshold.",
            severity=AlertSeverity.WARNING,
            facts=facts,
            mentions=self.config.mention_on_failure
        )

        return self.send_message(message)

    def send_recommendations(self, job_name: str, run_id: str,
                            recommendations: List[Dict[str, Any]]) -> bool:
        """Send optimization recommendations"""

        if not self.config.include_recommendations or not recommendations:
            return True

        rec_text = "\n".join([f"- {r.get('title', 'Recommendation')}: {r.get('description', '')}"
                             for r in recommendations[:5]])

        total_savings = sum(r.get('estimated_savings_usd', 0) for r in recommendations)

        message = TeamsMessage(
            title=f"Optimization Recommendations: {job_name}",
            text=f"We've identified optimization opportunities for job `{job_name}`:\n\n{rec_text}",
            severity=AlertSeverity.INFO,
            facts={
                "Run ID": run_id,
                "Total Recommendations": str(len(recommendations)),
                "Potential Monthly Savings": f"${total_savings:.2f}"
            },
            actions=[
                {"title": "View Details", "url": f"https://your-etl-dashboard.com/jobs/{job_name}/recommendations"}
            ]
        )

        return self.send_message(message)

    def send_daily_summary(self, summary: Dict[str, Any]) -> bool:
        """Send daily ETL summary"""

        message = TeamsMessage(
            title="Daily ETL Summary",
            text=f"Summary for {summary.get('date', datetime.now().strftime('%Y-%m-%d'))}",
            severity=AlertSeverity.INFO,
            facts={
                "Total Jobs": str(summary.get('total_jobs', 0)),
                "Successful": str(summary.get('successful_jobs', 0)),
                "Failed": str(summary.get('failed_jobs', 0)),
                "Total Rows Processed": f"{summary.get('total_rows', 0):,}",
                "Total Cost": f"${summary.get('total_cost_usd', 0):.2f}",
                "Avg DQ Score": f"{summary.get('avg_dq_score', 1.0):.1%}"
            },
            actions=[
                {"title": "View Dashboard", "url": "https://your-etl-dashboard.com/daily"}
            ]
        )

        return self.send_message(message)


class TeamsIntegrationFactory:
    """Factory for creating Teams integration from config"""

    @staticmethod
    def from_dict(config: Dict[str, Any]) -> TeamsIntegration:
        """Create TeamsIntegration from dictionary config"""

        teams_config = TeamsConfig(
            webhook_url=config.get('webhook_url', ''),
            channel_name=config.get('channel_name', 'etl-alerts'),
            enabled=config.get('enabled', True),
            notify_on_start=config.get('notify_on_start', False),
            notify_on_success=config.get('notify_on_success', True),
            notify_on_failure=config.get('notify_on_failure', True),
            notify_on_warning=config.get('notify_on_warning', True),
            notify_on_dq_failure=config.get('notify_on_dq_failure', True),
            notify_on_cost_alert=config.get('notify_on_cost_alert', True),
            cost_alert_threshold_usd=config.get('cost_alert_threshold_usd', 100.0),
            dq_score_threshold=config.get('dq_score_threshold', 0.8),
            duration_alert_minutes=config.get('duration_alert_minutes', 60),
            include_recommendations=config.get('include_recommendations', True),
            include_metrics=config.get('include_metrics', True),
            mention_on_failure=config.get('mention_on_failure', [])
        )

        return TeamsIntegration(teams_config)

    @staticmethod
    def from_json_flags(notifications_config: Dict[str, Any]) -> Optional[TeamsIntegration]:
        """Create TeamsIntegration from JSON notification flags

        Expected format:
        {
            "teams": {
                "enabled": "Y",
                "webhook_url": "https://...",
                ...
            }
        }
        """

        teams_config = notifications_config.get('teams', {})

        # Check if enabled (Y/N flag)
        enabled = teams_config.get('enabled', 'N').upper() == 'Y'

        if not enabled:
            return None

        return TeamsIntegrationFactory.from_dict({
            **teams_config,
            'enabled': True
        })
