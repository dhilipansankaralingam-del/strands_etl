"""
Unified Notification Manager for ETL Framework
Handles Slack, Teams, and Email notifications with Y/N flags from JSON config
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class NotificationType(Enum):
    """Types of notifications"""
    JOB_START = "job_start"
    JOB_SUCCESS = "job_success"
    JOB_FAILURE = "job_failure"
    JOB_WARNING = "job_warning"
    DQ_ALERT = "dq_alert"
    COST_ALERT = "cost_alert"
    RECOMMENDATION = "recommendation"
    DAILY_SUMMARY = "daily_summary"


@dataclass
class NotificationConfig:
    """Unified notification configuration from JSON"""

    # Global notification enable/disable
    notifications_enabled: bool = True

    # Slack Configuration
    slack_enabled: bool = False
    slack_webhook_url: str = ""
    slack_channel: str = "#etl-alerts"
    slack_bot_token: str = ""

    # Teams Configuration
    teams_enabled: bool = False
    teams_webhook_url: str = ""
    teams_channel: str = "etl-alerts"

    # Email Configuration
    email_enabled: bool = False
    email_sender: str = ""
    email_recipients: List[str] = field(default_factory=list)
    email_ses_region: str = "us-east-1"

    # Notification Preferences (what to notify on)
    notify_on_start: bool = False
    notify_on_success: bool = True
    notify_on_failure: bool = True
    notify_on_warning: bool = True
    notify_on_dq_failure: bool = True
    notify_on_cost_alert: bool = True
    notify_on_recommendations: bool = False

    # Thresholds
    dq_score_threshold: float = 0.8
    cost_alert_threshold_usd: float = 100.0
    duration_alert_minutes: int = 60

    # Mention/escalation settings
    mention_users_on_failure: List[str] = field(default_factory=list)

    @classmethod
    def from_json(cls, config: Dict[str, Any]) -> 'NotificationConfig':
        """Create NotificationConfig from JSON with Y/N flag support"""

        notifications = config.get('notifications', {})

        def parse_flag(value: Any, default: bool = False) -> bool:
            """Parse Y/N flag or boolean"""
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.upper() in ('Y', 'YES', 'TRUE', '1')
            return default

        # Parse Slack config
        slack_config = notifications.get('slack', {})
        slack_enabled = parse_flag(slack_config.get('enabled', 'N'))

        # Parse Teams config
        teams_config = notifications.get('teams', {})
        teams_enabled = parse_flag(teams_config.get('enabled', 'N'))

        # Parse Email config
        email_config = notifications.get('email', {})
        email_enabled = parse_flag(email_config.get('enabled', 'N'))

        # Parse notification preferences
        preferences = notifications.get('preferences', {})

        return cls(
            notifications_enabled=parse_flag(notifications.get('enabled', 'Y'), True),

            # Slack
            slack_enabled=slack_enabled,
            slack_webhook_url=slack_config.get('webhook_url', ''),
            slack_channel=slack_config.get('channel', '#etl-alerts'),
            slack_bot_token=slack_config.get('bot_token', ''),

            # Teams
            teams_enabled=teams_enabled,
            teams_webhook_url=teams_config.get('webhook_url', ''),
            teams_channel=teams_config.get('channel', 'etl-alerts'),

            # Email
            email_enabled=email_enabled,
            email_sender=email_config.get('sender', ''),
            email_recipients=email_config.get('recipients', []),
            email_ses_region=email_config.get('ses_region', 'us-east-1'),

            # Preferences
            notify_on_start=parse_flag(preferences.get('on_start', 'N')),
            notify_on_success=parse_flag(preferences.get('on_success', 'Y'), True),
            notify_on_failure=parse_flag(preferences.get('on_failure', 'Y'), True),
            notify_on_warning=parse_flag(preferences.get('on_warning', 'Y'), True),
            notify_on_dq_failure=parse_flag(preferences.get('on_dq_failure', 'Y'), True),
            notify_on_cost_alert=parse_flag(preferences.get('on_cost_alert', 'Y'), True),
            notify_on_recommendations=parse_flag(preferences.get('on_recommendations', 'N')),

            # Thresholds
            dq_score_threshold=float(preferences.get('dq_score_threshold', 0.8)),
            cost_alert_threshold_usd=float(preferences.get('cost_alert_threshold_usd', 100.0)),
            duration_alert_minutes=int(preferences.get('duration_alert_minutes', 60)),

            # Escalation
            mention_users_on_failure=preferences.get('mention_users_on_failure', [])
        )


class NotificationManager:
    """Unified notification manager for all channels"""

    def __init__(self, config: NotificationConfig):
        self.config = config
        self._slack = None
        self._teams = None
        self._email = None
        self._initialized = False

    def _lazy_init(self):
        """Lazy initialization of notification channels"""
        if self._initialized:
            return

        if self.config.slack_enabled:
            try:
                from integrations.slack_integration import SlackIntegration, SlackConfig
                slack_config = SlackConfig(
                    webhook_url=self.config.slack_webhook_url,
                    bot_token=self.config.slack_bot_token,
                    default_channel=self.config.slack_channel,
                    enabled=True
                )
                self._slack = SlackIntegration(slack_config)
                logger.info("Slack integration initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Slack: {e}")

        if self.config.teams_enabled:
            try:
                from integrations.teams_integration import TeamsIntegration, TeamsConfig
                teams_config = TeamsConfig(
                    webhook_url=self.config.teams_webhook_url,
                    channel_name=self.config.teams_channel,
                    enabled=True,
                    notify_on_start=self.config.notify_on_start,
                    notify_on_success=self.config.notify_on_success,
                    notify_on_failure=self.config.notify_on_failure,
                    notify_on_dq_failure=self.config.notify_on_dq_failure,
                    notify_on_cost_alert=self.config.notify_on_cost_alert,
                    dq_score_threshold=self.config.dq_score_threshold,
                    cost_alert_threshold_usd=self.config.cost_alert_threshold_usd,
                    mention_on_failure=self.config.mention_users_on_failure
                )
                self._teams = TeamsIntegration(teams_config)
                logger.info("Teams integration initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Teams: {e}")

        if self.config.email_enabled:
            try:
                from integrations.email_reporter import EmailReporter, EmailConfig
                email_config = EmailConfig(
                    sender_email=self.config.email_sender,
                    recipients=self.config.email_recipients,
                    ses_region=self.config.email_ses_region,
                    enabled=True
                )
                self._email = EmailReporter(email_config)
                logger.info("Email integration initialized")
            except Exception as e:
                logger.error(f"Failed to initialize Email: {e}")

        self._initialized = True

    def _should_notify(self, notification_type: NotificationType) -> bool:
        """Check if notification should be sent based on preferences"""
        if not self.config.notifications_enabled:
            return False

        type_map = {
            NotificationType.JOB_START: self.config.notify_on_start,
            NotificationType.JOB_SUCCESS: self.config.notify_on_success,
            NotificationType.JOB_FAILURE: self.config.notify_on_failure,
            NotificationType.JOB_WARNING: self.config.notify_on_warning,
            NotificationType.DQ_ALERT: self.config.notify_on_dq_failure,
            NotificationType.COST_ALERT: self.config.notify_on_cost_alert,
            NotificationType.RECOMMENDATION: self.config.notify_on_recommendations,
            NotificationType.DAILY_SUMMARY: True
        }

        return type_map.get(notification_type, False)

    def notify_job_start(self, job_name: str, run_id: str,
                         config: Dict[str, Any]) -> Dict[str, bool]:
        """Send job start notifications"""

        if not self._should_notify(NotificationType.JOB_START):
            return {"skipped": True, "reason": "notifications disabled for job_start"}

        self._lazy_init()
        results = {}

        if self._slack:
            try:
                results['slack'] = self._slack.send_etl_start_notification(
                    job_name, run_id, config
                )
            except Exception as e:
                logger.error(f"Slack notification failed: {e}")
                results['slack'] = False

        if self._teams:
            try:
                results['teams'] = self._teams.send_etl_start_notification(
                    job_name, run_id, config
                )
            except Exception as e:
                logger.error(f"Teams notification failed: {e}")
                results['teams'] = False

        return results

    def notify_job_success(self, job_name: str, run_id: str,
                          metrics: Dict[str, Any]) -> Dict[str, bool]:
        """Send job success notifications"""

        if not self._should_notify(NotificationType.JOB_SUCCESS):
            return {"skipped": True, "reason": "notifications disabled for job_success"}

        self._lazy_init()
        results = {}

        if self._slack:
            try:
                results['slack'] = self._slack.send_etl_success_notification(
                    job_name, run_id, metrics
                )
            except Exception as e:
                logger.error(f"Slack notification failed: {e}")
                results['slack'] = False

        if self._teams:
            try:
                results['teams'] = self._teams.send_etl_success_notification(
                    job_name, run_id, metrics
                )
            except Exception as e:
                logger.error(f"Teams notification failed: {e}")
                results['teams'] = False

        if self._email:
            try:
                results['email'] = self._email.send_job_completion_report(
                    job_name, run_id, metrics
                )
            except Exception as e:
                logger.error(f"Email notification failed: {e}")
                results['email'] = False

        return results

    def notify_job_failure(self, job_name: str, run_id: str,
                          error: str, metrics: Dict[str, Any] = None) -> Dict[str, bool]:
        """Send job failure notifications"""

        if not self._should_notify(NotificationType.JOB_FAILURE):
            return {"skipped": True, "reason": "notifications disabled for job_failure"}

        self._lazy_init()
        results = {}

        if self._slack:
            try:
                results['slack'] = self._slack.send_etl_failure_notification(
                    job_name, run_id, error, metrics
                )
            except Exception as e:
                logger.error(f"Slack notification failed: {e}")
                results['slack'] = False

        if self._teams:
            try:
                results['teams'] = self._teams.send_etl_failure_notification(
                    job_name, run_id, error, metrics
                )
            except Exception as e:
                logger.error(f"Teams notification failed: {e}")
                results['teams'] = False

        if self._email:
            try:
                results['email'] = self._email.send_failure_alert(
                    job_name, run_id, error, metrics
                )
            except Exception as e:
                logger.error(f"Email notification failed: {e}")
                results['email'] = False

        return results

    def notify_dq_alert(self, job_name: str, run_id: str,
                       dq_results: Dict[str, Any]) -> Dict[str, bool]:
        """Send data quality alert"""

        dq_score = dq_results.get('overall_score', 1.0)
        if dq_score >= self.config.dq_score_threshold:
            return {"skipped": True, "reason": "DQ score above threshold"}

        if not self._should_notify(NotificationType.DQ_ALERT):
            return {"skipped": True, "reason": "notifications disabled for dq_alert"}

        self._lazy_init()
        results = {}

        if self._slack:
            try:
                results['slack'] = self._slack.send_dq_alert(
                    job_name, run_id, dq_results
                )
            except Exception as e:
                logger.error(f"Slack DQ alert failed: {e}")
                results['slack'] = False

        if self._teams:
            try:
                results['teams'] = self._teams.send_dq_alert(
                    job_name, run_id, dq_results
                )
            except Exception as e:
                logger.error(f"Teams DQ alert failed: {e}")
                results['teams'] = False

        return results

    def notify_cost_alert(self, job_name: str, run_id: str,
                         estimated_cost: float, details: Dict[str, Any] = None) -> Dict[str, bool]:
        """Send cost alert"""

        if estimated_cost < self.config.cost_alert_threshold_usd:
            return {"skipped": True, "reason": "cost below threshold"}

        if not self._should_notify(NotificationType.COST_ALERT):
            return {"skipped": True, "reason": "notifications disabled for cost_alert"}

        self._lazy_init()
        results = {}

        if self._slack:
            try:
                results['slack'] = self._slack.send_cost_alert(
                    job_name, run_id, estimated_cost, details
                )
            except Exception as e:
                logger.error(f"Slack cost alert failed: {e}")
                results['slack'] = False

        if self._teams:
            try:
                results['teams'] = self._teams.send_cost_alert(
                    job_name, run_id, estimated_cost, details
                )
            except Exception as e:
                logger.error(f"Teams cost alert failed: {e}")
                results['teams'] = False

        return results

    def notify_recommendations(self, job_name: str, run_id: str,
                              recommendations: List[Dict[str, Any]]) -> Dict[str, bool]:
        """Send optimization recommendations"""

        if not recommendations:
            return {"skipped": True, "reason": "no recommendations"}

        if not self._should_notify(NotificationType.RECOMMENDATION):
            return {"skipped": True, "reason": "notifications disabled for recommendations"}

        self._lazy_init()
        results = {}

        if self._teams:
            try:
                results['teams'] = self._teams.send_recommendations(
                    job_name, run_id, recommendations
                )
            except Exception as e:
                logger.error(f"Teams recommendations failed: {e}")
                results['teams'] = False

        return results

    def send_daily_summary(self, summary: Dict[str, Any]) -> Dict[str, bool]:
        """Send daily summary to all channels"""

        self._lazy_init()
        results = {}

        if self._slack:
            try:
                results['slack'] = self._slack.send_daily_summary(summary)
            except Exception as e:
                logger.error(f"Slack daily summary failed: {e}")
                results['slack'] = False

        if self._teams:
            try:
                results['teams'] = self._teams.send_daily_summary(summary)
            except Exception as e:
                logger.error(f"Teams daily summary failed: {e}")
                results['teams'] = False

        if self._email:
            try:
                results['email'] = self._email.send_daily_summary(summary)
            except Exception as e:
                logger.error(f"Email daily summary failed: {e}")
                results['email'] = False

        return results

    def get_status(self) -> Dict[str, Any]:
        """Get notification manager status"""
        return {
            "notifications_enabled": self.config.notifications_enabled,
            "channels": {
                "slack": {
                    "enabled": self.config.slack_enabled,
                    "configured": bool(self.config.slack_webhook_url or self.config.slack_bot_token),
                    "channel": self.config.slack_channel
                },
                "teams": {
                    "enabled": self.config.teams_enabled,
                    "configured": bool(self.config.teams_webhook_url),
                    "channel": self.config.teams_channel
                },
                "email": {
                    "enabled": self.config.email_enabled,
                    "configured": bool(self.config.email_sender and self.config.email_recipients),
                    "recipients_count": len(self.config.email_recipients)
                }
            },
            "preferences": {
                "on_start": self.config.notify_on_start,
                "on_success": self.config.notify_on_success,
                "on_failure": self.config.notify_on_failure,
                "on_warning": self.config.notify_on_warning,
                "on_dq_failure": self.config.notify_on_dq_failure,
                "on_cost_alert": self.config.notify_on_cost_alert,
                "on_recommendations": self.config.notify_on_recommendations
            },
            "thresholds": {
                "dq_score": self.config.dq_score_threshold,
                "cost_alert_usd": self.config.cost_alert_threshold_usd,
                "duration_alert_minutes": self.config.duration_alert_minutes
            }
        }


def create_notification_manager(config: Dict[str, Any]) -> NotificationManager:
    """Factory function to create NotificationManager from JSON config"""
    notification_config = NotificationConfig.from_json(config)
    return NotificationManager(notification_config)


# ============ Example JSON Configuration Schema ============

NOTIFICATION_CONFIG_SCHEMA = {
    "notifications": {
        "enabled": "Y",  # Y/N - Master switch for all notifications

        "slack": {
            "enabled": "Y",  # Y/N
            "webhook_url": "https://hooks.slack.com/services/xxx/yyy/zzz",
            "channel": "#etl-alerts",
            "bot_token": "xoxb-xxx-yyy-zzz"  # Optional, for advanced features
        },

        "teams": {
            "enabled": "N",  # Y/N
            "webhook_url": "https://outlook.office.com/webhook/xxx",
            "channel": "etl-alerts"
        },

        "email": {
            "enabled": "Y",  # Y/N
            "sender": "etl-alerts@company.com",
            "recipients": ["team@company.com", "oncall@company.com"],
            "ses_region": "us-east-1"
        },

        "preferences": {
            "on_start": "N",      # Y/N - Notify when job starts
            "on_success": "Y",    # Y/N - Notify when job succeeds
            "on_failure": "Y",    # Y/N - Notify when job fails
            "on_warning": "Y",    # Y/N - Notify on warnings
            "on_dq_failure": "Y", # Y/N - Notify on data quality failures
            "on_cost_alert": "Y", # Y/N - Notify on cost threshold exceeded
            "on_recommendations": "N",  # Y/N - Notify with optimization recommendations

            # Thresholds
            "dq_score_threshold": 0.8,
            "cost_alert_threshold_usd": 100.0,
            "duration_alert_minutes": 60,

            # Escalation
            "mention_users_on_failure": ["user@company.com"]
        }
    }
}
