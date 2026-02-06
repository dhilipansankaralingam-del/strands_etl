#!/usr/bin/env python3
"""
Slack Integration
=================

Enterprise Slack integration for ETL framework:
1. Send notifications (success, failure, warnings)
2. Interactive bot commands to trigger ETL jobs
3. Voice message support for hands-free ETL triggering
4. Real-time job status updates
5. Interactive approval workflows
6. Rich message formatting with attachments

Requires:
- SLACK_BOT_TOKEN: Bot token for sending messages
- SLACK_SIGNING_SECRET: For request verification
- SLACK_CHANNEL_ID: Default channel for notifications
"""

import os
import json
import hashlib
import hmac
import time
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import urllib.request
import urllib.error


class MessageType(Enum):
    """Types of Slack messages."""
    SUCCESS = "success"
    FAILURE = "failure"
    WARNING = "warning"
    INFO = "info"
    APPROVAL = "approval"
    PROGRESS = "progress"


class VoiceCommandType(Enum):
    """Types of voice commands supported."""
    TRIGGER_JOB = "trigger_job"
    CHECK_STATUS = "check_status"
    GET_METRICS = "get_metrics"
    APPROVE_ACTION = "approve_action"
    CANCEL_JOB = "cancel_job"


@dataclass
class SlackMessage:
    """A Slack message to send."""
    channel: str
    text: str
    blocks: List[Dict] = field(default_factory=list)
    attachments: List[Dict] = field(default_factory=list)
    thread_ts: Optional[str] = None
    reply_broadcast: bool = False


@dataclass
class VoiceCommand:
    """Parsed voice command from Slack."""
    command_type: VoiceCommandType
    job_name: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    user_id: str = ""
    channel_id: str = ""
    confidence: float = 0.0


class SlackIntegration:
    """
    Slack integration for ETL notifications and interactive commands.
    """

    def __init__(self, config):
        self.config = config
        self.bot_token = config.slack_bot_token if hasattr(config, 'slack_bot_token') else os.getenv('SLACK_BOT_TOKEN')
        self.signing_secret = config.slack_signing_secret if hasattr(config, 'slack_signing_secret') else os.getenv('SLACK_SIGNING_SECRET')
        self.default_channel = config.slack_channel_id if hasattr(config, 'slack_channel_id') else os.getenv('SLACK_CHANNEL_ID')

        # Job trigger callbacks
        self.job_triggers: Dict[str, Callable] = {}

        # Voice command patterns
        self.voice_patterns = self._init_voice_patterns()

        # Message color mapping
        self.color_map = {
            MessageType.SUCCESS: "#36a64f",  # Green
            MessageType.FAILURE: "#dc3545",  # Red
            MessageType.WARNING: "#ffc107",  # Yellow
            MessageType.INFO: "#17a2b8",     # Blue
            MessageType.APPROVAL: "#6f42c1", # Purple
            MessageType.PROGRESS: "#007bff"  # Blue
        }

    def _init_voice_patterns(self) -> List[Dict]:
        """Initialize voice command recognition patterns."""
        return [
            {
                "patterns": [
                    r"(?:run|start|trigger|execute)\s+(?:the\s+)?(?:job|etl|pipeline)\s+(.+)",
                    r"(?:kick off|launch)\s+(.+)\s+(?:job|etl|pipeline)?",
                    r"(?:please\s+)?(?:run|start)\s+(.+)"
                ],
                "command_type": VoiceCommandType.TRIGGER_JOB,
                "extractor": lambda m: {"job_name": m.group(1).strip()}
            },
            {
                "patterns": [
                    r"(?:what(?:'s| is)\s+the\s+)?status\s+(?:of\s+)?(?:job\s+)?(.+)",
                    r"(?:how\s+is|check)\s+(.+)\s+(?:doing|running|going)",
                    r"is\s+(.+)\s+(?:running|done|finished|complete)"
                ],
                "command_type": VoiceCommandType.CHECK_STATUS,
                "extractor": lambda m: {"job_name": m.group(1).strip()}
            },
            {
                "patterns": [
                    r"(?:show|get|give)\s+(?:me\s+)?(?:the\s+)?metrics\s+(?:for\s+)?(.+)",
                    r"(?:how\s+(?:did|was))\s+(.+)\s+(?:perform|do)"
                ],
                "command_type": VoiceCommandType.GET_METRICS,
                "extractor": lambda m: {"job_name": m.group(1).strip()}
            },
            {
                "patterns": [
                    r"(?:approve|confirm|yes|go ahead|proceed)\s*(?:with\s+)?(.+)?",
                    r"(?:lgtm|looks good|ship it)"
                ],
                "command_type": VoiceCommandType.APPROVE_ACTION,
                "extractor": lambda m: {}
            },
            {
                "patterns": [
                    r"(?:cancel|stop|abort|kill)\s+(?:the\s+)?(?:job\s+)?(.+)",
                    r"(?:don't|do not)\s+(?:run|start)\s+(.+)"
                ],
                "command_type": VoiceCommandType.CANCEL_JOB,
                "extractor": lambda m: {"job_name": m.group(1).strip() if m.group(1) else None}
            }
        ]

    def send_notification(
        self,
        message_type: MessageType,
        title: str,
        details: Dict[str, Any],
        channel: Optional[str] = None,
        thread_ts: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Send a formatted notification to Slack.

        Args:
            message_type: Type of message (success, failure, etc.)
            title: Main message title
            details: Dictionary of details to include
            channel: Channel to post to (uses default if not specified)
            thread_ts: Thread timestamp for replies

        Returns:
            API response or error details
        """
        channel = channel or self.default_channel
        if not channel:
            return {"ok": False, "error": "No channel specified"}

        # Build message blocks
        blocks = self._build_notification_blocks(message_type, title, details)

        # Build attachment for color bar
        attachments = [{
            "color": self.color_map.get(message_type, "#17a2b8"),
            "blocks": blocks
        }]

        return self._send_message(SlackMessage(
            channel=channel,
            text=title,
            attachments=attachments,
            thread_ts=thread_ts
        ))

    def send_job_started(
        self,
        job_name: str,
        config: Dict[str, Any],
        channel: Optional[str] = None
    ) -> Dict[str, Any]:
        """Send job started notification."""
        return self.send_notification(
            message_type=MessageType.PROGRESS,
            title=f"🚀 ETL Job Started: {job_name}",
            details={
                "Job Name": job_name,
                "Platform": config.get("platform", "N/A"),
                "Workers": config.get("num_workers", "N/A"),
                "Started At": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                "Triggered By": config.get("triggered_by", "System")
            },
            channel=channel
        )

    def send_job_completed(
        self,
        job_name: str,
        metrics: Dict[str, Any],
        channel: Optional[str] = None
    ) -> Dict[str, Any]:
        """Send job completed notification."""
        duration = metrics.get("duration_seconds", 0)
        duration_str = f"{duration // 60}m {duration % 60}s" if duration else "N/A"

        return self.send_notification(
            message_type=MessageType.SUCCESS,
            title=f"✅ ETL Job Completed: {job_name}",
            details={
                "Job Name": job_name,
                "Duration": duration_str,
                "Records Processed": f"{metrics.get('records_processed', 'N/A'):,}" if isinstance(metrics.get('records_processed'), int) else metrics.get('records_processed', 'N/A'),
                "Cost": f"${metrics.get('cost', 0):.2f}",
                "Output Location": metrics.get("output_path", "N/A")
            },
            channel=channel
        )

    def send_job_failed(
        self,
        job_name: str,
        error: str,
        healing_attempted: bool = False,
        channel: Optional[str] = None
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
            message_type=MessageType.FAILURE,
            title=f"❌ ETL Job Failed: {job_name}",
            details=details,
            channel=channel
        )

    def send_dq_alert(
        self,
        job_name: str,
        dq_results: Dict[str, Any],
        channel: Optional[str] = None
    ) -> Dict[str, Any]:
        """Send data quality alert."""
        failed_rules = [r for r in dq_results.get("results", []) if r.get("status") == "failed"]

        details = {
            "Job Name": job_name,
            "Total Rules": dq_results.get("total_rules", 0),
            "Failed Rules": len(failed_rules),
            "Pass Rate": f"{dq_results.get('pass_rate', 0) * 100:.1f}%"
        }

        if failed_rules:
            details["Top Failures"] = ", ".join([r.get("rule_id", "Unknown") for r in failed_rules[:3]])

        message_type = MessageType.FAILURE if failed_rules else MessageType.SUCCESS
        title = f"📊 Data Quality {'Alert' if failed_rules else 'Passed'}: {job_name}"

        return self.send_notification(
            message_type=message_type,
            title=title,
            details=details,
            channel=channel
        )

    def send_compliance_alert(
        self,
        job_name: str,
        compliance_results: Dict[str, Any],
        channel: Optional[str] = None
    ) -> Dict[str, Any]:
        """Send compliance alert."""
        violations = compliance_results.get("violations", [])
        pii_findings = compliance_results.get("pii_findings", [])

        details = {
            "Job Name": job_name,
            "Status": compliance_results.get("status", "unknown").upper(),
            "PII Detected": len(pii_findings),
            "Compliance Violations": len(violations)
        }

        if violations:
            critical = [v for v in violations if v.get("severity") == "critical"]
            if critical:
                details["Critical Violations"] = len(critical)

        message_type = MessageType.FAILURE if violations else (MessageType.WARNING if pii_findings else MessageType.SUCCESS)
        title = f"🔒 Compliance Check: {job_name}"

        return self.send_notification(
            message_type=message_type,
            title=title,
            details=details,
            channel=channel
        )

    def send_approval_request(
        self,
        job_name: str,
        action: str,
        details: Dict[str, Any],
        callback_id: str,
        channel: Optional[str] = None
    ) -> Dict[str, Any]:
        """Send an interactive approval request."""
        channel = channel or self.default_channel

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🔔 Approval Required: {action}"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Job:* {job_name}\n*Action:* {action}"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*{k}:*\n{v}"}
                    for k, v in list(details.items())[:10]
                ]
            },
            {
                "type": "actions",
                "block_id": callback_id,
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "✅ Approve"},
                        "style": "primary",
                        "action_id": "approve_action",
                        "value": json.dumps({"job_name": job_name, "action": "approve"})
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "❌ Reject"},
                        "style": "danger",
                        "action_id": "reject_action",
                        "value": json.dumps({"job_name": job_name, "action": "reject"})
                    }
                ]
            }
        ]

        return self._send_message(SlackMessage(
            channel=channel,
            text=f"Approval required for {job_name}",
            blocks=blocks
        ))

    def parse_voice_command(self, text: str, user_id: str = "", channel_id: str = "") -> Optional[VoiceCommand]:
        """
        Parse a voice command from text (transcribed audio or text command).

        Args:
            text: The command text
            user_id: Slack user ID
            channel_id: Slack channel ID

        Returns:
            Parsed VoiceCommand or None if not recognized
        """
        import re

        text_lower = text.lower().strip()

        for pattern_info in self.voice_patterns:
            for pattern in pattern_info["patterns"]:
                match = re.search(pattern, text_lower, re.IGNORECASE)
                if match:
                    params = pattern_info["extractor"](match)
                    return VoiceCommand(
                        command_type=pattern_info["command_type"],
                        job_name=params.get("job_name"),
                        parameters=params,
                        user_id=user_id,
                        channel_id=channel_id,
                        confidence=0.9 if len(text) > 10 else 0.7
                    )

        return None

    def register_job_trigger(self, job_name: str, trigger_fn: Callable) -> None:
        """Register a callback function to trigger a job."""
        self.job_triggers[job_name.lower()] = trigger_fn

    def handle_voice_command(self, command: VoiceCommand) -> Dict[str, Any]:
        """
        Handle a parsed voice command.

        Args:
            command: Parsed VoiceCommand

        Returns:
            Result of the command execution
        """
        if command.command_type == VoiceCommandType.TRIGGER_JOB:
            return self._handle_trigger_job(command)
        elif command.command_type == VoiceCommandType.CHECK_STATUS:
            return self._handle_check_status(command)
        elif command.command_type == VoiceCommandType.GET_METRICS:
            return self._handle_get_metrics(command)
        elif command.command_type == VoiceCommandType.CANCEL_JOB:
            return self._handle_cancel_job(command)
        else:
            return {"success": False, "message": "Unknown command type"}

    def _handle_trigger_job(self, command: VoiceCommand) -> Dict[str, Any]:
        """Handle job trigger command."""
        job_name = command.job_name.lower() if command.job_name else ""

        if job_name in self.job_triggers:
            try:
                result = self.job_triggers[job_name](command.parameters)
                self.send_notification(
                    MessageType.INFO,
                    f"🎤 Voice Command: Triggered {job_name}",
                    {"Requested By": f"<@{command.user_id}>", "Job": job_name},
                    channel=command.channel_id
                )
                return {"success": True, "message": f"Job {job_name} triggered", "result": result}
            except Exception as e:
                return {"success": False, "message": f"Failed to trigger {job_name}: {str(e)}"}
        else:
            available = list(self.job_triggers.keys())
            return {
                "success": False,
                "message": f"Unknown job: {job_name}. Available: {', '.join(available) if available else 'None registered'}"
            }

    def _handle_check_status(self, command: VoiceCommand) -> Dict[str, Any]:
        """Handle status check command."""
        # This would integrate with your job monitoring system
        return {
            "success": True,
            "message": f"Status check for {command.job_name} - implement based on your monitoring system"
        }

    def _handle_get_metrics(self, command: VoiceCommand) -> Dict[str, Any]:
        """Handle metrics request command."""
        return {
            "success": True,
            "message": f"Metrics for {command.job_name} - implement based on your metrics store"
        }

    def _handle_cancel_job(self, command: VoiceCommand) -> Dict[str, Any]:
        """Handle job cancellation command."""
        return {
            "success": True,
            "message": f"Cancel request for {command.job_name} - implement based on your job management system"
        }

    def verify_slack_request(self, timestamp: str, signature: str, body: str) -> bool:
        """Verify that a request came from Slack."""
        if not self.signing_secret:
            return False

        # Check timestamp to prevent replay attacks
        current_time = time.time()
        if abs(current_time - float(timestamp)) > 60 * 5:
            return False

        # Compute signature
        sig_basestring = f"v0:{timestamp}:{body}"
        computed_sig = 'v0=' + hmac.new(
            self.signing_secret.encode(),
            sig_basestring.encode(),
            hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(computed_sig, signature)

    def _build_notification_blocks(
        self,
        message_type: MessageType,
        title: str,
        details: Dict[str, Any]
    ) -> List[Dict]:
        """Build Slack blocks for notification."""
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": title[:150]
                }
            }
        ]

        # Add details as fields
        if details:
            fields = []
            for key, value in list(details.items())[:10]:
                fields.append({
                    "type": "mrkdwn",
                    "text": f"*{key}:*\n{str(value)[:200]}"
                })

            # Slack allows max 10 fields, 2 per row
            for i in range(0, len(fields), 2):
                block_fields = fields[i:i+2]
                blocks.append({
                    "type": "section",
                    "fields": block_fields
                })

        # Add timestamp
        blocks.append({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": f"📅 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
            }]
        })

        return blocks

    def _send_message(self, message: SlackMessage) -> Dict[str, Any]:
        """Send a message to Slack using the Web API."""
        if not self.bot_token:
            return {"ok": False, "error": "No bot token configured"}

        url = "https://slack.com/api/chat.postMessage"

        payload = {
            "channel": message.channel,
            "text": message.text
        }

        if message.blocks:
            payload["blocks"] = message.blocks
        if message.attachments:
            payload["attachments"] = message.attachments
        if message.thread_ts:
            payload["thread_ts"] = message.thread_ts
            if message.reply_broadcast:
                payload["reply_broadcast"] = True

        try:
            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                url,
                data=data,
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                    "Authorization": f"Bearer {self.bot_token}"
                }
            )
            with urllib.request.urlopen(req, timeout=10) as response:
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.URLError as e:
            return {"ok": False, "error": str(e)}
        except Exception as e:
            return {"ok": False, "error": str(e)}


# Lambda handler for Slack events
def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    AWS Lambda handler for Slack events.

    Deploy this as a Lambda function and configure as Slack event endpoint.
    """
    # Handle URL verification
    body = json.loads(event.get("body", "{}"))
    if body.get("type") == "url_verification":
        return {
            "statusCode": 200,
            "body": body.get("challenge", "")
        }

    # Initialize integration
    class Config:
        slack_bot_token = os.getenv("SLACK_BOT_TOKEN")
        slack_signing_secret = os.getenv("SLACK_SIGNING_SECRET")
        slack_channel_id = os.getenv("SLACK_CHANNEL_ID")

    slack = SlackIntegration(Config())

    # Verify request
    headers = event.get("headers", {})
    timestamp = headers.get("x-slack-request-timestamp", "")
    signature = headers.get("x-slack-signature", "")

    if not slack.verify_slack_request(timestamp, signature, event.get("body", "")):
        return {"statusCode": 401, "body": "Invalid signature"}

    # Handle event
    event_type = body.get("event", {}).get("type")

    if event_type == "message":
        # Check if it's a command
        text = body.get("event", {}).get("text", "")
        user_id = body.get("event", {}).get("user", "")
        channel_id = body.get("event", {}).get("channel", "")

        command = slack.parse_voice_command(text, user_id, channel_id)
        if command:
            result = slack.handle_voice_command(command)
            slack.send_notification(
                MessageType.INFO,
                "Command Processed",
                {"Result": result.get("message", "")},
                channel=channel_id
            )

    return {"statusCode": 200, "body": "OK"}
