"""
Slack Integration - Bot for ETL operations with voice command support
Provides notifications, interactive controls, and voice-triggered ETL jobs
"""

import json
import logging
import re
import os
import tempfile
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MessageType(Enum):
    """Types of Slack messages."""
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"
    PROGRESS = "progress"


class CommandType(Enum):
    """Types of bot commands."""
    RUN_JOB = "run_job"
    STATUS = "status"
    LIST_JOBS = "list_jobs"
    CANCEL_JOB = "cancel_job"
    GET_LOGS = "get_logs"
    DATA_QUALITY = "data_quality"
    HELP = "help"


@dataclass
class SlackMessage:
    """A Slack message to send."""
    channel: str
    text: str
    blocks: Optional[List[Dict]] = None
    attachments: Optional[List[Dict]] = None
    thread_ts: Optional[str] = None


@dataclass
class VoiceCommand:
    """A parsed voice command."""
    raw_text: str
    command_type: CommandType
    job_name: Optional[str] = None
    parameters: Dict[str, Any] = None
    confidence: float = 1.0


class SlackIntegration:
    """
    Slack integration for sending notifications and alerts.

    Features:
    - Rich message formatting with blocks
    - Job status updates
    - Data quality reports
    - Error alerts with context
    - Interactive buttons
    """

    # Color codes for message types
    COLORS = {
        MessageType.INFO: "#1a73e8",
        MessageType.SUCCESS: "#28a745",
        MessageType.WARNING: "#ffc107",
        MessageType.ERROR: "#dc3545",
        MessageType.PROGRESS: "#17a2b8"
    }

    def __init__(self, webhook_url: str = None, bot_token: str = None):
        """
        Initialize Slack integration.

        Args:
            webhook_url: Slack webhook URL for sending messages
            bot_token: Slack bot token for API access
        """
        self.webhook_url = webhook_url or os.environ.get('SLACK_WEBHOOK_URL')
        self.bot_token = bot_token or os.environ.get('SLACK_BOT_TOKEN')

        # Use requests if available, otherwise use urllib
        try:
            import requests
            self._http_client = 'requests'
        except ImportError:
            self._http_client = 'urllib'

    def _send_request(self, url: str, data: Dict) -> Dict:
        """Send HTTP request to Slack API."""
        if self._http_client == 'requests':
            import requests
            headers = {'Content-Type': 'application/json'}
            if self.bot_token and 'api.slack.com' in url:
                headers['Authorization'] = f'Bearer {self.bot_token}'
            response = requests.post(url, json=data, headers=headers)
            return response.json() if response.text else {}
        else:
            import urllib.request
            import urllib.error
            headers = {'Content-Type': 'application/json'}
            if self.bot_token and 'api.slack.com' in url:
                headers['Authorization'] = f'Bearer {self.bot_token}'
            req = urllib.request.Request(
                url,
                data=json.dumps(data).encode('utf-8'),
                headers=headers
            )
            try:
                with urllib.request.urlopen(req) as response:
                    return json.loads(response.read().decode())
            except urllib.error.HTTPError as e:
                return {'ok': False, 'error': str(e)}

    def send_message(self, message: SlackMessage) -> bool:
        """
        Send a message to Slack.

        Args:
            message: SlackMessage to send

        Returns:
            True if successful
        """
        if not self.webhook_url and not self.bot_token:
            logger.warning("No Slack credentials configured")
            return False

        payload = {'text': message.text}

        if message.channel:
            payload['channel'] = message.channel
        if message.blocks:
            payload['blocks'] = message.blocks
        if message.attachments:
            payload['attachments'] = message.attachments
        if message.thread_ts:
            payload['thread_ts'] = message.thread_ts

        try:
            if self.bot_token:
                url = 'https://slack.com/api/chat.postMessage'
            else:
                url = self.webhook_url

            result = self._send_request(url, payload)
            return result.get('ok', True)  # Webhook returns empty on success

        except Exception as e:
            logger.error(f"Failed to send Slack message: {e}")
            return False

    def send_job_notification(self, job_name: str, status: str,
                               channel: str, details: Dict[str, Any] = None) -> bool:
        """
        Send a job status notification.

        Args:
            job_name: Name of the ETL job
            status: Job status (started, completed, failed, etc.)
            channel: Slack channel to post to
            details: Additional details to include

        Returns:
            True if successful
        """
        # Determine message type
        if status.lower() in ['completed', 'succeeded', 'success']:
            msg_type = MessageType.SUCCESS
            emoji = ":white_check_mark:"
        elif status.lower() in ['failed', 'error']:
            msg_type = MessageType.ERROR
            emoji = ":x:"
        elif status.lower() in ['running', 'started']:
            msg_type = MessageType.PROGRESS
            emoji = ":hourglass_flowing_sand:"
        elif status.lower() in ['warning', 'timeout']:
            msg_type = MessageType.WARNING
            emoji = ":warning:"
        else:
            msg_type = MessageType.INFO
            emoji = ":information_source:"

        # Build blocks
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} ETL Job: {job_name}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Status:*\n{status}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Time:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    }
                ]
            }
        ]

        # Add details if provided
        if details:
            detail_fields = []
            for key, value in list(details.items())[:10]:  # Limit to 10 fields
                detail_fields.append({
                    "type": "mrkdwn",
                    "text": f"*{key}:*\n{value}"
                })

            if detail_fields:
                blocks.append({
                    "type": "section",
                    "fields": detail_fields[:10]
                })

        # Add context
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Enterprise ETL Framework | <https://console.aws.amazon.com|AWS Console>"
                }
            ]
        })

        message = SlackMessage(
            channel=channel,
            text=f"ETL Job {job_name}: {status}",
            blocks=blocks,
            attachments=[{"color": self.COLORS[msg_type]}]
        )

        return self.send_message(message)

    def send_data_quality_report(self, report: Dict[str, Any], channel: str) -> bool:
        """
        Send a data quality report to Slack.

        Args:
            report: Data quality report dict
            channel: Slack channel

        Returns:
            True if successful
        """
        score = report.get('overall_score', 0)
        passed = report.get('passed_rules', 0)
        failed = report.get('failed_rules', 0)

        # Determine status color
        if score >= 0.9:
            color = self.COLORS[MessageType.SUCCESS]
            emoji = ":white_check_mark:"
        elif score >= 0.7:
            color = self.COLORS[MessageType.WARNING]
            emoji = ":warning:"
        else:
            color = self.COLORS[MessageType.ERROR]
            emoji = ":x:"

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} Data Quality Report",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Table:*\n{report.get('table_name', 'N/A')}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Score:*\n{score:.0%}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Passed:*\n{passed} rules"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Failed:*\n{failed} rules"
                    }
                ]
            }
        ]

        # Add failed rules if any
        failed_rules = [r for r in report.get('results', []) if not r.get('passed')]
        if failed_rules[:5]:  # Show up to 5 failed rules
            failure_text = "\n".join([
                f"- {r.get('rule', {}).get('name', 'Unknown')}: {r.get('pass_rate', 0):.1%}"
                for r in failed_rules[:5]
            ])
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Failed Rules:*\n{failure_text}"
                }
            })

        # Add recommendations
        recommendations = report.get('recommendations', [])
        if recommendations:
            rec_text = "\n".join([f"- {r}" for r in recommendations[:3]])
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Recommendations:*\n{rec_text}"
                }
            })

        message = SlackMessage(
            channel=channel,
            text=f"Data Quality Report: {score:.0%} score",
            blocks=blocks,
            attachments=[{"color": color}]
        )

        return self.send_message(message)

    def send_error_alert(self, job_name: str, error: str,
                          channel: str, context: Dict[str, Any] = None) -> bool:
        """
        Send an error alert with context and suggested actions.

        Args:
            job_name: Failed job name
            error: Error message
            channel: Slack channel
            context: Additional context (stack trace, suggestions, etc.)

        Returns:
            True if successful
        """
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":rotating_light: ETL Job Failed",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Job:*\n{job_name}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Time:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    }
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:*\n```{error[:1000]}```"
                }
            }
        ]

        # Add suggested actions if available
        if context and context.get('suggestions'):
            suggestions = context['suggestions']
            if isinstance(suggestions, list):
                suggestions = "\n".join([f"- {s}" for s in suggestions])
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Suggested Actions:*\n{suggestions}"
                }
            })

        # Add action buttons
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View Logs"},
                    "style": "primary",
                    "action_id": f"view_logs_{job_name}",
                    "url": context.get('logs_url', 'https://console.aws.amazon.com/cloudwatch')
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Retry Job"},
                    "action_id": f"retry_job_{job_name}"
                }
            ]
        })

        message = SlackMessage(
            channel=channel,
            text=f"ETL Job {job_name} Failed: {error[:100]}",
            blocks=blocks,
            attachments=[{"color": self.COLORS[MessageType.ERROR]}]
        )

        return self.send_message(message)


class SlackBot:
    """
    Slack Bot for interactive ETL operations.

    Features:
    - Text commands (run job, check status, etc.)
    - Voice command support (via transcription)
    - Interactive responses
    - Job management
    """

    # Command patterns
    COMMAND_PATTERNS = {
        CommandType.RUN_JOB: [
            r'run\s+(?:job\s+)?["\']?(\w+)["\']?',
            r'execute\s+(?:job\s+)?["\']?(\w+)["\']?',
            r'start\s+(?:job\s+)?["\']?(\w+)["\']?',
            r'trigger\s+(?:job\s+)?["\']?(\w+)["\']?',
        ],
        CommandType.STATUS: [
            r'status\s+(?:of\s+)?(?:job\s+)?["\']?(\w+)["\']?',
            r'check\s+(?:job\s+)?["\']?(\w+)["\']?',
            r'how\s+is\s+(?:job\s+)?["\']?(\w+)["\']?',
        ],
        CommandType.LIST_JOBS: [
            r'list\s+(?:all\s+)?jobs',
            r'show\s+(?:all\s+)?jobs',
            r'what\s+jobs',
        ],
        CommandType.CANCEL_JOB: [
            r'cancel\s+(?:job\s+)?["\']?(\w+)["\']?',
            r'stop\s+(?:job\s+)?["\']?(\w+)["\']?',
            r'kill\s+(?:job\s+)?["\']?(\w+)["\']?',
        ],
        CommandType.GET_LOGS: [
            r'logs?\s+(?:for\s+)?(?:job\s+)?["\']?(\w+)["\']?',
            r'show\s+logs?\s+(?:for\s+)?["\']?(\w+)["\']?',
        ],
        CommandType.DATA_QUALITY: [
            r'(?:data\s+)?quality\s+(?:check\s+)?(?:for\s+)?["\']?(\w+)["\']?',
            r'validate\s+(?:data\s+)?(?:for\s+)?["\']?(\w+)["\']?',
        ],
        CommandType.HELP: [
            r'^help$',
            r'what\s+can\s+you\s+do',
            r'how\s+do\s+i',
        ]
    }

    def __init__(self, region: str = 'us-east-1',
                 glue_client=None,
                 slack_integration: SlackIntegration = None):
        """
        Initialize Slack Bot.

        Args:
            region: AWS region
            glue_client: Optional pre-configured Glue client
            slack_integration: SlackIntegration instance for responses
        """
        self.region = region
        self.glue_client = glue_client or boto3.client('glue', region_name=region)
        self.slack = slack_integration or SlackIntegration()

        # For voice transcription
        self.transcribe_client = None
        try:
            self.transcribe_client = boto3.client('transcribe', region_name=region)
        except Exception:
            logger.warning("Transcribe client not available for voice commands")

    def parse_command(self, text: str) -> Optional[VoiceCommand]:
        """
        Parse a text command into a VoiceCommand.

        Args:
            text: Raw command text

        Returns:
            VoiceCommand if parsed, None otherwise
        """
        text_lower = text.lower().strip()

        for cmd_type, patterns in self.COMMAND_PATTERNS.items():
            for pattern in patterns:
                match = re.search(pattern, text_lower)
                if match:
                    job_name = match.group(1) if match.groups() else None
                    return VoiceCommand(
                        raw_text=text,
                        command_type=cmd_type,
                        job_name=job_name,
                        parameters={},
                        confidence=1.0
                    )

        return None

    def transcribe_voice(self, audio_url: str) -> Optional[str]:
        """
        Transcribe voice audio to text using AWS Transcribe.

        Args:
            audio_url: URL to audio file (must be accessible)

        Returns:
            Transcribed text or None
        """
        if not self.transcribe_client:
            logger.error("Transcribe client not available")
            return None

        try:
            import time
            job_name = f"etl-voice-{int(time.time())}"

            # Start transcription
            self.transcribe_client.start_transcription_job(
                TranscriptionJobName=job_name,
                Media={'MediaFileUri': audio_url},
                MediaFormat='mp3',
                LanguageCode='en-US'
            )

            # Wait for completion (max 60 seconds)
            for _ in range(30):
                result = self.transcribe_client.get_transcription_job(
                    TranscriptionJobName=job_name
                )
                status = result['TranscriptionJob']['TranscriptionJobStatus']

                if status == 'COMPLETED':
                    transcript_uri = result['TranscriptionJob']['Transcript']['TranscriptFileUri']
                    # Fetch transcript
                    import urllib.request
                    with urllib.request.urlopen(transcript_uri) as response:
                        transcript_data = json.loads(response.read())
                        return transcript_data['results']['transcripts'][0]['transcript']

                elif status == 'FAILED':
                    logger.error("Transcription failed")
                    return None

                time.sleep(2)

            logger.error("Transcription timed out")
            return None

        except Exception as e:
            logger.error(f"Voice transcription failed: {e}")
            return None

    def handle_voice_command(self, audio_url: str, channel: str) -> Dict[str, Any]:
        """
        Handle a voice command from Slack.

        Args:
            audio_url: URL to audio file
            channel: Slack channel for response

        Returns:
            Command result
        """
        # Transcribe audio
        text = self.transcribe_voice(audio_url)
        if not text:
            return {
                'success': False,
                'error': 'Could not transcribe voice command'
            }

        logger.info(f"Transcribed voice command: {text}")

        # Parse and execute
        return self.handle_text_command(text, channel)

    def handle_text_command(self, text: str, channel: str) -> Dict[str, Any]:
        """
        Handle a text command.

        Args:
            text: Command text
            channel: Slack channel for response

        Returns:
            Command result
        """
        command = self.parse_command(text)

        if not command:
            self.slack.send_message(SlackMessage(
                channel=channel,
                text=f"I didn't understand: `{text}`. Type `help` for available commands."
            ))
            return {'success': False, 'error': 'Unknown command'}

        # Execute command
        if command.command_type == CommandType.RUN_JOB:
            return self._handle_run_job(command, channel)
        elif command.command_type == CommandType.STATUS:
            return self._handle_status(command, channel)
        elif command.command_type == CommandType.LIST_JOBS:
            return self._handle_list_jobs(channel)
        elif command.command_type == CommandType.CANCEL_JOB:
            return self._handle_cancel_job(command, channel)
        elif command.command_type == CommandType.GET_LOGS:
            return self._handle_get_logs(command, channel)
        elif command.command_type == CommandType.DATA_QUALITY:
            return self._handle_data_quality(command, channel)
        elif command.command_type == CommandType.HELP:
            return self._handle_help(channel)

        return {'success': False, 'error': 'Command not implemented'}

    def _handle_run_job(self, command: VoiceCommand, channel: str) -> Dict[str, Any]:
        """Handle run job command."""
        job_name = command.job_name
        if not job_name:
            self.slack.send_message(SlackMessage(
                channel=channel,
                text="Please specify a job name. Example: `run job customer_360_etl`"
            ))
            return {'success': False, 'error': 'No job name specified'}

        try:
            response = self.glue_client.start_job_run(JobName=job_name)
            run_id = response['JobRunId']

            self.slack.send_job_notification(
                job_name=job_name,
                status='Started',
                channel=channel,
                details={'Run ID': run_id}
            )

            return {
                'success': True,
                'job_name': job_name,
                'run_id': run_id
            }

        except ClientError as e:
            error_msg = str(e)
            self.slack.send_message(SlackMessage(
                channel=channel,
                text=f":x: Failed to start job `{job_name}`: {error_msg}"
            ))
            return {'success': False, 'error': error_msg}

    def _handle_status(self, command: VoiceCommand, channel: str) -> Dict[str, Any]:
        """Handle status check command."""
        job_name = command.job_name
        if not job_name:
            self.slack.send_message(SlackMessage(
                channel=channel,
                text="Please specify a job name. Example: `status customer_360_etl`"
            ))
            return {'success': False, 'error': 'No job name specified'}

        try:
            response = self.glue_client.get_job_runs(
                JobName=job_name,
                MaxResults=1
            )

            if not response.get('JobRuns'):
                self.slack.send_message(SlackMessage(
                    channel=channel,
                    text=f"No runs found for job `{job_name}`"
                ))
                return {'success': True, 'status': 'No runs'}

            run = response['JobRuns'][0]
            status = run.get('JobRunState', 'Unknown')
            started = run.get('StartedOn', 'N/A')
            execution_time = run.get('ExecutionTime', 0)

            self.slack.send_job_notification(
                job_name=job_name,
                status=status,
                channel=channel,
                details={
                    'Started': str(started),
                    'Duration': f"{execution_time}s" if execution_time else 'N/A',
                    'Run ID': run.get('Id', 'N/A')
                }
            )

            return {'success': True, 'status': status}

        except ClientError as e:
            error_msg = str(e)
            self.slack.send_message(SlackMessage(
                channel=channel,
                text=f":x: Failed to get status for `{job_name}`: {error_msg}"
            ))
            return {'success': False, 'error': error_msg}

    def _handle_list_jobs(self, channel: str) -> Dict[str, Any]:
        """Handle list jobs command."""
        try:
            response = self.glue_client.get_jobs(MaxResults=20)
            jobs = response.get('Jobs', [])

            if not jobs:
                self.slack.send_message(SlackMessage(
                    channel=channel,
                    text="No jobs found in this account/region."
                ))
                return {'success': True, 'jobs': []}

            job_list = "\n".join([
                f"- `{job['Name']}` ({job.get('Command', {}).get('Name', 'unknown')})"
                for job in jobs
            ])

            self.slack.send_message(SlackMessage(
                channel=channel,
                text=f"*Available ETL Jobs:*\n{job_list}"
            ))

            return {'success': True, 'jobs': [j['Name'] for j in jobs]}

        except ClientError as e:
            self.slack.send_message(SlackMessage(
                channel=channel,
                text=f":x: Failed to list jobs: {e}"
            ))
            return {'success': False, 'error': str(e)}

    def _handle_cancel_job(self, command: VoiceCommand, channel: str) -> Dict[str, Any]:
        """Handle cancel job command."""
        job_name = command.job_name
        if not job_name:
            return {'success': False, 'error': 'No job name specified'}

        try:
            # Get running job run
            response = self.glue_client.get_job_runs(
                JobName=job_name,
                MaxResults=1
            )

            if not response.get('JobRuns'):
                self.slack.send_message(SlackMessage(
                    channel=channel,
                    text=f"No running jobs found for `{job_name}`"
                ))
                return {'success': False, 'error': 'No running jobs'}

            run = response['JobRuns'][0]
            if run.get('JobRunState') not in ['RUNNING', 'STARTING', 'WAITING']:
                self.slack.send_message(SlackMessage(
                    channel=channel,
                    text=f"Job `{job_name}` is not currently running (status: {run.get('JobRunState')})"
                ))
                return {'success': False, 'error': 'Job not running'}

            # Stop the job
            self.glue_client.batch_stop_job_run(
                JobName=job_name,
                JobRunIds=[run['Id']]
            )

            self.slack.send_message(SlackMessage(
                channel=channel,
                text=f":octagonal_sign: Cancelled job `{job_name}` (run: {run['Id']})"
            ))

            return {'success': True, 'cancelled_run': run['Id']}

        except ClientError as e:
            self.slack.send_message(SlackMessage(
                channel=channel,
                text=f":x: Failed to cancel job: {e}"
            ))
            return {'success': False, 'error': str(e)}

    def _handle_get_logs(self, command: VoiceCommand, channel: str) -> Dict[str, Any]:
        """Handle get logs command."""
        job_name = command.job_name
        if not job_name:
            return {'success': False, 'error': 'No job name specified'}

        # Provide CloudWatch logs link
        logs_url = (
            f"https://console.aws.amazon.com/cloudwatch/home?"
            f"region={self.region}#logsV2:log-groups/log-group/"
            f"%2Faws-glue%2Fjobs%2F{job_name}"
        )

        self.slack.send_message(SlackMessage(
            channel=channel,
            text=f"*Logs for {job_name}:*\n<{logs_url}|View in CloudWatch>"
        ))

        return {'success': True, 'logs_url': logs_url}

    def _handle_data_quality(self, command: VoiceCommand, channel: str) -> Dict[str, Any]:
        """Handle data quality check command."""
        table_name = command.job_name  # Reusing job_name field for table
        if not table_name:
            return {'success': False, 'error': 'No table name specified'}

        # This would integrate with the DataQualityAgent
        self.slack.send_message(SlackMessage(
            channel=channel,
            text=f":mag: Running data quality check for `{table_name}`... This may take a few minutes."
        ))

        # Placeholder - actual integration would call DataQualityAgent
        return {'success': True, 'status': 'initiated'}

    def _handle_help(self, channel: str) -> Dict[str, Any]:
        """Handle help command."""
        help_text = """
*Available Commands:*

:arrow_forward: *Run a job:*
`run job <job_name>` - Start an ETL job

:mag: *Check status:*
`status <job_name>` - Check job status

:clipboard: *List jobs:*
`list jobs` - Show all available jobs

:octagonal_sign: *Cancel job:*
`cancel <job_name>` - Stop a running job

:page_facing_up: *View logs:*
`logs <job_name>` - Get CloudWatch logs link

:white_check_mark: *Data quality:*
`quality <table_name>` - Run data quality check

:microphone: *Voice commands:*
Send a voice message with any of the above commands!
"""

        self.slack.send_message(SlackMessage(
            channel=channel,
            text=help_text
        ))

        return {'success': True}


# Lambda handler for Slack events
def lambda_handler(event, context):
    """
    AWS Lambda handler for Slack events.
    Deploy this as a Lambda function and configure as Slack event endpoint.
    """
    body = json.loads(event.get('body', '{}'))

    # Handle Slack URL verification
    if body.get('type') == 'url_verification':
        return {
            'statusCode': 200,
            'body': body.get('challenge')
        }

    # Handle events
    if body.get('type') == 'event_callback':
        slack_event = body.get('event', {})
        event_type = slack_event.get('type')

        # Ignore bot messages
        if slack_event.get('bot_id'):
            return {'statusCode': 200}

        bot = SlackBot()

        if event_type == 'message':
            text = slack_event.get('text', '')
            channel = slack_event.get('channel')

            # Check for voice message (audio file)
            files = slack_event.get('files', [])
            audio_file = next((f for f in files if f.get('mimetype', '').startswith('audio/')), None)

            if audio_file:
                result = bot.handle_voice_command(audio_file.get('url_private'), channel)
            else:
                result = bot.handle_text_command(text, channel)

            return {
                'statusCode': 200,
                'body': json.dumps(result)
            }

    return {'statusCode': 200}


# Example usage
if __name__ == '__main__':
    # Test Slack integration
    slack = SlackIntegration()

    # Test bot command parsing
    bot = SlackBot()

    test_commands = [
        "run job customer_360_etl",
        "status of customer_360_etl",
        "list all jobs",
        "cancel job failed_etl",
        "logs for my_job",
        "data quality check for orders",
        "help"
    ]

    for cmd in test_commands:
        parsed = bot.parse_command(cmd)
        if parsed:
            print(f"'{cmd}' -> {parsed.command_type.value}: {parsed.job_name}")
        else:
            print(f"'{cmd}' -> Not recognized")
