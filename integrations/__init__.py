"""
Enterprise ETL Integrations
Slack, Teams, Email, API Gateway, Streamlit, and Notification Manager
"""

from .slack_integration import SlackIntegration, SlackBot
from .email_reporter import EmailReporter
from .streamlit_dashboard import StreamlitDashboard
from .teams_integration import TeamsIntegration, TeamsConfig, TeamsIntegrationFactory
from .api_gateway import APIGatewayIntegration, APIConfig, ETLAPIHandlers, create_api_handler
from .notification_manager import (
    NotificationManager,
    NotificationConfig,
    NotificationType,
    create_notification_manager
)

__all__ = [
    # Slack
    'SlackIntegration',
    'SlackBot',
    # Teams
    'TeamsIntegration',
    'TeamsConfig',
    'TeamsIntegrationFactory',
    # Email
    'EmailReporter',
    # API Gateway
    'APIGatewayIntegration',
    'APIConfig',
    'ETLAPIHandlers',
    'create_api_handler',
    # Notification Manager
    'NotificationManager',
    'NotificationConfig',
    'NotificationType',
    'create_notification_manager',
    # Dashboard
    'StreamlitDashboard'
]
