"""
Enterprise ETL Framework - Integrations
=======================================

All integration modules for notifications and dashboards.
"""

from .slack_integration import SlackIntegration
from .teams_integration import TeamsIntegration
from .email_integration import EmailIntegration

__all__ = [
    'SlackIntegration',
    'TeamsIntegration',
    'EmailIntegration'
]
