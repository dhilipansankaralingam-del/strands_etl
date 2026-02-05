#!/usr/bin/env python3
"""
Setup Script: Slack Integration
Validates and tests Slack webhook and bot configuration
"""

import json
import sys
import os
import requests
from datetime import datetime


def validate_webhook_url(webhook_url: str) -> bool:
    """Validate Slack webhook URL format"""
    print("\n  Validating webhook URL format...")

    if not webhook_url:
        print("    [FAIL] Webhook URL is empty")
        return False

    if not webhook_url.startswith("https://hooks.slack.com/"):
        print("    [WARN] URL doesn't match standard Slack webhook format")
        print("    [INFO] Expected: https://hooks.slack.com/services/...")

    print("    [PASS] Webhook URL format valid")
    return True


def test_webhook(webhook_url: str, send_test: bool = False) -> bool:
    """Test Slack webhook connectivity"""
    print("\n  Testing webhook connectivity...")

    if not send_test:
        print("    [SKIP] Test message not sent (use --send-test to send)")
        return True

    try:
        payload = {
            "text": ":white_check_mark: *ETL Framework - Slack Integration Test*",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "ETL Framework Integration Test"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"This is a test message from the ETL Framework.\n\n*Timestamp:* {datetime.now().isoformat()}"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": "If you received this message, your Slack integration is working!"
                        }
                    ]
                }
            ]
        }

        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        if response.status_code == 200 and response.text == "ok":
            print("    [PASS] Test message sent successfully!")
            return True
        else:
            print(f"    [FAIL] Webhook returned: {response.status_code} - {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"    [FAIL] Request failed: {e}")
        return False


def store_webhook_in_secrets_manager(webhook_url: str, secret_name: str, region: str) -> bool:
    """Store webhook URL in AWS Secrets Manager"""
    print(f"\n  Storing webhook in Secrets Manager: {secret_name}")

    try:
        import boto3
        from botocore.exceptions import ClientError

        secrets_client = boto3.client('secretsmanager', region_name=region)

        secret_value = json.dumps({
            "webhook_url": webhook_url,
            "created_at": datetime.now().isoformat()
        })

        try:
            secrets_client.create_secret(
                Name=secret_name,
                Description='Slack webhook URL for ETL Framework',
                SecretString=secret_value,
                Tags=[
                    {'Key': 'Project', 'Value': 'ETL-Framework'},
                    {'Key': 'Component', 'Value': 'Slack'}
                ]
            )
            print(f"    [DONE] Secret created: {secret_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceExistsException':
                secrets_client.put_secret_value(
                    SecretId=secret_name,
                    SecretString=secret_value
                )
                print(f"    [DONE] Secret updated: {secret_name}")
            else:
                raise

        return True

    except ImportError:
        print("    [SKIP] boto3 not available - cannot store in Secrets Manager")
        return False
    except Exception as e:
        print(f"    [FAIL] Failed to store secret: {e}")
        return False


def generate_config_snippet(webhook_url: str = None, secret_arn: str = None):
    """Generate JSON config snippet for notifications"""
    print("\n  JSON Configuration Snippet:")
    print("  " + "-" * 50)

    url_value = secret_arn if secret_arn else (webhook_url if webhook_url else "${SLACK_WEBHOOK_URL}")

    config = {
        "notifications": {
            "enabled": "Y",
            "slack": {
                "enabled": "Y",
                "webhook_url": url_value,
                "channel": "#etl-alerts"
            },
            "preferences": {
                "on_start": "N",
                "on_success": "Y",
                "on_failure": "Y",
                "on_dq_failure": "Y"
            }
        }
    }

    print(json.dumps(config, indent=2))


def print_setup_instructions():
    """Print Slack app setup instructions"""
    print("""
================================================================================
SLACK INTEGRATION SETUP INSTRUCTIONS
================================================================================

STEP 1: Create Slack App
------------------------
1. Go to https://api.slack.com/apps
2. Click "Create New App" > "From scratch"
3. Name: "ETL Framework Alerts"
4. Select your workspace
5. Click "Create App"

STEP 2: Enable Incoming Webhooks
--------------------------------
1. In your app settings, go to "Incoming Webhooks"
2. Toggle "Activate Incoming Webhooks" to ON
3. Click "Add New Webhook to Workspace"
4. Select the channel for ETL alerts (e.g., #etl-alerts)
5. Click "Allow"
6. Copy the Webhook URL

STEP 3: Configure Permissions (Optional - for Bot features)
-----------------------------------------------------------
1. Go to "OAuth & Permissions"
2. Under "Scopes" > "Bot Token Scopes", add:
   - chat:write
   - chat:write.public
   - files:write (for attachments)
3. Install the app to your workspace
4. Copy the Bot User OAuth Token

STEP 4: Store Credentials Securely
----------------------------------
Option A: Environment Variable
  export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/..."

Option B: AWS Secrets Manager
  python setup_slack.py --webhook-url "..." --store-secret --region us-east-1

Option C: JSON Config with placeholder
  "webhook_url": "${SLACK_WEBHOOK_URL}"

================================================================================
""")


def setup_slack(
    webhook_url: str = None,
    send_test: bool = False,
    store_secret: bool = False,
    secret_name: str = 'etl-framework/slack-webhook',
    region: str = 'us-east-1'
):
    """Main setup function"""
    print("=" * 60)
    print("SETUP: Slack Integration")
    print("=" * 60)

    if not webhook_url:
        webhook_url = os.environ.get('SLACK_WEBHOOK_URL', '')

    if not webhook_url:
        print("\n  [INFO] No webhook URL provided")
        print_setup_instructions()
        return False

    results = []

    # Validate
    results.append(validate_webhook_url(webhook_url))

    # Test
    results.append(test_webhook(webhook_url, send_test))

    # Store in Secrets Manager
    if store_secret:
        results.append(store_webhook_in_secrets_manager(webhook_url, secret_name, region))

    # Generate config
    secret_arn = f"arn:aws:secretsmanager:{region}:*:secret:{secret_name}" if store_secret else None
    generate_config_snippet(webhook_url if not store_secret else None, secret_arn)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    if all(results):
        print("  [SUCCESS] Slack integration setup complete!")
        return True
    else:
        print("  [PARTIAL] Some steps failed - review above")
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Setup Slack integration for ETL Framework')
    parser.add_argument('--webhook-url', type=str, help='Slack webhook URL')
    parser.add_argument('--send-test', action='store_true', help='Send test message')
    parser.add_argument('--store-secret', action='store_true', help='Store in AWS Secrets Manager')
    parser.add_argument('--secret-name', type=str, default='etl-framework/slack-webhook',
                        help='Secret name in Secrets Manager')
    parser.add_argument('--region', type=str, default='us-east-1', help='AWS region')
    parser.add_argument('--instructions', action='store_true', help='Show setup instructions')

    args = parser.parse_args()

    if args.instructions:
        print_setup_instructions()
        sys.exit(0)

    success = setup_slack(
        webhook_url=args.webhook_url,
        send_test=args.send_test,
        store_secret=args.store_secret,
        secret_name=args.secret_name,
        region=args.region
    )

    sys.exit(0 if success else 1)
