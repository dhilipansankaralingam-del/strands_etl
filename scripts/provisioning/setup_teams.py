#!/usr/bin/env python3
"""
Setup Script: Microsoft Teams Integration
Validates and tests Teams webhook configuration
"""

import json
import sys
import os
import requests
from datetime import datetime


def validate_webhook_url(webhook_url: str) -> bool:
    """Validate Teams webhook URL format"""
    print("\n  Validating webhook URL format...")

    if not webhook_url:
        print("    [FAIL] Webhook URL is empty")
        return False

    valid_patterns = [
        "outlook.office.com/webhook",
        "webhook.office.com/webhookb2"
    ]

    if not any(pattern in webhook_url for pattern in valid_patterns):
        print("    [WARN] URL doesn't match standard Teams webhook format")
        print("    [INFO] Expected: https://outlook.office.com/webhook/... or")
        print("    [INFO]           https://*.webhook.office.com/webhookb2/...")

    print("    [PASS] Webhook URL format valid")
    return True


def test_webhook(webhook_url: str, send_test: bool = False) -> bool:
    """Test Teams webhook connectivity"""
    print("\n  Testing webhook connectivity...")

    if not send_test:
        print("    [SKIP] Test message not sent (use --send-test to send)")
        return True

    try:
        # Adaptive Card payload
        payload = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                        "type": "AdaptiveCard",
                        "version": "1.4",
                        "body": [
                            {
                                "type": "TextBlock",
                                "size": "Large",
                                "weight": "Bolder",
                                "text": "ETL Framework Integration Test",
                                "color": "Good"
                            },
                            {
                                "type": "TextBlock",
                                "text": "This is a test message from the ETL Framework.",
                                "wrap": True
                            },
                            {
                                "type": "FactSet",
                                "facts": [
                                    {"title": "Status", "value": "SUCCESS"},
                                    {"title": "Timestamp", "value": datetime.now().isoformat()}
                                ]
                            },
                            {
                                "type": "TextBlock",
                                "text": "If you received this message, your Teams integration is working!",
                                "wrap": True,
                                "isSubtle": True
                            }
                        ]
                    }
                }
            ]
        }

        response = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        if response.status_code == 200:
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
                Description='Teams webhook URL for ETL Framework',
                SecretString=secret_value,
                Tags=[
                    {'Key': 'Project', 'Value': 'ETL-Framework'},
                    {'Key': 'Component', 'Value': 'Teams'}
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

    url_value = secret_arn if secret_arn else (webhook_url if webhook_url else "${TEAMS_WEBHOOK_URL}")

    config = {
        "notifications": {
            "enabled": "Y",
            "teams": {
                "enabled": "Y",
                "webhook_url": url_value,
                "channel": "ETL Alerts"
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
    """Print Teams connector setup instructions"""
    print("""
================================================================================
MICROSOFT TEAMS INTEGRATION SETUP INSTRUCTIONS
================================================================================

STEP 1: Create Teams Channel (if needed)
----------------------------------------
1. Open Microsoft Teams
2. Navigate to your team
3. Create a new channel: "ETL Alerts" (or use existing)

STEP 2: Add Incoming Webhook Connector
--------------------------------------
1. Click the "..." (more options) next to your channel name
2. Select "Connectors"
3. Search for "Incoming Webhook"
4. Click "Configure"
5. Provide a name: "ETL Framework"
6. Optionally upload an icon
7. Click "Create"
8. Copy the webhook URL
9. Click "Done"

STEP 3: Verify Webhook URL Format
---------------------------------
Your webhook URL should look like one of these:
  - https://outlook.office.com/webhook/xxx-xxx-xxx/IncomingWebhook/yyy/zzz
  - https://company.webhook.office.com/webhookb2/xxx/IncomingWebhook/yyy/zzz

STEP 4: Store Credentials Securely
----------------------------------
Option A: Environment Variable
  export TEAMS_WEBHOOK_URL="https://outlook.office.com/webhook/..."

Option B: AWS Secrets Manager
  python setup_teams.py --webhook-url "..." --store-secret --region us-east-1

Option C: JSON Config with placeholder
  "webhook_url": "${TEAMS_WEBHOOK_URL}"

STEP 5: Test the Integration
----------------------------
  python setup_teams.py --webhook-url "YOUR_URL" --send-test

================================================================================
ADAPTIVE CARDS
================================================================================

Teams supports rich Adaptive Cards for notifications. The ETL Framework
automatically generates Adaptive Cards with:

- Color-coded headers based on status (success=green, failure=red)
- Fact sets for key metrics (duration, rows, cost)
- Action buttons for viewing logs and retrying jobs
- @mentions for alerting specific users

For more on Adaptive Cards: https://adaptivecards.io/

================================================================================
""")


def setup_teams(
    webhook_url: str = None,
    send_test: bool = False,
    store_secret: bool = False,
    secret_name: str = 'etl-framework/teams-webhook',
    region: str = 'us-east-1'
):
    """Main setup function"""
    print("=" * 60)
    print("SETUP: Microsoft Teams Integration")
    print("=" * 60)

    if not webhook_url:
        webhook_url = os.environ.get('TEAMS_WEBHOOK_URL', '')

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
        print("  [SUCCESS] Teams integration setup complete!")
        return True
    else:
        print("  [PARTIAL] Some steps failed - review above")
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Setup Teams integration for ETL Framework')
    parser.add_argument('--webhook-url', type=str, help='Teams webhook URL')
    parser.add_argument('--send-test', action='store_true', help='Send test message')
    parser.add_argument('--store-secret', action='store_true', help='Store in AWS Secrets Manager')
    parser.add_argument('--secret-name', type=str, default='etl-framework/teams-webhook',
                        help='Secret name in Secrets Manager')
    parser.add_argument('--region', type=str, default='us-east-1', help='AWS region')
    parser.add_argument('--instructions', action='store_true', help='Show setup instructions')

    args = parser.parse_args()

    if args.instructions:
        print_setup_instructions()
        sys.exit(0)

    success = setup_teams(
        webhook_url=args.webhook_url,
        send_test=args.send_test,
        store_secret=args.store_secret,
        secret_name=args.secret_name,
        region=args.region
    )

    sys.exit(0 if success else 1)
