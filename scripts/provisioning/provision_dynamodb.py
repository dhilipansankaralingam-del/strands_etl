#!/usr/bin/env python3
"""
Provisioning Script: DynamoDB Tables for ETL Audit
Creates DynamoDB tables for storing ETL audit records
"""

import boto3
import sys
import time
from botocore.exceptions import ClientError


def create_etl_run_audit_table(dynamodb):
    """Create ETL Run Audit table"""
    table_name = 'etl_run_audit'

    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'run_id', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'run_id', 'AttributeType': 'S'},
                {'AttributeName': 'job_name', 'AttributeType': 'S'},
                {'AttributeName': 'status', 'AttributeType': 'S'},
                {'AttributeName': 'started_date', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'job_name-index',
                    'KeySchema': [
                        {'AttributeName': 'job_name', 'KeyType': 'HASH'},
                        {'AttributeName': 'started_date', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                },
                {
                    'IndexName': 'status-index',
                    'KeySchema': [
                        {'AttributeName': 'status', 'KeyType': 'HASH'},
                        {'AttributeName': 'started_date', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            },
            Tags=[
                {'Key': 'Project', 'Value': 'ETL-Framework'},
                {'Key': 'Component', 'Value': 'Audit'}
            ]
        )

        print(f"  Creating table: {table_name}")
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print(f"  [DONE] Table {table_name} created")
        return True

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"  [SKIP] Table {table_name} already exists")
            return True
        else:
            print(f"  [ERROR] Failed to create {table_name}: {e}")
            return False


def create_dq_audit_table(dynamodb):
    """Create Data Quality Audit table"""
    table_name = 'etl_dq_audit'

    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'dq_id', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'dq_id', 'AttributeType': 'S'},
                {'AttributeName': 'run_id', 'AttributeType': 'S'},
                {'AttributeName': 'job_name', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'run_id-index',
                    'KeySchema': [
                        {'AttributeName': 'run_id', 'KeyType': 'HASH'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                },
                {
                    'IndexName': 'job_name-index',
                    'KeySchema': [
                        {'AttributeName': 'job_name', 'KeyType': 'HASH'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            },
            Tags=[
                {'Key': 'Project', 'Value': 'ETL-Framework'},
                {'Key': 'Component', 'Value': 'DataQuality'}
            ]
        )

        print(f"  Creating table: {table_name}")
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print(f"  [DONE] Table {table_name} created")
        return True

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"  [SKIP] Table {table_name} already exists")
            return True
        else:
            print(f"  [ERROR] Failed to create {table_name}: {e}")
            return False


def create_recommendations_table(dynamodb):
    """Create Platform Recommendations table"""
    table_name = 'etl_recommendations_audit'

    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {'AttributeName': 'recommendation_id', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'recommendation_id', 'AttributeType': 'S'},
                {'AttributeName': 'job_name', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'job_name-index',
                    'KeySchema': [
                        {'AttributeName': 'job_name', 'KeyType': 'HASH'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            },
            Tags=[
                {'Key': 'Project', 'Value': 'ETL-Framework'},
                {'Key': 'Component', 'Value': 'Recommendations'}
            ]
        )

        print(f"  Creating table: {table_name}")
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print(f"  [DONE] Table {table_name} created")
        return True

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"  [SKIP] Table {table_name} already exists")
            return True
        else:
            print(f"  [ERROR] Failed to create {table_name}: {e}")
            return False


def provision_tables(region: str = 'us-east-1'):
    """Provision all DynamoDB tables"""
    print("=" * 60)
    print("PROVISIONING: DynamoDB Tables")
    print("=" * 60)
    print(f"Region: {region}")
    print()

    dynamodb = boto3.resource('dynamodb', region_name=region)

    results = {
        'etl_run_audit': create_etl_run_audit_table(dynamodb),
        'etl_dq_audit': create_dq_audit_table(dynamodb),
        'etl_recommendations_audit': create_recommendations_table(dynamodb)
    }

    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)

    success = all(results.values())
    for table, status in results.items():
        print(f"  {table}: {'SUCCESS' if status else 'FAILED'}")

    if success:
        print("\n  [SUCCESS] All DynamoDB tables provisioned!")
    else:
        print("\n  [FAILURE] Some tables failed to provision")

    return success


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Provision DynamoDB tables for ETL audit')
    parser.add_argument('--region', type=str, default='us-east-1', help='AWS region')
    args = parser.parse_args()

    success = provision_tables(args.region)
    sys.exit(0 if success else 1)
