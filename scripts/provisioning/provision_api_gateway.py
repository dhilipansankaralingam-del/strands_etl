#!/usr/bin/env python3
"""
Provisioning Script: API Gateway for ETL Framework
Creates API Gateway with Lambda integration
"""

import boto3
import json
import sys
import time
import zipfile
import io
from botocore.exceptions import ClientError


def create_lambda_execution_role(iam, role_name: str) -> str:
    """Create IAM role for Lambda execution"""
    print(f"  Creating IAM role: {role_name}")

    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }

    try:
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy),
            Description='ETL Framework API Lambda Execution Role',
            Tags=[
                {'Key': 'Project', 'Value': 'ETL-Framework'},
                {'Key': 'Component', 'Value': 'API'}
            ]
        )
        role_arn = response['Role']['Arn']

        # Attach policies
        policies = [
            'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
            'arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess',
            'arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess'
        ]

        for policy_arn in policies:
            iam.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)

        print(f"  [DONE] Role created: {role_arn}")
        time.sleep(10)  # Wait for role to propagate
        return role_arn

    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            response = iam.get_role(RoleName=role_name)
            print(f"  [SKIP] Role already exists")
            return response['Role']['Arn']
        raise


def create_lambda_function(lambda_client, function_name: str, role_arn: str, region: str) -> str:
    """Create Lambda function for API"""
    print(f"  Creating Lambda function: {function_name}")

    # Create simple handler code
    handler_code = '''
import json
from datetime import datetime

def lambda_handler(event, context):
    path = event.get('path', '/')
    method = event.get('httpMethod', 'GET')

    # Handle CORS
    if method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers(),
            'body': json.dumps({'message': 'OK'})
        }

    # Route handling
    if path == '/health':
        return response(200, {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'version': '1.0.0'
        })

    elif path == '/jobs':
        return response(200, {
            'jobs': [],
            'count': 0,
            'message': 'ETL Jobs API - Connect to DynamoDB for real data'
        })

    elif path == '/metrics':
        return response(200, {
            'metrics': {
                'total_runs': 0,
                'success_rate': 0,
                'avg_duration': 0
            }
        })

    else:
        return response(404, {'error': f'Route not found: {method} {path}'})

def cors_headers():
    return {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Api-Key',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS'
    }

def response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': cors_headers(),
        'body': json.dumps(body)
    }
'''

    # Create ZIP file in memory
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('lambda_function.py', handler_code)
    zip_buffer.seek(0)

    try:
        response = lambda_client.create_function(
            FunctionName=function_name,
            Runtime='python3.9',
            Role=role_arn,
            Handler='lambda_function.lambda_handler',
            Code={'ZipFile': zip_buffer.read()},
            Description='ETL Framework API Handler',
            Timeout=30,
            MemorySize=256,
            Tags={
                'Project': 'ETL-Framework',
                'Component': 'API'
            }
        )

        function_arn = response['FunctionArn']
        print(f"  [DONE] Lambda created: {function_arn}")
        return function_arn

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceConflictException':
            response = lambda_client.get_function(FunctionName=function_name)
            print(f"  [SKIP] Lambda already exists")
            return response['Configuration']['FunctionArn']
        raise


def create_api_gateway(apigateway, api_name: str, lambda_arn: str, region: str, account_id: str) -> dict:
    """Create REST API Gateway"""
    print(f"  Creating API Gateway: {api_name}")

    try:
        # Create REST API
        api_response = apigateway.create_rest_api(
            name=api_name,
            description='ETL Framework REST API',
            endpointConfiguration={'types': ['REGIONAL']},
            tags={'Project': 'ETL-Framework'}
        )
        api_id = api_response['id']
        print(f"    API ID: {api_id}")

        # Get root resource
        resources = apigateway.get_resources(restApiId=api_id)
        root_id = [r for r in resources['items'] if r['path'] == '/'][0]['id']

        # Create proxy resource
        proxy_resource = apigateway.create_resource(
            restApiId=api_id,
            parentId=root_id,
            pathPart='{proxy+}'
        )
        proxy_id = proxy_resource['id']

        # Create ANY method on proxy
        apigateway.put_method(
            restApiId=api_id,
            resourceId=proxy_id,
            httpMethod='ANY',
            authorizationType='NONE'
        )

        # Create Lambda integration
        lambda_uri = f"arn:aws:apigateway:{region}:lambda:path/2015-03-31/functions/{lambda_arn}/invocations"

        apigateway.put_integration(
            restApiId=api_id,
            resourceId=proxy_id,
            httpMethod='ANY',
            type='AWS_PROXY',
            integrationHttpMethod='POST',
            uri=lambda_uri
        )

        # Deploy API
        deployment = apigateway.create_deployment(
            restApiId=api_id,
            stageName='prod',
            description='Production deployment'
        )

        api_url = f"https://{api_id}.execute-api.{region}.amazonaws.com/prod"

        print(f"  [DONE] API Gateway created")
        print(f"    URL: {api_url}")

        return {
            'api_id': api_id,
            'api_url': api_url
        }

    except ClientError as e:
        print(f"  [ERROR] Failed to create API Gateway: {e}")
        raise


def add_lambda_permission(lambda_client, function_name: str, api_id: str, region: str, account_id: str):
    """Add permission for API Gateway to invoke Lambda"""
    print("  Adding Lambda invoke permission for API Gateway")

    try:
        lambda_client.add_permission(
            FunctionName=function_name,
            StatementId='apigateway-invoke',
            Action='lambda:InvokeFunction',
            Principal='apigateway.amazonaws.com',
            SourceArn=f"arn:aws:execute-api:{region}:{account_id}:{api_id}/*"
        )
        print("  [DONE] Permission added")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceConflictException':
            print("  [SKIP] Permission already exists")
        else:
            raise


def provision_api_gateway(region: str = 'us-east-1'):
    """Provision API Gateway with Lambda backend"""
    print("=" * 60)
    print("PROVISIONING: API Gateway")
    print("=" * 60)
    print(f"Region: {region}")
    print()

    # Get account ID
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']
    print(f"Account ID: {account_id}")
    print()

    # Clients
    iam = boto3.client('iam', region_name=region)
    lambda_client = boto3.client('lambda', region_name=region)
    apigateway = boto3.client('apigateway', region_name=region)

    # Names
    role_name = 'etl-framework-api-lambda-role'
    function_name = 'etl-framework-api-handler'
    api_name = 'etl-framework-api'

    try:
        # Create resources
        role_arn = create_lambda_execution_role(iam, role_name)
        lambda_arn = create_lambda_function(lambda_client, function_name, role_arn, region)
        api_info = create_api_gateway(apigateway, api_name, lambda_arn, region, account_id)
        add_lambda_permission(lambda_client, function_name, api_info['api_id'], region, account_id)

        print()
        print("=" * 60)
        print("SUCCESS")
        print("=" * 60)
        print(f"  API URL: {api_info['api_url']}")
        print()
        print("  Test endpoints:")
        print(f"    curl {api_info['api_url']}/health")
        print(f"    curl {api_info['api_url']}/jobs")
        print(f"    curl {api_info['api_url']}/metrics")

        return True

    except Exception as e:
        print()
        print("=" * 60)
        print("FAILURE")
        print("=" * 60)
        print(f"  Error: {e}")
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Provision API Gateway for ETL Framework')
    parser.add_argument('--region', type=str, default='us-east-1', help='AWS region')
    args = parser.parse_args()

    success = provision_api_gateway(args.region)
    sys.exit(0 if success else 1)
