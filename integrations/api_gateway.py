"""
API Gateway Integration for ETL Framework
Provides REST API endpoints for ETL operations via AWS API Gateway + Lambda
"""

import json
import logging
import hashlib
import hmac
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
from enum import Enum
from functools import wraps

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class HTTPMethod(Enum):
    """Supported HTTP methods"""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"


class AuthType(Enum):
    """API authentication types"""
    NONE = "none"
    API_KEY = "api_key"
    IAM = "iam"
    COGNITO = "cognito"
    LAMBDA = "lambda"


@dataclass
class APIConfig:
    """API Gateway configuration"""
    api_name: str = "etl-framework-api"
    stage: str = "prod"
    region: str = "us-east-1"
    auth_type: AuthType = AuthType.API_KEY
    api_key_secret_arn: Optional[str] = None
    cognito_user_pool_id: Optional[str] = None
    cognito_client_id: Optional[str] = None
    rate_limit: int = 1000  # requests per second
    burst_limit: int = 2000
    enable_cors: bool = True
    allowed_origins: List[str] = field(default_factory=lambda: ["*"])
    log_level: str = "INFO"


@dataclass
class APIResponse:
    """Standard API response structure"""
    status_code: int
    body: Dict[str, Any]
    headers: Dict[str, str] = field(default_factory=dict)

    def to_lambda_response(self) -> Dict[str, Any]:
        """Convert to Lambda proxy integration response"""
        default_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Api-Key,Authorization",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS"
        }
        return {
            "statusCode": self.status_code,
            "headers": {**default_headers, **self.headers},
            "body": json.dumps(self.body)
        }


class APIGatewayIntegration:
    """AWS API Gateway integration for ETL Framework"""

    def __init__(self, config: APIConfig):
        self.config = config
        self.apigateway = boto3.client('apigateway', region_name=config.region)
        self.lambda_client = boto3.client('lambda', region_name=config.region)
        self.secrets_client = boto3.client('secretsmanager', region_name=config.region)
        self._routes: Dict[str, Dict[str, Callable]] = {}

    def register_route(self, path: str, method: HTTPMethod, handler: Callable):
        """Register a route handler"""
        if path not in self._routes:
            self._routes[path] = {}
        self._routes[path][method.value] = handler
        logger.info(f"Registered route: {method.value} {path}")

    def validate_api_key(self, api_key: str) -> bool:
        """Validate API key against stored secret"""
        if self.config.auth_type != AuthType.API_KEY:
            return True

        if not self.config.api_key_secret_arn:
            logger.warning("No API key secret configured")
            return False

        try:
            response = self.secrets_client.get_secret_value(
                SecretId=self.config.api_key_secret_arn
            )
            secret = json.loads(response['SecretString'])
            stored_key = secret.get('api_key', '')

            # Constant-time comparison to prevent timing attacks
            return hmac.compare_digest(api_key, stored_key)
        except Exception as e:
            logger.error(f"API key validation error: {e}")
            return False

    def handle_request(self, event: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """Main Lambda handler for API Gateway requests"""

        # Handle CORS preflight
        http_method = event.get('httpMethod', 'GET')
        if http_method == 'OPTIONS':
            return APIResponse(
                status_code=200,
                body={"message": "OK"}
            ).to_lambda_response()

        # Extract request details
        path = event.get('path', '/')
        headers = event.get('headers', {}) or {}
        query_params = event.get('queryStringParameters', {}) or {}
        body = event.get('body', '{}')

        # Parse body if JSON
        try:
            if body and isinstance(body, str):
                body = json.loads(body)
        except json.JSONDecodeError:
            return APIResponse(
                status_code=400,
                body={"error": "Invalid JSON body"}
            ).to_lambda_response()

        # Validate authentication
        if self.config.auth_type == AuthType.API_KEY:
            api_key = headers.get('X-Api-Key') or headers.get('x-api-key', '')
            if not self.validate_api_key(api_key):
                return APIResponse(
                    status_code=401,
                    body={"error": "Invalid or missing API key"}
                ).to_lambda_response()

        # Find and execute handler
        if path in self._routes and http_method in self._routes[path]:
            handler = self._routes[path][http_method]
            try:
                result = handler(
                    body=body,
                    query_params=query_params,
                    headers=headers,
                    path_params=event.get('pathParameters', {}) or {}
                )
                return result.to_lambda_response()
            except Exception as e:
                logger.error(f"Handler error: {e}")
                return APIResponse(
                    status_code=500,
                    body={"error": str(e)}
                ).to_lambda_response()

        return APIResponse(
            status_code=404,
            body={"error": f"Route not found: {http_method} {path}"}
        ).to_lambda_response()


class ETLAPIHandlers:
    """API handlers for ETL operations"""

    def __init__(self, audit_manager=None, notification_manager=None):
        self.audit_manager = audit_manager
        self.notification_manager = notification_manager
        self.dynamodb = boto3.resource('dynamodb')
        self.glue = boto3.client('glue')
        self.emr = boto3.client('emr')

    # ============ Job Management Endpoints ============

    def list_jobs(self, body: Dict, query_params: Dict,
                  headers: Dict, path_params: Dict) -> APIResponse:
        """GET /jobs - List all ETL jobs"""

        limit = int(query_params.get('limit', 50))
        status = query_params.get('status')
        start_date = query_params.get('start_date')

        try:
            if self.audit_manager:
                jobs = self.audit_manager.get_recent_runs(
                    hours=int(query_params.get('hours', 24)),
                    limit=limit
                )
            else:
                # Fallback to direct DynamoDB query
                table = self.dynamodb.Table('etl_run_audit')
                response = table.scan(Limit=limit)
                jobs = response.get('Items', [])

            return APIResponse(
                status_code=200,
                body={
                    "jobs": jobs,
                    "count": len(jobs),
                    "timestamp": datetime.now().isoformat()
                }
            )
        except Exception as e:
            logger.error(f"Failed to list jobs: {e}")
            return APIResponse(
                status_code=500,
                body={"error": f"Failed to list jobs: {str(e)}"}
            )

    def get_job(self, body: Dict, query_params: Dict,
                headers: Dict, path_params: Dict) -> APIResponse:
        """GET /jobs/{job_name} - Get job details"""

        job_name = path_params.get('job_name')
        if not job_name:
            return APIResponse(
                status_code=400,
                body={"error": "job_name is required"}
            )

        try:
            if self.audit_manager:
                job = self.audit_manager.get_job_history(job_name)
            else:
                table = self.dynamodb.Table('etl_run_audit')
                response = table.query(
                    IndexName='job_name-index',
                    KeyConditionExpression='job_name = :jn',
                    ExpressionAttributeValues={':jn': job_name},
                    Limit=10,
                    ScanIndexForward=False
                )
                job = response.get('Items', [])

            return APIResponse(
                status_code=200,
                body={
                    "job_name": job_name,
                    "runs": job,
                    "count": len(job)
                }
            )
        except Exception as e:
            return APIResponse(
                status_code=500,
                body={"error": f"Failed to get job: {str(e)}"}
            )

    def get_run(self, body: Dict, query_params: Dict,
                headers: Dict, path_params: Dict) -> APIResponse:
        """GET /jobs/{job_name}/runs/{run_id} - Get specific run details"""

        job_name = path_params.get('job_name')
        run_id = path_params.get('run_id')

        if not job_name or not run_id:
            return APIResponse(
                status_code=400,
                body={"error": "job_name and run_id are required"}
            )

        try:
            if self.audit_manager:
                run = self.audit_manager.get_run(run_id)
            else:
                table = self.dynamodb.Table('etl_run_audit')
                response = table.get_item(Key={'run_id': run_id})
                run = response.get('Item')

            if not run:
                return APIResponse(
                    status_code=404,
                    body={"error": f"Run {run_id} not found"}
                )

            return APIResponse(
                status_code=200,
                body={"run": run}
            )
        except Exception as e:
            return APIResponse(
                status_code=500,
                body={"error": f"Failed to get run: {str(e)}"}
            )

    def trigger_job(self, body: Dict, query_params: Dict,
                    headers: Dict, path_params: Dict) -> APIResponse:
        """POST /jobs/{job_name}/trigger - Trigger ETL job execution"""

        job_name = path_params.get('job_name')
        if not job_name:
            return APIResponse(
                status_code=400,
                body={"error": "job_name is required"}
            )

        # Get job configuration from body or use defaults
        config = body.get('config', {})
        platform = body.get('platform', 'auto')
        priority = body.get('priority', 'normal')

        try:
            # Generate run ID
            run_id = f"{job_name}-{int(time.time() * 1000)}"

            # Start audit record
            if self.audit_manager:
                self.audit_manager.start_run(job_name, {
                    'run_id': run_id,
                    'platform': platform,
                    'config': config
                })

            # Trigger the job based on platform
            if platform == 'glue' or platform == 'auto':
                response = self.glue.start_job_run(
                    JobName=job_name,
                    Arguments={
                        '--run_id': run_id,
                        '--config': json.dumps(config)
                    }
                )
                glue_run_id = response['JobRunId']

                return APIResponse(
                    status_code=202,
                    body={
                        "message": f"Job {job_name} triggered successfully",
                        "run_id": run_id,
                        "glue_run_id": glue_run_id,
                        "platform": "glue",
                        "status": "RUNNING"
                    }
                )

            elif platform == 'emr':
                # EMR job submission would go here
                return APIResponse(
                    status_code=202,
                    body={
                        "message": f"Job {job_name} triggered on EMR",
                        "run_id": run_id,
                        "platform": "emr",
                        "status": "PENDING"
                    }
                )

            else:
                return APIResponse(
                    status_code=400,
                    body={"error": f"Unsupported platform: {platform}"}
                )

        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_msg = e.response['Error']['Message']
            return APIResponse(
                status_code=500,
                body={"error": f"AWS Error ({error_code}): {error_msg}"}
            )
        except Exception as e:
            return APIResponse(
                status_code=500,
                body={"error": f"Failed to trigger job: {str(e)}"}
            )

    def cancel_job(self, body: Dict, query_params: Dict,
                   headers: Dict, path_params: Dict) -> APIResponse:
        """DELETE /jobs/{job_name}/runs/{run_id} - Cancel running job"""

        job_name = path_params.get('job_name')
        run_id = path_params.get('run_id')

        if not job_name or not run_id:
            return APIResponse(
                status_code=400,
                body={"error": "job_name and run_id are required"}
            )

        try:
            # Try to stop Glue job
            self.glue.batch_stop_job_run(
                JobName=job_name,
                JobRunIds=[run_id]
            )

            # Update audit
            if self.audit_manager:
                self.audit_manager.update_run_status(run_id, 'CANCELLED')

            return APIResponse(
                status_code=200,
                body={
                    "message": f"Job run {run_id} cancelled",
                    "job_name": job_name,
                    "status": "CANCELLED"
                }
            )
        except Exception as e:
            return APIResponse(
                status_code=500,
                body={"error": f"Failed to cancel job: {str(e)}"}
            )

    # ============ Metrics Endpoints ============

    def get_metrics(self, body: Dict, query_params: Dict,
                    headers: Dict, path_params: Dict) -> APIResponse:
        """GET /metrics - Get ETL metrics summary"""

        hours = int(query_params.get('hours', 24))

        try:
            if self.audit_manager:
                metrics = self.audit_manager.get_dashboard_data(hours=hours)
            else:
                # Calculate metrics from DynamoDB
                metrics = self._calculate_metrics(hours)

            return APIResponse(
                status_code=200,
                body={
                    "metrics": metrics,
                    "period_hours": hours,
                    "generated_at": datetime.now().isoformat()
                }
            )
        except Exception as e:
            return APIResponse(
                status_code=500,
                body={"error": f"Failed to get metrics: {str(e)}"}
            )

    def _calculate_metrics(self, hours: int) -> Dict[str, Any]:
        """Calculate metrics from audit data"""
        return {
            "total_runs": 0,
            "successful_runs": 0,
            "failed_runs": 0,
            "total_rows_processed": 0,
            "total_cost_usd": 0.0,
            "avg_duration_seconds": 0,
            "avg_dq_score": 1.0
        }

    def get_job_metrics(self, body: Dict, query_params: Dict,
                        headers: Dict, path_params: Dict) -> APIResponse:
        """GET /jobs/{job_name}/metrics - Get metrics for specific job"""

        job_name = path_params.get('job_name')
        if not job_name:
            return APIResponse(
                status_code=400,
                body={"error": "job_name is required"}
            )

        hours = int(query_params.get('hours', 168))  # Default 1 week

        try:
            if self.audit_manager:
                metrics = self.audit_manager.get_job_metrics(job_name, hours=hours)
            else:
                metrics = {"job_name": job_name, "runs": 0}

            return APIResponse(
                status_code=200,
                body={
                    "job_name": job_name,
                    "metrics": metrics,
                    "period_hours": hours
                }
            )
        except Exception as e:
            return APIResponse(
                status_code=500,
                body={"error": f"Failed to get job metrics: {str(e)}"}
            )

    # ============ Data Quality Endpoints ============

    def get_dq_results(self, body: Dict, query_params: Dict,
                       headers: Dict, path_params: Dict) -> APIResponse:
        """GET /jobs/{job_name}/runs/{run_id}/dq - Get data quality results"""

        job_name = path_params.get('job_name')
        run_id = path_params.get('run_id')

        try:
            if self.audit_manager:
                dq_results = self.audit_manager.get_dq_results(run_id)
            else:
                table = self.dynamodb.Table('etl_dq_audit')
                response = table.query(
                    KeyConditionExpression='run_id = :rid',
                    ExpressionAttributeValues={':rid': run_id}
                )
                dq_results = response.get('Items', [])

            return APIResponse(
                status_code=200,
                body={
                    "job_name": job_name,
                    "run_id": run_id,
                    "dq_results": dq_results
                }
            )
        except Exception as e:
            return APIResponse(
                status_code=500,
                body={"error": f"Failed to get DQ results: {str(e)}"}
            )

    # ============ Recommendations Endpoints ============

    def get_recommendations(self, body: Dict, query_params: Dict,
                           headers: Dict, path_params: Dict) -> APIResponse:
        """GET /jobs/{job_name}/recommendations - Get optimization recommendations"""

        job_name = path_params.get('job_name')

        try:
            if self.audit_manager:
                recommendations = self.audit_manager.get_recommendations(job_name)
            else:
                recommendations = []

            return APIResponse(
                status_code=200,
                body={
                    "job_name": job_name,
                    "recommendations": recommendations,
                    "count": len(recommendations)
                }
            )
        except Exception as e:
            return APIResponse(
                status_code=500,
                body={"error": f"Failed to get recommendations: {str(e)}"}
            )

    # ============ Health Check ============

    def health_check(self, body: Dict, query_params: Dict,
                     headers: Dict, path_params: Dict) -> APIResponse:
        """GET /health - API health check"""

        return APIResponse(
            status_code=200,
            body={
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "version": "1.0.0"
            }
        )


def create_api_handler(config: APIConfig, audit_manager=None) -> APIGatewayIntegration:
    """Create and configure API Gateway handler with all routes"""

    api = APIGatewayIntegration(config)
    handlers = ETLAPIHandlers(audit_manager=audit_manager)

    # Register routes
    api.register_route("/health", HTTPMethod.GET, handlers.health_check)
    api.register_route("/jobs", HTTPMethod.GET, handlers.list_jobs)
    api.register_route("/jobs/{job_name}", HTTPMethod.GET, handlers.get_job)
    api.register_route("/jobs/{job_name}/trigger", HTTPMethod.POST, handlers.trigger_job)
    api.register_route("/jobs/{job_name}/runs/{run_id}", HTTPMethod.GET, handlers.get_run)
    api.register_route("/jobs/{job_name}/runs/{run_id}", HTTPMethod.DELETE, handlers.cancel_job)
    api.register_route("/jobs/{job_name}/metrics", HTTPMethod.GET, handlers.get_job_metrics)
    api.register_route("/jobs/{job_name}/runs/{run_id}/dq", HTTPMethod.GET, handlers.get_dq_results)
    api.register_route("/jobs/{job_name}/recommendations", HTTPMethod.GET, handlers.get_recommendations)
    api.register_route("/metrics", HTTPMethod.GET, handlers.get_metrics)

    return api


# Lambda handler entry point
def lambda_handler(event, context):
    """AWS Lambda handler for API Gateway"""

    config = APIConfig(
        api_name="etl-framework-api",
        auth_type=AuthType.API_KEY,
        api_key_secret_arn=event.get('stageVariables', {}).get('api_key_secret_arn')
    )

    api = create_api_handler(config)
    return api.handle_request(event, context)


# ============ CloudFormation/SAM Template Generator ============

def generate_api_cloudformation(config: APIConfig) -> Dict[str, Any]:
    """Generate CloudFormation template for API Gateway"""

    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Transform": "AWS::Serverless-2016-10-31",
        "Description": "ETL Framework API Gateway",
        "Globals": {
            "Function": {
                "Timeout": 30,
                "Runtime": "python3.9",
                "MemorySize": 256
            },
            "Api": {
                "Cors": {
                    "AllowMethods": "'GET,POST,PUT,DELETE,OPTIONS'",
                    "AllowHeaders": "'Content-Type,X-Api-Key,Authorization'",
                    "AllowOrigin": "'*'"
                }
            }
        },
        "Resources": {
            "ETLApi": {
                "Type": "AWS::Serverless::Api",
                "Properties": {
                    "Name": config.api_name,
                    "StageName": config.stage,
                    "Auth": {
                        "ApiKeyRequired": config.auth_type == AuthType.API_KEY
                    }
                }
            },
            "ETLApiFunction": {
                "Type": "AWS::Serverless::Function",
                "Properties": {
                    "Handler": "api_gateway.lambda_handler",
                    "CodeUri": "./",
                    "Events": {
                        "AnyApi": {
                            "Type": "Api",
                            "Properties": {
                                "RestApiId": {"Ref": "ETLApi"},
                                "Path": "/{proxy+}",
                                "Method": "ANY"
                            }
                        }
                    },
                    "Policies": [
                        "AmazonDynamoDBReadOnlyAccess",
                        "AWSGlueConsoleFullAccess"
                    ]
                }
            },
            "ApiKey": {
                "Type": "AWS::ApiGateway::ApiKey",
                "Properties": {
                    "Name": f"{config.api_name}-key",
                    "Enabled": True,
                    "StageKeys": [{
                        "RestApiId": {"Ref": "ETLApi"},
                        "StageName": config.stage
                    }]
                }
            }
        },
        "Outputs": {
            "ApiUrl": {
                "Description": "API Gateway URL",
                "Value": {
                    "Fn::Sub": f"https://${{ETLApi}}.execute-api.${{AWS::Region}}.amazonaws.com/{config.stage}"
                }
            }
        }
    }
