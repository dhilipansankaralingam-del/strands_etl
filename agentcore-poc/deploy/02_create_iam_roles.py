"""Step 2 - Create the two IAM roles.

  <PROJECT>-runtime-role  : assumed by AgentCore Runtime to run the container
                            (ECR pull, Bedrock models, Memory, logs, traces, metrics)
  <PROJECT>-eval-role     : assumed by AgentCore Evaluations for online evaluation
                            (read spans from CloudWatch, call the judge model)

Idempotent: re-running updates the inline policies.
"""
import json
import time

import boto3
from botocore.exceptions import ClientError

from common import METRICS_NAMESPACE, PROJECT, REGION, account_id, save_state

iam = boto3.client("iam")
ACCT = account_id()


def runtime_trust():
    return {"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "bedrock-agentcore.amazonaws.com"},
        "Action": "sts:AssumeRole",
        "Condition": {
            "StringEquals": {"aws:SourceAccount": ACCT},
            "ArnLike": {"aws:SourceArn": f"arn:aws:bedrock-agentcore:{REGION}:{ACCT}:*"},
        },
    }]}


def runtime_policy():
    return {"Version": "2012-10-17", "Statement": [
        {"Sid": "ECRImageAccess", "Effect": "Allow",
         "Action": ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"],
         "Resource": f"arn:aws:ecr:{REGION}:{ACCT}:repository/*"},
        {"Sid": "ECRToken", "Effect": "Allow", "Action": "ecr:GetAuthorizationToken", "Resource": "*"},
        {"Sid": "Logs", "Effect": "Allow",
         "Action": ["logs:DescribeLogStreams", "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
         "Resource": [f"arn:aws:logs:{REGION}:{ACCT}:log-group:/aws/bedrock-agentcore/runtimes/*",
                      f"arn:aws:logs:{REGION}:{ACCT}:log-group:/aws/bedrock-agentcore/runtimes/*:log-stream:*"]},
        {"Sid": "LogsDescribe", "Effect": "Allow", "Action": "logs:DescribeLogGroups",
         "Resource": f"arn:aws:logs:{REGION}:{ACCT}:log-group:*"},
        {"Sid": "XRay", "Effect": "Allow",
         "Action": ["xray:PutTraceSegments", "xray:PutTelemetryRecords",
                    "xray:GetSamplingRules", "xray:GetSamplingTargets"],
         "Resource": "*"},
        {"Sid": "Metrics", "Effect": "Allow", "Action": "cloudwatch:PutMetricData", "Resource": "*",
         "Condition": {"StringEquals": {"cloudwatch:namespace": ["bedrock-agentcore", METRICS_NAMESPACE]}}},
        {"Sid": "WorkloadIdentity", "Effect": "Allow",
         "Action": ["bedrock-agentcore:GetWorkloadAccessToken",
                    "bedrock-agentcore:GetWorkloadAccessTokenForJWT",
                    "bedrock-agentcore:GetWorkloadAccessTokenForUserId"],
         "Resource": [f"arn:aws:bedrock-agentcore:{REGION}:{ACCT}:workload-identity-directory/default",
                      f"arn:aws:bedrock-agentcore:{REGION}:{ACCT}:workload-identity-directory/default/workload-identity/*"]},
        {"Sid": "BedrockModels", "Effect": "Allow",
         "Action": ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
         # cross-region "us." profiles fan out to several regions
         "Resource": ["arn:aws:bedrock:*::foundation-model/*",
                      f"arn:aws:bedrock:*:{ACCT}:inference-profile/*"]},
        # Only needed for the no-Docker path (deploy/04b_package_and_upload.py):
        # the service reads the deployment zip out of this bucket.
        {"Sid": "CodeDeploymentPackage", "Effect": "Allow",
         "Action": ["s3:GetObject", "s3:GetObjectVersion"],
         "Resource": f"arn:aws:s3:::bedrock-agentcore-code-{ACCT}-{REGION}/*"},
        {"Sid": "CodeDeploymentBucket", "Effect": "Allow",
         "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
         "Resource": f"arn:aws:s3:::bedrock-agentcore-code-{ACCT}-{REGION}"},
        {"Sid": "AgentCoreMemory", "Effect": "Allow",
         "Action": ["bedrock-agentcore:CreateEvent", "bedrock-agentcore:GetEvent",
                    "bedrock-agentcore:ListEvents", "bedrock-agentcore:DeleteEvent",
                    "bedrock-agentcore:RetrieveMemoryRecords", "bedrock-agentcore:ListMemoryRecords",
                    "bedrock-agentcore:GetMemoryRecord", "bedrock-agentcore:ListSessions",
                    "bedrock-agentcore:ListActors", "bedrock-agentcore:GetMemory"],
         "Resource": f"arn:aws:bedrock-agentcore:{REGION}:{ACCT}:memory/*"},
    ]}


def eval_trust():
    return {"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "bedrock-agentcore.amazonaws.com"},
        "Action": "sts:AssumeRole",
        "Condition": {
            "StringEquals": {"aws:SourceAccount": ACCT, "aws:ResourceAccount": ACCT},
            "ArnLike": {"aws:SourceArn": [
                f"arn:aws:bedrock-agentcore:{REGION}:{ACCT}:evaluator/*",
                f"arn:aws:bedrock-agentcore:{REGION}:{ACCT}:online-evaluation-config/*"]},
        },
    }]}


def eval_policy():
    return {"Version": "2012-10-17", "Statement": [
        {"Sid": "CloudWatchLogRead", "Effect": "Allow",
         "Action": ["logs:DescribeLogGroups", "logs:GetQueryResults", "logs:StartQuery"], "Resource": "*"},
        {"Sid": "CloudWatchLogWrite", "Effect": "Allow",
         "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
         "Resource": f"arn:aws:logs:{REGION}:{ACCT}:log-group:/aws/bedrock-agentcore/evaluations/*"},
        {"Sid": "CloudWatchIndexPolicy", "Effect": "Allow",
         "Action": ["logs:DescribeIndexPolicies", "logs:PutIndexPolicy"],
         "Resource": [f"arn:aws:logs:{REGION}:{ACCT}:log-group:aws/spans",
                      f"arn:aws:logs:{REGION}:{ACCT}:log-group:aws/spans:*"]},
        {"Sid": "BedrockInvoke", "Effect": "Allow",
         "Action": ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"],
         "Resource": ["arn:aws:bedrock:*::foundation-model/*",
                      f"arn:aws:bedrock:*:{ACCT}:inference-profile/*"]},
    ]}


def upsert_role(name: str, trust: dict, policy: dict, desc: str) -> str:
    try:
        arn = iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(trust), Description=desc)["Role"]["Arn"]
        print(f"  created role {name}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityAlreadyExists":
            raise
        iam.update_assume_role_policy(RoleName=name, PolicyDocument=json.dumps(trust))
        arn = iam.get_role(RoleName=name)["Role"]["Arn"]
        print(f"  role {name} exists - updated trust policy")
    iam.put_role_policy(RoleName=name, PolicyName=f"{name}-policy", PolicyDocument=json.dumps(policy))
    return arn


if __name__ == "__main__":
    print("==> IAM roles")
    runtime_arn = upsert_role(f"{PROJECT}-runtime-role", runtime_trust(), runtime_policy(),
                              "AgentCore Runtime execution role for the multi-agent POC")
    eval_arn = upsert_role(f"{PROJECT}-eval-role", eval_trust(), eval_policy(),
                           "AgentCore Evaluations execution role for the multi-agent POC")
    save_state(runtime_role_arn=runtime_arn, eval_role_arn=eval_arn)
    print(f"  runtime role: {runtime_arn}\n  eval role:    {eval_arn}")
    print("  waiting 15s for IAM propagation...")
    time.sleep(15)
