"""Tear everything down (runtime, online eval, memory, ECR repo, IAM roles, dashboard).

python deploy/99_cleanup.py --yes
"""
import sys

import boto3
from botocore.exceptions import ClientError

from common import PROJECT, REGION, STATE_FILE, control, load_state

if "--yes" not in sys.argv:
    sys.exit("This deletes all POC resources. Re-run with --yes to confirm.")

s = load_state()
cp = control()


def attempt(what, fn):
    try:
        fn()
        print(f"  deleted {what}")
    except ClientError as e:
        print(f"  skip {what}: {e.response['Error']['Code']}")


if s.get("online_eval_config_id"):
    attempt("online eval", lambda: cp.delete_online_evaluation_config(onlineEvaluationConfigId=s["online_eval_config_id"]))
if s.get("agent_runtime_id"):
    attempt("runtime", lambda: cp.delete_agent_runtime(agentRuntimeId=s["agent_runtime_id"]))
if s.get("memory_id"):
    attempt("memory", lambda: cp.delete_memory(memoryId=s["memory_id"]))

if s.get("code_bucket"):
    s3 = boto3.client("s3", region_name=REGION)
    attempt("code package", lambda: s3.delete_object(Bucket=s["code_bucket"], Key=s["code_key"]))
    # the bucket is left in place — it is shared by every code-deployed agent in the account

import os  # noqa: E402
repo = os.getenv("ECR_REPO", "multiagent-poc-agent")
attempt("ECR repo", lambda: boto3.client("ecr", region_name=REGION).delete_repository(repositoryName=repo, force=True))
attempt("dashboard", lambda: boto3.client("cloudwatch", region_name=REGION).delete_dashboards(
    DashboardNames=[f"{PROJECT}-dashboard"]))

iam = boto3.client("iam")
for role in (f"{PROJECT}-runtime-role", f"{PROJECT}-eval-role"):
    attempt(f"policy {role}", lambda r=role: iam.delete_role_policy(RoleName=r, PolicyName=f"{r}-policy"))
    attempt(f"role {role}", lambda r=role: iam.delete_role(RoleName=r))

STATE_FILE.unlink(missing_ok=True)
print("Done. (Transaction Search settings and log groups were left in place.)")
