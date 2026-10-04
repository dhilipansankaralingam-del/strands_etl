"""Shared helpers for the deploy / eval scripts.

All scripts read settings from environment variables (see deploy/config.env)
and share generated IDs through deploy/state.json.
"""
import json
import os
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

STATE_FILE = Path(__file__).parent / "state.json"

REGION = os.getenv("AWS_REGION", "us-east-1")
PROJECT = os.getenv("PROJECT", "multiagent-poc")
AGENT_NAME = os.getenv("AGENT_NAME", "research_orchestrator")
MODEL_ID = os.getenv("MODEL_ID", "us.anthropic.claude-sonnet-4-6")
METRICS_NAMESPACE = os.getenv("METRICS_NAMESPACE", "AgentCoreMultiAgentPOC")


def load_state() -> dict:
    return json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}


def save_state(**kwargs) -> dict:
    state = load_state()
    state.update(kwargs)
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str))
    return state


def require(state: dict, *keys: str):
    missing = [k for k in keys if not state.get(k)]
    if missing:
        sys.exit(f"Missing {missing} in {STATE_FILE}. Run the earlier deploy steps first.")


def account_id() -> str:
    return boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]


def control():
    return boto3.client("bedrock-agentcore-control", region_name=REGION)


def data():
    """Data-plane client, configured for a slow agent.

    Two settings matter and both defaults are wrong for this workload:
      read_timeout - botocore waits 60s by default; a full research->write->review
                     run takes 40-90s, so the socket times out mid-flight.
      retries      - after that timeout boto3 silently retries, which re-runs the
                     ENTIRE agent workflow. The call looks hung, costs multiply,
                     and duplicate turns land in memory. One attempt only.
    """
    return boto3.client("bedrock-agentcore", region_name=REGION, config=Config(
        read_timeout=900, connect_timeout=10, retries={"max_attempts": 0}))


def wait_for(fn, ok: set, bad: set, what: str, timeout=900, every=10) -> dict:
    start = time.time()
    while True:
        res = fn()
        status = res.get("status")
        print(f"  {what}: {status}")
        if status in ok:
            return res
        if status in bad:
            sys.exit(f"{what} ended in {status}: {res.get('failureReason') or res}")
        if time.time() - start > timeout:
            sys.exit(f"Timed out waiting for {what}")
        time.sleep(every)
