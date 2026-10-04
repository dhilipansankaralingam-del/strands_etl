"""Central configuration, all driven by environment variables.

Values are injected into the AgentCore Runtime container via the
`environmentVariables` field of CreateAgentRuntime (see deploy/05_create_runtime.py).
"""
import os

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Cross-region inference profile for Claude Sonnet. Override with MODEL_ID if your
# account has a newer model enabled (`aws bedrock list-inference-profiles`).
MODEL_ID = os.getenv("MODEL_ID", "us.anthropic.claude-sonnet-4-6")

# Optionally give the specialists a different (cheaper/faster) model.
SPECIALIST_MODEL_ID = os.getenv("SPECIALIST_MODEL_ID", MODEL_ID)

# AgentCore Memory resource id (created by deploy/03_create_memory.py).
# When empty the orchestrator runs without persistent memory (handy for local tests).
MEMORY_ID = os.getenv("MEMORY_ID", "")

# Custom CloudWatch metrics (on top of AgentCore's built-in observability).
METRICS_ENABLED = os.getenv("METRICS_ENABLED", "true").lower() == "true"
METRICS_NAMESPACE = os.getenv("METRICS_NAMESPACE", "AgentCoreMultiAgentPOC")
SERVICE_NAME = os.getenv("SERVICE_NAME", "research_orchestrator")

# Quality gate used by the orchestrator's review loop.
REVIEW_PASS_SCORE = float(os.getenv("REVIEW_PASS_SCORE", "7"))
MAX_REVISIONS = int(os.getenv("MAX_REVISIONS", "1"))
