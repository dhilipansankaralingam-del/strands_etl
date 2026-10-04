# AgentCore Runtime requires linux/arm64 images that listen on :8080
FROM --platform=linux/arm64 public.ecr.aws/docker/library/python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DOCKER_CONTAINER=1 \
    AWS_REGION=us-east-1 \
    AWS_DEFAULT_REGION=us-east-1

COPY requirements.txt .
RUN pip install -r requirements.txt

# Run as non-root
RUN useradd -m -u 1000 bedrock_agentcore
USER bedrock_agentcore

COPY agent/ ./agent/

EXPOSE 8080

# `opentelemetry-instrument` auto-instruments the process with ADOT so that
# Strands' spans (agent -> cycle -> model call -> tool call) land in CloudWatch.
# AgentCore Runtime injects the OTEL exporter settings for hosted agents.
CMD ["opentelemetry-instrument", "python", "-m", "agent.main"]
