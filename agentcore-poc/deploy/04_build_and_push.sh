#!/usr/bin/env bash
# Step 4 - Build the arm64 image and push it to Amazon ECR.
# Works on Apple Silicon natively and on x86 via docker buildx + QEMU emulation.
set -euo pipefail
: "${ECR_URI:?source deploy/config.env first}"
cd "$(dirname "$0")/.."

TAG=${TAG:-v$(date +%Y%m%d-%H%M%S)}

echo "==> Ensuring ECR repository ${ECR_REPO}"
aws ecr describe-repositories --repository-names "$ECR_REPO" >/dev/null 2>&1 || \
  aws ecr create-repository --repository-name "$ECR_REPO" \
      --image-scanning-configuration scanOnPush=true >/dev/null

echo "==> Logging Docker into ECR"
aws ecr get-login-password | docker login --username AWS --password-stdin "${ECR_URI%%/*}"

echo "==> Building linux/arm64 image and pushing ${ECR_URI}:${TAG}"
docker buildx inspect agentcore-builder >/dev/null 2>&1 || docker buildx create --name agentcore-builder --use >/dev/null
docker buildx use agentcore-builder
docker buildx build --platform linux/arm64 -t "${ECR_URI}:${TAG}" -t "${ECR_URI}:latest" --push .

python3 - <<EOF
import sys; sys.path.insert(0, "deploy")
from common import save_state
save_state(image_uri="${ECR_URI}:${TAG}")
print("Saved image_uri=${ECR_URI}:${TAG} to deploy/state.json")
EOF
