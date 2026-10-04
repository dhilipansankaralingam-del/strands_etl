"""Step 5 - Create (or update) the AgentCore Runtime.

Two artifact types are supported and the script picks whichever you built last:
    container  - the ECR image from 04_build_and_push.sh   (needs Docker)
    code       - the S3 zip from 04b_package_and_upload.py (no Docker; CloudShell-friendly)
Force one with --container / --code.

Re-run after every build to roll out a new version; the DEFAULT endpoint always
points at the latest one.
"""
import argparse
import os
import sys
import time

from botocore.exceptions import ClientError

from common import (AGENT_NAME, METRICS_NAMESPACE, MODEL_ID, REGION, control, load_state, require,
                    save_state, wait_for)

def artifact_kind(runtime: dict) -> str:
    """Which artifact type an existing runtime was created with."""
    return "code" if "codeConfiguration" in runtime.get("agentRuntimeArtifact", {}) else "container"


def delete_and_wait(cp, runtime_id: str, timeout: int = 900) -> None:
    """Delete a runtime and wait for it to disappear.

    Needed because the artifact type (container vs code) is fixed at creation:
    switching routes means replacing the runtime, not updating it. Memory,
    IAM roles and the dashboard are separate resources and are untouched.

    Safe to re-run: if a previous attempt already started the delete, this
    picks up waiting rather than failing.
    """
    print(f"==> Deleting runtime {runtime_id} (artifact type cannot be changed in place)")

    # Endpoints can hold the runtime open; remove the ones we can before deleting.
    try:
        for ep in cp.list_agent_runtime_endpoints(agentRuntimeId=runtime_id).get("runtimeEndpoints", []):
            if ep.get("name") != "DEFAULT":
                cp.delete_agent_runtime_endpoint(agentRuntimeId=runtime_id, endpointName=ep["name"])
                print(f"    deleted endpoint {ep['name']}")
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceNotFoundException":
            print(f"    (could not list endpoints: {exc.response['Error']['Code']})")

    try:
        cp.delete_agent_runtime(agentRuntimeId=runtime_id)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "ResourceNotFoundException":
            print("    already gone")
            return
        if code not in ("ConflictException", "ValidationException"):
            raise
        print(f"    delete already in progress ({code}) - waiting")

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status = cp.get_agent_runtime(agentRuntimeId=runtime_id).get("status")
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ResourceNotFoundException":
                print("    deleted")
                return
            raise
        print(f"    {status} ({int(deadline - time.time())}s left)")
        if status == "DELETE_FAILED":
            sys.exit(
                f"Deleting {runtime_id} failed. Check the console for what is holding it, or delete "
                f"it manually:\n  aws bedrock-agentcore-control delete-agent-runtime "
                f"--agent-runtime-id {runtime_id}")
        time.sleep(10)
    sys.exit(
        f"Still deleting after {timeout}s. It is probably fine - check with:\n"
        f"  aws bedrock-agentcore-control get-agent-runtime --agent-runtime-id {runtime_id}\n"
        f"Once that returns ResourceNotFoundException, re-run this script (no --recreate needed).")


def pick_artifact(state: dict, forced: str | None) -> tuple[dict, str]:
    has_code, has_image = bool(state.get("code_key")), bool(state.get("image_uri"))
    mode = forced or ("code" if has_code else "container")
    if mode == "code":
        if not has_code:
            raise SystemExit("No code package in state.json - run deploy/04b_package_and_upload.py first.")
        return ({"codeConfiguration": {
            "code": {"s3": {"bucket": state["code_bucket"], "prefix": state["code_key"]}},
            "runtime": state.get("code_runtime", "PYTHON_3_13"),
            # opentelemetry-instrument wraps the process so traces reach CloudWatch
            "entryPoint": ["opentelemetry-instrument", "main.py"],
        }}, f"code s3://{state['code_bucket']}/{state['code_key']}")
    if not has_image:
        raise SystemExit("No image in state.json - run deploy/04_build_and_push.sh first.")
    return ({"containerConfiguration": {"containerUri": state["image_uri"]}}, f"image {state['image_uri']}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--code", dest="mode", action="store_const", const="code", help="deploy the S3 zip")
    g.add_argument("--container", dest="mode", action="store_const", const="container", help="deploy the ECR image")
    ap.add_argument("--recreate", action="store_true",
                    help="delete and recreate when switching between the code and container routes")
    args = ap.parse_args()

    state = load_state()
    require(state, "runtime_role_arn", "memory_id")
    cp = control()
    artifact, described = pick_artifact(state, args.mode)
    print(f"==> Artifact: {described}")

    params = dict(
        agentRuntimeArtifact=artifact,
        roleArn=state["runtime_role_arn"],
        networkConfiguration={"networkMode": "PUBLIC"},
        protocolConfiguration={"serverProtocol": "HTTP"},
        lifecycleConfiguration={"idleRuntimeSessionTimeout": 900, "maxLifetime": 3600},
        environmentVariables={
            "AWS_REGION": REGION,
            "MODEL_ID": MODEL_ID,
            "MEMORY_ID": state["memory_id"],
            "METRICS_NAMESPACE": METRICS_NAMESPACE,
            "SERVICE_NAME": AGENT_NAME,
            "METRICS_ENABLED": "true",
            # Tells the ADOT distro to run in agent-observability mode. The runtime
            # normally injects the OTEL exporter settings itself; this flag is what
            # switches the distro on to use them.
            "AGENT_OBSERVABILITY_ENABLED": "true",
            # Optional tuning, set in config.env (or config.prod.env) rather than code,
            # so the same artifact behaves differently per environment.
            **{k: os.environ[k] for k in (
                "SPECIALIST_MODEL_ID",   # cheaper/faster model for the three specialists
                "REVIEW_PASS_SCORE",     # quality gate threshold, default 7
                "MAX_REVISIONS",         # rewrite budget, default 1
            ) if os.getenv(k)},
        },
        description="Multi-agent POC: orchestrator + researcher/writer/reviewer",
    )

    runtime_id = state.get("agent_runtime_id")
    if not runtime_id:  # maybe it exists already
        for r in cp.list_agent_runtimes().get("agentRuntimes", []):
            if r["agentRuntimeName"] == AGENT_NAME:
                runtime_id = r["agentRuntimeId"]

    if runtime_id:
        try:
            current = cp.get_agent_runtime(agentRuntimeId=runtime_id)
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
            # state.json points at a runtime that no longer exists (deleted here or
            # in the console). Forget it and create a fresh one.
            print(f"    runtime {runtime_id} no longer exists - creating a new one")
            current, runtime_id = None, None

    if runtime_id:
        existing = artifact_kind(current)
        wanted = "code" if "codeConfiguration" in artifact else "container"
        if existing != wanted:
            if not args.recreate:
                sys.exit(
                    f"The runtime '{AGENT_NAME}' was created from a {existing} artifact and you are "
                    f"deploying a {wanted} one.\nAWS does not allow the artifact type to change in "
                    f"place, so the runtime has to be replaced.\n\n"
                    f"  Replace it (keeps the name, new runtime id and ARN):\n"
                    f"    python deploy/05_deploy_runtime.py --recreate\n\n"
                    f"  Or keep both, by giving the new one its own name:\n"
                    f"    AGENT_NAME=research_orchestrator_code python deploy/05_deploy_runtime.py\n\n"
                    f"Memory, IAM roles and the dashboard are separate and survive either way.")
            delete_and_wait(cp, runtime_id)
            runtime_id = None

    if runtime_id:
        print(f"==> Updating runtime {runtime_id}")
        res = cp.update_agent_runtime(agentRuntimeId=runtime_id, **params)
    else:
        print(f"==> Creating runtime {AGENT_NAME}")
        res = cp.create_agent_runtime(agentRuntimeName=AGENT_NAME, **params)
        runtime_id = res["agentRuntimeId"]

    final = wait_for(lambda: cp.get_agent_runtime(agentRuntimeId=runtime_id),
                     ok={"READY"}, bad={"CREATE_FAILED", "UPDATE_FAILED"}, what="runtime")
    save_state(agent_runtime_id=runtime_id,
               agent_runtime_arn=final["agentRuntimeArn"],
               agent_runtime_version=final.get("agentRuntimeVersion"))
    print(f"\nREADY  arn={final['agentRuntimeArn']}  version={final.get('agentRuntimeVersion')}")
    print(f"Logs:  /aws/bedrock-agentcore/runtimes/{runtime_id}-DEFAULT")
