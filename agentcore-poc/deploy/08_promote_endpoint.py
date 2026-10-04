"""Step 8 (production) - point a named endpoint at a specific runtime version.

Why this exists
---------------
Every deploy creates a new immutable runtime *version*. The DEFAULT endpoint always
follows the newest one, which is right for dev and wrong for production: it means a
deploy is instantly live for users, with no gate in between.

A named endpoint (e.g. "prod") is a pointer you move deliberately:

    version 4  <- DEFAULT   (whatever you deployed last; your test traffic)
    version 3  <- prod      (what users get, until you promote)

Promotion and rollback are the same one-second operation - repoint the endpoint.
Callers keep using one ARN + qualifier and never learn a version number.

    python deploy/08_promote_endpoint.py                 # show current state
    python deploy/08_promote_endpoint.py --promote       # prod -> newest version
    python deploy/08_promote_endpoint.py --promote --version 3   # or a specific one
    python deploy/08_promote_endpoint.py --rollback      # prod -> previous version

The production worker Lambda invokes with qualifier = this endpoint name, so
nothing else changes when you promote.
"""
import argparse
import os
import sys
import time

from botocore.exceptions import ClientError

from common import control, load_state, require, save_state

ENDPOINT_NAME = os.getenv("ENDPOINT_NAME", "prod")


def versions(cp, runtime_id: str) -> list[str]:
    """Version numbers for this runtime, newest first."""
    out, token = [], None
    while True:
        kw = {"agentRuntimeId": runtime_id, **({"nextToken": token} if token else {})}
        res = cp.list_agent_runtime_versions(**kw)
        out += [r["agentRuntimeVersion"] for r in res.get("agentRuntimes", [])]
        token = res.get("nextToken")
        if not token:
            break
    return sorted(out, key=lambda v: int(v) if v.isdigit() else 0, reverse=True)


def endpoint_state(cp, runtime_id: str) -> dict | None:
    try:
        return cp.get_agent_runtime_endpoint(agentRuntimeId=runtime_id, endpointName=ENDPOINT_NAME)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ResourceNotFoundException":
            return None
        raise


def wait_ready(cp, runtime_id: str, timeout: int = 300) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ep = endpoint_state(cp, runtime_id)
        status = (ep or {}).get("status")
        print(f"    {ENDPOINT_NAME}: {status} (live={ep.get('liveVersion')} target={ep.get('targetVersion')})")
        if status == "READY":
            return ep
        if status in ("CREATE_FAILED", "UPDATE_FAILED"):
            sys.exit(f"Endpoint ended in {status}: {ep.get('failureReason')}")
        time.sleep(10)
    sys.exit("Timed out waiting for the endpoint.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--promote", action="store_true", help="move the endpoint to a newer version")
    g.add_argument("--rollback", action="store_true", help="move the endpoint back one version")
    ap.add_argument("--version", help="exact version to point at (default: newest)")
    args = ap.parse_args()

    state = load_state()
    require(state, "agent_runtime_id", "agent_runtime_arn")
    cp = control()
    runtime_id = state["agent_runtime_id"]

    all_versions = versions(cp, runtime_id)
    current = endpoint_state(cp, runtime_id)
    live = current.get("liveVersion") if current else None

    print(f"Runtime  : {runtime_id}")
    print(f"Versions : {', '.join(all_versions) or 'none'}   (newest first)")
    print(f"Endpoint : {ENDPOINT_NAME} -> {live or 'does not exist yet'}")

    if not (args.promote or args.rollback):
        print("\nNothing changed. Pass --promote or --rollback to move the endpoint.")
        if current:
            print(f"\nProduction callers should use:\n  arn      {state['agent_runtime_arn']}\n"
                  f"  qualifier {ENDPOINT_NAME}")
        sys.exit(0)

    if args.rollback:
        if not live or live not in all_versions:
            sys.exit("No live version to roll back from.")
        idx = all_versions.index(live)
        if idx + 1 >= len(all_versions):
            sys.exit(f"{live} is the oldest version - nothing to roll back to.")
        target = all_versions[idx + 1]
    else:
        target = args.version or (all_versions[0] if all_versions else None)
        if not target:
            sys.exit("No versions exist yet - deploy first with 05_deploy_runtime.py.")
        if target == live:
            sys.exit(f"{ENDPOINT_NAME} already points at version {target}.")

    print(f"\n==> Pointing '{ENDPOINT_NAME}' at version {target}")
    if current:
        cp.update_agent_runtime_endpoint(agentRuntimeId=runtime_id, endpointName=ENDPOINT_NAME,
                                         agentRuntimeVersion=target)
    else:
        cp.create_agent_runtime_endpoint(agentRuntimeId=runtime_id, name=ENDPOINT_NAME,
                                         agentRuntimeVersion=target,
                                         description="Production traffic - promoted deliberately")
    final = wait_ready(cp, runtime_id)
    save_state(prod_endpoint_name=ENDPOINT_NAME,
               prod_endpoint_arn=final["agentRuntimeEndpointArn"],
               prod_live_version=final.get("liveVersion"))

    print(f"\n'{ENDPOINT_NAME}' now serves version {final.get('liveVersion')}")
    print(f"Production callers use qualifier '{ENDPOINT_NAME}' against {state['agent_runtime_arn']}")
    print(f"Roll back with: python deploy/08_promote_endpoint.py --rollback")
