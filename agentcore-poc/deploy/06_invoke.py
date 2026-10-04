"""Step 6 - Trigger the deployed multi-agent system.

Examples
  python deploy/06_invoke.py "Write a short report on AgentCore Memory for engineers"
  python deploy/06_invoke.py --actor alice "I prefer bullet points and max 200 words"
  python deploy/06_invoke.py --actor alice --session <id> "Now do one on AgentCore Observability"

--session reuses a runtime session (same microVM + short-term memory).
A *new* session with the same --actor still sees long-term memory
(preferences/facts), which is extracted ~1 minute after a conversation.
"""
import argparse
import json
import sys
import threading
import time
import uuid

from botocore.exceptions import ClientError, ReadTimeoutError

from common import data, load_state, require


def _ticker(stop: threading.Event):
    """Print elapsed seconds so a slow run doesn't look like a hang."""
    start = time.time()
    while not stop.wait(5):
        sys.stderr.write(f"\r  working... {int(time.time() - start)}s ")
        sys.stderr.flush()
    sys.stderr.write("\r" + " " * 28 + "\r")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("prompt")
    ap.add_argument("--actor", default="demo-user", help="end-user id used for memory")
    ap.add_argument("--session", default=None, help="runtime session id (>=33 chars); new one if omitted")
    ap.add_argument("--raw", action="store_true", help="print the raw JSON response")
    ap.add_argument("--endpoint", default="DEFAULT",
                    help="runtime endpoint/qualifier - DEFAULT is the newest version, "
                         "'prod' is whatever you promoted (see 08_promote_endpoint.py)")
    args = ap.parse_args()

    state = load_state()
    require(state, "agent_runtime_arn")
    session_id = args.session or f"session-{uuid.uuid4()}"   # 44 chars

    print(f"Invoking {args.actor} / {session_id[:24]}... via {args.endpoint}  "
          f"(a full research-write-review run takes 40-90s)")
    stop = threading.Event()
    threading.Thread(target=_ticker, args=(stop,), daemon=True).start()
    started = time.time()
    try:
        resp = data().invoke_agent_runtime(
            agentRuntimeArn=state["agent_runtime_arn"],
            qualifier=args.endpoint,
            runtimeSessionId=session_id,
            payload=json.dumps({"prompt": args.prompt, "actor_id": args.actor}).encode(),
            contentType="application/json",
            accept="application/json",
        )
        body = json.loads(resp["response"].read())
    except ReadTimeoutError:
        stop.set()
        raise SystemExit(
            "The runtime did not answer within the client timeout.\n"
            "Watch what the agent is actually doing:\n"
            f"  aws logs tail /aws/bedrock-agentcore/runtimes/"
            f"{state.get('agent_runtime_id', '<runtime-id>')}-DEFAULT --follow")
    except ClientError as exc:
        stop.set()
        code = exc.response["Error"]["Code"]
        hint = {
            "ThrottlingException": "Bedrock is throttling. Wait a minute, or use a model with more capacity.",
            "AccessDeniedException": "Check Bedrock model access and the runtime role's bedrock:InvokeModel grant.",
            "ResourceNotFoundException": "The runtime ARN in deploy/state.json no longer exists.",
        }.get(code, "")
        raise SystemExit(f"{code}: {exc.response['Error']['Message']}\n{hint}")
    finally:
        stop.set()
    elapsed = time.time() - started
    if args.raw:
        print(json.dumps(body, indent=2))
    else:
        print(body.get("result", body))
        print("\n" + "-" * 70)
        print(f"session_id   : {session_id}")
        print(f"actor_id     : {body.get('actor_id')}")
        print(f"review scores: {body.get('review_scores')}   revisions: {body.get('revisions')}")
        print(f"latency      : {body.get('latency_ms')} ms agent-side / {elapsed:.1f}s wall clock")
        for c in body.get("agent_calls", []):
            print(f"  {c['agent']:<13} {c['latency_ms']:>7} ms  in={c['input_tokens']:<6} out={c['output_tokens']}")
    print(f"\nReuse this session:  --actor {args.actor} --session {session_id}")
