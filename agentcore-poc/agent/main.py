"""AgentCore Runtime entrypoint.

`BedrockAgentCoreApp` provides the HTTP contract AgentCore Runtime expects:
    POST /invocations   (payload -> invoke())
    GET  /ping          (health check)
on port 8080.

Request payload:
    {"prompt": "...", "actor_id": "alice"}          # actor_id = end-user id for memory
Response:
    {"result": "<markdown report>", "session_id": "...", "actor_id": "...",
     "review_scores": [...], "revisions": n, "agent_calls": [...], "latency_ms": n}
"""
import logging
import re
import time
import uuid

from bedrock_agentcore.runtime import BedrockAgentCoreApp

from . import config
from .metrics import MetricsRecorder
from .orchestrator import build_orchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("multiagent")


def _log_telemetry_state() -> None:
    """One line at startup saying whether tracing is actually wired up.

    If spans never show in CloudWatch, this tells you which half is broken:
    either the runtime did not inject the OTEL settings (env vars empty), or it
    did and no real TracerProvider was installed (the ADOT distro is missing or
    opentelemetry-instrument is not in the entry point).
    """
    import os
    otel = {k: v for k, v in os.environ.items()
            if k.startswith("OTEL_") or k == "AGENT_OBSERVABILITY_ENABLED"}
    try:
        from opentelemetry import trace
        provider = type(trace.get_tracer_provider()).__name__
    except Exception as exc:  # noqa: BLE001
        provider = f"unavailable ({exc})"
    log.info("TELEMETRY provider=%s otel_env=%s", provider, otel or "NONE")


_log_telemetry_state()

app = BedrockAgentCoreApp()

_SAFE_ID = re.compile(r"[^a-zA-Z0-9_\-]")


def _clean_id(value: str, default: str) -> str:
    value = _SAFE_ID.sub("-", (value or "").strip())[:100]
    return value if value and value[0].isalnum() else default


@app.entrypoint
def invoke(payload: dict, context) -> dict:
    prompt = (payload or {}).get("prompt", "")
    if not isinstance(prompt, str) or not prompt.strip():
        return {"error": "payload must contain a non-empty 'prompt' string"}

    actor_id = _clean_id(payload.get("actor_id", ""), "anonymous-user")
    # AgentCore Runtime passes the runtimeSessionId through the request context.
    session_id = _clean_id(getattr(context, "session_id", None) or payload.get("session_id", ""), f"local-{uuid.uuid4()}")

    recorder = MetricsRecorder(session_id)
    start = time.perf_counter()
    log.info("invoke session=%s actor=%s prompt_chars=%d", session_id, actor_id, len(prompt))
    try:
        orchestrator = build_orchestrator(session_id, actor_id, recorder)
        with recorder.track_agent("orchestrator") as info:
            result = orchestrator(prompt)
            info["usage"] = dict(result.metrics.accumulated_usage)
        answer = str(result)
        status = "ok"
    except Exception as exc:
        log.exception("orchestration failed")
        recorder.put("RequestErrors", 1, "Count")
        answer, status = f"Sorry, the agent team failed: {exc}", "error"
    finally:
        latency_ms = (time.perf_counter() - start) * 1000
        recorder.put("RequestLatencyMs", latency_ms, "Milliseconds")
        recorder.put("RevisionCount", recorder.revisions, "Count")
        recorder.put("Requests", 1, "Count")
        recorder.flush()

    return {
        "status": status,
        "result": answer,
        "session_id": session_id,
        "actor_id": actor_id,
        "review_scores": recorder.review_scores,
        "revisions": recorder.revisions,
        "agent_calls": recorder.agent_calls,
        "latency_ms": round(latency_ms),
    }


if __name__ == "__main__":
    app.run()
