"""Custom business metrics published to CloudWatch.

AgentCore Observability already captures traces, latency, token usage and errors
automatically (via ADOT). These custom metrics add *per-agent* and *quality*
signals that are useful on a dashboard:

  AgentLatencyMs      (dim: Agent)       - wall-clock time of each agent call
  AgentInputTokens    (dim: Agent)
  AgentOutputTokens   (dim: Agent)
  AgentErrors         (dim: Agent)
  ReviewScore         (dim: Service)     - 0-10 score given by the reviewer agent
  RevisionCount       (dim: Service)     - how many rewrite loops were needed
  RequestLatencyMs    (dim: Service)     - end-to-end orchestrator latency
"""
import logging
import threading
import time
from contextlib import contextmanager

from . import config

log = logging.getLogger(__name__)


class MetricsRecorder:
    """Collects metrics for one request and flushes them in a single API call."""

    def __init__(self, session_id: str):
        self.session_id = session_id
        self._data: list[dict] = []
        self._lock = threading.Lock()
        self.agent_calls: list[dict] = []   # also returned to the caller for visibility
        self.review_scores: list[float] = []
        self.revisions = 0

    def put(self, name: str, value: float, unit: str = "None", dims: dict | None = None):
        dims = dims or {"Service": config.SERVICE_NAME}
        with self._lock:
            self._data.append({
                "MetricName": name,
                "Value": float(value),
                "Unit": unit,
                "Dimensions": [{"Name": k, "Value": v} for k, v in dims.items()],
            })

    @contextmanager
    def track_agent(self, agent_name: str):
        """Time an agent call; caller sets info['usage'] from the Strands result."""
        info: dict = {"agent": agent_name, "usage": {}}
        start = time.perf_counter()
        try:
            yield info
        except Exception:
            self.put("AgentErrors", 1, "Count", {"Agent": agent_name})
            raise
        finally:
            ms = (time.perf_counter() - start) * 1000
            usage = info.get("usage") or {}
            self.put("AgentLatencyMs", ms, "Milliseconds", {"Agent": agent_name})
            self.put("AgentInputTokens", usage.get("inputTokens", 0), "Count", {"Agent": agent_name})
            self.put("AgentOutputTokens", usage.get("outputTokens", 0), "Count", {"Agent": agent_name})
            with self._lock:
                self.agent_calls.append({
                    "agent": agent_name,
                    "latency_ms": round(ms),
                    "input_tokens": usage.get("inputTokens", 0),
                    "output_tokens": usage.get("outputTokens", 0),
                })

    def flush(self):
        if not config.METRICS_ENABLED or not self._data:
            return
        try:
            import boto3
            cw = boto3.client("cloudwatch", region_name=config.AWS_REGION)
            for i in range(0, len(self._data), 500):  # API limit: 1000 metrics per call
                cw.put_metric_data(Namespace=config.METRICS_NAMESPACE, MetricData=self._data[i:i + 500])
        except Exception as exc:  # metrics must never break the agent
            log.warning("Failed to publish custom metrics: %s", exc)
        finally:
            self._data.clear()
