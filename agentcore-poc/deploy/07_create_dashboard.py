"""Step 7 - CloudWatch dashboard combining:

  * AgentCore built-in runtime metrics (namespace `bedrock-agentcore`)
  * our custom per-agent metrics      (namespace METRICS_NAMESPACE, from agent/metrics.py)
  * evaluation scores                 (published by eval/run_eval.py)

The richer trace view lives in CloudWatch -> GenAI Observability -> Bedrock AgentCore.

If a widget is rejected by the API, the script prints the exact validation message and
publishes the dashboard without that widget rather than failing outright.
"""
import json

import boto3
from botocore.exceptions import ClientError

from common import AGENT_NAME, METRICS_NAMESPACE, PROJECT, REGION, load_state

NS = METRICS_NAMESPACE
AGENTS = ["orchestrator", "researcher", "writer", "reviewer"]
cw = boto3.client("cloudwatch", region_name=REGION)


def w(title, metrics, x, y, stat="Average", width=12, height=6, view="timeSeries"):
    return {"type": "metric", "x": x, "y": y, "width": width, "height": height,
            "properties": {"title": title, "region": REGION, "stat": stat, "period": 300,
                           "view": view, "metrics": metrics}}


def per_agent(metric, stat):
    return [[NS, metric, "Agent", a, {"stat": stat, "label": a}] for a in AGENTS]


def search(expr, stat, label, wid):
    """A SEARCH expression entry.

    Three rules the API enforces, all of which are easy to get wrong:
      1. the expression object must sit inside its own inner array;
      2. a namespace containing anything non-alphanumeric (bedrock-agentcore) must be
         wrapped in double quotes inside the schema braces;
      3. several metric names are matched with `MetricName="a" OR MetricName="b"`,
         not with a parenthesised list.
    """
    return [[{"expression": f"SEARCH('{expr}', '{stat}', 300)", "label": label, "id": wid}]]


def build_widgets(state):
    return [
        {"type": "text", "x": 0, "y": 0, "width": 24, "height": 2, "properties": {"markdown":
            f"## {PROJECT} multi-agent POC\nRuntime `{state.get('agent_runtime_id', '?')}` · "
            f"Traces: CloudWatch → **GenAI Observability** → Bedrock AgentCore"}},
        w("Requests / errors (custom)", [[NS, "Requests", "Service", AGENT_NAME, {"stat": "Sum"}],
                                         [NS, "RequestErrors", "Service", AGENT_NAME, {"stat": "Sum"}]], 0, 2, "Sum"),
        w("End-to-end latency ms (p50/p90)", [[NS, "RequestLatencyMs", "Service", AGENT_NAME, {"stat": "p50"}],
                                              [NS, "RequestLatencyMs", "Service", AGENT_NAME, {"stat": "p90"}]], 12, 2),
        w("Latency per agent (avg ms)", per_agent("AgentLatencyMs", "Average"), 0, 8),
        w("Output tokens per agent (sum)", per_agent("AgentOutputTokens", "Sum"), 12, 8, "Sum"),
        w("Input tokens per agent (sum)", per_agent("AgentInputTokens", "Sum"), 0, 14, "Sum"),
        w("Agent errors", per_agent("AgentErrors", "Sum"), 12, 14, "Sum"),
        w("Reviewer score (0-10) & revisions", [[NS, "ReviewScore", "Service", AGENT_NAME, {"stat": "Average"}],
                                                [NS, "RevisionCount", "Service", AGENT_NAME, {"stat": "Average"}]], 0, 20),
        w("Evaluation scores (eval/run_eval.py)",
          search(f'{{{NS},Evaluator}} MetricName="EvalScore"', "Average", "Eval", "e1"), 12, 20),
        w("AgentCore runtime: invocations",
          search('{"bedrock-agentcore"} MetricName="Invocations"', "Sum", "Invocations", "e2"), 0, 26, "Sum"),
        w("AgentCore runtime: errors & throttles",
          search('{"bedrock-agentcore"} MetricName="SystemErrors" OR MetricName="UserErrors" '
                 'OR MetricName="Throttles"', "Sum", "Errors", "e3"), 12, 26, "Sum"),
    ]


def put(name, widgets):
    res = cw.put_dashboard(DashboardName=name, DashboardBody=json.dumps({"widgets": widgets}))
    for msg in res.get("DashboardValidationMessages", []):
        print(f"  warning: {msg.get('DataPath')} {msg.get('Message')}")
    return res


def put_with_fallback(name, widgets):
    try:
        return put(name, widgets)
    except ClientError as exc:
        print("The dashboard body was rejected. Validation messages:")
        for msg in exc.response.get("DashboardValidationMessages", []):
            print(f"  - {msg.get('DataPath')}: {msg.get('Message')}")
        print("Testing widgets one at a time...")
        good = []
        for i, widget in enumerate(widgets):
            try:
                cw.put_dashboard(DashboardName=f"{name}-probe",
                                 DashboardBody=json.dumps({"widgets": [widget]}))
                good.append(widget)
            except ClientError as one:
                title = widget.get("properties", {}).get("title", f"widget {i}")
                detail = "; ".join(m.get("Message", "") for m in one.response.get("DashboardValidationMessages", []))
                print(f"  dropping '{title}': {detail or one.response['Error']['Message']}")
        try:
            cw.delete_dashboards(DashboardNames=[f"{name}-probe"])
        except ClientError:
            pass
        if not good:
            raise
        return put(name, good)


if __name__ == "__main__":
    state = load_state()
    name = f"{PROJECT}-dashboard"
    put_with_fallback(name, build_widgets(state))
    print(f"Dashboard: https://{REGION}.console.aws.amazon.com/cloudwatch/home?region={REGION}#dashboards/dashboard/{name}")
    print(f"GenAI Observability: https://{REGION}.console.aws.amazon.com/cloudwatch/home?region={REGION}#gen-ai-observability/agent-core")
