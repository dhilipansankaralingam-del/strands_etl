"""Offline / batch evaluation of the deployed multi-agent system.

Two layers of evaluation per test case:

  1. Deterministic checks (computed here, no LLM):
       - required keywords present, word limit respected, has headings + sources,
         reviewer agent approved (score >= 7)
  2. AgentCore Evaluations (LLM-as-judge on the real OpenTelemetry trace):
       - Builtin.GoalSuccessRate      (session, uses our assertions as ground truth)
       - Builtin.Helpfulness          (trace)
       - Builtin.Faithfulness         (trace)
       - Builtin.ToolSelectionAccuracy(tool call)
       - Builtin.TrajectoryInOrderMatch (session, expected tool order researcher->writer->reviewer)

Results -> eval/results/<timestamp>.json + .md, and EvalScore metrics to CloudWatch.

Usage
  python eval/run_eval.py                       # invoke + evaluate everything
  python eval/run_eval.py --only memory-basics  # one case
  python eval/run_eval.py --evaluate-only eval/results/<file>.json   # re-score later
Spans take a few minutes to land in CloudWatch, so the script retries evaluation.
"""
import argparse
import json
import re
import statistics
import sys
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent / "deploy"))

import boto3  # noqa: E402
from bedrock_agentcore.evaluation.client import EvaluationClient, ReferenceInputs  # noqa: E402

from common import AGENT_NAME, METRICS_NAMESPACE, REGION, data, load_state, require  # noqa: E402

EVALUATORS = [
    "Builtin.GoalSuccessRate",
    "Builtin.Helpfulness",
    "Builtin.Faithfulness",
    "Builtin.ToolSelectionAccuracy",
    "Builtin.TrajectoryInOrderMatch",
]
RESULTS_DIR = ROOT / "results"


# ---------------------------------------------------------------- invoke
def invoke(arn: str, prompt: str, actor: str, session: str) -> dict:
    resp = data().invoke_agent_runtime(
        agentRuntimeArn=arn, qualifier="DEFAULT", runtimeSessionId=session,
        payload=json.dumps({"prompt": prompt, "actor_id": actor}).encode(),
        contentType="application/json", accept="application/json")
    return json.loads(resp["response"].read())


def deterministic_checks(case: dict, out: dict) -> dict:
    text = out.get("result", "")
    words = len(text.split())
    checks = {
        "status_ok": out.get("status") == "ok",
        "keywords": all(k.lower() in text.lower() for k in case.get("must_contain", [])),
        "word_limit": words <= case.get("max_words", 10_000),
        "has_headings": bool(re.search(r"^#{1,2} ", text, re.M)),
        "has_sources": "source" in text.lower() or bool(re.search(r"\[kb-\d+\]", text)),
        "reviewer_approved": bool(out.get("review_scores")) and out["review_scores"][-1] >= 7,
    }
    return {"words": words, "checks": checks, "pass_rate": sum(checks.values()) / len(checks)}


def run_cases(cases: list, arn: str) -> list:
    rows = []
    for case in cases:
        actor = f"eval-{case['id']}-{uuid.uuid4().hex[:6]}"
        session = f"eval-{case['id']}-{uuid.uuid4()}"
        print(f"\n▶ {case['id']}  (actor={actor})")
        for turn in case.get("setup_turns", []):
            print(f"   setup turn: {turn[:70]}...")
            invoke(arn, turn, actor, session)
        if case.get("setup_turns") and case.get("wait_seconds_after_setup"):
            print(f"   waiting {case['wait_seconds_after_setup']}s for long-term memory extraction...")
            time.sleep(case["wait_seconds_after_setup"])
        if case.get("new_session"):
            session = f"eval-{case['id']}-{uuid.uuid4()}"   # proves LTM crosses sessions
        t0 = time.time()
        out = invoke(arn, case["prompt"], actor, session)
        det = deterministic_checks(case, out)
        print(f"   {time.time() - t0:.1f}s  words={det['words']}  review={out.get('review_scores')}  "
              f"checks={sum(det['checks'].values())}/{len(det['checks'])}")
        rows.append({"case": case, "session_id": session, "actor_id": actor, "output": out, "deterministic": det})
    return rows


# ---------------------------------------------------------------- evaluate
def agentcore_evaluate(rows: list, runtime_id: str, max_wait_min: int = 12) -> None:
    ev = EvaluationClient(region_name=REGION)
    pending = [r for r in rows if not r.get("agentcore_eval")]
    deadline = time.time() + max_wait_min * 60
    while pending and time.time() < deadline:
        for r in list(pending):
            c = r["case"]
            try:
                results = ev.run(
                    evaluator_ids=EVALUATORS,
                    session_id=r["session_id"],
                    agent_id=runtime_id,
                    look_back_time=timedelta(hours=6),
                    reference_inputs=ReferenceInputs(assertions=c.get("assertions"),
                                                     expected_trajectory=c.get("expected_trajectory")),
                )
            except Exception as exc:
                print(f"   {c['id']}: evaluation error {exc}")
                results = []
            if results:
                r["agentcore_eval"] = summarise(results)
                pending.remove(r)
                print(f"   ✓ {c['id']}: " + ", ".join(f"{k.split('.')[-1]}={v['mean']}"
                                                  for k, v in r["agentcore_eval"].items()))
        if pending:
            print(f"   spans not in CloudWatch yet for {[p['case']['id'] for p in pending]} - retrying in 60s")
            time.sleep(60)
    for r in pending:
        print(f"   ✗ {r['case']['id']}: no spans found - re-run later with --evaluate-only")


def summarise(results: list) -> dict:
    by: dict = {}
    for res in results:
        if res.get("errorCode"):
            by.setdefault(res["evaluatorId"], {"values": [], "labels": [], "explanations": [], "errors": []})["errors"].append(
                res.get("errorMessage"))
            continue
        e = by.setdefault(res["evaluatorId"], {"values": [], "labels": [], "explanations": [], "errors": []})
        if res.get("value") is not None:
            e["values"].append(res["value"])
        e["labels"].append(res.get("label"))
        e["explanations"].append((res.get("explanation") or "")[:300])
    for e in by.values():
        e["mean"] = round(statistics.mean(e["values"]), 3) if e["values"] else None
    return by


# ---------------------------------------------------------------- report
def publish_metrics(rows: list) -> None:
    md = []
    for r in rows:
        md.append({"MetricName": "DeterministicPassRate", "Value": r["deterministic"]["pass_rate"],
                   "Dimensions": [{"Name": "Service", "Value": AGENT_NAME}]})
        for ev_id, e in (r.get("agentcore_eval") or {}).items():
            if e["mean"] is not None:
                md.append({"MetricName": "EvalScore", "Value": e["mean"],
                           "Dimensions": [{"Name": "Evaluator", "Value": ev_id}]})
    if md:
        cw = boto3.client("cloudwatch", region_name=REGION)
        for i in range(0, len(md), 500):
            cw.put_metric_data(Namespace=METRICS_NAMESPACE, MetricData=md[i:i + 500])
        print(f"Published {len(md)} eval metrics to CloudWatch namespace {METRICS_NAMESPACE}")


def write_report(rows: list, path: Path) -> None:
    path.write_text(json.dumps(rows, indent=2, default=str))
    lines = [f"# Eval run {path.stem}\n", "| case | words | det. pass | review | " + " | ".join(
        e.split(".")[-1] for e in EVALUATORS) + " |", "|---" * (4 + len(EVALUATORS)) + "|"]
    for r in rows:
        ae = r.get("agentcore_eval") or {}
        scores = [str(ae.get(e, {}).get("mean", "–")) for e in EVALUATORS]
        rv = r["output"].get("review_scores") or ["–"]
        lines.append(f"| {r['case']['id']} | {r['deterministic']['words']} | "
                     f"{r['deterministic']['pass_rate']:.0%} | {rv[-1]} | " + " | ".join(scores) + " |")
    lines.append("\nFailed deterministic checks:")
    for r in rows:
        failed = [k for k, v in r["deterministic"]["checks"].items() if not v]
        if failed:
            lines.append(f"- {r['case']['id']}: {', '.join(failed)}")
    path.with_suffix(".md").write_text("\n".join(lines) + "\n")
    print(f"\nResults: {path}\n         {path.with_suffix('.md')}\n")
    print("\n".join(lines))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="run a single case id")
    ap.add_argument("--evaluate-only", help="re-run AgentCore evaluation on a previous results json")
    ap.add_argument("--skip-agentcore", action="store_true", help="deterministic checks only")
    args = ap.parse_args()

    state = load_state()
    require(state, "agent_runtime_arn", "agent_runtime_id")
    RESULTS_DIR.mkdir(exist_ok=True)

    if args.evaluate_only:
        out_path = Path(args.evaluate_only)
        rows = json.loads(out_path.read_text())
    else:
        cases = json.loads((ROOT / "dataset.json").read_text())
        if args.only:
            cases = [c for c in cases if c["id"] == args.only]
        rows = run_cases(cases, state["agent_runtime_arn"])
        out_path = RESULTS_DIR / f"{datetime.now():%Y%m%d-%H%M%S}.json"
        write_report(rows, out_path)  # save early in case evaluation is interrupted

    if not args.skip_agentcore:
        print("\n== AgentCore Evaluations (waiting for spans to reach CloudWatch, usually 2-5 min) ==")
        time.sleep(0 if args.evaluate_only else 120)
        agentcore_evaluate(rows, state["agent_runtime_id"])
    publish_metrics(rows)
    write_report(rows, out_path)
