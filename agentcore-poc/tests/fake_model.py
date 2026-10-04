"""A scripted Strands model so the whole multi-agent flow can be tested
offline (no AWS credentials, no Bedrock cost)."""
import itertools
import json
from typing import Any

from strands.models import Model

_ids = itertools.count(1)


def _usage_event(inp=100, out=50):
    return {"metadata": {"usage": {"inputTokens": inp, "outputTokens": out, "totalTokens": inp + out},
                         "metrics": {"latencyMs": 1}}}


def _text_events(text):
    yield {"messageStart": {"role": "assistant"}}
    yield {"contentBlockStart": {"start": {}}}
    yield {"contentBlockDelta": {"delta": {"text": text}}}
    yield {"contentBlockStop": {}}
    yield {"messageStop": {"stopReason": "end_turn"}}
    yield _usage_event()


def _tool_events(name, args):
    yield {"messageStart": {"role": "assistant"}}
    yield {"contentBlockStart": {"start": {"toolUse": {"toolUseId": f"t{next(_ids)}", "name": name}}}}
    yield {"contentBlockDelta": {"delta": {"toolUse": {"input": json.dumps(args)}}}}
    yield {"contentBlockStop": {}}
    yield {"messageStop": {"stopReason": "tool_use"}}
    yield _usage_event()


def _tool_results(messages):
    out = []
    for m in messages:
        for block in m.get("content", []):
            if "toolResult" in block:
                txt = "".join(c.get("text", "") for c in block["toolResult"].get("content", []))
                out.append(txt)
    return out


class FakeModel(Model):
    review_scores = [6.0, 8.5]   # first review fails -> exercises the revision loop

    def __init__(self, *_, **__):
        self.config = {}

    def update_config(self, **kw):
        self.config.update(kw)

    def get_config(self):
        return self.config

    async def structured_output(self, output_model, prompt, system_prompt=None, **kwargs):
        raise NotImplementedError

    async def stream(self, messages, tool_specs=None, system_prompt=None, **kwargs: Any):
        role = (system_prompt or "").split("\n")[0]
        tool_names = {t["name"] for t in (tool_specs or [])}
        results = _tool_results(messages)
        events = self._script(role, tool_names, results, messages)
        for e in events:
            yield e

    def _script(self, role, tools, results, messages):
        first_user = next(c["text"] for m in messages if m["role"] == "user" for c in m["content"] if "text" in c)
        if "ORCHESTRATOR" in role:
            n = len(results)
            if n == 0:
                return _tool_events("researcher", {"topic": first_user})
            if n == 1:
                return _tool_events("writer", {"request": first_user, "research_notes": results[0]})
            if n in (2, 4):
                return _tool_events("reviewer", {"request": first_user, "research_notes": results[0], "draft": results[-1]})
            if n == 3:
                review = json.loads(results[2])
                if review["verdict"] == "revise":
                    return _tool_events("writer", {"request": first_user, "research_notes": results[0],
                                                   "reviewer_feedback": "; ".join(review["issues"])})
            return _text_events(results[-2] + f"\n\n_Review score: {json.loads(results[-1])['score']}/10_")
        if "RESEARCHER" in role:
            if not results:
                return _tool_events("search_knowledge_base", {"query": "agentcore memory"})
            return _text_events(f"- AgentCore Memory has STM and LTM [kb-003]\nKB: {results[0][:80]}")
        if "WRITER" in role:
            return _text_events("# Report\n\nSummary.\n\n## Section\nFacts [kb-003]\n\n## Sources\n- kb-003")
        if "REVIEWER" in role and "Review" in tools:
            score = FakeModel.review_scores.pop(0) if FakeModel.review_scores else 9.0
            return _tool_events("Review", {"score": score, "verdict": "x",
                                           "issues": ["add more detail"] if score < 7 else [], "strengths": ["clear"]})
        return _text_events("ok")


def fake_factory(model_id, max_tokens=2048):
    return FakeModel()
