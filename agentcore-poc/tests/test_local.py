"""Offline smoke test of the full orchestrator -> researcher/writer/reviewer flow.

Run:  METRICS_ENABLED=false python -m pytest -q tests/        (from the project root)
"""
import os

os.environ.setdefault("METRICS_ENABLED", "false")
os.environ["MEMORY_ID"] = ""

from agent import orchestrator as orch_mod  # noqa: E402
from agent import main  # noqa: E402
from agent.metrics import MetricsRecorder  # noqa: E402
from tests.fake_model import FakeModel, fake_factory  # noqa: E402


def test_full_flow_with_revision():
    FakeModel.review_scores = [6.0, 8.5]
    rec = MetricsRecorder("test-session")
    agent = orch_mod.build_orchestrator("test-session", "tester", rec, model_factory=fake_factory)
    result = agent("Write a short report on AgentCore Memory for engineers")
    text = str(result)
    assert "# Report" in text
    assert rec.review_scores == [6.0, 8.5]
    assert rec.revisions == 1
    agents = [c["agent"] for c in rec.agent_calls]
    assert agents == ["researcher", "writer", "reviewer", "writer", "reviewer"]


def test_entrypoint_contract(monkeypatch):
    FakeModel.review_scores = [9.0]
    real = orch_mod.build_orchestrator
    monkeypatch.setattr(main, "build_orchestrator",
                        lambda s, a, r: real(s, a, r, model_factory=fake_factory))

    class Ctx:
        session_id = "sess-0123456789-0123456789-0123456789"

    out = main.invoke({"prompt": "Explain AgentCore", "actor_id": "alice@example.com"}, Ctx())
    assert out["status"] == "ok"
    assert out["session_id"] == Ctx.session_id
    assert out["actor_id"] == "alice-example-com"
    assert out["revisions"] == 0
    assert "orchestrator" in [c["agent"] for c in out["agent_calls"]]


def test_rejects_empty_prompt():
    assert "error" in main.invoke({"prompt": ""}, None)
