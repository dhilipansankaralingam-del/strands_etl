"""The three specialist agents, exposed to the orchestrator as tools
("agents-as-tools" pattern).

    Researcher  -> gathers facts from the knowledge base (+ model knowledge)
    Writer      -> turns research notes into a structured report
    Reviewer    -> scores the draft 0-10 and lists concrete fixes

Each specialist is a separate Strands `Agent` with its own system prompt,
model and tools. Every call is timed and its token usage recorded.
"""
import json

from pydantic import BaseModel, Field
from strands import Agent, tool
from strands.models import BedrockModel

from . import config
from .metrics import MetricsRecorder
from .tools import search_knowledge_base, word_count

RESEARCHER_PROMPT = """You are the RESEARCHER in a multi-agent team.
Given a topic, call search_knowledge_base (1-3 times with different queries) and
produce concise research notes:
- 5-10 bullet-point facts, each tagged with its source id, e.g. [kb-003], or [general] if from your own knowledge
- a short 'Gaps / uncertainties' section
Do NOT write the final report. Be factual and concise."""

WRITER_PROMPT = """You are the WRITER in a multi-agent team.
Turn research notes into a clear markdown report with:
# Title, a 2-3 sentence executive summary, 3-5 '##' sections, and a 'Sources' list.
Only use facts present in the research notes and keep the source tags.
Respect the requested audience and length. If reviewer feedback is supplied,
address every point. Use word_count to check the length before answering.
Return ONLY the report."""

REVIEWER_PROMPT = """You are the REVIEWER (quality gate) in a multi-agent team.
Critically evaluate a draft report against the user's request on:
accuracy vs. research notes, completeness, structure, clarity, and citation of sources.
Score 0-10 (7+ = publishable). List specific, actionable issues."""


class Review(BaseModel):
    score: float = Field(description="Overall quality score from 0 to 10")
    verdict: str = Field(description="'approve' if score >= 7 else 'revise'")
    issues: list[str] = Field(default_factory=list, description="Concrete, actionable problems to fix")
    strengths: list[str] = Field(default_factory=list)


def _model(model_id: str, max_tokens: int = 2048) -> BedrockModel:
    return BedrockModel(model_id=model_id, region_name=config.AWS_REGION, max_tokens=max_tokens, temperature=0.3)


def _usage(result) -> dict:
    try:
        return dict(result.metrics.accumulated_usage)
    except Exception:
        return {}


def build_specialist_tools(recorder: MetricsRecorder, trace_attrs: dict, model_factory=_model) -> list:
    """Create the specialist tools bound to one request's metrics recorder.

    `model_factory` is injectable so tests can swap in a fake model.
    """

    def _agent(name: str, prompt: str, tools: list | None = None, max_tokens: int = 2048) -> Agent:
        return Agent(
            name=name,
            model=model_factory(config.SPECIALIST_MODEL_ID, max_tokens),
            system_prompt=prompt,
            tools=tools or [],
            callback_handler=None,  # no stdout streaming inside the container
            trace_attributes={**trace_attrs, "agent.role": name},
        )

    @tool
    def researcher(topic: str) -> str:
        """Research a topic and return fact-checked research notes with source ids.

        Args:
            topic: What to research, including any specific angle the user asked for.
        """
        with recorder.track_agent("researcher") as info:
            result = _agent("researcher", RESEARCHER_PROMPT, [search_knowledge_base])(f"Topic: {topic}")
            info["usage"] = _usage(result)
        return str(result)

    @tool
    def writer(request: str, research_notes: str, reviewer_feedback: str = "") -> str:
        """Write (or rewrite) a markdown report from research notes.

        Args:
            request: The user's original request incl. audience/length preferences.
            research_notes: Notes produced by the researcher tool.
            reviewer_feedback: Issues from the reviewer to address on a rewrite (optional).
        """
        prompt = f"USER REQUEST:\n{request}\n\nRESEARCH NOTES:\n{research_notes}"
        if reviewer_feedback:
            prompt += f"\n\nREVIEWER FEEDBACK TO ADDRESS:\n{reviewer_feedback}"
            recorder.revisions += 1
        with recorder.track_agent("writer") as info:
            result = _agent("writer", WRITER_PROMPT, [word_count], max_tokens=4096)(prompt)
            info["usage"] = _usage(result)
        return str(result)

    @tool
    def reviewer(request: str, research_notes: str, draft: str) -> str:
        """Score a draft report 0-10 and list issues. Returns JSON with score, verdict, issues.

        Args:
            request: The user's original request.
            research_notes: The research notes the draft should be based on.
            draft: The draft report to review.
        """
        prompt = f"USER REQUEST:\n{request}\n\nRESEARCH NOTES:\n{research_notes}\n\nDRAFT:\n{draft}"
        with recorder.track_agent("reviewer") as info:
            result = _agent("reviewer", REVIEWER_PROMPT)(prompt, structured_output_model=Review)
            info["usage"] = _usage(result)
        review: Review = result.structured_output or Review(score=0, verdict="revise", issues=["unparseable review"])
        review.verdict = "approve" if review.score >= config.REVIEW_PASS_SCORE else "revise"
        recorder.review_scores.append(review.score)
        recorder.put("ReviewScore", review.score)
        return json.dumps(review.model_dump())

    return [researcher, writer, reviewer]
