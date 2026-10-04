"""The orchestrator agent: plans the work and delegates to the specialists.

It is the only agent wired to AgentCore Memory:
  * short-term memory  -> conversation events for this runtime session
  * long-term memory   -> user preferences / facts / session summaries that the
                          Memory service extracts asynchronously and that are
                          injected into the prompt on later sessions.
"""
import logging

from strands import Agent
from strands.agent.conversation_manager import SlidingWindowConversationManager

from . import config
from .metrics import MetricsRecorder
from .specialists import _model, build_specialist_tools

log = logging.getLogger(__name__)

ORCHESTRATOR_PROMPT = f"""You are the ORCHESTRATOR of a research-and-report team.
Your specialists are tools:
  1. researcher(topic)                                  -> research notes
  2. writer(request, research_notes, reviewer_feedback) -> markdown draft
  3. reviewer(request, research_notes, draft)           -> JSON {{score, verdict, issues}}

Procedure for any request that needs a report or explanation:
  a. Call researcher once with a well-scoped topic.
  b. Call writer with the user's request (include audience / length / style preferences you
     know from memory) and the research notes.
  c. Call reviewer on the draft.
  d. If verdict == "revise", call writer again with the issues as reviewer_feedback, then
     reviewer again. Do at most {config.MAX_REVISIONS} revision(s).
  e. Reply with the final report verbatim, followed by one line:
     "_Review score: <score>/10 after <n> revision(s)._"

For small talk or questions about the user's preferences/past conversations, answer
directly from memory without calling tools.
Remember and respect user preferences (tone, audience, length) given earlier."""


def _memory_session_manager(session_id: str, actor_id: str):
    """AgentCore Memory session manager, or None when MEMORY_ID is not set."""
    if not config.MEMORY_ID:
        log.info("MEMORY_ID not set - running without AgentCore Memory")
        return None
    from bedrock_agentcore.memory.integrations.strands.config import AgentCoreMemoryConfig, RetrievalConfig
    from bedrock_agentcore.memory.integrations.strands.session_manager import AgentCoreMemorySessionManager

    mem_cfg = AgentCoreMemoryConfig(
        memory_id=config.MEMORY_ID,
        session_id=session_id,
        actor_id=actor_id,
        # Namespaces must match the strategies created in deploy/03_create_memory.py
        retrieval_config={
            "/preferences/{actorId}/": RetrievalConfig(top_k=5, relevance_score=0.3),
            "/facts/{actorId}/": RetrievalConfig(top_k=5, relevance_score=0.4),
            "/summaries/{actorId}/{sessionId}/": RetrievalConfig(top_k=3, relevance_score=0.4),
        },
        filter_restored_tool_context=True,  # keep restored history compact
    )
    return AgentCoreMemorySessionManager(mem_cfg, region_name=config.AWS_REGION)


def build_orchestrator(session_id: str, actor_id: str, recorder: MetricsRecorder, model_factory=_model) -> Agent:
    trace_attrs = {
        "session.id": session_id,   # groups spans per session in CloudWatch GenAI Observability
        "actor.id": actor_id,
        "app.name": config.SERVICE_NAME,
    }
    return Agent(
        name="orchestrator",
        model=model_factory(config.MODEL_ID, 4096),
        system_prompt=ORCHESTRATOR_PROMPT,
        tools=build_specialist_tools(recorder, trace_attrs, model_factory),
        session_manager=_memory_session_manager(session_id, actor_id),
        conversation_manager=SlidingWindowConversationManager(window_size=30),
        callback_handler=None,
        trace_attributes={**trace_attrs, "agent.role": "orchestrator"},
    )
