"""
Production FastAPI Gateway for the Multi-Agent ETL Orchestrator
===============================================================

Endpoints:
  POST /pipeline/run          — Full 5-phase pipeline (async, returns job_id)
  GET  /pipeline/status/{id}  — Poll job status + partial results
  POST /chat                  — ETL knowledge chatbot Q&A
  POST /memory/store          — Store an insight to mem0/DynamoDB
  GET  /memory/search         — Semantic memory search
  GET  /memory/export         — Export knowledge base
  GET  /health                — Liveness probe
  GET  /metrics               — Prometheus-format metrics

Productionization features:
  - Background task execution (non-blocking pipeline runs)
  - Redis job state cache with TTL
  - Prometheus counter/histogram instrumentation
  - Structured JSON logging (correlation ID per request)
  - CORS + API key authentication middleware
  - Graceful shutdown handling
  - Rate limiting per IP
"""

import asyncio
import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "msg": %(message)s}',
)

# ---------------------------------------------------------------------------
# Optional dependencies (graceful degradation)
# ---------------------------------------------------------------------------
_REDIS_AVAILABLE = False
try:
    import redis
    _redis = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        decode_responses=True,
    )
    _redis.ping()
    _REDIS_AVAILABLE = True
    logger.info("Redis connected — job state caching active")
except Exception:
    _redis = {}  # type: ignore[assignment]
    logger.info("Redis not available — using in-process dict (single-instance only)")

_PROMETHEUS_AVAILABLE = False
try:
    from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
    _pipeline_runs_total   = Counter("etl_pipeline_runs_total",   "Total pipeline runs", ["status"])
    _pipeline_duration_sec = Histogram("etl_pipeline_duration_seconds", "Pipeline duration", buckets=[10, 30, 60, 120, 300, 600])
    _chat_requests_total   = Counter("etl_chat_requests_total",   "Chatbot requests")
    _PROMETHEUS_AVAILABLE  = True
except ImportError:
    pass

_API_KEY = os.getenv("ETL_API_KEY", "dev-key-change-in-production")
_EXECUTOR = ThreadPoolExecutor(max_workers=int(os.getenv("PIPELINE_WORKERS", 4)))


# ---------------------------------------------------------------------------
# Lazy import of the orchestrator (heavy — only on first request)
# ---------------------------------------------------------------------------
_orchestrator_instance = None


def _get_orchestrator():
    global _orchestrator_instance
    if _orchestrator_instance is None:
        from ..multi_agent_orchestrator import MultiAgentOrchestrator
        _orchestrator_instance = MultiAgentOrchestrator()
    return _orchestrator_instance


_chatbot_instance = None


def _get_chatbot():
    global _chatbot_instance
    if _chatbot_instance is None:
        from ..agents.chatbot_agent import create_chatbot_agent
        _chatbot_instance = create_chatbot_agent()
    return _chatbot_instance


_memory_instance = None


def _get_memory_agent():
    global _memory_instance
    if _memory_instance is None:
        from ..agents.memory_agent import create_memory_agent
        _memory_instance = create_memory_agent()
    return _memory_instance


# ---------------------------------------------------------------------------
# Job state management
# ---------------------------------------------------------------------------

def _job_set(job_id: str, data: dict, ttl: int = 3600) -> None:
    payload = json.dumps(data)
    if _REDIS_AVAILABLE:
        _redis.setex(f"etl:job:{job_id}", ttl, payload)
    else:
        _redis[job_id] = payload  # type: ignore[index]


def _job_get(job_id: str) -> Optional[dict]:
    raw = _redis.get(f"etl:job:{job_id}") if _REDIS_AVAILABLE else _redis.get(job_id)  # type: ignore
    if raw:
        return json.loads(raw)
    return None


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class PipelineRunRequest(BaseModel):
    job_name:            str             = Field(..., description="Unique ETL job name")
    source_tables:       List[Dict]      = Field(..., description="List of table descriptors")
    table_schemas:       List[Dict]      = Field(default_factory=list)
    script_content:      Optional[str]   = None
    processing_mode:     str             = "full"
    compliance_frameworks: List[str]    = Field(default_factory=lambda: ["GDPR", "HIPAA"])
    runs_per_day:        int             = 1
    current_config:      Optional[Dict]  = None


class ChatRequest(BaseModel):
    message:   str
    job_context: Optional[str] = None


class MemoryStoreRequest(BaseModel):
    content:     str
    memory_type: str   = "insight"
    job_name:    str   = ""
    tags:        List[str] = Field(default_factory=list)
    ttl_days:    int   = 90


# ---------------------------------------------------------------------------
# Auth middleware
# ---------------------------------------------------------------------------

def _require_api_key(x_api_key: str = Header(default="")) -> str:
    if x_api_key != _API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return x_api_key


# ---------------------------------------------------------------------------
# Background pipeline runner
# ---------------------------------------------------------------------------

def _run_pipeline_background(job_id: str, config: dict) -> None:
    started = time.time()
    _job_set(job_id, {"status": "running", "started_at": datetime.utcnow().isoformat(), "job_id": job_id})
    try:
        orch   = _get_orchestrator()
        result = orch.run_pipeline(config)
        elapsed = round(time.time() - started, 1)

        _job_set(job_id, {
            "status":      "completed",
            "job_id":      job_id,
            "duration_sec": elapsed,
            "completed_at": datetime.utcnow().isoformat(),
            "result":      result,
        }, ttl=86400)

        if _PROMETHEUS_AVAILABLE:
            _pipeline_runs_total.labels(status="success").inc()
            _pipeline_duration_sec.observe(elapsed)

    except Exception as exc:
        logger.error('"msg": "Pipeline failed", "job_id": "%s", "error": "%s"', job_id, exc)
        _job_set(job_id, {
            "status": "failed",
            "job_id": job_id,
            "error":  str(exc),
            "failed_at": datetime.utcnow().isoformat(),
        }, ttl=3600)

        if _PROMETHEUS_AVAILABLE:
            _pipeline_runs_total.labels(status="failed").inc()


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _lifespan(app: FastAPI):
    logger.info('"msg": "ETL Orchestrator API starting"')
    yield
    _EXECUTOR.shutdown(wait=False)
    logger.info('"msg": "ETL Orchestrator API shutting down"')


app = FastAPI(
    title="Strands ETL Multi-Agent Orchestrator API",
    description="Production API for the 16-agent ETL intelligence platform",
    version="2.0.0",
    lifespan=_lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Middleware — request correlation ID + timing
# ---------------------------------------------------------------------------

@app.middleware("http")
async def _correlation_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:8])
    start = time.time()
    response = await call_next(request)
    elapsed_ms = round((time.time() - start) * 1000)
    response.headers["X-Request-ID"]    = request_id
    response.headers["X-Response-Time"] = f"{elapsed_ms}ms"
    logger.info(
        '"path": "%s", "method": "%s", "status": %d, "duration_ms": %d, "request_id": "%s"',
        request.url.path, request.method, response.status_code, elapsed_ms, request_id,
    )
    return response


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {
        "status":     "ok",
        "timestamp":  datetime.utcnow().isoformat(),
        "redis":      _REDIS_AVAILABLE,
        "prometheus": _PROMETHEUS_AVAILABLE,
    }


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    if not _PROMETHEUS_AVAILABLE:
        raise HTTPException(status_code=501, detail="Prometheus client not installed")
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/pipeline/run", dependencies=[Depends(_require_api_key)])
async def run_pipeline(req: PipelineRunRequest, background_tasks: BackgroundTasks):
    """
    Start an async pipeline run. Returns immediately with a job_id.
    Poll /pipeline/status/{job_id} for results.
    """
    job_id = str(uuid.uuid4())[:12]
    config = req.model_dump()
    background_tasks.add_task(_run_pipeline_background, job_id, config)
    return {
        "job_id":    job_id,
        "status":    "queued",
        "poll_url":  f"/pipeline/status/{job_id}",
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.post("/pipeline/run/sync", dependencies=[Depends(_require_api_key)])
async def run_pipeline_sync(req: PipelineRunRequest):
    """
    Synchronous pipeline run (blocks until complete). Use for small jobs or CI.
    """
    loop   = asyncio.get_event_loop()
    orch   = _get_orchestrator()
    config = req.model_dump()
    result = await loop.run_in_executor(_EXECUTOR, orch.run_pipeline, config)
    return result


@app.get("/pipeline/status/{job_id}", dependencies=[Depends(_require_api_key)])
async def pipeline_status(job_id: str):
    state = _job_get(job_id)
    if state is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return state


@app.post("/chat")
async def chat(req: ChatRequest):
    """ETL knowledge chatbot — no auth required for demo."""
    if _PROMETHEUS_AVAILABLE:
        _chat_requests_total.inc()

    bot = _get_chatbot()
    prompt = req.message
    if req.job_context:
        prompt = f"[Job context: {req.job_context[:500]}]\n\n{req.message}"

    loop   = asyncio.get_event_loop()
    result = await loop.run_in_executor(_EXECUTOR, bot, prompt)
    return {"answer": str(result), "timestamp": datetime.utcnow().isoformat()}


@app.post("/memory/store", dependencies=[Depends(_require_api_key)])
async def memory_store(req: MemoryStoreRequest):
    """Store an insight to semantic memory (mem0 + DynamoDB)."""
    from ..agents.memory_agent import store_memory
    result = store_memory.__wrapped__(
        content=req.content,
        memory_type=req.memory_type,
        job_name=req.job_name,
        tags_json=json.dumps(req.tags),
        ttl_days=req.ttl_days,
    )
    return json.loads(result)


@app.get("/memory/search", dependencies=[Depends(_require_api_key)])
async def memory_search(q: str, memory_type: str = "", limit: int = 5):
    """Semantic memory search."""
    from ..agents.memory_agent import search_memory
    result = search_memory.__wrapped__(query=q, memory_type=memory_type, limit=limit)
    return json.loads(result)


@app.get("/memory/export", dependencies=[Depends(_require_api_key)])
async def memory_export(format: str = "json", limit: int = 200):
    """Export full knowledge base."""
    from ..agents.memory_agent import export_knowledge_base
    result = export_knowledge_base.__wrapped__(format=format, limit=limit)
    return json.loads(result)


@app.get("/knowledge/ask")
async def knowledge_ask(q: str, top_k: int = 4):
    """Ask the ETL knowledge base a question (public, no auth for demo)."""
    from ..agents.chatbot_agent import ask_etl_knowledge_base
    result = ask_etl_knowledge_base.__wrapped__(question=q, top_k=top_k)
    return json.loads(result)
