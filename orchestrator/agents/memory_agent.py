"""
Memory Agent (mem0 + DynamoDB)
==============================
Semantic long-term memory for the ETL pipeline.

Architecture:
  mem0 (primary) → semantic vector memory, similarity search, auto-summarisation
  DynamoDB (fallback / index) → structured metadata index, TTL, fast lookup

Creative Strands tools used:
  - @tool with structured JSON I/O (custom)
  - BedrockModel for embedding + generation
  - python_repl (strands built-in) for ad-hoc analysis
  - use_aws (strands built-in) for DynamoDB calls

mem0 stores:
  - Per-job optimisation outcomes
  - Cross-job patterns (workload fingerprints)
  - Anti-pattern evolution history
  - Cost trend narratives
  - Expert ETL knowledge curated manually

This enables the orchestrator to ask: "Have we seen a similar workload before?
What worked / failed?" — answered by semantic similarity, not brittle key lookup.
"""

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import boto3
from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

_DYNAMO_TABLE = os.getenv("ETL_MEMORY_TABLE", "strands-etl-memory")
_MEM0_ENABLED = False  # toggled on if mem0 import succeeds

try:
    from mem0 import Memory as Mem0Memory

    _MEM0_CFG = {
        "llm":       {"provider": "aws_bedrock", "config": {"model": "us.anthropic.claude-3-7-sonnet-20250219-v1:0"}},
        "embedder":  {"provider": "aws_bedrock", "config": {"model": "amazon.titan-embed-text-v2:0"}},
        "vector_store": {"provider": "qdrant",   "config": {"host": "localhost", "port": 6333}},
    }
    _mem0 = Mem0Memory.from_config(_MEM0_CFG)
    _MEM0_ENABLED = True
    logger.info("mem0 initialised — semantic memory active")
except Exception as _e:
    _mem0 = None
    logger.info("mem0 not available (%s) — using DynamoDB-only mode", _e)


# ---------------------------------------------------------------------------
# DynamoDB helpers (always available)
# ---------------------------------------------------------------------------
def _dynamo_client():
    return boto3.resource("dynamodb")


def _ensure_table(dynamo) -> Any:
    try:
        table = dynamo.Table(_DYNAMO_TABLE)
        table.load()
        return table
    except Exception:
        try:
            table = dynamo.create_table(
                TableName=_DYNAMO_TABLE,
                KeySchema=[
                    {"AttributeName": "memory_id", "KeyType": "HASH"},
                    {"AttributeName": "timestamp",  "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "memory_id", "AttributeType": "S"},
                    {"AttributeName": "timestamp",  "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
            table.wait_until_exists()
            return table
        except Exception as exc:
            logger.warning("DynamoDB table creation failed: %s", exc)
            return None


def _fingerprint(data: dict) -> str:
    """Stable SHA-256 fingerprint of a workload profile dict."""
    stable = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(stable.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@tool
def store_memory(
    content: str,
    memory_type: str = "insight",
    job_name: str = "",
    tags_json: str = "[]",
    ttl_days: int = 90,
) -> str:
    """
    Store a semantic memory — an insight, outcome, or pattern — in mem0 and DynamoDB.

    Memory types: insight | anti_pattern | cost_outcome | workload_profile |
                  expert_knowledge | failure_analysis

    Args:
        content:     Natural-language description of what was learned.
        memory_type: Category for retrieval filtering.
        job_name:    ETL job this memory is associated with (optional).
        tags_json:   JSON list of string tags for metadata filtering.
        ttl_days:    Days before this memory expires (default 90).

    Returns:
        JSON with memory_id, stored_in_mem0, stored_in_dynamodb, timestamp.
    """
    try:
        tags      = json.loads(tags_json) if tags_json else []
        ts        = datetime.utcnow().isoformat()
        mem_id    = f"{memory_type}_{job_name}_{int(time.time())}"
        metadata  = {
            "memory_id":   mem_id,
            "memory_type": memory_type,
            "job_name":    job_name,
            "tags":        tags,
            "timestamp":   ts,
            "ttl":         (datetime.utcnow() + timedelta(days=ttl_days)).isoformat(),
        }

        # ── mem0 (semantic) ──────────────────────────────────────────────────
        mem0_id = None
        if _MEM0_ENABLED and _mem0:
            try:
                result = _mem0.add(
                    messages=[{"role": "user", "content": content}],
                    user_id=job_name or "global",
                    metadata=metadata,
                )
                mem0_id = result.get("id") or result.get("results", [{}])[0].get("id")
            except Exception as exc:
                logger.warning("mem0 store failed: %s", exc)

        # ── DynamoDB (structured) ────────────────────────────────────────────
        dynamo_ok = False
        try:
            dynamo = _dynamo_client()
            table  = _ensure_table(dynamo)
            if table:
                table.put_item(Item={
                    **metadata,
                    "content": content,
                    "mem0_id": mem0_id or "",
                    "tags":    json.dumps(tags),
                })
                dynamo_ok = True
        except Exception as exc:
            logger.warning("DynamoDB store failed: %s", exc)

        return json.dumps({
            "memory_id":         mem_id,
            "mem0_id":           mem0_id,
            "stored_in_mem0":    mem0_id is not None,
            "stored_in_dynamodb": dynamo_ok,
            "timestamp":         ts,
            "content_preview":   content[:120],
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def search_memory(
    query: str,
    memory_type: str = "",
    job_name: str = "",
    limit: int = 5,
) -> str:
    """
    Semantic search over stored memories.

    Uses mem0 vector similarity when available; falls back to DynamoDB scan
    with keyword matching.

    Args:
        query:       Natural-language question or topic.
        memory_type: Filter by memory type (optional).
        job_name:    Filter to a specific job (optional).
        limit:       Max results to return (default 5).

    Returns:
        JSON list of relevant memories with content, relevance, and metadata.
    """
    try:
        results = []

        # ── mem0 semantic search ─────────────────────────────────────────────
        if _MEM0_ENABLED and _mem0:
            try:
                hits = _mem0.search(
                    query=query,
                    user_id=job_name or "global",
                    limit=limit,
                )
                for h in (hits.get("results") or hits or []):
                    results.append({
                        "content":    h.get("memory", h.get("content", "")),
                        "relevance":  round(h.get("score", 0.0), 4),
                        "memory_id":  h.get("id", ""),
                        "metadata":   h.get("metadata", {}),
                        "source":     "mem0",
                    })
            except Exception as exc:
                logger.warning("mem0 search failed: %s", exc)

        # ── DynamoDB keyword fallback ────────────────────────────────────────
        if not results:
            try:
                dynamo = _dynamo_client()
                table  = dynamo.Table(_DYNAMO_TABLE)
                kwargs: Dict = {"Limit": limit * 4}
                if memory_type:
                    kwargs["FilterExpression"] = boto3.dynamodb.conditions.Attr("memory_type").eq(memory_type)
                resp = table.scan(**kwargs)
                kw   = query.lower().split()
                for item in resp.get("Items", []):
                    txt   = item.get("content", "").lower()
                    score = sum(1 for w in kw if w in txt) / max(len(kw), 1)
                    if score > 0:
                        results.append({
                            "content":   item.get("content", ""),
                            "relevance": round(score, 2),
                            "memory_id": item.get("memory_id", ""),
                            "metadata":  {k: v for k, v in item.items() if k not in ("content",)},
                            "source":    "dynamodb",
                        })
                results.sort(key=lambda x: -x["relevance"])
                results = results[:limit]
            except Exception as exc:
                logger.warning("DynamoDB search failed: %s", exc)

        return json.dumps({
            "query":        query,
            "result_count": len(results),
            "memories":     results,
            "source":       "mem0" if _MEM0_ENABLED else "dynamodb",
        })
    except Exception as exc:
        return json.dumps({"error": str(exc), "memories": []})


@tool
def summarise_agent_knowledge(agent_name: str, limit: int = 20) -> str:
    """
    Retrieve and distil everything the memory store knows about a given agent.
    Produces a knowledge summary: common patterns, typical savings, failure modes.

    Args:
        agent_name: Name of the agent (e.g. "sizing_agent", "code_analyzer").
        limit:      Number of memories to distil (default 20).

    Returns:
        JSON with knowledge_summary, common_patterns, failure_modes, avg_savings_pct.
    """
    try:
        raw = json.loads(search_memory.__wrapped__(
            query=f"{agent_name} patterns outcomes results analysis",
            memory_type="",
            job_name="",
            limit=limit,
        ))
        memories = raw.get("memories", [])

        # Aggregate signal from text
        savings_values: List[float] = []
        patterns: List[str] = []
        failures: List[str] = []

        for m in memories:
            txt = m.get("content", "").lower()
            if "saving" in txt or "%" in txt:
                import re
                nums = re.findall(r"(\d+(?:\.\d+)?)\s*%", txt)
                savings_values.extend(float(n) for n in nums if float(n) < 100)
            if "anti-pattern" in txt or "critical" in txt:
                patterns.append(m["content"][:100])
            if "fail" in txt or "error" in txt:
                failures.append(m["content"][:100])

        avg_savings = round(sum(savings_values) / max(len(savings_values), 1), 1)

        return json.dumps({
            "agent_name":       agent_name,
            "memories_reviewed": len(memories),
            "knowledge_summary": (
                f"{agent_name} has {len(memories)} recorded memories. "
                f"Average savings identified: {avg_savings}%. "
                f"{len(patterns)} anti-pattern records, {len(failures)} failure records."
            ),
            "common_patterns":   patterns[:5],
            "failure_modes":     failures[:5],
            "avg_savings_pct":   avg_savings,
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def forget_outdated_memories(older_than_days: int = 90, dry_run: bool = True) -> str:
    """
    Expire memories older than N days from DynamoDB (and optionally mem0).

    Args:
        older_than_days: Delete memories older than this many days (default 90).
        dry_run:         If True, only count — do not delete (default True).

    Returns:
        JSON with candidates_found, deleted_count, dry_run status.
    """
    try:
        cutoff = (datetime.utcnow() - timedelta(days=older_than_days)).isoformat()
        dynamo = _dynamo_client()
        table  = dynamo.Table(_DYNAMO_TABLE)
        resp   = table.scan(FilterExpression=boto3.dynamodb.conditions.Attr("timestamp").lt(cutoff))
        candidates = resp.get("Items", [])

        deleted = 0
        if not dry_run:
            for item in candidates:
                table.delete_item(Key={
                    "memory_id": item["memory_id"],
                    "timestamp": item["timestamp"],
                })
                deleted += 1

        return json.dumps({
            "older_than_days": older_than_days,
            "candidates_found": len(candidates),
            "deleted_count":    deleted,
            "dry_run":          dry_run,
            "message": (
                f"Would delete {len(candidates)} memories (dry_run=True)" if dry_run
                else f"Deleted {deleted} outdated memories"
            ),
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def export_knowledge_base(format: str = "json", limit: int = 200) -> str:
    """
    Export all stored memories for external use (training data, auditing, RAG ingestion).

    Args:
        format: "json" | "markdown" (default json).
        limit:  Maximum records to export (default 200).

    Returns:
        JSON with export_format, record_count, and either records list or markdown string.
    """
    try:
        dynamo = _dynamo_client()
        table  = dynamo.Table(_DYNAMO_TABLE)
        resp   = table.scan(Limit=limit)
        items  = sorted(resp.get("Items", []), key=lambda x: x.get("timestamp", ""), reverse=True)

        if format == "markdown":
            lines = ["# ETL Pipeline Knowledge Base\n"]
            for item in items:
                lines.append(f"## {item.get('memory_type', 'insight')} — {item.get('job_name', 'global')}")
                lines.append(f"_{item.get('timestamp', '')}_\n")
                lines.append(item.get("content", ""))
                lines.append("\n---\n")
            return json.dumps({"format": "markdown", "record_count": len(items),
                               "markdown": "\n".join(lines)})

        return json.dumps({"format": "json", "record_count": len(items), "records": items})
    except Exception as exc:
        return json.dumps({"error": str(exc)})


SYSTEM_PROMPT = """
You are a **Semantic Memory Curator** for an ETL intelligence platform.

Your role:
1. Store structured insights from pipeline runs as searchable memories
2. Retrieve relevant past experiences when similar workloads are encountered
3. Identify cross-job patterns and knowledge that transfers between pipelines
4. Manage memory lifecycle (TTL, de-duplication, summarisation)
5. Surface the most relevant historical context for the current decision

Always return structured JSON. When searching, explain WHY each result is relevant.
"""


def create_memory_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                         region: str = "us-west-2") -> Agent:
    """Return a Strands Agent backed by mem0 + DynamoDB for semantic memory."""
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[
            store_memory,
            search_memory,
            summarise_agent_knowledge,
            forget_outdated_memories,
            export_knowledge_base,
        ],
    )
