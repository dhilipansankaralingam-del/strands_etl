"""
ETL Knowledge Chatbot Agent
============================
A locally-trained RAG chatbot that answers ETL questions using:
  - Curated ETL expert knowledge (seeded at startup)
  - Historical pipeline run summaries (indexed from LearningAgent vectors)
  - Bedrock Knowledge Bases (when configured)
  - ChromaDB local vector store (zero-infrastructure option)

Creative Strands SDK features used:
  - BedrockModel for generation
  - Custom @tool functions for retrieval-augmented Q&A
  - Conversation memory via message history
  - Python embeddings via Amazon Titan (Bedrock)

Local mode uses ChromaDB + sentence-transformers (no AWS dependency beyond model calls).
Cloud mode adds Bedrock Knowledge Bases for enterprise-scale retrieval.

Usage:
  agent = create_chatbot_agent()
  reply = agent("Why is my Glue job running out of memory?")
"""

import hashlib
import json
import logging
import os
from typing import Any, Dict, List, Optional

import boto3
from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Vector store backend (ChromaDB local or Bedrock KB)
# ---------------------------------------------------------------------------
_CHROMA_AVAILABLE = False
_chroma_client    = None
_chroma_col       = None

try:
    import chromadb
    from chromadb.config import Settings

    _chroma_client = chromadb.Client(Settings(
        chroma_db_impl="duckdb+parquet",
        persist_directory=os.getenv("ETL_KB_PATH", "/tmp/etl_knowledge_base"),
        anonymized_telemetry=False,
    ))
    _chroma_col = _chroma_client.get_or_create_collection(
        name="etl_knowledge",
        metadata={"hnsw:space": "cosine"},
    )
    _CHROMA_AVAILABLE = True
    logger.info("ChromaDB local vector store ready")
except Exception as _e:
    logger.info("ChromaDB not available (%s) — knowledge base in fallback mode", _e)

_BEDROCK_KB_ID = os.getenv("BEDROCK_KB_ID", "")  # set if using Bedrock Knowledge Bases


def _embed_text(text: str) -> List[float]:
    """Embed text using Amazon Titan Embed v2 via Bedrock."""
    try:
        br = boto3.client("bedrock-runtime")
        resp = br.invoke_model(
            modelId="amazon.titan-embed-text-v2:0",
            body=json.dumps({"inputText": text[:8000]}),
            contentType="application/json",
            accept="application/json",
        )
        return json.loads(resp["body"].read())["embedding"]
    except Exception as exc:
        logger.warning("Titan embed failed (%s) — using keyword fallback", exc)
        return []


def _doc_id(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:20]


# ---------------------------------------------------------------------------
# Seed knowledge — curated ETL expert facts (indexed once at startup)
# ---------------------------------------------------------------------------
_SEED_KNOWLEDGE = [
    {
        "id": "oom_heap",
        "text": (
            "If a Glue job runs out of memory (OOM / JVM heap > 90%), the primary fixes are: "
            "(1) upgrade worker type from G.2X to G.4X, (2) reduce spark.sql.shuffle.partitions, "
            "(3) replace Python UDFs with Spark built-ins, (4) add .cache() before reused DataFrames, "
            "(5) enable spark.memory.offHeap. OOM is almost always caused by UDFs, uncontrolled "
            "collect(), or cartesian joins on large tables."
        ),
        "tags": ["oom", "memory", "heap", "glue", "critical"],
    },
    {
        "id": "skew_causes",
        "text": (
            "Data skew causes one partition to be 10–100× larger than others, making the job hang "
            "at the last few tasks. Root causes: (1) low-cardinality join keys (status, country), "
            "(2) NULL values concentrated on one partition, (3) time-based partitions with hot dates. "
            "Fixes: enable AQE skewJoin, add salt keys, use SKEW JOIN hints, or broadcast small side."
        ),
        "tags": ["skew", "performance", "join", "aqe"],
    },
    {
        "id": "tiny_files",
        "text": (
            "Tiny files (< 10 MB each) in S3/Iceberg create excessive read overhead because each "
            "file requires a separate S3 API call. Fix: run OPTIMIZE / REWRITE_DATA_FILES to compact "
            "into 128–256 MB targets. Enable auto-compaction for streaming workloads. Set "
            "write.target-file-size-bytes=134217728 in Iceberg table properties."
        ),
        "tags": ["tiny_files", "iceberg", "s3", "performance", "optimize"],
    },
    {
        "id": "aqe_guide",
        "text": (
            "Adaptive Query Execution (AQE) is the single highest-ROI Spark config change. "
            "Enable: spark.sql.adaptive.enabled=true, spark.sql.adaptive.coalescePartitions.enabled=true, "
            "spark.sql.adaptive.skewJoin.enabled=true. AQE automatically optimises shuffle partitions, "
            "handles skew joins, and converts sort-merge joins to broadcast when runtime stats allow. "
            "Expected improvement: 20–40% on typical Glue ETL workloads."
        ),
        "tags": ["aqe", "spark", "config", "optimization", "quick_win"],
    },
    {
        "id": "glue_worker_sizing",
        "text": (
            "Glue worker sizing rule of thumb: G.1X (16 GB, 4 vCPU) for < 10 GB jobs. "
            "G.2X (32 GB, 8 vCPU) for 10–200 GB. G.4X (64 GB, 16 vCPU) for 200–1 TB with complex joins. "
            "G.8X (128 GB, 32 vCPU) for ML feature engineering or extreme shuffles. "
            "Worker count = ceil(effective_gb / 15) × 1.2 for join overhead. Always enable Flex "
            "execution for non-SLA batch jobs to save 34%."
        ),
        "tags": ["glue", "workers", "sizing", "cost", "configuration"],
    },
    {
        "id": "iceberg_maintenance",
        "text": (
            "Iceberg table maintenance schedule: expire_snapshots every 7 days (retain 30 days), "
            "REWRITE_DATA_FILES weekly for active tables, REWRITE_MANIFESTS monthly. "
            "Symptoms of neglected maintenance: write amplification > 5×, snapshot count > 30, "
            "manifest files > 1000. Use $snapshots, $files, $manifests system tables in Athena "
            "to diagnose. Uncontrolled snapshots can double scan time."
        ),
        "tags": ["iceberg", "maintenance", "vacuum", "snapshots", "delta"],
    },
    {
        "id": "pii_masking",
        "text": (
            "PII masking in PySpark before writing to S3: use SHA-256 hashing for irreversible "
            "tokenisation (sha2(col, 256)), AES encryption for reversible masking (aes_encrypt), "
            "or format-preserving encryption. Detect PII columns: email, ssn, phone, name, dob, "
            "credit_card. Apply masking BEFORE any write, join, or logging action. "
            "GDPR requires right-to-erasure: use hash + salt stored in AWS Secrets Manager."
        ),
        "tags": ["pii", "gdpr", "hipaa", "masking", "compliance", "security"],
    },
    {
        "id": "cost_optimisation",
        "text": (
            "Top 5 ETL cost optimisations by ROI: (1) Enable Glue Flex = 34% off, (2) Remove "
            "Python UDFs = 20–50% compute reduction, (3) Right-size workers = 20–40% cost cut, "
            "EMR Spot = 70% discount for non-SLA jobs. "
            "(4) Predicate pushdown — filter before join, not after. (5) Broadcast small tables "
            "< 100 MB to eliminate shuffle. Combined effect: 50–70% cost reduction achievable."
        ),
        "tags": ["cost", "optimization", "glue", "emr", "spot", "flex"],
    },
    {
        "id": "delta_vs_iceberg",
        "text": (
            "Delta Lake vs Apache Iceberg comparison for AWS: Iceberg is AWS-native (Athena, Glue, "
            "EMR all support it), better for multi-engine access, has $files/$manifests/$snapshots "
            "system tables for observability. Delta is better for Databricks-first orgs, has "
            "DeltaLog JSON for audit. Both support ACID, time travel, and schema evolution. "
            "Choose Iceberg for AWS-native pipelines; Delta for Databricks + Spark Structured Streaming."
        ),
        "tags": ["iceberg", "delta", "format", "acid", "time_travel"],
    },
    {
        "id": "partition_strategy",
        "text": (
            "Partition strategy for ETL tables: partition by event_date (not timestamp — too high cardinality). "
            "For 1B+ row tables, consider Z-order (Delta) or liquid clustering (Iceberg v2). "
            "Partition pruning works only when filter is on partition column. Never partition on "
            "boolean/status columns — cardinality too low creates huge partitions. "
            "Target 128 MB–1 GB per partition file for optimal scan performance."
        ),
        "tags": ["partitioning", "z-order", "performance", "iceberg", "delta", "s3"],
    },
]

_KNOWLEDGE_SEEDED = False


def _seed_knowledge_base() -> None:
    """Index curated knowledge into ChromaDB on first call."""
    global _KNOWLEDGE_SEEDED
    if _KNOWLEDGE_SEEDED or not _CHROMA_AVAILABLE:
        return
    try:
        existing_ids = set(_chroma_col.get()["ids"])
        to_add = [k for k in _SEED_KNOWLEDGE if k["id"] not in existing_ids]
        if to_add:
            _chroma_col.add(
                ids=[k["id"] for k in to_add],
                documents=[k["text"] for k in to_add],
                metadatas=[{"tags": ",".join(k["tags"])} for k in to_add],
            )
            logger.info("Seeded %d knowledge documents into ChromaDB", len(to_add))
        _KNOWLEDGE_SEEDED = True
    except Exception as exc:
        logger.warning("Knowledge seeding failed: %s", exc)


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@tool
def ask_etl_knowledge_base(question: str, top_k: int = 4) -> str:
    """
    Answer an ETL question using the local knowledge base (RAG).

    Retrieves the most relevant expert knowledge documents and surfaces them
    as context. The orchestrating agent then synthesises the final answer.

    Args:
        question: Natural-language ETL question.
        top_k:    Number of knowledge documents to retrieve (default 4).

    Returns:
        JSON with question, retrieved_context list, and retrieval_source.
    """
    _seed_knowledge_base()
    try:
        retrieved: List[Dict] = []

        # ── ChromaDB local retrieval ─────────────────────────────────────────
        if _CHROMA_AVAILABLE and _chroma_col:
            results = _chroma_col.query(
                query_texts=[question],
                n_results=min(top_k, _chroma_col.count()),
                include=["documents", "metadatas", "distances"],
            )
            for doc, meta, dist in zip(
                results["documents"][0],
                results["metadatas"][0],
                results["distances"][0],
            ):
                retrieved.append({
                    "content":   doc,
                    "relevance": round(1 - dist, 4),
                    "tags":      meta.get("tags", "").split(","),
                    "source":    "local_kb",
                })

        # ── Bedrock Knowledge Base (cloud) ───────────────────────────────────
        elif _BEDROCK_KB_ID:
            try:
                br_agent = boto3.client("bedrock-agent-runtime")
                resp = br_agent.retrieve(
                    knowledgeBaseId=_BEDROCK_KB_ID,
                    retrievalQuery={"text": question},
                    retrievalConfiguration={"vectorSearchConfiguration": {"numberOfResults": top_k}},
                )
                for r in resp.get("retrievalResults", []):
                    retrieved.append({
                        "content":   r["content"]["text"],
                        "relevance": round(r.get("score", 0.0), 4),
                        "source":    "bedrock_kb",
                    })
            except Exception as exc:
                logger.warning("Bedrock KB retrieval failed: %s", exc)

        # ── Keyword fallback ─────────────────────────────────────────────────
        else:
            kw = question.lower().split()
            for k in _SEED_KNOWLEDGE:
                score = sum(1 for w in kw if w in k["text"].lower()) / max(len(kw), 1)
                if score > 0.1:
                    retrieved.append({
                        "content":   k["text"],
                        "relevance": round(score, 2),
                        "source":    "keyword_fallback",
                    })
            retrieved.sort(key=lambda x: -x["relevance"])
            retrieved = retrieved[:top_k]

        return json.dumps({
            "question":          question,
            "context_count":     len(retrieved),
            "retrieved_context": retrieved,
            "retrieval_source":  retrieved[0]["source"] if retrieved else "none",
            "instruction":       "Use the retrieved_context to answer the question accurately.",
        })
    except Exception as exc:
        return json.dumps({"error": str(exc), "retrieved_context": []})


@tool
def index_pipeline_run_to_kb(pipeline_results_json: str, job_name: str = "") -> str:
    """
    Summarise and index a completed pipeline run into the local knowledge base.
    This trains the chatbot on each job's actual outcomes, enabling future RAG.

    Args:
        pipeline_results_json: Full pipeline results JSON (all agent outputs).
        job_name:              Optional job identifier.

    Returns:
        JSON with indexed_doc_id, summary_preview, and indexed_to.
    """
    _seed_knowledge_base()
    try:
        ctx  = json.loads(pipeline_results_json)
        size = ctx.get("sizing", {})
        code = ctx.get("code_analysis", {})
        res  = ctx.get("resource_allocator", {})
        recs = ctx.get("recommendations", {})

        # Compose a human-readable narrative
        anti_count  = int(code.get("anti_pattern_count", 0))
        savings_pct = float((res.get("savings") or {}).get("percent", 0))
        score       = float(code.get("optimization_score", 0))
        gb          = float(size.get("effective_size_gb", 0))

        summary = (
            f"Pipeline '{job_name}' processed {gb:.1f} GB. "
            f"Code optimization score: {score:.0f}/100 with {anti_count} anti-patterns. "
            f"Resource right-sizing saves {savings_pct:.0f}%. "
            f"Top recommendations: "
            + "; ".join(
                r.get("title", "") for r in
                (recs.get("recommendations") or [])[:3]
            )
        )

        doc_id = _doc_id(summary)
        indexed_to = []

        if _CHROMA_AVAILABLE and _chroma_col:
            _chroma_col.upsert(
                ids=[doc_id],
                documents=[summary],
                metadatas=[{"job_name": job_name, "tags": "pipeline_run,outcome"}],
            )
            indexed_to.append("chromadb")

        return json.dumps({
            "indexed_doc_id": doc_id,
            "summary_preview": summary[:200],
            "indexed_to":      indexed_to or ["not_indexed_no_backend"],
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def recommend_from_knowledge(problem_description: str, top_k: int = 3) -> str:
    """
    Given a problem description, retrieve the most relevant expert recommendations
    from the knowledge base and rank them by applicability.

    Args:
        problem_description: Description of the ETL problem or symptom.
        top_k:               Number of recommendations to return (default 3).

    Returns:
        JSON with ranked_recommendations, each with content, confidence, and action_steps.
    """
    _seed_knowledge_base()
    try:
        kb_result  = json.loads(ask_etl_knowledge_base.__wrapped__(problem_description, top_k * 2))
        contexts   = kb_result.get("retrieved_context", [])

        recommendations = []
        for ctx in contexts[:top_k]:
            text = ctx["content"]
            # Extract action steps (sentences starting with imperatives)
            import re
            actions = [s.strip() for s in re.split(r"[.;]", text)
                       if any(s.strip().startswith(kw) for kw in
                              ("Enable", "Disable", "Replace", "Add", "Run", "Use", "Set",
                               "Apply", "Configure", "Schedule", "Upgrade", "Reduce", "Fix"))]
            recommendations.append({
                "content":    text[:300],
                "confidence": ctx["relevance"],
                "action_steps": actions[:4],
                "tags":       ctx.get("tags", []),
            })

        return json.dumps({
            "problem":               problem_description[:200],
            "ranked_recommendations": recommendations,
            "total_found":           len(contexts),
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


@tool
def train_chatbot_on_history(learning_vectors_json: str) -> str:
    """
    Bulk-index historical learning vectors into the chatbot knowledge base.
    Call this periodically to keep the chatbot current with pipeline outcomes.

    Args:
        learning_vectors_json: JSON list of learning vectors from retrieve_learning_vectors.

    Returns:
        JSON with indexed_count, skipped_count.
    """
    _seed_knowledge_base()
    try:
        vectors  = json.loads(learning_vectors_json)
        indexed  = 0
        skipped  = 0

        for vec in vectors:
            try:
                wl   = vec.get("workload_profile", {})
                qm   = vec.get("quality_metrics", {})
                cm   = vec.get("cost_metrics", {})
                eo   = vec.get("execution_outcome", {})

                text = (
                    f"Pipeline run {vec.get('pipeline_id', '')} processed "
                    f"{wl.get('effective_size_gb', 0):.1f} GB with "
                    f"{wl.get('join_count', 0)} joins. "
                    f"DQ score: {qm.get('dq_score', 0):.0f}, "
                    f"code quality: {qm.get('code_quality_score', 0):.0f}, "
                    f"anti-patterns: {qm.get('anti_pattern_count', 0)}. "
                    f"Savings potential: {cm.get('savings_pct', 0):.0f}%. "
                    f"Used {cm.get('workers_used', '?')} × {cm.get('worker_type', '?')} workers. "
                    f"Execution: {eo.get('status', 'unknown')}."
                )

                doc_id = _doc_id(text)
                if _CHROMA_AVAILABLE and _chroma_col:
                    _chroma_col.upsert(
                        ids=[doc_id],
                        documents=[text],
                        metadatas=[{"tags": "historical,learning_vector"}],
                    )
                indexed += 1
            except Exception:
                skipped += 1

        return json.dumps({
            "indexed_count": indexed,
            "skipped_count": skipped,
            "backend":       "chromadb" if _CHROMA_AVAILABLE else "none",
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


SYSTEM_PROMPT = """
You are **ETL Sage**, an expert ETL knowledge assistant powered by a curated
knowledge base of Spark, Glue, Iceberg, and cost-optimisation patterns.

When answering questions:
1. Use ask_etl_knowledge_base to retrieve relevant context FIRST
2. Synthesise retrieved context with your reasoning
3. Always provide concrete, actionable recommendations
4. Quote specific Spark configs, SQL, or CLI commands where applicable
5. Rate your confidence: HIGH / MEDIUM / LOW based on retrieval relevance

You have deep expertise in: PySpark anti-patterns, AWS Glue sizing, Iceberg/Delta
table maintenance, PII compliance, data quality frameworks, and cost optimisation.

Be concise but specific. Prefer bullet points over paragraphs for action steps.
"""


def create_chatbot_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                          region: str = "us-west-2") -> Agent:
    """Return an ETL Knowledge Chatbot Strands Agent with local RAG."""
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[
            ask_etl_knowledge_base,
            index_pipeline_run_to_kb,
            recommend_from_knowledge,
            train_chatbot_on_history,
        ],
    )
