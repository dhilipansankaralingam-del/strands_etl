# Architecture & Security Documentation

**For: Technical Architect + Security Team**
**System: Strands ETL Multi-Agent Framework**
**Version: 2.1.0**

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture Diagram - run_strands_etl.py (Rule-Based)](#2-architecture-diagram---run_strands_etlpy-rule-based)
3. [Architecture Diagram - run_strands_swarm.py (LLM-Powered)](#3-architecture-diagram---run_strands_swarmpy-llm-powered)
4. [Agent Interaction & Communication Model](#4-agent-interaction--communication-model)
5. [Data Flow & Storage Architecture](#5-data-flow--storage-architecture)
6. [Historical Pattern Analysis (ML Learning)](#6-historical-pattern-analysis-ml-learning)
7. [Network & VPC Boundaries](#7-network--vpc-boundaries)
8. [Security Considerations](#8-security-considerations)
9. [Data Classification & Sensitivity](#9-data-classification--sensitivity)
10. [Threat Model](#10-threat-model)

---

## 1. System Overview

The framework provides two execution modes for the same multi-agent ETL pipeline:

| Aspect | `run_strands_etl.py` | `run_strands_swarm.py` |
|--------|----------------------|------------------------|
| **Execution Type** | Rule-based (deterministic) | LLM-powered (AI-driven) |
| **External API Calls** | None (purely local) | Amazon Bedrock (Claude) |
| **Cost per Run** | $0 (compute only) | $0.01-$0.10 (Bedrock tokens) |
| **Agent Logic** | Hardcoded Python rules | LLM inference + tools |
| **Runs on** | EC2 instance | EC2 instance |
| **AWS Services Used** | Glue, S3, DynamoDB, CloudWatch, EMR, EKS | Same + Amazon Bedrock |

---

## 2. Architecture Diagram - run_strands_etl.py (Rule-Based)

```
                         EC2 INSTANCE (Private Subnet)
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                                                                             │
 │  ┌──────────────────────┐                                                   │
 │  │  run_strands_etl.py  │                                                   │
 │  │  (Entry Point)       │                                                   │
 │  └──────────┬───────────┘                                                   │
 │             │                                                               │
 │             │ Loads JSON config                                             │
 │             ▼                                                               │
 │  ┌──────────────────────────────────────────────────────────────┐           │
 │  │              StrandsOrchestrator                             │           │
 │  │  ┌──────────────────────────────────────────────────────┐   │           │
 │  │  │  Kahn's Algorithm (Topological Sort)                 │   │           │
 │  │  │  Resolves agent dependencies → builds phases         │   │           │
 │  │  └──────────────────────────────────────────────────────┘   │           │
 │  │                                                              │           │
 │  │  ┌──────────────────────────────────────────────────────┐   │           │
 │  │  │  ThreadPoolExecutor (max_workers=10)                 │   │           │
 │  │  │  Executes agents in parallel within each phase       │   │           │
 │  │  └──────────────────────────────────────────────────────┘   │           │
 │  │                                                              │           │
 │  │  ┌──────────────────────────────────────────────────────┐   │           │
 │  │  │  AgentContext (Thread-Safe Shared State)              │   │           │
 │  │  │  • shared_state: Dict protected by threading.Lock    │   │           │
 │  │  │  • agent_results: Dict of AgentResult per agent      │   │           │
 │  │  │  • config, job_name, execution_id, run_date          │   │           │
 │  │  └──────────────────────────────────────────────────────┘   │           │
 │  └──────────────────────────────────────────────────────────────┘           │
 │             │                                                               │
 │             │ Executes 5 phases sequentially                                │
 │             ▼                                                               │
 │  ┌─────────────────────────────────────────────────────────────┐            │
 │  │ PHASE 1 (Parallel)           PHASE 2 (Parallel)            │            │
 │  │ ┌────────────────┐           ┌─────────────────────┐       │            │
 │  │ │ SizingAgent    │           │ ResourceAllocator   │       │            │
 │  │ │ (Glue/S3 APIs) │           │ (size→workers calc) │       │            │
 │  │ └────────────────┘           └─────────────────────┘       │            │
 │  │ ┌────────────────┐           ┌─────────────────────┐       │            │
 │  │ │ ComplianceAgent│           │ DataQualityAgent    │       │            │
 │  │ │ (rule checks)  │           │ (DQ rule checks)    │       │            │
 │  │ └────────────────┘           └─────────────────────┘       │            │
 │  │ ┌────────────────┐           ┌─────────────────────┐       │            │
 │  │ │ CodeAnalysis   │           │ HealingAgent        │       │            │
 │  │ │ (regex/AST)    │           │ (retry strategies)  │       │            │
 │  │ └────────────────┘           └─────────────────────┘       │            │
 │  │                                                             │            │
 │  │ PHASE 3 (Parallel)           PHASE 4                       │            │
 │  │ ┌─────────────────────┐      ┌─────────────────────┐      │            │
 │  │ │ PlatformConversion  │      │ ExecutionAgent      │      │            │
 │  │ │ (size→platform)     │      │ (Glue/EMR/EKS API)  │      │            │
 │  │ └─────────────────────┘      └─────────────────────┘      │            │
 │  │ ┌─────────────────────┐                                    │            │
 │  │ │ CodeConversion      │      PHASE 5 (Parallel)           │            │
 │  │ │ (GlueCtx→Spark)    │      ┌─────────────────────┐      │            │
 │  │ └─────────────────────┘      │ LearningAgent       │      │            │
 │  │                               │ (ML model training) │      │            │
 │  │                               └─────────────────────┘      │            │
 │  │                               ┌─────────────────────┐      │            │
 │  │                               │ RecommendationAgent │      │            │
 │  │                               │ (aggregates results)│      │            │
 │  │                               └─────────────────────┘      │            │
 │  └─────────────────────────────────────────────────────────────┘            │
 │             │                                                               │
 │             │ Writes results                                                │
 │             ▼                                                               │
 │  ┌──────────────────────────────────────────────────────────────┐           │
 │  │              StrandsStorage (Fallback Chain)                 │           │
 │  │                                                              │           │
 │  │  Primary:  DynamoDB ──┐                                      │           │
 │  │                       │ on failure                           │           │
 │  │  Secondary: S3  ──────┤                                      │           │
 │  │                       │ on failure                           │           │
 │  │  Fallback:  Local ────┘                                      │           │
 │  │             (data/agent_store/*.psv, *.json)                 │           │
 │  └──────────────────────────────────────────────────────────────┘           │
 │                                                                             │
 └─────────────────────────────────────────────────────────────────────────────┘
                    │                              │
                    │ VPC Endpoint                  │ VPC Endpoint
                    ▼                              ▼
           ┌──────────────┐              ┌──────────────────┐
           │  AWS Glue    │              │  Amazon S3       │
           │  (start job) │              │  (data storage)  │
           └──────────────┘              └──────────────────┘
                    │                              │
                    ▼                              ▼
           ┌──────────────┐              ┌──────────────────┐
           │  DynamoDB    │              │  CloudWatch      │
           │  (state)     │              │  (metrics)       │
           └──────────────┘              └──────────────────┘
```

### Key Points (Rule-Based):
- **NO external internet calls** - All communication via VPC endpoints
- All agent logic executes **locally on EC2** as Python functions
- Agents are Python classes with deterministic rules (regex, math, thresholds)
- ThreadPoolExecutor manages concurrency (up to 10 parallel threads)
- Thread safety via `threading.Lock` on shared `AgentContext`

---

## 3. Architecture Diagram - run_strands_swarm.py (LLM-Powered)

```
                         EC2 INSTANCE (Private Subnet)
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                                                                             │
 │  ┌───────────────────────┐                                                  │
 │  │  run_strands_swarm.py │                                                  │
 │  │  (Entry Point)        │                                                  │
 │  └──────────┬────────────┘                                                  │
 │             │                                                               │
 │             │ Loads JSON config → ETLConfig                                 │
 │             ▼                                                               │
 │  ┌──────────────────────────────────────────────────────────────┐           │
 │  │              ETLSwarm (strands_sdk)                          │           │
 │  │                                                              │           │
 │  │  Uses strands.Agent (Strands SDK) for each agent:            │           │
 │  │  ┌──────────────────────────────────────────────────────┐   │           │
 │  │  │  strands.Agent(                                      │   │           │
 │  │  │    system_prompt = AGENT_PROMPTS[agent_name],        │   │           │
 │  │  │    model = BedrockModel(model_id="claude-sonnet"),   │   │           │
 │  │  │    tools = [aws_tool_1, aws_tool_2, ...]             │   │           │
 │  │  │  )                                                   │   │           │
 │  │  └──────────────────────────────────────────────────────┘   │           │
 │  │                                                              │           │
 │  │  Each agent call:                                            │           │
 │  │  1. Sends system_prompt + task → Bedrock (Claude)            │           │
 │  │  2. Claude reasons about the task                            │           │
 │  │  3. Claude invokes tools (AWS APIs) as needed                │           │
 │  │  4. Results returned to orchestrator                         │           │
 │  └──────────────────────────────────────────────────────────────┘           │
 │             │                                                               │
 │             │ Each agent → Bedrock API call                                 │
 │             ▼                                                               │
 │  ┌──────────────────────────────────────────────────────────────┐           │
 │  │ AGENT EXECUTION (Sequential via Swarm)                       │           │
 │  │                                                              │           │
 │  │  ┌────────────────────┐    ┌──────────────────────┐         │           │
 │  │  │ Sizing Agent       │───▶│ Resource Allocator   │         │           │
 │  │  │ prompt + config    │    │ prompt + sizing data  │         │           │
 │  │  │    ↕ Bedrock API   │    │    ↕ Bedrock API      │         │           │
 │  │  └────────────────────┘    └──────────────────────┘         │           │
 │  │            │                         │                       │           │
 │  │            ▼                         ▼                       │           │
 │  │  ┌────────────────────┐    ┌──────────────────────┐         │           │
 │  │  │ Code Analyzer      │───▶│ Platform Converter   │         │           │
 │  │  │ prompt + script    │    │ prompt + analysis     │         │           │
 │  │  │    ↕ Bedrock API   │    │    ↕ Bedrock API      │         │           │
 │  │  └────────────────────┘    └──────────────────────┘         │           │
 │  │            │                         │                       │           │
 │  │            ▼                         ▼                       │           │
 │  │  ┌──────────────────────────────────────────────┐           │           │
 │  │  │ Execution Agent  → runs actual Glue/EMR job  │           │           │
 │  │  │    ↕ Bedrock API + AWS Tool Calls             │           │           │
 │  │  └──────────────────────────────────────────────┘           │           │
 │  │            │                                                 │           │
 │  │            ▼                                                 │           │
 │  │  ┌────────────────────┐    ┌──────────────────────┐         │           │
 │  │  │ Learning Agent     │    │ Recommendation Agent │         │           │
 │  │  │    ↕ Bedrock API   │    │    ↕ Bedrock API      │         │           │
 │  │  └────────────────────┘    └──────────────────────┘         │           │
 │  └──────────────────────────────────────────────────────────────┘           │
 │                                                                             │
 └─────────────────────────────────────────────────────────────────────────────┘
          │              │                │               │
          │ HTTPS/TLS    │ VPC Endpoint   │ VPC Endpoint  │ VPC Endpoint
          ▼              ▼                ▼               ▼
   ┌─────────────┐ ┌──────────┐  ┌──────────────┐ ┌──────────────┐
   │  Amazon     │ │ AWS Glue │  │  Amazon S3   │ │  CloudWatch  │
   │  Bedrock    │ │          │  │              │ │              │
   │  (Claude)   │ │          │  │              │ │              │
   └─────────────┘ └──────────┘  └──────────────┘ └──────────────┘
```

### Key Points (LLM-Powered):
- **Calls Amazon Bedrock** - requires VPC endpoint or NAT Gateway for Bedrock API
- Claude (LLM) **reasons about** each agent's task and decides which tools to invoke
- Agent prompts contain specialized knowledge (PySpark architect, data engineer roles)
- Tool calls are AWS SDK operations invoked by Claude through the Strands SDK tool framework
- More flexible (handles novel scenarios) but costs $0.01-$0.10 per run in token usage

---

## 4. Agent Interaction & Communication Model

### 4.1 Agent Communication Flow (Both Modes)

```
                    ┌───────────────────────────────────────┐
                    │         AgentContext (Shared State)    │
                    │                                       │
                    │  shared_state: {                      │
                    │    "total_size_gb": 45.2,             │
                    │    "recommended_workers": 15,         │
                    │    "recommended_worker_type": "G.2X", │
                    │    "target_platform": "glue",         │
                    │    "code_issues": [...],              │
                    │    "compliance_findings": [...],      │
                    │    "dq_summary": {...},               │
                    │    "learned_patterns": [...],         │
                    │    "trained_models": [...],           │
                    │    "job_metrics": {...},              │
                    │    "job_execution": {...}             │
                    │  }                                    │
                    │                                       │
                    │  agent_results: {                     │
                    │    "sizing_agent": AgentResult,       │
                    │    "compliance_agent": AgentResult,   │
                    │    ...                                │
                    │  }                                    │
                    │                                       │
                    │  Protected by: threading.Lock         │
                    └───────────┬───────────────────────────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                  │
              ▼                 ▼                  ▼
        context.set_shared()  context.get_shared()  context.get_agent_result()
              │                 │                  │
    ┌─────────┤      ┌──────────┤       ┌──────────┤
    │         │      │          │       │          │
    ▼         ▼      ▼          ▼       ▼          ▼
  Agent A   Agent B  Agent C  Agent D  Agent E   Agent F
```

### 4.2 Inter-Agent Data Flow

Agents do **NOT** communicate directly. All data passes through `AgentContext`:

```
SizingAgent ──set_shared("total_size_gb", 45.2)──▶ AgentContext
                                                         │
ResourceAllocator ◀──get_shared("total_size_gb")─────────┘
         │
         └──set_shared("recommended_workers", 15)──▶ AgentContext
                                                         │
PlatformConversion ◀──get_shared("recommended_workers")──┘
         │
         └──set_shared("target_platform", "eks")──▶ AgentContext
                                                         │
CodeConversion ◀──get_shared("target_platform")──────────┘
         │
         └──set_shared("converted_script_path", "...")──▶ AgentContext
                                                              │
ExecutionAgent ◀──get_shared("converted_script_path")─────────┘
         │
         └──set_shared("job_metrics", {...})──▶ AgentContext
                                                    │
LearningAgent ◀──get_shared("job_metrics")──────────┘
```

### 4.3 Phase Execution Order

```
Time ──────────────────────────────────────────────────────────────▶

Phase 1 (Parallel)          Phase 2 (Parallel)        Phase 3          Phase 4         Phase 5
┌──────────────────┐   ┌──────────────────────┐  ┌──────────────┐  ┌───────────┐  ┌───────────────┐
│ SizingAgent      │──▶│ ResourceAllocator    │─▶│ Platform     │─▶│ Execution │─▶│ Learning      │
│ ComplianceAgent  │──▶│ DataQualityAgent     │  │ Conversion   │  │ Agent     │  │ Agent         │
│ CodeAnalysis     │──▶│ HealingAgent         │  │ Code         │  │           │  │ Recommendation│
│                  │   │                      │  │ Conversion   │  │           │  │ Agent         │
└──────────────────┘   └──────────────────────┘  └──────────────┘  └───────────┘  └───────────────┘
   │ ThreadPool            │ ThreadPool              │ ThreadPool      │ Sequential    │ ThreadPool
   │ max_workers=10        │ max_workers=10          │ max_workers=10  │               │ max_workers=10
```

---

## 5. Data Flow & Storage Architecture

### 5.1 Storage Fallback Chain

```
┌───────────────┐     ┌────────────────┐     ┌────────────────┐
│   DynamoDB    │────▶│    Amazon S3   │────▶│  Local Disk    │
│  (Primary)    │fail │  (Secondary)   │fail │  (Fallback)    │
│               │     │                │     │                │
│ Table prefix: │     │ Prefix:        │     │ Path:          │
│ etl_*         │     │ etl-data/      │     │ data/          │
│               │     │                │     │ agent_store/   │
│ Items: JSON   │     │ Objects: JSON  │     │                │
│ records       │     │ + .psv.gz      │     │ Files:         │
│               │     │                │     │ *.json (records)│
│               │     │                │     │ *.psv (pipe-   │
│               │     │                │     │  delimited)    │
└───────────────┘     └────────────────┘     └────────────────┘
```

### 5.2 What Gets Stored & Where

```
data/
├── agent_store/                          # StrandsStorage local fallback
│   ├── learning_agent_execution_history/ # Per-agent data tables
│   │   └── *.psv                         # Pipe-delimited execution records
│   ├── learning_agent_training_runs/
│   │   └── *.psv                         # Training run summaries
│   ├── learning_agent_patterns/
│   │   └── *.psv                         # Discovered data patterns
│   ├── audit_logs/
│   │   └── audit_2026_02_16.psv          # Daily audit logs (pipe-delimited)
│   └── {agent_name}_{data_type}/
│       └── *.json                        # Individual agent records
│
├── models/                               # LearningAgent ML models
│   ├── model_registry.json               # Model metadata registry
│   ├── execution_history.json            # Training data (last 1000 records)
│   ├── RES-{hash}.pkl                    # Resource predictor model (pickle)
│   ├── RUN-{hash}.pkl                    # Runtime estimator model (pickle)
│   └── COS-{hash}.pkl                    # Cost predictor model (pickle)
│
└── converted/                            # Code conversion output
    ├── {job_name}_emr.py                 # EMR-converted scripts
    └── {job_name}_eks.py                 # EKS-converted scripts
```

### 5.3 Data Write Operations by Agent

| Agent | Data Written | Storage Target | Format |
|-------|-------------|----------------|--------|
| **SizingAgent** | Table sizes, S3 stats | AgentContext only (in-memory) | - |
| **ComplianceAgent** | Compliance findings | AgentContext only (in-memory) | - |
| **CodeAnalysisAgent** | Code issues, patterns | AgentContext only (in-memory) | - |
| **ResourceAllocator** | Worker/type recommendations | AgentContext only (in-memory) | - |
| **DataQualityAgent** | DQ results | AgentContext only (in-memory) | - |
| **HealingAgent** | Retry strategies | AgentContext only (in-memory) | - |
| **PlatformConversion** | Platform decision | AgentContext only (in-memory) | - |
| **CodeConversion** | Converted script | Local disk (`converted/`) | `.py` |
| **ExecutionAgent** | Execution metrics, job state | StrandsStorage + AgentContext | JSON/PSV |
| **LearningAgent** | Execution history, trained models, patterns, training runs | StrandsStorage + local `data/models/` | JSON/PSV/PKL |
| **RecommendationAgent** | Aggregated recommendations | AgentContext + audit log | JSON/PSV |
| **Audit (Orchestrator)** | Orchestration complete event | StrandsStorage → audit_logs/ | PSV |

### 5.4 Pipe-Delimited File Format (PSV)

```
timestamp|job_name|event_type|event_data
2026-02-16T10:30:00|sales_etl|orchestration_complete|{"execution_id":"abc123",...}
2026-02-16T10:35:00|sales_etl|learning_training|{"models_trained":3,...}
```

- Delimiter: `|` (pipe)
- Escaping: `|` → `\|`, newlines → `\n`
- Daily rotation for audit logs
- Thread-safe writes via per-table `threading.Lock`

---

## 6. Historical Pattern Analysis (ML Learning)

### 6.1 Learning Data Flow

```
                    ┌─────────────────────────────────────────────┐
                    │              LearningAgent                   │
                    │                                              │
 ┌──────────┐      │  1. COLLECT                                  │
 │ Agent    │      │  ┌────────────────────────────────────┐     │
 │ Context  │─────▶│  │ _collect_execution_data(context)   │     │
 │ (shared  │      │  │ Extracts:                          │     │
 │  state)  │      │  │  • total_size_gb                   │     │
 └──────────┘      │  │  • recommended_workers             │     │
                    │  │  • duration_seconds                │     │
                    │  │  • cost_usd                        │     │
                    │  │  • platform, day_of_week           │     │
                    │  │  • records_read/written            │     │
                    │  └────────────┬───────────────────────┘     │
                    │               │                              │
                    │  2. STORE     ▼                              │
                    │  ┌────────────────────────────────────┐     │
                    │  │ execution_history.json              │     │
                    │  │ (last 1000 records)                 │     │
                    │  │ + learning_agent_execution_history/ │     │
                    │  │   (pipe-delimited archives)         │     │
                    │  └────────────┬───────────────────────┘     │
                    │               │                              │
                    │  3. TRAIN     ▼  (if >= 5 records)          │
                    │  ┌────────────────────────────────────┐     │
                    │  │ Three gradient descent models:      │     │
                    │  │                                     │     │
                    │  │ a) Resource Predictor               │     │
                    │  │    Features: size_gb, is_weekend,   │     │
                    │  │             day_of_week             │     │
                    │  │    Target:   recommended_workers    │     │
                    │  │    Output:   RES-{hash}.pkl         │     │
                    │  │                                     │     │
                    │  │ b) Runtime Estimator                │     │
                    │  │    Features: size_gb, workers,      │     │
                    │  │             size_per_worker         │     │
                    │  │    Target:   duration_seconds       │     │
                    │  │    Output:   RUN-{hash}.pkl         │     │
                    │  │                                     │     │
                    │  │ c) Cost Predictor                   │     │
                    │  │    Features: workers, duration_hrs, │     │
                    │  │             worker_hours            │     │
                    │  │    Target:   cost_usd               │     │
                    │  │    Output:   COS-{hash}.pkl         │     │
                    │  │                                     │     │
                    │  │ Algorithm: Linear regression via    │     │
                    │  │ gradient descent (100 iterations,   │     │
                    │  │ lr=0.01, feature normalization)     │     │
                    │  └────────────┬───────────────────────┘     │
                    │               │                              │
                    │  4. DETECT    ▼                              │
                    │  ┌────────────────────────────────────┐     │
                    │  │ Anomaly Detection:                  │     │
                    │  │ • Size > 1000 GB → warning          │     │
                    │  │ • |current - mean| > 3σ → alert     │     │
                    │  │                                     │     │
                    │  │ Pattern Analysis:                   │     │
                    │  │ • Size trends (growth detection)    │     │
                    │  │ • Worker-to-size ratios             │     │
                    │  │ • Weekend vs weekday patterns       │     │
                    │  └────────────┬───────────────────────┘     │
                    │               │                              │
                    │  5. PREDICT   ▼                              │
                    │  ┌────────────────────────────────────┐     │
                    │  │ Using trained models, predicts:     │     │
                    │  │ • predicted_workers (for this run)  │     │
                    │  │ • predicted_runtime_seconds         │     │
                    │  │ • predicted_cost_usd                │     │
                    │  │                                     │     │
                    │  │ Shared to context for other agents  │     │
                    │  └────────────────────────────────────┘     │
                    └─────────────────────────────────────────────┘
```

### 6.2 Model Versioning

```
model_registry.json
{
  "RES-a1b2c3d4e5f6": {
    "model_type": "resource_predictor",
    "version": "1.0.0",
    "created_at": "2026-02-16T10:30:00",
    "training_records": 25,
    "training_cost_usd": 0.0003,
    "metrics": { "mse": 2.1, "r2": 0.85 },
    "features": ["size_gb", "is_weekend", "day_of_week"]
  },
  "RUN-f6e5d4c3b2a1": { ... },
  "COS-1a2b3c4d5e6f": { ... }
}
```

- Models are versioned with unique SHA-256 hash IDs
- Each training run records cost ($0.00001/record + $0.0005/second compute)
- Models stored as Python pickle files (`.pkl`) on local disk
- Model registry tracks all versions, metrics, and costs
- All training happens **locally** on EC2 - no external ML services used

---

## 7. Network & VPC Boundaries

### 7.1 Network Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              AWS VPC                                             │
│                                                                                 │
│  ┌──────────────────────────────────────────────────────────────────┐           │
│  │                     PRIVATE SUBNET                               │           │
│  │                                                                  │           │
│  │  ┌──────────────────────────────────────────────────────────┐   │           │
│  │  │                    EC2 INSTANCE                           │   │           │
│  │  │                                                          │   │           │
│  │  │  ┌──────────────────┐     ┌────────────────────────┐    │   │           │
│  │  │  │ run_strands_     │     │ run_strands_           │    │   │           │
│  │  │  │ etl.py           │     │ swarm.py               │    │   │           │
│  │  │  │                  │     │                        │    │   │           │
│  │  │  │ NO external      │     │ Calls Bedrock API      │    │   │           │
│  │  │  │ API calls        │     │ (via VPC endpoint      │    │   │           │
│  │  │  │ (except AWS      │     │  or NAT Gateway)       │    │   │           │
│  │  │  │  service APIs)   │     │                        │    │   │           │
│  │  │  └─────────┬────────┘     └───────────┬────────────┘    │   │           │
│  │  │            │                           │                 │   │           │
│  │  │            │  ┌────────────────────────┘                 │   │           │
│  │  │            │  │                                          │   │           │
│  │  │  ┌─────────▼──▼────────────────────────────────────┐    │   │           │
│  │  │  │            Local Storage                         │    │   │           │
│  │  │  │  data/agent_store/ (PSV + JSON files)            │    │   │           │
│  │  │  │  data/models/      (ML models + history)         │    │   │           │
│  │  │  │  converted/        (converted PySpark scripts)   │    │   │           │
│  │  │  └──────────────────────────────────────────────────┘    │   │           │
│  │  └──────────────────────────────────────────────────────────┘   │           │
│  │                  │              │              │                  │           │
│  └──────────────────┼──────────────┼──────────────┼──────────────────┘           │
│                     │              │              │                              │
│  ┌──────────────────▼──┐ ┌────────▼────────┐ ┌──▼──────────────────────┐       │
│  │ VPC Endpoint:       │ │ VPC Endpoint:   │ │ VPC Endpoint:           │       │
│  │ com.amazonaws.      │ │ com.amazonaws.  │ │ com.amazonaws.          │       │
│  │ {region}.glue       │ │ {region}.s3     │ │ {region}.dynamodb       │       │
│  └──────────────────┬──┘ └────────┬────────┘ └──┬──────────────────────┘       │
│                     │             │              │                              │
│  ┌──────────────────▼──────────────▼──────────────▼───────────────────┐         │
│  │          VPC Endpoint: com.amazonaws.{region}.bedrock-runtime      │         │
│  │          (ONLY used by run_strands_swarm.py)                       │         │
│  └───────────────────────────────────────────────────────────────────┘         │
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────┐         │
│  │  OPTIONAL: VPC Endpoints for EMR/EKS (if using those platforms)   │         │
│  │  • com.amazonaws.{region}.elasticmapreduce                        │         │
│  │  • com.amazonaws.{region}.eks                                     │         │
│  │  • com.amazonaws.{region}.monitoring (CloudWatch)                 │         │
│  │  • com.amazonaws.{region}.logs (CloudWatch Logs)                  │         │
│  └────────────────────────────────────────────────────────────────────┘         │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 Network Call Summary

| Operation | `run_strands_etl.py` | `run_strands_swarm.py` | Protocol | Stays in VPC? |
|-----------|---------------------|----------------------|----------|---------------|
| Agent logic execution | Local (Python) | Bedrock API (HTTPS) | - / HTTPS | Yes / Yes (via VPC endpoint) |
| Read Glue Catalog | Glue API | Glue API | HTTPS | Yes (VPC endpoint) |
| Read S3 objects | S3 API | S3 API | HTTPS | Yes (VPC endpoint) |
| Write DynamoDB | DynamoDB API | DynamoDB API | HTTPS | Yes (VPC endpoint) |
| Start Glue Job | Glue API | Glue API | HTTPS | Yes (VPC endpoint) |
| CloudWatch metrics | CloudWatch API | CloudWatch API | HTTPS | Yes (VPC endpoint) |
| Start EMR Step | EMR API | EMR API | HTTPS | Yes (VPC endpoint) |
| Submit EKS Job | EKS/K8s API | EKS/K8s API | HTTPS | Yes (VPC endpoint) |
| Write local files | Local disk | Local disk | - | N/A (local) |
| **Internet access** | **NOT REQUIRED** | **NOT REQUIRED*** | - | - |

> *Assuming VPC endpoint for Bedrock is configured. If no VPC endpoint, NAT Gateway is needed for Bedrock API calls.

### 7.3 Required VPC Endpoints

**Minimum (for `run_strands_etl.py`):**
- `com.amazonaws.{region}.s3` (Gateway)
- `com.amazonaws.{region}.glue` (Interface)
- `com.amazonaws.{region}.dynamodb` (Gateway)
- `com.amazonaws.{region}.monitoring` (Interface)

**Additional (for `run_strands_swarm.py`):**
- `com.amazonaws.{region}.bedrock-runtime` (Interface)

**Optional (for EMR/EKS platforms):**
- `com.amazonaws.{region}.elasticmapreduce` (Interface)
- `com.amazonaws.{region}.eks` (Interface)
- `com.amazonaws.{region}.ecr.api` (Interface)
- `com.amazonaws.{region}.ecr.dkr` (Interface)

---

## 8. Security Considerations

### 8.1 IAM Permissions Required

**EC2 Instance Role - Minimum Permissions:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "GlueAccess",
      "Effect": "Allow",
      "Action": [
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetDatabase",
        "glue:GetPartitions",
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:BatchStopJobRun"
      ],
      "Resource": "arn:aws:glue:{region}:{account}:*"
    },
    {
      "Sid": "S3DataAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:PutObject",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::data-lake-bucket/*",
        "arn:aws:s3:::data-lake-bucket",
        "arn:aws:s3:::etl-storage-bucket/*",
        "arn:aws:s3:::etl-storage-bucket"
      ]
    },
    {
      "Sid": "DynamoDBAccess",
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:Query",
        "dynamodb:BatchWriteItem"
      ],
      "Resource": "arn:aws:dynamodb:{region}:{account}:table/etl_*"
    },
    {
      "Sid": "CloudWatchAccess",
      "Effect": "Allow",
      "Action": [
        "cloudwatch:GetMetricData",
        "cloudwatch:PutMetricData",
        "logs:GetLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
```

**Additional for `run_strands_swarm.py`:**

```json
{
  "Sid": "BedrockAccess",
  "Effect": "Allow",
  "Action": [
    "bedrock:InvokeModel",
    "bedrock:InvokeModelWithResponseStream"
  ],
  "Resource": "arn:aws:bedrock:{region}::foundation-model/anthropic.claude-*"
}
```

### 8.2 Data at Rest

| Data Type | Location | Encryption | Sensitivity |
|-----------|----------|------------|-------------|
| Configuration JSON | EC2 local disk | EBS encryption | Contains table names, S3 paths |
| Agent store (PSV/JSON) | EC2 `data/agent_store/` | EBS encryption | Execution metadata, metrics |
| ML models (.pkl) | EC2 `data/models/` | EBS encryption | Learned coefficients only |
| Execution history | EC2 `data/models/execution_history.json` | EBS encryption | Job metadata, sizes, costs |
| Audit logs (PSV) | EC2 `data/agent_store/audit_logs/` | EBS encryption | Execution events, timestamps |
| DynamoDB records | DynamoDB tables | AWS-managed SSE | Agent state, execution data |
| S3 objects | S3 bucket | SSE-S3 or SSE-KMS | Agent data, pipe-delimited archives |
| Converted scripts | EC2 `converted/` | EBS encryption | PySpark code (derived) |

**Key notes:**
- No customer PII/data flows through the agent framework - only **metadata** (table names, sizes, record counts)
- ML models contain only statistical coefficients - no raw data
- Pickle files (`.pkl`) are only read locally - never deserialized from untrusted sources

### 8.3 Data in Transit

| Connection | Encryption | Authentication |
|------------|------------|----------------|
| EC2 → S3 | HTTPS/TLS 1.2+ | IAM SigV4 |
| EC2 → DynamoDB | HTTPS/TLS 1.2+ | IAM SigV4 |
| EC2 → Glue | HTTPS/TLS 1.2+ | IAM SigV4 |
| EC2 → Bedrock | HTTPS/TLS 1.2+ | IAM SigV4 |
| EC2 → CloudWatch | HTTPS/TLS 1.2+ | IAM SigV4 |
| Inter-agent (in-process) | N/A (memory only) | N/A (same process) |

### 8.4 What Data Goes to Bedrock (LLM-Powered Mode Only)

When using `run_strands_swarm.py`, the following is sent to Amazon Bedrock:

| Data Sent to Bedrock | Contains PII? | Contains Raw Data? |
|----------------------|---------------|-------------------|
| System prompts (agent roles) | No | No |
| Config summary (table names, sizes) | No | No |
| PySpark script content (for analysis) | Possibly | No (code only) |
| Agent results (metadata) | No | No |

**Bedrock data handling:**
- Amazon Bedrock does **NOT** store or log model inputs/outputs
- Data is processed in-region and not used for model training
- All communication is encrypted via TLS 1.2+
- Model invocation logs can be enabled for audit (CloudWatch)

### 8.5 Secrets Management

| Secret | Storage | How Accessed |
|--------|---------|--------------|
| AWS credentials | EC2 Instance Profile (IAM Role) | Automatic via boto3 credential chain |
| Database passwords | Not used (IAM auth only) | N/A |
| API keys | None required | N/A |
| Bedrock access | EC2 Instance Profile (IAM Role) | Automatic via boto3 |

**No hardcoded credentials anywhere in the codebase.** All AWS authentication uses IAM roles via the EC2 instance profile.

---

## 9. Data Classification & Sensitivity

### 9.1 Data Flow Classification

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA CLASSIFICATION                          │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │ LOW SENSITIVITY (Metadata Only)                               │  │
│  │                                                               │  │
│  │  • Table names, database names                                │  │
│  │  • Record counts, data sizes (GB)                             │  │
│  │  • S3 paths (bucket/prefix)                                   │  │
│  │  • Worker counts, execution durations                         │  │
│  │  • Cost calculations                                          │  │
│  │  • ML model coefficients                                      │  │
│  │  • Audit timestamps and event types                           │  │
│  │                                                               │  │
│  │  → Stored locally + DynamoDB + S3                             │  │
│  │  → Sent to Bedrock (swarm mode only)                          │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │ MEDIUM SENSITIVITY (Code)                                     │  │
│  │                                                               │  │
│  │  • PySpark source scripts (may contain business logic)        │  │
│  │  • Converted scripts (EMR/EKS versions)                       │  │
│  │  • Code analysis results (anti-patterns, issues)              │  │
│  │                                                               │  │
│  │  → Stored locally on EC2                                      │  │
│  │  → Sent to Bedrock for analysis (swarm mode only)             │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │ NOT PROCESSED (Actual Data)                                   │  │
│  │                                                               │  │
│  │  • Actual table data/records (NOT read by framework)          │  │
│  │  • Customer PII                                               │  │
│  │  • Transaction records                                        │  │
│  │  • Sensitive business data                                    │  │
│  │                                                               │  │
│  │  → NEVER flows through the agent framework                    │  │
│  │  → Only processed by the actual Glue/EMR/EKS job              │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 10. Threat Model

### 10.1 Attack Surface

| Vector | Risk Level | Mitigation |
|--------|-----------|------------|
| EC2 instance compromise | Medium | Security groups, VPC isolation, SSM for access |
| Pickle deserialization (ML models) | Low | Models only created/loaded locally, never from external sources |
| Config injection | Low | JSON config loaded from local disk, validated at parse time |
| Bedrock prompt injection | Low | System prompts are hardcoded, user input is config-only |
| DynamoDB data tampering | Low | IAM policy restricts to `etl_*` tables, SSE enabled |
| S3 data exfiltration | Low | Bucket policies, VPC endpoint policy, no public access |
| Thread safety race condition | Low | `threading.Lock` on shared context, per-table locks on storage |
| Local file permissions | Low | Standard Linux file permissions, single-user EC2 |

### 10.2 Recommendations for Security Team

1. **VPC Endpoints** - Deploy interface VPC endpoints for all AWS services to eliminate NAT Gateway dependency and ensure all traffic stays within the VPC
2. **EBS Encryption** - Enable EBS encryption with KMS for the EC2 instance (protects `data/` directory at rest)
3. **S3 Bucket Policy** - Restrict the ETL storage bucket to the EC2 instance role only
4. **DynamoDB Encryption** - Use AWS-owned keys or CMK for DynamoDB SSE
5. **Bedrock Model Invocation Logging** - Enable CloudWatch logging for Bedrock invocations for audit trail
6. **Bedrock VPC Endpoint** - Use `com.amazonaws.{region}.bedrock-runtime` VPC endpoint to keep LLM calls within VPC
7. **Instance Profile** - Use a dedicated IAM role with least-privilege permissions (see Section 8.1)
8. **Security Groups** - No inbound ports required (EC2 only makes outbound calls); restrict outbound to VPC endpoints
9. **CloudTrail** - Enable CloudTrail for API-level auditing of all AWS service calls
10. **Log Rotation** - Implement log rotation for `data/agent_store/audit_logs/` PSV files

---

## Appendix: Quick Reference Comparison

```
┌────────────────────────────────────────────────────────────────────────────┐
│                    run_strands_etl.py vs run_strands_swarm.py              │
├────────────────────┬─────────────────────────┬─────────────────────────────┤
│ Aspect             │ run_strands_etl.py      │ run_strands_swarm.py        │
├────────────────────┼─────────────────────────┼─────────────────────────────┤
│ Agent execution    │ Local Python classes     │ Bedrock LLM + tools         │
│ Decision making    │ Hardcoded rules/regex    │ Claude AI reasoning         │
│ External API calls │ AWS services only        │ AWS services + Bedrock      │
│ VPC endpoints req  │ S3, Glue, DynamoDB, CW   │ Same + Bedrock Runtime      │
│ Internet required  │ No                       │ No (with VPC endpoints)     │
│ Data to LLM        │ None                     │ Config + code (metadata)    │
│ Cost per run       │ $0 (EC2 compute)         │ $0.01-$0.10 (Bedrock)       │
│ Parallelism        │ ThreadPoolExecutor(10)   │ Sequential via Swarm        │
│ Thread safety      │ threading.Lock           │ Single-threaded             │
│ ML training        │ Local gradient descent   │ Local gradient descent      │
│ Storage            │ DynamoDB→S3→Local        │ DynamoDB→S3→Local           │
│ Audit trail        │ PSV files + DynamoDB     │ PSV files + DynamoDB        │
│ Secrets            │ IAM Instance Profile     │ IAM Instance Profile        │
│ PII exposure       │ None (metadata only)     │ None (metadata only)        │
└────────────────────┴─────────────────────────┴─────────────────────────────┘
```
