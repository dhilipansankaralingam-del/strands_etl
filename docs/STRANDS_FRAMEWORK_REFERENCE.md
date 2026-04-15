# Strands ETL Orchestration & Agent Framework
### Technical & Executive Reference Document

**Version:** 2.2 — February 2026
**Audience:** VP/Director/Manager (Executive Summary); Senior Data & Platform Engineers (Technical Deep-Dive)

---

## ◆ What to Tell Executives in 60 Seconds

> "We built an intelligent pipeline manager that runs every ETL job through a team of specialised AI agents before, during, and after execution. Each agent does one job — size the data, check compliance, validate quality, pick the right compute platform, and learn from every run. Everything is logged to a single audit trail. The result: fewer failures, lower cloud bills, and a defensible compliance posture — all from a single JSON config file."

---

# Part I — Executive Summary

## What the Framework Does

The Strands ETL Orchestration Framework is an **automated decision-making layer** that sits between your data pipeline configuration and the AWS compute services (Glue, EMR, EKS) that execute it.

An engineer writes one JSON config file describing the job — source tables, target table, quality rules, compliance requirements. The framework takes over from there: 11 specialised agents run in coordinated phases, each enriching the execution plan, and then the job runs with the right compute, the right guardrails, and a full audit trail written automatically.

## The Business Problems It Solves

| Problem | Without the Framework | With the Framework |
|---|---|---|
| **Over-provisioned compute** | Engineers guess worker counts | `ResourceAllocatorAgent` calculates workers from actual data size + day-of-week patterns |
| **Silent data quality failures** | Bad data loads into the target | `DataQualityAgent` validates source tables *before* load and target tables *after* |
| **Compliance blind spots** | Manual column audits | `ComplianceAgent` auto-scans every table for PII, maps to GDPR/PCI-DSS/SOX/HIPAA |
| **Wrong compute platform** | Glue for everything, even 500 GB+ jobs | `PlatformConversionAgent` auto-migrates to EMR or EKS when thresholds are crossed |
| **No institutional memory** | Every run starts cold | `LearningAgent` trains per-job ML models to predict cost, runtime, and anomalies |
| **Poor auditability** | Scattered logs across services | One unified audit file (JSONL) per day, mirrored to DynamoDB and S3 |

## Key Outcomes (current system capability)

- **Compute right-sizing**: worker count calculated from measured data size, not guesses; weekend/month-end scaling factors applied automatically.
- **Pre + Post DQ gating**: jobs can be halted before load if critical rules fail; target table validated after load confirms row counts and completeness.
- **Compliance coverage**: PII detection across 9 column-name categories (name, email, phone, address, SSN, financial, ID, health, DOB) mapped to 4 regulatory frameworks.
- **Learning cadence**: per-job ML models (resource, runtime, cost prediction) trained after every 5+ runs; stored and versioned per job — no cross-contamination between pipelines.
- **Single audit record**: every agent event (start, complete, skip, error, DQ rule result, compliance finding, platform decision) written to `data/agent_store/unified_audit/audit_YYYY-MM-DD.jsonl` with an `execution_id` that correlates the entire run.

---

# Part II — High-Level Architecture

## Diagram: End-to-End Flow

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                          PRIVATE VPC (no public internet egress)                     │
│                                                                                      │
│   ┌─────────────────────────────────────────────────────────────────────────────┐    │
│   │  EC2 Orchestration Host  (run_strands_etl.py)                               │    │
│   │                                                                             │    │
│   │  INPUT: etl_config.json ──► StrandsOrchestrator                            │    │
│   │                                  │                                         │    │
│   │          ┌───────────────────────▼──────────────────────────────────┐      │    │
│   │          │  Phase Builder  (topological sort → dependency graph)    │      │    │
│   │          └──────┬─────────────────────────────────────────────┬─────┘      │    │
│   │                 │                                             │             │    │
│   │   ┌─────────────▼───────────────────────────────────────┐   │             │    │
│   │   │  ThreadPoolExecutor  max_workers = min(phase, 10)   │   │             │    │
│   │   │                                                      │   │             │    │
│   │   │  Phase 1 ║ SizingAgent  ComplianceAgent  CodeAnalysis│   │             │    │
│   │   │  Phase 2 ║ ResourceAllocator  DQAgent  HealingAgent  │   │             │    │
│   │   │  Phase 3 ║ PlatformConversion  CodeConversion        │   │             │    │
│   │   │  Phase 4 │ ExecutionAgent  (serial)                  │   │             │    │
│   │   │  Phase 5 ║ LearningAgent  RecommendationAgent        │   │             │    │
│   │   └──────────────────────┬───────────────────────────────┘   │             │    │
│   │                          │ AgentContext (thread-safe)         │             │    │
│   │                          │ shared_state / agent_results       │             │    │
│   └──────────────────────────┼────────────────────────────────────┘            │    │
│                              │                                                  │    │
│         ┌────────────────────┼──────────────────────────────────────────┐      │    │
│         │  VPC Endpoints     │                                           │      │    │
│         │                    ▼                                           │      │    │
│         │   ┌──────────────────────┐   ┌────────────────────────────┐   │      │    │
│         │   │   AWS Glue (jobs)    │   │  Amazon S3 (data + audit)  │   │      │    │
│         │   ├──────────────────────┤   ├────────────────────────────┤   │      │    │
│         │   │   Amazon EMR         │   │  Amazon DynamoDB (state)   │   │      │    │
│         │   ├──────────────────────┤   ├────────────────────────────┤   │      │    │
│         │   │   Amazon EKS         │   │  Amazon CloudWatch         │   │      │    │
│         │   └──────────────────────┘   └────────────────────────────┘   │      │    │
│         └──────────────────────────────────────────────────────────────-┘      │    │
│                                                                                 │    │
│   OUTPUTS:  HTML Dashboard  ·  audit_YYYY-MM-DD.jsonl  ·  CloudWatch metrics   │    │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

**Key constraint**: all AWS API calls traverse VPC endpoints. No traffic leaves the VPC to the public internet.

## Storage Fallback Chain

```
Write request
    │
    ├─► DynamoDB  ──(fail)──► S3  ──(fail)──► Local files
    │   etl_*                  etl-data/       data/agent_store/
    │   tables                 *.psv.gz        *.psv / *.json
    │
Unified Audit always writes local first, then propagates to DynamoDB + S3
```

---

# Part III — Detailed Architecture & Data Flow

## 3.1 Entry Point

**File**: `scripts/run_strands_etl.py`

```
python run_strands_etl.py \
  -c demo_configs/enterprise_sales_config.json \
  [--dry-run] [--platform glue|emr|eks] [--date YYYY-MM-DD] \
  [--verbose] [--use-llm] [--model <bedrock_model_id>]
```

The script:
1. Parses CLI args → applies overrides to the config dict in memory.
2. Constructs `StrandsOrchestrator(config, max_workers=10, fail_fast=False)`.
3. Calls `orchestrator.execute()`.
4. Writes exit code 0 (success) or 1 (failure).

No schema validation occurs at load time. A malformed JSON or missing required key will fail at first agent that reads it.

## 3.2 Orchestrator

**File**: `framework/strands/orchestrator.py`, class `StrandsOrchestrator`

### Initialisation
```python
execution_id = str(uuid.uuid4())[:12]   # e.g. "a3f2b1c94e77"
```
The `execution_id` is the primary correlation key across all audit records for a single run.

### Phase Builder (`_build_execution_phases()`)
Kahn's topological sort:
1. Build in-degree map from each agent's `DEPENDENCIES` list.
2. Enqueue agents with `in_degree == 0` → these form Phase 1.
3. After each phase, decrement in-degree of successors; newly-zero agents form the next phase.
4. Within a phase, agents are sorted by `priority` integer (lower = first, used as tiebreaker).
5. Circular dependencies log a warning but do not raise — the cycle participants may never execute.

### Thread Pool Execution (`_execute_phase()`)
```python
with ThreadPoolExecutor(
    max_workers=min(len(phase_agents), self.max_workers)  # capped at 10
) as executor:
    futures = {executor.submit(agent.run, context): agent for agent in phase_agents}
    for future in as_completed(futures):
        result = future.result()
```
- A fresh executor is created per phase (not shared across phases).
- `as_completed` processes results as they arrive, not in submission order.
- If `fail_fast=True` and an agent returns `AgentStatus.FAILED`, the executor is shut down immediately (`shutdown(wait=False)`).

### AgentContext (Thread-Safe Shared State)
```python
@dataclass
class AgentContext:
    job_name: str
    config: Dict
    execution_id: str
    run_date: datetime
    platform: str
    shared_state: Dict = {}       # protected by threading.Lock
    agent_results: Dict = {}      # one entry per completed agent
```
- `context.set_shared(key, value)` acquires the lock before writing.
- `context.get_shared(key, default)` reads without a lock (read is assumed atomic for Python dicts, but callers should treat values as eventually-consistent during parallel phases).
- Agents signal downstream agents by writing named keys to `shared_state` (e.g., `total_size_gb`, `target_platform`, `execution_phase`).

## 3.3 The Five Execution Phases

### Phase 1 — Pre-Analysis (Parallel)

| Agent | Input | Output (shared_state keys) |
|---|---|---|
| `SizingAgent` | `source_tables[]` in config | `total_size_gb`, `table_sizes`, `recommended_workers` |
| `ComplianceAgent` | `compliance.table_scans[]`, `global_pii_columns[]` | `compliance_findings`, `pii_columns_detected`, `compliance_violations` |
| `CodeAnalysisAgent` | `script.path` (PySpark file) | code anti-patterns, join optimisation recommendations |

ComplianceAgent scans each configured table for PII column names using pattern matching across 9 PII types. It maps findings to applicable frameworks (GDPR, PCI-DSS, SOX, HIPAA) and sets `compliance_status = compliant | non_compliant`.

### Phase 2 — Optimisation (Parallel)

| Agent | Dependencies | Key Logic |
|---|---|---|
| `ResourceAllocatorAgent` | `sizing_agent` | `workers = ceil(total_size_gb / GB_PER_WORKER)` × day/weekend/month-end scale factors |
| `DataQualityAgent` | `compliance_agent` | Reads `execution_phase` from shared state; runs `pre_load_rules` or `post_load_rules` accordingly |
| `HealingAgent` | none | Builds retry/healing strategy from config; ready to apply post-execution if job fails |

`DataQualityAgent` supports 4 rule types: `not_null`, `positive`, `unique`, `completeness`, `row_count_check`, `greater_than`, `less_than`, `pattern`, plus raw SQL rules. Each rule produces: `records_scanned`, `outliers_found`, `actual_value`, `threshold`, `pass_fail_status`.

Worker cost rates (ResourceAllocatorAgent):

| Worker Type | Cost/hr |
|---|---|
| G.1X | $0.44 |
| G.2X | $0.88 |
| G.4X | $1.76 |
| G.8X | $3.52 |

### Phase 3 — Platform Decision (Parallel)

| Agent | Logic |
|---|---|
| `PlatformConversionAgent` | `total_size_gb > 500` → EKS; `> 100` → EMR; else Glue. Checks `agents.platform_conversion_agent.enabled` first, then `platform.auto_convert.enabled`. |
| `CodeConversionAgent` | If platform changed, rewrites `GlueContext`/`DynamicFrame` calls to `SparkSession`/`DataFrame` API. Depends on `PlatformConversionAgent`. |

### Phase 4 — Execution (Serial)

`ExecutionAgent` is the only agent with `PARALLEL_SAFE = False`. It invokes the actual AWS job (Glue `start_job_run`, EMR `run_job_flow`, or EKS `SparkApplication` CRD submission) using the configuration emitted by Phase 3. On completion it writes `job_metrics` to shared state: `duration_seconds`, `records_read`, `records_written`, `cost_usd`.

### Phase 5 — Post-Analysis (Parallel)

| Agent | Logic |
|---|---|
| `LearningAgent` | Stores execution record in per-job history; trains 3 ML models if ≥5 records exist; saves models to `data/models/<job_name>/` |
| `RecommendationAgent` | Aggregates recommendations from all 9 upstream agents; prioritises by impact; writes consolidated report |

The orchestrator overrides `LearningAgent.DEPENDENCIES` to `['execution_agent']` at runtime, ensuring it only trains after a completed execution.

---

# Part IV — Agent Call Mechanics

## Per-Agent Execution Contract

Every agent class inherits from `StrandsAgent`. The orchestrator calls `agent.run(context)` (never `agent.execute()` directly).

```
agent.run(context)
    │
    ├─ 1. Check agents.<agent_name>.enabled in config
    │       → if "N" or missing: return AgentResult(skipped=True), log to audit, stop
    │
    ├─ 2. Check each dependency in DEPENDENCIES
    │       → if any not COMPLETED: return AgentResult(status=WAITING)
    │
    ├─ 3. Log agent_start event to UnifiedAuditLogger
    │
    ├─ 4. Call self.execute(context)   ← subclass implements this
    │
    ├─ 5. Record execution_time_ms
    │
    ├─ 6. Log agent_complete (or agent_skip if output['skipped']=True)
    │
    └─ 7. Log each recommendation as a separate audit event
         Return AgentResult
```

On unhandled exception: catches `Exception`, logs `agent_error` event with `error_type` and `error_message`, returns `AgentResult(status=FAILED)`. The exception does **not** propagate to the thread pool unless `fail_fast=True`.

## Enable/Disable Any Agent

```json
"agents": {
  "<agent_name>": {
    "enabled": "Y",   // "Y"|"y"|"yes"|"YES"|true|"true" = run
    "enabled": "N"    // anything else = skip
  }
}
```

Valid `agent_name` values: `sizing_agent`, `compliance_agent`, `code_analysis_agent`, `resource_allocator_agent`, `data_quality_agent`, `healing_agent`, `platform_conversion_agent`, `code_conversion_agent`, `execution_agent`, `learning_agent`, `recommendation_agent`.

If an agent is not present in the `agents` block, it defaults to **disabled**.

---

# Part V — Agent Training (LearningAgent)

## What Is Trained

Three `SimpleLinearModel` instances per job, using custom gradient descent (`learning_rate=0.01`, `iterations=100`, z-score normalised features):

| Model | Features | Target | Min Samples | Eval Metric |
|---|---|---|---|---|
| Resource Predictor | `size_gb`, `is_weekend`, `day_of_week` | `recommended_workers` | 3 | R² |
| Runtime Estimator | `size_gb`, `workers`, `size_per_worker` | `duration_seconds` | 3 | MAE |
| Cost Predictor | `workers`, `duration_hours`, `worker_hours` | `cost_usd` | 3 | MAPE% |

Training does not start until the per-job history has ≥ `min_training_records` (default **5**) entries. Configure via `learning.min_training_records`.

## Data Isolation

Each job writes to its own namespace:
```
data/models/
└── complex_enterprise_sales/     ← sanitised job_name
    ├── execution_history.json    ← capped at 500 records
    ├── model_registry.json
    ├── RES-<sha256[:12]>.pkl
    ├── RUN-<sha256[:12]>.pkl
    └── COS-<sha256[:12]>.pkl
```

No data from `job_A` ever enters `job_B`'s training set.

## Model Versioning

Model IDs are deterministic:
```
model_id = f"{type[:3].upper()}-{sha256(type:features:timestamp)[:12]}"
# e.g. "RES-a3f2b1c94e77"

version  = f"1.{len(registry)}.0"
# Monotonically increments with registry size
```

There is no formal promotion gate or A/B testing in the current implementation. Each training run replaces prior models in the registry by adding a new entry. Rollback = restore `model_registry.json` from S3/backup.

## Operational vs. Offline Training

All training is **online / incremental** — the LearningAgent runs in Phase 5 of every execution, trains immediately on the accumulated history, and writes the new model. There is no separate offline training pipeline. The agent reads the accumulated JSONL history file, not a data warehouse.

---

# Part VI — Cost Model & Per-Run Estimate

> **Note**: All unit prices below are **assumptions/placeholders**. Replace with your AWS pricing tier and any negotiated EDP rates.

## Cost Components Per Run

| Component | Formula | Placeholder Rate | Notes |
|---|---|---|---|
| **Glue compute** | `workers × cost_per_hour × duration_hours` | G.2X = $0.88/hr/worker | Actual rate from AWS console |
| **EMR compute** | `core_count × ec2_rate × duration_hours × (1 - spot_discount)` | m5.2xlarge ≈ $0.38/hr; spot ~60% discount | Only if PlatformConversion triggers EMR |
| **EKS compute** | `executor_instances × (cpu × cpu_rate + mem × mem_rate) × duration_hours` | Spot ARM64 ~$0.05/vCPU-hr (TBD) | Only if size > 500 GB |
| **S3 I/O** | `(PUT_count × $0.005/1k) + (GET_count × $0.0004/1k) + (storage_GB × $0.023/GB-month)` | Standard pricing | Scales with table count and audit volume |
| **DynamoDB** | `(WCU × $0.00065) + (RCU × $0.00013)` per million | On-demand pricing | ~1 WCU per audit event |
| **CloudWatch** | `log_GB × $0.50 + metrics × $0.30/metric/month` | Standard pricing | 11 custom metrics published |
| **LLM inference** | `(input_tokens × $INPUT_RATE) + (output_tokens × $OUTPUT_RATE)` | Bedrock: Claude 3.5 Sonnet ≈ $3/$15 per 1M tokens (placeholder) | Only if `--use-llm` flag set |
| **Orchestration host** | `ec2_hourly × (duration_hours)` | t3.medium ≈ $0.042/hr (on-demand) | Amortise across concurrent jobs |
| **Model training** | `records × $0.00001 + training_seconds × $0.0005` | Internal simulation cost | Low; ~$0.001 per training run |

## Worked Example — Enterprise Sales Job

```
Assumptions:
  - 21 source tables, ~150 GB total
  - Platform: Glue (below 100 GB EMR threshold per table, total triggers check)
  - Worker type: G.2X, 20 workers
  - Duration: 45 minutes (0.75 hr)
  - LLM: disabled
  - 11 DQ rules + 4 compliance table scans

Glue compute:   20 workers × $0.88 × 0.75 hr         = $13.20
S3 I/O:         ~500 PUTs + ~2000 GETs + 150 GB read  ≈ $0.01 + $0.001 + $3.45 = $3.46
DynamoDB:       ~200 audit events × 1 WCU             ≈ $0.00013
CloudWatch:     11 metrics × ~$0.0003                 ≈ $0.003
Orchestration:  t3.medium 0.75 hr                     = $0.032
─────────────────────────────────────────────────────────────────
Total per run:                                       ≈ $16.70

With --use-llm (est. 50k input + 5k output tokens):
  LLM add-on: (50k × $3 + 5k × $15) / 1M            ≈ $0.225
  LLM-enabled total:                                 ≈ $16.93
```

## Sensitivity Analysis (±25%)

| Variable | -25% | Baseline | +25% |
|---|---|---|---|
| Data volume (150 GB baseline) | ~$12.60 | ~$16.70 | ~$20.80 |
| Workers (20 baseline) | ~$12.60 | ~$16.70 | ~$20.80 |
| LLM token volume | ~$16.87 | ~$16.93 | ~$16.99 |
| Duration (45 min baseline) | ~$12.85 | ~$16.70 | ~$20.55 |

LLM cost is a small fraction at this scale. Compute (Glue workers × time) is the dominant variable.

---

# Part VII — Audit, Observability & Governance

## What Is Logged

Every agent event is emitted to the `UnifiedAuditLogger` with these fields:

```
event_id          — 8-char UUID fragment
event_type        — agent_start | agent_complete | agent_skip | agent_error |
                    data_quality | compliance_check | platform_conversion |
                    recommendation | learning_update | prediction | ...
timestamp         — ISO-8601 UTC with 'Z' suffix
job_name          — from config
execution_id      — 12-char UUID; correlates all events in one run
agent_name        — which agent emitted the event
status            — pass | fail | skipped | completed | started | ...
message           — human-readable summary
duration_ms       — agent wall-clock time
records_scanned   — (DQ events) rows checked
outliers_found    — (DQ events) rows violating the rule
pass_fail_status  — PASS | FAIL
dq_score          — 0-100 quality score
compliance_status — compliant | non_compliant
pii_columns[]     — list of detected PII column names
violations[]      — compliance violation descriptions
error_type        — Python exception class name
error_message     — exception message
recommendations[] — list of recommendation strings
cost_usd          — job cost for execution events
metadata{}        — agent-specific payload (e.g. DQ rule details)
```

**LLM prompts and responses** are logged inside `metadata` when `--use-llm` is active (CodeAnalysisAgent).

## Where It Is Stored

| Storage | Location | Format | Retention |
|---|---|---|---|
| **Local** | `data/agent_store/unified_audit/audit_YYYY-MM-DD.jsonl` | NDJSON | Configure per host |
| **S3** | `s3://<bucket>/audit-events/year=Y/month=M/day=D/hour=H/events_*.json.gz` | Gzip NDJSON, Hive-partitioned | S3 Lifecycle policy (TBD) |
| **DynamoDB** | `etl_unified_audit` table | PK=`JOB#<name>`, SK=`EVENT#<ts>#<id>`, GSI on event type | DynamoDB TTL (TBD) |
| **CloudWatch Logs** | `/etl-framework/jobs` | Structured text | CW Logs retention (TBD) |
| **Agent PSV files** | `data/agent_store/<agent_name>/<table>_<date>.psv` | Pipe-delimited | Local disk |

## Traceability & Replay

- Filter all events for a run: `execution_id = "a3f2b1c9..."`
- Query by event type: DynamoDB GSI `gsi1pk = TYPE#data_quality`
- Athena query: `WHERE execution_id = 'xxx' AND event_type = 'agent_error'`
- Replay: re-run `run_strands_etl.py` with the same config file. The framework is stateless at start; `execution_id` is re-generated each run.

## PII Safeguards

- Column names containing PII patterns are detected at compliance-check time (Phase 1).
- `mask_pii: "Y"` + `mask_strategy: "hash"` in config instructs downstream transforms to apply hashing.
- PII column names are written to audit records as `pii_columns[]` — column **names** only, never column **values**.

---

# Part VIII — Security & Networking Posture

## Network Boundary

All service calls route through VPC endpoints. No traffic to the public internet.

```
VPC Endpoint targets (configured externally to this framework):
  - com.amazonaws.<region>.glue
  - com.amazonaws.<region>.s3
  - com.amazonaws.<region>.dynamodb
  - com.amazonaws.<region>.logs
  - com.amazonaws.<region>.monitoring
  - com.amazonaws.<region>.bedrock-runtime   (only if --use-llm)
```

## IAM Least Privilege

IAM policies exist for each compute target:

| Role | Scope |
|---|---|
| `etl_framework_emr_policy.json` | EMR-specific S3, DynamoDB, CloudWatch |
| `etl_framework_eks_policy.json` | EKS Spark, Karpenter node provisioning |
| `etl_eks_role.json` | IRSA attachment with `AmazonEKSVPCResourceController` |

Policies include `ec2:DescribeSecurityGroups` for network interface inspection. No `*:*` actions.

## Encryption

- **At rest**: S3 SSE-S3 or SSE-KMS (configured via bucket policy, external to framework). DynamoDB encryption at rest enabled by default. Pickle model files stored locally — consider KMS-encrypted EBS if required.
- **In transit**: all AWS SDK calls use TLS 1.2+. VPC endpoint traffic never traverses the public internet.
- **Secrets**: no credentials are stored in the JSON config files. The framework relies on IAM instance profiles / IRSA.

---

# Part IX — SLAs, SLOs & Capacity Planning

## Expected Runtime Bands

| Data Volume | Platform | Workers | Expected Duration |
|---|---|---|---|
| < 10 GB | Glue | 5–8 × G.2X | 5–15 min |
| 10–100 GB | Glue | 10–20 × G.2X | 15–60 min |
| 100–500 GB | EMR (auto) | 10–20 × m5.2xlarge | 30–90 min |
| > 500 GB | EKS (auto) | 20–40 executors | 60–180 min |

These are indicative. Actual durations depend on join complexity (window functions carry a 2× weight in the complexity scoring model), data skew, and network I/O.

## Error Budget

The framework is designed for a **99% success rate** target:
- Auto-healing (`HealingAgent`) handles OOM, shuffle, timeout, and connection errors with configurable retries (default `max_retries=3`).
- `fail_fast=False` (default) means a single agent failure does not abort the run.
- CloudWatch alarm `ETL-HighFailureRate` fires when `SuccessRate < 80%` over a 1-hour window.

## Scaling Guidance

```
Workers needed ≈ ceil(data_size_GB / GB_per_worker_type)

G.2X: assume ~5 GB effective per worker (memory + shuffle headroom)
G.4X: assume ~12 GB effective per worker

Weekend scale factor:   0.6 × weekday workers
Month-end scale factor: 1.5 × weekday workers
Quarter-end:            2.0 × weekday workers
```

ThreadPoolExecutor `max_workers=10` applies to the **agent orchestration layer**, not to Spark workers. Glue/EMR worker counts are determined by the `ResourceAllocatorAgent` independently.

---

# Part X — Risks & Trade-offs

## Failure Modes

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| **Circular dependency in agent graph** | Low | Affected agents silently never run | Topological sort logs a warning; add a startup validation check (not yet implemented) |
| **AgentContext read-write race** | Low-Medium | Stale `shared_state` value in parallel phase | Writes are lock-protected; reads are not. Design agents so they write to unique keys, never the same key in parallel |
| **Pickle model files corrupted** | Low | LearningAgent skips model load, warns | Registry entry remains; old model still accessible if `.pkl` is intact |
| **S3 batch flush on process kill** | Medium | Up to 100 audit events lost | Flush is synchronous on normal exit; add SIGTERM handler (not yet implemented) |
| **DQ runs before data exists (post-load)** | Medium | Post-load DQ validates empty/partial table | Orchestrator must set `execution_phase = post_load` after ExecutionAgent completes |
| **Data skew on large joins** | High for wide joins | Shuffle OOM, job failure | `HealingAgent` detects and recommends `skewJoin.enabled`; AQE is enabled by default in Spark configs |

## Cost Spikes

- **Token surge (LLM)**: `--use-llm` with large PySpark scripts can generate 100k+ tokens. Cap script size sent to Bedrock, or disable LLM for non-complex jobs.
- **Platform mis-classification**: if Glue sizing data is stale (cached for 24 hrs), a 90 GB job may be classified as < 100 GB and run on Glue instead of EMR. Force refresh with `source_sizing.cache_ttl_hours: 0`.
- **Month-end amplification**: `2.0` quarter-end scale factor doubles worker count and cost. Verify this is intentional for your financial ETL windows.

## Model Drift

- The `SimpleLinearModel` is a linear regression. Non-linear relationships (e.g., exponential cost growth with skewed data) will underfit over time.
- No automatic drift detection; the `LearningAgent` does not compare new model metrics against previous versions before promotion.
- **Mitigation**: monitor `mape_percent` in the model registry. Set a threshold alert if MAPE exceeds 20%.

## Known Gaps (not yet implemented)

- No `startup validation` that checks all enabled agents have satisfied dependency definitions before the first phase runs.
- No `SIGTERM handler` to flush the S3 audit batch buffer on host shutdown.
- Post-load DQ phase requires the orchestrator to explicitly call `context.set_shared('execution_phase', 'post_load')` after `ExecutionAgent` completes — this wiring must be verified in `orchestrator.py`.
- Model promotion gate (compare new model R²/MAPE against prior before replacing) is not implemented.
- `config` schema validation is not performed at startup; bad configs fail mid-run.

---

# Part XI — Glossary

| Term | Definition |
|---|---|
| **agent** | A single-responsibility Python class that inherits `StrandsAgent`, implementing `execute(context)` |
| **AgentContext** | Thread-safe dataclass passed to every agent; holds `shared_state`, `agent_results`, `config`, `job_name`, `execution_id`, `run_date` |
| **execution_id** | 12-character UUID prefix generated once per orchestrator invocation; correlates all audit events for a run |
| **phase** | A group of agents whose dependencies are all satisfied; agents in the same phase run in parallel |
| **pre_load** | DQ validation executed on source tables before the ETL job runs |
| **post_load** | DQ validation executed on the target table after the ETL job completes |
| **PII** | Personally Identifiable Information; detected by column-name pattern matching |
| **PSV** | Pipe-Separated Values; legacy local file format for agent output (`.psv`) |
| **JSONL / NDJSON** | Newline-delimited JSON; one JSON object per line; used in unified audit files |
| **IRSA** | IAM Roles for Service Accounts (EKS-specific IAM binding mechanism) |
| **AQE** | Adaptive Query Execution — Spark 3 feature for runtime optimisation of joins and partitions |
| **DPU** | Data Processing Unit — AWS Glue's unit of compute capacity |
| **SimpleLinearModel** | Custom gradient-descent linear regression model in `learning_agent.py`; no external ML library dependency |
| **fail_fast** | Orchestrator mode where first agent failure aborts the entire run; default `False` |
| **StorageBackend** | Enum: `DYNAMODB`, `S3`, or `LOCAL`; drives the fallback chain in `StrandsStorage` |
| **unified audit** | Single JSONL event stream written by `UnifiedAuditLogger` covering all agent events in a run |
