# Enterprise ETL Framework - Architect Guide

## 1. Architecture Overview

The Enterprise ETL Framework uses a **multi-agent architecture** with two execution modes:

```
                         +---------------------------+
                         |    Configuration JSON     |
                         |  (enterprise_sales_config)|
                         +-------------+-------------+
                                       |
                          +------------+------------+
                          |                         |
                +---------v--------+      +---------v--------+
                |  Rule-Based CLI  |      | Strands SDK CLI  |
                |  (No Bedrock)    |      |  (LLM-Powered)   |
                | run_strands_etl  |      | run_strands_swarm |
                +--------+---------+      +--------+---------+
                         |                         |
                +--------v---------+     +---------v--------+
                | StrandsOrchest-  |     |   ETLSwarm       |
                | rator (framework)|     |  (strands_sdk)   |
                +--------+---------+     +--------+---------+
                         |                         |
              +----------v----------+   +----------v----------+
              | 11 Rule-Based Agents|   | 10 LLM Agents       |
              | (framework/strands) |   | (strands.Agent)      |
              +----------+----------+   +----------+----------+
                         |                         |
                +--------v-------------------------v--------+
                |            AWS Services                   |
                | Glue | EMR | EKS+Karpenter | S3 | CW     |
                +-----------------------------------------------+
```

### Two Execution Modes

| Aspect | Rule-Based CLI | Strands SDK CLI |
|--------|---------------|-----------------|
| Script | `scripts/run_strands_etl.py` | `scripts/run_strands_swarm.py` |
| Package | `framework/strands/` | `strands_sdk/` |
| LLM Required | No | Yes (Bedrock) |
| Agent Logic | Python code (deterministic) | LLM reasoning + tools |
| Cost | No LLM cost | Bedrock API charges |
| Use Case | Production, CI/CD, scheduled | Exploration, ad-hoc analysis |
| Orchestrator | `StrandsOrchestrator` | `ETLSwarm` |

### Strands SDK Integration

The `strands_sdk/` package uses the actual `strands-agents` Python library:

```python
from strands import Agent, tool

# Each agent is an LLM-powered entity
agent = Agent(
    model="us.anthropic.claude-sonnet-4-20250514-v1:0",  # Bedrock model
    system_prompt="You are a sizing agent...",
    tools=[get_table_size, list_glue_tables]
)

# Agent reasons and calls tools autonomously
response = agent("Analyze table sizes for the ETL job")
```

Key classes used from `strands`:
- `strands.Agent` - LLM-powered agent with tool calling
- `strands.tool` - Decorator to register functions as agent tools

> Note: `strands.swarm.Swarm` does NOT exist in the library. Orchestration is implemented via a custom `ETLSwarm` class using `ThreadPoolExecutor`.

---

## 2. Agent Execution Phases

Both CLIs follow the same phase-based execution model:

```
Phase 1 (Parallel)       Phase 2 (Parallel)       Phase 3
+------------------+     +------------------+     +------------------+
| SizingAgent      |---->| ResourceAlloc    |---->| PlatformConvert  |
| ComplianceAgent  |  +->| DataQualityAgent |     +------------------+
| CodeAnalysisAgent|  |  | HealingAgent     |            |
+------------------+  |  +------------------+            v
                      |                          Phase 4 (Parallel)
                      |                          +------------------+
                      |                          | CodeConversion   |
                      |                          | HealingAgent     |
                      |                          +------------------+
                      |                                  |
                      |                                  v
                      |                          Phase 5
                      |                          +------------------+
                      |                          | ExecutionAgent   |
                      |                          +------------------+
                      |                                  |
                      |                                  v
                      |                          Phase 6 (Parallel)
                      |                          +------------------+
                      +------------------------->| LearningAgent    |
                                                 | RecommendAgent   |
                                                 +------------------+
```

### Agent Dependency Graph

```
sizing_agent ----+---> resource_allocator_agent ---> platform_conversion_agent
                 |                                         |
                 +----------------------------------------->---> code_conversion_agent
                                                           |
compliance_agent ---> data_quality_agent                   v
                                                    execution_agent
code_analysis_agent (parallel, no deps)                    |
                                                           v
healing_agent (parallel, no deps)              learning_agent + recommendation_agent
```

Dependencies are resolved using **Kahn's algorithm** (topological sort) in `StrandsOrchestrator._build_execution_phases()`.

---

## 3. Resource Allocation Decision System

### Decision Flow: `force_from_config` Flag

```
                    +----------------------------+
                    | smart_allocation.enabled?  |
                    +------------+---------------+
                                 |
                         Yes     |      No
                    +------------+------+----> SKIP (use glue_config defaults)
                    |
                    v
          +---------------------------+
          | force_from_config = Y?    |
          +--------+--------+---------+
                   |        |
              Yes  |        | No
                   v        v
         +-----------+  +---------------------------+
         | Use values|  | Agent decides:            |
         | from      |  | 1. base_workers(size)     |
         | glue_     |  | 2. * scale_factor(day)    |
         | config    |  | 3. * complexity_factor    |
         | exactly   |  | 4. recommend_worker_type  |
         +-----------+  +---------------------------+
```

### Config Flag

In `enterprise_sales_config.json`:
```json
"smart_allocation": {
    "enabled": "Y",
    "force_from_config": "N",
    "_force_comment": "Y = use glue_config workers/type exactly. N = agent decides"
}
```

When `force_from_config = "Y"`:
- Workers count = `glue_config.number_of_workers`
- Worker type = `glue_config.worker_type`
- No scaling or complexity adjustments

When `force_from_config = "N"` (agent decides):

#### Step 1: Base Workers from Data Volume

| Data Size (GB) | Base Workers |
|----------------|-------------|
| < 10           | 2           |
| 10 - 50        | 5           |
| 50 - 100       | 10          |
| 100 - 500      | 20          |
| > 500          | min(50, size/20) |

#### Step 2: Day-Type Scale Factor

| Day Type     | Scale Factor | Rationale |
|-------------|-------------|-----------|
| Weekday     | 1.0         | Normal operations |
| Weekend     | 0.6         | Reduced load, no business activity |
| Month-End   | 1.5         | Financial close processing |
| Quarter-End | 2.0         | Heavy reporting, financial close |

#### Step 3: Complexity Factor

Complexity is calculated from script analysis (JOINs, window functions, aggregations, source table count):

```python
factor = 1.0

# Source table count
if source_count > 15:  factor *= 1.5
elif source_count > 10: factor *= 1.3
elif source_count > 5:  factor *= 1.1

# JOIN count (from code_analysis_agent)
if join_count > 10:     factor *= complexity_config.get('join_count_weight', 1.5)
elif join_count > 5:    factor *= 1.2

# Window functions
if window_count > 3:    factor *= complexity_config.get('window_function_weight', 2.0)
elif window_count > 0:  factor *= 1.3

# Aggregations (GROUP BY, DISTINCT)
if agg_count > 5:       factor *= complexity_config.get('aggregation_weight', 1.2)

# Cap at 3.0x
factor = min(factor, 3.0)
```

Config-driven weights:
```json
"complexity_factors": {
    "join_count_weight": 1.5,
    "window_function_weight": 2.0,
    "aggregation_weight": 1.2,
    "distinct_weight": 1.3
}
```

#### Final Calculation

```
recommended_workers = max(2, base_workers * scale_factor * complexity_factor)
```

#### Worker Type Selection

Based on memory per worker = `total_size_gb / workers * 2` (2x headroom):

| Memory/Worker (GB) | Worker Type | Memory | vCPUs | Cost/hr |
|--------------------|-------------|--------|-------|---------|
| <= 16              | G.1X        | 16 GB  | 4     | $0.44   |
| 16 - 32            | G.2X        | 32 GB  | 8     | $0.88   |
| 32 - 64            | G.4X        | 64 GB  | 16    | $1.76   |
| > 64               | G.8X        | 128 GB | 32    | $3.52   |

---

## 4. Platform Auto-Conversion

### Decision Criteria

```
                +---------------------------+
                | force_platform set?       |
                +--------+--------+---------+
                    Yes  |        | No
                         v        v
               Use forced   +---------------------------+
               platform     | total_size_gb > 500?      |
                            +--------+--------+---------+
                                Yes  |        | No
                                     v        v
                            Use EKS    +---------------------------+
                            Karpenter  | total_size_gb > 100?      |
                                       +--------+--------+---------+
                                           Yes  |        | No
                                                v        v
                                        Use EMR     Stay on Glue
```

Thresholds are configurable:
```json
"auto_convert": {
    "enabled": "Y",
    "convert_to_emr_threshold_gb": 100,
    "convert_to_eks_threshold_gb": 500
}
```

### What Each Platform Conversion Does

#### Glue to EMR

**Infrastructure conversion** (`platform_conversion_agent.py`):
```
Glue Worker Type  ->  EMR Instance Type
G.1X              ->  m5.xlarge
G.2X              ->  m5.2xlarge
G.4X              ->  m5.4xlarge
G.8X              ->  m5.8xlarge
```

Generates EMR cluster config:
- 1 Master node (ON_DEMAND)
- N Core nodes (SPOT or ON_DEMAND based on `emr_config.use_spot`)
- Spark dynamic allocation enabled
- Adaptive query execution enabled

**Code conversion** (`code_conversion_agent.py`):

| Glue Pattern | EMR Replacement |
|-------------|-----------------|
| `from awsglue.context import GlueContext` | `from pyspark.sql import SparkSession` |
| `from awsglue.job import Job` | *(removed)* |
| `from awsglue.dynamicframe import DynamicFrame` | *(removed)* |
| `GlueContext(spark_context)` | `SparkSession.builder.appName("EMR-Job").enableHiveSupport().getOrCreate()` |
| `glueContext.create_dynamic_frame.from_catalog(database=X, table_name=Y)` | `spark.table("X.Y")` |
| `dynamic_frame.toDF()` | *(identity - already DataFrame)* |
| `Job(glueContext); job.init(); job.commit()` | *(removed; replaced with print)* |
| `glueContext.` | `spark.` |

#### Glue to EKS with Karpenter

**Infrastructure conversion**:
- Generates a `SparkApplication` CRD (Kubernetes custom resource)
- Sets Karpenter provisioner node selectors
- Maps Glue worker types to K8s CPU/memory requests:

```
G.1X -> cpu: 4,  memory: 16Gi
G.2X -> cpu: 8,  memory: 32Gi
G.4X -> cpu: 16, memory: 64Gi
G.8X -> cpu: 32, memory: 128Gi
```

- Enables dynamic allocation with `min=2, initial=N, max=N*2`
- Karpenter provisioner allows both `spot` and `on-demand`, both `amd64` and `arm64`

**Code conversion** (extends EMR conversion):
- All EMR transformations apply first
- Additionally adds Kubernetes Spark configs:
  ```python
  .config("spark.kubernetes.container.image", "spark:3.5.0")
  .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")
  ```

---

## 5. ML Learning System

### Model Training Pipeline

```
Execution History (JSON) --> Feature Extraction --> Gradient Descent --> Model (pickle)
                                                                            |
                                                                     Model Registry (JSON)
```

### Three ML Models

| Model | Features | Target | Use Case |
|-------|----------|--------|----------|
| Resource Predictor | size_gb, is_weekend, day_of_week | recommended_workers | Pre-execution worker allocation |
| Runtime Estimator | size_gb, workers, size_per_worker | duration_seconds | Time estimation |
| Cost Predictor | workers, duration_hours, worker_hours | cost_usd | Budget forecasting |

### Training Requirements
- Minimum 5 execution records to start training
- Models retrained on every execution
- Model IDs: `RES-<sha256[:12]>`, `RUN-<sha256[:12]>`, `COS-<sha256[:12]>`
- Models persisted as pickle files in `data/models/`
- Registry stored as JSON in `data/models/model_registry.json`

### Training Cost Tracking
- $0.01 per 1000 records processed
- $0.50 per hour of compute time
- Costs tracked per model and aggregated

---

## 6. Storage Architecture

### Fallback Chain
```
DynamoDB (primary) --> S3 (secondary) --> Local pipe-delimited files (fallback)
```

All agent data stored as pipe-delimited (`|`) files in `data/agent_store/`:
```
data/agent_store/
  audit_logs/          - Timestamped audit events
  sizing_agent/        - Table size results
  resource_allocator/  - Allocation decisions
  compliance_agent/    - PII findings
  data_quality_agent/  - Quality rules and results
  learning_agent/      - Execution history, training logs
  recommendation_agent/ - Aggregated recommendations
```

### Why Pipe-Delimited
- No external dependencies (works without AWS)
- Human-readable in any text editor
- Compatible with Unix tools (`awk -F'|'`, `cut`)
- Can be uploaded to S3 and loaded into Athena/Redshift

---

## 7. Shared State Between Agents

Agents communicate through a shared context dictionary:

| Key | Set By | Used By |
|-----|--------|---------|
| `total_size_gb` | SizingAgent | ResourceAllocator, PlatformConversion |
| `recommended_workers` | ResourceAllocator | PlatformConversion, Execution |
| `recommended_worker_type` | ResourceAllocator | PlatformConversion, Execution |
| `target_platform` | PlatformConversion | CodeConversion, Execution |
| `converted_config` | PlatformConversion | Execution |
| `platform_conversion_needed` | PlatformConversion | CodeConversion |
| `converted_code` | CodeConversion | Execution |
| `code_analysis` | CodeAnalysis | ResourceAllocator (complexity) |
| `compliance_findings` | ComplianceAgent | Learning |
| `dq_summary` | DataQuality | Learning |
| `job_execution` | ExecutionAgent | Learning |
| `job_metrics` | ExecutionAgent | Learning |

---

## 8. Cost Model

### Per-Platform Cost Calculation

**Glue**: `cost = workers * DPU_rate * duration_hours`
- G.1X: $0.44/DPU-hr, G.2X: $0.88, G.4X: $1.76, G.8X: $3.52

**EMR**: `cost = instances * instance_rate * duration_hours`
- m5.xlarge: $0.192/hr, m5.2xlarge: $0.384/hr, m5.4xlarge: $0.768/hr

**EKS**: `cost = (executors * $0.40 * duration_hours) * 1.10`
- 10% overhead for EKS management

---

## 9. Security and Compliance

### Compliance Agent Checks
- PII detection: SSN, email, phone, address, credit card, DOB
- Encryption: S3 SSE-S3/SSE-KMS, TLS in transit
- Frameworks: GDPR, PCI-DSS, SOX, HIPAA
- Masking strategy: hash-based

### IAM Policies
Located in `iam/policies/`:
- Separate policies for Glue, EMR, EKS, S3, DynamoDB
- Trust policies for cross-service access
- Least-privilege principle

---

## 10. Error Recovery

### Auto-Healing Strategy
```
Job Failure
    |
    v
Healing Agent analyzes error
    |
    +-> Memory Error: Scale up worker type (G.2X -> G.4X)
    +-> Shuffle Error: Increase spark.sql.shuffle.partitions
    +-> Timeout: Increase timeout + add workers
    +-> Connection: Retry with exponential backoff
    +-> Data Skew: Enable skew join handling
    +-> Partition Error: Repartition before write
```

### Orchestrator Fail-Fast Mode
- `fail_fast: false` (default) - Continue executing remaining agents
- `fail_fast: true` - Stop on first agent failure

---

## 11. Configuration Schema

The configuration JSON (`enterprise_sales_config.json`) is the single source of truth:

```
enterprise_sales_config.json
  |
  +-- job_name, job_description
  +-- script (S3 path + local path)
  +-- job_arguments
  +-- platform
  |     +-- primary, fallback_chain
  |     +-- auto_convert (thresholds)
  |     +-- code_conversion
  +-- smart_allocation
  |     +-- force_from_config (Y/N)
  |     +-- complexity_factors
  |     +-- scale_factors
  +-- source_sizing (auto_detect, delta_mode)
  +-- storage (DynamoDB, S3, local fallback)
  +-- agents (per-agent enable/disable, phase, dependencies)
  +-- glue_config (workers, type, Spark configs)
  +-- emr_config (instance type, spot, Spark configs)
  +-- eks_config (Karpenter, namespace, executor config)
  +-- auto_healing
  +-- code_analysis
  +-- data_quality (NL rules, SQL rules, template rules)
  +-- compliance (PII columns, frameworks, masking)
  +-- learning (model training, anomaly detection)
  +-- recommendation
  +-- audit
  +-- source_tables[] (21 tables across 8 domains)
  +-- target_tables[]
  +-- schedule (cron)
```
