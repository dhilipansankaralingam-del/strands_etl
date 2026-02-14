# Enterprise ETL Framework - Data Engineer Guide

## Quick Start

### Prerequisites

```bash
# Python 3.9+
pip install strands-agents strands-agents-tools boto3

# Optional: For LLM-powered mode
export AWS_REGION=us-east-1
# Ensure Bedrock access is configured
```

### Run the Pipeline (Rule-Based, No Bedrock Required)

```bash
# Standard execution
python scripts/run_strands_etl.py -c demo_configs/enterprise_sales_config.json

# Dry-run mode (no actual AWS calls)
python scripts/run_strands_etl.py -c demo_configs/enterprise_sales_config.json --dry-run

# Force a specific platform
python scripts/run_strands_etl.py -c demo_configs/enterprise_sales_config.json --platform emr

# Override run date
python scripts/run_strands_etl.py -c demo_configs/enterprise_sales_config.json --date 2026-03-31

# Verbose logging
python scripts/run_strands_etl.py -c demo_configs/enterprise_sales_config.json -v
```

### Run the Pipeline (LLM-Powered via Strands SDK, Requires Bedrock)

```bash
# Dry-run (default - simulated AWS, real Bedrock calls)
python scripts/run_strands_swarm.py -c demo_configs/enterprise_sales_config.json

# Execute actual jobs
python scripts/run_strands_swarm.py -c demo_configs/enterprise_sales_config.json --execute

# Use a specific model
python scripts/run_strands_swarm.py -c demo_configs/enterprise_sales_config.json \
    --model us.anthropic.claude-sonnet-4-20250514-v1:0

# Run a single agent
python scripts/run_strands_swarm.py -c demo_configs/enterprise_sales_config.json \
    --agent sizing_agent --task "Analyze table sizes"

# List available agents
python scripts/run_strands_swarm.py --list-agents

# Customize an agent prompt
python scripts/run_strands_swarm.py -c demo_configs/enterprise_sales_config.json \
    --customize sizing_agent \
    --prompt "Focus only on Delta Lake tables larger than 50GB"
```

---

## Choosing Between the Two CLIs

| Scenario | Which CLI? | Why? |
|----------|-----------|------|
| Production scheduled job | `run_strands_etl.py` | Deterministic, no LLM cost, predictable |
| Exploring a new dataset | `run_strands_swarm.py` | LLM provides flexible analysis |
| CI/CD pipeline | `run_strands_etl.py` | No external API dependencies |
| Ad-hoc sizing analysis | `run_strands_swarm.py --agent sizing_agent` | Targeted single-agent analysis |
| Cost-sensitive runs | `run_strands_etl.py` | No Bedrock charges |
| Complex debugging | `run_strands_swarm.py -v` | LLM can reason about failures |

---

## Configuration Reference

### Minimal Config

```json
{
    "job_name": "my_job",
    "script": {
        "path": "s3://bucket/scripts/my_job.py",
        "local_path": "scripts/pyspark/my_job.py",
        "type": "pyspark"
    },
    "platform": {
        "primary": "glue"
    },
    "smart_allocation": {
        "enabled": "Y",
        "force_from_config": "N"
    },
    "glue_config": {
        "worker_type": "G.2X",
        "number_of_workers": 10,
        "timeout_minutes": 120
    },
    "source_tables": [
        {
            "database": "my_db",
            "table": "my_table",
            "format": "parquet",
            "location": "s3://bucket/data/my_table/",
            "auto_detect_size": "Y"
        }
    ]
}
```

### Resource Allocation Config

```json
"smart_allocation": {
    "enabled": "Y",

    "force_from_config": "N",
    "_force_comment": "Y = use glue_config values exactly. N = agent decides",

    "consider_complexity": "Y",
    "complexity_factors": {
        "join_count_weight": 1.5,
        "window_function_weight": 2.0,
        "aggregation_weight": 1.2,
        "distinct_weight": 1.3
    },

    "weekend_scale_factor": 0.6,
    "month_end_scale_factor": 1.5,
    "quarter_end_scale_factor": 2.0
}
```

**Setting `force_from_config: "Y"`**: The resource allocator agent will use the exact `number_of_workers` and `worker_type` from `glue_config`. No scaling or complexity adjustments are applied. Use this when you know the exact resources needed.

**Setting `force_from_config: "N"`**: The agent calculates resources based on:
1. Detected data volume (from SizingAgent)
2. Day type (weekend/month-end/quarter-end scaling)
3. Script complexity (JOINs, window functions, aggregations)

### Platform Auto-Conversion Config

```json
"platform": {
    "primary": "glue",
    "fallback_chain": ["emr", "eks"],
    "force_platform": null,
    "auto_convert": {
        "enabled": "Y",
        "convert_to_emr_threshold_gb": 100,
        "convert_to_eks_threshold_gb": 500
    },
    "code_conversion": {
        "enabled": "Y",
        "convert_pyspark_code": "Y",
        "preserve_original": "Y",
        "output_converted_script_path": "converted/"
    }
}
```

- Set `force_platform` to `"emr"` or `"eks"` to override auto-detection
- Thresholds control when automatic platform conversion triggers
- When code conversion is enabled, GlueContext code is automatically rewritten to SparkSession

### Source Table Config

```json
{
    "_domain": "FINANCE",
    "database": "finance_db",
    "table": "transactions",
    "format": "parquet",
    "location": "s3://bucket/finance/transactions/",
    "partition_column": "transaction_date",
    "auto_detect_size": "Y",
    "broadcast": true,
    "estimated_size_gb": 50
}
```

- `auto_detect_size: "Y"` - Queries Glue Catalog `sizeInBytes`, falls back to S3 scan
- `broadcast: true` - Hint for broadcast joins (small reference tables)
- `estimated_size_gb` - Fallback when auto-detection is unavailable
- `partition_column` - Used for delta sizing (only count today's partition)

---

## What Each Agent Does

### Phase 1: Discovery (Parallel)

**SizingAgent** - Detects table sizes
- Queries Glue Catalog `sizeInBytes` parameter
- Falls back to S3 `list_objects_v2` scan
- Falls back to `estimated_size_gb` from config
- Outputs: `total_size_gb`, per-table sizes
- Stored in: `data/agent_store/sizing_agent/`

**ComplianceAgent** - Checks for PII and regulatory compliance
- Scans column names for PII patterns (SSN, email, phone, etc.)
- Checks encryption requirements
- Reports by compliance framework (GDPR, PCI-DSS, SOX, HIPAA)
- Stored in: `data/agent_store/compliance_agent/`

**CodeAnalysisAgent** - Analyzes PySpark script
- Counts JOINs, window functions, aggregations
- Detects anti-patterns (cartesian joins, collect())
- Feeds complexity data to ResourceAllocator
- Stored in: `data/agent_store/code_analysis_agent/`

### Phase 2: Planning (Depends on Phase 1)

**ResourceAllocatorAgent** - Recommends workers and type
- Uses sizing data + complexity + day-type
- Respects `force_from_config` flag
- Stored in: `data/agent_store/resource_allocator_agent/`

**DataQualityAgent** - Defines quality rules
- Supports natural language rules, SQL rules, template rules
- Generates quality score (0-100%)
- Stored in: `data/agent_store/data_quality_agent/`

**HealingAgent** - Prepares healing strategies
- Pre-defines fixes for memory, shuffle, timeout, skew errors
- Stored in: `data/agent_store/healing_agent/`

### Phase 3: Platform Decision

**PlatformConversionAgent** - Decides Glue vs EMR vs EKS
- Applies size thresholds (configurable)
- Generates target platform config (EMR cluster config or EKS SparkApplication CRD)
- Stored in: `data/agent_store/platform_conversion_agent/`

### Phase 4: Code Preparation

**CodeConversionAgent** - Converts PySpark code
- Rewrites GlueContext imports to SparkSession
- Converts DynamicFrame to DataFrame operations
- For EKS: adds Kubernetes Spark configs
- Saves converted script to `converted/` directory
- Stored in: `data/agent_store/code_conversion_agent/`

### Phase 5: Execution

**ExecutionAgent** - Runs the job
- Starts Glue job / EMR step / EKS SparkApplication
- Monitors progress (polls every 30s for Glue)
- Collects CloudWatch metrics
- Calculates cost
- In dry-run: simulates with realistic metrics
- Stored in: `data/agent_store/execution_agent/`

### Phase 6: Learning

**LearningAgent** - Trains ML models
- Collects execution record
- Trains 3 models after 5+ records: resource predictor, runtime estimator, cost predictor
- Tracks model IDs, versions, training costs
- Makes predictions for current run
- Stored in: `data/models/` (pickle + JSON registry)

**RecommendationAgent** - Aggregates recommendations
- Collects recommendations from all agents
- Prioritizes by impact (CRITICAL > HIGH > MEDIUM > LOW)
- Stored in: `data/agent_store/recommendation_agent/`

---

## Working with ML Models

### Query Trained Models

```bash
# List all models
python scripts/query_models.py --list

# Show model details
python scripts/query_models.py --model RES-6df065bb278e

# Show training costs
python scripts/query_models.py --costs

# Make a prediction
python scripts/query_models.py --predict --size-gb 200 --workers 20
```

### How Many Runs to Train Models

Models require a minimum of **5 execution records** to start training. Each run appends to `data/models/execution_history.json`.

```bash
# Run 5+ times to accumulate training data
for i in $(seq 1 5); do
    python scripts/run_strands_etl.py -c demo_configs/enterprise_sales_config.json
done

# Check model status
python scripts/query_models.py --list
```

After 5 runs you will see output like:
```
Trained 3 models:
  RES-abc123 (resource_predictor, R2=0.85)
  RUN-def456 (runtime_estimator, MAE=45.2s)
  COS-ghi789 (cost_predictor, MAPE=8.3%)
```

---

## Output and Audit Files

### Pipe-Delimited Files

All agent outputs are stored as `|`-delimited files in `data/agent_store/`:

```bash
# View sizing results
cat data/agent_store/sizing_agent/sizing_results_*.psv

# View audit logs
cat data/agent_store/audit_logs/audit_*.psv

# View resource allocations
cat data/agent_store/resource_allocator_agent/allocations_*.psv
```

Example audit log format:
```
timestamp|job_name|event_type|agent_name|status|details
2026-02-14T10:00:00|complex_enterprise_sales|agent_start|sizing_agent|running|...
2026-02-14T10:00:02|complex_enterprise_sales|agent_complete|sizing_agent|completed|...
```

### ML Model Files

```bash
data/models/
  model_registry.json      # Model metadata (IDs, versions, metrics)
  execution_history.json   # Training data (last 1000 records)
  RES-abc123.pkl          # Resource predictor model
  RUN-def456.pkl          # Runtime estimator model
  COS-ghi789.pkl          # Cost predictor model
```

---

## Common Tasks

### Force a Specific Platform

```bash
# Force EMR regardless of data size
python scripts/run_strands_etl.py -c config.json --platform emr
```

Or in config:
```json
"platform": {
    "force_platform": "emr"
}
```

### Force Specific Worker Configuration

Set in config:
```json
"smart_allocation": {
    "force_from_config": "Y"
},
"glue_config": {
    "worker_type": "G.4X",
    "number_of_workers": 40
}
```

This tells the resource allocator to use exactly 40 G.4X workers with no adjustments.

### Disable Specific Agents

In config:
```json
"agents": {
    "compliance_agent": {
        "enabled": "N"
    },
    "healing_agent": {
        "enabled": "N"
    }
}
```

### Add Custom Data Quality Rules

```json
"data_quality": {
    "natural_language_rules": [
        "customer_id should not be null",
        "premium_amount should be positive when present"
    ],
    "sql_rules": [
        {
            "id": "MY_CUSTOM_CHECK",
            "sql": "SELECT COUNT(*) FROM db.table WHERE col IS NULL",
            "threshold": 0,
            "severity": "critical"
        }
    ]
}
```

### Customize Agent Prompt (Strands SDK CLI only)

```bash
python scripts/run_strands_swarm.py -c config.json \
    --customize sizing_agent \
    --prompt "You are a sizing agent. Focus on Delta Lake tables only.
              Ignore tables smaller than 1GB. Report partition-level sizes."
```

Or programmatically:
```python
from strands_sdk.agents.prompts import set_prompt

set_prompt('sizing_agent', '''
You are a sizing agent specialized in Delta Lake tables.
Focus on Z-ordering and partition pruning.
Report sizes at partition granularity.
''')
```

---

## Troubleshooting

### "No module named 'strands'"
```bash
pip install strands-agents strands-agents-tools
```

### "No module named 'strands.swarm'"
This is expected. The `strands-agents` library does not include a `Swarm` class. Our custom `ETLSwarm` in `strands_sdk/swarm.py` handles orchestration.

### Models Not Training
- Need minimum 5 execution records
- Check `data/models/execution_history.json` for record count
- Ensure `learning.enabled: "Y"` in config

### Agent Shows "skipped"
- Check that the agent's feature flag is set to `"Y"` in config
- Example: `"smart_allocation": { "enabled": "Y" }`

### No AWS Credentials (Dry-Run Works, Execute Fails)
```bash
# Configure AWS credentials
aws configure
# Or set environment variables
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
```

### Platform Conversion Not Triggering
- Check `auto_convert.enabled: "Y"`
- Verify `total_size_gb` exceeds the threshold
- In dry-run mode, sizes are simulated (random 5-100 GB per table)

---

## File Structure Reference

```
strands_etl/
  scripts/
    run_strands_etl.py        # Rule-based CLI (no Bedrock)
    run_strands_swarm.py       # LLM-powered CLI (Bedrock)
    query_models.py            # Query trained ML models

  framework/strands/           # Rule-based agent framework
    base_agent.py              # Agent base class, registry, context
    orchestrator.py            # Parallel orchestrator (ThreadPoolExecutor)
    storage.py                 # Pipe-delimited storage backend
    agents/
      sizing_agent.py          # Table size detection
      resource_allocator_agent.py  # Worker/type allocation
      platform_conversion_agent.py # Glue->EMR->EKS conversion
      code_conversion_agent.py     # PySpark code rewriting
      compliance_agent.py          # PII and compliance checks
      data_quality_agent.py        # Quality rules engine
      execution_agent.py           # Job execution + metrics
      learning_agent.py            # ML model training
      healing_agent.py             # Auto-heal strategies
      recommendation_agent.py      # Recommendation aggregation
      code_analysis_agent.py       # Script complexity analysis

  strands_sdk/                 # Strands SDK integration
    config.py                  # ETLConfig dataclass
    swarm.py                   # ETLSwarm orchestrator (uses strands.Agent)
    agents/
      prompts.py               # System prompts per agent
      agent_factory.py         # Creates strands.Agent instances with tools
    tools/
      aws_tools.py             # @tool-decorated AWS functions
      storage_tools.py         # @tool-decorated storage functions

  demo_configs/
    enterprise_sales_config.json  # Full enterprise config (21 tables)
    simple_demo_config.json       # Minimal demo config

  data/
    agent_store/               # Pipe-delimited agent output files
    models/                    # Trained ML models (pickle + JSON)
```
