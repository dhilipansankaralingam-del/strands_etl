# Strands SDK ETL Agents

Complete end-to-end agent pipeline for ETL optimization using the **Strands Agents SDK**.

## Overview

This system uses 6 LLM-powered agents that work together to analyze, optimize, and optionally execute ETL jobs:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    STRANDS ETL AGENT PIPELINE                       │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐         │
│  │ SIZING   │   │   CODE   │   │COMPLIANCE│   │ LEARNING │         │
│  │  AGENT   │   │ ANALYSIS │   │  AGENT   │   │  AGENT   │         │
│  └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬─────┘         │
│       │              │              │              │                │
│       └──────────────┴──────────────┴──────────────┘                │
│                              │                                      │
│                     ┌────────▼────────┐                             │
│                     │   EXECUTION     │                             │
│                     │     AGENT       │                             │
│                     └────────┬────────┘                             │
│                              │                                      │
│                     ┌────────▼────────┐                             │
│                     │ RECOMMENDATION  │                             │
│                     │     AGENT       │                             │
│                     └─────────────────┘                             │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Requirements

```bash
# Install Strands SDK
pip install strands-agents boto3

# Set AWS credentials (us-west-2)
export AWS_REGION=us-west-2
export AWS_PROFILE=your-profile
```

## Quick Start

### 1. Create a Config File

```json
{
  "job_name": "sales_daily_etl",
  "script_path": "./jobs/sales_transform.py",
  "glue_job_name": "sales-daily-etl",
  
  "source_tables": [
    {
      "database": "sales_db",
      "table": "raw_orders",
      "location": "s3://my-bucket/raw/orders/"
    },
    {
      "database": "sales_db",
      "table": "customers",
      "location": "s3://my-bucket/raw/customers/",
      "estimated_rows": 5000000
    }
  ],
  
  "current_config": {
    "NumberOfWorkers": 10,
    "WorkerType": "G.1X",
    "GlueVersion": "4.0"
  },
  
  "monthly_runs": 30,
  "avg_runtime_hours": 0.5
}
```

### 2. Run Analysis

```bash
# Analyze only (no job execution)
python scripts/strands_etl_agents.py --config demo_configs/sales_etl.json

# Analyze multiple configs
python scripts/strands_etl_agents.py --source demo_configs/ --dest reports/

# Analyze AND execute the Glue job
python scripts/strands_etl_agents.py --config job.json --execute
```

## Agents (7 Total)

| # | Agent | Tools | Purpose |
|---|-------|-------|---------|
| 1 | SizingAgent | `scan_s3_location`, `get_glue_table_info` | Determine data sizes |
| 2 | CodeAnalysisAgent | `read_file` | Line-by-line code review |
| 3 | DataQualityAgent | `validate_data_quality_rule`, `run_athena_query` | Validate data rules |
| 4 | ComplianceAgent | - | Security & best practices |
| 5 | LearningAgent | `get_glue_job_runs`, `load_execution_history` | Learn from history |
| 6 | ExecutionAgent | `start_glue_job`, `get_job_run_status`, `store_execution_history` | Run/track jobs |
| 7 | RecommendationAgent | `calculate_platform_costs` | Prioritized recommendations |

### 1. Sizing Agent

**Purpose:** Determine actual data sizes by scanning S3 and Glue Catalog.

**Tools:**
- `scan_s3_location(s3_uri)` - Scans S3 path, returns file count and total size
- `get_glue_table_info(database, table)` - Gets table metadata from Glue Catalog

**Output:**
```
[TOOL] scan_s3_location: s3://bucket/data/
[TOOL] Found 1523 files, 12.5 GB
```

### 2. Code Analysis Agent

**Purpose:** Analyze PySpark code for anti-patterns and provide line-by-line improvements.

**Tools:**
- `read_file(path)` - Reads script file contents

**Detects:**
- `collect()` - OOM risk
- `toPandas()` - Memory intensive
- `crossJoin()` - Expensive cartesian
- UDFs without type hints
- Missing broadcasts for small tables

**Output:**
```
Line 45: [ANTI-PATTERN] collect() - Loads all data to driver
         [FIX] Use .write.parquet() instead of collecting

Line 78: [ANTI-PATTERN] for row in df.collect():
         [FIX] Use df.foreach() or foreachPartition()
```

### 3. Data Quality Agent

**Purpose:** Validate data against quality rules using Athena.

**Tools:**
- `validate_data_quality_rule(database, table, rule_type, column)` - Run validation
- `run_athena_query(sql)` - Execute custom SQL

**Rule Types:**
- `not_null` - Check for NULL values
- `unique` - Check for duplicates
- `positive` - Check for negative values
- `row_count` - Verify minimum row count
- `completeness` - Check data completeness percentage

**Output:**
```
[TOOL] validate_data_quality_rule: sales_db.orders - not_null
[TOOL] Query returned 1 rows
Result: PASS | Records: 1,523,456 | Outliers: 0

[TOOL] validate_data_quality_rule: sales_db.orders - completeness
Result: FAIL | Records: 1,523,456 | Outliers: 45,231 | Pass Rate: 97.03%
```

**Config:**
```json
{
  "data_quality": {
    "rules": [
      {"table": "sales_db.orders", "type": "not_null", "column": "order_id"},
      {"table": "sales_db.orders", "type": "completeness", "column": "customer_id", "threshold": 0.99}
    ]
  }
}
```

### 4. Compliance Agent

**Purpose:** Check security, cost controls, and best practices.

**Checks:**
- IAM roles and encryption settings
- Timeout configurations
- Worker limits
- Logging enabled
- VPC settings

### 5. Learning Agent

**Purpose:** Learn from historical job runs to identify patterns and anomalies.

**Tools:**
- `get_glue_job_runs(job_name)` - Fetches recent Glue runs
- `load_execution_history(job_name)` - Loads stored execution records

**Output:**
```
Historical Analysis (last 20 runs):
- Avg Duration: 23.5 minutes
- Avg Cost: $4.12
- Success Rate: 95%
- Trend: Duration increasing 15% over last month
```

### 6. Execution Agent

**Purpose:** Execute jobs (or simulate) and store results for learning.

**Tools:**
- `start_glue_job(job_name, dry_run=True)` - Start or simulate job
- `get_job_run_status(job_name, run_id)` - Monitor job progress
- `store_execution_history(job_name, data)` - Store for learning

**Modes:**
- **Analyze mode (default):** Simulates execution, estimates cost/duration
- **Execute mode (`--execute`):** Actually starts the Glue job

### 7. Recommendation Agent

**Purpose:** Synthesize all findings into prioritized recommendations.

**Tools:**
- `calculate_platform_costs(workers, runtime, runs)` - Compare platform costs

**Output:**
```
CRITICAL:
1. Remove collect() on line 45 - OOM risk

HIGH PRIORITY:
1. Reduce workers from 10 to 6 based on data size
2. Switch to G.2X for memory-bound operations

PLATFORM COMPARISON:
| Platform          | Monthly Cost | Savings |
|-------------------|--------------|---------|
| GCP Dataproc      | $245         | 45%     |
| AWS EMR           | $312         | 30%     |
| AWS Glue (current)| $445         | -       |
```

## Execution Flow

```
1. SIZING AGENT
   └─> Scans S3 locations and Glue tables
   └─> Determines actual data volumes

2. CODE ANALYSIS AGENT
   └─> Reads script file
   └─> Finds anti-patterns with line numbers
   └─> Suggests specific fixes

3. DATA QUALITY AGENT
   └─> Runs Athena queries to validate rules
   └─> Checks NULL, duplicates, completeness
   └─> Reports pass/fail with outlier counts

4. COMPLIANCE AGENT
   └─> Reviews configuration
   └─> Checks security settings
   └─> Validates best practices

5. LEARNING AGENT
   └─> Loads historical execution data
   └─> Computes statistics (avg duration, cost)
   └─> Identifies trends and anomalies

6. EXECUTION AGENT
   └─> Simulates or executes job
   └─> Monitors progress (if executing)
   └─> Stores results for future learning

7. RECOMMENDATION AGENT
   └─> Aggregates all findings
   └─> Prioritizes by impact (critical DQ issues first)
   └─> Compares platform costs
   └─> Generates implementation roadmap
```

## Learning Loop

The system learns from each execution:

```
                    ┌─────────────────┐
                    │   Execute Job   │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │  Store Results  │
                    │ (duration, cost,│
                    │  errors, etc.)  │
                    └────────┬────────┘
                             │
              ┌──────────────▼──────────────┐
              │     Execution History       │
              │  data/execution_history/    │
              │      {job_name}.jsonl       │
              └──────────────┬──────────────┘
                             │
                    ┌────────▼────────┐
                    │  Learning Agent │
                    │  loads history  │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │ Better Recs for │
                    │   next run      │
                    └─────────────────┘
```

## Token Usage

Each agent call shows token usage:

```
[4/6] LEARNING AGENT
    Creating agent...
    Prompt: 312 chars
    Executing...
      [TOOL] load_execution_history: sales_etl (limit=20)
      [TOOL] Loaded 18 records
      [TOOL] get_glue_job_runs: sales-daily-etl
      [TOOL] Found 10 runs, workers: 10
    Tokens: 1,456 in / 892 out | Time: 5.3s
```

Final summary:

```
======================================================================
  COMPLETE
  Total Tokens: 8,234 in / 5,678 out
  Duration: 45.2s
  Est Cost: $0.1124
======================================================================
```

## Output Files

Results are saved to the output directory:

```
reports/
├── sales_daily_etl_20240420_143052.json   # Full analysis
└── sales_daily_etl_20240420_143052.html   # (optional HTML report)
```

JSON structure:
```json
{
  "job_name": "sales_daily_etl",
  "agents": {
    "sizing": {"analysis": "..."},
    "code_analysis": {"script_path": "...", "analysis": "..."},
    "compliance": {"analysis": "..."},
    "learning": {"analysis": "..."},
    "execution": {"executed": false, "analysis": "..."},
    "recommendations": {"analysis": "..."}
  },
  "token_usage": {
    "total_input_tokens": 8234,
    "total_output_tokens": 5678,
    "estimated_cost_usd": 0.1124
  }
}
```

## CLI Reference

```
usage: strands_etl_agents.py [-h] [--config CONFIG] [--source SOURCE]
                             [--dest DEST] [--model MODEL] [--execute]

Options:
  --config, -c    Single config JSON file
  --source, -s    Directory with config JSONs (batch mode)
  --dest, -d      Output directory (default: ./reports)
  --model, -m     Bedrock model ID (default: claude-sonnet-4-6)
  --execute, -e   Actually execute the Glue job (requires confirmation)

Examples:
  # Analyze single config
  python scripts/strands_etl_agents.py -c job.json

  # Batch analyze
  python scripts/strands_etl_agents.py -s configs/ -d reports/

  # Run with execution
  python scripts/strands_etl_agents.py -c job.json --execute
```

## Troubleshooting

### S3 Access Denied
```
[TOOL] ERROR: An error occurred (AccessDenied)
```
**Fix:** Ensure AWS credentials have s3:ListBucket and s3:GetObject permissions.

### Glue Job Not Found
```
[TOOL] ERROR: Job not found: my-job
```
**Fix:** Verify `glue_job_name` in config matches actual Glue job name.

### No Historical Data
```
[TOOL] No history found
```
**Fix:** Run with `--execute` at least once to populate history, or seed with existing Glue runs.

### Token Limit Exceeded
```
Agent error: Token limit exceeded
```
**Fix:** Reduce source_tables count or use batch mode with smaller configs.
