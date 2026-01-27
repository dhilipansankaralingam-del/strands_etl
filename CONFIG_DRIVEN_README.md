# Config-Driven ETL Framework with Strands Agents

A fully **config-driven ETL orchestration framework** where all job definitions, data sources, quality rules, and platform preferences are defined in `etl_config.json`.

## 🎯 What Makes This Framework Special?

### 1. **Table Name Support with Auto-Detection**
Specify table names (not just S3 paths) and let agents automatically detect:
- Table size (queries AWS Glue Catalog, S3, Redshift, RDS, DynamoDB)
- Partition structure
- Schema details
- File counts and formats

### 2. **Data Flow Analysis**
Agents analyze how tables are used:
- Identify fact vs dimension tables
- Detect join relationships
- Recommend broadcast strategies
- Optimize join order
- Calculate expected shuffle size

### 3. **User Preference with Agent Recommendation**
- User can specify preferred platform (Glue, EMR, Lambda, Batch)
- Agent still analyzes and provides recommendation
- User can allow agent override or enforce strict preference
- Agent explains reasoning for recommendations

### 4. **Complete Config-Driven Workflow**
- Data quality rules from config
- Compliance requirements from config
- Cost budgets from config
- Platform preferences from config
- All agent behavior configurable

## 🗂️ Configuration Structure

### Basic Structure

```json
{
  "config_version": "2.0",
  "templates": { ... },
  "variables": { ... },
  "jobs": [ ... ],
  "global_settings": { ... }
}
```

### Job Definition

```json
{
  "job_id": "customer_order_summary",
  "job_name": "Customer Order Summary ETL",
  "enabled": true,

  "execution": {
    "script_path": "./pyscript/customer_order_summary_glue.py",
    "script_type": "pyspark",
    "schedule": "0 2 * * *"
  },

  "platform": {
    "user_preference": "glue",
    "allow_agent_override": true,
    "dpu_count": 10,
    "auto_adjust_dpu": true
  },

  "data_sources": [ ... ],
  "data_quality": { ... },
  "compliance": { ... },
  "cost": { ... }
}
```

## 📊 Data Source Types

### 1. AWS Glue Catalog Tables

```json
{
  "name": "transactions",
  "source_type": "glue_catalog",
  "database": "analytics_db",
  "table": "fact_transactions",
  "auto_detect_size": true,
  "auto_detect_partitions": true,
  "auto_detect_schema": true,
  "role_in_flow": "fact",
  "joins_with": ["customers", "products"],
  "join_keys": {
    "customers": "customer_id",
    "products": "product_id"
  },
  "broadcast_eligible": "auto"
}
```

**What happens:**
- Agent queries Glue Catalog to get table metadata
- Detects total size (GB), file count, partition count
- Analyzes partition structure
- Determines if broadcast-eligible (< 1 GB)
- Recommends role (fact or dimension based on size)

### 2. S3 Paths

```json
{
  "name": "customer_segments",
  "source_type": "s3",
  "path": "s3://my-data-bucket/dimensions/customer_segments/",
  "format": "parquet",
  "auto_detect_size": true,
  "role_in_flow": "dimension"
}
```

**What happens:**
- Agent calculates total size by listing S3 objects
- Counts files and calculates average file size
- Detects small files problem (> 1000 files with < 128 MB each)
- Recommends coalescing if needed

### 3. Redshift Tables

```json
{
  "name": "products",
  "source_type": "redshift",
  "cluster": "analytics-cluster",
  "database": "master_db",
  "schema": "public",
  "table": "products",
  "auto_detect_size": true
}
```

### 4. RDS Tables

```json
{
  "name": "stores",
  "source_type": "rds",
  "instance": "operational-db",
  "database": "retail",
  "table": "stores",
  "auto_detect_size": true
}
```

### 5. DynamoDB Tables

```json
{
  "name": "user_profiles",
  "source_type": "dynamodb",
  "table_name": "user-profiles-prod",
  "role_in_flow": "lookup",
  "cache_recommended": true,
  "cache_ttl_seconds": 300
}
```

### 6. Kinesis Streams (for streaming jobs)

```json
{
  "name": "clickstream_events",
  "source_type": "kinesis",
  "stream_name": "clickstream-events-prod",
  "role_in_flow": "streaming_source",
  "auto_detect_throughput": true,
  "batch_size": 100
}
```

## 🔄 Data Flow Analysis

Enable data flow analysis to get optimal join strategies:

```json
{
  "data_flow": {
    "analysis_enabled": true,
    "auto_detect_joins": true,
    "auto_detect_broadcast_candidates": true,
    "auto_optimize_join_order": true
  }
}
```

**Agent provides:**
- Fact vs dimension classification
- Join graph showing all relationships
- Broadcast recommendations with expected impact
- Optimal join order (smallest dimensions first)
- Estimated shuffle size
- Performance optimization suggestions

**Example output:**
```
Data Flow Analysis:
- Fact tables: transactions (350 GB)
- Dimension tables: customers (0.05 GB), products (0.1 GB)
- Broadcast recommendations:
  1. Broadcast customers (0.05 GB < 1 GB) → 30-50% faster
  2. Broadcast products (0.1 GB < 1 GB) → 30-50% faster
- Optimal join order:
  1. Load transactions (fact)
  2. Broadcast join customers (smallest)
  3. Broadcast join products
- Estimated shuffle: 700 GB → 200 GB (with broadcasts)
```

## 🤖 Platform Decision with Agent Recommendation

### Scenario 1: User Preference with Agent Recommendation

```json
{
  "platform": {
    "user_preference": "glue",
    "allow_agent_override": true,
    "dpu_count": 10
  }
}
```

**What happens:**
1. User prefers Glue
2. Agent analyzes workload (data volume, complexity, joins)
3. Agent provides recommendation with reasoning
4. Final decision uses user preference BUT shows agent recommendation

**Example:**
```
Platform Decision:
- User preference: glue
- Agent recommendation: emr
  Reason: "Data volume (500 GB) with 5+ table joins performs better on EMR"
- Final platform: glue (using user preference)
- Note: "Consider EMR for 30-40% better performance"
```

### Scenario 2: No User Preference (Auto-Select)

```json
{
  "platform": {
    "user_preference": null,
    "allow_agent_override": true,
    "auto_select_platform": true
  }
}
```

**What happens:**
1. Agent analyzes workload
2. Agent recommends platform with reasoning
3. Agent's recommendation is used

### Scenario 3: Strict User Preference (No Override)

```json
{
  "platform": {
    "user_preference": "glue",
    "allow_agent_override": false
  }
}
```

**What happens:**
1. User preference is enforced
2. Agent does NOT provide alternative recommendations
3. Job runs on specified platform

## ✅ Data Quality Rules from Config

Define all quality checks in config:

```json
{
  "data_quality": {
    "enabled": true,
    "fail_on_error": true,
    "completeness_checks": [
      {
        "field": "transaction_id",
        "required": true,
        "null_threshold_percent": 0,
        "severity": "critical",
        "action": "fail_job"
      }
    ],
    "accuracy_rules": [
      {
        "field": "email",
        "rule_type": "regex",
        "pattern": "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$",
        "severity": "high",
        "action": "quarantine"
      },
      {
        "field": "amount",
        "rule_type": "range",
        "min": 0,
        "max": 1000000,
        "severity": "high"
      }
    ],
    "duplicate_check": {
      "enabled": true,
      "key_fields": ["transaction_id"],
      "threshold_percent": 1,
      "action": "warn"
    },
    "referential_integrity": [
      {
        "child_table": "transactions",
        "child_key": "customer_id",
        "parent_table": "customers",
        "parent_key": "customer_id",
        "allow_orphans": false,
        "severity": "critical"
      }
    ]
  }
}
```

**Quality Agent uses these rules to:**
- Validate data completeness
- Check accuracy (regex, ranges, dates)
- Detect duplicates
- Verify referential integrity
- Generate quality score
- Fail job if critical issues found

## 🔒 Compliance from Config

```json
{
  "compliance": {
    "pii_fields": ["email", "phone", "customer_name"],
    "auto_detect_pii": true,
    "gdpr_applicable": true,
    "gdpr_config": {
      "consent_required": true,
      "right_to_deletion": true,
      "right_to_export": true
    },
    "hipaa_applicable": false,
    "data_classification": "confidential",
    "retention_days": 2555,
    "encryption_at_rest": true,
    "encryption_in_transit": true,
    "audit_logging": true
  }
}
```

**Compliance Agent:**
- Detects PII in data
- Verifies GDPR requirements
- Checks HIPAA compliance (if applicable)
- Generates audit reports
- Ensures encryption
- Tracks data lineage

## 💰 Cost Management

```json
{
  "cost": {
    "budget_per_run_usd": 25.0,
    "optimization_priority": "balanced",
    "cost_alerts": {
      "enabled": true,
      "threshold_percent": 90,
      "alert_channels": ["slack", "email"]
    },
    "spot_instances": {
      "enabled": true,
      "max_spot_percentage": 70
    }
  }
}
```

**Cost Tracking Agent:**
- Monitors actual vs budget
- Sends alerts at 90% budget
- Identifies optimization opportunities
- Forecasts monthly costs
- Recommends spot instances

## 🎨 Creative Features

### 1. Template Inheritance

Define reusable templates:

```json
{
  "templates": {
    "default_platform": {
      "allow_agent_override": true,
      "glue_version": "3.0",
      "python_version": "3"
    }
  },
  "jobs": [
    {
      "job_id": "my_job",
      "extends": "default_platform",
      ...
    }
  ]
}
```

### 2. Variable Substitution

```json
{
  "variables": {
    "data_bucket": "my-data-bucket",
    "environment": "${ENV:production}"
  },
  "jobs": [
    {
      "data_sources": [
        {
          "path": "s3://${variables.data_bucket}/raw/data/"
        }
      ]
    }
  ]
}
```

**Supports:**
- `${variables.key}` - Reference to variables section
- `${ENV:key}` - Environment variable
- `${ENV:key:default}` - Environment variable with default
- `${SECRET:key}` - Secret placeholder (resolved at runtime)

### 3. Conditional Logic (planned)

```json
{
  "if": {"environment": "production"},
  "then": {
    "dpu_count": 20
  },
  "else": {
    "dpu_count": 5
  }
}
```

### 4. Auto-Adjustment

```json
{
  "platform": {
    "auto_adjust_dpu": true,
    "min_dpu": 2,
    "max_dpu": 20
  },
  "workload": {
    "auto_calculate_volume": true
  }
}
```

### 5. Feature Flags

```json
{
  "global_settings": {
    "feature_flags": {
      "enable_auto_size_detection": true,
      "enable_data_flow_analysis": true,
      "enable_auto_optimization": true,
      "enable_predictive_scaling": true
    }
  }
}
```

## 🚀 Usage

### 1. Define Configuration

Create or edit `etl_config.json` with your jobs.

### 2. Load and Validate

```python
from strands_agents.orchestrator.config_loader import ConfigLoader

loader = ConfigLoader('./etl_config.json')
config = loader.load()

validation = loader.validate_config()
if validation['valid']:
    print("✓ Configuration is valid")
```

### 3. Run Job by ID

```python
from strands_agents.orchestrator.config_driven_orchestrator import ConfigDrivenOrchestrator

orchestrator = ConfigDrivenOrchestrator(
    config_path='./etl_config.json',
    enable_auto_detection=True
)

result = orchestrator.run_job_by_id('customer_order_summary')
```

### 4. Run All Enabled Jobs

```python
results = orchestrator.run_all_enabled_jobs()
```

## 📈 Execution Flow

```
1. Load Configuration
   └─ Parse etl_config.json
   └─ Apply variable substitution
   └─ Validate schema

2. Pre-Execution Analysis
   └─ Auto-detect table sizes (if enabled)
   └─ Analyze data flow relationships
   └─ Calculate total data volume

3. Platform Decision
   └─ Check user preference
   └─ Agent analyzes workload
   └─ Agent provides recommendation
   └─ Final decision (user or agent)

4. Quality Setup
   └─ Load quality rules from config
   └─ Quality Agent prepares validation plan

5. Job Execution
   └─ Submit job to platform (Glue/EMR/Lambda)
   └─ Concurrent script analysis
   └─ Monitor execution
   └─ Run quality checks

6. Post-Execution Analysis
   └─ Agent swarm analyzes results
   └─ Generate recommendations
   └─ Store learnings for next run
```

## 🔧 Tools for Auto-Detection

### Catalog Tools

```python
from strands_agents.tools.catalog_tools import (
    detect_glue_table_size,
    detect_s3_path_size,
    auto_detect_all_table_sizes,
    analyze_data_flow_relationships,
    recommend_platform_based_on_data_flow
)
```

**Tools available:**
1. `detect_glue_table_size` - Query Glue Catalog
2. `detect_s3_path_size` - Calculate S3 path size
3. `detect_redshift_table_size` - Query Redshift
4. `auto_detect_all_table_sizes` - Detect all sources
5. `analyze_data_flow_relationships` - Analyze joins
6. `recommend_platform_based_on_data_flow` - Platform recommendation

## 📊 Example: Multi-Table Job with Auto-Detection

```json
{
  "job_id": "multi_table_analytics",
  "job_name": "Multi-Table Analytics",
  "enabled": true,

  "platform": {
    "user_preference": null,
    "allow_agent_override": true,
    "auto_select_platform": true
  },

  "data_sources": [
    {
      "name": "orders",
      "source_type": "glue_catalog",
      "database": "sales_db",
      "table": "orders",
      "auto_detect_size": true,
      "role_in_flow": "fact",
      "joins_with": ["customers", "products", "stores"]
    },
    {
      "name": "customers",
      "source_type": "glue_catalog",
      "database": "master_db",
      "table": "customers",
      "auto_detect_size": true,
      "role_in_flow": "dimension"
    },
    {
      "name": "products",
      "source_type": "redshift",
      "cluster": "analytics-cluster",
      "database": "master_db",
      "table": "products",
      "auto_detect_size": true,
      "role_in_flow": "dimension"
    },
    {
      "name": "stores",
      "source_type": "rds",
      "instance": "operational-db",
      "database": "retail",
      "table": "stores",
      "auto_detect_size": true,
      "role_in_flow": "dimension"
    }
  ],

  "data_flow": {
    "analysis_enabled": true,
    "auto_detect_joins": true,
    "auto_optimize_join_order": true
  }
}
```

**What happens:**
1. Agent queries Glue Catalog for `orders` table size
2. Agent queries Glue Catalog for `customers` table size
3. Agent queries Redshift for `products` table size
4. Agent queries RDS for `stores` table size
5. Agent analyzes data flow and join relationships
6. Agent determines which tables should be broadcast
7. Agent recommends optimal join order
8. Agent selects best platform (Glue/EMR) based on total volume
9. Job executes with optimizations applied

## 🎯 Benefits

1. **Zero Hardcoding**: All job logic in config
2. **Auto-Detection**: Agents detect sizes automatically
3. **Intelligent Decisions**: Agents analyze and recommend
4. **User Control**: User can override agent decisions
5. **Quality Built-In**: Quality rules in config, enforced by agents
6. **Compliance Automated**: Compliance checks from config
7. **Cost-Aware**: Budget tracking and optimization
8. **Self-Learning**: Agents learn from history and improve

## 📚 Examples

See `example_config_driven_usage.py` for comprehensive examples:

```bash
python example_config_driven_usage.py
```

## 🔗 Integration

This config-driven framework integrates with:
- **ConcurrentETLOrchestrator**: Executes jobs with concurrent analysis
- **ETLSwarm**: Multi-agent coordination for decisions
- **All 6 Agents**: Decision, Quality, Optimization, Learning, Compliance, Cost Tracking

## 📄 Configuration Schema

See `etl_config.json` for complete schema with examples of:
- Glue Catalog tables
- S3 paths
- Redshift tables
- RDS tables
- DynamoDB tables
- Kinesis streams
- Data quality rules
- Compliance requirements
- Cost budgets
- Templates and variables

---

Built with ❤️ using [Strands Agents SDK](https://github.com/strands-agents/sdk-python)
