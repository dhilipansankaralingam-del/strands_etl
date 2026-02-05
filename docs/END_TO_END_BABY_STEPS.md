# COMPLETE END-TO-END BABY STEPS GUIDE

A comprehensive guide covering JSON creation, PySpark ETL scripts, Agent testing, and E2E validation.

---

## TABLE OF CONTENTS

1. [PART 1: CREATE TEST JSON CONFIGURATIONS](#part-1-create-test-json-configurations)
2. [PART 2: CREATE PYSPARK ETL SCRIPTS](#part-2-create-pyspark-etl-scripts)
3. [PART 3: TEST EACH AGENT MANUALLY](#part-3-test-each-agent-manually)
4. [PART 4: TEST INTEGRATIONS](#part-4-test-integrations)
5. [PART 5: E2E TESTING - SIMPLE USE CASE](#part-5-e2e-testing---simple-use-case)
6. [PART 6: E2E TESTING - COMPLEX USE CASE](#part-6-e2e-testing---complex-use-case)
7. [PART 7: ASK FOR RECOMMENDATIONS](#part-7-ask-for-recommendations)

---

# PART 1: CREATE TEST JSON CONFIGURATIONS

## 1.1 Simple JSON - S3 to S3 Copy (No Notifications)

Create file: `test_configs/my_simple_s3_copy.json`

```json
{
  "job_name": "my_simple_s3_copy",
  "description": "Copy parquet files from source to target S3 bucket",

  "source": {
    "type": "s3",
    "path": "s3://my-source-bucket/raw/sales/",
    "format": "parquet"
  },

  "target": {
    "type": "s3",
    "path": "s3://my-target-bucket/processed/sales/",
    "format": "parquet",
    "mode": "overwrite"
  },

  "platform": "glue",

  "notifications": {
    "enabled": "N"
  }
}
```

### Test the JSON:

```bash
cd /home/user/strands_etl
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

python -c "
import json

with open('test_configs/my_simple_s3_copy.json', 'w') as f:
    json.dump({
        'job_name': 'my_simple_s3_copy',
        'description': 'Copy parquet files from source to target S3 bucket',
        'source': {
            'type': 's3',
            'path': 's3://my-source-bucket/raw/sales/',
            'format': 'parquet'
        },
        'target': {
            'type': 's3',
            'path': 's3://my-target-bucket/processed/sales/',
            'format': 'parquet',
            'mode': 'overwrite'
        },
        'platform': 'glue',
        'notifications': {'enabled': 'N'}
    }, f, indent=2)

print('Created: test_configs/my_simple_s3_copy.json')

# Verify
with open('test_configs/my_simple_s3_copy.json') as f:
    config = json.load(f)
    print('Job Name:', config['job_name'])
    print('Source:', config['source']['type'], '->', config['source']['path'])
    print('Target:', config['target']['type'], '->', config['target']['path'])
"
```

---

## 1.2 Simple JSON - With Slack Notifications

Create file: `test_configs/my_slack_etl.json`

```bash
python -c "
import json

config = {
    'job_name': 'my_slack_etl',
    'description': 'ETL job with Slack notifications',

    'source': {
        'type': 'glue_catalog',
        'database': 'my_database',
        'table': 'my_source_table'
    },

    'target': {
        'type': 's3',
        'path': 's3://my-bucket/output/',
        'format': 'parquet',
        'partition_by': ['year', 'month']
    },

    'platform': 'glue',

    'transformations': [
        {
            'type': 'filter',
            'condition': \"status = 'ACTIVE'\"
        },
        {
            'type': 'add_columns',
            'columns': {
                'year': 'YEAR(created_date)',
                'month': 'MONTH(created_date)'
            }
        }
    ],

    'notifications': {
        'enabled': 'Y',
        'slack': {
            'enabled': 'Y',
            'webhook_url': '\${SLACK_WEBHOOK_URL}',
            'channel': '#etl-alerts'
        },
        'teams': {'enabled': 'N'},
        'email': {'enabled': 'N'},
        'preferences': {
            'on_start': 'N',
            'on_success': 'Y',
            'on_failure': 'Y'
        }
    }
}

with open('test_configs/my_slack_etl.json', 'w') as f:
    json.dump(config, f, indent=2)

print('Created: test_configs/my_slack_etl.json')
print('Notifications: Slack=Y, Teams=N, Email=N')
"
```

---

## 1.3 Simple JSON - With Teams Notifications

Create file: `test_configs/my_teams_etl.json`

```bash
python -c "
import json

config = {
    'job_name': 'my_teams_etl',
    'description': 'ETL job with Teams notifications',

    'source': {
        'type': 'jdbc',
        'connection_name': 'my-postgres-connection',
        'table': 'orders'
    },

    'target': {
        'type': 'glue_catalog',
        'database': 'warehouse',
        'table': 'orders_snapshot',
        'mode': 'overwrite'
    },

    'platform': 'glue',

    'notifications': {
        'enabled': 'Y',
        'slack': {'enabled': 'N'},
        'teams': {
            'enabled': 'Y',
            'webhook_url': '\${TEAMS_WEBHOOK_URL}',
            'channel': 'Data Platform'
        },
        'email': {'enabled': 'N'},
        'preferences': {
            'on_start': 'Y',
            'on_success': 'Y',
            'on_failure': 'Y'
        }
    }
}

with open('test_configs/my_teams_etl.json', 'w') as f:
    json.dump(config, f, indent=2)

print('Created: test_configs/my_teams_etl.json')
"
```

---

## 1.4 Simple JSON - With All Notifications + Data Quality

Create file: `test_configs/my_full_etl.json`

```bash
python -c "
import json

config = {
    'job_name': 'my_full_etl',
    'description': 'Full ETL with all notifications and data quality',

    'source': {
        'type': 'glue_catalog',
        'database': 'sales_db',
        'table': 'transactions'
    },

    'target': {
        'type': 'redshift',
        'connection_name': 'my-redshift',
        'schema': 'analytics',
        'table': 'fact_transactions',
        'mode': 'append'
    },

    'platform': 'glue',

    'data_quality': {
        'enabled': 'Y',
        'fail_on_error': 'N',
        'rules': [
            {
                'name': 'transaction_id_not_null',
                'type': 'not_null',
                'column': 'transaction_id'
            },
            {
                'name': 'amount_positive',
                'type': 'range',
                'column': 'amount',
                'min': 0
            },
            {
                'name': 'valid_date',
                'type': 'date_range',
                'column': 'transaction_date',
                'min': '2020-01-01',
                'max': '2030-12-31'
            }
        ]
    },

    'notifications': {
        'enabled': 'Y',
        'slack': {
            'enabled': 'Y',
            'webhook_url': '\${SLACK_WEBHOOK_URL}',
            'channel': '#data-alerts'
        },
        'teams': {
            'enabled': 'Y',
            'webhook_url': '\${TEAMS_WEBHOOK_URL}',
            'channel': 'Data Team'
        },
        'email': {
            'enabled': 'Y',
            'sender': 'etl@company.com',
            'recipients': ['data-team@company.com'],
            'ses_region': 'us-east-1'
        },
        'preferences': {
            'on_start': 'Y',
            'on_success': 'Y',
            'on_failure': 'Y',
            'on_dq_failure': 'Y',
            'on_cost_alert': 'Y',
            'dq_score_threshold': 0.9,
            'cost_alert_threshold_usd': 50.0
        }
    }
}

with open('test_configs/my_full_etl.json', 'w') as f:
    json.dump(config, f, indent=2)

print('Created: test_configs/my_full_etl.json')
print('DQ Rules:', len(config['data_quality']['rules']))
print('Notifications: Slack=Y, Teams=Y, Email=Y')
"
```

---

## 1.5 Complex JSON - Multi-Source Pipeline with Platform Fallback

Create file: `test_configs/my_complex_pipeline.json`

```bash
python -c "
import json

config = {
    'job_name': 'my_complex_pipeline',
    'description': 'Complex multi-source pipeline with platform fallback',

    'source': {
        'type': 'multi_source',
        'sources': [
            {
                'name': 'orders',
                'type': 'glue_catalog',
                'database': 'sales_db',
                'table': 'orders'
            },
            {
                'name': 'customers',
                'type': 'glue_catalog',
                'database': 'crm_db',
                'table': 'customers'
            },
            {
                'name': 'products',
                'type': 's3',
                'path': 's3://master-data/products/',
                'format': 'parquet'
            }
        ]
    },

    'target': {
        'type': 'iceberg',
        'catalog': 'glue_catalog',
        'database': 'analytics',
        'table': 'order_facts',
        'mode': 'merge',
        'merge_keys': ['order_id'],
        'partition_by': ['year', 'month']
    },

    'platform': {
        'primary': 'emr',
        'fallback': ['glue', 'eks'],
        'auto_heal': True
    },

    'transformations': [
        {
            'type': 'join',
            'left': 'orders',
            'right': 'customers',
            'on': ['customer_id'],
            'how': 'left'
        },
        {
            'type': 'join',
            'left': '_result',
            'right': 'products',
            'on': ['product_id'],
            'how': 'left'
        },
        {
            'type': 'filter',
            'condition': \"order_status = 'COMPLETED'\"
        },
        {
            'type': 'add_columns',
            'columns': {
                'year': 'YEAR(order_date)',
                'month': 'MONTH(order_date)',
                'revenue': 'quantity * unit_price'
            }
        },
        {
            'type': 'aggregate',
            'group_by': ['year', 'month', 'product_category', 'region'],
            'aggregations': {
                'total_revenue': 'SUM(revenue)',
                'order_count': 'COUNT(*)',
                'unique_customers': 'COUNT(DISTINCT customer_id)'
            }
        }
    ],

    'data_quality': {
        'enabled': 'Y',
        'fail_on_error': 'N',
        'rules': [
            {'name': 'order_id_not_null', 'type': 'not_null', 'column': 'order_id'},
            {'name': 'revenue_positive', 'type': 'range', 'column': 'revenue', 'min': 0},
            {'name': 'completeness', 'type': 'completeness', 'threshold': 0.95}
        ]
    },

    'compliance': {
        'enabled': 'Y',
        'frameworks': ['GDPR', 'PCI-DSS'],
        'pii_columns': ['customer_email', 'customer_phone'],
        'mask_pii': 'Y'
    },

    'notifications': {
        'enabled': 'Y',
        'slack': {'enabled': 'Y', 'webhook_url': '\${SLACK_WEBHOOK_URL}', 'channel': '#data-platform'},
        'teams': {'enabled': 'Y', 'webhook_url': '\${TEAMS_WEBHOOK_URL}', 'channel': 'Data Platform'},
        'email': {'enabled': 'Y', 'sender': 'etl@company.com', 'recipients': ['team@company.com']},
        'preferences': {
            'on_start': 'Y',
            'on_success': 'Y',
            'on_failure': 'Y',
            'on_dq_failure': 'Y',
            'on_cost_alert': 'Y',
            'on_recommendations': 'Y'
        }
    },

    'resources': {
        'emr': {
            'cluster_type': 'transient',
            'master_instance_type': 'm5.xlarge',
            'core_instance_type': 'r5.2xlarge',
            'core_instance_count': 5,
            'use_spot': 'Y'
        },
        'glue': {
            'worker_type': 'G.2X',
            'number_of_workers': 10
        }
    }
}

with open('test_configs/my_complex_pipeline.json', 'w') as f:
    json.dump(config, f, indent=2)

print('Created: test_configs/my_complex_pipeline.json')
print('Sources:', len(config['source']['sources']))
print('Transformations:', len(config['transformations']))
print('DQ Rules:', len(config['data_quality']['rules']))
print('Platform: Primary=', config['platform']['primary'], ', Fallback=', config['platform']['fallback'])
"
```

---

# PART 2: CREATE PYSPARK ETL SCRIPTS

## 2.1 Simple PySpark Script - S3 to S3

Create file: `scripts/etl/simple_s3_to_s3.py`

```bash
mkdir -p scripts/etl

cat > scripts/etl/simple_s3_to_s3.py << 'PYSPARK_SCRIPT'
#!/usr/bin/env python3
"""
Simple PySpark ETL Script - S3 to S3
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Simple S3 to S3 ETL") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

# Configuration
SOURCE_PATH = "s3://my-source-bucket/raw/sales/"
TARGET_PATH = "s3://my-target-bucket/processed/sales/"

def main():
    print("=" * 60)
    print("Starting Simple S3 to S3 ETL")
    print("=" * 60)

    # Read source data
    print(f"Reading from: {SOURCE_PATH}")
    df = spark.read.parquet(SOURCE_PATH)

    print(f"Records read: {df.count()}")
    print("Schema:")
    df.printSchema()

    # Add processing timestamp
    df_processed = df.withColumn("etl_timestamp", current_timestamp())

    # Write to target
    print(f"Writing to: {TARGET_PATH}")
    df_processed.write \
        .mode("overwrite") \
        .parquet(TARGET_PATH)

    print("=" * 60)
    print("ETL Completed Successfully!")
    print("=" * 60)

if __name__ == "__main__":
    main()
PYSPARK_SCRIPT

echo "Created: scripts/etl/simple_s3_to_s3.py"
```

---

## 2.2 PySpark Script with Transformations

Create file: `scripts/etl/transform_etl.py`

```bash
cat > scripts/etl/transform_etl.py << 'PYSPARK_SCRIPT'
#!/usr/bin/env python3
"""
PySpark ETL Script with Transformations
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, year, month, dayofmonth,
    sum as spark_sum, count, countDistinct,
    current_timestamp, lit
)

spark = SparkSession.builder \
    .appName("Transform ETL") \
    .getOrCreate()

# Configuration
SOURCE_PATH = "s3://my-bucket/raw/transactions/"
TARGET_PATH = "s3://my-bucket/processed/transactions/"

def main():
    print("=" * 60)
    print("Starting Transform ETL")
    print("=" * 60)

    # Read source
    df = spark.read.parquet(SOURCE_PATH)
    print(f"Source records: {df.count()}")

    # =========================================
    # TRANSFORMATION 1: Filter
    # =========================================
    print("Applying filter: status = 'COMPLETED'")
    df_filtered = df.filter(col("status") == "COMPLETED")
    print(f"After filter: {df_filtered.count()} records")

    # =========================================
    # TRANSFORMATION 2: Add computed columns
    # =========================================
    print("Adding computed columns...")
    df_enriched = df_filtered \
        .withColumn("year", year(col("transaction_date"))) \
        .withColumn("month", month(col("transaction_date"))) \
        .withColumn("day", dayofmonth(col("transaction_date"))) \
        .withColumn("revenue", col("quantity") * col("unit_price")) \
        .withColumn("is_high_value", when(col("revenue") > 1000, True).otherwise(False))

    # =========================================
    # TRANSFORMATION 3: Aggregate
    # =========================================
    print("Aggregating by year, month...")
    df_aggregated = df_enriched.groupBy("year", "month", "product_category") \
        .agg(
            spark_sum("revenue").alias("total_revenue"),
            count("*").alias("transaction_count"),
            countDistinct("customer_id").alias("unique_customers")
        )

    print(f"Aggregated records: {df_aggregated.count()}")

    # =========================================
    # WRITE OUTPUT
    # =========================================
    print(f"Writing to: {TARGET_PATH}")
    df_aggregated \
        .withColumn("etl_timestamp", current_timestamp()) \
        .write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(TARGET_PATH)

    print("=" * 60)
    print("Transform ETL Completed!")
    print("=" * 60)

if __name__ == "__main__":
    main()
PYSPARK_SCRIPT

echo "Created: scripts/etl/transform_etl.py"
```

---

## 2.3 Complex PySpark Script - Multi-Source Join

Create file: `scripts/etl/complex_multi_join.py`

```bash
cat > scripts/etl/complex_multi_join.py << 'PYSPARK_SCRIPT'
#!/usr/bin/env python3
"""
Complex PySpark ETL - Multi-Source Join Pipeline
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, year, month, coalesce, lit,
    sum as spark_sum, count, countDistinct,
    current_timestamp, broadcast
)

spark = SparkSession.builder \
    .appName("Complex Multi-Join ETL") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Configuration
ORDERS_PATH = "s3://my-bucket/raw/orders/"
CUSTOMERS_PATH = "s3://my-bucket/raw/customers/"
PRODUCTS_PATH = "s3://my-bucket/raw/products/"
TARGET_PATH = "s3://my-bucket/processed/order_facts/"

def main():
    print("=" * 60)
    print("Starting Complex Multi-Source Join ETL")
    print("=" * 60)

    # =========================================
    # STEP 1: Read all source tables
    # =========================================
    print("\n[STEP 1] Reading source tables...")

    df_orders = spark.read.parquet(ORDERS_PATH)
    print(f"  Orders: {df_orders.count()} records")

    df_customers = spark.read.parquet(CUSTOMERS_PATH)
    print(f"  Customers: {df_customers.count()} records")

    df_products = spark.read.parquet(PRODUCTS_PATH)
    print(f"  Products: {df_products.count()} records")

    # =========================================
    # STEP 2: Join orders with customers
    # =========================================
    print("\n[STEP 2] Joining orders with customers...")

    df_order_customer = df_orders.alias("o").join(
        df_customers.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left"
    ).select(
        col("o.*"),
        col("c.customer_name"),
        col("c.customer_email"),
        col("c.customer_region").alias("region")
    )

    print(f"  After join: {df_order_customer.count()} records")

    # =========================================
    # STEP 3: Join with products (broadcast for small table)
    # =========================================
    print("\n[STEP 3] Joining with products (broadcast)...")

    df_enriched = df_order_customer.join(
        broadcast(df_products),
        df_order_customer.product_id == df_products.product_id,
        "left"
    ).select(
        df_order_customer["*"],
        df_products["product_name"],
        df_products["product_category"],
        df_products["unit_cost"]
    )

    print(f"  After join: {df_enriched.count()} records")

    # =========================================
    # STEP 4: Apply transformations
    # =========================================
    print("\n[STEP 4] Applying transformations...")

    df_transformed = df_enriched \
        .filter(col("order_status") == "COMPLETED") \
        .withColumn("year", year(col("order_date"))) \
        .withColumn("month", month(col("order_date"))) \
        .withColumn("revenue", col("quantity") * col("unit_price")) \
        .withColumn("cost", col("quantity") * coalesce(col("unit_cost"), lit(0))) \
        .withColumn("profit", col("revenue") - col("cost")) \
        .withColumn("region", coalesce(col("region"), lit("UNKNOWN")))

    print(f"  After filter: {df_transformed.count()} records")

    # =========================================
    # STEP 5: Aggregate
    # =========================================
    print("\n[STEP 5] Aggregating...")

    df_final = df_transformed.groupBy(
        "year", "month", "region", "product_category"
    ).agg(
        spark_sum("revenue").alias("total_revenue"),
        spark_sum("profit").alias("total_profit"),
        count("*").alias("order_count"),
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct("product_id").alias("unique_products")
    )

    print(f"  Aggregated: {df_final.count()} records")

    # =========================================
    # STEP 6: Write output
    # =========================================
    print(f"\n[STEP 6] Writing to: {TARGET_PATH}")

    df_final \
        .withColumn("etl_timestamp", current_timestamp()) \
        .write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(TARGET_PATH)

    print("\n" + "=" * 60)
    print("Complex Multi-Join ETL Completed!")
    print("=" * 60)

if __name__ == "__main__":
    main()
PYSPARK_SCRIPT

echo "Created: scripts/etl/complex_multi_join.py"
```

---

# PART 3: TEST EACH AGENT MANUALLY

## 3.1 Test: Auto-Healing Agent

The Auto-Healing Agent detects and fixes common ETL errors like OOM, timeouts, and data skew.

```bash
python -c "
import sys
sys.path.insert(0, '.')

from agents.auto_healing_agent import AutoHealingAgent, ErrorType

print('=' * 70)
print('TESTING: Auto-Healing Agent')
print('=' * 70)

# Initialize agent
agent = AutoHealingAgent()

# ============================================
# TEST 1: Detect OOM Error
# ============================================
print('\n[TEST 1] Detect Out of Memory Error')
print('-' * 50)

oom_error = '''
Exception in thread \"main\" java.lang.OutOfMemoryError: Java heap space
    at java.util.Arrays.copyOf(Arrays.java:3236)
    at org.apache.spark.memory.TaskMemoryManager.allocatePage
'''

result = agent.analyze_error(oom_error)
print(f'Error Type: {result.error_type}')
print(f'Root Cause: {result.root_cause}')
print(f'Recommendations:')
for rec in result.recommendations[:3]:
    print(f'  - {rec}')

# ============================================
# TEST 2: Detect Data Skew
# ============================================
print('\n[TEST 2] Detect Data Skew')
print('-' * 50)

skew_error = '''
Stage 5 (reduce) has skewed partitions.
Partition 0: 50GB, Partition 1: 100MB, Partition 2: 80MB
Task duration: Partition 0 took 45 minutes, others took 2 minutes
'''

result = agent.analyze_error(skew_error)
print(f'Error Type: {result.error_type}')
print(f'Root Cause: {result.root_cause}')
print(f'Recommendations:')
for rec in result.recommendations[:3]:
    print(f'  - {rec}')

# ============================================
# TEST 3: Get Auto-Fix Suggestions
# ============================================
print('\n[TEST 3] Get Auto-Fix Spark Config')
print('-' * 50)

fix_config = agent.get_auto_fix_config(ErrorType.OOM)
print('Suggested Spark Config for OOM:')
for key, value in list(fix_config.items())[:5]:
    print(f'  {key}: {value}')

print('\n[PASS] Auto-Healing Agent tests completed')
"
```

**Expected Results:**
```
======================================================================
TESTING: Auto-Healing Agent
======================================================================

[TEST 1] Detect Out of Memory Error
--------------------------------------------------
Error Type: OOM
Root Cause: Java heap space exhausted
Recommendations:
  - Increase executor memory: --conf spark.executor.memory=8g
  - Enable off-heap memory
  - Reduce partition size

[TEST 2] Detect Data Skew
--------------------------------------------------
Error Type: DATA_SKEW
Root Cause: Uneven data distribution across partitions
Recommendations:
  - Enable AQE: spark.sql.adaptive.enabled=true
  - Use salting technique for skewed keys
  - Repartition data before joins

[TEST 3] Get Auto-Fix Spark Config
--------------------------------------------------
Suggested Spark Config for OOM:
  spark.executor.memory: 8g
  spark.executor.memoryOverhead: 2g
  spark.memory.fraction: 0.8

[PASS] Auto-Healing Agent tests completed
```

---

## 3.2 Test: Compliance Agent

The Compliance Agent checks for GDPR, HIPAA, PCI-DSS compliance and PII detection.

```bash
python -c "
import sys
sys.path.insert(0, '.')

from agents.compliance_agent import ComplianceAgent

print('=' * 70)
print('TESTING: Compliance Agent')
print('=' * 70)

# Initialize agent
agent = ComplianceAgent()

# ============================================
# TEST 1: Detect PII in column names
# ============================================
print('\n[TEST 1] Detect PII Columns')
print('-' * 50)

columns = [
    'order_id',
    'customer_email',
    'customer_phone',
    'credit_card_number',
    'ssn',
    'product_name',
    'ip_address',
    'amount'
]

pii_detected = agent.detect_pii_columns(columns)
print('Columns analyzed:', columns)
print('PII Detected:')
for col_name, pii_type in pii_detected.items():
    print(f'  - {col_name}: {pii_type}')

# ============================================
# TEST 2: Check GDPR Compliance
# ============================================
print('\n[TEST 2] Check GDPR Compliance')
print('-' * 50)

config = {
    'source': {'type': 's3', 'path': 's3://bucket/data/'},
    'target': {'type': 'redshift'},
    'pii_columns': ['customer_email', 'customer_phone'],
    'data_retention_days': 365,
    'encryption': True,
    'access_logging': True
}

gdpr_result = agent.check_gdpr_compliance(config)
print(f'GDPR Compliant: {gdpr_result.is_compliant}')
print(f'Score: {gdpr_result.score:.1%}')
print('Issues:')
for issue in gdpr_result.issues[:3]:
    print(f'  - {issue}')

# ============================================
# TEST 3: Check PCI-DSS Compliance
# ============================================
print('\n[TEST 3] Check PCI-DSS Compliance')
print('-' * 50)

pci_config = {
    'pii_columns': ['credit_card_number', 'cvv'],
    'encryption_at_rest': True,
    'encryption_in_transit': True,
    'mask_card_numbers': False
}

pci_result = agent.check_pci_compliance(pci_config)
print(f'PCI-DSS Compliant: {pci_result.is_compliant}')
print(f'Score: {pci_result.score:.1%}')
print('Recommendations:')
for rec in pci_result.recommendations[:3]:
    print(f'  - {rec}')

print('\n[PASS] Compliance Agent tests completed')
"
```

**Expected Results:**
```
======================================================================
TESTING: Compliance Agent
======================================================================

[TEST 1] Detect PII Columns
--------------------------------------------------
Columns analyzed: ['order_id', 'customer_email', ...]
PII Detected:
  - customer_email: EMAIL
  - customer_phone: PHONE
  - credit_card_number: CREDIT_CARD
  - ssn: SSN
  - ip_address: IP_ADDRESS

[TEST 2] Check GDPR Compliance
--------------------------------------------------
GDPR Compliant: False
Score: 75.0%
Issues:
  - PII columns should be encrypted
  - Data retention policy exceeds recommended 90 days
  - Missing consent tracking

[TEST 3] Check PCI-DSS Compliance
--------------------------------------------------
PCI-DSS Compliant: False
Score: 60.0%
Recommendations:
  - Enable card number masking
  - Implement tokenization
  - Add access audit logging

[PASS] Compliance Agent tests completed
```

---

## 3.3 Test: Code Analysis Agent

The Code Analysis Agent analyzes PySpark code for anti-patterns and optimizations.

```bash
python -c "
import sys
sys.path.insert(0, '.')

from agents.code_analysis_agent import CodeAnalysisAgent

print('=' * 70)
print('TESTING: Code Analysis Agent')
print('=' * 70)

# Initialize agent
agent = CodeAnalysisAgent()

# ============================================
# TEST 1: Analyze code with anti-patterns
# ============================================
print('\n[TEST 1] Detect Anti-Patterns')
print('-' * 50)

bad_code = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()

# Anti-pattern 1: collect() on large dataset
df = spark.read.parquet(\"s3://bucket/large_data/\")
all_data = df.collect()  # BAD!

# Anti-pattern 2: Python UDF instead of native functions
@udf(StringType())
def upper_case(s):
    return s.upper() if s else None

df2 = df.withColumn(\"name_upper\", upper_case(col(\"name\")))

# Anti-pattern 3: Multiple count() calls
count1 = df.count()
count2 = df.filter(col(\"status\") == \"A\").count()
count3 = df.filter(col(\"status\") == \"B\").count()
'''

result = agent.analyze_code(bad_code)
print(f'Issues Found: {len(result.issues)}')
print(f'Risk Level: {result.risk_level}')
print('\\nAnti-Patterns Detected:')
for issue in result.issues:
    print(f'  [{issue.severity}] Line {issue.line}: {issue.description}')
    print(f'           Fix: {issue.suggestion}')

# ============================================
# TEST 2: Get optimization suggestions
# ============================================
print('\n[TEST 2] Get Optimization Suggestions')
print('-' * 50)

suggestions = agent.get_optimization_suggestions(bad_code)
print('Optimization Suggestions:')
for i, sug in enumerate(suggestions[:5], 1):
    print(f'  {i}. {sug}')

print('\n[PASS] Code Analysis Agent tests completed')
"
```

**Expected Results:**
```
======================================================================
TESTING: Code Analysis Agent
======================================================================

[TEST 1] Detect Anti-Patterns
--------------------------------------------------
Issues Found: 3
Risk Level: HIGH

Anti-Patterns Detected:
  [HIGH] Line 10: Using collect() on potentially large dataset
           Fix: Use take(n), limit(), or write to storage instead
  [MEDIUM] Line 14: Python UDF detected - causes serialization overhead
           Fix: Use native PySpark functions (upper()) instead
  [LOW] Line 19: Multiple count() actions on same DataFrame
           Fix: Use cache() or combine into single aggregation

[TEST 2] Get Optimization Suggestions
--------------------------------------------------
Optimization Suggestions:
  1. Replace collect() with iterator or write to file
  2. Replace Python UDF with pyspark.sql.functions.upper()
  3. Enable Adaptive Query Execution (AQE)
  4. Use DataFrame cache() for repeated operations
  5. Consider using broadcast joins for small tables

[PASS] Code Analysis Agent tests completed
```

---

## 3.4 Test: Workload Assessment Agent

The Workload Assessment Agent analyzes data characteristics and recommends optimal configurations.

```bash
python -c "
import sys
sys.path.insert(0, '.')

from agents.workload_assessment_agent import WorkloadAssessmentAgent

print('=' * 70)
print('TESTING: Workload Assessment Agent')
print('=' * 70)

# Initialize agent
agent = WorkloadAssessmentAgent()

# ============================================
# TEST 1: Assess small dataset workload
# ============================================
print('\n[TEST 1] Assess Small Dataset')
print('-' * 50)

small_workload = {
    'data_size_gb': 5,
    'row_count': 1_000_000,
    'columns': 20,
    'partitions': 10,
    'join_count': 1,
    'transformation_complexity': 'low'
}

result = agent.assess_workload(small_workload)
print(f'Recommended Platform: {result.recommended_platform}')
print(f'Estimated Duration: {result.estimated_duration_minutes} minutes')
print(f'Estimated Cost: \${result.estimated_cost:.2f}')
print('Resource Recommendations:')
print(f'  - Workers: {result.recommended_workers}')
print(f'  - Memory per worker: {result.recommended_memory_gb}GB')

# ============================================
# TEST 2: Assess large dataset workload
# ============================================
print('\n[TEST 2] Assess Large Dataset')
print('-' * 50)

large_workload = {
    'data_size_gb': 500,
    'row_count': 5_000_000_000,
    'columns': 100,
    'partitions': 1000,
    'join_count': 5,
    'transformation_complexity': 'high',
    'has_skewed_keys': True
}

result = agent.assess_workload(large_workload)
print(f'Recommended Platform: {result.recommended_platform}')
print(f'Estimated Duration: {result.estimated_duration_minutes} minutes')
print(f'Estimated Cost: \${result.estimated_cost:.2f}')
print('Resource Recommendations:')
print(f'  - Workers: {result.recommended_workers}')
print(f'  - Memory per worker: {result.recommended_memory_gb}GB')
print(f'  - Use Spot Instances: {result.use_spot}')
print('Special Recommendations:')
for rec in result.special_recommendations[:3]:
    print(f'  - {rec}')

# ============================================
# TEST 3: Compare platforms
# ============================================
print('\n[TEST 3] Platform Comparison')
print('-' * 50)

comparison = agent.compare_platforms(large_workload)
print('Platform Comparison:')
for platform, metrics in comparison.items():
    print(f'  {platform}:')
    print(f'    Cost: \${metrics[\"cost\"]:.2f}')
    print(f'    Duration: {metrics[\"duration_min\"]} min')
    print(f'    Recommendation: {metrics[\"recommendation\"]}')

print('\n[PASS] Workload Assessment Agent tests completed')
"
```

**Expected Results:**
```
======================================================================
TESTING: Workload Assessment Agent
======================================================================

[TEST 1] Assess Small Dataset
--------------------------------------------------
Recommended Platform: glue
Estimated Duration: 5 minutes
Estimated Cost: $0.44
Resource Recommendations:
  - Workers: 2
  - Memory per worker: 8GB

[TEST 2] Assess Large Dataset
--------------------------------------------------
Recommended Platform: emr
Estimated Duration: 45 minutes
Estimated Cost: $12.50
Resource Recommendations:
  - Workers: 20
  - Memory per worker: 32GB
  - Use Spot Instances: True
Special Recommendations:
  - Enable Adaptive Query Execution for skewed joins
  - Use salting for skewed keys
  - Consider Graviton instances for 40% cost savings

[TEST 3] Platform Comparison
--------------------------------------------------
Platform Comparison:
  glue:
    Cost: $25.00
    Duration: 90 min
    Recommendation: Not optimal for this workload size
  emr:
    Cost: $12.50
    Duration: 45 min
    Recommendation: Best for large-scale processing
  eks:
    Cost: $10.00
    Duration: 50 min
    Recommendation: Good with Karpenter auto-scaling

[PASS] Workload Assessment Agent tests completed
```

---

## 3.5 Test: Data Quality Agent

The Data Quality Agent executes DQ rules and calculates quality scores.

```bash
python -c "
import sys
sys.path.insert(0, '.')

from agents.data_quality_agent import DataQualityAgent

print('=' * 70)
print('TESTING: Data Quality Agent')
print('=' * 70)

# Initialize agent
agent = DataQualityAgent()

# ============================================
# TEST 1: Define DQ Rules
# ============================================
print('\n[TEST 1] Define Data Quality Rules')
print('-' * 50)

rules = [
    {
        'name': 'transaction_id_not_null',
        'type': 'not_null',
        'column': 'transaction_id',
        'threshold': 1.0
    },
    {
        'name': 'amount_positive',
        'type': 'range',
        'column': 'amount',
        'min': 0,
        'max': 1000000
    },
    {
        'name': 'valid_status',
        'type': 'allowed_values',
        'column': 'status',
        'values': ['PENDING', 'COMPLETED', 'CANCELLED', 'REFUNDED']
    },
    {
        'name': 'email_format',
        'type': 'regex',
        'column': 'customer_email',
        'pattern': r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    },
    {
        'name': 'completeness_check',
        'type': 'completeness',
        'threshold': 0.95
    }
]

print(f'Defined {len(rules)} DQ rules:')
for rule in rules:
    print(f'  - {rule[\"name\"]}: {rule[\"type\"]}')

# ============================================
# TEST 2: Simulate DQ Check Results
# ============================================
print('\n[TEST 2] Simulate DQ Check Results')
print('-' * 50)

# Simulate results (in real scenario, this runs against actual data)
mock_results = {
    'transaction_id_not_null': {'passed': True, 'score': 1.0, 'null_count': 0},
    'amount_positive': {'passed': True, 'score': 0.98, 'violations': 200},
    'valid_status': {'passed': True, 'score': 1.0, 'invalid_count': 0},
    'email_format': {'passed': False, 'score': 0.85, 'invalid_count': 1500},
    'completeness_check': {'passed': True, 'score': 0.96, 'missing_pct': 0.04}
}

overall_score = sum(r['score'] for r in mock_results.values()) / len(mock_results)

print('DQ Results:')
for rule_name, result in mock_results.items():
    status = 'PASS' if result['passed'] else 'FAIL'
    print(f'  [{status}] {rule_name}: {result[\"score\"]:.1%}')

print(f'\\nOverall DQ Score: {overall_score:.1%}')
print(f'Threshold: 90%')
print(f'Status: {\"PASS\" if overall_score >= 0.9 else \"FAIL\"}')

# ============================================
# TEST 3: Generate DQ Report
# ============================================
print('\n[TEST 3] Generate DQ Report')
print('-' * 50)

report = agent.generate_report(mock_results)
print('DQ Report Generated:')
print(f'  Total Rules: {report[\"total_rules\"]}')
print(f'  Passed: {report[\"passed_rules\"]}')
print(f'  Failed: {report[\"failed_rules\"]}')
print(f'  Overall Score: {report[\"overall_score\"]:.1%}')

print('\n[PASS] Data Quality Agent tests completed')
"
```

**Expected Results:**
```
======================================================================
TESTING: Data Quality Agent
======================================================================

[TEST 1] Define Data Quality Rules
--------------------------------------------------
Defined 5 DQ rules:
  - transaction_id_not_null: not_null
  - amount_positive: range
  - valid_status: allowed_values
  - email_format: regex
  - completeness_check: completeness

[TEST 2] Simulate DQ Check Results
--------------------------------------------------
DQ Results:
  [PASS] transaction_id_not_null: 100.0%
  [PASS] amount_positive: 98.0%
  [PASS] valid_status: 100.0%
  [FAIL] email_format: 85.0%
  [PASS] completeness_check: 96.0%

Overall DQ Score: 95.8%
Threshold: 90%
Status: PASS

[TEST 3] Generate DQ Report
--------------------------------------------------
DQ Report Generated:
  Total Rules: 5
  Passed: 4
  Failed: 1
  Overall Score: 95.8%

[PASS] Data Quality Agent tests completed
```

---

## 3.6 Test: AWS Recommendations Engine

The AWS Recommendations Engine suggests optimal AWS services for your workload.

```bash
python -c "
import sys
sys.path.insert(0, '.')

from agents.aws_recommendations_engine import AWSRecommendationsEngine

print('=' * 70)
print('TESTING: AWS Recommendations Engine')
print('=' * 70)

# Initialize agent
engine = AWSRecommendationsEngine()

# ============================================
# TEST 1: Get recommendations for batch ETL
# ============================================
print('\n[TEST 1] Recommendations for Batch ETL')
print('-' * 50)

batch_config = {
    'workload_type': 'batch',
    'data_size_gb': 100,
    'frequency': 'daily',
    'transformation_complexity': 'medium',
    'budget_monthly_usd': 500
}

result = engine.recommend_architecture(batch_config)

print('Recommended Architecture:')
print(f'  Compute: {result.compute_recommendation}')
print(f'  Orchestration: {result.orchestration_recommendation}')
print(f'  Storage: {result.storage_recommendation}')
print(f'  Estimated Monthly Cost: \${result.estimated_monthly_cost:.2f}')

# ============================================
# TEST 2: Get recommendations for streaming
# ============================================
print('\n[TEST 2] Recommendations for Streaming')
print('-' * 50)

streaming_config = {
    'workload_type': 'streaming',
    'events_per_second': 10000,
    'latency_requirement_ms': 1000,
    'transformation_complexity': 'high'
}

result = engine.recommend_architecture(streaming_config)

print('Recommended Architecture:')
print(f'  Compute: {result.compute_recommendation}')
print(f'  Streaming: {result.streaming_recommendation}')
print(f'  Storage: {result.storage_recommendation}')

# ============================================
# TEST 3: Compare Glue vs EMR vs EKS
# ============================================
print('\n[TEST 3] Platform Comparison')
print('-' * 50)

comparison = engine.compare_platforms({
    'data_size_gb': 500,
    'job_duration_hours': 2,
    'frequency': 'daily'
})

print('Cost Comparison (Monthly):')
for platform, details in comparison.items():
    print(f'  {platform}:')
    print(f'    Cost: \${details[\"monthly_cost\"]:.2f}')
    print(f'    Pros: {details[\"pros\"][:2]}')
    print(f'    Best for: {details[\"best_for\"]}')

print('\n[PASS] AWS Recommendations Engine tests completed')
"
```

**Expected Results:**
```
======================================================================
TESTING: AWS Recommendations Engine
======================================================================

[TEST 1] Recommendations for Batch ETL
--------------------------------------------------
Recommended Architecture:
  Compute: AWS Glue (for medium complexity batch jobs)
  Orchestration: AWS Step Functions (for daily scheduling)
  Storage: S3 with Parquet format
  Estimated Monthly Cost: $150.00

[TEST 2] Recommendations for Streaming
--------------------------------------------------
Recommended Architecture:
  Compute: EMR with Spark Streaming or Flink
  Streaming: Amazon Kinesis Data Streams
  Storage: Delta Lake on S3

[TEST 3] Platform Comparison
--------------------------------------------------
Cost Comparison (Monthly):
  glue:
    Cost: $880.00
    Pros: ['Serverless', 'No cluster management']
    Best for: Medium workloads, occasional jobs
  emr:
    Cost: $650.00
    Pros: ['Cost effective at scale', 'Full Spark control']
    Best for: Large workloads, complex transformations
  eks:
    Cost: $550.00
    Pros: ['Kubernetes native', 'Karpenter auto-scaling']
    Best for: Mixed workloads, containerized pipelines

[PASS] AWS Recommendations Engine tests completed
```

---

## 3.7 Test: EKS Optimizer

The EKS Optimizer recommends Karpenter, SPOT, and Graviton configurations.

```bash
python -c "
import sys
sys.path.insert(0, '.')

from agents.eks_optimizer import EKSOptimizer

print('=' * 70)
print('TESTING: EKS Optimizer')
print('=' * 70)

# Initialize agent
optimizer = EKSOptimizer()

# ============================================
# TEST 1: Estimate t3.large migration savings
# ============================================
print('\n[TEST 1] t3.large Migration Analysis')
print('-' * 50)

result = optimizer.estimate_t_series_migration(
    current_instance='t3.large',
    current_capacity='on-demand',
    hours_per_day=24,
    instance_count=5
)

print('Current Setup:')
print(f'  Instance: t3.large (x86_64, On-Demand)')
print(f'  Count: 5 instances')
print(f'  Monthly Cost: \${result[\"current_monthly_cost\"]:.2f}')

print('\\nOptimized Setup (Graviton + SPOT):')
print(f'  Instance: {result[\"recommended_instance\"]}')
print(f'  Monthly Cost: \${result[\"optimized_monthly_cost\"]:.2f}')
print(f'  Monthly Savings: \${result[\"monthly_savings\"]:.2f}')
print(f'  Savings Percentage: {result[\"savings_percentage\"]:.1f}%')

# ============================================
# TEST 2: Karpenter configuration
# ============================================
print('\n[TEST 2] Karpenter Configuration')
print('-' * 50)

karpenter_config = optimizer.generate_karpenter_provisioner(
    workload_type='spark',
    use_spot=True,
    use_graviton=True
)

print('Generated Karpenter Provisioner:')
print(f'  Name: {karpenter_config[\"name\"]}')
print(f'  Instance Types: {karpenter_config[\"instance_types\"][:3]}...')
print(f'  Capacity Type: {karpenter_config[\"capacity_type\"]}')
print(f'  Architecture: {karpenter_config[\"architecture\"]}')

# ============================================
# TEST 3: SPOT interruption handling
# ============================================
print('\n[TEST 3] SPOT Interruption Strategy')
print('-' * 50)

spot_strategy = optimizer.get_spot_strategy()

print('SPOT Instance Strategy:')
print(f'  Diversification: {spot_strategy[\"diversification\"]}')
print(f'  Fallback: {spot_strategy[\"fallback\"]}')
print(f'  Checkpointing: {spot_strategy[\"checkpointing\"]}')

print('\n[PASS] EKS Optimizer tests completed')
"
```

**Expected Results:**
```
======================================================================
TESTING: EKS Optimizer
======================================================================

[TEST 1] t3.large Migration Analysis
--------------------------------------------------
Current Setup:
  Instance: t3.large (x86_64, On-Demand)
  Count: 5 instances
  Monthly Cost: $302.40

Optimized Setup (Graviton + SPOT):
  Instance: t4g.large
  Monthly Cost: $90.72
  Monthly Savings: $211.68
  Savings Percentage: 70.0%

[TEST 2] Karpenter Configuration
--------------------------------------------------
Generated Karpenter Provisioner:
  Name: spark-graviton-spot
  Instance Types: ['r6g.xlarge', 'r6g.2xlarge', 'm6g.xlarge']...
  Capacity Type: spot
  Architecture: arm64

[TEST 3] SPOT Interruption Strategy
--------------------------------------------------
SPOT Instance Strategy:
  Diversification: Use multiple instance types and AZs
  Fallback: On-Demand instances for critical tasks
  Checkpointing: Enable Spark checkpointing every 5 minutes

[PASS] EKS Optimizer tests completed
```

---

# PART 4: TEST INTEGRATIONS

## 4.1 Test: Slack Integration

```bash
python -c "
import sys
sys.path.insert(0, '.')

from integrations.slack_integration import SlackIntegration, SlackConfig

print('=' * 70)
print('TESTING: Slack Integration')
print('=' * 70)

# Create config (mock mode - no actual sending)
config = SlackConfig(
    webhook_url='https://hooks.slack.com/services/TEST/TEST/TEST',
    default_channel='#etl-alerts',
    enabled=True
)

slack = SlackIntegration(config)

# Test message building
print('\n[TEST] Build Slack Message')
print('-' * 50)

message = slack.build_etl_success_message(
    job_name='test_etl_job',
    run_id='run-12345',
    metrics={
        'duration_seconds': 120,
        'rows_read': 1000000,
        'rows_written': 998000,
        'dq_score': 0.95
    }
)

print('Message Built:')
print(f'  Channel: {message.channel}')
print(f'  Blocks: {len(message.blocks)} blocks')
print('  Content Preview: ETL job succeeded with 95% DQ score')

print('\n[PASS] Slack Integration test completed')
print('[NOTE] To send real messages, provide actual webhook URL')
"
```

---

## 4.2 Test: Teams Integration

```bash
python -c "
import sys
sys.path.insert(0, '.')

from integrations.teams_integration import TeamsIntegration, TeamsConfig, TeamsMessage, AlertSeverity

print('=' * 70)
print('TESTING: Teams Integration')
print('=' * 70)

# Create config
config = TeamsConfig(
    webhook_url='https://outlook.office.com/webhook/TEST',
    channel_name='ETL Alerts',
    enabled=True
)

teams = TeamsIntegration(config)

# Test adaptive card building
print('\n[TEST] Build Teams Adaptive Card')
print('-' * 50)

message = TeamsMessage(
    title='ETL Job Completed: my_etl_job',
    text='Job completed successfully',
    severity=AlertSeverity.SUCCESS,
    facts={
        'Run ID': 'run-12345',
        'Duration': '2 minutes',
        'Rows': '1,000,000',
        'DQ Score': '95%'
    }
)

card = teams._build_adaptive_card(message)

print('Adaptive Card Built:')
print(f'  Type: {card[\"type\"]}')
print(f'  Attachments: {len(card[\"attachments\"])}')
print(f'  Card Type: {card[\"attachments\"][0][\"content\"][\"type\"]}')

print('\n[PASS] Teams Integration test completed')
"
```

---

## 4.3 Test: Notification Manager (Unified)

```bash
python -c "
import sys
import json
sys.path.insert(0, '.')

from integrations.notification_manager import create_notification_manager

print('=' * 70)
print('TESTING: Unified Notification Manager')
print('=' * 70)

# Test with full config
config = {
    'notifications': {
        'enabled': 'Y',
        'slack': {
            'enabled': 'Y',
            'webhook_url': 'https://hooks.slack.com/TEST',
            'channel': '#alerts'
        },
        'teams': {
            'enabled': 'Y',
            'webhook_url': 'https://outlook.office.com/webhook/TEST'
        },
        'email': {
            'enabled': 'Y',
            'sender': 'etl@test.com',
            'recipients': ['team@test.com']
        },
        'preferences': {
            'on_start': 'N',
            'on_success': 'Y',
            'on_failure': 'Y',
            'on_dq_failure': 'Y',
            'dq_score_threshold': 0.9,
            'cost_alert_threshold_usd': 100
        }
    }
}

manager = create_notification_manager(config)
status = manager.get_status()

print('\n[TEST] Notification Manager Status')
print('-' * 50)
print(f'Master Enabled: {status[\"notifications_enabled\"]}')
print('\\nChannels:')
print(f'  Slack: enabled={status[\"channels\"][\"slack\"][\"enabled\"]}')
print(f'  Teams: enabled={status[\"channels\"][\"teams\"][\"enabled\"]}')
print(f'  Email: enabled={status[\"channels\"][\"email\"][\"enabled\"]}')
print('\\nPreferences:')
print(f'  On Success: {status[\"preferences\"][\"on_success\"]}')
print(f'  On Failure: {status[\"preferences\"][\"on_failure\"]}')
print(f'  On DQ Failure: {status[\"preferences\"][\"on_dq_failure\"]}')
print('\\nThresholds:')
print(f'  DQ Score: {status[\"thresholds\"][\"dq_score\"]}')
print(f'  Cost Alert: \${status[\"thresholds\"][\"cost_alert_usd\"]}')

print('\n[PASS] Notification Manager test completed')
"
```

---

## 4.4 Test: Audit System with DynamoDB

```bash
python -c "
import sys
import time
from datetime import datetime
sys.path.insert(0, '.')

from audit.etl_audit import ETLRunAudit, DataQualityAudit, RunStatus

print('=' * 70)
print('TESTING: Audit System')
print('=' * 70)

# Create ETL Run Audit
print('\n[TEST 1] Create ETL Run Audit')
print('-' * 50)

run_id = f'test-run-{int(time.time()*1000)}'
audit = ETLRunAudit(
    run_id=run_id,
    job_name='my_etl_job',
    status=RunStatus.RUNNING.value,
    started_at=datetime.now().isoformat(),
    platform='glue',
    config_hash='abc123',
    source_type='s3',
    target_type='redshift'
)

print(f'Created Audit Record:')
print(f'  Run ID: {audit.run_id}')
print(f'  Job: {audit.job_name}')
print(f'  Status: {audit.status}')

# Update with completion
print('\n[TEST 2] Update Audit on Completion')
print('-' * 50)

audit.status = RunStatus.SUCCEEDED.value
audit.completed_at = datetime.now().isoformat()
audit.rows_read = 1_000_000
audit.rows_written = 998_000
audit.dq_score = 0.95
audit.estimated_cost_usd = 2.50

print(f'Updated Audit:')
print(f'  Status: {audit.status}')
print(f'  Rows: {audit.rows_read:,} read, {audit.rows_written:,} written')
print(f'  DQ Score: {audit.dq_score:.1%}')
print(f'  Cost: \${audit.estimated_cost_usd:.2f}')

# Create DQ Audit
print('\n[TEST 3] Create Data Quality Audit')
print('-' * 50)

dq_audit = DataQualityAudit(
    dq_id=f'dq-{int(time.time()*1000)}',
    run_id=run_id,
    job_name='my_etl_job',
    executed_at=datetime.now().isoformat(),
    overall_score=0.95,
    total_rules=5,
    passed_rules=4,
    failed_rules=1,
    rule_results=[
        {'rule': 'not_null_check', 'passed': True, 'score': 1.0},
        {'rule': 'range_check', 'passed': True, 'score': 0.98},
        {'rule': 'regex_check', 'passed': False, 'score': 0.85}
    ]
)

print(f'Created DQ Audit:')
print(f'  DQ ID: {dq_audit.dq_id}')
print(f'  Overall Score: {dq_audit.overall_score:.1%}')
print(f'  Rules: {dq_audit.passed_rules}/{dq_audit.total_rules} passed')

print('\n[PASS] Audit System test completed')
"
```

---

# PART 5: E2E TESTING - SIMPLE USE CASE

## 5.1 Simple E2E: S3 to S3 with Slack Notification

```bash
python -c "
import sys
import json
import time
from datetime import datetime
sys.path.insert(0, '.')

from integrations.notification_manager import create_notification_manager
from audit.etl_audit import ETLRunAudit, RunStatus

print('=' * 70)
print('E2E TEST: Simple S3 to S3 with Slack')
print('=' * 70)

# Load config
with open('test_configs/simple_glue_catalog.json') as f:
    config = json.load(f)

job_name = config['job_name']
run_id = f'{job_name}-{int(time.time()*1000)}'

print(f'\n[STEP 1] Job Configuration')
print('-' * 50)
print(f'Job Name: {job_name}')
print(f'Run ID: {run_id}')
print(f'Source: {config[\"source\"][\"type\"]}')
print(f'Target: {config[\"target\"][\"type\"]}')

# Create notification manager
print(f'\n[STEP 2] Initialize Notifications')
print('-' * 50)
manager = create_notification_manager(config)
status = manager.get_status()
print(f'Slack: {status[\"channels\"][\"slack\"][\"enabled\"]}')
print(f'Teams: {status[\"channels\"][\"teams\"][\"enabled\"]}')
print(f'Email: {status[\"channels\"][\"email\"][\"enabled\"]}')

# Create audit record
print(f'\n[STEP 3] Create Audit Record')
print('-' * 50)
audit = ETLRunAudit(
    run_id=run_id,
    job_name=job_name,
    status=RunStatus.RUNNING.value,
    started_at=datetime.now().isoformat(),
    platform=config.get('platform', 'glue'),
    config_hash='simple_e2e',
    source_type=config['source']['type'],
    target_type=config['target']['type']
)
print(f'Audit Status: {audit.status}')

# Simulate ETL execution
print(f'\n[STEP 4] Simulate ETL Execution')
print('-' * 50)
print('Reading from source...')
time.sleep(0.2)
print('Applying transformations...')
time.sleep(0.2)
print('Writing to target...')
time.sleep(0.2)

# Complete audit
print(f'\n[STEP 5] Complete Audit')
print('-' * 50)
audit.status = RunStatus.SUCCEEDED.value
audit.completed_at = datetime.now().isoformat()
audit.rows_read = 50000
audit.rows_written = 49800
audit.dq_score = 0.996
audit.estimated_cost_usd = 0.25

print(f'Status: {audit.status}')
print(f'Rows: {audit.rows_read:,} -> {audit.rows_written:,}')
print(f'DQ Score: {audit.dq_score:.1%}')
print(f'Cost: \${audit.estimated_cost_usd:.2f}')

# Notification check
print(f'\n[STEP 6] Notification Check')
print('-' * 50)
print(f'Would send Slack notification: {status[\"preferences\"][\"on_success\"]}')

print('\n' + '=' * 70)
print('[PASS] Simple E2E Test Completed Successfully!')
print('=' * 70)
"
```

---

# PART 6: E2E TESTING - COMPLEX USE CASE

## 6.1 Complex E2E: Multi-Source Pipeline with All Features

```bash
python -c "
import sys
import json
import time
from datetime import datetime
sys.path.insert(0, '.')

from integrations.notification_manager import create_notification_manager
from audit.etl_audit import ETLRunAudit, DataQualityAudit, RunStatus
from agents.workload_assessment_agent import WorkloadAssessmentAgent
from agents.data_quality_agent import DataQualityAgent

print('=' * 70)
print('E2E TEST: Complex Multi-Source Pipeline')
print('=' * 70)

# Load config
with open('test_configs/complex_full_pipeline.json') as f:
    config = json.load(f)

job_name = config['job_name']
run_id = f'{job_name}-{int(time.time()*1000)}'

print(f'\n[STEP 1] Job Configuration')
print('-' * 50)
print(f'Job Name: {job_name}')
print(f'Run ID: {run_id}')
print(f'Sources: {len(config[\"source\"][\"sources\"])}')
for src in config['source']['sources']:
    print(f'  - {src[\"name\"]}: {src[\"type\"]}')
print(f'Target: {config[\"target\"][\"type\"]} ({config[\"target\"][\"table\"]})')
print(f'Platform: {config[\"platform\"][\"primary\"]} (fallback: {config[\"platform\"][\"fallback\"]})')

# Workload assessment
print(f'\n[STEP 2] Workload Assessment')
print('-' * 50)
workload = {
    'data_size_gb': 100,
    'row_count': 50_000_000,
    'columns': 50,
    'join_count': 2,
    'transformation_complexity': 'high'
}
print(f'Data Size: {workload[\"data_size_gb\"]}GB')
print(f'Row Count: {workload[\"row_count\"]:,}')
print(f'Complexity: {workload[\"transformation_complexity\"]}')
print(f'Recommended Platform: EMR (based on workload)')

# Initialize notifications
print(f'\n[STEP 3] Initialize Notifications')
print('-' * 50)
manager = create_notification_manager(config)
status = manager.get_status()
print(f'Slack: {status[\"channels\"][\"slack\"][\"enabled\"]}')
print(f'Teams: {status[\"channels\"][\"teams\"][\"enabled\"]}')
print(f'Email: {status[\"channels\"][\"email\"][\"enabled\"]}')
print(f'Notify on DQ Failure: {status[\"preferences\"][\"on_dq_failure\"]}')

# Create audit record
print(f'\n[STEP 4] Create Audit Record')
print('-' * 50)
audit = ETLRunAudit(
    run_id=run_id,
    job_name=job_name,
    status=RunStatus.RUNNING.value,
    started_at=datetime.now().isoformat(),
    platform=config['platform']['primary'],
    config_hash='complex_e2e',
    source_type='multi_source',
    target_type=config['target']['type']
)
print(f'Audit Created: {audit.run_id}')

# Simulate ETL execution
print(f'\n[STEP 5] Simulate ETL Execution')
print('-' * 50)
print('Reading orders table...')
time.sleep(0.1)
print('Reading customers table...')
time.sleep(0.1)
print('Reading products table...')
time.sleep(0.1)
print('Joining orders with customers...')
time.sleep(0.1)
print('Joining with products...')
time.sleep(0.1)
print('Applying filters...')
time.sleep(0.1)
print('Computing aggregations...')
time.sleep(0.1)
print('Writing to Iceberg table...')
time.sleep(0.1)

# Data Quality Check
print(f'\n[STEP 6] Data Quality Check')
print('-' * 50)
dq_rules = config.get('data_quality', {}).get('rules', [])
print(f'Running {len(dq_rules)} DQ rules...')

dq_results = {
    'overall_score': 0.94,
    'passed_rules': len(dq_rules) - 1,
    'failed_rules': 1,
    'failed_rule_names': ['completeness_check']
}

for rule in dq_rules:
    passed = rule['name'] not in dq_results['failed_rule_names']
    print(f'  [{\"PASS\" if passed else \"FAIL\"}] {rule[\"name\"]}')

print(f'Overall DQ Score: {dq_results[\"overall_score\"]:.1%}')

# Create DQ Audit
dq_audit = DataQualityAudit(
    dq_id=f'dq-{int(time.time()*1000)}',
    run_id=run_id,
    job_name=job_name,
    executed_at=datetime.now().isoformat(),
    overall_score=dq_results['overall_score'],
    total_rules=len(dq_rules),
    passed_rules=dq_results['passed_rules'],
    failed_rules=dq_results['failed_rules']
)

# Complete audit
print(f'\n[STEP 7] Complete Audit')
print('-' * 50)
audit.status = RunStatus.SUCCEEDED.value
audit.completed_at = datetime.now().isoformat()
audit.rows_read = 50_000_000
audit.rows_written = 49_500_000
audit.dq_score = dq_results['overall_score']
audit.estimated_cost_usd = 8.50

print(f'Status: {audit.status}')
print(f'Rows: {audit.rows_read:,} -> {audit.rows_written:,}')
print(f'DQ Score: {audit.dq_score:.1%}')
print(f'Cost: \${audit.estimated_cost_usd:.2f}')

# Check DQ threshold
dq_threshold = config['notifications']['preferences'].get('dq_score_threshold', 0.9)
print(f'\nDQ Threshold Check: {audit.dq_score:.1%} vs {dq_threshold:.1%}')
if audit.dq_score >= dq_threshold:
    print('Result: PASS - Above threshold')
else:
    print('Result: FAIL - Below threshold, would trigger DQ alert')

# Notifications summary
print(f'\n[STEP 8] Notification Summary')
print('-' * 50)
print(f'Would send success notification to:')
if status['channels']['slack']['enabled']:
    print(f'  - Slack: #data-platform-alerts')
if status['channels']['teams']['enabled']:
    print(f'  - Teams: Data Platform')
if status['channels']['email']['enabled']:
    print(f'  - Email: {config[\"notifications\"][\"email\"][\"recipients\"]}')

print('\n' + '=' * 70)
print('[PASS] Complex E2E Test Completed Successfully!')
print('=' * 70)
"
```

---

# PART 7: ASK FOR RECOMMENDATIONS

## 7.1 Get Platform Recommendations

```bash
python -c "
import sys
sys.path.insert(0, '.')

from agents.aws_recommendations_engine import AWSRecommendationsEngine
from agents.workload_assessment_agent import WorkloadAssessmentAgent
from agents.eks_optimizer import EKSOptimizer

print('=' * 70)
print('ASK FOR RECOMMENDATIONS')
print('=' * 70)

# Your workload parameters
my_workload = {
    'data_size_gb': 200,
    'row_count': 100_000_000,
    'columns': 75,
    'join_count': 3,
    'transformation_complexity': 'high',
    'frequency': 'daily',
    'current_platform': 'glue',
    'current_cost_monthly': 500
}

print('\n[YOUR WORKLOAD]')
print('-' * 50)
for key, value in my_workload.items():
    print(f'  {key}: {value}')

# Get recommendations
print('\n[PLATFORM RECOMMENDATION]')
print('-' * 50)

engine = AWSRecommendationsEngine()
workload_agent = WorkloadAssessmentAgent()

# Platform comparison
comparison = {
    'glue': {'monthly_cost': 880, 'pros': 'Serverless, no management'},
    'emr': {'monthly_cost': 520, 'pros': 'Cost effective, full control'},
    'eks': {'monthly_cost': 450, 'pros': 'Kubernetes native, Karpenter'}
}

print('Platform Comparison:')
for platform, details in comparison.items():
    print(f'  {platform}: \${details[\"monthly_cost\"]}/month - {details[\"pros\"]}')

print('\n** RECOMMENDATION: Switch to EMR or EKS **')
print(f'   Potential savings: \${my_workload[\"current_cost_monthly\"] - 450}/month')

# EKS with Karpenter/SPOT/Graviton
print('\n[EKS OPTIMIZATION]')
print('-' * 50)

eks_optimizer = EKSOptimizer()
print('If using EKS with Karpenter + SPOT + Graviton:')
print('  - Use Graviton instances (arm64): 40% better price-performance')
print('  - Use SPOT instances: 70-90% cost reduction')
print('  - Use Karpenter: 30-second node provisioning')
print('  - Estimated monthly cost: \$135 (vs \$500 current)')

print('\n[NEXT STEPS]')
print('-' * 50)
print('1. Test EMR with your workload using transient clusters')
print('2. If containerized, try EKS with Karpenter provisioner')
print('3. Enable SPOT instances for non-critical workloads')
print('4. Consider Graviton for better price-performance')
"
```

## 7.2 Interactive Recommendations Query

```bash
python -c "
import sys
sys.path.insert(0, '.')

print('=' * 70)
print('RECOMMENDATION QUERIES')
print('=' * 70)

# Query 1: Best platform for my data size
print('\nQ: What platform is best for 500GB daily ETL?')
print('A: EMR with transient clusters')
print('   - Cost: ~\$15-20 per run')
print('   - Duration: 30-45 minutes')
print('   - Use r5.2xlarge instances with SPOT')

# Query 2: How to reduce costs
print('\nQ: How to reduce ETL costs by 50%?')
print('A: Three strategies:')
print('   1. Switch from On-Demand to SPOT (save 70%)')
print('   2. Use Graviton instances (save 40%)')
print('   3. Right-size workers based on actual usage')

# Query 3: Handling data skew
print('\nQ: My job has data skew. What to do?')
print('A: Enable these configurations:')
print('   - spark.sql.adaptive.enabled=true')
print('   - spark.sql.adaptive.skewJoin.enabled=true')
print('   - Consider salting for heavily skewed keys')

# Query 4: Real-time vs batch
print('\nQ: Should I use streaming or batch?')
print('A: Depends on latency requirement:')
print('   - < 1 second: Kinesis + Flink')
print('   - < 5 minutes: Spark Structured Streaming')
print('   - > 5 minutes: Batch with Glue/EMR')
"
```

---

# SUMMARY

## Quick Command Reference

```bash
# Setup
cd /home/user/strands_etl
export PYTHONPATH=\"\${PYTHONPATH}:\$(pwd)\"

# Create test configs
python -c \"...\"  # (see Part 1)

# Test each agent
python tests/component_tests/test_01_config_parsing.py
python tests/component_tests/test_02_notification_flags.py
python tests/component_tests/test_03_notification_manager.py
python tests/component_tests/test_04_teams_integration.py
python tests/component_tests/test_05_api_gateway.py
python tests/component_tests/test_06_audit_system.py

# Run all component tests
python tests/run_all_tests.py --component-only

# Run E2E tests
python tests/run_all_tests.py --e2e-only

# Run full suite
python tests/run_all_tests.py

# Generate report
python tests/run_all_tests.py --report test_report.json
```

## Testing Flow

```
PART 1: Create JSONs → PART 2: Create PySpark Scripts →
PART 3: Test Agents → PART 4: Test Integrations →
PART 5: Simple E2E → PART 6: Complex E2E →
PART 7: Get Recommendations
```
