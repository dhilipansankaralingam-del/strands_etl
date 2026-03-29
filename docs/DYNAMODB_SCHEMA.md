# Strands ETL - Production Tracking DynamoDB Schema

## Overview

This schema supports comprehensive tracking of all ETL job executions, metrics, recommendations, issues, costs, quality, and compliance.

## Tables

### 1. JobExecutions (Primary Table)

**Purpose**: Track every job execution with complete metadata

**Primary Key**:
- Partition Key: `execution_id` (String) - Format: `exec-{timestamp}-{uuid}`
- Sort Key: `timestamp` (Number) - Unix timestamp

**Attributes**:
```json
{
  "execution_id": "exec-2024-01-21-abc123",
  "timestamp": 1705843200,
  "job_name": "customer_order_summary",
  "job_type": "glue|emr|lambda",
  "status": "running|success|failed|timeout",
  "start_time": 1705843200,
  "end_time": 1705846800,
  "duration_seconds": 3600,

  "workload_characteristics": {
    "data_volume_gb": 250,
    "data_volume_category": "high",
    "complexity": "high",
    "criticality": "critical",
    "estimated_runtime_minutes": 60
  },

  "platform_decision": {
    "selected_platform": "glue",
    "confidence": 0.92,
    "decision_reasoning": "High volume favors Glue",
    "alternatives_considered": ["emr", "lambda"],
    "decision_agent_version": "v1.0"
  },

  "execution_metrics": {
    "memory_mb_avg": 4096,
    "memory_mb_max": 5120,
    "memory_mb_allocated": 8192,
    "memory_utilization_percent": 62.5,
    "cpu_utilization_percent": 75.0,
    "data_processed_gb": 250,
    "data_read_gb": 250,
    "data_written_gb": 180,
    "shuffle_read_gb": 45,
    "shuffle_write_gb": 42,
    "records_processed": 150000000,
    "records_per_second": 41666
  },

  "cost_breakdown": {
    "total_cost_usd": 8.50,
    "compute_cost_usd": 6.20,
    "storage_cost_usd": 1.80,
    "network_cost_usd": 0.50,
    "bedrock_cost_usd": 0.05,
    "cost_per_gb": 0.034,
    "dpu_hours": 12.4,
    "dpu_cost_per_hour": 0.50
  },

  "data_quality": {
    "overall_score": 0.95,
    "completeness_score": 0.98,
    "accuracy_score": 0.94,
    "consistency_score": 0.93,
    "null_percentage": 2.5,
    "duplicate_percentage": 0.8,
    "schema_violations": 0,
    "business_rule_violations": 3,
    "quality_checks_passed": 47,
    "quality_checks_failed": 3
  },

  "compliance": {
    "gdpr_compliant": true,
    "hipaa_compliant": false,
    "pii_detected": true,
    "pii_masked": true,
    "retention_policy_applied": true,
    "encryption_at_rest": true,
    "encryption_in_transit": true,
    "audit_log_enabled": true,
    "data_classification": "confidential"
  },

  "issues_detected": [
    {
      "issue_id": "issue-001",
      "severity": "high",
      "category": "performance",
      "type": "multiple_counts",
      "description": "Multiple .count() operations detected (5 occurrences)",
      "impact": "Causes 5 full table scans",
      "recommendation": "Cache DataFrame after first count, reuse cached version",
      "estimated_improvement": "40% faster execution",
      "code_location": "line 145-180"
    },
    {
      "issue_id": "issue-002",
      "severity": "medium",
      "category": "small_files",
      "type": "output_files",
      "description": "Generated 5000 small files (avg 2MB each)",
      "impact": "Increased S3 API costs, slower downstream reads",
      "recommendation": "Use coalesce(50) to reduce output files to 50",
      "estimated_improvement": "90% fewer files, 80% faster downstream",
      "code_location": "final write operation"
    },
    {
      "issue_id": "issue-003",
      "severity": "high",
      "category": "data_access",
      "type": "full_scan",
      "description": "Full table scan on 1TB source table instead of delta",
      "impact": "Processing 10x more data than necessary",
      "recommendation": "Implement incremental load using max(timestamp) from previous run",
      "estimated_improvement": "90% cost reduction, 10x faster",
      "code_location": "source data read"
    },
    {
      "issue_id": "issue-004",
      "severity": "medium",
      "category": "broadcast",
      "type": "missing_optimization",
      "description": "Small dimension table (50MB) not broadcasted in join",
      "impact": "Unnecessary shuffle of 250GB fact table",
      "recommendation": "Use broadcast hint: df.join(broadcast(dim_df))",
      "estimated_improvement": "30% faster join operation",
      "code_location": "line 220"
    }
  ],

  "optimizations_applied": [
    {
      "optimization_id": "opt-001",
      "type": "caching",
      "description": "Added DataFrame caching before multiple actions",
      "applied": true,
      "improvement_actual": "35% reduction in execution time"
    }
  ],

  "recommendations": [
    {
      "recommendation_id": "rec-001",
      "priority": "high",
      "category": "resource_allocation",
      "title": "Increase executor memory",
      "description": "Current memory utilization at 87%, frequent GC pauses",
      "action": "Increase executor memory from 4GB to 6GB",
      "estimated_impact": "15% faster, fewer OOM errors",
      "estimated_cost_impact": "+$1.20 per run"
    },
    {
      "recommendation_id": "rec-002",
      "priority": "high",
      "category": "partitioning",
      "title": "Optimize output partitioning",
      "description": "Current 200 partitions suboptimal for 250GB output",
      "action": "Change spark.sql.shuffle.partitions from 200 to 500",
      "estimated_impact": "25% faster aggregations",
      "estimated_cost_impact": "Neutral"
    }
  ],

  "error_details": {
    "error_occurred": false,
    "error_message": null,
    "error_type": null,
    "error_stack_trace": null,
    "retry_count": 0
  },

  "stakeholders": [
    "data-team@company.com",
    "analytics-lead@company.com"
  ],

  "tags": {
    "environment": "production",
    "team": "data-engineering",
    "cost_center": "analytics",
    "priority": "high"
  },

  "learning_vector_stored": true,
  "learning_vector_s3_path": "s3://strands-etl-learning/learning/vectors/learning/exec-2024-01-21-abc123.json",

  "notification_sent": true,
  "notification_timestamp": 1705846900,
  "notification_recipients": ["data-team@company.com"]
}
```

**GSI 1 - JobNameIndex**:
- Partition Key: `job_name`
- Sort Key: `timestamp`
- Purpose: Query all executions for a specific job

**GSI 2 - StatusIndex**:
- Partition Key: `status`
- Sort Key: `timestamp`
- Purpose: Find failed/running jobs quickly

**GSI 3 - CostIndex**:
- Partition Key: `job_name`
- Sort Key: `cost_breakdown.total_cost_usd`
- Purpose: Identify most expensive jobs

---

### 2. AgentInvocations

**Purpose**: Track every agent invocation for audit and debugging

**Primary Key**:
- Partition Key: `invocation_id` (String)
- Sort Key: `timestamp` (Number)

**Attributes**:
```json
{
  "invocation_id": "inv-agent-001",
  "timestamp": 1705843200,
  "execution_id": "exec-2024-01-21-abc123",
  "agent_name": "decision-agent",
  "agent_version": "v1.0",
  "input_parameters": {},
  "output_response": {},
  "duration_ms": 450,
  "tokens_consumed": {
    "input_tokens": 1200,
    "output_tokens": 800,
    "total_cost_usd": 0.0156
  },
  "success": true,
  "error_message": null
}
```

---

### 3. IssueRegistry

**Purpose**: Track all issues across executions for trend analysis

**Primary Key**:
- Partition Key: `issue_type` (String)
- Sort Key: `timestamp` (Number)

**Attributes**:
```json
{
  "issue_type": "multiple_counts",
  "timestamp": 1705843200,
  "execution_id": "exec-2024-01-21-abc123",
  "job_name": "customer_order_summary",
  "severity": "high",
  "occurrences": 5,
  "resolved": false,
  "resolution_timestamp": null
}
```

**GSI 1 - JobIssuesIndex**:
- Partition Key: `job_name`
- Sort Key: `timestamp`
- Purpose: Track issues for specific job over time

---

### 4. CostTrends

**Purpose**: Daily/monthly cost aggregations for budgeting

**Primary Key**:
- Partition Key: `date` (String) - Format: YYYY-MM-DD
- Sort Key: `job_name` (String)

**Attributes**:
```json
{
  "date": "2024-01-21",
  "job_name": "customer_order_summary",
  "total_executions": 3,
  "successful_executions": 3,
  "failed_executions": 0,
  "total_cost_usd": 25.50,
  "avg_cost_per_execution": 8.50,
  "total_duration_seconds": 10800,
  "total_data_processed_gb": 750
}
```

---

### 5. DataQualityHistory

**Purpose**: Track data quality metrics over time

**Primary Key**:
- Partition Key: `job_name` (String)
- Sort Key: `timestamp` (Number)

**Attributes**:
```json
{
  "job_name": "customer_order_summary",
  "timestamp": 1705843200,
  "execution_id": "exec-2024-01-21-abc123",
  "overall_score": 0.95,
  "completeness_score": 0.98,
  "accuracy_score": 0.94,
  "trend": "improving",
  "anomalies_detected": []
}
```

---

### 6. ComplianceAudit

**Purpose**: Audit trail for compliance and governance

**Primary Key**:
- Partition Key: `audit_id` (String)
- Sort Key: `timestamp` (Number)

**Attributes**:
```json
{
  "audit_id": "audit-001",
  "timestamp": 1705843200,
  "execution_id": "exec-2024-01-21-abc123",
  "job_name": "customer_order_summary",
  "compliance_checks": [
    {
      "check_name": "PII Detection",
      "passed": true,
      "details": "All PII fields properly masked"
    },
    {
      "check_name": "GDPR Right to be Forgotten",
      "passed": true,
      "details": "Deletion requests processed"
    }
  ],
  "auditor": "compliance-agent",
  "compliance_officer_notified": true
}
```

---

## Access Patterns

### 1. Get Latest Execution for Job
```
Query: JobNameIndex
PK = job_name
SortKeyCondition = timestamp (descending)
Limit = 1
```

### 2. Get All Failed Jobs in Last 24h
```
Query: StatusIndex
PK = "failed"
SortKeyCondition = timestamp > (now - 86400)
```

### 3. Get Cost Trend for Job
```
Query: CostTrends
PK = date (range)
SK = job_name
```

### 4. Get All Issues for Job
```
Query: IssueRegistry.JobIssuesIndex
PK = job_name
SortKeyCondition = timestamp (range)
```

### 5. Get Quality Trend
```
Query: DataQualityHistory
PK = job_name
SortKeyCondition = timestamp (range)
```

---

## Capacity Planning

**Read Capacity**: 25 RCU (auto-scaling enabled up to 100)
**Write Capacity**: 50 WCU (auto-scaling enabled up to 200)

**Estimated Item Sizes**:
- JobExecutions: ~8 KB per item
- AgentInvocations: ~2 KB per item
- IssueRegistry: ~1 KB per item
- CostTrends: ~500 bytes per item
- DataQualityHistory: ~1 KB per item
- ComplianceAudit: ~3 KB per item

**Estimated Monthly Costs** (1000 executions/month):
- Storage: ~$2.50
- Read/Write Operations: ~$15-25
- **Total: ~$20-30/month**

---

## TTL Configuration

**JobExecutions**: 90 days (align with S3 learning vector lifecycle)
**AgentInvocations**: 30 days
**IssueRegistry**: No TTL (keep for trend analysis)
**CostTrends**: 1 year
**DataQualityHistory**: 1 year
**ComplianceAudit**: 7 years (compliance requirement)
