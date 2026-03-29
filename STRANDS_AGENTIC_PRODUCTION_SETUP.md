# Strands ETL - Complete Agentic Production Setup with Full Tracking

## 🎯 Overview

This is the **production-grade** Strands ETL system with:
- ✅ **True Agentic Framework** with autonomous agents
- ✅ **Complete Job Tracking** in DynamoDB
- ✅ **7 Specialized Agents** (Decision, Quality, Optimization, Learning, Compliance, Cost Tracking, Supervisor)
- ✅ **Comprehensive Metrics**: Memory, CPU, cost, quality, compliance
- ✅ **Issue Detection**: Small files, full scans, missing broadcasts, PySpark anti-patterns
- ✅ **Email Notifications** via SES on job success/failure
- ✅ **Advanced Dashboard** with all metrics visualization
- ✅ **MCP Integration** for enhanced tooling
- ✅ **Cost: $650-1,400/month** (includes DynamoDB, SES)

---

## 📊 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      USER / STAKEHOLDERS                         │
│                  (Receive Email Notifications)                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │  Supervisor Agent    │◄─── Orchestrates all agents
              │   (Bedrock Agent)    │
              └──────────┬───────────┘
                         │
         ┌───────────────┼───────────────┬──────────────┬────────────────┐
         │               │               │              │                │
         ▼               ▼               ▼              ▼                ▼
    ┌─────────┐    ┌─────────┐    ┌──────────┐  ┌───────────┐   ┌────────────┐
    │Decision │    │ Quality │    │Optimiza- │  │ Learning  │   │Compliance  │
    │ Agent   │    │ Agent   │    │tion Agent│  │  Agent    │   │   Agent    │
    └────┬────┘    └────┬────┘    └────┬─────┘  └─────┬─────┘   └──────┬─────┘
         │              │              │              │                 │
         ▼              ▼              ▼              ▼                 ▼
    ┌─────────┐    ┌─────────┐    ┌──────────┐  ┌───────────┐   ┌────────────┐
    │Decision │    │ Quality │    │Optimiza- │  │ Learning  │   │Compliance  │
    │ Lambda  │    │ Lambda  │    │tion      │  │  Lambda   │   │   Lambda   │
    └────┬────┘    └────┬────┘    └─Lambda───┘  └─────┬─────┘   └──────┬─────┘
         │              │              │              │                 │
         └──────────────┴──────────────┴──────────────┴─────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
                    ▼                               ▼
         ┌────────────────────┐         ┌────────────────────┐
         │  Cost Tracking     │         │  Execution         │
         │  Agent + Lambda    │         │  Tracking Lambda   │
         └────────┬───────────┘         └─────────┬──────────┘
                  │                               │
                  └───────────┬───────────────────┘
                              │
                              ▼
                    ┌──────────────────────┐
                    │   DynamoDB Tables    │
                    │  - JobExecutions     │
                    │  - AgentInvocations  │
                    │  - IssueRegistry     │
                    │  - CostTrends        │
                    │  - DataQuality       │
                    │  - ComplianceAudit   │
                    └──────────┬───────────┘
                              │
                ┌─────────────┴─────────────┐
                │                           │
                ▼                           ▼
     ┌────────────────────┐      ┌────────────────────┐
     │  Email Notification│      │  Enhanced Dashboard│
     │  Lambda (SES)      │      │   (Streamlit)      │
     └────────────────────┘      └────────────────────┘
```

---

## 🆕 What's New vs Basic Version

### New Agents:
1. **Compliance Agent** - GDPR/HIPAA checks, PII detection
2. **Cost Tracking Agent** - Cost breakdowns, optimization, forecasting

### New Infrastructure:
1. **DynamoDB Tables** (6 tables) - Complete tracking
2. **SES Email** - Stakeholder notifications
3. **Tracking Lambda** - Writes all metrics to DynamoDB
4. **Email Lambda** - Sends formatted email summaries

### Enhanced Features:
1. **Issue Detection**:
   - Small file problems
   - Full scans vs delta loads
   - Missing broadcast joins
   - Multiple .count() operations
   - Inefficient window functions
   - Cartesian joins
   - SELECT * queries

2. **Metrics Tracking**:
   - Memory (avg, max, utilization %)
   - CPU utilization
   - Data processed/read/written
   - Records per second
   - Cost per GB
   - Quality scores (5 dimensions)
   - Compliance status

3. **Dashboard Enhancements**:
   - Real-time job status
   - Cost trends with forecasting
   - Quality score trends
   - Issue heat map
   - Compliance dashboard
   - Top cost offenders
   - Platform comparison charts

---

## 📦 Complete Component List

### AWS Services Required:
1. **Bedrock** - 7 agents + Claude 3.5 Sonnet
2. **Lambda** - 9 functions (7 agent lambdas + tracking + email)
3. **DynamoDB** - 6 tables
4. **S3** - 2 buckets (learning + schemas)
5. **SES** - Email notifications
6. **IAM** - 3 roles
7. **CloudWatch** - Logs + metrics

### Bedrock Agents:
1. Decision Agent
2. Quality Agent
3. Optimization Agent
4. Learning Agent
5. **Compliance Agent** (NEW)
6. **Cost Tracking Agent** (NEW)
7. Supervisor Agent

### Lambda Functions:
1. strands-decision-agent-lambda
2. strands-quality-agent-lambda
3. strands-optimization-agent-lambda
4. strands-learning-agent-lambda
5. **strands-compliance-agent-lambda** (NEW)
6. **strands-cost-tracking-agent-lambda** (NEW)
7. strands-execution-lambda
8. **strands-tracking-lambda** (NEW)
9. **strands-email-notification-lambda** (NEW)

### DynamoDB Tables:
1. **StrandsJobExecutions** - Primary tracking table
2. **StrandsAgentInvocations** - Agent audit trail
3. **StrandsIssueRegistry** - Issue tracking
4. **StrandsCostTrends** - Cost aggregations
5. **StrandsDataQualityHistory** - Quality trends
6. **StrandsComplianceAudit** - Compliance records

---

## 💰 Updated Cost Estimate

### Monthly Recurring Costs:

| Service | Component | Monthly Cost |
|---------|-----------|--------------|
| **Bedrock** | 7 agents + model | $400-800 |
| **Lambda** | 9 functions, 5K invocations | $150-250 |
| **DynamoDB** | 6 tables, 1K writes/day | $25-40 |
| **S3** | Storage + requests | $50-100 |
| **SES** | 10K emails/month | $1-5 |
| **CloudWatch** | Logs + metrics | $50-100 |
| **Glue/EMR** | Variable | $50-200 |
| **TOTAL** | | **$726-1,495/month** |

**Compared to Basic Version**:
- Basic S3-only: $570-1,240
- This production version: $726-1,495
- **Additional cost: $156-255/month** for complete tracking, compliance, and notifications

---

## 🚀 Complete Setup Guide

### Prerequisites

✅ AWS Account with permissions for all services
✅ AWS CLI configured
✅ This repository cloned locally
✅ 4-5 hours for complete setup

---

## PART 1: CREATE DYNAMODB TABLES (30 minutes)

### Step 1: Create JobExecutions Table

1. **AWS Console → DynamoDB → Create Table**
2. Fill in:
   ```
   Table name: StrandsJobExecutions
   Partition key: execution_id (String)
   Sort key: timestamp (Number)

   Table settings: Customize settings
   Read capacity: On-demand OR Provisioned (25 RCU)
   Write capacity: On-demand OR Provisioned (50 WCU)

   Enable: Auto scaling (if provisioned)
   Min: 5, Max: 100
   ```

3. **Create Global Secondary Indexes**:
   - **JobNameIndex**:
     - Partition key: `job_name` (String)
     - Sort key: `timestamp` (Number)
   - **StatusIndex**:
     - Partition key: `status` (String)
     - Sort key: `timestamp` (Number)
   - **CostIndex**:
     - Partition key: `job_name` (String)
     - Sort key: `cost_breakdown.total_cost_usd` (Number)

4. **Enable TTL**:
   - TTL attribute: `ttl`
   - TTL will be set to 90 days from creation

5. Click **Create table**

### Step 2: Create Remaining Tables

Repeat for each table with these specifications:

**StrandsAgentInvocations**:
```
Partition key: invocation_id (String)
Sort key: timestamp (Number)
Read/Write: 10/20 RCU/WCU
```

**StrandsIssueRegistry**:
```
Partition key: issue_type (String)
Sort key: timestamp (Number)
GSI: JobIssuesIndex (job_name, timestamp)
Read/Write: 10/10 RCU/WCU
```

**StrandsCostTrends**:
```
Partition key: date (String)
Sort key: job_name (String)
Read/Write: 10/20 RCU/WCU
```

**StrandsDataQualityHistory**:
```
Partition key: job_name (String)
Sort key: timestamp (Number)
Read/Write: 10/10 RCU/WCU
```

**StrandsComplianceAudit**:
```
Partition key: audit_id (String)
Sort key: timestamp (Number)
Read/Write: 5/10 RCU/WCU
TTL: 7 years (compliance requirement)
```

**✅ Checkpoint 1**: All 6 DynamoDB tables created!

---

## PART 2: SETUP SES FOR EMAIL NOTIFICATIONS (20 minutes)

### Step 3: Configure Amazon SES

1. **AWS Console → Amazon Simple Email Service (SES)**

2. **Verify Email Addresses**:
   - Click **Email Addresses** → **Verify a New Email Address**
   - Enter your email: `data-team@company.com`
   - Check your inbox and click verification link
   - Repeat for all stakeholder emails

3. **Request Production Access** (Optional but recommended):
   - By default, SES is in sandbox mode (can only send to verified emails)
   - Go to **Account dashboard** → **Request production access**
   - Fill out the form explaining your use case
   - Approval usually takes 24-48 hours

4. **Create Email Template**:
   - Go to **Email templates** → **Create template**
   - Template name: `strands-job-notification`
   - Subject: `[Strands ETL] Job {{JobName}} - {{Status}}`
   - HTML body:
```html
<html>
<body>
  <h2>Strands ETL Job Notification</h2>
  <p><strong>Job Name:</strong> {{JobName}}</p>
  <p><strong>Status:</strong> {{Status}}</p>
  <p><strong>Execution ID:</strong> {{ExecutionID}}</p>
  <p><strong>Duration:</strong> {{Duration}} minutes</p>
  <p><strong>Cost:</strong> ${{Cost}}</p>
  <p><strong>Quality Score:</strong> {{QualityScore}}/1.0</p>

  <h3>Issues Detected</h3>
  <ul>{{IssuesList}}</ul>

  <h3>Recommendations</h3>
  <ul>{{RecommendationsList}}</ul>

  <p>View full details in the <a href="{{DashboardURL}}">Strands Dashboard</a></p>
</body>
</html>
```

5. **Note your SES Region**: Usually `us-east-1`

**✅ Checkpoint 2**: SES configured and emails verified!

---

## PART 3: CREATE S3 BUCKETS (Same as basic version - 15 minutes)

Follow steps from COMPLETE_SETUP_GUIDE.md Part 1 to create:
- `strands-etl-learning` bucket
- `strands-etl-schemas` bucket

**✅ Checkpoint 3**: S3 buckets ready!

---

## PART 4: CREATE IAM ROLES (45 minutes)

### Step 4: Create Enhanced Lambda Execution Role

1. **IAM → Roles → Create role**
2. Trust entity: Lambda
3. **Add these managed policies**:
   - AWSLambdaBasicExecutionRole
4. **Create inline policy**:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3Access",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::strands-etl-learning/*",
        "arn:aws:s3:::strands-etl-schemas/*"
      ]
    },
    {
      "Sid": "DynamoDBAccess",
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:Query",
        "dynamodb:Scan",
        "dynamodb:UpdateItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:*:*:table/Strands*",
        "arn:aws:dynamodb:*:*:table/Strands*/index/*"
      ]
    },
    {
      "Sid": "SESAccess",
      "Effect": "Allow",
      "Action": [
        "ses:SendEmail",
        "ses:SendTemplatedEmail"
      ],
      "Resource": "*"
    },
    {
      "Sid": "GlueEMRAccess",
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "emr:RunJobFlow",
        "emr:DescribeCluster"
      ],
      "Resource": "*"
    },
    {
      "Sid": "BedrockAccess",
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel"
      ],
      "Resource": "*"
    }
  ]
}
```

5. Role name: `StrandsLambdaExecutionRoleEnhanced`
6. Create role

### Step 5: Create Bedrock Agent Role

(Same as basic version - see COMPLETE_SETUP_GUIDE.md Step 4)

Role name: `StrandsBedrockAgentRole`

**✅ Checkpoint 4**: IAM roles created with all permissions!

---

## PART 5: CREATE ALL LAMBDA FUNCTIONS (90 minutes)

You'll create 9 Lambda functions. For each:

### Lambda Functions to Create:

1. **strands-decision-agent-lambda** (existing, from basic version)
2. **strands-quality-agent-lambda** (existing)
3. **strands-optimization-agent-lambda** (existing)
4. **strands-learning-agent-lambda** (existing)
5. **strands-compliance-agent-lambda** (NEW)
6. **strands-cost-tracking-agent-lambda** (NEW)
7. **strands-execution-lambda** (existing)
8. **strands-tracking-lambda** (NEW)
9. **strands-email-notification-lambda** (NEW)

### Step 6: Package Lambda Code

```bash
cd /home/user/strands_etl/lambda_functions

# Package existing agents (if not already done)
cd decision && zip -r decision-function.zip handler.py && cd ..
cd quality && zip -r quality-function.zip handler.py && cd ..
cd optimization && zip -r optimization-function.zip handler.py && cd ..
cd learning && zip -r learning-function.zip handler.py && cd ..
cd execution && zip -r execution-function.zip handler.py && cd ..

# Package NEW agents
cd compliance && zip -r compliance-function.zip handler.py && cd ..
cd cost_tracking && zip -r cost-function.zip handler.py && cd ..
cd tracking && zip -r tracking-function.zip handler.py && cd ..
cd email_notification && zip -r email-function.zip handler.py && cd ..
```

### Step 7: Create Each Lambda Function

For each function, in AWS Console → Lambda → Create function:

**Common settings for all**:
- Runtime: Python 3.11
- Execution role: Use existing `StrandsLambdaExecutionRoleEnhanced`
- Upload respective .zip file
- Add Bedrock invoke permission

**Specific settings**:

| Function | Memory | Timeout | Env Vars |
|----------|--------|---------|----------|
| decision-agent-lambda | 512 MB | 60s | LEARNING_BUCKET |
| quality-agent-lambda | 1024 MB | 90s | LEARNING_BUCKET |
| optimization-agent-lambda | 512 MB | 60s | LEARNING_BUCKET |
| learning-agent-lambda | 512 MB | 60s | LEARNING_BUCKET |
| **compliance-agent-lambda** | 512 MB | 60s | COMPLIANCE_TABLE |
| **cost-tracking-agent-lambda** | 512 MB | 60s | COST_TRENDS_TABLE |
| execution-lambda | 512 MB | 300s | LEARNING_BUCKET |
| **tracking-lambda** | 256 MB | 30s | JOB_EXECUTIONS_TABLE |
| **email-notification-lambda** | 256 MB | 30s | SES_REGION, FROM_EMAIL |

**✅ Checkpoint 5**: All 9 Lambda functions created!

---

## PART 6: CREATE BEDROCK AGENTS (90 minutes)

Create 7 Bedrock Agents (5 existing + 2 new):

### Agents to Create:

1. Decision Agent (existing - see COMPLETE_SETUP_GUIDE.md)
2. Quality Agent (existing)
3. Optimization Agent (existing)
4. Learning Agent (existing)
5. **Compliance Agent** (NEW)
6. **Cost Tracking Agent** (NEW)
7. Supervisor Agent (existing)

### Step 8: Create Compliance Agent

1. **Bedrock → Agents → Create Agent**
2. Agent name: `strands-compliance-agent`
3. Description: `Data governance and regulatory compliance checks`
4. Model: Claude 3.5 Sonnet v2
5. Instructions: Copy from `bedrock_agents/configs/compliance_agent.json`
6. **Add Action Group**:
   - Name: `compliance-tools`
   - Lambda: `strands-compliance-agent-lambda`
   - Schema: Upload `compliance-agent-api.json` to S3 schemas bucket first, then select
7. Click **Prepare**

### Step 9: Create Cost Tracking Agent

1. **Bedrock → Agents → Create Agent**
2. Agent name: `strands-cost-tracking-agent`
3. Description: `Cost monitoring, analysis, and optimization`
4. Model: Claude 3.5 Sonnet v2
5. Instructions: Copy from `bedrock_agents/configs/cost_tracking_agent.json`
6. **Add Action Group**:
   - Name: `cost-tracking-tools`
   - Lambda: `strands-cost-tracking-agent-lambda`
   - Schema: Upload `cost-tracking-agent-api.json` to S3 schemas bucket first, then select
7. Click **Prepare**

### Step 10: Update Supervisor Agent

Update the existing Supervisor agent to add the 2 new agents to collaboration:

1. Go to `strands-supervisor-agent`
2. Edit → Agent collaboration
3. **Add agents**:
   - strands-compliance-agent
   - strands-cost-tracking-agent
4. Update instructions to mention the new agents
5. **Prepare** agent

**✅ Checkpoint 6**: All 7 Bedrock Agents created and collaborating!

---

## PART 7: CONFIGURE EVENT-DRIVEN TRACKING (30 minutes)

### Step 11: Create EventBridge Rule for Job Tracking

1. **EventBridge → Rules → Create rule**
2. Name: `strands-job-completion-tracking`
3. Event pattern:
```json
{
  "source": ["aws.glue", "aws.emr", "aws.lambda"],
  "detail-type": ["Glue Job State Change", "EMR Step Status Change", "Lambda Function Execution"]
}
```
4. Target: `strands-tracking-lambda`
5. Create rule

### Step 12: Create EventBridge Rule for Email Notifications

1. **EventBridge → Rules → Create rule**
2. Name: `strands-job-completion-email`
3. Event pattern: Same as above
4. Target: `strands-email-notification-lambda`
5. Create rule

**✅ Checkpoint 7**: Event-driven tracking configured!

---

## PART 8: DEPLOY ENHANCED DASHBOARD (45 minutes)

### Step 13: Install Dashboard Dependencies

```bash
cd /home/user/strands_etl/dashboard

# Install all dependencies
pip install streamlit boto3 pandas plotly python-dateutil

# Install additional packages for enhanced dashboard
pip install altair pydeck
```

### Step 14: Configure Dashboard

The enhanced dashboard (`strands_production_dashboard.py`) includes:

**New Features**:
1. **Cost Dashboard** - Trends, forecasting, top offenders
2. **Compliance Dashboard** - GDPR/HIPAA status, PII detection
3. **Issue Heat Map** - Visual representation of issue frequency
4. **Quality Trends** - 5-dimension quality tracking over time
5. **Platform Comparison** - Cost, speed, quality side-by-side
6. **Real-time Metrics** - Live job status updates

### Step 15: Launch Dashboard

```bash
streamlit run strands_production_dashboard.py --server.port 8501
```

Open browser: `http://localhost:8501`

**✅ Checkpoint 8**: Enhanced dashboard running with all metrics!

---

## PART 9: TEST COMPLETE SYSTEM (60 minutes)

### Step 16: Run End-to-End Test

1. **Test Supervisor Agent** in Bedrock Console:

```
I need to run a production ETL job with these requirements:

Job: customer_order_summary
Data: 300GB of transaction data
Processing: Complex joins with 5 dimension tables
Criticality: High (daily production job)
Stakeholders: data-team@company.com, analytics-lead@company.com

Please:
1. Select the best platform
2. Check for quality issues and anti-patterns
3. Detect any compliance concerns (PII, GDPR)
4. Calculate cost breakdown
5. Provide optimization recommendations
6. Capture learning pattern
7. Send email summary to stakeholders
```

**Expected**: Comprehensive response with all 7 agents coordinating!

### Step 17: Verify Data in DynamoDB

1. Go to DynamoDB → Tables → `StrandsJobExecutions`
2. Click **Explore table items**
3. Verify execution record with all metrics

### Step 18: Check Email Notification

1. Check stakeholder email inbox
2. Verify formatted email with:
   - Job status
   - Execution metrics
   - Issues detected
   - Recommendations
   - Cost breakdown
   - Quality score
   - Compliance status

### Step 19: View in Dashboard

1. Open dashboard at `http://localhost:8501`
2. Navigate through all sections:
   - Overview (job status)
   - Execution Timeline
   - Cost Trends
   - Quality Trends
   - Issues Heat Map
   - Compliance Dashboard
   - Platform Comparison

**✅ Checkpoint 9**: End-to-end system working perfectly!

---

## 📊 Dashboard Features Guide

### Main Dashboard Sections:

1. **Overview**
   - Current job status
   - Running/succeeded/failed counts
   - Latest execution summary

2. **Execution Timeline**
   - Gantt chart of all executions
   - Color-coded by status
   - Drill-down to details

3. **Cost Analysis**
   - Daily/weekly/monthly trends
   - Cost by job type
   - Cost forecasting (30 days)
   - Top 10 most expensive jobs
   - Cost optimization suggestions

4. **Quality Metrics**
   - Overall quality score trend
   - 5-dimension breakdown:
     - Completeness
     - Accuracy
     - Consistency
     - Timeliness
     - Validity
   - Quality alerts

5. **Issues Dashboard**
   - Issue frequency heat map
   - Issues by severity
   - Top recurring issues
   - Resolution status

6. **Compliance Dashboard**
   - GDPR compliance score
   - HIPAA compliance status
   - PII detection summary
   - Audit trail
   - Compliance alerts

7. **Platform Comparison**
   - Glue vs EMR vs Lambda
   - Cost comparison
   - Performance comparison
   - Use case recommendations

8. **Agent Activity**
   - Invocations per agent
   - Average response time
   - Success rate
   - Token consumption

---

## 🔧 Configuration Files Reference

### Agent Configurations:
- `bedrock_agents/configs/decision_agent.json`
- `bedrock_agents/configs/quality_agent.json`
- `bedrock_agents/configs/optimization_agent.json`
- `bedrock_agents/configs/learning_agent.json`
- `bedrock_agents/configs/compliance_agent.json` (NEW)
- `bedrock_agents/configs/cost_tracking_agent.json` (NEW)
- `bedrock_agents/configs/supervisor_agent.json`

### API Schemas:
- `bedrock_agents/schemas/decision-agent-api.json`
- `bedrock_agents/schemas/quality-agent-api.json`
- `bedrock_agents/schemas/optimization-agent-api.json`
- `bedrock_agents/schemas/learning-agent-api.json`
- `bedrock_agents/schemas/compliance-agent-api.json` (NEW)
- `bedrock_agents/schemas/cost-tracking-agent-api.json` (NEW)

### Lambda Functions:
- `lambda_functions/decision/handler.py`
- `lambda_functions/quality/handler.py`
- `lambda_functions/optimization/handler.py`
- `lambda_functions/learning/handler.py`
- `lambda_functions/compliance/handler.py` (NEW)
- `lambda_functions/cost_tracking/handler.py` (NEW)
- `lambda_functions/tracking/handler.py` (NEW)
- `lambda_functions/email_notification/handler.py` (NEW)
- `lambda_functions/execution/handler.py`

### Dashboard:
- `dashboard/strands_production_dashboard.py` (NEW - enhanced)

### Documentation:
- `docs/DYNAMODB_SCHEMA.md` - Complete schema reference
- `docs/ARCHITECTURE.md` - System architecture
- `docs/EMAIL_TEMPLATES.md` - Email notification templates

---

## 🎉 SETUP COMPLETE!

You now have a **production-grade Strands ETL system** with:

✅ 7 autonomous agents
✅ Complete tracking in 6 DynamoDB tables
✅ Email notifications to stakeholders
✅ Advanced dashboard with 8 sections
✅ Compliance & cost tracking
✅ Issue detection & recommendations
✅ Full audit trail

---

## 📈 Success Metrics

After setup, you should see:
- ✅ All agents responding and collaborating
- ✅ Executions tracked in DynamoDB with all metrics
- ✅ Email notifications sent on job completion
- ✅ Dashboard displaying real-time data
- ✅ Cost tracking and forecasting working
- ✅ Compliance checks passing
- ✅ Issues detected and recommendations provided

---

## 💡 Best Practices

### For Production Use:

1. **Set Budget Alerts**:
   - CloudWatch alarm for DynamoDB capacity
   - Cost Explorer budget alerts
   - SES sending limit monitoring

2. **Monitor Agent Performance**:
   - Track agent invocation times
   - Monitor token consumption
   - Set alerts for failed invocations

3. **Regular Reviews**:
   - Weekly cost review
   - Monthly compliance audit
   - Quarterly agent performance optimization

4. **Data Retention**:
   - DynamoDB TTL configured (90 days for JobExecutions)
   - S3 lifecycle policies (90 days for learning vectors)
   - Compliance audit logs (7 years)

5. **Security**:
   - Rotate IAM keys regularly
   - Review SES sending patterns
   - Audit DynamoDB access logs
   - Enable CloudTrail for compliance

---

## 🆘 Troubleshooting

### Issue: DynamoDB Throttling
**Solution**: Enable auto-scaling or switch to on-demand mode

### Issue: SES Emails Not Delivered
**Solution**:
- Check SES is out of sandbox mode
- Verify email addresses
- Check SES sending limits

### Issue: Dashboard Slow
**Solution**:
- Add DynamoDB query indexes
- Implement caching layer
- Paginate large result sets

### Issue: High Costs
**Solution**:
- Review DynamoDB capacity (switch to on-demand if spiky)
- Optimize Lambda memory allocation
- Review Bedrock token usage

---

## 📞 Support

- **Architecture Questions**: See `docs/ARCHITECTURE.md`
- **DynamoDB Schema**: See `docs/DYNAMODB_SCHEMA.md`
- **Cost Details**: See `AWS_PROVISIONING_GUIDE.md`

---

**Document Version**: 1.0 (Production Agentic)
**Setup Time**: 4-5 hours
**Monthly Cost**: $726-1,495
**Branch**: `claude/strands-agentic-production-mklys5sfa8e4xuqc-u0a48`
