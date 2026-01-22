# AWS Provisioning Guide for Strands ETL - Bedrock Agents Implementation

## 📋 Document Purpose

This document provides detailed AWS resource requirements for the Strands ETL Bedrock Agents implementation. Use this to submit work orders for provisioning AWS resources.

---

## 🎯 Executive Summary

### Project: Strands ETL Multi-Agent System with AWS Bedrock (S3-Only Cost-Optimized Version)
### Purpose: Intelligent ETL orchestration with AI-powered decision making, quality assessment, and continuous learning
### Architecture: Hybrid approach using AWS Bedrock Agents for coordination + Lambda for custom ETL logic + S3 for vector storage

### Total Estimated Monthly Cost: **$500 - $1,100** (Save $350-700/month vs OpenSearch version)
- AWS Bedrock Agents: $300-600
- Lambda: $100-200
- S3: $50-100
- CloudWatch: $50-100
- Glue/EMR: Variable based on usage

**Note**: This S3-only version eliminates OpenSearch Serverless ($350-700/month) by using S3 for learning vector storage with cosine similarity search. Ideal for scenarios where vector search performance is not critical.

---

## 📦 Required AWS Services

### ✅ 1. AWS Bedrock

**Service**: Amazon Bedrock (Agents and Models)

**What We Need**:
- Access to Bedrock service in region **us-east-1** (or your preferred region)
- Model access: `anthropic.claude-3-5-sonnet-20241022-v2:0`
- Bedrock Agents enabled
- Bedrock Knowledge Bases enabled

**Quota Requirements**:
- Model invocations: 10,000 per month
- Concurrent agent sessions: 50
- Agent collaboration enabled

**Monthly Cost**: ~$300-600
- $0.003 per 1K input tokens
- $0.015 per 1K output tokens
- Estimated 50-100M tokens/month

**IAM Permissions Needed**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel",
        "bedrock:InvokeAgent",
        "bedrock:CreateAgent",
        "bedrock:CreateAgentActionGroup",
        "bedrock:CreateAgentAlias",
        "bedrock:AssociateAgentKnowledgeBase",
        "bedrock:CreateKnowledgeBase",
        "bedrock:CreateDataSource"
      ],
      "Resource": "*"
    }
  ]
}
```

**Specific Agents to Create**: 5 agents
1. Supervisor Agent (orchestrator)
2. Decision Agent (platform selection)
3. Quality Agent (SQL/code analysis)
4. Optimization Agent (performance recommendations)
5. Learning Agent (pattern capture)

---

### ✅ 2. AWS Lambda

**Service**: AWS Lambda (Python 3.11)

**What We Need**: 5 Lambda functions

| Function Name | Memory | Timeout | Concurrent Executions |
|---------------|--------|---------|----------------------|
| strands-decision-agent-lambda | 512 MB | 60s | 10 |
| strands-quality-agent-lambda | 1024 MB | 90s | 10 |
| strands-optimization-agent-lambda | 512 MB | 60s | 10 |
| strands-learning-agent-lambda | 512 MB | 60s | 10 |
| strands-execution-lambda | 512 MB | 300s | 5 |

**Dependencies**:
- Python 3.11 runtime
- Lambda Layers for: boto3, numpy, pandas

**Monthly Cost**: ~$100-200
- 1,000 executions/month per function
- Average execution time: 10-30 seconds

**IAM Permissions Needed**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction",
        "lambda:CreateFunction",
        "lambda:UpdateFunctionCode",
        "lambda:AddPermission"
      ],
      "Resource": "arn:aws:lambda:*:*:function:strands-*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:log-group:/aws/lambda/strands-*"
    }
  ]
}
```

---

### ✅ 3. Amazon S3

**Service**: Amazon S3

**What We Need**: 2 buckets

1. **strands-etl-learning** (primary data storage)
   - Purpose: Store learning vectors, quality reports, optimization recommendations
   - Versioning: Enabled
   - Encryption: AES-256
   - Lifecycle Policy:
     - Delete vectors older than 90 days
     - Delete temp data after 1 day
   - Estimated Size: 10-50 GB

2. **strands-etl-schemas** (configuration storage)
   - Purpose: Store OpenAPI schemas for Bedrock Agent action groups
   - Versioning: Enabled
   - Encryption: AES-256
   - Estimated Size: < 1 GB

**Monthly Cost**: ~$50-100
- Storage: $0.023 per GB
- Requests: Minimal

**IAM Permissions Needed**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket",
        "s3:PutBucketVersioning",
        "s3:PutLifecycleConfiguration",
        "s3:PutEncryptionConfiguration",
        "s3:PutBucketPolicy"
      ],
      "Resource": [
        "arn:aws:s3:::strands-etl-learning",
        "arn:aws:s3:::strands-etl-schemas"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::strands-etl-learning/*",
        "arn:aws:s3:::strands-etl-schemas/*"
      ]
    }
  ]
}
```

---

### ✅ 4. AWS Glue (Existing - Additional Permissions)

**Service**: AWS Glue

**What We Need**:
- Enhanced IAM role: `StrandsETLGlueRole`
- Permissions to read from Glue Data Catalog
- Permissions to write to S3 learning bucket

**Monthly Cost**: Variable (pay per job)
- DPU hours based on actual usage
- Estimated: $50-200/month

**IAM Role Updates Needed**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:CreateJob"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::strands-etl-learning/*"
    }
  ]
}
```

---

### ✅ 5. Amazon CloudWatch

**Service**: CloudWatch Logs and Metrics

**What We Need**:
- Log Groups for each Lambda function (5 total)
- Retention: 30 days
- Custom metrics for agent performance
- Dashboards for monitoring

**Monthly Cost**: ~$50-100
- Log ingestion: $0.50 per GB
- Log storage: $0.03 per GB/month
- Estimated: 20-40 GB/month

**IAM Permissions Needed**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams"
      ],
      "Resource": "arn:aws:logs:*:*:log-group:/aws/lambda/strands-*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*"
    }
  ]
}
```

---

### ✅ 6. Amazon EMR (Optional - Existing)

**Service**: Amazon EMR

**What We Need**:
- No new resources
- Existing EMR permissions sufficient
- Additional S3 access to learning bucket

---

## 🔐 IAM Roles Summary

### Roles to Create:

1. **StrandsBedrockAgentRole**
   - Used by: All 5 Bedrock Agents
   - Permissions: Bedrock, Lambda invocation, S3 (schemas)
   - Trust Policy: bedrock.amazonaws.com

2. **StrandsLambdaExecutionRole**
   - Used by: All 5 Lambda functions
   - Permissions: S3 (learning bucket), Glue, EMR, Bedrock models, CloudWatch Logs
   - Trust Policy: lambda.amazonaws.com

3. **StrandsETLGlueRole** (enhanced existing)
   - Used by: Glue jobs
   - Permissions: S3 (data + learning), Glue Data Catalog
   - Trust Policy: glue.amazonaws.com

4. **StrandsDashboardRole** (for EC2/ECS running Streamlit)
   - Used by: Dashboard application
   - Permissions: S3 read (learning bucket), Bedrock agent invocation, CloudWatch read
   - Trust Policy: ec2.amazonaws.com or ecs-tasks.amazonaws.com

---

## 📊 Resource Limits and Quotas

| Service | Resource | Required Quota | Default Quota | Action Needed |
|---------|----------|----------------|---------------|---------------|
| Bedrock | Concurrent agent sessions | 50 | 10 | Request increase |
| Lambda | Concurrent executions | 50 (total) | 1000 | No action |
| S3 | Buckets | 2 | 100 | No action |
| CloudWatch | Log groups | 10 | 20 | No action |

### Quota Increase Request:
**If needed, request quota increase for**:
- Bedrock concurrent agent sessions: 10 → 50

---

## 🌍 Regional Requirements

**Primary Region**: **us-east-1** (N. Virginia)

**Why us-east-1**:
- AWS Bedrock Agents availability
- Latest Claude models available
- Lowest latency for Bedrock API calls
- S3 high availability and durability

**Fallback Region**: us-west-2 (if us-east-1 unavailable)

---

## 💰 Total Cost Estimate

### Monthly Recurring Costs:

| Service | Estimated Monthly Cost |
|---------|----------------------|
| AWS Bedrock (Agents + Models) | $300-600 |
| Lambda (5 functions) | $100-200 |
| S3 Storage | $50-100 |
| CloudWatch Logs | $50-100 |
| Glue/EMR (variable) | $50-200 |
| **TOTAL** | **$550-1,200/month** |

**Savings vs OpenSearch Version**: **$200-400/month** (eliminates $350-700 OpenSearch Serverless costs, with slight S3 increase for vector storage)

### One-Time Setup Costs:
- CloudFormation stack creation: $0 (no charge)
- Initial data ingestion: $10-20
- Testing/development: $50-100

### Cost Optimization Recommendations:
1. Use reserved capacity for Lambda if usage is predictable
2. Implement S3 lifecycle policies (already in CloudFormation)
3. Monitor and optimize Bedrock token usage
4. Consider S3 Intelligent-Tiering for learning vectors
5. Regularly clean up old learning vectors (90-day lifecycle policy enabled)

---

## 📝 Work Order Submission Checklist

Use this checklist when submitting to admin:

### ☐ AWS Services to Enable:
- [ ] AWS Bedrock (with Agents)
- [ ] AWS Lambda (Python 3.11 runtime)
- [ ] Amazon S3 (2 new buckets)
- [ ] Amazon CloudWatch Logs

### ☐ IAM Roles to Create:
- [ ] StrandsBedrockAgentRole
- [ ] StrandsLambdaExecutionRole
- [ ] StrandsDashboardRole
- [ ] StrandsETLGlueRole (update existing)

### ☐ IAM Policies to Attach:
- [ ] Bedrock full access to BedrockAgentRole
- [ ] Lambda execution policy to LambdaExecutionRole
- [ ] S3 access policies (learning + schema buckets)

### ☐ Quota Increases to Request:
- [ ] Bedrock concurrent agent sessions: 50 (if default is 10)

### ☐ S3 Buckets to Create:
- [ ] strands-etl-learning (with lifecycle policy)
- [ ] strands-etl-schemas

### ☐ Lambda Functions:
- [ ] strands-decision-agent-lambda (512MB, 60s)
- [ ] strands-quality-agent-lambda (1024MB, 90s)
- [ ] strands-optimization-agent-lambda (512MB, 60s)
- [ ] strands-learning-agent-lambda (512MB, 60s)
- [ ] strands-execution-lambda (512MB, 300s)

### ☐ CloudWatch Log Groups:
- [ ] /aws/lambda/strands-decision-agent-lambda
- [ ] /aws/lambda/strands-quality-agent-lambda
- [ ] /aws/lambda/strands-optimization-agent-lambda
- [ ] /aws/lambda/strands-learning-agent-lambda
- [ ] /aws/lambda/strands-execution-lambda

---

## 🚀 Deployment Approach

### Option 1: Automated (Recommended)
**Use CloudFormation Template**: `infrastructure/cloudformation/strands-bedrock-agents-stack.yaml`

**Command**:
```bash
aws cloudformation create-stack \
  --stack-name strands-bedrock-agents \
  --template-body file://infrastructure/cloudformation/strands-bedrock-agents-stack.yaml \
  --parameters ParameterKey=Environment,ParameterValue=Production \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1
```

**Provisions**:
- All IAM roles
- All S3 buckets (learning + schemas)
- All Lambda functions (with placeholder code)
- CloudWatch log groups
- Proper permissions and policies
- S3 lifecycle policies for cost optimization

**Time to Complete**: 15-20 minutes

### Option 2: Manual
Follow step-by-step instructions in `IMPLEMENTATION_PLAN.md`

---

## 🔒 Security Considerations

### Data Encryption:
- **S3**: AES-256 encryption at rest (enabled in CloudFormation)
- **Bedrock**: All data encrypted in transit (TLS 1.2+)
- **Lambda**: All environment variables encrypted

### Network Security:
- All Lambda functions run in AWS managed VPC
- S3 buckets block public access
- VPC endpoints available for S3 if needed

### Access Control:
- IAM roles follow least privilege principle
- Resource-based policies restrict access
- No hardcoded credentials (use IAM roles)

### Compliance:
- GDPR: Data residency in us-east-1
- SOC 2: AWS services are SOC 2 compliant
- HIPAA: Can be enabled if required (contact AWS)

---

## 📞 Support Contacts

### AWS Support:
- **Technical Support**: AWS Support Console
- **Quota Increases**: Service Quotas Console
- **Bedrock Access**: Request via Bedrock Console

### Internal Contacts:
- **Project Lead**: [Your Name]
- **AWS Admin**: [Admin Name]
- **Security Team**: [Security Contact]

---

## 📅 Timeline

### Phase 1: Provisioning (Week 1)
- Submit work order with this document
- Admin provisions resources via CloudFormation
- Enable Bedrock and request model access

### Phase 2: Configuration (Week 2)
- Create Bedrock Agents using configs
- Deploy Lambda function code
- Create Knowledge Base
- Configure agent collaboration

### Phase 3: Testing (Week 3)
- Test each agent independently
- Test end-to-end pipeline
- Deploy dashboard
- Performance testing

### Phase 4: Production (Week 4)
- Go live with limited workloads
- Monitor performance
- Optimize costs
- Scale up

---

## ✅ Next Steps After Provisioning

Once admin completes provisioning:

1. **Verify Resources**:
   ```bash
   aws cloudformation describe-stacks --stack-name strands-bedrock-agents
   ```

2. **Deploy Lambda Code**:
   ```bash
   cd lambda_functions/decision
   zip -r function.zip .
   aws lambda update-function-code \
     --function-name strands-decision-agent-lambda \
     --zip-file fileb://function.zip
   ```

3. **Upload Schemas to S3**:
   ```bash
   aws s3 cp bedrock_agents/schemas/ s3://strands-etl-schemas/schemas/ --recursive
   ```

4. **Create Bedrock Agents**:
   - Use configs in `bedrock_agents/configs/`
   - Follow instructions in `IMPLEMENTATION_PLAN.md`

5. **Deploy Dashboard**:
   ```bash
   streamlit run dashboard/strands_dashboard.py
   ```

---

## 📖 Reference Documents

- **CloudFormation Template**: `infrastructure/cloudformation/strands-bedrock-agents-stack.yaml`
- **Implementation Plan**: `IMPLEMENTATION_PLAN.md` (to be created)
- **Lambda Functions**: `lambda_functions/` directory
- **Agent Configurations**: `bedrock_agents/configs/` directory
- **Dashboard**: `dashboard/strands_dashboard.py`

---

**Document Version**: 2.0 (S3-Only Cost-Optimized)
**Last Updated**: 2024-01-21
**Prepared For**: AWS Admin - Work Order Submission
**Estimated Budget**: $550-1,200/month (saves $200-400/month vs OpenSearch version)

