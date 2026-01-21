# Strands ETL - AWS Bedrock Agents Implementation Plan

## 📋 Overview

This document provides detailed, step-by-step instructions for implementing the Strands ETL system using AWS Bedrock Agents after AWS resources have been provisioned.

**Prerequisites**: AWS resources provisioned via CloudFormation stack (see `AWS_PROVISIONING_GUIDE.md`)

---

## 🎯 Implementation Phases

### Phase 1: Infrastructure Validation (Day 1)
### Phase 2: Lambda Deployment (Days 2-3)
### Phase 3: Bedrock Agent Creation (Days 4-6)
### Phase 4: Knowledge Base Setup (Day 7)
### Phase 5: Agent Collaboration (Days 8-9)
### Phase 6: Dashboard Deployment (Day 10)
### Phase 7: Testing & Validation (Days 11-12)
### Phase 8: Production Cutover (Days 13-14)

---

## Phase 1: Infrastructure Validation

### ✅ Step 1.1: Verify CloudFormation Stack

```bash
# Check stack status
aws cloudformation describe-stacks \
  --stack-name strands-bedrock-agents \
  --query 'Stacks[0].StackStatus'

# Expected output: CREATE_COMPLETE

# Get all outputs
aws cloudformation describe-stacks \
  --stack-name strands-bedrock-agents \
  --query 'Stacks[0].Outputs' \
  --output table
```

**Expected Outputs**:
- LearningBucketName: strands-etl-learning
- SchemaBucketName: strands-etl-schemas
- OpenSearchCollectionEndpoint: xxxxx.us-east-1.aoss.amazonaws.com
- BedrockAgentRoleArn: arn:aws:iam::...
- 5 Lambda ARNs

### ✅ Step 1.2: Verify S3 Buckets

```bash
# List buckets
aws s3 ls | grep strands

# Should see:
# strands-etl-learning
# strands-etl-schemas

# Verify lifecycle policies
aws s3api get-bucket-lifecycle-configuration \
  --bucket strands-etl-learning
```

### ✅ Step 1.3: Verify OpenSearch Collection

```bash
# List collections
aws opensearchserverless list-collections \
  --query 'collectionSummaries[?name==`strands-learning-vectors`]'

# Get collection details
aws opensearchserverless batch-get-collection \
  --names strands-learning-vectors
```

### ✅ Step 1.4: Verify IAM Roles

```bash
# Check Bedrock Agent Role
aws iam get-role --role-name StrandsBedrockAgentRole

# Check Lambda Execution Role
aws iam get-role --role-name StrandsLambdaExecutionRole

# Check Glue Role
aws iam get-role --role-name StrandsETLGlueRole
```

**✓ Validation Complete**: All resources exist and are properly configured

---

## Phase 2: Lambda Deployment

### ✅ Step 2.1: Prepare Lambda Deployment Packages

```bash
cd /home/user/strands_etl

# Decision Agent Lambda
cd lambda_functions/decision
pip install -r requirements.txt -t .
zip -r decision-function.zip .
cd ../..

# Quality Agent Lambda
cd lambda_functions/quality
pip install -r requirements.txt -t .
zip -r quality-function.zip .
cd ../..

# Optimization Agent Lambda
cd lambda_functions/optimization
pip install -r requirements.txt -t .
zip -r optimization-function.zip .
cd ../..

# Learning Agent Lambda
cd lambda_functions/learning
pip install -r requirements.txt -t .
zip -r learning-function.zip .
cd ../..

# Execution Lambda
cd lambda_functions/execution
pip install -r requirements.txt -t .
zip -r execution-function.zip .
cd ../..
```

### ✅ Step 2.2: Deploy Lambda Functions

```bash
# Deploy Decision Agent
aws lambda update-function-code \
  --function-name strands-decision-agent-lambda \
  --zip-file fileb://lambda_functions/decision/decision-function.zip

# Deploy Quality Agent
aws lambda update-function-code \
  --function-name strands-quality-agent-lambda \
  --zip-file fileb://lambda_functions/quality/quality-function.zip

# Deploy Optimization Agent
aws lambda update-function-code \
  --function-name strands-optimization-agent-lambda \
  --zip-file fileb://lambda_functions/optimization/optimization-function.zip

# Deploy Learning Agent
aws lambda update-function-code \
  --function-name strands-learning-agent-lambda \
  --zip-file fileb://lambda_functions/learning/learning-function.zip

# Deploy Execution Lambda
aws lambda update-function-code \
  --function-name strands-execution-lambda \
  --zip-file fileb://lambda_functions/execution/execution-function.zip
```

### ✅ Step 2.3: Test Lambda Functions

```bash
# Test Decision Agent
aws lambda invoke \
  --function-name strands-decision-agent-lambda \
  --payload '{"apiPath": "/get_platform_statistics", "httpMethod": "GET"}' \
  response.json

cat response.json

# Expected: {"platform_scores": {...}}
```

**✓ Lambda Deployment Complete**: All 5 functions deployed and tested

---

## Phase 3: Bedrock Agent Creation

### ✅ Step 3.1: Upload API Schemas to S3

```bash
# Upload all schemas
aws s3 cp bedrock_agents/schemas/ \
  s3://strands-etl-schemas/schemas/ \
  --recursive

# Verify upload
aws s3 ls s3://strands-etl-schemas/schemas/
```

### ✅ Step 3.2: Get Lambda ARNs and Role ARN

```bash
# Get Lambda ARNs (save these)
DECISION_LAMBDA_ARN=$(aws lambda get-function --function-name strands-decision-agent-lambda --query 'Configuration.FunctionArn' --output text)
QUALITY_LAMBDA_ARN=$(aws lambda get-function --function-name strands-quality-agent-lambda --query 'Configuration.FunctionArn' --output text)
OPTIMIZATION_LAMBDA_ARN=$(aws lambda get-function --function-name strands-optimization-agent-lambda --query 'Configuration.FunctionArn' --output text)
LEARNING_LAMBDA_ARN=$(aws lambda get-function --function-name strands-learning-agent-lambda --query 'Configuration.FunctionArn' --output text)
EXECUTION_LAMBDA_ARN=$(aws lambda get-function --function-name strands-execution-lambda --query 'Configuration.FunctionArn' --output text)

# Get Bedrock Agent Role ARN
BEDROCK_ROLE_ARN=$(aws iam get-role --role-name StrandsBedrockAgentRole --query 'Role.Arn' --output text)

echo "Decision Lambda: $DECISION_LAMBDA_ARN"
echo "Quality Lambda: $QUALITY_LAMBDA_ARN"
echo "Optimization Lambda: $OPTIMIZATION_LAMBDA_ARN"
echo "Learning Lambda: $LEARNING_LAMBDA_ARN"
echo "Execution Lambda: $EXECUTION_LAMBDA_ARN"
echo "Bedrock Role: $BEDROCK_ROLE_ARN"
```

### ✅ Step 3.3: Create Decision Agent

**Using AWS CLI**:

```bash
# Create Decision Agent
aws bedrock-agent create-agent \
  --agent-name strands-decision-agent \
  --agent-resource-role-arn $BEDROCK_ROLE_ARN \
  --foundation-model anthropic.claude-3-5-sonnet-20241022-v2:0 \
  --instruction "$(cat bedrock_agents/configs/decision_agent.json | jq -r '.instruction')" \
  --description "Platform selection agent that analyzes workload characteristics" \
  --idle-session-ttl-in-seconds 600

# Save agent ID
DECISION_AGENT_ID=$(aws bedrock-agent list-agents --query 'agentSummaries[?agentName==`strands-decision-agent`].agentId' --output text)

echo "Decision Agent ID: $DECISION_AGENT_ID"
```

**Using AWS Console** (Alternative - Recommended for first-time):

1. Navigate to: AWS Console → Bedrock → Agents
2. Click "Create Agent"
3. Fill in details:
   - Agent name: `strands-decision-agent`
   - Model: `anthropic.claude-3-5-sonnet-20241022-v2:0`
   - Instructions: Copy from `bedrock_agents/configs/decision_agent.json`
   - IAM Role: Select `StrandsBedrockAgentRole`

4. Add Action Group:
   - Name: `platform-selection-tools`
   - Lambda function: `strands-decision-agent-lambda`
   - API Schema: `s3://strands-etl-schemas/schemas/decision-agent-api.json`

5. Click "Create"

### ✅ Step 3.4: Create Quality Agent

```bash
# Create Quality Agent
aws bedrock-agent create-agent \
  --agent-name strands-quality-agent \
  --agent-resource-role-arn $BEDROCK_ROLE_ARN \
  --foundation-model anthropic.claude-3-5-sonnet-20241022-v2:0 \
  --instruction "$(cat bedrock_agents/configs/quality_agent.json | jq -r '.instruction')" \
  --description "Quality assessment agent for SQL, PySpark, and NL analysis" \
  --idle-session-ttl-in-seconds 600

# Save agent ID
QUALITY_AGENT_ID=$(aws bedrock-agent list-agents --query 'agentSummaries[?agentName==`strands-quality-agent`].agentId' --output text)

echo "Quality Agent ID: $QUALITY_AGENT_ID"
```

### ✅ Step 3.5: Create Optimization Agent

*(Similar process - create agent and attach action group)*

### ✅ Step 3.6: Create Learning Agent

*(Similar process - create agent and attach action group)*

### ✅ Step 3.7: Create Supervisor Agent

**This is the most important agent - it coordinates all others**

```bash
# Create Supervisor Agent
aws bedrock-agent create-agent \
  --agent-name strands-supervisor-agent \
  --agent-resource-role-arn $BEDROCK_ROLE_ARN \
  --foundation-model anthropic.claude-3-5-sonnet-20241022-v2:0 \
  --instruction "$(cat bedrock_agents/configs/supervisor_agent.json | jq -r '.instruction')" \
  --description "Main supervisor for Strands ETL multi-agent orchestration" \
  --idle-session-ttl-in-seconds 1200

SUPERVISOR_AGENT_ID=$(aws bedrock-agent list-agents --query 'agentSummaries[?agentName==`strands-supervisor-agent`].agentId' --output text)

echo "Supervisor Agent ID: $SUPERVISOR_AGENT_ID"
```

**✓ Agent Creation Complete**: 5 agents created

---

## Phase 4: Knowledge Base Setup

### ✅ Step 4.1: Create Knowledge Base

```bash
# Create Knowledge Base
aws bedrock-agent create-knowledge-base \
  --name strands-learning-vectors \
  --description "Historical ETL execution patterns and learning vectors" \
  --role-arn $BEDROCK_ROLE_ARN \
  --knowledge-base-configuration '{
    "type": "VECTOR",
    "vectorKnowledgeBaseConfiguration": {
      "embeddingModelArn": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1"
    }
  }' \
  --storage-configuration '{
    "type": "OPENSEARCH_SERVERLESS",
    "opensearchServerlessConfiguration": {
      "collectionArn": "'$(aws opensearchserverless batch-get-collection --names strands-learning-vectors --query 'collectionDetails[0].arn' --output text)'",
      "vectorIndexName": "learning-vectors",
      "fieldMapping": {
        "vectorField": "embedding",
        "textField": "text",
        "metadataField": "metadata"
      }
    }
  }'

# Save Knowledge Base ID
KB_ID=$(aws bedrock-agent list-knowledge-bases --query 'knowledgeBaseSummaries[?name==`strands-learning-vectors`].knowledgeBaseId' --output text)

echo "Knowledge Base ID: $KB_ID"
```

### ✅ Step 4.2: Create Data Source (S3)

```bash
# Create S3 Data Source
aws bedrock-agent create-data-source \
  --knowledge-base-id $KB_ID \
  --name s3-learning-vectors \
  --description "S3 bucket containing learning vectors" \
  --data-source-configuration '{
    "type": "S3",
    "s3Configuration": {
      "bucketArn": "arn:aws:s3:::strands-etl-learning",
      "inclusionPrefixes": ["learning/vectors/"]
    }
  }'
```

### ✅ Step 4.3: Create OpenSearch Index

```bash
# Get OpenSearch endpoint
OPENSEARCH_ENDPOINT=$(aws opensearchserverless batch-get-collection \
  --names strands-learning-vectors \
  --query 'collectionDetails[0].collectionEndpoint' \
  --output text | sed 's/https:\/\///')

# Create index (using Python script)
python3 <<EOF
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3

credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    'us-east-1',
    'aoss',
    session_token=credentials.token
)

client = OpenSearch(
    hosts=[{'host': '$OPENSEARCH_ENDPOINT', 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

# Create index with vector mapping
index_body = {
    "settings": {
        "index.knn": True
    },
    "mappings": {
        "properties": {
            "embedding": {
                "type": "knn_vector",
                "dimension": 4
            },
            "text": {
                "type": "text"
            },
            "metadata": {
                "type": "object"
            }
        }
    }
}

response = client.indices.create('learning-vectors', body=index_body)
print(response)
EOF
```

### ✅ Step 4.4: Associate Knowledge Base with Agents

```bash
# Associate with Decision Agent
aws bedrock-agent associate-agent-knowledge-base \
  --agent-id $DECISION_AGENT_ID \
  --agent-version DRAFT \
  --knowledge-base-id $KB_ID \
  --description "Historical execution patterns for platform selection"

# Prepare Decision Agent
aws bedrock-agent prepare-agent \
  --agent-id $DECISION_AGENT_ID

# Repeat for Quality, Optimization, and Learning agents
```

**✓ Knowledge Base Setup Complete**: Vector database ready for learning

---

## Phase 5: Agent Collaboration Setup

### ✅ Step 5.1: Create Agent Aliases

```bash
# Create alias for each agent
for AGENT_ID in $DECISION_AGENT_ID $QUALITY_AGENT_ID $OPTIMIZATION_AGENT_ID $LEARNING_AGENT_ID; do
  aws bedrock-agent create-agent-alias \
    --agent-id $AGENT_ID \
    --agent-alias-name production \
    --description "Production alias"
done

# Create supervisor alias
aws bedrock-agent create-agent-alias \
  --agent-id $SUPERVISOR_AGENT_ID \
  --agent-alias-name production \
  --description "Production supervisor"
```

### ✅ Step 5.2: Configure Agent Collaboration

**Note**: Agent collaboration is configured in supervisor_agent.json. The supervisor will invoke other agents using their aliases.

**Prepare Supervisor Agent**:

```bash
# Prepare supervisor with collaboration
aws bedrock-agent prepare-agent \
  --agent-id $SUPERVISOR_AGENT_ID

# This makes the supervisor ready to coordinate other agents
```

### ✅ Step 5.3: Test Agent Collaboration

```bash
# Test supervisor can invoke sub-agents
aws bedrock-agent-runtime invoke-agent \
  --agent-id $SUPERVISOR_AGENT_ID \
  --agent-alias-id $(aws bedrock-agent list-agent-aliases --agent-id $SUPERVISOR_AGENT_ID --query 'agentAliasSummaries[?agentAliasName==`production`].agentAliasId' --output text) \
  --session-id test-session-1 \
  --input-text "Which platform should I use for high-volume customer analytics?" \
  --output text

# Expected: Supervisor delegates to Decision Agent and returns platform recommendation
```

**✓ Agent Collaboration Complete**: Supervisor can coordinate all agents

---

## Phase 6: Dashboard Deployment

### ✅ Step 6.1: Install Dashboard Dependencies

```bash
cd /home/user/strands_etl/dashboard

# Install requirements
pip install streamlit boto3 pandas plotly

# Or use requirements file
pip install -r requirements.txt
```

### ✅ Step 6.2: Configure Dashboard Secrets

```bash
# Create Streamlit secrets file
mkdir -p ~/.streamlit

cat > ~/.streamlit/secrets.toml <<EOF
SUPERVISOR_AGENT_ID = "$SUPERVISOR_AGENT_ID"
SUPERVISOR_ALIAS_ID = "$(aws bedrock-agent list-agent-aliases --agent-id $SUPERVISOR_AGENT_ID --query 'agentAliasSummaries[?agentAliasName==`production`].agentAliasId' --output text)"
AWS_REGION = "us-east-1"
LEARNING_BUCKET = "strands-etl-learning"
EOF
```

### ✅ Step 6.3: Run Dashboard Locally (Testing)

```bash
cd /home/user/strands_etl

streamlit run dashboard/strands_dashboard.py

# Dashboard will open at http://localhost:8501
```

### ✅ Step 6.4: Deploy Dashboard to EC2/ECS

**Option A: EC2 Deployment**

```bash
# On EC2 instance with StrandsDashboardRole attached

# Install dependencies
sudo yum install -y python3.11
pip3.11 install streamlit boto3 pandas plotly

# Copy dashboard files
scp -r dashboard/ ec2-user@<instance-ip>:~/

# Run as service
streamlit run dashboard/strands_dashboard.py --server.port 80
```

**Option B: ECS/Fargate Deployment** (Recommended for production)

1. Create Dockerfile:
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY dashboard/ /app/
COPY requirements.txt /app/

RUN pip install -r requirements.txt

EXPOSE 8501

CMD ["streamlit", "run", "strands_dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

2. Build and push to ECR
3. Create ECS task definition
4. Deploy to Fargate

**✓ Dashboard Deployed**: Accessible at configured URL

---

## Phase 7: Testing & Validation

### ✅ Step 7.1: Test Individual Agents

**Test Decision Agent**:
```bash
aws bedrock-agent-runtime invoke-agent \
  --agent-id $DECISION_AGENT_ID \
  --agent-alias-id <production-alias-id> \
  --session-id test-decision-1 \
  --input-text "Analyze workload: high data volume, complex transformations, critical business process" \
  --output text
```

**Expected Output**: Platform recommendation with confidence score

**Test Quality Agent**:
```bash
aws bedrock-agent-runtime invoke-agent \
  --agent-id $QUALITY_AGENT_ID \
  --agent-alias-id <production-alias-id> \
  --session-id test-quality-1 \
  --input-text "Analyze this SQL: SELECT * FROM customers WHERE UPPER(name) = 'JOHN'" \
  --output text
```

**Expected Output**: Quality issues detected (SELECT *, function on column)

### ✅ Step 7.2: Test End-to-End Pipeline

```bash
# Full pipeline through supervisor
aws bedrock-agent-runtime invoke-agent \
  --agent-id $SUPERVISOR_AGENT_ID \
  --agent-alias-id <production-alias-id> \
  --session-id test-pipeline-1 \
  --input-text "Execute ETL pipeline for customer order analytics: high volume, complex joins, quality checks required" \
  --output text
```

**Expected Flow**:
1. Supervisor analyzes request
2. Delegates to Decision Agent → Platform selected
3. Executes job (via Execution Lambda)
4. Delegates to Quality Agent → Quality assessed
5. Delegates to Optimization Agent → Recommendations generated
6. Delegates to Learning Agent → Pattern captured
7. Returns aggregated results

### ✅ Step 7.3: Verify Learning Vector Storage

```bash
# Check if learning vectors are being stored
aws s3 ls s3://strands-etl-learning/learning/vectors/learning/

# Should see JSON files with timestamps

# Read one vector
aws s3 cp s3://strands-etl-learning/learning/vectors/learning/<vector-id>.json - | jq .
```

### ✅ Step 7.4: Verify Knowledge Base Ingestion

```bash
# Start ingestion job
aws bedrock-agent start-ingestion-job \
  --knowledge-base-id $KB_ID \
  --data-source-id <data-source-id>

# Check ingestion status
aws bedrock-agent list-ingestion-jobs \
  --knowledge-base-id $KB_ID \
  --data-source-id <data-source-id>
```

### ✅ Step 7.5: Dashboard Verification

1. Open dashboard URL
2. Verify metrics displayed
3. Test Agent Console (send test request)
4. Verify execution timeline shows data
5. Check platform comparison charts

**✓ Testing Complete**: All components validated

---

## Phase 8: Production Cutover

### ✅ Step 8.1: Update Existing Code

**Update Original Orchestrator** (Optional - for backward compatibility):

```python
# In orchestrator/strands_orchestrator.py
# Add import at top:
import boto3
bedrock_runtime = boto3.client('bedrock-agent-runtime')

# Add method:
def orchestrate_pipeline_bedrock(self, user_request: str, config_path: str):
    """Use Bedrock Agents for orchestration"""
    response = bedrock_runtime.invoke_agent(
        agentId='<SUPERVISOR_AGENT_ID>',
        agentAliasId='<PRODUCTION_ALIAS_ID>',
        sessionId=str(uuid.uuid4()),
        inputText=user_request
    )

    # Stream and return response
    result = ""
    for event in response['completion']:
        if 'chunk' in event:
            result += event['chunk']['bytes'].decode('utf-8')

    return json.loads(result)
```

### ✅ Step 8.2: Gradual Rollout

**Week 1**: 10% of traffic
- Route 10% of pipelines through Bedrock Agents
- Monitor performance and errors

**Week 2**: 25% of traffic
**Week 3**: 50% of traffic
**Week 4**: 100% of traffic

### ✅ Step 8.3: Monitoring Setup

```bash
# Create CloudWatch Dashboard
aws cloudwatch put-dashboard \
  --dashboard-name Strands-ETL-Agents \
  --dashboard-body file://monitoring/cloudwatch-dashboard.json
```

**Key Metrics to Monitor**:
- Agent invocation count
- Agent error rate
- Average response time
- Learning vector count
- Quality scores over time
- Platform distribution

### ✅ Step 8.4: Set Up Alerts

```bash
# Create SNS topic for alerts
aws sns create-topic --name strands-etl-alerts

# Create alarm for high error rate
aws cloudwatch put-metric-alarm \
  --alarm-name strands-agent-errors \
  --alarm-description "Alert when agent error rate > 5%" \
  --metric-name Errors \
  --namespace AWS/Bedrock \
  --statistic Average \
  --period 300 \
  --threshold 0.05 \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 2 \
  --alarm-actions <SNS-TOPIC-ARN>
```

**✓ Production Cutover Complete**: System is live!

---

## 🔧 Troubleshooting

### Issue: Agent Returns Error

**Check**:
1. Agent prepared? `aws bedrock-agent get-agent --agent-id <id>`
2. Lambda permissions? Check IAM policies
3. Lambda logs: `aws logs tail /aws/lambda/strands-<agent>-lambda --follow`

### Issue: Learning Vectors Not Storing

**Check**:
1. S3 permissions for Lambda role
2. Lambda logs for errors
3. S3 bucket policy

### Issue: Knowledge Base Not Finding Similar Workloads

**Check**:
1. Ingestion job completed? `aws bedrock-agent list-ingestion-jobs`
2. OpenSearch index exists? Use Python script from Phase 4
3. Embedding dimension correct? (should be 4)

### Issue: Dashboard Not Showing Data

**Check**:
1. IAM role attached to EC2/ECS has S3 read permissions
2. Secrets configured correctly
3. S3 bucket has data: `aws s3 ls s3://strands-etl-learning/learning/vectors/`

---

## 📊 Success Criteria

### ✅ Technical:
- [ ] All 5 agents created and prepared
- [ ] Agent collaboration working
- [ ] Knowledge Base ingesting data
- [ ] Dashboard displaying metrics
- [ ] End-to-end pipeline executes successfully
- [ ] Learning vectors stored and retrieved

### ✅ Performance:
- [ ] Agent response time < 10 seconds
- [ ] Quality score > 0.90
- [ ] Platform selection confidence > 0.80
- [ ] Learning vectors accumulating (> 10 per day)

### ✅ Operational:
- [ ] CloudWatch dashboards configured
- [ ] Alerts set up and tested
- [ ] Documentation complete
- [ ] Team trained on new system

---

## 📚 Reference Commands

### Useful Bedrock Agent Commands:

```bash
# List all agents
aws bedrock-agent list-agents

# Get agent details
aws bedrock-agent get-agent --agent-id <id>

# Update agent instruction
aws bedrock-agent update-agent --agent-id <id> --instruction "new instruction"

# Delete agent
aws bedrock-agent delete-agent --agent-id <id>

# List agent versions
aws bedrock-agent list-agent-versions --agent-id <id>
```

### Useful Lambda Commands:

```bash
# Get function details
aws lambda get-function --function-name <name>

# View logs
aws logs tail /aws/lambda/<function-name> --follow

# Update environment variable
aws lambda update-function-configuration \
  --function-name <name> \
  --environment Variables={KEY=VALUE}
```

### Useful S3 Commands:

```bash
# List learning vectors
aws s3 ls s3://strands-etl-learning/learning/vectors/ --recursive

# Download vector
aws s3 cp s3://strands-etl-learning/learning/vectors/learning/<id>.json .

# Count vectors
aws s3 ls s3://strands-etl-learning/learning/vectors/learning/ | wc -l
```

---

## 🎓 Next Steps

1. **Optimize**: Review agent performance and tune instructions
2. **Scale**: Increase concurrent executions if needed
3. **Enhance**: Add custom agents for specific use cases
4. **Monitor**: Set up regular reviews of quality metrics
5. **Iterate**: Continuously improve based on learning data

---

**Document Version**: 1.0
**Implementation Time**: 10-14 days
**Team Size**: 2-3 engineers
**Success Rate**: High (with proper AWS resource provisioning)

