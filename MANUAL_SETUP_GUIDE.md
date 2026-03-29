# Strands ETL - Manual Setup Guide (Step-by-Step)

## 📖 About This Guide

This guide provides **beginner-friendly, step-by-step instructions** to manually set up the Strands ETL AWS Bedrock Agents implementation. No CloudFormation or automation required - just follow along!

**Prerequisites**:
- AWS Account with access to: Bedrock, Lambda, S3, IAM, CloudWatch
- AWS Console access
- Basic familiarity with AWS Console navigation

**Total Setup Time**: 3-4 hours (spread over multiple sessions if needed)

---

## 📋 Setup Overview

We'll complete these tasks in order:

1. **Phase 1**: Create S3 Buckets (15 minutes)
2. **Phase 2**: Create IAM Roles (30 minutes)
3. **Phase 3**: Create Lambda Functions (60 minutes)
4. **Phase 4**: Enable Bedrock & Create Agents (60 minutes)
5. **Phase 5**: Test the System (30 minutes)
6. **Phase 6**: Deploy Dashboard (30 minutes)

---

# PHASE 1: Create S3 Buckets (15 minutes)

## Step 1.1: Create Learning Bucket

### Open S3 Console
1. Log into AWS Console
2. In the search bar at the top, type **"S3"**
3. Click on **"S3"** service

### Create the Bucket
1. Click the orange **"Create bucket"** button
2. Fill in the form:
   - **Bucket name**: `strands-etl-learning`
   - **AWS Region**: Select `us-east-1` (N. Virginia)
   - **Object Ownership**: Leave as "ACLs disabled"
   - **Block Public Access settings**: ✅ Keep all 4 checkboxes checked (block all public access)
   - **Bucket Versioning**: Select **"Enable"**
   - **Default encryption**: Select **"Server-side encryption with Amazon S3 managed keys (SSE-S3)"**
3. Scroll to the bottom and click **"Create bucket"**

### Create Folder Structure
1. Click on the bucket name `strands-etl-learning`
2. Click **"Create folder"** button
3. Enter folder name: `learning`
4. Click **"Create folder"**
5. Click on the `learning` folder you just created
6. Click **"Create folder"** again
7. Enter folder name: `vectors`
8. Click **"Create folder"**
9. Click on the `vectors` folder
10. Click **"Create folder"** again
11. Enter folder name: `learning`
12. Click **"Create folder"**

**Result**: You should now have this structure: `strands-etl-learning/learning/vectors/learning/`

### Add Lifecycle Policy
1. Go back to the S3 Console (click "Amazon S3" in the breadcrumb at top)
2. Click on `strands-etl-learning` bucket
3. Click on the **"Management"** tab
4. Scroll down to **"Lifecycle rules"**
5. Click **"Create lifecycle rule"**
6. Fill in:
   - **Lifecycle rule name**: `delete-old-vectors`
   - **Rule scope**: Select "Limit the scope of this rule using one or more filters"
   - **Prefix**: `learning/vectors/`
   - ✅ Check **"I acknowledge that this lifecycle rule will apply to all objects with the prefix learning/vectors/"**
7. Under **"Lifecycle rule actions"**, check:
   - ✅ **"Expire current versions of objects"**
8. Under **"Expire current versions of objects"**:
   - **Days after object creation**: `90`
9. Click **"Create rule"**

**✅ Checkpoint**: Your learning bucket is ready with automatic cleanup after 90 days!

---

## Step 1.2: Create Schemas Bucket

### Create the Bucket
1. In S3 Console, click **"Create bucket"**
2. Fill in:
   - **Bucket name**: `strands-etl-schemas`
   - **AWS Region**: `us-east-1`
   - **Block Public Access**: ✅ Keep all checked
   - **Bucket Versioning**: **"Enable"**
   - **Default encryption**: **SSE-S3**
3. Click **"Create bucket"**

### Upload Schema Files
1. Click on the `strands-etl-schemas` bucket
2. Click **"Create folder"**
3. Folder name: `schemas`
4. Click **"Create folder"**
5. Click on the `schemas` folder
6. Click **"Upload"**
7. Click **"Add files"**
8. Navigate to your local `bedrock_agents/schemas/` directory
9. Select all `.json` files:
   - `decision-agent-api.json`
   - `quality-agent-api.json`
   - `optimization-agent-api.json`
   - `learning-agent-api.json`
10. Click **"Upload"**

**✅ Checkpoint**: Both S3 buckets are created and configured!

---

# PHASE 2: Create IAM Roles (30 minutes)

## Step 2.1: Create Lambda Execution Role

### Navigate to IAM
1. In AWS Console search bar, type **"IAM"**
2. Click on **"IAM"** service
3. In the left sidebar, click **"Roles"**
4. Click the orange **"Create role"** button

### Create the Role
1. **Select trusted entity**:
   - Trust entity type: **"AWS service"**
   - Use case: Select **"Lambda"**
   - Click **"Next"**

2. **Add permissions**:
   - In the search box, type: `AWSLambdaBasicExecutionRole`
   - ✅ Check the box next to **"AWSLambdaBasicExecutionRole"**
   - Click **"Next"**

3. **Name and create**:
   - **Role name**: `StrandsLambdaExecutionRole`
   - **Description**: `Execution role for Strands ETL Lambda functions`
   - Click **"Create role"**

### Add Additional Permissions
1. Find your newly created role in the list and click on `StrandsLambdaExecutionRole`
2. Click on **"Add permissions"** dropdown → **"Create inline policy"**
3. Click on the **"JSON"** tab
4. Replace the content with:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::strands-etl-learning/*",
        "arn:aws:s3:::strands-etl-learning",
        "arn:aws:s3:::strands-etl-schemas/*",
        "arn:aws:s3:::strands-etl-schemas"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "emr:RunJobFlow",
        "emr:DescribeCluster",
        "emr:ListClusters"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel"
      ],
      "Resource": "*"
    }
  ]
}
```

5. Click **"Next"**
6. **Policy name**: `StrandsLambdaCustomPolicy`
7. Click **"Create policy"**

**✅ Checkpoint**: Lambda execution role created with S3, Glue, EMR, and Bedrock permissions!

---

## Step 2.2: Create Bedrock Agent Role

### Create the Role
1. In IAM Console, click **"Roles"** → **"Create role"**
2. **Select trusted entity**:
   - Trust entity type: **"AWS service"**
   - Use case: Scroll down and select **"Bedrock"**
   - Under "Select your use case", choose **"Bedrock - Agents"**
   - Click **"Next"**

3. **Add permissions**:
   - Click **"Next"** (we'll add custom policies in the next step)

4. **Name and create**:
   - **Role name**: `StrandsBedrockAgentRole`
   - **Description**: `Service role for Strands ETL Bedrock Agents`
   - Click **"Create role"**

### Add Custom Permissions
1. Click on the newly created `StrandsBedrockAgentRole`
2. Click **"Add permissions"** → **"Create inline policy"**
3. Click the **"JSON"** tab
4. Paste:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel"
      ],
      "Resource": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-5-sonnet-20241022-v2:0"
    },
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": "arn:aws:lambda:us-east-1:*:function:strands-*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::strands-etl-schemas/*",
        "arn:aws:s3:::strands-etl-schemas"
      ]
    }
  ]
}
```

5. Click **"Next"**
6. **Policy name**: `StrandsBedrockAgentCustomPolicy`
7. Click **"Create policy"**

**✅ Checkpoint**: Bedrock Agent role created with Lambda invocation and S3 schema access!

---

# PHASE 3: Create Lambda Functions (60 minutes)

## Step 3.1: Prepare Lambda Code Packages

**On your local machine**, open a terminal/command prompt:

### Package Decision Agent Lambda

```bash
cd /home/user/strands_etl/lambda_functions/decision

# Create deployment package
zip -r decision-function.zip handler.py

# Verify the zip was created
ls -lh decision-function.zip
```

**Expected output**: You should see `decision-function.zip` (around 5-10 KB)

**Note**: We don't need to include dependencies (boto3, numpy) as they're already available in the Lambda Python 3.11 runtime for numpy, and boto3 is built-in. However, if you encounter issues, you can install them locally first:

```bash
pip install -t . boto3 numpy
zip -r decision-function.zip .
```

---

## Step 3.2: Create Decision Agent Lambda Function

### Navigate to Lambda Console
1. In AWS Console search bar, type **"Lambda"**
2. Click on **"Lambda"** service
3. Click the orange **"Create function"** button

### Create Function
1. Select **"Author from scratch"**
2. Fill in:
   - **Function name**: `strands-decision-agent-lambda`
   - **Runtime**: Select **"Python 3.11"**
   - **Architecture**: **x86_64**
   - **Permissions**:
     - Expand "Change default execution role"
     - Select **"Use an existing role"**
     - **Existing role**: Select `StrandsLambdaExecutionRole` from dropdown
3. Click **"Create function"**

### Upload Code
1. In the function page, scroll down to **"Code source"** section
2. Click **"Upload from"** dropdown → **".zip file"**
3. Click **"Upload"**
4. Select the `decision-function.zip` file you created
5. Click **"Save"**

### Configure Environment Variables
1. Click on the **"Configuration"** tab
2. Click on **"Environment variables"** in the left menu
3. Click **"Edit"**
4. Click **"Add environment variable"**
5. Add:
   - **Key**: `LEARNING_BUCKET`
   - **Value**: `strands-etl-learning`
6. Click **"Add environment variable"** again
7. Add:
   - **Key**: `AWS_REGION`
   - **Value**: `us-east-1`
8. Click **"Save"**

### Configure Function Settings
1. Still in **"Configuration"** tab, click on **"General configuration"**
2. Click **"Edit"**
3. Set:
   - **Memory**: `512 MB`
   - **Timeout**: `1 min 0 sec`
4. Click **"Save"**

**✅ Checkpoint**: Decision Agent Lambda function created!

---

## Step 3.3: Create Remaining Lambda Functions

**Repeat Step 3.2** for each of the following functions with these variations:

### Quality Agent Lambda
- **Function name**: `strands-quality-agent-lambda`
- **Code**: Package from `lambda_functions/quality/` (if exists, otherwise use placeholder)
- **Memory**: `1024 MB`
- **Timeout**: `1 min 30 sec`
- **Environment variables**:
  - `LEARNING_BUCKET`: `strands-etl-learning`

### Optimization Agent Lambda
- **Function name**: `strands-optimization-agent-lambda`
- **Code**: Package from `lambda_functions/optimization/` (if exists)
- **Memory**: `512 MB`
- **Timeout**: `1 min 0 sec`
- **Environment variables**:
  - `LEARNING_BUCKET`: `strands-etl-learning`

### Learning Agent Lambda
- **Function name**: `strands-learning-agent-lambda`
- **Code**: Package from `lambda_functions/learning/` (if exists)
- **Memory**: `512 MB`
- **Timeout**: `1 min 0 sec`
- **Environment variables**:
  - `LEARNING_BUCKET`: `strands-etl-learning`

### Execution Lambda
- **Function name**: `strands-execution-lambda`
- **Code**: Package from `lambda_functions/execution/` (if exists)
- **Memory**: `512 MB`
- **Timeout**: `5 min 0 sec`
- **Environment variables**:
  - `LEARNING_BUCKET`: `strands-etl-learning`

**Note**: If you don't have code for Quality/Optimization/Learning/Execution agents yet, you can create placeholder functions with this simple handler:

**placeholder-handler.py**:
```python
import json

def lambda_handler(event, context):
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Placeholder function - implement later'})
    }
```

**✅ Checkpoint**: All 5 Lambda functions created!

---

## Step 3.4: Test Decision Lambda

1. Go to `strands-decision-agent-lambda` function
2. Click on the **"Test"** tab
3. Click **"Create new event"**
4. Fill in:
   - **Event name**: `test-get-stats`
   - **Event JSON**:
```json
{
  "actionGroup": "platform-selection-tools",
  "apiPath": "/get_platform_statistics",
  "httpMethod": "GET",
  "requestBody": {
    "content": {
      "application/json": {
        "properties": [
          {
            "name": "platform",
            "value": "all"
          },
          {
            "name": "days",
            "value": "30"
          }
        ]
      }
    }
  }
}
```
5. Click **"Save"**
6. Click **"Test"** button

**Expected Result**: You should see a green success box with execution results showing platform statistics.

**✅ Checkpoint**: Lambda function is working correctly!

---

# PHASE 4: Enable Bedrock & Create Agents (60 minutes)

## Step 4.1: Enable Bedrock Model Access

### Navigate to Bedrock Console
1. In AWS Console search bar, type **"Bedrock"**
2. Click on **"Amazon Bedrock"** service
3. In the left sidebar, click on **"Model access"** (under "Foundation models")

### Request Model Access
1. Click the orange **"Manage model access"** button (or "Modify model access")
2. Find **"Anthropic"** in the list
3. ✅ Check the box next to **"Claude 3.5 Sonnet v2"** (anthropic.claude-3-5-sonnet-20241022-v2:0)
4. Scroll to bottom and click **"Request model access"** or **"Save changes"**

**Wait**: Model access is usually instant, but can take 1-5 minutes. Refresh the page until the status shows **"Access granted"** with a green checkmark.

**✅ Checkpoint**: Claude 3.5 Sonnet model access granted!

---

## Step 4.2: Create Decision Agent

### Navigate to Agents
1. In Bedrock Console, click on **"Agents"** in the left sidebar (under "Orchestration")
2. Click the orange **"Create Agent"** button

### Agent Details
1. **Provide agent details**:
   - **Agent name**: `strands-decision-agent`
   - **Description**: `Selects optimal ETL platform (Glue/EMR/Lambda) based on workload characteristics and historical patterns`
   - Click **"Next"**

2. **Select model**:
   - **Model**: Select **"Anthropic Claude 3.5 Sonnet v2"**
   - **Instructions for the Agent**: Copy and paste from `bedrock_agents/configs/decision_agent.json` - the `instruction` field. It should start with: "You are the Decision Agent..."
   - Click **"Next"**

3. **Add action groups**:
   - Click **"Add"** button
   - **Action group name**: `platform-selection-tools`
   - **Description**: `Tools for platform selection and learning vector search`
   - **Action group type**: Select **"Define with API schemas"**
   - **Action group invocation**:
     - Select **"Select an existing Lambda function"**
     - **Lambda function**: Select `strands-decision-agent-lambda`
   - **Action group schema**:
     - **API schema**: Select **"Select an existing S3 URI"**
     - Click **"Browse S3"**
     - Navigate to: `strands-etl-schemas` → `schemas` → Select `decision-agent-api.json`
     - Click **"Choose"**
   - Click **"Add"** (at the bottom of the action group form)
   - Click **"Next"**

4. **Review and create**:
   - Review all settings
   - Click **"Create Agent"**

### Prepare Agent
1. After creation, you'll see the agent details page
2. At the top right, click **"Prepare"** button
3. Wait for the status to show "Prepared" (30-60 seconds)

**✅ Checkpoint**: Decision Agent created and prepared!

---

## Step 4.3: Create Quality Agent

Repeat Step 4.2 with these changes:

1. **Agent name**: `strands-quality-agent`
2. **Description**: `Analyzes ETL job quality by detecting anti-patterns in SQL, PySpark code, and data quality metrics`
3. **Instructions**: Copy from `bedrock_agents/configs/quality_agent.json`
4. **Action group name**: `quality-analysis-tools`
5. **Lambda function**: `strands-quality-agent-lambda`
6. **API schema**: `quality-agent-api.json`

---

## Step 4.4: Create Optimization Agent

Repeat Step 4.2 with these changes:

1. **Agent name**: `strands-optimization-agent`
2. **Description**: `Provides optimization recommendations for ETL jobs based on performance metrics and best practices`
3. **Instructions**: Copy from `bedrock_agents/configs/optimization_agent.json`
4. **Action group name**: `optimization-tools`
5. **Lambda function**: `strands-optimization-agent-lambda`
6. **API schema**: `optimization-agent-api.json`

---

## Step 4.5: Create Learning Agent

Repeat Step 4.2 with these changes:

1. **Agent name**: `strands-learning-agent`
2. **Description**: `Captures execution patterns and stores learning vectors in S3 for continuous improvement`
3. **Instructions**: Copy from `bedrock_agents/configs/learning_agent.json`
4. **Action group name**: `learning-tools`
5. **Lambda function**: `strands-learning-agent-lambda`
6. **API schema**: `learning-agent-api.json`

---

## Step 4.6: Create Supervisor Agent

Repeat Step 4.2 with these changes:

1. **Agent name**: `strands-supervisor-agent`
2. **Description**: `Orchestrates the entire ETL pipeline by coordinating Decision, Quality, Optimization, and Learning agents`
3. **Instructions**: Copy from `bedrock_agents/configs/supervisor_agent.json`
4. **Action group**: **Skip** (supervisor doesn't have its own Lambda, it delegates to other agents)
5. **Agent collaboration**:
   - After creating the agent, click **"Edit in Agent builder"**
   - Scroll down to **"Advanced settings"**
   - Enable **"Agent collaboration"**
   - Click **"Add associated agent"**
   - Add all 4 agents: Decision, Quality, Optimization, Learning
   - Click **"Save"**

**✅ Checkpoint**: All 5 Bedrock Agents created!

---

## Step 4.7: Create Agent Aliases (Production Endpoints)

For each of the 5 agents:

1. Go to the agent details page
2. In the **"Aliases"** section, click **"Create alias"**
3. Fill in:
   - **Alias name**: `production`
   - **Description**: `Production version of the agent`
4. Click **"Create alias"**

**✅ Checkpoint**: All agents have production aliases!

---

# PHASE 5: Test the System (30 minutes)

## Step 5.1: Test Decision Agent

### In Bedrock Console:
1. Go to **"Agents"** → Click on `strands-decision-agent`
2. Click **"Test"** button (top right)
3. In the test panel on the right, type:
   ```
   What platform should I use for a high volume, high complexity ETL job processing 500GB of customer data?
   ```
4. Press Enter

**Expected Response**: The agent should analyze the workload and recommend a platform (likely EMR for very high volume).

---

## Step 5.2: Test Quality Agent

1. Go to `strands-quality-agent`
2. Click **"Test"**
3. Type:
   ```
   Analyze this SQL for anti-patterns: SELECT * FROM customers WHERE status = 'active'
   ```
4. Press Enter

**Expected Response**: Should identify "SELECT *" as an anti-pattern.

---

## Step 5.3: Test Supervisor Agent (End-to-End)

1. Go to `strands-supervisor-agent`
2. Click **"Test"**
3. Type:
   ```
   I need to run a medium-complexity ETL job to process 100GB of order data. Please select the best platform, check for any quality issues, and provide optimization recommendations.
   ```
4. Press Enter

**Expected Response**: The supervisor should:
- Delegate to Decision Agent for platform selection
- Use Quality Agent to check for issues
- Use Optimization Agent for recommendations
- Return a comprehensive response

**✅ Checkpoint**: Agents are working and collaborating!

---

# PHASE 6: Deploy Dashboard (30 minutes)

## Step 6.1: Install Dashboard Dependencies

On your local machine or EC2 instance:

```bash
# Navigate to dashboard directory
cd /home/user/strands_etl/dashboard

# Install dependencies
pip install streamlit boto3 pandas plotly
```

---

## Step 6.2: Configure AWS Credentials

If running locally, make sure you have AWS credentials configured:

```bash
aws configure
```

Enter your:
- AWS Access Key ID
- AWS Secret Access Key
- Default region: `us-east-1`
- Default output format: `json`

---

## Step 6.3: Run the Dashboard

```bash
# Run Streamlit dashboard
streamlit run strands_dashboard.py
```

The dashboard will open in your browser at `http://localhost:8501`

### Dashboard Features:
- **Overview**: Shows all agent statuses
- **Execution Timeline**: Visualizes pipeline runs
- **Platform Comparison**: Compares Glue vs EMR vs Lambda
- **Quality Metrics**: Shows quality scores over time
- **Live Agent Testing**: Test agents directly from dashboard

**✅ Checkpoint**: Dashboard is running!

---

# 🎉 Setup Complete!

## What You've Accomplished:

✅ Created 2 S3 buckets with proper configuration
✅ Created 2 IAM roles with appropriate permissions
✅ Created 5 Lambda functions with code and configuration
✅ Enabled Bedrock model access
✅ Created 5 Bedrock Agents with action groups
✅ Set up agent collaboration
✅ Tested the entire system
✅ Deployed the monitoring dashboard

---

## Next Steps:

### 1. Create Your First Learning Vector

To enable the learning functionality, create a sample learning vector in S3:

```bash
# Create sample vector file
cat > sample-vector.json << 'EOF'
{
  "execution_id": "exec-001",
  "timestamp": "2024-01-21T10:30:00Z",
  "workload_characteristics": {
    "data_volume": "high",
    "complexity": "medium",
    "criticality": "high",
    "estimated_runtime_minutes": 45
  },
  "execution_metrics": {
    "platform_used": "glue",
    "execution_time_seconds": 2700,
    "success": true,
    "data_quality_score": 0.95,
    "cost_dollars": 5.50
  },
  "decision_context": {
    "reason": "High data volume favors Glue"
  }
}
EOF

# Upload to S3
aws s3 cp sample-vector.json s3://strands-etl-learning/learning/vectors/learning/exec-001.json
```

### 2. Run Your First Pipeline

Use the supervisor agent to run a complete pipeline:

```
I need to process a customer orders ETL job with:
- Data volume: 250GB
- Complexity: High (multiple joins and transformations)
- Criticality: Medium
- Expected runtime: 1-2 hours

Please select the platform, analyze for quality issues, and provide optimization recommendations.
```

### 3. Monitor in Dashboard

Open your dashboard and watch the execution in real-time!

---

## Troubleshooting

### Issue: Lambda Permission Denied
**Fix**: Go to Lambda function → Configuration → Permissions → Check that execution role is `StrandsLambdaExecutionRole`

### Issue: Bedrock Agent Can't Invoke Lambda
**Fix**:
1. Go to Lambda function
2. Configuration → Permissions
3. Add resource-based policy:
```json
{
  "Effect": "Allow",
  "Principal": {
    "Service": "bedrock.amazonaws.com"
  },
  "Action": "lambda:InvokeFunction",
  "Resource": "arn:aws:lambda:us-east-1:YOUR_ACCOUNT:function:strands-*"
}
```

### Issue: Agent Not Finding Similar Workloads
**Fix**: Make sure learning vectors exist in S3 at path: `s3://strands-etl-learning/learning/vectors/learning/`

---

## Cost Management

Monitor your costs in AWS Cost Explorer:
- Bedrock invocations
- Lambda execution time
- S3 storage and requests

**Expected monthly cost**: $570-1,240 based on moderate usage (see `AWS_PROVISIONING_GUIDE.md` for details)

---

## Support & Documentation

- **Full Architecture**: See `BEDROCK_AGENTS_README.md`
- **Cost Details**: See `AWS_PROVISIONING_GUIDE.md`
- **API Schemas**: Check `bedrock_agents/schemas/`
- **Agent Configs**: Check `bedrock_agents/configs/`

---

**Document Version**: 1.0
**Last Updated**: 2024-01-21
**Difficulty Level**: Beginner-Friendly
**Estimated Setup Time**: 3-4 hours
