# Strands ETL - MCP (Model Context Protocol) Integration Guide

## 🎯 Overview

This guide shows how the Strands ETL framework uses **MCP (Model Context Protocol)** to create truly agentic capabilities by connecting agents to external tools and data sources.

### What is MCP?

MCP is an open protocol developed by Anthropic that enables AI assistants to securely connect to:
- Local and remote data sources
- Business tools and APIs
- External services
- File systems and databases

### Why MCP for Strands?

✅ **True Agentic Behavior**: Agents can autonomously query data, analyze code, and access resources
✅ **Unified Tool Interface**: Single protocol for all external integrations
✅ **Secure**: Scoped permissions and sandboxed execution
✅ **Extensible**: Easy to add new data sources and tools
✅ **Real-time**: Agents access live data, not just prompts

---

## 📊 MCP Architecture

```
┌────────────────────────────────────────────────────────────┐
│                   Bedrock Agents                           │
│  (Decision, Quality, Optimization, Learning, Compliance)   │
└─────────────────────┬──────────────────────────────────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │   Lambda Functions     │
         │  (with MCP Client)     │
         └────────────┬───────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │  MCP Protocol Layer    │
         └────────────┬───────────┘
                      │
         ┌────────────┴───────────┬──────────────┬────────────┐
         │                        │              │            │
         ▼                        ▼              ▼            ▼
   ┌──────────┐           ┌──────────┐    ┌──────────┐  ┌──────────┐
   │   AWS    │           │Filesystem│    │  GitHub  │  │  Slack   │
   │   MCP    │           │   MCP    │    │   MCP    │  │   MCP    │
   │  Server  │           │  Server  │    │  Server  │  │  Server  │
   └────┬─────┘           └────┬─────┘    └────┬─────┘  └────┬─────┘
        │                      │               │             │
        ▼                      ▼               ▼             ▼
   ┌──────────┐           ┌──────────┐    ┌──────────┐  ┌──────────┐
   │DynamoDB  │           │ETL Scripts│    │ Code Repo│  │ Channels │
   │Glue/EMR  │           │Configs   │    │ History  │  │ Messages │
   │S3/CW     │           │Docs      │    │ Issues   │  │          │
   └──────────┘           └──────────┘    └──────────┘  └──────────┘
```

---

## 🛠️ Available MCP Servers

### 1. AWS MCP Server (Custom)

**Purpose**: Direct AWS service access for real-time data

**Capabilities**:
- `query_execution_history`: Get recent job executions from DynamoDB
- `get_cost_trends`: Analyze cost trends over time
- `get_quality_trends`: Track data quality metrics
- `get_glue_job_status`: Check current Glue job status
- `list_learning_vectors`: List available learning vectors in S3
- `get_cloudwatch_metrics`: Query CloudWatch metrics
- `query_issues`: Search issue registry

**Use Cases**:
- Decision Agent: Query similar past executions for platform selection
- Cost Tracking Agent: Analyze historical costs and trends
- Quality Agent: Compare current quality to historical baseline
- Compliance Agent: Check audit trail in DynamoDB

**Example**:
```python
# In Lambda function
from mcp.mcp_client import get_mcp_client

client = get_mcp_client()

# Query recent executions
history = client.query_execution_history(
    job_name='customer_order_summary',
    limit=10,
    status='success'
)

# Analyze cost trends
costs = client.get_cost_trends(
    job_name='customer_order_summary',
    days=30
)
```

---

### 2. Filesystem MCP Server

**Purpose**: Access ETL scripts and configurations for analysis

**Capabilities**:
- `read_file`: Read file contents
- `list_directory`: List directory contents
- `search_files`: Search for files by pattern
- `get_file_info`: Get file metadata

**Use Cases**:
- Quality Agent: Read PySpark scripts to detect anti-patterns
- Optimization Agent: Analyze code for optimization opportunities
- Compliance Agent: Check configurations for compliance
- Learning Agent: Review script changes over time

**Example**:
```python
# Read a Glue script for quality analysis
script_content = client.read_file(
    '/home/user/strands_etl/pyscript/customer_order_summary_glue.py'
)

# Analyze for anti-patterns
if 'SELECT *' in script_content:
    issues.append({
        'type': 'SELECT_STAR',
        'severity': 'medium',
        'line': find_line_number(script_content, 'SELECT *')
    })
```

---

### 3. GitHub MCP Server

**Purpose**: Access code repository for historical analysis

**Capabilities**:
- `search_code`: Search codebase for patterns
- `get_file_contents`: Get file from any branch/commit
- `list_commits`: Get commit history
- `get_pr_details`: Get pull request information
- `search_issues`: Search GitHub issues

**Use Cases**:
- Quality Agent: Find historical anti-pattern fixes
- Optimization Agent: Learn from past optimization PRs
- Learning Agent: Track code evolution
- Decision Agent: Analyze platform selection decisions over time

**Example**:
```python
# Search for similar anti-patterns in history
results = client.search_code(
    query='SELECT * FROM in:file language:sql',
    repo='dhilipansankaralingam-del/strands_etl'
)

# Learn from past fixes
for result in results:
    if 'fixed' in result.get('commit_message', '').lower():
        recommendations.append({
            'pattern': result['pattern'],
            'fix': result['fix'],
            'pr': result['pr_url']
        })
```

---

### 4. Slack MCP Server

**Purpose**: Send real-time notifications to team

**Capabilities**:
- `send_message`: Send message to channel
- `post_alert`: Post alert with formatting
- `create_thread`: Create threaded conversation
- `update_message`: Update existing message

**Use Cases**:
- Supervisor Agent: Alert team on job completion
- Compliance Agent: Notify on critical violations
- Cost Tracking Agent: Alert on budget overruns
- Quality Agent: Report quality score drops

**Example**:
```python
# Send alert on high-severity issue
if issue['severity'] == 'critical':
    client.send_slack_message(
        message=f"🚨 Critical Issue Detected in {job_name}:\n"
                f"Type: {issue['type']}\n"
                f"Impact: {issue['impact']}\n"
                f"Recommendation: {issue['recommendation']}",
        channel='#strands-etl-alerts'
    )
```

---

## 🚀 Setup Instructions

### Step 1: Install Node.js and npm

MCP servers require Node.js:

```bash
# Check if Node.js is installed
node --version
npm --version

# If not installed:
# Ubuntu/Debian
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs

# macOS
brew install node

# Verify
node --version  # Should be v18+
npm --version
```

### Step 2: Install MCP Dependencies

```bash
cd /home/user/strands_etl/mcp

# Install dependencies
npm install

# This installs:
# - @modelcontextprotocol/sdk
# - aws-sdk
```

### Step 3: Configure MCP Servers

```bash
# Copy configuration to Lambda layer
cp -r /home/user/strands_etl/mcp /tmp/

# Set environment variables
export AWS_REGION=us-east-1
export DYNAMODB_TABLE_PREFIX=Strands

# Optional: GitHub token for code analysis
export GITHUB_TOKEN=ghp_your_token_here

# Optional: Slack token for notifications
export SLACK_BOT_TOKEN=xoxb-your-token-here
```

### Step 4: Test MCP Servers

```bash
# Test AWS MCP Server
echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"query_execution_history","arguments":{"job_name":"test","limit":5}}}' | node mcp/servers/aws-mcp-server.js

# Test Filesystem MCP Server
npx -y @modelcontextprotocol/server-filesystem /home/user/strands_etl

# Test GitHub MCP Server (requires token)
export GITHUB_PERSONAL_ACCESS_TOKEN=$GITHUB_TOKEN
npx -y @modelcontextprotocol/server-github
```

### Step 5: Update Lambda Functions

Add MCP client to Lambda functions:

```bash
# Add mcp_client.py to Lambda package
cd /home/user/strands_etl/lambda_functions/decision

# Copy MCP client
cp /home/user/strands_etl/mcp/mcp_client.py .

# Update requirements.txt
echo "subprocess32==3.5.4" >> requirements.txt

# Repackage
zip -r decision-function.zip handler.py mcp_client.py
```

### Step 6: Deploy Lambda Layer (Optional)

For better code reuse, create a Lambda Layer:

```bash
# Create layer directory
mkdir -p /tmp/lambda-layer/python

# Copy MCP client and configs
cp /home/user/strands_etl/mcp/mcp_client.py /tmp/lambda-layer/python/
cp -r /home/user/strands_etl/mcp/servers /tmp/lambda-layer/python/mcp/

# Package layer
cd /tmp/lambda-layer
zip -r strands-mcp-layer.zip python/

# Upload to AWS
aws lambda publish-layer-version \
  --layer-name strands-mcp-client \
  --zip-file fileb://strands-mcp-layer.zip \
  --compatible-runtimes python3.11 \
  --description "MCP client for Strands ETL agents"

# Attach layer to Lambda functions
aws lambda update-function-configuration \
  --function-name strands-decision-agent-lambda \
  --layers arn:aws:lambda:us-east-1:ACCOUNT_ID:layer:strands-mcp-client:1
```

---

## 💡 Agent Integration Examples

### Decision Agent with MCP

**Enhanced Capabilities**:
- Query historical executions from DynamoDB
- Analyze cost trends for similar workloads
- Compare quality scores across platforms

```python
# In decision agent Lambda
from mcp_client import get_mcp_client

def select_platform(workload):
    client = get_mcp_client()

    # Query similar executions via MCP
    history = client.query_execution_history(
        job_name=workload['job_name'],
        limit=20
    )

    # Analyze cost trends
    costs = client.get_cost_trends(
        job_name=workload['job_name'],
        days=30
    )

    # Make decision based on real data
    if costs['average_daily_cost'] > 10:
        # Consider cost optimization
        platform = 'lambda'  # Cheaper option
    else:
        platform = 'glue'   # Standard option

    return platform
```

### Quality Agent with MCP

**Enhanced Capabilities**:
- Read actual script files for analysis
- Search GitHub for similar anti-patterns
- Query issue history from DynamoDB

```python
# In quality agent Lambda
from mcp_client import get_mcp_client

def analyze_quality(script_path):
    client = get_mcp_client()

    # Read script via filesystem MCP
    script_content = client.read_file(script_path)

    # Detect anti-patterns
    issues = []
    if 'SELECT *' in script_content:
        # Search GitHub for similar fixes
        fixes = client.search_code(
            query='SELECT * FROM fixed',
            language='sql'
        )

        issues.append({
            'type': 'SELECT_STAR',
            'severity': 'medium',
            'historical_fixes': len(fixes),
            'recommendation': 'Use explicit column list'
        })

    # Query similar issues from history
    past_issues = client.query_issues(
        issue_type='SELECT_STAR',
        limit=10
    )

    return {
        'issues': issues,
        'historical_context': past_issues
    }
```

### Cost Tracking Agent with MCP

**Enhanced Capabilities**:
- Real-time cost data from DynamoDB
- Historical trend analysis
- Anomaly detection

```python
# In cost tracking agent Lambda
from mcp_client import get_mcp_client

def track_costs(execution_id, current_cost):
    client = get_mcp_client()

    # Get historical cost trends via MCP
    trends = client.get_cost_trends(
        job_name=execution['job_name'],
        days=30
    )

    # Detect anomalies
    avg_cost = trends['average_daily_cost']
    if current_cost > avg_cost * 2:
        # Send Slack alert
        client.send_slack_message(
            message=f"⚠️ Cost Alert: {execution['job_name']} "
                    f"cost ${current_cost} is 2x higher than average ${avg_cost}",
            channel='#strands-cost-alerts'
        )

    return {
        'cost': current_cost,
        'average': avg_cost,
        'anomaly': current_cost > avg_cost * 2
    }
```

---

## 🔧 Configuration Reference

### MCP Config File: `mcp/mcp-config.json`

```json
{
  "mcpServers": {
    "aws": {
      "command": "node",
      "args": ["mcp/servers/aws-mcp-server.js"],
      "enabled": true
    },
    "filesystem": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-filesystem", "."],
      "enabled": true
    },
    "github": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-github"],
      "enabled": true,
      "env": {
        "GITHUB_PERSONAL_ACCESS_TOKEN": "${GITHUB_TOKEN}"
      }
    },
    "slack": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-slack"],
      "enabled": false
    }
  },
  "agent_mappings": {
    "decision-agent": ["aws", "filesystem"],
    "quality-agent": ["aws", "filesystem", "github"],
    "cost-tracking-agent": ["aws", "slack"]
  }
}
```

### Environment Variables

```bash
# AWS MCP Server
AWS_REGION=us-east-1
DYNAMODB_TABLE_PREFIX=Strands

# GitHub MCP Server
GITHUB_PERSONAL_ACCESS_TOKEN=ghp_xxxxx

# Slack MCP Server
SLACK_BOT_TOKEN=xoxb-xxxxx
SLACK_CHANNEL=#strands-etl-alerts
```

---

## 📈 Benefits of MCP Integration

### Before MCP:
❌ Agents rely only on prompt context
❌ No access to real-time data
❌ Limited to pre-loaded information
❌ Can't query historical patterns
❌ Manual data gathering required

### After MCP:
✅ Agents query live data from DynamoDB
✅ Real-time AWS service status
✅ Access to complete code history
✅ Automatic historical pattern analysis
✅ Direct Slack/email notifications
✅ Read actual script files for analysis
✅ Search GitHub for similar issues
✅ True agentic autonomy

---

## 🔒 Security Considerations

### MCP Security Best Practices:

1. **Scoped Permissions**:
   - Each MCP server has minimal AWS IAM permissions
   - Filesystem server limited to ETL directory only
   - GitHub token with read-only access

2. **Sandboxed Execution**:
   - MCP servers run in isolated processes
   - 30-second timeout on all operations
   - Resource limits enforced

3. **Audit Trail**:
   - All MCP calls logged to CloudWatch
   - Agent invocations tracked in DynamoDB
   - Tool usage monitored

4. **Credential Management**:
   - No hardcoded tokens
   - Environment variables for secrets
   - AWS Secrets Manager integration available

---

## 🧪 Testing MCP Integration

### Test AWS MCP Server:

```python
from mcp.mcp_client import get_mcp_client

client = get_mcp_client()

# Test execution history query
history = client.query_execution_history('test_job', limit=5)
print(f"Found {len(history['executions'])} executions")

# Test cost trends
costs = client.get_cost_trends('test_job', days=7)
print(f"Average cost: ${costs['average_daily_cost']}")

# Test issue query
issues = client.query_issues(severity='high', limit=10)
print(f"Found {len(issues['issues'])} high-severity issues")
```

### Test Filesystem MCP Server:

```python
# Test file read
script = client.read_file('/home/user/strands_etl/pyscript/example.py')
print(f"Script length: {len(script)} characters")

# Test directory listing
files = client.list_directory('/home/user/strands_etl/pyscript')
print(f"Found {len(files)} Python scripts")
```

---

## 📚 Additional Resources

- **MCP Specification**: https://modelcontextprotocol.io/
- **MCP SDK**: https://github.com/modelcontextprotocol/sdk
- **Strands MCP Client**: `mcp/mcp_client.py`
- **Server Configs**: `mcp/servers/`
- **Setup Guide**: This document

---

## 🆘 Troubleshooting

### Issue: "MCP server not found"
**Solution**: Check server is listed in `mcp/mcp-config.json` and enabled

### Issue: "Connection timeout"
**Solution**: Increase timeout in config, check server logs

### Issue: "Permission denied"
**Solution**: Verify IAM roles have required permissions

### Issue: "GitHub token invalid"
**Solution**: Generate new token with repo:read scope

---

**MCP Integration Complete!** 🎉

Your agents now have true agentic capabilities with access to:
- ✅ Real-time AWS data
- ✅ Complete code history
- ✅ File system access
- ✅ Slack notifications
