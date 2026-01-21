

# Strands Framework - Complete Usage Guide

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Installation](#installation)
4. [Quick Start](#quick-start)
5. [Agent Details](#agent-details)
6. [Use Cases](#use-cases)
7. [API Reference](#api-reference)
8. [Best Practices](#best-practices)

---

## Overview

The Strands ETL Framework is a **truly agentic** system where multiple AI agents work independently and asynchronously to orchestrate, optimize, and learn from ETL pipeline executions.

### Key Differentiators

**Before (Traditional Orchestrator):**
- Sequential agent calls (method invocations)
- No real autonomy - agents were just functions
- Limited learning - data stored but not used
- Single-threaded execution
- Quality checks hard-coded

**After (Strands Framework):**
- ✅ Independent agents running concurrently
- ✅ True asynchronous communication via message bus
- ✅ ML-based decisions using historical patterns
- ✅ Self-learning from every execution
- ✅ SQL and natural language quality inputs
- ✅ Pattern-based optimization recommendations
- ✅ Autonomous agent lifecycle management

---

## Architecture

### Component Overview

```
Application Layer
    │
    ├── StrandsCoordinator (orchestrates everything)
    │
    └── Message Bus (async communication)
            │
            ├── Decision Agent (independent thread)
            │   └── Uses: Learning vectors, ML similarity
            │
            ├── Quality Agent (independent thread)
            │   └── Accepts: SQL, Code, Natural Language
            │
            ├── Optimization Agent (independent thread)
            │   └── Provides: Pattern-based recommendations
            │
            └── Learning Agent (independent thread)
                └── Stores: Comprehensive execution vectors
```

### Data Flow

```
1. User Request
   ↓
2. Coordinator creates pipeline context
   ↓
3. Decision Agent receives request
   ├─ Loads learning vectors from S3
   ├─ Finds similar workloads
   ├─ Scores platforms using ML
   ├─ Gets AI recommendation
   └─ Publishes decision (async)
   ↓
4. Execution on selected platform
   ↓
5. Quality Agent monitors execution
   └─ Stores quality report to S3
   ↓
6. Optimization Agent analyzes performance
   └─ Stores recommendations to S3
   ↓
7. Learning Agent aggregates all data
   └─ Creates comprehensive learning vector
   └─ Stores to S3 for future use
   ↓
8. All agents continue autonomous cycles
```

---

## Installation

### Prerequisites

```bash
# Python 3.9 or higher
python --version

# AWS credentials configured
aws configure

# S3 bucket for learning vectors
aws s3 mb s3://strands-etl-learning
```

### Install Dependencies

```bash
cd /home/user/strands_etl

# Install required packages
pip install -r requirements.txt

# Additional for Strands
pip install numpy  # For ML-based similarity calculations
```

### Verify Installation

```bash
python -c "from strands import StrandsCoordinator; print('✓ Strands installed')"
```

---

## Quick Start

### 1. Full Pipeline Orchestration

```python
import asyncio
from strands import StrandsCoordinator

async def run_pipeline():
    # Create coordinator
    coordinator = StrandsCoordinator()

    try:
        # Initialize (starts all agents)
        await coordinator.initialize()

        # Run ETL pipeline
        result = await coordinator.orchestrate_pipeline(
            user_request="Process customer order analytics",
            config_path="/home/user/strands_etl/etl_config.json"
        )

        # Check results
        print(f"Status: {result['status']}")
        print(f"Platform: {result['decision']['selected_platform']}")
        print(f"Confidence: {result['decision']['confidence_score']:.2f}")

    finally:
        await coordinator.shutdown()

# Run
asyncio.run(run_pipeline())
```

### 2. SQL Quality Analysis

```python
import asyncio
from strands import StrandsQualityAgent, get_message_bus, MessageType, start_message_bus

async def analyze_sql():
    # Start message bus
    await start_message_bus()

    # Create and start agent
    agent = StrandsQualityAgent()
    await agent.start()

    # Get bus
    bus = get_message_bus()

    # Analyze SQL
    sql = """
    SELECT * FROM customers
    WHERE UPPER(email) LIKE '%@gmail.com'
    """

    msg = bus.create_message(
        sender='my_app',
        message_type=MessageType.QUALITY_CHECK,
        payload={'check_type': 'sql', 'input': sql},
        target='quality_agent'
    )

    await bus.publish(msg)

    # Wait for response
    await asyncio.sleep(3)

    # Get results from message history
    messages = bus.get_message_history(correlation_id=msg.correlation_id)
    for m in messages:
        if m.message_type == MessageType.AGENT_RESPONSE:
            result = m.payload
            print(f"Quality Score: {result['query_analysis']['quality_score']}")
            for issue in result['query_analysis']['issues']:
                print(f"Issue: {issue['message']}")

    await agent.stop()

asyncio.run(analyze_sql())
```

### 3. Natural Language Quality Check

```python
async def nl_quality_check():
    # Setup (same as above)
    await start_message_bus()
    agent = StrandsQualityAgent()
    await agent.start()
    bus = get_message_bus()

    # Natural language request
    nl = "Check if all customer emails are valid and there are no duplicate customer IDs"

    msg = bus.create_message(
        sender='my_app',
        message_type=MessageType.QUALITY_CHECK,
        payload={'check_type': 'natural_language', 'input': nl},
        target='quality_agent'
    )

    await bus.publish(msg)
    await asyncio.sleep(5)  # NL processing takes longer

    # Results
    messages = bus.get_message_history(correlation_id=msg.correlation_id)
    for m in messages:
        if m.message_type == MessageType.AGENT_RESPONSE:
            result = m.payload
            print("Interpreted Checks:")
            for check in result['interpreted_checks']:
                print(f"  - {check['check_type']}: {check['description']}")

    await agent.stop()

asyncio.run(nl_quality_check())
```

### 4. Using Enhanced Orchestrator (Drop-in Replacement)

```python
# For existing code using the old orchestrator
from orchestrator.strands_enhanced_orchestrator import StrandsEnhancedOrchestrator

# Same interface as before
orchestrator = StrandsEnhancedOrchestrator()

result = orchestrator.orchestrate_pipeline(
    user_request="Process data",
    config_path="config.json"
)

# Compatible output format + enhanced Strands features
print(result['platform_decision']['selected_platform'])
print(result['strands_enhanced']['multi_agent_execution'])  # True

# Get learning summary
learning = orchestrator.get_learning_summary()
print(f"Success rate: {learning['success_rate']:.1%}")

# Cleanup
orchestrator.shutdown()
```

---

## Agent Details

### Decision Agent

**Purpose**: Select optimal platform using ML-based pattern matching

**How it learns**:
1. Loads historical learning vectors from S3
2. Extracts workload features (volume, complexity, criticality)
3. Calculates similarity to current workload using cosine similarity
4. Scores platforms based on historical success rates
5. Uses AI (Bedrock) for final recommendation
6. Stores decision as learning vector

**Key Methods**:
```python
agent = StrandsDecisionAgent()
await agent.start()

# Agent automatically handles platform_decision requests
# and publishes DECISION_MADE messages
```

**Learning Vector Path**: `s3://strands-etl-learning/learning/vectors/decision/`

### Quality Agent

**Purpose**: Analyze queries, code, and data quality

**Capabilities**:

1. **SQL Analysis**:
   - Detects anti-patterns (SELECT *, missing WHERE, etc.)
   - Provides recommendations
   - Scores query quality

2. **Code Analysis**:
   - PySpark anti-patterns (.count(), .collect(), etc.)
   - Line-by-line issue detection
   - Performance recommendations

3. **Natural Language**:
   - Converts NL to structured quality checks
   - Executes checks
   - Aggregates results

**Usage**:
```python
# SQL
payload = {'check_type': 'sql', 'input': 'SELECT * FROM ...'}

# Code
payload = {'check_type': 'code', 'input': 'df.count()...'}

# Natural Language
payload = {'check_type': 'natural_language', 'input': 'Check for nulls in email field'}
```

**Learning Vector Path**: `s3://strands-etl-learning/learning/vectors/quality/`

### Optimization Agent

**Purpose**: Provide pattern-based performance recommendations

**How it works**:
1. Establishes performance baselines from history
2. Compares executions to baselines
3. Identifies optimization opportunities
4. Generates prioritized recommendations
5. Estimates improvement impact

**Key Features**:
- Resource allocation analysis
- Join pattern optimization
- Configuration issue detection
- Historical pattern matching

**Learning Vector Path**: `s3://strands-etl-learning/learning/vectors/optimization/`

### Learning Agent

**Purpose**: Capture comprehensive execution patterns

**What it captures**:
- Workload characteristics
- Execution metrics (time, success, quality)
- Decision context (platform, confidence)
- Performance indicators
- Quality metrics
- Optimization recommendations
- AI-generated insights

**Lifecycle**:
1. Tracks execution from start to completion
2. Aggregates data from all other agents
3. Generates AI insights
4. Creates comprehensive learning vector
5. Stores to S3
6. Broadcasts learning update

**Learning Vector Path**: `s3://strands-etl-learning/learning/vectors/learning/`

---

## Use Cases

### Use Case 1: Automated Platform Selection

**Scenario**: You have a workload and want the system to automatically select the best platform based on past experiences.

```python
coordinator = StrandsCoordinator()
await coordinator.initialize()

result = await coordinator.orchestrate_pipeline(
    user_request="Process high-volume customer data",
    config_path="config.json"
)

# System automatically:
# 1. Finds similar past workloads
# 2. Analyzes which platform worked best
# 3. Selects optimal platform with confidence score
# 4. Executes on that platform
# 5. Learns from the execution

print(f"Selected: {result['decision']['selected_platform']}")
print(f"Reasoning: {result['decision']['reasoning']}")
```

### Use Case 2: SQL Query Optimization

**Scenario**: Analyze SQL queries before execution to catch performance issues.

```python
agent = StrandsQualityAgent()
await agent.start()

# Your ETL has this query
query = """
SELECT *
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE YEAR(order_date) = 2024
"""

# Analyze it
result = await analyze_query(agent, query)

# Result shows:
# - SELECT * should specify columns
# - Function on column (YEAR) prevents index usage
# - Recommendations for optimization
```

### Use Case 3: Natural Language Data Quality

**Scenario**: Non-technical users need to specify data quality checks.

```python
# User says in plain English:
nl_input = "Make sure all customer records from the last month have valid phone numbers and no missing addresses"

# Quality Agent converts this to:
# 1. Completeness check on phone_number field
# 2. Validity check on phone_number format
# 3. Completeness check on address field
# 4. Timeliness check (last month filter)

# And executes all checks automatically
```

### Use Case 4: Continuous Performance Improvement

**Scenario**: System learns from every execution and gets better over time.

```python
# First execution
result1 = await coordinator.orchestrate_pipeline(...)
# Platform: EMR, Time: 45 min, Quality: 0.92

# Second similar execution (system learned)
result2 = await coordinator.orchestrate_pipeline(...)
# Platform: Glue (learned it's faster), Time: 30 min, Quality: 0.95

# Third execution (even better)
result3 = await coordinator.orchestrate_pipeline(...)
# Platform: Glue, Time: 25 min (optimizations applied), Quality: 0.97
```

---

## API Reference

### StrandsCoordinator

```python
class StrandsCoordinator:
    async def initialize() -> None:
        """Initialize coordinator and all agents"""

    async def orchestrate_pipeline(user_request: str, config_path: str) -> Dict:
        """Run ETL pipeline with multi-agent orchestration"""

    async def get_agent_metrics() -> Dict[str, Any]:
        """Get performance metrics from all agents"""

    async def get_learning_summary() -> Dict[str, Any]:
        """Get learning summary from Learning Agent"""

    async def shutdown() -> None:
        """Shutdown coordinator and cleanup"""
```

### StrandsAgent (Base Class)

```python
class StrandsAgent:
    async def start() -> None:
        """Start agent's autonomous operation"""

    async def stop() -> None:
        """Stop the agent"""

    async def process_message(message: StrandsMessage) -> Optional[Dict]:
        """Process incoming message (override in subclass)"""

    async def autonomous_cycle() -> None:
        """Perform autonomous tasks (override in subclass)"""

    async def send_message(target, message_type, payload) -> None:
        """Send message to another agent"""

    async def invoke_bedrock(prompt: str, system_prompt: str) -> str:
        """Invoke AWS Bedrock for AI reasoning"""

    async def load_learning_vectors(limit: int) -> List[Dict]:
        """Load learning vectors from S3"""

    async def store_learning_vector(vector: Dict) -> bool:
        """Store learning vector to S3"""
```

### MessageType Enum

```python
class MessageType(Enum):
    AGENT_REQUEST = "agent_request"
    AGENT_RESPONSE = "agent_response"
    AGENT_ERROR = "agent_error"
    LEARNING_UPDATE = "learning_update"
    QUALITY_CHECK = "quality_check"
    OPTIMIZATION_RECOMMENDATION = "optimization_recommendation"
    DECISION_MADE = "decision_made"
    EXECUTION_STARTED = "execution_started"
    EXECUTION_COMPLETED = "execution_completed"
    EXECUTION_FAILED = "execution_failed"
```

---

## Best Practices

### 1. Agent Initialization

✅ **Do**: Initialize coordinator once at application start
```python
coordinator = StrandsCoordinator()
await coordinator.initialize()
# Use for multiple pipelines
```

❌ **Don't**: Initialize for each pipeline
```python
# Wasteful - agents startup is expensive
for pipeline in pipelines:
    coordinator = StrandsCoordinator()
    await coordinator.initialize()
```

### 2. Error Handling

✅ **Do**: Wrap in try/finally
```python
coordinator = StrandsCoordinator()
try:
    await coordinator.initialize()
    result = await coordinator.orchestrate_pipeline(...)
finally:
    await coordinator.shutdown()
```

### 3. Learning Vectors

✅ **Do**: Let the system accumulate learning vectors
- First 10-20 executions build baseline
- After 50+ executions, decisions become very accurate

❌ **Don't**: Expect perfect decisions immediately
- System needs data to learn from

### 4. Quality Checks

✅ **Do**: Use natural language for business users
```python
"Check customer_id is unique and email is valid"
```

✅ **Do**: Use SQL/code analysis for developers
```python
analyze_query("SELECT * FROM ...")
```

### 5. Message Bus

✅ **Do**: Reuse message bus instance
```python
bus = get_message_bus()  # Singleton
```

❌ **Don't**: Create new buses
```python
bus = StrandsMessageBus()  # Creates separate bus!
```

### 6. S3 Buckets

✅ **Do**: Configure proper lifecycle policies
```yaml
# Delete learning vectors older than 90 days
{
  "Rules": [{
    "Id": "DeleteOldVectors",
    "Expiration": {"Days": 90},
    "Filter": {"Prefix": "learning/vectors/"}
  }]
}
```

### 7. Monitoring

✅ **Do**: Check agent metrics periodically
```python
metrics = await coordinator.get_agent_metrics()
for agent, stats in metrics.items():
    if stats['errors'] > 10:
        logger.warning(f"{agent} has high error count")
```

---

## Troubleshooting

### Problem: Agents not responding

**Solution**:
```python
# Check agent state
metrics = await coordinator.get_agent_metrics()
for name, m in metrics.items():
    print(f"{name}: {m['state']}")
    # Should be 'idle' or 'processing', not 'error' or 'stopped'
```

### Problem: Learning vectors not being used

**Solution**:
```bash
# Verify vectors exist
aws s3 ls s3://strands-etl-learning/learning/vectors/decision/

# Check agent logs
# Should see: "Loaded N learning vectors"
```

### Problem: Quality checks returning errors

**Solution**:
```python
# Verify Bedrock access
import boto3
bedrock = boto3.client('bedrock-runtime')
response = bedrock.invoke_model(
    modelId='anthropic.claude-3-sonnet-20240229-v1:0',
    body='{"messages": [{"role": "user", "content": "test"}], "max_tokens": 100}'
)
```

---

## Performance Tips

1. **Parallel Pipelines**: Strands supports multiple concurrent pipelines
   ```python
   results = await asyncio.gather(
       coordinator.orchestrate_pipeline(...),
       coordinator.orchestrate_pipeline(...),
       coordinator.orchestrate_pipeline(...)
   )
   ```

2. **Learning Cache**: Decision agent caches learning vectors for 5 minutes
   - Reduces S3 calls
   - Faster decisions

3. **Message Priority**: Use high priority for critical workflows
   ```python
   msg = bus.create_message(..., priority=9)
   ```

4. **Agent Metrics**: Monitor `avg_processing_time`
   - If increasing, agents may be overloaded
   - Consider horizontal scaling

---

## Next Steps

1. **Run Examples**: Start with `examples/example_full_pipeline.py`
2. **Review Logs**: Check CloudWatch for agent activity
3. **Monitor S3**: Watch learning vectors accumulate
4. **Experiment**: Try different workloads and see learning improve
5. **Extend**: Create custom agents for your specific needs

---

## Support

For issues, questions, or contributions:
- Check `strands/README.md` for detailed documentation
- Review examples in `examples/` directory
- Check CloudWatch logs for debugging

---

**Remember**: Strands is a self-learning system. The more you use it, the smarter it gets! 🚀
