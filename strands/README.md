# Strands ETL Framework

An intelligent, multi-agent ETL orchestration framework powered by AI and machine learning. Strands enables autonomous, self-learning ETL pipelines through independent agents that communicate asynchronously.

## 🌟 Key Features

### **True Agentic Architecture**
- **Independent Agents**: Each agent runs autonomously in its own execution thread
- **Asynchronous Communication**: Message bus-based inter-agent communication
- **Parallel Execution**: Agents work concurrently for maximum performance

### **ML-Based Intelligence**
- **Decision Agent**: Uses historical patterns and ML to select optimal execution platforms
- **Quality Agent**: Analyzes SQL, PySpark code, and accepts natural language quality requests
- **Optimization Agent**: Provides pattern-based performance recommendations
- **Learning Agent**: Captures execution patterns for continuous improvement

### **Self-Learning Capabilities**
- Learns from every execution
- Stores learning vectors in S3 for persistence
- Uses past patterns to inform future decisions
- Continuously improves recommendations

## 📦 Components

### 1. **Message Bus** (`strands_message_bus.py`)
Event-driven communication system enabling asynchronous agent coordination.

```python
from strands.strands_message_bus import get_message_bus, MessageType

bus = get_message_bus()
message = bus.create_message(
    sender='my_agent',
    message_type=MessageType.AGENT_REQUEST,
    payload={'data': 'value'},
    target='target_agent'
)
await bus.publish(message)
```

### 2. **Agent Base** (`strands_agent_base.py`)
Foundation for all Strands agents with autonomous operation and lifecycle management.

```python
from strands.strands_agent_base import StrandsAgent

class MyCustomAgent(StrandsAgent):
    async def process_message(self, message):
        # Process incoming messages
        return {'result': 'processed'}

    async def autonomous_cycle(self):
        # Perform periodic tasks
        pass
```

### 3. **Decision Agent** (`strands_decision_agent.py`)
ML-based platform selection using historical execution patterns.

**Features:**
- Similarity-based workload matching
- Platform scoring using historical success rates
- AI-powered recommendations via AWS Bedrock
- Real-time learning cache refresh

**How it learns:**
- Extracts features from workload characteristics
- Calculates similarity to historical executions using cosine similarity
- Aggregates platform performance metrics (success rate, execution time, efficiency)
- Stores decisions as learning vectors for future use

### 4. **Quality Agent** (`strands_quality_agent.py`)
Intelligent quality assessment with SQL/code analysis and NL parsing.

**Features:**
- **SQL Analysis**: Detects performance anti-patterns in queries
  - SELECT * usage
  - Missing WHERE clauses
  - Subqueries in WHERE
  - Function on indexed columns
  - Multiple joins without indexes

- **PySpark Code Analysis**: Identifies performance issues
  - `.count()` actions triggering full scans
  - `.collect()` pulling all data to driver
  - Missing broadcast join hints
  - Nested field access
  - Multiple actions without caching

- **Natural Language**: Converts NL to quality checks
  ```python
  "Check if all customer records have valid email addresses"
  ```
  Converts to structured checks for completeness, validity, etc.

### 5. **Optimization Agent** (`strands_optimization_agent.py`)
Pattern-based performance recommendations.

**Features:**
- Compares executions against performance baselines
- Identifies optimization opportunities
- Provides prioritized, actionable recommendations
- Estimates improvement impact
- Learns from successful execution patterns

### 6. **Learning Agent** (`strands_learning_agent.py`)
Captures comprehensive execution patterns.

**Features:**
- Aggregates data from all agents
- Creates rich learning vectors with:
  - Workload characteristics
  - Execution metrics
  - Quality scores
  - Optimization insights
  - AI-generated learning insights
- Stores vectors to S3 for persistence
- Broadcasts learning updates to all agents

### 7. **Coordinator** (`strands_coordinator.py`)
Manages multi-agent orchestration and lifecycle.

**Features:**
- Agent initialization and management
- Pipeline orchestration workflow
- Message routing coordination
- Execution monitoring
- Results aggregation

## 🚀 Quick Start

### Basic Usage

```python
import asyncio
from strands import StrandsCoordinator

async def main():
    # Create coordinator
    coordinator = StrandsCoordinator()

    # Initialize all agents
    await coordinator.initialize()

    # Run ETL pipeline
    result = await coordinator.orchestrate_pipeline(
        user_request="Process customer orders with quality checks",
        config_path="s3://bucket/config.json"
    )

    # Check results
    print(f"Status: {result['status']}")
    print(f"Platform: {result['decision']['selected_platform']}")
    print(f"Quality: {result['quality_reports'][0]['overall_score']}")

    # Get learning summary
    learning = await coordinator.get_learning_summary()
    print(f"Success Rate: {learning['success_rate']:.1%}")

    # Cleanup
    await coordinator.shutdown()

asyncio.run(main())
```

### Using Individual Agents

#### Quality Agent for SQL Analysis

```python
from strands import StrandsQualityAgent, get_message_bus, MessageType

# Start agent
agent = StrandsQualityAgent()
await agent.start()

# Send SQL for analysis
bus = get_message_bus()
msg = bus.create_message(
    sender='app',
    message_type=MessageType.QUALITY_CHECK,
    payload={
        'check_type': 'sql',
        'input': 'SELECT * FROM large_table WHERE UPPER(name) = "JOHN"'
    },
    target='quality_agent'
)

await bus.publish(msg)
```

#### Quality Agent with Natural Language

```python
msg = bus.create_message(
    sender='app',
    message_type=MessageType.QUALITY_CHECK,
    payload={
        'check_type': 'natural_language',
        'input': 'Check for missing values in customer_id and validate email formats'
    },
    target='quality_agent'
)

await bus.publish(msg)
```

## 📊 Learning System

### How Strands Learns

1. **Execution**: Pipeline runs on selected platform
2. **Observation**: All agents observe execution metrics
3. **Analysis**: Quality, optimization, and performance analyzed
4. **Capture**: Learning Agent creates comprehensive vector
5. **Storage**: Vector stored to S3 with full context
6. **Application**: Future decisions use historical patterns

### Learning Vector Structure

```json
{
  "vector_id": "uuid",
  "timestamp": "ISO-8601",
  "workload_characteristics": {
    "data_volume": "high",
    "complexity": "high",
    "criticality": "high"
  },
  "execution_metrics": {
    "platform_used": "glue",
    "execution_time_seconds": 2700,
    "data_quality_score": 0.98,
    "success": true
  },
  "decision_context": {
    "selected_platform": "glue",
    "decision_confidence": 0.92,
    "decision_reasoning": "High data volume favors Glue..."
  },
  "performance_indicators": {
    "efficiency_score": 0.88,
    "optimization_opportunities": 2
  },
  "learning_insights": "Execution performed well with Glue..."
}
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Strands Coordinator                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      Message Bus                             │
│           (Asynchronous Event-Driven Communication)          │
└─────────────────────────────────────────────────────────────┘
          │           │            │             │
          ▼           ▼            ▼             ▼
    ┌─────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
    │Decision │ │ Quality  │ │Optimiza- │ │ Learning │
    │ Agent   │ │  Agent   │ │tion Agent│ │  Agent   │
    └─────────┘ └──────────┘ └──────────┘ └──────────┘
          │           │            │             │
          ▼           ▼            ▼             ▼
    ┌─────────────────────────────────────────────────┐
    │         S3 Learning Vector Storage               │
    └─────────────────────────────────────────────────┘
```

## 🔧 Configuration

### S3 Bucket Structure

```
strands-etl-learning/
├── learning/
│   └── vectors/
│       ├── decision/
│       ├── quality/
│       ├── optimization/
│       └── learning/
├── quality/
│   └── reports/
└── optimization/
    └── recommendations/
```

### Environment Variables

```bash
AWS_REGION=us-east-1
STRANDS_S3_BUCKET=strands-etl-learning
STRANDS_BEDROCK_MODEL=anthropic.claude-3-sonnet-20240229-v1:0
```

## 📖 Examples

See the `examples/` directory:
- `example_quality_check.py`: SQL, code, and NL quality analysis
- `example_full_pipeline.py`: Complete pipeline orchestration
- `example_decision_agent.py`: Platform selection with learning

## 🔄 Integration with Existing Code

The `strands_enhanced_orchestrator.py` provides drop-in replacement for the original orchestrator:

```python
# Old way
from orchestrator.strands_orchestrator import StrandsOrchestrator

# New way (enhanced with agentic features)
from orchestrator.strands_enhanced_orchestrator import StrandsEnhancedOrchestrator

orchestrator = StrandsEnhancedOrchestrator()
result = orchestrator.orchestrate_pipeline(user_request, config_path)
# Returns same format + enhanced Strands features
```

## 🎯 Benefits

1. **Autonomous Operation**: Agents work independently without blocking
2. **Intelligent Decisions**: ML-based platform selection improves over time
3. **Comprehensive Quality**: Analyzes SQL, code, and data quality
4. **Actionable Insights**: Prioritized optimization recommendations
5. **Continuous Learning**: Every execution makes the system smarter
6. **Scalable**: Message-based architecture scales horizontally
7. **Maintainable**: Clear separation of concerns across agents

## 🔬 Advanced Features

### Custom Agents

Create custom agents by extending `StrandsAgent`:

```python
from strands.strands_agent_base import StrandsAgent

class MyMonitoringAgent(StrandsAgent):
    def __init__(self):
        super().__init__(agent_name="monitoring_agent", agent_type="monitoring")

    async def process_message(self, message):
        # Handle messages
        pass

    async def autonomous_cycle(self):
        # Periodic monitoring tasks
        pass

    def _get_subscribed_message_types(self):
        return [MessageType.EXECUTION_STARTED, MessageType.EXECUTION_COMPLETED]
```

### Message Priority

```python
# High priority message
await bus.publish(
    bus.create_message(..., priority=9)
)
```

### Agent Metrics

```python
# Get performance metrics for all agents
metrics = await coordinator.get_agent_metrics()

for agent_name, stats in metrics.items():
    print(f"{agent_name}: {stats['messages_processed']} messages processed")
```

## 📝 License

Copyright © 2024 Strands ETL Team

## 🤝 Contributing

Contributions welcome! This framework is designed to be extensible.

## 🐛 Troubleshooting

### Agents not starting
- Check AWS credentials are configured
- Verify S3 bucket exists and is accessible
- Ensure Python 3.9+

### Messages not routing
- Verify agents subscribed to correct message types
- Check message bus is started: `await start_message_bus()`

### Learning vectors not persisting
- Check S3 bucket permissions
- Verify bucket name in configuration
- Review CloudWatch logs for S3 errors

## 📚 Further Reading

- [AWS Bedrock Documentation](https://docs.aws.amazon.com/bedrock/)
- [Multi-Agent Systems](https://en.wikipedia.org/wiki/Multi-agent_system)
- [Event-Driven Architecture](https://aws.amazon.com/event-driven-architecture/)
