# Learning Module - Baby Steps Guide

## Overview

The Learning Module captures, analyzes, and learns from ETL execution data to provide intelligent insights and predictions.

### What It Does

| Feature | Description |
|---------|-------------|
| **Capture** | Stores execution data after each run |
| **Analyze** | Identifies patterns in historical data |
| **Train** | Builds prediction models from data |
| **Predict** | Recommends platforms based on learned patterns |
| **Insights** | Answers questions using AI (Bedrock) |

---

## Prerequisites

Before starting, ensure:
- S3 bucket `strands-etl-learning` exists
- Bedrock access is enabled (for insights feature)
- AWS region is set: `export AWS_DEFAULT_REGION=us-west-2`

---

## Step 1: Create S3 Bucket for Learning

```bash
aws s3 mb s3://strands-etl-learning --region us-west-2
```

**Verify:**
```bash
aws s3 ls | grep learning
```

---

## Step 2: Test Learning Module Import

```bash
python -c "
from learning_module import LearningModule

learning = LearningModule(bucket_name='strands-etl-learning')
print('✓ Learning Module initialized')
"
```

**Expected:**
```
✓ Learning Module initialized
```

---

## Step 3: Capture Sample Execution Data

```python
python -c "
from learning_module import LearningModule

learning = LearningModule()

# Simulate a completed execution
context = {
    'pipeline_id': 'test-001',
    'start_time': '2024-01-15T10:00:00',
    'end_time': '2024-01-15T10:30:00',
    'agent_mode': 'recommend',
    'selected_platform': 'glue',
    'config': {
        'workload': {
            'name': 'test_etl',
            'data_volume': 'medium',
            'complexity': 'low',
            'criticality': 'medium',
            'time_sensitivity': 'low'
        }
    },
    'execution_result': {
        'platform': 'glue',
        'execution_type': 'existing_job',
        'job_name': 'my-test-job',
        'status': 'completed',
        'final_status': 'SUCCEEDED'
    },
    'quality_report': {'overall_score': 0.95},
    'optimization': {'efficiency_score': 0.85, 'cost_efficiency': 0.80}
}

vector = learning.capture_execution(context)
print(f'✓ Captured vector: {vector[\"vector_id\"][:8]}...')
"
```

**Expected:**
```
✓ Captured vector: abc12345...
```

---

## Step 4: Verify Data in S3

```bash
aws s3 ls s3://strands-etl-learning/learning/vectors/ --recursive
```

**Expected:**
```
2024-01-15 10:30:00        1234 learning/vectors/abc12345-....json
```

---

## Step 5: Analyze Patterns

```bash
python -c "
from learning_module import LearningModule
import json

learning = LearningModule()
analysis = learning.analyze_patterns()

print('=== ANALYSIS RESULTS ===')
print(f'Total executions: {analysis[\"summary\"][\"total_runs\"]}')
print(f'Success rate: {analysis[\"summary\"][\"success_rate\"]:.1%}')
print()
print('Platform Performance:')
for platform, data in analysis.get('platform_analysis', {}).items():
    print(f'  {platform}: {data[\"count\"]} runs, {data[\"success_rate\"]:.1%} success')
"
```

**Expected:**
```
=== ANALYSIS RESULTS ===
Total executions: 1
Success rate: 100.0%

Platform Performance:
  glue: 1 runs, 100.0% success
```

---

## Step 6: Add More Sample Data (for Training)

```bash
python examples/test_learning_module.py
```

This creates 5 sample executions with different:
- Platforms (glue, emr, lambda)
- Workload types (low/medium/high volume)
- Outcomes (success/failure)

---

## Step 7: Train the Model

```bash
python -c "
from learning_module import LearningModule
import json

learning = LearningModule()
result = learning.train_platform_predictor()

print('=== TRAINING RESULTS ===')
print(f'Model ID: {result[\"model_id\"][:8]}...')
print(f'Training samples: {result[\"training_samples\"]}')
print()
print('Learned Rules:')
for category, rule in result.get('rules', {}).items():
    print(f'  {category}: use {rule[\"recommended_platform\"]} (confidence: {rule[\"confidence\"]:.0%})')
"
```

**Expected:**
```
=== TRAINING RESULTS ===
Model ID: xyz78901...
Training samples: 4

Learned Rules:
  high_volume: use emr (confidence: 100%)
  medium_volume: use glue (confidence: 67%)
  low_volume: use lambda (confidence: 100%)
  high_complexity: use emr (confidence: 100%)
  medium_complexity: use glue (confidence: 67%)
  low_complexity: use lambda (confidence: 50%)
```

---

## Step 8: Test Predictions

```bash
python -c "
from learning_module import LearningModule

learning = LearningModule()

# Test different workloads
workloads = [
    {'data_volume': 'low', 'complexity': 'low'},
    {'data_volume': 'high', 'complexity': 'high'},
    {'data_volume': 'medium', 'complexity': 'medium'},
]

print('=== PREDICTIONS ===')
for w in workloads:
    pred = learning.predict_platform(w)
    print(f'Volume={w[\"data_volume\"]}, Complexity={w[\"complexity\"]}')
    print(f'  → Recommended: {pred[\"recommended_platform\"]} (confidence: {pred[\"confidence\"]:.0%})')
    print()
"
```

**Expected:**
```
=== PREDICTIONS ===
Volume=low, Complexity=low
  → Recommended: lambda (confidence: 75%)

Volume=high, Complexity=high
  → Recommended: emr (confidence: 100%)

Volume=medium, Complexity=medium
  → Recommended: glue (confidence: 67%)
```

---

## Step 9: Ask Questions (Get Insights)

```bash
python -c "
from learning_module import LearningModule

learning = LearningModule()

question = 'Which platform has the best success rate?'
print(f'Question: {question}')
print()

result = learning.get_insights(question)

if 'error' in result:
    print(f'Error: {result[\"error\"]}')
else:
    print(f'Answer: {result.get(\"answer\", \"No answer\")[:500]}')
"
```

**Note:** Requires Bedrock access. If not available, you'll see a fallback response.

---

## Step 10: Generate Reports

```bash
# Summary Report
python learning_module.py report --report-type summary

# Cost Report
python learning_module.py report --report-type cost

# Performance Report
python learning_module.py report --report-type performance
```

---

## Step 11: Use with Orchestrator

The orchestrator automatically captures learning data after each run:

```python
from orchestrator.strands_orchestrator import StrandsOrchestrator
from learning_module import LearningModule

# Run a job
orchestrator = StrandsOrchestrator()
result = orchestrator.orchestrate_pipeline(
    user_request="Run daily ETL",
    config_path="./config/simple_usecase.json"
)

# Data is automatically captured!
# Now query the learning module
learning = LearningModule()
analysis = learning.analyze_patterns()
print(f"Total runs: {analysis['summary']['total_runs']}")
```

---

## CLI Reference

```bash
# Analyze all data
python learning_module.py analyze

# Train the model
python learning_module.py train

# Get a prediction
python learning_module.py predict --workload '{"data_volume": "high", "complexity": "high"}'

# Ask a question
python learning_module.py insights --question "What causes most failures?"

# Generate report
python learning_module.py report --report-type detailed
```

---

## How Learning Improves Over Time

```
Run 1: Glue job succeeds (high volume)
       → Learning: "high volume + glue = success"

Run 2: Lambda fails (high volume)
       → Learning: "high volume + lambda = failure"

Run 3: EMR succeeds (high volume)
       → Learning: "high volume + emr = success"

Run 4: User asks for recommendation (high volume job)
       → Model predicts: "Use Glue or EMR, avoid Lambda"
       → Confidence based on historical success rates
```

---

## Data Flow Diagram

```
┌─────────────────┐
│ ETL Execution   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Capture Data    │ ──→ S3: learning/vectors/
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Analyze Patterns│ ──→ S3: learning/insights/
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Train Model     │ ──→ S3: learning/trained/
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Predict/Insights│ ──→ Better recommendations
└─────────────────┘
```

---

## Troubleshooting

### Error: NoSuchBucket
```bash
aws s3 mb s3://strands-etl-learning --region us-west-2
```

### Error: Not enough data to train
Run at least 5 successful executions before training.

### Error: Bedrock AccessDenied
```bash
aws iam attach-role-policy --role-name YOUR_ROLE --policy-arn arn:aws:iam::aws:policy/AmazonBedrockFullAccess
```

### No predictions available
Train the model first:
```bash
python learning_module.py train
```

---

## Success Checklist

- [ ] S3 bucket created
- [ ] Learning module imports correctly
- [ ] Sample data captured
- [ ] Analysis runs successfully
- [ ] Model trained
- [ ] Predictions working
- [ ] Reports generated

---

**Done!** You now have a working Learning Module that improves recommendations over time.
