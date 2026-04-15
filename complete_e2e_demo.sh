#!/bin/bash
# Complete End-to-End Demo for Strands ETL Framework
# This script demonstrates the full flow from execution to dashboard

set -e

echo "=========================================="
echo "STRANDS ETL FRAMEWORK - COMPLETE E2E DEMO"
echo "=========================================="
echo ""

# Set region
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-west-2}
echo "Using AWS Region: $AWS_DEFAULT_REGION"

# Navigate to project directory
cd "$(dirname "$0")"

echo ""
echo "=========================================="
echo "Step 1: Verify Prerequisites"
echo "=========================================="

# Check if S3 buckets exist
echo "Checking S3 buckets..."
if aws s3 ls s3://strands-etl-learning 2>/dev/null; then
    echo "  [OK] strands-etl-learning bucket exists"
else
    echo "  [CREATE] Creating strands-etl-learning bucket..."
    aws s3 mb s3://strands-etl-learning --region $AWS_DEFAULT_REGION
fi

if aws s3 ls s3://strands-etl-data 2>/dev/null; then
    echo "  [OK] strands-etl-data bucket exists"
else
    echo "  [CREATE] Creating strands-etl-data bucket..."
    aws s3 mb s3://strands-etl-data --region $AWS_DEFAULT_REGION
fi

echo ""
echo "=========================================="
echo "Step 2: Run Existing Glue Job (Dry Run)"
echo "=========================================="

if [ -f config/run_existing_job.json ]; then
    echo "Running dry-run with recommend mode..."
    python -m orchestrator.strands_orchestrator \
        --config config/run_existing_job.json \
        --request "Execute daily sales ETL job" \
        --mode recommend \
        --dry-run || echo "  [WARN] Dry run completed with warnings"
else
    echo "  [SKIP] Config file not found. Creating sample config..."
    mkdir -p config
    cat > config/run_existing_job.json << 'EOF'
{
    "pipeline_name": "sales_daily_pipeline",
    "description": "Run existing daily sales ETL job",
    "workload": {
        "name": "daily_sales_processing",
        "data_volume": "medium",
        "complexity": "medium",
        "criticality": "high",
        "time_sensitivity": "normal"
    },
    "execution": {
        "platform": "glue",
        "glue_job_name": "sales-etl-daily",
        "create_job": false
    }
}
EOF
    echo "  [OK] Sample config created at config/run_existing_job.json"
fi

echo ""
echo "=========================================="
echo "Step 3: Get Platform Recommendation"
echo "=========================================="

if [ -f config/agent_decides.json ]; then
    echo "Getting platform recommendation..."
    python -m orchestrator.strands_orchestrator \
        --config config/agent_decides.json \
        --recommend-only || echo "  [WARN] Recommendation completed with warnings"
else
    echo "  [SKIP] Agent config not found. Creating sample..."
    cat > config/agent_decides.json << 'EOF'
{
    "pipeline_name": "smart_sales_pipeline",
    "description": "Let agent decide the optimal platform",
    "workload": {
        "name": "smart_sales_processing",
        "data_volume": "high",
        "complexity": "high",
        "criticality": "high",
        "time_sensitivity": "urgent"
    },
    "execution": {
        "available_platforms": ["glue", "emr", "lambda"],
        "glue_job_name": "sales-etl-weekly"
    },
    "scripts": {
        "pyspark": "s3://strands-etl-scripts/etl/sales_etl_job.py"
    }
}
EOF
    echo "  [OK] Agent config created at config/agent_decides.json"
fi

echo ""
echo "=========================================="
echo "Step 4: Simulate Execution Data"
echo "=========================================="

echo "Creating simulated learning vectors for demo..."
python << 'PYEOF'
import json
import uuid
import boto3
from datetime import datetime, timedelta
import random

s3 = boto3.client('s3')
bucket = 'strands-etl-learning'

# Create 10 simulated execution vectors
platforms = ['glue', 'glue', 'glue', 'lambda', 'glue', 'emr', 'glue', 'lambda', 'glue', 'glue']
statuses = ['completed', 'completed', 'completed', 'completed', 'failed', 'completed', 'completed', 'completed', 'completed', 'completed']
volumes = ['medium', 'high', 'low', 'low', 'medium', 'high', 'medium', 'low', 'high', 'medium']
complexities = ['medium', 'high', 'low', 'low', 'medium', 'high', 'medium', 'low', 'high', 'medium']

print("Creating simulated learning data...")

for i in range(10):
    vector = {
        'vector_id': str(uuid.uuid4()),
        'timestamp': (datetime.utcnow() - timedelta(hours=i*2)).isoformat(),
        'pipeline_id': str(uuid.uuid4()),
        'workload': {
            'name': f'workload_{i+1}',
            'data_volume': volumes[i],
            'complexity': complexities[i],
            'criticality': 'high' if i % 3 == 0 else 'medium',
            'time_sensitivity': 'urgent' if i % 4 == 0 else 'normal'
        },
        'execution': {
            'platform': platforms[i],
            'execution_type': 'existing_job',
            'job_name': f'sample-job-{i+1}',
            'status': statuses[i],
            'final_status': 'SUCCEEDED' if statuses[i] == 'completed' else 'FAILED',
            'error': 'Timeout error' if statuses[i] == 'failed' else None
        },
        'metrics': {
            'execution_time_seconds': random.randint(60, 600),
            'dpu_seconds': random.randint(100, 1000) if platforms[i] == 'glue' else None,
            'estimated_cost_usd': round(random.uniform(0.5, 3.0), 4)
        },
        'quality': {
            'overall_score': round(random.uniform(0.85, 0.99), 2),
            'data_quality_score': round(random.uniform(0.90, 0.99), 2)
        },
        'optimization': {
            'efficiency_score': round(random.uniform(0.75, 0.95), 2),
            'cost_efficiency': round(random.uniform(0.70, 0.90), 2)
        },
        'agent_decisions': {
            'mode': 'recommend' if i % 2 == 0 else 'decide',
            'selected_platform': platforms[i]
        }
    }

    key = f"learning/vectors/{vector['vector_id']}.json"
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(vector, indent=2),
            ContentType='application/json'
        )
        print(f"  Created: {vector['workload']['name']} ({platforms[i]}, {statuses[i]})")
    except Exception as e:
        print(f"  Error: {e}")

print("\n  [OK] Created 10 simulated execution records")
PYEOF

echo ""
echo "=========================================="
echo "Step 5: Analyze Patterns"
echo "=========================================="

echo "Analyzing historical patterns..."
python learning_module.py analyze

echo ""
echo "=========================================="
echo "Step 6: Train Platform Predictor"
echo "=========================================="

echo "Training prediction model..."
python learning_module.py train

echo ""
echo "=========================================="
echo "Step 7: Predict Platform for New Workload"
echo "=========================================="

echo "Predicting platform for high-volume, high-complexity workload..."
python learning_module.py predict --workload '{"data_volume": "high", "complexity": "high"}'

echo ""
echo "=========================================="
echo "Step 8: Get AI Insights"
echo "=========================================="

echo "Asking: What is the best platform for my workloads?"
python learning_module.py insights --question "What is the best platform based on my execution history?" || echo "  [WARN] Insights may require Bedrock access"

echo ""
echo "=========================================="
echo "Step 9: Generate Reports"
echo "=========================================="

echo "Generating summary report..."
python learning_module.py report --report-type summary

echo ""
echo "Generating cost report..."
python learning_module.py report --report-type cost

echo ""
echo "=========================================="
echo "Step 10: Run Cost Analysis"
echo "=========================================="

echo "Running detailed cost analysis..."
python examples/cost_analysis.py

echo ""
echo "=========================================="
echo "Step 11: Run Full Analysis"
echo "=========================================="

echo "Running comprehensive analysis..."
python examples/full_analysis.py

echo ""
echo "=========================================="
echo "Step 12: Generate Dashboard"
echo "=========================================="

echo "Generating HTML dashboard..."
python examples/run_dashboard.py

echo ""
echo "=========================================="
echo "=========================================="
echo "DEMO COMPLETE!"
echo "=========================================="
echo ""
echo "Generated Outputs:"
echo "  - Dashboard:    output/dashboard.html"
echo "  - Full Report:  output/full_analysis_report.json"
echo ""
echo "Next Steps:"
echo "  1. Open output/dashboard.html in a browser"
echo "  2. Run actual Glue jobs to capture real data"
echo "  3. Re-run analysis to see updated insights"
echo ""
echo "=========================================="
