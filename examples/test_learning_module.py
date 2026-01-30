"""
Example: Testing the Learning Module
=====================================
This script demonstrates how to use the Learning Module for:
1. Capturing execution data
2. Analyzing patterns
3. Training the model
4. Getting predictions
5. Asking questions (insights)
"""

import json
import sys
sys.path.insert(0, '..')

from learning_module import LearningModule


def create_sample_vectors(learning: LearningModule):
    """Create sample learning vectors for testing."""
    print("\n" + "=" * 60)
    print("STEP 1: Creating Sample Learning Vectors")
    print("=" * 60)

    # Sample execution contexts (simulating completed runs)
    sample_executions = [
        {
            'pipeline_id': 'pipeline-001',
            'start_time': '2024-01-10T10:00:00',
            'end_time': '2024-01-10T10:30:00',
            'agent_mode': 'recommend',
            'selected_platform': 'glue',
            'config': {
                'workload': {
                    'name': 'daily_sales_etl',
                    'data_volume': 'medium',
                    'complexity': 'low',
                    'criticality': 'medium',
                    'time_sensitivity': 'low'
                }
            },
            'execution_result': {
                'platform': 'glue',
                'execution_type': 'existing_job',
                'job_name': 'daily-sales-job',
                'status': 'completed',
                'final_status': 'SUCCEEDED'
            },
            'quality_report': {'overall_score': 0.95},
            'optimization': {'efficiency_score': 0.85, 'cost_efficiency': 0.80}
        },
        {
            'pipeline_id': 'pipeline-002',
            'start_time': '2024-01-11T14:00:00',
            'end_time': '2024-01-11T16:00:00',
            'agent_mode': 'decide',
            'selected_platform': 'emr',
            'config': {
                'workload': {
                    'name': 'customer_360_analytics',
                    'data_volume': 'high',
                    'complexity': 'high',
                    'criticality': 'high',
                    'time_sensitivity': 'medium'
                }
            },
            'execution_result': {
                'platform': 'emr',
                'execution_type': 'existing_cluster',
                'job_name': 'customer-360-step',
                'status': 'completed',
                'final_status': 'COMPLETED'
            },
            'quality_report': {'overall_score': 0.92},
            'optimization': {'efficiency_score': 0.78, 'cost_efficiency': 0.70}
        },
        {
            'pipeline_id': 'pipeline-003',
            'start_time': '2024-01-12T08:00:00',
            'end_time': '2024-01-12T08:05:00',
            'agent_mode': 'recommend',
            'selected_platform': 'lambda',
            'config': {
                'workload': {
                    'name': 'event_processor',
                    'data_volume': 'low',
                    'complexity': 'low',
                    'criticality': 'low',
                    'time_sensitivity': 'high'
                }
            },
            'execution_result': {
                'platform': 'lambda',
                'execution_type': 'existing_function',
                'job_name': 'event-processor-lambda',
                'status': 'completed',
                'final_status': 'SUCCEEDED'
            },
            'quality_report': {'overall_score': 0.98},
            'optimization': {'efficiency_score': 0.95, 'cost_efficiency': 0.95}
        },
        {
            'pipeline_id': 'pipeline-004',
            'start_time': '2024-01-13T10:00:00',
            'end_time': '2024-01-13T10:45:00',
            'agent_mode': 'recommend',
            'selected_platform': 'glue',
            'config': {
                'workload': {
                    'name': 'inventory_sync',
                    'data_volume': 'medium',
                    'complexity': 'medium',
                    'criticality': 'high',
                    'time_sensitivity': 'medium'
                }
            },
            'execution_result': {
                'platform': 'glue',
                'execution_type': 'existing_job',
                'job_name': 'inventory-sync-job',
                'status': 'failed',
                'final_status': 'FAILED',
                'error': 'OutOfMemoryError: GC overhead limit exceeded'
            },
            'quality_report': {'overall_score': 0.0},
            'optimization': {'efficiency_score': 0.0, 'cost_efficiency': 0.0}
        },
        {
            'pipeline_id': 'pipeline-005',
            'start_time': '2024-01-14T10:00:00',
            'end_time': '2024-01-14T11:30:00',
            'agent_mode': 'decide',
            'selected_platform': 'glue',
            'config': {
                'workload': {
                    'name': 'inventory_sync',
                    'data_volume': 'medium',
                    'complexity': 'medium',
                    'criticality': 'high',
                    'time_sensitivity': 'medium'
                }
            },
            'execution_result': {
                'platform': 'glue',
                'execution_type': 'existing_job',
                'job_name': 'inventory-sync-job-v2',
                'status': 'completed',
                'final_status': 'SUCCEEDED',
                'job_run_details': {
                    'DPUSeconds': 3600,
                    'MaxCapacity': 10,
                    'NumberOfWorkers': 10,
                    'ExecutionTime': 5400
                }
            },
            'quality_report': {'overall_score': 0.94},
            'optimization': {'efficiency_score': 0.82, 'cost_efficiency': 0.75}
        }
    ]

    for i, context in enumerate(sample_executions, 1):
        vector = learning.capture_execution(context)
        print(f"  ✓ Created vector {i}: {vector['vector_id'][:8]}... ({context['config']['workload']['name']})")

    print(f"\n  Total vectors created: {len(sample_executions)}")


def test_analyze_patterns(learning: LearningModule):
    """Test pattern analysis."""
    print("\n" + "=" * 60)
    print("STEP 2: Analyzing Patterns")
    print("=" * 60)

    analysis = learning.analyze_patterns()

    print("\n📊 Summary:")
    summary = analysis.get('summary', {})
    print(f"  Total runs: {summary.get('total_runs', 0)}")
    print(f"  Successful: {summary.get('successful_runs', 0)}")
    print(f"  Failed: {summary.get('failed_runs', 0)}")
    print(f"  Success rate: {summary.get('success_rate', 0):.1%}")

    print("\n📊 Platform Analysis:")
    for platform, data in analysis.get('platform_analysis', {}).items():
        print(f"  {platform}: {data.get('count', 0)} runs, {data.get('success_rate', 0):.1%} success")

    print("\n📊 Recommendations:")
    for rec in analysis.get('recommendations', []):
        print(f"  [{rec.get('type')}] {rec.get('recommendation')}")


def test_train_model(learning: LearningModule):
    """Test model training."""
    print("\n" + "=" * 60)
    print("STEP 3: Training Platform Predictor")
    print("=" * 60)

    result = learning.train_platform_predictor()

    if 'error' in result:
        print(f"  ⚠ {result['error']}")
        return

    print(f"  Model ID: {result.get('model_id', 'N/A')[:8]}...")
    print(f"  Training samples: {result.get('training_samples', 0)}")
    print("\n  Learned Rules:")

    for category, rule in result.get('rules', {}).items():
        print(f"    {category}: {rule.get('recommended_platform')} (confidence: {rule.get('confidence', 0):.1%})")


def test_predictions(learning: LearningModule):
    """Test predictions."""
    print("\n" + "=" * 60)
    print("STEP 4: Testing Predictions")
    print("=" * 60)

    test_workloads = [
        {'data_volume': 'low', 'complexity': 'low', 'name': 'Small job'},
        {'data_volume': 'high', 'complexity': 'high', 'name': 'Large complex job'},
        {'data_volume': 'medium', 'complexity': 'medium', 'name': 'Medium job'},
    ]

    for workload in test_workloads:
        prediction = learning.predict_platform(workload)
        print(f"\n  Workload: {workload['name']} (volume={workload['data_volume']}, complexity={workload['complexity']})")
        print(f"  → Recommended: {prediction.get('recommended_platform')}")
        print(f"  → Confidence: {prediction.get('confidence', 0):.1%}")


def test_insights(learning: LearningModule):
    """Test asking questions for insights."""
    print("\n" + "=" * 60)
    print("STEP 5: Getting Insights (AI-Powered)")
    print("=" * 60)

    questions = [
        "Which platform has the best success rate?",
        "What are the main causes of failures?",
        "How can I reduce costs for high-volume jobs?"
    ]

    for question in questions:
        print(f"\n  Question: {question}")
        try:
            result = learning.get_insights(question)
            if 'error' in result:
                print(f"  ⚠ Error: {result['error']}")
                print(f"  Fallback: {result.get('fallback_analysis', {})}")
            else:
                answer = result.get('answer', 'No answer')
                if len(answer) > 200:
                    answer = answer[:200] + "..."
                print(f"  Answer: {answer}")
        except Exception as e:
            print(f"  ⚠ Could not get insights: {e}")


def test_reports(learning: LearningModule):
    """Test report generation."""
    print("\n" + "=" * 60)
    print("STEP 6: Generating Reports")
    print("=" * 60)

    report_types = ['summary', 'cost', 'performance']

    for report_type in report_types:
        report = learning.generate_report(report_type)
        print(f"\n  📄 {report_type.upper()} Report:")
        print(f"     Generated at: {report.get('generated_at', 'N/A')}")

        if report_type == 'summary':
            summary = report.get('summary', {})
            print(f"     Total runs: {summary.get('total_runs', 0)}")
            print(f"     Success rate: {summary.get('success_rate', 0):.1%}")

        elif report_type == 'cost':
            cost = report.get('cost_analysis', {}).get('by_platform', {})
            for platform, data in cost.items():
                print(f"     {platform}: avg ${data.get('avg_cost', 0):.2f}")

        elif report_type == 'performance':
            trends = report.get('performance_trends', {}).get('trends', {})
            print(f"     Execution time trend: {trends.get('execution_time_trend', 'N/A')}")
            print(f"     Cost trend: {trends.get('cost_trend', 'N/A')}")


def main():
    """Run all tests."""
    print("=" * 60)
    print("STRANDS ETL - LEARNING MODULE TEST")
    print("=" * 60)

    # Initialize learning module
    learning = LearningModule(bucket_name='strands-etl-learning')

    # Run tests
    create_sample_vectors(learning)
    test_analyze_patterns(learning)
    test_train_model(learning)
    test_predictions(learning)
    test_insights(learning)
    test_reports(learning)

    print("\n" + "=" * 60)
    print("✓ All tests completed!")
    print("=" * 60)


if __name__ == '__main__':
    main()
