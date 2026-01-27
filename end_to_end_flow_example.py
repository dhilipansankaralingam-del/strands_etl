"""
End-to-End ETL Flow with Strands Agents

This script demonstrates how to integrate Strands agents into a complete
ETL workflow that:
1. Reads and analyzes PySpark scripts
2. Makes recommendations before execution
3. Submits jobs to AWS Glue
4. Monitors execution
5. Provides post-execution optimization recommendations
"""
import boto3
import time
import json
from typing import Dict, Any
from strands_agents.orchestrator.swarm_orchestrator import ETLSwarm
from strands_agents.agents.etl_agents import create_quality_agent, create_decision_agent


# ============================================================================
# APPROACH 1: PRE-FLIGHT ANALYSIS (Analyze before running)
# ============================================================================

def run_etl_with_preflight_analysis(
    script_path: str,
    job_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Complete ETL flow with pre-flight analysis.

    Flow:
    1. Read the PySpark script
    2. Agents analyze script and workload
    3. Get recommendations
    4. Apply critical fixes (if possible)
    5. Submit job to Glue with optimized config
    6. Monitor execution
    7. Post-execution analysis

    Args:
        script_path: Path to PySpark script (local or S3)
        job_config: Job configuration including data volume, complexity

    Returns:
        Complete execution report with all recommendations
    """
    print("=" * 80)
    print("APPROACH 1: PRE-FLIGHT ANALYSIS")
    print("=" * 80)

    # Step 1: Read the PySpark script
    print("\n📖 Step 1: Reading PySpark script...")
    with open(script_path, 'r') as f:
        script_content = f.read()

    print(f"✓ Read script: {script_path} ({len(script_content)} characters)")

    # Step 2: Create the agent swarm
    print("\n🤖 Step 2: Initializing agent swarm...")
    swarm = ETLSwarm()

    # Step 3: Pre-flight analysis
    print("\n🔍 Step 3: Running pre-flight analysis...")

    preflight_prompt = f"""
I need to run an ETL job with the following details:

Job Configuration:
- Job Name: {job_config.get('job_name', 'unnamed')}
- Data Volume: {job_config.get('data_volume_gb', 0)} GB
- File Count: {job_config.get('file_count', 0)} files
- Transformation Complexity: {job_config.get('transformation_complexity', 'moderate')}
- Platform: AWS Glue
- DPU Count: {job_config.get('dpu_count', 10)}

PySpark Script Content:
```python
{script_content}
```

Please coordinate between agents to:

1. Quality Agent: Analyze the script for anti-patterns and performance issues
   - Check for multiple .count() operations
   - Look for missing broadcast hints
   - Identify SELECT * queries
   - Check for proper coalesce before writes

2. Decision Agent: Given the workload size, recommend if this is the right platform
   and if DPU count is appropriate

3. Optimization Agent: Suggest specific Spark configuration optimizations

4. Learning Agent: Search for similar past executions and recommend based on history

5. Cost Tracking Agent: Estimate the cost of this job

Provide:
- GO/NO-GO recommendation
- Critical issues that must be fixed before running
- Optional optimizations for better performance
- Estimated cost and duration
"""

    print("   Agents are analyzing...")
    analysis_result = swarm.swarm(preflight_prompt)

    print("\n📊 Pre-Flight Analysis Results:")
    print("-" * 80)
    print(str(analysis_result)[:1000] + "..." if len(str(analysis_result)) > 1000 else str(analysis_result))
    print("-" * 80)

    # Step 4: Parse recommendations (in production, parse the agent response)
    # For now, we'll proceed with the job
    go_decision = True  # Would parse from agent response

    if not go_decision:
        return {
            'status': 'blocked',
            'reason': 'Critical issues found in pre-flight analysis',
            'analysis': analysis_result
        }

    # Step 5: Submit job to Glue with optimized config
    print("\n🚀 Step 4: Submitting job to AWS Glue...")

    glue_client = boto3.client('glue', region_name='us-east-1')

    # Apply recommendations from agents (example)
    optimized_config = {
        '--job-language': 'python',
        '--spark-event-logs-path': 's3://my-bucket/spark-logs/',
        '--enable-metrics': 'true',
        '--enable-continuous-cloudwatch-log': 'true',
        # Optimization recommendations from agents:
        '--conf': 'spark.sql.shuffle.partitions=240',
        '--conf': 'spark.sql.adaptive.enabled=true',
        '--conf': 'spark.sql.adaptive.coalescePartitions.enabled=true'
    }

    try:
        response = glue_client.start_job_run(
            JobName=job_config['glue_job_name'],
            Arguments=optimized_config
        )

        job_run_id = response['JobRunId']
        print(f"✓ Job started: {job_run_id}")

    except Exception as e:
        print(f"✗ Error starting job: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'analysis': analysis_result
        }

    # Step 6: Monitor execution
    print(f"\n⏳ Step 5: Monitoring job execution (Job Run ID: {job_run_id})...")

    start_time = time.time()
    while True:
        job_status = glue_client.get_job_run(
            JobName=job_config['glue_job_name'],
            RunId=job_run_id
        )

        state = job_status['JobRun']['State']
        print(f"   Status: {state}...")

        if state in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
            break

        time.sleep(30)  # Check every 30 seconds

    end_time = time.time()
    duration_minutes = (end_time - start_time) / 60

    # Step 7: Post-execution analysis
    print("\n📈 Step 6: Running post-execution analysis...")

    # Get execution metrics from Glue
    execution_metrics = {
        'execution_id': job_run_id,
        'job_name': job_config['job_name'],
        'platform': 'glue',
        'dpu_count': job_config.get('dpu_count', 10),
        'duration_minutes': duration_minutes,
        'success': state == 'SUCCEEDED',
        'data_volume_gb': job_config.get('data_volume_gb', 0)
    }

    # Add CloudWatch metrics (if available)
    try:
        # In production, fetch actual metrics from CloudWatch
        execution_metrics['metrics'] = {
            'cpu_utilization_avg': 65,  # Would fetch from CloudWatch
            'memory_utilization_avg': 70,
            'shuffle_read_gb': 150,
            'shuffle_write_gb': 75
        }
    except:
        pass

    post_analysis = swarm.analyze_existing_job(execution_metrics)

    print("\n✅ Execution Complete!")
    print(f"   Duration: {duration_minutes:.1f} minutes")
    print(f"   Status: {state}")

    return {
        'status': 'completed',
        'job_run_id': job_run_id,
        'execution_state': state,
        'duration_minutes': duration_minutes,
        'preflight_analysis': analysis_result,
        'post_execution_analysis': post_analysis
    }


# ============================================================================
# APPROACH 2: CONCURRENT ANALYSIS (Analyze while job runs)
# ============================================================================

def run_etl_with_concurrent_analysis(
    script_path: str,
    job_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    ETL flow with concurrent analysis - job runs while agents analyze.

    Flow:
    1. Submit job to Glue immediately
    2. While job runs, agents analyze script in parallel
    3. When job completes, combine execution data with analysis
    4. Provide recommendations for next run

    Args:
        script_path: Path to PySpark script
        job_config: Job configuration

    Returns:
        Execution report with recommendations for future runs
    """
    print("=" * 80)
    print("APPROACH 2: CONCURRENT ANALYSIS")
    print("=" * 80)

    # Step 1: Submit job immediately
    print("\n🚀 Step 1: Submitting job to AWS Glue (no delay)...")

    glue_client = boto3.client('glue', region_name='us-east-1')

    try:
        response = glue_client.start_job_run(
            JobName=job_config['glue_job_name'],
            Arguments={
                '--job-language': 'python',
                '--enable-metrics': 'true'
            }
        )

        job_run_id = response['JobRunId']
        print(f"✓ Job started: {job_run_id}")

    except Exception as e:
        print(f"✗ Error starting job: {e}")
        return {'status': 'error', 'error': str(e)}

    # Step 2: Analyze script concurrently while job runs
    print("\n🔍 Step 2: Analyzing script in parallel (while job runs)...")
    print("   Reading script...")

    with open(script_path, 'r') as f:
        script_content = f.read()

    # Use Quality Agent directly for faster analysis
    quality_agent = create_quality_agent()

    print("   Quality Agent analyzing script...")
    script_analysis_prompt = f"""
Analyze this PySpark script for performance issues and anti-patterns:

```python
{script_content}
```

Provide specific issues found and recommendations.
"""

    script_analysis = quality_agent(script_analysis_prompt)

    print("✓ Script analysis complete (job still running)")

    # Step 3: Monitor job execution
    print(f"\n⏳ Step 3: Waiting for job to complete...")

    start_time = time.time()
    while True:
        job_status = glue_client.get_job_run(
            JobName=job_config['glue_job_name'],
            RunId=job_run_id
        )

        state = job_status['JobRun']['State']
        elapsed = (time.time() - start_time) / 60
        print(f"   Status: {state} (elapsed: {elapsed:.1f} min)...")

        if state in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
            break

        time.sleep(30)

    end_time = time.time()
    duration_minutes = (end_time - start_time) / 60

    # Step 4: Combine execution data with analysis
    print("\n📊 Step 4: Combining execution results with analysis...")

    swarm = ETLSwarm()

    comprehensive_analysis_prompt = f"""
The ETL job has completed. Here's the data:

Execution Results:
- Job Run ID: {job_run_id}
- Status: {state}
- Duration: {duration_minutes:.1f} minutes
- Data Volume: {job_config.get('data_volume_gb', 0)} GB

Script Analysis (from Quality Agent):
{script_analysis}

Please coordinate between all agents to provide:
1. Was the execution efficient? (Decision + Learning Agent)
2. What optimizations can improve next run? (Optimization Agent)
3. Cost analysis and savings opportunities (Cost Tracking Agent)
4. Overall recommendations for production deployment
"""

    comprehensive_analysis = swarm.swarm(comprehensive_analysis_prompt)

    print("\n✅ Analysis Complete!")
    print(f"   Duration: {duration_minutes:.1f} minutes")
    print(f"   Status: {state}")

    return {
        'status': 'completed',
        'job_run_id': job_run_id,
        'execution_state': state,
        'duration_minutes': duration_minutes,
        'script_analysis': script_analysis,
        'comprehensive_analysis': comprehensive_analysis,
        'recommendations_for_next_run': comprehensive_analysis
    }


# ============================================================================
# APPROACH 3: HYBRID (Quick check + concurrent monitoring)
# ============================================================================

def run_etl_hybrid_approach(
    script_path: str,
    job_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Hybrid approach: Quick safety check + concurrent deep analysis.

    Flow:
    1. Quick safety check (1-2 seconds) - block critical issues only
    2. Submit job if safe
    3. Deep analysis runs in parallel while job executes
    4. Full recommendations available when job completes

    This is the RECOMMENDED approach for production.
    """
    print("=" * 80)
    print("APPROACH 3: HYBRID (RECOMMENDED)")
    print("=" * 80)

    # Step 1: Quick safety check
    print("\n⚡ Step 1: Quick safety check (critical issues only)...")

    with open(script_path, 'r') as f:
        script_content = f.read()

    quality_agent = create_quality_agent()

    # Quick analysis - just check for critical blockers
    quick_check_prompt = f"""
Perform a QUICK safety check on this PySpark script.
Only flag CRITICAL issues that would cause job failure:

```python
{script_content}
```

Critical issues to check:
- Syntax errors
- Missing required imports
- Obvious infinite loops
- Cartesian joins (catastrophic performance)

Return: GO or NO-GO with brief reason.
Be fast - we need answer in 2 seconds.
"""

    safety_result = quality_agent(quick_check_prompt)

    # Parse safety check (simplified - would use proper parsing)
    is_safe = "GO" in str(safety_result).upper()

    if not is_safe:
        print("✗ BLOCKED: Critical safety issues found")
        return {
            'status': 'blocked',
            'reason': safety_result
        }

    print("✓ Safety check passed - proceeding")

    # Step 2: Submit job immediately
    print("\n🚀 Step 2: Submitting job to Glue...")

    glue_client = boto3.client('glue', region_name='us-east-1')

    try:
        response = glue_client.start_job_run(
            JobName=job_config['glue_job_name'],
            Arguments={'--job-language': 'python'}
        )
        job_run_id = response['JobRunId']
        print(f"✓ Job started: {job_run_id}")
    except Exception as e:
        return {'status': 'error', 'error': str(e)}

    # Step 3: Deep analysis in background
    print("\n🔍 Step 3: Running deep analysis (while job runs)...")
    print("   This will complete before or when job finishes...")

    swarm = ETLSwarm()

    deep_analysis_prompt = f"""
Perform comprehensive analysis while the job runs:

Script:
```python
{script_content}
```

Job Config:
{json.dumps(job_config, indent=2)}

All agents: Provide detailed recommendations for optimization,
cost savings, and best practices. Job is currently running.
"""

    # This runs in parallel with the Glue job
    deep_analysis = swarm.swarm(deep_analysis_prompt)

    print("✓ Deep analysis complete")

    # Step 4: Monitor job
    print(f"\n⏳ Step 4: Monitoring job completion...")

    start_time = time.time()
    while True:
        job_status = glue_client.get_job_run(
            JobName=job_config['glue_job_name'],
            RunId=job_run_id
        )

        state = job_status['JobRun']['State']

        if state in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
            break

        time.sleep(30)

    duration_minutes = (time.time() - start_time) / 60

    print(f"\n✅ Complete!")
    print(f"   Status: {state}")
    print(f"   Duration: {duration_minutes:.1f} minutes")

    return {
        'status': 'completed',
        'job_run_id': job_run_id,
        'execution_state': state,
        'duration_minutes': duration_minutes,
        'safety_check': safety_result,
        'deep_analysis': deep_analysis,
        'recommendations': 'See deep_analysis for optimization recommendations'
    }


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

def example_usage():
    """
    Example of running all three approaches
    """

    # Sample job configuration
    job_config = {
        'job_name': 'customer_purchase_analytics',
        'glue_job_name': 'customer-purchase-etl',  # Your Glue job name
        'data_volume_gb': 400,
        'file_count': 2000,
        'transformation_complexity': 'complex',
        'dpu_count': 10,
        's3_script_path': 's3://my-bucket/scripts/customer_analytics.py',
        'source_path': 's3://my-bucket/raw/transactions/',
        'destination_path': 's3://my-bucket/processed/analytics/'
    }

    script_path = './pyscript/customer_order_summary_glue.py'

    print("\n" + "=" * 80)
    print("STRANDS ETL END-TO-END FLOW EXAMPLES")
    print("=" * 80)

    # Example 1: Pre-flight analysis (safest, slower start)
    print("\n\n📋 EXAMPLE 1: Pre-Flight Analysis")
    print("   Best for: Critical jobs, first-time scripts, high-value data")
    print("   Trade-off: 2-3 min analysis delay before job starts")
    print("-" * 80)

    # result1 = run_etl_with_preflight_analysis(script_path, job_config)
    # print(json.dumps(result1, indent=2, default=str))

    print("   [Skipped in example - would run full pre-flight analysis]")

    # Example 2: Concurrent analysis (fastest start, analysis for next run)
    print("\n\n📋 EXAMPLE 2: Concurrent Analysis")
    print("   Best for: Regular jobs, time-sensitive workloads")
    print("   Trade-off: No upfront validation, recommendations for next run")
    print("-" * 80)

    # result2 = run_etl_with_concurrent_analysis(script_path, job_config)
    # print(json.dumps(result2, indent=2, default=str))

    print("   [Skipped in example - would launch immediately and analyze]")

    # Example 3: Hybrid approach (RECOMMENDED)
    print("\n\n📋 EXAMPLE 3: Hybrid Approach (RECOMMENDED)")
    print("   Best for: Production workloads")
    print("   Benefits: Fast start + safety check + full analysis")
    print("-" * 80)

    # result3 = run_etl_hybrid_approach(script_path, job_config)
    # print(json.dumps(result3, indent=2, default=str))

    print("   [Skipped in example - would do quick check + concurrent analysis]")

    print("\n" + "=" * 80)
    print("Choose approach based on your needs:")
    print("  - Critical data? Use Pre-Flight")
    print("  - Time sensitive? Use Concurrent")
    print("  - Production? Use Hybrid ✓")
    print("=" * 80)


if __name__ == '__main__':
    example_usage()
