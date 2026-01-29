"""
Config-Driven ETL Framework - Comprehensive Usage Examples

This example demonstrates how to use the config-driven Strands ETL framework
where all job definitions are in etl_config.json.

Features demonstrated:
1. Loading and validating configuration
2. Auto-detection of table sizes
3. Data flow analysis
4. Agent-based platform selection
5. User preference with agent recommendation
6. Quality checks from config
7. Running jobs by job_id
8. Running all enabled jobs
"""
from strands_agents.orchestrator.config_driven_orchestrator import ConfigDrivenOrchestrator
from strands_agents.orchestrator.config_loader import ConfigLoader


def example_1_config_validation():
    """
    Example 1: Load and validate configuration
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 1: Configuration Loading and Validation")
    print("=" * 80)

    loader = ConfigLoader('./etl_config.json')

    # Load configuration
    print("\n📂 Loading configuration...")
    config = loader.load()

    print(f"✓ Loaded config version: {config.get('config_version', 'unknown')}")
    print(f"✓ Total jobs defined: {len(config.get('jobs', []))}")

    # Validate configuration
    print("\n🔍 Validating configuration...")
    validation = loader.validate_config()

    if validation['valid']:
        print("✓ Configuration is valid")
    else:
        print(f"✗ Found {len(validation['errors'])} errors:")
        for error in validation['errors']:
            print(f"  - {error}")

    if validation['warnings']:
        print(f"\n⚠ {len(validation['warnings'])} warnings:")
        for warning in validation['warnings']:
            print(f"  - {warning}")

    # List enabled jobs
    print("\n📋 Enabled jobs:")
    enabled = loader.list_enabled_jobs()
    for job in enabled:
        print(f"  - {job['job_id']}: {job['job_name']}")
        print(f"    Data sources: {len(job.get('data_sources', []))}")
        print(f"    Platform preference: {job.get('platform', {}).get('user_preference', 'auto')}")


def example_2_inspect_job_config():
    """
    Example 2: Inspect a specific job configuration
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Inspecting Job Configuration")
    print("=" * 80)

    loader = ConfigLoader('./etl_config.json')
    loader.load()

    job_id = 'customer_order_summary'
    print(f"\n🔎 Getting job: {job_id}")

    job = loader.get_job_by_id(job_id)

    if not job:
        print(f"✗ Job {job_id} not found")
        return

    print(f"\n✓ Found job: {job['job_name']}")
    print(f"  Description: {job.get('description', 'N/A')}")
    print(f"  Enabled: {job.get('enabled', False)}")

    # Execution details
    execution = job.get('execution', {})
    print(f"\n📝 Execution:")
    print(f"  Script: {execution.get('script_path', 'N/A')}")
    print(f"  Type: {execution.get('script_type', 'N/A')}")
    print(f"  Schedule: {execution.get('schedule', 'N/A')}")
    print(f"  Timeout: {execution.get('timeout_minutes', 0)} minutes")

    # Platform configuration
    platform = job.get('platform', {})
    print(f"\n🖥️ Platform:")
    print(f"  User preference: {platform.get('user_preference', 'None (auto-select)')}")
    print(f"  Allow agent override: {platform.get('allow_agent_override', True)}")
    print(f"  DPU count: {platform.get('dpu_count', 0)}")
    print(f"  Auto-adjust DPU: {platform.get('auto_adjust_dpu', False)}")

    # Data sources
    data_sources = job.get('data_sources', [])
    print(f"\n📊 Data Sources ({len(data_sources)}):")
    for ds in data_sources:
        print(f"\n  {ds['name']}:")
        print(f"    Source type: {ds.get('source_type', ds.get('type', 'unknown'))}")

        if ds.get('source_type') == 'glue_catalog':
            print(f"    Database: {ds.get('database', 'N/A')}")
            print(f"    Table: {ds.get('table', 'N/A')}")
        elif ds.get('source_type') == 's3':
            print(f"    Path: {ds.get('path', 'N/A')}")

        print(f"    Auto-detect size: {ds.get('auto_detect_size', False)}")
        print(f"    Role in flow: {ds.get('role_in_flow', 'unknown')}")

        if ds.get('joins_with'):
            print(f"    Joins with: {', '.join(ds.get('joins_with', []))}")

        if ds.get('size_gb'):
            print(f"    Size: {ds.get('size_gb', 0)} GB (detected)")

    # Data quality
    data_quality = job.get('data_quality', {})
    if data_quality.get('enabled', False):
        print(f"\n✅ Data Quality:")
        print(f"  Completeness checks: {len(data_quality.get('completeness_checks', []))}")
        print(f"  Accuracy rules: {len(data_quality.get('accuracy_rules', []))}")
        print(f"  Duplicate check: {data_quality.get('duplicate_check', {}).get('enabled', False)}")
        print(f"  Fail on error: {data_quality.get('fail_on_error', True)}")

        # Show critical checks
        critical_checks = [
            check for check in data_quality.get('completeness_checks', [])
            if check.get('severity') == 'critical'
        ]
        if critical_checks:
            print(f"\n  Critical fields:")
            for check in critical_checks:
                print(f"    - {check.get('field', 'unknown')}: {check.get('action', 'warn')}")

    # Compliance
    compliance = job.get('compliance', {})
    if compliance:
        print(f"\n🔒 Compliance:")
        print(f"  PII fields: {', '.join(compliance.get('pii_fields', []))}")
        print(f"  GDPR applicable: {compliance.get('gdpr_applicable', False)}")
        print(f"  Data classification: {compliance.get('data_classification', 'N/A')}")
        print(f"  Retention: {compliance.get('retention_days', 0)} days")

    # Cost
    cost = job.get('cost', {})
    if cost:
        print(f"\n💰 Cost:")
        print(f"  Budget per run: ${cost.get('budget_per_run_usd', 0):.2f}")
        print(f"  Optimization priority: {cost.get('optimization_priority', 'balanced')}")


def example_3_run_single_job():
    """
    Example 3: Run a single job by job_id
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 3: Running Single Job")
    print("=" * 80)

    # Initialize orchestrator
    print("\n🚀 Initializing ConfigDrivenOrchestrator...")
    orchestrator = ConfigDrivenOrchestrator(
        config_path='./etl_config.json',
        enable_auto_detection=True
    )

    print("✓ Orchestrator initialized")

    # Run job
    job_id = 'customer_order_summary'
    print(f"\n▶ Running job: {job_id}")

    result = orchestrator.run_job_by_id(job_id)

    # Display results
    print("\n" + "=" * 80)
    print("EXECUTION RESULTS")
    print("=" * 80)

    print(f"\nJob: {result.get('job_name', 'Unknown')}")
    print(f"Status: {result.get('status', 'unknown')}")
    print(f"Timestamp: {result.get('timestamp', 'N/A')}")

    # Pre-execution analysis
    pre_analysis = result.get('pre_execution_analysis', {})
    print(f"\n📊 Pre-Execution Analysis:")
    print(f"  Total data volume: {pre_analysis.get('total_data_volume_gb', 0)} GB")
    print(f"  Auto-detected: {pre_analysis.get('auto_detected', False)}")
    print(f"  Complexity: {pre_analysis.get('complexity', 'unknown')}")
    print(f"  Query pattern: {pre_analysis.get('query_pattern', 'unknown')}")

    # Platform decision
    platform_decision = result.get('platform_decision', {})
    print(f"\n🖥️ Platform Decision:")
    print(f"  User preference: {platform_decision.get('user_preference', 'None')}")
    print(f"  Agent recommendation: {platform_decision.get('agent_recommendation', {}).get('recommended_platform', 'unknown')}")
    print(f"  Final platform: {platform_decision.get('final_platform', 'unknown')}")
    print(f"  Reason: {platform_decision.get('decision_reason', 'N/A')}")

    # Quality setup
    quality_setup = result.get('quality_setup', {})
    if quality_setup.get('enabled', False):
        print(f"\n✅ Quality Checks:")
        print(f"  Completeness checks: {len(quality_setup.get('completeness_checks', []))}")
        print(f"  Accuracy rules: {len(quality_setup.get('accuracy_rules', []))}")
        print(f"  Fail on error: {quality_setup.get('fail_on_error', False)}")

    # Execution result
    execution_result = result.get('execution_result', {})
    if execution_result.get('status') == 'completed':
        print(f"\n🎉 Execution:")
        print(f"  Job Run ID: {execution_result.get('job_run_id', 'N/A')}")
        print(f"  Duration: {execution_result.get('duration_minutes', 0)} minutes")
        print(f"  Execution state: {execution_result.get('execution_state', 'unknown')}")

        # Recommendations
        recommendations = execution_result.get('recommendations_for_next_run', [])
        if recommendations:
            print(f"\n💡 Recommendations for Next Run:")
            for i, rec in enumerate(recommendations[:3], 1):  # Show top 3
                print(f"\n  {i}. {rec.get('category', 'general').upper()}")
                print(f"     Priority: {rec.get('priority', 'medium')}")
                print(f"     {rec.get('recommendation', 'N/A')}")
    else:
        print(f"\n✗ Execution failed:")
        print(f"  Error: {execution_result.get('error', 'Unknown error')}")

    # Post-execution analysis
    post_analysis = result.get('post_execution_analysis', {})
    if post_analysis.get('recommendations_generated', False):
        print(f"\n📈 Post-Execution Analysis:")
        print(f"  Comprehensive analysis completed by agent swarm")
        print(f"  Timestamp: {post_analysis.get('timestamp', 'N/A')}")


def example_4_run_all_enabled_jobs():
    """
    Example 4: Run all enabled jobs
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 4: Running All Enabled Jobs")
    print("=" * 80)

    orchestrator = ConfigDrivenOrchestrator(
        config_path='./etl_config.json',
        enable_auto_detection=True
    )

    print("\n▶ Running all enabled jobs...")

    results = orchestrator.run_all_enabled_jobs()

    print("\n" + "=" * 80)
    print("ALL JOBS SUMMARY")
    print("=" * 80)

    for result in results:
        status_icon = "✓" if result.get('status') == 'completed' else "✗"
        print(f"\n{status_icon} {result.get('job_id', 'unknown')}")
        print(f"  Status: {result.get('status', 'unknown')}")

        if result.get('status') == 'completed':
            exec_result = result.get('execution_result', {})
            print(f"  Duration: {exec_result.get('duration_minutes', 0)} minutes")
            print(f"  Platform: {result.get('platform_decision', {}).get('final_platform', 'unknown')}")
        else:
            print(f"  Error: {result.get('error', 'Unknown')}")


def example_5_table_name_with_auto_detection():
    """
    Example 5: Demonstrate table name usage with auto-detection
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 5: Table Names with Auto-Detection")
    print("=" * 80)

    print("\n📚 Config supports multiple source types:")
    print("\n1. Glue Catalog Tables:")
    print("""
  {
    "name": "transactions",
    "source_type": "glue_catalog",
    "database": "analytics_db",
    "table": "fact_transactions",
    "auto_detect_size": true,
    "auto_detect_partitions": true,
    "role_in_flow": "fact"
  }
    """)

    print("\n2. S3 Paths:")
    print("""
  {
    "name": "customer_segments",
    "source_type": "s3",
    "path": "s3://my-bucket/dimensions/customer_segments/",
    "auto_detect_size": true,
    "role_in_flow": "dimension"
  }
    """)

    print("\n3. Redshift Tables:")
    print("""
  {
    "name": "products",
    "source_type": "redshift",
    "cluster": "analytics-cluster",
    "database": "master_db",
    "schema": "public",
    "table": "products",
    "auto_detect_size": true,
    "role_in_flow": "dimension"
  }
    """)

    print("\n4. RDS Tables:")
    print("""
  {
    "name": "stores",
    "source_type": "rds",
    "instance": "operational-db",
    "database": "retail",
    "table": "stores",
    "auto_detect_size": true,
    "role_in_flow": "dimension"
  }
    """)

    print("\n5. DynamoDB Tables:")
    print("""
  {
    "name": "user_profiles",
    "source_type": "dynamodb",
    "table_name": "user-profiles-prod",
    "role_in_flow": "lookup",
    "cache_recommended": true
  }
    """)

    print("\n✨ Key Features:")
    print("  - Auto-detect size: Agent queries AWS services to get actual table size")
    print("  - Auto-detect partitions: Agent analyzes partition structure")
    print("  - Role in flow: Helps agent determine if it's fact or dimension")
    print("  - Joins with: Agent analyzes data flow relationships")
    print("  - Broadcast eligible: Agent decides based on size")


def example_6_data_flow_analysis():
    """
    Example 6: Data flow analysis demonstration
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 6: Data Flow Analysis")
    print("=" * 80)

    print("\n🔄 Data flow analysis features:")
    print("\n1. Auto-detect joins between tables")
    print("2. Identify fact vs dimension tables")
    print("3. Recommend broadcast strategy")
    print("4. Optimize join order")
    print("5. Calculate expected shuffle size")

    print("\n📋 Example configuration:")
    print("""
  "data_sources": [
    {
      "name": "transactions",
      "role_in_flow": "fact",
      "joins_with": ["customers", "products"],
      "join_keys": {
        "customers": "customer_id",
        "products": "product_id"
      }
    },
    {
      "name": "customers",
      "role_in_flow": "dimension",
      "broadcast_eligible": "auto"
    }
  ],

  "data_flow": {
    "analysis_enabled": true,
    "auto_detect_joins": true,
    "auto_optimize_join_order": true
  }
    """)

    print("\n🤖 Agent analysis provides:")
    print("  - Optimal join order (smallest dimensions first)")
    print("  - Broadcast recommendations for small dimensions")
    print("  - Expected performance impact (30-50% faster with broadcasts)")
    print("  - Shuffle size estimates")


def main():
    """
    Run all examples
    """
    print("=" * 80)
    print("CONFIG-DRIVEN ETL FRAMEWORK - COMPREHENSIVE EXAMPLES")
    print("=" * 80)

    try:
        # Example 1: Config validation
        example_1_config_validation()

        # Example 2: Inspect job config
        example_2_inspect_job_config()

        # Example 3: Run single job
        # Uncomment to actually run (requires AWS credentials)
        # example_3_run_single_job()

        # Example 4: Run all jobs
        # Uncomment to actually run (requires AWS credentials)
        # example_4_run_all_enabled_jobs()

        # Example 5: Table names with auto-detection
        example_5_table_name_with_auto_detection()

        # Example 6: Data flow analysis
        example_6_data_flow_analysis()

        print("\n" + "=" * 80)
        print("✓ ALL EXAMPLES COMPLETED")
        print("=" * 80)

    except Exception as e:
        print(f"\n✗ Error running examples: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
