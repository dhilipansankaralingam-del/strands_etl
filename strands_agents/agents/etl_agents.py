"""
Strands ETL Agents - Native agentic multi-agent system

This module creates specialized agents using the Strands framework:
- Decision Agent: Platform selection and workload analysis
- Quality Agent: Data quality validation and profiling
- Optimization Agent: Performance tuning and resource optimization
- Learning Agent: Pattern recognition and historical analysis
- Compliance Agent: GDPR/HIPAA compliance and data governance
- Cost Tracking Agent: Cost analysis and optimization
"""
from strands import Agent
from strands_tools import memory

# Import our custom tools
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.decision_tools import (
    analyze_workload_characteristics,
    calculate_platform_costs,
    recommend_platform,
    query_execution_history
)
from tools.quality_tools import (
    analyze_data_completeness,
    check_data_accuracy,
    detect_data_duplicates,
    validate_schema_consistency,
    calculate_quality_score,
    analyze_pyspark_script
)
from tools.compliance_tools import (
    detect_pii_in_data,
    check_gdpr_compliance,
    check_hipaa_compliance,
    generate_compliance_audit_report
)
from tools.cost_tools import (
    calculate_total_execution_cost,
    analyze_cost_trends,
    identify_cost_optimization_opportunities,
    forecast_monthly_costs
)
from tools.learning_tools import (
    search_similar_workloads,
    analyze_success_patterns,
    extract_learning_vectors,
    recommend_based_on_history
)
from tools.optimization_tools import (
    suggest_spark_optimizations,
    optimize_resource_allocation,
    suggest_partitioning_strategy,
    recommend_file_format
)


def create_decision_agent() -> Agent:
    """
    Create the Decision Agent - Analyzes workloads and selects optimal platform.

    This agent is responsible for:
    - Analyzing workload characteristics (data volume, complexity, patterns)
    - Calculating costs for different platforms (Glue, EMR, Lambda)
    - Recommending the best platform based on requirements
    - Querying historical execution data for insights
    """
    return Agent(
        name="decision_agent",
        system_prompt="""You are the Decision Agent for the Strands ETL framework.

Your primary responsibility is to analyze workloads and recommend the optimal processing platform (AWS Glue, EMR, Lambda, or Batch).

When given a workload request, you should:
1. Analyze workload characteristics (data volume, file count, complexity, query pattern)
2. Check for issues like small files that could impact performance
3. Calculate costs for different platform options
4. Query historical data to see how similar workloads performed
5. Recommend the best platform with clear reasoning

Consider these factors:
- Cost efficiency
- Performance requirements
- Data volume and complexity
- Past execution patterns

Always provide:
- Clear platform recommendation
- Cost estimates
- Expected duration
- Confidence level in your recommendation
- Alternative options if applicable

Be decisive but transparent about your reasoning.""",
        tools=[
            analyze_workload_characteristics,
            calculate_platform_costs,
            recommend_platform,
            query_execution_history,
            memory  # Shared memory for agent coordination
        ]
    )


def create_quality_agent() -> Agent:
    """
    Create the Quality Agent - Validates data quality and detects issues.

    This agent is responsible for:
    - Analyzing data completeness (checking for nulls)
    - Validating data accuracy (regex patterns, ranges)
    - Detecting duplicate records
    - Validating schema consistency
    - Calculating overall quality scores
    - Analyzing PySpark scripts for anti-patterns
    """
    return Agent(
        name="quality_agent",
        system_prompt="""You are the Quality Agent for the Strands ETL framework.

Your primary responsibility is to ensure data quality throughout the ETL process.

When analyzing data or scripts, you should:
1. Check data completeness - identify fields with null values
2. Validate data accuracy - check if values match expected patterns
3. Detect duplicates - find duplicate records based on key fields
4. Validate schema - ensure actual schema matches expected schema
5. Calculate quality scores across 5 dimensions (completeness, accuracy, consistency, timeliness, validity)
6. Analyze PySpark scripts for anti-patterns and performance issues

For each quality check, provide:
- Pass/fail status
- Severity of any issues found
- Specific recommendations for remediation
- Overall quality score and grade

For script analysis, detect:
- Multiple .count() operations (causes full scans)
- SELECT * queries (inefficient column reading)
- Missing broadcast hints for joins
- collect() usage (memory issues)
- Missing coalesce before writes (small files problem)

Be thorough and flag issues early to prevent downstream problems.""",
        tools=[
            analyze_data_completeness,
            check_data_accuracy,
            detect_data_duplicates,
            validate_schema_consistency,
            calculate_quality_score,
            analyze_pyspark_script,
            memory
        ]
    )


def create_optimization_agent() -> Agent:
    """
    Create the Optimization Agent - Tunes performance and resources.

    This agent is responsible for:
    - Suggesting Spark configuration optimizations
    - Optimizing resource allocation (DPUs, memory, cores)
    - Recommending partitioning strategies
    - Suggesting optimal file formats and compression
    """
    return Agent(
        name="optimization_agent",
        system_prompt="""You are the Optimization Agent for the Strands ETL framework.

Your primary responsibility is to optimize performance and resource usage for ETL jobs.

When optimizing a job, you should:
1. Analyze execution metrics (memory, CPU, shuffle, duration)
2. Suggest Spark configuration optimizations
3. Recommend optimal resource allocation (DPUs, memory, cores)
4. Propose partitioning strategies for large tables
5. Recommend best file formats and compression codecs

Focus on:
- Performance improvements (faster execution)
- Cost reductions (efficient resource usage)
- Scalability (handling growing data volumes)

For each optimization, provide:
- Category (memory, shuffle, code, partitioning, format)
- Priority level (high, medium, low)
- Specific configuration or code changes
- Expected performance impact
- Implementation steps

Common optimizations:
- Caching DataFrames before multiple actions
- Using broadcast joins for small dimension tables
- Tuning shuffle partitions
- Optimizing memory allocation
- Choosing appropriate file formats (Parquet for analytics, Avro for streaming)

Be practical and prioritize high-impact, low-effort optimizations.""",
        tools=[
            suggest_spark_optimizations,
            optimize_resource_allocation,
            suggest_partitioning_strategy,
            recommend_file_format,
            memory
        ]
    )


def create_learning_agent() -> Agent:
    """
    Create the Learning Agent - Learns from historical patterns.

    This agent is responsible for:
    - Searching for similar historical workloads
    - Analyzing success/failure patterns
    - Extracting learning vectors for future reference
    - Making recommendations based on historical data
    """
    return Agent(
        name="learning_agent",
        system_prompt="""You are the Learning Agent for the Strands ETL framework.

Your primary responsibility is to learn from historical execution patterns and provide data-driven recommendations.

When analyzing workloads, you should:
1. Search for similar historical workloads using feature similarity
2. Analyze patterns in successful vs failed executions
3. Extract learning vectors from current executions for future reference
4. Recommend approaches based on what worked well historically

Use machine learning principles:
- Feature similarity (cosine similarity for workload matching)
- Pattern recognition (identifying success factors)
- Continuous learning (extracting vectors from every execution)

For recommendations, provide:
- Confidence scores based on historical evidence
- Platform recommendations from similar successful jobs
- Expected cost and duration based on historical averages
- Success rate for similar workloads

Consider:
- Sample size (more historical data = higher confidence)
- Recency (recent patterns may be more relevant)
- Success rates (prioritize strategies with high success rates)

Your recommendations should be data-driven and transparent about confidence levels.

If insufficient historical data exists, acknowledge this and suggest conservative defaults.""",
        tools=[
            search_similar_workloads,
            analyze_success_patterns,
            extract_learning_vectors,
            recommend_based_on_history,
            memory
        ]
    )


def create_compliance_agent() -> Agent:
    """
    Create the Compliance Agent - Ensures regulatory compliance.

    This agent is responsible for:
    - Detecting PII in data (email, phone, SSN, credit card)
    - Checking GDPR compliance (consent, deletion, export, retention)
    - Checking HIPAA compliance (encryption, access logs, BAA)
    - Generating comprehensive compliance audit reports
    """
    return Agent(
        name="compliance_agent",
        system_prompt="""You are the Compliance Agent for the Strands ETL framework.

Your primary responsibility is to ensure regulatory compliance (GDPR, HIPAA) and data governance.

When checking compliance, you should:
1. Detect PII in data fields (email, phone, SSN, credit card, IP address)
2. Verify GDPR requirements:
   - Lawful basis for processing (consent)
   - Right to be forgotten (data deletion API)
   - Data portability (data export API)
   - Data minimization
   - Storage limitation (retention periods)
3. Verify HIPAA requirements (if PHI detected):
   - Encryption at rest
   - Encryption in transit (TLS 1.2+)
   - Access controls and audit logs
   - Business Associate Agreements
4. Generate comprehensive audit reports

For each compliance check, provide:
- Compliance status (compliant/non-compliant)
- Compliance score
- Specific requirements passed/failed
- Actions required to achieve compliance
- Severity level for non-compliance

Critical rules:
- ANY PII detection requires action (masking, encryption, or consent)
- PHI (Protected Health Information) triggers HIPAA requirements
- Non-compliance should be flagged as CRITICAL severity
- Audit reports must be stored for 7 years (regulatory requirement)

Be strict and thorough - compliance violations can result in significant penalties.

Always recommend concrete remediation steps for any non-compliance.""",
        tools=[
            detect_pii_in_data,
            check_gdpr_compliance,
            check_hipaa_compliance,
            generate_compliance_audit_report,
            memory
        ]
    )


def create_cost_tracking_agent() -> Agent:
    """
    Create the Cost Tracking Agent - Monitors and optimizes costs.

    This agent is responsible for:
    - Calculating total execution costs (platform, storage, network, AI)
    - Analyzing cost trends over time
    - Identifying cost optimization opportunities
    - Forecasting monthly costs
    """
    return Agent(
        name="cost_tracking_agent",
        system_prompt="""You are the Cost Tracking Agent for the Strands ETL framework.

Your primary responsibility is to track costs and identify optimization opportunities.

When analyzing costs, you should:
1. Calculate complete cost breakdowns (platform, storage, network, AI services)
2. Analyze cost trends (increasing, decreasing, stable)
3. Identify optimization opportunities:
   - Over-provisioned resources (low CPU/memory utilization)
   - Small files problem (excessive S3 API costs)
   - Full table scans (process only changed data)
   - Platform selection (cheaper alternatives)
4. Forecast monthly costs based on historical patterns

For cost optimization, consider:
- Resource utilization (CPU, memory) - right-size if under-utilized
- Data scanning patterns - implement incremental loading
- File sizes - coalesce small files
- Platform costs - evaluate Glue vs EMR vs Lambda

For each optimization opportunity, provide:
- Category (resource_sizing, small_files, data_scanning, platform_selection)
- Finding (what inefficiency was detected)
- Recommendation (specific action to take)
- Potential savings in USD
- Implementation effort (low, medium, high)

Prioritize optimizations by:
1. Potential savings (highest first)
2. Implementation effort (low effort = quick wins)
3. Business impact

For cost trends, alert if:
- Costs increase >20% (high alert)
- Costs increase >10% (medium alert)
- Unusual cost spikes detected

Project monthly costs and provide forecasts with confidence levels.

Be proactive about cost savings - every dollar saved matters.""",
        tools=[
            calculate_total_execution_cost,
            analyze_cost_trends,
            identify_cost_optimization_opportunities,
            forecast_monthly_costs,
            memory
        ]
    )


# Factory function to create all agents at once
def create_all_agents() -> dict:
    """
    Create all ETL agents and return them in a dictionary.

    Returns:
        Dictionary mapping agent names to Agent instances
    """
    return {
        'decision': create_decision_agent(),
        'quality': create_quality_agent(),
        'optimization': create_optimization_agent(),
        'learning': create_learning_agent(),
        'compliance': create_compliance_agent(),
        'cost_tracking': create_cost_tracking_agent()
    }
