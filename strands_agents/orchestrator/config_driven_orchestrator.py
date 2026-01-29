"""
Config-Driven ETL Orchestrator for Strands Framework

Orchestrates ETL jobs based on etl_config.json with:
- Auto-detection of table sizes
- Data flow analysis
- Agent-based decision making
- User preference with agent recommendations
- Quality validation from config
- Compliance checking
- Cost tracking
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from strands_agents.orchestrator.config_loader import ConfigLoader
from strands_agents.orchestrator.concurrent_etl_orchestrator import ConcurrentETLOrchestrator
from strands_agents.orchestrator.swarm_orchestrator import ETLSwarm
from strands_agents.tools.catalog_tools import recommend_platform_based_on_data_flow
from strands_agents.agents.etl_agents import create_decision_agent, create_quality_agent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConfigDrivenOrchestrator:
    """
    Orchestrates ETL jobs from etl_config.json with full agent intelligence.
    """

    def __init__(
        self,
        config_path: str = './etl_config.json',
        enable_auto_detection: bool = True
    ):
        """
        Initialize the config-driven orchestrator.

        Args:
            config_path: Path to etl_config.json
            enable_auto_detection: Enable auto-detection of sizes and data flow
        """
        self.config_path = config_path
        self.enable_auto_detection = enable_auto_detection

        # Load configuration
        logger.info(f"Loading configuration from {config_path}")
        self.config_loader = ConfigLoader(config_path)
        self.config = self.config_loader.load()

        # Validate configuration
        validation = self.config_loader.validate_config()
        if not validation['valid']:
            logger.error(f"Configuration validation failed: {validation['errors']}")
            raise ValueError("Invalid configuration")

        if validation['warnings']:
            for warning in validation['warnings']:
                logger.warning(warning)

        logger.info(f"✓ Configuration loaded: {validation['total_jobs']} jobs, {validation['enabled_jobs']} enabled")

        # Initialize orchestrators
        global_settings = self.config_loader.get_global_settings()
        aws_region = global_settings.get('aws_region', 'us-east-1')

        self.concurrent_orchestrator = ConcurrentETLOrchestrator(aws_region=aws_region)
        self.swarm = ETLSwarm()

        # Initialize agents
        self.decision_agent = create_decision_agent()
        self.quality_agent = create_quality_agent()

        logger.info("✓ Orchestrator initialized successfully")

    def run_job_by_id(
        self,
        job_id: str,
        override_params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Run a specific job by job_id.

        Args:
            job_id: Job identifier from config
            override_params: Optional parameters to override config

        Returns:
            Dict with execution results
        """
        logger.info(f"=" * 80)
        logger.info(f"RUNNING JOB: {job_id}")
        logger.info(f"=" * 80)

        # Get job config
        job_config = self.config_loader.get_job_by_id(job_id)
        if not job_config:
            return {
                'status': 'error',
                'error': f'Job {job_id} not found in configuration'
            }

        # Check if job is enabled
        if not job_config.get('enabled', False):
            logger.warning(f"Job {job_id} is disabled in configuration")
            return {
                'status': 'skipped',
                'reason': 'Job is disabled'
            }

        # Apply overrides
        if override_params:
            job_config = {**job_config, **override_params}

        # Phase 1: Pre-Execution Agent Analysis
        logger.info("\n📊 Phase 1: Pre-Execution Agent Analysis")
        pre_execution_analysis = self._pre_execution_analysis(job_config)

        # Phase 2: Platform Decision
        logger.info("\n🤖 Phase 2: Platform Decision")
        platform_decision = self._platform_decision(job_config, pre_execution_analysis)

        # Phase 3: Quality Checks
        logger.info("\n✅ Phase 3: Quality Validation Setup")
        quality_setup = self._setup_quality_checks(job_config)

        # Phase 4: Execute Job
        logger.info("\n🚀 Phase 4: Job Execution")
        execution_result = self._execute_job(job_config, platform_decision, quality_setup)

        # Phase 5: Post-Execution Analysis
        logger.info("\n📈 Phase 5: Post-Execution Analysis")
        post_execution_analysis = self._post_execution_analysis(
            job_config,
            execution_result,
            pre_execution_analysis
        )

        # Compile final results
        final_result = {
            'job_id': job_id,
            'job_name': job_config.get('job_name', ''),
            'status': execution_result.get('status', 'unknown'),
            'timestamp': datetime.utcnow().isoformat(),
            'pre_execution_analysis': pre_execution_analysis,
            'platform_decision': platform_decision,
            'quality_setup': quality_setup,
            'execution_result': execution_result,
            'post_execution_analysis': post_execution_analysis
        }

        logger.info(f"\n✓ Job {job_id} completed with status: {final_result['status']}")

        return final_result

    def _pre_execution_analysis(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run pre-execution analysis using agents.

        - Auto-detect table sizes (if enabled)
        - Analyze data flow
        - Get initial recommendations

        Returns:
            Pre-execution analysis results
        """
        analysis = {
            'timestamp': datetime.utcnow().isoformat(),
            'auto_detection_enabled': self.enable_auto_detection
        }

        # Get data sources info
        data_sources = job_config.get('data_sources', [])
        workload = job_config.get('workload', {})

        # Check if auto-detection was already done by ConfigLoader
        if workload.get('auto_detected', False):
            logger.info("✓ Table sizes already auto-detected by ConfigLoader")
            analysis['total_data_volume_gb'] = workload.get('data_volume_gb', 0)
            analysis['auto_detected'] = True
        else:
            # Calculate from estimates
            total_volume = sum(
                ds.get('estimated_size_gb', ds.get('size_gb', 0))
                for ds in data_sources
            )
            analysis['total_data_volume_gb'] = total_volume
            analysis['auto_detected'] = False

        # Check if data flow was already analyzed
        data_flow = job_config.get('data_flow', {})
        if data_flow.get('analyzed', False):
            logger.info("✓ Data flow already analyzed by ConfigLoader")
            analysis['data_flow_analysis'] = data_flow.get('analysis_results', {})
        else:
            analysis['data_flow_analysis'] = {
                'analyzed': False,
                'note': 'Data flow analysis not enabled in config'
            }

        # Get complexity from workload
        analysis['complexity'] = workload.get('complexity', 'moderate')
        analysis['query_pattern'] = workload.get('query_pattern', 'batch')

        logger.info(f"  Total data volume: {analysis['total_data_volume_gb']} GB")
        logger.info(f"  Complexity: {analysis['complexity']}")
        logger.info(f"  Query pattern: {analysis['query_pattern']}")

        return analysis

    def _platform_decision(
        self,
        job_config: Dict[str, Any],
        pre_execution_analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Make platform decision with agent recommendation.

        Respects user preference but still provides agent recommendation.

        Returns:
            Platform decision with reasoning
        """
        platform_config = job_config.get('platform', {})
        user_preference = platform_config.get('user_preference', None)
        allow_agent_override = platform_config.get('allow_agent_override', True)

        logger.info(f"  User preference: {user_preference or 'None (auto-select)'}")
        logger.info(f"  Allow agent override: {allow_agent_override}")

        # Get agent recommendation
        data_flow_analysis = pre_execution_analysis.get('data_flow_analysis', {})
        total_volume = pre_execution_analysis.get('total_data_volume_gb', 0)
        complexity = pre_execution_analysis.get('complexity', 'moderate')

        if data_flow_analysis.get('success', False):
            agent_recommendation = recommend_platform_based_on_data_flow(
                data_flow_analysis,
                total_volume,
                complexity
            )
        else:
            # Fallback: Ask Decision Agent directly
            logger.info("  Asking Decision Agent for platform recommendation...")

            prompt = f"""
Analyze this ETL workload and recommend the optimal platform:

Workload Details:
- Total data volume: {total_volume} GB
- Complexity: {complexity}
- Query pattern: {pre_execution_analysis.get('query_pattern', 'batch')}
- Number of data sources: {len(job_config.get('data_sources', []))}

User preference: {user_preference or 'None'}

Provide platform recommendation (glue, emr, lambda, or batch) with reasoning.
            """

            agent_response = self.decision_agent(prompt)

            agent_recommendation = {
                'recommended_platform': 'glue',  # Default fallback
                'reason': str(agent_response),
                'confidence': 'medium'
            }

        logger.info(f"  Agent recommendation: {agent_recommendation['recommended_platform']}")
        logger.info(f"  Reason: {agent_recommendation['reason']}")

        # Make final decision
        if user_preference:
            if allow_agent_override:
                # Use user preference but note agent recommendation
                final_platform = user_preference
                decision_reason = f"Using user preference ({user_preference}), but agent recommends {agent_recommendation['recommended_platform']}: {agent_recommendation['reason']}"
            else:
                # Strict user preference
                final_platform = user_preference
                decision_reason = f"Using user preference ({user_preference}) - agent override disabled"
        else:
            # No user preference, use agent recommendation
            final_platform = agent_recommendation['recommended_platform']
            decision_reason = f"Agent-selected platform: {agent_recommendation['reason']}"

        logger.info(f"  ✓ Final decision: {final_platform}")

        return {
            'user_preference': user_preference,
            'agent_recommendation': agent_recommendation,
            'final_platform': final_platform,
            'decision_reason': decision_reason,
            'agent_override_allowed': allow_agent_override
        }

    def _setup_quality_checks(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Setup quality checks from config for Quality Agent.

        Returns:
            Quality setup configuration
        """
        data_quality_config = job_config.get('data_quality', {})

        if not data_quality_config.get('enabled', False):
            logger.info("  Data quality checks disabled")
            return {'enabled': False}

        logger.info("  Setting up data quality checks from config...")

        # Extract quality rules
        completeness_checks = data_quality_config.get('completeness_checks', [])
        accuracy_rules = data_quality_config.get('accuracy_rules', [])
        duplicate_check = data_quality_config.get('duplicate_check', {})
        referential_integrity = data_quality_config.get('referential_integrity', [])

        logger.info(f"    - Completeness checks: {len(completeness_checks)}")
        logger.info(f"    - Accuracy rules: {len(accuracy_rules)}")
        logger.info(f"    - Duplicate check: {duplicate_check.get('enabled', False)}")
        logger.info(f"    - Referential integrity: {len(referential_integrity)}")

        # Ask Quality Agent to prepare validation strategy
        prompt = f"""
Prepare a data quality validation strategy based on this configuration:

Completeness Checks: {len(completeness_checks)} rules
Accuracy Rules: {len(accuracy_rules)} rules
Duplicate Check: {duplicate_check.get('enabled', False)}
Referential Integrity: {len(referential_integrity)} constraints

Fail on error: {data_quality_config.get('fail_on_error', True)}

Provide a validation execution plan.
        """

        agent_response = self.quality_agent(prompt)

        return {
            'enabled': True,
            'fail_on_error': data_quality_config.get('fail_on_error', True),
            'completeness_checks': completeness_checks,
            'accuracy_rules': accuracy_rules,
            'duplicate_check': duplicate_check,
            'referential_integrity': referential_integrity,
            'agent_validation_plan': str(agent_response)
        }

    def _execute_job(
        self,
        job_config: Dict[str, Any],
        platform_decision: Dict[str, Any],
        quality_setup: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute the ETL job using ConcurrentETLOrchestrator.

        Returns:
            Execution results
        """
        execution_config = job_config.get('execution', {})
        script_path = execution_config.get('script_path', '')

        # Build job config for ConcurrentETLOrchestrator
        platform_config = job_config.get('platform', {})

        concurrent_job_config = {
            'glue_job_name': platform_config.get('glue_job_name', job_config.get('job_id', 'unnamed_job')),
            'data_volume_gb': job_config.get('workload', {}).get('data_volume_gb', 0),
            'file_count': job_config.get('workload', {}).get('estimated_file_count', 0),
            'transformation_complexity': job_config.get('workload', {}).get('complexity', 'moderate'),
            'dpu_count': platform_config.get('dpu_count', 10),
            'platform': platform_decision.get('final_platform', 'glue'),
            'quality_config': quality_setup
        }

        logger.info(f"  Executing job: {concurrent_job_config['glue_job_name']}")
        logger.info(f"  Platform: {concurrent_job_config['platform']}")
        logger.info(f"  DPU count: {concurrent_job_config['dpu_count']}")

        # Execute using ConcurrentETLOrchestrator
        try:
            result = self.concurrent_orchestrator.run_etl_job(
                script_path=script_path,
                job_config=concurrent_job_config,
                apply_previous_recommendations=True
            )

            logger.info(f"  ✓ Execution completed: {result.get('execution_state', 'unknown')}")

            return result

        except Exception as e:
            logger.error(f"  ✗ Execution failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }

    def _post_execution_analysis(
        self,
        job_config: Dict[str, Any],
        execution_result: Dict[str, Any],
        pre_execution_analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Run post-execution analysis using agent swarm.

        Returns:
            Post-execution analysis with recommendations
        """
        logger.info("  Running comprehensive post-execution analysis with agent swarm...")

        analysis_prompt = f"""
Analyze the completed ETL job and provide comprehensive recommendations:

Job: {job_config.get('job_name', 'Unknown')}
Status: {execution_result.get('execution_state', 'unknown')}
Duration: {execution_result.get('duration_minutes', 0)} minutes

Pre-Execution Analysis:
- Data volume: {pre_execution_analysis.get('total_data_volume_gb', 0)} GB
- Complexity: {pre_execution_analysis.get('complexity', 'unknown')}

Execution Metrics:
{execution_result.get('execution_metrics', {})}

Coordinate all agents (Decision, Quality, Optimization, Cost, Compliance, Learning)
to provide:
1. Performance analysis
2. Cost optimization opportunities
3. Quality assessment
4. Compliance verification
5. Recommendations for next run
        """

        swarm_response = self.swarm.swarm(analysis_prompt)

        logger.info("  ✓ Post-execution analysis complete")

        return {
            'timestamp': datetime.utcnow().isoformat(),
            'full_swarm_analysis': str(swarm_response),
            'recommendations_generated': True
        }

    def run_all_enabled_jobs(self) -> List[Dict[str, Any]]:
        """
        Run all enabled jobs from the configuration.

        Returns:
            List of execution results
        """
        enabled_jobs = self.config_loader.list_enabled_jobs()

        logger.info(f"Running {len(enabled_jobs)} enabled jobs")

        results = []
        for job in enabled_jobs:
            job_id = job['job_id']
            try:
                result = self.run_job_by_id(job_id)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to run job {job_id}: {e}")
                results.append({
                    'job_id': job_id,
                    'status': 'error',
                    'error': str(e)
                })

        return results


# Example usage
if __name__ == '__main__':
    print("=" * 80)
    print("CONFIG-DRIVEN ETL ORCHESTRATOR - EXAMPLE")
    print("=" * 80)

    # Initialize orchestrator
    orchestrator = ConfigDrivenOrchestrator(
        config_path='./etl_config.json',
        enable_auto_detection=True
    )

    # Run specific job
    print("\n🚀 Running job: customer_order_summary")
    result = orchestrator.run_job_by_id('customer_order_summary')

    print("\n" + "=" * 80)
    print("EXECUTION SUMMARY")
    print("=" * 80)
    print(f"Job: {result.get('job_name', 'Unknown')}")
    print(f"Status: {result.get('status', 'unknown')}")
    print(f"Platform: {result.get('platform_decision', {}).get('final_platform', 'unknown')}")

    if result.get('status') == 'completed':
        print(f"Duration: {result.get('execution_result', {}).get('duration_minutes', 0)} minutes")
        print("\n✓ Job completed successfully")
    else:
        print(f"\n✗ Job failed: {result.get('error', 'Unknown error')}")
