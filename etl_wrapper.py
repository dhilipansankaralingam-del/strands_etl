"""
Strands ETL Wrapper - Integrates Config Loading with Orchestrator
Provides end-to-end ETL pipeline execution
"""

import json
import logging
from typing import Dict, Any, List, Optional
from config_loader import ConfigLoader
from orchestrator.strands_orchestrator import StrandsOrchestrator, DateTimeEncoder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETLWrapper:
    """
    Wrapper class that integrates config loading, validation, and orchestration.
    Provides a unified interface for running ETL jobs.
    """

    def __init__(self, config_path: str):
        """
        Initialize ETL wrapper with config file path.

        Args:
            config_path: Path to the configuration file (test_config.json format with jobs array)
        """
        self.config_path = config_path
        self.config_loader = ConfigLoader(config_path)
        self.orchestrator = StrandsOrchestrator()
        self._config = None
        self._validation = None

    def load_and_validate(self) -> Dict[str, Any]:
        """Load and validate the configuration."""
        logger.info(f"Loading configuration from {self.config_path}")
        self._config = self.config_loader.load()

        logger.info("Validating configuration...")
        self._validation = self.config_loader.validate_config()

        if self._validation['valid']:
            logger.info(f"✓ Configuration valid. Jobs: {len(self._config.get('jobs', []))}, Enabled: {self._validation['enabled_jobs']}")
        else:
            logger.error(f"✗ Configuration invalid: {self._validation['errors']}")

        return self._validation

    def get_enabled_jobs(self) -> List[Dict[str, Any]]:
        """Get list of enabled jobs from config."""
        if self._config is None:
            self.load_and_validate()

        jobs = self._config.get('jobs', [])
        return [job for job in jobs if job.get('enabled', True)]

    def convert_job_to_orchestrator_format(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a job config (from test_config.json format) to orchestrator format.

        Args:
            job: Job configuration with nested structure

        Returns:
            Configuration compatible with StrandsOrchestrator
        """
        orchestrator_config = {
            'workload': {
                'name': job.get('name', 'unnamed_job'),
                'description': job.get('description', ''),
                'data_volume': job.get('workload', {}).get('data_volume', 'medium'),
                'complexity': job.get('workload', {}).get('complexity', 'medium'),
                'criticality': job.get('workload', {}).get('criticality', 'medium'),
                'time_sensitivity': job.get('workload', {}).get('time_sensitivity', 'medium'),
                'data_sources': job.get('data_sources', []),
                'target': job.get('target', {})
            },
            'platform': job.get('platform', {
                'preferred': 'glue',
                'fallback': ['emr', 'lambda'],
                'resource_allocation': {
                    'workers': 10,
                    'worker_type': 'G.1X',
                    'timeout': 3600
                }
            }),
            'scripts': job.get('scripts', {}),
            'resource_allocation': job.get('platform', {}).get('resource_allocation', {})
        }

        return orchestrator_config

    def run_job(self, job_name: str, user_request: Optional[str] = None) -> Dict[str, Any]:
        """
        Run a specific job by name.

        Args:
            job_name: Name of the job to run
            user_request: Optional custom request for the orchestrator

        Returns:
            Execution result from the orchestrator
        """
        if self._config is None:
            self.load_and_validate()

        # Find the job
        job = None
        for j in self._config.get('jobs', []):
            if j.get('name') == job_name:
                job = j
                break

        if job is None:
            return {'status': 'failed', 'error': f"Job '{job_name}' not found"}

        if not job.get('enabled', True):
            return {'status': 'failed', 'error': f"Job '{job_name}' is disabled"}

        # Convert to orchestrator format
        orchestrator_config = self.convert_job_to_orchestrator_format(job)

        # Create temporary config file for orchestrator
        temp_config_path = f'/tmp/{job_name}_config.json'
        with open(temp_config_path, 'w') as f:
            json.dump(orchestrator_config, f, indent=2)

        # Build user request if not provided
        if user_request is None:
            user_request = f"Execute ETL job: {job.get('description', job_name)}"

        logger.info(f"Running job '{job_name}' with orchestrator...")

        # Run the orchestrator pipeline
        result = self.orchestrator.orchestrate_pipeline(
            user_request=user_request,
            config_path=temp_config_path
        )

        return result

    def run_all_enabled_jobs(self, user_request: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Run all enabled jobs sequentially.

        Args:
            user_request: Optional custom request template

        Returns:
            List of execution results for each job
        """
        results = []
        enabled_jobs = self.get_enabled_jobs()

        logger.info(f"Running {len(enabled_jobs)} enabled jobs...")

        for job in enabled_jobs:
            job_name = job.get('name', 'unnamed')
            logger.info(f"Starting job: {job_name}")

            result = self.run_job(job_name, user_request)
            result['job_name'] = job_name
            results.append(result)

            if result.get('status') == 'failed':
                logger.error(f"Job '{job_name}' failed: {result.get('error')}")
            else:
                logger.info(f"Job '{job_name}' completed successfully")

        return results

    def print_summary(self, results: List[Dict[str, Any]]) -> None:
        """Print execution summary."""
        print("\n" + "=" * 60)
        print("ETL EXECUTION SUMMARY")
        print("=" * 60)

        succeeded = sum(1 for r in results if r.get('status') == 'completed')
        failed = len(results) - succeeded

        print(f"Total Jobs: {len(results)}")
        print(f"Succeeded: {succeeded}")
        print(f"Failed: {failed}")
        print("-" * 60)

        for result in results:
            status_icon = "✓" if result.get('status') == 'completed' else "✗"
            job_name = result.get('job_name', 'unknown')
            print(f"{status_icon} {job_name}: {result.get('status', 'unknown')}")
            if result.get('error'):
                print(f"  Error: {result.get('error')}")

        print("=" * 60)


def main():
    """Main entry point for running ETL jobs."""
    import sys

    config_path = sys.argv[1] if len(sys.argv) > 1 else './config/test_config.json'

    print("=" * 60)
    print("STRANDS ETL FRAMEWORK")
    print("=" * 60)

    # Initialize wrapper
    wrapper = ETLWrapper(config_path)

    # Step 1: Load and validate
    print("\n[Step 1] Loading and validating configuration...")
    validation = wrapper.load_and_validate()

    if not validation['valid']:
        print("Configuration validation failed. Exiting.")
        sys.exit(1)

    print(f"✓ Configuration loaded successfully")
    print(f"  Jobs defined: {len(wrapper._config.get('jobs', []))}")
    print(f"  Enabled jobs: {validation['enabled_jobs']}")

    # Step 2: List enabled jobs
    print("\n[Step 2] Enabled jobs:")
    for job in wrapper.get_enabled_jobs():
        print(f"  - {job.get('name')}: {job.get('description', 'No description')}")

    # Step 3: Run jobs (if user confirms)
    print("\n[Step 3] Ready to execute jobs.")

    # For demo, run all enabled jobs
    if len(sys.argv) > 2 and sys.argv[2] == '--run':
        print("Running all enabled jobs...")
        results = wrapper.run_all_enabled_jobs()
        wrapper.print_summary(results)
    else:
        print("Use '--run' flag to execute jobs: python etl_wrapper.py ./config/test_config.json --run")


if __name__ == '__main__':
    main()
