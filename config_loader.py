"""
Simple Config Loader and Validator for Strands ETL
Works with the test_config.json format
"""

import json
from typing import Dict, Any, List


class ConfigLoader:
    """Load and validate ETL configuration files."""

    def __init__(self, config_path: str):
        self.config_path = config_path
        self._config = None

    def load(self) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        with open(self.config_path, 'r') as f:
            self._config = json.load(f)
        return self._config

    def validate_config(self) -> Dict[str, Any]:
        """Validate the loaded configuration."""
        if self._config is None:
            return {
                'valid': False,
                'errors': ['Configuration not loaded. Call load() first.'],
                'enabled_jobs': 0
            }

        errors = []
        enabled_jobs = 0

        # Check for jobs array
        if 'jobs' not in self._config:
            errors.append("Missing required field: 'jobs'")
        else:
            jobs = self._config['jobs']
            if not isinstance(jobs, list):
                errors.append("'jobs' must be an array")
            else:
                for i, job in enumerate(jobs):
                    job_errors = self._validate_job(job, i)
                    errors.extend(job_errors)
                    if job.get('enabled', True):
                        enabled_jobs += 1

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'enabled_jobs': enabled_jobs
        }

    def _validate_job(self, job: Dict[str, Any], index: int) -> List[str]:
        """Validate a single job configuration."""
        errors = []
        required_fields = ['name', 'workload', 'platform', 'data_sources', 'target']

        for field in required_fields:
            if field not in job:
                errors.append(f"Job {index}: Missing required field '{field}'")

        if 'workload' in job:
            workload_fields = ['data_volume', 'complexity', 'criticality', 'time_sensitivity']
            for field in workload_fields:
                if field not in job['workload']:
                    errors.append(f"Job {index}: Missing workload field '{field}'")

        return errors


if __name__ == '__main__':
    # Test with the test_config.json
    import sys

    config_path = sys.argv[1] if len(sys.argv) > 1 else './config/test_config.json'

    loader = ConfigLoader(config_path)
    config = loader.load()

    print('✓ Configuration loaded successfully')
    print(f'  Jobs defined: {len(config.get("jobs", []))}')

    validation = loader.validate_config()
    if validation['valid']:
        print('✓ Configuration is valid')
        print(f'  Enabled jobs: {validation["enabled_jobs"]}')
    else:
        print('✗ Configuration has errors:')
        for error in validation['errors']:
            print(f'  - {error}')
