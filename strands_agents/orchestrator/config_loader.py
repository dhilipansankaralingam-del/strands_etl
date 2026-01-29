"""
Config Loader for Strands ETL Framework

Loads and processes etl_config.json with support for:
- Template inheritance
- Variable substitution
- Auto-detection of table sizes
- Data flow analysis
- Validation
"""
import json
import os
import re
from typing import Dict, Any, List, Optional
from strands_agents.tools.catalog_tools import (
    auto_detect_all_table_sizes,
    analyze_data_flow_relationships
)


class ConfigLoader:
    """
    Loads and processes ETL configuration with advanced features.
    """

    def __init__(self, config_path: str = './etl_config.json'):
        """
        Initialize ConfigLoader.

        Args:
            config_path: Path to etl_config.json
        """
        self.config_path = config_path
        self.raw_config = None
        self.processed_config = None
        self.templates = {}
        self.variables = {}

    def load(self) -> Dict[str, Any]:
        """
        Load and process the configuration file.

        Returns:
            Processed configuration dict
        """
        # Load raw config
        with open(self.config_path, 'r') as f:
            self.raw_config = json.load(f)

        # Extract templates and variables
        self.templates = self.raw_config.get('templates', {})
        self.variables = self.raw_config.get('variables', {})

        # Process each job
        processed_jobs = []
        for job in self.raw_config.get('jobs', []):
            processed_job = self._process_job(job)
            processed_jobs.append(processed_job)

        # Build final config
        self.processed_config = {
            'config_version': self.raw_config.get('config_version', '1.0'),
            'config_metadata': self.raw_config.get('config_metadata', {}),
            'jobs': processed_jobs,
            'global_settings': self.raw_config.get('global_settings', {})
        }

        return self.processed_config

    def _process_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single job configuration.

        Applies:
        - Template inheritance
        - Variable substitution
        - Auto-detection

        Args:
            job: Raw job config

        Returns:
            Processed job config
        """
        processed = job.copy()

        # Apply template inheritance
        if 'extends' in job:
            template_name = job['extends']
            if template_name in self.templates:
                template = self.templates[template_name]
                # Merge template with job (job overrides template)
                processed = {**template, **processed}

        # Apply variable substitution recursively
        processed = self._substitute_variables(processed)

        # Auto-detect table sizes if enabled
        if self._should_auto_detect_sizes(processed):
            processed = self._auto_detect_sizes(processed)

        # Analyze data flow if enabled
        if self._should_analyze_data_flow(processed):
            processed = self._analyze_data_flow(processed)

        return processed

    def _substitute_variables(self, obj: Any) -> Any:
        """
        Recursively substitute variables in configuration.

        Supports:
        - ${variables.key} - Reference to variables section
        - ${ENV:key} - Environment variable
        - ${SECRET:key} - Secret placeholder (not resolved here)

        Args:
            obj: Object to process (dict, list, str, etc.)

        Returns:
            Object with variables substituted
        """
        if isinstance(obj, dict):
            return {k: self._substitute_variables(v) for k, v in obj.items()}

        elif isinstance(obj, list):
            return [self._substitute_variables(item) for item in obj]

        elif isinstance(obj, str):
            # Substitute ${variables.key}
            pattern = r'\$\{variables\.(\w+)\}'
            matches = re.findall(pattern, obj)
            for match in matches:
                if match in self.variables:
                    value = self.variables[match]
                    obj = obj.replace(f'${{variables.{match}}}', str(value))

            # Substitute ${ENV:key}
            pattern = r'\$\{ENV:(\w+)(?::([^}]+))?\}'
            matches = re.findall(pattern, obj)
            for env_var, default in matches:
                value = os.environ.get(env_var, default if default else env_var)
                obj = obj.replace(f'${{ENV:{env_var}}}', value)
                if default:
                    obj = obj.replace(f'${{ENV:{env_var}:{default}}}', value)

            # Note: ${SECRET:key} is left as-is for runtime resolution

            return obj

        else:
            return obj

    def _should_auto_detect_sizes(self, job: Dict[str, Any]) -> bool:
        """
        Check if auto-detection of sizes is enabled for this job.
        """
        workload = job.get('workload', {})
        global_settings = self.raw_config.get('global_settings', {})
        feature_flags = global_settings.get('feature_flags', {})

        # Check global flag
        if not feature_flags.get('enable_auto_size_detection', False):
            return False

        # Check job-level flag
        if workload.get('auto_calculate_volume', False):
            return True

        # Check if any data source has auto_detect_size enabled
        data_sources = job.get('data_sources', [])
        return any(ds.get('auto_detect_size', False) for ds in data_sources)

    def _auto_detect_sizes(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Auto-detect sizes for all data sources in the job.
        """
        data_sources = job.get('data_sources', [])
        aws_region = self.raw_config.get('global_settings', {}).get('aws_region', 'us-east-1')

        # Call tool to detect sizes
        detection_results = auto_detect_all_table_sizes(data_sources, aws_region)

        if detection_results.get('success', False):
            # Update data sources with detected sizes
            results_by_name = {
                r['name']: r
                for r in detection_results.get('results', [])
                if r.get('success', False)
            }

            updated_sources = []
            for ds in data_sources:
                name = ds['name']
                if name in results_by_name:
                    result = results_by_name[name]
                    # Merge detected info
                    ds_updated = {**ds, **result}
                    updated_sources.append(ds_updated)
                else:
                    updated_sources.append(ds)

            job['data_sources'] = updated_sources

            # Update workload with total volume
            total_volume = detection_results.get('total_data_volume_gb', 0)
            if 'workload' not in job:
                job['workload'] = {}

            job['workload']['data_volume_gb'] = total_volume
            job['workload']['auto_detected'] = True

        return job

    def _should_analyze_data_flow(self, job: Dict[str, Any]) -> bool:
        """
        Check if data flow analysis is enabled.
        """
        data_flow = job.get('data_flow', {})
        global_settings = self.raw_config.get('global_settings', {})
        feature_flags = global_settings.get('feature_flags', {})

        return (
            feature_flags.get('enable_data_flow_analysis', False)
            and data_flow.get('analysis_enabled', False)
        )

    def _analyze_data_flow(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze data flow relationships for the job.
        """
        data_sources = job.get('data_sources', [])

        # Read script content if available
        script_path = job.get('execution', {}).get('script_path', '')
        script_content = None

        if script_path and os.path.exists(script_path):
            with open(script_path, 'r') as f:
                script_content = f.read()

        # Analyze data flow
        flow_analysis = analyze_data_flow_relationships(data_sources, script_content)

        if flow_analysis.get('success', False):
            # Store analysis in job
            if 'data_flow' not in job:
                job['data_flow'] = {}

            job['data_flow']['analysis_results'] = flow_analysis
            job['data_flow']['analyzed'] = True

        return job

    def get_job_by_id(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a job configuration by job_id.

        Args:
            job_id: Job identifier

        Returns:
            Job config dict or None if not found
        """
        if not self.processed_config:
            self.load()

        for job in self.processed_config.get('jobs', []):
            if job.get('job_id') == job_id:
                return job

        return None

    def list_enabled_jobs(self) -> List[Dict[str, Any]]:
        """
        Get all enabled jobs.

        Returns:
            List of enabled job configs
        """
        if not self.processed_config:
            self.load()

        return [
            job for job in self.processed_config.get('jobs', [])
            if job.get('enabled', False)
        ]

    def validate_config(self) -> Dict[str, Any]:
        """
        Validate the configuration.

        Returns:
            Dict with validation results
        """
        errors = []
        warnings = []

        if not self.processed_config:
            self.load()

        # Validate each job
        for job in self.processed_config.get('jobs', []):
            job_id = job.get('job_id', 'unknown')

            # Required fields
            if 'execution' not in job:
                errors.append(f"Job {job_id}: Missing 'execution' section")

            if 'data_sources' not in job or not job['data_sources']:
                errors.append(f"Job {job_id}: No data sources defined")

            # Validate data sources
            for ds in job.get('data_sources', []):
                ds_name = ds.get('name', 'unknown')
                source_type = ds.get('source_type', ds.get('type', None))

                if not source_type:
                    errors.append(f"Job {job_id}, Source {ds_name}: Missing source_type")

                # Validate source-specific requirements
                if source_type == 'glue_catalog':
                    if 'database' not in ds or 'table' not in ds:
                        errors.append(f"Job {job_id}, Source {ds_name}: glue_catalog requires database and table")

                elif source_type == 's3':
                    if 'path' not in ds:
                        errors.append(f"Job {job_id}, Source {ds_name}: s3 requires path")

            # Check for cost budget
            if 'cost' in job and job['cost'].get('budget_per_run_usd', 0) == 0:
                warnings.append(f"Job {job_id}: No cost budget set")

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'total_jobs': len(self.processed_config.get('jobs', [])),
            'enabled_jobs': len(self.list_enabled_jobs())
        }

    def get_global_settings(self) -> Dict[str, Any]:
        """
        Get global settings.

        Returns:
            Global settings dict
        """
        if not self.processed_config:
            self.load()

        return self.processed_config.get('global_settings', {})


# Example usage
if __name__ == '__main__':
    loader = ConfigLoader('./etl_config.json')

    print("=" * 80)
    print("CONFIG LOADER - EXAMPLE")
    print("=" * 80)

    # Load config
    print("\n📂 Loading configuration...")
    config = loader.load()

    print(f"✓ Loaded {len(config['jobs'])} jobs")

    # Validate
    print("\n🔍 Validating configuration...")
    validation = loader.validate_config()

    if validation['valid']:
        print("✓ Configuration is valid")
    else:
        print(f"✗ Found {len(validation['errors'])} errors")
        for error in validation['errors']:
            print(f"  - {error}")

    if validation['warnings']:
        print(f"⚠ {len(validation['warnings'])} warnings")
        for warning in validation['warnings']:
            print(f"  - {warning}")

    # List enabled jobs
    print("\n📋 Enabled jobs:")
    enabled = loader.list_enabled_jobs()
    for job in enabled:
        print(f"  - {job['job_id']}: {job['job_name']}")

    # Get specific job
    print("\n🔎 Getting job 'customer_order_summary'...")
    job = loader.get_job_by_id('customer_order_summary')

    if job:
        print(f"✓ Found job: {job['job_name']}")
        print(f"  Data sources: {len(job.get('data_sources', []))}")
        print(f"  Platform preference: {job.get('platform', {}).get('user_preference', 'auto')}")

        if job.get('workload', {}).get('auto_detected', False):
            print(f"  Total data volume (auto-detected): {job['workload']['data_volume_gb']} GB")

        if job.get('data_flow', {}).get('analyzed', False):
            analysis = job['data_flow']['analysis_results']
            print(f"  Data flow analysis:")
            print(f"    - Fact tables: {len(analysis.get('fact_tables', []))}")
            print(f"    - Dimension tables: {len(analysis.get('dimension_tables', []))}")
            print(f"    - Broadcast recommendations: {len(analysis.get('broadcast_recommendations', []))}")
