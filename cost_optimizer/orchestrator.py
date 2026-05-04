"""
Cost Optimization Orchestrator
==============================

Coordinates multiple agents to analyze PySpark jobs for cost optimization.
Supports both single-script and batch analysis.
"""

import json
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from .agents.base import AnalysisInput, AnalysisResult
from .agents.size_analyzer import SizeAnalyzerAgent
from .agents.code_analyzer import CodeAnalyzerAgent
from .agents.resource_allocator import ResourceAllocatorAgent
from .agents.recommendations import RecommendationsAgent


class CostOptimizationOrchestrator:
    """Orchestrates cost optimization analysis across multiple agents."""

    def __init__(self, use_llm: bool = False, model_id: str = None, parallel: bool = True):
        """
        Initialize orchestrator.

        Args:
            use_llm: Use LLM for analysis (requires Bedrock)
            model_id: Bedrock model ID
            parallel: Run independent agents in parallel
        """
        self.use_llm = use_llm
        self.model_id = model_id
        self.parallel = parallel

        # Initialize agents
        self.agents = {
            'size_analyzer': SizeAnalyzerAgent(use_llm, model_id),
            'code_analyzer': CodeAnalyzerAgent(use_llm, model_id),
            'resource_allocator': ResourceAllocatorAgent(use_llm, model_id),
            'recommendations': RecommendationsAgent(use_llm, model_id)
        }

    def analyze_script(
        self,
        script_path: str,
        source_tables: List[Dict],
        processing_mode: str = 'full',
        current_config: Dict = None,
        additional_context: Dict = None
    ) -> Dict[str, Any]:
        """
        Analyze a single PySpark script.

        Args:
            script_path: Path to PySpark script
            source_tables: List of source table definitions
            processing_mode: 'delta' or 'full'
            current_config: Current job configuration
            additional_context: Additional context (runs_per_day, etc.)

        Returns:
            Complete analysis results from all agents
        """
        start_time = time.time()

        # Load script
        script_file = Path(script_path)
        if not script_file.exists():
            return {
                'success': False,
                'error': f'Script not found: {script_path}',
                'script_path': script_path
            }

        script_content = script_file.read_text()

        # Build input
        input_data = AnalysisInput(
            script_path=script_path,
            script_content=script_content,
            source_tables=source_tables,
            processing_mode=processing_mode,
            current_config=current_config or {},
            job_name=script_file.stem,
            additional_context=additional_context or {}
        )

        # Phase 1: Size Analysis (runs first)
        size_result = self.agents['size_analyzer'].analyze(input_data, {})

        # Build context for next agents
        context = {
            'effective_size_gb': size_result.analysis.get('effective_size_gb', 100),
            'skew_risk_score': size_result.analysis.get('skew_risk_score', 20),
            'join_amplification_factor': size_result.analysis.get('join_amplification_factor', 1.0)
        }

        # Phase 2: Code Analysis and Resource Allocation (can run in parallel)
        if self.parallel:
            with ThreadPoolExecutor(max_workers=2) as executor:
                code_future = executor.submit(
                    self.agents['code_analyzer'].analyze, input_data, context
                )
                resource_future = executor.submit(
                    self.agents['resource_allocator'].analyze, input_data, context
                )

                code_result = code_future.result()
                resource_result = resource_future.result()
        else:
            code_result = self.agents['code_analyzer'].analyze(input_data, context)
            resource_result = self.agents['resource_allocator'].analyze(input_data, context)

        # Update context with code analysis results
        context.update({
            'complexity_score': code_result.analysis.get('complexity', {}).get('complexity_score', 50),
            'join_count': code_result.analysis.get('complexity', {}).get('join_count', 0),
            'anti_pattern_count': code_result.analysis.get('anti_pattern_count', 0)
        })

        # Phase 3: Recommendations (needs all previous results)
        full_context = {
            'size_analyzer': size_result.analysis,
            'code_analyzer': code_result.analysis,
            'resource_allocator': resource_result.analysis
        }
        full_context.update(context)

        reco_result = self.agents['recommendations'].analyze(input_data, full_context)

        # Compile final results
        total_time = time.time() - start_time

        return {
            'success': True,
            'script_path': script_path,
            'job_name': input_data.job_name,
            'processing_mode': processing_mode,
            'analysis_timestamp': datetime.utcnow().isoformat(),
            'total_analysis_time_seconds': round(total_time, 2),

            'agents': {
                'size_analyzer': size_result.to_dict(),
                'code_analyzer': code_result.to_dict(),
                'resource_allocator': resource_result.to_dict(),
                'recommendations': reco_result.to_dict()
            },

            'summary': {
                'effective_data_size_gb': size_result.analysis.get('effective_size_gb', 0),
                'current_cost_per_run': resource_result.analysis.get('current_config', {}).get('cost_per_run', 0),
                'optimal_cost_per_run': resource_result.analysis.get('optimal_config', {}).get('cost_per_run', 0),
                'potential_savings_percent': reco_result.analysis.get('cost_analysis', {}).get('savings_percent', 0),
                'potential_annual_savings': reco_result.analysis.get('cost_analysis', {}).get('potential_annual_savings', 0),
                'anti_patterns_found': code_result.analysis.get('anti_pattern_count', 0),
                'critical_issues': code_result.analysis.get('critical_issues', 0),
                'total_recommendations': len(reco_result.recommendations),
                'quick_wins': sum(1 for r in reco_result.recommendations if r.get('quick_win', False))
            },

            'executive_summary': reco_result.analysis.get('executive_summary', {}),
            'implementation_roadmap': reco_result.analysis.get('implementation_roadmap', {}),
            'all_recommendations': reco_result.recommendations
        }


class BatchAnalyzer:
    """Batch analyzer for processing multiple scripts."""

    def __init__(
        self,
        use_llm: bool = False,
        model_id: str = None,
        max_workers: int = 4,
        output_dir: str = 'cost_optimizer/reports'
    ):
        """
        Initialize batch analyzer.

        Args:
            use_llm: Use LLM for analysis
            model_id: Bedrock model ID
            max_workers: Maximum parallel script analysis
            output_dir: Directory for output reports
        """
        self.orchestrator = CostOptimizationOrchestrator(use_llm, model_id)
        self.max_workers = max_workers
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def analyze_batch(
        self,
        scripts: List[Dict],
        default_config: Dict = None,
        progress_callback=None
    ) -> Dict[str, Any]:
        """
        Analyze multiple scripts.

        Args:
            scripts: List of script configs, each with:
                - script_path: Path to PySpark script
                - source_tables: List of source tables
                - processing_mode: 'delta' or 'full'
                - current_config: Current job config
            default_config: Default config applied to all scripts
            progress_callback: Function called with (completed, total, script_path)

        Returns:
            Batch analysis results with aggregated metrics
        """
        start_time = time.time()
        results = []
        errors = []
        total = len(scripts)

        print(f"\n{'='*70}")
        print(f" Cost Optimization Analysis - {total} Scripts")
        print(f"{'='*70}\n")

        # Process scripts
        if self.max_workers > 1:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {}
                for script_config in scripts:
                    future = executor.submit(
                        self._analyze_single,
                        script_config,
                        default_config or {}
                    )
                    futures[future] = script_config.get('script_path', 'unknown')

                for i, future in enumerate(as_completed(futures), 1):
                    script_path = futures[future]
                    try:
                        result = future.result()
                        if result['success']:
                            results.append(result)
                            status = '✓'
                        else:
                            errors.append({'script': script_path, 'error': result.get('error')})
                            status = '✗'
                    except Exception as e:
                        errors.append({'script': script_path, 'error': str(e)})
                        status = '✗'

                    print(f"  [{i}/{total}] {status} {script_path}")
                    if progress_callback:
                        progress_callback(i, total, script_path)
        else:
            for i, script_config in enumerate(scripts, 1):
                script_path = script_config.get('script_path', 'unknown')
                try:
                    result = self._analyze_single(script_config, default_config or {})
                    if result['success']:
                        results.append(result)
                        status = '✓'
                    else:
                        errors.append({'script': script_path, 'error': result.get('error')})
                        status = '✗'
                except Exception as e:
                    errors.append({'script': script_path, 'error': str(e)})
                    status = '✗'

                print(f"  [{i}/{total}] {status} {script_path}")
                if progress_callback:
                    progress_callback(i, total, script_path)

        total_time = time.time() - start_time

        # Aggregate results
        aggregated = self._aggregate_results(results)

        # Generate batch report
        batch_report = {
            'batch_id': datetime.utcnow().strftime('%Y%m%d_%H%M%S'),
            'analysis_timestamp': datetime.utcnow().isoformat(),
            'total_scripts': total,
            'successful': len(results),
            'failed': len(errors),
            'total_analysis_time_seconds': round(total_time, 2),
            'aggregated_metrics': aggregated,
            'individual_results': results,
            'errors': errors
        }

        # Save report
        report_path = self._save_report(batch_report)
        batch_report['report_path'] = str(report_path)

        # Print summary
        self._print_summary(aggregated, total_time, len(results), len(errors))

        return batch_report

    def _analyze_single(self, script_config: Dict, default_config: Dict) -> Dict:
        """Analyze a single script."""
        # Merge with defaults
        config = {**default_config, **script_config}

        return self.orchestrator.analyze_script(
            script_path=config['script_path'],
            source_tables=config.get('source_tables', []),
            processing_mode=config.get('processing_mode', 'full'),
            current_config=config.get('current_config', {}),
            additional_context=config.get('additional_context', {})
        )

    def _aggregate_results(self, results: List[Dict]) -> Dict:
        """Aggregate results across all scripts."""
        if not results:
            return {}

        # Sum costs
        total_current_cost = sum(
            r['summary'].get('current_cost_per_run', 0) for r in results
        )
        total_optimal_cost = sum(
            r['summary'].get('optimal_cost_per_run', 0) for r in results
        )
        total_annual_savings = sum(
            r['summary'].get('potential_annual_savings', 0) for r in results
        )

        # Count issues
        total_anti_patterns = sum(
            r['summary'].get('anti_patterns_found', 0) for r in results
        )
        total_critical = sum(
            r['summary'].get('critical_issues', 0) for r in results
        )
        total_recommendations = sum(
            r['summary'].get('total_recommendations', 0) for r in results
        )
        total_quick_wins = sum(
            r['summary'].get('quick_wins', 0) for r in results
        )

        # Calculate averages
        avg_savings_pct = sum(
            r['summary'].get('potential_savings_percent', 0) for r in results
        ) / len(results)

        # Top issues by frequency
        all_anti_patterns = []
        for r in results:
            code_analysis = r.get('agents', {}).get('code_analyzer', {}).get('analysis', {})
            for pattern in code_analysis.get('anti_patterns', []):
                all_anti_patterns.append(pattern['pattern'])

        pattern_counts = {}
        for p in all_anti_patterns:
            pattern_counts[p] = pattern_counts.get(p, 0) + 1

        top_patterns = sorted(pattern_counts.items(), key=lambda x: -x[1])[:10]

        return {
            'total_scripts_analyzed': len(results),
            'total_current_cost_per_run': round(total_current_cost, 2),
            'total_optimal_cost_per_run': round(total_optimal_cost, 2),
            'total_savings_per_run': round(total_current_cost - total_optimal_cost, 2),
            'total_annual_savings': round(total_annual_savings, 2),
            'average_savings_percent': round(avg_savings_pct, 1),
            'total_anti_patterns': total_anti_patterns,
            'total_critical_issues': total_critical,
            'total_recommendations': total_recommendations,
            'total_quick_wins': total_quick_wins,
            'top_anti_patterns': [{'pattern': p, 'count': c} for p, c in top_patterns],
            'scripts_by_savings': sorted(
                [
                    {
                        'script': r['job_name'],
                        'savings_percent': r['summary'].get('potential_savings_percent', 0),
                        'annual_savings': r['summary'].get('potential_annual_savings', 0)
                    }
                    for r in results
                ],
                key=lambda x: -x['annual_savings']
            )[:20]
        }

    def _save_report(self, report: Dict) -> Path:
        """Save report to file."""
        filename = f"cost_optimization_report_{report['batch_id']}.json"
        filepath = self.output_dir / filename

        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        return filepath

    def _print_summary(self, aggregated: Dict, total_time: float, success: int, failed: int):
        """Print summary to console."""
        print(f"\n{'='*70}")
        print(" BATCH ANALYSIS SUMMARY")
        print(f"{'='*70}")
        print(f"  Scripts Analyzed:    {success} successful, {failed} failed")
        print(f"  Analysis Time:       {total_time:.1f} seconds")
        print(f"")
        print(f"  COST METRICS:")
        print(f"  Current Cost/Run:    ${aggregated.get('total_current_cost_per_run', 0):,.2f}")
        print(f"  Optimal Cost/Run:    ${aggregated.get('total_optimal_cost_per_run', 0):,.2f}")
        print(f"  Savings/Run:         ${aggregated.get('total_savings_per_run', 0):,.2f}")
        print(f"  Average Savings:     {aggregated.get('average_savings_percent', 0):.1f}%")
        print(f"  Annual Savings:      ${aggregated.get('total_annual_savings', 0):,.0f}")
        print(f"")
        print(f"  CODE QUALITY:")
        print(f"  Anti-patterns:       {aggregated.get('total_anti_patterns', 0)}")
        print(f"  Critical Issues:     {aggregated.get('total_critical_issues', 0)}")
        print(f"  Recommendations:     {aggregated.get('total_recommendations', 0)}")
        print(f"  Quick Wins:          {aggregated.get('total_quick_wins', 0)}")
        print(f"")

        top_patterns = aggregated.get('top_anti_patterns', [])[:5]
        if top_patterns:
            print(f"  TOP ANTI-PATTERNS:")
            for p in top_patterns:
                print(f"    - {p['pattern']}: {p['count']} occurrences")

        print(f"{'='*70}\n")
