#!/usr/bin/env python3
"""
Interactive PySpark Cost Optimizer
==================================

Conversational interface for cost optimization analysis.

Interactive Mode:
    python scripts/optimize_costs_interactive.py

With Initial Script:
    python scripts/optimize_costs_interactive.py --script my_job.py

Conversation Mode (LLM):
    python scripts/optimize_costs_interactive.py --chat

JSON Input:
    python scripts/optimize_costs_interactive.py --input analysis_request.json
"""

import sys
import json
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from cost_optimizer import CostOptimizationOrchestrator, BatchAnalyzer


class InteractiveCostOptimizer:
    """Interactive CLI for cost optimization with conversation support."""

    def __init__(self, use_llm: bool = False, model_id: str = None):
        self.use_llm = use_llm
        self.model_id = model_id or "us.anthropic.claude-sonnet-4-20250514-v1:0"
        self.orchestrator = CostOptimizationOrchestrator(use_llm, model_id)
        self.current_analysis = None
        self.conversation_history = []

        # LLM agent for conversation (lazy loaded)
        self._chat_agent = None

    def _get_chat_agent(self):
        """Get or create chat agent for conversation mode."""
        if self._chat_agent is None and self.use_llm:
            try:
                from strands import Agent
                from cost_optimizer.prompts.super_prompts import ORCHESTRATOR_PROMPT

                self._chat_agent = Agent(
                    model=self.model_id,
                    system_prompt=ORCHESTRATOR_PROMPT + """

You have access to the following analysis results from the cost optimization agents.
Use this data to answer questions and provide recommendations.

When the user asks about:
- Cost savings: Reference the resource_allocator findings
- Code issues: Reference the code_analyzer findings
- Data size: Reference the size_analyzer findings
- Recommendations: Reference the recommendations agent findings

Be specific with numbers, line references, and actionable advice.
"""
                )
            except ImportError:
                print("Warning: strands-agents not installed. Chat mode unavailable.")
                print("Install with: pip install strands-agents")
                return None
        return self._chat_agent

    def run_interactive(self):
        """Run interactive prompt-based input."""
        print("\n" + "="*70)
        print(" 🔍 PySpark Cost Optimization Analyzer")
        print("="*70)
        print(" Enter your job details for cost analysis")
        print(" (Press Ctrl+C to exit at any time)")
        print("="*70 + "\n")

        # Collect inputs
        script_path = self._prompt_script_path()
        source_tables = self._prompt_source_tables()
        processing_mode = self._prompt_processing_mode()
        current_config = self._prompt_current_config()
        complexity_override = self._prompt_complexity_override()

        # Build additional context
        additional_context = {}
        if complexity_override:
            additional_context['complexity_override'] = complexity_override

        runs_per_day = self._prompt_int("Runs per day", default=1)
        additional_context['runs_per_day'] = runs_per_day
        additional_context['runs_per_year'] = runs_per_day * 365

        # Confirm and run
        print("\n" + "-"*70)
        print(" ANALYSIS SUMMARY")
        print("-"*70)
        print(f" Script:          {script_path}")
        print(f" Source Tables:   {len(source_tables)}")
        print(f" Processing Mode: {processing_mode}")
        print(f" Current Workers: {current_config.get('number_of_workers', 'N/A')}")
        print(f" Worker Type:     {current_config.get('worker_type', 'N/A')}")
        print(f" Complexity:      {complexity_override or 'Auto-detect from code'}")
        print("-"*70)

        confirm = input("\nProceed with analysis? [Y/n]: ").strip().lower()
        if confirm == 'n':
            print("Analysis cancelled.")
            return

        # Run analysis
        print("\n⏳ Running analysis...")
        result = self.orchestrator.analyze_script(
            script_path=script_path,
            source_tables=source_tables,
            processing_mode=processing_mode,
            current_config=current_config,
            additional_context=additional_context
        )

        self.current_analysis = result

        # Display results
        self._display_results(result)

        # Offer conversation mode
        if self.use_llm:
            self._offer_conversation_mode(result)
        else:
            self._offer_followup_questions(result)

    def _prompt_script_path(self) -> str:
        """Prompt for script path."""
        while True:
            path = input("📄 PySpark script path: ").strip()
            if Path(path).exists():
                return path
            print(f"   ❌ File not found: {path}")
            print("   Please enter a valid path.")

    def _prompt_source_tables(self) -> List[Dict]:
        """Prompt for source tables."""
        print("\n📊 SOURCE TABLES")
        print("   Enter details for each input table")
        print("   (Leave table name empty to finish)\n")

        tables = []
        table_num = 1

        while True:
            print(f"   --- Table {table_num} ---")
            table_name = input(f"   Table name (or Enter to finish): ").strip()

            if not table_name:
                if not tables:
                    print("   ⚠️  At least one table is required.")
                    continue
                break

            # Get record count
            record_count = self._prompt_int(f"   Record count for {table_name}", default=1000000)

            # Get column count
            column_count = self._prompt_int(f"   Column count", default=30)

            # Get format
            format_type = input(f"   Format [parquet/orc/delta/csv] (default: parquet): ").strip().lower()
            if not format_type:
                format_type = 'parquet'

            # Check if broadcast candidate
            is_small = record_count < 1000000
            broadcast = False
            if is_small:
                broadcast_input = input(f"   Use as broadcast join? [y/N]: ").strip().lower()
                broadcast = broadcast_input == 'y'

            # Check for skew
            has_skew = False
            skew_input = input(f"   Has data skew? [y/N]: ").strip().lower()
            has_skew = skew_input == 'y'

            table = {
                'table': table_name,
                'record_count': record_count,
                'column_count': column_count,
                'format': format_type,
                'broadcast': broadcast,
                'has_skew': has_skew
            }

            tables.append(table)
            table_num += 1
            print()

        return tables

    def _prompt_processing_mode(self) -> str:
        """Prompt for processing mode."""
        print("\n⚙️  PROCESSING MODE")
        print("   1. Full - Process entire dataset")
        print("   2. Delta - Process only new/changed records")

        while True:
            choice = input("   Select [1/2] (default: 1): ").strip()
            if choice == '' or choice == '1':
                return 'full'
            elif choice == '2':
                delta_ratio = self._prompt_float("   Delta ratio (0.01-0.5)", default=0.05)
                return 'delta'
            print("   Please enter 1 or 2")

    def _prompt_current_config(self) -> Dict:
        """Prompt for current job configuration."""
        print("\n🔧 CURRENT CONFIGURATION")
        print("   Enter your current job settings")

        platform = input("   Platform [glue/emr/eks] (default: glue): ").strip().lower()
        if not platform:
            platform = 'glue'

        workers = self._prompt_int("   Number of workers", default=10)

        print("   Worker types: G.1X (16GB), G.2X (32GB), G.4X (64GB), G.8X (128GB)")
        worker_type = input("   Worker type (default: G.2X): ").strip().upper()
        if not worker_type:
            worker_type = 'G.2X'

        timeout = self._prompt_int("   Timeout minutes", default=120)

        return {
            'platform': platform,
            'number_of_workers': workers,
            'worker_type': worker_type,
            'timeout_minutes': timeout
        }

    def _prompt_complexity_override(self) -> Optional[str]:
        """Prompt for optional complexity override."""
        print("\n📈 COMPLEXITY (Optional)")
        print("   1. Auto-detect from code analysis")
        print("   2. Low - Simple transformations, few joins")
        print("   3. Medium - Multiple joins, aggregations")
        print("   4. High - Complex joins, window functions, UDFs")
        print("   5. Very High - Multiple large joins, heavy shuffles")

        choice = input("   Select [1-5] (default: 1 = auto): ").strip()

        complexity_map = {
            '2': 'low',
            '3': 'medium',
            '4': 'high',
            '5': 'very_high'
        }

        return complexity_map.get(choice)

    def _prompt_int(self, prompt: str, default: int = 0) -> int:
        """Prompt for integer input."""
        while True:
            value = input(f"   {prompt} (default: {default}): ").strip()
            if not value:
                return default
            try:
                return int(value.replace(',', ''))
            except ValueError:
                print(f"   Please enter a valid number")

    def _prompt_float(self, prompt: str, default: float = 0.0) -> float:
        """Prompt for float input."""
        while True:
            value = input(f"   {prompt} (default: {default}): ").strip()
            if not value:
                return default
            try:
                return float(value)
            except ValueError:
                print(f"   Please enter a valid number")

    def _display_results(self, result: Dict):
        """Display analysis results."""
        if not result.get('success'):
            print(f"\n❌ Analysis failed: {result.get('error')}")
            return

        summary = result.get('summary', {})
        exec_summary = result.get('executive_summary', {})

        print("\n" + "="*70)
        print(" 📊 ANALYSIS RESULTS")
        print("="*70)

        print(f"\n {exec_summary.get('headline', 'Analysis complete')}")

        print("\n 💰 COST ANALYSIS")
        print(f"    Current Cost/Run:      ${summary.get('current_cost_per_run', 0):.2f}")
        print(f"    Optimal Cost/Run:      ${summary.get('optimal_cost_per_run', 0):.2f}")
        print(f"    Savings Potential:     {summary.get('potential_savings_percent', 0):.0f}%")
        print(f"    Annual Savings:        ${summary.get('potential_annual_savings', 0):,.0f}")

        print("\n 🔍 CODE QUALITY")
        print(f"    Anti-patterns Found:   {summary.get('anti_patterns_found', 0)}")
        print(f"    Critical Issues:       {summary.get('critical_issues', 0)}")
        print(f"    Recommendations:       {summary.get('total_recommendations', 0)}")
        print(f"    Quick Wins:            {summary.get('quick_wins', 0)}")

        print("\n 📈 DATA")
        print(f"    Effective Size:        {summary.get('effective_data_size_gb', 0):.1f} GB")

        # Top recommendations
        recommendations = result.get('all_recommendations', [])[:5]
        if recommendations:
            print("\n 🎯 TOP RECOMMENDATIONS")
            for i, rec in enumerate(recommendations, 1):
                priority = rec.get('priority', 'P3')
                title = rec.get('title', 'Unknown')
                quick = " ⚡" if rec.get('quick_win') else ""
                print(f"    {i}. [{priority}] {title}{quick}")

        print("\n" + "="*70)

    def _offer_conversation_mode(self, result: Dict):
        """Offer LLM conversation mode."""
        print("\n💬 CONVERSATION MODE")
        print("   You can now ask questions about the analysis.")
        print("   Type 'exit' or 'quit' to end the conversation.")
        print("   Type 'save' to save the analysis report.")
        print("-"*70)

        agent = self._get_chat_agent()
        if not agent:
            self._offer_followup_questions(result)
            return

        # Provide context to agent
        context_message = f"""
Here are the analysis results to reference:

SUMMARY:
- Effective Data Size: {result['summary']['effective_data_size_gb']:.1f} GB
- Current Cost/Run: ${result['summary']['current_cost_per_run']:.2f}
- Optimal Cost/Run: ${result['summary']['optimal_cost_per_run']:.2f}
- Potential Savings: {result['summary']['potential_savings_percent']:.0f}%
- Annual Savings: ${result['summary']['potential_annual_savings']:,.0f}
- Anti-patterns Found: {result['summary']['anti_patterns_found']}
- Critical Issues: {result['summary']['critical_issues']}

DETAILED ANALYSIS:
{json.dumps(result['agents'], indent=2, default=str)[:5000]}

Ready to answer questions about this analysis.
"""

        # Initialize conversation
        agent(context_message)

        while True:
            try:
                user_input = input("\n🧑 You: ").strip()

                if not user_input:
                    continue

                if user_input.lower() in ('exit', 'quit', 'q'):
                    print("\n👋 Ending conversation. Goodbye!")
                    break

                if user_input.lower() == 'save':
                    self._save_report(result)
                    continue

                if user_input.lower() == 'help':
                    self._show_conversation_help()
                    continue

                # Get response from agent
                print("\n🤖 Agent: ", end="", flush=True)
                response = agent(user_input)
                print(response)

            except KeyboardInterrupt:
                print("\n\n👋 Conversation ended.")
                break

    def _offer_followup_questions(self, result: Dict):
        """Offer predefined followup questions (non-LLM mode)."""
        print("\n📝 FOLLOWUP OPTIONS")
        print("   1. Show all anti-patterns with line numbers")
        print("   2. Show recommended Spark configurations")
        print("   3. Show implementation roadmap")
        print("   4. Show platform comparison (Glue vs EMR vs EKS)")
        print("   5. Save report to JSON")
        print("   6. Exit")

        while True:
            choice = input("\n   Select [1-6]: ").strip()

            if choice == '1':
                self._show_anti_patterns(result)
            elif choice == '2':
                self._show_spark_configs(result)
            elif choice == '3':
                self._show_roadmap(result)
            elif choice == '4':
                self._show_platform_comparison(result)
            elif choice == '5':
                self._save_report(result)
            elif choice == '6':
                print("\n👋 Goodbye!")
                break
            else:
                print("   Please select 1-6")

    def _show_anti_patterns(self, result: Dict):
        """Show anti-patterns with details."""
        code_analysis = result.get('agents', {}).get('code_analyzer', {}).get('analysis', {})
        anti_patterns = code_analysis.get('anti_patterns', [])

        if not anti_patterns:
            print("\n   ✅ No anti-patterns detected!")
            return

        print(f"\n   ⚠️  ANTI-PATTERNS ({len(anti_patterns)} found)")
        print("   " + "-"*60)

        for pattern in anti_patterns:
            severity = pattern.get('severity', 'unknown').upper()
            name = pattern.get('pattern', 'unknown')
            lines = pattern.get('line_numbers', [])

            print(f"\n   [{severity}] {name}")
            print(f"   Lines: {lines}")
            print(f"   Issue: {pattern.get('description', '')}")
            print(f"   Fix: {pattern.get('fix', '')}")

    def _show_spark_configs(self, result: Dict):
        """Show recommended Spark configurations."""
        code_analysis = result.get('agents', {}).get('code_analyzer', {}).get('analysis', {})
        configs = code_analysis.get('spark_configs', [])

        print("\n   🔧 RECOMMENDED SPARK CONFIGURATIONS")
        print("   " + "-"*60)

        for config in configs:
            print(f"\n   {config.get('config')}")
            print(f"   Current:     {config.get('current_value')}")
            print(f"   Recommended: {config.get('recommended_value')}")
            print(f"   Reason:      {config.get('reason')}")

    def _show_roadmap(self, result: Dict):
        """Show implementation roadmap."""
        roadmap = result.get('implementation_roadmap', {})

        print("\n   📋 IMPLEMENTATION ROADMAP")
        print("   " + "-"*60)

        for phase_key in ['phase_1', 'phase_2', 'phase_3', 'phase_4']:
            phase = roadmap.get(phase_key, {})
            if phase.get('actions'):
                print(f"\n   {phase.get('name', phase_key)}")
                print(f"   Duration: {phase.get('duration')}")
                print(f"   Expected Savings: {phase.get('expected_savings_percent', 0):.0f}%")
                print("   Actions:")
                for action in phase.get('actions', []):
                    print(f"     • {action}")

    def _show_platform_comparison(self, result: Dict):
        """Show platform cost comparison."""
        resource = result.get('agents', {}).get('resource_allocator', {}).get('analysis', {})
        platforms = resource.get('platform_comparison', [])

        print("\n   💵 PLATFORM COST COMPARISON")
        print("   " + "-"*60)
        print(f"   {'Platform':<20} {'Cost/Run':<12} {'Annual':<15} {'Savings'}")
        print("   " + "-"*60)

        for p in platforms:
            name = p.get('platform', 'unknown')
            cost = p.get('cost_per_run', 0)
            annual = p.get('annual_cost', 0)
            savings = p.get('savings_vs_current_percent', 0)
            print(f"   {name:<20} ${cost:<11.2f} ${annual:<14,.0f} {savings:.0f}%")

    def _save_report(self, result: Dict):
        """Save report to JSON file."""
        default_name = f"cost_analysis_{result.get('job_name', 'report')}.json"
        filename = input(f"   Filename (default: {default_name}): ").strip()
        if not filename:
            filename = default_name

        output_path = Path('cost_optimizer/reports') / filename
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w') as f:
            json.dump(result, f, indent=2, default=str)

        print(f"   ✅ Report saved to: {output_path}")

    def _show_conversation_help(self):
        """Show conversation help."""
        print("""
   💡 CONVERSATION TIPS

   Ask questions like:
   • "What are the most impactful changes I can make?"
   • "Explain the anti-pattern on line 45"
   • "How much would I save by switching to EKS?"
   • "What Spark configs should I change?"
   • "How do I implement key salting for the skewed join?"
   • "Compare my options for reducing shuffle"
   • "What's the ROI if I fix just the UDF issues?"

   Commands:
   • 'save' - Save report to JSON
   • 'help' - Show this help
   • 'exit' - End conversation
""")

    def run_from_json(self, json_path: str):
        """Run analysis from JSON input file."""
        with open(json_path) as f:
            input_data = json.load(f)

        print(f"\n📄 Loading analysis request from: {json_path}")

        # Extract inputs
        script_path = input_data.get('script_path')
        if not script_path:
            print("Error: script_path is required in JSON")
            return

        source_tables = input_data.get('source_tables', [])
        if not source_tables:
            print("Error: source_tables is required in JSON")
            return

        processing_mode = input_data.get('processing_mode', 'full')
        current_config = input_data.get('current_config', {})
        additional_context = input_data.get('additional_context', {})

        # Run analysis
        print("\n⏳ Running analysis...")
        result = self.orchestrator.analyze_script(
            script_path=script_path,
            source_tables=source_tables,
            processing_mode=processing_mode,
            current_config=current_config,
            additional_context=additional_context
        )

        self.current_analysis = result
        self._display_results(result)

        # Offer conversation or followup
        if self.use_llm:
            self._offer_conversation_mode(result)
        else:
            self._offer_followup_questions(result)


def main():
    parser = argparse.ArgumentParser(description='Interactive PySpark Cost Optimizer')
    parser.add_argument('--script', '-s', help='Initial script to analyze')
    parser.add_argument('--input', '-i', help='JSON input file with analysis request')
    parser.add_argument('--chat', action='store_true', help='Enable LLM conversation mode')
    parser.add_argument('--model', default='us.anthropic.claude-sonnet-4-20250514-v1:0',
                        help='Bedrock model ID for chat mode')

    args = parser.parse_args()

    # Create optimizer
    optimizer = InteractiveCostOptimizer(
        use_llm=args.chat,
        model_id=args.model
    )

    try:
        if args.input:
            # Run from JSON
            optimizer.run_from_json(args.input)
        else:
            # Run interactive mode
            optimizer.run_interactive()
    except KeyboardInterrupt:
        print("\n\n👋 Goodbye!")
        sys.exit(0)


if __name__ == '__main__':
    main()
