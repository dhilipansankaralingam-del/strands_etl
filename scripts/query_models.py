#!/usr/bin/env python3
"""
Strands ETL Model Query CLI
============================

Query trained ML models, view training history, and get predictions.

Usage:
    python scripts/query_models.py list                    # List all models
    python scripts/query_models.py show <model_id>         # Show model details
    python scripts/query_models.py costs                   # Show training costs
    python scripts/query_models.py predict --size 100      # Get predictions
    python scripts/query_models.py history                 # Show training history
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# Add framework to path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / 'framework'))

from framework.strands.agents.learning_agent import StrandsLearningAgent


def format_table(headers: list, rows: list, col_widths: list = None) -> str:
    """Format data as an ASCII table."""
    if not col_widths:
        col_widths = [max(len(str(h)), max(len(str(r[i])) for r in rows) if rows else 0) + 2
                      for i, h in enumerate(headers)]

    # Header
    header_row = "|".join(str(h).center(w) for h, w in zip(headers, col_widths))
    separator = "+".join("-" * w for w in col_widths)

    lines = [separator, header_row, separator]

    # Data rows
    for row in rows:
        data_row = "|".join(str(c).center(w) for c, w in zip(row, col_widths))
        lines.append(data_row)

    lines.append(separator)
    return "\n".join(lines)


def cmd_list_models(agent: StrandsLearningAgent, args):
    """List all trained models."""
    models = agent.list_models()

    if not models:
        print("\nNo trained models found.")
        print("Run the ETL pipeline multiple times to generate training data.")
        return

    print(f"\n{'='*80}")
    print("TRAINED MODELS")
    print(f"{'='*80}\n")

    headers = ["Model ID", "Type", "Version", "Records", "Cost ($)", "Created"]
    rows = []

    for m in models:
        created = datetime.fromisoformat(m['created_at']).strftime('%Y-%m-%d %H:%M')
        rows.append([
            m['model_id'],
            m['model_type'],
            m['version'],
            m['training_records'],
            f"${m['training_cost_usd']:.4f}",
            created
        ])

    print(format_table(headers, rows, [16, 20, 10, 10, 12, 18]))
    print(f"\nTotal models: {len(models)}")


def cmd_show_model(agent: StrandsLearningAgent, args):
    """Show details of a specific model."""
    model = agent.get_model(args.model_id)

    if not model:
        print(f"\nModel not found: {args.model_id}")
        print("Use 'list' command to see available models.")
        return

    print(f"\n{'='*80}")
    print(f"MODEL DETAILS: {model['model_id']}")
    print(f"{'='*80}\n")

    print(f"Model ID:        {model['model_id']}")
    print(f"Type:            {model['model_type']}")
    print(f"Version:         {model['version']}")
    print(f"Target:          {model['target']}")
    print(f"Features:        {', '.join(model['features'])}")
    print(f"Created:         {model['created_at']}")
    print()
    print("Training Stats:")
    print(f"  - Records:      {model['training_records']}")
    print(f"  - Data Size:    {model['training_data_size'] / (1024**3):.2f} GB")
    print(f"  - Time:         {model['training_time_seconds']:.3f} seconds")
    print(f"  - Cost:         ${model['training_cost_usd']:.4f}")
    print()
    print("Metrics:")
    for metric, value in model['metrics'].items():
        if isinstance(value, float):
            print(f"  - {metric}: {value:.4f}")
        else:
            print(f"  - {metric}: {value}")
    print()
    print("Hyperparameters:")
    for param, value in model['hyperparameters'].items():
        print(f"  - {param}: {value}")


def cmd_costs(agent: StrandsLearningAgent, args):
    """Show training costs summary."""
    costs = agent.get_training_costs()

    print(f"\n{'='*80}")
    print("TRAINING COSTS SUMMARY")
    print(f"{'='*80}\n")

    print(f"Total Models Trained:     {costs['total_models']}")
    print(f"Total Training Records:   {costs['total_training_records']:,}")
    print(f"Total Training Time:      {costs['total_training_time_seconds']:.2f} seconds")
    print(f"Total Training Cost:      ${costs['total_training_cost_usd']:.4f}")
    print()

    if costs['by_model']:
        print("Cost Breakdown by Model:")
        print("-" * 60)

        headers = ["Model ID", "Type", "Cost ($)", "Time (s)"]
        rows = []

        for model_id, details in costs['by_model'].items():
            rows.append([
                model_id,
                details['type'],
                f"${details['cost_usd']:.4f}",
                f"{details['time_seconds']:.3f}"
            ])

        print(format_table(headers, rows, [16, 20, 12, 12]))


def cmd_predict(agent: StrandsLearningAgent, args):
    """Get predictions from trained models."""
    print(f"\n{'='*80}")
    print("MODEL PREDICTIONS")
    print(f"{'='*80}\n")

    print(f"Input Parameters:")
    print(f"  - Data Size:  {args.size} GB")
    print(f"  - Workers:    {args.workers or 'auto'}")
    print(f"  - Weekend:    {args.weekend}")
    print()

    # Create feature set
    current_record = {
        'total_size_gb': args.size,
        'is_weekend': args.weekend,
        'day_of_week': 5 if args.weekend else 2,
        'recommended_workers': args.workers or 10
    }

    predictions = agent._make_predictions(current_record)

    if not predictions:
        print("No predictions available.")
        print("Train models first by running the ETL pipeline multiple times.")
        return

    print("Predictions:")
    print("-" * 40)

    if 'predicted_workers' in predictions:
        print(f"  Predicted Workers:  {predictions['predicted_workers']}")
        print(f"    (using model: {predictions.get('resource_model_id', 'N/A')})")
        print()

    if 'predicted_runtime_seconds' in predictions:
        runtime = predictions['predicted_runtime_seconds']
        print(f"  Predicted Runtime:  {runtime} seconds ({runtime/60:.1f} minutes)")
        print(f"    (using model: {predictions.get('runtime_model_id', 'N/A')})")
        print()

    if 'predicted_cost_usd' in predictions:
        print(f"  Predicted Cost:     ${predictions['predicted_cost_usd']:.2f}")
        print(f"    (using model: {predictions.get('cost_model_id', 'N/A')})")


def cmd_history(agent: StrandsLearningAgent, args):
    """Show execution history used for training."""
    history_file = agent.models_dir / 'execution_history.json'

    if not history_file.exists():
        print("\nNo execution history found.")
        print("Run the ETL pipeline to generate training data.")
        return

    try:
        with open(history_file, 'r') as f:
            history = json.load(f)
    except Exception as e:
        print(f"Error loading history: {e}")
        return

    print(f"\n{'='*80}")
    print("EXECUTION HISTORY")
    print(f"{'='*80}\n")

    if not history:
        print("No execution records found.")
        return

    # Show summary
    print(f"Total Records: {len(history)}")
    print()

    # Show recent records
    recent = history[-10:]  # Last 10

    headers = ["Date", "Job", "Size (GB)", "Workers", "Duration (s)", "Cost ($)"]
    rows = []

    for rec in recent:
        run_date = datetime.fromisoformat(rec['run_date']).strftime('%Y-%m-%d')
        rows.append([
            run_date,
            rec.get('job_name', 'N/A')[:20],
            f"{rec.get('total_size_gb', 0):.1f}",
            rec.get('recommended_workers', 0),
            f"{rec.get('duration_seconds', 0):.0f}",
            f"${rec.get('cost_usd', 0):.2f}"
        ])

    print("Recent Executions (last 10):")
    print(format_table(headers, rows, [12, 22, 12, 10, 14, 10]))

    # Statistics
    if len(history) >= 5:
        sizes = [r.get('total_size_gb', 0) for r in history]
        costs = [r.get('cost_usd', 0) for r in history]

        print("\nStatistics:")
        print(f"  Average Size:     {sum(sizes)/len(sizes):.1f} GB")
        print(f"  Max Size:         {max(sizes):.1f} GB")
        print(f"  Average Cost:     ${sum(costs)/len(costs):.2f}")
        print(f"  Total Cost:       ${sum(costs):.2f}")


def main():
    parser = argparse.ArgumentParser(
        description='Query Strands ETL trained models',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/query_models.py list
  python scripts/query_models.py show RES-abc123def456
  python scripts/query_models.py costs
  python scripts/query_models.py predict --size 500 --workers 20
  python scripts/query_models.py history
        """
    )

    parser.add_argument('--models-dir', '-d', default='data/models',
                        help='Directory containing trained models')

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # List command
    subparsers.add_parser('list', help='List all trained models')

    # Show command
    show_parser = subparsers.add_parser('show', help='Show model details')
    show_parser.add_argument('model_id', help='Model ID to show')

    # Costs command
    subparsers.add_parser('costs', help='Show training costs summary')

    # Predict command
    predict_parser = subparsers.add_parser('predict', help='Get predictions from models')
    predict_parser.add_argument('--size', '-s', type=float, required=True,
                                help='Data size in GB')
    predict_parser.add_argument('--workers', '-w', type=int, default=None,
                                help='Number of workers (optional)')
    predict_parser.add_argument('--weekend', action='store_true',
                                help='Is weekend run')

    # History command
    subparsers.add_parser('history', help='Show execution history')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Initialize agent with models directory
    config = {'models_dir': args.models_dir}
    agent = StrandsLearningAgent(config)

    # Execute command
    commands = {
        'list': cmd_list_models,
        'show': cmd_show_model,
        'costs': cmd_costs,
        'predict': cmd_predict,
        'history': cmd_history
    }

    cmd_func = commands.get(args.command)
    if cmd_func:
        cmd_func(agent, args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
