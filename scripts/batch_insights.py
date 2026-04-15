#!/usr/bin/env python3
"""
Batch Insights Processor - Loops through configs from S3 and uploads results.

Usage:
    python scripts/batch_insights.py \
        --source s3://bucket/configs/ \
        --dest s3://bucket/results/ \
        --interval 300
"""

import argparse
import boto3
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.agent_cli import InteractiveAgentCLI, AgentDataStore, PredictionEngine


class BatchInsightsProcessor:
    """Process multiple job configs and generate insights."""

    def __init__(self, source_s3: str, dest_s3: str):
        self.s3 = boto3.client('s3')
        self.source_bucket, self.source_prefix = self._parse_s3_path(source_s3)
        self.dest_bucket, self.dest_prefix = self._parse_s3_path(dest_s3)
        self.data_store = AgentDataStore()
        self.prediction_engine = PredictionEngine(self.data_store)

    def _parse_s3_path(self, s3_path: str) -> tuple:
        """Parse s3://bucket/prefix into (bucket, prefix)."""
        path = s3_path.replace('s3://', '')
        parts = path.split('/', 1)
        return parts[0], parts[1] if len(parts) > 1 else ''

    def list_configs(self) -> List[str]:
        """List all config files from S3 source."""
        configs = []
        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.source_bucket, Prefix=self.source_prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.json'):
                    configs.append(obj['Key'])
        return configs

    def download_config(self, key: str) -> Dict:
        """Download and parse config from S3."""
        resp = self.s3.get_object(Bucket=self.source_bucket, Key=key)
        return json.loads(resp['Body'].read().decode('utf-8'))

    def process_config(self, config: Dict) -> Dict[str, Any]:
        """Run insights on a single config, return detailed results."""
        job_name = config.get('job_name', 'unknown')
        cli = InteractiveAgentCLI(config)

        result = {
            'job_name': job_name,
            'config': config,
            'timestamp': datetime.utcnow().isoformat(),
            'predictions': {},
            'compliance': {},
            'recommendations': [],
            'anomalies': [],
            'baselines': {}
        }

        # Get predictions
        from_records = config.get('baseline_records', 100000)
        to_records = config.get('target_records', from_records * 10)

        try:
            predictions = self.prediction_engine.predict_for_scale(
                job_name, from_records, to_records
            )
            for metric, pred in predictions.items():
                result['predictions'][metric] = {
                    'value': pred.predicted_value,
                    'confidence': pred.confidence,
                    'method': pred.method,
                    'factors': pred.factors
                }
        except Exception as e:
            result['predictions']['error'] = str(e)

        # Get compliance (from CLI's internal method)
        try:
            compliance = cli._load_compliance_history()
            result['compliance'] = compliance
        except Exception:
            pass

        # Get learning/baseline info
        try:
            learning = cli._load_learning_history()
            result['baselines'] = learning.get('baselines', {})
            result['recommendations'] = learning.get('recommendations', [])
        except Exception:
            pass

        return result

    def generate_html_summary(self, results: List[Dict]) -> str:
        """Generate HTML summary report."""
        html = """<!DOCTYPE html>
<html><head><title>ETL Insights Report</title>
<style>
body { font-family: Arial, sans-serif; margin: 20px; }
table { border-collapse: collapse; width: 100%; margin: 20px 0; }
th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
th { background: #4CAF50; color: white; }
tr:nth-child(even) { background: #f2f2f2; }
.ok { color: green; } .warn { color: orange; } .fail { color: red; }
h1, h2 { color: #333; }
</style></head><body>
<h1>ETL Batch Insights Report</h1>
<p>Generated: {timestamp}</p>
<h2>Summary</h2>
<table>
<tr><th>Job</th><th>Cost Est.</th><th>Duration Est.</th><th>Memory</th><th>Compliance</th></tr>
""".format(timestamp=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC'))

        for r in results:
            preds = r.get('predictions', {})
            cost = preds.get('cost', {}).get('value', '-')
            duration = preds.get('duration', {}).get('value', '-')
            memory = preds.get('memory', {}).get('value', '-')
            comp_status = r.get('compliance', {}).get('overall_status', 'unknown')
            comp_class = 'ok' if comp_status == 'pass' else 'warn' if comp_status == 'partial' else 'fail'

            cost_str = f"${cost:.2f}" if isinstance(cost, (int, float)) else cost
            dur_str = f"{duration/60:.1f}m" if isinstance(duration, (int, float)) else duration
            mem_str = f"{memory}GB" if isinstance(memory, (int, float)) else memory

            html += f"""<tr>
<td>{r['job_name']}</td>
<td>{cost_str}</td>
<td>{dur_str}</td>
<td>{mem_str}</td>
<td class="{comp_class}">{comp_status}</td>
</tr>"""

        html += """</table>
<h2>Recommendations</h2><ul>"""

        for r in results:
            for rec in r.get('recommendations', [])[:3]:
                html += f"<li><b>{r['job_name']}</b>: {rec}</li>"

        html += "</ul></body></html>"
        return html

    def upload_results(self, results: List[Dict], html: str):
        """Upload JSON and HTML results to S3."""
        ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')

        # Upload detailed JSON
        json_key = f"{self.dest_prefix}insights_{ts}.json"
        self.s3.put_object(
            Bucket=self.dest_bucket,
            Key=json_key,
            Body=json.dumps(results, indent=2, default=str),
            ContentType='application/json'
        )
        print(f"  Uploaded: s3://{self.dest_bucket}/{json_key}")

        # Upload HTML summary
        html_key = f"{self.dest_prefix}summary_{ts}.html"
        self.s3.put_object(
            Bucket=self.dest_bucket,
            Key=html_key,
            Body=html,
            ContentType='text/html'
        )
        print(f"  Uploaded: s3://{self.dest_bucket}/{html_key}")

    def run_once(self) -> int:
        """Run one batch of processing. Returns count of configs processed."""
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Starting batch...")

        configs = self.list_configs()
        if not configs:
            print("  No configs found in source.")
            return 0

        print(f"  Found {len(configs)} config(s)")

        results = []
        for key in configs:
            print(f"  Processing: {key}")
            try:
                config = self.download_config(key)
                result = self.process_config(config)
                results.append(result)
            except Exception as e:
                print(f"    Error: {e}")
                results.append({'job_name': key, 'error': str(e)})

        html = self.generate_html_summary(results)
        self.upload_results(results, html)

        return len(results)

    def run_loop(self, interval_seconds: int):
        """Run continuously with interval between batches."""
        print(f"Starting batch loop (interval: {interval_seconds}s)")
        print(f"  Source: s3://{self.source_bucket}/{self.source_prefix}")
        print(f"  Dest:   s3://{self.dest_bucket}/{self.dest_prefix}")

        while True:
            try:
                self.run_once()
            except KeyboardInterrupt:
                print("\nStopping...")
                break
            except Exception as e:
                print(f"  Batch error: {e}")

            print(f"  Sleeping {interval_seconds}s...")
            time.sleep(interval_seconds)


def main():
    parser = argparse.ArgumentParser(description='Batch ETL Insights Processor')
    parser.add_argument('--source', '-s', required=True, help='S3 path to config files (s3://bucket/prefix/)')
    parser.add_argument('--dest', '-d', required=True, help='S3 path for results (s3://bucket/prefix/)')
    parser.add_argument('--interval', '-i', type=int, default=0, help='Loop interval in seconds (0=run once)')
    parser.add_argument('--once', action='store_true', help='Run once and exit')

    args = parser.parse_args()

    processor = BatchInsightsProcessor(args.source, args.dest)

    if args.once or args.interval == 0:
        processor.run_once()
    else:
        processor.run_loop(args.interval)


if __name__ == '__main__':
    main()
