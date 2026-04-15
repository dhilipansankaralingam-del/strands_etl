"""
CloudWatch Dashboard - AWS native monitoring for ETL jobs
Creates and manages CloudWatch dashboards with ETL metrics, alarms, and insights
"""

import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CloudWatchDashboard:
    """
    CloudWatch Dashboard manager for ETL monitoring.

    Features:
    - Auto-generated dashboards for ETL jobs
    - Real-time metrics visualization
    - Custom alarms for job failures
    - Cost tracking widgets
    - Data quality metrics integration
    """

    # Dashboard widget templates
    WIDGET_TEMPLATES = {
        'job_status': {
            'type': 'metric',
            'properties': {
                'title': 'Job Execution Status',
                'view': 'timeSeries',
                'stacked': False,
                'period': 300,
                'stat': 'Sum'
            }
        },
        'execution_time': {
            'type': 'metric',
            'properties': {
                'title': 'Execution Time',
                'view': 'timeSeries',
                'stacked': False,
                'period': 300,
                'stat': 'Average'
            }
        },
        'memory_usage': {
            'type': 'metric',
            'properties': {
                'title': 'Memory Usage',
                'view': 'timeSeries',
                'stacked': True,
                'period': 60,
                'stat': 'Average'
            }
        },
        'text': {
            'type': 'text',
            'properties': {
                'markdown': ''
            }
        }
    }

    def __init__(self, region: str = 'us-east-1'):
        """
        Initialize CloudWatch Dashboard manager.

        Args:
            region: AWS region
        """
        self.region = region
        self.cloudwatch_client = boto3.client('cloudwatch', region_name=region)
        self.glue_client = boto3.client('glue', region_name=region)

    def create_etl_dashboard(self, dashboard_name: str,
                              job_names: List[str] = None,
                              include_data_quality: bool = True,
                              include_cost: bool = True) -> Dict[str, Any]:
        """
        Create a comprehensive ETL monitoring dashboard.

        Args:
            dashboard_name: Name for the dashboard
            job_names: List of job names to monitor (None = all jobs)
            include_data_quality: Include data quality widgets
            include_cost: Include cost tracking widgets

        Returns:
            Dashboard creation result
        """
        # Get jobs if not specified
        if not job_names:
            try:
                response = self.glue_client.get_jobs(MaxResults=20)
                job_names = [j['Name'] for j in response.get('Jobs', [])]
            except ClientError:
                job_names = []

        if not job_names:
            logger.warning("No jobs found to create dashboard")
            return {'success': False, 'error': 'No jobs found'}

        # Build dashboard body
        widgets = []
        y_position = 0

        # Header text widget
        widgets.append({
            'type': 'text',
            'x': 0,
            'y': y_position,
            'width': 24,
            'height': 2,
            'properties': {
                'markdown': f'# Enterprise ETL Dashboard\nMonitoring {len(job_names)} jobs | Region: {self.region} | Auto-refresh: 1 minute'
            }
        })
        y_position += 2

        # Job status overview
        widgets.append(self._create_job_status_widget(job_names, 0, y_position))
        widgets.append(self._create_execution_time_widget(job_names, 12, y_position))
        y_position += 6

        # Individual job metrics
        for i, job_name in enumerate(job_names[:6]):  # Limit to 6 jobs
            x_pos = (i % 2) * 12
            if i % 2 == 0 and i > 0:
                y_position += 6

            widgets.append(self._create_job_detail_widget(job_name, x_pos, y_position))

        y_position += 6

        # Memory and CPU metrics
        widgets.append(self._create_memory_widget(job_names, 0, y_position))
        widgets.append(self._create_shuffle_widget(job_names, 12, y_position))
        y_position += 6

        # Data quality section
        if include_data_quality:
            widgets.append({
                'type': 'text',
                'x': 0,
                'y': y_position,
                'width': 24,
                'height': 1,
                'properties': {
                    'markdown': '## Data Quality Metrics'
                }
            })
            y_position += 1

            widgets.append(self._create_data_quality_widget(0, y_position))
            y_position += 6

        # Cost tracking section
        if include_cost:
            widgets.append({
                'type': 'text',
                'x': 0,
                'y': y_position,
                'width': 24,
                'height': 1,
                'properties': {
                    'markdown': '## Cost Analysis'
                }
            })
            y_position += 1

            widgets.append(self._create_cost_widget(job_names, 0, y_position))
            widgets.append(self._create_dpu_usage_widget(job_names, 12, y_position))
            y_position += 6

        # Alarms status
        widgets.append(self._create_alarms_widget(0, y_position))

        # Create dashboard
        dashboard_body = {'widgets': widgets}

        try:
            self.cloudwatch_client.put_dashboard(
                DashboardName=dashboard_name,
                DashboardBody=json.dumps(dashboard_body)
            )

            dashboard_url = (
                f"https://{self.region}.console.aws.amazon.com/cloudwatch/home?"
                f"region={self.region}#dashboards:name={dashboard_name}"
            )

            logger.info(f"Created dashboard: {dashboard_name}")

            return {
                'success': True,
                'dashboard_name': dashboard_name,
                'dashboard_url': dashboard_url,
                'widgets_count': len(widgets)
            }

        except ClientError as e:
            logger.error(f"Failed to create dashboard: {e}")
            return {'success': False, 'error': str(e)}

    def _create_job_status_widget(self, job_names: List[str], x: int, y: int) -> Dict:
        """Create job status overview widget."""
        metrics = []

        for job_name in job_names[:10]:
            # Success count
            metrics.append([
                'Glue', 'glue.driver.aggregate.numCompletedTasks',
                'JobName', job_name,
                'Type', 'count',
                {'label': f'{job_name} (completed)'}
            ])

        return {
            'type': 'metric',
            'x': x,
            'y': y,
            'width': 12,
            'height': 6,
            'properties': {
                'title': 'Job Completion Status',
                'view': 'timeSeries',
                'stacked': False,
                'metrics': metrics,
                'region': self.region,
                'period': 300,
                'stat': 'Sum'
            }
        }

    def _create_execution_time_widget(self, job_names: List[str], x: int, y: int) -> Dict:
        """Create execution time widget."""
        metrics = []

        for job_name in job_names[:10]:
            metrics.append([
                'Glue', 'glue.driver.aggregate.elapsedTime',
                'JobName', job_name,
                'Type', 'gauge',
                {'label': job_name}
            ])

        return {
            'type': 'metric',
            'x': x,
            'y': y,
            'width': 12,
            'height': 6,
            'properties': {
                'title': 'Execution Time (ms)',
                'view': 'timeSeries',
                'stacked': False,
                'metrics': metrics,
                'region': self.region,
                'period': 300,
                'stat': 'Average'
            }
        }

    def _create_job_detail_widget(self, job_name: str, x: int, y: int) -> Dict:
        """Create detailed widget for a single job."""
        return {
            'type': 'metric',
            'x': x,
            'y': y,
            'width': 12,
            'height': 6,
            'properties': {
                'title': f'Job: {job_name}',
                'view': 'timeSeries',
                'stacked': False,
                'metrics': [
                    ['Glue', 'glue.driver.aggregate.elapsedTime', 'JobName', job_name, 'Type', 'gauge', {'label': 'Elapsed Time'}],
                    ['Glue', 'glue.driver.aggregate.numCompletedTasks', 'JobName', job_name, 'Type', 'count', {'label': 'Completed Tasks', 'yAxis': 'right'}],
                    ['Glue', 'glue.driver.aggregate.numFailedTasks', 'JobName', job_name, 'Type', 'count', {'label': 'Failed Tasks', 'yAxis': 'right'}]
                ],
                'region': self.region,
                'period': 300
            }
        }

    def _create_memory_widget(self, job_names: List[str], x: int, y: int) -> Dict:
        """Create memory usage widget."""
        metrics = []

        for job_name in job_names[:5]:
            metrics.append([
                'Glue', 'glue.driver.jvm.heap.usage',
                'JobName', job_name,
                'Type', 'gauge',
                {'label': f'{job_name} (driver)'}
            ])

        return {
            'type': 'metric',
            'x': x,
            'y': y,
            'width': 12,
            'height': 6,
            'properties': {
                'title': 'JVM Heap Usage',
                'view': 'timeSeries',
                'stacked': True,
                'metrics': metrics,
                'region': self.region,
                'period': 60,
                'stat': 'Average'
            }
        }

    def _create_shuffle_widget(self, job_names: List[str], x: int, y: int) -> Dict:
        """Create shuffle metrics widget."""
        metrics = []

        for job_name in job_names[:5]:
            metrics.append([
                'Glue', 'glue.driver.aggregate.shuffleBytesWritten',
                'JobName', job_name,
                'Type', 'count',
                {'label': f'{job_name} (shuffle write)'}
            ])

        return {
            'type': 'metric',
            'x': x,
            'y': y,
            'width': 12,
            'height': 6,
            'properties': {
                'title': 'Shuffle Bytes Written',
                'view': 'timeSeries',
                'stacked': False,
                'metrics': metrics,
                'region': self.region,
                'period': 60,
                'stat': 'Sum'
            }
        }

    def _create_data_quality_widget(self, x: int, y: int) -> Dict:
        """Create data quality metrics widget."""
        return {
            'type': 'metric',
            'x': x,
            'y': y,
            'width': 24,
            'height': 6,
            'properties': {
                'title': 'Data Quality Scores',
                'view': 'timeSeries',
                'stacked': False,
                'metrics': [
                    ['ETL/DataQuality', 'OverallScore', {'label': 'Overall Score'}],
                    ['ETL/DataQuality', 'CompletenessScore', {'label': 'Completeness'}],
                    ['ETL/DataQuality', 'ValidityScore', {'label': 'Validity'}],
                    ['ETL/DataQuality', 'UniquenessScore', {'label': 'Uniqueness'}]
                ],
                'region': self.region,
                'period': 300,
                'stat': 'Average',
                'yAxis': {
                    'left': {'min': 0, 'max': 100}
                }
            }
        }

    def _create_cost_widget(self, job_names: List[str], x: int, y: int) -> Dict:
        """Create cost tracking widget."""
        metrics = []

        for job_name in job_names[:5]:
            metrics.append([
                'Glue', 'glue.driver.aggregate.elapsedTime',
                'JobName', job_name,
                'Type', 'gauge',
                {'label': job_name, 'id': f'm{len(metrics)}'}
            ])

        # Add expression for cost estimate (DPU * time * rate)
        return {
            'type': 'metric',
            'x': x,
            'y': y,
            'width': 12,
            'height': 6,
            'properties': {
                'title': 'Estimated Cost by Job',
                'view': 'bar',
                'stacked': True,
                'metrics': metrics,
                'region': self.region,
                'period': 86400,  # Daily
                'stat': 'Sum'
            }
        }

    def _create_dpu_usage_widget(self, job_names: List[str], x: int, y: int) -> Dict:
        """Create DPU usage widget."""
        return {
            'type': 'metric',
            'x': x,
            'y': y,
            'width': 12,
            'height': 6,
            'properties': {
                'title': 'DPU Usage Trend',
                'view': 'timeSeries',
                'stacked': True,
                'metrics': [
                    [{'expression': 'SUM(METRICS())', 'label': 'Total DPU Hours', 'id': 'e1'}]
                ] + [
                    ['Glue', 'glue.driver.aggregate.elapsedTime', 'JobName', job, 'Type', 'gauge', {'id': f'm{i}', 'visible': False}]
                    for i, job in enumerate(job_names[:5])
                ],
                'region': self.region,
                'period': 3600,
                'stat': 'Sum'
            }
        }

    def _create_alarms_widget(self, x: int, y: int) -> Dict:
        """Create alarms status widget."""
        return {
            'type': 'alarm',
            'x': x,
            'y': y,
            'width': 24,
            'height': 4,
            'properties': {
                'title': 'Alarm Status',
                'alarms': []  # Will be populated with actual alarms
            }
        }

    def create_job_alarms(self, job_name: str,
                           failure_threshold: int = 1,
                           duration_threshold_minutes: int = 60,
                           sns_topic_arn: str = None) -> List[Dict[str, Any]]:
        """
        Create CloudWatch alarms for a Glue job.

        Args:
            job_name: Glue job name
            failure_threshold: Number of failures to trigger alarm
            duration_threshold_minutes: Max duration before alarm
            sns_topic_arn: SNS topic for notifications

        Returns:
            List of created alarms
        """
        alarms = []
        actions = [sns_topic_arn] if sns_topic_arn else []

        # Job failure alarm
        try:
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=f'{job_name}-FailureAlarm',
                AlarmDescription=f'Alarm when {job_name} fails',
                MetricName='glue.driver.aggregate.numFailedTasks',
                Namespace='Glue',
                Dimensions=[
                    {'Name': 'JobName', 'Value': job_name},
                    {'Name': 'Type', 'Value': 'count'}
                ],
                Statistic='Sum',
                Period=300,
                EvaluationPeriods=1,
                Threshold=failure_threshold,
                ComparisonOperator='GreaterThanOrEqualToThreshold',
                AlarmActions=actions,
                OKActions=actions,
                TreatMissingData='notBreaching'
            )
            alarms.append({
                'name': f'{job_name}-FailureAlarm',
                'type': 'failure',
                'threshold': failure_threshold
            })
            logger.info(f"Created failure alarm for {job_name}")

        except ClientError as e:
            logger.error(f"Failed to create failure alarm: {e}")

        # Duration alarm
        try:
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=f'{job_name}-DurationAlarm',
                AlarmDescription=f'Alarm when {job_name} exceeds {duration_threshold_minutes} minutes',
                MetricName='glue.driver.aggregate.elapsedTime',
                Namespace='Glue',
                Dimensions=[
                    {'Name': 'JobName', 'Value': job_name},
                    {'Name': 'Type', 'Value': 'gauge'}
                ],
                Statistic='Maximum',
                Period=60,
                EvaluationPeriods=duration_threshold_minutes,
                Threshold=duration_threshold_minutes * 60 * 1000,  # Convert to ms
                ComparisonOperator='GreaterThanThreshold',
                AlarmActions=actions,
                TreatMissingData='notBreaching'
            )
            alarms.append({
                'name': f'{job_name}-DurationAlarm',
                'type': 'duration',
                'threshold_minutes': duration_threshold_minutes
            })
            logger.info(f"Created duration alarm for {job_name}")

        except ClientError as e:
            logger.error(f"Failed to create duration alarm: {e}")

        # Memory alarm
        try:
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=f'{job_name}-MemoryAlarm',
                AlarmDescription=f'Alarm when {job_name} memory usage exceeds 90%',
                MetricName='glue.driver.jvm.heap.usage',
                Namespace='Glue',
                Dimensions=[
                    {'Name': 'JobName', 'Value': job_name},
                    {'Name': 'Type', 'Value': 'gauge'}
                ],
                Statistic='Maximum',
                Period=60,
                EvaluationPeriods=5,
                Threshold=0.9,
                ComparisonOperator='GreaterThanThreshold',
                AlarmActions=actions,
                TreatMissingData='notBreaching'
            )
            alarms.append({
                'name': f'{job_name}-MemoryAlarm',
                'type': 'memory',
                'threshold': '90%'
            })
            logger.info(f"Created memory alarm for {job_name}")

        except ClientError as e:
            logger.error(f"Failed to create memory alarm: {e}")

        return alarms

    def publish_custom_metric(self, metric_name: str, value: float,
                               dimensions: Dict[str, str] = None,
                               namespace: str = 'ETL/Custom') -> bool:
        """
        Publish a custom metric to CloudWatch.

        Args:
            metric_name: Name of the metric
            value: Metric value
            dimensions: Optional dimensions
            namespace: CloudWatch namespace

        Returns:
            True if successful
        """
        try:
            metric_data = {
                'MetricName': metric_name,
                'Value': value,
                'Unit': 'None',
                'Timestamp': datetime.utcnow()
            }

            if dimensions:
                metric_data['Dimensions'] = [
                    {'Name': k, 'Value': v}
                    for k, v in dimensions.items()
                ]

            self.cloudwatch_client.put_metric_data(
                Namespace=namespace,
                MetricData=[metric_data]
            )

            logger.debug(f"Published metric {metric_name}={value}")
            return True

        except ClientError as e:
            logger.error(f"Failed to publish metric: {e}")
            return False

    def publish_data_quality_metrics(self, report: Dict[str, Any]) -> bool:
        """
        Publish data quality metrics from a report.

        Args:
            report: Data quality report dict

        Returns:
            True if successful
        """
        try:
            metrics = []
            timestamp = datetime.utcnow()
            table_name = report.get('table_name', 'unknown')

            # Overall score
            metrics.append({
                'MetricName': 'OverallScore',
                'Value': report.get('overall_score', 0) * 100,
                'Unit': 'Percent',
                'Timestamp': timestamp,
                'Dimensions': [{'Name': 'Table', 'Value': table_name}]
            })

            # Rules passed/failed
            metrics.append({
                'MetricName': 'RulesPassed',
                'Value': report.get('passed_rules', 0),
                'Unit': 'Count',
                'Timestamp': timestamp,
                'Dimensions': [{'Name': 'Table', 'Value': table_name}]
            })

            metrics.append({
                'MetricName': 'RulesFailed',
                'Value': report.get('failed_rules', 0),
                'Unit': 'Count',
                'Timestamp': timestamp,
                'Dimensions': [{'Name': 'Table', 'Value': table_name}]
            })

            # Publish in batches of 20
            for i in range(0, len(metrics), 20):
                batch = metrics[i:i+20]
                self.cloudwatch_client.put_metric_data(
                    Namespace='ETL/DataQuality',
                    MetricData=batch
                )

            logger.info(f"Published {len(metrics)} data quality metrics")
            return True

        except ClientError as e:
            logger.error(f"Failed to publish data quality metrics: {e}")
            return False

    def get_dashboard_url(self, dashboard_name: str) -> str:
        """Get the URL for a CloudWatch dashboard."""
        return (
            f"https://{self.region}.console.aws.amazon.com/cloudwatch/home?"
            f"region={self.region}#dashboards:name={dashboard_name}"
        )

    def delete_dashboard(self, dashboard_name: str) -> bool:
        """Delete a CloudWatch dashboard."""
        try:
            self.cloudwatch_client.delete_dashboards(DashboardNames=[dashboard_name])
            logger.info(f"Deleted dashboard: {dashboard_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete dashboard: {e}")
            return False

    def list_dashboards(self) -> List[str]:
        """List all CloudWatch dashboards."""
        try:
            response = self.cloudwatch_client.list_dashboards()
            return [d['DashboardName'] for d in response.get('DashboardEntries', [])]
        except ClientError as e:
            logger.error(f"Failed to list dashboards: {e}")
            return []


# Example usage
if __name__ == '__main__':
    dashboard = CloudWatchDashboard()

    # Create ETL dashboard
    result = dashboard.create_etl_dashboard(
        dashboard_name='ETL-Monitoring-Dashboard',
        include_data_quality=True,
        include_cost=True
    )

    if result['success']:
        print(f"Dashboard created: {result['dashboard_url']}")
    else:
        print(f"Failed to create dashboard: {result.get('error')}")

    # Create alarms for a job
    alarms = dashboard.create_job_alarms(
        job_name='customer_360_etl',
        failure_threshold=1,
        duration_threshold_minutes=60
    )

    print(f"Created {len(alarms)} alarms")
