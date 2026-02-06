#!/usr/bin/env python3
"""
CloudWatch Dashboard Integration
================================

Creates and manages CloudWatch dashboards for ETL monitoring:
1. Job execution metrics
2. Resource utilization
3. Cost tracking
4. Data quality trends
5. Compliance status
6. Failure prediction indicators

Also handles CloudWatch log integration for centralized logging.
"""

import os
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta


class CloudWatchDashboard:
    """
    CloudWatch dashboard integration for ETL monitoring.
    """

    def __init__(self, config):
        self.config = config
        self.region = getattr(config, 'aws_region', os.getenv('AWS_REGION', 'us-east-1'))
        self.dashboard_name = getattr(config, 'dashboard_name', 'ETL-Framework-Dashboard')
        self.log_group = getattr(config, 'log_group', '/etl-framework/jobs')
        self.metrics_namespace = getattr(config, 'metrics_namespace', 'ETLFramework')

        # Try to import boto3
        try:
            import boto3
            self.cloudwatch = boto3.client('cloudwatch', region_name=self.region)
            self.logs = boto3.client('logs', region_name=self.region)
        except ImportError:
            self.cloudwatch = None
            self.logs = None

    def create_dashboard(self) -> Dict[str, Any]:
        """
        Create or update the CloudWatch dashboard.

        Returns:
            API response or error details
        """
        if not self.cloudwatch:
            return {"success": False, "error": "CloudWatch client not available"}

        dashboard_body = self._build_dashboard_body()

        try:
            response = self.cloudwatch.put_dashboard(
                DashboardName=self.dashboard_name,
                DashboardBody=json.dumps(dashboard_body)
            )
            return {"success": True, "response": response}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _build_dashboard_body(self) -> Dict:
        """Build the dashboard JSON structure."""
        return {
            "widgets": [
                # Header
                {
                    "type": "text",
                    "x": 0, "y": 0, "width": 24, "height": 1,
                    "properties": {
                        "markdown": "# 🚀 ETL Framework Dashboard\n*Real-time monitoring for all ETL jobs*"
                    }
                },

                # Job Success Rate
                {
                    "type": "metric",
                    "x": 0, "y": 1, "width": 6, "height": 6,
                    "properties": {
                        "title": "Job Success Rate",
                        "view": "gauge",
                        "metrics": [
                            [self.metrics_namespace, "SuccessRate", {"label": "Success Rate"}]
                        ],
                        "period": 3600,
                        "stat": "Average",
                        "yAxis": {"left": {"min": 0, "max": 100}}
                    }
                },

                # Jobs Running
                {
                    "type": "metric",
                    "x": 6, "y": 1, "width": 6, "height": 6,
                    "properties": {
                        "title": "Active Jobs",
                        "view": "singleValue",
                        "metrics": [
                            [self.metrics_namespace, "RunningJobs", {"label": "Running"}]
                        ],
                        "period": 60,
                        "stat": "Maximum"
                    }
                },

                # Total Cost Today
                {
                    "type": "metric",
                    "x": 12, "y": 1, "width": 6, "height": 6,
                    "properties": {
                        "title": "Cost Today ($)",
                        "view": "singleValue",
                        "metrics": [
                            [self.metrics_namespace, "JobCost", {"label": "Total Cost"}]
                        ],
                        "period": 86400,
                        "stat": "Sum"
                    }
                },

                # Data Quality Score
                {
                    "type": "metric",
                    "x": 18, "y": 1, "width": 6, "height": 6,
                    "properties": {
                        "title": "DQ Score",
                        "view": "gauge",
                        "metrics": [
                            [self.metrics_namespace, "DataQualityScore", {"label": "DQ Score"}]
                        ],
                        "period": 3600,
                        "stat": "Average",
                        "yAxis": {"left": {"min": 0, "max": 100}}
                    }
                },

                # Job Duration Timeline
                {
                    "type": "metric",
                    "x": 0, "y": 7, "width": 12, "height": 6,
                    "properties": {
                        "title": "Job Duration (minutes)",
                        "view": "timeSeries",
                        "stacked": False,
                        "metrics": [
                            [self.metrics_namespace, "JobDuration", "JobName", "simple_etl", {"label": "Simple ETL"}],
                            [self.metrics_namespace, "JobDuration", "JobName", "complex_pipeline", {"label": "Complex Pipeline"}]
                        ],
                        "period": 300,
                        "stat": "Average"
                    }
                },

                # Job Count by Status
                {
                    "type": "metric",
                    "x": 12, "y": 7, "width": 12, "height": 6,
                    "properties": {
                        "title": "Jobs by Status (24h)",
                        "view": "pie",
                        "metrics": [
                            [self.metrics_namespace, "JobCount", "Status", "SUCCESS", {"label": "Success"}],
                            [self.metrics_namespace, "JobCount", "Status", "FAILED", {"label": "Failed"}],
                            [self.metrics_namespace, "JobCount", "Status", "RUNNING", {"label": "Running"}]
                        ],
                        "period": 86400,
                        "stat": "Sum"
                    }
                },

                # Cost by Platform
                {
                    "type": "metric",
                    "x": 0, "y": 13, "width": 8, "height": 6,
                    "properties": {
                        "title": "Cost by Platform",
                        "view": "bar",
                        "metrics": [
                            [self.metrics_namespace, "JobCost", "Platform", "glue", {"label": "Glue"}],
                            [self.metrics_namespace, "JobCost", "Platform", "emr", {"label": "EMR"}],
                            [self.metrics_namespace, "JobCost", "Platform", "eks", {"label": "EKS"}]
                        ],
                        "period": 86400,
                        "stat": "Sum"
                    }
                },

                # Data Volume Processed
                {
                    "type": "metric",
                    "x": 8, "y": 13, "width": 8, "height": 6,
                    "properties": {
                        "title": "Data Volume (GB)",
                        "view": "timeSeries",
                        "metrics": [
                            [self.metrics_namespace, "DataVolumeGB", {"label": "Volume"}]
                        ],
                        "period": 3600,
                        "stat": "Sum"
                    }
                },

                # Compliance Status
                {
                    "type": "metric",
                    "x": 16, "y": 13, "width": 8, "height": 6,
                    "properties": {
                        "title": "Compliance Checks",
                        "view": "bar",
                        "metrics": [
                            [self.metrics_namespace, "ComplianceCheck", "Status", "COMPLIANT", {"label": "Compliant"}],
                            [self.metrics_namespace, "ComplianceCheck", "Status", "NON_COMPLIANT", {"label": "Non-Compliant"}],
                            [self.metrics_namespace, "ComplianceCheck", "Status", "NEEDS_REVIEW", {"label": "Needs Review"}]
                        ],
                        "period": 86400,
                        "stat": "Sum"
                    }
                },

                # Recent Logs
                {
                    "type": "log",
                    "x": 0, "y": 19, "width": 24, "height": 6,
                    "properties": {
                        "title": "Recent Job Logs",
                        "query": f"SOURCE '{self.log_group}' | fields @timestamp, @message | sort @timestamp desc | limit 50",
                        "region": self.region,
                        "view": "table"
                    }
                },

                # Error Logs
                {
                    "type": "log",
                    "x": 0, "y": 25, "width": 24, "height": 6,
                    "properties": {
                        "title": "Error Logs",
                        "query": f"SOURCE '{self.log_group}' | filter @message like /ERROR|FAILED|Exception/ | fields @timestamp, @message | sort @timestamp desc | limit 20",
                        "region": self.region,
                        "view": "table"
                    }
                }
            ]
        }

    def publish_metric(
        self,
        metric_name: str,
        value: float,
        dimensions: Optional[Dict[str, str]] = None,
        unit: str = "None"
    ) -> Dict[str, Any]:
        """
        Publish a custom metric to CloudWatch.

        Args:
            metric_name: Name of the metric
            value: Metric value
            dimensions: Optional dimension key-value pairs
            unit: Metric unit (e.g., Count, Seconds, Bytes)

        Returns:
            API response or error details
        """
        if not self.cloudwatch:
            return {"success": False, "error": "CloudWatch client not available"}

        metric_data = {
            "MetricName": metric_name,
            "Value": value,
            "Unit": unit,
            "Timestamp": datetime.utcnow()
        }

        if dimensions:
            metric_data["Dimensions"] = [
                {"Name": k, "Value": v} for k, v in dimensions.items()
            ]

        try:
            response = self.cloudwatch.put_metric_data(
                Namespace=self.metrics_namespace,
                MetricData=[metric_data]
            )
            return {"success": True, "response": response}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def publish_job_metrics(
        self,
        job_name: str,
        status: str,
        duration_seconds: int,
        cost: float,
        records_processed: int,
        platform: str
    ) -> List[Dict[str, Any]]:
        """
        Publish all metrics for a job execution.

        Args:
            job_name: Name of the job
            status: Job status (SUCCESS/FAILED)
            duration_seconds: Job duration
            cost: Job cost
            records_processed: Number of records processed
            platform: Platform used (glue/emr/eks)

        Returns:
            List of publish results
        """
        results = []

        # Job count by status
        results.append(self.publish_metric(
            "JobCount",
            1,
            {"Status": status, "JobName": job_name},
            "Count"
        ))

        # Duration
        results.append(self.publish_metric(
            "JobDuration",
            duration_seconds / 60,
            {"JobName": job_name},
            "None"
        ))

        # Cost
        results.append(self.publish_metric(
            "JobCost",
            cost,
            {"Platform": platform, "JobName": job_name},
            "None"
        ))

        # Records
        results.append(self.publish_metric(
            "RecordsProcessed",
            records_processed,
            {"JobName": job_name},
            "Count"
        ))

        # Success rate (1 for success, 0 for failure)
        results.append(self.publish_metric(
            "SuccessRate",
            100 if status == "SUCCESS" else 0,
            {"JobName": job_name},
            "Percent"
        ))

        return results

    def publish_dq_metrics(
        self,
        job_name: str,
        dq_score: float,
        rules_passed: int,
        rules_failed: int
    ) -> List[Dict[str, Any]]:
        """
        Publish data quality metrics.

        Args:
            job_name: Name of the job
            dq_score: Overall DQ score (0-100)
            rules_passed: Number of rules passed
            rules_failed: Number of rules failed

        Returns:
            List of publish results
        """
        results = []

        results.append(self.publish_metric(
            "DataQualityScore",
            dq_score,
            {"JobName": job_name},
            "Percent"
        ))

        results.append(self.publish_metric(
            "DQRulesPassed",
            rules_passed,
            {"JobName": job_name},
            "Count"
        ))

        results.append(self.publish_metric(
            "DQRulesFailed",
            rules_failed,
            {"JobName": job_name},
            "Count"
        ))

        return results

    def publish_compliance_metrics(
        self,
        job_name: str,
        status: str,
        pii_columns: int,
        violations: int
    ) -> List[Dict[str, Any]]:
        """
        Publish compliance metrics.

        Args:
            job_name: Name of the job
            status: Compliance status
            pii_columns: Number of PII columns detected
            violations: Number of violations

        Returns:
            List of publish results
        """
        results = []

        results.append(self.publish_metric(
            "ComplianceCheck",
            1,
            {"Status": status.upper(), "JobName": job_name},
            "Count"
        ))

        results.append(self.publish_metric(
            "PIIColumnsDetected",
            pii_columns,
            {"JobName": job_name},
            "Count"
        ))

        results.append(self.publish_metric(
            "ComplianceViolations",
            violations,
            {"JobName": job_name},
            "Count"
        ))

        return results

    def log_event(
        self,
        message: str,
        level: str = "INFO",
        job_name: Optional[str] = None,
        extra: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Log an event to CloudWatch Logs.

        Args:
            message: Log message
            level: Log level (INFO, WARNING, ERROR)
            job_name: Optional job name
            extra: Optional extra data

        Returns:
            API response or error details
        """
        if not self.logs:
            return {"success": False, "error": "CloudWatch Logs client not available"}

        log_stream = job_name or "framework"
        timestamp = int(datetime.utcnow().timestamp() * 1000)

        log_entry = {
            "level": level,
            "message": message,
            "timestamp": datetime.utcnow().isoformat()
        }
        if job_name:
            log_entry["job_name"] = job_name
        if extra:
            log_entry.update(extra)

        try:
            # Ensure log group exists
            try:
                self.logs.create_log_group(logGroupName=self.log_group)
            except self.logs.exceptions.ResourceAlreadyExistsException:
                pass

            # Ensure log stream exists
            try:
                self.logs.create_log_stream(
                    logGroupName=self.log_group,
                    logStreamName=log_stream
                )
            except self.logs.exceptions.ResourceAlreadyExistsException:
                pass

            # Put log event
            response = self.logs.put_log_events(
                logGroupName=self.log_group,
                logStreamName=log_stream,
                logEvents=[{
                    "timestamp": timestamp,
                    "message": json.dumps(log_entry)
                }]
            )
            return {"success": True, "response": response}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def create_alarm(
        self,
        alarm_name: str,
        metric_name: str,
        threshold: float,
        comparison: str = "GreaterThanThreshold",
        period: int = 300,
        evaluation_periods: int = 2,
        sns_topic_arn: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a CloudWatch alarm.

        Args:
            alarm_name: Name of the alarm
            metric_name: Metric to monitor
            threshold: Alarm threshold
            comparison: Comparison operator
            period: Evaluation period in seconds
            evaluation_periods: Number of periods to evaluate
            sns_topic_arn: Optional SNS topic for notifications

        Returns:
            API response or error details
        """
        if not self.cloudwatch:
            return {"success": False, "error": "CloudWatch client not available"}

        try:
            alarm_params = {
                "AlarmName": alarm_name,
                "MetricName": metric_name,
                "Namespace": self.metrics_namespace,
                "Statistic": "Average",
                "Period": period,
                "EvaluationPeriods": evaluation_periods,
                "Threshold": threshold,
                "ComparisonOperator": comparison,
                "TreatMissingData": "notBreaching"
            }

            if sns_topic_arn:
                alarm_params["AlarmActions"] = [sns_topic_arn]
                alarm_params["OKActions"] = [sns_topic_arn]

            response = self.cloudwatch.put_metric_alarm(**alarm_params)
            return {"success": True, "response": response}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def create_default_alarms(self, sns_topic_arn: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Create default alarms for ETL monitoring.

        Args:
            sns_topic_arn: SNS topic for alarm notifications

        Returns:
            List of alarm creation results
        """
        results = []

        # Job failure rate alarm
        results.append(self.create_alarm(
            alarm_name="ETL-HighFailureRate",
            metric_name="SuccessRate",
            threshold=80,
            comparison="LessThanThreshold",
            period=3600,
            evaluation_periods=1,
            sns_topic_arn=sns_topic_arn
        ))

        # DQ score alarm
        results.append(self.create_alarm(
            alarm_name="ETL-LowDQScore",
            metric_name="DataQualityScore",
            threshold=90,
            comparison="LessThanThreshold",
            period=3600,
            evaluation_periods=1,
            sns_topic_arn=sns_topic_arn
        ))

        # Cost alarm
        results.append(self.create_alarm(
            alarm_name="ETL-HighCost",
            metric_name="JobCost",
            threshold=100,  # $100 per day
            comparison="GreaterThanThreshold",
            period=86400,
            evaluation_periods=1,
            sns_topic_arn=sns_topic_arn
        ))

        # Compliance violations alarm
        results.append(self.create_alarm(
            alarm_name="ETL-ComplianceViolations",
            metric_name="ComplianceViolations",
            threshold=0,
            comparison="GreaterThanThreshold",
            period=3600,
            evaluation_periods=1,
            sns_topic_arn=sns_topic_arn
        ))

        return results

    def get_dashboard_url(self) -> str:
        """Get the URL for the CloudWatch dashboard."""
        return f"https://{self.region}.console.aws.amazon.com/cloudwatch/home?region={self.region}#dashboards:name={self.dashboard_name}"
