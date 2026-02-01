"""
Streamlit Dashboard - Interactive ETL monitoring and management UI
Provides real-time job monitoring, data quality views, and cost analysis
"""

import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError

# Note: Streamlit import is conditional to allow module import without streamlit installed
try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False
    st = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class DashboardConfig:
    """Dashboard configuration."""
    title: str = "Enterprise ETL Dashboard"
    refresh_interval: int = 60  # seconds
    max_jobs_display: int = 50
    enable_job_control: bool = True
    enable_data_quality: bool = True
    enable_cost_analysis: bool = True
    aws_region: str = 'us-east-1'


class StreamlitDashboard:
    """
    Streamlit-based dashboard for ETL monitoring.

    Features:
    - Real-time job status monitoring
    - Historical job execution trends
    - Data quality metrics
    - Cost analysis and forecasting
    - Interactive job management
    - Agent recommendations view
    """

    def __init__(self, config: DashboardConfig = None, region: str = 'us-east-1'):
        """
        Initialize the dashboard.

        Args:
            config: Dashboard configuration
            region: AWS region
        """
        self.config = config or DashboardConfig()
        self.region = region

        # Initialize AWS clients
        self.glue_client = boto3.client('glue', region_name=region)
        self.cloudwatch_client = boto3.client('cloudwatch', region_name=region)

    def get_jobs(self, max_results: int = 50) -> List[Dict[str, Any]]:
        """Get list of Glue jobs."""
        try:
            response = self.glue_client.get_jobs(MaxResults=max_results)
            return response.get('Jobs', [])
        except ClientError as e:
            logger.error(f"Failed to get jobs: {e}")
            return []

    def get_job_runs(self, job_name: str, max_results: int = 20) -> List[Dict[str, Any]]:
        """Get job run history."""
        try:
            response = self.glue_client.get_job_runs(
                JobName=job_name,
                MaxResults=max_results
            )
            return response.get('JobRuns', [])
        except ClientError as e:
            logger.error(f"Failed to get job runs: {e}")
            return []

    def get_all_recent_runs(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get all job runs in the last N hours."""
        all_runs = []
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)

        jobs = self.get_jobs()
        for job in jobs:
            runs = self.get_job_runs(job['Name'], max_results=10)
            for run in runs:
                if run.get('StartedOn', datetime.min) >= cutoff_time:
                    run['JobName'] = job['Name']
                    all_runs.append(run)

        return sorted(all_runs, key=lambda x: x.get('StartedOn', datetime.min), reverse=True)

    def get_job_metrics(self, job_name: str, hours: int = 24) -> Dict[str, Any]:
        """Get CloudWatch metrics for a job."""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)

        metrics = {}

        try:
            # Get execution time metric
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace='Glue',
                MetricName='glue.driver.aggregate.elapsedTime',
                Dimensions=[
                    {'Name': 'JobName', 'Value': job_name},
                    {'Name': 'Type', 'Value': 'gauge'}
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=3600,
                Statistics=['Average', 'Maximum']
            )
            metrics['execution_time'] = response.get('Datapoints', [])

        except ClientError as e:
            logger.warning(f"Could not get metrics: {e}")

        return metrics

    def render_dashboard(self):
        """Render the complete Streamlit dashboard."""
        if not STREAMLIT_AVAILABLE:
            raise RuntimeError("Streamlit is not installed. Run: pip install streamlit")

        # Page config
        st.set_page_config(
            page_title=self.config.title,
            page_icon="📊",
            layout="wide"
        )

        # Custom CSS
        st.markdown("""
        <style>
        .metric-card {
            background-color: #f8f9fa;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
        }
        .success { color: #28a745; }
        .error { color: #dc3545; }
        .warning { color: #ffc107; }
        .running { color: #17a2b8; }
        </style>
        """, unsafe_allow_html=True)

        # Header
        st.title(f"📊 {self.config.title}")
        st.markdown("---")

        # Sidebar
        self._render_sidebar()

        # Main content - tabs
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📈 Overview",
            "🔄 Job Status",
            "✅ Data Quality",
            "💰 Cost Analysis",
            "🤖 Agent Insights"
        ])

        with tab1:
            self._render_overview()

        with tab2:
            self._render_job_status()

        with tab3:
            self._render_data_quality()

        with tab4:
            self._render_cost_analysis()

        with tab5:
            self._render_agent_insights()

    def _render_sidebar(self):
        """Render sidebar with controls."""
        if not st:
            return

        st.sidebar.title("Controls")

        # Refresh button
        if st.sidebar.button("🔄 Refresh Data"):
            st.rerun()

        # Time range selector
        st.sidebar.selectbox(
            "Time Range",
            ["Last 24 Hours", "Last 7 Days", "Last 30 Days"],
            key="time_range"
        )

        # Job filter
        jobs = self.get_jobs()
        job_names = ["All Jobs"] + [j['Name'] for j in jobs]
        st.sidebar.selectbox(
            "Filter by Job",
            job_names,
            key="job_filter"
        )

        st.sidebar.markdown("---")
        st.sidebar.markdown("### Quick Actions")

        if self.config.enable_job_control:
            if st.sidebar.button("▶️ Run Job"):
                st.sidebar.info("Select a job from the Job Status tab to run")

        st.sidebar.markdown("---")
        st.sidebar.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")

    def _render_overview(self):
        """Render overview metrics."""
        if not st:
            return

        # Get recent runs
        runs = self.get_all_recent_runs(hours=24)

        # Calculate metrics
        total_runs = len(runs)
        successful = sum(1 for r in runs if r.get('JobRunState') == 'SUCCEEDED')
        failed = sum(1 for r in runs if r.get('JobRunState') == 'FAILED')
        running = sum(1 for r in runs if r.get('JobRunState') == 'RUNNING')

        # Display metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Runs (24h)", total_runs)

        with col2:
            st.metric("Successful", successful, delta=f"{successful/total_runs*100:.0f}%" if total_runs else "0%")

        with col3:
            st.metric("Failed", failed, delta=f"-{failed}" if failed else "0", delta_color="inverse")

        with col4:
            st.metric("Running", running)

        st.markdown("---")

        # Recent activity
        st.subheader("Recent Activity")

        if runs:
            # Create DataFrame for display
            run_data = []
            for run in runs[:10]:
                run_data.append({
                    'Job': run.get('JobName', 'Unknown'),
                    'Status': run.get('JobRunState', 'Unknown'),
                    'Started': run.get('StartedOn', 'N/A'),
                    'Duration': f"{run.get('ExecutionTime', 0)}s",
                    'DPU Hours': f"{run.get('DPUSeconds', 0) / 3600:.2f}"
                })

            st.dataframe(run_data, use_container_width=True)
        else:
            st.info("No recent job runs found")

        # Job success trend chart
        st.subheader("Success Rate Trend (7 days)")

        # Placeholder for chart - would need actual historical data
        chart_data = {
            'Date': [(datetime.now() - timedelta(days=i)).strftime('%m/%d') for i in range(6, -1, -1)],
            'Success Rate': [92, 95, 88, 94, 91, 97, 93]
        }
        st.line_chart(chart_data, x='Date', y='Success Rate')

    def _render_job_status(self):
        """Render job status view."""
        if not st:
            return

        st.subheader("Job Status")

        jobs = self.get_jobs()

        if not jobs:
            st.warning("No jobs found")
            return

        # Job selection
        selected_job = st.selectbox(
            "Select Job",
            [j['Name'] for j in jobs]
        )

        if selected_job:
            col1, col2 = st.columns([2, 1])

            with col1:
                # Job details
                job = next((j for j in jobs if j['Name'] == selected_job), None)
                if job:
                    st.markdown(f"**Type:** {job.get('Command', {}).get('Name', 'N/A')}")
                    st.markdown(f"**Workers:** {job.get('NumberOfWorkers', 'N/A')}")
                    st.markdown(f"**Worker Type:** {job.get('WorkerType', 'N/A')}")
                    st.markdown(f"**Timeout:** {job.get('Timeout', 'N/A')} minutes")

            with col2:
                if self.config.enable_job_control:
                    if st.button("▶️ Run Job", key="run_job_btn"):
                        try:
                            response = self.glue_client.start_job_run(JobName=selected_job)
                            st.success(f"Job started! Run ID: {response['JobRunId']}")
                        except ClientError as e:
                            st.error(f"Failed to start job: {e}")

            # Recent runs
            st.markdown("---")
            st.subheader("Recent Runs")

            runs = self.get_job_runs(selected_job)

            if runs:
                run_data = []
                for run in runs:
                    status = run.get('JobRunState', 'Unknown')
                    status_emoji = {
                        'SUCCEEDED': '✅',
                        'FAILED': '❌',
                        'RUNNING': '🔄',
                        'STARTING': '⏳',
                        'STOPPING': '⏹️',
                        'STOPPED': '⏹️'
                    }.get(status, '❓')

                    run_data.append({
                        'Status': f"{status_emoji} {status}",
                        'Run ID': run.get('Id', 'N/A')[:20] + '...',
                        'Started': run.get('StartedOn', 'N/A'),
                        'Duration': f"{run.get('ExecutionTime', 0)}s",
                        'DPU Hours': f"{run.get('DPUSeconds', 0) / 3600:.2f}",
                        'Error': run.get('ErrorMessage', '')[:50] if run.get('ErrorMessage') else ''
                    })

                st.dataframe(run_data, use_container_width=True)

                # Show error details for failed runs
                failed_runs = [r for r in runs if r.get('JobRunState') == 'FAILED']
                if failed_runs:
                    with st.expander("View Failed Run Details"):
                        for run in failed_runs[:3]:
                            st.error(f"**Run {run.get('Id', 'N/A')[:20]}**")
                            st.code(run.get('ErrorMessage', 'No error message'))
            else:
                st.info("No runs found for this job")

    def _render_data_quality(self):
        """Render data quality view."""
        if not st:
            return

        st.subheader("Data Quality Dashboard")

        # Sample data quality metrics (would come from DataQualityAgent in production)
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Overall Score", "94%", delta="2%")

        with col2:
            st.metric("Rules Passed", "47/50")

        with col3:
            st.metric("Tables Monitored", "12")

        st.markdown("---")

        # Quality by table
        st.subheader("Quality by Table")

        quality_data = [
            {'Table': 'orders', 'Score': '98%', 'Completeness': '100%', 'Validity': '96%', 'Uniqueness': '100%'},
            {'Table': 'customers', 'Score': '95%', 'Completeness': '98%', 'Validity': '94%', 'Uniqueness': '100%'},
            {'Table': 'products', 'Score': '92%', 'Completeness': '95%', 'Validity': '91%', 'Uniqueness': '98%'},
            {'Table': 'transactions', 'Score': '88%', 'Completeness': '90%', 'Validity': '88%', 'Uniqueness': '95%'},
        ]

        st.dataframe(quality_data, use_container_width=True)

        # Failed rules
        st.subheader("Failed Rules")

        failed_rules = [
            {'Rule': 'email_format', 'Table': 'customers', 'Fail Rate': '2.1%', 'Severity': '⚠️ Warning'},
            {'Rule': 'amount_positive', 'Table': 'transactions', 'Fail Rate': '0.5%', 'Severity': '❌ Error'},
            {'Rule': 'date_not_null', 'Table': 'orders', 'Fail Rate': '0.1%', 'Severity': '⚠️ Warning'},
        ]

        st.dataframe(failed_rules, use_container_width=True)

    def _render_cost_analysis(self):
        """Render cost analysis view."""
        if not st:
            return

        st.subheader("Cost Analysis")

        # Cost metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("MTD Cost", "$1,247.32", delta="-8%")

        with col2:
            st.metric("Projected Monthly", "$1,850.00")

        with col3:
            st.metric("Avg Cost/Job", "$2.45")

        with col4:
            st.metric("Cost Savings", "$156.00", delta="Flex Mode")

        st.markdown("---")

        # Cost by job
        st.subheader("Top Jobs by Cost")

        cost_data = [
            {'Job': 'customer_360_etl', 'Runs': 30, 'Total Cost': '$450.00', 'Avg Cost': '$15.00'},
            {'Job': 'orders_sync', 'Runs': 60, 'Total Cost': '$280.00', 'Avg Cost': '$4.67'},
            {'Job': 'inventory_update', 'Runs': 90, 'Total Cost': '$225.00', 'Avg Cost': '$2.50'},
            {'Job': 'reports_daily', 'Runs': 30, 'Total Cost': '$180.00', 'Avg Cost': '$6.00'},
        ]

        st.dataframe(cost_data, use_container_width=True)

        # Cost trend chart
        st.subheader("Daily Cost Trend")

        chart_data = {
            'Date': [(datetime.now() - timedelta(days=i)).strftime('%m/%d') for i in range(13, -1, -1)],
            'Cost': [45, 52, 48, 55, 42, 38, 51, 49, 53, 47, 44, 56, 41, 48]
        }
        st.bar_chart(chart_data, x='Date', y='Cost')

        # Optimization recommendations
        st.subheader("Cost Optimization Recommendations")

        recommendations = [
            "💡 Enable Flex mode for 'reports_daily' job to save ~$54/month",
            "💡 Reduce workers from 10 to 6 for 'inventory_update' based on utilization",
            "💡 Consider scheduling 'customer_360_etl' during off-peak hours",
        ]

        for rec in recommendations:
            st.info(rec)

    def _render_agent_insights(self):
        """Render agent insights and recommendations."""
        if not st:
            return

        st.subheader("AI Agent Insights")

        # Tabs for different agents
        agent_tab1, agent_tab2, agent_tab3, agent_tab4 = st.tabs([
            "🔧 Auto-Healing",
            "📊 Workload Assessment",
            "📝 Code Analysis",
            "🔒 Compliance"
        ])

        with agent_tab1:
            st.markdown("### Recent Auto-Healing Actions")

            healing_events = [
                {'Time': '2024-01-15 14:30', 'Job': 'customer_360_etl', 'Issue': 'OOM Error', 'Action': 'Increased memory to G.2X', 'Result': '✅ Resolved'},
                {'Time': '2024-01-15 10:15', 'Job': 'orders_sync', 'Issue': 'Timeout', 'Action': 'Extended timeout to 120min', 'Result': '✅ Resolved'},
                {'Time': '2024-01-14 22:45', 'Job': 'inventory_update', 'Issue': 'Data Skew', 'Action': 'Added salting to join keys', 'Result': '✅ Resolved'},
            ]

            st.dataframe(healing_events, use_container_width=True)

        with agent_tab2:
            st.markdown("### Workload Recommendations")

            workload_recs = [
                {
                    'Job': 'customer_360_etl',
                    'Current': 'Glue Standard (10 G.1X)',
                    'Recommended': 'Glue Flex (8 G.2X)',
                    'Reason': 'Non-urgent workload, can use Flex',
                    'Savings': '~35%'
                },
                {
                    'Job': 'realtime_sync',
                    'Current': 'Glue Standard',
                    'Recommended': 'EMR Serverless',
                    'Reason': 'Streaming workload needs EMR',
                    'Savings': 'N/A'
                }
            ]

            for rec in workload_recs:
                with st.expander(f"📋 {rec['Job']}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown(f"**Current:** {rec['Current']}")
                        st.markdown(f"**Recommended:** {rec['Recommended']}")
                    with col2:
                        st.markdown(f"**Reason:** {rec['Reason']}")
                        st.markdown(f"**Est. Savings:** {rec['Savings']}")

        with agent_tab3:
            st.markdown("### Code Analysis Results")

            code_issues = [
                {'Job': 'customer_360_etl', 'Issue': 'Non-broadcast join detected', 'Severity': '⚠️ Medium', 'Fix': 'Add broadcast hint for small table'},
                {'Job': 'orders_sync', 'Issue': 'Repartition before write missing', 'Severity': '💡 Low', 'Fix': 'Add coalesce() before write'},
                {'Job': 'inventory_update', 'Issue': 'Cache not used for reused DataFrame', 'Severity': '⚠️ Medium', 'Fix': 'Add .cache() after filter'},
            ]

            st.dataframe(code_issues, use_container_width=True)

        with agent_tab4:
            st.markdown("### Compliance Status")

            col1, col2 = st.columns(2)

            with col1:
                st.metric("PII Columns Detected", "15")
                st.metric("Properly Masked", "12")
                st.metric("Requiring Attention", "3")

            with col2:
                compliance_status = {
                    'GDPR': '✅ Compliant',
                    'HIPAA': '⚠️ Review Needed',
                    'PCI-DSS': '✅ Compliant',
                    'CCPA': '✅ Compliant'
                }

                for framework, status in compliance_status.items():
                    st.markdown(f"**{framework}:** {status}")

            st.markdown("---")
            st.markdown("### PII Detection Results")

            pii_data = [
                {'Table': 'customers', 'Column': 'email', 'PII Type': 'Email', 'Masked': '✅'},
                {'Table': 'customers', 'Column': 'phone', 'PII Type': 'Phone', 'Masked': '✅'},
                {'Table': 'customers', 'Column': 'ssn', 'PII Type': 'SSN', 'Masked': '❌'},
                {'Table': 'orders', 'Column': 'credit_card', 'PII Type': 'Credit Card', 'Masked': '❌'},
            ]

            st.dataframe(pii_data, use_container_width=True)


def run_dashboard():
    """Run the Streamlit dashboard."""
    dashboard = StreamlitDashboard()
    dashboard.render_dashboard()


# For running with: streamlit run streamlit_dashboard.py
if __name__ == '__main__':
    if STREAMLIT_AVAILABLE:
        run_dashboard()
    else:
        print("Streamlit is not installed. Install with: pip install streamlit")
        print("Then run: streamlit run integrations/streamlit_dashboard.py")
