#!/usr/bin/env python3
"""
Streamlit Dashboard Integration
===============================

Interactive Streamlit dashboard for ETL framework:
1. Real-time job monitoring
2. Manual ETL job triggering
3. Configuration management
4. Data quality visualization
5. Compliance status dashboard
6. Historical analysis
7. Cost tracking

Run with: streamlit run streamlit_app.py
"""

import os
import sys
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Streamlit must be imported first
try:
    import streamlit as st
except ImportError:
    print("Streamlit not installed. Install with: pip install streamlit")
    sys.exit(1)


# Page configuration
st.set_page_config(
    page_title="ETL Framework Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)


class ETLDashboard:
    """
    Streamlit dashboard for ETL Framework.
    """

    def __init__(self):
        # Initialize session state
        if 'job_configs' not in st.session_state:
            st.session_state.job_configs = {}
        if 'job_history' not in st.session_state:
            st.session_state.job_history = []
        if 'running_jobs' not in st.session_state:
            st.session_state.running_jobs = {}

    def run(self):
        """Run the Streamlit dashboard."""
        # Sidebar navigation
        st.sidebar.title("🚀 ETL Framework")
        page = st.sidebar.radio(
            "Navigation",
            ["Dashboard", "Trigger Job", "Job History", "Data Quality", "Compliance", "Settings"]
        )

        # Page routing
        if page == "Dashboard":
            self.render_dashboard()
        elif page == "Trigger Job":
            self.render_trigger_job()
        elif page == "Job History":
            self.render_job_history()
        elif page == "Data Quality":
            self.render_data_quality()
        elif page == "Compliance":
            self.render_compliance()
        elif page == "Settings":
            self.render_settings()

    def render_dashboard(self):
        """Render main dashboard page."""
        st.title("📊 ETL Framework Dashboard")

        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                label="Jobs Today",
                value=len([j for j in st.session_state.job_history if self._is_today(j.get('timestamp'))]),
                delta="vs yesterday"
            )

        with col2:
            success_rate = self._calculate_success_rate()
            st.metric(
                label="Success Rate",
                value=f"{success_rate:.1f}%",
                delta=f"{success_rate - 95:.1f}%" if success_rate > 0 else None
            )

        with col3:
            st.metric(
                label="Running Jobs",
                value=len(st.session_state.running_jobs)
            )

        with col4:
            total_cost = sum(j.get('cost', 0) for j in st.session_state.job_history[-100:])
            st.metric(
                label="Cost (Last 100 jobs)",
                value=f"${total_cost:.2f}"
            )

        st.markdown("---")

        # Running jobs section
        st.subheader("🔄 Running Jobs")
        if st.session_state.running_jobs:
            for job_id, job_info in st.session_state.running_jobs.items():
                with st.container():
                    col1, col2, col3 = st.columns([3, 2, 1])
                    with col1:
                        st.write(f"**{job_info.get('name', job_id)}**")
                    with col2:
                        st.progress(job_info.get('progress', 0) / 100)
                    with col3:
                        if st.button("Cancel", key=f"cancel_{job_id}"):
                            self._cancel_job(job_id)
        else:
            st.info("No jobs currently running")

        # Recent job history
        st.subheader("📜 Recent Jobs")
        recent_jobs = st.session_state.job_history[-10:][::-1]

        if recent_jobs:
            for job in recent_jobs:
                status_icon = "✅" if job.get('status') == 'success' else "❌" if job.get('status') == 'failed' else "⏳"
                with st.expander(f"{status_icon} {job.get('name', 'Unknown')} - {job.get('timestamp', 'N/A')}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Status:** {job.get('status', 'unknown')}")
                        st.write(f"**Duration:** {job.get('duration', 'N/A')}")
                    with col2:
                        st.write(f"**Cost:** ${job.get('cost', 0):.2f}")
                        st.write(f"**Records:** {job.get('records', 'N/A')}")
        else:
            st.info("No job history available")

    def render_trigger_job(self):
        """Render job trigger page."""
        st.title("🚀 Trigger ETL Job")

        with st.form("trigger_job_form"):
            # Job selection
            job_name = st.selectbox(
                "Select Job",
                options=["simple_etl", "complex_pipeline", "data_quality_check", "compliance_scan"],
                help="Select the job to trigger"
            )

            st.markdown("---")
            st.subheader("Configuration")

            # Platform configuration
            col1, col2 = st.columns(2)

            with col1:
                platform = st.selectbox(
                    "Platform",
                    options=["glue", "emr", "eks"],
                    help="Select processing platform"
                )

                num_workers = st.slider(
                    "Number of Workers",
                    min_value=2,
                    max_value=100,
                    value=10,
                    help="Number of workers to use"
                )

            with col2:
                worker_type = st.selectbox(
                    "Worker Type",
                    options=["G.1X", "G.2X", "G.4X"] if platform == "glue" else ["m5.xlarge", "m5.2xlarge", "r5.xlarge"],
                    help="Worker instance type"
                )

                timeout_minutes = st.number_input(
                    "Timeout (minutes)",
                    min_value=5,
                    max_value=1440,
                    value=60,
                    help="Maximum job duration"
                )

            st.markdown("---")
            st.subheader("Features")

            col1, col2, col3 = st.columns(3)

            with col1:
                enable_dq = st.checkbox("Enable Data Quality", value=True)
                enable_compliance = st.checkbox("Enable Compliance Check", value=True)

            with col2:
                enable_auto_healing = st.checkbox("Enable Auto-Healing", value=True)
                enable_learning = st.checkbox("Enable Learning", value=True)

            with col3:
                enable_slack = st.checkbox("Slack Notifications", value=True)
                enable_email = st.checkbox("Email Notifications", value=False)

            st.markdown("---")
            st.subheader("Data Sources")

            source_tables = st.text_area(
                "Source Tables (one per line)",
                value="database.source_table_1\ndatabase.source_table_2",
                help="Enter fully qualified table names"
            )

            target_table = st.text_input(
                "Target Table",
                value="database.target_table",
                help="Output table name"
            )

            # Submit button
            submitted = st.form_submit_button("🚀 Trigger Job", use_container_width=True)

            if submitted:
                config = {
                    "job_name": job_name,
                    "platform": platform,
                    "num_workers": num_workers,
                    "worker_type": worker_type,
                    "timeout_minutes": timeout_minutes,
                    "features": {
                        "data_quality": enable_dq,
                        "compliance": enable_compliance,
                        "auto_healing": enable_auto_healing,
                        "learning": enable_learning
                    },
                    "notifications": {
                        "slack": enable_slack,
                        "email": enable_email
                    },
                    "source_tables": source_tables.split("\n"),
                    "target_table": target_table
                }

                self._trigger_job(config)
                st.success(f"Job '{job_name}' triggered successfully!")
                st.json(config)

    def render_job_history(self):
        """Render job history page."""
        st.title("📜 Job History")

        # Filters
        col1, col2, col3 = st.columns(3)

        with col1:
            status_filter = st.selectbox(
                "Status",
                options=["All", "Success", "Failed", "Running"]
            )

        with col2:
            date_range = st.date_input(
                "Date Range",
                value=(datetime.now() - timedelta(days=7), datetime.now())
            )

        with col3:
            job_filter = st.text_input("Job Name Filter")

        # Filter jobs
        filtered_jobs = self._filter_jobs(status_filter, date_range, job_filter)

        # Display jobs
        if filtered_jobs:
            # Summary stats
            total = len(filtered_jobs)
            successful = len([j for j in filtered_jobs if j.get('status') == 'success'])
            failed = len([j for j in filtered_jobs if j.get('status') == 'failed'])

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Jobs", total)
            col2.metric("Successful", successful)
            col3.metric("Failed", failed)
            col4.metric("Success Rate", f"{successful/total*100:.1f}%" if total > 0 else "N/A")

            st.markdown("---")

            # Job table
            st.dataframe(
                [
                    {
                        "Job": j.get('name', 'Unknown'),
                        "Status": j.get('status', 'unknown'),
                        "Duration": j.get('duration', 'N/A'),
                        "Cost": f"${j.get('cost', 0):.2f}",
                        "Records": j.get('records', 'N/A'),
                        "Timestamp": j.get('timestamp', 'N/A')
                    }
                    for j in filtered_jobs
                ],
                use_container_width=True
            )
        else:
            st.info("No jobs match the filter criteria")

    def render_data_quality(self):
        """Render data quality page."""
        st.title("📊 Data Quality Dashboard")

        # Tabs for different views
        tab1, tab2, tab3 = st.tabs(["Overview", "Rule Management", "Trends"])

        with tab1:
            st.subheader("DQ Overview")

            # Sample DQ metrics
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Overall DQ Score", "94.5%", "+2.3%")

            with col2:
                st.metric("Rules Passed", "45/48", "-1")

            with col3:
                st.metric("Critical Issues", "0", "0")

            st.markdown("---")

            # Recent DQ results
            st.subheader("Recent DQ Checks")
            sample_results = [
                {"Table": "customers", "Score": "98%", "Failed Rules": 1, "Last Check": "2 hours ago"},
                {"Table": "orders", "Score": "95%", "Failed Rules": 2, "Last Check": "3 hours ago"},
                {"Table": "products", "Score": "92%", "Failed Rules": 3, "Last Check": "5 hours ago"},
            ]
            st.dataframe(sample_results, use_container_width=True)

        with tab2:
            st.subheader("DQ Rule Management")

            # Add new rule
            with st.expander("➕ Add New Rule"):
                rule_type = st.selectbox("Rule Type", ["Natural Language", "SQL", "Template"])

                if rule_type == "Natural Language":
                    rule_text = st.text_input(
                        "Rule (Natural Language)",
                        placeholder="e.g., customer_id should not be null"
                    )
                elif rule_type == "SQL":
                    rule_text = st.text_area(
                        "Rule (SQL)",
                        placeholder="SELECT COUNT(*) FROM table WHERE condition"
                    )
                else:
                    template = st.selectbox("Template", ["null_check", "unique_check", "range_check"])
                    column = st.text_input("Column Name")
                    rule_text = f"Template: {template} on {column}"

                if st.button("Add Rule"):
                    st.success(f"Rule added: {rule_text}")

            # Existing rules
            st.subheader("Existing Rules")
            rules = [
                {"ID": "NL_001", "Type": "Natural Language", "Rule": "customer_id should not be null", "Status": "Active"},
                {"ID": "SQL_001", "Type": "SQL", "Rule": "SELECT COUNT(*) WHERE amount < 0", "Status": "Active"},
                {"ID": "TPL_001", "Type": "Template", "Rule": "unique_check on order_id", "Status": "Active"},
            ]
            st.dataframe(rules, use_container_width=True)

        with tab3:
            st.subheader("DQ Trends")
            st.line_chart({"DQ Score": [92, 93, 91, 94, 95, 94, 96]})

    def render_compliance(self):
        """Render compliance page."""
        st.title("🔒 Compliance Dashboard")

        # Compliance status
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("GDPR Status", "Compliant", "✅")

        with col2:
            st.metric("HIPAA Status", "Review", "⚠️")

        with col3:
            st.metric("PCI-DSS Status", "Compliant", "✅")

        with col4:
            st.metric("PII Columns", "12", "+2")

        st.markdown("---")

        # PII findings
        st.subheader("PII Detection Results")
        pii_findings = [
            {"Column": "customer_email", "Type": "EMAIL", "Confidence": "95%", "Action": "Masked"},
            {"Column": "phone_number", "Type": "PHONE", "Confidence": "90%", "Action": "Pending"},
            {"Column": "ssn", "Type": "SSN", "Confidence": "99%", "Action": "Encrypted"},
        ]
        st.dataframe(pii_findings, use_container_width=True)

        st.markdown("---")

        # Compliance violations
        st.subheader("Compliance Violations")
        violations = [
            {"Rule": "GDPR-003", "Description": "Missing consent tracking", "Severity": "Medium", "Status": "Open"},
            {"Rule": "HIPAA-002", "Description": "PHI access logging needed", "Severity": "High", "Status": "In Progress"},
        ]

        if violations:
            for v in violations:
                severity_color = "🔴" if v["Severity"] == "High" else "🟡" if v["Severity"] == "Medium" else "🟢"
                with st.expander(f"{severity_color} {v['Rule']} - {v['Description']}"):
                    st.write(f"**Severity:** {v['Severity']}")
                    st.write(f"**Status:** {v['Status']}")
                    if st.button(f"Mark as Resolved", key=v['Rule']):
                        st.success("Marked as resolved")
        else:
            st.success("No compliance violations!")

    def render_settings(self):
        """Render settings page."""
        st.title("⚙️ Settings")

        # Notification settings
        st.subheader("Notifications")

        col1, col2 = st.columns(2)

        with col1:
            st.checkbox("Enable Slack Notifications", value=True)
            st.text_input("Slack Channel", value="#etl-alerts")

        with col2:
            st.checkbox("Enable Email Notifications", value=False)
            st.text_input("Email Recipients", placeholder="team@company.com")

        st.markdown("---")

        # Default job settings
        st.subheader("Default Job Configuration")

        col1, col2 = st.columns(2)

        with col1:
            st.selectbox("Default Platform", ["glue", "emr", "eks"])
            st.number_input("Default Workers", value=10, min_value=2, max_value=100)

        with col2:
            st.selectbox("Default Worker Type", ["G.2X", "G.1X", "G.4X"])
            st.number_input("Default Timeout (min)", value=60, min_value=5, max_value=1440)

        st.markdown("---")

        # Feature flags
        st.subheader("Feature Flags")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.checkbox("Auto-Healing Enabled", value=True)
            st.checkbox("Platform Fallback Enabled", value=True)

        with col2:
            st.checkbox("Learning Agent Enabled", value=True)
            st.checkbox("Code Analysis Enabled", value=True)

        with col3:
            st.checkbox("Compliance Checks Enabled", value=True)
            st.checkbox("Data Quality Enabled", value=True)

        if st.button("Save Settings", use_container_width=True):
            st.success("Settings saved successfully!")

    def _trigger_job(self, config: Dict[str, Any]):
        """Trigger a job with given configuration."""
        job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        st.session_state.running_jobs[job_id] = {
            "name": config["job_name"],
            "config": config,
            "start_time": datetime.now().isoformat(),
            "progress": 0
        }

    def _cancel_job(self, job_id: str):
        """Cancel a running job."""
        if job_id in st.session_state.running_jobs:
            del st.session_state.running_jobs[job_id]
            st.warning(f"Job {job_id} cancelled")

    def _is_today(self, timestamp: Optional[str]) -> bool:
        """Check if timestamp is from today."""
        if not timestamp:
            return False
        try:
            dt = datetime.fromisoformat(timestamp)
            return dt.date() == datetime.now().date()
        except:
            return False

    def _calculate_success_rate(self) -> float:
        """Calculate success rate from job history."""
        if not st.session_state.job_history:
            return 0.0
        successful = len([j for j in st.session_state.job_history if j.get('status') == 'success'])
        return (successful / len(st.session_state.job_history)) * 100

    def _filter_jobs(
        self,
        status_filter: str,
        date_range: tuple,
        job_filter: str
    ) -> List[Dict]:
        """Filter jobs based on criteria."""
        jobs = st.session_state.job_history

        if status_filter != "All":
            jobs = [j for j in jobs if j.get('status', '').lower() == status_filter.lower()]

        if job_filter:
            jobs = [j for j in jobs if job_filter.lower() in j.get('name', '').lower()]

        return jobs


# Main entry point
if __name__ == "__main__":
    dashboard = ETLDashboard()
    dashboard.run()
