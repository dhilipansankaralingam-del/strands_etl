"""
Strands ETL Dashboard - Real-time monitoring and visualization
Built with Streamlit for comprehensive pipeline monitoring
"""

import streamlit as st
import boto3
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging

# Configure page
st.set_page_config(
    page_title="Strands ETL Dashboard",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# AWS Clients
@st.cache_resource
def get_aws_clients():
    return {
        's3': boto3.client('s3'),
        'glue': boto3.client('glue'),
        'bedrock_agent_runtime': boto3.client('bedrock-agent-runtime'),
        'cloudwatch': boto3.client('cloudwatch'),
        'logs': boto3.client('logs')
    }

clients = get_aws_clients()

# Configuration
LEARNING_BUCKET = 'strands-etl-learning'
SUPERVISOR_AGENT_ID = st.secrets.get('SUPERVISOR_AGENT_ID', '')
SUPERVISOR_ALIAS_ID = st.secrets.get('SUPERVISOR_ALIAS_ID', 'TSTALIASID')


# ====================
# Data Loading Functions
# ====================

@st.cache_data(ttl=60)  # Cache for 1 minute
def load_recent_executions(limit: int = 100) -> List[Dict[str, Any]]:
    """Load recent pipeline executions from S3 learning vectors"""
    try:
        response = clients['s3'].list_objects_v2(
            Bucket=LEARNING_BUCKET,
            Prefix='learning/vectors/learning/',
            MaxKeys=limit
        )

        executions = []
        if 'Contents' in response:
            # Sort by last modified (most recent first)
            objects = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)

            for obj in objects[:limit]:
                try:
                    vector_data = clients['s3'].get_object(Bucket=LEARNING_BUCKET, Key=obj['Key'])
                    vector = json.loads(vector_data['Body'].read().decode('utf-8'))

                    executions.append({
                        'vector_id': vector.get('vector_id'),
                        'timestamp': vector.get('timestamp'),
                        'platform': vector.get('execution_metrics', {}).get('platform_used'),
                        'success': vector.get('execution_metrics', {}).get('success', False),
                        'execution_time': vector.get('execution_metrics', {}).get('execution_time_seconds'),
                        'quality_score': vector.get('execution_metrics', {}).get('data_quality_score'),
                        'workload': vector.get('workload_characteristics', {}),
                        'decision_confidence': vector.get('decision_context', {}).get('decision_confidence'),
                        'full_data': vector
                    })
                except Exception as e:
                    logging.warning(f"Error loading vector {obj['Key']}: {e}")
                    continue

        return executions

    except Exception as e:
        st.error(f"Error loading executions: {e}")
        return []


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_quality_reports(limit: int = 50) -> List[Dict[str, Any]]:
    """Load recent quality assessment reports"""
    try:
        response = clients['s3'].list_objects_v2(
            Bucket=LEARNING_BUCKET,
            Prefix='quality/reports/',
            MaxKeys=limit
        )

        reports = []
        if 'Contents' in response:
            objects = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)

            for obj in objects[:limit]:
                try:
                    report_data = clients['s3'].get_object(Bucket=LEARNING_BUCKET, Key=obj['Key'])
                    report = json.loads(report_data['Body'].read().decode('utf-8'))
                    reports.append(report)
                except:
                    continue

        return reports

    except Exception as e:
        st.error(f"Error loading quality reports: {e}")
        return []


@st.cache_data(ttl=300)
def load_optimization_recommendations(limit: int = 50) -> List[Dict[str, Any]]:
    """Load recent optimization recommendations"""
    try:
        response = clients['s3'].list_objects_v2(
            Bucket=LEARNING_BUCKET,
            Prefix='optimization/recommendations/',
            MaxKeys=limit
        )

        recommendations = []
        if 'Contents' in response:
            objects = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)

            for obj in objects[:limit]:
                try:
                    rec_data = clients['s3'].get_object(Bucket=LEARNING_BUCKET, Key=obj['Key'])
                    rec = json.loads(rec_data['Body'].read().decode('utf-8'))
                    recommendations.append(rec)
                except:
                    continue

        return recommendations

    except Exception as e:
        st.error(f"Error loading recommendations: {e}")
        return []


def get_active_glue_jobs() -> List[Dict[str, Any]]:
    """Get currently running Glue jobs"""
    try:
        response = clients['glue'].get_job_runs(
            JobName='',  # Empty to get all
            MaxResults=10
        )

        active_jobs = []
        for run in response.get('JobRuns', []):
            if run['JobRunState'] == 'RUNNING':
                active_jobs.append(run)

        return active_jobs

    except:
        return []


# ====================
# Visualization Components
# ====================

def render_metrics_overview(executions: List[Dict]):
    """Render top-level metrics"""
    st.header("📊 Overview Metrics")

    # Calculate metrics
    total = len(executions)
    successful = sum(1 for e in executions if e.get('success'))
    failed = total - successful
    success_rate = (successful / total * 100) if total > 0 else 0

    avg_execution_time = sum(e.get('execution_time', 0) or 0 for e in executions) / total if total > 0 else 0
    avg_quality = sum(e.get('quality_score', 0) or 0 for e in executions) / total if total > 0 else 0

    # Platform distribution
    platform_counts = {}
    for e in executions:
        platform = e.get('platform', 'unknown')
        platform_counts[platform] = platform_counts.get(platform, 0) + 1

    # Display in columns
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("Total Executions", total)

    with col2:
        st.metric("Success Rate", f"{success_rate:.1f}%", delta=None)

    with col3:
        st.metric("Avg Execution Time", f"{avg_execution_time/60:.1f} min")

    with col4:
        st.metric("Avg Quality Score", f"{avg_quality:.2f}")

    with col5:
        most_used = max(platform_counts.items(), key=lambda x: x[1])[0] if platform_counts else 'N/A'
        st.metric("Most Used Platform", most_used.upper())


def render_execution_timeline(executions: List[Dict]):
    """Render execution timeline chart"""
    st.header("📅 Execution Timeline")

    if not executions:
        st.info("No execution data available")
        return

    # Prepare data
    df = pd.DataFrame([
        {
            'Timestamp': datetime.fromisoformat(e['timestamp'].replace('Z', '+00:00')) if e.get('timestamp') else datetime.now(),
            'Platform': e.get('platform', 'unknown').upper(),
            'Status': 'Success' if e.get('success') else 'Failed',
            'Execution Time (min)': (e.get('execution_time') or 0) / 60,
            'Quality Score': e.get('quality_score', 0) or 0,
            'Vector ID': e.get('vector_id', '')[:8]
        }
        for e in executions
    ])

    # Sort by timestamp
    df = df.sort_values('Timestamp')

    # Create timeline chart
    fig = px.scatter(
        df,
        x='Timestamp',
        y='Execution Time (min)',
        color='Platform',
        size='Quality Score',
        symbol='Status',
        hover_data=['Vector ID', 'Quality Score'],
        title='Pipeline Execution Timeline',
        height=400
    )

    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Execution Time (minutes)",
        legend_title="Platform"
    )

    st.plotly_chart(fig, use_container_width=True)


def render_platform_comparison(executions: List[Dict]):
    """Render platform performance comparison"""
    st.header("⚖️ Platform Performance Comparison")

    if not executions:
        st.info("No data available for comparison")
        return

    # Group by platform
    platform_data = {}
    for e in executions:
        platform = e.get('platform', 'unknown')
        if platform not in platform_data:
            platform_data[platform] = {
                'total': 0,
                'successful': 0,
                'times': [],
                'quality_scores': []
            }

        platform_data[platform]['total'] += 1
        if e.get('success'):
            platform_data[platform]['successful'] += 1

        if e.get('execution_time'):
            platform_data[platform]['times'].append(e['execution_time'] / 60)

        if e.get('quality_score'):
            platform_data[platform]['quality_scores'].append(e['quality_score'])

    # Create comparison dataframe
    comparison_data = []
    for platform, data in platform_data.items():
        comparison_data.append({
            'Platform': platform.upper(),
            'Total Runs': data['total'],
            'Success Rate (%)': (data['successful'] / data['total'] * 100) if data['total'] > 0 else 0,
            'Avg Time (min)': sum(data['times']) / len(data['times']) if data['times'] else 0,
            'Avg Quality': sum(data['quality_scores']) / len(data['quality_scores']) if data['quality_scores'] else 0
        })

    df = pd.DataFrame(comparison_data)

    col1, col2 = st.columns(2)

    with col1:
        # Success rate comparison
        fig1 = go.Figure(data=[
            go.Bar(
                x=df['Platform'],
                y=df['Success Rate (%)'],
                text=df['Success Rate (%)'].round(1),
                textposition='auto',
                marker_color=['#00cc96' if x > 90 else '#ffa15a' if x > 70 else '#ef553b' for x in df['Success Rate (%)']]
            )
        ])
        fig1.update_layout(
            title='Success Rate by Platform',
            yaxis_title='Success Rate (%)',
            xaxis_title='Platform',
            showlegend=False,
            height=300
        )
        st.plotly_chart(fig1, use_container_width=True)

    with col2:
        # Execution time comparison
        fig2 = go.Figure(data=[
            go.Bar(
                x=df['Platform'],
                y=df['Avg Time (min)'],
                text=df['Avg Time (min)'].round(1),
                textposition='auto',
                marker_color='#636efa'
            )
        ])
        fig2.update_layout(
            title='Average Execution Time',
            yaxis_title='Time (minutes)',
            xaxis_title='Platform',
            showlegend=False,
            height=300
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Display table
    st.dataframe(df, use_container_width=True)


def render_quality_trends(executions: List[Dict]):
    """Render quality score trends"""
    st.header("📈 Quality Score Trends")

    if not executions:
        st.info("No quality data available")
        return

    # Prepare data
    df = pd.DataFrame([
        {
            'Timestamp': datetime.fromisoformat(e['timestamp'].replace('Z', '+00:00')) if e.get('timestamp') else datetime.now(),
            'Quality Score': e.get('quality_score', 0) or 0,
            'Platform': e.get('platform', 'unknown').upper()
        }
        for e in executions if e.get('quality_score')
    ])

    df = df.sort_values('Timestamp')

    # Create line chart
    fig = px.line(
        df,
        x='Timestamp',
        y='Quality Score',
        color='Platform',
        title='Quality Score Over Time',
        height=400
    )

    # Add threshold line
    fig.add_hline(y=0.95, line_dash="dash", line_color="green", annotation_text="Target: 0.95")
    fig.add_hline(y=0.70, line_dash="dash", line_color="red", annotation_text="Critical: 0.70")

    fig.update_layout(
        yaxis_title="Quality Score",
        xaxis_title="Time",
        yaxis_range=[0, 1.1]
    )

    st.plotly_chart(fig, use_container_width=True)


def render_recent_executions_table(executions: List[Dict]):
    """Render table of recent executions"""
    st.header("📋 Recent Executions")

    if not executions:
        st.info("No executions found")
        return

    # Prepare table data
    table_data = []
    for e in executions[:20]:  # Show last 20
        timestamp = e.get('timestamp', '')
        if timestamp:
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                timestamp_display = dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                timestamp_display = timestamp
        else:
            timestamp_display = 'N/A'

        table_data.append({
            'Time': timestamp_display,
            'Platform': e.get('platform', 'unknown').upper(),
            'Status': '✅ Success' if e.get('success') else '❌ Failed',
            'Duration (min)': f"{(e.get('execution_time') or 0) / 60:.1f}",
            'Quality': f"{e.get('quality_score', 0):.2f}",
            'Confidence': f"{e.get('decision_confidence', 0):.2f}",
            'Vector ID': e.get('vector_id', '')[:12]
        })

    df = pd.DataFrame(table_data)

    # Color code status
    def highlight_status(row):
        if '✅' in row['Status']:
            return ['background-color: #d4edda'] * len(row)
        else:
            return ['background-color: #f8d7da'] * len(row)

    styled_df = df.style.apply(highlight_status, axis=1)

    st.dataframe(styled_df, use_container_width=True, height=400)


def render_optimization_insights(recommendations: List[Dict]):
    """Render optimization recommendations"""
    st.header("💡 Optimization Insights")

    if not recommendations:
        st.info("No optimization recommendations available")
        return

    # Get most recent recommendations
    recent = recommendations[:5]

    for i, rec in enumerate(recent):
        with st.expander(f"Recommendation {i+1} - {rec.get('execution_id', 'N/A')[:12]}"):
            col1, col2 = st.columns([2, 1])

            with col1:
                recs = rec.get('recommendations', [])
                if recs:
                    st.subheader("Priority Actions:")
                    for j, action in enumerate(recs[:3], 1):
                        priority = action.get('priority', 'medium')
                        icon = '🔴' if priority == 'high' else '🟡' if priority == 'medium' else '🟢'
                        st.write(f"{icon} **{action.get('title', 'Action')}**")
                        st.write(f"   {action.get('description', '')}")
                        st.write(f"   _Action:_ {action.get('action', '')}")
                        st.divider()

            with col2:
                improvement = rec.get('estimated_improvement', {})
                st.metric("Estimated Improvement", f"{improvement.get('estimated_improvement_pct', 0)}%")
                st.metric("Confidence", improvement.get('confidence', 'medium').title())

                performance = rec.get('performance_analysis', {})
                if performance:
                    status = performance.get('status', 'unknown')
                    if status == 'excellent':
                        st.success("Performance: Excellent")
                    elif status == 'good':
                        st.info("Performance: Good")
                    else:
                        st.warning("Performance: Needs Improvement")


def render_agent_console():
    """Interactive console for invoking supervisor agent"""
    st.header("🤖 Agent Console")

    st.write("Send requests directly to the Strands Supervisor Agent")

    user_request = st.text_area(
        "Enter your ETL request:",
        placeholder="Process customer order analytics with high data volume and quality checks",
        height=100
    )

    col1, col2 = st.columns([1, 4])

    with col1:
        if st.button("🚀 Execute", type="primary"):
            if not user_request:
                st.error("Please enter a request")
            else:
                execute_agent_request(user_request)

    with col2:
        st.caption("The supervisor agent will coordinate Decision, Quality, Optimization, and Learning agents")


def execute_agent_request(user_request: str):
    """Execute request via Bedrock Agent"""
    if not SUPERVISOR_AGENT_ID:
        st.error("Supervisor Agent ID not configured")
        return

    with st.spinner("Invoking Strands Supervisor Agent..."):
        try:
            session_id = f"dashboard-{datetime.now().strftime('%Y%m%d%H%M%S')}"

            response = clients['bedrock_agent_runtime'].invoke_agent(
                agentId=SUPERVISOR_AGENT_ID,
                agentAliasId=SUPERVISOR_ALIAS_ID,
                sessionId=session_id,
                inputText=user_request
            )

            # Stream response
            st.subheader("Agent Response:")

            response_text = ""
            for event in response['completion']:
                if 'chunk' in event:
                    chunk = event['chunk']['bytes'].decode('utf-8')
                    response_text += chunk

            st.write(response_text)

            # Show trace (if available)
            if 'trace' in response:
                with st.expander("View Agent Trace"):
                    st.json(response['trace'])

        except Exception as e:
            st.error(f"Error invoking agent: {e}")


# ====================
# Main Dashboard
# ====================

def main():
    # Title and header
    st.title("🔄 Strands ETL Dashboard")
    st.markdown("Real-time monitoring and analytics for Strands multi-agent ETL framework")

    # Sidebar
    with st.sidebar:
        st.image("https://via.placeholder.com/300x100/1e3a8a/ffffff?text=Strands+ETL", use_column_width=True)

        st.header("🔧 Controls")

        # Refresh data
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()

        # Time range filter
        st.subheader("Time Range")
        time_range = st.selectbox(
            "Show data from:",
            ["Last Hour", "Last 24 Hours", "Last 7 Days", "Last 30 Days", "All Time"]
        )

        # Platform filter
        st.subheader("Platform Filter")
        platform_filter = st.multiselect(
            "Platforms:",
            ["Glue", "EMR", "Lambda"],
            default=["Glue", "EMR", "Lambda"]
        )

        # Status filter
        st.subheader("Status Filter")
        show_success = st.checkbox("Show Successful", value=True)
        show_failed = st.checkbox("Show Failed", value=True)

        st.divider()

        st.caption("Last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # Load data
    executions = load_recent_executions(limit=100)
    quality_reports = load_quality_reports(limit=50)
    optimization_recs = load_optimization_recommendations(limit=50)

    # Apply filters
    if executions:
        filtered_executions = [
            e for e in executions
            if (show_success and e.get('success')) or (show_failed and not e.get('success'))
        ]

        filtered_executions = [
            e for e in filtered_executions
            if e.get('platform', '').lower() in [p.lower() for p in platform_filter]
        ]
    else:
        filtered_executions = []

    # Render components
    if filtered_executions:
        render_metrics_overview(filtered_executions)
        st.divider()

        render_execution_timeline(filtered_executions)
        st.divider()

        render_platform_comparison(filtered_executions)
        st.divider()

        render_quality_trends(filtered_executions)
        st.divider()

        render_recent_executions_table(filtered_executions)
        st.divider()

        render_optimization_insights(optimization_recs)
        st.divider()

    else:
        st.warning("No execution data found matching filters")

    # Agent console at bottom
    render_agent_console()


if __name__ == "__main__":
    main()
