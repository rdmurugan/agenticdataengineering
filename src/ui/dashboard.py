"""
Streamlit Dashboard for Healthcare Pipeline Monitoring
Lightweight dashboard showing pipeline health, anomalies, and self-healing actions
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import json
from typing import Dict, Any, List, Optional
import logging

from .components import MetricsComponents, AlertsComponents, LineageComponents

logger = logging.getLogger(__name__)


class HealthcarePipelineDashboard:
    """
    Main dashboard for healthcare data pipeline monitoring
    Displays real-time metrics, alerts, and self-healing status
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.metrics_component = MetricsComponents()
        self.alerts_component = AlertsComponents()
        self.lineage_component = LineageComponents()
        
        # Page configuration
        st.set_page_config(
            page_title="Healthcare Pipeline Monitor",
            page_icon="🏥",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
    def render_dashboard(self):
        """Render the main dashboard"""
        
        # Header
        self._render_header()
        
        # Sidebar
        selected_pipeline, time_range = self._render_sidebar()
        
        # Main content
        if selected_pipeline:
            self._render_main_content(selected_pipeline, time_range)
        else:
            self._render_welcome_screen()
            
    def _render_header(self):
        """Render dashboard header"""
        
        st.title("🏥 Healthcare Data Pipeline Monitor")
        st.markdown("""
        **Self-Healing Medicaid/Medicare Feed Processing**
        
        Monitor pipeline health, data quality, and automated recovery actions in real-time.
        """)
        
        # Status indicators
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="System Health",
                value="98.5%",
                delta="0.5%"
            )
            
        with col2:
            st.metric(
                label="Data Quality",
                value="96.2%",
                delta="-0.3%"
            )
            
        with col3:
            st.metric(
                label="Cost Optimization",
                value="12.3%",
                delta="2.1%"
            )
            
        with col4:
            st.metric(
                label="Auto-Fixes",
                value="47",
                delta="3"
            )
            
        st.divider()
        
    def _render_sidebar(self):
        """Render sidebar with controls"""
        
        st.sidebar.header("Pipeline Selection")
        
        # Pipeline selector
        pipelines = [
            "medicaid_claims_pipeline",
            "medicare_claims_pipeline", 
            "provider_data_pipeline",
            "quality_monitoring_pipeline"
        ]
        
        selected_pipeline = st.sidebar.selectbox(
            "Select Pipeline",
            options=pipelines,
            index=0
        )
        
        # Time range selector
        st.sidebar.subheader("Time Range")
        time_ranges = {
            "Last 1 hour": 1,
            "Last 6 hours": 6,
            "Last 24 hours": 24,
            "Last 7 days": 168,
            "Last 30 days": 720
        }
        
        selected_time_range = st.sidebar.selectbox(
            "Time Range",
            options=list(time_ranges.keys()),
            index=2  # Default to 24 hours
        )
        
        time_range_hours = time_ranges[selected_time_range]
        
        # Filters
        st.sidebar.subheader("Filters")
        
        show_alerts_only = st.sidebar.checkbox("Show Alerts Only", False)
        show_anomalies = st.sidebar.checkbox("Show Anomalies", True)
        show_self_healing = st.sidebar.checkbox("Show Self-Healing Actions", True)
        
        # Refresh button
        st.sidebar.subheader("Actions")
        if st.sidebar.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
            
        return selected_pipeline, {
            "hours": time_range_hours,
            "show_alerts_only": show_alerts_only,
            "show_anomalies": show_anomalies,
            "show_self_healing": show_self_healing
        }
        
    def _render_main_content(self, pipeline_name: str, time_range: Dict[str, Any]):
        """Render main dashboard content"""
        
        # Pipeline overview
        self._render_pipeline_overview(pipeline_name)
        
        # Tabs for different views
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📊 Metrics", "🚨 Alerts", "🔧 Self-Healing", "🔄 Data Lineage", "💰 Cost Analysis"
        ])
        
        with tab1:
            self._render_metrics_tab(pipeline_name, time_range)
            
        with tab2:
            self._render_alerts_tab(pipeline_name, time_range)
            
        with tab3:
            self._render_self_healing_tab(pipeline_name, time_range)
            
        with tab4:
            self._render_lineage_tab(pipeline_name)
            
        with tab5:
            self._render_cost_analysis_tab(pipeline_name, time_range)
            
    def _render_pipeline_overview(self, pipeline_name: str):
        """Render pipeline overview section"""
        
        st.subheader(f"Pipeline: {pipeline_name}")
        
        # Pipeline status
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            # Pipeline health indicator
            health_score = 94.2  # Would fetch from metrics
            health_color = "green" if health_score > 90 else "orange" if health_score > 70 else "red"
            
            st.markdown(f"""
            <div style="display: flex; align-items: center; gap: 10px;">
                <div style="width: 20px; height: 20px; border-radius: 50%; background-color: {health_color};"></div>
                <span style="font-size: 18px; font-weight: bold;">Health Score: {health_score}%</span>
            </div>
            """, unsafe_allow_html=True)
            
        with col2:
            st.metric("Last Run", "5 min ago", delta="On time")
            
        with col3:
            st.metric("Records/Hour", "125K", delta="5K")
            
        # Recent activity
        with st.expander("Recent Activity", expanded=False):
            recent_activities = [
                {"time": "5 min ago", "event": "Pipeline completed successfully", "type": "success"},
                {"time": "1 hour ago", "event": "Auto-scaled cluster down (cost optimization)", "type": "info"},
                {"time": "3 hours ago", "event": "Schema drift detected and resolved", "type": "warning"},
                {"time": "6 hours ago", "event": "Quality alert resolved automatically", "type": "warning"}
            ]
            
            for activity in recent_activities:
                icon = "✅" if activity["type"] == "success" else "ℹ️" if activity["type"] == "info" else "⚠️"
                st.write(f"{icon} {activity['time']}: {activity['event']}")
                
    def _render_metrics_tab(self, pipeline_name: str, time_range: Dict[str, Any]):
        """Render metrics tab"""
        
        st.subheader("📊 Pipeline Metrics")
        
        # Data quality metrics over time
        quality_data = self._get_mock_quality_data(time_range["hours"])
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Quality score trend
            fig_quality = px.line(
                quality_data, 
                x="timestamp", 
                y="quality_score",
                title="Data Quality Score Over Time",
                labels={"quality_score": "Quality Score (%)", "timestamp": "Time"}
            )
            fig_quality.add_hline(y=90, line_dash="dash", line_color="green", 
                                 annotation_text="Target (90%)")
            fig_quality.add_hline(y=70, line_dash="dash", line_color="orange", 
                                 annotation_text="Warning (70%)")
            st.plotly_chart(fig_quality, use_container_width=True)
            
        with col2:
            # Throughput metrics
            throughput_data = self._get_mock_throughput_data(time_range["hours"])
            
            fig_throughput = px.area(
                throughput_data,
                x="timestamp",
                y="records_per_hour",
                title="Processing Throughput",
                labels={"records_per_hour": "Records/Hour", "timestamp": "Time"}
            )
            st.plotly_chart(fig_throughput, use_container_width=True)
            
        # Anomaly detection
        st.subheader("🔍 Anomaly Detection")
        
        anomaly_data = self._get_mock_anomaly_data(time_range["hours"])
        
        fig_anomaly = px.scatter(
            anomaly_data,
            x="timestamp", 
            y="anomaly_score",
            color="is_anomaly",
            size="claim_amount",
            title="Anomaly Detection in Claims Data",
            labels={"anomaly_score": "Anomaly Score", "timestamp": "Time"},
            color_discrete_map={True: "red", False: "blue"}
        )
        fig_anomaly.add_hline(y=0.7, line_dash="dash", line_color="red",
                             annotation_text="Anomaly Threshold")
        st.plotly_chart(fig_anomaly, use_container_width=True)
        
        # Detailed metrics table
        with st.expander("Detailed Metrics", expanded=False):
            metrics_df = pd.DataFrame([
                {"Metric": "Total Records Processed", "Value": "2,450,123", "Change": "+2.1%"},
                {"Metric": "Success Rate", "Value": "98.7%", "Change": "+0.3%"},
                {"Metric": "Average Processing Time", "Value": "1.2 sec", "Change": "-0.1 sec"},
                {"Metric": "Memory Utilization", "Value": "67%", "Change": "-5%"},
                {"Metric": "CPU Utilization", "Value": "72%", "Change": "+3%"},
                {"Metric": "Network I/O", "Value": "150 MB/s", "Change": "+10 MB/s"}
            ])
            st.dataframe(metrics_df, use_container_width=True)
            
    def _render_alerts_tab(self, pipeline_name: str, time_range: Dict[str, Any]):
        """Render alerts tab"""
        
        st.subheader("🚨 Active Alerts & Issues")
        
        # Alert summary
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Active Alerts", "3", delta="-2")
        with col2:
            st.metric("Critical", "0", delta="0")
        with col3:
            st.metric("Warning", "2", delta="-1")
        with col4:
            st.metric("Info", "1", delta="-1")
            
        # Alerts timeline
        alerts_data = self._get_mock_alerts_data(time_range["hours"])
        
        if not alerts_data.empty:
            fig_alerts = px.timeline(
                alerts_data,
                x_start="start_time",
                x_end="end_time",
                y="alert_type",
                color="severity",
                title="Alerts Timeline",
                color_discrete_map={
                    "critical": "red",
                    "warning": "orange", 
                    "info": "blue"
                }
            )
            st.plotly_chart(fig_alerts, use_container_width=True)
        else:
            st.success("No alerts in the selected time range! 🎉")
            
        # Active alerts list
        st.subheader("Active Alerts")
        
        active_alerts = [
            {
                "severity": "warning",
                "type": "Data Quality",
                "message": "Member ID validation rate below threshold (87%)",
                "table": "medicaid_claims.silver",
                "time": "2 hours ago",
                "auto_fix": "In Progress"
            },
            {
                "severity": "warning", 
                "type": "Performance",
                "message": "Processing latency increased by 15%",
                "table": "medicare_claims.bronze",
                "time": "4 hours ago",
                "auto_fix": "Scheduled"
            },
            {
                "severity": "info",
                "type": "Cost Optimization",
                "message": "Cluster underutilized, scaling down recommended",
                "table": "N/A",
                "time": "1 hour ago",
                "auto_fix": "Completed"
            }
        ]
        
        for alert in active_alerts:
            severity_color = {"critical": "🔴", "warning": "🟡", "info": "🔵"}[alert["severity"]]
            
            with st.container():
                col1, col2, col3, col4 = st.columns([1, 3, 1, 1])
                
                with col1:
                    st.write(f"{severity_color} {alert['type']}")
                with col2:
                    st.write(alert["message"])
                with col3:
                    st.write(alert["time"])
                with col4:
                    status_color = "🟢" if alert["auto_fix"] == "Completed" else "🟡"
                    st.write(f"{status_color} {alert['auto_fix']}")
                    
                st.divider()
                
    def _render_self_healing_tab(self, pipeline_name: str, time_range: Dict[str, Any]):
        """Render self-healing tab"""
        
        st.subheader("🔧 Self-Healing Actions")
        
        # Self-healing summary
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Actions Today", "12", delta="+3")
        with col2:
            st.metric("Success Rate", "94.2%", delta="+1.5%")
        with col3:
            st.metric("Avg Resolution Time", "4.2 min", delta="-0.8 min")
        with col4:
            st.metric("Cost Savings", "$1,247", delta="+$156")
            
        # Self-healing actions timeline
        self_healing_data = self._get_mock_self_healing_data(time_range["hours"])
        
        fig_healing = px.bar(
            self_healing_data,
            x="hour",
            y="actions_count",
            color="action_type",
            title="Self-Healing Actions Over Time",
            labels={"actions_count": "Number of Actions", "hour": "Hour"}
        )
        st.plotly_chart(fig_healing, use_container_width=True)
        
        # Recent self-healing actions
        st.subheader("Recent Self-Healing Actions")
        
        healing_actions = [
            {
                "time": "10 min ago",
                "action": "Schema Drift Resolution",
                "description": "Auto-adapted pipeline for new 'patient_risk_score' field",
                "status": "✅ Success",
                "impact": "Zero downtime",
                "savings": "$45"
            },
            {
                "time": "1 hour ago", 
                "action": "Cluster Auto-Scaling",
                "description": "Scaled down from 8 to 5 workers due to low utilization",
                "status": "✅ Success", 
                "impact": "30% cost reduction",
                "savings": "$78"
            },
            {
                "time": "3 hours ago",
                "action": "Data Quality Repair",
                "description": "Auto-corrected 1,247 records with invalid NPI formats",
                "status": "✅ Success",
                "impact": "Quality score +3.2%",
                "savings": "$23"
            },
            {
                "time": "6 hours ago",
                "action": "Retry with Backoff",
                "description": "Automatically retried failed ingestion job (network timeout)",
                "status": "✅ Success",
                "impact": "100% data recovery",
                "savings": "$156"
            }
        ]
        
        for action in healing_actions:
            with st.container():
                col1, col2, col3 = st.columns([1, 2, 1])
                
                with col1:
                    st.write(f"**{action['time']}**")
                    st.write(action["status"])
                    
                with col2:
                    st.write(f"**{action['action']}**")
                    st.write(action["description"])
                    st.write(f"*Impact: {action['impact']}*")
                    
                with col3:
                    st.metric("Cost Savings", action["savings"])
                    
                st.divider()
                
    def _render_lineage_tab(self, pipeline_name: str):
        """Render data lineage tab"""
        
        st.subheader("🔄 Data Lineage")
        
        # Lineage visualization would go here
        # For now, show a text-based representation
        
        lineage_data = {
            "Raw Layer": [
                "healthcare_data.raw.claims_raw",
                "healthcare_data.raw.members_raw",
                "healthcare_data.raw.providers_raw"
            ],
            "Bronze Layer": [
                "healthcare_data.bronze.claims_bronze",
                "healthcare_data.bronze.members_bronze", 
                "healthcare_data.bronze.providers_bronze"
            ],
            "Silver Layer": [
                "healthcare_data.silver.claims_silver",
                "healthcare_data.silver.members_silver",
                "healthcare_data.silver.providers_silver"
            ],
            "Gold Layer": [
                "healthcare_data.gold.claims_analytics",
                "healthcare_data.gold.provider_performance",
                "healthcare_data.gold.member_utilization"
            ]
        }
        
        col1, col2, col3, col4 = st.columns(4)
        
        for i, (layer, tables) in enumerate(lineage_data.items()):
            with [col1, col2, col3, col4][i]:
                st.subheader(layer)
                for table in tables:
                    table_name = table.split('.')[-1]
                    st.write(f"📊 {table_name}")
                    
        # Data quality by layer
        st.subheader("Data Quality by Layer")
        
        quality_by_layer = pd.DataFrame([
            {"Layer": "Raw", "Quality Score": 45.2, "Records": "2.5M"},
            {"Layer": "Bronze", "Quality Score": 78.5, "Records": "2.4M"},
            {"Layer": "Silver", "Quality Score": 94.1, "Records": "2.3M"},
            {"Layer": "Gold", "Quality Score": 98.7, "Records": "2.3M"}
        ])
        
        fig_quality_layer = px.bar(
            quality_by_layer,
            x="Layer",
            y="Quality Score",
            title="Data Quality Score by Layer",
            text="Records"
        )
        fig_quality_layer.update_traces(textposition="outside")
        st.plotly_chart(fig_quality_layer, use_container_width=True)
        
    def _render_cost_analysis_tab(self, pipeline_name: str, time_range: Dict[str, Any]):
        """Render cost analysis tab"""
        
        st.subheader("💰 Cost Analysis")
        
        # Cost summary
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Daily Cost", "$456", delta="-$67 (-12.8%)")
        with col2:
            st.metric("Monthly Projection", "$13,680", delta="-$2,010")
        with col3:
            st.metric("Cost per GB", "$0.045", delta="-$0.008")
        with col4:
            st.metric("Efficiency Score", "87.3%", delta="+5.2%")
            
        # Cost breakdown
        st.subheader("Cost Breakdown")
        
        cost_data = pd.DataFrame([
            {"Component": "Compute", "Cost": 278, "Percentage": 60.9},
            {"Component": "Storage", "Cost": 89, "Percentage": 19.5},
            {"Component": "Data Transfer", "Cost": 45, "Percentage": 9.9},
            {"Component": "Monitoring", "Cost": 34, "Percentage": 7.5},
            {"Component": "Other", "Cost": 10, "Percentage": 2.2}
        ])
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_pie = px.pie(
                cost_data,
                values="Cost",
                names="Component",
                title="Cost Distribution"
            )
            st.plotly_chart(fig_pie, use_container_width=True)
            
        with col2:
            # Cost optimization opportunities
            st.subheader("Optimization Opportunities")
            
            optimizations = [
                {"opportunity": "Increase spot instance usage", "savings": "$156/month"},
                {"opportunity": "Enable auto-scaling", "savings": "$234/month"},
                {"opportunity": "Optimize data partitioning", "savings": "$78/month"},
                {"opportunity": "Compress old data", "savings": "$45/month"}
            ]
            
            for opt in optimizations:
                st.write(f"💡 {opt['opportunity']}")
                st.write(f"   💰 Potential savings: {opt['savings']}")
                
        # Cost trend over time
        cost_trend_data = self._get_mock_cost_data(time_range["hours"])
        
        fig_cost = px.line(
            cost_trend_data,
            x="timestamp",
            y="hourly_cost",
            title="Cost Trend Over Time",
            labels={"hourly_cost": "Hourly Cost ($)", "timestamp": "Time"}
        )
        st.plotly_chart(fig_cost, use_container_width=True)
        
    def _render_welcome_screen(self):
        """Render welcome screen when no pipeline is selected"""
        
        st.markdown("""
        ## Welcome to Healthcare Pipeline Monitor
        
        Select a pipeline from the sidebar to start monitoring your healthcare data processing workflows.
        
        ### Key Features:
        - 📊 **Real-time Metrics**: Monitor data quality, throughput, and performance
        - 🚨 **Intelligent Alerts**: Get notified of issues before they impact operations
        - 🔧 **Self-Healing**: Automated recovery from common failures
        - 🔄 **Data Lineage**: Track data flow from raw to analytics-ready
        - 💰 **Cost Optimization**: Monitor and optimize cloud costs
        
        ### Recent Achievements:
        - ✅ 40% reduction in manual pipeline fixes
        - ✅ 15% improvement in cost efficiency
        - ✅ 99.2% uptime across all pipelines
        - ✅ Sub-5 minute issue resolution time
        """)
        
    def _get_mock_quality_data(self, hours: int) -> pd.DataFrame:
        """Generate mock quality data for demonstration"""
        
        import numpy as np
        
        timestamps = pd.date_range(
            start=datetime.now() - timedelta(hours=hours),
            end=datetime.now(),
            freq="H"
        )
        
        # Simulate quality score with some variation
        base_score = 92
        scores = base_score + np.random.normal(0, 3, len(timestamps))
        scores = np.clip(scores, 70, 100)  # Keep within reasonable range
        
        return pd.DataFrame({
            "timestamp": timestamps,
            "quality_score": scores
        })
        
    def _get_mock_throughput_data(self, hours: int) -> pd.DataFrame:
        """Generate mock throughput data"""
        
        import numpy as np
        
        timestamps = pd.date_range(
            start=datetime.now() - timedelta(hours=hours),
            end=datetime.now(),
            freq="H"
        )
        
        # Simulate throughput with daily patterns
        base_throughput = 125000
        hour_of_day = timestamps.hour
        
        # Higher throughput during business hours
        daily_pattern = 0.5 + 0.5 * np.sin(2 * np.pi * (hour_of_day - 6) / 24)
        throughput = base_throughput * daily_pattern + np.random.normal(0, 10000, len(timestamps))
        throughput = np.clip(throughput, 20000, 200000)
        
        return pd.DataFrame({
            "timestamp": timestamps,
            "records_per_hour": throughput
        })
        
    def _get_mock_anomaly_data(self, hours: int) -> pd.DataFrame:
        """Generate mock anomaly data"""
        
        import numpy as np
        
        # Generate fewer data points for anomaly detection (every 10 minutes)
        timestamps = pd.date_range(
            start=datetime.now() - timedelta(hours=hours),
            end=datetime.now(),
            freq="10min"
        )
        
        anomaly_scores = np.random.beta(2, 8, len(timestamps))  # Most scores low, few high
        claim_amounts = np.random.lognormal(8, 1.5, len(timestamps))  # Realistic claim distribution
        is_anomaly = anomaly_scores > 0.7
        
        return pd.DataFrame({
            "timestamp": timestamps,
            "anomaly_score": anomaly_scores,
            "claim_amount": claim_amounts,
            "is_anomaly": is_anomaly
        })
        
    def _get_mock_alerts_data(self, hours: int) -> pd.DataFrame:
        """Generate mock alerts data"""
        
        # Generate a few alerts over the time period
        alerts = []
        
        if hours >= 6:
            alerts.append({
                "alert_type": "Quality Alert",
                "severity": "warning",
                "start_time": datetime.now() - timedelta(hours=4),
                "end_time": datetime.now() - timedelta(hours=2),
                "message": "NPI validation rate below threshold"
            })
            
        if hours >= 12:
            alerts.append({
                "alert_type": "Performance Alert", 
                "severity": "info",
                "start_time": datetime.now() - timedelta(hours=8),
                "end_time": datetime.now() - timedelta(hours=7),
                "message": "Processing latency spike"
            })
            
        return pd.DataFrame(alerts)
        
    def _get_mock_self_healing_data(self, hours: int) -> pd.DataFrame:
        """Generate mock self-healing data"""
        
        import numpy as np
        
        hours_range = list(range(min(hours, 24)))  # Max 24 hours for display
        
        # Different types of self-healing actions
        action_types = ["Schema Drift", "Auto Scaling", "Retry Logic", "Quality Repair"]
        
        data = []
        for hour in hours_range:
            for action_type in action_types:
                count = max(0, int(np.random.poisson(0.5)))  # Low frequency events
                if count > 0:
                    data.append({
                        "hour": f"{hour}h ago",
                        "action_type": action_type,
                        "actions_count": count
                    })
                    
        return pd.DataFrame(data)
        
    def _get_mock_cost_data(self, hours: int) -> pd.DataFrame:
        """Generate mock cost data"""
        
        import numpy as np
        
        timestamps = pd.date_range(
            start=datetime.now() - timedelta(hours=hours),
            end=datetime.now(),
            freq="H"
        )
        
        # Simulate cost with slight variations
        base_cost = 19  # $19/hour average
        costs = base_cost + np.random.normal(0, 2, len(timestamps))
        costs = np.clip(costs, 10, 30)  # Keep within reasonable range
        
        return pd.DataFrame({
            "timestamp": timestamps,
            "hourly_cost": costs
        })


# Main execution
if __name__ == "__main__":
    config = {
        "databricks_host": "your-databricks-workspace",
        "catalog_name": "healthcare_data"
    }
    
    dashboard = HealthcarePipelineDashboard(config)
    dashboard.render_dashboard()