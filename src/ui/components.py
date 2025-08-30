"""
Reusable Streamlit components for the healthcare pipeline dashboard
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class MetricsComponents:
    """Components for displaying pipeline metrics"""
    
    @staticmethod
    def render_health_indicator(health_score: float, label: str = "Health Score") -> None:
        """Render a health indicator with color coding"""
        
        if health_score >= 90:
            color = "green"
            icon = "🟢"
        elif health_score >= 70:
            color = "orange" 
            icon = "🟡"
        else:
            color = "red"
            icon = "🔴"
            
        st.markdown(f"""
        <div style="display: flex; align-items: center; gap: 10px;">
            <span style="font-size: 20px;">{icon}</span>
            <span style="font-size: 18px; font-weight: bold; color: {color};">
                {label}: {health_score:.1f}%
            </span>
        </div>
        """, unsafe_allow_html=True)
        
    @staticmethod
    def render_metric_cards(metrics: List[Dict[str, Any]]) -> None:
        """Render a row of metric cards"""
        
        cols = st.columns(len(metrics))
        
        for i, metric in enumerate(metrics):
            with cols[i]:
                st.metric(
                    label=metric.get("label", "Metric"),
                    value=metric.get("value", "N/A"),
                    delta=metric.get("delta"),
                    help=metric.get("help")
                )
                
    @staticmethod
    def render_trend_chart(
        data: pd.DataFrame,
        x_col: str,
        y_col: str,
        title: str,
        threshold_lines: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        """Render a trend chart with optional threshold lines"""
        
        fig = px.line(data, x=x_col, y=y_col, title=title)
        
        if threshold_lines:
            for threshold in threshold_lines:
                fig.add_hline(
                    y=threshold["value"],
                    line_dash=threshold.get("style", "dash"),
                    line_color=threshold.get("color", "red"),
                    annotation_text=threshold.get("label", "")
                )
                
        st.plotly_chart(fig, use_container_width=True)
        
    @staticmethod
    def render_quality_breakdown(quality_metrics: Dict[str, float]) -> None:
        """Render data quality breakdown"""
        
        metrics_df = pd.DataFrame([
            {"Metric": "Completeness", "Score": quality_metrics.get("completeness", 0)},
            {"Metric": "Validity", "Score": quality_metrics.get("validity", 0)},
            {"Metric": "Consistency", "Score": quality_metrics.get("consistency", 0)},
            {"Metric": "Accuracy", "Score": quality_metrics.get("accuracy", 0)}
        ])
        
        fig = px.bar(
            metrics_df,
            x="Metric",
            y="Score", 
            title="Data Quality Breakdown",
            color="Score",
            color_continuous_scale="RdYlGn",
            range_color=[0, 100]
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    @staticmethod
    def render_progress_bar(value: float, max_value: float = 100, label: str = "Progress") -> None:
        """Render a progress bar"""
        
        progress_pct = value / max_value
        st.write(f"**{label}**: {value:.1f}%")
        st.progress(progress_pct)


class AlertsComponents:
    """Components for displaying alerts and notifications"""
    
    @staticmethod
    def render_alert_summary(alert_counts: Dict[str, int]) -> None:
        """Render alert summary cards"""
        
        severity_config = {
            "critical": {"color": "red", "icon": "🔴"},
            "warning": {"color": "orange", "icon": "🟡"},
            "info": {"color": "blue", "icon": "🔵"}
        }
        
        cols = st.columns(len(alert_counts))
        
        for i, (severity, count) in enumerate(alert_counts.items()):
            with cols[i]:
                config = severity_config.get(severity, {"color": "gray", "icon": "⚪"})
                
                st.markdown(f"""
                <div style="text-align: center; padding: 10px; border-radius: 5px; border: 2px solid {config['color']};">
                    <div style="font-size: 24px;">{config['icon']}</div>
                    <div style="font-size: 20px; font-weight: bold;">{count}</div>
                    <div style="font-size: 14px; text-transform: capitalize;">{severity}</div>
                </div>
                """, unsafe_allow_html=True)
                
    @staticmethod
    def render_alert_list(alerts: List[Dict[str, Any]], max_display: int = 10) -> None:
        """Render a list of alerts"""
        
        severity_icons = {
            "critical": "🔴",
            "warning": "🟡", 
            "info": "🔵"
        }
        
        for i, alert in enumerate(alerts[:max_display]):
            with st.container():
                col1, col2, col3, col4 = st.columns([1, 4, 2, 2])
                
                with col1:
                    icon = severity_icons.get(alert.get("severity", "info"), "⚪")
                    st.write(icon)
                    
                with col2:
                    st.write(f"**{alert.get('type', 'Alert')}**")
                    st.write(alert.get('message', 'No message'))
                    
                with col3:
                    st.write(alert.get('timestamp', 'Unknown time'))
                    
                with col4:
                    status = alert.get('status', 'Active')
                    status_color = "🟢" if status == "Resolved" else "🟡" if status == "In Progress" else "🔴"
                    st.write(f"{status_color} {status}")
                    
                if i < len(alerts[:max_display]) - 1:
                    st.divider()
                    
    @staticmethod
    def render_alert_timeline(alerts_data: pd.DataFrame) -> None:
        """Render alerts timeline chart"""
        
        if alerts_data.empty:
            st.info("No alerts to display for the selected time range.")
            return
            
        fig = px.timeline(
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
        
        st.plotly_chart(fig, use_container_width=True)


class LineageComponents:
    """Components for displaying data lineage"""
    
    @staticmethod
    def render_lineage_flow(lineage_data: Dict[str, List[str]]) -> None:
        """Render data lineage flow diagram"""
        
        # Simple text-based lineage for now
        # In production, would use a proper graph visualization library
        
        layers = list(lineage_data.keys())
        cols = st.columns(len(layers))
        
        for i, layer in enumerate(layers):
            with cols[i]:
                st.subheader(layer)
                
                tables = lineage_data[layer]
                for table in tables:
                    table_name = table.split('.')[-1] if '.' in table else table
                    st.markdown(f"📊 **{table_name}**")
                    
                # Add arrows between columns (except for last column)
                if i < len(layers) - 1:
                    st.markdown("⬇️", unsafe_allow_html=True)
                    
    @staticmethod
    def render_table_details(table_name: str, table_info: Dict[str, Any]) -> None:
        """Render detailed information about a table"""
        
        with st.expander(f"Table Details: {table_name}", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Basic Information**")
                st.write(f"- Records: {table_info.get('record_count', 'N/A')}")
                st.write(f"- Size: {table_info.get('size_gb', 'N/A')} GB")
                st.write(f"- Last Updated: {table_info.get('last_updated', 'N/A')}")
                
            with col2:
                st.write("**Quality Metrics**")
                st.write(f"- Quality Score: {table_info.get('quality_score', 'N/A')}%")
                st.write(f"- Completeness: {table_info.get('completeness', 'N/A')}%")
                st.write(f"- Freshness: {table_info.get('freshness_hours', 'N/A')} hours")
                
    @staticmethod
    def render_dependency_graph(dependencies: List[Dict[str, str]]) -> None:
        """Render table dependency graph"""
        
        st.write("**Table Dependencies:**")
        
        for dep in dependencies:
            source = dep.get('source', 'Unknown')
            target = dep.get('target', 'Unknown')
            transform = dep.get('transformation', 'Unknown')
            
            st.write(f"📊 {source} ➡️ {target} ({transform})")


class CostComponents:
    """Components for cost analysis and optimization"""
    
    @staticmethod
    def render_cost_breakdown(cost_data: pd.DataFrame) -> None:
        """Render cost breakdown pie chart"""
        
        fig = px.pie(
            cost_data,
            values="cost",
            names="component",
            title="Cost Breakdown by Component"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    @staticmethod
    def render_cost_trend(cost_trend_data: pd.DataFrame) -> None:
        """Render cost trend over time"""
        
        fig = px.line(
            cost_trend_data,
            x="timestamp",
            y="cost",
            title="Cost Trend Over Time",
            labels={"cost": "Cost ($)", "timestamp": "Time"}
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    @staticmethod
    def render_optimization_recommendations(recommendations: List[Dict[str, str]]) -> None:
        """Render cost optimization recommendations"""
        
        st.subheader("💡 Cost Optimization Recommendations")
        
        for rec in recommendations:
            with st.container():
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.write(f"**{rec.get('title', 'Optimization')}**")
                    st.write(rec.get('description', 'No description'))
                    
                with col2:
                    savings = rec.get('potential_savings', '$0')
                    st.metric("Potential Savings", savings)
                    
                st.divider()
                
    @staticmethod
    def render_efficiency_score(efficiency_metrics: Dict[str, float]) -> None:
        """Render efficiency score gauge"""
        
        overall_score = efficiency_metrics.get('overall', 0)
        
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=overall_score,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Efficiency Score"},
            delta={'reference': 80},
            gauge={
                'axis': {'range': [None, 100]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [0, 50], 'color': "lightgray"},
                    {'range': [50, 80], 'color': "gray"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75, 
                    'value': 90
                }
            }
        ))
        
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)


class SelfHealingComponents:
    """Components for self-healing actions and recovery"""
    
    @staticmethod
    def render_healing_summary(healing_stats: Dict[str, Any]) -> None:
        """Render self-healing summary metrics"""
        
        metrics = [
            {"label": "Actions Today", "value": healing_stats.get("actions_today", 0), "delta": "+3"},
            {"label": "Success Rate", "value": f"{healing_stats.get('success_rate', 0):.1f}%", "delta": "+1.2%"},
            {"label": "Avg Resolution Time", "value": f"{healing_stats.get('avg_resolution_min', 0):.1f} min", "delta": "-0.5 min"},
            {"label": "Cost Savings", "value": f"${healing_stats.get('cost_savings', 0):,.0f}", "delta": f"+${healing_stats.get('delta_savings', 0):,.0f}"}
        ]
        
        MetricsComponents.render_metric_cards(metrics)
        
    @staticmethod
    def render_healing_actions_timeline(actions_data: pd.DataFrame) -> None:
        """Render timeline of self-healing actions"""
        
        if actions_data.empty:
            st.info("No self-healing actions in the selected time range.")
            return
            
        fig = px.bar(
            actions_data,
            x="timestamp",
            y="action_count", 
            color="action_type",
            title="Self-Healing Actions Timeline",
            labels={"action_count": "Number of Actions", "timestamp": "Time"}
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    @staticmethod
    def render_healing_actions_list(actions: List[Dict[str, str]], max_display: int = 5) -> None:
        """Render list of recent self-healing actions"""
        
        st.subheader("Recent Self-Healing Actions")
        
        for action in actions[:max_display]:
            with st.container():
                col1, col2, col3 = st.columns([1, 2, 1])
                
                with col1:
                    status_icon = "✅" if action.get("status") == "Success" else "❌" if action.get("status") == "Failed" else "🟡"
                    st.write(f"**{action.get('timestamp', 'Unknown time')}**")
                    st.write(f"{status_icon} {action.get('status', 'Unknown')}")
                    
                with col2:
                    st.write(f"**{action.get('action_type', 'Unknown Action')}**")
                    st.write(action.get('description', 'No description'))
                    st.write(f"*Impact: {action.get('impact', 'Unknown impact')}*")
                    
                with col3:
                    savings = action.get('cost_savings', '$0')
                    st.metric("Cost Savings", savings)
                    
                st.divider()
                
    @staticmethod
    def render_healing_success_rate(success_data: pd.DataFrame) -> None:
        """Render self-healing success rate over time"""
        
        fig = px.line(
            success_data,
            x="timestamp",
            y="success_rate",
            title="Self-Healing Success Rate Over Time",
            labels={"success_rate": "Success Rate (%)", "timestamp": "Time"}
        )
        
        # Add target line
        fig.add_hline(
            y=95,
            line_dash="dash",
            line_color="green",
            annotation_text="Target (95%)"
        )
        
        st.plotly_chart(fig, use_container_width=True)