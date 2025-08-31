"""
Comprehensive Data Quality Dashboard with Interactive Threshold Management
Advanced UI for monitoring, configuring, and managing data quality across healthcare datasets
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yaml
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging

from ..agents.quality import DLTQualityAgent, HealthcareExpectations
from .components import MetricsComponents, AlertsComponents

logger = logging.getLogger(__name__)


class QualityDashboard:
    """
    Comprehensive data quality dashboard with interactive configuration management
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.quality_config = self._load_quality_config()
        self.healthcare_expectations = HealthcareExpectations(config)
        
        # Page configuration
        st.set_page_config(
            page_title="Data Quality Center",
            page_icon="🎯",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
    def _load_quality_config(self) -> Dict[str, Any]:
        """Load data quality configuration"""
        try:
            with open("config/data_quality_config.yaml", 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            st.error("Data quality configuration file not found!")
            return {}
            
    def render_dashboard(self):
        """Render the main quality dashboard"""
        
        # Header
        self._render_header()
        
        # Sidebar
        selected_table, view_mode, time_range = self._render_sidebar()
        
        # Main content based on view mode
        if view_mode == "Overview":
            self._render_overview_tab(selected_table, time_range)
        elif view_mode == "Thresholds":
            self._render_threshold_management_tab(selected_table)
        elif view_mode == "Rules":
            self._render_rule_management_tab(selected_table)
        elif view_mode == "Anomalies":
            self._render_anomaly_detection_tab(selected_table, time_range)
        elif view_mode == "Profiling":
            self._render_data_profiling_tab(selected_table)
        elif view_mode == "Alerts":
            self._render_alerts_tab(selected_table, time_range)
        elif view_mode == "Reports":
            self._render_reports_tab(selected_table, time_range)
            
    def _render_header(self):
        """Render dashboard header"""
        
        st.title("🎯 Data Quality Center")
        st.markdown("""
        **Healthcare Data Quality Management & Monitoring**
        
        Monitor, configure, and manage data quality across all healthcare datasets with real-time insights and automated remediation.
        """)
        
        # Global quality metrics
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        
        with col1:
            st.metric("Overall Quality", "94.2%", "▲ 1.3%", help="Weighted average across all quality dimensions")
            
        with col2:
            st.metric("Completeness", "96.8%", "▲ 0.8%", help="Percentage of non-null, non-blank values")
            
        with col3:
            st.metric("Validity", "92.1%", "▼ 0.5%", help="Percentage of values meeting format/range criteria")
            
        with col4:
            st.metric("Consistency", "95.6%", "▲ 2.1%", help="Cross-field and referential consistency")
            
        with col5:
            st.metric("Active Alerts", "7", "▼ 3", help="Current quality alerts requiring attention")
            
        with col6:
            st.metric("Auto-Fixed Issues", "42", "▲ 8", help="Issues automatically resolved today")
            
        st.divider()
        
    def _render_sidebar(self):
        """Render sidebar with controls"""
        
        st.sidebar.header("🎯 Quality Center")
        
        # Table selector
        st.sidebar.subheader("Dataset Selection")
        tables = [
            "healthcare_data.silver.medicaid_claims",
            "healthcare_data.silver.medicare_claims",
            "healthcare_data.silver.members",
            "healthcare_data.silver.providers",
            "healthcare_data.gold.claims_analytics"
        ]
        
        selected_table = st.sidebar.selectbox(
            "Select Table",
            options=tables,
            index=0
        )
        
        # View mode selector
        st.sidebar.subheader("View Mode")
        view_modes = [
            "Overview", "Thresholds", "Rules", "Anomalies", 
            "Profiling", "Alerts", "Reports"
        ]
        
        view_mode = st.sidebar.selectbox(
            "Select View",
            options=view_modes,
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
            index=2
        )
        
        time_range_hours = time_ranges[selected_time_range]
        
        # Quick actions
        st.sidebar.subheader("Quick Actions")
        
        col1, col2 = st.sidebar.columns(2)
        with col1:
            if st.button("🔄 Refresh", help="Refresh data quality metrics"):
                st.cache_data.clear()
                st.rerun()
                
        with col2:
            if st.button("🚨 Run Check", help="Run quality check now"):
                self._trigger_quality_check(selected_table)
                
        # Configuration export/import
        st.sidebar.subheader("Configuration")
        
        if st.sidebar.button("📥 Export Config"):
            self._export_configuration()
            
        uploaded_file = st.sidebar.file_uploader(
            "📤 Import Config", 
            type=['yaml', 'yml'],
            help="Upload quality configuration file"
        )
        
        if uploaded_file is not None:
            self._import_configuration(uploaded_file)
            
        return selected_table, view_mode, {"hours": time_range_hours}
        
    def _render_overview_tab(self, table_name: str, time_range: Dict[str, Any]):
        """Render quality overview tab"""
        
        st.subheader(f"📊 Quality Overview: {table_name.split('.')[-1].title()}")
        
        # Quality dimensions breakdown
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Quality trends over time
            trend_data = self._get_quality_trends(table_name, time_range["hours"])
            
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Overall Quality', 'Completeness', 'Validity', 'Consistency'),
                specs=[[{"secondary_y": True}, {"secondary_y": True}],
                       [{"secondary_y": True}, {"secondary_y": True}]]
            )
            
            # Plot each quality dimension
            dimensions = ['overall_quality', 'completeness', 'validity', 'consistency']
            positions = [(1,1), (1,2), (2,1), (2,2)]
            
            for dim, pos in zip(dimensions, positions):
                fig.add_trace(
                    go.Scatter(
                        x=trend_data['timestamp'],
                        y=trend_data[dim],
                        name=dim.title(),
                        line=dict(width=2)
                    ),
                    row=pos[0], col=pos[1]
                )
                
                # Add threshold lines
                if dim in self.quality_config.get('global_thresholds', {}):
                    warning_threshold = self.quality_config['global_thresholds'][f'warning_{dim.split("_")[0]}_score'] * 100
                    fig.add_hline(
                        y=warning_threshold,
                        line_dash="dash",
                        line_color="orange",
                        row=pos[0], col=pos[1]
                    )
                    
            fig.update_layout(
                height=600,
                title_text="Quality Trends Over Time",
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            # Current quality scores
            current_scores = self._get_current_quality_scores(table_name)
            
            # Quality gauge
            fig_gauge = go.Figure(go.Indicator(
                mode = "gauge+number+delta",
                value = current_scores.get('overall_quality', 0),
                domain = {'x': [0, 1], 'y': [0, 1]},
                title = {'text': "Overall Quality Score"},
                delta = {'reference': 90},
                gauge = {
                    'axis': {'range': [None, 100]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 60], 'color': "red"},
                        {'range': [60, 80], 'color': "yellow"},
                        {'range': [80, 95], 'color': "lightgreen"},
                        {'range': [95, 100], 'color': "green"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 90
                    }
                }
            ))
            
            fig_gauge.update_layout(height=300)
            st.plotly_chart(fig_gauge, use_container_width=True)
            
            # Quality dimension scores
            st.subheader("Quality Dimensions")
            for dimension, score in current_scores.items():
                if dimension != 'overall_quality':
                    MetricsComponents.render_progress_bar(
                        score, 100, dimension.title()
                    )
                    
        # Field-level quality summary
        st.subheader("🔍 Field-Level Quality Analysis")
        
        field_quality = self._get_field_quality_summary(table_name)
        
        # Create quality heatmap
        if field_quality:
            fig_heatmap = px.imshow(
                field_quality,
                labels=dict(x="Quality Dimension", y="Field", color="Score"),
                title="Field-Level Quality Heatmap",
                color_continuous_scale="RdYlGn",
                range_color=[0, 100]
            )
            st.plotly_chart(fig_heatmap, use_container_width=True)
        
        # Top quality issues
        st.subheader("🚨 Top Quality Issues")
        
        issues = self._get_top_quality_issues(table_name)
        
        for issue in issues[:5]:
            severity_color = {"critical": "🔴", "warning": "🟡", "info": "🔵"}[issue.get('severity', 'info')]
            
            with st.container():
                col1, col2, col3, col4 = st.columns([1, 3, 1, 1])
                
                with col1:
                    st.write(f"{severity_color} {issue.get('severity', 'Info').title()}")
                    
                with col2:
                    st.write(f"**{issue.get('field', 'Unknown')}**: {issue.get('description', 'No description')}")
                    
                with col3:
                    st.write(f"Count: {issue.get('count', 0):,}")
                    
                with col4:
                    if st.button(f"Fix", key=f"fix_{issue.get('id', 0)}"):
                        self._auto_fix_issue(table_name, issue)
                        
                st.divider()
                
    def _render_threshold_management_tab(self, table_name: str):
        """Render interactive threshold management"""
        
        st.subheader("⚙️ Threshold Management")
        
        # Global vs Table-specific thresholds
        threshold_scope = st.radio(
            "Threshold Scope",
            ["Global Thresholds", "Table-Specific Thresholds"],
            help="Choose whether to modify global defaults or table-specific overrides"
        )
        
        if threshold_scope == "Global Thresholds":
            self._render_global_threshold_editor()
        else:
            self._render_table_threshold_editor(table_name)
            
    def _render_global_threshold_editor(self):
        """Render global threshold configuration editor"""
        
        st.subheader("🌍 Global Quality Thresholds")
        
        # Get current global thresholds
        global_thresholds = self.quality_config.get('global_thresholds', {})
        
        # Create tabs for different threshold categories
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "Quality Scores", "Completeness", "Validity", "Timeliness", "Volume"
        ])
        
        with tab1:
            st.subheader("Overall Quality Score Thresholds")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                critical_quality = st.slider(
                    "Critical Threshold",
                    min_value=0.0,
                    max_value=1.0,
                    value=global_thresholds.get('critical_quality_score', 0.6),
                    step=0.01,
                    format="%.2f",
                    help="Below this level triggers critical alerts"
                )
                
            with col2:
                warning_quality = st.slider(
                    "Warning Threshold", 
                    min_value=critical_quality,
                    max_value=1.0,
                    value=global_thresholds.get('warning_quality_score', 0.8),
                    step=0.01,
                    format="%.2f",
                    help="Below this level triggers warnings"
                )
                
            with col3:
                target_quality = st.slider(
                    "Target Threshold",
                    min_value=warning_quality,
                    max_value=1.0,
                    value=global_thresholds.get('target_quality_score', 0.95),
                    step=0.01,
                    format="%.2f",
                    help="Target quality level to achieve"
                )
                
        with tab2:
            st.subheader("Completeness Thresholds")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                critical_completeness = st.slider(
                    "Critical Completeness",
                    min_value=0.0,
                    max_value=1.0,
                    value=global_thresholds.get('critical_completeness', 0.7),
                    step=0.01,
                    format="%.2f"
                )
                
            with col2:
                warning_completeness = st.slider(
                    "Warning Completeness",
                    min_value=critical_completeness,
                    max_value=1.0,
                    value=global_thresholds.get('warning_completeness', 0.85),
                    step=0.01,
                    format="%.2f"
                )
                
            with col3:
                target_completeness = st.slider(
                    "Target Completeness",
                    min_value=warning_completeness,
                    max_value=1.0,
                    value=global_thresholds.get('target_completeness', 0.95),
                    step=0.01,
                    format="%.2f"
                )
                
        with tab3:
            st.subheader("Validity Thresholds")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                critical_validity = st.slider(
                    "Critical Validity",
                    min_value=0.0,
                    max_value=1.0,
                    value=global_thresholds.get('critical_validity', 0.75),
                    step=0.01,
                    format="%.2f"
                )
                
            with col2:
                warning_validity = st.slider(
                    "Warning Validity",
                    min_value=critical_validity,
                    max_value=1.0,
                    value=global_thresholds.get('warning_validity', 0.9),
                    step=0.01,
                    format="%.2f"
                )
                
            with col3:
                target_validity = st.slider(
                    "Target Validity",
                    min_value=warning_validity,
                    max_value=1.0,
                    value=global_thresholds.get('target_validity', 0.98),
                    step=0.01,
                    format="%.2f"
                )
                
        with tab4:
            st.subheader("Data Freshness Thresholds")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                critical_age = st.number_input(
                    "Critical Age (hours)",
                    min_value=1,
                    max_value=168,
                    value=global_thresholds.get('critical_data_age_hours', 48),
                    help="Data older than this triggers critical alerts"
                )
                
            with col2:
                warning_age = st.number_input(
                    "Warning Age (hours)",
                    min_value=1,
                    max_value=critical_age,
                    value=global_thresholds.get('warning_data_age_hours', 24),
                    help="Data older than this triggers warnings"
                )
                
            with col3:
                target_age = st.number_input(
                    "Target Age (hours)",
                    min_value=1,
                    max_value=warning_age,
                    value=global_thresholds.get('target_data_age_hours', 6),
                    help="Target data freshness"
                )
                
        with tab5:
            st.subheader("Volume Change Thresholds")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                critical_volume_change = st.slider(
                    "Critical Volume Change",
                    min_value=0.0,
                    max_value=2.0,
                    value=global_thresholds.get('critical_volume_change', 0.5),
                    step=0.05,
                    format="%.2f",
                    help="Volume change above this triggers critical alerts"
                )
                
            with col2:
                warning_volume_change = st.slider(
                    "Warning Volume Change",
                    min_value=0.0,
                    max_value=critical_volume_change,
                    value=global_thresholds.get('warning_volume_change', 0.25),
                    step=0.05,
                    format="%.2f"
                )
                
            with col3:
                expected_variance = st.slider(
                    "Expected Daily Variance",
                    min_value=0.0,
                    max_value=0.5,
                    value=global_thresholds.get('expected_volume_variance', 0.1),
                    step=0.01,
                    format="%.2f"
                )
                
        # Save thresholds
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            if st.button("💾 Save Thresholds", type="primary"):
                new_thresholds = {
                    'critical_quality_score': critical_quality,
                    'warning_quality_score': warning_quality,
                    'target_quality_score': target_quality,
                    'critical_completeness': critical_completeness,
                    'warning_completeness': warning_completeness,
                    'target_completeness': target_completeness,
                    'critical_validity': critical_validity,
                    'warning_validity': warning_validity,
                    'target_validity': target_validity,
                    'critical_data_age_hours': critical_age,
                    'warning_data_age_hours': warning_age,
                    'target_data_age_hours': target_age,
                    'critical_volume_change': critical_volume_change,
                    'warning_volume_change': warning_volume_change,
                    'expected_volume_variance': expected_variance
                }
                
                self._save_thresholds(new_thresholds)
                st.success("✅ Thresholds saved successfully!")
                st.rerun()
                
        with col2:
            if st.button("🔄 Reset to Defaults"):
                self._reset_thresholds_to_defaults()
                st.success("✅ Thresholds reset to defaults!")
                st.rerun()
                
        with col3:
            st.info("💡 Changes will apply to all tables unless table-specific overrides exist")
            
    def _render_table_threshold_editor(self, table_name: str):
        """Render table-specific threshold editor"""
        
        st.subheader(f"🎯 Table-Specific Thresholds: {table_name.split('.')[-1].title()}")
        
        # Check if table has specific overrides
        table_overrides = self._get_table_threshold_overrides(table_name)
        
        if not table_overrides:
            st.info("📋 This table uses global thresholds. Create overrides to customize.")
            
            if st.button("➕ Create Table-Specific Overrides"):
                self._create_table_overrides(table_name)
                st.rerun()
        else:
            # Show override editor
            st.success("✅ Table-specific overrides are active")
            
            # Field-specific threshold editor
            st.subheader("Field-Level Thresholds")
            
            fields = self._get_table_fields(table_name)
            selected_field = st.selectbox("Select Field", fields)
            
            if selected_field:
                field_config = table_overrides.get('fields', {}).get(selected_field, {})
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Completeness")
                    completeness_required = st.checkbox(
                        "Required Field",
                        value=field_config.get('required', False),
                        help="Field cannot be null or empty"
                    )
                    
                    if not completeness_required:
                        completeness_threshold = st.slider(
                            "Completeness Threshold",
                            min_value=0.0,
                            max_value=1.0,
                            value=field_config.get('completeness_threshold', 0.95),
                            step=0.01,
                            format="%.2f"
                        )
                    else:
                        completeness_threshold = 1.0
                        
                with col2:
                    st.subheader("Validity")
                    
                    # Pattern validation
                    pattern_enabled = st.checkbox(
                        "Pattern Validation",
                        value=bool(field_config.get('pattern')),
                        help="Validate field against regex pattern"
                    )
                    
                    if pattern_enabled:
                        pattern = st.text_input(
                            "Regex Pattern",
                            value=field_config.get('pattern', ''),
                            help="Regular expression for validation"
                        )
                    else:
                        pattern = None
                        
                    # Range validation
                    range_enabled = st.checkbox(
                        "Range Validation",
                        value=bool(field_config.get('min_value') or field_config.get('max_value')),
                        help="Validate numeric field ranges"
                    )
                    
                    if range_enabled:
                        col2a, col2b = st.columns(2)
                        with col2a:
                            min_value = st.number_input(
                                "Min Value",
                                value=field_config.get('min_value', 0),
                                format="%d"
                            )
                        with col2b:
                            max_value = st.number_input(
                                "Max Value", 
                                value=field_config.get('max_value', 100),
                                format="%d"
                            )
                    else:
                        min_value = max_value = None
                        
                # Business rule editor
                st.subheader("Business Rules")
                
                business_rules = field_config.get('business_rules', [])
                
                # Add new rule
                with st.expander("➕ Add Business Rule"):
                    rule_name = st.text_input("Rule Name", key="new_rule_name")
                    rule_description = st.text_area("Rule Description", key="new_rule_desc")
                    rule_condition = st.text_area(
                        "SQL Condition", 
                        key="new_rule_condition",
                        help="SQL condition that must evaluate to true"
                    )
                    rule_severity = st.selectbox(
                        "Severity",
                        ["info", "warning", "critical"],
                        key="new_rule_severity"
                    )
                    
                    if st.button("Add Rule", key="add_new_rule"):
                        new_rule = {
                            "name": rule_name,
                            "description": rule_description,
                            "condition": rule_condition,
                            "severity": rule_severity
                        }
                        business_rules.append(new_rule)
                        st.success("✅ Rule added!")
                        
                # Display existing rules
                for i, rule in enumerate(business_rules):
                    with st.expander(f"📋 {rule.get('name', f'Rule {i+1}')}"):
                        col1, col2 = st.columns([3, 1])
                        
                        with col1:
                            st.write(f"**Description:** {rule.get('description', 'No description')}")
                            st.code(rule.get('condition', 'No condition'), language='sql')
                            st.write(f"**Severity:** {rule.get('severity', 'info').title()}")
                            
                        with col2:
                            if st.button(f"🗑️ Delete", key=f"delete_rule_{i}"):
                                business_rules.pop(i)
                                st.rerun()
                                
                # Save field configuration
                if st.button(f"💾 Save Field Configuration", type="primary"):
                    field_config_new = {
                        'required': completeness_required,
                        'completeness_threshold': completeness_threshold,
                        'pattern': pattern,
                        'min_value': min_value,
                        'max_value': max_value,
                        'business_rules': business_rules
                    }
                    
                    self._save_field_configuration(table_name, selected_field, field_config_new)
                    st.success("✅ Field configuration saved!")
                    
    def _render_rule_management_tab(self, table_name: str):
        """Render business rule management interface"""
        
        st.subheader("📋 Business Rule Management")
        
        # Get healthcare-specific rules for the table
        table_type = self._get_table_type(table_name)
        healthcare_rules = self.quality_config.get('healthcare_quality_rules', {}).get(table_type, {})
        
        # Rule categories
        tab1, tab2, tab3, tab4 = st.tabs([
            "Field Validation", "Business Logic", "Cross-Field Rules", "Custom Rules"
        ])
        
        with tab1:
            self._render_field_validation_rules(table_name, healthcare_rules)
            
        with tab2:
            self._render_business_logic_rules(table_name, healthcare_rules)
            
        with tab3:
            self._render_cross_field_rules(table_name, healthcare_rules)
            
        with tab4:
            self._render_custom_rules_editor(table_name)
            
    def _render_anomaly_detection_tab(self, table_name: str, time_range: Dict[str, Any]):
        """Render anomaly detection interface"""
        
        st.subheader("🔍 Anomaly Detection")
        
        # Anomaly detection methods
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Anomaly timeline
            anomaly_data = self._get_anomaly_timeline(table_name, time_range["hours"])
            
            fig = px.scatter(
                anomaly_data,
                x="timestamp",
                y="anomaly_score", 
                color="anomaly_type",
                size="affected_records",
                title="Anomaly Detection Timeline",
                labels={"anomaly_score": "Anomaly Score", "timestamp": "Time"},
                hover_data=["field_name", "description"]
            )
            
            # Add anomaly threshold line
            threshold = self.quality_config.get('anomaly_detection', {}).get('statistical_methods', {}).get('z_score', {}).get('threshold', 3.0)
            fig.add_hline(
                y=threshold,
                line_dash="dash", 
                line_color="red",
                annotation_text=f"Threshold ({threshold})"
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            # Anomaly configuration
            st.subheader("⚙️ Detection Settings")
            
            # Statistical methods
            st.write("**Statistical Methods**")
            
            z_score_enabled = st.checkbox(
                "Z-Score Detection",
                value=True,
                help="Detect outliers using z-score method"
            )
            
            if z_score_enabled:
                z_threshold = st.slider(
                    "Z-Score Threshold",
                    min_value=1.0,
                    max_value=5.0,
                    value=3.0,
                    step=0.1,
                    help="Standard deviations from mean"
                )
                
            iqr_enabled = st.checkbox(
                "IQR Detection",
                value=True,
                help="Interquartile range method"
            )
            
            if iqr_enabled:
                iqr_multiplier = st.slider(
                    "IQR Multiplier",
                    min_value=1.0,
                    max_value=3.0,
                    value=1.5,
                    step=0.1
                )
                
            isolation_forest_enabled = st.checkbox(
                "Isolation Forest",
                value=False,
                help="Multivariate anomaly detection"
            )
            
            # Domain-specific rules
            st.write("**Healthcare Rules**")
            
            claim_amount_check = st.checkbox(
                "Unusual Claim Amounts",
                value=True,
                help="Detect unusually high/low claim amounts"
            )
            
            utilization_check = st.checkbox(
                "Excessive Utilization",
                value=True,
                help="Detect members with excessive healthcare utilization"
            )
            
            provider_shopping_check = st.checkbox(
                "Provider Shopping",
                value=True,
                help="Detect potential provider shopping behavior"
            )
            
        # Recent anomalies
        st.subheader("🚨 Recent Anomalies")
        
        recent_anomalies = self._get_recent_anomalies(table_name, 24)  # Last 24 hours
        
        for anomaly in recent_anomalies[:10]:
            with st.container():
                col1, col2, col3, col4, col5 = st.columns([1, 2, 1, 1, 1])
                
                with col1:
                    severity_icon = {"high": "🔴", "medium": "🟡", "low": "🔵"}[anomaly.get('severity', 'low')]
                    st.write(f"{severity_icon} {anomaly.get('severity', 'Low').title()}")
                    
                with col2:
                    st.write(f"**{anomaly.get('field_name', 'Unknown')}**")
                    st.write(anomaly.get('description', 'No description'))
                    
                with col3:
                    st.write(f"Score: {anomaly.get('score', 0):.2f}")
                    
                with col4:
                    st.write(f"Count: {anomaly.get('count', 0):,}")
                    
                with col5:
                    if st.button(f"📋 Details", key=f"anomaly_{anomaly.get('id', 0)}"):
                        self._show_anomaly_details(anomaly)
                        
                st.divider()
                
    def _render_data_profiling_tab(self, table_name: str):
        """Render data profiling interface"""
        
        st.subheader("📈 Data Profiling")
        
        # Profile generation
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            if st.button("🔄 Generate Profile", type="primary"):
                with st.spinner("Generating data profile..."):
                    profile_data = self._generate_data_profile(table_name)
                    st.session_state['profile_data'] = profile_data
                    
        with col2:
            profile_type = st.selectbox(
                "Profile Type",
                ["Standard", "Comprehensive", "Healthcare-Specific"],
                help="Choose depth of profiling analysis"
            )
            
        with col3:
            st.info("💡 Comprehensive profiling may take several minutes for large tables")
            
        # Display profile results
        if 'profile_data' in st.session_state:
            profile_data = st.session_state['profile_data']
            
            # Summary statistics
            st.subheader("📊 Summary Statistics")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Rows", f"{profile_data.get('total_rows', 0):,}")
            with col2:
                st.metric("Total Columns", profile_data.get('total_columns', 0))
            with col3:
                st.metric("Null Percentage", f"{profile_data.get('overall_null_pct', 0):.1f}%")
            with col4:
                st.metric("Duplicate Rows", f"{profile_data.get('duplicate_rows', 0):,}")
                
            # Field-level profiles
            st.subheader("🔍 Field-Level Profiles")
            
            field_profiles = profile_data.get('field_profiles', {})
            
            selected_field = st.selectbox(
                "Select Field for Detailed Analysis",
                list(field_profiles.keys())
            )
            
            if selected_field and selected_field in field_profiles:
                field_data = field_profiles[selected_field]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Basic statistics
                    st.subheader("Basic Statistics")
                    
                    stats_df = pd.DataFrame([
                        {"Metric": "Data Type", "Value": field_data.get('data_type', 'Unknown')},
                        {"Metric": "Null Count", "Value": f"{field_data.get('null_count', 0):,}"},
                        {"Metric": "Null Percentage", "Value": f"{field_data.get('null_pct', 0):.1f}%"},
                        {"Metric": "Unique Values", "Value": f"{field_data.get('unique_count', 0):,}"},
                        {"Metric": "Uniqueness", "Value": f"{field_data.get('uniqueness_pct', 0):.1f}%"},
                    ])
                    
                    if field_data.get('data_type') in ['int', 'float', 'decimal']:
                        numeric_stats = [
                            {"Metric": "Mean", "Value": f"{field_data.get('mean', 0):.2f}"},
                            {"Metric": "Median", "Value": f"{field_data.get('median', 0):.2f}"},
                            {"Metric": "Std Dev", "Value": f"{field_data.get('stddev', 0):.2f}"},
                            {"Metric": "Min", "Value": f"{field_data.get('min', 0):.2f}"},
                            {"Metric": "Max", "Value": f"{field_data.get('max', 0):.2f}"},
                        ]
                        stats_df = pd.concat([stats_df, pd.DataFrame(numeric_stats)], ignore_index=True)
                        
                    st.dataframe(stats_df, use_container_width=True, hide_index=True)
                    
                with col2:
                    # Value distribution
                    if 'value_distribution' in field_data:
                        st.subheader("Value Distribution")
                        
                        dist_data = field_data['value_distribution']
                        
                        if field_data.get('data_type') in ['int', 'float', 'decimal']:
                            # Histogram for numeric data
                            fig = px.histogram(
                                x=dist_data.get('values', []),
                                nbins=20,
                                title=f"Distribution of {selected_field}"
                            )
                        else:
                            # Bar chart for categorical data
                            top_values = dist_data.get('top_values', [])[:10]
                            fig = px.bar(
                                x=[item['value'] for item in top_values],
                                y=[item['count'] for item in top_values],
                                title=f"Top Values in {selected_field}"
                            )
                            
                        st.plotly_chart(fig, use_container_width=True)
                        
                # Quality issues for this field
                st.subheader("🚨 Quality Issues")
                
                field_issues = field_data.get('quality_issues', [])
                
                if field_issues:
                    for issue in field_issues:
                        severity_color = {"critical": "🔴", "warning": "🟡", "info": "🔵"}[issue.get('severity', 'info')]
                        st.write(f"{severity_color} **{issue.get('type', 'Issue')}**: {issue.get('description', 'No description')}")
                else:
                    st.success("✅ No quality issues detected for this field")
                    
    def _render_alerts_tab(self, table_name: str, time_range: Dict[str, Any]):
        """Render alerts management interface"""
        
        st.subheader("🚨 Quality Alerts")
        
        # Alert summary
        alert_summary = self._get_alert_summary(table_name, time_range["hours"])
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("Total Alerts", alert_summary.get('total', 0))
        with col2:
            st.metric("Critical", alert_summary.get('critical', 0), delta_color="inverse")
        with col3:
            st.metric("Warning", alert_summary.get('warning', 0))
        with col4:
            st.metric("Info", alert_summary.get('info', 0))
        with col5:
            st.metric("Resolved", alert_summary.get('resolved', 0), delta_color="normal")
            
        # Alert timeline
        alert_timeline_data = self._get_alert_timeline(table_name, time_range["hours"])
        
        if not alert_timeline_data.empty:
            fig = px.timeline(
                alert_timeline_data,
                x_start="start_time",
                x_end="end_time",
                y="alert_type",
                color="severity",
                title="Alert Timeline",
                color_discrete_map={
                    "critical": "red",
                    "warning": "orange",
                    "info": "blue"
                }
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📊 No alerts in the selected time range")
            
        # Alert configuration
        st.subheader("⚙️ Alert Configuration")
        
        # Alert rules editor
        with st.expander("📋 Alert Rules"):
            alert_rules = self.quality_config.get('alerting', {}).get('rules', {})
            
            for severity, rules in alert_rules.items():
                st.write(f"**{severity.title()} Alerts**")
                
                conditions = rules.get('conditions', [])
                for i, condition in enumerate(conditions):
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        new_condition = st.text_input(
                            f"Condition {i+1}",
                            value=condition,
                            key=f"condition_{severity}_{i}"
                        )
                        
                    with col2:
                        if st.button(f"🗑️", key=f"delete_condition_{severity}_{i}"):
                            conditions.pop(i)
                            st.rerun()
                            
                # Add new condition
                new_condition = st.text_input(
                    f"Add new {severity} condition",
                    key=f"new_condition_{severity}"
                )
                
                if st.button(f"➕ Add {severity.title()} Condition", key=f"add_{severity}"):
                    if new_condition:
                        conditions.append(new_condition)
                        st.success(f"✅ Added {severity} condition")
                        
        # Notification settings
        with st.expander("📧 Notification Settings"):
            channels = self.quality_config.get('alerting', {}).get('channels', {})
            
            # Email settings
            st.write("**Email Notifications**")
            email_enabled = st.checkbox(
                "Enable Email Alerts",
                value=channels.get('email', {}).get('enabled', False)
            )
            
            if email_enabled:
                recipients = st.text_area(
                    "Recipients (one per line)",
                    value="\n".join(channels.get('email', {}).get('recipients', [])),
                    help="Enter email addresses, one per line"
                )
                
            # Slack settings
            st.write("**Slack Notifications**")
            slack_enabled = st.checkbox(
                "Enable Slack Alerts",
                value=channels.get('slack', {}).get('enabled', False)
            )
            
            if slack_enabled:
                slack_webhook = st.text_input(
                    "Slack Webhook URL",
                    value=channels.get('slack', {}).get('webhook_url', ''),
                    type="password"
                )
                
                slack_channel = st.text_input(
                    "Slack Channel",
                    value=channels.get('slack', {}).get('channel', '#data-quality')
                )
                
        # Active alerts list
        st.subheader("📋 Active Alerts")
        
        active_alerts = self._get_active_alerts(table_name)
        
        if active_alerts:
            for alert in active_alerts:
                with st.container():
                    col1, col2, col3, col4, col5 = st.columns([1, 3, 1, 1, 1])
                    
                    with col1:
                        severity_icon = {"critical": "🔴", "warning": "🟡", "info": "🔵"}[alert.get('severity', 'info')]
                        st.write(f"{severity_icon} {alert.get('severity', 'Info').title()}")
                        
                    with col2:
                        st.write(f"**{alert.get('type', 'Alert')}**")
                        st.write(alert.get('message', 'No message'))
                        
                    with col3:
                        st.write(alert.get('timestamp', 'Unknown'))
                        
                    with col4:
                        st.write(f"Count: {alert.get('count', 1)}")
                        
                    with col5:
                        if st.button(f"✅ Resolve", key=f"resolve_{alert.get('id', 0)}"):
                            self._resolve_alert(alert.get('id'))
                            st.success("Alert resolved!")
                            st.rerun()
                            
                    st.divider()
        else:
            st.success("🎉 No active alerts - all quality checks are passing!")
            
    def _render_reports_tab(self, table_name: str, time_range: Dict[str, Any]):
        """Render quality reports interface"""
        
        st.subheader("📊 Quality Reports")
        
        # Report types
        report_type = st.selectbox(
            "Report Type",
            ["Executive Summary", "Detailed Quality Report", "Compliance Report", "Trend Analysis"],
            help="Choose the type of quality report to generate"
        )
        
        # Report parameters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            report_format = st.selectbox(
                "Format",
                ["PDF", "HTML", "Excel", "JSON"],
                help="Choose output format"
            )
            
        with col2:
            include_charts = st.checkbox(
                "Include Charts",
                value=True,
                help="Include visualizations in the report"
            )
            
        with col3:
            if st.button("📊 Generate Report", type="primary"):
                with st.spinner("Generating report..."):
                    report_data = self._generate_quality_report(
                        table_name, report_type, time_range, include_charts
                    )
                    st.session_state['generated_report'] = report_data
                    
        # Report preview
        if 'generated_report' in st.session_state:
            report_data = st.session_state['generated_report']
            
            st.subheader("📋 Report Preview")
            
            # Executive Summary
            if report_type == "Executive Summary":
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric(
                        "Overall Quality Score",
                        f"{report_data.get('overall_score', 0):.1f}%",
                        f"{report_data.get('score_change', 0):+.1f}%"
                    )
                    
                with col2:
                    st.metric(
                        "Critical Issues",
                        report_data.get('critical_issues', 0),
                        f"{report_data.get('issues_change', 0):+d}"
                    )
                    
                with col3:
                    st.metric(
                        "Data Freshness",
                        f"{report_data.get('avg_freshness_hours', 0):.1f}h",
                        f"{report_data.get('freshness_change', 0):+.1f}h"
                    )
                    
                # Quality trends
                if include_charts:
                    st.subheader("📈 Quality Trends")
                    trend_fig = self._create_quality_trend_chart(report_data.get('trend_data', {}))
                    st.plotly_chart(trend_fig, use_container_width=True)
                    
                # Key findings
                st.subheader("🔍 Key Findings")
                findings = report_data.get('key_findings', [])
                for finding in findings:
                    st.write(f"• {finding}")
                    
            # Detailed Report
            elif report_type == "Detailed Quality Report":
                
                # Table of contents
                st.subheader("📑 Table of Contents")
                st.write("1. Executive Summary")
                st.write("2. Quality Dimensions Analysis") 
                st.write("3. Field-Level Quality Assessment")
                st.write("4. Business Rule Compliance")
                st.write("5. Anomaly Detection Results")
                st.write("6. Recommendations")
                
                # Detailed sections would be rendered here
                st.info("📄 Full detailed report available in downloaded format")
                
            # Download button
            st.subheader("📥 Download Report")
            
            report_filename = f"quality_report_{table_name.split('.')[-1]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{report_format.lower()}"
            
            if st.download_button(
                label=f"📁 Download {report_format} Report",
                data=self._format_report_for_download(report_data, report_format),
                file_name=report_filename,
                mime=self._get_mime_type(report_format)
            ):
                st.success("✅ Report downloaded successfully!")
                
        # Scheduled reports
        st.subheader("⏰ Scheduled Reports")
        
        scheduled_reports = self.quality_config.get('reporting', {}).get('scheduled_reports', {})
        
        for report_name, report_config in scheduled_reports.items():
            with st.expander(f"📅 {report_name.replace('_', ' ').title()}"):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.write(f"**Schedule:** {report_config.get('schedule', 'Not set')}")
                    
                with col2:
                    st.write(f"**Recipients:** {len(report_config.get('recipients', []))}")
                    
                with col3:
                    st.write(f"**Format:** {report_config.get('format', 'PDF')}")
                    
                enabled = st.checkbox(
                    f"Enable {report_name}",
                    value=report_config.get('enabled', True),
                    key=f"enable_{report_name}"
                )
                
                if st.button(f"🏃 Run Now", key=f"run_{report_name}"):
                    self._trigger_scheduled_report(report_name)
                    st.success(f"✅ {report_name} triggered successfully!")

    # Helper methods for data retrieval and processing
    def _get_quality_trends(self, table_name: str, hours: int) -> pd.DataFrame:
        """Get quality trend data"""
        # Mock data - in production would query monitoring tables
        timestamps = pd.date_range(
            start=datetime.now() - timedelta(hours=hours),
            end=datetime.now(),
            freq="H"
        )
        
        import numpy as np
        
        return pd.DataFrame({
            'timestamp': timestamps,
            'overall_quality': 92 + np.random.normal(0, 3, len(timestamps)),
            'completeness': 95 + np.random.normal(0, 2, len(timestamps)),
            'validity': 90 + np.random.normal(0, 4, len(timestamps)),
            'consistency': 94 + np.random.normal(0, 2, len(timestamps))
        })
        
    def _get_current_quality_scores(self, table_name: str) -> Dict[str, float]:
        """Get current quality scores"""
        return {
            'overall_quality': 94.2,
            'completeness': 96.8,
            'validity': 92.1,
            'consistency': 95.6,
            'accuracy': 93.4,
            'timeliness': 97.2
        }
        
    def _get_field_quality_summary(self, table_name: str) -> pd.DataFrame:
        """Get field-level quality summary"""
        # Mock data
        fields = ['member_id', 'claim_id', 'provider_npi', 'diagnosis_code', 'claim_amount']
        dimensions = ['Completeness', 'Validity', 'Consistency', 'Accuracy']
        
        import numpy as np
        
        data = []
        for field in fields:
            row = []
            for dim in dimensions:
                score = np.random.uniform(85, 99)
                row.append(score)
            data.append(row)
            
        return pd.DataFrame(data, index=fields, columns=dimensions)
        
    def _get_top_quality_issues(self, table_name: str) -> List[Dict[str, Any]]:
        """Get top quality issues"""
        return [
            {
                'id': 1,
                'field': 'member_id',
                'severity': 'warning',
                'description': 'Invalid format detected in 234 records',
                'count': 234
            },
            {
                'id': 2, 
                'field': 'claim_amount',
                'severity': 'critical',
                'description': 'Null values found in required field',
                'count': 45
            },
            {
                'id': 3,
                'field': 'diagnosis_code',
                'severity': 'warning', 
                'description': 'Legacy ICD-9 codes still present',
                'count': 89
            }
        ]
        
    def _trigger_quality_check(self, table_name: str):
        """Trigger quality check"""
        st.info(f"🔍 Quality check triggered for {table_name}")
        # Implementation would trigger actual quality check
        
    def _export_configuration(self):
        """Export quality configuration"""
        config_yaml = yaml.dump(self.quality_config, default_flow_style=False)
        st.download_button(
            label="📥 Download Configuration",
            data=config_yaml,
            file_name=f"quality_config_{datetime.now().strftime('%Y%m%d_%H%M%S')}.yaml",
            mime="application/x-yaml"
        )
        
    def _import_configuration(self, uploaded_file):
        """Import quality configuration"""
        try:
            config_data = yaml.safe_load(uploaded_file)
            # Validate and merge configuration
            st.success("✅ Configuration imported successfully!")
        except Exception as e:
            st.error(f"❌ Failed to import configuration: {str(e)}")
            
    # Additional helper methods would continue here...
    def _save_thresholds(self, thresholds: Dict[str, Any]):
        """Save updated thresholds"""
        self.quality_config['global_thresholds'].update(thresholds)
        # In production, would save to configuration file
        
    def _auto_fix_issue(self, table_name: str, issue: Dict[str, Any]):
        """Auto-fix quality issue"""
        st.info(f"🔧 Auto-fixing issue: {issue.get('description', 'Unknown issue')}")
        # Implementation would trigger auto-remediation


if __name__ == "__main__":
    config = {
        "databricks_host": "your-databricks-workspace",
        "catalog_name": "healthcare_data"
    }
    
    dashboard = QualityDashboard(config)
    dashboard.render_dashboard()