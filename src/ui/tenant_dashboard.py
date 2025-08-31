"""
Tenant-Specific Dashboard for SaaS Platform
Self-service interface for tenant users to manage their data pipelines and quality
"""

import streamlit as st
import asyncio
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging

# Import control plane services
from ..control_plane.data_plane_orchestrator import DataPlaneOrchestrator
from ..control_plane.billing_service import BillingService
from ..control_plane.usage_tracker import UsageTracker
from ..control_plane.tenant_manager import TenantManager

logger = logging.getLogger(__name__)

class TenantDashboard:
    """Self-service dashboard for tenant users"""
    
    def __init__(self):
        # Initialize services
        if 'data_plane_orchestrator' not in st.session_state:
            st.session_state.data_plane_orchestrator = DataPlaneOrchestrator()
            st.session_state.billing_service = BillingService()
            st.session_state.usage_tracker = UsageTracker()
            st.session_state.tenant_manager = TenantManager()
        
        # Mock authentication - in production this would come from JWT token
        if 'tenant_id' not in st.session_state:
            st.session_state.tenant_id = "tenant_12345678"  # Mock tenant ID
            st.session_state.user_role = "admin"
    
    def run(self):
        """Main dashboard application"""
        st.set_page_config(
            page_title="Agentic Data Engineering - Tenant Portal",
            page_icon="🏥",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        # Get tenant information
        tenant_id = st.session_state.tenant_id
        tenant_manager = st.session_state.tenant_manager
        tenant_config = tenant_manager.get_tenant(tenant_id)
        
        if not tenant_config:
            st.error("Tenant configuration not found. Please contact support.")
            return
        
        st.title(f"🏥 {tenant_config.organization_name}")
        st.markdown(f"**Healthcare Data Platform** | Tenant: `{tenant_id}`")
        
        # Sidebar navigation
        st.sidebar.title("Navigation")
        st.sidebar.write(f"👤 **Logged in as:** {st.session_state.user_role}")
        st.sidebar.write(f"🏢 **Tenant:** {tenant_config.organization_name}")
        st.sidebar.write(f"📊 **Plan:** {tenant_config.tier.value.title()}")
        
        page = st.sidebar.selectbox(
            "Select Page",
            [
                "Dashboard Overview",
                "Pipeline Management",
                "Data Quality",
                "Usage & Billing",
                "Settings"
            ]
        )
        
        # Route to appropriate page
        if page == "Dashboard Overview":
            self._render_dashboard_overview(tenant_config)
        elif page == "Pipeline Management":
            self._render_pipeline_management(tenant_id)
        elif page == "Data Quality":
            self._render_data_quality(tenant_id)
        elif page == "Usage & Billing":
            self._render_usage_billing(tenant_id, tenant_config)
        elif page == "Settings":
            self._render_settings(tenant_id, tenant_config)
    
    def _render_dashboard_overview(self, tenant_config):
        """Render main dashboard overview"""
        st.header("📊 Dashboard Overview")
        
        tenant_id = st.session_state.tenant_id
        data_plane_orchestrator = st.session_state.data_plane_orchestrator
        usage_tracker = st.session_state.usage_tracker
        
        # Key metrics
        pipelines = asyncio.run(data_plane_orchestrator.list_pipelines(tenant_id))
        resource_usage = asyncio.run(data_plane_orchestrator.get_tenant_resource_usage(tenant_id))
        current_usage = usage_tracker.get_current_tenant_usage(tenant_id)
        
        # Metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            active_pipelines = len([p for p in pipelines if p["status"] == "active"])
            st.metric(
                "Active Pipelines",
                active_pipelines,
                delta=f"/{tenant_config.max_concurrent_pipelines} limit"
            )
        
        with col2:
            gb_processed = float(current_usage.get("gb_processed", 0))
            st.metric(
                "GB Processed (MTD)",
                f"{gb_processed:.1f}",
                delta=f"/{tenant_config.max_monthly_gb_processed} limit"
            )
        
        with col3:
            if pipelines:
                avg_success_rate = sum(p["success_rate"] for p in pipelines) / len(pipelines)
                st.metric("Average Success Rate", f"{avg_success_rate:.1f}%")
            else:
                st.metric("Average Success Rate", "N/A")
        
        with col4:
            compute_hours = float(current_usage.get("compute_hours", 0))
            st.metric(
                "Compute Hours (MTD)",
                f"{compute_hours:.1f}",
                delta=f"/{tenant_config.max_compute_hours_monthly} limit"
            )
        
        # Usage progress bars
        st.subheader("Resource Usage")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # GB Processing usage
            gb_usage_pct = (gb_processed / tenant_config.max_monthly_gb_processed) * 100
            st.progress(min(gb_usage_pct / 100, 1.0))
            st.caption(f"Data Processing: {gb_usage_pct:.1f}% of monthly limit")
        
        with col2:
            # Compute usage
            compute_usage_pct = (compute_hours / tenant_config.max_compute_hours_monthly) * 100
            st.progress(min(compute_usage_pct / 100, 1.0))
            st.caption(f"Compute Hours: {compute_usage_pct:.1f}% of monthly limit")
        
        # Pipeline status overview
        st.subheader("Pipeline Status Overview")
        
        if pipelines:
            col1, col2 = st.columns(2)
            
            with col1:
                # Pipeline status distribution
                status_counts = {}
                for pipeline in pipelines:
                    status = pipeline["status"]
                    status_counts[status] = status_counts.get(status, 0) + 1
                
                fig = px.pie(
                    values=list(status_counts.values()),
                    names=list(status_counts.keys()),
                    title="Pipeline Status Distribution"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Recent pipeline activity
                recent_pipelines = sorted(
                    [p for p in pipelines if p["last_run_at"]],
                    key=lambda x: x["last_run_at"],
                    reverse=True
                )[:5]
                
                if recent_pipelines:
                    st.write("**Recent Pipeline Activity:**")
                    for pipeline in recent_pipelines:
                        status_emoji = "✅" if pipeline["success_rate"] > 90 else "⚠️" if pipeline["success_rate"] > 70 else "❌"
                        st.write(f"{status_emoji} {pipeline['name']} - {pipeline['success_rate']:.1f}% success")
                else:
                    st.info("No recent pipeline activity")
        else:
            st.info("No pipelines configured. Create your first pipeline to get started!")
            if st.button("Create First Pipeline"):
                st.session_state.show_create_pipeline = True
                st.rerun()
        
        # Quick actions
        st.subheader("Quick Actions")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("📊 View Quality Reports"):
                # Navigate to quality page
                pass
        
        with col2:
            if st.button("🔧 Create Pipeline"):
                st.session_state.show_create_pipeline = True
                st.rerun()
        
        with col3:
            if st.button("💰 View Billing"):
                # Navigate to billing page
                pass
        
        with col4:
            if st.button("⚙️ Settings"):
                # Navigate to settings page
                pass
    
    def _render_pipeline_management(self, tenant_id):
        """Render pipeline management interface"""
        st.header("🔄 Pipeline Management")
        
        data_plane_orchestrator = st.session_state.data_plane_orchestrator
        
        tab1, tab2, tab3 = st.tabs(["All Pipelines", "Create Pipeline", "Pipeline Details"])
        
        with tab1:
            self._render_pipeline_list(tenant_id)
        
        with tab2:
            self._render_create_pipeline_form(tenant_id)
        
        with tab3:
            self._render_pipeline_details(tenant_id)
    
    def _render_pipeline_list(self, tenant_id):
        """Render list of tenant pipelines"""
        st.subheader("Your Pipelines")
        
        data_plane_orchestrator = st.session_state.data_plane_orchestrator
        pipelines = asyncio.run(data_plane_orchestrator.list_pipelines(tenant_id))
        
        if not pipelines:
            st.info("No pipelines found. Create your first pipeline to get started!")
            return
        
        # Pipeline table
        pipeline_data = []
        for pipeline in pipelines:
            last_run = pipeline.get("last_run_at")
            if last_run:
                last_run = datetime.fromisoformat(last_run.replace('Z', '+00:00')).strftime("%Y-%m-%d %H:%M")
            
            pipeline_data.append({
                "Name": pipeline["name"],
                "Status": pipeline["status"],
                "Source Type": pipeline["source_type"],
                "Success Rate": f"{pipeline['success_rate']:.1f}%",
                "Total Runs": pipeline["total_runs"],
                "GB Processed": f"{pipeline['total_gb_processed']:.1f}",
                "Last Run": last_run or "Never",
                "Actions": pipeline["pipeline_id"]
            })
        
        df = pd.DataFrame(pipeline_data)
        st.dataframe(df.drop("Actions", axis=1), use_container_width=True)
        
        # Pipeline actions
        st.subheader("Pipeline Actions")
        
        selected_pipeline = st.selectbox(
            "Select Pipeline",
            options=[p["pipeline_id"] for p in pipelines],
            format_func=lambda x: next(p["name"] for p in pipelines if p["pipeline_id"] == x)
        )
        
        if selected_pipeline:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if st.button("▶️ Run Now"):
                    with st.spinner("Executing pipeline..."):
                        result = asyncio.run(data_plane_orchestrator.execute_pipeline(tenant_id, selected_pipeline))
                        if result["status"] == "success":
                            st.success(f"Pipeline executed successfully!")
                            st.info(f"Processed {result.get('gb_processed', 0):.1f} GB in {result.get('execution_time_seconds', 0):.1f}s")
                        else:
                            st.error(f"Pipeline failed: {result.get('error', 'Unknown error')}")
                        st.rerun()
            
            with col2:
                if st.button("⏸️ Pause"):
                    asyncio.run(data_plane_orchestrator.pause_pipeline(tenant_id, selected_pipeline))
                    st.success("Pipeline paused")
                    st.rerun()
            
            with col3:
                if st.button("▶️ Resume"):
                    asyncio.run(data_plane_orchestrator.resume_pipeline(tenant_id, selected_pipeline))
                    st.success("Pipeline resumed")
                    st.rerun()
            
            with col4:
                if st.button("🗑️ Delete", type="secondary"):
                    if st.session_state.get("confirm_delete"):
                        asyncio.run(data_plane_orchestrator.delete_pipeline(tenant_id, selected_pipeline))
                        st.success("Pipeline deleted")
                        st.session_state.confirm_delete = False
                        st.rerun()
                    else:
                        st.session_state.confirm_delete = True
                        st.warning("Click delete again to confirm")
    
    def _render_create_pipeline_form(self, tenant_id):
        """Render pipeline creation form"""
        st.subheader("Create New Pipeline")
        
        with st.form("create_pipeline_form"):
            # Basic information
            col1, col2 = st.columns(2)
            
            with col1:
                pipeline_name = st.text_input("Pipeline Name", placeholder="Healthcare Claims Processing")
                source_type = st.selectbox("Source Type", ["auto_loader", "batch", "streaming"])
            
            with col2:
                schedule = st.selectbox(
                    "Schedule",
                    ["none", "hourly", "daily", "weekly"],
                    help="How often should this pipeline run automatically?"
                )
            
            # Source configuration
            st.subheader("Data Source Configuration")
            
            if source_type == "auto_loader":
                source_path = st.text_input("Source Path", placeholder="/mnt/healthcare/claims/")
                file_format = st.selectbox("File Format", ["csv", "parquet", "json", "xml"])
                
                source_config = {
                    "source_path": source_path,
                    "file_format": file_format,
                    "schema_evolution": True,
                    "checkpoint_location": f"/mnt/checkpoints/{tenant_id}/",
                }
            
            elif source_type == "batch":
                source_path = st.text_input("Source Path", placeholder="/mnt/healthcare/batch_data/")
                file_format = st.selectbox("File Format", ["csv", "parquet", "json"])
                
                source_config = {
                    "source_path": source_path,
                    "file_format": file_format,
                    "read_options": {}
                }
            
            else:  # streaming
                kafka_bootstrap_servers = st.text_input("Kafka Bootstrap Servers", placeholder="localhost:9092")
                topic = st.text_input("Kafka Topic", placeholder="healthcare-claims")
                
                source_config = {
                    "kafka_bootstrap_servers": kafka_bootstrap_servers,
                    "topic": topic,
                    "starting_offsets": "latest"
                }
            
            # Quality configuration
            st.subheader("Data Quality Configuration")
            
            enable_completeness = st.checkbox("Completeness Checks", value=True)
            completeness_threshold = st.slider("Completeness Threshold", 0.0, 1.0, 0.95)
            
            enable_validity = st.checkbox("Validity Checks", value=True) 
            validity_threshold = st.slider("Validity Threshold", 0.0, 1.0, 0.98)
            
            enable_anomaly_detection = st.checkbox("Anomaly Detection", value=True)
            
            quality_config = {
                "completeness_check": enable_completeness,
                "completeness_threshold": completeness_threshold,
                "validity_check": enable_validity,
                "validity_threshold": validity_threshold,
                "anomaly_detection": enable_anomaly_detection,
                "healthcare_validations": True,  # Always enable for healthcare
                "custom_rules": []
            }
            
            # Target configuration
            st.subheader("Target Configuration")
            
            target_catalog = st.text_input("Target Catalog", value=f"tenant_{tenant_id}")
            target_schema = st.text_input("Target Schema", value="healthcare_data")
            target_table = st.text_input("Target Table", placeholder="claims_processed")
            
            target_config = {
                "catalog": target_catalog,
                "schema": target_schema,
                "table": target_table,
                "write_mode": "append",
                "partition_columns": ["processing_date"]
            }
            
            # Submit button
            submitted = st.form_submit_button("Create Pipeline")
            
            if submitted and pipeline_name and source_config and target_config:
                try:
                    data_plane_orchestrator = st.session_state.data_plane_orchestrator
                    
                    pipeline_config = asyncio.run(data_plane_orchestrator.create_pipeline(
                        tenant_id=tenant_id,
                        name=pipeline_name,
                        source_type=source_type,
                        source_config=source_config,
                        quality_config=quality_config,
                        target_config=target_config,
                        schedule=schedule if schedule != "none" else None
                    ))
                    
                    st.success(f"Successfully created pipeline: {pipeline_config['pipeline_id']}")
                    st.info(f"Status: {pipeline_config['status']}")
                    
                except Exception as e:
                    st.error(f"Failed to create pipeline: {str(e)}")
    
    def _render_pipeline_details(self, tenant_id):
        """Render detailed pipeline information"""
        st.subheader("Pipeline Details")
        
        data_plane_orchestrator = st.session_state.data_plane_orchestrator
        pipelines = asyncio.run(data_plane_orchestrator.list_pipelines(tenant_id))
        
        if not pipelines:
            st.info("No pipelines available")
            return
        
        # Pipeline selection
        selected_pipeline_id = st.selectbox(
            "Select Pipeline",
            options=[p["pipeline_id"] for p in pipelines],
            format_func=lambda x: next(p["name"] for p in pipelines if p["pipeline_id"] == x)
        )
        
        if selected_pipeline_id:
            pipeline_details = asyncio.run(data_plane_orchestrator.get_pipeline(tenant_id, selected_pipeline_id))
            
            if pipeline_details:
                # Basic information
                col1, col2 = st.columns(2)
                
                with col1:
                    st.info(f"""
                    **Name:** {pipeline_details['name']}
                    **Status:** {pipeline_details['status']}
                    **Source Type:** {pipeline_details['source_type']}
                    **Resource Tier:** {pipeline_details['resource_tier']}
                    **Schedule:** {pipeline_details['schedule'] or 'Manual'}
                    """)
                
                with col2:
                    st.info(f"""
                    **Created:** {pipeline_details['created_at'][:19]}
                    **Last Run:** {pipeline_details['last_run_at'][:19] if pipeline_details['last_run_at'] else 'Never'}
                    **Next Run:** {pipeline_details['next_run_at'][:19] if pipeline_details['next_run_at'] else 'Not scheduled'}
                    **Storage:** {pipeline_details['storage_location']}
                    """)
                
                # Statistics
                st.subheader("Performance Statistics")
                stats = pipeline_details["statistics"]
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total Runs", stats["total_runs"])
                with col2:
                    st.metric("Success Rate", f"{stats['success_rate']:.1f}%")
                with col3:
                    st.metric("GB Processed", f"{stats['total_gb_processed']:.1f}")
                with col4:
                    st.metric("Avg Runtime", f"{stats['avg_runtime_seconds']:.1f}s")
                
                # Configuration details
                st.subheader("Configuration")
                
                tab1, tab2, tab3 = st.tabs(["Source Config", "Quality Config", "Target Config"])
                
                with tab1:
                    st.json(pipeline_details["source_config"])
                
                with tab2:
                    st.json(pipeline_details["quality_config"])
                
                with tab3:
                    st.json(pipeline_details["target_config"])
    
    def _render_data_quality(self, tenant_id):
        """Render data quality monitoring"""
        st.header("📊 Data Quality")
        
        data_plane_orchestrator = st.session_state.data_plane_orchestrator
        
        # Get quality reports
        quality_reports = asyncio.run(data_plane_orchestrator.get_quality_reports(tenant_id, days=30))
        
        if not quality_reports:
            st.info("No quality reports available yet. Run some pipelines to see quality metrics.")
            return
        
        # Quality overview
        st.subheader("Quality Overview (Last 30 Days)")
        
        # Calculate aggregate metrics
        total_checks = sum(report["total_quality_checks"] for report in quality_reports)
        avg_quality_score = sum(report["quality_score"] for report in quality_reports) / len(quality_reports)
        total_anomalies = sum(report["anomalies_detected"] for report in quality_reports)
        total_data_processed = sum(report["data_processed_gb"] for report in quality_reports)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Quality Checks", f"{total_checks:,}")
        with col2:
            st.metric("Average Quality Score", f"{avg_quality_score:.3f}")
        with col3:
            st.metric("Anomalies Detected", total_anomalies)
        with col4:
            st.metric("Data Processed", f"{total_data_processed:.1f} GB")
        
        # Quality trends
        st.subheader("Quality Trends")
        
        # Create quality trend chart
        quality_data = []
        for report in quality_reports:
            quality_data.append({
                "Pipeline": report["pipeline_name"],
                "Quality Score": report["quality_score"],
                "Anomalies": report["anomalies_detected"],
                "GB Processed": report["data_processed_gb"]
            })
        
        df = pd.DataFrame(quality_data)
        
        # Quality score by pipeline
        fig = px.bar(df, x="Pipeline", y="Quality Score", title="Quality Score by Pipeline")
        st.plotly_chart(fig, use_container_width=True)
        
        # Detailed quality reports
        st.subheader("Detailed Quality Reports")
        
        for report in quality_reports:
            with st.expander(f"{report['pipeline_name']} - Quality Report"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Pipeline:** {report['pipeline_name']}")
                    st.write(f"**Quality Score:** {report['quality_score']:.3f}")
                    st.write(f"**Total Checks:** {report['total_quality_checks']:,}")
                
                with col2:
                    st.write(f"**Anomalies Detected:** {report['anomalies_detected']}")
                    st.write(f"**Data Processed:** {report['data_processed_gb']:.1f} GB")
                    st.write(f"**Report Generated:** {report['generated_at'][:19]}")
    
    def _render_usage_billing(self, tenant_id, tenant_config):
        """Render usage and billing information"""
        st.header("💰 Usage & Billing")
        
        billing_service = st.session_state.billing_service
        usage_tracker = st.session_state.usage_tracker
        
        tab1, tab2, tab3 = st.tabs(["Current Usage", "Billing Information", "Usage History"])
        
        with tab1:
            self._render_current_usage(tenant_id, tenant_config)
        
        with tab2:
            self._render_billing_info(tenant_id)
        
        with tab3:
            self._render_usage_history(tenant_id)
    
    def _render_current_usage(self, tenant_id, tenant_config):
        """Render current usage metrics"""
        st.subheader("Current Monthly Usage")
        
        usage_tracker = st.session_state.usage_tracker
        current_usage = usage_tracker.get_current_tenant_usage(tenant_id)
        
        # Usage metrics with limits
        col1, col2 = st.columns(2)
        
        with col1:
            # GB Processing
            gb_processed = float(current_usage.get("gb_processed", 0))
            gb_limit = tenant_config.max_monthly_gb_processed
            gb_usage_pct = (gb_processed / gb_limit) * 100
            
            st.metric("Data Processed", f"{gb_processed:.1f} GB", f"{gb_usage_pct:.1f}% of limit")
            st.progress(min(gb_usage_pct / 100, 1.0))
            st.caption(f"Limit: {gb_limit} GB/month")
            
            # Storage
            storage_gb = float(current_usage.get("storage_gb", 0))
            storage_limit = tenant_config.storage_quota_gb
            storage_usage_pct = (storage_gb / storage_limit) * 100
            
            st.metric("Storage Used", f"{storage_gb:.1f} GB", f"{storage_usage_pct:.1f}% of quota")
            st.progress(min(storage_usage_pct / 100, 1.0))
            st.caption(f"Quota: {storage_limit} GB")
        
        with col2:
            # Compute Hours
            compute_hours = float(current_usage.get("compute_hours", 0))
            compute_limit = tenant_config.max_compute_hours_monthly
            compute_usage_pct = (compute_hours / compute_limit) * 100
            
            st.metric("Compute Hours", f"{compute_hours:.1f}", f"{compute_usage_pct:.1f}% of limit")
            st.progress(min(compute_usage_pct / 100, 1.0))
            st.caption(f"Limit: {compute_limit} hours/month")
            
            # Quality Checks
            quality_checks = float(current_usage.get("quality_checks", 0))
            st.metric("Quality Checks", f"{quality_checks:,.0f}")
            st.caption("No limit for your plan")
        
        # Usage alerts
        if gb_usage_pct > 80 or compute_usage_pct > 80 or storage_usage_pct > 80:
            st.warning("⚠️ You're approaching your usage limits. Consider upgrading your plan or optimizing your pipelines.")
        
        if gb_usage_pct > 95 or compute_usage_pct > 95 or storage_usage_pct > 95:
            st.error("🚨 Critical: You're near your usage limits. Upgrade immediately to avoid service interruption.")
    
    def _render_billing_info(self, tenant_id):
        """Render billing information"""
        st.subheader("Billing Information")
        
        billing_service = st.session_state.billing_service
        
        try:
            billing_info = asyncio.run(billing_service.get_tenant_billing(tenant_id))
            
            # Account information
            account = billing_info["account"]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.info(f"""
                **Plan:** {account['plan_name']}
                **Billing Period:** {account['billing_period'].title()}
                **Account Status:** {account['account_status'].title()}
                **Base Monthly Fee:** {billing_info['base_monthly_fee']}
                """)
            
            with col2:
                st.info(f"""
                **Current Period:** {account['current_period_start'][:10]} to {account['current_period_end'][:10]}
                **Next Billing Date:** {account['next_billing_date'][:10]}
                **Features:** {len(billing_info['plan_features'])} included
                """)
            
            # Plan features
            st.subheader("Plan Features")
            
            for feature in billing_info['plan_features']:
                st.write(f"✅ {feature}")
            
            # Recent invoices
            st.subheader("Recent Invoices")
            
            recent_invoices = billing_info.get("recent_invoices", [])
            
            if recent_invoices:
                invoice_data = []
                for invoice in recent_invoices:
                    invoice_data.append({
                        "Invoice ID": invoice["invoice_id"],
                        "Period": f"{invoice['period_start'][:10]} to {invoice['period_end'][:10]}",
                        "Amount": f"${float(invoice['total_amount']):.2f}",
                        "Status": invoice["status"].title(),
                        "Created": invoice["created_at"][:10]
                    })
                
                invoice_df = pd.DataFrame(invoice_data)
                st.dataframe(invoice_df, use_container_width=True)
            else:
                st.info("No invoices available yet")
        
        except Exception as e:
            st.error(f"Unable to load billing information: {str(e)}")
            st.info("Please contact support if this issue persists.")
    
    def _render_usage_history(self, tenant_id):
        """Render usage history"""
        st.subheader("Usage History")
        
        usage_tracker = st.session_state.usage_tracker
        
        # Date range selector
        col1, col2 = st.columns(2)
        
        with col1:
            start_date = st.date_input("Start Date", datetime.now().replace(day=1))
        
        with col2:
            end_date = st.date_input("End Date", datetime.now())
        
        if start_date and end_date:
            usage_data = asyncio.run(usage_tracker.get_tenant_usage(
                tenant_id,
                datetime.combine(start_date, datetime.min.time()),
                datetime.combine(end_date, datetime.max.time())
            ))
            
            # Usage summary
            st.write("**Period Summary:**")
            summary = usage_data["usage_summary"]
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("GB Processed", f"{float(summary.get('gb_processed', 0)):.1f}")
            with col2:
                st.metric("Compute Hours", f"{float(summary.get('compute_hours', 0)):.1f}")
            with col3:
                st.metric("Storage GB", f"{float(summary.get('storage_gb', 0)):.1f}")
            with col4:
                st.metric("Quality Checks", f"{float(summary.get('quality_checks', 0)):,.0f}")
            
            # Daily usage chart
            daily_usage = usage_data.get("daily_usage", {})
            
            if daily_usage:
                st.subheader("Daily Usage Trends")
                
                dates = list(daily_usage.keys())
                gb_processed = [float(daily_usage[date].get('gb_processed', 0)) for date in dates]
                compute_hours = [float(daily_usage[date].get('compute_hours', 0)) for date in dates]
                
                fig = go.Figure()
                fig.add_trace(go.Scatter(x=dates, y=gb_processed, mode='lines+markers', name='GB Processed'))
                fig.add_trace(go.Scatter(x=dates, y=compute_hours, mode='lines+markers', name='Compute Hours', yaxis='y2'))
                
                fig.update_layout(
                    title="Daily Resource Usage",
                    xaxis_title="Date",
                    yaxis=dict(title="GB Processed", side="left"),
                    yaxis2=dict(title="Compute Hours", side="right", overlaying="y")
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
            # Usage by pipeline
            pipeline_usage = usage_data.get("usage_by_pipeline", {})
            
            if pipeline_usage:
                st.subheader("Usage by Pipeline")
                
                pipeline_data = []
                for pipeline_id, metrics in pipeline_usage.items():
                    pipeline_data.append({
                        "Pipeline ID": pipeline_id,
                        "GB Processed": f"{float(metrics.get('gb_processed', 0)):.1f}",
                        "Compute Hours": f"{float(metrics.get('compute_hours', 0)):.1f}",
                        "Quality Checks": f"{float(metrics.get('quality_checks', 0)):,.0f}"
                    })
                
                pipeline_df = pd.DataFrame(pipeline_data)
                st.dataframe(pipeline_df, use_container_width=True)
    
    def _render_settings(self, tenant_id, tenant_config):
        """Render tenant settings"""
        st.header("⚙️ Settings")
        
        tab1, tab2, tab3 = st.tabs(["Quality Thresholds", "Notifications", "Account Info"])
        
        with tab1:
            self._render_quality_settings(tenant_id, tenant_config)
        
        with tab2:
            self._render_notification_settings(tenant_id, tenant_config)
        
        with tab3:
            self._render_account_info(tenant_config)
    
    def _render_quality_settings(self, tenant_id, tenant_config):
        """Render quality threshold settings"""
        st.subheader("Quality Thresholds")
        st.write("Configure data quality thresholds for your pipelines.")
        
        current_thresholds = tenant_config.quality_thresholds
        
        with st.form("quality_thresholds_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                completeness_threshold = st.slider(
                    "Completeness Threshold",
                    0.0, 1.0,
                    current_thresholds.get("completeness_threshold", 0.95),
                    help="Minimum percentage of non-null values required"
                )
                
                validity_threshold = st.slider(
                    "Validity Threshold", 
                    0.0, 1.0,
                    current_thresholds.get("validity_threshold", 0.98),
                    help="Minimum percentage of valid values required"
                )
            
            with col2:
                consistency_threshold = st.slider(
                    "Consistency Threshold",
                    0.0, 1.0,
                    current_thresholds.get("consistency_threshold", 0.90),
                    help="Minimum consistency score across data sources"
                )
                
                timeliness_threshold = st.number_input(
                    "Timeliness Threshold (hours)",
                    0.0, 24.0,
                    current_thresholds.get("timeliness_threshold_hours", 2.0),
                    help="Maximum acceptable data latency in hours"
                )
            
            if st.form_submit_button("Update Thresholds"):
                new_thresholds = {
                    "completeness_threshold": completeness_threshold,
                    "validity_threshold": validity_threshold,
                    "consistency_threshold": consistency_threshold,
                    "timeliness_threshold_hours": timeliness_threshold
                }
                
                try:
                    tenant_manager = st.session_state.tenant_manager
                    asyncio.run(tenant_manager.update_tenant(tenant_id, {
                        "quality_thresholds": new_thresholds
                    }))
                    
                    # Update data plane
                    data_plane_orchestrator = st.session_state.data_plane_orchestrator
                    asyncio.run(data_plane_orchestrator.update_quality_thresholds(
                        tenant_id, new_thresholds
                    ))
                    
                    st.success("Quality thresholds updated successfully!")
                    st.rerun()
                
                except Exception as e:
                    st.error(f"Failed to update thresholds: {str(e)}")
    
    def _render_notification_settings(self, tenant_id, tenant_config):
        """Render notification settings"""
        st.subheader("Notification Settings")
        st.write("Configure how you receive alerts and notifications.")
        
        current_settings = tenant_config.notification_settings
        
        with st.form("notification_settings_form"):
            email_notifications = st.checkbox(
                "Email Notifications",
                current_settings.get("email_notifications", True)
            )
            
            if email_notifications:
                notification_email = st.text_input(
                    "Notification Email",
                    value=current_settings.get("notification_email", ""),
                    placeholder="alerts@yourcompany.com"
                )
            
            slack_notifications = st.checkbox("Slack Notifications", False)
            
            if slack_notifications:
                slack_webhook = st.text_input(
                    "Slack Webhook URL",
                    value=current_settings.get("slack_webhook", ""),
                    placeholder="https://hooks.slack.com/services/..."
                )
            
            pagerduty_integration = st.checkbox("PagerDuty Integration", False)
            
            # Alert types
            st.subheader("Alert Types")
            
            col1, col2 = st.columns(2)
            
            with col1:
                quality_alerts = st.checkbox("Data Quality Alerts", True)
                pipeline_failure_alerts = st.checkbox("Pipeline Failure Alerts", True)
            
            with col2:
                usage_alerts = st.checkbox("Usage Limit Alerts", True)
                billing_alerts = st.checkbox("Billing Alerts", True)
            
            if st.form_submit_button("Update Notification Settings"):
                new_settings = {
                    "email_notifications": email_notifications,
                    "slack_webhook": slack_webhook if slack_notifications else None,
                    "pagerduty_integration": pagerduty_integration,
                    "alert_types": {
                        "quality_alerts": quality_alerts,
                        "pipeline_failure_alerts": pipeline_failure_alerts,
                        "usage_alerts": usage_alerts,
                        "billing_alerts": billing_alerts
                    }
                }
                
                if email_notifications and notification_email:
                    new_settings["notification_email"] = notification_email
                
                try:
                    tenant_manager = st.session_state.tenant_manager
                    asyncio.run(tenant_manager.update_tenant(tenant_id, {
                        "notification_settings": new_settings
                    }))
                    
                    st.success("Notification settings updated successfully!")
                    st.rerun()
                
                except Exception as e:
                    st.error(f"Failed to update settings: {str(e)}")
    
    def _render_account_info(self, tenant_config):
        """Render account information"""
        st.subheader("Account Information")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.info(f"""
            **Organization:** {tenant_config.organization_name}
            **Tenant ID:** {tenant_config.tenant_id}
            **Service Tier:** {tenant_config.tier.value.title()}
            **Isolation Level:** {tenant_config.isolation_level.value.title()}
            """)
        
        with col2:
            st.info(f"""
            **Compliance Level:** {tenant_config.compliance_level}
            **Data Residency:** {tenant_config.data_residency}
            **Account Created:** {tenant_config.created_at.strftime('%Y-%m-%d')}
            **Last Updated:** {tenant_config.updated_at.strftime('%Y-%m-%d')}
            """)
        
        # Contact information
        st.subheader("Billing Contact")
        billing_contact = tenant_config.billing_contact
        
        if billing_contact:
            st.write(f"**Name:** {billing_contact.get('name', 'Not provided')}")
            st.write(f"**Email:** {billing_contact.get('email', 'Not provided')}")
            st.write(f"**Phone:** {billing_contact.get('phone', 'Not provided')}")
            st.write(f"**Address:** {billing_contact.get('address', 'Not provided')}")
        else:
            st.warning("No billing contact information on file")

def main():
    """Main entry point for tenant dashboard"""
    dashboard = TenantDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()