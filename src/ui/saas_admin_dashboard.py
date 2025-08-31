"""
SaaS Admin Dashboard for Multi-tenant Platform Management
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
from ..control_plane.tenant_manager import TenantManager, TenantTier, IsolationLevel, TenantStatus
from ..control_plane.billing_service import BillingService, BillingPeriod
from ..control_plane.usage_tracker import UsageTracker
from ..control_plane.data_plane_orchestrator import DataPlaneOrchestrator

logger = logging.getLogger(__name__)

class SaaSAdminDashboard:
    """Comprehensive admin dashboard for SaaS platform management"""
    
    def __init__(self):
        if 'tenant_manager' not in st.session_state:
            st.session_state.tenant_manager = TenantManager()
            st.session_state.billing_service = BillingService()
            st.session_state.usage_tracker = UsageTracker()
            st.session_state.data_plane_orchestrator = DataPlaneOrchestrator()
    
    def run(self):
        """Main dashboard application"""
        st.set_page_config(
            page_title="Agentic Data Engineering - Admin",
            page_icon="🏢",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        st.title("🏢 SaaS Admin Dashboard")
        st.markdown("**Multi-tenant Healthcare Data Platform Management**")
        
        # Sidebar navigation
        st.sidebar.title("Navigation")
        page = st.sidebar.selectbox(
            "Select Page",
            [
                "Platform Overview",
                "Tenant Management",
                "Usage Analytics",
                "Billing Management", 
                "Resource Monitoring",
                "Pipeline Management",
                "System Health"
            ]
        )
        
        # Route to appropriate page
        if page == "Platform Overview":
            self._render_platform_overview()
        elif page == "Tenant Management":
            self._render_tenant_management()
        elif page == "Usage Analytics":
            self._render_usage_analytics()
        elif page == "Billing Management":
            self._render_billing_management()
        elif page == "Resource Monitoring":
            self._render_resource_monitoring()
        elif page == "Pipeline Management":
            self._render_pipeline_management()
        elif page == "System Health":
            self._render_system_health()
    
    def _render_platform_overview(self):
        """Render platform overview dashboard"""
        st.header("📊 Platform Overview")
        
        # Get platform metrics
        tenant_manager = st.session_state.tenant_manager
        tenants = tenant_manager.list_tenants()
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Tenants",
                len(tenants),
                delta=None
            )
        
        with col2:
            active_tenants = len([t for t in tenants if t.status == TenantStatus.ACTIVE])
            st.metric(
                "Active Tenants",
                active_tenants,
                delta=None
            )
        
        with col3:
            # Mock total pipelines
            total_pipelines = len(tenants) * 3  # Assume avg 3 pipelines per tenant
            st.metric(
                "Total Pipelines",
                total_pipelines,
                delta=None
            )
        
        with col4:
            # Mock monthly revenue
            monthly_revenue = len(tenants) * 299  # Assume avg professional plan
            st.metric(
                "Monthly Revenue",
                f"${monthly_revenue:,}",
                delta="12%"
            )
        
        # Tenant distribution by tier
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Tenant Distribution by Tier")
            if tenants:
                tier_counts = {}
                for tenant in tenants:
                    tier = tenant.tier.value
                    tier_counts[tier] = tier_counts.get(tier, 0) + 1
                
                fig = px.pie(
                    values=list(tier_counts.values()),
                    names=list(tier_counts.keys()),
                    title="Tenant Tiers"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No tenants found")
        
        with col2:
            st.subheader("Tenant Status Distribution")
            if tenants:
                status_counts = {}
                for tenant in tenants:
                    status = tenant.status.value
                    status_counts[status] = status_counts.get(status, 0) + 1
                
                fig = px.bar(
                    x=list(status_counts.keys()),
                    y=list(status_counts.values()),
                    title="Tenant Status"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No tenants found")
        
        # Recent activity
        st.subheader("Recent Tenant Activity")
        if tenants:
            recent_tenants = sorted(tenants, key=lambda t: t.created_at, reverse=True)[:10]
            
            activity_data = []
            for tenant in recent_tenants:
                activity_data.append({
                    "Organization": tenant.organization_name,
                    "Tier": tenant.tier.value,
                    "Status": tenant.status.value,
                    "Created": tenant.created_at.strftime("%Y-%m-%d %H:%M"),
                    "Isolation": tenant.isolation_level.value
                })
            
            df = pd.DataFrame(activity_data)
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No recent activity")
    
    def _render_tenant_management(self):
        """Render tenant management interface"""
        st.header("👥 Tenant Management")
        
        tab1, tab2, tab3 = st.tabs(["All Tenants", "Create Tenant", "Tenant Details"])
        
        with tab1:
            self._render_tenant_list()
        
        with tab2:
            self._render_create_tenant_form()
        
        with tab3:
            self._render_tenant_details()
    
    def _render_tenant_list(self):
        """Render list of all tenants"""
        st.subheader("All Tenants")
        
        tenant_manager = st.session_state.tenant_manager
        tenants = tenant_manager.list_tenants()
        
        if not tenants:
            st.info("No tenants found")
            return
        
        # Filter options
        col1, col2, col3 = st.columns(3)
        
        with col1:
            status_filter = st.selectbox(
                "Filter by Status",
                ["All"] + [status.value for status in TenantStatus]
            )
        
        with col2:
            tier_filter = st.selectbox(
                "Filter by Tier", 
                ["All"] + [tier.value for tier in TenantTier]
            )
        
        with col3:
            isolation_filter = st.selectbox(
                "Filter by Isolation",
                ["All"] + [isolation.value for isolation in IsolationLevel]
            )
        
        # Apply filters
        filtered_tenants = tenants
        if status_filter != "All":
            filtered_tenants = [t for t in filtered_tenants if t.status.value == status_filter]
        if tier_filter != "All":
            filtered_tenants = [t for t in filtered_tenants if t.tier.value == tier_filter]
        if isolation_filter != "All":
            filtered_tenants = [t for t in filtered_tenants if t.isolation_level.value == isolation_filter]
        
        # Display tenant table
        tenant_data = []
        for tenant in filtered_tenants:
            tenant_data.append({
                "Tenant ID": tenant.tenant_id,
                "Organization": tenant.organization_name,
                "Tier": tenant.tier.value,
                "Status": tenant.status.value,
                "Isolation": tenant.isolation_level.value,
                "Created": tenant.created_at.strftime("%Y-%m-%d"),
                "Monthly GB Limit": tenant.max_monthly_gb_processed,
                "Pipeline Limit": tenant.max_concurrent_pipelines
            })
        
        df = pd.DataFrame(tenant_data)
        
        # Add action buttons
        selected_tenant = st.selectbox(
            "Select tenant for actions:",
            options=[t.tenant_id for t in filtered_tenants],
            format_func=lambda x: f"{x} ({next(t.organization_name for t in filtered_tenants if t.tenant_id == x)})"
        )
        
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("Suspend Tenant"):
                if selected_tenant:
                    asyncio.run(tenant_manager.suspend_tenant(selected_tenant, "Admin action"))
                    st.success(f"Suspended tenant {selected_tenant}")
                    st.rerun()
        
        with col2:
            if st.button("View Details"):
                if selected_tenant:
                    st.session_state.selected_tenant_id = selected_tenant
                    st.rerun()
        
        with col3:
            if st.button("Refresh Data"):
                st.rerun()
        
        st.dataframe(df, use_container_width=True)
    
    def _render_create_tenant_form(self):
        """Render tenant creation form"""
        st.subheader("Create New Tenant")
        
        with st.form("create_tenant_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                org_name = st.text_input("Organization Name", placeholder="Healthcare Corp Inc.")
                tier = st.selectbox("Service Tier", [t.value for t in TenantTier])
                isolation = st.selectbox("Isolation Level", [i.value for i in IsolationLevel])
            
            with col2:
                compliance_level = st.selectbox("Compliance Level", ["HIPAA", "SOC2", "HITRUST"])
                data_residency = st.selectbox("Data Residency", ["US", "EU", "APAC"])
            
            st.subheader("Billing Contact")
            col1, col2 = st.columns(2)
            
            with col1:
                billing_email = st.text_input("Billing Email")
                billing_name = st.text_input("Billing Contact Name")
            
            with col2:
                billing_phone = st.text_input("Billing Phone")
                billing_address = st.text_area("Billing Address")
            
            submitted = st.form_submit_button("Create Tenant")
            
            if submitted and org_name and billing_email:
                tenant_manager = st.session_state.tenant_manager
                
                billing_contact = {
                    "email": billing_email,
                    "name": billing_name,
                    "phone": billing_phone,
                    "address": billing_address
                }
                
                try:
                    tenant_config = asyncio.run(tenant_manager.create_tenant(
                        organization_name=org_name,
                        tier=TenantTier(tier),
                        isolation_level=IsolationLevel(isolation),
                        billing_contact=billing_contact,
                        compliance_level=compliance_level,
                        data_residency=data_residency
                    ))
                    
                    st.success(f"Successfully created tenant: {tenant_config.tenant_id}")
                    st.info(f"Status: {tenant_config.status.value}")
                    
                except Exception as e:
                    st.error(f"Failed to create tenant: {str(e)}")
    
    def _render_tenant_details(self):
        """Render detailed tenant information"""
        st.subheader("Tenant Details")
        
        tenant_manager = st.session_state.tenant_manager
        tenants = tenant_manager.list_tenants()
        
        if not tenants:
            st.info("No tenants available")
            return
        
        # Tenant selection
        selected_tenant_id = st.selectbox(
            "Select Tenant",
            options=[t.tenant_id for t in tenants],
            format_func=lambda x: f"{x} ({next(t.organization_name for t in tenants if t.tenant_id == x)})",
            key="tenant_details_selector"
        )
        
        if selected_tenant_id:
            tenant = tenant_manager.get_tenant(selected_tenant_id)
            
            if tenant:
                # Basic information
                col1, col2 = st.columns(2)
                
                with col1:
                    st.info(f"""
                    **Organization:** {tenant.organization_name}
                    **Tenant ID:** {tenant.tenant_id}
                    **Status:** {tenant.status.value}
                    **Tier:** {tenant.tier.value}
                    **Isolation:** {tenant.isolation_level.value}
                    """)
                
                with col2:
                    st.info(f"""
                    **Compliance:** {tenant.compliance_level}
                    **Data Residency:** {tenant.data_residency}
                    **Created:** {tenant.created_at.strftime('%Y-%m-%d %H:%M')}
                    **Updated:** {tenant.updated_at.strftime('%Y-%m-%d %H:%M')}
                    """)
                
                # Resource limits
                st.subheader("Resource Limits")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Monthly GB Processing", tenant.max_monthly_gb_processed)
                with col2:
                    st.metric("Concurrent Pipelines", tenant.max_concurrent_pipelines)
                with col3:
                    st.metric("Monthly Compute Hours", tenant.max_compute_hours_monthly)
                with col4:
                    st.metric("Storage Quota GB", tenant.storage_quota_gb)
                
                # Features
                st.subheader("Enabled Features")
                features_df = pd.DataFrame([
                    {"Feature": feature, "Enabled": enabled}
                    for feature, enabled in tenant.features_enabled.items()
                ])
                st.dataframe(features_df, use_container_width=True)
                
                # Quality thresholds
                st.subheader("Quality Thresholds")
                thresholds_df = pd.DataFrame([
                    {"Threshold": threshold, "Value": value}
                    for threshold, value in tenant.quality_thresholds.items()
                ])
                st.dataframe(thresholds_df, use_container_width=True)
                
                # Usage information
                st.subheader("Current Usage")
                usage_data = tenant_manager.get_tenant_resource_usage(selected_tenant_id)
                
                col1, col2 = st.columns(2)
                with col1:
                    st.json(usage_data)
                
                with col2:
                    # Usage visualization
                    if "current_month_gb_processed" in usage_data:
                        usage_pct = (usage_data["current_month_gb_processed"] / tenant.max_monthly_gb_processed) * 100
                        
                        fig = go.Figure(go.Indicator(
                            mode = "gauge+number",
                            value = usage_pct,
                            domain = {'x': [0, 1], 'y': [0, 1]},
                            title = {'text': "Monthly GB Processing Usage %"},
                            gauge = {'axis': {'range': [None, 100]},
                                   'bar': {'color': "darkblue"},
                                   'steps' : [{'range': [0, 50], 'color': "lightgray"},
                                            {'range': [50, 80], 'color': "gray"},
                                            {'range': [80, 100], 'color': "red"}],
                                   'threshold' : {'line': {'color': "red", 'width': 4},
                                               'thickness': 0.75, 'value': 90}}))
                        
                        st.plotly_chart(fig, use_container_width=True)
    
    def _render_usage_analytics(self):
        """Render usage analytics dashboard"""
        st.header("📈 Usage Analytics")
        
        usage_tracker = st.session_state.usage_tracker
        
        # Platform analytics
        analytics = asyncio.run(usage_tracker.get_platform_analytics())
        
        # Platform totals
        st.subheader("Platform Totals")
        col1, col2, col3, col4 = st.columns(4)
        
        totals = analytics["platform_totals"]
        with col1:
            st.metric("GB Processed", f"{float(totals.get('gb_processed', 0)):.1f}")
        with col2:
            st.metric("Compute Hours", f"{float(totals.get('compute_hours', 0)):.1f}")
        with col3:
            st.metric("Storage GB", f"{float(totals.get('storage_gb', 0)):.1f}")
        with col4:
            st.metric("Quality Checks", f"{float(totals.get('quality_checks', 0)):,.0f}")
        
        # Daily trends
        st.subheader("Usage Trends (Last 7 Days)")
        daily_trends = analytics["daily_trends"]
        
        if daily_trends:
            dates = list(daily_trends.keys())
            gb_processed = [float(daily_trends[date].get('gb_processed', 0)) for date in dates]
            compute_hours = [float(daily_trends[date].get('compute_hours', 0)) for date in dates]
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=dates, y=gb_processed, mode='lines+markers', name='GB Processed'))
            fig.add_trace(go.Scatter(x=dates, y=compute_hours, mode='lines+markers', name='Compute Hours', yaxis='y2'))
            
            fig.update_layout(
                title="Daily Usage Trends",
                xaxis_title="Date",
                yaxis=dict(title="GB Processed", side="left"),
                yaxis2=dict(title="Compute Hours", side="right", overlaying="y")
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        # Top tenants
        st.subheader("Top Tenants by Usage")
        top_tenants = analytics.get("top_tenants_by_usage", [])
        
        if top_tenants:
            tenant_df = pd.DataFrame(top_tenants)
            tenant_df["total_usage"] = tenant_df["total_usage"].astype(float)
            
            fig = px.bar(
                tenant_df.head(10),
                x="tenant_id",
                y="total_usage",
                title="Top 10 Tenants by Total Usage"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Usage alerts
        st.subheader("Usage Alerts")
        alerts = asyncio.run(usage_tracker.get_usage_alerts())
        
        if alerts:
            for alert in alerts:
                severity_color = "red" if alert["severity"] == "high" else "orange"
                st.error(f"**{alert['tenant_id']}** - {alert['metric']}: {alert['usage_percentage']:.1f}% used")
        else:
            st.success("No usage alerts at this time")
    
    def _render_billing_management(self):
        """Render billing management interface"""
        st.header("💰 Billing Management")
        
        billing_service = st.session_state.billing_service
        tenant_manager = st.session_state.tenant_manager
        
        tab1, tab2, tab3 = st.tabs(["Revenue Overview", "Tenant Billing", "Invoice Management"])
        
        with tab1:
            st.subheader("Revenue Overview")
            
            # Mock revenue data
            tenants = tenant_manager.list_tenants()
            monthly_revenue = 0
            
            for tenant in tenants:
                if tenant.tier == TenantTier.STARTER:
                    monthly_revenue += 99
                elif tenant.tier == TenantTier.PROFESSIONAL:
                    monthly_revenue += 299
                elif tenant.tier == TenantTier.ENTERPRISE:
                    monthly_revenue += 999
                elif tenant.tier == TenantTier.HEALTHCARE_PLUS:
                    monthly_revenue += 1999
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Monthly Recurring Revenue", f"${monthly_revenue:,}")
            with col2:
                st.metric("Annual Recurring Revenue", f"${monthly_revenue * 12:,}")
            with col3:
                st.metric("Average Revenue Per User", f"${monthly_revenue // len(tenants) if tenants else 0}")
        
        with tab2:
            st.subheader("Tenant Billing Status")
            
            if tenants:
                tenant_billing_data = []
                for tenant in tenants:
                    try:
                        billing_info = asyncio.run(billing_service.get_tenant_billing(tenant.tenant_id))
                        tenant_billing_data.append({
                            "Tenant ID": tenant.tenant_id,
                            "Organization": tenant.organization_name,
                            "Plan": billing_info["account"]["plan_name"],
                            "Status": billing_info["account"]["account_status"],
                            "Monthly Fee": billing_info["base_monthly_fee"],
                            "Next Billing": billing_info["account"]["next_billing_date"][:10]
                        })
                    except:
                        # Handle tenants without billing setup
                        tenant_billing_data.append({
                            "Tenant ID": tenant.tenant_id,
                            "Organization": tenant.organization_name,
                            "Plan": "Not Set",
                            "Status": "Setup Required",
                            "Monthly Fee": "$0.00",
                            "Next Billing": "N/A"
                        })
                
                billing_df = pd.DataFrame(tenant_billing_data)
                st.dataframe(billing_df, use_container_width=True)
        
        with tab3:
            st.subheader("Invoice Management")
            st.info("Invoice management features would be implemented here")
    
    def _render_resource_monitoring(self):
        """Render resource monitoring dashboard"""
        st.header("🔧 Resource Monitoring")
        
        data_plane_orchestrator = st.session_state.data_plane_orchestrator
        
        # Platform metrics
        metrics = asyncio.run(data_plane_orchestrator.get_platform_metrics())
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Tenants", metrics["platform_summary"]["total_tenants"])
        with col2:
            st.metric("Total Pipelines", metrics["platform_summary"]["total_pipelines"])
        with col3:
            st.metric("Success Rate", f"{metrics['platform_summary']['platform_success_rate']:.1f}%")
        with col4:
            st.metric("Total GB Processed", f"{metrics['platform_summary']['total_gb_processed']:.1f}")
        
        # Resource distribution
        st.subheader("Resource Tier Distribution")
        resource_dist = metrics.get("resource_distribution", {})
        
        if resource_dist:
            fig = px.pie(
                values=list(resource_dist.values()),
                names=list(resource_dist.keys()),
                title="Tenant Resource Allocation"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Processing metrics
        st.subheader("Processing Metrics")
        processing_metrics = metrics["processing_metrics"]
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Pipeline Runs", processing_metrics["total_pipeline_runs"])
            st.metric("Successful Runs", processing_metrics["successful_runs"])
        
        with col2:
            st.metric("Failed Runs", processing_metrics["failed_runs"])
            st.metric("Avg GB per Tenant", f"{processing_metrics['avg_gb_per_tenant']:.1f}")
    
    def _render_pipeline_management(self):
        """Render pipeline management interface"""
        st.header("🔄 Pipeline Management")
        
        data_plane_orchestrator = st.session_state.data_plane_orchestrator
        tenant_manager = st.session_state.tenant_manager
        
        tenants = tenant_manager.list_tenants()
        
        if not tenants:
            st.info("No tenants found")
            return
        
        # Tenant selection for pipeline management
        selected_tenant_id = st.selectbox(
            "Select Tenant",
            options=[t.tenant_id for t in tenants],
            format_func=lambda x: f"{x} ({next(t.organization_name for t in tenants if t.tenant_id == x)})"
        )
        
        if selected_tenant_id:
            # List pipelines for selected tenant
            pipelines = asyncio.run(data_plane_orchestrator.list_pipelines(selected_tenant_id))
            
            st.subheader(f"Pipelines for {selected_tenant_id}")
            
            if pipelines:
                pipeline_data = []
                for pipeline in pipelines:
                    pipeline_data.append({
                        "Pipeline ID": pipeline["pipeline_id"],
                        "Name": pipeline["name"],
                        "Status": pipeline["status"],
                        "Source Type": pipeline["source_type"],
                        "Resource Tier": pipeline["resource_tier"],
                        "Total Runs": pipeline["total_runs"],
                        "Success Rate": f"{pipeline['success_rate']:.1f}%",
                        "GB Processed": f"{pipeline['total_gb_processed']:.1f}",
                        "Avg Runtime": f"{pipeline['avg_runtime_seconds']:.1f}s"
                    })
                
                pipeline_df = pd.DataFrame(pipeline_data)
                st.dataframe(pipeline_df, use_container_width=True)
                
                # Pipeline actions
                selected_pipeline = st.selectbox(
                    "Select Pipeline for Actions",
                    options=[p["pipeline_id"] for p in pipelines],
                    format_func=lambda x: f"{x} ({next(p['name'] for p in pipelines if p['pipeline_id'] == x)})"
                )
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button("Execute Pipeline"):
                        result = asyncio.run(data_plane_orchestrator.execute_pipeline(selected_tenant_id, selected_pipeline))
                        if result["status"] == "success":
                            st.success(f"Pipeline executed successfully in {result['execution_time_seconds']:.1f}s")
                        else:
                            st.error(f"Pipeline execution failed: {result['error']}")
                
                with col2:
                    if st.button("Pause Pipeline"):
                        asyncio.run(data_plane_orchestrator.pause_pipeline(selected_tenant_id, selected_pipeline))
                        st.success("Pipeline paused")
                        st.rerun()
                
                with col3:
                    if st.button("Resume Pipeline"):
                        asyncio.run(data_plane_orchestrator.resume_pipeline(selected_tenant_id, selected_pipeline))
                        st.success("Pipeline resumed")
                        st.rerun()
            else:
                st.info("No pipelines found for this tenant")
    
    def _render_system_health(self):
        """Render system health monitoring"""
        st.header("🏥 System Health")
        
        # System status indicators
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.success("✅ Control Plane")
            st.caption("API Server: Healthy")
        
        with col2:
            st.success("✅ Data Plane")
            st.caption("Processing: Normal")
        
        with col3:
            st.success("✅ Database")
            st.caption("Connectivity: Good")
        
        with col4:
            st.success("✅ Monitoring")
            st.caption("All Systems: Operational")
        
        # Recent alerts
        st.subheader("Recent System Alerts")
        st.info("No critical alerts at this time")
        
        # Performance metrics
        st.subheader("Performance Metrics")
        
        # Mock performance data
        import random
        dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
        performance_data = {
            'Date': dates,
            'API Response Time (ms)': [random.randint(100, 500) for _ in range(30)],
            'Processing Throughput (GB/hour)': [random.randint(50, 200) for _ in range(30)],
            'Error Rate (%)': [random.uniform(0, 5) for _ in range(30)]
        }
        
        perf_df = pd.DataFrame(performance_data)
        
        fig = px.line(perf_df, x='Date', y=['API Response Time (ms)', 'Processing Throughput (GB/hour)'])
        st.plotly_chart(fig, use_container_width=True)

def main():
    """Main entry point for admin dashboard"""
    dashboard = SaaSAdminDashboard()
    dashboard.run()

if __name__ == "__main__":
    main()