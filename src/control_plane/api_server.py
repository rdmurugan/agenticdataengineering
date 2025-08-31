"""
Control Plane API Server for Multi-tenant SaaS Platform
"""

from fastapi import FastAPI, HTTPException, Depends, status, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
from datetime import datetime
import asyncio
import logging
import uvicorn
from contextlib import asynccontextmanager

from .tenant_manager import TenantManager, TenantConfiguration, TenantTier, IsolationLevel, TenantStatus
from .auth_service import AuthService, TokenData
from .billing_service import BillingService
from .usage_tracker import UsageTracker
from .data_plane_orchestrator import DataPlaneOrchestrator

logger = logging.getLogger(__name__)

# Pydantic models for API requests/responses
class TenantCreateRequest(BaseModel):
    organization_name: str = Field(..., min_length=1, max_length=100)
    tier: TenantTier
    isolation_level: IsolationLevel
    billing_contact: Dict[str, str]
    compliance_level: str = "HIPAA"
    data_residency: str = "US"

class TenantUpdateRequest(BaseModel):
    quality_thresholds: Optional[Dict[str, float]] = None
    notification_settings: Optional[Dict[str, Any]] = None
    billing_contact: Optional[Dict[str, str]] = None

class PipelineCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    source_type: str = Field(..., regex="^(auto_loader|batch|streaming)$")
    source_config: Dict[str, Any]
    quality_config: Dict[str, Any]
    target_config: Dict[str, Any]
    schedule: Optional[str] = None

class PipelineUpdateRequest(BaseModel):
    source_config: Optional[Dict[str, Any]] = None
    quality_config: Optional[Dict[str, Any]] = None
    target_config: Optional[Dict[str, Any]] = None
    schedule: Optional[str] = None
    enabled: Optional[bool] = None

class QualityThresholdUpdateRequest(BaseModel):
    thresholds: Dict[str, float]

# Global services
tenant_manager: Optional[TenantManager] = None
auth_service: Optional[AuthService] = None
billing_service: Optional[BillingService] = None
usage_tracker: Optional[UsageTracker] = None
data_plane_orchestrator: Optional[DataPlaneOrchestrator] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize services on startup"""
    global tenant_manager, auth_service, billing_service, usage_tracker, data_plane_orchestrator
    
    # Initialize services
    tenant_manager = TenantManager()
    auth_service = AuthService()
    billing_service = BillingService()
    usage_tracker = UsageTracker()
    data_plane_orchestrator = DataPlaneOrchestrator()
    
    # Start background tasks
    asyncio.create_task(usage_tracker.start_monitoring())
    asyncio.create_task(billing_service.start_billing_cycle())
    
    logger.info("Control plane services initialized")
    
    yield
    
    # Cleanup
    logger.info("Shutting down control plane services")

# Initialize FastAPI app
app = FastAPI(
    title="Agentic Data Engineering Control Plane",
    description="Multi-tenant SaaS platform for healthcare data processing",
    version="1.0.0",
    lifespan=lifespan
)

# Security
security = HTTPBearer()

# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://*.agenticdata.com", "https://app.agenticdata.com"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

app.add_middleware(
    TrustedHostMiddleware, 
    allowed_hosts=["*.agenticdata.com", "localhost", "127.0.0.1"]
)

# Authentication dependency
async def get_current_tenant(credentials: HTTPAuthorizationCredentials = Depends(security)) -> TokenData:
    """Validate JWT token and extract tenant information"""
    try:
        token_data = await auth_service.verify_token(credentials.credentials)
        return token_data
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid authentication credentials: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )

# Admin authentication (for platform operations)
async def get_admin_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> TokenData:
    """Validate admin JWT token"""
    token_data = await get_current_tenant(credentials)
    if not token_data.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required"
        )
    return token_data

# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

# Tenant Management Endpoints
@app.post("/api/v1/tenants", response_model=Dict[str, Any])
async def create_tenant(
    request: TenantCreateRequest,
    background_tasks: BackgroundTasks,
    admin_user: TokenData = Depends(get_admin_user)
):
    """Create a new tenant (admin only)"""
    try:
        tenant_config = await tenant_manager.create_tenant(
            organization_name=request.organization_name,
            tier=request.tier,
            isolation_level=request.isolation_level,
            billing_contact=request.billing_contact,
            compliance_level=request.compliance_level,
            data_residency=request.data_residency
        )
        
        # Start billing tracking
        background_tasks.add_task(
            billing_service.initialize_tenant_billing, 
            tenant_config.tenant_id
        )
        
        return {
            "tenant_id": tenant_config.tenant_id,
            "status": tenant_config.status.value,
            "organization_name": tenant_config.organization_name,
            "tier": tenant_config.tier.value,
            "created_at": tenant_config.created_at.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to create tenant: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/tenants/{tenant_id}")
async def get_tenant(
    tenant_id: str,
    current_tenant: TokenData = Depends(get_current_tenant)
):
    """Get tenant configuration"""
    # Ensure tenant can only access their own data (unless admin)
    if not current_tenant.is_admin and current_tenant.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    tenant_config = tenant_manager.get_tenant(tenant_id)
    if not tenant_config:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    return {
        "tenant_id": tenant_config.tenant_id,
        "organization_name": tenant_config.organization_name,
        "tier": tenant_config.tier.value,
        "isolation_level": tenant_config.isolation_level.value,
        "status": tenant_config.status.value,
        "features_enabled": tenant_config.features_enabled,
        "quality_thresholds": tenant_config.quality_thresholds,
        "resource_limits": {
            "max_monthly_gb_processed": tenant_config.max_monthly_gb_processed,
            "max_concurrent_pipelines": tenant_config.max_concurrent_pipelines,
            "max_compute_hours_monthly": tenant_config.max_compute_hours_monthly,
            "storage_quota_gb": tenant_config.storage_quota_gb
        },
        "created_at": tenant_config.created_at.isoformat(),
        "updated_at": tenant_config.updated_at.isoformat()
    }

@app.put("/api/v1/tenants/{tenant_id}")
async def update_tenant(
    tenant_id: str,
    request: TenantUpdateRequest,
    current_tenant: TokenData = Depends(get_current_tenant)
):
    """Update tenant configuration"""
    if not current_tenant.is_admin and current_tenant.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        updates = request.dict(exclude_unset=True)
        tenant_config = await tenant_manager.update_tenant(tenant_id, updates)
        
        return {
            "tenant_id": tenant_config.tenant_id,
            "updated_at": tenant_config.updated_at.isoformat(),
            "updated_fields": list(updates.keys())
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to update tenant {tenant_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/tenants")
async def list_tenants(
    status: Optional[TenantStatus] = None,
    admin_user: TokenData = Depends(get_admin_user)
):
    """List all tenants (admin only)"""
    tenants = tenant_manager.list_tenants(status)
    
    return {
        "tenants": [
            {
                "tenant_id": t.tenant_id,
                "organization_name": t.organization_name,
                "tier": t.tier.value,
                "status": t.status.value,
                "created_at": t.created_at.isoformat()
            }
            for t in tenants
        ],
        "total_count": len(tenants)
    }

# Pipeline Management Endpoints
@app.post("/api/v1/tenants/{tenant_id}/pipelines")
async def create_pipeline(
    tenant_id: str,
    request: PipelineCreateRequest,
    background_tasks: BackgroundTasks,
    current_tenant: TokenData = Depends(get_current_tenant)
):
    """Create a new data pipeline for tenant"""
    if current_tenant.tenant_id != tenant_id and not current_tenant.is_admin:
        raise HTTPException(status_code=403, detail="Access denied")
    
    # Verify tenant exists and is active
    tenant_config = tenant_manager.get_tenant(tenant_id)
    if not tenant_config:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    if tenant_config.status != TenantStatus.ACTIVE:
        raise HTTPException(status_code=400, detail="Tenant is not active")
    
    try:
        pipeline_config = await data_plane_orchestrator.create_pipeline(
            tenant_id=tenant_id,
            name=request.name,
            source_type=request.source_type,
            source_config=request.source_config,
            quality_config=request.quality_config,
            target_config=request.target_config,
            schedule=request.schedule
        )
        
        # Track usage
        background_tasks.add_task(
            usage_tracker.track_pipeline_creation,
            tenant_id,
            pipeline_config["pipeline_id"]
        )
        
        return pipeline_config
        
    except Exception as e:
        logger.error(f"Failed to create pipeline for tenant {tenant_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/tenants/{tenant_id}/pipelines")
async def list_pipelines(
    tenant_id: str,
    current_tenant: TokenData = Depends(get_current_tenant)
):
    """List pipelines for tenant"""
    if current_tenant.tenant_id != tenant_id and not current_tenant.is_admin:
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        pipelines = await data_plane_orchestrator.list_pipelines(tenant_id)
        return {"pipelines": pipelines}
        
    except Exception as e:
        logger.error(f"Failed to list pipelines for tenant {tenant_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/tenants/{tenant_id}/pipelines/{pipeline_id}")
async def get_pipeline(
    tenant_id: str,
    pipeline_id: str,
    current_tenant: TokenData = Depends(get_current_tenant)
):
    """Get pipeline details"""
    if current_tenant.tenant_id != tenant_id and not current_tenant.is_admin:
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        pipeline = await data_plane_orchestrator.get_pipeline(tenant_id, pipeline_id)
        if not pipeline:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        return pipeline
        
    except Exception as e:
        logger.error(f"Failed to get pipeline {pipeline_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Usage and Billing Endpoints
@app.get("/api/v1/tenants/{tenant_id}/usage")
async def get_tenant_usage(
    tenant_id: str,
    current_tenant: TokenData = Depends(get_current_tenant)
):
    """Get tenant resource usage"""
    if current_tenant.tenant_id != tenant_id and not current_tenant.is_admin:
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        usage_data = tenant_manager.get_tenant_resource_usage(tenant_id)
        detailed_usage = await usage_tracker.get_tenant_usage(tenant_id)
        
        return {
            **usage_data,
            "detailed_metrics": detailed_usage
        }
        
    except Exception as e:
        logger.error(f"Failed to get usage for tenant {tenant_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/tenants/{tenant_id}/billing")
async def get_tenant_billing(
    tenant_id: str,
    current_tenant: TokenData = Depends(get_current_tenant)
):
    """Get tenant billing information"""
    if current_tenant.tenant_id != tenant_id and not current_tenant.is_admin:
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        billing_data = await billing_service.get_tenant_billing(tenant_id)
        return billing_data
        
    except Exception as e:
        logger.error(f"Failed to get billing for tenant {tenant_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Quality Management Endpoints
@app.put("/api/v1/tenants/{tenant_id}/quality/thresholds")
async def update_quality_thresholds(
    tenant_id: str,
    request: QualityThresholdUpdateRequest,
    current_tenant: TokenData = Depends(get_current_tenant)
):
    """Update quality thresholds for tenant"""
    if current_tenant.tenant_id != tenant_id and not current_tenant.is_admin:
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        await tenant_manager.update_tenant(tenant_id, {
            "quality_thresholds": request.thresholds
        })
        
        # Apply to data plane
        await data_plane_orchestrator.update_quality_thresholds(
            tenant_id, request.thresholds
        )
        
        return {"message": "Quality thresholds updated successfully"}
        
    except Exception as e:
        logger.error(f"Failed to update quality thresholds for tenant {tenant_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/tenants/{tenant_id}/quality/reports")
async def get_quality_reports(
    tenant_id: str,
    days: int = 7,
    current_tenant: TokenData = Depends(get_current_tenant)
):
    """Get quality reports for tenant"""
    if current_tenant.tenant_id != tenant_id and not current_tenant.is_admin:
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        reports = await data_plane_orchestrator.get_quality_reports(tenant_id, days)
        return {"reports": reports}
        
    except Exception as e:
        logger.error(f"Failed to get quality reports for tenant {tenant_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Platform Analytics (Admin only)
@app.get("/api/v1/platform/analytics")
async def get_platform_analytics(admin_user: TokenData = Depends(get_admin_user)):
    """Get platform-wide analytics (admin only)"""
    try:
        analytics = await usage_tracker.get_platform_analytics()
        return analytics
        
    except Exception as e:
        logger.error(f"Failed to get platform analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(
        "control_plane.api_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )