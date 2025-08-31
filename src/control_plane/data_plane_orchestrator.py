"""
Control Plane Interface to Data Plane Operations
"""

from typing import Dict, List, Optional, Any
import asyncio
import logging
from datetime import datetime

from ..data_plane.tenant_orchestrator import (
    data_plane_orchestrator, 
    ResourceTier, 
    TenantPipeline
)

logger = logging.getLogger(__name__)

class DataPlaneOrchestrator:
    """Control plane interface for managing data plane operations"""
    
    def __init__(self):
        self.data_plane = data_plane_orchestrator
    
    async def create_pipeline(self,
                            tenant_id: str,
                            name: str,
                            source_type: str,
                            source_config: Dict[str, Any],
                            quality_config: Dict[str, Any],
                            target_config: Dict[str, Any],
                            schedule: Optional[str] = None) -> Dict[str, Any]:
        """Create a new pipeline via data plane"""
        
        # Ensure tenant resources are allocated
        await self._ensure_tenant_resources(tenant_id)
        
        pipeline = await self.data_plane.create_pipeline(
            tenant_id=tenant_id,
            name=name,
            source_type=source_type,
            source_config=source_config,
            quality_config=quality_config,
            target_config=target_config,
            schedule=schedule
        )
        
        return {
            "pipeline_id": pipeline.pipeline_id,
            "tenant_id": pipeline.tenant_id,
            "name": pipeline.name,
            "status": pipeline.status.value,
            "created_at": pipeline.created_at.isoformat(),
            "resource_tier": pipeline.resource_tier.value
        }
    
    async def _ensure_tenant_resources(self, tenant_id: str):
        """Ensure tenant has allocated resources in data plane"""
        if tenant_id not in self.data_plane.tenant_resources:
            # Default to shared resources for new tenants
            await self.data_plane.allocate_tenant_resources(
                tenant_id=tenant_id,
                resource_tier=ResourceTier.SHARED
            )
            logger.info(f"Auto-allocated shared resources for tenant {tenant_id}")
    
    async def list_pipelines(self, tenant_id: str) -> List[Dict[str, Any]]:
        """List pipelines for tenant"""
        return await self.data_plane.list_tenant_pipelines(tenant_id)
    
    async def get_pipeline(self, tenant_id: str, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """Get pipeline details"""
        pipeline = self.data_plane._get_tenant_pipeline(tenant_id, pipeline_id)
        if not pipeline:
            return None
        
        return {
            "pipeline_id": pipeline.pipeline_id,
            "tenant_id": pipeline.tenant_id,
            "name": pipeline.name,
            "status": pipeline.status.value,
            "source_type": pipeline.source_type,
            "source_config": pipeline.source_config,
            "quality_config": pipeline.quality_config,
            "target_config": pipeline.target_config,
            "schedule": pipeline.schedule,
            "resource_tier": pipeline.resource_tier.value,
            "compute_cluster_id": pipeline.compute_cluster_id,
            "storage_location": pipeline.storage_location,
            "created_at": pipeline.created_at.isoformat(),
            "updated_at": pipeline.updated_at.isoformat(),
            "last_run_at": pipeline.last_run_at.isoformat() if pipeline.last_run_at else None,
            "next_run_at": pipeline.next_run_at.isoformat() if pipeline.next_run_at else None,
            "statistics": {
                "total_runs": pipeline.total_runs,
                "successful_runs": pipeline.successful_runs,
                "failed_runs": pipeline.failed_runs,
                "success_rate": (pipeline.successful_runs / pipeline.total_runs * 100) if pipeline.total_runs > 0 else 0,
                "total_gb_processed": pipeline.total_gb_processed,
                "avg_runtime_seconds": pipeline.avg_runtime_seconds
            }
        }
    
    async def execute_pipeline(self, tenant_id: str, pipeline_id: str) -> Dict[str, Any]:
        """Execute pipeline"""
        return await self.data_plane.execute_pipeline(tenant_id, pipeline_id)
    
    async def update_quality_thresholds(self, tenant_id: str, thresholds: Dict[str, float]):
        """Update quality thresholds for all tenant pipelines"""
        if tenant_id in self.data_plane.tenant_agents:
            quality_engine = self.data_plane.tenant_agents[tenant_id]["quality_engine"]
            await quality_engine.update_tenant_thresholds(thresholds)
            logger.info(f"Updated quality thresholds for tenant {tenant_id}")
    
    async def get_quality_reports(self, tenant_id: str, days: int = 7) -> List[Dict[str, Any]]:
        """Get quality reports for tenant"""
        # This would integrate with the quality reporting system
        # For now, return mock data structure
        
        pipelines = await self.data_plane.list_tenant_pipelines(tenant_id)
        
        reports = []
        for pipeline in pipelines:
            if pipeline["total_runs"] > 0:
                reports.append({
                    "pipeline_id": pipeline["pipeline_id"],
                    "pipeline_name": pipeline["name"],
                    "period_days": days,
                    "total_quality_checks": pipeline["total_runs"] * 10,  # Estimate
                    "quality_score": pipeline["success_rate"] / 100,
                    "anomalies_detected": pipeline["failed_runs"],
                    "data_processed_gb": pipeline["total_gb_processed"],
                    "generated_at": datetime.utcnow().isoformat()
                })
        
        return reports
    
    async def get_tenant_resource_usage(self, tenant_id: str) -> Dict[str, Any]:
        """Get tenant resource usage from data plane"""
        return await self.data_plane.get_tenant_resource_usage(tenant_id)
    
    async def scale_tenant_resources(self, tenant_id: str, new_tier: str):
        """Scale tenant resources"""
        resource_tier = ResourceTier(new_tier)
        await self.data_plane.scale_tenant_resources(tenant_id, resource_tier)
    
    async def pause_pipeline(self, tenant_id: str, pipeline_id: str):
        """Pause pipeline execution"""
        pipeline = self.data_plane._get_tenant_pipeline(tenant_id, pipeline_id)
        if pipeline:
            pipeline.status = self.data_plane.PipelineStatus.PAUSED
            pipeline.updated_at = datetime.utcnow()
            self.data_plane._save_tenant_data()
            logger.info(f"Paused pipeline {pipeline_id} for tenant {tenant_id}")
    
    async def resume_pipeline(self, tenant_id: str, pipeline_id: str):
        """Resume pipeline execution"""
        pipeline = self.data_plane._get_tenant_pipeline(tenant_id, pipeline_id)
        if pipeline:
            pipeline.status = self.data_plane.PipelineStatus.ACTIVE
            pipeline.updated_at = datetime.utcnow()
            self.data_plane._save_tenant_data()
            logger.info(f"Resumed pipeline {pipeline_id} for tenant {tenant_id}")
    
    async def delete_pipeline(self, tenant_id: str, pipeline_id: str):
        """Delete pipeline"""
        tenant_pipelines = self.data_plane.tenant_pipelines.get(tenant_id, [])
        pipeline_to_remove = None
        
        for pipeline in tenant_pipelines:
            if pipeline.pipeline_id == pipeline_id:
                pipeline_to_remove = pipeline
                break
        
        if pipeline_to_remove:
            tenant_pipelines.remove(pipeline_to_remove)
            self.data_plane._save_tenant_data()
            
            # Clean up resources if it was the last pipeline
            if not tenant_pipelines:
                logger.info(f"Removed last pipeline for tenant {tenant_id}")
            
            logger.info(f"Deleted pipeline {pipeline_id} for tenant {tenant_id}")
    
    async def get_platform_metrics(self) -> Dict[str, Any]:
        """Get platform-wide metrics for admin dashboard"""
        total_tenants = len(self.data_plane.tenant_resources)
        total_pipelines = sum(len(pipelines) for pipelines in self.data_plane.tenant_pipelines.values())
        
        # Calculate resource utilization
        resource_distribution = {}
        for tenant_id, resource_allocation in self.data_plane.tenant_resources.items():
            tier = resource_allocation.resource_tier.value
            resource_distribution[tier] = resource_distribution.get(tier, 0) + 1
        
        # Calculate total processing volume
        total_gb_processed = 0
        total_runs = 0
        successful_runs = 0
        
        for pipelines in self.data_plane.tenant_pipelines.values():
            for pipeline in pipelines:
                total_gb_processed += pipeline.total_gb_processed
                total_runs += pipeline.total_runs
                successful_runs += pipeline.successful_runs
        
        success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0
        
        return {
            "platform_summary": {
                "total_tenants": total_tenants,
                "total_pipelines": total_pipelines,
                "total_gb_processed": total_gb_processed,
                "total_runs": total_runs,
                "platform_success_rate": success_rate,
                "generated_at": datetime.utcnow().isoformat()
            },
            "resource_distribution": resource_distribution,
            "processing_metrics": {
                "total_gb_processed": total_gb_processed,
                "avg_gb_per_tenant": total_gb_processed / total_tenants if total_tenants > 0 else 0,
                "total_pipeline_runs": total_runs,
                "successful_runs": successful_runs,
                "failed_runs": total_runs - successful_runs
            }
        }