"""
Tenant-Isolated Data Plane Orchestrator
Manages tenant-specific processing resources and workload isolation
"""

import asyncio
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
import json
import logging
from pathlib import Path

# Import existing agents (now made tenant-aware)
from ..agents.ingestion.auto_loader import AgenticAutoLoader
from ..agents.quality.quality_engine import QualityEngine
from ..agents.orchestration.orchestrator import AgenticOrchestrator
from ..databricks_hooks.unity_catalog_manager import UnityCatalogManager

logger = logging.getLogger(__name__)

class PipelineStatus(Enum):
    """Pipeline execution status"""
    CREATING = "creating"
    ACTIVE = "active"
    PAUSED = "paused"
    FAILED = "failed"
    TERMINATED = "terminated"

class ResourceTier(Enum):
    """Resource allocation tiers"""
    SHARED = "shared"
    DEDICATED_SMALL = "dedicated_small"
    DEDICATED_MEDIUM = "dedicated_medium"
    DEDICATED_LARGE = "dedicated_large"

@dataclass
class TenantPipeline:
    """Tenant-specific pipeline configuration"""
    pipeline_id: str
    tenant_id: str
    name: str
    status: PipelineStatus
    
    # Pipeline configuration
    source_type: str
    source_config: Dict[str, Any]
    quality_config: Dict[str, Any]
    target_config: Dict[str, Any]
    schedule: Optional[str]
    
    # Resource allocation
    resource_tier: ResourceTier
    compute_cluster_id: Optional[str]
    storage_location: str
    
    # Monitoring
    created_at: datetime
    updated_at: datetime
    last_run_at: Optional[datetime]
    next_run_at: Optional[datetime]
    
    # Statistics
    total_runs: int
    successful_runs: int
    failed_runs: int
    total_gb_processed: float
    avg_runtime_seconds: float

@dataclass
class TenantResourceAllocation:
    """Resource allocation for tenant"""
    tenant_id: str
    resource_tier: ResourceTier
    
    # Compute resources
    max_concurrent_pipelines: int
    cluster_configurations: Dict[str, Any]
    
    # Storage resources
    storage_quota_gb: int
    data_retention_days: int
    
    # Network isolation
    vpc_id: Optional[str]
    subnet_ids: List[str]
    security_group_ids: List[str]
    
    # Quality processing limits
    max_quality_checks_per_hour: int
    
    # Monitoring
    allocated_at: datetime
    last_scaled_at: Optional[datetime]

class TenantDataPlaneOrchestrator:
    """Orchestrates tenant-isolated data processing"""
    
    def __init__(self, storage_path: str = "data/data_plane"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # Tenant data
        self.tenant_pipelines: Dict[str, List[TenantPipeline]] = {}  # tenant_id -> pipelines
        self.tenant_resources: Dict[str, TenantResourceAllocation] = {}
        
        # Agent instances per tenant (for isolation)
        self.tenant_agents: Dict[str, Dict[str, Any]] = {}
        
        # Resource tier configurations
        self.resource_tiers = self._initialize_resource_tiers()
        
        # Load existing configurations
        self._load_tenant_data()
    
    def _initialize_resource_tiers(self) -> Dict[ResourceTier, Dict[str, Any]]:
        """Initialize resource tier configurations"""
        return {
            ResourceTier.SHARED: {
                "max_concurrent_pipelines": 3,
                "cluster_config": {
                    "cluster_type": "shared",
                    "min_workers": 1,
                    "max_workers": 3,
                    "instance_type": "Standard_DS3_v2"
                },
                "storage_quota_gb": 500,
                "max_quality_checks_per_hour": 1000
            },
            
            ResourceTier.DEDICATED_SMALL: {
                "max_concurrent_pipelines": 10,
                "cluster_config": {
                    "cluster_type": "dedicated",
                    "min_workers": 2,
                    "max_workers": 8,
                    "instance_type": "Standard_DS4_v2"
                },
                "storage_quota_gb": 2000,
                "max_quality_checks_per_hour": 5000
            },
            
            ResourceTier.DEDICATED_MEDIUM: {
                "max_concurrent_pipelines": 25,
                "cluster_config": {
                    "cluster_type": "dedicated",
                    "min_workers": 4,
                    "max_workers": 16,
                    "instance_type": "Standard_DS5_v2"
                },
                "storage_quota_gb": 10000,
                "max_quality_checks_per_hour": 25000
            },
            
            ResourceTier.DEDICATED_LARGE: {
                "max_concurrent_pipelines": 100,
                "cluster_config": {
                    "cluster_type": "dedicated",
                    "min_workers": 8,
                    "max_workers": 64,
                    "instance_type": "Standard_DS6_v2"
                },
                "storage_quota_gb": 50000,
                "max_quality_checks_per_hour": 100000
            }
        }
    
    def _load_tenant_data(self):
        """Load existing tenant pipeline and resource data"""
        # Load pipelines
        pipelines_file = self.storage_path / "tenant_pipelines.json"
        if pipelines_file.exists():
            try:
                with open(pipelines_file, 'r') as f:
                    data = json.load(f)
                    for tenant_id, pipelines_data in data.items():
                        pipelines = []
                        for pipeline_data in pipelines_data:
                            # Convert datetime strings and enums
                            pipeline_data['created_at'] = datetime.fromisoformat(pipeline_data['created_at'])
                            pipeline_data['updated_at'] = datetime.fromisoformat(pipeline_data['updated_at'])
                            if pipeline_data['last_run_at']:
                                pipeline_data['last_run_at'] = datetime.fromisoformat(pipeline_data['last_run_at'])
                            if pipeline_data['next_run_at']:
                                pipeline_data['next_run_at'] = datetime.fromisoformat(pipeline_data['next_run_at'])
                            
                            pipeline_data['status'] = PipelineStatus(pipeline_data['status'])
                            pipeline_data['resource_tier'] = ResourceTier(pipeline_data['resource_tier'])
                            
                            pipelines.append(TenantPipeline(**pipeline_data))
                        
                        self.tenant_pipelines[tenant_id] = pipelines
                        
            except Exception as e:
                logger.error(f"Failed to load tenant pipelines: {e}")
        
        # Load resource allocations
        resources_file = self.storage_path / "tenant_resources.json"
        if resources_file.exists():
            try:
                with open(resources_file, 'r') as f:
                    data = json.load(f)
                    for tenant_id, resource_data in data.items():
                        # Convert datetime strings and enums
                        resource_data['allocated_at'] = datetime.fromisoformat(resource_data['allocated_at'])
                        if resource_data['last_scaled_at']:
                            resource_data['last_scaled_at'] = datetime.fromisoformat(resource_data['last_scaled_at'])
                        
                        resource_data['resource_tier'] = ResourceTier(resource_data['resource_tier'])
                        
                        self.tenant_resources[tenant_id] = TenantResourceAllocation(**resource_data)
                        
            except Exception as e:
                logger.error(f"Failed to load tenant resources: {e}")
    
    def _save_tenant_data(self):
        """Persist tenant data to storage"""
        # Save pipelines
        pipelines_data = {}
        for tenant_id, pipelines in self.tenant_pipelines.items():
            pipelines_data[tenant_id] = []
            for pipeline in pipelines:
                pipeline_dict = asdict(pipeline)
                # Convert datetime objects and enums
                pipeline_dict['created_at'] = pipeline.created_at.isoformat()
                pipeline_dict['updated_at'] = pipeline.updated_at.isoformat()
                if pipeline.last_run_at:
                    pipeline_dict['last_run_at'] = pipeline.last_run_at.isoformat()
                if pipeline.next_run_at:
                    pipeline_dict['next_run_at'] = pipeline.next_run_at.isoformat()
                
                pipeline_dict['status'] = pipeline.status.value
                pipeline_dict['resource_tier'] = pipeline.resource_tier.value
                
                pipelines_data[tenant_id].append(pipeline_dict)
        
        with open(self.storage_path / "tenant_pipelines.json", 'w') as f:
            json.dump(pipelines_data, f, indent=2)
        
        # Save resource allocations
        resources_data = {}
        for tenant_id, resource in self.tenant_resources.items():
            resource_dict = asdict(resource)
            # Convert datetime objects and enums
            resource_dict['allocated_at'] = resource.allocated_at.isoformat()
            if resource.last_scaled_at:
                resource_dict['last_scaled_at'] = resource.last_scaled_at.isoformat()
            
            resource_dict['resource_tier'] = resource.resource_tier.value
            
            resources_data[tenant_id] = resource_dict
        
        with open(self.storage_path / "tenant_resources.json", 'w') as f:
            json.dump(resources_data, f, indent=2)
    
    async def allocate_tenant_resources(self, 
                                      tenant_id: str,
                                      resource_tier: ResourceTier,
                                      isolation_config: Optional[Dict[str, Any]] = None) -> TenantResourceAllocation:
        """Allocate compute and storage resources for tenant"""
        
        tier_config = self.resource_tiers[resource_tier]
        
        # Create tenant-specific storage location
        storage_location = f"/mnt/tenants/{tenant_id}"
        
        # Network isolation configuration
        network_config = isolation_config or {}
        
        resource_allocation = TenantResourceAllocation(
            tenant_id=tenant_id,
            resource_tier=resource_tier,
            
            max_concurrent_pipelines=tier_config["max_concurrent_pipelines"],
            cluster_configurations=tier_config["cluster_config"],
            
            storage_quota_gb=tier_config["storage_quota_gb"],
            data_retention_days=90,  # Default retention
            
            vpc_id=network_config.get("vpc_id"),
            subnet_ids=network_config.get("subnet_ids", []),
            security_group_ids=network_config.get("security_group_ids", []),
            
            max_quality_checks_per_hour=tier_config["max_quality_checks_per_hour"],
            
            allocated_at=datetime.utcnow(),
            last_scaled_at=None
        )
        
        self.tenant_resources[tenant_id] = resource_allocation
        self._save_tenant_data()
        
        # Initialize tenant-specific agent instances
        await self._initialize_tenant_agents(tenant_id, resource_allocation)
        
        logger.info(f"Allocated {resource_tier.value} resources for tenant {tenant_id}")
        return resource_allocation
    
    async def _initialize_tenant_agents(self, tenant_id: str, resource_allocation: TenantResourceAllocation):
        """Initialize tenant-specific agent instances"""
        
        # Create tenant-specific configurations
        tenant_config = {
            "tenant_id": tenant_id,
            "storage_location": f"/mnt/tenants/{tenant_id}",
            "catalog_name": f"tenant_{tenant_id}",
            "resource_limits": {
                "max_concurrent_jobs": resource_allocation.max_concurrent_pipelines,
                "storage_quota_gb": resource_allocation.storage_quota_gb
            }
        }
        
        # Initialize agents with tenant isolation
        agents = {
            "auto_loader": AgenticAutoLoader(tenant_config),
            "quality_engine": QualityEngine(tenant_config), 
            "orchestrator": AgenticOrchestrator(tenant_config),
            "unity_catalog": UnityCatalogManager(tenant_config)
        }
        
        self.tenant_agents[tenant_id] = agents
        
        logger.info(f"Initialized isolated agents for tenant {tenant_id}")
    
    async def create_pipeline(self,
                            tenant_id: str,
                            name: str,
                            source_type: str,
                            source_config: Dict[str, Any],
                            quality_config: Dict[str, Any],
                            target_config: Dict[str, Any],
                            schedule: Optional[str] = None) -> TenantPipeline:
        """Create a new tenant-isolated pipeline"""
        
        # Verify tenant has allocated resources
        if tenant_id not in self.tenant_resources:
            raise ValueError(f"No resources allocated for tenant {tenant_id}")
        
        resource_allocation = self.tenant_resources[tenant_id]
        
        # Check pipeline limits
        existing_pipelines = self.tenant_pipelines.get(tenant_id, [])
        active_pipelines = [p for p in existing_pipelines if p.status == PipelineStatus.ACTIVE]
        
        if len(active_pipelines) >= resource_allocation.max_concurrent_pipelines:
            raise ValueError(f"Tenant {tenant_id} has reached pipeline limit of {resource_allocation.max_concurrent_pipelines}")
        
        pipeline_id = f"pipeline_{tenant_id}_{uuid.uuid4().hex[:8]}"
        
        # Create tenant-specific storage path
        storage_location = f"{resource_allocation.tenant_id}/pipelines/{pipeline_id}"
        
        pipeline = TenantPipeline(
            pipeline_id=pipeline_id,
            tenant_id=tenant_id,
            name=name,
            status=PipelineStatus.CREATING,
            
            source_type=source_type,
            source_config=source_config,
            quality_config=quality_config,
            target_config=target_config,
            schedule=schedule,
            
            resource_tier=resource_allocation.resource_tier,
            compute_cluster_id=None,  # Will be assigned during activation
            storage_location=storage_location,
            
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            last_run_at=None,
            next_run_at=None,
            
            total_runs=0,
            successful_runs=0,
            failed_runs=0,
            total_gb_processed=0.0,
            avg_runtime_seconds=0.0
        )
        
        # Add to tenant's pipeline list
        if tenant_id not in self.tenant_pipelines:
            self.tenant_pipelines[tenant_id] = []
        
        self.tenant_pipelines[tenant_id].append(pipeline)
        self._save_tenant_data()
        
        # Start pipeline creation process
        asyncio.create_task(self._setup_pipeline_infrastructure(pipeline))
        
        logger.info(f"Created pipeline {pipeline_id} for tenant {tenant_id}")
        return pipeline
    
    async def _setup_pipeline_infrastructure(self, pipeline: TenantPipeline):
        """Set up isolated infrastructure for pipeline"""
        try:
            tenant_agents = self.tenant_agents[pipeline.tenant_id]
            
            # Create dedicated compute cluster if needed
            if pipeline.resource_tier != ResourceTier.SHARED:
                cluster_config = self.tenant_resources[pipeline.tenant_id].cluster_configurations
                pipeline.compute_cluster_id = await self._provision_compute_cluster(
                    pipeline.tenant_id, pipeline.pipeline_id, cluster_config
                )
            
            # Set up Unity Catalog structure for tenant
            await tenant_agents["unity_catalog"].create_pipeline_catalog_structure(
                pipeline.pipeline_id, pipeline.target_config
            )
            
            # Configure Auto Loader with tenant isolation
            await tenant_agents["auto_loader"].setup_tenant_ingestion(
                pipeline.pipeline_id,
                pipeline.source_config,
                pipeline.storage_location
            )
            
            # Initialize quality engine with tenant-specific rules
            await tenant_agents["quality_engine"].initialize_tenant_quality_rules(
                pipeline.pipeline_id,
                pipeline.quality_config
            )
            
            # Update pipeline status
            pipeline.status = PipelineStatus.ACTIVE
            pipeline.updated_at = datetime.utcnow()
            
            # Schedule first run if schedule provided
            if pipeline.schedule:
                pipeline.next_run_at = self._calculate_next_run_time(pipeline.schedule)
            
            self._save_tenant_data()
            
            logger.info(f"Successfully set up infrastructure for pipeline {pipeline.pipeline_id}")
            
        except Exception as e:
            logger.error(f"Failed to set up pipeline {pipeline.pipeline_id}: {e}")
            pipeline.status = PipelineStatus.FAILED
            pipeline.updated_at = datetime.utcnow()
            self._save_tenant_data()
            raise
    
    async def _provision_compute_cluster(self, tenant_id: str, pipeline_id: str, cluster_config: Dict[str, Any]) -> str:
        """Provision dedicated compute cluster for pipeline"""
        # Simulate cluster provisioning
        await asyncio.sleep(2)
        
        cluster_id = f"cluster_{tenant_id}_{pipeline_id}_{uuid.uuid4().hex[:6]}"
        
        logger.info(f"Provisioned compute cluster {cluster_id} for tenant {tenant_id}")
        return cluster_id
    
    def _calculate_next_run_time(self, schedule: str) -> datetime:
        """Calculate next run time based on cron schedule"""
        # Simplified scheduling - in production use croniter or similar
        if schedule == "hourly":
            return datetime.utcnow() + timedelta(hours=1)
        elif schedule == "daily":
            return datetime.utcnow() + timedelta(days=1)
        elif schedule == "weekly":
            return datetime.utcnow() + timedelta(weeks=1)
        else:
            # Default to hourly
            return datetime.utcnow() + timedelta(hours=1)
    
    async def execute_pipeline(self, tenant_id: str, pipeline_id: str) -> Dict[str, Any]:
        """Execute a specific tenant pipeline"""
        pipeline = self._get_tenant_pipeline(tenant_id, pipeline_id)
        if not pipeline:
            raise ValueError(f"Pipeline {pipeline_id} not found for tenant {tenant_id}")
        
        if pipeline.status != PipelineStatus.ACTIVE:
            raise ValueError(f"Pipeline {pipeline_id} is not active (status: {pipeline.status.value})")
        
        start_time = datetime.utcnow()
        
        try:
            tenant_agents = self.tenant_agents[tenant_id]
            
            # Execute ingestion
            ingestion_result = await tenant_agents["auto_loader"].run_pipeline(
                pipeline_id, pipeline.source_config
            )
            
            # Execute quality checks
            quality_result = await tenant_agents["quality_engine"].run_quality_assessment(
                pipeline_id, ingestion_result["table_path"]
            )
            
            # Update pipeline statistics
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            
            pipeline.total_runs += 1
            pipeline.successful_runs += 1
            pipeline.last_run_at = start_time
            pipeline.total_gb_processed += ingestion_result.get("gb_processed", 0)
            
            # Update average runtime
            pipeline.avg_runtime_seconds = (
                (pipeline.avg_runtime_seconds * (pipeline.total_runs - 1) + execution_time) /
                pipeline.total_runs
            )
            
            # Schedule next run
            if pipeline.schedule:
                pipeline.next_run_at = self._calculate_next_run_time(pipeline.schedule)
            
            pipeline.updated_at = datetime.utcnow()
            self._save_tenant_data()
            
            logger.info(f"Successfully executed pipeline {pipeline_id} for tenant {tenant_id}")
            
            return {
                "pipeline_id": pipeline_id,
                "execution_time_seconds": execution_time,
                "gb_processed": ingestion_result.get("gb_processed", 0),
                "quality_score": quality_result.get("overall_score", 0),
                "status": "success"
            }
            
        except Exception as e:
            # Update failure statistics
            pipeline.total_runs += 1
            pipeline.failed_runs += 1
            pipeline.last_run_at = start_time
            pipeline.updated_at = datetime.utcnow()
            self._save_tenant_data()
            
            logger.error(f"Pipeline {pipeline_id} execution failed for tenant {tenant_id}: {e}")
            
            return {
                "pipeline_id": pipeline_id,
                "status": "failed",
                "error": str(e)
            }
    
    def _get_tenant_pipeline(self, tenant_id: str, pipeline_id: str) -> Optional[TenantPipeline]:
        """Get specific pipeline for tenant"""
        tenant_pipelines = self.tenant_pipelines.get(tenant_id, [])
        return next((p for p in tenant_pipelines if p.pipeline_id == pipeline_id), None)
    
    async def list_tenant_pipelines(self, tenant_id: str) -> List[Dict[str, Any]]:
        """List all pipelines for tenant"""
        tenant_pipelines = self.tenant_pipelines.get(tenant_id, [])
        
        return [
            {
                "pipeline_id": p.pipeline_id,
                "name": p.name,
                "status": p.status.value,
                "source_type": p.source_type,
                "resource_tier": p.resource_tier.value,
                "created_at": p.created_at.isoformat(),
                "last_run_at": p.last_run_at.isoformat() if p.last_run_at else None,
                "next_run_at": p.next_run_at.isoformat() if p.next_run_at else None,
                "total_runs": p.total_runs,
                "successful_runs": p.successful_runs,
                "failed_runs": p.failed_runs,
                "success_rate": (p.successful_runs / p.total_runs * 100) if p.total_runs > 0 else 0,
                "total_gb_processed": p.total_gb_processed,
                "avg_runtime_seconds": p.avg_runtime_seconds
            }
            for p in tenant_pipelines
        ]
    
    async def get_tenant_resource_usage(self, tenant_id: str) -> Dict[str, Any]:
        """Get current resource usage for tenant"""
        if tenant_id not in self.tenant_resources:
            return {"error": "No resources allocated for tenant"}
        
        resource_allocation = self.tenant_resources[tenant_id]
        tenant_pipelines = self.tenant_pipelines.get(tenant_id, [])
        
        active_pipelines = [p for p in tenant_pipelines if p.status == PipelineStatus.ACTIVE]
        total_gb_processed = sum(p.total_gb_processed for p in tenant_pipelines)
        
        return {
            "tenant_id": tenant_id,
            "resource_tier": resource_allocation.resource_tier.value,
            "allocated_resources": {
                "max_concurrent_pipelines": resource_allocation.max_concurrent_pipelines,
                "storage_quota_gb": resource_allocation.storage_quota_gb,
                "max_quality_checks_per_hour": resource_allocation.max_quality_checks_per_hour
            },
            "current_usage": {
                "active_pipelines": len(active_pipelines),
                "total_pipelines": len(tenant_pipelines),
                "total_gb_processed": total_gb_processed,
                "storage_used_gb": total_gb_processed * 0.1,  # Estimate based on processing
                "utilization_percentage": (len(active_pipelines) / resource_allocation.max_concurrent_pipelines) * 100
            },
            "pipeline_statistics": {
                "total_runs": sum(p.total_runs for p in tenant_pipelines),
                "successful_runs": sum(p.successful_runs for p in tenant_pipelines),
                "failed_runs": sum(p.failed_runs for p in tenant_pipelines),
                "avg_runtime_seconds": sum(p.avg_runtime_seconds for p in tenant_pipelines) / len(tenant_pipelines) if tenant_pipelines else 0
            }
        }
    
    async def scale_tenant_resources(self, tenant_id: str, new_resource_tier: ResourceTier):
        """Scale tenant resources to new tier"""
        if tenant_id not in self.tenant_resources:
            raise ValueError(f"No resources allocated for tenant {tenant_id}")
        
        resource_allocation = self.tenant_resources[tenant_id]
        old_tier = resource_allocation.resource_tier
        
        # Update resource allocation
        tier_config = self.resource_tiers[new_resource_tier]
        resource_allocation.resource_tier = new_resource_tier
        resource_allocation.max_concurrent_pipelines = tier_config["max_concurrent_pipelines"]
        resource_allocation.cluster_configurations = tier_config["cluster_config"]
        resource_allocation.storage_quota_gb = tier_config["storage_quota_gb"]
        resource_allocation.max_quality_checks_per_hour = tier_config["max_quality_checks_per_hour"]
        resource_allocation.last_scaled_at = datetime.utcnow()
        
        self._save_tenant_data()
        
        logger.info(f"Scaled tenant {tenant_id} from {old_tier.value} to {new_resource_tier.value}")
    
    async def terminate_tenant_resources(self, tenant_id: str):
        """Clean up all tenant resources"""
        if tenant_id in self.tenant_pipelines:
            # Stop all pipelines
            for pipeline in self.tenant_pipelines[tenant_id]:
                pipeline.status = PipelineStatus.TERMINATED
                pipeline.updated_at = datetime.utcnow()
            
        # Clean up agents
        if tenant_id in self.tenant_agents:
            del self.tenant_agents[tenant_id]
        
        # Remove resource allocation
        if tenant_id in self.tenant_resources:
            del self.tenant_resources[tenant_id]
        
        self._save_tenant_data()
        
        logger.info(f"Terminated all resources for tenant {tenant_id}")

# Global orchestrator instance
data_plane_orchestrator = TenantDataPlaneOrchestrator()