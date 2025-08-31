"""
Tenant Management Service for Multi-tenant SaaS Platform
"""

import uuid
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class TenantTier(Enum):
    """Tenant service tiers with different resource allocations"""
    STARTER = "starter"
    PROFESSIONAL = "professional" 
    ENTERPRISE = "enterprise"
    HEALTHCARE_PLUS = "healthcare_plus"  # Specialized for healthcare compliance

class TenantStatus(Enum):
    """Tenant lifecycle status"""
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    DEPROVISIONING = "deprovisioning"
    TERMINATED = "terminated"

class IsolationLevel(Enum):
    """Data and compute isolation levels"""
    SHARED = "shared"  # Shared compute, isolated data
    DEDICATED_COMPUTE = "dedicated_compute"  # Dedicated clusters
    FULLY_ISOLATED = "fully_isolated"  # Separate infrastructure

@dataclass
class TenantConfiguration:
    """Comprehensive tenant configuration"""
    tenant_id: str
    organization_name: str
    tier: TenantTier
    isolation_level: IsolationLevel
    status: TenantStatus
    created_at: datetime
    updated_at: datetime
    
    # Resource limits
    max_monthly_gb_processed: int
    max_concurrent_pipelines: int
    max_compute_hours_monthly: int
    storage_quota_gb: int
    
    # Feature flags
    features_enabled: Dict[str, bool]
    
    # Healthcare specific
    compliance_level: str  # HIPAA, SOC2, etc.
    data_residency: str  # US, EU, etc.
    
    # Billing
    billing_contact: Dict[str, str]
    payment_method_id: Optional[str]
    
    # Technical configuration
    databricks_workspace_id: Optional[str]
    storage_account_id: Optional[str]
    network_config: Dict[str, Any]
    
    # Custom settings
    quality_thresholds: Dict[str, float]
    notification_settings: Dict[str, Any]

class TenantManager:
    """Manages tenant lifecycle, provisioning, and configuration"""
    
    def __init__(self, config_store_path: str = "data/tenants"):
        self.config_store_path = Path(config_store_path)
        self.config_store_path.mkdir(parents=True, exist_ok=True)
        self.tenants: Dict[str, TenantConfiguration] = {}
        self._load_tenant_configs()
    
    def _load_tenant_configs(self):
        """Load existing tenant configurations from storage"""
        for config_file in self.config_store_path.glob("*.json"):
            try:
                with open(config_file, 'r') as f:
                    data = json.load(f)
                    # Convert datetime strings back to datetime objects
                    data['created_at'] = datetime.fromisoformat(data['created_at'])
                    data['updated_at'] = datetime.fromisoformat(data['updated_at'])
                    data['tier'] = TenantTier(data['tier'])
                    data['isolation_level'] = IsolationLevel(data['isolation_level'])
                    data['status'] = TenantStatus(data['status'])
                    
                    tenant_config = TenantConfiguration(**data)
                    self.tenants[tenant_config.tenant_id] = tenant_config
                    
            except Exception as e:
                logger.error(f"Failed to load tenant config {config_file}: {e}")
    
    def _save_tenant_config(self, tenant_config: TenantConfiguration):
        """Persist tenant configuration to storage"""
        config_file = self.config_store_path / f"{tenant_config.tenant_id}.json"
        
        # Convert to dict and handle datetime serialization
        data = asdict(tenant_config)
        data['created_at'] = tenant_config.created_at.isoformat()
        data['updated_at'] = tenant_config.updated_at.isoformat()
        data['tier'] = tenant_config.tier.value
        data['isolation_level'] = tenant_config.isolation_level.value
        data['status'] = tenant_config.status.value
        
        with open(config_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    async def create_tenant(self, 
                          organization_name: str,
                          tier: TenantTier,
                          isolation_level: IsolationLevel,
                          billing_contact: Dict[str, str],
                          compliance_level: str = "HIPAA",
                          data_residency: str = "US") -> TenantConfiguration:
        """Create a new tenant with full provisioning"""
        
        tenant_id = f"tenant_{uuid.uuid4().hex[:8]}"
        
        # Define tier-based resource allocations
        tier_configs = {
            TenantTier.STARTER: {
                "max_monthly_gb_processed": 100,
                "max_concurrent_pipelines": 3,
                "max_compute_hours_monthly": 50,
                "storage_quota_gb": 500,
                "features_enabled": {
                    "advanced_ml_anomaly_detection": False,
                    "custom_quality_rules": False,
                    "priority_support": False,
                    "audit_logging": True,
                    "real_time_alerts": True
                }
            },
            TenantTier.PROFESSIONAL: {
                "max_monthly_gb_processed": 1000,
                "max_concurrent_pipelines": 10,
                "max_compute_hours_monthly": 200,
                "storage_quota_gb": 2000,
                "features_enabled": {
                    "advanced_ml_anomaly_detection": True,
                    "custom_quality_rules": True,
                    "priority_support": False,
                    "audit_logging": True,
                    "real_time_alerts": True
                }
            },
            TenantTier.ENTERPRISE: {
                "max_monthly_gb_processed": 10000,
                "max_concurrent_pipelines": 50,
                "max_compute_hours_monthly": 1000,
                "storage_quota_gb": 10000,
                "features_enabled": {
                    "advanced_ml_anomaly_detection": True,
                    "custom_quality_rules": True,
                    "priority_support": True,
                    "audit_logging": True,
                    "real_time_alerts": True
                }
            },
            TenantTier.HEALTHCARE_PLUS: {
                "max_monthly_gb_processed": 50000,
                "max_concurrent_pipelines": 100,
                "max_compute_hours_monthly": 5000,
                "storage_quota_gb": 50000,
                "features_enabled": {
                    "advanced_ml_anomaly_detection": True,
                    "custom_quality_rules": True,
                    "priority_support": True,
                    "audit_logging": True,
                    "real_time_alerts": True,
                    "hipaa_compliance_validation": True,
                    "phi_detection": True,
                    "dedicated_compliance_officer": True
                }
            }
        }
        
        config = tier_configs[tier]
        
        tenant_config = TenantConfiguration(
            tenant_id=tenant_id,
            organization_name=organization_name,
            tier=tier,
            isolation_level=isolation_level,
            status=TenantStatus.PROVISIONING,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            
            max_monthly_gb_processed=config["max_monthly_gb_processed"],
            max_concurrent_pipelines=config["max_concurrent_pipelines"],
            max_compute_hours_monthly=config["max_compute_hours_monthly"],
            storage_quota_gb=config["storage_quota_gb"],
            
            features_enabled=config["features_enabled"],
            
            compliance_level=compliance_level,
            data_residency=data_residency,
            
            billing_contact=billing_contact,
            payment_method_id=None,
            
            databricks_workspace_id=None,
            storage_account_id=None,
            network_config={},
            
            quality_thresholds={
                "completeness_threshold": 0.95,
                "validity_threshold": 0.98,
                "consistency_threshold": 0.90,
                "timeliness_threshold_hours": 2.0
            },
            notification_settings={
                "email_notifications": True,
                "slack_webhook": None,
                "pagerduty_integration": False
            }
        )
        
        # Store configuration
        self.tenants[tenant_id] = tenant_config
        self._save_tenant_config(tenant_config)
        
        # Trigger async provisioning
        asyncio.create_task(self._provision_tenant_infrastructure(tenant_config))
        
        logger.info(f"Created tenant {tenant_id} for {organization_name}")
        return tenant_config
    
    async def _provision_tenant_infrastructure(self, tenant_config: TenantConfiguration):
        """Provision cloud infrastructure for tenant"""
        try:
            logger.info(f"Starting infrastructure provisioning for {tenant_config.tenant_id}")
            
            # Step 1: Provision Databricks workspace
            if tenant_config.isolation_level != IsolationLevel.SHARED:
                workspace_id = await self._provision_databricks_workspace(tenant_config)
                tenant_config.databricks_workspace_id = workspace_id
            
            # Step 2: Provision storage account
            storage_account_id = await self._provision_storage_account(tenant_config)
            tenant_config.storage_account_id = storage_account_id
            
            # Step 3: Set up networking (if required)
            if tenant_config.isolation_level == IsolationLevel.FULLY_ISOLATED:
                network_config = await self._setup_tenant_networking(tenant_config)
                tenant_config.network_config = network_config
            
            # Step 4: Initialize data quality configurations
            await self._initialize_quality_configurations(tenant_config)
            
            # Step 5: Set up monitoring and alerting
            await self._setup_tenant_monitoring(tenant_config)
            
            # Update status to active
            tenant_config.status = TenantStatus.ACTIVE
            tenant_config.updated_at = datetime.utcnow()
            self._save_tenant_config(tenant_config)
            
            logger.info(f"Successfully provisioned tenant {tenant_config.tenant_id}")
            
        except Exception as e:
            logger.error(f"Failed to provision tenant {tenant_config.tenant_id}: {e}")
            tenant_config.status = TenantStatus.SUSPENDED
            self._save_tenant_config(tenant_config)
            raise
    
    async def _provision_databricks_workspace(self, tenant_config: TenantConfiguration) -> str:
        """Provision dedicated Databricks workspace for tenant"""
        # Simulate workspace provisioning
        await asyncio.sleep(2)
        workspace_id = f"databricks-{tenant_config.tenant_id}"
        logger.info(f"Provisioned Databricks workspace {workspace_id}")
        return workspace_id
    
    async def _provision_storage_account(self, tenant_config: TenantConfiguration) -> str:
        """Provision cloud storage for tenant"""
        await asyncio.sleep(1)
        storage_id = f"storage-{tenant_config.tenant_id}"
        logger.info(f"Provisioned storage account {storage_id}")
        return storage_id
    
    async def _setup_tenant_networking(self, tenant_config: TenantConfiguration) -> Dict[str, Any]:
        """Set up isolated networking for tenant"""
        await asyncio.sleep(1)
        network_config = {
            "vpc_id": f"vpc-{tenant_config.tenant_id}",
            "subnet_ids": [f"subnet-{tenant_config.tenant_id}-1", f"subnet-{tenant_config.tenant_id}-2"],
            "security_group_id": f"sg-{tenant_config.tenant_id}"
        }
        logger.info(f"Set up networking for tenant {tenant_config.tenant_id}")
        return network_config
    
    async def _initialize_quality_configurations(self, tenant_config: TenantConfiguration):
        """Initialize tenant-specific quality configurations"""
        await asyncio.sleep(0.5)
        logger.info(f"Initialized quality configs for tenant {tenant_config.tenant_id}")
    
    async def _setup_tenant_monitoring(self, tenant_config: TenantConfiguration):
        """Set up monitoring and alerting for tenant"""
        await asyncio.sleep(0.5)
        logger.info(f"Set up monitoring for tenant {tenant_config.tenant_id}")
    
    def get_tenant(self, tenant_id: str) -> Optional[TenantConfiguration]:
        """Retrieve tenant configuration"""
        return self.tenants.get(tenant_id)
    
    def list_tenants(self, status: Optional[TenantStatus] = None) -> List[TenantConfiguration]:
        """List all tenants, optionally filtered by status"""
        if status:
            return [config for config in self.tenants.values() if config.status == status]
        return list(self.tenants.values())
    
    async def update_tenant(self, tenant_id: str, updates: Dict[str, Any]) -> TenantConfiguration:
        """Update tenant configuration"""
        if tenant_id not in self.tenants:
            raise ValueError(f"Tenant {tenant_id} not found")
        
        tenant_config = self.tenants[tenant_id]
        
        # Update allowed fields
        updatable_fields = ['quality_thresholds', 'notification_settings', 'billing_contact']
        for field in updatable_fields:
            if field in updates:
                setattr(tenant_config, field, updates[field])
        
        tenant_config.updated_at = datetime.utcnow()
        self._save_tenant_config(tenant_config)
        
        logger.info(f"Updated tenant {tenant_id}")
        return tenant_config
    
    async def suspend_tenant(self, tenant_id: str, reason: str):
        """Suspend tenant operations"""
        if tenant_id not in self.tenants:
            raise ValueError(f"Tenant {tenant_id} not found")
        
        tenant_config = self.tenants[tenant_id]
        tenant_config.status = TenantStatus.SUSPENDED
        tenant_config.updated_at = datetime.utcnow()
        self._save_tenant_config(tenant_config)
        
        # TODO: Stop all running pipelines for this tenant
        logger.info(f"Suspended tenant {tenant_id}: {reason}")
    
    async def terminate_tenant(self, tenant_id: str):
        """Terminate tenant and deprovision resources"""
        if tenant_id not in self.tenants:
            raise ValueError(f"Tenant {tenant_id} not found")
        
        tenant_config = self.tenants[tenant_id]
        tenant_config.status = TenantStatus.DEPROVISIONING
        self._save_tenant_config(tenant_config)
        
        try:
            # Deprovision infrastructure
            await self._deprovision_tenant_infrastructure(tenant_config)
            
            # Mark as terminated
            tenant_config.status = TenantStatus.TERMINATED
            tenant_config.updated_at = datetime.utcnow()
            self._save_tenant_config(tenant_config)
            
            logger.info(f"Terminated tenant {tenant_id}")
            
        except Exception as e:
            logger.error(f"Failed to terminate tenant {tenant_id}: {e}")
            raise
    
    async def _deprovision_tenant_infrastructure(self, tenant_config: TenantConfiguration):
        """Clean up tenant infrastructure"""
        # Simulate deprovisioning
        await asyncio.sleep(3)
        logger.info(f"Deprovisioned infrastructure for tenant {tenant_config.tenant_id}")
    
    def get_tenant_resource_usage(self, tenant_id: str) -> Dict[str, Any]:
        """Get current resource usage for tenant"""
        # TODO: Integrate with actual monitoring systems
        return {
            "current_month_gb_processed": 75,
            "active_pipelines": 2,
            "compute_hours_used": 25,
            "storage_used_gb": 150,
            "last_updated": datetime.utcnow().isoformat()
        }