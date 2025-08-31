# Deployment & Operations Guide

Complete guide for deploying and operating the Agentic Data Engineering Platform in production environments.

## Overview

This guide covers production deployment strategies, infrastructure requirements, monitoring setup, and operational procedures for maintaining a healthcare data processing platform at scale.

### Deployment Models

- **Single-Tenant Deployment** - Dedicated infrastructure for one organization
- **Multi-Tenant SaaS** - Shared infrastructure with tenant isolation
- **Hybrid Deployment** - Mix of cloud and on-premises components
- **High Availability** - Multi-region deployment with failover

---

## Pre-Deployment Requirements

### Infrastructure Prerequisites

#### Databricks Requirements
```yaml
databricks_requirements:
  # Workspace Configuration
  workspace_tier: "Premium"  # Required for Unity Catalog
  unity_catalog: "enabled"
  compliance_security_features: "enabled"
  
  # Compute Requirements  
  cluster_policies: "configured"
  instance_pools: "recommended"
  auto_scaling: "enabled"
  
  # Storage Requirements
  dbfs_encryption: "customer_managed_keys"
  unity_catalog_storage: "external_location"
  
  # Network Security
  vpc_endpoints: "enabled"
  private_subnets: "required"
  security_groups: "configured"
```

#### Cloud Infrastructure
```yaml
# AWS Requirements
aws_infrastructure:
  # VPC Configuration
  vpc:
    cidr: "10.0.0.0/16"
    availability_zones: 3
    private_subnets: "required"
    public_subnets: "nat_gateway_only"
  
  # S3 Configuration
  s3:
    versioning: "enabled"
    encryption: "SSE-S3"
    lifecycle_policies: "configured"
    cross_region_replication: "recommended"
  
  # Security
  iam:
    least_privilege_roles: "required"
    cross_account_roles: "configured"
    mfa_enforcement: "enabled"
  
  kms:
    customer_managed_keys: "required"
    key_rotation: "annual"
    
# Azure Requirements (Alternative)
azure_infrastructure:
  # Virtual Network
  vnet:
    address_space: "10.0.0.0/16"
    subnets: "private_and_public"
    nsg_rules: "configured"
  
  # Storage Account
  storage:
    encryption: "customer_managed_keys"
    hierarchical_namespace: "enabled"  # Data Lake Gen2
    network_rules: "private_endpoints"
  
  # Security
  key_vault:
    access_policies: "configured"
    network_access: "private_endpoint"
```

### Security Requirements

#### Compliance Standards
```yaml
compliance_requirements:
  # Healthcare Compliance
  hipaa:
    business_associate_agreement: "required"
    phi_encryption: "required"
    audit_logging: "comprehensive"
    access_controls: "role_based"
    
  # Data Governance
  data_governance:
    data_classification: "automated"
    retention_policies: "7_years_minimum"
    right_to_deletion: "supported"
    data_lineage: "complete"
    
  # Security Frameworks
  security_frameworks:
    - "SOC 2 Type II"
    - "ISO 27001"
    - "NIST Cybersecurity Framework"
```

#### Network Security
```yaml
network_security:
  # Network Segmentation
  network_segmentation:
    data_plane_isolation: "required"
    control_plane_separation: "required"
    dmz_configuration: "recommended"
  
  # Traffic Encryption
  encryption:
    tls_version: "1.2_minimum"
    internal_traffic: "encrypted"
    certificate_management: "automated"
  
  # Access Controls
  access_controls:
    vpn_required: true
    ip_whitelisting: "enabled"
    multi_factor_auth: "required"
    session_timeouts: "configured"
```

---

## Production Deployment

### Environment Setup

#### Development Environment
```yaml
development_environment:
  # Resource Configuration
  databricks:
    workspace_tier: "Standard"
    cluster_config:
      node_type: "i3.large"
      min_workers: 1
      max_workers: 3
      auto_termination: 15
  
  # Cost Optimization
  cost_controls:
    spot_instances: true
    auto_shutdown: "aggressive"
    resource_limits: "enforced"
    
  # Data Configuration
  data:
    sample_datasets: "included"
    synthetic_data: "allowed"
    production_data_access: "restricted"
```

#### Staging Environment
```yaml
staging_environment:
  # Mirror Production Configuration
  databricks:
    workspace_tier: "Premium"
    cluster_config:
      node_type: "i3.xlarge" 
      min_workers: 2
      max_workers: 6
      auto_termination: 30
      
  # Testing Configuration
  testing:
    automated_testing: "enabled"
    performance_testing: "included"
    security_scanning: "automated"
    
  # Data Configuration
  data:
    production_mirror: "subset"
    data_anonymization: "required"
    refresh_frequency: "weekly"
```

#### Production Environment
```yaml
production_environment:
  # High-Availability Configuration
  databricks:
    workspace_tier: "Premium"
    multi_workspace: true
    cross_region_backup: true
    
  # Cluster Configuration
  cluster_config:
    node_type: "i3.xlarge"
    min_workers: 3
    max_workers: 20
    auto_scaling: true
    spot_instances: false  # On-demand for reliability
    
  # Monitoring & Alerting
  monitoring:
    comprehensive_logging: true
    real_time_alerting: true
    performance_monitoring: true
    cost_monitoring: true
    
  # Disaster Recovery
  disaster_recovery:
    rpo_target: "1_hour"
    rto_target: "4_hours"
    backup_frequency: "continuous"
    cross_region_replication: true
```

### Deployment Pipeline

#### CI/CD Configuration
```yaml
# .github/workflows/deploy.yml
name: Healthcare Platform Deployment

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

env:
  DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
  AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
  AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
          
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
          pip install -r requirements-dev.txt
          
      - name: Run unit tests
        run: pytest tests/unit/ --cov=src --cov-report=xml
        
      - name: Run security scan
        run: |
          pip install safety bandit
          safety check
          bandit -r src/ -f json -o security-report.json
          
      - name: Validate configuration
        run: python -m src.cli config validate

  deploy-staging:
    needs: test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/develop'
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Deploy to Staging
        run: |
          # Deploy configuration
          databricks fs cp -r config/ dbfs:/healthcare-platform/config/
          
          # Deploy source code
          databricks fs cp -r src/ dbfs:/healthcare-platform/src/
          
          # Update job configurations
          python scripts/deploy_jobs.py --environment staging
          
      - name: Run integration tests
        run: pytest tests/integration/ --environment staging
        
      - name: Performance tests
        run: python scripts/performance_tests.py --environment staging

  deploy-production:
    needs: [test, deploy-staging]
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    environment: production  # Requires manual approval
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Deploy to Production
        run: |
          # Blue-green deployment
          python scripts/blue_green_deploy.py
          
          # Health checks
          python scripts/health_check.py --environment production
          
          # Smoke tests
          pytest tests/smoke/ --environment production
```

#### Deployment Scripts

**Blue-Green Deployment Script**
```python
#!/usr/bin/env python
"""
Blue-Green deployment script for healthcare platform
Ensures zero-downtime deployment with rollback capability
"""

import time
import logging
from databricks.sdk import WorkspaceClient
from src.deployment.blue_green import BlueGreenDeployer

def main():
    # Initialize deployment
    deployer = BlueGreenDeployer(
        workspace_client=WorkspaceClient(),
        environment="production"
    )
    
    try:
        # Step 1: Deploy to green environment
        logging.info("Deploying to green environment...")
        deployer.deploy_green()
        
        # Step 2: Run health checks on green
        logging.info("Running health checks...")
        if not deployer.health_check_green():
            raise Exception("Green environment health check failed")
            
        # Step 3: Run smoke tests
        logging.info("Running smoke tests...")
        if not deployer.smoke_test_green():
            raise Exception("Green environment smoke tests failed")
            
        # Step 4: Switch traffic to green
        logging.info("Switching traffic to green...")
        deployer.switch_to_green()
        
        # Step 5: Monitor for 10 minutes
        logging.info("Monitoring deployment...")
        time.sleep(600)  # 10 minutes
        
        if deployer.monitor_deployment():
            logging.info("Deployment successful!")
            deployer.cleanup_blue()
        else:
            logging.error("Deployment issues detected, rolling back...")
            deployer.rollback_to_blue()
            raise Exception("Deployment rolled back due to issues")
            
    except Exception as e:
        logging.error(f"Deployment failed: {e}")
        deployer.rollback_to_blue()
        raise

if __name__ == "__main__":
    main()
```

---

## Infrastructure as Code

### Terraform Configuration

**Main Infrastructure**
```hcl
# terraform/main.tf
provider "aws" {
  region = var.aws_region
}

# VPC Configuration
module "vpc" {
  source = "terraform-aws-modules/vpc/aws"
  
  name = "healthcare-platform-vpc"
  cidr = "10.0.0.0/16"
  
  azs             = ["${var.aws_region}a", "${var.aws_region}b", "${var.aws_region}c"]
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
  
  enable_nat_gateway = true
  enable_vpn_gateway = false
  enable_dns_hostnames = true
  enable_dns_support = true
  
  tags = {
    Environment = var.environment
    Project     = "healthcare-platform"
    Terraform   = "true"
  }
}

# S3 Buckets for Data Storage
resource "aws_s3_bucket" "raw_data" {
  bucket = "${var.project_name}-raw-data-${var.environment}"
  
  tags = {
    Environment = var.environment
    DataClass   = "raw"
  }
}

resource "aws_s3_bucket_versioning" "raw_data_versioning" {
  bucket = aws_s3_bucket.raw_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_data_encryption" {
  bucket = aws_s3_bucket.raw_data.id
  
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.s3_key.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

# KMS Keys for Encryption
resource "aws_kms_key" "s3_key" {
  description             = "KMS key for S3 encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  
  tags = {
    Name = "healthcare-platform-s3-key"
    Environment = var.environment
  }
}

# Databricks Workspace
resource "databricks_workspace" "healthcare_platform" {
  account_id     = var.databricks_account_id
  workspace_name = "healthcare-platform-${var.environment}"
  cloud_provider = "aws"
  
  aws_region = var.aws_region
  
  network_configuration {
    vpc_id               = module.vpc.vpc_id
    subnet_ids           = module.vpc.private_subnets
    security_group_ids   = [aws_security_group.databricks.id]
  }
  
  storage_configuration {
    s3_bucket_name = aws_s3_bucket.workspace_storage.bucket
  }
  
  tags = {
    Environment = var.environment
    Project     = "healthcare-platform"
  }
}
```

**Databricks Configuration**
```hcl
# terraform/databricks.tf
provider "databricks" {
  host  = databricks_workspace.healthcare_platform.workspace_url
  token = var.databricks_token
}

# Unity Catalog Configuration
resource "databricks_catalog" "healthcare_data" {
  name    = "healthcare_data_${var.environment}"
  comment = "Healthcare data catalog for ${var.environment}"
  
  properties = {
    purpose = "healthcare_data_processing"
  }
  
  depends_on = [databricks_workspace.healthcare_platform]
}

# Schema Creation
resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.healthcare_data.id
  name         = "bronze"
  comment      = "Raw healthcare data"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.healthcare_data.id
  name         = "silver"
  comment      = "Validated and cleaned healthcare data"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.healthcare_data.id
  name         = "gold"
  comment      = "Analytics-ready healthcare data"
}

# Cluster Policy for Healthcare Workloads
resource "databricks_cluster_policy" "healthcare_policy" {
  name = "Healthcare Data Processing Policy"
  
  definition = jsonencode({
    "cluster_type": {
      "type": "fixed",
      "value": "job"
    },
    "node_type_id": {
      "type": "allowlist",
      "values": ["i3.large", "i3.xlarge", "i3.2xlarge"]
    },
    "driver_node_type_id": {
      "type": "allowlist", 
      "values": ["i3.large", "i3.xlarge"]
    },
    "autotermination_minutes": {
      "type": "range",
      "maxValue": 120,
      "defaultValue": 30
    },
    "enable_elastic_disk": {
      "type": "fixed",
      "value": true
    }
  })
}

# Instance Pool for Cost Optimization
resource "databricks_instance_pool" "healthcare_pool" {
  instance_pool_name = "healthcare-instance-pool"
  node_type_id       = "i3.xlarge"
  
  min_idle_instances = 1
  max_capacity       = 20
  
  idle_instance_autotermination_minutes = 15
  
  aws_attributes {
    availability           = "SPOT_WITH_FALLBACK"
    spot_bid_price_percent = 70
    zone_id                = "auto"
  }
  
  preloaded_spark_versions = ["13.3.x-scala2.12"]
}
```

### Kubernetes Deployment (Alternative)

**Helm Chart Configuration**
```yaml
# helm/healthcare-platform/values.yaml
global:
  environment: production
  region: us-east-1

# Dashboard Application
dashboard:
  replicaCount: 3
  image:
    repository: healthcare-platform/dashboard
    tag: "1.0.0"
    pullPolicy: IfNotPresent
  
  service:
    type: LoadBalancer
    port: 8501
  
  resources:
    limits:
      cpu: 1000m
      memory: 2Gi
    requests:
      cpu: 500m
      memory: 1Gi
      
  autoscaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 10
    targetCPUUtilizationPercentage: 70

# API Service
api:
  replicaCount: 5
  image:
    repository: healthcare-platform/api
    tag: "1.0.0"
  
  service:
    type: ClusterIP
    port: 8000
    
  ingress:
    enabled: true
    className: nginx
    annotations:
      cert-manager.io/cluster-issuer: letsencrypt-prod
      nginx.ingress.kubernetes.io/rate-limit: "100"
    hosts:
      - host: api.healthcare-platform.com
        paths:
          - path: /
            pathType: Prefix
    tls:
      - secretName: healthcare-platform-tls
        hosts:
          - api.healthcare-platform.com

# Configuration
config:
  databricks:
    host: https://your-workspace.cloud.databricks.com
    tokenSecretName: databricks-token
    
  storage:
    type: s3
    bucket: healthcare-platform-data
    region: us-east-1
    
  monitoring:
    prometheus:
      enabled: true
      serviceMonitor:
        enabled: true
    grafana:
      enabled: true
      dashboardsConfigMap: healthcare-dashboards

# Security
security:
  networkPolicies:
    enabled: true
  podSecurityPolicy:
    enabled: true
  rbac:
    create: true
```

---

## Monitoring & Observability

### Metrics and Logging

#### Prometheus Configuration
```yaml
# monitoring/prometheus-config.yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - "healthcare_platform_rules.yml"

scrape_configs:
  # Platform metrics
  - job_name: 'healthcare-platform-api'
    static_configs:
      - targets: ['api:8000']
    metrics_path: /metrics
    scrape_interval: 10s
    
  # Databricks metrics (via custom exporter)
  - job_name: 'databricks-exporter'
    static_configs:
      - targets: ['databricks-exporter:9090']
    scrape_interval: 30s
    
  # Quality metrics
  - job_name: 'quality-metrics'
    static_configs:
      - targets: ['quality-service:8001']
    metrics_path: /quality/metrics
    scrape_interval: 60s

alerting:
  alertmanagers:
    - static_configs:
        - targets: ['alertmanager:9093']
```

#### Alert Rules
```yaml
# monitoring/healthcare_platform_rules.yml
groups:
  - name: healthcare_platform
    rules:
      # Data Quality Alerts
      - alert: LowDataQuality
        expr: healthcare_quality_score < 80
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Data quality score below threshold"
          description: "Quality score {{ $value }}% for table {{ $labels.table }}"
      
      - alert: CriticalDataQuality
        expr: healthcare_quality_score < 60
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Critical data quality issue"
          description: "Quality score {{ $value }}% for table {{ $labels.table }}"
      
      # Pipeline Health Alerts  
      - alert: PipelineFailure
        expr: healthcare_pipeline_success_rate < 95
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Pipeline success rate below threshold"
          description: "Pipeline {{ $labels.pipeline }} success rate: {{ $value }}%"
      
      # Cost Alerts
      - alert: HighComputeCost
        expr: databricks_cluster_cost_hourly > 50
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "High compute costs detected"
          description: "Cluster {{ $labels.cluster }} costing ${{ $value }}/hour"
      
      # Security Alerts
      - alert: UnauthorizedAccess
        expr: increase(healthcare_unauthorized_requests[5m]) > 10
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Multiple unauthorized access attempts"
          description: "{{ $value }} unauthorized requests in 5 minutes"
```

#### Grafana Dashboards
```json
{
  "dashboard": {
    "title": "Healthcare Platform Overview",
    "panels": [
      {
        "title": "Data Quality Score",
        "type": "stat",
        "targets": [
          {
            "expr": "avg(healthcare_quality_score)",
            "legendFormat": "Overall Quality"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "color": {
              "mode": "thresholds"
            },
            "thresholds": {
              "steps": [
                {"color": "red", "value": 0},
                {"color": "yellow", "value": 80},
                {"color": "green", "value": 90}
              ]
            }
          }
        }
      },
      {
        "title": "Pipeline Success Rate",
        "type": "timeseries",
        "targets": [
          {
            "expr": "healthcare_pipeline_success_rate",
            "legendFormat": "{{ pipeline }}"
          }
        ]
      },
      {
        "title": "Cost Optimization",
        "type": "bargauge",
        "targets": [
          {
            "expr": "sum by (cluster) (databricks_cluster_cost_hourly)",
            "legendFormat": "{{ cluster }}"
          }
        ]
      }
    ]
  }
}
```

### Health Checks

#### Application Health Checks
```python
# src/health/health_checks.py
from typing import Dict, Any
import asyncio
from databricks.sdk import WorkspaceClient
from src.agents.quality import QualityEngine

class HealthChecker:
    """Comprehensive health checking for healthcare platform"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.workspace_client = WorkspaceClient()
        
    async def check_all(self) -> Dict[str, Any]:
        """Run all health checks concurrently"""
        
        checks = [
            self.check_databricks_connectivity(),
            self.check_unity_catalog(),
            self.check_storage_access(), 
            self.check_quality_engine(),
            self.check_pipeline_health(),
            self.check_cost_limits()
        ]
        
        results = await asyncio.gather(*checks, return_exceptions=True)
        
        return {
            "overall_health": "healthy" if all(r.get("status") == "healthy" for r in results if isinstance(r, dict)) else "unhealthy",
            "checks": {
                "databricks": results[0],
                "unity_catalog": results[1], 
                "storage": results[2],
                "quality_engine": results[3],
                "pipelines": results[4],
                "cost_limits": results[5]
            },
            "timestamp": datetime.utcnow().isoformat()
        }
    
    async def check_databricks_connectivity(self) -> Dict[str, Any]:
        """Check Databricks workspace connectivity"""
        try:
            user = self.workspace_client.current_user.me()
            clusters = list(self.workspace_client.clusters.list())
            
            return {
                "status": "healthy",
                "details": {
                    "user": user.display_name,
                    "active_clusters": len([c for c in clusters if c.state.value == "RUNNING"]),
                    "total_clusters": len(clusters)
                }
            }
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e)
            }
    
    async def check_pipeline_health(self) -> Dict[str, Any]:
        """Check pipeline health and recent runs"""
        try:
            # Get recent pipeline runs
            recent_runs = self.get_recent_pipeline_runs(hours=24)
            success_rate = sum(1 for r in recent_runs if r.status == "SUCCESS") / len(recent_runs) if recent_runs else 1.0
            
            return {
                "status": "healthy" if success_rate >= 0.95 else "unhealthy",
                "details": {
                    "success_rate": success_rate * 100,
                    "recent_runs": len(recent_runs),
                    "failed_runs": sum(1 for r in recent_runs if r.status == "FAILED")
                }
            }
        except Exception as e:
            return {
                "status": "unhealthy", 
                "error": str(e)
            }
```

---

## Backup & Disaster Recovery

### Backup Strategy

#### Data Backup Configuration
```yaml
backup_strategy:
  # Data Backup
  data_backup:
    frequency: "continuous"
    retention: "7_years"  # Healthcare compliance
    storage_class: "GLACIER"
    cross_region_replication: true
    
    backup_types:
      - type: "incremental"
        frequency: "hourly"
        retention: "30_days"
        
      - type: "full"
        frequency: "daily"
        retention: "1_year"
        
      - type: "archive"
        frequency: "monthly"
        retention: "7_years"
  
  # Configuration Backup
  configuration_backup:
    frequency: "daily"
    retention: "1_year"
    versioning: "enabled"
    
    includes:
      - databricks_job_configurations
      - unity_catalog_schemas
      - pipeline_definitions
      - quality_rules
      - access_controls
      
  # Code Backup
  code_backup:
    repository: "git_based"
    branching_strategy: "gitflow"
    tag_strategy: "semantic_versioning"
    backup_frequency: "real_time"
```

#### Backup Automation Script
```python
#!/usr/bin/env python
"""
Automated backup script for healthcare platform
Handles data, configuration, and metadata backups
"""

import logging
from datetime import datetime, timedelta
from src.backup.backup_manager import BackupManager

class HealthcarePlatformBackup:
    
    def __init__(self, config):
        self.config = config
        self.backup_manager = BackupManager(config)
        
    def run_daily_backup(self):
        """Run daily backup routine"""
        
        backup_id = f"daily_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            # 1. Backup Unity Catalog metadata
            logging.info("Backing up Unity Catalog metadata...")
            self.backup_manager.backup_unity_catalog(backup_id)
            
            # 2. Backup pipeline configurations
            logging.info("Backing up pipeline configurations...")
            self.backup_manager.backup_pipeline_configs(backup_id)
            
            # 3. Backup quality rules and thresholds
            logging.info("Backing up quality configurations...")
            self.backup_manager.backup_quality_configs(backup_id)
            
            # 4. Backup processed data (incremental)
            logging.info("Running incremental data backup...")
            self.backup_manager.backup_data_incremental(backup_id)
            
            # 5. Backup access controls and permissions
            logging.info("Backing up access controls...")
            self.backup_manager.backup_access_controls(backup_id)
            
            # 6. Generate backup manifest
            manifest = self.backup_manager.generate_manifest(backup_id)
            
            # 7. Validate backup integrity
            if self.backup_manager.validate_backup(backup_id):
                logging.info(f"Backup {backup_id} completed successfully")
                self.send_backup_notification(backup_id, "SUCCESS", manifest)
            else:
                logging.error(f"Backup {backup_id} validation failed")
                self.send_backup_notification(backup_id, "FAILED", None)
                
        except Exception as e:
            logging.error(f"Backup {backup_id} failed: {str(e)}")
            self.send_backup_notification(backup_id, "FAILED", str(e))
```

### Disaster Recovery

#### Recovery Time Objectives
```yaml
disaster_recovery_objectives:
  # Recovery Time Objectives (RTO)
  rto_targets:
    dashboard_service: "15_minutes"
    api_service: "30_minutes"
    pipeline_processing: "2_hours"
    full_platform_recovery: "4_hours"
    
  # Recovery Point Objectives (RPO)
  rpo_targets:
    configuration_data: "5_minutes"
    processed_data: "1_hour"
    raw_data: "24_hours"
    
  # Criticality Classification
  service_criticality:
    tier_1_critical:
      - api_service
      - quality_monitoring
      - alert_system
      
    tier_2_important:
      - dashboard_service
      - reporting_system
      - cost_monitoring
      
    tier_3_standard:
      - historical_analytics
      - batch_reporting
      - archive_processing
```

#### Disaster Recovery Procedures
```bash
#!/bin/bash
# disaster_recovery.sh - Emergency recovery procedures

set -e

ENVIRONMENT=${1:-production}
RECOVERY_POINT=${2:-latest}

echo "🚨 Healthcare Platform Disaster Recovery - ${ENVIRONMENT}"
echo "=================================================="

# Step 1: Assess damage and determine recovery strategy
echo "📊 Assessing system status..."
python scripts/assess_damage.py --environment ${ENVIRONMENT}

# Step 2: Restore critical infrastructure
echo "🏗️  Restoring infrastructure..."
terraform apply -var-file="environments/${ENVIRONMENT}.tfvars" -auto-approve

# Step 3: Restore Unity Catalog and schemas
echo "📚 Restoring Unity Catalog..."
python scripts/restore_unity_catalog.py --environment ${ENVIRONMENT} --recovery-point ${RECOVERY_POINT}

# Step 4: Restore pipeline configurations
echo "⚙️  Restoring pipeline configurations..."
python scripts/restore_pipelines.py --environment ${ENVIRONMENT} --recovery-point ${RECOVERY_POINT}

# Step 5: Restore quality rules and configurations
echo "✅ Restoring quality configurations..."
python scripts/restore_quality_configs.py --environment ${ENVIRONMENT} --recovery-point ${RECOVERY_POINT}

# Step 6: Validate system integrity
echo "🔍 Running system validation..."
python scripts/post_recovery_validation.py --environment ${ENVIRONMENT}

# Step 7: Restart critical services
echo "🚀 Starting critical services..."
python scripts/start_critical_services.py --environment ${ENVIRONMENT}

# Step 8: Run health checks
echo "❤️  Running comprehensive health checks..."
python scripts/health_check.py --environment ${ENVIRONMENT} --comprehensive

echo "✅ Disaster recovery completed for ${ENVIRONMENT}"
echo "📝 Please review recovery logs and update incident documentation"
```

---

## Performance Optimization

### Cluster Optimization

#### Auto-Scaling Configuration
```python
# src/optimization/cluster_optimizer.py
class ClusterOptimizer:
    """Intelligent cluster optimization for healthcare workloads"""
    
    def __init__(self, workspace_client, config):
        self.workspace_client = workspace_client
        self.config = config
        
    def optimize_cluster_configuration(self, workload_type: str, historical_data: Dict):
        """Optimize cluster configuration based on workload patterns"""
        
        if workload_type == "ingestion":
            return self._optimize_ingestion_cluster(historical_data)
        elif workload_type == "quality_validation":
            return self._optimize_quality_cluster(historical_data)
        elif workload_type == "analytics":
            return self._optimize_analytics_cluster(historical_data)
            
    def _optimize_ingestion_cluster(self, historical_data: Dict):
        """Optimize for high-throughput data ingestion"""
        
        # Analyze historical patterns
        avg_daily_volume = historical_data.get("avg_daily_volume_gb", 100)
        peak_hours = historical_data.get("peak_processing_hours", [8, 9, 10, 14, 15, 16])
        
        # Calculate optimal configuration
        if avg_daily_volume > 1000:  # > 1TB daily
            node_type = "i3.2xlarge"
            min_workers = 5
            max_workers = 25
        elif avg_daily_volume > 100:  # > 100GB daily
            node_type = "i3.xlarge"
            min_workers = 3
            max_workers = 15
        else:
            node_type = "i3.large"
            min_workers = 2
            max_workers = 8
            
        return {
            "node_type_id": node_type,
            "min_workers": min_workers,
            "max_workers": max_workers,
            "autoscale": {
                "min_workers": min_workers,
                "max_workers": max_workers
            },
            "auto_termination_minutes": 30,
            "enable_elastic_disk": True,
            "aws_attributes": {
                "instance_profile_arn": self.config.get("instance_profile_arn"),
                "availability": "SPOT_WITH_FALLBACK",
                "spot_bid_price_percent": 70
            }
        }
```

### Query Performance Optimization

#### Data Layout Optimization
```sql
-- Optimize table layout for healthcare queries
OPTIMIZE healthcare_data.silver.medicaid_claims
ZORDER BY (member_id, date_of_service, provider_npi);

-- Vacuum old files
VACUUM healthcare_data.silver.medicaid_claims RETAIN 168 HOURS; -- 7 days

-- Update table statistics
ANALYZE TABLE healthcare_data.silver.medicaid_claims COMPUTE STATISTICS;
ANALYZE TABLE healthcare_data.silver.medicaid_claims COMPUTE STATISTICS FOR ALL COLUMNS;
```

#### Performance Monitoring Queries
```sql
-- Monitor query performance
WITH query_performance AS (
  SELECT 
    query_id,
    start_time,
    end_time,
    execution_time_ms,
    rows_produced,
    bytes_produced,
    query_text
  FROM system.query_history
  WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
    AND query_text LIKE '%healthcare_data%'
)

SELECT 
  DATE(start_time) as query_date,
  COUNT(*) as query_count,
  AVG(execution_time_ms) as avg_execution_time,
  MAX(execution_time_ms) as max_execution_time,
  SUM(bytes_produced) / 1024 / 1024 / 1024 as total_gb_processed
FROM query_performance
GROUP BY DATE(start_time)
ORDER BY query_date DESC;
```

---

## Security Operations

### Security Monitoring

#### Security Event Detection
```python
# src/security/security_monitor.py
class SecurityMonitor:
    """Monitor security events and anomalies"""
    
    def __init__(self, config):
        self.config = config
        self.alert_thresholds = config.get("security_thresholds", {})
        
    def monitor_access_patterns(self):
        """Monitor unusual access patterns"""
        
        # Detect unusual access times
        unusual_access = self.detect_unusual_access_times()
        if unusual_access:
            self.send_security_alert("unusual_access_pattern", unusual_access)
            
        # Detect bulk data access
        bulk_access = self.detect_bulk_data_access()
        if bulk_access:
            self.send_security_alert("bulk_data_access", bulk_access)
            
        # Detect failed authentication attempts
        failed_auth = self.detect_failed_authentication()
        if failed_auth:
            self.send_security_alert("failed_authentication", failed_auth)
    
    def detect_phi_access(self):
        """Monitor PHI access and ensure compliance"""
        
        phi_access_query = """
        SELECT 
          user_id,
          table_name,
          accessed_columns,
          access_time,
          query_text
        FROM audit_logs
        WHERE accessed_columns LIKE '%member_id%'
           OR accessed_columns LIKE '%ssn%'
           OR accessed_columns LIKE '%phone%'
           AND access_time >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
        """
        
        phi_accesses = self.execute_query(phi_access_query)
        
        # Check for compliance violations
        violations = []
        for access in phi_accesses:
            if not self.validate_phi_access_authorization(access):
                violations.append(access)
                
        if violations:
            self.send_security_alert("phi_access_violation", violations)
            
        return violations
```

### Compliance Auditing

#### Audit Log Analysis
```sql
-- HIPAA compliance audit queries
-- 1. PHI access audit
SELECT 
  user_id,
  user_name,
  action_name,
  table_name,
  accessed_columns,
  request_params,
  access_time,
  source_ip_address
FROM audit_logs
WHERE (
  accessed_columns LIKE '%member_id%' OR
  accessed_columns LIKE '%ssn%' OR
  accessed_columns LIKE '%phone%' OR
  accessed_columns LIKE '%email%' OR
  accessed_columns LIKE '%address%'
)
AND access_time >= CURRENT_DATE - INTERVAL 30 DAY
ORDER BY access_time DESC;

-- 2. Data export audit
SELECT 
  user_id,
  action_name,
  table_name,
  rows_affected,
  export_destination,
  access_time
FROM audit_logs  
WHERE action_name IN ('EXPORT', 'DOWNLOAD', 'COPY')
AND access_time >= CURRENT_DATE - INTERVAL 90 DAY
ORDER BY access_time DESC;

-- 3. Administrative action audit
SELECT
  user_id,
  action_name,
  resource_type,
  resource_name,
  previous_value,
  new_value,
  access_time
FROM audit_logs
WHERE action_name IN ('CREATE', 'DROP', 'ALTER', 'GRANT', 'REVOKE')
AND access_time >= CURRENT_DATE - INTERVAL 30 DAY
ORDER BY access_time DESC;
```

---

## Operational Procedures

### Routine Maintenance

#### Weekly Maintenance Checklist
```bash
#!/bin/bash
# weekly_maintenance.sh - Weekly maintenance procedures

echo "🔧 Healthcare Platform Weekly Maintenance"
echo "========================================"

# 1. System health check
echo "1. Running comprehensive health check..."
python -m src.cli status --detailed --format json > health_report_$(date +%Y%m%d).json

# 2. Data quality assessment
echo "2. Running data quality assessment..."
python -m src.cli quality trends --days 7 --format json > quality_report_$(date +%Y%m%d).json

# 3. Cost analysis and optimization
echo "3. Analyzing costs and optimization opportunities..."
python scripts/cost_analysis.py --period weekly --output-file cost_report_$(date +%Y%m%d).json

# 4. Update cluster configurations based on usage patterns
echo "4. Optimizing cluster configurations..."
python scripts/optimize_clusters.py --dry-run

# 5. Clean up old logs and temporary files
echo "5. Cleaning up old files..."
python scripts/cleanup_old_files.py --days 30

# 6. Update backup retention policies
echo "6. Managing backup retention..."
python scripts/manage_backups.py --cleanup-old --retain-days 2555

# 7. Security scan
echo "7. Running security scan..."
python scripts/security_scan.py --comprehensive

# 8. Performance metrics collection
echo "8. Collecting performance metrics..."
python scripts/collect_metrics.py --period weekly

echo "✅ Weekly maintenance completed"
```

### Incident Response

#### Incident Response Playbook
```yaml
incident_response_playbook:
  # Severity Levels
  severity_levels:
    P0_critical:
      description: "Complete system outage or data breach"
      response_time: "15 minutes"
      escalation: "immediate"
      
    P1_high:
      description: "Significant functionality impaired"
      response_time: "1 hour"
      escalation: "2 hours"
      
    P2_medium:
      description: "Minor functionality impaired"
      response_time: "4 hours"
      escalation: "8 hours"
      
    P3_low:
      description: "Minor issues or feature requests"
      response_time: "24 hours"
      escalation: "48 hours"
  
  # Response Procedures
  response_procedures:
    data_quality_incident:
      triggers:
        - "quality_score < 60%"
        - "critical_validation_failures > 1000"
        - "processing_errors > 5%"
      
      immediate_actions:
        - "Stop affected pipelines"
        - "Isolate bad data"
        - "Notify stakeholders"
        - "Begin root cause analysis"
        
      recovery_steps:
        - "Identify data quality issue source"
        - "Apply corrective measures"
        - "Re-process affected data"
        - "Validate data quality restoration"
        - "Resume normal operations"
    
    system_outage:
      triggers:
        - "Service unavailable > 5 minutes"
        - "Multiple component failures"
        - "Complete dashboard outage"
        
      immediate_actions:
        - "Activate incident bridge"
        - "Assess scope of outage"
        - "Implement emergency procedures"
        - "Communicate with stakeholders"
        
      recovery_steps:
        - "Execute disaster recovery plan"
        - "Restore from backups if necessary"
        - "Validate system functionality"
        - "Gradual service restoration"
        - "Post-incident review"
```

---

## Best Practices

### Operational Excellence

1. **Automation First**: Automate routine tasks and deployments
2. **Infrastructure as Code**: Use Terraform/CloudFormation for all infrastructure
3. **Monitoring Everything**: Monitor performance, costs, quality, and security
4. **Regular Testing**: Test disaster recovery and backup procedures
5. **Documentation**: Maintain up-to-date operational documentation

### Cost Management

1. **Right-sizing**: Regularly review and optimize cluster sizes
2. **Spot Instances**: Use spot instances for non-critical workloads
3. **Auto-termination**: Configure aggressive auto-termination for dev/test
4. **Resource Tagging**: Tag all resources for cost allocation
5. **Regular Reviews**: Conduct monthly cost review meetings

### Security Best Practices

1. **Least Privilege**: Apply principle of least privilege access
2. **Regular Audits**: Conduct quarterly access reviews
3. **Encryption**: Encrypt all data at rest and in transit
4. **Monitoring**: Monitor all access to PHI data
5. **Incident Response**: Maintain and test incident response procedures

---

For more detailed information, refer to:
- [Security Guide](security.md)
- [Troubleshooting Guide](troubleshooting.md)
- [Cost Optimization Guide](cost-optimization.md)