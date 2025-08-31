# Configuration Guide

Complete guide to configuring the Agentic Data Engineering Platform for your healthcare data processing needs.

## Overview

The platform uses a hierarchical configuration system:

1. **Environment Variables** (`.env`) - Credentials and environment-specific settings
2. **Main Configuration** (`config/config.yaml`) - Platform and pipeline settings
3. **Quality Rules** (`config/data_quality_config.yaml`) - Data validation rules
4. **Runtime Configuration** - Dynamic settings through CLI and API

---

## Environment Configuration

### .env File Structure

```bash
# ==============================================================================
# DATABRICKS CONFIGURATION
# ==============================================================================

# Databricks Workspace Settings
DATABRICKS_TOKEN=dapi-your-token-here
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_WORKSPACE_ID=your-workspace-id

# Unity Catalog Configuration
DATABRICKS_CATALOG_NAME=healthcare_data
DATABRICKS_SCHEMA_BRONZE=bronze
DATABRICKS_SCHEMA_SILVER=silver  
DATABRICKS_SCHEMA_GOLD=gold
DATABRICKS_SCHEMA_MONITORING=monitoring

# ==============================================================================
# CLOUD STORAGE CONFIGURATION
# ==============================================================================

# AWS S3 Configuration
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
AWS_REGION=us-east-1
S3_BUCKET_RAW_DATA=healthcare-raw-data
S3_BUCKET_PROCESSED=healthcare-processed-data
S3_BUCKET_ARCHIVE=healthcare-archive

# Azure Configuration (alternative to AWS)
AZURE_TENANT_ID=your-azure-tenant-id
AZURE_CLIENT_ID=your-azure-client-id
AZURE_CLIENT_SECRET=your-azure-client-secret
AZURE_STORAGE_ACCOUNT=your-storage-account
AZURE_STORAGE_KEY=your-storage-key
AZURE_CONTAINER_RAW=healthcare-raw
AZURE_CONTAINER_PROCESSED=healthcare-processed

# Google Cloud Configuration (alternative)
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
GCP_PROJECT_ID=your-gcp-project-id
GCS_BUCKET=healthcare-data-platform

# ==============================================================================
# NOTIFICATION CONFIGURATION
# ==============================================================================

# Email Notifications
EMAIL_SMTP_SERVER=smtp.yourdomain.com
EMAIL_SMTP_PORT=587
EMAIL_USERNAME=your_email@company.com
EMAIL_PASSWORD=your_email_password
EMAIL_FROM_ADDRESS=data-platform@company.com
EMAIL_RECIPIENTS=data-team@company.com,ops-team@company.com

# Slack Integration
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
SLACK_CHANNEL=#data-quality-alerts
SLACK_USERNAME=Healthcare Data Bot

# PagerDuty (for critical alerts)
PAGERDUTY_API_KEY=your_pagerduty_integration_key
PAGERDUTY_SERVICE_KEY=your_service_key

# ==============================================================================
# SECURITY CONFIGURATION
# ==============================================================================

# Encryption Keys (generate secure 32-byte keys)
DATA_ENCRYPTION_KEY=your_32_byte_encryption_key_here
CONFIG_ENCRYPTION_KEY=your_32_byte_config_key_here

# Healthcare Compliance
HIPAA_COMPLIANT_MODE=true
PHI_DETECTION_ENABLED=true
AUDIT_LOG_RETENTION_DAYS=2555  # 7 years for healthcare compliance

# ==============================================================================
# PLATFORM CONFIGURATION
# ==============================================================================

# Environment Settings
ENVIRONMENT=development  # development, staging, production
LOG_LEVEL=INFO
DEBUG_MODE=false
MONITORING_ENABLED=true

# Self-Healing Configuration
AUTO_RETRY_ENABLED=true
MAX_RETRY_ATTEMPTS=3
RETRY_BACKOFF_MULTIPLIER=2
AUTO_SCALING_ENABLED=true
COST_OPTIMIZATION_ENABLED=true

# Dashboard Configuration
DASHBOARD_PORT=8501
DASHBOARD_HOST=localhost
DASHBOARD_REFRESH_INTERVAL_SECONDS=30

# ==============================================================================
# HEALTHCARE-SPECIFIC CONFIGURATION
# ==============================================================================

# NPI Registry API (for provider validation)
NPI_REGISTRY_API_URL=https://npiregistry.cms.hhs.gov/api/
NPI_REGISTRY_API_KEY=your_npi_api_key

# Medical Code Validation
ICD10_VALIDATION_ENABLED=true
CPT_VALIDATION_ENABLED=true
HCPCS_VALIDATION_ENABLED=true

# Provider Network Configuration
PROVIDER_NETWORK_VALIDATION=true
PROVIDER_CREDENTIALING_CHECK=true

# Claims Processing
CLAIMS_PROCESSING_BATCH_SIZE=10000
CLAIMS_VALIDATION_STRICT_MODE=true
DUPLICATE_CLAIMS_DETECTION=true

# Member Eligibility
ELIGIBILITY_VERIFICATION_ENABLED=true
ELIGIBILITY_CACHE_TTL_MINUTES=60
```

### Environment-Specific Configurations

**Development Environment**
```bash
# Development settings
ENVIRONMENT=development
DEBUG_MODE=true
LOG_LEVEL=DEBUG
MOCK_EXTERNAL_SERVICES=true
COST_OPTIMIZATION_ENABLED=false
AUTO_TERMINATION_MINUTES=15
```

**Staging Environment**
```bash
# Staging settings
ENVIRONMENT=staging
DEBUG_MODE=false
LOG_LEVEL=INFO
MOCK_EXTERNAL_SERVICES=false
COST_OPTIMIZATION_ENABLED=true
AUTO_TERMINATION_MINUTES=30
```

**Production Environment**
```bash
# Production settings
ENVIRONMENT=production
DEBUG_MODE=false
LOG_LEVEL=WARNING
MOCK_EXTERNAL_SERVICES=false
COST_OPTIMIZATION_ENABLED=true
SPOT_INSTANCES_ENABLED=false  # Use on-demand for reliability
AUTO_TERMINATION_MINUTES=60
```

---

## Main Platform Configuration

### config/config.yaml Structure

```yaml
# ==============================================================================
# DATABRICKS CONFIGURATION
# ==============================================================================
databricks:
  host: "${DATABRICKS_HOST}"
  token: "${DATABRICKS_TOKEN}"
  workspace_id: "${DATABRICKS_WORKSPACE_ID}"

# Unity Catalog Configuration
unity_catalog:
  catalog_name: "healthcare_data"
  default_storage_location: "s3://healthcare-data-platform/catalog/"
  
# Spark Configuration
spark:
  version: "13.3.x-scala2.12"
  driver_memory: "8g"
  executor_memory: "16g"
  executor_cores: 4
  adaptive_enabled: true
  photon_enabled: true

# ==============================================================================
# PIPELINE CONFIGURATIONS
# ==============================================================================
pipelines:
  # Medicaid Claims Pipeline
  medicaid_claims:
    source_path: "s3://healthcare-raw-data/medicaid/claims/"
    target_table: "healthcare_data.silver.medicaid_claims"
    schedule: "0 */6 * * *"  # Every 6 hours
    cluster_config:
      node_type: "i3.xlarge"
      min_workers: 2
      max_workers: 10
      auto_termination_minutes: 30
    quality_thresholds:
      min_quality_score: 0.85
      max_anomaly_percentage: 5.0
      max_stale_percentage: 10.0
    retry_config:
      max_retries: 3
      base_delay_seconds: 60
      max_delay_seconds: 1800

  # Medicare Claims Pipeline
  medicare_claims:
    source_path: "s3://healthcare-raw-data/medicare/claims/"
    target_table: "healthcare_data.silver.medicare_claims"
    schedule: "0 */4 * * *"  # Every 4 hours
    cluster_config:
      node_type: "i3.xlarge"
      min_workers: 3
      max_workers: 12
      auto_termination_minutes: 30
    quality_thresholds:
      min_quality_score: 0.87
      max_anomaly_percentage: 4.0
      max_stale_percentage: 8.0

  # Provider Data Pipeline
  provider_data:
    source_path: "s3://healthcare-raw-data/providers/"
    target_table: "healthcare_data.silver.providers"
    schedule: "0 6 * * *"  # Daily at 6 AM
    cluster_config:
      node_type: "i3.large"
      min_workers: 1
      max_workers: 4
      auto_termination_minutes: 15
    quality_thresholds:
      min_quality_score: 0.90
      max_anomaly_percentage: 2.0
      max_stale_percentage: 24.0

# ==============================================================================
# SELF-HEALING CONFIGURATION
# ==============================================================================
self_healing:
  # Retry Configuration
  retry:
    max_retries: 3
    base_delay_seconds: 60
    max_delay_seconds: 1800
    jitter_enabled: true
    exponential_base: 2

  # Auto-scaling Configuration
  auto_scaling:
    enabled: true
    scale_up_threshold: 0.80    # CPU/Memory utilization
    scale_down_threshold: 0.30
    scale_up_increment: 2       # Number of workers to add
    scale_down_increment: 1     # Number of workers to remove
    cooldown_minutes: 10        # Wait time between scaling events
    min_workers_override: 1     # Never scale below this
    max_workers_override: 20    # Never scale above this

  # Schema Drift Handling
  schema_drift:
    auto_adapt: true
    breaking_change_action: "quarantine"  # quarantine, fail, ignore
    notification_required: true
    compatibility_check: true
    backup_schema_versions: 3

  # Cost Optimization
  cost_optimization:
    spot_instances: true
    spot_bid_percentage: 70
    auto_termination_minutes: 30
    idle_timeout_minutes: 15
    enable_cluster_pooling: true

# ==============================================================================
# MONITORING & ALERTING CONFIGURATION
# ==============================================================================
monitoring:
  # Dashboard Configuration
  dashboard:
    refresh_interval_seconds: 30
    data_retention_days: 90
    max_concurrent_users: 50

  # Metrics Collection
  metrics:
    enabled: true
    export_interval_seconds: 60
    custom_metrics_enabled: true
    retention_days: 90

  # Health Checks
  health_checks:
    pipeline_health_check_minutes: 5
    data_freshness_check_minutes: 15
    quality_check_minutes: 30
    cluster_health_check_minutes: 10

  # Storage Locations
  storage:
    metrics_table: "healthcare_data.monitoring.pipeline_metrics"
    quality_table: "healthcare_data.monitoring.data_quality_metrics"
    alerts_table: "healthcare_data.monitoring.quality_alerts"
    events_table: "healthcare_data.monitoring.pipeline_events"

# ==============================================================================
# DATA QUALITY CONFIGURATION
# ==============================================================================
data_quality:
  # Reference to external quality config
  config_file: "config/data_quality_config.yaml"
  
  # Healthcare-specific validation schemas
  validation_schemas:
    medicaid_claims:
      member_id: "medicaid_member_id"
      provider_npi: "npi"
      diagnosis_code: "icd10_diagnosis"
      procedure_code: "cpt_procedure"
      date_of_service: "service_date"
      claim_amount: "currency_amount"
      place_of_service: "pos_code"

    medicare_claims:
      member_id: "medicare_member_id"
      provider_npi: "npi"
      diagnosis_code: "icd10_diagnosis"
      procedure_code: "cpt_procedure"
      date_of_service: "service_date"
      claim_amount: "currency_amount"
      provider_taxonomy: "taxonomy_code"

  # Monitoring Configuration
  monitoring:
    quality_check_frequency: "hourly"
    alert_thresholds:
      critical_quality_score: 0.70
      warning_quality_score: 0.80
      high_anomaly_rate: 0.10
      max_processing_delay_hours: 6

# ==============================================================================
# SECURITY CONFIGURATION
# ==============================================================================
security:
  # Encryption Configuration
  encryption:
    at_rest: true
    in_transit: true
    key_management: "databricks_managed"  # or "customer_managed"

  # Access Control
  access_control:
    rbac_enabled: true
    table_acls_enabled: true
    column_level_security: true
    ip_whitelist_enabled: false

  # Data Classification
  data_classification:
    phi_detection: true
    pii_detection: true
    automatic_tagging: true
    masking_rules:
      member_id: "hash"
      ssn: "mask"
      phone: "partial_mask"
      email: "domain_mask"

  # Audit Configuration
  audit:
    log_all_access: true
    log_data_changes: true
    retention_days: 2555  # 7 years for healthcare compliance
    export_enabled: true
    export_format: "json"

# ==============================================================================
# ENVIRONMENT-SPECIFIC OVERRIDES
# ==============================================================================
environments:
  development:
    databricks:
      host: "https://dev-workspace.cloud.databricks.com"
    unity_catalog:
      catalog_name: "healthcare_data_dev"
    cost_optimization:
      spot_instances: true
      auto_termination_minutes: 15
    monitoring:
      data_retention_days: 30

  staging:
    databricks:
      host: "https://staging-workspace.cloud.databricks.com"
    unity_catalog:
      catalog_name: "healthcare_data_staging"
    cost_optimization:
      spot_instances: true
      auto_termination_minutes: 30
    monitoring:
      data_retention_days: 60

  production:
    databricks:
      host: "https://prod-workspace.cloud.databricks.com"
    unity_catalog:
      catalog_name: "healthcare_data_prod"
    cost_optimization:
      spot_instances: false  # Use on-demand for production
      auto_termination_minutes: 60
    monitoring:
      data_retention_days: 90
```

---

## Quality Rules Configuration

### Healthcare-Specific Validation Rules

The `config/data_quality_config.yaml` file contains comprehensive healthcare validation rules. Key sections you may want to customize:

**Global Quality Thresholds**
```yaml
global_thresholds:
  critical_quality_score: 0.60    # Adjust based on your requirements
  warning_quality_score: 0.80     # Customize warning levels
  target_quality_score: 0.95      # Set your quality targets
  
  # Completeness thresholds
  critical_completeness: 0.70      # Minimum acceptable completeness
  warning_completeness: 0.85       # Warning threshold
  target_completeness: 0.95        # Target completeness
```

**State-Specific Member ID Formats**
```yaml
healthcare_quality_rules:
  member_data:
    field_validations:
      member_id:
        patterns:
          - "^[0-9]{9,12}$"                    # Generic 9-12 digits
          - "^[A-Z]{1,3}[0-9]{6,9}$"          # State prefix format
          - "^CA[0-9]{9}$"                     # California specific
          - "^NY[0-9]{8}[A-Z]$"               # New York specific  
          - "^TX[A-Z][0-9]{8}$"               # Texas specific
```

**Custom Business Rules**
```yaml
healthcare_quality_rules:
  claims_data:
    business_rules:
      - name: "custom_eligibility_check"
        description: "Your organization's eligibility rules"
        rule: |
          EXISTS (
            SELECT 1 FROM your_eligibility_table e
            WHERE e.member_id = claims.member_id
            AND claims.date_of_service BETWEEN e.effective_date AND e.term_date
          )
        severity: "critical"
```

---

## Advanced Configuration

### Multi-State Medicaid Configuration

For organizations processing multiple states' Medicaid data:

```yaml
# Add to config/config.yaml
state_configurations:
  california:
    member_id_format: "^CA[0-9]{9}$"
    specific_validations:
      - "ca_specific_provider_network"
      - "ca_taxonomy_requirements"
    data_sources:
      - "s3://medicaid-data/CA/claims/"
      - "s3://medicaid-data/CA/eligibility/"

  new_york:
    member_id_format: "^[0-9]{8}NY$"
    specific_validations:
      - "ny_managed_care_validation"
      - "ny_ltss_requirements"
    data_sources:
      - "s3://medicaid-data/NY/claims/"
      - "s3://medicaid-data/NY/eligibility/"

  texas:
    member_id_format: "^TX[A-Z][0-9]{8}$"
    specific_validations:
      - "tx_star_program_validation"
      - "tx_chip_requirements"
    data_sources:
      - "s3://medicaid-data/TX/claims/"
      - "s3://medicaid-data/TX/eligibility/"
```

### High-Volume Processing Configuration

For high-volume environments (>1M records/hour):

```yaml
# Optimize for high volume
pipelines:
  high_volume_claims:
    cluster_config:
      node_type: "i3.4xlarge"  # Larger instances
      min_workers: 5
      max_workers: 50          # Scale up significantly
      enable_autoscaling: true
    
    processing_options:
      batch_size: 50000        # Larger batch sizes
      parallel_streams: 8      # More parallel processing
      checkpointing_interval: 1000  # More frequent checkpoints
      
    performance_tuning:
      adaptive_query_execution: true
      dynamic_partition_pruning: true
      photon_enabled: true     # Use Photon for performance
```

### Security Hardening Configuration

For highly secure environments:

```yaml
security:
  enhanced_security:
    network_security:
      private_endpoints_only: true
      vpc_endpoints_required: true
      ssl_verification: "strict"
      
    data_encryption:
      customer_managed_keys: true
      key_rotation_days: 90
      encryption_at_rest_algorithm: "AES-256"
      encryption_in_transit_tls: "1.3"
      
    access_control:
      mfa_required: true
      session_timeout_minutes: 30
      ip_whitelist_strict: true
      privileged_access_logging: true
      
    audit_configuration:
      detailed_audit_logging: true
      real_time_monitoring: true
      anomaly_detection: true
      export_to_siem: true
```

---

## Configuration Validation

### Validate Configuration

```bash
# Validate environment configuration
python -m src.cli config validate

# Test specific components
python -m src.cli config test-databricks
python -m src.cli config test-storage
python -m src.cli config test-quality-rules
```

### Configuration Health Check

```bash
# Run comprehensive configuration check
python -c "
from src.config_validator import ConfigValidator
validator = ConfigValidator()
results = validator.validate_all()
for component, status in results.items():
    print(f'{component}: {"✅" if status else "❌"}')
"
```

### Common Configuration Issues

**1. Missing Required Environment Variables**
```bash
# Check for missing variables
python -c "
import os
required_vars = ['DATABRICKS_TOKEN', 'DATABRICKS_HOST', 'AWS_ACCESS_KEY_ID']
missing = [var for var in required_vars if not os.getenv(var)]
if missing:
    print(f'❌ Missing: {missing}')
else:
    print('✅ All required variables present')
"
```

**2. Invalid Databricks Configuration**
```bash
# Test Databricks connectivity
python -c "
from databricks.sdk import WorkspaceClient
try:
    w = WorkspaceClient()
    user = w.current_user.me()
    print(f'✅ Connected as: {user.display_name}')
except Exception as e:
    print(f'❌ Connection failed: {e}')
"
```

**3. Storage Access Issues**
```bash
# Test storage access
python -c "
import boto3
from botocore.exceptions import NoCredentialsError
try:
    s3 = boto3.client('s3')
    s3.list_buckets()
    print('✅ S3 access working')
except NoCredentialsError:
    print('❌ AWS credentials not configured')
except Exception as e:
    print(f'❌ S3 access failed: {e}')
"
```

---

## Best Practices

### Configuration Management

1. **Version Control**: Keep configuration files in version control (exclude `.env`)
2. **Environment Separation**: Use different configurations for dev/staging/prod
3. **Secret Management**: Use secure secret storage (AWS Secrets Manager, Azure Key Vault)
4. **Validation**: Validate configurations before deployment
5. **Documentation**: Document all custom configurations

### Security Best Practices

1. **Credential Rotation**: Rotate credentials regularly
2. **Least Privilege**: Use minimum required permissions
3. **Network Security**: Use private endpoints where possible
4. **Encryption**: Enable encryption at rest and in transit
5. **Audit Logging**: Enable comprehensive audit logging

### Performance Optimization

1. **Resource Sizing**: Right-size clusters for workloads
2. **Cost Monitoring**: Monitor and optimize costs regularly
3. **Auto-scaling**: Use auto-scaling for variable workloads
4. **Caching**: Enable caching where appropriate
5. **Performance Monitoring**: Monitor performance metrics

---

## Configuration Templates

### Small Organization Template

For organizations processing <100K records/day:

```yaml
# Small organization configuration
pipelines:
  medicaid_claims:
    cluster_config:
      node_type: "i3.large"
      min_workers: 1
      max_workers: 3
    processing_options:
      batch_size: 5000
      parallel_streams: 2

cost_optimization:
  spot_instances: true
  spot_bid_percentage: 60
  auto_termination_minutes: 15
```

### Large Organization Template

For organizations processing >1M records/day:

```yaml
# Large organization configuration
pipelines:
  medicaid_claims:
    cluster_config:
      node_type: "i3.2xlarge"
      min_workers: 5
      max_workers: 20
    processing_options:
      batch_size: 25000
      parallel_streams: 8

cost_optimization:
  spot_instances: false  # Use on-demand for reliability
  auto_termination_minutes: 60
```

---

## Next Steps

After configuring the platform:

1. **[Quick Start Tutorial](quickstart.md)** - Create your first pipeline
2. **[CLI Reference](cli/README.md)** - Learn command-line operations
3. **[Quality Rules Guide](quality/README.md)** - Customize data validation
4. **[Monitoring Guide](monitoring.md)** - Set up monitoring and alerting

For questions about configuration, see the **[Troubleshooting Guide](troubleshooting.md)** or open an issue on GitHub.