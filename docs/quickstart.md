# Quick Start Guide

Get up and running with the Agentic Data Engineering Platform for healthcare data processing in under 30 minutes.

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Python 3.8 or higher
- AWS S3, Azure ADLS, or Google Cloud Storage access
- Sample healthcare data (we'll provide test data)

## Step 1: Installation (5 minutes)

### Clone and Setup
```bash
# Clone the repository
git clone https://github.com/rdmurugan/agenticdataengineering.git
cd agenticdataengineering

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Configure Environment
```bash
# Copy environment template
cp .env.example .env

# Edit with your credentials (use your preferred editor)
nano .env
```

**Required settings in .env:**
```bash
# Databricks Configuration
DATABRICKS_TOKEN=dapi-your-token-here
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com

# Storage Configuration (choose one)
# AWS S3
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
S3_BUCKET_RAW_DATA=your-data-bucket

# Or Azure
AZURE_STORAGE_ACCOUNT=your-storage-account
AZURE_STORAGE_KEY=your-storage-key

# Or GCP
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
GCS_BUCKET=your-data-bucket
```

## Step 2: Initialize Platform (5 minutes)

### Setup Unity Catalog
```bash
# Initialize healthcare data catalog
python -m src.cli catalog init

# Verify setup
python -m src.cli catalog health healthcare_data
```

### Test Connection
```bash
# Verify platform status
python -m src.cli status

# Expected output:
# ✅ System Health: 98.5%
# ✅ Databricks Connection: Active
# ✅ Unity Catalog: Available
# ✅ Storage Access: Configured
```

## Step 3: Launch Dashboard (2 minutes)

```bash
# Start the monitoring dashboard
python -m src.cli dashboard

# Dashboard will be available at http://localhost:8501
```

**Dashboard Features:**
- Real-time pipeline monitoring
- Data quality metrics
- Cost optimization insights
- Healthcare-specific validations

## Step 4: Process Sample Data (10 minutes)

### Download Sample Data
```bash
# Download sample Medicaid claims data
curl -o sample_medicaid_claims.parquet \
  https://github.com/rdmurugan/agenticdataengineering/releases/download/v1.0.0/sample_medicaid_claims.parquet

# Upload to your storage (example for S3)
aws s3 cp sample_medicaid_claims.parquet s3://your-data-bucket/medicaid/claims/
```

### Create Your First Pipeline
```bash
# Create Medicaid claims processing pipeline
python -m src.cli pipeline create medicaid_claims_quickstart \
  --source-path s3://your-data-bucket/medicaid/claims/ \
  --target-table healthcare_data.silver.medicaid_claims \
  --data-type medicaid_claims \
  --cluster-profile small
```

### Run the Pipeline
```bash
# Trigger pipeline execution
python -m src.cli pipeline run medicaid_claims_quickstart --wait

# Monitor progress
python -m src.cli pipeline logs medicaid_claims_quickstart --follow
```

### Verify Results
```bash
# Check data quality
python -m src.cli quality check healthcare_data.silver.medicaid_claims

# View processed data
python -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
result = w.sql.execute('SELECT COUNT(*) as record_count FROM healthcare_data.silver.medicaid_claims')
print(f'Processed records: {result.fetchall()[0][0]}')
"
```

## Step 5: Explore Key Features (5 minutes)

### Healthcare Data Validation
```bash
# Run comprehensive healthcare code validation
python -m src.cli healthcare validate-codes \
  --file sample_npi_codes.txt \
  --code-type npi

# Check ICD-10 diagnosis codes
python -m src.cli healthcare validate-codes \
  --codes "Z00.00,I10,E11.9" \
  --code-type icd10
```

### Quality Monitoring
```bash
# View quality trends
python -m src.cli quality trends \
  --table healthcare_data.silver.medicaid_claims \
  --days 7

# Set up quality alerts
python -m src.cli quality alerts healthcare_data.silver.medicaid_claims \
  --min-quality 0.85 \
  --max-anomaly 5.0
```

### Cost Optimization
```bash
# Check current costs
python -m src.cli cluster monitor medicaid_claims_quickstart \
  --cost-analysis

# Get optimization recommendations
python -m src.cli cluster optimize medicaid_claims_quickstart --dry-run
```

## Step 6: Self-Healing Demo (3 minutes)

### Simulate Schema Evolution
```bash
# Create new data with additional column
python scripts/generate_sample_data.py \
  --output s3://your-data-bucket/medicaid/claims/new_format/ \
  --add-column member_phone

# Pipeline will automatically adapt
python -m src.cli pipeline run medicaid_claims_quickstart

# View schema evolution log
python -m src.cli schema evolution-log \
  --table healthcare_data.silver.medicaid_claims
```

### Test Quality Recovery
```bash
# Introduce data quality issues
python scripts/generate_bad_data.py \
  --output s3://your-data-bucket/medicaid/claims/bad_data/ \
  --error-rate 0.1

# Platform will quarantine bad data and alert
python -m src.cli pipeline run medicaid_claims_quickstart

# Check quarantine tables
python -m src.cli quality check healthcare_data.silver.medicaid_claims \
  --show-quarantine
```

## What You've Accomplished

✅ **Deployed** a production-ready healthcare data platform
✅ **Processed** real Medicaid claims data with validation
✅ **Monitored** data quality with healthcare-specific rules
✅ **Experienced** self-healing schema evolution
✅ **Optimized** costs with intelligent auto-scaling

## Next Steps

### Expand Your Data Processing
1. **Add Medicare Data**: [Medicare Pipeline Setup](medicare-setup.md)
2. **Configure Multiple States**: [Multi-State Setup](multi-state-config.md)
3. **Provider Data Integration**: [Provider Data Guide](provider-data.md)

### Production Deployment
1. **Security Hardening**: [Security Guide](security.md)
2. **Production Deployment**: [Deployment Guide](deployment.md)
3. **Monitoring & Alerting**: [Monitoring Guide](monitoring.md)

### Advanced Features
1. **Custom Validation Rules**: [Quality Rules Guide](quality/README.md)
2. **API Integration**: [API Documentation](api/README.md)
3. **Cost Optimization**: [Cost Optimization Guide](cost-optimization.md)

## Sample Queries

Once data is processed, try these queries:

```sql
-- View processed claims summary
SELECT 
    COUNT(*) as total_claims,
    SUM(allowed_amount) as total_amount,
    AVG(allowed_amount) as avg_claim_amount,
    COUNT(DISTINCT member_id) as unique_members,
    COUNT(DISTINCT provider_npi) as unique_providers
FROM healthcare_data.silver.medicaid_claims;

-- Quality metrics by provider
SELECT 
    provider_npi,
    COUNT(*) as claim_count,
    AVG(CASE WHEN data_quality_score > 0.9 THEN 1.0 ELSE 0.0 END) as quality_rate
FROM healthcare_data.silver.medicaid_claims
GROUP BY provider_npi
ORDER BY claim_count DESC
LIMIT 10;

-- Claims by diagnosis category
SELECT 
    LEFT(primary_diagnosis_code, 1) as icd_category,
    COUNT(*) as claim_count,
    SUM(allowed_amount) as total_amount
FROM healthcare_data.silver.medicaid_claims
GROUP BY LEFT(primary_diagnosis_code, 1)
ORDER BY claim_count DESC;
```

## Troubleshooting

### Common Issues

**Pipeline fails to start:**
```bash
# Check cluster status
python -m src.cli cluster list --status all

# Verify permissions
python -m src.cli catalog check-permissions healthcare_data
```

**Data quality issues:**
```bash
# View detailed quality report
python -m src.cli quality check healthcare_data.silver.medicaid_claims \
  --assessment-type comprehensive

# Check quarantine reasons
SELECT quarantine_reason, COUNT(*) 
FROM healthcare_data.monitoring.quarantine_log 
GROUP BY quarantine_reason;
```

**Performance problems:**
```bash
# Optimize table
python -m src.cli table optimize healthcare_data.silver.medicaid_claims

# Check cluster utilization
python -m src.cli cluster stats --detailed
```

## Support

- **Documentation**: [Complete Documentation Index](README.md#documentation)
- **Troubleshooting**: [Troubleshooting Guide](troubleshooting.md)
- **Contact**: durai@infinidatum.net

## What's Next?

This quick start covered the basics. For production use:

1. **Scale Up**: Configure larger clusters for high-volume processing
2. **Multi-Tenant**: Set up isolated environments for different organizations
3. **Advanced Analytics**: Build gold layer tables for business intelligence
4. **Compliance**: Enable full HIPAA compliance features
5. **Integration**: Connect to existing healthcare systems and EHRs

Welcome to intelligent healthcare data engineering! 🏥✨