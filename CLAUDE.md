# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Agentic Data Engineering Platform** - A self-healing, AI-powered data engineering platform for Medicaid/Medicare healthcare data processing. Built on Databricks with Unity Catalog, this platform delivers 40% reduction in manual fixes, 10-15% cost savings, and 99.2% SLA reliability.

## Key Architecture

### Core Modules
- **Ingestion Agent** (`src/agents/ingestion/`): Auto Loader + schema drift detection
- **Quality Agent** (`src/agents/quality/`): DLT expectations + healthcare validations  
- **Orchestration Agent** (`src/agents/orchestration/`): Jobs API + adaptive scaling + retry logic
- **Databricks Hooks** (`src/databricks_hooks/`): Unity Catalog + Delta Live Tables integration
- **UI Dashboard** (`src/ui/`): Streamlit monitoring dashboard

### Self-Healing Capabilities
- **Automatic retry** with exponential backoff and failure categorization
- **Schema drift adaptation** with forward/backward compatibility checking
- **Auto-scaling clusters** based on workload patterns and cost optimization
- **Quality issue repair** with quarantine and data correction workflows

## Development Commands

### Environment Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Edit .env with your Databricks credentials
```

### CLI Commands
```bash
# Initialize Unity Catalog
python -m src.cli catalog init

# Create pipeline
python -m src.cli pipeline create medicaid_claims --source-path s3://bucket/data/ --target-table healthcare_data.silver.claims

# Check data quality  
python -m src.cli quality check healthcare_data.silver.claims

# Start dashboard
python -m src.cli dashboard
# Or directly: streamlit run src/ui/dashboard.py
```

### Testing
```bash
# Run tests
pytest tests/

# With coverage
pytest --cov=src tests/

# Integration tests (requires Databricks)
pytest tests/integration/ -m integration
```

### Code Quality
```bash
# Format code
black src/ tests/
isort src/ tests/

# Type checking
mypy src/

# Linting  
flake8 src/ tests/
```

## Configuration

### Main Config: `config/config.yaml`
- Pipeline definitions with source/target mappings
- Quality thresholds and validation schemas
- Self-healing parameters (retry, scaling, cost optimization)
- Healthcare-specific validation rules (NPI, ICD-10, CPT codes)

### Environment: `.env` 
- Databricks workspace credentials
- Cloud storage access keys
- Notification webhooks (Slack, email)
- Security and encryption settings

## Healthcare Data Specifics

### Supported Formats
- **Medicaid Claims**: CMS-1500, UB-04 formats with state-specific variations
- **Medicare Claims**: Part A, B, C, D data with CMS compliance
- **Provider Data**: NPI registry, taxonomy codes, credentialing
- **Member Eligibility**: Enrollment periods, benefit coverage

### Quality Validations
- **Healthcare Codes**: NPI (Luhn algorithm), ICD-10, CPT, HCPCS validation
- **Business Rules**: Member eligibility, provider authorization, claim logic
- **Regulatory Compliance**: HIPAA, state Medicaid requirements
- **Data Integrity**: Duplicate detection, referential integrity, date validations

### Self-Healing Features
- **Schema Evolution**: Automatic adaptation to new fields and format changes
- **Cost Optimization**: Spot instances, auto-scaling, idle termination (10-15% savings)
- **Quality Recovery**: Quarantine bad records, auto-correction rules
- **Network Resilience**: Intelligent retry for transient failures

## Key Files for Development

### Core Agents
- `src/agents/ingestion/auto_loader.py` - Main ingestion logic with Auto Loader
- `src/agents/quality/dlt_quality_agent.py` - Healthcare-specific DLT expectations
- `src/agents/orchestration/jobs_orchestrator.py` - Pipeline job management
- `src/agents/orchestration/retry_handler.py` - Intelligent retry with failure analysis

### Databricks Integration  
- `src/databricks_hooks/unity_catalog_manager.py` - Schema registry and governance
- `src/databricks_hooks/delta_live_tables.py` - Multi-layered DLT pipelines

### Healthcare Validations
- `src/agents/quality/healthcare_expectations.py` - Medical code validation (NPI, ICD-10, CPT)

### Dashboard
- `src/ui/dashboard.py` - Main Streamlit dashboard with real-time monitoring
- `src/ui/components.py` - Reusable UI components for metrics, alerts, lineage

## Production Deployment

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Cloud storage with proper IAM permissions
- Environment variables configured for production

### Checklist
- [ ] Configure production Databricks workspace
- [ ] Set up Unity Catalog with healthcare data governance
- [ ] Configure encrypted cloud storage
- [ ] Set up monitoring and alerting (Slack, email)
- [ ] Test self-healing workflows
- [ ] Review HIPAA compliance settings

## Troubleshooting

### Common Issues
- **"Schema drift detected"**: Check `src/agents/ingestion/schema_drift_detector.py` for adaptation logic
- **"Quality expectations failed"**: Review validation rules in healthcare_expectations.py
- **"Cluster scaling issues"**: Check adaptive scaling logic in cluster_manager.py
- **"Databricks connection failed"**: Verify DATABRICKS_TOKEN and workspace URL in .env

### Monitoring
- Dashboard shows real-time pipeline health and self-healing actions
- Check monitoring tables in `healthcare_data.monitoring.*` for historical data
- Quality alerts stored in `healthcare_data.monitoring.quality_alerts`

## Healthcare Compliance Notes

- **HIPAA Compliance**: All PII/PHI data encrypted at rest and in transit
- **Data Governance**: Unity Catalog provides column-level security and lineage
- **Audit Trail**: Complete change history and access logs maintained
- **Retention**: 7-year data retention for healthcare regulatory compliance