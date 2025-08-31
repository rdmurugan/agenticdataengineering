# Installation Guide

This guide will help you install and configure the Agentic Data Engineering Platform for healthcare data processing.

## Prerequisites

### System Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| **Operating System** | Linux, macOS, Windows 10+ | Linux (Ubuntu 20.04+) |
| **Python** | 3.8+ | 3.9+ |
| **Memory** | 8 GB RAM | 16+ GB RAM |
| **Storage** | 10 GB free space | 50+ GB free space |
| **Network** | Internet connection | High-speed broadband |

### Cloud Platform Requirements

#### Databricks Workspace
- **Unity Catalog enabled** workspace
- **Admin permissions** for initial setup
- **Compute access** for cluster creation
- **Workspace files** access for deploying code

#### Cloud Storage
Choose one of the following:

**AWS S3**
- S3 bucket for raw data storage
- IAM role with S3 read/write permissions
- VPC endpoints (recommended for security)

**Azure Data Lake Storage (ADLS)**
- ADLS Gen2 storage account
- Service Principal with storage permissions
- Private endpoints (recommended)

**Google Cloud Storage (GCS)**
- GCS bucket for data storage
- Service Account with storage admin role
- Private Google Access enabled

### Access Requirements

- **Databricks workspace URL** and **Personal Access Token**
- **Cloud storage credentials** (access keys, service principals, etc.)
- **Network access** to Databricks APIs and cloud storage
- **Admin access** to configure Unity Catalog (initial setup only)

---

## Quick Installation

### 1. Clone Repository

```bash
# Clone the repository
git clone https://github.com/yourorg/agenticdataengineering.git
cd agenticdataengineering

# Verify structure
ls -la
```

### 2. Python Environment Setup

**Option A: Using venv (Recommended)**
```bash
# Create virtual environment
python -m venv venv

# Activate environment
# Linux/macOS:
source venv/bin/activate
# Windows:
venv\Scripts\activate

# Verify Python version
python --version  # Should be 3.8+
```

**Option B: Using conda**
```bash
# Create conda environment
conda create -n healthcare-platform python=3.9 -y
conda activate healthcare-platform
```

### 3. Install Dependencies

```bash
# Install core dependencies
pip install -r requirements.txt

# Verify installation
python -c "import databricks.sdk; print('✅ Databricks SDK installed')"
python -c "import streamlit; print('✅ Streamlit installed')"
```

### 4. Environment Configuration

```bash
# Copy environment template
cp .env.example .env

# Edit with your credentials
nano .env  # or your preferred editor
```

### 5. Verify Installation

```bash
# Test CLI
python -m src.cli --help

# Test imports
python -c "from src.agents.quality import QualityEngine; print('✅ Platform ready')"
```

---

## Detailed Installation Steps

### Step 1: Repository Setup

```bash
# Clone with specific branch (if needed)
git clone -b main https://github.com/yourorg/agenticdataengineering.git
cd agenticdataengineering

# Check repository status
git status
git log --oneline -5
```

### Step 2: Python Environment Management

**Virtual Environment Best Practices**
```bash
# Use specific Python version
python3.9 -m venv venv

# Upgrade pip and setuptools
pip install --upgrade pip setuptools wheel

# Install dependencies with version locking
pip install -r requirements.txt --no-deps
pip check  # Verify no dependency conflicts
```

**Environment Variables**
```bash
# Add to your shell profile (~/.bashrc, ~/.zshrc)
export HEALTHCARE_PLATFORM_HOME="$(pwd)"
export PATH="$HEALTHCARE_PLATFORM_HOME/venv/bin:$PATH"

# Reload shell or run:
source ~/.bashrc  # or ~/.zshrc
```

### Step 3: Core Dependencies Installation

**Required Packages**
```bash
# Core Databricks integration
pip install databricks-sdk>=0.20.0

# Data processing
pip install pyspark>=3.4.0 delta-spark>=2.4.0

# Web interface
pip install streamlit>=1.28.0 plotly>=5.17.0

# Data quality
pip install great-expectations>=0.17.0 pandera>=0.17.0

# Configuration and utilities
pip install pyyaml>=6.0 python-dotenv>=1.0.0 click>=8.1.0
```

**Optional Dependencies**
```bash
# For advanced features (install if needed)
pip install mlflow>=2.5.0          # ML model tracking
pip install prometheus-client>=0.17.0  # Advanced monitoring
pip install sentry-sdk>=1.38.0     # Error tracking
```

**Development Dependencies**
```bash
# Only for development environments
pip install -r requirements-dev.txt

# Or individually:
pip install pytest>=7.4.0 pytest-cov>=4.1.0 black>=23.0.0 mypy>=1.6.0
```

### Step 4: Environment Configuration

Create `.env` file with your specific configuration:

```bash
# Copy template
cp .env.example .env
```

**Required Environment Variables**

```bash
# Databricks Configuration
DATABRICKS_TOKEN=dapi-your-token-here
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_WORKSPACE_ID=your-workspace-id

# Unity Catalog Configuration
DATABRICKS_CATALOG_NAME=healthcare_data
DATABRICKS_SCHEMA_BRONZE=bronze
DATABRICKS_SCHEMA_SILVER=silver
DATABRICKS_SCHEMA_GOLD=gold
```

**Cloud Storage Configuration**

*AWS S3*
```bash
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1
S3_BUCKET_RAW_DATA=healthcare-raw-data
```

*Azure ADLS*
```bash
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret
AZURE_STORAGE_ACCOUNT=your-storage-account
```

*Google Cloud Storage*
```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
GCP_PROJECT_ID=your-project-id
GCS_BUCKET=healthcare-data-bucket
```

### Step 5: Databricks Configuration

**Personal Access Token Creation**
1. Log in to your Databricks workspace
2. Go to **User Settings** → **Developer** → **Access tokens**
3. Click **Generate new token**
4. Set expiration (recommend 90 days for production)
5. Copy token and add to `.env` file

**Unity Catalog Setup**
```bash
# Verify Unity Catalog access
python -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
catalogs = list(w.catalogs.list())
print(f'✅ Unity Catalog access verified. Found {len(catalogs)} catalogs')
"
```

### Step 6: Initial Platform Setup

**Initialize Unity Catalog Structure**
```bash
# Initialize the healthcare data catalog
python -m src.cli catalog init

# Verify catalog creation
python -m src.cli catalog health healthcare_data
```

**Test Platform Components**
```bash
# Test quality engine
python -c "
from src.agents.quality.quality_engine import QualityEngine
print('✅ Quality Engine imported successfully')
"

# Test dashboard
streamlit run src/ui/dashboard.py &
sleep 5
curl -f http://localhost:8501 && echo '✅ Dashboard accessible'
```

---

## Configuration Files

### Main Configuration (`config/config.yaml`)

```yaml
# Healthcare Data Platform Configuration
databricks:
  host: "${DATABRICKS_HOST}"
  token: "${DATABRICKS_TOKEN}"
  workspace_id: "${DATABRICKS_WORKSPACE_ID}"

unity_catalog:
  catalog_name: "healthcare_data"
  default_storage_location: "s3://healthcare-data-platform/catalog/"

pipelines:
  medicaid_claims:
    source_path: "s3://healthcare-raw-data/medicaid/claims/"
    target_table: "healthcare_data.silver.medicaid_claims"
    schedule: "0 */6 * * *"
    cluster_config:
      node_type: "i3.xlarge"
      min_workers: 2
      max_workers: 10
```

### Data Quality Configuration (`config/data_quality_config.yaml`)

This file is automatically included and contains healthcare-specific validation rules. You can customize thresholds:

```yaml
# Customize quality thresholds
global_thresholds:
  critical_quality_score: 0.60  # Adjust as needed
  warning_quality_score: 0.80
  target_quality_score: 0.95

# Add custom healthcare rules
healthcare_quality_rules:
  custom_member_validation:
    member_id_patterns:
      - "^[A-Z]{2}[0-9]{9}$"  # Your state format
```

---

## Verification & Testing

### Platform Health Check

```bash
# Run comprehensive health check
python -m src.cli status

# Expected output:
# ✅ System Health: 98.5%
# ✅ Data Quality: 96.2%
# ✅ Pipelines Running: 0/0 (none configured yet)
# ✅ Cost Optimization: Ready
# ✅ Self-Healing Actions: 0 today
```

### Component Tests

**1. Databricks Connectivity**
```bash
python -c "
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

# Test workspace connection
w = WorkspaceClient()
clusters = list(w.clusters.list())
print(f'✅ Databricks connected. Found {len(clusters)} clusters')

# Test Unity Catalog
catalogs = list(w.catalogs.list())
print(f'✅ Unity Catalog accessible. Found {len(catalogs)} catalogs')
"
```

**2. Storage Access**
```bash
# Test S3 access (if using AWS)
python -c "
import boto3
s3 = boto3.client('s3')
buckets = s3.list_buckets()['Buckets']
print(f'✅ S3 accessible. Found {len(buckets)} buckets')
"
```

**3. Dashboard Launch**
```bash
# Start dashboard in background
python -m src.cli dashboard --port 8501 &

# Wait and test
sleep 10
curl -f http://localhost:8501/_stcore/health
echo '✅ Dashboard healthy'

# Stop dashboard
pkill -f streamlit
```

### Sample Data Pipeline Test

```bash
# Create test pipeline (optional - requires data)
python -m src.cli pipeline create test_pipeline \
  --source-path s3://your-test-bucket/sample-data/ \
  --target-table healthcare_data.bronze.test_table

# Check pipeline status
python -m src.cli pipeline metrics test_pipeline
```

---

## Production Installation

### System Hardening

**Security Configuration**
```bash
# Set restrictive file permissions
chmod 600 .env
chmod -R 755 src/
chmod +x src/cli.py

# Create dedicated user (Linux)
sudo useradd -r -s /bin/false healthcare-platform
sudo chown -R healthcare-platform:healthcare-platform .
```

**Service Configuration**
```bash
# Create systemd service (Linux)
sudo tee /etc/systemd/system/healthcare-platform.service > /dev/null <<EOF
[Unit]
Description=Healthcare Data Platform Dashboard
After=network.target

[Service]
Type=simple
User=healthcare-platform
WorkingDirectory=/opt/healthcare-platform
ExecStart=/opt/healthcare-platform/venv/bin/python -m src.cli dashboard
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Enable and start service
sudo systemctl enable healthcare-platform
sudo systemctl start healthcare-platform
```

### Environment-Specific Configuration

**Development**
```bash
# Development-specific settings
export ENVIRONMENT=development
export DEBUG_MODE=true
export LOG_LEVEL=DEBUG
export MOCK_EXTERNAL_SERVICES=true
```

**Staging**
```bash
# Staging environment
export ENVIRONMENT=staging
export DEBUG_MODE=false
export LOG_LEVEL=INFO
export COST_OPTIMIZATION_ENABLED=false  # Disable for testing
```

**Production**
```bash
# Production environment
export ENVIRONMENT=production
export DEBUG_MODE=false
export LOG_LEVEL=WARNING
export HIPAA_COMPLIANT_MODE=true
export COST_OPTIMIZATION_ENABLED=true
```

---

## Docker Installation (Alternative)

### Using Docker Compose

```yaml
# docker-compose.yml
version: '3.8'
services:
  healthcare-platform:
    build: .
    ports:
      - "8501:8501"
    environment:
      - DATABRICKS_TOKEN=${DATABRICKS_TOKEN}
      - DATABRICKS_HOST=${DATABRICKS_HOST}
    volumes:
      - ./config:/app/config
      - ./logs:/app/logs
    restart: unless-stopped
```

```dockerfile
# Dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/ ./src/
COPY config/ ./config/

EXPOSE 8501
CMD ["python", "-m", "src.cli", "dashboard", "--host", "0.0.0.0"]
```

**Deploy with Docker**
```bash
# Build and run
docker-compose up -d

# Check logs
docker-compose logs -f healthcare-platform

# Health check
curl http://localhost:8501/_stcore/health
```

---

## Troubleshooting Installation

### Common Issues

**1. Python Version Conflicts**
```bash
# Issue: "Python version not supported"
# Solution: Use pyenv to manage Python versions
curl https://pyenv.run | bash
pyenv install 3.9.16
pyenv local 3.9.16
```

**2. Dependency Conflicts**
```bash
# Issue: Package conflicts during installation
# Solution: Use clean environment
pip cache purge
pip install --no-cache-dir --force-reinstall -r requirements.txt
```

**3. Databricks Connection Issues**
```bash
# Issue: "databricks.sdk authentication failed"
# Solution: Verify token and permissions
export DATABRICKS_TOKEN=your-token
python -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
print(w.current_user.me())
"
```

**4. Import Errors**
```bash
# Issue: "ModuleNotFoundError"
# Solution: Fix Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
# Or add to .bashrc/.zshrc
```

**5. Permission Errors**
```bash
# Issue: Permission denied
# Solution: Fix file permissions
sudo chown -R $USER:$USER .
chmod -R u+rwX .
```

### Getting Help

If you encounter issues:

1. **Check logs**: Look in `/tmp/healthcare-pipeline.log`
2. **Verify configuration**: Run `python -m src.cli status`
3. **Test connectivity**: Use the verification commands above
4. **Consult documentation**: See [Troubleshooting Guide](troubleshooting.md)
5. **Open an issue**: [GitHub Issues](https://github.com/yourorg/agenticdataengineering/issues)

---

## Next Steps

After successful installation:

1. **[Configuration Guide](configuration.md)** - Detailed platform configuration
2. **[Quick Start Tutorial](quickstart.md)** - Create your first pipeline
3. **[CLI Reference](cli/README.md)** - Learn all available commands
4. **[Dashboard Guide](dashboard/README.md)** - Explore monitoring capabilities

---

**Installation Complete! 🎉**

Your Agentic Data Engineering Platform is now ready for healthcare data processing.