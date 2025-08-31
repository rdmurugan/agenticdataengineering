# CLI Reference

Complete command-line interface reference for the Agentic Data Engineering Platform.

## Overview

The Healthcare Data Platform CLI provides a unified interface for managing pipelines, monitoring data quality, and controlling platform operations. Built with Click, it offers intuitive commands with comprehensive help and validation.

### Installation

The CLI is automatically available after installing the platform:

```bash
# Verify installation
python -m src.cli --help

# Or use the shorthand (if configured)
healthcare-cli --help
```

### Global Options

All commands support these global options:

```bash
python -m src.cli [GLOBAL_OPTIONS] COMMAND [ARGS]

Global Options:
  -c, --config PATH    Configuration file path (default: config/config.yaml)
  -v, --verbose        Enable verbose output
  -q, --quiet          Suppress non-error output
  --dry-run           Show what would be done without executing
  --format FORMAT     Output format: table, json, yaml (default: table)
  --no-color          Disable colored output
  --help              Show help message and exit
```

---

## Unity Catalog Commands

Manage Unity Catalog structure and metadata.

### catalog init

Initialize Unity Catalog structure for healthcare data.

```bash
python -m src.cli catalog init [OPTIONS]

Options:
  --catalog-name TEXT     Catalog name (default: healthcare_data)
  --force                 Force recreation if catalog exists
  --skip-permissions      Skip permission setup
  --dry-run              Show what would be created

Examples:
  # Initialize default catalog structure
  python -m src.cli catalog init

  # Initialize with custom catalog name
  python -m src.cli catalog init --catalog-name my_healthcare_data

  # Force recreation of existing catalog
  python -m src.cli catalog init --force
```

**What it creates:**
- Catalog: `healthcare_data`
- Schemas: `bronze`, `silver`, `gold`, `monitoring`
- Tables: Monitoring and metadata tables
- Permissions: Default access controls

### catalog health

Check Unity Catalog health and permissions.

```bash
python -m src.cli catalog health [CATALOG_NAME] [OPTIONS]

Arguments:
  CATALOG_NAME           Catalog to check (default: healthcare_data)

Options:
  --detailed            Show detailed health information
  --check-permissions   Verify user permissions
  --format FORMAT       Output format: table, json

Examples:
  # Basic health check
  python -m src.cli catalog health

  # Detailed health check with permissions
  python -m src.cli catalog health --detailed --check-permissions

  # JSON format output
  python -m src.cli catalog health --format json
```

### catalog list

List catalogs, schemas, and tables.

```bash
python -m src.cli catalog list [OPTIONS]

Options:
  --type TYPE           What to list: catalogs, schemas, tables
  --catalog TEXT        Catalog to list schemas/tables from
  --schema TEXT         Schema to list tables from
  --pattern TEXT        Filter by name pattern

Examples:
  # List all catalogs
  python -m src.cli catalog list --type catalogs

  # List schemas in healthcare catalog
  python -m src.cli catalog list --type schemas --catalog healthcare_data

  # List tables with pattern
  python -m src.cli catalog list --type tables --pattern "*claims*"
```

---

## Pipeline Management Commands

Create, manage, and monitor data processing pipelines.

### pipeline create

Create a new healthcare data pipeline.

```bash
python -m src.cli pipeline create PIPELINE_NAME [OPTIONS]

Arguments:
  PIPELINE_NAME         Name of the pipeline

Options:
  --source-path PATH    Source data path (required)
  --target-table NAME   Target Unity Catalog table (required)
  --schedule TEXT       Cron schedule expression
  --description TEXT    Pipeline description
  --data-type TYPE      Healthcare data type: medicaid_claims, medicare_claims, provider_data
  --state TEXT          State code for state-specific processing
  --cluster-profile     Cluster profile: small, medium, large, custom

Examples:
  # Basic Medicare claims pipeline
  python -m src.cli pipeline create medicare_claims \
    --source-path s3://healthcare-raw/medicare/claims/ \
    --target-table healthcare_data.silver.medicare_claims \
    --schedule "0 */4 * * *"

  # California Medicaid pipeline with state-specific validation
  python -m src.cli pipeline create ca_medicaid_claims \
    --source-path s3://data/ca/medicaid/ \
    --target-table healthcare_data.silver.ca_medicaid_claims \
    --data-type medicaid_claims \
    --state CA \
    --cluster-profile medium

  # Provider data pipeline with description
  python -m src.cli pipeline create provider_updates \
    --source-path s3://npi-registry/updates/ \
    --target-table healthcare_data.silver.providers \
    --description "Daily NPI registry updates" \
    --schedule "0 6 * * *"
```

### pipeline list

List all configured pipelines.

```bash
python -m src.cli pipeline list [OPTIONS]

Options:
  --status STATUS       Filter by status: running, stopped, failed, all
  --format FORMAT       Output format: table, json, yaml
  --sort-by FIELD       Sort by: name, status, last_run, quality_score
  --limit INTEGER       Limit number of results

Examples:
  # List all pipelines
  python -m src.cli pipeline list

  # List only running pipelines
  python -m src.cli pipeline list --status running

  # List with JSON output, sorted by quality score
  python -m src.cli pipeline list --format json --sort-by quality_score
```

### pipeline run

Trigger a pipeline run manually.

```bash
python -m src.cli pipeline run PIPELINE_NAME [OPTIONS]

Arguments:
  PIPELINE_NAME         Name of the pipeline to run

Options:
  --wait               Wait for run completion
  --timeout INTEGER    Timeout in minutes (default: 60)
  --parameters JSON    Runtime parameters as JSON
  --force              Force run even if another run is active

Examples:
  # Simple pipeline run
  python -m src.cli pipeline run medicare_claims

  # Run with wait and timeout
  python -m src.cli pipeline run medicare_claims --wait --timeout 30

  # Run with custom parameters
  python -m src.cli pipeline run medicare_claims \
    --parameters '{"start_date": "2024-01-01", "end_date": "2024-01-31"}'

  # Force run (stop existing run first)
  python -m src.cli pipeline run medicare_claims --force
```

### pipeline stop

Stop a running pipeline.

```bash
python -m src.cli pipeline stop PIPELINE_NAME [OPTIONS]

Options:
  --force              Force stop without waiting for graceful shutdown
  --wait               Wait for stop completion

Examples:
  # Graceful stop
  python -m src.cli pipeline stop medicare_claims

  # Force stop with wait
  python -m src.cli pipeline stop medicare_claims --force --wait
```

### pipeline metrics

Get pipeline health and performance metrics.

```bash
python -m src.cli pipeline metrics PIPELINE_NAME [OPTIONS]

Options:
  --days INTEGER       Number of days of history (default: 7)
  --granularity TEXT   Data granularity: hour, day, week
  --metrics LIST       Specific metrics: runtime, quality, cost, throughput

Examples:
  # Basic metrics for last 7 days
  python -m src.cli pipeline metrics medicare_claims

  # Detailed metrics for last 30 days
  python -m src.cli pipeline metrics medicare_claims --days 30

  # Only quality and cost metrics
  python -m src.cli pipeline metrics medicare_claims \
    --metrics quality,cost --granularity day
```

### pipeline logs

View pipeline execution logs.

```bash
python -m src.cli pipeline logs PIPELINE_NAME [OPTIONS]

Options:
  --lines INTEGER      Number of recent log lines (default: 100)
  --follow            Follow log output (like tail -f)
  --run-id TEXT       Specific run ID to view
  --level LEVEL       Log level: DEBUG, INFO, WARNING, ERROR
  --since TIMESTAMP   Show logs since timestamp

Examples:
  # Recent logs
  python -m src.cli pipeline logs medicare_claims

  # Follow live logs
  python -m src.cli pipeline logs medicare_claims --follow

  # Logs for specific run
  python -m src.cli pipeline logs medicare_claims --run-id run_12345

  # Only error logs from last 24 hours
  python -m src.cli pipeline logs medicare_claims \
    --level ERROR --since "24 hours ago"
```

### pipeline delete

Delete a pipeline and its configuration.

```bash
python -m src.cli pipeline delete PIPELINE_NAME [OPTIONS]

Options:
  --force              Force deletion without confirmation
  --keep-data          Keep processed data tables
  --backup             Create backup before deletion

Examples:
  # Delete with confirmation
  python -m src.cli pipeline delete old_pipeline

  # Force delete, keep data
  python -m src.cli pipeline delete old_pipeline --force --keep-data

  # Delete with backup
  python -m src.cli pipeline delete old_pipeline --backup
```

---

## Data Quality Commands

Monitor and manage data quality across healthcare datasets.

### quality check

Run comprehensive data quality assessment.

```bash
python -m src.cli quality check TABLE_NAME [OPTIONS]

Arguments:
  TABLE_NAME           Unity Catalog table name

Options:
  --assessment-type    Type: quick, standard, comprehensive
  --focus-areas LIST   Areas: completeness, validity, consistency, accuracy
  --custom-rules FILE  Custom validation rules file
  --output-file PATH   Save results to file
  --threshold FLOAT    Quality score threshold for success

Examples:
  # Quick quality check
  python -m src.cli quality check healthcare_data.silver.medicaid_claims

  # Comprehensive assessment with focus areas
  python -m src.cli quality check healthcare_data.silver.medicare_claims \
    --assessment-type comprehensive \
    --focus-areas completeness,validity,healthcare_codes

  # Quality check with custom rules
  python -m src.cli quality check healthcare_data.silver.providers \
    --custom-rules config/custom_provider_rules.yaml \
    --output-file quality_report.json
```

### quality trends

View data quality trends over time.

```bash
python -m src.cli quality trends [OPTIONS]

Options:
  --table TEXT         Table name to analyze
  --days INTEGER       Number of days (default: 30)
  --dimensions LIST    Quality dimensions to show
  --format FORMAT      Output format: table, chart, json

Examples:
  # Quality trends for claims table
  python -m src.cli quality trends \
    --table healthcare_data.silver.medicaid_claims \
    --days 14

  # Specific dimensions with chart output
  python -m src.cli quality trends \
    --table healthcare_data.silver.providers \
    --dimensions completeness,validity \
    --format chart
```

### quality alerts

Manage data quality alerts and thresholds.

```bash
python -m src.cli quality alerts TABLE_NAME [OPTIONS]

Options:
  --min-quality FLOAT     Minimum quality score threshold
  --max-anomaly FLOAT     Maximum anomaly percentage
  --max-stale FLOAT       Maximum stale data percentage
  --list                  List existing alerts
  --create                Create new alert rules
  --delete ALERT_ID       Delete specific alert

Examples:
  # Create quality alerts
  python -m src.cli quality alerts healthcare_data.silver.claims \
    --min-quality 0.85 \
    --max-anomaly 5.0 \
    --max-stale 10.0

  # List existing alerts
  python -m src.cli quality alerts --list

  # Create alerts with custom thresholds
  python -m src.cli quality alerts healthcare_data.silver.providers \
    --min-quality 0.95 \
    --max-anomaly 2.0
```

### quality report

Generate comprehensive quality reports.

```bash
python -m src.cli quality report [OPTIONS]

Options:
  --tables LIST        Specific tables to include
  --output-dir PATH    Output directory for reports
  --format FORMAT      Report format: html, pdf, json
  --include-charts     Include visualizations
  --email-recipients   Email report to recipients

Examples:
  # Generate report for all tables
  python -m src.cli quality report --format html --include-charts

  # Report for specific tables with PDF output
  python -m src.cli quality report \
    --tables healthcare_data.silver.claims,healthcare_data.silver.providers \
    --format pdf \
    --output-dir reports/

  # Generate and email report
  python -m src.cli quality report \
    --format html \
    --email-recipients data-team@company.com
```

---

## Monitoring Commands

Monitor platform health, performance, and costs.

### monitor create

Create a lakehouse monitor for automated monitoring.

```bash
python -m src.cli monitor create TABLE_NAME [OPTIONS]

Arguments:
  TABLE_NAME           Table to monitor

Options:
  --monitor-name TEXT  Custom monitor name
  --baseline-table     Baseline table for comparison
  --schedule TEXT      Monitoring schedule (cron format)
  --metrics LIST       Metrics to monitor

Examples:
  # Create basic monitor
  python -m src.cli monitor create healthcare_data.silver.claims

  # Monitor with baseline and custom schedule
  python -m src.cli monitor create healthcare_data.silver.claims \
    --baseline-table healthcare_data.gold.claims_baseline \
    --schedule "0 */2 * * *" \
    --monitor-name claims_quality_monitor
```

### monitor refresh

Refresh monitoring data and generate new insights.

```bash
python -m src.cli monitor refresh MONITOR_NAME [OPTIONS]

Options:
  --wait               Wait for refresh completion
  --full-refresh       Force full data refresh

Examples:
  # Refresh specific monitor
  python -m src.cli monitor refresh claims_quality_monitor

  # Full refresh with wait
  python -m src.cli monitor refresh claims_quality_monitor \
    --full-refresh --wait
```

### monitor list

List all configured monitors and their status.

```bash
python -m src.cli monitor list [OPTIONS]

Options:
  --status STATUS      Filter by status
  --table TEXT         Filter by table name

Examples:
  # List all monitors
  python -m src.cli monitor list

  # List only active monitors
  python -m src.cli monitor list --status active
```

---

## Cluster Management Commands

Manage and optimize Databricks clusters for cost and performance.

### cluster create

Create an adaptive cluster with optimization.

```bash
python -m src.cli cluster create CLUSTER_NAME WORKLOAD_TYPE [OPTIONS]

Arguments:
  CLUSTER_NAME         Name for the new cluster
  WORKLOAD_TYPE        Workload type: ingestion, quality, analytics

Options:
  --cost-level LEVEL   Cost optimization level: aggressive, balanced, performance
  --node-type TYPE     Databricks node type
  --min-workers INT    Minimum number of workers
  --max-workers INT    Maximum number of workers
  --auto-terminate     Auto-termination timeout in minutes

Examples:
  # Create ingestion cluster with balanced cost optimization
  python -m src.cli cluster create medicaid-ingestion-cluster ingestion \
    --cost-level balanced

  # Create high-performance analytics cluster
  python -m src.cli cluster create analytics-cluster analytics \
    --cost-level performance \
    --node-type i3.2xlarge \
    --min-workers 4 \
    --max-workers 20

  # Create cost-optimized cluster with auto-termination
  python -m src.cli cluster create temp-cluster ingestion \
    --cost-level aggressive \
    --auto-terminate 30
```

### cluster monitor

Monitor cluster utilization and get optimization recommendations.

```bash
python -m src.cli cluster monitor CLUSTER_NAME [OPTIONS]

Arguments:
  CLUSTER_NAME         Cluster to monitor

Options:
  --days INTEGER       Days of historical data
  --recommendations    Show optimization recommendations
  --cost-analysis      Include detailed cost analysis

Examples:
  # Basic cluster monitoring
  python -m src.cli cluster monitor medicaid-processing-cluster

  # Monitor with recommendations and cost analysis
  python -m src.cli cluster monitor analytics-cluster \
    --days 14 \
    --recommendations \
    --cost-analysis
```

### cluster optimize

Apply optimization recommendations to clusters.

```bash
python -m src.cli cluster optimize CLUSTER_NAME [OPTIONS]

Options:
  --apply-recommendations  Apply all recommended optimizations
  --schedule-time TIME     Schedule optimization for specific time
  --dry-run               Show what would be optimized

Examples:
  # Show optimization recommendations
  python -m src.cli cluster optimize analytics-cluster --dry-run

  # Apply optimizations immediately
  python -m src.cli cluster optimize analytics-cluster \
    --apply-recommendations

  # Schedule optimization for low-usage hours
  python -m src.cli cluster optimize analytics-cluster \
    --apply-recommendations \
    --schedule-time "02:00"
```

### cluster list

List all clusters with their status and utilization.

```bash
python -m src.cli cluster list [OPTIONS]

Options:
  --status STATUS      Filter by status: running, terminated, pending
  --sort-by FIELD      Sort by: name, status, cost, utilization
  --show-costs         Include cost information

Examples:
  # List all clusters
  python -m src.cli cluster list

  # List running clusters with costs
  python -m src.cli cluster list --status running --show-costs

  # List sorted by utilization
  python -m src.cli cluster list --sort-by utilization
```

---

## Healthcare-Specific Commands

Commands for healthcare data validation and compliance.

### healthcare validate-codes

Validate healthcare codes (NPI, ICD-10, CPT, HCPCS).

```bash
python -m src.cli healthcare validate-codes [OPTIONS]

Options:
  --file PATH          File containing codes to validate
  --code-type TYPE     Code type: npi, icd10, cpt, hcpcs
  --codes LIST         Comma-separated list of codes
  --output-file PATH   Save validation results

Examples:
  # Validate NPI codes from file
  python -m src.cli healthcare validate-codes \
    --file npi_codes.txt \
    --code-type npi

  # Validate specific CPT codes
  python -m src.cli healthcare validate-codes \
    --codes "99213,99214,80053" \
    --code-type cpt

  # Validate mixed codes with output file
  python -m src.cli healthcare validate-codes \
    --file healthcare_codes.csv \
    --output-file validation_results.json
```

### healthcare check-eligibility

Check member eligibility for specific services.

```bash
python -m src.cli healthcare check-eligibility [OPTIONS]

Options:
  --member-id TEXT     Member ID to check
  --service-date DATE  Date of service (YYYY-MM-DD)
  --provider-npi TEXT  Provider NPI
  --service-type TYPE  Service type: medical, pharmacy, dental

Examples:
  # Check member eligibility
  python -m src.cli healthcare check-eligibility \
    --member-id "M123456789" \
    --service-date "2024-01-15" \
    --provider-npi "1234567893"

  # Check pharmacy eligibility
  python -m src.cli healthcare check-eligibility \
    --member-id "CA123456789" \
    --service-date "2024-01-15" \
    --service-type pharmacy
```

### healthcare compliance-check

Run HIPAA and regulatory compliance checks.

```bash
python -m src.cli healthcare compliance-check TABLE_NAME [OPTIONS]

Arguments:
  TABLE_NAME           Table to check for compliance

Options:
  --compliance-type    Type: hipaa, state_medicaid, medicare
  --detailed-report    Generate detailed compliance report
  --fix-issues         Automatically fix common issues

Examples:
  # HIPAA compliance check
  python -m src.cli healthcare compliance-check \
    healthcare_data.silver.claims \
    --compliance-type hipaa \
    --detailed-report

  # State Medicaid compliance with auto-fix
  python -m src.cli healthcare compliance-check \
    healthcare_data.silver.ca_medicaid_claims \
    --compliance-type state_medicaid \
    --fix-issues
```

---

## Utility Commands

General platform utilities and administration.

### status

Show overall platform status and health.

```bash
python -m src.cli status [OPTIONS]

Options:
  --detailed           Show detailed component status
  --format FORMAT      Output format: table, json, yaml

Examples:
  # Basic status
  python -m src.cli status

  # Detailed status in JSON
  python -m src.cli status --detailed --format json
```

**Sample Output:**
```
🏥 Healthcare Data Platform Status
====================================
✅ System Health: 98.5%
✅ Data Quality: 96.2%
✅ Pipelines Running: 12/12
✅ Cost Optimization: 12.3% savings
✅ Self-Healing Actions: 47 today

🔍 Recent Activity:
   ✅ medicaid_claims pipeline completed (5 min ago)
   🔧 Auto-scaled cluster down for cost savings (1 hour ago)
   ⚠️  Schema drift detected and resolved (3 hours ago)
   ✅ Quality alert auto-resolved (6 hours ago)
```

### dashboard

Start the Streamlit monitoring dashboard.

```bash
python -m src.cli dashboard [OPTIONS]

Options:
  -p, --port INTEGER   Dashboard port (default: 8501)
  -h, --host TEXT      Dashboard host (default: localhost)
  --dev-mode          Enable development mode with auto-reload

Examples:
  # Start dashboard on default port
  python -m src.cli dashboard

  # Start on custom port and host
  python -m src.cli dashboard --port 8080 --host 0.0.0.0

  # Development mode with auto-reload
  python -m src.cli dashboard --dev-mode
```

### config

Configuration management utilities.

```bash
python -m src.cli config SUBCOMMAND [OPTIONS]

Subcommands:
  validate    Validate configuration files
  show        Show current configuration
  encrypt     Encrypt sensitive configuration values
  template    Generate configuration templates

Examples:
  # Validate all configuration
  python -m src.cli config validate

  # Show current configuration (sensitive values masked)
  python -m src.cli config show

  # Generate configuration template for new deployment
  python -m src.cli config template --environment production
```

### backup

Backup platform configuration and metadata.

```bash
python -m src.cli backup [OPTIONS]

Options:
  --output-dir PATH    Backup output directory
  --include-data       Include processed data in backup
  --compress           Compress backup files

Examples:
  # Basic configuration backup
  python -m src.cli backup

  # Full backup with data compression
  python -m src.cli backup \
    --include-data \
    --compress \
    --output-dir /backups/healthcare-platform/
```

---

## Advanced Usage

### Command Chaining

Chain multiple commands for complex workflows:

```bash
# Create pipeline, run it, and monitor quality
python -m src.cli pipeline create test_pipeline \
  --source-path s3://test-data/ \
  --target-table test.bronze.data && \
python -m src.cli pipeline run test_pipeline --wait && \
python -m src.cli quality check test.bronze.data
```

### Environment-Specific Commands

Use different configurations for different environments:

```bash
# Development environment
python -m src.cli --config config/dev.yaml pipeline list

# Production environment
python -m src.cli --config config/prod.yaml status --detailed
```

### JSON Output and Scripting

Use JSON output for scripting and automation:

```bash
# Get pipeline status as JSON for processing
python -m src.cli pipeline list --format json | jq '.pipelines[] | select(.status=="failed")'

# Monitor quality scores
python -m src.cli quality trends --format json --days 7 | \
  jq '.overall_quality_score' | \
  awk '$1 < 85 { print "Quality alert: " $1 }'
```

### Batch Operations

Perform batch operations on multiple resources:

```bash
# Stop all failed pipelines
for pipeline in $(python -m src.cli pipeline list --status failed --format json | jq -r '.pipelines[].name'); do
  python -m src.cli pipeline stop "$pipeline" --force
done

# Run quality checks on all silver tables
for table in $(python -m src.cli catalog list --type tables --pattern "*.silver.*" --format json | jq -r '.tables[].name'); do
  python -m src.cli quality check "$table" --assessment-type quick
done
```

---

## Troubleshooting

### Common CLI Issues

**Command not found:**
```bash
# Ensure you're in the correct environment
source venv/bin/activate
python -m src.cli --help
```

**Configuration errors:**
```bash
# Validate configuration
python -m src.cli config validate

# Check specific configuration sections
python -m src.cli config show --section databricks
```

**Connection issues:**
```bash
# Test Databricks connectivity
python -c "
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
print(w.current_user.me())
"
```

### Debug Mode

Enable verbose logging for troubleshooting:

```bash
# Verbose output
python -m src.cli --verbose pipeline run test_pipeline

# Debug with dry-run
python -m src.cli --dry-run --verbose pipeline create test \
  --source-path s3://test/ \
  --target-table test.table
```

### Getting Help

Each command and subcommand has built-in help:

```bash
# General help
python -m src.cli --help

# Command-specific help
python -m src.cli pipeline --help
python -m src.cli pipeline create --help

# Show all available options
python -m src.cli pipeline create --help | grep -E "^\s*--"
```

---

For more examples and use cases, see the [CLI Tutorial](tutorial.md) and [Best Practices](best-practices.md) guides.