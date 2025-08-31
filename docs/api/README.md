# API Documentation

Complete REST API reference for the Agentic Data Engineering Platform.

## Overview

The platform provides RESTful APIs for programmatic access to all healthcare data processing capabilities. The API is built with FastAPI and provides automatic OpenAPI documentation, authentication, and comprehensive error handling.

### Base URL
```
Production:  https://api.healthcare-platform.com/v1
Staging:     https://staging-api.healthcare-platform.com/v1
Development: http://localhost:8000/v1
```

### Authentication

All API endpoints require authentication using API keys:

```bash
# Include in headers
Authorization: Bearer your-api-key-here
```

### Content Type
```
Content-Type: application/json
Accept: application/json
```

---

## Pipeline Management API

### List Pipelines

Get all configured pipelines for your organization.

**Endpoint:** `GET /pipelines`

**Response:**
```json
{
  "pipelines": [
    {
      "id": "medicaid_claims",
      "name": "Medicaid Claims Pipeline",
      "status": "running",
      "source_path": "s3://healthcare-raw/medicaid/claims/",
      "target_table": "healthcare_data.silver.medicaid_claims",
      "schedule": "0 */6 * * *",
      "last_run": "2024-01-15T10:30:00Z",
      "next_run": "2024-01-15T16:30:00Z",
      "health_score": 94.2,
      "quality_score": 96.8
    }
  ],
  "total": 1,
  "page": 1,
  "per_page": 10
}
```

### Create Pipeline

Create a new healthcare data processing pipeline.

**Endpoint:** `POST /pipelines`

**Request Body:**
```json
{
  "name": "medicare_part_d",
  "description": "Medicare Part D prescription claims processing",
  "source_path": "s3://healthcare-raw/medicare/part-d/",
  "target_table": "healthcare_data.silver.medicare_part_d",
  "schedule": "0 */4 * * *",
  "cluster_config": {
    "node_type": "i3.xlarge",
    "min_workers": 2,
    "max_workers": 8,
    "auto_termination_minutes": 30
  },
  "quality_thresholds": {
    "min_quality_score": 0.85,
    "max_anomaly_percentage": 5.0,
    "max_stale_percentage": 10.0
  },
  "healthcare_config": {
    "data_type": "medicare_part_d",
    "validation_rules": [
      "npi_validation",
      "ndc_validation",
      "member_eligibility"
    ],
    "phi_fields": [
      "member_id",
      "prescriber_npi",
      "pharmacy_npi"
    ]
  }
}
```

**Response:**
```json
{
  "id": "medicare_part_d_001",
  "status": "created",
  "message": "Pipeline created successfully",
  "databricks_job_id": 12345,
  "estimated_setup_time": "5-10 minutes",
  "next_steps": [
    "Pipeline validation in progress",
    "Initial schema detection starting",
    "First run scheduled for next cron interval"
  ]
}
```

### Get Pipeline Details

Get detailed information about a specific pipeline.

**Endpoint:** `GET /pipelines/{pipeline_id}`

**Response:**
```json
{
  "id": "medicaid_claims",
  "name": "Medicaid Claims Pipeline",
  "status": "running",
  "created_at": "2024-01-01T00:00:00Z",
  "updated_at": "2024-01-15T10:30:00Z",
  "configuration": {
    "source_path": "s3://healthcare-raw/medicaid/claims/",
    "target_table": "healthcare_data.silver.medicaid_claims",
    "schedule": "0 */6 * * *",
    "cluster_config": {
      "node_type": "i3.xlarge",
      "min_workers": 2,
      "max_workers": 10,
      "current_workers": 4
    }
  },
  "health_metrics": {
    "health_score": 94.2,
    "quality_score": 96.8,
    "uptime_percentage": 99.1,
    "avg_processing_time_minutes": 12.5,
    "last_24h_runs": 4,
    "success_rate": 100.0
  },
  "recent_runs": [
    {
      "run_id": "run_001",
      "start_time": "2024-01-15T10:00:00Z",
      "end_time": "2024-01-15T10:12:30Z",
      "status": "success",
      "records_processed": 125000,
      "quality_score": 96.8
    }
  ]
}
```

### Update Pipeline

Update an existing pipeline configuration.

**Endpoint:** `PUT /pipelines/{pipeline_id}`

**Request Body:**
```json
{
  "schedule": "0 */3 * * *",  # More frequent processing
  "cluster_config": {
    "max_workers": 15  # Scale up for higher volume
  },
  "quality_thresholds": {
    "min_quality_score": 0.90  # Stricter quality requirements
  }
}
```

### Delete Pipeline

**Endpoint:** `DELETE /pipelines/{pipeline_id}`

**Query Parameters:**
- `force=true` - Force deletion even if pipeline is running
- `backup_data=true` - Backup processed data before deletion

---

## Data Quality API

### Quality Assessment

Run comprehensive data quality assessment on a table.

**Endpoint:** `POST /quality/assess`

**Request Body:**
```json
{
  "table_name": "healthcare_data.silver.medicaid_claims",
  "assessment_type": "comprehensive",  # comprehensive, quick, custom
  "custom_rules": [
    {
      "name": "state_specific_member_id",
      "description": "Validate state-specific member ID format",
      "condition": "member_id RLIKE '^CA[0-9]{9}$'",
      "severity": "warning"
    }
  ],
  "focus_areas": [
    "completeness",
    "validity", 
    "healthcare_codes"
  ]
}
```

**Response:**
```json
{
  "assessment_id": "qa_001",
  "status": "completed",
  "table_name": "healthcare_data.silver.medicaid_claims",
  "assessment_timestamp": "2024-01-15T14:30:00Z",
  "overall_quality_score": 94.2,
  "record_count": 250000,
  "dimension_scores": {
    "completeness": 96.5,
    "validity": 93.8,
    "consistency": 95.2,
    "accuracy": 91.8,
    "timeliness": 98.1,
    "uniqueness": 99.7
  },
  "healthcare_validation": {
    "npi_validation_rate": 98.9,
    "icd10_validation_rate": 95.2,
    "cpt_validation_rate": 97.1,
    "member_id_validation_rate": 94.8
  },
  "issues_found": [
    {
      "severity": "warning",
      "category": "validity",
      "field": "provider_npi",
      "issue": "Invalid NPI format",
      "affected_records": 2750,
      "percentage": 1.1,
      "recommendation": "Verify NPI registry lookup"
    }
  ],
  "recommendations": [
    "Implement real-time NPI validation",
    "Add provider network verification",
    "Enable automatic data correction for common formats"
  ]
}
```

### Quality Monitoring

Get quality monitoring data and trends.

**Endpoint:** `GET /quality/monitoring/{table_name}`

**Query Parameters:**
- `days=7` - Number of days of historical data
- `metrics=completeness,validity` - Specific metrics to retrieve
- `granularity=hour` - Data granularity (hour, day, week)

**Response:**
```json
{
  "table_name": "healthcare_data.silver.medicaid_claims",
  "monitoring_period": {
    "start_date": "2024-01-08T00:00:00Z",
    "end_date": "2024-01-15T00:00:00Z",
    "granularity": "hour"
  },
  "current_metrics": {
    "overall_quality_score": 94.2,
    "trend": "stable",
    "last_updated": "2024-01-15T14:00:00Z"
  },
  "historical_data": [
    {
      "timestamp": "2024-01-15T13:00:00Z",
      "overall_score": 94.2,
      "completeness": 96.5,
      "validity": 93.8,
      "records_processed": 10500
    }
  ],
  "anomalies_detected": [
    {
      "timestamp": "2024-01-14T08:00:00Z",
      "anomaly_type": "quality_drop",
      "severity": "medium",
      "description": "Validity score dropped to 89.2%",
      "auto_remediation_applied": true
    }
  ],
  "active_alerts": []
}
```

### Quality Alerts

Manage data quality alerts and thresholds.

**Endpoint:** `POST /quality/alerts`

**Request Body:**
```json
{
  "table_name": "healthcare_data.silver.medicaid_claims",
  "alert_rules": [
    {
      "name": "critical_quality_drop",
      "condition": "overall_quality_score < 85",
      "severity": "critical",
      "notification_channels": ["email", "slack"],
      "cooldown_minutes": 60
    },
    {
      "name": "high_npi_validation_failure",
      "condition": "npi_validation_rate < 95",
      "severity": "warning",
      "notification_channels": ["slack"],
      "cooldown_minutes": 30
    }
  ],
  "notification_config": {
    "email_recipients": ["data-team@company.com"],
    "slack_channel": "#data-quality",
    "escalation_enabled": true,
    "escalation_delay_minutes": 15
  }
}
```

---

## Cluster Management API

### List Clusters

**Endpoint:** `GET /clusters`

**Response:**
```json
{
  "clusters": [
    {
      "cluster_id": "cluster_001",
      "cluster_name": "medicaid-processing-cluster",
      "state": "RUNNING",
      "node_type": "i3.xlarge",
      "current_workers": 4,
      "target_workers": 4,
      "auto_termination_minutes": 30,
      "utilization": {
        "cpu_percent": 72.5,
        "memory_percent": 68.9,
        "efficiency_score": 85.2
      },
      "cost": {
        "hourly_cost_usd": 4.32,
        "daily_estimate_usd": 103.68,
        "cost_optimization_score": 78.5
      }
    }
  ]
}
```

### Cluster Optimization

Get cluster optimization recommendations.

**Endpoint:** `GET /clusters/{cluster_id}/optimize`

**Response:**
```json
{
  "cluster_id": "cluster_001",
  "current_configuration": {
    "node_type": "i3.xlarge",
    "workers": 4,
    "hourly_cost": 4.32
  },
  "recommendations": [
    {
      "type": "downsize",
      "priority": "high",
      "description": "Cluster is underutilized (avg 45% CPU over 24h)",
      "suggested_configuration": {
        "node_type": "i3.large",
        "workers": 3
      },
      "estimated_savings_monthly": 432.50,
      "performance_impact": "minimal",
      "confidence": 0.89
    },
    {
      "type": "schedule_optimization", 
      "priority": "medium",
      "description": "Enable auto-termination during low-usage hours",
      "configuration": {
        "auto_termination_minutes": 15,
        "idle_detection": true
      },
      "estimated_savings_monthly": 156.75
    }
  ],
  "auto_apply_available": true
}
```

### Apply Cluster Changes

**Endpoint:** `POST /clusters/{cluster_id}/apply-recommendations`

**Request Body:**
```json
{
  "recommendations": ["downsize", "schedule_optimization"],
  "apply_immediately": false,
  "schedule_time": "2024-01-16T02:00:00Z"
}
```

---

## Monitoring & Analytics API

### Platform Metrics

Get overall platform health and performance metrics.

**Endpoint:** `GET /metrics/platform`

**Query Parameters:**
- `timerange=24h` - Time range (1h, 24h, 7d, 30d)
- `aggregation=hourly` - Data aggregation level

**Response:**
```json
{
  "platform_health": {
    "overall_score": 94.8,
    "uptime_percentage": 99.2,
    "active_pipelines": 12,
    "total_pipelines": 12
  },
  "processing_metrics": {
    "records_processed_24h": 2450000,
    "avg_processing_latency_minutes": 4.2,
    "success_rate": 98.7
  },
  "quality_metrics": {
    "avg_quality_score": 94.2,
    "critical_issues": 0,
    "warning_issues": 3,
    "auto_resolved_issues": 15
  },
  "cost_metrics": {
    "daily_cost_usd": 456.78,
    "monthly_projection_usd": 13703.40,
    "cost_optimization_savings": 2156.80,
    "efficiency_score": 87.3
  },
  "self_healing_metrics": {
    "auto_fixes_24h": 8,
    "manual_interventions_24h": 1,
    "automation_rate": 88.9
  }
}
```

### Cost Analysis

**Endpoint:** `GET /analytics/costs`

**Response:**
```json
{
  "cost_summary": {
    "total_monthly_cost": 13703.40,
    "cost_by_component": {
      "compute": 8456.12,
      "storage": 2341.56,
      "data_transfer": 1234.78,
      "monitoring": 891.23,
      "other": 779.71
    },
    "optimization_opportunities": {
      "potential_monthly_savings": 2156.80,
      "optimization_score": 78.5
    }
  },
  "cost_breakdown_by_pipeline": [
    {
      "pipeline_id": "medicaid_claims",
      "monthly_cost": 4567.89,
      "cost_per_record": 0.0043,
      "efficiency_score": 82.1
    }
  ],
  "recommendations": [
    {
      "type": "spot_instances",
      "description": "Use spot instances for non-critical pipelines",
      "estimated_savings": 1234.56
    },
    {
      "type": "right_sizing",
      "description": "Optimize cluster sizes based on usage patterns",
      "estimated_savings": 789.12
    }
  ]
}
```

---

## Healthcare-Specific APIs

### Medical Code Validation

Validate healthcare codes (NPI, ICD-10, CPT, etc.).

**Endpoint:** `POST /healthcare/validate-codes`

**Request Body:**
```json
{
  "codes": [
    {
      "type": "npi",
      "value": "1234567893",
      "additional_validation": true
    },
    {
      "type": "icd10",
      "value": "Z00.00"
    },
    {
      "type": "cpt",
      "value": "99213"
    }
  ],
  "validation_level": "comprehensive"  # basic, standard, comprehensive
}
```

**Response:**
```json
{
  "validation_results": [
    {
      "code_type": "npi",
      "code_value": "1234567893",
      "is_valid": true,
      "validation_details": {
        "format_check": "passed",
        "luhn_check": "passed",
        "registry_check": "passed",
        "provider_details": {
          "name": "Dr. John Smith",
          "specialty": "Family Medicine",
          "status": "active"
        }
      }
    },
    {
      "code_type": "icd10",
      "code_value": "Z00.00",
      "is_valid": true,
      "validation_details": {
        "format_check": "passed",
        "code_description": "Encounter for general adult medical examination without abnormal findings",
        "billable": true,
        "category": "Factors influencing health status"
      }
    }
  ],
  "summary": {
    "total_codes": 3,
    "valid_codes": 3,
    "invalid_codes": 0,
    "validation_rate": 100.0
  }
}
```

### Member Eligibility Check

**Endpoint:** `POST /healthcare/eligibility-check`

**Request Body:**
```json
{
  "member_id": "M123456789",
  "service_date": "2024-01-15",
  "provider_npi": "1234567893",
  "service_type": "medical"  # medical, pharmacy, dental
}
```

**Response:**
```json
{
  "member_id": "M123456789",
  "eligibility_status": "active",
  "effective_dates": {
    "start_date": "2024-01-01",
    "end_date": "2024-12-31"
  },
  "coverage_details": {
    "medical": true,
    "pharmacy": true,
    "dental": false,
    "vision": true
  },
  "provider_network_status": "in_network",
  "prior_authorization_required": false,
  "copay_amount": 25.00,
  "deductible_info": {
    "annual_deductible": 500.00,
    "deductible_met": 125.00,
    "remaining": 375.00
  }
}
```

---

## Error Handling

### Standard Error Response

All API endpoints return standardized error responses:

```json
{
  "error": {
    "code": "INVALID_PIPELINE_CONFIG",
    "message": "Pipeline configuration validation failed",
    "details": [
      {
        "field": "cluster_config.node_type",
        "issue": "Invalid node type: i3.invalid",
        "suggestion": "Use supported node types: i3.large, i3.xlarge, i3.2xlarge"
      }
    ],
    "documentation_url": "https://docs.healthcare-platform.com/api/errors#INVALID_PIPELINE_CONFIG",
    "request_id": "req_12345"
  }
}
```

### HTTP Status Codes

| Code | Meaning | Usage |
|------|---------|-------|
| `200` | OK | Successful request |
| `201` | Created | Resource created successfully |
| `400` | Bad Request | Invalid request parameters |
| `401` | Unauthorized | Invalid or missing API key |
| `403` | Forbidden | Insufficient permissions |
| `404` | Not Found | Resource not found |
| `409` | Conflict | Resource already exists |
| `422` | Unprocessable Entity | Validation failed |
| `429` | Too Many Requests | Rate limit exceeded |
| `500` | Internal Server Error | Server error |
| `503` | Service Unavailable | Service temporarily unavailable |

### Common Error Codes

| Error Code | Description | Resolution |
|------------|-------------|------------|
| `INVALID_API_KEY` | API key is invalid or expired | Refresh API key |
| `PIPELINE_NOT_FOUND` | Pipeline does not exist | Check pipeline ID |
| `DATABRICKS_CONNECTION_FAILED` | Cannot connect to Databricks | Verify credentials |
| `QUALITY_ASSESSMENT_FAILED` | Quality assessment could not complete | Check table permissions |
| `CLUSTER_START_FAILED` | Cluster failed to start | Check cluster configuration |
| `HEALTHCARE_CODE_INVALID` | Healthcare code validation failed | Verify code format |

---

## Rate Limits

### API Rate Limits

| Endpoint Category | Rate Limit | Burst Limit |
|-------------------|------------|-------------|
| Pipeline Management | 100 req/min | 200 req/min |
| Quality Assessment | 50 req/min | 100 req/min |
| Monitoring | 200 req/min | 400 req/min |
| Healthcare Validation | 500 req/min | 1000 req/min |

### Rate Limit Headers

```http
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1642276800
X-RateLimit-Burst: 200
```

---

## SDKs and Examples

### Python SDK Example

```python
from healthcare_platform_sdk import HealthcarePlatformClient

# Initialize client
client = HealthcarePlatformClient(
    api_key="your-api-key",
    base_url="https://api.healthcare-platform.com/v1"
)

# Create pipeline
pipeline = client.pipelines.create(
    name="medicaid_claims_ca",
    source_path="s3://data/ca/medicaid/",
    target_table="healthcare_data.silver.ca_medicaid_claims",
    schedule="0 */6 * * *",
    healthcare_config={
        "data_type": "medicaid_claims",
        "state": "california",
        "validation_rules": ["npi", "icd10", "member_eligibility"]
    }
)

# Monitor quality
quality_report = client.quality.assess(
    table_name="healthcare_data.silver.ca_medicaid_claims",
    assessment_type="comprehensive"
)

print(f"Quality Score: {quality_report.overall_quality_score}")
```

### cURL Examples

**Create Pipeline:**
```bash
curl -X POST "https://api.healthcare-platform.com/v1/pipelines" \
  -H "Authorization: Bearer your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "medicare_part_d",
    "source_path": "s3://data/medicare/part-d/",
    "target_table": "healthcare_data.silver.medicare_part_d",
    "schedule": "0 */4 * * *"
  }'
```

**Get Quality Metrics:**
```bash
curl -X GET "https://api.healthcare-platform.com/v1/quality/monitoring/healthcare_data.silver.claims?days=7" \
  -H "Authorization: Bearer your-api-key"
```

---

## Interactive API Documentation

The platform provides interactive API documentation using Swagger/OpenAPI:

- **Production:** https://api.healthcare-platform.com/docs
- **Staging:** https://staging-api.healthcare-platform.com/docs
- **Development:** http://localhost:8000/docs

Features:
- Try out API endpoints directly
- View request/response schemas
- Download OpenAPI specification
- Generate client SDKs

---

## API Versioning

The API uses URL-based versioning:
- Current version: `/v1`
- Version support: Each version supported for 2 years
- Deprecation notice: 6 months advance notice
- Breaking changes: Only in new major versions

---

For more examples and detailed guides, see the [API Tutorial](tutorial.md) and [Integration Examples](examples.md).