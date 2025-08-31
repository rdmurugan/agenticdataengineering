# Healthcare Data Validation Rules

Comprehensive guide to healthcare-specific data validation rules and quality standards implemented in the Agentic Data Engineering Platform.

## Overview

The platform implements industry-standard healthcare data validation rules designed to ensure compliance with regulatory requirements (HIPAA, CMS, state Medicaid), improve data quality, and reduce downstream processing errors.

### Key Validation Categories

1. **Healthcare Code Validation** - NPI, ICD-10, CPT, HCPCS, NDC codes
2. **Business Rule Validation** - Eligibility, authorization, claim logic  
3. **Regulatory Compliance** - HIPAA, state Medicaid, Medicare requirements
4. **Data Integrity** - Completeness, consistency, referential integrity
5. **Anomaly Detection** - Statistical and domain-specific outliers

---

## Healthcare Code Validation

### National Provider Identifier (NPI) Validation

NPIs are validated using multiple criteria to ensure authenticity and accuracy.

#### Format Validation
```yaml
npi_validation:
  pattern: "^[0-9]{10}$"
  length: 10
  numeric_only: true
  leading_zeros_allowed: false
```

#### Luhn Algorithm Check
The platform implements the Luhn checksum algorithm for NPI validation:

```python
def validate_npi_luhn(npi_string):
    """
    Validates NPI using Luhn algorithm (mod-10 check digit)
    
    Examples:
    - Valid: 1234567893, 1679576722, 1245319599
    - Invalid: 1234567890, 1111111111, 0000000000
    """
    if not npi_string or len(npi_string) != 10:
        return False
    
    # Luhn algorithm implementation
    digits = [int(d) for d in npi_string]
    checksum = sum(digits[-1::-2])  # Sum odd positions
    
    for d in digits[-2::-2]:        # Sum even positions (doubled)
        doubled = d * 2
        checksum += doubled // 10 + doubled % 10
    
    return checksum % 10 == 0
```

#### Registry Validation (Optional)
```yaml
npi_registry_check:
  enabled: true
  api_endpoint: "https://npiregistry.cms.hhs.gov/api/"
  cache_ttl_hours: 24
  validation_fields:
    - provider_name
    - provider_type
    - specialty
    - status
```

### ICD-10 Diagnosis Code Validation

ICD-10 codes are validated for format compliance and clinical validity.

#### Format Rules
```yaml
icd10_validation:
  # Primary format: Letter + Digit + Alphanumeric (optional decimal extension)
  pattern: "^[A-TV-Z][0-9][A-Z0-9](\\.[A-Z0-9]{0,4})?$"
  
  # Excluded characters
  invalid_first_char: ["U"]  # Reserved for future use
  
  # Length constraints
  min_length: 3
  max_length: 7  # Including decimal point
  
  # Decimal rules
  decimal_required_categories: ["S", "T"]  # Injury, poisoning
  max_decimal_digits: 4
```

#### Valid ICD-10 Examples
```yaml
valid_icd10_codes:
  # General medical examinations
  - "Z00.00"  # Encounter for general adult medical examination without abnormal findings
  - "Z01.411" # Encounter for gynecological examination
  
  # Common conditions
  - "I10"     # Essential hypertension
  - "E11.9"   # Type 2 diabetes mellitus without complications
  - "J45.909" # Unspecified asthma, uncomplicated
  - "M79.3"   # Panniculitis, unspecified
  
  # Mental health
  - "F32.9"   # Major depressive disorder, single episode, unspecified
  - "F41.1"   # Generalized anxiety disorder
```

#### Invalid ICD-10 Examples
```yaml
invalid_icd10_codes:
  - "U07.1"    # COVID-19 (U codes require special authorization)
  - "123.45"   # Cannot start with number
  - "A"        # Too short
  - "Z00.00.1" # Too many decimal places
  - "I1O"      # Contains letter O instead of zero
```

### CPT Procedure Code Validation

Current Procedural Terminology (CPT) codes for medical procedures and services.

#### Format Rules
```yaml
cpt_validation:
  # Standard 5-digit format
  pattern: "^[0-9]{5}$"
  length: 5
  numeric_only: true
  
  # Category ranges
  category_1: "00100-99499"  # Category I (most common)
  category_2: "0001F-9999F"  # Category II (performance measures)
  category_3: "0001T-9999T"  # Category III (emerging technology)
```

#### Valid CPT Examples by Category
```yaml
common_cpt_codes:
  # Office visits
  - "99213"  # Office visit, established patient (level 3)
  - "99214"  # Office visit, established patient (level 4)
  - "99215"  # Office visit, established patient (level 5)
  
  # Preventive care
  - "99395"  # Periodic comprehensive preventive medicine (40-64 years)
  - "99396"  # Periodic comprehensive preventive medicine (65+ years)
  
  # Laboratory
  - "80053"  # Comprehensive metabolic panel
  - "85025"  # Blood count; complete (CBC), automated
  - "80061"  # Lipid panel
  
  # Radiology
  - "71020"  # Radiologic examination, chest, 2 views
  - "73630"  # Radiologic examination, foot; complete, minimum of 3 views
  
  # Surgery
  - "10060"  # Incision and drainage of abscess
  - "12001"  # Simple repair of superficial wounds
```

### HCPCS Level II Code Validation

Healthcare Common Procedure Coding System for supplies and services not covered by CPT.

#### Format Rules
```yaml
hcpcs_level_ii_validation:
  # Letter followed by 4 digits
  pattern: "^[A-V][0-9]{4}$"
  length: 5
  first_char_alpha: true
  remaining_numeric: true
  
  # Category meanings
  categories:
    A: "Transportation, Medical and Surgical Supplies"
    B: "Enteral and Parenteral Therapy"  
    C: "Outpatient PPS"
    D: "Dental Procedures"
    E: "Durable Medical Equipment"
    G: "Procedures/Professional Services (Temporary)"
    H: "Alcohol and Drug Abuse Treatment Services"
    J: "Drugs Administered Other Than Oral Method"
    K: "Temporary Codes"
    L: "Orthotic/Prosthetic Procedures"
    M: "Medical Services"
    P: "Pathology and Laboratory Services"
    Q: "Temporary Codes"
    R: "Diagnostic Radiology Services"
    S: "Temporary National Codes"
    T: "National T-Codes"
    V: "Vision Services"
```

### NDC (National Drug Code) Validation

For pharmacy and prescription data validation.

#### Format Rules
```yaml
ndc_validation:
  # Multiple valid formats
  formats:
    - "XXXXX-YYYY-ZZ"    # 5-4-2 format
    - "XXXX-YYYY-ZZ"     # 4-4-2 format  
    - "XXXXX-YYY-ZZ"     # 5-3-2 format
    - "XXXXXXXXXXX"      # 11-digit no hyphens
  
  # Components
  labeler_code: "4-5 digits"      # FDA assigned
  product_code: "3-4 digits"      # Product identification
  package_code: "1-2 digits"      # Package size/type
  
  # Validation rules
  total_digits: 10-11
  numeric_only: true
  leading_zeros_preserved: true
```

---

## Healthcare Business Rules

### Member Eligibility Validation

Ensures service dates fall within member eligibility periods.

```sql
-- Member eligibility validation rule
member_eligibility_check:
  rule_name: "service_date_within_eligibility"
  description: "Service date must be within member eligibility period"
  severity: "critical"
  
  sql_condition: |
    EXISTS (
      SELECT 1 FROM member_eligibility e
      WHERE e.member_id = claims.member_id
      AND claims.date_of_service BETWEEN e.effective_date 
        AND COALESCE(e.termination_date, '2099-12-31')
      AND e.status = 'ACTIVE'
    )
```

### Provider Network Validation

Validates provider network participation status.

```sql
provider_network_validation:
  rule_name: "provider_in_network"
  description: "Provider must be in network for service date"
  severity: "warning"  # May be out-of-network coverage
  
  sql_condition: |
    EXISTS (
      SELECT 1 FROM provider_network pn
      WHERE pn.provider_npi = claims.provider_npi
      AND claims.date_of_service BETWEEN pn.contract_start_date 
        AND COALESCE(pn.contract_end_date, '2099-12-31')
      AND pn.network_status = 'PARTICIPATING'
    )
```

### Prior Authorization Validation

Checks for required prior authorizations.

```sql
prior_authorization_validation:
  rule_name: "prior_auth_required_services"
  description: "High-cost services require prior authorization"
  severity: "critical"
  
  conditions:
    - claim_amount > 1000
    - procedure_code IN (SELECT code FROM high_cost_procedures)
    - place_of_service IN ('21', '22', '23')  # Inpatient, outpatient, emergency
  
  sql_condition: |
    EXISTS (
      SELECT 1 FROM prior_authorizations pa
      WHERE pa.member_id = claims.member_id
      AND pa.procedure_code = claims.procedure_code
      AND claims.date_of_service BETWEEN pa.auth_start_date AND pa.auth_end_date
      AND pa.status = 'APPROVED'
    )
```

### Duplicate Claims Detection

Identifies potential duplicate claim submissions.

```sql
duplicate_claims_detection:
  rule_name: "duplicate_claim_check"
  description: "Detect potential duplicate claims"
  severity: "warning"
  
  duplicate_criteria:
    - same_member_id
    - same_provider_npi
    - same_date_of_service
    - same_procedure_code
    - same_diagnosis_code
    - submitted_within_days: 30
  
  sql_condition: |
    NOT EXISTS (
      SELECT 1 FROM claims c2
      WHERE c2.member_id = claims.member_id
      AND c2.provider_npi = claims.provider_npi
      AND c2.date_of_service = claims.date_of_service
      AND c2.procedure_code = claims.procedure_code
      AND c2.diagnosis_code = claims.diagnosis_code
      AND c2.claim_id != claims.claim_id
      AND c2.submission_date BETWEEN 
        (claims.submission_date - INTERVAL 30 DAY) 
        AND claims.submission_date
      AND c2.claim_status NOT IN ('DENIED', 'VOIDED')
    )
```

---

## Regulatory Compliance Rules

### HIPAA Compliance Validation

Ensures Protected Health Information (PHI) is properly handled.

#### PHI Field Identification
```yaml
phi_fields:
  direct_identifiers:
    - member_id
    - ssn
    - medical_record_number
    - account_number
    - certificate_number
    - license_number
    - vehicle_identifiers
    - device_identifiers
    - biometric_identifiers
    - full_face_photos
    - any_other_unique_identifying_number
  
  indirect_identifiers:
    - name
    - address  # All geographic subdivisions smaller than state
    - birth_date
    - admission_date
    - discharge_date
    - death_date
    - age_over_89
    - phone_number
    - fax_number
    - email_address
    - web_urls
```

#### PHI Protection Rules
```yaml
phi_protection_rules:
  encryption_at_rest:
    required: true
    algorithm: "AES-256"
    key_management: "customer_managed"  # or databricks_managed
  
  encryption_in_transit:
    required: true
    tls_version: "1.2_minimum"
  
  access_controls:
    rbac_enabled: true
    minimum_necessary: true
    audit_all_access: true
  
  data_masking:
    ssn: "XXX-XX-{last_4_digits}"
    member_id: "{hash_function}"
    phone: "XXX-XXX-{last_4_digits}"
    email: "{first_letter}***@{domain}"
```

### State Medicaid Compliance

State-specific validation rules for Medicaid programs.

#### California Medicaid (Medi-Cal)
```yaml
california_medicaid_rules:
  member_id_format:
    pattern: "^[A-Z]{2}[0-9]{9}$"  # CA + 9 digits
    prefix_required: "CA"
  
  specific_validations:
    managed_care_plan:
      required: true
      valid_plans: ["ANTHEM", "BLUE_CROSS", "HEALTH_NET", "KAISER", "LA_CARE"]
    
    aid_code:
      required: true
      valid_codes: ["01", "02", "03", "04", "05"]  # Medi-Cal aid codes
    
    county_code:
      required: true
      range: "01-58"  # California county codes
```

#### New York Medicaid
```yaml
new_york_medicaid_rules:
  member_id_format:
    pattern: "^[0-9]{8}NY$"  # 8 digits + NY
    suffix_required: "NY"
  
  specific_validations:
    case_number:
      format: "^[A-Z][0-9]{7}$"
      required_for_families: true
    
    managed_care_enrollment:
      validation_required: true
      enrollment_date_check: true
```

#### Texas Medicaid
```yaml
texas_medicaid_rules:
  member_id_format:
    pattern: "^TX[A-Z][0-9]{8}$"  # TX + letter + 8 digits
    prefix_required: "TX"
  
  specific_validations:
    star_plus_enrollment:
      required_for_elderly: true
      required_for_disabled: true
    
    chip_eligibility:
      income_validation: true
      family_size_check: true
```

### Medicare Compliance

Medicare-specific validation rules.

#### Medicare Parts A, B, C, D
```yaml
medicare_parts_validation:
  part_a_claims:
    # Inpatient hospital services
    required_fields:
      - medicare_beneficiary_id
      - provider_npi
      - admission_date
      - discharge_date
      - drg_code
      - total_charges
    
    business_rules:
      - max_length_of_stay: 365
      - valid_drg_codes: "001-999"
      - principal_diagnosis_required: true
  
  part_b_claims:
    # Outpatient medical services
    required_fields:
      - medicare_beneficiary_id
      - provider_npi
      - date_of_service
      - procedure_code
      - diagnosis_code
    
    business_rules:
      - multiple_procedures_allowed: true
      - modifier_validation: true
      - place_of_service_validation: true
  
  part_d_claims:
    # Prescription drug benefits
    required_fields:
      - medicare_beneficiary_id
      - prescriber_npi
      - pharmacy_npi
      - ndc_code
      - quantity_dispensed
      - days_supply
    
    business_rules:
      - formulary_check: true
      - coverage_gap_calculation: true
      - late_enrollment_penalty: true
```

---

## Data Quality Dimensions

### Completeness Validation

Ensures required fields are populated and not null/empty.

```yaml
completeness_rules:
  critical_fields:
    # Must never be null
    - member_id
    - provider_npi
    - date_of_service
    - claim_id
    
  required_fields:
    # Should not be null (warnings if missing)
    - diagnosis_code
    - procedure_code
    - claim_amount
    - place_of_service
    
  conditional_requirements:
    # Required based on conditions
    - field: "prescriber_npi"
      condition: "claim_type = 'PHARMACY'"
    - field: "ndc_code"
      condition: "claim_type = 'PHARMACY'"
    - field: "admission_date"
      condition: "place_of_service = '21'"  # Inpatient

completeness_thresholds:
  critical: 100%    # Critical fields must be 100% complete
  required: 95%     # Required fields target 95% completeness
  conditional: 98%  # Conditional fields when condition is met
```

### Validity Validation

Ensures data values conform to expected formats and ranges.

```yaml
validity_rules:
  date_fields:
    date_of_service:
      format: "YYYY-MM-DD"
      min_date: "2000-01-01"
      max_date: "current_date + 30 days"
      not_future: false  # Allow some future dates
    
    date_of_birth:
      format: "YYYY-MM-DD"
      min_date: "1900-01-01"
      max_date: "current_date"
      age_validation:
        min_age: 0
        max_age: 120
  
  numeric_fields:
    claim_amount:
      data_type: "decimal"
      precision: 2
      min_value: 0.01
      max_value: 1000000.00
      not_negative: true
    
    age:
      data_type: "integer"
      min_value: 0
      max_value: 120
  
  categorical_fields:
    gender:
      allowed_values: ["M", "F", "U", "X"]  # Include non-binary
      case_sensitive: false
    
    claim_status:
      allowed_values: ["SUBMITTED", "PROCESSING", "PAID", "DENIED", "PENDING"]
```

### Consistency Validation

Ensures data is consistent across related fields and tables.

```yaml
consistency_rules:
  cross_field_validation:
    # Age should match date of birth
    - rule_name: "age_dob_consistency"
      fields: ["age", "date_of_birth"]
      validation: "DATEDIFF(current_date, date_of_birth) / 365 BETWEEN age - 1 AND age + 1"
    
    # Service date should be after birth date
    - rule_name: "service_after_birth"
      fields: ["date_of_service", "date_of_birth"]
      validation: "date_of_service > date_of_birth"
    
    # Discharge date after admission date
    - rule_name: "discharge_after_admission"
      fields: ["discharge_date", "admission_date"]
      validation: "discharge_date >= admission_date"
  
  referential_integrity:
    # Provider NPI must exist in provider table
    - rule_name: "provider_exists"
      foreign_key: "provider_npi"
      reference_table: "providers"
      reference_key: "npi"
    
    # Member ID must exist in eligibility table
    - rule_name: "member_exists"
      foreign_key: "member_id"
      reference_table: "member_eligibility"
      reference_key: "member_id"
```

---

## Anomaly Detection Rules

### Statistical Anomaly Detection

Identifies data points that deviate significantly from expected patterns.

```yaml
statistical_anomalies:
  z_score_detection:
    enabled: true
    threshold: 3.0  # Values beyond 3 standard deviations
    fields:
      - claim_amount
      - length_of_stay
      - units_of_service
    
  iqr_detection:
    enabled: true
    multiplier: 1.5  # Values beyond Q1 - 1.5*IQR or Q3 + 1.5*IQR
    fields:
      - claim_amount
      - patient_age
    
  isolation_forest:
    enabled: true
    contamination: 0.1  # Expected proportion of anomalies
    features:
      - claim_amount
      - procedure_code_frequency
      - provider_utilization
      - member_utilization
```

### Healthcare Domain Anomalies

Domain-specific anomaly detection rules.

```yaml
healthcare_anomalies:
  claim_amount_anomalies:
    # Unusually high claim amounts
    - rule_name: "high_claim_amount"
      condition: |
        claim_amount > (
          SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY claim_amount)
          FROM claims_history
          WHERE procedure_code = current.procedure_code
        )
      severity: "medium"
    
    # Zero dollar claims (may indicate coding errors)
    - rule_name: "zero_dollar_claim"
      condition: "claim_amount = 0 AND claim_status != 'DENIED'"
      severity: "high"
  
  utilization_anomalies:
    # Member with excessive utilization
    - rule_name: "excessive_member_utilization"
      condition: |
        (SELECT COUNT(*) FROM claims
         WHERE member_id = current.member_id
         AND date_of_service >= CURRENT_DATE - INTERVAL 30 DAY) > 20
      severity: "medium"
    
    # Provider with unusual billing patterns
    - rule_name: "provider_billing_spike"
      condition: |
        (SELECT COUNT(*) FROM claims
         WHERE provider_npi = current.provider_npi
         AND date_of_service >= CURRENT_DATE - INTERVAL 7 DAY) > 
        (SELECT AVG(weekly_claims) * 3
         FROM provider_utilization_stats
         WHERE provider_npi = current.provider_npi)
      severity: "high"
  
  temporal_anomalies:
    # Claims submitted long after service date
    - rule_name: "late_claim_submission"
      condition: "submission_date > date_of_service + INTERVAL 90 DAY"
      severity: "low"
    
    # Weekend/holiday service patterns
    - rule_name: "unusual_service_timing"
      condition: |
        DAYOFWEEK(date_of_service) IN (1, 7)  -- Sunday, Saturday
        AND procedure_code NOT IN (
          SELECT code FROM emergency_procedures
        )
      severity: "low"
```

---

## Quality Scoring and Thresholds

### Quality Score Calculation

The platform calculates weighted quality scores across dimensions.

```yaml
quality_scoring:
  dimension_weights:
    completeness: 0.25    # 25%
    validity: 0.30        # 30%
    consistency: 0.20     # 20%
    accuracy: 0.15        # 15%
    timeliness: 0.10      # 10%
  
  scoring_method: "weighted_average"
  scale: "0-100"  # Percentage scale
  
  calculation_formula: |
    overall_quality_score = 
      (completeness_score * 0.25) +
      (validity_score * 0.30) +
      (consistency_score * 0.20) +
      (accuracy_score * 0.15) +
      (timeliness_score * 0.10)
```

### Alert Thresholds

Configure when quality alerts are triggered.

```yaml
alert_thresholds:
  global_thresholds:
    critical_quality_score: 60    # Below 60% triggers critical alert
    warning_quality_score: 80     # Below 80% triggers warning
    target_quality_score: 95      # Target quality level
    excellent_quality_score: 98   # Excellence benchmark
  
  dimension_thresholds:
    completeness:
      critical: 70
      warning: 85
      target: 95
    
    validity:
      critical: 75
      warning: 90
      target: 98
    
    healthcare_codes:
      npi_validation_rate:
        critical: 90
        warning: 95
        target: 99
      
      icd10_validation_rate:
        critical: 85
        warning: 92
        target: 97
```

---

## Custom Validation Rules

### Creating Custom Rules

Organizations can define custom validation rules for specific requirements.

```yaml
# Example: Custom state-specific validation
custom_rules:
  ca_specific_validation:
    rule_name: "california_medi_cal_validation"
    description: "California-specific Medi-Cal validation rules"
    applies_to: "claims"
    conditions:
      - "state_code = 'CA'"
      - "program_type = 'MEDICAID'"
    
    validations:
      - field: "member_id"
        pattern: "^CA[0-9]{9}$"
        error_message: "California Medi-Cal member ID must start with 'CA' followed by 9 digits"
      
      - field: "county_code"
        valid_range: "01-58"
        lookup_table: "ca_county_codes"
        error_message: "Invalid California county code"
      
      - field: "aid_code"
        allowed_values: ["01", "02", "03", "04", "05", "06"]
        error_message: "Invalid California Medi-Cal aid code"

  # Example: Organization-specific business rule
  org_specific_rule:
    rule_name: "high_value_claim_review"
    description: "Flag high-value claims for manual review"
    applies_to: "claims"
    
    conditions:
      - "claim_amount > 10000"
      - "procedure_code NOT IN (SELECT code FROM pre_approved_high_cost_procedures)"
    
    action: "flag_for_review"
    severity: "medium"
    notification_required: true
```

### Rule Management

```yaml
rule_management:
  versioning:
    enabled: true
    version_format: "YYYY.MM.DD.sequence"
    change_log_required: true
  
  testing:
    test_data_required: true
    validation_test_suite: true
    regression_testing: true
  
  deployment:
    staging_validation: true
    rollback_capability: true
    gradual_rollout: true
  
  monitoring:
    rule_performance_tracking: true
    false_positive_monitoring: true
    effectiveness_metrics: true
```

---

## Implementation Examples

### Python Implementation

```python
from src.agents.quality import QualityEngine, ValidationRule, QualityDimension, RuleSeverity

# Initialize quality engine
engine = QualityEngine(spark, config)

# Create custom healthcare validation rule
custom_rule = ValidationRule(
    name="medicare_beneficiary_id_validation",
    description="Validate Medicare beneficiary ID format",
    dimension=QualityDimension.VALIDITY,
    severity=RuleSeverity.CRITICAL,
    condition="medicare_id RLIKE '^[0-9]{3}-[0-9]{2}-[0-9]{4}[A-Z]$'",
    field_names=["medicare_id", "medicare_beneficiary_id"],
    tags=["medicare", "format_validation"]
)

# Register the rule
engine.register_rule(custom_rule)

# Run quality assessment
results = engine.assess_table_quality(
    df=claims_dataframe,
    table_name="healthcare_data.silver.medicare_claims"
)

print(f"Overall Quality Score: {results['overall_score']:.2f}%")
```

### SQL Implementation

```sql
-- Example quality check query
WITH quality_metrics AS (
  SELECT 
    -- Completeness checks
    COUNT(*) as total_records,
    COUNT(member_id) / COUNT(*) * 100 as member_id_completeness,
    COUNT(provider_npi) / COUNT(*) * 100 as npi_completeness,
    
    -- Validity checks  
    COUNT(CASE WHEN provider_npi RLIKE '^[0-9]{10}$' THEN 1 END) / COUNT(*) * 100 as npi_format_validity,
    COUNT(CASE WHEN diagnosis_code RLIKE '^[A-TV-Z][0-9][A-Z0-9](\\.[A-Z0-9]{0,4})?$' THEN 1 END) / COUNT(*) * 100 as icd10_validity,
    
    -- Business rule checks
    COUNT(CASE WHEN claim_amount > 0 THEN 1 END) / COUNT(*) * 100 as positive_claim_amount,
    COUNT(CASE WHEN date_of_service <= CURRENT_DATE THEN 1 END) / COUNT(*) * 100 as service_date_not_future
    
  FROM healthcare_data.silver.claims
  WHERE processing_date >= CURRENT_DATE - INTERVAL 7 DAY
)

SELECT 
  total_records,
  (member_id_completeness + npi_completeness) / 2 as avg_completeness,
  (npi_format_validity + icd10_validity) / 2 as avg_validity,
  (positive_claim_amount + service_date_not_future) / 2 as avg_business_rules,
  
  -- Overall quality score
  (
    (member_id_completeness + npi_completeness) / 2 * 0.25 +
    (npi_format_validity + icd10_validity) / 2 * 0.30 +
    (positive_claim_amount + service_date_not_future) / 2 * 0.45
  ) as overall_quality_score
  
FROM quality_metrics;
```

---

## Best Practices

### Rule Design Principles

1. **Start Simple**: Begin with basic format and completeness rules
2. **Iterate**: Add complexity based on data patterns and business needs
3. **Document**: Maintain clear documentation for all rules
4. **Test**: Validate rules against known good/bad data
5. **Monitor**: Track rule performance and false positives

### Performance Optimization

1. **Indexing**: Ensure proper indexing on validated fields
2. **Sampling**: Use sampling for expensive validations on large datasets
3. **Caching**: Cache lookup table results and validation outcomes
4. **Parallel Processing**: Leverage Spark's parallel processing capabilities
5. **Rule Ordering**: Execute fast rules first, expensive rules last

### Maintenance Guidelines

1. **Regular Review**: Review and update rules quarterly
2. **Code Evolution**: Update rules as healthcare codes evolve
3. **Regulation Changes**: Monitor regulatory changes and update accordingly
4. **Performance Monitoring**: Track rule execution times and optimize
5. **False Positive Analysis**: Analyze and reduce false positives

---

For implementation details and examples, see:
- [Quality Engine API Documentation](../api/quality.md)
- [CLI Quality Commands](../cli/README.md#data-quality-commands)
- [Configuration Guide](../configuration.md#quality-rules-configuration)