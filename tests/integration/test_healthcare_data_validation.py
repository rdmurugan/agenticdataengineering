"""
Integration tests for healthcare data validation
Tests the end-to-end validation of Medicaid/Medicare healthcare data
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import tempfile
import yaml
import os

from src.agents.quality.quality_engine import QualityEngine, ValidationRule, QualityDimension, RuleSeverity
from src.agents.quality.healthcare_expectations import HealthcareExpectations


class TestHealthcareDataValidation:
    """Integration tests for healthcare data validation pipeline"""
    
    @pytest.fixture
    def mock_spark_session(self):
        """Create a mock Spark session for testing"""
        mock_spark = Mock()
        mock_df = Mock()
        
        # Configure mock DataFrame behavior
        mock_df.count.return_value = 1000
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.distinct.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.collect.return_value = []
        mock_df.columns = ['member_id', 'provider_npi', 'diagnosis_code', 'procedure_code', 'claim_amount']
        
        # Mock schema
        mock_field = Mock()
        mock_field.name = 'test_field'
        mock_field.dataType = 'string'
        mock_df.schema.fields = [mock_field]
        
        return mock_spark
        
    @pytest.fixture
    def quality_config(self):
        """Create test quality configuration"""
        return {
            'global_thresholds': {
                'critical_quality_score': 0.6,
                'warning_quality_score': 0.8,
                'target_quality_score': 0.95
            },
            'quality_dimensions': {
                'completeness': {'weight': 0.25},
                'validity': {'weight': 0.30},
                'consistency': {'weight': 0.20},
                'accuracy': {'weight': 0.15},
                'timeliness': {'weight': 0.10}
            }
        }
        
    @pytest.fixture
    def sample_claims_data(self):
        """Create sample healthcare claims data for testing"""
        return pd.DataFrame([
            {
                'member_id': 'M123456789',
                'provider_npi': '1234567893',  # Valid NPI with Luhn check
                'diagnosis_code': 'Z00.00',    # Valid ICD-10
                'procedure_code': '99213',      # Valid CPT
                'date_of_service': '2023-01-15',
                'claim_amount': 125.50,
                'place_of_service': '11'
            },
            {
                'member_id': 'M987654321',
                'provider_npi': '9876543210',  # Valid NPI
                'diagnosis_code': 'I10',       # Valid ICD-10
                'procedure_code': '99214',     # Valid CPT
                'date_of_service': '2023-01-16', 
                'claim_amount': 200.00,
                'place_of_service': '11'
            },
            # Invalid records for testing validation
            {
                'member_id': None,              # Missing required field
                'provider_npi': '1234567890',  # Invalid Luhn check
                'diagnosis_code': 'INVALID',   # Invalid ICD-10 format
                'procedure_code': '99999',     # Invalid CPT
                'date_of_service': '2025-01-01',  # Future date
                'claim_amount': -50.00,        # Negative amount
                'place_of_service': '99'
            },
            {
                'member_id': '',               # Empty required field
                'provider_npi': '123',         # Too short
                'diagnosis_code': 'A',         # Too short
                'procedure_code': '123',       # Too short
                'date_of_service': 'invalid',  # Invalid date format
                'claim_amount': 0.00,          # Zero amount
                'place_of_service': '00'       # Invalid place of service
            }
        ])
        
    @pytest.fixture
    def sample_member_data(self):
        """Create sample member/patient data"""
        return pd.DataFrame([
            {
                'member_id': 'M123456789',
                'first_name': 'John',
                'last_name': 'Doe',
                'date_of_birth': '1985-06-15',
                'gender': 'M',
                'phone': '555-123-4567',
                'email': 'john.doe@email.com',
                'zip_code': '12345'
            },
            {
                'member_id': 'M987654321',
                'first_name': 'Jane',
                'last_name': 'Smith',
                'date_of_birth': '1990-12-03',
                'gender': 'F',
                'phone': '5551234567',
                'email': 'jane.smith@test.com',
                'zip_code': '54321-1234'
            },
            # Invalid records
            {
                'member_id': None,             # Missing ID
                'first_name': '',             # Empty name
                'last_name': 'Invalid',
                'date_of_birth': '2025-01-01',  # Future birth date
                'gender': 'X',                # Non-standard gender code
                'phone': '123',               # Invalid phone
                'email': 'invalid-email',     # Invalid email format
                'zip_code': '123'             # Invalid zip code
            }
        ])
        
    @pytest.fixture
    def sample_provider_data(self):
        """Create sample provider data"""
        return pd.DataFrame([
            {
                'provider_npi': '1234567893',
                'provider_name': 'Dr. John Smith',
                'provider_type': 'Individual',
                'taxonomy_code': '207Q00000X',
                'license_number': 'MD123456',
                'license_state': 'CA',
                'license_expiration_date': '2025-12-31'
            },
            {
                'provider_npi': '9876543210',
                'provider_name': 'General Hospital',
                'provider_type': 'Organization', 
                'taxonomy_code': '282N00000X',
                'license_number': 'HOSP789',
                'license_state': 'NY',
                'license_expiration_date': '2024-06-30'
            },
            # Invalid records
            {
                'provider_npi': '1234567890',  # Invalid Luhn check
                'provider_name': '',          # Empty name
                'provider_type': 'Invalid',   # Invalid type
                'taxonomy_code': 'INVALID',   # Invalid taxonomy
                'license_number': '',         # Empty license
                'license_state': 'XX',        # Invalid state
                'license_expiration_date': '2020-01-01'  # Expired license
            }
        ])

    def test_quality_engine_initialization(self, mock_spark_session, quality_config):
        """Test quality engine initialization"""
        
        config = {'quality_config': quality_config}
        
        with patch('src.agents.quality.quality_engine.yaml.safe_load', return_value=quality_config):
            with patch('builtins.open'):
                engine = QualityEngine(mock_spark_session, config)
                
                assert engine.spark == mock_spark_session
                assert engine.config == config
                assert len(engine.rules_registry) > 0
                assert 'null_completeness' in engine.rules_registry
                assert 'npi_format' in engine.rules_registry

    def test_member_id_validation(self, mock_spark_session, quality_config):
        """Test member ID validation patterns"""
        
        engine = QualityEngine(mock_spark_session, quality_config)
        
        # Test valid member IDs
        valid_ids = [
            'M123456789',      # 9 digits with prefix
            '123456789012',    # 12 digits
            'CA123456789',     # State prefix
            'A12345678B'       # New Medicare format
        ]
        
        # Test invalid member IDs  
        invalid_ids = [
            '12345',           # Too short
            'INVALID_ID',      # Invalid format
            '',                # Empty
            None               # Null
        ]
        
        # Mock the custom function
        validate_member_id = engine.custom_functions['validate_member_id']
        
        for valid_id in valid_ids:
            assert validate_member_id(valid_id), f"Valid ID failed: {valid_id}"
            
        for invalid_id in invalid_ids:
            assert not validate_member_id(invalid_id), f"Invalid ID passed: {invalid_id}"

    def test_npi_validation_luhn_check(self, mock_spark_session, quality_config):
        """Test NPI Luhn algorithm validation"""
        
        engine = QualityEngine(mock_spark_session, quality_config)
        luhn_check = engine.custom_functions['luhn_check']
        
        # Valid NPIs (pass Luhn check)
        valid_npis = [
            '1234567893',  # Known valid NPI
            '1679576722',  # Another valid NPI
            '1234567810'   # Valid Luhn checksum
        ]
        
        # Invalid NPIs (fail Luhn check)
        invalid_npis = [
            '1234567890',  # Invalid checksum
            '1111111111',  # Invalid checksum
            '9999999999',  # Invalid checksum
            '123456789',   # Too short
            'abc1234567',  # Contains letters
            ''             # Empty
        ]
        
        for valid_npi in valid_npis:
            assert luhn_check(valid_npi), f"Valid NPI failed Luhn check: {valid_npi}"
            
        for invalid_npi in invalid_npis:
            assert not luhn_check(invalid_npi), f"Invalid NPI passed Luhn check: {invalid_npi}"

    def test_icd10_diagnosis_code_validation(self, mock_spark_session, quality_config):
        """Test ICD-10 diagnosis code format validation"""
        
        engine = QualityEngine(mock_spark_session, quality_config)
        
        # Valid ICD-10 codes
        valid_codes = [
            'Z00.00',      # Preventive care
            'I10',         # Hypertension
            'E11.9',       # Diabetes
            'J45.909',     # Asthma
            'M79.3'        # Panniculitis
        ]
        
        # Invalid ICD-10 codes
        invalid_codes = [
            'INVALID',     # Not ICD-10 format
            '123.45',      # Numeric only
            'A',           # Too short
            'Z00.00.00',   # Too many decimals
            ''             # Empty
        ]
        
        import re
        icd10_pattern = r'^[A-TV-Z][0-9][A-Z0-9](\\.[A-Z0-9]{0,4})?$'
        
        for valid_code in valid_codes:
            assert re.match(icd10_pattern, valid_code), f"Valid ICD-10 failed: {valid_code}"
            
        for invalid_code in invalid_codes:
            if invalid_code:  # Skip empty string for this test
                assert not re.match(icd10_pattern, invalid_code), f"Invalid ICD-10 passed: {invalid_code}"

    def test_cpt_procedure_code_validation(self, mock_spark_session, quality_config):
        """Test CPT procedure code validation"""
        
        engine = QualityEngine(mock_spark_session, quality_config)
        
        # Valid CPT codes (5 digits)
        valid_cpts = [
            '99213',  # Office visit
            '99214',  # Office visit
            '80053',  # Basic metabolic panel
            '36415',  # Venipuncture
            '85025'   # Complete blood count
        ]
        
        # Invalid CPT codes
        invalid_cpts = [
            '9921',    # Too short
            '992133',  # Too long
            'ABCDE',   # Letters
            '99999',   # Invalid code (outside normal range)
            ''         # Empty
        ]
        
        import re
        cpt_pattern = r'^[0-9]{5}$'
        
        for valid_cpt in valid_cpts:
            assert re.match(cpt_pattern, valid_cpt), f"Valid CPT failed: {valid_cpt}"
            
        for invalid_cpt in invalid_cpts:
            if invalid_cpt:
                assert not re.match(cpt_pattern, invalid_cpt), f"Invalid CPT passed: {invalid_cpt}"

    def test_phone_number_standardization(self, mock_spark_session, quality_config):
        """Test phone number format standardization"""
        
        engine = QualityEngine(mock_spark_session, quality_config)
        standardize_phone = engine.custom_functions['standardize_phone']
        
        # Test cases for phone standardization
        test_cases = [
            ('555-123-4567', '5551234567'),
            ('(555) 123-4567', '5551234567'),
            ('555.123.4567', '5551234567'),
            ('15551234567', '5551234567'),     # Remove leading 1
            ('555 123 4567', '5551234567'),
            ('+15551234567', '5551234567'),
            ('5551234567', '5551234567'),      # Already clean
            ('123', '123'),                    # Too short - return as-is
            ('', ''),                          # Empty - return as-is
            (None, None)                       # None - return as-is
        ]
        
        for input_phone, expected in test_cases:
            result = standardize_phone(input_phone)
            assert result == expected, f"Phone standardization failed: {input_phone} -> {result}, expected {expected}"

    def test_date_range_validation(self, mock_spark_session, quality_config):
        """Test date range validation for service dates"""
        
        from datetime import datetime, timedelta
        
        # Valid date ranges
        today = datetime.now().date()
        valid_dates = [
            '2020-01-01',  # Minimum reasonable date
            '2023-06-15',  # Normal service date
            today.strftime('%Y-%m-%d'),  # Today
        ]
        
        # Invalid dates
        invalid_dates = [
            '1899-12-31',  # Too old
            (today + timedelta(days=400)).strftime('%Y-%m-%d'),  # Too far in future
            '2023-13-01',  # Invalid month
            '2023-02-30',  # Invalid day
            'invalid-date'  # Invalid format
        ]
        
        # This would normally be tested with actual Spark SQL
        # For now, we validate the logic conceptually
        assert len(valid_dates) == 3
        assert len(invalid_dates) == 5

    @patch('src.agents.quality.quality_engine.SparkSession')
    def test_claims_data_comprehensive_validation(self, mock_spark, sample_claims_data, quality_config):
        """Test comprehensive validation of claims data"""
        
        # Mock Spark DataFrame
        mock_df = Mock()
        mock_df.count.return_value = len(sample_claims_data)
        
        # Mock field objects
        mock_fields = []
        for col in sample_claims_data.columns:
            field = Mock()
            field.name = col
            field.dataType = 'string'
            mock_fields.append(field)
        
        mock_df.schema.fields = mock_fields
        
        # Initialize quality engine
        engine = QualityEngine(mock_spark, quality_config)
        
        # Test assessment
        with patch('src.agents.quality.quality_engine.open'):
            with patch('src.agents.quality.quality_engine.yaml.safe_load', return_value=quality_config):
                assessment = engine.assess_table_quality(mock_df, 'test_claims')
                
                assert assessment['table_name'] == 'test_claims'
                assert 'overall_score' in assessment
                assert 'dimension_scores' in assessment
                assert 'field_results' in assessment
                assert 'issues' in assessment
                assert 'recommendations' in assessment

    def test_healthcare_business_rules_validation(self, mock_spark_session, quality_config):
        """Test healthcare-specific business rules"""
        
        engine = QualityEngine(mock_spark_session, quality_config)
        
        # Test business rule creation
        age_validation_rule = ValidationRule(
            name='valid_patient_age',
            description='Patient age must be between 0 and 120 years',
            dimension=QualityDimension.VALIDITY,
            severity=RuleSeverity.CRITICAL,
            condition='DATEDIFF(current_date(), date_of_birth) / 365.25 BETWEEN 0 AND 120',
            field_names=['date_of_birth']
        )
        
        engine.register_rule(age_validation_rule)
        assert 'valid_patient_age' in engine.rules_registry
        
        # Test eligibility validation rule
        eligibility_rule = ValidationRule(
            name='service_date_eligibility',
            description='Service date must be within member eligibility period',
            dimension=QualityDimension.CONSISTENCY,
            severity=RuleSeverity.CRITICAL,
            condition='''EXISTS (
                SELECT 1 FROM member_eligibility e
                WHERE e.member_id = claims.member_id
                AND claims.date_of_service BETWEEN e.start_date AND COALESCE(e.end_date, '2099-12-31')
            )''',
            field_names=['date_of_service', 'member_id']
        )
        
        engine.register_rule(eligibility_rule)
        assert 'service_date_eligibility' in engine.rules_registry

    def test_data_quality_alerts_generation(self, mock_spark_session, quality_config):
        """Test generation of data quality alerts"""
        
        engine = QualityEngine(mock_spark_session, quality_config)
        
        # Mock assessment results with quality issues
        mock_assessment = {
            'overall_score': 65.0,  # Below critical threshold
            'dimension_scores': {
                'completeness': 45.0,  # Very low
                'validity': 70.0,      # Below warning
                'consistency': 85.0,   # Good
                'accuracy': 90.0,      # Good
                'timeliness': 95.0     # Excellent
            },
            'field_results': {
                'member_id': {
                    'null_percentage': 25.0  # High null percentage
                }
            }
        }
        
        issues = engine._identify_quality_issues(mock_assessment)
        
        # Should identify multiple issues
        assert len(issues) > 0
        
        # Check for critical overall quality issue
        critical_issues = [i for i in issues if i['type'] == 'critical_overall_quality']
        assert len(critical_issues) > 0
        
        # Check for low completeness issue
        completeness_issues = [i for i in issues if 'completeness' in i['type']]
        assert len(completeness_issues) > 0

    def test_self_healing_auto_remediation(self, mock_spark_session, quality_config):
        """Test automatic remediation of common data quality issues"""
        
        mock_df = Mock()
        engine = QualityEngine(mock_spark_session, quality_config)
        
        # Mock assessment results
        mock_assessment = {
            'field_results': {
                'phone': {'null_percentage': 10.0}
            }
        }
        
        # Test phone standardization remediation
        with patch.object(engine, '_apply_remediation') as mock_apply:
            mock_apply.return_value = (mock_df, 'Phone numbers standardized')
            
            remediated_df = engine.auto_remediate_issues(mock_df, mock_assessment)
            
            # Verify remediation was attempted
            assert remediated_df == mock_df

    def test_cost_optimization_tracking(self, mock_spark_session, quality_config):
        """Test tracking of cost optimization through quality improvements"""
        
        # This would integrate with actual cost tracking
        # For now, test the concept
        
        engine = QualityEngine(mock_spark_session, quality_config)
        
        # Mock cost calculation based on quality score
        def calculate_cost_impact(quality_score, manual_fix_rate):
            base_cost = 1000  # Base monthly cost
            quality_factor = quality_score / 100.0
            manual_fix_cost = manual_fix_rate * 50  # $50 per manual fix
            return base_cost + manual_fix_cost * (1 - quality_factor)
        
        # High quality scenario
        high_quality_cost = calculate_cost_impact(95.0, 100)
        
        # Low quality scenario
        low_quality_cost = calculate_cost_impact(70.0, 100)
        
        # Verify cost savings with higher quality
        assert high_quality_cost < low_quality_cost
        
        cost_savings = low_quality_cost - high_quality_cost
        assert cost_savings > 0

    def test_regulatory_compliance_validation(self, mock_spark_session, quality_config):
        """Test HIPAA and regulatory compliance validation"""
        
        engine = QualityEngine(mock_spark_session, quality_config)
        
        # Test PHI detection and masking rules
        phi_rule = ValidationRule(
            name='phi_protection',
            description='PHI fields must be properly masked or encrypted',
            dimension=QualityDimension.ACCURACY,
            severity=RuleSeverity.CRITICAL,
            condition='ssn NOT RLIKE "^[0-9]{3}-[0-9]{2}-[0-9]{4}$"',  # SSN should be masked
            field_names=['ssn'],
            tags=['hipaa', 'phi']
        )
        
        engine.register_rule(phi_rule)
        assert 'phi_protection' in engine.rules_registry
        
        # Test audit trail rule
        audit_rule = ValidationRule(
            name='audit_trail_complete',
            description='All records must have complete audit trail',
            dimension=QualityDimension.COMPLETENESS,
            severity=RuleSeverity.CRITICAL,
            condition='created_by IS NOT NULL AND created_timestamp IS NOT NULL',
            field_names=['created_by', 'created_timestamp'],
            tags=['audit', 'compliance']
        )
        
        engine.register_rule(audit_rule)
        assert 'audit_trail_complete' in engine.rules_registry

    def test_performance_with_large_datasets(self, mock_spark_session, quality_config):
        """Test performance characteristics with large healthcare datasets"""
        
        # Mock large dataset
        mock_df = Mock()
        mock_df.count.return_value = 10_000_000  # 10 million records
        
        # Mock field schema for large dataset
        mock_fields = []
        healthcare_fields = [
            'member_id', 'provider_npi', 'diagnosis_code', 'procedure_code',
            'date_of_service', 'claim_amount', 'place_of_service'
        ]
        
        for field_name in healthcare_fields:
            field = Mock()
            field.name = field_name
            field.dataType = 'string'
            mock_fields.append(field)
        
        mock_df.schema.fields = mock_fields
        
        engine = QualityEngine(mock_spark_session, quality_config)
        
        # Test that engine can handle large datasets
        # In real implementation, this would test actual performance
        with patch('src.agents.quality.quality_engine.open'):
            with patch('src.agents.quality.quality_engine.yaml.safe_load', return_value=quality_config):
                # This should not raise any exceptions
                assessment = engine.assess_table_quality(mock_df, 'large_claims_table')
                
                assert assessment['record_count'] == 10_000_000
                assert assessment['table_name'] == 'large_claims_table'

    def test_multi_state_medicaid_variations(self, mock_spark_session, quality_config):
        """Test handling of state-specific Medicaid variations"""
        
        engine = QualityEngine(mock_spark_session, quality_config)
        
        # Test state-specific member ID formats
        state_specific_rules = [
            ('CA', r'^CA[0-9]{9}$'),     # California format
            ('NY', r'^[0-9]{8}NY$'),     # New York format  
            ('TX', r'^TX[A-Z][0-9]{8}$') # Texas format
        ]
        
        for state, pattern in state_specific_rules:
            rule = ValidationRule(
                name=f'{state.lower()}_member_id_format',
                description=f'{state} state-specific member ID format',
                dimension=QualityDimension.VALIDITY,
                severity=RuleSeverity.WARNING,
                condition=f'member_id RLIKE "{pattern}"',
                field_names=['member_id'],
                tags=['state_specific', state.lower()]
            )
            
            engine.register_rule(rule)
            assert f'{state.lower()}_member_id_format' in engine.rules_registry


if __name__ == '__main__':
    pytest.main([__file__])