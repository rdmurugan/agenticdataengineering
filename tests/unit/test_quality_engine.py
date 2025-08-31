"""
Unit tests for the Quality Engine core functionality
"""

import pytest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime

from src.agents.quality.quality_engine import (
    QualityEngine, ValidationRule, QualityDimension, RuleSeverity, QualityResult
)


class TestQualityEngine:
    """Unit tests for QualityEngine class"""
    
    @pytest.fixture
    def mock_spark(self):
        return Mock()
        
    @pytest.fixture
    def basic_config(self):
        return {
            'global_thresholds': {
                'critical_quality_score': 0.6,
                'warning_quality_score': 0.8
            },
            'quality_dimensions': {
                'completeness': {'weight': 0.3},
                'validity': {'weight': 0.4},
                'consistency': {'weight': 0.3}
            }
        }
        
    def test_engine_initialization(self, mock_spark, basic_config):
        """Test quality engine initialization"""
        with patch('builtins.open'), patch('yaml.safe_load', return_value=basic_config):
            engine = QualityEngine(mock_spark, {'quality_config_path': 'test.yaml'})
            
            assert engine.spark == mock_spark
            assert len(engine.rules_registry) > 0
            assert len(engine.custom_functions) > 0
            
    def test_luhn_algorithm(self, mock_spark, basic_config):
        """Test Luhn algorithm implementation for NPI validation"""
        with patch('builtins.open'), patch('yaml.safe_load', return_value=basic_config):
            engine = QualityEngine(mock_spark, {})
            luhn_check = engine.custom_functions['luhn_check']
            
            # Test valid NPIs
            assert luhn_check('1234567893') == True
            assert luhn_check('1679576722') == True
            
            # Test invalid NPIs
            assert luhn_check('1234567890') == False
            assert luhn_check('1111111111') == False
            assert luhn_check('') == False
            assert luhn_check(None) == False
            assert luhn_check('abc1234567') == False
            
    def test_phone_standardization(self, mock_spark, basic_config):
        """Test phone number standardization"""
        with patch('builtins.open'), patch('yaml.safe_load', return_value=basic_config):
            engine = QualityEngine(mock_spark, {})
            standardize_phone = engine.custom_functions['standardize_phone']
            
            # Test various phone formats
            assert standardize_phone('555-123-4567') == '5551234567'
            assert standardize_phone('(555) 123-4567') == '5551234567'
            assert standardize_phone('15551234567') == '5551234567'
            assert standardize_phone('555 123 4567') == '5551234567'
            
            # Test edge cases
            assert standardize_phone('') == ''
            assert standardize_phone(None) == None
            assert standardize_phone('123') == '123'  # Too short
            
    def test_member_id_validation(self, mock_spark, basic_config):
        """Test member ID validation patterns"""
        with patch('builtins.open'), patch('yaml.safe_load', return_value=basic_config):
            engine = QualityEngine(mock_spark, {})
            validate_member_id = engine.custom_functions['validate_member_id']
            
            # Valid formats
            assert validate_member_id('123456789') == True      # 9 digits
            assert validate_member_id('123456789012') == True   # 12 digits
            assert validate_member_id('CA123456789') == True    # State prefix
            assert validate_member_id('A12345678B') == True     # Medicare format
            
            # Invalid formats
            assert validate_member_id('12345') == False        # Too short
            assert validate_member_id('') == False             # Empty
            assert validate_member_id(None) == False           # None
            assert validate_member_id('INVALID') == False      # Invalid format
            
    def test_rule_registration(self, mock_spark, basic_config):
        """Test rule registration and management"""
        with patch('builtins.open'), patch('yaml.safe_load', return_value=basic_config):
            engine = QualityEngine(mock_spark, {})
            
            # Create custom rule
            custom_rule = ValidationRule(
                name='test_rule',
                description='Test rule',
                dimension=QualityDimension.VALIDITY,
                severity=RuleSeverity.WARNING,
                condition='test_field IS NOT NULL',
                field_names=['test_field']
            )
            
            # Test registration
            engine.register_rule(custom_rule)
            assert 'test_rule' in engine.rules_registry
            
            # Test unregistration
            engine.unregister_rule('test_rule')
            assert 'test_rule' not in engine.rules_registry
            
    def test_rule_field_matching(self, mock_spark, basic_config):
        """Test rule field pattern matching"""
        with patch('builtins.open'), patch('yaml.safe_load', return_value=basic_config):
            engine = QualityEngine(mock_spark, {})
            
            rule = ValidationRule(
                name='pattern_test',
                description='Pattern test',
                dimension=QualityDimension.VALIDITY,
                severity=RuleSeverity.WARNING,
                condition='test',
                field_names=['*email*', 'phone', '*_id']
            )
            
            # Test pattern matching
            assert engine._rule_applies_to_field(rule, 'user_email', None) == True
            assert engine._rule_applies_to_field(rule, 'phone', None) == True
            assert engine._rule_applies_to_field(rule, 'member_id', None) == True
            assert engine._rule_applies_to_field(rule, 'random_field', None) == False
            
    def test_dimension_score_calculation(self, mock_spark, basic_config):
        """Test quality dimension score calculation"""
        with patch('builtins.open'), patch('yaml.safe_load', return_value=basic_config):
            engine = QualityEngine(mock_spark, {})
            
            # Mock field results
            field_results = {
                'field1': {
                    'dimension_scores': {
                        'completeness': 95.0,
                        'validity': 87.0,
                        'consistency': None  # No applicable rules
                    }
                },
                'field2': {
                    'dimension_scores': {
                        'completeness': 80.0,
                        'validity': 92.0,
                        'consistency': 88.0
                    }
                }
            }
            
            dimension_scores = engine._calculate_dimension_scores(field_results)
            
            # Check calculated scores
            assert dimension_scores['completeness'] == 87.5  # Average of 95 and 80
            assert dimension_scores['validity'] == 89.5      # Average of 87 and 92
            assert dimension_scores['consistency'] == 88.0   # Only field2 has score
            
    def test_overall_score_calculation(self, mock_spark, basic_config):
        """Test overall quality score calculation with weights"""
        with patch('builtins.open'), patch('yaml.safe_load', return_value=basic_config):
            engine = QualityEngine(mock_spark, {})
            
            dimension_scores = {
                'completeness': 90.0,
                'validity': 85.0,
                'consistency': 95.0
            }
            
            overall_score = engine._calculate_overall_score(dimension_scores)
            
            # Calculate expected weighted average
            # completeness: 90 * 0.3 = 27
            # validity: 85 * 0.4 = 34  
            # consistency: 95 * 0.3 = 28.5
            # Total: 89.5
            expected_score = 89.5
            
            assert abs(overall_score - expected_score) < 0.1
            
    def test_quality_issue_identification(self, mock_spark, basic_config):
        """Test identification of quality issues"""
        with patch('builtins.open'), patch('yaml.safe_load', return_value=basic_config):
            engine = QualityEngine(mock_spark, {})
            
            # Mock low quality assessment
            assessment = {
                'overall_score': 55.0,  # Below critical threshold
                'dimension_scores': {
                    'completeness': 45.0,  # Very low
                    'validity': 65.0       # Low
                },
                'field_results': {
                    'test_field': {
                        'null_percentage': 35.0  # High null percentage
                    }
                }
            }
            
            issues = engine._identify_quality_issues(assessment)
            
            assert len(issues) >= 2  # Should identify multiple issues
            
            # Check for critical overall quality issue
            critical_issues = [i for i in issues if i['severity'] == 'critical']
            assert len(critical_issues) > 0
            
    def test_recommendation_generation(self, mock_spark, basic_config):
        """Test quality improvement recommendation generation"""
        with patch('builtins.open'), patch('yaml.safe_load', return_value=basic_config):
            engine = QualityEngine(mock_spark, {})
            
            assessment = {
                'overall_score': 75.0,
                'dimension_scores': {
                    'completeness': 65.0,  # Low completeness
                    'validity': 80.0,
                    'consistency': 85.0
                },
                'issues': [
                    {'description': 'npi validation failed'},
                    {'description': 'member id format invalid'}
                ]
            }
            
            recommendations = engine._generate_recommendations(assessment)
            
            assert len(recommendations) > 0
            
            # Check for completeness-related recommendation
            completeness_recs = [r for r in recommendations if 'completeness' in r.lower() or 'missing' in r.lower()]
            assert len(completeness_recs) > 0
            
            # Check for healthcare-specific recommendations
            healthcare_recs = [r for r in recommendations if any(term in r.lower() for term in ['npi', 'healthcare', 'medical'])]
            assert len(healthcare_recs) > 0


class TestValidationRule:
    """Unit tests for ValidationRule class"""
    
    def test_rule_creation(self):
        """Test validation rule creation"""
        rule = ValidationRule(
            name='test_rule',
            description='Test description',
            dimension=QualityDimension.COMPLETENESS,
            severity=RuleSeverity.CRITICAL,
            condition='field IS NOT NULL',
            field_names=['test_field'],
            threshold=95.0,
            tags=['test']
        )
        
        assert rule.name == 'test_rule'
        assert rule.dimension == QualityDimension.COMPLETENESS
        assert rule.severity == RuleSeverity.CRITICAL
        assert rule.threshold == 95.0
        assert 'test' in rule.tags
        
    def test_rule_defaults(self):
        """Test validation rule default values"""
        rule = ValidationRule(
            name='minimal_rule',
            description='Minimal rule',
            dimension=QualityDimension.VALIDITY,
            severity=RuleSeverity.WARNING,
            condition='test',
            field_names=['field']
        )
        
        assert rule.enabled == True
        assert rule.auto_remediate == False
        assert rule.remediation_action is None
        assert rule.tags == []


class TestQualityResult:
    """Unit tests for QualityResult class"""
    
    def test_result_creation(self):
        """Test quality result creation"""
        result = QualityResult(
            rule_name='test_rule',
            dimension=QualityDimension.COMPLETENESS,
            severity=RuleSeverity.CRITICAL,
            passed=False,
            score=85.5,
            violation_count=15,
            total_count=100,
            details={'test': 'value'}
        )
        
        assert result.rule_name == 'test_rule'
        assert result.score == 85.5
        assert result.violation_count == 15
        assert result.passed == False
        assert result.timestamp is not None
        
    def test_result_timestamp_default(self):
        """Test that timestamp is auto-generated"""
        result = QualityResult(
            rule_name='test',
            dimension=QualityDimension.VALIDITY,
            severity=RuleSeverity.INFO,
            passed=True,
            score=100.0,
            violation_count=0,
            total_count=100,
            details={}
        )
        
        # Verify timestamp was set and is recent
        timestamp_dt = datetime.fromisoformat(result.timestamp)
        assert (datetime.now() - timestamp_dt).total_seconds() < 5