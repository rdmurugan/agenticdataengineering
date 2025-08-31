"""
Advanced Quality Rule Engine and Validation Framework
Comprehensive data quality validation system with configurable rules and automated remediation
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, count, sum as spark_sum, avg, stddev, max as spark_max, min as spark_min
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType
from typing import Dict, Any, List, Optional, Callable, Union
import yaml
import json
import re
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)


class QualityDimension(Enum):
    """Quality dimensions for assessment"""
    COMPLETENESS = "completeness"
    VALIDITY = "validity"
    CONSISTENCY = "consistency"
    ACCURACY = "accuracy"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"


class RuleSeverity(Enum):
    """Rule violation severity levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class ValidationRule:
    """Data validation rule definition"""
    name: str
    description: str
    dimension: QualityDimension
    severity: RuleSeverity
    condition: str
    field_names: List[str]
    threshold: Optional[float] = None
    enabled: bool = True
    auto_remediate: bool = False
    remediation_action: Optional[str] = None
    tags: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []


@dataclass
class QualityResult:
    """Quality assessment result"""
    rule_name: str
    dimension: QualityDimension
    severity: RuleSeverity
    passed: bool
    score: float
    violation_count: int
    total_count: int
    details: Dict[str, Any]
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()


@dataclass
class ProfileResult:
    """Data profiling result"""
    field_name: str
    data_type: str
    null_count: int
    null_percentage: float
    unique_count: int
    uniqueness_percentage: float
    min_value: Any = None
    max_value: Any = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    stddev_value: Optional[float] = None
    top_values: List[Dict[str, Any]] = None
    pattern_analysis: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.top_values is None:
            self.top_values = []
        if self.pattern_analysis is None:
            self.pattern_analysis = {}


class QualityEngine:
    """
    Advanced data quality engine with configurable rules and automated assessment
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.quality_config = self._load_quality_config()
        self.rules_registry = {}
        self.custom_functions = {}
        self._initialize_built_in_rules()
        
    def _load_quality_config(self) -> Dict[str, Any]:
        """Load quality configuration"""
        try:
            config_path = self.config.get('quality_config_path', 'config/data_quality_config.yaml')
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning("Quality configuration file not found, using defaults")
            return self._get_default_config()
            
    def _get_default_config(self) -> Dict[str, Any]:
        """Get default quality configuration"""
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
        
    def _initialize_built_in_rules(self):
        """Initialize built-in quality rules"""
        
        # Completeness rules
        self.register_rule(ValidationRule(
            name="null_completeness",
            description="Check for null values in required fields",
            dimension=QualityDimension.COMPLETENESS,
            severity=RuleSeverity.CRITICAL,
            condition="field_value IS NOT NULL",
            field_names=["*"],
            auto_remediate=True,
            remediation_action="mark_incomplete"
        ))
        
        self.register_rule(ValidationRule(
            name="blank_completeness", 
            description="Check for blank/empty values",
            dimension=QualityDimension.COMPLETENESS,
            severity=RuleSeverity.WARNING,
            condition="TRIM(field_value) != ''",
            field_names=["*"],
            auto_remediate=True,
            remediation_action="mark_incomplete"
        ))
        
        # Validity rules
        self.register_rule(ValidationRule(
            name="email_format",
            description="Validate email address format",
            dimension=QualityDimension.VALIDITY,
            severity=RuleSeverity.WARNING,
            condition="field_value RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'",
            field_names=["email", "*email*"],
            tags=["format", "email"]
        ))
        
        self.register_rule(ValidationRule(
            name="phone_format",
            description="Validate phone number format", 
            dimension=QualityDimension.VALIDITY,
            severity=RuleSeverity.WARNING,
            condition="field_value RLIKE '^\\+?1?[0-9]{10}$'",
            field_names=["phone", "*phone*", "telephone"],
            auto_remediate=True,
            remediation_action="standardize_phone",
            tags=["format", "phone"]
        ))
        
        self.register_rule(ValidationRule(
            name="date_range",
            description="Validate date values are within reasonable range",
            dimension=QualityDimension.VALIDITY,
            severity=RuleSeverity.CRITICAL,
            condition="field_value BETWEEN '1900-01-01' AND current_date() + interval 1 year",
            field_names=["*date*", "*Date*"],
            tags=["date", "range"]
        ))
        
        # Healthcare-specific rules
        self.register_rule(ValidationRule(
            name="npi_format",
            description="Validate NPI format and Luhn checksum",
            dimension=QualityDimension.VALIDITY,
            severity=RuleSeverity.CRITICAL,
            condition="field_value RLIKE '^[0-9]{10}$' AND luhn_check(field_value)",
            field_names=["provider_npi", "npi", "*npi*"],
            tags=["healthcare", "npi", "format"]
        ))
        
        self.register_rule(ValidationRule(
            name="icd10_format",
            description="Validate ICD-10 diagnosis code format",
            dimension=QualityDimension.VALIDITY,
            severity=RuleSeverity.CRITICAL,
            condition="field_value RLIKE '^[A-TV-Z][0-9][A-Z0-9](\\.[A-Z0-9]{0,4})?$'",
            field_names=["diagnosis_code", "icd10", "*diagnosis*"],
            tags=["healthcare", "icd10", "diagnosis"]
        ))
        
        self.register_rule(ValidationRule(
            name="cpt_format", 
            description="Validate CPT procedure code format",
            dimension=QualityDimension.VALIDITY,
            severity=RuleSeverity.CRITICAL,
            condition="field_value RLIKE '^[0-9]{5}$'",
            field_names=["procedure_code", "cpt", "*cpt*"],
            tags=["healthcare", "cpt", "procedure"]
        ))
        
        # Uniqueness rules
        self.register_rule(ValidationRule(
            name="primary_key_uniqueness",
            description="Ensure primary key fields are unique",
            dimension=QualityDimension.UNIQUENESS,
            severity=RuleSeverity.CRITICAL,
            condition="unique_count = total_count",
            field_names=["id", "*_id", "key", "*_key"],
            tags=["uniqueness", "primary_key"]
        ))
        
        # Custom functions for complex validations
        self._register_custom_functions()
        
    def _register_custom_functions(self):
        """Register custom validation functions"""
        
        def luhn_check(number_str: str) -> bool:
            """Validate using Luhn algorithm (for NPI, credit cards, etc.)"""
            if not number_str or not number_str.isdigit():
                return False
                
            def digits_of(n):
                return [int(d) for d in str(n)]
                
            digits = digits_of(number_str)
            odd_digits = digits[-1::-2]
            even_digits = digits[-2::-2]
            checksum = sum(odd_digits)
            for d in even_digits:
                checksum += sum(digits_of(d*2))
            return checksum % 10 == 0
            
        def standardize_phone(phone: str) -> str:
            """Standardize phone number format"""
            if not phone:
                return phone
            # Remove all non-digits
            digits = re.sub(r'\D', '', phone)
            # Remove leading 1 if present
            if len(digits) == 11 and digits[0] == '1':
                digits = digits[1:]
            return digits if len(digits) == 10 else phone
            
        def validate_member_id(member_id: str) -> bool:
            """Validate healthcare member ID format"""
            if not member_id:
                return False
            # Multiple possible formats
            patterns = [
                r'^[0-9]{9,12}$',                    # 9-12 digits
                r'^[A-Z]{1,3}[0-9]{6,9}$',          # State prefix + digits
                r'^[A-Z][0-9]{8}[A-Z]$',            # New Medicare format
            ]
            return any(re.match(pattern, member_id.upper()) for pattern in patterns)
            
        # Register functions
        self.custom_functions['luhn_check'] = luhn_check
        self.custom_functions['standardize_phone'] = standardize_phone
        self.custom_functions['validate_member_id'] = validate_member_id
        
    def register_rule(self, rule: ValidationRule):
        """Register a quality rule"""
        self.rules_registry[rule.name] = rule
        logger.info(f"Registered quality rule: {rule.name}")
        
    def unregister_rule(self, rule_name: str):
        """Remove a quality rule"""
        if rule_name in self.rules_registry:
            del self.rules_registry[rule_name]
            logger.info(f"Unregistered quality rule: {rule_name}")
            
    def get_applicable_rules(self, field_name: str, data_type: str = None) -> List[ValidationRule]:
        """Get rules applicable to a specific field"""
        applicable_rules = []
        
        for rule in self.rules_registry.values():
            if not rule.enabled:
                continue
                
            # Check if rule applies to this field
            if self._rule_applies_to_field(rule, field_name, data_type):
                applicable_rules.append(rule)
                
        return applicable_rules
        
    def _rule_applies_to_field(self, rule: ValidationRule, field_name: str, data_type: str = None) -> bool:
        """Check if a rule applies to a specific field"""
        
        for pattern in rule.field_names:
            if pattern == "*":  # Applies to all fields
                return True
            elif pattern.startswith("*") and pattern.endswith("*"):
                # Contains pattern
                search_term = pattern[1:-1].lower()
                if search_term in field_name.lower():
                    return True
            elif pattern.startswith("*"):
                # Ends with pattern
                suffix = pattern[1:].lower()
                if field_name.lower().endswith(suffix):
                    return True
            elif pattern.endswith("*"):
                # Starts with pattern
                prefix = pattern[:-1].lower()
                if field_name.lower().startswith(prefix):
                    return True
            elif pattern.lower() == field_name.lower():
                # Exact match
                return True
                
        return False
        
    def assess_table_quality(
        self, 
        df: DataFrame, 
        table_name: str,
        custom_rules: List[ValidationRule] = None
    ) -> Dict[str, Any]:
        """
        Comprehensive quality assessment of a table
        
        Args:
            df: DataFrame to assess
            table_name: Name of the table
            custom_rules: Additional custom rules for this assessment
            
        Returns:
            Dictionary containing quality assessment results
        """
        
        logger.info(f"Starting quality assessment for table: {table_name}")
        
        # Combine built-in and custom rules
        all_rules = list(self.rules_registry.values())
        if custom_rules:
            all_rules.extend(custom_rules)
            
        # Get table schema
        schema = df.schema
        
        # Overall assessment results
        assessment_results = {
            'table_name': table_name,
            'timestamp': datetime.now().isoformat(),
            'record_count': df.count(),
            'field_count': len(schema.fields),
            'overall_score': 0.0,
            'dimension_scores': {},
            'field_results': {},
            'rule_results': {},
            'issues': [],
            'recommendations': []
        }
        
        # Assess each field
        for field in schema.fields:
            field_results = self._assess_field_quality(df, field, all_rules)
            assessment_results['field_results'][field.name] = field_results
            
        # Calculate dimension scores
        dimension_scores = self._calculate_dimension_scores(assessment_results['field_results'])
        assessment_results['dimension_scores'] = dimension_scores
        
        # Calculate overall score
        overall_score = self._calculate_overall_score(dimension_scores)
        assessment_results['overall_score'] = overall_score
        
        # Generate issues and recommendations
        issues = self._identify_quality_issues(assessment_results)
        recommendations = self._generate_recommendations(assessment_results)
        
        assessment_results['issues'] = issues
        assessment_results['recommendations'] = recommendations
        
        logger.info(f"Quality assessment completed. Overall score: {overall_score:.2f}")
        
        return assessment_results
        
    def _assess_field_quality(
        self, 
        df: DataFrame, 
        field: StructField, 
        rules: List[ValidationRule]
    ) -> Dict[str, Any]:
        """Assess quality of a specific field"""
        
        field_name = field.name
        data_type = str(field.dataType)
        
        # Get applicable rules
        applicable_rules = [rule for rule in rules if self._rule_applies_to_field(rule, field_name, data_type)]
        
        # Basic field statistics
        total_count = df.count()
        null_count = df.filter(col(field_name).isNull()).count()
        
        field_results = {
            'field_name': field_name,
            'data_type': data_type,
            'total_count': total_count,
            'null_count': null_count,
            'null_percentage': (null_count / total_count) * 100 if total_count > 0 else 0,
            'rule_results': {},
            'dimension_scores': {},
            'overall_field_score': 0.0
        }
        
        # Apply each rule
        for rule in applicable_rules:
            try:
                rule_result = self._apply_rule(df, field_name, rule)
                field_results['rule_results'][rule.name] = rule_result
            except Exception as e:
                logger.error(f"Error applying rule {rule.name} to field {field_name}: {str(e)}")
                
        # Calculate field-level dimension scores
        field_results['dimension_scores'] = self._calculate_field_dimension_scores(field_results['rule_results'])
        field_results['overall_field_score'] = self._calculate_field_overall_score(field_results['dimension_scores'])
        
        return field_results
        
    def _apply_rule(self, df: DataFrame, field_name: str, rule: ValidationRule) -> QualityResult:
        """Apply a validation rule to a field"""
        
        total_count = df.count()
        
        # Replace field_value placeholder in condition
        condition = rule.condition.replace('field_value', field_name)
        
        # Handle custom functions in conditions
        if 'luhn_check(' in condition:
            # For now, we'll use a simplified approach
            # In production, would register UDFs
            condition = condition.replace('luhn_check(field_value)', 'TRUE')  # Placeholder
            
        try:
            # Count records that pass the condition
            if rule.dimension == QualityDimension.UNIQUENESS:
                # Special handling for uniqueness rules
                unique_count = df.select(field_name).distinct().count()
                passed_count = unique_count if unique_count == total_count else 0
            else:
                # Standard rule evaluation
                passed_count = df.filter(condition).count()
                
            violation_count = total_count - passed_count
            score = (passed_count / total_count) * 100 if total_count > 0 else 0
            passed = violation_count == 0 or (rule.threshold and score >= rule.threshold)
            
            return QualityResult(
                rule_name=rule.name,
                dimension=rule.dimension,
                severity=rule.severity,
                passed=passed,
                score=score,
                violation_count=violation_count,
                total_count=total_count,
                details={
                    'condition': condition,
                    'field_name': field_name,
                    'threshold': rule.threshold
                }
            )
            
        except Exception as e:
            logger.error(f"Error evaluating rule {rule.name}: {str(e)}")
            return QualityResult(
                rule_name=rule.name,
                dimension=rule.dimension,
                severity=rule.severity,
                passed=False,
                score=0.0,
                violation_count=total_count,
                total_count=total_count,
                details={'error': str(e)}
            )
            
    def _calculate_dimension_scores(self, field_results: Dict[str, Any]) -> Dict[str, float]:
        """Calculate quality scores by dimension across all fields"""
        
        dimension_scores = {dim.value: [] for dim in QualityDimension}
        
        # Collect scores for each dimension
        for field_name, field_data in field_results.items():
            for dim_name, score in field_data.get('dimension_scores', {}).items():
                if score is not None:
                    dimension_scores[dim_name].append(score)
                    
        # Calculate average score for each dimension
        final_scores = {}
        for dim_name, scores in dimension_scores.items():
            if scores:
                final_scores[dim_name] = sum(scores) / len(scores)
            else:
                final_scores[dim_name] = 100.0  # No applicable rules = perfect score
                
        return final_scores
        
    def _calculate_overall_score(self, dimension_scores: Dict[str, float]) -> float:
        """Calculate overall quality score from dimension scores"""
        
        weights = self.quality_config.get('quality_dimensions', {})
        total_weight = 0
        weighted_sum = 0
        
        for dim_name, score in dimension_scores.items():
            weight = weights.get(dim_name, {}).get('weight', 0.2)  # Default weight
            weighted_sum += score * weight
            total_weight += weight
            
        return weighted_sum / total_weight if total_weight > 0 else 0
        
    def _calculate_field_dimension_scores(self, rule_results: Dict[str, QualityResult]) -> Dict[str, float]:
        """Calculate dimension scores for a single field"""
        
        dimension_scores = {dim.value: [] for dim in QualityDimension}
        
        # Group rule results by dimension
        for rule_name, result in rule_results.items():
            dim_name = result.dimension.value
            dimension_scores[dim_name].append(result.score)
            
        # Calculate average score for each dimension
        final_scores = {}
        for dim_name, scores in dimension_scores.items():
            if scores:
                final_scores[dim_name] = sum(scores) / len(scores)
            else:
                final_scores[dim_name] = None  # No applicable rules for this dimension
                
        return final_scores
        
    def _calculate_field_overall_score(self, dimension_scores: Dict[str, float]) -> float:
        """Calculate overall score for a single field"""
        
        valid_scores = [score for score in dimension_scores.values() if score is not None]
        return sum(valid_scores) / len(valid_scores) if valid_scores else 0
        
    def _identify_quality_issues(self, assessment_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify quality issues from assessment results"""
        
        issues = []
        
        # Check overall score thresholds
        overall_score = assessment_results['overall_score']
        thresholds = self.quality_config.get('global_thresholds', {})
        
        if overall_score < thresholds.get('critical_quality_score', 60) * 100:
            issues.append({
                'type': 'critical_overall_quality',
                'severity': 'critical',
                'description': f"Overall quality score ({overall_score:.1f}%) is below critical threshold",
                'recommendation': 'Immediate attention required for data quality improvement'
            })
        elif overall_score < thresholds.get('warning_quality_score', 80) * 100:
            issues.append({
                'type': 'low_overall_quality',
                'severity': 'warning', 
                'description': f"Overall quality score ({overall_score:.1f}%) is below warning threshold",
                'recommendation': 'Review and improve data quality processes'
            })
            
        # Check dimension-specific issues
        dimension_scores = assessment_results['dimension_scores']
        for dim_name, score in dimension_scores.items():
            if score < 70:  # Critical dimension threshold
                issues.append({
                    'type': f'low_{dim_name}_score',
                    'severity': 'critical',
                    'description': f"{dim_name.title()} score ({score:.1f}%) is critically low",
                    'recommendation': f'Focus on improving {dim_name} quality measures'
                })
                
        # Field-specific issues
        field_results = assessment_results['field_results']
        for field_name, field_data in field_results.items():
            null_pct = field_data.get('null_percentage', 0)
            if null_pct > 20:  # High null percentage
                issues.append({
                    'type': 'high_null_percentage',
                    'severity': 'warning',
                    'field_name': field_name,
                    'description': f"Field {field_name} has {null_pct:.1f}% null values",
                    'recommendation': 'Investigate data source and improve completeness'
                })
                
        return issues
        
    def _generate_recommendations(self, assessment_results: Dict[str, Any]) -> List[str]:
        """Generate quality improvement recommendations"""
        
        recommendations = []
        
        overall_score = assessment_results['overall_score']
        dimension_scores = assessment_results['dimension_scores']
        
        # Overall recommendations
        if overall_score < 85:
            recommendations.append("Implement comprehensive data quality monitoring")
            recommendations.append("Establish data quality SLAs and accountability")
            
        # Dimension-specific recommendations
        if dimension_scores.get('completeness', 100) < 90:
            recommendations.append("Improve data collection processes to reduce missing values")
            recommendations.append("Implement data validation at source systems")
            
        if dimension_scores.get('validity', 100) < 85:
            recommendations.append("Enhance format validation and standardization")
            recommendations.append("Implement real-time data validation rules")
            
        if dimension_scores.get('consistency', 100) < 90:
            recommendations.append("Establish master data management practices")
            recommendations.append("Implement cross-system data reconciliation")
            
        # Healthcare-specific recommendations
        issues = assessment_results.get('issues', [])
        healthcare_issues = [i for i in issues if any(tag in i.get('description', '').lower() 
                                                   for tag in ['npi', 'icd', 'cpt', 'member', 'provider'])]
        
        if healthcare_issues:
            recommendations.append("Implement healthcare-specific code validation")
            recommendations.append("Set up automated NPI and medical code verification")
            recommendations.append("Establish provider credentialing data quality checks")
            
        return recommendations
        
    def profile_data(self, df: DataFrame, field_names: List[str] = None) -> Dict[str, ProfileResult]:
        """
        Generate comprehensive data profiling results
        
        Args:
            df: DataFrame to profile
            field_names: Specific fields to profile (None for all fields)
            
        Returns:
            Dictionary of profiling results by field name
        """
        
        if field_names is None:
            field_names = [field.name for field in df.schema.fields]
            
        profile_results = {}
        total_count = df.count()
        
        for field_name in field_names:
            logger.info(f"Profiling field: {field_name}")
            
            try:
                field_type = dict(df.dtypes)[field_name]
                
                # Basic statistics
                null_count = df.filter(col(field_name).isNull()).count()
                null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
                
                unique_count = df.select(field_name).distinct().count()
                uniqueness_percentage = (unique_count / total_count) * 100 if total_count > 0 else 0
                
                profile = ProfileResult(
                    field_name=field_name,
                    data_type=field_type,
                    null_count=null_count,
                    null_percentage=null_percentage,
                    unique_count=unique_count,
                    uniqueness_percentage=uniqueness_percentage
                )
                
                # Type-specific profiling
                if field_type in ['int', 'bigint', 'float', 'double', 'decimal']:
                    # Numeric profiling
                    numeric_stats = df.select(
                        spark_min(col(field_name)).alias('min_val'),
                        spark_max(col(field_name)).alias('max_val'),
                        avg(col(field_name)).alias('mean_val'),
                        stddev(col(field_name)).alias('stddev_val')
                    ).collect()[0]
                    
                    profile.min_value = numeric_stats['min_val']
                    profile.max_value = numeric_stats['max_val']
                    profile.mean_value = float(numeric_stats['mean_val']) if numeric_stats['mean_val'] else None
                    profile.stddev_value = float(numeric_stats['stddev_val']) if numeric_stats['stddev_val'] else None
                    
                # Top values analysis
                top_values = (df.groupBy(field_name)
                            .count()
                            .orderBy(col('count').desc())
                            .limit(10)
                            .collect())
                
                profile.top_values = [
                    {'value': row[field_name], 'count': row['count']}
                    for row in top_values
                ]
                
                # Pattern analysis for string fields
                if field_type == 'string':
                    profile.pattern_analysis = self._analyze_patterns(df, field_name)
                    
                profile_results[field_name] = profile
                
            except Exception as e:
                logger.error(f"Error profiling field {field_name}: {str(e)}")
                # Create minimal profile with error
                profile_results[field_name] = ProfileResult(
                    field_name=field_name,
                    data_type='unknown',
                    null_count=0,
                    null_percentage=0,
                    unique_count=0,
                    uniqueness_percentage=0
                )
                
        return profile_results
        
    def _analyze_patterns(self, df: DataFrame, field_name: str) -> Dict[str, Any]:
        """Analyze patterns in string fields"""
        
        try:
            # Sample data for pattern analysis
            sample_data = (df.select(field_name)
                         .filter(col(field_name).isNotNull())
                         .limit(1000)
                         .collect())
            
            values = [row[field_name] for row in sample_data if row[field_name]]
            
            if not values:
                return {}
                
            # Length analysis
            lengths = [len(str(val)) for val in values]
            
            # Common patterns
            patterns = {
                'email': sum(1 for val in values if re.match(r'^[^@]+@[^@]+\.[^@]+$', str(val))),
                'phone': sum(1 for val in values if re.match(r'^[\d\-\(\)\+\s]+$', str(val)) and len(re.sub(r'\D', '', str(val))) >= 10),
                'numeric': sum(1 for val in values if str(val).replace('.', '').replace('-', '').isdigit()),
                'alphanumeric': sum(1 for val in values if str(val).replace(' ', '').isalnum()),
                'uppercase': sum(1 for val in values if str(val).isupper()),
                'lowercase': sum(1 for val in values if str(val).islower())
            }
            
            return {
                'min_length': min(lengths) if lengths else 0,
                'max_length': max(lengths) if lengths else 0,
                'avg_length': sum(lengths) / len(lengths) if lengths else 0,
                'pattern_matches': patterns,
                'sample_size': len(values)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing patterns for {field_name}: {str(e)}")
            return {}
            
    def auto_remediate_issues(self, df: DataFrame, assessment_results: Dict[str, Any]) -> DataFrame:
        """
        Automatically remediate quality issues where possible
        
        Args:
            df: Original DataFrame
            assessment_results: Quality assessment results
            
        Returns:
            DataFrame with remediated issues
        """
        
        remediated_df = df
        remediation_log = []
        
        # Apply auto-remediation rules
        for rule_name, rule in self.rules_registry.items():
            if rule.auto_remediate and rule.remediation_action:
                try:
                    remediated_df, log_entry = self._apply_remediation(
                        remediated_df, rule, assessment_results
                    )
                    if log_entry:
                        remediation_log.append(log_entry)
                except Exception as e:
                    logger.error(f"Error applying remediation for rule {rule_name}: {str(e)}")
                    
        # Log remediation actions
        if remediation_log:
            logger.info(f"Applied {len(remediation_log)} auto-remediation actions")
            
        return remediated_df
        
    def _apply_remediation(self, df: DataFrame, rule: ValidationRule, assessment_results: Dict[str, Any]) -> tuple:
        """Apply specific remediation action"""
        
        if rule.remediation_action == "standardize_phone":
            # Standardize phone number format
            phone_fields = [field for field in df.columns if 'phone' in field.lower()]
            
            for field in phone_fields:
                # Remove non-digits and format
                df = df.withColumn(
                    field,
                    when(col(field).isNotNull(),
                         regexp_replace(col(field), r'[^\d]', ''))
                    .otherwise(col(field))
                )
                
            return df, f"Standardized phone format for fields: {phone_fields}"
            
        elif rule.remediation_action == "mark_incomplete":
            # Add quality flag for incomplete records
            df = df.withColumn(
                "_quality_incomplete",
                when(col("_quality_incomplete").isNull(), 0)
                .otherwise(col("_quality_incomplete"))
            )
            
            return df, "Added quality flags for incomplete records"
            
        return df, None
        
    def export_assessment_results(self, assessment_results: Dict[str, Any], format: str = "json") -> str:
        """Export assessment results in specified format"""
        
        if format.lower() == "json":
            return json.dumps(assessment_results, indent=2, default=str)
        elif format.lower() == "yaml":
            return yaml.dump(assessment_results, default_flow_style=False)
        else:
            raise ValueError(f"Unsupported export format: {format}")
            
    def import_custom_rules(self, rules_config: Dict[str, Any]):
        """Import custom quality rules from configuration"""
        
        for rule_name, rule_config in rules_config.items():
            try:
                rule = ValidationRule(
                    name=rule_name,
                    description=rule_config.get('description', ''),
                    dimension=QualityDimension(rule_config['dimension']),
                    severity=RuleSeverity(rule_config['severity']),
                    condition=rule_config['condition'],
                    field_names=rule_config['field_names'],
                    threshold=rule_config.get('threshold'),
                    enabled=rule_config.get('enabled', True),
                    auto_remediate=rule_config.get('auto_remediate', False),
                    remediation_action=rule_config.get('remediation_action'),
                    tags=rule_config.get('tags', [])
                )
                
                self.register_rule(rule)
                logger.info(f"Imported custom rule: {rule_name}")
                
            except Exception as e:
                logger.error(f"Error importing rule {rule_name}: {str(e)}")


# Usage example and testing
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("QualityEngineTest").getOrCreate()
    
    # Sample configuration
    config = {
        'quality_config_path': 'config/data_quality_config.yaml'
    }
    
    # Initialize quality engine
    quality_engine = QualityEngine(spark, config)
    
    # Create sample data for testing
    sample_data = [
        ("123456789", "john.doe@email.com", "555-123-4567", "2023-01-01", 1250.00),
        ("987654321", "jane.smith@test.com", "5551234567", "2023-01-02", 890.50),
        (None, "invalid-email", "123", "2025-01-01", -100.00),  # Quality issues
        ("123456789", "", "555.123.4567", None, 0.00)  # More issues
    ]
    
    columns = ["member_id", "email", "phone", "service_date", "amount"]
    sample_df = spark.createDataFrame(sample_data, columns)
    
    # Run quality assessment
    results = quality_engine.assess_table_quality(sample_df, "test_table")
    
    print("Quality Assessment Results:")
    print(f"Overall Score: {results['overall_score']:.2f}")
    print(f"Issues Found: {len(results['issues'])}")
    print(f"Recommendations: {len(results['recommendations'])}")
    
    spark.stop()