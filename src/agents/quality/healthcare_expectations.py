"""
Healthcare-specific data expectations and validations
Specialized rules for Medicaid/Medicare data quality
"""

from typing import Dict, Any, List, Optional, Callable
import re
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class HealthcareExpectations:
    """
    Healthcare-specific data quality expectations and validation rules
    Focuses on Medicaid/Medicare compliance requirements
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.validation_rules = self._initialize_validation_rules()
        
    def _initialize_validation_rules(self) -> Dict[str, Callable]:
        """Initialize healthcare validation rules"""
        
        return {
            "member_id": self.validate_member_id,
            "npi": self.validate_npi,
            "icd10_diagnosis": self.validate_icd10_diagnosis,
            "cpt_procedure": self.validate_cpt_procedure,
            "hcpcs_procedure": self.validate_hcpcs_procedure,
            "date_of_service": self.validate_service_date,
            "claim_amount": self.validate_claim_amount,
            "provider_taxonomy": self.validate_provider_taxonomy,
            "place_of_service": self.validate_place_of_service,
            "admission_date": self.validate_admission_date,
            "discharge_date": self.validate_discharge_date
        }
        
    def validate_member_id(self, value: str) -> Dict[str, Any]:
        """
        Validate Medicaid/Medicare member ID formats
        
        Common formats:
        - Medicaid: 9-12 digits or state prefix + digits
        - Medicare: 9-12 characters with specific patterns
        """
        
        if not value or not isinstance(value, str):
            return {"valid": False, "error": "Member ID is required and must be a string"}
            
        # Remove spaces and convert to uppercase
        clean_value = value.strip().upper()
        
        # Medicaid patterns (varies by state)
        medicaid_patterns = [
            r'^\d{9,12}$',                    # 9-12 digits
            r'^[A-Z]{1,3}\d{6,9}$',          # State prefix + digits
            r'^[A-Z]{2}\d{8}[A-Z]$',         # State + 8 digits + letter
        ]
        
        # Medicare patterns
        medicare_patterns = [
            r'^\d{9}[A-Z]?\d?[A-Z]?$',       # SSN-based format
            r'^[A-Z]\d{8}[A-Z]$',            # New Medicare format
        ]
        
        all_patterns = medicaid_patterns + medicare_patterns
        
        for pattern in all_patterns:
            if re.match(pattern, clean_value):
                return {"valid": True, "format": "member_id", "clean_value": clean_value}
                
        return {
            "valid": False, 
            "error": f"Invalid member ID format: {value}",
            "expected_formats": "9-12 digits, state prefix + digits, or Medicare format"
        }
        
    def validate_npi(self, value: str) -> Dict[str, Any]:
        """
        Validate National Provider Identifier (NPI)
        NPI is exactly 10 digits with Luhn algorithm checksum
        """
        
        if not value or not isinstance(value, str):
            return {"valid": False, "error": "NPI is required and must be a string"}
            
        # Remove spaces and non-digits
        clean_value = re.sub(r'\D', '', value.strip())
        
        # Must be exactly 10 digits
        if len(clean_value) != 10:
            return {"valid": False, "error": f"NPI must be exactly 10 digits, got {len(clean_value)}"}
            
        # Cannot be all zeros or nines
        if clean_value in ['0000000000', '9999999999']:
            return {"valid": False, "error": "NPI cannot be all zeros or nines"}
            
        # Validate Luhn algorithm checksum
        if not self._validate_luhn(clean_value):
            return {"valid": False, "error": "NPI fails Luhn algorithm checksum"}
            
        return {"valid": True, "format": "npi", "clean_value": clean_value}
        
    def _validate_luhn(self, number: str) -> bool:
        """Validate Luhn algorithm checksum"""
        
        def luhn_checksum(card_num):
            def digits_of(n):
                return [int(d) for d in str(n)]
            digits = digits_of(card_num)
            odd_digits = digits[-1::-2]
            even_digits = digits[-2::-2]
            checksum = sum(odd_digits)
            for d in even_digits:
                checksum += sum(digits_of(d*2))
            return checksum % 10
        
        return luhn_checksum(number) == 0
        
    def validate_icd10_diagnosis(self, value: str) -> Dict[str, Any]:
        """
        Validate ICD-10 diagnosis codes
        Format: Letter + 2 digits + optional decimal + up to 4 alphanumeric
        """
        
        if not value or not isinstance(value, str):
            return {"valid": False, "error": "ICD-10 diagnosis code is required"}
            
        clean_value = value.strip().upper()
        
        # ICD-10-CM pattern: A00-Z99 with optional subcategories
        icd10_pattern = r'^[A-TV-Z][0-9][A-Z0-9](\.[A-Z0-9]{1,4})?$'
        
        if re.match(icd10_pattern, clean_value):
            return {"valid": True, "format": "icd10", "clean_value": clean_value}
            
        # Also check for ICD-9 format (legacy support)
        icd9_pattern = r'^[0-9]{3}(\.[0-9]{1,2})?$'
        if re.match(icd9_pattern, clean_value):
            return {"valid": True, "format": "icd9", "clean_value": clean_value, 
                   "warning": "ICD-9 format detected, consider updating to ICD-10"}
            
        return {
            "valid": False,
            "error": f"Invalid diagnosis code format: {value}",
            "expected_format": "ICD-10: A00.0 - Z99.9 or ICD-9: 000.0 - 999.99"
        }
        
    def validate_cpt_procedure(self, value: str) -> Dict[str, Any]:
        """
        Validate CPT (Current Procedural Terminology) codes
        Format: 5 digits (00100-99999)
        """
        
        if not value or not isinstance(value, str):
            return {"valid": False, "error": "CPT procedure code is required"}
            
        clean_value = re.sub(r'\D', '', value.strip())
        
        if len(clean_value) != 5:
            return {"valid": False, "error": f"CPT code must be exactly 5 digits, got {len(clean_value)}"}
            
        code_num = int(clean_value)
        if not (100 <= code_num <= 99999):
            return {"valid": False, "error": f"CPT code must be between 00100-99999, got {clean_value}"}
            
        return {"valid": True, "format": "cpt", "clean_value": clean_value}
        
    def validate_hcpcs_procedure(self, value: str) -> Dict[str, Any]:
        """
        Validate HCPCS Level II codes
        Format: Letter + 4 digits (A0000-V9999, excluding I, O)
        """
        
        if not value or not isinstance(value, str):
            return {"valid": False, "error": "HCPCS procedure code is required"}
            
        clean_value = value.strip().upper()
        
        # HCPCS Level II pattern
        hcpcs_pattern = r'^[A-HJKLMNP-V][0-9]{4}$'
        
        if re.match(hcpcs_pattern, clean_value):
            return {"valid": True, "format": "hcpcs", "clean_value": clean_value}
            
        return {
            "valid": False,
            "error": f"Invalid HCPCS code format: {value}",
            "expected_format": "Letter (A-V, excluding I,O) + 4 digits (e.g., A0100)"
        }
        
    def validate_service_date(self, value: str) -> Dict[str, Any]:
        """
        Validate date of service
        Must be a valid date, not in the future, not too old
        """
        
        if not value:
            return {"valid": False, "error": "Date of service is required"}
            
        try:
            if isinstance(value, str):
                # Try different date formats
                date_formats = ['%Y-%m-%d', '%m/%d/%Y', '%Y%m%d']
                service_date = None
                
                for fmt in date_formats:
                    try:
                        service_date = datetime.strptime(value.strip(), fmt)
                        break
                    except ValueError:
                        continue
                        
                if not service_date:
                    return {"valid": False, "error": f"Invalid date format: {value}"}
            else:
                service_date = value
                
            # Check date range
            today = datetime.now()
            min_date = datetime(2020, 1, 1)  # Reasonable minimum for healthcare data
            
            if service_date > today:
                return {"valid": False, "error": "Date of service cannot be in the future"}
                
            if service_date < min_date:
                return {"valid": False, "error": f"Date of service too old: {service_date.date()}"}
                
            return {"valid": True, "format": "date", "clean_value": service_date.strftime('%Y-%m-%d')}
            
        except Exception as e:
            return {"valid": False, "error": f"Invalid date: {str(e)}"}
            
    def validate_claim_amount(self, value: Any) -> Dict[str, Any]:
        """
        Validate claim amounts
        Must be numeric, non-negative, within reasonable range
        """
        
        if value is None:
            return {"valid": False, "error": "Claim amount is required"}
            
        try:
            if isinstance(value, str):
                # Remove currency symbols and whitespace
                clean_value = re.sub(r'[$,\s]', '', value.strip())
                amount = float(clean_value)
            else:
                amount = float(value)
                
            # Validation rules
            if amount < 0:
                return {"valid": False, "error": "Claim amount cannot be negative"}
                
            if amount == 0:
                return {"valid": True, "format": "currency", "clean_value": 0.00, 
                       "warning": "Zero claim amount detected"}
                
            if amount > 1000000:  # $1M limit
                return {"valid": False, "error": f"Claim amount too high: ${amount:,.2f}"}
                
            return {"valid": True, "format": "currency", "clean_value": round(amount, 2)}
            
        except (ValueError, TypeError):
            return {"valid": False, "error": f"Invalid claim amount: {value}"}
            
    def validate_provider_taxonomy(self, value: str) -> Dict[str, Any]:
        """
        Validate provider taxonomy codes
        Format: 10-character alphanumeric code
        """
        
        if not value or not isinstance(value, str):
            return {"valid": False, "error": "Provider taxonomy code is required"}
            
        clean_value = value.strip()
        
        # Taxonomy code pattern: 10 characters, letters and numbers only
        if len(clean_value) != 10:
            return {"valid": False, "error": f"Taxonomy code must be 10 characters, got {len(clean_value)}"}
            
        if not re.match(r'^[A-Z0-9]{10}$', clean_value.upper()):
            return {"valid": False, "error": "Taxonomy code must contain only letters and numbers"}
            
        return {"valid": True, "format": "taxonomy", "clean_value": clean_value.upper()}
        
    def validate_place_of_service(self, value: str) -> Dict[str, Any]:
        """
        Validate Place of Service codes
        2-digit numeric codes (01-99)
        """
        
        if not value:
            return {"valid": False, "error": "Place of service code is required"}
            
        clean_value = str(value).strip().zfill(2)
        
        try:
            pos_code = int(clean_value)
            if not (1 <= pos_code <= 99):
                return {"valid": False, "error": f"Place of service must be 01-99, got {clean_value}"}
                
            return {"valid": True, "format": "pos", "clean_value": clean_value}
            
        except ValueError:
            return {"valid": False, "error": f"Invalid place of service code: {value}"}
            
    def validate_admission_date(self, value: str, service_date: Optional[str] = None) -> Dict[str, Any]:
        """Validate admission date (for inpatient claims)"""
        
        date_result = self.validate_service_date(value)
        if not date_result["valid"]:
            return date_result
            
        # Additional check: admission should be before or same as service date
        if service_date:
            try:
                admission_dt = datetime.strptime(date_result["clean_value"], '%Y-%m-%d')
                service_dt = datetime.strptime(service_date, '%Y-%m-%d')
                
                if admission_dt > service_dt:
                    return {"valid": False, "error": "Admission date cannot be after service date"}
                    
            except ValueError:
                pass  # If service date is invalid, let it be caught elsewhere
                
        return {"valid": True, "format": "date", "clean_value": date_result["clean_value"]}
        
    def validate_discharge_date(self, value: str, admission_date: Optional[str] = None) -> Dict[str, Any]:
        """Validate discharge date (for inpatient claims)"""
        
        date_result = self.validate_service_date(value)
        if not date_result["valid"]:
            return date_result
            
        # Additional check: discharge should be after admission
        if admission_date:
            try:
                discharge_dt = datetime.strptime(date_result["clean_value"], '%Y-%m-%d')
                admission_dt = datetime.strptime(admission_date, '%Y-%m-%d')
                
                if discharge_dt < admission_dt:
                    return {"valid": False, "error": "Discharge date cannot be before admission date"}
                    
                # Check for reasonable length of stay (e.g., < 365 days)
                los_days = (discharge_dt - admission_dt).days
                if los_days > 365:
                    return {"valid": True, "format": "date", "clean_value": date_result["clean_value"],
                           "warning": f"Long length of stay: {los_days} days"}
                    
            except ValueError:
                pass
                
        return {"valid": True, "format": "date", "clean_value": date_result["clean_value"]}
        
    def validate_record(self, record: Dict[str, Any], validation_schema: Dict[str, str]) -> Dict[str, Any]:
        """
        Validate a complete healthcare record against a schema
        
        Args:
            record: Data record to validate
            validation_schema: Maps field names to validation rule names
        """
        
        validation_results = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "field_results": {}
        }
        
        for field_name, rule_name in validation_schema.items():
            if rule_name not in self.validation_rules:
                validation_results["errors"].append(f"Unknown validation rule: {rule_name}")
                continue
                
            field_value = record.get(field_name)
            validator = self.validation_rules[rule_name]
            
            try:
                # Special handling for date cross-validation
                if rule_name == "admission_date" and "date_of_service" in record:
                    result = validator(field_value, record["date_of_service"])
                elif rule_name == "discharge_date" and "admission_date" in record:
                    result = validator(field_value, record["admission_date"])
                else:
                    result = validator(field_value)
                    
                validation_results["field_results"][field_name] = result
                
                if not result["valid"]:
                    validation_results["valid"] = False
                    validation_results["errors"].append(f"{field_name}: {result['error']}")
                    
                if "warning" in result:
                    validation_results["warnings"].append(f"{field_name}: {result['warning']}")
                    
            except Exception as e:
                validation_results["valid"] = False
                validation_results["errors"].append(f"{field_name}: Validation error - {str(e)}")
                
        return validation_results
        
    def get_default_medicaid_schema(self) -> Dict[str, str]:
        """Get default validation schema for Medicaid claims"""
        
        return {
            "member_id": "member_id",
            "provider_npi": "npi", 
            "diagnosis_code": "icd10_diagnosis",
            "procedure_code": "cpt_procedure",
            "date_of_service": "date_of_service",
            "claim_amount": "claim_amount",
            "place_of_service": "place_of_service"
        }
        
    def get_default_medicare_schema(self) -> Dict[str, str]:
        """Get default validation schema for Medicare claims"""
        
        return {
            "member_id": "member_id",
            "provider_npi": "npi",
            "diagnosis_code": "icd10_diagnosis", 
            "procedure_code": "cpt_procedure",
            "date_of_service": "date_of_service",
            "claim_amount": "claim_amount",
            "provider_taxonomy": "provider_taxonomy",
            "place_of_service": "place_of_service"
        }