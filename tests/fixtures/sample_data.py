"""
Test data fixtures for healthcare data validation testing
"""

import pandas as pd
from datetime import datetime, timedelta
import random


def get_valid_claims_data():
    """Generate valid healthcare claims data for testing"""
    return pd.DataFrame([
        {
            'claim_id': 'CLM001234567890',
            'member_id': 'M123456789',
            'provider_npi': '1234567893',  # Valid NPI with Luhn check
            'diagnosis_code': 'Z00.00',    # Annual physical
            'procedure_code': '99213',      # Office visit
            'date_of_service': '2023-06-15',
            'claim_amount': 125.50,
            'place_of_service': '11',      # Office
            'claim_status': 'PAID',
            'created_timestamp': '2023-06-16T10:30:00',
            'created_by': 'system_user'
        },
        {
            'claim_id': 'CLM001234567891',
            'member_id': 'M987654321',
            'provider_npi': '9876543210',  # Valid NPI
            'diagnosis_code': 'I10',       # Hypertension
            'procedure_code': '99214',     # Office visit (higher level)
            'date_of_service': '2023-06-16',
            'claim_amount': 200.00,
            'place_of_service': '11',
            'claim_status': 'PROCESSING',
            'created_timestamp': '2023-06-17T09:15:00',
            'created_by': 'claims_processor'
        },
        {
            'claim_id': 'CLM001234567892',
            'member_id': 'CA123456789',    # California format
            'provider_npi': '1679576722',  # Valid NPI
            'diagnosis_code': 'E11.9',     # Diabetes
            'procedure_code': '80053',     # Basic metabolic panel
            'date_of_service': '2023-06-17',
            'claim_amount': 45.75,
            'place_of_service': '81',      # Lab
            'claim_status': 'PAID',
            'created_timestamp': '2023-06-18T14:22:00',
            'created_by': 'auto_adjudication'
        }
    ])


def get_invalid_claims_data():
    """Generate invalid healthcare claims data for testing validation rules"""
    return pd.DataFrame([
        {
            'claim_id': None,              # Missing required field
            'member_id': 'INVALID_ID',     # Invalid format
            'provider_npi': '1234567890',  # Invalid Luhn checksum
            'diagnosis_code': 'INVALID',   # Invalid ICD-10 format
            'procedure_code': '99999',     # Invalid CPT code
            'date_of_service': '2025-01-01',  # Future date
            'claim_amount': -50.00,        # Negative amount
            'place_of_service': '99',      # Invalid place
            'claim_status': 'UNKNOWN',
            'created_timestamp': None,     # Missing audit field
            'created_by': ''               # Empty audit field
        },
        {
            'claim_id': 'TOO_SHORT',       # Too short
            'member_id': '',               # Empty required field
            'provider_npi': '123',         # Too short
            'diagnosis_code': 'A',         # Too short
            'procedure_code': '123',       # Too short
            'date_of_service': 'invalid-date',  # Invalid format
            'claim_amount': 0.00,          # Zero amount
            'place_of_service': '00',      # Invalid
            'claim_status': '',
            'created_timestamp': '2023-13-45T25:70:99',  # Invalid datetime
            'created_by': None
        }
    ])


def get_valid_member_data():
    """Generate valid member/patient data"""
    return pd.DataFrame([
        {
            'member_id': 'M123456789',
            'first_name': 'John',
            'last_name': 'Doe',
            'date_of_birth': '1985-06-15',
            'gender': 'M',
            'phone': '555-123-4567',
            'email': 'john.doe@email.com',
            'address_line1': '123 Main St',
            'city': 'Anytown',
            'state': 'CA',
            'zip_code': '12345-6789',
            'eligibility_start': '2023-01-01',
            'eligibility_end': '2023-12-31',
            'plan_type': 'MEDICAID'
        },
        {
            'member_id': 'M987654321',
            'first_name': 'Jane',
            'last_name': 'Smith',
            'date_of_birth': '1990-12-03',
            'gender': 'F',
            'phone': '(555) 987-6543',
            'email': 'jane.smith@test.com',
            'address_line1': '456 Oak Ave',
            'city': 'Another City',
            'state': 'NY',
            'zip_code': '54321',
            'eligibility_start': '2023-01-01',
            'eligibility_end': None,  # Ongoing eligibility
            'plan_type': 'MEDICARE'
        }
    ])


def get_invalid_member_data():
    """Generate invalid member data for validation testing"""
    return pd.DataFrame([
        {
            'member_id': None,             # Missing ID
            'first_name': '',             # Empty name
            'last_name': 'X',             # Too short
            'date_of_birth': '2025-01-01',  # Future birth date
            'gender': 'INVALID',          # Invalid gender code
            'phone': '123',               # Invalid phone
            'email': 'invalid-email',     # Invalid email format
            'address_line1': '',          # Empty address
            'city': None,
            'state': 'XX',                # Invalid state
            'zip_code': '123',            # Invalid zip
            'eligibility_start': 'invalid-date',
            'eligibility_end': '2020-01-01',  # End before start
            'plan_type': 'UNKNOWN'
        }
    ])


def get_valid_provider_data():
    """Generate valid provider data"""
    return pd.DataFrame([
        {
            'provider_npi': '1234567893',
            'provider_name': 'Dr. John Smith, MD',
            'provider_type': 'Individual',
            'taxonomy_code': '207Q00000X',  # Family Medicine
            'license_number': 'MD123456',
            'license_state': 'CA',
            'license_expiration_date': '2025-12-31',
            'practice_address': '789 Medical Center Dr',
            'practice_city': 'Healthcare City',
            'practice_state': 'CA',
            'practice_zip': '90210',
            'phone': '555-MEDICAL',
            'status': 'ACTIVE'
        },
        {
            'provider_npi': '9876543210',
            'provider_name': 'General Hospital',
            'provider_type': 'Organization',
            'taxonomy_code': '282N00000X',  # General Acute Care Hospital
            'license_number': 'HOSP789',
            'license_state': 'NY',
            'license_expiration_date': '2024-06-30',
            'practice_address': '100 Hospital Way',
            'practice_city': 'Metro City',
            'practice_state': 'NY',
            'practice_zip': '10001-1234',
            'phone': '555-HOSPITAL',
            'status': 'ACTIVE'
        }
    ])


def get_invalid_provider_data():
    """Generate invalid provider data for validation testing"""
    return pd.DataFrame([
        {
            'provider_npi': '1234567890',  # Invalid Luhn check
            'provider_name': '',          # Empty name
            'provider_type': 'Invalid',   # Invalid type
            'taxonomy_code': 'INVALID',   # Invalid taxonomy
            'license_number': '',         # Empty license
            'license_state': 'XX',        # Invalid state
            'license_expiration_date': '2020-01-01',  # Expired
            'practice_address': None,
            'practice_city': '',
            'practice_state': None,
            'practice_zip': '123',        # Invalid zip
            'phone': '123',              # Invalid phone
            'status': 'UNKNOWN'
        }
    ])


def get_quality_test_scenarios():
    """Get predefined quality test scenarios with expected results"""
    return [
        {
            'name': 'high_quality_scenario',
            'description': 'Data with minimal quality issues',
            'data': get_valid_claims_data(),
            'expected_quality_score': 95.0,
            'expected_issues': 0,
            'expected_alerts': 0
        },
        {
            'name': 'low_quality_scenario', 
            'description': 'Data with multiple quality issues',
            'data': get_invalid_claims_data(),
            'expected_quality_score': 30.0,
            'expected_issues': 10,
            'expected_alerts': 5
        },
        {
            'name': 'mixed_quality_scenario',
            'description': 'Mix of valid and invalid data',
            'data': pd.concat([get_valid_claims_data(), get_invalid_claims_data()]),
            'expected_quality_score': 62.5,
            'expected_issues': 5,
            'expected_alerts': 2
        }
    ]


def get_healthcare_code_test_data():
    """Get test data specifically for healthcare code validation"""
    return {
        'valid_npis': [
            '1234567893',  # Valid Luhn checksum
            '1679576722',  # Valid Luhn checksum 
            '1234567810'   # Valid Luhn checksum
        ],
        'invalid_npis': [
            '1234567890',  # Invalid Luhn checksum
            '1111111111',  # All ones (invalid)
            '123456789',   # Too short
            'ABCD567893',  # Contains letters
            ''             # Empty
        ],
        'valid_icd10_codes': [
            'Z00.00',      # Encounter for general adult medical examination
            'I10',         # Essential hypertension
            'E11.9',       # Type 2 diabetes mellitus without complications
            'J45.909',     # Unspecified asthma, uncomplicated
            'M79.3',       # Panniculitis, unspecified
            'F32.9',       # Major depressive disorder, single episode, unspecified
            'K21.9'        # Gastro-esophageal reflux disease without esophagitis
        ],
        'invalid_icd10_codes': [
            'INVALID',     # Not ICD-10 format
            '123.45',      # Numbers only
            'A',           # Too short
            'Z00.00.00',   # Too many decimal places
            'U99.99',      # U codes reserved for special purposes
            ''             # Empty
        ],
        'valid_cpt_codes': [
            '99213',       # Office visit, established patient
            '99214',       # Office visit, established patient (higher level)
            '80053',       # Comprehensive metabolic panel
            '36415',       # Collection of venous blood by venipuncture
            '85025',       # Blood count; complete (CBC), automated
            '90791',       # Psychiatric diagnostic evaluation
            '96116'        # Neurobehavioral status exam
        ],
        'invalid_cpt_codes': [
            '9921',        # Too short
            '992133',      # Too long  
            'ABCDE',       # Letters
            '00000',       # All zeros (typically invalid)
            '99999',       # Outside normal range
            ''             # Empty
        ]
    }


def generate_large_dataset_sample(num_records=10000):
    """Generate a large sample dataset for performance testing"""
    
    # Base templates for generating varied data
    member_id_templates = ['M{}', 'CA{}', 'NY{}', 'TX{}']
    diagnosis_codes = ['Z00.00', 'I10', 'E11.9', 'J45.909', 'M79.3', 'F32.9']
    procedure_codes = ['99213', '99214', '80053', '36415', '85025', '90791']
    
    data = []
    for i in range(num_records):
        # Generate mostly valid data with some invalid records
        is_valid = random.random() > 0.05  # 95% valid data
        
        if is_valid:
            member_template = random.choice(member_id_templates)
            member_id = member_template.format(random.randint(100000000, 999999999))
            
            record = {
                'claim_id': f'CLM{i:015d}',
                'member_id': member_id,
                'provider_npi': '1234567893',  # Always valid for simplicity
                'diagnosis_code': random.choice(diagnosis_codes),
                'procedure_code': random.choice(procedure_codes),
                'date_of_service': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),
                'claim_amount': round(random.uniform(50.0, 500.0), 2),
                'place_of_service': '11',
                'claim_status': 'PAID'
            }
        else:
            # Generate invalid record
            record = {
                'claim_id': None if random.random() > 0.5 else f'INVALID{i}',
                'member_id': None if random.random() > 0.5 else 'INVALID',
                'provider_npi': '1234567890',  # Invalid Luhn
                'diagnosis_code': 'INVALID',
                'procedure_code': '99999',
                'date_of_service': '2025-01-01',  # Future date
                'claim_amount': -100.0,           # Negative
                'place_of_service': '99',         # Invalid
                'claim_status': 'ERROR'
            }
            
        data.append(record)
    
    return pd.DataFrame(data)


def get_state_specific_test_data():
    """Get state-specific test data for multi-state Medicaid testing"""
    return {
        'california': {
            'member_id_format': r'^CA[0-9]{9}$',
            'sample_ids': ['CA123456789', 'CA987654321', 'CA555666777'],
            'invalid_ids': ['CA12345678', 'CA12345678A', '123456789CA']
        },
        'new_york': {
            'member_id_format': r'^[0-9]{8}NY$', 
            'sample_ids': ['12345678NY', '98765432NY', '11111111NY'],
            'invalid_ids': ['1234567NY', '123456789NY', 'NY12345678']
        },
        'texas': {
            'member_id_format': r'^TX[A-Z][0-9]{8}$',
            'sample_ids': ['TXA12345678', 'TXB98765432', 'TXZ11111111'],
            'invalid_ids': ['TX12345678', 'TXa12345678', 'TXAA12345678']
        }
    }


def get_performance_test_parameters():
    """Get parameters for performance testing"""
    return {
        'small_dataset': 1000,
        'medium_dataset': 10000, 
        'large_dataset': 100000,
        'xlarge_dataset': 1000000,
        'max_processing_time_seconds': {
            'small_dataset': 5,
            'medium_dataset': 30,
            'large_dataset': 300,
            'xlarge_dataset': 1800  # 30 minutes max
        }
    }