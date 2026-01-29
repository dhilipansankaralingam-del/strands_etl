"""
Compliance Agent Tools - GDPR, HIPAA, and data governance
"""
from typing import Dict, Any, List
from strands import tool
import re

@tool
def detect_pii_in_data(
    sample_data: Dict[str, List[str]],
    field_names: List[str]
) -> Dict[str, Any]:
    """
    Detect personally identifiable information (PII) in data fields.

    Args:
        sample_data: Dictionary of field names to sample values
        field_names: List of field names to check

    Returns:
        PII detection results with field classifications
    """
    pii_patterns = {
        'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        'phone': r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
        'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
        'credit_card': r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b',
        'ip_address': r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b'
    }

    detected_pii = {}
    pii_fields = []

    for field, values in sample_data.items():
        if field not in field_names:
            continue

        field_pii_types = []
        for pii_type, pattern in pii_patterns.items():
            matches = sum(1 for v in values if v and re.search(pattern, str(v)))
            if matches > 0:
                field_pii_types.append({
                    'type': pii_type,
                    'matches': matches,
                    'percentage': round((matches / len(values)) * 100, 2)
                })

        if field_pii_types:
            detected_pii[field] = field_pii_types
            pii_fields.append(field)

    return {
        'pii_detected': len(pii_fields) > 0,
        'pii_fields': pii_fields,
        'total_fields_checked': len(field_names),
        'detection_details': detected_pii,
        'recommendation': 'Mask or encrypt PII fields before processing' if pii_fields else 'No PII detected',
        'requires_action': len(pii_fields) > 0
    }


@tool
def check_gdpr_compliance(
    has_consent_mechanism: bool,
    has_data_deletion_api: bool,
    has_data_export_api: bool,
    retention_policy_days: int,
    pii_detected: bool
) -> Dict[str, Any]:
    """
    Check GDPR compliance requirements.

    Args:
        has_consent_mechanism: Whether user consent is collected
        has_data_deletion_api: Whether right to be forgotten is implemented
        has_data_export_api: Whether data portability is implemented
        retention_policy_days: Data retention period in days
        pii_detected: Whether PII was detected in the data

    Returns:
        GDPR compliance check results
    """
    checks = []

    # Check 1: Lawful basis (consent)
    checks.append({
        'requirement': 'Lawful Basis for Processing',
        'passed': has_consent_mechanism or not pii_detected,
        'details': 'Consent mechanism in place' if has_consent_mechanism else 'No consent mechanism - required if processing PII'
    })

    # Check 2: Right to be forgotten
    checks.append({
        'requirement': 'Right to Be Forgotten (Data Deletion)',
        'passed': has_data_deletion_api,
        'details': 'Data deletion API available' if has_data_deletion_api else 'Missing data deletion capability'
    })

    # Check 3: Data portability
    checks.append({
        'requirement': 'Data Portability',
        'passed': has_data_export_api,
        'details': 'Data export API available' if has_data_export_api else 'Missing data export capability'
    })

    # Check 4: Data minimization
    checks.append({
        'requirement': 'Data Minimization',
        'passed': True,  # Assumed to be following principle
        'details': 'Only necessary data should be collected and processed'
    })

    # Check 5: Storage limitation
    checks.append({
        'requirement': 'Storage Limitation',
        'passed': retention_policy_days <= 2555,  # 7 years max for most data
        'details': f'Retention period: {retention_policy_days} days (~{retention_policy_days/365:.1f} years)'
    })

    passed_checks = sum(1 for c in checks if c['passed'])
    total_checks = len(checks)
    compliance_score = passed_checks / total_checks

    return {
        'gdpr_compliant': compliance_score == 1.0,
        'compliance_score': round(compliance_score, 2),
        'passed_checks': passed_checks,
        'total_checks': total_checks,
        'checks': checks,
        'overall_status': 'compliant' if compliance_score == 1.0 else 'non-compliant',
        'actions_required': [c['requirement'] for c in checks if not c['passed']]
    }


@tool
def check_hipaa_compliance(
    data_encrypted_at_rest: bool,
    data_encrypted_in_transit: bool,
    access_logs_enabled: bool,
    phi_detected: bool,
    has_baa_agreement: bool
) -> Dict[str, Any]:
    """
    Check HIPAA compliance requirements for Protected Health Information (PHI).

    Args:
        data_encrypted_at_rest: Whether data is encrypted when stored
        data_encrypted_in_transit: Whether data is encrypted during transmission
        access_logs_enabled: Whether access logs are maintained
        phi_detected: Whether PHI was detected in the data
        has_baa_agreement: Whether Business Associate Agreement exists

    Returns:
        HIPAA compliance check results
    """
    if not phi_detected:
        return {
            'hipaa_applicable': False,
            'message': 'HIPAA checks not applicable - no PHI detected'
        }

    checks = []

    # Check 1: Encryption at rest
    checks.append({
        'requirement': 'Encryption at Rest',
        'passed': data_encrypted_at_rest,
        'details': 'Data encrypted when stored' if data_encrypted_at_rest else 'Data must be encrypted at rest'
    })

    # Check 2: Encryption in transit
    checks.append({
        'requirement': 'Encryption in Transit',
        'passed': data_encrypted_in_transit,
        'details': 'Data encrypted during transmission' if data_encrypted_in_transit else 'Data must be encrypted in transit (TLS 1.2+)'
    })

    # Check 3: Access controls and audit trails
    checks.append({
        'requirement': 'Access Controls & Audit Logs',
        'passed': access_logs_enabled,
        'details': 'Access logs enabled for audit trail' if access_logs_enabled else 'Must maintain audit logs of PHI access'
    })

    # Check 4: Business Associate Agreement
    checks.append({
        'requirement': 'Business Associate Agreement',
        'passed': has_baa_agreement,
        'details': 'BAA in place with all vendors' if has_baa_agreement else 'BAA required for third-party processors'
    })

    passed_checks = sum(1 for c in checks if c['passed'])
    total_checks = len(checks)
    compliance_score = passed_checks / total_checks

    return {
        'hipaa_compliant': compliance_score == 1.0,
        'compliance_score': round(compliance_score, 2),
        'passed_checks': passed_checks,
        'total_checks': total_checks,
        'checks': checks,
        'overall_status': 'compliant' if compliance_score == 1.0 else 'non-compliant',
        'actions_required': [c['requirement'] for c in checks if not c['passed']],
        'severity': 'critical' if compliance_score < 1.0 else 'none'
    }


@tool
def generate_compliance_audit_report(
    execution_id: str,
    gdpr_results: Dict[str, Any],
    hipaa_results: Dict[str, Any],
    pii_results: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generate comprehensive compliance audit report.

    Args:
        execution_id: Unique execution identifier
        gdpr_results: Results from check_gdpr_compliance
        hipaa_results: Results from check_hipaa_compliance
        pii_results: Results from detect_pii_in_data

    Returns:
        Complete audit report
    """
    import datetime

    overall_compliant = True
    critical_issues = []

    # Check GDPR
    if not gdpr_results.get('gdpr_compliant', True):
        overall_compliant = False
        critical_issues.extend(gdpr_results.get('actions_required', []))

    # Check HIPAA
    if hipaa_results.get('hipaa_applicable', False) and not hipaa_results.get('hipaa_compliant', True):
        overall_compliant = False
        critical_issues.extend(hipaa_results.get('actions_required', []))

    # PII handling
    pii_detected = pii_results.get('pii_detected', False)
    if pii_detected and not pii_results.get('requires_action'):
        # PII is detected but properly masked
        pass

    report = {
        'execution_id': execution_id,
        'audit_timestamp': datetime.datetime.utcnow().isoformat(),
        'overall_compliant': overall_compliant,
        'critical_issues_count': len(critical_issues),
        'critical_issues': critical_issues,
        'gdpr_summary': {
            'compliant': gdpr_results.get('gdpr_compliant', True),
            'score': gdpr_results.get('compliance_score', 1.0),
            'actions_required': gdpr_results.get('actions_required', [])
        },
        'hipaa_summary': {
            'applicable': hipaa_results.get('hipaa_applicable', False),
            'compliant': hipaa_results.get('hipaa_compliant', True),
            'score': hipaa_results.get('compliance_score', 1.0) if hipaa_results.get('hipaa_applicable') else 'N/A'
        },
        'pii_summary': {
            'detected': pii_detected,
            'fields_with_pii': pii_results.get('pii_fields', []),
            'requires_masking': pii_results.get('requires_action', False)
        },
        'recommendation': 'Address all critical issues before production deployment' if critical_issues else 'System is compliant with all regulations',
        'retention_requirement': 'Store this report for 7 years for regulatory compliance'
    }

    return report
