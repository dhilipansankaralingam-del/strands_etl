"""
Data Compliance Agent
=====================
Handles data compliance checks, PII detection, and governance.

Features:
- PII detection and classification
- GDPR/HIPAA/PCI-DSS compliance checks
- Data masking and encryption recommendations
- Audit logging
- Data lineage tracking
"""

import json
import re
import logging
import boto3
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PIIType(Enum):
    """Types of PII that can be detected."""
    EMAIL = "email"
    PHONE = "phone"
    SSN = "ssn"
    CREDIT_CARD = "credit_card"
    ADDRESS = "address"
    NAME = "name"
    DOB = "date_of_birth"
    IP_ADDRESS = "ip_address"
    PASSPORT = "passport"
    DRIVER_LICENSE = "driver_license"
    BANK_ACCOUNT = "bank_account"
    MEDICAL_RECORD = "medical_record"


class ComplianceFramework(Enum):
    """Compliance frameworks."""
    GDPR = "gdpr"
    HIPAA = "hipaa"
    PCI_DSS = "pci_dss"
    SOX = "sox"
    CCPA = "ccpa"
    CUSTOM = "custom"


class ComplianceAgent:
    """
    Agent for data compliance checks and governance.
    """

    # Regex patterns for PII detection
    PII_PATTERNS = {
        PIIType.EMAIL: r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        PIIType.PHONE: r'\b(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}\b',
        PIIType.SSN: r'\b(?!000|666|9\d{2})\d{3}[-\s]?(?!00)\d{2}[-\s]?(?!0000)\d{4}\b',
        PIIType.CREDIT_CARD: r'\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\b',
        PIIType.IP_ADDRESS: r'\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b',
        PIIType.DOB: r'\b(?:0[1-9]|1[0-2])[-/](?:0[1-9]|[12][0-9]|3[01])[-/](?:19|20)\d{2}\b',
    }

    # Column name patterns that suggest PII
    PII_COLUMN_PATTERNS = {
        PIIType.EMAIL: ['email', 'e_mail', 'email_address', 'mail'],
        PIIType.PHONE: ['phone', 'telephone', 'mobile', 'cell', 'contact_number'],
        PIIType.SSN: ['ssn', 'social_security', 'social_sec', 'ss_number'],
        PIIType.CREDIT_CARD: ['credit_card', 'card_number', 'cc_number', 'payment_card'],
        PIIType.ADDRESS: ['address', 'street', 'city', 'zip', 'postal', 'zip_code'],
        PIIType.NAME: ['name', 'first_name', 'last_name', 'full_name', 'customer_name'],
        PIIType.DOB: ['dob', 'birth_date', 'date_of_birth', 'birthday'],
        PIIType.IP_ADDRESS: ['ip', 'ip_address', 'client_ip', 'source_ip'],
        PIIType.BANK_ACCOUNT: ['bank_account', 'account_number', 'routing_number'],
        PIIType.MEDICAL_RECORD: ['medical_id', 'patient_id', 'health_record', 'diagnosis']
    }

    # Compliance requirements by framework
    FRAMEWORK_REQUIREMENTS = {
        ComplianceFramework.GDPR: {
            'pii_types': [PIIType.EMAIL, PIIType.NAME, PIIType.ADDRESS, PIIType.PHONE, PIIType.DOB, PIIType.IP_ADDRESS],
            'required_actions': ['consent_tracking', 'right_to_deletion', 'data_portability', 'breach_notification'],
            'retention_required': True,
            'encryption_required': True
        },
        ComplianceFramework.HIPAA: {
            'pii_types': [PIIType.SSN, PIIType.NAME, PIIType.DOB, PIIType.MEDICAL_RECORD],
            'required_actions': ['access_controls', 'audit_logging', 'encryption', 'minimum_necessary'],
            'retention_required': True,
            'encryption_required': True
        },
        ComplianceFramework.PCI_DSS: {
            'pii_types': [PIIType.CREDIT_CARD, PIIType.BANK_ACCOUNT],
            'required_actions': ['encryption', 'access_controls', 'vulnerability_scanning', 'logging'],
            'retention_required': True,
            'encryption_required': True
        },
        ComplianceFramework.SOX: {
            'pii_types': [],
            'required_actions': ['audit_trail', 'access_controls', 'data_integrity'],
            'retention_required': True,
            'encryption_required': False
        },
        ComplianceFramework.CCPA: {
            'pii_types': [PIIType.EMAIL, PIIType.NAME, PIIType.ADDRESS, PIIType.PHONE],
            'required_actions': ['opt_out', 'disclosure', 'deletion_rights'],
            'retention_required': True,
            'encryption_required': False
        }
    }

    def __init__(self, config: Dict[str, Any], region: str = None):
        """
        Initialize Compliance Agent.

        Args:
            config: Pipeline configuration with compliance settings
            region: AWS region
        """
        self.config = config
        self.region = region
        self.compliance_config = config.get('compliance', {})

        client_kwargs = {'region_name': region} if region else {}
        self.bedrock = boto3.client('bedrock-runtime', **client_kwargs)
        self.glue = boto3.client('glue', **client_kwargs)

        self.findings = []
        self.audit_log = []

    def is_enabled(self) -> bool:
        """Check if compliance checking is enabled."""
        return self.compliance_config.get('enabled', False)

    def get_active_frameworks(self) -> List[ComplianceFramework]:
        """Get the list of active compliance frameworks."""
        framework_names = self.compliance_config.get('frameworks', [])
        return [ComplianceFramework(f) for f in framework_names if f in [e.value for e in ComplianceFramework]]

    def detect_pii_in_column_name(self, column_name: str) -> List[PIIType]:
        """
        Detect potential PII based on column name.

        Args:
            column_name: Name of the column to check

        Returns:
            List of potential PII types
        """
        detected = []
        column_lower = column_name.lower()

        for pii_type, patterns in self.PII_COLUMN_PATTERNS.items():
            for pattern in patterns:
                if pattern in column_lower:
                    detected.append(pii_type)
                    break

        return detected

    def detect_pii_in_value(self, value: str) -> List[Tuple[PIIType, str]]:
        """
        Detect PII in a string value using regex patterns.

        Args:
            value: String value to scan

        Returns:
            List of (PIIType, matched_value) tuples
        """
        if not isinstance(value, str):
            return []

        detected = []
        for pii_type, pattern in self.PII_PATTERNS.items():
            matches = re.findall(pattern, value)
            for match in matches:
                detected.append((pii_type, match))

        return detected

    def scan_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Scan a table schema for potential PII columns.

        Args:
            schema: Table schema with column definitions

        Returns:
            Scan results with PII findings
        """
        results = {
            'scanned_at': datetime.utcnow().isoformat(),
            'total_columns': 0,
            'pii_columns': [],
            'risk_level': 'low'
        }

        columns = schema.get('columns', [])
        results['total_columns'] = len(columns)

        for column in columns:
            column_name = column.get('name', column.get('Name', ''))
            detected_pii = self.detect_pii_in_column_name(column_name)

            if detected_pii:
                results['pii_columns'].append({
                    'column_name': column_name,
                    'detected_pii_types': [p.value for p in detected_pii],
                    'data_type': column.get('type', column.get('Type', 'unknown'))
                })

        # Determine risk level
        pii_count = len(results['pii_columns'])
        if pii_count == 0:
            results['risk_level'] = 'low'
        elif pii_count <= 3:
            results['risk_level'] = 'medium'
        else:
            results['risk_level'] = 'high'

        return results

    def scan_table(self, database: str, table: str) -> Dict[str, Any]:
        """
        Scan a Glue table for PII.

        Args:
            database: Glue database name
            table: Table name

        Returns:
            Comprehensive scan results
        """
        results = {
            'database': database,
            'table': table,
            'scanned_at': datetime.utcnow().isoformat(),
            'schema_scan': None,
            'sample_scan': None,
            'compliance_status': {},
            'recommendations': []
        }

        try:
            # Get table schema
            table_response = self.glue.get_table(DatabaseName=database, Name=table)
            table_def = table_response['Table']
            columns = table_def['StorageDescriptor']['Columns']

            # Scan schema
            schema = {'columns': columns}
            results['schema_scan'] = self.scan_schema(schema)

            # Check compliance status for each framework
            for framework in self.get_active_frameworks():
                results['compliance_status'][framework.value] = self.check_framework_compliance(
                    framework,
                    results['schema_scan']
                )

            # Generate recommendations
            results['recommendations'] = self.generate_recommendations(results)

            # Log finding
            self.findings.append(results)
            self._audit_log('table_scan', {'database': database, 'table': table, 'risk_level': results['schema_scan']['risk_level']})

        except Exception as e:
            logger.error(f"Failed to scan table {database}.{table}: {e}")
            results['error'] = str(e)

        return results

    def check_framework_compliance(
        self,
        framework: ComplianceFramework,
        schema_scan: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Check compliance against a specific framework.

        Args:
            framework: The compliance framework to check
            schema_scan: Results from schema scanning

        Returns:
            Compliance check results
        """
        requirements = self.FRAMEWORK_REQUIREMENTS.get(framework, {})
        required_pii_types = requirements.get('pii_types', [])

        result = {
            'framework': framework.value,
            'compliant': True,
            'violations': [],
            'warnings': [],
            'required_actions': requirements.get('required_actions', [])
        }

        # Check for PII that requires protection
        pii_columns = schema_scan.get('pii_columns', [])
        for pii_col in pii_columns:
            for pii_type_str in pii_col['detected_pii_types']:
                try:
                    pii_type = PIIType(pii_type_str)
                    if pii_type in required_pii_types:
                        result['violations'].append({
                            'column': pii_col['column_name'],
                            'pii_type': pii_type_str,
                            'message': f"Column contains {pii_type_str} which requires protection under {framework.value}"
                        })
                        result['compliant'] = False
                except ValueError:
                    pass

        # Check encryption requirement
        if requirements.get('encryption_required') and pii_columns:
            pii_config = self.compliance_config.get('pii_detection', {})
            if pii_config.get('action') not in ['encrypt', 'hash']:
                result['warnings'].append({
                    'type': 'encryption',
                    'message': f"{framework.value} requires encryption for PII data"
                })

        return result

    def generate_recommendations(self, scan_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Generate compliance recommendations based on scan results.

        Args:
            scan_results: Results from table scanning

        Returns:
            List of recommendations
        """
        recommendations = []
        schema_scan = scan_results.get('schema_scan', {})
        pii_columns = schema_scan.get('pii_columns', [])

        if pii_columns:
            # Recommend masking/encryption
            recommendations.append({
                'priority': 'high',
                'type': 'data_protection',
                'title': 'Implement Data Masking',
                'description': f"Found {len(pii_columns)} columns with potential PII. Consider implementing data masking or encryption.",
                'columns': [c['column_name'] for c in pii_columns],
                'implementation': {
                    'pyspark': self._generate_masking_code(pii_columns)
                }
            })

            # Recommend access controls
            recommendations.append({
                'priority': 'high',
                'type': 'access_control',
                'title': 'Implement Column-Level Security',
                'description': "Restrict access to PII columns using Lake Formation or Glue Data Catalog permissions.",
                'aws_services': ['AWS Lake Formation', 'AWS Glue Data Catalog']
            })

        # Check compliance status for additional recommendations
        for framework, status in scan_results.get('compliance_status', {}).items():
            if not status.get('compliant', True):
                recommendations.append({
                    'priority': 'critical',
                    'type': 'compliance_violation',
                    'title': f'{framework.upper()} Compliance Required',
                    'description': f"Violations detected for {framework} compliance",
                    'violations': status.get('violations', []),
                    'required_actions': status.get('required_actions', [])
                })

        return recommendations

    def _generate_masking_code(self, pii_columns: List[Dict]) -> str:
        """Generate PySpark code for data masking."""
        code_lines = [
            "# PII Masking Implementation",
            "from pyspark.sql import functions as F",
            ""
        ]

        for col in pii_columns:
            col_name = col['column_name']
            pii_types = col['detected_pii_types']

            if 'email' in pii_types:
                code_lines.append(f"# Mask email: {col_name}")
                code_lines.append(f"df = df.withColumn('{col_name}', F.regexp_replace(F.col('{col_name}'), r'(.).*@', r'$1***@'))")
            elif 'phone' in pii_types:
                code_lines.append(f"# Mask phone: {col_name}")
                code_lines.append(f"df = df.withColumn('{col_name}', F.regexp_replace(F.col('{col_name}'), r'\\d{{4}}$', 'XXXX'))")
            elif 'ssn' in pii_types:
                code_lines.append(f"# Mask SSN: {col_name}")
                code_lines.append(f"df = df.withColumn('{col_name}', F.lit('XXX-XX-XXXX'))")
            elif 'credit_card' in pii_types:
                code_lines.append(f"# Mask credit card: {col_name}")
                code_lines.append(f"df = df.withColumn('{col_name}', F.regexp_replace(F.col('{col_name}'), r'\\d{{12}}(\\d{{4}})', r'************$1'))")
            else:
                code_lines.append(f"# Hash column: {col_name}")
                code_lines.append(f"df = df.withColumn('{col_name}', F.sha2(F.col('{col_name}').cast('string'), 256))")

            code_lines.append("")

        return "\n".join(code_lines)

    def _audit_log(self, action: str, details: Dict[str, Any]) -> None:
        """Add entry to audit log."""
        self.audit_log.append({
            'timestamp': datetime.utcnow().isoformat(),
            'action': action,
            'details': details
        })

    def run_compliance_check(self, data_sources: List[Dict], target: Dict) -> Dict[str, Any]:
        """
        Run compliance checks on all data sources and target.

        Args:
            data_sources: List of data source configurations
            target: Target configuration

        Returns:
            Comprehensive compliance report
        """
        if not self.is_enabled():
            return {'enabled': False, 'message': 'Compliance checking is disabled'}

        report = {
            'checked_at': datetime.utcnow().isoformat(),
            'frameworks': [f.value for f in self.get_active_frameworks()],
            'source_scans': [],
            'target_scan': None,
            'overall_status': 'compliant',
            'risk_level': 'low',
            'total_violations': 0,
            'recommendations': [],
            'audit_log': []
        }

        # Scan data sources
        for source in data_sources:
            if source.get('type') in ['glue_catalog', 'glue']:
                database = source.get('database')
                table = source.get('table')
                if database and table:
                    scan_result = self.scan_table(database, table)
                    report['source_scans'].append(scan_result)

        # Scan target
        if target.get('type') in ['glue_catalog', 'glue']:
            database = target.get('database')
            table = target.get('table')
            if database and table:
                try:
                    report['target_scan'] = self.scan_table(database, table)
                except:
                    report['target_scan'] = {'note': 'Target table may not exist yet'}

        # Aggregate results
        all_scans = report['source_scans'] + ([report['target_scan']] if report['target_scan'] and 'error' not in report['target_scan'] else [])

        for scan in all_scans:
            if not scan:
                continue

            # Collect violations
            for framework, status in scan.get('compliance_status', {}).items():
                if not status.get('compliant', True):
                    report['overall_status'] = 'non_compliant'
                    report['total_violations'] += len(status.get('violations', []))

            # Update risk level
            scan_risk = scan.get('schema_scan', {}).get('risk_level', 'low')
            if scan_risk == 'high':
                report['risk_level'] = 'high'
            elif scan_risk == 'medium' and report['risk_level'] != 'high':
                report['risk_level'] = 'medium'

            # Collect recommendations
            report['recommendations'].extend(scan.get('recommendations', []))

        report['audit_log'] = self.audit_log

        return report

    def get_compliance_summary(self) -> Dict[str, Any]:
        """Get a summary of all compliance findings."""
        return {
            'total_scans': len(self.findings),
            'findings': self.findings,
            'audit_log': self.audit_log
        }
