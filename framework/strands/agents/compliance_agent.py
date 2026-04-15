#!/usr/bin/env python3
"""
Strands Compliance Agent
========================

Checks data compliance with regulatory frameworks (GDPR, PCI-DSS, SOX, HIPAA).

Configuration Structure:
------------------------
"compliance": {
    "enabled": "Y",
    "frameworks": ["GDPR", "PCI_DSS", "SOX", "HIPAA"],

    "table_scans": [
        {
            "table": "customer_master.customer_profile",
            "columns": ["customer_name", "email", "phone", "ssn"],
            "pii_check": true,
            "masking_required": true
        },
        {
            "table": "finance_accounting_master.transactions",
            "columns": ["credit_card_number", "account_number"],
            "pii_check": true,
            "frameworks": ["PCI_DSS"]
        }
    ],

    "global_pii_columns": [
        "customer_name", "customer_email", "phone", "address",
        "ssn", "driver_license", "date_of_birth", "credit_card_number"
    ],

    "scan_all_tables": "Y",
    "check_sources": "Y",
    "check_targets": "Y",
    "mask_pii": "Y",
    "mask_strategy": "hash",
    "audit_data_access": "Y",
    "retention_days": 365
}
"""

import re
from typing import Dict, List, Any, Optional, Set
from datetime import datetime
from ..base_agent import StrandsAgent, AgentResult, AgentStatus, AgentContext, register_agent
from ..tools import tool
from ..storage import StrandsStorage

# Unified audit logger (optional)
try:
    from ..unified_audit import get_audit_logger
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False
    get_audit_logger = None


@register_agent
class StrandsComplianceAgent(StrandsAgent):
    """
    Checks data compliance with regulatory frameworks.

    Scans tables for PII columns and validates against compliance frameworks:
    - GDPR: Personal data protection
    - PCI-DSS: Payment card data
    - SOX: Financial data integrity
    - HIPAA: Healthcare data protection
    """

    AGENT_NAME = "compliance_agent"
    AGENT_VERSION = "2.1.0"
    AGENT_DESCRIPTION = "Validates compliance with GDPR, PCI-DSS, SOX, HIPAA across configured tables"

    DEPENDENCIES = []
    PARALLEL_SAFE = True

    # PII detection patterns
    PII_PATTERNS = {
        'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
        'email': r'\b[\w.-]+@[\w.-]+\.\w+\b',
        'phone': r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
        'credit_card': r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b',
        'ip_address': r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b',
        'date_of_birth': r'\b\d{4}[-/]\d{2}[-/]\d{2}\b'
    }

    # Column name patterns that likely contain PII
    PII_COLUMN_PATTERNS = {
        'name': ['name', 'customer_name', 'first_name', 'last_name', 'full_name', 'contact_name'],
        'email': ['email', 'email_address', 'customer_email', 'contact_email'],
        'phone': ['phone', 'phone_number', 'mobile', 'telephone', 'cell'],
        'address': ['address', 'street', 'city', 'state', 'zip', 'postal', 'country'],
        'ssn': ['ssn', 'social_security', 'tax_id', 'taxpayer_id'],
        'financial': ['credit_card', 'card_number', 'account_number', 'bank_account', 'routing_number'],
        'id': ['driver_license', 'passport', 'national_id', 'license_number'],
        'health': ['diagnosis', 'medical_record', 'health_id', 'patient_id', 'prescription'],
        'dob': ['date_of_birth', 'dob', 'birth_date', 'birthdate']
    }

    # Framework requirements
    FRAMEWORK_REQUIREMENTS = {
        'GDPR': {
            'description': 'EU General Data Protection Regulation',
            'pii_types': ['name', 'email', 'phone', 'address', 'ssn', 'dob', 'id'],
            'requirements': ['consent', 'right_to_erasure', 'data_portability', 'breach_notification'],
            'masking_required': True
        },
        'PCI_DSS': {
            'description': 'Payment Card Industry Data Security Standard',
            'pii_types': ['financial'],
            'requirements': ['encryption', 'access_control', 'audit_logging', 'network_security'],
            'masking_required': True
        },
        'SOX': {
            'description': 'Sarbanes-Oxley Act',
            'pii_types': ['financial'],
            'requirements': ['data_integrity', 'audit_trail', 'access_control'],
            'masking_required': False
        },
        'HIPAA': {
            'description': 'Health Insurance Portability and Accountability Act',
            'pii_types': ['health', 'name', 'ssn', 'dob', 'address'],
            'requirements': ['encryption', 'access_control', 'audit_logging', 'minimum_necessary'],
            'masking_required': True
        }
    }

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.storage = StrandsStorage(config)

    def execute(self, context: AgentContext) -> AgentResult:
        """Execute compliance checks across configured tables."""
        compliance_config = context.config.get('compliance', {})

        # Note: agents.compliance_agent.enabled is already checked by base_agent.run()
        # This checks the separate compliance.enabled flag for backward compatibility
        if not compliance_config.get('enabled') in ('Y', 'y', True, 'true', 'True'):
            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.COMPLETED,
                output={'skipped': True, 'reason': 'compliance.enabled is not Y'}
            )

        frameworks = compliance_config.get('frameworks', ['GDPR'])
        global_pii_columns = set(compliance_config.get('global_pii_columns', []))
        global_pii_columns.update(compliance_config.get('pii_columns', []))  # Backward compat

        # Get tables to scan
        source_tables = context.config.get('source_tables', [])
        target_tables = context.config.get('target_tables', [])

        check_sources = compliance_config.get('check_sources') in ('Y', 'y', True)
        check_targets = compliance_config.get('check_targets') in ('Y', 'y', True)
        scan_all = compliance_config.get('scan_all_tables') in ('Y', 'y', True)

        # Collect all findings
        findings = []
        pii_findings = []
        violations = []
        recommendations = []

        # 1. Process explicit table_scans configuration
        table_scans = compliance_config.get('table_scans', [])
        for scan_config in table_scans:
            table_name = scan_config.get('table')
            columns = scan_config.get('columns', [])
            scan_frameworks = scan_config.get('frameworks', frameworks)

            scan_result = self._scan_table(
                table_name=table_name,
                columns=columns,
                frameworks=scan_frameworks,
                pii_check=scan_config.get('pii_check', True),
                masking_required=scan_config.get('masking_required', False),
                global_pii_columns=global_pii_columns
            )

            findings.extend(scan_result['findings'])
            pii_findings.extend(scan_result['pii_columns'])
            violations.extend(scan_result['violations'])

        # 2. Scan source tables if enabled
        if check_sources and scan_all:
            for table_config in source_tables:
                table_name = f"{table_config.get('database')}.{table_config.get('table')}"

                scan_result = self._scan_table(
                    table_name=table_name,
                    columns=[],  # Will check column names for PII patterns
                    frameworks=frameworks,
                    pii_check=True,
                    masking_required=compliance_config.get('mask_pii') in ('Y', 'y', True),
                    global_pii_columns=global_pii_columns,
                    table_config=table_config
                )

                findings.extend(scan_result['findings'])
                pii_findings.extend(scan_result['pii_columns'])
                violations.extend(scan_result['violations'])

        # 3. Scan target tables if enabled
        if check_targets:
            for table_config in target_tables:
                table_name = f"{table_config.get('database')}.{table_config.get('table')}"

                scan_result = self._scan_table(
                    table_name=table_name,
                    columns=[],
                    frameworks=frameworks,
                    pii_check=True,
                    masking_required=compliance_config.get('mask_pii') in ('Y', 'y', True),
                    global_pii_columns=global_pii_columns,
                    table_config=table_config
                )

                findings.extend(scan_result['findings'])
                pii_findings.extend(scan_result['pii_columns'])
                violations.extend(scan_result['violations'])

        # Generate recommendations
        if pii_findings:
            if compliance_config.get('mask_pii') in ('Y', 'y', True):
                recommendations.append(f"PII masking is enabled - {len(pii_findings)} columns will be protected")
            else:
                recommendations.append(f"CRITICAL: {len(pii_findings)} PII columns detected - enable mask_pii for compliance")

        if violations:
            recommendations.append(f"WARNING: {len(violations)} compliance violations detected")

        # Build framework summary
        framework_summary = {}
        for fw in frameworks:
            fw_findings = [f for f in findings if fw in f.get('frameworks', [])]
            framework_summary[fw] = {
                'description': self.FRAMEWORK_REQUIREMENTS.get(fw, {}).get('description', ''),
                'findings_count': len(fw_findings),
                'status': 'compliant' if len([f for f in fw_findings if f.get('type') == 'VIOLATION']) == 0 else 'non_compliant'
            }

        # Store compliance results
        self.storage.store_agent_data(
            self.AGENT_NAME,
            'compliance_findings',
            findings,
            use_pipe_delimited=True
        )

        # Share with other agents
        context.set_shared('compliance_findings', findings)
        context.set_shared('compliance_frameworks', frameworks)
        context.set_shared('pii_columns_detected', pii_findings)
        context.set_shared('compliance_violations', violations)

        # Log to unified audit for dashboard
        status = 'compliant' if len(violations) == 0 else 'non_compliant'
        if AUDIT_AVAILABLE and get_audit_logger:
            try:
                audit = get_audit_logger(self.config)
                audit.log_compliance(
                    job_name=context.job_name,
                    execution_id=context.execution_id,
                    status=status,
                    pii_columns=[f.get('column') for f in pii_findings],
                    violations=[v.get('description') for v in violations],
                    frameworks=frameworks
                )
            except Exception:
                pass

        return AgentResult(
            agent_name=self.AGENT_NAME,
            agent_id=self.agent_id,
            status=AgentStatus.COMPLETED,
            output={
                'frameworks_checked': frameworks,
                'framework_summary': framework_summary,
                'tables_scanned': len(set(f.get('table') for f in findings)),
                'findings_count': len(findings),
                'pii_columns_count': len(pii_findings),
                'violations_count': len(violations),
                'findings': findings,
                'pii_columns': pii_findings,
                'violations': violations
            },
            metrics={
                'findings_count': len(findings),
                'pii_columns_count': len(pii_findings),
                'violations_count': len(violations),
                'compliance_score': 100 * (1 - len(violations) / max(len(findings), 1))
            },
            recommendations=recommendations
        )

    def _scan_table(
        self,
        table_name: str,
        columns: List[str],
        frameworks: List[str],
        pii_check: bool,
        masking_required: bool,
        global_pii_columns: Set[str],
        table_config: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Scan a table for compliance issues.

        Args:
            table_name: Full table name (database.table)
            columns: Specific columns to check (empty = check all)
            frameworks: Compliance frameworks to check against
            pii_check: Whether to check for PII
            masking_required: Whether masking is required
            global_pii_columns: Set of globally defined PII column names
            table_config: Original table configuration

        Returns:
            Dict with findings, pii_columns, violations
        """
        findings = []
        pii_columns = []
        violations = []

        # Determine columns to check
        columns_to_check = columns if columns else list(global_pii_columns)

        for column in columns_to_check:
            column_lower = column.lower()

            # Check if column matches PII patterns
            pii_type = self._detect_pii_type(column_lower)

            if pii_type:
                finding = {
                    'table': table_name,
                    'column': column,
                    'type': 'PII_DETECTED',
                    'pii_type': pii_type,
                    'frameworks': self._get_applicable_frameworks(pii_type, frameworks),
                    'masking_required': masking_required,
                    'detected_at': datetime.utcnow().isoformat()
                }
                findings.append(finding)
                pii_columns.append(finding)

                # Check for violations
                if masking_required and pii_type in ['ssn', 'financial', 'health']:
                    for fw in finding['frameworks']:
                        fw_req = self.FRAMEWORK_REQUIREMENTS.get(fw, {})
                        if fw_req.get('masking_required'):
                            violations.append({
                                'table': table_name,
                                'column': column,
                                'type': 'VIOLATION',
                                'framework': fw,
                                'description': f"Column {column} contains {pii_type} data - masking required by {fw}",
                                'severity': 'critical' if pii_type in ['ssn', 'financial'] else 'warning'
                            })

        # Also check if column is in global PII list
        for global_col in global_pii_columns:
            if global_col not in [c.get('column') for c in findings]:
                pii_type = self._detect_pii_type(global_col.lower())
                if pii_type:
                    findings.append({
                        'table': table_name,
                        'column': global_col,
                        'type': 'PII_CONFIGURED',
                        'pii_type': pii_type,
                        'frameworks': self._get_applicable_frameworks(pii_type, frameworks),
                        'masking_required': masking_required,
                        'detected_at': datetime.utcnow().isoformat(),
                        'source': 'global_pii_columns'
                    })

        return {
            'findings': findings,
            'pii_columns': pii_columns,
            'violations': violations
        }

    def _detect_pii_type(self, column_name: str) -> Optional[str]:
        """Detect PII type based on column name."""
        for pii_type, patterns in self.PII_COLUMN_PATTERNS.items():
            for pattern in patterns:
                if pattern in column_name:
                    return pii_type
        return None

    def _get_applicable_frameworks(self, pii_type: str, frameworks: List[str]) -> List[str]:
        """Get frameworks that apply to a PII type."""
        applicable = []
        for fw in frameworks:
            fw_req = self.FRAMEWORK_REQUIREMENTS.get(fw, {})
            if pii_type in fw_req.get('pii_types', []):
                applicable.append(fw)
        return applicable if applicable else frameworks  # Default to all if no specific match
