#!/usr/bin/env python3
"""
Compliance Agent
================

Intelligent agent that performs data compliance checks:
1. PII detection and masking
2. Data governance framework compliance (GDPR, HIPAA, SOX, PCI-DSS)
3. Data lineage tracking
4. Access pattern monitoring
5. Retention policy enforcement
6. Cross-border data transfer validation

Works on both source and target tables based on config flags.
"""

import re
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime


class ComplianceFramework(Enum):
    """Supported compliance frameworks."""
    GDPR = "gdpr"
    HIPAA = "hipaa"
    SOX = "sox"
    PCI_DSS = "pci_dss"
    CCPA = "ccpa"
    FERPA = "ferpa"
    CUSTOM = "custom"


class PIIType(Enum):
    """Types of PII data."""
    EMAIL = "email"
    PHONE = "phone"
    SSN = "ssn"
    CREDIT_CARD = "credit_card"
    NAME = "name"
    ADDRESS = "address"
    DATE_OF_BIRTH = "dob"
    IP_ADDRESS = "ip_address"
    MEDICAL_RECORD = "medical_record"
    FINANCIAL_ACCOUNT = "financial_account"
    PASSPORT = "passport"
    DRIVER_LICENSE = "driver_license"


class ComplianceStatus(Enum):
    """Compliance check status."""
    COMPLIANT = "compliant"
    NON_COMPLIANT = "non_compliant"
    NEEDS_REVIEW = "needs_review"
    UNKNOWN = "unknown"


@dataclass
class PIIFinding:
    """A PII detection finding."""
    column_name: str
    pii_type: PIIType
    confidence: float
    sample_pattern: str = ""
    row_count: int = 0
    recommendation: str = ""


@dataclass
class ComplianceViolation:
    """A compliance violation."""
    framework: ComplianceFramework
    rule_id: str
    rule_description: str
    severity: str
    column_name: Optional[str] = None
    table_name: Optional[str] = None
    remediation: str = ""


@dataclass
class ComplianceResult:
    """Result of compliance analysis."""
    status: ComplianceStatus = ComplianceStatus.UNKNOWN
    pii_findings: List[PIIFinding] = field(default_factory=list)
    violations: List[ComplianceViolation] = field(default_factory=list)
    lineage: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)
    masked_columns: List[str] = field(default_factory=list)
    audit_log: List[Dict[str, Any]] = field(default_factory=list)


class ComplianceAgent:
    """
    Agent that performs compliance checks and PII detection.
    """

    def __init__(self, config):
        self.config = config
        self.pii_patterns = self._init_pii_patterns()
        self.framework_rules = self._init_framework_rules()

    def _init_pii_patterns(self) -> Dict[PIIType, Dict]:
        """Initialize PII detection patterns."""
        return {
            PIIType.EMAIL: {
                "pattern": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
                "column_hints": ["email", "mail", "e_mail", "email_address", "contact"],
                "confidence_boost": 0.3
            },
            PIIType.PHONE: {
                "pattern": r"(\+\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}",
                "column_hints": ["phone", "mobile", "cell", "telephone", "fax", "contact_number"],
                "confidence_boost": 0.25
            },
            PIIType.SSN: {
                "pattern": r"\d{3}[-\s]?\d{2}[-\s]?\d{4}",
                "column_hints": ["ssn", "social_security", "sin", "national_id", "tax_id"],
                "confidence_boost": 0.4
            },
            PIIType.CREDIT_CARD: {
                "pattern": r"\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}",
                "column_hints": ["credit_card", "card_number", "cc_number", "payment_card", "pan"],
                "confidence_boost": 0.35
            },
            PIIType.NAME: {
                "pattern": r"^[A-Z][a-z]+(\s+[A-Z][a-z]+)+$",
                "column_hints": ["name", "full_name", "first_name", "last_name", "customer_name", "user_name", "person"],
                "confidence_boost": 0.2
            },
            PIIType.ADDRESS: {
                "pattern": r"\d+\s+[\w\s]+(?:street|st|avenue|ave|road|rd|drive|dr|lane|ln|way|blvd|boulevard)",
                "column_hints": ["address", "street", "addr", "location", "residence", "mailing"],
                "confidence_boost": 0.25
            },
            PIIType.DATE_OF_BIRTH: {
                "pattern": r"\d{4}[-/]\d{2}[-/]\d{2}|\d{2}[-/]\d{2}[-/]\d{4}",
                "column_hints": ["dob", "birth_date", "date_of_birth", "birthday", "born"],
                "confidence_boost": 0.3
            },
            PIIType.IP_ADDRESS: {
                "pattern": r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}",
                "column_hints": ["ip", "ip_address", "client_ip", "source_ip", "remote_addr"],
                "confidence_boost": 0.2
            },
            PIIType.MEDICAL_RECORD: {
                "pattern": r"MRN\d+|MR\d+|\d{6,10}",
                "column_hints": ["mrn", "medical_record", "patient_id", "health_id", "nhi"],
                "confidence_boost": 0.35
            },
            PIIType.FINANCIAL_ACCOUNT: {
                "pattern": r"\d{8,17}",
                "column_hints": ["account_number", "bank_account", "iban", "routing", "aba"],
                "confidence_boost": 0.3
            },
            PIIType.PASSPORT: {
                "pattern": r"[A-Z]{1,2}\d{6,9}",
                "column_hints": ["passport", "passport_number", "travel_document"],
                "confidence_boost": 0.35
            },
            PIIType.DRIVER_LICENSE: {
                "pattern": r"[A-Z]{1,2}\d{5,8}",
                "column_hints": ["driver_license", "dl_number", "license_number", "driving_permit"],
                "confidence_boost": 0.3
            }
        }

    def _init_framework_rules(self) -> Dict[ComplianceFramework, List[Dict]]:
        """Initialize compliance framework rules."""
        return {
            ComplianceFramework.GDPR: [
                {
                    "rule_id": "GDPR-001",
                    "description": "PII must be encrypted at rest",
                    "check": "encryption_at_rest",
                    "severity": "high",
                    "remediation": "Enable S3 server-side encryption (SSE-S3 or SSE-KMS)"
                },
                {
                    "rule_id": "GDPR-002",
                    "description": "PII must be encrypted in transit",
                    "check": "encryption_in_transit",
                    "severity": "high",
                    "remediation": "Use HTTPS/TLS for all data transfers"
                },
                {
                    "rule_id": "GDPR-003",
                    "description": "Data subject consent must be tracked",
                    "check": "consent_tracking",
                    "severity": "medium",
                    "remediation": "Add consent_date and consent_source columns"
                },
                {
                    "rule_id": "GDPR-004",
                    "description": "Right to erasure must be supported",
                    "check": "deletion_support",
                    "severity": "high",
                    "remediation": "Implement soft delete with deletion_date column"
                },
                {
                    "rule_id": "GDPR-005",
                    "description": "Data retention period must be defined",
                    "check": "retention_policy",
                    "severity": "medium",
                    "remediation": "Add data_expiry_date column and implement lifecycle policy"
                },
                {
                    "rule_id": "GDPR-006",
                    "description": "Cross-border transfers require adequacy",
                    "check": "cross_border",
                    "severity": "high",
                    "remediation": "Ensure data stays in approved regions or use SCCs"
                }
            ],
            ComplianceFramework.HIPAA: [
                {
                    "rule_id": "HIPAA-001",
                    "description": "PHI must be de-identified or encrypted",
                    "check": "phi_protection",
                    "severity": "critical",
                    "remediation": "Apply de-identification or encryption to all PHI columns"
                },
                {
                    "rule_id": "HIPAA-002",
                    "description": "Access to PHI must be logged",
                    "check": "access_logging",
                    "severity": "high",
                    "remediation": "Enable CloudTrail and S3 access logging"
                },
                {
                    "rule_id": "HIPAA-003",
                    "description": "Minimum necessary standard",
                    "check": "minimum_necessary",
                    "severity": "medium",
                    "remediation": "Only include necessary PHI columns in output"
                },
                {
                    "rule_id": "HIPAA-004",
                    "description": "BAA must be in place",
                    "check": "baa_verification",
                    "severity": "critical",
                    "remediation": "Verify BAA with all data processors"
                }
            ],
            ComplianceFramework.PCI_DSS: [
                {
                    "rule_id": "PCI-001",
                    "description": "Card data must be masked/tokenized",
                    "check": "card_masking",
                    "severity": "critical",
                    "remediation": "Mask PAN to show only last 4 digits or use tokenization"
                },
                {
                    "rule_id": "PCI-002",
                    "description": "CVV must never be stored",
                    "check": "cvv_storage",
                    "severity": "critical",
                    "remediation": "Remove CVV column from all storage"
                },
                {
                    "rule_id": "PCI-003",
                    "description": "Access must be restricted",
                    "check": "access_control",
                    "severity": "high",
                    "remediation": "Implement row-level security and column-level encryption"
                },
                {
                    "rule_id": "PCI-004",
                    "description": "Encryption keys must be managed",
                    "check": "key_management",
                    "severity": "high",
                    "remediation": "Use AWS KMS with key rotation enabled"
                }
            ],
            ComplianceFramework.SOX: [
                {
                    "rule_id": "SOX-001",
                    "description": "Financial data must have audit trail",
                    "check": "audit_trail",
                    "severity": "high",
                    "remediation": "Track all changes with created_by, updated_by, timestamps"
                },
                {
                    "rule_id": "SOX-002",
                    "description": "Segregation of duties",
                    "check": "segregation",
                    "severity": "medium",
                    "remediation": "Separate roles for data creation, approval, and reporting"
                },
                {
                    "rule_id": "SOX-003",
                    "description": "Data integrity controls",
                    "check": "integrity_controls",
                    "severity": "high",
                    "remediation": "Implement checksums and reconciliation checks"
                }
            ],
            ComplianceFramework.CCPA: [
                {
                    "rule_id": "CCPA-001",
                    "description": "Consumer data must be discoverable",
                    "check": "data_discovery",
                    "severity": "medium",
                    "remediation": "Maintain data catalog with PII classification"
                },
                {
                    "rule_id": "CCPA-002",
                    "description": "Opt-out must be supported",
                    "check": "opt_out_support",
                    "severity": "high",
                    "remediation": "Add do_not_sell flag column"
                }
            ]
        }

    def analyze_compliance(
        self,
        table_schema: Dict[str, Any],
        table_name: str,
        sample_data: Optional[List[Dict]] = None,
        is_source: bool = True
    ) -> ComplianceResult:
        """
        Analyze a table for compliance issues.

        Args:
            table_schema: Dictionary with column names and types
            table_name: Name of the table being analyzed
            sample_data: Optional sample rows for pattern detection
            is_source: Whether this is a source (True) or target (False) table
        """
        result = ComplianceResult()

        # Skip based on config
        if is_source and not self.config.check_sources:
            return result
        if not is_source and not self.config.check_targets:
            return result

        # Detect PII
        pii_findings = self._detect_pii(table_schema, sample_data)
        result.pii_findings = pii_findings

        # Check against configured frameworks
        for framework_name in self.config.frameworks:
            try:
                framework = ComplianceFramework(framework_name.lower())
                violations = self._check_framework_compliance(
                    framework, table_schema, table_name, pii_findings
                )
                result.violations.extend(violations)
            except ValueError:
                continue

        # Generate masking recommendations if PII found
        if pii_findings and self.config.mask_pii:
            result.masked_columns = [f.column_name for f in pii_findings]
            result.recommendations.extend(
                self._generate_masking_recommendations(pii_findings)
            )

        # Determine overall status
        if result.violations:
            critical_count = sum(1 for v in result.violations if v.severity == "critical")
            high_count = sum(1 for v in result.violations if v.severity == "high")

            if critical_count > 0:
                result.status = ComplianceStatus.NON_COMPLIANT
            elif high_count > 0:
                result.status = ComplianceStatus.NEEDS_REVIEW
            else:
                result.status = ComplianceStatus.COMPLIANT
        else:
            result.status = ComplianceStatus.COMPLIANT

        # Add audit entry
        result.audit_log.append({
            "timestamp": datetime.utcnow().isoformat(),
            "action": "compliance_check",
            "table": table_name,
            "is_source": is_source,
            "status": result.status.value,
            "pii_columns": len(result.pii_findings),
            "violations": len(result.violations)
        })

        return result

    def _detect_pii(
        self,
        schema: Dict[str, Any],
        sample_data: Optional[List[Dict]] = None
    ) -> List[PIIFinding]:
        """Detect PII columns based on schema and sample data."""
        findings = []

        columns = schema.get("columns", [])
        if isinstance(columns, dict):
            columns = [{"name": k, "type": v} for k, v in columns.items()]

        for col in columns:
            col_name = col.get("name", "").lower() if isinstance(col, dict) else str(col).lower()

            for pii_type, pattern_info in self.pii_patterns.items():
                # Check column name hints
                confidence = 0.0
                for hint in pattern_info["column_hints"]:
                    if hint in col_name:
                        confidence = 0.5 + pattern_info["confidence_boost"]
                        break

                # Check sample data if available
                if sample_data and confidence > 0:
                    matched_count = 0
                    total_count = len(sample_data)
                    pattern = pattern_info["pattern"]

                    for row in sample_data:
                        value = str(row.get(col_name, "") or row.get(col.get("name", ""), ""))
                        if re.match(pattern, value, re.IGNORECASE):
                            matched_count += 1

                    if total_count > 0:
                        match_ratio = matched_count / total_count
                        confidence = min(1.0, confidence + match_ratio * 0.3)

                if confidence >= 0.5:
                    findings.append(PIIFinding(
                        column_name=col.get("name", col_name) if isinstance(col, dict) else col_name,
                        pii_type=pii_type,
                        confidence=confidence,
                        recommendation=self._get_pii_recommendation(pii_type)
                    ))

        # Check configured PII columns
        for pii_col in self.config.pii_columns:
            col_lower = pii_col.lower()
            for col in columns:
                col_name = col.get("name", "").lower() if isinstance(col, dict) else str(col).lower()
                if col_lower == col_name:
                    # Already found via pattern matching
                    if not any(f.column_name.lower() == col_lower for f in findings):
                        findings.append(PIIFinding(
                            column_name=pii_col,
                            pii_type=PIIType.NAME,  # Default type for configured columns
                            confidence=1.0,
                            recommendation="Configured as PII column - apply masking"
                        ))

        return findings

    def _check_framework_compliance(
        self,
        framework: ComplianceFramework,
        schema: Dict[str, Any],
        table_name: str,
        pii_findings: List[PIIFinding]
    ) -> List[ComplianceViolation]:
        """Check compliance against a specific framework."""
        violations = []
        rules = self.framework_rules.get(framework, [])

        columns = schema.get("columns", [])
        col_names = set()
        for col in columns:
            if isinstance(col, dict):
                col_names.add(col.get("name", "").lower())
            else:
                col_names.add(str(col).lower())

        for rule in rules:
            check_type = rule["check"]

            if check_type == "encryption_at_rest" and pii_findings:
                # Check if encryption metadata is present
                if not schema.get("encryption_enabled"):
                    violations.append(ComplianceViolation(
                        framework=framework,
                        rule_id=rule["rule_id"],
                        rule_description=rule["description"],
                        severity=rule["severity"],
                        table_name=table_name,
                        remediation=rule["remediation"]
                    ))

            elif check_type == "consent_tracking" and pii_findings:
                consent_cols = {"consent_date", "consent_given", "consent_source", "gdpr_consent"}
                if not consent_cols.intersection(col_names):
                    violations.append(ComplianceViolation(
                        framework=framework,
                        rule_id=rule["rule_id"],
                        rule_description=rule["description"],
                        severity=rule["severity"],
                        table_name=table_name,
                        remediation=rule["remediation"]
                    ))

            elif check_type == "deletion_support":
                deletion_cols = {"deleted_at", "is_deleted", "deletion_date", "soft_delete"}
                if not deletion_cols.intersection(col_names):
                    violations.append(ComplianceViolation(
                        framework=framework,
                        rule_id=rule["rule_id"],
                        rule_description=rule["description"],
                        severity=rule["severity"],
                        table_name=table_name,
                        remediation=rule["remediation"]
                    ))

            elif check_type == "retention_policy":
                retention_cols = {"expiry_date", "data_retention_date", "ttl", "valid_until"}
                if not retention_cols.intersection(col_names):
                    violations.append(ComplianceViolation(
                        framework=framework,
                        rule_id=rule["rule_id"],
                        rule_description=rule["description"],
                        severity=rule["severity"],
                        table_name=table_name,
                        remediation=rule["remediation"]
                    ))

            elif check_type == "audit_trail":
                audit_cols = {"created_at", "updated_at", "created_by", "updated_by"}
                if len(audit_cols.intersection(col_names)) < 2:
                    violations.append(ComplianceViolation(
                        framework=framework,
                        rule_id=rule["rule_id"],
                        rule_description=rule["description"],
                        severity=rule["severity"],
                        table_name=table_name,
                        remediation=rule["remediation"]
                    ))

            elif check_type == "phi_protection":
                # Check if medical/health PII found
                medical_pii = [f for f in pii_findings if f.pii_type == PIIType.MEDICAL_RECORD]
                if medical_pii:
                    violations.append(ComplianceViolation(
                        framework=framework,
                        rule_id=rule["rule_id"],
                        rule_description=rule["description"],
                        severity=rule["severity"],
                        table_name=table_name,
                        column_name=medical_pii[0].column_name,
                        remediation=rule["remediation"]
                    ))

            elif check_type == "card_masking":
                # Check if credit card PII found
                card_pii = [f for f in pii_findings if f.pii_type == PIIType.CREDIT_CARD]
                if card_pii:
                    violations.append(ComplianceViolation(
                        framework=framework,
                        rule_id=rule["rule_id"],
                        rule_description=rule["description"],
                        severity=rule["severity"],
                        table_name=table_name,
                        column_name=card_pii[0].column_name,
                        remediation=rule["remediation"]
                    ))

            elif check_type == "cvv_storage":
                cvv_cols = {"cvv", "cvc", "cvv2", "security_code", "card_security"}
                found_cvv = cvv_cols.intersection(col_names)
                if found_cvv:
                    violations.append(ComplianceViolation(
                        framework=framework,
                        rule_id=rule["rule_id"],
                        rule_description=rule["description"],
                        severity=rule["severity"],
                        table_name=table_name,
                        column_name=list(found_cvv)[0],
                        remediation=rule["remediation"]
                    ))

            elif check_type == "opt_out_support":
                opt_out_cols = {"do_not_sell", "opt_out", "marketing_consent", "data_sharing"}
                if not opt_out_cols.intersection(col_names):
                    violations.append(ComplianceViolation(
                        framework=framework,
                        rule_id=rule["rule_id"],
                        rule_description=rule["description"],
                        severity=rule["severity"],
                        table_name=table_name,
                        remediation=rule["remediation"]
                    ))

        return violations

    def _get_pii_recommendation(self, pii_type: PIIType) -> str:
        """Get masking recommendation for PII type."""
        recommendations = {
            PIIType.EMAIL: "Mask email: john.doe@example.com -> j***@example.com",
            PIIType.PHONE: "Mask phone: 555-123-4567 -> ***-***-4567",
            PIIType.SSN: "Mask SSN: 123-45-6789 -> ***-**-6789",
            PIIType.CREDIT_CARD: "Mask card: 4111-1111-1111-1111 -> ****-****-****-1111",
            PIIType.NAME: "Use pseudonymization or hash",
            PIIType.ADDRESS: "Generalize to city/state level only",
            PIIType.DATE_OF_BIRTH: "Generalize to year or age range",
            PIIType.IP_ADDRESS: "Mask last octet: 192.168.1.100 -> 192.168.1.xxx",
            PIIType.MEDICAL_RECORD: "Remove or hash completely",
            PIIType.FINANCIAL_ACCOUNT: "Mask: 12345678 -> ****5678",
            PIIType.PASSPORT: "Remove or hash completely",
            PIIType.DRIVER_LICENSE: "Remove or hash completely"
        }
        return recommendations.get(pii_type, "Apply appropriate masking")

    def _generate_masking_recommendations(self, findings: List[PIIFinding]) -> List[str]:
        """Generate masking code recommendations."""
        recommendations = []

        recommendations.append("# PySpark masking functions:")
        recommendations.append("from pyspark.sql.functions import col, regexp_replace, sha2, concat, lit, substring")
        recommendations.append("")

        for finding in findings:
            col_name = finding.column_name
            pii_type = finding.pii_type

            if pii_type == PIIType.EMAIL:
                recommendations.append(f"""# Mask email: {col_name}
df = df.withColumn("{col_name}_masked",
    regexp_replace(col("{col_name}"), r"^(.)[^@]*(@.*)$", "$1***$2"))""")

            elif pii_type == PIIType.PHONE:
                recommendations.append(f"""# Mask phone: {col_name}
df = df.withColumn("{col_name}_masked",
    regexp_replace(col("{col_name}"), r"\\d{{3}}[-.]?\\d{{3}}", "***-***"))""")

            elif pii_type == PIIType.SSN:
                recommendations.append(f"""# Mask SSN: {col_name}
df = df.withColumn("{col_name}_masked",
    concat(lit("***-**-"), substring(col("{col_name}"), -4, 4)))""")

            elif pii_type == PIIType.CREDIT_CARD:
                recommendations.append(f"""# Mask credit card: {col_name}
df = df.withColumn("{col_name}_masked",
    concat(lit("****-****-****-"), substring(col("{col_name}"), -4, 4)))""")

            elif pii_type == PIIType.NAME:
                recommendations.append(f"""# Hash name: {col_name}
df = df.withColumn("{col_name}_hashed", sha2(col("{col_name}"), 256))""")

            else:
                recommendations.append(f"""# Hash {pii_type.value}: {col_name}
df = df.withColumn("{col_name}_masked", sha2(col("{col_name}"), 256))""")

        return recommendations

    def generate_compliance_report(self, result: ComplianceResult, table_name: str) -> str:
        """Generate a compliance report in markdown format."""
        report = []
        report.append(f"# Compliance Report: {table_name}")
        report.append(f"\n**Status:** {result.status.value.upper()}")
        report.append(f"**Generated:** {datetime.utcnow().isoformat()}")

        # PII Summary
        report.append("\n## PII Detection Summary")
        if result.pii_findings:
            report.append(f"\nFound {len(result.pii_findings)} columns with potential PII:")
            report.append("\n| Column | PII Type | Confidence | Recommendation |")
            report.append("|--------|----------|------------|----------------|")
            for finding in result.pii_findings:
                report.append(
                    f"| {finding.column_name} | {finding.pii_type.value} | "
                    f"{finding.confidence:.0%} | {finding.recommendation} |"
                )
        else:
            report.append("\nNo PII detected.")

        # Violations
        report.append("\n## Compliance Violations")
        if result.violations:
            report.append(f"\nFound {len(result.violations)} violations:")
            for v in result.violations:
                report.append(f"\n### {v.rule_id} ({v.severity.upper()})")
                report.append(f"**Framework:** {v.framework.value}")
                report.append(f"**Description:** {v.rule_description}")
                if v.column_name:
                    report.append(f"**Column:** {v.column_name}")
                report.append(f"**Remediation:** {v.remediation}")
        else:
            report.append("\nNo compliance violations found.")

        # Recommendations
        if result.recommendations:
            report.append("\n## Masking Recommendations")
            report.append("\n```python")
            report.append("\n".join(result.recommendations))
            report.append("```")

        return "\n".join(report)
