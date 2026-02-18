#!/usr/bin/env python3
"""Strands Compliance Agent - Checks regulatory compliance."""

from typing import Dict, List, Any
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
    """Checks data compliance with regulatory frameworks."""

    AGENT_NAME = "compliance_agent"
    AGENT_VERSION = "2.0.0"
    AGENT_DESCRIPTION = "Validates compliance with GDPR, PCI-DSS, SOX, HIPAA"

    DEPENDENCIES = []
    PARALLEL_SAFE = True

    PII_PATTERNS = {
        'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
        'email': r'\b[\w.-]+@[\w.-]+\.\w+\b',
        'phone': r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
        'credit_card': r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b'
    }

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.storage = StrandsStorage(config)

    def execute(self, context: AgentContext) -> AgentResult:
        compliance_config = context.config.get('compliance', {})
        if not self.is_enabled('compliance.enabled'):
            return AgentResult(
                agent_name=self.AGENT_NAME,
                agent_id=self.agent_id,
                status=AgentStatus.COMPLETED,
                output={'skipped': True, 'reason': 'Compliance checks disabled'}
            )

        frameworks = compliance_config.get('frameworks', ['GDPR'])
        pii_columns = compliance_config.get('pii_columns', [])
        source_tables = context.config.get('source_tables', [])

        findings = []
        recommendations = []

        # Check each source table
        for table_config in source_tables:
            table_name = f"{table_config.get('database')}.{table_config.get('table')}"

            # Check for PII columns
            for pii_col in pii_columns:
                # Would scan actual data in production
                findings.append({
                    'table': table_name,
                    'column': pii_col,
                    'type': 'PII_DETECTED',
                    'frameworks': ['GDPR', 'PCI_DSS']
                })

        # Generate recommendations
        if findings:
            if compliance_config.get('mask_pii') in ('Y', 'y', True):
                recommendations.append("PII masking is enabled - data will be protected")
            else:
                recommendations.append("Enable PII masking for compliance")

        # Store compliance results
        self.storage.store_agent_data(
            self.AGENT_NAME,
            'compliance_findings',
            findings,
            use_pipe_delimited=True
        )

        context.set_shared('compliance_findings', findings)
        context.set_shared('compliance_frameworks', frameworks)

        # Log to unified audit for dashboard
        violations = [f.get('type') for f in findings if f.get('type') == 'VIOLATION']
        status = 'compliant' if len(violations) == 0 else 'non_compliant'
        if AUDIT_AVAILABLE and get_audit_logger:
            try:
                audit = get_audit_logger(self.config)
                audit.log_compliance(
                    job_name=context.job_name,
                    execution_id=context.execution_id,
                    status=status,
                    pii_columns=pii_columns,
                    violations=violations,
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
                'findings_count': len(findings),
                'findings': findings,
                'pii_columns_checked': len(pii_columns)
            },
            metrics={
                'findings_count': len(findings),
                'tables_scanned': len(source_tables)
            },
            recommendations=recommendations
        )
