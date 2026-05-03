"""
Compliance Agent
================
PII detection, GDPR/HIPAA/PCI-DSS compliance checks, masking recommendations,
and audit-trail generation. Mirrors ComplianceAgent from PR-6 (HTv7q).
"""

import json
import logging
import re
from enum import Enum
from typing import Any, Dict, List, Tuple

from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a **Chief Data Privacy Officer** and compliance expert.

Your job is to:
1. Detect PII in column names and schema metadata.
2. Map detected PII to applicable frameworks (GDPR, HIPAA, PCI-DSS, SOX, CCPA).
3. Recommend masking / encryption / tokenisation strategies.
4. Produce an audit report and a compliance gap analysis.

Return a structured JSON report:
{
  "compliance_score": 0-100,
  "pii_columns_found": [],
  "framework_gaps": { "gdpr": [], "hipaa": [], "pci_dss": [], "sox": [], "ccpa": [] },
  "masking_recommendations": [],
  "encryption_required": true/false,
  "audit_requirements": [],
  "overall_risk_level": "low|medium|high|critical"
}

Be specific: table name, column name, PII type, required control, and example Spark/SQL code.
Return ONLY valid JSON.
"""

# ---------------------------------------------------------------------------
# PII column-name heuristics (from PR-6 ComplianceAgent)
# ---------------------------------------------------------------------------
_PII_COLUMN_PATTERNS: Dict[str, List[str]] = {
    "email":          ["email", "e_mail", "email_address", "mail"],
    "phone":          ["phone", "telephone", "mobile", "cell", "contact_number"],
    "ssn":            ["ssn", "social_security", "social_sec", "ss_number"],
    "credit_card":    ["credit_card", "card_number", "cc_number", "payment_card"],
    "address":        ["address", "street", "city", "zip", "postal", "zip_code"],
    "name":           ["name", "first_name", "last_name", "full_name", "customer_name"],
    "date_of_birth":  ["dob", "birth_date", "date_of_birth", "birthday"],
    "ip_address":     ["ip", "ip_address", "client_ip", "source_ip"],
    "bank_account":   ["bank_account", "account_number", "routing_number"],
    "medical_record": ["medical_id", "patient_id", "health_record", "diagnosis"],
}

_FRAMEWORK_PII_MAP: Dict[str, List[str]] = {
    "gdpr":    ["email", "name", "address", "phone", "date_of_birth", "ip_address"],
    "hipaa":   ["ssn", "name", "date_of_birth", "medical_record"],
    "pci_dss": ["credit_card", "bank_account"],
    "sox":     [],
    "ccpa":    ["email", "name", "address", "phone"],
}

_MASKING_STRATEGIES: Dict[str, str] = {
    "email":         "Hash domain-part: CONCAT(SHA2(local_part,256), '@', domain)",
    "phone":         "Mask last 7 digits: CONCAT(LEFT(phone,3), '****', RIGHT(phone,2))",
    "ssn":           "Tokenise via AWS KMS or store only last 4: CONCAT('***-**-', RIGHT(ssn,4))",
    "credit_card":   "PCI-compliant tokenisation; display last 4: CONCAT('****-****-****-', RIGHT(cc,4))",
    "name":          "Pseudonymise with deterministic hash or replace with ID",
    "address":       "Generalise to ZIP/postcode level for analytics",
    "date_of_birth": "Generalise to birth_year or age_band for analytics",
    "ip_address":    "Mask last octet: REGEXP_REPLACE(ip, r'\\d+$', '0')",
    "bank_account":  "Tokenise — never store in plain text",
    "medical_record":"Tokenise via HIPAA-compliant vault",
}


def _detect_pii_columns(schema: Dict) -> List[Dict]:
    """Detect PII columns from schema metadata."""
    pii_found = []
    for col in schema.get("columns", []):
        col_name  = col.get("name", "").lower()
        for pii_type, patterns in _PII_COLUMN_PATTERNS.items():
            if any(p in col_name for p in patterns):
                pii_found.append({
                    "table":          schema.get("name", "unknown"),
                    "column":         col.get("name"),
                    "pii_type":       pii_type,
                    "data_type":      col.get("type", "string"),
                    "masking_strategy": _MASKING_STRATEGIES.get(pii_type, "Tokenise or hash"),
                })
                break
    return pii_found


def _build_framework_gaps(pii_columns: List[Dict], active_frameworks: List[str]) -> Dict[str, List]:
    """Identify compliance gaps per framework."""
    gaps: Dict[str, List] = {fw: [] for fw in _FRAMEWORK_PII_MAP}
    detected_types = {c["pii_type"] for c in pii_columns}

    for framework, required_pii in _FRAMEWORK_PII_MAP.items():
        if framework not in active_frameworks:
            continue
        for pii_type in required_pii:
            if pii_type in detected_types:
                affected = [c for c in pii_columns if c["pii_type"] == pii_type]
                gaps[framework].append({
                    "pii_type":     pii_type,
                    "affected_cols": [c["column"] for c in affected],
                    "required_control": _MASKING_STRATEGIES.get(pii_type, "Tokenise"),
                    "gap": "Masking / tokenisation not confirmed in pipeline",
                })
    return gaps


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------
@tool
def scan_schema_for_pii(table_schemas_json: str, active_frameworks_json: str = '["gdpr","pci_dss"]') -> str:
    """
    Scan table schemas for PII columns and produce a compliance report.

    Args:
        table_schemas_json:     JSON list of table schema objects (name, columns[]).
        active_frameworks_json: JSON list of compliance frameworks to check against.

    Returns:
        JSON compliance report with pii_columns_found, framework_gaps, masking_recommendations.
    """
    try:
        schemas    = json.loads(table_schemas_json)
        frameworks = json.loads(active_frameworks_json)

        all_pii: List[Dict] = []
        for schema in schemas:
            all_pii.extend(_detect_pii_columns(schema))

        gaps         = _build_framework_gaps(all_pii, frameworks)
        total_gaps   = sum(len(v) for v in gaps.values())
        risk_level   = ("critical" if total_gaps > 10 else
                        "high"     if total_gaps > 5  else
                        "medium"   if total_gaps > 2  else "low")
        enc_required = any(fw in frameworks for fw in ("gdpr", "hipaa", "pci_dss"))
        score        = max(0, 100 - total_gaps * 10 - len(all_pii) * 3)

        masking_recs = list({c["masking_strategy"] for c in all_pii})
        audit_reqs   = [
            "Enable AWS CloudTrail for all data-store access",
            "Tag PII columns in Glue Data Catalog using AWS Lake Formation",
            "Configure S3 Object Lock for audit-log immutability",
        ]
        if "hipaa" in frameworks:
            audit_reqs.append("Implement HIPAA Minimum Necessary access controls via Lake Formation")
        if "pci_dss" in frameworks:
            audit_reqs.append("Enable Macie scans on S3 buckets storing payment data")

        report = {
            "compliance_score":       max(0, score),
            "pii_columns_found":      all_pii,
            "total_pii_columns":      len(all_pii),
            "framework_gaps":         gaps,
            "masking_recommendations": masking_recs,
            "encryption_required":    enc_required,
            "audit_requirements":     audit_reqs,
            "overall_risk_level":     risk_level,
            "active_frameworks":      frameworks,
        }
        return json.dumps(report)

    except Exception as exc:
        logger.error("Compliance scan failed: %s", exc)
        return json.dumps({"error": str(exc), "compliance_score": 0, "overall_risk_level": "unknown"})


@tool
def generate_masking_code(pii_columns_json: str) -> str:
    """
    Generate PySpark masking code for detected PII columns.

    Args:
        pii_columns_json: JSON list of PII column objects (table, column, pii_type).

    Returns:
        JSON with generated PySpark masking transformations per table.
    """
    try:
        pii_cols = json.loads(pii_columns_json)
        by_table: Dict[str, List[str]] = {}

        _spark_masks = {
            "email":        lambda c: f"sha2(split({c}, '@')[0], 256) || '@' || split({c}, '@')[1]",
            "phone":        lambda c: f"concat(left({c}, 3), '****', right({c}, 2))",
            "ssn":          lambda c: f"concat('***-**-', right({c}, 4))",
            "credit_card":  lambda c: f"concat('****-****-****-', right({c}, 4))",
            "name":         lambda c: f"sha2({c}, 256)",
            "address":      lambda c: f"{c}",   # Generalise upstream
            "date_of_birth":lambda c: f"date_format(date_trunc('year', {c}), 'yyyy-01-01')",
            "ip_address":   lambda c: f"regexp_replace({c}, r'\\\\d+$', '0')",
            "bank_account": lambda c: f"sha2({c}, 256)",
            "medical_record": lambda c: f"sha2({c}, 256)",
        }

        for item in pii_cols:
            table   = item.get("table", "df")
            col     = item.get("column", "col")
            pii_type = item.get("pii_type", "")
            mask_fn = _spark_masks.get(pii_type, lambda c: f"sha2({c}, 256)")
            expr    = mask_fn(col)
            by_table.setdefault(table, []).append(
                f'df = df.withColumn("{col}", expr("{expr}"))'
            )

        result = {
            table: {
                "masking_code": "\n".join(lines),
                "import": "from pyspark.sql.functions import expr, sha2, regexp_replace",
            }
            for table, lines in by_table.items()
        }
        return json.dumps(result)

    except Exception as exc:
        logger.error("Masking code gen failed: %s", exc)
        return json.dumps({"error": str(exc)})


def create_compliance_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                             region: str = "us-west-2") -> Agent:
    """Return a Strands Agent for compliance checking."""
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[scan_schema_for_pii, generate_masking_code],
    )
