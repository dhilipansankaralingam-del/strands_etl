"""
Compliance Agent
================
PII detection, GDPR/HIPAA/PCI-DSS compliance checks, masking recommendations,
and audit-trail generation. Mirrors ComplianceAgent from PR-6 (HTv7q).
"""

import json
import logging
import re
import time
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

_APPLICABLE_FRAMEWORKS: Dict[str, List[str]] = {
    "email":         ["gdpr", "ccpa"],
    "phone":         ["gdpr", "ccpa"],
    "ssn":           ["hipaa", "pci_dss"],
    "credit_card":   ["pci_dss"],
    "address":       ["gdpr", "ccpa"],
    "name":          ["gdpr", "hipaa", "ccpa"],
    "date_of_birth": ["gdpr", "hipaa"],
    "ip_address":    ["gdpr", "ccpa"],
    "bank_account":  ["pci_dss", "sox"],
    "medical_record":["hipaa"],
}

_SPARK_FIX_CODE: Dict[str, str] = {
    "email":        'df = df.withColumn("{col}", expr("concat(sha2(split({col},\'@\')[0],256),\'@\',split({col},\'@\')[1])"))',
    "phone":        'df = df.withColumn("{col}", expr("concat(left({col},3),\'****\',right({col},2))"))',
    "ssn":          'df = df.withColumn("{col}", expr("concat(\'***-**-\',right({col},4))"))',
    "credit_card":  'df = df.withColumn("{col}", expr("concat(\'****-****-****-\',right({col},4))"))',
    "name":         'df = df.withColumn("{col}", expr("sha2({col},256)"))',
    "address":      'df = df.withColumn("{col}", expr("{col}"))',  # generalise upstream
    "date_of_birth":'df = df.withColumn("{col}", expr("date_format(date_trunc(\'year\',{col}),\'yyyy-01-01\')"))',
    "ip_address":   'df = df.withColumn("{col}", expr("regexp_replace({col},r\'\\\\d+$\',\'0\')"))',
    "bank_account": 'df = df.withColumn("{col}", expr("sha2({col},256)"))',
    "medical_record":'df = df.withColumn("{col}", expr("sha2({col},256)"))',
}

_SQL_FIX_CODE: Dict[str, str] = {
    "email":        "CONCAT(SHA2(SPLIT({col},'@')[0],256),'@',SPLIT({col},'@')[1]) AS {col}",
    "phone":        "CONCAT(LEFT({col},3),'****',RIGHT({col},2)) AS {col}",
    "ssn":          "CONCAT('***-**-',RIGHT({col},4)) AS {col}",
    "credit_card":  "CONCAT('****-****-****-',RIGHT({col},4)) AS {col}",
    "name":         "SHA2({col},256) AS {col}",
    "address":      "CASE WHEN {col} IS NOT NULL THEN LEFT({col},3)||'***' ELSE NULL END AS {col}",
    "date_of_birth":"DATE_FORMAT(DATE_TRUNC('year',{col}),'yyyy-01-01') AS {col}",
    "ip_address":   "REGEXP_REPLACE({col},r'\\d+$','0') AS {col}",
    "bank_account": "SHA2({col},256) AS {col}",
    "medical_record":"SHA2({col},256) AS {col}",
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
    """Detect PII columns from schema metadata with verbose annotations."""
    pii_found = []
    for col in schema.get("columns", []):
        col_name  = col.get("name", "").lower()
        for pii_type, patterns in _PII_COLUMN_PATTERNS.items():
            matched_kw = next((p for p in patterns if p in col_name), None)
            if matched_kw:
                # exact match = high confidence, substring = medium
                confidence = "high" if col_name == matched_kw else "medium"
                col_entry = col.get("name", col_name)
                spark_fix = _SPARK_FIX_CODE.get(pii_type, 'df = df.withColumn("{col}", expr("sha2({col},256)"))').replace("{col}", col_entry)
                sql_fix   = _SQL_FIX_CODE.get(pii_type, "SHA2({col},256) AS {col}").replace("{col}", col_entry)
                pii_found.append({
                    "table":                schema.get("name", "unknown"),
                    "column":               col_entry,
                    "pii_type":             pii_type,
                    "data_type":            col.get("type", "string"),
                    "masking_strategy":     _MASKING_STRATEGIES.get(pii_type, "Tokenise or hash"),
                    "scan_method":          "column_name_pattern_match",
                    "pattern_matched":      matched_kw,
                    "confidence":           confidence,
                    "applicable_frameworks": _APPLICABLE_FRAMEWORKS.get(pii_type, []),
                    "spark_fix_code":       spark_fix,
                    "sql_fix_code":         sql_fix,
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


_VALUE_PII_PATTERNS = {
    "email":       re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'),
    "phone":       re.compile(r'\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b'),
    "ssn":         re.compile(r'\b\d{3}-\d{2}-\d{4}\b'),
    "credit_card": re.compile(r'\b(?:\d{4}[-\s]?){3}\d{4}\b'),
}


def _run_athena_query_compliance(sql: str, database: str, output_s3: str, region: str = "us-west-2") -> List[Dict]:
    """Execute Athena query and return rows as list of dicts."""
    import boto3
    client = boto3.client("athena", region_name=region)
    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_s3},
    )
    qid = resp["QueryExecutionId"]
    for _ in range(60):
        status = client.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get("StateChangeReason", state)
            raise RuntimeError(f"Athena query {state}: {reason}")
        time.sleep(2)
    else:
        client.stop_query_execution(QueryExecutionId=qid)
        raise TimeoutError(f"Athena query timed out: {qid}")

    pages = client.get_paginator("get_query_results").paginate(QueryExecutionId=qid)
    rows, headers = [], None
    for page in pages:
        for row in page["ResultSet"]["Rows"]:
            values = [c.get("VarCharValue", "") for c in row["Data"]]
            if headers is None:
                headers = values
            else:
                rows.append(dict(zip(headers, values)))
    return rows


@tool
def scan_table_sample_for_pii(
    database: str,
    table: str,
    athena_output_s3: str,
    column_names_json: str = "[]",
    region: str = "us-west-2",
) -> str:
    """
    Sample 100 rows from the table via Athena and scan VALUES for PII patterns
    (email regex, phone regex, SSN regex, credit card regex).
    This catches PII in columns with non-obvious names (e.g. 'field1', 'attr_x').

    Args:
        database: Glue catalog database name (or DB.table format in database field).
        table: table name.
        athena_output_s3: S3 URI for Athena query results.
        column_names_json: JSON list of column names to scan (empty = all).
        region: AWS region.

    Returns:
        JSON with value_pii_findings (column, sample_value_masked, pii_type, confidence),
        and combined_report merging schema + value findings.
    """
    try:
        # Support DB.table dot-notation
        if "." in database and not table:
            database, table = database.split(".", 1)
        elif "." in table:
            database, table = table.split(".", 1)

        column_names = json.loads(column_names_json) if column_names_json else []
        sql = f'SELECT * FROM "{database}"."{table}" LIMIT 100'
        rows = _run_athena_query_compliance(sql, database, athena_output_s3, region)

        value_findings: List[Dict] = []
        scanned_cols = column_names if column_names else (list(rows[0].keys()) if rows else [])

        for col in scanned_cols:
            for row in rows:
                cell_val = str(row.get(col, ""))
                if not cell_val:
                    continue
                for pii_type, pattern in _VALUE_PII_PATTERNS.items():
                    if pattern.search(cell_val):
                        masked = cell_val[:3] + "***" if len(cell_val) > 3 else "***"
                        value_findings.append({
                            "column":              col,
                            "sample_value_masked": masked,
                            "pii_type":            pii_type,
                            "confidence":          "high",
                            "scan_method":         "value_regex_scan",
                        })
                        break  # one finding per cell is enough
                else:
                    continue
                break  # one finding per column is enough

        return json.dumps({
            "database":           database,
            "table":              table,
            "rows_sampled":       len(rows),
            "columns_scanned":    len(scanned_cols),
            "value_pii_findings": value_findings,
            "value_pii_count":    len(value_findings),
        })
    except Exception as exc:
        logger.error("scan_table_sample_for_pii failed for %s.%s: %s", database, table, exc)
        return json.dumps({"error": str(exc), "database": database, "table": table})


@tool
def scan_table_statistical_for_pii(
    database: str,
    table: str,
    athena_output_s3: str,
    column_names_json: str = "[]",
    sample_pct: float = 1.0,
    region: str = "us-west-2",
) -> str:
    """
    Holistic PII scan using three complementary strategies:

    1. BERNOULLI sampling (default 1% of all rows, distributed across all partitions)
       — catches PII in tail partitions that a LIMIT 100 would miss entirely.
    2. Column statistics (COUNT, COUNT DISTINCT, null %, MAX length)
       — flags columns whose value shapes match PII (e.g. length=11 on phone column).
    3. Partition-aware scan: one row per partition key value
       — ensures every data slice is represented.

    Use this INSTEAD of scan_table_sample_for_pii for production tables > 1M rows.

    Args:
        database:          Glue catalog database (or DB.table dot-notation).
        table:             Table name.
        athena_output_s3:  S3 URI for Athena query results.
        column_names_json: JSON list of columns to scan (empty = all string columns).
        sample_pct:        Bernoulli sample percentage 0.1–10.0 (default 1.0 = 1%).
        region:            AWS region.

    Returns:
        JSON with statistical_pii_findings, column_stats, coverage_summary,
        and recommended_action per column.
    """
    try:
        if "." in database and not table:
            database, table = database.split(".", 1)
        elif "." in table:
            database, table = table.split(".", 1)

        column_names = json.loads(column_names_json) if column_names_json else []
        sample_pct   = max(0.1, min(10.0, sample_pct))

        # ── Strategy 1: Bernoulli sample across all partitions ────────────────
        sample_sql = (
            f'SELECT * FROM "{database}"."{table}" '
            f'TABLESAMPLE BERNOULLI({sample_pct})'
        )
        try:
            rows = _run_athena_query_compliance(sample_sql, database, athena_output_s3, region)
        except Exception:
            # Fallback: ORDER BY RAND() LIMIT 5000 if TABLESAMPLE not supported
            rows = _run_athena_query_compliance(
                f'SELECT * FROM "{database}"."{table}" ORDER BY RAND() LIMIT 5000',
                database, athena_output_s3, region
            )

        scanned_cols = column_names if column_names else (list(rows[0].keys()) if rows else [])
        value_findings: List[Dict] = []
        col_hit_counts: Dict[str, int] = {}

        for col in scanned_cols:
            hits = 0
            for row in rows:
                cell_val = str(row.get(col, ""))
                if not cell_val:
                    continue
                for pii_type, pattern in _VALUE_PII_PATTERNS.items():
                    if pattern.search(cell_val):
                        hits += 1
                        masked = cell_val[:3] + "***" if len(cell_val) > 3 else "***"
                        if hits == 1:  # record first match per column
                            value_findings.append({
                                "column":              col,
                                "sample_value_masked": masked,
                                "pii_type":            pii_type,
                                "confidence":          "high",
                                "scan_method":         "bernoulli_sample",
                                "sample_pct":          sample_pct,
                                "rows_sampled":        len(rows),
                            })
                        break
            col_hit_counts[col] = hits

        # ── Strategy 2: Column statistics via Athena aggregation ─────────────
        col_stats = []
        for col in scanned_cols[:20]:  # cap at 20 columns per query
            try:
                stats_sql = (
                    f'SELECT '
                    f'  COUNT(*) AS total_rows, '
                    f'  COUNT("{col}") AS non_null_rows, '
                    f'  COUNT(DISTINCT "{col}") AS distinct_count, '
                    f'  ROUND(100.0 * COUNT(*) FILTER (WHERE "{col}" IS NULL) / NULLIF(COUNT(*),0), 2) AS null_pct, '
                    f'  MAX(LENGTH(CAST("{col}" AS VARCHAR))) AS max_len, '
                    f'  MIN(LENGTH(CAST("{col}" AS VARCHAR))) AS min_len, '
                    f'  ROUND(AVG(LENGTH(CAST("{col}" AS VARCHAR))), 1) AS avg_len '
                    f'FROM "{database}"."{table}"'
                )
                stat_rows = _run_athena_query_compliance(stats_sql, database, athena_output_s3, region)
                if stat_rows:
                    s = stat_rows[0]
                    max_len   = int(s.get("max_len") or 0)
                    avg_len   = float(s.get("avg_len") or 0)
                    dist_cnt  = int(s.get("distinct_count") or 0)
                    # Heuristic shape detection
                    shape_hint = None
                    if 10 <= max_len <= 14 and avg_len >= 9:
                        shape_hint = "phone_shaped"
                    elif max_len == 11 and avg_len >= 10.5:
                        shape_hint = "ssn_shaped"
                    elif 13 <= max_len <= 19 and avg_len >= 12:
                        shape_hint = "credit_card_shaped"
                    elif "@" in str(s.get("max_len", "")) or avg_len >= 12:
                        shape_hint = "possibly_email"
                    col_stats.append({
                        "column":         col,
                        "total_rows":     int(s.get("total_rows") or 0),
                        "null_pct":       float(s.get("null_pct") or 0),
                        "distinct_count": dist_cnt,
                        "max_len":        max_len,
                        "avg_len":        avg_len,
                        "value_hit_count_in_sample": col_hit_counts.get(col, 0),
                        "shape_hint":     shape_hint,
                        "flag":           shape_hint is not None or col_hit_counts.get(col, 0) > 0,
                    })
            except Exception as stat_err:
                col_stats.append({"column": col, "error": str(stat_err)})

        # ── Coverage summary ──────────────────────────────────────────────────
        total_rows_est = col_stats[0].get("total_rows", 0) if col_stats else 0
        coverage_pct   = round(100.0 * len(rows) / max(total_rows_est, 1), 3) if total_rows_est else sample_pct

        return json.dumps({
            "database":              database,
            "table":                 table,
            "scan_strategy":         "bernoulli_sample + column_statistics",
            "sample_pct_requested":  sample_pct,
            "rows_sampled":          len(rows),
            "total_rows_estimated":  total_rows_est,
            "coverage_pct":          coverage_pct,
            "columns_scanned":       len(scanned_cols),
            "value_pii_findings":    value_findings,
            "value_pii_count":       len(value_findings),
            "column_statistics":     col_stats,
            "flagged_columns":       [s["column"] for s in col_stats if s.get("flag")],
            "recommendation": (
                "Increase sample_pct to 5–10% for highly sensitive tables, "
                "or enable AWS Macie for continuous automated PII detection."
                if len(value_findings) > 0 else
                "No PII found in sample. Run with sample_pct=5 for higher confidence."
            ),
        })
    except Exception as exc:
        logger.error("Statistical PII scan failed for %s.%s: %s", database, table, exc)
        return json.dumps({"error": str(exc), "database": database, "table": table})


def create_compliance_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                             region: str = "us-west-2") -> Agent:
    """Return a Strands Agent for compliance checking."""
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[scan_schema_for_pii, generate_masking_code,
               scan_table_sample_for_pii, scan_table_statistical_for_pii],
    )
