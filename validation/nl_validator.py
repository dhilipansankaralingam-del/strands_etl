"""
validation/nl_validator.py
===========================
Natural Language Validation engine.

Converts a plain-English question into Athena SQL, runs it, interprets
the result with AI, and returns a structured finding.

Every prompt sent to Bedrock is logged to the console via PromptLogger.

Usage (from config)
-------------------
    natural_language_validations:
      - table: "audit_db.etl_orchestrator_audit"
        query: "Are there any runs where a GLUE_TRIGGER has no matching VALIDATION?"
        severity: "HIGH"

Usage (programmatic)
--------------------
    from validation.nl_validator import NLValidator

    v = NLValidator(aws_region="us-east-1")
    findings = v.run_all(cfg["natural_language_validations"], cfg)
    for f in findings:
        print(f["verdict"], f["explanation"])
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3

from validation.prompt_logger import get_logger

logger = logging.getLogger("strands.nl_validator")

_NL_SYSTEM_PROMPT = """
You are a senior data engineer specialising in AWS Athena (Presto/Trino SQL)
and data quality analysis.

## Your Role
You receive:
1. A natural-language data quality question from a user
2. The Athena schema for the table(s) involved

Your tasks are:
A. Convert the question to valid Athena SQL
B. After the SQL result is provided, interpret it and give a clear verdict

## SQL Rules
- Always qualify table names: database.table
- Use DATE_PARSE, CAST(... AS DATE), DATE_DIFF for date arithmetic
- Use DATE_ADD('day', -N, current_date) for relative date filters
- Limit to 1000 rows unless the question asks for aggregates
- Use TRY_CAST to avoid type errors on messy data

## SQL Output Format (Step A)
{
  "sql": "<complete Athena SQL>",
  "intent": "<one sentence: what this query checks>",
  "tables_used": ["db.table"]
}

## Interpretation Output Format (Step B — given SQL result)
{
  "verdict": "PASS | FAIL | WARNING | INCONCLUSIVE",
  "confidence": <float 0.0-1.0>,
  "explanation": "<clear 2-3 sentence finding>",
  "row_count_found": <int>,
  "severity": "LOW | MEDIUM | HIGH | CRITICAL",
  "recommended_action": "<what to do about it>",
  "sample_evidence": ["<row summary 1>", "<row summary 2>"]
}
"""


@dataclass
class NLFinding:
    nl_query: str
    table: str
    severity: str
    sql_generated: str
    sql_intent: str
    row_count: int
    verdict: str               # PASS | FAIL | WARNING | INCONCLUSIVE
    confidence: float
    explanation: str
    recommended_action: str
    sample_evidence: List[str]
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    run_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_csv_row(self) -> Dict[str, Any]:
        return {
            "type":               "NL_VALIDATION",
            "table":              self.table,
            "nl_query":           self.nl_query,
            "sql_generated":      self.sql_generated.replace("\n", " ")[:500],
            "verdict":            self.verdict,
            "confidence":         self.confidence,
            "row_count":          self.row_count,
            "severity":           self.severity,
            "explanation":        self.explanation,
            "recommended_action": self.recommended_action,
            "sample_evidence":    " | ".join(self.sample_evidence[:3]),
            "input_tokens":       self.input_tokens,
            "output_tokens":      self.output_tokens,
            "cost_usd":           round(self.cost_usd, 6),
            "run_at":             self.run_at,
        }


class NLValidator:

    def __init__(
        self,
        athena_output: str = "s3://strands-etl-athena-results/nl-validation/",
        aws_region: str = "us-east-1",
        model_id: str = "anthropic.claude-3-sonnet-20240229-v1:0",
    ):
        self.athena_output = athena_output
        self.model_id      = model_id
        self.athena        = boto3.client("athena",          region_name=aws_region)
        self.bedrock       = boto3.client("bedrock-runtime", region_name=aws_region)
        self.plogger       = get_logger()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_all(
        self,
        nl_validations: List[Dict[str, Any]],
        cfg: Dict[str, Any],
    ) -> List[NLFinding]:
        """Run every NL validation defined in the config."""
        findings: List[NLFinding] = []
        for item in nl_validations:
            table    = item["table"]
            nl_query = item["query"]
            severity = item.get("severity", "MEDIUM")
            logger.info(f"[NLValidator] Running: {nl_query[:80]}...")
            try:
                finding = self._run_one(table, nl_query, severity, cfg)
                findings.append(finding)
                self._print_finding(finding)
            except Exception as e:
                logger.error(f"[NLValidator] Failed for query '{nl_query[:60]}': {e}")
        return findings

    # ------------------------------------------------------------------
    # Core flow
    # ------------------------------------------------------------------

    def _run_one(
        self,
        table: str,
        nl_query: str,
        severity: str,
        cfg: Dict[str, Any],
    ) -> NLFinding:
        db, tbl = table.split(".", 1)
        schema  = self._get_schema_description(db, tbl, cfg)

        # ── Step 1: NL → SQL
        sql_prompt = self._build_sql_prompt(nl_query, table, schema)

        self.plogger.log_prompt(
            agent_type="nl_sql_gen",
            system_prompt=_NL_SYSTEM_PROMPT,
            user_prompt=sql_prompt,
            table_name=table,
            extra_context={"nl_query": nl_query[:120]},
        )

        sql_raw, in_tok1, out_tok1 = self._invoke_bedrock(sql_prompt)
        self.plogger.log_response("nl_sql_gen", table, sql_raw, in_tok1, out_tok1)

        sql_parsed  = self._parse_json(sql_raw)
        sql         = sql_parsed.get("sql", "")
        sql_intent  = sql_parsed.get("intent", nl_query)

        if not sql:
            return NLFinding(
                nl_query=nl_query, table=table, severity=severity,
                sql_generated="", sql_intent="SQL generation failed",
                row_count=0, verdict="INCONCLUSIVE", confidence=0.0,
                explanation="AI could not generate SQL for this question.",
                recommended_action="Rephrase the query or check schema.",
                sample_evidence=[], input_tokens=in_tok1, output_tokens=out_tok1,
                cost_usd=(in_tok1 * 3e-6 + out_tok1 * 15e-6),
            )

        # ── Step 2: Run the SQL
        rows = self._run_athena(sql) or []
        row_count = len(rows)
        sample_rows = rows[:5]

        # ── Step 3: Interpret result
        interp_prompt = self._build_interp_prompt(nl_query, sql, sql_intent, sample_rows, row_count)

        self.plogger.log_prompt(
            agent_type="nl_interpreter",
            system_prompt=_NL_SYSTEM_PROMPT,
            user_prompt=interp_prompt,
            table_name=table,
            extra_context={"rows_returned": row_count, "sql_intent": sql_intent[:80]},
        )

        interp_raw, in_tok2, out_tok2 = self._invoke_bedrock(interp_prompt)
        self.plogger.log_response("nl_interpreter", table, interp_raw, in_tok2, out_tok2)

        interp = self._parse_json(interp_raw)
        total_in  = in_tok1 + in_tok2
        total_out = out_tok1 + out_tok2

        return NLFinding(
            nl_query=nl_query,
            table=table,
            severity=interp.get("severity", severity),
            sql_generated=sql,
            sql_intent=sql_intent,
            row_count=row_count,
            verdict=interp.get("verdict", "INCONCLUSIVE"),
            confidence=float(interp.get("confidence", 0.5)),
            explanation=interp.get("explanation", ""),
            recommended_action=interp.get("recommended_action", ""),
            sample_evidence=interp.get("sample_evidence", []),
            input_tokens=total_in,
            output_tokens=total_out,
            cost_usd=round(total_in * 3e-6 + total_out * 15e-6, 6),
        )

    # ------------------------------------------------------------------
    # Prompt builders
    # ------------------------------------------------------------------

    def _build_sql_prompt(self, nl_query: str, table: str, schema: str) -> str:
        return f"""
## Table Schema
Table  : {table}
{schema}

## Natural Language Question
"{nl_query}"

## Step A — Your Task
Convert this question to a valid Athena SQL statement.
Respond ONLY with the JSON format defined in the system prompt (Step A).
""".strip()

    def _build_interp_prompt(
        self,
        nl_query: str,
        sql: str,
        intent: str,
        sample_rows: List[Dict],
        row_count: int,
    ) -> str:
        sample_text = json.dumps(sample_rows, indent=2, default=str) if sample_rows else "(no rows returned)"
        return f"""
## Original Question
"{nl_query}"

## SQL That Was Run
```sql
{sql}
```

## Intent
{intent}

## Query Result
- Total rows returned : {row_count}
- Sample rows (up to 5):
```json
{sample_text}
```

## Step B — Your Task
Interpret these results. Does this indicate a data quality problem?
Give a clear PASS / FAIL / WARNING / INCONCLUSIVE verdict.
Respond ONLY with the JSON format defined in the system prompt (Step B).
""".strip()

    def _get_schema_description(self, db: str, table: str, cfg: Dict[str, Any]) -> str:
        full_name = f"{db}.{table}"
        tc = cfg.get("table_config", {}).get(full_name, {})
        key_cols = tc.get("key_columns", [])
        if key_cols:
            return f"Key columns: {', '.join(key_cols)}"
        return f"Table: {full_name} (schema not pre-configured — AI will infer from context)"

    # ------------------------------------------------------------------
    # Bedrock / Athena helpers
    # ------------------------------------------------------------------

    def _invoke_bedrock(self, prompt: str):
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1500,
            "system": _NL_SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": prompt}],
        }
        resp = self.bedrock.invoke_model(
            modelId=self.model_id,
            body=json.dumps(body),
        )
        result = json.loads(resp["body"].read())
        text    = result["content"][0]["text"]
        usage   = result.get("usage", {})
        return text, usage.get("input_tokens", 0), usage.get("output_tokens", 0)

    def _run_athena(self, sql: str, timeout_s: int = 60) -> Optional[List[Dict]]:
        try:
            resp = self.athena.start_query_execution(
                QueryString=sql,
                ResultConfiguration={"OutputLocation": self.athena_output},
            )
            qid      = resp["QueryExecutionId"]
            deadline = time.time() + timeout_s
            while time.time() < deadline:
                status = self.athena.get_query_execution(QueryExecutionId=qid)
                state  = status["QueryExecution"]["Status"]["State"]
                if state == "SUCCEEDED":
                    break
                if state in ("FAILED", "CANCELLED"):
                    reason = status["QueryExecution"]["Status"].get("StateChangeReason", "")
                    logger.warning(f"Athena NL query {state}: {reason[:200]}")
                    return None
                time.sleep(1)

            result = self.athena.get_query_results(QueryExecutionId=qid)
            rs   = result["ResultSet"]
            cols = [c["Label"] for c in rs["ResultSetMetadata"]["ColumnInfo"]]
            rows = []
            for row in rs["Rows"][1:]:
                data = row.get("Data", [])
                rows.append({cols[i]: data[i].get("VarCharValue") for i in range(len(cols))})
            return rows
        except Exception as e:
            logger.error(f"Athena NL error: {e}")
            return None

    def _parse_json(self, raw: str) -> Dict[str, Any]:
        text = raw.strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
        for marker in ("```json", "```"):
            if marker in text:
                start = text.index(marker) + len(marker)
                end   = text.rfind("```")
                if end > start:
                    try:
                        return json.loads(text[start:end].strip())
                    except json.JSONDecodeError:
                        pass
        return {}

    def _print_finding(self, f: NLFinding) -> None:
        colours = {"PASS": "GREEN", "FAIL": "RED", "WARNING": "YELLOW", "INCONCLUSIVE": "GREY"}
        colour  = colours.get(f.verdict, "GREY")
        C = {
            "GREEN":  "\033[92m", "RED":    "\033[91m",
            "YELLOW": "\033[93m", "GREY":   "\033[37m",
            "RESET":  "\033[0m",  "BOLD":   "\033[1m",
        }
        print(f"\n  {C[colour]}{C['BOLD']}[NL VALIDATION] {f.verdict}{C['RESET']}  "
              f"confidence={f.confidence:.0%}  severity={f.severity}")
        print(f"  Query   : {f.nl_query}")
        print(f"  Rows    : {f.row_count:,}  |  Intent: {f.sql_intent}")
        print(f"  Finding : {f.explanation}")
        print(f"  Action  : {f.recommended_action}")
        if f.sample_evidence:
            print(f"  Evidence: {'; '.join(f.sample_evidence[:2])}")
