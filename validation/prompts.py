"""
Prompt library for the Strands Validation Analysis Agent.

Four prompt categories:
  1. SYSTEM_PROMPT      – primary persona injected into every Bedrock call
  2. DECISION_PROMPT    – classifies a single failed record
  3. LEARNING_PROMPT    – extracts patterns from resolved outcomes
  4. SQL_PROMPT         – converts natural-language queries to Athena SQL
"""

from typing import Dict, Any, List
import json


# ---------------------------------------------------------------------------
# 1.  Primary system prompt – injected as the agent's standing persona
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """
You are the **Strands Validation Intelligence Agent**, a production-grade AI assistant
embedded in a data-quality and ETL governance framework.

## Your Role
You analyse failed data-validation records to determine:
- Whether a failure is a TRUE_FAILURE, FALSE_POSITIVE, or NEEDS_INVESTIGATION
- The root cause of each failure
- The best corrective action to take
- Patterns across many failures that suggest systemic issues

## Your Reasoning Approach
1. **Context-first**: Before classifying, examine the rule type, column semantics,
   historical outcomes for the same rule, and current data distribution.
2. **Confidence-calibrated**: Only output HIGH confidence (≥ 0.85) when the evidence
   strongly supports one classification. Default to NEEDS_INVESTIGATION when uncertain.
3. **Actionable**: Every classification must come with a concrete recommended action
   and at least one verification SQL query against Athena.
4. **Learning-oriented**: Reference similar historical failures when they exist,
   and explain how the current case differs or resembles them.

## Output Format
Always respond with a valid JSON object matching this schema:
{
  "classification": "TRUE_FAILURE | FALSE_POSITIVE | NEEDS_INVESTIGATION",
  "confidence": <float 0.0-1.0>,
  "explanation": "<clear, non-technical explanation>",
  "root_causes": ["<cause 1>", "<cause 2>"],
  "recommended_action": "IGNORE | RERUN | FIX_LOGIC | DATA_CORRECTION | ESCALATE | MONITOR",
  "suggested_next_steps": ["<step 1>", "<step 2>"],
  "validation_sql": "<optional Athena SQL to verify>"
}

If you cannot produce valid JSON, wrap your answer in <json>...</json> tags.
"""

# ---------------------------------------------------------------------------
# 2.  Decision prompt – classification of one failed ValidationRecord
# ---------------------------------------------------------------------------

DECISION_PROMPT_TEMPLATE = """
## Failed Validation Record

```json
{record_json}
```

## Rule Metadata
- Rule type    : {rule_type}
- Severity     : {severity}
- Failure rate : {failure_rate:.2%}  ({row_count_failed} of {total_row_count} rows)

## Historical Context
Similar past failures for rule **{rule_name}** on table **{table_name}**:
```json
{historical_json}
```

## Your Task
Classify this failed validation record. Consider:
1. Is the `expected_constraint` ({expected_constraint}) reasonable for column `{column_name}`?
2. Does the `failed_value` ({failed_value}) suggest a data-entry error, pipeline bug,
   rule misconfiguration, or genuine bad data?
3. Do the historical outcomes indicate this rule generates false positives?
4. What is the statistical significance given the failure rate?

Respond ONLY with the JSON schema defined in the system prompt.
"""


def build_decision_prompt(
    record: Dict[str, Any],
    historical_outcomes: List[Dict[str, Any]],
) -> str:
    failure_rate = 0.0
    total = record.get("total_row_count", 0)
    failed = record.get("row_count_failed", 1)
    if total > 0:
        failure_rate = failed / total

    return DECISION_PROMPT_TEMPLATE.format(
        record_json=json.dumps(record, indent=2, default=str),
        rule_type=record.get("rule_type", "UNKNOWN"),
        severity=record.get("severity", "MEDIUM"),
        failure_rate=failure_rate,
        row_count_failed=failed,
        total_row_count=total,
        rule_name=record.get("rule_name", ""),
        table_name=record.get("table_name", ""),
        historical_json=json.dumps(historical_outcomes, indent=2, default=str),
        expected_constraint=record.get("expected_constraint", ""),
        failed_value=record.get("failed_value", ""),
        column_name=record.get("column_name", ""),
    )


# ---------------------------------------------------------------------------
# 3.  Learning prompt – extract patterns from a batch of resolved outcomes
# ---------------------------------------------------------------------------

LEARNING_PROMPT_TEMPLATE = """
## Resolved Validation Outcomes (batch)

The following {outcome_count} validation failures have been resolved by human operators
or automated pipelines. Your job is to extract reusable patterns and heuristics.

```json
{outcomes_json}
```

## Analysis Tasks
1. **False-positive patterns**: Which rule types / table+column combinations
   consistently produce FALSE_POSITIVE classifications?  Why?
2. **True-failure signatures**: What data characteristics reliably signal TRUE_FAILURE?
3. **Action effectiveness**: Which recommended actions led to `was_correct = true`?
4. **Emerging issues**: Are there clusters of failures that suggest a systemic pipeline
   or source-data problem?
5. **Confidence calibration**: When was the agent under- or over-confident?

## Output Format
Respond with a JSON object:
{{
  "false_positive_patterns": [
    {{"rule_type": "...", "table_name": "...", "pattern": "...", "frequency": <int>}}
  ],
  "true_failure_signatures": ["<signature 1>", ...],
  "effective_actions": {{"action": "observation"}},
  "systemic_issues": ["<issue description>"],
  "confidence_calibration_notes": "<text>",
  "learning_vector_summary": "<one-paragraph summary>"
}}
"""


def build_learning_prompt(outcomes: List[Dict[str, Any]]) -> str:
    return LEARNING_PROMPT_TEMPLATE.format(
        outcome_count=len(outcomes),
        outcomes_json=json.dumps(outcomes, indent=2, default=str),
    )


# ---------------------------------------------------------------------------
# 4.  SQL generation prompt – NL → Athena SQL
# ---------------------------------------------------------------------------

SQL_PROMPT_TEMPLATE = """
## Athena Schema Context

Database  : {database}
Tables and columns available:
```json
{schema_json}
```

## Conversation History (last 3 turns)
{history_text}

## User Query
"{nl_query}"

## Your Task
Convert the user's natural-language query into a valid **AWS Athena SQL** statement
(Presto/Trino dialect).

Rules:
- Use only tables and columns listed in the schema above.
- Always qualify table names with the database: `{database}.<table>`.
- For date filters use `date_parse(col, '%Y-%m-%d')` or `CAST(col AS DATE)`.
- Limit results to 1000 rows unless the user explicitly asks for more.
- If the query is ambiguous, choose the most likely interpretation and add a comment.
- If the query cannot be answered from the schema, return:
  {{"error": "Cannot answer: <reason>"}}

Respond ONLY with a JSON object:
{{
  "sql": "<complete Athena SQL statement>",
  "explanation": "<one-sentence description of what the query returns>",
  "tables_used": ["<table1>", ...],
  "assumed_intent": "<what you interpreted the user to want>"
}}
"""


def build_sql_prompt(
    nl_query: str,
    database: str,
    schema: Dict[str, Any],
    conversation_history: List[Dict[str, str]] | None = None,
) -> str:
    history_lines: List[str] = []
    if conversation_history:
        for turn in conversation_history[-3:]:
            role = turn.get("role", "user").upper()
            content = turn.get("content", "")
            history_lines.append(f"[{role}] {content[:300]}")
    history_text = "\n".join(history_lines) if history_lines else "(none)"

    return SQL_PROMPT_TEMPLATE.format(
        database=database,
        schema_json=json.dumps(schema, indent=2, default=str),
        history_text=history_text,
        nl_query=nl_query,
    )


# ---------------------------------------------------------------------------
# 5.  Batch-summary prompt – summarise multiple AnalysisResults
# ---------------------------------------------------------------------------

BATCH_SUMMARY_PROMPT_TEMPLATE = """
## Batch Validation Analysis Summary Request

You have analysed {result_count} failed validation records. Here are the results:

```json
{results_json}
```

Provide a concise executive summary covering:
1. Overall failure breakdown (TRUE_FAILURE / FALSE_POSITIVE / NEEDS_INVESTIGATION counts)
2. Top 3 most impactful issues requiring immediate attention
3. Recurring patterns across multiple records
4. Recommended prioritisation order for remediation
5. Estimated data-quality score (0-100) for the current pipeline run

Respond with JSON:
{{
  "true_failure_count": <int>,
  "false_positive_count": <int>,
  "needs_investigation_count": <int>,
  "data_quality_score": <int 0-100>,
  "top_issues": ["<issue 1>", "<issue 2>", "<issue 3>"],
  "recurring_patterns": ["<pattern>"],
  "remediation_priority": ["<record_id or rule_name>"],
  "executive_summary": "<2-3 sentence summary>"
}}
"""


def build_batch_summary_prompt(results: List[Dict[str, Any]]) -> str:
    return BATCH_SUMMARY_PROMPT_TEMPLATE.format(
        result_count=len(results),
        results_json=json.dumps(results, indent=2, default=str),
    )
