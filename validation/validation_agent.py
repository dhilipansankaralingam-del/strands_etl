"""
Strands Validation Analysis Agent.

This module provides the ValidationAnalysisAgent, a multi-agent system that:

  1. decision_agent   – classifies a single ValidationRecord and scores confidence
  2. learning_agent   – reads/writes learning history from S3 and extracts patterns
  3. batch_agent      – analyses a list of records and produces an executive summary
  4. feedback_agent   – ingests user feedback to improve future classifications

All AI reasoning is backed by Amazon Bedrock (Claude 3 Sonnet by default).
Learning data (resolved outcomes, vectors) is persisted in S3.

Architecture mirrors strands_orchestrator.py to stay consistent with the project.
"""

import json
import logging
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Any, Optional

import boto3

from validation.models import (
    ValidationRecord,
    AnalysisResult,
    HistoricalOutcome,
    ValidationClassification,
    RecommendedAction,
)
from validation.prompts import (
    SYSTEM_PROMPT,
    build_decision_prompt,
    build_learning_prompt,
    build_batch_summary_prompt,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# JSON helper (same pattern as orchestrator)
# ---------------------------------------------------------------------------

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


# ---------------------------------------------------------------------------
# Main agent class
# ---------------------------------------------------------------------------

class ValidationAnalysisAgent:
    """
    Strands multi-agent system for validation failure analysis.

    Parameters
    ----------
    learning_bucket : str
        S3 bucket where learning data (outcomes, vectors) is persisted.
    aws_region : str
        AWS region for Bedrock, S3 clients.
    model_id : str
        Bedrock model to use for reasoning.
    """

    DEFAULT_MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"
    LEARNING_PREFIX = "validation/learning/outcomes/"
    VECTOR_PREFIX = "validation/learning/vectors/"

    def __init__(
        self,
        learning_bucket: str = "strands-etl-learning",
        aws_region: str = "us-east-1",
        model_id: str = DEFAULT_MODEL_ID,
    ):
        self.learning_bucket = learning_bucket
        self.model_id = model_id

        self.bedrock = boto3.client("bedrock-runtime", region_name=aws_region)
        self.s3 = boto3.client("s3", region_name=aws_region)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def analyze(self, record: ValidationRecord) -> AnalysisResult:
        """
        Analyse a single failed validation record.

        Steps:
          1. Load similar historical outcomes from S3
          2. Run decision_agent (Bedrock) → classification + confidence
          3. Persist a learning vector for future use
          4. Return AnalysisResult
        """
        logger.info(f"Analysing record {record.record_id} (rule={record.rule_name})")

        # Step 1 – historical context
        history = self._get_similar_outcomes(record, limit=10)

        # Step 2 – decision agent
        result = self.decision_agent(record, history)

        # Step 3 – persist a lightweight learning vector
        self._store_learning_vector(record, result)

        return result

    def analyze_batch(self, records: List[ValidationRecord]) -> Dict[str, Any]:
        """
        Analyse a list of ValidationRecords and return per-record results
        plus a batch executive summary.
        """
        results: List[AnalysisResult] = []
        for record in records:
            results.append(self.analyze(record))

        summary = self.batch_agent(results)
        return {
            "results": [r.to_dict() for r in results],
            "summary": summary,
            "total": len(results),
            "timestamp": datetime.utcnow().isoformat(),
        }

    def submit_feedback(
        self,
        record_id: str,
        classification: str,
        was_correct: bool,
        actual_action_taken: str,
        resolution_notes: str = "",
        resolved_by: str = "user",
    ) -> HistoricalOutcome:
        """
        Record human feedback on a previous classification.
        Stored in S3 for future learning.
        """
        outcome = HistoricalOutcome(
            outcome_id=str(uuid.uuid4()),
            record_id=record_id,
            rule_name="",           # enriched on reload if needed
            table_name="",
            column_name="",
            classification=classification,
            actual_action_taken=actual_action_taken,
            was_correct=was_correct,
            resolution_notes=resolution_notes,
            resolved_by=resolved_by,
            timestamp=datetime.utcnow().isoformat(),
        )
        self._store_outcome(outcome)
        logger.info(f"Feedback stored for record {record_id}: correct={was_correct}")
        return outcome

    def extract_patterns(self, limit: int = 100) -> Dict[str, Any]:
        """
        Run the learning_agent over the most recent *limit* resolved outcomes
        to extract reusable heuristics.
        """
        outcomes = self._load_recent_outcomes(limit=limit)
        if not outcomes:
            return {"message": "No resolved outcomes available for pattern extraction."}
        return self.learning_agent(outcomes)

    # ------------------------------------------------------------------
    # Sub-agents
    # ------------------------------------------------------------------

    def decision_agent(
        self,
        record: ValidationRecord,
        history: List[Dict[str, Any]],
    ) -> AnalysisResult:
        """
        Core classification agent.

        Builds a prompt from the failed record + historical context,
        invokes Bedrock, and parses the structured JSON response.
        """
        prompt = build_decision_prompt(record.to_dict(), history)
        raw = self._invoke_bedrock(prompt, agent_type="decision")
        parsed = self._parse_json(raw)

        classification = ValidationClassification(
            parsed.get("classification", ValidationClassification.NEEDS_INVESTIGATION)
        )
        action_str = parsed.get("recommended_action", RecommendedAction.MONITOR.value)
        try:
            action = RecommendedAction(action_str)
        except ValueError:
            action = RecommendedAction.MONITOR

        return AnalysisResult(
            record_id=record.record_id,
            classification=classification,
            confidence=float(parsed.get("confidence", 0.5)),
            explanation=parsed.get("explanation", ""),
            recommended_action=action,
            root_causes=parsed.get("root_causes", []),
            suggested_next_steps=parsed.get("suggested_next_steps", []),
            validation_sql=parsed.get("validation_sql"),
            similar_historical_failures=history[:5],
            raw_agent_response=raw,
        )

    def learning_agent(self, outcomes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyses resolved outcomes and returns structured learning insights.
        """
        prompt = build_learning_prompt(outcomes)
        raw = self._invoke_bedrock(prompt, agent_type="learning")
        parsed = self._parse_json(raw)

        # Persist the insights as a new learning vector
        vector_id = str(uuid.uuid4())
        vector = {
            "vector_id": vector_id,
            "type": "pattern_extraction",
            "timestamp": datetime.utcnow().isoformat(),
            "outcomes_analysed": len(outcomes),
            "insights": parsed,
        }
        try:
            self.s3.put_object(
                Bucket=self.learning_bucket,
                Key=f"{self.VECTOR_PREFIX}{vector_id}.json",
                Body=json.dumps(vector, indent=2, cls=DateTimeEncoder),
            )
            logger.info(f"Learning vector {vector_id} persisted to S3")
        except Exception as e:
            logger.warning(f"Could not persist learning vector: {e}")

        return parsed

    def batch_agent(self, results: List[AnalysisResult]) -> Dict[str, Any]:
        """
        Generates an executive summary for a batch of AnalysisResults.
        """
        prompt = build_batch_summary_prompt([r.to_dict() for r in results])
        raw = self._invoke_bedrock(prompt, agent_type="batch")
        return self._parse_json(raw)

    # ------------------------------------------------------------------
    # S3 learning data persistence
    # ------------------------------------------------------------------

    def _store_outcome(self, outcome: HistoricalOutcome) -> None:
        """Persist a HistoricalOutcome to S3."""
        try:
            key = f"{self.LEARNING_PREFIX}{outcome.outcome_id}.json"
            self.s3.put_object(
                Bucket=self.learning_bucket,
                Key=key,
                Body=json.dumps(outcome.to_dict(), indent=2, cls=DateTimeEncoder),
            )
            logger.info(f"Outcome {outcome.outcome_id} stored: s3://{self.learning_bucket}/{key}")
        except Exception as e:
            logger.error(f"Failed to store outcome {outcome.outcome_id}: {e}")

    def _store_learning_vector(
        self, record: ValidationRecord, result: AnalysisResult
    ) -> None:
        """Store a compact learning vector after each analysis."""
        vector = {
            "vector_id": str(uuid.uuid4()),
            "type": "analysis",
            "timestamp": datetime.utcnow().isoformat(),
            "features": {
                "rule_name": record.rule_name,
                "table_name": record.table_name,
                "column_name": record.column_name,
                "rule_type": record.rule_type,
                "severity": record.severity,
                "failure_rate": record.failure_rate(),
            },
            "prediction": {
                "classification": result.classification.value,
                "confidence": result.confidence,
                "recommended_action": result.recommended_action.value,
            },
        }
        try:
            key = f"{self.VECTOR_PREFIX}{vector['vector_id']}.json"
            self.s3.put_object(
                Bucket=self.learning_bucket,
                Key=key,
                Body=json.dumps(vector, indent=2, cls=DateTimeEncoder),
            )
        except Exception as e:
            logger.warning(f"Could not persist learning vector: {e}")

    def _load_recent_outcomes(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Load the most recent resolved outcomes from S3."""
        try:
            self.s3.head_bucket(Bucket=self.learning_bucket)
        except Exception:
            logger.info(f"Bucket {self.learning_bucket} not accessible; no outcomes loaded.")
            return []

        try:
            response = self.s3.list_objects_v2(
                Bucket=self.learning_bucket,
                Prefix=self.LEARNING_PREFIX,
                MaxKeys=limit,
            )
            objects = sorted(
                response.get("Contents", []),
                key=lambda x: x["LastModified"],
                reverse=True,
            )
            outcomes = []
            for obj in objects[:limit]:
                try:
                    body = self.s3.get_object(
                        Bucket=self.learning_bucket, Key=obj["Key"]
                    )["Body"].read()
                    outcomes.append(json.loads(body))
                except Exception as e:
                    logger.warning(f"Could not load outcome {obj['Key']}: {e}")
            return outcomes
        except Exception as e:
            logger.error(f"Failed to list outcomes from S3: {e}")
            return []

    def _get_similar_outcomes(
        self, record: ValidationRecord, limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Return historical outcomes that share the same rule_name + table_name
        as the current record, ordered by recency.
        """
        all_outcomes = self._load_recent_outcomes(limit=200)
        similar = [
            o for o in all_outcomes
            if o.get("rule_name") == record.rule_name
            or o.get("table_name") == record.table_name
        ]
        return similar[:limit]

    # ------------------------------------------------------------------
    # Bedrock helper
    # ------------------------------------------------------------------

    def _invoke_bedrock(self, prompt: str, agent_type: str = "decision") -> str:
        """Invoke Bedrock Claude with system + user prompt."""
        agent_system_notes = {
            "decision": "Focus: classify the record accurately. Be concise.",
            "learning": "Focus: extract generalizable patterns from historical data.",
            "batch": "Focus: high-level executive summary. Be brief and actionable.",
        }
        system = SYSTEM_PROMPT + "\n\n" + agent_system_notes.get(agent_type, "")

        try:
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 2048,
                "system": system,
                "messages": [{"role": "user", "content": prompt}],
            }
            response = self.bedrock.invoke_model(
                modelId=self.model_id,
                body=json.dumps(body),
            )
            result = json.loads(response["body"].read())
            return result["content"][0]["text"]
        except Exception as e:
            logger.error(f"Bedrock invocation failed ({agent_type}): {e}")
            return json.dumps({
                "classification": ValidationClassification.NEEDS_INVESTIGATION.value,
                "confidence": 0.0,
                "explanation": f"Agent invocation failed: {e}",
                "root_causes": [],
                "recommended_action": RecommendedAction.ESCALATE.value,
                "suggested_next_steps": ["Check Bedrock connectivity and IAM permissions."],
            })

    def _parse_json(self, raw: str) -> Dict[str, Any]:
        """Extract JSON from a Bedrock response (handles markdown fences, tags)."""
        text = raw.strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        for marker in ("```json", "```"):
            if marker in text:
                start = text.index(marker) + len(marker)
                end = text.rfind("```")
                if end > start:
                    try:
                        return json.loads(text[start:end].strip())
                    except json.JSONDecodeError:
                        pass

        if "<json>" in text and "</json>" in text:
            start = text.index("<json>") + 6
            end = text.index("</json>")
            try:
                return json.loads(text[start:end].strip())
            except json.JSONDecodeError:
                pass

        logger.warning("Could not parse JSON from agent response; returning raw text.")
        return {
            "classification": ValidationClassification.NEEDS_INVESTIGATION.value,
            "confidence": 0.0,
            "explanation": text[:1000],
            "root_causes": [],
            "recommended_action": RecommendedAction.MONITOR.value,
            "suggested_next_steps": [],
            "raw_response": text[:500],
        }
