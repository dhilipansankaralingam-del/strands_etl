"""
Athena Query Engine for the Strands Validation Framework.

Responsibilities:
  1. Discover the Glue Catalog schema (databases, tables, columns).
  2. Convert a natural-language query to Athena SQL via Bedrock.
  3. Submit the SQL to Athena, poll until complete, and return structured results.
  4. Cache schema metadata to avoid repeated Glue API calls.
"""

import json
import time
import logging
import boto3
from typing import Dict, List, Any, Optional

from validation.models import AthenaQueryResult
from validation.prompts import build_sql_prompt, SYSTEM_PROMPT

logger = logging.getLogger(__name__)


class AthenaQueryEngine:
    """
    Translates natural-language questions into Athena SQL and executes them.

    Usage:
        engine = AthenaQueryEngine(
            database="validation_db",
            s3_output="s3://my-bucket/athena-results/",
        )
        result = engine.nl_query("Show me all failed NOT_NULL rules in the last 7 days")
        print(result.to_markdown_table())
    """

    DEFAULT_MODEL_ID = "anthropic.claude-3-sonnet-20240229-v1:0"
    MAX_POLL_ATTEMPTS = 60   # 60 × 5 s = 5 min timeout
    POLL_INTERVAL_S = 5

    def __init__(
        self,
        database: str,
        s3_output_location: str,
        aws_region: str = "us-east-1",
        workgroup: str = "primary",
        model_id: str = DEFAULT_MODEL_ID,
    ):
        self.database = database
        self.s3_output_location = s3_output_location
        self.workgroup = workgroup
        self.model_id = model_id

        self.athena = boto3.client("athena", region_name=aws_region)
        self.glue = boto3.client("glue", region_name=aws_region)
        self.bedrock = boto3.client("bedrock-runtime", region_name=aws_region)

        # Schema cache: {database: {table_name: [{"name": col, "type": type}, ...]}}
        self._schema_cache: Dict[str, Dict[str, List[Dict[str, str]]]] = {}

        # Conversation history for multi-turn SQL context
        self._conversation_history: List[Dict[str, str]] = []

    # ------------------------------------------------------------------
    # Schema discovery
    # ------------------------------------------------------------------

    def get_schema(self, database: Optional[str] = None, refresh: bool = False) -> Dict[str, Any]:
        """
        Return the Glue Catalog schema for *database* as a dict:
          {table_name: [{"name": col_name, "type": col_type}, ...]}

        Results are cached; pass refresh=True to force a reload.
        """
        db = database or self.database
        if db in self._schema_cache and not refresh:
            return self._schema_cache[db]

        schema: Dict[str, List[Dict[str, str]]] = {}
        try:
            paginator = self.glue.get_paginator("get_tables")
            for page in paginator.paginate(DatabaseName=db):
                for table in page.get("TableList", []):
                    tname = table["Name"]
                    cols = [
                        {"name": c["Name"], "type": c["Type"]}
                        for c in table.get("StorageDescriptor", {}).get("Columns", [])
                    ]
                    # Also include partition keys
                    cols += [
                        {"name": c["Name"], "type": c["Type"], "partition": True}
                        for c in table.get("PartitionKeys", [])
                    ]
                    schema[tname] = cols
            self._schema_cache[db] = schema
            logger.info(f"Schema loaded for database '{db}': {len(schema)} tables")
        except Exception as e:
            logger.warning(f"Could not load Glue schema for '{db}': {e}. Using empty schema.")
            self._schema_cache[db] = {}

        return self._schema_cache[db]

    def list_databases(self) -> List[str]:
        """Return all Glue Catalog database names."""
        try:
            paginator = self.glue.get_paginator("get_databases")
            dbs = []
            for page in paginator.paginate():
                dbs.extend(d["Name"] for d in page.get("DatabaseList", []))
            return dbs
        except Exception as e:
            logger.warning(f"Could not list Glue databases: {e}")
            return []

    # ------------------------------------------------------------------
    # NL → SQL via Bedrock
    # ------------------------------------------------------------------

    def nl_to_sql(self, nl_query: str, database: Optional[str] = None) -> Dict[str, Any]:
        """
        Convert *nl_query* to an Athena SQL statement using Bedrock.

        Returns a dict with keys: sql, explanation, tables_used, assumed_intent
        (or {"error": "..."} on failure).
        """
        db = database or self.database
        schema = self.get_schema(db)
        prompt = build_sql_prompt(
            nl_query=nl_query,
            database=db,
            schema=schema,
            conversation_history=self._conversation_history,
        )

        raw_response = self._invoke_bedrock(prompt, agent_type="sql")

        # Parse JSON out of the response
        parsed = self._parse_json_response(raw_response)

        # Record in conversation history for multi-turn context
        self._conversation_history.append({"role": "user", "content": nl_query})
        self._conversation_history.append({
            "role": "assistant",
            "content": parsed.get("sql", raw_response)[:500],
        })

        return parsed

    # ------------------------------------------------------------------
    # Athena execution
    # ------------------------------------------------------------------

    def execute_sql(self, sql: str, nl_query: str = "") -> AthenaQueryResult:
        """
        Submit *sql* to Athena, wait for completion, and return an AthenaQueryResult.
        """
        execution_id = self._start_query(sql)
        status, stats = self._wait_for_query(execution_id)

        if status != "SUCCEEDED":
            reason = stats.get("QueryExecution", {}).get(
                "Status", {}
            ).get("StateChangeReason", "Unknown error")
            return AthenaQueryResult(
                query_execution_id=execution_id,
                sql=sql,
                nl_query=nl_query,
                columns=[],
                rows=[],
                row_count=0,
                execution_time_ms=0,
                data_scanned_bytes=0,
                status=status,
                error_message=reason,
            )

        columns, rows = self._fetch_results(execution_id)
        engine_stats = stats.get("QueryExecution", {}).get("Statistics", {})

        return AthenaQueryResult(
            query_execution_id=execution_id,
            sql=sql,
            nl_query=nl_query,
            columns=columns,
            rows=rows,
            row_count=len(rows),
            execution_time_ms=engine_stats.get("TotalExecutionTimeInMillis", 0),
            data_scanned_bytes=engine_stats.get("DataScannedInBytes", 0),
            status="SUCCEEDED",
        )

    def nl_query(self, nl_query: str, database: Optional[str] = None) -> AthenaQueryResult:
        """
        End-to-end: NL query → SQL → Athena execution → AthenaQueryResult.
        """
        sql_response = self.nl_to_sql(nl_query, database)

        if "error" in sql_response:
            return AthenaQueryResult(
                query_execution_id="",
                sql="",
                nl_query=nl_query,
                columns=[],
                rows=[],
                row_count=0,
                execution_time_ms=0,
                data_scanned_bytes=0,
                status="FAILED",
                error_message=sql_response["error"],
            )

        sql = sql_response.get("sql", "")
        if not sql:
            return AthenaQueryResult(
                query_execution_id="",
                sql="",
                nl_query=nl_query,
                columns=[],
                rows=[],
                row_count=0,
                execution_time_ms=0,
                data_scanned_bytes=0,
                status="FAILED",
                error_message="Bedrock returned empty SQL.",
            )

        logger.info(f"Executing NL query: '{nl_query}' → SQL: {sql[:200]}")
        return self.execute_sql(sql, nl_query=nl_query)

    def reset_conversation(self) -> None:
        """Clear the multi-turn conversation history."""
        self._conversation_history = []

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _start_query(self, sql: str) -> str:
        response = self.athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": self.s3_output_location},
            WorkGroup=self.workgroup,
        )
        return response["QueryExecutionId"]

    def _wait_for_query(self, execution_id: str) -> tuple:
        """Poll until terminal state. Returns (status_str, full_stats_dict)."""
        terminal_states = {"SUCCEEDED", "FAILED", "CANCELLED"}
        for attempt in range(self.MAX_POLL_ATTEMPTS):
            stats = self.athena.get_query_execution(QueryExecutionId=execution_id)
            state = stats["QueryExecution"]["Status"]["State"]
            if state in terminal_states:
                logger.info(f"Athena query {execution_id} finished: {state}")
                return state, stats
            time.sleep(self.POLL_INTERVAL_S)
        # Timed out
        logger.error(f"Athena query {execution_id} timed out after {self.MAX_POLL_ATTEMPTS * self.POLL_INTERVAL_S}s")
        return "FAILED", {}

    def _fetch_results(self, execution_id: str) -> tuple:
        """
        Page through Athena GetQueryResults and return (columns, rows).
        rows is a list of {column_name: value} dicts.
        """
        columns: List[str] = []
        rows: List[Dict[str, str]] = []
        next_token: Optional[str] = None
        first_page = True

        while True:
            kwargs: Dict[str, Any] = {
                "QueryExecutionId": execution_id,
                "MaxResults": 1000,
            }
            if next_token:
                kwargs["NextToken"] = next_token

            response = self.athena.get_query_results(**kwargs)

            # First page: extract column names from ResultSetMetadata
            if first_page:
                columns = [
                    col["Label"]
                    for col in response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
                ]
                first_page = False

            result_rows = response["ResultSet"]["Rows"]
            # Skip the header row on the first page
            data_rows = result_rows[1:] if not rows and result_rows else result_rows

            for row in data_rows:
                values = [d.get("VarCharValue", "") for d in row.get("Data", [])]
                rows.append(dict(zip(columns, values)))

            next_token = response.get("NextToken")
            if not next_token:
                break

        return columns, rows

    def _invoke_bedrock(self, prompt: str, agent_type: str = "sql") -> str:
        """Invoke Bedrock Claude with the given prompt and return the text response."""
        try:
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 2048,
                "system": SYSTEM_PROMPT,
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
            return json.dumps({"error": str(e)})

    def _parse_json_response(self, raw: str) -> Dict[str, Any]:
        """
        Extract and parse JSON from a Bedrock text response.
        Handles: plain JSON, ```json ... ```, and <json>...</json> wrapping.
        """
        text = raw.strip()

        # Try direct parse first
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # Try markdown code block
        for marker in ("```json", "```"):
            if marker in text:
                start = text.index(marker) + len(marker)
                end = text.rfind("```")
                if end > start:
                    try:
                        return json.loads(text[start:end].strip())
                    except json.JSONDecodeError:
                        pass

        # Try <json>...</json>
        if "<json>" in text and "</json>" in text:
            start = text.index("<json>") + 6
            end = text.index("</json>")
            try:
                return json.loads(text[start:end].strip())
            except json.JSONDecodeError:
                pass

        # Give up – return raw text as error
        logger.warning("Could not parse JSON from Bedrock response")
        return {"error": "Could not parse JSON from model response", "raw": text[:500]}
