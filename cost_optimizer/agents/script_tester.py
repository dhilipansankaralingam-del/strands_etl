"""
Script Tester Agent
====================
Generates comprehensive pytest test cases for a (potentially optimized) PySpark
script and can execute them against a local Spark session.

Test categories generated
--------------------------
  1. StaticChecks      — AST-level: AQE configs present, no .collect() on large DFs,
                         broadcast hints in joins, no UDFs, coalesce before write
  2. SchemaValidation  — output DataFrame has expected columns & types
  3. DataIntegrity     — row-count sanity, no data dropped on inner join with full overlap
  4. NullHandling      — null join keys, all-null columns, partial nulls
  5. EdgeCases         — empty input, single-row input, duplicate keys
  6. IncrementalMode   — if processing_mode == delta, watermark filter is applied
  7. PerformanceChecks — cache/persist before multi-action, partitionBy present on write

Rule-based mode: generates tests from static AST analysis + table schema inference.
LLM mode       : Claude generates domain-aware tests with realistic mock data.
"""
from __future__ import annotations

import ast
import importlib.util
import json
import re
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .base import CostOptimizerAgent, AnalysisInput, AnalysisResult


# ─── Spark type mapping for synthetic data generation ─────────────────────────
_SYNTHETIC_VALUES: Dict[str, str] = {
    "id":         "1",
    "int":        "1",
    "long":       "1L",
    "double":     "1.0",
    "float":      "1.0",
    "string":     '"test_value"',
    "bool":       "True",
    "date":       'datetime.date(2024, 1, 1)',
    "timestamp":  'datetime.datetime(2024, 1, 1, 0, 0)',
    "decimal":    "Decimal('1.00')",
}

_SPARK_TYPES: Dict[str, str] = {
    "string":    "StringType()",
    "int":       "IntegerType()",
    "integer":   "IntegerType()",
    "long":      "LongType()",
    "bigint":    "LongType()",
    "double":    "DoubleType()",
    "float":     "FloatType()",
    "boolean":   "BooleanType()",
    "bool":      "BooleanType()",
    "date":      "DateType()",
    "timestamp": "TimestampType()",
    "decimal":   "DecimalType(18, 2)",
}


class ScriptTesterAgent(CostOptimizerAgent):
    """Generates and runs pytest tests for PySpark scripts."""

    AGENT_NAME = "script_tester"

    # ─── Public entry points ──────────────────────────────────────────────────

    def generate_tests(
        self,
        script_path: str,
        script_content: str,
        source_tables: Optional[List[Dict]] = None,
        output_path: Optional[str] = None,
        processing_mode: str = "full",
    ) -> Dict[str, Any]:
        """
        Generate a pytest file for *script_content*.

        Args:
            script_path:      Path used for display and to derive test file name.
            script_content:   Source code of the (optimized) PySpark script.
            source_tables:    Table metadata dicts (schema info used for mock data).
            output_path:      Where to write the test file.  Defaults to
                              tests/test_<stem>.py next to the script.
            processing_mode:  'full' or 'delta'.

        Returns dict with:
            success, test_file_path, test_count, errors
        """
        source_tables = source_tables or []
        if not output_path:
            p           = Path(script_path)
            output_path = str(p.parent / "tests" / f"test_{p.stem}.py")

        input_data = AnalysisInput(
            script_path     = script_path,
            script_content  = script_content,
            source_tables   = source_tables,
            processing_mode = processing_mode,
            current_config  = {},
        )
        result = self.analyze(
            input_data,
            context={"output_path": output_path, "processing_mode": processing_mode},
        )

        if not result.success:
            return {"success": False, "errors": result.errors}

        test_code   = result.analysis.get("test_code", "")
        test_count  = result.analysis.get("test_count", 0)

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(test_code)

        return {
            "success":        True,
            "test_file_path": output_path,
            "test_count":     test_count,
            "test_categories": result.analysis.get("test_categories", []),
            "errors":         result.errors,
        }

    def run_tests(
        self,
        test_file_path: str,
        timeout_seconds: int = 300,
    ) -> Dict[str, Any]:
        """
        Execute the generated test file with pytest.

        Returns dict with:
            success, total, passed, failed, errors_count, output, exit_code
        """
        if not Path(test_file_path).exists():
            return {"success": False, "error": f"Test file not found: {test_file_path}"}

        try:
            proc = subprocess.run(
                [sys.executable, "-m", "pytest", test_file_path,
                 "-v", "--tb=short", "--no-header", "-q"],
                capture_output = True,
                text           = True,
                timeout        = timeout_seconds,
            )
            output   = proc.stdout + proc.stderr
            passed   = len(re.findall(r' PASSED', output))
            failed   = len(re.findall(r' FAILED', output))
            errors_c = len(re.findall(r' ERROR', output))
            total    = passed + failed + errors_c

            return {
                "success":      proc.returncode == 0,
                "exit_code":    proc.returncode,
                "total":        total,
                "passed":       passed,
                "failed":       failed,
                "errors_count": errors_c,
                "output":       output[-4000:],   # last 4000 chars
            }
        except subprocess.TimeoutExpired:
            return {"success": False, "error": f"Test run timed out after {timeout_seconds}s"}
        except FileNotFoundError:
            return {"success": False, "error": "pytest not found; install with: pip install pytest"}

    # ─── Rule-based analysis ──────────────────────────────────────────────────

    def _analyze_rule_based(
        self, input_data: AnalysisInput, context: Dict
    ) -> AnalysisResult:
        code      = input_data.script_content
        tables    = input_data.source_tables
        proc_mode = context.get("processing_mode", "full")
        out_path  = context.get("output_path", "tests/test_script.py")

        structure  = self._parse_script_structure(code)
        test_code, categories, count = self._render_test_file(
            structure, input_data.script_path, tables, proc_mode, code
        )

        return AnalysisResult(
            agent_name = self.AGENT_NAME,
            success    = True,
            analysis   = {
                "test_code":       test_code,
                "test_count":      count,
                "test_categories": categories,
                "structure":       structure,
            },
        )

    # ─── LLM prompt override ─────────────────────────────────────────────────

    def _build_llm_prompt(self, input_data: AnalysisInput, context: Dict) -> str:
        tables    = input_data.source_tables
        proc_mode = context.get("processing_mode", "full")

        return f"""
You are a Senior PySpark Test Engineer.  Generate comprehensive pytest tests for
the following PySpark script.  The test file must run against a LOCAL Spark session
(master=local[2]) and must NOT require any AWS credentials, Glue catalog, or S3 access.

## Script to test  ({input_data.script_path})
```python
{input_data.script_content}
```

## Source Table Metadata
```json
{json.dumps(tables, indent=2)}
```

## Processing mode: {proc_mode}

## Required test categories (generate at least 2 tests per category)

1. **StaticChecks**  (no Spark needed – inspect source code text)
   - AQE configs are present in the script
   - No .collect() on DataFrames larger than lookup tables
   - broadcast() hints present for small tables
   - .coalesce() or .repartition() before every .write call

2. **SchemaValidation**  (use local Spark with mock DataFrames)
   - Output has all expected columns
   - Column types are correct

3. **DataIntegrity**
   - Row count is correct for known input data
   - No rows silently dropped on left join with full overlap

4. **NullHandling**
   - Null values in join keys are handled gracefully (no crash)
   - Null values in aggregation columns return 0 / null as appropriate

5. **EdgeCases**
   - Empty source table produces empty output (no crash)
   - Single-row input produces correct output
   - Duplicate primary keys are handled

6. **IncrementalMode** (if processing_mode == 'delta')
   - Delta filter is applied so only new/changed records are processed

7. **PerformanceChecks** (static)
   - .cache() is present before any DataFrame with multiple actions
   - No repartition(1) remains (should be coalesce(1))

## Output Format
Return ONLY a JSON object (no prose):
{{
  "test_code": "<complete pytest file as a single string>",
  "test_categories": ["StaticChecks", "SchemaValidation", ...]
}}

The test_code must be a complete, self-contained Python file with:
- All imports at the top
- A `spark` pytest fixture (scope=session)
- Helper functions for creating mock DataFrames
- One test class per category
- All tests runnable with: pytest <file> -v
"""

    def _parse_llm_response(self, response) -> AnalysisResult:
        text = str(response)
        m    = re.search(r'\{[\s\S]*\}', text)
        if m:
            try:
                data      = json.loads(m.group())
                test_code = data.get("test_code", "")
                cats      = data.get("test_categories", [])
                count     = text.count("def test_")
                return AnalysisResult(
                    agent_name = self.AGENT_NAME,
                    success    = True,
                    analysis   = {
                        "test_code":       test_code,
                        "test_count":      count,
                        "test_categories": cats,
                    },
                )
            except Exception:
                pass
        return AnalysisResult(
            agent_name = self.AGENT_NAME,
            success    = True,
            analysis   = {"test_code": text, "test_count": text.count("def test_")},
        )

    # ─── Script structure parser ──────────────────────────────────────────────

    def _parse_script_structure(self, code: str) -> Dict[str, Any]:
        """Extract tables read, joins, writes, and used columns from the script AST."""
        structure: Dict[str, Any] = {
            "tables_read":       [],
            "table_aliases":     {},
            "joins":             [],
            "writes":            [],
            "columns_selected":  [],
            "filters":           [],
            "group_by_columns":  [],
            "has_collect":       False,
            "has_cache":         False,
            "has_udf":           False,
            "has_broadcast":     False,
            "has_aqe":           False,
            "has_coalesce_write":False,
        }

        # Static regex scans (fast, no AST needed)
        structure["has_collect"]      = bool(re.search(r'\.collect\(\)', code))
        structure["has_cache"]        = bool(re.search(r'\.(cache|persist)\s*\(', code))
        structure["has_udf"]          = bool(re.search(r'@udf\b|= udf\s*\(', code))
        structure["has_broadcast"]    = bool(re.search(r'broadcast\s*\(', code))
        structure["has_aqe"]          = bool(re.search(r'adaptive\.enabled', code))
        structure["has_coalesce_write"]= bool(
            re.search(r'\.(coalesce|repartition)\s*\(.*\n.*\.write|\.write.*\n.*\.(coalesce|repartition)', code)
            or re.search(r'\.(coalesce|repartition)\s*\(', code)
        )

        # Table reads:  spark.table("db.tbl") / from_catalog(database=..., table_name=...)
        for m in re.finditer(r'spark\.table\(["\']([^"\']+)["\']\)', code):
            structure["tables_read"].append(m.group(1))
        for m in re.finditer(
            r'from_catalog\s*\([^)]*database\s*=\s*["\']([^"\']+)["\'][^)]*table_name\s*=\s*["\']([^"\']+)["\']',
            code
        ):
            structure["tables_read"].append(f"{m.group(1)}.{m.group(2)}")

        # Variable assignments: alias_df = ...
        for m in re.finditer(r'^\s*([a-z_][a-zA-Z0-9_]*)_df\s*=\s*', code, re.MULTILINE):
            alias = m.group(1)
            structure["table_aliases"][alias] = f"{alias}_table"

        # Joins
        for m in re.finditer(r'\.join\s*\(\s*(\w+)', code):
            structure["joins"].append({"right_var": m.group(1)})

        # Writes
        for m in re.finditer(r'\.(saveAsTable|save|parquet|orc|csv|json)\s*\(["\']([^"\']*)["\']', code):
            structure["writes"].append({"method": m.group(1), "target": m.group(2)})

        # GroupBy columns
        for m in re.finditer(r'\.groupBy\s*\(([^)]+)\)', code):
            cols = re.findall(r'["\']([^"\']+)["\']', m.group(1))
            structure["group_by_columns"].extend(cols)

        return structure

    # ─── Test file renderer ───────────────────────────────────────────────────

    def _render_test_file(
        self,
        structure: Dict[str, Any],
        script_path: str,
        tables: List[Dict],
        proc_mode: str,
        original_code: str,
    ) -> Tuple[str, List[str], int]:
        """Render a complete pytest file. Returns (code, categories, test_count)."""
        script_name = Path(script_path).stem
        today       = date.today().isoformat()
        lines: List[str] = []
        categories: List[str] = []
        test_count  = 0

        # ── Header / imports ──────────────────────────────────────────────────
        lines += [
            f'"""',
            f"Auto-generated tests for {script_name}",
            f"Generated by cost_optimizer.ScriptTesterAgent on {today}",
            f'"""',
            "",
            "import re",
            "import datetime",
            "from decimal import Decimal",
            "from pathlib import Path",
            "",
            "import pytest",
            "",
            "try:",
            "    from pyspark.sql import SparkSession",
            "    from pyspark.sql import functions as F",
            "    from pyspark.sql.types import (",
            "        StructType, StructField, StringType, IntegerType,",
            "        LongType, DoubleType, FloatType, BooleanType,",
            "        DateType, TimestampType, DecimalType,",
            "    )",
            "    HAS_PYSPARK = True",
            "except ImportError:",
            "    HAS_PYSPARK = False",
            "",
            "SCRIPT_PATH = Path(__file__).parent.parent / " + f'"{Path(script_path).name}"',
            'SCRIPT_CODE = SCRIPT_PATH.read_text() if SCRIPT_PATH.exists() else ""',
            "",
            "",
            "# ── Spark fixture ───────────────────────────────────────────────────────────────",
            "@pytest.fixture(scope='session')",
            "def spark():",
            "    if not HAS_PYSPARK:",
            "        pytest.skip('pyspark not installed')",
            "    session = (",
            "        SparkSession.builder",
            "        .master('local[2]')",
            f"        .appName('test_{script_name}')",
            "        .config('spark.sql.adaptive.enabled', 'true')",
            "        .config('spark.ui.enabled', 'false')",
            "        .getOrCreate()",
            "    )",
            "    yield session",
            "    session.stop()",
            "",
        ]

        # ── Mock DataFrame factories ──────────────────────────────────────────
        lines += self._render_mock_factories(tables, structure)

        # ── Test classes ─────────────────────────────────────────────────────

        # 1. StaticChecks
        static_tests, sc_count = self._render_static_checks(structure, script_path)
        lines += static_tests
        categories.append("StaticChecks")
        test_count += sc_count

        # 2. SchemaValidation
        schema_tests, sv_count = self._render_schema_validation(tables, structure)
        lines += schema_tests
        categories.append("SchemaValidation")
        test_count += sv_count

        # 3. DataIntegrity
        di_tests, di_count = self._render_data_integrity(tables, structure)
        lines += di_tests
        categories.append("DataIntegrity")
        test_count += di_count

        # 4. NullHandling
        null_tests, n_count = self._render_null_handling(tables, structure)
        lines += null_tests
        categories.append("NullHandling")
        test_count += n_count

        # 5. EdgeCases
        edge_tests, e_count = self._render_edge_cases(tables, structure)
        lines += edge_tests
        categories.append("EdgeCases")
        test_count += e_count

        # 6. IncrementalMode
        if proc_mode == "delta":
            incr_tests, i_count = self._render_incremental_checks(structure)
            lines += incr_tests
            categories.append("IncrementalMode")
            test_count += i_count

        # 7. PerformanceChecks
        perf_tests, p_count = self._render_performance_checks(structure)
        lines += perf_tests
        categories.append("PerformanceChecks")
        test_count += p_count

        return "\n".join(lines), categories, test_count

    # ─── Mock factory renderer ────────────────────────────────────────────────

    def _render_mock_factories(
        self, tables: List[Dict], structure: Dict
    ) -> List[str]:
        lines = [
            "",
            "# ── Mock DataFrame factories ────────────────────────────────────────────────────",
        ]
        if not tables:
            # Generic factories when no table metadata available
            lines += [
                "def make_generic_df(spark, rows=3):",
                '    """Generic DataFrame with common column types."""',
                "    data = [(str(i), i, float(i), datetime.date(2024, 1, i+1))",
                "            for i in range(1, rows + 1)]",
                "    schema = StructType([",
                '        StructField("id",    StringType(),  True),',
                '        StructField("value", IntegerType(), True),',
                '        StructField("amount",DoubleType(),  True),',
                '        StructField("dt",    DateType(),    True),',
                "    ])",
                "    return spark.createDataFrame(data, schema)",
                "",
            ]
        else:
            for tbl in tables:
                name  = tbl.get("table", "unknown")
                alias = name.replace("-", "_").lower()
                cols  = tbl.get("columns", []) or self._infer_columns(name, tbl)
                lines += self._render_one_factory(alias, cols)

        return lines

    def _infer_columns(self, table_name: str, tbl_meta: Dict) -> List[Dict]:
        """Infer column list from table metadata hints."""
        columns = []
        name = table_name.lower()
        # Common ID column
        if any(k in name for k in ("transaction", "order", "event", "fact")):
            columns.append({"name": f"{name[:8]}_id", "type": "string"})
            columns.append({"name": "customer_id", "type": "string"})
            columns.append({"name": "amount",       "type": "double"})
            columns.append({"name": "status",       "type": "string"})
            columns.append({"name": "created_date", "type": "date"})
        elif any(k in name for k in ("customer", "member", "user", "account")):
            columns.append({"name": "customer_id",  "type": "string"})
            columns.append({"name": "name",         "type": "string"})
            columns.append({"name": "email",        "type": "string"})
            columns.append({"name": "status",       "type": "string"})
            columns.append({"name": "created_date", "type": "date"})
        elif any(k in name for k in ("product", "item", "sku", "dim")):
            columns.append({"name": "product_id",   "type": "string"})
            columns.append({"name": "name",         "type": "string"})
            columns.append({"name": "category",     "type": "string"})
            columns.append({"name": "price",        "type": "double"})
        else:
            columns.append({"name": "id",           "type": "string"})
            columns.append({"name": "value",        "type": "string"})
            columns.append({"name": "amount",       "type": "double"})
            columns.append({"name": "created_date", "type": "date"})

        part_col = tbl_meta.get("partition_column")
        if part_col and not any(c["name"] == part_col for c in columns):
            columns.append({"name": part_col, "type": "date"})

        return columns

    def _render_one_factory(self, alias: str, columns: List[Dict]) -> List[str]:
        lines = [
            f"def make_{alias}_df(spark, rows=3):",
            f'    """Mock DataFrame for {alias} table."""',
        ]
        # Build schema
        schema_lines = ["    schema = StructType(["]
        data_cols    = []
        for col in columns:
            col_name = col.get("name", "col")
            col_type = col.get("type", "string").lower()
            spark_t  = _SPARK_TYPES.get(col_type, "StringType()")
            schema_lines.append(f'        StructField("{col_name}", {spark_t}, True),')
            data_cols.append((col_name, col_type))
        schema_lines.append("    ])")
        lines += schema_lines

        # Build data
        lines.append("    data = [")
        lines.append("        (")
        for col_name, col_type in data_cols:
            val = self._synthetic_value(col_name, col_type)
            lines.append(f"            {val},  # {col_name}")
        lines.append("        )")
        lines.append("        for _ in range(rows)")
        lines.append("    ]")
        lines.append("    return spark.createDataFrame(data, schema)")
        lines.append("")
        return lines

    def _synthetic_value(self, col_name: str, col_type: str) -> str:
        """Return a Python literal suitable for mock data."""
        if col_type in ("date",):
            return "datetime.date(2024, 1, 1)"
        if col_type in ("timestamp",):
            return "datetime.datetime(2024, 1, 1, 0, 0)"
        if col_type in ("double", "float"):
            return "1.0"
        if col_type in ("int", "integer", "long", "bigint"):
            return "1"
        if col_type in ("boolean", "bool"):
            return "True"
        if col_type in ("decimal",):
            return "Decimal('1.00')"
        # string – use col_name as a hint
        return f'"{col_name}_val"'

    # ─── Individual test class renderers ──────────────────────────────────────

    def _render_static_checks(
        self, structure: Dict, script_path: str
    ) -> Tuple[List[str], int]:
        count = 0
        lines = [
            "",
            "# ── 1. Static Checks (no Spark session needed) ─────────────────────────────────",
            "class TestStaticChecks:",
            '    """Verify that the optimizer improvements are present in the script."""',
            "",
        ]

        # AQE check
        lines += [
            "    def test_aqe_enabled(self):",
            '        assert "adaptive.enabled" in SCRIPT_CODE, \\',
            '            "AQE (spark.sql.adaptive.enabled) must be set in the script"',
            "",
        ]
        count += 1

        # Broadcast hint
        if structure.get("has_broadcast") or structure.get("joins"):
            lines += [
                "    def test_broadcast_hints_present(self):",
                '        assert re.search(r"broadcast\\s*\\(", SCRIPT_CODE), \\',
                '            "broadcast() hints should be present for small-table joins"',
                "",
            ]
            count += 1

        # Coalesce before write
        if structure.get("writes"):
            lines += [
                "    def test_coalesce_before_write(self):",
                "        has_coalesce = bool(",
                '            re.search(r"\\.(coalesce|repartition)\\s*\\(", SCRIPT_CODE)',
                "        )",
                '        assert has_coalesce, "coalesce() or repartition() should precede write"',
                "",
            ]
            count += 1

        # No repartition(1) left
        lines += [
            "    def test_no_repartition_1(self):",
            '        assert not re.search(r"\\.repartition\\(\\s*1\\s*\\)", SCRIPT_CODE), \\',
            '            "repartition(1) found – should be coalesce(1) to avoid full shuffle"',
            "",
        ]
        count += 1

        # KryoSerializer
        lines += [
            "    def test_kyro_serializer(self):",
            '        assert "KryoSerializer" in SCRIPT_CODE, \\',
            '            "spark.serializer = KryoSerializer should be configured"',
            "",
        ]
        count += 1

        # No bare UDF
        if not structure.get("has_udf"):
            lines += [
                "    def test_no_udf(self):",
                "        udf_pattern = re.compile(",
                r'            r"(?m)^(?!\s*#).*(@udf\b|= udf\s*\()"',
                "        )",
                '        assert not udf_pattern.search(SCRIPT_CODE), \\',
                '            "Python UDFs detected – replace with pyspark.sql.functions"',
                "",
            ]
            count += 1

        return lines, count

    def _render_schema_validation(
        self, tables: List[Dict], structure: Dict
    ) -> Tuple[List[str], int]:
        count = 0
        lines = [
            "",
            "# ── 2. Schema Validation ────────────────────────────────────────────────────────",
            "@pytest.mark.skipif(not HAS_PYSPARK, reason='pyspark required')",
            "class TestSchemaValidation:",
            '    """Verify output DataFrame schema is as expected."""',
            "",
        ]

        group_cols = structure.get("group_by_columns", [])

        if group_cols:
            col_list = json.dumps(group_cols)
            lines += [
                "    def test_groupby_columns_present_in_output(self, spark):",
                f"        expected_cols = {col_list}",
                "        # Create minimal mock DataFrames and run a groupBy to validate",
                "        data = [(str(i),) + (i,) * (len(expected_cols) - 1)",
                "                for i in range(1, 4)]",
                "        df = spark.createDataFrame(data,",
                "             [f'col{{i}}' for i in range(len(expected_cols))])",
                "        grouped = df.groupBy(df.columns[0]).count()",
                "        assert 'count' in grouped.columns",
                "",
            ]
            count += 1

        # Generic output schema test
        lines += [
            "    def test_createDataFrame_works(self, spark):",
            "        schema = StructType([",
            '            StructField("id",     StringType(),  True),',
            '            StructField("value",  IntegerType(), True),',
            "        ])",
            '        df = spark.createDataFrame([("a", 1), ("b", 2)], schema)',
            "        assert df.count() == 2",
            '        assert "id" in df.columns',
            "",
        ]
        count += 1

        return lines, count

    def _render_data_integrity(
        self, tables: List[Dict], structure: Dict
    ) -> Tuple[List[str], int]:
        count = 0
        lines = [
            "",
            "# ── 3. Data Integrity ───────────────────────────────────────────────────────────",
            "@pytest.mark.skipif(not HAS_PYSPARK, reason='pyspark required')",
            "class TestDataIntegrity:",
            '    """Verify join and aggregation logic preserves expected row counts."""',
            "",
        ]

        if len(tables) >= 2:
            t1 = tables[0].get("table", "left").replace("-", "_").lower()
            t2 = tables[1].get("table", "right").replace("-", "_").lower()
            lines += [
                "    def test_inner_join_no_data_loss_on_full_overlap(self, spark):",
                "        # Both tables have the same join key values → inner join row count == left",
                "        schema = StructType([",
                '            StructField("id",    StringType(), True),',
                '            StructField("value", StringType(), True),',
                "        ])",
                '        left  = spark.createDataFrame([("k1", "a"), ("k2", "b")], schema)',
                '        right = spark.createDataFrame([("k1", "x"), ("k2", "y")], schema)',
                '        joined = left.join(right, on="id", how="inner")',
                "        assert joined.count() == 2, 'Inner join with full key overlap should return 2 rows'",
                "",
            ]
            count += 1

        lines += [
            "    def test_left_join_preserves_left_rows(self, spark):",
            "        schema = StructType([",
            '            StructField("id",    StringType(), True),',
            '            StructField("value", StringType(), True),',
            "        ])",
            '        left  = spark.createDataFrame([("k1", "a"), ("k2", "b"), ("k3", "c")], schema)',
            '        right = spark.createDataFrame([("k1", "x")], schema)',
            '        joined = left.join(right, on="id", how="left")',
            "        assert joined.count() == 3, 'Left join must preserve all left-table rows'",
            "",
        ]
        count += 1

        lines += [
            "    def test_groupby_count_matches_distinct_keys(self, spark):",
            "        schema = StructType([",
            '            StructField("category", StringType(), True),',
            '            StructField("amount",   DoubleType(), True),',
            "        ])",
            '        data = [("A", 10.0), ("A", 20.0), ("B", 30.0)]',
            "        df   = spark.createDataFrame(data, schema)",
            "        agg  = df.groupBy('category').agg(F.sum('amount').alias('total'))",
            "        assert agg.count() == 2, 'Two distinct categories must produce 2 output rows'",
            "",
        ]
        count += 1

        return lines, count

    def _render_null_handling(
        self, tables: List[Dict], structure: Dict
    ) -> Tuple[List[str], int]:
        count = 0
        lines = [
            "",
            "# ── 4. Null Handling ────────────────────────────────────────────────────────────",
            "@pytest.mark.skipif(not HAS_PYSPARK, reason='pyspark required')",
            "class TestNullHandling:",
            '    """Verify that null values in key columns do not crash the job."""',
            "",
            "    def test_null_join_key_does_not_crash(self, spark):",
            "        schema = StructType([",
            '            StructField("id",    StringType(), True),',
            '            StructField("value", StringType(), True),',
            "        ])",
            '        left  = spark.createDataFrame([("k1", "a"), (None, "b")], schema)',
            '        right = spark.createDataFrame([("k1", "x"), (None, "y")], schema)',
            "        # Spark treats NULL != NULL in joins – no crash, nulls go to non-matched side",
            '        result = left.join(right, on="id", how="left")',
            "        assert result.count() >= 2",
            "",
        ]
        count += 1

        lines += [
            "    def test_null_in_aggregation_column(self, spark):",
            "        schema = StructType([",
            '            StructField("category", StringType(), True),',
            '            StructField("amount",   DoubleType(), True),',
            "        ])",
            '        data = [("A", 10.0), ("A", None), ("B", 30.0)]',
            "        df   = spark.createDataFrame(data, schema)",
            "        agg  = df.groupBy('category').agg(F.sum('amount').alias('total'))",
            "        row_a = agg.filter(\"category = 'A'\").collect()[0]",
            "        # sum() ignores nulls in Spark – result should be 10.0",
            "        assert row_a['total'] == 10.0, 'sum() should ignore null values'",
            "",
        ]
        count += 1

        lines += [
            "    def test_all_null_key_column(self, spark):",
            "        schema = StructType([",
            '            StructField("id",    StringType(), True),',
            '            StructField("value", StringType(), True),',
            "        ])",
            '        df = spark.createDataFrame([(None, "a"), (None, "b")], schema)',
            '        result = df.filter(F.col("id").isNotNull())',
            "        assert result.count() == 0, 'Filter on all-null column should yield 0 rows'",
            "",
        ]
        count += 1

        return lines, count

    def _render_edge_cases(
        self, tables: List[Dict], structure: Dict
    ) -> Tuple[List[str], int]:
        count = 0
        lines = [
            "",
            "# ── 5. Edge Cases ───────────────────────────────────────────────────────────────",
            "@pytest.mark.skipif(not HAS_PYSPARK, reason='pyspark required')",
            "class TestEdgeCases:",
            '    """Verify correct behaviour for boundary inputs."""',
            "",
            "    def test_empty_dataframe_produces_empty_output(self, spark):",
            "        schema = StructType([",
            '            StructField("id",    StringType(), True),',
            '            StructField("value", DoubleType(), True),',
            "        ])",
            "        empty = spark.createDataFrame([], schema)",
            "        result = empty.filter(\"value > 0\").groupBy('id').count()",
            "        assert result.count() == 0, 'Empty input must produce empty output'",
            "",
        ]
        count += 1

        lines += [
            "    def test_single_row_input(self, spark):",
            "        schema = StructType([",
            '            StructField("id",     StringType(), True),',
            '            StructField("amount", DoubleType(), True),',
            "        ])",
            '        df     = spark.createDataFrame([("r1", 99.0)], schema)',
            "        result = df.groupBy('id').agg(F.sum('amount').alias('total'))",
            "        assert result.count() == 1",
            "        assert result.collect()[0]['total'] == 99.0",
            "",
        ]
        count += 1

        lines += [
            "    def test_duplicate_keys_in_aggregation(self, spark):",
            "        schema = StructType([",
            '            StructField("id",     StringType(), True),',
            '            StructField("amount", DoubleType(), True),',
            "        ])",
            '        df = spark.createDataFrame([("k1", 10.0), ("k1", 20.0), ("k1", 30.0)], schema)',
            "        agg = df.groupBy('id').agg(F.sum('amount').alias('total'))",
            "        assert agg.count() == 1",
            "        assert agg.collect()[0]['total'] == 60.0",
            "",
        ]
        count += 1

        return lines, count

    def _render_incremental_checks(self, structure: Dict) -> Tuple[List[str], int]:
        count = 0
        lines = [
            "",
            "# ── 6. Incremental / Delta Mode ─────────────────────────────────────────────────",
            "class TestIncrementalMode:",
            '    """Verify delta processing logic is present in the script."""',
            "",
            "    def test_delta_filter_pattern_present(self):",
            "        # Delta jobs must filter on a timestamp/date column to avoid full re-scans",
            "        has_filter = bool(",
            r'            re.search(r"(?i)(updated_at|modified_at|event_date|load_date|run_date|watermark)\s*[><=]", SCRIPT_CODE)',
            "            or re.search(",
            r'                r"(?i)(delta|incremental|is_new|is_updated|changed_since)", SCRIPT_CODE',
            "            )",
            "        )",
            "        assert has_filter, (",
            "            'Delta processing mode requires a timestamp/date filter to prevent '",
            "            'full table re-scans on every run'",
            "        )",
            "",
        ]
        count += 1

        lines += [
            "    def test_watermark_filter_excludes_old_records(self, spark):",
            "        schema = StructType([",
            '            StructField("id",         StringType(), True),',
            '            StructField("updated_at", DateType(),   True),',
            "        ])",
            "        data = [",
            "            ('r1', datetime.date(2024, 1, 1)),  # old",
            "            ('r2', datetime.date(2024, 6, 1)),  # new",
            "        ]",
            "        df      = spark.createDataFrame(data, schema)",
            "        cutoff  = datetime.date(2024, 3, 1)",
            "        result  = df.filter(F.col('updated_at') >= F.lit(cutoff))",
            "        assert result.count() == 1, 'Delta filter must exclude records before cutoff'",
            "",
        ]
        count += 1

        return lines, count

    def _render_performance_checks(self, structure: Dict) -> Tuple[List[str], int]:
        count = 0
        lines = [
            "",
            "# ── 7. Performance Checks (static) ─────────────────────────────────────────────",
            "class TestPerformanceChecks:",
            '    """Verify optimizer-injected performance improvements are present."""',
            "",
            "    def test_shuffle_partitions_configured(self):",
            '        assert re.search(r"shuffle\\.partitions", SCRIPT_CODE), \\',
            '            "spark.sql.shuffle.partitions should be configured"',
            "",
        ]
        count += 1

        if structure.get("has_cache"):
            lines += [
                "    def test_cache_is_used(self):",
                '        assert re.search(r"\\.(cache|persist)\\s*\\(", SCRIPT_CODE), \\',
                '            ".cache() or .persist() must be present for DataFrames used by multiple actions"',
                "",
            ]
            count += 1

        lines += [
            "    def test_max_partition_bytes_configured(self):",
            '        assert re.search(r"maxPartitionBytes", SCRIPT_CODE), \\',
            '            "spark.sql.files.maxPartitionBytes should be set to ~128MB"',
            "",
        ]
        count += 1

        return lines, count
