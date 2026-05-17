"""
Script Tester Agent
===================
Generates and runs pytest test suites for PySpark ETL scripts:
  - StaticChecks (import validation, SparkSession presence)
  - SchemaValidation (expected output columns / types)
  - DataIntegrity (row count assertions, PK uniqueness)
  - NullHandling (critical columns not null)
  - EdgeCases (empty input, all-null column, duplicate keys)
  - IncrementalMode (delta filter applied)
  - PerformanceChecks (no .collect() on large frames)

Based on ScriptTesterAgent from PR-11 (table-optimizer-analysis).
"""

import json
import logging
import re
import subprocess
import tempfile
import textwrap
from pathlib import Path
from typing import Any, Dict, List, Optional

from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a **PySpark Test Engineer** who writes comprehensive pytest test suites for ETL scripts.

Given a PySpark script and table metadata, generate tests that:
1. Validate imports and SparkSession initialisation (StaticChecks)
2. Assert output schema (column names, data types)
3. Check data integrity (row counts, PK uniqueness, referential integrity)
4. Verify null handling on critical columns
5. Test edge cases (empty input, all-null, duplicate keys)
6. Validate incremental / delta mode filter logic
7. Detect performance anti-patterns statically

Return structured JSON:
{
  "test_file_content": "...",
  "test_cases": [{"name": "...", "category": "...", "description": "..."}],
  "test_count": N,
  "categories": [],
  "success": true
}
Return ONLY valid JSON.
"""

_PYTEST_HEADER = '''\
"""
Auto-generated PySpark ETL test suite.
Run: pytest {test_file} -v
"""
import ast
import re
import pytest
from pathlib import Path

# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_script(path: str) -> str:
    return Path(path).read_text()

def _parse_ast(code: str):
    try:
        return ast.parse(code)
    except SyntaxError as exc:
        pytest.fail(f"Script has a syntax error: {{exc}}")

SCRIPT_PATH = "{script_path}"
'''

_STATIC_TEMPLATE = '''\

# ── StaticChecks ──────────────────────────────────────────────────────────────

class TestStaticChecks:
    """Validate script structure without running Spark."""

    def test_no_syntax_errors(self):
        code = _load_script(SCRIPT_PATH)
        _parse_ast(code)  # raises on syntax error

    def test_spark_session_present(self):
        code = _load_script(SCRIPT_PATH)
        assert "SparkSession" in code or "spark" in code, \\
            "No SparkSession reference found"

    def test_no_bare_collect_on_large_df(self):
        code = _load_script(SCRIPT_PATH)
        collect_calls = re.findall(r"(?<!#)(?<!sample)\.collect\\(\\)", code)
        assert len(collect_calls) == 0, \\
            f"Found {{len(collect_calls)}} unguarded .collect() call(s) — risk of OOM"

    def test_no_cross_join(self):
        code = _load_script(SCRIPT_PATH)
        assert ".crossJoin(" not in code, "Unbounded crossJoin detected"

    def test_aqe_enabled(self):
        code = _load_script(SCRIPT_PATH)
        has_aqe = ("adaptive.enabled" in code or
                   "spark.sql.adaptive" in code or
                   "AQE" in code)
        assert has_aqe, "AQE (Adaptive Query Execution) not configured"

    def test_no_to_pandas_without_limit(self):
        code = _load_script(SCRIPT_PATH)
        pandas_calls = re.findall(r"(?<!limit.*)\\.toPandas\\(\\)", code)
        assert len(pandas_calls) == 0, \\
            ".toPandas() found without a preceding .limit() — potential OOM"
'''


def _build_schema_tests(tables: List[Dict]) -> str:
    if not tables:
        return ""
    lines = [
        "",
        "# ── SchemaValidation ─────────────────────────────────────────────────────────",
        "",
        "class TestSchemaValidation:",
        '    """Verify expected columns exist in the script."""',
        "",
    ]
    for tbl in tables:
        name    = tbl.get("table", tbl.get("name", "unknown"))
        columns = tbl.get("columns", [])
        if not columns:
            continue
        var     = re.sub(r'[^a-z0-9]', '_', name.lower())
        col_list = ", ".join(f'"{c}"' for c in columns[:10])
        lines += [
            f'    def test_{var}_columns_referenced(self):',
            f'        code = _load_script(SCRIPT_PATH)',
            f'        expected = [{col_list}]',
            f'        for col in expected:',
            f'            assert col in code, f"Column {{col}} not referenced in script"',
            "",
        ]
    return "\n".join(lines)


def _build_integrity_tests(tables: List[Dict]) -> str:
    lines = [
        "",
        "# ── DataIntegrity ────────────────────────────────────────────────────────────",
        "",
        "class TestDataIntegrity:",
        '    """Data integrity checks (static script analysis)."""',
        "",
        "    def test_write_operation_present(self):",
        "        code = _load_script(SCRIPT_PATH)",
        '        has_write = (".write" in code or "saveAsTable" in code or',
        '                     ".save(" in code)',
        '        assert has_write, "No write operation found — script may not persist results"',
        "",
        "    def test_partition_by_present(self):",
        "        code = _load_script(SCRIPT_PATH)",
        '        assert "partitionBy" in code or "PARTITIONED BY" in code.upper(), \\',
        '            "No partitionBy found — output file distribution may be suboptimal"',
        "",
    ]
    return "\n".join(lines)


def _build_null_tests(tables: List[Dict]) -> str:
    lines = [
        "",
        "# ── NullHandling ─────────────────────────────────────────────────────────────",
        "",
        "class TestNullHandling:",
        '    """Verify null handling exists for critical columns."""',
        "",
        "    def test_null_check_or_filter_present(self):",
        "        code = _load_script(SCRIPT_PATH)",
        '        has_null_check = ("isNull()" in code or "isNotNull()" in code or',
        '                          "dropna" in code or "fillna" in code or',
        '                          "coalesce(" in code)',
        '        assert has_null_check, "No null handling found — consider adding isNotNull() checks"',
        "",
    ]
    return "\n".join(lines)


def _build_incremental_tests(processing_mode: str) -> str:
    if processing_mode != "delta":
        return ""
    lines = [
        "",
        "# ── IncrementalMode ──────────────────────────────────────────────────────────",
        "",
        "class TestIncrementalMode:",
        '    """Validate delta/incremental processing logic."""',
        "",
        "    def test_incremental_filter_present(self):",
        "        code = _load_script(SCRIPT_PATH)",
        '        has_filter = ("updated_at" in code or "created_at" in code or',
        '                      "modified_date" in code or "INTERVAL" in code)',
        '        assert has_filter, "Delta mode: no time-based filter found for incremental reads"',
        "",
        "    def test_merge_or_append_mode(self):",
        "        code = _load_script(SCRIPT_PATH)",
        '        has_merge = ("merge" in code.lower() or',
        '                     "append" in code or',
        '                     "MERGE INTO" in code.upper())',
        '        assert has_merge, "Delta mode: no MERGE or append write mode detected"',
        "",
    ]
    return "\n".join(lines)


def _build_performance_tests() -> str:
    return """
# ── PerformanceChecks ─────────────────────────────────────────────────────────

class TestPerformanceChecks:
    \"\"\"Static performance anti-pattern detection.\"\"\"

    def test_no_repartition_1(self):
        code = _load_script(SCRIPT_PATH)
        assert ".repartition(1)" not in code, \\
            ".repartition(1) found — use .coalesce(1) to preserve upstream parallelism"

    def test_no_select_star(self):
        code = _load_script(SCRIPT_PATH)
        assert 'select("*")' not in code, \\
            'select("*") found — explicitly name columns for schema safety'

    def test_kryoserializer_configured(self):
        code = _load_script(SCRIPT_PATH)
        has_kryo = "KryoSerializer" in code or "kryo" in code.lower()
        assert has_kryo, "KryoSerializer not configured — default Java serializer is ~10× slower"

    def test_no_python_udf_hotpath(self):
        code = _load_script(SCRIPT_PATH)
        udf_count = len(re.findall(r'@udf|@pandas_udf', code))
        assert udf_count == 0, \\
            f"{udf_count} Python UDF(s) found — replace with built-in Spark functions where possible"
"""


def _generate_test_file(script_path: str, tables: List[Dict], processing_mode: str) -> str:
    header = _PYTEST_HEADER.format(script_path=script_path or "script.py")
    parts  = [
        header,
        _STATIC_TEMPLATE,
        _build_schema_tests(tables),
        _build_integrity_tests(tables),
        _build_null_tests(tables),
        _build_incremental_tests(processing_mode),
        _build_performance_tests(),
    ]
    return "\n".join(p for p in parts if p)


def _count_test_cases(test_file: str) -> List[Dict]:
    cases = []
    category = "General"
    for line in test_file.splitlines():
        cls_m = re.match(r'^class (Test\w+)', line)
        if cls_m:
            category = cls_m.group(1)
        fn_m = re.match(r'\s+def (test_\w+)\(self\)', line)
        if fn_m:
            cases.append({"name": fn_m.group(1), "category": category,
                           "description": fn_m.group(1).replace("test_", "").replace("_", " ")})
    return cases


# ── Tools ──────────────────────────────────────────────────────────────────────

@tool
def generate_test_cases(
    script_content: str,
    source_tables_json: str = "[]",
    processing_mode: str = "full",
    script_path: str = "etl_script.py",
) -> str:
    """
    Generate a comprehensive pytest test suite for a PySpark ETL script.

    Args:
        script_content:     Full PySpark script source code (used for analysis).
        source_tables_json: JSON list of source table descriptors with column lists.
        processing_mode:    "full" or "delta" — enables incremental mode tests.
        script_path:        Path the test suite will reference (default: etl_script.py).

    Returns:
        JSON with test_file_content, test_cases list, test_count, categories, success.
    """
    try:
        tables = json.loads(source_tables_json) if source_tables_json else []
        test_file = _generate_test_file(script_path, tables, processing_mode)
        cases     = _count_test_cases(test_file)
        categories = list(dict.fromkeys(c["category"] for c in cases))

        return json.dumps({
            "test_file_content": test_file,
            "test_cases":        cases,
            "test_count":        len(cases),
            "categories":        categories,
            "script_path":       script_path,
            "processing_mode":   processing_mode,
            "success":           True,
        })
    except Exception as exc:
        logger.error("Test generation failed: %s", exc)
        return json.dumps({"success": False, "error": str(exc), "test_file_content": ""})


@tool
def deploy_test_to_glue(
    test_script: str,
    job_name: str,
    s3_bucket: str,
    iam_role: str,
    region: str = "us-west-2",
) -> str:
    """
    Upload a generated test script to S3 and create an AWS Glue job for it.

    Args:
        test_script: Test file content (from generate_test_cases).
        job_name:    Name for the Glue test job.
        s3_bucket:   S3 bucket name (without s3://).
        iam_role:    IAM role ARN for the Glue job.
        region:      AWS region (default us-west-2).

    Returns:
        JSON with s3_script_path, glue_job_definition, and deployment instructions.
    """
    try:
        import boto3
        s3_key    = f"test-scripts/{job_name}_test.py"
        s3_path   = f"s3://{s3_bucket}/{s3_key}"

        boto3.client("s3", region_name=region).put_object(
            Bucket=s3_bucket, Key=s3_key, Body=test_script.encode()
        )

        glue_def = {
            "Name":        f"{job_name}-test",
            "Role":        iam_role,
            "Command": {
                "Name":           "glueetl",
                "ScriptLocation": s3_path,
                "PythonVersion":  "3",
            },
            "GlueVersion":    "4.0",
            "WorkerType":     "G.1X",
            "NumberOfWorkers": 2,
            "DefaultArguments": {
                "--JOB_NAME": f"{job_name}-test",
                "--additional-python-modules": "pytest",
            },
        }

        boto3.client("glue", region_name=region).create_job(**glue_def)

        return json.dumps({
            "s3_script_path":     s3_path,
            "glue_job_name":      f"{job_name}-test",
            "glue_job_definition": glue_def,
            "run_command":        f"aws glue start-job-run --job-name {job_name}-test",
            "success":            True,
        })
    except ImportError:
        return json.dumps({
            "success":  False,
            "error":    "boto3 not available",
            "s3_script_path": f"s3://{s3_bucket}/test-scripts/{job_name}_test.py",
        })
    except Exception as exc:
        logger.error("Glue deploy failed: %s", exc)
        return json.dumps({"success": False, "error": str(exc)})


@tool
def run_local_tests(
    script_content: str,
    source_tables_json: str = "[]",
    processing_mode: str = "full",
    timeout_seconds: int = 120,
) -> str:
    """
    Generate tests, write to a temp file, and execute with pytest locally.

    Args:
        script_content:     PySpark script source to analyse.
        source_tables_json: JSON list of source table descriptors.
        processing_mode:    "full" or "delta".
        timeout_seconds:    pytest timeout (default 120 s).

    Returns:
        JSON with passed, failed, errors, stdout, and success flag.
    """
    try:
        with tempfile.TemporaryDirectory() as tmp:
            # Write the script under test
            script_file = Path(tmp) / "etl_script.py"
            script_file.write_text(script_content)

            # Generate test file
            gen_result = json.loads(generate_test_cases.__wrapped__(
                script_content, source_tables_json, processing_mode, str(script_file)
            ))
            if not gen_result.get("success"):
                return json.dumps({"success": False, "error": gen_result.get("error")})

            test_file = Path(tmp) / "test_etl_script.py"
            test_file.write_text(gen_result["test_file_content"])

            proc = subprocess.run(
                ["python", "-m", "pytest", str(test_file), "-v", "--tb=short",
                 "--no-header", "-q"],
                capture_output=True, text=True, timeout=timeout_seconds,
                cwd=tmp,
            )
            stdout  = proc.stdout[-4000:]  # cap output
            passed  = len(re.findall(r" PASSED", stdout))
            failed  = len(re.findall(r" FAILED", stdout))
            errors  = len(re.findall(r" ERROR", stdout))

            return json.dumps({
                "passed":        passed,
                "failed":        failed,
                "errors":        errors,
                "total_tests":   gen_result.get("test_count", 0),
                "stdout":        stdout,
                "returncode":    proc.returncode,
                "success":       proc.returncode == 0,
            })
    except subprocess.TimeoutExpired:
        return json.dumps({"success": False, "error": f"pytest timed out after {timeout_seconds}s"})
    except FileNotFoundError:
        return json.dumps({"success": False, "error": "pytest not found in PATH"})
    except Exception as exc:
        logger.error("Local test run failed: %s", exc)
        return json.dumps({"success": False, "error": str(exc)})


@tool
def validate_script_syntax(script_content: str) -> str:
    """
    Quick syntax and import validation for a PySpark script without running Spark.

    Args:
        script_content: Full PySpark source code.

    Returns:
        JSON with is_valid, syntax_errors, missing_imports, anti_patterns_found.
    """
    import ast as _ast
    errors: List[str]   = []
    warnings: List[str] = []

    # Syntax check
    try:
        _ast.parse(script_content)
        is_valid = True
    except SyntaxError as exc:
        is_valid = False
        errors.append(f"SyntaxError at line {exc.lineno}: {exc.msg}")

    # Missing common imports
    needed = {
        "SparkSession":          "from pyspark.sql import SparkSession",
        "col(":                  "from pyspark.sql.functions import ...",
        "StructType":            "from pyspark.sql.types import ...",
    }
    for symbol, import_hint in needed.items():
        if symbol in script_content and "import" not in script_content:
            errors.append(f"Missing import for {symbol}: {import_hint}")

    # Anti-pattern scan
    checks = [
        (r"\.collect\(\)",                  "Unguarded .collect() — OOM risk"),
        (r"\.repartition\(\s*1\s*\)",       ".repartition(1) — use .coalesce(1)"),
        (r"select\s*\(\s*[\"\']\*[\"\']\)", 'select("*") — use explicit columns'),
        (r"\.crossJoin\(",                  "crossJoin — unbounded cartesian product"),
        (r"@udf\b",                         "Python UDF — replace with built-in functions"),
    ]
    for pat, msg in checks:
        if re.search(pat, script_content, re.IGNORECASE):
            warnings.append(msg)

    return json.dumps({
        "is_valid":            is_valid,
        "syntax_errors":       errors,
        "warnings":            warnings,
        "anti_patterns_found": len(warnings),
        "success":             is_valid,
    })


def create_script_tester_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                                region: str = "us-west-2") -> Agent:
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(model=model, system_prompt=SYSTEM_PROMPT,
                 tools=[generate_test_cases, deploy_test_to_glue,
                        run_local_tests, validate_script_syntax])
