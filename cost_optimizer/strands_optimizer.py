"""
cost_optimizer/strands_optimizer.py
=====================================
Strands-powered PySpark cost optimizer.

Features
--------
- Full-repo script scanning (auto-detects PySpark files)
- Line-by-line code analysis with inline findings
- Auto table detection - no config file needed
- Table size estimation via Glue Catalog + S3 (with heuristic fallback)
- Small-file problem detection
- Multi-cloud cost comparison: AWS / Azure / GCP / Databricks
- S3 result storage with  <prefix>/<job_name>/<date>/  layout
- Interactive Strands-agent chat mode

CLI examples
------------
    # Auto-scan a whole repo, auto-detect tables
    python -m cost_optimizer.strands_optimizer --scripts-dir ./etl_scripts

    # Single script with explicit table config
    python -m cost_optimizer.strands_optimizer \\
        --script jobs/customer_orders.py --config tables.json

    # Analyse, save to S3, then go interactive
    python -m cost_optimizer.strands_optimizer \\
        --scripts-dir ./glue_scripts \\
        --s3-bucket my-optimizer-bucket --s3-prefix reports \\
        --interactive
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ── Optional dependencies ────────────────────────────────────────────────────
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False

try:
    from strands import Agent
    HAS_STRANDS = True
    try:
        from strands.tools import tool as strands_tool
    except ImportError:
        from strands import tool as strands_tool
except ImportError:
    HAS_STRANDS = False

    def strands_tool(fn):  # no-op decorator when strands isn't installed
        return fn

# ── Local imports (existing cost_optimizer modules) ──────────────────────────
try:
    from .orchestrator import CostOptimizationOrchestrator
    HAS_ORCHESTRATOR = True
except ImportError:
    try:
        from cost_optimizer.orchestrator import CostOptimizationOrchestrator
        HAS_ORCHESTRATOR = True
    except ImportError:
        HAS_ORCHESTRATOR = False

log = logging.getLogger("strands_optimizer")

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

REPORT_SCHEMA_VERSION   = "2.0"
SMALL_FILE_THRESHOLD_MB = 128   # Parquet/ORC files below this are "small"
TINY_FILE_THRESHOLD_MB  = 10    # Extremely small - likely shuffle output
MIN_SMALL_FILE_COUNT    = 10    # Need at least this many files to flag

# Bedrock / AWS defaults
DEFAULT_MODEL_ID = "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
DEFAULT_REGION   = "us-west-2"

PYSPARK_SIGNATURES = [
    r"from\s+pyspark",
    r"import\s+pyspark",
    r"SparkContext\b",
    r"SparkSession\b",
    r"GlueContext\b",
    r"glueContext\b",
    r"getResolvedOptions\b",
    r"from\s+awsglue",
    r"import\s+awsglue",
    r"DynamicFrame\b",
]

# Multi-cloud pricing (cost per node-hour, USD; tune per region)
PLATFORM_CATALOG: Dict[str, Dict] = {
    # ── AWS ────────────────────────────────────────────────────────────────
    "aws_glue_g1x": {
        "label": "AWS Glue G.1X", "cloud": "aws",
        "node_cost_usd": 0.44, "memory_gb": 16, "vcpu": 4,
        "notes": "$0.44/DPU-hour; managed, no cluster overhead",
    },
    "aws_glue_g2x": {
        "label": "AWS Glue G.2X", "cloud": "aws",
        "node_cost_usd": 0.88, "memory_gb": 32, "vcpu": 8,
        "notes": "$0.88/DPU-hour",
    },
    "aws_emr_od": {
        "label": "AWS EMR (On-Demand m5.2xl)", "cloud": "aws",
        "node_cost_usd": 0.461, "memory_gb": 32, "vcpu": 8,
        "notes": "EC2 $0.384 + EMR $0.077/h; add master node",
    },
    "aws_emr_spot": {
        "label": "AWS EMR (Spot m5.2xl)", "cloud": "aws",
        "node_cost_usd": 0.138, "memory_gb": 32, "vcpu": 8,
        "notes": "~70% spot discount; subject to interruption",
    },
    "aws_eks_karpenter": {
        "label": "AWS EKS + Karpenter (Spot)", "cloud": "aws",
        "node_cost_usd": 0.115, "memory_gb": 32, "vcpu": 8,
        "notes": "Spot + Graviton; Karpenter bin-packs; no EMR fee",
    },
    # ── Azure ─────────────────────────────────────────────────────────────
    "azure_hdinsight": {
        "label": "Azure HDInsight (D4s_v3)", "cloud": "azure",
        "node_cost_usd": 0.192, "memory_gb": 16, "vcpu": 4,
        "notes": "Pay-as-you-go; HDInsight surcharge included",
    },
    "azure_synapse_spark": {
        "label": "Azure Synapse Spark", "cloud": "azure",
        "node_cost_usd": 0.32, "memory_gb": 28, "vcpu": 4,
        "notes": "Medium node pool; serverless; no cluster mgmt",
    },
    # ── Databricks ────────────────────────────────────────────────────────
    "databricks_aws_jobs": {
        "label": "Databricks on AWS (Jobs Compute)", "cloud": "databricks_aws",
        "node_cost_usd": 0.534, "memory_gb": 32, "vcpu": 8,
        "notes": "$0.15 DBU + $0.384 EC2 (m5.2xl) per node-hour",
    },
    "databricks_aws_spot": {
        "label": "Databricks on AWS (Spot)", "cloud": "databricks_aws",
        "node_cost_usd": 0.265, "memory_gb": 32, "vcpu": 8,
        "notes": "$0.15 DBU + spot EC2 ~$0.115 / h",
    },
    "databricks_azure_jobs": {
        "label": "Databricks on Azure (Jobs Compute)", "cloud": "databricks_azure",
        "node_cost_usd": 0.512, "memory_gb": 28, "vcpu": 4,
        "notes": "$0.32 DBU + $0.192 VM",
    },
    # ── GCP ───────────────────────────────────────────────────────────────
    "gcp_dataproc_n2_8": {
        "label": "GCP Dataproc (n2-standard-8)", "cloud": "gcp",
        "node_cost_usd": 0.485, "memory_gb": 32, "vcpu": 8,
        "notes": "Compute Engine $0.388 + Dataproc $0.097/h",
    },
    "gcp_dataproc_spot": {
        "label": "GCP Dataproc (Spot n2-standard-8)", "cloud": "gcp",
        "node_cost_usd": 0.145, "memory_gb": 32, "vcpu": 8,
        "notes": "~70% Spot discount on Compute Engine",
    },
    "gcp_dataproc_serverless": {
        "label": "GCP Dataproc Serverless", "cloud": "gcp",
        "node_cost_usd": 0.066, "memory_gb": 4, "vcpu": 0.5,
        "notes": "$0.066/DCU-hour; auto-scales; no cluster mgmt",
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# Shared session state (read by tools and interactive agent)
# ─────────────────────────────────────────────────────────────────────────────

_state: Dict[str, Any] = {
    "scan_results":     {},  # script_path -> analysis result
    "detected_tables":  {},  # script_path -> [table dicts]
    "small_file_flags": {},  # table_key   -> small-file report
    "last_report_s3":   None,
}

# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _is_pyspark_script(path: Path) -> bool:
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return False
    return any(re.search(pat, text) for pat in PYSPARK_SIGNATURES)


def _parse_s3_path(s3_url: str) -> Tuple[str, str]:
    s3_url = s3_url.replace("s3n://", "s3://").replace("s3a://", "s3://")
    parts = s3_url[5:].split("/", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


def _get_s3_object_sizes(bucket: str, prefix: str) -> List[int]:
    """List object sizes under an S3 prefix.  Returns [] when no access."""
    if not HAS_BOTO3:
        return []
    try:
        s3 = boto3.client("s3", region_name=DEFAULT_REGION)
        paginator = s3.get_paginator("list_objects_v2")
        sizes: List[int] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith("/"):
                    sizes.append(obj["Size"])
        return sizes
    except (ClientError, NoCredentialsError, Exception):
        return []


def _glue_table_info(database: str, table_name: str) -> Dict:
    """Fetch table metadata from Glue Catalog. Returns {} on failure."""
    if not HAS_BOTO3 or not database or not table_name:
        return {}
    try:
        glue = boto3.client("glue", region_name=DEFAULT_REGION)
        tbl  = glue.get_table(DatabaseName=database, Name=table_name)["Table"]
        params = tbl.get("Parameters", {})
        sd     = tbl.get("StorageDescriptor", {})
        cols   = sd.get("Columns", [])
        partition_keys = tbl.get("PartitionKeys", [])
        location = sd.get("Location", "")

        record_count = int(params.get("numRows", params.get("recordCount", 0)) or 0)
        size_bytes   = int(params.get("totalSize", params.get("sizeKey", 0)) or 0)

        fmt_raw = sd.get("InputFormat", "").lower()
        fmt = (
            "parquet" if "parquet" in fmt_raw else
            "orc"     if "orc"     in fmt_raw else
            "avro"    if "avro"    in fmt_raw else
            "json"    if "json"    in fmt_raw else
            "csv"     if "csv"     in fmt_raw or "text" in fmt_raw else
            "parquet"
        )

        # If catalog stats are absent, estimate size by listing S3
        if size_bytes == 0 and location.startswith("s3"):
            bkt, pfx = _parse_s3_path(location)
            sizes = _get_s3_object_sizes(bkt, pfx)
            if sizes:
                size_bytes = sum(sizes)
                if record_count == 0:
                    record_count = max(1, size_bytes // 500)  # ~500 bytes/row guess

        info: Dict[str, Any] = {
            "database":         database,
            "table":            table_name,
            "location":         location,
            "record_count":     record_count,
            "column_count":     len(cols) + len(partition_keys),
            "partition_column": partition_keys[0]["Name"] if partition_keys else None,
            "format":           fmt,
            "source":           "glue_catalog",
        }
        if size_bytes:
            info["size_gb"] = round(size_bytes / (1024 ** 3), 3)
        return info
    except Exception as exc:
        log.debug("Glue catalog lookup failed for %s.%s: %s", database, table_name, exc)
        return {}


def _heuristic_table_info(table_name: str, database: str = "") -> Dict:
    """Estimate table size when Glue Catalog is unavailable."""
    name = table_name.lower()
    if any(kw in name for kw in ("transaction", "event", "log", "click", "stream", "fact")):
        records = 50_000_000
    elif any(kw in name for kw in ("order", "sale", "invoice", "record")):
        records = 10_000_000
    elif any(kw in name for kw in ("customer", "account", "member", "user", "product", "item", "dim")):
        records = 1_000_000
    else:
        records = 5_000_000
    return {
        "database":     database,
        "table":        table_name,
        "record_count": records,
        "column_count": 30,
        "format":       "parquet",
        "source":       "heuristic",
        "_note":        "Estimated from name; provide config for accuracy",
    }


def _annotate_lines(script_content: str, anti_patterns: List[Dict]) -> List[Dict]:
    """Return per-line entries with inline finding annotations."""
    lines = script_content.splitlines()
    findings_map: Dict[int, List[Dict]] = {}
    for ap in anti_patterns:
        for lineno in ap.get("line_numbers", []):
            findings_map.setdefault(lineno, []).append({
                "type":        ap["pattern"],
                "severity":    ap["severity"],
                "description": ap["description"],
                "fix":         ap["fix"],
            })
    annotated = []
    for i, text in enumerate(lines, 1):
        entry: Dict[str, Any] = {"line": i, "code": text}
        if i in findings_map:
            entry["findings"] = findings_map[i]
        annotated.append(entry)
    return annotated


def _compute_platform_costs(
    workers: int, duration_hours: float, size_gb: float
) -> List[Dict]:
    """Cost across every platform in PLATFORM_CATALOG, sorted ascending."""
    out = []
    for key, p in PLATFORM_CATALOG.items():
        cost = workers * p["node_cost_usd"] * duration_hours
        # Add 1 master/driver for cluster-based platforms (skip serverless)
        if "serverless" not in key and p["cloud"] in ("aws", "gcp", "azure"):
            cost += p["node_cost_usd"] * duration_hours
        out.append({
            "platform_id":        key,
            "label":              p["label"],
            "cloud":              p["cloud"],
            "cost_per_run":       round(cost, 3),
            "annual_cost":        round(cost * 365, 0),
            "cost_per_gb":        round(cost / max(size_gb, 0.1), 4),
            "memory_per_node_gb": p["memory_gb"],
            "vcpu_per_node":      p["vcpu"],
            "notes":              p["notes"],
        })
    out.sort(key=lambda x: x["cost_per_run"])
    return out


def _small_file_recommendation(avg_mb: float, count: int) -> str:
    parts = [
        f"Table has {count} files with avg size {avg_mb:.1f} MB "
        f"(threshold {SMALL_FILE_THRESHOLD_MB} MB).",
    ]
    if avg_mb < TINY_FILE_THRESHOLD_MB:
        parts += [
            "CRITICAL small-file problem.",
            "1) Run OPTIMIZE / compaction (Delta Lake / Iceberg).",
            "2) Add .coalesce(N) or .repartition(N) before write in producer job.",
            "3) Enable auto-compaction (Delta: spark.databricks.delta.autoCompact.enabled=true).",
            "4) Increase ingestion batch size or move to less-frequent loads.",
        ]
    else:
        parts += [
            "Moderate small-file issue.",
            "1) Add .coalesce(target_files) before write.",
            "2) Schedule periodic OPTIMIZE / compaction.",
        ]
    return " ".join(parts)


# =============================================================================
# Strands Tools
# =============================================================================

@strands_tool
def scan_scripts_in_directory(directory: str, recursive: bool = True) -> Dict:
    """
    Scan a directory for PySpark scripts.

    Args:
        directory: Path to the directory (local filesystem).
        recursive: Search sub-directories (default True).

    Returns dict with 'pyspark_scripts' list and counts.
    """
    root = Path(directory)
    if not root.exists():
        return {"error": f"Directory not found: {directory}", "pyspark_scripts": []}

    pattern = "**/*.py" if recursive else "*.py"
    all_py  = list(root.glob(pattern))
    pyspark_files = [str(p) for p in all_py if _is_pyspark_script(p)]

    return {
        "directory":         str(root.resolve()),
        "total_py_files":    len(all_py),
        "pyspark_scripts":   pyspark_files,
        "pyspark_count":     len(pyspark_files),
        "non_pyspark_count": len(all_py) - len(pyspark_files),
    }


@strands_tool
def auto_detect_tables(script_path: str) -> Dict:
    """
    Extract table/data-source references from a PySpark script without a config.

    Detects:
    - AWS Glue catalog reads  (from_catalog / create_dynamic_frame)
    - spark.table("db.table")
    - SQL FROM clauses in spark.sql() strings
    - Direct S3 paths via spark.read.*

    Args:
        script_path: Path to the .py script.

    Returns dict with 'tables' list enriched from Glue Catalog or heuristics.
    """
    path = Path(script_path)
    if not path.exists():
        return {"error": f"Script not found: {script_path}", "tables": []}

    code   = path.read_text(errors="replace")
    tables: Dict[str, Dict] = {}   # keyed for dedup

    # Pattern 1 — Glue from_catalog
    for m in re.finditer(r'from_catalog\s*\([^)]+\)', code, re.IGNORECASE | re.DOTALL):
        block = m.group()
        db_m  = re.search(r'database\s*=\s*["\']([^"\']+)["\']', block)
        tbl_m = re.search(r'table_name\s*=\s*["\']([^"\']+)["\']', block)
        if tbl_m:
            db  = db_m.group(1) if db_m else ""
            tbl = tbl_m.group(1)
            key = f"{db}.{tbl}" if db else tbl
            tables[key] = {"database": db, "table": tbl, "detect_method": "glue_catalog"}

    # Pattern 2 — spark.table("db.tbl")
    for m in re.finditer(r'spark\.table\s*\(\s*["\']([^"\']+)["\']', code, re.IGNORECASE):
        ref   = m.group(1)
        parts = ref.split(".")
        db, tbl = (parts[0], parts[1]) if len(parts) == 2 else ("", parts[0])
        if ref not in tables:
            tables[ref] = {"database": db, "table": tbl, "detect_method": "spark_table"}

    # Pattern 3 — spark.sql("… FROM db.table …")
    for m in re.finditer(r'spark\.sql\s*\(\s*(?:f?["\']|"{3}|' + r"'{3})([\s\S]{1,3000}?)(?:[\"']{1,3})\s*\)",
                         code, re.IGNORECASE):
        sql_text = m.group(1)
        for fm in re.finditer(
            r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',
            sql_text, re.IGNORECASE
        ):
            ref = fm.group(1)
            db, tbl = ref.split(".", 1)
            if ref not in tables:
                tables[ref] = {"database": db, "table": tbl, "detect_method": "sql_from"}

    # Pattern 4 — direct S3 reads
    for m in re.finditer(
        r'spark\.read[^(]*\.[^(]+\(\s*["\']?(s3[an]?://[^"\'"\s,)]+)',
        code, re.IGNORECASE
    ):
        s3path = m.group(1).rstrip("/")
        if s3path not in tables:
            tables[s3path] = {
                "database": "", "table": s3path.rsplit("/", 1)[-1],
                "location": s3path, "detect_method": "s3_direct",
            }

    # Enrich each table
    enriched: List[Dict] = []
    for key, base in tables.items():
        if "location" in base and base.get("detect_method") == "s3_direct":
            enriched.append({**base, "source": "s3_path"})
        else:
            db  = base.get("database", "")
            tbl = base["table"]
            info = _glue_table_info(db, tbl) if db else {}
            if not info:
                info = _heuristic_table_info(tbl, db)
            enriched.append({**base, **info})

    _state["detected_tables"][script_path] = enriched
    return {
        "script_path":  script_path,
        "tables_found": len(enriched),
        "tables":       enriched,
        "note": (
            "Sizes/record-counts from Glue Catalog or name-based heuristics. "
            "Pass --config tables.json for precise values."
        ),
    }


@strands_tool
def detect_small_file_problem(
    location: str = "",
    database: str = "",
    table_name: str = "",
) -> Dict:
    """
    Check whether a table/S3 path suffers from the small-file problem.

    Provide either:
      location              - S3 URI (s3://bucket/prefix)
      database + table_name - resolved via Glue Catalog

    Returns a report with file counts, average size, and fix recommendations.
    """
    if not location and database and table_name:
        info = _glue_table_info(database, table_name)
        location = info.get("location", "")

    if not location:
        return {"checked": False, "reason": "No S3 location; provide location or database+table_name"}
    if not location.startswith("s3"):
        return {"checked": False, "reason": "Only S3 locations are supported"}

    bucket, prefix = _parse_s3_path(location)
    sizes = _get_s3_object_sizes(bucket, prefix)

    if not sizes:
        return {
            "checked": True, "table": table_name or location,
            "file_count": 0, "has_problem": False,
            "note": "No files found or no S3 access",
        }

    total_bytes = sum(sizes)
    avg_mb      = (total_bytes / len(sizes)) / (1024 * 1024)
    small_count = sum(1 for s in sizes if s < SMALL_FILE_THRESHOLD_MB * 1024 * 1024)
    has_problem = avg_mb < SMALL_FILE_THRESHOLD_MB and len(sizes) >= MIN_SMALL_FILE_COUNT

    key = f"{database}.{table_name}" if database and table_name else location
    report = {
        "checked":            True,
        "table":              table_name or location,
        "location":           location,
        "file_count":         len(sizes),
        "total_size_gb":      round(total_bytes / (1024 ** 3), 3),
        "avg_file_size_mb":   round(avg_mb, 2),
        "min_file_size_mb":   round(min(sizes) / (1024 * 1024), 2),
        "max_file_size_mb":   round(max(sizes) / (1024 * 1024), 2),
        "small_file_count":   small_count,
        "small_file_pct":     round(small_count / len(sizes) * 100, 1),
        "has_problem":        has_problem,
        "severity": (
            "critical" if has_problem and avg_mb < TINY_FILE_THRESHOLD_MB else
            "warning"  if has_problem else "ok"
        ),
        "recommendation": (
            _small_file_recommendation(avg_mb, len(sizes)) if has_problem else None
        ),
    }
    _state["small_file_flags"][key] = report
    return report


@strands_tool
def analyze_pyspark_script(
    script_path: str,
    tables: Optional[List[Dict]] = None,
    processing_mode: str = "full",
    current_config: Optional[Dict] = None,
    use_llm: bool = False,
) -> Dict:
    """
    Run full cost-optimization analysis on a single PySpark script.

    If tables is None or empty, tables are auto-detected from the script.

    Args:
        script_path:     Path to the .py file.
        tables:          Source table definitions (auto-detected if absent).
        processing_mode: 'full' (default) or 'delta'.
        current_config:  Dict with number_of_workers, worker_type, platform.
        use_llm:         Use Amazon Bedrock for deeper analysis.

    Returns full analysis dict including line_annotations and
    multiplatform_comparison.
    """
    if not HAS_ORCHESTRATOR:
        return {"error": "cost_optimizer package not importable"}

    path = Path(script_path)
    if not path.exists():
        return {"error": f"Script not found: {script_path}"}

    if not tables:
        detection = auto_detect_tables(script_path)
        tables = detection.get("tables", [])

    orchestrator = CostOptimizationOrchestrator(
        use_llm  = use_llm,
        model_id = DEFAULT_MODEL_ID,
    )
    result = orchestrator.analyze_script(
        script_path     = script_path,
        source_tables   = tables,
        processing_mode = processing_mode,
        current_config  = current_config or {},
    )

    if result.get("success"):
        # Attach line-by-line annotations
        anti_patterns = (
            result.get("agents", {})
                  .get("code_analyzer", {})
                  .get("analysis", {})
                  .get("anti_patterns", [])
        )
        try:
            content = path.read_text(errors="replace")
            result["line_annotations"] = _annotate_lines(content, anti_patterns)
        except OSError:
            pass

        # Attach multi-cloud comparison
        resource  = result.get("agents", {}).get("resource_allocator", {}).get("analysis", {})
        duration  = resource.get("estimated_duration_hours", 1.0)
        size_gb   = resource.get("effective_size_gb", 10.0)
        workers   = resource.get("optimal_config", {}).get("workers", 5)
        result["multiplatform_comparison"] = _compute_platform_costs(workers, duration, size_gb)

    _state["scan_results"][script_path] = result
    return result


@strands_tool
def get_multiplatform_cost_comparison(
    workers: int = 10,
    duration_hours: float = 1.0,
    effective_size_gb: float = 100.0,
) -> Dict:
    """
    Compare compute costs across AWS, Azure, GCP and Databricks for a workload.

    Args:
        workers:           Number of worker/executor nodes.
        duration_hours:    Estimated job duration in hours.
        effective_size_gb: Effective data size in GB.

    Returns a sorted table and top recommendation.
    """
    platforms = _compute_platform_costs(workers, duration_hours, effective_size_gb)
    cheapest   = platforms[0]
    glue_g2x   = next((p for p in platforms if p["platform_id"] == "aws_glue_g2x"), platforms[-1])
    savings_pct = round(
        (glue_g2x["cost_per_run"] - cheapest["cost_per_run"])
        / max(glue_g2x["cost_per_run"], 0.01) * 100, 1
    )
    return {
        "inputs": {
            "workers": workers,
            "duration_hours": duration_hours,
            "effective_size_gb": effective_size_gb,
        },
        "platforms_ranked":          platforms,
        "cheapest_option":           cheapest["label"],
        "cheapest_cost_per_run":     cheapest["cost_per_run"],
        "vs_glue_g2x_savings_pct":   savings_pct,
        "recommendation": (
            f"Cheapest: {cheapest['label']} at ${cheapest['cost_per_run']:.3f}/run "
            f"({savings_pct}% cheaper than AWS Glue G.2X)."
        ),
    }


@strands_tool
def save_results_to_s3(
    job_name: str,
    s3_bucket: str,
    s3_prefix: str = "optimizer-results",
    script_path: str = "",
) -> Dict:
    """
    Persist the analysis report to S3 under:
      s3://<bucket>/<prefix>/<job_name>/<YYYY-MM-DD>/<ts>_report.json

    Args:
        job_name:    Identifier used as folder name.
        s3_bucket:   Target S3 bucket.
        s3_prefix:   Top-level key prefix (default: optimizer-results).
        script_path: Save only this script's result; blank = save all.

    Returns the S3 URI of the saved report.
    """
    if not HAS_BOTO3:
        return {"error": "boto3 not installed; cannot write to S3"}

    date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    ts_str   = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")

    if script_path and script_path in _state["scan_results"]:
        payload_results = _state["scan_results"][script_path]
    else:
        payload_results = {
            "batch_summary":    _build_batch_summary(_state["scan_results"]),
            "individual_jobs":  _state["scan_results"],
            "small_file_flags": _state["small_file_flags"],
        }

    payload = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "job_name":       job_name,
        "generated_at":   datetime.now(tz=timezone.utc).isoformat(),
        "results":        payload_results,
    }

    key = f"{s3_prefix}/{job_name}/{date_str}/{ts_str}_report.json"
    try:
        boto3.client("s3", region_name=DEFAULT_REGION).put_object(
            Bucket      = s3_bucket,
            Key         = key,
            Body        = json.dumps(payload, indent=2, default=str),
            ContentType = "application/json",
        )
        uri = f"s3://{s3_bucket}/{key}"
        _state["last_report_s3"] = uri
        return {"success": True, "s3_uri": uri, "key": key}
    except (ClientError, NoCredentialsError) as exc:
        return {"error": str(exc)}


@strands_tool
def get_analysis_summary() -> Dict:
    """Return a high-level summary of all scripts analysed in this session."""
    results = _state["scan_results"]
    if not results:
        return {"message": "No scripts analysed yet. Run analyze_pyspark_script first."}

    total        = len(results)
    successful   = [r for r in results.values() if r.get("success")]
    anti_patterns = sum(
        r.get("agents", {}).get("code_analyzer", {}).get("analysis", {}).get("anti_pattern_count", 0)
        for r in successful
    )
    annual_savings = sum(
        r.get("summary", {}).get("potential_annual_savings", 0) for r in successful
    )
    small_file_issues = sum(
        1 for r in _state["small_file_flags"].values() if r.get("has_problem")
    )
    return {
        "scripts_analyzed":            total,
        "successful":                  len(successful),
        "total_anti_patterns":         anti_patterns,
        "small_file_issues":           small_file_issues,
        "potential_annual_savings_usd": round(annual_savings, 2),
        "scripts": [
            {
                "path":              path,
                "job_name":          r.get("job_name", ""),
                "anti_patterns":     r.get("agents", {}).get("code_analyzer", {})
                                      .get("analysis", {}).get("anti_pattern_count", 0),
                "savings_pct":       r.get("summary", {}).get("potential_savings_percent", 0),
            }
            for path, r in results.items() if r.get("success")
        ],
        "last_report_s3": _state["last_report_s3"],
    }


# =============================================================================
# Batch helpers
# =============================================================================

def _build_batch_summary(results: Dict[str, Any]) -> Dict:
    successful = [r for r in results.values() if r.get("success")]
    if not successful:
        return {}

    total_anti = sum(
        r.get("agents", {}).get("code_analyzer", {}).get("analysis", {}).get("anti_pattern_count", 0)
        for r in successful
    )
    total_savings = sum(
        r.get("summary", {}).get("potential_annual_savings", 0) for r in successful
    )
    avg_pct = (
        sum(r.get("summary", {}).get("potential_savings_percent", 0) for r in successful)
        / len(successful)
    )

    freq: Dict[str, int] = {}
    for r in successful:
        for ap in r.get("agents", {}).get("code_analyzer", {}).get("analysis", {}).get("anti_patterns", []):
            freq[ap["pattern"]] = freq.get(ap["pattern"], 0) + 1

    return {
        "scripts_analyzed":                  len(successful),
        "total_anti_patterns":               total_anti,
        "total_potential_annual_savings_usd": round(total_savings, 2),
        "avg_savings_percent":               round(avg_pct, 1),
        "top_anti_patterns": sorted(
            [{"pattern": k, "count": v} for k, v in freq.items()],
            key=lambda x: -x["count"]
        )[:10],
    }


def _print_line_annotations(annotated: List[Dict], context_lines: int = 2) -> None:
    """Print source with inline finding annotations, showing context."""
    finding_linenos = {ln["line"] for ln in annotated if "findings" in ln}
    show: set = set()
    for fl in finding_linenos:
        show.update(range(max(1, fl - context_lines), fl + context_lines + 1))

    prev = 0
    for ln in annotated:
        n = ln["line"]
        if n not in show:
            continue
        if prev and n > prev + 1:
            print("    ...")
        code_preview = ln["code"][:120].rstrip()
        if "findings" in ln:
            for f in ln["findings"]:
                sev = f["severity"].upper()
                print(f"  L{n:>5}  [{sev}] {f['description']}")
                print(f"          Fix: {f['fix']}")
                print(f"          >>> {code_preview}")
        else:
            print(f"  L{n:>5}      {code_preview}")
        prev = n


def _save_local(report: Dict, output_dir: str, job_name: str) -> str:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    ts_str   = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    fname    = out / f"{job_name}_{date_str}_{ts_str}_report.json"
    fname.write_text(json.dumps(report, indent=2, default=str))
    return str(fname)


def _save_per_script_reports(
    all_results: Dict[str, Any],
    batch_summary: Dict,
    output_dir: str,
    job_name: str,
) -> List[str]:
    """
    Save one JSON + one HTML per script, plus a batch-level JSON + HTML.
    Returns list of all file paths written.
    """
    date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    ts_str   = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    out      = Path(output_dir) / job_name / date_str
    out.mkdir(parents=True, exist_ok=True)
    written: List[str] = []

    for script_path, result in all_results.items():
        script_name = Path(script_path).stem

        # Per-script JSON
        json_path = out / f"{script_name}_{ts_str}.json"
        json_path.write_text(json.dumps(result, indent=2, default=str))
        written.append(str(json_path))

        # Per-script HTML
        html_path = out / f"{script_name}_{ts_str}.html"
        html_path.write_text(_render_script_html(result, script_name, date_str))
        written.append(str(html_path))

    # Batch JSON
    batch_json = out / f"batch_summary_{ts_str}.json"
    batch_json.write_text(json.dumps({
        "schema_version": REPORT_SCHEMA_VERSION,
        "job_name":       job_name,
        "generated_at":   datetime.now(tz=timezone.utc).isoformat(),
        "batch_summary":  batch_summary,
        "scripts":        list(all_results.keys()),
    }, indent=2, default=str))
    written.append(str(batch_json))

    # Batch HTML
    batch_html = out / f"batch_summary_{ts_str}.html"
    batch_html.write_text(_render_batch_html(batch_summary, all_results, job_name, date_str))
    written.append(str(batch_html))

    return written


# ── HTML renderers ────────────────────────────────────────────────────────────

def _sev_color(sev: str) -> str:
    return {"critical": "#d32f2f", "high": "#f57c00",
            "medium": "#fbc02d", "low": "#388e3c"}.get(sev.lower(), "#555")


def _render_script_html(result: Dict, script_name: str, date_str: str) -> str:
    s   = result.get("summary", {})
    recs = result.get("all_recommendations", [])
    mp  = result.get("multiplatform_comparison", [])
    ann = result.get("line_annotations", [])

    # ── Recommendations table rows ────────────────────────────────────────────
    rec_rows = ""
    for r in sorted(recs, key=lambda x: (x.get("priority", "P9"),)):
        prio  = r.get("priority", "")
        title = r.get("title", "")
        desc  = r.get("description", r.get("implementation", ""))[:200]
        sav   = r.get("estimated_savings_percent", 0)
        pcolor = {"P0": "#d32f2f", "P1": "#f57c00", "P2": "#1565c0", "P3": "#555"}.get(prio, "#555")
        rec_rows += (
            f"<tr><td><span style='color:{pcolor};font-weight:700'>{prio}</span></td>"
            f"<td>{title}</td><td>{desc}</td>"
            f"<td style='text-align:center'>{sav}%</td></tr>\n"
        )

    # ── Multi-platform table rows ─────────────────────────────────────────────
    mp_rows = ""
    for i, p in enumerate(mp[:10]):
        bg = "#e8f5e9" if i == 0 else ("#fff" if i % 2 == 0 else "#fafafa")
        mp_rows += (
            f"<tr style='background:{bg}'>"
            f"<td>{'★ ' if i==0 else ''}{p['label']}</td>"
            f"<td style='text-align:right'>${p['cost_per_run']:.3f}</td>"
            f"<td style='text-align:right'>${p['annual_cost']:,.0f}</td>"
            f"<td style='text-align:right'>{p.get('memory_per_node_gb','-')} GB</td>"
            f"<td style='font-size:11px;color:#777'>{p.get('notes','')}</td></tr>\n"
        )

    # ── Line findings ─────────────────────────────────────────────────────────
    finding_lines = [ln for ln in ann if "findings" in ln]
    line_rows = ""
    for ln in finding_lines[:200]:
        for f in ln["findings"]:
            sev = f["severity"]
            col = _sev_color(sev)
            line_rows += (
                f"<tr><td style='text-align:center;font-weight:700'>{ln['line']}</td>"
                f"<td><span style='color:{col};font-weight:700'>{sev.upper()}</span></td>"
                f"<td>{f['description']}</td>"
                f"<td style='font-family:monospace;font-size:11px'>{f['fix']}</td>"
                f"<td style='font-family:monospace;font-size:11px;max-width:300px;overflow:hidden'>"
                f"{ln['code'].strip()[:120]}</td></tr>\n"
            )
    if not line_rows:
        line_rows = "<tr><td colspan='5' style='color:#388e3c;text-align:center'>No issues found</td></tr>"

    cur  = s.get("current_cost_per_run", 0)
    opt  = s.get("optimal_cost_per_run", 0)
    pct  = s.get("potential_savings_percent", 0)
    ann_sav = s.get("potential_annual_savings", 0)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>PySpark Optimizer – {script_name}</title>
<style>
  body{{font-family:system-ui,sans-serif;margin:0;padding:20px;background:#f5f5f5;color:#222}}
  h1{{background:#1a237e;color:#fff;padding:16px 24px;border-radius:8px;margin:0 0 20px}}
  h2{{color:#1a237e;border-bottom:2px solid #1a237e;padding-bottom:4px;margin-top:28px}}
  .cards{{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:24px}}
  .card{{background:#fff;border-radius:8px;padding:16px 24px;flex:1;min-width:160px;
          box-shadow:0 1px 4px rgba(0,0,0,.12)}}
  .card .val{{font-size:28px;font-weight:700;color:#1a237e}}
  .card .lbl{{font-size:12px;color:#666;margin-top:4px}}
  .savings .val{{color:#2e7d32}}
  table{{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;
          overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.1)}}
  th{{background:#1a237e;color:#fff;padding:10px 12px;text-align:left;font-size:13px}}
  td{{padding:8px 12px;border-bottom:1px solid #eee;font-size:13px;vertical-align:top}}
  tr:last-child td{{border-bottom:none}}
  .footer{{margin-top:32px;font-size:11px;color:#999;text-align:center}}
  details summary{{cursor:pointer;font-weight:600;color:#1a237e;padding:6px 0}}
</style>
</head>
<body>
<h1>PySpark Cost Optimizer — {script_name}</h1>
<p style="color:#555">Generated: {date_str} &nbsp;|&nbsp; Region: {DEFAULT_REGION} &nbsp;|&nbsp; Model: {DEFAULT_MODEL_ID}</p>

<div class="cards">
  <div class="card"><div class="val">{s.get('anti_patterns_found',0)}</div><div class="lbl">Anti-patterns</div></div>
  <div class="card"><div class="val">{s.get('critical_issues',0)}</div><div class="lbl">Critical issues</div></div>
  <div class="card"><div class="val">${cur:.3f}</div><div class="lbl">Current cost/run</div></div>
  <div class="card"><div class="val">${opt:.3f}</div><div class="lbl">Optimal cost/run</div></div>
  <div class="card savings"><div class="val">{pct:.0f}%</div><div class="lbl">Potential savings</div></div>
  <div class="card savings"><div class="val">${ann_sav:,.0f}</div><div class="lbl">Annual savings (est.)</div></div>
</div>

<h2>Recommendations</h2>
<table>
<thead><tr><th>Priority</th><th>Title</th><th>Description</th><th>Est. Savings</th></tr></thead>
<tbody>{rec_rows}</tbody>
</table>

<h2>Multi-Cloud Cost Comparison</h2>
<table>
<thead><tr><th>Platform</th><th>Cost/Run</th><th>Annual Cost</th><th>Memory/Node</th><th>Notes</th></tr></thead>
<tbody>{mp_rows}</tbody>
</table>

<h2>Line-by-Line Findings</h2>
<details open>
<summary>{len(finding_lines)} line(s) with issues</summary>
<table style="margin-top:8px">
<thead><tr><th>Line</th><th>Severity</th><th>Issue</th><th>Fix</th><th>Code</th></tr></thead>
<tbody>{line_rows}</tbody>
</table>
</details>

<div class="footer">strands_optimizer v{REPORT_SCHEMA_VERSION} — {DEFAULT_MODEL_ID}</div>
</body>
</html>"""


def _render_batch_html(
    batch_summary: Dict,
    all_results: Dict[str, Any],
    job_name: str,
    date_str: str,
) -> str:
    # Top anti-patterns chart rows
    ap_rows = ""
    for ap in batch_summary.get("top_anti_patterns", [])[:10]:
        bar_w = int(ap["count"] / max(batch_summary.get("top_anti_patterns", [{}])[0].get("count", 1), 1) * 200)
        ap_rows += (
            f"<tr><td>{ap['pattern']}</td><td>{ap['count']}</td>"
            f"<td><div style='background:#1a237e;height:14px;width:{bar_w}px;border-radius:3px'></div></td></tr>\n"
        )

    # Per-script summary rows
    script_rows = ""
    for path, r in all_results.items():
        s = r.get("summary", {})
        name = Path(path).stem
        ok   = "✔" if r.get("success") else "✖"
        ap   = s.get("anti_patterns_found", 0)
        crit = s.get("critical_issues", 0)
        pct  = s.get("potential_savings_percent", 0)
        sav  = s.get("potential_annual_savings", 0)
        pcolor = "#d32f2f" if crit else ("#f57c00" if ap else "#2e7d32")
        script_rows += (
            f"<tr><td>{ok} {name}</td>"
            f"<td style='text-align:center;color:{pcolor};font-weight:700'>{ap} ({crit} crit)</td>"
            f"<td style='text-align:right'>{pct:.0f}%</td>"
            f"<td style='text-align:right'>${sav:,.0f}</td></tr>\n"
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>PySpark Optimizer – Batch Summary – {job_name}</title>
<style>
  body{{font-family:system-ui,sans-serif;margin:0;padding:20px;background:#f5f5f5;color:#222}}
  h1{{background:#1a237e;color:#fff;padding:16px 24px;border-radius:8px;margin:0 0 20px}}
  h2{{color:#1a237e;border-bottom:2px solid #1a237e;padding-bottom:4px;margin-top:28px}}
  .cards{{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:24px}}
  .card{{background:#fff;border-radius:8px;padding:16px 24px;flex:1;min-width:140px;
          box-shadow:0 1px 4px rgba(0,0,0,.12)}}
  .card .val{{font-size:28px;font-weight:700;color:#1a237e}}
  .card .lbl{{font-size:12px;color:#666;margin-top:4px}}
  .savings .val{{color:#2e7d32}}
  table{{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;
          overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.1)}}
  th{{background:#1a237e;color:#fff;padding:10px 12px;text-align:left;font-size:13px}}
  td{{padding:8px 12px;border-bottom:1px solid #eee;font-size:13px}}
  tr:last-child td{{border-bottom:none}}
  .footer{{margin-top:32px;font-size:11px;color:#999;text-align:center}}
</style>
</head>
<body>
<h1>PySpark Cost Optimizer — Batch Summary — {job_name}</h1>
<p style="color:#555">Generated: {date_str} &nbsp;|&nbsp; Region: {DEFAULT_REGION}</p>

<div class="cards">
  <div class="card"><div class="val">{batch_summary.get('scripts_analyzed',0)}</div><div class="lbl">Scripts analysed</div></div>
  <div class="card"><div class="val">{batch_summary.get('total_anti_patterns',0)}</div><div class="lbl">Total anti-patterns</div></div>
  <div class="card savings"><div class="val">{batch_summary.get('avg_savings_percent',0):.0f}%</div><div class="lbl">Avg potential savings</div></div>
  <div class="card savings"><div class="val">${batch_summary.get('total_potential_annual_savings_usd',0):,.0f}</div><div class="lbl">Total annual savings</div></div>
</div>

<h2>Per-Script Results</h2>
<table>
<thead><tr><th>Script</th><th>Anti-patterns</th><th>Savings %</th><th>Annual Savings</th></tr></thead>
<tbody>{script_rows}</tbody>
</table>

<h2>Top Anti-patterns Across All Scripts</h2>
<table>
<thead><tr><th>Pattern</th><th>Count</th><th>Frequency</th></tr></thead>
<tbody>{ap_rows}</tbody>
</table>

<div class="footer">strands_optimizer v{REPORT_SCHEMA_VERSION} — {DEFAULT_MODEL_ID}</div>
</body>
</html>"""
# =============================================================================

_INTERACTIVE_SYSTEM_PROMPT = """
You are an expert PySpark and Big Data cost-optimization assistant built into
the strands_optimizer tool.

You have direct access to the following tools:
  scan_scripts_in_directory    – discover PySpark scripts in a repo directory
  auto_detect_tables           – extract table references from a script
  detect_small_file_problem    – check a table/S3 path for small-file issues
  analyze_pyspark_script       – run full cost analysis on one script
  get_multiplatform_cost_comparison – compare AWS / Azure / GCP / Databricks costs
  save_results_to_s3           – persist reports to S3 with job_name/date prefix
  get_analysis_summary         – high-level summary of the current session

Behaviour guidelines:
- Be concise and cite line numbers where relevant.
- Prioritise P0/P1 recommendations (highest impact, easiest fix).
- Mention small-file problems whenever detected.
- Compare non-AWS platforms when they offer material savings.
- When saving to S3 always confirm the s3_uri to the user.
""".strip()

_INTERACTIVE_TOOLS = [
    scan_scripts_in_directory,
    auto_detect_tables,
    detect_small_file_problem,
    analyze_pyspark_script,
    get_multiplatform_cost_comparison,
    save_results_to_s3,
    get_analysis_summary,
]


def interactive_mode() -> None:
    """
    Start a Strands-agent REPL for interactive analysis and Q&A.
    Falls back to a simple echo loop when strands-agents is not installed.
    """
    if not HAS_STRANDS:
        print(
            "\n[WARNING] strands-agents not installed.  "
            "Install: pip install strands-agents\n"
            "Falling back to simple prompt (no AI).\n"
        )
        while True:
            try:
                user_input = input("optimizer> ").strip()
            except (KeyboardInterrupt, EOFError):
                break
            if user_input.lower() in ("exit", "quit", "q"):
                break
            print(f"[echo] {user_input}")
        return

    try:
        from strands.models import BedrockModel
        _model = BedrockModel(model_id=DEFAULT_MODEL_ID, region_name=DEFAULT_REGION)
    except ImportError:
        _model = DEFAULT_MODEL_ID  # older strands-agents: pass model_id string

    agent = Agent(
        model         = _model,
        system_prompt = _INTERACTIVE_SYSTEM_PROMPT,
        tools         = _INTERACTIVE_TOOLS,
    )

    summary = get_analysis_summary()
    print("\n" + "=" * 65)
    print("  PySpark Cost Optimizer  –  Interactive Mode")
    print("  Ask questions or give commands.  Type 'exit' to quit.")
    print("=" * 65)
    if isinstance(summary, dict) and summary.get("scripts_analyzed"):
        print(f"\n  Session: {summary['scripts_analyzed']} script(s) analysed")
        print(f"  Anti-patterns total : {summary['total_anti_patterns']}")
        print(f"  Potential savings   : ${summary.get('potential_annual_savings_usd', 0):,.0f}/year")
    print()

    while True:
        try:
            user_input = input("optimizer> ").strip()
        except (KeyboardInterrupt, EOFError):
            print()
            break
        if not user_input:
            continue
        if user_input.lower() in ("exit", "quit", "q", "bye"):
            print("Goodbye!")
            break
        try:
            response = agent(user_input)
            print(f"\n{response}\n")
        except Exception as exc:
            print(f"[Error] {exc}\n")


# =============================================================================
# CLI
# =============================================================================

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="strands_optimizer",
        description="Strands-powered PySpark cost optimizer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Auto-scan repo, auto-detect tables:
  python -m cost_optimizer.strands_optimizer --scripts-dir ./etl_scripts

  # Single script + explicit table config:
  python -m cost_optimizer.strands_optimizer \\
      --script jobs/orders.py --config tables.json

  # Analyse, save to S3, interactive mode:
  python -m cost_optimizer.strands_optimizer \\
      --scripts-dir ./glue_scripts \\
      --s3-bucket my-bucket --s3-prefix reports --interactive
""",
    )
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--scripts-dir", metavar="DIR",
                     help="Directory to scan for PySpark scripts")
    src.add_argument("--script", metavar="FILE",
                     help="Analyse a single .py script")

    p.add_argument("--config", metavar="FILE",
                   help="JSON file with table definitions (omit for auto-detection)")
    p.add_argument(
        "--tables", nargs="+", metavar="DB.TABLE",
        help=(
            "One or more tables as db.table (e.g. sales_db.transactions customer_db.customers). "
            "Resolved via Glue Catalog for accurate size/partition data. "
            "Takes precedence over --config when both are given."
        ),
    )
    p.add_argument("--processing-mode", choices=["full", "delta"], default="full",
                   help="Processing mode: full (default) or delta/incremental")
    p.add_argument("--workers", type=int, default=None,
                   help="Current worker count for cost baseline")
    p.add_argument("--worker-type", default="G.2X",
                   choices=["G.1X", "G.2X", "G.4X", "G.8X"],
                   help="Current Glue worker type (default G.2X)")
    p.add_argument("--use-llm", action="store_true",
                   help="Use Amazon Bedrock for deeper analysis (requires credentials)")
    p.add_argument("--model-id", default=DEFAULT_MODEL_ID,
                   help=f"Bedrock model ID (default: {DEFAULT_MODEL_ID})")
    p.add_argument("--region", default=DEFAULT_REGION,
                   help=f"AWS region for Bedrock/Glue/S3 calls (default: {DEFAULT_REGION})")
    p.add_argument("--s3-bucket", metavar="BUCKET",
                   help="S3 bucket to persist results")
    p.add_argument("--s3-prefix", default="optimizer-results",
                   help="S3 key prefix (default: optimizer-results)")
    p.add_argument("--output-dir", default="optimizer_reports",
                   help="Local output directory when S3 is not configured")
    p.add_argument("--job-name", default=None,
                   help="Job name prefix for reports (inferred from script name)")
    p.add_argument("--interactive", "-i", action="store_true",
                   help="Start interactive Strands agent after analysis")
    p.add_argument("--small-files", action="store_true",
                   help="Check detected S3 locations for small-file problems")
    p.add_argument("--show-lines", action="store_true",
                   help="Print annotated source with inline findings")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="Enable debug logging")
    return p


def _load_config(config_path: str) -> List[Dict]:
    with open(config_path) as fh:
        data = json.load(fh)
    if isinstance(data, list):
        return data
    return data.get("tables", [data])


def _resolve_table_args(table_args: List[str]) -> List[Dict]:
    """
    Convert CLI  --tables db.table [db.table ...]  to enriched table dicts.

    Each entry is first looked up in the Glue Catalog for accurate size,
    partition column, and file location; falls back to heuristics if Glue
    is unavailable or the table is not found.
    """
    tables: List[Dict] = []
    for entry in table_args:
        entry = entry.strip()
        if "." in entry:
            db, tbl = entry.split(".", 1)
        else:
            db, tbl = "", entry

        print(f"  Resolving table: {entry} ...", end=" ", flush=True)
        info = _glue_table_info(db, tbl) if db else {}
        if info:
            source = info.get("source", "glue_catalog")
            size_note = (
                f"{info['size_gb']:.2f} GB" if "size_gb" in info
                else f"{info.get('record_count', 0):,} rows (estimated)"
            )
            print(f"[{source}] {size_note}")
        else:
            info = _heuristic_table_info(tbl, db)
            print(f"[heuristic] {info.get('record_count', 0):,} rows (name-based estimate)")

        # Check for small-file problem on the resolved location
        location = info.get("location", "")
        if location.startswith("s3"):
            sf = detect_small_file_problem(location=location, database=db, table_name=tbl)
            if sf.get("has_problem"):
                print(
                    f"    ⚠ Small-file problem: {sf['file_count']} files, "
                    f"avg {sf['avg_file_size_mb']:.1f} MB [{sf['severity'].upper()}]"
                )

        tables.append(info)
    return tables


def _run_one(
    script_path: str,
    tables: List[Dict],
    processing_mode: str,
    current_config: Dict,
    use_llm: bool,
    show_lines: bool,
    check_small_files: bool,
) -> Dict:
    job_name = Path(script_path).stem
    print(f"\n{'─'*65}")
    print(f"  Analysing : {job_name}")
    print(f"{'─'*65}")

    result = analyze_pyspark_script(
        script_path     = script_path,
        tables          = tables,
        processing_mode = processing_mode,
        current_config  = current_config,
        use_llm         = use_llm,
    )

    if not result.get("success"):
        print(f"  [ERROR] {result.get('error', 'Analysis failed')}")
        return result

    s = result.get("summary", {})
    print(f"  Tables          : {len(tables or _state['detected_tables'].get(script_path, []))}")
    print(f"  Effective data  : {s.get('effective_data_size_gb', 0):.1f} GB")
    print(f"  Anti-patterns   : {s.get('anti_patterns_found', 0)} "
          f"(critical: {s.get('critical_issues', 0)})")
    print(f"  Current cost    : ${s.get('current_cost_per_run', 0):.3f}/run")
    print(f"  Optimal cost    : ${s.get('optimal_cost_per_run', 0):.3f}/run")
    print(f"  Potential saving: {s.get('potential_savings_percent', 0):.0f}% "
          f"(${s.get('potential_annual_savings', 0):,.0f}/year)")

    mp = result.get("multiplatform_comparison", [])
    if mp:
        print(f"\n  Multi-cloud top-3:")
        for rank, p in enumerate(mp[:3], 1):
            print(f"    {rank}. {p['label']:<44}  ${p['cost_per_run']:.3f}/run")

    recs = result.get("all_recommendations", [])
    p0 = [r for r in recs if r.get("priority") == "P0"]
    if p0:
        print(f"\n  Top P0 recommendations:")
        for r in p0[:3]:
            print(f"    • {r.get('title', '')}")

    if show_lines and "line_annotations" in result:
        print(f"\n  Line-by-line findings:")
        _print_line_annotations(result["line_annotations"])

    if check_small_files:
        for tbl in _state["detected_tables"].get(script_path, []):
            loc = tbl.get("location", "")
            db  = tbl.get("database", "")
            tn  = tbl.get("table", "")
            if loc or (db and tn):
                sf = detect_small_file_problem(location=loc, database=db, table_name=tn)
                if sf.get("has_problem"):
                    print(f"\n  [SMALL FILE] {tn}: {sf['file_count']} files "
                          f"avg {sf['avg_file_size_mb']:.1f} MB [{sf['severity'].upper()}]")

    return result


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args   = parser.parse_args(argv)

    logging.basicConfig(
        level  = logging.DEBUG if args.verbose else logging.WARNING,
        format = "%(levelname)s %(name)s %(message)s",
    )

    # Apply region / model overrides at module level so all helpers pick them up
    global DEFAULT_REGION, DEFAULT_MODEL_ID
    DEFAULT_REGION   = args.region
    DEFAULT_MODEL_ID = args.model_id

    # Collect scripts
    if args.script:
        scripts          = [args.script]
        default_job_name = Path(args.script).stem
    else:
        scan = scan_scripts_in_directory(args.scripts_dir)
        if "error" in scan:
            print(f"[ERROR] {scan['error']}")
            return 1
        scripts = scan["pyspark_scripts"]
        if not scripts:
            print(f"No PySpark scripts found in {args.scripts_dir}")
            return 0
        print(f"\nFound {len(scripts)} PySpark script(s) in {args.scripts_dir}")
        default_job_name = Path(args.scripts_dir).name or "batch"

    job_name = args.job_name or default_job_name

    # Resolve input tables  (--tables takes priority over --config)
    config_tables: Optional[List[Dict]] = None
    if args.tables:
        print(f"\nResolving {len(args.tables)} table(s) from --tables arg:")
        config_tables = _resolve_table_args(args.tables)
        print(f"  → {len(config_tables)} table(s) resolved\n")
    elif args.config:
        try:
            config_tables = _load_config(args.config)
            print(f"Loaded {len(config_tables)} table(s) from {args.config}")
        except Exception as exc:
            print(f"[WARNING] Could not load config {args.config}: {exc}")

    current_config: Dict[str, Any] = {"worker_type": args.worker_type, "platform": "glue"}
    if args.workers:
        current_config["number_of_workers"] = args.workers

    # Analyse each script
    all_results: Dict[str, Any] = {}
    for script_path in scripts:
        result = _run_one(
            script_path     = script_path,
            tables          = config_tables or [],
            processing_mode = args.processing_mode,
            current_config  = current_config,
            use_llm         = args.use_llm,
            show_lines      = args.show_lines,
            check_small_files = args.small_files,
        )
        all_results[script_path] = result

    # Batch summary
    if len(scripts) > 1:
        bsum = _build_batch_summary(all_results)
        print(f"\n{'='*65}")
        print("  BATCH SUMMARY")
        print(f"{'='*65}")
        print(f"  Scripts analysed     : {bsum.get('scripts_analyzed', 0)}")
        print(f"  Total anti-patterns  : {bsum.get('total_anti_patterns', 0)}")
        print(f"  Avg potential savings: {bsum.get('avg_savings_percent', 0):.0f}%")
        print(f"  Total annual savings : ${bsum.get('total_potential_annual_savings_usd', 0):,.0f}")
        for ap in bsum.get("top_anti_patterns", [])[:5]:
            print(f"    {ap['count']:>3}×  {ap['pattern']}")

    # ── Save per-script JSON + HTML, plus batch summary ──────────────────────
    bsummary = _build_batch_summary(all_results)
    written  = _save_per_script_reports(all_results, bsummary, args.output_dir, job_name)

    print(f"\n  Output directory → {Path(args.output_dir) / job_name}")
    for f in written:
        ext  = Path(f).suffix
        name = Path(f).name
        icon = "📄" if ext == ".json" else "🌐"
        print(f"    {icon}  {name}")

    # ── Optionally also push to S3 ────────────────────────────────────────────
    if args.s3_bucket:
        s3r = save_results_to_s3(
            job_name  = job_name,
            s3_bucket = args.s3_bucket,
            s3_prefix = args.s3_prefix,
        )
        if s3r.get("success"):
            print(f"\n  S3 report      → {s3r['s3_uri']}")
        else:
            print(f"\n  [WARNING] S3 save failed: {s3r.get('error')}")

    if args.interactive:
        interactive_mode()

    return 0


if __name__ == "__main__":
    sys.exit(main())
