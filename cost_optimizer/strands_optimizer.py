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

try:
    from .agents.base import TOKEN_TRACKER
except ImportError:
    try:
        from cost_optimizer.agents.base import TOKEN_TRACKER
    except ImportError:
        TOKEN_TRACKER = None  # type: ignore[assignment]

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
    # ── Alternate / serverless query engines ─────────────────────────────────
    "aws_athena": {
        "label": "AWS Athena (Trino/Presto)", "cloud": "aws",
        "pricing_model": "per_tb_scanned",
        "cost_per_tb_usd": 5.0,
        "memory_gb": 0, "vcpu": 0,   # serverless – no fixed node size
        "notes": (
            "$5/TB scanned; serverless Trino/Presto engine; ANSI SQL only; "
            "no cluster mgmt; partition pruning & columnar formats reduce cost; "
            "NOT suitable for multi-step PySpark transformations"
        ),
    },
    "aws_lambda_etl": {
        "label": "AWS Lambda (simple ETL)", "cloud": "aws",
        "pricing_model": "per_gb_second",
        "cost_per_gb_second": 0.0000166667,
        "max_memory_gb": 10,
        "max_duration_s": 900,
        "memory_gb": 10, "vcpu": 6,
        "notes": (
            "$0.0000166667/GB-s; max 10 GB RAM / 15 min; "
            "no Spark runtime; suitable for lightweight transforms < 10 GB data; "
            "use Step Functions to chain; NOT suitable for heavy Spark jobs"
        ),
    },
    "aws_emr_serverless": {
        "label": "AWS EMR Serverless (Spark)", "cloud": "aws",
        "pricing_model": "per_vcpu_memory_hour",
        "cost_per_vcpu_hour": 0.052,
        "cost_per_gb_hour": 0.0057,
        "memory_gb": 0, "vcpu": 0,   # auto-scales
        "notes": (
            "$0.052/vCPU-h + $0.0057/GB-h; native PySpark; auto-scales; "
            "no cluster to manage; pre-initialized capacity available; "
            "billed per second; good replacement for Glue when cost is key"
        ),
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
        file_count = 0
        if size_bytes == 0 and location.startswith("s3"):
            bkt, pfx = _parse_s3_path(location)
            sizes = _get_s3_object_sizes(bkt, pfx)
            if sizes:
                file_count = len(sizes)
                size_bytes = sum(sizes)
                if record_count == 0:
                    record_count = max(1, size_bytes // 500)  # ~500 bytes/row guess

        # Track whether we have real stats or are working blind
        has_stats = size_bytes > 0 or record_count > 0

        info: Dict[str, Any] = {
            "database":         database,
            "table":            table_name,
            "location":         location,
            "record_count":     record_count,
            "column_count":     len(cols) + len(partition_keys),
            "partition_column": partition_keys[0]["Name"] if partition_keys else None,
            "format":           fmt,
            "source":           "glue_catalog",
            "has_stats":        has_stats,
        }
        if size_bytes:
            info["size_gb"] = round(size_bytes / (1024 ** 3), 3)
        if file_count:
            info["file_count"] = file_count
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
    """
    Cost across every platform in PLATFORM_CATALOG, sorted ascending.

    Supports three pricing models:
      - node_cost_usd       (traditional cluster: workers × node_cost × hours)
      - per_tb_scanned      (Athena: $5/TB)
      - per_gb_second       (Lambda: GB-s)
      - per_vcpu_memory_hour(EMR Serverless: vCPU-h + GB-h)
    """
    out = []
    for key, p in PLATFORM_CATALOG.items():
        model = p.get("pricing_model", "node_cost_usd")

        if model == "per_tb_scanned":
            # Athena: cost is purely based on data scanned
            # Assume columnar format (Parquet) reduces scanned bytes by ~80%
            effective_tb = max(size_gb, 0.1) / 1024 * 0.2  # columnar compression
            cost = effective_tb * p["cost_per_tb_usd"]
            pricing_note = f"${p['cost_per_tb_usd']}/TB scanned (assumes Parquet/ORC, ~80% reduction)"

        elif model == "per_gb_second":
            # Lambda: cost = memory_gb × duration_s × rate
            mem_gb   = min(size_gb * 0.5, p.get("max_memory_gb", 10))  # heuristic
            dur_s    = duration_hours * 3600
            # Lambda has 15-min hard limit; large jobs must be chained
            if dur_s > p.get("max_duration_s", 900):
                invocations = max(1, int(dur_s / p["max_duration_s"]))
                dur_s       = p["max_duration_s"]
                cost = mem_gb * dur_s * p["cost_per_gb_second"] * invocations
                pricing_note = (
                    f"${p['cost_per_gb_second']}/GB-s × {mem_gb:.0f} GB × "
                    f"{dur_s}s × {invocations} invocations (chained)"
                )
            else:
                cost = mem_gb * dur_s * p["cost_per_gb_second"]
                pricing_note = (
                    f"${p['cost_per_gb_second']}/GB-s × {mem_gb:.0f} GB × {dur_s:.0f}s"
                )

        elif model == "per_vcpu_memory_hour":
            # EMR Serverless: billed per vCPU-hour + GB-hour
            # Heuristic: assume 4 vCPU + 16 GB per executor, scaled to workers
            vcpus_used  = workers * 4
            mem_gb_used = workers * 16
            cost = (
                vcpus_used  * p["cost_per_vcpu_hour"]  * duration_hours
                + mem_gb_used * p["cost_per_gb_hour"]   * duration_hours
            )
            pricing_note = (
                f"${p['cost_per_vcpu_hour']}/vCPU-h × {vcpus_used} vCPUs + "
                f"${p['cost_per_gb_hour']}/GB-h × {mem_gb_used} GB"
            )

        else:
            # Traditional node_cost_usd model
            cost = workers * p["node_cost_usd"] * duration_hours
            if "serverless" not in key and p["cloud"] in ("aws", "gcp", "azure"):
                cost += p["node_cost_usd"] * duration_hours  # driver/master
            pricing_note = (
                f"${p['node_cost_usd']}/node-h × {workers} workers × {duration_hours:.2f}h"
            )

        out.append({
            "platform_id":        key,
            "label":              p["label"],
            "cloud":              p["cloud"],
            "pricing_model":      model,
            "cost_per_run":       round(cost, 4),
            "annual_cost":        round(cost * 365, 0),
            "cost_per_gb":        round(cost / max(size_gb, 0.1), 5),
            "memory_per_node_gb": p.get("memory_gb", 0),
            "vcpu_per_node":      p.get("vcpu", 0),
            "notes":              p["notes"],
            "pricing_note":       pricing_note,
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
            # For direct S3 reads, list objects to get real size + file count
            s3_info: Dict[str, Any] = {**base, "source": "s3_path"}
            try:
                bkt, pfx = _parse_s3_path(base["location"])
                sizes = _get_s3_object_sizes(bkt, pfx)
                if sizes:
                    s3_info["size_gb"]    = round(sum(sizes) / (1024 ** 3), 3)
                    s3_info["file_count"] = len(sizes)
                    s3_info["record_count"] = max(1, sum(sizes) // 500)
                    s3_info["has_stats"]  = True
                else:
                    s3_info["has_stats"]  = False
            except Exception:
                s3_info["has_stats"] = False
            enriched.append(s3_info)
        else:
            db  = base.get("database", "")
            tbl = base["table"]
            info = _glue_table_info(db, tbl) if db else {}
            if not info:
                info = _heuristic_table_info(tbl, db)
            elif not info.get("has_stats", True):
                # Glue found the table but has no row/size stats — supplement
                # record_count with a name-based heuristic so SizeAnalyzer
                # gets a non-zero baseline rather than silently computing 0 GB.
                heur = _heuristic_table_info(tbl, db)
                info["record_count"]  = heur["record_count"]
                info["size_source"]   = "glue_schema+name_heuristic"
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

    # ── Always store resolved tables so _run_one / --small-files can use them
    # regardless of whether they came from --config or auto-detection.
    _state["detected_tables"][script_path] = tables

    # ── Auto small-file scan for every table that has an S3 location ──────────
    # Runs unconditionally (no --small-files flag needed) so the results are
    # available to the recommendations context below.
    for tbl in tables:
        loc = tbl.get("location", "")
        db  = tbl.get("database", "")
        tn  = tbl.get("table", "")
        if loc.startswith("s3") or (db and tn):
            sf = detect_small_file_problem(location=loc, database=db, table_name=tn)
            if sf.get("has_problem"):
                tbl["_small_file_report"] = sf   # carry into orchestrator context

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

        # ── Inject small-file recommendations into the unified rec list ────────
        # These are based on real S3 file counts, not just code patterns, so
        # they get their own P1 entries with concrete cost-impact numbers.
        sf_recs: List[Dict] = []
        for tbl in tables:
            sf = tbl.get("_small_file_report")
            if not sf or not sf.get("has_problem"):
                continue
            fc        = sf["file_count"]
            avg_mb    = sf["avg_file_size_mb"]
            sev       = sf["severity"]          # "warning" | "critical"
            tname     = sf.get("table") or tbl.get("table", "?")
            priority  = "P0" if sev == "critical" else "P1"
            # Cost impact: each extra S3 LIST call and metadata overhead.
            # Very rough: 1000 small files ≈ 10× more S3 API calls vs 100 ideal files.
            overhead_factor = round(fc / max(fc / (size_gb * 8 + 1), 1), 1)
            sf_recs.append({
                "priority":              priority,
                "category":              "small_files",
                "title":                 f"Small-file problem: {tname}",
                "description": (
                    f"{fc:,} files averaging {avg_mb:.1f} MB "
                    f"(threshold {SMALL_FILE_THRESHOLD_MB} MB). "
                    f"Severity: {sev.upper()}. "
                    f"Each Spark task reads one file — too many small files "
                    f"causes excessive task overhead and S3 LIST cost."
                ),
                "fix":                   sf.get("recommendation", ""),
                "estimated_savings_percent": 15 if sev == "warning" else 30,
                "quick_win":             False,
                "source":                "s3_scan",
                "file_count":            fc,
                "avg_file_size_mb":      avg_mb,
            })
        if sf_recs:
            existing = result.get("all_recommendations", [])
            result["all_recommendations"] = sf_recs + existing
            result["small_file_issues"] = sf_recs

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
# Apply-recommendations tool
# =============================================================================

@strands_tool
def apply_recommendations_to_script(
    script_path: str,
    output_path: Optional[str] = None,
    dry_run: bool = False,
    glue_metrics: Optional[Dict[str, Any]] = None,
    current_workers: int = 10,
    current_worker_type: str = "G.2X",
    executor_memory_gb: float = 4.0,
) -> Dict:
    """
    Apply the optimization recommendations from the latest analysis to a script.

    Fetches the cached analysis result for *script_path* (produced by
    analyze_pyspark_script), runs RecommendationApplierAgent over the original
    source, and writes the modified script to *output_path*.

    When *glue_metrics* is provided (raw CloudWatch metric dict from
    fetch_glue_metrics or a JSON file), the agent also tunes:
      - spark.executor.memory and spark.executor.memoryOverhead (from JVM heap)
      - spark.memory.fraction (from heap pressure)
      - spark.executor.cores (from CPU utilisation)
      - spark.dynamicAllocation (from worker utilisation ratio)
      - number_of_workers recommendation (emitted in changelog)

    Args:
        script_path:          Path to the PySpark script previously analysed.
        output_path:          Where to write the fixed script (optional).
        dry_run:              If True, return the changelog without writing files.
        glue_metrics:         Raw Glue CloudWatch metric dict.
        current_workers:      Current worker count (baseline for right-sizing).
        current_worker_type:  Current Glue worker type (G.1X … G.8X).
        executor_memory_gb:   Current executor memory in GB.

    Returns dict with:
        success, fixes_applied, changelog, output_path, diff_summary,
        worker_recommendation, metric_findings
    """
    result = _state["scan_results"].get(script_path)
    if not result:
        return {
            "error": (
                f"No analysis found for '{script_path}'. "
                "Run analyze_pyspark_script first."
            )
        }

    try:
        script_content = Path(script_path).read_text(errors="replace")
    except OSError as exc:
        return {"error": f"Cannot read '{script_path}': {exc}"}

    try:
        from .agents.recommendation_applier import RecommendationApplierAgent
    except ImportError:
        from cost_optimizer.agents.recommendation_applier import RecommendationApplierAgent

    agent  = RecommendationApplierAgent(
        use_llm  = False,
        model_id = DEFAULT_MODEL_ID,
        region   = DEFAULT_REGION,
    )
    tables = _state["detected_tables"].get(script_path, [])
    out    = agent.apply(
        script_path               = script_path,
        script_content            = script_content,
        analysis_result           = result,
        source_tables             = tables,
        glue_metrics              = glue_metrics,
        current_workers           = current_workers,
        current_worker_type       = current_worker_type,
        current_executor_memory_gb= executor_memory_gb,
    )

    if not out["success"]:
        return {"error": out.get("errors", ["unknown error"])[0]}

    # Derive output path
    if not output_path:
        p           = Path(script_path)
        output_path = str(p.parent / f"{p.stem}_optimized{p.suffix}")

    if not dry_run:
        Path(output_path).write_text(out["modified_script"])

    # Compact diff summary
    orig_lines = script_content.splitlines()
    new_lines  = out["modified_script"].splitlines()
    added      = sum(1 for l in new_lines if l not in set(orig_lines))
    removed    = sum(1 for l in orig_lines if l not in set(new_lines))

    # Print metric findings if any
    for finding in out.get("metric_findings", []):
        print(f"  [Metric] {finding}")
    w_rec = out.get("worker_recommendation")
    if w_rec and w_rec.get("changed"):
        print(
            f"  [Worker] {w_rec['current_workers']}×{w_rec['current_worker_type']} → "
            f"{w_rec['recommended_workers']}×{w_rec['recommended_type']}: "
            + "; ".join(w_rec.get("reason", []))
        )

    return {
        "success":               True,
        "fixes_applied":         out["fixes_applied"],
        "output_path":           output_path if not dry_run else "(dry-run – not written)",
        "dry_run":               dry_run,
        "diff_summary":          {"lines_added": added, "lines_removed": removed},
        "changelog":             out["changelog"],
        "worker_recommendation": out.get("worker_recommendation"),
        "metric_findings":       out.get("metric_findings", []),
    }


# =============================================================================
# Job-generator tool
# =============================================================================

@strands_tool
def generate_pyspark_job(
    job_spec: Dict[str, Any],
    reference_script_path: Optional[str] = None,
    output_path: Optional[str] = None,
) -> Dict:
    """
    Generate a production-ready PySpark ETL job from a structured spec dict.

    The spec describes source tables, joins, transformations, aggregations,
    and the target table.  An optional *reference_script_path* can be provided
    so the generator mirrors the coding style of an existing job.

    Args:
        job_spec:              Dict matching the JobGeneratorAgent spec format.
        reference_script_path: Path to an existing PySpark script for style guidance.
        output_path:           Where to write the generated script.  Defaults to
                               generated_jobs/<job_name>.py.

    Returns dict with:
        success, generated_script, output_path, job_name, design_notes, errors
    """
    try:
        from .agents.job_generator import JobGeneratorAgent
    except ImportError:
        from cost_optimizer.agents.job_generator import JobGeneratorAgent

    reference = ""
    if reference_script_path:
        try:
            reference = Path(reference_script_path).read_text(errors="replace")
        except OSError as exc:
            return {"error": f"Cannot read reference script '{reference_script_path}': {exc}"}

    # Determine output path
    job_name = job_spec.get("job_name", "generated_job")
    if not output_path:
        output_path = str(Path("generated_jobs") / f"{job_name}.py")

    agent = JobGeneratorAgent(
        use_llm  = HAS_STRANDS,   # use LLM when strands is available
        model_id = DEFAULT_MODEL_ID,
        region   = DEFAULT_REGION,
    )
    result = agent.generate(
        job_spec         = job_spec,
        reference_script = reference,
        output_path      = output_path,
    )

    if result["success"]:
        print(f"\n  Generated job: {output_path}")
        lines = result["generated_script"].count("\n") + 1
        print(f"  Script length: {lines} lines")
        print(f"  Platform     : {result['platform']}")

    return result


# =============================================================================
# Glue metrics tool
# =============================================================================

@strands_tool
def fetch_glue_metrics(
    job_name: str,
    run_id: str,
    metrics_file: Optional[str] = None,
) -> Dict:
    """
    Retrieve Glue CloudWatch metrics for a specific job run.

    Either fetches live from CloudWatch (requires boto3 + cloudwatch:GetMetricData)
    or loads a pre-exported JSON file (*metrics_file*).

    Supported metrics:
      glue.ALL.jvm.heap.usage
      glue.driver.system.cpuSystemLoad
      glue.ALL.system.cpuSystemLoad
      glue.driver.workerutilized
      glue.driver.aggregate.numCompletedStages
      glue.driver.ExecutorAllocationManager.executors.numberAllExecutors

    Returns dict with:
        success, raw_metrics, analysis, spark_config_overrides,
        worker_recommendation, findings
    """
    try:
        from .agents.glue_metrics_analyzer import GlueMetricsAnalyzer
    except ImportError:
        from cost_optimizer.agents.glue_metrics_analyzer import GlueMetricsAnalyzer

    analyzer = GlueMetricsAnalyzer()

    # Load raw metrics
    if metrics_file:
        try:
            import json as _json
            with open(metrics_file) as fh:
                raw = _json.load(fh)
        except Exception as exc:
            return {"error": f"Cannot load metrics file '{metrics_file}': {exc}"}
    elif HAS_BOTO3:
        raw = analyzer.fetch_from_cloudwatch(job_name, run_id, region=DEFAULT_REGION)
        if "error" in raw:
            return raw
    else:
        return {"error": "Provide --glue-metrics FILE or install boto3 for live CloudWatch fetch"}

    # Parse + analyse
    parsed   = analyzer.parse_metrics(raw)
    analysis = analyzer.analyze(parsed)
    overrides= analyzer.get_spark_config_overrides(analysis)
    worker_r = analyzer.get_worker_recommendation(analysis, current_workers=10)

    return {
        "success":               True,
        "job_name":              job_name,
        "run_id":                run_id,
        "raw_metrics":           raw,
        "analysis":              analysis,
        "spark_config_overrides": overrides,
        "worker_recommendation": worker_r,
        "findings":              analysis.get("findings", []),
    }


# =============================================================================
# Script tester tool
# =============================================================================

@strands_tool
def generate_and_run_tests(
    script_path: str,
    test_output_dir: Optional[str] = None,
    run: bool = True,
    processing_mode: str = "full",
) -> Dict:
    """
    Generate pytest test cases for a PySpark script and optionally execute them.

    Uses the cached analysis result (from analyze_pyspark_script) to populate
    table metadata.  Runs tests against a local Spark session (no AWS needed).

    Args:
        script_path:     Path to the (optimized) PySpark script.
        test_output_dir: Directory for the generated test file.  Defaults to
                         <script_dir>/tests/.
        run:             If True, execute the tests immediately after generation.
        processing_mode: 'full' or 'delta' (affects incremental test category).

    Returns dict with:
        success, test_file_path, test_count, test_categories,
        run_result (if run=True)
    """
    try:
        from .agents.script_tester import ScriptTesterAgent
    except ImportError:
        from cost_optimizer.agents.script_tester import ScriptTesterAgent

    try:
        script_content = Path(script_path).read_text(errors="replace")
    except OSError as exc:
        return {"error": f"Cannot read '{script_path}': {exc}"}

    tables = _state["detected_tables"].get(script_path, [])

    # Also pull source_tables from analysis if not in state
    analysis = _state["scan_results"].get(script_path, {})
    if not tables:
        tables = analysis.get("source_tables", [])

    if test_output_dir:
        p           = Path(script_path)
        test_path   = str(Path(test_output_dir) / f"test_{p.stem}.py")
    else:
        test_path   = None   # ScriptTesterAgent will default to tests/ next to script

    agent  = ScriptTesterAgent(
        use_llm  = HAS_STRANDS,
        model_id = DEFAULT_MODEL_ID,
        region   = DEFAULT_REGION,
    )
    gen = agent.generate_tests(
        script_path     = script_path,
        script_content  = script_content,
        source_tables   = tables,
        output_path     = test_path,
        processing_mode = processing_mode,
    )

    if not gen.get("success"):
        return gen

    print(f"\n  Test file → {gen['test_file_path']}")
    print(f"  Tests     : {gen.get('test_count', 0)}")
    print(f"  Categories: {', '.join(gen.get('test_categories', []))}")

    result = {**gen}

    if run:
        print("  Running tests …")
        run_result = agent.run_tests(gen["test_file_path"])
        result["run_result"] = run_result
        status = "PASSED" if run_result.get("success") else "FAILED"
        total  = run_result.get("total",  0)
        passed = run_result.get("passed", 0)
        failed = run_result.get("failed", 0)
        print(f"  Result : {status}  ({passed}/{total} passed, {failed} failed)")
        if run_result.get("failed", 0) > 0:
            print("\n  Test output (last 2000 chars):")
            print(run_result.get("output", "")[-2000:])

    return result


# =============================================================================
# Glue job creation tool
# =============================================================================

@strands_tool
def create_glue_job(
    script_path: str,
    job_name: str,
    s3_bucket: str,
    iam_role_arn: str,
    s3_script_prefix: str = "glue-scripts",
    worker_type: str = "G.2X",
    number_of_workers: int = 10,
    glue_version: str = "4.0",
    timeout_minutes: int = 2880,
    max_retries: int = 1,
    connections: Optional[List[str]] = None,
    extra_job_args: Optional[Dict[str, Any]] = None,
    tags: Optional[Dict[str, str]] = None,
    start_after_create: bool = False,
    generate_docs: bool = True,
    dry_run: bool = False,
) -> Dict:
    """
    Upload an (optimized) PySpark script to S3 and create or update an AWS Glue job.

    Also generates Markdown documentation for the job.  If worker_recommendation
    is present in the cached analysis result (from --apply-fixes with --glue-metrics),
    the recommended worker count and type are applied automatically.

    Args:
        script_path:          Local path to the PySpark script to deploy.
        job_name:             AWS Glue job name.
        s3_bucket:            S3 bucket for the script.
        iam_role_arn:         IAM role ARN for the Glue job.
        s3_script_prefix:     S3 key prefix (default: glue-scripts).
        worker_type:          G.1X | G.2X | G.4X | G.8X (default: G.2X).
        number_of_workers:    Worker count (default: 10).
        glue_version:         Glue version (default: 4.0).
        timeout_minutes:      Job timeout in minutes (default: 2880 = 48 h).
        max_retries:          Retry count on failure (default: 1).
        connections:          Glue connection names list.
        extra_job_args:       Additional default job arguments.
        tags:                 AWS resource tags dict.
        start_after_create:   If True, trigger a job run immediately.
        generate_docs:        If True, write a Markdown doc file alongside the script.
        dry_run:              If True, return the payload without calling AWS.

    Returns dict with:
        success, action (created|updated|dry_run), job_name, s3_script_uri,
        console_url, worker_type, number_of_workers, docs_path, run_id (if started)
    """
    try:
        from .agents.glue_job_creator import GlueJobCreatorAgent
    except ImportError:
        from cost_optimizer.agents.glue_job_creator import GlueJobCreatorAgent

    # Pull metric-based worker recommendation from cached state if available
    worker_rec: Optional[Dict] = None
    analysis   = _state["scan_results"].get(script_path, {})
    cached_rec = analysis.get("worker_recommendation")
    if cached_rec and cached_rec.get("changed"):
        worker_rec = cached_rec
        print(
            f"  [Metric tuning] {cached_rec['current_workers']}×{cached_rec['current_worker_type']}"
            f" → {cached_rec['recommended_workers']}×{cached_rec['recommended_type']}"
        )

    creator = GlueJobCreatorAgent(region=DEFAULT_REGION)
    result  = creator.create_or_update(
        script_local_path   = script_path,
        job_name            = job_name,
        s3_bucket           = s3_bucket,
        iam_role_arn        = iam_role_arn,
        s3_script_prefix    = s3_script_prefix,
        worker_type         = worker_type,
        number_of_workers   = number_of_workers,
        glue_version        = glue_version,
        timeout_minutes     = timeout_minutes,
        max_retries         = max_retries,
        connections         = connections or [],
        extra_job_args      = extra_job_args or {},
        tags                = tags or {},
        worker_recommendation = worker_rec,
        dry_run             = dry_run,
    )

    if not result.get("success"):
        return result

    print(f"\n  Glue job {result['action']}: {job_name}")
    print(f"  Script S3 URI  : {result['s3_script_uri']}")
    print(f"  Workers        : {result['number_of_workers']}×{result['worker_type']}")
    print(f"  Console        : {result['console_url']}")

    # Generate documentation
    docs_path = None
    if generate_docs:
        script_content = ""
        try:
            script_content = Path(script_path).read_text(errors="replace")
        except OSError:
            pass

        tables = _state["detected_tables"].get(script_path, [])
        doc_md = creator.generate_job_documentation(
            job_name        = job_name,
            script_content  = script_content,
            job_definition  = result.get("job_definition", {}),
            source_tables   = tables,
            analysis_result = analysis,
        )
        docs_path = str(Path(script_path).parent / f"README_{job_name}.md")
        Path(docs_path).write_text(doc_md)
        print(f"  Documentation  : {docs_path}")

    # Optionally start a job run
    run_id = None
    if start_after_create and not dry_run:
        run_result = creator.start_job_run(job_name=job_name)
        if run_result.get("success"):
            run_id = run_result["run_id"]
            print(f"  Run ID         : {run_id}")
            print(f"  CloudWatch     : {run_result.get('cloudwatch_url', '')}")
        else:
            print(f"  [WARNING] Could not start run: {run_result.get('errors')}")

    return {**result, "docs_path": docs_path, "run_id": run_id}


# =============================================================================
# Column Lineage tool
# =============================================================================

@strands_tool
def analyze_column_lineage(
    script_path: str,
    output_dir: Optional[str] = None,
) -> Dict:
    """
    Build a column-level data lineage graph for a PySpark script.

    Tracks: withColumn, withColumnRenamed, select/alias, groupBy/agg,
            join keys, drop, source tables, and write sinks.

    Args:
        script_path: Path to the PySpark script.
        output_dir:  Optional directory to write lineage files
                     (lineage.json, lineage.mmd, lineage.dot).

    Returns dict with:
        success, edges (list), sources, sinks, mermaid (str), dot (str)
    """
    try:
        from .agents.column_lineage import ColumnLineageAgent
    except ImportError:
        from cost_optimizer.agents.column_lineage import ColumnLineageAgent

    agent  = ColumnLineageAgent()
    result = agent.analyze(script_path)

    if result.get("success") and output_dir:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        stem = Path(script_path).stem
        (out / f"{stem}_lineage.json").write_text(
            json.dumps({"edges": result["edges"], "sources": result["sources"],
                        "sinks": result["sinks"]}, indent=2)
        )
        if result.get("mermaid"):
            (out / f"{stem}_lineage.mmd").write_text(result["mermaid"])
        if result.get("dot"):
            (out / f"{stem}_lineage.dot").write_text(result["dot"])
        print(f"  Lineage files written to {output_dir}/")

    print(f"  Edges   : {len(result.get('edges', []))}")
    print(f"  Sources : {result.get('sources', [])}")
    print(f"  Sinks   : {result.get('sinks', [])}")

    return result


# =============================================================================
# Delta / Iceberg detector tool
# =============================================================================

@strands_tool
def detect_delta_iceberg(
    script_path: str,
) -> Dict:
    """
    Detect Delta Lake or Apache Iceberg usage in a PySpark script and emit
    tailored maintenance + optimization recommendations.

    Covers: OPTIMIZE, ZORDER, VACUUM, auto-optimize, Change Data Feed (Delta);
            rewrite_data_files, expire_snapshots, remove_orphan_files,
            hidden partitioning, partition evolution (Iceberg).

    Args:
        script_path: Path to the PySpark script to inspect.

    Returns dict with:
        success, format_detected ("delta"|"iceberg"|"both"|"none"),
        recommendations (list), maintenance_commands (list), code_snippets (dict)
    """
    try:
        from .agents.delta_iceberg_detector import DeltaIcebergDetectorAgent
    except ImportError:
        from cost_optimizer.agents.delta_iceberg_detector import DeltaIcebergDetectorAgent

    agent  = DeltaIcebergDetectorAgent()
    result = agent.detect(script_path)

    fmt = result.get("format_detected", "none")
    print(f"\n  Format detected  : {fmt.upper()}")
    if fmt != "none":
        print(f"  Recommendations  : {len(result.get('recommendations', []))}")
        for rec in result.get("recommendations", [])[:3]:
            print(f"    [{rec.get('priority','P?')}] {rec.get('title','')}")
        print(f"  Maintenance cmds : {len(result.get('maintenance_commands', []))}")
    else:
        print("  No Delta or Iceberg patterns detected.")

    return result


# =============================================================================
# Spark event-log parser tool
# =============================================================================

@strands_tool
def parse_spark_event_log(
    log_path: str,
    region: str = DEFAULT_REGION,
) -> Dict:
    """
    Parse a Spark event log (JSONL) from a local path or S3 URI.

    Extracts: stage durations, shuffle read/write GB, per-stage task skew
    (max/median ratio), GC overhead %, peak executor memory MB, and identifies
    bottleneck stages.

    Args:
        log_path: Local filesystem path or s3://bucket/key URI.
        region:   AWS region for S3 access (default: us-west-2).

    Returns dict with:
        success, app_name, app_duration_ms, stage_durations, shuffle_read_gb,
        shuffle_write_gb, task_skew, gc_overhead_pct, peak_executor_mb,
        bottleneck_stages, findings, recommendations
    """
    try:
        from .agents.spark_event_log_parser import SparkEventLogParser
    except ImportError:
        from cost_optimizer.agents.spark_event_log_parser import SparkEventLogParser

    parser = SparkEventLogParser()
    result = parser.parse(log_path, region=region)

    if result.get("success"):
        dur_min = result.get("app_duration_ms", 0) / 60000
        print(f"\n  App              : {result.get('app_name', '')}")
        print(f"  Duration         : {dur_min:.1f} min")
        print(f"  Stages           : {result.get('stage_count', 0)}")
        print(f"  Shuffle write    : {result.get('shuffle_write_gb', 0):.2f} GB")
        print(f"  GC overhead      : {result.get('gc_overhead_pct', 0):.1f}%")
        print(f"  Peak executor mem: {result.get('peak_executor_mb', 0):.0f} MB")
        bn = result.get("bottleneck_stages", [])
        if bn:
            print(f"  Bottleneck stages:")
            for b in bn[:3]:
                print(f"    Stage {b['stage_id']} ({b.get('name','')[:40]}): "
                      f"skew={b['skew_ratio']:.1f}x  issues={b['issues']}")
        for finding in result.get("findings", [])[:3]:
            print(f"  ⚠ {finding}")
    else:
        print(f"  [ERROR] {result.get('errors', ['Unknown error'])}")

    return result


# =============================================================================
# Deploy tests to Glue tool
# =============================================================================

@strands_tool
def deploy_tests_to_glue(
    script_path: str,
    glue_role_arn: str,
    s3_bucket: str,
    s3_prefix: str = "glue-test-scripts",
    glue_job_name: Optional[str] = None,
    glue_version: str = "4.0",
    region: str = DEFAULT_REGION,
    start_run: bool = False,
    processing_mode: str = "full",
    dry_run: bool = False,
) -> Dict:
    """
    Generate a Glue-native validation job for a PySpark script, upload it to S3,
    create/update the Glue job, and optionally trigger a run.

    Unlike the local pytest runner, this deploys checks directly to Glue so they
    run in the same environment as the production job.

    Args:
        script_path:    Local path to the script to validate.
        glue_role_arn:  IAM role ARN for the test Glue job.
        s3_bucket:      S3 bucket for the test script.
        s3_prefix:      S3 key prefix (default: glue-test-scripts).
        glue_job_name:  Override test job name (default: test_<stem>).
        glue_version:   Glue version (default: 4.0).
        region:         AWS region.
        start_run:      If True, trigger a test job run immediately.
        processing_mode: full | delta (affects incremental checks).
        dry_run:        If True, generate the script without creating anything in AWS.

    Returns dict with:
        success, glue_job_name, action, s3_script_uri, local_test_file,
        run_id (if started), cloudwatch_url
    """
    try:
        from .agents.script_tester import ScriptTesterAgent
    except ImportError:
        from cost_optimizer.agents.script_tester import ScriptTesterAgent

    try:
        script_content = Path(script_path).read_text(errors="replace")
    except OSError as exc:
        return {"error": f"Cannot read '{script_path}': {exc}"}

    tables = _state["detected_tables"].get(script_path, [])
    if not tables:
        tables = _state["scan_results"].get(script_path, {}).get("source_tables", [])

    agent  = ScriptTesterAgent(use_llm=False)
    result = agent.deploy_test_to_glue(
        script_path     = script_path,
        script_content  = script_content,
        source_tables   = tables,
        glue_role_arn   = glue_role_arn,
        s3_bucket       = s3_bucket,
        s3_prefix       = s3_prefix,
        glue_job_name   = glue_job_name,
        glue_version    = glue_version,
        region          = region,
        start_run       = start_run,
        processing_mode = processing_mode,
        dry_run         = dry_run,
    )

    job_n = result.get("glue_job_name", "")
    action = result.get("action", "dry_run" if dry_run else "unknown")
    print(f"\n  Glue test job {action}: {job_n}")
    print(f"  Local test file : {result.get('local_test_file', '')}")
    if not dry_run:
        print(f"  S3 script URI   : {result.get('s3_script_uri', '')}")
    if result.get("run_id"):
        print(f"  Run ID          : {result['run_id']}")
        print(f"  CloudWatch      : {result.get('cloudwatch_url', '')}")

    return result


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


def _write_annotated_script(
    script_path: str,
    result: Dict,
    output_path: str,
) -> None:
    """
    Write a new Python file where every flagged line has an inline comment
    block above it, and a full COST OPTIMIZER REPORT comment section at the bottom.
    """
    try:
        original = Path(script_path).read_text()
    except OSError:
        return

    lines          = original.splitlines()
    annotations    = result.get("line_annotations", [])
    recs           = result.get("all_recommendations", [])
    s              = result.get("summary", {})
    mp             = result.get("multiplatform_comparison", [])
    sf_issues      = result.get("small_file_issues", [])

    # Build a map: lineno → list[finding]
    findings_map: Dict[int, List[Dict]] = {}
    for entry in annotations:
        if "findings" in entry:
            findings_map[entry["line"]] = entry["findings"]

    SEV_ICON = {
        "critical": "🔴 CRITICAL",
        "high":     "🟠 HIGH    ",
        "medium":   "🟡 MEDIUM  ",
        "low":      "🟢 LOW     ",
    }

    out_lines: List[str] = [
        f"# ═══════════════════════════════════════════════════════════════════",
        f"# ANNOTATED SCRIPT  — generated by Cost Optimizer",
        f"# Original : {script_path}",
        f"# Generated: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"# ═══════════════════════════════════════════════════════════════════",
        "",
    ]

    for i, code in enumerate(lines, 1):
        if i in findings_map:
            out_lines.append(f"    # {'─'*60}")
            for f in findings_map[i]:
                sev_label = SEV_ICON.get(f["severity"].lower(), f["severity"].upper())
                out_lines.append(f"    # [{sev_label}]  {f['description']}")
                out_lines.append(f"    #   Fix ▶  {f['fix']}")
            out_lines.append(f"    # {'─'*60}")
        out_lines.append(code)

    # ── Summary block ─────────────────────────────────────────────────────────
    W = 70
    def _c(text: str = "") -> str:
        return f"# {text}"

    out_lines += [
        "",
        _c("═" * W),
        _c(f"  COST OPTIMIZER REPORT"),
        _c("═" * W),
        _c(),
        _c("  COST SUMMARY"),
        _c(f"  {'Metric':<40} {'Value':>20}"),
        _c("  " + "─" * 62),
        _c(f"  {'Current cost / run':<40} {'${:.4f}'.format(s.get('current_cost_per_run', 0)):>20}"),
        _c(f"  {'Optimal cost / run':<40} {'${:.4f}'.format(s.get('optimal_cost_per_run', 0)):>20}"),
        _c(f"  {'Potential savings':<40} {'{:.0f}%'.format(s.get('potential_savings_percent', 0)):>20}"),
        _c(f"  {'Potential annual savings':<40} {'${:,.0f}'.format(s.get('potential_annual_savings', 0)):>20}"),
        _c(f"  {'Effective data size':<40} {'{:.1f} GB'.format(s.get('effective_data_size_gb', 0)):>20}"),
        _c(f"  {'Anti-patterns found':<40} {str(s.get('anti_patterns_found', 0)):>20}"),
        _c(f"  {'Critical issues':<40} {str(s.get('critical_issues', 0)):>20}"),
        _c(),
    ]

    if mp:
        out_lines += [
            _c("  MULTI-PLATFORM COST COMPARISON"),
            _c(f"  {'Platform':<44} {'Cost/run':>12}  {'Monthly (20×)':>14}"),
            _c("  " + "─" * 74),
        ]
        for p in mp:
            monthly = p["cost_per_run"] * 20
            out_lines.append(
                _c(f"  {p['label']:<44} {'${:.4f}'.format(p['cost_per_run']):>12}  {'${:,.2f}'.format(monthly):>14}")
            )
        out_lines.append(_c())

    if recs:
        out_lines += [
            _c("  RECOMMENDATIONS (by priority)"),
            _c("  " + "─" * 62),
        ]
        priority_order = ["P0", "P1", "P2", "P3", "P4"]
        grouped: Dict[str, List] = {}
        for r in recs:
            p = r.get("priority", "P9")
            grouped.setdefault(p, []).append(r)
        for pri in sorted(grouped, key=lambda x: priority_order.index(x) if x in priority_order else 99):
            out_lines.append(_c(f"  [{pri}]"))
            for r in grouped[pri]:
                title = r.get("title", "")
                desc  = r.get("description", "")
                impl  = r.get("implementation", "")
                impact = r.get("estimated_impact", "")
                out_lines.append(_c(f"    • {title}"))
                if desc:
                    for part in [desc[i:i+80] for i in range(0, len(desc), 80)]:
                        out_lines.append(_c(f"      {part}"))
                if impl:
                    out_lines.append(_c(f"      Implementation: {impl[:100]}"))
                if impact:
                    out_lines.append(_c(f"      Impact: {impact}"))
            out_lines.append(_c())

    if sf_issues:
        out_lines += [
            _c("  SMALL FILE ISSUES"),
            _c("  " + "─" * 62),
        ]
        for sf in sf_issues:
            out_lines.append(_c(f"    • {sf.get('title', '')}"))
            out_lines.append(_c(f"      Files: {sf.get('file_count', 0):,}  "
                                f"Avg: {sf.get('avg_file_size_mb', 0):.1f} MB  "
                                f"Severity: {sf.get('severity', '').upper()}"))
            if sf.get("fix"):
                out_lines.append(_c(f"      Fix: {sf['fix'][:100]}"))
        out_lines.append(_c())

    out_lines += [
        _c("═" * W),
        "",
    ]

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text("\n".join(out_lines))
    print(f"  Annotated script → {output_path}")


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

You have direct access to the following 16 tools organised by capability:

DISCOVERY
  scan_scripts_in_directory    – find all PySpark scripts under a directory
  auto_detect_tables           – extract table references from a script
  detect_small_file_problem    – check a table/S3 path for small-file issues

ANALYSIS
  analyze_pyspark_script       – full 4-agent cost analysis on one script
  get_analysis_summary         – session-level summary across all analysed scripts
  detect_delta_iceberg         – detect Delta Lake / Iceberg usage and get maintenance SQL
  parse_spark_event_log        – parse a Spark event log (skew, GC, shuffle, bottlenecks)
  analyze_column_lineage       – trace column-level data flow; outputs Mermaid + DOT graphs

OPTIMISATION
  apply_recommendations_to_script – apply LLM + metric-driven fixes; write optimised .py
  fetch_glue_metrics           – pull CloudWatch metrics for a Glue job run
  get_multiplatform_cost_comparison – compare Glue / Athena / Lambda / EMR Serverless costs

JOB GENERATION
  generate_pyspark_job         – generate a new PySpark Glue script from a JSON spec

TESTING & DEPLOYMENT
  generate_and_run_tests       – generate unit tests and run them locally (needs local Spark)
  deploy_tests_to_glue         – deploy a Glue-native validation job to AWS (no local Spark needed)
  create_glue_job              – upload script to S3 and create/update a Glue job
  save_results_to_s3           – persist analysis reports to S3

Behaviour guidelines:
- Always call auto_detect_tables first when given a script, before analyze_pyspark_script.
- Prioritise P0/P1 recommendations (highest impact, easiest fix).
- Call fetch_glue_metrics before apply_recommendations_to_script when a job name is known.
- Use detect_delta_iceberg when the script writes to a table — format matters for tuning.
- Use parse_spark_event_log when the user mentions slow stages, skew, or OOM errors.
- Call get_multiplatform_cost_comparison when cost reduction is the primary concern.
- Be concise; cite line numbers where relevant.
- When saving to S3 always confirm the s3_uri to the user.
""".strip()

_INTERACTIVE_TOOLS = [
    # Discovery
    scan_scripts_in_directory,
    auto_detect_tables,
    detect_small_file_problem,
    # Analysis
    analyze_pyspark_script,
    get_analysis_summary,
    detect_delta_iceberg,
    parse_spark_event_log,
    analyze_column_lineage,
    # Optimisation
    apply_recommendations_to_script,
    fetch_glue_metrics,
    get_multiplatform_cost_comparison,
    # Job generation
    generate_pyspark_job,
    # Testing & deployment
    generate_and_run_tests,
    deploy_tests_to_glue,
    create_glue_job,
    save_results_to_s3,
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
    src = p.add_mutually_exclusive_group(required=False)
    src.add_argument("--scripts-dir", metavar="DIR",
                     help="Directory to scan for PySpark scripts")
    src.add_argument("--script", metavar="FILE",
                     help="Analyse a single .py script")
    src.add_argument("--generate-job", metavar="SPEC_FILE",
                     help="Generate a new PySpark job from a JSON spec file (skips analysis)")

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

    # ── Apply-fixes mode ─────────────────────────────────────────────────────
    p.add_argument("--apply-fixes", action="store_true",
                   help=(
                       "After analysis, apply recommendations to each analysed script "
                       "and write <script>_optimized.py alongside the original"
                   ))
    p.add_argument("--fixes-output-dir", default=None, metavar="DIR",
                   help=(
                       "Directory for optimized scripts from --apply-fixes "
                       "(default: next to each original script)"
                   ))

    # ── Glue metrics for memory/executor/worker tuning ───────────────────────
    p.add_argument("--glue-metrics", metavar="FILE",
                   help=(
                       "JSON file with Glue CloudWatch metrics from a previous run "
                       "(glue.ALL.jvm.heap.usage, glue.driver.system.cpuSystemLoad, "
                       "glue.driver.workerutilized, etc.). Used to tune executor "
                       "memory, driver settings, and worker count in --apply-fixes mode."
                   ))
    p.add_argument("--glue-job-name", metavar="NAME",
                   help="Glue job name for live CloudWatch metric fetch (used with --apply-fixes)")
    p.add_argument("--glue-run-id", metavar="RUN_ID",
                   help="Glue job run ID for live CloudWatch metric fetch")
    p.add_argument("--current-workers", type=int, default=10,
                   help="Current number_of_workers baseline for metric-based tuning (default: 10)")
    p.add_argument("--executor-memory-gb", type=float, default=4.0,
                   help="Current executor memory in GB for metric-based tuning (default: 4.0)")

    # ── Test generation mode ─────────────────────────────────────────────────
    p.add_argument("--run-tests", action="store_true",
                   help=(
                       "After analysis (and optional --apply-fixes), generate pytest "
                       "test cases for each script and run them against a local Spark session"
                   ))
    p.add_argument("--tests-output-dir", default=None, metavar="DIR",
                   help="Directory for generated test files (default: tests/ next to each script)")

    # ── Job-generation mode ──────────────────────────────────────────────────
    p.add_argument("--reference-script", metavar="FILE",
                   help="Reference PySpark script for style guidance when --generate-job is used")
    p.add_argument("--output-script", metavar="FILE",
                   help="Output path for the generated script (default: generated_jobs/<job_name>.py)")

    # ── Glue job deployment ──────────────────────────────────────────────────
    p.add_argument("--create-glue-job", action="store_true",
                   help=(
                       "After analysis + apply-fixes, upload the optimized script to S3 "
                       "and create/update an AWS Glue job"
                   ))
    p.add_argument("--glue-role-arn", metavar="ARN",
                   help="IAM role ARN for the Glue job (required with --create-glue-job)")
    p.add_argument("--glue-script-bucket", metavar="BUCKET",
                   help="S3 bucket for Glue script upload")
    p.add_argument("--glue-script-prefix", default="glue-scripts", metavar="PREFIX",
                   help="S3 key prefix for script uploads (default: glue-scripts)")
    p.add_argument("--glue-version", default="4.0",
                   choices=["4.0", "3.0", "2.0"],
                   help="Glue version (default: 4.0)")
    p.add_argument("--glue-timeout", type=int, default=2880, metavar="MINUTES",
                   help="Glue job timeout in minutes (default: 2880 = 48 h)")
    p.add_argument("--glue-max-retries", type=int, default=1,
                   help="Glue job max retries (default: 1)")
    p.add_argument("--glue-connections", nargs="+", metavar="NAME",
                   help="Glue connection names to attach to the job")
    p.add_argument("--glue-tags", metavar="JSON",
                   help='JSON string of AWS tags e.g. \'{"env":"prod","team":"data"}\'')
    p.add_argument("--start-glue-run", action="store_true",
                   help="Trigger a Glue job run immediately after create/update")
    p.add_argument("--glue-dry-run", action="store_true",
                   help="Print Glue job definition without creating anything in AWS")

    # ── Deploy tests to Glue ─────────────────────────────────────────────────
    p.add_argument("--deploy-tests-to-glue", action="store_true",
                   help=(
                       "Generate a Glue-native validation job and deploy it to AWS Glue "
                       "(uses --glue-role-arn, --glue-script-bucket, --glue-version)"
                   ))
    p.add_argument("--start-test-run", action="store_true",
                   help="Trigger the Glue test job run immediately after deployment")

    # ── Column lineage ───────────────────────────────────────────────────────
    p.add_argument("--lineage", action="store_true",
                   help="After analysis, generate a column-level lineage graph (Mermaid + DOT)")
    p.add_argument("--lineage-output-dir", default=None, metavar="DIR",
                   help="Directory for lineage output files (default: <output-dir>/lineage/)")

    # ── Delta/Iceberg detection ──────────────────────────────────────────────
    p.add_argument("--detect-table-format", action="store_true",
                   help="Detect Delta Lake / Iceberg usage and emit maintenance recommendations")

    # ── Spark event-log parser ───────────────────────────────────────────────
    p.add_argument("--spark-event-log", metavar="PATH_OR_S3_URI",
                   help=(
                       "Parse a Spark event log (JSONL) from a local path or S3 URI. "
                       "Extracts stage durations, shuffle bytes, task skew, GC overhead."
                   ))

    p.add_argument("--verbose", "-v", action="store_true",
                   help="Enable debug logging")
    p.add_argument("--show-prompts", action="store_true",
                   help=(
                       "Print full LLM prompt and response text for every agent call. "
                       "Without this flag only the first 400 chars are shown. "
                       "Always shows: call type (strands/bedrock_direct), model, region, "
                       "token counts, and any AWS credential/permission errors."
                   ))
    return p


def _load_config(config_path: str) -> List[Dict]:
    """
    Load table metadata from a config JSON file.

    Accepts three formats:
    1. Plain list:  [ {"table": "orders", "size_gb": 10}, ... ]
    2. Optimizer format:  {"tables": [ ... ]}
    3. Workload format (etl_config.json style):
         {"workload": {"data_sources": [ {"database": "db", "table": "t"}, ... ] }}
    """
    with open(config_path) as fh:
        data = json.load(fh)

    # Format 1: bare list
    if isinstance(data, list):
        return data

    # Format 2: optimizer {"tables": [...]}
    if "tables" in data:
        return data["tables"]

    # Format 3: workload format – extract from data_sources
    workload = data.get("workload", {})
    data_sources = workload.get("data_sources", [])
    if data_sources:
        tables: List[Dict] = []
        for src in data_sources:
            entry: Dict[str, Any] = {
                "table":    src.get("table", ""),
                "database": src.get("database", ""),
            }
            # Carry over size/record hints if present in the workload file
            for key in ("size_gb", "record_count", "column_count", "format",
                        "partition_column", "location", "has_skew"):
                if key in src:
                    entry[key] = src[key]
            if entry["table"]:
                tables.append(entry)
        if tables:
            return tables

    # Fallback: treat the whole object as a single table entry
    return [data]


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
    output_dir: Optional[str] = None,
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

    # ── Table size confidence report ─────────────────────────────────────────
    detected = _state["detected_tables"].get(script_path, [])
    if detected:
        real_count  = sum(1 for t in detected if t.get("has_stats", False))
        est_count   = len(detected) - real_count
        conf_label  = "HIGH" if real_count == len(detected) else (
                      "MEDIUM" if real_count > 0 else "LOW")
        print(f"\n  Table size data : {real_count}/{len(detected)} tables have real stats "
              f"[confidence: {conf_label}]")
        for t in detected:
            tname  = t.get("table", "?")
            src    = t.get("size_source", t.get("source", "unknown"))
            has_s  = t.get("has_stats", False)
            sz_lbl = (f"{t['size_gb']:.2f} GB" if "size_gb" in t
                      else f"{t.get('record_count', 0):,} rows (est)")
            fc     = f", {t['file_count']} files" if "file_count" in t else ""
            flag   = "" if has_s else "  [ESTIMATED]"
            print(f"    {'✓' if has_s else '~'} {tname:<30} {sz_lbl}{fc}  [{src}]{flag}")
        if est_count:
            print(f"  [!] {est_count} table(s) use estimated sizes. Pass --config tables.json "
                  f"or run COMPUTE STATISTICS on Glue tables for accurate recommendations.")

    s = result.get("summary", {})
    print(f"\n  Tables          : {len(detected or tables or [])}")
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

    # ── Small-file findings (always shown when detected, no flag required) ─────
    sf_issues = result.get("small_file_issues", [])
    if sf_issues:
        print(f"\n  Small-file issues ({len(sf_issues)} table(s)):")
        for sf in sf_issues:
            sev_icon = "!!" if sf.get("estimated_savings_percent", 0) >= 30 else " !"
            print(f"    [{sev_icon}] {sf['title']}")
            print(f"         {sf['file_count']:,} files  avg {sf['avg_file_size_mb']:.1f} MB  "
                  f"→ {sf.get('fix','')[:80]}")

    if show_lines and "line_annotations" in result:
        print(f"\n  Line-by-line findings:")
        _print_line_annotations(result["line_annotations"])

    # ── Write annotated script (always, when output_dir given) ───────────────
    if output_dir and result.get("line_annotations"):
        job_name   = Path(script_path).stem
        date_str   = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        ann_path   = str(Path(output_dir) / job_name / date_str / f"{job_name}_annotated.py")
        _write_annotated_script(script_path, result, ann_path)

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
        level  = logging.DEBUG if args.verbose else logging.INFO,
        format = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt= "%H:%M:%S",
    )

    # Enable full prompt/response display when --show-prompts or --verbose is passed
    show_prompts = getattr(args, "show_prompts", False) or args.verbose
    try:
        from .agents.base import set_llm_verbose as _set_llm_verbose
    except ImportError:
        try:
            from cost_optimizer.agents.base import set_llm_verbose as _set_llm_verbose
        except ImportError:
            _set_llm_verbose = None
    if _set_llm_verbose:
        _set_llm_verbose(show_prompts)

    # Validate: one of scripts-dir / script / generate-job must be provided
    if not args.scripts_dir and not args.script and not args.generate_job:
        parser.error("one of --scripts-dir, --script, or --generate-job is required")

    # Apply region / model overrides at module level so all helpers pick them up
    global DEFAULT_REGION, DEFAULT_MODEL_ID
    DEFAULT_REGION   = args.region
    DEFAULT_MODEL_ID = args.model_id

    # ── Mode A: generate a new job from a spec file ───────────────────────────
    if args.generate_job:
        return _run_generate_mode(args)

    # ── Mode B: analyse existing scripts ─────────────────────────────────────
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
            script_path       = script_path,
            tables            = config_tables or [],
            processing_mode   = args.processing_mode,
            current_config    = current_config,
            use_llm           = args.use_llm,
            show_lines        = args.show_lines,
            check_small_files = args.small_files,
            output_dir        = args.output_dir,
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

    # ── Mode C: apply fixes to analysed scripts (post-analysis) ──────────────
    if args.apply_fixes:
        _run_apply_fixes(all_results, args)

    # ── Mode D: generate + run tests for each (optionally optimized) script ──
    if args.run_tests:
        print(f"\n{'─'*65}")
        print("  GENERATING & RUNNING TESTS")
        print(f"{'─'*65}")
        for script_path in all_results:
            target = _resolved_script_path(script_path, args)
            generate_and_run_tests(
                script_path      = target,
                test_output_dir  = args.tests_output_dir,
                run              = True,
                processing_mode  = args.processing_mode,
            )

    # ── Mode D2: deploy tests as Glue validation job ──────────────────────────
    if getattr(args, "deploy_tests_to_glue", False):
        if not getattr(args, "glue_role_arn", None):
            print("[ERROR] --glue-role-arn is required with --deploy-tests-to-glue")
        elif not getattr(args, "glue_script_bucket", None):
            print("[ERROR] --glue-script-bucket is required with --deploy-tests-to-glue")
        else:
            print(f"\n{'─'*65}")
            print("  DEPLOYING GLUE VALIDATION JOB(S)")
            print(f"{'─'*65}")
            for script_path in all_results:
                if not all_results[script_path].get("success"):
                    continue
                target = _resolved_script_path(script_path, args)
                deploy_tests_to_glue(
                    script_path     = target,
                    glue_role_arn   = args.glue_role_arn,
                    s3_bucket       = args.glue_script_bucket,
                    s3_prefix       = getattr(args, "glue_script_prefix", "glue-test-scripts"),
                    glue_version    = getattr(args, "glue_version", "4.0"),
                    region          = args.region,
                    start_run       = getattr(args, "start_test_run", False),
                    processing_mode = args.processing_mode,
                    dry_run         = getattr(args, "glue_dry_run", False),
                )

    # ── Mode F: column lineage graph ──────────────────────────────────────────
    if getattr(args, "lineage", False):
        print(f"\n{'─'*65}")
        print("  COLUMN LINEAGE ANALYSIS")
        print(f"{'─'*65}")
        lin_dir = getattr(args, "lineage_output_dir", None) or str(
            Path(args.output_dir) / job_name / "lineage"
        )
        for script_path in all_results:
            analyze_column_lineage(
                script_path = script_path,
                output_dir  = lin_dir,
            )

    # ── Mode G: Delta/Iceberg detection ──────────────────────────────────────
    if getattr(args, "detect_table_format", False):
        print(f"\n{'─'*65}")
        print("  DELTA / ICEBERG FORMAT DETECTION")
        print(f"{'─'*65}")
        for script_path in all_results:
            detect_delta_iceberg(script_path=script_path)

    # ── Mode H: Spark event-log parsing ──────────────────────────────────────
    if getattr(args, "spark_event_log", None):
        print(f"\n{'─'*65}")
        print("  SPARK EVENT LOG ANALYSIS")
        print(f"{'─'*65}")
        parse_spark_event_log(
            log_path = args.spark_event_log,
            region   = args.region,
        )

    # ── Mode E: create/update Glue job from optimized script ─────────────────
    if args.create_glue_job:
        if not args.glue_role_arn:
            print("[ERROR] --glue-role-arn is required with --create-glue-job")
        elif not args.glue_script_bucket:
            print("[ERROR] --glue-script-bucket is required with --create-glue-job")
        else:
            print(f"\n{'─'*65}")
            print("  DEPLOYING GLUE JOB(S)")
            print(f"{'─'*65}")
            glue_tags = {}
            if getattr(args, "glue_tags", None):
                try:
                    glue_tags = json.loads(args.glue_tags)
                except Exception:
                    print(f"  [WARNING] Could not parse --glue-tags JSON; ignoring tags")

            for script_path in all_results:
                if not all_results[script_path].get("success"):
                    continue
                target   = _resolved_script_path(script_path, args)
                gjob     = job_name if len(all_results) == 1 else Path(script_path).stem
                gj_result= create_glue_job(
                    script_path         = target,
                    job_name            = gjob,
                    s3_bucket           = args.glue_script_bucket,
                    iam_role_arn        = args.glue_role_arn,
                    s3_script_prefix    = args.glue_script_prefix,
                    worker_type         = args.worker_type,
                    number_of_workers   = args.current_workers,
                    glue_version        = args.glue_version,
                    timeout_minutes     = args.glue_timeout,
                    max_retries         = args.glue_max_retries,
                    connections         = args.glue_connections or [],
                    tags                = glue_tags,
                    start_after_create  = args.start_glue_run,
                    generate_docs       = True,
                    dry_run             = args.glue_dry_run,
                )
                if not gj_result.get("success"):
                    print(f"  [ERROR] {gj_result.get('errors', ['unknown'])}")

    if args.interactive:
        interactive_mode()

    # ── Print LLM token usage summary (only if any LLM calls were made) ───────
    if args.use_llm and TOKEN_TRACKER is not None:
        TOKEN_TRACKER.print_summary()

    return 0


def _resolved_script_path(script_path: str, args: argparse.Namespace) -> str:
    """Return the optimized script path if --apply-fixes was used, else original."""
    if not getattr(args, "apply_fixes", False):
        return script_path
    if getattr(args, "fixes_output_dir", None):
        opt = Path(args.fixes_output_dir) / f"{Path(script_path).stem}_optimized{Path(script_path).suffix}"
    else:
        p   = Path(script_path)
        opt = p.parent / f"{p.stem}_optimized{p.suffix}"
    return str(opt) if opt.exists() else script_path


def _run_generate_mode(args: argparse.Namespace) -> int:
    """Handle --generate-job mode: read spec, call JobGeneratorAgent, write output."""
    spec_path = args.generate_job
    try:
        with open(spec_path) as fh:
            job_spec = json.load(fh)
    except Exception as exc:
        print(f"[ERROR] Cannot load job spec '{spec_path}': {exc}")
        return 1

    job_name   = job_spec.get("job_name", Path(spec_path).stem)
    out_script = args.output_script or f"generated_jobs/{job_name}.py"

    reference = ""
    if args.reference_script:
        try:
            reference = Path(args.reference_script).read_text(errors="replace")
            print(f"  Reference script: {args.reference_script}")
        except OSError as exc:
            print(f"[WARNING] Cannot read reference script: {exc}")

    print(f"\n{'─'*65}")
    print(f"  Generating PySpark job: {job_name}")
    print(f"  Platform              : {job_spec.get('platform', 'glue')}")
    print(f"  Processing mode       : {job_spec.get('processing_mode', 'full')}")
    print(f"  Source tables         : {len(job_spec.get('source_tables', []))}")
    print(f"{'─'*65}")

    result = generate_pyspark_job(
        job_spec              = job_spec,
        reference_script_path = args.reference_script,
        output_path           = out_script,
    )

    if not result.get("success"):
        print(f"[ERROR] {result.get('errors', ['Generation failed'])[0]}")
        return 1

    lines = result.get("generated_script", "").count("\n") + 1
    print(f"\n  Generated script  → {out_script}  ({lines} lines)")
    for note in result.get("design_notes", []):
        print(f"  Note: {note}")

    return 0


def _load_glue_metrics(args: argparse.Namespace) -> Optional[Dict]:
    """Load Glue metrics from file or return None."""
    if getattr(args, "glue_metrics", None):
        try:
            with open(args.glue_metrics) as fh:
                raw = json.load(fh)
            print(f"  Loaded Glue metrics from {args.glue_metrics} ({len(raw)} metric(s))")
            return raw
        except Exception as exc:
            print(f"  [WARNING] Could not load Glue metrics: {exc}")
    return None


def _run_apply_fixes(all_results: Dict[str, Any], args: argparse.Namespace) -> None:
    """Apply recommendations to each successfully analysed script."""
    print(f"\n{'─'*65}")
    print("  APPLYING FIXES")
    print(f"{'─'*65}")

    glue_metrics = _load_glue_metrics(args)
    if glue_metrics:
        print(f"  Tuning from Glue metrics: {', '.join(glue_metrics.keys())}")

    cur_workers = getattr(args, "current_workers", 10)
    cur_type    = getattr(args, "worker_type", "G.2X")
    cur_mem_gb  = getattr(args, "executor_memory_gb", 4.0)

    for script_path, result in all_results.items():
        if not result.get("success"):
            print(f"  Skipping {script_path} (analysis failed)")
            continue

        if args.fixes_output_dir:
            out = str(
                Path(args.fixes_output_dir)
                / f"{Path(script_path).stem}_optimized{Path(script_path).suffix}"
            )
        else:
            p   = Path(script_path)
            out = str(p.parent / f"{p.stem}_optimized{p.suffix}")

        fix_result = apply_recommendations_to_script(
            script_path         = script_path,
            output_path         = out,
            dry_run             = False,
            glue_metrics        = glue_metrics,
            current_workers     = cur_workers,
            current_worker_type = cur_type,
            executor_memory_gb  = cur_mem_gb,
        )
        if fix_result.get("success"):
            n = fix_result.get("fixes_applied", 0)
            print(f"  {Path(script_path).name:<40}  {n:>3} fix(es)  → {out}")
            for entry in fix_result.get("changelog", []):
                fix_type = entry.get("fix", "")
                desc     = entry.get("description", "")[:90]
                line_no  = entry.get("line", "")
                loc      = f" (L{line_no})" if line_no else ""
                print(f"      • [{fix_type}]{loc} {desc}")
        else:
            print(f"  [ERROR] {Path(script_path).name}: {fix_result.get('error')}")


if __name__ == "__main__":
    sys.exit(main())
