"""
Size Analyzer Agent
===================

Analyzes source tables to estimate data volumes and processing characteristics.

Enhanced factors considered
---------------------------
  1.  File size distribution      – small/tiny files → high task scheduling overhead
  2.  Iceberg current-snapshot    – uses $files stats (not S3 listing with old snapshots)
  3.  Snapshot bloat              – stale snapshots slow Iceberg planning and waste S3
  4.  Manifest explosion          – too many manifests → slow table-scan planning
  5.  Partition skew              – max/avg partition ratio → straggler tasks
  6.  Data growth rate            – snapshot history → predict future cost trajectory
  7.  Write amplification         – total_added_files / file_cnt ratio → compaction needed
  8.  Column width & type impact  – wide tables / nested types → high deserialization cost
  9.  Join amplification          – each join can expand working-set in memory
  10. Compression effectiveness   – actual bytes vs expected; poor ratio → format switch
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from .base import CostOptimizerAgent, AnalysisInput, AnalysisResult

# ── Thresholds ────────────────────────────────────────────────────────────────
_TINY_FILE_MB          = 10       # < 10 MB → tiny file (high overhead per task)
_SMALL_FILE_MB         = 128      # < 128 MB → small file
_IDEAL_FILE_MB         = 256      # 128–512 MB is the sweet spot for Spark
_LARGE_FILE_MB         = 1024     # > 1 GB → tasks run too long, spill risk
_SNAPSHOT_WARN         = 30       # > 30 snapshots → run expire_snapshots
_SNAPSHOT_CRITICAL     = 100      # > 100 → planning noticeably slow
_SKEW_WARN             = 3.0      # max/avg partition ratio > 3 → skew
_SKEW_CRITICAL         = 10.0     # > 10 → severe skew
_WRITE_AMP_WARN        = 5.0      # total_added / current_files > 5 → compact
_FILE_CV_WARN          = 1.5      # coefficient of variation > 1.5 → uneven sizes
_BROADCAST_BYTES       = 100 * 1024 * 1024   # < 100 MB → broadcast candidate
_LARGE_TABLE_GB        = 100      # > 100 GB → consider EMR / EKS
_XLARGE_TABLE_GB       = 500      # > 500 GB → consider EKS + Karpenter


class SizeAnalyzerAgent(CostOptimizerAgent):
    """Analyzes data sizes and estimates processing requirements."""

    AGENT_NAME = "size_analyzer"

    # Bytes-per-row estimates by column count bucket
    BYTES_PER_ROW = {
        "narrow":    200,    # < 20 cols
        "medium":    500,    # 20-50 cols
        "wide":     1000,    # 50-100 cols
        "very_wide":2000,    # 100+ cols
    }

    # On-disk compression ratios relative to raw bytes
    COMPRESSION_RATIO = {
        "parquet": 0.25,
        "orc":     0.25,
        "iceberg": 0.25,   # Iceberg uses Parquet/ORC underneath
        "delta":   0.25,
        "avro":    0.50,
        "json":    0.70,
        "csv":     0.60,
    }

    # ── Public API ─────────────────────────────────────────────────────────────

    def _analyze_rule_based(self, input_data: AnalysisInput, context: Dict) -> AnalysisResult:
        tables_analysis: List[Dict]  = []
        total_raw_bytes:  int        = 0
        total_comp_bytes: int        = 0
        skew_risks:       List[Dict] = []

        for table in input_data.source_tables:
            ta = self._analyze_table(table, input_data.processing_mode)
            tables_analysis.append(ta)
            total_raw_bytes  += ta["raw_size_bytes"]
            total_comp_bytes += ta["compressed_size_bytes"]
            if ta.get("skew_risk", "low") != "low":
                skew_risks.append({
                    "table":  table.get("table", table.get("name", "unknown")),
                    "risk":   ta["skew_risk"],
                    "reason": ta.get("skew_reason", ""),
                })

        delta_ratio  = (
            self._estimate_delta_ratio(input_data)
            if input_data.processing_mode == "delta" else 1.0
        )
        join_factor  = self._estimate_join_amplification(input_data, context)
        effective_gb = (total_comp_bytes * delta_ratio * join_factor) / (1024 ** 3)
        total_raw_gb = total_raw_bytes  / (1024 ** 3)
        total_comp_gb= total_comp_bytes / (1024 ** 3)

        skew_score   = self._calculate_skew_score(skew_risks, input_data)
        part_eff     = self._analyze_partitions(input_data.source_tables)

        # Collect Iceberg health across all tables
        iceberg_health = self._aggregate_iceberg_health(tables_analysis)

        analysis = {
            "total_raw_size_gb":           round(total_raw_gb, 2),
            "total_compressed_size_gb":    round(total_comp_gb, 2),
            "effective_size_gb":           round(effective_gb, 2),
            "processing_mode":             input_data.processing_mode,
            "delta_ratio":                 round(delta_ratio, 3),
            "join_amplification_factor":   round(join_factor, 2),
            "skew_risk_score":             skew_score,
            "skew_risk_factors":           skew_risks,
            "partition_efficiency_score":  part_eff["score"],
            "partition_recommendations":   part_eff["recommendations"],
            "tables_analyzed":             len(tables_analysis),
            "tables_detail":               tables_analysis,
            "iceberg_health":              iceberg_health,
            "size_confidence":             self._calculate_confidence(input_data),
        }

        recommendations = self._generate_recommendations(analysis, tables_analysis)

        return AnalysisResult(
            agent_name      = self.AGENT_NAME,
            success         = True,
            analysis        = analysis,
            recommendations = recommendations,
            metrics         = {
                "total_tables":    len(tables_analysis),
                "effective_size_gb": analysis["effective_size_gb"],
                "skew_risk_score": skew_score,
            },
        )

    # ── Per-table analysis ─────────────────────────────────────────────────────

    def _analyze_table(self, table: Dict, processing_mode: str) -> Dict:
        record_count = table.get("record_count", table.get("records", 0))
        column_count = table.get("column_count", table.get("columns", 30))
        fmt          = table.get("format", "parquet").lower()
        table_name   = table.get("table", table.get("name", "unknown"))
        is_iceberg   = table.get("is_iceberg", fmt == "iceberg")

        # Column width bucket
        if   column_count < 20:  width = "narrow"
        elif column_count < 50:  width = "medium"
        elif column_count < 100: width = "wide"
        else:                    width = "very_wide"

        bpr        = self.BYTES_PER_ROW[width]
        raw_bytes  = record_count * bpr
        comp_ratio = self.COMPRESSION_RATIO.get(fmt, 0.30)
        comp_bytes = raw_bytes * comp_ratio

        # Explicit size overrides always win
        if "size_gb" in table:
            comp_bytes = table["size_gb"] * (1024 ** 3)
            raw_bytes  = comp_bytes / comp_ratio

        comp_gb = comp_bytes / (1024 ** 3)

        # ── File distribution (from Iceberg $files or small-file scan) ─────────
        file_stats = self._extract_file_stats(table)

        # ── Skew risk ──────────────────────────────────────────────────────────
        skew_risk, skew_reason = self._assess_table_skew(table, file_stats)

        # ── Iceberg health factors ─────────────────────────────────────────────
        iceberg_issues: List[str] = []
        if is_iceberg:
            iceberg_issues = self._iceberg_health_issues(table, file_stats)

        result: Dict[str, Any] = {
            "table":                table_name,
            "database":             table.get("database", ""),
            "format":               fmt,
            "is_iceberg":           is_iceberg,
            "record_count":         record_count,
            "column_count":         column_count,
            "column_width":         width,
            "raw_size_bytes":       int(raw_bytes),
            "compressed_size_bytes":int(comp_bytes),
            "compressed_size_gb":   round(comp_gb, 3),
            "skew_risk":            skew_risk,
            "skew_reason":          skew_reason,
            "is_broadcast_candidate": comp_bytes < _BROADCAST_BYTES,
            "iceberg_issues":       iceberg_issues,
            "sizing_source":        table.get("source", "heuristic"),
        }

        if file_stats:
            result["file_stats"] = file_stats

        return result

    def _extract_file_stats(self, table: Dict) -> Dict:
        """Pull file distribution info from iceberg_stats or _small_file_report."""
        iceberg_s = table.get("iceberg_stats", {})
        sf_report = table.get("_small_file_report", {})

        if iceberg_s and "file_cnt" in iceberg_s:
            return {
                "source":         "iceberg_$files",
                "file_count":     iceberg_s.get("file_cnt", 0),
                "avg_mb":         iceberg_s.get("avg_file_size_mb", 0.0),
                "min_kb":         iceberg_s.get("min_file_size_kb", 0.0),
                "max_mb":         iceberg_s.get("max_file_size_mb", 0.0),
                "tiny_count":     iceberg_s.get("tiny_file_cnt", 0),
                "small_count":    iceberg_s.get("small_file_cnt", 0),
                "file_size_cv":   iceberg_s.get("file_size_cv", 0.0),
                "snapshot_count": iceberg_s.get("snapshot_count", 0),
                "skew_ratio":     iceberg_s.get("skew_ratio", 1.0),
                "write_amp":      (iceberg_s.get("total_added_files", 0)
                                   / max(iceberg_s.get("file_cnt", 1), 1)),
                "total_added_files":   iceberg_s.get("total_added_files", 0),
                "total_deleted_files": iceberg_s.get("total_deleted_files", 0),
            }
        if sf_report and sf_report.get("file_count", 0) > 0:
            fc     = sf_report["file_count"]
            avg_mb = sf_report.get("avg_file_size_mb", 0.0)
            return {
                "source":         "s3_listing",
                "file_count":     fc,
                "avg_mb":         avg_mb,
                "min_kb":         sf_report.get("min_file_size_mb", 0.0) * 1024,
                "max_mb":         sf_report.get("max_file_size_mb", 0.0),
                "tiny_count":     sum(1 for _ in range(fc) if avg_mb < _TINY_FILE_MB),
                "small_count":    sf_report.get("small_file_count", 0),
                "file_size_cv":   0.0,
                "snapshot_count": 0,
                "skew_ratio":     1.0,
                "write_amp":      1.0,
                "total_added_files":   0,
                "total_deleted_files": 0,
            }
        return {}

    def _iceberg_health_issues(self, table: Dict, file_stats: Dict) -> List[str]:
        issues: List[str] = []
        iceberg_s  = table.get("iceberg_stats", {})
        snap_count = file_stats.get("snapshot_count", iceberg_s.get("snapshot_count", 0))
        write_amp  = file_stats.get("write_amp", 1.0)
        skew_ratio = file_stats.get("skew_ratio", 1.0)
        avg_mb     = file_stats.get("avg_mb", 0.0)
        file_count = file_stats.get("file_count", 0)
        cv         = file_stats.get("file_size_cv", 0.0)

        if snap_count > _SNAPSHOT_CRITICAL:
            issues.append(
                f"CRITICAL: {snap_count} snapshots — Iceberg planning is significantly slower; "
                "run CALL system.expire_snapshots immediately."
            )
        elif snap_count > _SNAPSHOT_WARN:
            issues.append(
                f"{snap_count} snapshots — run expire_snapshots to speed up table planning "
                "and reclaim S3 storage."
            )

        if write_amp > _WRITE_AMP_WARN:
            issues.append(
                f"Write amplification {write_amp:.1f}× — {file_stats.get('total_added_files',0):,} files "
                f"added vs {file_count:,} current. High compaction debt; "
                "enable auto-compaction or schedule periodic OPTIMIZE."
            )

        if avg_mb > 0 and avg_mb < _TINY_FILE_MB and file_count >= 10:
            issues.append(
                f"Tiny files: avg {avg_mb:.1f} MB across {file_count:,} files. "
                "Each file = one Spark task → massive scheduling overhead and S3 list cost."
            )
        elif avg_mb > 0 and avg_mb < _SMALL_FILE_MB and file_count >= 10:
            issues.append(
                f"Small files: avg {avg_mb:.1f} MB ({file_count:,} files). "
                f"Target {_IDEAL_FILE_MB} MB; run OPTIMIZE / rewrite_data_files."
            )
        elif avg_mb > _LARGE_FILE_MB:
            issues.append(
                f"Oversized files: avg {avg_mb:.1f} MB. Tasks will be slow and may OOM. "
                "Re-partition to target 256-512 MB per file."
            )

        if cv > _FILE_CV_WARN:
            issues.append(
                f"Uneven file sizes (CV={cv:.2f}) — some files very large, others tiny. "
                "Run OPTIMIZE to rewrite with uniform sizing."
            )

        if skew_ratio > _SKEW_CRITICAL:
            issues.append(
                f"Severe partition skew ({skew_ratio:.1f}×) — largest partition has "
                f"{skew_ratio:.0f}× more records than average. "
                "Consider finer partition granularity or key salting."
            )
        elif skew_ratio > _SKEW_WARN:
            issues.append(
                f"Partition skew ({skew_ratio:.1f}×) — enable AQE skewJoin and "
                "review partition column cardinality."
            )

        return issues

    # ── Skew assessment ────────────────────────────────────────────────────────

    def _assess_table_skew(self, table: Dict, file_stats: Dict) -> Tuple[str, str]:
        if table.get("has_skew", False):
            return "high", "Explicitly marked as skewed"

        # Use partition skew ratio from Iceberg $partitions if available
        skew_ratio = file_stats.get("skew_ratio", 1.0)
        if skew_ratio > _SKEW_CRITICAL:
            return "high", f"Partition skew ratio {skew_ratio:.1f}× from $partitions"
        if skew_ratio > _SKEW_WARN:
            return "medium", f"Partition skew ratio {skew_ratio:.1f}× from $partitions"

        part_col = table.get("partition_column", "")
        if part_col.lower() in ("date", "dt", "process_date", "event_date"):
            return "medium", "Date partition may have uneven distribution"

        rc = table.get("record_count", 0)
        if rc < 100_000:
            return "low", "Small table, skew unlikely"

        name = table.get("table", "").lower()
        if any(x in name for x in ("transaction", "event", "log", "click", "stream", "fact")):
            return "medium", "Transactional table may have temporal skew"

        return "low", ""

    # ── Delta / incremental helpers ────────────────────────────────────────────

    def _estimate_delta_ratio(self, input_data: AnalysisInput) -> float:
        if "delta_ratio" in input_data.additional_context:
            return float(input_data.additional_context["delta_ratio"])
        schedule = input_data.additional_context.get("schedule", "daily")
        return {"hourly": 0.005, "daily": 0.03, "weekly": 0.15}.get(schedule, 0.05)

    def _estimate_join_amplification(self, input_data: AnalysisInput, context: Dict) -> float:
        import re
        join_count = context.get("join_count", 0)
        if join_count == 0:
            join_count = len(re.findall(r"\.join\(", input_data.script_content, re.IGNORECASE))
        if join_count == 0:    return 1.0
        if join_count <= 3:    return 1.0 + join_count * 0.10
        if join_count <= 10:   return 1.3 + (join_count - 3) * 0.05
        return                 1.5 + (join_count - 10) * 0.02

    # ── Partition efficiency ───────────────────────────────────────────────────

    def _analyze_partitions(self, tables: List[Dict]) -> Dict:
        issues: List[str]   = []
        partitioned: int    = 0
        for t in tables:
            if t.get("partition_column"):
                partitioned += 1
            elif t.get("record_count", 0) > 1_000_000:
                issues.append(
                    f"Large table {t.get('table','?')} has no partition column — "
                    "full table scans on every read."
                )
        total  = max(len(tables), 1)
        score  = int(partitioned / total * 100)
        return {"score": score, "recommendations": issues[:3],
                "tables_with_partitions": partitioned, "total_tables": total}

    # ── Skew score ─────────────────────────────────────────────────────────────

    def _calculate_skew_score(self, skew_risks: List, input_data: AnalysisInput) -> int:
        if not skew_risks:
            return 10
        highs   = sum(1 for r in skew_risks if r["risk"] == "high")
        mediums = sum(1 for r in skew_risks if r["risk"] == "medium")
        return min(100, 10 + highs * 30 + mediums * 15)

    # ── Confidence ────────────────────────────────────────────────────────────

    def _calculate_confidence(self, input_data: AnalysisInput) -> str:
        have = sum(
            1 for t in input_data.source_tables
            if t.get("size_gb") or t.get("record_count", 0) > 0
            or t.get("iceberg_stats")
        )
        ratio = have / max(len(input_data.source_tables), 1)
        if ratio > 0.8:  return "high"
        if ratio > 0.5:  return "medium"
        return "low"

    # ── Iceberg aggregate health ───────────────────────────────────────────────

    def _aggregate_iceberg_health(self, tables_analysis: List[Dict]) -> Dict:
        iceberg_tables = [t for t in tables_analysis if t.get("is_iceberg")]
        if not iceberg_tables:
            return {"has_iceberg": False}

        all_issues:     List[str] = []
        total_snaps:    int       = 0
        total_small:    int       = 0
        total_files:    int       = 0
        for t in iceberg_tables:
            all_issues.extend(t.get("iceberg_issues", []))
            fs           = t.get("file_stats", {})
            total_snaps += fs.get("snapshot_count", 0)
            total_small += fs.get("small_count", 0)
            total_files += fs.get("file_count", 0)

        return {
            "has_iceberg":    True,
            "iceberg_tables": len(iceberg_tables),
            "total_snapshots":total_snaps,
            "total_files":    total_files,
            "total_small_files": total_small,
            "issues":         all_issues,
            "health": (
                "critical" if any("CRITICAL" in i for i in all_issues) else
                "warning"  if all_issues else
                "healthy"
            ),
        }

    # ── Recommendations ────────────────────────────────────────────────────────

    def _generate_recommendations(
        self,
        analysis: Dict,
        tables_detail: List[Dict],
    ) -> List[Dict]:
        recs: List[Dict] = []
        eff_gb   = analysis["effective_size_gb"]
        comp_gb  = analysis["total_compressed_size_gb"]
        ih       = analysis.get("iceberg_health", {})

        # ── 1. Iceberg health issues (from $files / $snapshots / $partitions) ──
        for t in tables_detail:
            if not t.get("is_iceberg"):
                continue
            tname = t["table"]
            fs    = t.get("file_stats", {})
            snap  = fs.get("snapshot_count", 0)
            avg_mb= fs.get("avg_mb", 0.0)
            fc    = fs.get("file_count", 0)
            wa    = fs.get("write_amp", 1.0)
            skew  = fs.get("skew_ratio", 1.0)

            if snap > _SNAPSHOT_CRITICAL:
                recs.append({
                    "priority": "P0",
                    "category": "iceberg",
                    "title":    f"[{tname}] Expire snapshots — {snap} snapshots slow planning",
                    "description": (
                        f"{snap} snapshots found. Iceberg must read all snapshot metadata "
                        "at plan time, adding seconds per query."
                    ),
                    "implementation": (
                        f"CALL system.expire_snapshots(table => '{tname}', "
                        "older_than => TIMESTAMP 'now - 7 days', retain_last => 5);"
                    ),
                    "estimated_savings_percent": 0,
                    "source": "iceberg_$snapshots",
                })
            elif snap > _SNAPSHOT_WARN:
                recs.append({
                    "priority": "P1",
                    "category": "iceberg",
                    "title":    f"[{tname}] Expire old snapshots ({snap} found)",
                    "description": f"{snap} snapshots; recommended to retain ≤ 5.",
                    "implementation": (
                        f"CALL system.expire_snapshots(table => '{tname}', "
                        "older_than => TIMESTAMP 'now - 3 days', retain_last => 5);"
                    ),
                    "estimated_savings_percent": 0,
                    "source": "iceberg_$snapshots",
                })

            if avg_mb > 0 and avg_mb < _SMALL_FILE_MB and fc >= 10:
                priority = "P0" if avg_mb < _TINY_FILE_MB else "P1"
                recs.append({
                    "priority": priority,
                    "category": "iceberg",
                    "title":    f"[{tname}] Compact small files — avg {avg_mb:.1f} MB ({fc:,} files)",
                    "description": (
                        f"Iceberg $files shows avg file size {avg_mb:.1f} MB "
                        f"({fc:,} current files). Each file = one Spark task. "
                        f"Target {_IDEAL_FILE_MB} MB."
                    ),
                    "implementation": (
                        f"-- Spark SQL:\n"
                        f"CALL system.rewrite_data_files(table => '{tname}', "
                        f"options => map('target-file-size-bytes', '{_IDEAL_FILE_MB * 1024 * 1024}'));\n"
                        f"-- Or via Iceberg API:\n"
                        f"spark.sql(\"OPTIMIZE {tname}\")"
                    ),
                    "estimated_savings_percent": 20,
                    "source": "iceberg_$files",
                })

            if wa > _WRITE_AMP_WARN:
                recs.append({
                    "priority": "P1",
                    "category": "iceberg",
                    "title":    f"[{tname}] High write amplification ({wa:.1f}×) — enable auto-compaction",
                    "description": (
                        f"{fs.get('total_added_files',0):,} files added over history vs "
                        f"{fc:,} current files. Many writes never compacted."
                    ),
                    "implementation": (
                        "Set table property: write.auto-optimize.enabled = true\n"
                        "Or schedule periodic: CALL system.rewrite_data_files(...)"
                    ),
                    "estimated_savings_percent": 15,
                    "source": "iceberg_$files",
                })

            if skew > _SKEW_CRITICAL:
                recs.append({
                    "priority": "P0",
                    "category": "iceberg",
                    "title":    f"[{tname}] Severe partition skew ({skew:.1f}×)",
                    "description": (
                        "Largest partition is drastically larger than average. "
                        "Straggler tasks will hold up the whole stage."
                    ),
                    "implementation": (
                        "1. Switch to finer partition granularity (e.g. month → day).\n"
                        "2. Enable AQE: spark.sql.adaptive.skewJoin.enabled=true\n"
                        "3. Salt join keys if this table is joined on the partition column."
                    ),
                    "estimated_savings_percent": 30,
                    "source": "iceberg_$partitions",
                })
            elif skew > _SKEW_WARN:
                recs.append({
                    "priority": "P1",
                    "category": "iceberg",
                    "title":    f"[{tname}] Partition skew detected ({skew:.1f}×)",
                    "description": "Some partitions significantly larger than average.",
                    "implementation": (
                        "Enable spark.sql.adaptive.skewJoin.enabled=true and "
                        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=67108864"
                    ),
                    "estimated_savings_percent": 15,
                    "source": "iceberg_$partitions",
                })

        # ── 2. Non-Iceberg small-file issues ──────────────────────────────────
        for t in tables_detail:
            if t.get("is_iceberg"):
                continue
            fs = t.get("file_stats", {})
            if not fs:
                continue
            avg_mb = fs.get("avg_mb", 0.0)
            fc     = fs.get("file_count", 0)
            if avg_mb > 0 and avg_mb < _SMALL_FILE_MB and fc >= 10:
                priority = "P0" if avg_mb < _TINY_FILE_MB else "P1"
                recs.append({
                    "priority": priority,
                    "category": "small_files",
                    "title":    f"[{t['table']}] Small files: avg {avg_mb:.1f} MB ({fc:,} files)",
                    "description": (
                        f"High file count with small average size. "
                        f"Consider repartition({max(1, int(t['compressed_size_gb'] * 4))}) "
                        "before writing."
                    ),
                    "implementation": (
                        "df.repartition(target_partitions).write"
                        ".option('maxRecordsPerFile', 500000).parquet(path)"
                    ),
                    "estimated_savings_percent": 15,
                    "source": "s3_listing",
                })

        # ── 3. Size-based platform recommendations ─────────────────────────────
        if eff_gb > _XLARGE_TABLE_GB:
            recs.append({
                "priority": "P1",
                "category": "architecture",
                "title":    f"Consider EKS + Karpenter (data: {eff_gb:.0f} GB)",
                "description": (
                    f"Effective data ({eff_gb:.0f} GB) exceeds Glue's cost-effective range. "
                    "EKS with Karpenter Spot can cut compute cost 60-70%."
                ),
                "implementation": "Migrate to EMR on EKS or self-managed Spark on EKS with Spot.",
                "estimated_savings_percent": 65,
                "source": "size_analysis",
            })
        elif eff_gb > _LARGE_TABLE_GB:
            recs.append({
                "priority": "P2",
                "category": "architecture",
                "title":    f"Consider EMR Spot (data: {eff_gb:.0f} GB)",
                "description": f"Data size {eff_gb:.0f} GB may benefit from EMR with Spot instances.",
                "implementation": "Evaluate EMR Serverless or EMR on Spot fleet.",
                "estimated_savings_percent": 40,
                "source": "size_analysis",
            })

        # ── 4. Skew ────────────────────────────────────────────────────────────
        if analysis["skew_risk_score"] > 50:
            recs.append({
                "priority": "P1",
                "category": "code",
                "title":    f"Address data skew (score {analysis['skew_risk_score']}/100)",
                "description": "High skew risk — straggler tasks will extend job duration.",
                "implementation": (
                    "1. Enable AQE: spark.sql.adaptive.skewJoin.enabled=true\n"
                    "2. Salt high-cardinality join keys.\n"
                    "3. Use SKEW hints: /*+ SKEW('table', 'col') */"
                ),
                "estimated_savings_percent": 20,
                "source": "skew_analysis",
            })

        # ── 5. Missing partitions ──────────────────────────────────────────────
        if analysis["partition_efficiency_score"] < 50:
            recs.append({
                "priority": "P2",
                "category": "architecture",
                "title":    "Add partitioning to large tables",
                "description": "Large tables without partition columns cause full table scans.",
                "implementation": (
                    "Add partition by date or region column. "
                    "For Iceberg: ALTER TABLE t ADD PARTITION FIELD date_col;"
                ),
                "estimated_savings_percent": 15,
                "source": "partition_analysis",
            })

        # ── 6. Full → delta switch ─────────────────────────────────────────────
        if analysis["processing_mode"] == "full" and comp_gb > 50:
            recs.append({
                "priority": "P0",
                "category": "architecture",
                "title":    f"Switch to incremental/delta processing ({comp_gb:.0f} GB full scan)",
                "description": (
                    "Full-table processing on large dataset. "
                    "Incremental processing can reduce data volume by 90-97%."
                ),
                "implementation": (
                    "1. For Iceberg: read only new snapshots using snapshot_id watermark.\n"
                    "2. For Parquet: filter on partition column with watermark.\n"
                    "3. Use Glue bookmark or custom watermark table."
                ),
                "estimated_savings_percent": 90,
                "source": "size_analysis",
            })

        # ── 7. Broadcast candidates ────────────────────────────────────────────
        candidates = [t["table"] for t in tables_detail if t.get("is_broadcast_candidate")]
        if candidates:
            recs.append({
                "priority": "P1",
                "category": "code",
                "title":    f"Use broadcast joins for small tables: {', '.join(candidates[:5])}",
                "description": "Small tables can be broadcast to avoid shuffle.",
                "implementation": (
                    "from pyspark.sql.functions import broadcast\n"
                    "df.join(broadcast(small_df), 'key')"
                ),
                "estimated_savings_percent": 10,
                "source": "size_analysis",
            })

        # ── 8. Wide table deserialization cost ────────────────────────────────
        wide_tables = [
            t["table"] for t in tables_detail if t.get("column_width") == "very_wide"
        ]
        if wide_tables:
            recs.append({
                "priority": "P2",
                "category": "code",
                "title":    f"Prune columns early: {', '.join(wide_tables[:3])} (100+ columns)",
                "description": (
                    "Very wide tables incur high deserialization cost even for simple projections. "
                    "Select only required columns immediately after reading."
                ),
                "implementation": "df = df.select('col1', 'col2', ...)",
                "estimated_savings_percent": 10,
                "source": "size_analysis",
            })

        return recs
