"""
Recommendation Applier Agent
==============================
Applies optimization recommendations to existing PySpark scripts.

Rule-based mode
  - Injects recommended Spark configs (AQE, KryoSerializer, shuffle tuning)
    into the SparkSession builder chain
  - Replaces .repartition(1) with .coalesce(1) (no full shuffle needed)
  - Adds .coalesce() hint comment before write operations lacking one
  - Adds broadcast() hints for small / flagged tables inside .join() calls
  - Annotates .collect() calls on potentially large DataFrames
  - Annotates UDF definitions with built-in function suggestions
  - Inserts .cache() before DataFrames used by multiple actions

LLM mode
  - Sends full script + analysis results to Claude; receives a complete
    rewritten script with every recommendation applied and a changelog.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from .base import CostOptimizerAgent, AnalysisInput, AnalysisResult


# ─── Recommended Spark configuration values ───────────────────────────────────

_SPARK_CONFIGS: Dict[str, str] = {
    "spark.sql.adaptive.enabled":                       "true",
    "spark.sql.adaptive.coalescePartitions.enabled":    "true",
    "spark.sql.adaptive.skewJoin.enabled":              "true",
    "spark.sql.adaptive.localShuffleReader.enabled":    "true",
    "spark.sql.autoBroadcastJoinThreshold":             "10485760",   # 10 MB
    "spark.sql.broadcastTimeout":                       "300",
    "spark.sql.shuffle.partitions":                     "auto",
    "spark.sql.files.maxPartitionBytes":                "134217728",  # 128 MB
    "spark.sql.adaptive.advisoryPartitionSizeInBytes":  "134217728",
    "spark.serializer":     "org.apache.spark.serializer.KryoSerializer",
    "spark.kryoserializer.buffer.max":                  "512m",
}

# Broadcast small tables whose record count is below this threshold
_BROADCAST_ROW_THRESHOLD = 500_000


class RecommendationApplierAgent(CostOptimizerAgent):
    """Apply analysis recommendations to a PySpark script."""

    AGENT_NAME = "recommendation_applier"

    # ─── Public entry point ────────────────────────────────────────────────────

    def apply(
        self,
        script_path: str,
        script_content: str,
        analysis_result: Dict[str, Any],
        source_tables: Optional[List[Dict]] = None,
    ) -> Dict[str, Any]:
        """
        Apply recommendations to *script_content*.

        Returns:
            modified_script  – transformed source code (str)
            original_script  – unchanged original (str)
            changelog        – list of fix dicts {line, fix, description, ...}
            fixes_applied    – total number of fixes (int)
            success          – bool
            errors           – list[str]
        """
        input_data = AnalysisInput(
            script_path     = script_path,
            script_content  = script_content,
            source_tables   = source_tables or [],
            processing_mode = "full",
            current_config  = {},
        )
        result = self.analyze(input_data, context={"analysis_result": analysis_result})
        return {
            "success":         result.success,
            "modified_script": result.analysis.get("modified_script", script_content),
            "original_script": script_content,
            "changelog":       result.recommendations,
            "fixes_applied":   result.analysis.get("fixes_count", 0),
            "errors":          result.errors,
        }

    # ─── Rule-based core ──────────────────────────────────────────────────────

    def _analyze_rule_based(
        self, input_data: AnalysisInput, context: Dict
    ) -> AnalysisResult:
        code      = input_data.script_content
        tables    = input_data.source_tables
        analysis  = context.get("analysis_result", {})
        changelog: List[Dict] = []

        code = self._inject_spark_configs(code, changelog)
        code = self._fix_repartition_1(code, changelog)
        code = self._add_coalesce_before_write(code, changelog)
        code = self._add_broadcast_hints(code, tables, changelog)
        code = self._annotate_collect_calls(code, changelog)
        code = self._annotate_udf_usage(code, changelog)
        code = self._add_cache_before_multi_action(code, changelog)
        code = self._apply_analysis_refactoring(code, analysis, changelog)

        return AnalysisResult(
            agent_name      = self.AGENT_NAME,
            success         = True,
            analysis        = {
                "modified_script": code,
                "original_script": input_data.script_content,
                "changelog":       changelog,
                "fixes_count":     len(changelog),
            },
            recommendations = changelog,
        )

    # ─── LLM prompt override ──────────────────────────────────────────────────

    def _build_llm_prompt(self, input_data: AnalysisInput, context: Dict) -> str:
        import json
        analysis = context.get("analysis_result", {})
        return f"""
You are a Senior PySpark Engineer tasked with applying optimization recommendations
to the following script.  Produce a COMPLETE, runnable, modified version of the script
with every recommendation applied, followed by a JSON changelog.

## Original Script  ({input_data.script_path})
```python
{input_data.script_content}
```

## Analysis Recommendations
```json
{json.dumps(analysis, indent=2, default=str)}
```

## Source Tables
```json
{json.dumps(input_data.source_tables, indent=2)}
```

## Required Changes (apply ALL of these)
1. Inject AQE Spark configs into the SparkSession builder.
2. Replace `.repartition(1)` with `.coalesce(1)` where appropriate.
3. Add `broadcast()` hints for small tables (< 500k rows) inside `.join()` calls.
4. Add `.cache()` before DataFrames used by multiple actions.
5. Add `.coalesce(N)` before write operations lacking one.
6. Replace UDFs with equivalent pyspark.sql.functions where feasible.
7. Apply every `code_refactoring` entry from the analysis JSON above.

## Output Format
Return a JSON object with:
{{
  "modified_script": "<complete modified Python source>",
  "changelog": [
    {{"line": <int>, "fix": "<type>", "description": "<what changed and why>"}}
  ]
}}
"""

    # ─── Individual fix helpers ────────────────────────────────────────────────

    def _inject_spark_configs(self, code: str, log: List[Dict]) -> str:
        """Insert recommended Spark configs after the SparkSession builder chain."""
        lines = code.splitlines()

        # Find last line of the SparkSession builder (.getOrCreate / .enableHiveSupport)
        builder_end_re = re.compile(r'\.(getOrCreate|enableHiveSupport)\s*\(')
        insert_after   = -1
        for i, line in enumerate(lines):
            if builder_end_re.search(line):
                insert_after = i

        if insert_after == -1:
            log.append({
                "line":        0,
                "fix":         "spark_config_manual",
                "description": (
                    "SparkSession builder not found – add these configs manually:\n"
                    + "\n".join(
                        f'spark.conf.set("{k}", "{v}")'
                        for k, v in _SPARK_CONFIGS.items()
                    )
                ),
            })
            return code

        # Which configs are already set?
        existing = set(re.findall(r'spark\.conf\.set\(["\']([^"\']+)', code))
        missing  = {k: v for k, v in _SPARK_CONFIGS.items() if k not in existing}
        if not missing:
            return code

        # Detect indentation from the builder line or use 4 spaces
        indent_m = re.match(r'^(\s+)', lines[insert_after])
        indent   = indent_m.group(1) if indent_m else "    "

        config_block = ["", f"{indent}# ── Optimizer-injected Spark configs ──────────────────────"]
        for k, v in missing.items():
            config_block.append(f'{indent}spark.conf.set("{k}", "{v}")')

        new_lines = lines[: insert_after + 1] + config_block + lines[insert_after + 1 :]
        log.append({
            "line":        insert_after + 1,
            "fix":         "spark_configs_injected",
            "description": f"Injected {len(missing)} Spark config(s): AQE, KryoSerializer, shuffle/partition tuning",
            "configs":     list(missing.keys()),
        })
        return "\n".join(new_lines)

    def _fix_repartition_1(self, code: str, log: List[Dict]) -> str:
        """Replace .repartition(1) → .coalesce(1) to avoid a full shuffle."""
        result = []
        for i, line in enumerate(code.splitlines(), 1):
            fixed = re.sub(r'\.repartition\(\s*1\s*\)', '.coalesce(1)', line)
            if fixed != line:
                log.append({
                    "line":        i,
                    "fix":         "repartition_to_coalesce",
                    "description": (
                        "Replaced .repartition(1) with .coalesce(1) – avoids a full shuffle "
                        "when collapsing to a single partition for write"
                    ),
                    "original":    line.strip(),
                    "fixed":       fixed.strip(),
                })
            result.append(fixed)
        return "\n".join(result)

    def _add_coalesce_before_write(self, code: str, log: List[Dict]) -> str:
        """Add a TODO comment before write calls that lack coalesce/repartition."""
        lines  = code.splitlines()
        result = []
        for i, line in enumerate(lines):
            if re.search(
                r'\.write\s*\.\s*(parquet|orc|csv|json|save|saveAsTable|format)\s*\(', line
            ):
                lookback = "\n".join(lines[max(0, i - 4): i])
                if not re.search(r'\.(coalesce|repartition)\s*\(', lookback):
                    indent = re.match(r'^(\s*)', line).group(1)
                    result.append(
                        f"{indent}"
                        "# TODO [optimizer]: add .coalesce(N) before .write to control "
                        "output file count and prevent the small-file problem"
                    )
                    log.append({
                        "line":        i + 1,
                        "fix":         "coalesce_before_write",
                        "description": "Missing coalesce/repartition before write – may produce many small files",
                    })
            result.append(line)
        return "\n".join(result)

    def _add_broadcast_hints(
        self, code: str, tables: List[Dict], log: List[Dict]
    ) -> str:
        """Add broadcast() hints around small-table references inside .join() calls."""
        broadcast_names = {
            t.get("table", "").lower()
            for t in tables
            if t.get("broadcast") or int(t.get("record_count", 9_999_999)) < _BROADCAST_ROW_THRESHOLD
        }
        if not broadcast_names:
            return code

        lines  = code.splitlines()
        result = []
        for i, line in enumerate(lines, 1):
            fixed = line
            if ".join(" in line:
                for tbl in broadcast_names:
                    # Match bare variable references that aren't already wrapped
                    pat = rf'(?<!\bbroadcast\()(\b(?:{re.escape(tbl)}(?:_df|_data)?)\b)'
                    if re.search(pat, fixed, re.IGNORECASE):
                        new = re.sub(
                            pat, r'broadcast(\1)', fixed, count=1, flags=re.IGNORECASE
                        )
                        if new != fixed:
                            log.append({
                                "line":        i,
                                "fix":         "broadcast_hint",
                                "description": f"Added broadcast() hint for small table '{tbl}' in join",
                                "original":    line.strip(),
                                "fixed":       new.strip(),
                            })
                            fixed = new
            result.append(fixed)
        return "\n".join(result)

    def _annotate_collect_calls(self, code: str, log: List[Dict]) -> str:
        """Prepend a warning comment to every .collect() call."""
        lines  = code.splitlines()
        result = []
        for i, line in enumerate(lines, 1):
            if re.search(r'\.collect\(\)', line) and not line.strip().startswith("#"):
                indent = re.match(r'^(\s*)', line).group(1)
                result.append(
                    f"{indent}# WARNING [optimizer]: .collect() pulls ALL data to the driver; "
                    "ensure the DataFrame is pre-filtered / aggregated to a small size"
                )
                log.append({
                    "line":        i,
                    "fix":         "collect_warning",
                    "description": ".collect() may cause driver OOM on large DataFrames",
                })
            result.append(line)
        return "\n".join(result)

    def _annotate_udf_usage(self, code: str, log: List[Dict]) -> str:
        """Prepend a suggestion comment above UDF definitions/decorators."""
        lines  = code.splitlines()
        result = []
        for i, line in enumerate(lines, 1):
            if re.search(r'@udf\b|@pandas_udf\b|= udf\s*\(|=udf\s*\(', line) and \
               not line.strip().startswith("#"):
                indent = re.match(r'^(\s*)', line).group(1)
                result.append(
                    f"{indent}# SUGGESTION [optimizer]: Replace this UDF with a "
                    "pyspark.sql.functions built-in where possible – UDFs are "
                    "10-100× slower (Catalyst cannot optimize them)"
                )
                log.append({
                    "line":        i,
                    "fix":         "udf_suggestion",
                    "description": "UDF detected – consider pyspark.sql.functions alternative for 10-100× speedup",
                })
            result.append(line)
        return "\n".join(result)

    def _add_cache_before_multi_action(self, code: str, log: List[Dict]) -> str:
        """
        Insert .cache() before the first action on a DataFrame variable that
        is referenced by 2+ action calls (.count, .show, .collect, .write, etc.).
        Only considers snake_case identifiers containing 'df' to reduce false positives.
        """
        lines     = code.splitlines()
        action_re = re.compile(r'\.(count|show|collect|write|toPandas|first|take|head)\s*\(')
        df_var_re = re.compile(r'^[a-z][a-zA-Z0-9_]*$')

        # Collect all variables assigned a DataFrame-like value
        assigned: Dict[str, int] = {}
        for i, line in enumerate(lines):
            m = re.match(r'^\s*([a-z_][a-zA-Z0-9_]*)\s*=\s*[^=]', line)
            if m:
                var = m.group(1)
                if ("df" in var or var.endswith("_data") or var.endswith("_result")) \
                   and df_var_re.match(var):
                    assigned.setdefault(var, i)

        # Find variables with multiple action calls
        candidates: List[Tuple[str, int]] = []
        for var, assign_line in assigned.items():
            action_lines = [
                i for i, l in enumerate(lines)
                if var in l and action_re.search(l) and i > assign_line
            ]
            if len(action_lines) >= 2:
                candidates.append((var, min(action_lines)))

        if not candidates:
            return code

        out    = list(lines)
        offset = 0
        for var, first_use in sorted(candidates, key=lambda x: x[1]):
            context_window = "\n".join(lines[max(0, first_use - 5): first_use + 2])
            if f"{var}.cache()" in context_window or f"{var}.persist(" in context_window:
                continue
            insert_at = first_use + offset
            indent    = re.match(r'^(\s*)', out[insert_at]).group(1)
            out.insert(
                insert_at,
                f"{indent}{var}.cache()  # [optimizer] cached: used by multiple actions",
            )
            offset += 1
            log.append({
                "line":        first_use + 1,
                "fix":         "cache_multi_action",
                "description": f"'{var}' referenced by ≥2 actions – .cache() added to avoid recomputation",
            })
        return "\n".join(out)

    def _apply_analysis_refactoring(
        self, code: str, analysis: Dict, log: List[Dict]
    ) -> str:
        """Apply explicit code_refactoring entries from the analysis result JSON."""
        refactors: List[Dict] = []
        for key in ("code_analysis", "analysis", "recommendations"):
            block = analysis.get(key, {})
            if isinstance(block, dict):
                refactors.extend(block.get("code_refactoring", []))
            elif isinstance(block, list):
                for item in block:
                    if isinstance(item, dict):
                        refactors.extend(item.get("code_refactoring", []))

        for rf in refactors:
            orig     = (rf.get("original_code") or "").strip()
            improved = (rf.get("improved_code") or "").strip()
            benefit  = rf.get("benefit", "")
            if orig and improved and orig != improved and orig in code:
                code = code.replace(orig, improved, 1)
                log.append({
                    "fix":         "analysis_refactor",
                    "description": f"Applied refactoring from analysis: {benefit}",
                    "original":    orig[:200],
                    "fixed":       improved[:200],
                })
        return code
