"""
Job Generator Agent
=====================
Generates production-ready PySpark ETL jobs from a structured JSON spec
and an optional reference script (for style / pattern guidance).

Job Spec Format (JSON)
----------------------
{
  "job_name":        "customer_monthly_revenue",
  "description":     "Join orders with products, compute monthly revenue by category",
  "platform":        "glue",           // glue | emr | spark | databricks
  "processing_mode": "delta",          // full | delta
  "source_tables": [
    {
      "database": "sales_db",
      "table":    "orders",
      "alias":    "orders",
      "filters":  ["order_date >= '2024-01-01'", "status = 'completed'"],
      "columns":  ["order_id", "customer_id", "product_id", "amount", "order_date"],
      "broadcast": false
    },
    {
      "database": "product_db",
      "table":    "products",
      "alias":    "products",
      "broadcast": true,
      "columns":  ["product_id", "category", "name"]
    }
  ],
  "joins": [
    {"left": "orders", "right": "products", "on": "product_id", "type": "left"}
  ],
  "transformations": [
    "derive month as date_trunc('month', order_date)",
    "rename amount to revenue"
  ],
  "aggregations": {
    "group_by": ["month", "category"],
    "metrics":  [
      "sum(revenue) as total_revenue",
      "count(order_id) as order_count",
      "avg(revenue) as avg_order_value"
    ]
  },
  "target_table": {
    "database":    "reports_db",
    "table":       "monthly_revenue_by_category",
    "partition_by": "month",
    "write_mode":  "overwrite",
    "format":      "parquet"
  },
  "spark_configs": {}   // optional overrides on top of defaults
}

Rule-based mode:  generates a complete PySpark/Glue template from the spec fields.
LLM mode:         sends spec + reference script to Claude for a production-grade job.
"""
from __future__ import annotations

import json
import textwrap
from datetime import date
from typing import Any, Dict, List, Optional

from .base import CostOptimizerAgent, AnalysisInput, AnalysisResult


# Spark configs injected into every generated job
_DEFAULT_SPARK_CONFIGS: Dict[str, str] = {
    "spark.sql.adaptive.enabled":                      "true",
    "spark.sql.adaptive.coalescePartitions.enabled":   "true",
    "spark.sql.adaptive.skewJoin.enabled":             "true",
    "spark.sql.autoBroadcastJoinThreshold":            "10485760",
    "spark.sql.shuffle.partitions":                    "auto",
    "spark.sql.files.maxPartitionBytes":               "134217728",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
}

_GLUE_EXTRA_IMPORTS = """\
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext"""

_PLATFORM_NOTES = {
    "glue":       "AWS Glue 4.0+",
    "emr":        "AWS EMR on EC2 / EKS",
    "spark":      "Standalone Spark / Kubernetes",
    "databricks": "Databricks Runtime",
}


class JobGeneratorAgent(CostOptimizerAgent):
    """Generate a new PySpark ETL job from a structured spec."""

    AGENT_NAME = "job_generator"

    # ─── Public entry point ────────────────────────────────────────────────────

    def generate(
        self,
        job_spec: Dict[str, Any],
        reference_script: Optional[str] = None,
        output_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate a PySpark job from *job_spec*.

        Args:
            job_spec:         Structured job description (see module docstring).
            reference_script: Optional existing script to model style from.
            output_path:      If given, write the generated script here.

        Returns:
            generated_script  – str (Python source)
            job_name          – str
            platform          – str
            output_path       – str | None
            success           – bool
            errors            – list[str]
        """
        # Pack everything into AnalysisInput so the base class routing works
        input_data = AnalysisInput(
            script_path     = job_spec.get("job_name", "generated_job") + ".py",
            script_content  = reference_script or "",
            source_tables   = job_spec.get("source_tables", []),
            processing_mode = job_spec.get("processing_mode", "full"),
            current_config  = {},
            job_name        = job_spec.get("job_name", "generated_job"),
        )
        result = self.analyze(input_data, context={"job_spec": job_spec})

        script = result.analysis.get("generated_script", "")

        if output_path and script:
            try:
                from pathlib import Path
                p = Path(output_path)
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(script)
            except Exception as exc:
                result.errors.append(f"Could not write to {output_path}: {exc}")

        return {
            "success":          result.success,
            "generated_script": script,
            "job_name":         job_spec.get("job_name", "generated_job"),
            "platform":         job_spec.get("platform", "glue"),
            "output_path":      output_path,
            "errors":           result.errors,
        }

    # ─── Rule-based implementation ─────────────────────────────────────────────

    def _analyze_rule_based(
        self, input_data: AnalysisInput, context: Dict
    ) -> AnalysisResult:
        job_spec  = context.get("job_spec", {})
        reference = input_data.script_content  # optional style reference

        script = self._render_template(job_spec, reference)

        return AnalysisResult(
            agent_name = self.AGENT_NAME,
            success    = True,
            analysis   = {
                "generated_script": script,
                "job_spec":         job_spec,
                "template_based":   True,
            },
        )

    # ─── LLM prompt ───────────────────────────────────────────────────────────

    def _build_llm_prompt(self, input_data: AnalysisInput, context: Dict) -> str:
        job_spec  = context.get("job_spec", {})
        reference = input_data.script_content
        platform  = job_spec.get("platform", "glue")

        ref_section = ""
        if reference.strip():
            ref_section = f"""
## Reference Script (follow this coding style and conventions)
```python
{reference}
```
"""
        return f"""
You are a Senior PySpark Data Engineer. Generate a complete, production-ready PySpark ETL job
based on the specification below.  The job must:

1. Run on **{_PLATFORM_NOTES.get(platform, platform)}**
2. Include ALL recommended Spark configs (AQE, KryoSerializer, shuffle tuning)
3. Apply broadcast() hints for tables marked broadcast=true or with < 500k rows
4. Use .cache() strategically for DataFrames used in multiple places
5. Include proper error handling and logging
6. Follow PySpark best practices: predicate pushdown, column pruning, partition pruning
7. Use `date_trunc`, `col`, `lit`, `when` from pyspark.sql.functions — never Python UDFs
8. Add .coalesce(N) before write to control output file count
9. Include inline comments explaining non-obvious logic

## Job Specification
```json
{json.dumps(job_spec, indent=2)}
```
{ref_section}
## Required Output Format
Return ONLY a JSON object (no prose outside JSON):
{{
  "generated_script": "<complete Python source code as a single string>",
  "design_notes": ["<note 1>", "<note 2>"]
}}

The generated_script must be a complete, runnable .py file.
"""

    def _parse_llm_response(self, response) -> AnalysisResult:
        import re, json as _json
        text = str(response)
        m = re.search(r'\{[\s\S]*\}', text)
        if m:
            try:
                data = _json.loads(m.group())
                script = data.get("generated_script", "")
                if script:
                    return AnalysisResult(
                        agent_name = self.AGENT_NAME,
                        success    = True,
                        analysis   = {
                            "generated_script": script,
                            "design_notes":     data.get("design_notes", []),
                        },
                    )
            except Exception:
                pass
        # Fallback: treat whole response as raw script
        return AnalysisResult(
            agent_name = self.AGENT_NAME,
            success    = True,
            analysis   = {"generated_script": text},
        )

    # ─── Template renderer ─────────────────────────────────────────────────────

    def _render_template(self, spec: Dict[str, Any], reference: str) -> str:
        platform  = spec.get("platform", "glue").lower()
        job_name  = spec.get("job_name", "generated_job")
        desc      = spec.get("description", "Generated PySpark ETL job")
        proc_mode = spec.get("processing_mode", "full")
        tables    = spec.get("source_tables", [])
        joins     = spec.get("joins", [])
        xforms    = spec.get("transformations", [])
        agg_spec  = spec.get("aggregations", {})
        target    = spec.get("target_table", {})
        extra_cfg = spec.get("spark_configs", {})

        today     = date.today().isoformat()

        # ── Header ────────────────────────────────────────────────────────────
        lines: List[str] = [
            '"""',
            f'{job_name}.py',
            f'{"=" * (len(job_name) + 3)}',
            f'{desc}',
            f'',
            f'Platform       : {_PLATFORM_NOTES.get(platform, platform)}',
            f'Processing mode: {proc_mode}',
            f'Generated by   : cost_optimizer.JobGeneratorAgent  ({today})',
            f'"""',
            "",
        ]

        # ── Imports ───────────────────────────────────────────────────────────
        lines += [
            "import sys",
            "import logging",
            "from datetime import datetime, date",
            "from pyspark.sql import SparkSession",
            "from pyspark.sql import functions as F",
            "from pyspark.sql.functions import (",
            "    col, lit, when, coalesce, broadcast,",
            "    date_trunc, to_date, year, month, dayofmonth,",
            "    sum as spark_sum, count, avg, max as spark_max, min as spark_min,",
            ")",
            "from pyspark.sql.window import Window",
        ]

        if platform == "glue":
            lines += ["", _GLUE_EXTRA_IMPORTS]

        lines += ["", "log = logging.getLogger(__name__)", ""]

        # ── SparkSession ──────────────────────────────────────────────────────
        if platform == "glue":
            lines += self._glue_session_block(job_name, extra_cfg)
        else:
            lines += self._plain_session_block(job_name, extra_cfg)

        # ── Read source tables ────────────────────────────────────────────────
        lines += ["", "", "# ── Read source tables " + "─" * 50]
        for tbl in tables:
            lines += self._read_table_block(tbl, platform, proc_mode)

        # ── Joins ─────────────────────────────────────────────────────────────
        if joins:
            lines += ["", "# ── Joins " + "─" * 62]
            lines += self._join_block(joins, tables)

        # ── Transformations ───────────────────────────────────────────────────
        if xforms:
            lines += ["", "# ── Transformations " + "─" * 52]
            lines += self._transform_block(xforms, joins, tables)

        # ── Aggregations ──────────────────────────────────────────────────────
        if agg_spec:
            lines += ["", "# ── Aggregations " + "─" * 55]
            lines += self._aggregation_block(agg_spec, joins, tables)

        # ── Write output ──────────────────────────────────────────────────────
        if target:
            lines += ["", "# ── Write output " + "─" * 55]
            lines += self._write_block(target, platform)

        # ── Glue job commit ───────────────────────────────────────────────────
        if platform == "glue":
            lines += ["", "    job.commit()"]

        # ── Entry point ───────────────────────────────────────────────────────
        lines += [
            "",
            "",
            'if __name__ == "__main__":',
            "    logging.basicConfig(",
            '        level=logging.INFO,',
            '        format="%(asctime)s %(levelname)s %(name)s %(message)s"',
            "    )",
            "    main()",
        ]

        return "\n".join(lines)

    # ─── Sub-renderers ─────────────────────────────────────────────────────────

    def _spark_config_lines(self, indent: str, extra: Dict[str, str]) -> List[str]:
        merged = {**_DEFAULT_SPARK_CONFIGS, **extra}
        out    = [f"", f"{indent}# ── Recommended Spark configs ──────────────────────────"]
        for k, v in merged.items():
            out.append(f'{indent}spark.conf.set("{k}", "{v}")')
        return out

    def _glue_session_block(self, job_name: str, extra_cfg: Dict) -> List[str]:
        lines = [
            "def main():",
            '    args = getResolvedOptions(sys.argv, ["JOB_NAME"])',
            "    sc           = SparkContext()",
            "    glueContext  = GlueContext(sc)",
            "    spark        = glueContext.spark_session",
            "    job          = Job(glueContext)",
            '    job.init(args["JOB_NAME"], args)',
            "",
        ]
        lines += self._spark_config_lines("    ", extra_cfg)
        return lines

    def _plain_session_block(self, job_name: str, extra_cfg: Dict) -> List[str]:
        lines = [
            "def main():",
            "    spark = (",
            "        SparkSession.builder",
            f'        .appName("{job_name}")',
            "        .enableHiveSupport()",
            "        .getOrCreate()",
            "    )",
        ]
        lines += self._spark_config_lines("    ", extra_cfg)
        return lines

    def _read_table_block(
        self, tbl: Dict, platform: str, proc_mode: str
    ) -> List[str]:
        db      = tbl.get("database", "")
        table   = tbl.get("table", "unknown")
        alias   = tbl.get("alias", table)
        filters = tbl.get("filters", [])
        cols    = tbl.get("columns", [])
        is_bcast= tbl.get("broadcast", False)
        fq      = f"{db}.{table}" if db else table

        lines = [""]

        if platform == "glue":
            lines += [
                f"    {alias}_dyf = glueContext.create_dynamic_frame.from_catalog(",
                f'        database="{db}", table_name="{table}"',
                "    )",
                f"    {alias}_df = {alias}_dyf.toDF()",
            ]
        else:
            lines += [
                f'    {alias}_df = spark.table("{fq}")',
            ]

        # Column pruning
        if cols:
            col_list = ", ".join(f'"{c}"' for c in cols)
            lines.append(f"    {alias}_df = {alias}_df.select({col_list})")

        # Predicate pushdown / filters
        if filters:
            for f_expr in filters:
                lines.append(f'    {alias}_df = {alias}_df.filter("{f_expr}")')
        elif proc_mode == "delta":
            lines.append(
                f"    # TODO: add delta filter, e.g.: "
                f'{alias}_df = {alias}_df.filter("updated_at >= \'{{run_date}}\'")'
            )

        # Broadcast hint
        if is_bcast:
            lines.append(f"    {alias}_df = broadcast({alias}_df)")

        lines.append(
            f"    log.info("
            f'"{alias}_df loaded: %d rows", {alias}_df.count())'
        )
        return lines

    def _join_block(self, joins: List[Dict], tables: List[Dict]) -> List[str]:
        if not joins:
            return []

        first = joins[0]
        left  = first.get("left", "left")
        right = first.get("right", "right")
        on    = first.get("on", "id")
        jtype = first.get("type", "left")

        lines = [
            f"",
            f"    result_df = {left}_df.join(",
            f"        {right}_df,",
            f"        on=\"{on}\",",
            f'        how="{jtype}",',
            f"    )",
        ]

        for jn in joins[1:]:
            l2    = jn.get("left", "result")
            r2    = jn.get("right", "right")
            on2   = jn.get("on", "id")
            jtype2= jn.get("type", "left")
            lines += [
                f"    result_df = result_df.join(",
                f"        {r2}_df,",
                f"        on=\"{on2}\",",
                f'        how="{jtype2}",',
                f"    )",
            ]

        lines.append(
            '    log.info("After joins: %d rows", result_df.count())'
        )
        return lines

    def _transform_block(
        self, xforms: List[str], joins: List[Dict], tables: List[Dict]
    ) -> List[str]:
        base_var = "result_df" if joins else (
            (tables[0].get("alias", tables[0].get("table", "source")) + "_df")
            if tables else "df"
        )
        lines = [f""]
        for xf in xforms:
            xf_lower = xf.lower()
            if "derive" in xf_lower or "date_trunc" in xf_lower:
                # e.g. "derive month as date_trunc('month', order_date)"
                m_alias = xf_lower.split(" as ")
                if len(m_alias) == 2:
                    new_col  = m_alias[0].replace("derive", "").strip()
                    expr_raw = m_alias[1].strip()
                    lines.append(
                        f'    {base_var} = {base_var}.withColumn("{new_col}", F.expr("{expr_raw}"))'
                    )
                else:
                    lines.append(f"    # TODO transform: {xf}")
            elif "rename" in xf_lower:
                parts = xf_lower.replace("rename", "").split(" to ")
                if len(parts) == 2:
                    old_c, new_c = parts[0].strip(), parts[1].strip()
                    lines.append(
                        f'    {base_var} = {base_var}.withColumnRenamed("{old_c}", "{new_c}")'
                    )
                else:
                    lines.append(f"    # TODO transform: {xf}")
            elif "filter" in xf_lower:
                expr = xf_lower.replace("filter", "").strip()
                lines.append(f'    {base_var} = {base_var}.filter(F.expr("{expr}"))')
            elif "drop" in xf_lower:
                col_name = xf_lower.replace("drop", "").replace("column", "").strip()
                lines.append(f'    {base_var} = {base_var}.drop("{col_name}")')
            else:
                lines.append(f"    # TODO transform: {xf}")
        return lines

    def _aggregation_block(
        self, agg_spec: Dict, joins: List[Dict], tables: List[Dict]
    ) -> List[str]:
        base_var  = "result_df" if joins else (
            (tables[0].get("alias", tables[0].get("table", "source")) + "_df")
            if tables else "df"
        )
        group_by  = agg_spec.get("group_by", [])
        metrics   = agg_spec.get("metrics", [])

        if not group_by and not metrics:
            return []

        gb_cols   = ", ".join(f'"{c}"' for c in group_by)
        agg_exprs = []
        for metric in metrics:
            agg_exprs.append(f'        F.expr("{metric}")')

        lines = [
            "",
            f"    {base_var} = (",
            f"        {base_var}",
            f"        .groupBy({gb_cols})",
            f"        .agg(",
        ]
        lines += [expr + "," for expr in agg_exprs]
        lines += [
            "        )",
            "    )",
            f'    log.info("After aggregation: %d rows", {base_var}.count())',
        ]
        return lines

    def _write_block(self, target: Dict, platform: str) -> List[str]:
        db         = target.get("database", "")
        table      = target.get("table", "output")
        part_by    = target.get("partition_by", "")
        mode       = target.get("write_mode", "overwrite")
        fmt        = target.get("format", "parquet")
        location   = target.get("location", "")
        fq         = f"{db}.{table}" if db else table

        base_var   = "result_df"

        lines = [
            "",
            f"    # Cache before write (if result is also logged/counted above)",
            f"    {base_var}.cache()",
            f'    log.info("Writing %d rows to {fq}", {base_var}.count())',
            "",
        ]

        writer = f"    {base_var}.coalesce(20)  # tune N to target ~128 MB output files"
        lines.append(writer)

        if platform == "glue" and not location:
            lines += [
                f"    {base_var}_dyf = DynamicFrame.fromDF({base_var}, glueContext, \"{table}\")",
                f"    glueContext.write_dynamic_frame.from_catalog(",
                f'        frame="{base_var}_dyf",',
                f'        database="{db}",',
                f'        table_name="{table}",',
                f'        additional_options={{"enableUpdateCatalog": True}}',
                f"    )",
            ]
        elif location:
            lines += [
                f"    (",
                f"        {base_var}",
                f"        .write",
                f'        .mode("{mode}")',
                f'        .format("{fmt}")',
            ]
            if part_by:
                lines.append(f'        .partitionBy("{part_by}")')
            lines += [
                f'        .save("{location}")',
                "    )",
            ]
        else:
            lines += [
                "    (",
                f"        {base_var}",
                f"        .write",
                f'        .mode("{mode}")',
                f'        .format("{fmt}")',
            ]
            if part_by:
                lines.append(f'        .partitionBy("{part_by}")')
            lines += [
                f'        .saveAsTable("{fq}")',
                "    )",
            ]

        lines.append(f'    log.info("Write to {fq} complete")')
        return lines
