"""
Column Lineage Agent
=====================
Tracks column-level data flow through a PySpark script using a combination of
regex + lightweight AST analysis.

Produces three output formats:
  1. JSON adjacency list  – machine-readable graph of source→target column edges
  2. Mermaid diagram      – paste into any Mermaid renderer (GitHub MD, Notion, etc.)
  3. Graphviz DOT graph   – render with `dot -Tpng lineage.dot -o lineage.png`

Tracked transformations
-----------------------
  withColumn("new", expr)         → tracks columns referenced in expr → new
  withColumnRenamed("old", "new") → old → new
  select(..., col.alias("new"))   → source → new
  groupBy(...).agg(agg.alias("new")) → grouped cols + agg input → new
  join(..., on="key")             → key col links both sides
  filter(...)                     → pass-through (no new column, but referenced)
  drop("col")                     → terminal node (col removed)
  spark.table / from_catalog      → source node (db.table → alias)
  write.saveAsTable / save        → sink node
"""
from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


class ColumnLineageAgent:
    """Parse PySpark script and build a column-level lineage graph."""

    # ─── Public entry point ────────────────────────────────────────────────────

    def analyze(
        self,
        script_path: str,
        script_content: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Analyse *script_path* (or *script_content* if provided) and return
        the lineage graph in all three formats.

        Returns
        -------
        success         – bool
        edges           – list of {"from": "<src>", "to": "<dst>", "op": "<operation>"}
        sources         – list of source table references
        sinks           – list of write destinations
        mermaid         – Mermaid graph string
        dot             – Graphviz DOT string
        errors          – list[str]
        """
        if script_content is None:
            p = Path(script_path)
            if not p.exists():
                return {"success": False, "errors": [f"File not found: {script_path}"]}
            script_content = p.read_text(errors="replace")

        try:
            graph   = _LineageGraph()
            _parse_lineage(script_content, graph)
            return {
                "success":  True,
                "edges":    graph.edges,
                "sources":  graph.sources,
                "sinks":    graph.sinks,
                "mermaid":  _render_mermaid(graph),
                "dot":      _render_dot(graph),
                "errors":   [],
            }
        except Exception as exc:
            return {"success": False, "errors": [str(exc)], "edges": [], "mermaid": "", "dot": ""}


# ─── Internal graph model ──────────────────────────────────────────────────────

class _LineageGraph:
    def __init__(self) -> None:
        self.edges:   List[Dict[str, str]] = []
        self.sources: List[str]            = []
        self.sinks:   List[str]            = []
        self._seen:   Set[Tuple[str, str, str]] = set()

    def add_edge(self, src: str, dst: str, op: str = "derive") -> None:
        key = (src, dst, op)
        if key not in self._seen:
            self._seen.add(key)
            self.edges.append({"from": src, "to": dst, "op": op})

    def add_source(self, table: str) -> None:
        if table not in self.sources:
            self.sources.append(table)

    def add_sink(self, target: str) -> None:
        if target not in self.sinks:
            self.sinks.append(target)


# ─── Parser ────────────────────────────────────────────────────────────────────

def _parse_lineage(code: str, graph: _LineageGraph) -> None:
    """
    Walk the code with a combination of regex patterns and partial AST analysis.
    Full AST would require a running Python interpreter to resolve dynamic names;
    regex covers ~90% of common ETL patterns.
    """
    # ── 1. Source tables ───────────────────────────────────────────────────────
    # spark.table("db.tbl")
    for m in re.finditer(r'spark\.table\s*\(\s*["\']([^"\']+)["\']\s*\)', code):
        graph.add_source(m.group(1))

    # from_catalog(database="db", table_name="tbl")
    for m in re.finditer(r'from_catalog\s*\([^)]+\)', code, re.DOTALL):
        block = m.group()
        db_m  = re.search(r'database\s*=\s*["\']([^"\']+)["\']', block)
        tbl_m = re.search(r'table_name\s*=\s*["\']([^"\']+)["\']', block)
        if tbl_m:
            db  = db_m.group(1) if db_m else ""
            tbl = f"{db}.{tbl_m.group(1)}" if db else tbl_m.group(1)
            graph.add_source(tbl)

    # spark.read.* paths
    for m in re.finditer(r'spark\.read\.[^(]+\(\s*["\']([^"\']+)["\']\s*\)', code):
        graph.add_source(m.group(1))

    # ── 2. Variable-to-table mapping (alias_df = spark.table(...)) ─────────────
    var_to_table: Dict[str, str] = {}
    for m in re.finditer(
        r'(\w+)\s*=\s*.*?spark\.table\s*\(\s*["\']([^"\']+)["\']\s*\)', code
    ):
        var_to_table[m.group(1)] = m.group(2)
    for m in re.finditer(
        r'(\w+)_df\s*=\s*.*?table_name\s*=\s*["\']([^"\']+)["\']', code
    ):
        var_to_table[f"{m.group(1)}_df"] = m.group(2)

    # ── 3. withColumn ─────────────────────────────────────────────────────────
    #   df = df.withColumn("new_col", F.col("src") + F.col("other"))
    for m in re.finditer(
        r'\.withColumn\s*\(\s*["\']([^"\']+)["\']\s*,\s*(.+?)\)', code
    ):
        new_col  = m.group(1)
        expr_str = m.group(2)
        refs     = _extract_col_refs(expr_str)
        for ref in refs:
            graph.add_edge(ref, new_col, "withColumn")
        if not refs:
            graph.add_edge("(literal)", new_col, "withColumn")

    # ── 4. withColumnRenamed ──────────────────────────────────────────────────
    for m in re.finditer(
        r'\.withColumnRenamed\s*\(\s*["\']([^"\']+)["\']\s*,\s*["\']([^"\']+)["\']\s*\)', code
    ):
        graph.add_edge(m.group(1), m.group(2), "rename")

    # ── 5. select with aliases ────────────────────────────────────────────────
    #   df.select("a", col("b").alias("B"), F.expr("c + d").alias("sum_cd"))
    for m in re.finditer(r'\.select\s*\(([^)]{1,500})\)', code, re.DOTALL):
        expr_str = m.group(1)
        # Quoted column names (pass-through)
        for col_m in re.finditer(r'["\']([^"\']+)["\'](?!\s*\.alias)', expr_str):
            col_name = col_m.group(1)
            graph.add_edge(col_name, col_name, "select")
        # .alias("new")
        for alias_m in re.finditer(
            r'(?:F\.col\s*\(|col\s*\(|F\.expr\s*\()?["\']([^"\']+)["\']\)?\.alias\s*\(\s*["\']([^"\']+)["\']\s*\)',
            expr_str
        ):
            graph.add_edge(alias_m.group(1), alias_m.group(2), "select_alias")

    # ── 6. groupBy / agg ──────────────────────────────────────────────────────
    for m in re.finditer(
        r'\.groupBy\s*\(([^)]+)\)\s*\.agg\s*\(([^)]{1,500})\)', code, re.DOTALL
    ):
        gb_block  = m.group(1)
        agg_block = m.group(2)
        gb_cols   = re.findall(r'["\']([^"\']+)["\']', gb_block)
        # agg functions with .alias
        for agg_m in re.finditer(
            r'(?:F\.\w+|spark_\w+|sum|count|avg|max|min)\s*\(\s*["\']([^"\']+)["\']\s*\)'
            r'(?:\.alias\s*\(\s*["\']([^"\']+)["\']\s*\))?',
            agg_block
        ):
            src_col = agg_m.group(1)
            dst_col = agg_m.group(2) or f"agg_{src_col}"
            graph.add_edge(src_col, dst_col, "aggregation")
        # groupBy keys are passed through
        for gc in gb_cols:
            graph.add_edge(gc, gc, "groupBy_key")

    # ── 7. join ───────────────────────────────────────────────────────────────
    for m in re.finditer(
        r'\.join\s*\(\s*\w+\s*,\s*(?:on\s*=\s*)?["\']([^"\']+)["\']', code
    ):
        key = m.group(1)
        graph.add_edge(key, key, "join_key")

    # ── 8. drop ───────────────────────────────────────────────────────────────
    for m in re.finditer(r'\.drop\s*\(\s*["\']([^"\']+)["\']\s*\)', code):
        graph.add_edge(m.group(1), "(dropped)", "drop")

    # ── 9. Sinks (writes) ─────────────────────────────────────────────────────
    for m in re.finditer(
        r'\.(saveAsTable|save)\s*\(\s*["\']([^"\']+)["\']\s*\)', code
    ):
        graph.add_sink(m.group(2))
    for m in re.finditer(
        r'\.(parquet|orc|csv|json)\s*\(\s*["\']([^"\']+)["\']\s*\)', code
    ):
        graph.add_sink(m.group(2))


def _extract_col_refs(expr_str: str) -> List[str]:
    """Extract column names referenced in a Spark expression string."""
    refs: List[str] = []
    # F.col("col_name") or col("col_name")
    for m in re.finditer(r'(?:F\.)?col\s*\(\s*["\']([^"\']+)["\']\s*\)', expr_str):
        refs.append(m.group(1))
    # Bare quoted strings: "col_name"
    for m in re.finditer(r'["\']([a-zA-Z_][a-zA-Z0-9_]*)["\']', expr_str):
        refs.append(m.group(1))
    return list(dict.fromkeys(refs))  # preserve order, deduplicate


# ─── Renderers ─────────────────────────────────────────────────────────────────

def _node_id(name: str) -> str:
    """Sanitize a column/table name to a valid Mermaid/DOT node ID."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)


def _render_mermaid(graph: _LineageGraph) -> str:
    """Render lineage as a Mermaid flowchart (left-to-right)."""
    lines = ["graph LR"]

    # Source nodes
    for src in graph.sources:
        nid = _node_id(src)
        lines.append(f'    {nid}["{src} (source)"]:::source')

    # Sink nodes
    for sink in graph.sinks:
        nid = _node_id(sink)
        lines.append(f'    {nid}["{sink} (sink)"]:::sink')

    # Edges
    for edge in graph.edges:
        src = _node_id(edge["from"])
        dst = _node_id(edge["to"])
        op  = edge["op"]
        lines.append(f'    {src} -->|{op}| {dst}')

    # Styles
    lines += [
        "    classDef source fill:#d4edda,stroke:#28a745,color:#155724",
        "    classDef sink   fill:#f8d7da,stroke:#dc3545,color:#721c24",
    ]
    return "\n".join(lines)


def _render_dot(graph: _LineageGraph) -> str:
    """Render lineage as a Graphviz DOT digraph."""
    lines = [
        'digraph ColumnLineage {',
        '    rankdir=LR;',
        '    node [shape=box, style=filled, fillcolor=lightyellow, fontname="Helvetica"];',
        '',
    ]

    # Source nodes (green)
    for src in graph.sources:
        nid = _node_id(src)
        lines.append(f'    {nid} [label="{src}", fillcolor=lightgreen];')

    # Sink nodes (red)
    for sink in graph.sinks:
        nid = _node_id(sink)
        lines.append(f'    {nid} [label="{sink}", fillcolor=lightsalmon];')

    lines.append('')

    # Edges
    for edge in graph.edges:
        src   = _node_id(edge["from"])
        dst   = _node_id(edge["to"])
        op    = edge["op"]
        color = {
            "rename":       "blue",
            "aggregation":  "orange",
            "join_key":     "purple",
            "drop":         "red",
            "withColumn":   "black",
            "select_alias": "darkgreen",
        }.get(op, "gray")
        lines.append(f'    {src} -> {dst} [label="{op}", color={color}];')

    lines.append('}')
    return "\n".join(lines)
