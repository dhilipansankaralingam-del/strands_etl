"""
Column Lineage Agent
=====================
Tracks column-level data flow through a PySpark script using regex + lightweight
AST analysis. Produces JSON adjacency list, Mermaid diagram, and Graphviz DOT.

Based on ColumnLineageAgent from PR-11 (table-optimizer-analysis).
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple

from strands import Agent
from strands.models import BedrockModel
from strands.tools import tool

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a **Data Lineage Architect** who traces column-level data flows through PySpark ETL scripts.

For each script analyse:
1. Source tables and the columns they contribute
2. Transformations: withColumn, withColumnRenamed, select/alias, groupBy/agg, join keys
3. Sink tables / write destinations
4. Build a directed graph: source_column → operation → target_column

Return structured JSON:
{
  "edges": [{"from": "...", "to": "...", "op": "..."}],
  "sources": ["table1", "table2"],
  "sinks": ["target_table"],
  "mermaid": "flowchart LR\\n  ...",
  "dot": "digraph lineage { ... }",
  "column_count": N,
  "transformation_count": N
}
Return ONLY valid JSON.
"""


class _LineageGraph:
    def __init__(self):
        self.edges:   List[Dict[str, str]] = []
        self.sources: List[str]            = []
        self.sinks:   List[str]            = []
        self._seen:   Set[Tuple]           = set()

    def add_edge(self, src: str, dst: str, op: str = "derive"):
        key = (src, dst, op)
        if key not in self._seen:
            self._seen.add(key)
            self.edges.append({"from": src, "to": dst, "op": op})

    def add_source(self, t: str):
        if t and t not in self.sources:
            self.sources.append(t)

    def add_sink(self, t: str):
        if t and t not in self.sinks:
            self.sinks.append(t)


def _col_refs(expr_str: str) -> List[str]:
    refs = re.findall(r'col\s*\(\s*["\']([^"\']+)["\']\s*\)', expr_str)
    refs += re.findall(r'F\.col\s*\(\s*["\']([^"\']+)["\']\s*\)', expr_str)
    refs += re.findall(r'["\']([^"\']+)["\']', expr_str)
    return list(dict.fromkeys(refs))  # dedup preserving order


def _build_graph(code: str) -> _LineageGraph:
    g = _LineageGraph()

    # Sources: spark.table / spark.read
    for m in re.finditer(r'spark\.table\s*\(\s*["\']([^"\']+)["\']\s*\)', code):
        g.add_source(m.group(1))
    for m in re.finditer(r'spark\.read\.[^\(]+\(\s*["\']([^"\']+)["\']\s*\)', code):
        g.add_source(m.group(1))
    for m in re.finditer(r'from_catalog\s*\([^)]+\)', code, re.DOTALL):
        blk = m.group()
        db  = re.search(r'database\s*=\s*["\']([^"\']+)["\']', blk)
        tb  = re.search(r'table_name\s*=\s*["\']([^"\']+)["\']', blk)
        if tb:
            name = f"{db.group(1)}.{tb.group(1)}" if db else tb.group(1)
            g.add_source(name)

    # Sinks
    for m in re.finditer(r'\.saveAsTable\s*\(\s*["\']([^"\']+)["\']\s*\)', code):
        g.add_sink(m.group(1))
    for m in re.finditer(r'\.format\s*\(\s*["\'](?:delta|iceberg|parquet|orc)["\']\s*\).*?\.save\s*\(\s*["\']([^"\']+)["\']\s*\)', code):
        g.add_sink(m.group(1))

    # withColumn
    for m in re.finditer(r'\.withColumn\s*\(\s*["\']([^"\']+)["\']\s*,\s*(.+?)\)', code):
        new_col, expr = m.group(1), m.group(2)
        for ref in _col_refs(expr):
            if ref != new_col:
                g.add_edge(ref, new_col, "withColumn")

    # withColumnRenamed
    for m in re.finditer(r'\.withColumnRenamed\s*\(\s*["\']([^"\']+)["\']\s*,\s*["\']([^"\']+)["\']\s*\)', code):
        g.add_edge(m.group(1), m.group(2), "rename")

    # select with alias
    for m in re.finditer(r'\.select\s*\(([^)]{1,500})\)', code, re.DOTALL):
        block = m.group(1)
        for am in re.finditer(r'(?:["\']([^"\']+)["\']|col\(["\']([^"\']+)["\']\))\s*\.alias\s*\(\s*["\']([^"\']+)["\']\s*\)', block):
            src = am.group(1) or am.group(2)
            dst = am.group(3)
            if src:
                g.add_edge(src, dst, "select_alias")

    # groupBy + agg
    for m in re.finditer(r'\.groupBy\s*\(([^)]+)\)\s*\.agg\s*\(([^)]{1,500})\)', code, re.DOTALL):
        for gc in re.findall(r'["\']([^"\']+)["\']', m.group(1)):
            g.add_edge(gc, gc, "groupBy_key")
        for am in re.finditer(r'(?:sum|count|avg|max|min)\s*\(\s*["\']([^"\']+)["\']\s*\)(?:\.alias\s*\(\s*["\']([^"\']+)["\']\s*\))?', m.group(2)):
            src = am.group(1)
            dst = am.group(2) or f"agg_{src}"
            g.add_edge(src, dst, "aggregation")

    # join keys
    for m in re.finditer(r'\.join\s*\(\s*\w+\s*,\s*(?:on\s*=\s*)?["\']([^"\']+)["\']', code):
        g.add_edge(m.group(1), m.group(1), "join_key")

    return g


def _render_mermaid(g: _LineageGraph) -> str:
    lines = ["flowchart LR"]
    for src in g.sources:
        safe = src.replace(".", "_").replace("-", "_")
        lines.append(f'  {safe}[("{src}")]')
    for sink in g.sinks:
        safe = sink.replace(".", "_").replace("-", "_")
        lines.append(f'  {safe}[("{sink}")]')
    for e in g.edges[:80]:  # cap to keep diagram readable
        s = e["from"].replace(".", "_").replace("-", "_").replace(" ", "_")
        d = e["to"].replace(".", "_").replace("-", "_").replace(" ", "_")
        lines.append(f'  {s} -->|{e["op"]}| {d}')
    return "\n".join(lines)


def _render_dot(g: _LineageGraph) -> str:
    lines = ["digraph lineage {", '  rankdir=LR;', '  node [shape=box];']
    for src in g.sources:
        lines.append(f'  "{src}" [style=filled fillcolor=lightblue];')
    for sink in g.sinks:
        lines.append(f'  "{sink}" [style=filled fillcolor=lightgreen];')
    for e in g.edges[:80]:
        lines.append(f'  "{e["from"]}" -> "{e["to"]}" [label="{e["op"]}"];')
    lines.append("}")
    return "\n".join(lines)


# ── Tools ──────────────────────────────────────────────────────────────────────

@tool
def trace_column_lineage(script_content: str) -> str:
    """
    Trace column-level lineage through a PySpark script.

    Args:
        script_content: Full PySpark script source code.

    Returns:
        JSON with edges (from/to/op), sources, sinks, mermaid diagram, DOT graph.
    """
    try:
        g = _build_graph(script_content)
        return json.dumps({
            "edges":               g.edges,
            "sources":             g.sources,
            "sinks":               g.sinks,
            "mermaid":             _render_mermaid(g),
            "dot":                 _render_dot(g),
            "column_count":        len({e["from"] for e in g.edges} | {e["to"] for e in g.edges}),
            "transformation_count": len(g.edges),
            "success":             True,
        })
    except Exception as exc:
        logger.error("Lineage tracing failed: %s", exc)
        return json.dumps({"success": False, "error": str(exc), "edges": [], "mermaid": ""})


@tool
def generate_lineage_diagram(script_content: str, format: str = "mermaid") -> str:
    """
    Generate a lineage diagram in Mermaid or Graphviz DOT format.

    Args:
        script_content: Full PySpark script source code.
        format:         "mermaid" or "dot" (default: mermaid).

    Returns:
        JSON with the diagram string and a brief summary.
    """
    try:
        g = _build_graph(script_content)
        diagram = _render_mermaid(g) if format == "mermaid" else _render_dot(g)
        return json.dumps({
            "format":  format,
            "diagram": diagram,
            "sources": g.sources,
            "sinks":   g.sinks,
            "edge_count": len(g.edges),
        })
    except Exception as exc:
        return json.dumps({"error": str(exc), "diagram": ""})


@tool
def find_pii_lineage(script_content: str, pii_columns_json: str) -> str:
    """
    Trace the flow of known PII columns through the script to detect
    whether they are masked/dropped before reaching sink tables.

    Args:
        script_content:   Full PySpark script source code.
        pii_columns_json: JSON list of PII column names (from ComplianceAgent).

    Returns:
        JSON with pii_reaches_sink (bool), unmasked_flows, and warnings.
    """
    try:
        pii_cols = set(json.loads(pii_columns_json))
        g        = _build_graph(script_content)

        # Build forward reachability from each PII col
        children: Dict[str, List[str]] = {}
        for e in g.edges:
            children.setdefault(e["from"], []).append(e["to"])

        def reachable(start: str) -> Set[str]:
            visited, queue = set(), [start]
            while queue:
                node = queue.pop()
                if node in visited:
                    continue
                visited.add(node)
                queue.extend(children.get(node, []))
            return visited

        warnings: List[str] = []
        unmasked: List[Dict] = []
        pii_at_sink = False

        for col in pii_cols:
            reached = reachable(col)
            sink_exposure = [s for s in g.sinks if s in reached or col in reached]
            if sink_exposure:
                pii_at_sink = True
                # Check if masking op appears on path
                masked = any(e["op"] in ("withColumn",) and
                              e["from"] == col and
                              ("sha" in e["to"].lower() or "mask" in e["to"].lower() or "hash" in e["to"].lower())
                              for e in g.edges)
                if not masked:
                    unmasked.append({"pii_column": col, "reached_sinks": sink_exposure})
                    warnings.append(f"PII column '{col}' flows to sink without detected masking")

        return json.dumps({
            "pii_reaches_sink": pii_at_sink,
            "unmasked_flows":   unmasked,
            "warnings":         warnings,
            "checked_columns":  list(pii_cols),
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


def create_column_lineage_agent(model_id: str = "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                                 region: str = "us-west-2") -> Agent:
    model = BedrockModel(model_id=model_id, region_name=region)
    return Agent(model=model, system_prompt=SYSTEM_PROMPT,
                 tools=[trace_column_lineage, generate_lineage_diagram, find_pii_lineage])
