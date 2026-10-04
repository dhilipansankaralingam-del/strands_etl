"""Plain (non-agent) tools used by the specialist agents."""
import json
import re
from pathlib import Path

from strands import tool

_KB = json.loads((Path(__file__).parent / "knowledge_base.json").read_text())


def _tokens(text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", text.lower()))


@tool
def search_knowledge_base(query: str, top_k: int = 3) -> str:
    """Search the internal knowledge base for documents relevant to a query.

    Args:
        query: Natural-language search query.
        top_k: Maximum number of documents to return (default 3).

    Returns:
        JSON list of matching documents with id, title and text. Cite the ids.
    """
    q = _tokens(query)
    scored = []
    for doc in _KB:
        score = len(q & _tokens(doc["text"])) + 3 * len(q & _tokens(" ".join(doc["tags"]) + " " + doc["title"]))
        if score:
            scored.append((score, doc))
    scored.sort(key=lambda s: s[0], reverse=True)
    hits = [{"id": d["id"], "title": d["title"], "text": d["text"]} for _, d in scored[: max(1, top_k)]]
    return json.dumps(hits or [{"note": "No matching documents; rely on general knowledge and say so."}])


@tool
def word_count(text: str) -> int:
    """Count the words in a piece of text.

    Args:
        text: The text to count.
    """
    return len(text.split())
