"""The tree must be extractable into a public repo: no project ids, service
accounts, chat ids, tokens, or references to the host repo's private trees."""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FORBIDDEN = [
    r"project-[0-9a-f]{8}-", r"iam\.gserviceaccount\.com", r"AIstack/", r"cloudrun/production", r"telegram", r"TELEGRAM",
    r"api_key=", r"AKIA[0-9A-Z]{16}", r"-----BEGIN",
]
SKIP_DIRS = {"__pycache__", ".pytest_cache", "site"}
SKIP_FILES = {"fredgraph_tail.csv", "ieso_demand_tail.csv", "test_no_private_leakage.py"}


def test_tree_is_clean():
    hits = []
    for p in ROOT.rglob("*"):
        if not p.is_file() or SKIP_DIRS & set(p.relative_to(ROOT).parts) or p.name in SKIP_FILES or p.suffix in (".db", ".pyc"):
            continue
        text = p.read_text(encoding="utf-8", errors="ignore")
        for pat in FORBIDDEN:
            for m in re.finditer(pat, text):
                hits.append(f"{p.relative_to(ROOT)}: {pat!r} -> {text[max(0, m.start()-20):m.end()+20]!r}")
    assert hits == [], "\n".join(hits)
