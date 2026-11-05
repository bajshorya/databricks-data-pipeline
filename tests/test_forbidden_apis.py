import re
from pathlib import Path

ROOT = Path(__file__).parent.parent

# Serverless (Spark Connect, ANSI on) rules from Part B2 of the plan.
FORBIDDEN = {
    r"\.rdd\b": "df.rdd",
    r"sparkContext|\bsc\.": "sparkContext",
    r"input_file_name": "input_file_name()",
    r"\bcheckpoint\(": "checkpoint()",
    r"CACHE TABLE|cacheTable|REFRESH TABLE": "table caching",
    r"/dbfs/": "/dbfs paths",
    r"createGlobalTempView|createOrReplaceGlobalTempView": "global temp views",
    r"@udf|\budf\(|pandas_udf": "Python UDFs",
    r"^\s*from \.|^\s*import \.": "relative imports",
}


def scan(text: str) -> list[str]:
    return [label for pattern, label in FORBIDDEN.items()
            if re.search(pattern, text, re.MULTILINE)]


def test_scanner_catches_planted_examples():
    assert "df.rdd" in scan("df.rdd.map(f)")
    assert "Python UDFs" in scan("@udf('int')\ndef f(x): ...")
    assert "relative imports" in scan("from .cleaning import x")
    assert scan("df.select('a')") == []


def test_no_forbidden_apis_in_src_or_jobs():
    bad = {}
    for folder in ("src", "jobs"):
        for f in (ROOT / folder).rglob("*.py"):
            if hits := scan(f.read_text()):
                bad[str(f.relative_to(ROOT))] = hits
    assert not bad, bad
