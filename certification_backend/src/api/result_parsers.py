from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class StageScore:
    """Container for a single stage's outcome."""
    name: str
    passed: Optional[int] = None
    failed: Optional[int] = None
    skipped: Optional[int] = None
    errors: Optional[int] = None
    score: Optional[float] = None
    detail: Optional[str] = None


# PUBLIC_INTERFACE
def parse_pytest_junit(junit_path: str | Path) -> StageScore:
    """Parse a pytest-generated JUnit XML file to extract pass/fail/skip/error counts and a score.

    Score logic: percent passed = passed / (passed + failed + errors) * 100, rounded to 2 decimals.
    Skipped is excluded from denominator.
    """
    p = Path(junit_path)
    score = StageScore(name="pytest")
    if not p.exists():
        score.detail = "junit.xml not found"
        return score
    try:
        tree = ET.parse(str(p))
        root = tree.getroot()
        # JUnit root attributes vary (testsuite or testsuites). Aggregate all.
        total_tests = 0
        total_failures = 0
        total_errors = 0
        total_skipped = 0

        suites = []
        if root.tag.lower().endswith("testsuite"):
            suites = [root]
        else:
            suites = list(root.findall(".//testsuite"))

        for s in suites:
            total_tests += int(s.attrib.get("tests", "0"))
            total_failures += int(s.attrib.get("failures", "0"))
            total_errors += int(s.attrib.get("errors", "0"))
            # handle both skipped and skips
            skipped_attr = s.attrib.get("skipped", s.attrib.get("skips", "0"))
            total_skipped += int(skipped_attr or "0")

        passed = max(total_tests - total_failures - total_errors - total_skipped, 0)
        denom = max(passed + total_failures + total_errors, 0)
        pct = round((passed / denom) * 100.0, 2) if denom > 0 else None

        score.passed = passed
        score.failed = total_failures
        score.errors = total_errors
        score.skipped = total_skipped
        score.score = pct
        return score
    except Exception as exc:
        score.detail = f"parse error: {exc}"
        return score


# PUBLIC_INTERFACE
def parse_pylint_log(log_path: str | Path) -> StageScore:
    """Parse pylint output log to derive a score.

    Heuristic:
    - Extract the global "rated at X.Y/10" line if present and convert to 0..100 scale.
    - Fallback to counting 'error' and 'fatal' occurrences for fail count.
    """
    p = Path(log_path)
    score = StageScore(name="pylint")
    if not p.exists():
        score.detail = "pylint.log not found"
        return score
    try:
        text = p.read_text(errors="ignore")
        m = re.search(r"rated at\s+([0-9]+(?:\.[0-9]+)?)/10", text, flags=re.IGNORECASE)
        if m:
            value = float(m.group(1))
            score.score = round(value * 10.0, 2)  # 0..100
            # Derive pseudo pass/fail: consider >= 80 as passed count 1 else failed 1
            score.passed = 1 if score.score >= 80 else 0
            score.failed = 0 if score.score >= 80 else 1
        else:
            # Count issues
            errors = len(re.findall(r"\b(E|F):", text))
            score.failed = errors
            score.passed = 1 if errors == 0 else 0
            score.score = 100.0 if errors == 0 else 0.0
        return score
    except Exception as exc:
        score.detail = f"parse error: {exc}"
        return score


# PUBLIC_INTERFACE
def parse_bandit_log(log_path: str | Path) -> StageScore:
    """Parse bandit log to extract issue counts and compute a score.

    Heuristic:
    - Look for a JSON summary block (Bandit supports -f json). Since we run text, fallback to summary lines.
    - If 'No issues identified' -> score 100.
    - Else count 'Issue:' lines as failed, set score 0 if any.
    """
    p = Path(log_path)
    score = StageScore(name="bandit")
    if not p.exists():
        score.detail = "bandit.log not found"
        return score
    try:
        txt = p.read_text(errors="ignore")
        # Try to extract a JSON blob if present
        json_blob = None
        try:
            # naive attempt: content might be pure JSON
            json_blob = json.loads(txt)
        except Exception:
            json_blob = None

        if isinstance(json_blob, dict) and "results" in json_blob:
            issues = len(json_blob.get("results") or [])
            score.failed = issues
            score.passed = 1 if issues == 0 else 0
            score.score = 100.0 if issues == 0 else 0.0
            return score

        # Fallback text parsing
        if re.search(r"No issues identified", txt, flags=re.IGNORECASE):
            score.failed = 0
            score.passed = 1
            score.score = 100.0
            return score

        # Count 'Issue:' occurrences
        issues = len(re.findall(r"\bIssue:\b", txt))
        score.failed = issues
        score.passed = 1 if issues == 0 else 0
        score.score = 100.0 if issues == 0 else 0.0
        return score
    except Exception as exc:
        score.detail = f"parse error: {exc}"
        return score


# PUBLIC_INTERFACE
def parse_airflow_stage_log(log_path: str | Path, stage: str) -> StageScore:
    """Parse the generated airflow log JSON to extract states and compute a simple score.

    For MVP:
    - If final 'complete.state' equals 'success' -> passed=1, failed=0, score=100
    - Else -> passed=0, failed=1, score=0
    """
    p = Path(log_path)
    s = StageScore(name=stage)
    if not p.exists():
        s.detail = "airflow stage log not found"
        return s
    try:
        data = json.loads(p.read_text() or "{}")
        final = (data.get("complete") or {})
        state = str(final.get("state") or final.get("status") or "").lower()
        if state == "success":
            s.passed, s.failed, s.score = 1, 0, 100.0
        else:
            s.passed, s.failed, s.score = 0, 1, 0.0
        return s
    except Exception as exc:
        s.detail = f"parse error: {exc}"
        return s
