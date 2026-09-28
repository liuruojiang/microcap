"""Generate a review-only unified diff; never write strategy sources."""

from __future__ import annotations

import difflib
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
FILES = [
    "analyze_top100_rebalance_frequency.py",
    "microcap_top100_mom16_biweekly_live.py",
    "microcap_top100_mom16_biweekly_live_v2_0.py",
]

OLD_ST = '''def is_st_name(name: str | None) -> bool:
    text = str(name or "").strip().upper().replace(" ", "")
    return text.startswith(("*ST", "ST", "PT"))
'''
NEW_ST = '''def is_st_name(name: str | None) -> bool:
    text = str(name or "").upper()
    return re.match(r"^\\s*(?:[SG]\\s*)?\\*?\\s*(?:ST|PT)(?![A-Z])", text) is not None
'''

OLD_LIVE = '''def is_live_tradable_name(name: str) -> bool:
    """Current/live member rule; deliberately separate from historical ST masks."""
    text = str(name or "").strip().upper().replace(" ", "")
    return is_tradable_name(name) and not text.startswith(("*ST", "ST", "PT"))
'''
NEW_LIVE = '''def is_live_tradable_name(name: str) -> bool:
    """Current/live member rule; deliberately separate from historical ST masks."""
    text = str(name or "").upper()
    return is_tradable_name(name) and re.match(r"^\\s*(?:[SG]\\s*)?\\*?\\s*(?:ST|PT)(?![A-Z])", text) is None
'''

OLD_RATIO = '''    if is_st:
        return 0.05
'''
NEW_RATIO = '''    if is_st:
        mainland_main_board = code.startswith(("000", "001", "002", "003", "600", "601", "603", "605"))
        if mainland_main_board and pd.Timestamp(trade_date).normalize() >= pd.Timestamp("2026-07-06"):
            return 0.1
        return 0.05
'''

OLD_LIMIT = '''    limit_price = round_limit_price(float(prev_close) * (1.0 + direction * float(limit_ratio)))
'''
NEW_LIMIT = '''    exact_limit = Decimal(str(prev_close)) * (Decimal("1") + Decimal(direction) * Decimal(str(limit_ratio)))
    limit_price = round_limit_price(exact_limit)
'''


def replace_once(text: str, before: str, after: str, path: str) -> str:
    pattern = re.compile(re.escape(before).replace("\\" + "\n", r"\r?\n"))
    matches = list(pattern.finditer(text))
    if len(matches) != 1:
        raise AssertionError(f"{path}: expected one exact match, got {len(matches)}: {before[:45]!r}")
    match = matches[0]
    old_lines = match.group(0).splitlines(keepends=True)
    new_lines = after.splitlines(keepends=True)
    dominant = "\r\n" if sum(line.endswith("\r\n") for line in old_lines) > len(old_lines) / 2 else "\n"
    replacement = "".join(line.removesuffix("\n").removesuffix("\r") + dominant for line in new_lines)
    return text[:match.start()] + replacement + text[match.end():]


def transformed(path: str) -> tuple[str, str]:
    original = (ROOT / path).read_bytes().decode("utf-8")
    result = original
    if path in {FILES[0], FILES[2]}:
        result = replace_once(result, OLD_ST, NEW_ST, path)
        result = replace_once(result, OLD_RATIO, NEW_RATIO, path)
        result = replace_once(result, OLD_LIMIT, NEW_LIMIT, path)
    if path in {FILES[1], FILES[2]}:
        result = replace_once(result, OLD_LIVE, NEW_LIVE, path)
    return original, result


def main() -> None:
    parts = []
    for path in FILES:
        original, candidate = transformed(path)
        parts.extend(difflib.unified_diff(
            original.splitlines(keepends=True), candidate.splitlines(keepends=True),
            fromfile=f"a/{path}", tofile=f"b/{path}", n=3,
        ))
    patch = "".join(parts)
    assert patch and patch.endswith(("\n", "\r\n"))
    (OUT / "candidate_code_fix.patch").write_bytes(patch.encode("utf-8"))
    print(f"wrote {OUT / 'candidate_code_fix.patch'} ({len(patch.splitlines())} lines)")


if __name__ == "__main__":
    main()
