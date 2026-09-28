"""Conservative L1 title triage; never promotes title-only dates to formal ST history."""

from __future__ import annotations

import re
from html import unescape


ST_TERMS = ("风险警示", "特别处理", "摘帽")
ENTRY_TERMS = ("实施", "实行", "被实行")
POSSIBILITY_PHRASES = (
    "股票可能被实施", "股票可能实施", "可能会被实施", "可能被实施",
    "存在被实施", "存在实施", "股票存在被实施",
)
PENDING_EXIT = ("申请撤销", "为撤销", "争取撤销", "拟撤销")


def classify_title(title: str) -> str:
    """Return entry, exit, remain_st, context, or needs_review."""
    t = re.sub(r"\s+", "", re.sub(r"<[^>]*>", "", unescape(str(title or ""))))
    if "债券" in t and "股票" not in t:
        return "context"
    if not any(token in t for token in ST_TERMS):
        return "context"
    if "进展" in t or any(token in t for token in PENDING_EXIT):
        return "context"
    if any(token in t for token in POSSIBILITY_PHRASES):
        return "context"
    if "继续" in t and any(token in t for token in ENTRY_TERMS):
        return "remain_st"
    has_exit = "摘帽" in t or "撤销" in t
    has_entry = any(token in t for token in ENTRY_TERMS)
    if has_exit and has_entry:
        # Retiring one warning while imposing another keeps the ST state on.
        if re.search(r"撤销.{0,15}(?:并|及|同时|暨).{0,15}(?:实施|实行)", t):
            return "remain_st"
        return "needs_review"
    if has_exit:
        # A removal of delisting warning may leave other ST treatment in place
        # (600080 in 2008, 600180 in 2011). The body must settle the state.
        return "needs_review"
    if has_entry:
        return "entry"
    return "context"


def build_intervals(first_trade: str, notices: list[dict]) -> list[dict]:
    """Use notice dates as approximate boundaries; preserve initial-state uncertainty."""
    ordered = sorted(notices, key=lambda x: x["notice_time_shanghai"] or "")
    intervals: list[dict] = []
    active_start = None
    active_basis = None
    for row in ordered:
        date = (row["notice_time_shanghai"] or "")[:10]
        if not date:
            continue
        action = classify_title(row["title"])
        if action == "entry":
            if active_start is None:
                active_start, active_basis = date, "explicit_entry"
        elif action == "remain_st":
            if active_start is None:
                active_start, active_basis = first_trade, "inferred_before_notice"
        elif action in {"exit", "needs_review"}:
            # Censor the title-supported interval at this unresolved event.
            # Do not infer ST before an exit with no observed entry.
            if active_start is None:
                continue
            if active_start < date:
                intervals.append({"start_notice_date": active_start, "end_notice_date": date, "basis": active_basis})
            active_start, active_basis = None, None
    if active_start is not None:
        intervals.append({"start_notice_date": active_start, "end_notice_date": None, "basis": active_basis})
    return intervals


def status_on_target(date: str, interval: dict) -> str | None:
    """Boundaries need PDF effective-session proof; interior is title-supported."""
    start, end = interval["start_notice_date"], interval["end_notice_date"]
    if date < start or (end is not None and date > end):
        return None
    if date == start or (end is not None and date == end):
        return "boundary_needs_pdf"
    if interval["basis"] == "explicit_entry":
        return "explicit_entry_interior"
    return "initial_state_inferred"
