"""Portable code identity for the shared stock input calculation contract."""
from __future__ import annotations

import hashlib
from pathlib import Path


SOURCE_FILES = (
    "scripts/exchange_calendar.py",
    "scripts/stock_input_contract.py",
    "scripts/stock_return_basis.py",
    "scripts/stock_return_basis_store.py",
    "scripts/stock_return_pipeline.py",
    "scripts/tradeability_evidence.py",
    "scripts/st_notice_archive.py",
    "scripts/st_history_scope.py",
    "scripts/st_notice_incremental.py",
    "scripts/st_notice_refresh.py",
    "scripts/st_metadata_refresh.py",
    "scripts/stock_tail_returns.py",
    "scripts/share_change_source.py",
    "scripts/share_unchanged_tail.py",
    "data/share_source_future_plan_reviews.json",
    "scripts/sina_share_history.py",
    "scripts/stock_daily_inputs.py",
    "scripts/stock_daily_alternative.py",
)


def source_fingerprint(root: Path) -> dict[str, str]:
    return {
        name: hashlib.sha256((Path(root) / name).read_bytes().replace(b"\r\n", b"\n")).hexdigest()
        for name in SOURCE_FILES
    }
