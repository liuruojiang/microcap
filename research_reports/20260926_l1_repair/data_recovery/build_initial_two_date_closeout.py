"""Reconcile the two earliest paper Top100 counterexample sets.

This is research-only. It does not modify strategy inputs, members, or NAV.
"""

from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parent
ST_FILE = ROOT.parent / "st_timing/initial_2010_st/two_date_st_status_evidence.csv"
DATES = ("2010-01-04", "2010-01-14")
SOURCES = {
    "000023": "INITIAL_000023_CASE.md",
    "000502": "INITIAL_SMALL_NOTICE_GROUP.md",
    "000982": "INITIAL_SHARE_EVIDENCE_EXPANSION.md",
    "002013": "INITIAL_SHARE_EVIDENCE_EXPANSION.md",
    "002071": "INITIAL_EARLY_PAIR.md",
    "002113": "INITIAL_MEMBER_RECONSTRUCTION.md",
    "002260": "INITIAL_SHARE_EVIDENCE_EXPANSION.md",
    "600070": "INITIAL_EARLY_PAIR.md",
    "600077": "600077_CASE.md",
    "600213": "INITIAL_FIRST_TARGET_ONLY.md",
    "600306": "INITIAL_FIRST_TARGET_ONLY.md",
    "600355": "INITIAL_SMALL_NOTICE_GROUP.md",
    "600466": "INITIAL_SMALL_NOTICE_GROUP.md",
    "600532": "INITIAL_SHARE_EVIDENCE_EXPANSION.md",
    "600634": "600634_CASE.md",
    "600647": "FULL_REBUILD_FEASIBILITY.md",
    "600687": "600687_CASE.md",
    "600766": "INITIAL_600766_CASE.md",
    "600856": "INITIAL_SMALL_NOTICE_GROUP.md",
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def main() -> None:
    status = read_csv(ST_FILE)
    caps = read_csv(ROOT / "share_pit_challenge_rows.csv")
    shares = {
        row["symbol"]: Decimal(row["shares"])
        for row in read_csv(ROOT / "initial_share_evidence_expansion.csv")
    }
    # The first independently established counterexample has a separate source file.
    shares["002113"] = Decimal("118400000")
    prices = {
        (row["date"], row["symbol"]): Decimal(row["close"])
        for row in read_csv(ROOT / "initial_share_two_date_ohlcv_crosscheck.csv")
        if row["close"] and not row["error"]
    }
    prices.update(
        {
            (row["date"], "002113"): Decimal(row["close_raw"])
            for row in read_csv(ROOT / "prices_raw/002113.csv")
            if row["date"] in DATES
        }
    )
    cap_rows = {(row["date"], row["symbol"]): row for row in caps}
    old_seed = read_csv(ROOT / "initial_formal_seed_caps.csv")
    old_first_target = [
        row
        for row in read_csv(
            ROOT.parent.parent.parent / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
        )
        if row["rebalance_date"] == DATES[1]
    ]
    old_members = {
        DATES[0]: {row["symbol"].zfill(6) for row in old_seed},
        DATES[1]: {row["symbol"].zfill(6) for row in old_first_target},
    }
    old_thresholds = {
        DATES[0]: Decimal(next(row["market_cap"] for row in old_seed if row["rank"] == "100")),
        DATES[1]: Decimal(
            next(row["market_cap"] for row in old_first_target if row["rank"] == "100")
        ),
    }
    assert all(len(group) == 100 for group in old_members.values())
    rows: list[dict[str, str]] = []
    for date in DATES:
        on_date = [row for row in status if row["target_date"] == date]
        assert len(on_date) == (24 if date == DATES[0] else 27)
        eligible = [row for row in on_date if row["status"] == "proven_non_ST"]
        assert len(eligible) == 17
        for row in eligible:
            symbol = row["symbol"]
            key = (date, symbol)
            cap_row = cap_rows[key]
            share_count = shares[symbol]
            close = prices[key]
            market_cap = share_count * close
            threshold = old_thresholds[date]
            margin = threshold - market_cap
            assert margin > 0, key
            assert symbol not in old_members[date], key
            assert (ROOT / SOURCES[symbol]).is_file(), SOURCES[symbol]
            assert abs(market_cap - Decimal(cap_row["candidate_cap"])) < Decimal("0.01")
            assert abs(threshold - Decimal(cap_row["old_rank100_cap"])) < Decimal("0.01")
            rows.append(
                {
                    "date": date,
                    "symbol": symbol,
                    "shares": str(share_count),
                    "raw_close": str(close),
                    "market_cap_rmb": f"{market_cap:.2f}",
                    "old_rank100_cap_rmb": f"{threshold:.2f}",
                    "below_margin_rmb": f"{margin:.2f}",
                    "st_evidence_status": row["status"],
                    "in_old_top100": "False",
                    "source_report": SOURCES[symbol],
                    "source_scope": "public_source_limited_paper_candidate",
                }
            )
    assert len(rows) == 34
    assert len({row["symbol"] for row in rows}) == 19
    target = ROOT / "INITIAL_TWO_DATE_CLOSEOUT.csv"
    with target.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"{target}: {len(rows)} rows, 19 symbols, 17 per date")
    for date in DATES:
        subset = [row for row in rows if row["date"] == date]
        closest = min(subset, key=lambda row: Decimal(row["below_margin_rmb"]))
        print(date, "closest", closest["symbol"], closest["below_margin_rmb"])


if __name__ == "__main__":
    main()
