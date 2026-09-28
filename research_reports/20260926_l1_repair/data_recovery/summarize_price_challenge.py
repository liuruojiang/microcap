"""Audit 251-vendor comparison and locate differences in potential member windows."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


HERE = Path(__file__).resolve().parent
OUT = HERE / "price_challenge"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    pieces = [pd.read_csv(OUT / f"summary_{stage}.csv", dtype={"symbol": str}) for stage in ("priority", "remaining")]
    summary = pd.concat(pieces, ignore_index=True).sort_values("symbol")
    assert len(summary) == 251 and summary["symbol"].is_unique
    assert summary["status"].eq("checked").all(), summary.loc[summary["status"].ne("checked"), ["symbol", "status", "error"]].to_dict("records")
    source_manifest = pd.read_csv(HERE / "source_manifest_both.csv", dtype={"symbol": str}).set_index("symbol")
    for row in summary.itertuples(index=False):
        s = row.symbol
        assert sha256(HERE / "prices_raw" / f"{s}.csv") == row.sina_file_sha256 == source_manifest.loc[s, "prices_raw_sha256"]
        assert sha256(OUT / "tencent_raw" / f"{s}.csv") == row.tencent_file_sha256
    # Reconcile offline against the security master's listing window. The raw
    # Sina cache includes seven Sunday observations after delisting, which must
    # be kept as source anomalies rather than counted as tradable date gaps.
    inventory = pd.read_csv(HERE / "missing_delisted_inventory.csv", dtype={"symbol": str}, parse_dates=["list_date", "delist_date"]).set_index("symbol")
    rebalance_dates = set(pd.read_csv(HERE / "candidate_cap_comparisons.csv", usecols=["rebalance_date"])["rebalance_date"].astype(str))
    difference_parts = []
    outside_parts = []
    for ix, row in summary.iterrows():
        symbol = row["symbol"]
        first = max(pd.Timestamp("2010-01-04"), inventory.loc[symbol, "list_date"])
        last = min(pd.Timestamp("2026-09-24"), inventory.loc[symbol, "delist_date"])
        sina = pd.read_csv(HERE / "prices_raw" / f"{symbol}.csv", parse_dates=["date"])
        tx = pd.read_csv(OUT / "tencent_raw" / f"{symbol}.csv", parse_dates=["date"])
        outside = sina.loc[~sina["date"].between(first, last)].copy()
        if not outside.empty:
            outside["symbol"] = symbol
            outside["reason"] = outside["date"].apply(lambda day: "before_listing" if day < first else "after_delisting")
            outside_parts.append(outside)
        sina = sina.loc[sina["date"].between(first, last)]
        tx = tx.loc[tx["date"].between(first, last)]
        merged = sina.merge(tx, on="date", how="outer", indicator=True)
        merged["abs_error"] = (merged["close_raw"] - merged["close_tencent"]).abs()
        merged["symbol"] = symbol
        merged["is_rebalance_date"] = merged["date"].dt.strftime("%Y-%m-%d").isin(rebalance_dates)
        changed = merged.loc[merged["_merge"].ne("both") | merged["abs_error"].gt(1e-6)]
        if not changed.empty:
            difference_parts.append(changed)
        nonzero = merged.loc[merged["abs_error"].gt(1e-6)]
        over_cent = merged.loc[merged["abs_error"].gt(0.01000001)]
        updates = {
            "sina_rows_unfiltered": len(sina) + len(outside),
            "sina_outside_listing_dates": len(outside),
            "sina_rows": len(sina),
            "tencent_rows": len(tx),
            "overlap_rows": int(merged["_merge"].eq("both").sum()),
            "sina_only_dates": int(merged["_merge"].eq("left_only").sum()),
            "tencent_only_dates": int(merged["_merge"].eq("right_only").sum()),
            "nonzero_price_difference_dates": len(nonzero),
            "over_0p01_price_difference_dates": len(over_cent),
            "max_abs_price_difference": float(nonzero["abs_error"].max()) if len(nonzero) else 0.0,
            "first_price_difference_date": nonzero["date"].min().date().isoformat() if len(nonzero) else "",
            "first_over_0p01_difference_date": over_cent["date"].min().date().isoformat() if len(over_cent) else "",
            "price_difference_on_rebalance_dates": int(nonzero["is_rebalance_date"].sum()),
        }
        for key, value in updates.items():
            summary.at[ix, key] = value
    difference = pd.concat(difference_parts, ignore_index=True).sort_values(["symbol", "date"]) if difference_parts else pd.DataFrame()
    outside = pd.concat(outside_parts, ignore_index=True).sort_values(["symbol", "date"]) if outside_parts else pd.DataFrame(columns=["symbol", "date", "close_raw", "reason"])
    outside.to_csv(OUT / "sina_outside_listing_dates.csv", index=False, encoding="utf-8")
    initial = pd.read_csv(HERE / "initial_2010_01_04_candidate_caps.csv", dtype={"symbol": str})
    initial_possible = dict(zip(initial["symbol"], initial["possible_below_rank100"].fillna(False)))
    initial_rows = initial.set_index("symbol")
    target = pd.read_csv(HERE / "candidate_cap_comparisons.csv", dtype={"symbol": str}, parse_dates=["rebalance_date"])
    grouped = {symbol: frame.sort_values("rebalance_date") for symbol, frame in target.groupby("symbol")}
    enriched = []
    for event in difference.itertuples(index=False):
        symbol = event.symbol
        day = pd.Timestamp(event.date)
        rows = grouped.get(symbol)
        previous = rows.loc[rows["rebalance_date"].le(day)] if rows is not None else pd.DataFrame()
        if previous.empty:
            signal_day = pd.Timestamp("2010-01-04")
            under_cap = bool(initial_possible.get(symbol, False))
            if symbol in initial_rows.index:
                initial_row = initial_rows.loc[symbol]
                price_at_signal = initial_row["close_raw"]
                shares_at_signal = initial_row["total_shares_10k"]
                threshold = initial_row["initial_formal_rank100_cap"]
            else:
                price_at_signal = None
                shares_at_signal = None
                threshold = None
        else:
            last = previous.iloc[-1]
            signal_day = last["rebalance_date"]
            under_cap = bool(last["possible_below_rank100"])
            price_at_signal = last["close_raw"]
            shares_at_signal = last["total_shares_10k"]
            threshold = last["formal_rank100_cap"]
        event_record = dict(zip(difference.columns, event))
        event_record["last_signal_date"] = signal_day
        event_record["under_old_rank100_at_last_signal"] = under_cap
        tencent_cap_at_signal = event.close_tencent * shares_at_signal * 10000 if day == signal_day and pd.notna(event.close_tencent) and shares_at_signal is not None and pd.notna(shares_at_signal) else None
        tencent_under_cap = bool(tencent_cap_at_signal < threshold) if tencent_cap_at_signal is not None and threshold is not None and pd.notna(threshold) else False
        event_record["possible_rank_effect_on_signal_date"] = bool(day == signal_day and (under_cap or tencent_under_cap))
        event_record["rank100_classification_flip_due_to_vendor_price"] = bool(day == signal_day and tencent_cap_at_signal is not None and under_cap != tencent_under_cap)
        event_record["possible_holding_period_price_effect"] = bool(day > signal_day and under_cap)
        event_record["last_signal_candidate_cap"] = (price_at_signal * shares_at_signal * 10000) if price_at_signal is not None and pd.notna(price_at_signal) and pd.notna(shares_at_signal) else None
        event_record["last_signal_old_rank100_cap"] = threshold
        enriched.append(event_record)
    diff = pd.DataFrame(enriched)
    diff.to_csv(OUT / "differences_with_possible_member_window.csv", index=False, encoding="utf-8")
    diff.loc[diff["abs_error"].gt(0.01000001)].to_csv(OUT / "price_differences_over_0p01.csv", index=False, encoding="utf-8")
    diff.loc[diff["possible_holding_period_price_effect"].eq(True)].to_csv(OUT / "possible_holding_date_differences.csv", index=False, encoding="utf-8")
    if not diff.empty:
        price_differences = diff.loc[diff["abs_error"].gt(1e-6)].copy()
        if not price_differences.empty:
            max_by_symbol = price_differences.sort_values(["symbol", "abs_error", "date"], ascending=[True, False, True]).drop_duplicates("symbol")
            max_date = dict(zip(max_by_symbol["symbol"], max_by_symbol["date"].dt.strftime("%Y-%m-%d")))
            summary["max_difference_date"] = summary["symbol"].map(max_date).fillna("")
    summary.to_csv(OUT / "summary_all_251.csv", index=False, encoding="utf-8")
    request_parts = [pd.read_csv(OUT / f"request_manifest_{stage}.csv", dtype={"symbol": str}) for stage in ("priority", "remaining")]
    requests = pd.concat(request_parts, ignore_index=True)
    requests.to_csv(OUT / "request_manifest_all_251.csv", index=False, encoding="utf-8")
    all_price_differences = diff.loc[diff["abs_error"].gt(1e-6)] if not diff.empty else pd.DataFrame()
    result = {
        "symbols": len(summary),
        "sina_rows": int(summary["sina_rows"].sum()),
        "sina_rows_unfiltered": int(summary["sina_rows_unfiltered"].sum()),
        "sina_outside_listing_dates": int(summary["sina_outside_listing_dates"].sum()),
        "tencent_rows": int(summary["tencent_rows"].sum()),
        "overlap_rows": int(summary["overlap_rows"].sum()),
        "symbols_with_sina_only_dates": int(summary["sina_only_dates"].gt(0).sum()),
        "sina_only_dates": int(summary["sina_only_dates"].sum()),
        "symbols_with_tencent_only_dates": int(summary["tencent_only_dates"].gt(0).sum()),
        "tencent_only_dates": int(summary["tencent_only_dates"].sum()),
        "nonzero_price_difference_dates": int(summary["nonzero_price_difference_dates"].sum()),
        "over_0p01_price_difference_dates": int(summary["over_0p01_price_difference_dates"].sum()),
        "symbols_with_price_difference": int(summary["nonzero_price_difference_dates"].gt(0).sum()),
        "max_abs_price_difference": float(summary["max_abs_price_difference"].max()),
        "first_difference_date": all_price_differences["date"].min().date().isoformat() if not all_price_differences.empty else "",
        "price_difference_on_rebalance_dates": int(summary["price_difference_on_rebalance_dates"].sum()),
        "difference_dates_in_possible_holding_period": int(diff["possible_holding_period_price_effect"].sum()) if len(diff) else 0,
        "difference_dates_on_possible_admission_signal": int(diff["possible_rank_effect_on_signal_date"].sum()) if len(diff) else 0,
        "rank100_classification_flips_due_to_vendor_price": int(diff["rank100_classification_flip_due_to_vendor_price"].sum()) if len(diff) else 0,
        "request_rows": len(requests),
        "tencent_files_hash_checked": len(summary),
        "sina_files_hash_checked": len(summary),
    }
    (OUT / "summary_all_251.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
