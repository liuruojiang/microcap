"""Read-only full-history effective-member replay with true prior traded closes.

The official helper uses a sparse common date calendar for quick reconstruction.
This audit loads only each symbol's rebalance dates and its actual preceding
traded date, preserving the close-limit semantics of the full daily calendar.
"""

from __future__ import annotations

import json
import hashlib
import io
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as runtime

freq = runtime.freq_mod


def tradeability_on_rebalances(symbol: str, dates: pd.DatetimeIndex) -> tuple[str, pd.Series, pd.Series, dict]:
    price_path = freq.resolve_cache_path(freq.PRICE_DIR, freq.SHARED_PRICE_DIR, symbol)
    share_path = freq.resolve_cache_path(freq.SHARE_DIR, freq.SHARED_SHARE_DIR, symbol)
    false = pd.Series(False, index=dates, dtype=bool)
    if price_path is None or share_path is None:
        return symbol, false, false.copy(), {"symbol": symbol, "status": "missing_price_or_shares"}
    price = pd.read_csv(price_path, usecols=["date", "close_raw"])
    price["date"] = pd.to_datetime(price["date"], errors="coerce")
    price["close_raw"] = pd.to_numeric(price["close_raw"], errors="coerce")
    price = price.dropna().sort_values("date").drop_duplicates("date", keep="last")
    if price.empty:
        return symbol, false, false.copy(), {"symbol": symbol, "status": "empty_price"}
    if pd.read_csv(share_path, usecols=["total_shares_10k"])["total_shares_10k"].dropna().empty:
        return symbol, false, false.copy(), {"symbol": symbol, "status": "empty_shares"}

    # Prior closes are from actual prints, including after a long suspension.
    traded = pd.DatetimeIndex(price["date"])
    prior_idx = traded.searchsorted(dates, side="left") - 1
    prior = traded[prior_idx[prior_idx >= 0]]
    selected = pd.DatetimeIndex(sorted(set(dates) | set(prior)))
    # The initial formal price panel starts on 2010-01-04; earlier prints have
    # no bearing on first-window returns and were unavailable to the model.
    panel_start = pd.Timestamp("2010-01-04")
    selected = selected[selected >= panel_start]
    # For price series that start later, the real loader returns None until
    # the first local print. It still receives all rebalance dates here.
    result = freq.load_symbol_cache(symbol, selected, pd.DatetimeIndex([]), "close", False)
    if result is None:
        return symbol, false, false.copy(), {"symbol": symbol, "status": "loader_none"}
    buy = result[3].reindex(dates).fillna(False).astype(bool)
    sell = result[4].reindex(dates).fillna(False).astype(bool)
    return symbol, buy, sell, {"symbol": symbol, "status": "ok", "selected_dates": len(selected)}


def main() -> None:
    target_path = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
    turnover_path = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
    seed_path = ROOT / "research_reports/20260926_l1_repair/corporate_actions/initial_2010_01_04_executed_seed.csv"
    target = pd.read_csv(target_path, dtype={"symbol": str})
    target["symbol"] = target["symbol"].str.zfill(6)
    target["rebalance_date"] = pd.to_datetime(target["rebalance_date"]).dt.normalize()
    target = target.sort_values(["rebalance_date", "rank"])
    target_map = {dt: group["symbol"].tolist() for dt, group in target.groupby("rebalance_date")}
    dates = pd.DatetimeIndex(sorted(target_map))
    current = pd.read_csv(seed_path, dtype={"symbol": str})["symbol"].str.zfill(6).tolist()
    assert len(current) == len(set(current)) == 100
    bridge = pd.Timestamp("2026-09-03")
    archive_path = ROOT / "outputs/repair_20260917_cloud_evidence/cloud_20260916_whole.zip"
    seed_name = "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_effective_members.csv"
    with zipfile.ZipFile(archive_path) as archive:
        seed_bytes = archive.read(seed_name)
    seed_sha = hashlib.sha256(seed_bytes).hexdigest()
    assert seed_sha == "9558082806163587d9813100bd19cafe92337a2c086e3fc8ac6aa8a71e5825ee"
    approved_seed = pd.read_csv(io.BytesIO(seed_bytes), dtype={"symbol": str}).sort_values("rank")["symbol"].tolist()
    assert len(approved_seed) == len(set(approved_seed)) == 100
    symbols = sorted(set(target["symbol"]) | set(current) | set(approved_seed))

    buy_path = OUT / "buyable.parquet"
    sell_path = OUT / "sellable.parquet"
    loader_path = OUT / "symbol_loader.csv"
    if buy_path.exists() and sell_path.exists() and loader_path.exists():
        buy_df = pd.read_parquet(buy_path)
        sell_df = pd.read_parquet(sell_path)
        loader_rows = pd.read_csv(loader_path).to_dict("records")
        assert buy_df.index.equals(dates) and sell_df.index.equals(dates)
        assert set(buy_df.columns) == set(sell_df.columns)
        missing = sorted(set(symbols) - set(buy_df.columns))
        if missing:
            print(f"loading {len(missing)} approved-seed-only symbols", flush=True)
            with ThreadPoolExecutor(max_workers=12) as pool:
                for fut in as_completed([pool.submit(tradeability_on_rebalances, symbol, dates) for symbol in missing]):
                    symbol, buy, sell, info = fut.result()
                    buy_df[symbol] = buy
                    sell_df[symbol] = sell
                    loader_rows.append(info)
            buy_df.to_parquet(buy_path)
            sell_df.to_parquet(sell_path)
            pd.DataFrame(loader_rows).to_csv(loader_path, index=False, encoding="utf-8-sig")
    else:
        buys: dict[str, pd.Series] = {}
        sells: dict[str, pd.Series] = {}
        loader_rows: list[dict] = []
        with ThreadPoolExecutor(max_workers=12) as pool:
            futures = {pool.submit(tradeability_on_rebalances, symbol, dates): symbol for symbol in symbols}
            for completed, fut in enumerate(as_completed(futures), 1):
                symbol, buy, sell, info = fut.result()
                buys[symbol] = buy
                sells[symbol] = sell
                loader_rows.append(info)
                if completed % 200 == 0:
                    print(f"loaded {completed}/{len(symbols)} symbols", flush=True)
        buy_df = pd.DataFrame(buys, index=dates)
        sell_df = pd.DataFrame(sells, index=dates)
        buy_df.to_parquet(buy_path)
        sell_df.to_parquet(sell_path)
        pd.DataFrame(loader_rows).to_csv(loader_path, index=False, encoding="utf-8-sig")

    special = pd.Timestamp("2014-09-18")
    special_symbol = "600862"
    actual_price = pd.read_csv(freq.resolve_cache_path(freq.PRICE_DIR, freq.SHARED_PRICE_DIR, special_symbol))
    actual_price["date"] = pd.to_datetime(actual_price["date"])
    actual_price = actual_price.sort_values("date")
    special_prior = actual_price.loc[actual_price["date"].lt(special)].iloc[-1]
    special_today = actual_price.loc[actual_price["date"].eq(special)].iloc[0]
    ratio = freq.get_price_limit_ratio(special_symbol, special)
    up, down = freq.detect_close_limit_blocks(special_symbol, special, float(special_prior["close_raw"]), float(special_today["close_raw"]))
    special_report = {
        "symbol": special_symbol, "date": str(special.date()),
        "prior_actual_trade_date": str(special_prior["date"].date()),
        "prior_actual_close": float(special_prior["close_raw"]),
        "reopen_close": float(special_today["close_raw"]),
        "price_limit_ratio": ratio, "up_blocked": bool(up), "down_blocked": bool(down),
        "buyable": bool(buy_df.at[special, special_symbol]),
        "target_rank": int(target.loc[(target["rebalance_date"].eq(special)) & (target["symbol"].eq(special_symbol)), "rank"].iloc[0]),
    }

    rows: list[dict] = []
    member_rows: list[dict] = []
    special_report["previously_held"] = None
    special_report["entered"] = None
    special_report["blocked_entry"] = None
    bridge_differences = None
    terminal_details = None
    for dt in dates:
        before = set(current)
        result = freq.apply_trade_constraints(current, target_map[dt], dt, buy_df, sell_df, top_n=100)
        if dt == pd.Timestamp("2026-09-17"):
            terminal_details = {"entered": result["entered"], "exited": result["exited"], "blocked_entries": result["blocked_entries"], "blocked_exits": result["blocked_exits"]}
        if dt == special:
            special_report["previously_held"] = special_symbol in before
            special_report["entered"] = special_symbol in result["entered"]
            special_report["blocked_entry"] = special_symbol in result["blocked_entries"]
        current = result["members_after"]
        assert len(current) == len(set(current)) and len(current) <= 100, dt
        if dt == bridge:
            bridge_differences = {
                "replay_only": sorted(set(current) - set(approved_seed)),
                "approved_seed_only": sorted(set(approved_seed) - set(current)),
            }
            # The approved 2026-09-17 lineage migration explicitly freezes
            # the 2026-09-03 cloud effective-member seed before replaying its
            # recent tail. Preserve that authoritative bridge in this audit.
            current = approved_seed.copy()
        member_rows.extend({"rebalance_date": dt, "symbol": symbol} for symbol in current)
        rows.append({
            "rebalance_date": dt,
            "entry_count": len(result["entered"]),
            "exit_count": len(result["exited"]),
            "blocked_entry_count": len(result["blocked_entries"]),
            "blocked_exit_count": len(result["blocked_exits"]),
            "holding_count_after": len(current),
        })

    replay = pd.DataFrame(rows)
    official = pd.read_csv(turnover_path)
    official["rebalance_date"] = pd.to_datetime(official["rebalance_date"])
    cols = ["entry_count", "exit_count", "blocked_entry_count", "blocked_exit_count", "holding_count_after"]
    comparison = replay.merge(official[["rebalance_date", *cols]], on="rebalance_date", how="outer", suffixes=("_replay", "_official"), validate="one_to_one", indicator=True)
    comparison["parity"] = comparison["_merge"].eq("both")
    for col in cols:
        comparison["parity"] &= comparison[f"{col}_replay"].eq(comparison[f"{col}_official"])
    comparison.to_csv(OUT / "turnover_comparison.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(member_rows).to_csv(OUT / "members_by_rebalance.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(loader_rows).to_csv(OUT / "symbol_loader.csv", index=False, encoding="utf-8-sig")
    mismatch = comparison.loc[~comparison["parity"]]
    summary = {
        "symbols": len(symbols), "rebalance_dates": len(dates),
        "turnover_parity_count": int(comparison["parity"].sum()),
        "first_mismatch": None if mismatch.empty else str(mismatch.iloc[0]["rebalance_date"].date()),
        "special_600862": special_report,
        "approved_2026_09_03_seed_sha256": seed_sha,
        "bridge_2026_09_03_membership_difference": bridge_differences,
        "terminal_2026_09_17": terminal_details,
        "loader_status_counts": pd.DataFrame(loader_rows)["status"].value_counts().to_dict(),
    }
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
