"""Frozen old-proxy parity and a fail-closed next-open clock diagnostic.

This does not certify fills, historical members, or economic portfolio returns.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import baostock as bs
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
CUTOFF = pd.Timestamp("2026-09-24")
EVENTS = [pd.Timestamp(x) for x in ("2010-01-14", "2010-01-28", "2010-02-11")]
PATHS = {
    "proxy": ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv",
    "replay": ROOT / "research_reports/20260926_l1_repair/full_replay/all_daily_returns.csv",
    "effective": ROOT / "research_reports/20260926_l1_repair/full_replay/members_by_rebalance.csv",
    "seed": ROOT / "research_reports/20260926_l1_repair/corporate_actions/initial_2010_01_04_executed_seed.csv",
    "turnover": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv",
}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def close_path(symbol: str) -> Path:
    path = ROOT / ".microcap_index_cache/prices_raw" / f"{symbol}.csv"
    if not path.exists():
        raise FileNotFoundError(path)
    return path


def query_open_bars(symbols: list[str], first: str, last: str) -> pd.DataFrame:
    cache = OUT / "baostock_raw_open_windows.csv"
    manifest = OUT / "baostock_raw_open_windows_manifest.json"
    if cache.exists() and manifest.exists():
        m = json.loads(manifest.read_text(encoding="utf-8"))
        if m["symbols"] == symbols and m["first"] == first and m["last"] == last and m["sha256"] == sha(cache):
            return pd.read_csv(cache, dtype={"symbol": str})
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"BaoStock login: {login.error_code} {login.error_msg}")
    fields = "date,code,open,close,preclose,volume,tradestatus,isST"
    rows: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []
    try:
        for i, symbol in enumerate(symbols, 1):
            code = ("sh." if symbol.startswith(("6", "9")) else "sz.") + symbol
            result = bs.query_history_k_data_plus(code, fields, start_date=first, end_date=last, frequency="d", adjustflag="3")
            if result.error_code != "0":
                errors.append({"symbol": symbol, "error": result.error_msg})
                continue
            while result.next():
                row = dict(zip(fields.split(","), result.get_row_data()))
                row["symbol"] = symbol
                rows.append(row)
            if i % 25 == 0:
                print(f"BaoStock {i}/{len(symbols)}", flush=True)
    finally:
        bs.logout()
    if errors:
        (OUT / "query_errors.json").write_text(json.dumps(errors, ensure_ascii=False, indent=2), encoding="utf-8")
        raise RuntimeError(f"BaoStock query failed for {len(errors)} symbols")
    frame = pd.DataFrame(rows)
    frame.to_csv(cache, index=False, encoding="utf-8")
    manifest.write_text(json.dumps({"source": "BaoStock anonymous query_history_k_data_plus", "fields": fields,
        "adjustflag": "3", "frequency": "d", "first": first, "last": last, "symbols": symbols,
        "rows": len(frame), "sha256": sha(cache)}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return frame


def event_members(effective: pd.DataFrame, seed: list[str], dt: pd.Timestamp) -> tuple[list[str], list[str]]:
    dates = sorted(effective.rebalance_date.unique())
    previous = [x for x in dates if x < dt]
    before = (effective.loc[effective.rebalance_date.eq(previous[-1]), "symbol"].tolist()
              if previous else seed)
    after = effective.loc[effective.rebalance_date.eq(dt), "symbol"].tolist()
    assert len(before) == len(after) == 100
    assert len(set(before)) == len(set(after)) == 100
    return before, after


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    inputs = {k: {"path": str(p), "sha256": sha(p)} for k, p in PATHS.items()}
    proxy = pd.read_csv(PATHS["proxy"])
    replay = pd.read_csv(PATHS["replay"])
    proxy["date"] = pd.to_datetime(proxy.date)
    replay["date"] = pd.to_datetime(replay.date)
    p = proxy.loc[proxy.date.le(CUTOFF), ["date", "daily_return"]].merge(
        replay.loc[replay.date.le(CUTOFF), ["date", "calculated_return"]],
        on="date", how="outer", validate="one_to_one", indicator=True)
    assert p._merge.eq("both").all() and len(p) == 4064
    baseline_max_error = float(np.max(np.abs(p.daily_return - p.calculated_return)))
    assert baseline_max_error < 1e-10

    effective = pd.read_csv(PATHS["effective"], dtype={"symbol": str})
    effective["rebalance_date"] = pd.to_datetime(effective.rebalance_date)
    seed = pd.read_csv(PATHS["seed"], dtype={"symbol": str}).sort_values("rank").symbol.tolist()
    turnover = pd.read_csv(PATHS["turnover"])
    turnover["rebalance_date"] = pd.to_datetime(turnover.rebalance_date)
    turnover = turnover.loc[turnover.rebalance_date.le(CUTOFF)]
    assert len(turnover) == 432
    all_events = []
    all_symbols: set[str] = set()
    for dt in EVENTS:
        before, after = event_members(effective, seed, dt)
        following = proxy.loc[proxy.date.gt(dt), "date"].min()
        assert pd.notna(following)
        all_events.append((dt, following, before, after))
        all_symbols.update(before)
        all_symbols.update(after)
    symbols = sorted(all_symbols)
    bars = query_open_bars(symbols, str(EVENTS[0].date()), str(max(e[1] for e in all_events).date()))
    bars["date"] = pd.to_datetime(bars.date)
    for col in ("open", "close", "preclose", "volume"):
        bars[col] = pd.to_numeric(bars[col], errors="coerce")
    assert not bars.duplicated(["date", "symbol"]).any()
    bar_key = bars.set_index(["date", "symbol"])

    rows = []
    used_raw: set[str] = set()
    for signal, execution, before, after in all_events:
        item: dict[str, object] = {"signal_close": str(signal.date()), "candidate_open": str(execution.date()),
                                   "old_members": len(before), "target_members": len(after),
                                   "status": "blocked", "reason": ""}
        assert execution > signal
        official = turnover.loc[turnover.rebalance_date.eq(signal)].iloc[0]
        assert official.execution_timing == "close" and official.execution_date == str(signal.date())
        pairs = [(symbol, dt) for symbol in sorted(set(before) | set(after)) for dt in (signal, execution)]
        absent = [(symbol, str(dt.date())) for symbol, dt in pairs if (dt, symbol) not in bar_key.index]
        if absent:
            item["reason"] = f"missing raw OHLC: {absent[:8]} total={len(absent)}"
            rows.append(item)
            continue
        close_mismatch = []
        for symbol, dt in pairs:
            local_path = close_path(symbol)
            used_raw.add(symbol)
            local = pd.read_csv(local_path)
            selected = local.loc[local.date.eq(str(dt.date())), "close_raw"]
            if len(selected) != 1 or abs(float(selected.iloc[0]) - float(bar_key.loc[(dt, symbol), "close"])) > 1e-8:
                close_mismatch.append((symbol, str(dt.date())))
        if close_mismatch:
            item["reason"] = f"local/BaoStock close mismatch: {close_mismatch[:8]} total={len(close_mismatch)}"
            rows.append(item)
            continue
        old_overnight = np.array([bar_key.loc[(execution, s), "open"] / bar_key.loc[(signal, s), "close"] - 1 for s in before])
        new_intraday = np.array([bar_key.loc[(execution, s), "close"] / bar_key.loc[(execution, s), "open"] - 1 for s in after])
        wrong_new_overnight = np.array([bar_key.loc[(execution, s), "open"] / bar_key.loc[(signal, s), "close"] - 1 for s in after])
        old_close_to_close = np.array([bar_key.loc[(execution, s), "close"] / bar_key.loc[(signal, s), "close"] - 1 for s in after])
        if not all(np.isfinite(x).all() for x in (old_overnight, new_intraday, wrong_new_overnight, old_close_to_close)):
            item["reason"] = "nonfinite bar prices"
            rows.append(item)
            continue
        stored = float(proxy.loc[proxy.date.eq(execution), "daily_return"].iloc[0])
        old_replayed = float(old_close_to_close.mean())
        assert abs(stored - old_replayed) < 1e-10, (signal, stored, old_replayed)
        time_candidate = (1 + float(old_overnight.mean())) * (1 + float(new_intraday.mean())) - 1
        wrong = (1 + float(wrong_new_overnight.mean())) * (1 + float(new_intraday.mean())) - 1
        item.update({"status": "clock_decomposition_only", "reason": "no exchange auction fills, open queue, or cash/share ledger",
            "entries": int(official.entry_count), "exits": int(official.exit_count),
            "old_proxy_return": stored, "old_close_to_close_replay": old_replayed,
            "old_members_close_to_next_open": float(old_overnight.mean()),
            "new_members_open_to_close": float(new_intraday.mean()),
            "next_open_clock_decomposition": time_candidate,
            "wrong_new_members_overnight_decomposition": wrong,
            "wrong_new_members_overnight_error": wrong - time_candidate,
            "fault_same_day_execution_rejected": bool(not (signal > signal)),
            "fault_wrong_overnight_members_detected": bool(abs(wrong - time_candidate) > 1e-10),
        })
        rows.append(item)
    out = pd.DataFrame(rows)
    out.to_csv(OUT / "event_clock_diagnostics.csv", index=False, encoding="utf-8")
    result = {"cutoff": str(CUTOFF.date()), "input_files": inputs,
        "frozen_baseline": {"rows": len(p), "max_abs_daily_return_error": baseline_max_error,
                            "first": str(p.date.min().date()), "last": str(p.date.max().date())},
        "raw_close_files_used": len(used_raw),
        "raw_close_sha256": {s: sha(close_path(s)) for s in sorted(used_raw)},
        "event_rows": rows,
        "claim": "isolated price-clock diagnostic; no filled trade, corrected Top100 NAV or certified performance"}
    (OUT / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"baseline": result["frozen_baseline"], "events": rows}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
