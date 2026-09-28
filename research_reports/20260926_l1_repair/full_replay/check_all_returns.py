"""Full-sample raw-close proxy-return parity using independently replayed members."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import analyze_top100_rebalance_frequency as freq


def load_return(symbol: str, dates: pd.DatetimeIndex) -> tuple[str, np.ndarray]:
    path = freq.resolve_cache_path(freq.PRICE_DIR, freq.SHARED_PRICE_DIR, symbol)
    if path is None:
        raise FileNotFoundError(symbol)
    price = pd.read_csv(path, usecols=["date", "close_raw"])
    price["date"] = pd.to_datetime(price["date"], errors="coerce")
    price["close_raw"] = pd.to_numeric(price["close_raw"], errors="coerce")
    price = price.dropna().sort_values("date").drop_duplicates("date", keep="last")
    close = price.set_index("date")["close_raw"].reindex(dates)
    in_coverage = (dates >= price["date"].min()) & (dates <= price["date"].max())
    close = close.where(in_coverage).ffill().where(in_coverage)
    ret = close.pct_change(fill_method=None).to_numpy(dtype=float)
    meta_path = freq.resolve_security_meta_path(symbol)
    if meta_path is not None:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        active = freq.build_active_status_series(meta, dates).to_numpy(dtype=bool)
        ret[~active] = np.nan
    return symbol, ret


def main() -> None:
    proxy = pd.read_csv(ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv")
    proxy["date"] = pd.to_datetime(proxy["date"])
    proxy = proxy.sort_values("date")
    dates = pd.DatetimeIndex([pd.Timestamp("2010-01-04"), *proxy["date"].tolist()])
    replay = pd.read_csv(OUT / "members_by_rebalance.csv", dtype={"symbol": str})
    replay["rebalance_date"] = pd.to_datetime(replay["rebalance_date"])
    grouped = {dt: group["symbol"].tolist() for dt, group in replay.groupby("rebalance_date")}
    rebalance_dates = pd.DatetimeIndex(sorted(grouped))
    seed = pd.read_csv(ROOT / "research_reports/20260926_l1_repair/corporate_actions/initial_2010_01_04_executed_seed.csv", dtype={"symbol": str})["symbol"].tolist()
    symbols = sorted(set(replay["symbol"]) | set(seed))
    arr = np.full((len(dates), len(symbols)), np.nan, dtype=float)
    pos = {s: i for i, s in enumerate(symbols)}
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = [pool.submit(load_return, s, dates) for s in symbols]
        for count, fut in enumerate(as_completed(futures), 1):
            symbol, ret = fut.result()
            arr[:, pos[symbol]] = ret
            if count % 300 == 0:
                print(f"returns loaded {count}/{len(symbols)}", flush=True)

    rows = []
    for i, dt in enumerate(proxy["date"], start=1):
        prior_rebalance = rebalance_dates.searchsorted(dt, side="left") - 1
        held = seed if prior_rebalance < 0 else grouped[rebalance_dates[prior_rebalance]]
        values = arr[i, [pos[s] for s in held]]
        calculated = float(np.nan_to_num(values, nan=0.0).mean())
        official = float(proxy.iloc[i-1]["daily_return"])
        rows.append({"date": dt, "held_count": len(held), "calculated_return": calculated, "official_return": official, "difference": calculated - official, "missing_stock_returns": int(np.isnan(values).sum())})
    result = pd.DataFrame(rows)
    result.to_csv(OUT / "all_daily_returns.csv", index=False, encoding="utf-8-sig")
    mismatch = result.loc[result["difference"].abs().gt(1e-10)]
    summary = {
        "symbols": len(symbols), "days": len(result), "within_1e_10": int(len(result)-len(mismatch)),
        "max_abs_difference": float(result["difference"].abs().max()),
        "first_mismatch": None if mismatch.empty else mismatch.iloc[0].to_dict(),
    }
    (OUT / "all_returns_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str), flush=True)


if __name__ == "__main__":
    main()
