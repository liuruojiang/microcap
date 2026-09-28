"""Independent Tencent raw-close spot check against isolated Sina histories."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from akshare.stock_feature.stock_hist_tx import stock_zh_a_hist_tx


HERE = Path(__file__).resolve().parent
SAMPLES = ["000005", "000023", "002447", "300023", "600005", "600068", "600093"]


def main() -> None:
    summary = []
    differences = []
    for symbol in SAMPLES:
        record = {"symbol": symbol, "independent_source": "Tencent stock_zh_a_hist_tx adjust=''"}
        try:
            sina = pd.read_csv(HERE / "prices_raw" / f"{symbol}.csv", parse_dates=["date"])
            sina = sina.loc[sina["date"].ge(pd.Timestamp("2010-01-05"))].copy()
            prefix = "sh" if symbol.startswith("6") else "sz"
            tx = stock_zh_a_hist_tx(symbol=prefix + symbol, start_date="2010-01-05", end_date="2026-09-24", adjust="", timeout=25)
            tx = tx[["date", "close"]].copy()
            tx["date"] = pd.to_datetime(tx["date"])
            tx["close"] = pd.to_numeric(tx["close"], errors="coerce")
            merged = sina.merge(tx, on="date", how="inner")
            merged["abs_error"] = (merged["close_raw"] - merged["close"]).abs()
            record.update(
                sina_rows=len(sina), tencent_rows=len(tx), overlap_rows=len(merged),
                overlap_first=merged["date"].min().date().isoformat() if len(merged) else "",
                overlap_last=merged["date"].max().date().isoformat() if len(merged) else "",
                max_abs_close_error=float(merged["abs_error"].max()) if len(merged) else None,
                nonzero_error_rows=int(merged["abs_error"].gt(1e-6).sum()) if len(merged) else None,
                sina_only_dates=len(pd.DatetimeIndex(sina["date"]).difference(pd.DatetimeIndex(tx["date"]))),
                tencent_only_dates=len(pd.DatetimeIndex(tx["date"]).difference(pd.DatetimeIndex(sina["date"]))),
            )
            if len(merged):
                differences.append(merged.loc[merged["abs_error"].gt(1e-6)].assign(symbol=symbol))
        except Exception as exc:
            record["error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
        summary.append(record)
        print(json.dumps(record, ensure_ascii=False), flush=True)
    pd.DataFrame(summary).to_csv(HERE / "price_crosscheck_summary.csv", index=False, encoding="utf-8")
    if differences:
        pd.concat(differences, ignore_index=True).to_csv(HERE / "price_crosscheck_differences.csv", index=False, encoding="utf-8")


if __name__ == "__main__":
    main()
