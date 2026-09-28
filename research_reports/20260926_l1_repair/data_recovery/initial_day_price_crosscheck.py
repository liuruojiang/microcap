"""Independent Tencent check for the newly added 2010-01-04 close."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from akshare.stock_feature.stock_hist_tx import stock_zh_a_hist_tx


HERE = Path(__file__).resolve().parent
DAY = pd.Timestamp("2010-01-04")
SAMPLES = ["000005", "000023", "002447", "300023", "600005", "600068", "600093"]


def main() -> None:
    records = []
    for symbol in SAMPLES:
        record = {"symbol": symbol, "date": "2010-01-04"}
        try:
            sina = pd.read_csv(HERE / "prices_raw" / f"{symbol}.csv", parse_dates=["date"])
            exact = sina.loc[sina["date"].eq(DAY), "close_raw"]
            record["sina_close"] = float(exact.iloc[0]) if len(exact) == 1 else None
            prefix = "sh" if symbol.startswith("6") else "sz"
            tx = stock_zh_a_hist_tx(symbol=prefix + symbol, start_date="2010-01-04", end_date="2010-01-04", adjust="", timeout=25)
            record["tencent_close"] = float(tx.iloc[0]["close"]) if len(tx) == 1 else None
            record["difference"] = record["sina_close"] - record["tencent_close"] if record["sina_close"] is not None and record["tencent_close"] is not None else None
        except Exception as exc:
            record["error"] = f"{type(exc).__name__}: {str(exc)[:160]}"
        records.append(record)
        print(json.dumps(record, ensure_ascii=False), flush=True)
    pd.DataFrame(records).to_csv(HERE / "initial_day_price_crosscheck.csv", index=False, encoding="utf-8")


if __name__ == "__main__":
    main()
