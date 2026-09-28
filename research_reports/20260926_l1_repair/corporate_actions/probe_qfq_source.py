"""Probe Tencent/AkShare qfq coverage without modifying formal cache."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as runtime

OUT = Path(__file__).resolve().parent / "qfq_probe"
OUT.mkdir(exist_ok=True)


def inspect(symbol: str) -> dict[str, object]:
    raw_path = ROOT / ".microcap_index_cache/prices_raw" / f"{symbol}.csv"
    result: dict[str, object] = {
        "symbol": symbol,
        "source": "akshare.stock_feature.stock_hist_tx.stock_zh_a_hist_tx adjust=qfq (Tencent)",
        "requested_start": "2010-01-01",
        "requested_end": "2026-09-24",
        "retrieved_utc": datetime.now(timezone.utc).isoformat(),
    }
    try:
        qfq = runtime.fetch_mod._fetch_adjusted_price_history_tx(
            symbol, pd.Timestamp(result["requested_start"]), pd.Timestamp(result["requested_end"])
        )
        qfq = qfq.sort_values("date").drop_duplicates("date", keep="last")
        output_path = OUT / f"{symbol}_qfq_candidate.csv"
        qfq.to_csv(output_path, index=False, encoding="utf-8")
        result.update({
            "status": "ok",
            "qfq_rows": int(len(qfq)),
            "qfq_first": str(qfq["date"].min().date()),
            "qfq_last": str(qfq["date"].max().date()),
            "qfq_file": str(output_path.relative_to(ROOT)),
            "qfq_sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(),
        })
        if raw_path.exists():
            raw = pd.read_csv(raw_path, usecols=["date", "close_raw"])
            raw["date"] = pd.to_datetime(raw["date"])
            raw = raw[(raw["date"] >= pd.Timestamp("2010-01-01")) & (raw["date"] <= pd.Timestamp("2026-09-24"))]
            raw_dates, qfq_dates = set(raw["date"]), set(qfq["date"])
            result.update({
                "raw_rows": int(len(raw)),
                "raw_first": str(raw["date"].min().date()) if len(raw) else None,
                "raw_last": str(raw["date"].max().date()) if len(raw) else None,
                "raw_dates_missing_qfq": len(raw_dates - qfq_dates),
                "qfq_dates_missing_raw": len(qfq_dates - raw_dates),
                "date_intersection": len(raw_dates & qfq_dates),
            })
        else:
            result["raw_status"] = "missing_local_raw"
    except Exception as exc:
        result.update({"status": "error", "error": f"{type(exc).__name__}: {exc}"})
    return result


def main() -> None:
    rows = [inspect(symbol) for symbol in ("002057", "300092", "688701", "000005")]
    path = OUT / "probe.json"
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(rows, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
