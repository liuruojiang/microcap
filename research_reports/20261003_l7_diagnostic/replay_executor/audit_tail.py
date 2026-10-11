"""Read-only replay of the frozen no-rebalance Top100 proxy tail.

This is a diagnostic of the already saved legacy paper proxy, not a historical
lineage migration or a replacement for whole-delivery certification.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys

import pandas as pd


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as runtime


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    proxy_path = ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
    effective_path = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_effective_members.csv"
    turnover_path = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
    input_hashes = {p.name: sha256(p) for p in (proxy_path, effective_path, turnover_path)}
    proxy = pd.read_csv(proxy_path)
    proxy["date"] = pd.to_datetime(proxy["date"])
    turnover = pd.read_csv(turnover_path)
    turnover["rebalance_date"] = pd.to_datetime(turnover["rebalance_date"])
    seed = pd.read_csv(effective_path, dtype={"symbol": str}).sort_values("rank")
    assert len(seed) == seed["symbol"].nunique() == 100
    bridge = pd.Timestamp(seed["as_of_date"].unique().item())
    assert turnover["rebalance_date"].max() == bridge
    tail = proxy.loc[proxy["date"].ge(bridge)].copy()
    assert len(tail) > 1 and tail["date"].iloc[0] == bridge
    assert not turnover["rebalance_date"].gt(bridge).any()
    dates = pd.DatetimeIndex(tail["date"])

    returns = pd.DataFrame(index=dates)
    source_counts = {"adjusted": 0, "raw": 0}
    cache_hashes: dict[str, str] = {}
    for symbol in seed["symbol"]:
        adjusted = runtime.freq_mod.resolve_cache_path(
            runtime.freq_mod.ADJ_PRICE_DIR, runtime.freq_mod.SHARED_ADJ_PRICE_DIR, symbol
        )
        raw = runtime.freq_mod.resolve_cache_path(
            runtime.freq_mod.PRICE_DIR, runtime.freq_mod.SHARED_PRICE_DIR, symbol
        )
        source = adjusted or raw
        if source is None:
            raise FileNotFoundError(f"no price cache for executed member {symbol}")
        source_counts["adjusted" if adjusted else "raw"] += 1
        cache_hashes[symbol] = sha256(source)
        frame = pd.read_csv(source)
        close_col = next((col for col in ("close_qfq", "close_adj", "close_raw", "close") if col in frame), None)
        if close_col is None or "date" not in frame:
            raise ValueError(f"invalid price cache {source}")
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame[close_col] = pd.to_numeric(frame[close_col], errors="coerce")
        close = (frame.dropna(subset=["date", close_col])
                 .sort_values("date").drop_duplicates("date", keep="last")
                 .set_index("date")[close_col].reindex(dates).ffill())
        if close.isna().any() or close.le(0).any():
            raise ValueError(f"missing nonpositive close in tail for {symbol}")
        returns[symbol] = close.pct_change(fill_method=None).fillna(0.0)

    daily = returns.fillna(0.0).mean(axis=1).iloc[1:]
    anchor_close = float(tail["close"].iloc[0])
    batch_close = anchor_close * (1.0 + daily).cumprod()
    # Simulate two deterministic resume batches from the same settled bridge.
    split = max(1, len(daily) // 2)
    first = anchor_close * (1.0 + daily.iloc[:split]).cumprod()
    second = float(first.iloc[-1]) * (1.0 + daily.iloc[split:]).cumprod()
    incremental_close = pd.concat([first, second])
    target = tail.set_index("date").iloc[1:]
    detail = pd.DataFrame({
        "date": daily.index,
        "saved_return": target["daily_return"].to_numpy(),
        "replayed_return": daily.to_numpy(),
        "saved_close": target["close"].to_numpy(),
        "batch_close": batch_close.to_numpy(),
        "two_batch_close": incremental_close.to_numpy(),
    })
    detail["return_abs_error"] = (detail["replayed_return"] - detail["saved_return"]).abs()
    detail["batch_close_abs_error"] = (detail["batch_close"] - detail["saved_close"]).abs()
    detail["resume_close_abs_error"] = (detail["two_batch_close"] - detail["saved_close"]).abs()
    detail.to_csv(HERE / "tail_replay.csv", index=False, encoding="utf-8")
    maximum = {col: float(detail[col].max()) for col in (
        "return_abs_error", "batch_close_abs_error", "resume_close_abs_error"
    )}
    report = {
        "status": "PASS" if maximum["return_abs_error"] < 1e-12 and
                              maximum["batch_close_abs_error"] < 1e-7 and
                              maximum["resume_close_abs_error"] < 1e-7 else "FAIL",
        "bridge": bridge.date().isoformat(),
        "tail_end": dates[-1].date().isoformat(),
        "tail_sessions": len(daily),
        "source_counts": source_counts,
        "max_abs_error": maximum,
        "input_sha256": input_hashes,
        "cache_count": len(cache_hashes),
        "cache_sha256_digest": hashlib.sha256(json.dumps(cache_hashes, sort_keys=True).encode()).hexdigest(),
        "read_only_frozen_legacy_proxy": True,
    }
    (HERE / "tail_replay.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
