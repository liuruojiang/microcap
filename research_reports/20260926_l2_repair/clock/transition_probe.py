"""Trace real T-close/T+1-open component availability; no portfolio NAV."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from clock_candidate import EVENTS, OUT, PATHS, ROOT, close_path, event_members


def causal_execution(signal_close: str, execution_open: str) -> bool:
    return execution_open > signal_close


def overnight_earners(members_before: set[str]) -> set[str]:
    return members_before


def main() -> None:
    bars_path = OUT / "baostock_raw_open_windows.csv"
    bars = pd.read_csv(bars_path, dtype={"symbol": str, "tradestatus": str})
    bars["date"] = pd.to_datetime(bars.date)
    for col in ("open", "close", "preclose", "volume"):
        bars[col] = pd.to_numeric(bars[col], errors="coerce")
    indexed = bars.set_index(["date", "symbol"])
    effective = pd.read_csv(PATHS["effective"], dtype={"symbol": str})
    effective["rebalance_date"] = pd.to_datetime(effective.rebalance_date)
    seed = pd.read_csv(PATHS["seed"], dtype={"symbol": str}).sort_values("rank").symbol.tolist()
    proxy = pd.read_csv(PATHS["proxy"])
    proxy.date = pd.to_datetime(proxy.date)
    close_data = {}
    rows = []
    for signal in EVENTS:
        execution = proxy.loc[proxy.date.gt(signal), "date"].min()
        before, after = event_members(effective, seed, signal)
        before_set, after_set = set(before), set(after)
        for symbol in sorted(before_set | after_set):
            key0, key1 = (signal, symbol), (execution, symbol)
            bar0 = indexed.loc[key0] if key0 in indexed.index else None
            bar1 = indexed.loc[key1] if key1 in indexed.index else None
            if symbol not in close_data:
                local_path = close_path(symbol)
                local = pd.read_csv(local_path)
                close_data[symbol] = (dict(zip(local.date, local.close_raw)), hashlib.sha256(local_path.read_bytes()).hexdigest())
            local_closes = close_data[symbol][0]
            date0, date1 = str(signal.date()), str(execution.date())
            local0, local1 = local_closes.get(date0), local_closes.get(date1)
            trade0, trade1 = None if bar0 is None else str(bar0.tradestatus), None if bar1 is None else str(bar1.tradestatus)
            complete = (bar0 is not None and bar1 is not None and local0 is not None and local1 is not None
                and trade0 == trade1 == "1" and abs(float(local0) - float(bar0["close"])) < 1e-8
                and abs(float(local1) - float(bar1["close"])) < 1e-8
                and float(bar1["open"]) > 0)
            reason = "complete" if complete else (
                "BaoStock reports nontrading session" if "0" in (trade0, trade1)
                else "local raw close missing" if local0 is None or local1 is None
                else "bar unavailable" if bar0 is None or bar1 is None else "price mismatch or invalid open")
            rows.append({"signal_close": date0, "candidate_open": date1, "symbol": symbol,
                "position_before": symbol in before_set, "position_after": symbol in after_set,
                "transition": "entry" if symbol not in before_set else "exit" if symbol not in after_set else "hold",
                "baostock_trade_T": trade0, "baostock_trade_T1": trade1,
                "local_close_T": local0, "raw_open_T1": None if bar1 is None else float(bar1["open"]),
                "local_close_T1": local1, "complete_price_window": bool(complete), "reason": reason,
                "overnight_Tclose_to_T1open": (float(bar1["open"]) / float(local0) - 1) if complete else None,
                "intraday_T1open_to_T1close": (float(local1) / float(bar1["open"]) - 1) if complete else None,
            })
    table = pd.DataFrame(rows)
    table.to_csv(OUT / "transition_components.csv", index=False, encoding="utf-8")
    summary = []
    for (signal, execution), group in table.groupby(["signal_close", "candidate_open"], sort=True):
        entry = group.loc[group.transition.eq("entry") & group.complete_price_window]
        exit_ = group.loc[group.transition.eq("exit") & group.complete_price_window]
        assert len(entry) > 0 and len(exit_) > 0, (signal, execution)
        # Deliberate fault injections: the two invalid clocks must be rejected.
        assert causal_execution(signal, execution)
        fault_same_close = not causal_execution(signal, signal)
        before = set(group.loc[group.position_before, "symbol"])
        after = set(group.loc[group.position_after, "symbol"])
        assert set(entry.symbol).isdisjoint(overnight_earners(before))
        fault_new_overnight = bool(set(entry.symbol) & overnight_earners(after))
        assert fault_same_close and fault_new_overnight
        summary.append({"signal_close": signal, "candidate_open": execution,
            "union_symbols": len(group), "complete_price_windows": int(group.complete_price_window.sum()),
            "incomplete_windows": int((~group.complete_price_window).sum()),
            "complete_entries": len(entry), "complete_exits": len(exit_),
            "first_entry": entry.sort_values("symbol").iloc[0][["symbol", "overnight_Tclose_to_T1open", "intraday_T1open_to_T1close"]].to_dict(),
            "first_exit": exit_.sort_values("symbol").iloc[0][["symbol", "overnight_Tclose_to_T1open", "intraday_T1open_to_T1close"]].to_dict(),
            "fault_same_close_rejected": fault_same_close,
            "fault_new_entry_overnight_rejected": fault_new_overnight,
            "complete_portfolio_next_open_nav": False})
    report = {"price_source": "BaoStock anonymous raw OHLC, adjustflag=3; local raw close cross-check",
              "baostock_file_sha256": hashlib.sha256(bars_path.read_bytes()).hexdigest(),
              "local_raw_files": {k: v[1] for k, v in sorted(close_data.items())}, "events": summary,
              "boundary": "T close selection; T+1 open target is diagnostic only; no auction fills or whole-book cash ledger"}
    (OUT / "transition_probe.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
