from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import analyze_top100_rebalance_frequency as freq


DEFAULT_MEMBERS = ROOT / "outputs" / "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
DEFAULT_REPORT = ROOT / "outputs" / "microcap_top100_st_metadata_refresh_report.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh audited historical ST metadata for recent Top100 candidates.")
    parser.add_argument("--members", type=Path, default=DEFAULT_MEMBERS)
    parser.add_argument("--since", default="2025-08-19")
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    members = pd.read_csv(args.members, dtype={"symbol": str})
    members["symbol"] = members["symbol"].astype(str).str.zfill(6)
    members["rebalance_date"] = pd.to_datetime(members["rebalance_date"], errors="coerce")
    recent_symbols = set(
        members.loc[members["rebalance_date"] >= pd.Timestamp(args.since), "symbol"].dropna().astype(str)
    )
    current_st_names_all = freq.load_current_st_name_map()
    backtest_universe = set(freq.list_backtest_universe_symbols())
    current_st_names = {
        symbol: name for symbol, name in current_st_names_all.items() if symbol in backtest_universe
    }
    symbols = sorted(recent_symbols | set(current_st_names))

    results: dict[str, dict[str, object]] = {}
    failures: dict[str, str] = {}
    completed = 0
    workers = max(1, min(int(args.max_workers), 16))

    def refresh_one(symbol: str) -> tuple[str, dict[str, object] | None]:
        return symbol, freq.build_security_meta(symbol)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(refresh_one, symbol): symbol for symbol in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                code, meta = future.result()
                if meta is None:
                    failures[code] = "metadata build returned None"
                else:
                    results[code] = meta
            except Exception as exc:
                failures[symbol] = f"{type(exc).__name__}: {exc}"
            completed += 1
            if completed % 25 == 0 or completed == len(symbols):
                print(
                    f"[st-meta-refresh] {completed}/{len(symbols)} "
                    f"built={len(results)} failures={len(failures)}",
                    flush=True,
                )

    unresolved_current_st = {
        symbol: {
            "name": current_st_names[symbol],
            "notice_query_status": meta.get("notice_query_status"),
            "name_history_status": meta.get("name_history_status"),
            "st_intervals": meta.get("st_intervals"),
        }
        for symbol, meta in results.items()
        if symbol in current_st_names and not bool(meta.get("current_st_history_resolved"))
    }
    notice_failures = {
        symbol: str(meta.get("notice_query_status"))
        for symbol, meta in results.items()
        if meta.get("notice_query_status") != "ok"
    }
    report = {
        "generated_at": datetime.now().astimezone().isoformat(),
        "members_path": str(args.members),
        "since": str(pd.Timestamp(args.since).date()),
        "recent_member_symbols": len(recent_symbols),
        "current_st_symbols_total": len(current_st_names_all),
        "current_st_symbols_in_backtest_universe": len(current_st_names),
        "backtest_universe_symbols": len(backtest_universe),
        "requested_symbols": len(symbols),
        "built_symbols": len(results),
        "build_failures": failures,
        "notice_failures": notice_failures,
        "unresolved_current_st": unresolved_current_st,
        "st_notice_policy_version": freq.ST_NOTICE_POLICY_VERSION,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items() if key not in {"build_failures", "notice_failures", "unresolved_current_st"}}, ensure_ascii=False, indent=2))
    if failures or notice_failures or unresolved_current_st:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
