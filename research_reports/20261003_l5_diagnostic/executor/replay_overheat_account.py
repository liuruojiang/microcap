"""Isolated L5 diagnostic: matched v2.3 ON/OFF recent stock plus hedge accounts.

The recent stock-account proxy and the L2 ledger engine are frozen inputs. This
runner only writes beside itself and never changes official outputs or code.
"""

from __future__ import annotations

import hashlib
import json
import math
import sys
from pathlib import Path

import baostock as bs
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
RUN = Path(__file__).resolve().parent
L2 = ROOT / "research_reports/20261002_l2_full_repair"
INTEGRATION = L2 / "version_integration"
ENGINE = L2 / "engine"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(INTEGRATION))
sys.path.insert(0, str(ENGINE))

import microcap_top100_mom16_biweekly_live_v2_3 as v23  # noqa: E402
import rebuild_recent_signals as signals  # noqa: E402
import strategy_replay as account  # noqa: E402
from extend_ledger import Source, digest, rows  # noqa: E402


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source() -> Source:
    src = Source()
    # Reuse response rows already sealed by the L2 runs, including symbols
    # missing from the broad cache.  Further needed rows are queried live.
    for path in sorted(INTEGRATION.glob("recent_*_v2_3_strategy_queried_bars.csv")):
        for row in rows(path):
            src.bars[(row["symbol"].zfill(6), row["date"])] = row
    return src


def account_summary(path: Path) -> dict:
    df = pd.read_csv(path, dtype={"date": str})
    for col in ("cash", "swap_margin", "swap_debt", "equity", "stock_value", "short_notional_after_close", "stock_trade_fees", "swap_drag"):
        df[col] = pd.to_numeric(df[col], errors="raise")
    if not np.isfinite(df[["cash", "swap_margin", "equity", "stock_value"]].to_numpy()).all():
        raise ValueError("nonfinite_account_fields")
    if (df.cash < -1e-8).any() or (df.equity <= 0).any():
        raise ValueError("invalid_cash_or_equity")
    if not np.allclose(df.swap_debt, np.maximum(-df.swap_margin, 0), rtol=0, atol=1e-7):
        raise ValueError("swap_debt_identity")
    df["liquid_after_margin"] = df.cash + df.swap_margin
    df["funding_gap_if_cash_covers_margin"] = np.maximum(-df.liquid_after_margin, 0)
    df["return"] = df.equity.pct_change()
    first = df.iloc[0]
    last = df.iloc[-1]
    nav = df.equity / first.equity
    mdd = (nav / nav.cummax() - 1).min()
    eligible = df.loc[df.funding_gap_if_cash_covers_margin > 1e-8]
    debt = df.loc[df.swap_debt > 1e-8]
    return {
        "date_first": first.date, "date_last": last.date, "days": len(df),
        "first_equity": float(first.equity), "last_equity": float(last.equity),
        "period_return": float(last.equity / first.equity - 1),
        "period_max_drawdown": float(mdd),
        "min_stock_cash": float(df.cash.min()),
        "first_swap_debt_date": None if debt.empty else debt.iloc[0].date,
        "max_swap_debt": float(df.swap_debt.max()),
        "first_funding_gap_date": None if eligible.empty else eligible.iloc[0].date,
        "max_funding_gap": float(df.funding_gap_if_cash_covers_margin.max()),
        "sum_stock_trade_fees": float(df.stock_trade_fees.sum()),
        "sum_swap_drag": float(df.swap_drag.sum()),
        "negative_equity_days": int((df.equity <= 0).sum()),
    }


def main() -> None:
    RUN.mkdir(parents=True, exist_ok=True)
    initial_hashes = {
        str(p.relative_to(ROOT)): sha(p)
        for p in [INTEGRATION / f"recent_{b}_rebuilt_signals.csv" for b in ("prefunded", "netted")]
        + [ENGINE / f"recent_{b}_daily.csv" for b in ("prefunded", "netted")]
        + [INTEGRATION / f"recent_{b}_v2_3_strategy_daily.csv" for b in ("prefunded", "netted")]
        + [INTEGRATION / f"recent_{b}_v2_3_strategy_queried_bars.csv" for b in ("prefunded", "netted")]
        + [ENGINE / "queried_baostock_bars.csv", L2 / "data/event_candidates_2010_20260930.csv",
           ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv",
           ROOT / "outputs/microcap_top100_mom16_plain_fixed1_v2_0_costed_nav.csv",
           INTEGRATION / "strategy_replay.py", INTEGRATION / "rebuild_recent_signals.py",
           ENGINE / "extend_ledger.py", ENGINE / "recent_slice.py",
           ROOT / "microcap_top100_mom16_biweekly_live_v2_3.py"]
    }
    baseline = {}
    for branch in ("prefunded", "netted"):
        stock = ENGINE / f"recent_{branch}_daily.csv"
        _, close = signals.load_new_proxy(stock)
        gross = v23.build_spread_log_wls_gross(close)
        zero_turnover = pd.DataFrame(columns=["rebalance_date", "two_side_cost_rate"])
        saved = pd.read_csv(INTEGRATION / f"recent_{branch}_rebuilt_signals.csv", dtype={"date": str}).set_index("date")
        saved.index = pd.to_datetime(saved.index)
        on = v23.apply_overheat_defense(gross, zero_turnover)
        score_diff = (pd.to_numeric(saved.loc[gross.index, "v2_3_score"]) - gross.annualized_log_wls_score).abs().max()
        if score_diff > 1e-12 or not saved.loc[gross.index, "v2_3_next_session_actionable_scale"].astype(float).equals(on.next_session_actionable_scale.astype(float)):
            raise ValueError(f"on_signal_parity_failed:{branch}:{score_diff}")
        baseline[branch] = {"signal_rows": len(on), "max_abs_score_diff": float(score_diff), "signal_scale_mismatches": 0}
        trigger = v23.OVERHEAT_TRIGGER_THRESHOLD
        try:
            v23.OVERHEAT_TRIGGER_THRESHOLD = math.inf
            off = v23.apply_overheat_defense(gross, zero_turnover)
        finally:
            v23.OVERHEAT_TRIGGER_THRESHOLD = trigger
        assert off.holding.equals(off.base_holding) and off.next_holding.equals(off.base_next_holding)
        for mode, scaled in (("on", on), ("off", off)):
            out = RUN / f"{branch}_{mode}"
            out.mkdir(exist_ok=True)
            frame = saved.copy()
            clock = signals.next_close_clock(scaled.next_holding.ne("cash"))
            for col, series in {
                "score": gross.annualized_log_wls_score,
                "current_execution_scale": scaled.current_execution_scale,
                "next_session_actionable_scale": scaled.next_session_actionable_scale,
                "scale_change_cost": scaled.scale_change_cost,
                **{col: clock[col] for col in clock},
            }.items():
                frame.loc[series.index, "v2_3_" + col] = series
            frame.to_csv(out / f"recent_{branch}_rebuilt_signals.csv", index_label="date", encoding="utf-8")
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"baostock_login_failed:{login.error_code}:{login.error_msg}")
    result = {"label": "frozen_old_pool_recent_conditional_account_not_formal", "on_signal_parity": baseline,
              "model": "T_close_signal_T_plus_1_close_frozen_shares_theoretical_daily_reset_0p8_short_swap",
              "initial_input_sha256": initial_hashes, "cases": {}}
    try:
        for branch in ("prefunded", "netted"):
            for mode in ("on", "off"):
                out = RUN / f"{branch}_{mode}"
                account.HERE = out
                src = source()
                case = account.run_account(branch, "v2.3", src)
                case["source_queries"] = src.queries
                queried = [row for (symbol, date), row in sorted(src.bars.items())
                           if any(item["symbol"] == symbol and item["start_date"] <= date <= item["end_date"]
                                  for item in src.queries)]
                if queried:
                    response_path = out / "new_queried_baostock_bars.csv"
                    account.write_csv(response_path, queried)
                    case["new_query_response"] = {"path": str(response_path), "rows": len(queried),
                                                  "sha256": sha(response_path)}
                if case["first_block"] is not None or case["last_settled"] != "2026-09-30":
                    result["cases"][f"{branch}_{mode}"] = case
                    continue
                day = out / f"recent_{branch}_v2_3_strategy_daily.csv"
                case["summary"] = account_summary(day)
                case["input_signal_sha256"] = sha(out / f"recent_{branch}_rebuilt_signals.csv")
                result["cases"][f"{branch}_{mode}"] = case
    finally:
        bs.logout()
    result["pairs"] = {}
    for branch in ("prefunded", "netted"):
        on_dir, off_dir = RUN / f"{branch}_on", RUN / f"{branch}_off"
        on_sig = pd.read_csv(on_dir / f"recent_{branch}_rebuilt_signals.csv", dtype={"date": str})
        off_sig = pd.read_csv(off_dir / f"recent_{branch}_rebuilt_signals.csv", dtype={"date": str})
        a = pd.to_numeric(on_sig.v2_3_next_session_actionable_scale, errors="coerce")
        b = pd.to_numeric(off_sig.v2_3_next_session_actionable_scale, errors="coerce")
        different_signal = on_sig.loc[a.notna() & b.notna() & a.ne(b), "date"].tolist()
        on_daily = pd.read_csv(on_dir / f"recent_{branch}_v2_3_strategy_daily.csv", dtype={"date": str})
        off_daily = pd.read_csv(off_dir / f"recent_{branch}_v2_3_strategy_daily.csv", dtype={"date": str})
        if not on_daily.date.equals(off_daily.date):
            raise ValueError(f"paired_calendar_mismatch:{branch}")
        different_equity = on_daily.loc[(on_daily.equity - off_daily.equity).abs() > 1e-7, "date"].tolist()
        old_parity = {}
        for kind in ("daily", "holdings", "orders", "rights"):
            old_path = INTEGRATION / f"recent_{branch}_v2_3_strategy_{kind}.csv"
            new_path = on_dir / f"recent_{branch}_v2_3_strategy_{kind}.csv"
            old_parity[kind] = sha(old_path) == sha(new_path)
        if not all(old_parity.values()):
            raise ValueError(f"on_existing_account_parity:{branch}:{old_parity}")
        result["pairs"][branch] = {
            "valid_signal_diff_days": len(different_signal),
            "first_signal_diff": different_signal[0] if different_signal else None,
            "equity_diff_days": len(different_equity),
            "first_equity_diff": different_equity[0] if different_equity else None,
            "on_existing_account_byte_parity": old_parity,
        }
    for path, old_sha in initial_hashes.items():
        if sha(ROOT / path) != old_sha:
            raise AssertionError(f"source_mutated:{path}")
    result["initial_sources_unchanged"] = True
    (RUN / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"on_signal_parity": baseline, "cases": {k: {"first_block": v["first_block"], "last_settled": v["last_settled"], "summary": v.get("summary")} for k,v in result["cases"].items()}}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
