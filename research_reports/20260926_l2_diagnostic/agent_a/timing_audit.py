"""Read-only, independent timing and cost invariants for frozen v2.0 files.

Run at repository root with ``python -X utf8 research_reports/.../timing_audit.py``.
This script reads saved CSVs; it never imports project backtest or cost helpers.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
CUTOFF = "2026-09-24"
FILES = {
    "strategy": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv",
    "base": ROOT / "outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv",
    "turnover": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv",
    "proxy": ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv",
    "replay_returns": ROOT / "research_reports/20260926_l1_repair/full_replay/all_daily_returns.csv",
}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(key: str, date_key: str) -> pd.DataFrame:
    frame = pd.read_csv(FILES[key], dtype={"symbol": str})
    frame[date_key] = pd.to_datetime(frame[date_key], errors="raise")
    return frame.loc[frame[date_key].le(pd.Timestamp(CUTOFF))].copy().sort_values(date_key).reset_index(drop=True)


def first_bad(frame: pd.DataFrame, actual: pd.Series, expected: pd.Series, tolerance: float = 1e-10) -> dict:
    a = pd.to_numeric(actual, errors="coerce").to_numpy(dtype=float)
    e = pd.to_numeric(expected, errors="coerce").to_numpy(dtype=float)
    diffs = np.abs(a - e)
    bad = np.flatnonzero(~np.isfinite(diffs) | (diffs > tolerance))
    if len(bad):
        i = int(bad[0])
        return {"count": int(len(bad)), "first_date": str(frame.loc[i, "date"].date()),
                "actual": float(a[i]), "expected": float(e[i]),
                "max_abs_difference": float(np.nanmax(diffs))}
    return {"count": 0, "first_date": None, "max_abs_difference": float(np.nanmax(diffs))}


def audit() -> dict:
    data = {k: read(k, "rebalance_date" if k == "turnover" else "date") for k in FILES}
    strategy = data["strategy"]
    base = data["base"]
    turnover = data["turnover"]
    for date_col in ["execution_date", "effective_date", "return_start_date"]:
        turnover[date_col] = pd.to_datetime(turnover[date_col], errors="coerce", format="mixed")
    proxy = data["proxy"]
    replay = data["replay_returns"]
    for name in ["strategy", "base", "proxy", "replay_returns"]:
        assert data[name]["date"].is_unique, name
    assert turnover["rebalance_date"].is_unique
    assert set(turnover["execution_timing"]) == {"close"}

    findings: dict[str, object] = {}
    active = strategy["holding"].ne("cash").to_numpy()
    next_active = strategy["next_holding"].ne("cash").to_numpy()
    carry = strategy["holding"].iloc[1:].to_numpy() == strategy["next_holding"].iloc[:-1].to_numpy()
    findings["strategy_state_carry"] = {"transitions": len(carry), "mismatches": int((~carry).sum()),
                                         "first_mismatch": None if carry.all() else str(strategy.date.iloc[np.flatnonzero(~carry)[0] + 1].date())}
    findings["strategy_signal_equals_next"] = int((strategy.signal_on.astype(bool).to_numpy() != next_active).sum())

    gross = pd.to_numeric(strategy["microcap_ret"], errors="coerce").to_numpy() - .8 * pd.to_numeric(strategy["hedge_ret"], errors="coerce").to_numpy() - .00024
    gross = np.where(active, gross, 0.0)
    findings["strategy_raw_return_from_current_holding"] = first_bad(strategy, strategy.return_raw, pd.Series(gross))
    findings["strategy_overlay_gross_from_raw"] = first_bad(strategy, strategy.overlay_pre_cost_return, pd.Series(gross))
    wrong_future = np.where(next_active, pd.to_numeric(strategy.microcap_ret) - .8 * pd.to_numeric(strategy.hedge_ret) - .00024, 0.0)
    findings["fault_injection_future_signal_as_current_holding"] = first_bad(strategy, strategy.overlay_pre_cost_return, pd.Series(wrong_future))

    turn = turnover.set_index("execution_date")["two_side_cost_rate"].astype(float)
    mapped = strategy.date.map(turn).fillna(0.0).to_numpy()
    expected_entry_exit = np.where(active != next_active, .003, 0.0)
    expected_rebalance = np.where(active & next_active, mapped, 0.0)
    expected_cost = expected_entry_exit + expected_rebalance
    findings["strategy_entry_exit_fee"] = first_bad(strategy, strategy.entry_exit_cost, pd.Series(expected_entry_exit))
    findings["strategy_rebalance_fee"] = first_bad(strategy, strategy.rebalance_cost, pd.Series(expected_rebalance))
    findings["strategy_total_fee"] = first_bad(strategy, strategy.total_cost, pd.Series(expected_cost))
    expected_net = (1 + gross) * (1 - expected_cost) - 1
    findings["strategy_net_from_gross_and_fee"] = first_bad(strategy, strategy.return_net, pd.Series(expected_net))
    findings["strategy_nav_compound"] = first_bad(strategy, strategy.nav_net, pd.Series(np.cumprod(1 + expected_net)))
    findings["strategy_return_alias_net"] = first_bad(strategy, strategy["return"], strategy.return_net)
    findings["fault_injection_missing_fee"] = first_bad(strategy, strategy.return_net, pd.Series(gross))
    findings["fault_injection_fee_one_day_late"] = first_bad(strategy, strategy.return_net,
        pd.Series((1 + gross) * (1 - pd.Series(expected_cost).shift(1, fill_value=0).to_numpy()) - 1))

    base_active = base.holding.ne("cash").to_numpy()
    base_next = base.next_holding.ne("cash").to_numpy()
    base_carry = base.holding.iloc[1:].to_numpy() == base.next_holding.iloc[:-1].to_numpy()
    findings["base_state_carry"] = {"transitions": len(base_carry), "mismatches": int((~base_carry).sum()),
                                     "first_mismatch": None if base_carry.all() else str(base.date.iloc[np.flatnonzero(~base_carry)[0] + 1].date())}
    base_gross = np.where(base_active, pd.to_numeric(base.microcap_ret) - .8 * pd.to_numeric(base.hedge_ret) - .00024, 0)
    findings["base_raw_return_from_current_holding"] = first_bad(base, base.return_raw, pd.Series(base_gross))
    base_mapped = base.date.map(turn).fillna(0).to_numpy()
    base_entry_exit = np.where(base_active != base_next, .003, 0)
    base_rebalance = np.where(base_active & base_next, base_mapped, 0)
    findings["base_entry_exit_fee"] = first_bad(base, base.entry_exit_cost, pd.Series(base_entry_exit))
    findings["base_rebalance_fee"] = first_bad(base, base.rebalance_cost, pd.Series(base_rebalance))
    findings["base_net_from_gross_and_fee"] = first_bad(base, base.return_net,
        pd.Series((1 + base_gross) * (1 - base_entry_exit - base_rebalance) - 1))

    merged = strategy.merge(base[["date", "microcap_close", "hedge_close"]], on="date",
                            how="left", validate="one_to_one", suffixes=("_strategy", "_base"))
    findings["strategy_base_microcap_close"] = first_bad(merged, merged.microcap_close_strategy,
                                                          merged.microcap_close_base)
    findings["strategy_base_hedge_close"] = first_bad(merged, merged.hedge_close_strategy,
                                                       merged.hedge_close_base)
    micro_mom = base.microcap_close.astype(float).div(base.microcap_close.astype(float).shift(16)).sub(1)
    hedge_mom = base.hedge_close.astype(float).div(base.hedge_close.astype(float).shift(16)).sub(1)
    expected_mom = base[["date"]].assign(micro_mom=micro_mom, hedge_mom=hedge_mom)
    signal_check = strategy.merge(expected_mom, on="date", how="left", validate="one_to_one")
    findings["signal_microcap_mom_from_prior_16_close"] = first_bad(signal_check, signal_check.microcap_mom,
                                                                     signal_check.micro_mom)
    findings["signal_hedge_mom_from_prior_16_close"] = first_bad(signal_check, signal_check.hedge_mom_x,
                                                                  signal_check.hedge_mom_y)
    expected_signal = signal_check.micro_mom.to_numpy() > signal_check.hedge_mom_y.to_numpy()
    findings["signal_next_holding_from_momentum_gap"] = {
        "rows": len(signal_check), "mismatches": int(np.sum(expected_signal != next_active)),
        "first_mismatch": None if np.all(expected_signal == next_active) else
            str(strategy.date.iloc[np.flatnonzero(expected_signal != next_active)[0]].date())}

    p = proxy.merge(replay[["date", "calculated_return"]], on="date", how="left", validate="one_to_one")
    findings["proxy_raw_replay_parity"] = first_bad(p, p.daily_return, p.calculated_return)
    proxy_days = proxy.date.to_numpy(dtype="datetime64[ns]")
    expected_next = pd.to_datetime(proxy_days[np.searchsorted(proxy_days, turnover.execution_date.to_numpy(dtype="datetime64[ns]"), side="right")])
    recorded_start = pd.to_datetime(turnover.return_start_date, errors="coerce")
    next_mismatch = recorded_start.ne(expected_next)
    findings["proxy_turnover_date_contract"] = {
        "count": len(turnover),
        "date_mismatch": int((turnover.rebalance_date.ne(turnover.execution_date) |
                               turnover.rebalance_date.ne(turnover.effective_date)).sum()),
        "return_start_not_after_execution": int(turnover.return_start_date.notna().sum() -
            (pd.to_datetime(turnover.return_start_date, errors="coerce") > turnover.execution_date).sum()),
        "return_start_not_next_proxy_day": int(next_mismatch.sum()),
        "first_return_start_mismatch": None if not next_mismatch.any() else str(turnover.loc[next_mismatch, "rebalance_date"].iloc[0].date()),
        "first": str(turnover.rebalance_date.iloc[0].date()),
        "last": str(turnover.rebalance_date.iloc[-1].date()),
    }
    expected_turn_rate = .003 * (turnover.entry_count.to_numpy() + turnover.exit_count.to_numpy()) / 100
    findings["proxy_turnover_fee_formula"] = first_bad(turnover.rename(columns={"rebalance_date": "date"}),
        turnover.two_side_cost_rate, pd.Series(expected_turn_rate))
    injection_turn = turnover.copy()
    injection_turn.loc[0, "return_start_date"] = injection_turn.loc[0, "execution_date"]
    findings["fault_injection_same_day_member_return"] = {
        "detected": bool(pd.to_datetime(injection_turn.return_start_date, errors="coerce").le(injection_turn.execution_date).any()),
        "first_date": str(injection_turn.rebalance_date.iloc[0].date())}

    return {
        "scope": "frozen saved-v2.0 arithmetic diagnostic, not PIT/execution certification",
        "cutoff": CUTOFF,
        "files": {k: {"path": str(p), "sha256_full_file": digest(p), "read_rows": len(data[k]),
                       "read_first_date": str(data[k].iloc[0]["rebalance_date" if k == "turnover" else "date"].date()),
                       "read_last_date": str(data[k].iloc[-1]["rebalance_date" if k == "turnover" else "date"].date())}
                  for k, p in FILES.items()},
        "findings": findings,
    }


if __name__ == "__main__":
    report = audit()
    OUT.joinpath("audit.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
