"""Independent, read-only replay of the frozen v2.0 plain Mom16 paper baseline.

This intentionally does not import the strategy implementation or write outputs/.
It uses the saved proxy close, hedge panel and turnover event files, and compares
its result to the saved formal v2.0 CSV. The result is diagnostic only.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
CUTOFF = pd.Timestamp("2026-09-24")  # L0 frozen common close
FILES = {
    "source": ROOT / "microcap_top100_mom16_biweekly_live_v2_0.py",
    "proxy": ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv",
    "panel": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv",
    "turnover": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv",
    "base": ROOT / "outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv",
    "official": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv",
}


def read(name: str, date_col: str = "date") -> pd.DataFrame:
    df = pd.read_csv(FILES[name])
    df[date_col] = pd.to_datetime(df[date_col], errors="raise", format="mixed")
    df = df.loc[df[date_col].le(CUTOFF)].sort_values(date_col).reset_index(drop=True)
    assert df[date_col].is_unique, name
    return df


def check(a: pd.DataFrame, b: pd.DataFrame, name: str, tol: float = 1e-10) -> dict:
    av = a[name].to_numpy()
    bv = b[name].to_numpy()
    if name in {"holding", "next_holding", "strategy_revision"}:
        wrong = av.astype(str) != bv.astype(str)
        max_abs = None
    else:
        aa, bb = av.astype(float), bv.astype(float)
        diff = np.abs(aa - bb)
        wrong = ~np.isfinite(diff) | (diff > tol)
        max_abs = float(np.nanmax(diff))
    ids = np.flatnonzero(wrong)
    return {
        "mismatch_count": int(len(ids)),
        "first_date": str(a.date.iloc[ids[0]].date()) if len(ids) else None,
        "actual": str(b[name].iloc[ids[0]]) if len(ids) else None,
        "replay": str(a[name].iloc[ids[0]]) if len(ids) else None,
        "max_abs": max_abs,
    }


def run() -> dict:
    source = FILES["source"].read_text(encoding="utf-8")
    for token in (
        'STRATEGY_REVISION = "plain_mom16_fixed1_20260904"',
        "TARGET_VOL_ENABLED = False",
        "OVERHEAT_ENABLED = False",
        "LOOKBACK = 16",
        "REQUIRE_POSITIVE_MICROCAP_MOM = False",
        "V2_0_MOMENTUM_GAP_EXIT_BUFFER = 0.0",
        "BASE_HEDGE_RATIO = 0.8",
    ):
        assert token in source, token
    proxy = read("proxy")[["date", "close", "holding_effective"]]
    panel = read("panel")[["date", "1.000852"]]
    turnover = read("turnover", "rebalance_date")
    turnover["execution_date"] = pd.to_datetime(turnover.execution_date, format="mixed")
    assert set(turnover.execution_timing) == {"close"}
    cost = dict(zip(turnover.execution_date, turnover.two_side_cost_rate.astype(float), strict=True))
    base = read("base")
    official = read("official")
    assert set(official.strategy_revision) == {"plain_mom16_fixed1_20260904"}

    # The source loader takes the effective proxy and panel intersection. The
    # saved embedded-base first day fixes the historical warmup origin.
    daily = proxy.merge(panel, on="date", how="inner", validate="one_to_one")
    daily = daily.loc[daily.holding_effective.astype(bool) & daily.date.ge(base.date.min())].reset_index(drop=True)
    daily = daily.rename(columns={"close": "microcap_close", "1.000852": "hedge_close"})
    assert (daily[["microcap_close", "hedge_close"]] > 0).all().all()
    daily["microcap_ret"] = daily.microcap_close.pct_change(fill_method=None)
    daily["hedge_ret"] = daily.hedge_close.pct_change(fill_method=None)
    daily["microcap_mom"] = daily.microcap_close.div(daily.microcap_close.shift(16)).sub(1)
    daily["hedge_mom"] = daily.hedge_close.div(daily.hedge_close.shift(16)).sub(1)
    daily["momentum_gap"] = daily.microcap_mom - daily.hedge_mom
    valid = np.flatnonzero(daily[["microcap_mom", "hedge_mom"]].notna().all(axis=1).to_numpy())
    assert len(valid)
    daily = daily.iloc[valid[0] + 1:].reset_index(drop=True)  # source run_backtest skips valid_start row
    assert daily.date.equals(official.date), "date-index parity failed"

    rows = []
    active = False
    nav = 1.0
    for row in daily.itertuples(index=False):
        next_active = bool(row.momentum_gap >= 0) if active else bool(row.momentum_gap > 0)
        gross = float(row.microcap_ret - .8 * row.hedge_ret - .00024) if active else 0.0
        event_cost = float(cost.get(row.date, 0.0)) if active and next_active else 0.0
        state_cost = .003 if active != next_active else 0.0
        fee = event_cost + state_cost
        net = (1.0 + gross) * (1.0 - fee) - 1.0
        nav *= 1.0 + net
        rows.append({
            "date": row.date, "microcap_close": row.microcap_close,
            "hedge_close": row.hedge_close, "microcap_ret": row.microcap_ret,
            "hedge_ret": row.hedge_ret, "microcap_mom": row.microcap_mom,
            "hedge_mom": row.hedge_mom, "momentum_gap": row.momentum_gap,
            "holding": "long_microcap_short_zz1000" if active else "cash",
            "next_holding": "long_microcap_short_zz1000" if next_active else "cash",
            "current_execution_scale": 1.0 if active else 0.0,
            "entry_exit_cost": state_cost, "rebalance_cost": event_cost,
            "total_cost": fee, "base_pre_cost_return": gross,
            "return_net": net, "nav_net": nav,
            "strategy_revision": "plain_mom16_fixed1_20260904",
        })
        active = next_active
    replay = pd.DataFrame(rows)
    fields = ["microcap_close", "hedge_close", "microcap_ret", "hedge_ret",
              "microcap_mom", "hedge_mom", "momentum_gap", "holding",
              "next_holding", "current_execution_scale", "entry_exit_cost",
              "rebalance_cost", "total_cost", "base_pre_cost_return",
              "return_net", "nav_net", "strategy_revision"]
    parity = {field: check(replay, official, field) for field in fields}
    assert all(item["mismatch_count"] == 0 for item in parity.values()), parity
    assert not official[["target_vol_enabled", "overheat_enabled", "peak_decay_enabled", "net_drawdown_stop_enabled"]].astype(bool).any().any()
    assert (official[["scale_change_cost", "financing_cost", "idle_cash_yield"]].astype(float) == 0).all().all()

    wrong_signal = daily.microcap_mom.gt(.8 * daily.hedge_mom).to_numpy()
    right_signal = replay.next_holding.ne("cash").to_numpy()
    wrong_signal_days = np.flatnonzero(wrong_signal != right_signal)
    missing_fee_days = np.flatnonzero(np.abs(replay.return_net.to_numpy(float) - replay.base_pre_cost_return.to_numpy(float)) > 1e-10)
    assert len(wrong_signal_days) and len(missing_fee_days)

    # An independent simple diagnostic metric, never a revised official figure.
    running = np.maximum.accumulate(np.r_[1.0, replay.nav_net.to_numpy(float)])
    drawdown = np.r_[1.0, replay.nav_net.to_numpy(float)] / running - 1.0
    summary = {
        "scope": "frozen old-input paper-path parity only; not executable or formal historical certification",
        "cutoff": str(CUTOFF.date()),
        "date_first": str(replay.date.iloc[0].date()),
        "date_last": str(replay.date.iloc[-1].date()),
        "rows": len(replay),
        "file_sha256": {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in FILES.items()},
        "file_full_rows": {name: int(len(pd.read_csv(path))) for name, path in FILES.items() if name != "source"},
        "official_read_rows": len(official),
        "signal_on_days": int(replay.next_holding.ne("cash").sum()),
        "held_days": int(replay.holding.ne("cash").sum()),
        "state_changes": int(replay.holding.ne(replay.next_holding).sum()),
        "fee_days": int(replay.total_cost.gt(0).sum()),
        "total_fee_rate_sum": float(replay.total_cost.sum()),
        "member_turnover_fee_rate_sum": float(replay.rebalance_cost.sum()),
        "final_nav_diagnostic": float(replay.nav_net.iloc[-1]),
        "annualized_diagnostic": float(replay.nav_net.iloc[-1] ** (244 / len(replay)) - 1),
        "max_drawdown_diagnostic": float(drawdown.min()),
        "parity": parity,
        "fault_probes": {
            "wrong_signal_hedge_0p8_instead_of_1": {
                "rejected_days": int(len(wrong_signal_days)),
                "first_date": str(replay.date.iloc[wrong_signal_days[0]].date()),
            },
            "omitted_fees": {
                "rejected_days": int(len(missing_fee_days)),
                "first_date": str(replay.date.iloc[missing_fee_days[0]].date()),
            },
        },
    }
    replay.to_csv(HERE / "diagnostic_daily.csv", index=False, encoding="utf-8")
    (HERE / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: summary[k] for k in ("rows", "date_first", "date_last", "signal_on_days", "fee_days", "final_nav_diagnostic", "annualized_diagnostic", "max_drawdown_diagnostic")}, ensure_ascii=False))
    return summary


if __name__ == "__main__":
    run()
