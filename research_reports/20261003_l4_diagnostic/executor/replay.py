"""Frozen-input v2.3 overheat ON/OFF diagnostic. Never writes official outputs."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
RUN = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_3 as v  # noqa: E402


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def describe_csv(path: Path, date_col: str) -> dict:
    dates = pd.read_csv(path, usecols=[date_col], parse_dates=[date_col])[date_col].dropna()
    return {"path": str(path), "sha256": digest(path), "rows": len(dates),
            "first_date": str(dates.min().date()), "last_date": str(dates.max().date())}


def drawdown_dates(part: pd.Series) -> tuple[float, str, str]:
    nav = (1.0 + part.astype(float)).cumprod().to_numpy()
    running = np.maximum.accumulate(np.r_[1.0, nav])[1:]
    dd = nav / running - 1.0
    trough_at = int(np.argmin(dd))
    prior = nav[:trough_at + 1]
    peak_at = int(np.argmax(prior))
    peak_date = str(part.index[peak_at].date()) if prior[peak_at] >= 1.0 else "window_initial_nav"
    return float(dd[trough_at]), peak_date, str(part.index[trough_at].date())


def main() -> None:
    RUN.mkdir(parents=True, exist_ok=True)
    paths = v.v2_0._resolve_base_paths()
    seed_panel_path = Path(paths.panel_path)
    panel_path = Path(paths.output_paths["panel_shadow"])
    proxy_path = Path(paths.index_csv)
    turnover_path = Path(paths.output_paths["proxy_turnover"])
    base_nav_path = Path(paths.costed_nav_csv)
    v20_nav_path = Path(v.v2_0.COSTED_NAV_CSV)
    official_path = Path(v.COSTED_NAV_CSV)
    files = {
        "source_panel_seed_not_directly_used": describe_csv(seed_panel_path, "date"),
        "panel_shadow_used": describe_csv(panel_path, "date"),
        "proxy_index": describe_csv(proxy_path, "date"),
        "proxy_turnover": describe_csv(turnover_path, "rebalance_date"),
        "base_costed_nav": describe_csv(base_nav_path, "date"),
        "v2_0_costed_nav": describe_csv(v20_nav_path, "date"),
        "v2_3_official_frozen": describe_csv(official_path, "date"),
    }
    _, cached, turnover = v.v2_0.embedded_context._load_embedded_base_context()
    for item in files.values():
        assert digest(Path(item["path"])) == item["sha256"], f"input changed while loading: {item['path']}"
    close = v._close_df_from_base(cached)
    official20 = pd.read_csv(v20_nav_path, parse_dates=["date"]).set_index("date")
    official = pd.read_csv(official_path, parse_dates=["date"]).set_index("date")
    index = v.build_v2_3_common_index(close, official20.index)
    gross = v.build_spread_log_wls_gross(close, index)
    on = v.apply_overheat_defense(gross, turnover)
    assert on.index.equals(official.index)
    cols_num = ["return_net", "nav_net", "total_cost", "rebalance_cost", "entry_exit_cost", "annualized_log_wls_score", "overheat_feature_value"]
    cols_bool = ["holding", "next_holding", "base_holding", "base_next_holding", "overheat_risk_off", "overheat_exit_triggered", "overheat_block_entry_triggered", "overheat_reentry_triggered"]
    parity = []
    for col in cols_num:
        a, b = pd.to_numeric(on[col], errors="coerce"), pd.to_numeric(official[col], errors="coerce")
        assert np.allclose(a, b, atol=1e-12, rtol=0, equal_nan=True), col
        parity.append({"column": col, "max_abs_diff": float((a - b).abs().max()), "mismatch_rows": 0})
    for col in cols_bool:
        mismatch = int(on[col].ne(official[col]).sum())
        assert mismatch == 0, col
        parity.append({"column": col, "max_abs_diff": None, "mismatch_rows": mismatch})
    pd.DataFrame(parity).to_csv(RUN / "on_official_parity.csv", index=False, encoding="utf-8-sig")

    trigger = v.OVERHEAT_TRIGGER_THRESHOLD
    try:
        v.OVERHEAT_TRIGGER_THRESHOLD = np.inf
        off = v.apply_overheat_defense(gross, turnover)
    finally:
        v.OVERHEAT_TRIGGER_THRESHOLD = trigger
    assert off.index.equals(on.index)
    assert not off[["overheat_risk_off", "overheat_exit_triggered", "overheat_reentry_triggered", "overheat_block_entry_triggered"]].any().any()
    assert off.holding.equals(off.base_holding) and off.next_holding.equals(off.base_next_holding)
    pure = v.apply_cost(gross, turnover)
    assert np.allclose(off.return_net, pure.return_net, rtol=0, atol=1e-12)
    assert np.allclose(off.nav_net, pure.nav_net, rtol=0, atol=1e-12)
    for label, data in (("on", on), ("off", off)):
        assert data.index.max() == pd.Timestamp("2026-09-30")
        assert np.isfinite(data.return_net).all()
        assert (data.loc[data.holding.eq("cash") & data.next_holding.eq("cash"), "return_net"] == 0).all()
        assert data.holding.iloc[1:].reset_index(drop=True).equals(data.next_holding.iloc[:-1].reset_index(drop=True))
        no_cost_nav = (1.0 + data.base_pre_cost_return).cumprod()
        assert (data.nav_net <= no_cost_nav + 1e-10).all()
        data.to_csv(RUN / f"v23_{label}_daily.csv.gz", index_label="date", compression="gzip")

    paired = pd.DataFrame(index=index)
    paired["score"] = on.annualized_log_wls_score
    paired["feature"] = on.overheat_feature_value
    paired["on_holding"] = on.holding
    paired["off_holding"] = off.holding
    paired["on_next_holding"] = on.next_holding
    paired["off_next_holding"] = off.next_holding
    paired["on_return_net"] = on.return_net
    paired["off_return_net"] = off.return_net
    paired["on_cost"] = on.total_cost
    paired["off_cost"] = off.total_cost
    paired["on_nav"] = on.nav_net
    paired["off_nav"] = off.nav_net
    for col in ("overheat_risk_off", "overheat_exit_triggered", "overheat_block_entry_triggered", "overheat_reentry_triggered"):
        paired[col] = on[col]
    paired["state_diff"] = on.holding.ne(off.holding) | on.next_holding.ne(off.next_holding)
    paired["return_diff"] = (on.return_net - off.return_net).abs() > 1e-12
    paired.to_csv(RUN / "paired_daily.csv.gz", index_label="date", compression="gzip")
    paired.loc[paired[["overheat_exit_triggered", "overheat_block_entry_triggered", "overheat_reentry_triggered"]].any(axis=1)].to_csv(RUN / "events.csv", index_label="date", encoding="utf-8-sig")

    metrics = []
    for label, data in (("on", on), ("off", off)):
        for item in v.summarize_required_windows(data.return_net):
            part = data.loc[item["start_date"]:item["end_date"]]
            max_dd, peak_date, trough_date = drawdown_dates(part.return_net)
            assert abs(max_dd * 100.0 - float(item["max_drawdown_pct"])) < 1e-10
            metrics.append({"mode": label, **item, "max_dd_peak_date": peak_date,
                            "max_dd_trough_date": trough_date,
                            "held_days": int(part.holding.ne("cash").sum()),
                            "held_day_ratio": float(part.holding.ne("cash").mean()),
                            "cash_days": int(part.holding.eq("cash").sum()),
                            "entry_exit_fee_event_days": int(part.entry_exit_cost.gt(0).sum()),
                            "rebalance_fee_event_days": int(part.rebalance_cost.gt(0).sum()),
                            "total_fee_event_days": int(part.total_cost.gt(0).sum()),
                            "sum_daily_cost_rates": float(part.total_cost.sum())})
    pd.DataFrame(metrics).to_csv(RUN / "five_windows.csv", index=False, encoding="utf-8-sig")
    selected = paired.loc[paired.state_diff | paired.return_diff]
    meta = {
        "status": "diagnostic_only_frozen_old_inputs",
        "reason_formal_blocked": "L1/L2 PIT lineage unresolved and local/remote source identity mismatch; no official refresh or migration",
        "source_sha256": {str(p.relative_to(ROOT)): digest(p) for p in (ROOT / "microcap_top100_mom16_biweekly_live_v2_3.py", ROOT / "microcap_top100_mom16_biweekly_live_v2_0.py")},
        "files": files,
        "output": {"rows": len(on), "first_date": str(on.index.min().date()), "last_date": str(on.index.max().date())},
        "on_official_parity": {"numerical_tolerance": 1e-12, "all_checked_pass": True},
        "off_pure_base_parity": True,
        "lagged_holding_state_and_nonnegative_cost_path_checks": True,
        "parameters": {"lookback": v.LOOKBACK, "halflife": v.HALFLIFE, "r2_gate": v.R2_ENTRY_GATE, "exit_buffer": v.MOMENTUM_GAP_EXIT_BUFFER, "vol_window": v.OVERHEAT_FEATURE_WINDOW, "trigger": trigger, "recovery": v.OVERHEAT_RECOVERY_THRESHOLD, "execution_hedge_ratio": v.EXECUTION_HEDGE_RATIO, "target_vol_enabled": v.TARGET_VOL_ENABLED, "cash_yield_enabled": v.CASH_DAY_YIELD_ENABLED, "financing_enabled": v.FINANCING_ENABLED},
        "events": {"risk_off_days": int(on.overheat_risk_off.sum()), "exits": int(on.overheat_exit_triggered.sum()), "blocked_entries": int(on.overheat_block_entry_triggered.sum()), "reentries": int(on.overheat_reentry_triggered.sum()), "held_state_diff_days": int(on.holding.ne(off.holding).sum()), "any_state_diff_days": int(paired.state_diff.sum()), "return_diff_days": int(paired.return_diff.sum()), "first_state_diff": str(selected.index.min().date()) if len(selected) else None, "first_return_diff": str(paired.index[paired.return_diff][0].date()) if paired.return_diff.any() else None,
                   "first_exit": str(on.index[on.overheat_exit_triggered][0].date()) if on.overheat_exit_triggered.any() else None,
                   "last_exit": str(on.index[on.overheat_exit_triggered][-1].date()) if on.overheat_exit_triggered.any() else None,
                   "first_block_entry": str(on.index[on.overheat_block_entry_triggered][0].date()) if on.overheat_block_entry_triggered.any() else None,
                   "last_block_entry": str(on.index[on.overheat_block_entry_triggered][-1].date()) if on.overheat_block_entry_triggered.any() else None,
                   "first_reentry": str(on.index[on.overheat_reentry_triggered][0].date()) if on.overheat_reentry_triggered.any() else None,
                   "last_reentry": str(on.index[on.overheat_reentry_triggered][-1].date()) if on.overheat_reentry_triggered.any() else None},
        "costs_sum": {"on": float(on.total_cost.sum()), "off": float(off.total_cost.sum())},
        "command": "python -X utf8 research_reports/20261003_l4_diagnostic/executor/replay.py",
    }
    for item in files.values():
        assert digest(Path(item["path"])) == item["sha256"], f"input changed during replay: {item['path']}"
    (RUN / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    print(json.dumps({"output": meta["output"], "events": meta["events"], "costs_sum": meta["costs_sum"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
