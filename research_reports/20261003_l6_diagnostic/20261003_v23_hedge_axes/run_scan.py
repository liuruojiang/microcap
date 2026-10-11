"""Research-only v2.3 hedge-axis scan on the frozen 2026-09-30 inputs."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_3 as v  # noqa: E402


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def dates(path: Path, column: str) -> dict:
    frame = pd.read_csv(path, usecols=[column], dtype={column: str})
    values = frame[column].dropna()
    if values.duplicated().any() or not values.is_monotonic_increasing:
        raise ValueError(f"bad_date_index:{path}")
    return {"path": str(path.relative_to(ROOT)), "sha256": sha(path), "rows": len(values),
            "first": values.iloc[0], "last": values.iloc[-1]}


def peaks(part: pd.Series) -> tuple[str, str]:
    nav = (1.0 + part.astype(float)).cumprod().to_numpy()
    running = np.maximum.accumulate(np.r_[1.0, nav])[1:]
    dd = nav / running - 1.0
    trough = int(np.argmin(dd))
    prior = nav[:trough + 1]
    peak = int(np.argmax(prior))
    return (str(part.index[peak].date()) if prior[peak] >= 1.0 else "window_initial_nav",
            str(part.index[trough].date()))


def summarize(candidate: str, frame: pd.DataFrame, signal_ratio: float, execution_ratio: float) -> tuple[list[dict], dict]:
    records = []
    wide = {"candidate": candidate, "signal_hedge_ratio": signal_ratio,
            "execution_hedge_ratio": execution_ratio}
    end = frame.index[-1]
    for window, years in v.REQUIRED_PERFORMANCE_WINDOWS:
        part = frame if years is None else frame.loc[frame.index >= end - pd.DateOffset(years=years)]
        if years is not None and frame.index[0] > end - pd.DateOffset(years=years):
            raise ValueError(f"incomplete_window:{candidate}:{window}")
        ret = pd.to_numeric(part.return_net, errors="raise")
        if ret.isna().any() or not np.isfinite(ret).all():
            raise ValueError(f"nonfinite_return:{candidate}:{window}")
        metric = v.summarize_returns(ret)
        peak, trough = peaks(ret)
        record = {"candidate": candidate, "segment": window, "start": metric["start_date"],
                  "end": metric["end_date"], "rows": metric["days"],
                  "ann_return": metric["annual_pct"] / 100.0,
                  "ann_vol": metric["vol_pct"] / 100.0,
                  "sharpe_repo": metric["sharpe"],
                  "max_dd": metric["max_drawdown_pct"] / 100.0,
                  "final_nav": metric["final_nav"],
                  "holding_days": int(part.holding.ne("cash").sum()),
                  "cost_total": float(pd.to_numeric(part.total_cost).sum()),
                  "peak_date": peak, "trough_date": trough,
                  "signal_hedge_ratio": signal_ratio, "execution_hedge_ratio": execution_ratio}
        records.append(record)
        wide[f"ann_return_{window}"] = record["ann_return"]
        wide[f"max_dd_{window}"] = record["max_dd"]
    wide["full_final_nav"] = records[0]["final_nav"]
    wide["full_holding_days"] = records[0]["holding_days"]
    wide["full_cost_total"] = records[0]["cost_total"]
    wide["full_overheat_exits"] = int(frame.overheat_exit_triggered.sum())
    wide["full_overheat_blocks"] = int(frame.overheat_block_entry_triggered.sum())
    return records, wide


def main() -> None:
    paths = v.v2_0._resolve_base_paths()
    inputs = {
        "panel": dates(Path(paths.output_paths["panel_shadow"]), "date"),
        "proxy_index": dates(Path(paths.index_csv), "date"),
        "proxy_turnover": dates(Path(paths.output_paths["proxy_turnover"]), "rebalance_date"),
        "base_costed_nav": dates(Path(paths.costed_nav_csv), "date"),
        "v2_0_costed_nav": dates(Path(v.v2_0.COSTED_NAV_CSV), "date"),
        "v2_3_costed_nav": dates(Path(v.COSTED_NAV_CSV), "date"),
    }
    for key in ("panel", "proxy_index", "base_costed_nav", "v2_0_costed_nav", "v2_3_costed_nav"):
        if inputs[key]["last"] != "2026-09-30":
            raise ValueError(f"stale_frozen_input:{key}:{inputs[key]['last']}")
    if inputs["proxy_turnover"]["last"] != "2026-09-17":
        raise ValueError("unexpected_latest_rebalance_event")
    official = pd.read_csv(v.COSTED_NAV_CSV, parse_dates=["date"]).set_index("date")
    official20 = pd.read_csv(v.v2_0.COSTED_NAV_CSV, parse_dates=["date"]).set_index("date")
    _raw, cached, turnover = v.v2_0.embedded_context._load_embedded_base_context()
    close = v._close_df_from_base(cached)
    index = v.build_v2_3_common_index(close, official20.index)
    if not index.equals(official.index) or len(index) != 3988:
        raise AssertionError("current_official_calendar_parity_failed")
    grid = [
        ("formal_s1_e08", 1.0, 0.8),
        ("signal_off_s0_e08", 0.0, 0.8),
        ("signal_s08_e08", 0.8, 0.8),
        ("signal_s12_e08", 1.2, 0.8),
        ("execution_off_s1_e0", 1.0, 0.0),
        ("execution_s1_e07", 1.0, 0.7),
        ("execution_s1_e09", 1.0, 0.9),
        ("hedge_off_s0_e0", 0.0, 0.0),
    ]
    original = (v.SIGNAL_SPREAD_HEDGE_RATIO, v.EXECUTION_HEDGE_RATIO, v.BASE_HEDGE_RATIO)
    all_long, all_wide, outputs = [], [], {}
    parity = {}
    folder = HERE / "daily_outputs"
    folder.mkdir(exist_ok=True)
    try:
        for label, signal_ratio, execution_ratio in grid:
            v.SIGNAL_SPREAD_HEDGE_RATIO = signal_ratio
            v.EXECUTION_HEDGE_RATIO = execution_ratio
            v.BASE_HEDGE_RATIO = execution_ratio
            gross = v.build_spread_log_wls_gross(close, index)
            result = v.apply_overheat_defense(gross, turnover)
            if not result.index.equals(index) or len(result) != 3988:
                raise AssertionError(f"calendar_changed:{label}")
            if label == "formal_s1_e08":
                numeric = ("return_net", "nav_net", "total_cost", "rebalance_cost", "entry_exit_cost")
                states = ("holding", "next_holding", "overheat_risk_off", "overheat_exit_triggered",
                          "overheat_block_entry_triggered", "overheat_reentry_triggered")
                for col in numeric:
                    left = pd.to_numeric(result[col], errors="raise")
                    right = pd.to_numeric(official[col], errors="raise")
                    max_diff = float((left - right).abs().max())
                    if not np.allclose(left, right, rtol=0, atol=1e-12):
                        raise AssertionError(f"formal_baseline_parity:{col}:{max_diff}")
                    parity[col] = max_diff
                for col in states:
                    bad = int(result[col].ne(official[col]).sum())
                    if bad:
                        raise AssertionError(f"formal_baseline_state:{col}:{bad}")
                    parity[col] = bad
            if not (result.loc[result.holding.eq("cash") & result.next_holding.eq("cash"), "return_net"] == 0).all():
                raise AssertionError(f"cash_days_nonzero:{label}")
            if not result.holding.iloc[1:].reset_index(drop=True).equals(result.next_holding.iloc[:-1].reset_index(drop=True)):
                raise AssertionError(f"same_day_state_leak:{label}")
            path = folder / f"{label}.csv.gz"
            result.to_csv(path, index_label="date", compression="gzip")
            outputs[label] = {"path": str(path.relative_to(ROOT)), "sha256": sha(path),
                              "rows": len(result), "first": str(result.index[0].date()),
                              "last": str(result.index[-1].date())}
            records, wide = summarize(label, result, signal_ratio, execution_ratio)
            all_long.extend(records)
            all_wide.append(wide)
    finally:
        v.SIGNAL_SPREAD_HEDGE_RATIO, v.EXECUTION_HEDGE_RATIO, v.BASE_HEDGE_RATIO = original
    pd.DataFrame(all_long).to_csv(HERE / "scan_summary.csv", index=False, encoding="utf-8")
    pd.DataFrame(all_wide).to_csv(HERE / "window_metrics.csv", index=False, encoding="utf-8")
    for key, item in inputs.items():
        if sha(ROOT / item["path"]) != item["sha256"]:
            raise AssertionError(f"input_mutated:{key}")
    meta_path = HERE / "scan_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta.update({"scan_type": "single_parameter_axes_with_joint_ratio_off_control",
                 "parameter_group": "signal_and_execution_hedge_ratio",
                 "baseline": {"candidate": "formal_s1_e08", "signal_hedge_ratio": 1.0, "execution_hedge_ratio": 0.8},
                 "candidate_grid": [x[0] for x in grid],
                 "data_snapshot": {"inputs": inputs, "period": [str(index[0].date()), str(index[-1].date())],
                                   "input_note": "frozen_old_PIT_proxy_diagnostic_only"},
                 "cost_model": {"entry_exit_each_side": 0.003, "member_rebalance": "official_turnover_event_map",
                                "hedge_drag_per_notional_day": float(v.v2_0.base_mod.FUTURES_DRAG),
                                "cash_yield": False, "financing": False, "timing": "T_close_signal_T_plus_1_state"},
                 "baseline_parity": parity,
                 "daily_outputs": outputs,
                 "formal_claim": "none_research_only_L1_L2_L5_open"})
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with (HERE / "command_log.txt").open("a", encoding="utf-8") as handle:
        handle.write("python -X utf8 research_reports/20261003_l6_diagnostic/20261003_v23_hedge_axes/run_scan.py\n")
    print(json.dumps({"candidates": len(grid), "rows_per_candidate": len(index),
                      "baseline_max_numeric_diff": max(float(x) for x in parity.values()),
                      "output": str(HERE)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
