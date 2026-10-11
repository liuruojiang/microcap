"""Independent daily/account/metric audit for the L6 core and hedge axes."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from audit_l6_daily import OFFICIAL, ROOT, TOL, TURNOVER, compare, replay, sha, turnover_by_date, window_metrics


HERE = Path(__file__).resolve().parent
L6 = HERE.parent
CASES = (
    ("core", L6 / "core_executor", "daily", "formal", "meta.json"),
    ("hedge", L6 / "20261003_v23_hedge_axes", "daily_outputs", "formal_s1_e08", "scan_meta.json"),
)


def check_numeric(left: pd.Series, right: pd.Series, tol: float = 1e-12) -> dict[str, object]:
    a, b = pd.to_numeric(left, errors="coerce").to_numpy(float), pd.to_numeric(right, errors="coerce").to_numpy(float)
    bad = ~np.isclose(a, b, rtol=0, atol=tol, equal_nan=True)
    where = np.flatnonzero(bad)
    return {"count": int(len(where)), "max_abs": float(np.max(np.abs(a[np.isfinite(a) & np.isfinite(b)] - b[np.isfinite(a) & np.isfinite(b)]))),
            "first_index": int(where[0]) if len(where) else None}


def audit_case(kind: str, directory: Path, daily_dir: str, baseline: str, meta_file: str,
               official: pd.DataFrame, rebalance: np.ndarray) -> dict[str, object]:
    meta = json.loads((directory / meta_file).read_text(encoding="utf-8"))
    files = sorted((directory / daily_dir).glob("*.csv.gz"))
    window_col = "window" if kind == "core" else "segment"
    saved = pd.read_csv(directory / "scan_summary.csv").set_index(["candidate", window_col])
    wide = pd.read_csv(directory / "window_metrics.csv").set_index("candidate")
    summary = {"source_and_input_sha_match": {}, "candidate_count": len(files), "cases": {}, "metric_mismatches": []}
    if kind == "core":
        for path, digest in meta["source_and_input_sha256"].items():
            summary["source_and_input_sha_match"][path] = sha(Path(path)) == digest
    else:
        for name, entry in meta["data_snapshot"]["inputs"].items():
            summary["source_and_input_sha_match"][name] = sha(ROOT / entry["path"]) == entry["sha256"]
    for path in files:
        name = path.name.removesuffix(".csv.gz")
        frame = pd.read_csv(path, parse_dates=["date"])
        if len(frame) != len(official) or not frame.date.equals(official.date):
            raise AssertionError(f"{kind}/{name}: dates diverge from official")
        width = int(pd.to_numeric(frame.overheat_feature_window).iloc[0])
        trigger = float(pd.to_numeric(frame.overheat_trigger_threshold).iloc[0])
        recovery = float(pd.to_numeric(frame.overheat_recovery_threshold).iloc[0])
        expected = replay(frame, rebalance, width, trigger, recovery, False)
        parity = compare(frame, expected)
        held = frame.base_holding.ne("cash").to_numpy(bool)
        next_base = frame.base_next_holding.ne("cash").to_numpy(bool)
        base_lag_bad = np.flatnonzero(held[1:] != next_base[:-1])
        ratio = float(frame.execution_hedge_ratio.iloc[0])
        leg_raw = np.where(held,
                           frame.microcap_ret.fillna(0.0).to_numpy(float)
                           - ratio * frame.hedge_ret.fillna(0.0).to_numpy(float)
                           - frame.futures_drag.fillna(0.0).to_numpy(float), 0.0)
        gross_bad = np.flatnonzero(~np.isclose(leg_raw, frame.return_raw.to_numpy(float), rtol=0, atol=2e-12))
        blocked_cash = frame.overheat_risk_off.astype(bool).to_numpy() & ~expected["held"] & ~expected["next"]
        blocked_cash_bad = np.flatnonzero(blocked_cash & (np.abs(expected["return_net"]) > TOL))
        basic_frozen = {}
        for col in ("microcap_close", "hedge_close", "microcap_ret", "hedge_ret"):
            basic_frozen[col] = check_numeric(frame[col], official[col])["count"]
        if kind == "core":
            setting = next(item["settings"] for item in meta["candidate_grid"] if item["candidate"] == name)
            formal = meta["formal_tuple"].copy()
            formal.update(setting)
            mapping = {"LOOKBACK": "lookback", "HALFLIFE": "halflife", "R2_ENTRY_GATE": "r2_entry_gate",
                       "MOMENTUM_GAP_ENTRY_THRESHOLD": "entry_threshold",
                       "MOMENTUM_GAP_EXIT_BUFFER": "momentum_gap_exit_buffer",
                       "SIGNAL_SPREAD_HEDGE_RATIO": "signal_spread_hedge_ratio",
                       "EXECUTION_HEDGE_RATIO": "execution_hedge_ratio"}
            setting_mismatch = {col: float(frame[col].iloc[0]) for key, col in mapping.items()
                                if frame[col].nunique(dropna=False) != 1
                                or not np.isclose(float(frame[col].iloc[0]), formal[key], atol=1e-12, rtol=0)}
        else:
            first_saved = saved.loc[(name, "full")]
            setting_mismatch = {}
            for col, field in (("signal_spread_hedge_ratio", "signal_hedge_ratio"),
                               ("execution_hedge_ratio", "execution_hedge_ratio")):
                if frame[col].nunique(dropna=False) != 1 or not np.isclose(float(frame[col].iloc[0]), float(first_saved[field]), atol=1e-12, rtol=0):
                    setting_mismatch[col] = float(frame[col].iloc[0])
        if name == baseline:
            numeric_parity = {col: check_numeric(frame[col], official[col])
                              for col in ("return_net", "nav_net", "total_cost", "rebalance_cost",
                                          "entry_exit_cost", "annualized_log_wls_score", "overheat_feature_value")}
            state_parity = {col: int(np.sum(frame[col].astype(str).to_numpy() != official[col].astype(str).to_numpy()))
                            for col in ("holding", "next_holding", "base_holding", "base_next_holding",
                                        "overheat_risk_off", "overheat_exit_triggered", "overheat_block_entry_triggered",
                                        "overheat_reentry_triggered")}
        else:
            numeric_parity = state_parity = None
        expected_metrics = window_metrics(frame.date, expected["return_net"].astype(float))
        for window, independent in expected_metrics.items():
            row = saved.loc[(name, window)]
            scale = 1 if kind == "core" else 100
            pairs = (("annual_pct" if kind == "core" else "ann_return", independent["annual_pct"] / scale),
                     ("max_drawdown_pct" if kind == "core" else "max_dd", independent["max_drawdown_pct"] / scale),
                     ("ending_nav" if kind == "core" else "final_nav", independent["final_nav"]),
                     ("days" if kind == "core" else "rows", independent["days"]))
            for field, value in pairs:
                if not np.isclose(float(row[field]), value, rtol=0, atol=2e-10):
                    summary["metric_mismatches"].append({"candidate": name, "window": window, "field": field,
                                                          "saved": float(row[field]), "independent": value})
            if str(row["start"]) != independent["start_date"] or str(row["end"]) != independent["end_date"]:
                summary["metric_mismatches"].append({"candidate": name, "window": window, "field": "date_bounds"})
            peak_field, trough_field = (("max_drawdown_peak_date", "max_drawdown_trough_date")
                                        if kind == "core" else ("peak_date", "trough_date"))
            expected_peak = independent["peak_date"] or "window_initial_nav"
            expected_trough = independent["trough_date"] or "window_initial_nav"
            if str(row[peak_field]) != str(expected_peak) or str(row[trough_field]) != str(expected_trough):
                summary["metric_mismatches"].append({"candidate": name, "window": window, "field": "peak_trough_dates",
                                                      "saved": [str(row[peak_field]), str(row[trough_field])],
                                                      "independent": [expected_peak, expected_trough]})
            wide_pairs = (
                ((f"annual_pct_{window}", independent["annual_pct"]),
                 (f"max_drawdown_pct_{window}", independent["max_drawdown_pct"]))
                if kind == "core" else
                ((f"ann_return_{window}", independent["annual_pct"] / 100),
                 (f"max_dd_{window}", independent["max_drawdown_pct"] / 100))
            )
            for field, value in wide_pairs:
                if not np.isclose(float(wide.loc[name, field]), value, rtol=0, atol=2e-10):
                    summary["metric_mismatches"].append({"candidate": name, "window": window,
                                                          "field": f"wide_{field}"})
        recorded_hash = meta["daily_outputs"][name]["sha256"] if kind == "hedge" else None
        summary["cases"][name] = {
            "rows": len(frame), "sha256": sha(path),
            "recorded_hash_match": recorded_hash == sha(path) if recorded_hash is not None else None,
            "state_cost_return_nav_parity": parity,
            "base_holding_lag_mismatch": int(len(base_lag_bad)),
            "first_base_lag_mismatch_date": str(frame.date.iloc[base_lag_bad[0] + 1].date()) if len(base_lag_bad) else None,
            "gross_from_legs_mismatch": int(len(gross_bad)),
            "first_gross_mismatch_date": str(frame.date.iloc[gross_bad[0]].date()) if len(gross_bad) else None,
            "blocked_cash_nonzero_return": int(len(blocked_cash_bad)),
            "frozen_market_mismatches": basic_frozen,
            "parameter_mismatches": setting_mismatch,
            "official_baseline_numeric_parity": numeric_parity,
            "official_baseline_state_parity": state_parity,
            "metrics": expected_metrics,
        }
    summary["all_daily_parity_pass"] = all(
        all(x.get("mismatch_count") == 0 for x in case["state_cost_return_nav_parity"].values())
        and case["base_holding_lag_mismatch"] == 0
        and case["gross_from_legs_mismatch"] == 0
        and case["blocked_cash_nonzero_return"] == 0
        and all(value == 0 for value in case["frozen_market_mismatches"].values())
        and not case["parameter_mismatches"]
        for case in summary["cases"].values()
    )
    summary["all_metric_parity_pass"] = not summary["metric_mismatches"]
    if kind == "hedge" and "hedge_off_s0_e0" in summary["cases"]:
        off = pd.read_csv(directory / daily_dir / "hedge_off_s0_e0.csv.gz")
        base_active = off.base_holding.ne("cash").to_numpy(bool)
        micro_return = off.microcap_ret.fillna(0).to_numpy(float)
        spread_return = off.spread_nav.pct_change(fill_method=None).to_numpy(float)
        summary["joint_hedge_off"] = {
            "signal_ratio_zero_all_rows": bool(off.signal_spread_hedge_ratio.eq(0).all()),
            "execution_ratio_zero_all_rows": bool(off.execution_hedge_ratio.eq(0).all()),
            "futures_drag_max_abs": float(off.futures_drag.abs().max()),
            "gross_minus_microcap_only_max_abs": float(np.max(np.abs(
                off.return_raw.to_numpy(float) - np.where(base_active, micro_return, 0.0)))),
            "spread_return_minus_microcap_max_abs": float(np.nanmax(np.abs(spread_return[1:] - micro_return[1:]))),
            "retained_entry_exit_cost_sum": float(off.entry_exit_cost.sum()),
            "retained_rebalance_cost_sum": float(off.rebalance_cost.sum()),
            "fee_component_decomposition_available": False,
        }
    return summary


def main() -> None:
    official = pd.read_csv(OFFICIAL, parse_dates=["date"])
    assert official.date.is_unique and official.date.is_monotonic_increasing
    rebalance = turnover_by_date(official.date)
    report = {kind: audit_case(kind, path, daily_dir, baseline, meta_file, official, rebalance)
              for kind, path, daily_dir, baseline, meta_file in CASES}
    path = HERE / "core_hedge_audit.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({kind: {"cases": x["candidate_count"], "daily": x["all_daily_parity_pass"],
                             "metrics": x["all_metric_parity_pass"], "metric_mismatches": len(x["metric_mismatches"])}
                      for kind, x in report.items()}, ensure_ascii=False))


if __name__ == "__main__":
    main()
