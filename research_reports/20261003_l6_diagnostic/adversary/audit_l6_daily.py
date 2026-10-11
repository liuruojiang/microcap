"""Independent accounting audit of the frozen v2.3 L6 overheat scan.

The script deliberately does not import the strategy or executor.  It reads the
frozen official base signal and turnover events, rebuilds feature, state,
costed daily return and NAV, and checks each saved candidate CSV.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from bisect import bisect_left, bisect_right
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
OFFICIAL = ROOT / "outputs/microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv"
TURNOVER = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
EXPECTED_OFFICIAL_SHA = "9aa647b2f661ca3edb65b835870827ff4913b92044064c3ef25eccf7c31c82c5"
TRADING_DAYS = 244
ENTRY_EXIT_COST = 0.003
TOL = 2e-10


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def window_metrics(dates: pd.Series, ret: np.ndarray) -> dict[str, dict[str, object]]:
    result = {}
    start, end = dates.iloc[0], dates.iloc[-1]
    for label, years in (("full", None), ("last_10y", 10), ("last_5y", 5), ("last_3y", 3), ("last_1y", 1)):
        boundary = start if years is None else end - pd.DateOffset(years=years)
        mask = dates.ge(boundary).to_numpy()
        selected = ret[mask]
        ds = dates[mask].reset_index(drop=True)
        nav = np.cumprod(1 + selected)
        high = np.maximum.accumulate(np.r_[1.0, nav])
        dd = np.r_[1.0, nav] / high - 1
        vol = float(np.std(selected, ddof=1) * np.sqrt(TRADING_DAYS))
        trough = int(np.argmin(dd))
        peak = int(np.argmax(np.r_[1.0, nav][: trough + 1]))
        result[label] = {
            "start_date": str(ds.iloc[0].date()),
            "end_date": str(ds.iloc[-1].date()),
            "days": len(selected),
            "annual_pct": float((nav[-1] ** (TRADING_DAYS / len(selected)) - 1) * 100),
            "annual_vol_pct": vol * 100,
            "sharpe_repo": float((nav[-1] ** (TRADING_DAYS / len(selected)) - 1) / vol) if vol else None,
            "max_drawdown_pct": float(dd[trough] * 100),
            "final_nav": float(nav[-1]),
            "peak_date": None if peak == 0 else str(ds.iloc[peak - 1].date()),
            "trough_date": None if trough == 0 else str(ds.iloc[trough - 1].date()),
        }
    return result


def turnover_by_date(dates: pd.Series) -> np.ndarray:
    events = pd.read_csv(TURNOVER)
    date_list = list(dates)
    result = np.zeros(len(dates))
    for event in events.itertuples(index=False):
        when = pd.Timestamp(event.execution_date)
        if event.execution_timing == "close":
            i = bisect_left(date_list, when)
        else:
            i = bisect_right(date_list, when)
        if i < len(date_list):
            result[i] += float(event.two_side_cost_rate)
    return result


def calc_vol(spread: np.ndarray, width: int) -> np.ndarray:
    ret = np.zeros(len(spread))
    ret[1:] = spread[1:] / spread[:-1] - 1
    return pd.Series(ret).rolling(width).std(ddof=1).to_numpy() * np.sqrt(TRADING_DAYS)


def replay(frozen: pd.DataFrame, rebalance: np.ndarray, width: int, trigger: float,
           recovery: float, disabled: bool) -> dict[str, np.ndarray]:
    feature = calc_vol(frozen["spread_nav"].to_numpy(float), width)
    base_next = frozen["base_next_holding"].ne("cash").to_numpy(bool)
    gross = frozen["return_raw"].to_numpy(float)
    current = bool(frozen["base_holding"].iloc[0] != "cash")
    defensive = False
    names = ("held", "next", "risk_off", "exit", "reentry", "block", "entry_exit_cost",
             "rebalance_cost", "total_cost", "return_net", "nav_net")
    out: dict[str, list[float | bool]] = {name: [] for name in names}
    wealth = 1.0
    for i in range(len(frozen)):
        desired = bool(base_next[i])
        old_defensive = defensive
        exit_event = reentry_event = block_event = False
        if not disabled:
            if defensive:
                if np.isfinite(feature[i]) and feature[i] <= recovery:
                    defensive = False
                    reentry_event = desired
                else:
                    desired = False
            elif desired and np.isfinite(feature[i]) and feature[i] >= trigger:
                if current:
                    exit_event = True
                else:
                    block_event = True
                desired = False
                defensive = True
        entry_exit = ENTRY_EXIT_COST if current != desired else 0.0
        rebalance_cost = rebalance[i] if current and desired else 0.0
        cost = entry_exit + rebalance_cost
        market = gross[i] if current else 0.0
        net = (1.0 + market) * (1.0 - cost) - 1.0
        wealth *= 1.0 + net
        values = (current, desired, old_defensive, exit_event, reentry_event, block_event,
                  entry_exit, rebalance_cost, cost, net, wealth)
        for name, value in zip(names, values):
            out[name].append(value)
        current = desired
    converted = {key: np.asarray(value) for key, value in out.items()}
    converted["feature"] = feature
    return converted


def compare(candidate: pd.DataFrame, expected: dict[str, np.ndarray]) -> dict[str, dict[str, object]]:
    mapping = {
        "held": "holding", "next": "next_holding", "risk_off": "overheat_risk_off",
        "exit": "overheat_exit_triggered", "reentry": "overheat_reentry_triggered",
        "block": "overheat_block_entry_triggered", "entry_exit_cost": "entry_exit_cost",
        "rebalance_cost": "rebalance_cost", "total_cost": "total_cost", "return_net": "return_net",
        "nav_net": "nav_net", "feature": "overheat_feature_value",
    }
    report = {}
    for key, col in mapping.items():
        if col not in candidate:
            report[key] = {"error": f"missing {col}"}
            continue
        if key in {"held", "next"}:
            actual = candidate[col].ne("cash").to_numpy(bool)
            bad = actual != expected[key]
            delta = None
        elif key in {"risk_off", "exit", "reentry", "block"}:
            actual = candidate[col].astype(bool).to_numpy()
            bad = actual != expected[key]
            delta = None
        else:
            actual = pd.to_numeric(candidate[col], errors="coerce").to_numpy(float)
            want = expected[key].astype(float)
            bad = ~np.isclose(actual, want, rtol=0, atol=TOL, equal_nan=True)
            finite = np.isfinite(actual) & np.isfinite(want)
            delta = float(np.max(np.abs(actual[finite] - want[finite]))) if finite.any() else 0.0
        positions = np.flatnonzero(bad)
        report[key] = {
            "mismatch_count": int(len(positions)),
            "first_mismatch_date": str(candidate["date"].iloc[positions[0]].date()) if len(positions) else None,
            "max_abs_delta": delta,
        }
    return report


def parameters(candidate: pd.DataFrame, filename: str) -> tuple[int, float, float, bool]:
    needed = ("overheat_feature_window", "overheat_trigger_threshold", "overheat_recovery_threshold")
    missing = [x for x in needed if x not in candidate]
    if missing:
        raise ValueError(f"{filename}: parameter columns absent: {missing}")
    values = []
    for col in needed:
        unique = pd.to_numeric(candidate[col], errors="coerce").dropna().unique()
        if len(unique) != 1:
            raise ValueError(f"{filename}: {col} is not constant: {unique}")
        values.append(float(unique[0]))
    disabled = "off" in filename.lower()
    return int(values[0]), values[1], values[2], disabled


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=Path)
    args = parser.parse_args()
    run_dir = args.run_dir.resolve()
    meta = json.loads((run_dir / "scan_meta.json").read_text(encoding="utf-8"))
    frozen = pd.read_csv(OFFICIAL, parse_dates=["date"]).sort_values("date").reset_index(drop=True)
    assert frozen.date.is_unique and frozen.date.is_monotonic_increasing
    assert sha(OFFICIAL) == EXPECTED_OFFICIAL_SHA, "official frozen source changed"
    rebalance = turnover_by_date(frozen.date)
    baseline_expected = replay(frozen, rebalance, 10, 0.26, 0.20, False)
    official_parity = compare(frozen, baseline_expected)
    legs_gross = np.where(
        frozen["base_holding"].ne("cash").to_numpy(bool),
        frozen["microcap_ret"].fillna(0.0).to_numpy(float)
        - .8 * frozen["hedge_ret"].fillna(0.0).to_numpy(float)
        - frozen["futures_drag"].fillna(0.0).to_numpy(float),
        0.0,
    )
    files = sorted((run_dir / "daily").glob("*.csv.gz"))
    if not files:
        raise FileNotFoundError(f"no candidate daily CSVs in {run_dir / 'daily'}")
    result: dict[str, object] = {
        "official_sha256": sha(OFFICIAL), "turnover_sha256": sha(TURNOVER),
        "official_rows": len(frozen), "start": str(frozen.date.iloc[0].date()),
        "end": str(frozen.date.iloc[-1].date()), "official_independent_parity": official_parity,
        "gross_from_legs_max_abs_delta": float(np.max(np.abs(legs_gross - frozen.return_raw.to_numpy(float)))),
        "source_hashes_match": {name: sha(ROOT / name) == digest for name, digest in meta["source_hashes"].items()},
        "snapshot_hashes_match": {name: sha(Path(item["path"])) == item["sha256"]
                                  for name, item in meta["data_snapshot"].items()},
        "summary_hashes_match": {name: sha(run_dir / name) == digest
                                 for name, digest in meta["output_hashes"].items()},
        "candidates": {},
    }
    for path in files:
        candidate = pd.read_csv(path, parse_dates=["date"])
        if len(candidate) != len(frozen) or not candidate.date.equals(frozen.date):
            raise AssertionError(f"{path.name}: dates differ from frozen official")
        width, trigger, recovery, disabled = parameters(candidate, path.name)
        expected = replay(frozen, rebalance, width, trigger, recovery, disabled)
        checks = compare(candidate, expected)
        frozen_columns = ("spread_nav", "return_raw", "microcap_ret", "hedge_ret", "annualized_log_wls_score",
                          "base_holding", "base_next_holding", "lookback", "halflife", "r2_entry_gate",
                          "entry_threshold", "momentum_gap_exit_buffer", "signal_spread_hedge_ratio",
                          "execution_hedge_ratio", "futures_drag", "target_vol_enabled",
                          "cash_day_yield_enabled", "financing_enabled")
        frozen_mismatch = {}
        for col in frozen_columns:
            if col not in candidate:
                frozen_mismatch[col] = {"error": "missing"}
                continue
            left, right = candidate[col], frozen[col]
            if pd.api.types.is_numeric_dtype(right):
                bad = ~np.isclose(pd.to_numeric(left, errors="coerce"), pd.to_numeric(right, errors="coerce"),
                                  rtol=0, atol=1e-12, equal_nan=True)
            else:
                bad = (left.astype(str) != right.astype(str)).to_numpy()
            if np.any(bad):
                first = int(np.flatnonzero(bad)[0])
                frozen_mismatch[col] = {"count": int(np.sum(bad)), "first_date": str(candidate.date.iloc[first].date())}
        audit = {
            "sha256": sha(path), "window": width, "trigger": trigger,
            "recovery": recovery, "disabled": disabled,
            "parity": checks,
            "frozen_column_mismatches": frozen_mismatch,
            "metrics": window_metrics(candidate.date, expected["return_net"].astype(float)),
            "holding_days": int(expected["held"].sum()),
            "sum_daily_cost_rates": float(expected["total_cost"].sum()),
            "exit_count": int(expected["exit"].sum()),
            "block_count": int(expected["block"].sum()),
            "reentry_count": int(expected["reentry"].sum()),
            "risk_off_days": int(expected["risk_off"].sum()),
            "cash_day_with_nonzero_gross_count": int((~expected["held"] & (np.abs(frozen.return_raw.to_numpy(float)) > 1e-12)).sum()),
            "cash_day_net_minus_fee_max_abs": float(np.max(np.abs(expected["return_net"][~expected["held"]] + expected["total_cost"][~expected["held"]]))),
        }
        result["candidates"][path.name] = audit
        saved = meta["daily_files"][path.name.removesuffix(".csv.gz")]
        audit["meta_hash_match"] = saved["sha256"] == audit["sha256"]
        audit["meta_parameter_match"] = (int(saved["feature_window"]) == width
                                          and bool(saved["enabled"]) == (not disabled)
                                          and (disabled or (abs(float(saved["trigger"]) - trigger) < 1e-12
                                                            and abs(float(saved["recovery"]) - recovery) < 1e-12)))
    wide = pd.read_csv(run_dir / "window_metrics.csv").set_index("candidate")
    long = pd.read_csv(run_dir / "scan_summary.csv").set_index(["candidate", "segment"])
    metric_checks = []
    for filename, candidate in result["candidates"].items():
        name = filename.removesuffix(".csv.gz")
        for label, independent in candidate["metrics"].items():
            row = long.loc[(name, label)]
            fields = {
                "ann_return": independent["annual_pct"] / 100,
                "ann_vol": independent["annual_vol_pct"] / 100,
                "sharpe_repo": independent["sharpe_repo"],
                "max_dd": independent["max_drawdown_pct"] / 100,
                "final_nav": independent["final_nav"],
                "rows": independent["days"],
                "holding_days": int(candidate["holding_days"]) if label == "full" else None,
            }
            for field, expected_value in fields.items():
                if expected_value is None:
                    continue
                actual_value = row[field]
                if not np.isclose(float(actual_value), float(expected_value), atol=2e-10, rtol=0):
                    metric_checks.append({"candidate": name, "window": label, "field": field,
                                          "saved": float(actual_value), "independent": float(expected_value)})
            if str(row["start"]) != independent["start_date"] or str(row["end"]) != independent["end_date"]:
                metric_checks.append({"candidate": name, "window": label, "field": "date_bounds"})
            for field, expected_value in (("ann_return", independent["annual_pct"] / 100),
                                          ("max_dd", independent["max_drawdown_pct"] / 100),
                                          ("ann_vol", independent["annual_vol_pct"] / 100),
                                          ("sharpe_repo", independent["sharpe_repo"])):
                actual_value = wide.loc[name, f"{field}_{label}"]
                if not np.isclose(float(actual_value), float(expected_value), atol=2e-10, rtol=0):
                    metric_checks.append({"candidate": name, "window": label, "field": f"wide_{field}",
                                          "saved": float(actual_value), "independent": float(expected_value)})
    result["metric_mismatches"] = metric_checks
    event_mismatches = []
    for filename, candidate in result["candidates"].items():
        name = filename.removesuffix(".csv.gz")
        for field, expected in (("exit_events", candidate["exit_count"]),
                                ("blocked_entry_events", candidate["block_count"]),
                                ("reentry_events", candidate["reentry_count"]),
                                ("holding_days", candidate["holding_days"])):
            saved = int(wide.loc[name, field]) if field in wide.columns else None
            if saved is not None and saved != expected:
                event_mismatches.append({"candidate": name, "field": field,
                                         "saved": saved, "independent": expected})
        if int(wide.loc[name, "risk_off_days"]) != candidate["risk_off_days"]:
            event_mismatches.append({"candidate": name, "field": "risk_off_days"})
    result["event_mismatches"] = event_mismatches
    observed_grid = {(int(x["window"]), float(x["trigger"]), float(x["recovery"]))
                     for x in result["candidates"].values() if not x["disabled"] and not x["sha256"] == ""}
    required_grid = {(w, t, r) for w in (9, 10, 11) for t in (.255, .26, .265) for r in (.195, .20, .205)}
    result["grid_missing"] = sorted(required_grid - observed_grid)
    result["grid_unique_parameter_count"] = len(observed_grid)
    result["formal_tuple_files"] = sorted(name for name, x in result["candidates"].items()
                                          if not x["disabled"] and x["window"] == 10
                                          and abs(x["trigger"] - .26) < 1e-12
                                          and abs(x["recovery"] - .20) < 1e-12)
    result["daily_file_count_matches_manifest"] = len(files) == len(meta["daily_files"])
    # Two mutations of real rows: a one-day lookahead in the held state and a
    # false nonzero cash-market return must be rejected by the identity check.
    base_held = baseline_expected["held"].astype(bool)
    gross = frozen.return_raw.to_numpy(float)
    wrong_held = np.r_[base_held[1:], base_held[-1]]
    timing_delta = np.abs(np.where(wrong_held, gross, 0.0) - np.where(base_held, gross, 0.0))
    cash_indices = np.flatnonzero((~base_held) & (np.abs(gross) > 1e-8))
    baseline_file = pd.read_csv(run_dir / "daily/formal_baseline.csv.gz", parse_dates=["date"])
    cash_mutated = baseline_file.copy()
    cash_mutated.at[int(cash_indices[0]), "return_net"] += gross[cash_indices[0]]
    cash_mutation_rejected = compare(cash_mutated, baseline_expected)["return_net"]["mismatch_count"] >= 1
    first_transition = int(np.flatnonzero(base_held != wrong_held)[0])
    lag_mutated = baseline_file.copy()
    lag_mutated.at[first_transition, "holding"] = (
        "long_microcap_short_zz1000" if bool(wrong_held[first_transition]) else "cash")
    lag_mutation_rejected = compare(lag_mutated, baseline_expected)["held"]["mismatch_count"] >= 1
    result["injections"] = {
        "one_day_lookahead_rejected_days": int(np.sum(timing_delta > 1e-12)),
        "first_lookahead_rejected_date": str(frozen.date.iloc[np.flatnonzero(timing_delta > 1e-12)[0]].date()),
        "cash_gross_leak_date": str(frozen.date.iloc[cash_indices[0]].date()),
        "cash_gross_leak_return_delta": float(abs(gross[cash_indices[0]])),
        "cash_return_mutation_rejected": bool(cash_mutation_rejected),
        "one_day_state_mutation_rejected": bool(lag_mutation_rejected),
        "wrong_N_minus_1_full_annual_pct": float((baseline_expected["nav_net"][-1] ** (TRADING_DAYS / (len(frozen)-1)) - 1) * 100),
        "correct_N_full_annual_pct": result["candidates"]["formal_baseline.csv.gz"]["metrics"]["full"]["annual_pct"],
    }
    result["all_candidate_parity_pass"] = all(
        all(entry.get("mismatch_count") == 0 for entry in candidate["parity"].values())
        for candidate in result["candidates"].values()
    ) and all(entry.get("mismatch_count") == 0 for entry in official_parity.values())
    out = HERE / "audit_result.json"
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"candidates": len(files), "all_candidate_parity_pass": result["all_candidate_parity_pass"],
                      "result": str(out)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
