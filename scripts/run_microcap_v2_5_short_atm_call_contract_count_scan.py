from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import csi1000_short_atm_call_research as shortscan  # noqa: E402


common = shortscan.common
DEFAULT_RUN_FOLDER = (
    ROOT / "quant_param_scan_runs" / "20260820_v2_5_short_atm_call_2_3_contracts"
)
DEFAULT_RAW_DIR = shortscan.DEFAULT_RAW_DIR
V25_NAV = (
    ROOT
    / "outputs"
    / "microcap_top100_mom16_lb17_hl3_entry46_exit25_no_targetvol_v2_5_costed_nav.csv"
)
V25_SUMMARY = ROOT / "outputs" / "microcap_top100_mom16_biweekly_live_v2_5_summary.json"


def _load_v25() -> tuple[pd.DataFrame, dict[str, Any]]:
    frame = pd.read_csv(V25_NAV, parse_dates=["date"]).set_index("date").sort_index()
    if frame.index.has_duplicates:
        raise RuntimeError("Official v2.5 NAV contains duplicate dates.")
    with V25_SUMMARY.open("r", encoding="utf-8") as fh:
        summary = json.load(fh)
    candidate = summary["data_freshness_proof"]["candidate"]
    if str(frame.index.max().date()) != candidate["latest_date"]:
        raise RuntimeError("v2.5 NAV latest date disagrees with its freshness proof.")
    if len(frame) != int(candidate["row_count"]):
        raise RuntimeError("v2.5 NAV row count disagrees with its freshness proof.")
    if summary["historical_rewrite_audit"]["status"] != "clean":
        raise RuntimeError("v2.5 historical rewrite audit is not clean.")
    return frame, summary


def _assert_v25_stock_leg_parity(v25: pd.DataFrame) -> dict[str, Any]:
    active = v25["holding"].fillna("cash").astype(str).ne("cash")
    stock_return = pd.to_numeric(v25["microcap_ret"], errors="raise").where(active, 0.0)
    cost = pd.to_numeric(v25["total_cost"], errors="raise")
    reconstructed = (1.0 + stock_return) * (1.0 - cost) - 1.0
    official = pd.to_numeric(v25["return_net"], errors="raise")
    max_diff = float((reconstructed - official).abs().max())
    if max_diff > 1e-12:
        raise RuntimeError(f"v2.5 stock-leg parity failed: max diff={max_diff}")
    return {"status": "pass", "rows": len(v25), "max_abs_return_diff": max_diff}


def _baseline(v25: pd.DataFrame) -> pd.DataFrame:
    test = v25.loc[v25.index >= common.LAUNCH_DATE].copy()
    if test.empty or test.index.min() != common.LAUNCH_DATE:
        raise RuntimeError("Official v2.5 NAV does not contain the MO listing date.")
    returns = pd.to_numeric(test["return_net"], errors="raise").astype(float)
    returns.iloc[0] = 0.0
    out = pd.DataFrame(index=test.index)
    out["candidate"] = "original_v2.5_long_only"
    out["contract_count"] = 0
    out["daily_return"] = returns
    out["nav"] = (1.0 + returns).cumprod()
    out["holding"] = test["holding"]
    out["next_holding"] = test["next_holding"]
    for column in (
        "option_pnl_return",
        "option_trade_cost_return",
        "option_fee_cost_return",
        "option_slippage_cost_return",
        "option_expiry_cost_return",
    ):
        out[column] = 0.0
    for column in ("opened_today", "early_close", "expired_today", "rolled_today"):
        out[column] = False
    out["entry_delta_ratio"] = np.nan
    out["held_delta_ratio"] = np.nan
    return out


def _relabel(path: pd.DataFrame, label: str) -> pd.DataFrame:
    out = path.copy()
    out["candidate"] = label
    return out


def _assert_lifecycle(two: pd.DataFrame, three: pd.DataFrame) -> dict[str, Any]:
    if not two.index.equals(three.index):
        raise RuntimeError("Two- and three-contract calendars differ.")
    two_contract = two["contract"].fillna("").astype(str)
    three_contract = three["contract"].fillna("").astype(str)
    if not two_contract.equals(three_contract):
        raise RuntimeError("Two- and three-contract paths selected different contracts.")
    pnl_error = float(
        (three["option_pnl_cny"] - 1.5 * two["option_pnl_cny"]).abs().max()
    )
    cost_error = float(
        (three["option_trade_cost_cny"] - 1.5 * two["option_trade_cost_cny"]).abs().max()
    )
    if pnl_error > 1e-8 or cost_error > 1e-8:
        raise RuntimeError(
            f"Contract-count scaling failed: pnl_error={pnl_error}, cost_error={cost_error}"
        )
    illegal_changes: list[str] = []
    previous = ""
    for date, row in two.iterrows():
        current = str(row["contract"]) if pd.notna(row["contract"]) else ""
        if previous and current and current != previous and not bool(row["rolled_today"]):
            illegal_changes.append(str(pd.Timestamp(date).date()))
        previous = current
    if illegal_changes:
        raise RuntimeError(f"Non-expiry contract changes detected: {illegal_changes[:5]}")
    return {
        "status": "pass",
        "same_contract_path": True,
        "max_abs_three_vs_1p5_two_option_pnl_cny": pnl_error,
        "max_abs_three_vs_1p5_two_cost_cny": cost_error,
        "non_expiry_contract_change_count": 0,
        "two_contract_openings": int(two["opened_today"].sum()),
        "two_contract_early_closes": int(two["early_close"].sum()),
        "two_contract_expiry_rolls": int(two["rolled_today"].sum()),
    }


def _write_record(run_folder: Path, wide: pd.DataFrame, meta: dict[str, Any]) -> None:
    rows = wide.set_index("candidate")
    lines = [
        "# v2.5 Fixed Short-ATM-Call Contract-Count Scan",
        "",
        "## Scope",
        "",
        "- Compare original v2.5 with fixed two/three short front-month ATM MO calls on the exact v2.5 stock timing.",
        "- Hold the selected option contract and count until expiry or v2.5 stock exit; no daily Delta or strike rebalance.",
        "- At expiry, roll to the next front-month ATM call only when v2.5 remains invested.",
        "",
        "## Data Snapshot",
        "",
        f"- Test window: {meta['data_snapshot']['start']} through {meta['data_snapshot']['latest']}, {meta['data_snapshot']['rows']} sessions.",
        f"- Official CFFEX MO calls: {meta['option_data']['rows']:,} rows and {meta['option_data']['contracts']:,} contracts.",
        "- v2.5 panel, proxy index, turnover, base costed NAV, candidate NAV, and MO data share the same 2026-08-20 endpoint.",
        "- The microcap series is the refreshed local/public Top100 proxy, not Wind 868008.WI.",
        "",
        "## Cost and Execution Assumptions",
        "",
        "- Reference stock notional: RMB 1.5m; option multiplier: RMB 100 per point.",
        "- Option fee: RMB 15 per contract per opening/early-close side; expiry fee: RMB 2 when settlement is positive.",
        "- Slippage: one index point on opening and early close; expiry uses official cash settlement.",
        "- Premium is not recognized as entry profit; subsequent settlement mark-to-market P&L is recognized.",
        "- Short-option margin funding and premium cash yield are excluded.",
        "",
        "## Full-Sample Results",
        "",
    ]
    for candidate, row in rows.iterrows():
        lines.append(
            f"- `{candidate}`: annual return {row['ann_return_full']:.2%}, "
            f"max drawdown {row['max_dd_full']:.2%}, Sharpe {row['sharpe_repo_full']:.3f}, "
            f"final NAV {row['final_nav_full']:.4f}."
        )
    lines.extend(
        [
            "",
            "## Window Results",
            "",
            "- `window_metrics.csv` contains full, 10Y, 5Y, 3Y, and 1Y fields.",
            "- 10Y and 5Y are explicitly truncated to the post-MO-listing history and are not full-length evidence.",
            "",
            "## Validation",
            "",
            f"- Official v2.5 stock-leg parity: `{meta['baseline_parity']['status']}`, maximum absolute error {meta['baseline_parity']['max_abs_return_diff']:.3g}.",
            f"- Option lifecycle and 2-to-3 contract scaling: `{meta['lifecycle_validation']['status']}`.",
            "- Candidate calendars match the official v2.5 A-share close calendar; no forward-filled option settlements are used.",
            "",
            "## Stability Classification",
            "",
            f"- Stability label: `{meta['stability_label']}`.",
            "- Cost sensitivity is recorded in `cost_sensitivity.csv`.",
            "",
            "## Decision",
            "",
            f"- Decision: `{meta['decision']}`.",
            "- Production v2.5 source and constants remain unchanged.",
        ]
    )
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(run_folder: Path, raw_dir: Path) -> None:
    started = time.time()
    run_folder.mkdir(parents=True, exist_ok=True)
    with (run_folder / "command_log.txt").open("a", encoding="utf-8") as fh:
        fh.write(
            f"\n[{pd.Timestamp.now().isoformat()}] cwd={ROOT}\n"
            f"python scripts/run_microcap_v2_5_short_atm_call_contract_count_scan.py "
            f"--run-folder \"{run_folder}\" --raw-dir \"{raw_dir}\"\n"
        )

    v25, v25_summary = _load_v25()
    baseline_parity = _assert_v25_stock_leg_parity(v25)
    chain, option_info = shortscan._load_calls(raw_dir)
    latest = pd.Timestamp(v25.index.max())
    if pd.Timestamp(chain["date"].max()) != latest:
        raise RuntimeError("v2.5 and MO option data latest dates do not match.")
    if pd.Timestamp(chain["date"].min()) != common.LAUNCH_DATE:
        raise RuntimeError("MO call history does not start on the 2022-07-22 listing date.")

    two_raw = shortscan._simulate(v25, chain, 2)
    three_raw = shortscan._simulate(v25, chain, 3)
    lifecycle = _assert_lifecycle(two_raw, three_raw)
    paths: dict[str, pd.DataFrame] = {
        "original_v2.5_long_only": _baseline(v25),
        "short_atm_call_2_contracts_on_v2.5": _relabel(
            two_raw, "short_atm_call_2_contracts_on_v2.5"
        ),
        "short_atm_call_3_contracts_on_v2.5": _relabel(
            three_raw, "short_atm_call_3_contracts_on_v2.5"
        ),
    }
    expected_index = paths["original_v2.5_long_only"].index
    if any(not path.index.equals(expected_index) for path in paths.values()):
        raise RuntimeError("Candidate calendars do not match exactly.")

    summary, wide = shortscan._summarize(paths, run_folder)
    wide["final_nav_full"] = wide["candidate"].map(
        {label: float(path["nav"].iloc[-1]) for label, path in paths.items()}
    )
    cost_sensitivity = shortscan._write_cost_sensitivity(paths, run_folder)
    baseline = wide.loc[wide["candidate"].eq("original_v2.5_long_only")].iloc[0]
    candidates = wide.loc[wide["candidate"].str.startswith("short_atm_call")].copy()
    best = candidates.sort_values(["sharpe_repo_full", "ann_return_full"], ascending=False).iloc[0]
    dominates = bool(
        best["ann_return_full"] > baseline["ann_return_full"]
        and best["max_dd_full"] >= baseline["max_dd_full"]
    )
    if dominates:
        decision = "watchlist_best_short_call_on_v2.5"
        stability = "short_call_dominates_baseline"
    else:
        decision = "keep_original_v2.5"
        stability = "short_call_overlay_not_superior"
    wide["decision_hint"] = decision
    wide["stability_label"] = stability
    wide.to_csv(run_folder / "window_metrics.csv", index=False, encoding="utf-8")

    freshness = common._freshness_files(v25_summary)
    data_snapshot = {
        "start": str(common.LAUNCH_DATE.date()),
        "latest": str(latest.date()),
        "rows": len(expected_index),
        "freshness_files": freshness,
        "v2_5_candidate": v25_summary["data_freshness_proof"]["candidate"],
        "historical_rewrite_audit": v25_summary["historical_rewrite_audit"],
    }
    meta = {
        "run_id": run_folder.name,
        "created_at": pd.Timestamp.now(tz="Asia/Shanghai").isoformat(),
        "phase": "analysis_written",
        "project": "microcap_top100",
        "strategy": "v2.5",
        "subsystem": "csi1000_short_atm_call_fixed_contracts",
        "repo_root": str(ROOT),
        "entrypoint": "microcap_top100_mom16_biweekly_live_v2_5.py",
        "research_harness": "scripts/run_microcap_v2_5_short_atm_call_contract_count_scan.py",
        "shared_helper": "scripts/csi1000_short_atm_call_research.py",
        "git_branch": common._git(["branch", "--show-current"]),
        "git_commit": common._git(["rev-parse", "HEAD"]),
        "git_status_before": common._git(["status", "--short"]),
        "git_status_after": common._git(["status", "--short"]),
        "scan_type": "single_parameter",
        "parameter_group": "short_call_contract_count",
        "baseline": {"candidate": "original_v2.5_long_only", "execution_hedge_ratio": 0.0},
        "candidate_grid": list(paths),
        "data_snapshot": data_snapshot,
        "option_data": {key: value for key, value in option_info.items() if key != "manifest"},
        "option_data_manifest_sha256": hashlib.sha256(
            json.dumps(option_info["manifest"], sort_keys=True).encode("utf-8")
        ).hexdigest(),
        "option_data_source": {
            "publisher": "China Financial Futures Exchange (CFFEX)",
            "url_pattern": "http://www.cffex.com.cn/sj/historysj/YYYYMM/zip/YYYYMM.zip",
            "raw_dir": str(raw_dir),
            "fields": ["settlement", "volume", "open_interest", "Delta"],
        },
        "cost_model": {
            "reference_stock_notional_cny": shortscan.REFERENCE_STOCK_NOTIONAL,
            "stock_costs": "official_v2.5_total_cost_unchanged",
            "option_fee_per_contract_per_side_cny": shortscan.OPTION_FEE_PER_CONTRACT_PER_SIDE,
            "option_slippage_points_per_side": shortscan.OPTION_SLIPPAGE_POINTS_PER_SIDE,
            "option_expiry_fee_per_contract_cny": shortscan.OPTION_EXPIRY_FEE_PER_CONTRACT,
            "margin_funding_rate": 0.0,
            "premium_cash_yield": 0.0,
        },
        "execution_assumptions": {
            "direction": "short_call",
            "contract_count_grid": list(shortscan.CONTRACT_COUNTS),
            "entry_selection": "front_month_ATM_call_at_v2.5_stock_entry_or_option_expiry_roll",
            "between_entry_and_exit": "hold_exact_contract_and_fixed_count_no_delta_rebalance",
            "stock_exit": "buy_back_call_same_close",
            "option_expiry": "cash_settle_then_open_next_front_month_ATM_if_v2.5_remains_active",
            "mark_price": "official_daily_settlement",
            "execution_timing": "close",
            "calendar_timezone": "official_v2.5_A-share_close_dates_Asia/Shanghai",
        },
        "baseline_parity": baseline_parity,
        "lifecycle_validation": lifecycle,
        "source_hashes": {
            "official_v2_5_nav_sha256": common._sha256(V25_NAV),
            "official_v2_5_summary_sha256": common._sha256(V25_SUMMARY),
            "research_harness_sha256": common._sha256(Path(__file__)),
            "shared_helper_sha256": common._sha256(Path(shortscan.__file__)),
        },
        "outputs": {
            "record": str(run_folder / "record.md"),
            "scan_summary": str(run_folder / "scan_summary.csv"),
            "window_metrics": str(run_folder / "window_metrics.csv"),
            "cost_sensitivity": str(run_folder / "cost_sensitivity.csv"),
            "scan_meta": str(run_folder / "scan_meta.json"),
            "command_log": str(run_folder / "command_log.txt"),
            "daily_outputs": str(run_folder / "daily_outputs"),
        },
        "decision": decision,
        "stability_label": stability,
        "best_short_call_candidate": str(best["candidate"]),
        "warnings": [
            "The v2.5 microcap series is the refreshed local/public proxy, not Wind 868008.WI.",
            "Short-call margin funding cost and broker-specific margin rules are excluded.",
            "Historical daily CFFEX data lack bid/ask; one-point slippage is imposed on opens and early closes.",
            "10Y and 5Y windows are truncated to post-listing history.",
        ],
        "elapsed_sec": round(time.time() - started, 3),
        "row_counts": {
            "scan_summary": len(summary),
            "window_metrics": len(wide),
            "cost_sensitivity": len(cost_sensitivity),
        },
    }
    common._write_json(run_folder / "scan_meta.json", meta)
    _write_record(run_folder, wide, meta)
    print(wide.to_string(index=False))
    print(f"\nrun_folder={run_folder}")
    print(f"decision={decision}")
    print(f"stability_label={stability}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-folder", type=Path, default=DEFAULT_RUN_FOLDER)
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    args = parser.parse_args()
    run(args.run_folder, args.raw_dir)


if __name__ == "__main__":
    main()
