"""Version-specific L8 paper replays for Top100 v2.0 and v2.5.

Only the current strategy functions calculate signals, costs and returns. This
runner stores diagnostic streams in quant_param_scan_runs; it does not publish.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as v20  # noqa: E402
import microcap_top100_mom16_biweekly_live_v2_5 as v25  # noqa: E402
from scripts import top100_delivery as delivery  # noqa: E402

RUNS = {
    "v20": ROOT / "quant_param_scan_runs/20261003_v20_l8_integrated_adversarial",
    "v25": ROOT / "quant_param_scan_runs/20261003_v25_l8_integrated_adversarial",
}
CASES = {
    "v20": [
        ("formal", "formal_full_on", {}),
        ("signal_hedge_off", "group_off", {"signal_hedge": 0.0}),
        ("execution_hedge_off", "group_off", {"execution_hedge": 0.0}),
        ("both_hedges_off", "group_off_pair", {"signal_hedge": 0.0, "execution_hedge": 0.0}),
        ("lookback15", "local_neighborhood", {"lookback": 15}),
        ("lookback17", "local_neighborhood", {"lookback": 17}),
        ("signal_hedge08", "local_neighborhood", {"signal_hedge": 0.8}),
        ("signal_hedge12", "local_neighborhood", {"signal_hedge": 1.2}),
        ("execution_hedge07", "local_neighborhood", {"execution_hedge": 0.7}),
        ("execution_hedge09", "local_neighborhood", {"execution_hedge": 0.9}),
        ("exit_buffer001", "one_sided_neighborhood", {"exit_buffer": 0.001}),
        ("exit_buffer003", "old_research_observation", {"exit_buffer": 0.003}),
        ("positive_micro_mom_on", "disabled_gate_activation", {"positive_micro_mom": True}),
        ("pair_lb17_execution_off", "pair_interaction", {"lookback": 17, "execution_hedge": 0.0}),
        ("pair_lb17_buffer003", "pair_old_research_observation", {"lookback": 17, "exit_buffer": 0.003}),
        ("pair_execution_off_buffer003", "pair_interaction", {"execution_hedge": 0.0, "exit_buffer": 0.003}),
        ("pair_signal_off_buffer003", "pair_interaction", {"signal_hedge": 0.0, "exit_buffer": 0.003}),
        ("pair_both_off_buffer003", "pair_interaction", {"signal_hedge": 0.0, "execution_hedge": 0.0, "exit_buffer": 0.003}),
    ],
    "v25": [
        ("formal", "formal_full_on", {}),
        ("signal_filter_off_always_on", "core_off_reference", {"entry": -1e9, "exit": -1e9}),
        ("lookback19", "local_neighborhood", {"lookback": 19}),
        ("lookback21", "local_neighborhood", {"lookback": 21}),
        ("halflife2p5", "local_neighborhood", {"halflife": 2.5}),
        ("halflife3p5", "local_neighborhood", {"halflife": 3.5}),
        ("entry_m001", "local_neighborhood", {"entry": -0.01}),
        ("entry_p001", "local_neighborhood", {"entry": 0.01}),
        ("exit_m001", "local_neighborhood", {"exit": -0.01}),
        ("exit_p001", "local_neighborhood", {"exit": 0.01}),
        ("old_entry004", "rejected_old_watchlist", {"entry": 0.04}),
        ("old_lookback40", "old_research_observation", {"lookback": 40}),
        ("old_exit003", "old_research_observation", {"exit": 0.03}),
        ("old_retired_tuple", "retired_old_tuple", {"lookback": 17, "halflife": 3.0, "entry": 0.46, "exit": 0.25}),
        ("pair_lb21_halflife3p5", "pair_interaction", {"lookback": 21, "halflife": 3.5}),
        ("pair_entry001_exit001", "pair_interaction", {"entry": 0.01, "exit": 0.01}),
        ("pair_lb40_exit003", "pair_old_research_observation", {"lookback": 40, "exit": 0.03}),
        ("pair_lb21_entry001", "pair_interaction", {"lookback": 21, "entry": 0.01}),
    ],
}


def sha(path: Path, *, text_file: bool = False) -> str:
    data = path.read_bytes()
    if text_file:
        data = data.replace(b"\r\n", b"\n")
    return hashlib.sha256(data).hexdigest()


def input_hashes() -> dict[str, str]:
    paths = v20._resolve_base_paths()
    inputs = [
        Path(__file__),
        ROOT / "microcap_top100_mom16_biweekly_live_v2_0.py",
        ROOT / "microcap_top100_mom16_biweekly_live_v2_5.py",
        Path(paths.output_paths["panel_shadow"]),
        Path(paths.index_csv),
        Path(paths.output_paths["proxy_turnover"]),
        Path(paths.costed_nav_csv),
        Path(v20.COSTED_NAV_CSV),
        Path(v25.COSTED_NAV_CSV),
        ROOT / delivery.AUTHORITY,
    ]
    return {str(p.relative_to(ROOT) if p.is_relative_to(ROOT) else p): sha(p, text_file=True) for p in inputs}


def frozen_cases(version: str) -> list[dict]:
    return [{"candidate": n, "group": g, "settings": s} for n, g, s in CASES[version]]


@contextmanager
def override_v20(settings: dict):
    base = v20.embedded_context.base_mod
    embedded = v20.embedded_context
    overlay = v20.overlay_mod
    old = {
        "lookback": base.LOOKBACK,
        "execution_hedge": base.FIXED_HEDGE_RATIO,
        "positive_micro_mom": base.REQUIRE_POSITIVE_MICROCAP_MOM,
        "embedded_hedge": embedded.BASE_HEDGE_RATIO,
        "overlay_hedge": overlay.BASE_HEDGE_RATIO,
    }
    try:
        base.LOOKBACK = settings.get("lookback", old["lookback"])
        base.FIXED_HEDGE_RATIO = settings.get("execution_hedge", old["execution_hedge"])
        base.REQUIRE_POSITIVE_MICROCAP_MOM = settings.get("positive_micro_mom", old["positive_micro_mom"])
        embedded.BASE_HEDGE_RATIO = base.FIXED_HEDGE_RATIO
        overlay.BASE_HEDGE_RATIO = base.FIXED_HEDGE_RATIO
        yield
    finally:
        base.LOOKBACK = old["lookback"]
        base.FIXED_HEDGE_RATIO = old["execution_hedge"]
        base.REQUIRE_POSITIVE_MICROCAP_MOM = old["positive_micro_mom"]
        embedded.BASE_HEDGE_RATIO = old["embedded_hedge"]
        overlay.BASE_HEDGE_RATIO = old["overlay_hedge"]


@contextmanager
def override_v25(settings: dict):
    old = {key: getattr(v25, key) for key in ("LOOKBACK", "HALFLIFE", "ENTRY_THRESHOLD", "EXIT_THRESHOLD")}
    score_original = v25.log_wls_score_and_r2
    weights_original = v25.exp_weights

    def weights(lookback=None, halflife=None):
        return weights_original(v25.LOOKBACK if lookback is None else lookback,
                                v25.HALFLIFE if halflife is None else halflife)

    def score(series, lookback=None, halflife=None):
        return score_original(series, v25.LOOKBACK if lookback is None else lookback,
                              v25.HALFLIFE if halflife is None else halflife)

    try:
        v25.LOOKBACK = settings.get("lookback", old["LOOKBACK"])
        v25.HALFLIFE = settings.get("halflife", old["HALFLIFE"])
        v25.ENTRY_THRESHOLD = settings.get("entry", old["ENTRY_THRESHOLD"])
        v25.EXIT_THRESHOLD = settings.get("exit", old["EXIT_THRESHOLD"])
        v25.exp_weights = weights
        v25.log_wls_score_and_r2 = score
        yield
    finally:
        for key, value in old.items():
            setattr(v25, key, value)
        v25.exp_weights = weights_original
        v25.log_wls_score_and_r2 = score_original


def build_v20(close: pd.DataFrame, turnover: pd.DataFrame, settings: dict) -> pd.DataFrame:
    with override_v20(settings):
        base = v20.embedded_context.base_mod
        gross = base.run_signal(close).sort_index()
        # The original Mom16 signal has a fixed one-for-one index comparison.
        # Rebuild only its signal axis here; the real state/cost functions follow.
        signal_ratio = float(settings.get("signal_hedge", 1.0))
        gross = gross.copy()
        gross["momentum_gap"] = gross["microcap_mom"] - signal_ratio * gross["hedge_mom"]
        gross = base.apply_momentum_gap_exit_buffer(gross, settings.get("exit_buffer", 0.0))
        return v20.overlay_mod.apply_v2_0_execution(gross, turnover)


def build_v25(close: pd.DataFrame, turnover: pd.DataFrame, index: pd.DatetimeIndex, settings: dict) -> pd.DataFrame:
    with override_v25(settings):
        return v25.build_v2_5_result(close, turnover, index)


def freeze(version: str) -> None:
    run = RUNS[version]
    proof = json.loads((run / "preflight_delivery_check.json").read_text(encoding="utf-8"))
    assert proof["ok"] and proof["scope"] == "whole_workspace_delivery"
    assert proof["expected_date"] == "2026-09-30"
    assert proof["release_sha"] == subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    cases = frozen_cases(version)
    assert len(cases) == len({x["candidate"] for x in cases})
    grid_path = run / "cases.json"
    if grid_path.exists():
        assert json.loads(grid_path.read_text(encoding="utf-8")) == cases
    else:
        grid_path.write_text(json.dumps(cases, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    meta_path = run / "scan_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta.update({
        "scan_type": "candidate_bundle",
        "baseline": ({"candidate": "formal", "lookback": 16, "signal_hedge": 1.0, "execution_hedge": 0.8,
                      "exit_buffer": 0.0, "overheat": "OFF", "target_vol": "OFF"} if version == "v20" else
                     {"candidate": "formal", "lookback": 20, "halflife": 3.0, "entry": 0.0, "exit": 0.0,
                      "signal_hedge": 0.0, "execution_hedge": 0.0, "overheat": "OFF", "target_vol": "OFF"}),
        "candidate_grid": cases,
        "data_snapshot": {"scope": "frozen_public_local_proxy_paper", "date": "2026-09-30",
                          "preflight": str((run / "preflight_delivery_check.json").relative_to(ROOT)),
                          "source_and_input_sha256": input_hashes()},
        "cost_model": {"entry_exit_each_side": 0.003, "rebalance": "dated proxy member turnover",
                       "signal_clock": "T close confirmed", "model_cost_clock": "same T close",
                       "new_holding_return_clock": "T+1 full session", "cash_yield": "OFF",
                       "financing": "OFF", "target_vol": "OFF", "execution": "conditional paper model"},
        "decision_target": "keep_default_or_watchlist_no_production_promotion",
        "source_change_rule": "research_only_no_source_change",
    })
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    with (run / "command_log.txt").open("a", encoding="utf-8") as f:
        f.write(f"python -X utf8 research_reports/20261003_l8_diagnostic/run_l8_v20_v25.py {version} --freeze\n")
    print(json.dumps({"version": version, "frozen_cases": len(cases), "input_files": len(meta["data_snapshot"]["source_and_input_sha256"])}, ensure_ascii=False))


def peak_trough(ret: pd.Series) -> tuple[str, str]:
    nav = (1.0 + ret).cumprod()
    trough = (nav / nav.cummax().clip(lower=1.0) - 1.0).idxmin()
    pre = nav.loc[:trough]
    return (str(pre.idxmax().date()) if pre.max() >= 1.0 else "initial_equity", str(trough.date()))


def run(version: str) -> None:
    run_dir = RUNS[version]
    cases = json.loads((run_dir / "cases.json").read_text(encoding="utf-8"))
    assert cases == frozen_cases(version)
    meta_path = run_dir / "scan_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    before = input_hashes()
    assert before == meta["data_snapshot"]["source_and_input_sha256"]
    _, cached, turnover = v20.embedded_context._load_embedded_base_context()
    close = cached[["microcap_close", "hedge_close"]].rename(columns={"microcap_close": "microcap", "hedge_close": "hedge"})
    official = pd.read_csv(v20.COSTED_NAV_CSV if version == "v20" else v25.COSTED_NAV_CSV,
                           parse_dates=["date"]).set_index("date")
    assert str(official.index[-1].date()) == "2026-09-30"
    if version == "v25":
        assert v25.build_v2_5_common_index(close, official.index).equals(official.index)
    paths: dict[str, pd.DataFrame] = {}
    full_parity: dict[str, float | int] = {}
    for case in cases:
        name, settings = case["candidate"], case["settings"]
        out = (build_v20(close, turnover, settings) if version == "v20"
               else build_v25(close, turnover, official.index, settings))
        assert out.index.is_monotonic_increasing and not out.index.has_duplicates
        assert str(out.index[-1].date()) == "2026-09-30"
        assert np.isfinite(out.return_net).all() and out.total_cost.ge(0).all()
        assert out.holding.iloc[1:].tolist() == out.next_holding.iloc[:-1].tolist()
        if name == "formal":
            assert out.index.equals(official.index)
            for col in ("return_net", "nav_net", "total_cost", "entry_exit_cost", "rebalance_cost", "overlay_pre_cost_return"):
                a, b = pd.to_numeric(out[col], errors="coerce"), pd.to_numeric(official[col], errors="coerce")
                np.testing.assert_allclose(a, b, atol=1e-12, rtol=0, equal_nan=True)
                full_parity[col] = float((a - b).abs().max())
            for col in ("holding", "next_holding"):
                full_parity[col] = int(out[col].astype(str).ne(official[col].astype(str)).sum())
                assert full_parity[col] == 0
        paths[name] = out
        print(f"L8 {version} {name}", flush=True)
    common = official.index
    for out in paths.values():
        common = common.intersection(out.index)
    assert len(common) >= 3900 and str(common[-1].date()) == "2026-09-30"
    assert common.equals(official.loc[common[0]:].index), "candidate added internal missing dates"
    baseline = paths["formal"].loc[common]
    assert input_hashes() == before
    daily_dir = run_dir / "daily"
    daily_dir.mkdir(exist_ok=True)
    long_rows: list[dict] = []
    wide_rows: list[dict] = []
    daily_files: dict[str, dict] = {}
    summarizer = v20.overlay_mod.summarize_required_windows if version == "v20" else v25.summarize_required_windows
    for case in cases:
        name = case["candidate"]
        out = paths[name].loc[common].copy()
        out["nav_net"] = (1.0 + out.return_net).cumprod()
        if "nav" in out:
            out["nav"] = out.nav_net
        assert out.holding.iloc[1:].tolist() == out.next_holding.iloc[:-1].tolist()
        assert out.loc[out.holding.eq("cash") & out.next_holding.eq("cash"), "return_net"].eq(0).all()
        if version == "v20" and name == "execution_hedge_off":
            assert out.futures_drag.eq(0).all()
        if version == "v25":
            assert out.futures_drag.eq(0).all()
        path = daily_dir / f"{name}.csv.gz"
        out.to_csv(path, index_label="date", compression={"method": "gzip", "mtime": 0})
        daily_files[name] = {"path": str(path.relative_to(ROOT)), "sha256": sha(path), "rows": len(out),
                             "start": str(common[0].date()), "end": str(common[-1].date())}
        base_cols = {"candidate": name, "group": case["group"], "settings": json.dumps(case["settings"], sort_keys=True)}
        wide = base_cols.copy()
        for m in summarizer(out.return_net):
            assert not m["unavailable_reason"]
            part = out.loc[m["start_date"]:m["end_date"]]
            peak, trough = peak_trough(part.return_net)
            held = int(part.holding.ne("cash").sum())
            cost = float(part.total_cost.sum())
            hedge_ratio = float(case["settings"].get("execution_hedge", 0.8)) if version == "v20" else 0.0
            record = {**base_cols, "segment": m["window"], "start": m["start_date"], "end": m["end_date"],
                      "rows": m["days"], "ann_return": m["annual_pct"] / 100.0,
                      "ann_vol": m["vol_pct"] / 100.0, "sharpe_repo": m["sharpe"],
                      "max_dd": m["max_drawdown_pct"] / 100.0,
                      "peak_date": peak, "trough_date": trough, "final_nav": m["final_nav"],
                      "holding_days": held, "transition_days": int(part.holding.ne(part.next_holding).sum()),
                      "fee_days": int(part.total_cost.gt(0).sum()), "cost_rate_sum": cost,
                      "theoretical_hedge_notional_days": held * hedge_ratio,
                      "signal_diff_days_vs_formal": int(part.next_holding.ne(baseline.loc[part.index, "next_holding"]).sum()),
                      "return_diff_days_vs_formal": int((part.return_net - baseline.loc[part.index, "return_net"]).abs().gt(1e-12).sum())}
            long_rows.append(record)
            for col in ("ann_return", "max_dd", "sharpe_repo"):
                wide[f"{col}_{m['window']}"] = record[col]
            if m["window"] == "full":
                for col in ("holding_days", "transition_days", "fee_days", "cost_rate_sum", "theoretical_hedge_notional_days"):
                    wide[col] = record[col]
        wide_rows.append(wide)
    assert input_hashes() == before
    pd.DataFrame(long_rows).to_csv(run_dir / "scan_summary.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(wide_rows).to_csv(run_dir / "window_metrics.csv", index=False, encoding="utf-8-sig")
    meta.update({"full_official_parity": full_parity, "common_window": [str(common[0].date()), str(common[-1].date())],
                 "common_rows": len(common), "official_rows": len(official), "daily_outputs": daily_files,
                 "candidate_count": len(cases), "formal_claim": "none_diagnostic_only_L1_L2_L5_limits_accepted_for_research"})
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    with (run_dir / "command_log.txt").open("a", encoding="utf-8") as f:
        f.write(f"python -X utf8 research_reports/20261003_l8_diagnostic/run_l8_v20_v25.py {version} --run\n")
    print(json.dumps({"version": version, "candidates": len(cases), "common_rows": len(common),
                      "max_full_official_numeric_diff": max(float(v) for v in full_parity.values())}, ensure_ascii=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("version", choices=tuple(RUNS))
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--freeze", action="store_true")
    action.add_argument("--run", action="store_true")
    args = parser.parse_args()
    freeze(args.version) if args.freeze else run(args.version)
