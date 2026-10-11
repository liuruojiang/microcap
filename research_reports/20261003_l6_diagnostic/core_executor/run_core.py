"""Isolated L6 v2.3 core one-axis replay on frozen 2026-09-30 inputs."""
from __future__ import annotations

import hashlib
import json
import sys
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd

RUN = Path(__file__).resolve().parent
ROOT = RUN.parents[2]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_3 as v  # noqa: E402


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


@contextmanager
def override(**settings: float):
    keys = ("LOOKBACK", "HALFLIFE", "MOMENTUM_GAP_ENTRY_THRESHOLD", "MOMENTUM_GAP_EXIT_BUFFER", "R2_ENTRY_GATE")
    old = {key: getattr(v, key) for key in keys}
    score_original = v.log_wls_score_and_r2
    weights_original = v.exp_weights

    def weights(lookback=None, halflife=None):
        return weights_original(v.LOOKBACK if lookback is None else lookback,
                                v.HALFLIFE if halflife is None else halflife)

    def score(series, lookback=None, halflife=None, r2_window=None):
        return score_original(series, v.LOOKBACK if lookback is None else lookback,
                              v.HALFLIFE if halflife is None else halflife, r2_window)

    try:
        for key, value in settings.items():
            if key not in old:
                raise ValueError(key)
            setattr(v, key, value)
        v.log_wls_score_and_r2 = score
        v.exp_weights = weights
        yield
    finally:
        for key, value in old.items():
            setattr(v, key, value)
        v.log_wls_score_and_r2 = score_original
        v.exp_weights = weights_original


CASES = [
    ("formal", "formal", {}),
    ("lookback_24", "lookback", {"LOOKBACK": 24}),
    ("lookback_26", "lookback", {"LOOKBACK": 26}),
    ("lookback_27", "lookback", {"LOOKBACK": 27}),
    ("lookback_40", "lookback", {"LOOKBACK": 40}),
    ("halflife_2p25", "halflife", {"HALFLIFE": 2.25}),
    ("halflife_2p75", "halflife", {"HALFLIFE": 2.75}),
    ("halflife_3p00", "halflife", {"HALFLIFE": 3.0}),
    ("entry_m0p01", "entry", {"MOMENTUM_GAP_ENTRY_THRESHOLD": -0.01}),
    ("entry_p0p01", "entry", {"MOMENTUM_GAP_ENTRY_THRESHOLD": 0.01}),
    ("exit_0p07", "exit", {"MOMENTUM_GAP_EXIT_BUFFER": 0.07}),
    ("exit_0p09", "exit", {"MOMENTUM_GAP_EXIT_BUFFER": 0.09}),
    ("exit_0p00", "exit", {"MOMENTUM_GAP_EXIT_BUFFER": 0.0}),
    ("r2_on_0p08", "r2_gate", {"R2_ENTRY_GATE": 0.08}),
]


def main() -> None:
    RUN.mkdir(parents=True, exist_ok=True)
    paths = v.v2_0._resolve_base_paths()
    inputs = [
        ROOT / "microcap_top100_mom16_biweekly_live_v2_0.py",
        ROOT / "microcap_top100_mom16_biweekly_live_v2_3.py",
        Path(paths.output_paths["panel_shadow"]),
        Path(paths.index_csv),
        Path(paths.output_paths["proxy_turnover"]),
        Path(paths.costed_nav_csv),
        Path(v.v2_0.COSTED_NAV_CSV),
        Path(v.COSTED_NAV_CSV),
    ]
    hashes_before = {str(p): sha(p) for p in inputs}
    input_readback = {}
    for path in inputs[2:]:
        date_col = "rebalance_date" if path == Path(paths.output_paths["proxy_turnover"]) else "date"
        dates = pd.to_datetime(pd.read_csv(path, usecols=[date_col])[date_col], errors="raise")
        input_readback[str(path)] = {"rows": len(dates), "first_date": str(dates.min().date()),
                                     "last_date": str(dates.max().date()), "date_column": date_col}
    _, cached, turnover = v.v2_0.embedded_context._load_embedded_base_context()
    close = v._close_df_from_base(cached)
    official20 = pd.read_csv(v.v2_0.COSTED_NAV_CSV, parse_dates=["date"]).set_index("date")
    official = pd.read_csv(v.COSTED_NAV_CSV, parse_dates=["date"]).set_index("date")
    index = v.build_v2_3_common_index(close, official20.index)
    assert index.equals(official.index)
    assert str(index.max().date()) == "2026-09-30"
    assert len(index) == 3988

    daily_dir = RUN / "daily"
    daily_dir.mkdir(exist_ok=True)
    rows = []
    parity = []
    baseline = None
    for candidate, group, settings in CASES:
        with override(**settings):
            assert v.build_v2_3_common_index(close, official20.index).equals(index), candidate
            out = v.build_v2_3_result(close, turnover, index)
        assert out.index.equals(index)
        assert np.isfinite(out.return_net).all()
        assert out.holding.iloc[1:].tolist() == out.next_holding.iloc[:-1].tolist()
        assert out.loc[out.holding.eq("cash") & out.next_holding.eq("cash"), "return_net"].eq(0).all()
        assert out.total_cost.ge(0).all()
        if candidate == "formal":
            baseline = out
            for col in ("return_net", "nav_net", "total_cost", "annualized_log_wls_score", "log_wls_r2", "overheat_feature_value"):
                a = pd.to_numeric(out[col], errors="coerce")
                b = pd.to_numeric(official[col], errors="coerce")
                np.testing.assert_allclose(a, b, atol=1e-12, rtol=0, equal_nan=True)
                parity.append({"column": col, "max_abs_diff": float((a - b).abs().max())})
            for col in ("holding", "next_holding", "base_holding", "base_next_holding", "overheat_risk_off"):
                assert out[col].astype(str).equals(official[col].astype(str)), col
                parity.append({"column": col, "mismatch_rows": 0})
        else:
            assert baseline is not None
            # Each axis changes only its declared source parameter. Untouched exogenous features stay fixed.
            if group in ("entry", "exit", "r2_gate"):
                np.testing.assert_allclose(out.annualized_log_wls_score, baseline.annualized_log_wls_score, atol=1e-12, rtol=0)
                np.testing.assert_allclose(out.overheat_feature_value, baseline.overheat_feature_value, atol=1e-12, rtol=0, equal_nan=True)
            if group == "r2_gate":
                np.testing.assert_allclose(out.log_wls_r2, baseline.log_wls_r2, atol=1e-12, rtol=0)
        out.to_csv(daily_dir / f"{candidate}.csv.gz", index_label="date", compression="gzip")
        back = pd.read_csv(daily_dir / f"{candidate}.csv.gz", parse_dates=["date"]).set_index("date")
        assert back.index.equals(index) and np.isfinite(back.return_net).all()
        for m in v.summarize_required_windows(back.return_net):
            part = back.loc[m["start_date"]:m["end_date"]]
            window_nav = (1.0 + part.return_net.astype(float)).cumprod()
            running_peak = window_nav.cummax().clip(lower=1.0)
            drawdown = window_nav / running_peak - 1.0
            trough_date = drawdown.idxmin()
            peak_slice = window_nav.loc[:trough_date]
            peak_date = peak_slice.idxmax() if peak_slice.max() >= 1.0 else None
            rows.append({
                "candidate": candidate, "parameter_group": group, "setting": json.dumps(settings, sort_keys=True),
                "window": m["window"], "start": m["start_date"], "end": m["end_date"], "days": m["days"],
                "annual_pct": m["annual_pct"], "max_drawdown_pct": m["max_drawdown_pct"],
                "max_drawdown_peak_date": str(peak_date.date()) if peak_date is not None else "window_initial_nav",
                "max_drawdown_trough_date": str(trough_date.date()),
                "ending_nav": float(window_nav.iloc[-1]), "held_days": int(part.holding.ne("cash").sum()),
                "transitions": int(part.holding.ne(part.next_holding).sum()),
                "exit_triggers": int(part.overheat_exit_triggered.sum()),
                "blocked_entries": int(part.overheat_block_entry_triggered.sum()),
                "reentries": int(part.overheat_reentry_triggered.sum()),
                "fee_days": int(part.total_cost.gt(0).sum()), "sum_daily_cost_rates": float(part.total_cost.sum()),
                "held_day_exposure_ratio": float(part.holding.ne("cash").mean()),
                "signal_diff_days_vs_formal": int(part.next_holding.ne(baseline.loc[part.index, "next_holding"]).sum()),
            })
        print(candidate, flush=True)
    assert baseline is not None
    table = pd.DataFrame(rows)
    base = table[table.candidate.eq("formal")].set_index("window")
    table["annual_delta_pp_vs_formal"] = [row.annual_pct - float(base.loc[row.window, "annual_pct"]) for row in table.itertuples()]
    table["drawdown_improvement_pp_vs_formal"] = [row.max_drawdown_pct - float(base.loc[row.window, "max_drawdown_pct"]) for row in table.itertuples()]
    table.to_csv(RUN / "scan_summary.csv", index=False, encoding="utf-8-sig")
    wide = table.pivot(index=["candidate", "parameter_group", "setting"], columns="window", values=["annual_pct", "max_drawdown_pct", "annual_delta_pp_vs_formal", "drawdown_improvement_pp_vs_formal"])
    wide.columns = [f"{a}_{b}" for a, b in wide.columns]
    wide.reset_index().to_csv(RUN / "window_metrics.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(parity).to_csv(RUN / "parity.csv", index=False, encoding="utf-8-sig")
    hashes_after = {str(p): sha(p) for p in inputs}
    assert hashes_before == hashes_after, "source/input changed during scan"
    metadata = {
        "status": "diagnostic_only_frozen_old_inputs", "reason_formal_blocked": "L1/L2 PIT and migration unresolved; L5 shared-cash model stops on funding gaps",
        "date_range": [str(index.min().date()), str(index.max().date())], "rows": len(index),
        "source_and_input_sha256": hashes_before,
        "input_readback": input_readback,
        "formal_tuple": {k: getattr(v, k) for k in ("LOOKBACK", "HALFLIFE", "R2_WINDOW", "R2_ENTRY_GATE", "MOMENTUM_GAP_ENTRY_THRESHOLD", "MOMENTUM_GAP_EXIT_BUFFER", "SIGNAL_SPREAD_HEDGE_RATIO", "EXECUTION_HEDGE_RATIO", "OVERHEAT_FEATURE_WINDOW", "OVERHEAT_TRIGGER_THRESHOLD", "OVERHEAT_RECOVERY_THRESHOLD")},
        "candidate_count": len(CASES), "candidate_grid": [{"candidate": a, "group": b, "settings": c} for a, b, c in CASES],
        "cost_and_execution": "Official old close-model: dated member turnover, 0.3% entry/exit, theoretical 0.8x index short plus 0.0003 daily drag; TV/cash yield/financing OFF. Not common-cash executable account.",
        "natural_off_control": "N/A for continuous WLS lookback/half-life/entry/exit axes; formal tuple is same-batch comparator. R2 OFF is formal and ON 0.08 is historical candidate.",
        "baseline_parity_tolerance": 1e-12,
        "command": "python -X utf8 research_reports/20261003_l6_diagnostic/core_executor/run_core.py",
    }
    (RUN / "meta.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")


if __name__ == "__main__":
    main()
