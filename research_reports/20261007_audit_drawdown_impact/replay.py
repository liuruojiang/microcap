"""Frozen-history effect of audit repairs; diagnostic, no formal output writes."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as current20
import microcap_top100_mom16_biweekly_live_v2_3 as current23
import microcap_top100_mom16_biweekly_live_v2_5 as current25
from scripts.exchange_calendar import latest_completed_session


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def stack_at(phase: str, directory: Path):
    module20 = load_module(f"impact_{phase}_v2_0", directory / "microcap_top100_mom16_biweekly_live_v2_0.py")
    canonical = "microcap_top100_mom16_biweekly_live_v2_0"
    saved = sys.modules[canonical]
    sys.modules[canonical] = module20
    try:
        module23 = load_module(f"impact_{phase}_v2_3", directory / "microcap_top100_mom16_biweekly_live_v2_3.py")
        module25 = load_module(f"impact_{phase}_v2_5", directory / "microcap_top100_mom16_biweekly_live_v2_5.py")
        assert module23.v2_0._load() is module20
        assert module25.v2_0._load() is module20
    finally:
        sys.modules[canonical] = saved
    return {20: module20, 23: module23, 25: module25}


def compare(a: pd.DataFrame, b: pd.DataFrame) -> dict:
    assert a.index.equals(b.index), "chronological coverage differs"
    result = {}
    for field in ("holding", "next_holding", "signal_on", "return_net", "nav_net",
                  "entry_exit_cost", "rebalance_cost", "total_cost", "current_execution_scale"):
        if pd.api.types.is_numeric_dtype(a[field]) and pd.api.types.is_numeric_dtype(b[field]):
            delta = np.abs(a[field].to_numpy(dtype=float) - b[field].to_numpy(dtype=float))
            assert np.isfinite(delta).all()
            result[field] = float(delta.max())
        else:
            result[field] = int(a[field].ne(b[field]).sum())
    return result


def independent_metrics(returns: pd.Series, years: int | None) -> dict:
    end = returns.index[-1]
    start = returns.index[0] if years is None else end - pd.DateOffset(years=years)
    part = returns.loc[returns.index >= start]
    assert not part.isna().any() and np.isfinite(part).all() and len(part)
    assert returns.index[0] <= start
    nav = np.cumprod(1.0 + part.to_numpy(dtype=float))
    high = np.maximum.accumulate(np.r_[1.0, nav])[1:]
    dd = nav / high - 1.0
    trough = int(np.argmin(dd))
    peak_positions = np.flatnonzero(nav[:trough + 1] == high[trough])
    return {"rows": len(part), "start_date": str(part.index[0].date()), "end_date": str(part.index[-1].date()),
            "annual_pct": float((nav[-1] ** (244.0 / len(part)) - 1.0) * 100.0),
            "max_drawdown_pct": float(dd[trough] * 100.0),
            "max_drawdown_peak_date": str(part.index[peak_positions[-1]].date()) if len(peak_positions) else "initial_capital",
            "max_drawdown_trough_date": str(part.index[trough].date())}


def main():
    proof = json.loads((ROOT / "research_reports/20261007_audit_round3/baseline_and_freshness.json").read_text(encoding="utf-8"))
    target = pd.Timestamp(proof["latest_completed_session"])
    assert latest_completed_session().isoformat() == str(target.date())
    paths = {name: Path(item["path"]) for name, item in proof["artifacts"].items()}
    reads = {}
    for name, path in paths.items():
        meta = proof["artifacts"][name]
        assert sha(path) == meta["raw_sha256"], name
        frame = pd.read_csv(path, encoding="utf-8-sig")
        assert len(frame) == meta["rows"]
        dates = pd.to_datetime(frame[meta["date_field"]], format="mixed", errors="raise")
        assert str(dates.max().date()) == meta["latest_date"]
        reads[name] = frame
    assert current23.v2_0._load() is current20 and current25.v2_0._load() is current20
    phases = {
        "before_any_audit": ROOT / ".codex_backups/20261007_143135",
        "before_recent_two_audits": ROOT / ".codex_backups/20261007_161253",
        "after_second_audit": ROOT / ".codex_backups/20261007_200436",
        "after_third_audit": ROOT,
    }
    first_manifest = json.loads((ROOT / "research_reports/20261007_script_adversarial/final_source_manifest.json").read_text(encoding="utf-8"))
    second_manifest = json.loads((ROOT / "research_reports/20261007_audit_doublecheck/final_source_manifest.json").read_text(encoding="utf-8"))
    third_manifest = json.loads((ROOT / "research_reports/20261007_audit_round3/final_source_manifest.json").read_text(encoding="utf-8"))
    original = json.loads((ROOT / "research_reports/20261007_audit_doublecheck/math_real_independent.json").read_text(encoding="utf-8"))
    sources = {}
    for phase, directory in phases.items():
        sources[phase] = {}
        for version in (20, 23, 25):
            name = f"microcap_top100_mom16_biweekly_live_v2_{version % 10}.py"
            digest = sha(directory / name)
            expected = (original["versions"][f"v2.{version % 10}"]["original_source_sha256"]
                        if phase == "before_any_audit" else
                        {"before_recent_two_audits": first_manifest, "after_second_audit": second_manifest,
                         "after_third_audit": third_manifest}[phase][name]["sha256"])
            assert digest == expected, (phase, name)
            sources[phase][name] = digest
    sources["after_third_audit"]["scripts/top100_data_contracts.py"] = sha(ROOT / "scripts/top100_data_contracts.py")
    assert sources["after_third_audit"]["scripts/top100_data_contracts.py"] == third_manifest["scripts/top100_data_contracts.py"]["sha256"]
    events = reads["turnover"].copy()
    for column in ("rebalance_date", "execution_date", "effective_date", "return_start_date"):
        if column in events:
            events[column] = pd.to_datetime(events[column], format="mixed", errors="raise")
    assert len(events) == 432 and events.execution_timing.eq("close").all()
    official_index = pd.DatetimeIndex(pd.to_datetime(reads["v20_nav"].date))
    outputs, comparisons, metric_rows, curves, event_readbacks = {}, {}, [], [], {}
    windows = (("full", None), ("last_10y", 10), ("last_5y", 5), ("last_3y", 3), ("last_1y", 1))
    input_close = None
    for phase, directory in phases.items():
        modules = {20: current20, 23: current23, 25: current25} if phase == "after_third_audit" else stack_at(phase, directory)
        phase_events = modules[20].base_mod.freq_mod.cost_mod.load_turnover_table(paths["turnover"])
        assert len(phase_events) == len(events)
        assert np.array_equal(phase_events.two_side_cost_rate.to_numpy(), events.two_side_cost_rate.to_numpy())
        phase_dates = pd.to_datetime(phase_events.rebalance_date, format="mixed", errors="raise")
        assert np.array_equal(phase_dates.to_numpy(), events.rebalance_date.to_numpy())
        event_readbacks[phase] = {"loader": "base_mod.freq_mod.cost_mod.load_turnover_table",
                                 "rows": len(phase_events), "execution_timing": "close", "fee_rates_and_signal_dates_match_raw_events": True}
        close_all = modules[20].base_mod.load_close_df(paths["panel"], paths["proxy_index"], max_date=target)
        base_gross = modules[20].base_mod.run_signal(close_all).sort_index()
        close = modules[25]._close_df_from_base(base_gross)
        if input_close is None:
            input_close = close.copy()
        else:
            pd.testing.assert_frame_equal(close, input_close, check_exact=True)
        outputs[phase] = {}
        for version, module in modules.items():
            if version == 20:
                gross = module.base_mod.run_signal(close)
                gross = module.base_mod.apply_momentum_gap_exit_buffer(gross, module.V2_0_MOMENTUM_GAP_EXIT_BUFFER)
                result = module.overlay_mod.apply_v2_0_execution(gross, phase_events)
                perf_module = module.overlay_mod
            else:
                common = (module.build_v2_3_common_index if version == 23 else module.build_v2_5_common_index)(close, official_index)
                result = (module.build_v2_3_result if version == 23 else module.build_v2_5_result)(close, phase_events, common)
                perf_module = module
            assert np.isfinite(result.return_net).all() and result.nav_net.gt(0).all()
            assert np.allclose(result.nav_net, (1.0 + result.return_net).cumprod(), atol=2e-12, rtol=0)
            outputs[phase][version] = result
            native = {row["window"]: row for row in perf_module.summarize_required_windows(result.return_net)}
            for window, years in windows:
                metrics = independent_metrics(result.return_net, years)
                native_dd = float(native[window]["max_drawdown_pct"])
                if phase != "before_any_audit":
                    assert abs(native_dd - metrics["max_drawdown_pct"]) < 1e-12
                metric_rows.append({"phase": phase, "version": f"v2.{version % 10}", "window": window,
                                    **metrics, "native_report_max_drawdown_pct": native_dd})
            curves.append(result[["return_net", "nav_net"]].assign(phase=phase, version=f"v2.{version % 10}").reset_index(names="date"))
    keys = list(phases)
    for before, after in zip(keys, keys[1:]):
        comparisons[f"{before}__to__{after}"] = {
            f"v2.{version % 10}": compare(outputs[before][version], outputs[after][version]) for version in (20, 23, 25)}
    official_comparisons = {}
    for version in (20, 23, 25):
        key = "v20_nav" if version == 20 else f"v{version}_costed_nav"
        official = reads[key].copy()
        official.index = pd.DatetimeIndex(pd.to_datetime(official.pop("date")))
        official.index.name = outputs[keys[-1]][version].index.name
        official_comparisons[f"v2.{version % 10}"] = compare(outputs[keys[-1]][version], official)
    table = pd.DataFrame(metric_rows)
    table.to_csv(OUT / "window_metrics.csv", index=False, encoding="utf-8-sig")
    pd.concat(curves, ignore_index=True).to_csv(OUT / "replayed_net_curves.csv", index=False, encoding="utf-8-sig")
    for name, path in paths.items():
        assert sha(path) == proof["artifacts"][name]["raw_sha256"], name
    for phase, directory in phases.items():
        for name, expected in sources[phase].items():
            assert sha(directory / name) == expected
    metadata = {"scope": "frozen_history_audit_impact_diagnostic_only", "cutoff": str(target.date()),
                "recent_two_audits": ["second_11_groups", "third_18_groups"], "sources": sources,
                "freshness": proof["artifacts"], "formal_refresh": proof["formal_refresh"],
                "turnover_rows_preserved": len(events), "turnover_execution_timing": "close",
                "native_turnover_loader_readbacks": event_readbacks,
                "return_column": "return_net", "annualization_sessions": 244,
                "drawdown_definition": "compound each window from initial capital 1; include initial high-water mark",
                "adjacent_phase_comparisons": comparisons, "current_vs_official_csv": official_comparisons,
                "normal_returns_and_nav_changed": any(v != 0 for group in comparisons.values() for fields in group.values() for v in fields.values()),
                "all_source_and_official_input_hashes_stable": True, "window_metrics": metric_rows,
                "output_sha256": {name: sha(OUT / name) for name in ("window_metrics.csv", "replayed_net_curves.csv", "replay.py")}}
    (OUT / "comparison.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    print(table.loc[table.phase.eq("after_third_audit"), ["version", "window", "annual_pct", "max_drawdown_pct", "max_drawdown_peak_date", "max_drawdown_trough_date"]].to_string(index=False))
    print(json.dumps({"all_real_phase_paths_exactly_equal": not metadata["normal_returns_and_nav_changed"],
                      "metrics_rows": len(metric_rows), "scope": metadata["scope"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
