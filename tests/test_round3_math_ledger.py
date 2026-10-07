"""Third independent diagnostic audit of the actual Top100 execution builders.

All dependency mutations are isolated and artificial.  Current formal artifacts,
authority and caches are read-only.  The approved round-three freshness record is
required even for diagnostic synthetic boundary cases.  No performance promotion.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import sys
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
REPORT = ROOT / "research_reports" / "20261007_audit_round3"
import microcap_top100_mom16_biweekly_live_v2_0 as v20
import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_5 as v25

SOURCES = ["microcap_top100_mom16_biweekly_live.py"] + [
    f"microcap_top100_mom16_biweekly_live_v2_{n}.py" for n in (0, 3, 5)
]
if (ROOT / "scripts" / "top100_data_contracts.py").exists():
    SOURCES.append("scripts/top100_data_contracts.py")
FIELDS = ["holding", "next_holding", "signal_on", "return_net", "nav_net",
          "entry_exit_cost", "rebalance_cost", "total_cost",
          "overlay_pre_cost_return", "current_execution_scale"]


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _record(case: str, **values: object) -> None:
    row = {"scope": "round3_math_diagnostic_only", "case": case,
           "loaded_source_sha256": CONTEXT_SOURCES, **values}
    with (REPORT / "math_faults.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")


CONTEXT_SOURCES = {name: _sha(ROOT / name) for name in SOURCES}


@lru_cache(maxsize=1)
def _authentic() -> dict:
    proof = json.loads((REPORT / "baseline_and_freshness.json").read_text(encoding="utf-8"))
    assert proof["scope"] == "round3_diagnostic_only"
    paths = {key: Path(value["path"]) for key, value in proof["artifacts"].items()}
    frames = {}
    for key, path in paths.items():
        meta = proof["artifacts"][key]
        assert _sha(path) == meta["raw_sha256"], key
        frame = pd.read_csv(path, encoding="utf-8-sig")
        assert len(frame) == meta["rows"], key
        dates = pd.to_datetime(frame[meta["date_field"]], format="mixed", errors="raise")
        assert str(dates.max().date()) == meta["latest_date"], key
        if key != "turnover":
            assert str(dates.max().date()) == proof["latest_completed_session"], key
        frames[key] = frame
    close_all = v20.base_mod.load_close_df(paths["panel"], paths["proxy_index"],
                                         max_date=pd.Timestamp(proof["latest_completed_session"]))
    base_gross = v20.base_mod.run_signal(close_all).sort_index()
    close = v25._close_df_from_base(base_gross)
    events = frames["turnover"].copy()
    for column in ["rebalance_date", "execution_date", "effective_date", "return_start_date"]:
        if column in events:
            events[column] = pd.to_datetime(events[column], format="mixed", errors="raise")
    assert events.execution_timing.eq("close").all()
    official = pd.DatetimeIndex(pd.to_datetime(frames["v20_nav"].date))
    index23 = v23.build_v2_3_common_index(close, official)
    index25 = v25.build_v2_5_common_index(close, official)
    normal23 = v23.build_v2_3_result(close, events, index23)
    normal25 = v25.build_v2_5_result(close, events, index25)
    normal20 = _build_v20(v20, close, events)
    return {"proof": proof, "paths": paths, "frames": frames, "close": close,
            "events": events, "indices": {23: index23, 25: index25},
            "normal": {20: normal20, 23: normal23, 25: normal25}}


def _build_v20(module, close, events):
    gross = module.base_mod.run_signal(close)
    gross = module.base_mod.apply_momentum_gap_exit_buffer(gross, module.V2_0_MOMENTUM_GAP_EXIT_BUFFER)
    return module.overlay_mod.apply_v2_0_execution(gross, events)


@pytest.fixture(scope="session")
def authentic():
    context = _authentic()
    yield context
    after = {key: _sha(path) for key, path in context["paths"].items()}
    expected = {key: row["raw_sha256"] for key, row in context["proof"]["artifacts"].items()}
    assert after == expected, "formal input/output bytes changed during diagnostics"


def _expect_block(case: str, build, normal: pd.DataFrame, **details: object) -> None:
    try:
        out = build()
    except (ValueError, RuntimeError, KeyError) as error:
        _record(case, outcome="rejected", error_type=type(error).__name__, error=str(error), **details)
        return
    common = out.index.intersection(normal.index)
    numeric_change = {column: float((pd.to_numeric(out.loc[common, column]) -
                                     pd.to_numeric(normal.loc[common, column])).abs().max())
                      for column in ("return_net", "total_cost", "nav_net")}
    state_changes = {column: int(out.loc[common, column].ne(normal.loc[common, column]).sum())
                     for column in ("holding", "next_holding")}
    _record(case, outcome="accepted_wrong_dependency", rows=len(out), normal_rows=len(normal),
            date_index_matches=out.index.equals(normal.index),
            net_formula_self_consistent=bool(np.allclose(out.return_net,
                (1 + out.overlay_pre_cost_return) * (1 - out.total_cost) - 1, atol=1e-12, rtol=1e-10)),
            nav_self_consistent=bool(np.allclose(out.nav_net, (1 + out.return_net).cumprod(), atol=1e-12, rtol=1e-10)),
            max_absolute_changes=numeric_change, state_changes=state_changes,
            end_nav_ratio=float(out.nav_net.iloc[-1] / normal.nav_net.iloc[-1]), **details)
    pytest.fail(f"{case}: actual execution builder accepted the corrupted upstream result")


@pytest.mark.parametrize("component", ["entry_exit_cost", "rebalance_cost"])
def test_v25_self_consistent_fee_erasure_is_rejected_in_actual_build(authentic, monkeypatch, component):
    costmod = v25.v2_0.base_mod.freq_mod.cost_mod
    original = costmod.apply_cost_model
    baseline = authentic["normal"][25]
    date = baseline.index[baseline[component].gt(0)][0]
    original_rate = float(baseline.at[date, component])

    def erase(gross, events):
        out = original(gross, events)
        out.at[date, component] = 0.0
        out["total_cost"] = out.entry_exit_cost + out.rebalance_cost
        out["return_net"] = (1 + out["return"]) * (1 - out.total_cost) - 1
        out["nav_net"] = (1 + out.return_net).cumprod()
        return out

    monkeypatch.setattr(costmod, "apply_cost_model", erase)
    _expect_block(f"v25_erase_{component}",
        lambda: v25.build_v2_5_result(authentic["close"], authentic["events"], authentic["indices"][25]),
        baseline, mutated_date=str(date.date()), erased_fee=original_rate)


@pytest.mark.parametrize("attack", ["erase_fee", "shift_fee"])
def test_v23_mapped_fee_event_contract_is_checked_in_actual_build(authentic, monkeypatch, attack):
    costmod = v23.v2_0.base_mod.freq_mod.cost_mod
    original = costmod.map_rebalance_apply_costs
    baseline = authentic["normal"][23]
    date = baseline.index[baseline.rebalance_cost.gt(0)][0]
    rate = float(baseline.at[date, "rebalance_cost"])
    position = baseline.index.get_loc(date)

    def corrupt(index, events):
        fees = original(index, events)
        fees.at[date] = 0.0
        if attack == "shift_fee":
            fees.iloc[position + 1] += rate
        return fees

    monkeypatch.setattr(costmod, "map_rebalance_apply_costs", corrupt)
    _expect_block(f"v23_{attack}",
        lambda: v23.build_v2_3_result(authentic["close"], authentic["events"], authentic["indices"][23]),
        baseline, mutated_date=str(date.date()), displaced_fee=rate)


def test_v20_mapped_fee_cannot_be_erased_in_actual_fixed_exposure_dispatch(authentic, monkeypatch):
    costmod = v20.base_mod.freq_mod.cost_mod
    original = costmod.map_rebalance_apply_costs
    baseline = authentic["normal"][20]
    date = baseline.index[baseline.rebalance_cost.gt(0)][0]
    rate = float(baseline.at[date, "rebalance_cost"])

    def corrupt(index, events):
        fees = original(index, events)
        fees.at[date] = 0.0
        return fees

    monkeypatch.setattr(costmod, "map_rebalance_apply_costs", corrupt)
    _expect_block("v20_erase_mapped_rebalance_fee",
        lambda: _build_v20(v20, authentic["close"], authentic["events"]), baseline,
        mutated_date=str(date.date()), erased_fee=rate)


@pytest.mark.parametrize("constant", ["ENTRY_COST", "EXIT_COST"])
def test_v23_base_fee_contract_cannot_certify_changed_approved_cost_constants(authentic, monkeypatch, constant):
    costmod = v23.v2_0.base_mod.freq_mod.cost_mod
    monkeypatch.setattr(costmod, constant, 0.0)
    try:
        v23.validate_v2_0_contract()
    except (ValueError, RuntimeError) as error:
        _record(f"v23_cost_constant_{constant}", outcome="rejected", error=str(error))
        return
    _expect_block(f"v23_cost_constant_{constant}",
        lambda: v23.build_v2_3_result(authentic["close"], authentic["events"], authentic["indices"][23]),
        authentic["normal"][23], approved_constant=constant, changed_to=0.0)


@pytest.mark.parametrize("column", ["holding", "next_holding", "signal_on"])
def test_v25_cost_dependency_cannot_change_execution_state(authentic, monkeypatch, column):
    costmod = v25.v2_0.base_mod.freq_mod.cost_mod
    original = costmod.apply_cost_model
    baseline = authentic["normal"][25]
    # A genuine cash day makes changing holding to active bypass the cash-return
    # leakage check while contradicting the actual source gross state.
    date = baseline.index[baseline.holding.eq("cash") & baseline.next_holding.eq("cash")][0]
    value = True if column == "signal_on" else "long_microcap_top100"

    def corrupt(gross, events):
        out = original(gross, events)
        out.at[date, column] = value
        return out

    monkeypatch.setattr(costmod, "apply_cost_model", corrupt)
    _expect_block(f"v25_cost_changes_{column}",
        lambda: v25.build_v2_5_result(authentic["close"], authentic["events"], authentic["indices"][25]),
        baseline, mutated_date=str(date.date()))


@pytest.mark.parametrize("bad", [np.nan, 0.0, -1.0, np.inf, "damaged"])
def test_v23_corrupt_spread_nav_cannot_disable_defense_in_actual_build(authentic, monkeypatch, bad):
    original = v23.build_spread_log_wls_gross
    baseline = authentic["normal"][23]
    date = baseline.index[baseline.overheat_exit_triggered][0]

    def corrupt(close, index=None):
        out = original(close, index)
        out["spread_nav"] = out.spread_nav.astype(object)
        out.at[date, "spread_nav"] = bad
        return out

    monkeypatch.setattr(v23, "build_spread_log_wls_gross", corrupt)
    _expect_block("v23_bad_spread_nav_" + str(bad),
        lambda: v23.build_v2_3_result(authentic["close"], authentic["events"], authentic["indices"][23]),
        baseline, mutated_date=str(date.date()), bad_value=str(bad))


@pytest.mark.parametrize("bad", [np.nan, "damaged", "long_microcap_top100"])
def test_v23_foreign_or_missing_next_state_cannot_be_silently_executed(authentic, monkeypatch, bad):
    original = v23.build_spread_log_wls_gross
    baseline = authentic["normal"][23]
    date = baseline.index[baseline.holding.ne("cash") & baseline.next_holding.eq("cash") &
                          baseline.annualized_log_wls_score.lt(-v23.MOMENTUM_GAP_EXIT_BUFFER)][0]

    def corrupt(close, index=None):
        out = original(close, index)
        out.at[date, "next_holding"] = bad
        return out

    monkeypatch.setattr(v23, "build_spread_log_wls_gross", corrupt)
    _expect_block("v23_bad_next_state_" + str(bad),
        lambda: v23.build_v2_3_result(authentic["close"], authentic["events"], authentic["indices"][23]),
        baseline, mutated_date=str(date.date()), bad_value=str(bad))


@pytest.mark.parametrize("version", [23, 25])
def test_actual_gross_dependency_cannot_omit_a_non_event_active_session(authentic, monkeypatch, version):
    module = v23 if version == 23 else v25
    gross_name = "build_spread_log_wls_gross" if version == 23 else "build_microcap_log_wls_gross"
    original = getattr(module, gross_name)
    build = module.build_v2_3_result if version == 23 else module.build_v2_5_result
    baseline = authentic["normal"][version]
    event_dates = pd.DatetimeIndex(authentic["events"].execution_date)
    valid = baseline.index[baseline.holding.ne("cash") & baseline.return_net.abs().gt(1e-4)]
    date = valid.difference(event_dates)[10]

    def corrupt(close, index=None):
        return original(close, index).drop(index=date)

    monkeypatch.setattr(module, gross_name, corrupt)
    _expect_block(f"v{version}_gross_missing_active_session",
        lambda: build(authentic["close"], authentic["events"], authentic["indices"][version]),
        baseline, omitted_date=str(date.date()), omitted_net_return=float(baseline.at[date, "return_net"]))


@pytest.mark.parametrize("version", [23, 25])
def test_official_common_index_cannot_silently_drop_leading_valid_sessions(authentic, version):
    module = v23 if version == 23 else v25
    get_index = module.build_v2_3_common_index if version == 23 else module.build_v2_5_common_index
    build = module.build_v2_3_result if version == 23 else module.build_v2_5_result
    damaged_official = authentic["indices"][version][1:]
    _expect_block(f"v{version}_missing_leading_official_session",
        lambda: build(authentic["close"], authentic["events"], get_index(authentic["close"], damaged_official)),
        authentic["normal"][version], omitted_date=str(authentic["indices"][version][0].date()))


def test_v23_exact_hot_cool_thresholds_keep_lag_and_legal_cash_fees(authentic, monkeypatch):
    dates = pd.bdate_range("2025-01-02", periods=5)
    active = "long_microcap_short_zz1000"
    gross = pd.DataFrame({"holding": ["cash", active, active, active, active],
        "next_holding": active, "return": [0.0, 0.01, 0.9, 0.9, 0.01], "spread_nav": 1.0}, index=dates)
    feature = pd.Series([0.1, 0.26, 0.20000001, 0.20, 0.1], index=dates)
    monkeypatch.setattr(v23, "_overheat_feature_series", lambda _: feature)
    out = v23.apply_overheat_defense(gross, pd.DataFrame())
    assert out.holding.ne("cash").tolist() == [False, True, False, False, True]
    assert out.next_holding.ne("cash").tolist() == [True, False, False, True, True]
    assert out.overheat_exit_triggered.tolist() == [False, True, False, False, False]
    assert out.overheat_reentry_triggered.tolist() == [False, False, False, True, False]
    assert out.overlay_pre_cost_return.tolist() == [0.0, 0.01, 0.0, 0.0, 0.01]
    assert out.entry_exit_cost.tolist() == [0.003, 0.003, 0.0, 0.003, 0.0]
    assert out.loc[dates[[0, 3]], "return_net"].eq((1 - .003) - 1).all()


@pytest.mark.parametrize("timing", ["close", "next_open"])
def test_v25_normal_cost_dependency_keeps_legitimate_cash_entry_exit_fees(authentic, timing):
    prices = authentic["close"].iloc[-320:].copy()
    # Explicit events use real dates and authentic event rates; alternate timing
    # is diagnostic, never a promoted historical execution assumption.
    events = authentic["events"].loc[authentic["events"].execution_date.ge(prices.index[30])].copy()
    events["execution_timing"] = timing
    gross = v25.build_microcap_log_wls_gross(prices)
    out = v25.apply_no_target_vol(v25.apply_cost(gross, events))
    active = gross.holding.ne("cash").to_numpy()
    after = gross.next_holding.ne("cash").to_numpy() if timing == "close" else active
    before = active if timing == "close" else np.r_[False, active[:-1]]
    expected_transition = np.where(before != after, .003, 0.0)
    np.testing.assert_array_equal(out.entry_exit_cost.to_numpy(), expected_transition)
    assert (out.holding.eq("cash") & out.total_cost.gt(0)).any()
    assert out.loc[out.holding.eq("cash"), "overlay_pre_cost_return"].eq(0).all()


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_frozen_stack():
    proof = _authentic()["proof"]
    backup = Path(proof["old_source_backup"])
    old20 = _load_module("round3_math_before_v2_0", backup / "microcap_top100_mom16_biweekly_live_v2_0.py")
    normal_name = "microcap_top100_mom16_biweekly_live_v2_0"
    saved = sys.modules[normal_name]
    sys.modules[normal_name] = old20
    try:
        old23 = _load_module("round3_math_before_v2_3", backup / "microcap_top100_mom16_biweekly_live_v2_3.py")
        old25 = _load_module("round3_math_before_v2_5", backup / "microcap_top100_mom16_biweekly_live_v2_5.py")
        assert old23.v2_0._load() is old20
        assert old25.v2_0._load() is old20
    finally:
        sys.modules[normal_name] = saved
    return {20: old20, 23: old23, 25: old25}


def _compare(left, right, *, tolerance=2e-12):
    assert left.index.equals(right.index), "whole chronological index changed"
    result = {}
    for field in FIELDS:
        a, b = left[field], right[field]
        if pd.api.types.is_numeric_dtype(a) and pd.api.types.is_numeric_dtype(b):
            difference = np.abs(a.to_numpy(dtype=float) - b.to_numpy(dtype=float))
            assert np.isfinite(difference).all(), field
            maximum = float(difference.max())
            assert maximum <= tolerance, (field, maximum, tolerance)
            result[field] = maximum
        else:
            changed = int(a.ne(b).sum())
            assert changed == 0, (field, changed)
            result[field] = changed
    return result


def real_audit():
    context = _authentic()
    old = _load_frozen_stack()
    # Reuse the separately implemented pairwise WLS and independent state/fee
    # ledger as a computation, not its previous PASS or saved result table.
    reference = _load_module("round3_math_independent_reference",
                             ROOT / "tests" / "test_doublecheck_math_adversarial.py")
    result = {"scope": "round3_math_diagnostic_only", "latest_completed_session":
              context["proof"]["latest_completed_session"], "formal_refresh": context["proof"]["formal_refresh"],
              "loaded_source_sha256": CONTEXT_SOURCES, "versions": {}}
    for version, module in [(20, v20), (23, v23), (25, v25)]:
        current = context["normal"][version]
        if version == 20:
            previous = _build_v20(old[version], context["close"], context["events"])
            ledger = reference._independent_v20_ledger(context["close"], current.index, context["events"])
        else:
            old_build = old[version].build_v2_3_result if version == 23 else old[version].build_v2_5_result
            previous = old_build(context["close"], context["events"], context["indices"][version])
            ledger, scores = reference._independent_ledger(module, context["close"], current.index, context["events"])
            assert np.sign(current.annualized_log_wls_score).eq(np.sign(scores.annualized_log_wls_score)).all()
        official_key = "v20_nav" if version == 20 else f"v{version}_costed_nav"
        official = context["frames"][official_key].copy()
        official.index = pd.DatetimeIndex(pd.to_datetime(official.pop("date")))
        official.index.name = current.index.name
        cash = current.holding.eq("cash")
        no_cost = (1 + current.overlay_pre_cost_return).cumprod()
        assert (current.nav_net <= no_cost + 3e-12).all()
        assert current.loc[cash, "overlay_pre_cost_return"].eq(0).all()
        result["versions"][f"v2.{version % 10}"] = {
            "rows": len(current), "first_date": str(current.index[0].date()),
            "latest_date": str(current.index[-1].date()),
            "same_round_frozen_entire_stack": _compare(current, previous, tolerance=0),
            "independent_price_state_fee_ledger": _compare(current, ledger),
            "official_saved_csv": _compare(current, official),
            "cash_with_legal_trade_fee_days": int((cash & current.total_cost.gt(0)).sum()),
            "cash_pre_cost_zero": True, "costed_not_above_same_state_no_cost": True}
    gross23 = v23.build_spread_log_wls_gross(context["close"], context["indices"][23])
    signal_nav, *_ = v23.always_on_spread_nav(context["close"])
    full_history_feature = signal_nav.pct_change(fill_method=None).rolling(v23.OVERHEAT_FEATURE_WINDOW).std(ddof=1) * math.sqrt(v23.TRADING_DAYS)
    existing_feature = v23._overheat_feature_series(gross23)
    first_rows = gross23.index[:v23.OVERHEAT_FEATURE_WINDOW]
    warm_notes = [{"date": str(date.date()), "full_history_vol10": float(full_history_feature.at[date]),
                   "formal_truncated_vol10": None if pd.isna(existing_feature.at[date]) else float(existing_feature.at[date]),
                   "full_history_is_hot": bool(full_history_feature.at[date] >= v23.OVERHEAT_TRIGGER_THRESHOLD),
                   "formal_next_active": bool(context["normal"][23].at[date, "next_holding"] != "cash")}
                  for date in first_rows]
    result["warm_start_observation"] = {"scope": "definition_note_not_a_parameter_or_history_change",
        "formal_rule_uses_gross_start_as_feature_start": True, "first_rows": warm_notes,
        "reason_not_promoted_to_repair": "Changing warm-start volatility changes approved semantics; current diagnostic checks preserve normal paths."}
    after = {key: _sha(path) for key, path in context["paths"].items()}
    assert after == {key: row["raw_sha256"] for key, row in context["proof"]["artifacts"].items()}
    result["official_artifacts_unchanged"] = True
    result["sources_after"] = {name: _sha(ROOT / name) for name in SOURCES}
    assert result["sources_after"] == CONTEXT_SOURCES, "source changed mid-audit; rerun on a stable stack"
    result["test_sha256"] = _sha(Path(__file__))
    (REPORT / "math_real.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"scope": result["scope"], "result": "PASS", "versions": {
        version: {"rows": data["rows"], "same_round_frozen_max_difference": max(data["same_round_frozen_entire_stack"].values()),
                  "independent_ledger_max_difference": max(data["independent_price_state_fee_ledger"].values()),
                  "official_saved_max_difference": max(data["official_saved_csv"].values())}
        for version, data in result["versions"].items()}}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    if "--real" in sys.argv:
        real_audit()
    else:
        raise SystemExit("Run with pytest or --real; all mutations remain diagnostic-only.")
