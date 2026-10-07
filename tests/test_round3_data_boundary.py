"""Round-three diagnostic boundaries, using real strategy parsers and consumers.

Fixtures live only under the explicitly supplied task-local pytest basetemp.
No formal output, cache, authority, source migration, or market metric is written.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest


def _load_strategy(name):
    root = os.environ.get("MICROCAP_ROUND3_SOURCE_ROOT")
    if not root:
        return __import__(name)
    spec = importlib.util.spec_from_file_location(f"_round3_data_{name}", Path(root) / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    # Runtime compatibility wrappers import the canonical module name. Keep
    # those imports on this same frozen source in the diagnostic process.
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


v20 = _load_strategy("microcap_top100_mom16_biweekly_live_v2_0")
mainline = _load_strategy("microcap_top100_mom16_biweekly_live")
v23 = _load_strategy("microcap_top100_mom16_biweekly_live_v2_3")
v25 = _load_strategy("microcap_top100_mom16_biweekly_live_v2_5")


@pytest.fixture(params=[v20.base_mod, mainline], ids=["shared_v20", "mainline"])
def base(request):
    return request.param


@pytest.mark.parametrize("reverse", [False, True], ids=["date_then_midnight", "midnight_then_date"])
def test_daily_parser_preserves_legal_mixed_midnight_text(base, reverse):
    dates = ["2026-09-01", "2026-09-02 00:00:00"]
    if reverse:
        dates = ["2026-09-01 00:00:00", "2026-09-02"]
    frame = pd.DataFrame({"date": dates, "value": [1.0, 2.0]})
    parsed = base._normalise_dated_frame(frame, "round3 diagnostic mixed legal date labels")
    assert parsed.index.equals(pd.bdate_range("2026-09-01", periods=2))
    assert parsed["value"].tolist() == [1.0, 2.0]


@pytest.mark.parametrize("mixed_source", ["panel", "proxy"])
def test_daily_close_loader_preserves_every_legal_mixed_format_row(base, tmp_path, mixed_source):
    dates = pd.bdate_range("2026-08-03", periods=40)
    panel = pd.DataFrame({"date": dates.strftime("%Y-%m-%d"), base.HEDGE_COLUMN: np.arange(40) + 100.0})
    proxy = pd.DataFrame({"date": dates.strftime("%Y-%m-%d"), "close": np.arange(40) + 200.0,
                          "holding_effective": True})
    changed = panel if mixed_source == "panel" else proxy
    changed.loc[39, "date"] += " 00:00:00"
    panel_path, proxy_path = tmp_path / "panel.csv", tmp_path / "proxy.csv"
    panel.to_csv(panel_path, index=False)
    proxy.to_csv(proxy_path, index=False)
    parsed = base.load_close_df(panel_path, proxy_path)
    assert parsed.index.equals(dates)
    assert len(parsed) == 40


def test_freeze_accepts_same_daily_rows_with_legal_date_text_variations(base):
    previous = pd.DataFrame({"date": ["2026-09-01", "2026-09-02"], "nav_net": [1.0, 1.01]})
    candidate = previous.copy()
    candidate.loc[1, "date"] += " 00:00:00"
    base.assert_no_historical_rewrite(previous, candidate, ["nav_net"], 0, "round3 diagnostic unchanged daily values")


@pytest.mark.parametrize("before,after", [(np.nan, None), (None, pd.NA), (pd.NA, np.nan)])
def test_freeze_preserves_true_missing_when_all_warmup_cells_change_null_representation(base, before, after):
    dates = pd.bdate_range("2026-09-01", periods=2)
    previous = pd.DataFrame({"date": dates, "warmup": pd.Series([before, before], dtype=object)})
    candidate = pd.DataFrame({"date": dates, "warmup": pd.Series([after, after], dtype=object)})
    assert previous["warmup"].isna().all() and candidate["warmup"].isna().all()
    base.assert_no_historical_rewrite(previous, candidate, ["warmup"], 0, "round3 diagnostic unchanged all-warmup nulls")


@pytest.mark.parametrize("reverse", [False, True])
def test_turnover_loader_preserves_legal_mixed_rebalance_text(tmp_path, reverse):
    dates = ["2026-09-01", "2026-09-02 00:00:00"]
    if reverse:
        dates = ["2026-09-01 00:00:00", "2026-09-02"]
    path = tmp_path / "turnover.csv"
    pd.DataFrame({"rebalance_date": dates, "execution_date": dates,
                  "execution_timing": "close", "two_side_cost_rate": [0.001, 0.002]}).to_csv(path, index=False)
    event = v20.cost_mod.load_turnover_table(path)
    cost = v20.cost_mod.map_rebalance_apply_costs(pd.bdate_range("2026-09-01", periods=3), event)
    assert len(event) == 2
    assert cost.tolist() == pytest.approx([0.001, 0.002, 0.0])


def _embedded_inputs(tmp_path, monkeypatch, events):
    """Run the real embedded-context loader; only refresh/locks/reference I/O are isolated."""
    dates = pd.bdate_range("2026-08-03", periods=40)
    panel_path, proxy_path, costed_path = (tmp_path / name for name in ("panel.csv", "proxy.csv", "costed.csv"))
    turnover_path = tmp_path / "turnover.csv"
    pd.DataFrame({"date": dates, v20.base_mod.HEDGE_COLUMN: np.arange(40) + 100.0}).to_csv(panel_path, index=False)
    pd.DataFrame({"date": dates, "close": np.arange(40) * 4.0 + 200.0,
                  "holding_effective": True}).to_csv(proxy_path, index=False)
    pd.DataFrame({"date": dates, "return_net": 0.0, "nav_net": 1.0}).to_csv(costed_path, index=False)
    events.to_csv(turnover_path, index=False)
    loader = v20.embedded_context._load_embedded_base_context
    namespace = loader.__globals__
    monkeypatch.setitem(namespace, "_v2_base_build_lock", contextlib.nullcontext)
    monkeypatch.setitem(namespace, "realtime_state_required", lambda: True)
    monkeypatch.setitem(namespace, "_ensure_base_outputs_unlocked", lambda **kwargs: None)
    monkeypatch.setitem(namespace, "_build_base_args", lambda: argparse.Namespace(index_csv=proxy_path))
    monkeypatch.setitem(namespace, "_resolve_base_paths", lambda args: SimpleNamespace(
        output_paths={"panel_shadow": panel_path, "proxy_turnover": turnover_path}, costed_nav_csv=costed_path))
    monkeypatch.setitem(namespace, "_load_reference_summary_unlocked", lambda *args, **kwargs: {"diagnostic_only": True})
    return loader, dates


def test_actual_embedded_loader_does_not_silently_drop_legal_midnight_event(tmp_path, monkeypatch):
    events = pd.DataFrame({"rebalance_date": ["2026-09-01", "2026-09-02 00:00:00"],
                           "execution_date": ["2026-09-01", "2026-09-02 00:00:00"],
                           "execution_timing": "close", "two_side_cost_rate": [0.001, 0.002]})
    loader, dates = _embedded_inputs(tmp_path, monkeypatch, events)
    _, gross, consumed = loader()
    assert len(consumed) == 2, "A valid midnight event disappeared before the cost mapper"
    costs = v20.cost_mod.map_rebalance_apply_costs(gross.index, consumed)
    assert costs.loc["2026-09-01"] == pytest.approx(0.001)
    assert costs.loc["2026-09-02"] == pytest.approx(0.002)
    costed = v20.cost_mod.apply_cost_model(gross, consumed)
    assert costed.loc["2026-09-02", "holding"] != "cash"
    assert costed.loc["2026-09-02", "rebalance_cost"] == pytest.approx(0.002)


@pytest.mark.parametrize("rebalance_date,cost", [(None, 0.002), ("NaT", 0.002),
                                                  ("invalid-date", 0.002), (None, np.inf)])
def test_actual_embedded_loader_never_discards_bad_event_before_validation(tmp_path, monkeypatch, rebalance_date, cost):
    events = pd.DataFrame({"rebalance_date": ["2026-09-01", rebalance_date],
                           "execution_date": ["2026-09-01", "2026-09-02"],
                           "execution_timing": "close", "two_side_cost_rate": [0.001, cost]})
    loader, _ = _embedded_inputs(tmp_path, monkeypatch, events)
    with pytest.raises(ValueError, match="date|cost|event|turnover"):
        _, gross, consumed = loader()
        v20.cost_mod.map_rebalance_apply_costs(gross.index, consumed)


@pytest.mark.parametrize("second_signal", ["2026-09-02 00:00:00", None], ids=["legal_mixed", "missing_signal"])
def test_actual_base_nav_rebuild_preserves_event_or_rejects_bad_event(base, tmp_path, second_signal):
    dates = pd.bdate_range("2026-08-03", periods=40)
    panel_path, proxy_path, turnover_path, nav_path = (tmp_path / name for name in
        ["panel.csv", "proxy.csv", "turnover.csv", "diagnostic_costed_nav.csv"])
    pd.DataFrame({"date": dates, base.HEDGE_COLUMN: np.arange(40) + 100.0}).to_csv(panel_path, index=False)
    pd.DataFrame({"date": dates, "close": np.arange(40) * 4.0 + 200.0,
                  "holding_effective": True}).to_csv(proxy_path, index=False)
    pd.DataFrame({"rebalance_date": ["2026-09-01", second_signal],
                  "execution_date": ["2026-09-01", "2026-09-02"],
                  "execution_timing": "close", "two_side_cost_rate": [0.001, 0.002]}).to_csv(turnover_path, index=False)
    args = argparse.Namespace(index_csv=proxy_path, costed_nav_csv=nav_path)
    if second_signal is None:
        with pytest.raises(ValueError, match="date|event|turnover"):
            base.rebuild_costed_nav_from_proxy_turnover(args, {"proxy_turnover": turnover_path}, panel_path)
        assert not nav_path.exists(), "A malformed event must fail before any diagnostic NAV is written"
    else:
        base.rebuild_costed_nav_from_proxy_turnover(args, {"proxy_turnover": turnover_path}, panel_path)
        read_back = pd.read_csv(nav_path).set_index("date")
        assert read_back.loc["2026-09-02", "holding"] != "cash"
        assert read_back.loc["2026-09-02", "rebalance_cost"] == pytest.approx(0.002)


@pytest.mark.parametrize("route", ["mapper", "loader", "embedded"])
def test_repeated_identical_turnover_event_is_not_double_charged(tmp_path, monkeypatch, route):
    events = pd.DataFrame({"rebalance_date": ["2026-09-01"] * 2,
                           "execution_date": ["2026-09-01"] * 2,
                           "execution_timing": "close", "two_side_cost_rate": [0.006] * 2})
    with pytest.raises(ValueError, match="duplicate|repeat|event|rebalance"):
        if route == "embedded":
            loader, _ = _embedded_inputs(tmp_path, monkeypatch, events)
            _, gross, consumed = loader()
            v20.cost_mod.map_rebalance_apply_costs(gross.index, consumed)
        elif route == "loader":
            path = tmp_path / "duplicate_turnover.csv"
            events.to_csv(path, index=False)
            consumed = v20.cost_mod.load_turnover_table(path)
            v20.cost_mod.map_rebalance_apply_costs(pd.bdate_range("2026-09-01", periods=3), consumed)
        else:
            v20.cost_mod.map_rebalance_apply_costs(pd.bdate_range("2026-09-01", periods=3), events)


def test_explicit_execution_cannot_hide_non_session_signal_inside_window():
    dates = pd.to_datetime(["2026-09-04", "2026-09-07", "2026-09-08"])
    events = pd.DataFrame({"rebalance_date": ["2026-09-05"], "execution_date": ["2026-09-07"],
                           "execution_timing": ["next_open"], "two_side_cost_rate": [0.006]})
    with pytest.raises(ValueError, match="date|session|rebalance"):
        v20.cost_mod.map_rebalance_apply_costs(dates, events)


def test_strategy_cost_consumer_rejects_mixed_execution_timing_in_one_stream():
    dates = pd.bdate_range("2026-09-01", periods=3)
    gross = pd.DataFrame({"holding": ["cash", "microcap", "microcap"],
                          "next_holding": ["microcap", "microcap", "cash"], "return": 0.0}, index=dates)
    events = pd.DataFrame({"rebalance_date": [dates[0], dates[1]],
                           "execution_date": [dates[0], dates[2]],
                           "execution_timing": ["close", "next_open"], "two_side_cost_rate": [0.006, 0.006]})
    with pytest.raises(ValueError, match="timing|mixed|event|stream"):
        v20.cost_mod.apply_cost_model(gross, events)


@pytest.mark.parametrize("timing", ["close", "next_open"])
def test_legal_uniform_timing_preserves_entry_exit_cost_semantics(timing):
    dates = pd.bdate_range("2026-09-01", periods=3)
    gross = pd.DataFrame({"holding": ["cash", "microcap", "microcap"],
                          "next_holding": ["microcap", "microcap", "cash"], "return": 0.0}, index=dates)
    events = pd.DataFrame({"rebalance_date": [dates[0]], "execution_timing": [timing], "two_side_cost_rate": [0.006]})
    costed = v20.cost_mod.apply_cost_model(gross, events)
    entry = float(v20.cost_mod.ENTRY_COST)
    exit_cost = float(v20.cost_mod.EXIT_COST)
    expected = [entry, 0.0, exit_cost] if timing == "close" else [0.0, entry, 0.0]
    assert costed["entry_exit_cost"].tolist() == pytest.approx(expected)


def test_candidate_frame_hash_preserves_legal_mixed_date_canonicalization():
    mixed = pd.DataFrame({"date": ["2026-09-01", "2026-09-02 00:00:00", "2026-09-04"],
                          "return_net": [0.01, 0.02, 0.03]})
    canonical = mixed.copy()
    canonical["date"] = pd.to_datetime(canonical["date"], format="mixed")
    assert v20.overlay_mod._candidate_frame_sha256(mixed) == v20.overlay_mod._candidate_frame_sha256(canonical)


def test_candidate_frame_hash_binds_each_intermediate_mixed_format_session():
    original = pd.DataFrame({"date": ["2026-09-01", "2026-09-02 00:00:00", "2026-09-04"],
                             "return_net": [0.01, 0.02, 0.03]})
    changed = original.copy()
    changed.loc[1, "date"] = "2026-09-03 00:00:00"
    assert v20.overlay_mod._candidate_frame_sha256(original) != v20.overlay_mod._candidate_frame_sha256(changed)


@pytest.fixture(params=["2.0", "2.3", "2.5"])
def migration_case(request, tmp_path, monkeypatch):
    version = request.param
    module = {"2.0": v20.overlay_mod, "2.3": v23, "2.5": v25}[version]
    if version != "2.0":
        monkeypatch.setattr(module, "v2_0", v20)
    previous = pd.DataFrame({"date": pd.to_datetime(["2026-09-01"]), "return_net": [0.0]})
    candidate = pd.DataFrame({"date": pd.to_datetime(["2026-09-01", "2026-09-02"]), "return_net": [0.01, 0.02]})
    previous_path, base_path = tmp_path / "previous.csv", tmp_path / "base_v20.csv"
    meta_path, audit_path, report_path = tmp_path / "meta.json", tmp_path / "audit.csv", tmp_path / "report.json"
    previous.to_csv(previous_path, index=False)
    previous.to_csv(base_path, index=False)
    meta_path.write_text('{"diagnostic_only": true}', encoding="utf-8")
    audit_path.write_text("date,column,change_type,previous,candidate\n2026-09-01,return_net,value_changed,0,0.01\n", encoding="utf-8")
    monkeypatch.setitem(v20.overlay_mod.v2_0_rewrite_audit_matches_approved_lineage_migration.__globals__,
                        "COSTED_NAV_CSV", previous_path)
    monkeypatch.setitem(v20.overlay_mod.v2_0_rewrite_audit_matches_approved_lineage_migration.__globals__,
                        "_resolve_base_paths", lambda: SimpleNamespace(output_paths={"proxy_meta": meta_path}))
    monkeypatch.setattr(v20, "COSTED_NAV_CSV", base_path)
    monkeypatch.setattr(v20, "_resolve_base_paths", lambda: SimpleNamespace(output_paths={"proxy_meta": meta_path}))
    if version != "2.0":
        monkeypatch.setattr(module, "COSTED_NAV_CSV", previous_path)
    digest = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
    report = {"schema_version": 1, "approved": True,
              "previous_costed_nav_sha256": digest(previous_path),
              "candidate_frame_sha256": v20.overlay_mod._candidate_frame_sha256(candidate),
              "base_proxy_meta_sha256": digest(meta_path), "rewrite_audit_sha256": digest(audit_path),
              "previous_row_count": 1, "candidate_row_count": 2,
              "previous_latest_date": "2026-09-01", "candidate_latest_date": "2026-09-02",
              "new_member_st_violations": 0, "new_member_bad_policy_count": 0, "proxy_meta_matches_current_cache": True}
    if version != "2.0":
        report.update(version=version, v2_0_costed_nav_sha256=digest(base_path))
    verifier = getattr(module, f"v2_{version[-1]}_rewrite_audit_matches_approved_lineage_migration")
    return verifier, report_path, report, previous, candidate, audit_path


def test_lineage_migration_requires_actual_boolean_approval(migration_case):
    verifier, report_path, report, previous, candidate, audit_path = migration_case
    report["approved"] = 1
    report_path.write_text(json.dumps(report), encoding="utf-8")
    assert verifier(report_path, previous, candidate, audit_path) is False


def test_lineage_migration_preserves_exact_boolean_approved_control(migration_case):
    verifier, report_path, report, previous, candidate, audit_path = migration_case
    report_path.write_text(json.dumps(report), encoding="utf-8")
    assert verifier(report_path, previous, candidate, audit_path) is True


@pytest.mark.parametrize("hash_field", ["previous_costed_nav_sha256", "candidate_frame_sha256",
                                        "base_proxy_meta_sha256", "rewrite_audit_sha256"])
def test_lineage_migration_still_rejects_each_changed_exact_hash(migration_case, hash_field):
    verifier, report_path, report, previous, candidate, audit_path = migration_case
    report[hash_field] = "0" * 64
    report_path.write_text(json.dumps(report), encoding="utf-8")
    assert verifier(report_path, previous, candidate, audit_path) is False


def test_lineage_migration_binds_legal_intermediate_mixed_format_session(migration_case):
    verifier, report_path, report, previous, _, audit_path = migration_case
    candidate = pd.DataFrame({"date": ["2026-09-01", "2026-09-02 00:00:00", "2026-09-04"],
                              "return_net": [0.01, 0.02, 0.03]})
    report.update(candidate_frame_sha256=v20.overlay_mod._candidate_frame_sha256(candidate),
                  candidate_row_count=3, candidate_latest_date="2026-09-04")
    report_path.write_text(json.dumps(report), encoding="utf-8")
    assert verifier(report_path, previous, candidate, audit_path) is True
    changed = candidate.copy()
    changed.loc[1, "date"] = "2026-09-03 00:00:00"
    assert verifier(report_path, previous, changed, audit_path) is False
