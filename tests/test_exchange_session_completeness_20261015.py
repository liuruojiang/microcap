"""Missing real sessions must not disappear behind mutually consistent streams."""
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v20
from scripts import exchange_calendar, top100_delivery as delivery
from scripts.top100_data_contracts import validate_exchange_session_coverage
from test_top100_delivery import workspace  # reusable isolated publication fixture


@pytest.fixture
def sessions(monkeypatch):
    days = tuple(date(2026, 9, day) for day in (1, 2, 3, 4))
    monkeypatch.setattr(exchange_calendar, "sessions_for_day", lambda _: days)
    return days


def panel(dates):
    return pd.DataFrame({"date": dates, v20.base_mod.HEDGE_COLUMN: [100.] * len(dates)})


def test_common_missing_session_is_rejected_in_panel_and_continuity(sessions, tmp_path):
    dates = pd.to_datetime(["2026-09-01", "2026-09-03"])
    with pytest.raises(ValueError, match="missing.*2026-09-02"):
        v20.base_mod.validate_panel_exchange_sessions(panel(dates), tmp_path / "panel.csv")
    with pytest.raises(RuntimeError, match="missing.*2026-09-02"):
        v20.overlay_mod.validate_daily_stream_continuity(dates, dates, "shared_gap")


@pytest.mark.parametrize("corrupt_cache", [False, True])
def test_internal_gap_stops_before_fetch_and_preserves_shadow(sessions, tmp_path, monkeypatch, corrupt_cache):
    source, shadow = tmp_path / "panel.csv", tmp_path / "shadow.csv"
    full = panel(["2026-09-01", "2026-09-02", "2026-09-03"])
    gap = full.drop(index=1)
    (full if corrupt_cache else gap).to_csv(source, index=False)
    (gap if corrupt_cache else full).to_csv(shadow, index=False)
    before = shadow.read_bytes()
    ns = v20.base_mod.build_refreshed_panel_shadow.__globals__
    monkeypatch.setitem(ns, "fetch_eastmoney_index_history", lambda *a: pytest.fail("fetch before gap guard"))
    monkeypatch.setitem(ns, "panel_shadow_cache_is_reusable", lambda *a: True)
    with pytest.raises(ValueError, match="2026-09-02"):
        v20.base_mod.build_refreshed_panel_shadow(SimpleNamespace(panel_path=source), {"panel_shadow": shadow})
    assert shadow.read_bytes() == before


def test_provider_tail_gap_is_rejected_before_atomic_write(sessions, tmp_path, monkeypatch):
    source, shadow = tmp_path / "panel.csv", tmp_path / "shadow.csv"
    panel(["2026-09-01"]).to_csv(source, index=False)
    original = source.read_bytes()
    ns = v20.base_mod.build_refreshed_panel_shadow.__globals__
    monkeypatch.setitem(ns, "panel_shadow_cache_is_reusable", lambda *a: False)
    monkeypatch.setitem(ns, "fetch_eastmoney_index_history", lambda *a: pd.DataFrame({
        "date": pd.to_datetime(["2026-09-01", "2026-09-03"]), "close": [100., 102.]}))
    monkeypatch.setitem(ns, "latest_closed_history_date", lambda *a: pd.Timestamp("2026-09-03"))
    with pytest.raises(ValueError, match="2026-09-02"):
        v20.base_mod.build_refreshed_panel_shadow(SimpleNamespace(panel_path=source), {"panel_shadow": shadow})
    assert not shadow.exists()
    assert source.read_bytes() == original


@pytest.mark.parametrize("bad_dates", [
    ["2026-09-01", "2026-09-01", "2026-09-02"],
    ["2026-09-02", "2026-09-01"],
    ["2026-09-01", "2026-09-02T15:00:00"],
    ["2026-09-01", "2026-09-02T00:00:00+08:00"],
    ["2026-09-01", "NaT"],
])
def test_raw_dates_are_rejected_instead_of_cleaned(sessions, tmp_path, monkeypatch, bad_dates):
    source = tmp_path / "panel.csv"
    panel(bad_dates).to_csv(source, index=False)
    monkeypatch.setitem(v20.base_mod.build_refreshed_panel_shadow.__globals__,
                        "fetch_eastmoney_index_history", lambda *a: pytest.fail("fetch before raw validation"))
    with pytest.raises(ValueError):
        v20.base_mod.build_refreshed_panel_shadow(SimpleNamespace(panel_path=source), {"panel_shadow": tmp_path / "out.csv"})
    assert not (tmp_path / "out.csv").exists()


def test_complete_stream_and_legitimate_warmup_start(sessions):
    validate_exchange_session_coverage(["2026-09-02", "2026-09-03"], "warmup", sessions=sessions)
    v20.overlay_mod.validate_daily_stream_continuity(
        pd.to_datetime(["2026-09-02", "2026-09-03"]),
        pd.to_datetime(["2026-09-02", "2026-09-03"]), "warmup")


def test_rebuilt_complete_shadow_passes_without_changing_old_prices(sessions, tmp_path, monkeypatch):
    source, shadow = tmp_path / "panel.csv", tmp_path / "shadow.csv"
    panel(["2026-09-01"]).to_csv(source, index=False)
    ns = v20.base_mod.build_refreshed_panel_shadow.__globals__
    monkeypatch.setitem(ns, "panel_shadow_cache_is_reusable", lambda *a: False)
    monkeypatch.setitem(ns, "fetch_eastmoney_index_history", lambda *a: pd.DataFrame({
        "date": pd.to_datetime(["2026-09-01", "2026-09-02", "2026-09-03"]), "close": [100., 101., 102.]}))
    monkeypatch.setitem(ns, "latest_closed_history_date", lambda *a: pd.Timestamp("2026-09-03"))
    result, target = v20.base_mod.build_refreshed_panel_shadow(SimpleNamespace(panel_path=source), {"panel_shadow": shadow})
    assert target == pd.Timestamp("2026-09-03")
    assert pd.read_csv(result)[v20.base_mod.HEDGE_COLUMN].tolist() == [100., 101., 102.]


def test_final_gate_rejects_common_missing_day_even_with_matching_manifest(workspace, monkeypatch):
    # Fixture's two daily rows are 09/02 and 09/03. Move only the earlier day
    # to 09/01 in every daily stream; all hashes and cross-stream dates agree.
    monkeypatch.setattr(exchange_calendar, "sessions_for_day", lambda _: (
        date(2026, 8, 20), date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3), date(2026, 9, 4)))
    for p in (workspace / "outputs").glob("*.csv"):
        p.write_text(p.read_text(encoding="utf-8").replace("2026-09-02", "2026-09-01"), encoding="utf-8")
    report = delivery.inspect_outputs(workspace, "2026-09-03")
    delivery.write_manifest(workspace, {**report, "status": "complete"})
    checked = delivery.validate_manifest(workspace, report)
    assert not checked["ok"]
    assert any("missing" in error and "2026-09-02" in error for error in checked["errors"])


def test_final_gate_keeps_sparse_turnover_and_single_signal_valid(workspace):
    report = delivery.inspect_outputs(workspace, "2026-09-03")
    assert report["ok"], report["errors"]


def _isolated_freshness_files(tmp_path, monkeypatch, panel_dates, stream_dates):
    """Exercise real CSV parsing/fingerprinting and both public freshness helpers."""
    paths = {}
    for key, values in {
        "base_panel_shadow": panel_dates,
        "base_index_csv": stream_dates,
        "base_costed_nav": stream_dates,
        "base_proxy_turnover": ["2026-09-03"],
    }.items():
        path = tmp_path / f"{key}.csv"
        # Keep blank date cells as actual rows, not parser-skipped empty lines.
        pd.DataFrame({"date": values, "value": [1.] * len(values)}).to_csv(path, index=False)
        paths[key] = path
    files = {key: v20.overlay_mod._file_state_fingerprint(path) for key, path in paths.items()}
    monkeypatch.setitem(v20.overlay_mod.assert_top100_outputs_fresh.__globals__,
                        "current_data_state_fingerprint", lambda: {"files": files})


@pytest.mark.parametrize("bad_source", ["panel", "stream"])
@pytest.mark.parametrize("mutation", ["unordered", "nat"])
def test_output_freshness_cannot_clean_bad_dates_before_continuity(
    sessions, tmp_path, monkeypatch, bad_source, mutation,
):
    full = ["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04"]
    bad = [full[1], full[0], *full[2:]] if mutation == "unordered" else [full[0], None, *full[1:]]
    _isolated_freshness_files(tmp_path, monkeypatch,
                              bad if bad_source == "panel" else full,
                              bad if bad_source == "stream" else full)
    with pytest.raises((ValueError, RuntimeError)):
        v20.overlay_mod.assert_top100_outputs_fresh(
            "2026-09-04", daily_keys=["base_index_csv", "base_costed_nav"],
        )


@pytest.mark.parametrize("mutation", ["unordered", "nat"])
def test_candidate_freshness_cannot_clean_bad_reference_panel(sessions, tmp_path, monkeypatch, mutation):
    full = ["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04"]
    bad = [full[1], full[0], *full[2:]] if mutation == "unordered" else [full[0], None, *full[1:]]
    _isolated_freshness_files(tmp_path, monkeypatch, bad, full)
    with pytest.raises((ValueError, RuntimeError)):
        v20.overlay_mod.assert_top100_candidate_fresh(pd.to_datetime(full), "2026-09-04", "diagnostic")


def test_actual_freshness_helpers_keep_warmup_and_sparse_turnover_valid(sessions, tmp_path, monkeypatch):
    full = ["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04"]
    _isolated_freshness_files(tmp_path, monkeypatch, full, full[1:])
    proof = v20.overlay_mod.assert_top100_outputs_fresh(
        "2026-09-04", daily_keys=["base_index_csv", "base_costed_nav"],
    )
    assert proof["files"]["base_proxy_turnover"]["row_count"] == 1
    candidate = v20.overlay_mod.assert_top100_candidate_fresh(
        pd.to_datetime(full[2:]), "2026-09-04", "diagnostic",
    )
    assert candidate["candidate"]["row_count"] == 2
