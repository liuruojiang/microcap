from contextlib import nullcontext
from pathlib import Path

import pandas as pd

import microcap_top100_mom16_biweekly_live_v2_5 as v25


def _write_realtime(path: Path, **overrides: object) -> None:
    row: dict[str, object] = {
        "strategy_revision": v25.STRATEGY_REVISION,
        "lookback": v25.LOOKBACK,
        "halflife": v25.HALFLIFE,
        "entry_threshold": v25.ENTRY_THRESHOLD,
        "exit_threshold": v25.EXIT_THRESHOLD,
        "target_vol_enabled": False,
        "cash_day_yield_enabled": False,
        "financing_enabled": False,
        "signal_spread_hedge_ratio": v25.SIGNAL_SPREAD_HEDGE_RATIO,
        "execution_hedge_ratio": v25.EXECUTION_HEDGE_RATIO,
    }
    row.update(overrides)
    pd.DataFrame([row]).to_csv(path, index=False, encoding="utf-8")


def test_retired_realtime_signal_is_incompatible_even_when_summary_is_current(
    tmp_path: Path,
    monkeypatch,
) -> None:
    summary = tmp_path / "summary.json"
    realtime = tmp_path / "realtime.csv"
    summary.write_text("{}", encoding="utf-8")
    _write_realtime(realtime, strategy_revision="", lookback=17, entry_threshold=0.46, exit_threshold=0.25)
    monkeypatch.setattr(v25, "SUMMARY_JSON", summary)
    monkeypatch.setattr(v25, "REALTIME_SIGNAL_CSV", realtime)
    monkeypatch.setattr(v25, "stale_v2_5_legacy_retest_outputs", lambda: [])
    monkeypatch.setattr(v25, "summary_matches_current_v2_5_base", lambda _summary: True)

    assert not v25.realtime_signal_matches_current_v2_5()
    assert v25.incompatible_v2_5_outputs() == [realtime]
    assert v25._stale_outputs_to_remove_after_generate([realtime], set()) == [realtime]
    monkeypatch.setattr(v25, "v2_5_realtime_output_lock", nullcontext)
    v25._remove_stale_outputs_after_generate([realtime], set())
    assert not realtime.exists()


def test_current_realtime_signal_survives_close_confirmed_cleanup(tmp_path: Path, monkeypatch) -> None:
    realtime = tmp_path / "realtime.csv"
    _write_realtime(realtime)
    monkeypatch.setattr(v25, "REALTIME_SIGNAL_CSV", realtime)
    assert v25.realtime_signal_matches_current_v2_5()
    assert v25._stale_outputs_to_remove_after_generate([realtime], set()) == []


def test_any_core_identity_change_invalidates_realtime_signal(tmp_path: Path) -> None:
    realtime = tmp_path / "realtime.csv"
    _write_realtime(realtime, execution_hedge_ratio=0.8)
    assert not v25.realtime_signal_matches_current_v2_5(realtime)
