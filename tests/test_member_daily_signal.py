"""Offline deterministic fixtures only; these are not current market evidence."""
from datetime import datetime
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scripts import member_daily_signal as daily


@pytest.fixture
def bundle(tmp_path):
    root = tmp_path / "seed"
    root.mkdir()
    x = np.arange(400)
    index = pd.bdate_range("2020-01-01", periods=400)
    frame = pd.DataFrame({"microcap": 1000 * np.exp(.0008*x + .03*np.sin(x/19)),
                          "hedge": 1000 * np.exp(.0002*x + .02*np.cos(x/23))}, index=index)
    day = (index[-1] + pd.offsets.BDay()).date().isoformat()
    anchor = index[-1].date().isoformat()
    symbols = [str(600000 + i) for i in range(100)]
    period = dict(schema="member_period_v1", anchor_day=anchor, effective_from=anchor,
                  source_manifest_sha256="f"*64, source_files={"offline_fixture": "f"*64},
                  effective_members=[dict(rank=i+1, symbol=s, target_weight=.01) for i, s in enumerate(symbols)])
    daily.write_json(root / "period.json", period)
    frame.rename_axis("date").to_csv(root / "history.csv")
    turnover = pd.DataFrame(columns=["rebalance_date", "two_side_cost_rate"])
    turnover.to_csv(root / "turnover.csv", index=False)
    states = {v: daily.clean(daily.calculate_native(v, m, frame, turnover)[0].iloc[0].to_dict())
              for v, m in daily.native_modules().items()}
    daily.write_json(root / "version_state.json", {"schema": "member_version_state_v1", "versions": states})
    daily.write_json(root / "member_prices.json", dict(schema="member_price_basis_v1", as_of_date=anchor,
                     rows=[dict(symbol=s, previous_raw_close=10., previous_raw_date=anchor) for s in symbols]))
    cp = dict(schema=daily.SCHEMA, as_of_date=anchor, next_trade_date=day)
    for key, name in [("period", "period.json"), ("history", "history.csv"), ("turnover", "turnover.csv"),
                      ("version_state", "version_state.json"), ("prices", "member_prices.json")]:
        cp[key] = dict(path=name, sha256=daily.digest((root / name).read_bytes()))
    daily.write_json(root / "checkpoint.json", cp)
    quotes = dict(market_date=day, collected_at=day+"T16:00:00+08:00", close_confirmed=True,
                  rows=[dict(symbol=s, trade_date=day, previous_date=anchor, close_adj=10.1,
                             previous_close_adj=10., previous_raw_close=10., raw_close=10.1, return_basis_revision=daily.RETURN_BASIS,
                             source="offline deterministic fixture") for s in symbols])
    hedge = dict(market_date=day, collected_at=quotes["collected_at"], close_confirmed=True,
                 close=float(frame.hedge.iloc[-1])*1.002, source="offline deterministic fixture")
    return root, frame, quotes, hedge, day, list(index.date) + [pd.Timestamp(day).date(), (pd.Timestamp(day)+pd.offsets.BDay()).date()]


def run(bundle, tmp_path, hedge=True):
    root, frame, quotes, h, day, sessions = bundle
    return daily.build(Path(__file__).resolve().parents[1], root / "checkpoint.json", quotes,
                       h if hedge else None, day, "close_confirmed", tmp_path / "result",
                       now=datetime.fromisoformat(day+"T16:01:00+08:00"), calendar_sessions=sessions)


def test_native_three_version_parity_and_checkpoint_prices(bundle, tmp_path):
    root, history, quotes, hedge, day, _ = bundle
    before = daily.digest((root / "checkpoint.json").read_bytes())
    result = run(bundle, tmp_path)
    assert result["status"] == "OK"
    appended = history.copy()
    appended.loc[pd.Timestamp(day)] = [history.microcap.iloc[-1]*1.01, hedge["close"]]
    turnover = pd.read_csv(root / "turnover.csv")
    for entry in result["versions"]:
        module = daily.native_modules()[entry["version"]]
        native, _ = daily.calculate_native(entry["version"], module, appended, turnover)
        for key in ("current_holding", "next_holding", "strategy_revision", "next_session_target_scale"):
            assert entry["signal"][key] == native.iloc[0][key]
    cp = json.loads((tmp_path / "result/checkpoint.json").read_bytes())
    prices = json.loads((tmp_path / "result/member_prices.json").read_bytes())
    assert cp["next_trade_date"] > day
    assert prices["as_of_date"] == day and prices["rows"][0]["previous_raw_close"] == 10.1
    assert daily.digest((root / "checkpoint.json").read_bytes()) == before
    from scripts.member_period_handoff import verify
    verify(tmp_path / "result/checkpoint.json")


def test_hedge_failure_keeps_v25_and_does_not_advance_shared_checkpoint(bundle, tmp_path):
    result = run(bundle, tmp_path, hedge=False)
    assert [row["status"] for row in result["versions"]] == ["partial", "partial", "verified"]
    assert result["checkpoint"] is None


@pytest.mark.parametrize("case", ["missing", "duplicate", "wrong_date", "wrong_basis"])
def test_member_input_failure_is_explicit_partial(bundle, tmp_path, case):
    rows = bundle[2]["rows"]
    if case == "missing": rows.pop()
    if case == "duplicate": rows[0] = dict(rows[1])
    if case == "wrong_date": rows[0]["trade_date"] = "1999-01-01"
    if case == "wrong_basis": rows[0]["return_basis_revision"] = "raw_close_fallback"
    result = run(bundle, tmp_path)
    assert result["status"] == "PARTIAL" and result["checkpoint"] is None
    assert all(row["signal"] is None and row["reason"] for row in result["versions"])
    if case == "missing":
        assert result["quote_coverage"] == "99/100" and result["observed_member_count"] == 99
        assert len(result["missing_member_symbols"]) == 1


def test_hashed_input_tampering_is_rejected(bundle, tmp_path):
    (bundle[0] / "history.csv").write_text("changed", encoding="utf8")
    with pytest.raises(ValueError, match="hash mismatch"):
        run(bundle, tmp_path)


def test_confirmed_state_mismatch_does_not_silently_restart(bundle, tmp_path):
    root = bundle[0]
    states = json.loads((root / "version_state.json").read_bytes())
    states["versions"]["v2.3"]["next_holding"] = "unmatched_state"
    daily.write_json(root / "version_state.json", states)
    cp = json.loads((root / "checkpoint.json").read_bytes())
    cp["version_state"]["sha256"] = daily.digest((root / "version_state.json").read_bytes())
    daily.write_json(root / "checkpoint.json", cp)
    result = run(bundle, tmp_path)
    assert result["versions"][1]["status"] == "partial"
    assert "model-state mismatch" in result["versions"][1]["reason"]
    assert result["versions"][0]["status"] == "verified"


def test_stale_period_labels_old_basket_reference(bundle, tmp_path):
    root = bundle[0]
    period = json.loads((root / "period.json").read_bytes())
    period["anchor_day"] = "2020-01-01"
    daily.write_json(root / "period.json", period)
    cp = json.loads((root / "checkpoint.json").read_bytes())
    cp["period"]["sha256"] = daily.digest((root / "period.json").read_bytes())
    daily.write_json(root / "checkpoint.json", cp)
    result = run(bundle, tmp_path)
    assert result["period_status"] == "overdue_reference"
    assert all(row["status"] == "reference" and row["signal"] for row in result["versions"])
    assert result["checkpoint"] == "checkpoint.json"
    cp = json.loads((tmp_path / "result/checkpoint.json").read_bytes())
    assert cp["period_status"] == "awaiting_handoff"


def test_raw_price_basis_mismatch_does_not_advance(bundle, tmp_path):
    bundle[2]["rows"][0]["previous_raw_close"] = 9.5
    result = run(bundle, tmp_path)
    assert all(row["status"] == "partial" and "prior checkpoint raw price" in row["reason"] for row in result["versions"])


def test_close_collection_after_midnight_retains_true_market_date(bundle, tmp_path):
    root, _, quotes, hedge, day, sessions = bundle
    next_day = (pd.Timestamp(day) + pd.Timedelta(days=1)).date().isoformat()
    quotes["collected_at"] = next_day + "T00:01:00+08:00"
    hedge["collected_at"] = quotes["collected_at"]
    result = daily.build(Path(__file__).resolve().parents[1], root / "checkpoint.json", quotes, hedge, day,
                         "close_confirmed", tmp_path / "late", now=datetime.fromisoformat(next_day+"T00:02:00+08:00"),
                         calendar_sessions=sessions)
    assert result["status"] == "OK" and result["market_date"] == day


def test_cli_imports_calendar_after_inserting_selected_root(bundle, tmp_path, monkeypatch):
    # Exercise the real build entry order; a missing repo sys.path used to fail
    # before component-level handling when invoked as python scripts/foo.py.
    from scripts import exchange_calendar
    monkeypatch.setattr(exchange_calendar, "sessions_for_day", lambda day: bundle[-1])
    root, _, quotes, hedge, day, _ = bundle
    result = daily.build(Path(__file__).resolve().parents[1], root / "checkpoint.json", quotes, hedge, day,
                         "close_confirmed", tmp_path / "cli", now=datetime.fromisoformat(day+"T16:01:00+08:00"))
    assert result["status"] == "OK"


def test_two_confirmed_days_resume_and_verify_same_checkpoint_contract(bundle, tmp_path):
    from scripts.member_period_handoff import verify
    root, _, quotes, hedge, day, sessions = bundle
    first = run(bundle, tmp_path)
    assert first["status"] == "OK"
    cp = tmp_path / "result/checkpoint.json"
    verify(cp)
    next_day = sessions[-1].isoformat()
    for row in quotes["rows"]:
        row.update(trade_date=next_day, previous_date=day, previous_raw_close=10.1,
                   previous_close_adj=10.1, close_adj=10.2, raw_close=10.2)
    quotes.update(market_date=next_day, collected_at=next_day+"T16:00:00+08:00")
    hedge.update(market_date=next_day, collected_at=quotes["collected_at"], close=hedge["close"]*1.001)
    result = daily.build(Path(__file__).resolve().parents[1], cp, quotes, hedge, next_day,
                         "close_confirmed", tmp_path / "second", now=datetime.fromisoformat(next_day+"T16:01:00+08:00"),
                         calendar_sessions=sessions+[(pd.Timestamp(next_day)+pd.offsets.BDay()).date()])
    # This deterministic second session is a native rebalance Thursday. The
    # old basket remains a reference, but its earned-return checkpoint must
    # still be available for the atomic same-close handoff.
    assert result["period_status"] == "overdue_reference", result
    assert all(row["status"] == "reference" and row["signal"] for row in result["versions"]), result
    verify(tmp_path / "second/checkpoint.json")


def test_saved_close_renders_original_rows_without_collection_or_calculation(bundle, tmp_path, monkeypatch):
    root, _, quotes, hedge, day, _ = bundle
    run(bundle, tmp_path)
    def forbidden(*args, **kwargs):
        raise AssertionError("Saved-close rendering must not calculate or collect")
    monkeypatch.setattr(daily, "native_modules", forbidden)
    monkeypatch.setattr(daily, "member_level", forbidden)
    result = daily.saved_close_result(tmp_path / "result/checkpoint.json", day, tmp_path / "saved",
                                      now=datetime.fromisoformat(day+"T17:00:00+08:00"))
    original = json.loads((tmp_path / "result/result.json").read_bytes())
    assert result["reused_close_checkpoint"] and result["collection_performed"] is False
    assert [row["signal"] for row in result["versions"]] == [row["signal"] for row in original["versions"]]
    from scripts.member_period_handoff import verify
    verify(tmp_path / "saved/checkpoint.json")


@pytest.mark.parametrize("case", ["future", "wrong_day", "intraday_state", "missing_source"])
def test_saved_close_rejects_unconfirmed_or_unbound_checkpoint(bundle, tmp_path, case):
    root, _, _, _, day, _ = bundle
    run(bundle, tmp_path)
    saved = tmp_path / "result/checkpoint.json"
    cp = json.loads(saved.read_bytes())
    now = datetime.fromisoformat(day+"T17:00:00+08:00")
    if case == "future": now = datetime.fromisoformat(day+"T14:00:00+08:00")
    if case == "wrong_day": day = (pd.Timestamp(day)+pd.Timedelta(days=1)).date().isoformat()
    if case == "intraday_state":
        states = json.loads((saved.parent / "version_state.json").read_bytes())
        states["versions"]["v2.0"]["official_close_confirmed_signal"] = False
        daily.write_json(saved.parent / "version_state.json", states)
        cp["version_state"]["sha256"] = daily.digest((saved.parent / "version_state.json").read_bytes())
    if case == "missing_source":
        period = json.loads((saved.parent / "period.json").read_bytes())
        period.pop("source_manifest_sha256")
        daily.write_json(saved.parent / "period.json", period)
        cp["period"]["sha256"] = daily.digest((saved.parent / "period.json").read_bytes())
    daily.write_json(saved, cp)
    with pytest.raises(ValueError):
        daily.saved_close_result(saved, day, tmp_path / "rejected", now=now)


def replay_inputs(bundle, tmp_path, *, failed_first=False):
    import copy
    root, _, quotes, hedge, first_day, sessions = bundle
    last_day = sessions[-1].isoformat()
    directory = tmp_path / "observations"
    directory.mkdir()
    first_quotes, first_hedge = copy.deepcopy(quotes), copy.deepcopy(hedge)
    # Both days were actually collected after the second close. The first
    # observation remains a retrospective daily bar, never an earlier report.
    first_quotes["collected_at"] = last_day + "T16:00:00+08:00"
    first_hedge["collected_at"] = first_quotes["collected_at"]
    if failed_first: first_quotes["rows"].pop()
    final_quotes, final_hedge = copy.deepcopy(quotes), copy.deepcopy(hedge)
    for row in final_quotes["rows"]:
        row.update(trade_date=last_day, previous_date=first_day, previous_raw_close=10.1,
                   previous_close_adj=10.1, close_adj=10.2, raw_close=10.2)
    final_quotes.update(market_date=last_day, collected_at=last_day+"T16:00:00+08:00")
    final_hedge.update(market_date=last_day, collected_at=final_quotes["collected_at"], close=final_hedge["close"]*1.001)
    for name, value in (("q1.json", first_quotes), ("h1.json", first_hedge), ("q2.json", final_quotes), ("h2.json", final_hedge)):
        daily.write_json(directory / name, value)
    manifest = dict(schema="member_daily_replay_v1", anchor_date=json.loads((root / "checkpoint.json").read_bytes())["as_of_date"],
                    latest_date=last_day, mode="close_confirmed", days=[dict(date=first_day, quotes="q1.json", hedge="h1.json"),
                    dict(date=last_day, quotes="q2.json", hedge="h2.json")])
    daily.write_json(directory / "replay_manifest.json", manifest)
    return directory / "replay_manifest.json", last_day, sessions+[(pd.Timestamp(last_day)+pd.offsets.BDay()).date()]


def test_missing_close_sessions_replay_only_real_supplied_tail(bundle, tmp_path):
    from scripts.member_period_handoff import verify
    manifest, day, sessions = replay_inputs(bundle, tmp_path)
    result = daily.replay_from_manifest(Path(__file__).resolve().parents[1], bundle[0] / "checkpoint.json", manifest,
                 day, "close_confirmed", tmp_path / "replayed", now=datetime.fromisoformat(day+"T16:01:00+08:00"),
                 calendar_sessions=sessions)
    assert all(row["signal"] is not None for row in result["versions"])
    verify(tmp_path / "replayed/checkpoint.json")
    earlier = json.loads((tmp_path / "replayed_replay" / bundle[4] / "result.json").read_bytes())
    assert earlier["retrospective_replay"] is True and earlier["generated_at"].startswith(day)
    assert all(row["signal"]["retrospective_replay"] for row in earlier["versions"])
    assert result["replay"]["originally_published"] is False


def test_incomplete_replay_retains_today_information_without_filling_missing_day(bundle, tmp_path):
    manifest, day, sessions = replay_inputs(bundle, tmp_path, failed_first=True)
    result = daily.replay_from_manifest(Path(__file__).resolve().parents[1], bundle[0] / "checkpoint.json", manifest,
                 day, "close_confirmed", tmp_path / "partial_tail", now=datetime.fromisoformat(day+"T16:01:00+08:00"),
                 calendar_sessions=sessions)
    assert result["status"] == "PARTIAL" and result["checkpoint"] is None
    assert all(row["signal"] is None and "Missing confirmed" in row["reason"] for row in result["versions"])
    retained = json.loads((tmp_path / "partial_tail/quotes.json").read_bytes())
    assert retained["market_date"] == day and len(retained["rows"]) == 100
    assert result["warnings"] and not (tmp_path / "partial_tail/history.csv").exists()


def test_missing_earlier_hedge_does_not_hide_next_day_unhedged_signal(bundle, tmp_path):
    from scripts.member_period_handoff import verify
    manifest_path, day, sessions = replay_inputs(bundle, tmp_path)
    manifest = json.loads(manifest_path.read_bytes())
    manifest["days"][0].pop("hedge")
    daily.write_json(manifest_path, manifest)
    result = daily.replay_from_manifest(Path(__file__).resolve().parents[1], bundle[0] / "checkpoint.json", manifest_path,
                 day, "close_confirmed", tmp_path / "hedge_gap", now=datetime.fromisoformat(day+"T16:01:00+08:00"),
                 calendar_sessions=sessions)
    assert result["status"] == "PARTIAL" and result["checkpoint"] is None
    assert [row["status"] for row in result["versions"][:2]] == ["partial", "partial"]
    assert result["versions"][2]["signal"] is not None
    observation = tmp_path / "hedge_gap" / result["component_observation"]
    cp = json.loads(observation.read_bytes())
    assert cp["component_only"] is True and cp["publishable_shared_checkpoint"] is False
    state = json.loads((observation.parent / "version_state.json").read_bytes())["versions"]
    assert pd.Timestamp(state["v2.0"]["date"]).date().isoformat() == json.loads((bundle[0]/"checkpoint.json").read_bytes())["as_of_date"]
    with pytest.raises(ValueError, match="Unsupported"):
        verify(observation)
    with pytest.raises(ValueError, match="Unsupported"):
        daily.load_checkpoint(observation)
