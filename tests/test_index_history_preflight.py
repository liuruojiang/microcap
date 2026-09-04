from datetime import date
from types import SimpleNamespace

from hypothesis import given, settings, strategies as st
import pandas as pd
import pytest

from scripts import index_history_preflight as history

START, END = date(2026, 9, 1), date(2026, 9, 4)
SESSIONS = tuple(date(2026, 9, day) for day in range(1, 5))


def good():
    return pd.DataFrame(dict(date=SESSIONS, open=100., close=101., high=102., low=99., volume=10.))


@settings(max_examples=24)
@given(st.permutations(range(4)))
def test_validation_preserves_prices_and_is_idempotent(order):
    value = history.validate_history(good().iloc[list(order)], START, END, SESSIONS)
    pd.testing.assert_frame_equal(value, history.validate_history(value, START, END, SESSIONS))
    assert value.close.tolist() == [101.] * 4
    assert value.date.dt.date.tolist() == list(SESSIONS)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -1., 0.])
@pytest.mark.parametrize("column", ["open", "close", "high", "low"])
def test_invalid_prices_rejected(column, bad):
    frame = good()
    frame.loc[1, column] = bad
    with pytest.raises(ValueError):
        history.validate_history(frame, START, END, SESSIONS)


@pytest.mark.parametrize("index", range(4))
def test_missing_intervening_or_last_session_rejected(index):
    with pytest.raises(ValueError, match="calendar"):
        history.validate_history(good().drop(index), START, END, SESSIONS)


def test_duplicate_and_non_session_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        history.validate_history(pd.concat([good(), good().iloc[:1]]), START, END, SESSIONS)
    with pytest.raises(ValueError, match="calendar"):
        history.validate_history(good(), START, END, (SESSIONS[0], SESSIONS[2], SESSIONS[3]))


def calendar(monkeypatch):
    monkeypatch.setattr(history.exchange_calendar, "latest_completed_session", lambda: END)
    monkeypatch.setattr(history.exchange_calendar, "sessions_for_day", lambda day: SESSIONS)


def test_stale_primary_falls_back_with_durable_provenance(monkeypatch):
    calendar(monkeypatch)
    monkeypatch.setattr(history, "sina_static", lambda *a: good().iloc[:-1])
    monkeypatch.setattr(history, "tencent", lambda *a: good())
    value = history.fetch_preflight_history(START, END)
    assert value.attrs["independent_history_source"] == "tencent"
    assert [x["ok"] for x in value.attrs["independent_history_attempts"]] == [False, True]


def test_fail_closed_when_all_sources_stale(monkeypatch):
    calendar(monkeypatch)
    for provider in ("sina_static", "tencent"):
        monkeypatch.setattr(history, provider, lambda *a: good().iloc[:-1])
    with pytest.raises(RuntimeError, match="All independent"):
        history.fetch_preflight_history(START, END)


def test_expected_date_cannot_bypass_calendar(monkeypatch):
    calendar(monkeypatch)
    with pytest.raises(RuntimeError, match="calendar"):
        history.fetch_preflight_history(START, START)


def test_tencent_rejects_wrong_symbol_and_adjusted_series(monkeypatch):
    for data in ({"sz000852": {"day": []}}, {"sh000852": {"qfqday": []}}):
        monkeypatch.setattr(history, "_get", lambda *a, **k: SimpleNamespace(json=lambda: {"code": 0, "data": data}))
        with pytest.raises(KeyError):
            history.tencent(START, END)


def test_request_is_bounded_and_https(monkeypatch):
    def get(url, **kwargs):
        assert url.startswith("https://")
        assert kwargs["timeout"] == 8
        return SimpleNamespace(raise_for_status=lambda: None)
    monkeypatch.setattr(history.requests, "get", get)
    history._get("https://example.com")


def test_sina_decodes_only_string_not_remote_javascript(monkeypatch):
    from akshare.index import index_stock_zh
    class Decoder:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def eval(self, code):
            assert code == index_stock_zh.hk_js_decode
        def call(self, name, value):
            assert name == "d" and value == "encoded"
            return [dict(date="2026-09-04T00:00:00.000Z", open=100., close=101., high=102., low=99., volume=10.)]
    monkeypatch.setattr(index_stock_zh.py_mini_racer, "MiniRacer", Decoder)
    monkeypatch.setattr(history, "_get", lambda *a: SimpleNamespace(text='var KLC_KL_sh000852="encoded";evil();'))
    value = history.sina_static(END, END)
    assert history.validate_history(value, END, END, SESSIONS).date.iloc[0] == pd.Timestamp(END)


@pytest.mark.parametrize("winner", ["eastmoney", "sina_legacy", "sina_static", "tencent"])
def test_official_history_entrypoint_uses_same_validated_fallback_chain(monkeypatch, winner):
    import microcap_top100_mom16_biweekly_live_v2_0 as v20
    calendar(monkeypatch)
    calls = []
    def provider(name):
        def fetch(*args):
            calls.append(name)
            if name != winner:
                raise TimeoutError("simulated transport blockage")
            return good()
        return fetch
    names = ["eastmoney", "sina_legacy", "sina_static", "tencent"]
    for name in names:
        monkeypatch.setattr(history, name, provider(name))
    value = v20.base_mod.fetch_eastmoney_index_history("1.000852", pd.Timestamp(START), pd.Timestamp(END))
    assert value.attrs["independent_history_source"] == winner
    assert calls == names[:names.index(winner)+1]


@settings(max_examples=30)
@given(st.floats(min_value=-0.005, max_value=0.005, allow_nan=False, allow_infinity=False))
def test_backup_precision_never_rewrites_canonical_closes(difference):
    source = good()
    source.close += difference
    reference = good().rename(columns={"close": "hedge"})
    value = history.preserve_existing_closes(source, reference, "hedge")
    assert value.close.tolist() == [101.] * 4
    assert value.attrs["canonical_overlap_rows"] == 4


def test_material_source_disagreement_blocks_instead_of_rewriting():
    reference = good().rename(columns={"close": "hedge"})
    reference.loc[0, "hedge"] += 1
    with pytest.raises(ValueError, match="materially disagree"):
        history.preserve_existing_closes(good(), reference, "hedge")


def test_backup_new_session_kept_but_existing_prices_unchanged():
    reference = good().iloc[:-1].rename(columns={"close": "hedge"})
    reference.hedge += .005
    value = history.preserve_existing_closes(good(), reference, "hedge")
    assert value.close.tolist() == [101.005]*3 + [101.]


@pytest.mark.parametrize("provider", ["eastmoney", "sina_legacy", "sina_static", "tencent"])
def test_official_shadow_path_preserves_history_when_fallback_used(tmp_path, monkeypatch, provider):
    import microcap_top100_mom16_biweekly_live_v2_0 as v20
    base = v20.base_mod
    original = good()[["date", "close"]].rename(columns={"close": base.HEDGE_COLUMN})
    source = good()
    source.close += .005
    source.attrs["independent_history_source"] = provider
    original.to_csv(tmp_path / "panel.csv", index=False)
    original.to_csv(tmp_path / "shadow.csv", index=False)
    globals_ = base.build_refreshed_panel_shadow.__globals__
    monkeypatch.setitem(globals_, "panel_shadow_cache_is_reusable", lambda *a: False)
    monkeypatch.setitem(globals_, "fetch_eastmoney_index_history", lambda *a: source)
    monkeypatch.setitem(globals_, "latest_closed_history_date", lambda *a: pd.Timestamp(END))
    output, target = base.build_refreshed_panel_shadow(SimpleNamespace(panel_path=tmp_path / "panel.csv"),
                                                       {"panel_shadow": tmp_path / "shadow.csv"})
    assert pd.read_csv(output)[base.HEDGE_COLUMN].tolist() == [101.]*4
    assert target == pd.Timestamp(END)
