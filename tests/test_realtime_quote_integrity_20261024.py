"""Synthetic provider fault inputs against the real standalone/embedded quote code.

No market downloads, historical rewrites, final-output writes or performance claims.
"""
import importlib
import json
import socket
from urllib.parse import parse_qs, urlparse

import numpy as np
import pandas as pd
import pytest
import requests


DAY = "2026-10-23"
ANCHOR = "2026-10-22"
NOW = pd.Timestamp(DAY + " 14:50:00", tz="Asia/Shanghai")
SYMBOLS = [f"{number:06d}" for number in range(1, 101)]
TARGET = SYMBOLS[-1]


@pytest.fixture(params=["standalone", "embedded"])
def base(request, monkeypatch):
    module = importlib.import_module("microcap_top100_mom16_biweekly_live" if request.param == "standalone" else "microcap_top100_mom16_biweekly_live_v2_0")
    result = module if request.param == "standalone" else module.base_mod
    ns = result.build_realtime_signal_fast.__globals__
    original = ns["_cn_timestamp"]
    monkeypatch.setitem(ns, "_cn_timestamp", lambda now=None: NOW if now is None else original(now))
    monkeypatch.setitem(ns, "REALTIME_QUOTE_FETCH_ATTEMPTS", 1)
    monkeypatch.setattr(socket.socket, "connect", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("No network in synthetic quote tests")))
    return result


class Response:
    encoding = "utf-8"
    def __init__(self, payload=None, text=""):
        self.payload, self.text = payload, text
    def json(self):
        return self.payload
    def raise_for_status(self):
        pass


class Quotes:
    def __init__(self, bad=None, times=None):
        self.bad, self.times, self.calls = bad or {}, times or {}, []
    def price(self, provider, code):
        return self.bad.get((provider, code), 10.1)
    def stamp(self, provider):
        value = self.times.get(provider, NOW - pd.Timedelta(seconds=30))
        return None if value is None else pd.Timestamp(value)
    def __call__(self, url, **kwargs):
        self.calls.append(url)
        query = parse_qs(urlparse(url).query)
        if "ulist.np" in url:
            stamp = self.stamp("em")
            rows = [{"f12": item.split(".")[-1], "f14": "SYNTHETIC", "f2": self.price("em", item.split(".")[-1]),
                     "f18": 10.0, "f124": "" if stamp is None else int(stamp.timestamp()), "f297": DAY.replace("-", "")}
                    for item in query["secids"][0].split(",")]
            return Response({"data": {"diff": rows}})
        if "qt.gtimg.cn" in url:
            stamp = self.stamp("tx")
            lines = []
            for item in url.split("/q=", 1)[1].split(","):
                fields = [""] * 31
                fields[1:5] = ["SYNTHETIC", item[-6:], str(self.price("tx", item[-6:])), "10"]
                fields[30] = "" if stamp is None else stamp.strftime("%Y%m%d%H%M%S")
                lines.append(f'v_{item}="' + "~".join(fields) + '";')
            return Response(text="".join(lines))
        if "/stock/get" in url:
            code = query["secid"][0].split(".")[-1]
            provider = "hedge_em" if code == "000852" else "spot"
            stamp = self.stamp(provider)
            value = self.price(provider, code)
            raw_value = value * 100 if isinstance(value, (int, float)) else value
            return Response({"data": {"f43": raw_value, "f60": 1000, "f57": code,
                                      "f86": "" if stamp is None else int(stamp.timestamp())}})
        raise AssertionError("Unexpected provider request: " + url)


def context():
    index = pd.bdate_range(end=ANCHOR, periods=120)
    return {"close_df": pd.DataFrame({"microcap": 1000 * np.exp(np.arange(120) * .001),
                                       "hedge": 1000 * np.exp(np.arange(120) * .0002)}, index=index),
            "effective_members": pd.DataFrame({"symbol": SYMBOLS, "name": ["SYNTHETIC"] * 100, "rank": range(1, 101)}),
            "expected_latest_completed_trade_date": ANCHOR,
            "expected_latest_completed_trade_date_source": "independent_close_history_refresh",
            "expected_latest_completed_trade_date_verified_on": DAY}


def build(base, monkeypatch, provider):
    monkeypatch.setattr(requests, "get", provider)
    monkeypatch.setitem(base.build_realtime_signal_fast.__globals__, "ensure_realtime_last_close_map", lambda *a, **k: dict.fromkeys(SYMBOLS, 10.0))
    value = base.build_realtime_signal_fast(context())
    return value[0], value[1]


@pytest.mark.parametrize("bad", ["-", 0, -1, float("nan"), float("inf")])
@pytest.mark.parametrize("adapter,provider", [("fetch_eastmoney_batch_realtime_quotes", "em"),
                                              ("fetch_tencent_realtime_quotes", "tx"),
                                              ("fetch_eastmoney_stock_spot", "spot")])
def test_invalid_latest_is_not_previous_close(base, monkeypatch, bad, adapter, provider):
    monkeypatch.setattr(requests, "get", Quotes(bad={(provider, TARGET): bad}))
    fn = getattr(base, adapter)
    result = fn(TARGET if provider == "spot" else [TARGET])
    assert result is None if provider == "spot" else result.empty


@pytest.mark.parametrize("adapter,provider", [("fetch_eastmoney_batch_realtime_quotes", "em"),
                                              ("fetch_tencent_realtime_quotes", "tx"),
                                              ("fetch_eastmoney_stock_spot", "spot")])
def test_fresh_positive_latest_keeps_price_units_and_provenance(base, monkeypatch, adapter, provider):
    monkeypatch.setattr(requests, "get", Quotes())
    result = getattr(base, adapter)(TARGET if provider == "spot" else [TARGET])
    row = result if provider == "spot" else result.iloc[0]
    assert row["rt_price"] == pytest.approx(10.1)
    assert row["pre_close"] == pytest.approx(10.0)
    assert base.realtime_quote_row_is_genuine(row)
    normalized = base._normalize_realtime_quote_frame(pd.DataFrame([dict(row)]))
    assert normalized.iloc[0]["quoted_at"] == row["quoted_at"]
    assert normalized.iloc[0]["fetched_at"] == NOW.isoformat()


@pytest.mark.parametrize("primary_time", [NOW - pd.Timedelta(hours=5), NOW + pd.Timedelta(minutes=9), None])
def test_invalid_primary_clock_uses_fresh_backup_before_dedup(base, monkeypatch, primary_time):
    provider = Quotes(times={"em": primary_time})
    monkeypatch.setattr(requests, "get", provider)
    frame = base.fetch_member_realtime_quotes([TARGET])
    assert len(frame) == 1
    assert frame.iloc[0]["quote_source"] == "tencent_batch_free"
    assert base._valid_quote_code_set(frame) == {TARGET}
    assert any("qt.gtimg.cn" in call for call in provider.calls)


@pytest.mark.parametrize("bad", ["-", 0, -1])
def test_missing_primary_price_uses_backup(base, monkeypatch, bad):
    provider = Quotes(bad={("em", TARGET): bad})
    monkeypatch.setattr(requests, "get", provider)
    result = base.fetch_member_realtime_quotes([TARGET])
    assert result.iloc[0]["quote_source"] == "tencent_batch_free"
    assert result.iloc[0]["rt_price"] == pytest.approx(10.1)


@pytest.mark.parametrize("bad", ["-", 0, -1])
def test_both_hedge_providers_missing_latest_fail_closed(base, monkeypatch, bad):
    provider = Quotes(bad={("hedge_em", "000852"): bad, ("tx", "000852"): bad})
    monkeypatch.setattr(requests, "get", provider)
    with pytest.raises(RuntimeError, match="no fresh genuine hedge"):
        base.fetch_hedge_realtime_quote_fast()
    assert len(provider.calls) == 2


def test_hedge_backup_keeps_legacy_unpacking_and_new_proof(base, monkeypatch):
    monkeypatch.setattr(requests, "get", Quotes(bad={("hedge_em", "000852"): "-"}))
    result = base.fetch_hedge_realtime_quote_fast()
    price, source, day = result
    assert price == pytest.approx(10.1) and source == "tencent_batch_free" and day == DAY
    assert base.extract_hedge_quote_integrity_meta(result)["hedge_price_origin"] == "provider_latest"
    assert base.normalize_hedge_realtime_quote_result(result) == (price, source, day)


@pytest.mark.parametrize("time", [NOW - pd.Timedelta(hours=5), NOW + pd.Timedelta(seconds=1), None])
def test_all_hedge_clocks_invalid_fail_closed(base, monkeypatch, time):
    monkeypatch.setattr(requests, "get", Quotes(times={"hedge_em": time, "tx": time}))
    with pytest.raises(RuntimeError, match="no fresh genuine hedge"):
        base.fetch_hedge_realtime_quote_fast()


def test_normal_full_build_and_gate(base, monkeypatch):
    signal, meta = build(base, monkeypatch, Quotes())
    base.assert_realtime_meta_is_actionable(meta)
    base.assert_realtime_quote_integrity_matches_signal(signal, meta)
    assert meta["member_price_count"] == meta["member_quote_integrity_count"] == 100
    assert meta["member_quote_flat_fallback_count"] == 0


@pytest.mark.parametrize("bad", ["-", 0, -1])
def test_full_build_all_providers_missing_one_member_is_preview(base, monkeypatch, bad):
    provider = Quotes(bad={(source, TARGET): bad for source in ("em", "tx", "spot")})
    signal, meta = build(base, monkeypatch, provider)
    assert meta["member_price_count"] == meta["member_quote_integrity_count"] == 99
    assert meta["member_quote_flat_fallback_count"] == 1
    with pytest.raises(RuntimeError, match="synthetic last-close"):
        base.assert_realtime_meta_is_actionable(meta)
    assert signal.iloc[0]["member_price_count"] == 99


@pytest.mark.parametrize("time", [NOW - pd.Timedelta(hours=5), NOW + pd.Timedelta(minutes=9), None])
def test_all_member_providers_invalid_clock_have_no_genuine_coverage(base, monkeypatch, time):
    provider = Quotes(times={source: time for source in ("em", "tx", "spot")})
    monkeypatch.setattr(requests, "get", provider)
    result = base.fetch_member_realtime_quotes([TARGET])
    assert result.empty and base._valid_quote_code_set(result) == set()
    assert len(provider.calls) == 3


def test_source_clock_ages_during_cache_use(base, monkeypatch, tmp_path):
    signal, meta = build(base, monkeypatch, Quotes())
    paths = {"cache_fast_realtime_meta": tmp_path / "meta.json", "cache_fast_realtime_signal": tmp_path / "signal.csv"}
    paths["cache_fast_realtime_meta"].write_text(json.dumps(meta), encoding="utf-8")
    signal.to_csv(paths["cache_fast_realtime_signal"], index=False)
    assert base.load_cached_fast_realtime_signal(paths, 30, pd.Timestamp(ANCHOR)) is not None
    ns = base.build_realtime_signal_fast.__globals__
    original = ns["_cn_timestamp"]
    monkeypatch.setitem(ns, "_cn_timestamp", lambda now=None: NOW + pd.Timedelta(seconds=100) if now is None else original(now))
    # File is still brand new, but source age is 130 seconds, beyond policy.
    assert base.load_cached_fast_realtime_signal(paths, 30, pd.Timestamp(ANCHOR)) is None


def test_csv_cannot_drop_or_contradict_quote_proof(base, monkeypatch, tmp_path):
    signal, meta = build(base, monkeypatch, Quotes())
    signal.loc[signal.index[0], "hedge_price_origin"] = "last_close_preview"
    with pytest.raises(RuntimeError, match="CSV/meta mismatch"):
        base.assert_realtime_quote_integrity_matches_signal(signal, meta)
    paths = {"cache_fast_realtime_meta": tmp_path / "meta.json", "cache_fast_realtime_signal": tmp_path / "signal.csv"}
    paths["cache_fast_realtime_meta"].write_text(json.dumps(meta), encoding="utf-8")
    signal.to_csv(paths["cache_fast_realtime_signal"], index=False)
    assert base.load_cached_fast_realtime_signal(paths, 30, pd.Timestamp(ANCHOR)) is None


def test_lunch_is_excluded_but_source_timestamp_is_not_relabelled(base):
    quote = pd.Timestamp(DAY + " 11:29:00", tz="Asia/Shanghai")
    fetched = pd.Timestamp(DAY + " 12:45:00", tz="Asia/Shanghai")
    assert base.realtime_quote_active_age_seconds(quote, fetched) == 60
    assert base.realtime_quote_clock_is_valid(quote.isoformat(), fetched.isoformat(), DAY, fetched)
    assert base.realtime_quote_clock_is_valid(quote.isoformat(), fetched.isoformat(), DAY, fetched.replace(hour=13, minute=1))
    assert not base.realtime_quote_clock_is_valid(quote.isoformat(), fetched.isoformat(), DAY, fetched.replace(hour=13, minute=2))
    assert quote.hour == 11 and quote.minute == 29


def test_unknown_date_only_time_and_unverified_suspension_do_not_pass(base):
    assert base.parse_realtime_source_timestamp(DAY) is None
    assert base.parse_realtime_source_timestamp("20261023") is None
    row = {"rt_price": 10.0, "trade_date": DAY, "quoted_at": (NOW - pd.Timedelta(hours=1)).isoformat(),
           "fetched_at": NOW.isoformat(), "price_origin": "provider_latest", "suspended": True}
    assert not base.realtime_quote_row_is_genuine(row)


def test_legacy_slow_signal_without_source_proof_remains_nonactionable(base, monkeypatch):
    ns = base.build_realtime_signal.__globals__
    quotes = pd.DataFrame({"code": SYMBOLS, "rt_price": [10.1] * 100, "trade_date": [DAY] * 100})
    monkeypatch.setitem(ns, "ensure_realtime_last_close_map", lambda *a, **k: dict.fromkeys(SYMBOLS, 10.0))
    monkeypatch.setitem(ns, "build_realtime_quote_map", lambda *a, **k: (quotes, "synthetic_legacy_quote_map"))
    monkeypatch.setitem(ns, "load_or_refresh_index_spot", lambda *a, **k: pd.DataFrame({"代码": ["000852"], "最新价": [1000.0], "昨收": [1000.0]}))
    _, meta = base.build_realtime_signal(context(), cache_seconds=0)
    assert not base.realtime_meta_is_actionable(meta)
    with pytest.raises(RuntimeError, match="proof missing or obsolete"):
        base.assert_realtime_quote_integrity(meta)


def test_second_state_cache_rejects_missing_and_tampered_proof(base, monkeypatch, tmp_path):
    signal, meta = build(base, monkeypatch, Quotes())
    meta.update(latest_rebalance=ANCHOR, effective_rebalance=None, rebalance_effective_date=None)
    ns = base.save_realtime_state_cache.__globals__
    monkeypatch.setitem(ns, "REALTIME_DIR", tmp_path / "lock_dir")
    paths = {"cache_realtime_" + key: tmp_path / (key + suffix) for key, suffix in
             (("meta", ".json"), ("signal", ".csv"), ("members", ".csv"), ("changes", ".csv"))}
    members = context()["effective_members"]
    changes = pd.DataFrame(columns=["symbol", "action"])
    base.save_realtime_state_cache(paths, meta, signal, members, changes)
    def load():
        return base.load_cached_realtime_state(paths, 30, pd.Timestamp(ANCHOR), pd.Timestamp(ANCHOR), None, None, None)
    assert load() is not None
    tampered = signal.copy()
    tampered["hedge_price_origin"] = "last_close_preview"
    with pytest.raises(RuntimeError, match="CSV/meta mismatch"):
        base.save_realtime_state_cache(paths, meta, tampered, members, changes)
    tampered.to_csv(paths["cache_realtime_signal"], index=False)
    assert load() is None
    signal.to_csv(paths["cache_realtime_signal"], index=False)
    missing_meta = dict(meta)
    missing_meta.pop("quote_integrity_policy")
    paths["cache_realtime_meta"].write_text(json.dumps(missing_meta), encoding="utf-8")
    assert load() is None


@pytest.mark.parametrize("version", ["2_0", "2_3", "2_5"])
def test_three_version_final_publication_helper_revalidates_clock_and_writes_proof(monkeypatch, version):
    module = importlib.import_module("microcap_top100_mom16_biweekly_live_v" + version)
    v20 = module if version == "2_0" else module.v2_0
    base = v20.realtime_core.base_mod
    ns = base.build_realtime_signal_fast.__globals__
    original = ns["_cn_timestamp"]
    monkeypatch.setitem(ns, "_cn_timestamp", lambda now=None: NOW if now is None else original(now))
    signal, meta = build(base, monkeypatch, Quotes())
    helper = v20.overlay_mod._apply_realtime_meta_columns_to_signal_row
    output = pd.DataFrame({"placeholder": [True]})
    helper(output, meta)
    assert set(base.REALTIME_QUOTE_INTEGRITY_META_FIELDS).issubset(output.columns)
    base.assert_realtime_quote_integrity_matches_signal(output, meta)
    # The caller's calculation has taken long enough that the same source quote expires.
    monkeypatch.setitem(ns, "_cn_timestamp", lambda now=None: NOW + pd.Timedelta(seconds=100) if now is None else original(now))
    with pytest.raises(RuntimeError, match="Realtime quote integrity"):
        helper(pd.DataFrame({"placeholder": [True]}), meta)
    # This is the real shared final helper reached by each named version, not an output-file or mail E2E test.
