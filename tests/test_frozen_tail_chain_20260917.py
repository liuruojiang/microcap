"""Synthetic state-boundary tests; never use these fixtures as market results."""
import hashlib
import json
from types import SimpleNamespace

import pandas as pd
import pytest

import microcap_top100_mom16_biweekly_live_v2_0 as v


@pytest.fixture
def chain(tmp_path, monkeypatch):
    base = v.base_mod
    ns = base.ensure_strategy_files.__globals__
    monkeypatch.setitem(ns, "TOP_N", 2)
    monkeypatch.setitem(ns, "_proxy_meta_core_matches_execution_model", lambda meta: True)
    monkeypatch.setitem(ns, "proxy_meta_matches_execution_model", lambda meta: False)
    monkeypatch.setitem(ns, "refresh_price_cache_tail", lambda *a, **kw: None)
    dates = pd.bdate_range("2026-09-17", "2026-10-02")
    seed = dates[0]
    paths = {key: tmp_path / (key + ".csv") for key in ["proxy_members", "proxy_turnover", "proxy_effective_members", "panel_shadow"]}
    paths["proxy_meta"] = tmp_path / "meta.json"
    args = SimpleNamespace(index_csv=tmp_path / "index.csv", costed_nav_csv=tmp_path / "costed.csv",
                           max_workers=1, force_refresh=False, rebuild_index_if_missing=True, allow_full_rebuild=False)
    symbols = ["000001", "000002"]
    pd.DataFrame({"date": dates}).to_csv(paths["panel_shadow"], index=False)
    pd.DataFrame({"rebalance_date": [seed] * 2, "rank": [1, 2], "symbol": symbols}).to_csv(paths["proxy_members"], index=False)
    pd.DataFrame({"rebalance_date": [seed], "entry_count": [0], "exit_count": [0]}).to_csv(paths["proxy_turnover"], index=False)
    pd.DataFrame({"as_of_date": [seed] * 2, "rank": [1, 2], "symbol": symbols}).to_csv(paths["proxy_effective_members"], index=False)
    pd.DataFrame({"date": [seed], "close": [100.0], "daily_return": [0.01], "holding_count": [2], "holding_effective": [True]}).to_csv(args.index_csv, index=False)
    gross = pd.DataFrame(index=dates.rename("date"))
    for key, value in {"return": 0.01, "holding": "long_microcap_short_zz1000", "next_holding": "long_microcap_short_zz1000",
                       "signal_on": True, "hedge_close": 100.0, "microcap_mom": 0.02, "hedge_mom": 0.01, "momentum_gap": 0.01}.items():
        gross[key] = value
    gross["microcap_close"] = [100 * 1.01 ** i for i in range(len(dates))]
    costed = gross.iloc[:1].copy()
    for key, value in {"entry_exit_cost": 0.0, "rebalance_cost": 0.0, "total_cost": 0.0, "return_net": 0.01,
                       "overlay_pre_cost_return": 0.01, "nav_net": 2.0}.items():
        costed[key] = value
    costed.to_csv(args.costed_nav_csv, index_label="date")
    monkeypatch.setitem(ns, "load_close_df", lambda panel, index, max_date=None: gross.loc[:max_date])
    monkeypatch.setitem(ns, "run_signal", lambda close: close.copy())
    cache = {}
    for symbol in symbols:
        path = tmp_path / (symbol + ".csv")
        pd.DataFrame({"date": dates, "close_raw": gross.microcap_close.to_numpy()}).to_csv(path, index=False)
        cache[symbol] = path
    monkeypatch.setattr(base.freq_mod, "resolve_cache_path", lambda primary, shared, symbol: cache[symbol] if primary == base.freq_mod.PRICE_DIR else None)
    monkeypatch.setattr(base.freq_mod, "list_backtest_universe_symbols", lambda: symbols)
    st = tmp_path / "current_st.csv"
    st.write_text("code,name\n999999,*ST test\n", encoding="utf-8")
    monkeypatch.setattr(base.freq_mod, "CURRENT_ST", st)
    monkeypatch.setattr(base.freq_mod, "load_current_st_name_map", lambda: {"999999": "*ST test"})
    fp = {"present_count": 10, "missing_count": 0, "sha256": "test-authorized-metadata"}
    meta = {"core_params": {"security_meta_cache_fingerprint": fp}}
    paths["proxy_meta"].write_text(json.dumps(meta), encoding="utf-8")
    authority = {"version": base.FROZEN_TAIL_AUTHORITY_VERSION, "seed_end_date": str(seed.date()),
                 "security_meta_cache_fingerprint": fp, "seed_file_rows": {"proxy_index": 1, "costed_nav": 1},
                 "seed_file_sha256": {key: base._file_sha256(path) for key, path in {
                     "proxy_index": args.index_csv, "costed_nav": args.costed_nav_csv,
                     **{k: paths[k] for k in ["proxy_meta", "proxy_members", "proxy_turnover", "proxy_effective_members"]}}.items()}}
    auth_path = tmp_path / "authority.json"
    auth_path.write_text(json.dumps(authority), encoding="utf-8")
    monkeypatch.setitem(ns, "FROZEN_TAIL_AUTHORITY_PATH", auth_path)
    frozen_bytes = {key: path.read_bytes() for key, path in {"proxy_index": args.index_csv, "costed_nav": args.costed_nav_csv,
                    "proxy_effective_members": paths["proxy_effective_members"], "proxy_turnover": paths["proxy_turnover"]}.items()}
    return SimpleNamespace(base=base, args=args, paths=paths, seed=seed, frozen=frozen_bytes, authority=authority)


def extend(state, target):
    target = pd.Timestamp(target)
    state.base.ensure_strategy_files(state.args, state.paths, state.paths["panel_shadow"], target)
    meta = json.loads(state.paths["proxy_meta"].read_text(encoding="utf-8"))
    assert state.base.frozen_tail_extension_matches_authority(state.args, state.paths, meta, target, target)
    return meta


def test_repeated_daily_extensions_keep_seed_identity_and_frozen_bytes(chain):
    first = extend(chain, "2026-09-18")
    second = extend(chain, "2026-09-21")
    assert first["tail_extension_rows"] == 1
    assert second["tail_extension_rows"] == 2
    assert second["tail_extension_start"] == "2026-09-17"
    assert chain.args.index_csv.read_bytes().startswith(chain.frozen["proxy_index"])
    assert chain.args.costed_nav_csv.read_bytes().startswith(chain.frozen["costed_nav"])
    assert chain.paths["proxy_effective_members"].read_bytes() == chain.frozen["proxy_effective_members"]
    assert chain.paths["proxy_turnover"].read_bytes() == chain.frozen["proxy_turnover"]


def test_no_rebalance_chain_can_cross_ten_calendar_days(chain):
    extend(chain, "2026-09-18")
    meta = extend(chain, "2026-09-28")
    assert meta["tail_extension_start"] == "2026-09-17"
    assert meta["tail_extension_rows"] == 7


def test_long_single_gap_uses_calendar_and_never_rebuilds(chain):
    meta = extend(chain, "2026-09-30")
    assert meta["tail_extension_rows"] == 9


@pytest.mark.parametrize("tamper", ["proxy_prefix", "costed_prefix", "effective", "turnover"])
def test_certified_chain_rejects_frozen_content_tampering(chain, tamper):
    meta = extend(chain, "2026-09-21")
    path = {"proxy_prefix": chain.args.index_csv, "costed_prefix": chain.args.costed_nav_csv,
            "effective": chain.paths["proxy_effective_members"], "turnover": chain.paths["proxy_turnover"]}[tamper]
    raw = path.read_bytes()
    path.write_bytes(raw.replace(b"2026-09-17", b"2026-09-16", 1))
    assert not chain.base.frozen_tail_extension_matches_authority(chain.args, chain.paths, meta, pd.Timestamp("2026-09-21"), pd.Timestamp("2026-09-21"))


def test_next_scheduled_rebalance_is_not_silently_carried_as_no_trade(chain):
    extend(chain, "2026-09-28")
    before = chain.args.index_csv.read_bytes()
    with pytest.raises(RuntimeError, match="narrow runner cache"):
        chain.base.ensure_strategy_files(chain.args, chain.paths, chain.paths["panel_shadow"], pd.Timestamp("2026-10-01"))
    assert chain.args.index_csv.read_bytes() == before


def test_full_cache_uses_same_exact_seed_append_and_keeps_executed_members(chain, monkeypatch):
    ns = chain.base.ensure_strategy_files.__globals__
    monkeypatch.setitem(ns, "proxy_meta_matches_execution_model", lambda meta: True)
    monkeypatch.setattr(chain.base.freq_mod, "list_backtest_universe_symbols", lambda: [str(i) for i in range(10)])
    monkeypatch.setitem(ns, "_reconstruct_effective_members_from_saved_targets", lambda **kw: pytest.fail("must retain executed seed"))
    extend(chain, "2026-09-18")
    extend(chain, "2026-09-28")
    assert chain.paths["proxy_effective_members"].read_bytes() == chain.frozen["proxy_effective_members"]
    assert chain.args.index_csv.read_bytes().startswith(chain.frozen["proxy_index"])


def test_non_authority_full_cache_reuses_valid_execution_date_without_replay(chain, monkeypatch):
    ns = chain.base.ensure_strategy_files.__globals__
    monkeypatch.setitem(ns, "proxy_meta_matches_execution_model", lambda meta: True)
    monkeypatch.setitem(ns, "_load_frozen_tail_authority", lambda: None)
    monkeypatch.setitem(ns, "_reconstruct_effective_members_from_saved_targets", lambda **kw: pytest.fail("must not reconstruct frozen executions"))
    for target in ["2026-09-18", "2026-09-21"]:
        chain.base.ensure_strategy_files(chain.args, chain.paths, chain.paths["panel_shadow"], pd.Timestamp(target))
    assert chain.paths["proxy_effective_members"].read_bytes() == chain.frozen["proxy_effective_members"]
