"""Second, independent arithmetic audit. All artificial fixtures are diagnostic.

Production files and official artifacts are read-only. The optional --real audit
uses the second-round freshness manifest and writes only math_* report artifacts.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import sys
from decimal import Decimal, localcontext
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import microcap_top100_mom16_biweekly_live_v2_0 as v20
import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_5 as v25

REPORT = ROOT / "research_reports" / "20261007_audit_doublecheck"
FIELDS = [
    "holding", "next_holding", "return_net", "nav_net", "total_cost",
    "entry_exit_cost", "rebalance_cost", "overlay_pre_cost_return",
    "current_execution_scale", "signal_on",
]


@lru_cache(maxsize=16)
def _decimal_weights(length: int, half: float) -> tuple[Decimal, ...]:
    with localcontext() as context:
        context.prec = 75
        ln2 = Decimal(2).ln()
        return tuple((-(Decimal(length - 1 - i) / Decimal(str(half))) * ln2).exp() for i in range(length))


def _decimal_regression(values: np.ndarray, half: float, annual: int) -> tuple[float, float]:
    """75-digit pairwise covariance identity, independent of centered WLS/lstsq."""
    length = len(values)
    with localcontext() as context:
        context.prec = 75
        weights = _decimal_weights(length, half)
        logs = [Decimal.from_float(float(value)).ln() for value in values]
        numerator = Decimal(0)
        denominator = Decimal(0)
        for i in range(length):
            for j in range(i + 1, length):
                dx = Decimal(j - i)
                product = weights[i] * weights[j]
                numerator += product * dx * (logs[j] - logs[i])
                denominator += product * dx * dx
        slope = numerator / denominator
        total_weight = sum(weights)
        x_average = sum(weights[i] * Decimal(i) for i in range(length)) / total_weight
        y_average = sum(weights[i] * logs[i] for i in range(length)) / total_weight
        total = sum(weights[i] * (logs[i] - y_average) ** 2 for i in range(length))
        residual = sum(weights[i] * (logs[i] - y_average - slope * (Decimal(i) - x_average)) ** 2 for i in range(length))
        r2 = Decimal(0) if total == 0 else max(Decimal(0), min(Decimal(1), 1 - residual / total))
        return float(slope * annual), float(r2)


def _pairwise_wls(nav: pd.Series, length: int, half: float, annual: int) -> pd.DataFrame:
    """Rolling pairwise identity; separate from the source's centered dot product."""
    x = np.arange(length, dtype=np.longdouble)
    weights = np.exp2(-(length - 1 - x) / np.longdouble(half))
    pair_weights = weights[:, None] * weights[None, :]
    dx = x[None, :] - x[:, None]
    select = np.triu(np.ones((length, length), dtype=bool), 1)
    numerator_coefficients = (pair_weights * dx)[select]
    denominator = np.sum((pair_weights * dx * dx)[select], dtype=np.longdouble)
    logs = np.log(nav.to_numpy(dtype=np.longdouble))
    rows = []
    for end in range(length - 1, len(logs)):
        y = logs[end + 1 - length:end + 1]
        slope = np.sum(numerator_coefficients * (y[None, :] - y[:, None])[select], dtype=np.longdouble) / denominator
        mean_y = np.sum(weights * y) / np.sum(weights)
        mean_x = np.sum(weights * x) / np.sum(weights)
        total = np.sum(weights * (y - mean_y) ** 2)
        residual = np.sum(weights * (y - mean_y - slope * (x - mean_x)) ** 2)
        r2 = 0.0 if np.ptp(y) == 0 else float(np.clip(1 - residual / total, 0, 1))
        rows.append((float(slope * annual), r2))
    return pd.DataFrame(rows, index=nav.index[length - 1:], columns=["annualized_log_wls_score", "log_wls_r2"])


def _prices(periods: int = 260) -> pd.DataFrame:
    """A diagnostic path with entry, cooling, and re-entry phases."""
    t = np.arange(periods, dtype=float)
    micro_return = 0.002 + 0.0008 * np.sin(t / 5)
    micro_return[65:110] += 0.035 * np.where(np.arange(45) % 2 == 0, 1.0, -1.0)
    micro_return[155:185] -= 0.013
    hedge_return = 0.0004 + 0.0002 * np.cos(t / 4)
    return pd.DataFrame({"microcap": 100 * np.cumprod(1 + micro_return), "hedge": 200 * np.cumprod(1 + hedge_return)}, index=pd.bdate_range("2025-01-02", periods=periods))


def _events(prices: pd.DataFrame, timing: str = "close") -> pd.DataFrame:
    positions = [3, 31, 48, 84, 135, 214]
    rebalance = prices.index[positions]
    execute = rebalance if timing == "close" else prices.index[[i + 1 for i in positions]]
    return pd.DataFrame({"rebalance_date": rebalance, "execution_date": execute, "execution_timing": timing, "two_side_cost_rate": [0.006, 0.003, 0.009, 0.004, 0.006, 0.008]})


def _independent_fee_map(dates: pd.DatetimeIndex, events: pd.DataFrame) -> np.ndarray:
    fees = np.zeros(len(dates))
    for row in events.to_dict("records"):
        explicit = row.get("execution_date")
        has_explicit = explicit is not None and pd.notna(explicit)
        date = pd.Timestamp(explicit if has_explicit else row["rebalance_date"])
        if has_explicit or row.get("execution_timing", "next_open") == "close":
            if len(dates) and date < dates[0]:
                continue
            eligible = dates[dates >= date]
        else:
            eligible = dates[dates > date]
        if len(eligible):
            fees[dates.get_loc(eligible[0])] += float(row["two_side_cost_rate"])
    return fees


def _independent_ledger(module, prices: pd.DataFrame, dates: pd.DatetimeIndex, events: pd.DataFrame, *, timing: str = "close") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute price returns, WLS state, volatility defense and actual fees separately."""
    micro_return = prices.microcap / prices.microcap.shift(1) - 1
    hedge_return = prices.hedge / prices.hedge.shift(1) - 1
    signal_return = micro_return.fillna(0)
    if module is v23:
        signal_return = signal_return - hedge_return.fillna(0) - 0.0003
    signal_nav = pd.Series(np.cumprod(1 + signal_return.to_numpy()), index=prices.index)
    scores = _pairwise_wls(signal_nav, module.LOOKBACK, module.HALFLIFE, module.TRADING_DAYS).loc[dates]
    signal_subset = signal_nav.loc[dates].to_numpy()
    feature_return = np.r_[0.0, signal_subset[1:] / signal_subset[:-1] - 1]
    features = np.array([np.std(feature_return[i - 9:i + 1], ddof=1) * math.sqrt(244) if i >= 9 else np.nan for i in range(len(dates))])
    mapped_fees = _independent_fee_map(dates, events)
    base_state = False
    active = False
    previous_active = False
    risk = False
    capital = 1.0
    rows = []
    active_label = "long_microcap_short_zz1000" if module is v23 else "long_microcap_top100"
    for i, date in enumerate(dates):
        score = float(scores.at[date, "annualized_log_wls_score"])
        if module is v23:
            base_next = score >= -0.08 if base_state else score > 0.0
            feature = features[i]
            if risk:
                next_active = bool(base_next and feature <= 0.20)
                next_risk = not feature <= 0.20
            else:
                next_risk = bool(base_next and feature >= 0.26)
                next_active = bool(base_next and not next_risk)
            base_state = bool(base_next)
            raw = float(micro_return.at[date]) - 0.8 * float(hedge_return.at[date]) - 0.00024 if active else 0.0
        else:
            next_active = score > 0.0
            next_risk = False
            raw = float(micro_return.at[date]) if active else 0.0
        cost_before, cost_after = (active, next_active) if timing == "close" else (previous_active, active)
        transition = 0.003 if cost_before != cost_after else 0.0
        rebalance = mapped_fees[i] if cost_before and cost_after else 0.0
        cost = transition + rebalance
        net = (1 + raw) * (1 - cost) - 1
        capital = capital * (1 + net)
        rows.append({"holding": active_label if active else "cash", "next_holding": active_label if next_active else "cash", "signal_on": bool(next_active), "overlay_pre_cost_return": raw, "entry_exit_cost": transition, "rebalance_cost": rebalance, "total_cost": cost, "return_net": net, "nav_net": capital, "current_execution_scale": float(active), "risk_before": risk, "feature": features[i]})
        previous_active = active
        active = bool(next_active)
        risk = bool(next_risk)
    return pd.DataFrame(rows, index=dates), scores


def _independent_v20_ledger(prices: pd.DataFrame, dates: pd.DatetimeIndex, events: pd.DataFrame) -> pd.DataFrame:
    micro_return = prices.microcap / prices.microcap.shift(1) - 1
    hedge_return = prices.hedge / prices.hedge.shift(1) - 1
    gap = (prices.microcap / prices.microcap.shift(16) - 1) - (prices.hedge / prices.hedge.shift(16) - 1)
    fees = _independent_fee_map(dates, events)
    active = False
    capital = 1.0
    rows = []
    for i, date in enumerate(dates):
        score = float(gap.at[date])
        next_active = bool(score >= 0 if active else score > 0)
        raw = float(micro_return.at[date]) - 0.8 * float(hedge_return.at[date]) - 0.00024 if active else 0.0
        transition = 0.003 if active != next_active else 0.0
        rebalance = fees[i] if active and next_active else 0.0
        cost = transition + rebalance
        net = (1 + raw) * (1 - cost) - 1
        capital *= 1 + net
        rows.append({"holding": "long_microcap_short_zz1000" if active else "cash", "next_holding": "long_microcap_short_zz1000" if next_active else "cash", "signal_on": next_active, "overlay_pre_cost_return": raw, "entry_exit_cost": transition, "rebalance_cost": rebalance, "total_cost": cost, "return_net": net, "nav_net": capital, "current_execution_scale": float(active)})
        active = next_active
    return pd.DataFrame(rows, index=dates)


def _v20_result(module, prices: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    gross = module.base_mod.run_signal(prices)
    gross = module.base_mod.apply_momentum_gap_exit_buffer(gross, module.V2_0_MOMENTUM_GAP_EXIT_BUFFER)
    return module.overlay_mod.apply_v2_0_execution(gross, events)


def test_v20_fixed_exposure_math_against_independent_daily_ledger() -> None:
    assert v20.V2_0_MOMENTUM_GAP_EXIT_BUFFER == 0
    assert not v20.TARGET_VOL_ENABLED and not v20.OVERHEAT_ENABLED
    assert not v20.base_mod.REQUIRE_POSITIVE_MICROCAP_MOM
    prices = _prices()
    events = _events(prices)
    actual = _v20_result(v20, prices, events)
    expected = _independent_v20_ledger(prices, actual.index, events)
    pd.testing.assert_frame_equal(actual[FIELDS], expected[FIELDS], check_dtype=False, rtol=0, atol=2e-12)


@pytest.mark.parametrize("module", [v23, v25], ids=["v23", "v25"])
@pytest.mark.parametrize("level", [1e-200, 1.0, 1e200])
@pytest.mark.parametrize("slope", [-0.002, 0.0, 0.002, 1e-13])
def test_wls_against_75_digit_pairwise_reference(module, level: float, slope: float) -> None:
    t = np.arange(module.LOOKBACK + 6)
    nav = pd.Series(level * np.exp(slope * t), index=pd.bdate_range("2025-01-02", periods=len(t)))
    original = nav.copy(deep=True)
    actual = module.log_wls_score_and_r2(nav).iloc[-1]
    expected = _decimal_regression(nav.to_numpy()[-module.LOOKBACK:], module.HALFLIFE, module.TRADING_DAYS)
    assert abs(actual.annualized_log_wls_score - expected[0]) < 3e-12
    if slope == 0:
        assert actual.annualized_log_wls_score == 0.0
        assert actual.log_wls_r2 == 0.0
    elif abs(slope) > 1e-10:
        assert abs(actual.log_wls_r2 - expected[1]) < 1e-12
    else:
        assert np.sign(actual.annualized_log_wls_score) == np.sign(expected[0])
    pd.testing.assert_series_equal(nav, original, check_exact=True)


@pytest.mark.parametrize("module", [v23, v25], ids=["v23", "v25"])
def test_entire_formal_execution_stack_against_independent_daily_ledger(module) -> None:
    prices = _prices()
    events = _events(prices)
    actual = module.build_v2_3_result(prices, events) if module is v23 else module.build_v2_5_result(prices, events)
    expected, _ = _independent_ledger(module, prices, actual.index, events)
    pd.testing.assert_frame_equal(actual[FIELDS], expected[FIELDS], check_dtype=False, rtol=0, atol=2e-12)
    if module is v23:
        assert actual.overheat_exit_triggered.sum() > 0
        assert actual.overheat_reentry_triggered.sum() > 0
        assert actual.loc[actual.overheat_risk_off & actual.holding.eq("cash"), "overlay_pre_cost_return"].eq(0).all()
    assert actual.loc[actual.holding.eq("cash") & actual.next_holding.ne("cash"), "entry_exit_cost"].eq(0.003).all()


def test_v25_next_open_entry_exit_costs_preserve_legal_cash_exit_fee() -> None:
    prices = _prices()
    events = _events(prices, "next_open")
    actual = v25.build_v2_5_result(prices, events)
    expected, _ = _independent_ledger(v25, prices, actual.index, events, timing="next_open")
    pd.testing.assert_frame_equal(actual[FIELDS], expected[FIELDS], check_dtype=False, rtol=0, atol=2e-12)
    cash_fee = actual.holding.eq("cash") & actual.entry_exit_cost.gt(0)
    assert cash_fee.any()
    assert actual.loc[cash_fee, "overlay_pre_cost_return"].eq(0).all()
    assert actual.loc[cash_fee, "return_net"].lt(0).all()


@pytest.mark.parametrize("module", [v23, v25], ids=["v23", "v25"])
@pytest.mark.parametrize("cut", [54, 69, 90, 111, 147, 185, 225])
def test_prefix_causality_with_future_mutation_and_fee_events(module, cut: int) -> None:
    prices = _prices()
    events = _events(prices)
    method = module.build_v2_3_result if module is v23 else module.build_v2_5_result
    full = method(prices, events)
    prefix = method(prices.iloc[:cut], events)
    changed = prices.copy()
    changed.iloc[cut:, 0] *= np.linspace(1, 50, len(changed) - cut)
    changed.iloc[cut:, 1] *= np.linspace(1, 0.5, len(changed) - cut)
    altered = method(changed, events)
    pd.testing.assert_frame_equal(prefix[FIELDS], full.loc[prefix.index, FIELDS], check_exact=True)
    pd.testing.assert_frame_equal(prefix[FIELDS], altered.loc[prefix.index, FIELDS], check_exact=True)


@pytest.mark.parametrize("cost", [0.0, 0.003, 0.8, np.nextafter(1.0, 0.0)])
def test_v25_legal_cash_fees_remain_legal(cost: float) -> None:
    frame = pd.DataFrame({"return_net": [-cost], "total_cost": [cost], "overlay_pre_cost_return": [0.0], "holding": ["cash"], "next_holding": ["long_microcap_top100"]}, index=pd.bdate_range("2025-01-02", periods=1))
    actual = v25.apply_no_target_vol(frame)
    assert actual.return_net.iloc[0] == -cost
    assert actual.nav_net.iloc[0] > 0
    assert actual.current_execution_scale.iloc[0] == 0


def test_v25_cost_model_cannot_drop_a_gross_session(monkeypatch: pytest.MonkeyPatch) -> None:
    prices = _prices()
    gross = v25.build_microcap_log_wls_gross(prices)
    cost_module = v25.v2_0.base_mod.freq_mod.cost_mod
    original = cost_module.apply_cost_model
    monkeypatch.setattr(cost_module, "apply_cost_model", lambda frame, events: original(frame, events).drop(index=frame.index[15]))
    with pytest.raises(RuntimeError, match="index|session|aligned"):
        v25.apply_cost(gross, _events(prices))


def test_v25_cost_model_component_contradiction_cannot_hide_an_entry_fee(monkeypatch: pytest.MonkeyPatch) -> None:
    prices = _prices()
    gross = v25.build_microcap_log_wls_gross(prices)
    cost_module = v25.v2_0.base_mod.freq_mod.cost_mod
    original = cost_module.apply_cost_model

    def omit_total(frame: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
        out = original(frame, events)
        date = out.index[out.entry_exit_cost.gt(0)][0]
        out.at[date, "total_cost"] = 0.0
        out.at[date, "return_net"] = out.at[date, "return"]
        out["nav_net"] = (1 + out.return_net).cumprod()
        return out

    monkeypatch.setattr(cost_module, "apply_cost_model", omit_total)
    with pytest.raises((RuntimeError, ValueError), match="cost|fee"):
        v25.apply_no_target_vol(v25.apply_cost(gross, _events(prices)))


@pytest.mark.parametrize("mutation", ["duplicate", "reverse", "extra"])
def test_v25_cost_model_requires_exact_full_chronological_index(monkeypatch: pytest.MonkeyPatch, mutation: str) -> None:
    prices = _prices()
    gross = v25.build_microcap_log_wls_gross(prices)
    cost_module = v25.v2_0.base_mod.freq_mod.cost_mod
    original = cost_module.apply_cost_model

    def mutate(frame: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
        out = original(frame, events)
        if mutation == "duplicate":
            return pd.concat([out.iloc[:1], out])
        if mutation == "reverse":
            return out.iloc[::-1]
        extra = out.iloc[-1:].copy()
        extra.index = pd.DatetimeIndex([out.index[-1] + pd.offsets.BDay(1)])
        return pd.concat([out, extra])

    monkeypatch.setattr(cost_module, "apply_cost_model", mutate)
    with pytest.raises(RuntimeError, match="index|session|aligned"):
        v25.apply_cost(gross, _events(prices))


@pytest.mark.parametrize("column", ["entry_exit_cost", "rebalance_cost", "total_cost"])
def test_v25_upstream_cost_model_cannot_omit_a_required_fee_component(monkeypatch: pytest.MonkeyPatch, column: str) -> None:
    prices = _prices()
    gross = v25.build_microcap_log_wls_gross(prices)
    cost_module = v25.v2_0.base_mod.freq_mod.cost_mod
    original = cost_module.apply_cost_model
    monkeypatch.setattr(cost_module, "apply_cost_model", lambda frame, events: original(frame, events).drop(columns=[column]))
    with pytest.raises((RuntimeError, ValueError), match="cost|columns"):
        v25.apply_cost(gross, _events(prices))


@pytest.mark.parametrize("column", ["entry_exit_cost", "rebalance_cost"])
@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf, -0.001, "fee-corrupt"])
def test_v25_upstream_bad_fee_components_fail_closed(monkeypatch: pytest.MonkeyPatch, column: str, bad: object) -> None:
    prices = _prices()
    gross = v25.build_microcap_log_wls_gross(prices)
    cost_module = v25.v2_0.base_mod.freq_mod.cost_mod
    original = cost_module.apply_cost_model

    def mutate(frame: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
        out = original(frame, events)
        out[column] = out[column].astype(object)
        out.at[out.index[10], column] = bad
        return out

    monkeypatch.setattr(cost_module, "apply_cost_model", mutate)
    with pytest.raises((RuntimeError, ValueError), match="cost|fee"):
        v25.apply_cost(gross, _events(prices))


@pytest.mark.parametrize("timing", ["close", "next_open"])
def test_v25_legal_upstream_fee_components_survive_the_contract(timing: str) -> None:
    prices = _prices()
    gross = v25.build_microcap_log_wls_gross(prices)
    events = _events(prices, timing)
    expected = v25.v2_0.base_mod.freq_mod.cost_mod.apply_cost_model(gross, events)
    actual = v25.apply_cost(gross, events)
    columns = ["entry_exit_cost", "rebalance_cost", "total_cost", "return_net", "nav_net"]
    pd.testing.assert_frame_equal(actual[columns], expected[columns], check_exact=True)
    assert actual.entry_exit_cost.gt(0).any()
    assert actual.rebalance_cost.gt(0).any()


@pytest.mark.parametrize("module", [v23, v25], ids=["v23", "v25"])
def test_formal_score_equality_boundary_preserves_each_version_state_machine(monkeypatch: pytest.MonkeyPatch, module) -> None:
    prices = _prices().iloc[:60]
    values = [0.01, 0.0, -0.079, -0.08, np.nextafter(-0.08, -np.inf), 0.0, np.nextafter(0.0, np.inf)]
    dates = prices.index[module.LOOKBACK - 1:module.LOOKBACK - 1 + len(values)]

    def scores(nav: pd.Series, *args, **kwargs) -> pd.DataFrame:
        out = pd.DataFrame({"annualized_log_wls_score": 0.0, "log_wls_r2": 0.0}, index=nav.index)
        out.loc[dates, "annualized_log_wls_score"] = values
        return out

    monkeypatch.setattr(module, "log_wls_score_and_r2", scores)
    actual = module.build_spread_log_wls_gross(prices, dates) if module is v23 else module.build_microcap_log_wls_gross(prices, dates)
    expected = [True, True, True, True, False, False, True] if module is v23 else [True, False, False, False, False, False, True]
    assert actual.next_holding.ne("cash").tolist() == expected
    assert actual.holding.ne("cash").tolist() == [False] + expected[:-1]


def _load_module(name: str, source: Path):
    spec = importlib.util.spec_from_file_location(name, source)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_original_stack():
    backup = ROOT / ".codex_backups" / "20261007_143135"
    old_v20 = _load_module("math_original_v2_0", backup / "microcap_top100_mom16_biweekly_live_v2_0.py")
    normal_name = "microcap_top100_mom16_biweekly_live_v2_0"
    saved = sys.modules[normal_name]
    sys.modules[normal_name] = old_v20
    try:
        old_v23 = _load_module("math_original_v2_3", backup / "microcap_top100_mom16_biweekly_live_v2_3.py")
        old_v25 = _load_module("math_original_v2_5", backup / "microcap_top100_mom16_biweekly_live_v2_5.py")
        old_v23.v2_0._load()
        old_v25.v2_0._load()
    finally:
        sys.modules[normal_name] = saved
    assert old_v23.v2_0._load() is old_v20 and old_v25.v2_0._load() is old_v20
    return old_v20, old_v23, old_v25


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _comparison(left: pd.DataFrame, right: pd.DataFrame, columns: list[str], *, tolerance: float = 2e-12) -> dict:
    assert left.index.equals(right.index), "full chronological index differs"
    result = {}
    for column in columns:
        if pd.api.types.is_numeric_dtype(left[column]) and pd.api.types.is_numeric_dtype(right[column]):
            a = left[column].to_numpy(dtype=float)
            b = right[column].to_numpy(dtype=float)
            assert np.isfinite(a).all() and np.isfinite(b).all(), column
            delta = np.abs(a - b)
            result[column] = {"max_absolute_difference": float(delta.max()), "different_beyond_tolerance": int(np.count_nonzero(delta > tolerance))}
            assert not (delta > tolerance).any(), (column, delta.max())
        else:
            count = int(left[column].ne(right[column]).sum())
            result[column] = {"different_rows": count}
            assert count == 0, column
    return result


def real_audit() -> None:
    freshness = json.loads((REPORT / "baseline_and_freshness.json").read_text(encoding="utf-8"))
    artifacts = freshness["artifacts"]
    paths = {name: Path(row["path"]) for name, row in artifacts.items()}
    before = {name: _sha256(path) for name, path in paths.items()}
    source_paths = {name: ROOT / name for name in ["microcap_top100_mom16_biweekly_live.py", "microcap_top100_mom16_biweekly_live_v2_0.py", "microcap_top100_mom16_biweekly_live_v2_3.py", "microcap_top100_mom16_biweekly_live_v2_5.py"]}
    sources_before = {name: _sha256(path) for name, path in source_paths.items()}
    assert before == {name: row["raw_sha256"] for name, row in artifacts.items()}
    target = pd.Timestamp(freshness["latest_completed_session"])
    reads = {name: pd.read_csv(path, encoding="utf-8-sig") for name, path in paths.items()}
    for name, frame in reads.items():
        assert len(frame) == artifacts[name]["rows"]
        column = "rebalance_date" if name == "turnover" else "date"
        assert str(pd.to_datetime(frame[column]).max().date()) == artifacts[name]["latest_date"]
    close_all = v20.base_mod.load_close_df(paths["panel"], paths["proxy_index"], max_date=target)
    base_gross = v20.base_mod.run_signal(close_all).sort_index()
    close = v25._close_df_from_base(base_gross)
    events = reads["turnover"].copy()
    for column in ["rebalance_date", "execution_date", "effective_date", "return_start_date"]:
        if column in events:
            events[column] = pd.to_datetime(events[column], format="mixed", errors="raise")
    assert events.execution_timing.eq("close").all()
    official_index = pd.DatetimeIndex(pd.to_datetime(reads["v20_nav"].date))
    old_v20, old_v23, old_v25 = _load_original_stack()
    original_close = old_v20.base_mod.load_close_df(paths["panel"], paths["proxy_index"], max_date=target)
    original_gross = old_v20.base_mod.run_signal(original_close).sort_index()
    original_input = old_v25._close_df_from_base(original_gross)
    pd.testing.assert_frame_equal(close, original_input, check_exact=True)
    report = {"scope": "read_only_second_arithmetic_audit_diagnostic", "target": str(target.date()), "independent_method": "pairwise WLS covariance identity plus 75-digit Decimal spot checks; independent price/state/fee ledger; original entire v20/v23/v25 stack", "versions": {}, "freshness": artifacts}
    actual_v20 = _v20_result(v20, close, events)
    old_v20_result = _v20_result(old_v20, original_input, events)
    official_v20 = reads["v20_nav"].copy()
    official_v20.index = pd.DatetimeIndex(pd.to_datetime(official_v20.pop("date")))
    official_v20.index.name = actual_v20.index.name
    ledger_v20 = _independent_v20_ledger(close, actual_v20.index, events)
    report["versions"]["v2.0"] = {"rows": len(actual_v20), "first_date": str(actual_v20.index[0].date()), "last_date": str(actual_v20.index[-1].date()), "source_sha256": _sha256(ROOT / "microcap_top100_mom16_biweekly_live_v2_0.py"), "original_source_sha256": _sha256(ROOT / ".codex_backups" / "20261007_143135" / "microcap_top100_mom16_biweekly_live_v2_0.py"), "comparisons": {"original_entire_stack": _comparison(actual_v20, old_v20_result, FIELDS, tolerance=0), "official_csv": _comparison(actual_v20, official_v20, FIELDS), "independent_ledger": _comparison(actual_v20, ledger_v20, FIELDS)}, "cash_pre_cost_zero": bool(actual_v20.loc[actual_v20.holding.eq("cash"), "overlay_pre_cost_return"].eq(0).all())}
    for module, old, suffix in [(v23, old_v23, "23"), (v25, old_v25, "25")]:
        build = module.build_v2_3_result if module is v23 else module.build_v2_5_result
        old_build = old.build_v2_3_result if module is v23 else old.build_v2_5_result
        common = module.build_v2_3_common_index(close, official_index) if module is v23 else module.build_v2_5_common_index(close, official_index)
        actual = build(close, events, common)
        original = old_build(original_input, events, common)
        official = reads[f"v{suffix}_costed_nav"].copy()
        official.index = pd.DatetimeIndex(pd.to_datetime(official.pop("date")))
        official.index.name = actual.index.name
        ledger, independent_scores = _independent_ledger(module, close, actual.index, events)
        comparisons = {"original_entire_stack": _comparison(actual, original, FIELDS, tolerance=0), "official_csv": _comparison(actual, official, FIELDS), "independent_ledger": _comparison(actual, ledger, FIELDS)}
        score_difference = np.abs(actual.annualized_log_wls_score - independent_scores.annualized_log_wls_score)
        assert not np.sign(actual.annualized_log_wls_score).ne(np.sign(independent_scores.annualized_log_wls_score)).any()
        signal_nav = module.always_on_spread_nav(close)[0] if module is v23 else module.microcap_nav(close)[0]
        spot_dates = pd.DatetimeIndex(actual.annualized_log_wls_score.abs().nsmallest(16).index).union(actual.index[[0, 500, 1500, 3000, len(actual) - 1]])
        spots = []
        for date in spot_dates:
            end = signal_nav.index.get_loc(date)
            decimal_score, decimal_r2 = _decimal_regression(signal_nav.iloc[end + 1 - module.LOOKBACK:end + 1].to_numpy(), module.HALFLIFE, module.TRADING_DAYS)
            score = float(actual.at[date, "annualized_log_wls_score"])
            r2 = float(actual.at[date, "log_wls_r2"])
            assert abs(score - decimal_score) < 3e-12
            assert abs(r2 - decimal_r2) < 3e-12
            assert np.sign(score) == np.sign(decimal_score)
            spots.append({"date": str(date.date()), "production_score": score, "decimal_score": decimal_score, "score_error": score - decimal_score, "r2_error": r2 - decimal_r2})
        prefix_rows = []
        for count in [40, 79, 137, 503, 1207, 2511, 3967]:
            cut_date = actual.index[min(count, len(actual) - 1)]
            truncated = close.loc[:cut_date]
            prefix_common = common[common <= cut_date]
            prefix = build(truncated, events, prefix_common)
            prefix_rows.append({"date": str(cut_date.date()), "rows": len(prefix), "comparison": _comparison(prefix, actual.loc[prefix.index], FIELDS, tolerance=0)})
        no_cost_growth = np.cumprod(1 + actual.overlay_pre_cost_return.to_numpy())
        assert (actual.nav_net.to_numpy() <= no_cost_growth + 3e-12).all()
        spots_path = REPORT / f"math_v{suffix}_decimal_spots.csv"
        pd.DataFrame(spots).to_csv(spots_path, index=False, encoding="utf-8-sig")
        report["versions"][f"v2.{suffix[-1]}"] = {"rows": len(actual), "first_date": str(actual.index[0].date()), "last_date": str(actual.index[-1].date()), "source_sha256": _sha256(ROOT / f"microcap_top100_mom16_biweekly_live_v2_{suffix[-1]}.py"), "original_source_sha256": _sha256(ROOT / ".codex_backups" / "20261007_143135" / f"microcap_top100_mom16_biweekly_live_v2_{suffix[-1]}.py"), "comparisons": comparisons, "all_sample_pairwise_max_score_error": float(score_difference.max()), "score_sign_changes": 0, "decimal_spots": len(spots), "prefix": prefix_rows, "cash_fee_days": int((actual.holding.eq("cash") & actual.total_cost.gt(0)).sum()), "cash_pre_cost_zero": bool(actual.loc[actual.holding.eq("cash"), "overlay_pre_cost_return"].eq(0).all()), "costed_below_same_path_no_cost": True}
    after = {name: _sha256(path) for name, path in paths.items()}
    assert before == after
    assert sources_before == {name: _sha256(path) for name, path in source_paths.items()}, "production source changed during audit"
    report["source_sha256"] = sources_before
    report["production_sources_stable_during_audit"] = True
    report["official_artifacts_unchanged"] = True
    report["result"] = "PASS"
    (REPORT / "math_real_independent.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"result": "PASS", "official_artifacts_unchanged": True, "versions": {name: {key: value for key, value in row.items() if key not in {"comparisons", "prefix"}} for name, row in report["versions"].items()}}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    if sys.argv[1:] == ["--real"]:
        real_audit()
    else:
        raise SystemExit("Use pytest for synthetic diagnostics, or --real for the read-only official-sample audit.")
