"""Read-only source-to-output replay for frozen v2.3/v2.5 paper streams."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
CUTOFF = pd.Timestamp("2026-09-24")
P = {
    "proxy": ROOT / "outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv",
    "panel": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv",
    "turnover": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv",
    "official_index": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv",
    "base": ROOT / "outputs/microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv",
    "v23": ROOT / "outputs/microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv",
    "v25": ROOT / "outputs/microcap_top100_mom16_lb20_hl3_entry0_exit0_no_targetvol_v2_5_costed_nav.csv",
}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(key: str, date_col: str = "date") -> pd.DataFrame:
    frame = pd.read_csv(P[key])
    frame[date_col] = pd.to_datetime(frame[date_col])
    return frame.loc[frame[date_col].le(CUTOFF)].copy().sort_values(date_col).reset_index(drop=True)


def first_error(index: pd.DatetimeIndex, actual: pd.Series, expected: np.ndarray, tol: float = 1e-9) -> dict:
    a = pd.to_numeric(actual, errors="coerce").to_numpy(float)
    e = np.asarray(expected, dtype=float)
    error = np.abs(a - e)
    wrong = np.flatnonzero((~np.isfinite(error)) | (error > tol))
    return {
        "mismatch_count": int(len(wrong)),
        "first_date": str(index[wrong[0]].date()) if len(wrong) else None,
        "actual": float(a[wrong[0]]) if len(wrong) else None,
        "expected": float(e[wrong[0]]) if len(wrong) else None,
        "max_abs_error": float(np.nanmax(error)) if len(error) else None,
    }


def first_label_error(index: pd.DatetimeIndex, actual: pd.Series, expected: list[str] | list[bool]) -> dict:
    a = actual.astype(str).str.lower().to_numpy()
    e = np.asarray([str(x).lower() for x in expected])
    wrong = np.flatnonzero(a != e)
    return {
        "mismatch_count": int(len(wrong)),
        "first_date": str(index[wrong[0]].date()) if len(wrong) else None,
        "actual": str(a[wrong[0]]) if len(wrong) else None,
        "expected": str(e[wrong[0]]) if len(wrong) else None,
    }


def score(nav: np.ndarray, n: int, half: float) -> tuple[np.ndarray, np.ndarray]:
    age = np.arange(n - 1, -1, -1, dtype=float)
    w = 0.5 ** (age / half)
    w /= w.sum()
    x = np.arange(n, dtype=float)
    xbar = float(w @ x)
    xc = x - xbar
    den = float(w @ (xc * xc))
    y = np.log(nav)
    slopes = np.full(len(nav), np.nan)
    r2 = np.full(len(nav), np.nan)
    for j in range(n - 1, len(nav)):
        yw = y[j - n + 1:j + 1]
        if not np.isfinite(yw).all():
            continue
        ym = float(w @ yw)
        yc = yw - ym
        b = float((w * xc) @ yc / den)
        fitted = ym + b * xc
        tot = float(w @ (yc * yc))
        resid = float(w @ ((yw - fitted) ** 2))
        slopes[j] = b * 244
        r2[j] = np.clip(1 - resid / tot, 0, 1) if tot > 0 else 0
    return slopes, r2


def prepare() -> tuple[pd.DataFrame, dict, pd.DataFrame]:
    proxy = read("proxy")[["date", "close", "holding_effective"]]
    panel = read("panel")[["date", "1.000852"]]
    close = proxy.merge(panel, on="date", how="inner", validate="one_to_one").sort_values("date")
    close = close.loc[close.holding_effective.astype(bool)].reset_index(drop=True)
    # The embedded base signal drops its 16-day warmup; its first close is the
    # source of the spread/microcap NAV normalization, not the first proxy close.
    base_start = read("base").date.min()
    close = close.loc[close.date.ge(base_start)].reset_index(drop=True)
    close = close.rename(columns={"close": "microcap", "1.000852": "hedge"})
    assert close.date.is_unique and (close[["microcap", "hedge"]] > 0).all().all()
    dates = pd.DatetimeIndex(close.date)
    close["mr"] = close.microcap.pct_change(fill_method=None).fillna(0.0)
    close["hr"] = close.hedge.pct_change(fill_method=None).fillna(0.0)
    official_dates = pd.DatetimeIndex(read("official_index").date)
    turnover = read("turnover", "rebalance_date")
    turnover["execution_date"] = pd.to_datetime(turnover.execution_date, format="mixed")
    cost = turnover.set_index("execution_date").two_side_cost_rate.astype(float).to_dict()
    assert dates.is_unique and official_dates.is_unique and turnover.execution_date.is_unique
    return close, cost, turnover


def replay_v25(close: pd.DataFrame, cost: dict, saved: pd.DataFrame) -> dict:
    dates = pd.DatetimeIndex(close.date)
    micro_nav = np.cumprod(1 + close.mr.to_numpy(float))
    scores, r2 = score(micro_nav, 20, 3.0)
    idx = pd.DatetimeIndex(saved.date)
    expected_idx = dates[(dates >= pd.Timestamp("2010-05-05")) & np.isfinite(scores)].intersection(pd.DatetimeIndex(read("official_index").date))
    assert idx.equals(expected_idx), "v2.5 saved date index differs from raw-signal/official index intersection"
    pos = dates.get_indexer(idx)
    assert (pos >= 0).all()
    assert idx.min() == pd.Timestamp("2010-05-05")
    checks = {
        "microcap_close": first_error(idx, saved.microcap_close, close.microcap.iloc[pos].to_numpy()),
        "microcap_ret": first_error(idx, saved.microcap_ret, close.mr.iloc[pos].to_numpy()),
        "microcap_nav": first_error(idx, saved.microcap_nav, micro_nav[pos]),
        "annualized_log_wls_score": first_error(idx, saved.annualized_log_wls_score, scores[pos]),
        "log_wls_r2": first_error(idx, saved.log_wls_r2, r2[pos]),
    }
    active = False
    hold, nex, gross, fees, entry_exit, rebalance, net, navs = [], [], [], [], [], [], [], []
    nav = 1.0
    for j, dt in enumerate(idx):
        mret = float(close.mr.iloc[pos[j]])
        before = active
        after = bool(scores[pos[j]] > 0)
        r = mret if before else 0.0
        e = .003 if before != after else 0.0
        b = float(cost.get(dt, 0.0)) if before and after else 0.0
        f = e + b
        n = (1 + r) * (1 - f) - 1
        nav *= 1 + n
        hold.append("long_microcap_top100" if before else "cash")
        nex.append("long_microcap_top100" if after else "cash")
        gross.append(r); fees.append(f); entry_exit.append(e); rebalance.append(b); net.append(n); navs.append(nav)
        active = after
    checks.update({
        "holding": first_label_error(idx, saved.holding, hold),
        "next_holding": first_label_error(idx, saved.next_holding, nex),
        "base_pre_cost_return": first_error(idx, saved.base_pre_cost_return, gross),
        "entry_exit_cost": first_error(idx, saved.entry_exit_cost, entry_exit),
        "rebalance_cost": first_error(idx, saved.rebalance_cost, rebalance),
        "total_cost": first_error(idx, saved.total_cost, fees),
        "return_net": first_error(idx, saved.return_net, net),
        "nav_net": first_error(idx, saved.nav_net, navs),
        "fault_next_state_as_current": first_error(idx, saved.base_pre_cost_return, np.where(np.asarray(nex) != "cash", close.mr.iloc[pos].to_numpy(), 0)),
        "fault_missing_fee": first_error(idx, saved.return_net, gross),
        "fault_fee_delayed": first_error(idx, saved.return_net, (1 + np.asarray(gross)) * (1 - np.r_[0, np.asarray(fees)[:-1]]) - 1),
    })
    return {"rows": len(idx), "first": str(idx[0].date()), "last": str(idx[-1].date()), "active_days": int(sum(x != "cash" for x in hold)), "fee_days": int(sum(x > 0 for x in fees)), "checks": checks}


def replay_v23(close: pd.DataFrame, cost: dict, saved: pd.DataFrame) -> dict:
    dates = pd.DatetimeIndex(close.date)
    mr, hr = close.mr.to_numpy(float), close.hr.to_numpy(float)
    signal_return = mr - hr - .0003
    spread_nav = np.cumprod(1 + signal_return)
    scores, r2 = score(spread_nav, 25, 2.5)
    idx = pd.DatetimeIndex(saved.date)
    expected_idx = dates[(dates >= pd.Timestamp("2010-05-05")) & np.isfinite(scores)].intersection(pd.DatetimeIndex(read("official_index").date))
    assert idx.equals(expected_idx), "v2.3 saved date index differs from raw-signal/official index intersection"
    pos = dates.get_indexer(idx)
    assert (pos >= 0).all() and idx.min() == pd.Timestamp("2010-05-05")
    checks = {
        "microcap_close": first_error(idx, saved.microcap_close, close.microcap.iloc[pos].to_numpy()),
        "hedge_close": first_error(idx, saved.hedge_close, close.hedge.iloc[pos].to_numpy()),
        "spread_nav": first_error(idx, saved.spread_nav, spread_nav[pos]),
        "annualized_log_wls_score": first_error(idx, saved.annualized_log_wls_score, scores[pos]),
        "log_wls_r2": first_error(idx, saved.log_wls_r2, r2[pos]),
    }
    base_prev = False
    base_hold, base_next, base_gross = [], [], []
    for j in pos:
        s = scores[j]
        after = bool(s >= -.08) if base_prev else bool(s > 0)
        base_hold.append("long_microcap_short_zz1000" if base_prev else "cash")
        base_next.append("long_microcap_short_zz1000" if after else "cash")
        base_gross.append(float(mr[j] - .8 * hr[j] - .00024) if base_prev else 0.0)
        base_prev = after
    checks["base_holding"] = first_label_error(idx, saved.base_holding, base_hold)
    checks["base_next_holding"] = first_label_error(idx, saved.base_next_holding, base_next)
    # Feature is computed on the formal common index, as in the production overlay.
    spread_slice = pd.Series(spread_nav[pos], index=idx)
    feature = spread_slice.pct_change(fill_method=None).fillna(0.0).rolling(10).std(ddof=1).mul(math.sqrt(244)).to_numpy()
    checks["overheat_feature_value"] = first_error_allow_nan(idx, saved.overheat_feature_value, feature)
    active = False; risk_off = False; nav = 1.0
    hold=[]; nex=[]; gross=[]; fees=[]; entry_exit=[]; rebalance=[]; net=[]; navs=[]; risk=[]; exits=[]; reentries=[]; blocked=[]
    for k, dt in enumerate(idx):
        hot = np.isfinite(feature[k]) and feature[k] >= .26
        cool = np.isfinite(feature[k]) and feature[k] <= .20
        desired = base_next[k] != "cash"
        was_off = risk_off
        exit_trigger = reentry = block = False
        if risk_off:
            if cool:
                risk_off = False
                reentry = desired
            else:
                desired = False
        elif desired and hot:
            exit_trigger = active
            block = not active
            desired = False
            risk_off = True
        raw = base_gross[k] if active else 0.0
        e = .003 if active != desired else 0.0
        b = float(cost.get(dt, 0.0)) if active and desired else 0.0
        f = e + b
        n = (1 + raw) * (1 - f) - 1
        nav *= 1 + n
        hold.append("long_microcap_short_zz1000" if active else "cash")
        nex.append("long_microcap_short_zz1000" if desired else "cash")
        gross.append(raw); fees.append(f); entry_exit.append(e); rebalance.append(b); net.append(n); navs.append(nav)
        risk.append(was_off); exits.append(exit_trigger); reentries.append(reentry); blocked.append(block)
        active = desired
    checks.update({
        "holding": first_label_error(idx, saved.holding, hold),
        "next_holding": first_label_error(idx, saved.next_holding, nex),
        "overheat_risk_off": first_label_error(idx, saved.overheat_risk_off, risk),
        "overheat_exit_triggered": first_label_error(idx, saved.overheat_exit_triggered, exits),
        "overheat_reentry_triggered": first_label_error(idx, saved.overheat_reentry_triggered, reentries),
        "overheat_block_entry_triggered": first_label_error(idx, saved.overheat_block_entry_triggered, blocked),
        "base_pre_cost_return": first_error(idx, saved.base_pre_cost_return, gross),
        "entry_exit_cost": first_error(idx, saved.entry_exit_cost, entry_exit),
        "rebalance_cost": first_error(idx, saved.rebalance_cost, rebalance),
        "total_cost": first_error(idx, saved.total_cost, fees),
        "return_net": first_error(idx, saved.return_net, net),
        "nav_net": first_error(idx, saved.nav_net, navs),
        "fault_next_state_as_current": first_error(idx, saved.base_pre_cost_return, np.where(np.asarray(nex) != "cash", mr[pos] - .8 * hr[pos] - .00024, 0)),
        "fault_missing_fee": first_error(idx, saved.return_net, gross),
        "fault_fee_delayed": first_error(idx, saved.return_net, (1 + np.asarray(gross)) * (1 - np.r_[0, np.asarray(fees)[:-1]]) - 1),
        "fault_overheat_ignored": first_label_error(idx, saved.next_holding, base_next),
        "fault_reentry_suppressed": first_label_error(idx, saved.overheat_reentry_triggered, [False] * len(idx)),
    })
    return {"rows": len(idx), "first": str(idx[0].date()), "last": str(idx[-1].date()), "base_active_days": int(sum(x != "cash" for x in base_hold)), "active_days": int(sum(x != "cash" for x in hold)), "overheat_exit_days": int(sum(exits)), "overheat_reentry_days": int(sum(reentries)), "blocked_entry_days": int(sum(blocked)), "fee_days": int(sum(x > 0 for x in fees)), "checks": checks}


def first_error_allow_nan(index, actual, expected, tol=1e-9):
    a = pd.to_numeric(actual, errors="coerce").to_numpy(float)
    e = np.asarray(expected, float)
    wrong = np.flatnonzero(~((np.isnan(a) & np.isnan(e)) | (np.abs(a-e) <= tol)))
    finite = np.abs(a[np.isfinite(a) & np.isfinite(e)] - e[np.isfinite(a) & np.isfinite(e)])
    return {"mismatch_count": int(len(wrong)), "first_date": str(index[wrong[0]].date()) if len(wrong) else None, "actual": float(a[wrong[0]]) if len(wrong) else None, "expected": float(e[wrong[0]]) if len(wrong) else None, "max_abs_error": float(finite.max()) if len(finite) else None}


def drawdown_probe(saved: pd.DataFrame) -> list[dict]:
    series = saved.set_index("date").return_net.astype(float)
    end = series.index.max()
    out = []
    for label, years in (("full", None), ("10y", 10), ("5y", 5), ("3y", 3), ("1y", 1)):
        part = series if years is None else series.loc[series.index >= end - pd.DateOffset(years=years)]
        nav = (1 + part).cumprod()
        old = float((nav / nav.cummax() - 1).min())
        fixed = float((nav / nav.cummax().clip(lower=1) - 1).min())
        out.append({"window": label, "rows": len(part), "first": str(part.index[0].date()), "first_return": float(part.iloc[0]), "old_mdd_pct": old*100, "anchored_mdd_pct": fixed*100, "delta_percentage_points": (fixed-old)*100})
    injected = pd.Series([-.1, .05], index=pd.to_datetime(["2000-01-03", "2000-01-04"]))
    nav = (1 + injected).cumprod()
    old = float((nav / nav.cummax() - 1).min())
    fixed = float((nav / nav.cummax().clip(lower=1) - 1).min())
    assert old == 0 and abs(fixed + .1) < 1e-12
    out.append({"window": "injected_first_day_loss", "rows": 2, "first": "2000-01-03", "first_return": -.1, "old_mdd_pct": old*100, "anchored_mdd_pct": fixed*100, "delta_percentage_points": (fixed-old)*100})
    for year, part in series.groupby(series.index.year):
        nav = (1 + part).cumprod()
        old = float((nav / nav.cummax() - 1).min())
        fixed = float((nav / nav.cummax().clip(lower=1) - 1).min())
        if abs(old-fixed) > 1e-12:
            bad = part.loc[part.lt(0)]
            out.append({"window": f"year_{year}", "rows": len(part), "first": str(part.index[0].date()), "first_return": float(part.iloc[0]), "first_negative_date": str(bad.index[0].date()) if len(bad) else None, "old_mdd_pct": old*100, "anchored_mdd_pct": fixed*100, "delta_percentage_points": (fixed-old)*100})
    return out


def main() -> None:
    close, cost, turnover = prepare()
    saved23, saved25 = read("v23"), read("v25")
    for key, frame in (("v23", saved23), ("v25", saved25)):
        assert frame.date.is_unique and frame.date.is_monotonic_increasing, key
    result = {
        "cutoff": str(CUTOFF.date()),
        "files": {name: {"path": str(path.relative_to(ROOT)), "sha256": sha(path), "rows_through_cutoff": len(read(name, "rebalance_date" if name == "turnover" else "date"))} for name, path in P.items()},
        "raw_aligned_close": {"rows": len(close), "first": str(close.date.iloc[0].date()), "last": str(close.date.iloc[-1].date())},
        "turnover": {"rows": len(turnover), "first": str(turnover.rebalance_date.iloc[0].date()), "last": str(turnover.rebalance_date.iloc[-1].date())},
        "v23": replay_v23(close, cost, saved23),
        "v25": replay_v25(close, cost, saved25),
        "v25_drawdown_anchor": drawdown_probe(saved25),
    }
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "replay.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    for key in ("v23", "v25"):
        bad = {name: v for name, v in result[key]["checks"].items() if not name.startswith("fault_") and v["mismatch_count"]}
        print(key, "rows", result[key]["rows"], "bad", bad)
        faults = {name: v["first_date"] for name, v in result[key]["checks"].items() if name.startswith("fault_")}
        print(key, "fault_first", faults)
    print("v25_drawdown_anchor", result["v25_drawdown_anchor"])


if __name__ == "__main__":
    main()
