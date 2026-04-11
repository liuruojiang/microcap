import json

import numpy as np
import pandas as pd

import analyze_subb_leverage_scenarios as lev


def build_combined_daily_components(mod, cn_result, cn_dk_result, us_rot_result, us_prod_daily):
    prod_monthly = us_prod_daily.resample("ME").last()
    last_daily = us_prod_daily.index[-1]
    last_monthly_period = prod_monthly.index[-1].to_period("M")
    today_period = pd.Timestamp.now().to_period("M")
    if last_daily.to_period("M") == last_monthly_period == today_period:
        prod_monthly = prod_monthly.iloc[:-1]

    prod_sig_a = mod.make_abs_mom_signals(prod_monthly, mod.PROD_ABS_MOM_LB)
    prod_sig_b = mod.make_sma_signals(prod_monthly, mod.PROD_SMA_WINDOW, mod.PROD_SMA_BAND)
    if not mod.PROD_USE_TIMING:
        prod_sig_a = pd.DataFrame(1.0, index=prod_sig_a.index, columns=prod_sig_a.columns)
        prod_sig_b = prod_sig_a.copy()

    subc_daily = mod._get_subc_daily_ret(us_prod_daily, prod_sig_a, prod_sig_b=prod_sig_b)

    nav_parts = {
        "Sub-A": (1.0 + cn_result["return"]).cumprod(),
        "Sub-A-DK": (1.0 + cn_dk_result["return"]).cumprod(),
        "Sub-B": (1.0 + us_rot_result["return"]).cumprod(),
        "Sub-C": (1.0 + subc_daily).cumprod(),
    }
    nav_parts = {name: series / series.iloc[0] for name, series in nav_parts.items() if len(series) > 1}
    common_start = max(series.index[0] for series in nav_parts.values())
    all_daily_dates = sorted(set().union(*(series.index for series in nav_parts.values())))
    all_daily_dates = [date for date in all_daily_dates if date >= common_start]
    date_index = pd.DatetimeIndex(all_daily_dates)

    nav_df = pd.DataFrame({name: series.reindex(date_index).ffill() for name, series in nav_parts.items()})
    w = mod.COMBINED_WEIGHTS
    wdf = nav_df.notna().astype(float)
    for col in wdf.columns:
        wdf[col] *= w.get(col, 0.0)
    ws = wdf.sum(axis=1).replace(0, np.nan)
    wdf = wdf.div(ws, axis=0)

    nav_comb = (nav_df.fillna(0.0) * wdf).sum(axis=1)
    nav_comb = nav_comb / nav_comb.iloc[0]
    ret_comb = nav_comb.pct_change().dropna()
    return {
        "nav_parts": nav_df,
        "weights_df": wdf,
        "base_nav": nav_comb,
        "base_ret": ret_comb,
    }


def summarize_return_series(mod, ret_series):
    ret = ret_series.dropna()
    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else np.nan
    vol = ret.std(ddof=1) * np.sqrt(252)
    sharpe = annual / vol if pd.notna(vol) and vol > 0 else np.nan
    max_dd = ((nav - nav.cummax()) / nav.cummax()).min()
    summary = {
        "annual_return": float(annual),
        "annual_vol": float(vol),
        "sharpe": float(sharpe),
        "max_drawdown": float(max_dd),
        "total_return": float(nav.iloc[-1] - 1.0),
    }

    segments = {}
    for label, start, end in [
        ("2010_2015", "2010-01-01", "2015-12-31"),
        ("2016_2020", "2016-01-01", "2020-12-31"),
        ("2021_2026", "2021-01-01", "2026-12-31"),
    ]:
        seg = ret.loc[(ret.index >= start) & (ret.index <= end)]
        if len(seg) < 20:
            continue
        seg_nav = (1.0 + seg).cumprod()
        seg_years = (seg.index[-1] - seg.index[0]).days / 365.25
        segments[label] = {
            "annual_return": float(seg_nav.iloc[-1] ** (1.0 / seg_years) - 1.0) if seg_years > 0 else None,
            "annual_vol": float(seg.std(ddof=1) * np.sqrt(252)),
            "sharpe": float((seg_nav.iloc[-1] ** (1.0 / seg_years) - 1.0) / (seg.std(ddof=1) * np.sqrt(252)))
            if seg_years > 0 and seg.std(ddof=1) > 0
            else None,
            "max_drawdown": float(((seg_nav - seg_nav.cummax()) / seg_nav.cummax()).min()),
        }
    summary["segments"] = segments
    return summary


def apply_drawdown_throttle(
    base_ret,
    *,
    dd_moderate,
    dd_severe,
    scale_moderate,
    scale_severe,
    recover_dd,
    step_up,
    rebal_cost_bps=0.0,
):
    base_ret = base_ret.dropna()
    out = pd.DataFrame(index=base_ret.index)
    out["base_return"] = base_ret

    nav = 1.0
    peak = 1.0
    current_scale = 1.0
    daily_cost_per_unit = rebal_cost_bps / 10000.0
    rows = []

    for dt, base_r in base_ret.items():
        prev_dd = nav / peak - 1.0

        if prev_dd <= -dd_severe:
            target_scale = scale_severe
            state = "severe"
        elif prev_dd <= -dd_moderate:
            target_scale = scale_moderate
            state = "moderate"
        elif prev_dd >= -recover_dd:
            target_scale = 1.0
            state = "normal"
        else:
            target_scale = current_scale
            state = "recovery_band"

        if target_scale < current_scale:
            new_scale = target_scale
        elif target_scale > current_scale:
            new_scale = min(current_scale + step_up, target_scale)
        else:
            new_scale = current_scale

        turnover = abs(new_scale - current_scale)
        cost = turnover * daily_cost_per_unit if turnover > 0 else 0.0
        adj_r = new_scale * base_r - cost

        nav *= 1.0 + adj_r
        peak = max(peak, nav)
        current_scale = new_scale

        rows.append(
            {
                "date": dt,
                "return": adj_r,
                "scale": new_scale,
                "target_scale": target_scale,
                "state": state,
                "prev_drawdown": prev_dd,
                "turnover": turnover,
                "cost": cost,
                "nav": nav,
                "peak": peak,
                "drawdown": nav / peak - 1.0,
            }
        )

    return pd.DataFrame(rows).set_index("date")


def summarize_throttle(run_df):
    scale = run_df["scale"]
    states = run_df["state"]
    summary = {
        "avg_scale": float(scale.mean()),
        "median_scale": float(scale.median()),
        "min_scale": float(scale.min()),
        "share_below_1": float((scale < 0.999999).mean()),
        "share_at_severe_scale": float((scale <= scale.min() + 1e-9).mean()),
        "avg_turnover": float(run_df["turnover"].mean()),
        "annualized_turnover": float(run_df["turnover"].sum() / len(run_df) * 252.0),
        "moderate_or_worse_share": float(states.isin(["moderate", "severe", "recovery_band"]).mean()),
        "severe_share": float((states == "severe").mean()),
        "recovery_band_share": float((states == "recovery_band").mean()),
        "n_scale_changes": int((run_df["turnover"] > 0).sum()),
    }

    episode_lengths = []
    current = 0
    for in_risk_off in (scale < 0.999999):
        if in_risk_off:
            current += 1
        elif current > 0:
            episode_lengths.append(current)
            current = 0
    if current > 0:
        episode_lengths.append(current)
    summary["max_risk_off_days"] = int(max(episode_lengths)) if episode_lengths else 0
    summary["n_risk_off_episodes"] = int(len(episode_lengths))
    return summary


def main():
    mod = lev.load_main_module()
    cn_close, cn_close_with_bond, cn_dk_close = lev.load_cn_data()
    us_rot_close, us_prod_daily = lev.load_us_prod_daily(mod)

    cn_result = mod.run_cn_strategy(cn_close_with_bond, mod.CN_EQUITY_CODES)
    cn_dk_result = mod.run_dk_strategy(cn_close, cn_dk_close)
    us_rot_result = lev.run_rotation_variant(
        mod,
        us_rot_close,
        target_vol=mod.US_ROT_TARGET_VOL,
        max_lev=mod.US_ROT_MAX_LEV,
        leverage_mode="futures_only",
        finance_spread_bps=0.0,
    )

    combined = build_combined_daily_components(mod, cn_result, cn_dk_result, us_rot_result, us_prod_daily)
    base_ret = combined["base_ret"]

    scenarios = {
        "baseline": None,
        "mild_7_10": {
            "dd_moderate": 0.07,
            "dd_severe": 0.10,
            "scale_moderate": 0.85,
            "scale_severe": 0.70,
            "recover_dd": 0.04,
            "step_up": 0.01,
            "rebal_cost_bps": 2.0,
        },
        "balanced_6_9": {
            "dd_moderate": 0.06,
            "dd_severe": 0.09,
            "scale_moderate": 0.80,
            "scale_severe": 0.60,
            "recover_dd": 0.03,
            "step_up": 0.01,
            "rebal_cost_bps": 2.0,
        },
        "gradual_5_8": {
            "dd_moderate": 0.05,
            "dd_severe": 0.08,
            "scale_moderate": 0.80,
            "scale_severe": 0.55,
            "recover_dd": 0.025,
            "step_up": 0.005,
            "rebal_cost_bps": 2.0,
        },
        "aggressive_4_7": {
            "dd_moderate": 0.04,
            "dd_severe": 0.07,
            "scale_moderate": 0.75,
            "scale_severe": 0.50,
            "recover_dd": 0.02,
            "step_up": 0.005,
            "rebal_cost_bps": 2.0,
        },
    }

    output = {
        "baseline": summarize_return_series(mod, base_ret),
    }

    for name, cfg in scenarios.items():
        if cfg is None:
            continue
        run_df = apply_drawdown_throttle(base_ret, **cfg)
        output[name] = {
            "performance": summarize_return_series(mod, run_df["return"]),
            "throttle": summarize_throttle(run_df),
            "config": cfg,
        }

    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
