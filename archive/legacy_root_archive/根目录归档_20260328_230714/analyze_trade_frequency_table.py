import json

import numpy as np
import pandas as pd

import analyze_combined_drawdown_throttle as combined_dd
import analyze_combined_multisleeve_guard as multisleeve_guard
import analyze_subb_leverage_scenarios as lev


EPS = 1e-12


def annualize_count(count, years):
    return float(count / years) if years > 0 else None


def series_changed(series):
    return series.ne(series.shift(1)).fillna(False)


def build_prod_inputs(mod, us_prod_daily):
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

    monthly_ret = prod_monthly.pct_change().dropna(how="all")
    cash_ret = monthly_ret[mod.PROD_CASH]
    return prod_monthly, monthly_ret, cash_ret, prod_sig_a, prod_sig_b


def analyze_suba(cn_result, start, end):
    df = cn_result.loc[(cn_result.index >= start) & (cn_result.index <= end)].copy()
    years = (df.index[-1] - df.index[0]).days / 365.25
    structural = df["is_signal"].fillna(False)
    scale_only = (~structural) & df["holding"].ne("cash") & df["weight"].diff().abs().gt(EPS)
    total = structural | scale_only

    prev_holding = df["holding"].shift(1).fillna("cash")
    structural_legs = np.where(
        structural,
        np.where((prev_holding == "cash") | (df["holding"] == "cash"), 1, 2),
        0,
    )

    return {
        "sample_start": str(df.index[0].date()),
        "sample_end": str(df.index[-1].date()),
        "years": years,
        "structural_trade_days": int(structural.sum()),
        "structural_trade_days_per_year": annualize_count(int(structural.sum()), years),
        "scale_trade_days": int(scale_only.sum()),
        "scale_trade_days_per_year": annualize_count(int(scale_only.sum()), years),
        "total_trade_days": int(total.sum()),
        "total_trade_days_per_year": annualize_count(int(total.sum()), years),
        "structural_leg_count": int(np.sum(structural_legs)),
        "scale_turnover_units": float(df.loc[scale_only, "weight"].diff().abs().fillna(0.0).sum()),
        "notes": "Sub-A counts holding switches separately from vol-scaling-only adjustments.",
    }


def analyze_adk(dk_result, start, end):
    df = dk_result.loc[(dk_result.index >= start) & (dk_result.index <= end)].copy()
    years = (df.index[-1] - df.index[0]).days / 365.25
    structural = df["is_signal"].fillna(False)
    scale_only = (~structural) & df["weight"].diff().abs().gt(EPS)
    total = structural | scale_only

    pair_changed = df["pair_changed"].fillna(False)
    direction_changed = df["direction_changed"].fillna(False)

    return {
        "sample_start": str(df.index[0].date()),
        "sample_end": str(df.index[-1].date()),
        "years": years,
        "structural_trade_days": int(structural.sum()),
        "structural_trade_days_per_year": annualize_count(int(structural.sum()), years),
        "pair_change_days": int(pair_changed.sum()),
        "direction_change_days": int(direction_changed.sum()),
        "scale_trade_days": int(scale_only.sum()),
        "scale_trade_days_per_year": annualize_count(int(scale_only.sum()), years),
        "total_trade_days": int(total.sum()),
        "total_trade_days_per_year": annualize_count(int(total.sum()), years),
        "scale_turnover_units": float(df.loc[scale_only, "weight"].diff().abs().fillna(0.0).sum()),
        "notes": "Sub-A-DK counts top-pair or direction changes separately from pure leverage changes.",
    }


def analyze_subb(us_result, start, end):
    df = us_result.loc[(us_result.index >= start) & (us_result.index <= end)].copy()
    years = (df.index[-1] - df.index[0]).days / 365.25
    w_cols = [col for col in df.columns if col.startswith("w_")]
    weight_delta = df[w_cols].fillna(0.0).diff().abs().sum(axis=1).fillna(0.0) / 2.0
    structural = df["rebalanced"].fillna(False)
    scale_only = (~structural) & weight_delta.gt(EPS)
    total = structural | scale_only

    return {
        "sample_start": str(df.index[0].date()),
        "sample_end": str(df.index[-1].date()),
        "years": years,
        "structural_trade_days": int(structural.sum()),
        "structural_trade_days_per_year": annualize_count(int(structural.sum()), years),
        "scale_trade_days": int(scale_only.sum()),
        "scale_trade_days_per_year": annualize_count(int(scale_only.sum()), years),
        "total_trade_days": int(total.sum()),
        "total_trade_days_per_year": annualize_count(int(total.sum()), years),
        "turnover_units": float(weight_delta.sum()),
        "turnover_units_per_year": annualize_count(float(weight_delta.sum()), years),
        "notes": "Under current Sub-B logic, weight changes happen on rebalance days; there are no separate scale-only days.",
    }


def analyze_subc(mod, us_prod_daily, start, end):
    _, monthly_ret, cash_ret, prod_sig_a, prod_sig_b = build_prod_inputs(mod, us_prod_daily)
    _, detail = mod.simulate_prod_btc_phased(
        monthly_ret,
        prod_sig_a,
        cash_ret,
        rebal_month=mod.PROD_REBAL_MONTH,
        sig_b=prod_sig_b,
        blend_a=mod.PROD_BLEND_A,
        commission=mod.PROD_COMMISSION,
    )

    raw = mod._compute_daily_subc_phased(
        us_prod_daily,
        prod_sig_a,
        mod.PROD_CASH,
        prod_sig_b=prod_sig_b,
        blend_a=mod.PROD_BLEND_A,
    )
    _, actual_scale, _ = mod._apply_subc_vol_scaling(raw, us_prod_daily)

    structural_dates = set()
    detail = detail.loc[(detail.index >= start) & (detail.index <= end)].copy()
    if len(detail) > 0:
        structural_dates.update(detail.index[detail.index.month == mod.PROD_REBAL_MONTH].tolist())
        for phase_start in [mod.DBMF_BT_START, mod.BTC_BT_START]:
            phase_candidates = detail.index[detail.index >= phase_start]
            if len(phase_candidates) > 0:
                structural_dates.add(phase_candidates[0])

    actual_scale = actual_scale.loc[(actual_scale.index >= start) & (actual_scale.index <= end)]
    structural = pd.Series(False, index=actual_scale.index)
    if structural_dates:
        mapped_dates = []
        trade_index = actual_scale.index
        for dt in sorted(structural_dates):
            pos = trade_index.searchsorted(dt, side="right") - 1
            if pos >= 0:
                mapped_dates.append(trade_index[pos])
        structural.loc[structural.index.isin(mapped_dates)] = True

    structural = structural.reindex(actual_scale.index).fillna(False)
    scale_only = (~structural) & actual_scale.diff().abs().gt(EPS)
    total = structural | scale_only
    years = (actual_scale.index[-1] - actual_scale.index[0]).days / 365.25

    signal_change_days = 0
    if mod.PROD_USE_TIMING and len(detail) > 0:
        sig_cols = [col for col in detail.columns if col.startswith("sig_")]
        if sig_cols:
            signal_change_days = int(detail[sig_cols].diff().abs().fillna(0.0).sum(axis=1).gt(EPS).sum())

    return {
        "sample_start": str(actual_scale.index[0].date()),
        "sample_end": str(actual_scale.index[-1].date()),
        "years": years,
        "structural_trade_days": int(structural.sum()),
        "structural_trade_days_per_year": annualize_count(int(structural.sum()), years),
        "timing_signal_change_days": signal_change_days,
        "scale_trade_days": int(scale_only.sum()),
        "scale_trade_days_per_year": annualize_count(int(scale_only.sum()), years),
        "total_trade_days": int(total.sum()),
        "total_trade_days_per_year": annualize_count(int(total.sum()), years),
        "scale_turnover_units": float(actual_scale.diff().abs().fillna(0.0).sum()),
        "scale_turnover_units_per_year": annualize_count(float(actual_scale.diff().abs().fillna(0.0).sum()), years),
        "notes": "Sub-C structural days capture annual rebalance plus phase changes; scale days capture vol-scaling adjustments.",
    }


def analyze_combined_guard(run_df, years, label):
    structural = run_df["turnover"].gt(EPS)
    return {
        "years": years,
        "extra_scale_trade_days": int(structural.sum()),
        "extra_scale_trade_days_per_year": annualize_count(int(structural.sum()), years),
        "extra_turnover_units": float(run_df["turnover"].sum()),
        "extra_turnover_units_per_year": annualize_count(float(run_df["turnover"].sum()), years),
        "share_below_full_risk": float((run_df["scale"] < 0.999999).mean()),
        "notes": label,
    }


def main():
    mod = lev.load_main_module()
    cn_close, cn_close_with_bond, cn_dk_close = lev.load_cn_data()
    us_rot_close, us_prod_daily = lev.load_us_prod_daily(mod)

    cn_result = mod.run_cn_strategy(cn_close_with_bond, mod.CN_EQUITY_CODES)
    dk_result = mod.run_dk_strategy(cn_close, cn_dk_close)
    us_base = lev.run_rotation_variant(
        mod,
        us_rot_close,
        target_vol=mod.US_ROT_TARGET_VOL,
        max_lev=mod.US_ROT_MAX_LEV,
        leverage_mode="futures_only",
        finance_spread_bps=0.0,
    )
    us_aggressive = lev.run_rotation_variant(
        mod,
        us_rot_close,
        target_vol=0.25,
        max_lev=2.0,
        leverage_mode="futures_only",
        finance_spread_bps=0.0,
    )

    combined = combined_dd.build_combined_daily_components(mod, cn_result, dk_result, us_base, us_prod_daily)
    start = combined["base_ret"].index[0]
    end = combined["base_ret"].index[-1]
    years = (end - start).days / 365.25
    cash_ret = us_prod_daily["BIL"].pct_change().reindex(combined["base_ret"].index).fillna(0.0)
    weakness = multisleeve_guard.build_sleeve_weakness(combined["nav_parts"], sleeve_dd_threshold=0.06)

    guard_daily = multisleeve_guard.apply_multisleeve_guard(
        combined["base_ret"],
        cash_ret,
        weakness["weak_count"],
        moderate_count=2,
        severe_count=3,
        scale_moderate=0.90,
        scale_severe=0.75,
        recover_count=1,
        step_up=0.02,
        rerisk_frequency="daily",
        rebal_cost_bps=2.0,
    )
    guard_weekly = multisleeve_guard.apply_multisleeve_guard(
        combined["base_ret"],
        cash_ret,
        weakness["weak_count"],
        moderate_count=2,
        severe_count=3,
        scale_moderate=0.90,
        scale_severe=0.75,
        recover_count=1,
        step_up=0.05,
        rerisk_frequency="weekly",
        rebal_cost_bps=2.0,
    )

    output = {
        "common_sample": {
            "start": str(start.date()),
            "end": str(end.date()),
            "years": years,
        },
        "baseline_current": {
            "Sub-A": analyze_suba(cn_result, start, end),
            "Sub-A-DK": analyze_adk(dk_result, start, end),
            "Sub-B_20pct_1.5x": analyze_subb(us_base, start, end),
            "Sub-C": analyze_subc(mod, us_prod_daily, start, end),
        },
        "candidates": {
            "Sub-B_25pct_2.0x": analyze_subb(us_aggressive, start, end),
            "Combined_guard_daily_weakdd6_mild": analyze_combined_guard(
                guard_daily,
                years,
                "Extra portfolio-level scale changes under daily re-risk.",
            ),
            "Combined_guard_weekly_weakdd6_mild": analyze_combined_guard(
                guard_weekly,
                years,
                "Extra portfolio-level scale changes under weekly re-risk.",
            ),
        },
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
