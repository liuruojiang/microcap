import json

import numpy as np
import pandas as pd

import analyze_adk_pair_retention as adk_ret
import analyze_subb_leverage_scenarios as lev
import analyze_trade_frequency_table as trade_freq


def build_weekly_confirmed_top_pair(signals_df):
    shifted = signals_df.shift(1)
    chosen = []
    current = None

    idx = shifted.index
    week_last = set()
    week_best = {}
    for i, dt in enumerate(idx):
        yr, wk, _ = dt.isocalendar()
        key = (int(yr), int(wk))
        week_best[key] = i
    week_last = set(week_best.values())

    for i, (_, row) in enumerate(shifted.iterrows()):
        row = row.dropna()
        if len(row) == 0:
            chosen.append("none")
            current = None
            continue

        if current is None:
            current = row.idxmax()
        elif i in week_last:
            current = row.idxmax()

        chosen.append(current)

    return pd.Series(chosen, index=signals_df.index, name="top_pair")


def build_dk_result_weekly_confirm(mod, base_dk_result):
    signals_df = base_dk_result.attrs["signals_df"]
    pair_data = base_dk_result.attrs["pair_data"]
    common_idx = base_dk_result.index

    top_pair_series = build_weekly_confirmed_top_pair(signals_df.reindex(common_idx)).reindex(common_idx)
    top_dir_series = []
    rets = []
    weights = []
    scale_raw = []
    realized_vol = []

    for dt, pair in top_pair_series.items():
        if pair == "none" or pair not in pair_data or dt not in pair_data[pair].index:
            top_dir_series.append(0)
            rets.append(0.0)
            weights.append(1.0)
            scale_raw.append(1.0)
            realized_vol.append(np.nan)
            continue
        row = pair_data[pair].loc[dt]
        sig_val = row["signal"] if "signal" in row.index and not pd.isna(row["signal"]) else 0
        top_dir_series.append(int(sig_val))
        rets.append(float(row["strategy_ret"]))
        weights.append(float(row["scale"]) if "scale" in row.index and not pd.isna(row["scale"]) else 1.0)
        scale_raw.append(float(row["scale_raw"]) if "scale_raw" in row.index and not pd.isna(row["scale_raw"]) else weights[-1])
        realized_vol.append(float(row["realized_vol"]) if "realized_vol" in row.index and not pd.isna(row["realized_vol"]) else np.nan)

    top_dir_series = pd.Series(top_dir_series, index=common_idx, name="direction")
    pair_changed = top_pair_series.ne(top_pair_series.shift(1))
    direction_changed = top_dir_series.ne(top_dir_series.shift(1))
    is_signal = pair_changed | direction_changed
    pair_changed.iloc[0] = False
    direction_changed.iloc[0] = False
    is_signal.iloc[0] = False

    pair_a = []
    pair_b = []
    long_leg = []
    short_leg = []
    for p, d in zip(top_pair_series.tolist(), top_dir_series.tolist()):
        if p == "none" or d == 0:
            pair_a.append(None)
            pair_b.append(None)
            long_leg.append(None)
            short_leg.append(None)
            continue
        a, b = p.split("/")
        pair_a.append(a)
        pair_b.append(b)
        if d == 1:
            long_leg.append(a)
            short_leg.append(b)
        else:
            long_leg.append(b)
            short_leg.append(a)

    result = pd.DataFrame(
        {
            "return": pd.Series(rets, index=common_idx),
            "nav": (1.0 + pd.Series(rets, index=common_idx)).cumprod(),
            "top_pair": top_pair_series,
            "direction": top_dir_series,
            "holding": [f"{p}_{d}" for p, d in zip(top_pair_series.tolist(), top_dir_series.tolist())],
            "pair_a": pair_a,
            "pair_b": pair_b,
            "long_leg": long_leg,
            "short_leg": short_leg,
            "pair_changed": pair_changed,
            "direction_changed": direction_changed,
            "is_signal": is_signal,
            "target": None,
            "weight": weights,
            "scale_raw": scale_raw,
            "realized_vol": realized_vol,
        },
        index=common_idx,
    )
    result.attrs.update(base_dk_result.attrs)
    result.attrs["pair_confirmation"] = "weekly_last_trading_day"
    return result


def main():
    mod = lev.load_main_module()
    cn_close, cn_close_with_bond, cn_dk_close = lev.load_cn_data()
    us_rot_close, us_prod_daily = lev.load_us_prod_daily(mod)

    cn_result = mod.run_cn_strategy(cn_close_with_bond, mod.CN_EQUITY_CODES)
    dk_base = mod.run_dk_strategy(cn_close, cn_dk_close)
    dk_weekly = build_dk_result_weekly_confirm(mod, dk_base)
    us_rot_result = lev.run_rotation_variant(
        mod,
        us_rot_close,
        target_vol=mod.US_ROT_TARGET_VOL,
        max_lev=mod.US_ROT_MAX_LEV,
        leverage_mode="futures_only",
        finance_spread_bps=0.0,
    )

    start = pd.Timestamp("2010-11-23")
    end = pd.Timestamp("2026-03-27")

    output = {
        "baseline": {
            "adk": adk_ret.calc_metrics(mod, dk_base.loc[(dk_base.index >= start) & (dk_base.index <= end), "return"]),
            "trade_frequency": trade_freq.analyze_adk(dk_base, start, end),
            "pair_behavior": adk_ret.summarize_pair_behavior(dk_base.loc[(dk_base.index >= start) & (dk_base.index <= end)]),
            "combined": lev.compute_combined_summary(
                mod,
                us_rot_result=us_rot_result,
                cn_result=cn_result,
                cn_dk_result=dk_base,
                us_prod_daily=us_prod_daily,
            ),
        },
        "weekly_confirm": {
            "adk": adk_ret.calc_metrics(mod, dk_weekly.loc[(dk_weekly.index >= start) & (dk_weekly.index <= end), "return"]),
            "trade_frequency": trade_freq.analyze_adk(dk_weekly, start, end),
            "pair_behavior": adk_ret.summarize_pair_behavior(dk_weekly.loc[(dk_weekly.index >= start) & (dk_weekly.index <= end)]),
            "combined": lev.compute_combined_summary(
                mod,
                us_rot_result=us_rot_result,
                cn_result=cn_result,
                cn_dk_result=dk_weekly,
                us_prod_daily=us_prod_daily,
            ),
        },
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
