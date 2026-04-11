import json

import numpy as np
import pandas as pd

import analyze_adk_pair_retention as adk_ret
import analyze_subb_leverage_scenarios as lev


EPS = 1e-12


def annualize_count(count, years):
    return float(count / years) if years > 0 else None


def build_top2_selection(signals_df):
    shifted = signals_df.shift(1)
    selections = []

    for _, row in shifted.iterrows():
        row = row.dropna()
        if len(row) == 0:
            selections.append(())
            continue
        top_pairs = tuple(row.nlargest(2).index.tolist())
        selections.append(top_pairs)

    return pd.Series(selections, index=signals_df.index, name="top_pairs")


def build_top2_result(base_dk_result):
    signals_df = base_dk_result.attrs["signals_df"]
    pair_data = base_dk_result.attrs["pair_data"]
    common_idx = base_dk_result.index
    top2_series = build_top2_selection(signals_df.reindex(common_idx)).reindex(common_idx)

    returns = []
    avg_scale = []
    avg_scale_raw = []
    avg_realized_vol = []
    direction_map = []

    for dt, pairs in top2_series.items():
        if not pairs:
            returns.append(0.0)
            avg_scale.append(1.0)
            avg_scale_raw.append(1.0)
            avg_realized_vol.append(np.nan)
            direction_map.append({})
            continue

        pair_returns = []
        pair_scales = []
        pair_scales_raw = []
        pair_rv = []
        dirs = {}
        for pair in pairs:
            if pair not in pair_data or dt not in pair_data[pair].index:
                continue
            row = pair_data[pair].loc[dt]
            pair_returns.append(float(row["strategy_ret"]))
            pair_scales.append(float(row["scale"]) if "scale" in row.index and not pd.isna(row["scale"]) else 1.0)
            pair_scales_raw.append(float(row["scale_raw"]) if "scale_raw" in row.index and not pd.isna(row["scale_raw"]) else pair_scales[-1])
            pair_rv.append(float(row["realized_vol"]) if "realized_vol" in row.index and not pd.isna(row["realized_vol"]) else np.nan)
            dirs[pair] = int(row["signal"]) if "signal" in row.index and not pd.isna(row["signal"]) else 0

        if len(pair_returns) == 0:
            returns.append(0.0)
            avg_scale.append(1.0)
            avg_scale_raw.append(1.0)
            avg_realized_vol.append(np.nan)
            direction_map.append({})
        else:
            returns.append(float(np.mean(pair_returns)))
            avg_scale.append(float(np.mean(pair_scales)))
            avg_scale_raw.append(float(np.mean(pair_scales_raw)))
            avg_realized_vol.append(float(np.nanmean(pair_rv)) if np.any(pd.notna(pair_rv)) else np.nan)
            direction_map.append(dirs)

    df = pd.DataFrame(
        {
            "return": pd.Series(returns, index=common_idx),
            "nav": (1.0 + pd.Series(returns, index=common_idx)).cumprod(),
            "top_pairs": top2_series,
            "weight": avg_scale,
            "scale_raw": avg_scale_raw,
            "realized_vol": avg_realized_vol,
        },
        index=common_idx,
    )

    prev_pairs = top2_series.shift(1)
    pair_set_changed = pd.Series(False, index=common_idx)
    direction_changed = pd.Series(False, index=common_idx)
    scale_only_changed = pd.Series(False, index=common_idx)

    prev_dirs = {}
    prev_scales = {}
    for i, dt in enumerate(common_idx):
        pairs = tuple(top2_series.iloc[i])
        dirs = direction_map[i]
        cur_pair_set = set(pairs)
        prev_pair_set = set(prev_pairs.iloc[i]) if i > 0 and isinstance(prev_pairs.iloc[i], tuple) else set()
        if i > 0:
            pair_set_changed.iloc[i] = cur_pair_set != prev_pair_set
            shared = cur_pair_set & prev_pair_set
            if shared:
                direction_changed.iloc[i] = any(dirs.get(p) != prev_dirs.get(p) for p in shared if p in dirs and p in prev_dirs)
                scale_only_changed.iloc[i] = (
                    (not pair_set_changed.iloc[i])
                    and (not direction_changed.iloc[i])
                    and any(abs(df.at[dt, "weight"] - prev_scales.get("avg_weight", df.at[dt, "weight"])) > EPS for _ in [0])
                )
        prev_dirs = dirs
        prev_scales = {"avg_weight": df.at[dt, "weight"]}

    pair_set_changed.iloc[0] = False
    direction_changed.iloc[0] = False
    scale_only_changed.iloc[0] = False
    df["pair_set_changed"] = pair_set_changed
    df["direction_changed"] = direction_changed
    df["is_signal"] = pair_set_changed | direction_changed
    df["holding"] = top2_series.astype(str)
    df.attrs.update(base_dk_result.attrs)
    return df


def analyze_top2_frequency(df, start, end):
    seg = df.loc[(df.index >= start) & (df.index <= end)].copy()
    years = (seg.index[-1] - seg.index[0]).days / 365.25
    structural = seg["is_signal"].fillna(False)
    scale_only = (~structural) & seg["weight"].diff().abs().gt(EPS)
    total = structural | scale_only

    return {
        "sample_start": str(seg.index[0].date()),
        "sample_end": str(seg.index[-1].date()),
        "years": years,
        "structural_trade_days": int(structural.sum()),
        "structural_trade_days_per_year": annualize_count(int(structural.sum()), years),
        "pair_set_change_days": int(seg["pair_set_changed"].sum()),
        "direction_change_days": int(seg["direction_changed"].sum()),
        "scale_trade_days": int(scale_only.sum()),
        "scale_trade_days_per_year": annualize_count(int(scale_only.sum()), years),
        "total_trade_days": int(total.sum()),
        "total_trade_days_per_year": annualize_count(int(total.sum()), years),
        "scale_turnover_units": float(seg.loc[scale_only, "weight"].diff().abs().fillna(0.0).sum()),
    }


def summarize_set_behavior(df):
    spells = []
    current = None
    n = 0
    for pairs in df["top_pairs"]:
        if pairs == current:
            n += 1
        else:
            if current is not None:
                spells.append(n)
            current = pairs
            n = 1
    if current is not None:
        spells.append(n)

    return {
        "pair_set_change_days": int(df["pair_set_changed"].sum()),
        "direction_change_days": int(df["direction_changed"].sum()),
        "median_pair_set_spell_days": float(pd.Series(spells).median()) if spells else None,
    }


def main():
    mod = lev.load_main_module()
    cn_close, cn_close_with_bond, cn_dk_close = lev.load_cn_data()
    us_rot_close, us_prod_daily = lev.load_us_prod_daily(mod)

    cn_result = mod.run_cn_strategy(cn_close_with_bond, mod.CN_EQUITY_CODES)
    dk_base = mod.run_dk_strategy(cn_close, cn_dk_close)
    dk_top2 = build_top2_result(dk_base)
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
            "trade_frequency": trade_freq_baseline(dk_base, start, end),
            "pair_behavior": adk_ret.summarize_pair_behavior(dk_base.loc[(dk_base.index >= start) & (dk_base.index <= end)]),
            "combined": lev.compute_combined_summary(
                mod,
                us_rot_result=us_rot_result,
                cn_result=cn_result,
                cn_dk_result=dk_base,
                us_prod_daily=us_prod_daily,
            ),
        },
        "top2_equal_weight": {
            "adk": adk_ret.calc_metrics(mod, dk_top2.loc[(dk_top2.index >= start) & (dk_top2.index <= end), "return"]),
            "trade_frequency": analyze_top2_frequency(dk_top2, start, end),
            "pair_behavior": summarize_set_behavior(dk_top2.loc[(dk_top2.index >= start) & (dk_top2.index <= end)]),
            "combined": lev.compute_combined_summary(
                mod,
                us_rot_result=us_rot_result,
                cn_result=cn_result,
                cn_dk_result=dk_top2.rename(columns={"top_pairs": "top_pair"}),
                us_prod_daily=us_prod_daily,
            ),
        },
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


def trade_freq_baseline(dk_base, start, end):
    import analyze_trade_frequency_table as trade_freq

    return trade_freq.analyze_adk(dk_base, start, end)


if __name__ == "__main__":
    main()
