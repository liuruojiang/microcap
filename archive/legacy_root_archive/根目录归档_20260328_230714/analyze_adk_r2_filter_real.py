import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

import compare_strategy_versions as compare


ROOT = Path(__file__).resolve().parent
SCRIPT = ROOT / "mnt_bot V 6.1 plus.py"
OUT_JSON = ROOT / "adk_r2_filter_real_summary.json"
OUT_CSV = ROOT / "adk_r2_filter_real_metrics.csv"
OUT_MD = ROOT / "mnt_adk_r2_filter_real_20260328.md"

DK_R2_WINDOW = 20
DK_R2_THRESHOLD = 0.30


def calc_metrics(ret: pd.Series):
    ret = ret.dropna()
    if len(ret) < 2:
        return None
    nav = (1 + ret).cumprod()
    total_return = nav.iloc[-1] / nav.iloc[0] - 1.0
    years = (nav.index[-1] - nav.index[0]).days / 365.25
    annual_return = (nav.iloc[-1] / nav.iloc[0]) ** (1.0 / years) - 1.0 if years > 0 else np.nan
    annual_vol = ret.std(ddof=1) * math.sqrt(252) if len(ret) > 1 else np.nan
    sharpe = annual_return / annual_vol if pd.notna(annual_vol) and annual_vol > 0 else np.nan
    max_dd = ((nav - nav.cummax()) / nav.cummax()).min()
    return {
        "start": nav.index[0].strftime("%Y-%m-%d"),
        "end": nav.index[-1].strftime("%Y-%m-%d"),
        "days": int(len(nav)),
        "total_return": float(total_return),
        "annual_return": float(annual_return),
        "annual_vol": float(annual_vol) if pd.notna(annual_vol) else None,
        "sharpe": float(sharpe) if pd.notna(sharpe) else None,
        "max_drawdown": float(max_dd),
    }


def slice_ret(ret: pd.Series, years: int | None):
    ret = ret.dropna()
    if years is None:
        return ret
    end = ret.index[-1]
    start = end - pd.DateOffset(years=years)
    return ret[ret.index >= start]


def combine_returns(mod, return_map: dict[str, pd.Series]):
    all_dates = sorted(set().union(*(s.index for s in return_map.values() if len(s) > 0)))
    ret_df = pd.DataFrame({k: v.reindex(pd.DatetimeIndex(all_dates)) for k, v in return_map.items()})
    avail = ret_df.notna().astype(float)
    for col in avail.columns:
        avail[col] *= mod.COMBINED_WEIGHTS.get(col, 0.0)
    weight_sum = avail.sum(axis=1).replace(0, np.nan)
    weights = avail.div(weight_sum, axis=0)
    combined = (ret_df.fillna(0.0) * weights).sum(axis=1)
    return combined.dropna()


def build_r2_filtered_runner(mod):
    base_single = mod._run_single_pair_dk

    def _single_with_r2(a_prices, b_prices):
        ret, abs_bias_mom, pair_df = base_single(a_prices, b_prices)
        if ret is None or pair_df is None:
            return ret, abs_bias_mom, pair_df
        d = pair_df.copy()
        spread_cum = d["spread_ret"].cumsum()
        r2_vals = mod.rolling_r2_fast(spread_cum, DK_R2_WINDOW)
        r2_filter = (((r2_vals < DK_R2_THRESHOLD) & r2_vals.notna()).shift(1).fillna(False)).astype(bool)
        d["r2"] = r2_vals
        d["r2_filtered"] = r2_filter
        d.loc[r2_filter, "strategy_ret"] = 0.0
        return d["strategy_ret"], d["bias_mom"].abs(), d

    def _run(cn_close, cn_dk_close):
        old = mod._run_single_pair_dk
        try:
            mod._run_single_pair_dk = _single_with_r2
            return mod.run_dk_strategy(cn_close, cn_dk_close)
        finally:
            mod._run_single_pair_dk = old

    return _run


def summarize_pair_filter_days(dk_result: pd.DataFrame):
    pair_data = dk_result.attrs.get("pair_data", {})
    total = 0
    filtered = 0
    per_pair = {}
    for pair, df in pair_data.items():
        if "r2_filtered" not in df.columns:
            continue
        cnt = int(df["r2_filtered"].fillna(False).sum())
        obs = int(df["r2_filtered"].notna().sum())
        per_pair[pair] = {
            "filtered_days": cnt,
            "observed_days": obs,
            "filtered_pct": (cnt / obs * 100.0) if obs else 0.0,
        }
        total += obs
        filtered += cnt
    return {
        "filtered_days_total": filtered,
        "observed_days_total": total,
        "filtered_pct_total": (filtered / total * 100.0) if total else 0.0,
        "per_pair": per_pair,
    }


def main():
    mod = compare.load_module(SCRIPT, "mod_adk_r2_real")
    cn_close, cn_close_with_bond, cn_dk_close = compare.load_cn_data()
    us_rot_close, us_prod_daily = compare.load_us_data(mod)

    cn_result = mod.run_cn_strategy(cn_close_with_bond.copy(), mod.CN_EQUITY_CODES)
    us_rot_result = mod.run_us_rotation(
        us_rot_close.copy(),
        mod.US_ROT_POOL,
        btc_ticker=mod.US_ROT_BTC_TICKER,
        btc_start=mod.US_ROT_BTC_START,
        btc_max_w=mod.US_ROT_BTC_MAX_W,
    )
    if getattr(mod, "US_ROT_VOLREG_ENABLED", False) and "SPY" in us_rot_close.columns:
        us_rot_result = mod.apply_vol_regime_overlay(us_rot_result, us_rot_close["SPY"])
    subc_daily_ret = compare.compute_subc_outputs(mod, us_prod_daily.copy())

    dk_no_r2 = mod.run_dk_strategy(cn_close.copy(), cn_dk_close.copy())
    dk_with_r2 = build_r2_filtered_runner(mod)(cn_close.copy(), cn_dk_close.copy())

    return_map_base = {
        "Sub-A": cn_result["return"].dropna(),
        "Sub-A-DK": dk_no_r2["return"].dropna(),
        "Sub-B": us_rot_result["return"].dropna(),
        "Sub-C": subc_daily_ret.dropna(),
    }
    return_map_r2 = {
        "Sub-A": cn_result["return"].dropna(),
        "Sub-A-DK": dk_with_r2["return"].dropna(),
        "Sub-B": us_rot_result["return"].dropna(),
        "Sub-C": subc_daily_ret.dropna(),
    }

    combined_no_r2 = combine_returns(mod, return_map_base)
    combined_with_r2 = combine_returns(mod, return_map_r2)

    periods = [("最长", None), ("10年", 10), ("5年", 5), ("2年", 2)]
    rows = []
    summary = {
        "script": str(SCRIPT),
        "dk_r2_window": DK_R2_WINDOW,
        "dk_r2_threshold": DK_R2_THRESHOLD,
        "weights": dict(mod.COMBINED_WEIGHTS),
        "filter_stats": summarize_pair_filter_days(dk_with_r2),
        "periods": {},
    }

    series_map = {
        "ADK_当前无R2过滤": dk_no_r2["return"].dropna(),
        "ADK_加回R2过滤": dk_with_r2["return"].dropna(),
        "组合_当前无R2过滤": combined_no_r2,
        "组合_加回R2过滤": combined_with_r2,
    }

    for period_name, years in periods:
        summary["periods"][period_name] = {}
        for label, ret in series_map.items():
            sliced = slice_ret(ret, years)
            metrics = calc_metrics(sliced)
            if not metrics:
                continue
            summary["periods"][period_name][label] = metrics
            rows.append({
                "period": period_name,
                "series": label,
                "start": metrics["start"],
                "end": metrics["end"],
                "days": metrics["days"],
                "total_return_pct": metrics["total_return"] * 100,
                "annual_return_pct": metrics["annual_return"] * 100,
                "annual_vol_pct": metrics["annual_vol"] * 100 if metrics["annual_vol"] is not None else np.nan,
                "sharpe": metrics["sharpe"],
                "max_drawdown_pct": metrics["max_drawdown"] * 100,
            })

    df = pd.DataFrame(rows)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    with OUT_MD.open("w", encoding="utf-8") as f:
        f.write("# ADK R²过滤真实脚本对照\n\n")
        f.write(f"- 脚本: `{SCRIPT.name}`\n")
        f.write(f"- 数据: 根目录本地 `mnt_strategy_data_cn.csv` / `mnt_strategy_data_us.csv` / `.cn_official_cache`\n")
        f.write(f"- 对照: 当前 `ADK 无R²过滤` vs 按历史口径加回 `R²({DK_R2_WINDOW}) < {DK_R2_THRESHOLD} -> 次日收益置零`\n\n")
        f.write("## 过滤统计\n\n")
        fs = summary["filter_stats"]
        f.write(f"- 总观测天数: {fs['observed_days_total']}\n")
        f.write(f"- 被R²过滤天数: {fs['filtered_days_total']}\n")
        f.write(f"- 过滤占比: {fs['filtered_pct_total']:.2f}%\n\n")
        f.write("## 分段结果\n\n")
        f.write("| 分段 | 序列 | 年化 | 波动 | Sharpe | 最大回撤 |\n")
        f.write("|:-|:-|:-|:-|:-|:-|\n")
        for _, row in df.iterrows():
            sharpe = "—" if pd.isna(row["sharpe"]) else f"{row['sharpe']:.3f}"
            f.write(
                f"| {row['period']} | {row['series']} | {row['annual_return_pct']:.2f}% | "
                f"{row['annual_vol_pct']:.2f}% | {sharpe} | {row['max_drawdown_pct']:.2f}% |\n"
            )

    print(f"JSON: {OUT_JSON}")
    print(f"CSV: {OUT_CSV}")
    print(f"MD: {OUT_MD}")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
