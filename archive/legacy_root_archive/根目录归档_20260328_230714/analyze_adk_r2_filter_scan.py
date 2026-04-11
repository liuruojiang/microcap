import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

import compare_strategy_versions as compare


ROOT = Path(__file__).resolve().parent
SCRIPT = ROOT / "mnt_bot V 6.1 plus.py"
OUT_CSV = ROOT / "adk_r2_filter_scan_results.csv"
OUT_JSON = ROOT / "adk_r2_filter_scan_summary.json"
OUT_MD = ROOT / "mnt_adk_r2_filter_scan_20260328.md"

WINDOWS = [10, 15, 20, 30, 40]
THRESHOLDS = [0.10, 0.20, 0.25, 0.30, 0.35, 0.40]
PERIODS = [("最长", None), ("10年", 10), ("5年", 5), ("2年", 2)]


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


def build_r2_filtered_runner(mod, window: int, threshold: float):
    base_single = mod._run_single_pair_dk

    def _single_with_r2(a_prices, b_prices):
        ret, abs_bias_mom, pair_df = base_single(a_prices, b_prices)
        if ret is None or pair_df is None:
            return ret, abs_bias_mom, pair_df
        d = pair_df.copy()
        spread_cum = d["spread_ret"].cumsum()
        r2_vals = mod.rolling_r2_fast(spread_cum, window)
        r2_mask = ((r2_vals < threshold) & r2_vals.notna()).shift(1)
        r2_filter = pd.Series(r2_mask.to_numpy(dtype=bool, na_value=False), index=d.index)
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


def summarize_filter_stats(dk_result: pd.DataFrame):
    pair_data = dk_result.attrs.get("pair_data", {})
    total = 0
    filtered = 0
    for _, df in pair_data.items():
        if "r2_filtered" not in df.columns:
            continue
        cnt = int(df["r2_filtered"].fillna(False).sum())
        obs = int(df["r2_filtered"].notna().sum())
        total += obs
        filtered += cnt
    return filtered, total, (filtered / total * 100.0) if total else 0.0


def main():
    mod = compare.load_module(SCRIPT, "mod_adk_r2_scan")
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

    dk_base = mod.run_dk_strategy(cn_close.copy(), cn_dk_close.copy())
    combined_base = combine_returns(
        mod,
        {
            "Sub-A": cn_result["return"].dropna(),
            "Sub-A-DK": dk_base["return"].dropna(),
            "Sub-B": us_rot_result["return"].dropna(),
            "Sub-C": subc_daily_ret.dropna(),
        },
    )

    rows = []
    summary = {
        "script": str(SCRIPT),
        "weights": dict(mod.COMBINED_WEIGHTS),
        "baseline": {},
        "best_by_period": {},
    }

    for period_name, years in PERIODS:
        base_metrics = calc_metrics(slice_ret(combined_base, years))
        summary["baseline"][period_name] = base_metrics

    for window in WINDOWS:
        for threshold in THRESHOLDS:
            dk_r2 = build_r2_filtered_runner(mod, window, threshold)(cn_close.copy(), cn_dk_close.copy())
            combined_r2 = combine_returns(
                mod,
                {
                    "Sub-A": cn_result["return"].dropna(),
                    "Sub-A-DK": dk_r2["return"].dropna(),
                    "Sub-B": us_rot_result["return"].dropna(),
                    "Sub-C": subc_daily_ret.dropna(),
                },
            )
            filtered_days, observed_days, filtered_pct = summarize_filter_stats(dk_r2)

            for period_name, years in PERIODS:
                adk_metrics = calc_metrics(slice_ret(dk_r2["return"], years))
                combo_metrics = calc_metrics(slice_ret(combined_r2, years))
                base_combo = summary["baseline"][period_name]
                rows.append({
                    "period": period_name,
                    "window": window,
                    "threshold": threshold,
                    "filtered_days_total": filtered_days,
                    "observed_days_total": observed_days,
                    "filtered_pct_total": filtered_pct,
                    "adk_annual_return_pct": adk_metrics["annual_return"] * 100,
                    "adk_annual_vol_pct": adk_metrics["annual_vol"] * 100 if adk_metrics["annual_vol"] is not None else np.nan,
                    "adk_sharpe": adk_metrics["sharpe"],
                    "adk_max_drawdown_pct": adk_metrics["max_drawdown"] * 100,
                    "combined_annual_return_pct": combo_metrics["annual_return"] * 100,
                    "combined_annual_vol_pct": combo_metrics["annual_vol"] * 100 if combo_metrics["annual_vol"] is not None else np.nan,
                    "combined_sharpe": combo_metrics["sharpe"],
                    "combined_max_drawdown_pct": combo_metrics["max_drawdown"] * 100,
                    "combined_annual_delta_pct": combo_metrics["annual_return"] * 100 - base_combo["annual_return"] * 100,
                    "combined_sharpe_delta": combo_metrics["sharpe"] - base_combo["sharpe"],
                    "combined_max_drawdown_delta_pct": combo_metrics["max_drawdown"] * 100 - base_combo["max_drawdown"] * 100,
                })

    df = pd.DataFrame(rows)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    for period_name, _ in PERIODS:
        period_df = df[df["period"] == period_name].copy()
        period_df = period_df.sort_values(
            by=["combined_sharpe_delta", "combined_annual_delta_pct", "combined_max_drawdown_delta_pct"],
            ascending=[False, False, False],
        )
        summary["best_by_period"][period_name] = period_df.head(10).to_dict(orient="records")

    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    with OUT_MD.open("w", encoding="utf-8") as f:
        f.write("# ADK R²过滤参数扫描\n\n")
        f.write(f"- 脚本: `{SCRIPT.name}`\n")
        f.write("- 扫描对象: 当前根目录 6.1 真脚本上的 ADK，临时加回 R² 过滤后做窗口/阈值扫描\n")
        f.write("- 过滤逻辑: `spread cumulative rolling R² < threshold -> 次日收益置零`\n\n")
        f.write("## 基线组合（当前无R²过滤）\n\n")
        f.write("| 分段 | 年化 | 波动 | Sharpe | 最大回撤 |\n")
        f.write("|:-|:-|:-|:-|:-|\n")
        for period_name, _ in PERIODS:
            m = summary["baseline"][period_name]
            sharpe = "—" if m["sharpe"] is None else f"{m['sharpe']:.3f}"
            f.write(
                f"| {period_name} | {m['annual_return']*100:.2f}% | {m['annual_vol']*100:.2f}% | "
                f"{sharpe} | {m['max_drawdown']*100:.2f}% |\n"
            )
        f.write("\n## 各分段最优前5（按组合Sharpe增量排序）\n\n")
        for period_name, _ in PERIODS:
            f.write(f"### {period_name}\n\n")
            f.write("| window | threshold | 过滤占比 | 组合年化 | Δ年化 | 组合Sharpe | ΔSharpe | 组合MaxDD | ΔMaxDD |\n")
            f.write("|:-|:-|:-|:-|:-|:-|:-|:-|:-|\n")
            for row in summary["best_by_period"][period_name][:5]:
                f.write(
                    f"| {int(row['window'])} | {row['threshold']:.2f} | {row['filtered_pct_total']:.2f}% | "
                    f"{row['combined_annual_return_pct']:.2f}% | {row['combined_annual_delta_pct']:+.2f}% | "
                    f"{row['combined_sharpe']:.3f} | {row['combined_sharpe_delta']:+.3f} | "
                    f"{row['combined_max_drawdown_pct']:.2f}% | {row['combined_max_drawdown_delta_pct']:+.2f}% |\n"
                )
            f.write("\n")

    print(f"CSV: {OUT_CSV}")
    print(f"JSON: {OUT_JSON}")
    print(f"MD: {OUT_MD}")
    for period_name, _ in PERIODS:
        print(f"\n=== {period_name} TOP 5 ===")
        cols = [
            "window", "threshold", "filtered_pct_total",
            "combined_annual_return_pct", "combined_annual_delta_pct",
            "combined_sharpe", "combined_sharpe_delta",
            "combined_max_drawdown_pct", "combined_max_drawdown_delta_pct",
        ]
        print(df[df["period"] == period_name].sort_values(
            by=["combined_sharpe_delta", "combined_annual_delta_pct", "combined_max_drawdown_delta_pct"],
            ascending=[False, False, False],
        )[cols].head(5).to_string(index=False))


if __name__ == "__main__":
    main()
