import importlib.util
import json
import math
import sys
import types
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "Arial Unicode MS", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


ROOT = Path(__file__).resolve().parent
CN_DATA_CSV = ROOT / "mnt_strategy_data_cn.csv"
US_DATA_CSV = ROOT / "mnt_strategy_data_us.csv"

VERSIONS = {
    "V6.1": ROOT / "mnt_bot V 6.1 plus.py",
    "V6.2": ROOT / "mnt_bot V 6.2 plus.py",
    "V6.3": ROOT / "mnt_bot B 6.3 plus.py",
}

OUT_CHART = ROOT / "version_compare_v61_v62_b63.png"
OUT_CSV = ROOT / "version_compare_metrics.csv"
OUT_MD = ROOT / "version_compare_metrics.md"
OUT_JSON = ROOT / "version_compare_summary.json"


def load_module(script_path: Path, module_name: str):
    class BotError(Exception):
        pass

    class DummySettingsResponse:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

    poe = types.SimpleNamespace(
        BotError=BotError,
        start_message=lambda: None,
        default_chat=None,
        query=types.SimpleNamespace(text="", attachments=[]),
        call=lambda *args, **kwargs: None,
        update_settings=lambda *args, **kwargs: None,
    )

    fastapi_poe = types.ModuleType("fastapi_poe")
    fastapi_poe_types = types.ModuleType("fastapi_poe.types")
    fastapi_poe_types.SettingsResponse = DummySettingsResponse
    sys.modules["fastapi_poe"] = fastapi_poe
    sys.modules["fastapi_poe.types"] = fastapi_poe_types

    spec = importlib.util.spec_from_file_location(module_name, script_path)
    module = importlib.util.module_from_spec(spec)
    module.poe = poe
    spec.loader.exec_module(module)
    return module


def _load_close_csv(path: Path):
    return pd.read_csv(path, parse_dates=["date"]).set_index("date").sort_index()["close"]


def load_cn_data():
    cn_raw = pd.read_csv(CN_DATA_CSV, parse_dates=["date"]).set_index("date").sort_index()

    cn_close = pd.concat(
        [
            _load_close_csv(ROOT / ".cn_official_cache" / "1_H20955.csv").rename("1.H20955"),
            _load_close_csv(ROOT / ".cn_official_cache" / "0_399606.csv").rename("0.399606"),
            _load_close_csv(ROOT / ".cn_official_cache" / "1_H00016.csv").rename("1.H00016"),
            _load_close_csv(ROOT / ".cn_official_cache" / "1_H00852.csv").rename("1.H00852"),
            _load_close_csv(ROOT / ".cn_official_cache" / "1_H00905.csv").rename("1.H00905"),
        ],
        axis=1,
    ).ffill().dropna()

    bond = _load_close_csv(ROOT / ".cn_official_cache" / "1_H11077.csv").rename("1.H11077")
    cn_close_with_bond = pd.concat([cn_close, bond], axis=1).ffill().dropna()

    cn_dk_close = pd.DataFrame(index=cn_raw.index)
    cn_dk_close["DK_ZZ1000"] = cn_raw["1.000852"]
    cn_dk_close["DK_SZ50"] = cn_raw["1.000016"]
    cn_dk_close["DK_HS300"] = cn_raw["1.000300"]
    cn_dk_close["DK_ZZ500"] = cn_raw["1.000905"]
    cn_dk_close["DK_CYB"] = cn_raw["0.399006"]
    cn_dk_close = cn_dk_close.ffill().dropna()

    return cn_close, cn_close_with_bond, cn_dk_close


def load_us_data(mod):
    us = pd.read_csv(US_DATA_CSV, parse_dates=["date"]).set_index("date").sort_index()
    emxc = us["EMXC_spliced"].combine_first(us["EMXC"]).combine_first(us["EEM"])

    us_raw = {}
    tickers = set(
        list(mod.US_ROT_POOL)
        + ["BIL", "SPY", mod.US_ROT_EMXC_BT_PROXY]
        + [cfg["proxy"] for cfg in mod.PROD_PORTFOLIO.values()]
        + list(mod.US_ROT_ASSETS.keys())
        + list(mod.PROD_PORTFOLIO.keys())
        + ["EEM", "BTC-USD", "DBMF"]
    )
    for ticker in tickers:
        if ticker == "EMXC":
            ser = emxc
        elif ticker in us.columns:
            ser = us[ticker]
        else:
            continue
        us_raw[ticker] = pd.DataFrame({"close": ser}).dropna()

    rot_tickers = list(mod.US_ROT_POOL) + ["BIL"]
    late_rot = {"BTC-USD", "EMXC"}
    rot_core = [t for t in rot_tickers if t not in late_rot]
    if "EMXC" in mod.US_ROT_POOL and mod.US_ROT_EMXC_BT_PROXY not in rot_core and mod.US_ROT_EMXC_BT_PROXY in us_raw:
        rot_core.append(mod.US_ROT_EMXC_BT_PROXY)

    us_rot_close = pd.concat(
        [us_raw[t][["close"]].rename(columns={"close": t}) for t in rot_core if t in us_raw],
        axis=1,
    ).ffill().dropna()

    if "EMXC" in mod.US_ROT_POOL and mod.US_ROT_EMXC_BT_PROXY in us_raw:
        eem_col = us_rot_close[mod.US_ROT_EMXC_BT_PROXY].copy() if mod.US_ROT_EMXC_BT_PROXY in us_rot_close.columns else None
        emxc_raw = us_raw.get("EMXC")
        if eem_col is not None:
            hybrid = eem_col.rename("EMXC")
            if emxc_raw is not None and len(emxc_raw) > 0:
                emxc_ser = emxc_raw["close"].reindex(hybrid.index)
                switch_idx = hybrid.index >= mod.US_ROT_EMXC_BT_START
                first_emxc_date = emxc_ser.loc[switch_idx].first_valid_index() if switch_idx.any() else None
                if first_emxc_date is not None:
                    scale_factor = hybrid.loc[first_emxc_date] / emxc_ser.loc[first_emxc_date]
                    hybrid.loc[switch_idx] = emxc_ser.loc[switch_idx] * scale_factor
            us_rot_close["EMXC"] = hybrid
            if mod.US_ROT_EMXC_BT_PROXY in us_rot_close.columns and mod.US_ROT_EMXC_BT_PROXY not in mod.US_ROT_POOL:
                us_rot_close = us_rot_close.drop(columns=[mod.US_ROT_EMXC_BT_PROXY])

    for t in late_rot:
        if t == "EMXC":
            continue
        if t in us_raw:
            us_rot_close = us_rot_close.join(us_raw[t][["close"]].rename(columns={"close": t}), how="left")

    prod_proxies = list(set([cfg["proxy"] for cfg in mod.PROD_PORTFOLIO.values()] + [mod.PROD_CASH]))
    late_prod = {"BTC-USD", "DBMF"}
    prod_core = [t for t in prod_proxies if t not in late_prod]
    us_prod_daily = pd.concat(
        [us_raw[t][["close"]].rename(columns={"close": t}) for t in prod_core if t in us_raw],
        axis=1,
    ).ffill().dropna()
    for t in late_prod:
        if t in us_raw:
            us_prod_daily = us_prod_daily.join(us_raw[t][["close"]].rename(columns={"close": t}), how="left")

    if "SPY" not in us_rot_close.columns and "SPY" in us_raw:
        us_rot_close["SPY"] = us_raw["SPY"]["close"].reindex(us_rot_close.index)

    stock_cols_rot = [t for t in rot_tickers if t in us_raw and t != "BTC-USD"]
    if stock_cols_rot:
        last_stock_date = max(us_raw[t].index[-1] for t in stock_cols_rot)
        us_rot_close = us_rot_close.loc[:last_stock_date]

    stock_cols_prod = [t for t in prod_proxies if t in us_raw and t != "BTC-USD"]
    if stock_cols_prod:
        last_prod_date = max(us_raw[t].index[-1] for t in stock_cols_prod)
        us_prod_daily = us_prod_daily.loc[:last_prod_date]

    for live_ticker in set(list(mod.US_ROT_ASSETS.keys()) + list(mod.PROD_PORTFOLIO.keys())):
        if live_ticker in us_raw:
            live_col = us_raw[live_ticker]["close"]
            if live_ticker not in us_rot_close.columns:
                us_rot_close[live_ticker] = live_col.reindex(us_rot_close.index)
            if live_ticker not in us_prod_daily.columns:
                us_prod_daily[live_ticker] = live_col.reindex(us_prod_daily.index)

    return us_rot_close, us_prod_daily


def compute_subc_outputs(mod, us_prod_daily):
    prod_monthly = us_prod_daily.resample("ME").last()
    last_daily = us_prod_daily.index[-1]
    if len(prod_monthly) and last_daily.to_period("M") == prod_monthly.index[-1].to_period("M"):
        prod_monthly = prod_monthly.iloc[:-1]
    prod_sig_a = mod.make_abs_mom_signals(prod_monthly, mod.PROD_ABS_MOM_LB)
    prod_sig_b = mod.make_sma_signals(prod_monthly, mod.PROD_SMA_WINDOW, mod.PROD_SMA_BAND)
    if not mod.PROD_USE_TIMING:
        prod_sig_a = pd.DataFrame(1.0, index=prod_sig_a.index, columns=prod_sig_a.columns)
        prod_sig_b = prod_sig_a.copy()
    prod_monthly_ret = prod_monthly.pct_change().dropna(how="all")
    cash_ret = prod_monthly_ret[mod.PROD_CASH] if mod.PROD_CASH in prod_monthly_ret.columns else pd.Series(0, index=prod_monthly_ret.index)
    mod.simulate_prod_btc_phased(
        prod_monthly_ret,
        prod_sig_a,
        cash_ret,
        mod.PROD_REBAL_MONTH,
        sig_b=prod_sig_b,
        blend_a=mod.PROD_BLEND_A,
        commission=mod.PROD_COMMISSION,
    )
    subc_daily_ret = mod._get_subc_daily_ret(us_prod_daily, prod_sig_a, prod_sig_b=prod_sig_b)
    return subc_daily_ret


def compute_version_nav(mod, cn_close, cn_close_with_bond, cn_dk_close, us_rot_close, us_prod_daily):
    cn_result = mod.run_cn_strategy(cn_close_with_bond.copy(), mod.CN_EQUITY_CODES)
    cn_dk_result = mod.run_dk_strategy(cn_close.copy(), cn_dk_close.copy())
    us_rot_result = mod.run_us_rotation(
        us_rot_close.copy(),
        mod.US_ROT_POOL,
        btc_ticker=mod.US_ROT_BTC_TICKER,
        btc_start=mod.US_ROT_BTC_START,
        btc_max_w=mod.US_ROT_BTC_MAX_W,
    )
    if getattr(mod, "US_ROT_VOLREG_ENABLED", False) and "SPY" in us_rot_close.columns:
        us_rot_result = mod.apply_vol_regime_overlay(us_rot_result, us_rot_close["SPY"])

    subc_daily_ret = compute_subc_outputs(mod, us_prod_daily.copy())

    nav_series = {}
    daily_returns = {
        "Sub-A": cn_result["return"].dropna(),
        "Sub-A-DK": cn_dk_result["return"].dropna(),
        "Sub-B": us_rot_result["return"].dropna(),
        "Sub-C": subc_daily_ret.dropna(),
    }
    for name, dret in daily_returns.items():
        if len(dret) > 1:
            nav = (1 + dret).cumprod()
            nav_series[name] = nav / nav.iloc[0]

    all_nav_dates = sorted(set().union(*(s.index for s in nav_series.values())))
    nav_df = pd.DataFrame({
        name: s.reindex(pd.DatetimeIndex(all_nav_dates)).ffill()
        for name, s in nav_series.items()
    })
    weight_df = nav_df.notna().astype(float)
    weights = mod.COMBINED_WEIGHTS
    for col in weight_df.columns:
        weight_df[col] *= weights.get(col, 0.0)
    weight_sum = weight_df.sum(axis=1).replace(0, np.nan)
    weight_df = weight_df.div(weight_sum, axis=0)
    nav_comb = (nav_df.fillna(0.0) * weight_df).sum(axis=1)
    nav_comb = nav_comb / nav_comb.iloc[0]
    return nav_comb.dropna()


def period_slice(nav: pd.Series, years: int | None):
    if years is None:
        return nav
    end = nav.index[-1]
    start = end - pd.DateOffset(years=years)
    return nav[nav.index >= start]


def calc_metrics(nav: pd.Series):
    nav = nav.dropna()
    ret = nav.pct_change().dropna()
    if len(nav) < 2 or len(ret) < 1:
        return None
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


def main():
    cn_close, cn_close_with_bond, cn_dk_close = load_cn_data()

    version_nav = {}
    version_meta = {}
    for label, script_path in VERSIONS.items():
        mod = load_module(script_path, f"mod_{label.replace('.', '_')}")
        us_rot_close, us_prod_daily = load_us_data(mod)
        nav = compute_version_nav(mod, cn_close, cn_close_with_bond, cn_dk_close, us_rot_close, us_prod_daily)
        version_nav[label] = nav
        version_meta[label] = {
            "script": str(script_path),
            "weights": dict(mod.COMBINED_WEIGHTS),
        }

    common_start = max(nav.index[0] for nav in version_nav.values())
    common_end = min(nav.index[-1] for nav in version_nav.values())
    aligned_nav = {
        label: nav[(nav.index >= common_start) & (nav.index <= common_end)]
        for label, nav in version_nav.items()
    }
    aligned_nav = {label: nav / nav.iloc[0] for label, nav in aligned_nav.items() if len(nav) > 1}

    periods = [("最长", None), ("10年", 10), ("5年", 5), ("2年", 2)]
    rows = []
    summary = {"chart": str(OUT_CHART), "versions": version_meta, "periods": {}}
    for period_name, years in periods:
        summary["periods"][period_name] = {}
        for label, nav in aligned_nav.items():
            metrics = calc_metrics(period_slice(nav, years))
            if not metrics:
                continue
            summary["periods"][period_name][label] = metrics
            weights = version_meta[label]["weights"]
            rows.append({
                "period": period_name,
                "version": label,
                "w_sub_a_pct": weights.get("Sub-A", 0.0) * 100,
                "w_sub_adk_pct": weights.get("Sub-A-DK", 0.0) * 100,
                "w_sub_b_pct": weights.get("Sub-B", 0.0) * 100,
                "w_sub_c_pct": weights.get("Sub-C", 0.0) * 100,
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
    period_order = {"最长": 0, "10年": 1, "5年": 2, "2年": 3}
    version_order = {"V6.1": 0, "V6.2": 1, "V6.3": 2}
    df["period_order"] = df["period"].map(period_order)
    df["version_order"] = df["version"].map(version_order)
    df = df.sort_values(["period_order", "version_order"]).drop(columns=["period_order", "version_order"])
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    fig = plt.figure(figsize=(16, 11))
    gs = fig.add_gridspec(nrows=2, ncols=1, height_ratios=[3.3, 2.2])
    ax = fig.add_subplot(gs[0, 0])
    ax_tbl = fig.add_subplot(gs[1, 0])

    colors = {"V6.1": "#E74C3C", "V6.2": "#2980B9", "V6.3": "#27AE60"}
    styles = {
        "V6.1": {"linestyle": "--", "linewidth": 2.8, "zorder": 5},
        "V6.2": {"linestyle": "-", "linewidth": 2.0, "zorder": 3},
        "V6.3": {"linestyle": "-", "linewidth": 2.4, "zorder": 4},
    }
    labels = {
        "V6.1": "V6.1 (与V6.2重合, 虚线置顶)",
        "V6.2": "V6.2",
        "V6.3": "V6.3",
    }
    for label in ["V6.2", "V6.3", "V6.1"]:
        nav = aligned_nav[label]
        ax.plot(nav.index, nav.values, label=labels[label], color=colors[label], **styles[label])
    ax.axhline(1.0, color="gray", linestyle="--", linewidth=0.8, alpha=0.6)
    ax.set_title(
        f"V6.1 / V6.2 / B6.3 组合净值对比 ({common_start:%Y-%m-%d} 至 {common_end:%Y-%m-%d})",
        fontsize=15,
        fontweight="bold",
    )
    ax.set_ylabel("净值 (起点=1.0)")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left", fontsize=10, framealpha=0.95)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.setp(ax.get_xticklabels(), rotation=30, ha="right")

    ax_tbl.axis("off")
    display_df = df.copy()
    display_df["A"] = display_df["w_sub_a_pct"].map(lambda x: f"{x:.0f}%")
    display_df["ADK"] = display_df["w_sub_adk_pct"].map(lambda x: f"{x:.0f}%")
    display_df["B"] = display_df["w_sub_b_pct"].map(lambda x: f"{x:.0f}%")
    display_df["C"] = display_df["w_sub_c_pct"].map(lambda x: f"{x:.0f}%")
    display_df["累计收益"] = display_df["total_return_pct"].map(lambda x: f"{x:+.2f}%")
    display_df["年化收益"] = display_df["annual_return_pct"].map(lambda x: f"{x:+.2f}%")
    display_df["年化波动"] = display_df["annual_vol_pct"].map(lambda x: f"{x:.2f}%")
    display_df["Sharpe"] = display_df["sharpe"].map(lambda x: f"{x:.3f}")
    display_df["最大回撤"] = display_df["max_drawdown_pct"].map(lambda x: f"{x:.2f}%")
    table_df = display_df[["period", "version", "A", "ADK", "B", "C", "累计收益", "年化收益", "年化波动", "Sharpe", "最大回撤"]].rename(
        columns={"period": "分段", "version": "版本"}
    )
    table = ax_tbl.table(
        cellText=table_df.values,
        colLabels=table_df.columns,
        cellLoc="center",
        colLoc="center",
        loc="center",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.5)
    for (row, col), cell in table.get_celld().items():
        if row == 0:
            cell.set_text_props(weight="bold", color="white")
            cell.set_facecolor("#34495E")
        elif row > 0:
            period_name = table_df.iloc[row - 1, 0]
            cell.set_facecolor("#F7F9FB" if period_order[period_name] % 2 == 0 else "#EEF3F8")

    fig.tight_layout()
    fig.savefig(OUT_CHART, dpi=180, bbox_inches="tight")
    plt.close(fig)

    md_lines = [
        "# 版本对比",
        "",
        f"曲线图: `{OUT_CHART.name}`",
        "",
        "| 分段 | 版本 | A | ADK | B | C | 起始 | 结束 | 交易日 | 累计收益 | 年化收益 | 年化波动 | Sharpe | 最大回撤 |",
        "|:-|:-|:-:|:-:|:-:|:-:|:-|:-|--:|--:|--:|--:|--:|--:|",
    ]
    for _, r in df.iterrows():
        md_lines.append(
            f"| {r['period']} | {r['version']} | {r['w_sub_a_pct']:.0f}% | {r['w_sub_adk_pct']:.0f}% | {r['w_sub_b_pct']:.0f}% | {r['w_sub_c_pct']:.0f}% | "
            f"{r['start']} | {r['end']} | {int(r['days'])} | "
            f"{r['total_return_pct']:+.2f}% | {r['annual_return_pct']:+.2f}% | {r['annual_vol_pct']:.2f}% | "
            f"{r['sharpe']:.3f} | {r['max_drawdown_pct']:.2f}% |"
        )
    OUT_MD.write_text("\n".join(md_lines), encoding="utf-8")
    OUT_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Chart: {OUT_CHART}")
    print(f"CSV: {OUT_CSV}")
    print(f"MD: {OUT_MD}")
    print(f"JSON: {OUT_JSON}")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
