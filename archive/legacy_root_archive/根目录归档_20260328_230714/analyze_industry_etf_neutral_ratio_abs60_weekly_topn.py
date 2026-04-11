from __future__ import annotations

import importlib.util
import json
import sys
import types
from dataclasses import asdict, dataclass
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
MAIN_SCRIPT = ROOT / "mnt_bot V 6.1 plus.py"

OUT_CLOSE_CSV = ROOT / "industry_etf_neutral_ratio_abs60_weekly_topn_close.csv"
OUT_NAV_CSV = ROOT / "industry_etf_neutral_ratio_abs60_weekly_topn_nav.csv"
OUT_SUMMARY_JSON = ROOT / "industry_etf_neutral_ratio_abs60_weekly_topn_summary.json"
OUT_PNG = ROOT / "industry_etf_neutral_ratio_abs60_weekly_topn_curve.png"
OUT_MD = ROOT / "mnt_industry_etf_neutral_ratio_abs60_weekly_topn_20260328.md"

HEDGE_SECID = "1.000905"
HEDGE_NAME = "中证500价格指数"
CN_TRADING_DAYS = 244
LOOKBACK = 60
MIN_ACTIVE_ETFS = 5
TRADE_COST_PER_LEG = 0.0
TOP_NS = [1, 2, 3]

INDUSTRY_ETFS = [
    ("159870", "化工"),
    ("512400", "有色金属"),
    ("515880", "通信"),
    ("515220", "煤炭"),
    ("512000", "证券"),
    ("515790", "光伏"),
    ("512690", "酒"),
    ("159995", "半导体/芯片"),
    ("512800", "银行"),
    ("512660", "军工"),
    ("512010", "医药"),
    ("512170", "医疗"),
    ("512980", "传媒"),
    ("159928", "消费"),
    ("515210", "钢铁"),
    ("516160", "新能源"),
    ("512200", "房地产"),
    ("159770", "机器人/中游制造"),
    ("515080", "红利策略"),
    ("515170", "食品饮料"),
    ("159745", "建材"),
]


@dataclass
class VariantMetrics:
    annual: float
    vol: float
    sharpe: float
    max_dd: float
    calmar: float
    win_rate: float
    total_return: float


def load_main_module():
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

    spec = importlib.util.spec_from_file_location("mntbot_v61_ratio_abs60_weekly_topn", MAIN_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    module.poe = poe
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def etf_code_to_secid(code: str) -> str:
    if code.startswith("159"):
        return f"0.{code}"
    return f"1.{code}"


def calc_abs_momentum(series: pd.Series, lookback: int = LOOKBACK) -> pd.Series:
    return series.div(series.shift(lookback)).sub(1.0)


def calc_metrics(ret: pd.Series) -> VariantMetrics:
    ret = ret.dropna()
    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else np.nan
    vol = ret.std(ddof=1) * np.sqrt(CN_TRADING_DAYS)
    sharpe = annual / vol if pd.notna(vol) and vol > 0 else np.nan
    max_dd = ((nav - nav.cummax()) / nav.cummax()).min()
    calmar = annual / abs(max_dd) if pd.notna(max_dd) and max_dd != 0 else np.nan
    monthly = ret.groupby(ret.index.to_period("M")).apply(lambda x: (1.0 + x).prod() - 1.0)
    win_rate = float((monthly > 0).mean()) if len(monthly) else np.nan
    total_return = nav.iloc[-1] - 1.0
    return VariantMetrics(
        annual=float(annual),
        vol=float(vol),
        sharpe=float(sharpe),
        max_dd=float(max_dd),
        calmar=float(calmar),
        win_rate=float(win_rate),
        total_return=float(total_return),
    )


def fetch_close_panel(mod) -> tuple[pd.DataFrame, dict[str, str]]:
    close_map: dict[str, pd.Series] = {}
    name_map: dict[str, str] = {}
    for code, label in INDUSTRY_ETFS:
        secid = etf_code_to_secid(code)
        df, source = mod.fetch_cn_kline(secid)
        close_map[secid] = df["close"].rename(secid)
        name_map[secid] = label
        print(f"fetched {label} {secid} [{source}] {df.index[0].date()} -> {df.index[-1].date()} ({len(df)})")
    hedge_df, hedge_source = mod.fetch_cn_kline(HEDGE_SECID)
    close_map[HEDGE_SECID] = hedge_df["close"].rename(HEDGE_SECID)
    name_map[HEDGE_SECID] = HEDGE_NAME
    print(f"fetched hedge {HEDGE_SECID} [{hedge_source}] {hedge_df.index[0].date()} -> {hedge_df.index[-1].date()} ({len(hedge_df)})")
    close_df = pd.concat(close_map.values(), axis=1).sort_index().ffill()
    return close_df, name_map


def weekly_signal_days(index: pd.DatetimeIndex, start_idx: int) -> set[int]:
    week_best: dict[tuple[int, int], tuple[int, int]] = {}
    for i in range(start_idx, len(index)):
        dt = index[i]
        dow = dt.dayofweek
        if dow > 3:
            continue
        yr, wk, _ = dt.isocalendar()
        key = (yr, wk)
        if key not in week_best or dow > week_best[key][1]:
            week_best[key] = (i, dow)
    return {v[0] for v in week_best.values()}


def pick_top_targets(scores: pd.Series, top_n: int) -> tuple[str, ...]:
    positive = scores[scores > 0].sort_values(ascending=False)
    if positive.empty:
        return tuple()
    return tuple(positive.index[:top_n])


def run_variant(
    close_df: pd.DataFrame,
    score_df: pd.DataFrame,
    name_map: dict[str, str],
    top_n: int,
) -> pd.DataFrame:
    hedge_ret = close_df[HEDGE_SECID].pct_change(fill_method=None)
    start_idx = LOOKBACK + 1
    signal_days = weekly_signal_days(close_df.index, start_idx)

    rows = []
    holding: tuple[str, ...] = tuple()
    target: tuple[str, ...] = tuple()
    for i in range(1, len(close_df)):
        date = close_df.index[i]
        spread_ret = 0.0
        cost = 0.0

        if holding:
            long_rets = close_df.loc[close_df.index[i], list(holding)].div(
                close_df.loc[close_df.index[i - 1], list(holding)]
            ).sub(1.0)
            short_ret = hedge_ret.iloc[i]
            if pd.notna(short_ret) and long_rets.notna().all():
                spread_ret = float(long_rets.mean() - short_ret)

        is_rebalance_day = i in signal_days
        if is_rebalance_day:
            today_scores = score_df.iloc[i].dropna()
            target = pick_top_targets(today_scores, top_n)
        is_signal = is_rebalance_day and target != holding

        if is_signal and TRADE_COST_PER_LEG > 0:
            if not holding and target:
                cost = 2 * TRADE_COST_PER_LEG
            elif holding and not target:
                cost = 2 * TRADE_COST_PER_LEG
            elif holding and target:
                cost = 2 * TRADE_COST_PER_LEG

        day_ret = (1.0 + spread_ret) * (1.0 - cost) - 1.0
        rows.append(
            {
                "date": date,
                "return": day_ret,
                "holding": "|".join(holding) if holding else "cash",
                "next_target": "|".join(target) if target else "cash",
                "holding_name": " / ".join(name_map.get(x, x) for x in holding) if holding else "cash",
                "next_target_name": " / ".join(name_map.get(x, x) for x in target) if target else "cash",
                "n_holdings": len(holding),
                "is_signal": is_signal,
                "rebalance_day": is_rebalance_day,
                "hedge": HEDGE_SECID if holding else "cash",
            }
        )
        if is_rebalance_day:
            holding = target

    result = pd.DataFrame(rows).set_index("date")
    result["nav"] = (1.0 + result["return"]).cumprod()
    return result


def build_score_df(close_df: pd.DataFrame) -> pd.DataFrame:
    etf_secids = [c for c in close_df.columns if c != HEDGE_SECID]
    ratio_df = close_df[etf_secids].div(close_df[HEDGE_SECID], axis=0)
    score_df = pd.DataFrame(index=close_df.index)
    for secid in etf_secids:
        score_df[secid] = calc_abs_momentum(ratio_df[secid], LOOKBACK)
    return score_df


def latest_selected_flags(scores: pd.Series, selected: tuple[str, ...], name_map: dict[str, str]) -> list[dict]:
    frame = pd.DataFrame(
        {
            "secid": scores.index,
            "name": [name_map.get(x, x) for x in scores.index],
            "score": scores.values,
        }
    ).sort_values("score", ascending=False)
    frame["selected"] = frame["secid"].isin(selected)
    return frame.head(10).to_dict(orient="records")


def build_summary(
    result: pd.DataFrame,
    score_df: pd.DataFrame,
    close_df: pd.DataFrame,
    name_map: dict[str, str],
    top_n: int,
) -> dict:
    metrics = calc_metrics(result["return"])
    holding_series = result["holding"]
    active = holding_series[holding_series != "cash"]
    spell_lengths = active.ne(active.shift()).cumsum()
    holding_spells = active.groupby(spell_lengths).size()
    yearly = {}
    for year in sorted(result.index.year.unique()):
        part = result.loc[result.index.year == year, "return"]
        if len(part) > 10:
            yearly[str(year)] = float((1.0 + part).prod() - 1.0)

    latest_holding_raw = result["next_target"].iloc[-1]
    latest_holding = tuple(latest_holding_raw.split("|")) if latest_holding_raw != "cash" else tuple()
    latest_scores = score_df.loc[score_df.index[-1]].dropna().sort_values(ascending=False)
    latest_close = {}
    if latest_holding:
        latest_close = {
            name_map.get(secid, secid): float(close_df[secid].iloc[-1]) for secid in latest_holding
        }
        latest_close["hedge_index"] = float(close_df[HEDGE_SECID].iloc[-1])

    return {
        "strategy": f"industry_etf_neutral_ratio_abs60_weekly_top{top_n}",
        "signal_mode": f"60d absolute momentum on ETF / 中证500 ratio, weekly rebalance, top{top_n} equal weight",
        "top_n": top_n,
        "hedge_proxy": {"secid": HEDGE_SECID, "name": HEDGE_NAME},
        "trade_cost_per_leg": TRADE_COST_PER_LEG,
        "lookback": LOOKBACK,
        "rebalance": "weekly",
        "min_active_etfs": MIN_ACTIVE_ETFS,
        "start_date": str(result.index[0].date()),
        "end_date": str(result.index[-1].date()),
        "n_days": int(len(result)),
        **asdict(metrics),
        "signal_changes": int(result["is_signal"].sum()),
        "rebalance_days": int(result["rebalance_day"].sum()),
        "long_days_pct": float((holding_series != "cash").mean()),
        "cash_days_pct": float((holding_series == "cash").mean()),
        "unique_etfs_used": sorted(
            {
                name_map.get(secid, secid)
                for raw in holding_series.unique()
                if raw != "cash"
                for secid in raw.split("|")
            }
        ),
        "median_holding_spell": float(holding_spells.median()) if len(holding_spells) else 0.0,
        "current_signal": {
            "date": str(result.index[-1].date()),
            "target": list(latest_holding),
            "target_name": [name_map.get(secid, secid) for secid in latest_holding],
            "hedge": HEDGE_NAME if latest_holding else "cash",
            "top10": latest_selected_flags(latest_scores, latest_holding, name_map),
        },
        "latest_close": latest_close,
        "yearly": yearly,
    }


def plot_nav(nav_df: pd.DataFrame) -> None:
    plt.figure(figsize=(12, 6))
    for col in nav_df.columns:
        plt.plot(nav_df.index, nav_df[col], linewidth=1.6, label=col)
    plt.title("Industry ETF Neutral Ratio Abs60 Weekly TopN NAV")
    plt.legend()
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(OUT_PNG, dpi=160)
    plt.close()


def write_md(compare_rows: list[dict], summaries: dict[str, dict]) -> None:
    compare_lines = []
    for row in compare_rows:
        compare_lines.append(
            f"| {row['variant']} | {row['annual']:.2%} | {row['vol']:.2%} | {row['sharpe']:.3f} | "
            f"{row['max_dd']:.2%} | {row['signal_changes']} | {row['median_holding_spell']:.1f} |"
        )

    signal_lines = []
    for variant in ["top1", "top2", "top3"]:
        summary = summaries[variant]
        targets = " / ".join(summary["current_signal"]["target_name"]) if summary["current_signal"]["target_name"] else "cash"
        signal_lines.append(f"| {variant} | {targets} | {summary['current_signal']['hedge']} |")

    md = f"""# 行业ETF市场中性策略 TopN 对比
## 设定

- 信号: `ETF / 中证500` 的 `{LOOKBACK}` 日绝对动量
- 调仓: 周频
- 对冲: 等额做空 `{HEDGE_NAME}` (`{HEDGE_SECID}`)
- 组合: 选取 `top1 / top2 / top3` 正动量行业ETF，等权做多
- 空仓规则: 若正动量ETF数量为 0，则空仓

## 回测对比

| 版本 | 年化 | 波动 | 夏普 | 最大回撤 | 换仓次数 | 持仓中位天数 |
|:-|-----:|-----:|-----:|--------:|--------:|------------:|
{chr(10).join(compare_lines)}

## 当前信号

| 版本 | 多头ETF | 对冲腿 |
|:-|:-|:-|
{chr(10).join(signal_lines)}
"""
    OUT_MD.write_text(md, encoding="utf-8")


def main() -> None:
    mod = load_main_module()
    close_df, name_map = fetch_close_panel(mod)
    close_df.to_csv(OUT_CLOSE_CSV, index_label="date", encoding="utf-8")

    score_df = build_score_df(close_df)
    active_counts = score_df.notna().sum(axis=1)
    valid_start = active_counts[active_counts >= MIN_ACTIVE_ETFS].index.min()
    if pd.isna(valid_start):
        raise ValueError(f"not enough valid ETFs to start strategy: need {MIN_ACTIVE_ETFS}")

    close_df = close_df.loc[valid_start:].copy()
    score_df = score_df.loc[valid_start:].copy()

    summaries: dict[str, dict] = {}
    nav_df = pd.DataFrame(index=close_df.index[1:])
    compare_rows = []
    for top_n in TOP_NS:
        variant = f"top{top_n}"
        result = run_variant(close_df, score_df, name_map, top_n)
        nav_df[variant] = result["nav"]
        summary = build_summary(result, score_df, close_df, name_map, top_n)
        summaries[variant] = summary
        compare_rows.append(
            {
                "variant": variant,
                "annual": summary["annual"],
                "vol": summary["vol"],
                "sharpe": summary["sharpe"],
                "max_dd": summary["max_dd"],
                "signal_changes": summary["signal_changes"],
                "median_holding_spell": summary["median_holding_spell"],
            }
        )

    nav_df.to_csv(OUT_NAV_CSV, index_label="date", encoding="utf-8")
    payload = {
        "strategy_family": "industry_etf_neutral_ratio_abs60_weekly_topn",
        "compare": compare_rows,
        "variants": summaries,
    }
    OUT_SUMMARY_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    plot_nav(nav_df)
    write_md(compare_rows, summaries)

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"saved {OUT_CLOSE_CSV.name}")
    print(f"saved {OUT_NAV_CSV.name}")
    print(f"saved {OUT_SUMMARY_JSON.name}")
    print(f"saved {OUT_PNG.name}")
    print(f"saved {OUT_MD.name}")


if __name__ == "__main__":
    main()
