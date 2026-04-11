from __future__ import annotations

import importlib.util
import json
import sys
import types
from dataclasses import dataclass
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
MAIN_SCRIPT = ROOT / "mnt_bot V 6.1 plus.py"

OUT_CLOSE_CSV = ROOT / "industry_etf_neutral_ratio_abs60_weekly_close.csv"
OUT_NAV_CSV = ROOT / "industry_etf_neutral_ratio_abs60_weekly_nav.csv"
OUT_SIGNAL_CSV = ROOT / "industry_etf_neutral_ratio_abs60_weekly_latest_signal.csv"
OUT_SUMMARY_JSON = ROOT / "industry_etf_neutral_ratio_abs60_weekly_summary.json"
OUT_PNG = ROOT / "industry_etf_neutral_ratio_abs60_weekly_curve.png"
OUT_MD = ROOT / "mnt_industry_etf_neutral_ratio_abs60_weekly_20260328.md"

HEDGE_SECID = "1.000905"
HEDGE_NAME = "中证500价格指数"
CN_TRADING_DAYS = 244
LOOKBACK = 60
MIN_ACTIVE_ETFS = 5
TRADE_COST_PER_LEG = 0.0

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

    spec = importlib.util.spec_from_file_location("mntbot_v61_ratio_abs60_weekly", MAIN_SCRIPT)
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


def run_rotation_market_neutral(close_df: pd.DataFrame, name_map: dict[str, str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    etf_secids = [c for c in close_df.columns if c != HEDGE_SECID]
    ratio_df = close_df[etf_secids].div(close_df[HEDGE_SECID], axis=0)

    score_df = pd.DataFrame(index=close_df.index)
    for secid in etf_secids:
        score_df[secid] = calc_abs_momentum(ratio_df[secid], LOOKBACK)

    active_counts = score_df.notna().sum(axis=1)
    valid_start = active_counts[active_counts >= MIN_ACTIVE_ETFS].index.min()
    if pd.isna(valid_start):
        raise ValueError(f"not enough valid ETFs to start strategy: need {MIN_ACTIVE_ETFS}")

    close_df = close_df.loc[valid_start:].copy()
    score_df = score_df.loc[valid_start:].copy()
    hedge_ret = close_df[HEDGE_SECID].pct_change(fill_method=None)
    start_idx = LOOKBACK + 1
    signal_days = weekly_signal_days(close_df.index, start_idx)

    rows = []
    holding = "cash"
    target = "cash"
    for i in range(1, len(close_df)):
        date = close_df.index[i]
        spread_ret = 0.0
        cost = 0.0
        if holding != "cash":
            long_ret = close_df.iloc[i][holding] / close_df.iloc[i - 1][holding] - 1.0
            short_ret = hedge_ret.iloc[i]
            if pd.isna(long_ret) or pd.isna(short_ret):
                spread_ret = 0.0
            else:
                spread_ret = long_ret - short_ret

        is_sig = i in signal_days
        if is_sig:
            today_scores = score_df.iloc[i].dropna()
            target = "cash"
            if len(today_scores) > 0:
                best = today_scores.idxmax()
                if today_scores[best] > 0:
                    target = best
        is_signal = is_sig and target != holding

        if is_signal and TRADE_COST_PER_LEG > 0:
            if holding == "cash" and target != "cash":
                cost = 2 * TRADE_COST_PER_LEG
            elif holding != "cash" and target == "cash":
                cost = 2 * TRADE_COST_PER_LEG
            elif holding != "cash" and target != "cash":
                cost = 2 * TRADE_COST_PER_LEG
        day_ret = (1.0 + spread_ret) * (1.0 - cost) - 1.0
        rows.append(
            {
                "date": date,
                "return": day_ret,
                "holding": holding,
                "next_target": target,
                "is_signal": is_signal,
                "rebalance_day": is_sig,
                "score": float(score_df.iloc[i].get(target, np.nan)) if target != "cash" else np.nan,
                "hedge": HEDGE_SECID if holding != "cash" else "cash",
            }
        )
        if is_sig:
            holding = target

    result = pd.DataFrame(rows).set_index("date")
    result["nav"] = (1.0 + result["return"]).cumprod()
    result["holding_name"] = result["holding"].map(lambda x: name_map.get(x, x))
    result["next_target_name"] = result["next_target"].map(lambda x: name_map.get(x, x))
    return result, score_df


def plot_nav(nav: pd.Series) -> None:
    plt.figure(figsize=(12, 6))
    plt.plot(nav.index, nav.values, linewidth=1.8, label="Industry ETF Neutral Ratio Abs60 Weekly")
    plt.title("Industry ETF Neutral Ratio Absolute-Momentum(60d, Weekly) NAV")
    plt.legend()
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig(OUT_PNG, dpi=160)
    plt.close()


def build_latest_signal(result: pd.DataFrame, score_df: pd.DataFrame, name_map: dict[str, str]) -> pd.DataFrame:
    last_dt = score_df.index[-1]
    latest_scores = score_df.loc[last_dt].dropna().sort_values(ascending=False)
    frame = pd.DataFrame(
        {
            "secid": latest_scores.index,
            "name": [name_map.get(x, x) for x in latest_scores.index],
            "score": latest_scores.values,
        }
    )
    current_holding = result["next_target"].iloc[-1]
    frame["selected"] = frame["secid"] == current_holding
    return frame


def build_summary(result: pd.DataFrame, latest_signal: pd.DataFrame, close_df: pd.DataFrame, name_map: dict[str, str]) -> dict:
    metrics = calc_metrics(result["return"])
    holding_series = result["holding"]
    non_cash = holding_series[holding_series != "cash"]
    spell_lengths = non_cash.ne(non_cash.shift()).cumsum()
    holding_spells = non_cash.groupby(spell_lengths).size()
    yearly = {}
    for year in sorted(result.index.year.unique()):
        part = result.loc[result.index.year == year, "return"]
        if len(part) > 10:
            yearly[str(year)] = float((1.0 + part).prod() - 1.0)
    latest_holding = result["next_target"].iloc[-1]
    latest_close = {}
    if latest_holding != "cash":
        latest_close["long_etf"] = float(close_df[latest_holding].iloc[-1])
        latest_close["hedge_index"] = float(close_df[HEDGE_SECID].iloc[-1])
    return {
        "strategy": "industry_etf_neutral_ratio_abs60_weekly",
        "signal_mode": "60d absolute momentum on ETF / 中证500 ratio, weekly rebalance",
        "hedge_proxy": {"secid": HEDGE_SECID, "name": HEDGE_NAME},
        "trade_cost_per_leg": TRADE_COST_PER_LEG,
        "lookback": LOOKBACK,
        "rebalance": "weekly",
        "min_active_etfs": MIN_ACTIVE_ETFS,
        "start_date": str(result.index[0].date()),
        "end_date": str(result.index[-1].date()),
        "n_days": int(len(result)),
        "annual": metrics.annual,
        "vol": metrics.vol,
        "sharpe": metrics.sharpe,
        "max_dd": metrics.max_dd,
        "calmar": metrics.calmar,
        "win_rate": metrics.win_rate,
        "total_return": metrics.total_return,
        "signal_changes": int(result["is_signal"].sum()),
        "rebalance_days": int(result["rebalance_day"].sum()),
        "long_days_pct": float((holding_series != "cash").mean()),
        "cash_days_pct": float((holding_series == "cash").mean()),
        "unique_etfs_used": sorted({name_map.get(x, x) for x in holding_series.unique() if x != "cash"}),
        "median_holding_spell": float(holding_spells.median()) if len(holding_spells) else 0.0,
        "current_signal": {
            "date": str(result.index[-1].date()),
            "target": latest_holding,
            "target_name": name_map.get(latest_holding, latest_holding),
            "hedge": HEDGE_NAME if latest_holding != "cash" else "cash",
            "top5": latest_signal.head(5).to_dict(orient="records"),
        },
        "latest_close": latest_close,
        "yearly": yearly,
    }


def write_md(summary: dict, latest_signal: pd.DataFrame) -> None:
    top_rows = []
    for _, row in latest_signal.head(10).iterrows():
        marker = " ✅" if bool(row["selected"]) else ""
        top_rows.append(f"| {row['name']}{marker} | {row['secid']} | {row['score']:+.3%} |")
    yearly_rows = []
    for year, ret in summary["yearly"].items():
        yearly_rows.append(f"| {year} | {ret:.1%} |")
    current_target = summary["current_signal"]["target_name"]
    md = f"""# 行业ETF市场中性策略 Ratio-Abs60 Weekly 版

## 设定

- 多头池子: 行业ETF 21 只
- 对冲腿: {HEDGE_NAME} (`{HEDGE_SECID}`) 价格指数，作为中证500期货的价格代理
- 信号模式: `ETF / 中证500` 的 `{LOOKBACK}` 日绝对动量
- 调仓频率: 周频，只在每周最后一个有效信号日检查是否换仓
- 交易规则:
  - 每周比较全部行业ETF相对中证500的 `{LOOKBACK}` 日超额走势
  - 动量最高且 `> 0` 时，做多该ETF，同时等额做空中证500
  - 若新的ETF动量超过当前持仓，则切换多头ETF
  - 若最高动量 `<= 0`，则空仓
- 启动条件: 至少 `{MIN_ACTIVE_ETFS}` 只ETF有有效动量分数后才开始回测

## 回测结果

| 指标 | 数值 |
|:-|:-|
| 区间 | {summary['start_date']} 到 {summary['end_date']} |
| 年化收益 | {summary['annual']:.2%} |
| 年化波动 | {summary['vol']:.2%} |
| 夏普 | {summary['sharpe']:.3f} |
| 最大回撤 | {summary['max_dd']:.2%} |
| Calmar | {summary['calmar']:.3f} |
| 月胜率 | {summary['win_rate']:.2%} |
| 累计收益 | {summary['total_return']:.2%} |
| 换仓次数 | {summary['signal_changes']} |
| 周频检查次数 | {summary['rebalance_days']} |
| 持仓天数占比 | {summary['long_days_pct']:.2%} |
| 空仓天数占比 | {summary['cash_days_pct']:.2%} |
| 持仓中位持续天数 | {summary['median_holding_spell']:.1f} |

## 当前信号

- 日期: `{summary['current_signal']['date']}`
- 当前目标: `{current_target}`
- 对冲腿: `{summary['current_signal']['hedge']}`

### 最新动量排名

| ETF | secid | 60d ratio abs momentum |
|:-|:-|------:|
{chr(10).join(top_rows)}

## 年度收益

| 年份 | 收益 |
|:-|------:|
{chr(10).join(yearly_rows)}
"""
    OUT_MD.write_text(md, encoding="utf-8")


def main() -> None:
    mod = load_main_module()
    close_df, name_map = fetch_close_panel(mod)
    close_df.to_csv(OUT_CLOSE_CSV, index_label="date", encoding="utf-8")

    result, score_df = run_rotation_market_neutral(close_df, name_map)
    result[["return", "nav", "holding", "next_target", "is_signal", "rebalance_day", "score"]].to_csv(
        OUT_NAV_CSV, index_label="date", encoding="utf-8"
    )
    latest_signal = build_latest_signal(result, score_df, name_map)
    latest_signal.to_csv(OUT_SIGNAL_CSV, index=False, encoding="utf-8")
    summary = build_summary(result, latest_signal, close_df, name_map)
    OUT_SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    plot_nav(result["nav"])
    write_md(summary, latest_signal)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"saved {OUT_CLOSE_CSV.name}")
    print(f"saved {OUT_NAV_CSV.name}")
    print(f"saved {OUT_SIGNAL_CSV.name}")
    print(f"saved {OUT_SUMMARY_JSON.name}")
    print(f"saved {OUT_PNG.name}")
    print(f"saved {OUT_MD.name}")


if __name__ == "__main__":
    main()
