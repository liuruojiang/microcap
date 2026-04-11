from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

import analyze_microcap_zz1000_hedge as hedge_mod
import scan_top100_momentum_costs as cost_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

INDEX_CSV = OUTPUT_DIR / "wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
TURNOVER_CSV = OUTPUT_DIR / "microcap_top100_biweekly_thursday_turnover_stats.csv"
DEFENSIVE_PANEL_CSV = (
    ROOT
    / "archive"
    / "legacy_root_archive"
    / "根目录归档_20260328_230714"
    / "industry_etf_neutral_ratio_bias_close.csv"
)

OUT_PREFIX = "microcap_top100_mom16_biweekly_defensive_guard"
OUT_NAV = OUTPUT_DIR / f"{OUT_PREFIX}_nav.csv"
OUT_SIGNAL = OUTPUT_DIR / f"{OUT_PREFIX}_latest_signal.csv"
OUT_SUMMARY = OUTPUT_DIR / f"{OUT_PREFIX}_summary.json"
OUT_COMPARE = OUTPUT_DIR / f"{OUT_PREFIX}_compare.json"

LOOKBACK = 16
FUTURES_DRAG = hedge_mod.DEFAULT_FUTURES_DRAG
HEDGE_COLUMN = hedge_mod.DEFAULT_HEDGE_COLUMN
MIN_ACTIVE_INDUSTRIES = 5
BREADTH_WINDOW = 20

# Original JoinQuant defensive industries:
# 银行I, 煤炭I, 采掘I, 钢铁I
# Local industry ETF panel does not include a dedicated "采掘" ETF, so the guard
# uses the three directly available defensive sector ETFs.
DEFENSIVE_ETFS = {
    "1.512800": "银行",
    "1.515220": "煤炭",
    "1.515210": "钢铁",
}

INDUSTRY_NAME_MAP = {
    "0.159745": "建材",
    "0.159770": "机器人",
    "0.159870": "化工",
    "0.159928": "消费",
    "0.159995": "半导体",
    "1.512000": "证券",
    "1.512010": "医药",
    "1.512170": "医疗",
    "1.512200": "房地产",
    "1.512400": "有色金属",
    "1.512660": "军工",
    "1.512690": "酒",
    "1.512800": "银行",
    "1.512980": "传媒",
    "1.515080": "红利策略",
    "1.515170": "食品饮料",
    "1.515210": "钢铁",
    "1.515220": "煤炭",
    "1.515790": "光伏",
    "1.515880": "通信",
    "1.516160": "新能源",
}


def build_close_df() -> pd.DataFrame:
    panel = pd.read_csv(hedge_mod.DEFAULT_PANEL, usecols=["date", HEDGE_COLUMN])
    panel["date"] = pd.to_datetime(panel["date"])
    hedge = panel.set_index("date")[HEDGE_COLUMN].rename("hedge").astype(float)

    microcap = pd.read_csv(INDEX_CSV, usecols=["date", "close"])
    microcap["date"] = pd.to_datetime(microcap["date"])
    microcap = microcap.set_index("date")["close"].rename("microcap").astype(float)

    close_df = pd.concat([microcap, hedge], axis=1).sort_index().dropna()
    if len(close_df) < LOOKBACK + 3:
        raise ValueError(f"Not enough aligned rows for lookback={LOOKBACK}: got {len(close_df)}")
    return close_df


def load_turnover_table() -> pd.DataFrame:
    turnover = pd.read_csv(TURNOVER_CSV)
    turnover["rebalance_date"] = pd.to_datetime(turnover["rebalance_date"])
    return turnover.sort_values("rebalance_date").reset_index(drop=True)


def load_defensive_panel() -> pd.DataFrame:
    panel = pd.read_csv(DEFENSIVE_PANEL_CSV)
    panel["date"] = pd.to_datetime(panel["date"])
    panel = panel.set_index("date").sort_index()
    industry_cols = [col for col in panel.columns if col != HEDGE_COLUMN]
    panel = panel[industry_cols].apply(pd.to_numeric, errors="coerce")
    return panel


def build_defensive_guard(industry_panel: pd.DataFrame) -> pd.DataFrame:
    ma = industry_panel.rolling(BREADTH_WINDOW).mean()
    bias = industry_panel.div(ma).sub(1.0)
    active_count = bias.notna().sum(axis=1)
    score_for_rank = bias.fillna(float("-inf"))
    top_code = score_for_rank.idxmax(axis=1)
    top_score = score_for_rank.max(axis=1)
    top_code = top_code.where(active_count.gt(0))
    top_score = top_score.where(active_count.gt(0))
    guard_on = (
        active_count.ge(MIN_ACTIVE_INDUSTRIES)
        & top_code.isin(DEFENSIVE_ETFS)
        & top_score.gt(0.0)
    )
    out = pd.DataFrame(
        {
            "guard_on": guard_on,
            "guard_top_code": top_code,
            "guard_top_name": top_code.map(lambda x: INDUSTRY_NAME_MAP.get(x, x)),
            "guard_top_score": top_score,
            "guard_active_industries": active_count,
        }
    )
    return out


def run_backtest_with_guard(close_df: pd.DataFrame, guard_df: pd.DataFrame) -> pd.DataFrame:
    work = close_df.copy()
    work["microcap_ret"] = work["microcap"].pct_change(fill_method=None)
    work["hedge_ret"] = work["hedge"].pct_change(fill_method=None)
    work["microcap_mom"] = hedge_mod.calc_momentum(work["microcap"], LOOKBACK)
    work["hedge_mom"] = hedge_mod.calc_momentum(work["hedge"], LOOKBACK)
    work["momentum_gap"] = work["microcap_mom"] - work["hedge_mom"]

    valid_mask = work[["microcap_mom", "hedge_mom"]].notna().all(axis=1)
    valid_start = valid_mask[valid_mask].index.min()
    if pd.isna(valid_start):
        raise ValueError("No valid momentum history after alignment.")

    work = work.loc[valid_start:].copy()
    guard = guard_df.reindex(work.index)
    rows: list[dict[str, object]] = []
    holding = False

    for i in range(1, len(work)):
        date = work.index[i]
        active_ret = 0.0
        drag = FUTURES_DRAG if holding else 0.0
        if holding:
            microcap_ret = work["microcap_ret"].iloc[i]
            hedge_ret = work["hedge_ret"].iloc[i]
            if pd.notna(microcap_ret) and pd.notna(hedge_ret):
                active_ret = float(microcap_ret - hedge_ret)

        base_signal_on = bool(
            pd.notna(work["microcap_mom"].iloc[i])
            and pd.notna(work["hedge_mom"].iloc[i])
            and work["microcap_mom"].iloc[i] > work["hedge_mom"].iloc[i]
        )
        guard_on = bool(pd.notna(guard["guard_on"].iloc[i]) and guard["guard_on"].iloc[i])
        signal_on = base_signal_on and not guard_on
        day_ret = active_ret - drag
        next_holding = "long_microcap_short_zz1000" if signal_on else "cash"

        rows.append(
            {
                "date": date,
                "return": day_ret,
                "holding": "long_microcap_short_zz1000" if holding else "cash",
                "next_holding": next_holding,
                "signal_on": signal_on,
                "base_signal_on": base_signal_on,
                "guard_on": guard_on,
                "microcap_close": float(work["microcap"].iloc[i]),
                "hedge_close": float(work["hedge"].iloc[i]),
                "microcap_ret": float(work["microcap_ret"].iloc[i]) if pd.notna(work["microcap_ret"].iloc[i]) else np.nan,
                "hedge_ret": float(work["hedge_ret"].iloc[i]) if pd.notna(work["hedge_ret"].iloc[i]) else np.nan,
                "microcap_mom": float(work["microcap_mom"].iloc[i]),
                "hedge_mom": float(work["hedge_mom"].iloc[i]),
                "momentum_gap": float(work["momentum_gap"].iloc[i]),
                "guard_top_code": guard["guard_top_code"].iloc[i] if "guard_top_code" in guard else None,
                "guard_top_name": guard["guard_top_name"].iloc[i] if "guard_top_name" in guard else None,
                "guard_top_score": float(guard["guard_top_score"].iloc[i]) if pd.notna(guard["guard_top_score"].iloc[i]) else np.nan,
                "guard_active_industries": int(guard["guard_active_industries"].iloc[i]) if pd.notna(guard["guard_active_industries"].iloc[i]) else 0,
                "futures_drag": drag,
                "active_spread_ret": active_ret,
            }
        )
        holding = signal_on

    result = pd.DataFrame(rows).set_index("date")
    result["nav"] = (1.0 + result["return"]).cumprod()
    return result


def summarize_period(ret: pd.Series, start_date: pd.Timestamp | None = None) -> dict[str, float | str | None]:
    part = ret.copy()
    if start_date is not None:
        part = part.loc[part.index >= start_date]
    part = part.dropna()
    if len(part) < 30:
        return {
            "start_date": None if start_date is None else str(start_date.date()),
            "end_date": None,
            "annual_pct": np.nan,
            "max_drawdown_pct": np.nan,
            "sharpe": np.nan,
            "total_return_pct": np.nan,
        }
    metrics = hedge_mod.calc_metrics(part)
    return {
        "start_date": str(part.index[0].date()),
        "end_date": str(part.index[-1].date()),
        "annual_pct": metrics.annual * 100.0,
        "max_drawdown_pct": metrics.max_dd * 100.0,
        "sharpe": metrics.sharpe,
        "total_return_pct": metrics.total_return * 100.0,
    }


def build_latest_signal(net: pd.DataFrame) -> pd.DataFrame:
    last = net.iloc[-1].copy()
    out = pd.DataFrame(
        [
            {
                "signal_date": str(net.index[-1].date()),
                "current_holding": last["holding"],
                "next_holding": last["next_holding"],
                "trade_state": (
                    "hold"
                    if last["holding"] == last["next_holding"]
                    else "exit_to_cash"
                    if last["next_holding"] == "cash"
                    else "enter"
                ),
                "base_signal_on": bool(last["base_signal_on"]),
                "guard_on": bool(last["guard_on"]),
                "guard_top_name": last["guard_top_name"],
                "guard_top_score": float(last["guard_top_score"]) if pd.notna(last["guard_top_score"]) else np.nan,
                "microcap_mom": float(last["microcap_mom"]),
                "hedge_mom": float(last["hedge_mom"]),
                "momentum_gap": float(last["momentum_gap"]),
            }
        ]
    )
    return out


def main() -> None:
    close_df = build_close_df()
    turnover = load_turnover_table()
    defensive_panel = load_defensive_panel()
    guard_df = build_defensive_guard(defensive_panel)

    baseline_gross = hedge_mod.run_backtest(
        close_df=close_df,
        signal_model="momentum",
        lookback=LOOKBACK,
        bias_n=hedge_mod.DEFAULT_BIAS_N,
        bias_mom_day=hedge_mod.DEFAULT_BIAS_MOM_DAY,
        futures_drag=FUTURES_DRAG,
        require_positive_microcap_mom=False,
        r2_window=hedge_mod.DEFAULT_R2_WINDOW,
        r2_threshold=0.0,
        vol_scale_enabled=False,
        target_vol=hedge_mod.DEFAULT_TARGET_VOL,
        vol_window=hedge_mod.DEFAULT_VOL_WINDOW,
        max_lev=hedge_mod.DEFAULT_MAX_LEV,
        min_lev=hedge_mod.DEFAULT_MIN_LEV,
        scale_threshold=hedge_mod.DEFAULT_SCALE_THRESHOLD,
    )
    baseline_net = cost_mod.apply_cost_model(baseline_gross, turnover)

    guarded_gross = run_backtest_with_guard(close_df, guard_df)
    guarded_net = cost_mod.apply_cost_model(guarded_gross, turnover)

    latest_signal = build_latest_signal(guarded_net)
    latest_signal.to_csv(OUT_SIGNAL, index=False, encoding="utf-8")
    guarded_net.to_csv(OUT_NAV, index_label="date", encoding="utf-8")

    full_baseline = summarize_period(baseline_net["return_net"])
    full_guarded = summarize_period(guarded_net["return_net"])
    end_date = guarded_net.index[-1]
    last_5y_start = end_date - pd.DateOffset(years=5)
    last_10y_start = end_date - pd.DateOffset(years=10)
    compare = {
        "baseline": {
            "full_sample": full_baseline,
            "last_5_years": summarize_period(baseline_net["return_net"], last_5y_start),
            "last_10_years": summarize_period(baseline_net["return_net"], last_10y_start),
        },
        "with_defensive_guard": {
            "full_sample": full_guarded,
            "last_5_years": summarize_period(guarded_net["return_net"], last_5y_start),
            "last_10_years": summarize_period(guarded_net["return_net"], last_10y_start),
        },
    }
    OUT_COMPARE.write_text(json.dumps(compare, ensure_ascii=False, indent=2), encoding="utf-8")

    summary = {
        "strategy": OUT_PREFIX,
        "base_strategy": "top100_microcap_biweekly_thursday_mom16_zz1000_hedge",
        "guard_definition": {
            "source_logic": "adapted from JSG defensive-sector filter",
            "breadth_proxy": "industry_etf_close / MA20 - 1",
            "top_n": 1,
            "trigger": "top industry is defensive and score > 0",
            "defensive_etfs": DEFENSIVE_ETFS,
            "original_missing_sector": ["采掘I"],
        },
        "sample": {
            "start_date": str(guarded_net.index[0].date()),
            "end_date": str(guarded_net.index[-1].date()),
            "rows": int(len(guarded_net)),
        },
        "guard_stats": {
            "guard_days": int(guarded_net["guard_on"].sum()),
            "guard_days_pct": float(guarded_net["guard_on"].mean()),
            "base_signal_blocked_days": int((guarded_net["base_signal_on"] & guarded_net["guard_on"]).sum()),
            "top_guard_industries": (
                guarded_net.loc[guarded_net["guard_on"], "guard_top_name"]
                .value_counts()
                .head(10)
                .to_dict()
            ),
        },
        "performance": compare,
        "latest_signal": latest_signal.iloc[0].to_dict(),
        "files": {
            "nav_csv": str(OUT_NAV),
            "latest_signal_csv": str(OUT_SIGNAL),
            "summary_json": str(OUT_SUMMARY),
            "compare_json": str(OUT_COMPARE),
        },
    }
    OUT_SUMMARY.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"saved nav: {OUT_NAV}")
    print(f"saved signal: {OUT_SIGNAL}")
    print(f"saved summary: {OUT_SUMMARY}")
    print(json.dumps(compare["with_defensive_guard"]["last_5_years"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
