from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import analyze_microcap_rsrs_proxy as rsrs_mod
import analyze_microcap_rsrs_proxy_zz1000_hedge as base_path_mod
import analyze_microcap_zz1000_hedge as hedge_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

TOP100_OHLC_CSV = OUTPUT_DIR / "wind_microcap_top_100_monthly_16y_ohlc_anchored.csv"
HEDGE_COLUMN = hedge_mod.DEFAULT_HEDGE_COLUMN
FUTURES_DRAG = hedge_mod.DEFAULT_FUTURES_DRAG

REG_WINDOW = 20
Z_WINDOW = 80
SIGNAL_COL = "right_skew"

OUT_PREFIX = "top100_relative_rsrs_n20_m80_zz1000_hedged"
OUT_NAV = OUTPUT_DIR / f"{OUT_PREFIX}_nav.csv"
OUT_SIGNAL = OUTPUT_DIR / f"{OUT_PREFIX}_latest_signal.csv"
OUT_SUMMARY = OUTPUT_DIR / f"{OUT_PREFIX}_summary.json"
OUT_CHART = OUTPUT_DIR / f"{OUT_PREFIX}_curve.png"


def load_hedge_series() -> pd.Series:
    panel_path = base_path_mod.resolve_panel_path()
    panel = pd.read_csv(panel_path, usecols=["date", HEDGE_COLUMN])
    panel["date"] = pd.to_datetime(panel["date"])
    panel[HEDGE_COLUMN] = pd.to_numeric(panel[HEDGE_COLUMN], errors="coerce")
    panel = panel.dropna(subset=[HEDGE_COLUMN]).sort_values("date").drop_duplicates(subset="date")
    return panel.set_index("date")[HEDGE_COLUMN].rename("hedge")


def build_close_df() -> pd.DataFrame:
    top100_ohlc = rsrs_mod.load_microcap_ohlc(TOP100_OHLC_CSV)
    hedge = load_hedge_series()
    close_df = pd.concat(
        [
            top100_ohlc.rename(columns={"close": "microcap"}),
            hedge,
        ],
        axis=1,
    ).sort_index()
    close_df = close_df.dropna(subset=["open", "high", "low", "microcap", "hedge"]).copy()
    return close_df


def build_relative_ohlc(close_df: pd.DataFrame) -> pd.DataFrame:
    rel = pd.DataFrame(index=close_df.index)
    rel["open"] = close_df["open"] / close_df["hedge"]
    rel["high"] = close_df["high"] / close_df["hedge"]
    rel["low"] = close_df["low"] / close_df["hedge"]
    rel["close"] = close_df["microcap"] / close_df["hedge"]
    rel["high"] = rel[["open", "high", "low", "close"]].max(axis=1)
    rel["low"] = rel[["open", "high", "low", "close"]].min(axis=1)
    return rel


def run_backtest(close_df: pd.DataFrame) -> pd.DataFrame:
    relative_ohlc = build_relative_ohlc(close_df)
    rsrs_df = rsrs_mod.compute_proxy_rsrs(relative_ohlc, reg_window=REG_WINDOW, z_window=Z_WINDOW)

    work = close_df.join(
        relative_ohlc.rename(
            columns={
                "open": "ratio_open",
                "high": "ratio_high",
                "low": "ratio_low",
                "close": "ratio_close",
            }
        ),
        how="left",
    ).join(
        rsrs_df[["beta", "r2", "zscore", "zscore_r2", "right_skew"]],
        how="left",
    ).copy()
    work["microcap_ret"] = work["microcap"].pct_change(fill_method=None)
    work["hedge_ret"] = work["hedge"].pct_change(fill_method=None)

    valid_start = work[SIGNAL_COL].dropna().index.min()
    if pd.isna(valid_start):
        raise ValueError("No valid relative RSRS history after alignment.")

    work = work.loc[valid_start:].copy()
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

        signal_on = bool(pd.notna(work[SIGNAL_COL].iloc[i]) and work[SIGNAL_COL].iloc[i] > 0.0)
        rows.append(
            {
                "date": date,
                "return": active_ret - drag,
                "holding": "long_top100_short_zz1000" if holding else "cash",
                "next_holding": "long_top100_short_zz1000" if signal_on else "cash",
                "signal_on": signal_on,
                "microcap_close": float(work["microcap"].iloc[i]),
                "hedge_close": float(work["hedge"].iloc[i]),
                "ratio_close": float(work["ratio_close"].iloc[i]),
                "microcap_ret": float(work["microcap_ret"].iloc[i]) if pd.notna(work["microcap_ret"].iloc[i]) else np.nan,
                "hedge_ret": float(work["hedge_ret"].iloc[i]) if pd.notna(work["hedge_ret"].iloc[i]) else np.nan,
                "active_spread_ret": active_ret,
                "futures_drag": drag,
                "beta": float(work["beta"].iloc[i]) if pd.notna(work["beta"].iloc[i]) else np.nan,
                "r2": float(work["r2"].iloc[i]) if pd.notna(work["r2"].iloc[i]) else np.nan,
                "zscore": float(work["zscore"].iloc[i]) if pd.notna(work["zscore"].iloc[i]) else np.nan,
                "zscore_r2": float(work["zscore_r2"].iloc[i]) if pd.notna(work["zscore_r2"].iloc[i]) else np.nan,
                "right_skew": float(work["right_skew"].iloc[i]) if pd.notna(work["right_skew"].iloc[i]) else np.nan,
            }
        )
        holding = signal_on

    result = pd.DataFrame(rows).set_index("date")
    result["nav"] = (1.0 + result["return"]).cumprod()
    return result


def build_summary(result: pd.DataFrame, close_df: pd.DataFrame) -> dict[str, object]:
    metrics = hedge_mod.calc_metrics(result["return"])
    active_series = result["holding"] != "cash"
    spell_ids = result["holding"].ne(result["holding"].shift()).cumsum()
    spells = pd.DataFrame({"holding": result["holding"], "spell_id": spell_ids}).loc[active_series].groupby("spell_id").size()
    latest = result.iloc[-1]
    yearly: dict[str, float] = {}
    for year in sorted(result.index.year.unique()):
        part = result.loc[result.index.year == year, "return"]
        if len(part) > 10:
            yearly[str(year)] = float((1.0 + part).prod() - 1.0)
    return {
        "strategy": OUT_PREFIX,
        "top100_csv": str(TOP100_OHLC_CSV),
        "panel_path": str(base_path_mod.resolve_panel_path()),
        "hedge_column": HEDGE_COLUMN,
        "reg_window": REG_WINDOW,
        "z_window": Z_WINDOW,
        "signal_col": SIGNAL_COL,
        "signal_price_definition": "relative_ohlc = top100_ohlc / zz1000_close",
        "signal_note": (
            "Top100 OHLC is reconstructed from constituent OHLC and anchored to the existing Top100 close series. "
            "Relative OHLC then divides Top100 OHLC by same-day CSI1000 close."
        ),
        "entry_rule": "relative right_skew > 0",
        "execution_rule": "signal decided on day t, effective on day t+1",
        "return_rule_when_active": "top100_return - hedge_return - futures_drag_per_day",
        "futures_drag_per_day": FUTURES_DRAG,
        "start_date": str(result.index[0].date()),
        "end_date": str(result.index[-1].date()),
        "n_days": int(len(result)),
        "active_days_pct": float(active_series.mean()),
        "cash_days_pct": float((~active_series).mean()),
        "signal_changes": int(result["signal_on"].ne(result["signal_on"].shift()).sum() - 1),
        "median_holding_spell": float(spells.median()) if len(spells) else 0.0,
        "latest_signal": {
            "date": str(result.index[-1].date()),
            "next_holding": str(latest["next_holding"]),
            "right_skew": float(latest["right_skew"]),
            "beta": float(latest["beta"]),
            "r2": float(latest["r2"]),
            "zscore": float(latest["zscore"]),
            "zscore_r2": float(latest["zscore_r2"]),
            "ratio_close": float(latest["ratio_close"]),
            "top100_close": float(close_df["microcap"].loc[result.index[-1]]),
            "hedge_close": float(close_df["hedge"].loc[result.index[-1]]),
        },
        "metrics": {
            "annual": metrics.annual,
            "vol": metrics.vol,
            "sharpe": metrics.sharpe,
            "max_dd": metrics.max_dd,
            "calmar": metrics.calmar,
            "total_return": metrics.total_return,
            "win_rate": metrics.win_rate,
        },
        "yearly": yearly,
    }


def build_latest_signal(result: pd.DataFrame) -> pd.DataFrame:
    last = result.iloc[[-1]].copy().reset_index()
    last["signal_label"] = np.where(last["next_holding"] == "cash", "cash", "long_top100_short_zz1000")
    return last[
        [
            "date",
            "signal_label",
            "next_holding",
            "microcap_close",
            "hedge_close",
            "ratio_close",
            "beta",
            "r2",
            "zscore",
            "zscore_r2",
            "right_skew",
            "futures_drag",
        ]
    ]


def plot_nav(result: pd.DataFrame) -> None:
    plt.figure(figsize=(12, 6))
    plt.plot(result.index, result["nav"], linewidth=1.8, label="Top100 Relative RSRS / CSI1000 Hedge")
    plt.title("Top100 Relative RSRS + CSI1000 Hedge NAV")
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUT_CHART, dpi=160)
    plt.close()


def main() -> None:
    close_df = build_close_df()
    result = run_backtest(close_df)
    latest_signal = build_latest_signal(result)
    summary = build_summary(result, close_df)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUT_NAV, index_label="date", encoding="utf-8")
    latest_signal.to_csv(OUT_SIGNAL, index=False, encoding="utf-8")
    OUT_SUMMARY.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    plot_nav(result)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"saved {OUT_NAV.name}")
    print(f"saved {OUT_SIGNAL.name}")
    print(f"saved {OUT_SUMMARY.name}")
    print(f"saved {OUT_CHART.name}")


if __name__ == "__main__":
    main()
