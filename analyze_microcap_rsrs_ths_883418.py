from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from py_mini_racer import MiniRacer

import akshare.stock_feature.stock_board_concept_ths as ths_module


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

INDEX_CODE = "883418"
INDEX_NAME = "同花顺微盘股"
START_DATE = "20100101"
END_DATE = "20260330"

OUT_CSV = OUTPUT_DIR / "microcap_rsrs_ths_883418_nav.csv"
OUT_SUMMARY = OUTPUT_DIR / "microcap_rsrs_ths_883418_summary.json"
OUT_SIGNAL = OUTPUT_DIR / "microcap_rsrs_ths_883418_latest_signal.csv"
OUT_CHART = OUTPUT_DIR / "microcap_rsrs_ths_883418_curve.png"


def build_v_cookie() -> str:
    js_code = MiniRacer()
    js_code.eval(ths_module._get_file_content_ths("ths.js"))
    return js_code.call("v")


def fetch_ths_index_history(index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    v_code = build_v_cookie()
    begin_year = int(start_date[:4])
    end_year = int(end_date[:4])
    frames: list[pd.DataFrame] = []
    session = requests.Session()

    for year in range(begin_year, end_year + 1):
        url = f"https://d.10jqka.com.cn/v4/line/bk_{index_code}/01/{year}.js"
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "http://q.10jqka.com.cn",
            "Host": "d.10jqka.com.cn",
            "Cookie": f"v={v_code}",
        }
        text = ""
        for _ in range(3):
            try:
                resp = session.get(url, headers=headers, timeout=20)
                if resp.status_code == 200:
                    text = resp.text
                    break
            except requests.RequestException:
                continue
        left = text.find("{")
        if left < 0:
            continue
        try:
            payload = json.loads(text[left:-1])
        except json.JSONDecodeError:
            continue
        data = payload.get("data", "")
        if not data:
            continue
        temp = pd.DataFrame([row.split(",") for row in data.split(";") if row.strip()])
        if temp.empty:
            continue
        temp = temp.iloc[:, :7]
        temp.columns = ["date", "open", "high", "low", "close", "volume", "amount"]
        frames.append(temp)

    if not frames:
        raise RuntimeError(f"No THS history fetched for {index_code}")

    df = pd.concat(frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["date", "high", "low", "close"]).drop_duplicates(subset="date").sort_values("date")
    df = df[(df["date"] >= pd.Timestamp(start_date)) & (df["date"] <= pd.Timestamp(end_date))]
    if df.empty:
        raise RuntimeError(f"THS history for {index_code} is empty after date filter")
    return df.set_index("date")


def ols_slope_and_r2(x: np.ndarray, y: np.ndarray) -> tuple[float, float]:
    mask = np.isfinite(x) & np.isfinite(y)
    x = x[mask]
    y = y[mask]
    if len(x) < 2:
        return np.nan, np.nan

    x_mean = x.mean()
    y_mean = y.mean()
    ss_xx = np.square(x - x_mean).sum()
    if ss_xx == 0:
        return np.nan, np.nan

    beta = ((x - x_mean) * (y - y_mean)).sum() / ss_xx
    alpha = y_mean - beta * x_mean
    fitted = alpha + beta * x
    ss_tot = np.square(y - y_mean).sum()
    ss_res = np.square(y - fitted).sum()
    r2 = 0.0 if ss_tot == 0 else 1.0 - ss_res / ss_tot
    return float(beta), float(r2)


def compute_rsrs(df: pd.DataFrame, reg_window: int = 18, z_window: int = 600) -> pd.DataFrame:
    out = df.copy()
    beta_values = np.full(len(out), np.nan)
    r2_values = np.full(len(out), np.nan)

    for i in range(reg_window - 1, len(out)):
        window = out.iloc[i - reg_window + 1 : i + 1]
        beta, r2 = ols_slope_and_r2(window["low"].to_numpy(), window["high"].to_numpy())
        beta_values[i] = beta
        r2_values[i] = r2

    out["beta"] = beta_values
    out["r2"] = r2_values
    beta_mean = out["beta"].rolling(z_window).mean()
    beta_std = out["beta"].rolling(z_window).std(ddof=0).replace(0, np.nan)
    out["zscore"] = (out["beta"] - beta_mean) / beta_std
    out["zscore_r2"] = out["zscore"] * out["r2"]
    out["right_skew"] = out["zscore_r2"] * out["beta"]
    return out


def run_timing(
    df: pd.DataFrame,
    signal_col: str = "right_skew",
    buy_threshold: float = 0.7,
    sell_threshold: float = -0.7,
) -> pd.DataFrame:
    out = df.copy()
    pos = 0
    positions = []

    for score in out[signal_col]:
        if np.isnan(score):
            positions.append(pos)
            continue
        if score > buy_threshold:
            pos = 1
        elif score < sell_threshold:
            pos = 0
        positions.append(pos)

    out["position"] = positions
    out["asset_ret"] = out["close"].pct_change().fillna(0.0)
    out["strategy_ret"] = out["position"].shift(1).fillna(0.0) * out["asset_ret"]
    out["asset_nav"] = (1.0 + out["asset_ret"]).cumprod()
    out["strategy_nav"] = (1.0 + out["strategy_ret"]).cumprod()
    out["strategy_peak"] = out["strategy_nav"].cummax()
    out["strategy_drawdown"] = out["strategy_nav"] / out["strategy_peak"] - 1.0
    return out


def annual_return(nav: pd.Series) -> float:
    years = (nav.index[-1] - nav.index[0]).days / 365.25
    if years <= 0:
        return np.nan
    return float(nav.iloc[-1] ** (1.0 / years) - 1.0)


def sharpe_ratio(ret: pd.Series) -> float:
    vol = ret.std()
    if pd.isna(vol) or vol == 0:
        return 0.0
    return float(ret.mean() / vol * np.sqrt(252))


def summarize(
    df: pd.DataFrame,
    signal_col: str,
    buy_threshold: float,
    sell_threshold: float,
    reg_window: int,
    z_window: int,
) -> dict:
    latest = df.iloc[-1]
    return {
        "index_code": INDEX_CODE,
        "index_name": INDEX_NAME,
        "sample_start": df.index[0].strftime("%Y-%m-%d"),
        "sample_end": df.index[-1].strftime("%Y-%m-%d"),
        "rows": int(len(df)),
        "reg_window": reg_window,
        "z_window": z_window,
        "signal_col": signal_col,
        "buy_threshold": buy_threshold,
        "sell_threshold": sell_threshold,
        "buy_hold_total_return": float(df["asset_nav"].iloc[-1] - 1.0),
        "rsrs_total_return": float(df["strategy_nav"].iloc[-1] - 1.0),
        "buy_hold_annual_return": annual_return(df["asset_nav"]),
        "rsrs_annual_return": annual_return(df["strategy_nav"]),
        "buy_hold_sharpe": sharpe_ratio(df["asset_ret"]),
        "rsrs_sharpe": sharpe_ratio(df["strategy_ret"]),
        "rsrs_max_drawdown": float(df["strategy_drawdown"].min()),
        "position_days_pct": float(df["position"].mean()),
        "latest_date": df.index[-1].strftime("%Y-%m-%d"),
        "latest_close": float(latest["close"]),
        "latest_signal": float(latest[signal_col]) if pd.notna(latest[signal_col]) else None,
        "latest_position": int(latest["position"]),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def plot_result(df: pd.DataFrame, signal_col: str, buy_threshold: float, sell_threshold: float) -> None:
    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)

    axes[0].plot(df.index, df["close"], color="#1f77b4", linewidth=1.2, label="THS Microcap")
    axes[0].set_title(f"THS Microcap {INDEX_CODE} close")
    axes[0].legend(loc="upper left")
    axes[0].grid(alpha=0.25)

    axes[1].plot(df.index, df["beta"], color="#ff7f0e", linewidth=1.0, label="beta")
    axes[1].plot(df.index, df["zscore_r2"], color="#9467bd", linewidth=1.0, label="zscore_r2")
    axes[1].plot(df.index, df[signal_col], color="#d62728", linewidth=1.0, label=signal_col)
    axes[1].axhline(buy_threshold, color="green", linestyle="--", linewidth=0.8)
    axes[1].axhline(sell_threshold, color="red", linestyle="--", linewidth=0.8)
    axes[1].set_title("RSRS signal")
    axes[1].legend(loc="upper left")
    axes[1].grid(alpha=0.25)

    axes[2].plot(df.index, df["asset_nav"], color="gray", linewidth=1.2, label="buy and hold")
    axes[2].plot(df.index, df["strategy_nav"], color="#2ca02c", linewidth=1.5, label="RSRS timing")
    axes[2].set_title("NAV")
    axes[2].legend(loc="upper left")
    axes[2].grid(alpha=0.25)

    plt.tight_layout()
    fig.savefig(OUT_CHART, dpi=160, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    raw = fetch_ths_index_history(INDEX_CODE, START_DATE, END_DATE)
    rsrs = compute_rsrs(raw, reg_window=18, z_window=600)
    result = run_timing(rsrs, signal_col="right_skew", buy_threshold=0.7, sell_threshold=-0.7)

    summary = summarize(
        result,
        signal_col="right_skew",
        buy_threshold=0.7,
        sell_threshold=-0.7,
        reg_window=18,
        z_window=600,
    )

    keep_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "beta",
        "r2",
        "zscore",
        "zscore_r2",
        "right_skew",
        "position",
        "asset_nav",
        "strategy_nav",
        "strategy_drawdown",
    ]
    result[keep_cols].to_csv(OUT_CSV, encoding="utf-8")
    plot_result(result, signal_col="right_skew", buy_threshold=0.7, sell_threshold=-0.7)

    latest = result.iloc[[-1]][["close", "right_skew", "position"]].copy()
    latest = latest.reset_index().rename(columns={"date": "signal_date"})
    latest.to_csv(OUT_SIGNAL, index=False, encoding="utf-8")

    with OUT_SUMMARY.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"saved nav: {OUT_CSV}")
    print(f"saved summary: {OUT_SUMMARY}")
    print(f"saved chart: {OUT_CHART}")
    print(f"saved signal: {OUT_SIGNAL}")


if __name__ == "__main__":
    main()
