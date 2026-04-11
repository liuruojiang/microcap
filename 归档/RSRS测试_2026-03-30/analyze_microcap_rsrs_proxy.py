from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
INPUT_CSV = OUTPUT_DIR / "wind_microcap_868008_monthly_16y_ohlc_anchored.csv"
OUT_SUMMARY = OUTPUT_DIR / "microcap_rsrs_proxy_summary.json"
OUT_NAV = OUTPUT_DIR / "microcap_rsrs_proxy_nav.csv"
OUT_CHART = OUTPUT_DIR / "microcap_rsrs_proxy_curve.png"
OUT_SIGNAL = OUTPUT_DIR / "microcap_rsrs_proxy_latest_signal.csv"


def load_microcap_ohlc(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"])
    required = ["close"]
    if {"high", "low"}.issubset(df.columns):
        required += ["high", "low"]
    for col in set(required + ["open"]):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates(subset="date")
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


def compute_proxy_rsrs(
    price_df: pd.DataFrame,
    price_window: int = 5,
    reg_window: int = 18,
    z_window: int = 600,
) -> pd.DataFrame:
    df = price_df.copy()
    use_real_ohlc = {"high", "low"}.issubset(df.columns)
    if use_real_ohlc:
        df["proxy_high"] = df["high"]
        df["proxy_low"] = df["low"]
        df["rsrs_mode"] = "daily_ohlc"
    else:
        df["proxy_high"] = df["close"].rolling(price_window).max()
        df["proxy_low"] = df["close"].rolling(price_window).min()
        df["rsrs_mode"] = "close_envelope_proxy"

    beta_values = np.full(len(df), np.nan)
    r2_values = np.full(len(df), np.nan)

    for i in range(reg_window - 1, len(df)):
        window = df.iloc[i - reg_window + 1 : i + 1]
        beta, r2 = ols_slope_and_r2(
            window["proxy_low"].to_numpy(),
            window["proxy_high"].to_numpy(),
        )
        beta_values[i] = beta
        r2_values[i] = r2

    df["beta"] = beta_values
    df["r2"] = r2_values
    beta_mean = df["beta"].rolling(z_window).mean()
    beta_std = df["beta"].rolling(z_window).std(ddof=0).replace(0, np.nan)
    df["zscore"] = (df["beta"] - beta_mean) / beta_std
    df["zscore_r2"] = df["zscore"] * df["r2"]
    df["right_skew"] = df["zscore_r2"] * df["beta"]
    return df


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
    return nav.iloc[-1] ** (1.0 / years) - 1.0


def sharpe_ratio(ret: pd.Series) -> float:
    vol = ret.std()
    if pd.isna(vol) or vol == 0:
        return 0.0
    return float(ret.mean() / vol * np.sqrt(252))


def summarize(df: pd.DataFrame, signal_col: str, buy_threshold: float, sell_threshold: float, price_window: int) -> dict:
    latest = df.iloc[-1]
    rsrs_mode = str(df["rsrs_mode"].dropna().iloc[-1]) if "rsrs_mode" in df.columns and df["rsrs_mode"].notna().any() else "unknown"
    return {
        "input_file": str(INPUT_CSV),
        "note": (
            "RSRS on the Wind microcap proxy. When daily high/low exists, use direct OHLC; "
            "otherwise fall back to rolling close envelopes."
        ),
        "sample_start": df.index[0].strftime("%Y-%m-%d"),
        "sample_end": df.index[-1].strftime("%Y-%m-%d"),
        "rows": int(len(df)),
        "price_window": int(price_window),
        "rsrs_mode": rsrs_mode,
        "signal_col": signal_col,
        "buy_threshold": buy_threshold,
        "sell_threshold": sell_threshold,
        "buy_hold_total_return": float(df["asset_nav"].iloc[-1] - 1.0),
        "rsrs_total_return": float(df["strategy_nav"].iloc[-1] - 1.0),
        "buy_hold_annual_return": float(annual_return(df["asset_nav"])),
        "rsrs_annual_return": float(annual_return(df["strategy_nav"])),
        "buy_hold_sharpe": float(sharpe_ratio(df["asset_ret"])),
        "rsrs_sharpe": float(sharpe_ratio(df["strategy_ret"])),
        "rsrs_max_drawdown": float(df["strategy_drawdown"].min()),
        "position_days_pct": float(df["position"].mean()),
        "latest_date": df.index[-1].strftime("%Y-%m-%d"),
        "latest_close": float(latest["close"]),
        "latest_signal": float(latest[signal_col]) if pd.notna(latest[signal_col]) else None,
        "latest_position": int(latest["position"]),
    }


def plot_result(df: pd.DataFrame, signal_col: str) -> None:
    fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)

    axes[0].plot(df.index, df["close"], color="#1f77b4", linewidth=1.2, label="microcap close")
    axes[0].plot(df.index, df["proxy_high"], color="#ff7f0e", linewidth=0.9, alpha=0.8, label="high")
    axes[0].plot(df.index, df["proxy_low"], color="#2ca02c", linewidth=0.9, alpha=0.8, label="low")
    axes[0].set_title("Wind microcap proxy OHLC")
    axes[0].legend(loc="upper left")
    axes[0].grid(alpha=0.25)

    axes[1].plot(df.index, df["beta"], color="#ff7f0e", linewidth=1.0, label="beta")
    axes[1].plot(df.index, df["zscore_r2"], color="#9467bd", linewidth=1.0, label="zscore_r2")
    axes[1].plot(df.index, df[signal_col], color="#d62728", linewidth=1.0, label=signal_col)
    axes[1].axhline(0.7, color="green", linestyle="--", linewidth=0.8)
    axes[1].axhline(-0.7, color="red", linestyle="--", linewidth=0.8)
    axes[1].set_title("RSRS proxy signal")
    axes[1].legend(loc="upper left")
    axes[1].grid(alpha=0.25)

    axes[2].plot(df.index, df["asset_nav"], color="gray", linewidth=1.2, label="buy and hold")
    axes[2].plot(df.index, df["strategy_nav"], color="#2ca02c", linewidth=1.5, label="RSRS proxy timing")
    axes[2].set_title("NAV")
    axes[2].legend(loc="upper left")
    axes[2].grid(alpha=0.25)

    plt.tight_layout()
    fig.savefig(OUT_CHART, dpi=160, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    ohlc_df = load_microcap_ohlc(INPUT_CSV)
    rsrs_df = compute_proxy_rsrs(ohlc_df, price_window=5, reg_window=18, z_window=600)
    result = run_timing(rsrs_df, signal_col="right_skew", buy_threshold=0.7, sell_threshold=-0.7)

    summary = summarize(
        result,
        signal_col="right_skew",
        buy_threshold=0.7,
        sell_threshold=-0.7,
        price_window=5,
    )

    nav_cols = [
        "close",
        "proxy_high",
        "proxy_low",
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
    result[nav_cols].to_csv(OUT_NAV, encoding="utf-8")
    plot_result(result, signal_col="right_skew")

    latest = result.iloc[[-1]][["close", "right_skew", "position"]].copy()
    latest = latest.reset_index().rename(columns={"date": "signal_date"})
    latest.to_csv(OUT_SIGNAL, index=False, encoding="utf-8")

    with OUT_SUMMARY.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"saved summary: {OUT_SUMMARY}")
    print(f"saved nav: {OUT_NAV}")
    print(f"saved chart: {OUT_CHART}")
    print(f"saved signal: {OUT_SIGNAL}")


if __name__ == "__main__":
    main()
