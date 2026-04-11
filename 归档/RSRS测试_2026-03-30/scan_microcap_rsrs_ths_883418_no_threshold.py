from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

import analyze_microcap_rsrs_ths_883418 as base


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

OUT_GRID = OUTPUT_DIR / "microcap_rsrs_ths_883418_no_threshold_scan.csv"
OUT_SUMMARY = OUTPUT_DIR / "microcap_rsrs_ths_883418_no_threshold_summary.json"
OUT_CHART = OUTPUT_DIR / "microcap_rsrs_ths_883418_no_threshold_scan.png"

REG_WINDOW = 18
SIGNAL_COL = "right_skew"
M_WINDOWS = [20, 30, 40, 50, 60, 80, 100, 120, 160, 200, 250, 300, 400, 500]


def run_no_threshold(df: pd.DataFrame, signal_col: str) -> pd.DataFrame:
    out = df.copy()
    out["position"] = (out[signal_col] > 0).astype(int)
    out["asset_ret"] = out["close"].pct_change().fillna(0.0)
    out["strategy_ret"] = out["position"].shift(1).fillna(0.0) * out["asset_ret"]
    out["asset_nav"] = (1.0 + out["asset_ret"]).cumprod()
    out["strategy_nav"] = (1.0 + out["strategy_ret"]).cumprod()
    out["strategy_peak"] = out["strategy_nav"].cummax()
    out["strategy_drawdown"] = out["strategy_nav"] / out["strategy_peak"] - 1.0
    return out


def compute_trade_count(position: pd.Series) -> int:
    shifted = position.shift(1).fillna(position.iloc[0])
    return int((position != shifted).sum())


def scan_grid() -> pd.DataFrame:
    raw = base.fetch_ths_index_history(base.INDEX_CODE, base.START_DATE, base.END_DATE)
    rows = []

    for z_window in M_WINDOWS:
        rsrs = base.compute_rsrs(raw, reg_window=REG_WINDOW, z_window=z_window)
        result = run_no_threshold(rsrs, signal_col=SIGNAL_COL)
        rows.append(
            {
                "z_window": z_window,
                "rows": len(result),
                "sample_start": result.index[0].strftime("%Y-%m-%d"),
                "sample_end": result.index[-1].strftime("%Y-%m-%d"),
                "total_return": float(result["strategy_nav"].iloc[-1] - 1.0),
                "annual_return": base.annual_return(result["strategy_nav"]),
                "sharpe": base.sharpe_ratio(result["strategy_ret"]),
                "max_drawdown": float(result["strategy_drawdown"].min()),
                "position_days_pct": float(result["position"].mean()),
                "trades": compute_trade_count(result["position"]),
            }
        )

    return pd.DataFrame(rows).sort_values(["sharpe", "annual_return"], ascending=False).reset_index(drop=True)


def build_summary(df: pd.DataFrame) -> dict:
    return {
        "index_code": base.INDEX_CODE,
        "index_name": base.INDEX_NAME,
        "signal_col": SIGNAL_COL,
        "reg_window": REG_WINDOW,
        "mode": "no_threshold_sign",
        "rule": "hold when signal > 0, else cash",
        "m_windows": M_WINDOWS,
        "combos": int(len(df)),
        "sample_start": str(df["sample_start"].iloc[0]),
        "sample_end": str(df["sample_end"].iloc[0]),
        "best_by_sharpe": df.sort_values(["sharpe", "annual_return"], ascending=False).iloc[0].to_dict(),
        "best_by_annual_return": df.sort_values(["annual_return", "sharpe"], ascending=False).iloc[0].to_dict(),
    }


def plot_scan(df: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(10, 9), sharex=True)

    axes[0].plot(df["z_window"], df["sharpe"], marker="o", color="#1f77b4", linewidth=1.5)
    axes[0].set_title("THS 883418 RSRS no-threshold scan")
    axes[0].set_ylabel("Sharpe")
    axes[0].grid(alpha=0.25)

    axes[1].plot(df["z_window"], df["annual_return"], marker="o", color="#2ca02c", linewidth=1.5, label="annual")
    axes[1].plot(df["z_window"], df["max_drawdown"], marker="o", color="#d62728", linewidth=1.2, label="max drawdown")
    axes[1].set_xlabel("M window")
    axes[1].set_ylabel("Return / Drawdown")
    axes[1].legend(loc="best")
    axes[1].grid(alpha=0.25)

    plt.tight_layout()
    fig.savefig(OUT_CHART, dpi=160, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    df = scan_grid()
    df.to_csv(OUT_GRID, index=False, encoding="utf-8")

    summary = build_summary(df)
    with OUT_SUMMARY.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    plot_scan(df)

    print(df.to_string(index=False))
    print()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"saved grid: {OUT_GRID}")
    print(f"saved summary: {OUT_SUMMARY}")
    print(f"saved chart: {OUT_CHART}")


if __name__ == "__main__":
    main()
