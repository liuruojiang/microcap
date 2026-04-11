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

OUT_GRID = OUTPUT_DIR / "microcap_rsrs_ths_883418_scan.csv"
OUT_SUMMARY = OUTPUT_DIR / "microcap_rsrs_ths_883418_scan_summary.json"
OUT_HEATMAP = OUTPUT_DIR / "microcap_rsrs_ths_883418_scan_heatmap.png"

REG_WINDOW = 18
SIGNAL_COL = "right_skew"
M_WINDOWS = [40, 60, 80, 100, 120, 160, 200, 250, 300, 400, 500]
THRESHOLDS = [0.3, 0.5, 0.7, 0.9, 1.1, 1.3, 1.5]


def compute_trade_count(position: pd.Series) -> int:
    shifted = position.shift(1).fillna(position.iloc[0])
    return int((position != shifted).sum())


def scan_grid() -> pd.DataFrame:
    raw = base.fetch_ths_index_history(base.INDEX_CODE, base.START_DATE, base.END_DATE)
    rows = []

    for z_window in M_WINDOWS:
        rsrs = base.compute_rsrs(raw, reg_window=REG_WINDOW, z_window=z_window)
        for threshold in THRESHOLDS:
            result = base.run_timing(
                rsrs,
                signal_col=SIGNAL_COL,
                buy_threshold=threshold,
                sell_threshold=-threshold,
            )
            rows.append(
                {
                    "z_window": z_window,
                    "threshold": threshold,
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

    df = pd.DataFrame(rows)
    return df.sort_values(["sharpe", "annual_return"], ascending=False).reset_index(drop=True)


def build_summary(df: pd.DataFrame) -> dict:
    top_sharpe = df.sort_values(["sharpe", "annual_return"], ascending=False).iloc[0].to_dict()
    top_annual = df.sort_values(["annual_return", "sharpe"], ascending=False).iloc[0].to_dict()
    low_dd = df.sort_values(["max_drawdown", "sharpe"], ascending=[False, False]).iloc[0].to_dict()

    return {
        "index_code": base.INDEX_CODE,
        "index_name": base.INDEX_NAME,
        "signal_col": SIGNAL_COL,
        "reg_window": REG_WINDOW,
        "m_windows": M_WINDOWS,
        "thresholds": THRESHOLDS,
        "combos": int(len(df)),
        "sample_start": str(df["sample_start"].iloc[0]),
        "sample_end": str(df["sample_end"].iloc[0]),
        "best_by_sharpe": top_sharpe,
        "best_by_annual_return": top_annual,
        "smallest_drawdown": low_dd,
    }


def plot_heatmap(df: pd.DataFrame) -> None:
    pivot = df.pivot(index="z_window", columns="threshold", values="sharpe").sort_index()

    fig, ax = plt.subplots(figsize=(10, 7))
    im = ax.imshow(pivot.values, aspect="auto", cmap="RdYlGn")
    ax.set_title("THS 883418 RSRS scan heatmap (Sharpe)")
    ax.set_xlabel("Threshold")
    ax.set_ylabel("M window")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([f"{x:.1f}" for x in pivot.columns])
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels([str(x) for x in pivot.index])

    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            value = pivot.iloc[i, j]
            ax.text(j, i, f"{value:.2f}", ha="center", va="center", fontsize=8, color="black")

    fig.colorbar(im, ax=ax, shrink=0.85, label="Sharpe")
    plt.tight_layout()
    fig.savefig(OUT_HEATMAP, dpi=160, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    df = scan_grid()
    df.to_csv(OUT_GRID, index=False, encoding="utf-8")

    summary = build_summary(df)
    with OUT_SUMMARY.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    plot_heatmap(df)

    print(df.head(15).to_string(index=False))
    print()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"saved grid: {OUT_GRID}")
    print(f"saved summary: {OUT_SUMMARY}")
    print(f"saved heatmap: {OUT_HEATMAP}")


if __name__ == "__main__":
    main()
