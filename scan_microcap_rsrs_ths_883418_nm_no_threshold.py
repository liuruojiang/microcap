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

OUT_GRID = OUTPUT_DIR / "microcap_rsrs_ths_883418_nm_no_threshold_scan.csv"
OUT_SUMMARY = OUTPUT_DIR / "microcap_rsrs_ths_883418_nm_no_threshold_summary.json"
OUT_HEATMAP = OUTPUT_DIR / "microcap_rsrs_ths_883418_nm_no_threshold_heatmap.png"

SIGNAL_COL = "right_skew"
N_WINDOWS = [6, 8, 10, 12, 14, 16, 18, 20, 24, 30, 40]
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

    for reg_window in N_WINDOWS:
        for z_window in M_WINDOWS:
            if z_window <= reg_window:
                continue
            rsrs = base.compute_rsrs(raw, reg_window=reg_window, z_window=z_window)
            result = run_no_threshold(rsrs, signal_col=SIGNAL_COL)
            rows.append(
                {
                    "reg_window": reg_window,
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
    best_by_sharpe = df.sort_values(["sharpe", "annual_return"], ascending=False).iloc[0].to_dict()
    best_by_annual = df.sort_values(["annual_return", "sharpe"], ascending=False).iloc[0].to_dict()
    practical = df[
        (df["trades"] >= 12)
        & (df["position_days_pct"] >= 0.15)
        & (df["position_days_pct"] <= 0.70)
    ].sort_values(["sharpe", "annual_return"], ascending=False)
    best_practical = practical.iloc[0].to_dict() if not practical.empty else None
    return {
        "index_code": base.INDEX_CODE,
        "index_name": base.INDEX_NAME,
        "signal_col": SIGNAL_COL,
        "mode": "no_threshold_sign",
        "rule": "hold when signal > 0, else cash",
        "n_windows": N_WINDOWS,
        "m_windows": M_WINDOWS,
        "combos": int(len(df)),
        "sample_start": str(df["sample_start"].iloc[0]),
        "sample_end": str(df["sample_end"].iloc[0]),
        "best_by_sharpe": best_by_sharpe,
        "best_by_annual_return": best_by_annual,
        "best_practical": best_practical,
    }


def plot_heatmap(df: pd.DataFrame) -> None:
    pivot = df.pivot(index="reg_window", columns="z_window", values="sharpe").sort_index()

    fig, ax = plt.subplots(figsize=(12, 8))
    im = ax.imshow(pivot.values, aspect="auto", cmap="RdYlGn")
    ax.set_title("THS 883418 RSRS N-M scan (Sharpe, no threshold)")
    ax.set_xlabel("M window")
    ax.set_ylabel("N window")
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels([str(x) for x in pivot.columns], rotation=45, ha="right")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels([str(x) for x in pivot.index])

    for i in range(pivot.shape[0]):
        for j in range(pivot.shape[1]):
            value = pivot.iloc[i, j]
            if pd.notna(value):
                ax.text(j, i, f"{value:.2f}", ha="center", va="center", fontsize=7, color="black")

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

    print(df.head(20).to_string(index=False))
    print()
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"saved grid: {OUT_GRID}")
    print(f"saved summary: {OUT_SUMMARY}")
    print(f"saved heatmap: {OUT_HEATMAP}")


if __name__ == "__main__":
    main()
