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

TARGET_N = 24
TARGET_M = 200
SIGNAL_COL = "right_skew"

OUT_SUMMARY = OUTPUT_DIR / "microcap_rsrs_ths_883418_overfit_check_summary.json"
OUT_SEGMENTS = OUTPUT_DIR / "microcap_rsrs_ths_883418_overfit_check_segments.csv"
OUT_NEIGHBOR = OUTPUT_DIR / "microcap_rsrs_ths_883418_overfit_check_neighbors.csv"
OUT_STRESS = OUTPUT_DIR / "microcap_rsrs_ths_883418_overfit_check_stress.csv"
OUT_CHART = OUTPUT_DIR / "microcap_rsrs_ths_883418_overfit_check.png"


def run_no_threshold(
    df: pd.DataFrame,
    signal_col: str = SIGNAL_COL,
    execution_delay_days: int = 1,
    switch_cost: float = 0.0,
) -> pd.DataFrame:
    out = df.copy()
    out["signal_position"] = (out[signal_col] > 0).astype(int)
    exec_pos = out["signal_position"].shift(execution_delay_days).fillna(0.0)
    out["position"] = exec_pos
    out["asset_ret"] = out["close"].pct_change().fillna(0.0)
    out["turnover"] = out["position"].diff().abs().fillna(out["position"].abs())
    out["strategy_ret_gross"] = out["position"] * out["asset_ret"]
    out["strategy_ret"] = out["strategy_ret_gross"] - out["turnover"] * switch_cost
    out["asset_nav"] = (1.0 + out["asset_ret"]).cumprod()
    out["strategy_nav"] = (1.0 + out["strategy_ret"]).cumprod()
    out["strategy_peak"] = out["strategy_nav"].cummax()
    out["strategy_drawdown"] = out["strategy_nav"] / out["strategy_peak"] - 1.0
    return out


def annual_return(nav: pd.Series) -> float:
    years = (nav.index[-1] - nav.index[0]).days / 365.25
    if years <= 0:
        return 0.0
    return float(nav.iloc[-1] ** (1.0 / years) - 1.0)


def sharpe_ratio(ret: pd.Series) -> float:
    vol = ret.std()
    if pd.isna(vol) or vol == 0:
        return 0.0
    return float(ret.mean() / vol * (252 ** 0.5))


def summarize_run(df: pd.DataFrame, label: str) -> dict:
    return {
        "label": label,
        "sample_start": df.index[0].strftime("%Y-%m-%d"),
        "sample_end": df.index[-1].strftime("%Y-%m-%d"),
        "rows": int(len(df)),
        "total_return": float(df["strategy_nav"].iloc[-1] - 1.0),
        "annual_return": annual_return(df["strategy_nav"]),
        "sharpe": sharpe_ratio(df["strategy_ret"]),
        "max_drawdown": float(df["strategy_drawdown"].min()),
        "position_days_pct": float(df["position"].mean()),
        "trades": int((df["position"].diff().abs().fillna(df["position"].abs()) > 0).sum()),
    }


def build_segment_table(df: pd.DataFrame) -> pd.DataFrame:
    segments = []

    thirds = pd.qcut(range(len(df)), q=3, labels=["segment_1", "segment_2", "segment_3"])
    temp = df.copy()
    temp["segment"] = list(thirds)
    for name, sub in temp.groupby("segment", observed=False):
        segments.append(summarize_run(sub.drop(columns=["segment"]), str(name)))

    for year, sub in df.groupby(df.index.year):
        if len(sub) < 30:
            continue
        segments.append(summarize_run(sub, f"year_{year}"))

    return pd.DataFrame(segments)


def build_neighbor_table(raw: pd.DataFrame) -> pd.DataFrame:
    rows = []
    n_grid = [16, 20, 24, 30]
    m_grid = [120, 160, 200, 250, 300]
    for reg_window in n_grid:
        for z_window in m_grid:
            if z_window <= reg_window:
                continue
            rsrs = base.compute_rsrs(raw, reg_window=reg_window, z_window=z_window)
            result = run_no_threshold(rsrs)
            item = summarize_run(result, f"N{reg_window}_M{z_window}")
            item["reg_window"] = reg_window
            item["z_window"] = z_window
            rows.append(item)
    return pd.DataFrame(rows).sort_values(["sharpe", "annual_return"], ascending=False).reset_index(drop=True)


def build_stress_table(rsrs: pd.DataFrame) -> pd.DataFrame:
    rows = []
    stress_cases = [
        ("base_delay1_cost0bp", 1, 0.0000),
        ("delay2_cost0bp", 2, 0.0000),
        ("delay1_cost10bp", 1, 0.0010),
        ("delay1_cost20bp", 1, 0.0020),
        ("delay2_cost10bp", 2, 0.0010),
    ]
    for label, delay_days, switch_cost in stress_cases:
        result = run_no_threshold(rsrs, execution_delay_days=delay_days, switch_cost=switch_cost)
        item = summarize_run(result, label)
        item["delay_days"] = delay_days
        item["switch_cost"] = switch_cost
        rows.append(item)
    return pd.DataFrame(rows)


def plot_diagnostics(base_result: pd.DataFrame, neighbor_df: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(12, 10))

    axes[0].plot(base_result.index, base_result["asset_nav"], color="gray", linewidth=1.2, label="buy and hold")
    axes[0].plot(base_result.index, base_result["strategy_nav"], color="#2ca02c", linewidth=1.6, label=f"RSRS N={TARGET_N}, M={TARGET_M}")
    axes[0].set_title("THS 883418 overfit check")
    axes[0].legend(loc="upper left")
    axes[0].grid(alpha=0.25)

    heat = neighbor_df.pivot(index="reg_window", columns="z_window", values="sharpe").sort_index()
    im = axes[1].imshow(heat.values, aspect="auto", cmap="RdYlGn")
    axes[1].set_title("Neighbor Sharpe surface")
    axes[1].set_xlabel("M window")
    axes[1].set_ylabel("N window")
    axes[1].set_xticks(range(len(heat.columns)))
    axes[1].set_xticklabels([str(x) for x in heat.columns])
    axes[1].set_yticks(range(len(heat.index)))
    axes[1].set_yticklabels([str(x) for x in heat.index])
    for i in range(heat.shape[0]):
        for j in range(heat.shape[1]):
            value = heat.iloc[i, j]
            axes[1].text(j, i, f"{value:.2f}", ha="center", va="center", fontsize=8, color="black")
    fig.colorbar(im, ax=axes[1], shrink=0.85, label="Sharpe")

    plt.tight_layout()
    fig.savefig(OUT_CHART, dpi=160, bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    raw = base.fetch_ths_index_history(base.INDEX_CODE, base.START_DATE, base.END_DATE)
    rsrs = base.compute_rsrs(raw, reg_window=TARGET_N, z_window=TARGET_M)
    base_result = run_no_threshold(rsrs)
    base_summary = summarize_run(base_result, "target")

    segment_df = build_segment_table(base_result)
    neighbor_df = build_neighbor_table(raw)
    stress_df = build_stress_table(rsrs)
    plot_diagnostics(base_result, neighbor_df)

    segment_df.to_csv(OUT_SEGMENTS, index=False, encoding="utf-8")
    neighbor_df.to_csv(OUT_NEIGHBOR, index=False, encoding="utf-8")
    stress_df.to_csv(OUT_STRESS, index=False, encoding="utf-8")

    target_rank = int(
        neighbor_df.reset_index()
        .query("reg_window == @TARGET_N and z_window == @TARGET_M")["index"]
        .iloc[0]
        + 1
    )

    summary = {
        "index_code": base.INDEX_CODE,
        "index_name": base.INDEX_NAME,
        "target_params": {"N": TARGET_N, "M": TARGET_M, "signal_col": SIGNAL_COL, "rule": "signal > 0 => long"},
        "base_summary": base_summary,
        "neighbor_rank_by_sharpe": target_rank,
        "neighbor_best": neighbor_df.iloc[0].to_dict(),
        "segment_rows": segment_df.to_dict(orient="records"),
        "stress_rows": stress_df.to_dict(orient="records"),
    }

    with OUT_SUMMARY.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("Base summary")
    print(json.dumps(base_summary, ensure_ascii=False, indent=2))
    print()
    print("Segments")
    print(segment_df.to_string(index=False))
    print()
    print("Neighbor rank by Sharpe:", target_rank)
    print(neighbor_df.head(12).to_string(index=False))
    print()
    print("Stress")
    print(stress_df.to_string(index=False))
    print()
    print(f"saved summary: {OUT_SUMMARY}")
    print(f"saved segments: {OUT_SEGMENTS}")
    print(f"saved neighbors: {OUT_NEIGHBOR}")
    print(f"saved stress: {OUT_STRESS}")
    print(f"saved chart: {OUT_CHART}")


if __name__ == "__main__":
    main()
