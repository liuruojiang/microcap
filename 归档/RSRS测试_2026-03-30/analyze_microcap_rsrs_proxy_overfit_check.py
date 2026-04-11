from __future__ import annotations

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

import analyze_microcap_rsrs_proxy as base


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"

TARGET_N = 24
TARGET_M = 60
ALT_N = 20
ALT_M = 80
SIGNAL_COL = "right_skew"

OUT_SUMMARY = OUTPUT_DIR / "microcap_rsrs_proxy_overfit_check_summary.json"
OUT_SEGMENTS = OUTPUT_DIR / "microcap_rsrs_proxy_overfit_check_segments.csv"
OUT_NEIGHBOR = OUTPUT_DIR / "microcap_rsrs_proxy_overfit_check_neighbors.csv"
OUT_STRESS = OUTPUT_DIR / "microcap_rsrs_proxy_overfit_check_stress.csv"
OUT_CHART = OUTPUT_DIR / "microcap_rsrs_proxy_overfit_check.png"


def run_no_threshold(
    df: pd.DataFrame,
    signal_col: str = SIGNAL_COL,
    execution_delay_days: int = 1,
    switch_cost: float = 0.0,
) -> pd.DataFrame:
    out = df.copy()
    out["signal_position"] = (out[signal_col] > 0).astype(int)
    out["position"] = out["signal_position"].shift(execution_delay_days).fillna(0.0)
    out["asset_ret"] = out["close"].pct_change().fillna(0.0)
    out["turnover"] = out["position"].diff().abs().fillna(out["position"].abs())
    out["strategy_ret_gross"] = out["position"] * out["asset_ret"]
    out["strategy_ret"] = out["strategy_ret_gross"] - out["turnover"] * switch_cost
    out["asset_nav"] = (1.0 + out["asset_ret"]).cumprod()
    out["strategy_nav"] = (1.0 + out["strategy_ret"]).cumprod()
    out["strategy_peak"] = out["strategy_nav"].cummax()
    out["strategy_drawdown"] = out["strategy_nav"] / out["strategy_peak"] - 1.0
    return out


def summarize_run(df: pd.DataFrame, label: str, reg_window: int | None = None, z_window: int | None = None) -> dict:
    asset_nav = (1.0 + df["asset_ret"].fillna(0.0)).cumprod()
    strategy_nav = (1.0 + df["strategy_ret"].fillna(0.0)).cumprod()
    strategy_peak = strategy_nav.cummax()
    strategy_drawdown = strategy_nav / strategy_peak - 1.0
    return {
        "label": label,
        "reg_window": reg_window,
        "z_window": z_window,
        "sample_start": df.index[0].strftime("%Y-%m-%d"),
        "sample_end": df.index[-1].strftime("%Y-%m-%d"),
        "rows": int(len(df)),
        "total_return": float(strategy_nav.iloc[-1] - 1.0),
        "annual_return": float(base.annual_return(strategy_nav)),
        "sharpe": float(base.sharpe_ratio(df["strategy_ret"])),
        "max_drawdown": float(strategy_drawdown.min()),
        "position_days_pct": float(df["position"].mean()),
        "trades": int((df["position"].diff().abs().fillna(df["position"].abs()) > 0).sum()),
    }


def compute_result(raw: pd.DataFrame, reg_window: int, z_window: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    rsrs = base.compute_proxy_rsrs(raw, reg_window=reg_window, z_window=z_window)
    result = run_no_threshold(rsrs)
    return rsrs, result


def build_segment_table(result_map: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for name, df in result_map.items():
        thirds = pd.qcut(range(len(df)), q=3, labels=["segment_1", "segment_2", "segment_3"])
        temp = df.copy()
        temp["segment"] = list(thirds)
        for seg_name, sub in temp.groupby("segment", observed=False):
            rows.append(summarize_run(sub.drop(columns=["segment"]), f"{name}_{seg_name}"))

        for year, sub in df.groupby(df.index.year):
            if len(sub) < 30:
                continue
            rows.append(summarize_run(sub, f"{name}_year_{year}"))
    return pd.DataFrame(rows)


def build_neighbor_table(raw: pd.DataFrame) -> pd.DataFrame:
    rows = []
    n_grid = [16, 18, 20, 24, 30, 40]
    m_grid = [40, 60, 80, 100, 120, 160, 200, 250, 300, 500]
    for reg_window in n_grid:
        for z_window in m_grid:
            if z_window <= reg_window:
                continue
            _, result = compute_result(raw, reg_window, z_window)
            item = summarize_run(result, f"N{reg_window}_M{z_window}", reg_window, z_window)
            rows.append(item)
    return pd.DataFrame(rows).sort_values(["sharpe", "annual_return"], ascending=False).reset_index(drop=True)


def build_stress_table(rsrs_map: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    stress_cases = [
        ("base_delay1_cost0bp", 1, 0.0000),
        ("delay2_cost0bp", 2, 0.0000),
        ("delay1_cost10bp", 1, 0.0010),
        ("delay1_cost20bp", 1, 0.0020),
        ("delay2_cost10bp", 2, 0.0010),
    ]
    for param_name, rsrs_df in rsrs_map.items():
        for label, delay_days, switch_cost in stress_cases:
            result = run_no_threshold(rsrs_df, execution_delay_days=delay_days, switch_cost=switch_cost)
            item = summarize_run(result, f"{param_name}_{label}")
            item["param_name"] = param_name
            item["delay_days"] = delay_days
            item["switch_cost"] = switch_cost
            rows.append(item)
    return pd.DataFrame(rows)


def plot_diagnostics(target_result: pd.DataFrame, alt_result: pd.DataFrame, neighbor_df: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(12, 10))

    axes[0].plot(target_result.index, target_result["asset_nav"], color="gray", linewidth=1.2, label="buy and hold")
    axes[0].plot(
        target_result.index,
        target_result["strategy_nav"],
        color="#2ca02c",
        linewidth=1.5,
        label=f"RSRS N={TARGET_N}, M={TARGET_M}",
    )
    axes[0].plot(
        alt_result.index,
        alt_result["strategy_nav"],
        color="#1f77b4",
        linewidth=1.3,
        alpha=0.9,
        label=f"RSRS N={ALT_N}, M={ALT_M}",
    )
    axes[0].set_title("Wind Microcap Proxy RSRS Overfit Check")
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


def find_rank(neighbor_df: pd.DataFrame, reg_window: int, z_window: int) -> int:
    match = neighbor_df.reset_index().query("reg_window == @reg_window and z_window == @z_window")
    return int(match["index"].iloc[0] + 1)


def main() -> None:
    raw = base.load_microcap_ohlc(base.INPUT_CSV)

    target_rsrs, target_result = compute_result(raw, TARGET_N, TARGET_M)
    alt_rsrs, alt_result = compute_result(raw, ALT_N, ALT_M)

    result_map = {
        f"N{TARGET_N}_M{TARGET_M}": target_result,
        f"N{ALT_N}_M{ALT_M}": alt_result,
    }
    rsrs_map = {
        f"N{TARGET_N}_M{TARGET_M}": target_rsrs,
        f"N{ALT_N}_M{ALT_M}": alt_rsrs,
    }

    segment_df = build_segment_table(result_map)
    neighbor_df = build_neighbor_table(raw)
    stress_df = build_stress_table(rsrs_map)
    plot_diagnostics(target_result, alt_result, neighbor_df)

    segment_df.to_csv(OUT_SEGMENTS, index=False, encoding="utf-8")
    neighbor_df.to_csv(OUT_NEIGHBOR, index=False, encoding="utf-8")
    stress_df.to_csv(OUT_STRESS, index=False, encoding="utf-8")

    target_summary = summarize_run(target_result, "target", TARGET_N, TARGET_M)
    alt_summary = summarize_run(alt_result, "alt", ALT_N, ALT_M)

    summary = {
        "input_file": str(base.INPUT_CSV),
        "signal_col": SIGNAL_COL,
        "rule": "hold when signal > 0, else cash",
        "target_params": {"N": TARGET_N, "M": TARGET_M},
        "alt_params": {"N": ALT_N, "M": ALT_M},
        "target_summary": target_summary,
        "alt_summary": alt_summary,
        "target_neighbor_rank_by_sharpe": find_rank(neighbor_df, TARGET_N, TARGET_M),
        "alt_neighbor_rank_by_sharpe": find_rank(neighbor_df, ALT_N, ALT_M),
        "neighbor_best": neighbor_df.iloc[0].to_dict(),
        "segment_rows": segment_df.to_dict(orient="records"),
        "stress_rows": stress_df.to_dict(orient="records"),
    }

    with OUT_SUMMARY.open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("Target summary")
    print(json.dumps(target_summary, ensure_ascii=False, indent=2))
    print()
    print("Alt summary")
    print(json.dumps(alt_summary, ensure_ascii=False, indent=2))
    print()
    print("Neighbor top")
    print(neighbor_df.head(15).to_string(index=False))
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
