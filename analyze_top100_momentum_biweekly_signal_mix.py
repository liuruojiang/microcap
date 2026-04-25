from __future__ import annotations

import itertools
import json
from pathlib import Path

import numpy as np
import pandas as pd

import analyze_top100_momentum_biweekly_mix as weight_mix_mod
import microcap_top100_mom16_biweekly_live as live_mod
import scan_top100_momentum_costs as cost_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "docs" / "top100_momentum_biweekly_signal_mix_20260425"
TURNOVER_CSV = ROOT / "outputs" / "microcap_top100_mom16_biweekly_live_proxy_turnover.csv"
BASE_LOOKBACK = 16
SCAN_LBS = tuple(range(4, 27))
FOCUS_PAIRS = (
    (4, 16),
    (6, 16),
    (8, 16),
    (10, 16),
    (12, 16),
    (13, 16),
    (14, 16),
    (15, 16),
    (16, 17),
    (16, 18),
    (16, 24),
    (16, 25),
    (16, 26),
)


def build_signal_mix_result(
    gross_map: dict[int, pd.DataFrame],
    turnover: pd.DataFrame,
    lbs: tuple[int, int],
) -> pd.DataFrame:
    frames = [gross_map[lb] for lb in lbs]
    common = frames[0].index
    for frame in frames[1:]:
        common = common.intersection(frame.index)

    aligned = [frame.loc[common].copy() for frame in frames]
    momentum_gap = pd.concat([frame["momentum_gap"] for frame in aligned], axis=1).mean(axis=1)
    signal_on = momentum_gap.gt(0.0)

    current_w = signal_on.shift(1, fill_value=False).astype(float)
    next_w = signal_on.astype(float)
    spread_ret = (
        aligned[0]["microcap_ret"].fillna(0.0)
        - aligned[0]["hedge_ret"].fillna(0.0)
        - aligned[0]["futures_drag"].fillna(0.0)
    )

    out = weight_mix_mod.apply_fractional_cost_model(
        spread_ret=spread_ret,
        current_w=current_w,
        next_w=next_w,
        turnover=turnover,
    )
    out["signal_on"] = signal_on
    out["holding"] = np.where(current_w.gt(0.0), "long_microcap_short_zz1000", "cash")
    out["next_holding"] = np.where(signal_on, "long_microcap_short_zz1000", "cash")
    out["avg_momentum_gap"] = momentum_gap
    for lb, frame in zip(lbs, aligned):
        out[f"momentum_gap_{lb}"] = frame["momentum_gap"]
    out["lb_a"] = lbs[0]
    out["lb_b"] = lbs[1]
    return out


def validate_signal_mix_16(gross_map: dict[int, pd.DataFrame], turnover: pd.DataFrame) -> dict[str, object]:
    rebuilt = build_signal_mix_result(gross_map, turnover, (BASE_LOOKBACK, BASE_LOOKBACK))
    live_single = cost_mod.apply_cost_model(gross_map[BASE_LOOKBACK], turnover)
    common = rebuilt.index.intersection(live_single.index)
    diff_nav = (rebuilt.loc[common, "nav_net"] - live_single.loc[common, "nav_net"]).abs()
    diff_ret = (rebuilt.loc[common, "return_net"] - live_single.loc[common, "return_net"]).abs()
    return {
        "common_rows": int(len(common)),
        "max_abs_nav_diff": float(diff_nav.max()) if len(common) else None,
        "max_abs_ret_diff": float(diff_ret.max()) if len(common) else None,
        "validation_pass": bool(len(common) and diff_nav.max() < 1e-12 and diff_ret.max() < 1e-12),
    }


def add_delta_columns(row: dict[str, object], stats: dict[str, float], base_stats: dict[str, float]) -> None:
    for window_name in ("last_3y", "last_5y", "full_common"):
        row[f"{window_name}_annual_delta_vs_16"] = stats[f"{window_name}_annual"] - base_stats[f"{window_name}_annual"]
        row[f"{window_name}_sharpe_delta_vs_16"] = stats[f"{window_name}_sharpe"] - base_stats[f"{window_name}_sharpe"]
        row[f"{window_name}_maxdd_delta_vs_16"] = stats[f"{window_name}_max_dd"] - base_stats[f"{window_name}_max_dd"]


def write_summary(
    validation: dict[str, object],
    baseline_stats: dict[str, float],
    pair_df: pd.DataFrame,
    focus_df: pd.DataFrame,
    with_16_df: pd.DataFrame,
) -> None:
    lines = [
        "# Top100 Momentum Biweekly Signal-Mix Study",
        "",
        "- Data source: live rebuild path from `microcap_top100_mom16_biweekly_live.py` via `load_close_df()`",
        "- Cost path: `scan_top100_momentum_costs.apply_cost_model()` parity for `16/16`, then the same entry/exit/rebalance cost formula for mixed signals",
        "- Baseline: `lookback=16` live semantics",
        "- User-logic mix: average `momentum_gap = microcap_mom - hedge_mom` first, then select one binary long/cash signal",
        "- Comparison note: older `top100_momentum_biweekly_mix_20260424` used weight-average semantics.",
        f"- Validation: `16/16` signal mix vs live costed path max_abs_nav_diff = `{validation.get('max_abs_nav_diff')}`; max_abs_ret_diff = `{validation.get('max_abs_ret_diff')}`",
        "",
        "## Baseline 16",
        f"- last_3y: CAGR {baseline_stats['last_3y_annual']:.2%}, Sharpe {baseline_stats['last_3y_sharpe']:.3f}, MaxDD {baseline_stats['last_3y_max_dd']:.2%}",
        f"- last_5y: CAGR {baseline_stats['last_5y_annual']:.2%}, Sharpe {baseline_stats['last_5y_sharpe']:.3f}, MaxDD {baseline_stats['last_5y_max_dd']:.2%}",
        f"- full_common: CAGR {baseline_stats['full_common_annual']:.2%}, Sharpe {baseline_stats['full_common_sharpe']:.3f}, MaxDD {baseline_stats['full_common_max_dd']:.2%}",
        "",
        "## Top Signal-Mix Pairs",
    ]
    top_cols = [
        "pair",
        "last_3y_annual",
        "last_3y_sharpe",
        "last_3y_max_dd",
        "last_5y_annual",
        "last_5y_sharpe",
        "last_5y_max_dd",
        "full_common_annual",
        "full_common_sharpe",
        "full_common_max_dd",
    ]
    for _, row in pair_df.head(12)[top_cols].iterrows():
        lines.append(
            f"- {row['pair']}: 3Y CAGR {row['last_3y_annual']:.2%}, Sharpe {row['last_3y_sharpe']:.3f}, MaxDD {row['last_3y_max_dd']:.2%}; "
            f"5Y CAGR {row['last_5y_annual']:.2%}, Sharpe {row['last_5y_sharpe']:.3f}, MaxDD {row['last_5y_max_dd']:.2%}; "
            f"Full CAGR {row['full_common_annual']:.2%}, Sharpe {row['full_common_sharpe']:.3f}, MaxDD {row['full_common_max_dd']:.2%}"
        )
    lines.extend(["", "## Focus Pairs"])
    for _, row in focus_df.iterrows():
        lines.append(
            f"- {row['pair']}: 3Y Sharpe {row['last_3y_sharpe']:.3f} ({row['last_3y_sharpe_delta_vs_16']:+.3f} vs 16); "
            f"5Y Sharpe {row['last_5y_sharpe']:.3f} ({row['last_5y_sharpe_delta_vs_16']:+.3f}); "
            f"Full Sharpe {row['full_common_sharpe']:.3f} ({row['full_common_sharpe_delta_vs_16']:+.3f})"
        )
    lines.extend(["", "## Pairs With 16"])
    for _, row in with_16_df.iterrows():
        lines.append(
            f"- {row['pair']}: 3Y CAGR {row['last_3y_annual']:.2%}, Sharpe {row['last_3y_sharpe']:.3f}, MaxDD {row['last_3y_max_dd']:.2%}; "
            f"5Y CAGR {row['last_5y_annual']:.2%}, Sharpe {row['last_5y_sharpe']:.3f}, MaxDD {row['last_5y_max_dd']:.2%}; "
            f"Full Sharpe {row['full_common_sharpe']:.3f}"
        )
    (OUTPUT_DIR / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    close_df = weight_mix_mod.build_close_df()
    turnover = cost_mod.load_turnover_table(TURNOVER_CSV)
    gross_map = {lb: weight_mix_mod.run_single_gross(close_df, lb) for lb in SCAN_LBS}

    validation = validate_signal_mix_16(gross_map, turnover)
    (OUTPUT_DIR / "signal_mix_validation.json").write_text(
        json.dumps(validation, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if not validation["validation_pass"]:
        raise RuntimeError(f"Signal-mix 16/16 validation failed: {validation}")

    baseline = build_signal_mix_result(gross_map, turnover, (BASE_LOOKBACK, BASE_LOOKBACK))
    baseline_stats = weight_mix_mod.summarize_windows(baseline)
    pd.DataFrame([{"pair": "16/16", **baseline_stats}]).to_csv(
        OUTPUT_DIR / "signal_mix_baseline_16.csv",
        index=False,
        encoding="utf-8",
    )

    rows: list[dict[str, object]] = []
    pair_results: dict[tuple[int, int], pd.DataFrame] = {}
    for a, b in itertools.combinations(SCAN_LBS, 2):
        key = (a, b)
        result = build_signal_mix_result(gross_map, turnover, key)
        pair_results[key] = result
        stats = weight_mix_mod.summarize_windows(result)
        row: dict[str, object] = {"pair": f"{a}/{b}", "lb_a": a, "lb_b": b, **stats}
        add_delta_columns(row, stats, baseline_stats)
        rows.append(row)

    pair_df = pd.DataFrame(rows).sort_values(
        ["last_5y_sharpe", "full_common_sharpe", "last_3y_sharpe"],
        ascending=False,
    ).reset_index(drop=True)
    pair_df.to_csv(OUTPUT_DIR / "signal_mix_pair_scan.csv", index=False, encoding="utf-8")
    with_16_df = pair_df[(pair_df["lb_a"] == BASE_LOOKBACK) | (pair_df["lb_b"] == BASE_LOOKBACK)].copy()
    with_16_df = with_16_df.sort_values(
        ["last_5y_sharpe", "full_common_sharpe", "last_3y_sharpe"],
        ascending=False,
    ).reset_index(drop=True)
    with_16_df.to_csv(OUTPUT_DIR / "signal_mix_with_16_pairs.csv", index=False, encoding="utf-8")

    focus_rows: list[dict[str, object]] = []
    for a, b in FOCUS_PAIRS:
        key = tuple(sorted((a, b)))
        result = pair_results[key]
        stats = weight_mix_mod.summarize_windows(result)
        row = {"pair": f"{key[0]}/{key[1]}", **stats}
        add_delta_columns(row, stats, baseline_stats)
        focus_rows.append(row)
    focus_df = pd.DataFrame(focus_rows).sort_values(
        ["last_5y_sharpe", "full_common_sharpe"],
        ascending=False,
    )
    focus_df.to_csv(OUTPUT_DIR / "signal_mix_focus_pairs.csv", index=False, encoding="utf-8")

    write_summary(validation, baseline_stats, pair_df, focus_df, with_16_df)

    print(json.dumps(validation, ensure_ascii=False, indent=2))
    print("Top signal-mix pair scan rows:")
    print(
        pair_df.head(12)[
            [
                "pair",
                "last_3y_annual",
                "last_3y_sharpe",
                "last_3y_max_dd",
                "last_5y_annual",
                "last_5y_sharpe",
                "last_5y_max_dd",
                "full_common_annual",
                "full_common_sharpe",
                "full_common_max_dd",
            ]
        ].to_string(index=False)
    )
    print("Focus pairs:")
    print(focus_df.to_string(index=False))


if __name__ == "__main__":
    main()
