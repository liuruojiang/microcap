from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

import analyze_microcap_zz1000_hedge as hedge_mod
import microcap_top100_mom16_biweekly_live as live_mod
import microcap_top100_mom16_biweekly_live_v1_4 as v1_4_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "docs" / "top100_momentum_biweekly_buffer_20260425"
BASE_LOOKBACK = 16
BUFFER_LEVELS = (
    ("base_0p000", 0.000),
    ("buffer_0p001", 0.001),
    ("buffer_0p0015", 0.0015),
    ("buffer_0p002", 0.002),
    ("buffer_0p0025", 0.0025),
    ("buffer_0p003", 0.003),
    ("buffer_0p0035", 0.0035),
    ("buffer_0p004", 0.004),
    ("buffer_0p0045", 0.0045),
    ("buffer_0p005", 0.005),
    ("buffer_0p0075", 0.0075),
    ("buffer_0p010", 0.010),
    ("buffer_0p015", 0.015),
    ("buffer_0p020", 0.020),
    ("buffer_0p030", 0.030),
    ("buffer_0p040", 0.040),
    ("buffer_0p050", 0.050),
)
WINDOWS = (
    ("last_1y", 1),
    ("last_3y", 3),
    ("last_5y", 5),
    ("last_10y", 10),
    ("full_common", None),
)


def build_v1_4_with_buffer(base_gross: pd.DataFrame, turnover: pd.DataFrame, exit_buffer: float) -> pd.DataFrame:
    buffered = live_mod.apply_momentum_gap_exit_buffer(base_gross, exit_buffer)
    return live_mod.apply_momentum_gap_peak_decay_derisk(
        gross_result=buffered,
        turnover_df=turnover,
        decay_ratio_threshold=v1_4_mod.DECAY_RATIO_THRESHOLD,
        derisk_scale=v1_4_mod.DERISK_SCALE,
        recovery_ratio_threshold=v1_4_mod.RECOVERY_RATIO_THRESHOLD,
    )


def calc_metrics(ret: pd.Series) -> dict[str, float]:
    m = hedge_mod.calc_metrics(ret.dropna())
    return {
        "annual": float(m.annual),
        "vol": float(m.vol),
        "sharpe": float(m.sharpe),
        "max_dd": float(m.max_dd),
        "calmar": float(m.calmar),
        "total_return": float(m.total_return),
        "win_rate": float(m.win_rate),
    }


def slice_returns(ret: pd.Series, years: int | None) -> pd.Series:
    clean = ret.dropna()
    if years is None:
        return clean
    start = clean.index.max() - pd.DateOffset(years=years)
    return clean.loc[clean.index >= start]


def signal_stats(net: pd.DataFrame) -> dict[str, float | int]:
    active = net["holding"].ne("cash")
    active_prev = active.shift(1, fill_value=False)
    entries = int((active & ~active_prev).sum())
    exits = int((~active & active_prev).sum())
    changes = int(net["signal_on"].ne(net["signal_on"].shift()).sum() - 1)
    return {
        "active_days_pct": float(active.mean()),
        "signal_changes": changes,
        "entry_days": entries,
        "exit_days": exits,
        "entry_exit_cost_sum": float(net["entry_exit_cost"].sum()),
        "rebalance_cost_sum": float(net["rebalance_cost"].sum()),
        "total_cost_sum": float(net["total_cost"].sum()),
    }


def summarize(net_map: dict[str, pd.DataFrame], base_key: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for window_name, years in WINDOWS:
        base_ret = slice_returns(net_map[base_key]["return_net"], years)
        base_metrics = calc_metrics(base_ret)
        for name, net in net_map.items():
            ret = slice_returns(net["return_net"], years)
            metrics = calc_metrics(ret)
            row: dict[str, object] = {
                "window": window_name,
                "variant": name,
                **metrics,
                **signal_stats(net),
                "base_annual": base_metrics["annual"],
                "base_max_dd": base_metrics["max_dd"],
                "base_sharpe": base_metrics["sharpe"],
                "annual_delta": metrics["annual"] - base_metrics["annual"],
                "max_dd_delta": metrics["max_dd"] - base_metrics["max_dd"],
                "sharpe_delta": metrics["sharpe"] - base_metrics["sharpe"],
            }
            rows.append(row)
    return pd.DataFrame(rows)


def write_summary(summary_df: pd.DataFrame, validation: dict[str, object]) -> None:
    lines = [
        "# Top100 Momentum Biweekly Buffer Scan",
        "",
        "- Baseline script: `microcap_top100_mom16_biweekly_live_v1_4.py`",
        "- Baseline: v1.4 original rule, `base_version=v1.1`, 0.8x hedge, peak-decay derisk overlay",
        "- Buffer rule: entry remains `momentum_gap > 0`; when already long, exit only when `momentum_gap < -buffer`",
        "- Overlay path: buffer is applied before v1.4 peak-decay derisk, then v1.4 costed return is recomputed",
        f"- Validation: `buffer={validation.get('official_v1_4_buffer')}` vs v1.4 official output max_abs_nav_diff = `{validation.get('max_abs_nav_diff')}`; max_abs_ret_diff = `{validation.get('max_abs_ret_diff')}`",
        "",
    ]
    core = summary_df[summary_df["window"].isin(["last_1y", "last_3y", "last_5y", "last_10y", "full_common"])].copy()
    for window in ["last_1y", "last_3y", "last_5y", "last_10y", "full_common"]:
        lines.append(f"## {window}")
        sub = core[core["window"] == window].sort_values(["sharpe", "annual"], ascending=False)
        for _, row in sub.iterrows():
            lines.append(
                f"- {row['variant']}: CAGR {row['annual']:.2%} ({row['annual_delta']:+.2%}), "
                f"Sharpe {row['sharpe']:.3f} ({row['sharpe_delta']:+.3f}), "
                f"MaxDD {row['max_dd']:.2%} ({row['max_dd_delta']:+.2%}), "
                f"changes {int(row['signal_changes'])}"
            )
        lines.append("")
    (OUTPUT_DIR / "summary.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    _, _, base_gross, turnover = v1_4_mod._load_base_v1_1_context()
    _, _, official_v1_4 = v1_4_mod.generate_v1_4_outputs()

    net_map: dict[str, pd.DataFrame] = {}
    nav_dir = OUTPUT_DIR / "nav"
    nav_dir.mkdir(exist_ok=True)
    for name, buffer_value in BUFFER_LEVELS:
        net = build_v1_4_with_buffer(base_gross, turnover, buffer_value)
        net["buffer"] = buffer_value
        net.to_csv(nav_dir / f"{name}.csv", encoding="utf-8")
        net_map[name] = net

    common = official_v1_4.index.intersection(net_map["buffer_0p0025"].index)
    diff_nav = (official_v1_4.loc[common, "nav_net"] - net_map["buffer_0p0025"].loc[common, "nav_net"]).abs()
    diff_ret = (official_v1_4.loc[common, "return_net"] - net_map["buffer_0p0025"].loc[common, "return_net"]).abs()
    validation = {
        "common_rows": int(len(common)),
        "max_abs_nav_diff": float(diff_nav.max()) if len(common) else None,
        "max_abs_ret_diff": float(diff_ret.max()) if len(common) else None,
        "validation_pass": bool(len(common) and diff_nav.max() < 1e-12 and diff_ret.max() < 1e-12),
        "official_v1_4_buffer": v1_4_mod.V1_4_MOMENTUM_GAP_EXIT_BUFFER,
    }
    (OUTPUT_DIR / "validation.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2), encoding="utf-8")
    if not validation["validation_pass"]:
        raise RuntimeError(f"official v1.4 buffer validation failed: {validation}")

    summary_df = summarize(net_map, "base_0p000")
    summary_df.to_csv(OUTPUT_DIR / "summary.csv", index=False, encoding="utf-8")
    write_summary(summary_df, validation)

    print(json.dumps(validation, ensure_ascii=False, indent=2))
    for window in ["last_1y", "last_3y", "last_5y", "last_10y", "full_common"]:
        print(window)
        sub = summary_df[summary_df["window"] == window].sort_values(["sharpe", "annual"], ascending=False)
        print(sub[["variant", "annual", "annual_delta", "sharpe", "sharpe_delta", "max_dd", "max_dd_delta", "signal_changes"]].head(6).to_string(index=False))


if __name__ == "__main__":
    main()
