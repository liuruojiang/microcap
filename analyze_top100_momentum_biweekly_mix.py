from __future__ import annotations

import itertools
import json
from pathlib import Path

import numpy as np
import pandas as pd

import analyze_microcap_zz1000_hedge as hedge_mod
import microcap_top100_mom16_biweekly_live as live_mod
import scan_top100_momentum_costs as cost_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "docs" / "top100_momentum_biweekly_mix_20260424"
PANEL_PATH = ROOT / "mnt_strategy_data_cn.csv"
INDEX_CSV = ROOT / "outputs" / "wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
TURNOVER_CSV = ROOT / "outputs" / "microcap_top100_mom16_biweekly_live_proxy_turnover.csv"
BASE_LOOKBACK = 16
SCAN_LBS = tuple(range(12, 27))
FOCUS_PAIRS = (
    (14, 16),
    (15, 16),
    (16, 17),
    (14, 17),
    (16, 24),
    (16, 25),
    (16, 26),
    (14, 24),
)


def build_close_df() -> pd.DataFrame:
    return live_mod.load_close_df(PANEL_PATH, INDEX_CSV)


def run_single_gross(close_df: pd.DataFrame, lookback: int) -> pd.DataFrame:
    return hedge_mod.run_backtest(
        close_df=close_df,
        signal_model="momentum",
        lookback=lookback,
        bias_n=hedge_mod.DEFAULT_BIAS_N,
        bias_mom_day=hedge_mod.DEFAULT_BIAS_MOM_DAY,
        futures_drag=live_mod.FUTURES_DRAG * live_mod.FIXED_HEDGE_RATIO,
        require_positive_microcap_mom=live_mod.REQUIRE_POSITIVE_MICROCAP_MOM,
        r2_window=hedge_mod.DEFAULT_R2_WINDOW,
        r2_threshold=0.0,
        vol_scale_enabled=False,
        target_vol=hedge_mod.DEFAULT_TARGET_VOL,
        vol_window=hedge_mod.DEFAULT_VOL_WINDOW,
        max_lev=hedge_mod.DEFAULT_MAX_LEV,
        min_lev=hedge_mod.DEFAULT_MIN_LEV,
        scale_threshold=hedge_mod.DEFAULT_SCALE_THRESHOLD,
        hedge_ratio=live_mod.FIXED_HEDGE_RATIO,
    )


def apply_fractional_cost_model(spread_ret: pd.Series, current_w: pd.Series, next_w: pd.Series, turnover: pd.DataFrame) -> pd.DataFrame:
    idx = spread_ret.index
    rebalance_base = cost_mod.map_rebalance_apply_costs(idx, turnover)

    entry = (next_w - current_w).clip(lower=0.0) * cost_mod.ENTRY_COST
    exit_ = (current_w - next_w).clip(lower=0.0) * cost_mod.EXIT_COST
    rebalance = np.minimum(current_w, next_w) * rebalance_base

    out = pd.DataFrame(index=idx)
    out["weight_prev"] = current_w
    out["weight_next"] = next_w
    out["spread_ret"] = spread_ret
    out["return"] = spread_ret * current_w
    out["entry_exit_cost"] = entry + exit_
    out["rebalance_cost"] = rebalance
    out["total_cost"] = out["entry_exit_cost"] + out["rebalance_cost"]
    out["return_net"] = (1.0 + out["return"]) * (1.0 - out["total_cost"]) - 1.0
    out["nav_net"] = (1.0 + out["return_net"]).cumprod()
    return out


def build_mix_result(gross_map: dict[int, pd.DataFrame], turnover: pd.DataFrame, lbs: tuple[int, int]) -> pd.DataFrame:
    base = gross_map[lbs[0]]
    other = gross_map[lbs[1]]
    common = base.index.intersection(other.index)
    base = base.loc[common].copy()
    other = other.loc[common].copy()

    current_w = (
        base["holding"].ne("cash").astype(float)
        + other["holding"].ne("cash").astype(float)
    ) / 2.0
    next_w = (
        base["next_holding"].ne("cash").astype(float)
        + other["next_holding"].ne("cash").astype(float)
    ) / 2.0
    spread_ret = base["microcap_ret"].fillna(0.0) - base["hedge_ret"].fillna(0.0) - base["futures_drag"].fillna(0.0)

    out = apply_fractional_cost_model(spread_ret=spread_ret, current_w=current_w, next_w=next_w, turnover=turnover)
    out["lb_a"] = lbs[0]
    out["lb_b"] = lbs[1]
    return out


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


def summarize_windows(result: pd.DataFrame) -> dict[str, float]:
    ret = result["return_net"].dropna()
    out: dict[str, float] = {}
    for label, years in (("last_3y", 3), ("last_5y", 5), ("full_common", None)):
        if years is None:
            part = ret
        else:
            start = ret.index.max() - pd.DateOffset(years=years)
            part = ret.loc[ret.index >= start]
        m = calc_metrics(part)
        for k, v in m.items():
            out[f"{label}_{k}"] = v
    out["entry_exit_cost_sum"] = float(result["entry_exit_cost"].sum())
    out["rebalance_cost_sum"] = float(result["rebalance_cost"].sum())
    out["total_cost_sum"] = float(result["total_cost"].sum())
    out["avg_prev_weight"] = float(result["weight_prev"].mean())
    out["avg_next_weight"] = float(result["weight_next"].mean())
    return out


def validate_single_16(gross_map: dict[int, pd.DataFrame], turnover: pd.DataFrame) -> dict[str, float | bool | int]:
    rebuilt = build_mix_result(gross_map, turnover, (BASE_LOOKBACK, BASE_LOOKBACK))
    live_single = cost_mod.apply_cost_model(gross_map[BASE_LOOKBACK], turnover)
    common = rebuilt.index.intersection(live_single.index)
    diff_nav = (rebuilt.loc[common, "nav_net"] - live_single.loc[common, "nav_net"]).abs()
    diff_ret = (rebuilt.loc[common, "return_net"] - live_single.loc[common, "return_net"]).abs()
    return {
        "common_rows": int(len(common)),
        "max_abs_nav_diff": float(diff_nav.max()) if len(common) else np.nan,
        "max_abs_ret_diff": float(diff_ret.max()) if len(common) else np.nan,
        "validation_pass": bool(len(common) and diff_nav.max() < 1e-12 and diff_ret.max() < 1e-12),
    }


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    close_df = build_close_df()
    turnover = cost_mod.load_turnover_table(TURNOVER_CSV)
    gross_map = {lb: run_single_gross(close_df, lb) for lb in SCAN_LBS}

    validation = validate_single_16(gross_map, turnover)
    (OUTPUT_DIR / "mix_validation.json").write_text(json.dumps(validation, ensure_ascii=False, indent=2), encoding="utf-8")

    rows: list[dict[str, object]] = []
    pair_results: dict[tuple[int, int], pd.DataFrame] = {}
    for a, b in itertools.combinations(SCAN_LBS, 2):
        result = build_mix_result(gross_map, turnover, (a, b))
        pair_results[(a, b)] = result
        row = {"lb_a": a, "lb_b": b}
        row.update(summarize_windows(result))
        rows.append(row)

    combo_df = pd.DataFrame(rows)
    combo_df = combo_df.sort_values(["last_5y_sharpe", "full_common_sharpe", "last_3y_sharpe"], ascending=False).reset_index(drop=True)
    combo_df.to_csv(OUTPUT_DIR / "mix_pair_scan.csv", index=False, encoding="utf-8")

    focus_rows = []
    baseline = build_mix_result(gross_map, turnover, (BASE_LOOKBACK, BASE_LOOKBACK))
    base_stats = summarize_windows(baseline)
    for a, b in FOCUS_PAIRS:
        key = tuple(sorted((a, b)))
        stats = summarize_windows(pair_results[key])
        focus_rows.append(
            {
                "pair": f"{key[0]}/{key[1]}",
                **stats,
                "last_3y_annual_delta_vs_16": stats["last_3y_annual"] - base_stats["last_3y_annual"],
                "last_3y_sharpe_delta_vs_16": stats["last_3y_sharpe"] - base_stats["last_3y_sharpe"],
                "last_3y_maxdd_delta_vs_16": stats["last_3y_max_dd"] - base_stats["last_3y_max_dd"],
                "last_5y_annual_delta_vs_16": stats["last_5y_annual"] - base_stats["last_5y_annual"],
                "last_5y_sharpe_delta_vs_16": stats["last_5y_sharpe"] - base_stats["last_5y_sharpe"],
                "last_5y_maxdd_delta_vs_16": stats["last_5y_max_dd"] - base_stats["last_5y_max_dd"],
                "full_annual_delta_vs_16": stats["full_common_annual"] - base_stats["full_common_annual"],
                "full_sharpe_delta_vs_16": stats["full_common_sharpe"] - base_stats["full_common_sharpe"],
                "full_maxdd_delta_vs_16": stats["full_common_max_dd"] - base_stats["full_common_max_dd"],
            }
        )
    focus_df = pd.DataFrame(focus_rows).sort_values(["last_5y_sharpe", "full_common_sharpe"], ascending=False)
    focus_df.to_csv(OUTPUT_DIR / "mix_focus_pairs.csv", index=False, encoding="utf-8")

    baseline_row = pd.DataFrame([{"pair": "16/16", **base_stats}])
    baseline_row.to_csv(OUTPUT_DIR / "mix_baseline_16.csv", index=False, encoding="utf-8")

    print(json.dumps(validation, ensure_ascii=False, indent=2))
    print("Top pair scan rows:")
    print(combo_df.head(12)[["lb_a", "lb_b", "last_3y_annual", "last_3y_sharpe", "last_3y_max_dd", "last_5y_annual", "last_5y_sharpe", "last_5y_max_dd", "full_common_annual", "full_common_sharpe", "full_common_max_dd"]].to_string(index=False))
    print("Focus pairs:")
    print(focus_df.to_string(index=False))


if __name__ == "__main__":
    main()
