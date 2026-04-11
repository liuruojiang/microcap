import itertools
import json
from pathlib import Path

import numpy as np
import pandas as pd

from suba_cross_asset_momentum import (
    CASH_CODE,
    DEFAULT_PARAMS,
    RISKY_CODES,
    build_close_matrix,
    calc_summary,
    calc_bias_momentum,
    calc_rolling_r2,
)


WORKDIR = Path(".")


def load_close_matrix():
    for name in ("suba_cross_asset_close_fixed.csv", "suba_cross_asset_close.csv"):
        path = WORKDIR / name
        if path.exists():
            df = pd.read_csv(path, parse_dates=["date"]).set_index("date").sort_index()
            return df
    close_df, _ = build_close_matrix()
    return close_df


def score_summary(summary):
    annual = summary.get("annual_return") or -1.0
    sharpe = summary.get("sharpe") or -99.0
    calmar = summary.get("calmar") or -99.0
    max_dd = abs(summary.get("max_drawdown") or -1.0)
    return (
        sharpe * 1000
        + calmar * 100
        + annual * 10
        - max_dd * 5
    )


def build_signal_caches(close_df, bias_pairs, r2_windows):
    bias_cache = {}
    r2_cache = {}
    for bias_n, mom_day in bias_pairs:
        bias_cache[(bias_n, mom_day)] = {
            code: calc_bias_momentum(close_df[code], bias_n, mom_day)
            for code in RISKY_CODES
        }
    for window in r2_windows:
        r2_cache[window] = {
            code: calc_rolling_r2(close_df[code], window)
            for code in RISKY_CODES
        }
    return bias_cache, r2_cache


def run_with_caches(close_df, params, bias_cache, r2_cache):
    cfg = DEFAULT_PARAMS.copy()
    cfg.update(params)
    bias = bias_cache[(cfg["bias_n"], cfg["mom_day"])]
    r2 = r2_cache[cfg["r2_window"]]
    start_idx = cfg["bias_n"] + cfg["mom_day"]
    holding = CASH_CODE
    rows = []
    for i in range(start_idx, len(close_df)):
        date = close_df.index[i]
        scores = {}
        for code in RISKY_CODES:
            value = bias[code].iloc[i]
            if not np.isnan(value):
                scores[code] = value
        ideal = CASH_CODE
        if scores:
            best = max(scores, key=scores.get)
            if scores[best] > 0:
                r2_value = r2[best].iloc[i]
                if not np.isnan(r2_value) and r2_value >= cfg["r2_threshold"]:
                    ideal = best
        target = ideal if ideal != holding else None
        held_asset = holding
        held_ret = close_df.iloc[i][held_asset] / close_df.iloc[i - 1][held_asset] - 1
        if target is not None:
            legs = 1 if held_asset == CASH_CODE or target == CASH_CODE else 2
            day_ret = (1 + held_ret) * ((1 - cfg["commission"]) ** legs) - 1
            holding = target
        else:
            day_ret = held_ret
        rows.append(
            {
                "date": date,
                "return_raw": day_ret,
                "holding": holding,
                "target": target,
                "is_signal": target is not None,
            }
        )
    result = pd.DataFrame(rows).set_index("date")
    realized_vol = result["return_raw"].rolling(cfg["vol_window"]).std() * np.sqrt(cfg["trading_days"])
    scale_raw = (cfg["target_vol"] / realized_vol).clip(cfg["min_lev"], cfg["max_lev"]).shift(1)
    if cfg["scale_threshold"] > 0:
        scale_arr = scale_raw.to_numpy(copy=True)
        last_scale = np.nan
        for i in range(len(scale_arr)):
            if np.isnan(scale_arr[i]):
                continue
            if np.isnan(last_scale):
                last_scale = scale_arr[i]
            elif abs(scale_arr[i] - last_scale) >= cfg["scale_threshold"] - 1e-9:
                last_scale = scale_arr[i]
            else:
                scale_arr[i] = last_scale
        scale_raw = pd.Series(scale_arr, index=result.index)
    weights = scale_raw.fillna(1.0)
    weights[result["holding"] == CASH_CODE] = 1.0
    prev_weights = weights.shift(1).fillna(weights.iloc[0])
    scale_turnover = (weights - prev_weights).abs()
    scale_tc = np.where(
        (~result["is_signal"]) & (result["holding"] != CASH_CODE),
        cfg["commission"] * scale_turnover,
        0.0,
    )
    result["realized_vol"] = realized_vol
    result["scale_raw"] = scale_raw
    result["weight"] = weights
    result["scale_tc"] = scale_tc
    result["return"] = (1 + result["return_raw"] * weights) * (1 - scale_tc) - 1
    result["nav"] = (1 + result["return"]).cumprod()
    return result


def run_grid(close_df, param_grid, base_params, bias_cache, r2_cache):
    records = []
    keys = list(param_grid.keys())
    values = [param_grid[k] for k in keys]
    for combo in itertools.product(*values):
        params = base_params.copy()
        params.update(dict(zip(keys, combo)))
        result = run_with_caches(close_df, params, bias_cache, r2_cache)
        summary = calc_summary(result, params)
        summary["score"] = score_summary(summary)
        summary["params"] = params.copy()
        records.append(summary)
    return pd.DataFrame(records)


def main():
    close_df = load_close_matrix()

    coarse_grid = {
        "bias_n": [40, 60, 80, 100],
        "mom_day": [10, 20, 30, 40],
        "r2_window": [10, 20, 30, 40],
        "r2_threshold": [0.0, 0.1, 0.2, 0.3, 0.4, 0.5],
        "target_vol": [0.15, 0.20, 0.25, 0.30],
    }
    bias_pairs = set(
        itertools.product(coarse_grid["bias_n"], coarse_grid["mom_day"])
    )
    r2_windows = set(coarse_grid["r2_window"])
    bias_cache, r2_cache = build_signal_caches(close_df, bias_pairs, r2_windows)

    coarse_results = run_grid(close_df, coarse_grid, DEFAULT_PARAMS, bias_cache, r2_cache)
    coarse_results = coarse_results.sort_values(
        ["score", "sharpe", "calmar", "annual_return"], ascending=False
    ).reset_index(drop=True)
    best_coarse = coarse_results.iloc[0]["params"]

    fine_grid = {
        "vol_window": [20, 40, 60, 80],
        "scale_threshold": [0.0, 0.05, 0.10],
        "max_lev": [1.0, 1.25, 1.5],
    }
    fine_results = run_grid(close_df, fine_grid, best_coarse, bias_cache, r2_cache)
    fine_results = fine_results.sort_values(
        ["score", "sharpe", "calmar", "annual_return"], ascending=False
    ).reset_index(drop=True)
    best_params = fine_results.iloc[0]["params"]

    best_result = run_with_caches(close_df, best_params, bias_cache, r2_cache)
    best_summary = calc_summary(best_result, best_params)

    coarse_out = coarse_results.copy()
    coarse_out["params"] = coarse_out["params"].apply(json.dumps, ensure_ascii=False)
    coarse_out.to_csv(WORKDIR / "x_strategy_optimization_coarse.csv", index=False, encoding="utf-8-sig")

    fine_out = fine_results.copy()
    fine_out["params"] = fine_out["params"].apply(json.dumps, ensure_ascii=False)
    fine_out.to_csv(WORKDIR / "x_strategy_optimization_fine.csv", index=False, encoding="utf-8-sig")

    best_result.to_csv(WORKDIR / "x_strategy_optimized_nav.csv", encoding="utf-8-sig")
    with open(WORKDIR / "x_strategy_optimized_summary.json", "w", encoding="utf-8") as fh:
        json.dump(
            {
                "best_params": best_params,
                "summary": best_summary,
            },
            fh,
            ensure_ascii=False,
            indent=2,
        )

    top = fine_results.head(10).copy()
    top["params"] = top["params"].apply(json.dumps, ensure_ascii=False)
    print(top[["score", "annual_return", "sharpe", "calmar", "max_drawdown", "params"]].to_string(index=False))
    print("\nBEST")
    print(json.dumps({"best_params": best_params, "summary": best_summary}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
