import itertools
import json
from pathlib import Path

import pandas as pd

from optimize_x_strategy import (
    build_signal_caches,
    load_close_matrix,
    run_with_caches,
    score_summary,
)
from suba_cross_asset_momentum import CASH_CODE, DEFAULT_PARAMS, RISKY_CODES, calc_summary


WORKDIR = Path(".")

CURRENT_OPT_PARAMS = {
    "commission": 0.001,
    "trading_days": 252,
    "bias_n": 60,
    "mom_day": 40,
    "r2_window": 20,
    "r2_threshold": 0.2,
    "target_vol": 0.3,
    "vol_window": 40,
    "max_lev": 1.5,
    "min_lev": 0.1,
    "scale_threshold": 0.0,
}

BASELINE_PARAMS = {
    "commission": 0.001,
    "trading_days": 252,
    "bias_n": 60,
    "mom_day": 20,
    "r2_window": 20,
    "r2_threshold": 0.3,
    "target_vol": 0.2,
    "vol_window": 60,
    "max_lev": 1.5,
    "min_lev": 0.1,
    "scale_threshold": 0.1,
}


def summarize_period(result, start=None, end=None, params=None):
    sliced = result
    if start is not None:
        sliced = sliced[sliced.index >= pd.Timestamp(start)]
    if end is not None:
        sliced = sliced[sliced.index <= pd.Timestamp(end)]
    if sliced.empty:
        return {
            "start": start,
            "end": end,
            "annual_return": None,
            "annual_vol": None,
            "sharpe": None,
            "max_drawdown": None,
            "calmar": None,
            "monthly_win_rate": None,
            "months": 0,
        }
    sliced = sliced.copy()
    sliced["nav"] = (1 + sliced["return"]).cumprod()
    summary = calc_summary(sliced, params)
    return summary


def main():
    close_df = load_close_matrix()
    cutoff_idx = int(len(close_df) * 0.70)
    cutoff_date = close_df.index[cutoff_idx]

    grid = {
        "bias_n": [40, 60, 80],
        "mom_day": [20, 40, 60],
        "r2_window": [10, 20, 30],
        "r2_threshold": [0.1, 0.2, 0.3],
        "target_vol": [0.2, 0.3],
        "vol_window": [20, 40],
        "max_lev": [1.25, 1.5],
        "scale_threshold": [0.0, 0.1],
    }

    bias_pairs = set(itertools.product(grid["bias_n"], grid["mom_day"]))
    r2_windows = set(grid["r2_window"])
    bias_cache, r2_cache = build_signal_caches(close_df, bias_pairs, r2_windows)

    keys = list(grid.keys())
    values = [grid[k] for k in keys]
    records = []
    for combo in itertools.product(*values):
        params = DEFAULT_PARAMS.copy()
        params.update(dict(zip(keys, combo)))
        result = run_with_caches(close_df, params, bias_cache, r2_cache)
        train_summary = summarize_period(result, end=cutoff_date, params=params)
        test_summary = summarize_period(result, start=cutoff_date + pd.Timedelta(days=1), params=params)
        train_score = score_summary(train_summary)
        test_score = score_summary(test_summary)
        records.append(
            {
                "params": params,
                "train_score": train_score,
                "train_sharpe": train_summary.get("sharpe"),
                "train_calmar": train_summary.get("calmar"),
                "train_annual_return": train_summary.get("annual_return"),
                "train_max_drawdown": train_summary.get("max_drawdown"),
                "test_score": test_score,
                "test_sharpe": test_summary.get("sharpe"),
                "test_calmar": test_summary.get("calmar"),
                "test_annual_return": test_summary.get("annual_return"),
                "test_max_drawdown": test_summary.get("max_drawdown"),
            }
        )

    df = pd.DataFrame(records)
    df["train_rank"] = df["train_score"].rank(method="min", ascending=False)
    df["test_rank"] = df["test_score"].rank(method="min", ascending=False)
    df = df.sort_values(["train_score", "test_score"], ascending=False).reset_index(drop=True)

    selected = df.iloc[0].copy()

    current_result = run_with_caches(close_df, CURRENT_OPT_PARAMS, bias_cache, r2_cache)
    baseline_result = run_with_caches(close_df, BASELINE_PARAMS, bias_cache, r2_cache)

    current_train = summarize_period(current_result, end=cutoff_date, params=CURRENT_OPT_PARAMS)
    current_test = summarize_period(current_result, start=cutoff_date + pd.Timedelta(days=1), params=CURRENT_OPT_PARAMS)
    baseline_train = summarize_period(baseline_result, end=cutoff_date, params=BASELINE_PARAMS)
    baseline_test = summarize_period(baseline_result, start=cutoff_date + pd.Timedelta(days=1), params=BASELINE_PARAMS)

    rolling_windows = [
        ("2010-10-18", "2014-12-31"),
        ("2015-01-01", "2018-12-31"),
        ("2019-01-01", "2022-12-31"),
        ("2023-01-01", "2026-03-27"),
    ]
    current_rolling = []
    for start, end in rolling_windows:
        s = summarize_period(current_result, start=start, end=end, params=CURRENT_OPT_PARAMS)
        s["start"] = start
        s["end"] = end
        current_rolling.append(s)

    train_test_corr = df[["train_score", "test_score"]].corr().iloc[0, 1]
    top_decile = max(1, int(len(df) * 0.10))
    top_train = df.nsmallest(0, "train_rank")
    top_train = df[df["train_rank"] <= top_decile]

    result_payload = {
        "split": {
            "cutoff_date": str(cutoff_date.date()),
            "train_start": str(close_df.index[0].date()),
            "train_end": str(cutoff_date.date()),
            "test_start": str((cutoff_date + pd.Timedelta(days=1)).date()),
            "test_end": str(close_df.index[-1].date()),
        },
        "selected_from_train": {
            "params": selected["params"],
            "train_rank": int(selected["train_rank"]),
            "test_rank": int(selected["test_rank"]),
            "train_score": float(selected["train_score"]),
            "test_score": float(selected["test_score"]),
            "train_sharpe": float(selected["train_sharpe"]),
            "test_sharpe": float(selected["test_sharpe"]),
            "train_calmar": float(selected["train_calmar"]),
            "test_calmar": float(selected["test_calmar"]),
            "train_annual_return": float(selected["train_annual_return"]),
            "test_annual_return": float(selected["test_annual_return"]),
        },
        "current_opt_params": {
            "params": CURRENT_OPT_PARAMS,
            "train": current_train,
            "test": current_test,
        },
        "baseline_params": {
            "params": BASELINE_PARAMS,
            "train": baseline_train,
            "test": baseline_test,
        },
        "stability": {
            "combo_count": int(len(df)),
            "train_test_score_corr": float(train_test_corr),
            "top_train_decile_avg_test_sharpe": float(top_train["test_sharpe"].mean()),
            "top_train_decile_median_test_sharpe": float(top_train["test_sharpe"].median()),
            "overall_median_test_sharpe": float(df["test_sharpe"].median()),
        },
        "rolling_current_opt": current_rolling,
    }

    out_df = df.copy()
    out_df["params"] = out_df["params"].apply(json.dumps, ensure_ascii=False)
    out_df.to_csv(WORKDIR / "x_strategy_overfit_grid.csv", index=False, encoding="utf-8-sig")
    with open(WORKDIR / "x_strategy_overfit_report.json", "w", encoding="utf-8") as fh:
        json.dump(result_payload, fh, ensure_ascii=False, indent=2)

    print(json.dumps(result_payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
