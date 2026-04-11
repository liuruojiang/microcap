import itertools
import json
from pathlib import Path

import pandas as pd

from x_strategy_subb_top1 import DEFAULT_PARAMS, calc_summary, load_close_matrix, run_strategy


WORKDIR = Path(".")


def score_summary(summary):
    annual = summary.get("annual_return") or -1.0
    sharpe = summary.get("sharpe") or -99.0
    calmar = summary.get("calmar") or -99.0
    max_dd = abs(summary.get("max_drawdown") or -1.0)
    return sharpe * 1000 + calmar * 100 + annual * 10 - max_dd * 5


def run_grid(close_df, grid, base_params):
    records = []
    keys = list(grid.keys())
    values = [grid[k] for k in keys]
    for combo in itertools.product(*values):
        params = base_params.copy()
        params.update(dict(zip(keys, combo)))
        result = run_strategy(close_df, params)
        summary = calc_summary(result, params)
        summary["score"] = score_summary(summary)
        summary["params"] = params.copy()
        records.append(summary)
    return pd.DataFrame(records)


def main():
    close_df = load_close_matrix()

    coarse_grid = {
        "lb": [120, 160, 200],
        "signal_mode": ["weekly", "daily"],
        "target_vol": [0.15, 0.20, 0.25],
        "vol_window": [20, 40],
        "min_turnover": [0.0, 0.10],
        "abs_threshold": [-0.05, 0.0, 0.05],
    }
    coarse_results = run_grid(close_df, coarse_grid, DEFAULT_PARAMS)
    coarse_results = coarse_results.sort_values(
        ["score", "sharpe", "calmar", "annual_return"], ascending=False
    ).reset_index(drop=True)
    best_coarse = coarse_results.iloc[0]["params"]

    fine_grid = {
        "max_lev": [1.0, 1.25, 1.5],
        "vol_window": [20, 30, 40],
        "target_vol": [0.15, 0.20, 0.25, 0.30],
        "min_turnover": [0.0, 0.05, 0.10],
    }
    fine_results = run_grid(close_df, fine_grid, best_coarse)
    fine_results = fine_results.sort_values(
        ["score", "sharpe", "calmar", "annual_return"], ascending=False
    ).reset_index(drop=True)
    best_params = fine_results.iloc[0]["params"]

    best_result = run_strategy(close_df, best_params)
    best_summary = calc_summary(best_result, best_params)

    coarse_out = coarse_results.copy()
    coarse_out["params"] = coarse_out["params"].apply(json.dumps, ensure_ascii=False)
    coarse_out.to_csv(WORKDIR / "x_strategy_subb_top1_optimization_coarse.csv", index=False, encoding="utf-8-sig")

    fine_out = fine_results.copy()
    fine_out["params"] = fine_out["params"].apply(json.dumps, ensure_ascii=False)
    fine_out.to_csv(WORKDIR / "x_strategy_subb_top1_optimization_fine.csv", index=False, encoding="utf-8-sig")

    best_result.to_csv(WORKDIR / "x_strategy_subb_top1_optimized_nav.csv", encoding="utf-8-sig")
    with open(WORKDIR / "x_strategy_subb_top1_optimized_summary.json", "w", encoding="utf-8") as fh:
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
