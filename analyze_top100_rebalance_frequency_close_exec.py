from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

import analyze_top100_rebalance_frequency as base_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
OUTPUT_PREFIX = "microcap_top100_rebalance_frequency_close_exec"


def main() -> None:
    trading_dates = base_mod.load_trading_dates()
    rebalance_map = base_mod.build_all_rebalance_dates(trading_dates)
    all_cap_dates = pd.DatetimeIndex(sorted(set().union(*[set(v) for v in rebalance_map.values()])))
    symbols = base_mod.load_universe()
    returns_df, caps_by_date, buyable_df, sellable_df = base_mod.load_cache_panels(
        symbols,
        trading_dates,
        all_cap_dates,
        max_workers=8,
    )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary_rows: list[dict[str, object]] = []
    recent_rows: list[dict[str, object]] = []
    compare_rows: list[dict[str, object]] = []

    for label, rebalance_dates in rebalance_map.items():
        index_df, turnover_df = base_mod.build_index_and_turnover(
            trading_dates,
            returns_df,
            caps_by_date,
            buyable_df,
            sellable_df,
            rebalance_dates,
            execution_timing=base_mod.EXECUTION_TIMING_CLOSE,
        )
        net = base_mod.run_strategy(index_df, turnover_df)

        index_path = OUTPUT_DIR / f"wind_microcap_top_100_{label}_16y_cached_close_exec.csv"
        turnover_path = OUTPUT_DIR / f"microcap_top100_{label}_turnover_stats_close_exec.csv"
        nav_path = OUTPUT_DIR / f"microcap_top100_mom16_hedge_zz1000_{label}_16y_costed_nav_close_exec.csv"
        index_df.to_csv(index_path, index=False, encoding="utf-8")
        turnover_df.to_csv(turnover_path, index=False, encoding="utf-8")
        net.to_csv(nav_path, index_label="date", encoding="utf-8")

        summary = base_mod.summarize(label, net, turnover_df)
        summary_rows.append(
            {
                "schedule": label,
                "execution_timing": base_mod.EXECUTION_TIMING_CLOSE,
                **{k: v for k, v in summary.items() if k != "recent_windows"},
            }
        )
        for row in summary["recent_windows"]:
            recent_rows.append(
                {
                    "schedule": label,
                    "execution_timing": base_mod.EXECUTION_TIMING_CLOSE,
                    "window_years": row["window_years"],
                    "annual": row["annual"],
                    "max_dd": row["max_dd"],
                    "sharpe": row["sharpe"],
                }
            )

        baseline_path = OUTPUT_DIR / f"microcap_top100_mom16_hedge_zz1000_{label}_16y_costed_nav.csv"
        if baseline_path.exists():
            baseline = pd.read_csv(baseline_path, parse_dates=["date"]).sort_values("date").set_index("date")
            aligned = pd.concat(
                [
                    baseline["nav_net"].rename("baseline_nav"),
                    net["nav_net"].rename("close_exec_nav"),
                ],
                axis=1,
            ).dropna()
            if not aligned.empty:
                compare_rows.append(
                    {
                        "schedule": label,
                        "baseline_end_date": str(aligned.index[-1].date()),
                        "baseline_final_nav": float(aligned["baseline_nav"].iloc[-1]),
                        "close_exec_final_nav": float(aligned["close_exec_nav"].iloc[-1]),
                        "final_nav_delta": float(aligned["close_exec_nav"].iloc[-1] - aligned["baseline_nav"].iloc[-1]),
                        "final_nav_delta_pct": float(
                            aligned["close_exec_nav"].iloc[-1] / aligned["baseline_nav"].iloc[-1] - 1.0
                        ),
                    }
                )

    summary_df = pd.DataFrame(summary_rows).sort_values("net_sharpe", ascending=False)
    recent_df = pd.DataFrame(recent_rows).sort_values(["window_years", "sharpe"], ascending=[True, False])
    compare_df = pd.DataFrame(compare_rows).sort_values("schedule")

    summary_path = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.csv"
    recent_path = OUTPUT_DIR / f"{OUTPUT_PREFIX}_recent_windows.csv"
    compare_path = OUTPUT_DIR / f"{OUTPUT_PREFIX}_vs_baseline.csv"
    summary_df.to_csv(summary_path, index=False, encoding="utf-8")
    recent_df.to_csv(recent_path, index=False, encoding="utf-8")
    compare_df.to_csv(compare_path, index=False, encoding="utf-8")

    payload = {
        "execution_timing": base_mod.EXECUTION_TIMING_CLOSE,
        "summary_csv": str(summary_path),
        "recent_windows_csv": str(recent_path),
        "baseline_compare_csv": str(compare_path),
        "schedules": summary_df.to_dict(orient="records"),
        "baseline_compare": compare_df.to_dict(orient="records"),
    }
    json_path = OUTPUT_DIR / f"{OUTPUT_PREFIX}_summary.json"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(summary_df.to_string(index=False))
    if not compare_df.empty:
        print(compare_df.to_string(index=False))
    print(f"saved {summary_path.name}")
    print(f"saved {recent_path.name}")
    print(f"saved {compare_path.name}")
    print(f"saved {json_path.name}")


if __name__ == "__main__":
    main()
