from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

import analyze_top100_rebalance_frequency as base_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
OUTPUT_PREFIX = "microcap_top100_rebalance_frequency_close_conservative"


def _load_nav(path: Path) -> pd.Series | None:
    if not path.exists():
        return None
    df = pd.read_csv(path, parse_dates=["date"]).sort_values("date").set_index("date")
    if "nav_net" not in df.columns:
        return None
    return df["nav_net"].astype(float)


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
        trade_constraint_mode=base_mod.TRADE_CONSTRAINT_MODE_CLOSE,
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

        index_path = OUTPUT_DIR / f"wind_microcap_top_100_{label}_16y_cached_close_conservative.csv"
        turnover_path = OUTPUT_DIR / f"microcap_top100_{label}_turnover_stats_close_conservative.csv"
        nav_path = OUTPUT_DIR / f"microcap_top100_mom16_hedge_zz1000_{label}_16y_costed_nav_close_conservative.csv"
        index_df.to_csv(index_path, index=False, encoding="utf-8")
        turnover_df.to_csv(turnover_path, index=False, encoding="utf-8")
        net.to_csv(nav_path, index_label="date", encoding="utf-8")

        summary = base_mod.summarize(label, net, turnover_df)
        summary_rows.append(
            {
                "schedule": label,
                "execution_timing": base_mod.EXECUTION_TIMING_CLOSE,
                "trade_constraint_mode": base_mod.TRADE_CONSTRAINT_MODE_CLOSE,
                **{k: v for k, v in summary.items() if k != "recent_windows"},
            }
        )
        for row in summary["recent_windows"]:
            recent_rows.append(
                {
                    "schedule": label,
                    "execution_timing": base_mod.EXECUTION_TIMING_CLOSE,
                    "trade_constraint_mode": base_mod.TRADE_CONSTRAINT_MODE_CLOSE,
                    "window_years": row["window_years"],
                    "annual": row["annual"],
                    "max_dd": row["max_dd"],
                    "sharpe": row["sharpe"],
                }
            )

        baseline_nav = _load_nav(OUTPUT_DIR / f"microcap_top100_mom16_hedge_zz1000_{label}_16y_costed_nav.csv")
        close_exec_nav = _load_nav(OUTPUT_DIR / f"microcap_top100_mom16_hedge_zz1000_{label}_16y_costed_nav_close_exec.csv")
        conservative_nav = net["nav_net"].astype(float)

        series_parts = [conservative_nav.rename("close_conservative_nav")]
        if baseline_nav is not None:
            series_parts.append(baseline_nav.rename("baseline_nav"))
        if close_exec_nav is not None:
            series_parts.append(close_exec_nav.rename("close_exec_nav"))
        compare_frame = pd.concat(series_parts, axis=1)
        compare_frame = compare_frame.dropna()
        if not compare_frame.empty:
            row: dict[str, object] = {
                "schedule": label,
                "end_date": str(compare_frame.index[-1].date()),
                "close_conservative_final_nav": float(compare_frame["close_conservative_nav"].iloc[-1]),
            }
            if "baseline_nav" in compare_frame.columns:
                row["baseline_final_nav"] = float(compare_frame["baseline_nav"].iloc[-1])
                row["vs_baseline_delta"] = float(
                    compare_frame["close_conservative_nav"].iloc[-1] - compare_frame["baseline_nav"].iloc[-1]
                )
                row["vs_baseline_delta_pct"] = float(
                    compare_frame["close_conservative_nav"].iloc[-1] / compare_frame["baseline_nav"].iloc[-1] - 1.0
                )
            if "close_exec_nav" in compare_frame.columns:
                row["close_exec_final_nav"] = float(compare_frame["close_exec_nav"].iloc[-1])
                row["vs_close_exec_delta"] = float(
                    compare_frame["close_conservative_nav"].iloc[-1] - compare_frame["close_exec_nav"].iloc[-1]
                )
                row["vs_close_exec_delta_pct"] = float(
                    compare_frame["close_conservative_nav"].iloc[-1] / compare_frame["close_exec_nav"].iloc[-1] - 1.0
                )
            compare_rows.append(row)

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
        "trade_constraint_mode": base_mod.TRADE_CONSTRAINT_MODE_CLOSE,
        "summary_csv": str(summary_path),
        "recent_windows_csv": str(recent_path),
        "compare_csv": str(compare_path),
        "schedules": summary_df.to_dict(orient="records"),
        "compare": compare_df.to_dict(orient="records"),
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
