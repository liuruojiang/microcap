from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

import analyze_microcap_zz1000_hedge as hedge_mod


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
PRACTICAL_TOP100_CSV = OUTPUT_DIR / "wind_microcap_top_100_monthly_16y_cached.csv"


def run_strategy(
    *,
    output_prefix: str,
    signal_model: str,
    lookback: int,
    bias_n: int,
    bias_mom_day: int,
) -> tuple[pd.DataFrame, dict[str, object]]:
    args = hedge_mod.parse_args()
    args.microcap_csv = PRACTICAL_TOP100_CSV
    args.microcap_column = hedge_mod.DEFAULT_MICROCAP_COLUMN
    args.output_prefix = output_prefix
    args.signal_model = signal_model
    args.lookback = lookback
    args.bias_n = bias_n
    args.bias_mom_day = bias_mom_day
    args.require_positive_microcap_mom = False

    output_paths = hedge_mod.build_output_paths(args.output_prefix)
    close_df = hedge_mod.build_close_df(args)
    result = hedge_mod.run_backtest(
        close_df=close_df,
        signal_model=args.signal_model,
        lookback=args.lookback,
        bias_n=args.bias_n,
        bias_mom_day=args.bias_mom_day,
        futures_drag=args.futures_drag,
        require_positive_microcap_mom=args.require_positive_microcap_mom,
        r2_window=args.r2_window,
        r2_threshold=args.r2_threshold,
        vol_scale_enabled=args.vol_scale_enabled,
        target_vol=args.target_vol,
        vol_window=args.vol_window,
        max_lev=args.max_lev,
        min_lev=args.min_lev,
        scale_threshold=args.scale_threshold,
    )
    latest_signal = hedge_mod.build_latest_signal(result)
    summary = hedge_mod.build_summary(result=result, args=args, close_df=close_df)
    summary["proxy_variant"] = "top100_monthly_practical_cached"
    summary["method_note"] = (
        "Uses outputs/wind_microcap_top_100_monthly_16y_cached.csv as the microcap proxy, "
        "which is rebuilt with suspension and limit-lock tradeability handling."
    )

    result.to_csv(output_paths["nav"], index_label="date", encoding="utf-8")
    latest_signal.to_csv(output_paths["signal"], index=False, encoding="utf-8")
    output_paths["summary"].write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    hedge_mod.plot_nav(result["nav"], output_paths["curve"])
    return result, summary


def metrics_row(summary: dict[str, object]) -> dict[str, object]:
    metrics = summary["metrics"]
    return {
        "strategy": summary["strategy"],
        "annual": metrics["annual"],
        "max_dd": metrics["max_dd"],
        "sharpe": metrics["sharpe"],
        "vol": metrics["vol"],
        "total_return": metrics["total_return"],
        "win_rate": metrics["win_rate"],
        "active_days_pct": summary["active_days_pct"],
        "trades_per_year": summary["signal_changes"] / ((summary["n_days"] - 1) / hedge_mod.CN_TRADING_DAYS) / 2.0,
        "peak_date": None,
        "trough_date": None,
        "recovery_date": None,
    }


def add_drawdown_dates(row: dict[str, object], nav: pd.Series) -> None:
    peak = nav.cummax()
    dd = nav / peak - 1.0
    trough_date = dd.idxmin()
    peak_date = nav.loc[:trough_date].idxmax()
    recovery_slice = nav.loc[trough_date:]
    recovery = recovery_slice[recovery_slice >= peak.loc[trough_date]]
    row["peak_date"] = str(pd.Timestamp(peak_date).date())
    row["trough_date"] = str(pd.Timestamp(trough_date).date())
    row["recovery_date"] = None if recovery.empty else str(pd.Timestamp(recovery.index[0]).date())


def plot_compare(series_map: dict[str, pd.Series], path: Path, years: int | None = None) -> None:
    plt.figure(figsize=(12, 6))
    for label, nav in series_map.items():
        part = nav
        if years is not None:
            start = nav.index.max() - pd.DateOffset(years=years)
            part = nav.loc[nav.index >= start]
            part = part / part.iloc[0]
        plt.plot(part.index, part.values, linewidth=1.8, label=label)
    title = "Top100 Practical Hedged Signal Compare"
    if years is not None:
        title += f" Recent {years}Y"
    plt.title(title)
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(path, dpi=160)
    plt.close()


def main() -> None:
    runs = [
        ("mom8_top100_practical_16y", "momentum", 8, hedge_mod.DEFAULT_BIAS_N, hedge_mod.DEFAULT_BIAS_MOM_DAY),
        ("mom5_top100_practical_16y", "momentum", 5, hedge_mod.DEFAULT_BIAS_N, hedge_mod.DEFAULT_BIAS_MOM_DAY),
        ("bias20_10_top100_practical_16y", "bias_momentum", 20, 20, 10),
    ]

    navs: dict[str, pd.Series] = {}
    rows: list[dict[str, object]] = []
    summaries: list[dict[str, object]] = []
    for output_prefix, signal_model, lookback, bias_n, bias_mom_day in runs:
        result, summary = run_strategy(
            output_prefix=output_prefix,
            signal_model=signal_model,
            lookback=lookback,
            bias_n=bias_n,
            bias_mom_day=bias_mom_day,
        )
        navs[output_prefix] = result["nav"]
        row = metrics_row(summary)
        add_drawdown_dates(row, result["nav"])
        rows.append(row)
        summaries.append(summary)

    compare_df = pd.DataFrame(rows)
    compare_path = OUTPUT_DIR / "microcap_top100_three_signal_compare.csv"
    compare_df.to_csv(compare_path, index=False, encoding="utf-8")

    plot_compare(navs, OUTPUT_DIR / "microcap_top100_three_signal_compare.png")
    plot_compare(navs, OUTPUT_DIR / "microcap_top100_three_signal_compare_recent6y.png", years=6)

    payload = {
        "strategy": "top100_practical_three_signal_compare",
        "as_of_date": str(max(nav.index.max() for nav in navs.values()).date()),
        "proxy_variant": "top100_monthly_practical_cached",
        "rows": compare_df.to_dict(orient="records"),
        "summaries": summaries,
    }
    (OUTPUT_DIR / "microcap_top100_three_signal_compare.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(compare_df.to_string(index=False))
    print("saved microcap_top100_three_signal_compare.csv")
    print("saved microcap_top100_three_signal_compare.png")
    print("saved microcap_top100_three_signal_compare_recent6y.png")
    print("saved microcap_top100_three_signal_compare.json")


if __name__ == "__main__":
    main()
