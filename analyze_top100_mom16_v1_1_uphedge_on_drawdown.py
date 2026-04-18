from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
INPUT_NAV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv"

BASE_RATIO = 0.8
STRESS_RATIO = 1.0
DRAWDOWN_TRIGGER = 0.04
FUTURES_DRAG_PER_UNIT = 0.0003
HEDGE_REBAL_COST_BPS = 2.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest v1.1 uphedge overlay: 0.8x baseline, switch to 1.0x after drawdown trigger.")
    parser.add_argument("--drawdown-trigger", type=float, default=DRAWDOWN_TRIGGER, help="Realized NAV drawdown trigger, e.g. 0.04 for 4%%.")
    parser.add_argument("--base-ratio", type=float, default=BASE_RATIO, help="Normal active hedge ratio.")
    parser.add_argument("--stress-ratio", type=float, default=STRESS_RATIO, help="Active hedge ratio once trigger is hit.")
    parser.add_argument("--hedge-rebal-cost-bps", type=float, default=HEDGE_REBAL_COST_BPS, help="Extra hedge adjustment cost in bps per 1.0x ratio change.")
    return parser.parse_args()


def threshold_tag(drawdown_trigger: float) -> str:
    pct = int(round(float(drawdown_trigger) * 100))
    return f"{pct}pct"


def build_output_paths(drawdown_trigger: float) -> dict[str, Path]:
    stem = f"microcap_top100_mom16_v1_1_uphedge_on_drawdown_{threshold_tag(drawdown_trigger)}"
    return {
        "stem": Path(stem),
        "nav_csv": OUTPUT_DIR / f"{stem}_nav.csv",
        "windows_csv": OUTPUT_DIR / f"{stem}_windows.csv",
        "summary_json": OUTPUT_DIR / f"{stem}_summary.json",
        "plot_png": OUTPUT_DIR / f"{stem}_recent10y_compare.png",
        "rebased_csv": OUTPUT_DIR / f"{stem}_recent10y_rebased_nav.csv",
    }


def load_base_nav() -> pd.DataFrame:
    df = pd.read_csv(INPUT_NAV, parse_dates=["date"]).sort_values("date").set_index("date")
    required = {"holding", "next_holding", "hedge_ret", "return", "total_cost", "return_net"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise KeyError(f"Missing columns in {INPUT_NAV.name}: {missing}")
    return df


def compute_target_active_ratio_path(
    returns: pd.Series,
    base_ratio: float,
    stress_ratio: float,
    drawdown_trigger: float,
) -> pd.Series:
    nav = 1.0
    peak = 1.0
    current_ratio = float(base_ratio)
    out: list[float] = []
    for ret in returns.fillna(0.0).astype(float):
        prev_dd = nav / peak - 1.0
        if prev_dd <= -float(drawdown_trigger):
            current_ratio = float(stress_ratio)
        elif prev_dd >= 0.0:
            current_ratio = float(base_ratio)
        out.append(current_ratio)
        nav *= 1.0 + float(ret)
        peak = max(peak, nav)
    return pd.Series(out, index=returns.index, dtype=float)


def simulate_uphedge_strategy(
    base_df: pd.DataFrame,
    base_ratio: float = BASE_RATIO,
    stress_ratio: float = STRESS_RATIO,
    drawdown_trigger: float = DRAWDOWN_TRIGGER,
    hedge_rebal_cost_bps: float = HEDGE_REBAL_COST_BPS,
) -> pd.DataFrame:
    nav = 1.0
    peak = 1.0
    current_target_ratio = float(base_ratio)
    prev_applied_ratio = 0.0
    rows: list[dict[str, float | str | pd.Timestamp]] = []

    for dt, row in base_df.iterrows():
        prev_dd = nav / peak - 1.0
        if prev_dd <= -float(drawdown_trigger):
            current_target_ratio = float(stress_ratio)
        elif prev_dd >= 0.0:
            current_target_ratio = float(base_ratio)

        active = str(row["holding"]) != "cash"
        base_applied_ratio = float(base_ratio) if active else 0.0
        applied_ratio = float(current_target_ratio) if active else 0.0
        hedge_delta = applied_ratio - base_applied_ratio

        gross_return = float(row["return"]) - hedge_delta * float(row["hedge_ret"]) - hedge_delta * FUTURES_DRAG_PER_UNIT
        hedge_turnover = abs(applied_ratio - prev_applied_ratio)
        hedge_adj_cost = hedge_turnover * float(hedge_rebal_cost_bps) / 10000.0
        total_cost = float(row["total_cost"]) + hedge_adj_cost
        return_net = (1.0 + gross_return) * (1.0 - total_cost) - 1.0

        nav *= 1.0 + return_net
        peak = max(peak, nav)

        rows.append(
            {
                "date": dt,
                "holding": row["holding"],
                "next_holding": row["next_holding"],
                "base_return": float(row["return"]),
                "base_return_net": float(row["return_net"]),
                "hedge_ret": float(row["hedge_ret"]),
                "prev_drawdown": prev_dd,
                "target_active_ratio": float(current_target_ratio),
                "applied_hedge_ratio": applied_ratio,
                "hedge_ratio_delta": hedge_delta,
                "hedge_turnover": hedge_turnover,
                "hedge_adj_cost": hedge_adj_cost,
                "gross_return": gross_return,
                "stock_and_base_cost": float(row["total_cost"]),
                "total_cost": total_cost,
                "return_net": return_net,
                "nav_net": nav,
                "drawdown": nav / peak - 1.0,
            }
        )
        prev_applied_ratio = applied_ratio

    return pd.DataFrame(rows).set_index("date")


def summarize_returns(ret: pd.Series) -> dict[str, float | int | str]:
    ret = ret.fillna(0.0).astype(float)
    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else np.nan
    vol = ret.std(ddof=1) * np.sqrt(252)
    sharpe = annual / vol if pd.notna(vol) and vol > 0 else np.nan
    dd = nav / nav.cummax() - 1.0
    return {
        "start_date": str(pd.Timestamp(ret.index[0]).date()),
        "end_date": str(pd.Timestamp(ret.index[-1]).date()),
        "days": int(len(ret)),
        "final_nav": float(nav.iloc[-1]),
        "total_return": float(nav.iloc[-1] - 1.0),
        "annual_return": float(annual),
        "annual_vol": float(vol),
        "sharpe": float(sharpe),
        "max_drawdown": float(dd.min()),
    }


def summarize_window(ret: pd.Series, start: pd.Timestamp, label: str) -> dict[str, float | int | str] | None:
    seg = ret.loc[ret.index >= start].dropna()
    if len(seg) < 20:
        return None
    out = summarize_returns(seg)
    out["window"] = label
    return out


def build_window_table(series_map: dict[str, pd.Series]) -> pd.DataFrame:
    latest = max(series.index[-1] for series in series_map.values())
    windows = [
        ("ytd", pd.Timestamp(year=latest.year, month=1, day=1)),
        ("1y", latest - pd.DateOffset(years=1)),
        ("3y", latest - pd.DateOffset(years=3)),
        ("5y", latest - pd.DateOffset(years=5)),
        ("10y", latest - pd.DateOffset(years=10)),
        ("15y", latest - pd.DateOffset(years=15)),
    ]
    rows = []
    for name, series in series_map.items():
        for label, start in windows:
            item = summarize_window(series, start, label)
            if item is None:
                continue
            item["strategy"] = name
            rows.append(item)
    return pd.DataFrame(rows)


def build_plot(nav_map: dict[str, pd.Series], output_rebased_csv: Path, output_plot_png: Path, title: str) -> None:
    latest = max(series.index[-1] for series in nav_map.values())
    start = latest - pd.DateOffset(years=10)
    rebased = {}
    for name, nav in nav_map.items():
        seg = nav.loc[nav.index >= start].copy()
        seg = seg / seg.iloc[0]
        rebased[name] = seg
    rebased_df = pd.DataFrame(rebased)
    rebased_df.to_csv(output_rebased_csv, index_label="date", encoding="utf-8-sig")

    plt.figure(figsize=(13, 7))
    plt.plot(rebased_df.index, rebased_df["v1.1_baseline_0p8x"], linewidth=2.2, label="v1.1 baseline 0.8x")
    strategy_name = next(col for col in rebased_df.columns if col != "v1.1_baseline_0p8x")
    plt.plot(rebased_df.index, rebased_df[strategy_name], linewidth=2.2, label=strategy_name)
    plt.title(title)
    plt.ylabel("Rebased NAV")
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_plot_png, dpi=160)
    plt.close()


def run_study(
    drawdown_trigger: float = DRAWDOWN_TRIGGER,
    base_ratio: float = BASE_RATIO,
    stress_ratio: float = STRESS_RATIO,
    hedge_rebal_cost_bps: float = HEDGE_REBAL_COST_BPS,
) -> dict[str, object]:
    base_df = load_base_nav()
    paths = build_output_paths(drawdown_trigger)
    sim_df = simulate_uphedge_strategy(
        base_df,
        base_ratio=base_ratio,
        stress_ratio=stress_ratio,
        drawdown_trigger=drawdown_trigger,
        hedge_rebal_cost_bps=hedge_rebal_cost_bps,
    )

    baseline_ret = base_df["return_net"].fillna(0.0).astype(float)
    strategy_ret = sim_df["return_net"].fillna(0.0).astype(float)
    strategy_label = f"v1.1_dd{int(round(drawdown_trigger * 100))}_uphedge_to_{str(stress_ratio).replace('.', 'p')}"

    sim_df.to_csv(paths["nav_csv"], encoding="utf-8-sig")
    windows_df = build_window_table(
        {
            "v1.1_baseline_0p8x": baseline_ret,
            strategy_label: strategy_ret,
        }
    )
    windows_df.to_csv(paths["windows_csv"], index=False, encoding="utf-8-sig")

    build_plot(
        {
            "v1.1_baseline_0p8x": (1.0 + baseline_ret).cumprod(),
            strategy_label: sim_df["nav_net"],
        },
        output_rebased_csv=paths["rebased_csv"],
        output_plot_png=paths["plot_png"],
        title=f"Top100 Mom16 v1.1 Uphedge On Drawdown {int(round(drawdown_trigger * 100))}% - Recent 10Y",
    )

    target_ratio_series = sim_df["target_active_ratio"]
    summary = {
        "as_of_date": str(pd.Timestamp(sim_df.index.max()).date()),
        "rule": {
            "base_ratio": float(base_ratio),
            "stress_ratio": float(stress_ratio),
            "drawdown_trigger": float(drawdown_trigger),
            "timing_rule": "use prior close realized drawdown; next session active hedge ratio switches to stress ratio once trigger is exceeded",
            "recover_rule": "restore to 0.8x only after nav recovers to a new high on a later session",
            "hedge_rebal_cost_bps": float(hedge_rebal_cost_bps),
        },
        "baseline": summarize_returns(baseline_ret),
        "strategy": summarize_returns(strategy_ret),
        "strategy_label": strategy_label,
        "overlay_stats": {
            "share_at_1p0_when_active": float(
                (
                    sim_df.loc[sim_df["applied_hedge_ratio"] > 0.0, "applied_hedge_ratio"] >= float(stress_ratio) - 1e-12
                ).mean()
            ),
            "avg_applied_hedge_ratio_when_active": float(
                sim_df.loc[sim_df["applied_hedge_ratio"] > 0.0, "applied_hedge_ratio"].mean()
            ),
            "n_hedge_ratio_changes": int((sim_df["hedge_turnover"] > 0).sum()),
            "extra_hedge_rebal_cost_total": float(sim_df["hedge_adj_cost"].sum()),
            "latest_prev_drawdown": float(sim_df["prev_drawdown"].iloc[-1]),
            "latest_target_active_ratio": float(target_ratio_series.iloc[-1]),
        },
        "artifacts": {
            "nav_csv": str(paths["nav_csv"]),
            "windows_csv": str(paths["windows_csv"]),
            "plot_png": str(paths["plot_png"]),
            "rebased_nav_csv": str(paths["rebased_csv"]),
        },
    }
    paths["summary_json"].write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    args = parse_args()
    summary = run_study(
        drawdown_trigger=args.drawdown_trigger,
        base_ratio=args.base_ratio,
        stress_ratio=args.stress_ratio,
        hedge_rebal_cost_bps=args.hedge_rebal_cost_bps,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
