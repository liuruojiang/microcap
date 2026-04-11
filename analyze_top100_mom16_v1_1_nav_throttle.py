import json
from dataclasses import asdict, dataclass
from itertools import product
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
INPUT_NAV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv"

SCAN_CSV = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_scan.csv"
TOP_CSV = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_top20.csv"
WINDOWS_CSV = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_windows.csv"
SUMMARY_JSON = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_summary.json"
PLOT_PNG = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_recent10y_compare.png"
REBASed_CSV = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_recent10y_rebased_nav.csv"

REBAL_COST_BPS = 2.0


@dataclass(frozen=True)
class ThrottleConfig:
    dd_moderate: float
    dd_severe: float
    scale_moderate: float
    scale_severe: float
    recover_dd: float
    step_up: float
    rebal_cost_bps: float = REBAL_COST_BPS

    @property
    def label(self) -> str:
        return (
            f"dd{int(round(self.dd_moderate * 100))}_{int(round(self.dd_severe * 100))}"
            f"_sc{int(round(self.scale_moderate * 100))}_{int(round(self.scale_severe * 100))}"
            f"_rec{int(round(self.recover_dd * 1000))}"
            f"_step{int(round(self.step_up * 1000))}"
        )


OLD_BEST = ThrottleConfig(
    dd_moderate=0.06,
    dd_severe=0.10,
    scale_moderate=0.90,
    scale_severe=0.75,
    recover_dd=0.03,
    step_up=0.02,
)

NAMED_SCENARIOS = {
    "old_best_mild": OLD_BEST,
    "mild_7_10": ThrottleConfig(0.07, 0.10, 0.85, 0.70, 0.04, 0.01),
    "balanced_6_9": ThrottleConfig(0.06, 0.09, 0.80, 0.60, 0.03, 0.01),
    "gradual_5_8": ThrottleConfig(0.05, 0.08, 0.80, 0.55, 0.025, 0.005),
    "aggressive_4_7": ThrottleConfig(0.04, 0.07, 0.75, 0.50, 0.02, 0.005),
}


def load_base_returns() -> tuple[pd.Series, pd.DataFrame]:
    df = pd.read_csv(INPUT_NAV, parse_dates=["date"])
    df = df.sort_values("date").set_index("date")
    if "return_net" not in df.columns:
        raise ValueError(f"return_net missing in {INPUT_NAV}")
    base_ret = df["return_net"].astype(float).dropna()
    return base_ret, df


def summarize_returns(ret: pd.Series) -> dict:
    ret = ret.dropna()
    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else np.nan
    vol = ret.std(ddof=1) * np.sqrt(252)
    sharpe = annual / vol if pd.notna(vol) and vol > 0 else np.nan
    drawdown = nav / nav.cummax() - 1.0
    return {
        "annual_return": float(annual),
        "annual_vol": float(vol),
        "sharpe": float(sharpe),
        "max_drawdown": float(drawdown.min()),
        "total_return": float(nav.iloc[-1] - 1.0),
        "start": str(ret.index[0].date()),
        "end": str(ret.index[-1].date()),
        "days": int(len(ret)),
    }


def summarize_window(ret: pd.Series, start: pd.Timestamp, label: str) -> dict | None:
    seg = ret.loc[ret.index >= start].dropna()
    if len(seg) < 20:
        return None
    summary = summarize_returns(seg)
    summary["window"] = label
    return summary


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
            summary = summarize_window(series, start, label)
            if summary is None:
                continue
            summary["strategy"] = name
            rows.append(summary)
    return pd.DataFrame(rows)


def apply_drawdown_throttle(base_ret: pd.Series, cfg: ThrottleConfig) -> pd.DataFrame:
    base_ret = base_ret.dropna()
    nav = 1.0
    peak = 1.0
    current_scale = 1.0
    daily_cost_per_unit = cfg.rebal_cost_bps / 10000.0
    rows = []

    for dt, base_r in base_ret.items():
        prev_dd = nav / peak - 1.0
        if prev_dd <= -cfg.dd_severe:
            target_scale = cfg.scale_severe
            state = "severe"
        elif prev_dd <= -cfg.dd_moderate:
            target_scale = cfg.scale_moderate
            state = "moderate"
        elif prev_dd >= -cfg.recover_dd:
            target_scale = 1.0
            state = "normal"
        else:
            target_scale = current_scale
            state = "recovery_band"

        if target_scale < current_scale:
            new_scale = target_scale
        elif target_scale > current_scale:
            new_scale = min(current_scale + cfg.step_up, target_scale)
        else:
            new_scale = current_scale

        turnover = abs(new_scale - current_scale)
        cost = turnover * daily_cost_per_unit if turnover > 0 else 0.0
        adj_r = new_scale * base_r - cost
        nav *= 1.0 + adj_r
        peak = max(peak, nav)
        current_scale = new_scale

        rows.append(
            {
                "date": dt,
                "base_return": base_r,
                "return": adj_r,
                "scale": new_scale,
                "target_scale": target_scale,
                "state": state,
                "prev_drawdown": prev_dd,
                "turnover": turnover,
                "cost": cost,
                "nav": nav,
                "peak": peak,
                "drawdown": nav / peak - 1.0,
            }
        )

    return pd.DataFrame(rows).set_index("date")


def summarize_throttle(run_df: pd.DataFrame) -> dict:
    scale = run_df["scale"]
    states = run_df["state"]
    summary = {
        "avg_scale": float(scale.mean()),
        "median_scale": float(scale.median()),
        "min_scale": float(scale.min()),
        "share_below_1": float((scale < 0.999999).mean()),
        "share_at_severe_scale": float((scale <= scale.min() + 1e-9).mean()),
        "avg_turnover": float(run_df["turnover"].mean()),
        "annualized_turnover": float(run_df["turnover"].sum() / len(run_df) * 252.0),
        "moderate_or_worse_share": float(states.isin(["moderate", "severe", "recovery_band"]).mean()),
        "severe_share": float((states == "severe").mean()),
        "recovery_band_share": float((states == "recovery_band").mean()),
        "n_scale_changes": int((run_df["turnover"] > 0).sum()),
    }
    return summary


def iter_scan_configs() -> list[ThrottleConfig]:
    configs = []
    for dd_m, dd_s, sc_m, sc_s, rec, step in product(
        [0.05, 0.06, 0.07],
        [0.08, 0.09, 0.10],
        [0.85, 0.90, 0.95],
        [0.70, 0.75, 0.80],
        [0.03, 0.04],
        [0.01, 0.02],
    ):
        if dd_s <= dd_m:
            continue
        if sc_s >= sc_m:
            continue
        configs.append(
            ThrottleConfig(
                dd_moderate=dd_m,
                dd_severe=dd_s,
                scale_moderate=sc_m,
                scale_severe=sc_s,
                recover_dd=rec,
                step_up=step,
            )
        )
    return configs


def make_scan_table(base_ret: pd.Series) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    rows = []
    run_map = {}
    for cfg in iter_scan_configs():
        run_df = apply_drawdown_throttle(base_ret, cfg)
        perf = summarize_returns(run_df["return"])
        throttle = summarize_throttle(run_df)
        row = {
            "label": cfg.label,
            **asdict(cfg),
            **perf,
            **throttle,
        }
        rows.append(row)
        run_map[cfg.label] = run_df

    scan_df = pd.DataFrame(rows).sort_values(
        ["sharpe", "max_drawdown", "annual_return"],
        ascending=[False, False, False],
    )
    return scan_df.reset_index(drop=True), run_map


def build_plot(nav_map: dict[str, pd.Series]) -> None:
    latest = max(series.index[-1] for series in nav_map.values())
    start = latest - pd.DateOffset(years=10)
    rebased = {}
    for name, nav in nav_map.items():
        seg = nav.loc[nav.index >= start].copy()
        seg = seg / seg.iloc[0]
        rebased[name] = seg

    rebased_df = pd.DataFrame(rebased)
    rebased_df.to_csv(REBASed_CSV, index_label="date")

    plt.figure(figsize=(13, 7))
    for name, series in rebased.items():
        plt.plot(series.index, series.values, linewidth=2.0, label=name)
    plt.title("Top100 Mom16 V1.1 NAV Throttle Compare - Recent 10Y")
    plt.ylabel("Rebased NAV")
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(PLOT_PNG, dpi=160)
    plt.close()


def main() -> None:
    base_ret, raw_df = load_base_returns()
    baseline_summary = summarize_returns(base_ret)

    scan_df, run_map = make_scan_table(base_ret)
    best_label = scan_df.iloc[0]["label"]
    best_cfg = ThrottleConfig(
        dd_moderate=float(scan_df.iloc[0]["dd_moderate"]),
        dd_severe=float(scan_df.iloc[0]["dd_severe"]),
        scale_moderate=float(scan_df.iloc[0]["scale_moderate"]),
        scale_severe=float(scan_df.iloc[0]["scale_severe"]),
        recover_dd=float(scan_df.iloc[0]["recover_dd"]),
        step_up=float(scan_df.iloc[0]["step_up"]),
        rebal_cost_bps=float(scan_df.iloc[0]["rebal_cost_bps"]),
    )

    old_best_run = apply_drawdown_throttle(base_ret, OLD_BEST)
    named_rows = []
    for name, cfg in NAMED_SCENARIOS.items():
        run_df = apply_drawdown_throttle(base_ret, cfg)
        named_rows.append(
            {
                "scenario": name,
                "label": cfg.label,
                **asdict(cfg),
                **summarize_returns(run_df["return"]),
                **summarize_throttle(run_df),
            }
        )
    named_df = pd.DataFrame(named_rows).sort_values(
        ["sharpe", "max_drawdown", "annual_return"],
        ascending=[False, False, False],
    )

    top_df = scan_df.head(20).copy()
    top_df.to_csv(TOP_CSV, index=False, encoding="utf-8-sig")
    scan_df.to_csv(SCAN_CSV, index=False, encoding="utf-8-sig")

    best_run = run_map[best_label]
    windows_df = build_window_table(
        {
            "baseline_v1_1": base_ret,
            "old_best_mild": old_best_run["return"],
            "best_v1_1_scan": best_run["return"],
        }
    )
    windows_df.to_csv(WINDOWS_CSV, index=False, encoding="utf-8-sig")

    baseline_nav = (1.0 + base_ret).cumprod()
    old_best_nav = (1.0 + old_best_run["return"]).cumprod()
    best_nav = (1.0 + best_run["return"]).cumprod()
    build_plot(
        {
            "baseline_v1.1": baseline_nav,
            "old_best_mild": old_best_nav,
            "best_v1.1_scan": best_nav,
        }
    )

    summary = {
        "input_nav_csv": str(INPUT_NAV),
        "latest_date": str(raw_df.index.max().date()),
        "baseline_v1_1": baseline_summary,
        "old_best_reference": {
            "source": "archive/mnt_combined_drawdown_throttle_20260327.md",
            "config": asdict(OLD_BEST),
            "performance": summarize_returns(old_best_run["return"]),
            "throttle": summarize_throttle(old_best_run),
            "scan_rank_by_sharpe": int(scan_df.index[scan_df["label"] == OLD_BEST.label][0]) + 1
            if (scan_df["label"] == OLD_BEST.label).any()
            else None,
        },
        "best_scan_config": {
            "config": asdict(best_cfg),
            "label": best_label,
            "performance": summarize_returns(best_run["return"]),
            "throttle": summarize_throttle(best_run),
        },
        "scan_grid": {
            "dd_moderate": [0.05, 0.06, 0.07],
            "dd_severe": [0.08, 0.09, 0.10],
            "scale_moderate": [0.85, 0.90, 0.95],
            "scale_severe": [0.70, 0.75, 0.80],
            "recover_dd": [0.03, 0.04],
            "step_up": [0.01, 0.02],
            "rebal_cost_bps": REBAL_COST_BPS,
            "n_configs": int(len(scan_df)),
        },
        "named_scenarios_ranked": named_df.to_dict(orient="records"),
        "artifacts": {
            "scan_csv": str(SCAN_CSV),
            "top_csv": str(TOP_CSV),
            "windows_csv": str(WINDOWS_CSV),
            "plot_png": str(PLOT_PNG),
            "rebased_nav_csv": str(REBASed_CSV),
        },
    }
    SUMMARY_JSON.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
