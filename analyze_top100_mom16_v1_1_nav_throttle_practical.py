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

SCAN_CSV = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_practical_scan.csv"
TOP_CSV = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_practical_top20.csv"
WINDOWS_CSV = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_practical_windows.csv"
SUMMARY_JSON = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_practical_summary.json"
PLOT_PNG = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_practical_recent10y_compare.png"
REBASed_CSV = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_throttle_practical_recent10y_rebased_nav.csv"

REBAL_COST_BPS = 2.0


@dataclass(frozen=True)
class PracticalThrottleConfig:
    dd_moderate: float
    dd_severe: float
    scale_moderate: float
    scale_severe: float
    recover_dd: float
    rebal_cost_bps: float = REBAL_COST_BPS

    @property
    def label(self) -> str:
        return (
            f"dd{int(round(self.dd_moderate * 100))}_{int(round(self.dd_severe * 100))}"
            f"_sc{int(round(self.scale_moderate * 100))}_{int(round(self.scale_severe * 100))}"
            f"_rec{int(round(self.recover_dd * 100))}"
        )


REFERENCE_CONFIG = PracticalThrottleConfig(
    dd_moderate=0.03,
    dd_severe=0.06,
    scale_moderate=0.85,
    scale_severe=0.70,
    recover_dd=0.03,
)


def load_base_returns() -> tuple[pd.Series, pd.DataFrame]:
    df = pd.read_csv(INPUT_NAV, parse_dates=["date"]).sort_values("date").set_index("date")
    if "return_net" not in df.columns:
        raise ValueError(f"return_net missing in {INPUT_NAV}")
    return df["return_net"].astype(float).dropna(), df


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


def target_scale_from_drawdown(prev_dd: float, cfg: PracticalThrottleConfig, current_scale: float) -> tuple[float, str]:
    if prev_dd <= -cfg.dd_severe:
        return cfg.scale_severe, "severe"
    if prev_dd <= -cfg.dd_moderate:
        return cfg.scale_moderate, "moderate"
    if prev_dd >= -cfg.recover_dd:
        return 1.0, "normal"
    return current_scale, "recovery_band"


def apply_practical_throttle(base_ret: pd.Series, cfg: PracticalThrottleConfig) -> pd.DataFrame:
    """
    Uses yesterday's realized NAV drawdown to set today's scale.
    This is equivalent to: T close observes signal, T+1 session applies new scale.
    """
    base_ret = base_ret.dropna()
    nav = 1.0
    peak = 1.0
    current_scale = 1.0
    daily_cost_per_unit = cfg.rebal_cost_bps / 10000.0
    rows = []

    for dt, base_r in base_ret.items():
        prev_dd = nav / peak - 1.0
        target_scale, state = target_scale_from_drawdown(prev_dd, cfg, current_scale)
        new_scale = target_scale

        turnover = abs(new_scale - current_scale)
        cost = turnover * daily_cost_per_unit if turnover > 0 else 0.0
        adj_r = new_scale * base_r - cost

        nav *= 1.0 + adj_r
        peak = max(peak, nav)
        prior_scale = current_scale
        current_scale = new_scale

        rows.append(
            {
                "date": dt,
                "base_return": base_r,
                "return": adj_r,
                "scale": new_scale,
                "prior_scale": prior_scale,
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
    return {
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


def iter_scan_configs() -> list[PracticalThrottleConfig]:
    configs = []
    for dd_m, dd_s, sc_m, sc_s, rec in product(
        [0.03, 0.04, 0.05],
        [0.06, 0.08, 0.10],
        [0.85, 0.90, 0.95],
        [0.65, 0.70, 0.75, 0.80],
        [0.02, 0.03],
    ):
        if dd_s <= dd_m:
            continue
        if sc_s >= sc_m:
            continue
        if rec > dd_m:
            continue
        configs.append(
            PracticalThrottleConfig(
                dd_moderate=dd_m,
                dd_severe=dd_s,
                scale_moderate=sc_m,
                scale_severe=sc_s,
                recover_dd=rec,
            )
        )
    return configs


def make_scan_table(base_ret: pd.Series) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    rows = []
    run_map = {}
    for cfg in iter_scan_configs():
        run_df = apply_practical_throttle(base_ret, cfg)
        rows.append(
            {
                "label": cfg.label,
                **asdict(cfg),
                **summarize_returns(run_df["return"]),
                **summarize_throttle(run_df),
            }
        )
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
    plt.title("Top100 Mom16 V1.1 Practical NAV Throttle Compare - Recent 10Y")
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
    best_row = scan_df.iloc[0]
    best_cfg = PracticalThrottleConfig(
        dd_moderate=float(best_row["dd_moderate"]),
        dd_severe=float(best_row["dd_severe"]),
        scale_moderate=float(best_row["scale_moderate"]),
        scale_severe=float(best_row["scale_severe"]),
        recover_dd=float(best_row["recover_dd"]),
        rebal_cost_bps=float(best_row["rebal_cost_bps"]),
    )

    ref_run = apply_practical_throttle(base_ret, REFERENCE_CONFIG)
    best_run = run_map[best_cfg.label]

    scan_df.to_csv(SCAN_CSV, index=False, encoding="utf-8-sig")
    scan_df.head(20).to_csv(TOP_CSV, index=False, encoding="utf-8-sig")

    windows_df = build_window_table(
        {
            "baseline_v1_1": base_ret,
            "reference_3_6": ref_run["return"],
            "best_practical_scan": best_run["return"],
        }
    )
    windows_df.to_csv(WINDOWS_CSV, index=False, encoding="utf-8-sig")

    build_plot(
        {
            "baseline_v1.1": (1.0 + base_ret).cumprod(),
            "reference_3_6": (1.0 + ref_run["return"]).cumprod(),
            "best_practical_scan": (1.0 + best_run["return"]).cumprod(),
        }
    )

    summary = {
        "input_nav_csv": str(INPUT_NAV),
        "latest_date": str(raw_df.index.max().date()),
        "logic_notes": {
            "signal_timing": "uses prior close drawdown to set next session scale",
            "rerisking": "no daily step-up; once drawdown recovers inside recover_dd, restore to full immediately on next session",
            "trimmed_capital_return": 0.0,
            "rebal_cost_bps_per_unit_turnover": REBAL_COST_BPS,
        },
        "baseline_v1_1": baseline_summary,
        "reference_config": {
            "config": asdict(REFERENCE_CONFIG),
            "performance": summarize_returns(ref_run["return"]),
            "throttle": summarize_throttle(ref_run),
            "scan_rank_by_sharpe": int(scan_df.index[scan_df["label"] == REFERENCE_CONFIG.label][0]) + 1
            if (scan_df["label"] == REFERENCE_CONFIG.label).any()
            else None,
        },
        "best_scan_config": {
            "config": asdict(best_cfg),
            "label": best_cfg.label,
            "performance": summarize_returns(best_run["return"]),
            "throttle": summarize_throttle(best_run),
        },
        "scan_grid": {
            "dd_moderate": [0.03, 0.04, 0.05],
            "dd_severe": [0.06, 0.08, 0.10],
            "scale_moderate": [0.85, 0.90, 0.95],
            "scale_severe": [0.65, 0.70, 0.75, 0.80],
            "recover_dd": [0.02, 0.03],
            "n_configs": int(len(scan_df)),
        },
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
