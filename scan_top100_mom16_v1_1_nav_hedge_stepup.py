import json
from dataclasses import asdict, dataclass
from itertools import product
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

import analyze_top100_rebalance_frequency as freq_mod
from analyze_top100_mom16_hedge0p5_nav_throttle_practical import (
    apply_fixed_hedge_ratio,
    load_gross_result,
    load_turnover,
    summarize_returns,
)


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
BASE_HEDGE_RATIO = 0.8

SCAN_CSV = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_hedge_stepup_scan.csv"
TOP_CSV = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_hedge_stepup_top20.csv"
WINDOWS_CSV = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_hedge_stepup_windows.csv"
SUMMARY_JSON = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_hedge_stepup_summary.json"
PLOT_PNG = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_hedge_stepup_recent10y_compare.png"
REBASed_CSV = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_hedge_stepup_recent10y_rebased_nav.csv"
BEST_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_v1_1_nav_hedge_stepup_best_16y_costed_nav.csv"


@dataclass(frozen=True)
class HedgeStepConfig:
    dd_moderate: float
    dd_severe: float
    hedge_ratio_moderate: float
    hedge_ratio_severe: float
    recover_dd: float

    @property
    def label(self) -> str:
        return (
            f"dd{int(round(self.dd_moderate * 100))}_{int(round(self.dd_severe * 100))}"
            f"_hr{int(round(self.hedge_ratio_moderate * 100))}_{int(round(self.hedge_ratio_severe * 100))}"
            f"_rec{int(round(self.recover_dd * 100))}"
        )


def load_baseline_net() -> pd.DataFrame:
    turnover = load_turnover()
    gross = load_gross_result()
    gross_0p8 = apply_fixed_hedge_ratio(gross, BASE_HEDGE_RATIO)
    net = freq_mod.cost_mod.apply_cost_model(gross_0p8, turnover)
    net.index = pd.to_datetime(net.index)
    return net


def summarize_window(ret: pd.Series, start: pd.Timestamp, label: str) -> dict | None:
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


def iter_scan_configs() -> list[HedgeStepConfig]:
    configs = []
    for dd_m, dd_s, hr_m, hr_s, rec in product(
        [0.03, 0.04, 0.05],
        [0.06, 0.08, 0.10],
        [0.9, 1.0],
        [1.0, 1.1, 1.2, 1.3],
        [0.02, 0.03],
    ):
        if dd_s <= dd_m:
            continue
        if hr_m <= BASE_HEDGE_RATIO:
            continue
        if hr_s <= hr_m:
            continue
        if rec > dd_m:
            continue
        configs.append(
            HedgeStepConfig(
                dd_moderate=dd_m,
                dd_severe=dd_s,
                hedge_ratio_moderate=hr_m,
                hedge_ratio_severe=hr_s,
                recover_dd=rec,
            )
        )
    return configs


def apply_nav_hedge_stepup(baseline_net: pd.DataFrame, cfg: HedgeStepConfig) -> pd.DataFrame:
    """
    Practical timing:
    - Observe realized NAV drawdown at T close
    - Apply the new hedge ratio on T+1
    """
    base = baseline_net.copy()
    nav = 1.0
    peak = 1.0
    current_hedge_ratio = BASE_HEDGE_RATIO
    rows = []

    for dt, row in base.iterrows():
        prev_dd = nav / peak - 1.0
        active = row["holding"] != "cash"

        if prev_dd <= -cfg.dd_severe:
            target_ratio = cfg.hedge_ratio_severe
            state = "severe"
        elif prev_dd <= -cfg.dd_moderate:
            target_ratio = cfg.hedge_ratio_moderate
            state = "moderate"
        elif prev_dd >= -cfg.recover_dd:
            target_ratio = BASE_HEDGE_RATIO
            state = "normal"
        else:
            target_ratio = current_hedge_ratio
            state = "recovery_band"

        hedge_ratio = target_ratio if active else 0.0
        futures_drag = row["futures_drag"]
        if active:
            microcap_ret = row["microcap_ret"]
            hedge_ret = row["hedge_ret"]
            active_spread_ret = microcap_ret - hedge_ratio * hedge_ret
            futures_drag = hedge_ratio * row["futures_drag"] / BASE_HEDGE_RATIO if BASE_HEDGE_RATIO > 0 else 0.0
            return_raw = active_spread_ret - futures_drag
        else:
            active_spread_ret = 0.0
            return_raw = 0.0

        total_cost = row["total_cost"]
        return_net = return_raw - total_cost

        nav *= 1.0 + return_net
        peak = max(peak, nav)
        current_hedge_ratio = target_ratio

        rows.append(
            {
                "date": dt,
                "holding": row["holding"],
                "signal_on": row["signal_on"],
                "microcap_ret": row["microcap_ret"],
                "hedge_ret": row["hedge_ret"],
                "entry_exit_cost": row["entry_exit_cost"],
                "rebalance_cost": row["rebalance_cost"],
                "total_cost": total_cost,
                "prev_drawdown": prev_dd,
                "nav_hedge_state": state,
                "target_hedge_ratio": target_ratio,
                "hedge_ratio_live": hedge_ratio,
                "active_spread_ret": active_spread_ret,
                "futures_drag_dynamic": futures_drag,
                "return_raw_dynamic": return_raw,
                "return_net": return_net,
                "nav_net": nav,
            }
        )

    return pd.DataFrame(rows).set_index("date")


def summarize_overlay(run_df: pd.DataFrame) -> dict:
    live_ratio = run_df["hedge_ratio_live"]
    states = run_df["nav_hedge_state"]
    active_ratio = live_ratio[run_df["holding"].ne("cash")]
    return {
        "hedge_ratio_mean_live": float(live_ratio.mean()),
        "hedge_ratio_median_live": float(live_ratio.median()),
        "hedge_ratio_max_live": float(live_ratio.max()),
        "hedge_ratio_mean_active": float(active_ratio.mean()) if len(active_ratio) else None,
        "hedge_ratio_median_active": float(active_ratio.median()) if len(active_ratio) else None,
        "share_above_0p8": float((live_ratio > BASE_HEDGE_RATIO + 1e-9).mean()),
        "share_above_1p0": float((live_ratio > 1.0 + 1e-9).mean()),
        "moderate_or_worse_share": float(states.isin(["moderate", "severe", "recovery_band"]).mean()),
        "severe_share": float((states == "severe").mean()),
        "recovery_band_share": float((states == "recovery_band").mean()),
        "n_ratio_changes": int((live_ratio.diff().fillna(0.0).abs() > 1e-12).sum()),
    }


def build_plot(nav_map: dict[str, pd.Series]) -> None:
    latest = max(series.index[-1] for series in nav_map.values())
    start = latest - pd.DateOffset(years=10)
    rebased = {}
    for name, nav in nav_map.items():
        seg = nav.loc[nav.index >= start].copy()
        seg = seg / seg.iloc[0]
        rebased[name] = seg
    pd.DataFrame(rebased).to_csv(REBASed_CSV, index_label="date")

    plt.figure(figsize=(13, 7))
    for name, series in rebased.items():
        plt.plot(series.index, series.values, linewidth=2.0, label=name)
    plt.title("Top100 Mom16 V1.1 NAV Hedge Step-Up Compare - Recent 10Y")
    plt.ylabel("Rebased NAV")
    plt.grid(alpha=0.25)
    plt.legend()
    plt.tight_layout()
    plt.savefig(PLOT_PNG, dpi=160)
    plt.close()


def main() -> None:
    baseline = load_baseline_net()
    baseline_ret = baseline["return_net"].fillna(0.0)

    rows = []
    run_map: dict[str, pd.DataFrame] = {}
    for cfg in iter_scan_configs():
        run_df = apply_nav_hedge_stepup(baseline, cfg)
        perf = summarize_returns(run_df["return_net"])
        rows.append(
            {
                "label": cfg.label,
                **asdict(cfg),
                **perf,
                **summarize_overlay(run_df),
            }
        )
        run_map[cfg.label] = run_df

    scan_df = pd.DataFrame(rows).sort_values(
        ["sharpe", "max_drawdown", "annual_return"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    scan_df.to_csv(SCAN_CSV, index=False, encoding="utf-8-sig")
    scan_df.head(20).to_csv(TOP_CSV, index=False, encoding="utf-8-sig")

    best_row = scan_df.iloc[0]
    best_cfg = HedgeStepConfig(
        dd_moderate=float(best_row["dd_moderate"]),
        dd_severe=float(best_row["dd_severe"]),
        hedge_ratio_moderate=float(best_row["hedge_ratio_moderate"]),
        hedge_ratio_severe=float(best_row["hedge_ratio_severe"]),
        recover_dd=float(best_row["recover_dd"]),
    )
    best_run = run_map[best_cfg.label]
    best_run.to_csv(BEST_NAV_CSV, encoding="utf-8-sig")

    windows_df = build_window_table(
        {
            "baseline_v1_1": baseline_ret,
            "best_nav_hedge_stepup": best_run["return_net"].fillna(0.0),
        }
    )
    windows_df.to_csv(WINDOWS_CSV, index=False, encoding="utf-8-sig")

    build_plot(
        {
            "baseline_v1.1": (1.0 + baseline_ret).cumprod(),
            "best_nav_hedge_stepup": (1.0 + best_run["return_net"].fillna(0.0)).cumprod(),
        }
    )

    payload = {
        "as_of_date": str(pd.Timestamp(baseline.index.max()).date()),
        "base_hedge_ratio": BASE_HEDGE_RATIO,
        "logic_notes": {
            "trigger_basis": "realized NAV drawdown at T close",
            "execution_timing": "new hedge ratio applies on T+1",
            "position_change": "stock leg unchanged; only hedge ratio steps up",
            "recover_rule": "restore to 0.8x once drawdown recovers inside recover_dd",
            "recovery_band": "between recover_dd and moderate threshold, keep current hedge ratio",
            "futures_rehedge_fee": "not added; same as prior hedge-ratio scans",
        },
        "baseline_v1_1": summarize_returns(baseline_ret),
        "best_scan_config": {
            "config": asdict(best_cfg),
            "label": best_cfg.label,
            "performance": summarize_returns(best_run["return_net"].fillna(0.0)),
            "overlay": summarize_overlay(best_run),
        },
        "scan_grid": {
            "dd_moderate": [0.03, 0.04, 0.05],
            "dd_severe": [0.06, 0.08, 0.10],
            "hedge_ratio_moderate": [0.9, 1.0],
            "hedge_ratio_severe": [1.0, 1.1, 1.2, 1.3],
            "recover_dd": [0.02, 0.03],
            "n_configs": int(len(scan_df)),
        },
        "artifacts": {
            "scan_csv": str(SCAN_CSV),
            "top_csv": str(TOP_CSV),
            "windows_csv": str(WINDOWS_CSV),
            "best_nav_csv": str(BEST_NAV_CSV),
            "plot_png": str(PLOT_PNG),
            "rebased_nav_csv": str(REBASed_CSV),
        },
    }
    SUMMARY_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
