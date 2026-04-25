from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "outputs"
BASE_NAV_CSV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_gapderisk_newpeak_v1_4_costed_nav.csv"
OUT_SUMMARY = OUTPUT_DIR / "microcap_top100_v1_4_same_side_overheat_summary.csv"
OUT_WINDOWS = OUTPUT_DIR / "microcap_top100_v1_4_same_side_overheat_windows.csv"
OUT_NAV = OUTPUT_DIR / "microcap_top100_v1_4_same_side_overheat_nav.csv"

BIAS_WINDOW = 60
TRADING_DAYS = 244
SCALE_CHANGE_COST = 0.003
DERISK_SCALE = 0.0


@dataclass(frozen=True)
class Scenario:
    name: str
    enter: float
    exit: float


SCENARIOS = [
    Scenario("scan_10_08", 0.10, 0.08),
    Scenario("scan_12_10", 0.12, 0.10),
    Scenario("scan_15_12", 0.15, 0.12),
    Scenario("scan_18_15", 0.18, 0.15),
    Scenario("scan_20_16", 0.20, 0.16),
    Scenario("adk_22_18", 0.22, 0.18),
    Scenario("scan_25_20", 0.25, 0.20),
    Scenario("suba_36_34", 0.36, 0.34),
]


WINDOWS = {
    "full": None,
    "recent_10y": 10,
    "recent_5y": 5,
    "recent_3y": 3,
    "recent_1y": 1,
}


def load_base() -> pd.DataFrame:
    if not BASE_NAV_CSV.exists():
        raise FileNotFoundError(BASE_NAV_CSV)
    df = pd.read_csv(BASE_NAV_CSV, parse_dates=["date"]).set_index("date").sort_index()
    required = {
        "return_net",
        "holding",
        "microcap_close",
        "hedge_close",
        "ratio_bias_mom",
        "nav_net",
    }
    missing = required.difference(df.columns)
    if missing:
        raise KeyError(f"Missing required v1.4 columns: {sorted(missing)}")
    return df


def metric_row(ret: pd.Series) -> dict[str, float]:
    ret = pd.to_numeric(ret, errors="coerce").fillna(0.0)
    nav = (1.0 + ret).cumprod()
    if len(nav) < 2:
        return {
            "annual_return": np.nan,
            "max_drawdown": np.nan,
            "sharpe": np.nan,
            "final_nav": float(nav.iloc[-1]) if len(nav) else np.nan,
        }
    years = (nav.index[-1] - nav.index[0]).days / 365.25
    annual = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else np.nan
    drawdown = nav / nav.cummax() - 1.0
    std = ret.std()
    sharpe = ret.mean() / std * math.sqrt(TRADING_DAYS) if std > 0 else np.nan
    return {
        "annual_return": float(annual),
        "max_drawdown": float(drawdown.min()),
        "sharpe": float(sharpe),
        "final_nav": float(nav.iloc[-1]),
    }


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    ratio = pd.to_numeric(df["microcap_close"], errors="coerce") / pd.to_numeric(df["hedge_close"], errors="coerce")
    bias = ratio.div(ratio.rolling(BIAS_WINDOW).mean()).sub(1.0)
    bias_mom = pd.to_numeric(df["ratio_bias_mom"], errors="coerce")
    same_side = (bias > 0.0) & (bias_mom > 0.0) & bias.notna() & bias_mom.notna()
    return pd.DataFrame(
        {
            "same_side_overheat_bias": bias,
            "same_side_overheat_bias_mom": bias_mom,
            "same_side_overheat_signal": same_side,
        },
        index=df.index,
    )


def apply_same_side_overheat(df: pd.DataFrame, features: pd.DataFrame, scenario: Scenario) -> pd.DataFrame:
    if not 0 < scenario.exit < scenario.enter:
        raise ValueError(f"Invalid scenario thresholds: {scenario}")

    base_ret = pd.to_numeric(df["return_net"], errors="coerce").fillna(0.0)
    holding = df["holding"].fillna("cash").astype(str)
    bias = features["same_side_overheat_bias"]
    same_side = features["same_side_overheat_signal"].fillna(False)

    defense_on = False
    prev_scale = 1.0
    returns: list[float] = []
    scales: list[float] = []
    triggered: list[bool] = []
    recovered: list[bool] = []
    costs: list[float] = []

    for dt in df.index:
        eligible = holding.loc[dt] != "cash"
        current_scale = DERISK_SCALE if defense_on and eligible else 1.0

        if not eligible:
            next_defense_on = False
        elif pd.notna(bias.loc[dt]) and bool(same_side.loc[dt]):
            if defense_on:
                next_defense_on = not (float(bias.loc[dt]) <= scenario.exit)
            else:
                next_defense_on = bool(float(bias.loc[dt]) >= scenario.enter)
        else:
            next_defense_on = False

        scale_change = abs(current_scale - prev_scale)
        transition_cost = SCALE_CHANGE_COST * scale_change if scale_change > 1e-12 else 0.0
        realized = (1.0 + float(base_ret.loc[dt]) * current_scale) * (1.0 - transition_cost) - 1.0

        returns.append(float(realized))
        scales.append(float(current_scale))
        triggered.append(bool(current_scale < 0.999999 and prev_scale >= 0.999999))
        recovered.append(bool(current_scale >= 0.999999 and prev_scale < 0.999999))
        costs.append(float(transition_cost))

        defense_on = next_defense_on
        prev_scale = current_scale

    out = df.copy()
    out["same_side_overheat_return_net"] = pd.Series(returns, index=df.index, dtype=float)
    out["same_side_overheat_nav_net"] = (1.0 + out["same_side_overheat_return_net"]).cumprod()
    out["same_side_overheat_scale"] = pd.Series(scales, index=df.index, dtype=float)
    out["same_side_overheat_triggered"] = pd.Series(triggered, index=df.index, dtype=bool)
    out["same_side_overheat_recovered"] = pd.Series(recovered, index=df.index, dtype=bool)
    out["same_side_overheat_cost"] = pd.Series(costs, index=df.index, dtype=float)
    out["same_side_overheat_enter"] = float(scenario.enter)
    out["same_side_overheat_exit"] = float(scenario.exit)
    for col in features.columns:
        out[col] = features[col]
    return out


def slice_window(df: pd.DataFrame, years: int | None) -> pd.DataFrame:
    if years is None:
        return df
    cutoff = df.index.max() - pd.DateOffset(years=years)
    return df.loc[df.index >= cutoff].copy()


def summarize_scenario(base: pd.DataFrame, scenario_df: pd.DataFrame, scenario: Scenario) -> dict[str, float | str | int]:
    base_metrics = metric_row(base["return_net"])
    scenario_metrics = metric_row(scenario_df["same_side_overheat_return_net"])
    return {
        "scenario": scenario.name,
        "enter": scenario.enter,
        "exit": scenario.exit,
        "base_annual_return": base_metrics["annual_return"],
        "annual_return": scenario_metrics["annual_return"],
        "annual_return_delta": scenario_metrics["annual_return"] - base_metrics["annual_return"],
        "base_max_drawdown": base_metrics["max_drawdown"],
        "max_drawdown": scenario_metrics["max_drawdown"],
        "max_drawdown_delta": scenario_metrics["max_drawdown"] - base_metrics["max_drawdown"],
        "base_sharpe": base_metrics["sharpe"],
        "sharpe": scenario_metrics["sharpe"],
        "sharpe_delta": scenario_metrics["sharpe"] - base_metrics["sharpe"],
        "base_final_nav": base_metrics["final_nav"],
        "final_nav": scenario_metrics["final_nav"],
        "defense_days": int((scenario_df["same_side_overheat_scale"] < 0.999999).sum()),
        "defense_ratio": float((scenario_df["same_side_overheat_scale"] < 0.999999).mean()),
        "trigger_count": int(scenario_df["same_side_overheat_triggered"].sum()),
        "recovery_count": int(scenario_df["same_side_overheat_recovered"].sum()),
        "extra_transition_cost_sum": float(scenario_df["same_side_overheat_cost"].sum()),
    }


def main() -> None:
    base = load_base()
    features = build_features(base)
    nav_cols = {
        "v1_4_base": (1.0 + pd.to_numeric(base["return_net"], errors="coerce").fillna(0.0)).cumprod()
    }
    summary_rows = []
    window_rows = []

    for scenario in SCENARIOS:
        scenario_df = apply_same_side_overheat(base, features, scenario)
        summary_rows.append(summarize_scenario(base, scenario_df, scenario))
        nav_cols[scenario.name] = scenario_df["same_side_overheat_nav_net"]

        for window_name, years in WINDOWS.items():
            base_window = slice_window(base, years)
            scenario_window = slice_window(scenario_df, years)
            row = summarize_scenario(base_window, scenario_window, scenario)
            row["window"] = window_name
            row["start_date"] = str(scenario_window.index.min().date())
            row["end_date"] = str(scenario_window.index.max().date())
            window_rows.append(row)

    summary = pd.DataFrame(summary_rows)
    windows = pd.DataFrame(window_rows)
    nav = pd.DataFrame(nav_cols)
    nav.index.name = "date"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUT_SUMMARY, index=False, encoding="utf-8")
    windows.to_csv(OUT_WINDOWS, index=False, encoding="utf-8")
    nav.to_csv(OUT_NAV, encoding="utf-8")

    print(f"source: {BASE_NAV_CSV}")
    print(f"rows: {len(base)} date: {base.index.min().date()}~{base.index.max().date()}")
    print("bias quantiles on v1.4 active holding days:")
    active_bias = features.loc[base["holding"].astype(str) != "cash", "same_side_overheat_bias"]
    print(active_bias.quantile([0.50, 0.75, 0.90, 0.95, 0.97, 0.99]).to_string())
    cols = [
        "scenario",
        "enter",
        "exit",
        "annual_return",
        "annual_return_delta",
        "max_drawdown",
        "max_drawdown_delta",
        "sharpe",
        "sharpe_delta",
        "defense_days",
        "trigger_count",
    ]
    print(summary[cols].to_string(index=False, float_format=lambda x: f"{x:.6f}"))
    print(f"saved: {OUT_SUMMARY}")
    print(f"saved: {OUT_WINDOWS}")
    print(f"saved: {OUT_NAV}")


if __name__ == "__main__":
    main()
