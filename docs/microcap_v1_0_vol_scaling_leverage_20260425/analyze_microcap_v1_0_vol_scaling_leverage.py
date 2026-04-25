from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "docs" / "microcap_v1_0_vol_scaling_leverage_20260425"
SOURCE_NAV = ROOT / "outputs" / "microcap_top100_mom16_hedge_zz1000_biweekly_thursday_16y_costed_nav.csv"

TRADING_DAYS = 244
VOL_WINDOW = 60
TARGET_VOLS = [0.15, 0.20, 0.25, 0.30, 0.35]
MAX_LEV = 1.5
SCALE_CHANGE_COST = 0.001
FINANCING_RATE = 0.03

WINDOWS = [
    ("full", None, None),
    ("10y", "2016-04-25", "2026-04-24"),
    ("5y", "2021-04-25", "2026-04-24"),
    ("3y", "2023-04-25", "2026-04-24"),
    ("1y", "2025-04-25", "2026-04-24"),
]


def load_base() -> pd.DataFrame:
    df = pd.read_csv(SOURCE_NAV, parse_dates=["date"]).set_index("date").sort_index()
    required = {"return", "return_net", "total_cost", "holding"}
    missing = required.difference(df.columns)
    if missing:
        raise KeyError(f"Missing required columns in {SOURCE_NAV}: {sorted(missing)}")
    return df


def apply_scaled(
    base: pd.DataFrame,
    scale: pd.Series,
    scenario: str,
    financing: bool,
) -> pd.DataFrame:
    scale = scale.reindex(base.index).astype(float).fillna(1.0)
    active = base["holding"].astype(str).ne("cash")
    scale.loc[~active] = 0.0

    gross = pd.to_numeric(base["return"], errors="coerce").fillna(0.0)
    cost = pd.to_numeric(base["total_cost"], errors="coerce").fillna(0.0)
    scale_change_cost = scale.diff().abs().fillna(scale.abs()) * SCALE_CHANGE_COST
    financing_cost = (scale.sub(1.0).clip(lower=0.0) * FINANCING_RATE / TRADING_DAYS) if financing else 0.0

    ret = (1.0 + gross * scale) * (1.0 - cost * scale) * (1.0 - scale_change_cost) - 1.0
    if financing:
        ret = ret - financing_cost

    out = base.copy()
    out["scenario"] = scenario
    out["scale"] = scale
    out["scale_change_cost"] = scale_change_cost
    out["financing_cost"] = financing_cost if isinstance(financing_cost, pd.Series) else 0.0
    out["scaled_return"] = ret
    out["scaled_nav"] = (1.0 + ret).cumprod()
    return out


def build_scenarios(base: pd.DataFrame) -> dict[str, pd.DataFrame]:
    scenarios: dict[str, pd.DataFrame] = {}
    active = base["holding"].astype(str).ne("cash")
    scenarios["baseline_v1_0_costed"] = base.assign(
        scenario="baseline_v1_0_costed",
        scale=np.where(active, 1.0, 0.0),
        scale_change_cost=0.0,
        financing_cost=0.0,
        scaled_return=pd.to_numeric(base["return_net"], errors="coerce").fillna(0.0),
    )
    scenarios["baseline_v1_0_costed"]["scaled_nav"] = (
        1.0 + scenarios["baseline_v1_0_costed"]["scaled_return"]
    ).cumprod()

    for lev in [1.2, 1.5]:
        scale = pd.Series(lev, index=base.index)
        scenarios[f"fixed_{lev:.1f}x_no_financing"] = apply_scaled(
            base, scale, f"fixed_{lev:.1f}x_no_financing", financing=False
        )
        scenarios[f"fixed_{lev:.1f}x_3pct_financing"] = apply_scaled(
            base, scale, f"fixed_{lev:.1f}x_3pct_financing", financing=True
        )

    realized = pd.to_numeric(base["return"], errors="coerce").fillna(0.0).rolling(VOL_WINDOW).std(ddof=1)
    realized = realized * math.sqrt(TRADING_DAYS)
    for target in TARGET_VOLS:
        scale = (target / realized).clip(lower=0.0, upper=MAX_LEV).shift(1).fillna(1.0)
        name = f"targetvol_{int(target * 100)}_w60_max1p5_no_financing"
        scenarios[name] = apply_scaled(base, scale, name, financing=False)
        name = f"targetvol_{int(target * 100)}_w60_max1p5_3pct_financing"
        scenarios[name] = apply_scaled(base, scale, name, financing=True)
    return scenarios


def metrics(ret: pd.Series) -> dict[str, float]:
    ret = ret.dropna()
    nav = (1.0 + ret).cumprod()
    years = (ret.index[-1] - ret.index[0]).days / 365.25
    vol = ret.std(ddof=1) * math.sqrt(TRADING_DAYS)
    cagr = nav.iloc[-1] ** (1.0 / years) - 1.0
    dd = (nav / nav.cummax() - 1.0).min()
    return {
        "annual": float(cagr),
        "vol": float(vol),
        "sharpe": float(ret.mean() / ret.std(ddof=1) * math.sqrt(TRADING_DAYS)),
        "max_dd": float(dd),
        "total_return": float(nav.iloc[-1] - 1.0),
    }


def summarize(scenarios: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for name, df in scenarios.items():
        for window, start, end in WINDOWS:
            part = df.loc[start:end] if start else df
            active_scale = part.loc[part["scale"] > 0, "scale"]
            row = {
                "scenario": name,
                "window": window,
                "start": str(part.index[0].date()),
                "end": str(part.index[-1].date()),
                "days": int(len(part)),
                "avg_active_scale": float(active_scale.mean()) if len(active_scale) else np.nan,
                "max_active_scale": float(active_scale.max()) if len(active_scale) else np.nan,
                "pct_active_days_above_1x": float((active_scale > 1.0 + 1e-12).mean()) if len(active_scale) else 0.0,
                "scale_change_cost_sum": float(part["scale_change_cost"].sum()),
                "financing_cost_sum": float(part["financing_cost"].sum()),
            }
            row.update(metrics(part["scaled_return"]))
            rows.append(row)
    return pd.DataFrame(rows)


def fmt_pct(value: float) -> str:
    return "nan" if pd.isna(value) else f"{value:.2%}"


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    base = load_base()
    scenarios = build_scenarios(base)
    summary = summarize(scenarios)

    summary.to_csv(OUT_DIR / "summary.csv", index=False, encoding="utf-8")
    nav = pd.DataFrame({name: frame["scaled_nav"] for name, frame in scenarios.items()})
    nav.to_csv(OUT_DIR / "nav.csv", index=True, encoding="utf-8")
    meta = {
        "source_nav": str(SOURCE_NAV),
        "source_start": str(base.index.min().date()),
        "source_end": str(base.index.max().date()),
        "baseline": "v1.0 costed Top100 microcap long / CSI1000 short strategy",
        "vol_window": VOL_WINDOW,
        "target_vols": TARGET_VOLS,
        "max_leverage": MAX_LEV,
        "scale_change_cost": SCALE_CHANGE_COST,
        "financing_rate": FINANCING_RATE,
    }
    (OUT_DIR / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    selected = [
        "baseline_v1_0_costed",
        "fixed_1.5x_3pct_financing",
        "targetvol_15_w60_max1p5_3pct_financing",
        "targetvol_20_w60_max1p5_3pct_financing",
        "targetvol_25_w60_max1p5_3pct_financing",
    ]
    lines = [
        "# Microcap v1.0 Vol-Scaling / Leverage Study",
        "",
        f"- Source: `{SOURCE_NAV.relative_to(ROOT)}`.",
        "- Baseline is the refreshed v1.0 costed strategy, not v1.4.",
        "- Scaling is a research overlay: gross strategy return and existing trade cost scale with exposure; cash days remain cash.",
        "- Financing sensitivity uses 3% annualized cost on exposure above 1.0x.",
        "",
        "| Scenario | Window | Annual | Vol | Sharpe | Max DD | Avg active scale |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for _, row in summary.loc[summary["scenario"].isin(selected)].iterrows():
        lines.append(
            "| {scenario} | {window} | {annual} | {vol} | {sharpe:.2f} | {max_dd} | {scale:.2f}x |".format(
                scenario=row["scenario"],
                window=row["window"],
                annual=fmt_pct(row["annual"]),
                vol=fmt_pct(row["vol"]),
                sharpe=row["sharpe"],
                max_dd=fmt_pct(row["max_dd"]),
                scale=row["avg_active_scale"],
            )
        )
    (OUT_DIR / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
