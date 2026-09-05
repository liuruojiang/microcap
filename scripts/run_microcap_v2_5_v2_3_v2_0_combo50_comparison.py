from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime
from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import run_microcap_v2_3_v2_5_combo50_comparison as common


OUTPUT_DIR = ROOT / "outputs"
RUN_FOLDER = (
    ROOT
    / "quant_param_scan_runs"
    / "20260905_microcap_top100_v25_v23_v20_combo50_comparison"
)

NAV_PATHS = {
    "v2_0": OUTPUT_DIR / "microcap_top100_mom16_plain_fixed1_v2_0_costed_nav.csv",
    "v2_3": OUTPUT_DIR
    / "microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv",
    "v2_5": OUTPUT_DIR
    / "microcap_top100_mom16_lb20_hl3_entry0_exit0_no_targetvol_v2_5_costed_nav.csv",
}

EXPECTED_IDENTITIES = {
    "v2_0": ("2.0", "plain_mom16_fixed1_20260904"),
    "v2_3": ("2.3", "plain_lb25_hl2p5_r2off_vol10_26_20_20260904"),
    "v2_5": ("2.5", "plain_lb20_hl3_entry0_exit0_20260905"),
}

WINDOWS: tuple[tuple[str, int | None], ...] = (
    ("full", None),
    ("last_10y", 10),
    ("last_5y", 5),
    ("last_3y", 3),
    ("last_1y", 1),
)
TRADING_DAYS = 244


def _load_stream(label: str, path: Path) -> pd.DataFrame:
    frame = common._load_costed_nav(path, label)
    if not frame.index.is_unique or not frame.index.is_monotonic_increasing:
        raise RuntimeError(f"{label} dates must be unique and ordered")
    returns = pd.to_numeric(frame["return_net"], errors="coerce")
    if returns.isna().any() or not np.isfinite(returns.to_numpy()).all():
        raise RuntimeError(f"{label} return_net contains missing or non-finite values")
    expected_version, expected_revision = EXPECTED_IDENTITIES[label]
    for column, expected in (
        ("version", expected_version),
        ("strategy_revision", expected_revision),
    ):
        if column not in frame.columns:
            raise RuntimeError(f"{label} missing identity column {column}")
        actual = set(frame[column].dropna().astype(str).unique())
        if actual != {expected}:
            raise RuntimeError(
                f"{label} identity mismatch for {column}: expected {expected!r}, got {sorted(actual)!r}"
            )
    rebuilt = (1.0 + returns).cumprod()
    saved = pd.to_numeric(frame["nav_net"], errors="coerce")
    nav_error = float((rebuilt - saved).abs().max())
    if not math.isfinite(nav_error) or nav_error > 1e-10:
        raise RuntimeError(f"{label} nav parity failed: max error {nav_error}")
    return frame


def _metrics(
    returns: pd.Series,
    *,
    candidate: str,
    segment: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> dict[str, object]:
    part = returns.loc[(returns.index >= start) & (returns.index <= end)].astype(float)
    if part.empty:
        return {
            "candidate": candidate,
            "segment": segment,
            "start": str(start.date()),
            "end": str(end.date()),
            "rows": 0,
            "ann_return": np.nan,
            "max_dd": np.nan,
            "ann_vol": np.nan,
            "sharpe_repo": np.nan,
        }
    nav = (1.0 + part).cumprod()
    elapsed_years = (part.index[-1] - part.index[0]).days / 365.25
    ann_return = float(nav.iloc[-1] ** (1.0 / elapsed_years) - 1.0) if elapsed_years > 0 else 0.0
    ann_vol = float(part.std(ddof=1) * math.sqrt(TRADING_DAYS))
    # Include the initial unit of capital so a loss on the first included day counts.
    running_peak = np.maximum.accumulate(np.r_[1.0, nav.to_numpy(dtype=float)])
    drawdown = np.r_[1.0, nav.to_numpy(dtype=float)] / running_peak - 1.0
    return {
        "candidate": candidate,
        "segment": segment,
        "start": str(part.index[0].date()),
        "end": str(part.index[-1].date()),
        "rows": int(len(part)),
        "ann_return": ann_return,
        "max_dd": float(drawdown.min()),
        "ann_vol": ann_vol,
        "sharpe_repo": float(ann_return / ann_vol) if ann_vol > 0 else np.nan,
        "final_nav": float(nav.iloc[-1]),
        "total_return": float(nav.iloc[-1] - 1.0),
    }


def _build_daily(streams: dict[str, pd.DataFrame]) -> pd.DataFrame:
    common_index = streams["v2_5"].index
    for label in ("v2_3", "v2_0"):
        common_index = common_index.intersection(streams[label].index)
    common_index = common_index.sort_values()
    if common_index.empty:
        raise RuntimeError("v2.0, v2.3, and v2.5 have no common dates")
    out = pd.DataFrame(index=common_index)
    for label, frame in streams.items():
        aligned = frame.loc[common_index]
        out[f"{label}_return_net"] = pd.to_numeric(aligned["return_net"], errors="raise")
        out[f"{label}_holding"] = aligned["holding"].astype(str)
        out[f"{label}_rebased_nav"] = (1.0 + out[f"{label}_return_net"]).cumprod()
    out["combo_v2_5_v2_3_return_net"] = 0.5 * (
        out["v2_5_return_net"] + out["v2_3_return_net"]
    )
    out["combo_v2_5_v2_0_return_net"] = 0.5 * (
        out["v2_5_return_net"] + out["v2_0_return_net"]
    )
    for combo in ("combo_v2_5_v2_3", "combo_v2_5_v2_0"):
        out[f"{combo}_nav"] = (1.0 + out[f"{combo}_return_net"]).cumprod()
    out.insert(0, "date", out.index)
    return out.reset_index(drop=True)


def _window_metrics(daily: pd.DataFrame) -> pd.DataFrame:
    frame = daily.set_index(pd.DatetimeIndex(pd.to_datetime(daily["date"]))).drop(columns=["date"])
    returns = {
        "v2_5": frame["v2_5_return_net"],
        "v2_3": frame["v2_3_return_net"],
        "v2_0": frame["v2_0_return_net"],
        "combo_v2_5_v2_3": frame["combo_v2_5_v2_3_return_net"],
        "combo_v2_5_v2_0": frame["combo_v2_5_v2_0_return_net"],
    }
    rows: list[dict[str, object]] = []
    end = pd.Timestamp(frame.index[-1])
    for segment, years in WINDOWS:
        start = pd.Timestamp(frame.index[0]) if years is None else max(
            pd.Timestamp(frame.index[0]), end - pd.DateOffset(years=years)
        )
        for candidate, series in returns.items():
            rows.append(
                _metrics(
                    series,
                    candidate=candidate,
                    segment=segment,
                    start=start,
                    end=end,
                )
            )
    return pd.DataFrame(rows)


def _comparison(metrics: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    pair_defs = (
        ("combo_v2_5_v2_3", "v2_3"),
        ("combo_v2_5_v2_0", "v2_0"),
    )
    for segment, group in metrics.groupby("segment", sort=False):
        lookup = group.set_index("candidate")
        for combo, partner in pair_defs:
            combo_row = lookup.loc[combo]
            v25_row = lookup.loc["v2_5"]
            partner_row = lookup.loc[partner]
            rows.append(
                {
                    "segment": segment,
                    "combo": combo,
                    "partner": partner,
                    "start": combo_row["start"],
                    "end": combo_row["end"],
                    "rows": int(combo_row["rows"]),
                    "combo_ann_return": float(combo_row["ann_return"]),
                    "combo_max_dd": float(combo_row["max_dd"]),
                    "combo_ann_vol": float(combo_row["ann_vol"]),
                    "combo_sharpe_repo": float(combo_row["sharpe_repo"]),
                    "v2_5_ann_return": float(v25_row["ann_return"]),
                    "v2_5_max_dd": float(v25_row["max_dd"]),
                    "combo_minus_v2_5_ann_return_pp": 100.0
                    * (float(combo_row["ann_return"]) - float(v25_row["ann_return"])),
                    "combo_minus_v2_5_max_dd_improvement_pp": 100.0
                    * (float(combo_row["max_dd"]) - float(v25_row["max_dd"])),
                    "partner_ann_return": float(partner_row["ann_return"]),
                    "partner_max_dd": float(partner_row["max_dd"]),
                    "combo_minus_partner_ann_return_pp": 100.0
                    * (float(combo_row["ann_return"]) - float(partner_row["ann_return"])),
                    "combo_minus_partner_max_dd_improvement_pp": 100.0
                    * (float(combo_row["max_dd"]) - float(partner_row["max_dd"])),
                }
            )
    return pd.DataFrame(rows)


def _correlations(daily: pd.DataFrame) -> pd.DataFrame:
    cols = ["v2_5_return_net", "v2_3_return_net", "v2_0_return_net"]
    corr = daily[cols].corr()
    return corr.rename_axis("stream").reset_index()


def _write_chart(daily: pd.DataFrame) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(12, 9), sharex=True)
    dates = pd.to_datetime(daily["date"])
    pairs = (
        (axes[0], "v2.5 + v2.3", "v2_3_rebased_nav", "combo_v2_5_v2_3_nav"),
        (axes[1], "v2.5 + v2.0", "v2_0_rebased_nav", "combo_v2_5_v2_0_nav"),
    )
    for ax, title, partner_nav, combo_nav in pairs:
        ax.plot(dates, daily["v2_5_rebased_nav"], label="v2.5", linewidth=1.5)
        ax.plot(dates, daily[partner_nav], label=title[-4:], linewidth=1.3)
        ax.plot(dates, daily[combo_nav], label=f"50/50 {title}", linewidth=2.0)
        ax.set_yscale("log")
        ax.set_title(title)
        ax.grid(alpha=0.25)
        ax.legend()
    fig.tight_layout()
    fig.savefig(RUN_FOLDER / "nav_comparison.png", dpi=160)
    plt.close(fig)


def main(*, official_latest_close_date: str) -> None:
    freshness = common.validate_formal_freshness(
        expected_latest_date=official_latest_close_date
    )
    streams = {label: _load_stream(label, path) for label, path in NAV_PATHS.items()}
    daily = _build_daily(streams)
    metrics = _window_metrics(daily)
    comparison = _comparison(metrics)
    correlations = _correlations(daily)

    expected_end = str(freshness["expected_latest_date"])
    actual_end = str(pd.Timestamp(daily["date"].iloc[-1]).date())
    if actual_end != expected_end:
        raise RuntimeError(f"common combo end date {actual_end} != official latest {expected_end}")

    combo_checks = {}
    for combo, left, right in (
        ("combo_v2_5_v2_3", "v2_5", "v2_3"),
        ("combo_v2_5_v2_0", "v2_5", "v2_0"),
    ):
        expected = 0.5 * (daily[f"{left}_return_net"] + daily[f"{right}_return_net"])
        return_error = float((daily[f"{combo}_return_net"] - expected).abs().max())
        rebuilt_nav = (1.0 + daily[f"{combo}_return_net"]).cumprod()
        nav_error = float((daily[f"{combo}_nav"] - rebuilt_nav).abs().max())
        combo_checks[combo] = {
            "return_parity_max_abs_error": return_error,
            "nav_parity_max_abs_error": nav_error,
        }
        if return_error > 1e-15 or nav_error > 1e-12:
            raise RuntimeError(f"{combo} parity failed")

    RUN_FOLDER.mkdir(parents=True, exist_ok=True)
    daily.to_csv(RUN_FOLDER / "daily_returns_and_nav.csv", index=False, encoding="utf-8")
    metrics.to_csv(RUN_FOLDER / "window_metrics_long.csv", index=False, encoding="utf-8")
    comparison.to_csv(RUN_FOLDER / "combo_comparison.csv", index=False, encoding="utf-8")
    correlations.to_csv(RUN_FOLDER / "return_correlations.csv", index=False, encoding="utf-8")
    _write_chart(daily)

    meta = {
        "run_id": RUN_FOLDER.name,
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "phase": "complete",
        "entrypoint": "scripts/run_microcap_v2_5_v2_3_v2_0_combo50_comparison.py",
        "command": (
            "python -X utf8 scripts/run_microcap_v2_5_v2_3_v2_0_combo50_comparison.py "
            f"--official-latest-close-date {expected_end}"
        ),
        "data": {
            "common_start": str(pd.Timestamp(daily["date"].iloc[0]).date()),
            "common_end": actual_end,
            "common_rows": int(len(daily)),
            "freshness_proof": freshness,
            "source_paths": {key: str(value) for key, value in NAV_PATHS.items()},
            "identities": {
                key: {"version": value[0], "strategy_revision": value[1]}
                for key, value in EXPECTED_IDENTITIES.items()
            },
        },
        "portfolio_assumption": {
            "weights": "50/50",
            "method": "daily arithmetic average of official costed sleeve returns",
            "sleeve_costs": "included",
            "additional_portfolio_rebalance_cost": 0.0,
            "initial_capital_included_in_max_drawdown": True,
        },
        "integrity": combo_checks,
    }
    (RUN_FOLDER / "scan_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    lines = [
        "# 新版 v2.5 分别与 v2.3 / v2.0 的 50:50 组合比较",
        "",
        f"- 共同样本：{meta['data']['common_start']} 至 {actual_end}，{len(daily)} 行。",
        "- 组合口径：两条正式费后日收益每日各占50%；不另计组合层调仓成本。",
        "- 最大回撤：计入初始本金1.0。",
        "",
        comparison.to_markdown(index=False),
        "",
        "## 收益相关性",
        "",
        correlations.to_markdown(index=False),
    ]
    (RUN_FOLDER / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {RUN_FOLDER}")
    print(f"rows={len(daily)} start={meta['data']['common_start']} end={actual_end}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--official-latest-close-date", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    main(official_latest_close_date=args.official_latest_close_date)
