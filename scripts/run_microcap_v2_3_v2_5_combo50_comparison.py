from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import microcap_top100_mom16_biweekly_live_v2_0 as v2_0


OUTPUT_DIR = ROOT / "outputs"
RUN_FOLDER = ROOT / "quant_param_scan_runs" / "20260629_microcap_top100_v2_3_v2_5_combo50_comparison"

V2_3_COSTED_NAV = OUTPUT_DIR / "microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv"
V2_5_COSTED_NAV = OUTPUT_DIR / "microcap_top100_mom16_lb17_hl3_entry46_exit25_no_targetvol_v2_5_costed_nav.csv"
V2_0_COSTED_NAV = OUTPUT_DIR / "microcap_top100_mom16_plain_fixed1_v2_0_costed_nav.csv"
BASE_PANEL = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv"
PROXY_INDEX = OUTPUT_DIR / "wind_microcap_top_100_biweekly_thursday_16y_cached.csv"
PROXY_TURNOVER = OUTPUT_DIR / "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv"
BASE_COSTED_NAV = OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv"

TRADING_DAYS = 244
WINDOWS: tuple[tuple[str, int | None], ...] = (
    ("full", None),
    ("last_10y", 10),
    ("last_5y", 5),
    ("last_3y", 3),
    ("last_1y", 1),
)
FORMAL_DAILY_KEYS = (
    "base_panel_shadow",
    "base_index_csv",
    "base_costed_nav",
    "v2_0_costed_nav",
    "v2_3_costed_nav",
    "v2_5_costed_nav",
)


def _json_default(value: object) -> object:
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if pd.isna(value):
        return None
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _git_output(args: list[str]) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except Exception as exc:
        return f"git unavailable: {exc}"
    return (result.stdout or result.stderr).strip()


def _artifact_state(path: Path, date_columns: tuple[str, ...] = ("date",)) -> dict[str, object]:
    state: dict[str, object] = {
        "path": str(path),
        "exists": path.exists(),
        "size": path.stat().st_size if path.exists() else 0,
        "row_count": 0,
        "first_date": None,
        "latest_date": None,
        "date_column": None,
    }
    if not path.exists() or path.stat().st_size <= 0:
        return state
    try:
        header = pd.read_csv(path, nrows=0)
        date_column = next((col for col in date_columns if col in header.columns), None)
        if date_column is None:
            return state
        df = pd.read_csv(path, usecols=[date_column], parse_dates=[date_column])
    except Exception as exc:
        state["read_error"] = repr(exc)
        return state
    dates = pd.to_datetime(df[date_column], errors="coerce").dropna()
    state["row_count"] = int(len(df))
    state["date_column"] = date_column
    if len(dates):
        state["first_date"] = str(pd.Timestamp(dates.min()).date())
        state["latest_date"] = str(pd.Timestamp(dates.max()).date())
    return state


def _formal_artifact_specs() -> dict[str, tuple[Path, tuple[str, ...]]]:
    return {
        "base_panel_shadow": (BASE_PANEL, ("date",)),
        "base_index_csv": (PROXY_INDEX, ("date",)),
        "base_proxy_turnover": (PROXY_TURNOVER, ("rebalance_date", "date")),
        "base_costed_nav": (BASE_COSTED_NAV, ("date",)),
        "v2_0_costed_nav": (V2_0_COSTED_NAV, ("date",)),
        "v2_3_costed_nav": (V2_3_COSTED_NAV, ("date",)),
        "v2_5_costed_nav": (V2_5_COSTED_NAV, ("date",)),
    }


def validate_formal_freshness(
    artifacts: dict[str, tuple[Path, tuple[str, ...]]] | None = None,
    *,
    expected_latest_date: object | None = None,
) -> dict[str, object]:
    expected = v2_0.overlay_mod._coerce_date_text(expected_latest_date)
    if not expected:
        raise RuntimeError(
            "formal combo50 requires an independent official latest close date produced by the "
            "v2.0 refresh/context path"
        )
    specs = _formal_artifact_specs() if artifacts is None else artifacts
    states: dict[str, dict[str, object]] = {}
    issues: list[str] = []
    for name, (path, date_columns) in specs.items():
        state = _artifact_state(Path(path), tuple(date_columns))
        states[name] = state
        if not bool(state.get("exists")):
            issues.append(f"{name} missing: {path}")
        elif int(state.get("size") or 0) <= 0:
            issues.append(f"{name} is empty: {path}")
        elif state.get("read_error"):
            issues.append(f"{name} unreadable: {state['read_error']}")
        elif int(state.get("row_count") or 0) <= 0:
            issues.append(f"{name} has no rows: {path}")
        elif not state.get("latest_date"):
            issues.append(f"{name} has no valid latest date: {path}")

    if issues:
        raise RuntimeError("formal combo50 freshness validation failed: " + "; ".join(issues))

    panel_path, panel_date_columns = specs["base_panel_shadow"]
    panel_dates = v2_0.overlay_mod._read_artifact_date_index(
        Path(panel_path),
        panel_date_columns[0],
    )
    expected_rebalance = v2_0.overlay_mod._latest_required_rebalance_date(panel_dates, expected)
    if not expected_rebalance:
        raise RuntimeError(
            "formal combo50 freshness validation cannot derive the expected latest biweekly rebalance "
            f"from refreshed panel dates through {expected}"
        )
    try:
        official_proof = v2_0.overlay_mod.validate_top100_freshness_proof(
            states,
            expected_latest_date=expected,
            expected_latest_rebalance_date=expected_rebalance,
            daily_keys=FORMAL_DAILY_KEYS,
        )
    except RuntimeError as exc:
        raise RuntimeError(f"formal combo50 freshness validation failed: {exc}") from exc
    return {
        "expected_latest_date": official_proof["expected_latest_date"],
        "expected_latest_rebalance_date": official_proof["expected_latest_rebalance_date"],
        "artifacts": states,
    }


def _load_costed_nav(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"{label} costed NAV is missing: {path}")
    df = pd.read_csv(path, parse_dates=["date"]).sort_values("date")
    required = {"date", "return_net", "nav_net", "holding", "next_holding"}
    missing = required.difference(df.columns)
    if missing:
        raise RuntimeError(f"{label} costed NAV missing columns: {sorted(missing)}")
    df = df.drop_duplicates("date", keep="last").set_index("date")
    df.index = pd.DatetimeIndex(df.index).normalize()
    return df


def _aligned_daily(v23: pd.DataFrame, v25: pd.DataFrame) -> pd.DataFrame:
    common_index = v23.index.intersection(v25.index).sort_values()
    if common_index.empty:
        raise RuntimeError("v2.3 and v2.5 have no common dates")
    v23 = v23.loc[common_index]
    v25 = v25.loc[common_index]
    combo_ret = (
        pd.to_numeric(v23["return_net"], errors="coerce").fillna(0.0).mul(0.5)
        + pd.to_numeric(v25["return_net"], errors="coerce").fillna(0.0).mul(0.5)
    )
    out = pd.DataFrame(index=common_index)
    for label, frame in (("v2_3", v23), ("v2_5", v25)):
        for col in [
            "return_net",
            "nav_net",
            "holding",
            "next_holding",
            "current_execution_scale",
            "execution_scale",
            "actual_execution_scale",
            "target_vol_enabled",
            "cash_day_yield_enabled",
            "financing_enabled",
        ]:
            if col in frame.columns:
                out[f"{label}_{col}"] = frame[col]
    out["combo50_return_net"] = combo_ret
    out["combo50_nav_net"] = (1.0 + combo_ret).cumprod()
    out["v2_3_aligned_nav_net"] = (1.0 + pd.to_numeric(v23["return_net"], errors="coerce").fillna(0.0)).cumprod()
    out["v2_5_aligned_nav_net"] = (1.0 + pd.to_numeric(v25["return_net"], errors="coerce").fillna(0.0)).cumprod()
    out.insert(0, "date", out.index)
    return out.reset_index(drop=True)


def _window_slice(index: pd.DatetimeIndex, years: int | None) -> tuple[pd.Timestamp, pd.Timestamp]:
    end = pd.Timestamp(index[-1])
    if years is None:
        start = pd.Timestamp(index[0])
    else:
        start = max(pd.Timestamp(index[0]), end - pd.DateOffset(years=int(years)))
    return start, end


def _metrics(ret: pd.Series, candidate: str, line_label: str, window: str, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, object]:
    part = ret.loc[(ret.index >= start) & (ret.index <= end)].dropna().astype(float)
    if part.empty:
        return {
            "candidate": candidate,
            "line_label": line_label,
            "segment": window,
            "start": str(start.date()),
            "end": str(end.date()),
            "rows": 0,
            "ann_return": np.nan,
            "ann_vol": np.nan,
            "sharpe_repo": np.nan,
            "max_dd": np.nan,
            "final_nav": np.nan,
            "total_return": np.nan,
        }
    nav = (1.0 + part).cumprod()
    years = (part.index[-1] - part.index[0]).days / 365.25
    ann_return = nav.iloc[-1] ** (1.0 / years) - 1.0 if years > 0 else 0.0
    ann_vol = part.std(ddof=1) * math.sqrt(TRADING_DAYS)
    drawdown = nav.div(nav.cummax()).sub(1.0)
    trough = drawdown.idxmin()
    peak = nav.loc[:trough].idxmax() if len(nav.loc[:trough]) else pd.NaT
    recovery = pd.NaT
    if pd.notna(peak):
        peak_nav = nav.loc[peak]
        after = nav.loc[trough:]
        recovered = after[after >= peak_nav]
        if len(recovered):
            recovery = recovered.index[0]
    return {
        "candidate": candidate,
        "line_label": line_label,
        "segment": window,
        "start": str(pd.Timestamp(part.index[0]).date()),
        "end": str(pd.Timestamp(part.index[-1]).date()),
        "rows": int(len(part)),
        "ann_return": float(ann_return),
        "ann_vol": float(ann_vol),
        "sharpe_repo": float(ann_return / ann_vol) if ann_vol > 0 else 0.0,
        "max_dd": float(drawdown.min()),
        "final_nav": float(nav.iloc[-1]),
        "total_return": float(nav.iloc[-1] - 1.0),
        "max_dd_peak_date": "" if pd.isna(peak) else str(pd.Timestamp(peak).date()),
        "max_dd_trough_date": "" if pd.isna(trough) else str(pd.Timestamp(trough).date()),
        "max_dd_recovery_date": "" if pd.isna(recovery) else str(pd.Timestamp(recovery).date()),
    }


def _long_window_metrics(daily: pd.DataFrame) -> pd.DataFrame:
    frame = daily.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.set_index("date")
    streams = [
        ("v2_3", "v2.3 official costed", frame["v2_3_return_net"]),
        ("v2_5", "v2.5 official costed", frame["v2_5_return_net"]),
        ("combo50_v2_3_v2_5", "50/50 daily rebalanced combo", frame["combo50_return_net"]),
    ]
    rows: list[dict[str, object]] = []
    for window, years in WINDOWS:
        start, end = _window_slice(frame.index, years)
        for candidate, label, ret in streams:
            rows.append(_metrics(ret, candidate, label, window, start, end))
    return pd.DataFrame(rows)


def _wide_window_metrics(long_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for (candidate, line_label), group in long_df.groupby(["candidate", "line_label"], sort=False):
        row: dict[str, object] = {"candidate": candidate, "line_label": line_label}
        for item in group.itertuples(index=False):
            segment = str(item.segment)
            for col in ["ann_return", "ann_vol", "sharpe_repo", "max_dd", "final_nav", "total_return", "rows"]:
                row[f"{col}_{segment}"] = getattr(item, col)
        rows.append(row)
    return pd.DataFrame(rows)


def _combo_vs_sleeves(long_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for segment, group in long_df.groupby("segment", sort=False):
        by_candidate = {str(row["candidate"]): row for row in group.to_dict(orient="records")}
        combo = by_candidate["combo50_v2_3_v2_5"]
        row: dict[str, object] = {
            "segment": segment,
            "start": combo["start"],
            "end": combo["end"],
            "rows": combo["rows"],
            "combo_ann_return": combo["ann_return"],
            "combo_max_dd": combo["max_dd"],
        }
        for sleeve in ["v2_3", "v2_5"]:
            base = by_candidate[sleeve]
            row[f"{sleeve}_ann_return"] = base["ann_return"]
            row[f"{sleeve}_max_dd"] = base["max_dd"]
            row[f"combo_minus_{sleeve}_ann_return_pp"] = (combo["ann_return"] - base["ann_return"]) * 100.0
            row[f"combo_minus_{sleeve}_max_dd_improvement_pp"] = (base["max_dd"] - combo["max_dd"]) * 100.0
        rows.append(row)
    return pd.DataFrame(rows)


def _state_correlation(daily: pd.DataFrame) -> pd.DataFrame:
    v23_active = daily["v2_3_holding"].astype(str).ne("cash")
    v25_active = daily["v2_5_holding"].astype(str).ne("cash")
    return pd.DataFrame(
        [
            {
                "rows": int(len(daily)),
                "both_active_days": int((v23_active & v25_active).sum()),
                "both_cash_days": int((~v23_active & ~v25_active).sum()),
                "v2_3_only_active_days": int((v23_active & ~v25_active).sum()),
                "v2_5_only_active_days": int((~v23_active & v25_active).sum()),
                "v2_3_active_ratio": float(v23_active.mean()),
                "v2_5_active_ratio": float(v25_active.mean()),
                "daily_return_corr_v23_v25": float(daily["v2_3_return_net"].corr(daily["v2_5_return_net"])),
                "daily_return_corr_combo_v23": float(daily["combo50_return_net"].corr(daily["v2_3_return_net"])),
                "daily_return_corr_combo_v25": float(daily["combo50_return_net"].corr(daily["v2_5_return_net"])),
            }
        ]
    )


def _write_chart(daily: pd.DataFrame, path: Path) -> None:
    plt.figure(figsize=(12, 6))
    plt.plot(daily["date"], daily["v2_3_aligned_nav_net"], label="v2.3")
    plt.plot(daily["date"], daily["v2_5_aligned_nav_net"], label="v2.5")
    plt.plot(daily["date"], daily["combo50_nav_net"], label="combo50")
    plt.grid(alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()


def _write_record(run_folder: Path, long_df: pd.DataFrame, combo_vs: pd.DataFrame, state_corr: pd.DataFrame, meta: dict[str, object]) -> None:
    lines = [
        "# v2.3 / v2.5 50:50 Combo Backtest",
        "",
        "## Decision",
        "",
        "combo50_backtest_complete_for_review",
        "",
        "## Data",
        "",
        f"- v2.3 stream: `{V2_3_COSTED_NAV}`",
        f"- v2.5 stream: `{V2_5_COSTED_NAV}`",
        f"- Common sample: {meta['data_snapshot']['common_start']} to {meta['data_snapshot']['common_end']}, {meta['data_snapshot']['common_rows']} rows.",
        "- Combo assumption: daily close 50/50 rebalance across sleeves, no extra portfolio-level rebalance cost; sleeve costed returns already include their own costs.",
        "",
        "## Window Metrics",
        "",
        long_df.to_markdown(index=False),
        "",
        "## Combo Vs Sleeves",
        "",
        combo_vs.to_markdown(index=False),
        "",
        "## State And Correlation",
        "",
        state_corr.to_markdown(index=False),
        "",
        "## Reproducibility",
        "",
        f"- Command: `{meta['command']}`",
        f"- Generated at: {meta['created_at']}",
    ]
    (run_folder / "record.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(*, expected_latest_date: object | None = None) -> None:
    freshness_proof = validate_formal_freshness(expected_latest_date=expected_latest_date)
    RUN_FOLDER.mkdir(parents=True, exist_ok=True)
    v23 = _load_costed_nav(V2_3_COSTED_NAV, "v2.3")
    v25 = _load_costed_nav(V2_5_COSTED_NAV, "v2.5")
    daily = _aligned_daily(v23, v25)
    daily.to_csv(RUN_FOLDER / "daily_aligned_combo50.csv", index=False, encoding="utf-8")

    long_df = _long_window_metrics(daily)
    wide_df = _wide_window_metrics(long_df)
    combo_vs = _combo_vs_sleeves(long_df)
    state_corr = _state_correlation(daily)
    long_df.to_csv(RUN_FOLDER / "scan_summary.csv", index=False, encoding="utf-8")
    wide_df.to_csv(RUN_FOLDER / "window_metrics.csv", index=False, encoding="utf-8")
    combo_vs.to_csv(RUN_FOLDER / "combo_vs_sleeves.csv", index=False, encoding="utf-8")
    state_corr.to_csv(RUN_FOLDER / "state_correlation_summary.csv", index=False, encoding="utf-8")
    _write_chart(daily, RUN_FOLDER / "nav_comparison.png")

    meta: dict[str, Any] = {
        "run_id": RUN_FOLDER.name,
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "phase": "complete",
        "project": "microcap Top100",
        "strategy": "v2.3 v2.5 50/50 combo comparison",
        "entrypoint": "scripts/run_microcap_v2_3_v2_5_combo50_comparison.py",
        "command": (
            "python scripts/run_microcap_v2_3_v2_5_combo50_comparison.py "
            f"--official-latest-close-date {freshness_proof['expected_latest_date']}"
        ),
        "git_branch": _git_output(["branch", "--show-current"]),
        "git_commit": _git_output(["rev-parse", "HEAD"]),
        "git_status": _git_output(["status", "--short"]),
        "scan_type": "portfolio_combo_comparison",
        "portfolio_assumption": {
            "weights": {"v2_3": 0.5, "v2_5": 0.5},
            "rebalance_frequency": "daily close on common trading dates",
            "additional_combo_rebalance_cost": 0.0,
            "sleeve_costs": "included in each official costed return stream",
        },
        "data_snapshot": {
            "common_start": str(pd.Timestamp(daily["date"].iloc[0]).date()),
            "common_end": str(pd.Timestamp(daily["date"].iloc[-1]).date()),
            "common_rows": int(len(daily)),
            "expected_latest_close_date": freshness_proof["expected_latest_date"],
            "expected_latest_rebalance_date": freshness_proof["expected_latest_rebalance_date"],
            "freshness_proof": freshness_proof["artifacts"],
        },
        "outputs": {
            "daily_aligned_combo50": str(RUN_FOLDER / "daily_aligned_combo50.csv"),
            "scan_summary": str(RUN_FOLDER / "scan_summary.csv"),
            "window_metrics": str(RUN_FOLDER / "window_metrics.csv"),
            "combo_vs_sleeves": str(RUN_FOLDER / "combo_vs_sleeves.csv"),
            "state_correlation_summary": str(RUN_FOLDER / "state_correlation_summary.csv"),
            "nav_comparison": str(RUN_FOLDER / "nav_comparison.png"),
            "record": str(RUN_FOLDER / "record.md"),
        },
    }
    (RUN_FOLDER / "scan_meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2, default=_json_default) + "\n", encoding="utf-8")
    (RUN_FOLDER / "command_log.txt").write_text(
        f"[{meta['created_at']}] python scripts/run_microcap_v2_3_v2_5_combo50_comparison.py\n",
        encoding="utf-8",
    )
    _write_record(RUN_FOLDER, long_df, combo_vs, state_corr, meta)
    print(f"wrote {RUN_FOLDER}")
    print(f"rows={len(daily)} start={meta['data_snapshot']['common_start']} end={meta['data_snapshot']['common_end']}")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the formal v2.3/v2.5 combo50 comparison.")
    parser.add_argument(
        "--official-latest-close-date",
        required=True,
        help="Latest close-confirmed date produced independently by the official v2.0 refresh/context path.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    cli_args = _parse_args()
    sys.exit(main(expected_latest_date=cli_args.official_latest_close_date))
