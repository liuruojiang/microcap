from __future__ import annotations

import json
import math
import subprocess
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SOURCE_RUN = ROOT / "quant_param_scan_runs" / "20260602_microcap_top100_v2_3_v2_5_target_vol_overlay_target_vol_maxlev1"
RUN_FOLDER = ROOT / "quant_param_scan_runs" / "20260602_microcap_top100_v2_3_v2_5_combo_overlay_equal_sleeve_leverage"
SOURCE_DAILY = SOURCE_RUN / "daily_results.csv"
SOURCE_META = SOURCE_RUN / "scan_meta.json"
SOURCE_CANDIDATES = {
    "v2.3": "v2.3_tv26_maxlev1",
    "v2.5": "v2.5_tv26_maxlev1",
}
TRADING_DAYS = 244
FINANCING_RATE = 0.03
WINDOWS = {
    "full": None,
    "last_10y": pd.DateOffset(years=10),
    "last_5y": pd.DateOffset(years=5),
    "last_3y": pd.DateOffset(years=3),
    "last_1y": pd.DateOffset(years=1),
}
CANDIDATES = [
    {
        "candidate": "v2.3_tv26_maxlev1_solo_100",
        "w_v23": 1.0,
        "w_v25": 0.0,
        "description": "v2.3 no-leverage sleeve only",
    },
    {
        "candidate": "v2.5_tv26_maxlev1_solo_100",
        "w_v23": 0.0,
        "w_v25": 1.0,
        "description": "v2.5 no-leverage sleeve only",
    },
    {
        "candidate": "combo_50_50_gross100",
        "w_v23": 0.5,
        "w_v25": 0.5,
        "description": "unlevered 50/50 reference",
    },
    {
        "candidate": "combo_75_75_gross150",
        "w_v23": 0.75,
        "w_v25": 0.75,
        "description": "capital 100, each sleeve allocated 75",
    },
    {
        "candidate": "combo_100_100_gross200",
        "w_v23": 1.0,
        "w_v25": 1.0,
        "description": "capital 100, each sleeve allocated 100",
    },
]


def _git(args: list[str]) -> str:
    try:
        return subprocess.check_output(["git", *args], cwd=ROOT, text=True, stderr=subprocess.STDOUT).strip()
    except Exception as exc:  # noqa: BLE001
        return f"git_error:{exc}"


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        value = float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def _load_source_returns() -> pd.DataFrame:
    if not SOURCE_DAILY.exists():
        raise FileNotFoundError(SOURCE_DAILY)
    df = pd.read_csv(SOURCE_DAILY, parse_dates=["date"])
    required = {"date", "scan_version", "scan_target_vol", "scan_max_leverage", "return_net"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"source daily missing columns: {sorted(missing)}")
    frames = []
    for version, candidate in SOURCE_CANDIDATES.items():
        sub = df.loc[
            df["scan_version"].eq(version)
            & np.isclose(pd.to_numeric(df["scan_target_vol"], errors="coerce"), 0.26)
            & np.isclose(pd.to_numeric(df["scan_max_leverage"], errors="coerce"), 1.0)
        ].copy()
        if sub.empty:
            raise RuntimeError(f"missing source candidate {candidate}")
        sub = sub.sort_values("date").drop_duplicates("date", keep="last")
        sub = sub[["date", "return_net"]].rename(columns={"return_net": f"return_{version.replace('.', '')}"})
        frames.append(sub)
    out = frames[0].merge(frames[1], on="date", how="inner").sort_values("date")
    if out.empty:
        raise RuntimeError("no common dates between v2.3 and v2.5 source streams")
    return out


def _window_bounds(index: pd.DatetimeIndex) -> dict[str, tuple[pd.Timestamp, pd.Timestamp]]:
    start = pd.Timestamp(index.min())
    end = pd.Timestamp(index.max())
    bounds = {"full": (start, end)}
    for name, offset in WINDOWS.items():
        if offset is not None:
            bounds[name] = (max(start, end - offset), end)
    return bounds


def _metrics(df: pd.DataFrame, window_name: str, start: pd.Timestamp, end: pd.Timestamp) -> dict[str, Any]:
    sub = df.loc[(df["date"] >= start) & (df["date"] <= end)].copy()
    ret = pd.to_numeric(sub["return_net"], errors="coerce").dropna().astype(float)
    if ret.empty:
        raise RuntimeError(f"empty return series for {window_name}")
    dates = pd.DatetimeIndex(sub.loc[ret.index, "date"])
    nav = (1.0 + ret).cumprod()
    span_years = (dates[-1] - dates[0]).days / 365.25
    ann_return = nav.iloc[-1] ** (1.0 / span_years) - 1.0 if span_years > 0 and nav.iloc[-1] > 0 else np.nan
    ann_vol = ret.std(ddof=1) * math.sqrt(TRADING_DAYS) if len(ret) > 1 else 0.0
    sharpe = ann_return / ann_vol if ann_vol > 0 else np.nan
    drawdown = nav / nav.cummax() - 1.0
    return {
        "segment": window_name,
        "start": str(dates[0].date()),
        "end": str(dates[-1].date()),
        "rows": int(len(ret)),
        "ann_return": float(ann_return),
        "ann_vol": float(ann_vol),
        "sharpe_repo": float(sharpe),
        "max_dd": float(drawdown.min()),
        "calmar": float(ann_return / abs(drawdown.min())) if drawdown.min() < 0 else np.nan,
        "final_nav": float(nav.iloc[-1]),
        "sum_combo_financing_cost": float(pd.to_numeric(sub["combo_financing_cost"], errors="coerce").fillna(0.0).sum()),
    }


def main() -> None:
    RUN_FOLDER.mkdir(parents=True, exist_ok=True)
    source = _load_source_returns()
    source["return_v23"] = pd.to_numeric(source["return_v23"], errors="coerce").fillna(0.0)
    source["return_v25"] = pd.to_numeric(source["return_v25"], errors="coerce").fillna(0.0)

    daily_rows: list[pd.DataFrame] = []
    summary_rows: list[dict[str, Any]] = []
    bounds = _window_bounds(pd.DatetimeIndex(source["date"]))
    for spec in CANDIDATES:
        candidate = str(spec["candidate"])
        w23 = float(spec["w_v23"])
        w25 = float(spec["w_v25"])
        gross_weight = w23 + w25
        financing_cost = max(gross_weight - 1.0, 0.0) * FINANCING_RATE / TRADING_DAYS
        out = source.copy()
        out["candidate"] = candidate
        out["w_v23"] = w23
        out["w_v25"] = w25
        out["gross_weight"] = gross_weight
        out["combo_financing_cost"] = financing_cost
        out["return_before_combo_financing"] = w23 * out["return_v23"] + w25 * out["return_v25"]
        out["return_net"] = out["return_before_combo_financing"] - financing_cost
        out["nav_net"] = (1.0 + out["return_net"]).cumprod()
        daily_rows.append(out)
        for segment, (start, end) in bounds.items():
            summary_rows.append(
                {
                    "candidate": candidate,
                    "w_v23": w23,
                    "w_v25": w25,
                    "gross_weight": gross_weight,
                    "combo_financing_rate": FINANCING_RATE,
                    "description": spec["description"],
                    **_metrics(out, segment, start, end),
                }
            )

    scan_summary = pd.DataFrame(summary_rows)
    full = scan_summary.loc[scan_summary["segment"].eq("full")].copy()
    full["rank_sharpe"] = full["sharpe_repo"].rank(ascending=False, method="min")
    full["rank_ann_return"] = full["ann_return"].rank(ascending=False, method="min")
    full["rank_calmar"] = full["calmar"].rank(ascending=False, method="min")
    scan_summary = scan_summary.merge(
        full[["candidate", "rank_sharpe", "rank_ann_return", "rank_calmar"]],
        on="candidate",
        how="left",
    )

    wide_rows: list[dict[str, Any]] = []
    for candidate, group in scan_summary.groupby("candidate", sort=False):
        first = group.iloc[0]
        row: dict[str, Any] = {
            "candidate": candidate,
            "w_v23": first["w_v23"],
            "w_v25": first["w_v25"],
            "gross_weight": first["gross_weight"],
            "combo_financing_rate": first["combo_financing_rate"],
            "rank_sharpe": first["rank_sharpe"],
            "rank_ann_return": first["rank_ann_return"],
            "rank_calmar": first["rank_calmar"],
            "description": first["description"],
        }
        for _, item in group.iterrows():
            segment = str(item["segment"])
            for metric in [
                "ann_return",
                "ann_vol",
                "sharpe_repo",
                "max_dd",
                "calmar",
                "final_nav",
                "sum_combo_financing_cost",
            ]:
                row[f"{metric}_{segment}"] = item[metric]
        wide_rows.append(row)
    window_metrics = pd.DataFrame(wide_rows)
    daily = pd.concat(daily_rows, ignore_index=True)

    scan_summary.to_csv(RUN_FOLDER / "scan_summary.csv", index=False, encoding="utf-8-sig")
    window_metrics.to_csv(RUN_FOLDER / "window_metrics.csv", index=False, encoding="utf-8-sig")
    daily.to_csv(RUN_FOLDER / "daily_results.csv", index=False, encoding="utf-8-sig")

    source_meta = json.loads(SOURCE_META.read_text(encoding="utf-8")) if SOURCE_META.exists() else {}
    meta_path = RUN_FOLDER / "scan_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
    meta.update(
        {
            "scan_type": "v2.3_v2.5_no_leverage_sleeve_combo_with_portfolio_leverage",
            "source_run": str(SOURCE_RUN),
            "source_candidates": SOURCE_CANDIDATES,
            "candidate_grid": CANDIDATES,
            "portfolio_financing_model": {
                "return_column": "return_net",
                "bottom_sleeve_costs": "source v2.3/v2.5 no-leverage return_net already includes each version's current trading, scale-change, cash-yield, and financing conventions",
                "combo_extra_financing": "max(w_v23 + w_v25 - 1, 0) * 3% / 244 deducted daily",
                "financing_rate": FINANCING_RATE,
                "trading_days": TRADING_DAYS,
                "extra_rebalance_cost_between_sleeves": 0.0,
            },
            "cost_model": {
                "return_column": "return_net",
                "bottom_sleeve_costs": "source v2.3/v2.5 no-leverage return_net already includes each version's current trading, scale-change, cash-yield, and financing conventions",
                "combo_financing_rate": FINANCING_RATE,
                "combo_financing_daily_rule": "max(w_v23 + w_v25 - 1, 0) * combo_financing_rate / trading_days",
                "trading_days": TRADING_DAYS,
                "extra_rebalance_cost_between_sleeves": 0.0,
            },
            "data_snapshot": {
                "common_rows": int(len(source)),
                "start_date": str(source["date"].min().date()),
                "end_date": str(source["date"].max().date()),
                "source_data_snapshot": source_meta.get("data_snapshot", {}),
            },
            "outputs": {
                **meta.get("outputs", {}),
                "record": str(RUN_FOLDER / "record.md"),
                "scan_summary": str(RUN_FOLDER / "scan_summary.csv"),
                "window_metrics": str(RUN_FOLDER / "window_metrics.csv"),
                "daily_results": str(RUN_FOLDER / "daily_results.csv"),
                "scan_meta": str(RUN_FOLDER / "scan_meta.json"),
                "command_log": str(RUN_FOLDER / "command_log.txt"),
            },
            "ranking_rule": "compare full-sample return/risk for equal-sleeve portfolio leverage candidates; windows: full, last_10y, last_5y, last_3y, last_1y",
            "source_change_rule": "rerun if the source maxlev1 scan, v2.3/v2.5 cost model, financing rate, trading-day count, or base data refresh changes",
            "git_status_after": _git(["status", "--short"]),
        }
    )
    _write_json(meta_path, meta)

    with (RUN_FOLDER / "command_log.txt").open("a", encoding="utf-8") as fh:
        fh.write("\npython scripts/run_microcap_v2_3_v2_5_maxlev1_combo_leverage_scan.py\n")

    full_table = scan_summary.loc[scan_summary["segment"].eq("full")].sort_values("rank_sharpe")
    record = f"""# v2.3/v2.5 No-Leverage Sleeve Combo Leverage Scan

## Run Metadata

- Run folder: `{RUN_FOLDER.relative_to(ROOT)}`
- Source run: `{SOURCE_RUN.relative_to(ROOT)}`
- Source candidates: `{SOURCE_CANDIDATES["v2.3"]}`, `{SOURCE_CANDIDATES["v2.5"]}`

## Research Question

Use the no-leverage v2.3 and v2.5 sleeves together at portfolio level:

- capital 100, v2.3 100 + v2.5 100 (`gross_weight = 2.0`)
- capital 100, v2.3 75 + v2.5 75 (`gross_weight = 1.5`)

Keep solo sleeves and an unlevered 50/50 mix as references.

## Implementation Anchor

The script reads `{SOURCE_DAILY.relative_to(ROOT)}` and uses only the `return_net` series for `v2.3_tv26_maxlev1` and `v2.5_tv26_maxlev1`.

## Data Snapshot

- Common rows: `{len(source)}`
- Date range: `{source["date"].min().date()}` to `{source["date"].max().date()}`

## Cost and Execution Assumptions

- Bottom sleeve returns use `return_net`, so each version's own trading costs, target-vol scale-change costs, cash-day yield, and version-level financing conventions are already included.
- Portfolio-level financing follows the current v2.3/v2.5 convention: `3%` annual on exposure above `1.0x`, using `{TRADING_DAYS}` trading days.
- Combo daily return: `w_v23 * r_v23 + w_v25 * r_v25 - max(w_v23 + w_v25 - 1, 0) * 3% / {TRADING_DAYS}`.
- No extra rebalance cost is added between the two strategy sleeves.

## Runtime Override Plan

No production runtime override. This is an artifact-level composition of already generated no-leverage sleeve returns.

## Commands

```powershell
python scripts/run_microcap_v2_3_v2_5_maxlev1_combo_leverage_scan.py
```

## Output Files

- `scan_summary.csv`
- `window_metrics.csv`
- `daily_results.csv`
- `scan_meta.json`
- `command_log.txt`

## Full-Sample Results

{full_table[["candidate", "w_v23", "w_v25", "gross_weight", "ann_return", "ann_vol", "sharpe_repo", "max_dd", "calmar", "rank_sharpe", "rank_ann_return"]].to_string(index=False)}

## Window Results

See `window_metrics.csv` for full, 10Y, 5Y, 3Y, and 1Y columns.

## Stability Classification

Research scan using the validated max-leverage-1 source run. Rerun if source returns, base data, or financing assumptions change.

## Decision

Filled by finalizer after review.

## User-Facing Summary

The 75/75 and 100/100 sleeves test portfolio-level leverage on top of no-leverage v2.3/v2.5 source streams, with current-style financing cost applied to the borrowed portion.
"""
    (RUN_FOLDER / "record.md").write_text(record, encoding="utf-8")
    print(f"wrote {RUN_FOLDER}")
    print(full_table[["candidate", "ann_return", "ann_vol", "sharpe_repo", "max_dd", "calmar"]].to_string(index=False))


if __name__ == "__main__":
    main()
