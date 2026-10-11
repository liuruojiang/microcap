"""Normalize existing L6 real-run CSVs; does not rerun strategy candidates."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

RUN = Path(__file__).resolve().parent
CORE = RUN.parents[1]
SOURCE = CORE / "scan_summary.csv"
SOURCE_META = CORE / "meta.json"
DAILY = CORE / "daily"
WINDOWS = ("full", "last_10y", "last_5y", "last_3y", "last_1y")


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def main() -> None:
    source = pd.read_csv(SOURCE, encoding="utf-8-sig")
    base_meta = json.loads(SOURCE_META.read_text(encoding="utf-8"))
    meta_path = RUN / "scan_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert len(source) == 70 and source.candidate.nunique() == 14
    assert set(source.window) == set(WINDOWS)
    source_hashes = {str(SOURCE): sha(SOURCE), str(SOURCE_META): sha(SOURCE_META)}
    source_rows = []
    for candidate, group in source.groupby("candidate", sort=False):
        assert tuple(group.window) == WINDOWS, candidate
        path = DAILY / f"{candidate}.csv.gz"
        source_hashes[str(path)] = sha(path)
        daily = pd.read_csv(path, parse_dates=["date"]).set_index("date")
        assert len(daily) == 3988 and str(daily.index.max().date()) == "2026-09-30"
        settings = json.loads(group.setting.iloc[0])
        parameters = dict(base_meta["formal_tuple"])
        parameters.update(settings)
        for row in group.itertuples():
            part = daily.loc[row.start:row.end, "return_net"].astype(float)
            assert len(part) == int(row.days)
            annual = float(row.annual_pct) / 100.0
            max_dd = float(row.max_drawdown_pct) / 100.0
            nav = (1.0 + part).cumprod()
            independently_normalized_annual = float(nav.iloc[-1] ** (244.0 / len(part)) - 1.0)
            independently_normalized_dd = float((nav / nav.cummax().clip(lower=1.0) - 1.0).min())
            assert abs(annual - independently_normalized_annual) < 1e-12, (candidate, row.window)
            assert abs(max_dd - independently_normalized_dd) < 1e-12, (candidate, row.window)
            vol = float(part.std(ddof=1) * np.sqrt(244.0))
            sharpe = annual / vol if vol > 0 else 0.0
            source_rows.append({
                "candidate": candidate, "parameter_group": row.parameter_group,
                **parameters, "segment": row.window, "start": row.start, "end": row.end,
                "rows": int(row.days), "ann_return": annual, "ann_vol": vol,
                "sharpe_repo": sharpe, "max_dd": max_dd,
                "holding_days": int(row.held_days), "holding_day_ratio": float(row.held_day_exposure_ratio),
                "cost_total": float(row.sum_daily_cost_rates),
                "transition_events": int(row.transitions),
                "exit_triggers": int(row.exit_triggers),
                "blocked_entries": int(row.blocked_entries),
                "reentries": int(row.reentries),
                "signal_diff_days_vs_formal": int(row.signal_diff_days_vs_formal),
                "ann_return_delta_pp_vs_formal": float(row.annual_delta_pp_vs_formal),
                "dd_improvement_pp_vs_formal": float(row.drawdown_improvement_pp_vs_formal),
            })
    long = pd.DataFrame(source_rows)
    long.to_csv(RUN / "scan_summary.csv", index=False, encoding="utf-8-sig")
    params = ["parameter_group", *base_meta["formal_tuple"].keys()]
    wide = long.pivot(index=["candidate", *params], columns="segment",
                      values=["ann_return", "max_dd", "sharpe_repo", "ann_vol"])
    wide.columns = [f"{metric}_{window}" for metric, window in wide.columns]
    wide = wide.reset_index()
    wide.to_csv(RUN / "window_metrics.csv", index=False, encoding="utf-8-sig")
    assert len(long) == 70 and len(wide) == 14
    assert set(long.candidate) == set(wide.candidate)
    meta.update({
        "scan_type": "normalized_existing_one_axis_replay",
        "baseline": base_meta["formal_tuple"],
        "candidate_grid": base_meta["candidate_grid"],
        "data_snapshot": {
            "first_date": base_meta["date_range"][0], "last_date": base_meta["date_range"][1],
            "rows": base_meta["rows"], "inputs": base_meta["input_readback"],
            "source_and_input_sha256": base_meta["source_and_input_sha256"],
            "basis": "frozen old proxy, diagnostic only; formal historical accuracy N/A",
        },
        "cost_model": {"description": base_meta["cost_and_execution"],
                       "cost_total_semantics": "sum of daily cost rates, not currency or percentage of initial capital"},
        "source_hashes": source_hashes,
        "normalization": {
            "source": "core_executor/scan_summary.csv plus all 14 real daily/*.csv.gz streams",
            "strategy_rerun": False,
            "ann_return": "annual_pct / 100, independently checked from daily NAV with 244/number_of_returns",
            "max_dd": "max_drawdown_pct / 100, independently checked from daily NAV with initial capital high-water mark 1",
            "ann_vol": "daily return sample std(ddof=1) * sqrt(244)",
            "sharpe_repo": "ann_return / ann_vol when positive volatility; otherwise 0",
            "window_mapping": {w: w for w in WINDOWS},
        },
        "parity_check": "Core formal daily replay vs frozen official v2.3: return max 9.99e-17, NAV max 3.55e-15; normalized annual/DD from daily checked 1e-12 for every candidate/window",
        "warnings": ["Old proxy/PIT/ST/corporate-action/calendar lineage not certified", "L5 common-cash account and real futures execution incomplete", "No production promotion", "No out-of-sample validation"],
    })
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    with (RUN / "command_log.txt").open("a", encoding="utf-8") as f:
        f.write("\nNormalized original core_executor 14-candidate / 70-window real-run outputs; no strategy rerun.\n")
        f.write("python -X utf8 research_reports/20261003_l6_diagnostic/core_executor/standardized/20261003_v23_core_one_axis/normalize_existing.py\n")
    print(json.dumps({"candidates": len(wide), "window_rows": len(long), "strategy_rerun": False}, ensure_ascii=False))


if __name__ == "__main__":
    main()
