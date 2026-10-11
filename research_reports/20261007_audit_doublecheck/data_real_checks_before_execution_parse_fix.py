"""Read-only real-input checks after the root agent confirmed same-batch freshness."""
from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
import sys

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as v20


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def inspect_dates(path, date_column):
    frame = pd.read_csv(path)
    dates = pd.to_datetime(frame[date_column], errors="coerce")
    valid = dates.dropna()
    return frame, {
        "path": str(path), "rows": len(frame), "first": str(valid.min().date()),
        "last": str(valid.max().date()), "invalid_dates": int(dates.isna().sum()),
        "duplicate_session_labels": int(dates.dt.normalize().duplicated().sum()),
        "nonmidnight_labels": int(dates.ne(dates.dt.normalize()).sum()), "raw_sha256": sha256(path),
    }


def main():
    destination = Path(__file__).with_name("data_real_checks.json")
    freshness = json.loads(Path(__file__).with_name("baseline_and_freshness.json").read_text(encoding="utf-8"))
    assert freshness["latest_completed_session"] == "2026-09-30"
    assert freshness["fresh_written_artifacts_rechecked_and_unchanged"]
    report = {"scope": "readonly_current_input_diagnostics_no_performance_claim",
              "latest_completed_session": freshness["latest_completed_session"],
              "freshness_evidence": "baseline_and_freshness.json", "artifacts": {},
              "guard_failures_are_isolated_counterexamples_not_observed_current_corruption": True}
    data = {}
    for name, detail in freshness["artifacts"].items():
        path = Path(detail["path"])
        date_column = "rebalance_date" if name == "turnover" else "date"
        frame, result = inspect_dates(path, date_column)
        assert result["raw_sha256"] == detail["raw_sha256"], name
        assert result["rows"] == detail["rows"], name
        assert result["last"] == detail["latest_date"], name
        report["artifacts"][name] = result
        data[name] = frame
    close = v20.base_mod.load_close_df(
        Path(freshness["artifacts"]["panel"]["path"]),
        Path(freshness["artifacts"]["proxy_index"]["path"]),
        max_date=pd.Timestamp(freshness["latest_completed_session"]),
    )
    report["actual_aligned_close"] = {"rows": len(close), "last": str(close.index.max().date()),
                                      "invalid_prices": int((~np.isfinite(close.to_numpy(dtype=float))).sum()),
                                      "duplicate_session_labels": int(close.index.normalize().duplicated().sum())}
    for name in ("base_costed_nav", "v20_nav", "v23_costed_nav", "v25_costed_nav"):
        checked = v20.base_mod.validate_performance_frame(data[name], "return_net", "nav_net", name)
        report["artifacts"][name]["return_nav_guard_passed"] = len(checked) == len(data[name])
    turnover = data["turnover"]
    turnover_dates = pd.to_datetime(turnover["rebalance_date"], errors="coerce")
    execution_dates = pd.to_datetime(turnover["execution_date"], errors="coerce")
    rates = pd.to_numeric(turnover["two_side_cost_rate"], errors="coerce")
    report["turnover_semantics"] = {
        "execution_timing": turnover["execution_timing"].value_counts(dropna=False).to_dict(),
        "explicit_execution_dates": int(execution_dates.notna().sum()),
        "execution_dates_before_signal": int(execution_dates.lt(turnover_dates).sum()),
        "nonmidnight_execution_dates": int((execution_dates.notna() & execution_dates.ne(execution_dates.dt.normalize())).sum()),
        "execution_dates_not_in_proxy": int((execution_dates.notna() & ~execution_dates.isin(pd.to_datetime(data["proxy_index"].date))).sum()),
        "invalid_rates": int((~np.isfinite(rates) | rates.lt(0) | rates.ge(1)).sum()),
        "legacy_without_execution_date": int(execution_dates.isna().sum()),
        "legacy_missing_execution_rows": turnover.loc[execution_dates.isna()].replace({np.nan: None}).to_dict("records"),
    }
    report["cost_prefix_invariance"] = {}
    for name in ("base_costed_nav", "v20_nav", "v23_costed_nav", "v25_costed_nav"):
        index = pd.DatetimeIndex(pd.to_datetime(data[name].date))
        full = v20.cost_mod.map_rebalance_apply_costs(index, turnover)
        errors = {}
        for rows in (100, 500, 1500):
            tail = index[-rows:]
            trimmed = v20.cost_mod.map_rebalance_apply_costs(tail, turnover)
            errors[str(rows)] = float((trimmed - full.reindex(tail)).abs().max())
        report["cost_prefix_invariance"][name] = errors
    namespace = v20.freq_mod.load_security_meta.__globals__
    meta_paths = sorted(Path(namespace["SECURITY_META_DIR"]).glob("*.json"))
    current_st = namespace["load_current_st_name_map"]()
    counts = Counter()
    parse_failures = []
    for path in meta_paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            parse_failures.append({"path": str(path), "error": str(exc)})
            continue
        counts["readable"] += 1
        if str(payload.get("symbol") or "").zfill(6) != path.stem:
            counts["symbol_mismatch"] += 1
        policy = payload.get("st_notice_policy_version")
        counts["missing_policy" if policy is None else "current_policy" if policy == namespace["ST_NOTICE_POLICY_VERSION"] else "obsolete_policy"] += 1
        if str(payload.get("current_st_snapshot_name") or "") != current_st.get(path.stem, ""):
            counts["current_snapshot_mismatch"] += 1
        intervals = payload.get("st_intervals")
        if not isinstance(intervals, list):
            counts["invalid_interval_type"] += 1
            continue
        counts["nonempty_history" if intervals else "empty_history"] += 1
        for interval in intervals:
            if not isinstance(interval, dict):
                counts["invalid_interval_row"] += 1
                continue
            start = pd.to_datetime(interval.get("start"), errors="coerce")
            end = pd.to_datetime(interval.get("end"), errors="coerce")
            if pd.isna(start) or (interval.get("end") is not None and pd.isna(end)) or (pd.notna(start) and pd.notna(end) and end < start):
                counts["invalid_interval_row"] += 1
    report["current_security_metadata"] = {"local_directory": str(namespace["SECURITY_META_DIR"]),
                                            "file_count": len(meta_paths), "counts": dict(counts),
                                            "parse_failures": parse_failures,
                                            "note": "Schema/cache inspection only; does not establish historical ST evidence completeness."}
    report["fresh_inputs_unchanged_after_reads"] = all(
        sha256(detail["path"]) == detail["raw_sha256"] for detail in freshness["artifacts"].values()
    )
    report["reviewed_source_sha256"] = {
        name: sha256(ROOT / name) for name in
        ("microcap_top100_mom16_biweekly_live.py", "microcap_top100_mom16_biweekly_live_v2_0.py",
         "tests/test_doublecheck_data_adversarial.py")
    }
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"output": str(destination), "artifacts": len(report["artifacts"]),
                      "aligned_rows": len(close), "security_metadata_files": len(meta_paths),
                      "input_hashes_unchanged": report["fresh_inputs_unchanged_after_reads"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
