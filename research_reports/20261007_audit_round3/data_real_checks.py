"""Read-only data-contract verification; no market metrics or formal certification."""
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


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def meta_manifest(paths):
    return {str(path): sha(path) for path in paths}


def main():
    report_dir = Path(__file__).resolve().parent
    proof = json.loads((report_dir / "baseline_and_freshness.json").read_text(encoding="utf-8"))
    assert proof["latest_completed_session"] == "2026-09-30"
    assert proof["formal_delivery_manifest_left_blocked"] is True
    source_paths = [ROOT / name for name in ("microcap_top100_mom16_biweekly_live.py",
                                            "microcap_top100_mom16_biweekly_live_v2_0.py",
                                            "microcap_top100_mom16_biweekly_live_v2_3.py",
                                            "microcap_top100_mom16_biweekly_live_v2_5.py")]
    source_before = {str(path): sha(path) for path in source_paths}
    report = {"scope": "readonly_diagnostic_data_contract_no_strategy_or_delivery_certification",
              "freshness_evidence": "baseline_and_freshness.json", "latest_completed_session": "2026-09-30",
              "formal_refresh_state": proof["formal_refresh"], "source_sha256": source_before,
              "artifacts": {}}
    frames = {}
    for key, detail in proof["artifacts"].items():
        path = Path(detail["path"])
        assert sha(path) == detail["raw_sha256"], f"Input identity changed before check: {key}"
        frame = pd.read_csv(path)
        dates = pd.to_datetime(frame[detail["date_field"]], format="mixed", errors="coerce")
        assert len(frame) == detail["rows"]
        assert not dates.isna().any()
        assert str(dates.max().date()) == detail["latest_date"]
        result = {"path": str(path), "raw_sha256": detail["raw_sha256"], "rows": len(frame),
                  "latest_date": str(dates.max().date()), "missing_date_count": int(dates.isna().sum()),
                  "duplicate_session_count": int(dates.duplicated().sum()),
                  "nonmidnight_session_count": int(dates.ne(dates.dt.normalize()).sum())}
        if key in {"base_costed_nav", "v20_nav", "v23_costed_nav", "v25_costed_nav"}:
            checked = v20.base_mod.validate_performance_frame(frame, "return_net", "nav_net", key)
            result["finite_positive_nav_and_return_chain_passed"] = len(checked) == len(frame)
        frames[key] = frame
        report["artifacts"][key] = result

    close = v20.base_mod.load_close_df(Path(proof["artifacts"]["panel"]["path"]),
                                      Path(proof["artifacts"]["proxy_index"]["path"]))
    report["real_aligned_close"] = {"rows": len(close), "latest_date": str(close.index[-1].date()),
                                     "nonfinite_price_count": int((~np.isfinite(close.to_numpy(dtype=float))).sum())}
    turnover = v20.cost_mod.load_turnover_table(Path(proof["artifacts"]["turnover"]["path"]))
    raw = frames["turnover"]
    raw_missing = raw["execution_date"].isna() | raw["execution_date"].astype("string").str.strip().eq("").fillna(False)
    parsed_exec = pd.to_datetime(raw["execution_date"], format="mixed", errors="coerce")
    report["real_turnover"] = {"raw_rows": len(raw), "consumed_rows": len(turnover),
                               "raw_missing_execution_dates": int(raw_missing.sum()),
                               "nonempty_unparseable_execution_dates": int((~raw_missing & parsed_exec.isna()).sum()),
                               "timestamp_text_rows": int(raw["execution_date"].astype("string").str.contains(" ", regex=False).sum()),
                               "execution_timing": raw["execution_timing"].value_counts(dropna=False).to_dict(),
                               "duplicate_rebalance_dates": int(turnover["rebalance_date"].duplicated().sum())}
    assert len(turnover) == len(raw) == 432
    report["cost_window_invariance"] = {}
    for key in ["base_costed_nav", "v20_nav", "v23_costed_nav", "v25_costed_nav"]:
        index = pd.DatetimeIndex(pd.to_datetime(frames[key]["date"], format="mixed"))
        full = v20.cost_mod.map_rebalance_apply_costs(index, turnover)
        report["cost_window_invariance"][key] = {}
        for rows in [100, 500, 1500]:
            tail = index[-rows:]
            direct = v20.cost_mod.map_rebalance_apply_costs(tail, turnover)
            delta = float((direct - full.reindex(tail)).abs().max())
            assert delta == 0.0
            report["cost_window_invariance"][key][str(rows)] = delta

    meta_namespace = v20.freq_mod.load_security_meta.__globals__
    paths = sorted(Path(meta_namespace["SECURITY_META_DIR"]).glob("*.json"))
    before = meta_manifest(paths)
    counts = Counter()
    for path in paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        counts["readable_files"] += 1
        if str(payload.get("symbol") or "").zfill(6) != path.stem:
            counts["symbol_mismatch"] += 1
        policy = payload.get("st_notice_policy_version")
        counts["missing_policy" if policy is None else "current_policy" if policy == meta_namespace["ST_NOTICE_POLICY_VERSION"]
               else "different_policy"] += 1
        intervals = payload.get("st_intervals")
        if not isinstance(intervals, list):
            counts["invalid_interval_container"] += 1
        elif intervals:
            counts["nonempty_st_history"] += 1
    assert before == meta_manifest(paths)
    aggregate = hashlib.sha256("\n".join(f"{path}\0{digest}" for path, digest in before.items()).encode("utf-8")).hexdigest()
    report["security_meta_readonly"] = {"files": len(paths), "counts": dict(counts), "content_manifest_sha256": aggregate,
                                         "all_bytes_unchanged_during_read": True,
                                         "boundary": "Content/schema readback only; historical ST completeness is not certified."}
    report["official_inputs_unchanged"] = all(sha(detail["path"]) == detail["raw_sha256"]
                                              for detail in proof["artifacts"].values())
    report["source_unchanged_during_check"] = source_before == {str(path): sha(path) for path in source_paths}
    assert report["official_inputs_unchanged"]
    assert report["source_unchanged_during_check"]
    destination = report_dir / "data_real_checks.json"
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"output": str(destination), "input_hashes_unchanged": True, "turnover_rows_retained": len(turnover),
                      "metadata_files_unchanged": len(paths)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
