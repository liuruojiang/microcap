"""Exact-hash approved migration driver; default prepare never changes formal files.

Base-state installation is deliberately external. This driver verifies the
prepared base, computes official pure candidates, and uses official CLI guards.
It never copies a final NAV over a guarded output.
"""
import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as v20
import microcap_top100_mom16_biweekly_live_v2_3 as v23
import microcap_top100_mom16_biweekly_live_v2_5 as v25

v20._sync_embedded_base_config()
BASE = v20.base_mod
OV = v20.overlay_mod
MODULES = {"v2_0": OV, "v2_3": v23, "v2_5": v25}
TARGET = pd.Timestamp("2026-09-17")
FROZEN = pd.Timestamp("2026-09-03")


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, payload):
    Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def read_frame(path):
    frame = pd.read_csv(path)
    frame["date"] = pd.to_datetime(frame.date)
    return frame.set_index("date").sort_index()


def assert_same(left, right, label):
    if not left.index.equals(right.index) or set(left.columns) != set(right.columns):
        raise RuntimeError(f"{label}: dates or complete column set differ")
    differences = []
    for col in left.columns:
        a, b = left[col], right[col]
        if col.endswith("_date"):
            x, y = pd.to_datetime(a, errors="coerce"), pd.to_datetime(b, errors="coerce")
            equal = x.eq(y) | (x.isna() & y.isna())
        elif pd.api.types.is_numeric_dtype(a) and pd.api.types.is_numeric_dtype(b):
            equal = pd.Series(np.isclose(a.astype(float), b.astype(float), atol=1e-12, rtol=1e-10, equal_nan=True), index=a.index)
        else:
            equal = a.fillna("").astype(str).eq(b.fillna("").astype(str))
        for day in equal.index[~equal][:5]:
            differences.append({"date": str(day.date()), "column": col, "left": a.at[day], "right": b.at[day]})
    if differences:
        raise RuntimeError(f"{label}: field mismatch {differences[:10]}")


def approval():
    auth = json.loads((HERE / "user_approval.json").read_text(encoding="utf-8"))
    report_path = Path(auth["approved_report"])
    if auth.get("approved") is not True or sha(report_path) != auth["approved_report_sha256"]:
        raise RuntimeError("Missing approval or approved review-report hash mismatch")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    for key, info in report["candidates"].items():
        if sha(info["path"]) != info["sha256"] or info["sha256"] != auth["candidate_hashes"][key]:
            raise RuntimeError("Approved candidate changed: " + key)
    return auth, report


def compute_candidates():
    paths = v20._resolve_base_paths()
    base_frame = read_frame(paths.costed_nav_csv)
    if base_frame.index.max() != TARGET:
        raise RuntimeError("Prepared base costed NAV is not at approved target")
    close = base_frame[["microcap_close", "hedge_close"]].rename(columns={"microcap_close": "microcap", "hedge_close": "hedge"})
    turnover = pd.read_csv(paths.output_paths["proxy_turnover"])
    turnover["rebalance_date"] = pd.to_datetime(turnover.rebalance_date)
    gross = BASE.apply_momentum_gap_exit_buffer(BASE.run_signal(close), OV.V2_0_MOMENTUM_GAP_EXIT_BUFFER)
    a = OV.apply_v2_0_execution(gross, turnover)
    return {"v2_0": a,
            "v2_3": v23.build_v2_3_result(close, turnover, v23.build_v2_3_common_index(close, a.index)),
            "v2_5": v25.build_v2_5_result(close, turnover, v25.build_v2_5_common_index(close, a.index))}


def member_audit(path):
    members = pd.read_csv(path, dtype={"symbol": str})
    members["symbol"] = members.symbol.str.zfill(6)
    members["rebalance_date"] = pd.to_datetime(members.rebalance_date)
    freq = BASE.freq_mod
    violations, bad_policy = [], []
    for symbol, rows in members.groupby("symbol"):
        meta_path = freq.resolve_security_meta_path(symbol)
        if meta_path is None:
            bad_policy.append(symbol)
            continue
        meta = json.loads(Path(meta_path).read_text(encoding="utf-8"))
        if meta.get("st_notice_policy_version") != freq.ST_NOTICE_POLICY_VERSION:
            bad_policy.append(symbol)
        for interval in meta.get("st_intervals") or []:
            start, end = pd.to_datetime(interval.get("start")), pd.to_datetime(interval.get("end"))
            if pd.isna(start):
                continue
            mask = rows.rebalance_date.ge(start)
            if pd.notna(end):
                mask &= rows.rebalance_date.le(end)
            violations.extend({"symbol": symbol, "date": str(day.date())} for day in rows.loc[mask, "rebalance_date"])
    return {"st_violations": len(violations), "bad_policy_count": len(set(bad_policy)),
            "violations": violations, "bad_policy_symbols": sorted(set(bad_policy))}


def prepare(key):
    auth, review = approval()
    module = MODULES[key]
    candidates = compute_candidates()
    candidate = candidates[key]
    approved = read_frame(review["candidates"][key]["path"])
    assert_same(approved, candidate, key + " pure candidate vs approved artifact")
    previous_path = Path(module.COSTED_NAV_CSV)
    previous = read_frame(previous_path)
    assert_same(previous.loc[:FROZEN], candidate.loc[:FROZEN], key + " frozen prefix")
    if len(previous.index.difference(candidate.index)):
        raise RuntimeError("Migration may not remove published dates")
    paths = v20._resolve_base_paths().output_paths
    meta = json.loads(paths["proxy_meta"].read_text(encoding="utf-8"))
    compatible = BASE.proxy_meta_matches_execution_model(meta)
    audit_members = member_audit(paths["proxy_members"])
    if not compatible or audit_members["st_violations"] or audit_members["bad_policy_count"]:
        save(HERE / (key + "_member_gate_blocked.json"), {"proxy_meta_matches_current_cache": compatible, **audit_members})
        raise RuntimeError("Real member-policy/fingerprint audit blocked; do not invent zero counts")
    if key != "v2_0":
        assert_same(read_frame(v20.COSTED_NAV_CSV), candidates["v2_0"], "v2.0 must be migrated before downstream reports")
    suffix = key.split("_")[-1]
    audit = HERE / (key + "_official_rewrite_audit.csv")
    frame = candidate.rename_axis("date").reset_index()
    try:
        BASE.assert_no_historical_rewrite(previous=previous.reset_index(), candidate=frame,
            key_columns=getattr(module, f"V2_{suffix}_REWRITE_AUDIT_KEY_COLUMNS"),
            allowed_tail_rows=getattr(module, f"_v2_{suffix}_rewrite_allowed_tail_rows")(),
            label=f"v2.{suffix} official costed NAV", audit_path=audit,
            column_allowed_tail_rows=getattr(module, f"V2_{suffix}_REWRITE_AUDIT_ALLOWED_TAIL_ROWS_BY_COLUMN"))
    except RuntimeError:
        if not audit.is_file():
            raise
    clean = not audit.exists()
    report = {"schema_version": 1, "approved": True, "version": "2." + suffix,
        "approval_source": auth["approval_source"], "approved_review_report_sha256": auth["approved_report_sha256"],
        "user_approval_sha256": sha(HERE / "user_approval.json"),
        "previous_costed_nav_sha256": sha(previous_path),
        "candidate_frame_sha256": OV._candidate_frame_sha256(frame),
        "approved_candidate_csv_sha256": review["candidates"][key]["sha256"],
        "base_proxy_meta_sha256": sha(paths["proxy_meta"]),
        "rewrite_audit_sha256": None if clean else sha(audit),
        "previous_row_count": len(previous), "candidate_row_count": len(candidate),
        "previous_latest_date": str(previous.index.max().date()), "candidate_latest_date": str(candidate.index.max().date()),
        "new_member_st_violations": audit_members["st_violations"],
        "new_member_bad_policy_count": audit_members["bad_policy_count"],
        "proxy_meta_matches_current_cache": bool(compatible), "already_clean": clean,
        "source_sha256": sha(ROOT / ("microcap_top100_mom16_biweekly_live_" + key + ".py"))}
    if key != "v2_0":
        report["v2_0_costed_nav_sha256"] = sha(v20.COSTED_NAV_CSV)
    report_path = HERE / (key + "_approved_official_migration.json")
    save(report_path, report)
    return report_path, report, approved


def verify(key, approved, require_clean):
    module = MODULES[key]
    assert_same(approved, read_frame(module.COSTED_NAV_CSV), key + " final costed NAV readback")
    assert_same(approved, read_frame(ROOT / "outputs" / (module.OUTPUT_PREFIX + "_nav.csv")), key + " final exported NAV readback")
    summary = json.loads(Path(module.SUMMARY_JSON).read_text(encoding="utf-8"))
    status = summary.get("historical_rewrite_audit", {}).get("status")
    if require_clean and status != "clean":
        raise RuntimeError(f"{key} no-flag rerun did not finish clean: {status}")
    return {"status": status, "costed_sha256": sha(module.COSTED_NAV_CSV), "rows": len(approved), "latest_date": str(approved.index.max().date())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "run"])
    parser.add_argument("--version", choices=["v2_0", "v2_3", "v2_5", "all"], default="v2_0")
    args = parser.parse_args()
    keys = list(MODULES) if args.version == "all" else [args.version]
    outcomes = {}
    for key in keys:
        report_path, report, approved = prepare(key)
        cmd = [sys.executable, "-X", "utf8", str(ROOT / ("microcap_top100_mom16_biweekly_live_" + key + ".py"))]
        migration_cmd = cmd if report["already_clean"] else cmd + ["--audited-history-migration-report", str(report_path)]
        if args.action == "prepare":
            outcomes[key] = {"prepared": str(report_path), "migration_command": migration_cmd, "clean_command": cmd}
            continue
        with (HERE / (key + "_migration_run.log")).open("w", encoding="utf-8") as log:
            subprocess.run(migration_cmd, cwd=ROOT, check=True, stdout=log, stderr=subprocess.STDOUT)
        migrated = verify(key, approved, require_clean=report["already_clean"])
        with (HERE / (key + "_clean_rerun.log")).open("w", encoding="utf-8") as log:
            subprocess.run(cmd, cwd=ROOT, check=True, stdout=log, stderr=subprocess.STDOUT)
        clean = verify(key, approved, require_clean=True)
        outcomes[key] = {"migration": migrated, "clean_rerun": clean}
        save(HERE / "official_migration_execution.json", outcomes)
    print(json.dumps(outcomes, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
