from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import microcap_top100_mom16_biweekly_live_v2_0 as v2_0
import microcap_top100_mom16_biweekly_live_v2_3 as v2_3
import microcap_top100_mom16_biweekly_live_v2_5 as v2_5
from scripts.audit_v2_st_history_migration import member_st_audit, performance_windows, sha256_path


OUTPUT_DIR = ROOT / "outputs"


def build_candidate(module, version: str) -> pd.DataFrame:
    official_v2_0 = module._load_official_v2_0_out()
    _summary, base_cached, turnover = v2_0.embedded_context._load_embedded_base_context()
    close_df = module._close_df_from_base(base_cached)
    common_index = getattr(module, f"build_v2_{version[-1]}_common_index")(close_df, official_v2_0.index)
    return getattr(module, f"build_v2_{version[-1]}_result")(close_df, turnover, common_index)


def audit_version(module, version: str) -> dict[str, object]:
    suffix = version.split(".", 1)[1]
    previous_path = module.COSTED_NAV_CSV
    previous = module._read_costed_nav_csv(parse_dates=["date"])
    candidate = build_candidate(module, version)
    candidate_frame = candidate.rename_axis("date").reset_index()
    previous_norm = v2_0.base_mod._normalise_dated_frame(previous, f"v{version} migration previous")
    candidate_norm = v2_0.base_mod._normalise_dated_frame(candidate_frame, f"v{version} migration candidate")
    audit_path = OUTPUT_DIR / f"{module.OUTPUT_PREFIX}_historical_rewrite_audit.csv"
    try:
        v2_0.base_mod.assert_no_historical_rewrite(
            previous=previous,
            candidate=candidate_frame,
            key_columns=getattr(module, f"V2_{suffix}_REWRITE_AUDIT_KEY_COLUMNS"),
            allowed_tail_rows=getattr(module, f"_v2_{suffix}_rewrite_allowed_tail_rows")(),
            label=f"v{version} migration audit",
            audit_path=audit_path,
            column_allowed_tail_rows=getattr(
                module,
                f"V2_{suffix}_REWRITE_AUDIT_ALLOWED_TAIL_ROWS_BY_COLUMN",
            ),
        )
    except RuntimeError:
        pass
    if not audit_path.exists():
        raise RuntimeError(f"v{version} migration audit CSV was not written")

    common = previous_norm.index.intersection(candidate_norm.index).sort_values()
    previous_returns = pd.to_numeric(previous_norm.loc[common, "return_net"], errors="coerce")
    candidate_returns = pd.to_numeric(candidate_norm.loc[common, "return_net"], errors="coerce")
    return_changed = (previous_returns - candidate_returns).abs().gt(1e-12)
    base_paths = v2_0._resolve_base_paths().output_paths
    new_member_audit = member_st_audit(base_paths["proxy_members"])
    proxy_meta = json.loads(base_paths["proxy_meta"].read_text(encoding="utf-8"))
    proxy_meta_matches = v2_0.base_mod.proxy_meta_matches_execution_model(proxy_meta)
    v2_0_official = v2_0.base_mod._normalise_dated_frame(
        v2_0.overlay_mod._read_costed_nav_csv(v2_0.COSTED_NAV_CSV),
        "v2.0 official migration anchor",
    )
    approved = bool(
        new_member_audit["st_violations"] == 0
        and new_member_audit["bad_policy_count"] == 0
        and proxy_meta_matches
        and candidate_norm.index.max() == pd.Timestamp("2026-08-20")
        and len(previous_norm.index.difference(candidate_norm.index)) == 0
        and pd.Timestamp(v2_0_official.index.max()) == pd.Timestamp("2026-08-20")
    )
    candidate_path = OUTPUT_DIR / f"microcap_top100_v2_{suffix}_audited_history_migration_candidate.csv"
    report_path = OUTPUT_DIR / f"microcap_top100_v2_{suffix}_audited_history_migration_report.json"
    candidate_frame.to_csv(candidate_path, index=False, encoding="utf-8-sig")
    report = {
        "schema_version": 1,
        "version": version,
        "approved": approved,
        "approval_reason": "Exact-hash downstream migration after the fully audited v2.0 ST and historical-universe rebuild.",
        "previous_costed_nav": str(previous_path),
        "previous_costed_nav_sha256": sha256_path(previous_path),
        "candidate_csv": str(candidate_path),
        "candidate_frame_sha256": v2_0.overlay_mod._candidate_frame_sha256(candidate_frame),
        "v2_0_costed_nav": str(v2_0.COSTED_NAV_CSV),
        "v2_0_costed_nav_sha256": sha256_path(v2_0.COSTED_NAV_CSV),
        "base_proxy_meta": str(base_paths["proxy_meta"]),
        "base_proxy_meta_sha256": sha256_path(base_paths["proxy_meta"]),
        "rewrite_audit": str(audit_path),
        "rewrite_audit_sha256": sha256_path(audit_path),
        "previous_row_count": int(len(previous_norm)),
        "candidate_row_count": int(len(candidate_norm)),
        "previous_latest_date": str(previous_norm.index.max().date()),
        "candidate_latest_date": str(candidate_norm.index.max().date()),
        "dates_removed": [str(value.date()) for value in previous_norm.index.difference(candidate_norm.index)],
        "dates_added": [str(value.date()) for value in candidate_norm.index.difference(previous_norm.index)],
        "common_return_changed_rows": int(return_changed.sum()),
        "first_common_return_change": (
            None if not return_changed.any() else str(common[return_changed.to_numpy()][0].date())
        ),
        "new_member_st_violations": int(new_member_audit["st_violations"]),
        "new_member_bad_policy_count": int(new_member_audit["bad_policy_count"]),
        "proxy_meta_matches_current_cache": bool(proxy_meta_matches),
        "previous_performance": performance_windows(previous_norm),
        "candidate_performance": performance_windows(candidate_norm),
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"report": str(report_path), **report}


def main() -> None:
    results = [audit_version(v2_3, "2.3"), audit_version(v2_5, "2.5")]
    print(
        json.dumps(
            [
                {
                    "version": item["version"],
                    "approved": item["approved"],
                    "previous_row_count": item["previous_row_count"],
                    "candidate_row_count": item["candidate_row_count"],
                    "candidate_latest_date": item["candidate_latest_date"],
                    "common_return_changed_rows": item["common_return_changed_rows"],
                    "first_common_return_change": item["first_common_return_change"],
                    "report": item["report"],
                }
                for item in results
            ],
            ensure_ascii=True,
            indent=2,
        )
    )
    if not all(bool(item["approved"]) for item in results):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
