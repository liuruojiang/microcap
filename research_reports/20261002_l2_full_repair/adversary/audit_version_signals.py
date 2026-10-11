"""Independent readback of recent three-version signal comparison and clock."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
VERSION = ROOT / "research_reports/20261002_l2_full_repair/version_integration"
OLD = {
    "v2.0": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv",
    "v2.3": ROOT / "outputs/microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv",
    "v2.5": ROOT / "outputs/microcap_top100_mom16_lb20_hl3_entry0_exit0_no_targetvol_v2_5_costed_nav.csv",
}


def rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def boolvalue(value: str) -> bool:
    if value not in ("True", "False"):
        raise AssertionError(f"not_bool:{value}")
    return value == "True"


def main() -> None:
    old = {v: {r["date"]: r["next_holding"] != "cash" for r in rows(path)} for v, path in OLD.items()}
    formal_scale_checks = {}
    for version,path in OLD.items():
        official = [r for r in rows(path) if "2025-10-09" <= r["date"] <= "2026-09-30"]
        assert len(official) == 241
        assert all(r["target_vol_enabled"] == "False" for r in official)
        assert all(r["current_execution_scale"] in ("0.0","1.0") and
                   r["next_session_actionable_scale"] in ("0.0","1.0") and
                   float(r["scale_change_cost"]) == 0 for r in official)
        formal_scale_checks[version] = {"sessions":len(official),"target_vol_disabled":True,
                                        "current_and_next_scale_only_0_or_1":True,"scale_change_cost_zero":True}
    output = {}
    for branch in ("prefunded", "netted"):
        manifest = json.loads((VERSION / f"recent_{branch}_signal_manifest.json").read_text(encoding="utf-8"))
        summary = json.loads((VERSION / f"recent_{branch}_signal_comparison_summary.json").read_text(encoding="utf-8"))
        new_path = VERSION / f"recent_{branch}_rebuilt_signals.csv"
        cmp_path = VERSION / f"recent_{branch}_signal_comparison.csv"
        assert sha(new_path) == manifest["output_sha256"] == summary["new_signal_sha256"]
        assert sha(cmp_path) == summary["comparison_output_sha256"]
        assert sha(Path(manifest["source_ledger"])) == manifest["source_ledger_sha256"]
        assert sha(Path(manifest["hedge_source"])) == manifest["hedge_source_sha256"]
        signals = rows(new_path)
        signal_by_date = {r["date"]: r for r in signals}
        assert len(signal_by_date) == len(signals)
        # All paper order execution dates must follow the preceding trading
        # signal close; held-through-close is the prior ending state.
        clock_checks = 0
        for i, row in enumerate(signals):
            if i == 0:
                continue
            prev = signals[i - 1]
            for version in OLD:
                key = version.replace(".", "_")
                if row[f"{key}_next_session_actionable_scale"]:
                    assert float(row[f"{key}_next_session_actionable_scale"]) in (0.0,1.0)
                    assert float(row[f"{key}_current_execution_scale"]) in (0.0,1.0)
                    assert float(row[f"{key}_scale_change_cost"]) == 0
                    assert boolvalue(row[f"{key}_signal_at_close"]) == (float(row[f"{key}_next_session_actionable_scale"]) > 0)
                if row[f"{key}_filled_signal_date"]:
                    assert row[f"{key}_filled_signal_date"] == prev["date"]
                    assert row[f"{key}_order_filled_at_close"] in ("True", "False")
                    if prev[f"{key}_signal_at_close"]:
                        assert row[f"{key}_holding_after_close"] == prev[f"{key}_signal_at_close"]
                    clock_checks += 1
                if row[f"{key}_holding_through_close"] and prev[f"{key}_holding_after_close"]:
                    assert row[f"{key}_holding_through_close"] == prev[f"{key}_holding_after_close"]
        comparison = rows(cmp_path)
        branch_out = {"clock_checks": clock_checks, "versions": {}}
        for version in OLD:
            key = version.replace(".", "_")
            part = [r for r in comparison if r["version"] == version]
            assert all(r["branch"] == branch for r in part)
            assert len(part) == summary["versions"][version]["compared_sessions"]
            assert len({r["date"] for r in part}) == len(part)
            diff_new = []
            diff_slice = []
            categories = Counter()
            for r in part:
                date = r["date"]
                n, s, o = (boolvalue(r[col]) for col in
                           ("new_proxy_signal", "old_close_short_slice_signal", "old_formal_signal"))
                assert date in old[version] and o == old[version][date]
                assert date in signal_by_date and n == boolvalue(signal_by_date[date][f"{key}_signal_at_close"])
                assert boolvalue(r["new_vs_old"]) == (n != o)
                assert boolvalue(r["slice_vs_old"]) == (s != o)
                if n != o:
                    diff_new.append(date)
                    categories[r["category"]] += 1
                else:
                    assert r["category"] == "same"
                if s != o:
                    diff_slice.append(date)
            expected = summary["versions"][version]
            assert len(diff_new) == expected["new_vs_old_count"]
            assert len(diff_slice) == expected["sliced_old_vs_old_count"]
            assert (diff_new[0] if diff_new else None) == expected["first_new_difference"]
            assert dict(categories) == expected["difference_categories"]
            branch_out["versions"][version] = {"compared_sessions": len(part),
                                               "new_vs_old_count": len(diff_new),
                                               "old_close_slice_vs_formal_count": len(diff_slice),
                                               "first_new_difference": diff_new[0] if diff_new else None}
        output[branch] = branch_out
    out = {"status": "three_version_recent_signal_comparison_readback_pass",
           "old_stream_sha256": {v: sha(path) for v, path in OLD.items()},
           "formal_scale_checks": formal_scale_checks,
           "branches": output,
           "scope": "Readback comparison and clock only. Rebuilt signal algorithms not independently reimplemented; always-invested stock proxy is not an integrated strategy cash account."}
    HERE.mkdir(parents=True, exist_ok=True)
    (HERE / "recent_three_version_signal_audit.json").write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": out["status"], "branches": output}, ensure_ascii=False))


if __name__ == "__main__":
    main()
