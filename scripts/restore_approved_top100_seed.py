"""Restore one already approved, exact-data production seed; never approve new history."""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path


BASE_FILES = {
    "proxy_index": "wind_microcap_top_100_biweekly_thursday_16y_cached.csv",
    "costed_nav": "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_v2_0_base_costed_nav.csv",
    "proxy_meta": "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json",
    "proxy_members": "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv",
    "proxy_turnover": "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_turnover.csv",
    "proxy_effective_members": "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_effective_members.csv",
}
COSTED = {
    "0": "microcap_top100_mom16_plain_fixed1_v2_0_costed_nav.csv",
    "3": "microcap_top100_mom16_lb25_hl2p5_r2w25_g0p08_eb0p08_vol10_oh_t0p26_rr0p75_exec0p8_v2_3_costed_nav.csv",
    "5": "microcap_top100_mom16_lb17_hl3_entry46_exit25_no_targetvol_v2_5_costed_nav.csv",
}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed-root", type=Path, required=True)
    parser.add_argument("--backup", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    seed = args.seed_root.resolve()
    backup = args.backup.resolve()
    if root not in backup.parents or not (backup / "manifest.json").is_file():
        raise ValueError("An existing in-repository backup manifest is required")
    authority_name = "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_frozen_tail_authority.json"
    authority = json.loads((root / authority_name).read_text(encoding="utf-8"))
    if authority["version"] != "2026-09-03-post-rebalance-bootstrap-v3":
        raise ValueError("This recovery is only approved for the 2026-09-03 seed")
    if digest(root / authority_name) != digest(seed / authority_name):
        raise ValueError("Seed authority differs from the checked-out approved authority")
    evidence = {}
    panel_name = "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_panel_refreshed.csv"
    if digest(root / panel_name) != digest(seed / panel_name):
        raise ValueError("Refreshed panel differs from the approved seed workspace")
    evidence[panel_name] = digest(root / panel_name)
    for key, name in BASE_FILES.items():
        expected = authority["seed_file_sha256"][key]
        for directory in (root, seed):
            if digest(directory / "outputs" / name) != expected:
                raise ValueError(f"Unapproved base content: {directory / name}")
        evidence[name] = expected
    planned = []
    for version, costed in COSTED.items():
        source = f"microcap_top100_mom16_biweekly_live_v2_{version}.py"
        if digest(root / source) != digest(seed / source):
            raise ValueError(f"Strategy source differs: {source}")
        prefix = f"microcap_top100_mom16_biweekly_live_v2_{version}"
        summary = json.loads((seed / "outputs" / f"{prefix}_summary.json").read_text(encoding="utf-8"))
        if summary["historical_rewrite_audit"]["status"] != "clean" or summary["latest_nav_date"] != authority["seed_end_date"]:
            raise ValueError(f"Seed v2.{version} was not clean and current")
        names = [costed] + [f"{prefix}_{suffix}" for suffix in (
            "nav.csv", "latest_signal.csv", "summary.json", "performance_nav.csv",
            "performance_summary.csv", "performance_summary.json", "performance_yearly.csv",
        )]
        for name in names:
            old = root / "outputs" / name
            archived = backup / "outputs" / name
            if old.exists() and (not archived.exists() or digest(old) != digest(archived)):
                raise ValueError(f"Current output not covered by unchanged backup: {name}")
            planned.append({"name": name, "before": digest(old) if old.exists() else None,
                            "approved_seed": digest(seed / "outputs" / name)})
    # All checks finish before any replacement. The old files remain in backup.
    report = {"scope": "restore already approved seed, not new lineage approval",
              "seed_root": str(seed), "authority": authority["version"],
              "base_hashes": evidence, "files": planned, "status": "restoring"}
    report_path = backup / "approved_seed_restore.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    for item in planned:
        target = root / "outputs" / item["name"]
        shutil.copy2(seed / "outputs" / item["name"], target)
        if digest(target) != item["approved_seed"]:
            raise RuntimeError(f"Read-back mismatch: {target}")
    report["status"] = "restored_pending_official_rerun"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(report_path)


if __name__ == "__main__":
    main()
