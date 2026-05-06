from __future__ import annotations

import argparse
import importlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Iterable

import pandas as pd


VERSIONS = {
    "v1_6": "microcap_top100_mom16_biweekly_live_v1_6",
    "v1_7": "microcap_top100_mom16_biweekly_live_v1_7",
    "v1_8": "microcap_top100_mom16_biweekly_live_v1_8",
}
COMPARE_COLUMNS = [
    "holding",
    "next_holding",
    "return_net",
    "nav_net",
    "execution_scale",
    "total_cost",
]
CATEGORICAL_COLUMNS = ["holding", "next_holding"]
NUMERIC_COLUMNS = ["return_net", "nav_net", "execution_scale", "total_cost"]


def normalize_version(value: str) -> str:
    version = value.strip().lower().replace(".", "_")
    if not version.startswith("v"):
        version = f"v{version}"
    if version not in VERSIONS:
        raise ValueError(f"Unsupported version: {value}. Expected one of {', '.join(VERSIONS)}.")
    return version


def compare_frames(
    current: pd.DataFrame,
    backup: pd.DataFrame,
    *,
    tolerance: float,
    compare_columns: Iterable[str] = COMPARE_COLUMNS,
) -> dict[str, object]:
    required = {"date", *compare_columns}
    missing_current = sorted(required.difference(current.columns))
    missing_backup = sorted(required.difference(backup.columns))
    if missing_current:
        raise KeyError(f"Current frame missing columns: {missing_current}")
    if missing_backup:
        raise KeyError(f"Backup frame missing columns: {missing_backup}")

    current_sorted = current.copy()
    backup_sorted = backup.copy()
    current_sorted["date"] = pd.to_datetime(current_sorted["date"], errors="raise")
    backup_sorted["date"] = pd.to_datetime(backup_sorted["date"], errors="raise")
    current_sorted = current_sorted.sort_values("date").set_index("date")
    backup_sorted = backup_sorted.sort_values("date").set_index("date")

    current_duplicates = int(current_sorted.index.duplicated().sum())
    backup_duplicates = int(backup_sorted.index.duplicated().sum())
    common = current_sorted.index.intersection(backup_sorted.index)
    missing_in_current = backup_sorted.index.difference(current_sorted.index)
    missing_in_backup = current_sorted.index.difference(backup_sorted.index)

    report: dict[str, object] = {
        "current_rows": int(len(current_sorted)),
        "backup_rows": int(len(backup_sorted)),
        "current_start": str(current_sorted.index.min().date()),
        "current_end": str(current_sorted.index.max().date()),
        "backup_start": str(backup_sorted.index.min().date()),
        "backup_end": str(backup_sorted.index.max().date()),
        "current_duplicate_dates": current_duplicates,
        "backup_duplicate_dates": backup_duplicates,
        "common_index_rows": int(len(common)),
        "missing_in_current": int(len(missing_in_current)),
        "missing_in_backup": int(len(missing_in_backup)),
        "categorical_mismatches": {},
        "numeric_max_abs_diff": {},
        "numeric_mismatch_count_gt_tolerance": {},
    }

    equal = not (
        current_duplicates
        or backup_duplicates
        or len(missing_in_current)
        or len(missing_in_backup)
    )

    categorical_report = report["categorical_mismatches"]
    assert isinstance(categorical_report, dict)
    for column in CATEGORICAL_COLUMNS:
        if column not in compare_columns:
            continue
        left = current_sorted.loc[common, column].astype("string").fillna("<NA>")
        right = backup_sorted.loc[common, column].astype("string").fillna("<NA>")
        mismatches = int((left != right).sum())
        categorical_report[column] = mismatches
        if mismatches:
            equal = False

    max_diff_report = report["numeric_max_abs_diff"]
    mismatch_report = report["numeric_mismatch_count_gt_tolerance"]
    assert isinstance(max_diff_report, dict)
    assert isinstance(mismatch_report, dict)
    for column in NUMERIC_COLUMNS:
        if column not in compare_columns:
            continue
        left = pd.to_numeric(current_sorted.loc[common, column], errors="coerce")
        right = pd.to_numeric(backup_sorted.loc[common, column], errors="coerce")
        diff = (left - right).abs()
        both_nan = left.isna() & right.isna()
        mismatches = ((diff > tolerance) | (left.isna() ^ right.isna())) & ~both_nan
        max_diff = diff.dropna().max()
        max_diff_report[column] = 0.0 if pd.isna(max_diff) else float(max_diff)
        mismatch_report[column] = int(mismatches.sum())
        if int(mismatches.sum()):
            equal = False

    report["equal_within_tolerance"] = equal
    return report


def read_comparison_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, parse_dates=["date"])


def compare_output_files(
    output_dir: Path,
    versions: Iterable[str],
    *,
    tolerance: float,
) -> dict[str, object]:
    report: dict[str, object] = {}
    all_equal = True
    for version in versions:
        current = read_comparison_csv(output_dir / f"current_{version}_comparison.csv")
        backup = read_comparison_csv(output_dir / f"backup_{version}_comparison.csv")
        version_report = compare_frames(current, backup, tolerance=tolerance)
        report[version] = version_report
        if not bool(version_report["equal_within_tolerance"]):
            all_equal = False
    report["tolerance"] = tolerance
    report["all_equal_within_tolerance"] = all_equal
    return report


def _child_export_code() -> str:
    return r"""
import importlib
from pathlib import Path
import sys

repo = Path(sys.argv[1]).resolve()
backup_arg = sys.argv[2]
backup = Path(backup_arg).resolve() if backup_arg else None
out_dir = Path(sys.argv[3]).resolve()
source = sys.argv[4]
versions = sys.argv[5].split(",")

if source == "backup":
    sys.path.insert(0, str(backup))
    sys.path.insert(1, str(repo))
else:
    sys.path.insert(0, str(repo))

modules = {
    "v1_6": "microcap_top100_mom16_biweekly_live_v1_6",
    "v1_7": "microcap_top100_mom16_biweekly_live_v1_7",
    "v1_8": "microcap_top100_mom16_biweekly_live_v1_8",
}
columns = ["holding", "next_holding", "return_net", "nav_net", "execution_scale", "total_cost"]
out_dir.mkdir(parents=True, exist_ok=True)

for version in versions:
    mod = importlib.import_module(modules[version])
    if source == "backup":
        candidates = []
        if hasattr(mod, "v1_4_mod"):
            candidates.append(mod.v1_4_mod)
        context = getattr(mod, "v14_context", None)
        if context is not None and hasattr(context, "_v1_4"):
            candidates.append(context._v1_4)
        for v14 in candidates:
            v14.BASE_SUMMARY_JSON = repo / "outputs" / "microcap_top100_mom16_biweekly_live_v1_1_summary.json"
            v14.V1_0_SUMMARY_JSON = repo / "outputs" / "microcap_top100_mom16_biweekly_live_summary.json"
    func = getattr(mod, f"generate_{version}_outputs")
    _, _, result = func()
    available = ["date"] + [column for column in columns if column in result.columns]
    export = result.copy().rename_axis("date").reset_index()[available]
    export.to_csv(out_dir / f"{source}_{version}_comparison.csv", index=False, encoding="utf-8-sig")
    print(
        source,
        version,
        "rows",
        len(export),
        "start",
        export["date"].min(),
        "end",
        export["date"].max(),
        "cols",
        ",".join(available[1:]),
    )
"""


def export_comparison_files(
    repo_root: Path,
    backup_dir: Path,
    output_dir: Path,
    versions: list[str],
) -> None:
    for source, backup_arg in [("current", ""), ("backup", str(backup_dir))]:
        subprocess.run(
            [
                sys.executable,
                "-c",
                _child_export_code(),
                str(repo_root),
                backup_arg,
                str(output_dir),
                source,
                ",".join(versions),
            ],
            check=True,
            cwd=repo_root,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate and compare current Top100 v1.6/v1.7/v1.8 outputs against "
            "a pre-refactor backup directory."
        )
    )
    parser.add_argument("--backup-dir", required=True, type=Path)
    parser.add_argument("--repo-root", default=Path(__file__).resolve().parents[1], type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--versions", default="v1_6,v1_7,v1_8")
    parser.add_argument("--tolerance", default=1e-12, type=float)
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Compare existing current_* and backup_* CSV files in output-dir.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    backup_dir = args.backup_dir.resolve()
    output_dir = (
        args.output_dir.resolve()
        if args.output_dir
        else backup_dir / "equivalence_validation"
    )
    versions = [normalize_version(value) for value in args.versions.split(",") if value.strip()]

    if not backup_dir.exists():
        raise FileNotFoundError(f"Backup directory does not exist: {backup_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    if not args.skip_generate:
        export_comparison_files(repo_root, backup_dir, output_dir, versions)

    report = compare_output_files(output_dir, versions, tolerance=args.tolerance)
    report_path = output_dir / "equivalence_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"Wrote {report_path}")
    return 0 if bool(report["all_equal_within_tolerance"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
