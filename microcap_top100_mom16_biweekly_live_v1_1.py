from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd

import microcap_top100_mom16_biweekly_live as base_mod


_ORIGINAL_BUILD_SUMMARY = base_mod.build_summary
EXPECTED_VERSION_ROLE = "backup_alternative"
EXPECTED_VERSION_NOTE_PREFIX = "Backup alternative to v1.0."
V1_0_OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live"


base_mod.FIXED_HEDGE_RATIO = 0.8
base_mod.DEFAULT_OUTPUT_PREFIX = "microcap_top100_mom16_biweekly_live_v1_1"
base_mod.DEFAULT_COSTED_NAV_CSV = (
    base_mod.OUTPUT_DIR / "microcap_top100_mom16_hedge_zz1000_0p8x_biweekly_thursday_16y_costed_nav.csv"
)
base_mod.STRATEGY_TITLE = "Top100 Microcap Mom16 Biweekly v1.1 Backup"


def build_summary(
    result,
    latest_signal,
    latest_rebalance,
    prev_rebalance,
    next_rebalance,
    members_df,
    changes_df,
    capital,
    anchor_freshness,
):
    summary = _ORIGINAL_BUILD_SUMMARY(
        result=result,
        latest_signal=latest_signal,
        latest_rebalance=latest_rebalance,
        prev_rebalance=prev_rebalance,
        next_rebalance=next_rebalance,
        members_df=members_df,
        changes_df=changes_df,
        capital=capital,
        anchor_freshness=anchor_freshness,
    )
    summary["version"] = "1.1"
    summary["version_role"] = EXPECTED_VERSION_ROLE
    summary["version_note"] = (
        "Backup alternative to v1.0. Same live framework as v1.0, "
        "but fixed hedge ratio is reduced from 1.0x to 0.8x."
    )
    return summary


base_mod.build_summary = build_summary


def summary_is_current_v1_1(summary: dict[str, object]) -> bool:
    if not isinstance(summary, dict):
        return False
    return (
        str(summary.get("version")) == "1.1"
        and str(summary.get("version_role")) == EXPECTED_VERSION_ROLE
        and str(summary.get("version_note", "")).startswith(EXPECTED_VERSION_NOTE_PREFIX)
    )


def _paths_to_invalidate(paths: dict[str, Path], costed_nav_csv: Path) -> list[Path]:
    ordered_paths = [costed_nav_csv, *paths.values()]
    unique_paths: list[Path] = []
    seen: set[Path] = set()
    for path in ordered_paths:
        resolved = Path(path)
        if resolved in seen:
            continue
        seen.add(resolved)
        unique_paths.append(resolved)
    return unique_paths


def invalidate_incompatible_outputs(
    paths: dict[str, Path] | None = None,
    costed_nav_csv: Path | None = None,
) -> list[Path]:
    actual_paths = paths if paths is not None else base_mod.build_output_paths(base_mod.DEFAULT_OUTPUT_PREFIX)
    actual_costed_nav_csv = Path(costed_nav_csv) if costed_nav_csv is not None else base_mod.DEFAULT_COSTED_NAV_CSV
    summary_path = Path(actual_paths["summary"])
    if not summary_path.exists():
        return []

    try:
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        summary = None

    if summary_is_current_v1_1(summary):
        return []

    removed: list[Path] = []
    for path in _paths_to_invalidate(actual_paths, actual_costed_nav_csv):
        if path.exists():
            path.unlink(missing_ok=True)
            removed.append(path)
    return removed


def seed_proxy_bundle_from_v1_0(paths: dict[str, Path]) -> list[Path]:
    source_paths = base_mod.build_output_paths(V1_0_OUTPUT_PREFIX)
    if not (
        source_paths["proxy_meta"].exists()
        and source_paths["proxy_members"].exists()
        and source_paths["proxy_turnover"].exists()
    ):
        return []

    try:
        source_meta = json.loads(source_paths["proxy_meta"].read_text(encoding="utf-8"))
    except Exception:
        return []
    if not base_mod.proxy_meta_matches_execution_model(source_meta):
        return []

    copied: list[Path] = []
    for key in ("proxy_meta", "proxy_members", "proxy_turnover"):
        src = source_paths[key]
        dst = paths[key]
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
        copied.append(dst)
    return copied


def costed_nav_matches_current_hedge_ratio(costed_nav_csv: Path, hedge_ratio: float) -> bool:
    path = Path(costed_nav_csv)
    if not path.exists():
        return False
    try:
        frame = pd.read_csv(path)
    except Exception:
        return False
    required = {"holding", "microcap_ret", "hedge_ret", "futures_drag", "return_raw"}
    if required.difference(frame.columns):
        return False
    active = frame.loc[frame["holding"].astype(str) != "cash"].copy()
    if active.empty:
        return True
    for col in ("microcap_ret", "hedge_ret", "futures_drag", "return_raw"):
        active[col] = pd.to_numeric(active[col], errors="coerce")
    active = active.dropna(subset=["microcap_ret", "hedge_ret", "futures_drag", "return_raw"])
    if active.empty:
        return False
    expected = active["microcap_ret"] - float(hedge_ratio) * active["hedge_ret"] - active["futures_drag"]
    return bool((active["return_raw"] - expected).abs().le(1e-10).all())


def prepare_current_v1_1_outputs(
    paths: dict[str, Path] | None = None,
    costed_nav_csv: Path | None = None,
) -> dict[str, list[Path]]:
    actual_paths = paths if paths is not None else base_mod.build_output_paths(base_mod.DEFAULT_OUTPUT_PREFIX)
    actual_costed_nav_csv = Path(costed_nav_csv) if costed_nav_csv is not None else base_mod.DEFAULT_COSTED_NAV_CSV
    removed = invalidate_incompatible_outputs(paths=actual_paths, costed_nav_csv=actual_costed_nav_csv)
    if actual_costed_nav_csv.exists() and not costed_nav_matches_current_hedge_ratio(
        actual_costed_nav_csv,
        hedge_ratio=base_mod.FIXED_HEDGE_RATIO,
    ):
        actual_costed_nav_csv.unlink(missing_ok=True)
        removed.append(actual_costed_nav_csv)
        for key in ("performance_summary", "performance_yearly", "performance_nav", "performance_chart", "performance_json"):
            actual_paths[key].unlink(missing_ok=True)
    proxy_bundle_missing = any(
        not actual_paths[key].exists() for key in ("proxy_meta", "proxy_members", "proxy_turnover")
    )
    copied = seed_proxy_bundle_from_v1_0(actual_paths) if (removed or proxy_bundle_missing) else []
    return {"removed": removed, "copied": copied}


def main() -> None:
    prepare_current_v1_1_outputs()
    base_mod.main()


if __name__ == "__main__":
    main()
