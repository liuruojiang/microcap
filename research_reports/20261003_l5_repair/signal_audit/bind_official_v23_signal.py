"""Bind one frozen official v2.3 signal to both recent paper funding ledgers.

This is a diagnostic input adapter. It does not refresh or rewrite official NAV.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
OFFICIAL = ROOT / "outputs/microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_vol10_oh26_recovery20_exec0p8_v2_3_costed_nav.csv"
OLD_SIGNALS = ROOT / "research_reports/20261002_l2_full_repair/version_integration"
START = "2025-10-09"
END = "2026-09-30"
EXPECTED_DAYS = 241


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_unique(path: Path, usecols: list[str]) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=usecols, dtype={"date": str})
    if df.date.isna().any() or df.date.duplicated().any() or not df.date.is_monotonic_increasing:
        raise ValueError(f"invalid_date_index:{path}")
    return df


def main() -> None:
    HERE.mkdir(parents=True, exist_ok=True)
    official = load_unique(
        OFFICIAL,
        ["date", "hedge_close", "next_session_actionable_scale", "next_holding", "base_next_holding", "annualized_log_wls_score"],
    )
    window = official.loc[official.date.between(START, END)].copy()
    if len(window) != EXPECTED_DAYS or window.date.iloc[[0, -1]].tolist() != [START, END]:
        raise ValueError("official_window_count_or_boundary_mismatch")
    scale = pd.to_numeric(window.next_session_actionable_scale, errors="raise")
    hedge = pd.to_numeric(window.hedge_close, errors="raise")
    if scale.isna().any() or not scale.isin([0.0, 1.0]).all() or hedge.isna().any() or (hedge <= 0).any():
        raise ValueError("invalid_official_scale_or_hedge")
    if not (window.next_holding.ne("cash") == scale.eq(1.0)).all():
        raise ValueError("official_next_holding_disagrees_with_scale")

    output = pd.DataFrame({
        "date": window.date.to_numpy(),
        "hedge_close": hedge.to_numpy(),
        "v2_3_score": pd.to_numeric(window.annualized_log_wls_score, errors="raise").to_numpy(),
        "v2_3_next_session_actionable_scale": scale.to_numpy(),
        "v2_3_signal_at_close": scale.eq(1.0).to_numpy(),
        "v2_3_source_next_holding": window.next_holding.to_numpy(),
    })
    off = output.copy()
    off_scale = window.base_next_holding.ne("cash").astype(float).reset_index(drop=True)
    if window.base_next_holding.isna().any():
        raise ValueError("official_base_next_holding_missing")
    off["v2_3_next_session_actionable_scale"] = off_scale
    off["v2_3_signal_at_close"] = off_scale.eq(1.0)
    off["v2_3_source_next_holding"] = window.base_next_holding.to_numpy()
    report = {
        "scope": "frozen_official_v2_3_signal_binding_diagnostic_only",
        "official_nav": str(OFFICIAL.relative_to(ROOT)),
        "official_nav_sha256": sha256(OFFICIAL),
        "official_rows": len(official),
        "official_first_date": official.date.iloc[0],
        "official_last_date": official.date.iloc[-1],
        "bound_rows": len(output),
        "bound_first_date": output.date.iloc[0],
        "bound_last_date": output.date.iloc[-1],
        "off_definition": "official_v2_3_base_next_holding_before_overheat; L4/L5 established inf-trigger OFF parity",
        "branches": {},
    }
    for branch in ("prefunded", "netted"):
        prior_path = OLD_SIGNALS / f"recent_{branch}_rebuilt_signals.csv"
        prior = load_unique(prior_path, ["date", "v2_3_next_session_actionable_scale"])
        if not prior.date.equals(window.date.reset_index(drop=True)):
            raise ValueError(f"official_to_{branch}_calendar_mismatch")
        prior_scale = pd.to_numeric(prior.v2_3_next_session_actionable_scale, errors="coerce")
        prior_valid = prior_scale.notna()
        outdir = HERE / branch
        outdir.mkdir(exist_ok=True)
        target = outdir / f"recent_{branch}_rebuilt_signals.csv"
        output.to_csv(target, index=False, encoding="utf-8")
        readback = load_unique(target, list(output.columns))
        if not readback.date.equals(window.date.reset_index(drop=True)):
            raise AssertionError(f"{branch}_readback_calendar_mismatch")
        if not pd.to_numeric(readback.v2_3_next_session_actionable_scale).equals(scale.reset_index(drop=True)):
            raise AssertionError(f"{branch}_readback_scale_mismatch")
        report["branches"][branch] = {
            "prior_signal": str(prior_path.relative_to(ROOT)),
            "prior_signal_sha256": sha256(prior_path),
            "prior_valid_signal_rows": int(prior_valid.sum()),
            "prior_vs_official_different_valid_days": int((prior_valid & prior_scale.ne(scale.reset_index(drop=True))).sum()),
            "output": str(target.relative_to(ROOT)),
            "output_sha256": sha256(target),
        }
        offdir = HERE / f"{branch}_off"
        offdir.mkdir(exist_ok=True)
        off_target = offdir / f"recent_{branch}_rebuilt_signals.csv"
        off.to_csv(off_target, index=False, encoding="utf-8")
        off_readback = load_unique(off_target, list(off.columns))
        if not off_readback.date.equals(window.date.reset_index(drop=True)):
            raise AssertionError(f"{branch}_off_readback_calendar_mismatch")
        if not pd.to_numeric(off_readback.v2_3_next_session_actionable_scale).equals(off_scale):
            raise AssertionError(f"{branch}_off_readback_scale_mismatch")
        report["branches"][branch]["off_output"] = str(off_target.relative_to(ROOT))
        report["branches"][branch]["off_output_sha256"] = sha256(off_target)
        report["branches"][branch]["official_on_off_signal_diff_days"] = int(scale.reset_index(drop=True).ne(off_scale).sum())
    prefunded = pd.read_csv(HERE / "prefunded/recent_prefunded_rebuilt_signals.csv")
    netted = pd.read_csv(HERE / "netted/recent_netted_rebuilt_signals.csv")
    if not prefunded.equals(netted):
        raise AssertionError("funding_branches_have_different_bound_signals")
    if not pd.read_csv(HERE / "prefunded_off/recent_prefunded_rebuilt_signals.csv").equals(
        pd.read_csv(HERE / "netted_off/recent_netted_rebuilt_signals.csv")
    ):
        raise AssertionError("funding_branches_have_different_off_signals")
    old_pref = load_unique(OLD_SIGNALS / "recent_prefunded_rebuilt_signals.csv", ["date", "v2_3_next_session_actionable_scale"])
    old_net = load_unique(OLD_SIGNALS / "recent_netted_rebuilt_signals.csv", ["date", "v2_3_next_session_actionable_scale"])
    old_a = pd.to_numeric(old_pref.v2_3_next_session_actionable_scale, errors="coerce")
    old_b = pd.to_numeric(old_net.v2_3_next_session_actionable_scale, errors="coerce")
    old_diff = old_a.notna() & old_b.notna() & old_a.ne(old_b)
    report["old_branch_signal_diff_days"] = int(old_diff.sum())
    report["old_branch_first_signal_diff"] = old_pref.loc[old_diff, "date"].iloc[0] if old_diff.any() else None
    report["bound_branch_signal_diff_days"] = 0
    (HERE / "binding_result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
