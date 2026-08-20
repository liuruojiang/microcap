from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import microcap_top100_mom16_biweekly_live_v2_0 as v2


DEFAULT_BACKUP = ROOT / ".codex_backups" / "20260820_134953" / "outputs"
DEFAULT_REPORT = ROOT / "outputs" / "microcap_top100_v2_0_audited_history_migration_report.json"
DEFAULT_CANDIDATE = ROOT / "outputs" / "microcap_top100_v2_0_audited_history_migration_candidate.csv"


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def performance_windows(frame: pd.DataFrame) -> dict[str, dict[str, object]]:
    returns = pd.to_numeric(frame["return_net"], errors="coerce").fillna(0.0)
    returns.index = pd.DatetimeIndex(frame.index)
    end = returns.index.max()
    windows = {"full": None, "last_10y": 10, "last_5y": 5, "last_3y": 3, "last_1y": 1}
    out: dict[str, dict[str, object]] = {}
    for label, years in windows.items():
        part = returns if years is None else returns.loc[returns.index >= end - pd.DateOffset(years=years)]
        nav = (1.0 + part).cumprod()
        elapsed_years = max((part.index[-1] - part.index[0]).days / 365.2425, len(part) / 244.0)
        annual = float(nav.iloc[-1] ** (1.0 / elapsed_years) - 1.0) if elapsed_years > 0 else 0.0
        drawdown = float((nav / nav.cummax() - 1.0).min())
        out[label] = {
            "start_date": str(part.index.min().date()),
            "end_date": str(part.index.max().date()),
            "rows": int(len(part)),
            "annualized_return": annual,
            "max_drawdown": drawdown,
            "final_nav": float(nav.iloc[-1]),
        }
    return out


def member_st_audit(members_path: Path) -> dict[str, object]:
    members = pd.read_csv(members_path, dtype={"symbol": str})
    members["symbol"] = members["symbol"].astype(str).str.zfill(6)
    members["rebalance_date"] = pd.to_datetime(members["rebalance_date"], errors="coerce")
    violations: list[dict[str, object]] = []
    bad_policy: list[str] = []
    for symbol, group in members.groupby("symbol"):
        meta_path = v2.freq_mod.resolve_security_meta_path(symbol)
        if meta_path is None:
            bad_policy.append(symbol)
            continue
        meta = json.loads(Path(meta_path).read_text(encoding="utf-8"))
        if meta.get("st_notice_policy_version") != v2.freq_mod.ST_NOTICE_POLICY_VERSION:
            bad_policy.append(symbol)
        for interval in meta.get("st_intervals") or []:
            start = pd.to_datetime(interval.get("start"), errors="coerce")
            end = pd.to_datetime(interval.get("end"), errors="coerce")
            if pd.isna(start):
                continue
            mask = group["rebalance_date"].ge(start)
            if pd.notna(end):
                mask &= group["rebalance_date"].le(end)
            for row in group.loc[mask, ["rebalance_date", "symbol", "name"]].itertuples(index=False):
                violations.append(
                    {
                        "date": str(pd.Timestamp(row.rebalance_date).date()),
                        "symbol": row.symbol,
                        "name": row.name,
                        "st_start": str(pd.Timestamp(start).date()),
                        "st_end": None if pd.isna(end) else str(pd.Timestamp(end).date()),
                    }
                )
    return {
        "member_rows": int(len(members)),
        "rebalance_dates": int(members["rebalance_date"].nunique()),
        "distinct_symbols": int(members["symbol"].nunique()),
        "bad_policy_count": int(len(set(bad_policy))),
        "bad_policy_symbols": sorted(set(bad_policy)),
        "st_violations": int(len(violations)),
        "st_violation_symbols": sorted({row["symbol"] for row in violations}),
        "st_violation_rebalance_dates": sorted({row["date"] for row in violations}),
        "st_violation_sample": violations[:50],
    }


def build_candidate() -> pd.DataFrame:
    _summary, base_cached, turnover = v2._load_embedded_base_context()
    close_df = base_cached[["microcap_close", "hedge_close"]].rename(
        columns={"microcap_close": "microcap", "hedge_close": "hedge"}
    )
    base_gross = v2.base_mod.run_signal(close_df).sort_index()
    gross = v2.base_mod.apply_momentum_gap_exit_buffer(base_gross, v2.V2_0_MOMENTUM_GAP_EXIT_BUFFER)
    lineage = v2.overlay_mod.apply_volatility_overheat_exit(gross, turnover)
    return v2.overlay_mod.apply_target_vol_scaling(lineage)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backup", type=Path, default=DEFAULT_BACKUP)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--candidate", type=Path, default=DEFAULT_CANDIDATE)
    args = parser.parse_args()

    previous_path = v2.overlay_mod.COSTED_NAV_CSV
    previous = v2.overlay_mod._read_costed_nav_csv(previous_path)
    candidate = build_candidate()
    candidate_frame = candidate.rename_axis("date").reset_index()
    args.candidate.parent.mkdir(parents=True, exist_ok=True)
    candidate_frame.to_csv(args.candidate, index=False, encoding="utf-8-sig")

    previous_norm = v2.base_mod._normalise_dated_frame(previous, "migration previous")
    candidate_norm = v2.base_mod._normalise_dated_frame(candidate_frame, "migration candidate")
    common = previous_norm.index.intersection(candidate_norm.index).sort_values()
    previous_returns = pd.to_numeric(previous_norm.loc[common, "return_net"], errors="coerce")
    candidate_returns = pd.to_numeric(candidate_norm.loc[common, "return_net"], errors="coerce")
    return_changed = (previous_returns - candidate_returns).abs().gt(1e-12)

    base_paths = v2._resolve_base_paths().output_paths
    old_members = args.backup / "microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
    new_members = base_paths["proxy_members"]
    old_audit = member_st_audit(old_members)
    new_audit = member_st_audit(new_members)
    audit_path = v2.OUTPUT_DIR / f"{v2.overlay_mod.OUTPUT_PREFIX}_historical_rewrite_audit.csv"
    proxy_meta = json.loads(base_paths["proxy_meta"].read_text(encoding="utf-8"))
    proxy_meta_matches = v2.base_mod.proxy_meta_matches_execution_model(proxy_meta)
    approved = bool(
        new_audit["st_violations"] == 0
        and new_audit["bad_policy_count"] == 0
        and proxy_meta_matches
        and candidate_norm.index.max() == pd.Timestamp("2026-08-20")
        and len(previous_norm.index.difference(candidate_norm.index)) == 0
    )
    report = {
        "schema_version": 1,
        "approved": approved,
        "approval_reason": "Exact-hash one-time migration after full v3 ST interval audit and historical-universe rebuild.",
        "previous_costed_nav": str(previous_path),
        "previous_costed_nav_sha256": sha256_path(previous_path),
        "candidate_csv": str(args.candidate),
        "candidate_frame_sha256": v2.overlay_mod._candidate_frame_sha256(candidate_frame),
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
        "first_common_return_change": None if not return_changed.any() else str(common[return_changed.to_numpy()][0].date()),
        "old_member_audit": old_audit,
        "new_member_audit": new_audit,
        "new_member_st_violations": int(new_audit["st_violations"]),
        "new_member_bad_policy_count": int(new_audit["bad_policy_count"]),
        "proxy_meta_matches_current_cache": bool(proxy_meta_matches),
        "previous_performance": performance_windows(previous_norm),
        "candidate_performance": performance_windows(candidate_norm),
    }
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=True, indent=2))
    if not approved:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
