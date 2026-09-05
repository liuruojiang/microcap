# /// script
# requires-python = ">=3.11"
# dependencies = ["numpy", "pandas", "requests", "urllib3", "akshare", "matplotlib", "openpyxl"]
# ///
"""Prepare the exact-hash v2.5 parameter migration or verify its final artifacts."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import microcap_top100_mom16_biweekly_live_v2_5 as v
from scripts import top100_delivery as delivery

RUN = ROOT / "research_reports/20260905_v25_plain_promotion"
BACKUP = ROOT / ".codex_backups/20260905_105104"
RESEARCH = ROOT / "quant_param_scan_runs/20260904_v25_halflife_seven_lines_old_snapshot"
ORACLE = RESEARCH / "daily_outputs/lb20_h3.csv.gz"


def read(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, index_col="date", parse_dates=True)


def parity(actual: pd.DataFrame, expected: pd.DataFrame) -> None:
    assert actual.index.equals(expected.index)
    for field in (
        "return_net",
        "nav_net",
        "current_execution_scale",
        "next_session_actionable_scale",
        "total_cost",
        "annualized_log_wls_score",
    ):
        np.testing.assert_allclose(actual[field], expected[field], atol=1e-12, rtol=0, err_msg=field)
    for field in ("holding", "next_holding"):
        assert actual[field].equals(expected[field]), field


def unaffected_stream_hashes() -> dict[str, str]:
    paths = {
        "v2_0_costed": ROOT / "outputs" / delivery.COSTED["0"],
        "v2_0_nav": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv",
        "v2_3_costed": ROOT / "outputs" / delivery.COSTED["3"],
        "v2_3_nav": ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_3_nav.csv",
    }
    return {name: delivery.sha(path) for name, path in paths.items()}


def build_candidate() -> pd.DataFrame:
    official = v._load_official_v2_0_out()
    _, base, turnover = v.v2_0.embedded_context._load_embedded_base_context()
    close = v._close_df_from_base(base)
    common = v.build_v2_5_common_index(close, official.index)
    return v.build_v2_5_result(close, turnover, common)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare", action="store_true")
    args = parser.parse_args()
    RUN.mkdir(parents=True, exist_ok=True)

    target = delivery.independent_target(ROOT)
    oracle = read(ORACLE)
    assert str(oracle.index[-1].date()) == target

    if args.prepare:
        candidate = build_candidate()
        parity(candidate, oracle)
        previous = read(v.PREVIOUS_COSTED_NAV_CSV)
        candidate_frame = candidate.rename_axis("date").reset_index()
        audit = RUN / "strategy_rewrite_audit.csv"
        try:
            v.v2_0.base_mod.assert_no_historical_rewrite(
                previous=previous.reset_index(),
                candidate=candidate_frame,
                key_columns=v.V2_5_REWRITE_AUDIT_KEY_COLUMNS,
                allowed_tail_rows=v._v2_5_rewrite_allowed_tail_rows(),
                label="v2.5 official costed NAV",
                audit_path=audit,
                column_allowed_tail_rows=v.V2_5_REWRITE_AUDIT_ALLOWED_TAIL_ROWS_BY_COLUMN,
            )
        except RuntimeError:
            pass
        assert audit.exists()
        result = v.strategy_promotion_evidence(v.PREVIOUS_COSTED_NAV_CSV, candidate_frame, audit)
        result.update(
            approved=True,
            user_authorization="先把我们朴素化后的2.5参数替换原有2.5参数，形成新的2.5版本",
            backup=str(BACKUP),
            oracle=str(ORACLE),
            unchanged_proxy_lineage=True,
            unaffected_stream_hashes_before=unaffected_stream_hashes(),
        )
        destination = RUN / "approved_strategy_migration.json"
    else:
        result = delivery.inspect_outputs(ROOT, target)
        assert result["ok"], result["errors"]
        actual = read(v.COSTED_NAV_CSV)
        parity(actual, oracle)
        migration = json.loads((RUN / "approved_strategy_migration.json").read_text(encoding="utf-8"))
        assert unaffected_stream_hashes() == migration["unaffected_stream_hashes_before"]
        result.update(
            oracle_parity=True,
            unaffected_v20_v23_parity=True,
            windows=v.summarize_required_windows(actual.return_net),
            scope="local_acceptance_not_cloud_delivery",
            backup=str(BACKUP),
        )
        destination = RUN / "local_acceptance.json"

    destination.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "report": str(destination)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
