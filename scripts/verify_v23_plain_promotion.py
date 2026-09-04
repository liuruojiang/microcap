# /// script
# requires-python = ">=3.11"
# dependencies = ["numpy", "pandas", "requests", "urllib3", "akshare", "matplotlib", "openpyxl"]
# ///
"""Prepare exact-hash v2.3 parameter migration or verify final written artifacts."""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_3 as v
from scripts import top100_delivery as delivery

RUN = ROOT / "research_reports/20260904_v23_plain_promotion"
BACKUP = ROOT / ".codex_backups/20260904_224941"
RESEARCH = ROOT / "quant_param_scan_runs/20260904_v23_entry_score_width_26_20"


def parity(actual, expected):
    assert actual.index.equals(expected.index)
    for field in ("return_net", "nav_net", "current_execution_scale", "next_session_actionable_scale", "total_cost"):
        np.testing.assert_allclose(actual[field], expected[field], atol=1e-12, rtol=0, err_msg=field)
    for field in ("holding", "next_holding"):
        assert actual[field].equals(expected[field]), field


def read(path):
    return pd.read_csv(path, index_col="date", parse_dates=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare", action="store_true")
    args = parser.parse_args()
    RUN.mkdir(parents=True, exist_ok=True)
    frozen = json.loads((RESEARCH / "freshness_after.json").read_text(encoding="utf-8-sig"))
    for name, digest in frozen["inputs"].items():
        if name != "microcap_top100_mom16_biweekly_live_v2_3.py":
            assert delivery.sha(ROOT / name) == digest, name
    target = delivery.independent_target(ROOT)
    oracle = read(RESEARCH / "daily_outputs/primary_e+000.csv.gz")
    assert str(oracle.index[-1].date()) == target
    if args.prepare:
        official = v._load_official_v2_0_out()
        _, base, turnover = v.v2_0.embedded_context._load_embedded_base_context()
        close = v._close_df_from_base(base)
        out = v.build_v2_3_result(close, turnover, v.build_v2_3_common_index(close, official.index))
        parity(out, oracle)
        previous = read(v.PREVIOUS_COSTED_NAV_CSV)
        parity(previous, read(RESEARCH / "native_production_26_195.csv.gz"))
        candidate = out.rename_axis("date").reset_index()
        audit = RUN / "strategy_rewrite_audit.csv"
        try:
            v.v2_0.base_mod.assert_no_historical_rewrite(
                previous=previous.reset_index(), candidate=candidate,
                key_columns=v.V2_3_REWRITE_AUDIT_KEY_COLUMNS,
                allowed_tail_rows=v._v2_3_rewrite_allowed_tail_rows(),
                label="v2.3 official costed NAV", audit_path=audit,
                column_allowed_tail_rows=v.V2_3_REWRITE_AUDIT_ALLOWED_TAIL_ROWS_BY_COLUMN)
        except RuntimeError:
            pass
        assert audit.exists()
        result = v.strategy_promotion_evidence(v.PREVIOUS_COSTED_NAV_CSV, candidate, audit)
        result.update(approved=True, user_authorization="替换现在的2.3参数",
                      backup=str(BACKUP), oracle=str(RESEARCH / "daily_outputs/primary_e+000.csv.gz"),
                      unchanged_proxy_lineage=True)
        destination = RUN / "approved_strategy_migration.json"
    else:
        result = delivery.inspect_outputs(ROOT, target)
        assert result["ok"], result["errors"]
        actual = read(v.COSTED_NAV_CSV)
        parity(actual, oracle)
        for suffix in ("0", "5"):
            name = f"outputs/microcap_top100_mom16_biweekly_live_v2_{suffix}_nav.csv"
            parity(read(ROOT / name), read(BACKUP / name))
        result.update(oracle_parity=True, unaffected_v20_v25_parity=True,
                      windows=v.summarize_required_windows(actual.return_net),
                      scope="local_acceptance_not_cloud_delivery", backup=str(BACKUP))
        destination = RUN / "local_acceptance.json"
    destination.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "report": str(destination)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
