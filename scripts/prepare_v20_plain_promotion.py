# /// script
# requires-python = ">=3.11"
# dependencies = ["numpy", "pandas"]
# ///
"""Bind the user-approved plain v2.0 replacement to the frozen research oracle."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import microcap_top100_mom16_biweekly_live_v2_0 as v
from scripts import top100_delivery as delivery


def main():
    run = ROOT / "research_reports/20260904_v20_plain_promotion"
    run.mkdir(exist_ok=True, parents=True)
    reference_root = ROOT / "research_reports/20260904_v20_plain_vs_original"
    frozen = json.loads((reference_root / "freshness_before.json").read_text(encoding="utf-8"))
    for name, expected in frozen["inputs"].items():
        if name.startswith("outputs/"):
            assert delivery.sha(ROOT / name) == expected, f"Underlying data changed: {name}"
    target = delivery.independent_target(ROOT)
    for name in [delivery.BASE_PANEL, delivery.BASE_FILES["proxy_index"], delivery.BASE_FILES["costed_nav"]]:
        info, _ = delivery.csv_info(ROOT / "outputs" / name)
        assert info["latest_date"] == target
    _, cached, turnover = v.embedded_context._load_embedded_base_context()
    close = cached[["microcap_close", "hedge_close"]].rename(columns={"microcap_close": "microcap", "hedge_close": "hedge"})
    gross = v.base_mod.run_signal(close).sort_index()
    gross = v.base_mod.apply_momentum_gap_exit_buffer(gross, 0.)
    out = v.overlay_mod.apply_v2_0_execution(gross, turnover)
    oracle = pd.read_csv(reference_root / "plain16_fixed1.csv.gz", parse_dates=["date"]).set_index("date")
    assert out.index.equals(oracle.index) and str(out.index.max().date()) == target
    for name in ("return_net", "nav_net", "current_execution_scale", "next_session_actionable_scale"):
        np.testing.assert_allclose(out[name], oracle[name], atol=1e-12, rtol=0)
    for name in ("holding", "next_holding"):
        assert out[name].equals(oracle[name])
    previous_path = v.overlay_mod.PREVIOUS_V2_0_COSTED_NAV_CSV
    old_oracle = pd.read_csv(reference_root / "original_v20.csv.gz", parse_dates=["date"]).set_index("date")
    previous = pd.read_csv(previous_path, parse_dates=["date"])
    np.testing.assert_allclose(previous.return_net, old_oracle.return_net, atol=1e-12, rtol=0)
    candidate = out.rename_axis("date").reset_index()
    audit = run / "strategy_rewrite_audit.csv"
    try:
        v.base_mod.assert_no_historical_rewrite(previous=previous, candidate=candidate,
            key_columns=v.overlay_mod.V2_0_REWRITE_AUDIT_KEY_COLUMNS,
            allowed_tail_rows=v.overlay_mod._v2_0_rewrite_allowed_tail_rows(),
            label="v2.0 official costed NAV", audit_path=audit,
            column_allowed_tail_rows=v.overlay_mod.V2_0_REWRITE_AUDIT_ALLOWED_TAIL_ROWS_BY_COLUMN)
    except RuntimeError:
        pass  # Expected parameter replacement. Never waive a failed comparison to the oracle.
    assert audit.exists()
    evidence = v.overlay_mod.strategy_promotion_evidence(previous_path, candidate, audit)
    evidence.update(approved=True, user_authorization="替换现有2.0", backup=".codex_backups/20260904_182723",
                    oracle="research_reports/20260904_v20_plain_vs_original/plain16_fixed1.csv.gz",
                    unchanged_proxy_lineage=True, known_historical_policy_gap="688592 retained, not relabeled or repaired",
                    validation="native4016rows return/state/scale exact oracle parity at1e-12; same pre-approved real data")
    report = run / "approved_strategy_migration.json"
    report.write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8")
    out.to_csv(run / "approved_candidate.csv.gz", index_label="date", compression="gzip")
    print(report)
    print("PASS: unchanged real data; old and new frozen oracle parity; explicit user strategy authorization, no metadata migration")


if __name__ == "__main__":
    main()
