# /// script
# requires-python = ">=3.11"
# dependencies = ["numpy", "pandas"]
# ///
"""Read-back acceptance of the user-approved v2.0 replacement; no strategy writes."""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import top100_delivery as delivery


def main():
    target = delivery.independent_target(ROOT)
    report = delivery.inspect_outputs(ROOT, target)
    assert report["ok"], report["errors"]
    old_root = ROOT / ".codex_backups/20260904_182723/outputs"
    oracle = pd.read_csv(ROOT / "research_reports/20260904_v20_plain_vs_original/plain16_fixed1.csv.gz")
    results = {}
    for version, filename in delivery.COSTED.items():
        actual = pd.read_csv(ROOT / "outputs" / filename)
        reference = oracle if version == "0" else pd.read_csv(old_root / filename)
        assert actual.date.equals(reference.date)
        for field in ("return_net", "nav_net"):
            np.testing.assert_allclose(actual[field], reference[field], atol=1e-12, rtol=0)
        for field in ("holding", "next_holding", "current_execution_scale", "next_session_actionable_scale"):
            if field in reference:
                assert actual[field].equals(reference[field]), (version, field)
        results[version] = {"rows": len(actual), "latest_date": actual.date.iloc[-1],
                            "reference": "confirmed plain research" if version == "0" else "pre-promotion official backup",
                            "return_nav_state_parity": True}
    report["promotion_parity"] = results
    report["scope"] = "local_artifact_readback_not_cloud_deployment"
    path = ROOT / "research_reports/20260904_v20_plain_promotion/local_acceptance.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"ok": True, "parity": results, "report": str(path)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
