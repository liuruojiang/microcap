"""Read the three saved final streams and whole-delivery manifest without writes."""

from __future__ import annotations

import json
from pathlib import Path
import sys


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(ROOT))
from scripts import top100_delivery as delivery


def main() -> None:
    frozen_date = "2026-09-30"
    stream_report = delivery.inspect_outputs(ROOT, frozen_date)
    manifest_report = delivery.validate_manifest(ROOT, delivery.inspect_outputs(ROOT, frozen_date))
    manifest = json.loads((ROOT / delivery.MANIFEST).read_text(encoding="utf-8"))
    now_hashes = delivery.input_hashes(ROOT)
    recorded = manifest.get("inputs", {})
    changed = sorted(name for name in set(recorded) | set(now_hashes) if recorded.get(name) != now_hashes.get(name))
    final_rows = {}
    for version in delivery.COSTED:
        name = f"microcap_top100_mom16_biweekly_live_v2_{version}_latest_signal.csv"
        info, rows = delivery.csv_info(ROOT / "outputs" / name)
        signal = rows[0]
        final_rows[f"v2.{version}"] = {
            "file": name,
            "sha256_normalized": info["sha256"],
            "rows": info["rows"],
            "date": info["latest_date"],
            "version": signal.get("version"),
            "strategy_revision": signal.get("strategy_revision"),
            "member_rebalance_signal_date": signal.get("member_rebalance_signal_date"),
            "member_rebalance_execution_date": signal.get("member_rebalance_execution_date"),
            "member_rebalance_actionable": signal.get("member_rebalance_actionable"),
            "member_rebalance_required": signal.get("member_rebalance_required"),
            "member_enter_count": signal.get("member_enter_count"),
            "member_exit_count": signal.get("member_exit_count"),
            "current_holding": signal.get("current_holding"),
            "next_holding": signal.get("next_holding"),
        }
    output = {
        "frozen_date": frozen_date,
        "inspect_outputs_ok": stream_report["ok"],
        "inspect_outputs_errors": stream_report["errors"],
        "stream_count": len(stream_report["streams"]),
        "stream_dates_and_rows": {name: {"rows": info["rows"], "latest_date": info["latest_date"]}
                                  for name, info in stream_report["streams"].items()},
        "manifest_status": manifest.get("status"),
        "manifest_expected_date": manifest.get("expected_date"),
        "manifest_validation_ok": manifest_report["ok"],
        "manifest_errors": manifest_report["errors"],
        "changed_manifest_input_names": changed,
        "final_signal_rows": final_rows,
        "read_only": True,
    }
    (HERE / "delivery_readback.json").write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"inspect_outputs_ok": output["inspect_outputs_ok"],
                      "manifest_validation_ok": output["manifest_validation_ok"],
                      "changed_manifest_input_names": changed}, ensure_ascii=False))


if __name__ == "__main__":
    main()
