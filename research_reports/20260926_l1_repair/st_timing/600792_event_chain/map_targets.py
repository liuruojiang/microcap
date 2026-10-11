"""Map the frozen old target rows to the dated 600792 issuer event windows."""
from __future__ import annotations

import csv
import hashlib
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
TARGETS = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv"
EXPECTED = "04a851bb6e7e67f181607c499a0ca3264443b6e3b655817ac0933532517f7517"


def main() -> None:
    assert hashlib.sha256(TARGETS.read_bytes()).hexdigest() == EXPECTED
    with TARGETS.open(encoding="utf-8-sig", newline="") as source:
        rows = [r for r in csv.DictReader(source) if r["symbol"].zfill(6) == "600792"]
    assert len(rows) == 32
    assert rows[0]["rebalance_date"] == "2010-01-14"
    assert rows[-1]["rebalance_date"] == "2011-03-24"
    for row in rows:
        date = row["rebalance_date"]
        assert "2009-03-20" <= date < "2011-03-29"
        row["bracketing_official_events"] = "2009-03-20_ST_entry|2011-03-29_risk_warning_entry"
        row["conclusion"] = "ST_supported_by_official_bounding_events_not_exhaustive_daily_status"
    with (HERE / "old_target_impact.csv").open("w", encoding="utf-8-sig", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=list(rows[0])); writer.writeheader(); writer.writerows(rows)
    print(len(rows), rows[0]["rebalance_date"], rows[-1]["rebalance_date"])


if __name__ == "__main__":
    main()
