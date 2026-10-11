"""One-variable old v2.0 signal schedule under T+1-close paper execution.

This does not regenerate momentum from the new fixed-share proxy and is never
a formal repaired strategy return stream.
"""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
SRC = ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_nav.csv"
START = "2025-10-01"


def main() -> None:
    with SRC.open(encoding="utf-8-sig", newline="") as stream:
        old = list(csv.DictReader(stream))
    dates = [r["date"] for r in old]
    assert dates == sorted(set(dates))
    records = []
    for i, r in enumerate(old):
        if r["date"] < START or i < 2:
            continue
        during = old[i - 2]["next_holding"]
        after_close = old[i - 1]["next_holding"]
        prior_after_close = old[i - 2]["next_holding"]
        # An order implied by yesterday's signal executes at today's close.
        transition = after_close != prior_after_close
        records.append({"date": r["date"], "old_same_close_holding": r["holding"],
                        "old_same_close_next_holding": r["next_holding"],
                        "new_state_during_day_from_frozen_old_signal": during,
                        "new_state_after_close_from_frozen_old_signal": after_close,
                        "order_signal_date": old[i - 1]["date"],
                        "state_transition_at_close": str(transition).lower(),
                        "no_old_new_day_state_match": str(during != r["holding"]).lower()})
    assert records[-1]["date"] == "2026-09-30"
    late = [x for x in records if "2026-09-22" <= x["date"] <= "2026-09-25"]
    assert next(x for x in late if x["date"] == "2026-09-23")["new_state_during_day_from_frozen_old_signal"] == "cash"
    assert next(x for x in late if x["date"] == "2026-09-23")["state_transition_at_close"] == "true"
    assert next(x for x in late if x["date"] == "2026-09-24")["new_state_during_day_from_frozen_old_signal"] != "cash"
    out = {"status": "frozen_old_signal_clock_reconstructed",
           "source_sha256": hashlib.sha256(SRC.read_bytes()).hexdigest(),
           "window_start": records[0]["date"], "window_end": records[-1]["date"],
           "rows": len(records),
           "close_transitions": sum(x["state_transition_at_close"] == "true" for x in records),
           "days_during_state_differs_from_old_holding": sum(x["no_old_new_day_state_match"] == "true" for x in records),
           "late_example": late,
           "scope": "Freeze old v2.0 next_holding only. New fixed-share proxy changes Mom16 input and requires generating all signals anew. No return or fee amount is inferred."}
    HERE.mkdir(parents=True, exist_ok=True)
    with (HERE / "frozen_v20_signal_clock.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(records[0]))
        writer.writeheader(); writer.writerows(records)
    (HERE / "frozen_v20_signal_clock.json").write_text(json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: out[k] for k in ("rows", "close_transitions", "days_during_state_differs_from_old_holding")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
