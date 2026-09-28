"""Join separately certified ST target rows to replayed effective members."""

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent
cert = pd.read_csv(ROOT / "research_reports/20260926_l1_repair/st_timing/certified_st_target_lower_bound.csv", dtype={"symbol": str})
members = pd.read_csv(OUT / "members_by_rebalance.csv", dtype={"symbol": str})
assert not cert.duplicated(["target_date", "symbol"]).any()
assert not members.duplicated(["rebalance_date", "symbol"]).any()
joined = cert.merge(members, left_on=["target_date", "symbol"], right_on=["rebalance_date", "symbol"], how="left", indicator=True, validate="one_to_one")
joined["entered_effective_members"] = joined["_merge"].eq("both")
joined.drop(columns=["_merge"]).to_csv(OUT / "certified_st_effective_join.csv", index=False, encoding="utf-8-sig")
matched = joined.loc[joined["entered_effective_members"]]
bridge = joined.loc[joined["target_date"].eq("2026-09-03")]
summary = {
    "certified_target_rows": len(joined),
    "certified_symbols": int(joined["symbol"].nunique()),
    "effective_rows": len(matched),
    "effective_symbols": int(matched["symbol"].nunique()),
    "first_effective_date": str(matched["target_date"].min()),
    "bridge_target_rows": len(bridge),
    "bridge_effective_rows": int(bridge["entered_effective_members"].sum()),
    "other_noneffective_rows": int((~joined["entered_effective_members"] & ~joined["target_date"].eq("2026-09-03")).sum()),
}
(OUT / "certified_st_effective_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(summary, ensure_ascii=False, indent=2))
