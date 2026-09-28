"""Read-only impact of S/G prefixed historical ST abbreviations."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from replay_cninfo_st import load_freq


OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[2]


def old_status(date: str, intervals: list[dict]) -> bool:
    # The frozen source build_st_status_series uses an inclusive end.
    return any(i["start"] <= date and (i.get("end") is None or date <= i["end"]) for i in intervals)


def corrected_st_name(name: str) -> bool:
    text = re.sub(r"\s+", "", str(name or "").upper())
    return bool(re.match(r"^(?:[SG])?\*?(?:ST|PT)", text))


def main() -> None:
    freq = load_freq()
    members = pd.read_csv(ROOT / "outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_members.csv",
                          dtype={"symbol": str, "rebalance_date": str})
    members["symbol"] = members["symbol"].str.zfill(6)
    changes = pd.read_csv(ROOT / ".microcap_index_cache/sz_name_change_short.csv", dtype={"symbol": str})
    changes["symbol"] = changes["symbol"].str.zfill(6)
    rows = []
    affected_changes = []
    for symbol in members["symbol"].unique():
        meta = json.loads((ROOT / ".microcap_index_cache/security_meta" / f"{symbol}.json").read_text(encoding="utf-8"))
        symbol_changes = changes.loc[changes["symbol"] == symbol]
        if symbol_changes.empty:
            continue
        for item in symbol_changes.itertuples(index=False):
            for field in ("old_name", "new_name"):
                value = getattr(item, field)
                if corrected_st_name(value) and not freq.is_st_name(value):
                    affected_changes.append({"symbol": symbol, "change_date": item.change_date,
                                             "field": field, "name": value})
        source_globals = freq.build_st_intervals_from_name_changes.__globals__
        previous = source_globals["is_st_name"]
        try:
            source_globals["is_st_name"] = corrected_st_name
            candidate = freq.build_st_intervals_from_name_changes(
                pd.Timestamp(meta["first_trade_date"]), pd.Timestamp(meta["last_trade_date"]),
                symbol_changes[["change_date", "old_name", "new_name"]],
            )
        finally:
            source_globals["is_st_name"] = previous
        for target in members.loc[members["symbol"] == symbol].itertuples(index=False):
            date = str(target.rebalance_date)
            newly_st = any(i["start"] <= date and (i.get("end") is None or date < i["end"]) for i in candidate)
            frozen_st = old_status(date, meta.get("st_intervals") or [])
            if newly_st and not frozen_st:
                rows.append({"symbol": symbol, "target_date": date, "rank": target.rank,
                             "name_snapshot_in_target": target.name,
                             "corrected_name_intervals": json.dumps(candidate, ensure_ascii=False, separators=(",", ":"))})
    out = pd.DataFrame(rows, columns=["symbol", "target_date", "rank", "name_snapshot_in_target", "corrected_name_intervals"])
    out.to_csv(OUT / "legacy_prefix_target_differences.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(affected_changes).to_csv(OUT / "legacy_prefix_name_changes.csv", index=False, encoding="utf-8-sig")
    summary = {
        "affected_name_change_symbols_among_1185": len({x["symbol"] for x in affected_changes}),
        "affected_name_change_rows_among_1185": len(affected_changes),
        "newly_st_target_rows": len(out),
        "newly_st_target_symbols": out["symbol"].nunique(),
        "first_newly_st_target": out.sort_values("target_date").head(1)[["symbol", "target_date", "rank"]].to_dict("records"),
    }
    (OUT / "legacy_prefix_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
