"""Read historical SZSE short names on the two early candidate dates."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import pandas as pd

from initial_2010_st_fetch import target_symbols


OUT = Path(__file__).resolve().parent / "initial_2010_st"
ROOT = OUT.parents[3]
SOURCE_URL = "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=SSGSGMXX&TABKEY=tab2"


def main() -> None:
    path = ROOT / ".microcap_index_cache/sz_name_change_short.csv"
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    names = pd.read_csv(path, dtype={"symbol": str}).sort_values(["symbol", "change_date"])
    initial, first = target_symbols()
    records = []
    for symbol in sorted(initial | first):
        if symbol.startswith("6"):
            continue
        group = names.loc[names["symbol"] == symbol]
        for target_date in ("2010-01-04", "2010-01-14"):
            if target_date == "2010-01-04" and symbol not in initial:
                continue
            if target_date == "2010-01-14" and symbol not in first:
                continue
            prior = group.loc[group["change_date"] <= target_date]
            later = group.loc[group["change_date"] > target_date]
            row = prior.iloc[-1] if not prior.empty else None
            name = row["new_name"] if row is not None else (later.iloc[0]["old_name"] if not later.empty else "")
            normalized = re.sub(r"\s+", "", str(name)).upper()
            records.append({"symbol": symbol, "target_date": target_date,
                            "historical_short_name": name,
                            "is_st_prefix": bool(re.match(r"^(?:[SG])?\*?(?:ST|PT)", normalized)),
                            "last_change_date": row["change_date"] if row is not None else "",
                            "next_change_date": later.iloc[0]["change_date"] if not later.empty else "",
                            "source_url": SOURCE_URL, "local_derived_cache_sha256": digest})
    df = pd.DataFrame(records)
    df.to_csv(OUT / "szse_historical_names_two_dates.csv", index=False, encoding="utf-8-sig")
    summary = {"rows": len(df), "symbols": df["symbol"].nunique(), "st_rows": int(df["is_st_prefix"].sum()),
               "non_st_rows": int((~df["is_st_prefix"]).sum()), "source_url": SOURCE_URL,
               "derived_cache_sha256": digest}
    (OUT / "szse_historical_names_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
