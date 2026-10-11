"""Independent, read-only BaoStock checks for two suspended ex dates."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import baostock as bs


HERE = Path(__file__).resolve().parent
FIELDS = "date,code,close,preclose,volume,tradestatus"
REQUESTS = (
    ("sh.600211", "2010-09-03", "2010-09-09"),
    ("sz.000810", "2011-06-15", "2011-06-20"),
    ("sz.000810", "2012-02-08", "2012-02-14"),
)


def main() -> None:
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"BaoStock login: {login.error_code}: {login.error_msg}")
    rows = []
    try:
        for symbol, start, end in REQUESTS:
            query = bs.query_history_k_data_plus(
                symbol,
                FIELDS,
                start_date=start,
                end_date=end,
                frequency="d",
                adjustflag="3",
            )
            if query.error_code != "0":
                raise RuntimeError(
                    f"BaoStock {symbol} {start}..{end}: {query.error_code}: {query.error_msg}"
                )
            while query.next():
                rows.append(dict(zip(query.fields, query.get_row_data(), strict=True)))
    finally:
        bs.logout()

    output = HERE / "two_event_baostock_raw.csv"
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS.split(","))
        writer.writeheader()
        writer.writerows(rows)
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    (HERE / "two_event_baostock_manifest.json").write_text(
        json.dumps(
            {
                "source": "BaoStock query_history_k_data_plus",
                "fields": FIELDS,
                "frequency": "d",
                "adjustflag": "3 (unadjusted)",
                "requests": REQUESTS,
                "rows": len(rows),
                "csv_sha256": digest,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"{len(rows)} rows, sha256={digest}")


if __name__ == "__main__":
    main()
