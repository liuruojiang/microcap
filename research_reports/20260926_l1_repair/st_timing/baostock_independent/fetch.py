"""Fresh independent BaoStock ST probe. No shared probe code or cache."""

import csv
import hashlib
import importlib.metadata
import json
from datetime import datetime, timezone
from pathlib import Path

import baostock as bs


HERE = Path(__file__).resolve().parent
FIELDS = "date,code,close,volume,tradestatus,isST"
QUERIES = [
    ("sh.600603", "2003-01-01", "2016-03-31"),
    ("sh.600137", "2008-06-01", "2008-07-15"),
    ("sh.600506", "2009-03-01", "2012-09-10"),
    ("sh.600241", "2020-04-01", "2023-06-15"),
    ("sz.000010", "2009-12-01", "2010-02-28"),
    ("sz.002071", "2009-12-28", "2010-01-20"),
    ("sh.600070", "2009-12-28", "2010-01-20"),
    ("sh.600687", "2009-12-28", "2010-01-20"),
    ("sh.600634", "2009-11-01", "2010-01-20"),
    ("sz.000787", "2009-12-28", "2010-01-20"),
]


def save_csv(path, fields, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(fields)
        writer.writerows(rows)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(f"BaoStock login {login.error_code}: {login.error_msg}")
    manifest = {
        "retrieved_started_utc": datetime.now(timezone.utc).isoformat(),
        "baostock_version": importlib.metadata.version("baostock"),
        "fields_requested": FIELDS,
        "frequency": "d",
        "adjustflag": "3",
        "queries": [],
    }
    try:
        for code, start, end in QUERIES:
            result = bs.query_history_k_data_plus(code, FIELDS, start_date=start, end_date=end, frequency="d", adjustflag="3")
            if result.error_code != "0":
                raise RuntimeError(f"{code} {result.error_code}: {result.error_msg}")
            rows = []
            while result.next():
                rows.append(result.get_row_data())
            output = HERE / f"{code.replace('.', '_')}_{start}_{end}.csv"
            digest = save_csv(output, result.fields, rows)
            summary = {
                "code": code, "start": start, "end": end,
                "returned_fields": result.fields, "rows": len(rows),
                "first_date": rows[0][0] if rows else None,
                "last_date": rows[-1][0] if rows else None,
                "isST_counts": {v: sum(r[-1] == v for r in rows) for v in sorted({r[-1] for r in rows})},
                "tradestatus_counts": {v: sum(r[-2] == v for r in rows) for v in sorted({r[-2] for r in rows})},
                "csv_sha256": digest,
                "csv_path": output.name,
            }
            manifest["queries"].append(summary)
            print(code, len(rows), summary["isST_counts"], summary["tradestatus_counts"], flush=True)
    finally:
        bs.logout()
    manifest["retrieved_finished_utc"] = datetime.now(timezone.utc).isoformat()
    (HERE / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
