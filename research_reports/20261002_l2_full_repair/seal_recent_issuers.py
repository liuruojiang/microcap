"""Seal isolated primary-source evidence; does not write formal strategy artifacts."""
from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal
from pathlib import Path

BASE = Path(__file__).resolve().parent


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rights(symbol: str, record: str):
    path = BASE / "engine" / "full_history_rights_events.csv"
    selected = []
    with path.open(encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            if row.get("symbol") == symbol and row.get("date") == record and row.get("phase") == "record":
                selected.append(row)
    return selected


def main():
    specs = [
        {
            "folder": "issuer_603088_20180622", "symbol": "603088",
            "issuer_url": "https://static.cninfo.com.cn/finalpage/2018-06-15/1205060636.PDF",
            "notice": "2018-06-15", "record": "2018-06-21", "ex": "2018-06-22",
            "cash_pay": "2018-06-22", "new_shares_first_tradable": "2018-06-25",
            "cash_per_share_gross": "0.15", "bonus_per_old_share": "0.4",
            "pre_ex_true_close": "18.01", "ex_preclose": "12.76", "ex_true_close": "12.96",
            "formula": "(18.01 - 0.15) / 1.4 = 12.757142857..., exchange preclose 12.76",
            "boundary": "On ex date 06-22 the new shares are pending for valuation; issuer states first unrestricted-share listing 06-25. Actual CSDC account posting timestamp not observed.",
        },
        {
            "folder": "issuer_603330_20180625", "symbol": "603330",
            "issuer_url": "https://static.cninfo.com.cn/finalpage/2018-06-19/1205064922.PDF",
            "notice": "2018-06-19", "record": "2018-06-25", "ex": "2018-06-26",
            "cash_pay": "2018-06-26", "new_shares_first_tradable": "2018-06-27",
            "cash_per_share_gross": "0.258", "bonus_per_old_share": "0.3",
            "pre_ex_true_close": "28.10", "ex_preclose": "21.42", "ex_true_close": "21.62",
            "formula": "(28.10 - 0.258) / 1.3 = 21.416923..., exchange preclose 21.42",
            "boundary": "On ex date 06-26 the new shares are pending for valuation; issuer states first unrestricted-share listing 06-27. Actual CSDC account posting timestamp not observed.",
        },
        {
            "folder": "issuer_600796_20180627", "symbol": "600796",
            "issuer_url": "https://static.cninfo.com.cn/finalpage/2018-06-21/1205077482.PDF",
            "suspension_url": "https://static.cninfo.com.cn/finalpage/2018-05-03/1204888142.PDF",
            "resumption_url": "https://static.cninfo.com.cn/finalpage/2018-10-31/1205562485.PDF",
            "notice": "2018-06-21", "record": "2018-06-26", "ex": "2018-06-27",
            "cash_pay": "2018-06-27", "new_shares_first_tradable": None,
            "cash_per_share_gross": "0.03", "bonus_per_old_share": "0",
            "last_true_trade": "2018-04-27", "last_true_close": "6.11",
            "ex_zero_volume_reference": "6.08", "first_true_trade": "2018-10-31",
            "first_true_preclose": "6.08", "first_true_close": "5.47",
            "formula": "6.11 - 0.03 = 6.08",
            "boundary": "Issuer suspends 05-02 through 10-30; 06-27 BaoStock 6.08 is zero-volume ex-dividend reference for valuation only, not an executable quote.",
        },
    ]
    for spec in specs:
        folder = BASE / spec["folder"]
        rows = rights(spec["symbol"], spec["record"])
        spec["rights_record_rows"] = rows
        spec["rights_record_status"] = "two_branch_record_observed" if len(rows) == 2 else "record_not_yet_complete"
        spec["frozen_event_candidates_sha256"] = digest(BASE / "data" / "event_candidates_2010_20260930.csv")
        spec["record_source_sha256_at_seal"] = digest(BASE / "engine" / "full_history_rights_events.csv")
        (folder / "evidence.json").write_text(json.dumps(spec, ensure_ascii=False, indent=2), encoding="utf-8")
        manifest = {p.name: digest(p) for p in sorted(folder.iterdir()) if p.is_file() and p.name != "manifest.json"}
        (folder / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        print(spec["folder"], spec["rights_record_status"], digest(folder / "manifest.json"))


if __name__ == "__main__":
    main()
