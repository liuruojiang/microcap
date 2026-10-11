"""Summarize already downloaded issuer PDFs; no additional retrieval or formal edits."""
from __future__ import annotations

import csv
import hashlib
import json
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def norm(s: str) -> str:
    y, m, d = s.split("/")
    return f"{y}-{int(m):02d}-{int(d):02d}"


def main() -> None:
    rows = json.loads((HERE / "results.json").read_text(encoding="utf-8"))
    for r in rows:
        p = HERE / r["pdf_file"]
        t = p.with_suffix(".txt").read_text(encoding="utf-8")
        r["pdf_sha256"] = sha(p)
        r["text_sha256"] = sha(p.with_suffix(".txt"))
        if r["symbol"] == "600423":
            assert "转增股份不向股东分配" in re.sub(r"\s+", "", t)
            r["status"] = "special_restructuring_no_old_shareholder_bonus"
            r["issuer_listing_date"] = "2018-12-19"
            r["ordinary_bonus_eligible"] = False
            r["actual_csdc_account_posting"] = "unobserved"
            continue
        header = t.split("一、", 1)[0]
        dates = re.findall(r"20\d{2}/\d{1,2}/\d{1,2}", header)
        assert len(dates) >= 3, (p, dates)
        record, ex, listing = map(norm, dates[:3])
        assert (record, ex) == (r["record_date"], r["ex_date"]), (p, record, ex)
        assert "新增无限售" in re.sub(r"\s+", "", header), p
        r["issuer_record_date"] = record
        r["issuer_ex_date"] = ex
        r["issuer_listing_date"] = listing
        r["issuer_cash_pay_date"] = norm(dates[3]) if len(dates) > 3 else None
        r["issuer_dates_match_frozen"] = True
        r["actual_csdc_account_posting"] = "unobserved"
        r["differential_ex_rights"] = bool(re.search(r"差异化分红送转\s*[：:]\s*是", t[:2500]))
        r["status"] = "issuer_listing_date_proven_differential" if r["differential_ex_rights"] else "issuer_listing_date_proven"
    (HERE / "results.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    keys = ["symbol", "record_date", "ex_date", "announcement_date", "bonus_per_10", "cash_per_10_gross", "issuer_listing_date", "issuer_cash_pay_date", "status", "differential_ex_rights", "source_url", "pdf_sha256"]
    with (HERE / "summary.csv").open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    manifest = {p.name: sha(p) for p in sorted(HERE.iterdir()) if p.is_file() and p.name != "manifest.json"}
    (HERE / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    counts = {}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    print("counts", counts)
    print("manifest_sha256", sha(HERE / "manifest.json"))


if __name__ == "__main__":
    main()
