"""Read exact issuer PDF bytes for the four dated ST notices of 600792."""
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import fitz
import requests

HERE = Path(__file__).resolve().parent
IDS = {"38393788", "50320907", "59183922", "60140477"}


def main() -> None:
    with (HERE / "all_notices.csv").open(encoding="utf-8-sig", newline="") as f:
        rows = [r for r in csv.DictReader(f) if r["id"] in IDS]
    assert {r["id"] for r in rows} == IDS
    out = []
    for row in rows:
        pdf = HERE / f"{row['id']}.pdf"
        if not pdf.exists():
            response = requests.get(row["url"], timeout=30)
            response.raise_for_status()
            pdf.write_bytes(response.content)
        raw = pdf.read_bytes()
        assert raw.startswith(b"%PDF")
        with fitz.open(stream=raw, filetype="pdf") as doc:
            body = "\n".join(page.get_text() for page in doc)
            pages = len(doc)
        (HERE / f"{row['id']}.txt").write_text(body, encoding="utf-8")
        out.append({**row, "pdf_sha256": hashlib.sha256(raw).hexdigest(), "pdf_bytes": len(raw), "pages": pages})
    with (HERE / "event_source_manifest.csv").open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(out[0])); writer.writeheader(); writer.writerows(out)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
