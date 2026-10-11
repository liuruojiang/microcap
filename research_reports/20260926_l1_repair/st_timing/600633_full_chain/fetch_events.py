"""Archive 600633 event and near-target issuer PDFs from the frozen notice index."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import fitz
import requests


HERE = Path(__file__).resolve().parent
OUT = HERE / "pdfs"
IDS = (
    "50424637",  # ST entry
    "57435883", "57472798", "57494686",  # Early 2010 anchors
    "57616191", "57705138", "57803955", "57898745",  # To last target
    "57957450",  # Suspension
    "59984992", "59989011",  # Rename and restoration/full ST exit
)
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/new/index"}


def main() -> None:
    OUT.mkdir(exist_ok=True)
    with (HERE / "all_notices.csv").open(encoding="utf-8-sig", newline="") as handle:
        notices = {x["id"]: x for x in csv.DictReader(handle)}
    assert set(IDS) <= notices.keys()
    manifest = []
    for announcement_id in IDS:
        notice = notices[announcement_id]
        path = OUT / f"{announcement_id}.pdf"
        if path.exists():
            raw = path.read_bytes()
        else:
            response = requests.get(notice["url"], headers=HEADERS, timeout=45)
            response.raise_for_status()
            raw = response.content
            path.write_bytes(raw)
        doc = fitz.open(stream=raw, filetype="pdf")
        text = "\n".join(page.get_text() for page in doc)
        (OUT / f"{announcement_id}.txt").write_text(text, encoding="utf-8")
        assert "600633" in text[:1500]
        manifest.append({**notice, "pdf_sha256": hashlib.sha256(raw).hexdigest(),
                         "pdf_bytes": len(raw), "pdf_pages": len(doc),
                         "text_chars": len(text), "first_page_excerpt": text[:650]})
    (HERE / "pdf_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"pdfs": len(manifest), "errors": 0}))


if __name__ == "__main__":
    main()
