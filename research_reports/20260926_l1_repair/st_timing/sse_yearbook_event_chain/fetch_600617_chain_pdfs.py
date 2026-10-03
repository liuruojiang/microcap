"""Archive issuer originals for the 600617 ST state transitions."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import fitz
import requests


ROOT = Path(__file__).resolve().parent / "600617_chain"
IDS = {"51947726", "57900859", "60349071", "60349070", "62087422",
       "62352262", "62415175", "62441396", "63679727", "63713707"}
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/new/index"}


if __name__ == "__main__":
    with (ROOT / "all_notices.csv").open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    selected = [x for x in rows if x["id"] in IDS]
    assert len(selected) == len(IDS)
    (ROOT / "pdfs").mkdir(exist_ok=True)
    out = []
    for x in selected:
        path = ROOT / "pdfs" / f"{x['id']}.pdf"
        if path.exists():
            raw = path.read_bytes()
        else:
            response = requests.get(x["url"], headers=HEADERS, timeout=60)
            response.raise_for_status()
            raw = response.content
            path.write_bytes(raw)
        doc = fitz.open(stream=raw, filetype="pdf")
        text = "\n".join(page.get_text(sort=True) for page in doc[:min(2, len(doc))])
        path.with_suffix(".first_two_pages.txt").write_text(text, encoding="utf-8")
        out.append({**x, "pdf_file": str(path.relative_to(ROOT)), "pdf_sha256": hashlib.sha256(raw).hexdigest(),
                    "bytes": len(raw), "pages": len(doc),
                    "first_two_pages_text_file": str(path.with_suffix(".first_two_pages.txt").relative_to(ROOT))})
    (ROOT / "transition_pdf_manifest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    for x in out:
        print(x["date"], x["id"], x["title"], x["pdf_sha256"])
