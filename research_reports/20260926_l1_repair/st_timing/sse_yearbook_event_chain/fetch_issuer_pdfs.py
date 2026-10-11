"""Save near-date issuer originals for the three newly flagged old members."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import fitz
import requests


ROOT = Path(__file__).resolve().parent
IDS = {"600633": {"50424637", "57196558", "57196278", "57435883", "57472798", "57494686"},
       "600617": {"51947726", "57457285", "57336253"},
       "600792": {"50320907", "57443562", "57491007"}}
HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cninfo.com.cn/new/index"}


if __name__ == "__main__":
    notices = json.loads((ROOT / "three_issuer_notice_manifest.json").read_text(encoding="utf-8"))
    entry_notices = json.loads((ROOT / "entry_window_query_manifest.json").read_text(encoding="utf-8"))
    entries_by_code = {x["code"]: x["notices"] for x in entry_notices}
    out = []
    (ROOT / "issuer_pdfs").mkdir(exist_ok=True)
    for group in notices:
        code = group["code"]
        selected = [x for x in group["notices"] + entries_by_code[code] if x["id"] in IDS[code]]
        assert len(selected) == len(IDS[code])
        for item in selected:
            path = ROOT / "issuer_pdfs" / f"{code}_{item['id']}.pdf"
            if path.exists():
                raw = path.read_bytes()
            else:
                response = requests.get(item["url"], headers=HEADERS, timeout=60)
                response.raise_for_status()
                raw = response.content
                path.write_bytes(raw)
            doc = fitz.open(stream=raw, filetype="pdf")
            first = doc[0].get_text(sort=True)
            (path.with_suffix(".first_page.txt")).write_text(first, encoding="utf-8")
            out.append({**item, "date": item.get("date", item["url"].split("/finalpage/")[1][:10]),
                        "code": code, "pdf_file": str(path.relative_to(ROOT)),
                        "pdf_sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw),
                        "first_page_text_file": str(path.with_suffix(".first_page.txt").relative_to(ROOT)),
                        "first_page_prefix": first[:320]})
    (ROOT / "issuer_pdf_manifest.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    for x in out:
        print(x["code"], x["date"], x["id"], x["pdf_sha256"], repr(x["first_page_prefix"]))
