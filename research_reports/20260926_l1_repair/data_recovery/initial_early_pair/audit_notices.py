"""Freeze and index 2009 Q3-to-2010-01-14 issuer notice bodies for two symbols."""

import csv
import hashlib
from pathlib import Path

import fitz
import requests


HERE = Path(__file__).resolve().parent
SOURCE = HERE.parent / "initial_share_post_q3_notice_index.csv"
SYMBOLS = {"002071", "600070"}


def main() -> None:
    rows = []
    with SOURCE.open(encoding="utf-8-sig", newline="") as stream:
        notices = [r for r in csv.DictReader(stream) if r["symbol"] in SYMBOLS]
    for notice in notices:
        symbol, aid, url = notice["symbol"], notice["announcement_id"], notice["pdf_url"]
        destination = HERE / "pdfs" / f"{symbol}_{aid}.pdf"
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            body = destination.read_bytes()
        else:
            response = requests.get(url, timeout=40)
            response.raise_for_status()
            body = response.content
            if not body.startswith(b"%PDF"):
                raise RuntimeError(f"Non-PDF response: {url}")
            destination.write_bytes(body)
        document = fitz.open(stream=body, filetype="pdf")
        page_texts = [page.get_text() for page in document]
        all_text = "\n".join(page_texts)
        keyword_pages = [str(i + 1) for i, text in enumerate(page_texts) if any(term in text for term in ("股本", "注册资本", "发行股份", "增发", "转增", "送股", "注销"))]
        rows.append({
            "symbol": symbol,
            "announcement_date": notice["announcement_date"],
            "announcement_id": aid,
            "title": notice["title"],
            "url": url,
            "sha256": hashlib.sha256(body).hexdigest(),
            "bytes": len(body),
            "pages": len(page_texts),
            "extract_chars": len(all_text),
            "capital_keyword_pages": ";".join(keyword_pages),
            "local_pdf": str(destination.relative_to(HERE)),
        })
    with (HERE / "notice_body_manifest.csv").open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"notices={len(rows)} pdf={sum(x['extract_chars'] > 0 for x in rows)}")


if __name__ == "__main__":
    main()
