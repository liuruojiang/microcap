"""Archive public SSE annual factbooks for an isolated L1 ST event audit."""

from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import fitz
import requests


ROOT = Path(__file__).resolve().parent
URLS = {
    2008: "https://www.sse.com.cn/aboutus/publication/factbook/documents/c/10170573/files/073e042b8b87407b919e3d8e75ee91a5.pdf",
    2009: "https://www.sse.com.cn/aboutus/publication/factbook/documents/c/10170572/files/36d0635dee474838943e931bcc04c3df.pdf",
    2015: "https://www.sse.com.cn/aboutus/publication/factbook/documents/c/10170566/files/23ad0cdde73846fa88b1ee417d306eb9.pdf",
}


def one(year: int, url: str) -> dict:
    path = ROOT / f"sse_factbook_{year}.pdf"
    if path.exists():
        data = path.read_bytes()
    else:
        response = requests.get(url, timeout=90)
        response.raise_for_status()
        data = response.content
        if not data.startswith(b"%PDF"):
            raise ValueError(f"{year}: response is not a PDF")
        path.write_bytes(data)
    doc = fitz.open(stream=data, filetype="pdf")
    return {
        "covered_year": year,
        "publisher": "Shanghai Stock Exchange",
        "url": url,
        "local_file": path.name,
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "pdf_pages": len(doc),
    }


if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=3) as pool:
        rows = sorted(pool.map(lambda kv: one(*kv), URLS.items()), key=lambda x: x["covered_year"])
    (ROOT / "source_manifest.json").write_text(
        json.dumps(rows, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    for row in rows:
        print(row["covered_year"], row["pdf_pages"], row["bytes"], row["sha256"])
