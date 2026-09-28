"""Bounded, read-only probe of Sohu memo pages for two historical ST cases."""

import csv
import hashlib
import json
import re
from pathlib import Path

import requests
from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parent / "sohu_name_event_probe"
MAX_PAGES = 80
SYMBOLS = ("600603", "600137")


def dump_csv(path, rows, names):
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=names)
        writer.writeheader()
        writer.writerows(rows)


def main():
    ROOT.mkdir(exist_ok=True)
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; historical-name-research/1.0)"})
    manifest = []
    events = []
    summary = {}
    for symbol in SYMBOLS:
        d = ROOT / symbol
        d.mkdir(exist_ok=True)
        seen_events = set()
        all_dates = []
        exhausted = False
        for page in range(1, MAX_PAGES + 1):
            url = f"https://q.stock.sohu.com/cn/{symbol}/bw_{page}.shtml"
            response = session.get(url, timeout=25)
            response.raise_for_status()
            raw = response.content
            html = raw.decode("gb18030", errors="replace")
            soup = BeautifulSoup(html, "html.parser")
            table = next((t for t in soup.find_all("table") if "公告日期" in t.get_text(" ", strip=True) and "备忘事项" in t.get_text(" ", strip=True)), None)
            if table is None:
                raise RuntimeError(f"Expected event table missing: {url}")
            rows = 0
            page_dates = []
            for tr in table.find_all("tr", id=re.compile(r"^tr\d+$")):
                cells = tr.find_all("td", recursive=False)
                if len(cells) < 3:
                    continue
                date, title, event_type = (cell.get_text(" ", strip=True) for cell in cells[:3])
                if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
                    continue
                rows += 1
                page_dates.append(date)
                all_dates.append(date)
                if "证券简称" in title or "证券简称" in event_type or "简称变更" in event_type:
                    key = (date, title, event_type)
                    if key not in seen_events:
                        seen_events.add(key)
                        events.append(dict(symbol=symbol, page=page, date=date, title=title, event_type=event_type, url=url))
            if not rows:
                raise RuntimeError(f"No dated event rows: {url}")
            raw_path = d / f"bw_{page}.html"
            raw_path.write_bytes(raw)
            links = {a.get("href", "") for a in soup.find_all("a", href=True)}
            next_link = f"bw_{page+1}.shtml" in links
            manifest.append(dict(symbol=symbol, page=page, url=url, status=response.status_code, bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest(), encoding="gb18030_errors_replace", replacements=html.count("\ufffd"), rows=rows, first_date=min(page_dates), last_date=max(page_dates), links_next_page=next_link, local_path=str(raw_path)))
            if not next_link:
                exhausted = True
                break
        summary[symbol] = dict(pages=page, pagination_exhausted=exhausted, event_rows=sum(x["rows"] for x in manifest if x["symbol"] == symbol), min_date=min(all_dates), max_date=max(all_dates), name_events=sum(x["symbol"] == symbol for x in events))
    dump_csv(ROOT / "page_manifest.csv", manifest, list(manifest[0]))
    dump_csv(ROOT / "name_events.csv", events, ["symbol", "page", "date", "title", "event_type", "url"])
    (ROOT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    for row in events:
        print(row["symbol"], row["date"], row["title"])


if __name__ == "__main__":
    main()
