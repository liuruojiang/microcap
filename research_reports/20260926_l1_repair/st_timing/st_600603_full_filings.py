"""Archive all CNInfo 600603 filings across the disputed ST interval, isolated."""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import time
from pathlib import Path

import pandas as pd
import requests

OUT = Path(__file__).resolve().parent / 'expansion_top30' / '600603_full_filings'
OUT.mkdir(parents=True, exist_ok=True)
URL = 'https://www.cninfo.com.cn/new/hisAnnouncement/query'
ORG = 'gssh0600603'
P = dict(pageNum='1', pageSize='30', column='sse', tabName='fulltext', plate='', stock='600603,'+ORG,
         searchkey='', secid='', category='', trade='', seDate='2004-05-10~2012-01-19',
         sortName='', sortType='', isHLtitle='true')
HEADERS = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://www.cninfo.com.cn/new/index'}

def page(n: int) -> tuple[dict, str, int]:
    dest = OUT / f'page_{n:03d}.json.gz'
    if dest.exists():
        raw = gzip.decompress(dest.read_bytes())
    else:
        last = None
        for retry in range(3):
            try:
                resp = requests.post(URL, data={**P, 'pageNum': str(n)}, headers=HEADERS, timeout=30)
                resp.raise_for_status()
                raw = resp.content
                data = json.loads(raw)
                assert isinstance(data.get('announcements'), list)
                dest.write_bytes(gzip.compress(raw))
                break
            except Exception as exc:
                last = exc
                time.sleep(1 + retry)
        else:
            raise RuntimeError(f'page {n} failed') from last
    data = json.loads(raw)
    assert isinstance(data.get('announcements'), list)
    return data, hashlib.sha256(raw).hexdigest(), len(raw)

def main() -> None:
    first, _, _ = page(1)
    total = int(first['totalAnnouncement'])
    n_pages = math.ceil(total / 30)
    manifest = []
    rows = []
    for n in range(1, n_pages+1):
        data, digest, size = page(n)
        assert int(data['totalAnnouncement']) == total
        items = data['announcements']
        manifest.append(dict(page=n, total=total, returned=len(items), bytes=size, sha256=digest))
        for item in items:
            rows.append(dict(announcementId=str(item['announcementId']),
                             notice_date=pd.to_datetime(item['announcementTime'], unit='ms', utc=True).tz_convert('Asia/Shanghai').strftime('%Y-%m-%d'),
                             title=item.get('announcementTitle'), adjunctUrl=item.get('adjunctUrl')))
    df = pd.DataFrame(rows)
    assert len(df) == total and df.announcementId.nunique() == total
    df.to_csv(OUT/'all_announcement_index.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(manifest).to_csv(OUT/'page_manifest.csv', index=False, encoding='utf-8-sig')
    summary = dict(symbol='600603', start='2004-05-10', end='2012-01-19', total=total, pages=n_pages,
                   rows=len(df), unique_ids=df.announcementId.nunique(), page_sha_verified=n_pages)
    (OUT/'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(summary, ensure_ascii=False, indent=2))

if __name__ == '__main__': main()
