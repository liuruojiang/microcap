"""Hash official periodic reports bracketing the disputed 600603 target dates."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import fitz
import pandas as pd
import requests

OUT = Path(__file__).resolve().parent / 'expansion_top30' / '600603_full_filings'
INDEX = pd.read_csv(OUT/'all_announcement_index.csv', dtype={'announcementId': str})
IDS = ['57227211','57644728','57865470','58259083','58317387','58601221','59043229',
       '59289504','59724935','60135160','60458105','60458361']
HEADERS = {'User-Agent':'Mozilla/5.0','Referer':'https://www.cninfo.com.cn/new/index'}

def one(notice_id):
    row = INDEX.loc[INDEX.announcementId.eq(notice_id)].iloc[0]
    url = 'https://static.cninfo.com.cn/' + row.adjunctUrl.lstrip('/')
    path = OUT/f'{notice_id}.pdf'
    if path.exists(): raw = path.read_bytes()
    else:
        resp = requests.get(url, headers=HEADERS, timeout=60); resp.raise_for_status(); raw = resp.content
        assert raw.startswith(b'%PDF'); path.write_bytes(raw)
    body = '\n'.join(p.get_text() for p in fitz.open(stream=raw,filetype='pdf'))
    (OUT/f'{notice_id}.txt').write_text(body,encoding='utf-8')
    return dict(announcementId=notice_id,notice_date=row.notice_date,title=row.title,url=url,
                sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw),extracted_chars=len(body),
                st_name_in_body=('ST兴业' in body.replace(' ','').replace('\n','')),
                local_source=str(path))

def main():
    rows=[]
    with ThreadPoolExecutor(max_workers=3) as pool:
        for fut in as_completed([pool.submit(one,n) for n in IDS]): rows.append(fut.result())
    df=pd.DataFrame(rows).sort_values('notice_date')
    df.to_csv(OUT/'periodic_source_manifest.csv',index=False,encoding='utf-8-sig')
    print(df[['announcementId','notice_date','bytes','extracted_chars','st_name_in_body']].to_string(index=False))

if __name__=='__main__':main()
