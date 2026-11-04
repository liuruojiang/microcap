"""Bounded primary issuer documents for the real-data ratio-return pilot."""
import json,hashlib
from pathlib import Path
import requests,fitz
BASE=Path(__file__).resolve().parent
OUT=BASE/'return_basis_candidate_20261030/event_evidence';OUT.mkdir(exist_ok=True)
SOURCES=[('000418_cash2012','https://static.cninfo.com.cn/finalpage/2012-08-23/61463955.PDF'),
         ('000559_bonus2011','https://disc.static.szse.cn/disc/disk01/finalpage/2012-03-20/fd57332b-e3b0-4ecc-873a-840fecb03a86.PDF'),
         ('600036_rights2013','https://static.cninfo.com.cn/finalpage/2013-09-05/63050244.PDF'),
         ('600036_rights_terms2013','https://www1.hkexnews.hk/listedco/listconews/sehk/2013/0822/ltn20130822798_c.pdf')]
for name,url in SOURCES:
    receipt=OUT/f'{name}.json'
    if receipt.exists():print(name,'existing receipt, no retry');continue
    r={'url':url}
    try:
        response=requests.get(url,timeout=20)
        p=OUT/f'{name}.bin';p.write_bytes(response.content)
        r.update(status=response.status_code,sha256=hashlib.sha256(p.read_bytes()).hexdigest(),file=p.name)
        response.raise_for_status();assert response.content.startswith(b'%PDF')
        text='\n'.join(page.get_text() for page in fitz.open(stream=response.content,filetype='pdf'))
        (OUT/f'{name}.txt').write_text(text,encoding='utf-8')
        r['body_received']=True
    except Exception as e:r.update(body_received=False,error=repr(e))
    receipt.write_text(json.dumps(r,ensure_ascii=False,indent=2),encoding='utf-8')
    print(name,r['body_received'])
