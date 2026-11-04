"""Read-only public CNInfo name-history capability probe; preserve raw receipts."""
import datetime, hashlib, json
from pathlib import Path
import requests
import akshare as ak

folder = Path(__file__).parent / 'cninfo_name_history_probe_20261101'
folder.mkdir(exist_ok=True)
namespace = ak.stock_share_change_cninfo.__globals__
encoder = namespace['py_mini_racer'].MiniRacer()
encoder.eval(namespace['_get_file_content_cninfo']('cninfo.js'))
url = 'https://webapi.cninfo.com.cn/api/stock/p_stock2109'
for symbol in ['002072', '600403', '000001']:
    if (folder / f'{symbol}_receipt.json').exists():
        raise RuntimeError('Never overwrite an existing probe receipt')
    params = {'scode': symbol, 'sdate': '1990-01-01', 'edate': '2026-10-29'}
    headers = {'Accept-Enckey': encoder.call('getResCode1'), 'Origin': 'https://webapi.cninfo.com.cn',
               'Referer': 'https://webapi.cninfo.com.cn/', 'User-Agent': 'Mozilla/5.0'}
    response = requests.post(url, params=params, headers=headers, timeout=20)
    raw = response.content
    (folder / f'{symbol}_response.json').write_bytes(raw)
    receipt = dict(url=url, params=params, status=response.status_code,
        retrieved_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        response_sha256=hashlib.sha256(raw).hexdigest())
    (folder / f'{symbol}_receipt.json').write_text(json.dumps(receipt, indent=2), encoding='utf-8')
    print(symbol, response.status_code, raw.decode('utf-8', errors='replace')[:16000], flush=True)
    if response.status_code != 200:
        break
