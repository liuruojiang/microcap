import os,json,hashlib,ctypes,re,html,shutil
from pathlib import Path
root=Path.cwd();report=root/'research_reports/storage_cleanup_20261102'
fn=ctypes.windll.kernel32.GetCompressedFileSizeW;fn.argtypes=[ctypes.c_wchar_p,ctypes.POINTER(ctypes.c_ulong)];fn.restype=ctypes.c_ulong
results=[]
for group in ['replay','evidence']:
 rows=json.loads((report/f'{group}_compression_inventory.json').read_text(encoding='utf-8'))
 before=after=checks=0;fail=[]
 for row in rows:
  p=Path(row['path']);s=p.stat();hi=ctypes.c_ulong();lo=fn('\\\\?\\'+str(p),ctypes.byref(hi));after+=(hi.value<<32)+lo;before+=row['allocated_before']
  if s.st_size!=row['size']:fail.append(str(p))
  if 'sha256_before' in row:
   with p.open('rb') as f:actual=hashlib.file_digest(f,'sha256').hexdigest()
   checks+=1
   if actual!=row['sha256_before']:fail.append(str(p))
 results.append({'group':group,'files':len(rows),'before_bytes':before,'after_bytes':after,'saved_bytes':before-after,'hash_checks':checks,'failures':fail})
test=[]
for p in report.glob('test*.log'):
 m=re.search(r'([\d,]+) total bytes of data are stored in ([\d,]+) bytes',p.read_text(encoding='utf-8-sig'))
 if m:test.append({'directory':p.stem,'logical_bytes':int(m[1].replace(',','')),'compressed_bytes':int(m[2].replace(',',''))})
result={'date':'2026-11-02','method':'Windows NTFS in-place compression; no deletion','results':results,'test_directories':test,'verified_saved_bytes':sum(x['saved_bytes'] for x in results),'drive_free_bytes':shutil.disk_usage(root).free,'deletion_status':'blocked by policy; six test directories retained and compressed, no alternate deletion attempted','preserved':'production outputs, primary source data, running repair inputs, recovery backups, Git objects, test source and logs'}
(report/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
rows=''.join(f'<tr><td>{x["group"]}</td><td>{x["files"]:,}</td><td>{x["before_bytes"]/1024**3:.2f}</td><td>{x["after_bytes"]/1024**3:.2f}</td><td>{x["saved_bytes"]/1024**3:.2f}</td></tr>' for x in results)
page=f'''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>微盘股测试文件压缩记录</title><style>body{{font:16px system-ui;max-width:1000px;margin:50px auto;background:#f5f7fa;color:#17253b}}table{{border-collapse:collapse;width:100%;background:white}}td,th{{padding:14px;border-bottom:1px solid #ddd;text-align:left}}h1{{font-size:28px}}</style><h1>微盘股测试文件压缩记录 · 2026-11-02</h1><p>已核验回放及来源证据文件节省 {result['verified_saved_bytes']/1024**3:.2f} GiB；另完成 6 个测试临时目录的原地压缩。文件路径保持不变，无需解压即可使用。</p><table><tr><th>类别</th><th>文件数</th><th>压缩前 GiB</th><th>压缩后 GiB</th><th>节省 GiB</th></tr>{rows}</table><p>逐项核对文件长度，并抽样校验 24 个大文件的 SHA-256。正式结果、主数据、恢复备份、Git 数据、测试源码及日志保留。</p><p>删除操作被平台自动审批拒绝（blocked by policy），六个目标目录未删除，改为用户已授权的原地压缩。平台未返回进一步审批入口。</p><p>详细清单：replay_compression_inventory.json、evidence_compression_inventory.json、result.json；执行日志保存在同目录。</p></html>'''
(report/'report.html').write_text(page,encoding='utf-8')
print(json.dumps(result,ensure_ascii=False),flush=True)
assert all(not x['failures'] for x in results)
