"""Exact-source offline notice adapter; derived records never establish authority."""
from pathlib import Path
from datetime import datetime,date,timedelta
from zoneinfo import ZoneInfo
import hashlib,json,os,re,csv,io
from scripts.st_notice_evidence import StNoticeEvidenceError,_registry,is_equity_issuer
API="https://np-anotice-stock.eastmoney.com/api/security/ann"
WORDS=("风险警示","特别处理","摘帽")
def fail(text):raise StNoticeEvidenceError(text)
def _nonrisk_investor_record(item,symbol):
    """Authenticate provider INV records without promoting them to A-share notices."""
    codes=item.get('codes');columns=item.get('columns');title=item.get('title')
    if not isinstance(codes,list) or not codes or not isinstance(columns,list) or not columns or not isinstance(title,str):return False
    venue='1' if symbol.startswith('6') else '0'
    if not all(isinstance(c,dict) and c.get('stock_code')==symbol and str(c.get('market_code'))==venue and c.get('ann_type')=='INV' for c in codes):return False
    investor_columns={'050001':'媒体采访','050002':'路演活动','050003':'调研活动','050004':'管理制度','050005':'其他投资者关系'}
    return not any(w in title for w in WORDS) and all(isinstance(c,dict) and investor_columns.get(c.get('column_code'))==c.get('column_name') and c.get('column_code') in investor_columns for c in columns)
def _read_bound(root,spec):
    relative=Path(spec["path"]);path=(root/relative).resolve()
    if relative.is_absolute() or not path.is_relative_to(root):fail("Notice archive path escapes replay root")
    raw=path.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=spec["sha256"]:fail("Notice archive source hash mismatch: "+str(relative))
    return raw

def _load_notice_archive(root,symbol,start_date,end_date,*,_manifest=None,_expected=None):
    root=Path(root).resolve()
    if _manifest is None:
        from scripts.st_history_scope import resolve_archive_reference
        configured,expected=resolve_archive_reference(root)
        if not configured and not expected:return None
        if not configured or not expected or re.fullmatch(r"[0-9a-f]{64}",expected) is None:fail("Notice archive requires path and exact SHA256")
        path=Path(configured);path=(path if path.is_absolute() else root/path).resolve()
        if not path.is_relative_to(root):fail("Notice manifest escapes replay root")
        raw=path.read_bytes()
        if hashlib.sha256(raw).hexdigest()!=expected:fail("Notice manifest hash mismatch")
        manifest=json.loads(raw)
    else:
        manifest=_manifest;expected=_expected
    if manifest.get("schema_version")==3:
        from scripts.st_history_scope import load_scope_archive
        return load_scope_archive(root,symbol,start_date,end_date,manifest,expected)
    registry,fingerprint=_registry(root)
    if manifest.get("schema_version")!=2 or manifest.get("date_policy")!="notice_date_exclusion_inclusive_exit_unchanged" or manifest.get("selection_revision")!="all-api-pages-risk-title-or-category-v1":fail("Unsupported notice archive policy")
    if manifest.get("registry_sha256")!=fingerprint:fail("Notice archive registry differs")
    entry=manifest.get("symbols",{}).get(symbol)
    if not entry or entry.get("status")!="complete_symbol_archive":fail("Issuer archive incomplete")
    start=date.fromisoformat(str(start_date)[:10]) if "-" in str(start_date) else datetime.strptime(str(start_date),"%Y%m%d").date()
    end=date.fromisoformat(str(end_date)[:10]) if "-" in str(end_date) else datetime.strptime(str(end_date),"%Y%m%d").date()
    specs={s["path"]:s for s in entry["source_files"]};blobs={k:_read_bound(root,s) for k,s in specs.items()}
    native={};bindings={};ranges=[];excluded_investor_records=[]
    for partition in entry.get("partitions",[]):
        pages=[];seen=set();total=None;query=None
        for page in partition["pages"]:
            rawpage=blobs[page["response_path"]];receipt=json.loads(blobs[page["receipt_path"]]);params=receipt.get("params",{})
            if receipt.get("status")!=200 or receipt.get("url")!=API or receipt.get("sha256")!=hashlib.sha256(rawpage).hexdigest():fail("API response/receipt mismatch")
            if any(str(params.get(k))!=v for k,v in {"stock_list":symbol,"f_node":"0","s_node":"0","ann_type":"A","page_size":"100","sr":"-1"}.items()):fail("Incomplete or different API query scope")
            q=(params.get("begin_time"),params.get("end_time"))
            if query is None:query=q
            if q!=query:fail("Pagination request range changed")
            data=json.loads(rawpage)
            if data.get("success")!=1:fail("API query unsuccessful")
            data=data["data"];number=int(data["page_index"]);count=int(data["total_hits"])
            if int(params["page_index"])!=number or int(data["page_size"])!=100 or number in seen:fail("Page identity/size duplicated or mismatched")
            if total is None:total=count
            if count!=total or total<0:fail("Page total changed")
            rows=data["list"]
            if len(rows)!=max(0,min(100,total-(number-1)*100)):fail("Page row count incomplete")
            seen.add(number);pages.extend(rows)
            for index,item in enumerate(rows):
                day=item["notice_date"][:10]
                if not q[0]<=day<=q[1]:fail("Raw notice outside actual query")
                if not is_equity_issuer(item,symbol):
                    if not _nonrisk_investor_record(item,symbol):fail("Raw API issuer mismatch")
                    excluded_investor_records.append(dict(announcement_id=item['art_code'],response_path=page['response_path'],index=index,reason='same_issuer_INV_investor_category_not_equity_notice'))
                    continue
                art=item["art_code"]
                if art in native and native[art]!=item:fail("Conflicting native identity")
                native[art]=item;bindings[(page["response_path"],index)]=item
        if query is None or seen!=set(range(1,max(1,(total+99)//100)+1)) or len({r["art_code"] for r in pages})!=total:fail("Missing pages or duplicate native IDs")
        ranges.append((date.fromisoformat(query[0]),date.fromisoformat(query[1])))
    cursor=start
    for a,b in sorted(ranges):
        if a>cursor:break
        if b>=cursor:cursor=b+timedelta(days=1)
    if cursor<=end:fail("Actual API queries do not cover requested history")
    rows=json.loads(_read_bound(root,entry["records"]));actual_native=set();actual_old=set()
    for row in rows:
        provider=row.get("source_provider");event=row.get("announcement_id");binding=row.get("source_row_binding",{})
        if row.get("issuer_symbol")!=symbol:fail("Normalized issuer mismatch")
        if provider=="eastmoney":
            original=bindings.get((binding.get("path"),binding.get("index")))
            if original is None or original["art_code"]!=event:fail("Normalized AN source-row binding mismatch")
            title=original["title"];parts=re.split("[:：]",title,maxsplit=1);clean=parts[-1];prefix=title[:-len(clean)] if clean else title
            if row.get("notice_date")!=original["notice_date"][:10] or row.get("provider_title")!=title or row.get("title")!=clean or row.get("title_transform",{}).get("removed_prefix")!=prefix or row.get("source_url")!=f"https://pdf.dfcfw.com/pdf/H2_{event}_1.pdf":fail("Normalized native date/title/source changed")
            if event in actual_native:fail("Duplicate normalized native ID")
            actual_native.add(event)
        elif provider=="cninfo":
            if binding.get("kind")=="reviewed_registry":
                original=registry.get(event)
                if not original or original.get("source_provider")=="eastmoney":fail("Unreviewed CNInfo identity")
                expected_old=dict(issuer_symbol=original["symbol"],notice_date=original["notice_date"],title=original["title"],source_url=original["source_url"])
            else:
                payload=json.loads(blobs[binding["path"]]);original=payload["announcements"][binding["index"]]
                if str(original["announcementId"])!=event:fail("CNInfo original ID mismatch")
                expected_old=dict(issuer_symbol=original["secCode"],notice_date=datetime.fromtimestamp(original["announcementTime"]/1000,ZoneInfo("Asia/Shanghai")).date().isoformat(),title=re.sub(r"<[^>]+>","",original["announcementTitle"]).strip(),source_url="https://static.cninfo.com.cn/"+original["adjunctUrl"].lstrip("/"))
            if any(row.get(k)!=v for k,v in expected_old.items()):fail("Normalized CNInfo identity changed")
            actual_old.add(event)
        elif provider=="sse":
            original=registry.get(event)
            if not original or original.get("source_type")!="reviewed_exchange_html" or binding.get("kind")!="reviewed_registry":fail("Unreviewed exchange HTML identity")
            expected_exchange=dict(issuer_symbol=original["symbol"],notice_date=original["notice_date"],title=original["title"],source_url=original["source_url"])
            if any(row.get(k)!=v for k,v in expected_exchange.items()):fail("Normalized exchange HTML identity changed")
            actual_old.add(event)
        else:fail("Unknown normalized provider")
    suppressed=set()
    for link in entry.get("crosswalk",[]):
        if not link.get("body_verified"):continue
        old=registry.get(link["cninfo_id"]);arts=link.get("eastmoney_ids",[])
        if not old or old["symbol"]!=symbol or old["announcement_id"] not in actual_old or len(arts)!=1:fail("Crosswalk lacks reviewed original")
        art=arts[0];item=native.get(art)
        if not item or item["notice_date"][:10]!=old["notice_date"] or re.split("[:：]",item["title"],maxsplit=1)[-1]!=old["title"]:fail("Crosswalk date/title/ID changed")
        if link.get("status") not in {"exact_same_pdf_as_registered_original","registered_dual_provider_and_body_binding"}:fail("Crosswalk not authorized by registry body")
        pdfspec=link.get("mirror_pdf")
        if not pdfspec or hashlib.sha256(_read_bound(root,pdfspec)).hexdigest()!=old["sha256"]:fail("Crosswalk body is not exact reviewed bytes")
        suppressed.add(art)
    candidates={art for art,item in native.items() if (art in registry and registry[art]["symbol"]==symbol) or any(word in item["title"] for word in WORDS) or any(c.get("column_code","").startswith("001002004003") for c in item.get("columns",[]))}
    if actual_native!=candidates-suppressed:fail("Normalized candidate set omits or adds source events")
    rows=[r for r in rows if start<=date.fromisoformat(r["notice_date"])<=end]
    return dict(rows=rows,manifest_sha256=expected,source_providers=sorted({r["source_provider"] for r in rows}),notice_query_start_date=min(a for a,b in ranges).isoformat(),notice_query_end_date=max(b for a,b in ranges).isoformat(),excluded_investor_records=excluded_investor_records)

def notice_archive_cache_matches(payload, root=None):
    from scripts.st_history_scope import resolve_archive_reference
    root=Path.cwd() if root is None else root
    configured,expected=resolve_archive_reference(root)
    if not configured and not expected:
        if payload.get("st_notice_archive_sha256"):fail("Bound ST metadata has no archive reference")
        return True
    if not (configured and expected and re.fullmatch(r"[0-9a-f]{64}",expected) and payload.get("st_notice_archive_sha256")==expected):return False
    verified=load_notice_archive(root,str(payload["symbol"]).zfill(6),payload["notice_query_start_date"],payload["notice_query_end_date"])
    return verified is not None and verified["manifest_sha256"]==expected


def load_notice_archive(root,symbol,start_date,end_date):
    try:
        result=_load_notice_archive(root,symbol,start_date,end_date)
        if result is not None:
            from scripts.st_history_scope import resolve_archive_reference
            configured,_=resolve_archive_reference(root);p=Path(configured)
            result["manifest_path"]=(p.resolve() if p.is_absolute() else (Path(root)/p).resolve()).relative_to(Path(root).resolve()).as_posix()
        return result
    except StNoticeEvidenceError:raise
    except Exception as exc:raise StNoticeEvidenceError(f"Cannot authenticate notice archive: {type(exc).__name__}: {exc}") from exc
