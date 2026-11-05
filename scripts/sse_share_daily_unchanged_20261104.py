"""User-approved, single-day SSE authority for the preserved 603028 share tail."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys
import uuid
from urllib.parse import parse_qs, urlparse

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

KIND = "sse_dated_unchanged_tail_20261104_v1"
PREFIX = "data/share_source_sse_review_20261104/"
REVIEW_SHA = "8ecabf6af932b1ab0c158675098087414a951936c4b30a07a359aad86998bb7a"
APPROVAL_SHA = "89a0e9f07057ec85674ce41a91fc8c9f7220838196b8a964dc9d87c31c6c3b43"
SSE_URL = "https://query.sse.com.cn/commonQuery.do"
SSE_REFERER = "https://www.sse.com.cn/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=603028"
CAPITAL_PARAMS = dict(isPagination="false", sqlId="COMMON_SSE_CP_GPJCTPZ_GPLB_GPGK_GBJG_C", COMPANY_CODE="603028")
LIST_PARAMS = {"STOCK_TYPE":"1","REG_PROVINCE":"","CSRC_CODE":"","STOCK_CODE":"603028",
              "sqlId":"COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L","COMPANY_STATUS":"2,4,5,7,8","type":"inParams",
              "isPagination":"true","pageHelp.cacheSize":"1","pageHelp.beginPage":"1",
              "pageHelp.pageSize":"50","pageHelp.pageNo":"1","pageHelp.endPage":"1"}
_review = json.loads((Path(__file__).resolve().parents[1] / PREFIX / "review.json").read_bytes())
SOURCE_FILES = ["scripts/sse_share_daily_unchanged_20261104.py", PREFIX+"review.json", PREFIX+"approval.json", PREFIX+"REVIEW.md"]
SOURCE_FILES += [PREFIX+name for name in _review["files"]]


def require(ok, message):
    if not ok:
        raise ValueError("SSE dated unchanged source: " + message)


def approval(root, *, symbol, previous_day, end, candidate):
    require((symbol, previous_day, end)==("603028","2026-11-03","2026-11-04"), "not the approved single stock/day")
    directory = Path(root) / PREFIX
    raw = (directory / "review.json").read_bytes()
    require(hashlib.sha256(raw).hexdigest()==REVIEW_SHA, "review bytes changed")
    review = json.loads(raw)
    raw = (directory / "approval.json").read_bytes()
    require(hashlib.sha256(raw).hexdigest()==APPROVAL_SHA, "approval bytes changed")
    decision = json.loads(raw)
    require(review["approved"] is False and decision["approved"] is True
            and decision["review_sha256"]==REVIEW_SHA and decision["symbol"]==symbol
            and decision["previous_session"]==previous_day and decision["signal_day"]==end
            and decision["disclosure_required"] is True and decision["legal_cancellation_completion_not_claimed"] is True,
            "missing exact user authority")
    require(hashlib.sha256(candidate).hexdigest()==decision["history_csv_sha256_unchanged"], "share history changed")
    for name, spec in review["files"].items():
        path = (directory / name).resolve()
        require(path.is_relative_to(directory.resolve()), "review path escapes")
        body = path.read_bytes()
        require(hashlib.sha256(body).hexdigest()==spec["sha256"] and len(body)==spec["bytes"], "review evidence changed: "+name)
    return review, decision


def _sse(entry, bound, params, now):
    from scripts import share_unchanged_tail as native
    raw = bound(entry["raw_path"], entry["raw_sha256"])
    receipt = json.loads(bound(entry["receipt_path"], entry["receipt_sha256"]))
    require(receipt.get("url")==SSE_URL and receipt.get("params")==params and receipt.get("http_status")==200
            and receipt.get("tls_verification") is True and receipt.get("raw_sha256")==hashlib.sha256(raw).hexdigest()
            and type(receipt.get("bytes")) is int and receipt["bytes"]==len(raw), "SSE HTTP identity/TLS/raw receipt")
    actual = urlparse(receipt["actual_url"])
    require(actual.scheme=="https" and actual.netloc=="query.sse.com.cn" and actual.path=="/commonQuery.do"
            and parse_qs(actual.query,keep_blank_values=True)=={k:[v] for k,v in params.items()}, "SSE actual endpoint/issuer query")
    start, complete = native.clock(receipt["http_start"]), native.clock(receipt["http_complete"])
    require(datetime.fromisoformat("2026-11-04T15:00:00+08:00")<=start<=complete<=now, "SSE collection clock/close")
    require("headers" not in receipt and "request_headers" not in receipt, "private header inventory")
    body = json.loads(raw)
    require(body.get("sqlId")==params["sqlId"] and body.get("actionErrors")==[]
            and body.get("actionMessages")==[] and body.get("fieldErrors")=={}
            and isinstance(body.get("result"),list) and len(body["result"])==1, "SSE incomplete/error response")
    return body, receipt


def _verified_facts(manifest, *, symbol, previous_day, end, candidate, bound, now=None, root=None):
    from scripts import share_unchanged_tail as native
    now = now or datetime.now(timezone.utc)
    review, decision = approval(root, symbol=symbol, previous_day=previous_day, end=end, candidate=candidate)
    require(manifest.get("schema")==KIND and manifest.get("symbol")==symbol
            and manifest.get("previous_day")==previous_day and manifest.get("query_end")==end
            and manifest.get("review_sha256")==REVIEW_SHA and manifest.get("approval_sha256")==APPROVAL_SHA,
            "manifest authority/window")
    native.require_adjacent(previous_day,end)
    files=manifest["files"]
    require(set(files)=={"unfiltered","bounded","sina","quote","sse_listing","sse_capital"}
            and manifest.get("future_plan_documents")==[], "unexpected source inventory")
    d,t=native.day(previous_day),native.day(end)
    old = native.pd.read_csv(native.io.BytesIO(candidate),dtype=str,keep_default_na=False)
    require(not old.empty and list(old.columns)==["change_date","total_shares_10k","reason"], "candidate schema")
    dates=[native.day(value[:10]) for value in old.change_date]
    require(dates==sorted(set(dates)) and max(dates)<=d, "candidate history dates")
    before=Decimal("28514.52");after=Decimal("28483.52")
    require(native.amount(old.iloc[-1].total_shares_10k)==before, "previous certified boundary")
    unfiltered=native._em_inventory(files["unfiltered"],bound,symbol,end,False,now)
    bounded=native._em_inventory(files["bounded"],bound,symbol,end,True,now)
    require(unfiltered==bounded and unfiltered, "EM full/bounded/future inventory")
    planned=[];historical=[]
    for row in unfiltered:
        ds=tuple(native._em_date(row[key]) for key in native.DATE_FIELDS)
        if any(d<value<=t for value in ds):
            require(ds==(t,t,native.day("2026-10-31")) and native.amount(row["TOTAL_SHARES"])==after*10000
                    and row.get("CHANGE_REASON")=="回购", "unreviewed EM tail event")
            planned.append(row)
        else:
            require(max(ds)<=d, "EM event crosses certified cutoff")
            historical.append(row)
    require(len(planned)==1 and historical and native.amount(historical[0]["TOTAL_SHARES"])==before*10000,
            "EM known plan/prior boundary")
    plan=[r for r in native.plan_reviews(root) if r["symbol"]==symbol and r["planned_date"]==end]
    require(len(plan)==1 and plan[0]["status"]=="verified_future_plan_not_executed"
            and plan[0]["announcement_id"]=="AN202610301831245917" and plan[0]["before_total_shares"]==int(before*10000)
            and plan[0]["after_total_shares"]==int(after*10000) and plan[0]["change_total_shares"]==-310000
            and plan[0]["pdf_sha256"]==review["issuer_plan"]["pdf_sha256"], "reviewed plan identity; no execution claim")
    sina_raw,_=native._artifact(files["sina"],bound,url=native.SINA_URL.format(symbol=symbol),params={},end=end,now=now)
    sina=native.parse_sina_total_history(sina_raw,symbol=symbol,observed_session=end,allowed_future_plans=[])
    tail=[r for r in sina["records"] if d<native.day(r["provider_date"])<=t]
    prior=[r for r in sina["records"] if native.day(r["provider_date"])<=d]
    require(len(tail)==1 and tail[0]["provider_date"]==end and native.amount(tail[0]["total_shares_10k"])==after
            and prior and native.amount(prior[-1]["total_shares_10k"])==before, "Sina exact known plan/prior boundary")
    quote_raw,quote_receipt=native._artifact(files["quote"],bound,url=native.QUOTE_URL,params=native.quote_params(symbol),end=end,now=now)
    quote=json.loads(quote_raw,parse_float=Decimal);q=quote.get("data")
    require(quote.get("rc")==0 and isinstance(q,dict) and q.get("f57")==symbol and native.amount(q.get("f84"))==after*10000,
            "quote known disagreement/issuer")
    require(native.integer(q.get("f86"))>0,"quote close clock missing")
    for key in ("f86","f124"):
        value=native.integer(q.get(key)) if key=="f86" or q.get(key) is not None else 0
        if value:
            stamp=datetime.fromtimestamp(value,timezone.utc)
            require(stamp.astimezone(timezone(native.timedelta(hours=8))).date()==t
                    and datetime.fromisoformat(end+"T15:00:00+08:00")<=stamp<=native.clock(quote_receipt["http_complete"]),
                    "quote target-close clock")
    listing,_=_sse(files["sse_listing"],bound,LIST_PARAMS,now)
    issuer=listing["result"][0]
    require(type(listing["pageHelp"]["total"]) is int and listing["pageHelp"]["total"]==1 and listing["pageHelp"]["data"]==listing["result"]
            and issuer.get("COMPANY_CODE")==symbol and issuer.get("A_STOCK_CODE")==symbol
            and issuer.get("B_STOCK_CODE")=="-" and issuer.get("FULL_NAME")=="江苏赛福天集团股份有限公司", "SSE issuer identity")
    capital,receipt=_sse(files["sse_capital"],bound,CAPITAL_PARAMS,now)
    expected=json.loads((Path(root)/PREFIX/"sse_site_capital_structure.raw").read_bytes())["result"][0]
    require(capital["result"][0]==expected and expected["TRADE_DATE"]=="20261104"
            and native.amount(expected["TOTAL_DOMESTIC_VOL"])==before
            and native.amount(expected["A_LIMIT_VOL"])+native.amount(expected["TOTAL_UNLIMIT_VOL"])==before,
            "SSE exact approved daily capital snapshot facts")
    return {"kind":KIND,"symbol":symbol,"query_end":end,"previous_day":previous_day,
            "total_shares_10k":str(before),"unchanged_observations":True,"selected_source":"SSE dated capital structure",
            "official_data_date":end,"official_observation_complete":receipt["http_complete"],
            "conflicting_commercial_total_shares_10k":str(after),"source_conflict_disclosed":True,
            "legal_cancellation_completion_not_claimed":True,"review_sha256":REVIEW_SHA,"approval_sha256":APPROVAL_SHA}


def verify_manifest(manifest, **kwargs):
    facts=_verified_facts(manifest,**kwargs)
    require(manifest.get("source_selection_disclosure")==facts,"verified conflict disclosure missing or changed")
    return facts


def collect_override(*, root, source, directory, manifest, symbol, previous_day, end, candidate):
    from scripts import share_unchanged_tail as native
    approval(root,symbol=symbol,previous_day=previous_day,end=end,candidate=candidate)
    bucket=source/"providers"/KIND/symbol/uuid.uuid4().hex;bucket.mkdir(parents=True)
    manifest={**manifest,"schema":KIND,"files":dict(manifest["files"]),"review_sha256":REVIEW_SHA,"approval_sha256":APPROVAL_SHA}
    for key,params in (("sse_listing",LIST_PARAMS),("sse_capital",CAPITAL_PARAMS)):
        reference,_=native._save_public_observation(directory=bucket,source=source,url=SSE_URL,params=params,
            purpose=key,end=end,referer=SSE_REFERER)
        manifest["files"][key]=reference
    def bound(name,digest):
        path=(source/name).resolve();require(path.is_relative_to(source.resolve()),"source path escapes")
        raw=path.read_bytes();require(hashlib.sha256(raw).hexdigest()==digest,"source digest differs");return raw
    verified=_verified_facts(manifest,symbol=symbol,previous_day=previous_day,end=end,candidate=candidate,bound=bound,root=root)
    manifest["source_selection_disclosure"]=verified
    verify_manifest(manifest,symbol=symbol,previous_day=previous_day,end=end,candidate=candidate,bound=bound,root=root)
    return manifest


def disclosures(root,expected_date):
    from scripts.share_change_source import verify_share_cache
    if expected_date == "2026-11-05":
        from scripts.share_unchanged_tail import _approved_unchanged_event
        source = Path(root)/".microcap_index_cache/share_sources"
        state_path = source/"301288.json"
        if not state_path.is_file():
            return []
        state = json.loads(state_path.read_bytes())
        require(state.get("query_end") == expected_date, "301288 disclosure source day differs")
        proof = verify_share_cache(root=root,symbol="301288",as_of=expected_date)
        candidate = (Path(root)/".microcap_index_cache/share_change/301288.csv").read_bytes()
        from datetime import datetime, timezone
        event = _approved_unchanged_event(root=root,symbol="301288",previous_day="2026-11-04",
                                         end=expected_date,candidate=candidate,now=datetime.now(timezone.utc))
        return [{"symbol":"301288","signal_date":expected_date,"review_sha256":event["review_sha256"],
                 "text":event["disclosure"],"source_files":proof["source_files"],
                 "selection":{k:v for k,v in event.items() if k!="row"}}]
    source=Path(root)/".microcap_index_cache/share_sources";path=source/"603028.json"
    if not path.is_file():
        return []
    state=json.loads(path.read_bytes())
    if state.get("kind")!=KIND or state.get("query_end")!=expected_date:
        return []
    require(state["query_end"]==expected_date=="2026-11-04","disclosure signal/source day differs")
    proof=verify_share_cache(root=root,symbol="603028",as_of=expected_date)
    ref=state["evidence_manifest"];manifest=json.loads((source/ref["path"]).read_bytes())
    return [{"symbol":"603028","signal_date":expected_date,"review_sha256":REVIEW_SHA,
             "text":"603028股本来源存在分歧：本次按用户授权采用上交所标注2026-11-04的28514.52万股；东方财富历史、新浪历史及quote为28483.52万股，与已登记注销计划值相同。股本历史CSV原字节保持不变，不据此宣称注销已完成。",
             "source_files":proof["source_files"],"selection":manifest["source_selection_disclosure"]}]


if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--root",type=Path,required=True);p.add_argument("--expected-date",required=True);p.add_argument("--out",type=Path,required=True);a=p.parse_args()
    value={"schema":"verified_share_source_disclosures_v1","signal_date":a.expected_date,"native_verified":True,"disclosures":disclosures(a.root,a.expected_date)}
    a.out.parent.mkdir(parents=True,exist_ok=True);a.out.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding="utf-8");print(json.dumps({"disclosures":len(value["disclosures"])}))
