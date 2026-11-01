"""Shared formal stock-return loader for full replay and daily continuation."""
from __future__ import annotations

from pathlib import Path
import datetime as dt
import hashlib
import json
import io

import pandas as pd

from scripts.exchange_calendar import sessions_for_day
from scripts.stock_return_basis_store import load_or_build, _extended
from scripts.stock_return_basis import build_ratio_return_basis, validate_cached_candidate, RETURN_BASIS_REVISION
from scripts.tradeability_evidence import enrich_price_with_trade_status


class ReturnBasisError(ValueError):
    """A missing or contradictory basis must stop the universe build."""


def load_return_prices(*, root, symbol, raw_path, master, as_of, allow_network=False):
    """Require an explicit lifecycle and a verified complete ratio basis.

    Raw prices remain authoritative for capitalization and tradeability. This
    loader never uses the legacy additive qfq directory or raw-return fallback.
    """
    symbol = str(symbol).zfill(6)
    if not {"symbol", "list_date", "delist_date"}.issubset(master.columns):
        raise ReturnBasisError("Return basis requires the historical security master")
    rows = master.loc[master.symbol.astype(str).str.zfill(6).eq(symbol)]
    if len(rows) != 1 or pd.isna(rows.iloc[0].list_date):
        raise ReturnBasisError(f"Missing or ambiguous return-basis lifecycle: {symbol}")
    row = rows.iloc[0]
    cutoff = pd.Timestamp(as_of).normalize()
    try:
        sessions = [day.isoformat() for day in sessions_for_day(cutoff.date())]
        frame, manifest = load_or_build(
            basis_root=Path(root) / ".microcap_index_cache" / "return_basis",
            symbol=symbol, raw_path=raw_path, as_of=cutoff.strftime("%Y-%m-%d"),
            sessions=sessions, list_date=pd.Timestamp(row.list_date).strftime("%Y-%m-%d"),
            delist_date=None if pd.isna(row.delist_date) else pd.Timestamp(row.delist_date).strftime("%Y-%m-%d"),
            allow_network=allow_network,
        )
    except Exception as exc:
        raise ReturnBasisError(f"Unverified ratio return basis for {symbol}: {exc}") from exc
    frame.attrs["return_basis_manifest"] = manifest
    return frame


def _consumption_requirements(root, target, requested):
    """Derive frozen/full and live-tail requirements from delivered authorities."""
    prefix=root/"outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy"
    path=Path(str(prefix)+"_meta.json")
    if not path.exists():return target,set(requested),{}
    payload=path.read_bytes();meta=json.loads(payload);proof=meta.get("return_basis_consumption")
    if proof is None:return target,set(requested),{path:payload}
    if proof.get("revision")!="stock_return_consumption_v1":raise ReturnBasisError("Unknown return consumption revision")
    full=proof.get("full_rebuild_anchor")
    if not isinstance(full,str) or str(pd.Timestamp(full).date())!=full or full>target:raise ReturnBasisError("Invalid full rebuild anchor")
    tails=proof.get("tail_extensions")
    if not isinstance(tails,list):raise ReturnBasisError("Missing tail consumption list")
    stable={path:payload};cursor=full;latest=set()
    session_set={day.isoformat() for day in sessions_for_day(pd.Timestamp(target).date())}
    if full not in session_set or target not in session_set:raise ReturnBasisError("Consumption anchors are not exchange sessions")
    if tails:
        proxy=root/'outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv';body=proxy.read_bytes();stable[proxy]=body
        index=pd.read_csv(proxy);days=pd.to_datetime(index['date'],errors='raise').dt.strftime('%Y-%m-%d')
        expected={day for day in session_set if full<day<=target}
        actual=days[(days>full)&(days<=target)]
        if actual.duplicated().any() or set(actual)!=expected:raise ReturnBasisError("Proxy tail lacks complete exchange-session coverage")
    for tail in tails:
        start=tail.get("start_exclusive");end=tail.get("end")
        if start!=cursor or not isinstance(end,str) or not start<end<=target or start not in session_set or end not in session_set:raise ReturnBasisError("Consumption tail is not contiguous")
        frames={}
        for name in ("effective_members","members","turnover"):
            p=Path(str(prefix)+"_"+name+".csv");body=p.read_bytes();stable[p]=body
            key={"effective_members":"effective_members_sha256","members":"proxy_members_sha256","turnover":"proxy_turnover_sha256"}[name]
            if hashlib.sha256(body).hexdigest()!=tail.get(key):raise ReturnBasisError("Consumption authority hash mismatch")
            frames[name]=pd.read_csv(p,dtype={"symbol":str})
        effective=frames["effective_members"]
        if not {"as_of_date","rank","symbol"}.issubset(effective.columns):raise ReturnBasisError("Invalid effective authority schema")
        dates=set(effective.as_of_date.astype(str));members=effective.symbol.astype(str).str.zfill(6).tolist()
        if len(dates)!=1 or next(iter(dates))>start or len(members)!=100 or len(set(members))!=100 or set(pd.to_numeric(effective['rank']))!=set(range(1,101)):
            raise ReturnBasisError("Effective authority is not a valid dated 100-member set")
        if set(tail.get("symbols",[]))!=set(members) or len(tail.get("symbols",[]))!=100 or not set(members).issubset(requested):raise ReturnBasisError("Consumption symbols disagree with independent authority")
        turnover=frames["turnover"];observed=[];turnover_days={}
        if turnover.empty or frames['members'].empty:raise ReturnBasisError("Consumption turnover/member authority is empty")
        for column in ("execution_date","effective_date","return_start_date"):
            if column not in turnover:raise ReturnBasisError("Turnover lacks execution dates")
            parsed=pd.to_datetime(turnover[column],errors="raise")
            if parsed.isna().any():raise ReturnBasisError("Turnover has unproven execution dates")
            days=parsed.dt.strftime("%Y-%m-%d")
            if not set(days).issubset(session_set):raise ReturnBasisError("Turnover execution date is not an exchange session")
            turnover_days[column]=days
            if column!="return_start_date":
                if ((days>start)&(days<=end)).any():raise ReturnBasisError("Tail crosses executed rebalance")
                observed.extend(days[days<=start].tolist())
        # Close execution is already in the anchor's effective membership;
        # its first return may legitimately be the next session in this tail.
        first_return=turnover_days["return_start_date"]
        newly_consumed=(first_return>start)&(first_return<=end)
        executed=turnover_days["execution_date"];became_effective=turnover_days["effective_date"]
        if ((first_return<executed)|(first_return<became_effective)).any():raise ReturnBasisError("Turnover return starts before execution/effectiveness")
        if (newly_consumed&((executed>start)|(became_effective>start))).any():raise ReturnBasisError("Tail return belongs to unexecuted rebalance")
        if observed and next(iter(dates))<max(observed):raise ReturnBasisError("Effective members predate latest execution")
        planned=pd.to_datetime(frames['members']['rebalance_date'],errors='raise').dt.strftime('%Y-%m-%d')
        if ((planned>start)&(planned<=end)).any():raise ReturnBasisError("Tail crosses new saved rebalance")
        latest.update(members);cursor=end
    if cursor!=target:raise ReturnBasisError("Consumption proof does not cover proxy anchor")
    return full,(latest if tails else set(requested)),stable


def verify_return_basis_transport(root, anchor, symbols):
    """Read-only whole-universe source, lineage and exact reconstruction gate.

    Does not call the writing store, create locks, repair files or infer source
    freshness. All symbols must pass, including delisted securities. A legacy
    bundle without this revision is rejected by this verifier itself.
    """
    root=_extended(root);cache=root/".microcap_index_cache";basis=cache/"return_basis"
    target=pd.Timestamp(anchor).strftime("%Y-%m-%d")
    close=dt.datetime.fromisoformat(target+"T15:00:00+08:00");now=dt.datetime.now(dt.timezone.utc)
    if close>now:raise ReturnBasisError("Transport anchor is not close-confirmed")
    def sha(payload):return hashlib.sha256(payload).hexdigest()
    master_path=cache/"security_master.csv";master_bytes=master_path.read_bytes()
    master=pd.read_csv(master_path,dtype={"symbol":str})
    required={"symbol","list_date","delist_date"}
    if not required.issubset(master.columns):raise ReturnBasisError("Transport master lacks lifecycle fields")
    sessions=[day.isoformat() for day in sessions_for_day(pd.Timestamp(target).date())]
    requested=[str(s).zfill(6) for s in symbols]
    if not requested or len(set(requested))!=len(requested):raise ReturnBasisError("Transport symbols empty or duplicated")
    full_anchor,live_symbols,authorities=_consumption_requirements(root,target,set(requested))
    verified=[];failures={}
    for symbol in requested:
        try:
            if (basis/"locks"/f"{symbol}.lock").exists():raise ValueError("Concurrent basis writer lock exists")
            mp=basis/"manifests"/f"{symbol}.json";manifest_bytes=mp.read_bytes();manifest=json.loads(manifest_bytes)
            if manifest.get("return_basis_revision")!=RETURN_BASIS_REVISION:raise ValueError("Missing/current basis revision mismatch")
            certified=manifest.get("as_of")
            if not isinstance(certified,str) or not full_anchor<=certified<=target or (symbol in live_symbols and certified!=target):
                raise ValueError("Basis does not cover independently proven consumption dates")
            symbol_close=dt.datetime.fromisoformat(certified+"T15:00:00+08:00")
            if manifest.get("source_package_verified_through") is not None:raise ValueError("Research freshness attestation cannot certify formal transport")
            rows=master.loc[master.symbol.astype(str).str.zfill(6).eq(symbol)]
            if len(rows)!=1:raise ValueError("Missing or ambiguous master symbol")
            row=rows.iloc[0];listed=pd.Timestamp(row.list_date).strftime("%Y-%m-%d")
            delisted=None if pd.isna(row.delist_date) or not str(row.delist_date).strip() else pd.Timestamp(row.delist_date).strftime("%Y-%m-%d")
            folder=basis/"sources"/symbol;bodies={};receipts={}
            for kind in ("qfq","hfq"):
                bodies[kind]=(folder/f"{kind}.js").read_bytes();receipts[kind]=json.loads((folder/f"{kind}.json").read_text(encoding="utf-8"))
                stamp=receipts[kind].get("retrieved_at_utc")
                if not stamp:raise ValueError("Source receipt has no real timestamp")
                fetched=dt.datetime.fromisoformat(stamp.replace("Z","+00:00"))
                if fetched.tzinfo is None or fetched<symbol_close or fetched>now:raise ValueError("Stale/future/naive source timestamp")
            raw=(cache/"prices_raw"/f"{symbol}.csv").read_bytes()
            status_root=cache/'trade_status';status_path=status_root/(symbol+'.json');status_bytes=status_path.read_bytes();status=json.loads(status_bytes)
            bound={status_path:status_bytes}
            def collect_bound(value):
                if isinstance(value,dict):
                    if 'path' in value and 'sha256' in value:
                        path=(status_root/value['path']).resolve()
                        if not path.is_relative_to(status_root.resolve()):raise ValueError('Trade status nested path escapes root')
                        body=path.read_bytes()
                        if sha(body)!=value['sha256']:raise ValueError('Trade status nested source hash mismatch')
                        bound[path]=body
                    for child in value.values():collect_bound(child)
                elif isinstance(value,list):
                    for child in value:collect_bound(child)
            collect_bound(status)
            raw_frame=pd.read_csv(io.BytesIO(raw),parse_dates=['date'])
            status_sessions=pd.DatetimeIndex([day for day in sessions if day<=certified])
            enriched=enrich_price_with_trade_status(root=root,symbol=symbol,raw_path=cache/'prices_raw'/(symbol+'.csv'),price=raw_frame,master=master,trading_dates=status_sessions,require_evidence=True)
            if symbol in live_symbols:
                consumed={day for day in sessions if full_anchor<day<=target}
                if not consumed.issubset(set(enriched.attrs.get('verified_trade_status_dates',[]))):raise ValueError('Tail consumption lacks explicit verified trade-status dates')
            frame,expected=build_ratio_return_basis(symbol=symbol,raw_csv=raw,qfq_source=bodies["qfq"],hfq_source=bodies["hfq"],
                qfq_receipt=receipts["qfq"],hfq_receipt=receipts["hfq"],as_of=certified,sessions=sessions,
                list_date=listed,delist_date=delisted,anchor_date=manifest["anchor_date"])
            expected["source_package_verified_through"]=None
            expected["source_receipts_sha256"]={kind:sha(json.dumps(receipts[kind],sort_keys=True,ensure_ascii=False).encode()) for kind in receipts}
            candidate=(basis/"prices_adj"/f"{symbol}.csv").read_bytes()
            validate_cached_candidate(candidate,manifest,expected_manifest=expected)
            bundle=basis/"bundles"/symbol/sha(json.dumps(manifest,sort_keys=True).encode())
            snapshot=(bundle/"raw.csv").read_bytes()
            if sha(snapshot)!=manifest["raw_sha256"] or snapshot!=raw:
                raise ValueError("Missing/mismatched complete raw snapshot for future daily continuation")
            if (cache/"prices_raw"/f"{symbol}.csv").read_bytes()!=raw:
                raise ValueError("Raw source changed during read-only transport audit")
            for kind in receipts:
                if (folder/f"{kind}.js").read_bytes()!=bodies[kind] or json.loads((folder/f"{kind}.json").read_text(encoding="utf-8"))!=receipts[kind]:
                    raise ValueError("Factor source changed during read-only transport audit")
            if mp.read_bytes()!=manifest_bytes or (basis/"locks"/f"{symbol}.lock").exists():raise ValueError("Basis changed during read-only transport audit")
            if (basis/"prices_adj"/f"{symbol}.csv").read_bytes()!=candidate or (bundle/"raw.csv").read_bytes()!=snapshot:
                raise ValueError("Candidate/raw snapshot changed during read-only transport audit")
            if any(path.read_bytes()!=body for path,body in bound.items()):raise ValueError('Trade status nested source changed during audit')
            verified.append(dict(symbol=symbol,manifest_sha256=sha(manifest_bytes),candidate_sha256=sha(candidate),raw_sha256=sha(raw),rows=len(frame),last=expected["last"],trade_status_files={path.relative_to(cache).as_posix():sha(body) for path,body in bound.items()}))
        except Exception as exc:failures[symbol]=f"{type(exc).__name__}: {exc}"
    if failures:
        sample="; ".join(f"{s}: {error}" for s,error in list(failures.items())[:10])
        raise ReturnBasisError(f"Transport ratio basis failed {len(failures)}/{len(requested)} symbols: {sample}")
    if master_path.read_bytes()!=master_bytes:raise ReturnBasisError("Master changed during transport audit")
    if any(p.read_bytes()!=body for p,body in authorities.items()):raise ReturnBasisError("Consumption authorities changed during audit")
    return dict(return_basis_revision=RETURN_BASIS_REVISION,anchor=target,verified_count=len(verified),
        master_sha256=sha(master_bytes),symbols=verified,read_only=True)
