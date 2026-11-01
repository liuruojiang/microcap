"""Offline import of already collected real factor bytes; never forge timestamps."""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path

from scripts.stock_return_basis import _factor


def import_sources(source_root, basis_root, as_of):
    source_root=Path(source_root);basis_root=Path(basis_root)
    close=dt.datetime.fromisoformat(as_of+"T15:00:00+08:00");now=dt.datetime.now(dt.timezone.utc)
    rows=[]
    for folder in sorted(source_root.iterdir()):
        if not folder.is_dir() or len(folder.name)!=6 or not folder.name.isascii() or not folder.name.isdigit():continue
        row=dict(symbol=folder.name,formal_source_freshness_eligible=False)
        try:
            pair={};fresh=True
            for kind in ("qfq","hfq"):
                body=(folder/f"{kind}.js").read_bytes();receipt_bytes=(folder/f"{kind}.json").read_bytes();r=json.loads(receipt_bytes)
                _factor(folder.name,kind,body,r)
                stamp=r.get("retrieved_at_utc")
                if not stamp:fresh=False
                else:
                    t=dt.datetime.fromisoformat(stamp.replace("Z","+00:00"))
                    if t.tzinfo is None or t>now:raise ValueError("Invalid/future source timestamp")
                    if t<close:fresh=False
                pair[kind]=(body,receipt_bytes)
                row[kind+"_sha256"]=hashlib.sha256(body).hexdigest()
                row[kind+"_receipt_sha256"]=hashlib.sha256(receipt_bytes).hexdigest()
            dest=basis_root/"sources"/folder.name
            for kind,(body,rb) in pair.items():
                for name,payload in ((kind+".js",body),(kind+".json",rb)):
                    path=dest/name
                    if path.exists() and path.read_bytes()!=payload:raise ValueError("Destination source conflict; explicit migration required")
            dest.mkdir(parents=True,exist_ok=True)
            for kind,(body,rb) in pair.items():
                (dest/f"{kind}.js").write_bytes(body);(dest/f"{kind}.json").write_bytes(rb)
            row.update(status="imported",formal_source_freshness_eligible=fresh)
            if not fresh:row["freshness_blocker"]="Missing or stale original receipt timestamp; no inferred replacement"
        except Exception as exc:row.update(status="blocked",error=str(exc))
        rows.append(row)
    result=dict(as_of=as_of,network_requests=0,source_root=str(source_root.resolve()),basis_root=str(basis_root.resolve()),rows=rows)
    basis_root.mkdir(parents=True,exist_ok=True)
    (basis_root/"source_import_manifest.json").write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")
    return result


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--source-root",required=True);p.add_argument("--basis-root",required=True);p.add_argument("--as-of",required=True);a=p.parse_args()
    result=import_sources(a.source_root,a.basis_root,a.as_of)
    print(json.dumps(dict(symbols=len(result["rows"]),imported=sum(r["status"]=="imported" for r in result["rows"]),fresh=sum(r["formal_source_freshness_eligible"] for r in result["rows"])),ensure_ascii=False))
