import json
from pathlib import Path

import pytest

from scripts.stock_return_basis_store import load_or_build

F=Path(__file__).parent/"fixtures/stock_return_basis_real"


def args(tmp_path):
    return dict(basis_root=tmp_path,symbol="000418",raw_path=F/"000418_raw.csv",
        as_of="2012-06-12",sessions=["2012-06-08","2012-06-11","2012-06-12"],
        list_date="1993-11-29",source_dir=F,source_verified_through="2012-06-12",allow_research_attestation=True)


def test_real_package_reuse_and_corruption_rejection(tmp_path):
    kw=args(tmp_path);f,m=load_or_build(**kw);again,m2=load_or_build(**kw)
    assert m==m2 and f.equals(again)
    p=tmp_path/"prices_adj/000418.csv";p.write_bytes(p.read_bytes()+b"\n")
    with pytest.raises(ValueError,match="provenance"):load_or_build(**kw)


def test_no_implicit_freshness_or_legacy_fallback(tmp_path):
    kw=args(tmp_path);kw["source_verified_through"]=None
    with pytest.raises(ValueError,match="freshness"):load_or_build(**kw)
    kw=args(tmp_path);p=tmp_path/"prices_adj/000418.csv";p.parent.mkdir();p.write_bytes(b"date,close_adj\n")
    with pytest.raises(ValueError,match="legacy"):load_or_build(**kw)


def test_stale_package_is_not_accepted(tmp_path):
    kw=args(tmp_path);kw["source_verified_through"]="2012-06-11"
    with pytest.raises(ValueError,match="stale"):load_or_build(**kw)


def test_changed_complete_raw_rebuild_preserves_anchor(tmp_path):
    kw=args(tmp_path)
    raw=tmp_path/"raw.csv";raw.write_bytes(b"date,close_raw\n2012-06-08,9.87\n2012-06-11,9.91\n")
    kw["raw_path"]=raw;short,m2=load_or_build(**kw)
    raw.write_bytes((F/"000418_raw.csv").read_bytes());full,m=load_or_build(**kw)
    assert m2["anchor_date"]==m["anchor_date"] and len(short)==2
    assert m2["raw_sha256"]!=m["raw_sha256"]
    raw.write_bytes(b"date,close_raw\n2012-06-08,9.87\n2012-06-11,9.91\n")
    with pytest.raises(ValueError,match="deletion"):load_or_build(**kw)


def test_historical_raw_change_cannot_hide_in_new_hash(tmp_path):
    kw=args(tmp_path);raw=tmp_path/"raw.csv";raw.write_bytes((F/"000418_raw.csv").read_bytes());kw["raw_path"]=raw
    load_or_build(**kw);raw.write_bytes(raw.read_bytes().replace(b"9.87",b"9.88"))
    with pytest.raises(ValueError,match="rewrite"):load_or_build(**kw)


def test_production_rejects_unsupported_attestation(tmp_path):
    kw=args(tmp_path);kw["allow_research_attestation"]=False
    with pytest.raises(ValueError,match="research-only"):load_or_build(**kw)


def test_parallel_symbol_is_locked(tmp_path):
    p=tmp_path/"locks/000418.lock";p.parent.mkdir();p.write_text("existing")
    with pytest.raises(RuntimeError,match="locked"):load_or_build(**args(tmp_path))
    assert p.read_text()=="existing"


def test_future_source_timestamp_is_rejected(tmp_path):
    import shutil
    folder=tmp_path/"source";shutil.copytree(F,folder)
    p=folder/"qfq.json";r=json.loads(p.read_text());r["retrieved_at_utc"]="2999-01-01T00:00:00+00:00";p.write_text(json.dumps(r))
    kw=args(tmp_path);kw["source_dir"]=folder
    with pytest.raises(ValueError,match="Future"):load_or_build(**kw)


def test_offline_source_import_preserves_original_receipts(tmp_path):
    import shutil
    from scripts.import_stock_return_basis_sources import import_sources
    source=tmp_path/"incoming";shutil.copytree(F,source/"000418")
    target=tmp_path/"store";report=import_sources(source,target,"2012-06-12")
    assert report["network_requests"]==0 and report["rows"][0]["status"]=="imported"
    assert not report["rows"][0]["formal_source_freshness_eligible"]
    assert (target/"sources/000418/qfq.json").read_bytes()==(F/"qfq.json").read_bytes()


def test_factor_rewrite_cannot_change_existing_adjusted_prefix(tmp_path,monkeypatch):
    import hashlib
    import scripts.stock_return_basis_store as store
    kw=args(tmp_path);load_or_build(**kw);original=store.build_ratio_return_basis
    def changed(**kwargs):
        frame,manifest=original(**kwargs)
        frame.loc[1,'close_adj']*=1.01
        manifest['candidate_sha256']=hashlib.sha256(frame.to_csv(index=False,lineterminator="\r\n").encode()).hexdigest()
        return frame,manifest
    monkeypatch.setattr(store,'build_ratio_return_basis',changed)
    before=(tmp_path/'prices_adj/000418.csv').read_bytes()
    with pytest.raises(ValueError,match='factor rewrite'):load_or_build(**kw)
    assert (tmp_path/'prices_adj/000418.csv').read_bytes()==before
