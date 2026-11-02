import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from scripts.stock_return_basis import build_ratio_return_basis, validate_cached_candidate

F = Path(__file__).parent / "fixtures/stock_return_basis_real"


def inputs():
    return dict(symbol="000418", raw_csv=(F/"000418_raw.csv").read_bytes(),
        qfq_source=(F/"qfq.js").read_bytes(), hfq_source=(F/"hfq.js").read_bytes(),
        qfq_receipt=json.loads((F/"qfq.json").read_text()),
        hfq_receipt=json.loads((F/"hfq.json").read_text()),
        as_of="2012-06-12", sessions=["2012-06-08", "2012-06-11", "2012-06-12"],
        list_date="1993-11-29")


def test_real_cash_dividend_and_fixed_anchor_prefix():
    kw=inputs(); full, manifest=build_ratio_return_basis(**kw)
    assert full.close_adj.iloc[1]/full.close_adj.iloc[0]-1 == pytest.approx(9.91/9.67-1, abs=1e-12)
    kw["as_of"]="2012-06-11"; kw["anchor_date"]=manifest["anchor_date"]
    early, _=build_ratio_return_basis(**kw)
    pd.testing.assert_frame_equal(early,full.iloc[:2])
    assert manifest["future_factor_rows_excluded"]>0
    assert not manifest["cash_dividend_wealth_accounting"]


def test_future_exchange_calendar_extension_preserves_certified_manifest():
    kw=inputs();before,manifest=build_ratio_return_basis(**kw)
    kw['sessions'] += ['2012-06-13','2012-06-14','2013-01-04']
    after,extended=build_ratio_return_basis(**kw)
    pd.testing.assert_frame_equal(before,after)
    assert manifest==extended


def test_real_raw_tail_update_preserves_anchor_and_returns():
    kw=inputs(); full, manifest=build_ratio_return_basis(**kw)
    lines=kw["raw_csv"].decode().splitlines(); kw["raw_csv"]=("\n".join(lines[:3])+"\n").encode()
    short, previous=build_ratio_return_basis(**kw)
    kw["raw_csv"] = inputs()["raw_csv"]; kw["anchor_date"]=previous["anchor_date"]
    updated, after=build_ratio_return_basis(**kw)
    pd.testing.assert_frame_equal(updated.iloc[:2],short)
    assert previous["raw_sha256"] != after["raw_sha256"]
    with pytest.raises(ValueError,match="provenance"):
        validate_cached_candidate(short.to_csv(index=False,lineterminator="\r\n").encode(),previous,expected_manifest=after)


@pytest.mark.parametrize("change", ["status", "hash", "symbol", "revision", "raw_negative", "anchor"])
def test_fail_closed_for_wrong_identity_and_basis(change):
    kw=inputs()
    if change=="status":kw["qfq_receipt"]["status"]=503
    if change=="hash":kw["hfq_source"]+=b" "
    if change=="symbol":kw["symbol"]="000559"
    if change=="revision":kw["return_basis_revision"]="legacy_raw"
    if change=="raw_negative":kw["raw_csv"]=kw["raw_csv"].replace(b"9.87",b"-9.87")
    if change=="anchor":kw["anchor_date"]="2012-06-07"
    with pytest.raises(ValueError):build_ratio_return_basis(**kw)


def test_cache_rejects_changed_payload_and_missing_revision():
    out,m=build_ratio_return_basis(**inputs());data=out.to_csv(index=False,lineterminator="\r\n").encode()
    validate_cached_candidate(data,m,expected_manifest=m)
    with pytest.raises(ValueError):validate_cached_candidate(data+b"\n",m,expected_manifest=m)
    old=dict(m);old.pop("return_basis_revision")
    with pytest.raises(ValueError):validate_cached_candidate(data,old,expected_manifest=m)


def test_off_session_raw_is_explicitly_excluded_not_filled():
    kw=inputs();kw["raw_csv"]+=b"2012-06-10,9.87\n"
    out,m=build_ratio_return_basis(**kw)
    assert len(out)==3 and m["excluded_raw_dates"]==["2012-06-10"]


@pytest.mark.parametrize("mode", ["missing_anchor", "basis_disagreement", "duplicate_event"])
def test_rejects_incomplete_or_inconsistent_factor_history(mode):
    # Explicit corruption tests derived from the real source; not market evidence.
    kw=inputs();source=kw["hfq_source"].decode();prefix,body=source.split("=",1)
    obj,_=json.JSONDecoder().raw_decode(body.lstrip())
    if mode=="missing_anchor":obj["data"]=[r for r in obj["data"] if r["d"]>="2012-06-11"]
    if mode=="basis_disagreement":
        for row in obj["data"]:
            if row["d"]=="2012-06-11":row["f"]=str(float(row["f"])*1.01)
    if mode=="duplicate_event":obj["data"].append(dict(obj["data"][0]))
    obj["total"]=len(obj["data"]);kw["hfq_source"]=(prefix+"="+json.dumps(obj)).encode()
    kw["hfq_receipt"]["sha256"]=hashlib.sha256(kw["hfq_source"]).hexdigest()
    with pytest.raises(ValueError):build_ratio_return_basis(**kw)
