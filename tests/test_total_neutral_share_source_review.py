"""Exact source contracts, not approval for changed historical data."""
import copy,json
from pathlib import Path
import pytest
from scripts import realtime_state_bundle as state,scoped_source_consumer as sources
from scripts.stock_input_contract import source_fingerprint

ROOT=Path(__file__).resolve().parents[1]


def identities():
    old=json.loads((ROOT/'tests/fixtures/native_scoped_n9_stock_fingerprint_20261110.json').read_bytes())
    current=copy.deepcopy(old);current['code']=source_fingerprint(ROOT)
    return old,current


def test_actual_native_n9_bytes_accept_only_the_exact_total_neutral_source_successor():
    old,current=identities();before=copy.deepcopy((old,current))
    assert state.stock_input_evidence_matches_validated_source(old,current)
    assert {n for n in old['code'] if old['code'][n]!=current['code'][n]}=={'scripts/share_change_source.py'}
    assert (old,current)==before


@pytest.mark.parametrize('fault',['data','count','other_source','unknown_share','partial','missing'])
def test_new_source_review_does_not_waive_data_or_other_code_changes(fault):
    old,current=identities()
    if fault=='data':old['sha256']=current['sha256']='0'*64
    elif fault=='count':old['file_count']=current['file_count']=1
    elif fault=='other_source':current['code']['scripts/stock_return_pipeline.py']='f'*64
    elif fault=='unknown_share':current['code']['scripts/share_change_source.py']='f'*64
    elif fault=='partial':current['code']['scripts/share_change_source.py']='3bd6eab8a34f3f032fb528cff7457b3133f9c177e6fdaea4228f1af4b6432dab'
    else:current['code'].pop('scripts/share_change_source.py')
    assert not state.stock_input_evidence_matches_validated_source(old,current)


def test_old_source_packages_keep_exact_composed_reader_and_share_contract():
    current=source_fingerprint(ROOT);old=copy.deepcopy(current)
    old['scripts/stock_return_pipeline.py']=sources.FORWARD_CONSUMPTION_READER[0]
    old['scripts/st_notice_refresh.py']=sources.ARCHIVED_PROOF_READER[0]
    old['scripts/share_change_source.py']=sources.TOTAL_NEUTRAL_SHARE_READER[0]
    manifest=dict(runtime_sha=sources.PRODUCER,producer_context=dict(runtime_sha=sources.PRODUCER,native_sources=old))
    assert sources.validate_source_contract(ROOT,manifest)==current
    manifest['producer_context']['native_sources']['scripts/share_change_source.py']='f'*64
    with pytest.raises(ValueError,match='Unreviewed'):sources.validate_source_contract(ROOT,manifest)


def test_frozen_d4_source_approval_is_not_expanded():
    value=json.loads((ROOT/'tests/fixtures/validation_source_identity_20261104.json').read_bytes())
    old=value.get('stock_input_evidence_fingerprint',value)
    current=copy.deepcopy(old);current['code']=source_fingerprint(ROOT)
    assert not state.stock_input_evidence_matches_validated_source(old,current)
