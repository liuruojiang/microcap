"""Exact D4 inputs permit only the independently reviewed metadata-tail successor."""
import copy
import hashlib
import json
from pathlib import Path
import pytest
from scripts import realtime_state_bundle as state
from scripts.stock_input_contract import SOURCE_FILES, source_fingerprint

ROOT = Path(__file__).resolve().parents[1]

@pytest.fixture
def exact_successor_identities():
    # This existing authenticated fixture is byte-value identical to the original
    # proxy metadata extracted from the verified 641... D4 whole bundle.
    old = json.loads((ROOT / 'tests/fixtures/validation_source_identity_20261104.json').read_text(encoding='utf8'))['stock_input_evidence_fingerprint']
    current = copy.deepcopy(old)
    current['code'] = source_fingerprint(ROOT)
    # Keep the independently accepted 6fa transition's exact historical targets.
    for name, pair in state.APPROVED_603028_SOURCE_REVIEW['changes'].items():
        if name not in state.SEMANTIC_TAIL_SOURCE_REVIEW['changes']:
            current['code'][name] = pair[0]
    return old, current


def test_exact_successor_real_d4_identity_and_actual_source(exact_successor_identities):
    old, current = exact_successor_identities
    review = state.SEMANTIC_TAIL_SOURCE_REVIEW
    assert old['file_count'] == 16315
    assert old['sha256'] == '188aab4e5aea57f2cc230c22fee462fa96533e53edb02458c32997f1411afe3a'
    assert len(old['code']) == len(SOURCE_FILES) == 18
    assert hashlib.sha256(json.dumps(old,sort_keys=True,separators=(',',':')).encode()).hexdigest() == review['stock_fingerprint_sha256']
    changed = {n for n in old['code'] if old['code'][n] != current['code'][n]}
    assert changed == set(review['changes'])
    assert len(changed) == 4
    assert all((old['code'][n],current['code'][n]) == tuple(review['changes'][n]) for n in changed)
    before = copy.deepcopy((old,current))
    assert state.stock_input_evidence_matches_validated_source(old,current)
    assert (old,current) == before
    assert hashlib.sha256(json.dumps(state.VALIDATION_SOURCE_REVIEW,sort_keys=True,separators=(',',':')).encode()).hexdigest() == '687ac3273207ab90236ba6505647452fd574e635c39483352ca8b25d7e3c0293'


@pytest.mark.parametrize('side',['old','current','both'])
@pytest.mark.parametrize('field',['data','count','missing','extra','unknown','malformed'])
def test_exact_successor_rejects_all_other_identity_changes(exact_successor_identities,side,field):
    old,current=exact_successor_identities
    for value in ([old,current] if side=='both' else [old if side=='old' else current]):
        if field=='data': value['sha256']='0'*64
        elif field=='count': value['file_count']+=1
        elif field=='missing': value['code'].pop('scripts/st_metadata_refresh.py')
        elif field=='extra': value['code']['unreviewed.py']='0'*64
        elif field=='unknown': value['code']['scripts/share_change_source.py']='0'*64
        else: value['file_count']=True
    assert not state.stock_input_evidence_matches_validated_source(old,current)


@pytest.mark.parametrize('name', ['scripts/st_history_scope.py','scripts/st_notice_incremental.py','scripts/st_notice_refresh.py','scripts/st_metadata_refresh.py'])
@pytest.mark.parametrize('fault',['partial','unknown'])
def test_exact_successor_requires_all_four_exact_targets(exact_successor_identities,name,fault):
    old,current=exact_successor_identities
    current['code'][name]=old['code'][name] if fault=='partial' else 'f'*64
    # The prior already-approved three-source transition remains independently
    # valid. Omitting only the new metadata target is that old authorization.
    if fault=='partial' and name=='scripts/st_metadata_refresh.py':
        assert state.stock_input_evidence_matches_validated_source(old,current)
        assert {n for n in old['code'] if old['code'][n]!=current['code'][n]} == set(state.VALIDATION_SOURCE_REVIEW['changes']) & set(SOURCE_FILES)
    else:
        assert not state.stock_input_evidence_matches_validated_source(old,current)


def test_exact_successor_cannot_chain_reverse_or_authorize_future_code(exact_successor_identities):
    old,current=exact_successor_identities
    future=copy.deepcopy(current);future['code']['scripts/st_metadata_refresh.py']='f'*64
    assert not state.stock_input_evidence_matches_validated_source(current,old)
    assert not state.stock_input_evidence_matches_validated_source(current,future)
    assert not state.stock_input_evidence_matches_validated_source(old,future)


def test_exact_successor_never_changes_historical_review_authority():
    assert 'scripts/st_metadata_refresh.py' not in state.validation_source_review_metadata()['changes']
    with pytest.raises(ValueError,match='Unreviewed validation source'):
        state.validation_source_review(ROOT)


def test_approved_301288_successor_actual_source_keeps_exact_data_identity(exact_successor_identities):
    old, current = exact_successor_identities
    current['code'] = copy.deepcopy(old['code'])
    review = state.APPROVED_301288_SOURCE_REVIEW
    current['code'].update({name:pair[1] for name,pair in review['changes'].items()})
    changed = {n for n in old['code'] if old['code'][n] != current['code'][n]}
    assert changed == set(review['changes']) and len(changed) == 6
    assert all((old['code'][n], current['code'][n]) == tuple(review['changes'][n]) for n in changed)
    assert state.stock_input_evidence_matches_validated_source(old, current)
    for name in changed:
        bad = copy.deepcopy(current); bad['code'][name] = '0'*64
        assert not state.stock_input_evidence_matches_validated_source(old, bad)
    current['sha256'] = '0'*64
    assert not state.stock_input_evidence_matches_validated_source(old,current)


def test_approved_603028_actual_source_and_exact_d4_unchanged_inputs(exact_successor_identities):
    old,current=exact_successor_identities
    current['code']=source_fingerprint(ROOT)
    review=state.APPROVED_603028_SOURCE_REVIEW
    changed={name for name in old['code'] if old['code'][name]!=current['code'][name]}
    assert changed==set(review['changes']) and len(changed)==7
    assert all((old['code'][n],current['code'][n])==tuple(review['changes'][n]) for n in changed)
    assert state.stock_input_evidence_matches_validated_source(old,current)
    for name in changed:
        bad=copy.deepcopy(current);bad['code'][name]='0'*64
        assert not state.stock_input_evidence_matches_validated_source(old,bad)
    current['sha256']='0'*64
    assert not state.stock_input_evidence_matches_validated_source(old,current)
