"""M8 source-only migration on the genuine frozen M3 economic/data identity.

Small transaction tests reuse the explicit native-gate spy in the predecessor
suite.  They do not claim a complete archive restore or a live D12 refresh.
"""
import copy
import json

import pytest

from scripts import scoped_archive_source_revalidation as a
from scripts import st_notice_boundary_20261112_0726 as st
from test_scoped_archive_source_revalidation import (
    ROOT, current_report, frozen, offline, transaction,
)


def test_v4_closes_the_actual_typed_quote_and_exact_st_source_inputs(frozen):
    original=json.loads(frozen['raw']('bundle-manifest.json'))
    changes=a.input_changes()
    assert len(changes)==201
    assert set(a.input_changes(revision=a.OBSERVATION_REVISION)) <= set(changes)
    assert set(st.SOURCE_FILES) <= set(changes)
    assert all(changes[name][0] is None for name in st.SOURCE_FILES)
    assert changes['scripts/st_notice_boundary_review.py'][0] == original['inputs']['scripts/st_notice_boundary_review.py']
    assert changes['scripts/scoped_source_consumer.py'][1] == 'a2c39c698edaf69a5cb9e667644b76fa72fd5959da84a528163eeab0982eec06'
    a.checked_source(ROOT)
    a.require_transition(original,current_report(original))


def test_actual_m8_stock_successor_preserves_the_exact_old_data_anchor(tmp_path,frozen):
    from scripts.stock_input_contract import source_fingerprint
    proxy=tmp_path/a.PROXY;proxy.parent.mkdir(parents=True)
    proxy.write_bytes(frozen['raw']('proxy.json'))
    recorded=copy.deepcopy(a.BASELINE['stock_fingerprint'])
    current={**recorded,'code':source_fingerprint(ROOT)}
    assert a.stock_compatible(recorded,current,tmp_path)
    assert {n for n in current['code'] if recorded['code'][n]!=current['code'][n]}==set(a.M8_STOCK_CHANGES)
    current['code']['scripts/share_unchanged_tail.py']=a.CHANGES['scripts/share_unchanged_tail.py'][1]
    assert not a.stock_compatible(recorded,current,tmp_path)


@pytest.mark.parametrize('fault',['v3_share','v3_tail','v3_reader','missing_st','changed_st','economic_csv'])
def test_v4_rejects_old_partial_or_unreviewed_source_sets(frozen,fault):
    original=json.loads(frozen['raw']('bundle-manifest.json'));current=current_report(original)
    if fault in ('v3_share','v3_tail','v3_reader'):
        name={'v3_share':'scripts/share_change_source.py','v3_tail':'scripts/share_unchanged_tail.py',
              'v3_reader':'scripts/scoped_source_consumer.py'}[fault]
        current['inputs'][name]=a.input_changes(revision=a.OBSERVATION_REVISION)[name][1]
    elif fault=='missing_st':current['inputs'].pop(st.REGISTRY)
    elif fault=='changed_st':current['inputs'][st.REGISTRY]='a'*64
    else:current['artifacts'][next(iter(current['artifacts']))]='a'*64
    with pytest.raises(ValueError):a.require_transition(original,current)


def test_v3_manifest_cannot_be_relabelled_or_used_to_earn_v4(transaction):
    root,calls,_=transaction;saved=a.recertify(root,a.DATE)
    receipt=saved[a.ACTIVE];receipt['revision']=a.OBSERVATION_REVISION
    changes=a.input_changes(revision=a.OBSERVATION_REVISION)
    original=json.loads(receipt['original_manifest_utf8'])
    receipt['source_changes']={n:list(pair) for n,pair in changes.items()}
    receipt['validator_inputs']={**original['inputs'],**{n:pair[1] for n,pair in changes.items()},
                                 a.MODULE:receipt['validator_inputs'][a.MODULE]}
    a.checked_receipt(receipt)
    path=root/'outputs/top100_delivery_manifest.json';raw=a.encoded(saved);path.write_bytes(raw)
    before=len(calls)
    with pytest.raises(ValueError,match='exact original M3 archive'):
        a.recertify(root,a.DATE)
    assert path.read_bytes()==raw and len(calls)==before


def test_v4_cannot_be_earned_with_an_unfrozen_consumer(monkeypatch):
    monkeypatch.setattr(a,'M8_ARCHIVE_INPUT_CHANGES',{})
    with pytest.raises(ValueError,match='not frozen'):a.input_changes()
