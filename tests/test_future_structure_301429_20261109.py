"""Real future structure observations; neither future date enters today's curve."""
import base64,copy,gzip,json
from datetime import datetime
from pathlib import Path
import pytest
from scripts import share_change_source as source
from frozen_exchange_calendar import frozen_exchange_sessions


@pytest.fixture(autouse=True)
def calendar(frozen_exchange_sessions):return frozen_exchange_sessions


@pytest.fixture
def case():
    with gzip.open(Path(__file__).parent/'fixtures/future_structure_301429_20261109.json.gz','rb') as f:value=json.load(f)
    return value,{p:base64.b64decode(raw) for p,raw in value['objects_b64'].items()}


def verify(case):
    v,bodies=case
    def bound(path,digest):
        raw=bodies[path]
        if source._sha(raw)!=digest:raise ValueError('source digest differs')
        return raw
    return source.verify_dated_window(v['manifest'],symbol=v['symbol'],previous_day=v['previous_day'],end=v['end'],
        previous_candidate=base64.b64decode(v['previous_candidate_b64']),bound=bound,
        root=Path(__file__).resolve().parents[1],now=datetime.fromisoformat(v['observed_at']))


def rewrite(case,entry,raw):
    bodies=case[1];bodies[entry['raw_path']]=raw;entry['raw_sha256']=source._sha(raw)
    receipt=json.loads(bodies[entry['receipt_path']]);receipt.update(raw_sha256=source._sha(raw),bytes=len(raw))
    raw_receipt=json.dumps(receipt).encode();bodies[entry['receipt_path']]=raw_receipt;entry['receipt_sha256']=source._sha(raw_receipt)


def test_actual_two_future_total_neutral_rows_preserve_exact_prior_curve(case):
    v,_=case;old=copy.deepcopy(case)
    result,facts=verify(case)
    assert result==base64.b64decode(v['previous_candidate_b64'])
    assert facts==v['verified_facts'] and facts['changes']==[]
    assert facts['future_structure_not_consumed']['provider_dates']==['2026-11-10','2026-11-12']
    assert facts['future_structure_not_consumed']['total_shares_unchanged']==118220000
    assert b'2026-11-10' not in result and b'2026-11-12' not in result
    assert case==old


@pytest.mark.parametrize('field',['END_DATE','LISTING_DATE','NOTICE_DATE','INFO_CODE','TOTAL_SHARES',
                                 'TOTAL_SHARES_CHANGE','LIMITED_SHARES_CHANGE','SECURITY_CODE'])
def test_unknown_row_date_quantity_and_issuer_cannot_use_document_review(case,field):
    entry=case[0]['manifest']['files']['unfiltered'][0]
    value=json.loads(case[1][entry['raw_path']]);row=value['result']['data'][0]
    row[field]=row[field]+1 if isinstance(row[field],int) else 'unreviewed'
    rewrite(case,entry,json.dumps(value).encode())
    with pytest.raises(ValueError):verify(case)


@pytest.mark.parametrize('fault',['missing','changed','duplicate','remove_row','extra_row','current_total'])
def test_review_remains_exact_and_requires_independent_target_total(case,fault):
    files=case[0]['manifest']['files']
    if fault=='missing':files.pop('later_structure_documents')
    elif fault=='changed':
        entry=files['later_structure_documents'][0];rewrite(case,entry,case[1][entry['raw_path']]+b'changed')
    elif fault=='duplicate':files['later_structure_documents']*=2
    else:
        entry=files['dated_window'] if fault=='current_total' else files['unfiltered'][0]
        value=json.loads(case[1][entry['raw_path']])
        if fault=='remove_row':value['result']['data'].pop(0);value['result']['count']-=1
        elif fault=='extra_row':
            row=copy.deepcopy(value['result']['data'][0]);row['END_DATE']='2026-11-13 00:00:00'
            value['result']['data'].insert(0,row);value['result']['count']+=1
        else:value['result']['data'][0]['TOTAL_SHARES']+=1
        rewrite(case,entry,json.dumps(value).encode())
    with pytest.raises(ValueError):verify(case)
