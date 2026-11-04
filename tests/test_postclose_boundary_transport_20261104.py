import copy,json
from pathlib import Path
import pytest
from scripts import st_notice_refresh as refresh
from scripts import st_notice_postclose_boundary_20261104 as extension
from scripts import st_notice_boundary_review as original
from scripts.st_notice_evidence import StNoticeEvidenceError

ROOT=Path(__file__).resolve().parents[1]

def rows():
    before=json.loads((original.DIRECTORY/'reviewed_20261104/registry.json').read_bytes())['previous_rows']
    after=json.loads((extension.DIRECTORY/'candidate.json').read_bytes())['current_rows']
    return before,after

def test_exact_extension_preserves_native_original_review_and_risk_projection():
    before,after=rows();result=refresh._boundary_precision_drift(before,after)
    assert result['policy']==extension.POLICY and result['risk_projection_unchanged']
    assert len(result['reviewed_non_st_added_ids'])==83
    assert result['reviewed_existing_st_progress_ids']==['AN202611031831376420']
    assert len(result['reviewed_metadata_changed_ids'])==16
    assert refresh._boundary_summary_matches(result,refresh._boundary_precision_drift(before,after),before,after)

@pytest.mark.parametrize('change',['extra','delete','title','issuer','date','columns','old_risk'])
def test_exact_extension_rejects_every_unreviewed_native_change(change):
    before,after=rows();after=copy.deepcopy(after)
    key='AN202611041831390838'
    if change=='extra':after['UNREVIEWED']=dict(after[key],art_code='UNREVIEWED')
    elif change=='delete':del after[next(iter(before))]
    elif change=='title':after[key]['title']='新华都:关于公司股票被实施退市风险警示的公告'
    elif change=='issuer':after[key]['codes'][0]['stock_code']='000001'
    elif change=='date':after[key]['notice_date']='2026-11-04 00:00:00'
    elif change=='columns':after['AN202611021831317290']['columns']=[]
    else:after['AN202611021831333702']['title']+=' changed'
    with pytest.raises(StNoticeEvidenceError):refresh._boundary_precision_drift(before,after)

@pytest.mark.parametrize('kind',['candidate.json','AN202611041831390838.pdf','AN202611041831390838.txt','AN202611041831390838.receipt.json'])
def test_exact_extension_rejects_changed_certificate_and_source_bytes(tmp_path,monkeypatch,kind):
    import shutil
    before,after=rows();target=tmp_path/'review';shutil.copytree(extension.DIRECTORY,target)
    (target/kind).write_bytes((target/kind).read_bytes()+b'changed')
    monkeypatch.setattr(extension,'DIRECTORY',target)
    with pytest.raises(StNoticeEvidenceError):refresh._boundary_precision_drift(before,after)
