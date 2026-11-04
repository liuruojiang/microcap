import sys,zipfile
from pathlib import Path
sys.path.insert(0,str(Path.cwd()))
sys.path.insert(0,str(Path.cwd()/'tests'))
from test_top100_delivery import workspace
from scripts import top100_cloud_delivery as cloud, top100_delivery as delivery, realtime_state_bundle as state

def test_cache_mutation_after_semantic_validation_is_published(workspace,monkeypatch):
    monkeypatch.setattr(state,'validate_state',lambda *a,**kw:{'ok':True,'errors':[],'anchor_dates':{'proxy_index':'2026-09-03','costed_nav':'2026-09-03','panel_shadow':'2026-09-03'}})
    monkeypatch.setattr(cloud.subprocess,'check_output',lambda *a,**kw:'a'*40)
    assert state.validate_member_cache_transport(workspace,state._parse_date('2026-09-03'))==[]
    relative=state.PRICE_CACHE_DIR+'/000001.csv';p=workspace/relative
    old=p.read_bytes();original=state._iter_bundle_files
    def concurrent_writer(root):
        p.write_bytes(b'date,close_raw\n2020-01-01,10\n')
        return original(root)
    monkeypatch.setattr(state,'_iter_bundle_files',concurrent_writer)
    original_pack=state.pack_state
    def restore_after_archiving(*a,**kw):
        try:return original_pack(*a,**kw)
        finally:p.write_bytes(old)
    monkeypatch.setattr(state,'pack_state',restore_after_archiving)
    bundle=workspace.parent/'race.zip'
    result=cloud.pack({v:workspace for v in delivery.COSTED},bundle,'2026-09-03')
    assert result['ok']
    with zipfile.ZipFile(bundle) as z:
        assert z.read(relative)!=old
        state._verify_bundle_manifest(z)
    with zipfile.ZipFile(bundle) as z:p.write_bytes(z.read(relative))
    errors=state.validate_member_cache_transport(workspace,state._parse_date('2026-09-03'))
    assert any('000001' in error and 'before anchor' in error for error in errors)
    print('REPRODUCED: successful ZIP contains stale raw-price cache, archive hash check passes, semantic member check rejects')
