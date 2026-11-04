import sys,zipfile
from pathlib import Path
sys.path.insert(0,str(Path.cwd()));sys.path.insert(0,str(Path.cwd()/'tests'))
from test_top100_delivery import workspace
from scripts import top100_cloud_delivery as cloud, top100_delivery as delivery, realtime_state_bundle as state

def test_missing_cache_created_after_inventory_can_be_omitted(workspace,monkeypatch):
    relative=state.PRICE_CACHE_DIR+'/000001.csv';p=workspace/relative;old=p.read_bytes()
    def validate(*a,**kw):
        p.write_bytes(old)
        return {'ok':True,'errors':[],'anchor_dates':{'proxy_index':'2026-09-03','costed_nav':'2026-09-03','panel_shadow':'2026-09-03'}}
    monkeypatch.setattr(state,'validate_state',validate)
    monkeypatch.setattr(cloud.subprocess,'check_output',lambda *a,**kw:'a'*40)
    original_pack=state.pack_state
    def remove_for_inventory(*a,**kw):
        p.unlink()
        return original_pack(*a,**kw)
    monkeypatch.setattr(state,'pack_state',remove_for_inventory)
    bundle=workspace.parent/'missing-cache.zip'
    result=cloud.pack({v:workspace for v in delivery.COSTED},bundle,'2026-09-03')
    assert result['ok']
    assert p.read_bytes()==old
    assert not state.validate_member_cache_transport(workspace,state._parse_date('2026-09-03'))
    with zipfile.ZipFile(bundle) as z:
        state._verify_bundle_manifest(z)
        assert relative not in z.namelist()
    print('REPRODUCED: successful manifest-valid ZIP omits raw cache restored before validation')
