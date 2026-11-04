"""Bind unchanged accepted share histories to their existing source closure.

No share CSV, NAV, historical value, or policy is changed. Public-disclosure PIT
limitations remain those of the already accepted historical model.
"""
from pathlib import Path
import hashlib
import json
import sys
import zipfile

root = Path.cwd()
sys.path.insert(0, str(root))
from scripts.share_change_source import install_approved_share_baseline, verify_share_cache

task = root / 'research_reports/20261029_st_history_repair'
evidence = root / 'research_reports/20261102_deferred_repairs'
out = evidence / 'existing_share_baselines'
out.mkdir(exist_ok=True)
sha = lambda raw: hashlib.sha256(raw).hexdigest()
mapping_path = task / 'share_install_20261029/manifest.json'
mapping = json.loads(mapping_path.read_bytes())
mapping_sha = sha(mapping_path.read_bytes())
records = {r['symbol']: r for r in mapping['records']}
assert len(records) == 5438
bundle = evidence / 'microcap-native-whole-delivery-20261030.zip'
bundle_sha = sha(bundle.read_bytes())
assert bundle_sha == '2c673040c8d919476f8896e5596b7a693db6443304d4ad9e3f50925383c8a140'
em = {r['symbol']: r for r in json.loads((task / 'cross_source_shares/normalized_candidate/manifest.json').read_bytes())['files']}
cn = {r['symbol']: r for r in json.loads((task / 'new463_cninfo_shares_20261029/final_independent_manifest.json').read_bytes())['records']}
previous = task / 'formal_base_install_backups/bcb459276dcfa5e130ba14654a045b85d454dadc5f9d1e97796a02571e351a8a/.microcap_index_cache/share_change'
prepared = []
with zipfile.ZipFile(bundle) as archive:
    for symbol, record in sorted(records.items()):
        live = root / '.microcap_index_cache/share_change' / (symbol + '.csv')
        payload = live.read_bytes()
        assert sha(payload) == record['candidate_sha256'], symbol
        assert archive.read('.microcap_index_cache/share_change/' + symbol + '.csv') == payload, symbol
        state = root / '.microcap_index_cache/share_sources' / (symbol + '.json')
        if state.exists():
            continue
        package = out / symbol
        package.mkdir(exist_ok=True)
        saved = package / 'approval.json'
        if saved.exists():
            raw = saved.read_bytes()
            prior = json.loads(raw)
            assert prior['accepted_whole_bundle_sha256'] == bundle_sha
            assert prior['selected_history_mapping_sha256'] == mapping_sha
            assert prior['selected_history_record'] == record
            assert prior['records'][0]['candidate_sha256'] == sha(payload)
            for item in prior['records'][0]['source_files']:
                assert sha((package / item['path']).read_bytes()) == item['sha256']
            prepared.append(dict(symbol=symbol, approval_path=str(saved), sha256=sha(raw), candidate_sha256=sha(payload)))
            continue
        sources = []
        def bind(src, name, expected=None):
            raw = src.read_bytes()
            if expected is not None:
                assert sha(raw) == expected, (symbol, str(src))
            dest = package / name
            if dest.exists():
                assert dest.read_bytes() == raw, dest
            else:
                dest.write_bytes(raw)
            sources.append(dict(path=name, sha256=sha(raw)))
        bind(live, 'selected_history.csv', record['candidate_sha256'])
        bind(root / 'docs/release_evidence_20261101/combined_review.json', 'existing_migration_review.json')
        if 'source_receipt_path' in record:
            receipt_path = Path(record['source_receipt_path'])
            bind(receipt_path, 'source_receipt.json', record['source_receipt_sha256'])
            receipt = json.loads(receipt_path.read_bytes())
            bind(previous / (symbol + '.csv'), 'previous_history.csv', record['baseline_sha256'])
            for i, request in enumerate(receipt.get('requests', [])):
                bind(Path(request['raw_path']), f'source_page_{i}.json', request['raw_sha256'])
            if 'source_raw_path' in record:
                bind(Path(record['source_raw_path']), 'source_raw.json', record['source_raw_sha256'])
            disposition = record.get('issuer_disposition', {})
            if disposition.get('source_path') and disposition.get('source_sha256'):
                bind(Path(disposition['source_path']), 'accepted_issuer_evidence.bin', disposition['source_sha256'])
        else:
            if symbol in em:
                item = em[symbol]
                original = task / 'cross_source_shares' / item['source']
                original_sha = item['source_sha256']
            else:
                original = Path(record['source_path'])
                original_sha = record['source_sha256']
            bind(original, 'em_original.csv', original_sha)
            request_path = original.with_name(symbol + '_requests.json')
            bind(request_path, 'em_requests.json')
            for i, request in enumerate(json.loads(request_path.read_bytes())):
                bind(original.with_name(symbol + f'_page{i+1}.json'), f'em_page_{i+1}.json', request['sha256'])
            for field in ('raw', 'request', 'candidate'):
                item = cn.get(symbol, {})
                if item.get(field + '_path') and item.get(field + '_sha256'):
                    bind(Path(item[field + '_path']), 'cninfo_' + field + ('.csv' if field == 'candidate' else '.json'), item[field + '_sha256'])
        approval = dict(schema='share_baseline_approval_v1', approved=True,
            approved_by='assistant executing existing explicit repair and deployment authorization',
            user_separately_reviewed_generated_hashes=False,
            authorization='Bind only the unchanged, already accepted economic share history; no new historical values or PIT certification.',
            accepted_whole_bundle_sha256=bundle_sha,
            selected_history_mapping_sha256=mapping_sha,
            selected_history_record=record,
            records=[dict(symbol=symbol, candidate_sha256=sha(payload), certified_cutoff='2026-10-29',
                          source_files=sources, scope='Existing selected economic history and original source closure; prior public-disclosure PIT limitations retained.')])
        approval_path = package / 'approval.json'
        raw = json.dumps(approval, ensure_ascii=False, indent=2).encode()
        if approval_path.exists():
            assert approval_path.read_bytes() == raw
        else:
            approval_path.write_bytes(raw)
        prepared.append(dict(symbol=symbol, approval_path=str(approval_path), sha256=sha(raw), candidate_sha256=sha(payload)))
        if len(prepared) % 500 == 0:
            print('PREPARED', len(prepared), flush=True)
receipt = dict(scope='unchanged accepted share histories only', prepared=prepared, original_bundle_sha256=bundle_sha,
               installed=False, historical_values_changed=False)
(out / 'preparation_receipt.json').write_text(json.dumps(receipt, indent=2), encoding='utf-8')
print('PREPARATION_PASS', len(prepared), flush=True)
if '--install' in sys.argv:
    assert not (root / 'outputs/top100_delivery.lock').exists(), 'Wait for the active delivery writer to finish'
    for i, item in enumerate(prepared, 1):
        cache = root / '.microcap_index_cache/share_change' / (item['symbol'] + '.csv')
        install_approved_share_baseline(root=root, symbol=item['symbol'], cache_path=cache,
                                       approval_path=Path(item['approval_path']), approval_sha256=item['sha256'])
        assert sha(cache.read_bytes()) == item['candidate_sha256']
        if i % 500 == 0:
            print('INSTALLED_UNCHANGED', i, flush=True)
    receipt['installed'] = True
    (out / 'installation_receipt.json').write_text(json.dumps(receipt, indent=2), encoding='utf-8')
    print('INSTALLATION_PASS', len(prepared), flush=True)
