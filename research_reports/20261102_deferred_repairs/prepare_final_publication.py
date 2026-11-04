"""Prepare exact publication identities only after the full pack receipt exists."""
import hashlib
import json
from pathlib import Path

root = Path.cwd()
evidence = root / 'research_reports/20261102_deferred_repairs'
packed = json.loads((evidence / 'current_whole_pack_receipt.json').read_bytes())
delta = json.loads((evidence / 'current_delta_receipt.json').read_bytes())
bundle = Path(packed['bundle'])
assert packed['ok'] and packed['expected_date'] == '2026-11-02'
assert hashlib.sha256(bundle.read_bytes()).hexdigest() == packed['sha256'] == delta['target_sha256']
assert delta['exact_reconstruction'] is True
assert delta['base_sha256'] == '2c673040c8d919476f8896e5596b7a693db6443304d4ad9e3f50925383c8a140'
tag = 'top100-tencent-daily-20261102'
workflow = root / '.github/workflows/native-state-release.yml'
text = workflow.read_text(encoding='utf-8')
replacements = {
    'top100-shared-history-20261101': 'top100-native-daily-20261102',
    'microcap-whole-delivery-20261030.zip': 'microcap-native-whole-delivery-20261030.zip',
    'native-state-delta.zip': 'scoped-state-delta.zip',
    '8d6af3322aeb693f0bcee2e61281d6389c21b496b8305f3e1f1c27834fdc8664': delta['base_sha256'],
    '2c97eb4a6d85612a92c84b2029d7dd73758f55aa09de53844556aa762bf9e884': delta['delta_sha256'],
}
# Change target-specific lines before replacing old base names/hashes.
text = text.replace("assert len(result)==452877430", f"assert len(result)=={packed['bytes']}")
text = text.replace("assert hashlib.sha256(result).hexdigest()=='2c673040c8d919476f8896e5596b7a693db6443304d4ad9e3f50925383c8a140'", f"assert hashlib.sha256(result).hexdigest()=='{packed['sha256']}'")
text = text.replace("Path('microcap-native-whole-delivery-20261030.zip').write_bytes(result)", f"Path('{bundle.name}').write_bytes(result)")
text = text.replace('gh release download top100-native-daily-20261102 --repo liuruojiang/microcap --pattern native-state-delta.zip', f'gh release download {tag} --repo liuruojiang/microcap --pattern scoped-state-delta.zip')
text = text.replace('gh release upload top100-native-daily-20261102 microcap-native-whole-delivery-20261030.zip', f'gh release upload {tag} {bundle.name}')
text = text.replace('gh release edit top100-native-daily-20261102', f'gh release edit {tag}')
for old, new in replacements.items():
    text = text.replace(old, new)
workflow.write_text(text, encoding='utf-8', newline='\n')

automation = Path('D:/Codex/workspaces/microcap-no-overheat-daily')
config = automation / 'config/microcap_approved_state.json'
old = json.loads(config.read_bytes())
daily = automation / '.github/workflows/microcap-realtime-digest.yml'
text = daily.read_text(encoding='utf-8')
assert text.count('ref: ' + old['strategy_sha']) == 3
daily.write_text(text.replace('ref: ' + old['strategy_sha'], 'ref: ' + packed['release_sha']), encoding='utf-8', newline='\n')
old.update(strategy_sha=packed['release_sha'], state_url=f'https://github.com/liuruojiang/microcap/releases/download/{tag}/{bundle.name}', state_sha256=packed['sha256'], state_date='2026-11-02')
config.write_text(json.dumps(old, indent=2) + '\n', encoding='utf-8')
notes = f"Three-version whole delivery for 2026-11-02. Local refresh-all and independent check passed. Strategy {packed['release_sha']}. Bundle SHA-256 {packed['sha256']}. Exact-byte transport reconstruction required. User authorized the normal 2026-11-02 close-confirmed daily digest. Cloud run and SMTP/inbox evidence are recorded separately; no extra test email.\n"
(evidence / 'release-notes-final.txt').write_text(notes, encoding='utf-8')
print(json.dumps(old, indent=2))
