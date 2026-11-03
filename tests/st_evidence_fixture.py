"""Small authentic registry closures for mutation tests; never market evidence."""
import json
import shutil
from pathlib import Path
from scripts.st_notice_evidence import evidence_fingerprint, evidence_source_files
from scripts.st_notice_boundary_review import SOURCE_FILES as BOUNDARY_SOURCE_FILES


def copy_real_evidence_subset(source_root, destination_root, event_ids):
    source_root, destination_root = Path(source_root), Path(destination_root)
    registry_name = 'data/st_notice_evidence/registry.json'
    registry = json.loads((source_root / registry_name).read_bytes())
    selected = set(event_ids)
    registry['events'] = [event for event in registry['events'] if event['announcement_id'] in selected]
    assert {event['announcement_id'] for event in registry['events']} == selected
    files = {'scripts/st_notice_evidence.py', 'scripts/st_notice_archive.py'}
    # The real source contract includes boundary reviews independently of the
    # selected event list. Copy their authentic bytes and retain the closure
    # equality assertion below; an incomplete fixture must still fail closed.
    files.update(BOUNDARY_SOURCE_FILES)
    for event in registry['events']:
        files.add('data/st_notice_evidence/' + (event.get('relative_html_path') or event['relative_pdf_path']))
        for category in ('conflict_evidence', 'unavailable_evidence', 'native_evidence', 'exchange_evidence'):
            files.update('data/st_notice_evidence/' + artifact['relative_path']
                         for artifact in event.get(category, {}).values())
    for relative in sorted(files):
        destination = destination_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_root / relative, destination)
    path = destination_root / registry_name
    path.write_text(json.dumps(registry, ensure_ascii=False, indent=2), encoding='utf-8')
    # Validate authentic body/receipt/identity bindings before each mutation.
    evidence_fingerprint(destination_root)
    assert set(evidence_source_files(destination_root)) == files | {registry_name}
    return path, registry
