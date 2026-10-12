# Current ST history cache guard (2026-10-12)

## Failure addressed

A current `ST`/`*ST` name snapshot and a matching metadata date do not prove the historical start of that security's point-in-time ST interval. A failed name-history or CNInfo notice query could leave only a snapshot-derived interval. Reusing that cache could admit a security into earlier proxy membership; a newly built unresolved record could also be written before the loader rejected it.

## Code behavior

- A current-ST metadata cache is reusable only when `current_st_history_resolved` is explicitly `true`.
- `build_security_meta` rejects unresolved current-ST history before writing new metadata. If the existing certified-history guard retains a previous record, its exact bytes remain unchanged; unresolved retained evidence raises the dedicated fail-closed exception.
- `SecurityMetaHistoryEvidenceUnresolved` derives from `SecurityMetaHistoryCorrectionRequired`, so the existing historical-membership build aggregation rejects the symbol and the full build fails closed.
- `_preserve_security_meta_evidence` and its exact-hash lineage review contract are unchanged. This patch does not approve, rewrite, or publish any historical member lineage or authority.

## Regression coverage

`tests/test_security_meta_current_st_history_resolution_20261012.py` covers unresolved same-date cache rejection, official resolved metadata, preserving an existing unresolved record, rejecting a new unresolved record before file creation, and the formal cache-panel aggregation failure. `tests/test_security_meta_preserve_20260917.py` retains certified bytes and rejects unresolved or unreviewed historical replacement.

The isolated candidate source SHA-256 `fe9642dc8222eb4ddcf3a6690cee84cbc3d1988b4bfef5dc2d72af588f93206f` passed the cumulative 20-file delivery suite (420 passed, zero failures/errors/skips, 49.242 seconds, exit 0) on 2026-10-12. The run record is stored with the automation audit artifacts; this source note does not certify candidate historical data for production adoption.
