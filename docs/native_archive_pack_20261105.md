# Explicit native archive packing

Ordinary `pack` validates the latest completed exchange session. Packing an
approved 11/04 archive after the 11/05 close correctly failed that live-readiness
check, even when all historical input and output bytes remained intact.

`scripts/top100_cloud_delivery.py pack-archive` is now a separate transport
entrypoint. It takes the same root/version-root/bundle/expected-date arguments
as `pack`. The explicit archive session must be a covered exchange session and
must not exceed the independent latest completed session. The three daily base
anchors and all three final daily streams must equal this session. Turnover and
member-rebalance dates retain their existing event-stream checks.

This entrypoint retains native whole inspection, source-validation ancestry,
exclusive writer locking, member/ST and raw/share transport checks, complete
state checks, before/after source and final-artifact comparisons, complete ZIP
hash verification, and atomic shared-bundle publication. Its base validation
uses the same `require_current_refresh_proof=False` integrity mode as existing
historical restore. It does not rewrite old proof dates or generate fresh quotes.
`archive_session` and `close_confirmed_session` in `pack_state` are mutually
exclusive. Ordinary pack and every normal freshness default are unchanged.

Return JSON, whole-delivery manifest and ZIP validation all retain the actual
archive date and explicitly record:

```json
{
  "signal_ready": false,
  "current_signal_ready": false,
  "freshness": "verified_archive_only_requires_current_refresh"
}
```

These fields prove only a validated historical transport artifact. A caller
adopting an approved state for a new runtime must independently bind the original
ZIP SHA, exact declared source changes, unchanged economic inputs/artifacts/
streams, preserved original ZIP and native ancestry. Current-session refresh
and publication checks remain required afterwards. The native archive entrypoint
does not itself authorize a historical migration or sending mail.

The 16 new cases are collected once through existing
`tests/test_top100_cloud_delivery.py`, with the non-discoverable
`archive_pack_cases_20261105.py` helper. They cover D4 proof rejection after D5,
archive integrity mode and labels, invalid/future/non-session targets, wrong or
absent anchors, data-integrity failure, mutually exclusive modes, unchanged
source files, normal-pack rejection, raw gaps/wrong dates, final-stream damage,
and writer locks. Targeted run `a93b97e4d5504c98a233e237d2fcd967` passed all 16
cases with no failures/errors/skips; unrelated existing cases were deselected
and run separately. Logs/JUnit are retained by the test-lifecycle tool.
The 25 existing pack/restore contract cases also passed with zero failures,
errors or skips (run `3e590029ce83460796dfb14976d8b124`, 62.40 seconds).

No production refresh, historical reconstruction, pack, cloud dispatch or mail
was performed to implement this change. The pre-edit candidate commit
`e8c55c773f4e9b04a923aacbdac15ca4948eddb4` is the code rollback point.
