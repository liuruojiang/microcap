# v2.0 / v2.3 / v2.5 script repairs, 2026-10-15

The shared strategy runtime now stops on unresolved security metadata and malformed CNINFO responses, missing internal exchange sessions, nonfinite active returns, and incomplete adjusted-price coverage that would otherwise splice adjusted and raw closes. Raw return prices must be finite and positive before constituent selection. Existing approved historical evidence, formal parameters, and healthy-input paper-return definitions are retained.

The regression fixtures now use a manifest-authenticated frozen real-data snapshot, preserve its bytes, isolate probe logs and module patches, and distinguish synthetic realtime contract cases from market evidence. Real native freshness and post-promotion checks remain active. Old tracked display exports are not treated as current approved seed evidence.

Validation on the repaired source: all 52 test files, 1,457 cases, zero failures/errors/skips. Real recalculation through 2026-10-14 matched the three refreshed formal streams and an independent price/state/cost ledger; the maximum CSV serialization difference was 1.42e-14. All 4,975 security-metadata records and 12,822,346 raw prices passed compatibility checks. These checks do not certify executable-account performance or historical point-in-time completeness.

The exact already approved v5 seed is dated 2026-10-12 and remains bound to the unchanged frozen-tail authority. Publishing that seed repairs the repository's previous mismatch between tracked exports and its authority; it does not authorize a new historical migration. New final daily streams must be regenerated through the official group refresh and independent check after publication.

Daily deployment must update all three strategy checkouts and the approved whole-state recovery configuration together. The recovery ZIP must be built and verified against the published source, approved authority and final streams. Local synchronization is accepted only after `scripts/top100_delivery.py refresh-all` and `check` both pass in the primary workspace; cloud acceptance additionally requires the actual workflow, final CSV readback, dated member actions and transport evidence.

F5 remains an account-definition issue: member-count costs do not constitute a full cash/share/fill ledger. This release retains the approved paper cost definition. A complete fee-account replacement requires a separate definition and historical migration review.

Local repair evidence: `research_reports/20261015_script_bugfix/repair_report.html`, `full_regression.json`, `patch_manifest.json` and `math_timing/repaired_real_ledger_results.json`. Deployment evidence is recorded separately in `research_reports/20261015_remote_daily_sync/`.
