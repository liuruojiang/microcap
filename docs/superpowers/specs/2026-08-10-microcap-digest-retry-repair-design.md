# Microcap Digest Retry Repair Design

## Goal

Restore a reliable production Microcap Top100 digest for v2.0, v2.3, and v2.5 after the 2026-08-10 fresh-runner failure, without weakening final-CSV identity or freshness gates.

## Root Cause

A fresh GitHub runner had no raw member price caches. `load_member_snapshot()` raised before the existing proxy-members fallback could run, even though the official proxy-members artifact contained 100 rows for both required rebalance dates. The state validator classified every missing current-member cache as a warning, so the refresh command returned zero after reusing incomplete state. The workflow then sent an abnormal email, created the normal delivery marker, and prevented redundant cron runs from retrying.

## Design

1. Strategy member snapshots use raw price/share caches when available. If that path cannot produce a complete Top100 snapshot, the existing dated proxy-members artifact supplies only the missing rebalance dates. If neither source supplies exactly 100 members, the refresh fails closed.
2. State validation treats zero usable current-member price caches as an error unless complete dated static member context is already present. A caught refresh exception cannot be reported as reusable state when the realtime signal consumer would immediately fail.
3. The automation workflow creates the normal daily delivery marker only when all three signal scripts exit zero and the digest email sends successfully. An abnormal diagnostic email may still be sent, but it leaves the normal marker absent so later redundant cron runs retry.
4. Production pins the merged strategy repair SHA. A correction run is accepted only after its artifact contains readable v2.0/v2.3/v2.5 final CSVs with exact identities, the expected latest anchor/quote dates, and a delivered corrected email.

## Testing

- Strategy regression tests reproduce a fresh runner with empty raw caches and complete proxy-members rows, and verify the fallback produces complete snapshots.
- State-bundle regression tests verify incomplete cached state cannot return `ok=true`.
- Automation workflow tests verify failed signals cannot create the normal delivery marker, while successful signals can.
- Run the full strategy suite, Microcap automation suites, production correction workflow, artifact readback, and remote SHA checks.

