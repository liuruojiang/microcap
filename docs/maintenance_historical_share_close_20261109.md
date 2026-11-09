# Previous-session share maintenance

The Nov9 09:00 task retains complete prior-session base maintenance. There is no
other daily full-base updater. Neither the schedule nor maintenance scope is
reduced. Run 42551243114 passed notice/static/ST renewal but failed on 002316:
the live quote frame was Nov9 while the requested completed session was Nov6.

`share_unchanged_tail.py` now collects an additional immutable historical daily
valuation observation from the existing public Eastmoney
`RPT_VALUEANALYSIS_DET` report when the quote is genuinely a later-day observation.
The original quote remains recorded and is explicitly marked as a later quote,
not a target close. Without the new evidence the original timestamp failure is
unchanged. Missing, stale, future or conflicting clocks still fail.

The date-bounded query must contain exactly two unique adjacent exchange
sessions, in native descending order, with the exact issuer/code/exchange.
Both raw integer TOTAL_SHARES values must equal the authenticated old boundary.
Positive close and total-capitalization fields must confirm the share units.
The original full/bounded Eastmoney event inventories, Sina complete history,
future-plan document checks, source endpoint/TLS/receipt/hash checks, adjacency
and no-historical-rewrite guards all remain mandatory. A changed share total or
ambiguous event still requires its original separate transition process.

This is provider-date-labelled corroboration, not a guarantee of disclosure
latency or provider revisions. No quote time is relabelled and no CNINFO stop
is removed or retried.

Actual Nov9 public observations for 002316 are preserved in
`tests/fixtures/historical_share_close_002316_20261109.json`. Nov5 and Nov6 each
report 393,120,000 total shares, equal to the original 39,312 ten-thousand-share
boundary. All 100 current members also passed the live historical-source
precheck. This precheck alone is not full maintenance acceptance.

The real native 002316 recursive source chain was separately renewed Nov5 to
Nov6 and read back in the isolated worktree. All 45 copied primary source files,
the original share CSV and the original CNINFO denial marker remained unchanged.

The source-only compatibility review binds the exact Nov5 input identity
`992f1e8a37d3184e61042c01976b9a11ea0ad33ce8daed65bc534747d246d13d`,
16,315 freshly rehashed input files and only the exact paired share-verifier/
continuation source transition. Old proxy metadata and all previous sealed reviews are retained.
It grants no permission for changed historical data, reverse transitions,
partial upgrades or other future code. After genuine tail completion the native
new metadata records the new evidence normally.

For a seed older than one session, the public per-symbol lock now encloses
successive native adjacent-session renewals. Each session still authenticates
the old chain, qualifies its own evidence and publishes/readbacks normally.
Future/non-session targets fail, and a failure retains the last verified
session. Retrying resumes at the first missing session. Real preserved Nov4,
Nov5 and Nov6 evidence tests exercise both complete renewal and second-session
failure/resume, without changing the historical share CSV or old source bytes.

Rollback uses the preceding committed runtime/config pair; source evidence and
approved old state are retained. Real complete remote maintenance acceptance
must be recorded separately before claiming the task repaired.
