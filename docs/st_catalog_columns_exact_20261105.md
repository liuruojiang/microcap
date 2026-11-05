# Exact non-ST catalog correction, 2026-11-05

The 11/05 close refresh failed because the 11/04 all-market catalog no longer
matched its reviewed boundary. A separate source-only observation at
2026-11-05 16:48:03–16:48:17 Beijing returned 1,040 rows on 11 complete pages.
It contains the previously reviewed removal of investor-relations record
AN202611041831417267, no additions, and two new non-time field differences:
AN202611041831408711 and AN202611041831408713 lost only category
001003002001 (保荐/核查意见), retaining 001003001001003 (专项说明/独立意见).
Their title, issuer, notice day, and every other non-display-time field agree.

The original issuer PDFs are 301128 强瑞技术 notices 2026-116 and 2026-117:
2025 and 2026 equity-incentive recipient-list disclosure/verification reports.
Their full text concerns incentive eligibility and contains no ST implementation,
removal, or effective-date event. PDF SHA-256 values are:

- 8711: `2853e49fba9226cc2aae54b23c4e54fe0a26c931f7aafb5d1b3c951263b0437f`
- 8713: `cad5dc2e4df5553188c3d993a09007d352d8d3120a7f370d1a65097f5b8671b2`

`tests/fixtures/st_notice_boundary_20261105` preserves all actual raw pages,
receipts and manifest byte-for-byte. `tests/fixtures/st_notice_columns_20261105`
preserves issuer PDFs, extracted complete text, HTTP receipts and review.
These are isolated software-acceptance fixtures, not new production authority.

The adapter is limited to the full old native hash
`df3ea42747570be4b1f6e795ed08684a245f1da4a699ea3d8a65a0d9db9c5ceb`
and full current non-display-time hash
`e8a4f1664df17eebf93f596e897120d1eae30df87f2a1d858c3136cee2e63647`.
The exact observation's full native hash is
`01af438221d6323995d23fc7b0b17a93084df79fbc5725ebf5441796731d70b4`.
It rechecks both original title-risk and daily issuer/category selection rules,
restores only these two category lists in a private copy, then invokes the
unchanged exact INV-removal and time-precision guard. Unknown additions,
deletions, fields, ST selectors, day/second/close-boundary time changes still fail.
Neither caller dictionaries nor old archives are rewritten.

The existing daily core file imports this repair's cases exactly once from a
non-discoverable helper; full-library discovery does not duplicate them.
Final candidate checks: 21 new cases passed (111 unrelated cases deselected),
and 77 affected existing cases passed, zero failures/errors/skips. Managed run
IDs: `f2c226f1906544568387ab8557430dd8` and
`441bfb7f141c4863a6b5645363dbfc4c`; logs/JUnit are retained under
`D:/Codex/test-lifecycle/records/`. Earlier exploratory 89-pass validation is
retained separately and is not substituted for these final checks.

Only `scripts/st_notice_boundary_review.py` changes runtime semantics.
`st_notice_refresh.py`, stock input contract, production evidence path set,
authority, ST intervals, strategy parameters, and old artifacts remain unchanged.
The boundary module's own source hash is still a changed whole-delivery input;
an approved old package is not automatically a new-source certified package.
The existing D4-only source recertifier rejects chained receipts and a current
D5 calendar, so it must not be presented as an existing generic chain mechanism.
Publishing/adoption and strict D5 refresh/whole verification remain separate
coordinator work. No production refresh, pack, dispatch, or mail was run here.
The complete pre-edit commit `49f43adee9df6e8947ff6924f2397a14900f1580`
is the recoverable code baseline.
