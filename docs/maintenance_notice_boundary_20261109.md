# Nov9 independent maintenance notice-boundary repair

Run 42543177978 restored the approved Nov5 baseline, then stopped at the Nov5
notice-boundary comparison. The original authority pointer stayed unchanged.
Artifact 13643390852 (SHA-256
`6ff2770d7e99cd4161af4c1a4d8a33c9a5ab2b0366145297fd8fc8fd7f5f71f6`)
contains the exact old and failed observations. Their stable native identities
also match the previously retained Nov6 failure.

The reviewed change is exactly 1082 to 1085 records: three `INV` investor
relations documents (688809, 600221, 688100) were added; five existing notices
changed only their catalog columns (920395, 300568, 300956, 301358, 301631).
The actual attachments are three DOCX documents served behind PDF-looking URLs
and five PDFs. Their bytes, extracted text, download receipts, issuer/subject
checks and SHA-256 review are retained in
`tests/fixtures/maintenance_boundary_20261109/`.

No affected title or native issuer/category selector produces an ST event.
The adapter accepts only the exact complete old observation hash and exact
complete stable current hash. It checks the native selectors again, then sends
all common records through the unchanged subsecond, same-day, no-close-crossing
publication-clock guard. Every other addition, removal or field change remains
blocked. Original raw pages and historical authority are never rewritten.

This is a maintenance source compatibility repair. It does not approve a
historical migration, regenerate economic history, alter strategy parameters,
send a report or establish current-market freshness. Restore remains under the
original pinned baseline; only a verified exact source transition may activate
this native fix before the normal refresh. Daily delivery remains independent.

Validation retained the original failing counterexample, both real Nov6/Nov9
observation pairs, unreviewed-content/time/selector counterexamples, and the
existing notice refresh, prior transport and release-audit regression tests.
Actual live maintenance acceptance is recorded separately from software tests.
