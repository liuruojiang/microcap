# Exact late-boundary transport review

The existing 11/03 observation contained 1,435 native announcement rows at
18:34 Beijing. A later real observation contains 1,518 rows: 83 additions,
no removals, six 000972 issuer-name display updates and one 920267 sort-clock
update. The original subsecond display-time rule remains unchanged.

This release recognizes only that exact source pair. All added rows still pass
the original title-action and issuer/category selectors. The sole selected
301030 progress notice is bound to the reviewed original issuer PDF: it reports
existing risk warnings and monthly progress, not a new effective ST event. The
920267 PDF describes a capital/articles shareholder resolution. The 000972
display updates retain the previously approved issuer/document binding and do
not move the actual resumption or name-change date into the announcement day.

The prior 631-event registry, archive authority, security metadata and economic
outputs are unchanged. New/unreviewed rows, document changes, different issuer
identities, newly effective risk actions and unknown metadata changes still
fail. This is neither a general progress-notice exemption nor approval to
rewrite historical ST intervals or seeds.

The old 18:34 observation is not extended to cover the later 83 announcements.
The normal native refresh must first verify the new source observation. A
successful observation can then be reused only while both its real collection
start and completion fall on the current Beijing day, its target remains the
independent latest completed session, and the complete source/authority hashes
still pass. All real clocks are retained; crossing midnight, a new close,
clock reversal or source mutation blocks reuse. No old proof is re-dated.
