# Explicit completed-session state preflight

`preflight --publication-mode close-confirmed-check --expected-date D` requires
D to equal the independent formal calendar's latest completed session. It can
therefore check D after midnight without pretending that today's market has
closed. A missing target, an unclosed/future target, an expired target or an
unavailable calendar fails. The clock is read again after calendar acquisition;
crossing midnight or a new close requires another check, with a bounded retry.

This mode audits 100 unique member quotes from the existing provider readers.
Each source quote must belong to D, have a real timestamp at/after D15:00,
positive finite price and `provider_latest` origin, with quote <= collection <=
the real validation clock. It retains the source's actual collection day after
midnight. It cannot use collection time as quote time, use a previous-close
fallback as a new quote, or accept current-day intraday quotes as D's close.
The original member/ST checks remain mandatory.

The existing refresh-proof path stores all 100 parsed quote rows, their source
labels and clocks, plus an identity over the original native input/hash
inventory. Readback rechecks the rows, aggregates, membership, ST, target,
generation clock and input identity. Only today's fully checked explicit close
proof can avoid a natural-day age rejection over a holiday. Explicit archive
inspection can read a dated close proof while its session remains latest; it
does not turn that proof into today's live proof or grant that age exception.

Every explicit history/member request and proof commit rechecks the target.
The initial native validation is followed immediately by the same check.
Deep calendar acquisition remains a read-only calendar operation; this close
mode never registers an intraday capability or changes an intraday deadline.

The intraday quote day/age, 15:00 request and publication limits, strategy
formulas, the stock-source dependencies, historical outputs and mail permission
are unchanged. This mode produces a state-readiness proof, not a realtime
signal, transaction, SMTP submission or inbox receipt. At 10:00, if providers
already quote the new trading day, those quotes cannot certify D's close;
morning state preparation does not waive the separate 14:30 realtime checks.
