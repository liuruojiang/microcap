# Strict unchanged share-tail fallback

`dual_source_unchanged_tail_v1` certifies only observations with no share change
between adjacent independent exchange sessions D and T. It does not determine
the execution date of a new share event, rewrite history, or guarantee that a
provider has disclosed every event without delay.

The verifier requires complete Eastmoney unfiltered ALL-field and bounded
inventories, the complete native Sina TotalStock charts and visible tables, and
a T-dated Eastmoney quote frame. Their as-of totals must equal the verified D
CSV value exactly. The quote frame clock is not an independent timestamp for
its fundamental total-share field; that limitation remains explicit.

A future row is retained verbatim and excluded from T consumption only when
both providers agree and the immutable reviewed-plan registry binds the exact
issuer PDF, date and quantities. Unreviewed or ambiguous future rows fail.
When a planned date becomes T, the review cannot certify execution or authorize
a changed CSV; a qualified actual event source is required.

Legacy schema1 CNINFO chains retain their existing semantics, including their
general historical query windows. Schema2 keeps all old state bytes and CSV
bytes and recursively verifies the complete mixed chain. It never installs an
approved baseline over an existing state.

The original CNINFO denial marker and private diagnostics are preserved.
Existing denial or a prior fallback state selects the fallback for that symbol.
A first CNINFO403/429 preserves the real denial and, only with a verified prior
state, attempts the qualified fallback once inside the same per-symbol lock.
It does not repeat CNINFO. A fallback failure preserves both errors and a private
diagnostic; interruption is not swallowed. Without a verified prior state, the
original source failure remains closed.
The private marker is excluded only from the economic fingerprint, not deleted
or promoted to evidence that the primary API is healthy.

Public receipts contain only allowlisted response headers. A derived receipt
identifies its original private receipt hash and transformation; it is not
presented as the original byte stream. Raw source bodies and issuer PDFs remain
hash-bound. This source repair is not whole-delivery acceptance, a current
research signal, SMTP submission or inbox delivery.
