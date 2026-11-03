# Frozen regression baseline hash contract

`tests/frozen_formal_baseline.py` authenticates an existing whole-delivery
manifest. Files under `data/st_notice_evidence/` use their original raw-byte
SHA-256, exactly as `top100_delivery.input_hashes` and release verification do.
Other inputs retain the producer's CRLF/LF-equivalent hash. Assertions, manifest
contents and the native whole-output inspection remain unchanged.

This is a test-only correction. It neither certifies today's freshness nor
changes historical state, strategy calculations, authority or release payloads.
The new regression file must read a separately authenticated immutable baseline
through `MICROCAP_TEST_DATA_ROOT`; evidence corruption controls use private
negative copies and never write that baseline.

The existing independent-calendar preflight test also uses a legal completed
session: at 2026-08-10 08:30 Beijing, the latest completed session is August 7.
Its request and persisted-proof generation clocks agree. It supplies complete
synthetic August 7 closing quotes only at the acquisition boundary, leaving the
native clock, date, exact 100-member, ST and typed-proof checks active. The
August 14 negative still rejects and preserves the original proof bytes.

For the November 4 acceptance, the runtime and original package author remain
`d7615a3e3383e02f2b1e0eedfadfcb499ae7317a`; the fixed test source is recorded
separately by its own immutable Git commit. Do not relabel the original package,
edit its manifest or put this helper into a sealed runtime checkout without
recording that source distinction. The approved immutable seed is also kept
separate and unchanged.

Daily software acceptance must explicitly bind the independent test-source
commit when consuming this corrected preflight test. A runtime archive made
from the original production commit does not implicitly contain these test
changes. A test-source binding does not authorize a different runtime or seed.
