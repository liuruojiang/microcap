# Adjacent directory inventory reuse

This is a validator-only optimization. Strategy parameters, economic inputs,
authority, historical records and the approved whole-state package are unchanged.
`realtime_state_bundle.py` belongs to RELEASE_FILES, but is not an economic
input hash, stock-input source, or whole-package SOURCE_FILES member.

On a successful process-local memo lookup the two complete byte-hash passes
remain mandatory. Only the directory inventory between those adjacent passes
is shared, reducing four inventories to three. The final independent inventory,
links, locks, calendar, module identity and changed-content checks remain.
A full semantic verifier between scans disables this reuse. No inventory is
persisted or shared with a later request. Cold checks still use four inventories.

`return_basis_scan_metrics(root)` reports inventory/content time and counts;
it is bounded, copied on read, and never used as certification or an input hash.
This does not establish a production wall-clock speedup or remove full scans.

Targeted verification: lifecycle records 32ff6b04b8df48dfb6a8179c4edb7eda
(56 passed), 1d1ab0f7176f4ce3870aec1fba9cc903 (17 selected contracts passed),
047017566c5c4ca193704b0b1c73e870 (2 additional cases passed). Cases cover
same-size/same-mtime changes, links, inventory changes, locks, calendar/module
changes, frozen original-fingerprint parity, and both complete hash passes.
The existing portable archive test from a911fa239b995031dcd1d8324c62d03f63e77374
remains a separately declared test source; the existing two-role verification contract is retained.
These are targeted software checks, not a new market signal or historical replay.
