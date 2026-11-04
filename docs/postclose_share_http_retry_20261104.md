# Public share-source transient transport repair

Actual no-send state producer run41816389671 restored the original approved archive and passed the exact source-only transition. Its strict November 4 refresh reached the 102-stock verified input refresh, then failed on the 003042 public quote endpoint with HTTP502. No SMTP, delivery intent, accepted receipt or delivered marker ran.

The unchanged-tail provider previously made one public HTTPS request per observation. Each failed connection/timeout or HTTP500/502/503/504 now has at most one retry of that same endpoint and parameters, with the original20-second request timeout and TLS verification. Failed HTTP bytes and allowlisted receipt are retained separately from any successful observation. Transport failures with no response have their own diagnostics. TLS failure,403/429 and other permanent status codes are not retried. Invalid successful data is not retried or promoted.

All original issuer/date/inventory/share-count/source-clock/future-plan/previous-state checks remain. The CNINFO stop and its archived response stay intact. No new provider, strategy parameter, historical change, authority migration, old quote fallback, SMTP path or schedule is introduced. The full original share-source regression and executable transient/permanent/data-invalid counterexamples run together.
