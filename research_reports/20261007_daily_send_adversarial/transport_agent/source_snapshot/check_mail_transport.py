"""Verify the production SMTP route without submitting a message body."""
from __future__ import annotations

import argparse
import json
import os
import smtplib
import ssl
from email.utils import getaddresses
from pathlib import Path


def probe() -> dict:
    required = ("MAIL_SERVER", "MAIL_PORT", "MAIL_USERNAME", "MAIL_PASSWORD", "MAIL_FROM", "MAIL_TO")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ValueError("Missing required mail secrets: " + ", ".join(missing))
    port = int(os.environ["MAIL_PORT"])
    sender = [address for _, address in getaddresses([os.environ["MAIL_FROM"]])]
    recipients = [address for _, address in getaddresses([os.environ["MAIL_TO"]])]
    if len(sender) != 1 or not recipients or any("@" not in address for address in sender + recipients):
        raise ValueError("Invalid sender or recipient configuration")
    use_ssl = os.environ.get("MAIL_USE_SSL", "").strip().lower() in {"1", "true", "yes"} or port == 465
    context = ssl.create_default_context()
    if use_ssl:
        client = smtplib.SMTP_SSL(os.environ["MAIL_SERVER"], port, context=context, timeout=30)
    else:
        client = smtplib.SMTP(os.environ["MAIL_SERVER"], port, timeout=30)
    with client as smtp:
        smtp.ehlo()
        if not use_ssl:
            smtp.starttls(context=context)
            smtp.ehlo()
        smtp.login(os.environ["MAIL_USERNAME"], os.environ["MAIL_PASSWORD"])
        code, _ = smtp.mail(sender[0])
        if code != 250:
            raise RuntimeError("SMTP rejected sender envelope")
        try:
            for recipient in recipients:
                code, _ = smtp.rcpt(recipient)
                if code not in {250, 251}:
                    raise RuntimeError("SMTP rejected recipient envelope")
        finally:
            code, _ = smtp.rset()
            if code != 250:
                raise RuntimeError("SMTP did not confirm envelope reset")
    return {"status": "ok", "tls_verified": True, "authenticated": True,
            "sender_envelope_accepted": True, "recipients_accepted": len(recipients),
            "envelope_reset": True, "message_sent": False}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    try:
        result = probe()
    except Exception as exc:
        result = {"status": "failed", "exception_type": type(exc).__name__,
                  "smtp_code": getattr(exc, "smtp_code", None), "message_sent": False}
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result))
    return 0 if result["status"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
