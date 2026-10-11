from __future__ import annotations

import os
import smtplib
import ssl
from email.message import EmailMessage
from pathlib import Path


def send_mail(
    subject: str,
    body: str,
    attachment: str | Path | None = None,
    html_body: str | None = None,
) -> None:
    required = [
        "MAIL_SERVER",
        "MAIL_PORT",
        "MAIL_USERNAME",
        "MAIL_PASSWORD",
        "MAIL_FROM",
        "MAIL_TO",
    ]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise SystemExit("Missing required mail secrets: " + ", ".join(missing))

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = os.environ["MAIL_FROM"]
    message["To"] = os.environ["MAIL_TO"]
    message.set_content(body)
    if html_body:
        message.add_alternative(html_body, subtype="html")

    if attachment:
        path = Path(attachment)
        message.add_attachment(
            path.read_bytes(),
            maintype="text",
            subtype="markdown",
            filename=path.name,
        )

    server = os.environ["MAIL_SERVER"]
    port = int(os.environ["MAIL_PORT"])
    use_ssl = os.environ.get("MAIL_USE_SSL", "").strip().lower() in {"1", "true", "yes"} or port == 465
    context = ssl.create_default_context()
    if use_ssl:
        with smtplib.SMTP_SSL(server, port, context=context, timeout=30) as smtp:
            smtp.login(os.environ["MAIL_USERNAME"], os.environ["MAIL_PASSWORD"])
            refused = smtp.send_message(message)
            if refused:
                raise smtplib.SMTPRecipientsRefused(refused)
    else:
        with smtplib.SMTP(server, port, timeout=30) as smtp:
            smtp.starttls(context=context)
            smtp.login(os.environ["MAIL_USERNAME"], os.environ["MAIL_PASSWORD"])
            refused = smtp.send_message(message)
            if refused:
                raise smtplib.SMTPRecipientsRefused(refused)
