from __future__ import annotations

import json
import sys
from pathlib import Path

from mail_utils import send_mail


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: send_report.py <metadata.json>")
    meta = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    send_mail(
        meta["subject"],
        meta["body"],
        meta.get("attachment"),
        html_body=meta.get("html_body"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
