"""Read-only identity mutation probe against the current final signal rows."""

import csv
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from scripts import top100_delivery as delivery  # noqa: E402


CASES = {
    "0": (delivery.plain_v20_identity, {"lookback": "17", "fixed_hedge_ratio": "0.7"}),
    "3": (
        delivery.plain_v23_identity,
        {
            "lookback": "26",
            "halflife": "3.0",
            "execution_hedge_ratio": "0.7",
            "overheat_feature_window": "9",
            "overheat_enabled": "False",
        },
    ),
    "5": (delivery.plain_v25_identity, {"overheat_enabled": "True"}),
}


def main() -> None:
    result = {"source": str(Path(delivery.__file__).relative_to(ROOT)), "versions": {}}
    for version, (check, changes) in CASES.items():
        path = ROOT / "outputs" / f"microcap_top100_mom16_biweekly_live_v2_{version}_latest_signal.csv"
        with path.open(encoding="utf-8-sig", newline="") as stream:
            rows = list(csv.DictReader(stream))
        assert len(rows) == 1
        row = rows[0]
        result["versions"][version] = {
            "path": str(path.relative_to(ROOT)),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "date": row["date"],
            "original_accepted": check(row),
            "mutations": [
                {
                    "field": field,
                    "original": row.get(field),
                    "mutated": value,
                    "accepted": check({**row, field: value}),
                }
                for field, value in changes.items()
            ],
        }
    output = Path(__file__).with_name("identity_probe.json")
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
