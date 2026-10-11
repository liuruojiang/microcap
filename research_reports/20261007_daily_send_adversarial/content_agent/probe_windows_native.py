"""Preserve the native Windows newline compatibility regression and repair."""
import csv
import importlib.util
import json
from pathlib import Path

import pandas as pd

import probe_content as probe

HERE = Path(__file__).resolve().parent
BEFORE = probe.AUTO / ".codex_backups/20261007_231652/scripts/build_microcap_realtime_digest.py"


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def main():
    before = load("windows_before_reader", BEFORE)
    after = load("windows_after_reader", probe.BUILDER)
    records = []
    for version in probe.VERSIONS:
        filename = f"microcap_top100_mom16_biweekly_live_{version.replace('.', '_')}_latest_signal.csv"
        path = probe.ROOT / "outputs" / filename
        payload = path.read_bytes()
        with path.open(encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.reader(handle, strict=True))
        dataframe = pd.read_csv(path)
        old_row, old_note = before.read_last_csv_row(str(path))
        row, note = after.read_last_csv_row(str(path))
        assert len(dataframe) == 1
        assert sum(value != [] for value in rows) == 2  # One header and one signal.
        assert not old_row and old_note == "final signal CSV must contain exactly one signal row"
        assert not note and row["date"] == "2026-09-30"
        records.append({"version": version, "path": str(path), "sha256": probe.hash_file(path),
                        "pandas_data_rows": len(dataframe), "raw_csv_records": len(rows),
                        "completely_empty_records": sum(value == [] for value in rows),
                        "nonempty_records_including_header": sum(value != [] for value in rows),
                        "signal_data_rows": sum(value != [] for value in rows) - 1,
                        "crcrlf_separators": payload.count(b"\r\r\n"),
                        "before_note": old_note, "after_note": note,
                        "after_identity_version": row["version"], "after_signal_date": row["date"]})
    metadata = json.loads((HERE / "native_windows_preview/metadata.json").read_text(encoding="utf-8"))
    assert metadata["status"] == "OK" and metadata["signal_date"] == "2026-09-30"
    assert metadata["publication_mode"] == "close_confirmed" and "不作为今日交易指令" in metadata["body"]
    result = {"scope": "native_windows_single_signal_csv_empty_record_compatibility_no_smtp",
              "before_builder_sha256": probe.hash_file(BEFORE), "after_builder_sha256": probe.hash_file(probe.BUILDER),
              "versions": records, "primary_csv_modified": False, "signals_reexecuted_by_agent": False,
              "native_full_log_digest_metadata_path": str(HERE / "native_windows_preview/metadata.json"),
              "digest_status": metadata["status"], "signal_date": metadata["signal_date"],
              "sent_mail": False, "note": "All three native files contain one nonempty signal data row, not multiple signals."}
    (HERE / "windows_native_compatibility.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"native_versions_passed": len(records), "digest_status": metadata["status"],
                      "signal_date": metadata["signal_date"], "primary_csv_modified": False, "sent_mail": False}))


if __name__ == "__main__":
    main()
