"""Retain the complete CSV read contract without materializing every row."""
import csv

import pytest

from scripts.realtime_state_bundle import _read_csv_header_and_rows


def original(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        header = list(reader.fieldnames or [])
        rows = list(reader)
    return header, len(rows), rows[-1] if rows else None


@pytest.mark.parametrize("raw", [
    b"", b"\n", b"date,value\n", b"\xef\xbb\xbfdate,value\r\n2026-11-04,1\r\n",
    b"date,value\n2026-11-03,1\n\n2026-11-04,2\n",
    b'date,value\n2026-11-03,"line1\nline2"\n2026-11-04,3\n',
    b"date,value\n2026-11-03\n2026-11-04,2,extra\n",
    b"date,date\n2026-11-03,2026-11-04\n",
    b"date,value\n" + b"2026-11-03,1\n" * 10000 + b"2026-11-04,2\n",
], ids=["empty", "blank", "header", "bom", "blank-record", "multiline", "uneven", "duplicate-header", "large"])
def test_exact_original_csv_contract(tmp_path, raw):
    path = tmp_path / "state.csv"
    path.write_bytes(raw)
    assert _read_csv_header_and_rows(path) == original(path)


@pytest.mark.parametrize("raw", [b"date,value\n2026-11-04,\xff\n", b"date,value\n" + b"x" * 140000], ids=["invalid-utf8", "oversized-field"])
def test_parse_failure_not_hidden(tmp_path, raw):
    path = tmp_path / "state.csv"
    path.write_bytes(raw)
    with pytest.raises((UnicodeDecodeError, csv.Error)) as expected:
        original(path)
    with pytest.raises(type(expected.value)):
        _read_csv_header_and_rows(path)


def test_reads_through_final_record(tmp_path, monkeypatch):
    path = tmp_path / "state.csv"
    path.write_text("date,value\n2026-11-04,1\n", encoding="utf8")
    real = csv.DictReader
    def reader(handle):
        inner = real(handle)
        class Rows:
            fieldnames = inner.fieldnames
            def __iter__(self):
                yield from inner
                raise OSError("late read failed")
        return Rows()
    monkeypatch.setattr(csv, "DictReader", reader)
    with pytest.raises(OSError, match="late read failed"):
        _read_csv_header_and_rows(path)
