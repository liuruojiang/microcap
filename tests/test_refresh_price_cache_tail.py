from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

import microcap_top100_mom16_biweekly_live as live


class RefreshPriceCacheTailTests(unittest.TestCase):
    def test_failure_audit_records_stage_and_exception_message(self) -> None:
        symbols = [f"{idx:06d}" for idx in range(21)]

        def fail_price(symbol: str, *_args: object, **_kwargs: object) -> None:
            raise RuntimeError(f"price source rejected {symbol}")

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            with (
                mock.patch.object(live, "OUTPUT_DIR", output_dir),
                mock.patch.object(live.freq_mod, "load_current_universe", return_value=symbols),
                mock.patch.object(live.freq_mod, "START_DATE", "2025-01-02"),
                mock.patch.object(live.fetch_mod, "fetch_price_history", side_effect=fail_price),
            ):
                with self.assertRaisesRegex(RuntimeError, "price source rejected 000000"):
                    live.refresh_price_cache_tail(pd.Timestamp("2026-05-21"), max_workers=1)

            audit_path = output_dir / "price_cache_refresh_failures_2026-05-21.csv"
            audit = pd.read_csv(audit_path)

        self.assertEqual(set(["symbol", "stage", "error_type", "error"]).issubset(audit.columns), True)
        self.assertEqual(audit.loc[0, "stage"], "price")
        self.assertEqual(audit.loc[0, "error_type"], "RuntimeError")
        self.assertIn("price source rejected 000000", audit.loc[0, "error"])


if __name__ == "__main__":
    unittest.main()
