from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd
import requests

import microcap_top100_mom16_biweekly_live as live


class RefreshPriceCacheTailTests(unittest.TestCase):
    def test_retries_transient_price_refresh_disconnect(self) -> None:
        symbols = ["000001"]
        calls = {"price": 0}

        def flaky_price(symbol: str, *_args: object, **_kwargs: object) -> pd.DataFrame:
            calls["price"] += 1
            if calls["price"] == 1:
                raise requests.exceptions.ConnectionError("RemoteDisconnected test")
            return pd.DataFrame({"date": [pd.Timestamp("2026-05-21")], "close_raw": [10.0]})

        with (
            mock.patch.object(live, "PRICE_REFRESH_RETRY_DELAY_SECONDS", 0.0),
            mock.patch.object(live.freq_mod, "load_current_universe", return_value=symbols),
            mock.patch.object(live.freq_mod, "START_DATE", "2025-01-02"),
            mock.patch.object(live.fetch_mod, "fetch_price_history", side_effect=flaky_price),
            mock.patch.object(live.fetch_mod, "fetch_share_change", return_value=pd.DataFrame()),
        ):
            live.refresh_price_cache_tail(pd.Timestamp("2026-05-21"), max_workers=1)

        self.assertEqual(calls["price"], 2)

    def test_creates_underlying_fetch_cache_directories_before_refresh(self) -> None:
        symbols = ["000001"]

        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp) / ".microcap_index_cache"
            price_dir = cache_dir / "prices_raw"
            share_dir = cache_dir / "share_change"

            def write_price(symbol: str, *_args: object, **_kwargs: object) -> pd.DataFrame:
                frame = pd.DataFrame({"date": [pd.Timestamp("2026-05-21")], "close_raw": [10.0]})
                frame.to_csv(price_dir / f"{symbol}.csv", index=False)
                return frame

            def write_share(symbol: str, *_args: object, **_kwargs: object) -> pd.DataFrame:
                frame = pd.DataFrame(
                    {
                        "change_date": [pd.Timestamp("2026-05-21")],
                        "total_shares_10k": [100.0],
                        "reason": ["test"],
                    }
                )
                frame.to_csv(share_dir / f"{symbol}.csv", index=False)
                return frame

            with (
                mock.patch.object(live.fetch_mod, "ensure_dirs", None, create=True),
                mock.patch.object(live.fetch_mod, "PRICE_CACHE_DIR", price_dir),
                mock.patch.object(live.fetch_mod, "ADJ_PRICE_CACHE_DIR", cache_dir / "prices_qfq"),
                mock.patch.object(live.fetch_mod, "SHARE_CACHE_DIR", share_dir),
                mock.patch.object(live.freq_mod, "PRICE_DIR", price_dir),
                mock.patch.object(live.freq_mod, "ADJ_PRICE_DIR", cache_dir / "prices_qfq"),
                mock.patch.object(live.freq_mod, "SHARE_DIR", share_dir),
                mock.patch.object(live.freq_mod, "load_current_universe", return_value=symbols),
                mock.patch.object(live.freq_mod, "START_DATE", "2025-01-02"),
                mock.patch.object(live.fetch_mod, "fetch_price_history", side_effect=write_price),
                mock.patch.object(live.fetch_mod, "fetch_share_change", side_effect=write_share),
            ):
                live.refresh_price_cache_tail(pd.Timestamp("2026-05-21"), max_workers=1)

            self.assertTrue((price_dir / "000001.csv").exists())
            self.assertTrue((share_dir / "000001.csv").exists())

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
