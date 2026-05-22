from __future__ import annotations

import types
import unittest
from unittest import mock

import pandas as pd

import microcap_top100_mom16_biweekly_live_v2_3 as v23


class V23RealtimeStateOnlyTests(unittest.TestCase):
    def test_realtime_build_does_not_generate_full_v20_outputs(self) -> None:
        dates = pd.bdate_range("2026-04-01", periods=30)
        close_df = pd.DataFrame(
            {
                "microcap": [100.0 + i for i in range(len(dates))],
                "hedge": [1000.0 + i for i in range(len(dates))],
            },
            index=dates,
        )
        realtime_base = types.SimpleNamespace(
            realtime_close_df=close_df,
            meta={
                "snapshot_row_appended": True,
                "member_price_count": 100,
                "member_count": 100,
            },
            turnover_df=pd.DataFrame({"rebalance_date": dates[-2:]}),
            reference_summary={},
            context={"changes_df": pd.DataFrame()},
        )
        base_out = pd.DataFrame({"return_net": [0.0]}, index=[dates[-1]])
        signal_row = pd.DataFrame(
            {
                "date": [dates[-1]],
                "current_holding": ["cash"],
                "next_holding": ["cash"],
                "trade_state": ["hold"],
            }
        )
        common_index_calls: list[pd.DatetimeIndex] = []

        def fake_common_index(frame: pd.DataFrame, official_index: pd.DatetimeIndex) -> pd.DatetimeIndex:
            common_index_calls.append(pd.DatetimeIndex(official_index))
            return pd.DatetimeIndex(frame.index)

        with (
            mock.patch.object(v23, "ensure_output_dir"),
            mock.patch.object(v23.v2_0.realtime_core, "load_realtime_base", return_value=realtime_base),
            mock.patch.object(v23.v2_0, "generate_v2_0_outputs", side_effect=AssertionError("full v2.0 build called")),
            mock.patch.object(v23, "build_v2_3_common_index", side_effect=fake_common_index),
            mock.patch.object(v23, "build_spread_log_wls_gross", return_value=base_out),
            mock.patch.object(v23.v2_0.base_mod, "apply_momentum_gap_exit_buffer", side_effect=lambda frame, _buffer: frame),
            mock.patch.object(v23.v2_0.base_mod, "apply_momentum_gap_no_peak_decay_cost_model", side_effect=lambda frame, _turnover: frame),
            mock.patch.object(v23, "apply_target_vol", return_value=base_out),
            mock.patch.object(v23, "_build_signal_row", return_value=signal_row),
            mock.patch.object(
                v23.v2_0.realtime_core.base_mod,
                "augment_signal_with_member_rebalance",
                side_effect=lambda row, _changes: row,
            ),
            mock.patch.object(v23.v2_0.overlay_mod, "_apply_realtime_meta_columns_to_signal_row"),
            mock.patch.object(v23, "_atomic_write_text"),
        ):
            out_signal, meta, out = v23._build_realtime_v2_3_outputs_unlocked()

        self.assertEqual(meta["member_price_count"], 100)
        self.assertEqual(out_signal.iloc[0]["quote_coverage"], "100/100")
        self.assertIs(out, base_out)
        self.assertEqual(len(common_index_calls), 1)
        self.assertEqual(common_index_calls[0].max(), dates[-1])


if __name__ == "__main__":
    unittest.main()
