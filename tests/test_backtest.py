from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from perp_arb.backtest import build_backtest_payload, summarize_state_store
from perp_arb.persistence import StateStore
from perp_arb.state import OpportunityStatePoint, SnapshotStatePoint


class BacktestSummaryTest(unittest.TestCase):
    def setUp(self):
        self._tmpfile = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._tmpfile.close()
        self.db_path = self._tmpfile.name

    def tearDown(self):
        os.unlink(self.db_path)

    def test_summarize_state_store_returns_candidate_series(self):
        store = StateStore(self.db_path)
        snapshot_key = ("hyperliquid", "BTC", "USD")
        snapshot_points = [
            SnapshotStatePoint(
                timestamp=datetime(2026, 3, 21, 10, 0, tzinfo=timezone.utc) + timedelta(minutes=i),
                mark_price=68000.0 + i,
                funding_rate_bps=1.0,
                oi_usd=100_000_000.0,
                premium_bps=1.0,
            )
            for i in range(40)
        ]
        store.save_snapshot_points(snapshot_key, snapshot_points)

        opp_key = ("BTC", "USD", "binance", "hyperliquid")
        opp_points = [
            OpportunityStatePoint(
                timestamp=datetime(2026, 3, 21, 10, 0, tzinfo=timezone.utc) + timedelta(minutes=i),
                executable_spread_bps=5.0,
            )
            for i in range(39)
        ]
        opp_points.append(
            OpportunityStatePoint(
                timestamp=datetime(2026, 3, 21, 10, 39, tzinfo=timezone.utc),
                executable_spread_bps=15.0,
            )
        )
        store.save_opportunity_points(opp_key, opp_points)
        store.close()

        summary = summarize_state_store(self.db_path, min_samples=30, signal_zscore=2.0)
        self.assertEqual(summary.snapshot_series_count, 1)
        self.assertEqual(summary.opportunity_series_count, 1)
        self.assertEqual(summary.candidate_count, 1)
        self.assertEqual(summary.candidates[0].asset, "BTC")
        self.assertGreater(summary.candidates[0].latest_zscore, 2.0)
        self.assertEqual(summary.total_signal_count, 0)
        self.assertEqual(summary.total_hit_count, 0)

    def test_build_backtest_payload_serializes_summary(self):
        store = StateStore(self.db_path)
        opp_key = ("ETH", "USD", "bybit", "okx")
        opp_points = [
            OpportunityStatePoint(
                timestamp=datetime(2026, 3, 21, 10, 0, tzinfo=timezone.utc) + timedelta(minutes=i),
                executable_spread_bps=float(i),
            )
            for i in range(35)
        ]
        store.save_opportunity_points(opp_key, opp_points)
        store.close()

        summary = summarize_state_store(self.db_path, min_samples=10, signal_zscore=0.5)
        payload = build_backtest_payload(summary)
        self.assertIn("candidate_count", payload)
        self.assertIn("candidates", payload)
        self.assertIn("total_signal_count", payload)
        self.assertIn("asset_rankings", payload)
        self.assertIn("venue_pair_rankings", payload)
        self.assertIn("dashboard_summary", payload)
        self.assertIn("metric_cards", payload)
        self.assertIn("candidate_cards", payload)
        self.assertEqual(payload["opportunity_series_count"], 1)

    def test_backtest_counts_forward_reversion_hits(self):
        store = StateStore(self.db_path)
        opp_key = ("SOL", "USD", "binance", "bybit")
        spreads = [5.0] * 30 + [15.0, 10.0, 7.0, 6.0, 5.5]
        opp_points = [
            OpportunityStatePoint(
                timestamp=datetime(2026, 3, 21, 10, 0, tzinfo=timezone.utc) + timedelta(minutes=i),
                executable_spread_bps=spread,
            )
            for i, spread in enumerate(spreads)
        ]
        store.save_opportunity_points(opp_key, opp_points)
        store.close()

        summary = summarize_state_store(
            self.db_path,
            min_samples=20,
            signal_zscore=2.0,
            forward_points=4,
            reversion_ratio=0.5,
        )
        self.assertEqual(summary.total_signal_count, 1)
        self.assertEqual(summary.total_hit_count, 1)
        self.assertGreater(summary.total_hit_rate, 0.9)

    def test_backtest_hit_uses_reversion_toward_history_mean(self):
        store = StateStore(self.db_path)
        opp_key = ("BTC", "USD", "binance", "okx")
        spreads = [5.0] * 30 + [15.0, 11.0, 10.0, 9.8]
        opp_points = [
            OpportunityStatePoint(
                timestamp=datetime(2026, 3, 21, 10, 0, tzinfo=timezone.utc) + timedelta(minutes=i),
                executable_spread_bps=spread,
            )
            for i, spread in enumerate(spreads)
        ]
        store.save_opportunity_points(opp_key, opp_points)
        store.close()

        summary = summarize_state_store(
            self.db_path,
            min_samples=20,
            signal_zscore=2.0,
            forward_points=3,
            reversion_ratio=0.5,
        )
        self.assertEqual(summary.total_signal_count, 1)
        self.assertEqual(summary.total_hit_count, 1)

    def test_fixed_spread_threshold_can_trigger_signal_without_high_zscore(self):
        store = StateStore(self.db_path)
        opp_key = ("ARB", "USD", "binance", "bybit")
        spreads = [5.0, 5.1, 4.9, 5.0, 5.2, 4.8, 5.0, 5.1, 11.5]
        points = [
            OpportunityStatePoint(
                timestamp=datetime(2026, 3, 21, 10, 0, tzinfo=timezone.utc) + timedelta(minutes=i),
                executable_spread_bps=spread,
            )
            for i, spread in enumerate(spreads)
        ]
        store.save_opportunity_points(opp_key, points)
        store.close()

        summary = summarize_state_store(
            self.db_path,
            min_samples=5,
            signal_zscore=99.0,
            min_signal_spread_bps=4.0,
            forward_points=2,
            reversion_ratio=0.8,
        )
        self.assertEqual(summary.candidate_count, 1)
        self.assertEqual(summary.candidates[0].asset, "ARB")

    def test_lookback_hours_filters_out_old_history(self):
        store = StateStore(self.db_path)
        old_base = datetime.now(timezone.utc) - timedelta(hours=48)
        recent_base = datetime.now(timezone.utc) - timedelta(hours=2)
        old_key = ("BTC", "USD", "binance", "okx")
        recent_key = ("ETH", "USD", "bybit", "hyperliquid")
        old_points = [
            OpportunityStatePoint(
                timestamp=old_base + timedelta(minutes=i),
                executable_spread_bps=5.0 + i,
            )
            for i in range(35)
        ]
        recent_points = [
            OpportunityStatePoint(
                timestamp=recent_base + timedelta(minutes=i),
                executable_spread_bps=10.0 if i == 34 else 5.0,
            )
            for i in range(35)
        ]
        store.save_opportunity_points(old_key, old_points)
        store.save_opportunity_points(recent_key, recent_points)
        store.close()

        summary = summarize_state_store(
            self.db_path,
            min_samples=10,
            signal_zscore=2.0,
            lookback_hours=6,
        )
        self.assertEqual(summary.opportunity_series_count, 1)
        self.assertEqual(summary.candidate_count, 1)
        self.assertEqual(summary.candidates[0].asset, "ETH")

    def test_backtest_prefers_archive_history_for_longer_window(self):
        store = StateStore(self.db_path, archive_retention_hours=12.0)
        archive_points = [
            OpportunityStatePoint(
                timestamp=datetime.now(timezone.utc) - timedelta(hours=11, minutes=30) + timedelta(minutes=i),
                executable_spread_bps=5.0 if i < 39 else 12.0,
            )
            for i in range(40)
        ]
        rolling_points = [
            OpportunityStatePoint(
                timestamp=datetime.now(timezone.utc) - timedelta(minutes=39 - i),
                executable_spread_bps=5.0,
            )
            for i in range(40)
        ]
        store.save_opportunity_points(("BTC", "USD", "binance", "okx"), rolling_points)
        for point in archive_points:
            store.append_opportunity_state_archive({
                ("BTC", "USD", "binance", "okx"): [point]
            })
        store.close()

        summary = summarize_state_store(
            self.db_path,
            min_samples=30,
            signal_zscore=2.0,
            lookback_hours=12,
            max_points=3600,
        )
        self.assertEqual(summary.opportunity_series_count, 1)
        self.assertEqual(summary.candidate_count, 1)

    def test_backtest_builds_asset_and_venue_pair_rankings(self):
        store = StateStore(self.db_path)
        series = [
            ("BTC", "USD", "binance", "hyperliquid"),
            ("BTC", "USD", "okx", "hyperliquid"),
            ("ETH", "USD", "binance", "bybit"),
        ]
        for offset, key in enumerate(series):
            spreads = [5.0] * 30 + [12.0 + offset]
            points = [
                OpportunityStatePoint(
                    timestamp=datetime(2026, 3, 21, 10, 0, tzinfo=timezone.utc) + timedelta(minutes=i + offset),
                    executable_spread_bps=spread,
                )
                for i, spread in enumerate(spreads)
            ]
            store.save_opportunity_points(key, points)
        store.close()

        summary = summarize_state_store(self.db_path, min_samples=20, signal_zscore=2.0, top_n=10)
        self.assertTrue(summary.asset_rankings)
        self.assertTrue(summary.venue_pair_rankings)
        self.assertEqual(summary.asset_rankings[0].key, "BTC/USD")
        self.assertEqual(summary.asset_rankings[0].candidate_count, 2)


if __name__ == "__main__":
    unittest.main()
