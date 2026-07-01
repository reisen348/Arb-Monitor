from __future__ import annotations

import os
import tempfile
import threading
import unittest
from collections import deque
from datetime import datetime, timedelta, timezone

from perp_arb.persistence import StateStore
from perp_arb.state import SnapshotStatePoint, OpportunityStatePoint, MarketStateTracker, OpportunityStateTracker


class StateStoreTest(unittest.TestCase):
    def setUp(self):
        self._tmpfile = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._tmpfile.close()
        self.db_path = self._tmpfile.name

    def tearDown(self):
        os.unlink(self.db_path)

    def test_snapshot_round_trip(self):
        store = StateStore(self.db_path)
        key = ("hyperliquid", "BTC", "USD")
        points = [
            SnapshotStatePoint(
                timestamp=datetime(2026, 3, 21, 10, i, tzinfo=timezone.utc),
                mark_price=68000.0 + i * 10,
                funding_rate_bps=2.0 + i * 0.1,
                oi_usd=100_000_000.0,
                premium_bps=1.5,
            )
            for i in range(5)
        ]
        store.save_snapshot_points(key, points)
        store.close()

        # Load in a fresh store instance
        store2 = StateStore(self.db_path)
        loaded = store2.load_snapshot_history(max_points=120)
        store2.close()

        self.assertIn(key, loaded)
        self.assertEqual(len(loaded[key]), 5)
        self.assertAlmostEqual(loaded[key][0].mark_price, 68000.0)
        self.assertAlmostEqual(loaded[key][4].mark_price, 68040.0)

    def test_opportunity_round_trip(self):
        store = StateStore(self.db_path)
        key = ("BTC", "USD", "drift", "hyperliquid")
        points = [
            OpportunityStatePoint(
                timestamp=datetime(2026, 3, 21, 10, i, tzinfo=timezone.utc),
                executable_spread_bps=5.0 + i,
            )
            for i in range(3)
        ]
        store.save_opportunity_points(key, points)
        store.close()

        store2 = StateStore(self.db_path)
        loaded = store2.load_opportunity_history(max_points=120)
        store2.close()

        self.assertIn(key, loaded)
        self.assertEqual(len(loaded[key]), 3)
        self.assertAlmostEqual(loaded[key][0].executable_spread_bps, 5.0)

    def test_source_timestamp_round_trip(self):
        store = StateStore(self.db_path)
        ts = datetime(2026, 3, 21, 12, 0, tzinfo=timezone.utc)
        store.save_source_timestamp("mock-hl", ts)
        store.close()

        store2 = StateStore(self.db_path)
        loaded = store2.load_source_timestamps()
        store2.close()

        self.assertIn("mock-hl", loaded)
        self.assertEqual(loaded["mock-hl"], ts)

    def test_restore_into_market_tracker(self):
        """Test that persisted history can be restored into a MarketStateTracker."""
        store = StateStore(self.db_path)
        key = ("hyperliquid", "ETH", "USD")
        points = [
            SnapshotStatePoint(
                timestamp=datetime(2026, 3, 21, 10, i, tzinfo=timezone.utc),
                mark_price=3400.0 + i,
                funding_rate_bps=1.0,
                oi_usd=50_000_000.0,
                premium_bps=0.5,
            )
            for i in range(10)
        ]
        store.save_snapshot_points(key, points)
        loaded = store.load_snapshot_history()
        store.close()

        tracker = MarketStateTracker()
        tracker.restore_history(loaded)
        self.assertEqual(len(tracker._history[key]), 10)

    def test_restore_into_opportunity_tracker(self):
        store = StateStore(self.db_path)
        key = ("ETH", "USD", "drift", "hyperliquid")
        points = [
            OpportunityStatePoint(
                timestamp=datetime(2026, 3, 21, 10, i, tzinfo=timezone.utc),
                executable_spread_bps=3.0 + i * 0.5,
            )
            for i in range(8)
        ]
        store.save_opportunity_points(key, points)
        loaded = store.load_opportunity_history()
        store.close()

        tracker = OpportunityStateTracker()
        tracker.restore_history(loaded)
        self.assertEqual(len(tracker._history[key]), 8)

    def test_max_points_trimming(self):
        store = StateStore(self.db_path)
        key = ("hyperliquid", "BTC", "USD")
        from datetime import timedelta
        base = datetime(2026, 3, 21, 10, 0, 0, tzinfo=timezone.utc)
        points = [
            SnapshotStatePoint(
                timestamp=base + timedelta(seconds=i),
                mark_price=68000.0 + i,
                funding_rate_bps=2.0,
                oi_usd=100_000_000.0,
                premium_bps=1.0,
            )
            for i in range(200)
        ]
        store.save_snapshot_points(key, points)
        loaded = store.load_snapshot_history(max_points=50)
        store.close()

        self.assertEqual(len(loaded[key]), 50)
        self.assertAlmostEqual(loaded[key][0].mark_price, 68150.0)
        self.assertAlmostEqual(loaded[key][-1].mark_price, 68199.0)

    def test_store_can_write_from_background_thread(self):
        store = StateStore(self.db_path)
        key = ("BTC", "USD", "drift", "hyperliquid")
        points = [
            OpportunityStatePoint(
                timestamp=datetime(2026, 3, 21, 10, i, tzinfo=timezone.utc),
                executable_spread_bps=5.0 + i,
            )
            for i in range(3)
        ]
        errors = []

        def worker():
            try:
                store.save_opportunity_points(key, points)
            except Exception as exc:
                errors.append(exc)

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join()
        self.assertEqual(errors, [])

        loaded = store.load_opportunity_history(max_points=120)
        store.close()
        self.assertIn(key, loaded)
        self.assertEqual(len(loaded[key]), 3)

    def test_count_snapshot_series_supports_lookback(self):
        store = StateStore(self.db_path)
        now = datetime.now(timezone.utc)
        recent_key = ("hyperliquid", "BTC", "USD")
        old_key = ("hyperliquid", "ETH", "USD")
        recent_points = [
            SnapshotStatePoint(
                timestamp=now - timedelta(hours=1),
                mark_price=68000.0,
                funding_rate_bps=1.0,
                oi_usd=100_000_000.0,
                premium_bps=1.0,
            )
        ]
        old_points = [
            SnapshotStatePoint(
                timestamp=now - timedelta(days=7),
                mark_price=3500.0,
                funding_rate_bps=1.0,
                oi_usd=50_000_000.0,
                premium_bps=1.0,
            )
        ]
        store.save_snapshot_points(recent_key, recent_points)
        store.save_snapshot_points(old_key, old_points)
        self.assertEqual(store.count_snapshot_series(), 2)
        self.assertEqual(store.count_snapshot_series(lookback_hours=24), 1)
        store.close()

    def test_flush_market_state_replaces_entire_table(self):
        store = StateStore(self.db_path)
        first_history = {
            ("hyperliquid", "BTC", "USD"): deque([
                SnapshotStatePoint(
                    timestamp=datetime(2026, 3, 21, 10, 0, tzinfo=timezone.utc),
                    mark_price=68000.0,
                    funding_rate_bps=1.0,
                    oi_usd=100_000_000.0,
                    premium_bps=1.0,
                )
            ])
        }
        second_history = {
            ("hyperliquid", "ETH", "USD"): deque([
                SnapshotStatePoint(
                    timestamp=datetime(2026, 3, 21, 10, 1, tzinfo=timezone.utc),
                    mark_price=3500.0,
                    funding_rate_bps=1.0,
                    oi_usd=50_000_000.0,
                    premium_bps=0.5,
                )
            ])
        }
        store.flush_market_state(first_history)
        store.flush_market_state(second_history)
        loaded = store.load_snapshot_history()
        store.close()

        self.assertNotIn(("hyperliquid", "BTC", "USD"), loaded)
        self.assertIn(("hyperliquid", "ETH", "USD"), loaded)
        self.assertEqual(len(loaded[("hyperliquid", "ETH", "USD")]), 1)

    def test_flush_opportunity_state_replaces_entire_table(self):
        store = StateStore(self.db_path)
        first_history = {
            ("BTC", "USD", "binance", "hyperliquid"): deque([
                OpportunityStatePoint(
                    timestamp=datetime(2026, 3, 21, 10, 0, tzinfo=timezone.utc),
                    executable_spread_bps=5.0,
                )
            ])
        }
        second_history = {
            ("ETH", "USD", "bybit", "grvt"): deque([
                OpportunityStatePoint(
                    timestamp=datetime(2026, 3, 21, 10, 1, tzinfo=timezone.utc),
                    executable_spread_bps=3.0,
                )
            ])
        }
        store.flush_opportunity_state(first_history)
        store.flush_opportunity_state(second_history)
        loaded = store.load_opportunity_history()
        store.close()

        self.assertNotIn(("BTC", "USD", "binance", "hyperliquid"), loaded)
        self.assertIn(("ETH", "USD", "bybit", "grvt"), loaded)
        self.assertEqual(len(loaded[("ETH", "USD", "bybit", "grvt")]), 1)

    def test_append_opportunity_state_archive_retains_recent_points(self):
        store = StateStore(self.db_path, archive_retention_hours=12.0)
        old_history = {
            ("BTC", "USD", "binance", "okx"): deque([
                OpportunityStatePoint(
                    timestamp=datetime.now(timezone.utc) - timedelta(hours=13),
                    executable_spread_bps=5.0,
                )
            ])
        }
        recent_history = {
            ("BTC", "USD", "binance", "okx"): deque([
                OpportunityStatePoint(
                    timestamp=datetime.now(timezone.utc) - timedelta(minutes=5),
                    executable_spread_bps=3.0,
                )
            ])
        }
        store.append_opportunity_state_archive(old_history)
        store.append_opportunity_state_archive(recent_history)
        loaded = store.load_opportunity_history_archive(max_points=3600, lookback_hours=12)
        store.close()

        self.assertIn(("BTC", "USD", "binance", "okx"), loaded)
        self.assertEqual(len(loaded[("BTC", "USD", "binance", "okx")]), 1)
        self.assertAlmostEqual(loaded[("BTC", "USD", "binance", "okx")][0].executable_spread_bps, 3.0)


if __name__ == "__main__":
    unittest.main()
