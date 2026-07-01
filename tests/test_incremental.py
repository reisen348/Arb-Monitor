"""Tests for incremental statistics helpers in state.py."""
from __future__ import annotations

import math
import statistics
import unittest
from datetime import datetime, timedelta, timezone

from perp_arb.state import OpportunityStateTracker, RunningStats, JumpCounter, ReturnAccumulator, StateTrackerConfig


class RunningStatsTest(unittest.TestCase):
    def test_mean_and_pstdev_match_stdlib(self):
        values = [1.0, 3.0, 5.0, 7.0, 2.0, 4.0, 6.0]
        rs = RunningStats(max_n=120)
        for v in values:
            rs.push(v)
        self.assertAlmostEqual(rs.mean, statistics.mean(values), places=10)
        self.assertAlmostEqual(rs.pstdev, statistics.pstdev(values), places=10)

    def test_window_eviction(self):
        rs = RunningStats(max_n=3)
        for v in [10.0, 20.0, 30.0, 40.0]:
            rs.push(v)
        # Window should contain [20, 30, 40]
        self.assertEqual(rs.count, 3)
        self.assertAlmostEqual(rs.mean, 30.0)

    def test_zscore_min_points_semantics(self):
        """min_points counts total values including new_value."""
        rs = RunningStats(max_n=120)
        rs.push(1.0)
        rs.push(2.0)
        # count=2, min_points=3 → count+1=3 >= 3 → should compute
        result = rs.zscore(3.0, min_points=3)
        self.assertIsNotNone(result)
        # count=2, min_points=4 → count+1=3 < 4 → None
        result2 = rs.zscore(3.0, min_points=4)
        self.assertIsNone(result2)

    def test_zscore_returns_none_when_insufficient_baseline(self):
        rs = RunningStats(max_n=120)
        rs.push(5.0)
        # count=1, min_points=3 → count+1=2 < 3 → None
        self.assertIsNone(rs.zscore(6.0, min_points=3))

    def test_zscore_zero_for_constant_baseline(self):
        rs = RunningStats(max_n=120)
        for _ in range(5):
            rs.push(10.0)
        self.assertEqual(rs.zscore(10.0, min_points=3), 0.0)

    def test_restore_rebuilds_state(self):
        rs1 = RunningStats(max_n=120)
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        for v in values:
            rs1.push(v)

        rs2 = RunningStats(max_n=120)
        rs2.restore(values)

        self.assertAlmostEqual(rs1.mean, rs2.mean)
        self.assertAlmostEqual(rs1.pstdev, rs2.pstdev)


class OpportunityStateTrackerTest(unittest.TestCase):
    def test_uses_dedicated_opportunity_window_points(self):
        config = StateTrackerConfig(max_points=600, opportunity_max_points=4)
        tracker = OpportunityStateTracker(config)

        self.assertEqual(config.opportunity_points, 4)
        self.assertEqual(tracker._history[("BTC", "USD", "binance", "okx")].maxlen, 4)


class JumpCounterTest(unittest.TestCase):
    def test_jump_frequency_basic(self):
        jc = JumpCounter(threshold_bps=8.0, window_seconds=60.0)
        base = datetime(2026, 3, 21, 12, 0, tzinfo=timezone.utc)
        # Push prices with a big jump
        jc.push(base, 100.0)
        jc.push(base + timedelta(seconds=1), 100.0)  # no jump
        freq = jc.push(base + timedelta(seconds=2), 100.5)  # ~50 bps jump
        self.assertGreater(freq, 0.0)

    def test_window_eviction(self):
        jc = JumpCounter(threshold_bps=8.0, window_seconds=5.0)
        base = datetime(2026, 3, 21, 12, 0, tzinfo=timezone.utc)
        jc.push(base, 100.0)
        jc.push(base + timedelta(seconds=1), 100.5)  # big jump
        # Push a point far in the future — old points should be evicted
        freq = jc.push(base + timedelta(seconds=60), 100.5)
        # Only 1 point in window after eviction → 0.0
        self.assertEqual(freq, 0.0)

    def test_no_jumps_returns_zero(self):
        jc = JumpCounter(threshold_bps=8.0, window_seconds=60.0)
        base = datetime(2026, 3, 21, 12, 0, tzinfo=timezone.utc)
        # Very small price changes
        jc.push(base, 100.0)
        freq = jc.push(base + timedelta(seconds=1), 100.001)
        self.assertEqual(freq, 0.0)


class ReturnAccumulatorTest(unittest.TestCase):
    def test_realized_vol_matches_full_pass(self):
        """Incremental vol should match the batch formula for same data."""
        prices = [100.0, 100.5, 99.8, 101.0, 100.2, 100.9, 99.5]
        acc = ReturnAccumulator(max_n=120)
        for i in range(1, len(prices)):
            acc.push(prices[i - 1], prices[i])

        # Compute batch returns
        returns = [math.log(prices[i] / prices[i - 1]) * 10_000 for i in range(1, len(prices))]
        sigma = statistics.pstdev(returns)
        expected_vol = min(100.0, sigma * math.sqrt(len(returns)) * 0.9)
        self.assertAlmostEqual(acc.realized_vol_score, expected_vol, places=5)

    def test_trend_efficiency_pure_uptrend(self):
        acc = ReturnAccumulator(max_n=120)
        prices = [100.0, 101.0, 102.0, 103.0]
        for i in range(1, len(prices)):
            acc.push(prices[i - 1], prices[i])
        # All returns positive → efficiency close to 1.0
        self.assertGreater(acc.trend_efficiency, 0.9)

    def test_window_eviction(self):
        acc = ReturnAccumulator(max_n=3)
        prices = [100.0, 101.0, 102.0, 103.0, 104.0]
        for i in range(1, len(prices)):
            acc.push(prices[i - 1], prices[i])
        # Window should have 3 returns
        self.assertEqual(acc._count, 3)


if __name__ == "__main__":
    unittest.main()
