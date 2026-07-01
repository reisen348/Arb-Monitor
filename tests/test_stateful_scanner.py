from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from perp_arb.market_data import MarketSnapshot
from perp_arb.scanner import RealtimeScanner


class SequenceAdapter:
    def __init__(self, name: str, batches):
        self.name = name
        self._batches = list(batches)
        self._index = 0

    def poll(self):
        batch = self._batches[min(self._index, len(self._batches) - 1)]
        self._index += 1
        return list(batch)


def make_snapshot(
    venue: str,
    bid: float,
    ask: float,
    mark: float,
    funding_bps: float,
    oi_usd: float,
    timestamp: datetime,
) -> MarketSnapshot:
    return MarketSnapshot(
        venue=venue,
        market_type="perp_dex",
        asset="ETH",
        quote="USD" if venue == "hyperliquid" else "USDT",
        best_bid=bid,
        best_ask=ask,
        mark_price=mark,
        oracle_price=mark - 0.5,
        index_price=mark - 0.25,
        taker_fee_bps=2.5,
        depth_10k_usd=100_000.0,
        depth_50k_usd=250_000.0,
        top_1pct_depth_usd=300_000.0,
        volume_depth_ratio=5.0,
        oi_usd=oi_usd,
        funding_rate_bps=funding_bps,
        impact_cost_10k_bps=1.0,
        impact_cost_50k_bps=4.0,
        slippage_bps=1.0,
        latency_ms=100.0,
        staleness_ms=120.0,
        timestamp=timestamp,
    )


class StatefulScannerTest(unittest.TestCase):
    def test_scanner_fills_rolling_snapshot_and_opportunity_indicators(self) -> None:
        base_time = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)
        hyper_batches = [
            [make_snapshot("hyperliquid", 100.0, 100.2, 100.1, 1.0, 1_000_000.0, base_time)],
            [make_snapshot("hyperliquid", 100.8, 101.0, 100.9, 2.2, 1_080_000.0, base_time + timedelta(seconds=5))],
            [make_snapshot("hyperliquid", 101.6, 101.8, 101.7, 3.7, 1_160_000.0, base_time + timedelta(seconds=10))],
        ]
        grvt_batches = [
            [make_snapshot("grvt", 100.5, 100.7, 100.6, -0.8, 980_000.0, base_time)],
            [make_snapshot("grvt", 101.6, 101.8, 101.7, -1.9, 1_020_000.0, base_time + timedelta(seconds=5))],
            [make_snapshot("grvt", 103.0, 103.2, 103.1, -3.1, 1_120_000.0, base_time + timedelta(seconds=10))],
        ]
        scanner = RealtimeScanner([SequenceAdapter("hl", hyper_batches), SequenceAdapter("grvt", grvt_batches)])

        scanner.scan_once()
        second_batch = scanner.scan_once()
        third_batch = scanner.scan_once()

        second_hyper = next(snapshot for snapshot in second_batch.snapshots if snapshot.venue == "hyperliquid")
        self.assertNotEqual(second_hyper.funding_change_bps, 0.0)
        self.assertGreater(second_hyper.oi_change_pct, 0.0)
        self.assertGreater(second_hyper.jump_frequency, 0.0)
        self.assertNotEqual(second_hyper.trend_vs_mean_reversion, 0.0)

        third_opportunity = third_batch.opportunities[0]
        self.assertNotEqual(third_opportunity.spread_widening_speed_bps_per_min, 0.0)
        self.assertNotEqual(third_opportunity.spread_zscore, 0.0)


if __name__ == "__main__":
    unittest.main()
