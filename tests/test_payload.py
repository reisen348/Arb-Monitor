from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from perp_arb.market_data import MarketSnapshot, ScanBatch, SourceStatus
from perp_arb.models import OpportunityBucket, PerpArbOpportunity, PerpLegSnapshot
from perp_arb.payload import build_batch_payload
from perp_arb.scoring import ArbitrageScorer


class PayloadSourceLatencyTest(unittest.TestCase):
    def test_source_status_prefers_snapshot_latency_for_ws_sources(self) -> None:
        batch = ScanBatch(
            timestamp=datetime.now(timezone.utc),
            snapshots=[
                MarketSnapshot(
                    venue="binance",
                    market_type="perp",
                    asset="BTC",
                    quote="USD",
                    best_bid=1.0,
                    best_ask=1.1,
                    mark_price=1.05,
                    oracle_price=1.05,
                    index_price=1.05,
                    taker_fee_bps=1.0,
                    latency_ms=87.0,
                    staleness_ms=0.0,
                )
            ],
            opportunities=[],
            scored_opportunities=[],
            source_statuses=[
                SourceStatus(
                    adapter_name="binance-ws",
                    ok=True,
                    snapshot_count=1,
                    poll_duration_ms=0.0,
                    timestamp=datetime.now(timezone.utc),
                )
            ],
            scan_duration_ms=10.0,
        )

        payload = build_batch_payload(batch)
        self.assertEqual(payload["source_statuses"][0]["poll_duration_ms"], 0.0)
        self.assertEqual(payload["source_statuses"][0]["display_latency_ms"], 87.0)
        self.assertEqual(payload["source_statuses"][0]["max_snapshot_latency_ms"], 87.0)

    def test_source_status_uses_typical_latency_not_single_stale_market(self) -> None:
        snapshots = []
        for asset, staleness_ms in (("BTC", 20.0), ("ETH", 30.0), ("SOL", 45_000.0)):
            snapshots.append(
                MarketSnapshot(
                    venue="bybit",
                    market_type="perp",
                    asset=asset,
                    quote="USD",
                    best_bid=1.0,
                    best_ask=1.1,
                    mark_price=1.05,
                    oracle_price=1.05,
                    index_price=1.05,
                    taker_fee_bps=1.0,
                    latency_ms=10.0,
                    staleness_ms=staleness_ms,
                )
            )
        batch = ScanBatch(
            timestamp=datetime.now(timezone.utc),
            snapshots=snapshots,
            opportunities=[],
            scored_opportunities=[],
            source_statuses=[
                SourceStatus(
                    adapter_name="bybit-ws",
                    ok=True,
                    snapshot_count=len(snapshots),
                    poll_duration_ms=0.0,
                    timestamp=datetime.now(timezone.utc),
                )
            ],
            scan_duration_ms=10.0,
        )

        payload = build_batch_payload(batch)
        self.assertEqual(payload["source_statuses"][0]["display_latency_ms"], 30.0)
        self.assertEqual(payload["source_statuses"][0]["max_snapshot_latency_ms"], 45_000.0)

    def test_opportunity_payload_exposes_estimation_notional(self) -> None:
        now = datetime.now(timezone.utc)
        leg_a = PerpLegSnapshot(
            venue="bybit",
            market_type="perp",
            asset="LIT",
            best_bid=1.000,
            best_ask=1.002,
            mark_price=1.001,
            oracle_price=1.001,
            index_price=1.001,
            taker_fee_bps=2.0,
            maker_fee_bps=0.4,
            depth_10k_usd=80_000.0,
            depth_50k_usd=200_000.0,
            top_1pct_depth_usd=250_000.0,
            volume_depth_ratio=5.0,
            oi_usd=5_000_000.0,
            funding_rate_bps=0.0,
            funding_interval_hours=1.0,
            next_funding_time=now + timedelta(hours=1),
        )
        leg_b = PerpLegSnapshot(
            venue="lighter",
            market_type="perp_dex",
            asset="LIT",
            best_bid=1.020,
            best_ask=1.022,
            mark_price=1.021,
            oracle_price=1.021,
            index_price=1.021,
            taker_fee_bps=1.5,
            maker_fee_bps=-0.1,
            depth_10k_usd=80_000.0,
            depth_50k_usd=200_000.0,
            top_1pct_depth_usd=250_000.0,
            volume_depth_ratio=5.0,
            oi_usd=5_000_000.0,
            funding_rate_bps=0.0,
            funding_interval_hours=8.0,
            next_funding_time=now + timedelta(hours=8),
        )
        opportunity = PerpArbOpportunity(
            asset="LIT",
            quote="USD",
            leg_a=leg_a,
            leg_b=leg_b,
            notional_usd=12_345.0,
            capital_used_usd=3_086.25,
            slippage_bps=1.0,
            impact_cost_10k_bps=1.0,
            impact_cost_50k_bps=3.0,
            spread_zscore=3.0,
            spread_mean_bps=1.0,
            funding_persistence_score=0.7,
            bucket_hint=OpportunityBucket.DISLOCATION,
            now=now,
            metadata={"leg_a_ticker_only": False, "leg_b_ticker_only": True},
        )
        scored = ArbitrageScorer().score(opportunity)
        batch = ScanBatch(
            timestamp=now,
            snapshots=[],
            opportunities=[opportunity],
            scored_opportunities=[scored],
            source_statuses=[],
            scan_duration_ms=1.0,
        )

        payload = build_batch_payload(batch)
        serialized = payload["opportunities"][0]

        self.assertEqual(serialized["notional_usd"], 12_345.0)
        self.assertEqual(serialized["capital_used_usd"], 3_086.25)
        self.assertEqual(serialized["oi_a_usd"], 5_000_000.0)
        self.assertEqual(serialized["oi_b_usd"], 5_000_000.0)
        self.assertIn("volume_a_24h_usd", serialized)
        self.assertIn("volume_b_24h_usd", serialized)
        self.assertEqual(serialized["funding_a_interval_h"], 1.0)
        self.assertEqual(serialized["funding_b_interval_h"], 8.0)
        self.assertEqual(serialized["next_funding_a"], (now + timedelta(hours=1)).isoformat())
        self.assertEqual(serialized["next_funding_b"], (now + timedelta(hours=8)).isoformat())
        self.assertFalse(serialized["venue_a_ticker_only"])
        self.assertTrue(serialized["venue_b_ticker_only"])
        self.assertEqual(serialized["maker_fee_a_bps"], 0.4)
        self.assertEqual(serialized["taker_fee_a_bps"], 2.0)
        self.assertEqual(serialized["maker_fee_b_bps"], -0.1)
        self.assertEqual(serialized["taker_fee_b_bps"], 1.5)
        self.assertEqual(serialized["best_bid_a"], 1.000)
        self.assertEqual(serialized["best_ask_a"], 1.002)
        self.assertEqual(serialized["best_bid_b"], 1.020)
        self.assertEqual(serialized["best_ask_b"], 1.022)
        self.assertAlmostEqual(
            serialized["expected_profit_usd"],
            serialized["notional_usd"] * serialized["expected_profit_bps"] / 10_000.0,
            places=4,
        )


if __name__ == "__main__":
    unittest.main()
