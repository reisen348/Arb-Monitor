from __future__ import annotations

import unittest
from datetime import datetime

from perp_arb.models import OpportunityBucket, PerpArbOpportunity, PerpLegSnapshot
from perp_arb.practice import PracticeGateConfig, PracticeTracker
from perp_arb.scoring import ArbitrageScorer


def make_leg(venue: str, bid: float, ask: float) -> PerpLegSnapshot:
    mark = (bid + ask) / 2.0
    return PerpLegSnapshot(
        venue=venue,
        market_type="perp",
        asset="ETH",
        best_bid=bid,
        best_ask=ask,
        mark_price=mark,
        oracle_price=mark,
        index_price=mark,
        taker_fee_bps=2.0,
        maker_fee_bps=0.0,
        depth_10k_usd=150_000.0,
        depth_50k_usd=300_000.0,
        top_1pct_depth_usd=350_000.0,
        volume_depth_ratio=8.0,
        oi_usd=20_000_000.0,
        oi_change_pct=0.5,
        funding_rate_bps=0.0,
        funding_change_bps=0.1,
    )


def score_opportunity(edge_bid_b: float = 101.0, *, asset: str = "ETH", venue_a: str = "bybit", venue_b: str = "lighter"):
    opp = PerpArbOpportunity(
        asset=asset,
        quote="USD",
        leg_a=make_leg(venue_a, 99.9, 100.0),
        leg_b=make_leg(venue_b, edge_bid_b, edge_bid_b + 0.1),
        notional_usd=50_000.0,
        capital_used_usd=12_500.0,
        slippage_bps=1.0,
        impact_cost_10k_bps=1.0,
        impact_cost_50k_bps=3.0,
        realized_vol=12.0,
        jump_frequency=1.0,
        micro_jump_frequency=1.0,
        shock_jump_frequency=1.0,
        spread_zscore=3.0,
        spread_mean_bps=5.0,
        trend_vs_mean_reversion=-0.5,
        latency_ms=100.0,
        staleness_ms=100.0,
        holding_window_hours=1.0,
        funding_interval_hours=8.0,
        funding_persistence_score=0.7,
        spread_widening_speed_bps_per_min=0.2,
        bucket_hint=OpportunityBucket.DISLOCATION,
        now=datetime.utcnow(),
    )
    return ArbitrageScorer().score(opp)


class PracticeTrackerTest(unittest.TestCase):
    def test_requires_consecutive_scans_before_opening_paper_trade(self) -> None:
        tracker = PracticeTracker(PracticeGateConfig(required_consecutive_scans=3))
        scored = score_opportunity()

        first = tracker.update([scored], now=100.0)
        second = tracker.update([scored], now=103.0)
        third = tracker.update([scored], now=106.0)

        self.assertEqual(first["summary"]["active_count"], 0)
        self.assertEqual(second["summary"]["active_count"], 0)
        self.assertEqual(third["summary"]["active_count"], 1)
        self.assertTrue(third["candidates"][0]["ready"])

    def test_gate_rejects_non_whitelisted_pairs(self) -> None:
        tracker = PracticeTracker()
        scored = score_opportunity(asset="PAXG", venue_a="grvt", venue_b="lighter")

        payload = tracker.update([scored], now=100.0)

        candidate = payload["candidates"][0]
        self.assertFalse(candidate["gate_passed"])
        self.assertIn("asset_not_whitelisted", candidate["reasons"])
        self.assertIn("venue_pair_not_whitelisted", candidate["reasons"])

    def test_active_paper_trade_closes_on_target_reversion(self) -> None:
        tracker = PracticeTracker(PracticeGateConfig(required_consecutive_scans=1))
        entry = score_opportunity(edge_bid_b=101.0)
        opened = tracker.update([entry], now=100.0)
        self.assertEqual(opened["summary"]["active_count"], 1)

        reverted = score_opportunity(edge_bid_b=100.35)
        closed = tracker.update([reverted], now=130.0)

        self.assertEqual(closed["summary"]["active_count"], 0)
        self.assertEqual(closed["summary"]["completed_count"], 1)
        self.assertEqual(closed["recent_completed"][0]["close_reason"], "target_reversion")


if __name__ == "__main__":
    unittest.main()
