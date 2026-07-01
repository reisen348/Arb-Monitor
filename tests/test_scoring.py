from __future__ import annotations

import unittest
from datetime import datetime, timedelta

from perp_arb.models import ExecutionLabel, OpportunityBucket, PerpArbOpportunity, PerpLegSnapshot
from perp_arb.scoring import ArbitrageScorer


def make_leg(
    venue: str,
    best_bid: float,
    best_ask: float,
    market_type: str = "perp_dex",
    mark_price: float | None = None,
    oracle_price: float | None = None,
    index_price: float | None = None,
    taker_fee_bps: float = 2.0,
    depth_10k_usd: float = 100_000.0,
    depth_50k_usd: float = 300_000.0,
    top_1pct_depth_usd: float = 350_000.0,
    volume_depth_ratio: float = 6.0,
    oi_change_pct: float = 1.0,
    funding_rate_bps: float = 0.0,
    funding_change_bps: float = 0.5,
) -> PerpLegSnapshot:
    mark = mark_price if mark_price is not None else (best_bid + best_ask) / 2.0
    oracle = oracle_price if oracle_price is not None else mark
    index = index_price if index_price is not None else mark
    return PerpLegSnapshot(
        venue=venue,
        market_type=market_type,
        asset="ETH",
        best_bid=best_bid,
        best_ask=best_ask,
        mark_price=mark,
        oracle_price=oracle,
        index_price=index,
        taker_fee_bps=taker_fee_bps,
        maker_fee_bps=0.0,
        depth_10k_usd=depth_10k_usd,
        depth_50k_usd=depth_50k_usd,
        top_1pct_depth_usd=top_1pct_depth_usd,
        volume_depth_ratio=volume_depth_ratio,
        oi_usd=10_000_000.0,
        oi_change_pct=oi_change_pct,
        funding_rate_bps=funding_rate_bps,
        funding_change_bps=funding_change_bps,
        next_funding_time=datetime.utcnow() + timedelta(hours=4),
    )


def make_opportunity(**overrides) -> PerpArbOpportunity:
    leg_a = overrides.pop("leg_a", make_leg("dex_a", 101.30, 101.40, funding_rate_bps=2.0))
    leg_b = overrides.pop("leg_b", make_leg("dex_b", 101.90, 102.00, funding_rate_bps=-1.0))
    params = {
        "asset": "ETH",
        "quote": "USD",
        "leg_a": leg_a,
        "leg_b": leg_b,
        "notional_usd": 50_000.0,
        "capital_used_usd": 12_500.0,
        "slippage_bps": 1.0,
        "impact_cost_10k_bps": 1.5,
        "impact_cost_50k_bps": 4.0,
        "realized_vol": 18.0,
        "jump_frequency": 2.0,
        "spread_zscore": 1.0,
        "trend_vs_mean_reversion": -0.4,
        "latency_ms": 120.0,
        "staleness_ms": 150.0,
        "holding_window_hours": 1.0,
        "funding_interval_hours": 8.0,
        "funding_persistence_score": 0.7,
        "spread_widening_speed_bps_per_min": 0.5,
        "bucket_hint": OpportunityBucket.DISLOCATION,
        "now": datetime.utcnow(),
    }
    params.update(overrides)
    return PerpArbOpportunity(**params)


class ArbitrageScorerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.scorer = ArbitrageScorer()

    def test_positive_spread_can_become_blocked_after_costs(self) -> None:
        leg_a = make_leg("dex_a", 101.30, 101.40, funding_rate_bps=2.0)
        leg_b = make_leg("dex_b", 101.58, 101.68, funding_rate_bps=-1.0)
        opportunity = make_opportunity(
            leg_a=leg_a,
            leg_b=leg_b,
            slippage_bps=8.0,
            impact_cost_10k_bps=9.0,
            impact_cost_50k_bps=24.0,
        )
        scored = self.scorer.score(opportunity)
        self.assertEqual(scored.label, ExecutionLabel.BLOCKED)
        self.assertIn("expected_profit_non_positive", scored.block_reasons)

    def test_carry_bucket_degrades_when_exit_cost_exceeds_funding_edge(self) -> None:
        leg_a = make_leg("dex_a", 100.0, 100.1, funding_rate_bps=14.0)
        leg_b = make_leg("dex_b", 100.3, 100.4, funding_rate_bps=-10.0)
        opportunity = make_opportunity(
            leg_a=leg_a,
            leg_b=leg_b,
            holding_window_hours=8.0,
            funding_persistence_score=0.9,
            impact_cost_10k_bps=8.0,
            impact_cost_50k_bps=22.0,
            slippage_bps=6.0,
            bucket_hint=OpportunityBucket.CARRY,
        )
        scored = self.scorer.score(opportunity)
        self.assertEqual(scored.breakdown.bucket_type, OpportunityBucket.CARRY)
        self.assertLess(scored.breakdown.bucket_score, 65.0)
        self.assertNotEqual(scored.label, ExecutionLabel.TRADABLE)

    def test_thin_top_depth_hurts_execution_score(self) -> None:
        thin_leg = make_leg("dex_a", 101.3, 101.4, top_1pct_depth_usd=18_000.0, depth_50k_usd=60_000.0)
        opportunity = make_opportunity(leg_a=thin_leg)
        scored = self.scorer.score(opportunity)
        self.assertLess(scored.breakdown.execution_score, 50.0)

    def test_oi_and_funding_spike_raise_strong_penalty(self) -> None:
        leg_a = make_leg("dex_a", 101.3, 101.4, oi_change_pct=15.0, funding_change_bps=10.0)
        leg_b = make_leg("dex_b", 101.9, 102.0, oi_change_pct=14.0, funding_change_bps=9.0)
        opportunity = make_opportunity(leg_a=leg_a, leg_b=leg_b)
        scored = self.scorer.score(opportunity)
        self.assertTrue(scored.risk_flags.oi_spike)
        self.assertTrue(scored.risk_flags.funding_spike)
        self.assertIn("oi_and_funding_spike", scored.block_reasons)

    def test_oracle_divergence_blocks_even_with_good_spread(self) -> None:
        leg_a = make_leg("dex_a", 101.3, 101.4, mark_price=101.35, oracle_price=100.80, index_price=101.30)
        opportunity = make_opportunity(leg_a=leg_a)
        scored = self.scorer.score(opportunity)
        self.assertEqual(scored.label, ExecutionLabel.BLOCKED)
        self.assertIn("oracle_offset_too_high", scored.block_reasons)

    def test_impact_cost_changes_ranking_by_notional(self) -> None:
        richer = make_opportunity(
            leg_a=make_leg("dex_a", 101.5, 101.55),
            leg_b=make_leg("dex_b", 102.05, 102.10),
            impact_cost_10k_bps=1.0,
            impact_cost_50k_bps=3.0,
            capital_used_usd=12_000.0,
        )
        worse = make_opportunity(
            leg_a=make_leg("dex_a", 101.5, 101.55),
            leg_b=make_leg("dex_b", 102.05, 102.10),
            impact_cost_10k_bps=5.0,
            impact_cost_50k_bps=18.0,
            capital_used_usd=12_000.0,
        )
        ranked = self.scorer.rank([worse, richer])
        self.assertIs(ranked[0].opportunity, richer)

    def test_roi_per_capital_breaks_ties(self) -> None:
        high_roi = make_opportunity(capital_used_usd=10_000.0)
        low_roi = make_opportunity(capital_used_usd=20_000.0)
        ranked = self.scorer.rank([low_roi, high_roi])
        self.assertIs(ranked[0].opportunity, high_roi)

    def test_dislocation_high_score_is_tradable(self) -> None:
        scored = self.scorer.score(make_opportunity())
        self.assertEqual(scored.breakdown.bucket_type, OpportunityBucket.DISLOCATION)
        self.assertEqual(scored.label, ExecutionLabel.TRADABLE)
        self.assertGreaterEqual(scored.breakdown.composite_score, 70.0)

    def test_soft_staleness_downgrades_to_watch_without_blocking(self) -> None:
        scored = self.scorer.score(make_opportunity(staleness_ms=6_000.0))

        self.assertEqual(scored.label, ExecutionLabel.WATCH)
        self.assertTrue(scored.risk_flags.stale_data)
        self.assertIn("freshness_risk", scored.tags)
        self.assertIn("freshness_watch", scored.advisories)
        self.assertIn("tradable_downgraded_by_freshness", scored.advisories)
        self.assertNotIn("staleness_too_high", scored.block_reasons)
        self.assertEqual(scored.execution_plan.action, "observe")

    def test_hard_staleness_still_blocks_dislocation(self) -> None:
        scored = self.scorer.score(make_opportunity(staleness_ms=16_000.0))

        self.assertEqual(scored.label, ExecutionLabel.BLOCKED)
        self.assertIn("staleness_too_high", scored.block_reasons)

    def test_soft_latency_downgrades_without_hard_block(self) -> None:
        scored = self.scorer.score(make_opportunity(latency_ms=2_000.0))

        self.assertTrue(scored.risk_flags.stale_data)
        self.assertIn("freshness_risk", scored.tags)
        self.assertNotIn("latency_too_high", scored.block_reasons)

    def test_carry_high_score_is_stable_after_funding_window(self) -> None:
        now = datetime.utcnow()
        leg_a = make_leg(
            "dex_a",
            100.0,
            100.1,
            funding_rate_bps=-15.0,
            funding_change_bps=1.0,
        )
        leg_b = make_leg(
            "dex_b",
            100.2,
            100.3,
            funding_rate_bps=12.0,
            funding_change_bps=1.0,
        )
        leg_a = PerpLegSnapshot(**{**leg_a.__dict__, "next_funding_time": now + timedelta(hours=3)})
        leg_b = PerpLegSnapshot(**{**leg_b.__dict__, "next_funding_time": now + timedelta(hours=3)})
        opportunity = make_opportunity(
            leg_a=leg_a,
            leg_b=leg_b,
            bucket_hint=OpportunityBucket.CARRY,
            holding_window_hours=8.0,
            funding_persistence_score=0.95,
            trend_vs_mean_reversion=0.3,
            impact_cost_10k_bps=1.0,
            impact_cost_50k_bps=4.0,
            slippage_bps=1.2,
            now=now,
        )
        scored = self.scorer.score(opportunity)
        self.assertEqual(scored.breakdown.bucket_type, OpportunityBucket.CARRY)
        self.assertGreater(scored.breakdown.carry_edge_bps, 20.0)
        self.assertEqual(scored.label, ExecutionLabel.TRADABLE)

    def test_micro_and_shock_jump_penalize_fast_and_slow_layers_separately(self) -> None:
        micro_risky = self.scorer.score(
            make_opportunity(micro_jump_frequency=30.0, shock_jump_frequency=0.0, jump_frequency=30.0)
        )
        shock_risky = self.scorer.score(
            make_opportunity(micro_jump_frequency=0.0, shock_jump_frequency=30.0, jump_frequency=30.0)
        )
        baseline = self.scorer.score(make_opportunity(micro_jump_frequency=0.0, shock_jump_frequency=0.0, jump_frequency=0.0))

        self.assertLess(micro_risky.breakdown.fast_score, baseline.breakdown.fast_score)
        self.assertLess(micro_risky.breakdown.execution_score, baseline.breakdown.execution_score)
        self.assertLess(shock_risky.breakdown.slow_score, baseline.breakdown.slow_score)
        self.assertLess(shock_risky.breakdown.risk_integrity_score, baseline.breakdown.risk_integrity_score)

    def test_micro_jump_downgrades_tradable_to_watch(self) -> None:
        scored = self.scorer.score(
            make_opportunity(micro_jump_frequency=30.0, shock_jump_frequency=0.0, jump_frequency=30.0)
        )
        self.assertEqual(scored.label, ExecutionLabel.WATCH)
        self.assertIn("micro_execution_risk", scored.tags)
        self.assertIn("tradable_downgraded_by_micro_jump", scored.advisories)
        self.assertFalse(scored.policy.allow_taker)
        self.assertTrue(scored.policy.allow_maker)
        self.assertIn("maker_only", scored.policy.notes)
        self.assertEqual(scored.execution_plan.action, "observe")
        self.assertEqual(scored.execution_plan.execution_style, "maker_only")
        self.assertEqual(scored.execution_plan.target_notional_usd, 0.0)
        self.assertIn("dislocation", scored.execution_plan.allowed_strategies)
        self.assertEqual(len(scored.execution_plan.legs), 2)
        self.assertTrue(all(leg.order_type == "limit" for leg in scored.execution_plan.legs))
        self.assertTrue(all(leg.post_only for leg in scored.execution_plan.legs))
        self.assertTrue(all(not leg.enabled for leg in scored.execution_plan.legs))

    def test_shock_jump_adds_regime_and_liquidation_tags(self) -> None:
        scored = self.scorer.score(
            make_opportunity(
                micro_jump_frequency=0.0,
                shock_jump_frequency=30.0,
                jump_frequency=30.0,
                bucket_hint=OpportunityBucket.CARRY,
            )
        )
        self.assertEqual(scored.label, ExecutionLabel.BLOCKED)
        self.assertIn("regime_unstable", scored.tags)
        self.assertIn("liquidation_risk", scored.tags)
        self.assertIn("shock_jump_liquidation_risk", scored.advisories)
        self.assertIn("carry_blocked_by_liquidation_risk", scored.block_reasons)
        self.assertFalse(scored.policy.allow_carry)
        self.assertLess(scored.policy.size_multiplier, 1.0)
        self.assertEqual(scored.execution_plan.action, "skip")
        self.assertEqual(scored.execution_plan.execution_style, "disabled")
        self.assertNotIn("carry", scored.execution_plan.allowed_strategies)
        self.assertIn("blocked:carry_blocked_by_liquidation_risk", scored.execution_plan.notes)
        self.assertEqual(len(scored.execution_plan.legs), 2)
        self.assertTrue(all(leg.order_type == "none" for leg in scored.execution_plan.legs))
        self.assertTrue(all(not leg.enabled for leg in scored.execution_plan.legs))

    def test_dex_jump_threshold_is_more_relaxed_than_cex(self) -> None:
        dex_scored = self.scorer.score(
            make_opportunity(
                micro_jump_frequency=20.0,
                shock_jump_frequency=20.0,
                jump_frequency=20.0,
            )
        )
        cex_scored = self.scorer.score(
            make_opportunity(
                leg_a=make_leg("cex_a", 101.3, 101.4, market_type="perp_cex"),
                leg_b=make_leg("cex_b", 101.9, 102.0, market_type="perp_cex"),
                micro_jump_frequency=20.0,
                shock_jump_frequency=20.0,
                jump_frequency=20.0,
            )
        )
        self.assertNotIn("liquidation_risk", dex_scored.tags)
        self.assertIn("liquidation_risk", cex_scored.tags)
        self.assertGreater(dex_scored.breakdown.fast_score, cex_scored.breakdown.fast_score)

    def test_dislocation_widening_speed_penalty_is_more_material(self) -> None:
        baseline = self.scorer.score(make_opportunity(spread_widening_speed_bps_per_min=1.0))
        widening = self.scorer.score(make_opportunity(spread_widening_speed_bps_per_min=12.0))
        self.assertLess(widening.breakdown.fast_score, baseline.breakdown.fast_score - 8.0)

    def test_carry_squeeze_now_reduces_size_and_downgrades_instead_of_blocking(self) -> None:
        scored = self.scorer.score(
            make_opportunity(
                bucket_hint=OpportunityBucket.CARRY,
                holding_window_hours=8.0,
                funding_persistence_score=0.9,
                leg_a=make_leg("dex_a", 100.0, 100.1, funding_rate_bps=-22.0, oi_change_pct=14.0),
                leg_b=make_leg("dex_b", 100.3, 100.4, funding_rate_bps=18.0, oi_change_pct=13.0),
                realized_vol=86.0,
            )
        )
        self.assertEqual(scored.label, ExecutionLabel.WATCH)
        self.assertIn("carry_squeeze_risk", scored.tags)
        self.assertIn("carry_squeeze_reduce_only", scored.advisories)
        self.assertNotIn("carry_squeeze_risk", scored.block_reasons)
        self.assertAlmostEqual(scored.policy.size_multiplier, 0.5)
        self.assertIn("carry_squeeze_reduce_size", scored.policy.notes)
        self.assertTrue(scored.policy.allow_carry)

    def test_tradable_execution_plan_contains_ioc_dual_leg_draft(self) -> None:
        scored = self.scorer.score(make_opportunity())
        self.assertEqual(scored.execution_plan.action, "execute")
        self.assertEqual(scored.execution_plan.execution_style, "taker_or_maker")
        self.assertEqual(len(scored.execution_plan.legs), 2)
        self.assertTrue(all(leg.order_type == "marketable_limit" for leg in scored.execution_plan.legs))
        self.assertTrue(all(leg.time_in_force == "ioc" for leg in scored.execution_plan.legs))
        self.assertTrue(all(leg.enabled for leg in scored.execution_plan.legs))
        self.assertEqual(scored.execution_plan.legs[0].side, "buy")
        self.assertEqual(scored.execution_plan.legs[1].side, "sell")


if __name__ == "__main__":
    unittest.main()
