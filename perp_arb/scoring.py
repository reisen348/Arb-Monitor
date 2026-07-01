from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Sequence, Tuple

from .models import (
    ExecutionLegPlan,
    ExecutionPlan,
    ExecutionPolicy,
    ExecutionLabel,
    OpportunityBucket,
    PerpArbOpportunity,
    PerpLegSnapshot,
    RiskFlags,
    ScoreBreakdown,
    ScoredOpportunity,
)


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def linear_score(value: float, good: float, bad: float, inverse: bool = False) -> float:
    if inverse:
        if value <= good:
            return 100.0
        if value >= bad:
            return 0.0
        return (bad - value) / (bad - good) * 100.0
    if value <= bad:
        return 0.0
    if value >= good:
        return 100.0
    return (value - bad) / (good - bad) * 100.0


@dataclass(frozen=True)
class ScoringConfig:
    min_depth_10k_usd: float = 10_000.0
    min_depth_50k_usd: float = 50_000.0
    max_impact_cost_50k_bps: float = 25.0
    max_slippage_bps: float = 12.0
    # Soft freshness thresholds reduce score and raise risk flags. Hard
    # thresholds below are used for outright blocking.
    max_staleness_ms: float = 5_000.0
    max_latency_ms: float = 1_500.0
    hard_staleness_dislocation_ms: float = 15_000.0
    hard_staleness_carry_ms: float = 60_000.0
    hard_latency_ms: float = 5_000.0
    max_oracle_offset_bps: float = 35.0
    max_mark_index_divergence_bps: float = 35.0
    oi_spike_threshold_pct: float = 12.0
    funding_spike_threshold_bps: float = 8.0
    min_exit_liquidity_score: float = 40.0
    jump_risk_threshold: float = 18.0
    dex_jump_risk_threshold: float = 25.0
    tradable_score_threshold: float = 70.0
    watch_score_threshold: float = 50.0
    min_confidence_for_tradable: float = 55.0
    safety_margin_ratio: float = 0.12
    carry_persistence_floor: float = 0.35
    carry_direction_uncertainty_hours: float = 0.5
    high_risk_realized_vol: float = 85.0
    # Structural spread detection: block dislocation when spread z-score is
    # too low relative to the absolute spread — means the "big" spread is
    # actually the normal state and won't revert.
    structural_spread_min_bps: float = 20.0       # only check when entry_edge > this
    structural_spread_max_zscore: float = 0.8      # z-score below this = structural
    high_risk_funding_bps: float = 20.0
    carry_squeeze_signal_count_for_action: int = 2
    carry_squeeze_size_multiplier: float = 0.5
    notional_reference_low: float = 10_000.0
    notional_reference_high: float = 50_000.0
    micro_jump_watch_ratio: float = 0.6
    shock_jump_watch_ratio: float = 0.6
    # DEX-relaxed depth / impact thresholds
    dex_min_depth_10k_usd: float = 3_000.0
    dex_min_depth_50k_usd: float = 8_000.0
    dex_max_impact_cost_50k_bps: float = 50.0
    # Bucket-specific expected_profit edge weights
    dislocation_entry_weight: float = 1.0
    dislocation_carry_weight: float = 0.15
    carry_entry_weight: float = 0.25
    carry_carry_weight: float = 1.0
    # Bucket-specific thresholds
    carry_funding_diff_good_bps: float = 3.0
    dislocation_zscore_good: float = 3.0
    # Multi-period carry: extend holding when funding is stable
    carry_max_periods: int = 3
    carry_extend_persistence_threshold: float = 0.55


def _has_dex_leg(leg_a: PerpLegSnapshot, leg_b: PerpLegSnapshot) -> bool:
    """Return True if either leg is from a DEX venue."""
    return "dex" in leg_a.market_type or "dex" in leg_b.market_type


class ArbitrageScorer:
    def __init__(self, config: ScoringConfig | None = None) -> None:
        self.config = config or ScoringConfig()

    def score(self, opportunity: PerpArbOpportunity) -> ScoredOpportunity:
        direction, long_leg, short_leg, entry_edge_bps = self._choose_direction(opportunity)
        bucket = self._bucket_for(opportunity, entry_edge_bps, long_leg, short_leg)
        carry_edge_bps = self._carry_edge_bps(opportunity, long_leg, short_leg)
        fee_bps = self._effective_fee_bps(long_leg, short_leg, bucket)
        impact_buffer_bps = self._impact_buffer_bps(opportunity)
        exit_liquidity_score = self._exit_liquidity_score(opportunity)
        exit_cost_buffer_bps = self._exit_cost_buffer_bps(opportunity, exit_liquidity_score)
        oracle_offset_bps = max(long_leg.oracle_offset_bps, short_leg.oracle_offset_bps)
        mark_index_divergence_bps = max(
            long_leg.mark_index_divergence_bps,
            short_leg.mark_index_divergence_bps,
        )
        oracle_risk_buffer_bps = self._oracle_risk_buffer_bps(
            oracle_offset_bps,
            mark_index_divergence_bps,
        )
        # Estimate the revertable portion of entry_edge.
        # Only the spread *above* the historical mean can revert, and only when
        # the z-score indicates a meaningful deviation (zscore >= 1).  When
        # zscore < 1 the current spread is within normal noise — no reliable
        # reversion edge exists, so revertable_edge → 0.
        zscore = abs(opportunity.spread_zscore)
        if opportunity.spread_mean_bps > 0:
            excess_bps = max(0.0, entry_edge_bps - opportunity.spread_mean_bps)
            # Scale by how far outside normal range we are.
            # zscore < 1 → inside 1σ → reversion_frac = 0
            # zscore = 2 → reversion_frac = 0.5  (expect half the excess to revert)
            # zscore = 3 → reversion_frac = 0.67
            # zscore ≥ 4 → cap at 0.75  (never assume full reversion)
            reversion_frac = min(max(0.0, 1.0 - 1.0 / max(zscore, 1e-9)), 0.75) if zscore >= 1.0 else 0.0
            revertable_edge_bps = excess_bps * reversion_frac
        else:
            # No history yet — use full entry_edge; structural spread
            # detection will kick in once enough data points accumulate.
            revertable_edge_bps = entry_edge_bps

        if bucket == OpportunityBucket.DISLOCATION:
            weighted_edge = (
                revertable_edge_bps * self.config.dislocation_entry_weight
                + carry_edge_bps * self.config.dislocation_carry_weight
            )
        else:
            weighted_edge = (
                revertable_edge_bps * self.config.carry_entry_weight
                + carry_edge_bps * self.config.carry_carry_weight
            )
        expected_profit_bps = (
            weighted_edge
            - fee_bps
            - opportunity.slippage_bps
            - impact_buffer_bps
            - exit_cost_buffer_bps
            - oracle_risk_buffer_bps
        )
        expected_profit_usd = opportunity.notional_usd * expected_profit_bps / 10_000.0
        roi_per_capital = safe_div(expected_profit_usd, opportunity.capital_used_usd)
        risk_flags = self._risk_flags(
            opportunity,
            oracle_offset_bps,
            mark_index_divergence_bps,
            exit_liquidity_score,
        )

        net_edge_score = self._net_edge_score(
            opportunity,
            bucket,
            entry_edge_bps,
            carry_edge_bps,
            expected_profit_bps,
            roi_per_capital,
        )
        execution_score = self._execution_score(opportunity, exit_liquidity_score)
        risk_integrity_score = self._risk_integrity_score(
            opportunity,
            bucket,
            oracle_offset_bps,
            mark_index_divergence_bps,
        )
        slow_score = self._slow_score(opportunity, bucket)
        fast_score = self._fast_score(
            opportunity,
            bucket,
            entry_edge_bps,
            oracle_offset_bps,
            mark_index_divergence_bps,
            long_leg,
            short_leg,
        )
        bucket_score = self._bucket_score(
            bucket,
            net_edge_score,
            execution_score,
            risk_integrity_score,
        )
        composite_score = self._composite_score(bucket, bucket_score, slow_score, fast_score)
        confidence = self._confidence(opportunity, exit_liquidity_score, risk_integrity_score)
        block_reasons = self._block_reasons(
            opportunity,
            bucket,
            expected_profit_bps,
            entry_edge_bps,
            oracle_offset_bps,
            mark_index_divergence_bps,
            exit_liquidity_score,
            risk_flags,
            long_leg,
            short_leg,
        )
        label = self._label(block_reasons, composite_score, confidence)
        tags, advisories, label = self._apply_freshness_actions(opportunity, risk_flags, label)
        tags, advisories, label = self._apply_jump_actions(opportunity, label, tags, advisories)
        tags, advisories, label = self._apply_carry_squeeze_actions(
            opportunity,
            bucket,
            tags,
            advisories,
            label,
        )
        policy, label, block_reasons, advisories = self._derive_execution_policy(
            opportunity,
            bucket,
            label,
            block_reasons,
            tags,
            advisories,
        )
        execution_plan = self._derive_execution_plan(
            opportunity,
            bucket,
            direction,
            label,
            block_reasons,
            policy,
        )

        breakdown = ScoreBreakdown(
            bucket_type=bucket,
            direction=direction,
            net_edge_score=round(net_edge_score, 2),
            execution_score=round(execution_score, 2),
            risk_integrity_score=round(risk_integrity_score, 2),
            slow_score=round(slow_score, 2),
            fast_score=round(fast_score, 2),
            bucket_score=round(bucket_score, 2),
            composite_score=round(composite_score, 2),
            expected_profit_bps=round(expected_profit_bps, 4),
            expected_profit_usd=round(expected_profit_usd, 4),
            roi_per_capital=round(roi_per_capital, 6),
            confidence=round(confidence, 2),
            entry_edge_bps=round(entry_edge_bps, 4),
            carry_edge_bps=round(carry_edge_bps, 4),
            fee_bps=round(fee_bps, 4),
            impact_buffer_bps=round(impact_buffer_bps, 4),
            exit_cost_buffer_bps=round(exit_cost_buffer_bps, 4),
            oracle_risk_buffer_bps=round(oracle_risk_buffer_bps, 4),
            exit_liquidity_score=round(exit_liquidity_score, 2),
            oracle_offset_bps=round(oracle_offset_bps, 4),
            mark_index_divergence_bps=round(mark_index_divergence_bps, 4),
        )
        return ScoredOpportunity(
            opportunity=opportunity,
            breakdown=breakdown,
            risk_flags=risk_flags,
            label=label,
            block_reasons=block_reasons,
            tags=tags,
            advisories=advisories,
            policy=policy,
            execution_plan=execution_plan,
        )

    def rank(self, opportunities: Sequence[PerpArbOpportunity]) -> List[ScoredOpportunity]:
        scored = [self.score(opportunity) for opportunity in opportunities]
        return sorted(
            scored,
            key=lambda item: (
                item.label != ExecutionLabel.TRADABLE,
                item.breakdown.expected_profit_bps <= 0.0,
                -item.breakdown.composite_score,
                -item.breakdown.roi_per_capital,
            ),
        )

    def _choose_direction(
        self,
        opportunity: PerpArbOpportunity,
    ) -> Tuple[str, PerpLegSnapshot, PerpLegSnapshot, float]:
        a_long_b_short = (
            (opportunity.leg_b.best_bid - opportunity.leg_a.best_ask)
            / max(opportunity.leg_a.mid_price, 1e-9)
            * 10_000
        )
        b_long_a_short = (
            (opportunity.leg_a.best_bid - opportunity.leg_b.best_ask)
            / max(opportunity.leg_b.mid_price, 1e-9)
            * 10_000
        )
        if a_long_b_short >= b_long_a_short:
            return "long_a_short_b", opportunity.leg_a, opportunity.leg_b, a_long_b_short
        return "long_b_short_a", opportunity.leg_b, opportunity.leg_a, b_long_a_short

    def _bucket_for(
        self,
        opportunity: PerpArbOpportunity,
        entry_edge_bps: float,
        long_leg: PerpLegSnapshot,
        short_leg: PerpLegSnapshot,
    ) -> OpportunityBucket:
        if opportunity.bucket_hint is not None:
            return opportunity.bucket_hint
        carry_edge = self._carry_edge_bps(opportunity, long_leg, short_leg)
        if carry_edge > abs(entry_edge_bps) * 0.6 and opportunity.holding_window_hours >= 4:
            return OpportunityBucket.CARRY
        return OpportunityBucket.DISLOCATION

    def _carry_edge_bps(
        self,
        opportunity: PerpArbOpportunity,
        long_leg: PerpLegSnapshot,
        short_leg: PerpLegSnapshot,
    ) -> float:
        holding_hours = max(opportunity.holding_window_hours, 0.0)
        persistence = clamp(opportunity.funding_persistence_score, 0.0, 1.0)

        # When funding is stable enough, extend holding to multiple base periods.
        base_period_hours = max(opportunity.funding_interval_hours, 1e-9)
        base_windows = holding_hours / base_period_hours
        if persistence >= self.config.carry_extend_persistence_threshold:
            extend_ratio = (persistence - self.config.carry_extend_persistence_threshold) / (
                1.0 - self.config.carry_extend_persistence_threshold
            )
            extra_periods = extend_ratio * (self.config.carry_max_periods - 1)
            effective_hours = holding_hours * (1.0 + extra_periods)
        else:
            effective_hours = holding_hours

        # Per-leg funding: each leg settles at its own interval.
        # Compute net funding earned during effective_hours.
        long_interval = max(long_leg.funding_interval_hours, 1e-9)
        short_interval = max(short_leg.funding_interval_hours, 1e-9)
        long_settlements = effective_hours / long_interval
        short_settlements = effective_hours / short_interval

        # Long pays funding, short receives funding.
        # Net = -long_funding * long_settlements + short_funding * short_settlements
        raw_funding_edge = (
            -long_leg.funding_rate_bps * long_settlements
            + short_leg.funding_rate_bps * short_settlements
        )
        return raw_funding_edge * persistence

    def _impact_buffer_bps(self, opportunity: PerpArbOpportunity) -> float:
        low_weight = clamp(
            1.0 - safe_div(opportunity.notional_usd - self.config.notional_reference_low,
                           self.config.notional_reference_high - self.config.notional_reference_low),
            0.0,
            1.0,
        )
        high_weight = 1.0 - low_weight
        return (
            opportunity.impact_cost_10k_bps * low_weight
            + opportunity.impact_cost_50k_bps * high_weight
        )

    def _exit_liquidity_score(self, opportunity: PerpArbOpportunity) -> float:
        depth_symmetry = safe_div(
            min(opportunity.leg_a.depth_50k_usd, opportunity.leg_b.depth_50k_usd),
            max(opportunity.leg_a.depth_50k_usd, opportunity.leg_b.depth_50k_usd, 1e-9),
        )
        top_depth = min(opportunity.leg_a.top_1pct_depth_usd, opportunity.leg_b.top_1pct_depth_usd)
        top_depth_score = linear_score(top_depth, 200_000.0, 20_000.0)
        return clamp(depth_symmetry * 50.0 + top_depth_score * 0.5, 0.0, 100.0)

    def _effective_fee_bps(
        self,
        long_leg: PerpLegSnapshot,
        short_leg: PerpLegSnapshot,
        bucket: OpportunityBucket,
    ) -> float:
        """Use maker fee for the passive leg when conditions favour maker execution.

        Carry trades hold long enough to place both legs as maker orders.
        Dislocation trades use maker for the primary leg (the one with better
        depth) and taker for the hedge leg.
        """
        if bucket == OpportunityBucket.CARRY:
            return long_leg.maker_fee_bps + short_leg.maker_fee_bps
        # Dislocation: primary leg as maker, hedge leg as taker
        # The leg with deeper book is more likely to fill as maker.
        if long_leg.depth_10k_usd >= short_leg.depth_10k_usd:
            return long_leg.maker_fee_bps + short_leg.taker_fee_bps
        return long_leg.taker_fee_bps + short_leg.maker_fee_bps

    def _exit_cost_buffer_bps(self, opportunity: PerpArbOpportunity, exit_liquidity_score: float) -> float:
        liquidity_penalty = (100.0 - exit_liquidity_score) / 100.0 * 8.0
        widening_penalty = clamp(opportunity.spread_widening_speed_bps_per_min, 0.0, 15.0) * 0.3
        safety_margin = opportunity.slippage_bps * self.config.safety_margin_ratio
        return liquidity_penalty + widening_penalty + safety_margin

    def _oracle_risk_buffer_bps(self, oracle_offset_bps: float, mark_index_divergence_bps: float) -> float:
        return oracle_offset_bps * 0.12 + mark_index_divergence_bps * 0.08

    def _risk_flags(
        self,
        opportunity: PerpArbOpportunity,
        oracle_offset_bps: float,
        mark_index_divergence_bps: float,
        exit_liquidity_score: float,
    ) -> RiskFlags:
        max_oi_change = max(abs(opportunity.leg_a.oi_change_pct), abs(opportunity.leg_b.oi_change_pct))
        max_funding_change = max(
            abs(opportunity.leg_a.funding_change_bps),
            abs(opportunity.leg_b.funding_change_bps),
        )
        min_depth = min(opportunity.leg_a.depth_10k_usd, opportunity.leg_b.depth_10k_usd)
        is_dex = _has_dex_leg(opportunity.leg_a, opportunity.leg_b)
        depth_10k_threshold = self.config.dex_min_depth_10k_usd if is_dex else self.config.min_depth_10k_usd
        jump_threshold = self._jump_risk_threshold(opportunity)
        stale_data = (
            opportunity.staleness_ms > self.config.max_staleness_ms
            or opportunity.latency_ms > self.config.max_latency_ms
        )
        return RiskFlags(
            oracle_deviation=oracle_offset_bps > self.config.max_oracle_offset_bps,
            mark_index_divergence=mark_index_divergence_bps > self.config.max_mark_index_divergence_bps,
            funding_spike=max_funding_change >= self.config.funding_spike_threshold_bps,
            oi_spike=max_oi_change >= self.config.oi_spike_threshold_pct,
            thin_book=min_depth < depth_10k_threshold,
            stale_data=stale_data,
            jump_risk=max(opportunity.micro_jump_frequency, opportunity.shock_jump_frequency) >= jump_threshold,
            exit_risk=exit_liquidity_score < self.config.min_exit_liquidity_score,
        )

    def _net_edge_score(
        self,
        opportunity: PerpArbOpportunity,
        bucket: OpportunityBucket,
        entry_edge_bps: float,
        carry_edge_bps: float,
        expected_profit_bps: float,
        roi_per_capital: float,
    ) -> float:
        profit_score = linear_score(expected_profit_bps, 30.0, 0.0)
        roi_score = linear_score(roi_per_capital * 10_000, 120.0, 0.0)
        if bucket == OpportunityBucket.DISLOCATION:
            spread_score = linear_score(entry_edge_bps, 40.0, -5.0)
            # Funding is bonus-only for dislocation: positive carry helps, negative carry ignored
            funding_bonus = linear_score(max(carry_edge_bps, 0.0), 10.0, 0.0)
            return spread_score * 0.40 + profit_score * 0.35 + roi_score * 0.20 + funding_bonus * 0.05
        # CARRY
        funding_score = linear_score(carry_edge_bps, self.config.carry_funding_diff_good_bps, -5.0)
        persistence_score = linear_score(opportunity.funding_persistence_score, 1.0, 0.3)
        spread_timing = linear_score(entry_edge_bps, 20.0, -5.0)
        return funding_score * 0.40 + persistence_score * 0.15 + profit_score * 0.25 + roi_score * 0.15 + spread_timing * 0.05

    def _execution_score(self, opportunity: PerpArbOpportunity, exit_liquidity_score: float) -> float:
        jump_threshold = self._jump_risk_threshold(opportunity)
        top_depth = min(opportunity.leg_a.top_1pct_depth_usd, opportunity.leg_b.top_1pct_depth_usd)
        top_depth_score = linear_score(top_depth, 250_000.0, 25_000.0)
        impact_10_score = linear_score(opportunity.impact_cost_10k_bps, 1.0, 12.0, inverse=True)
        impact_50_score = linear_score(opportunity.impact_cost_50k_bps, 4.0, 25.0, inverse=True)
        slippage_score = linear_score(opportunity.slippage_bps, 1.5, self.config.max_slippage_bps, inverse=True)
        micro_jump_score = linear_score(
            opportunity.micro_jump_frequency,
            2.0,
            jump_threshold,
            inverse=True,
        )
        volume_depth_score = linear_score(
            min(opportunity.leg_a.volume_depth_ratio, opportunity.leg_b.volume_depth_ratio),
            8.0,
            0.5,
        )
        staleness_score = linear_score(opportunity.staleness_ms, 500.0, self.config.max_staleness_ms, inverse=True)
        return (
            top_depth_score * 0.38
            + impact_10_score * 0.08
            + impact_50_score * 0.08
            + slippage_score * 0.08
            + micro_jump_score * 0.12
            + volume_depth_score * 0.04
            + staleness_score * 0.07
            + exit_liquidity_score * 0.15
        )

    def _risk_integrity_score(
        self,
        opportunity: PerpArbOpportunity,
        bucket: OpportunityBucket,
        oracle_offset_bps: float,
        mark_index_divergence_bps: float,
    ) -> float:
        jump_threshold = self._jump_risk_threshold(opportunity)
        oi_change = max(abs(opportunity.leg_a.oi_change_pct), abs(opportunity.leg_b.oi_change_pct))
        funding_change = max(
            abs(opportunity.leg_a.funding_change_bps),
            abs(opportunity.leg_b.funding_change_bps),
        )
        oracle_score = linear_score(oracle_offset_bps, 4.0, self.config.max_oracle_offset_bps, inverse=True)
        mark_score = linear_score(
            mark_index_divergence_bps,
            4.0,
            self.config.max_mark_index_divergence_bps,
            inverse=True,
        )
        oi_score = linear_score(oi_change, 3.0, self.config.oi_spike_threshold_pct, inverse=True)
        funding_score = linear_score(
            funding_change,
            2.0,
            self.config.funding_spike_threshold_bps,
            inverse=True,
        )
        vol_score = linear_score(opportunity.realized_vol, 12.0, 90.0, inverse=True)
        shock_jump_score = linear_score(
            opportunity.shock_jump_frequency,
            2.0,
            jump_threshold,
            inverse=True,
        )
        spread_score = linear_score(abs(opportunity.spread_zscore), 0.5, 3.5, inverse=True)
        regime_score = self._trend_regime_score(opportunity, bucket)
        return (
            oracle_score * 0.18
            + mark_score * 0.12
            + oi_score * 0.12
            + funding_score * 0.12
            + vol_score * 0.14
            + shock_jump_score * 0.14
            + spread_score * 0.1
            + regime_score * 0.08
        )

    def _slow_score(self, opportunity: PerpArbOpportunity, bucket: OpportunityBucket) -> float:
        jump_threshold = self._jump_risk_threshold(opportunity)
        vol_regime_score = linear_score(opportunity.realized_vol, 10.0, 80.0, inverse=True)
        trend_regime_score = self._trend_regime_score(opportunity, bucket)
        shock_jump_score = linear_score(
            opportunity.shock_jump_frequency,
            2.0,
            jump_threshold,
            inverse=True,
        )
        oi_anomaly_score = linear_score(
            max(abs(opportunity.leg_a.oi_change_pct), abs(opportunity.leg_b.oi_change_pct)),
            2.0,
            self.config.oi_spike_threshold_pct,
            inverse=True,
        )
        funding_anomaly_score = linear_score(
            max(abs(opportunity.leg_a.funding_change_bps), abs(opportunity.leg_b.funding_change_bps)),
            1.0,
            self.config.funding_spike_threshold_bps,
            inverse=True,
        )
        if bucket == OpportunityBucket.DISLOCATION:
            mean_reversion_score = self._trend_regime_score(opportunity, bucket)
            zscore_persistence = linear_score(abs(opportunity.spread_zscore), self.config.dislocation_zscore_good, 0.5)
            return (
                mean_reversion_score * 0.25
                + vol_regime_score * 0.20
                + zscore_persistence * 0.15
                + shock_jump_score * 0.15
                + oi_anomaly_score * 0.13
                + funding_anomaly_score * 0.12
            )
        # CARRY
        funding_persistence = linear_score(opportunity.funding_persistence_score, 1.0, 0.1)
        return (
            funding_persistence * 0.30
            + funding_anomaly_score * 0.15
            + vol_regime_score * 0.15
            + trend_regime_score * 0.15
            + shock_jump_score * 0.15
            + oi_anomaly_score * 0.10
        )

    def _fast_score(
        self,
        opportunity: PerpArbOpportunity,
        bucket: OpportunityBucket,
        entry_edge_bps: float,
        oracle_offset_bps: float,
        mark_index_divergence_bps: float,
        long_leg: PerpLegSnapshot,
        short_leg: PerpLegSnapshot,
    ) -> float:
        jump_threshold = self._jump_risk_threshold(opportunity)
        impact_score = linear_score(self._impact_buffer_bps(opportunity), 2.0, self.config.max_impact_cost_50k_bps, inverse=True)
        depth_score = linear_score(
            min(opportunity.leg_a.top_1pct_depth_usd, opportunity.leg_b.top_1pct_depth_usd),
            250_000.0,
            25_000.0,
        )
        micro_jump_score = linear_score(
            opportunity.micro_jump_frequency,
            2.0,
            jump_threshold,
            inverse=True,
        )
        if bucket == OpportunityBucket.DISLOCATION:
            spread_score = linear_score(entry_edge_bps, 35.0, -5.0)
            zscore_score = linear_score(abs(opportunity.spread_zscore), self.config.dislocation_zscore_good, 0.3)
            oracle_score = linear_score(oracle_offset_bps + mark_index_divergence_bps, 8.0, 60.0, inverse=True)
            widening_score = linear_score(opportunity.spread_widening_speed_bps_per_min, 1.0, 15.0, inverse=True)
            return (
                spread_score * 0.25
                + zscore_score * 0.08
                + impact_score * 0.14
                + depth_score * 0.14
                + micro_jump_score * 0.11
                + oracle_score * 0.10
                + widening_score * 0.18
            )
        # CARRY
        funding_diff = abs(long_leg.funding_rate_bps - short_leg.funding_rate_bps)
        funding_diff_score = linear_score(funding_diff, self.config.carry_funding_diff_good_bps, 2.0)
        hours_to_funding = self._hours_to_next_funding(opportunity)
        if hours_to_funding is not None and hours_to_funding > 0:
            hours_score = linear_score(hours_to_funding, 0.5, 6.0, inverse=True)
        else:
            hours_score = 50.0
        spread_entry = linear_score(entry_edge_bps, 20.0, -5.0)
        return (
            funding_diff_score * 0.30
            + hours_score * 0.20
            + depth_score * 0.15
            + impact_score * 0.15
            + micro_jump_score * 0.10
            + spread_entry * 0.10
        )

    def _trend_regime_score(
        self,
        opportunity: PerpArbOpportunity,
        bucket: OpportunityBucket,
    ) -> float:
        trend_signal = clamp(opportunity.trend_vs_mean_reversion, -1.0, 1.0)
        if bucket == OpportunityBucket.DISLOCATION:
            return (1.0 - trend_signal) * 50.0
        return (trend_signal + 1.0) * 50.0

    def _bucket_score(
        self,
        bucket: OpportunityBucket,
        net_edge_score: float,
        execution_score: float,
        risk_integrity_score: float,
    ) -> float:
        if bucket == OpportunityBucket.CARRY:
            return net_edge_score * 0.50 + execution_score * 0.15 + risk_integrity_score * 0.35
        return net_edge_score * 0.35 + execution_score * 0.40 + risk_integrity_score * 0.25

    def _composite_score(self, bucket: OpportunityBucket, bucket_score: float, slow_score: float, fast_score: float) -> float:
        if bucket == OpportunityBucket.CARRY:
            return bucket_score * 0.55 + slow_score * 0.30 + fast_score * 0.15
        return bucket_score * 0.50 + slow_score * 0.15 + fast_score * 0.35

    def _confidence(
        self,
        opportunity: PerpArbOpportunity,
        exit_liquidity_score: float,
        risk_integrity_score: float,
    ) -> float:
        freshness_score = linear_score(
            max(opportunity.staleness_ms, opportunity.latency_ms),
            250.0,
            max(self.config.max_staleness_ms, self.config.max_latency_ms),
            inverse=True,
        )
        return clamp(freshness_score * 0.35 + exit_liquidity_score * 0.3 + risk_integrity_score * 0.35, 0.0, 100.0)

    def _block_reasons(
        self,
        opportunity: PerpArbOpportunity,
        bucket: OpportunityBucket,
        expected_profit_bps: float,
        entry_edge_bps: float,
        oracle_offset_bps: float,
        mark_index_divergence_bps: float,
        exit_liquidity_score: float,
        risk_flags: RiskFlags,
        long_leg: PerpLegSnapshot,
        short_leg: PerpLegSnapshot,
    ) -> List[str]:
        reasons: List[str] = []
        # For carry, funding edge alone can justify the trade even if
        # the combined expected_profit (which includes entry-edge costs)
        # is negative.  Only block carry when carry_edge itself is tiny.
        if expected_profit_bps <= 0.0:
            reasons.append("expected_profit_non_positive")
        # Structural spread: large spread with low z-score means it's the
        # normal state for this pair — no reversion expected.
        if (
            bucket == OpportunityBucket.DISLOCATION
            and abs(entry_edge_bps) > self.config.structural_spread_min_bps
            and abs(opportunity.spread_zscore) < self.config.structural_spread_max_zscore
        ):
            reasons.append("structural_spread")
        is_dex = _has_dex_leg(long_leg, short_leg)
        depth_10k_min = self.config.dex_min_depth_10k_usd if is_dex else self.config.min_depth_10k_usd
        depth_50k_min = self.config.dex_min_depth_50k_usd if is_dex else self.config.min_depth_50k_usd
        impact_max = self.config.dex_max_impact_cost_50k_bps if is_dex else self.config.max_impact_cost_50k_bps
        if min(long_leg.depth_10k_usd, short_leg.depth_10k_usd) < depth_10k_min:
            reasons.append("insufficient_depth_10k")
        if min(long_leg.depth_50k_usd, short_leg.depth_50k_usd) < depth_50k_min:
            reasons.append("insufficient_depth_50k")
        # Carry typically uses smaller notional (10k); 50k impact is less critical
        if opportunity.impact_cost_50k_bps > impact_max and bucket != OpportunityBucket.CARRY:
            reasons.append("impact_cost_50k_too_high")
        # Carry uses maker entry — slippage is less relevant
        if opportunity.slippage_bps > self.config.max_slippage_bps and bucket != OpportunityBucket.CARRY:
            reasons.append("slippage_too_high")
        staleness_limit = (
            self.config.hard_staleness_carry_ms
            if bucket == OpportunityBucket.CARRY
            else self.config.hard_staleness_dislocation_ms
        )
        if opportunity.staleness_ms > staleness_limit:
            reasons.append("staleness_too_high")
        if opportunity.latency_ms > self.config.hard_latency_ms:
            reasons.append("latency_too_high")
        if oracle_offset_bps > self.config.max_oracle_offset_bps:
            reasons.append("oracle_offset_too_high")
        if mark_index_divergence_bps > self.config.max_mark_index_divergence_bps:
            reasons.append("mark_index_divergence_too_high")
        if risk_flags.oi_spike and risk_flags.funding_spike:
            reasons.append("oi_and_funding_spike")
        if exit_liquidity_score < self.config.min_exit_liquidity_score:
            reasons.append("exit_liquidity_too_low")
        if bucket == OpportunityBucket.CARRY:
            if opportunity.funding_persistence_score < self.config.carry_persistence_floor:
                reasons.append("funding_instability")
            hours_to_next = self._hours_to_next_funding(opportunity)
            if (
                hours_to_next is not None
                and hours_to_next <= self.config.carry_direction_uncertainty_hours
                and opportunity.funding_persistence_score < 0.5
            ):
                reasons.append("funding_window_uncertain")
            max_funding = max(abs(long_leg.funding_rate_bps), abs(short_leg.funding_rate_bps))
            max_oi = max(abs(long_leg.oi_change_pct), abs(short_leg.oi_change_pct))
            if (
                self._carry_squeeze_signal_count(opportunity, long_leg, short_leg)
                >= self.config.carry_squeeze_signal_count_for_action
            ):
                pass
        return reasons

    def _hours_to_next_funding(self, opportunity: PerpArbOpportunity) -> float | None:
        now = opportunity.now or datetime.utcnow()
        times = [
            funding_time
            for funding_time in (opportunity.leg_a.next_funding_time, opportunity.leg_b.next_funding_time)
            if funding_time is not None
        ]
        if not times:
            return None
        return min((funding_time - now).total_seconds() / 3600.0 for funding_time in times)

    def _label(self, block_reasons: Sequence[str], composite_score: float, confidence: float) -> ExecutionLabel:
        if block_reasons:
            return ExecutionLabel.BLOCKED
        if composite_score >= self.config.tradable_score_threshold and confidence >= self.config.min_confidence_for_tradable:
            return ExecutionLabel.TRADABLE
        if composite_score >= self.config.watch_score_threshold:
            return ExecutionLabel.WATCH
        return ExecutionLabel.BLOCKED

    def _apply_freshness_actions(
        self,
        opportunity: PerpArbOpportunity,
        risk_flags: RiskFlags,
        label: ExecutionLabel,
    ) -> Tuple[List[str], List[str], ExecutionLabel]:
        tags: List[str] = []
        advisories: List[str] = []
        if risk_flags.stale_data:
            tags.append("freshness_risk")
            advisories.append("freshness_watch")
            if label == ExecutionLabel.TRADABLE:
                label = ExecutionLabel.WATCH
                advisories.append("tradable_downgraded_by_freshness")
        return tags, advisories, label

    def _apply_jump_actions(
        self,
        opportunity: PerpArbOpportunity,
        label: ExecutionLabel,
        tags: Sequence[str] = (),
        advisories: Sequence[str] = (),
    ) -> Tuple[List[str], List[str], ExecutionLabel]:
        tags = list(tags)
        advisories = list(advisories)
        jump_threshold = self._jump_risk_threshold(opportunity)
        micro_warn = jump_threshold * self.config.micro_jump_watch_ratio
        shock_warn = jump_threshold * self.config.shock_jump_watch_ratio

        if opportunity.micro_jump_frequency >= micro_warn:
            tags.append("micro_execution_risk")
            advisories.append("micro_jump_watch")
        if opportunity.micro_jump_frequency >= jump_threshold and label == ExecutionLabel.TRADABLE:
            label = ExecutionLabel.WATCH
            advisories.append("tradable_downgraded_by_micro_jump")

        if opportunity.shock_jump_frequency >= shock_warn:
            tags.append("regime_unstable")
            advisories.append("shock_jump_watch")
        if opportunity.shock_jump_frequency >= jump_threshold:
            tags.append("liquidation_risk")
            advisories.append("shock_jump_liquidation_risk")

        # Preserve deterministic ordering while removing duplicates.
        tags = list(dict.fromkeys(tags))
        advisories = list(dict.fromkeys(advisories))
        return tags, advisories, label

    def _jump_risk_threshold(self, opportunity: PerpArbOpportunity) -> float:
        if _has_dex_leg(opportunity.leg_a, opportunity.leg_b):
            return self.config.dex_jump_risk_threshold
        return self.config.jump_risk_threshold

    def _carry_squeeze_signal_count(
        self,
        opportunity: PerpArbOpportunity,
        long_leg: PerpLegSnapshot,
        short_leg: PerpLegSnapshot,
    ) -> int:
        max_funding = max(abs(long_leg.funding_rate_bps), abs(short_leg.funding_rate_bps))
        max_oi = max(abs(long_leg.oi_change_pct), abs(short_leg.oi_change_pct))
        signals = 0
        if max_funding >= self.config.high_risk_funding_bps:
            signals += 1
        if max_oi >= self.config.oi_spike_threshold_pct:
            signals += 1
        if opportunity.realized_vol >= self.config.high_risk_realized_vol:
            signals += 1
        return signals

    def _apply_carry_squeeze_actions(
        self,
        opportunity: PerpArbOpportunity,
        bucket: OpportunityBucket,
        tags: Sequence[str],
        advisories: Sequence[str],
        label: ExecutionLabel,
    ) -> Tuple[List[str], List[str], ExecutionLabel]:
        if bucket != OpportunityBucket.CARRY:
            return list(tags), list(advisories), label

        signal_count = self._carry_squeeze_signal_count(opportunity, opportunity.leg_a, opportunity.leg_b)
        next_tags = list(tags)
        next_advisories = list(advisories)
        next_label = label

        if signal_count >= self.config.carry_squeeze_signal_count_for_action:
            next_tags.append("carry_squeeze_risk")
            next_advisories.append("carry_squeeze_reduce_only")
            if next_label == ExecutionLabel.TRADABLE:
                next_label = ExecutionLabel.WATCH
                next_advisories.append("tradable_downgraded_by_carry_squeeze")

        return list(dict.fromkeys(next_tags)), list(dict.fromkeys(next_advisories)), next_label

    def _derive_execution_policy(
        self,
        opportunity: PerpArbOpportunity,
        bucket: OpportunityBucket,
        label: ExecutionLabel,
        block_reasons: List[str],
        tags: Sequence[str],
        advisories: List[str],
    ) -> Tuple[ExecutionPolicy, ExecutionLabel, List[str], List[str]]:
        allow_taker = True
        allow_maker = True
        size_multiplier = 1.0
        allow_carry = True
        allow_dislocation = True
        notes: List[str] = []

        if "micro_execution_risk" in tags:
            allow_taker = False
            notes.append("maker_only")

        if "regime_unstable" in tags:
            size_multiplier = min(size_multiplier, 0.5)
            notes.append("reduce_size_half")

        if bucket == OpportunityBucket.CARRY and "carry_squeeze_risk" in tags:
            size_multiplier = min(size_multiplier, self.config.carry_squeeze_size_multiplier)
            notes.append("carry_squeeze_reduce_size")

        if "liquidation_risk" in tags:
            allow_carry = False
            size_multiplier = min(size_multiplier, 0.35 if bucket == OpportunityBucket.DISLOCATION else 0.5)
            notes.append("carry_blocked")
            if bucket == OpportunityBucket.DISLOCATION:
                notes.append("dislocation_only_short_horizon")
            else:
                notes.append("carry_not_allowed_under_liquidation_risk")
                if "carry_blocked_by_liquidation_risk" not in block_reasons:
                    block_reasons = list(block_reasons) + ["carry_blocked_by_liquidation_risk"]
                if "carry_blocked_by_liquidation_risk" not in advisories:
                    advisories = list(advisories) + ["carry_blocked_by_liquidation_risk"]
                label = ExecutionLabel.BLOCKED

        policy = ExecutionPolicy(
            allow_taker=allow_taker,
            allow_maker=allow_maker,
            size_multiplier=size_multiplier,
            allow_carry=allow_carry,
            allow_dislocation=allow_dislocation,
            notes=list(dict.fromkeys(notes)),
        )
        return policy, label, block_reasons, advisories

    def _derive_execution_plan(
        self,
        opportunity: PerpArbOpportunity,
        bucket: OpportunityBucket,
        direction: str,
        label: ExecutionLabel,
        block_reasons: Sequence[str],
        policy: ExecutionPolicy,
    ) -> ExecutionPlan:
        allowed_strategies: List[str] = []
        if policy.allow_dislocation:
            allowed_strategies.append(OpportunityBucket.DISLOCATION.value)
        if policy.allow_carry:
            allowed_strategies.append(OpportunityBucket.CARRY.value)

        max_notional_usd = round(opportunity.notional_usd * policy.size_multiplier, 4)
        if label == ExecutionLabel.TRADABLE:
            action = "execute"
            target_notional_usd = max_notional_usd
        elif label == ExecutionLabel.WATCH:
            action = "observe"
            target_notional_usd = 0.0
        else:
            action = "skip"
            target_notional_usd = 0.0

        if label == ExecutionLabel.BLOCKED:
            execution_style = "disabled"
        elif not policy.allow_taker and policy.allow_maker:
            execution_style = "maker_only"
        elif policy.allow_taker and not policy.allow_maker:
            execution_style = "taker_only"
        else:
            execution_style = "taker_or_maker"

        notes = list(policy.notes)
        if bucket.value not in allowed_strategies:
            notes.append(f"{bucket.value}_strategy_disabled")
        if label == ExecutionLabel.BLOCKED and block_reasons:
            notes.append(f"blocked:{block_reasons[0]}")
        legs = self._build_execution_legs(
            opportunity,
            direction,
            action,
            execution_style,
            target_notional_usd,
        )

        return ExecutionPlan(
            action=action,
            execution_style=execution_style,
            target_notional_usd=target_notional_usd,
            max_notional_usd=max_notional_usd,
            allowed_strategies=allowed_strategies,
            notes=list(dict.fromkeys(notes)),
            legs=legs,
        )

    def _build_execution_legs(
        self,
        opportunity: PerpArbOpportunity,
        direction: str,
        action: str,
        execution_style: str,
        target_notional_usd: float,
    ) -> List[ExecutionLegPlan]:
        if direction == "long_a_short_b":
            ordered_legs = [
                ("primary", opportunity.leg_a, "buy"),
                ("hedge", opportunity.leg_b, "sell"),
            ]
        else:
            ordered_legs = [
                ("primary", opportunity.leg_b, "buy"),
                ("hedge", opportunity.leg_a, "sell"),
            ]

        legs: List[ExecutionLegPlan] = []
        for role, leg, side in ordered_legs:
            reference_price = leg.best_ask if side == "buy" else leg.best_bid
            limit_price, order_type, tif, post_only = self._execution_pricing(
                leg,
                side,
                execution_style,
            )
            notes: List[str] = []
            if action != "execute":
                notes.append("draft_only")
            if role == "hedge":
                notes.append("hedge_leg")
            legs.append(
                ExecutionLegPlan(
                    venue=leg.venue,
                    side=side,
                    role=role,
                    order_type=order_type,
                    time_in_force=tif,
                    reference_price=round(reference_price, 8),
                    limit_price=round(limit_price, 8),
                    notional_usd=round(target_notional_usd, 4),
                    estimated_quantity=round(safe_div(target_notional_usd, max(reference_price, 1e-9)), 8),
                    post_only=post_only,
                    reduce_only=False,
                    enabled=action == "execute",
                    notes=notes,
                )
            )
        return legs

    def _execution_pricing(
        self,
        leg: PerpLegSnapshot,
        side: str,
        execution_style: str,
    ) -> Tuple[float, str, str, bool]:
        if execution_style == "maker_only":
            if side == "buy":
                return leg.best_bid, "limit", "gtc", True
            return leg.best_ask, "limit", "gtc", True
        if execution_style == "disabled":
            reference = leg.best_ask if side == "buy" else leg.best_bid
            return reference, "none", "none", False
        if side == "buy":
            return leg.best_ask, "marketable_limit", "ioc", False
        return leg.best_bid, "marketable_limit", "ioc", False


def score_opportunities(
    opportunities: Iterable[PerpArbOpportunity],
    config: ScoringConfig | None = None,
) -> List[ScoredOpportunity]:
    scorer = ArbitrageScorer(config=config)
    return scorer.rank(list(opportunities))
