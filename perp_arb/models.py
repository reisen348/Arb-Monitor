from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional


class OpportunityBucket(str, Enum):
    DISLOCATION = "dislocation"
    CARRY = "carry"


class ExecutionLabel(str, Enum):
    TRADABLE = "tradable"
    WATCH = "watch"
    BLOCKED = "blocked"


@dataclass(frozen=True)
class PerpLegSnapshot:
    venue: str
    market_type: str
    asset: str
    best_bid: float
    best_ask: float
    mark_price: float
    oracle_price: float
    index_price: float
    taker_fee_bps: float
    maker_fee_bps: float = 0.0
    depth_10k_usd: float = 0.0
    depth_50k_usd: float = 0.0
    top_1pct_depth_usd: float = 0.0
    volume_depth_ratio: float = 0.0
    volume_24h_usd: float = 0.0
    oi_usd: float = 0.0
    oi_change_pct: float = 0.0
    funding_rate_bps: float = 0.0
    funding_change_bps: float = 0.0
    funding_interval_hours: float = 8.0
    next_funding_time: Optional[datetime] = None

    @property
    def mid_price(self) -> float:
        return (self.best_bid + self.best_ask) / 2.0

    @property
    def oracle_offset_bps(self) -> float:
        if not self.oracle_price:
            return 0.0
        return abs(self.mark_price - self.oracle_price) / self.oracle_price * 10_000

    @property
    def mark_index_divergence_bps(self) -> float:
        if not self.index_price:
            return 0.0
        return abs(self.mark_price - self.index_price) / self.index_price * 10_000


@dataclass(frozen=True)
class PerpArbOpportunity:
    asset: str
    quote: str
    leg_a: PerpLegSnapshot
    leg_b: PerpLegSnapshot
    notional_usd: float
    capital_used_usd: float
    slippage_bps: float = 0.0
    impact_cost_10k_bps: float = 0.0
    impact_cost_50k_bps: float = 0.0
    realized_vol: float = 0.0
    jump_frequency: float = 0.0
    micro_jump_frequency: float = 0.0
    shock_jump_frequency: float = 0.0
    spread_zscore: float = 0.0
    spread_mean_bps: float = 0.0
    trend_vs_mean_reversion: float = 0.0
    latency_ms: float = 0.0
    staleness_ms: float = 0.0
    holding_window_hours: float = 1.0
    funding_interval_hours: float = 8.0
    funding_persistence_score: float = 0.5
    spread_widening_speed_bps_per_min: float = 0.0
    bucket_hint: Optional[OpportunityBucket] = None
    now: Optional[datetime] = None
    metadata: dict = field(default_factory=dict)


@dataclass(frozen=True)
class RiskFlags:
    oracle_deviation: bool = False
    mark_index_divergence: bool = False
    funding_spike: bool = False
    oi_spike: bool = False
    thin_book: bool = False
    stale_data: bool = False
    jump_risk: bool = False
    exit_risk: bool = False


@dataclass(frozen=True)
class ScoreBreakdown:
    bucket_type: OpportunityBucket
    direction: str
    net_edge_score: float
    execution_score: float
    risk_integrity_score: float
    slow_score: float
    fast_score: float
    bucket_score: float
    composite_score: float
    expected_profit_bps: float
    expected_profit_usd: float
    roi_per_capital: float
    confidence: float
    entry_edge_bps: float
    carry_edge_bps: float
    fee_bps: float
    impact_buffer_bps: float
    exit_cost_buffer_bps: float
    oracle_risk_buffer_bps: float
    exit_liquidity_score: float
    oracle_offset_bps: float
    mark_index_divergence_bps: float


@dataclass(frozen=True)
class ExecutionPolicy:
    allow_taker: bool = True
    allow_maker: bool = True
    size_multiplier: float = 1.0
    allow_carry: bool = True
    allow_dislocation: bool = True
    notes: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class ExecutionLegPlan:
    venue: str
    side: str
    role: str
    order_type: str
    time_in_force: str
    reference_price: float
    limit_price: float
    notional_usd: float
    estimated_quantity: float
    post_only: bool = False
    reduce_only: bool = False
    enabled: bool = True
    notes: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class ExecutionPlan:
    action: str = "observe"
    execution_style: str = "taker_or_maker"
    target_notional_usd: float = 0.0
    max_notional_usd: float = 0.0
    allowed_strategies: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    legs: List[ExecutionLegPlan] = field(default_factory=list)


@dataclass(frozen=True)
class ScoredOpportunity:
    opportunity: PerpArbOpportunity
    breakdown: ScoreBreakdown
    risk_flags: RiskFlags
    label: ExecutionLabel
    block_reasons: List[str]
    tags: List[str]
    advisories: List[str]
    policy: ExecutionPolicy
    execution_plan: ExecutionPlan
