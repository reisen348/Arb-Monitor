from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Deque, DefaultDict, Iterable, List, Optional, Sequence, Tuple

from .market_data import MarketSnapshot
from .models import PerpArbOpportunity


@dataclass(frozen=True)
class StateTrackerConfig:
    max_points: int = 600
    opportunity_max_points: Optional[int] = None
    min_points_for_zscore: int = 3
    jump_threshold_bps: float = 8.0
    micro_jump_window_seconds: float = 5.0
    shock_jump_window_seconds: float = 60.0

    @property
    def opportunity_points(self) -> int:
        points = self.max_points if self.opportunity_max_points is None else self.opportunity_max_points
        return max(1, int(points))


@dataclass(frozen=True)
class SnapshotStatePoint:
    timestamp: datetime
    mark_price: float
    funding_rate_bps: float
    oi_usd: float
    premium_bps: float


@dataclass(frozen=True)
class OpportunityStatePoint:
    timestamp: datetime
    executable_spread_bps: float


# ---------------------------------------------------------------------------
# Incremental statistics helpers
# ---------------------------------------------------------------------------

class RunningStats:
    """Welford online algorithm for incremental mean/variance over a bounded window."""

    __slots__ = ("_max_n", "_values", "_sum", "_sum_sq", "_count")

    def __init__(self, max_n: int = 120) -> None:
        self._max_n = max_n
        self._values: Deque[float] = deque(maxlen=max_n)
        self._sum = 0.0
        self._sum_sq = 0.0
        self._count = 0

    def push(self, value: float) -> None:
        if self._count >= self._max_n:
            evicted = self._values[0]
            self._sum -= evicted
            self._sum_sq -= evicted * evicted
            self._count -= 1
        self._values.append(value)
        self._sum += value
        self._sum_sq += value * value
        self._count += 1

    @property
    def count(self) -> int:
        return self._count

    @property
    def mean(self) -> float:
        if self._count == 0:
            return 0.0
        return self._sum / self._count

    @property
    def pstdev(self) -> float:
        if self._count < 2:
            return 0.0
        variance = self._sum_sq / self._count - (self._sum / self._count) ** 2
        return math.sqrt(max(0.0, variance))

    def zscore(self, new_value: float, min_points: int) -> Optional[float]:
        """Z-score of new_value against current window (excluding new_value).

        min_points refers to the total number of values including new_value,
        matching the semantics of the original _zscore() function.
        """
        if self._count + 1 < min_points:
            return None
        if self._count < 2:
            return None
        sigma = self.pstdev
        if sigma <= 1e-9:
            return 0.0
        return (new_value - self.mean) / sigma

    def values_list(self) -> List[float]:
        return list(self._values)

    def restore(self, values: Sequence[float]) -> None:
        """Bulk-load historical values."""
        for v in values:
            self.push(v)


class JumpCounter:
    """Incremental jump frequency tracker over a time-windowed deque."""

    __slots__ = ("_threshold_bps", "_window_seconds", "_points")

    def __init__(self, threshold_bps: float, window_seconds: float) -> None:
        self._threshold_bps = threshold_bps
        self._window_seconds = window_seconds
        self._points: Deque[Tuple[float, float]] = deque()  # (epoch, mark_price)

    def push(self, timestamp: datetime, mark_price: float) -> float:
        epoch = timestamp.timestamp()
        self._points.append((epoch, mark_price))
        # Evict points outside window
        cutoff = epoch - self._window_seconds
        while self._points and self._points[0][0] < cutoff:
            self._points.popleft()
        return self._compute()

    def _compute(self) -> float:
        if len(self._points) < 2:
            return 0.0
        returns_count = 0
        jump_count = 0
        items = list(self._points)
        for i in range(1, len(items)):
            prev_price = items[i - 1][1]
            cur_price = items[i][1]
            if prev_price <= 0 or cur_price <= 0:
                continue
            ret = abs(math.log(cur_price / prev_price) * 10_000.0)
            returns_count += 1
            if ret >= self._threshold_bps:
                jump_count += 1
        if returns_count == 0:
            return 0.0
        return jump_count / returns_count * 100.0

    def restore(self, points: Sequence[Tuple[float, float]]) -> None:
        for epoch, price in points:
            self._points.append((epoch, price))


class ReturnAccumulator:
    """Tracks returns for realized vol and trend efficiency incrementally."""

    __slots__ = ("_max_n", "_returns", "_sum", "_sum_abs", "_sum_sq", "_count")

    def __init__(self, max_n: int = 120) -> None:
        self._max_n = max_n
        self._returns: Deque[float] = deque(maxlen=max_n)
        self._sum = 0.0
        self._sum_abs = 0.0
        self._sum_sq = 0.0
        self._count = 0

    def push(self, prev_price: float, cur_price: float) -> None:
        if prev_price <= 0 or cur_price <= 0:
            return
        ret = math.log(cur_price / prev_price) * 10_000.0
        if self._count >= self._max_n:
            evicted = self._returns[0]
            self._sum -= evicted
            self._sum_abs -= abs(evicted)
            self._sum_sq -= evicted * evicted
            self._count -= 1
        self._returns.append(ret)
        self._sum += ret
        self._sum_abs += abs(ret)
        self._sum_sq += ret * ret
        self._count += 1

    @property
    def realized_vol_score(self) -> float:
        if self._count < 2:
            return 0.0
        variance = self._sum_sq / self._count - (self._sum / self._count) ** 2
        sigma = math.sqrt(max(0.0, variance))
        return min(100.0, sigma * math.sqrt(self._count) * 0.9)

    @property
    def trend_efficiency(self) -> float:
        if self._count == 0 or self._sum_abs == 0:
            return 0.0
        return max(-1.0, min(1.0, self._sum / self._sum_abs))


# ---------------------------------------------------------------------------
# Per-key incremental state bundle
# ---------------------------------------------------------------------------

class _SnapshotIncrementalState:
    """All incremental accumulators for one (venue, asset, quote) key."""

    __slots__ = ("premium_stats", "return_acc", "micro_jump", "shock_jump")

    def __init__(self, config: StateTrackerConfig) -> None:
        self.premium_stats = RunningStats(config.max_points)
        self.return_acc = ReturnAccumulator(config.max_points)
        self.micro_jump = JumpCounter(config.jump_threshold_bps, config.micro_jump_window_seconds)
        self.shock_jump = JumpCounter(config.jump_threshold_bps, config.shock_jump_window_seconds)


class _OpportunityIncrementalState:
    """Incremental accumulator for one opportunity key."""

    __slots__ = ("spread_stats",)

    def __init__(self, config: StateTrackerConfig) -> None:
        self.spread_stats = RunningStats(config.opportunity_points)


# ---------------------------------------------------------------------------
# Main trackers (public API unchanged)
# ---------------------------------------------------------------------------

class MarketStateTracker:
    def __init__(self, config: StateTrackerConfig | None = None) -> None:
        self.config = config or StateTrackerConfig()
        self._history: DefaultDict[Tuple[str, str, str], Deque[SnapshotStatePoint]] = defaultdict(
            lambda: deque(maxlen=self.config.max_points)
        )
        self._incremental: DefaultDict[Tuple[str, str, str], _SnapshotIncrementalState] = defaultdict(
            lambda: _SnapshotIncrementalState(self.config)
        )

    def restore_history(self, history: DefaultDict[Tuple[str, str, str], Deque[SnapshotStatePoint]]) -> None:
        """Restore snapshot history from persistence layer and rebuild incremental state."""
        for key, points in history.items():
            target = self._history[key]
            inc = self._incremental[key]
            prev_price = None
            for point in points:
                target.append(point)
                inc.premium_stats.push(point.premium_bps)
                if prev_price is not None:
                    inc.return_acc.push(prev_price, point.mark_price)
                inc.micro_jump.push(point.timestamp, point.mark_price)
                inc.shock_jump.push(point.timestamp, point.mark_price)
                prev_price = point.mark_price

    def enrich_snapshots(self, snapshots: Iterable[MarketSnapshot]) -> List[MarketSnapshot]:
        enriched: List[MarketSnapshot] = []
        for snapshot in snapshots:
            enriched_snapshot = self._enrich_snapshot(snapshot)
            enriched.append(enriched_snapshot)
            self._append_snapshot(enriched_snapshot)
        return enriched

    def _enrich_snapshot(self, snapshot: MarketSnapshot) -> MarketSnapshot:
        key = self._key(snapshot)
        history = self._history[key]
        inc = self._incremental[key]
        timestamp = snapshot.timestamp or datetime.now(timezone.utc)
        premium_bps = self._premium_bps(snapshot)
        previous = history[-1] if history else None

        funding_change_bps = snapshot.funding_rate_bps - previous.funding_rate_bps if previous else snapshot.funding_change_bps
        oi_change_pct = (
            (snapshot.oi_usd - previous.oi_usd) / previous.oi_usd * 100.0
            if previous and previous.oi_usd > 0
            else snapshot.oi_change_pct
        )

        # Incremental realized vol and trend efficiency
        if previous is not None:
            inc.return_acc.push(previous.mark_price, snapshot.mark_price)
        computed_realized_vol = inc.return_acc.realized_vol_score
        trend_vs_mean_reversion = inc.return_acc.trend_efficiency

        # Incremental jump frequencies
        micro_jump_frequency = inc.micro_jump.push(timestamp, snapshot.mark_price)
        shock_jump_frequency = inc.shock_jump.push(timestamp, snapshot.mark_price)
        jump_frequency = max(snapshot.jump_frequency, micro_jump_frequency, shock_jump_frequency)

        # Incremental z-score: compute against current window, then push
        spread_zscore = inc.premium_stats.zscore(premium_bps, self.config.min_points_for_zscore)
        inc.premium_stats.push(premium_bps)

        return replace(
            snapshot,
            oi_change_pct=oi_change_pct,
            funding_change_bps=funding_change_bps,
            realized_vol=max(snapshot.realized_vol, computed_realized_vol),
            jump_frequency=jump_frequency,
            micro_jump_frequency=max(snapshot.micro_jump_frequency, micro_jump_frequency),
            shock_jump_frequency=max(snapshot.shock_jump_frequency, shock_jump_frequency),
            spread_zscore=spread_zscore if spread_zscore is not None else snapshot.spread_zscore,
            trend_vs_mean_reversion=trend_vs_mean_reversion if previous else snapshot.trend_vs_mean_reversion,
            timestamp=timestamp,
        )

    def _append_snapshot(self, snapshot: MarketSnapshot) -> None:
        self._history[self._key(snapshot)].append(
            SnapshotStatePoint(
                timestamp=snapshot.timestamp or datetime.now(timezone.utc),
                mark_price=snapshot.mark_price,
                funding_rate_bps=snapshot.funding_rate_bps,
                oi_usd=snapshot.oi_usd,
                premium_bps=self._premium_bps(snapshot),
            )
        )

    @staticmethod
    def _key(snapshot: MarketSnapshot) -> Tuple[str, str, str]:
        quote = snapshot.quote.upper()
        if quote in {"USD", "USDT", "USDC", "FDUSD", "USDE"}:
            quote = "USD"
        return snapshot.venue, snapshot.asset.upper(), quote

    @staticmethod
    def _premium_bps(snapshot: MarketSnapshot) -> float:
        reference = snapshot.index_price or snapshot.oracle_price or snapshot.mark_price
        if not reference:
            return 0.0
        return (snapshot.mark_price - reference) / reference * 10_000.0


class OpportunityStateTracker:
    def __init__(self, config: StateTrackerConfig | None = None) -> None:
        self.config = config or StateTrackerConfig()
        self._history: DefaultDict[Tuple[str, str, str, str], Deque[OpportunityStatePoint]] = defaultdict(
            lambda: deque(maxlen=self.config.opportunity_points)
        )
        self._incremental: DefaultDict[Tuple[str, str, str, str], _OpportunityIncrementalState] = defaultdict(
            lambda: _OpportunityIncrementalState(self.config)
        )

    def restore_history(self, history: DefaultDict[Tuple[str, str, str, str], Deque[OpportunityStatePoint]]) -> None:
        """Restore opportunity history from persistence layer and rebuild incremental state."""
        for key, points in history.items():
            target = self._history[key]
            inc = self._incremental[key]
            for point in points:
                target.append(point)
                inc.spread_stats.push(point.executable_spread_bps)

    def enrich_opportunities(self, opportunities: Iterable[PerpArbOpportunity]) -> List[PerpArbOpportunity]:
        enriched: List[PerpArbOpportunity] = []
        for opportunity in opportunities:
            enriched_opportunity = self._enrich_opportunity(opportunity)
            enriched.append(enriched_opportunity)
            self._append_opportunity(enriched_opportunity)
        return enriched

    def _enrich_opportunity(self, opportunity: PerpArbOpportunity) -> PerpArbOpportunity:
        key = self._key(opportunity)
        history = self._history[key]
        inc = self._incremental[key]
        current_spread = _entry_edge_bps(opportunity)

        # Incremental z-score: compute against current window, then push
        spread_zscore = inc.spread_stats.zscore(current_spread, self.config.min_points_for_zscore)
        inc.spread_stats.push(current_spread)

        spread_widening_speed = opportunity.spread_widening_speed_bps_per_min
        if history:
            previous = history[-1]
            # Consecutive scans in local tests can happen almost instantly; clamp to 1 second.
            minutes = max((_opportunity_time(opportunity) - previous.timestamp).total_seconds() / 60.0, 1.0 / 60.0)
            spread_widening_speed = (current_spread - previous.executable_spread_bps) / minutes
        spread_mean = inc.spread_stats.mean if inc.spread_stats.count >= self.config.min_points_for_zscore else 0.0
        return replace(
            opportunity,
            spread_zscore=spread_zscore if spread_zscore is not None else opportunity.spread_zscore,
            spread_mean_bps=spread_mean,
            spread_widening_speed_bps_per_min=spread_widening_speed,
        )

    def _append_opportunity(self, opportunity: PerpArbOpportunity) -> None:
        self._history[self._key(opportunity)].append(
            OpportunityStatePoint(
                timestamp=_opportunity_time(opportunity),
                executable_spread_bps=_entry_edge_bps(opportunity),
            )
        )

    @staticmethod
    def _key(opportunity: PerpArbOpportunity) -> Tuple[str, str, str, str]:
        venues = sorted((opportunity.leg_a.venue, opportunity.leg_b.venue))
        quote = opportunity.quote.upper()
        if quote in {"USD", "USDT", "USDC", "FDUSD", "USDE"}:
            quote = "USD"
        return opportunity.asset.upper(), quote, venues[0], venues[1]


def _opportunity_time(opportunity: PerpArbOpportunity) -> datetime:
    return opportunity.now or datetime.now(timezone.utc)


def _entry_edge_bps(opportunity: PerpArbOpportunity) -> float:
    a_long_b_short = (
        (opportunity.leg_b.best_bid - opportunity.leg_a.best_ask)
        / max(opportunity.leg_a.mid_price, 1e-9)
        * 10_000.0
    )
    b_long_a_short = (
        (opportunity.leg_a.best_bid - opportunity.leg_b.best_ask)
        / max(opportunity.leg_b.mid_price, 1e-9)
        * 10_000.0
    )
    return max(a_long_b_short, b_long_a_short)
