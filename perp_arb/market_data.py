from __future__ import annotations

import math
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence

from .models import ExecutionLabel, PerpArbOpportunity, PerpLegSnapshot, ScoredOpportunity


@dataclass(frozen=True)
class SourceStatus:
    """Per-adapter health metadata for a single poll cycle."""
    adapter_name: str
    ok: bool
    snapshot_count: int = 0
    poll_duration_ms: float = 0.0
    error: Optional[str] = None
    timestamp: Optional[datetime] = None


@dataclass(frozen=True)
class MarketSnapshot:
    venue: str
    market_type: str
    asset: str
    quote: str
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
    oi_usd: float = 0.0
    oi_change_pct: float = 0.0
    funding_rate_bps: float = 0.0
    funding_change_bps: float = 0.0
    next_funding_time: Optional[datetime] = None
    impact_cost_10k_bps: Optional[float] = None
    impact_cost_50k_bps: Optional[float] = None
    slippage_bps: Optional[float] = None
    realized_vol: float = 0.0
    jump_frequency: float = 0.0
    micro_jump_frequency: float = 0.0
    shock_jump_frequency: float = 0.0
    spread_zscore: float = 0.0
    trend_vs_mean_reversion: float = 0.0
    latency_ms: float = 0.0
    staleness_ms: float = 0.0
    timestamp: Optional[datetime] = None
    metadata: dict = field(default_factory=dict)


@dataclass(frozen=True)
class PairingConfig:
    default_notional_usd: float = 10_000.0
    leverage_assumption: float = 4.0
    capital_buffer_ratio: float = 0.15
    dislocation_holding_hours: float = 1.0
    carry_holding_hours: float = 8.0
    funding_interval_hours: float = 8.0
    funding_dominance_ratio: float = 0.6
    carry_funding_diff_min_bps: float = 1.0  # absolute minimum funding diff to consider carry
    carry_entry_edge_max_bps: float = 5.0  # allow standalone carry only when entry spread is already small
    funding_persistence_default: float = 0.65


@dataclass(frozen=True)
class ScannerConfig:
    top_n: int = 10
    min_label: ExecutionLabel = ExecutionLabel.WATCH
    scan_interval_seconds: float = 2.0


@dataclass(frozen=True)
class ScanBatch:
    timestamp: datetime
    snapshots: Sequence[MarketSnapshot]
    opportunities: Sequence[PerpArbOpportunity]
    scored_opportunities: Sequence[ScoredOpportunity]
    source_statuses: Sequence[SourceStatus] = field(default_factory=list)
    scan_duration_ms: float = 0.0


class MarketDataAdapter(ABC):
    name: str

    @abstractmethod
    def poll(self) -> Sequence[MarketSnapshot]:
        raise NotImplementedError


class OpportunityBuilder:
    def __init__(self, config: PairingConfig | None = None) -> None:
        self.config = config or PairingConfig()

    # Asset aliases: map exchange-specific symbols to a canonical name
    # so cross-venue pairs tracking the same underlying can be matched.
    ASSET_ALIASES: Dict[str, str] = {
        "XAU": "PAXG",
        "XAUT": "PAXG",
    }

    STOCK_LIKE_VENUES = {"aster", "bitget", "okx", "hyperliquid:xyz"}
    DYNAMIC_STOCK_INFERENCE_VENUES = {"okx"}
    STOCK_OR_ETF_SYMBOLS = {
        "AAPL", "AAOI", "ADBE", "ADVANTEST", "AMD", "AMAT", "AMZN", "ANTHROPIC",
        "ARM", "ASML", "ASTS", "AVGO", "AXTI", "BBX", "BE", "BMNR", "BP", "BRKB",
        "BX", "CAT", "COHR", "COIN", "COST", "CRM", "CRCL", "CRDO", "CRWD",
        "CSCO", "DELL", "DIS", "DKNG", "EBAY", "EWJ", "EWZ", "EWT", "FLNC",
        "FUTU", "GME", "GOOG", "GOOGL", "HD", "HIMS", "HPE", "HYUNDAI",
        "IBM", "IONQ", "IREN", "IWM", "JPM", "LLY", "META", "MRVL", "MSFT",
        "MSTR", "MU", "NBIS", "NFLX", "NOK", "NOW", "NVDA", "NVO", "OPENAI", "ORCL",
        "PLTR", "QCOM", "QQQ", "RKLB", "RIVN", "CRM", "RTX", "SAMSUNG", "SKHYNIX",
        "SOXL", "SPY", "SPCX", "MINIMAX", "TSLA", "UBER", "URNM", "USAR", "UVXY", "V",
        "WDC", "WMT", "XLE", "ZM",
    }

    def build(self, snapshots: Iterable[MarketSnapshot]) -> List[PerpArbOpportunity]:
        snapshot_list = list(snapshots)
        dynamic_stock_assets = {
            self._normalized_asset(snapshot.asset)
            for snapshot in snapshot_list
            if snapshot.metadata.get("stock_like")
        }
        best_by_venue: Dict[tuple, MarketSnapshot] = {}
        for snapshot in snapshot_list:
            if not snapshot.market_type.startswith("perp"):
                continue
            if snapshot.oi_usd <= 0:
                continue
            canonical_asset = self._normalized_asset(snapshot.asset)
            market_family = self._market_family(snapshot, canonical_asset, dynamic_stock_assets)
            key = (canonical_asset, self._normalized_quote(snapshot.quote), market_family, snapshot.venue)
            current = best_by_venue.get(key)
            if current is None or self._snapshot_liquidity_rank(snapshot) > self._snapshot_liquidity_rank(current):
                best_by_venue[key] = snapshot

        grouped: Dict[tuple, List[MarketSnapshot]] = {}
        for asset, quote, market_family, venue in best_by_venue:
            grouped.setdefault((asset, quote, market_family), []).append(best_by_venue[(asset, quote, market_family, venue)])

        opportunities: List[PerpArbOpportunity] = []
        for (asset, quote, _market_family), markets in grouped.items():
            if len(markets) < 2:
                continue
            ordered_markets = sorted(markets, key=lambda item: item.venue)
            for index, snapshot_a in enumerate(ordered_markets):
                for snapshot_b in ordered_markets[index + 1:]:
                    if self._price_alignment_suspect(snapshot_a, snapshot_b):
                        continue
                    opportunities.append(self._build_opportunity(asset, quote, snapshot_a, snapshot_b))
        return opportunities

    @classmethod
    def _market_family(cls, snapshot: MarketSnapshot, canonical_asset: str, dynamic_stock_assets: set[str]) -> str:
        stock_like = snapshot.metadata.get("stock_like")
        if stock_like is True:
            return "stock"
        if stock_like is False:
            return "crypto"
        if canonical_asset in cls.STOCK_OR_ETF_SYMBOLS and snapshot.mark_price >= 1.0:
            return "stock"
        venue = snapshot.venue.removesuffix("-ws").lower()
        if (
            snapshot.mark_price >= 1.0
            and venue in cls.DYNAMIC_STOCK_INFERENCE_VENUES
            and canonical_asset in dynamic_stock_assets
        ):
            return "stock"
        return "crypto"

    def _price_alignment_suspect(self, snapshot_a: MarketSnapshot, snapshot_b: MarketSnapshot) -> bool:
        price_a = snapshot_a.mark_price
        price_b = snapshot_b.mark_price
        if price_a <= 0 or price_b <= 0:
            return False
        ratio = max(price_a, price_b) / min(price_a, price_b)
        if ratio >= 1.20:
            return True
        if ratio >= 1.25 and (self._is_sparse_price_source(snapshot_a) or self._is_sparse_price_source(snapshot_b)):
            return True
        if ratio >= 1.01 and self._has_lighter_ticker_only_price(snapshot_a, snapshot_b):
            return True
        if ratio >= 1.02 and self._has_ticker_only_price(snapshot_a, snapshot_b):
            return True
        volume_a = self._volume_24h_usd(snapshot_a)
        volume_b = self._volume_24h_usd(snapshot_b)
        min_volume = min(volume_a, volume_b)
        min_oi = min(snapshot_a.oi_usd, snapshot_b.oi_usd)
        if ratio >= 1.02 and min_volume < 100_000.0:
            return True
        if ratio >= 1.05 and (min_volume < 100_000.0 or min_oi < 50_000.0):
            return True
        if ratio >= 1.04 and min_oi < 25_000.0:
            return True
        if ratio >= 1.03 and (min_volume < 50_000.0 or min_oi < 10_000.0):
            return True
        if ratio >= 1.02 and (min_volume < 50_000.0 or min_oi < 50_000.0):
            return True
        if ratio >= 1.01 and (min_volume < 10_000.0 or min_oi < 10_000.0):
            return True
        return False

    def _is_sparse_price_source(self, snapshot: MarketSnapshot) -> bool:
        volume_24h = self._volume_24h_usd(snapshot)
        source = str(snapshot.metadata.get("source") or snapshot.venue).lower()
        if volume_24h > 0:
            return False
        return source == "nado" or bool(snapshot.metadata.get("ticker_only"))

    @staticmethod
    def _has_ticker_only_price(*snapshots: MarketSnapshot) -> bool:
        return any(bool(snapshot.metadata.get("ticker_only")) for snapshot in snapshots)

    @staticmethod
    def _has_lighter_ticker_only_price(*snapshots: MarketSnapshot) -> bool:
        return any(
            bool(snapshot.metadata.get("ticker_only"))
            and str(snapshot.metadata.get("source") or snapshot.venue).lower() == "lighter"
            for snapshot in snapshots
        )

    def _snapshot_liquidity_rank(self, snapshot: MarketSnapshot) -> tuple:
        full_book = 0 if snapshot.metadata.get("ticker_only") else 1
        return (
            full_book,
            snapshot.oi_usd,
            self._volume_24h_usd(snapshot),
            snapshot.top_1pct_depth_usd,
        )

    @classmethod
    def _normalized_asset(cls, asset: str) -> str:
        upper = asset.upper()
        return cls.ASSET_ALIASES.get(upper, upper)

    @staticmethod
    def _normalized_quote(quote: str) -> str:
        stable_aliases = {"USD", "USDT", "USDC", "FDUSD", "USDE", "USDT0"}
        if quote.upper() in stable_aliases:
            return "USD"
        return quote.upper()

    def _build_opportunity(
        self,
        asset: str,
        quote: str,
        snapshot_a: MarketSnapshot,
        snapshot_b: MarketSnapshot,
    ) -> PerpArbOpportunity:
        leg_a = self._build_leg(snapshot_a)
        leg_b = self._build_leg(snapshot_b)
        notional_usd = self._effective_notional(snapshot_a, snapshot_b)
        capital_used_usd = (
            notional_usd / max(self.config.leverage_assumption, 1e-9) * (1.0 + self.config.capital_buffer_ratio)
        )
        slippage_bps = self._slippage_estimate(snapshot_a, snapshot_b, notional_usd)
        impact_cost_10k_bps = self._impact_estimate(snapshot_a, snapshot_b, 10_000.0)
        impact_cost_50k_bps = self._impact_estimate(snapshot_a, snapshot_b, 50_000.0)
        now = self._snapshot_time(snapshot_a, snapshot_b)
        executable_entry = self._entry_edge_bps(leg_a, leg_b)
        bucket_hint = None
        holding_window_hours = self.config.dislocation_holding_hours
        signed_daily_funding_edge = self._directional_daily_funding_edge_bps(leg_a, leg_b)
        signed_carrying_edge = signed_daily_funding_edge * (self.config.carry_holding_hours / 24.0)
        # Carry bucket only when funding is positive for the executable direction.
        # Otherwise a large entry spread can be mislabeled as carry even when the
        # selected hedge direction pays funding.
        if (
            signed_carrying_edge > abs(executable_entry) * self.config.funding_dominance_ratio
            or (
                signed_daily_funding_edge >= self.config.carry_funding_diff_min_bps
                and abs(executable_entry) <= self.config.carry_entry_edge_max_bps
            )
        ):
            bucket_hint = self._carry_bucket()
            holding_window_hours = self.config.carry_holding_hours

        return PerpArbOpportunity(
            asset=asset,
            quote=quote,
            leg_a=leg_a,
            leg_b=leg_b,
            notional_usd=notional_usd,
            capital_used_usd=capital_used_usd,
            slippage_bps=slippage_bps,
            impact_cost_10k_bps=impact_cost_10k_bps,
            impact_cost_50k_bps=impact_cost_50k_bps,
            realized_vol=max(snapshot_a.realized_vol, snapshot_b.realized_vol),
            jump_frequency=max(snapshot_a.jump_frequency, snapshot_b.jump_frequency),
            micro_jump_frequency=max(snapshot_a.micro_jump_frequency, snapshot_b.micro_jump_frequency),
            shock_jump_frequency=max(snapshot_a.shock_jump_frequency, snapshot_b.shock_jump_frequency),
            spread_zscore=max(abs(snapshot_a.spread_zscore), abs(snapshot_b.spread_zscore)),
            trend_vs_mean_reversion=(snapshot_a.trend_vs_mean_reversion + snapshot_b.trend_vs_mean_reversion) / 2.0,
            latency_ms=max(snapshot_a.latency_ms, snapshot_b.latency_ms),
            staleness_ms=max(snapshot_a.staleness_ms, snapshot_b.staleness_ms),
            holding_window_hours=holding_window_hours,
            funding_interval_hours=self.config.funding_interval_hours,
            funding_persistence_score=self._funding_persistence(snapshot_a, snapshot_b),
            spread_widening_speed_bps_per_min=abs(snapshot_a.spread_zscore - snapshot_b.spread_zscore),
            bucket_hint=bucket_hint,
            now=now,
            metadata={
                "pair": f"{snapshot_a.venue}:{snapshot_b.venue}",
                "leg_a_source": snapshot_a.metadata.get("source", snapshot_a.venue),
                "leg_b_source": snapshot_b.metadata.get("source", snapshot_b.venue),
                "leg_a_ticker_only": bool(snapshot_a.metadata.get("ticker_only")),
                "leg_b_ticker_only": bool(snapshot_b.metadata.get("ticker_only")),
                "source_timestamps": [
                    snapshot_a.timestamp.isoformat() if snapshot_a.timestamp else None,
                    snapshot_b.timestamp.isoformat() if snapshot_b.timestamp else None,
                ],
            },
        )

    def _build_leg(self, snapshot: MarketSnapshot) -> PerpLegSnapshot:
        return PerpLegSnapshot(
            venue=snapshot.venue,
            market_type=snapshot.market_type,
            asset=snapshot.asset,
            best_bid=snapshot.best_bid,
            best_ask=snapshot.best_ask,
            mark_price=snapshot.mark_price,
            oracle_price=snapshot.oracle_price,
            index_price=snapshot.index_price,
            taker_fee_bps=snapshot.taker_fee_bps,
            maker_fee_bps=snapshot.maker_fee_bps,
            depth_10k_usd=snapshot.depth_10k_usd,
            depth_50k_usd=snapshot.depth_50k_usd,
            top_1pct_depth_usd=snapshot.top_1pct_depth_usd,
            volume_depth_ratio=snapshot.volume_depth_ratio,
            volume_24h_usd=self._volume_24h_usd(snapshot),
            oi_usd=snapshot.oi_usd,
            oi_change_pct=snapshot.oi_change_pct,
            funding_rate_bps=snapshot.funding_rate_bps,
            funding_change_bps=snapshot.funding_change_bps,
            funding_interval_hours=snapshot.metadata.get("funding_interval_hours", 8.0),
            next_funding_time=snapshot.next_funding_time,
        )

    @staticmethod
    def _volume_24h_usd(snapshot: MarketSnapshot) -> float:
        for key in (
            "day_quote_volume_usd",
            "turnover_24h_usd",
            "day_ntl_volume_usd",
            "day_volume_usd",
            "volume_24h_usd",
        ):
            value = snapshot.metadata.get(key)
            if value is not None:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    continue
        if snapshot.volume_depth_ratio > 0 and snapshot.top_1pct_depth_usd > 0:
            return snapshot.volume_depth_ratio * snapshot.top_1pct_depth_usd
        return 0.0

    def _impact_estimate(self, snapshot_a: MarketSnapshot, snapshot_b: MarketSnapshot, notional_usd: float) -> float:
        estimates = []
        for snapshot in (snapshot_a, snapshot_b):
            if notional_usd <= 10_000.0 and snapshot.impact_cost_10k_bps is not None:
                estimates.append(snapshot.impact_cost_10k_bps)
                continue
            if notional_usd >= 50_000.0 and snapshot.impact_cost_50k_bps is not None:
                estimates.append(snapshot.impact_cost_50k_bps)
                continue
            reference_depth = snapshot.depth_10k_usd if notional_usd <= 10_000.0 else snapshot.depth_50k_usd
            pressure = notional_usd / max(reference_depth, 1.0)
            estimates.append(clamp(pressure * 4.0, 0.5, 30.0))
        return sum(estimates)

    def _slippage_estimate(self, snapshot_a: MarketSnapshot, snapshot_b: MarketSnapshot, notional_usd: float) -> float:
        estimates = []
        for snapshot in (snapshot_a, snapshot_b):
            if snapshot.slippage_bps is not None:
                estimates.append(snapshot.slippage_bps)
                continue
            depth_ratio = notional_usd / max(snapshot.top_1pct_depth_usd, 1.0)
            estimates.append(clamp(depth_ratio * 8.0, 0.2, 20.0))
        return sum(estimates)

    def _effective_notional(self, snapshot_a: MarketSnapshot, snapshot_b: MarketSnapshot) -> float:
        """Scale notional down for shallow-depth venues (DEX).

        Cap at 50% of the shallower leg's depth_10k to avoid excessive
        market impact on thin orderbooks.
        """
        default = self.config.default_notional_usd
        is_dex = "dex" in snapshot_a.market_type or "dex" in snapshot_b.market_type
        if not is_dex:
            return default
        min_depth = min(snapshot_a.depth_10k_usd, snapshot_b.depth_10k_usd)
        depth_cap = min_depth * 0.5
        return clamp(depth_cap, 1_000.0, default)

    def _funding_persistence(self, snapshot_a: MarketSnapshot, snapshot_b: MarketSnapshot) -> float:
        base = self.config.funding_persistence_default
        avg_change = (abs(snapshot_a.funding_change_bps) + abs(snapshot_b.funding_change_bps)) / 2.0
        return clamp(base - avg_change / 20.0, 0.0, 1.0)

    def _funding_diff_bps(self, leg_a: PerpLegSnapshot, leg_b: PerpLegSnapshot) -> float:
        """Daily funding difference in bps, normalized by each leg's interval."""
        daily_a = leg_a.funding_rate_bps * (24.0 / max(leg_a.funding_interval_hours, 1e-9))
        daily_b = leg_b.funding_rate_bps * (24.0 / max(leg_b.funding_interval_hours, 1e-9))
        return abs(daily_a - daily_b)

    def _directional_daily_funding_edge_bps(self, leg_a: PerpLegSnapshot, leg_b: PerpLegSnapshot) -> float:
        """Daily funding edge for the same long/short direction as entry execution."""
        a_long_b_short = (leg_b.best_bid - leg_a.best_ask) / max(leg_a.mid_price, 1e-9) * 10_000
        b_long_a_short = (leg_a.best_bid - leg_b.best_ask) / max(leg_b.mid_price, 1e-9) * 10_000
        if a_long_b_short >= b_long_a_short:
            long_leg, short_leg = leg_a, leg_b
        else:
            long_leg, short_leg = leg_b, leg_a
        long_daily = long_leg.funding_rate_bps * (24.0 / max(long_leg.funding_interval_hours, 1e-9))
        short_daily = short_leg.funding_rate_bps * (24.0 / max(short_leg.funding_interval_hours, 1e-9))
        return -long_daily + short_daily

    def _entry_edge_bps(self, leg_a: PerpLegSnapshot, leg_b: PerpLegSnapshot) -> float:
        a_long_b_short = (leg_b.best_bid - leg_a.best_ask) / max(leg_a.mid_price, 1e-9) * 10_000
        b_long_a_short = (leg_a.best_bid - leg_b.best_ask) / max(leg_b.mid_price, 1e-9) * 10_000
        return max(a_long_b_short, b_long_a_short)

    def _snapshot_time(self, snapshot_a: MarketSnapshot, snapshot_b: MarketSnapshot) -> Optional[datetime]:
        candidates = [snapshot.timestamp for snapshot in (snapshot_a, snapshot_b) if snapshot.timestamp is not None]
        if not candidates:
            return None
        return max(candidates)

    def _carry_bucket(self):
        from .models import OpportunityBucket

        return OpportunityBucket.CARRY


class MockMarketDataAdapter(MarketDataAdapter):
    def __init__(
        self,
        name: str,
        venue: str,
        assets: Sequence[str] | None = None,
        seed: int = 0,
    ) -> None:
        self.name = name
        self.venue = venue
        self.assets = list(assets or ("BTC", "ETH", "SOL"))
        self._rng = random.Random(seed)
        self._tick = 0

    def poll(self) -> Sequence[MarketSnapshot]:
        self._tick += 1
        snapshots: List[MarketSnapshot] = []
        now = datetime.utcnow()
        venue_bias = (sum(ord(char) for char in self.venue) % 7 - 3) * 0.0008
        venue_funding_bias = (sum(ord(char) for char in self.venue) % 5 - 2) * 1.2
        for asset_index, asset in enumerate(self.assets):
            base_price = {"BTC": 68_000.0, "ETH": 3_400.0, "SOL": 180.0}.get(asset, 100.0)
            wave = math.sin((self._tick + asset_index) / 2.5) * base_price * 0.0025
            noise = self._rng.uniform(-0.0006, 0.0006) * base_price
            mid = base_price * (1.0 + venue_bias) + wave + noise
            spread = max(base_price * 0.0006, 0.05)
            mark = mid * (1.0 + math.sin((self._tick + asset_index) / 3.0) * 0.0004)
            oracle = mid * (1.0 + math.cos((self._tick + asset_index) / 4.2) * 0.0002)
            index = mid * (1.0 + math.sin((self._tick + asset_index) / 5.5) * 0.00015)
            depth_multiplier = 1.0 + (asset_index * 0.25)
            top_depth = 250_000.0 * depth_multiplier * (1.0 + self._rng.uniform(-0.1, 0.1))
            depth_50k = top_depth * 0.8
            depth_10k = top_depth * 0.35
            funding = venue_funding_bias + math.sin((self._tick + asset_index) / 4.5) * 5.0
            snapshots.append(
                MarketSnapshot(
                    venue=self.venue,
                    market_type="perp_dex",
                    asset=asset,
                    quote="USD",
                    best_bid=mid - spread / 2.0,
                    best_ask=mid + spread / 2.0,
                    mark_price=mark,
                    oracle_price=oracle,
                    index_price=index,
                    taker_fee_bps=2.0 + self._rng.uniform(-0.25, 0.25),
                    maker_fee_bps=0.0,
                    depth_10k_usd=depth_10k,
                    depth_50k_usd=depth_50k,
                    top_1pct_depth_usd=top_depth,
                    volume_depth_ratio=4.0 + self._rng.uniform(-1.0, 4.0),
                    oi_usd=120_000_000.0 * depth_multiplier,
                    oi_change_pct=self._rng.uniform(-3.0, 3.0),
                    funding_rate_bps=funding,
                    funding_change_bps=self._rng.uniform(-2.0, 2.0),
                    next_funding_time=now + timedelta(hours=8 - (self._tick % 8)),
                    impact_cost_10k_bps=max(0.5, 1.2 + self._rng.uniform(-0.4, 0.7)),
                    impact_cost_50k_bps=max(2.5, 4.5 + self._rng.uniform(-1.0, 3.0)),
                    slippage_bps=max(0.4, 0.9 + self._rng.uniform(-0.3, 1.0)),
                    realized_vol=20.0 + self._rng.uniform(-5.0, 15.0),
                    jump_frequency=max(0.5, 2.0 + self._rng.uniform(-1.0, 6.0)),
                    spread_zscore=abs(self._rng.gauss(1.0, 0.6)),
                    trend_vs_mean_reversion=self._rng.uniform(-0.7, 0.7),
                    latency_ms=70.0 + self._rng.uniform(0.0, 120.0),
                    staleness_ms=90.0 + self._rng.uniform(0.0, 180.0),
                    timestamp=now,
                )
            )
        return snapshots


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))
