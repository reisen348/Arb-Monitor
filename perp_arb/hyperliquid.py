from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib import request

from .market_data import MarketDataAdapter, MarketSnapshot


@dataclass(frozen=True)
class HyperliquidAdapterConfig:
    base_url: str = "https://api.hyperliquid.xyz"
    dex: str = ""
    extra_dexes: Sequence[str] = ("xyz",)
    assets: Optional[Sequence[str]] = None
    timeout_seconds: float = 5.0
    top_book_markets: int = 10  # Only fetch l2Book for top N markets by volume


class HyperliquidApiError(RuntimeError):
    pass


class HyperliquidInfoClient:
    def __init__(self, config: HyperliquidAdapterConfig | None = None) -> None:
        self.config = config or HyperliquidAdapterConfig()

    def post_info(self, payload: dict) -> Tuple[object, float]:
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            f"{self.config.base_url.rstrip('/')}/info",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        started = time.perf_counter()
        with request.urlopen(req, timeout=self.config.timeout_seconds) as response:
            raw = response.read()
        latency_ms = (time.perf_counter() - started) * 1000.0
        return json.loads(raw.decode("utf-8")), latency_ms


class HyperliquidAdapter(MarketDataAdapter):
    def __init__(
        self,
        config: HyperliquidAdapterConfig | None = None,
        client: HyperliquidInfoClient | None = None,
    ) -> None:
        self.config = config or HyperliquidAdapterConfig()
        self.client = client or HyperliquidInfoClient(self.config)
        self.name = "hyperliquid"

    def poll(self) -> Sequence[MarketSnapshot]:
        snapshots: List[MarketSnapshot] = []
        for dex in self._dexes_to_poll():
            snapshots.extend(self._poll_dex(dex))
        return snapshots

    def _poll_dex(self, dex: str) -> Sequence[MarketSnapshot]:
        payload = {"type": "metaAndAssetCtxs"}
        if dex:
            payload["dex"] = dex
        response, meta_latency_ms = self.client.post_info(payload)
        asset_contexts = self._parse_meta_and_asset_ctxs(response)
        selected_assets = self._selected_assets(asset_contexts.keys())

        # Only fetch l2Book for top N assets by 24h volume
        top_assets = self._rank_top_markets(selected_assets, asset_contexts)

        snapshots: List[MarketSnapshot] = []
        for asset in selected_assets:
            ctx = asset_contexts[asset]
            try:
                if asset in top_assets:
                    book_payload = {"type": "l2Book", "coin": asset}
                    if dex:
                        book_payload["dex"] = dex
                    book, book_latency_ms = self.client.post_info(book_payload)
                    snapshots.append(self._build_snapshot(asset, ctx, book, max(meta_latency_ms, book_latency_ms)))
                else:
                    snapshots.append(self._build_snapshot_from_ctx(asset, ctx, meta_latency_ms))
            except Exception:
                continue
        return snapshots

    def _dexes_to_poll(self) -> List[str]:
        if self.config.dex:
            return [self.config.dex]
        seen = {""}
        dexes = [""]
        for dex in self.config.extra_dexes:
            normalized = str(dex or "").strip()
            if normalized and normalized not in seen:
                dexes.append(normalized)
                seen.add(normalized)
        return dexes

    def _rank_top_markets(self, assets: List[str], contexts: Dict[str, dict]) -> set:
        """Return top N by volume ∪ top N by OI (deduplicated)."""
        n = self.config.top_book_markets
        vol_list = sorted(assets, key=lambda a: self._coerce_float(contexts[a].get("dayNtlVlm")) or 0.0, reverse=True)
        oi_list = sorted(assets, key=lambda a: self._coerce_float(contexts[a].get("openInterest")) or 0.0, reverse=True)
        return set(vol_list[:n]) | set(oi_list[:n])

    def _selected_assets(self, asset_names: Iterable[str]) -> List[str]:
        names = sorted(asset_names)
        if self.config.assets:
            asset_set = {asset.upper() for asset in self.config.assets}
            return [
                asset for asset in names
                if asset.upper() in asset_set or self._display_asset(asset).upper() in asset_set
            ]
        return names

    @staticmethod
    def _parse_meta_and_asset_ctxs(response: object) -> Dict[str, dict]:
        if not isinstance(response, list) or len(response) != 2:
            raise HyperliquidApiError("Unexpected metaAndAssetCtxs response shape")
        meta, asset_ctxs = response
        universe = meta.get("universe") if isinstance(meta, dict) else None
        if not isinstance(universe, list) or not isinstance(asset_ctxs, list):
            raise HyperliquidApiError("metaAndAssetCtxs response is missing universe or assetCtxs")

        parsed: Dict[str, dict] = {}
        for coin_meta, ctx in zip(universe, asset_ctxs):
            if not isinstance(coin_meta, dict) or not isinstance(ctx, dict):
                continue
            coin = coin_meta.get("name")
            if not coin or coin_meta.get("isDelisted"):
                continue
            parsed[str(coin)] = ctx
        return parsed

    def _build_snapshot(self, asset: str, ctx: dict, book: object, latency_ms: float) -> MarketSnapshot:
        bids, asks, book_time_ms = self._parse_l2_book(book)
        best_bid = bids[0][0] if bids else self._coerce_float(ctx.get("midPx")) or self._coerce_float(ctx.get("markPx"))
        best_ask = asks[0][0] if asks else self._coerce_float(ctx.get("midPx")) or self._coerce_float(ctx.get("markPx"))
        if best_bid is None or best_ask is None:
            raise HyperliquidApiError(f"Hyperliquid book for {asset} has no usable prices")

        mid_px = self._coerce_float(ctx.get("midPx")) or (best_bid + best_ask) / 2.0
        mark_px = self._coerce_float(ctx.get("markPx")) or mid_px
        oracle_px = self._coerce_float(ctx.get("oraclePx")) or mark_px
        index_px = oracle_px
        prev_day_px = self._coerce_float(ctx.get("prevDayPx")) or mark_px
        day_ntl_vlm = self._coerce_float(ctx.get("dayNtlVlm")) or 0.0
        open_interest = self._coerce_float(ctx.get("openInterest")) or 0.0
        funding_rate_bps = (self._coerce_float(ctx.get("funding")) or 0.0) * 10_000.0
        premium_bps = abs((self._coerce_float(ctx.get("premium")) or 0.0) * 10_000.0)
        staleness_ms = 0.0
        timestamp = datetime.now(timezone.utc)
        if book_time_ms is not None:
            staleness_ms = max(0.0, timestamp.timestamp() * 1000.0 - book_time_ms)
            timestamp = datetime.fromtimestamp(book_time_ms / 1000.0, tz=timezone.utc)

        symmetric_depth = min(
            self._book_notional(bids),
            self._book_notional(asks),
        )
        top_1pct_depth = min(
            self._book_notional_within_band(bids, mid_px, side="bid", pct=0.01),
            self._book_notional_within_band(asks, mid_px, side="ask", pct=0.01),
        )
        impact_10k = self._estimate_roundtrip_impact_bps(bids, asks, mid_px, 10_000.0)
        impact_50k = self._estimate_roundtrip_impact_bps(bids, asks, mid_px, 50_000.0)
        realized_vol = abs(math.log(mark_px / prev_day_px)) * 100.0 if prev_day_px > 0 else 0.0
        volume_depth_ratio = day_ntl_vlm / max(top_1pct_depth, 1.0)

        return MarketSnapshot(
            venue=self._venue(asset),
            market_type="perp_dex",
            asset=self._display_asset(asset),
            quote="USD",
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_px,
            oracle_price=oracle_px,
            index_price=index_px,
            taker_fee_bps=4.5,
            maker_fee_bps=1.5,
            depth_10k_usd=min(10_000.0, symmetric_depth),
            depth_50k_usd=min(50_000.0, symmetric_depth),
            top_1pct_depth_usd=top_1pct_depth,
            volume_depth_ratio=volume_depth_ratio,
            oi_usd=open_interest * mark_px,
            oi_change_pct=0.0,
            funding_rate_bps=funding_rate_bps,
            funding_change_bps=0.0,
            next_funding_time=None,
            impact_cost_10k_bps=impact_10k,
            impact_cost_50k_bps=impact_50k,
            slippage_bps=max(0.25, impact_10k * 0.5),
            realized_vol=realized_vol,
            jump_frequency=0.0,
            spread_zscore=premium_bps / 10.0,
            trend_vs_mean_reversion=0.0,
            latency_ms=latency_ms,
            staleness_ms=staleness_ms,
            timestamp=timestamp,
            metadata=self._metadata(asset, {
                "source": "hyperliquid",
                "premium_bps": premium_bps,
                "day_ntl_volume_usd": day_ntl_vlm,
                "funding_interval_hours": 1.0,
                "_book_bids": bids,
                "_book_asks": asks,
            }),
        )

    def _build_snapshot_from_ctx(self, asset: str, ctx: dict, latency_ms: float) -> MarketSnapshot:
        """Build snapshot using only metaAndAssetCtxs data (no l2Book request)."""
        mid_px = self._coerce_float(ctx.get("midPx"))
        mark_px = self._coerce_float(ctx.get("markPx")) or mid_px or 0.0
        oracle_px = self._coerce_float(ctx.get("oraclePx")) or mark_px
        prev_day_px = self._coerce_float(ctx.get("prevDayPx")) or mark_px
        day_ntl_vlm = self._coerce_float(ctx.get("dayNtlVlm")) or 0.0
        open_interest = self._coerce_float(ctx.get("openInterest")) or 0.0
        funding_rate_bps = (self._coerce_float(ctx.get("funding")) or 0.0) * 10_000.0
        premium_bps = abs((self._coerce_float(ctx.get("premium")) or 0.0) * 10_000.0)
        realized_vol = abs(math.log(mark_px / prev_day_px)) * 100.0 if prev_day_px > 0 else 0.0

        # Use impactPxs for bid/ask estimate if available
        impact_pxs = ctx.get("impactPxs")
        if isinstance(impact_pxs, list) and len(impact_pxs) >= 2:
            best_bid = self._coerce_float(impact_pxs[0]) or mark_px
            best_ask = self._coerce_float(impact_pxs[1]) or mark_px
        elif mid_px:
            best_bid = mid_px
            best_ask = mid_px
        else:
            best_bid = mark_px
            best_ask = mark_px

        return MarketSnapshot(
            venue=self._venue(asset),
            market_type="perp_dex",
            asset=self._display_asset(asset),
            quote="USD",
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_px,
            oracle_price=oracle_px,
            index_price=oracle_px,
            taker_fee_bps=4.5,
            maker_fee_bps=1.5,
            depth_10k_usd=0.0,
            depth_50k_usd=0.0,
            top_1pct_depth_usd=0.0,
            volume_depth_ratio=0.0,
            oi_usd=open_interest * mark_px,
            oi_change_pct=0.0,
            funding_rate_bps=funding_rate_bps,
            funding_change_bps=0.0,
            next_funding_time=None,
            impact_cost_10k_bps=50.0,
            impact_cost_50k_bps=50.0,
            slippage_bps=25.0,
            realized_vol=realized_vol,
            jump_frequency=0.0,
            spread_zscore=premium_bps / 10.0,
            trend_vs_mean_reversion=0.0,
            latency_ms=latency_ms,
            staleness_ms=0.0,
            timestamp=datetime.now(timezone.utc),
            metadata=self._metadata(asset, {
                "source": "hyperliquid",
                "premium_bps": premium_bps,
                "day_ntl_volume_usd": day_ntl_vlm,
                "funding_interval_hours": 1.0,
                "ticker_only": True,
            }),
        )

    def _venue(self, asset: str) -> str:
        dex = self._asset_dex(asset) or self.config.dex
        return "hyperliquid" if not dex else f"hyperliquid:{dex}"

    @staticmethod
    def _asset_dex(asset: str) -> str:
        if ":" not in asset:
            return ""
        return asset.split(":", 1)[0].lower()

    @staticmethod
    def _display_asset(asset: str) -> str:
        return asset.split(":", 1)[1].upper() if ":" in asset else asset.upper()

    def _metadata(self, asset: str, values: dict) -> dict:
        metadata = dict(values)
        metadata["symbol"] = asset
        dex = self._asset_dex(asset)
        if dex:
            metadata["perp_dex"] = dex
        if dex == "xyz":
            metadata["stock_like"] = True
        return metadata

    @staticmethod
    def _parse_l2_book(book: object) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]], Optional[float]]:
        if not isinstance(book, dict):
            raise HyperliquidApiError("Unexpected l2Book response shape")
        levels = book.get("levels")
        if not isinstance(levels, list) or len(levels) != 2:
            raise HyperliquidApiError("l2Book response is missing levels")
        bids = [HyperliquidAdapter._parse_level(level) for level in levels[0]]
        asks = [HyperliquidAdapter._parse_level(level) for level in levels[1]]
        timestamp = book.get("time")
        return bids, asks, float(timestamp) if timestamp is not None else None

    @staticmethod
    def _parse_level(level: dict) -> Tuple[float, float]:
        return float(level["px"]), float(level["sz"])

    @staticmethod
    def _book_notional(levels: Sequence[Tuple[float, float]]) -> float:
        return sum(price * size for price, size in levels)

    @staticmethod
    def _book_notional_within_band(
        levels: Sequence[Tuple[float, float]],
        mid_price: float,
        side: str,
        pct: float,
    ) -> float:
        if side == "bid":
            eligible = [price * size for price, size in levels if price >= mid_price * (1.0 - pct)]
        else:
            eligible = [price * size for price, size in levels if price <= mid_price * (1.0 + pct)]
        return sum(eligible)

    @staticmethod
    def _estimate_roundtrip_impact_bps(
        bids: Sequence[Tuple[float, float]],
        asks: Sequence[Tuple[float, float]],
        mid_price: float,
        notional_usd: float,
    ) -> float:
        buy_impact = HyperliquidAdapter._walk_impact_bps(asks, mid_price, notional_usd, is_buy=True)
        sell_impact = HyperliquidAdapter._walk_impact_bps(bids, mid_price, notional_usd, is_buy=False)
        return buy_impact + sell_impact

    @staticmethod
    def _walk_impact_bps(
        levels: Sequence[Tuple[float, float]],
        reference_price: float,
        target_notional_usd: float,
        is_buy: bool,
    ) -> float:
        filled_notional = 0.0
        weighted_price = 0.0
        for price, size in levels:
            level_notional = price * size
            if level_notional <= 0:
                continue
            take_notional = min(level_notional, target_notional_usd - filled_notional)
            weighted_price += price * take_notional
            filled_notional += take_notional
            if filled_notional >= target_notional_usd:
                break
        if filled_notional <= 0:
            return 50.0
        avg_price = weighted_price / filled_notional
        if is_buy:
            impact = (avg_price - reference_price) / max(reference_price, 1e-9)
        else:
            impact = (reference_price - avg_price) / max(reference_price, 1e-9)
        shortage_penalty = 0.0 if filled_notional >= target_notional_usd else 15.0
        return max(0.0, impact * 10_000.0) + shortage_penalty

    @staticmethod
    def _coerce_float(value: object) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
