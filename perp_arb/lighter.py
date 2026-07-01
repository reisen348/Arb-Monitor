"""Lighter (zklighter) perpetual DEX REST adapter.

Public endpoints (no auth required):
  GET /api/v1/orderBooks?filter=perp            → market list + fees
  GET /api/v1/orderBookDetails?filter=perp      → OI, last price, volume
  GET /api/v1/orderBookOrders?market_id=X&limit=20  → orderbook
  GET /api/v1/funding-rates                     → current funding rates

Note: mark_price and index_price are only available via WebSocket.
REST adapter uses last_trade_price as a proxy.
"""
from __future__ import annotations

import json
import logging
import math
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple
from urllib import request

from .market_data import MarketDataAdapter, MarketSnapshot


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LighterAdapterConfig:
    base_url: str = "https://mainnet.zklighter.elliot.ai"
    assets: Optional[Sequence[str]] = None
    timeout_seconds: float = 5.0
    book_depth: int = 20
    top_book_markets: int = 10  # Only fetch orderbooks for top N markets by volume
    book_request_workers: int = 4
    min_request_interval_seconds: float = 0.5
    rate_limit_retries: int = 4
    rate_limit_backoff_seconds: float = 1.0
    book_cooldown_seconds: float = 30.0
    book_cache_ttl_seconds: float = 30.0
    book_stale_ttl_seconds: float = 300.0
    background_book_refresh_per_poll: int = 5


class LighterApiError(RuntimeError):
    pass


class LighterRateLimitError(LighterApiError):
    pass


class LighterClient:
    def __init__(self, config: LighterAdapterConfig | None = None) -> None:
        self.config = config or LighterAdapterConfig()
        self._request_lock = threading.Lock()
        self._last_request_at = 0.0

    def get(self, path: str) -> Tuple[object, float]:
        url = f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"
        req = request.Request(url, method="GET", headers={
            "Accept": "application/json",
        })
        started = time.perf_counter()
        backoff = self.config.rate_limit_backoff_seconds
        for attempt in range(self.config.rate_limit_retries + 1):
            self._wait_for_request_slot()
            try:
                with request.urlopen(req, timeout=self.config.timeout_seconds) as response:
                    raw = response.read()
                latency_ms = (time.perf_counter() - started) * 1000.0
                return json.loads(raw.decode("utf-8")), latency_ms
            except Exception as exc:
                if getattr(exc, "code", None) != 429:
                    raise
                if attempt >= self.config.rate_limit_retries:
                    raise LighterRateLimitError("Lighter API rate limited") from exc
                import logging as _log
                _log.getLogger(__name__).warning("lighter 429 rate-limit, retry %d backoff=%.1fs", attempt + 1, backoff)
                time.sleep(backoff)
                backoff *= 2.0
        raise RuntimeError("unreachable")

    def _wait_for_request_slot(self) -> None:
        with self._request_lock:
            now = time.monotonic()
            interval = self.config.min_request_interval_seconds
            wait = max(0.0, self._last_request_at + interval - now)
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()
            self._last_request_at = now


class LighterAdapter(MarketDataAdapter):
    def __init__(
        self,
        config: LighterAdapterConfig | None = None,
        client: LighterClient | None = None,
    ) -> None:
        self.config = config or LighterAdapterConfig()
        self.client = client or LighterClient(self.config)
        self.name = "lighter"
        self._book_executor = ThreadPoolExecutor(
            max_workers=max(1, self.config.book_request_workers),
            thread_name_prefix="lighter-book",
        )
        self._book_cooldown_until = 0.0
        self._book_cooldown_lock = threading.Lock()
        self._book_cache: Dict[int, Tuple[object, float, float]] = {}
        self._book_cache_lock = threading.Lock()
        self._book_refresh_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="lighter-book-refresh")
        self._book_refresh_lock = threading.Lock()
        self._book_refresh_future: Future | None = None
        self._book_sweep_lock = threading.Lock()
        self._book_sweep_index = 0

    def poll(self) -> Sequence[MarketSnapshot]:
        # 1: orderBookDetails contains market info + volume/OI (replaces orderBooks)
        details_resp, details_latency = self.client.get("api/v1/orderBookDetails?filter=perp")
        details = self._parse_details(details_resp)
        markets = self._parse_markets_from_details(details)

        # 2: Funding rates
        funding_resp, funding_latency = self.client.get("api/v1/funding-rates")
        funding_map = self._parse_funding(funding_resp)

        batch_latency = max(details_latency, funding_latency)
        selected = self._select_markets(markets)

        # 3: Rank by volume ∪ OI, only fetch orderbooks for top N
        top_ids = self._rank_top_markets(selected, details)
        book_responses: Dict[int, Tuple[object, float]] = {}
        if self._book_requests_allowed():
            try:
                book_responses = self._fetch_books_with_cache(top_ids, all_market_ids=selected.keys())
            except LighterRateLimitError:
                self._enter_book_cooldown()
                book_responses = self._cached_books(selected.keys(), allow_stale=True)
        else:
            book_responses = self._cached_books(selected.keys(), allow_stale=True)

        snapshots: List[MarketSnapshot] = []
        for market_id, market_info in selected.items():
            detail = details.get(market_id, {})
            funding_rate = funding_map.get(market_id, 0.0)
            try:
                book_data = book_responses.get(market_id)
                if book_data is not None:
                    book_resp, book_latency = book_data
                    snapshot = self._build_snapshot(
                        market_info, detail, book_resp, funding_rate,
                        max(batch_latency, book_latency),
                    )
                else:
                    snapshot = self._build_snapshot_from_ticker(
                        market_info, detail, funding_rate, batch_latency,
                    )
                snapshots.append(snapshot)
            except Exception:
                continue
        return snapshots

    def _fetch_books_with_cache(
        self,
        market_ids: Sequence[int],
        all_market_ids: Sequence[int] | None = None,
    ) -> Dict[int, Tuple[object, float]]:
        if not market_ids:
            market_ids = []
        all_ids = tuple(all_market_ids if all_market_ids is not None else market_ids)
        cached, _ = self._cached_books_with_refresh_ids(all_ids, allow_stale=True, collect_refresh=False)
        _, refresh_ids = self._cached_books_with_refresh_ids(market_ids, allow_stale=True, collect_refresh=True)
        missing = [market_id for market_id in market_ids if market_id not in cached]
        if missing:
            fetched = self._fetch_books(missing)
            if fetched:
                self._store_cached_books(fetched)
                cached.update(fetched)
        refresh_ids.update(self._next_background_book_refresh_ids(all_ids, set(market_ids)))
        if refresh_ids:
            self._request_background_book_refresh(refresh_ids)
        return cached

    def _fetch_books(self, market_ids: Sequence[int]) -> Dict[int, Tuple[object, float]]:
        if not market_ids:
            return {}
        results: Dict[int, Tuple[object, float]] = {}
        futures = {
            self._book_executor.submit(
                self.client.get,
                f"api/v1/orderBookOrders?market_id={market_id}&limit={self.config.book_depth}",
            ): market_id
            for market_id in market_ids
        }
        for future in as_completed(futures):
            market_id = futures[future]
            try:
                results[market_id] = future.result()
            except LighterRateLimitError:
                raise
            except Exception:
                continue
        return results

    def _cached_books(self, market_ids: Sequence[int], allow_stale: bool = False) -> Dict[int, Tuple[object, float]]:
        cached, _ = self._cached_books_with_refresh_ids(market_ids, allow_stale=allow_stale)
        return cached

    def _cached_books_with_refresh_ids(
        self,
        market_ids: Sequence[int],
        allow_stale: bool = False,
        collect_refresh: bool = True,
    ) -> Tuple[Dict[int, Tuple[object, float]], set[int]]:
        ttl = self.config.book_cache_ttl_seconds
        if ttl <= 0:
            return {}, set()
        stale_ttl = max(ttl, self.config.book_stale_ttl_seconds)
        now = time.monotonic()
        results: Dict[int, Tuple[object, float]] = {}
        refresh_ids: set[int] = set()
        with self._book_cache_lock:
            for market_id in market_ids:
                cached = self._book_cache.get(market_id)
                if cached is None:
                    continue
                book_resp, latency_ms, cached_at = cached
                age = now - cached_at
                if age <= ttl:
                    results[market_id] = (book_resp, latency_ms)
                    continue
                if allow_stale and age <= stale_ttl:
                    results[market_id] = (book_resp, latency_ms)
                    if collect_refresh:
                        refresh_ids.add(market_id)
                    continue
                if age > stale_ttl:
                    self._book_cache.pop(market_id, None)
                    continue
        return results, refresh_ids

    def _next_background_book_refresh_ids(self, all_market_ids: Sequence[int], top_ids: set[int]) -> set[int]:
        limit = max(0, int(self.config.background_book_refresh_per_poll))
        if limit <= 0:
            return set()
        candidates = sorted(set(all_market_ids) - set(top_ids))
        if not candidates:
            return set()
        chosen: set[int] = set()
        with self._book_sweep_lock:
            start = self._book_sweep_index % len(candidates)
            examined = 0
            while len(chosen) < limit and examined < len(candidates):
                market_id = candidates[(start + examined) % len(candidates)]
                examined += 1
                if self._book_cache_is_fresh(market_id):
                    continue
                chosen.add(market_id)
            self._book_sweep_index = (start + examined) % len(candidates)
        return chosen

    def _book_cache_is_fresh(self, market_id: int) -> bool:
        ttl = self.config.book_cache_ttl_seconds
        if ttl <= 0:
            return False
        now = time.monotonic()
        with self._book_cache_lock:
            cached = self._book_cache.get(market_id)
            if cached is None:
                return False
            return now - cached[2] <= ttl

    def _store_cached_books(self, book_responses: Dict[int, Tuple[object, float]]) -> None:
        now = time.monotonic()
        with self._book_cache_lock:
            for market_id, (book_resp, latency_ms) in book_responses.items():
                self._book_cache[market_id] = (book_resp, latency_ms, now)

    def _request_background_book_refresh(self, market_ids: Sequence[int]) -> None:
        if not market_ids or not self._book_requests_allowed():
            return
        with self._book_refresh_lock:
            if self._book_refresh_future is not None and not self._book_refresh_future.done():
                return
            self._book_refresh_future = self._book_refresh_executor.submit(
                self._refresh_books_in_background,
                tuple(dict.fromkeys(market_ids)),
            )

    def _refresh_books_in_background(self, market_ids: Sequence[int]) -> None:
        try:
            fetched = self._fetch_books(market_ids)
            if fetched:
                self._store_cached_books(fetched)
        except LighterRateLimitError:
            self._enter_book_cooldown()
        except Exception:
            logger.debug("lighter background orderbook refresh failed", exc_info=True)

    def _book_requests_allowed(self) -> bool:
        with self._book_cooldown_lock:
            return time.monotonic() >= self._book_cooldown_until

    def _enter_book_cooldown(self) -> None:
        with self._book_cooldown_lock:
            self._book_cooldown_until = time.monotonic() + self.config.book_cooldown_seconds
        logger.warning("lighter orderbook requests cooling down for %.0fs; falling back to ticker-only", self.config.book_cooldown_seconds)

    def stop(self) -> None:
        self._book_refresh_executor.shutdown(wait=False, cancel_futures=False)
        self._book_executor.shutdown(wait=False, cancel_futures=False)

    def _rank_top_markets(self, markets: Dict[int, dict], details: Dict[int, dict]) -> set:
        """Return top N by volume ∪ top N by OI (deduplicated)."""
        n = self.config.top_book_markets
        mids = list(markets.keys())
        vol_list = sorted(mids, key=lambda m: _coerce_float(details.get(m, {}).get("daily_quote_token_volume")) or 0.0, reverse=True)
        oi_list = sorted(mids, key=lambda m: _coerce_float(details.get(m, {}).get("open_interest")) or 0.0, reverse=True)
        combined = list(dict.fromkeys(vol_list[:n] + oi_list[:n]))  # dedup, preserve order
        return set(combined)

    def _parse_markets(self, response: object) -> Dict[int, dict]:
        """Parse orderBooks response into {market_id: info}."""
        if not isinstance(response, dict):
            return {}
        order_books = response.get("order_books", [])
        parsed: Dict[int, dict] = {}
        for item in order_books:
            if not isinstance(item, dict):
                continue
            if item.get("market_type") != "perp":
                continue
            if item.get("status") != "active":
                continue
            market_id = item.get("market_id")
            if market_id is None:
                continue
            parsed[int(market_id)] = {
                "symbol": item.get("symbol", ""),
                "market_id": int(market_id),
                "taker_fee": _coerce_float(item.get("taker_fee")) or 0.0,
                "maker_fee": _coerce_float(item.get("maker_fee")) or 0.0,
            }
        return parsed

    def _parse_markets_from_details(self, details: Dict[int, dict]) -> Dict[int, dict]:
        """Extract market info from already-parsed details (avoids separate orderBooks call)."""
        parsed: Dict[int, dict] = {}
        for market_id, item in details.items():
            if item.get("market_type") != "perp":
                continue
            if item.get("status") != "active":
                continue
            parsed[market_id] = {
                "symbol": item.get("symbol", ""),
                "market_id": market_id,
                "taker_fee": _coerce_float(item.get("taker_fee")) or 0.0,
                "maker_fee": _coerce_float(item.get("maker_fee")) or 0.0,
            }
        return parsed

    def _parse_details(self, response: object) -> Dict[int, dict]:
        """Parse orderBookDetails response into {market_id: details}."""
        if not isinstance(response, dict):
            return {}
        details_list = response.get("order_book_details", [])
        parsed: Dict[int, dict] = {}
        for item in details_list:
            if not isinstance(item, dict):
                continue
            market_id = item.get("market_id")
            if market_id is None:
                continue
            parsed[int(market_id)] = item
        return parsed

    def _parse_funding(self, response: object) -> Dict[int, float]:
        """Parse funding-rates response into {market_id: rate_bps} for Lighter exchange."""
        if not isinstance(response, dict):
            return {}
        rates = response.get("funding_rates", [])
        parsed: Dict[int, float] = {}
        for item in rates:
            if not isinstance(item, dict):
                continue
            if item.get("exchange") != "lighter":
                continue
            market_id = item.get("market_id")
            rate = _coerce_float(item.get("rate")) or 0.0
            if market_id is not None:
                # Lighter rate is already a decimal (e.g. -8.26e-06)
                parsed[int(market_id)] = rate * 10_000.0
        return parsed

    def _select_markets(self, markets: Dict[int, dict]) -> Dict[int, dict]:
        if not self.config.assets:
            return markets
        asset_set = {a.upper() for a in self.config.assets}
        return {
            mid: info for mid, info in markets.items()
            if info["symbol"].upper() in asset_set
        }

    def _build_snapshot(
        self,
        market_info: dict,
        detail: dict,
        book_resp: object,
        funding_rate_bps: float,
        latency_ms: float,
    ) -> MarketSnapshot:
        asset = market_info["symbol"]
        market_id = market_info["market_id"]

        # Parse orderbook
        bids, asks = self._parse_book(book_resp)
        if not bids or not asks:
            raise LighterApiError(f"Lighter orderbook for {asset} (market_id={market_id}) is empty")

        best_bid = bids[0][0]
        best_ask = asks[0][0]
        mid_price = (best_bid + best_ask) / 2.0

        # orderBookDetails.last_trade_price can lag materially; when we have a
        # live book, use the executable mid for cross-venue spread calculations.
        last_price = _coerce_float(detail.get("last_trade_price")) or mid_price
        mark_price = mid_price
        oracle_price = mid_price
        index_price = mid_price

        # Open interest — always base-denominated on Lighter, convert to USD
        oi_base = _coerce_float(detail.get("open_interest")) or 0.0
        oi_usd = oi_base * mark_price

        # Volume
        volume_24h = _coerce_float(detail.get("daily_quote_token_volume")) or 0.0
        daily_price_change = _coerce_float(detail.get("daily_price_change")) or 0.0

        # Fee — Lighter currently has zero fees
        taker_fee_bps = market_info["taker_fee"] * 10_000.0

        # Depth metrics
        symmetric_depth = min(self._book_notional(bids), self._book_notional(asks))
        top_1pct_depth = min(
            self._book_notional_within_band(bids, mid_price, "bid", 0.01),
            self._book_notional_within_band(asks, mid_price, "ask", 0.01),
        )
        impact_10k = self._estimate_roundtrip_impact_bps(bids, asks, mid_price, 10_000.0)
        impact_50k = self._estimate_roundtrip_impact_bps(bids, asks, mid_price, 50_000.0)
        volume_depth_ratio = volume_24h / max(top_1pct_depth, 1.0)

        realized_vol = abs(daily_price_change)

        timestamp = datetime.now(timezone.utc)

        return MarketSnapshot(
            venue="lighter",
            market_type="perp_dex",
            asset=asset,
            quote="USD",
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=oracle_price,
            index_price=index_price,
            taker_fee_bps=taker_fee_bps,
            maker_fee_bps=market_info["maker_fee"] * 10_000.0,
            depth_10k_usd=min(10_000.0, symmetric_depth),
            depth_50k_usd=min(50_000.0, symmetric_depth),
            top_1pct_depth_usd=top_1pct_depth,
            volume_depth_ratio=volume_depth_ratio,
            oi_usd=oi_usd,
            oi_change_pct=0.0,
            funding_rate_bps=funding_rate_bps,
            funding_change_bps=0.0,
            next_funding_time=None,
            impact_cost_10k_bps=impact_10k,
            impact_cost_50k_bps=impact_50k,
            slippage_bps=max(0.25, impact_10k * 0.5),
            realized_vol=realized_vol,
            jump_frequency=0.0,
            spread_zscore=0.0,
            trend_vs_mean_reversion=0.0,
            latency_ms=latency_ms,
            staleness_ms=0.0,
            timestamp=timestamp,
            metadata={
                "source": "lighter",
                "market_id": market_id,
                "last_trade_price": last_price,
                "volume_24h_usd": volume_24h,
                "funding_interval_hours": 8.0,
                "_book_bids": bids,
                "_book_asks": asks,
            },
        )

    def _build_snapshot_from_ticker(
        self,
        market_info: dict,
        detail: dict,
        funding_rate_bps: float,
        latency_ms: float,
    ) -> MarketSnapshot:
        """Build snapshot using only batch data (no orderbook request)."""
        asset = market_info["symbol"]
        market_id = market_info["market_id"]

        last_price = _coerce_float(detail.get("last_trade_price")) or 0.0
        if last_price <= 0:
            raise LighterApiError(f"No price for {asset}")
        mark_price = last_price
        oi_base = _coerce_float(detail.get("open_interest")) or 0.0
        oi_usd = oi_base * mark_price
        volume_24h = _coerce_float(detail.get("daily_quote_token_volume")) or 0.0
        daily_price_change = _coerce_float(detail.get("daily_price_change")) or 0.0
        taker_fee_bps = market_info["taker_fee"] * 10_000.0

        return MarketSnapshot(
            venue="lighter",
            market_type="perp_dex",
            asset=asset,
            quote="USD",
            best_bid=mark_price,
            best_ask=mark_price,
            mark_price=mark_price,
            oracle_price=mark_price,
            index_price=mark_price,
            taker_fee_bps=taker_fee_bps,
            maker_fee_bps=market_info["maker_fee"] * 10_000.0,
            depth_10k_usd=0.0,
            depth_50k_usd=0.0,
            top_1pct_depth_usd=0.0,
            volume_depth_ratio=0.0,
            oi_usd=oi_usd,
            oi_change_pct=0.0,
            funding_rate_bps=funding_rate_bps,
            funding_change_bps=0.0,
            next_funding_time=None,
            impact_cost_10k_bps=50.0,
            impact_cost_50k_bps=50.0,
            slippage_bps=25.0,
            realized_vol=abs(daily_price_change),
            jump_frequency=0.0,
            spread_zscore=0.0,
            trend_vs_mean_reversion=0.0,
            latency_ms=latency_ms,
            staleness_ms=0.0,
            timestamp=datetime.now(timezone.utc),
            metadata={
                "source": "lighter",
                "market_id": market_id,
                "volume_24h_usd": volume_24h,
                "funding_interval_hours": 8.0,
                "ticker_only": True,
            },
        )

    @staticmethod
    def _parse_book(book_resp: object) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        if not isinstance(book_resp, dict):
            return [], []
        bids = [
            (float(o["price"]), float(o["remaining_base_amount"]))
            for o in book_resp.get("bids", [])
            if isinstance(o, dict) and "price" in o and "remaining_base_amount" in o
        ]
        asks = [
            (float(o["price"]), float(o["remaining_base_amount"]))
            for o in book_resp.get("asks", [])
            if isinstance(o, dict) and "price" in o and "remaining_base_amount" in o
        ]
        return bids, asks

    @staticmethod
    def _book_notional(levels: Sequence[Tuple[float, float]]) -> float:
        return sum(price * size for price, size in levels)

    @staticmethod
    def _book_notional_within_band(
        levels: Sequence[Tuple[float, float]], mid_price: float, side: str, pct: float,
    ) -> float:
        if side == "bid":
            return sum(p * s for p, s in levels if p >= mid_price * (1.0 - pct))
        return sum(p * s for p, s in levels if p <= mid_price * (1.0 + pct))

    @staticmethod
    def _estimate_roundtrip_impact_bps(
        bids: Sequence[Tuple[float, float]],
        asks: Sequence[Tuple[float, float]],
        mid_price: float,
        target_notional: float,
    ) -> float:
        buy = _walk_impact_bps(asks, mid_price, target_notional, is_buy=True)
        sell = _walk_impact_bps(bids, mid_price, target_notional, is_buy=False)
        return buy + sell


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


def _coerce_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
