"""Paradex perpetual DEX REST adapter.

Public endpoints (no auth required):
  GET /v1/markets/summary?market=ALL   → prices, OI, funding
  GET /v1/orderbook/{market}?depth=20  → orderbook
  GET /v1/markets                      → fee config
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
class ParadexAdapterConfig:
    base_url: str = "https://api.prod.paradex.trade/v1"
    assets: Optional[Sequence[str]] = None
    timeout_seconds: float = 5.0
    book_depth: int = 20
    top_book_markets: int = 10  # Only fetch orderbooks for top N markets by volume
    book_request_workers: int = 4
    book_cache_ttl_seconds: float = 30.0
    book_stale_ttl_seconds: float = 300.0


class ParadexApiError(RuntimeError):
    pass


class ParadexClient:
    def __init__(self, config: ParadexAdapterConfig | None = None) -> None:
        self.config = config or ParadexAdapterConfig()

    def get(self, path: str) -> Tuple[object, float]:
        url = f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"
        req = request.Request(url, method="GET", headers={
            "Accept": "application/json",
        })
        started = time.perf_counter()
        with request.urlopen(req, timeout=self.config.timeout_seconds) as response:
            raw = response.read()
        latency_ms = (time.perf_counter() - started) * 1000.0
        return json.loads(raw.decode("utf-8")), latency_ms


class ParadexAdapter(MarketDataAdapter):
    def __init__(
        self,
        config: ParadexAdapterConfig | None = None,
        client: ParadexClient | None = None,
    ) -> None:
        self.config = config or ParadexAdapterConfig()
        self.client = client or ParadexClient(self.config)
        self.name = "paradex"
        self._fee_cache: Optional[Dict[str, float]] = None
        self._book_executor = ThreadPoolExecutor(
            max_workers=max(1, self.config.book_request_workers),
            thread_name_prefix="paradex-book",
        )
        self._book_refresh_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="paradex-book-refresh")
        self._book_refresh_future: Future | None = None
        self._book_cache: Dict[str, Tuple[object, float, float]] = {}
        self._book_cache_lock = threading.Lock()

    def poll(self) -> Sequence[MarketSnapshot]:
        # Fetch markets summary (all tickers in one call)
        summary_resp, summary_latency = self.client.get("markets/summary?market=ALL")
        summaries = self._parse_summaries(summary_resp)

        # Fetch fee config once
        if self._fee_cache is None:
            self._fee_cache = self._fetch_fees()

        selected = self._select_assets(summaries)
        top_symbols = self._rank_top_markets(selected)
        book_responses = self._fetch_books_with_cache(top_symbols)

        snapshots: List[MarketSnapshot] = []
        for symbol, ticker in selected.items():
            try:
                if symbol in top_symbols:
                    book_data = book_responses.get(symbol)
                    if book_data is not None:
                        book_resp, book_latency = book_data
                        snapshot = self._build_snapshot(
                            symbol, ticker, book_resp,
                            max(summary_latency, book_latency),
                        )
                    else:
                        snapshot = self._build_snapshot_from_ticker(
                            symbol, ticker, summary_latency,
                        )
                else:
                    snapshot = self._build_snapshot_from_ticker(
                        symbol, ticker, summary_latency,
                    )
                snapshots.append(snapshot)
            except Exception:
                continue
        return snapshots

    def _fetch_books_with_cache(self, symbols: Sequence[str]) -> Dict[str, Tuple[object, float]]:
        if not symbols:
            return {}
        cached, refresh_symbols = self._cached_books_with_refresh_symbols(symbols, allow_stale=True)
        missing = [symbol for symbol in symbols if symbol not in cached]
        if missing:
            fetched = self._fetch_books(missing)
            if fetched:
                self._store_cached_books(fetched)
                cached.update(fetched)
        if refresh_symbols:
            self._request_background_book_refresh(refresh_symbols)
        return cached

    def _fetch_books(self, symbols: Sequence[str]) -> Dict[str, Tuple[object, float]]:
        if not symbols:
            return {}
        futures = {
            self._book_executor.submit(
                self.client.get,
                f"orderbook/{symbol}?depth={self.config.book_depth}",
            ): symbol
            for symbol in symbols
        }
        results: Dict[str, Tuple[object, float]] = {}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                results[symbol] = future.result()
            except Exception:
                continue
        return results

    def _cached_books_with_refresh_symbols(
        self,
        symbols: Sequence[str],
        allow_stale: bool = False,
    ) -> Tuple[Dict[str, Tuple[object, float]], set[str]]:
        ttl = self.config.book_cache_ttl_seconds
        if ttl <= 0:
            return {}, set()
        stale_ttl = max(ttl, self.config.book_stale_ttl_seconds)
        now = time.monotonic()
        results: Dict[str, Tuple[object, float]] = {}
        refresh_symbols: set[str] = set()
        with self._book_cache_lock:
            for symbol in symbols:
                cached = self._book_cache.get(symbol)
                if cached is None:
                    continue
                book_resp, latency_ms, cached_at = cached
                age = now - cached_at
                if age <= ttl:
                    results[symbol] = (book_resp, latency_ms)
                    continue
                if allow_stale and age <= stale_ttl:
                    results[symbol] = (book_resp, latency_ms)
                    refresh_symbols.add(symbol)
                    continue
                if age > stale_ttl:
                    self._book_cache.pop(symbol, None)
        return results, refresh_symbols

    def _store_cached_books(self, book_responses: Dict[str, Tuple[object, float]]) -> None:
        now = time.monotonic()
        with self._book_cache_lock:
            for symbol, (book_resp, latency_ms) in book_responses.items():
                self._book_cache[symbol] = (book_resp, latency_ms, now)

    def _request_background_book_refresh(self, symbols: Sequence[str]) -> None:
        if not symbols:
            return
        if self._book_refresh_future is not None and not self._book_refresh_future.done():
            return
        self._book_refresh_future = self._book_refresh_executor.submit(
            self._refresh_books_in_background,
            tuple(dict.fromkeys(symbols)),
        )

    def _refresh_books_in_background(self, symbols: Sequence[str]) -> None:
        try:
            fetched = self._fetch_books(symbols)
            if fetched:
                self._store_cached_books(fetched)
        except Exception:
            logger.debug("paradex background orderbook refresh failed", exc_info=True)

    def stop(self) -> None:
        self._book_refresh_executor.shutdown(wait=False, cancel_futures=False)
        self._book_executor.shutdown(wait=False, cancel_futures=False)

    def _rank_top_markets(self, summaries: Dict[str, dict]) -> set:
        """Return top N by volume ∪ top N by OI (deduplicated)."""
        n = self.config.top_book_markets
        syms = list(summaries.keys())
        vol_list = sorted(syms, key=lambda s: _coerce_float(summaries[s].get("volume_24h")) or 0.0, reverse=True)
        oi_list = sorted(syms, key=lambda s: _coerce_float(summaries[s].get("open_interest")) or 0.0, reverse=True)
        return set(vol_list[:n]) | set(oi_list[:n])

    def _fetch_fees(self) -> Dict[str, dict]:
        """Fetch taker/maker fee bps per market from /markets.

        Paradex fee_config has three tiers: interactive_fee, api_fee, rpi_fee.
        We use api_fee as that matches programmatic trading.
        """
        try:
            resp, _ = self.client.get("markets")
            results = resp if isinstance(resp, list) else resp.get("results", [])
            fees: Dict[str, dict] = {}
            for market in results:
                symbol = market.get("symbol", "")
                fee_config = market.get("fee_config", {})
                # Try api_fee first, fall back to flat keys
                api_fee = fee_config.get("api_fee", {})
                taker_raw = api_fee.get("taker_fee", {})
                maker_raw = api_fee.get("maker_fee", {})
                # Fee can be nested {"fee": "0.0002"} or flat "0.0002"
                taker = _coerce_float(taker_raw.get("fee") if isinstance(taker_raw, dict) else taker_raw) or 0.0
                maker = _coerce_float(maker_raw.get("fee") if isinstance(maker_raw, dict) else maker_raw) or 0.0
                fees[symbol] = {
                    "taker_bps": taker * 10_000.0,
                    "maker_bps": maker * 10_000.0,
                }
            return fees
        except Exception:
            return {}

    def _parse_summaries(self, response: object) -> Dict[str, dict]:
        results = response if isinstance(response, list) else []
        if isinstance(response, dict):
            results = response.get("results", [])
        parsed: Dict[str, dict] = {}
        for item in results:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol", "")
            if not symbol.endswith("-PERP"):
                continue
            parsed[symbol] = item
        return parsed

    def _select_assets(self, summaries: Dict[str, dict]) -> Dict[str, dict]:
        if not self.config.assets:
            return summaries
        selected: Dict[str, dict] = {}
        for asset in self.config.assets:
            symbol = f"{asset}-USD-PERP"
            if symbol in summaries:
                selected[symbol] = summaries[symbol]
        return selected

    def _build_snapshot(
        self,
        symbol: str,
        ticker: dict,
        book: object,
        latency_ms: float,
    ) -> MarketSnapshot:
        # Parse asset from symbol (e.g. "BTC-USD-PERP" → "BTC")
        asset = symbol.split("-")[0]

        mark_price = _coerce_float(ticker.get("mark_price"))
        underlying_price = _coerce_float(ticker.get("underlying_price"))
        last_price = _coerce_float(ticker.get("last_traded_price"))
        ticker_bid = _coerce_float(ticker.get("bid"))
        ticker_ask = _coerce_float(ticker.get("ask"))

        if mark_price is None:
            mark_price = last_price or 0.0
        oracle_price = underlying_price or mark_price
        index_price = underlying_price or mark_price

        # Parse orderbook
        bids, asks = self._parse_book(book)
        best_bid = bids[0][0] if bids else (ticker_bid or mark_price)
        best_ask = asks[0][0] if asks else (ticker_ask or mark_price)
        mid_price = (best_bid + best_ask) / 2.0

        # Open interest (in base units)
        oi_base = _coerce_float(ticker.get("open_interest")) or 0.0
        oi_usd = oi_base * mark_price

        # Funding rate — Paradex returns as decimal per period
        funding_rate = _coerce_float(ticker.get("funding_rate")) or 0.0
        funding_rate_bps = funding_rate * 10_000.0

        # Volume
        volume_24h = _coerce_float(ticker.get("volume_24h")) or 0.0

        # Timestamp
        created_at = _coerce_float(ticker.get("created_at"))
        timestamp = datetime.now(timezone.utc)
        staleness_ms = 0.0
        if created_at is not None:
            timestamp = datetime.fromtimestamp(created_at / 1000.0, tz=timezone.utc)
            staleness_ms = max(0.0, datetime.now(timezone.utc).timestamp() * 1000.0 - created_at)

        # Depth metrics
        symmetric_depth = min(self._book_notional(bids), self._book_notional(asks))
        top_1pct_depth = min(
            self._book_notional_within_band(bids, mid_price, "bid", 0.01),
            self._book_notional_within_band(asks, mid_price, "ask", 0.01),
        )
        impact_10k = self._estimate_roundtrip_impact_bps(bids, asks, mid_price, 10_000.0)
        impact_50k = self._estimate_roundtrip_impact_bps(bids, asks, mid_price, 50_000.0)
        volume_depth_ratio = volume_24h / max(top_1pct_depth, 1.0)

        # Fee
        taker_fee_bps = 0.75
        maker_fee_bps = 0.0

        # Realized vol proxy
        price_change = _coerce_float(ticker.get("price_change_rate_24h")) or 0.0
        realized_vol = abs(price_change) * 100.0

        return MarketSnapshot(
            venue="paradex",
            market_type="perp_dex",
            asset=asset,
            quote="USD",
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=oracle_price,
            index_price=index_price,
            taker_fee_bps=taker_fee_bps,
            maker_fee_bps=maker_fee_bps,
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
            spread_zscore=abs(mark_price - index_price) / max(index_price, 1e-9) * 10_000.0 / 10.0,
            trend_vs_mean_reversion=0.0,
            latency_ms=latency_ms,
            staleness_ms=staleness_ms,
            timestamp=timestamp,
            metadata={
                "source": "paradex",
                "symbol": symbol,
                "volume_24h_usd": volume_24h,
                "funding_interval_hours": 8.0,
                "_book_bids": bids,
                "_book_asks": asks,
            },
        )

    def _build_snapshot_from_ticker(
        self,
        symbol: str,
        ticker: dict,
        latency_ms: float,
    ) -> MarketSnapshot:
        """Build snapshot using only summary data (no orderbook request)."""
        asset = symbol.split("-")[0]
        mark_price = _coerce_float(ticker.get("mark_price")) or _coerce_float(ticker.get("last_traded_price")) or 0.0
        oracle_price = _coerce_float(ticker.get("underlying_price")) or mark_price
        ticker_bid = _coerce_float(ticker.get("bid")) or mark_price
        ticker_ask = _coerce_float(ticker.get("ask")) or mark_price
        oi_base = _coerce_float(ticker.get("open_interest")) or 0.0
        funding_rate = _coerce_float(ticker.get("funding_rate")) or 0.0
        volume_24h = _coerce_float(ticker.get("volume_24h")) or 0.0
        price_change = _coerce_float(ticker.get("price_change_rate_24h")) or 0.0
        taker_fee_bps = 0.75
        maker_fee_bps = 0.0
        created_at = _coerce_float(ticker.get("created_at"))
        timestamp = datetime.now(timezone.utc)
        staleness_ms = 0.0
        if created_at is not None:
            timestamp = datetime.fromtimestamp(created_at / 1000.0, tz=timezone.utc)
            staleness_ms = max(0.0, datetime.now(timezone.utc).timestamp() * 1000.0 - created_at)

        return MarketSnapshot(
            venue="paradex",
            market_type="perp_dex",
            asset=asset,
            quote="USD",
            best_bid=ticker_bid,
            best_ask=ticker_ask,
            mark_price=mark_price,
            oracle_price=oracle_price,
            index_price=oracle_price,
            taker_fee_bps=taker_fee_bps,
            maker_fee_bps=maker_fee_bps,
            depth_10k_usd=0.0,
            depth_50k_usd=0.0,
            top_1pct_depth_usd=0.0,
            volume_depth_ratio=0.0,
            oi_usd=oi_base * mark_price,
            oi_change_pct=0.0,
            funding_rate_bps=funding_rate * 10_000.0,
            funding_change_bps=0.0,
            next_funding_time=None,
            impact_cost_10k_bps=50.0,
            impact_cost_50k_bps=50.0,
            slippage_bps=25.0,
            realized_vol=abs(price_change) * 100.0,
            jump_frequency=0.0,
            spread_zscore=abs(mark_price - oracle_price) / max(oracle_price, 1e-9) * 10_000.0 / 10.0,
            trend_vs_mean_reversion=0.0,
            latency_ms=latency_ms,
            staleness_ms=staleness_ms,
            timestamp=timestamp,
            metadata={
                "source": "paradex",
                "symbol": symbol,
                "volume_24h_usd": volume_24h,
                "funding_interval_hours": 8.0,
                "ticker_only": True,
            },
        )

    @staticmethod
    def _parse_book(book: object) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        if not isinstance(book, dict):
            return [], []
        bids = [(float(p), float(s)) for p, s in (book.get("bids") or [])]
        asks = [(float(p), float(s)) for p, s in (book.get("asks") or [])]
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
