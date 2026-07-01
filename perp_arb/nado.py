"""Nado perpetual DEX REST adapter.

Public endpoints (no auth required):
  GET /v2/contracts                             → all perp contracts (mark, index, funding, OI, volume)
  GET /v2/orderbook?ticker_id=X&depth=20        → orderbook for a specific contract
  GET /v2/pairs?market=perp                     → list of perp pairs

Nado runs on Ink L2.  Archive base returns aggregated data;
Gateway base returns real-time orderbook data.
"""
from __future__ import annotations

import json
import logging
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
class NadoAdapterConfig:
    archive_url: str = "https://archive.prod.nado.xyz"
    gateway_url: str = "https://gateway.prod.nado.xyz"
    assets: Optional[Sequence[str]] = None
    timeout_seconds: float = 5.0
    book_depth: int = 20
    top_book_markets: int = 10
    book_request_workers: int = 4
    book_cache_ttl_seconds: float = 30.0
    book_stale_ttl_seconds: float = 300.0


class NadoApiError(RuntimeError):
    pass


class NadoClient:
    def __init__(self, config: NadoAdapterConfig | None = None) -> None:
        self.config = config or NadoAdapterConfig()

    def get(self, base: str, path: str) -> Tuple[object, float]:
        url = f"{base.rstrip('/')}/{path.lstrip('/')}"
        req = request.Request(url, method="GET", headers={
            "Accept": "application/json",
            "User-Agent": "perp-arb/0.1",
        })
        started = time.perf_counter()
        with request.urlopen(req, timeout=self.config.timeout_seconds) as response:
            raw = response.read()
        latency_ms = (time.perf_counter() - started) * 1000.0
        return json.loads(raw.decode("utf-8")), latency_ms


class NadoAdapter(MarketDataAdapter):
    def __init__(
        self,
        config: NadoAdapterConfig | None = None,
        client: NadoClient | None = None,
    ) -> None:
        self.config = config or NadoAdapterConfig()
        self.client = client or NadoClient(self.config)
        self.name = "nado"
        self._book_executor = ThreadPoolExecutor(
            max_workers=max(1, self.config.book_request_workers),
            thread_name_prefix="nado-book",
        )
        self._book_refresh_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="nado-book-refresh")
        self._book_refresh_future: Future | None = None
        self._book_cache: Dict[str, Tuple[object, float, float]] = {}
        self._book_cache_lock = threading.Lock()

    def poll(self) -> Sequence[MarketSnapshot]:
        # 1: Batch — contracts endpoint has everything we need
        contracts_resp, contracts_latency = self.client.get(
            self.config.archive_url, "v2/contracts"
        )
        if not isinstance(contracts_resp, dict):
            return []

        # Filter to selected assets
        selected = self._select_contracts(contracts_resp)

        # 2: Rank by volume, only fetch orderbooks for top N
        top_tickers = self._rank_top_markets(selected)
        book_responses = self._fetch_books_with_cache(top_tickers)

        # 3: Build snapshots
        snapshots: List[MarketSnapshot] = []
        for ticker_id, contract in selected.items():
            try:
                if ticker_id in top_tickers:
                    book_data = book_responses.get(ticker_id)
                    if book_data is not None:
                        book_resp, book_latency = book_data
                        snapshot = self._build_snapshot(
                            contract, book_resp,
                            max(contracts_latency, book_latency),
                        )
                    else:
                        snapshot = self._build_snapshot_from_contract(
                            contract, contracts_latency,
                        )
                else:
                    snapshot = self._build_snapshot_from_contract(
                        contract, contracts_latency,
                    )
                if snapshot is not None:
                    snapshots.append(snapshot)
            except Exception:
                continue
        return snapshots

    def _fetch_books_with_cache(self, ticker_ids: Sequence[str]) -> Dict[str, Tuple[object, float]]:
        if not ticker_ids:
            return {}
        cached, refresh_ids = self._cached_books_with_refresh_ids(ticker_ids, allow_stale=True)
        missing = [ticker_id for ticker_id in ticker_ids if ticker_id not in cached]
        if missing:
            fetched = self._fetch_books(missing)
            if fetched:
                self._store_cached_books(fetched)
                cached.update(fetched)
        if refresh_ids:
            self._request_background_book_refresh(refresh_ids)
        return cached

    def _fetch_books(self, ticker_ids: Sequence[str]) -> Dict[str, Tuple[object, float]]:
        futures = {
            self._book_executor.submit(
                self.client.get,
                self.config.gateway_url,
                f"v2/orderbook?ticker_id={ticker_id}&depth={self.config.book_depth}",
            ): ticker_id
            for ticker_id in ticker_ids
        }
        results: Dict[str, Tuple[object, float]] = {}
        for future in as_completed(futures):
            ticker_id = futures[future]
            try:
                results[ticker_id] = future.result()
            except Exception:
                continue
        return results

    def _cached_books_with_refresh_ids(
        self,
        ticker_ids: Sequence[str],
        allow_stale: bool = False,
    ) -> Tuple[Dict[str, Tuple[object, float]], set[str]]:
        ttl = self.config.book_cache_ttl_seconds
        if ttl <= 0:
            return {}, set()
        stale_ttl = max(ttl, self.config.book_stale_ttl_seconds)
        now = time.monotonic()
        results: Dict[str, Tuple[object, float]] = {}
        refresh_ids: set[str] = set()
        with self._book_cache_lock:
            for ticker_id in ticker_ids:
                cached = self._book_cache.get(ticker_id)
                if cached is None:
                    continue
                book_resp, latency_ms, cached_at = cached
                age = now - cached_at
                if age <= ttl:
                    results[ticker_id] = (book_resp, latency_ms)
                    continue
                if allow_stale and age <= stale_ttl:
                    results[ticker_id] = (book_resp, latency_ms)
                    refresh_ids.add(ticker_id)
                    continue
                if age > stale_ttl:
                    self._book_cache.pop(ticker_id, None)
        return results, refresh_ids

    def _store_cached_books(self, book_responses: Dict[str, Tuple[object, float]]) -> None:
        now = time.monotonic()
        with self._book_cache_lock:
            for ticker_id, (book_resp, latency_ms) in book_responses.items():
                self._book_cache[ticker_id] = (book_resp, latency_ms, now)

    def _request_background_book_refresh(self, ticker_ids: Sequence[str]) -> None:
        if not ticker_ids:
            return
        if self._book_refresh_future is not None and not self._book_refresh_future.done():
            return
        self._book_refresh_future = self._book_refresh_executor.submit(
            self._refresh_books_in_background,
            tuple(dict.fromkeys(ticker_ids)),
        )

    def _refresh_books_in_background(self, ticker_ids: Sequence[str]) -> None:
        try:
            fetched = self._fetch_books(ticker_ids)
            if fetched:
                self._store_cached_books(fetched)
        except Exception:
            logger.debug("nado background orderbook refresh failed", exc_info=True)

    def stop(self) -> None:
        self._book_refresh_executor.shutdown(wait=False, cancel_futures=False)
        self._book_executor.shutdown(wait=False, cancel_futures=False)

    def _select_contracts(self, contracts: dict) -> Dict[str, dict]:
        """Filter contracts to selected assets."""
        result: Dict[str, dict] = {}
        for ticker_id, info in contracts.items():
            if not isinstance(info, dict):
                continue
            if info.get("product_type") != "perpetual":
                continue
            asset = self._extract_asset(ticker_id)
            if not asset:
                continue
            if self.config.assets is not None:
                if asset.upper() not in {a.upper() for a in self.config.assets}:
                    continue
            result[ticker_id] = info
        return result

    def _rank_top_markets(self, contracts: Dict[str, dict]) -> set:
        """Return top N by volume ∪ top N by OI (deduplicated)."""
        n = self.config.top_book_markets
        tids = list(contracts.keys())
        vol_list = sorted(tids, key=lambda t: _coerce_float(contracts[t].get("quote_volume")) or 0.0, reverse=True)
        oi_list = sorted(tids, key=lambda t: _coerce_float(contracts[t].get("open_interest_usd")) or 0.0, reverse=True)
        return set(vol_list[:n]) | set(oi_list[:n])

    def _build_snapshot(
        self,
        contract: dict,
        book_resp: object,
        latency_ms: float,
    ) -> Optional[MarketSnapshot]:
        ticker_id = contract.get("ticker_id", "")
        asset = self._extract_asset(ticker_id)
        if not asset:
            return None

        bids, asks = self._parse_book(book_resp)
        if not bids or not asks:
            return self._build_snapshot_from_contract(contract, latency_ms)

        best_bid = bids[0][0]
        best_ask = asks[0][0]
        mid_price = (best_bid + best_ask) / 2.0

        mark_price = _coerce_float(contract.get("mark_price")) or mid_price
        index_price = _coerce_float(contract.get("index_price")) or mark_price
        oracle_price = index_price

        oi_usd = _coerce_float(contract.get("open_interest_usd")) or 0.0
        volume_24h = _coerce_float(contract.get("quote_volume")) or 0.0

        # funding_rate from contracts is a 24h rate (decimal).
        # Store as bps per 24h settlement and set interval=24h in metadata.
        funding_24h = _coerce_float(contract.get("funding_rate")) or 0.0
        funding_rate_bps = funding_24h * 10_000.0

        # Depth metrics
        symmetric_depth = min(self._book_notional(bids), self._book_notional(asks))
        top_1pct_depth = min(
            self._book_notional_within_band(bids, mid_price, "bid", 0.01),
            self._book_notional_within_band(asks, mid_price, "ask", 0.01),
        )
        impact_10k = self._estimate_roundtrip_impact_bps(bids, asks, mid_price, 10_000.0)
        impact_50k = self._estimate_roundtrip_impact_bps(bids, asks, mid_price, 50_000.0)
        volume_depth_ratio = volume_24h / max(top_1pct_depth, 1.0)

        price_change = _coerce_float(contract.get("price_change_percent_24h")) or 0.0
        realized_vol = abs(price_change) / 100.0

        return MarketSnapshot(
            venue="nado",
            market_type="perp_dex",
            asset=asset,
            quote="USD",
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=oracle_price,
            index_price=index_price,
            taker_fee_bps=4.5,
            maker_fee_bps=1.0,
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
            timestamp=datetime.now(timezone.utc),
            metadata={
                "source": "nado",
                "ticker_id": ticker_id,
                "product_id": contract.get("product_id"),
                "volume_24h_usd": volume_24h,
                "funding_interval_hours": 24.0,
                "_book_bids": bids,
                "_book_asks": asks,
            },
        )

    def _build_snapshot_from_contract(
        self,
        contract: dict,
        latency_ms: float,
    ) -> Optional[MarketSnapshot]:
        """Build snapshot using only contract data (no orderbook)."""
        ticker_id = contract.get("ticker_id", "")
        asset = self._extract_asset(ticker_id)
        if not asset:
            return None

        mark_price = _coerce_float(contract.get("mark_price")) or 0.0
        last_price = _coerce_float(contract.get("last_price")) or 0.0
        if mark_price <= 0 and last_price <= 0:
            return None
        if mark_price <= 0:
            mark_price = last_price

        index_price = _coerce_float(contract.get("index_price")) or mark_price
        oi_usd = _coerce_float(contract.get("open_interest_usd")) or 0.0
        volume_24h = _coerce_float(contract.get("quote_volume")) or 0.0

        funding_24h = _coerce_float(contract.get("funding_rate")) or 0.0
        funding_rate_bps = funding_24h * 10_000.0

        price_change = _coerce_float(contract.get("price_change_percent_24h")) or 0.0
        realized_vol = abs(price_change) / 100.0

        # Use last_price for bid/ask if available, otherwise mark_price
        price = last_price if last_price > 0 else mark_price

        return MarketSnapshot(
            venue="nado",
            market_type="perp_dex",
            asset=asset,
            quote="USD",
            best_bid=price,
            best_ask=price,
            mark_price=mark_price,
            oracle_price=index_price,
            index_price=index_price,
            taker_fee_bps=4.5,
            maker_fee_bps=1.0,
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
            realized_vol=realized_vol,
            jump_frequency=0.0,
            spread_zscore=0.0,
            trend_vs_mean_reversion=0.0,
            latency_ms=latency_ms,
            staleness_ms=0.0,
            timestamp=datetime.now(timezone.utc),
            metadata={
                "source": "nado",
                "ticker_id": ticker_id,
                "product_id": contract.get("product_id"),
                "volume_24h_usd": volume_24h,
                "funding_interval_hours": 24.0,
                "ticker_only": True,
            },
        )

    @staticmethod
    def _extract_asset(ticker_id: str) -> Optional[str]:
        """Extract asset symbol from ticker_id like 'BTC-PERP_USDT0'."""
        if "-PERP" not in ticker_id:
            return None
        return ticker_id.split("-PERP")[0]

    @staticmethod
    def _parse_book(book_resp: object) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        if not isinstance(book_resp, dict):
            return [], []
        bids = [
            (float(level[0]), float(level[1]))
            for level in book_resp.get("bids", [])
            if isinstance(level, (list, tuple)) and len(level) >= 2
        ]
        asks = [
            (float(level[0]), float(level[1]))
            for level in book_resp.get("asks", [])
            if isinstance(level, (list, tuple)) and len(level) >= 2
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
