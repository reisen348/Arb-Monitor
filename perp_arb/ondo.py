"""Ondo Perps public REST adapter.

Public endpoints (no auth required):
  GET /v1/perps/contracts?sparkline=false       → all perp contracts and market stats
  GET /v1/perps/mark_prices                     → all mark/oracle prices
  GET /v1/perps/depth?market=X&depth=20         → orderbook for one market
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
from urllib import parse, request

from .market_data import MarketDataAdapter, MarketSnapshot


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OndoAdapterConfig:
    base_url: str = "https://api.ondoperps.xyz"
    assets: Optional[Sequence[str]] = None
    timeout_seconds: float = 5.0
    book_depth: int = 20
    top_book_markets: int = 10
    book_request_workers: int = 4
    book_cache_ttl_seconds: float = 10.0
    book_stale_ttl_seconds: float = 60.0


class OndoApiError(RuntimeError):
    pass


class OndoClient:
    def __init__(self, config: OndoAdapterConfig | None = None) -> None:
        self.config = config or OndoAdapterConfig()

    def get(self, path: str) -> Tuple[object, float]:
        url = f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"
        req = request.Request(url, method="GET", headers={
            "Accept": "application/json",
            "User-Agent": "perp-arb/0.1",
        })
        started = time.perf_counter()
        with request.urlopen(req, timeout=self.config.timeout_seconds) as response:
            raw = response.read()
        latency_ms = (time.perf_counter() - started) * 1000.0
        return json.loads(raw.decode("utf-8")), latency_ms


class OndoAdapter(MarketDataAdapter):
    def __init__(
        self,
        config: OndoAdapterConfig | None = None,
        client: OndoClient | None = None,
    ) -> None:
        self.config = config or OndoAdapterConfig()
        self.client = client or OndoClient(self.config)
        self.name = "ondo"
        self._book_executor = ThreadPoolExecutor(
            max_workers=max(1, self.config.book_request_workers),
            thread_name_prefix="ondo-book",
        )
        self._book_refresh_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ondo-book-refresh")
        self._book_refresh_future: Future | None = None
        self._book_cache: Dict[str, Tuple[object, float, float]] = {}
        self._book_cache_lock = threading.Lock()

    def poll(self) -> Sequence[MarketSnapshot]:
        contracts_resp, contracts_latency = self.client.get("v1/perps/contracts?sparkline=false")
        contracts = self._response_result(contracts_resp)
        if not isinstance(contracts, list):
            return []

        selected = self._select_contracts(contracts)
        if not selected:
            return []

        try:
            mark_resp, mark_latency = self.client.get("v1/perps/mark_prices")
            mark_prices = self._response_result(mark_resp)
            mark_latency_ms = mark_latency
        except Exception:
            mark_prices = {}
            mark_latency_ms = 0.0
        if not isinstance(mark_prices, dict):
            mark_prices = {}

        top_markets = self._rank_top_markets(selected)
        book_responses = self._fetch_books_with_cache(top_markets)

        snapshots: List[MarketSnapshot] = []
        for market, contract in selected.items():
            try:
                mark_info = mark_prices.get(market) if isinstance(mark_prices, dict) else None
                latency_ms = max(contracts_latency, mark_latency_ms)
                if market in top_markets and market in book_responses:
                    book_resp, book_latency = book_responses[market]
                    snapshot = self._build_snapshot(
                        contract,
                        mark_info if isinstance(mark_info, dict) else {},
                        book_resp,
                        max(latency_ms, book_latency),
                    )
                else:
                    snapshot = self._build_snapshot_from_contract(
                        contract,
                        mark_info if isinstance(mark_info, dict) else {},
                        latency_ms,
                    )
                if snapshot is not None:
                    snapshots.append(snapshot)
            except Exception:
                logger.debug("ondo snapshot build failed for %s", market, exc_info=True)
                continue
        return snapshots

    def _select_contracts(self, contracts: Sequence[object]) -> Dict[str, dict]:
        selected_assets = {asset.upper() for asset in self.config.assets} if self.config.assets is not None else None
        result: Dict[str, dict] = {}
        for item in contracts:
            if not isinstance(item, dict):
                continue
            if item.get("productType") != "perpetual":
                continue
            if bool(item.get("disabled")):
                continue
            market = str(item.get("market") or "")
            asset = self._extract_asset(item)
            if not market or not asset:
                continue
            if selected_assets is not None and asset.upper() not in selected_assets:
                continue
            result[market] = item
        return result

    def _rank_top_markets(self, contracts: Dict[str, dict]) -> set[str]:
        n = max(0, self.config.top_book_markets)
        if n <= 0:
            return set()
        markets = list(contracts.keys())
        vol_list = sorted(markets, key=lambda m: _coerce_float(contracts[m].get("usdVolume")) or 0.0, reverse=True)
        oi_list = sorted(markets, key=lambda m: _coerce_float(contracts[m].get("openInterestUsd")) or 0.0, reverse=True)
        return set(vol_list[:n]) | set(oi_list[:n])

    def _fetch_books_with_cache(self, markets: Sequence[str] | set[str]) -> Dict[str, Tuple[object, float]]:
        market_list = list(markets)
        if not market_list:
            return {}
        cached, refresh_ids = self._cached_books_with_refresh_ids(market_list, allow_stale=True)
        missing = [market for market in market_list if market not in cached]
        if missing:
            fetched = self._fetch_books(missing)
            if fetched:
                self._store_cached_books(fetched)
                cached.update(fetched)
        if refresh_ids:
            self._request_background_book_refresh(refresh_ids)
        return cached

    def _fetch_books(self, markets: Sequence[str]) -> Dict[str, Tuple[object, float]]:
        futures = {
            self._book_executor.submit(
                self.client.get,
                "v1/perps/depth?"
                + parse.urlencode({"market": market, "depth": max(1, min(100, self.config.book_depth))}),
            ): market
            for market in markets
        }
        results: Dict[str, Tuple[object, float]] = {}
        for future in as_completed(futures):
            market = futures[future]
            try:
                results[market] = future.result()
            except Exception:
                logger.debug("ondo orderbook fetch failed for %s", market, exc_info=True)
                continue
        return results

    def _cached_books_with_refresh_ids(
        self,
        markets: Sequence[str],
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
            for market in markets:
                cached = self._book_cache.get(market)
                if cached is None:
                    continue
                book_resp, latency_ms, cached_at = cached
                age = now - cached_at
                if age <= ttl:
                    results[market] = (book_resp, latency_ms)
                    continue
                if allow_stale and age <= stale_ttl:
                    results[market] = (book_resp, latency_ms)
                    refresh_ids.add(market)
                    continue
                if age > stale_ttl:
                    self._book_cache.pop(market, None)
        return results, refresh_ids

    def _store_cached_books(self, book_responses: Dict[str, Tuple[object, float]]) -> None:
        now = time.monotonic()
        with self._book_cache_lock:
            for market, (book_resp, latency_ms) in book_responses.items():
                self._book_cache[market] = (book_resp, latency_ms, now)

    def _request_background_book_refresh(self, markets: Sequence[str]) -> None:
        if not markets:
            return
        if self._book_refresh_future is not None and not self._book_refresh_future.done():
            return
        self._book_refresh_future = self._book_refresh_executor.submit(
            self._refresh_books_in_background,
            tuple(dict.fromkeys(markets)),
        )

    def _refresh_books_in_background(self, markets: Sequence[str]) -> None:
        try:
            fetched = self._fetch_books(markets)
            if fetched:
                self._store_cached_books(fetched)
        except Exception:
            logger.debug("ondo background orderbook refresh failed", exc_info=True)

    def stop(self) -> None:
        self._book_refresh_executor.shutdown(wait=False, cancel_futures=False)
        self._book_executor.shutdown(wait=False, cancel_futures=False)

    def _build_snapshot(
        self,
        contract: dict,
        mark_info: dict,
        book_resp: object,
        latency_ms: float,
    ) -> Optional[MarketSnapshot]:
        market = str(contract.get("market") or "")
        asset = self._extract_asset(contract)
        if not market or not asset:
            return None
        bids, asks = self._parse_book(book_resp)
        if not bids or not asks:
            return self._build_snapshot_from_contract(contract, mark_info, latency_ms)

        best_bid = bids[0][0]
        best_ask = asks[0][0]
        mid_price = (best_bid + best_ask) / 2.0
        mark_price, oracle_price, index_price = self._prices(contract, mark_info, mid_price)
        common = self._common_values(contract, mark_info, mark_price, best_bid, best_ask)

        symmetric_depth = min(self._book_notional(bids), self._book_notional(asks))
        top_1pct_depth = min(
            self._book_notional_within_band(bids, mid_price, "bid", 0.01),
            self._book_notional_within_band(asks, mid_price, "ask", 0.01),
        )
        impact_10k = self._estimate_roundtrip_impact_bps(bids, asks, mid_price, 10_000.0)
        impact_50k = self._estimate_roundtrip_impact_bps(bids, asks, mid_price, 50_000.0)
        volume_depth_ratio = common["volume_24h"] / max(top_1pct_depth, 1.0)

        return MarketSnapshot(
            venue="ondo",
            market_type="perp_dex",
            asset=asset,
            quote="USD",
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=oracle_price,
            index_price=index_price,
            taker_fee_bps=common["taker_fee_bps"],
            maker_fee_bps=common["maker_fee_bps"],
            depth_10k_usd=min(10_000.0, symmetric_depth),
            depth_50k_usd=min(50_000.0, symmetric_depth),
            top_1pct_depth_usd=top_1pct_depth,
            volume_depth_ratio=volume_depth_ratio,
            oi_usd=common["oi_usd"],
            oi_change_pct=0.0,
            funding_rate_bps=common["funding_rate_bps"],
            funding_change_bps=common["funding_change_bps"],
            next_funding_time=common["next_funding_time"],
            impact_cost_10k_bps=impact_10k,
            impact_cost_50k_bps=impact_50k,
            slippage_bps=max(0.25, impact_10k * 0.5),
            realized_vol=common["realized_vol"],
            jump_frequency=0.0,
            spread_zscore=0.0,
            trend_vs_mean_reversion=0.0,
            latency_ms=latency_ms,
            staleness_ms=common["staleness_ms"],
            timestamp=common["timestamp"],
            metadata={
                "source": "ondo",
                "market": market,
                "display_name": contract.get("displayName"),
                "tags": contract.get("tags") or [],
                "stock_like": self._is_stock_like(contract),
                "volume_24h_usd": common["volume_24h"],
                "funding_interval_hours": common["funding_interval_hours"],
                "_book_bids": bids,
                "_book_asks": asks,
            },
        )

    def _build_snapshot_from_contract(
        self,
        contract: dict,
        mark_info: dict,
        latency_ms: float,
    ) -> Optional[MarketSnapshot]:
        market = str(contract.get("market") or "")
        asset = self._extract_asset(contract)
        if not market or not asset:
            return None

        best_bid = _coerce_float(contract.get("bid")) or 0.0
        best_ask = _coerce_float(contract.get("ask")) or 0.0
        last_price = _coerce_float(contract.get("lastPrice")) or 0.0
        mark_seed = _coerce_float(mark_info.get("markPrice")) or _coerce_float(contract.get("indexPrice")) or last_price
        if best_bid <= 0 or best_ask <= 0:
            price = last_price if last_price > 0 else (mark_seed or 0.0)
            best_bid = price
            best_ask = price
        if best_bid <= 0 or best_ask <= 0:
            return None

        mid_price = (best_bid + best_ask) / 2.0
        mark_price, oracle_price, index_price = self._prices(contract, mark_info, mid_price)
        common = self._common_values(contract, mark_info, mark_price, best_bid, best_ask)
        spread_bps = max(0.25, (best_ask - best_bid) / max(mid_price, 1e-12) * 10_000.0)
        open_interest_base = _coerce_float(contract.get("openInterest")) or 0.0
        quoted_depth = max(0.0, min(best_bid, best_ask) * open_interest_base)

        return MarketSnapshot(
            venue="ondo",
            market_type="perp_dex",
            asset=asset,
            quote="USD",
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=oracle_price,
            index_price=index_price,
            taker_fee_bps=common["taker_fee_bps"],
            maker_fee_bps=common["maker_fee_bps"],
            depth_10k_usd=min(10_000.0, quoted_depth),
            depth_50k_usd=min(50_000.0, quoted_depth),
            top_1pct_depth_usd=0.0,
            volume_depth_ratio=0.0,
            oi_usd=common["oi_usd"],
            oi_change_pct=0.0,
            funding_rate_bps=common["funding_rate_bps"],
            funding_change_bps=common["funding_change_bps"],
            next_funding_time=common["next_funding_time"],
            impact_cost_10k_bps=spread_bps,
            impact_cost_50k_bps=spread_bps + 15.0,
            slippage_bps=max(0.5, spread_bps * 0.5),
            realized_vol=common["realized_vol"],
            jump_frequency=0.0,
            spread_zscore=0.0,
            trend_vs_mean_reversion=0.0,
            latency_ms=latency_ms,
            staleness_ms=common["staleness_ms"],
            timestamp=common["timestamp"],
            metadata={
                "source": "ondo",
                "market": market,
                "display_name": contract.get("displayName"),
                "tags": contract.get("tags") or [],
                "stock_like": self._is_stock_like(contract),
                "volume_24h_usd": common["volume_24h"],
                "funding_interval_hours": common["funding_interval_hours"],
                "ticker_only": True,
            },
        )

    def _common_values(self, contract: dict, mark_info: dict, mark_price: float, best_bid: float, best_ask: float) -> dict:
        timestamp = self._parse_datetime(mark_info.get("lastUpdatedTime")) or datetime.now(timezone.utc)
        staleness_ms = max(0.0, (datetime.now(timezone.utc) - timestamp).total_seconds() * 1000.0)
        next_funding_time = self._parse_datetime(contract.get("nextFundingRateTimestamp"))
        funding_interval_hours = self._funding_interval_hours(next_funding_time)
        funding_rate = _coerce_float(contract.get("nextFundingRate"))
        if funding_rate is None:
            funding_rate = _coerce_float(contract.get("fundingRate")) or 0.0
        last_funding_rate = _coerce_float(contract.get("fundingRate")) or 0.0
        bid = max(_coerce_float(contract.get("bid")) or best_bid, 0.0)
        ask = max(_coerce_float(contract.get("ask")) or best_ask, 0.0)
        return {
            "timestamp": timestamp,
            "staleness_ms": staleness_ms,
            "next_funding_time": next_funding_time,
            "funding_interval_hours": funding_interval_hours,
            "funding_rate_bps": funding_rate * 10_000.0,
            "funding_change_bps": (funding_rate - last_funding_rate) * 10_000.0,
            "maker_fee_bps": (_coerce_float(contract.get("makerFee")) or 0.0) * 10_000.0,
            "taker_fee_bps": (_coerce_float(contract.get("takerFee")) or 0.0) * 10_000.0,
            "volume_24h": _coerce_float(contract.get("usdVolume")) or _coerce_float(contract.get("quoteVolume")) or 0.0,
            "oi_usd": _coerce_float(contract.get("openInterestUsd")) or 0.0,
            "realized_vol": abs(_coerce_float(contract.get("priceChangePercent")) or 0.0) / 100.0,
            "quoted_mid": (bid + ask) / 2.0 if bid > 0 and ask > 0 else mark_price,
        }

    @staticmethod
    def _response_result(response: object) -> object:
        if isinstance(response, dict) and response.get("success") is False:
            raise OndoApiError(str(response.get("error") or response.get("error_code") or "ondo API error"))
        if isinstance(response, dict) and "result" in response:
            return response.get("result")
        return response

    @staticmethod
    def _extract_asset(contract: dict) -> Optional[str]:
        base = contract.get("baseCurrency")
        if isinstance(base, str) and base:
            return base.upper()
        market = str(contract.get("market") or "")
        if "-USD.P" in market:
            return market.split("-USD.P", 1)[0].upper()
        if "-" in market:
            return market.split("-", 1)[0].upper()
        return None

    @staticmethod
    def _prices(contract: dict, mark_info: dict, fallback_price: float) -> Tuple[float, float, float]:
        mark_price = (
            _coerce_float(mark_info.get("markPrice"))
            or _coerce_float(mark_info.get("price"))
            or fallback_price
            or _coerce_float(contract.get("lastPrice"))
        )
        oracle_price = _coerce_float(mark_info.get("oraclePrice")) or _coerce_float(contract.get("indexPrice")) or mark_price
        index_price = _coerce_float(contract.get("indexPrice")) or oracle_price
        return mark_price, oracle_price, index_price

    @staticmethod
    def _parse_book(book_resp: object) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        if isinstance(book_resp, dict) and "result" in book_resp:
            book_resp = book_resp.get("result")
        if not isinstance(book_resp, dict):
            return [], []
        bids = [
            (float(level[0]), float(level[1]))
            for level in book_resp.get("bids", [])
            if isinstance(level, (list, tuple)) and len(level) >= 2 and _coerce_float(level[0]) and _coerce_float(level[1])
        ]
        asks = [
            (float(level[0]), float(level[1]))
            for level in book_resp.get("asks", [])
            if isinstance(level, (list, tuple)) and len(level) >= 2 and _coerce_float(level[0]) and _coerce_float(level[1])
        ]
        bids.sort(key=lambda item: item[0], reverse=True)
        asks.sort(key=lambda item: item[0])
        return bids, asks

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
            return sum(price * size for price, size in levels if price >= mid_price * (1.0 - pct))
        return sum(price * size for price, size in levels if price <= mid_price * (1.0 + pct))

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

    @staticmethod
    def _parse_datetime(value: object) -> Optional[datetime]:
        if not isinstance(value, str) or not value:
            return None
        text = value.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            if "." in text:
                head, tail = text.split(".", 1)
                offset = "+00:00" if tail.endswith("+00:00") else ""
                frac = tail.removesuffix("+00:00")[:6]
                try:
                    dt = datetime.fromisoformat(f"{head}.{frac}{offset}")
                except ValueError:
                    return None
            else:
                return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    @staticmethod
    def _funding_interval_hours(next_funding_time: Optional[datetime]) -> float:
        # Ondo settles funding hourly; keep the fallback explicit if the timestamp is missing.
        return 1.0 if next_funding_time is not None else 1.0

    @staticmethod
    def _is_stock_like(contract: dict) -> bool:
        tags = {str(tag).lower() for tag in (contract.get("tags") or [])}
        return bool(tags & {"stock", "etf", "index"})


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
