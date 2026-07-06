"""Variational Omni market-data adapter.

Universe discovery uses the official ``GET /metadata/stats`` endpoint.
Realtime prices come from the community-validated ``/prices`` websocket, and
``/api/quotes/indicative`` is used only for a small candidate set.
"""
from __future__ import annotations

import base64
import json
import logging
import math
import os
import socket
import ssl
import struct
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse
from urllib import request

from .market_data import MarketDataAdapter, MarketSnapshot, OpportunityBuilder


logger = logging.getLogger(__name__)

_MAJOR_ASSETS = {"BTC", "ETH", "SOL"}
_DEFAULT_RWA_ASSETS = set(OpportunityBuilder.STOCK_OR_ETF_SYMBOLS)


@dataclass(frozen=True)
class VariationalAdapterConfig:
    stats_base_url: str = "https://omni-client-api.prod.ap-northeast-1.variational.io"
    quote_base_url: str = "https://omni.variational.io"
    ws_url: str = "wss://omni-ws-server.prod.ap-northeast-1.variational.io/prices"
    assets: Optional[Sequence[str]] = None
    timeout_seconds: float = 5.0
    min_volume_usd: float = 50_000.0
    min_oi_usd: float = 10_000.0
    stats_refresh_seconds: float = 60.0
    price_stale_seconds: float = 30.0
    ws_initial_wait_seconds: float = 3.0
    settlement_asset: str = "USDC"
    subscription_batch_size: int = 75
    indicative_quote_markets: int = 20
    indicative_quote_qty: str = "0.001"
    indicative_quote_ttl_seconds: float = 10.0
    forwarder_snapshot_path: Optional[str] = None
    forwarder_quote_ttl_seconds: float = 15.0
    enable_websocket: bool = True
    enable_stats_mark_fallback: bool = True


@dataclass(frozen=True)
class _PriceEntry:
    price: float
    underlying_price: float
    interest_rate: Optional[float]
    received_at: float
    timestamp: datetime


@dataclass(frozen=True)
class _QuoteEntry:
    bid: float
    ask: float
    mark_price: Optional[float]
    received_at: float
    timestamp: datetime
    source: str = "indicative_quote"


class VariationalApiError(RuntimeError):
    pass


class VariationalClient:
    def __init__(self, config: VariationalAdapterConfig | None = None) -> None:
        self.config = config or VariationalAdapterConfig()

    def get_metadata_stats(self) -> Tuple[object, float]:
        url = f"{self.config.stats_base_url.rstrip('/')}/metadata/stats"
        return self._request_json(url, method="GET")

    def get_indicative_quote(self, payload: dict) -> Tuple[object, float]:
        url = f"{self.config.quote_base_url.rstrip('/')}/api/quotes/indicative"
        return self._request_json(url, method="POST", payload=payload)

    def _request_json(self, url: str, method: str, payload: Optional[dict] = None) -> Tuple[object, float]:
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "User-Agent": "perp-arb/0.1",
            "Origin": self.config.quote_base_url.rstrip("/"),
            "Referer": self.config.quote_base_url.rstrip("/") + "/",
        }
        req = request.Request(url, method=method, headers=headers, data=data)
        started = time.perf_counter()
        with request.urlopen(req, timeout=self.config.timeout_seconds) as response:
            raw = response.read()
        latency_ms = (time.perf_counter() - started) * 1000.0
        return json.loads(raw.decode("utf-8")), latency_ms


class VariationalAdapter(MarketDataAdapter):
    def __init__(
        self,
        config: VariationalAdapterConfig | None = None,
        client: VariationalClient | None = None,
    ) -> None:
        self.config = config or VariationalAdapterConfig()
        self.client = client or VariationalClient(self.config)
        self.name = "variational"
        self._lock = threading.RLock()
        self._listings: Dict[str, dict] = {}
        self._instruments: Dict[Tuple[str, str, int], dict] = {}
        self._last_stats_refresh = 0.0
        self._last_stats_latency_ms = 0.0
        self._last_stats_timestamp: Optional[datetime] = None
        self._price_cache: Dict[Tuple[str, str, int], _PriceEntry] = {}
        self._quote_cache: Dict[Tuple[str, str, int], _QuoteEntry] = {}
        self._stream_thread: Optional[threading.Thread] = None
        self._stream_stop = threading.Event()
        self._stream_signature: Tuple[Tuple[str, str, int], ...] = ()
        self._unsupported_instruments: set[Tuple[str, str, int]] = set()

    def poll(self) -> Sequence[MarketSnapshot]:
        self._refresh_universe_if_needed()
        instruments = self._instrument_items()
        if not instruments:
            return []
        if self.config.enable_websocket:
            self._ensure_streaming(instruments)
            self._wait_for_initial_prices([key for key, _ in instruments])
        self._load_forwarder_quotes(instruments)

        prices = self._fresh_prices()
        if prices:
            candidates = self._candidate_keys(prices, instruments)
            self._refresh_indicative_quotes(candidates)

        snapshots: List[MarketSnapshot] = []
        quotes = self._fresh_quotes()
        listings = self._listing_copy()
        for key, instrument in instruments:
            entry = prices.get(key)
            if entry is None:
                continue
            listing = listings.get(key[0], instrument)
            snapshot = self._build_snapshot(key, listing, entry, quotes.get(key))
            if snapshot is not None:
                snapshots.append(snapshot)
        quote_fallbacks = self._build_forwarder_quote_fallback_snapshots(listings, prices, quotes)
        snapshots.extend(quote_fallbacks)
        snapshots.extend(
            self._build_stats_mark_fallback_snapshots(
                listings,
                prices,
                {snapshot.asset for snapshot in quote_fallbacks},
            )
        )
        return snapshots

    def stop(self) -> None:
        self._stream_stop.set()
        thread = self._stream_thread
        if thread is not None:
            thread.join(timeout=2.0)
        self._stream_thread = None

    def _refresh_universe_if_needed(self) -> None:
        now = time.monotonic()
        with self._lock:
            if self._listings and now - self._last_stats_refresh < self.config.stats_refresh_seconds:
                return
        response, latency_ms = self.client.get_metadata_stats()
        listings = self._parse_listings(response)
        selected: Dict[str, dict] = {}
        instruments: Dict[Tuple[str, str, int], dict] = {}
        for listing in listings:
            ticker = str(listing.get("ticker") or "").upper()
            if not ticker or not self._allow_listing(ticker, listing):
                continue
            selected[ticker] = listing
            for interval in self._instrument_intervals(self._funding_interval_s(listing)):
                key = (ticker, self.config.settlement_asset.upper(), interval)
                instruments[key] = listing
        with self._lock:
            self._listings = selected
            self._instruments = instruments
            self._last_stats_refresh = now
            self._last_stats_latency_ms = latency_ms
            self._last_stats_timestamp = datetime.now(timezone.utc)

    @staticmethod
    def _parse_listings(response: object) -> Sequence[dict]:
        if not isinstance(response, dict):
            raise VariationalApiError("Unexpected Variational metadata response shape")
        listings = response.get("listings")
        if not isinstance(listings, list):
            raise VariationalApiError("Variational metadata response missing listings")
        return [item for item in listings if isinstance(item, dict)]

    def _allow_listing(self, ticker: str, listing: dict) -> bool:
        assets = self._allowed_assets()
        if ticker not in assets:
            return False
        volume_24h = _coerce_float(listing.get("volume_24h")) or 0.0
        oi_usd = self._listing_oi_usd(listing)
        return volume_24h > self.config.min_volume_usd and oi_usd > self.config.min_oi_usd

    def _allowed_assets(self) -> set[str]:
        if self.config.assets is not None:
            return {str(asset).upper() for asset in self.config.assets}
        return set(_MAJOR_ASSETS) | set(_DEFAULT_RWA_ASSETS)

    def _instrument_items(self) -> List[Tuple[Tuple[str, str, int], dict]]:
        with self._lock:
            return sorted(self._instruments.items())

    def _listing_copy(self) -> Dict[str, dict]:
        with self._lock:
            return dict(self._listings)

    def _ensure_streaming(self, instruments: Sequence[Tuple[Tuple[str, str, int], dict]]) -> None:
        with self._lock:
            unsupported = set(self._unsupported_instruments)
        signature = tuple(key for key, _ in sorted(instruments) if key not in unsupported)
        if not signature:
            return
        with self._lock:
            thread = self._stream_thread
            if thread is not None and thread.is_alive() and signature == self._stream_signature:
                return
            if thread is not None and thread.is_alive():
                self._stream_stop.set()
                thread.join(timeout=1.0)
            self._stream_stop.clear()
            self._stream_signature = signature
            self._stream_thread = threading.Thread(
                target=self._run_stream_thread,
                args=(signature,),
                name="variational-prices",
                daemon=True,
            )
            self._stream_thread.start()

    def _run_stream_thread(self, signature: Tuple[Tuple[str, str, int], ...]) -> None:
        try:
            self._stream_forever(signature)
        except Exception:
            logger.debug("variational websocket thread stopped", exc_info=True)

    def _stream_forever(self, signature: Tuple[Tuple[str, str, int], ...]) -> None:
        while not self._stream_stop.is_set():
            sock = None
            try:
                sock = _ws_connect(self.config.ws_url, timeout=self.config.timeout_seconds)
                for batch in _batched(signature, max(1, self.config.subscription_batch_size)):
                    _ws_send_text(sock, json.dumps(self._subscription_message(batch)))
                while not self._stream_stop.is_set():
                    raw = _ws_recv_text(sock, timeout=1.0)
                    if raw is None:
                        continue
                    if not raw.lstrip().startswith("{"):
                        self._handle_ws_text(raw)
                        continue
                    try:
                        self._handle_ws_payload(json.loads(raw))
                    except Exception:
                        logger.debug("variational websocket payload ignored", exc_info=True)
            except Exception:
                if self._stream_stop.is_set():
                    break
                time.sleep(1.0)
            finally:
                if sock is not None:
                    try:
                        sock.close()
                    except Exception:
                        pass

    @staticmethod
    def _subscription_message(keys: Sequence[Tuple[str, str, int]]) -> dict:
        return {
            "action": "subscribe",
            "instruments": [
                {
                    "underlying": asset,
                    "instrument_type": "perpetual_future",
                    "settlement_asset": settlement,
                    "funding_interval_s": interval,
                }
                for asset, settlement, interval in keys
            ],
        }

    def _handle_ws_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        channel = payload.get("channel")
        key = _parse_price_channel(channel) if isinstance(channel, str) else None
        if key is None:
            return
        pricing = payload.get("pricing")
        if not isinstance(pricing, dict):
            data = payload.get("data")
            pricing = data if isinstance(data, dict) else payload
        price = _coerce_float(pricing.get("price") or pricing.get("mark_price") or pricing.get("markPrice"))
        if price is None or price <= 0:
            return
        underlying_price = _coerce_float(
            pricing.get("underlying_price") or pricing.get("underlyingPrice") or pricing.get("index_price")
        )
        interest_rate = _coerce_float(pricing.get("interest_rate") or pricing.get("interestRate"))
        now = datetime.now(timezone.utc)
        with self._lock:
            self._price_cache[key] = _PriceEntry(
                price=price,
                underlying_price=underlying_price or price,
                interest_rate=interest_rate,
                received_at=time.monotonic(),
                timestamp=now,
            )

    def _handle_ws_text(self, text: str) -> None:
        marker = "unsupported instrument:"
        if marker not in text:
            return
        instrument = text.split(marker, 1)[1].strip().split()[0]
        parts = instrument.split("-")
        if len(parts) != 4 or parts[0] != "P":
            return
        try:
            key = (parts[1].upper(), parts[2].upper(), int(parts[3]))
        except ValueError:
            return
        with self._lock:
            self._unsupported_instruments.add(key)

    def _wait_for_initial_prices(self, keys: Sequence[Tuple[str, str, int]]) -> None:
        if self.config.ws_initial_wait_seconds <= 0:
            return
        deadline = time.monotonic() + self.config.ws_initial_wait_seconds
        key_set = set(keys)
        while time.monotonic() < deadline:
            with self._lock:
                if any(key in self._price_cache for key in key_set):
                    return
            time.sleep(0.05)

    def _fresh_prices(self) -> Dict[Tuple[str, str, int], _PriceEntry]:
        now = time.monotonic()
        ttl = max(0.5, self.config.price_stale_seconds)
        with self._lock:
            return {
                key: entry
                for key, entry in self._price_cache.items()
                if key in self._instruments and now - entry.received_at <= ttl
            }

    def _fresh_quotes(self) -> Dict[Tuple[str, str, int], _QuoteEntry]:
        now = time.monotonic()
        with self._lock:
            return {
                key: entry
                for key, entry in self._quote_cache.items()
                if now - entry.received_at <= self._quote_ttl(entry)
            }

    def _quote_ttl(self, entry: _QuoteEntry) -> float:
        if entry.source == "forwarder":
            return max(0.5, self.config.forwarder_quote_ttl_seconds)
        return max(0.5, self.config.indicative_quote_ttl_seconds)

    def _load_forwarder_quotes(
        self,
        instruments: Sequence[Tuple[Tuple[str, str, int], dict]],
    ) -> None:
        if not self.config.forwarder_snapshot_path:
            return
        path = Path(self.config.forwarder_snapshot_path)
        try:
            stat = path.stat()
        except OSError:
            return
        now_wall = time.time()
        ttl = max(0.5, self.config.forwarder_quote_ttl_seconds)
        if now_wall - stat.st_mtime > ttl:
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.debug("variational forwarder snapshot ignored", exc_info=True)
            return
        if not isinstance(data, dict):
            return
        rows = data.get("quotes")
        if not isinstance(rows, dict):
            return
        instrument_keys = {key for key, _ in instruments}
        generated_at = _parse_datetime(data.get("generated_at"))
        received_at = time.monotonic() - max(0.0, now_wall - stat.st_mtime)
        updates: Dict[Tuple[str, str, int], _QuoteEntry] = {}
        for asset, row in rows.items():
            if not isinstance(row, dict):
                continue
            quote = self._parse_forwarder_quote(row, received_at, generated_at)
            if quote is None:
                continue
            key = self._forwarder_quote_key(str(asset), row, instrument_keys)
            if key is None:
                continue
            updates[key] = quote
        if not updates:
            return
        with self._lock:
            self._quote_cache.update(updates)

    def _forwarder_quote_key(
        self,
        asset: str,
        row: dict,
        instrument_keys: set[Tuple[str, str, int]],
    ) -> Optional[Tuple[str, str, int]]:
        raw = row.get("raw")
        payload = raw if isinstance(raw, dict) else row
        instrument = payload.get("instrument") if isinstance(payload, dict) else None
        if not isinstance(instrument, dict):
            instrument = row.get("instrument") if isinstance(row.get("instrument"), dict) else {}
        asset_upper = str(instrument.get("underlying") or asset).upper()
        settlement = str(instrument.get("settlement_asset") or self.config.settlement_asset).upper()
        interval = _coerce_float(instrument.get("funding_interval_s"))
        if interval is not None and interval > 0:
            key = (asset_upper, settlement, int(interval))
            if key in instrument_keys:
                return key
        candidates = sorted(key for key in instrument_keys if key[0] == asset_upper and key[1] == settlement)
        return candidates[0] if candidates else None

    @staticmethod
    def _parse_forwarder_quote(
        row: dict,
        received_at: float,
        fallback_timestamp: Optional[datetime],
    ) -> Optional[_QuoteEntry]:
        raw = row.get("raw")
        payload = raw if isinstance(raw, dict) else row
        bid = _coerce_float(row.get("bid") or payload.get("bid") or payload.get("best_bid") or payload.get("bestBid"))
        ask = _coerce_float(row.get("ask") or payload.get("ask") or payload.get("best_ask") or payload.get("bestAsk"))
        mark = _coerce_float(
            row.get("mark_price") or payload.get("mark_price") or payload.get("markPrice")
        )
        if bid is None or ask is None or bid <= 0 or ask <= 0:
            return None
        if mark is None or mark <= 0:
            mark = (bid + ask) / 2.0
        timestamp = _parse_datetime(row.get("timestamp") or payload.get("timestamp")) or fallback_timestamp
        return _QuoteEntry(
            bid=bid,
            ask=ask,
            mark_price=mark,
            received_at=received_at,
            timestamp=timestamp or datetime.now(timezone.utc),
            source="forwarder",
        )

    def _candidate_keys(
        self,
        prices: Dict[Tuple[str, str, int], _PriceEntry],
        instruments: Sequence[Tuple[Tuple[str, str, int], dict]],
    ) -> List[Tuple[str, str, int]]:
        limit = max(0, int(self.config.indicative_quote_markets))
        if limit <= 0:
            return []
        listing_by_key = dict(instruments)
        ranked = sorted(
            prices,
            key=lambda key: (
                self._basis_bps(prices[key]),
                self._listing_oi_usd(listing_by_key.get(key, {})),
                _coerce_float(listing_by_key.get(key, {}).get("volume_24h")) or 0.0,
            ),
            reverse=True,
        )
        return ranked[:limit]

    @staticmethod
    def _basis_bps(entry: _PriceEntry) -> float:
        if entry.underlying_price <= 0:
            return 0.0
        return abs(entry.price - entry.underlying_price) / entry.underlying_price * 10_000.0

    def _refresh_indicative_quotes(self, keys: Sequence[Tuple[str, str, int]]) -> None:
        if not keys:
            return
        now = time.monotonic()
        for key in keys:
            with self._lock:
                cached = self._quote_cache.get(key)
                if cached is not None and now - cached.received_at <= self._quote_ttl(cached):
                    continue
            payload = self._indicative_quote_payload(key)
            try:
                response, _latency = self.client.get_indicative_quote(payload)
                quote = self._parse_indicative_quote(response)
            except Exception:
                logger.debug("variational indicative quote failed for %s", key, exc_info=True)
                continue
            if quote is None:
                continue
            with self._lock:
                self._quote_cache[key] = quote

    def _indicative_quote_payload(self, key: Tuple[str, str, int]) -> dict:
        asset, settlement, interval = key
        return {
            "instrument": {
                "underlying": asset,
                "instrument_type": "perpetual_future",
                "settlement_asset": settlement,
                "funding_interval_s": interval,
            },
            "qty": str(self.config.indicative_quote_qty),
        }

    @staticmethod
    def _parse_indicative_quote(response: object) -> Optional[_QuoteEntry]:
        if not isinstance(response, dict):
            return None
        data = response.get("quote")
        if not isinstance(data, dict):
            data = response
        bid = _coerce_float(data.get("bid") or data.get("best_bid") or data.get("bestBid"))
        ask = _coerce_float(data.get("ask") or data.get("best_ask") or data.get("bestAsk"))
        mark = _coerce_float(data.get("mark_price") or data.get("markPrice"))
        if bid is None or ask is None or bid <= 0 or ask <= 0:
            return None
        return _QuoteEntry(
            bid=bid,
            ask=ask,
            mark_price=mark,
            received_at=time.monotonic(),
            timestamp=datetime.now(timezone.utc),
        )

    def _build_snapshot(
        self,
        key: Tuple[str, str, int],
        listing: dict,
        price: _PriceEntry,
        quote: Optional[_QuoteEntry],
    ) -> Optional[MarketSnapshot]:
        asset, settlement, interval = key
        mark_price = quote.mark_price if quote and quote.mark_price and quote.mark_price > 0 else price.price
        if mark_price <= 0:
            return None
        if quote is not None:
            best_bid = quote.bid
            best_ask = quote.ask
            quote_verified = True
        else:
            base_spread_bps = _coerce_float(listing.get("base_spread_bps")) or 2.0
            half_spread = max(base_spread_bps, 0.5) / 20_000.0
            best_bid = mark_price * (1.0 - half_spread)
            best_ask = mark_price * (1.0 + half_spread)
            quote_verified = False
        if best_bid <= 0 or best_ask <= 0:
            return None

        spread_bps = abs(best_ask - best_bid) / max((best_ask + best_bid) / 2.0, 1e-12) * 10_000.0
        volume_24h = _coerce_float(listing.get("volume_24h")) or 0.0
        oi_usd = self._listing_oi_usd(listing)
        funding_rate = price.interest_rate
        funding_rate_bps = (
            funding_rate * 10_000.0
            if funding_rate is not None
            else self._stats_funding_rate_bps(listing, interval)
        )
        age_ms = max(0.0, (time.monotonic() - price.received_at) * 1000.0)
        funding_hours = max(interval / 3600.0, 1e-9)
        stock_like = self._is_rwa_asset(asset)
        return MarketSnapshot(
            venue="variational",
            market_type="perp_dex",
            asset=asset,
            quote=settlement,
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=price.underlying_price or mark_price,
            index_price=price.underlying_price or mark_price,
            taker_fee_bps=0.0,
            maker_fee_bps=0.0,
            depth_10k_usd=0.0,
            depth_50k_usd=0.0,
            top_1pct_depth_usd=0.0,
            volume_depth_ratio=0.0,
            oi_usd=oi_usd,
            funding_rate_bps=funding_rate_bps,
            next_funding_time=_next_funding_time(interval),
            impact_cost_10k_bps=max(0.5, spread_bps),
            impact_cost_50k_bps=max(1.0, spread_bps + 5.0),
            slippage_bps=max(0.25, spread_bps * 0.5),
            realized_vol=0.0,
            latency_ms=self._last_stats_latency_ms,
            staleness_ms=age_ms,
            timestamp=price.timestamp,
            metadata={
                "source": "variational",
                "ticker": asset,
                "instrument": f"P-{asset}-{settlement}-{interval}",
                "settlement_asset": settlement,
                "funding_interval_hours": funding_hours,
                "volume_24h_usd": volume_24h,
                "day_quote_volume_usd": volume_24h,
                "stock_like": stock_like,
                "market_family": "stock" if stock_like else "crypto",
                "price_source": "ws_prices",
                "funding_rate_source": "ws_prices" if funding_rate is not None else "metadata_stats_annualized",
                "indicative_quote": quote_verified,
                "ticker_only": not quote_verified,
                "ws_received_at": price.timestamp.isoformat(),
            },
        )

    def _build_stats_mark_fallback_snapshots(
        self,
        listings: Dict[str, dict],
        prices: Dict[Tuple[str, str, int], _PriceEntry],
        quote_assets: set[str] | None = None,
    ) -> List[MarketSnapshot]:
        if not self.config.enable_stats_mark_fallback:
            return []
        quote_assets = quote_assets or set()
        priced_assets = {key[0] for key in prices}
        snapshots: List[MarketSnapshot] = []
        for asset, listing in sorted(listings.items()):
            if asset in priced_assets or asset in quote_assets or not self._is_rwa_asset(asset):
                continue
            snapshot = self._build_stats_mark_fallback_snapshot(asset, listing)
            if snapshot is not None:
                snapshots.append(snapshot)
        return snapshots

    def _build_forwarder_quote_fallback_snapshots(
        self,
        listings: Dict[str, dict],
        prices: Dict[Tuple[str, str, int], _PriceEntry],
        quotes: Dict[Tuple[str, str, int], _QuoteEntry],
    ) -> List[MarketSnapshot]:
        priced_assets = {key[0] for key in prices}
        snapshots: List[MarketSnapshot] = []
        for key, quote in sorted(quotes.items()):
            asset = key[0]
            if asset in priced_assets:
                continue
            listing = listings.get(asset)
            if listing is None:
                continue
            snapshot = self._build_forwarder_quote_fallback_snapshot(key, listing, quote)
            if snapshot is not None:
                snapshots.append(snapshot)
        return snapshots

    def _build_forwarder_quote_fallback_snapshot(
        self,
        key: Tuple[str, str, int],
        listing: dict,
        quote: _QuoteEntry,
    ) -> Optional[MarketSnapshot]:
        asset, settlement, interval = key
        best_bid = quote.bid
        best_ask = quote.ask
        mark_price = quote.mark_price if quote.mark_price and quote.mark_price > 0 else (best_bid + best_ask) / 2.0
        if best_bid <= 0 or best_ask <= 0 or mark_price <= 0:
            return None
        spread_bps = abs(best_ask - best_bid) / max((best_ask + best_bid) / 2.0, 1e-12) * 10_000.0
        volume_24h = _coerce_float(listing.get("volume_24h")) or 0.0
        oi_usd = self._listing_oi_usd(listing)
        funding_hours = max(interval / 3600.0, 1e-9)
        age_ms = max(0.0, (time.monotonic() - quote.received_at) * 1000.0)
        stock_like = self._is_rwa_asset(asset)
        return MarketSnapshot(
            venue="variational",
            market_type="perp_dex",
            asset=asset,
            quote=settlement,
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=mark_price,
            index_price=mark_price,
            taker_fee_bps=0.0,
            maker_fee_bps=0.0,
            depth_10k_usd=0.0,
            depth_50k_usd=0.0,
            top_1pct_depth_usd=0.0,
            volume_depth_ratio=0.0,
            oi_usd=oi_usd,
            funding_rate_bps=self._stats_funding_rate_bps(listing, interval),
            next_funding_time=_next_funding_time(interval),
            impact_cost_10k_bps=max(0.5, spread_bps),
            impact_cost_50k_bps=max(1.0, spread_bps + 5.0),
            slippage_bps=max(0.25, spread_bps * 0.5),
            realized_vol=0.0,
            latency_ms=self._last_stats_latency_ms,
            staleness_ms=age_ms,
            timestamp=quote.timestamp,
            metadata={
                "source": "variational",
                "ticker": asset,
                "instrument": f"P-{asset}-{settlement}-{interval}",
                "settlement_asset": settlement,
                "funding_interval_hours": funding_hours,
                "volume_24h_usd": volume_24h,
                "day_quote_volume_usd": volume_24h,
                "stock_like": stock_like,
                "market_family": "stock" if stock_like else "crypto",
                "price_source": "frontend_indicative_quote",
                "funding_rate_source": "metadata_stats_annualized",
                "forwarder_quote": True,
                "indicative_quote": True,
                "ticker_only": False,
                "forwarder_quote_received_at": quote.timestamp.isoformat(),
            },
        )

    def _build_stats_mark_fallback_snapshot(self, asset: str, listing: dict) -> Optional[MarketSnapshot]:
        mark_price = _coerce_float(listing.get("mark_price"))
        if mark_price is None or mark_price <= 0:
            return None
        interval = self._funding_interval_s(listing)
        settlement = self.config.settlement_asset.upper()
        base_spread_bps = _coerce_float(listing.get("base_spread_bps")) or 10.0
        # Do not use metadata/stats cached quotes for RWA fallback.  This bid/ask
        # is a conservative display envelope around mark, not an executable quote.
        half_spread = max(base_spread_bps, 10.0) / 20_000.0
        best_bid = mark_price * (1.0 - half_spread)
        best_ask = mark_price * (1.0 + half_spread)
        volume_24h = _coerce_float(listing.get("volume_24h")) or 0.0
        oi_usd = self._listing_oi_usd(listing)
        funding_rate_bps = self._stats_funding_rate_bps(listing, interval)
        timestamp = self._last_stats_timestamp or datetime.now(timezone.utc)
        staleness_ms = max(0.0, (datetime.now(timezone.utc) - timestamp).total_seconds() * 1000.0)
        return MarketSnapshot(
            venue="variational",
            market_type="perp_dex",
            asset=asset,
            quote=settlement,
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=mark_price,
            index_price=mark_price,
            taker_fee_bps=0.0,
            maker_fee_bps=0.0,
            depth_10k_usd=0.0,
            depth_50k_usd=0.0,
            top_1pct_depth_usd=0.0,
            volume_depth_ratio=0.0,
            oi_usd=oi_usd,
            funding_rate_bps=funding_rate_bps,
            next_funding_time=_next_funding_time(interval),
            impact_cost_10k_bps=50.0,
            impact_cost_50k_bps=75.0,
            slippage_bps=25.0,
            realized_vol=0.0,
            latency_ms=self._last_stats_latency_ms,
            staleness_ms=staleness_ms,
            timestamp=timestamp,
            metadata={
                "source": "variational",
                "ticker": asset,
                "instrument": f"P-{asset}-{settlement}-{interval}",
                "settlement_asset": settlement,
                "funding_interval_hours": max(interval / 3600.0, 1e-9),
                "volume_24h_usd": volume_24h,
                "day_quote_volume_usd": volume_24h,
                "stock_like": True,
                "market_family": "stock",
                "price_source": "metadata_stats_mark",
                "funding_rate_source": "metadata_stats_annualized",
                "stats_mark_fallback": True,
                "indicative_quote": False,
                "ticker_only": True,
                "stats_fetched_at": timestamp.isoformat(),
            },
        )

    @staticmethod
    def _funding_interval_s(listing: dict) -> int:
        value = _coerce_float(listing.get("funding_interval_s"))
        if value is None or value <= 0:
            return 3600
        return int(value)

    @staticmethod
    def _instrument_intervals(primary: int) -> List[int]:
        intervals = [primary]
        for fallback in (3600, 28800):
            if fallback not in intervals:
                intervals.append(fallback)
        return intervals

    @staticmethod
    def _listing_oi_usd(listing: dict) -> float:
        oi = listing.get("open_interest")
        if isinstance(oi, dict):
            long_oi = _coerce_float(oi.get("long_open_interest")) or 0.0
            short_oi = _coerce_float(oi.get("short_open_interest")) or 0.0
            return long_oi + short_oi
        return _coerce_float(oi) or 0.0

    @staticmethod
    def _stats_funding_rate_bps(listing: dict, interval_s: int) -> float:
        annual_rate = _coerce_float(listing.get("funding_rate")) or 0.0
        # metadata/stats funding_rate behaves like an annualized decimal for
        # stock/RWA markets. Convert it to the per-settlement bps expected by
        # the rest of the scanner and dashboard.
        interval_hours = max(float(interval_s) / 3600.0, 1e-9)
        return annual_rate * (interval_hours / 24.0 / 365.0) * 10_000.0

    @staticmethod
    def _is_rwa_asset(asset: str) -> bool:
        return asset.upper() in _DEFAULT_RWA_ASSETS


def _parse_price_channel(channel: str) -> Optional[Tuple[str, str, int]]:
    prefix = "instrument_price:"
    if not channel.startswith(prefix):
        return None
    instrument = channel[len(prefix):]
    parts = instrument.split("-")
    if len(parts) != 4 or parts[0] != "P":
        return None
    try:
        interval = int(parts[3])
    except ValueError:
        return None
    return parts[1].upper(), parts[2].upper(), interval


def _batched(values: Sequence[Tuple[str, str, int]], size: int) -> Iterable[Sequence[Tuple[str, str, int]]]:
    for index in range(0, len(values), size):
        yield values[index:index + size]


def _next_funding_time(interval_s: int) -> datetime:
    now = datetime.now(timezone.utc)
    interval = max(int(interval_s), 1)
    seconds_until = interval - (int(now.timestamp()) % interval)
    return now + timedelta(seconds=seconds_until)


def _coerce_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def _parse_datetime(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _ws_connect(url: str, timeout: float) -> socket.socket:
    parsed = urlparse(url)
    if parsed.scheme != "wss" or not parsed.hostname:
        raise VariationalApiError(f"Unsupported websocket URL: {url}")
    port = parsed.port or 443
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    raw_sock = socket.create_connection((parsed.hostname, port), timeout=timeout)
    sock = ssl.create_default_context().wrap_socket(raw_sock, server_hostname=parsed.hostname)
    sock.settimeout(timeout)
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    request_text = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {parsed.hostname}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "User-Agent: perp-arb/0.1\r\n"
        "\r\n"
    )
    sock.sendall(request_text.encode("ascii"))
    header = b""
    while b"\r\n\r\n" not in header:
        chunk = sock.recv(4096)
        if not chunk:
            raise VariationalApiError("Variational websocket closed during handshake")
        header += chunk
        if len(header) > 65536:
            raise VariationalApiError("Variational websocket handshake too large")
    if b" 101 " not in header.split(b"\r\n", 1)[0]:
        raise VariationalApiError(f"Variational websocket handshake failed: {header[:200]!r}")
    return sock


def _ws_send_text(sock: socket.socket, text: str) -> None:
    payload = text.encode("utf-8")
    header = bytearray([0x81])
    length = len(payload)
    if length < 126:
        header.append(0x80 | length)
    elif length < 65536:
        header.append(0x80 | 126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(0x80 | 127)
        header.extend(struct.pack("!Q", length))
    mask = os.urandom(4)
    masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    sock.sendall(bytes(header) + mask + masked)


def _ws_recv_text(sock: socket.socket, timeout: float) -> Optional[str]:
    sock.settimeout(timeout)
    try:
        first = _recv_exact(sock, 2)
    except socket.timeout:
        return None
    if not first:
        raise VariationalApiError("Variational websocket closed")
    b1, b2 = first
    opcode = b1 & 0x0F
    length = b2 & 0x7F
    masked = bool(b2 & 0x80)
    if length == 126:
        length = struct.unpack("!H", _recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", _recv_exact(sock, 8))[0]
    mask = _recv_exact(sock, 4) if masked else b""
    payload = _recv_exact(sock, length) if length else b""
    if masked:
        payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    if opcode == 8:
        raise VariationalApiError("Variational websocket close frame")
    if opcode == 9:
        _ws_send_pong(sock, payload)
        return None
    if opcode != 1:
        return None
    return payload.decode("utf-8", "replace")


def _ws_send_pong(sock: socket.socket, payload: bytes) -> None:
    header = bytearray([0x8A])
    length = len(payload)
    if length >= 126:
        payload = payload[:125]
        length = len(payload)
    header.append(0x80 | length)
    mask = os.urandom(4)
    masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    sock.sendall(bytes(header) + mask + masked)


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        chunk = sock.recv(size - len(chunks))
        if not chunk:
            raise VariationalApiError("Variational websocket closed")
        chunks.extend(chunk)
    return bytes(chunks)
