"""Binance Futures USDⓈ-M perpetual CEX REST adapter.

Public endpoints (no auth required):
  GET /fapi/v1/premiumIndex       → mark price, index price, funding rate (batch)
  GET /fapi/v1/ticker/24hr        → 24h volume and price change stats (batch)
  GET /fapi/v1/ticker/bookTicker  → best bid/ask (batch)
  GET /fapi/v1/openInterest?symbol=X → current open interest for one symbol
  GET /fapi/v1/depth?symbol=X&limit=20 → orderbook for one symbol
"""
from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple
from urllib import request

from .market_data import MarketDataAdapter, MarketSnapshot


@dataclass(frozen=True)
class BinanceAdapterConfig:
    base_url: str = "https://fapi.binance.com"
    assets: Optional[Sequence[str]] = None
    timeout_seconds: float = 5.0
    top_book_markets: int = 10  # Only fetch depth for top N markets by volume
    oi_markets: int = 100  # Fetch official OI for top N markets independently of depth


class BinanceApiError(RuntimeError):
    pass


class BinanceClient:
    def __init__(self, config: BinanceAdapterConfig | None = None) -> None:
        self.config = config or BinanceAdapterConfig()

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


class BinanceAdapter(MarketDataAdapter):
    def __init__(
        self,
        config: BinanceAdapterConfig | None = None,
        client: BinanceClient | None = None,
    ) -> None:
        self.config = config or BinanceAdapterConfig()
        self.client = client or BinanceClient(self.config)
        self.name = "binance"
        self._funding_interval_map: Dict[str, float] = {}

    def _load_funding_intervals(self) -> None:
        """Load per-symbol funding intervals from fundingInfo endpoint."""
        try:
            resp, _ = self.client.get("fapi/v1/fundingInfo")
            if isinstance(resp, list):
                for item in resp:
                    symbol = item.get("symbol", "")
                    interval = _coerce_float(item.get("fundingIntervalHours"))
                    if symbol and interval and interval > 0:
                        self._funding_interval_map[symbol] = interval
        except Exception:
            pass  # Fall back to default 8h

    def poll(self) -> Sequence[MarketSnapshot]:
        # 0. Lazy-load funding interval map (one-time)
        if not self._funding_interval_map:
            self._load_funding_intervals()

        # 1. Batch: mark price, index price, funding rate for ALL symbols
        premium_resp, premium_latency = self.client.get("fapi/v1/premiumIndex")
        premium_map = self._parse_premium_index(premium_resp)

        # 2. Batch: 24h volume/change stats and top-of-book for ALL symbols.
        ticker_resp, ticker_latency = self.client.get("fapi/v1/ticker/24hr")
        ticker_map = self._parse_tickers(ticker_resp)
        book_ticker_resp, book_ticker_latency = self.client.get("fapi/v1/ticker/bookTicker")
        book_ticker_map = self._parse_book_tickers(book_ticker_resp)

        batch_latency = max(premium_latency, ticker_latency, book_ticker_latency)

        # Filter to only USDT perpetual symbols present in both responses
        symbols = self._select_symbols(premium_map, ticker_map)

        # 3. Rank by official 24h quote volume, then fetch depth and OI for
        # detailed markets. Binance does not expose current OI in the 24h
        # ticker batch, so OI is fetched through the official per-symbol
        # openInterest endpoint for the detailed set.
        top_symbols = self._rank_top_markets(symbols, ticker_map)
        oi_symbols = symbols if self.config.assets else sorted(
            top_symbols | self._rank_oi_markets(symbols, ticker_map)
        )
        open_interest_map, oi_latency = self._fetch_open_interest(oi_symbols)

        snapshots: List[MarketSnapshot] = []
        for symbol in symbols:
            premium = premium_map[symbol]
            ticker = ticker_map.get(symbol, {})
            book_ticker = book_ticker_map.get(symbol, {})
            open_interest = open_interest_map.get(symbol)
            try:
                if symbol in top_symbols:
                    book_resp, book_latency = self.client.get(
                        f"fapi/v1/depth?symbol={symbol}&limit=20"
                    )
                    snapshot = self._build_snapshot(
                        symbol, premium, ticker, book_ticker, open_interest, book_resp,
                        max(batch_latency, book_latency, oi_latency),
                    )
                else:
                    snapshot = self._build_snapshot_from_ticker(
                        symbol, premium, ticker, book_ticker, open_interest,
                        max(batch_latency, oi_latency if open_interest is not None else 0.0),
                    )
                snapshots.append(snapshot)
            except Exception:
                continue
        return snapshots

    def _rank_top_markets(self, symbols: List[str], ticker_map: Dict[str, dict]) -> set:
        """Return top N markets by official 24h quote volume."""
        n = self.config.top_book_markets
        vol_list = sorted(
            symbols,
            key=lambda s: _coerce_float(ticker_map.get(s, {}).get("quoteVolume")) or 0.0,
            reverse=True,
        )
        return set(vol_list[:n])

    def _rank_oi_markets(self, symbols: List[str], ticker_map: Dict[str, dict]) -> set:
        """Return symbols that should receive per-symbol official OI requests."""
        n = max(0, self.config.oi_markets)
        if n <= 0:
            return set()
        vol_list = sorted(
            symbols,
            key=lambda s: _coerce_float(ticker_map.get(s, {}).get("quoteVolume")) or 0.0,
            reverse=True,
        )
        return set(vol_list[:n])

    def _select_symbols(self, premium_map: Dict[str, dict], ticker_map: Dict[str, dict]) -> List[str]:
        """Select USDT perpetual symbols, optionally filtered by config.assets."""
        # premium_map already filtered to USDT perps
        available = sorted(premium_map.keys())
        if self.config.assets:
            asset_set = {a.upper() for a in self.config.assets}
            available = [s for s in available if s.replace("USDT", "") in asset_set]
        return available

    @staticmethod
    def _parse_premium_index(response: object) -> Dict[str, dict]:
        """Parse premiumIndex batch response into {symbol: info}.

        Only includes USDT perpetual contracts.
        """
        if not isinstance(response, list):
            raise BinanceApiError("Unexpected premiumIndex response shape")
        parsed: Dict[str, dict] = {}
        for item in response:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol", "")
            if not symbol.endswith("USDT"):
                continue
            # premiumIndex only returns perpetual contracts by default;
            # filter by pair field as safety check
            pair = item.get("pair", "")
            if pair and not pair.endswith("USDT"):
                continue
            parsed[symbol] = item
        return parsed

    @staticmethod
    def _parse_tickers(response: object) -> Dict[str, dict]:
        """Parse ticker/24hr batch response into {symbol: info}."""
        if not isinstance(response, list):
            raise BinanceApiError("Unexpected ticker/24hr response shape")
        parsed: Dict[str, dict] = {}
        for item in response:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol", "")
            if symbol:
                parsed[symbol] = item
        return parsed

    @staticmethod
    def _parse_book_tickers(response: object) -> Dict[str, dict]:
        """Parse ticker/bookTicker batch response into {symbol: info}."""
        if not isinstance(response, list):
            raise BinanceApiError("Unexpected ticker/bookTicker response shape")
        parsed: Dict[str, dict] = {}
        for item in response:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol", "")
            if symbol:
                parsed[symbol] = item
        return parsed

    def _fetch_open_interest(self, symbols: Sequence[str]) -> Tuple[Dict[str, float], float]:
        parsed: Dict[str, float] = {}
        max_latency = 0.0
        for symbol in symbols:
            try:
                response, latency = self.client.get(f"fapi/v1/openInterest?symbol={symbol}")
            except Exception:
                continue
            max_latency = max(max_latency, latency)
            value = self._parse_open_interest(response)
            if value is not None:
                parsed[symbol] = value
        return parsed, max_latency

    @staticmethod
    def _parse_open_interest(response: object) -> Optional[float]:
        if not isinstance(response, dict):
            return None
        return _coerce_float(response.get("openInterest"))

    def _build_snapshot(
        self,
        symbol: str,
        premium: dict,
        ticker: dict,
        book_ticker: dict,
        open_interest: Optional[float],
        book_resp: object,
        latency_ms: float,
    ) -> MarketSnapshot:
        asset = symbol.replace("USDT", "")

        # Parse orderbook
        bids, asks = self._parse_book(book_resp)
        if not bids or not asks:
            raise BinanceApiError(f"Binance orderbook for {symbol} is empty")

        best_bid = bids[0][0]
        best_ask = asks[0][0]
        mid_price = (best_bid + best_ask) / 2.0

        # Prices from premiumIndex
        mark_price = _coerce_float(premium.get("markPrice")) or mid_price
        index_price = _coerce_float(premium.get("indexPrice")) or mark_price
        oracle_price = index_price

        # Funding
        funding_rate = _coerce_float(premium.get("lastFundingRate")) or 0.0
        funding_rate_bps = funding_rate * 10_000.0
        next_funding_ts = _coerce_float(premium.get("nextFundingTime"))
        next_funding_time: Optional[datetime] = None
        if next_funding_ts and next_funding_ts > 0:
            next_funding_time = datetime.fromtimestamp(next_funding_ts / 1000.0, tz=timezone.utc)

        # Volume & OI from ticker
        volume_24h = _coerce_float(ticker.get("quoteVolume")) or 0.0
        oi_contracts = open_interest or 0.0  # base amount from official openInterest endpoint
        oi_usd = oi_contracts * mark_price

        # Price change for realized vol
        prev_close = _coerce_float(ticker.get("openPrice")) or _coerce_float(ticker.get("prevClosePrice")) or mark_price
        realized_vol = abs(math.log(mark_price / prev_close)) * 100.0 if prev_close > 0 else 0.0

        # Depth metrics
        symmetric_depth = min(self._book_notional(bids), self._book_notional(asks))
        top_1pct_depth = min(
            self._book_notional_within_band(bids, mid_price, "bid", 0.01),
            self._book_notional_within_band(asks, mid_price, "ask", 0.01),
        )
        impact_10k = self._estimate_roundtrip_impact_bps(bids, asks, mid_price, 10_000.0)
        impact_50k = self._estimate_roundtrip_impact_bps(bids, asks, mid_price, 50_000.0)
        volume_depth_ratio = volume_24h / max(top_1pct_depth, 1.0)

        # Premium as spread z-score proxy
        premium_bps = abs((mark_price - index_price) / max(index_price, 1e-9)) * 10_000.0

        timestamp = datetime.now(timezone.utc)

        return MarketSnapshot(
            venue="binance",
            market_type="perp_cex",
            asset=asset,
            quote="USDT",
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=oracle_price,
            index_price=index_price,
            taker_fee_bps=4.5,
            maker_fee_bps=2.0,
            depth_10k_usd=min(10_000.0, symmetric_depth),
            depth_50k_usd=min(50_000.0, symmetric_depth),
            top_1pct_depth_usd=top_1pct_depth,
            volume_depth_ratio=volume_depth_ratio,
            oi_usd=oi_usd,
            oi_change_pct=0.0,
            funding_rate_bps=funding_rate_bps,
            funding_change_bps=0.0,
            next_funding_time=next_funding_time,
            impact_cost_10k_bps=impact_10k,
            impact_cost_50k_bps=impact_50k,
            slippage_bps=max(0.25, impact_10k * 0.5),
            realized_vol=realized_vol,
            jump_frequency=0.0,
            spread_zscore=premium_bps / 10.0,
            trend_vs_mean_reversion=0.0,
            latency_ms=latency_ms,
            staleness_ms=0.0,
            timestamp=timestamp,
            metadata={
                "source": "binance",
                "symbol": symbol,
                "premium_bps": premium_bps,
                "day_quote_volume_usd": volume_24h,
                "open_interest_contracts": oi_contracts,
                "book_ticker_time": book_ticker.get("time"),
                "funding_interval_hours": self._funding_interval_map.get(symbol, 8.0),
                "_book_bids": bids,
                "_book_asks": asks,
            },
        )

    def _build_snapshot_from_ticker(
        self,
        symbol: str,
        premium: dict,
        ticker: dict,
        book_ticker: dict,
        open_interest: Optional[float],
        latency_ms: float,
    ) -> MarketSnapshot:
        """Build snapshot using only batch data (no depth request)."""
        asset = symbol.replace("USDT", "")

        mark_price = _coerce_float(premium.get("markPrice")) or 0.0
        index_price = _coerce_float(premium.get("indexPrice")) or mark_price
        oracle_price = index_price

        if mark_price <= 0:
            raise BinanceApiError(f"No price for {symbol}")

        # Funding
        funding_rate = _coerce_float(premium.get("lastFundingRate")) or 0.0
        funding_rate_bps = funding_rate * 10_000.0
        next_funding_ts = _coerce_float(premium.get("nextFundingTime"))
        next_funding_time: Optional[datetime] = None
        if next_funding_ts and next_funding_ts > 0:
            next_funding_time = datetime.fromtimestamp(next_funding_ts / 1000.0, tz=timezone.utc)

        # Bid/ask from official bookTicker batch endpoint.
        best_bid = _coerce_float(book_ticker.get("bidPrice")) or mark_price
        best_ask = _coerce_float(book_ticker.get("askPrice")) or mark_price

        # Volume & OI
        volume_24h = _coerce_float(ticker.get("quoteVolume")) or 0.0
        oi_contracts = open_interest or 0.0
        oi_usd = oi_contracts * mark_price

        # Price change
        prev_close = _coerce_float(ticker.get("openPrice")) or _coerce_float(ticker.get("prevClosePrice")) or mark_price
        realized_vol = abs(math.log(mark_price / prev_close)) * 100.0 if prev_close > 0 else 0.0

        premium_bps = abs((mark_price - index_price) / max(index_price, 1e-9)) * 10_000.0

        return MarketSnapshot(
            venue="binance",
            market_type="perp_cex",
            asset=asset,
            quote="USDT",
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=oracle_price,
            index_price=index_price,
            taker_fee_bps=4.5,
            maker_fee_bps=2.0,
            depth_10k_usd=0.0,
            depth_50k_usd=0.0,
            top_1pct_depth_usd=0.0,
            volume_depth_ratio=0.0,
            oi_usd=oi_usd,
            oi_change_pct=0.0,
            funding_rate_bps=funding_rate_bps,
            funding_change_bps=0.0,
            next_funding_time=next_funding_time,
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
            metadata={
                "source": "binance",
                "symbol": symbol,
                "premium_bps": premium_bps,
                "day_quote_volume_usd": volume_24h,
                "open_interest_contracts": oi_contracts,
                "book_ticker_time": book_ticker.get("time"),
                "funding_interval_hours": self._funding_interval_map.get(symbol, 8.0),
                "ticker_only": True,
            },
        )

    @staticmethod
    def _parse_book(book_resp: object) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        """Parse Binance depth response.

        Format: {"bids": [["price", "qty"], ...], "asks": [["price", "qty"], ...]}
        """
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
