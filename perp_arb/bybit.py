"""Bybit V5 perpetual CEX REST adapter.

Public endpoints (no auth required):
  GET /v5/market/tickers?category=linear      → all linear perp tickers
  GET /v5/market/orderbook?category=linear&symbol=X&limit=20 → orderbook

The tickers endpoint returns everything in one call: bid/ask, mark, index,
funding, OI, and volume for every linear perpetual.
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
class BybitAdapterConfig:
    base_url: str = "https://api.bybit.com"
    assets: Optional[Sequence[str]] = None
    timeout_seconds: float = 5.0
    top_book_markets: int = 10  # Only fetch orderbooks for top N markets by volume


class BybitApiError(RuntimeError):
    pass


class BybitClient:
    def __init__(self, config: BybitAdapterConfig | None = None) -> None:
        self.config = config or BybitAdapterConfig()

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


class BybitAdapter(MarketDataAdapter):
    def __init__(
        self,
        config: BybitAdapterConfig | None = None,
        client: BybitClient | None = None,
    ) -> None:
        self.config = config or BybitAdapterConfig()
        self.client = client or BybitClient(self.config)
        self.name = "bybit"

    def poll(self) -> Sequence[MarketSnapshot]:
        # 1: Single batch request for ALL linear perp tickers
        tickers_resp, tickers_latency = self.client.get(
            "v5/market/tickers?category=linear"
        )
        tickers = self._parse_tickers(tickers_resp)
        selected = self._select_tickers(tickers)

        # 2: Rank by 24h turnover, only fetch orderbooks for top N
        top_symbols = self._rank_top_markets(selected)

        snapshots: List[MarketSnapshot] = []
        for symbol, ticker in selected.items():
            try:
                if symbol in top_symbols:
                    book_resp, book_latency = self.client.get(
                        f"v5/market/orderbook?category=linear&symbol={symbol}&limit=20"
                    )
                    snapshot = self._build_snapshot(
                        ticker, book_resp, max(tickers_latency, book_latency),
                    )
                else:
                    snapshot = self._build_snapshot_from_ticker(
                        ticker, tickers_latency,
                    )
                snapshots.append(snapshot)
            except Exception:
                continue
        return snapshots

    def _parse_tickers(self, response: object) -> Dict[str, dict]:
        """Parse tickers response into {symbol: ticker_data}."""
        if not isinstance(response, dict):
            raise BybitApiError("Unexpected tickers response shape")
        result = response.get("result")
        if not isinstance(result, dict):
            raise BybitApiError("Tickers response missing result")
        ticker_list = result.get("list", [])
        parsed: Dict[str, dict] = {}
        for item in ticker_list:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol", "")
            # Only USDT linear perps
            if not symbol.endswith("USDT"):
                continue
            parsed[symbol] = item
        return parsed

    def _select_tickers(self, tickers: Dict[str, dict]) -> Dict[str, dict]:
        if not self.config.assets:
            return tickers
        asset_set = {a.upper() for a in self.config.assets}
        return {
            sym: data for sym, data in tickers.items()
            if sym.replace("USDT", "") in asset_set
        }

    def _rank_top_markets(self, tickers: Dict[str, dict]) -> set:
        """Return top N by volume ∪ top N by OI (deduplicated)."""
        n = self.config.top_book_markets
        syms = list(tickers.keys())
        vol_list = sorted(syms, key=lambda s: _coerce_float(tickers[s].get("turnover24h")) or 0.0, reverse=True)
        oi_list = sorted(syms, key=lambda s: _coerce_float(tickers[s].get("openInterest")) or 0.0, reverse=True)
        return set(vol_list[:n]) | set(oi_list[:n])

    def _build_snapshot(
        self,
        ticker: dict,
        book_resp: object,
        latency_ms: float,
    ) -> MarketSnapshot:
        symbol = ticker["symbol"]
        asset = symbol.replace("USDT", "")

        # Parse orderbook
        bids, asks = self._parse_book(book_resp)

        # Prices from ticker
        last_price = _coerce_float(ticker.get("lastPrice")) or 0.0
        best_bid = _coerce_float(ticker.get("bid1Price")) or (bids[0][0] if bids else last_price)
        best_ask = _coerce_float(ticker.get("ask1Price")) or (asks[0][0] if asks else last_price)
        mark_price = _coerce_float(ticker.get("markPrice")) or last_price
        index_price = _coerce_float(ticker.get("indexPrice")) or mark_price
        oracle_price = index_price

        mid_price = (best_bid + best_ask) / 2.0 if (best_bid and best_ask) else mark_price

        # OI (in contracts/coins — multiply by mark_price)
        open_interest = _coerce_float(ticker.get("openInterest")) or 0.0
        oi_usd = open_interest * mark_price

        # Volume
        turnover_24h = _coerce_float(ticker.get("turnover24h")) or 0.0

        # Funding
        funding_rate = _coerce_float(ticker.get("fundingRate")) or 0.0
        funding_rate_bps = funding_rate * 10_000.0
        funding_interval_hours = _coerce_float(ticker.get("fundingIntervalHour")) or 8.0
        next_funding_time = _parse_millis_timestamp(ticker.get("nextFundingTime"))

        # Price change for realized vol proxy
        prev_price_24h = _coerce_float(ticker.get("prevPrice24h")) or mark_price
        realized_vol = abs(math.log(mark_price / prev_price_24h)) * 100.0 if prev_price_24h > 0 else 0.0

        # Premium (mark vs index)
        premium_bps = 0.0
        if index_price > 0:
            premium_bps = abs((mark_price - index_price) / index_price) * 10_000.0

        # Depth metrics from orderbook
        if bids and asks:
            symmetric_depth = min(self._book_notional(bids), self._book_notional(asks))
            top_1pct_depth = min(
                self._book_notional_within_band(bids, mid_price, "bid", 0.01),
                self._book_notional_within_band(asks, mid_price, "ask", 0.01),
            )
            impact_10k = self._estimate_roundtrip_impact_bps(bids, asks, mid_price, 10_000.0)
            impact_50k = self._estimate_roundtrip_impact_bps(bids, asks, mid_price, 50_000.0)
        else:
            symmetric_depth = 0.0
            top_1pct_depth = 0.0
            impact_10k = 50.0
            impact_50k = 50.0

        volume_depth_ratio = turnover_24h / max(top_1pct_depth, 1.0)
        timestamp = datetime.now(timezone.utc)

        return MarketSnapshot(
            venue="bybit",
            market_type="perp_cex",
            asset=asset,
            quote="USDT",
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=oracle_price,
            index_price=index_price,
            taker_fee_bps=5.5,
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
                "source": "bybit",
                "symbol": symbol,
                "turnover_24h_usd": turnover_24h,
                "funding_interval_hours": funding_interval_hours,
                "_book_bids": bids,
                "_book_asks": asks,
            },
        )

    def _build_snapshot_from_ticker(
        self,
        ticker: dict,
        latency_ms: float,
    ) -> MarketSnapshot:
        """Build snapshot using only ticker data (no orderbook request)."""
        symbol = ticker["symbol"]
        asset = symbol.replace("USDT", "")

        last_price = _coerce_float(ticker.get("lastPrice")) or 0.0
        best_bid = _coerce_float(ticker.get("bid1Price")) or last_price
        best_ask = _coerce_float(ticker.get("ask1Price")) or last_price
        mark_price = _coerce_float(ticker.get("markPrice")) or last_price
        index_price = _coerce_float(ticker.get("indexPrice")) or mark_price
        oracle_price = index_price

        if mark_price <= 0:
            raise BybitApiError(f"No price for {symbol}")

        open_interest = _coerce_float(ticker.get("openInterest")) or 0.0
        oi_usd = open_interest * mark_price
        turnover_24h = _coerce_float(ticker.get("turnover24h")) or 0.0

        funding_rate = _coerce_float(ticker.get("fundingRate")) or 0.0
        funding_rate_bps = funding_rate * 10_000.0
        funding_interval_hours = _coerce_float(ticker.get("fundingIntervalHour")) or 8.0
        next_funding_time = _parse_millis_timestamp(ticker.get("nextFundingTime"))

        prev_price_24h = _coerce_float(ticker.get("prevPrice24h")) or mark_price
        realized_vol = abs(math.log(mark_price / prev_price_24h)) * 100.0 if prev_price_24h > 0 else 0.0

        premium_bps = 0.0
        if index_price > 0:
            premium_bps = abs((mark_price - index_price) / index_price) * 10_000.0

        return MarketSnapshot(
            venue="bybit",
            market_type="perp_cex",
            asset=asset,
            quote="USDT",
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=oracle_price,
            index_price=index_price,
            taker_fee_bps=5.5,
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
                "source": "bybit",
                "symbol": symbol,
                "turnover_24h_usd": turnover_24h,
                "funding_interval_hours": funding_interval_hours,
                "ticker_only": True,
            },
        )

    @staticmethod
    def _parse_book(
        book_resp: object,
    ) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        """Parse Bybit V5 orderbook response.

        Response shape: {"result": {"b": [["price","size"], ...], "a": [["price","size"], ...]}}
        """
        if not isinstance(book_resp, dict):
            return [], []
        result = book_resp.get("result")
        if not isinstance(result, dict):
            return [], []
        bids = [
            (float(level[0]), float(level[1]))
            for level in result.get("b", [])
            if isinstance(level, (list, tuple)) and len(level) >= 2
        ]
        asks = [
            (float(level[0]), float(level[1]))
            for level in result.get("a", [])
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


def _parse_millis_timestamp(value: object) -> Optional[datetime]:
    try:
        millis = int(str(value))
    except (TypeError, ValueError):
        return None
    if millis <= 0:
        return None
    return datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc)
