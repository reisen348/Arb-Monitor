"""OKX perpetual CEX REST adapter.

Public endpoints (no auth required):
  GET /api/v5/market/tickers?instType=SWAP          → all swap tickers
  GET /api/v5/public/mark-price?instType=SWAP        → all mark prices
  GET /api/v5/public/open-interest?instType=SWAP      → all open interest
  GET /api/v5/public/funding-rate?instId=XXX          → funding rate per symbol
  GET /api/v5/market/books?instId=XXX&sz=20           → orderbook

OKX response format: {"code": "0", "data": [...]}
Symbol format: BTC-USDT-SWAP, ETH-USDT-SWAP
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
class OkxAdapterConfig:
    base_url: str = "https://www.okx.com"
    assets: Optional[Sequence[str]] = None
    timeout_seconds: float = 5.0
    book_depth: int = 20
    top_book_markets: int = 10  # Only fetch orderbooks for top N markets by volume


class OkxApiError(RuntimeError):
    pass


class OkxClient:
    def __init__(self, config: OkxAdapterConfig | None = None) -> None:
        self.config = config or OkxAdapterConfig()

    def get(self, path: str) -> Tuple[object, float]:
        url = f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"
        req = request.Request(url, method="GET", headers={
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; perp-arb/0.1)",
        })
        started = time.perf_counter()
        with request.urlopen(req, timeout=self.config.timeout_seconds) as response:
            raw = response.read()
        latency_ms = (time.perf_counter() - started) * 1000.0
        return json.loads(raw.decode("utf-8")), latency_ms


class OkxAdapter(MarketDataAdapter):
    def __init__(
        self,
        config: OkxAdapterConfig | None = None,
        client: OkxClient | None = None,
    ) -> None:
        self.config = config or OkxAdapterConfig()
        self.client = client or OkxClient(self.config)
        self.name = "okx"

    def poll(self) -> Sequence[MarketSnapshot]:
        # 1. Batch: all swap tickers → bid/ask/volume
        tickers_resp, tickers_latency = self.client.get(
            "api/v5/market/tickers?instType=SWAP"
        )
        tickers = self._parse_tickers(tickers_resp)

        # 2. Batch: all mark prices
        mark_resp, mark_latency = self.client.get(
            "api/v5/public/mark-price?instType=SWAP"
        )
        mark_map = self._parse_mark_prices(mark_resp)

        # 3. Batch: all open interest
        oi_resp, oi_latency = self.client.get(
            "api/v5/public/open-interest?instType=SWAP"
        )
        oi_map = self._parse_open_interest(oi_resp)

        batch_latency = max(tickers_latency, mark_latency, oi_latency)

        # Filter to USDT-SWAP symbols only
        selected = self._select_symbols(tickers)

        # 4. Rank by volume ∪ OI, only fetch orderbooks + funding for top N
        top_symbols = self._rank_top_markets(selected, tickers, oi_map)

        # Fetch funding rates only for top N symbols (avoid per-symbol rate limiting)
        funding_map: Dict[str, float] = {}
        for inst_id in top_symbols:
            try:
                fr_resp, fr_latency = self.client.get(
                    f"api/v5/public/funding-rate?instId={inst_id}"
                )
                batch_latency = max(batch_latency, fr_latency)
                funding_map[inst_id] = self._parse_funding_rate(fr_resp)
            except Exception:
                funding_map[inst_id] = 0.0

        snapshots: List[MarketSnapshot] = []
        for inst_id in selected:
            ticker = tickers[inst_id]
            mark_px = mark_map.get(inst_id)
            oi_val = oi_map.get(inst_id, 0.0)
            funding_rate_bps = funding_map.get(inst_id, 0.0)
            asset = inst_id.split("-")[0]
            try:
                if inst_id in top_symbols:
                    book_resp, book_latency = self.client.get(
                        f"api/v5/market/books?instId={inst_id}&sz={self.config.book_depth}"
                    )
                    snapshot = self._build_snapshot(
                        asset, inst_id, ticker, mark_px, oi_val,
                        funding_rate_bps, book_resp,
                        max(batch_latency, book_latency),
                    )
                else:
                    snapshot = self._build_snapshot_from_ticker(
                        asset, inst_id, ticker, mark_px, oi_val,
                        funding_rate_bps, batch_latency,
                    )
                snapshots.append(snapshot)
            except Exception:
                continue
        return snapshots

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_tickers(response: object) -> Dict[str, dict]:
        """Parse /api/v5/market/tickers response into {instId: ticker_data}."""
        if not isinstance(response, dict) or response.get("code") != "0":
            raise OkxApiError("Unexpected tickers response")
        parsed: Dict[str, dict] = {}
        for item in response.get("data", []):
            if not isinstance(item, dict):
                continue
            inst_id = item.get("instId", "")
            if inst_id.endswith("-USDT-SWAP"):
                parsed[inst_id] = item
        return parsed

    @staticmethod
    def _parse_mark_prices(response: object) -> Dict[str, Optional[float]]:
        """Parse /api/v5/public/mark-price response into {instId: mark_price}."""
        if not isinstance(response, dict) or response.get("code") != "0":
            return {}
        parsed: Dict[str, Optional[float]] = {}
        for item in response.get("data", []):
            if not isinstance(item, dict):
                continue
            inst_id = item.get("instId", "")
            parsed[inst_id] = _coerce_float(item.get("markPx"))
        return parsed

    @staticmethod
    def _parse_open_interest(response: object) -> Dict[str, float]:
        """Parse /api/v5/public/open-interest response into {instId: oi_usd}.

        Uses the ``oiUsd`` field returned by OKX which already accounts for
        the per-contract value (``ctVal``).  Falling back to raw ``oi`` would
        be wrong because contract sizes vary wildly across instruments
        (e.g. BTC ctVal=0.01, SHIB ctVal=1000000).
        """
        if not isinstance(response, dict) or response.get("code") != "0":
            return {}
        parsed: Dict[str, float] = {}
        for item in response.get("data", []):
            if not isinstance(item, dict):
                continue
            inst_id = item.get("instId", "")
            oi_usd = _coerce_float(item.get("oiUsd")) or 0.0
            parsed[inst_id] = oi_usd
        return parsed

    @staticmethod
    def _parse_funding_rate(response: object) -> float:
        """Parse /api/v5/public/funding-rate response → funding rate in bps."""
        if not isinstance(response, dict) or response.get("code") != "0":
            return 0.0
        data = response.get("data", [])
        if not data or not isinstance(data[0], dict):
            return 0.0
        rate = _coerce_float(data[0].get("fundingRate")) or 0.0
        # OKX returns funding as decimal (e.g. 0.0001), convert to bps
        return rate * 10_000.0

    @staticmethod
    def _parse_next_funding_time(response_item: dict) -> Optional[datetime]:
        """Extract next funding time from a funding rate data item."""
        ts = _coerce_float(response_item.get("nextFundingTime"))
        if ts is not None and ts > 0:
            return datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
        return None

    def _select_symbols(self, tickers: Dict[str, dict]) -> List[str]:
        """Return list of instIds to include, filtered by config.assets."""
        if not self.config.assets:
            return sorted(tickers.keys())
        asset_set = {a.upper() for a in self.config.assets}
        return sorted(
            inst_id for inst_id in tickers
            if inst_id.split("-")[0].upper() in asset_set
        )

    def _rank_top_markets(self, symbols: List[str], tickers: Dict[str, dict], oi_map: Dict[str, float]) -> set:
        """Return top N by volume ∪ top N by OI (deduplicated)."""
        n = self.config.top_book_markets
        vol_list = sorted(
            symbols,
            key=lambda s: _coerce_float(tickers.get(s, {}).get("volCcy24h")) or 0.0,
            reverse=True,
        )
        oi_list = sorted(
            symbols,
            key=lambda s: oi_map.get(s, 0.0),
            reverse=True,
        )
        return set(vol_list[:n]) | set(oi_list[:n])

    # ------------------------------------------------------------------
    # Orderbook parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_book(response: object) -> Tuple[
        List[Tuple[float, float]], List[Tuple[float, float]], Optional[float]
    ]:
        """Parse /api/v5/market/books response.

        OKX book format: {"code": "0", "data": [{"bids": [[px, sz, _, _], ...],
                                                   "asks": [[px, sz, _, _], ...],
                                                   "ts": "1234567890123"}]}
        Returns (bids, asks, timestamp_ms).
        """
        if not isinstance(response, dict) or response.get("code") != "0":
            return [], [], None
        data = response.get("data", [])
        if not data or not isinstance(data[0], dict):
            return [], [], None
        book = data[0]
        bids = []
        for level in book.get("bids", []):
            if isinstance(level, (list, tuple)) and len(level) >= 2:
                px = _coerce_float(level[0])
                sz = _coerce_float(level[1])
                if px is not None and sz is not None and px > 0 and sz > 0:
                    bids.append((px, sz))
        asks = []
        for level in book.get("asks", []):
            if isinstance(level, (list, tuple)) and len(level) >= 2:
                px = _coerce_float(level[0])
                sz = _coerce_float(level[1])
                if px is not None and sz is not None and px > 0 and sz > 0:
                    asks.append((px, sz))
        ts = _coerce_float(book.get("ts"))
        return bids, asks, ts

    # ------------------------------------------------------------------
    # Snapshot builders
    # ------------------------------------------------------------------

    def _build_snapshot(
        self,
        asset: str,
        inst_id: str,
        ticker: dict,
        mark_px_from_batch: Optional[float],
        oi_usd: float,
        funding_rate_bps: float,
        book_resp: object,
        latency_ms: float,
    ) -> MarketSnapshot:
        bids, asks, book_ts_ms = self._parse_book(book_resp)
        if not bids or not asks:
            raise OkxApiError(f"OKX orderbook for {inst_id} is empty")

        best_bid = bids[0][0]
        best_ask = asks[0][0]
        mid_px = (best_bid + best_ask) / 2.0

        last_px = _coerce_float(ticker.get("last")) or mid_px
        mark_px = mark_px_from_batch or last_px
        oracle_px = mark_px
        index_px = mark_px

        # Volume: volCcy24h is quote-denominated 24h volume
        volume_24h = _coerce_float(ticker.get("volCcy24h")) or 0.0

        # Staleness
        staleness_ms = 0.0
        timestamp = datetime.now(timezone.utc)
        if book_ts_ms is not None:
            staleness_ms = max(0.0, timestamp.timestamp() * 1000.0 - book_ts_ms)
            timestamp = datetime.fromtimestamp(book_ts_ms / 1000.0, tz=timezone.utc)

        # Depth metrics
        symmetric_depth = min(self._book_notional(bids), self._book_notional(asks))
        top_1pct_depth = min(
            self._book_notional_within_band(bids, mid_px, "bid", 0.01),
            self._book_notional_within_band(asks, mid_px, "ask", 0.01),
        )
        impact_10k = self._estimate_roundtrip_impact_bps(bids, asks, mid_px, 10_000.0)
        impact_50k = self._estimate_roundtrip_impact_bps(bids, asks, mid_px, 50_000.0)
        volume_depth_ratio = volume_24h / max(top_1pct_depth, 1.0)

        # Realized vol proxy from 24h open/last
        open_px = _coerce_float(ticker.get("open24h")) or last_px
        realized_vol = abs(math.log(last_px / open_px)) * 100.0 if open_px > 0 else 0.0

        return MarketSnapshot(
            venue="okx",
            market_type="perp_cex",
            asset=asset,
            quote="USDT",
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_px,
            oracle_price=oracle_px,
            index_price=index_px,
            taker_fee_bps=5.0,
            maker_fee_bps=2.0,
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
            staleness_ms=staleness_ms,
            timestamp=timestamp,
            metadata={
                "source": "okx",
                "inst_id": inst_id,
                "funding_interval_hours": 8.0,
                "day_volume_usd": volume_24h,
                "_book_bids": bids,
                "_book_asks": asks,
            },
        )

    def _build_snapshot_from_ticker(
        self,
        asset: str,
        inst_id: str,
        ticker: dict,
        mark_px_from_batch: Optional[float],
        oi_usd: float,
        funding_rate_bps: float,
        latency_ms: float,
    ) -> MarketSnapshot:
        """Build snapshot using only batch data (no orderbook request)."""
        best_bid = _coerce_float(ticker.get("bidPx")) or 0.0
        best_ask = _coerce_float(ticker.get("askPx")) or 0.0
        last_px = _coerce_float(ticker.get("last")) or 0.0

        if best_bid <= 0 and best_ask <= 0 and last_px <= 0:
            raise OkxApiError(f"No usable price for {inst_id}")

        if best_bid <= 0:
            best_bid = last_px
        if best_ask <= 0:
            best_ask = last_px

        mark_px = mark_px_from_batch or last_px or (best_bid + best_ask) / 2.0
        volume_24h = _coerce_float(ticker.get("volCcy24h")) or 0.0

        open_px = _coerce_float(ticker.get("open24h")) or last_px
        realized_vol = abs(math.log(last_px / open_px)) * 100.0 if open_px > 0 and last_px > 0 else 0.0

        return MarketSnapshot(
            venue="okx",
            market_type="perp_cex",
            asset=asset,
            quote="USDT",
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_px,
            oracle_price=mark_px,
            index_price=mark_px,
            taker_fee_bps=5.0,
            maker_fee_bps=2.0,
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
                "source": "okx",
                "inst_id": inst_id,
                "funding_interval_hours": 8.0,
                "day_volume_usd": volume_24h,
                "ticker_only": True,
            },
        )

    # ------------------------------------------------------------------
    # Depth / impact static methods
    # ------------------------------------------------------------------

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


# ------------------------------------------------------------------
# Module-level helper functions
# ------------------------------------------------------------------

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
