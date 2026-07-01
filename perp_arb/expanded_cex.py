"""Additional CEX REST adapters with broad market coverage.

These adapters prefer batch ticker/funding endpoints for full-market coverage
and fetch full orderbooks only for the configured top markets.
"""
from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Tuple
from urllib import parse, request

from .market_data import MarketDataAdapter, MarketSnapshot


_STABLE_QUOTES = {"USD", "USDT", "USDC", "USDE", "USD1", "USDT0"}
_BITGET_STOCK_SUFFIX = "STOCK"
_GATE_CONTRACT_ASSET_ALIASES = {
    "EDGEX_USDT": "EDGE",
    "EDGE_USDT": "GATE_EDGE",
}


@dataclass(frozen=True)
class BitgetAdapterConfig:
    base_url: str = "https://api.bitget.com"
    assets: Optional[Sequence[str]] = None
    timeout_seconds: float = 5.0
    top_book_markets: int = 10
    product_type: str = "USDT-FUTURES"


@dataclass(frozen=True)
class GateAdapterConfig:
    base_url: str = "https://api.gateio.ws"
    assets: Optional[Sequence[str]] = None
    timeout_seconds: float = 5.0
    top_book_markets: int = 10
    settle: str = "usdt"


@dataclass(frozen=True)
class KrakenAdapterConfig:
    base_url: str = "https://futures.kraken.com"
    assets: Optional[Sequence[str]] = None
    timeout_seconds: float = 5.0
    top_book_markets: int = 10


@dataclass(frozen=True)
class AsterAdapterConfig:
    base_url: str = "https://fapi.asterdex.com"
    assets: Optional[Sequence[str]] = None
    timeout_seconds: float = 5.0
    top_book_markets: int = 10


class ExpandedCexApiError(RuntimeError):
    pass


class BitgetApiError(ExpandedCexApiError):
    pass


class GateApiError(ExpandedCexApiError):
    pass


class KrakenApiError(ExpandedCexApiError):
    pass


class AsterApiError(ExpandedCexApiError):
    pass


class _JsonHttpClient:
    def __init__(self, base_url: str, timeout_seconds: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def get(self, path: str, params: Optional[dict] = None) -> Tuple[object, float]:
        url = f"{self.base_url}/{path.lstrip('/')}"
        if params:
            url = f"{url}?{parse.urlencode(params)}"
        req = request.Request(url, method="GET", headers={"Accept": "application/json"})
        started = time.perf_counter()
        with request.urlopen(req, timeout=self.timeout_seconds) as response:
            raw = response.read()
        latency_ms = (time.perf_counter() - started) * 1000.0
        return json.loads(raw.decode("utf-8")), latency_ms


class BitgetClient(_JsonHttpClient):
    def __init__(self, config: BitgetAdapterConfig | None = None) -> None:
        config = config or BitgetAdapterConfig()
        super().__init__(config.base_url, config.timeout_seconds)


class GateClient(_JsonHttpClient):
    def __init__(self, config: GateAdapterConfig | None = None) -> None:
        config = config or GateAdapterConfig()
        super().__init__(config.base_url, config.timeout_seconds)


class KrakenClient(_JsonHttpClient):
    def __init__(self, config: KrakenAdapterConfig | None = None) -> None:
        config = config or KrakenAdapterConfig()
        super().__init__(config.base_url, config.timeout_seconds)


class AsterClient(_JsonHttpClient):
    def __init__(self, config: AsterAdapterConfig | None = None) -> None:
        config = config or AsterAdapterConfig()
        super().__init__(config.base_url, config.timeout_seconds)


class BitgetAdapter(MarketDataAdapter):
    def __init__(self, config: BitgetAdapterConfig | None = None, client: BitgetClient | None = None) -> None:
        self.config = config or BitgetAdapterConfig()
        self.client = client or BitgetClient(self.config)
        self.name = "bitget"

    def poll(self) -> Sequence[MarketSnapshot]:
        ticker_resp, ticker_latency = self.client.get(
            "api/v2/mix/market/tickers",
            {"productType": self.config.product_type},
        )
        ticker_map = self._parse_tickers(ticker_resp)
        symbols = self._select_symbols(ticker_map)
        top_symbols = self._rank_top_markets(symbols, ticker_map)

        snapshots: List[MarketSnapshot] = []
        for symbol in symbols:
            ticker = ticker_map[symbol]
            try:
                if symbol in top_symbols:
                    try:
                        book_resp, book_latency = self.client.get(
                            "api/v2/mix/market/merge-depth",
                            {"symbol": symbol, "productType": self.config.product_type, "limit": "20"},
                        )
                        snapshots.append(self._build_snapshot(symbol, ticker, book_resp, max(ticker_latency, book_latency)))
                        continue
                    except Exception:
                        pass
                snapshots.append(self._build_snapshot_from_ticker(symbol, ticker, ticker_latency))
            except Exception:
                continue
        return snapshots

    @staticmethod
    def _parse_tickers(response: object) -> Dict[str, dict]:
        data = _bitget_data(response)
        if not isinstance(data, list):
            raise BitgetApiError("Unexpected Bitget tickers response shape")
        parsed: Dict[str, dict] = {}
        for item in data:
            if isinstance(item, dict):
                symbol = str(item.get("symbol") or "").upper()
                if symbol:
                    parsed[symbol] = item
        return parsed

    def _select_symbols(self, ticker_map: Dict[str, dict]) -> List[str]:
        symbols = [s for s in sorted(ticker_map) if _symbol_quote(s) in _STABLE_QUOTES]
        if self.config.assets:
            assets = {asset.upper() for asset in self.config.assets}
            symbols = [s for s in symbols if _bitget_display_asset(s) in assets]
        return symbols

    def _rank_top_markets(self, symbols: List[str], ticker_map: Dict[str, dict]) -> set:
        n = max(int(self.config.top_book_markets), 0)
        if n <= 0:
            return set()
        vol_list = sorted(symbols, key=lambda s: _coerce_float(ticker_map[s].get("quoteVolume")) or 0.0, reverse=True)
        oi_list = sorted(
            symbols,
            key=lambda s: (_coerce_float(ticker_map[s].get("holdingAmount")) or 0.0)
            * (_coerce_float(ticker_map[s].get("markPrice")) or _coerce_float(ticker_map[s].get("lastPr")) or 0.0),
            reverse=True,
        )
        return set(vol_list[:n]) | set(oi_list[:n])

    def _build_snapshot(self, symbol: str, ticker: dict, book_resp: object, latency_ms: float) -> MarketSnapshot:
        bids, asks = _parse_array_book(_bitget_data(book_resp))
        if not bids or not asks:
            return self._build_snapshot_from_ticker(symbol, ticker, latency_ms)
        best_bid = bids[0][0]
        best_ask = asks[0][0]
        return self._snapshot(symbol, ticker, best_bid, best_ask, bids, asks, latency_ms, ticker_only=False)

    def _build_snapshot_from_ticker(self, symbol: str, ticker: dict, latency_ms: float) -> MarketSnapshot:
        mark_price = _coerce_float(ticker.get("markPrice")) or _coerce_float(ticker.get("lastPr")) or 0.0
        if mark_price <= 0:
            raise BitgetApiError(f"No price for {symbol}")
        best_bid = _coerce_float(ticker.get("bidPr")) or mark_price
        best_ask = _coerce_float(ticker.get("askPr")) or mark_price
        return self._snapshot(symbol, ticker, best_bid, best_ask, [], [], latency_ms, ticker_only=True)

    def _snapshot(
        self,
        symbol: str,
        ticker: dict,
        best_bid: float,
        best_ask: float,
        bids: Sequence[Tuple[float, float]],
        asks: Sequence[Tuple[float, float]],
        latency_ms: float,
        ticker_only: bool,
    ) -> MarketSnapshot:
        asset = _bitget_display_asset(symbol)
        mark_price = _coerce_float(ticker.get("markPrice")) or _coerce_float(ticker.get("lastPr")) or (best_bid + best_ask) / 2.0
        index_price = _coerce_float(ticker.get("indexPrice")) or mark_price
        volume_24h = _coerce_float(ticker.get("usdtVolume")) or _coerce_float(ticker.get("quoteVolume")) or 0.0
        oi_base = _coerce_float(ticker.get("holdingAmount")) or _coerce_float(ticker.get("openInterest")) or 0.0
        oi_usd = _coerce_float(ticker.get("openInterestValue")) or oi_base * mark_price
        if oi_usd <= 0 and volume_24h > 0:
            oi_usd = volume_24h * 0.1
        funding_rate_bps = (_coerce_float(ticker.get("fundingRate")) or 0.0) * 10_000.0
        prev_price = _coerce_float(ticker.get("open24h")) or _coerce_float(ticker.get("openUtc")) or mark_price
        realized_vol = abs(math.log(mark_price / prev_price)) * 100.0 if prev_price > 0 and mark_price > 0 else 0.0
        mid_price = (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else mark_price
        metrics = _book_metrics(bids, asks, mid_price) if bids and asks else _empty_book_metrics()
        metadata = {
            "source": "bitget",
            "symbol": symbol,
            "day_quote_volume_usd": volume_24h,
            "funding_interval_hours": 8.0,
        }
        if _bitget_is_stock_symbol(symbol):
            metadata["stock_like"] = True
        if ticker_only:
            metadata["ticker_only"] = True
        return MarketSnapshot(
            venue="bitget",
            market_type="perp_cex",
            asset=asset,
            quote=_symbol_quote(symbol),
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=index_price,
            index_price=index_price,
            taker_fee_bps=6.0,
            maker_fee_bps=2.0,
            depth_10k_usd=metrics["depth_10k_usd"],
            depth_50k_usd=metrics["depth_50k_usd"],
            top_1pct_depth_usd=metrics["top_1pct_depth_usd"],
            volume_depth_ratio=volume_24h / max(metrics["top_1pct_depth_usd"], 1.0) if metrics["top_1pct_depth_usd"] > 0 else 0.0,
            oi_usd=oi_usd,
            funding_rate_bps=funding_rate_bps,
            next_funding_time=_next_interval_time(8),
            impact_cost_10k_bps=metrics["impact_10k_bps"],
            impact_cost_50k_bps=metrics["impact_50k_bps"],
            slippage_bps=metrics["slippage_bps"],
            realized_vol=realized_vol,
            spread_zscore=abs(mark_price - index_price) / max(index_price, 1e-9) * 1000.0,
            latency_ms=latency_ms,
            timestamp=datetime.now(timezone.utc),
            metadata=metadata,
        )


class GateAdapter(MarketDataAdapter):
    def __init__(self, config: GateAdapterConfig | None = None, client: GateClient | None = None) -> None:
        self.config = config or GateAdapterConfig()
        self.client = client or GateClient(self.config)
        self.name = "gate"

    def poll(self) -> Sequence[MarketSnapshot]:
        settle = self.config.settle.lower()
        ticker_resp, ticker_latency = self.client.get(f"api/v4/futures/{settle}/tickers")
        ticker_map = self._parse_tickers(ticker_resp)
        contracts = self._select_contracts(ticker_map)
        top_contracts = self._rank_top_markets(contracts, ticker_map)

        snapshots: List[MarketSnapshot] = []
        for contract in contracts:
            ticker = ticker_map[contract]
            try:
                if contract in top_contracts:
                    try:
                        book_resp, book_latency = self.client.get(
                            f"api/v4/futures/{settle}/order_book",
                            {"contract": contract, "limit": "20"},
                        )
                        snapshots.append(self._build_snapshot(contract, ticker, book_resp, max(ticker_latency, book_latency)))
                        continue
                    except Exception:
                        pass
                snapshots.append(self._build_snapshot_from_ticker(contract, ticker, ticker_latency))
            except Exception:
                continue
        return snapshots

    @staticmethod
    def _parse_tickers(response: object) -> Dict[str, dict]:
        if not isinstance(response, list):
            raise GateApiError("Unexpected Gate tickers response shape")
        parsed: Dict[str, dict] = {}
        for item in response:
            if isinstance(item, dict):
                contract = str(item.get("contract") or "").upper()
                if contract:
                    parsed[contract] = item
        return parsed

    def _select_contracts(self, ticker_map: Dict[str, dict]) -> List[str]:
        contracts = []
        for contract in sorted(ticker_map):
            asset, quote = _gate_contract_parts(contract)
            if quote not in _STABLE_QUOTES:
                continue
            if self.config.assets and asset not in {a.upper() for a in self.config.assets}:
                continue
            contracts.append(contract)
        return contracts

    def _rank_top_markets(self, contracts: List[str], ticker_map: Dict[str, dict]) -> set:
        n = max(int(self.config.top_book_markets), 0)
        if n <= 0:
            return set()
        vol_list = sorted(contracts, key=lambda c: _gate_volume_usd(ticker_map[c]), reverse=True)
        oi_list = sorted(contracts, key=lambda c: _gate_oi_usd(ticker_map[c]), reverse=True)
        return set(vol_list[:n]) | set(oi_list[:n])

    def _build_snapshot(self, contract: str, ticker: dict, book_resp: object, latency_ms: float) -> MarketSnapshot:
        multiplier = _coerce_float(ticker.get("quanto_multiplier")) or 1.0
        bids, asks = _parse_gate_book(book_resp, multiplier)
        if not bids or not asks:
            return self._build_snapshot_from_ticker(contract, ticker, latency_ms)
        return self._snapshot(contract, ticker, bids[0][0], asks[0][0], bids, asks, latency_ms, ticker_only=False)

    def _build_snapshot_from_ticker(self, contract: str, ticker: dict, latency_ms: float) -> MarketSnapshot:
        mark_price = _coerce_float(ticker.get("mark_price")) or _coerce_float(ticker.get("last")) or 0.0
        if mark_price <= 0:
            raise GateApiError(f"No price for {contract}")
        best_bid = _coerce_float(ticker.get("highest_bid")) or mark_price
        best_ask = _coerce_float(ticker.get("lowest_ask")) or mark_price
        return self._snapshot(contract, ticker, best_bid, best_ask, [], [], latency_ms, ticker_only=True)

    def _snapshot(
        self,
        contract: str,
        ticker: dict,
        best_bid: float,
        best_ask: float,
        bids: Sequence[Tuple[float, float]],
        asks: Sequence[Tuple[float, float]],
        latency_ms: float,
        ticker_only: bool,
    ) -> MarketSnapshot:
        asset, quote = _gate_contract_parts(contract)
        mark_price = _coerce_float(ticker.get("mark_price")) or _coerce_float(ticker.get("last")) or (best_bid + best_ask) / 2.0
        index_price = _coerce_float(ticker.get("index_price")) or mark_price
        volume_24h = _gate_volume_usd(ticker)
        oi_usd = _gate_oi_usd(ticker)
        if oi_usd <= 0 and volume_24h > 0:
            oi_usd = volume_24h * 0.1
        funding_rate_bps = (_coerce_float(ticker.get("funding_rate")) or _coerce_float(ticker.get("funding_rate_indicative")) or 0.0) * 10_000.0
        change_pct = (_coerce_float(ticker.get("change_percentage")) or 0.0) / 100.0
        prev_price = mark_price / (1.0 + change_pct) if change_pct > -0.99 else mark_price
        realized_vol = abs(math.log(mark_price / prev_price)) * 100.0 if prev_price > 0 and mark_price > 0 else 0.0
        mid_price = (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else mark_price
        metrics = _book_metrics(bids, asks, mid_price) if bids and asks else _empty_book_metrics()
        metadata = {
            "source": "gate",
            "symbol": contract,
            "day_quote_volume_usd": volume_24h,
            "funding_interval_hours": 8.0,
            "quanto_multiplier": _coerce_float(ticker.get("quanto_multiplier")) or 1.0,
        }
        if ticker_only:
            metadata["ticker_only"] = True
        return MarketSnapshot(
            venue="gate",
            market_type="perp_cex",
            asset=asset,
            quote=quote,
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=index_price,
            index_price=index_price,
            taker_fee_bps=5.0,
            maker_fee_bps=1.5,
            depth_10k_usd=metrics["depth_10k_usd"],
            depth_50k_usd=metrics["depth_50k_usd"],
            top_1pct_depth_usd=metrics["top_1pct_depth_usd"],
            volume_depth_ratio=volume_24h / max(metrics["top_1pct_depth_usd"], 1.0) if metrics["top_1pct_depth_usd"] > 0 else 0.0,
            oi_usd=oi_usd,
            funding_rate_bps=funding_rate_bps,
            next_funding_time=_next_interval_time(8),
            impact_cost_10k_bps=metrics["impact_10k_bps"],
            impact_cost_50k_bps=metrics["impact_50k_bps"],
            slippage_bps=metrics["slippage_bps"],
            realized_vol=realized_vol,
            spread_zscore=abs(mark_price - index_price) / max(index_price, 1e-9) * 1000.0,
            latency_ms=latency_ms,
            timestamp=datetime.now(timezone.utc),
            metadata=metadata,
        )


class KrakenAdapter(MarketDataAdapter):
    def __init__(self, config: KrakenAdapterConfig | None = None, client: KrakenClient | None = None) -> None:
        self.config = config or KrakenAdapterConfig()
        self.client = client or KrakenClient(self.config)
        self.name = "kraken"

    def poll(self) -> Sequence[MarketSnapshot]:
        ticker_resp, ticker_latency = self.client.get("derivatives/api/v3/tickers")
        ticker_map = self._parse_tickers(ticker_resp)
        symbols = self._select_symbols(ticker_map)
        top_symbols = self._rank_top_markets(symbols, ticker_map)

        snapshots: List[MarketSnapshot] = []
        for symbol in symbols:
            ticker = ticker_map[symbol]
            try:
                if symbol in top_symbols:
                    try:
                        book_resp, book_latency = self.client.get("derivatives/api/v3/orderbook", {"symbol": symbol})
                        snapshots.append(self._build_snapshot(symbol, ticker, book_resp, max(ticker_latency, book_latency)))
                        continue
                    except Exception:
                        pass
                snapshots.append(self._build_snapshot_from_ticker(symbol, ticker, ticker_latency))
            except Exception:
                continue
        return snapshots

    @staticmethod
    def _parse_tickers(response: object) -> Dict[str, dict]:
        if not isinstance(response, dict) or response.get("result") != "success":
            raise KrakenApiError("Unexpected Kraken tickers response shape")
        tickers = response.get("tickers")
        if not isinstance(tickers, list):
            raise KrakenApiError("Unexpected Kraken tickers response shape")
        parsed: Dict[str, dict] = {}
        for item in tickers:
            if isinstance(item, dict):
                symbol = str(item.get("symbol") or "").upper()
                if symbol:
                    parsed[symbol] = item
        return parsed

    def _select_symbols(self, ticker_map: Dict[str, dict]) -> List[str]:
        symbols = []
        asset_filter = {a.upper() for a in self.config.assets} if self.config.assets else None
        for symbol in sorted(ticker_map):
            ticker = ticker_map[symbol]
            if str(ticker.get("tag") or "").lower() != "perpetual":
                continue
            if ticker.get("suspended") is True:
                continue
            asset, quote, _ = _kraken_asset_quote(ticker)
            if quote not in _STABLE_QUOTES:
                continue
            if asset_filter and asset not in asset_filter:
                continue
            symbols.append(symbol)
        return symbols

    def _rank_top_markets(self, symbols: List[str], ticker_map: Dict[str, dict]) -> set:
        n = max(int(self.config.top_book_markets), 0)
        if n <= 0:
            return set()
        vol_list = sorted(symbols, key=lambda s: _coerce_float(ticker_map[s].get("volumeQuote")) or 0.0, reverse=True)
        oi_list = sorted(
            symbols,
            key=lambda s: (_coerce_float(ticker_map[s].get("openInterest")) or 0.0)
            * (_coerce_float(ticker_map[s].get("markPrice")) or _coerce_float(ticker_map[s].get("last")) or 0.0),
            reverse=True,
        )
        return set(vol_list[:n]) | set(oi_list[:n])

    def _build_snapshot(self, symbol: str, ticker: dict, book_resp: object, latency_ms: float) -> MarketSnapshot:
        book = book_resp.get("orderBook") if isinstance(book_resp, dict) else None
        bids, asks = _parse_array_book(book)
        mark_price = _coerce_float(ticker.get("markPrice")) or _coerce_float(ticker.get("last")) or 0.0
        if mark_price > 0:
            bids = [(price, size) for price, size in bids if 0 < price <= mark_price * 1.2]
            asks = [(price, size) for price, size in asks if price >= mark_price * 0.8]
        if not bids or not asks:
            return self._build_snapshot_from_ticker(symbol, ticker, latency_ms)
        return self._snapshot(symbol, ticker, bids[0][0], asks[0][0], bids[:50], asks[:50], latency_ms, ticker_only=False)

    def _build_snapshot_from_ticker(self, symbol: str, ticker: dict, latency_ms: float) -> MarketSnapshot:
        mark_price = _coerce_float(ticker.get("markPrice")) or _coerce_float(ticker.get("last")) or 0.0
        if mark_price <= 0:
            raise KrakenApiError(f"No price for {symbol}")
        best_bid = _coerce_float(ticker.get("bid")) or mark_price
        best_ask = _coerce_float(ticker.get("ask")) or mark_price
        return self._snapshot(symbol, ticker, best_bid, best_ask, [], [], latency_ms, ticker_only=True)

    def _snapshot(
        self,
        symbol: str,
        ticker: dict,
        best_bid: float,
        best_ask: float,
        bids: Sequence[Tuple[float, float]],
        asks: Sequence[Tuple[float, float]],
        latency_ms: float,
        ticker_only: bool,
    ) -> MarketSnapshot:
        asset, quote, stock_like = _kraken_asset_quote(ticker)
        mark_price = _coerce_float(ticker.get("markPrice")) or _coerce_float(ticker.get("last")) or (best_bid + best_ask) / 2.0
        index_price = _coerce_float(ticker.get("indexPrice")) or mark_price
        volume_24h = _coerce_float(ticker.get("volumeQuote")) or 0.0
        oi_base = _coerce_float(ticker.get("openInterest")) or 0.0
        oi_usd = oi_base * mark_price
        if oi_usd <= 0 and volume_24h > 0:
            oi_usd = volume_24h * 0.1
        # Kraken Futures publishes fundingRate in percentage points.
        funding_rate_bps = (_coerce_float(ticker.get("fundingRate")) or 0.0) * 100.0
        prev_price = _coerce_float(ticker.get("open24h")) or mark_price
        realized_vol = abs(math.log(mark_price / prev_price)) * 100.0 if prev_price > 0 and mark_price > 0 else 0.0
        mid_price = (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else mark_price
        metrics = _book_metrics(bids, asks, mid_price) if bids and asks else _empty_book_metrics()
        metadata = {
            "source": "kraken",
            "symbol": symbol,
            "day_quote_volume_usd": volume_24h,
            "funding_interval_hours": 1.0,
        }
        if stock_like:
            metadata["stock_like"] = True
        if ticker_only:
            metadata["ticker_only"] = True
        return MarketSnapshot(
            venue="kraken",
            market_type="perp_cex",
            asset=asset,
            quote=quote,
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=index_price,
            index_price=index_price,
            taker_fee_bps=5.0,
            maker_fee_bps=2.0,
            depth_10k_usd=metrics["depth_10k_usd"],
            depth_50k_usd=metrics["depth_50k_usd"],
            top_1pct_depth_usd=metrics["top_1pct_depth_usd"],
            volume_depth_ratio=volume_24h / max(metrics["top_1pct_depth_usd"], 1.0) if metrics["top_1pct_depth_usd"] > 0 else 0.0,
            oi_usd=oi_usd,
            funding_rate_bps=funding_rate_bps,
            next_funding_time=_next_interval_time(1),
            impact_cost_10k_bps=metrics["impact_10k_bps"],
            impact_cost_50k_bps=metrics["impact_50k_bps"],
            slippage_bps=metrics["slippage_bps"],
            realized_vol=realized_vol,
            spread_zscore=abs(mark_price - index_price) / max(index_price, 1e-9) * 1000.0,
            latency_ms=latency_ms,
            timestamp=datetime.now(timezone.utc),
            metadata=metadata,
        )


class AsterAdapter(MarketDataAdapter):
    def __init__(self, config: AsterAdapterConfig | None = None, client: AsterClient | None = None) -> None:
        self.config = config or AsterAdapterConfig()
        self.client = client or AsterClient(self.config)
        self.name = "aster"
        self._symbol_meta: Dict[str, dict] = {}

    def poll(self) -> Sequence[MarketSnapshot]:
        if not self._symbol_meta:
            self._symbol_meta = self._load_exchange_info()
        premium_resp, premium_latency = self.client.get("fapi/v1/premiumIndex")
        ticker_resp, ticker_latency = self.client.get("fapi/v1/ticker/24hr")
        book_ticker_resp, book_ticker_latency = self.client.get("fapi/v1/ticker/bookTicker")
        premium_map = self._parse_premium_index(premium_resp)
        ticker_map = self._parse_tickers(ticker_resp)
        book_ticker_map = self._parse_tickers(book_ticker_resp)
        batch_latency = max(premium_latency, ticker_latency, book_ticker_latency)
        symbols = self._select_symbols(premium_map, ticker_map)
        top_symbols = self._rank_top_markets(symbols, ticker_map)

        snapshots: List[MarketSnapshot] = []
        for symbol in symbols:
            premium = premium_map[symbol]
            ticker = ticker_map.get(symbol, {})
            book_ticker = book_ticker_map.get(symbol, {})
            oi_resp = None
            try:
                if symbol in top_symbols:
                    try:
                        oi_resp, oi_latency = self.client.get("fapi/v1/openInterest", {"symbol": symbol})
                    except Exception:
                        oi_latency = 0.0
                    try:
                        book_resp, book_latency = self.client.get("fapi/v1/depth", {"symbol": symbol, "limit": "20"})
                        snapshots.append(self._build_snapshot(symbol, premium, ticker, book_ticker, oi_resp, book_resp, max(batch_latency, oi_latency, book_latency)))
                        continue
                    except Exception:
                        batch_latency = max(batch_latency, oi_latency)
                snapshots.append(self._build_snapshot_from_ticker(symbol, premium, ticker, book_ticker, oi_resp, batch_latency))
            except Exception:
                continue
        return snapshots

    def _load_exchange_info(self) -> Dict[str, dict]:
        resp, _ = self.client.get("fapi/v1/exchangeInfo")
        if not isinstance(resp, dict):
            raise AsterApiError("Unexpected Aster exchangeInfo response shape")
        parsed: Dict[str, dict] = {}
        for item in resp.get("symbols", []):
            if not isinstance(item, dict):
                continue
            symbol = str(item.get("symbol") or "").upper()
            if not symbol:
                continue
            parsed[symbol] = item
        return parsed

    @staticmethod
    def _parse_premium_index(response: object) -> Dict[str, dict]:
        items = response if isinstance(response, list) else [response]
        parsed: Dict[str, dict] = {}
        for item in items:
            if isinstance(item, dict):
                symbol = str(item.get("symbol") or "").upper()
                if symbol:
                    parsed[symbol] = item
        return parsed

    @staticmethod
    def _parse_tickers(response: object) -> Dict[str, dict]:
        items = response if isinstance(response, list) else [response]
        parsed: Dict[str, dict] = {}
        for item in items:
            if isinstance(item, dict):
                symbol = str(item.get("symbol") or "").upper()
                if symbol:
                    parsed[symbol] = item
        return parsed

    def _select_symbols(self, premium_map: Dict[str, dict], ticker_map: Dict[str, dict]) -> List[str]:
        symbols = []
        asset_filter = {a.upper() for a in self.config.assets} if self.config.assets else None
        for symbol in sorted(premium_map):
            meta = self._symbol_meta.get(symbol, {})
            if meta:
                if str(meta.get("status") or "").upper() != "TRADING":
                    continue
                if str(meta.get("contractType") or "").upper() != "PERPETUAL":
                    continue
                quote = str(meta.get("quoteAsset") or "").upper()
                asset = str(meta.get("baseAsset") or _symbol_asset(symbol)).upper()
            else:
                quote = _symbol_quote(symbol)
                asset = _symbol_asset(symbol)
            if quote not in _STABLE_QUOTES:
                continue
            if symbol not in ticker_map:
                continue
            if asset_filter and asset not in asset_filter:
                continue
            symbols.append(symbol)
        return symbols

    def _rank_top_markets(self, symbols: List[str], ticker_map: Dict[str, dict]) -> set:
        n = max(int(self.config.top_book_markets), 0)
        if n <= 0:
            return set()
        return set(sorted(symbols, key=lambda s: _coerce_float(ticker_map.get(s, {}).get("quoteVolume")) or 0.0, reverse=True)[:n])

    def _build_snapshot(
        self,
        symbol: str,
        premium: dict,
        ticker: dict,
        book_ticker: dict,
        oi_resp: Optional[object],
        book_resp: object,
        latency_ms: float,
    ) -> MarketSnapshot:
        bids, asks = _parse_array_book(book_resp)
        if not bids or not asks:
            return self._build_snapshot_from_ticker(symbol, premium, ticker, book_ticker, oi_resp, latency_ms)
        return self._snapshot(symbol, premium, ticker, book_ticker, oi_resp, bids[0][0], asks[0][0], bids, asks, latency_ms, ticker_only=False)

    def _build_snapshot_from_ticker(
        self,
        symbol: str,
        premium: dict,
        ticker: dict,
        book_ticker: dict,
        oi_resp: Optional[object],
        latency_ms: float,
    ) -> MarketSnapshot:
        raw_mark_price = _coerce_float(premium.get("markPrice")) or _coerce_float(ticker.get("lastPrice")) or 0.0
        best_bid = _coerce_float(book_ticker.get("bidPrice")) or raw_mark_price
        best_ask = _coerce_float(book_ticker.get("askPrice")) or raw_mark_price
        if raw_mark_price <= 0 and _book_mid_price(best_bid, best_ask) <= 0:
            raise AsterApiError(f"No price for {symbol}")
        return self._snapshot(symbol, premium, ticker, book_ticker, oi_resp, best_bid, best_ask, [], [], latency_ms, ticker_only=True)

    def _snapshot(
        self,
        symbol: str,
        premium: dict,
        ticker: dict,
        book_ticker: dict,
        oi_resp: Optional[object],
        best_bid: float,
        best_ask: float,
        bids: Sequence[Tuple[float, float]],
        asks: Sequence[Tuple[float, float]],
        latency_ms: float,
        ticker_only: bool,
    ) -> MarketSnapshot:
        meta = self._symbol_meta.get(symbol, {})
        asset = str(meta.get("baseAsset") or _symbol_asset(symbol)).upper()
        quote = str(meta.get("quoteAsset") or _symbol_quote(symbol)).upper()
        book_mid_price = _book_mid_price(best_bid, best_ask)
        raw_mark_price = _coerce_float(premium.get("markPrice")) or _coerce_float(ticker.get("lastPrice")) or 0.0
        mark_price = book_mid_price or raw_mark_price
        if mark_price <= 0:
            raise AsterApiError(f"No price for {symbol}")
        index_price = _coerce_float(premium.get("indexPrice")) or raw_mark_price or mark_price
        volume_24h = _coerce_float(ticker.get("quoteVolume")) or 0.0
        oi_contracts = _coerce_float(oi_resp.get("openInterest")) if isinstance(oi_resp, dict) else None
        oi_usd = (oi_contracts or 0.0) * mark_price
        if oi_usd <= 0 and volume_24h > 0:
            oi_usd = volume_24h * 0.1
        funding_rate_bps = (_coerce_float(premium.get("lastFundingRate")) or 0.0) * 10_000.0
        next_funding_ts = _coerce_float(premium.get("nextFundingTime"))
        next_funding_time = datetime.fromtimestamp(next_funding_ts / 1000.0, tz=timezone.utc) if next_funding_ts and next_funding_ts > 0 else _next_interval_time(8)
        prev_price = _coerce_float(ticker.get("openPrice")) or raw_mark_price or mark_price
        realized_vol = abs(math.log(mark_price / prev_price)) * 100.0 if prev_price > 0 and mark_price > 0 else 0.0
        metrics = _book_metrics(bids, asks, book_mid_price or mark_price) if bids and asks else _empty_book_metrics()
        subtype = meta.get("underlyingSubType") if isinstance(meta.get("underlyingSubType"), list) else []
        mark_book_deviation_bps = (
            abs(raw_mark_price - book_mid_price) / book_mid_price * 10_000.0
            if raw_mark_price > 0 and book_mid_price > 0
            else 0.0
        )
        metadata = {
            "source": "aster",
            "symbol": symbol,
            "day_quote_volume_usd": volume_24h,
            "funding_interval_hours": 8.0,
            "stock_like": "STOCK" in subtype or "ETF" in subtype or meta.get("symbolType") == 1,
            "underlying_subtype": subtype,
            "name": meta.get("name") or "",
            "channel": meta.get("channel") or "",
            "premium_mark_price": raw_mark_price,
            "book_mid_price": book_mid_price,
            "mark_book_deviation_bps": mark_book_deviation_bps,
        }
        if ticker_only:
            metadata["ticker_only"] = True
        return MarketSnapshot(
            venue="aster",
            market_type="perp_cex",
            asset=asset,
            quote=quote,
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=index_price,
            index_price=index_price,
            taker_fee_bps=3.5,
            maker_fee_bps=1.0,
            depth_10k_usd=metrics["depth_10k_usd"],
            depth_50k_usd=metrics["depth_50k_usd"],
            top_1pct_depth_usd=metrics["top_1pct_depth_usd"],
            volume_depth_ratio=volume_24h / max(metrics["top_1pct_depth_usd"], 1.0) if metrics["top_1pct_depth_usd"] > 0 else 0.0,
            oi_usd=oi_usd,
            funding_rate_bps=funding_rate_bps,
            next_funding_time=next_funding_time,
            impact_cost_10k_bps=metrics["impact_10k_bps"],
            impact_cost_50k_bps=metrics["impact_50k_bps"],
            slippage_bps=metrics["slippage_bps"],
            realized_vol=realized_vol,
            spread_zscore=abs(mark_price - index_price) / max(index_price, 1e-9) * 1000.0,
            latency_ms=latency_ms,
            timestamp=datetime.now(timezone.utc),
            metadata=metadata,
        )


def _bitget_data(response: object) -> object:
    if isinstance(response, dict) and "code" in response:
        if response.get("code") != "00000":
            raise BitgetApiError(str(response.get("msg") or "Bitget API error"))
        return response.get("data")
    return response


def _symbol_quote(symbol: str) -> str:
    upper = symbol.upper()
    for quote in sorted(_STABLE_QUOTES, key=len, reverse=True):
        if upper.endswith(quote):
            return quote
    return "USDT"


def _symbol_asset(symbol: str) -> str:
    upper = symbol.upper()
    quote = _symbol_quote(upper)
    if upper.endswith(quote):
        return upper[: -len(quote)]
    return upper


def _bitget_display_asset(symbol: str) -> str:
    asset = _symbol_asset(symbol)
    if asset.endswith(_BITGET_STOCK_SUFFIX) and len(asset) > len(_BITGET_STOCK_SUFFIX):
        return asset[: -len(_BITGET_STOCK_SUFFIX)]
    return asset


def _bitget_is_stock_symbol(symbol: str) -> bool:
    asset = _symbol_asset(symbol)
    return asset.endswith(_BITGET_STOCK_SUFFIX) and len(asset) > len(_BITGET_STOCK_SUFFIX)


def _gate_contract_parts(contract: str) -> Tuple[str, str]:
    upper = contract.upper()
    parts = upper.split("_")
    if len(parts) >= 2:
        return _GATE_CONTRACT_ASSET_ALIASES.get(upper, parts[0]), parts[-1]
    return _GATE_CONTRACT_ASSET_ALIASES.get(upper, _symbol_asset(upper)), _symbol_quote(upper)


def _kraken_asset_quote(ticker: dict) -> Tuple[str, str, bool]:
    pair = str(ticker.get("pair") or "")
    stock_like = False
    if ":" in pair:
        raw_asset, raw_quote = pair.split(":", 1)
        stock_like = raw_asset.endswith("x")
        asset = raw_asset[:-1].upper() if stock_like else raw_asset.upper()
        quote = raw_quote.upper()
    else:
        symbol = str(ticker.get("symbol") or "").upper()
        body = symbol[3:] if symbol.startswith("PF_") else symbol
        quote = "USD"
        for candidate in sorted(_STABLE_QUOTES, key=len, reverse=True):
            if body.endswith(candidate):
                asset = body[: -len(candidate)]
                quote = candidate
                break
        else:
            asset = body
    if asset == "XBT":
        asset = "BTC"
    return asset, quote, stock_like


def _gate_volume_usd(ticker: dict) -> float:
    for key in ("volume_24h_quote", "volume_24h_settle", "volume_24h_usd", "quote_volume"):
        value = _coerce_float(ticker.get(key))
        if value and value > 0:
            return value
    last = _coerce_float(ticker.get("mark_price")) or _coerce_float(ticker.get("last")) or 0.0
    volume_base = _coerce_float(ticker.get("volume_24h_base")) or _coerce_float(ticker.get("volume_24h")) or 0.0
    return volume_base * last


def _gate_oi_usd(ticker: dict) -> float:
    for key in ("open_interest_usd", "oi_usd"):
        value = _coerce_float(ticker.get(key))
        if value and value > 0:
            return value
    mark = _coerce_float(ticker.get("mark_price")) or _coerce_float(ticker.get("last")) or 0.0
    total_size = _coerce_float(ticker.get("total_size")) or 0.0
    multiplier = _coerce_float(ticker.get("quanto_multiplier")) or 1.0
    return total_size * multiplier * mark


def _parse_array_book(book: object) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
    if not isinstance(book, dict):
        return [], []
    bids = _parse_level_array(book.get("bids"), reverse=True)
    asks = _parse_level_array(book.get("asks"), reverse=False)
    return bids, asks


def _parse_level_array(rows: object, reverse: bool) -> List[Tuple[float, float]]:
    levels: List[Tuple[float, float]] = []
    if not isinstance(rows, list):
        return levels
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        price = _coerce_float(row[0])
        size = _coerce_float(row[1])
        if price and size and price > 0 and size > 0:
            levels.append((price, size))
    return sorted(levels, key=lambda item: item[0], reverse=reverse)


def _parse_gate_book(book: object, multiplier: float) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
    if not isinstance(book, dict):
        return [], []
    bids = _parse_gate_levels(book.get("bids"), multiplier, reverse=True)
    asks = _parse_gate_levels(book.get("asks"), multiplier, reverse=False)
    return bids, asks


def _parse_gate_levels(rows: object, multiplier: float, reverse: bool) -> List[Tuple[float, float]]:
    levels: List[Tuple[float, float]] = []
    if not isinstance(rows, list):
        return levels
    for row in rows:
        if isinstance(row, dict):
            price = _coerce_float(row.get("p"))
            size = _coerce_float(row.get("s"))
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            price = _coerce_float(row[0])
            size = _coerce_float(row[1])
        else:
            continue
        if price and size and price > 0 and size > 0:
            levels.append((price, size * multiplier))
    return sorted(levels, key=lambda item: item[0], reverse=reverse)


def _book_metrics(
    bids: Sequence[Tuple[float, float]],
    asks: Sequence[Tuple[float, float]],
    mid_price: float,
) -> dict:
    symmetric_depth = min(_book_notional(bids), _book_notional(asks))
    top_1pct_depth = min(
        _book_notional_within_band(bids, mid_price, "bid", 0.01),
        _book_notional_within_band(asks, mid_price, "ask", 0.01),
    )
    impact_10k = _estimate_roundtrip_impact_bps(bids, asks, mid_price, 10_000.0)
    impact_50k = _estimate_roundtrip_impact_bps(bids, asks, mid_price, 50_000.0)
    return {
        "depth_10k_usd": min(10_000.0, symmetric_depth),
        "depth_50k_usd": min(50_000.0, symmetric_depth),
        "top_1pct_depth_usd": top_1pct_depth,
        "impact_10k_bps": impact_10k,
        "impact_50k_bps": impact_50k,
        "slippage_bps": max(0.25, impact_10k * 0.5),
    }


def _empty_book_metrics() -> dict:
    return {
        "depth_10k_usd": 0.0,
        "depth_50k_usd": 0.0,
        "top_1pct_depth_usd": 0.0,
        "impact_10k_bps": 50.0,
        "impact_50k_bps": 50.0,
        "slippage_bps": 25.0,
    }


def _book_notional(levels: Sequence[Tuple[float, float]]) -> float:
    return sum(price * size for price, size in levels)


def _book_mid_price(best_bid: float, best_ask: float) -> float:
    if best_bid > 0 and best_ask > 0:
        return (best_bid + best_ask) / 2.0
    if best_bid > 0:
        return best_bid
    if best_ask > 0:
        return best_ask
    return 0.0


def _book_notional_within_band(levels: Sequence[Tuple[float, float]], mid_price: float, side: str, pct: float) -> float:
    if side == "bid":
        return sum(price * size for price, size in levels if price >= mid_price * (1.0 - pct))
    return sum(price * size for price, size in levels if price <= mid_price * (1.0 + pct))


def _estimate_roundtrip_impact_bps(
    bids: Sequence[Tuple[float, float]],
    asks: Sequence[Tuple[float, float]],
    mid_price: float,
    target_notional: float,
) -> float:
    return _walk_impact_bps(asks, mid_price, target_notional, is_buy=True) + _walk_impact_bps(bids, mid_price, target_notional, is_buy=False)


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


def _next_interval_time(interval_hours: int) -> datetime:
    now = datetime.now(timezone.utc)
    base = now.replace(minute=0, second=0, microsecond=0)
    next_hour = ((now.hour // interval_hours) + 1) * interval_hours
    if next_hour >= 24:
        base = (base + timedelta(days=1)).replace(hour=0)
    else:
        base = base.replace(hour=next_hour)
    return base


def _coerce_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
