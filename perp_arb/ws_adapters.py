from __future__ import annotations

import math
from dataclasses import replace
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from .binance import BinanceAdapter, BinanceAdapterConfig
from .bybit import BybitAdapter, BybitAdapterConfig
from .grvt import GrvtAdapter, GrvtAdapterConfig, GrvtMarketDataClient
from .hyperliquid import HyperliquidAdapter, HyperliquidAdapterConfig, HyperliquidInfoClient
from .market_data import MarketSnapshot
from .okx import OkxAdapter, OkxAdapterConfig
from .streaming import BaseWebsocketAdapter


class HyperliquidWebsocketAdapter(BaseWebsocketAdapter):
    def __init__(
        self,
        config: HyperliquidAdapterConfig | None = None,
        client: HyperliquidInfoClient | None = None,
        reconnect_seconds: float = 1.0,
    ) -> None:
        self.rest_adapter = HyperliquidAdapter(config=config, client=client)
        self.config = self.rest_adapter.config
        super().__init__(name="hyperliquid-ws", reconnect_seconds=reconnect_seconds, jump_threshold_bps=2.0)

    def _seed_snapshots(self) -> Sequence[MarketSnapshot]:
        return self.rest_adapter.poll()

    def _snapshot_key(self, snapshot: MarketSnapshot) -> str:
        return snapshot.asset.upper()

    def _websocket_url(self) -> str:
        base = self.config.base_url.rstrip("/")
        if base.startswith("https://"):
            return "wss://" + base[len("https://") :] + "/ws"
        if base.startswith("http://"):
            return "ws://" + base[len("http://") :] + "/ws"
        return base + "/ws"

    def _subscription_messages(self) -> Sequence[dict]:
        snapshots = self.poll()
        messages: List[dict] = []
        for snapshot in snapshots:
            messages.append({"method": "subscribe", "subscription": {"type": "l2Book", "coin": snapshot.asset}})
            messages.append({"method": "subscribe", "subscription": {"type": "activeAssetCtx", "coin": snapshot.asset}})
        return messages

    def _message_updates(self, payload: object) -> Sequence[MarketSnapshot]:
        if not isinstance(payload, dict):
            return []
        channel = payload.get("channel")
        data = payload.get("data")
        if channel == "l2Book" and isinstance(data, dict):
            updated = self._apply_l2_book_message(data)
            return [updated] if updated is not None else []
        if channel == "activeAssetCtx" and isinstance(data, dict):
            updated = self._apply_active_asset_ctx_message(data)
            return [updated] if updated is not None else []
        return []

    def _apply_l2_book_message(self, data: dict) -> Optional[MarketSnapshot]:
        asset = data.get("coin")
        if not asset:
            return None
        current = self._get_snapshot(str(asset).upper())
        if current is None:
            return None
        bids, asks, book_time_ms = self.rest_adapter._parse_l2_book(data)
        if not bids or not asks:
            return None
        mid_px = (bids[0][0] + asks[0][0]) / 2.0
        symmetric_depth = min(
            self.rest_adapter._book_notional(bids),
            self.rest_adapter._book_notional(asks),
        )
        top_1pct_depth = min(
            self.rest_adapter._book_notional_within_band(bids, mid_px, side="bid", pct=0.01),
            self.rest_adapter._book_notional_within_band(asks, mid_px, side="ask", pct=0.01),
        )
        impact_10k = self.rest_adapter._estimate_roundtrip_impact_bps(bids, asks, mid_px, 10_000.0)
        impact_50k = self.rest_adapter._estimate_roundtrip_impact_bps(bids, asks, mid_px, 50_000.0)
        timestamp = (
            datetime.fromtimestamp(book_time_ms / 1000.0, tz=timezone.utc)
            if book_time_ms is not None
            else datetime.now(timezone.utc)
        )
        updated = replace(
            current,
            best_bid=bids[0][0],
            best_ask=asks[0][0],
            depth_10k_usd=min(10_000.0, symmetric_depth),
            depth_50k_usd=min(50_000.0, symmetric_depth),
            top_1pct_depth_usd=top_1pct_depth,
            impact_cost_10k_bps=impact_10k,
            impact_cost_50k_bps=impact_50k,
            slippage_bps=max(0.25, impact_10k * 0.5),
        )
        return self._with_timestamp(updated, timestamp)

    def _apply_active_asset_ctx_message(self, data: dict) -> Optional[MarketSnapshot]:
        asset = data.get("coin")
        ctx = data.get("ctx")
        if not asset or not isinstance(ctx, dict):
            return None
        current = self._get_snapshot(str(asset).upper())
        if current is None:
            return None
        mark_px = self.rest_adapter._coerce_float(ctx.get("markPx")) or current.mark_price
        oracle_px = self.rest_adapter._coerce_float(ctx.get("oraclePx")) or current.oracle_price
        prev_day_px = self.rest_adapter._coerce_float(ctx.get("prevDayPx")) or current.mark_price
        raw_funding = ctx.get("funding")
        funding_rate_bps = (self.rest_adapter._coerce_float(raw_funding) or 0.0) * 10_000.0 if raw_funding is not None else current.funding_rate_bps
        open_interest = self.rest_adapter._coerce_float(ctx.get("openInterest")) or 0.0
        day_ntl_vlm = self.rest_adapter._coerce_float(ctx.get("dayNtlVlm")) or 0.0
        volume_depth_ratio = day_ntl_vlm / max(current.top_1pct_depth_usd, 1.0)
        realized_vol = current.realized_vol
        if prev_day_px > 0:
            import math

            realized_vol = abs(math.log(mark_px / prev_day_px)) * 100.0
        updated = replace(
            current,
            mark_price=mark_px,
            oracle_price=oracle_px,
            index_price=oracle_px,
            funding_rate_bps=funding_rate_bps,
            oi_usd=open_interest * mark_px,
            volume_depth_ratio=volume_depth_ratio,
            realized_vol=realized_vol,
            metadata={**current.metadata, "day_ntl_volume_usd": day_ntl_vlm},
        )
        return self._with_timestamp(updated, datetime.now(timezone.utc))


class GrvtWebsocketAdapter(BaseWebsocketAdapter):
    def __init__(
        self,
        config: GrvtAdapterConfig | None = None,
        client: GrvtMarketDataClient | None = None,
        reconnect_seconds: float = 1.0,
    ) -> None:
        self.rest_adapter = GrvtAdapter(config=config, client=client)
        self.config = self.rest_adapter.config
        self._books: Dict[str, Dict[str, Dict[float, float]]] = {}
        super().__init__(name="grvt-ws", reconnect_seconds=reconnect_seconds, jump_threshold_bps=2.0)

    def _seed_snapshots(self) -> Sequence[MarketSnapshot]:
        snapshots = self.rest_adapter.poll()
        self._books = {}
        for snapshot in snapshots:
            instrument = str(snapshot.metadata.get("instrument", ""))
            if not instrument:
                continue
            self._books[instrument] = {
                "bids": {float(price): float(size) for price, size in snapshot.metadata.get("_book_bids", [])},
                "asks": {float(price): float(size) for price, size in snapshot.metadata.get("_book_asks", [])},
            }
        return snapshots

    def _snapshot_key(self, snapshot: MarketSnapshot) -> str:
        return str(snapshot.metadata.get("instrument", f"{snapshot.asset}:{snapshot.quote}"))

    def _websocket_url(self) -> str:
        base = self.config.base_url.rstrip("/")
        if base.startswith("https://"):
            return "wss://" + base[len("https://") :] + "/ws/lite"
        if base.startswith("http://"):
            return "ws://" + base[len("http://") :] + "/ws/lite"
        return base + "/ws/lite"

    def _subscription_messages(self) -> Sequence[dict]:
        snapshots = self.poll()
        ticker_selectors = [f"{snapshot.metadata['instrument']}@500" for snapshot in snapshots]
        book_selectors = [f"{snapshot.metadata['instrument']}@100" for snapshot in snapshots]
        return [
            {"j": "2.0", "m": "subscribe", "p": {"s": "v1.ticker.s", "s1": ticker_selectors}, "i": 1},
            {"j": "2.0", "m": "subscribe", "p": {"s": "v1.book.d", "s1": book_selectors}, "i": 2},
        ]

    def _message_updates(self, payload: object) -> Sequence[MarketSnapshot]:
        if not isinstance(payload, dict):
            return []
        stream = payload.get("s")
        feed = payload.get("f")
        selector = payload.get("s1")
        if stream == "v1.ticker.s" and isinstance(feed, dict):
            updated = self._apply_ticker_message(feed, selector)
            return [updated] if updated is not None else []
        if stream in {"v1.book.s", "v1.book.d"} and isinstance(feed, dict):
            updated = self._apply_book_message(feed, selector)
            return [updated] if updated is not None else []
        return []

    def _apply_ticker_message(self, feed: dict, selector: object) -> Optional[MarketSnapshot]:
        instrument = str(feed.get("i") or self._instrument_from_selector(selector) or "")
        if not instrument:
            return None
        current = self._get_snapshot(instrument)
        if current is None:
            return None
        mark_price = self.rest_adapter._coerce_float(feed.get("mp")) or current.mark_price
        index_price = self.rest_adapter._coerce_float(feed.get("ip")) or current.index_price
        best_bid = self.rest_adapter._coerce_float(feed.get("bb")) or current.best_bid
        best_ask = self.rest_adapter._coerce_float(feed.get("ba")) or current.best_ask
        buy_volume_q = self.rest_adapter._coerce_float(feed.get("bv1")) or 0.0
        sell_volume_q = self.rest_adapter._coerce_float(feed.get("sv1")) or 0.0
        volume_depth_ratio = (buy_volume_q + sell_volume_q) / max(current.top_1pct_depth_usd, 1.0)
        open_interest_base = self.rest_adapter._coerce_float(feed.get("oi")) or 0.0
        if feed.get("fr2") is not None or feed.get("fr") is not None:
            funding_rate_bps = self.rest_adapter._funding_bps(feed)
        else:
            funding_rate_bps = current.funding_rate_bps
        open_price = self.rest_adapter._coerce_float(feed.get("op")) or current.mark_price
        realized_vol = current.realized_vol
        if open_price > 0:
            import math

            realized_vol = abs(math.log(mark_price / open_price)) * 100.0
        next_funding_time = self.rest_adapter._parse_ns_timestamp(feed.get("nf"))
        event_time = self.rest_adapter._parse_ns_timestamp(feed.get("et"))
        updated = replace(
            current,
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=index_price,
            index_price=index_price,
            volume_depth_ratio=volume_depth_ratio,
            oi_usd=open_interest_base * mark_price,
            funding_rate_bps=funding_rate_bps,
            next_funding_time=next_funding_time,
            realized_vol=realized_vol,
        )
        return self._with_timestamp(updated, event_time)

    def _apply_book_message(self, feed: dict, selector: object) -> Optional[MarketSnapshot]:
        instrument = str(feed.get("i") or self._instrument_from_selector(selector) or "")
        if not instrument:
            return None
        current = self._get_snapshot(instrument)
        if current is None:
            return None
        book_state = self._books.setdefault(instrument, {"bids": {}, "asks": {}})
        self._apply_delta_levels(book_state["bids"], feed.get("b", []))
        self._apply_delta_levels(book_state["asks"], feed.get("a", []))
        bids = sorted(book_state["bids"].items(), key=lambda item: item[0], reverse=True)
        asks = sorted(book_state["asks"].items(), key=lambda item: item[0])
        if not bids or not asks:
            return None
        mid_price = (bids[0][0] + asks[0][0]) / 2.0
        top_1pct_depth = min(
            self.rest_adapter._book_notional_within_band(bids, mid_price, side="bid", pct=0.01),
            self.rest_adapter._book_notional_within_band(asks, mid_price, side="ask", pct=0.01),
        )
        symmetric_depth = min(self.rest_adapter._book_notional(bids), self.rest_adapter._book_notional(asks))
        impact_10k = self.rest_adapter._estimate_roundtrip_impact_bps(bids, asks, mid_price, 10_000.0)
        impact_50k = self.rest_adapter._estimate_roundtrip_impact_bps(bids, asks, mid_price, 50_000.0)
        event_time = self.rest_adapter._parse_ns_timestamp(feed.get("et"))
        updated = replace(
            current,
            best_bid=bids[0][0],
            best_ask=asks[0][0],
            depth_10k_usd=min(10_000.0, symmetric_depth),
            depth_50k_usd=min(50_000.0, symmetric_depth),
            top_1pct_depth_usd=top_1pct_depth,
            impact_cost_10k_bps=impact_10k,
            impact_cost_50k_bps=impact_50k,
            slippage_bps=max(0.25, impact_10k * 0.5),
            metadata={**current.metadata, "_book_bids": bids, "_book_asks": asks},
        )
        return self._with_timestamp(updated, event_time)

    @staticmethod
    def _instrument_from_selector(selector: object) -> Optional[str]:
        if not isinstance(selector, str):
            return None
        return selector.split("@", 1)[0]

    @staticmethod
    def _apply_delta_levels(target: Dict[float, float], levels: object) -> None:
        if not isinstance(levels, list):
            return
        for level in levels:
            if not isinstance(level, dict):
                continue
            price = GrvtAdapter._coerce_float(level.get("p"))
            size = GrvtAdapter._coerce_float(level.get("s"))
            if price is None or size is None:
                continue
            if size <= 0:
                target.pop(price, None)
            else:
                target[price] = size


# ---------------------------------------------------------------------------
# Shared helpers for CEX WS adapters
# ---------------------------------------------------------------------------

def _coerce_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _book_from_levels(
    raw_levels: list,
) -> List[Tuple[float, float]]:
    """Parse [[price, qty], ...] into [(float, float), ...]."""
    out: List[Tuple[float, float]] = []
    for level in raw_levels:
        if isinstance(level, (list, tuple)) and len(level) >= 2:
            try:
                out.append((float(level[0]), float(level[1])))
            except (TypeError, ValueError):
                continue
    return out


def _depth_metrics(
    bids: List[Tuple[float, float]],
    asks: List[Tuple[float, float]],
    adapter,
) -> dict:
    """Compute depth / impact metrics reusing the REST adapter's helpers."""
    if not bids or not asks:
        return {}
    mid_px = (bids[0][0] + asks[0][0]) / 2.0
    symmetric_depth = min(adapter._book_notional(bids), adapter._book_notional(asks))
    top_1pct = min(
        adapter._book_notional_within_band(bids, mid_px, side="bid", pct=0.01),
        adapter._book_notional_within_band(asks, mid_px, side="ask", pct=0.01),
    )
    impact_10k = adapter._estimate_roundtrip_impact_bps(bids, asks, mid_px, 10_000.0)
    impact_50k = adapter._estimate_roundtrip_impact_bps(bids, asks, mid_px, 50_000.0)
    return {
        "best_bid": bids[0][0],
        "best_ask": asks[0][0],
        "depth_10k_usd": min(10_000.0, symmetric_depth),
        "depth_50k_usd": min(50_000.0, symmetric_depth),
        "top_1pct_depth_usd": top_1pct,
        "impact_cost_10k_bps": impact_10k,
        "impact_cost_50k_bps": impact_50k,
        "slippage_bps": max(0.25, impact_10k * 0.5),
    }


# Max streams per WS connection (Binance limit is 200; OKX/Bybit similar).
# We use 2 channels per asset, so cap at 80 assets = 160 streams.
_MAX_WS_ASSETS = 80


def _select_ws_assets(
    snapshots: Sequence[MarketSnapshot],
    configured_assets: Optional[Sequence[str]],
    max_assets: int = _MAX_WS_ASSETS,
) -> List[str]:
    """Pick which assets to subscribe via WS.

    If the user specified assets, use those.  Otherwise, sort all seeded
    snapshots by top_1pct_depth (deepest first) and take the top N.
    """
    if configured_assets:
        return [a.upper() for a in configured_assets]
    ranked = sorted(snapshots, key=lambda s: s.top_1pct_depth_usd, reverse=True)
    return [s.asset.upper() for s in ranked[:max_assets]]


# ---------------------------------------------------------------------------
# Binance Futures WebSocket adapter
# ---------------------------------------------------------------------------

class BinanceWebsocketAdapter(BaseWebsocketAdapter):
    """Binance USDⓈ-M Futures combined WS stream.

    Subscribes to ``<symbol>@depth20@100ms`` and ``<symbol>@markPrice@1s``
    for each tracked asset.
    """

    def __init__(
        self,
        config: BinanceAdapterConfig | None = None,
        reconnect_seconds: float = 1.0,
    ) -> None:
        self.rest_adapter = BinanceAdapter(config=config)
        self.config = self.rest_adapter.config
        self._symbols: Dict[str, str] = {}  # asset -> symbol (e.g. "BTC" -> "BTCUSDT")
        super().__init__(name="binance-ws", reconnect_seconds=reconnect_seconds, jump_threshold_bps=2.0)

    def _seed_snapshots(self) -> Sequence[MarketSnapshot]:
        snapshots = self.rest_adapter.poll()
        ws_assets = _select_ws_assets(snapshots, self.config.assets)
        ws_set = set(ws_assets)
        kept: List[MarketSnapshot] = []
        for snapshot in snapshots:
            asset_upper = snapshot.asset.upper()
            symbol = snapshot.metadata.get("symbol", f"{asset_upper}USDT")
            if asset_upper in ws_set:
                self._symbols[asset_upper] = symbol
                kept.append(snapshot)
        return kept

    def _snapshot_key(self, snapshot: MarketSnapshot) -> str:
        return snapshot.asset.upper()

    def _websocket_url(self) -> str:
        # Build combined stream URL (capped by _select_ws_assets)
        streams = []
        for symbol in self._symbols.values():
            s = symbol.lower()
            streams.append(f"{s}@depth20@100ms")
            streams.append(f"{s}@markPrice@1s")
        combined = "/".join(streams)
        return f"wss://fstream.binance.com/stream?streams={combined}"

    def _subscription_messages(self) -> Sequence[dict]:
        # Combined stream URL already subscribes; no extra messages needed.
        return []

    def _message_updates(self, payload: object) -> Sequence[MarketSnapshot]:
        if not isinstance(payload, dict):
            return []
        # Combined stream wraps data in {"stream": "...", "data": {...}}
        stream = payload.get("stream", "")
        data = payload.get("data", payload)
        if not isinstance(data, dict):
            return []
        if "@depth" in stream:
            updated = self._apply_depth(data, stream)
            return [updated] if updated else []
        if "@markPrice" in stream:
            updated = self._apply_mark_price(data, stream)
            return [updated] if updated else []
        return []

    def _apply_depth(self, data: dict, stream: str) -> Optional[MarketSnapshot]:
        # Extract asset from stream name: "btcusdt@depth20@100ms"
        asset = self._asset_from_stream(stream)
        if not asset:
            return None
        current = self._get_snapshot(asset)
        if current is None:
            return None
        bids = _book_from_levels(data.get("b", data.get("bids", [])))
        asks = _book_from_levels(data.get("a", data.get("asks", [])))
        metrics = _depth_metrics(bids, asks, self.rest_adapter)
        if not metrics:
            return None
        event_time = data.get("E") or data.get("T")
        ts = (
            datetime.fromtimestamp(event_time / 1000.0, tz=timezone.utc)
            if event_time
            else datetime.now(timezone.utc)
        )
        updated = replace(current, **metrics)
        return self._with_timestamp(updated, ts)

    def _apply_mark_price(self, data: dict, stream: str) -> Optional[MarketSnapshot]:
        asset = self._asset_from_stream(stream)
        if not asset:
            return None
        current = self._get_snapshot(asset)
        if current is None:
            return None
        mark_px = _coerce_float(data.get("p")) or current.mark_price
        index_px = _coerce_float(data.get("i")) or current.index_price
        raw_funding = data.get("r")
        if raw_funding is not None:
            funding_bps = (_coerce_float(raw_funding) or 0.0) * 10_000.0
        else:
            funding_bps = current.funding_rate_bps
        next_funding_ms = data.get("T")
        next_funding_time = (
            datetime.fromtimestamp(next_funding_ms / 1000.0, tz=timezone.utc)
            if next_funding_ms
            else current.next_funding_time
        )
        event_time = data.get("E")
        ts = (
            datetime.fromtimestamp(event_time / 1000.0, tz=timezone.utc)
            if event_time
            else datetime.now(timezone.utc)
        )
        updated = replace(
            current,
            mark_price=mark_px,
            oracle_price=index_px,
            index_price=index_px,
            funding_rate_bps=funding_bps,
            next_funding_time=next_funding_time,
        )
        return self._with_timestamp(updated, ts)

    def _asset_from_stream(self, stream: str) -> Optional[str]:
        """Extract asset key from stream name like 'btcusdt@depth20@100ms'."""
        symbol_lower = stream.split("@")[0]
        for asset, sym in self._symbols.items():
            if sym.lower() == symbol_lower:
                return asset
        # Fallback: strip 'usdt' suffix
        if symbol_lower.endswith("usdt"):
            return symbol_lower[:-4].upper()
        return None


# ---------------------------------------------------------------------------
# OKX WebSocket adapter
# ---------------------------------------------------------------------------

class OkxWebsocketAdapter(BaseWebsocketAdapter):
    """OKX public WS channel.

    Subscribes to ``tickers`` and ``books5`` for each USDT-SWAP instrument.
    """

    def __init__(
        self,
        config: OkxAdapterConfig | None = None,
        reconnect_seconds: float = 1.0,
    ) -> None:
        self.rest_adapter = OkxAdapter(config=config)
        self.config = self.rest_adapter.config
        self._inst_ids: Dict[str, str] = {}  # asset -> instId (e.g. "BTC" -> "BTC-USDT-SWAP")
        super().__init__(name="okx-ws", reconnect_seconds=reconnect_seconds, jump_threshold_bps=2.0)

    def _seed_snapshots(self) -> Sequence[MarketSnapshot]:
        snapshots = self.rest_adapter.poll()
        ws_assets = _select_ws_assets(snapshots, self.config.assets)
        ws_set = set(ws_assets)
        kept: List[MarketSnapshot] = []
        for snapshot in snapshots:
            asset_upper = snapshot.asset.upper()
            if asset_upper in ws_set:
                inst_id = snapshot.metadata.get("inst_id", f"{asset_upper}-USDT-SWAP")
                self._inst_ids[asset_upper] = inst_id
                kept.append(snapshot)
        return kept

    def _snapshot_key(self, snapshot: MarketSnapshot) -> str:
        return snapshot.asset.upper()

    def _websocket_url(self) -> str:
        return "wss://ws.okx.com:8443/ws/v5/public"

    def _subscription_messages(self) -> Sequence[dict]:
        inst_ids = list(self._inst_ids.values())
        return [
            {
                "op": "subscribe",
                "args": [{"channel": "tickers", "instId": iid} for iid in inst_ids],
            },
            {
                "op": "subscribe",
                "args": [{"channel": "books5", "instId": iid} for iid in inst_ids],
            },
        ]

    def _message_updates(self, payload: object) -> Sequence[MarketSnapshot]:
        if not isinstance(payload, dict):
            return []
        arg = payload.get("arg")
        data = payload.get("data")
        if not isinstance(arg, dict) or not isinstance(data, list) or not data:
            return []
        channel = arg.get("channel", "")
        results: List[MarketSnapshot] = []
        for entry in data:
            if not isinstance(entry, dict):
                continue
            if channel == "tickers":
                updated = self._apply_ticker(entry)
            elif channel == "books5":
                updated = self._apply_book5(entry, arg)
            else:
                continue
            if updated is not None:
                results.append(updated)
        return results

    def _apply_ticker(self, entry: dict) -> Optional[MarketSnapshot]:
        inst_id = entry.get("instId", "")
        asset = self._asset_from_inst(inst_id)
        if not asset:
            return None
        current = self._get_snapshot(asset)
        if current is None:
            return None
        best_bid = _coerce_float(entry.get("bidPx")) or current.best_bid
        best_ask = _coerce_float(entry.get("askPx")) or current.best_ask
        mark_px = _coerce_float(entry.get("markPx")) or current.mark_price
        index_px = _coerce_float(entry.get("idxPx")) or current.index_price
        last_px = _coerce_float(entry.get("last")) or mark_px
        open_24h = _coerce_float(entry.get("open24h")) or mark_px
        vol_24h = _coerce_float(entry.get("volCcy24h")) or 0.0
        volume_depth_ratio = vol_24h / max(current.top_1pct_depth_usd, 1.0)
        realized_vol = current.realized_vol
        if open_24h > 0 and last_px > 0:
            realized_vol = abs(math.log(last_px / open_24h)) * 100.0
        ts_ms = _coerce_float(entry.get("ts"))
        ts = (
            datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            if ts_ms
            else datetime.now(timezone.utc)
        )
        updated = replace(
            current,
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_px,
            oracle_price=index_px,
            index_price=index_px,
            volume_depth_ratio=volume_depth_ratio,
            realized_vol=realized_vol,
        )
        return self._with_timestamp(updated, ts)

    def _apply_book5(self, entry: dict, arg: dict) -> Optional[MarketSnapshot]:
        inst_id = arg.get("instId", entry.get("instId", ""))
        asset = self._asset_from_inst(inst_id)
        if not asset:
            return None
        current = self._get_snapshot(asset)
        if current is None:
            return None
        bids = _book_from_levels(entry.get("bids", []))
        asks = _book_from_levels(entry.get("asks", []))
        metrics = _depth_metrics(bids, asks, self.rest_adapter)
        if not metrics:
            return None
        ts_ms = _coerce_float(entry.get("ts"))
        ts = (
            datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            if ts_ms
            else datetime.now(timezone.utc)
        )
        updated = replace(current, **metrics)
        return self._with_timestamp(updated, ts)

    def _asset_from_inst(self, inst_id: str) -> Optional[str]:
        for asset, iid in self._inst_ids.items():
            if iid == inst_id:
                return asset
        # Fallback: BTC-USDT-SWAP -> BTC
        if inst_id.endswith("-USDT-SWAP"):
            return inst_id.split("-")[0].upper()
        return None


# ---------------------------------------------------------------------------
# Bybit WebSocket adapter
# ---------------------------------------------------------------------------

class BybitWebsocketAdapter(BaseWebsocketAdapter):
    """Bybit V5 linear public WS.

    Subscribes to ``tickers.<symbol>`` and ``orderbook.25.<symbol>``
    for each tracked asset.
    """

    def __init__(
        self,
        config: BybitAdapterConfig | None = None,
        reconnect_seconds: float = 1.0,
    ) -> None:
        self.rest_adapter = BybitAdapter(config=config)
        self.config = self.rest_adapter.config
        self._symbols: Dict[str, str] = {}  # asset -> symbol (e.g. "BTC" -> "BTCUSDT")
        super().__init__(name="bybit-ws", reconnect_seconds=reconnect_seconds, jump_threshold_bps=2.0)

    def _seed_snapshots(self) -> Sequence[MarketSnapshot]:
        snapshots = self.rest_adapter.poll()
        ws_assets = _select_ws_assets(snapshots, self.config.assets)
        ws_set = set(ws_assets)
        kept: List[MarketSnapshot] = []
        for snapshot in snapshots:
            asset_upper = snapshot.asset.upper()
            if asset_upper in ws_set:
                symbol = snapshot.metadata.get("symbol", f"{asset_upper}USDT")
                self._symbols[asset_upper] = symbol
                kept.append(snapshot)
        return kept

    def _snapshot_key(self, snapshot: MarketSnapshot) -> str:
        return snapshot.asset.upper()

    def _websocket_url(self) -> str:
        return "wss://stream.bybit.com/v5/public/linear"

    def _subscription_messages(self) -> Sequence[dict]:
        # Bybit requires separate subscribe messages per channel type
        ticker_topics = [f"tickers.{s}" for s in self._symbols.values()]
        book_topics = [f"orderbook.25.{s}" for s in self._symbols.values()]
        return [
            {"op": "subscribe", "args": ticker_topics},
            {"op": "subscribe", "args": book_topics},
        ]

    def _message_updates(self, payload: object) -> Sequence[MarketSnapshot]:
        if not isinstance(payload, dict):
            return []
        topic = payload.get("topic", "")
        data = payload.get("data")
        if not data:
            return []
        # Bybit puts ts at the top level, not inside data
        ts_ms = _coerce_float(payload.get("ts"))
        ts = (
            datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
            if ts_ms
            else datetime.now(timezone.utc)
        )
        if topic.startswith("tickers.") and isinstance(data, dict):
            updated = self._apply_ticker(data, topic, ts)
            return [updated] if updated else []
        if topic.startswith("orderbook.") and isinstance(data, dict):
            updated = self._apply_orderbook(data, topic, ts)
            return [updated] if updated else []
        return []

    def _apply_ticker(self, data: dict, topic: str, ts: datetime) -> Optional[MarketSnapshot]:
        symbol = topic.split(".", 1)[1] if "." in topic else ""
        asset = self._asset_from_symbol(symbol)
        if not asset:
            return None
        current = self._get_snapshot(asset)
        if current is None:
            return None
        best_bid = _coerce_float(data.get("bid1Price")) or current.best_bid
        best_ask = _coerce_float(data.get("ask1Price")) or current.best_ask
        mark_px = _coerce_float(data.get("markPrice")) or current.mark_price
        index_px = _coerce_float(data.get("indexPrice")) or current.index_price
        last_px = _coerce_float(data.get("lastPrice")) or mark_px
        open_24h = _coerce_float(data.get("prevPrice24h")) or mark_px
        raw_funding = data.get("fundingRate")
        if raw_funding is not None:
            funding_bps = (_coerce_float(raw_funding) or 0.0) * 10_000.0
        else:
            funding_bps = current.funding_rate_bps
        next_funding_ms = _coerce_float(data.get("nextFundingTime"))
        next_funding_time = (
            datetime.fromtimestamp(next_funding_ms / 1000.0, tz=timezone.utc)
            if next_funding_ms
            else current.next_funding_time
        )
        oi_value = _coerce_float(data.get("openInterestValue")) or current.oi_usd
        realized_vol = current.realized_vol
        if open_24h > 0 and last_px > 0:
            realized_vol = abs(math.log(last_px / open_24h)) * 100.0
        updated = replace(
            current,
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_px,
            oracle_price=index_px,
            index_price=index_px,
            funding_rate_bps=funding_bps,
            next_funding_time=next_funding_time,
            oi_usd=oi_value,
            realized_vol=realized_vol,
        )
        return self._with_timestamp(updated, ts)

    def _apply_orderbook(self, data: dict, topic: str, ts: datetime) -> Optional[MarketSnapshot]:
        # topic: "orderbook.25.BTCUSDT"
        parts = topic.split(".")
        symbol = parts[2] if len(parts) >= 3 else ""
        asset = self._asset_from_symbol(symbol)
        if not asset:
            return None
        current = self._get_snapshot(asset)
        if current is None:
            return None
        bids = _book_from_levels(data.get("b", []))
        asks = _book_from_levels(data.get("a", []))
        metrics = _depth_metrics(bids, asks, self.rest_adapter)
        if not metrics:
            return None
        updated = replace(current, **metrics)
        return self._with_timestamp(updated, ts)

    def _asset_from_symbol(self, symbol: str) -> Optional[str]:
        for asset, sym in self._symbols.items():
            if sym == symbol:
                return asset
        if symbol.endswith("USDT"):
            return symbol[:-4].upper()
        return None
