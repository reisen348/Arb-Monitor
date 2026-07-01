from __future__ import annotations

import json
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple
from urllib import request

from .market_data import MarketDataAdapter, MarketSnapshot


@dataclass(frozen=True)
class GrvtAdapterConfig:
    base_url: str = "https://market-data.grvt.io"
    assets: Optional[Sequence[str]] = None
    quotes: Sequence[str] = ("USDT", "USDC")
    timeout_seconds: float = 5.0
    depth: int = 50
    top_book_markets: int = 100  # GRVT has ~95 perps and no batch ticker endpoint; fetch all
    request_workers: int = 8


class GrvtApiError(RuntimeError):
    pass


class GrvtMarketDataClient:
    def __init__(self, config: GrvtAdapterConfig | None = None) -> None:
        self.config = config or GrvtAdapterConfig()

    def post_lite(self, path: str, payload: dict) -> Tuple[object, float]:
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            f"{self.config.base_url.rstrip('/')}/lite/v1/{path.lstrip('/')}",
            data=body,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                # GRVT's edge appears to reject Python's default urllib user agent.
                "User-Agent": "Mozilla/5.0 (compatible; perp-arb/0.1; +https://api-docs.grvt.io/)",
                "Origin": "https://api-docs.grvt.io",
                "Referer": "https://api-docs.grvt.io/",
            },
            method="POST",
        )
        started = time.perf_counter()
        with request.urlopen(req, timeout=self.config.timeout_seconds) as response:
            raw = response.read()
        latency_ms = (time.perf_counter() - started) * 1000.0
        return json.loads(raw.decode("utf-8")), latency_ms


class GrvtAdapter(MarketDataAdapter):
    def __init__(
        self,
        config: GrvtAdapterConfig | None = None,
        client: GrvtMarketDataClient | None = None,
    ) -> None:
        self.config = config or GrvtAdapterConfig()
        self.client = client or GrvtMarketDataClient(self.config)
        self.name = "grvt"

    def poll(self) -> Sequence[MarketSnapshot]:
        payload = {"k": ["PERPETUAL"], "q": list(self.config.quotes), "ia": True, "l": 500}
        if self.config.assets:
            payload["b"] = list(self.config.assets)
        instruments, instruments_latency_ms = self.client.post_lite("instruments", payload)
        instrument_rows = self._parse_instruments(instruments)

        # GRVT has no batch ticker/book endpoint — limit to top N instruments
        # to avoid rate limiting (each instrument = 2 HTTP requests)
        instrument_rows = instrument_rows[:self.config.top_book_markets]

        snapshots: List[MarketSnapshot] = []
        max_workers = max(1, min(self.config.request_workers, len(instrument_rows)))
        if max_workers == 1:
            for instrument in instrument_rows:
                snapshot = self._fetch_snapshot(instrument, instruments_latency_ms)
                if snapshot is not None:
                    snapshots.append(snapshot)
            return snapshots

        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="grvt-seed") as executor:
            futures = {
                executor.submit(self._fetch_snapshot, instrument, instruments_latency_ms): instrument
                for instrument in instrument_rows
            }
            for future in as_completed(futures):
                try:
                    snapshot = future.result()
                except Exception:
                    continue
                if snapshot is not None:
                    snapshots.append(snapshot)
        return snapshots

    def _fetch_snapshot(self, instrument: dict, instruments_latency_ms: float) -> Optional[MarketSnapshot]:
        try:
            ticker, ticker_latency_ms = self.client.post_lite("ticker", {"i": instrument["instrument"]})
            book, book_latency_ms = self.client.post_lite(
                "book",
                {"i": instrument["instrument"], "d": self.config.depth},
            )
        except Exception:
            return None
        return self._build_snapshot(
            instrument=instrument,
            ticker=ticker,
            book=book,
            latency_ms=max(instruments_latency_ms, ticker_latency_ms, book_latency_ms),
        )

    def _parse_instruments(self, response: object) -> List[dict]:
        rows = response.get("r") if isinstance(response, dict) else None
        if not isinstance(rows, list):
            raise GrvtApiError("Unexpected GRVT instruments response shape")
        parsed = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if row.get("k") != "PERPETUAL":
                continue
            instrument = row.get("i")
            base = row.get("b")
            quote = row.get("q")
            if not instrument or not base or not quote:
                continue
            parsed.append(
                {
                    "instrument": str(instrument),
                    "base": str(base),
                    "quote": str(quote),
                    "funding_interval_hours": self._coerce_float(row.get("fi")),
                    "tick_size": self._coerce_float(row.get("ts")),
                    "min_notional": self._coerce_float(row.get("mn")),
                }
            )
        return parsed

    def _build_snapshot(
        self,
        instrument: dict,
        ticker: object,
        book: object,
        latency_ms: float,
    ) -> Optional[MarketSnapshot]:
        ticker_row = ticker.get("r") if isinstance(ticker, dict) else None
        book_row = book.get("r") if isinstance(book, dict) else None
        if not isinstance(ticker_row, dict) or not isinstance(book_row, dict):
            return None

        bids = [self._parse_level(level) for level in book_row.get("b", [])]
        asks = [self._parse_level(level) for level in book_row.get("a", [])]
        if not bids or not asks:
            return None

        mark_price = self._coerce_float(ticker_row.get("mp")) or self._coerce_float(ticker_row.get("mp1"))
        index_price = self._coerce_float(ticker_row.get("ip")) or mark_price
        mid_price = self._coerce_float(ticker_row.get("mp1")) or (bids[0][0] + asks[0][0]) / 2.0
        best_bid = self._coerce_float(ticker_row.get("bb")) or bids[0][0]
        best_ask = self._coerce_float(ticker_row.get("ba")) or asks[0][0]
        if mark_price is None or index_price is None:
            return None

        open_interest_base = self._coerce_float(ticker_row.get("oi")) or 0.0
        funding_rate_bps = self._funding_bps(ticker_row)
        event_time_ns = self._coerce_int(ticker_row.get("et")) or self._coerce_int(book_row.get("et"))
        timestamp = datetime.now(timezone.utc)
        staleness_ms = 0.0
        if event_time_ns is not None:
            event_time_ms = event_time_ns / 1_000_000.0
            timestamp = datetime.fromtimestamp(event_time_ms / 1000.0, tz=timezone.utc)
            staleness_ms = max(0.0, datetime.now(timezone.utc).timestamp() * 1000.0 - event_time_ms)

        top_1pct_depth = min(
            self._book_notional_within_band(bids, mid_price, side="bid", pct=0.01),
            self._book_notional_within_band(asks, mid_price, side="ask", pct=0.01),
        )
        symmetric_depth = min(self._book_notional(bids), self._book_notional(asks))
        impact_10k = self._estimate_roundtrip_impact_bps(bids, asks, mid_price, 10_000.0)
        impact_50k = self._estimate_roundtrip_impact_bps(bids, asks, mid_price, 50_000.0)
        buy_volume_q = self._coerce_float(ticker_row.get("bv1")) or 0.0
        sell_volume_q = self._coerce_float(ticker_row.get("sv1")) or 0.0
        volume_depth_ratio = (buy_volume_q + sell_volume_q) / max(top_1pct_depth, 1.0)
        open_price = self._coerce_float(ticker_row.get("op")) or mark_price
        realized_vol = abs(math.log(mark_price / open_price)) * 100.0 if open_price > 0 else 0.0
        next_funding_time = self._parse_ns_timestamp(ticker_row.get("nf"))

        return MarketSnapshot(
            venue="grvt",
            market_type="perp_dex",
            asset=instrument["base"],
            quote=instrument["quote"],
            best_bid=best_bid,
            best_ask=best_ask,
            mark_price=mark_price,
            oracle_price=index_price,
            index_price=index_price,
            taker_fee_bps=4.2,
            maker_fee_bps=-0.04,
            depth_10k_usd=min(10_000.0, symmetric_depth),
            depth_50k_usd=min(50_000.0, symmetric_depth),
            top_1pct_depth_usd=top_1pct_depth,
            volume_depth_ratio=volume_depth_ratio,
            oi_usd=open_interest_base * mark_price,
            oi_change_pct=0.0,
            funding_rate_bps=funding_rate_bps,
            funding_change_bps=0.0,
            next_funding_time=next_funding_time,
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
                "source": "grvt",
                "instrument": instrument["instrument"],
                "funding_interval_hours": instrument.get("funding_interval_hours"),
                "_book_bids": bids,
                "_book_asks": asks,
            },
        )

    @staticmethod
    def _parse_level(level: dict) -> Tuple[float, float]:
        return float(level["p"]), float(level["s"])

    @staticmethod
    def _funding_bps(ticker_row: dict) -> float:
        # GRVT funding_rate is in percentage points (0.001 = 0.001%).
        # Convert to bps: multiply by 100 (1% = 100 bps).
        raw = ticker_row.get("fr2") or ticker_row.get("fr")
        if raw is not None:
            return float(raw) * 100.0
        return 0.0

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
        target_notional_usd: float,
    ) -> float:
        buy = GrvtAdapter._walk_impact_bps(asks, mid_price, target_notional_usd, is_buy=True)
        sell = GrvtAdapter._walk_impact_bps(bids, mid_price, target_notional_usd, is_buy=False)
        return buy + sell

    @staticmethod
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

    @staticmethod
    def _parse_ns_timestamp(value: object) -> Optional[datetime]:
        as_int = GrvtAdapter._coerce_int(value)
        if as_int is None:
            return None
        return datetime.fromtimestamp(as_int / 1_000_000_000.0, tz=timezone.utc)

    @staticmethod
    def _coerce_float(value: object) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_int(value: object) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
