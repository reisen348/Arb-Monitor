"""Tests for the Nado perpetual DEX adapter."""
from __future__ import annotations

import unittest
import time
from typing import Tuple

from perp_arb.nado import NadoAdapter, NadoAdapterConfig, NadoClient


FAKE_CONTRACTS = {
    "BTC-PERP_USDT0": {
        "product_id": 2,
        "ticker_id": "BTC-PERP_USDT0",
        "base_currency": "BTC-PERP",
        "quote_currency": "USDT0",
        "last_price": 68600.0,
        "base_volume": 1250.5,
        "quote_volume": 85784300.0,
        "product_type": "perpetual",
        "open_interest": 4500.0,
        "open_interest_usd": 308700000.0,
        "index_price": 68590.0,
        "mark_price": 68595.0,
        "funding_rate": -0.0005,
        "next_funding_rate_timestamp": 1774170000,
        "price_change_percent_24h": -1.5,
    },
    "ETH-PERP_USDT0": {
        "product_id": 4,
        "ticker_id": "ETH-PERP_USDT0",
        "base_currency": "ETH-PERP",
        "quote_currency": "USDT0",
        "last_price": 2080.0,
        "base_volume": 15000.0,
        "quote_volume": 31200000.0,
        "product_type": "perpetual",
        "open_interest": 95000.0,
        "open_interest_usd": 197600000.0,
        "index_price": 2079.5,
        "mark_price": 2080.2,
        "funding_rate": 0.00012,
        "next_funding_rate_timestamp": 1774170000,
        "price_change_percent_24h": -2.3,
    },
    "SOL-PERP_USDT0": {
        "product_id": 8,
        "ticker_id": "SOL-PERP_USDT0",
        "base_currency": "SOL-PERP",
        "quote_currency": "USDT0",
        "last_price": 65.0,
        "base_volume": 50000.0,
        "quote_volume": 3250000.0,
        "product_type": "perpetual",
        "open_interest": 200000.0,
        "open_interest_usd": 13000000.0,
        "index_price": 64.95,
        "mark_price": 65.02,
        "funding_rate": 0.0001,
        "next_funding_rate_timestamp": 1774170000,
        "price_change_percent_24h": -3.0,
    },
}

FAKE_BOOK = {
    "product_id": 2,
    "ticker_id": "BTC-PERP_USDT0",
    "bids": [
        [68590.0, 1.5],
        [68585.0, 2.0],
        [68580.0, 3.0],
    ],
    "asks": [
        [68600.0, 1.2],
        [68605.0, 1.8],
        [68610.0, 2.5],
    ],
    "timestamp": 1774168477448,
}


class FakeNadoClient(NadoClient):
    def __init__(self, config=None):
        super().__init__(config)
        self.call_log: list = []

    def get(self, base: str, path: str) -> Tuple[object, float]:
        self.call_log.append((base, path))
        if "contracts" in path:
            return FAKE_CONTRACTS, 50.0
        if "orderbook" in path:
            return FAKE_BOOK, 30.0
        return {}, 10.0


class NadoAdapterTest(unittest.TestCase):
    def test_poll_returns_snapshots(self) -> None:
        adapter = NadoAdapter(
            config=NadoAdapterConfig(assets=["BTC", "ETH"]),
            client=FakeNadoClient(),
        )
        snapshots = adapter.poll()
        self.assertEqual(len(snapshots), 2)
        assets = {s.asset for s in snapshots}
        self.assertEqual(assets, {"BTC", "ETH"})
        for s in snapshots:
            self.assertEqual(s.venue, "nado")
            self.assertEqual(s.market_type, "perp_dex")
            self.assertEqual(s.quote, "USD")
            self.assertGreater(s.mark_price, 0)
            self.assertEqual(s.taker_fee_bps, 4.5)
            self.assertEqual(s.maker_fee_bps, 1.0)

    def test_asset_filter(self) -> None:
        adapter = NadoAdapter(
            config=NadoAdapterConfig(assets=["SOL"]),
            client=FakeNadoClient(),
        )
        snapshots = adapter.poll()
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].asset, "SOL")

    def test_top_book_markets_limits_orderbook_requests(self) -> None:
        client = FakeNadoClient()
        adapter = NadoAdapter(
            config=NadoAdapterConfig(assets=None, top_book_markets=2),
            client=client,
        )
        snapshots = adapter.poll()
        self.assertGreaterEqual(len(snapshots), 3)
        orderbook_calls = [c for c in client.call_log if "orderbook" in c[1]]
        self.assertEqual(len(orderbook_calls), 2)

    def test_reuses_cached_orderbooks_within_ttl(self) -> None:
        client = FakeNadoClient()
        adapter = NadoAdapter(
            config=NadoAdapterConfig(assets=["BTC"], top_book_markets=1, book_cache_ttl_seconds=60.0),
            client=client,
        )
        first = adapter.poll()
        second = adapter.poll()

        orderbook_calls = [c for c in client.call_log if "orderbook" in c[1]]
        self.assertEqual(len(orderbook_calls), 1)
        self.assertFalse(first[0].metadata.get("ticker_only", False))
        self.assertFalse(second[0].metadata.get("ticker_only", False))
        self.assertGreater(second[0].top_1pct_depth_usd, 0.0)

    def test_refetches_orderbooks_when_cache_disabled(self) -> None:
        client = FakeNadoClient()
        adapter = NadoAdapter(
            config=NadoAdapterConfig(assets=["BTC"], top_book_markets=1, book_cache_ttl_seconds=0.0),
            client=client,
        )
        adapter.poll()
        adapter.poll()

        orderbook_calls = [c for c in client.call_log if "orderbook" in c[1]]
        self.assertEqual(len(orderbook_calls), 2)

    def test_refreshes_stale_orderbooks_in_background(self) -> None:
        class SlowRefreshClient(FakeNadoClient):
            def __init__(self) -> None:
                super().__init__()
                self.book_calls = 0

            def get(self, base: str, path: str) -> Tuple[object, float]:
                if "orderbook" in path:
                    self.book_calls += 1
                    if self.book_calls > 1:
                        time.sleep(0.2)
                return super().get(base, path)

        client = SlowRefreshClient()
        adapter = NadoAdapter(
            config=NadoAdapterConfig(
                assets=["BTC"],
                top_book_markets=1,
                book_cache_ttl_seconds=0.01,
                book_stale_ttl_seconds=60.0,
            ),
            client=client,
        )
        try:
            adapter.poll()
            time.sleep(0.02)

            t0 = time.monotonic()
            snapshots = adapter.poll()
            elapsed = time.monotonic() - t0
        finally:
            adapter.stop()

        self.assertLess(elapsed, 0.15)
        self.assertEqual(len(snapshots), 1)
        self.assertGreater(snapshots[0].top_1pct_depth_usd, 0.0)

    def test_extract_asset(self) -> None:
        self.assertEqual(NadoAdapter._extract_asset("BTC-PERP_USDT0"), "BTC")
        self.assertEqual(NadoAdapter._extract_asset("kBONK-PERP_USDT0"), "kBONK")
        self.assertIsNone(NadoAdapter._extract_asset("BTC_USDT0"))

    def test_funding_rate_conversion(self) -> None:
        """funding_rate is 24h rate; stored as bps/24h with interval=24h."""
        adapter = NadoAdapter(
            config=NadoAdapterConfig(assets=["BTC"]),
            client=FakeNadoClient(),
        )
        snapshots = adapter.poll()
        btc = snapshots[0]
        # -0.0005 * 10000 = -5.0 bps per 24h settlement
        self.assertAlmostEqual(btc.funding_rate_bps, -0.0005 * 10_000.0, places=4)
        self.assertEqual(btc.metadata.get("funding_interval_hours"), 24.0)

    def test_scan_all_when_no_assets(self) -> None:
        adapter = NadoAdapter(
            config=NadoAdapterConfig(assets=None),
            client=FakeNadoClient(),
        )
        snapshots = adapter.poll()
        self.assertEqual(len(snapshots), 3)


if __name__ == "__main__":
    unittest.main()
