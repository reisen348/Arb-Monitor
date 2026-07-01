"""Tests for the Ondo Perps REST adapter."""
from __future__ import annotations

import time
import unittest
from typing import Tuple

from perp_arb.ondo import OndoAdapter, OndoAdapterConfig, OndoClient


FAKE_CONTRACTS = [
    {
        "market": "AAPL-USD.P",
        "displayName": "AAPLUSD",
        "productType": "perpetual",
        "contractType": "linear",
        "baseCurrency": "AAPL",
        "quoteCurrency": "USD",
        "disabled": False,
        "lastPrice": "290.0",
        "baseVolume": "500",
        "quoteVolume": "145000",
        "usdVolume": "145000",
        "bid": "289.9",
        "ask": "290.1",
        "openInterest": "1000",
        "openInterestUsd": "290000",
        "indexPrice": "290.0",
        "fundingRate": "0.0000063",
        "nextFundingRate": "0.0000100",
        "nextFundingRateTimestamp": "2026-07-01T13:00:00Z",
        "makerFee": "0.00015",
        "takerFee": "0.00035",
        "priceChangePercent": "2.5",
        "tags": ["Stock"],
    },
    {
        "market": "XAU-USD.P",
        "displayName": "XAUUSD",
        "productType": "perpetual",
        "contractType": "linear",
        "baseCurrency": "XAU",
        "quoteCurrency": "USD",
        "disabled": False,
        "lastPrice": "4020",
        "baseVolume": "100",
        "quoteVolume": "402000",
        "usdVolume": "402000",
        "bid": "4019.5",
        "ask": "4020.5",
        "openInterest": "50",
        "openInterestUsd": "201000",
        "indexPrice": "4021",
        "fundingRate": "0.0000063",
        "nextFundingRate": "0.0000063",
        "nextFundingRateTimestamp": "2026-07-01T13:00:00Z",
        "makerFee": "0.00015",
        "takerFee": "0.00035",
        "priceChangePercent": "-1.0",
        "tags": ["Commodity"],
    },
    {
        "market": "SPY-USD.P",
        "productType": "perpetual",
        "baseCurrency": "SPY",
        "quoteCurrency": "USD",
        "disabled": True,
        "lastPrice": "0",
        "openInterestUsd": "0",
        "tags": ["ETF"],
    },
]

FAKE_MARKS = {
    "AAPL-USD.P": {
        "market": "AAPL-USD.P",
        "markPrice": "290.05",
        "oraclePrice": "290.04",
        "lastUpdatedTime": "2026-07-01T12:58:20.498867685Z",
        "pair": {"base": "AAPL", "quote": "USD"},
    },
    "XAU-USD.P": {
        "market": "XAU-USD.P",
        "markPrice": "4020.0",
        "oraclePrice": "4021.0",
        "lastUpdatedTime": "2026-07-01T12:58:20.498867685Z",
        "pair": {"base": "XAU", "quote": "USD"},
    },
}

FAKE_BOOK = {
    "success": True,
    "result": {
        "market": "AAPL-USD.P",
        "time": "2026-07-01T12:58:22Z",
        "bids": [["289.9", "20"], ["289.8", "25"]],
        "asks": [["290.1", "20"], ["290.2", "25"]],
    },
}


class FakeOndoClient(OndoClient):
    def __init__(self, config=None):
        super().__init__(config)
        self.call_log: list[str] = []

    def get(self, path: str) -> Tuple[object, float]:
        self.call_log.append(path)
        if path.startswith("v1/perps/contracts"):
            return {"success": True, "result": FAKE_CONTRACTS}, 25.0
        if path.startswith("v1/perps/mark_prices"):
            return {"success": True, "result": FAKE_MARKS}, 20.0
        if path.startswith("v1/perps/depth"):
            return FAKE_BOOK, 30.0
        return {"success": False, "error": "unexpected"}, 10.0


class OndoAdapterTest(unittest.TestCase):
    def test_poll_returns_enabled_snapshots(self) -> None:
        adapter = OndoAdapter(
            config=OndoAdapterConfig(assets=None, top_book_markets=1),
            client=FakeOndoClient(),
        )
        snapshots = adapter.poll()
        self.assertEqual({s.asset for s in snapshots}, {"AAPL", "XAU"})
        aapl = next(s for s in snapshots if s.asset == "AAPL")
        self.assertEqual(aapl.venue, "ondo")
        self.assertEqual(aapl.market_type, "perp_dex")
        self.assertEqual(aapl.quote, "USD")
        self.assertAlmostEqual(aapl.best_bid, 289.9)
        self.assertAlmostEqual(aapl.best_ask, 290.1)
        self.assertAlmostEqual(aapl.mark_price, 290.05)
        self.assertAlmostEqual(aapl.oracle_price, 290.04)
        self.assertAlmostEqual(aapl.maker_fee_bps, 1.5)
        self.assertAlmostEqual(aapl.taker_fee_bps, 3.5)
        self.assertAlmostEqual(aapl.funding_rate_bps, 0.0000100 * 10_000.0)
        self.assertAlmostEqual(aapl.funding_change_bps, (0.0000100 - 0.0000063) * 10_000.0)
        self.assertEqual(aapl.metadata.get("funding_interval_hours"), 1.0)
        self.assertTrue(aapl.metadata.get("stock_like"))
        self.assertFalse(aapl.metadata.get("ticker_only", False))

        xau = next(s for s in snapshots if s.asset == "XAU")
        self.assertFalse(xau.metadata.get("stock_like"))

    def test_asset_filter_and_disabled_market_filter(self) -> None:
        adapter = OndoAdapter(
            config=OndoAdapterConfig(assets=["SPY", "XAU"], top_book_markets=0),
            client=FakeOndoClient(),
        )
        snapshots = adapter.poll()
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].asset, "XAU")
        self.assertTrue(snapshots[0].metadata.get("ticker_only"))

    def test_top_book_markets_limits_depth_requests(self) -> None:
        client = FakeOndoClient()
        adapter = OndoAdapter(
            config=OndoAdapterConfig(assets=None, top_book_markets=1),
            client=client,
        )
        adapter.poll()
        depth_calls = [path for path in client.call_log if path.startswith("v1/perps/depth")]
        self.assertEqual(len(depth_calls), 2)

    def test_reuses_cached_orderbooks_within_ttl(self) -> None:
        client = FakeOndoClient()
        adapter = OndoAdapter(
            config=OndoAdapterConfig(assets=["AAPL"], top_book_markets=1, book_cache_ttl_seconds=60.0),
            client=client,
        )
        first = adapter.poll()
        second = adapter.poll()
        depth_calls = [path for path in client.call_log if path.startswith("v1/perps/depth")]
        self.assertEqual(len(depth_calls), 1)
        self.assertFalse(first[0].metadata.get("ticker_only", False))
        self.assertFalse(second[0].metadata.get("ticker_only", False))

    def test_refreshes_stale_orderbooks_in_background(self) -> None:
        class SlowRefreshClient(FakeOndoClient):
            def __init__(self) -> None:
                super().__init__()
                self.book_calls = 0

            def get(self, path: str) -> Tuple[object, float]:
                if path.startswith("v1/perps/depth"):
                    self.book_calls += 1
                    if self.book_calls > 1:
                        time.sleep(0.2)
                return super().get(path)

        client = SlowRefreshClient()
        adapter = OndoAdapter(
            config=OndoAdapterConfig(
                assets=["AAPL"],
                top_book_markets=1,
                book_cache_ttl_seconds=0.01,
                book_stale_ttl_seconds=60.0,
            ),
            client=client,
        )
        try:
            adapter.poll()
            time.sleep(0.02)
            started = time.monotonic()
            snapshots = adapter.poll()
            elapsed = time.monotonic() - started
        finally:
            adapter.stop()

        self.assertLess(elapsed, 0.15)
        self.assertEqual(len(snapshots), 1)
        self.assertGreater(snapshots[0].top_1pct_depth_usd, 0.0)


if __name__ == "__main__":
    unittest.main()
