from __future__ import annotations

import time
import unittest

from perp_arb.paradex import ParadexAdapter, ParadexAdapterConfig, ParadexClient


class FakeParadexClient(ParadexClient):
    def __init__(self) -> None:
        self.config = ParadexAdapterConfig(assets=["BTC"])
        self.requests = []

    def get(self, path: str):
        self.requests.append(path)
        if "markets/summary" in path:
            return (
                {
                    "results": [
                        {
                            "symbol": "BTC-USD-PERP",
                            "mark_price": "68005.0",
                            "underlying_price": "68000.0",
                            "last_traded_price": "68003.0",
                            "bid": "68000.0",
                            "ask": "68010.0",
                            "open_interest": "125.5",
                            "funding_rate": "0.0000125",
                            "volume_24h": "1500000.0",
                            "price_change_rate_24h": "0.012",
                            "created_at": 1700000000000,
                        },
                        {
                            "symbol": "ETH-USD-PERP",
                            "mark_price": "3400.0",
                            "underlying_price": "3398.0",
                            "last_traded_price": "3399.0",
                            "bid": "3398.0",
                            "ask": "3402.0",
                            "open_interest": "500.0",
                            "funding_rate": "0.0000080",
                            "volume_24h": "800000.0",
                            "price_change_rate_24h": "-0.005",
                        },
                    ]
                },
                30.0,
            )
        if "markets" in path and "summary" not in path:
            return (
                {
                    "results": [
                        {
                            "symbol": "BTC-USD-PERP",
                            "fee_config": {
                                "api_fee": {
                                    "taker_fee": {"fee": "0.0002"},
                                    "maker_fee": {"fee": "0.00003"},
                                },
                            },
                        },
                        {
                            "symbol": "ETH-USD-PERP",
                            "fee_config": {
                                "api_fee": {
                                    "taker_fee": {"fee": "0.0002"},
                                    "maker_fee": {"fee": "0.00003"},
                                },
                            },
                        },
                    ]
                },
                25.0,
            )
        if "orderbook" in path:
            return (
                {
                    "bids": [
                        ["68000.0", "0.80"],
                        ["67995.0", "0.60"],
                        ["67990.0", "0.40"],
                    ],
                    "asks": [
                        ["68010.0", "0.90"],
                        ["68015.0", "0.70"],
                        ["68020.0", "0.50"],
                    ],
                },
                40.0,
            )
        raise AssertionError(f"unexpected path: {path}")


class ParadexAdapterTest(unittest.TestCase):
    def test_poll_converts_responses_to_market_snapshots(self) -> None:
        adapter = ParadexAdapter(
            config=ParadexAdapterConfig(assets=["BTC"]),
            client=FakeParadexClient(),
        )
        snapshots = adapter.poll()
        self.assertEqual(len(snapshots), 1)
        snapshot = snapshots[0]
        self.assertEqual(snapshot.venue, "paradex")
        self.assertEqual(snapshot.asset, "BTC")
        self.assertEqual(snapshot.quote, "USD")
        self.assertAlmostEqual(snapshot.best_bid, 68000.0)
        self.assertAlmostEqual(snapshot.best_ask, 68010.0)
        self.assertAlmostEqual(snapshot.mark_price, 68005.0)
        self.assertAlmostEqual(snapshot.oracle_price, 68000.0)
        self.assertAlmostEqual(snapshot.funding_rate_bps, 0.125)
        self.assertGreater(snapshot.oi_usd, 0.0)
        self.assertGreater(snapshot.top_1pct_depth_usd, 0.0)
        self.assertGreater(snapshot.impact_cost_10k_bps, 0.0)

    def test_poll_without_asset_filter_returns_all_perps(self) -> None:
        adapter = ParadexAdapter(
            config=ParadexAdapterConfig(assets=None),
            client=FakeParadexClient(),
        )
        snapshots = adapter.poll()
        self.assertEqual(len(snapshots), 2)
        assets = {s.asset for s in snapshots}
        self.assertEqual(assets, {"BTC", "ETH"})

    def test_taker_fee_parsed_from_markets_endpoint(self) -> None:
        adapter = ParadexAdapter(
            config=ParadexAdapterConfig(assets=["BTC"]),
            client=FakeParadexClient(),
        )
        snapshots = adapter.poll()
        self.assertAlmostEqual(snapshots[0].taker_fee_bps, 0.75)
        self.assertAlmostEqual(snapshots[0].maker_fee_bps, 0.0)

    def test_reuses_cached_orderbooks_within_ttl(self) -> None:
        client = FakeParadexClient()
        adapter = ParadexAdapter(
            config=ParadexAdapterConfig(assets=["BTC"], top_book_markets=1, book_cache_ttl_seconds=60.0),
            client=client,
        )
        first = adapter.poll()
        second = adapter.poll()

        orderbook_requests = [path for path in client.requests if "orderbook" in path]
        self.assertEqual(len(orderbook_requests), 1)
        self.assertFalse(first[0].metadata.get("ticker_only", False))
        self.assertFalse(second[0].metadata.get("ticker_only", False))
        self.assertGreater(second[0].top_1pct_depth_usd, 0.0)

    def test_refetches_orderbooks_when_cache_disabled(self) -> None:
        client = FakeParadexClient()
        adapter = ParadexAdapter(
            config=ParadexAdapterConfig(assets=["BTC"], top_book_markets=1, book_cache_ttl_seconds=0.0),
            client=client,
        )
        adapter.poll()
        adapter.poll()

        orderbook_requests = [path for path in client.requests if "orderbook" in path]
        self.assertEqual(len(orderbook_requests), 2)

    def test_refreshes_stale_orderbooks_in_background(self) -> None:
        class SlowRefreshClient(FakeParadexClient):
            def __init__(self) -> None:
                super().__init__()
                self.book_calls = 0

            def get(self, path: str):
                if "orderbook" in path:
                    self.book_calls += 1
                    if self.book_calls > 1:
                        time.sleep(0.2)
                return super().get(path)

        client = SlowRefreshClient()
        adapter = ParadexAdapter(
            config=ParadexAdapterConfig(
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

    def test_falls_back_to_ticker_when_book_fetch_fails(self) -> None:
        class FailingBookClient(FakeParadexClient):
            def get(self, path: str):
                if "orderbook" in path:
                    raise RuntimeError("rate limited")
                return super().get(path)

        adapter = ParadexAdapter(
            config=ParadexAdapterConfig(assets=["BTC"], top_book_markets=1),
            client=FailingBookClient(),
        )
        snapshots = adapter.poll()

        self.assertEqual(len(snapshots), 1)
        self.assertTrue(snapshots[0].metadata.get("ticker_only"))
        self.assertEqual(snapshots[0].top_1pct_depth_usd, 0.0)


if __name__ == "__main__":
    unittest.main()
