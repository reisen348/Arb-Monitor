from __future__ import annotations

import time
import unittest
from urllib.error import HTTPError

from perp_arb.lighter import LighterAdapter, LighterAdapterConfig, LighterClient


class FakeLighterClient(LighterClient):
    def __init__(self) -> None:
        self.config = LighterAdapterConfig(assets=["BTC"])
        self.requests = []

    def get(self, path: str):
        self.requests.append(path)
        if "orderBooks?" in path and "Orders" not in path and "Details" not in path:
            return (
                {
                    "order_books": [
                        {
                            "market_id": 1,
                            "symbol": "BTC",
                            "market_type": "perp",
                            "status": "active",
                            "taker_fee": "0",
                            "maker_fee": "0",
                        },
                        {
                            "market_id": 2,
                            "symbol": "ETH",
                            "market_type": "perp",
                            "status": "active",
                            "taker_fee": "0",
                            "maker_fee": "0",
                        },
                        {
                            "market_id": 3,
                            "symbol": "SOL",
                            "market_type": "perp",
                            "status": "inactive",
                            "taker_fee": "0",
                            "maker_fee": "0",
                        },
                    ]
                },
                20.0,
            )
        if "orderBookDetails" in path:
            return (
                {
                    "order_book_details": [
                        {
                            "market_id": 1,
                            "symbol": "BTC",
                            "market_type": "perp",
                            "status": "active",
                            "taker_fee": "0",
                            "maker_fee": "0",
                            "last_trade_price": "68005.0",
                            "open_interest": "200.5",
                            "daily_quote_token_volume": "2500000.0",
                            "daily_price_change": "0.015",
                        },
                        {
                            "market_id": 2,
                            "symbol": "ETH",
                            "market_type": "perp",
                            "status": "active",
                            "taker_fee": "0",
                            "maker_fee": "0",
                            "last_trade_price": "3400.0",
                            "open_interest": "1500.0",
                            "daily_quote_token_volume": "900000.0",
                            "daily_price_change": "-0.008",
                        },
                        {
                            "market_id": 3,
                            "symbol": "SOL",
                            "market_type": "perp",
                            "status": "inactive",
                            "taker_fee": "0",
                            "maker_fee": "0",
                            "last_trade_price": "150.0",
                            "open_interest": "100.0",
                            "daily_quote_token_volume": "50000.0",
                            "daily_price_change": "0.005",
                        },
                    ]
                },
                22.0,
            )
        if "funding-rates" in path:
            return (
                {
                    "funding_rates": [
                        {
                            "market_id": 1,
                            "exchange": "lighter",
                            "rate": "-0.00000826",
                        },
                        {
                            "market_id": 2,
                            "exchange": "lighter",
                            "rate": "0.00001200",
                        },
                        {
                            "market_id": 1,
                            "exchange": "other_exchange",
                            "rate": "0.0001",
                        },
                    ]
                },
                18.0,
            )
        if "orderBookOrders" in path:
            return (
                {
                    "bids": [
                        {"price": "68000.0", "remaining_base_amount": "0.75"},
                        {"price": "67995.0", "remaining_base_amount": "0.50"},
                        {"price": "67990.0", "remaining_base_amount": "0.30"},
                    ],
                    "asks": [
                        {"price": "68010.0", "remaining_base_amount": "0.80"},
                        {"price": "68015.0", "remaining_base_amount": "0.65"},
                        {"price": "68020.0", "remaining_base_amount": "0.45"},
                    ],
                },
                35.0,
            )
        raise AssertionError(f"unexpected path: {path}")


class LighterAdapterTest(unittest.TestCase):
    def test_poll_converts_responses_to_market_snapshots(self) -> None:
        adapter = LighterAdapter(
            config=LighterAdapterConfig(assets=["BTC"]),
            client=FakeLighterClient(),
        )
        snapshots = adapter.poll()
        self.assertEqual(len(snapshots), 1)
        snapshot = snapshots[0]
        self.assertEqual(snapshot.venue, "lighter")
        self.assertEqual(snapshot.asset, "BTC")
        self.assertEqual(snapshot.quote, "USD")
        self.assertAlmostEqual(snapshot.best_bid, 68000.0)
        self.assertAlmostEqual(snapshot.best_ask, 68010.0)
        self.assertAlmostEqual(snapshot.mark_price, 68005.0)
        # Funding rate: -0.00000826 * 10000 = -0.0826 bps
        self.assertAlmostEqual(snapshot.funding_rate_bps, -0.0826, places=3)
        self.assertGreater(snapshot.oi_usd, 0.0)
        self.assertGreater(snapshot.top_1pct_depth_usd, 0.0)
        self.assertGreater(snapshot.impact_cost_10k_bps, 0.0)

    def test_uses_orderbook_mid_when_last_trade_price_is_stale(self) -> None:
        class StaleLastTradeClient(FakeLighterClient):
            def get(self, path: str):
                response, latency = super().get(path)
                if "orderBookDetails" in path:
                    for item in response["order_book_details"]:
                        if item["symbol"] == "BTC":
                            item["last_trade_price"] = "65000.0"
                return response, latency

        adapter = LighterAdapter(
            config=LighterAdapterConfig(assets=["BTC"]),
            client=StaleLastTradeClient(),
        )
        snapshot = adapter.poll()[0]

        self.assertAlmostEqual(snapshot.best_bid, 68000.0)
        self.assertAlmostEqual(snapshot.best_ask, 68010.0)
        self.assertAlmostEqual(snapshot.mark_price, 68005.0)
        self.assertAlmostEqual(snapshot.metadata["last_trade_price"], 65000.0)

    def test_poll_without_asset_filter_returns_active_perps(self) -> None:
        adapter = LighterAdapter(
            config=LighterAdapterConfig(assets=None),
            client=FakeLighterClient(),
        )
        snapshots = adapter.poll()
        # SOL is inactive, so only BTC and ETH
        self.assertEqual(len(snapshots), 2)
        assets = {s.asset for s in snapshots}
        self.assertEqual(assets, {"BTC", "ETH"})

    def test_zero_fees(self) -> None:
        adapter = LighterAdapter(
            config=LighterAdapterConfig(assets=["BTC"]),
            client=FakeLighterClient(),
        )
        snapshots = adapter.poll()
        self.assertAlmostEqual(snapshots[0].taker_fee_bps, 0.0)
        self.assertAlmostEqual(snapshots[0].maker_fee_bps, 0.0)

    def test_filters_only_lighter_exchange_funding(self) -> None:
        adapter = LighterAdapter(
            config=LighterAdapterConfig(assets=["BTC"]),
            client=FakeLighterClient(),
        )
        snapshots = adapter.poll()
        # Should use lighter exchange rate, not other_exchange
        self.assertAlmostEqual(snapshots[0].funding_rate_bps, -0.0826, places=3)

    def test_top_book_markets_limits_orderbook_requests(self) -> None:
        client = FakeLighterClient()
        adapter = LighterAdapter(
            config=LighterAdapterConfig(assets=None, top_book_markets=1),
            client=client,
        )
        snapshots = adapter.poll()
        self.assertEqual(len(snapshots), 2)
        orderbook_requests = [path for path in client.requests if "orderBookOrders" in path]
        self.assertLessEqual(len(orderbook_requests), 2)
        self.assertEqual(len(orderbook_requests), 2)

    def test_reuses_cached_orderbooks_within_ttl(self) -> None:
        client = FakeLighterClient()
        adapter = LighterAdapter(
            config=LighterAdapterConfig(assets=["BTC"], top_book_markets=1, book_cache_ttl_seconds=60.0),
            client=client,
        )
        first = adapter.poll()
        second = adapter.poll()

        orderbook_requests = [path for path in client.requests if "orderBookOrders" in path]
        self.assertEqual(len(orderbook_requests), 1)
        self.assertFalse(first[0].metadata.get("ticker_only", False))
        self.assertFalse(second[0].metadata.get("ticker_only", False))
        self.assertGreater(second[0].top_1pct_depth_usd, 0.0)

    def test_refetches_orderbooks_when_cache_disabled(self) -> None:
        client = FakeLighterClient()
        adapter = LighterAdapter(
            config=LighterAdapterConfig(assets=["BTC"], top_book_markets=1, book_cache_ttl_seconds=0.0),
            client=client,
        )
        adapter.poll()
        adapter.poll()

        orderbook_requests = [path for path in client.requests if "orderBookOrders" in path]
        self.assertEqual(len(orderbook_requests), 2)

    def test_refreshes_stale_orderbooks_in_background(self) -> None:
        class SlowRefreshClient(FakeLighterClient):
            def __init__(self) -> None:
                super().__init__()
                self.book_calls = 0

            def get(self, path: str):
                if "orderBookOrders" in path:
                    self.book_calls += 1
                    if self.book_calls > 1:
                        time.sleep(0.2)
                return super().get(path)

        client = SlowRefreshClient()
        adapter = LighterAdapter(
            config=LighterAdapterConfig(
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

    def test_background_refresh_covers_non_top_ticker_only_markets(self) -> None:
        client = FakeLighterClient()
        adapter = LighterAdapter(
            config=LighterAdapterConfig(
                assets=None,
                top_book_markets=0,
                background_book_refresh_per_poll=1,
                book_cache_ttl_seconds=60.0,
            ),
            client=client,
        )
        try:
            first = adapter.poll()
            eth_first = next(s for s in first if s.asset == "ETH")
            self.assertTrue(eth_first.metadata.get("ticker_only"))

            for _ in range(20):
                if adapter._book_refresh_future is not None:
                    adapter._book_refresh_future.result(timeout=1.0)
                second = adapter.poll()
                eth_second = next(s for s in second if s.asset == "ETH")
                if not eth_second.metadata.get("ticker_only"):
                    break
                time.sleep(0.01)
            else:
                self.fail("ETH did not receive a background-refreshed orderbook")
        finally:
            adapter.stop()

        self.assertFalse(eth_second.metadata.get("ticker_only", False))
        self.assertAlmostEqual(eth_second.best_bid, 68000.0)
        self.assertAlmostEqual(eth_second.best_ask, 68010.0)

    def test_book_requests_are_sequential_with_rate_limit(self) -> None:
        class SlowLighterClient(FakeLighterClient):
            def __init__(self) -> None:
                super().__init__()
                self.config = LighterAdapterConfig(assets=None, top_book_markets=2, book_request_workers=4,
                                                   min_request_interval_seconds=0.01)

            def get(self, path: str):
                if "orderBookOrders" in path:
                    time.sleep(0.05)
                return super().get(path)

        client = SlowLighterClient()
        adapter = LighterAdapter(config=client.config, client=client)
        snapshots = adapter.poll()

        self.assertEqual(len(snapshots), 2)

    def test_falls_back_to_ticker_when_book_fetch_fails(self) -> None:
        class PartiallyFailingClient(FakeLighterClient):
            def get(self, path: str):
                if "orderBookOrders" in path and "market_id=1" in path:
                    raise RuntimeError("rate limited")
                return super().get(path)

        adapter = LighterAdapter(
            config=LighterAdapterConfig(assets=["BTC"], top_book_markets=1),
            client=PartiallyFailingClient(),
        )
        snapshots = adapter.poll()
        self.assertEqual(len(snapshots), 1)
        self.assertTrue(snapshots[0].metadata.get("ticker_only"))
        self.assertEqual(snapshots[0].top_1pct_depth_usd, 0.0)

    def test_client_retries_on_429(self) -> None:
        class RetryLighterClient(LighterClient):
            def __init__(self) -> None:
                super().__init__(LighterAdapterConfig(rate_limit_retries=2, rate_limit_backoff_seconds=0.01))
                self.calls = 0

        client = RetryLighterClient()
        original_urlopen = __import__("perp_arb.lighter", fromlist=["request"]).request.urlopen

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b'{"ok": true}'

        def fake_urlopen(req, timeout):
            client.calls += 1
            if client.calls == 1:
                raise HTTPError(req.full_url, 429, "Too Many Requests", hdrs=None, fp=None)
            return FakeResponse()

        import perp_arb.lighter as lighter_module
        lighter_module.request.urlopen = fake_urlopen
        try:
            payload, _ = client.get("api/v1/funding-rates")
        finally:
            lighter_module.request.urlopen = original_urlopen

        self.assertEqual(client.calls, 2)
        self.assertEqual(payload, {"ok": True})

    def test_client_respects_min_request_interval(self) -> None:
        client = LighterClient(LighterAdapterConfig(min_request_interval_seconds=0.05))
        t0 = time.monotonic()
        client._wait_for_request_slot()
        client._wait_for_request_slot()
        elapsed = time.monotonic() - t0
        self.assertGreaterEqual(elapsed, 0.05)


if __name__ == "__main__":
    unittest.main()
