from __future__ import annotations

import unittest

from perp_arb.bybit import BybitAdapter, BybitAdapterConfig, BybitClient


class FakeBybitClient(BybitClient):
    def __init__(self) -> None:
        self.config = BybitAdapterConfig(assets=["BTC", "ETH"])
        self.requests = []

    def get(self, path: str):
        self.requests.append(path)
        if "tickers" in path:
            return (
                {
                    "result": {
                        "list": [
                            {
                                "symbol": "BTCUSDT",
                                "lastPrice": "68003.0",
                                "bid1Price": "68000.0",
                                "ask1Price": "68010.0",
                                "markPrice": "68005.0",
                                "indexPrice": "68000.0",
                                "fundingRate": "0.0001",
                                "nextFundingTime": "1700003600000",
                                "openInterest": "125.5",
                                "volume24h": "15000.0",
                                "turnover24h": "1000000000.0",
                                "prevPrice24h": "67000.0",
                                "price24hPcnt": "0.015",
                            },
                            {
                                "symbol": "ETHUSDT",
                                "lastPrice": "3399.0",
                                "bid1Price": "3398.0",
                                "ask1Price": "3402.0",
                                "markPrice": "3400.0",
                                "indexPrice": "3398.0",
                                "fundingRate": "-0.00005",
                                "nextFundingTime": "1700003600000",
                                "openInterest": "5000.0",
                                "volume24h": "80000.0",
                                "turnover24h": "270000000.0",
                                "prevPrice24h": "3350.0",
                                "price24hPcnt": "-0.005",
                            },
                            {
                                "symbol": "BTCPERP",
                                "lastPrice": "68000.0",
                                "bid1Price": "67999.0",
                                "ask1Price": "68001.0",
                                "markPrice": "68000.0",
                                "indexPrice": "68000.0",
                                "fundingRate": "0.0001",
                                "openInterest": "10.0",
                                "volume24h": "100.0",
                                "turnover24h": "6800000.0",
                            },
                        ]
                    }
                },
                20.0,
            )
        if "orderbook" in path:
            return (
                {
                    "result": {
                        "b": [
                            ["68000.0", "0.80"],
                            ["67995.0", "0.60"],
                        ],
                        "a": [
                            ["68010.0", "0.90"],
                            ["68015.0", "0.70"],
                        ],
                    }
                },
                35.0,
            )
        raise AssertionError(f"unexpected path: {path}")


class BybitAdapterTest(unittest.TestCase):
    def test_poll_converts_responses_to_snapshots(self) -> None:
        adapter = BybitAdapter(
            config=BybitAdapterConfig(assets=["BTC"]),
            client=FakeBybitClient(),
        )
        snapshots = adapter.poll()
        self.assertEqual(len(snapshots), 1)
        s = snapshots[0]
        self.assertEqual(s.venue, "bybit")
        self.assertEqual(s.asset, "BTC")
        self.assertEqual(s.quote, "USDT")
        self.assertAlmostEqual(s.mark_price, 68005.0)
        self.assertAlmostEqual(s.index_price, 68000.0)
        self.assertAlmostEqual(s.best_bid, 68000.0)
        self.assertAlmostEqual(s.best_ask, 68010.0)
        self.assertAlmostEqual(s.funding_rate_bps, 1.0)
        self.assertEqual(s.metadata["funding_interval_hours"], 8.0)
        self.assertIsNotNone(s.next_funding_time)
        self.assertEqual(s.next_funding_time.isoformat(), "2023-11-14T23:13:20+00:00")
        self.assertAlmostEqual(s.taker_fee_bps, 5.5)
        self.assertGreater(s.oi_usd, 0.0)

    def test_filters_non_usdt_symbols(self) -> None:
        adapter = BybitAdapter(
            config=BybitAdapterConfig(assets=None),
            client=FakeBybitClient(),
        )
        snapshots = adapter.poll()
        assets = {s.asset for s in snapshots}
        # BTCPERP should be excluded
        self.assertEqual(assets, {"BTC", "ETH"})

    def test_single_batch_request_for_tickers(self) -> None:
        client = FakeBybitClient()
        adapter = BybitAdapter(
            config=BybitAdapterConfig(assets=["BTC"], top_book_markets=1),
            client=client,
        )
        adapter.poll()
        # 1 ticker batch + 1 orderbook = 2 requests total
        self.assertEqual(len(client.requests), 2)


if __name__ == "__main__":
    unittest.main()
