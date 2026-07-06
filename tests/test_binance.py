from __future__ import annotations

import unittest

from perp_arb.binance import BinanceAdapter, BinanceAdapterConfig, BinanceClient


class FakeBinanceClient(BinanceClient):
    def __init__(self) -> None:
        self.config = BinanceAdapterConfig(assets=["BTC", "ETH"])
        self.requests = []

    def get(self, path: str):
        self.requests.append(path)
        if "premiumIndex" in path:
            return (
                [
                    {
                        "symbol": "BTCUSDT",
                        "pair": "BTCUSDT",
                        "markPrice": "68005.0",
                        "indexPrice": "68000.0",
                        "lastFundingRate": "0.0001",
                        "nextFundingTime": 1700003600000,
                        "interestRate": "0.0001",
                    },
                    {
                        "symbol": "ETHUSDT",
                        "pair": "ETHUSDT",
                        "markPrice": "3400.0",
                        "indexPrice": "3398.0",
                        "lastFundingRate": "-0.00005",
                        "nextFundingTime": 1700003600000,
                        "interestRate": "0.0001",
                    },
                    {
                        "symbol": "BTCUSD_PERP",
                        "pair": "BTCUSD",
                        "markPrice": "68000.0",
                        "indexPrice": "68000.0",
                        "lastFundingRate": "0.0001",
                    },
                ],
                25.0,
            )
        if "ticker/24hr" in path:
            return (
                [
                    {
                        "symbol": "BTCUSDT",
                        "lastPrice": "68003.0",
                        "quoteVolume": "5000000000.0",
                        "openPrice": "67000.0",
                        "priceChangePercent": "1.2",
                    },
                    {
                        "symbol": "ETHUSDT",
                        "lastPrice": "3399.0",
                        "quoteVolume": "2000000000.0",
                        "openPrice": "3350.0",
                        "priceChangePercent": "-0.5",
                    },
                ],
                30.0,
            )
        if "ticker/bookTicker" in path:
            return (
                [
                    {
                        "symbol": "BTCUSDT",
                        "bidPrice": "68000.0",
                        "bidQty": "3.0",
                        "askPrice": "68010.0",
                        "askQty": "2.8",
                        "time": 1700000000000,
                    },
                    {
                        "symbol": "ETHUSDT",
                        "bidPrice": "3398.0",
                        "bidQty": "45.0",
                        "askPrice": "3402.0",
                        "askQty": "38.0",
                        "time": 1700000000000,
                    },
                ],
                28.0,
            )
        if "openInterest" in path:
            symbol = path.rsplit("=", 1)[-1]
            values = {"BTCUSDT": "125.5", "ETHUSDT": "5000.0"}
            return ({"symbol": symbol, "openInterest": values[symbol], "time": 1700000000000}, 15.0)
        if "depth" in path:
            return (
                {
                    "bids": [
                        ["68000.0", "0.80"],
                        ["67995.0", "0.60"],
                    ],
                    "asks": [
                        ["68010.0", "0.90"],
                        ["68015.0", "0.70"],
                    ],
                },
                40.0,
            )
        raise AssertionError(f"unexpected path: {path}")


class BinanceAdapterTest(unittest.TestCase):
    def test_poll_converts_responses_to_snapshots(self) -> None:
        adapter = BinanceAdapter(
            config=BinanceAdapterConfig(assets=["BTC"]),
            client=FakeBinanceClient(),
        )
        snapshots = adapter.poll()
        self.assertEqual(len(snapshots), 1)
        s = snapshots[0]
        self.assertEqual(s.venue, "binance")
        self.assertEqual(s.asset, "BTC")
        self.assertEqual(s.quote, "USDT")
        self.assertAlmostEqual(s.mark_price, 68005.0)
        self.assertAlmostEqual(s.index_price, 68000.0)
        self.assertAlmostEqual(s.best_bid, 68000.0)
        self.assertAlmostEqual(s.best_ask, 68010.0)
        # funding: 0.0001 * 10000 = 1.0 bps
        self.assertAlmostEqual(s.funding_rate_bps, 1.0)
        self.assertAlmostEqual(s.taker_fee_bps, 4.5)
        self.assertGreater(s.oi_usd, 0.0)
        self.assertIn("fapi/v1/ticker/bookTicker", adapter.client.requests)
        self.assertIn("fapi/v1/openInterest?symbol=BTCUSDT", adapter.client.requests)

    def test_filters_out_non_usdt_contracts(self) -> None:
        adapter = BinanceAdapter(
            config=BinanceAdapterConfig(assets=None),
            client=FakeBinanceClient(),
        )
        snapshots = adapter.poll()
        assets = {s.asset for s in snapshots}
        # BTCUSD_PERP should be excluded
        self.assertEqual(assets, {"BTC", "ETH"})

    def test_top_book_markets_limits_depth_requests(self) -> None:
        client = FakeBinanceClient()
        adapter = BinanceAdapter(
            config=BinanceAdapterConfig(assets=None, top_book_markets=1),
            client=client,
        )
        snapshots = adapter.poll()
        self.assertEqual(len(snapshots), 2)
        depth_requests = [r for r in client.requests if "depth" in r]
        # top_book_markets=1 selects top1 by volume ∪ top1 by OI (may overlap or not)
        self.assertLessEqual(len(depth_requests), 2)


if __name__ == "__main__":
    unittest.main()
