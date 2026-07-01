from __future__ import annotations

import unittest

from perp_arb.okx import OkxAdapter, OkxAdapterConfig, OkxClient


class FakeOkxClient(OkxClient):
    def __init__(self) -> None:
        self.config = OkxAdapterConfig(assets=["BTC", "ETH"])
        self.requests = []

    def get(self, path: str):
        self.requests.append(path)
        if "market/tickers" in path:
            return (
                {
                    "code": "0",
                    "data": [
                        {
                            "instId": "BTC-USDT-SWAP",
                            "last": "68003.0",
                            "bidPx": "68000.0",
                            "askPx": "68010.0",
                            "vol24h": "15000",
                            "volCcy24h": "1020000000",
                            "ts": "1700000000000",
                        },
                        {
                            "instId": "ETH-USDT-SWAP",
                            "last": "3399.0",
                            "bidPx": "3398.0",
                            "askPx": "3402.0",
                            "vol24h": "80000",
                            "volCcy24h": "270000000",
                            "ts": "1700000000000",
                        },
                        {
                            "instId": "BTC-USD-SWAP",
                            "last": "68000.0",
                            "bidPx": "67999.0",
                            "askPx": "68001.0",
                            "vol24h": "100",
                            "volCcy24h": "6800000",
                            "ts": "1700000000000",
                        },
                    ],
                },
                25.0,
            )
        if "mark-price" in path:
            return (
                {
                    "code": "0",
                    "data": [
                        {"instId": "BTC-USDT-SWAP", "markPx": "68005.0"},
                        {"instId": "ETH-USDT-SWAP", "markPx": "3400.0"},
                        {"instId": "BTC-USD-SWAP", "markPx": "68000.0"},
                    ],
                },
                20.0,
            )
        if "open-interest" in path:
            return (
                {
                    "code": "0",
                    "data": [
                        {"instId": "BTC-USDT-SWAP", "oi": "125.5", "oiCcy": "125.5", "oiUsd": "8534627.5"},
                        {"instId": "ETH-USDT-SWAP", "oi": "5000.0", "oiCcy": "5000.0", "oiUsd": "17000000.0"},
                    ],
                },
                18.0,
            )
        if "funding-rate" in path:
            return (
                {
                    "code": "0",
                    "data": [
                        {
                            "instId": path.split("instId=")[1] if "instId=" in path else "",
                            "fundingRate": "0.0001",
                            "nextFundingTime": "1700003600000",
                        }
                    ],
                },
                15.0,
            )
        if "books" in path:
            return (
                {
                    "code": "0",
                    "data": [
                        {
                            "bids": [
                                ["68000.0", "0.80", "0", "5"],
                                ["67995.0", "0.60", "0", "4"],
                            ],
                            "asks": [
                                ["68010.0", "0.90", "0", "6"],
                                ["68015.0", "0.70", "0", "5"],
                            ],
                            "ts": "1700000000000",
                        }
                    ],
                },
                40.0,
            )
        raise AssertionError(f"unexpected path: {path}")


class OkxAdapterTest(unittest.TestCase):
    def test_poll_converts_responses_to_snapshots(self) -> None:
        adapter = OkxAdapter(
            config=OkxAdapterConfig(assets=["BTC"]),
            client=FakeOkxClient(),
        )
        snapshots = adapter.poll()
        self.assertEqual(len(snapshots), 1)
        s = snapshots[0]
        self.assertEqual(s.venue, "okx")
        self.assertEqual(s.asset, "BTC")
        self.assertEqual(s.quote, "USDT")
        self.assertAlmostEqual(s.mark_price, 68005.0)
        self.assertAlmostEqual(s.best_bid, 68000.0)
        self.assertAlmostEqual(s.best_ask, 68010.0)
        # funding: 0.0001 * 10000 = 1.0 bps
        self.assertAlmostEqual(s.funding_rate_bps, 1.0)
        self.assertAlmostEqual(s.taker_fee_bps, 5.0)
        self.assertGreater(s.oi_usd, 0.0)

    def test_filters_non_usdt_swap(self) -> None:
        adapter = OkxAdapter(
            config=OkxAdapterConfig(assets=None),
            client=FakeOkxClient(),
        )
        snapshots = adapter.poll()
        assets = {s.asset for s in snapshots}
        # BTC-USD-SWAP should be excluded
        self.assertEqual(assets, {"BTC", "ETH"})

    def test_funding_only_fetched_for_top_markets(self) -> None:
        client = FakeOkxClient()
        adapter = OkxAdapter(
            config=OkxAdapterConfig(assets=None, top_book_markets=1),
            client=client,
        )
        adapter.poll()
        funding_requests = [r for r in client.requests if "funding-rate" in r]
        # top_book_markets=1 selects top1 by volume ∪ top1 by OI (may overlap or not)
        self.assertLessEqual(len(funding_requests), 2)


if __name__ == "__main__":
    unittest.main()
