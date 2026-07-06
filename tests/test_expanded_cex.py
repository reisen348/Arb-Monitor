from __future__ import annotations

import unittest

from perp_arb.aster import AsterAdapter, AsterAdapterConfig
from perp_arb.bitget import BitgetAdapter, BitgetAdapterConfig
from perp_arb.gate import GateAdapter, GateAdapterConfig
from perp_arb.kraken import KrakenAdapter, KrakenAdapterConfig


class FakeBitgetClient:
    def __init__(self) -> None:
        self.requests = []

    def get(self, path: str, params=None):
        self.requests.append((path, params or {}))
        if "tickers" in path:
            return (
                {
                    "code": "00000",
                    "data": [
                        {
                            "symbol": "BTCUSDT",
                            "lastPr": "68000",
                            "bidPr": "67990",
                            "askPr": "68010",
                            "markPrice": "68000",
                            "indexPrice": "68005",
                            "fundingRate": "0.0001",
                            "quoteVolume": "5000000000",
                            "usdtVolume": "5000000000",
                            "holdingAmount": "120",
                            "open24h": "67000",
                        },
                        {
                            "symbol": "NVDAUSDT",
                            "lastPr": "201",
                            "bidPr": "200.9",
                            "askPr": "201.1",
                            "markPrice": "201",
                            "indexPrice": "200.8",
                            "fundingRate": "0",
                            "quoteVolume": "9000000",
                            "usdtVolume": "9000000",
                            "holdingAmount": "50000",
                            "open24h": "199",
                        },
                        {
                            "symbol": "BBSTOCKUSDT",
                            "lastPr": "10.5",
                            "bidPr": "10.49",
                            "askPr": "10.51",
                            "markPrice": "10.5",
                            "indexPrice": "10.52",
                            "fundingRate": "0.0001",
                            "quoteVolume": "1000000",
                            "usdtVolume": "1000000",
                            "holdingAmount": "10000",
                            "open24h": "10.1",
                        },
                    ],
                },
                12.0,
            )
        if "merge-depth" in path:
            return (
                {
                    "code": "00000",
                    "data": {
                        "bids": [[67990.0, 1.0], [67980.0, 1.0]],
                        "asks": [[68010.0, 1.0], [68020.0, 1.0]],
                    },
                },
                20.0,
            )
        raise AssertionError(path)


class FakeGateClient:
    def __init__(self) -> None:
        self.requests = []

    def get(self, path: str, params=None):
        self.requests.append((path, params or {}))
        if "tickers" in path:
            return (
                [
                    {
                        "contract": "BTC_USDT",
                        "last": "68000",
                        "mark_price": "68000",
                        "index_price": "68010",
                        "highest_bid": "67990",
                        "lowest_ask": "68010",
                        "funding_rate": "0.0001",
                        "volume_24h_quote": "5000000000",
                        "total_size": "2000000",
                        "quanto_multiplier": "0.0001",
                    },
                    {
                        "contract": "AAPL_USDT",
                        "last": "288",
                        "mark_price": "288",
                        "index_price": "287.9",
                        "highest_bid": "287.7",
                        "lowest_ask": "288.2",
                        "funding_rate": "0",
                        "volume_24h_quote": "3000000",
                        "total_size": "100000",
                        "quanto_multiplier": "0.01",
                    },
                    {
                        "contract": "EDGE_USDT",
                        "last": "0.064",
                        "mark_price": "0.064",
                        "index_price": "0.065",
                        "highest_bid": "0.063",
                        "lowest_ask": "0.065",
                        "funding_rate": "0",
                        "volume_24h_quote": "1000000",
                        "total_size": "100000",
                        "quanto_multiplier": "10",
                    },
                    {
                        "contract": "EDGEX_USDT",
                        "last": "0.337",
                        "mark_price": "0.337",
                        "index_price": "0.337",
                        "highest_bid": "0.336",
                        "lowest_ask": "0.338",
                        "funding_rate": "0",
                        "volume_24h_quote": "900000",
                        "total_size": "100000",
                        "quanto_multiplier": "10",
                    },
                ],
                10.0,
            )
        if "order_book" in path:
            return (
                {
                    "bids": [{"p": "67990", "s": 1000}],
                    "asks": [{"p": "68010", "s": 1000}],
                },
                18.0,
            )
        raise AssertionError(path)


class FakeKrakenClient:
    def __init__(self) -> None:
        self.requests = []

    def get(self, path: str, params=None):
        self.requests.append((path, params or {}))
        if "tickers" in path:
            return (
                {
                    "result": "success",
                    "tickers": [
                        {
                            "symbol": "PF_XBTUSD",
                            "tag": "perpetual",
                            "pair": "XBT:USD",
                            "markPrice": 68000.0,
                            "bid": 67990.0,
                            "ask": 68010.0,
                            "volumeQuote": 400000000.0,
                            "openInterest": 250.0,
                            "fundingRate": 0.68,
                            "indexPrice": 68005.0,
                            "open24h": 67000.0,
                            "suspended": False,
                        },
                        {
                            "symbol": "PF_AAPLXUSD",
                            "tag": "perpetual",
                            "pair": "AAPLx:USD",
                            "markPrice": 288.0,
                            "bid": 287.7,
                            "ask": 288.4,
                            "volumeQuote": 30000.0,
                            "openInterest": 100.0,
                            "fundingRate": 0.02881,
                            "indexPrice": 288.1,
                            "open24h": 290.0,
                            "suspended": False,
                        },
                        {
                            "symbol": "PF_XAUTUSD",
                            "tag": "perpetual",
                            "pair": "XAUT:USD",
                            "markPrice": 4161.53560689251,
                            "bid": 4161.5,
                            "ask": 4161.9,
                            "volumeQuote": 543596.0846,
                            "openInterest": 648.326,
                            "fundingRate": 0.08963465221566805,
                            "indexPrice": 4160.7,
                            "open24h": 4157.4,
                            "suspended": False,
                        },
                        {
                            "symbol": "FI_XBTUSD_260925",
                            "tag": "quarter",
                            "pair": "XBT:USD",
                            "markPrice": 68100.0,
                        },
                    ],
                },
                15.0,
            )
        if "orderbook" in path:
            return (
                {
                    "result": "success",
                    "orderBook": {
                        "bids": [[67990.0, 1.0], [67980.0, 1.0]],
                        "asks": [[68010.0, 1.0], [68020.0, 1.0]],
                    },
                },
                22.0,
            )
        raise AssertionError(path)


class FakeAsterClient:
    def __init__(self) -> None:
        self.requests = []

    def get(self, path: str, params=None):
        params = params or {}
        self.requests.append((path, params))
        if "exchangeInfo" in path:
            return (
                {
                    "symbols": [
                        {
                            "symbol": "BTCUSDT",
                            "status": "TRADING",
                            "contractType": "PERPETUAL",
                            "baseAsset": "BTC",
                            "quoteAsset": "USDT",
                            "underlyingSubType": ["Top"],
                        },
                        {
                            "symbol": "NVDAUSDT",
                            "status": "TRADING",
                            "contractType": "PERPETUAL",
                            "baseAsset": "NVDA",
                            "quoteAsset": "USDT",
                            "underlyingSubType": ["STOCK"],
                            "symbolType": 1,
                        },
                    ]
                },
                5.0,
            )
        if "premiumIndex" in path:
            return (
                [
                    {"symbol": "BTCUSDT", "markPrice": "68000", "indexPrice": "68005", "lastFundingRate": "0.0001", "nextFundingTime": 1700003600000},
                    {"symbol": "NVDAUSDT", "markPrice": "201", "indexPrice": "200.8", "lastFundingRate": "0", "nextFundingTime": 1700003600000},
                ],
                8.0,
            )
        if "ticker/24hr" in path:
            return (
                [
                    {"symbol": "BTCUSDT", "lastPrice": "68000", "openPrice": "67000", "quoteVolume": "5000000000"},
                    {"symbol": "NVDAUSDT", "lastPrice": "201", "openPrice": "199", "quoteVolume": "9000000"},
                ],
                9.0,
            )
        if "ticker/bookTicker" in path:
            return (
                [
                    {"symbol": "BTCUSDT", "bidPrice": "67990", "askPrice": "68010"},
                    {"symbol": "NVDAUSDT", "bidPrice": "200.9", "askPrice": "201.1"},
                ],
                7.0,
            )
        if "openInterest" in path:
            return ({"symbol": params["symbol"], "openInterest": "200"}, 11.0)
        if "depth" in path:
            return (
                {
                    "bids": [["67990", "1"], ["67980", "1"]],
                    "asks": [["68010", "1"], ["68020", "1"]],
                },
                16.0,
            )
        raise AssertionError(path)


class ExpandedCexAdapterTest(unittest.TestCase):
    def test_bitget_keeps_stock_markets_and_limits_depth(self) -> None:
        client = FakeBitgetClient()
        adapter = BitgetAdapter(BitgetAdapterConfig(top_book_markets=1), client=client)
        snapshots = adapter.poll()

        self.assertEqual({snap.asset for snap in snapshots}, {"BTC", "NVDA", "BB"})
        self.assertTrue(all(snap.oi_usd > 0 for snap in snapshots))
        bb = next(snap for snap in snapshots if snap.metadata["symbol"] == "BBSTOCKUSDT")
        self.assertEqual(bb.asset, "BB")
        self.assertTrue(bb.metadata["stock_like"])
        self.assertLessEqual(len([r for r in client.requests if "merge-depth" in r[0]]), 2)

    def test_gate_keeps_stock_markets_and_converts_contract_oi(self) -> None:
        client = FakeGateClient()
        adapter = GateAdapter(GateAdapterConfig(top_book_markets=1), client=client)
        snapshots = adapter.poll()

        self.assertEqual({snap.asset for snap in snapshots}, {"BTC", "AAPL", "EDGE", "GATE_EDGE"})
        self.assertTrue(all(snap.oi_usd > 0 for snap in snapshots))
        by_symbol = {snap.metadata["symbol"]: snap for snap in snapshots}
        self.assertEqual(by_symbol["EDGEX_USDT"].asset, "EDGE")
        self.assertEqual(by_symbol["EDGE_USDT"].asset, "GATE_EDGE")
        self.assertLessEqual(len([r for r in client.requests if "order_book" in r[0]]), 1)

    def test_kraken_normalizes_xbt_stock_suffix_and_absolute_funding(self) -> None:
        client = FakeKrakenClient()
        adapter = KrakenAdapter(KrakenAdapterConfig(top_book_markets=1), client=client)
        snapshots = adapter.poll()

        self.assertEqual({snap.asset for snap in snapshots}, {"BTC", "AAPL", "XAUT"})
        self.assertEqual({snap.quote for snap in snapshots}, {"USD"})
        self.assertTrue(all(snap.metadata["funding_interval_hours"] == 1.0 for snap in snapshots))
        btc = next(snap for snap in snapshots if snap.asset == "BTC")
        aapl = next(snap for snap in snapshots if snap.asset == "AAPL")
        xaut = next(snap for snap in snapshots if snap.asset == "XAUT")
        self.assertAlmostEqual(btc.funding_rate_bps, 0.68 / 68005.0 * 10_000.0)
        self.assertAlmostEqual(aapl.funding_rate_bps, 0.02881 / 288.1 * 10_000.0)
        self.assertAlmostEqual(xaut.funding_rate_bps, 0.08963465221566805 / 4160.7 * 10_000.0)
        self.assertEqual(xaut.metadata["funding_rate_source"], "fundingRate/indexPrice")
        self.assertLessEqual(len([r for r in client.requests if "orderbook" in r[0]]), 1)

    def test_aster_discovers_stock_symbols_and_uses_volume_oi_fallback(self) -> None:
        client = FakeAsterClient()
        adapter = AsterAdapter(AsterAdapterConfig(top_book_markets=1), client=client)
        snapshots = adapter.poll()

        by_asset = {snap.asset: snap for snap in snapshots}
        self.assertEqual(set(by_asset), {"BTC", "NVDA"})
        self.assertGreater(by_asset["BTC"].oi_usd, 0.0)
        self.assertGreater(by_asset["NVDA"].oi_usd, 0.0)
        self.assertTrue(by_asset["NVDA"].metadata["stock_like"])
        self.assertLessEqual(len([r for r in client.requests if "depth" in r[0]]), 1)

    def test_aster_uses_book_mid_price_over_premium_mark_for_display_price(self) -> None:
        client = FakeAsterClient()
        adapter = AsterAdapter(AsterAdapterConfig(top_book_markets=1), client=client)
        snapshots = adapter.poll()

        btc = next(snap for snap in snapshots if snap.asset == "BTC")

        self.assertAlmostEqual(btc.best_bid, 67990.0)
        self.assertAlmostEqual(btc.best_ask, 68010.0)
        self.assertAlmostEqual(btc.mark_price, 68000.0)
        self.assertAlmostEqual(btc.metadata["premium_mark_price"], 68000.0)
        self.assertAlmostEqual(btc.metadata["book_mid_price"], 68000.0)

    def test_aster_ticker_only_uses_book_ticker_mid_when_premium_mark_diverges(self) -> None:
        class DivergentAsterClient(FakeAsterClient):
            def get(self, path: str, params=None):
                if "premiumIndex" in path:
                    return (
                        [
                            {
                                "symbol": "BTCUSDT",
                                "markPrice": "69000",
                                "indexPrice": "68005",
                                "lastFundingRate": "0.0001",
                                "nextFundingTime": 1700003600000,
                            },
                            {
                                "symbol": "NVDAUSDT",
                                "markPrice": "205",
                                "indexPrice": "200.8",
                                "lastFundingRate": "0",
                                "nextFundingTime": 1700003600000,
                            },
                        ],
                        8.0,
                    )
                return super().get(path, params)

        client = DivergentAsterClient()
        adapter = AsterAdapter(AsterAdapterConfig(top_book_markets=0), client=client)
        snapshots = adapter.poll()

        btc = next(snap for snap in snapshots if snap.asset == "BTC")

        self.assertAlmostEqual(btc.best_bid, 67990.0)
        self.assertAlmostEqual(btc.best_ask, 68010.0)
        self.assertAlmostEqual(btc.mark_price, 68000.0)
        self.assertAlmostEqual(btc.metadata["premium_mark_price"], 69000.0)
        self.assertAlmostEqual(btc.metadata["book_mid_price"], 68000.0)
        self.assertGreater(btc.metadata["mark_book_deviation_bps"], 100.0)


if __name__ == "__main__":
    unittest.main()
