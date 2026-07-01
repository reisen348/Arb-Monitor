from __future__ import annotations

import unittest

from perp_arb.grvt import GrvtAdapter, GrvtAdapterConfig, GrvtMarketDataClient
from perp_arb.market_data import OpportunityBuilder


class FakeGrvtClient(GrvtMarketDataClient):
    def __init__(self) -> None:
        self.config = GrvtAdapterConfig(assets=["BTC"], quotes=["USDT"])
        self.requests = []

    def post_lite(self, path: str, payload: dict):
        self.requests.append((path, payload))
        if path == "instruments":
            return (
                {
                    "r": [
                        {
                            "i": "BTC_USDT_Perp",
                            "b": "BTC",
                            "q": "USDT",
                            "k": "PERPETUAL",
                            "fi": 8,
                            "ts": "0.1",
                            "mn": "10.0",
                        }
                    ]
                },
                40.0,
            )
        if path == "ticker":
            return (
                {
                    "r": {
                        "et": "1700000000000000000",
                        "i": "BTC_USDT_Perp",
                        "mp": "68005.0",
                        "ip": "68000.0",
                        "mp1": "68002.5",
                        "bb": "68000.0",
                        "bb1": "0.8",
                        "ba": "68005.0",
                        "ba1": "0.9",
                        "bv1": "1500000.0",
                        "sv1": "1400000.0",
                        "op": "67000.0",
                        "oi": "125.0",
                        "fr2": "0.0012",
                        "nf": "1700003600000000000",
                    }
                },
                55.0,
            )
        if path == "book":
            return (
                {
                    "r": {
                        "et": "1700000000000000000",
                        "i": "BTC_USDT_Perp",
                        "b": [
                            {"p": "68000.0", "s": "0.8", "no": 5},
                            {"p": "67995.0", "s": "0.6", "no": 4},
                        ],
                        "a": [
                            {"p": "68005.0", "s": "0.9", "no": 6},
                            {"p": "68010.0", "s": "0.7", "no": 5},
                        ],
                    }
                },
                60.0,
            )
        raise AssertionError(path)


class GrvtAdapterTest(unittest.TestCase):
    def test_poll_converts_grvt_market_data_to_snapshot(self) -> None:
        adapter = GrvtAdapter(
            config=GrvtAdapterConfig(assets=["BTC"], quotes=["USDT"]),
            client=FakeGrvtClient(),
        )
        snapshots = adapter.poll()
        self.assertEqual(len(snapshots), 1)
        snapshot = snapshots[0]
        self.assertEqual(snapshot.venue, "grvt")
        self.assertEqual(snapshot.asset, "BTC")
        self.assertEqual(snapshot.quote, "USDT")
        self.assertAlmostEqual(snapshot.funding_rate_bps, 0.12)
        self.assertGreater(snapshot.top_1pct_depth_usd, 50_000.0)
        self.assertGreater(snapshot.oi_usd, 0.0)

    def test_builder_normalizes_stable_quotes_for_cross_venue_pairing(self) -> None:
        adapter = GrvtAdapter(
            config=GrvtAdapterConfig(assets=["BTC"], quotes=["USDT"]),
            client=FakeGrvtClient(),
        )
        grvt_snapshot = adapter.poll()[0]
        hyperliquid_like = type(grvt_snapshot)(
            **{
                **grvt_snapshot.__dict__,
                "venue": "hyperliquid",
                "quote": "USD",
                "best_bid": 68110.0,
                "best_ask": 68115.0,
            }
        )
        opportunities = OpportunityBuilder().build([grvt_snapshot, hyperliquid_like])
        self.assertEqual(len(opportunities), 1)
        self.assertEqual(opportunities[0].quote, "USD")


if __name__ == "__main__":
    unittest.main()
