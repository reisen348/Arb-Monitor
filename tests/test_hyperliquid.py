from __future__ import annotations

import unittest

from perp_arb.hyperliquid import (
    HyperliquidAdapter,
    HyperliquidAdapterConfig,
    HyperliquidInfoClient,
)


class FakeHyperliquidClient(HyperliquidInfoClient):
    def __init__(self) -> None:
        self.config = HyperliquidAdapterConfig(assets=["BTC"])
        self.requests = []

    def post_info(self, payload: dict):
        self.requests.append(payload)
        if payload["type"] == "metaAndAssetCtxs":
            return (
                [
                    {
                        "universe": [
                            {"name": "BTC", "szDecimals": 5, "maxLeverage": 50},
                            {"name": "ETH", "szDecimals": 4, "maxLeverage": 50},
                        ]
                    },
                    [
                        {
                            "dayNtlVlm": "1169046.29406",
                            "funding": "0.0000125",
                            "impactPxs": ["14.3047", "14.3444"],
                            "markPx": "68020.0",
                            "midPx": "68010.0",
                            "openInterest": "688.11",
                            "oraclePx": "68000.0",
                            "premium": "0.00031774",
                            "prevDayPx": "67000.0",
                        },
                        {
                            "dayNtlVlm": "1426126.295175",
                            "funding": "0.0000105",
                            "impactPxs": ["3400.0", "3405.0"],
                            "markPx": "3402.0",
                            "midPx": "3401.0",
                            "openInterest": "1882.55",
                            "oraclePx": "3403.0",
                            "premium": "0.00028119",
                            "prevDayPx": "3300.0",
                        },
                    ],
                ],
                35.0,
            )
        if payload["type"] == "l2Book":
            return (
                {
                    "coin": payload["coin"],
                    "time": 1_700_000_000_000,
                    "levels": [
                        [
                            {"px": "68000.0", "sz": "0.75", "n": 6},
                            {"px": "67990.0", "sz": "0.50", "n": 4},
                        ],
                        [
                            {"px": "68010.0", "sz": "0.80", "n": 5},
                            {"px": "68020.0", "sz": "0.60", "n": 4},
                        ],
                    ],
                },
                42.0,
            )
        raise AssertionError(f"unexpected payload: {payload}")


class FakeHyperliquidXyzClient(HyperliquidInfoClient):
    def __init__(self) -> None:
        self.config = HyperliquidAdapterConfig(assets=None, top_book_markets=1)
        self.requests = []

    def post_info(self, payload: dict):
        self.requests.append(payload)
        if payload["type"] == "metaAndAssetCtxs" and "dex" not in payload:
            return (
                [
                    {"universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 50}]},
                    [{
                        "dayNtlVlm": "1000",
                        "funding": "0.00001",
                        "impactPxs": ["68000", "68010"],
                        "markPx": "68005",
                        "midPx": "68005",
                        "openInterest": "1",
                        "oraclePx": "68000",
                        "premium": "0.0001",
                        "prevDayPx": "67000",
                    }],
                ],
                10.0,
            )
        if payload["type"] == "metaAndAssetCtxs" and payload.get("dex") == "xyz":
            return (
                [
                    {"universe": [{"name": "xyz:AAPL", "szDecimals": 3, "maxLeverage": 30}]},
                    [{
                        "dayNtlVlm": "2000",
                        "funding": "0.00002",
                        "impactPxs": ["278.39", "278.42"],
                        "markPx": "278.4",
                        "midPx": "278.405",
                        "openInterest": "10",
                        "oraclePx": "278.3",
                        "premium": "0.0002",
                        "prevDayPx": "275",
                    }],
                ],
                11.0,
            )
        if payload["type"] == "l2Book":
            price = "278.39" if payload["coin"] == "xyz:AAPL" else "68000.0"
            ask = "278.42" if payload["coin"] == "xyz:AAPL" else "68010.0"
            return (
                {
                    "coin": payload["coin"],
                    "time": 1_700_000_000_000,
                    "levels": [
                        [{"px": price, "sz": "10", "n": 1}],
                        [{"px": ask, "sz": "10", "n": 1}],
                    ],
                },
                12.0,
            )
        raise AssertionError(f"unexpected payload: {payload}")


class HyperliquidAdapterTest(unittest.TestCase):
    def test_poll_converts_responses_to_market_snapshots(self) -> None:
        adapter = HyperliquidAdapter(
            config=HyperliquidAdapterConfig(assets=["BTC"], extra_dexes=()),
            client=FakeHyperliquidClient(),
        )
        snapshots = adapter.poll()
        self.assertEqual(len(snapshots), 1)
        snapshot = snapshots[0]
        self.assertEqual(snapshot.venue, "hyperliquid")
        self.assertEqual(snapshot.asset, "BTC")
        self.assertAlmostEqual(snapshot.best_bid, 68000.0)
        self.assertAlmostEqual(snapshot.best_ask, 68010.0)
        self.assertAlmostEqual(snapshot.funding_rate_bps, 0.125)
        self.assertGreater(snapshot.top_1pct_depth_usd, 50_000.0)
        self.assertGreater(snapshot.impact_cost_10k_bps, 0.0)
        self.assertGreater(snapshot.oi_usd, 0.0)

    def test_default_poll_includes_xyz_stock_dex(self) -> None:
        client = FakeHyperliquidXyzClient()
        adapter = HyperliquidAdapter(
            config=HyperliquidAdapterConfig(assets=None, top_book_markets=1),
            client=client,
        )

        snapshots = adapter.poll()

        by_asset = {snapshot.asset: snapshot for snapshot in snapshots}
        self.assertIn("BTC", by_asset)
        self.assertIn("AAPL", by_asset)
        self.assertEqual(by_asset["AAPL"].venue, "hyperliquid:xyz")
        self.assertEqual(by_asset["AAPL"].metadata["symbol"], "xyz:AAPL")
        self.assertTrue(by_asset["AAPL"].metadata["stock_like"])
        self.assertIn({"type": "metaAndAssetCtxs", "dex": "xyz"}, client.requests)

    def test_asset_filter_matches_xyz_display_asset(self) -> None:
        adapter = HyperliquidAdapter(
            config=HyperliquidAdapterConfig(assets=["AAPL"], top_book_markets=0),
            client=FakeHyperliquidXyzClient(),
        )

        snapshots = adapter.poll()

        self.assertEqual([snapshot.asset for snapshot in snapshots], ["AAPL"])


if __name__ == "__main__":
    unittest.main()
