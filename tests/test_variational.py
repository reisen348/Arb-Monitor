from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from typing import Tuple

from perp_arb.variational import VariationalAdapter, VariationalAdapterConfig


FAKE_STATS = {
    "num_markets": 6,
    "listings": [
        {
            "ticker": "BTC",
            "name": "Bitcoin",
            "mark_price": "100.0",
            "volume_24h": "1000000",
            "open_interest": {"long_open_interest": "100000", "short_open_interest": "100000"},
            "funding_rate": "0.0001",
            "funding_interval_s": 3600,
            "base_spread_bps": "2",
            "quotes": {"updated_at": "2026-01-01T00:00:00Z", "size_1k": {"bid": "1", "ask": "2"}},
        },
        {
            "ticker": "AAPL",
            "name": "Apple",
            "mark_price": "200.0",
            "volume_24h": "500000",
            "open_interest": {"long_open_interest": "50000", "short_open_interest": "50000"},
            "funding_rate": "0.0002",
            "funding_interval_s": 28800,
            "base_spread_bps": "4",
        },
        {
            "ticker": "DOGE",
            "name": "Dogecoin",
            "mark_price": "0.2",
            "volume_24h": "999999999",
            "open_interest": {"long_open_interest": "500000", "short_open_interest": "500000"},
            "funding_rate": "0.0001",
            "funding_interval_s": 3600,
        },
        {
            "ticker": "SPY",
            "name": "SPDR S&P 500 ETF",
            "mark_price": "600",
            "volume_24h": "5",
            "open_interest": {"long_open_interest": "100000", "short_open_interest": "100000"},
            "funding_rate": "0.0001",
            "funding_interval_s": 3600,
        },
    ],
}


class FakeVariationalClient:
    def __init__(self) -> None:
        self.quote_payloads: list[dict] = []

    def get_metadata_stats(self) -> Tuple[object, float]:
        return FAKE_STATS, 12.0

    def get_indicative_quote(self, payload: dict) -> Tuple[object, float]:
        self.quote_payloads.append(payload)
        underlying = payload["instrument"]["underlying"]
        if underlying == "AAPL":
            return {"bid": "199.5", "ask": "200.5", "mark_price": "200.0"}, 8.0
        return {"bid": "99.5", "ask": "100.5", "mark_price": "100.0"}, 8.0


class VariationalAdapterTest(unittest.TestCase):
    def test_filters_to_majors_and_rwa_assets(self) -> None:
        client = FakeVariationalClient()
        adapter = VariationalAdapter(
            config=VariationalAdapterConfig(
                enable_websocket=False,
                min_volume_usd=10,
                min_oi_usd=10,
                indicative_quote_markets=1,
            ),
            client=client,
        )
        adapter._handle_ws_payload({
            "channel": "instrument_price:P-BTC-USDC-3600",
            "pricing": {"price": "100", "underlying_price": "100", "interest_rate": "0.0001"},
        })
        adapter._handle_ws_payload({
            "channel": "instrument_price:P-AAPL-USDC-28800",
            "pricing": {"price": "200", "underlying_price": "190", "interest_rate": "0.0002"},
        })
        adapter._handle_ws_payload({
            "channel": "instrument_price:P-DOGE-USDC-3600",
            "pricing": {"price": "0.2", "underlying_price": "0.2", "interest_rate": "0.0001"},
        })
        adapter._handle_ws_payload({
            "channel": "instrument_price:P-SPY-USDC-3600",
            "pricing": {"price": "600", "underlying_price": "600", "interest_rate": "0.0001"},
        })

        snapshots = adapter.poll()
        by_asset = {snapshot.asset: snapshot for snapshot in snapshots}

        self.assertEqual(set(by_asset), {"AAPL", "BTC"})
        self.assertEqual(by_asset["AAPL"].quote, "USDC")
        self.assertEqual(by_asset["AAPL"].metadata["funding_interval_hours"], 8.0)
        self.assertTrue(by_asset["AAPL"].metadata["stock_like"])
        self.assertFalse(by_asset["BTC"].metadata["stock_like"])
        self.assertEqual([p["instrument"]["underlying"] for p in client.quote_payloads], ["AAPL"])

    def test_stats_quotes_do_not_drive_realtime_bid_ask(self) -> None:
        adapter = VariationalAdapter(
            config=VariationalAdapterConfig(
                enable_websocket=False,
                min_volume_usd=10,
                min_oi_usd=10,
                indicative_quote_markets=0,
            ),
            client=FakeVariationalClient(),
        )
        adapter._handle_ws_payload({
            "channel": "instrument_price:P-BTC-USDC-3600",
            "pricing": {"price": "100", "underlying_price": "100", "interest_rate": "0.0001"},
        })

        btc = next(snapshot for snapshot in adapter.poll() if snapshot.asset == "BTC")

        self.assertNotEqual(btc.best_bid, 1.0)
        self.assertNotEqual(btc.best_ask, 2.0)
        self.assertAlmostEqual(btc.mark_price, 100.0)
        self.assertTrue(btc.metadata["ticker_only"])
        self.assertEqual(btc.metadata["price_source"], "ws_prices")

    def test_rwa_uses_stats_mark_fallback_when_ws_has_no_price(self) -> None:
        adapter = VariationalAdapter(
            config=VariationalAdapterConfig(
                enable_websocket=False,
                min_volume_usd=10,
                min_oi_usd=10,
                indicative_quote_markets=0,
            ),
            client=FakeVariationalClient(),
        )

        snapshots = adapter.poll()
        by_asset = {snapshot.asset: snapshot for snapshot in snapshots}

        self.assertEqual(set(by_asset), {"AAPL"})
        aapl = by_asset["AAPL"]
        self.assertAlmostEqual(aapl.mark_price, 200.0)
        self.assertEqual(aapl.metadata["price_source"], "metadata_stats_mark")
        self.assertTrue(aapl.metadata["stats_mark_fallback"])
        self.assertTrue(aapl.metadata["ticker_only"])
        self.assertFalse(aapl.metadata["indicative_quote"])
        self.assertEqual(aapl.metadata["funding_rate_source"], "metadata_stats_annualized")
        self.assertAlmostEqual(aapl.funding_rate_bps, 0.0002 * (8 / 24 / 365) * 10_000.0)
        self.assertGreaterEqual(aapl.impact_cost_50k_bps, 75.0)

    def test_stats_mark_fallback_can_be_disabled(self) -> None:
        adapter = VariationalAdapter(
            config=VariationalAdapterConfig(
                enable_websocket=False,
                enable_stats_mark_fallback=False,
                min_volume_usd=10,
                min_oi_usd=10,
                indicative_quote_markets=0,
            ),
            client=FakeVariationalClient(),
        )

        self.assertEqual(adapter.poll(), [])

    def test_forwarder_quote_snapshot_overrides_rwa_stats_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_path = os.path.join(tmpdir, "monitor_state.json")
            with open(snapshot_path, "w", encoding="utf-8") as fh:
                json.dump(
                    {
                        "generated_at": "2026-01-01T00:00:03Z",
                        "quotes": {
                            "AAPL": {
                                "bid": "199.75",
                                "ask": "200.25",
                                "mark_price": "200.0",
                                "timestamp": "2026-01-01T00:00:02Z",
                                "raw": {
                                    "instrument": {
                                        "underlying": "AAPL",
                                        "settlement_asset": "USDC",
                                        "funding_interval_s": 28800,
                                    }
                                },
                            }
                        },
                    },
                    fh,
                )

            adapter = VariationalAdapter(
                config=VariationalAdapterConfig(
                    enable_websocket=False,
                    min_volume_usd=10,
                    min_oi_usd=10,
                    indicative_quote_markets=0,
                    forwarder_snapshot_path=snapshot_path,
                    forwarder_quote_ttl_seconds=30,
                ),
                client=FakeVariationalClient(),
            )

            snapshots = adapter.poll()

        by_asset = {snapshot.asset: snapshot for snapshot in snapshots}
        self.assertEqual(set(by_asset), {"AAPL"})
        aapl = by_asset["AAPL"]
        self.assertEqual(aapl.metadata["price_source"], "frontend_indicative_quote")
        self.assertTrue(aapl.metadata["forwarder_quote"])
        self.assertFalse(aapl.metadata["ticker_only"])
        self.assertTrue(aapl.metadata["indicative_quote"])
        self.assertAlmostEqual(aapl.best_bid, 199.75)
        self.assertAlmostEqual(aapl.best_ask, 200.25)

    def test_stale_forwarder_quote_snapshot_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_path = os.path.join(tmpdir, "monitor_state.json")
            with open(snapshot_path, "w", encoding="utf-8") as fh:
                json.dump(
                    {
                        "generated_at": "2026-01-01T00:00:03Z",
                        "quotes": {"AAPL": {"bid": "199.75", "ask": "200.25", "mark_price": "200.0"}},
                    },
                    fh,
                )
            old_time = time.time() - 60
            os.utime(snapshot_path, (old_time, old_time))

            adapter = VariationalAdapter(
                config=VariationalAdapterConfig(
                    enable_websocket=False,
                    min_volume_usd=10,
                    min_oi_usd=10,
                    indicative_quote_markets=0,
                    forwarder_snapshot_path=snapshot_path,
                    forwarder_quote_ttl_seconds=1,
                ),
                client=FakeVariationalClient(),
            )

            snapshots = adapter.poll()

        aapl = next(snapshot for snapshot in snapshots if snapshot.asset == "AAPL")
        self.assertEqual(aapl.metadata["price_source"], "metadata_stats_mark")
        self.assertTrue(aapl.metadata["stats_mark_fallback"])

    def test_parses_ws_channel_key(self) -> None:
        adapter = VariationalAdapter(
            config=VariationalAdapterConfig(enable_websocket=False),
            client=FakeVariationalClient(),
        )

        adapter._handle_ws_payload({
            "channel": "instrument_price:P-ETH-USDC-28800",
            "pricing": {"price": "3000", "underlying_price": "2990", "interest_rate": "-0.00001"},
        })

        self.assertIn(("ETH", "USDC", 28800), adapter._price_cache)


if __name__ == "__main__":
    unittest.main()
