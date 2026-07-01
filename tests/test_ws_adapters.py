from __future__ import annotations

import unittest
from datetime import datetime, timezone

from perp_arb.grvt import GrvtAdapterConfig
from perp_arb.hyperliquid import HyperliquidAdapterConfig
from perp_arb.market_data import MarketSnapshot
from perp_arb.ws_adapters import GrvtWebsocketAdapter, HyperliquidWebsocketAdapter


class FakeHyperliquidWsAdapter(HyperliquidWebsocketAdapter):
    def _seed_snapshots(self):
        return [
            MarketSnapshot(
                venue="hyperliquid",
                market_type="perp_dex",
                asset="BTC",
                quote="USD",
                best_bid=68000.0,
                best_ask=68010.0,
                mark_price=68005.0,
                oracle_price=68000.0,
                index_price=68000.0,
                taker_fee_bps=2.5,
                depth_10k_usd=10_000.0,
                depth_50k_usd=50_000.0,
                top_1pct_depth_usd=90_000.0,
                volume_depth_ratio=10.0,
                oi_usd=1_000_000.0,
                funding_rate_bps=1.0,
                impact_cost_10k_bps=1.0,
                impact_cost_50k_bps=4.0,
                slippage_bps=0.5,
                timestamp=datetime.now(timezone.utc),
                metadata={"source": "hyperliquid"},
            )
        ]

    def start(self) -> None:
        with self._lock:
            if not self._snapshots:
                self._seed_locked()


class FakeGrvtWsAdapter(GrvtWebsocketAdapter):
    def _seed_snapshots(self):
        return [
            MarketSnapshot(
                venue="grvt",
                market_type="perp_dex",
                asset="BTC",
                quote="USDT",
                best_bid=68100.0,
                best_ask=68110.0,
                mark_price=68105.0,
                oracle_price=68100.0,
                index_price=68100.0,
                taker_fee_bps=2.5,
                depth_10k_usd=10_000.0,
                depth_50k_usd=50_000.0,
                top_1pct_depth_usd=100_000.0,
                volume_depth_ratio=12.0,
                oi_usd=1_250_000.0,
                funding_rate_bps=0.2,
                impact_cost_10k_bps=1.0,
                impact_cost_50k_bps=4.0,
                slippage_bps=0.5,
                timestamp=datetime.now(timezone.utc),
                metadata={"source": "grvt", "instrument": "BTC_USDT_Perp"},
            )
        ]

    def start(self) -> None:
        with self._lock:
            if not self._snapshots:
                self._seed_locked()


class WebsocketAdapterTest(unittest.TestCase):
    def test_hyperliquid_book_and_ctx_messages_update_snapshot(self) -> None:
        adapter = FakeHyperliquidWsAdapter(HyperliquidAdapterConfig(assets=["BTC"]))
        adapter.start()
        adapter._handle_payload(
            {
                "channel": "activeAssetCtx",
                "data": {
                    "coin": "BTC",
                    "ctx": {
                        "markPx": "68120.0",
                        "oraclePx": "68100.0",
                        "funding": "0.00015",
                        "openInterest": "20",
                        "dayNtlVlm": "1500000",
                        "prevDayPx": "67000.0",
                    },
                },
            }
        )
        adapter._handle_payload(
            {
                "channel": "l2Book",
                "data": {
                    "coin": "BTC",
                    "time": 1_700_000_000_000,
                    "levels": [
                        [{"px": "68110.0", "sz": "1.0", "n": 2}],
                        [{"px": "68120.0", "sz": "1.2", "n": 2}],
                    ],
                },
            }
        )
        snapshot = adapter.poll()[0]
        self.assertAlmostEqual(snapshot.best_bid, 68110.0)
        self.assertAlmostEqual(snapshot.mark_price, 68120.0)
        self.assertAlmostEqual(snapshot.funding_rate_bps, 1.5)
        self.assertGreater(snapshot.oi_usd, 0.0)

    def test_grvt_ticker_and_book_messages_update_snapshot(self) -> None:
        adapter = FakeGrvtWsAdapter(GrvtAdapterConfig(assets=["BTC"], quotes=["USDT"]))
        adapter.start()
        adapter._handle_payload(
            {
                "s": "v1.ticker.s",
                "s1": "BTC_USDT_Perp@500",
                "f": {
                    "i": "BTC_USDT_Perp",
                    "et": "1700000000000000000",
                    "mp": "68200.0",
                    "ip": "68190.0",
                    "bb": "68195.0",
                    "ba": "68205.0",
                    "bv1": "2000000",
                    "sv1": "1800000",
                    "oi": "30",
                    "fr2": "0.0015",
                    "op": "67000.0",
                    "nf": "1700003600000000000",
                },
            }
        )
        adapter._handle_payload(
            {
                "s": "v1.book.d",
                "s1": "BTC_USDT_Perp@100",
                "f": {
                    "i": "BTC_USDT_Perp",
                    "et": "1700000000000000000",
                    "b": [{"p": "68195.0", "s": "1.5", "no": 3}],
                    "a": [{"p": "68205.0", "s": "1.6", "no": 3}],
                },
            }
        )
        snapshot = adapter.poll()[0]
        self.assertAlmostEqual(snapshot.best_bid, 68195.0)
        self.assertAlmostEqual(snapshot.best_ask, 68205.0)
        self.assertAlmostEqual(snapshot.funding_rate_bps, 0.15)
        self.assertGreater(snapshot.top_1pct_depth_usd, 0.0)

    def test_websocket_micro_moves_drive_jump_frequency(self) -> None:
        adapter = FakeGrvtWsAdapter(GrvtAdapterConfig(assets=["BTC"], quotes=["USDT"]))
        adapter.start()
        for bid, ask in [(68120.0, 68130.0), (68160.0, 68170.0), (68110.0, 68120.0)]:
            adapter._handle_payload(
                {
                    "s": "v1.book.d",
                    "s1": "BTC_USDT_Perp@100",
                    "f": {
                        "i": "BTC_USDT_Perp",
                        "et": "1700000000000000000",
                        "b": [{"p": str(bid), "s": "1.5", "no": 3}],
                        "a": [{"p": str(ask), "s": "1.6", "no": 3}],
                    },
                }
            )
        snapshot = adapter.poll()[0]
        self.assertGreater(snapshot.jump_frequency, 0.0)
        self.assertGreater(snapshot.micro_jump_frequency, 0.0)
        self.assertGreaterEqual(snapshot.shock_jump_frequency, 0.0)


if __name__ == "__main__":
    unittest.main()
