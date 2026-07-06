from __future__ import annotations

import unittest
from types import SimpleNamespace

from perp_arb.cli import build_adapters


class CliSourceTest(unittest.TestCase):
    def test_binance_bybit_source_builds_only_official_adapters(self) -> None:
        args = SimpleNamespace(
            source="binance_bybit",
            transport="rest",
            assets=["BTC"],
            timeout=1.0,
            top_book_markets=1,
        )

        adapters = build_adapters(args)

        self.assertEqual([adapter.name for adapter in adapters], ["binance", "bybit"])

    def test_binance_bybit_ws_source_builds_only_official_adapters(self) -> None:
        args = SimpleNamespace(
            source="binance_bybit",
            transport="ws",
            assets=["BTC"],
            timeout=1.0,
            top_book_markets=1,
        )

        adapters = build_adapters(args)

        self.assertEqual([adapter.name for adapter in adapters], ["binance-ws", "bybit-ws"])


if __name__ == "__main__":
    unittest.main()
