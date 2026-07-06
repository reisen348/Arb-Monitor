from __future__ import annotations

import io
import json
import unittest
from dataclasses import replace

from perp_arb.cli import main
from perp_arb.market_data import MarketSnapshot, MockMarketDataAdapter, OpportunityBuilder
from perp_arb.models import ExecutionLabel
from perp_arb.scanner import RealtimeScanner


class StaticAdapter:
    def __init__(self, name: str, snapshots):
        self.name = name
        self._snapshots = snapshots

    def poll(self):
        return list(self._snapshots)


def make_snapshot(venue: str, asset: str, bid: float, ask: float, funding: float) -> MarketSnapshot:
    mid = (bid + ask) / 2.0
    return MarketSnapshot(
        venue=venue,
        market_type="perp_dex",
        asset=asset,
        quote="USD",
        best_bid=bid,
        best_ask=ask,
        mark_price=mid,
        oracle_price=mid,
        index_price=mid,
        taker_fee_bps=2.0,
        depth_10k_usd=100_000.0,
        depth_50k_usd=250_000.0,
        top_1pct_depth_usd=300_000.0,
        volume_depth_ratio=5.0,
        oi_usd=100_000_000.0,
        oi_change_pct=1.5,
        funding_rate_bps=funding,
        funding_change_bps=0.6,
        impact_cost_10k_bps=1.0,
        impact_cost_50k_bps=4.0,
        slippage_bps=1.0,
        realized_vol=20.0,
        jump_frequency=2.0,
        spread_zscore=1.0,
        trend_vs_mean_reversion=-0.2,
        latency_ms=100.0,
        staleness_ms=100.0,
    )


class OpportunityBuilderTest(unittest.TestCase):
    def test_builder_creates_pairwise_opportunities(self) -> None:
        builder = OpportunityBuilder()
        snapshots = [
            make_snapshot("hyperliquid", "ETH", 101.5, 101.6, 2.0),
            make_snapshot("drift", "ETH", 102.0, 102.1, -1.0),
            make_snapshot("aevo", "ETH", 101.8, 101.9, 1.0),
        ]
        opportunities = builder.build(snapshots)
        self.assertEqual(len(opportunities), 3)
        self.assertTrue(all(opportunity.asset == "ETH" for opportunity in opportunities))

    def test_builder_keeps_ticker_only_snapshots_for_coverage(self) -> None:
        builder = OpportunityBuilder()
        snapshots = [
            replace(make_snapshot("bitget", "NVDA", 200.0, 200.2, 0.0), metadata={"ticker_only": True}),
            replace(make_snapshot("aster", "NVDA", 200.4, 200.6, 0.0), metadata={"ticker_only": True}),
        ]
        opportunities = builder.build(snapshots)
        self.assertEqual(len(opportunities), 1)
        self.assertEqual(opportunities[0].asset, "NVDA")
        self.assertTrue(opportunities[0].metadata["leg_a_ticker_only"])
        self.assertTrue(opportunities[0].metadata["leg_b_ticker_only"])

    def test_builder_dedupes_same_asset_same_venue_before_pairing(self) -> None:
        builder = OpportunityBuilder()
        snapshots = [
            make_snapshot("kraken", "BTC", 100.0, 100.2, 0.0),
            replace(make_snapshot("kraken", "BTC", 100.1, 100.3, 0.0), oi_usd=200_000_000.0),
            make_snapshot("aster", "BTC", 101.0, 101.2, 0.0),
        ]
        opportunities = builder.build(snapshots)
        self.assertEqual(len(opportunities), 1)
        self.assertEqual({opportunities[0].leg_a.venue, opportunities[0].leg_b.venue}, {"kraken", "aster"})

    def test_builder_separates_stock_and_crypto_symbol_collisions(self) -> None:
        builder = OpportunityBuilder()
        snapshots = [
            replace(make_snapshot("bitget", "CAT", 1000.0, 1001.0, 0.0), metadata={"stock_like": True}),
            make_snapshot("kraken", "CAT", 0.001, 0.0011, 0.0),
            make_snapshot("gate", "CAT", 0.001, 0.0011, 0.0),
            replace(make_snapshot("aster", "CAT", 1002.0, 1003.0, 0.0), metadata={"stock_like": True}),
        ]
        opportunities = builder.build(snapshots)
        venue_pairs = {frozenset((opp.leg_a.venue, opp.leg_b.venue)) for opp in opportunities}
        self.assertEqual(venue_pairs, {frozenset(("bitget", "aster")), frozenset(("gate", "kraken"))})

    def test_builder_uses_price_heuristic_for_stock_symbols_without_metadata(self) -> None:
        builder = OpportunityBuilder()
        snapshots = [
            replace(make_snapshot("gate", "MSFT", 350.0, 350.5, 0.0), metadata={}),
            replace(make_snapshot("nado", "MSFT", 351.0, 351.5, 0.0), metadata={}),
            replace(make_snapshot("kraken", "MSFT", 349.0, 349.5, 0.0), metadata={"stock_like": True}),
        ]
        opportunities = builder.build(snapshots)
        venue_pairs = {frozenset((opp.leg_a.venue, opp.leg_b.venue)) for opp in opportunities}
        self.assertEqual(venue_pairs, {frozenset(("gate", "nado")), frozenset(("gate", "kraken")), frozenset(("nado", "kraken"))})

    def test_builder_treats_dram_as_rwa_without_exchange_metadata(self) -> None:
        builder = OpportunityBuilder()
        snapshots = [
            replace(make_snapshot("grvt", "DRAM", 65.5, 65.6, 0.0), metadata={}),
            replace(make_snapshot("lighter", "DRAM", 65.4, 65.5, 0.0), metadata={}),
            replace(make_snapshot("aster", "DRAM", 65.6, 65.7, 0.0), metadata={"stock_like": True}),
        ]
        opportunities = builder.build(snapshots)

        venue_pairs = {frozenset((opp.leg_a.venue, opp.leg_b.venue)) for opp in opportunities}
        self.assertEqual(
            venue_pairs,
            {
                frozenset(("aster", "grvt")),
                frozenset(("aster", "lighter")),
                frozenset(("grvt", "lighter")),
            },
        )
        self.assertTrue(all(opp.metadata["market_family"] == "stock" for opp in opportunities))

    def test_builder_pairs_stock_symbols_across_stock_like_venues(self) -> None:
        builder = OpportunityBuilder()
        snapshots = [
            replace(make_snapshot("bitget", "NVDA", 200.0, 200.2, 0.0), metadata={"stock_like": True}),
            replace(make_snapshot("okx", "NVDA", 200.4, 200.6, 0.0), metadata={}),
            replace(make_snapshot("kraken", "NVDA", 200.5, 200.7, 0.0), metadata={"stock_like": True}),
        ]
        opportunities = builder.build(snapshots)
        venue_pairs = {frozenset((opp.leg_a.venue, opp.leg_b.venue)) for opp in opportunities}
        self.assertEqual(venue_pairs, {frozenset(("bitget", "okx")), frozenset(("bitget", "kraken")), frozenset(("okx", "kraken"))})

    def test_builder_respects_explicit_non_stock_for_same_symbol(self) -> None:
        builder = OpportunityBuilder()
        snapshots = [
            replace(make_snapshot("aster", "BB", 0.018, 0.019, 0.0), metadata={"stock_like": False}),
            replace(make_snapshot("binance", "BB", 0.018, 0.019, 0.0), metadata={}),
            replace(make_snapshot("bitget", "BB", 0.018, 0.019, 0.0), metadata={}),
            replace(make_snapshot("hyperliquid:xyz", "BB", 10.4, 10.5, 0.0), metadata={"stock_like": True}),
            replace(make_snapshot("bitget", "BB", 10.5, 10.6, 0.0), metadata={"stock_like": True}),
        ]
        opportunities = builder.build(snapshots)

        venue_pairs = {frozenset((opp.leg_a.venue, opp.leg_b.venue)) for opp in opportunities}
        self.assertEqual(
            venue_pairs,
            {
                frozenset(("aster", "binance")),
                frozenset(("aster", "bitget")),
                frozenset(("binance", "bitget")),
                frozenset(("hyperliquid:xyz", "bitget")),
            },
        )

    def test_builder_does_not_promote_unmarked_bitget_crypto_from_dynamic_stock_asset(self) -> None:
        builder = OpportunityBuilder()
        snapshots = [
            replace(make_snapshot("bitget", "QNT", 69.4, 69.5, 0.0), metadata={"stock_like": True}),
            replace(make_snapshot("hyperliquid:xyz", "QNT", 69.3, 69.4, 0.0), metadata={"stock_like": True}),
            replace(make_snapshot("okx", "QNT", 69.2, 69.3, 0.0), metadata={}),
            replace(make_snapshot("bitget", "QNT", 63.6, 63.7, 0.0), metadata={}),
            replace(make_snapshot("gate", "QNT", 63.5, 63.6, 0.0), metadata={}),
            replace(make_snapshot("kraken", "QNT", 63.4, 63.5, 0.0), metadata={}),
        ]

        opportunities = builder.build(snapshots)

        venue_pairs = {frozenset((opp.leg_a.venue, opp.leg_b.venue)) for opp in opportunities}
        self.assertEqual(
            venue_pairs,
            {
                frozenset(("bitget", "hyperliquid:xyz")),
                frozenset(("bitget", "okx")),
                frozenset(("hyperliquid:xyz", "okx")),
                frozenset(("bitget", "gate")),
                frozenset(("bitget", "kraken")),
                frozenset(("gate", "kraken")),
            },
        )
        for opportunity in opportunities:
            if "hyperliquid:xyz" in {opportunity.leg_a.venue, opportunity.leg_b.venue}:
                self.assertGreater(min(opportunity.leg_a.mark_price, opportunity.leg_b.mark_price), 69.0)

    def test_builder_skips_sparse_price_sources_with_large_price_mismatch(self) -> None:
        builder = OpportunityBuilder()
        snapshots = [
            replace(make_snapshot("bitget", "USELESS", 0.0705, 0.0706, 0.0), metadata={"volume_24h_usd": 1_400_000.0}),
            replace(make_snapshot("bybit", "USELESS", 0.0706, 0.0707, 0.0), metadata={"volume_24h_usd": 3_500_000.0}),
            replace(make_snapshot("nado", "USELESS", 0.1050, 0.1051, 0.0), metadata={"source": "nado", "volume_24h_usd": 0.0}),
        ]

        opportunities = builder.build(snapshots)

        self.assertEqual(len(opportunities), 1)
        self.assertEqual({opportunities[0].leg_a.venue, opportunities[0].leg_b.venue}, {"bitget", "bybit"})

    def test_builder_skips_extreme_price_mismatches_even_with_volume(self) -> None:
        builder = OpportunityBuilder()
        snapshots = [
            replace(make_snapshot("bybit", "BP", 0.4300, 0.4310, 0.0), metadata={"turnover_24h_usd": 440_000.0}),
            replace(make_snapshot("gate", "BP", 0.5250, 0.5260, 0.0), metadata={"day_quote_volume_usd": 290_000.0}),
            replace(make_snapshot("okx", "BP", 0.4320, 0.4330, 0.0), metadata={"day_volume_usd": 2_000_000.0}),
        ]

        opportunities = builder.build(snapshots)

        venue_pairs = {frozenset((opp.leg_a.venue, opp.leg_b.venue)) for opp in opportunities}
        self.assertEqual(venue_pairs, {frozenset(("bybit", "okx"))})

    def test_builder_skips_low_oi_large_price_mismatches(self) -> None:
        builder = OpportunityBuilder()
        snapshots = [
            replace(
                make_snapshot("gate", "MINIMAX", 52.4, 52.5, 0.0),
                oi_usd=166_000.0,
                metadata={"day_quote_volume_usd": 50_000.0},
            ),
            replace(
                make_snapshot("lighter", "MINIMAX", 55.3, 55.4, 0.0),
                oi_usd=11_500.0,
                metadata={"volume_24h_usd": 1_400_000.0},
            ),
            replace(
                make_snapshot("okx", "MINIMAX", 52.6, 52.7, 0.0),
                oi_usd=120_000.0,
                metadata={"day_volume_usd": 500_000.0},
            ),
        ]

        opportunities = builder.build(snapshots)

        self.assertEqual(len(opportunities), 1)
        self.assertEqual({opportunities[0].leg_a.venue, opportunities[0].leg_b.venue}, {"gate", "okx"})

    def test_builder_skips_low_volume_two_percent_price_mismatches(self) -> None:
        builder = OpportunityBuilder()
        snapshots = [
            replace(
                make_snapshot("bitget", "LITE", 822.4, 822.6, 0.0),
                oi_usd=6_700_000.0,
                metadata={"day_volume_usd": 78_400_000.0},
            ),
            replace(
                make_snapshot("gate", "LITE", 821.8, 822.0, 0.0),
                oi_usd=750_000.0,
                metadata={"day_quote_volume_usd": 1_800_000.0},
            ),
            replace(
                make_snapshot("lighter", "LITE", 844.3, 844.5, 0.0),
                oi_usd=213_000.0,
                metadata={"volume_24h_usd": 61_000.0},
            ),
        ]

        opportunities = builder.build(snapshots)

        self.assertEqual(len(opportunities), 1)
        self.assertEqual({opportunities[0].leg_a.venue, opportunities[0].leg_b.venue}, {"bitget", "gate"})

    def test_builder_skips_ticker_only_two_percent_price_mismatches(self) -> None:
        builder = OpportunityBuilder()
        snapshots = [
            replace(
                make_snapshot("bybit", "WIF", 0.14262, 0.14264, 0.0),
                oi_usd=8_500_000.0,
                metadata={"turnover_24h_usd": 22_000_000.0},
            ),
            replace(
                make_snapshot("gate", "WIF", 0.14249, 0.14251, 0.0),
                oi_usd=1_600_000.0,
                metadata={"day_quote_volume_usd": 2_100_000.0},
            ),
            replace(
                make_snapshot("lighter", "WIF", 0.13740, 0.13742, 0.0),
                oi_usd=95_000.0,
                metadata={"ticker_only": True, "volume_24h_usd": 130_000.0},
            ),
        ]

        opportunities = builder.build(snapshots)

        self.assertEqual(len(opportunities), 1)
        self.assertEqual({opportunities[0].leg_a.venue, opportunities[0].leg_b.venue}, {"bybit", "gate"})

    def test_builder_skips_lighter_ticker_only_one_percent_price_mismatches(self) -> None:
        builder = OpportunityBuilder()
        snapshots = [
            replace(
                make_snapshot("hyperliquid:xyz", "RKLB", 80.88, 80.89, 0.0),
                oi_usd=6_300_000.0,
                metadata={"stock_like": True, "volume_24h_usd": 6_000_000.0},
            ),
            replace(
                make_snapshot("gate", "RKLB", 80.91, 80.93, 0.0),
                oi_usd=337_000.0,
                metadata={"stock_like": True, "day_quote_volume_usd": 334_000.0},
            ),
            replace(
                make_snapshot("lighter", "RKLB", 79.80, 79.82, 0.0),
                oi_usd=153_000.0,
                metadata={"source": "lighter", "ticker_only": True, "volume_24h_usd": 229_000.0, "stock_like": True},
            ),
        ]

        opportunities = builder.build(snapshots)

        self.assertEqual(len(opportunities), 1)
        self.assertEqual({opportunities[0].leg_a.venue, opportunities[0].leg_b.venue}, {"hyperliquid:xyz", "gate"})

    def test_builder_separates_rtx_stock_from_crypto_price(self) -> None:
        builder = OpportunityBuilder()
        snapshots = [
            replace(make_snapshot("bitget", "RTX", 189.0, 189.2, 0.0), metadata={"stock_like": True}),
            replace(make_snapshot("gate", "RTX", 187.8, 188.0, 0.0), metadata={}),
            replace(make_snapshot("aster", "RTX", 1.13, 1.14, 0.0), metadata={"stock_like": False}),
        ]

        opportunities = builder.build(snapshots)

        self.assertEqual(len(opportunities), 1)
        self.assertEqual({opportunities[0].leg_a.venue, opportunities[0].leg_b.venue}, {"bitget", "gate"})


class RealtimeScannerTest(unittest.TestCase):
    def test_scan_once_returns_ranked_results(self) -> None:
        adapters = [
            StaticAdapter("a", [make_snapshot("hyperliquid", "ETH", 101.5, 101.6, 2.0)]),
            StaticAdapter("b", [make_snapshot("drift", "ETH", 102.0, 102.1, -1.0)]),
        ]
        scanner = RealtimeScanner(adapters)
        batch = scanner.scan_once()
        self.assertEqual(len(batch.snapshots), 2)
        self.assertEqual(len(batch.opportunities), 1)
        self.assertGreaterEqual(len(batch.scored_opportunities), 1)
        self.assertIn(batch.scored_opportunities[0].label, {ExecutionLabel.TRADABLE, ExecutionLabel.WATCH})

    def test_scan_once_keeps_multiple_exchange_pairs_for_same_asset(self) -> None:
        adapters = [
            StaticAdapter("a", [make_snapshot("bitget", "AAPL", 100.0, 100.1, 0.0)]),
            StaticAdapter("b", [make_snapshot("kraken", "AAPL", 100.2, 100.3, 0.0)]),
            StaticAdapter("c", [make_snapshot("hyperliquid:xyz", "AAPL", 104.0, 104.1, 0.0)]),
        ]
        scanner = RealtimeScanner(adapters)

        batch = scanner.scan_once()

        venue_pairs = {
            frozenset((item.opportunity.leg_a.venue, item.opportunity.leg_b.venue))
            for item in batch.scored_opportunities
        }
        self.assertEqual(
            venue_pairs,
            {
                frozenset(("bitget", "kraken")),
                frozenset(("bitget", "hyperliquid:xyz")),
                frozenset(("kraken", "hyperliquid:xyz")),
            },
        )


class FailingAdapter:
    """Adapter that always raises, for testing failure isolation."""
    def __init__(self, name: str):
        self.name = name

    def poll(self):
        raise ConnectionError("simulated network failure")


class ParallelPollingTest(unittest.TestCase):
    def test_parallel_polling_collects_from_multiple_adapters(self) -> None:
        adapters = [
            StaticAdapter("a", [make_snapshot("hyperliquid", "ETH", 101.5, 101.6, 2.0)]),
            StaticAdapter("b", [make_snapshot("drift", "ETH", 102.0, 102.1, -1.0)]),
        ]
        scanner = RealtimeScanner(adapters)
        batch = scanner.scan_once()
        self.assertEqual(len(batch.snapshots), 2)
        self.assertEqual(len(batch.source_statuses), 2)
        self.assertTrue(all(status.ok for status in batch.source_statuses))
        self.assertTrue(all(status.snapshot_count == 1 for status in batch.source_statuses))
        self.assertTrue(all(status.poll_duration_ms >= 0 for status in batch.source_statuses))

    def test_failing_adapter_does_not_block_scan(self) -> None:
        adapters = [
            StaticAdapter("good", [make_snapshot("hyperliquid", "ETH", 101.5, 101.6, 2.0)]),
            FailingAdapter("broken"),
        ]
        scanner = RealtimeScanner(adapters)
        batch = scanner.scan_once()
        # Should still get snapshots from the working adapter
        self.assertEqual(len(batch.snapshots), 1)
        self.assertEqual(len(batch.source_statuses), 2)
        ok_statuses = [s for s in batch.source_statuses if s.ok]
        fail_statuses = [s for s in batch.source_statuses if not s.ok]
        self.assertEqual(len(ok_statuses), 1)
        self.assertEqual(len(fail_statuses), 1)
        self.assertIn("ConnectionError", fail_statuses[0].error)

    def test_all_adapters_fail_produces_empty_batch(self) -> None:
        adapters = [FailingAdapter("broken1"), FailingAdapter("broken2")]
        scanner = RealtimeScanner(adapters)
        batch = scanner.scan_once()
        self.assertEqual(len(batch.snapshots), 0)
        self.assertEqual(len(batch.opportunities), 0)
        self.assertEqual(len(batch.scored_opportunities), 0)
        self.assertTrue(all(not s.ok for s in batch.source_statuses))

    def test_source_statuses_in_json_payload(self) -> None:
        buffer = io.StringIO()
        exit_code = main(
            ["scan", "--iterations", "1", "--top", "2", "--format", "json",
             "--venues", "hyperliquid", "drift"],
            out=buffer,
        )
        self.assertEqual(exit_code, 0)
        payload = json.loads(buffer.getvalue().strip())
        self.assertIn("source_statuses", payload)
        self.assertGreater(len(payload["source_statuses"]), 0)
        for status in payload["source_statuses"]:
            self.assertIn("adapter_name", status)
            self.assertIn("ok", status)
            self.assertIn("poll_duration_ms", status)


class CliSmokeTest(unittest.TestCase):
    def test_cli_scan_json_output(self) -> None:
        buffer = io.StringIO()
        exit_code = main(
            [
                "scan",
                "--iterations",
                "1",
                "--top",
                "2",
                "--format",
                "json",
                "--venues",
                "hyperliquid",
                "drift",
            ],
            out=buffer,
        )
        self.assertEqual(exit_code, 0)
        payload = json.loads(buffer.getvalue().strip())
        self.assertIn("timestamp", payload)
        self.assertIn("dashboard_summary", payload)
        self.assertIn("sort_policy", payload)
        self.assertLessEqual(len(payload["opportunities"]), 2)
        if payload["opportunities"]:
            opportunity = payload["opportunities"][0]
            self.assertIn("sort_basis", opportunity)
            self.assertIn("alerts", opportunity)
            self.assertIn("tags", opportunity)
            self.assertIn("raw_tags", opportunity)
            self.assertIn("advisories", opportunity)
            self.assertIn("raw_advisories", opportunity)
            self.assertIn("dashboard_card", opportunity)
            self.assertIn("policy", opportunity)
            self.assertIn("execution_plan", opportunity)
            self.assertIn("legs", opportunity["execution_plan"])
            self.assertIn("label_display", opportunity)
            self.assertIn("bucket_display", opportunity)

    def test_json_output_contains_jump_sorting_basis_and_alerts(self) -> None:
        buffer = io.StringIO()
        exit_code = main(
            [
                "scan",
                "--iterations",
                "3",
                "--interval",
                "0",
                "--top",
                "2",
                "--format",
                "json",
                "--source",
                "mock",
                "--venues",
                "hyperliquid",
                "grvt",
                "--assets",
                "BTC",
            ],
            out=buffer,
        )
        self.assertEqual(exit_code, 0)
        lines = [json.loads(line) for line in buffer.getvalue().strip().splitlines() if line.strip()]
        self.assertGreaterEqual(len(lines), 1)
        for payload in lines:
            for opportunity in payload["opportunities"]:
                self.assertIn("micro_jump_frequency", opportunity["sort_basis"])
                self.assertIn("shock_jump_frequency", opportunity["sort_basis"])
                self.assertIn("micro_jump_penalty", opportunity["sort_basis"])
                self.assertIn("shock_jump_penalty", opportunity["sort_basis"])
                self.assertIsInstance(opportunity["alerts"], list)
                self.assertIn("dashboard_card", opportunity)
                self.assertIn("execution_plan", opportunity)
                self.assertIn("legs", opportunity["execution_plan"])
                if opportunity["alerts"]:
                    self.assertIn(opportunity["alerts"][0]["severity"], {"高", "中", "低"})

    def test_json_output_uses_readable_chinese_labels(self) -> None:
        buffer = io.StringIO()
        exit_code = main(
            [
                "scan",
                "--iterations",
                "1",
                "--top",
                "2",
                "--format",
                "json",
                "--source",
                "mock",
                "--venues",
                "hyperliquid",
                "grvt",
                "--assets",
                "BTC",
                "--min-label",
                "blocked",
            ],
            out=buffer,
        )
        self.assertEqual(exit_code, 0)
        payload = json.loads(buffer.getvalue().strip())
        self.assertIn("market_regime_display", payload["dashboard_summary"])
        if payload["opportunities"]:
            opportunity = payload["opportunities"][0]
            self.assertIn(opportunity["label_display"], {"可交易", "观察", "拦截"})
            self.assertIsInstance(opportunity["dashboard_card"]["risks"], list)
            if opportunity["dashboard_card"]["risks"]:
                self.assertNotIn("_", opportunity["dashboard_card"]["risks"][0])

    def test_dashboard_format_outputs_watchboard_style_summary(self) -> None:
        buffer = io.StringIO()
        exit_code = main(
            [
                "scan",
                "--iterations",
                "1",
                "--top",
                "2",
                "--format",
                "dashboard",
                "--source",
                "mock",
                "--venues",
                "hyperliquid",
                "grvt",
                "--assets",
                "BTC",
                "--min-label",
                "blocked",
            ],
            out=buffer,
        )
        self.assertEqual(exit_code, 0)
        output = buffer.getvalue()
        self.assertIn("市场状态=", output)
        self.assertIn("标签分布=", output)
        self.assertIn("观点=", output)

    def test_mock_adapter_produces_live_snapshots(self) -> None:
        adapter = MockMarketDataAdapter(name="mock-hl", venue="hyperliquid", assets=["BTC"], seed=1)
        first = adapter.poll()[0]
        second = adapter.poll()[0]
        self.assertNotEqual(first.best_bid, second.best_bid)


if __name__ == "__main__":
    unittest.main()
