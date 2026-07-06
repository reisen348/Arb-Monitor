from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from perp_arb.dashboard import (
    _DashboardApp,
    _normalize_blacklist_asset,
    _opportunity_zscore_window_points,
    _spread_vs_mean_pct,
    build_dashboard_html,
)
from perp_arb.market_data import SourceStatus
from perp_arb.models import OpportunityBucket, PerpArbOpportunity, PerpLegSnapshot
from perp_arb.persistence import StateStore
from perp_arb.state import OpportunityStatePoint


class _ScannerStub:
    adapters = []
    config = type("Config", (), {"top_n": 10})()

    class _MarketState:
        @staticmethod
        def enrich_snapshots(snapshots):
            return snapshots

    class _Builder:
        @staticmethod
        def build(snapshots):
            return []

    class _OpportunityState:
        @staticmethod
        def enrich_opportunities(opportunities):
            return opportunities

    class _Scorer:
        @staticmethod
        def rank(opportunities):
            return []

    market_state = _MarketState()
    builder = _Builder()
    opportunity_state = _OpportunityState()
    scorer = _Scorer()

    @staticmethod
    def _filter_scored(scored):
        return scored


class DashboardHtmlTest(unittest.TestCase):
    def test_dashboard_html_contains_core_sections(self) -> None:
        html = build_dashboard_html(refresh_seconds=2.5)
        self.assertIn("Funding Terminal", html)
        self.assertIn("warm charcoal + cream + coral", html)
        self.assertIn("--accent:#d97757", html)
        self.assertIn('<div class="topbar">', html)
        self.assertIn('<div class="brand">PERP<span>/</span>ARB<em>Funding Terminal</em></div>', html)
        self.assertIn(".table-wrap{flex:1;min-height:0;overflow:auto;background:var(--panel);border:1px solid var(--line);border-radius:10px}", html)
        self.assertNotIn("--bg:#000;--text:#d8d8df", html)
        self.assertIn("/api/dashboard", html)
        self.assertIn("tbl-body", html)
        self.assertIn("2500", html)  # REFRESH_MS
        self.assertNotIn("开始交易", html)
        self.assertIn("最大价差/百分比", html)
        self.assertIn("对手所", html)
        self.assertLess(html.index('<th class="counterparty">对手所</th>'), html.index('<th class="symbol">币种</th>'))
        self.assertLess(html.index('<td class="counterparty"'), html.index('<td class="symbol">'))
        self.assertIn("价差-4H均值", html)
        self.assertIn("toggleSpreadMeanSort()", html)
        self.assertIn('activeSort==="spreadMean"', html)
        self.assertIn("sortSpreadMeanDir", html)
        self.assertIn("资费方向", html)
        self.assertIn("fundingAligned", html)
        self.assertIn("fundingDirectionTitle", html)
        self.assertIn("const fundingCarry=longIsA?(-dailyA+dailyB):(-dailyB+dailyA);", html)
        self.assertIn("4H价差Z", html)
        self.assertIn("手续费", html)
        self.assertIn("fmtFeeBps", html)
        self.assertIn("feeSum", html)
        self.assertIn("maker_fee_a_bps", html)
        self.assertIn("taker_fee_b_bps", html)
        self.assertIn("venue_a_ticker_only", html)
        self.assertIn("ticker-only", html)
        self.assertLess(html.index("价差-4H均值 ↕"), html.index("4H价差Z"))
        self.assertLess(html.index("4H价差Z"), html.index("手续费"))
        self.assertLess(html.index("手续费"), html.index("资费方向"))
        self.assertLess(html.index('<td class="stdspread num">'), html.index('<td class="limit num">'))
        self.assertLess(html.index('<td class="limit num">'), html.index('<td class="feesum num">'))
        self.assertLess(html.index('<td class="feesum num">'), html.index('<td class="funddir num">'))
        self.assertIn("fmtSpreadVsMean(m.spread.pct,item.spread_mean_bps)", html)
        self.assertIn("const diff=currentPct-meanPct;", html)
        self.assertIn("fmtZScore(item.spread_zscore)", html)
        self.assertNotIn("4H标准价差", html)
        self.assertNotIn("4小时窗口价差z-score", html)
        self.assertNotIn("上限 / 下限", html)
        self.assertNotIn('<th class="interval num">间隔</th>', html)
        self.assertIn("counterpartyVenue", html)
        self.assertIn('colspan="12"', html)
        self.assertIn("toggleSpreadSort()", html)
        self.assertIn('activeSort==="spread"', html)
        self.assertIn('const ALL_VENUES=["Binance","Bybit","OKX","Bitget","Gate","Kraken","Aster","Hyperliquid","Lighter","Grvt","Paradex","Nado","Ondo","Variational"];', html)
        self.assertIn("const ACTIVE_HINTS=new Set(ALL_VENUES);", html)
        for venue in ["Coinbase", "HTX", "KuCoin", "MEXC", "Backpack", "Ethereal", "Pacifica", "Extended", "StandX", "ApeX Omni"]:
            self.assertNotIn(venue, html)
        self.assertIn("const DEFAULT_VISIBLE_ROWS=20;", html)
        self.assertIn("const DISPLAY_MIN_OI_USD=25000;", html)
        self.assertIn("const DISPLAY_MIN_VOLUME_USD=50000;", html)
        self.assertIn("function displayQuality(item)", html)
        self.assertIn("const displayPool=query?matched:matched.filter(x=>displayQuality(x.item));", html)
        self.assertIn("sorted.slice(0,DEFAULT_VISIBLE_ROWS)", html)
        self.assertNotIn("sortExpandedRows", html)
        self.assertNotIn("const showAll=", html)
        self.assertIn("未平仓额", html)
        self.assertIn("日成交额", html)
        self.assertIn("const annual=(primaryIsA?(dailyA-dailyB):(dailyB-dailyA))*365/100;", html)
        self.assertIn("const oiA=Number(item.oi_a_usd)||0,oiB=Number(item.oi_b_usd)||0;", html)
        self.assertIn("const minOi=Math.min(oiA,oiB);", html)
        self.assertIn("const minVol=Math.min(volA,volB);", html)
        self.assertIn('id="blacklist-input"', html)
        self.assertIn('placeholder="拉黑币种"', html)
        self.assertIn('const BLACKLIST_STORAGE_KEY="perpArb.assetBlacklist.v1";', html)
        self.assertIn("let blacklistedAssets=new Set();", html)
        self.assertIn("function normAsset(v)", html)
        self.assertIn("function assetKeys(item)", html)
        self.assertIn("function isBlacklistedAsset(item)", html)
        self.assertIn("return assetKeys(item).some(asset=>blacklistedAssets.has(asset))", html)
        self.assertIn('href="/rwa"', html)
        self.assertIn('const PAGE_MODE="main";', html)
        self.assertIn("const RWA_STOCK_ASSETS=new Set", html)
        self.assertIn('"DRAM"', html)
        self.assertIn('"PAXG"', html)
        self.assertIn('"WTI"', html)
        self.assertIn("function isRwaStock(item)", html)
        self.assertIn('if(PAGE_MODE==="rwa"&&!isRwaStock(item))return false;', html)
        self.assertIn("localStorage.setItem(BLACKLIST_STORAGE_KEY", html)
        self.assertIn('fetch("/api/asset-blacklist"', html)
        self.assertIn('JSON.stringify({assets:[...blacklistedAssets]})', html)
        self.assertIn("d.asset_blacklist", html)
        self.assertIn("function removeBlacklistAsset(asset)", html)
        self.assertIn('data-asset="${esc(asset)}"', html)
        self.assertIn("if(isBlacklistedAsset(item))return false;", html)
        self.assertLess(
            html.index("if(isBlacklistedAsset(item))return false;"),
            html.index('const q=($("search-input").value||"").trim().toLowerCase();'),
        )
        self.assertIn("if(!q.split(/\\s+/).every(t=>hay.includes(t)))return false;\n    return true;", html)
        self.assertLess(
            html.index('const q=($("search-input").value||"").trim().toLowerCase();'),
            html.index("if(selectedVenues.size&&!(selectedVenues.has(va)||selectedVenues.has(vb)))return false;"),
        )
        self.assertIn(".slider span{display:inline-block;min-width:64px;text-align:right", html)
        self.assertIn(".slider input{width:220px;accent-color:var(--accent)}", html)
        self.assertIn('<input id="oi-slider" type="range" min="0" max="100" step="1" value="10">', html)
        self.assertIn('<input id="vol-slider" type="range" min="0" max="100" step="1" value="0">', html)
        self.assertNotIn("interval-slider", html)
        self.assertNotIn("interval-label", html)
        self.assertNotIn("function intervalThreshold", html)
        self.assertNotIn("间隔≤", html)
        self.assertIn("const THRESHOLD_UNIT_USD=100000;", html)
        self.assertIn('if(m.minOi<threshold($("oi-slider")))return false;', html)
        self.assertIn("const funding=primaryIsA?fa:fb;", html)
        self.assertIn("const bidA=Number(item.best_bid_a)||0,askA=Number(item.best_ask_a)||0;", html)
        self.assertIn("const aLongBShort=bidB-askA;", html)
        self.assertIn("const bLongAShort=bidA-askB;", html)
        self.assertIn("longIsA:aLongBShort>=bLongAShort", html)
        self.assertNotIn("fundDiff", html)
        self.assertIn("function threshold(slider)", html)
        self.assertIn("return value*THRESHOLD_UNIT_USD;", html)
        self.assertIn('threshold($("oi-slider"))', html)
        self.assertIn("let filterTimer=0;", html)
        self.assertIn("function scheduleApplyFilters(delay=120)", html)
        self.assertIn('el.addEventListener("input",()=>{updateSliderLabels();scheduleApplyFilters(120)})', html)
        self.assertIn('el.addEventListener("change",()=>{cancelScheduledFilter();updateSliderLabels();applyFilters()})', html)
        self.assertNotIn('addEventListener("input",()=>{updateSliderLabels();applyFilters()})', html)
        self.assertIn("function fmtThresholdMoney(v)", html)
        self.assertIn('v<=0)return"$0"', html)
        self.assertIn('threshold($("vol-slider"))', html)
        self.assertIn("Bitget", html)
        self.assertIn("Gate", html)
        self.assertIn("Kraken", html)
        self.assertIn('"Aster","Hyperliquid","Lighter","Grvt","Paradex","Nado","Ondo","Variational"', html)
        self.assertIn("Aster", html)
        self.assertNotIn("dedupeKey", html)
        self.assertNotIn("/api/backtest", html)
        self.assertNotIn("backtest-cards", html)
        self.assertNotIn("历史验证概览", html)
        self.assertNotIn("热点聚合", html)
        self.assertNotIn("候选卡片", html)

    def test_rwa_dashboard_html_enables_stock_only_page(self) -> None:
        html = build_dashboard_html(refresh_seconds=2.5, page="rwa")

        self.assertIn('const PAGE_MODE="rwa";', html)
        self.assertIn('class="page-tab active" href="/rwa">RWA 股票</a>', html)
        self.assertIn("无RWA股票匹配结果", html)
        self.assertIn("item.is_rwa_stock===true", html)

    def test_opportunity_zscore_window_points_tracks_four_hours(self) -> None:
        self.assertEqual(_opportunity_zscore_window_points(10), 1440)
        self.assertEqual(_opportunity_zscore_window_points(3), 4800)
        self.assertEqual(_opportunity_zscore_window_points(0), 14400)

    def test_spread_vs_mean_uses_executable_spread(self) -> None:
        opportunity = _make_scored_opportunity().opportunity

        self.assertAlmostEqual(_spread_vs_mean_pct(opportunity), 0.55, places=2)

    def test_blacklist_asset_normalization_is_exact(self) -> None:
        self.assertEqual(_normalize_blacklist_asset("bb"), "BB")
        self.assertEqual(_normalize_blacklist_asset("BB/USD"), "BB")
        self.assertEqual(_normalize_blacklist_asset("BB-USDT"), "BB")
        self.assertEqual(_normalize_blacklist_asset("BBB"), "BBB")

    def test_tg_alert_requires_spread_mean_delta_zscore_and_duration(self) -> None:
        app = _DashboardApp(scanner=_ScannerStub(), refresh_seconds=2.0)
        scored = _make_scored_opportunity(spread_zscore=1.25, spread_mean_bps=10.0)

        with patch("perp_arb.dashboard.time.monotonic", side_effect=[100.0, 159.0, 161.0]):
            self.assertEqual(app.check_tradable_alerts([scored]), [])
            self.assertEqual(app.check_tradable_alerts([scored]), [])
            self.assertEqual(app.check_tradable_alerts([scored]), [scored])

    def test_tg_alert_skips_blacklisted_asset(self) -> None:
        app = _DashboardApp(scanner=_ScannerStub(), refresh_seconds=2.0)
        app.set_asset_blacklist(["ETH"])
        scored = _make_scored_opportunity(spread_zscore=1.25, spread_mean_bps=10.0)

        with patch("perp_arb.dashboard.time.monotonic", side_effect=[100.0, 161.0]):
            self.assertEqual(app.check_tradable_alerts([scored]), [])
            self.assertEqual(app.check_tradable_alerts([scored]), [])

    def test_tg_alert_skips_blacklisted_leg_asset_alias(self) -> None:
        app = _DashboardApp(scanner=_ScannerStub(), refresh_seconds=2.0)
        app.set_asset_blacklist(["XAU"])
        scored = _make_scored_opportunity(
            asset="PAXG",
            leg_a_asset="XAU",
            leg_b_asset="PAXG",
            spread_zscore=1.25,
            spread_mean_bps=10.0,
        )

        with patch("perp_arb.dashboard.time.monotonic", side_effect=[100.0, 161.0]):
            self.assertEqual(app.check_tradable_alerts([scored]), [])
            self.assertEqual(app.check_tradable_alerts([scored]), [])

    def test_tg_alert_blacklist_does_not_match_substrings(self) -> None:
        app = _DashboardApp(scanner=_ScannerStub(), refresh_seconds=2.0)
        app.set_asset_blacklist(["ET"])
        scored = _make_scored_opportunity(spread_zscore=1.25, spread_mean_bps=10.0)

        with patch("perp_arb.dashboard.time.monotonic", side_effect=[100.0, 161.0]):
            self.assertEqual(app.check_tradable_alerts([scored]), [])
            self.assertEqual(app.check_tradable_alerts([scored]), [scored])

    def test_tg_alert_ignores_score_label_and_profit_but_requires_thresholds(self) -> None:
        app = _DashboardApp(scanner=_ScannerStub(), refresh_seconds=2.0)
        scored = _make_scored_opportunity(
            label_value="blocked",
            composite_score=1.0,
            confidence=1.0,
            expected_profit_bps=-100.0,
            spread_zscore=1.24,
            spread_mean_bps=10.0,
        )

        with patch("perp_arb.dashboard.time.monotonic", side_effect=[100.0, 161.0]):
            self.assertEqual(app.check_tradable_alerts([scored]), [])
            self.assertEqual(app.check_tradable_alerts([scored]), [])

    def test_tg_alert_requires_one_million_min_leg_oi(self) -> None:
        app = _DashboardApp(scanner=_ScannerStub(), refresh_seconds=2.0)
        scored = _make_scored_opportunity(spread_zscore=1.25, spread_mean_bps=10.0, leg_b_oi_usd=999_999.0)

        with patch("perp_arb.dashboard.time.monotonic", side_effect=[100.0, 161.0]):
            self.assertEqual(app.check_tradable_alerts([scored]), [])
            self.assertEqual(app.check_tradable_alerts([scored]), [])

    def test_tg_alert_message_omits_score(self) -> None:
        app = _DashboardApp(scanner=_ScannerStub(), refresh_seconds=2.0)
        sent: list[str] = []
        app._notifier = SimpleNamespace(send=sent.append)
        scored = _make_scored_opportunity(spread_zscore=1.25, spread_mean_bps=10.0, composite_score=88.8)

        with patch("perp_arb.dashboard.time.monotonic", side_effect=[100.0, 161.0]):
            self.assertEqual(app.check_tradable_alerts([scored]), [])
            self.assertEqual(app.check_tradable_alerts([scored]), [scored])

        self.assertEqual(len(sent), 1)
        self.assertNotIn("评分", sent[0])
        self.assertNotIn("88.8", sent[0])
        self.assertIn("类型:", sent[0])

    def test_tg_alert_skips_lighter_ticker_only_leg(self) -> None:
        app = _DashboardApp(scanner=_ScannerStub(), refresh_seconds=2.0)
        scored = _make_scored_opportunity(spread_zscore=1.25, spread_mean_bps=10.0, leg_b_ticker_only=True)

        with patch("perp_arb.dashboard.time.monotonic", side_effect=[100.0, 161.0]):
            self.assertEqual(app.check_tradable_alerts([scored]), [])
            self.assertEqual(app.check_tradable_alerts([scored]), [])

    def test_tg_alert_resets_when_signal_disappears(self) -> None:
        app = _DashboardApp(scanner=_ScannerStub(), refresh_seconds=2.0)
        strong = _make_scored_opportunity(spread_zscore=1.25, spread_mean_bps=10.0)
        weak = _make_scored_opportunity(spread_zscore=1.25, spread_mean_bps=20.0)

        with patch("perp_arb.dashboard.time.monotonic", side_effect=[100.0, 130.0, 140.0, 201.0]):
            self.assertEqual(app.check_tradable_alerts([strong]), [])
            self.assertEqual(app.check_tradable_alerts([weak]), [])
            self.assertEqual(app.check_tradable_alerts([strong]), [])
            self.assertEqual(app.check_tradable_alerts([strong]), [strong])

    def test_source_status_update_preserves_existing_opportunities(self) -> None:
        app = _DashboardApp(scanner=_ScannerStub(), refresh_seconds=2.0)
        app._cached_payload = {
            "timestamp": "old",
            "snapshot_count": 1,
            "opportunity_count": 2,
            "scan_duration_ms": 123.0,
            "source_statuses": [],
            "dashboard_summary": {"market_regime": "stable"},
            "alert_sound": False,
            "alert_items": [],
            "opportunities": [{"asset": "BTC"}, {"asset": "ETH"}],
        }
        app._adapter_cache = {
            "binance-ws": (
                [object()],
                SourceStatus(adapter_name="binance-ws", ok=True, snapshot_count=1, poll_duration_ms=0.0),
            )
        }

        app._update_source_status_payload()

        self.assertEqual(app._cached_payload["opportunities"], [{"asset": "BTC"}, {"asset": "ETH"}])
        self.assertEqual(app._cached_payload["snapshot_count"], 1)
        self.assertEqual(len(app._cached_payload["source_statuses"]), 1)

    def test_rebuild_payload_sets_scan_duration_ms(self) -> None:
        tmpfile = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmpfile.close()
        try:
            store = StateStore(tmpfile.name)
            points = [
                OpportunityStatePoint(
                    timestamp=datetime.now(timezone.utc) - timedelta(minutes=35 - i),
                    executable_spread_bps=5.0 if i < 34 else 12.0,
                )
                for i in range(35)
            ]
            store.save_opportunity_points(("BTC", "USD", "binance", "hyperliquid"), points)
            store.close()

            app = _DashboardApp(scanner=_ScannerStub(), refresh_seconds=2.0)
            with patch("perp_arb.dashboard.build_batch_payload", return_value={"scan_duration_ms": 321.0}) as payload_mock:
                with patch.object(app, "check_tradable_alerts", return_value=[]):
                    app._rebuild_payload(scan_duration_ms=321.0, timestamp=datetime.now(timezone.utc))

            self.assertEqual(app._cached_payload["scan_duration_ms"], 321.0)
            self.assertEqual(payload_mock.call_count, 1)
        finally:
            os.unlink(tmpfile.name)

def _make_leg(venue: str, bid: float, ask: float) -> PerpLegSnapshot:
    mid = (bid + ask) / 2.0
    return PerpLegSnapshot(
        venue=venue,
        market_type="perp",
        asset="ETH",
        best_bid=bid,
        best_ask=ask,
        mark_price=mid,
        oracle_price=mid,
        index_price=mid,
        taker_fee_bps=2.0,
        maker_fee_bps=0.0,
        depth_10k_usd=100_000.0,
        depth_50k_usd=200_000.0,
        top_1pct_depth_usd=250_000.0,
        volume_depth_ratio=5.0,
        oi_usd=5_000_000.0,
        funding_rate_bps=0.0,
    )


def _make_scored_opportunity(
    *,
    asset: str = "ETH",
    leg_a_asset: str = "ETH",
    leg_b_asset: str = "ETH",
    spread_zscore: float = 1.25,
    spread_mean_bps: float = 10.0,
    label_value: str = "blocked",
    composite_score: float = 1.0,
    confidence: float = 1.0,
    expected_profit_bps: float = -100.0,
    leg_a_oi_usd: float = 5_000_000.0,
    leg_b_oi_usd: float = 5_000_000.0,
    leg_b_ticker_only: bool = False,
):
    leg_a = _make_leg("bybit", 100.0, 100.1)
    leg_b = _make_leg("lighter", 100.75, 100.85)
    leg_a = PerpLegSnapshot(**{**leg_a.__dict__, "asset": leg_a_asset, "oi_usd": leg_a_oi_usd})
    leg_b = PerpLegSnapshot(**{**leg_b.__dict__, "asset": leg_b_asset, "oi_usd": leg_b_oi_usd})
    opportunity = PerpArbOpportunity(
        asset=asset,
        quote="USD",
        leg_a=leg_a,
        leg_b=leg_b,
        notional_usd=10_000.0,
        capital_used_usd=2_500.0,
        slippage_bps=1.0,
        impact_cost_10k_bps=1.0,
        impact_cost_50k_bps=2.0,
        spread_zscore=spread_zscore,
        spread_mean_bps=spread_mean_bps,
        funding_persistence_score=0.5,
        bucket_hint=OpportunityBucket.DISLOCATION,
        now=datetime.now(timezone.utc),
        metadata={"leg_a_ticker_only": False, "leg_b_ticker_only": leg_b_ticker_only},
    )
    breakdown = SimpleNamespace(
        composite_score=composite_score,
        confidence=confidence,
        expected_profit_bps=expected_profit_bps,
        expected_profit_usd=-10.0,
        roi_per_capital=-0.001,
        bucket_type=OpportunityBucket.DISLOCATION,
    )
    return SimpleNamespace(opportunity=opportunity, breakdown=breakdown, label=SimpleNamespace(value=label_value))


if __name__ == "__main__":
    unittest.main()
