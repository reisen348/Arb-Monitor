from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional, Sequence, TextIO

from .backtest import build_backtest_payload, format_backtest_text, summarize_state_store
from .display import (
    bucket_display,
    display_many,
    display_term,
    label_display,
    label_rank,
    market_regime_display,
    order_type_display,
    role_display,
    side_display,
)
from .aster import AsterAdapter, AsterAdapterConfig
from .binance import BinanceAdapter, BinanceAdapterConfig
from .bitget import BitgetAdapter, BitgetAdapterConfig
from .bybit import BybitAdapter, BybitAdapterConfig
from .gate import GateAdapter, GateAdapterConfig
from .grvt import GrvtAdapter, GrvtAdapterConfig
from .hyperliquid import HyperliquidAdapter, HyperliquidAdapterConfig
from .kraken import KrakenAdapter, KrakenAdapterConfig
from .lighter import LighterAdapter, LighterAdapterConfig
from .nado import NadoAdapter, NadoAdapterConfig
from .ondo import OndoAdapter, OndoAdapterConfig
from .okx import OkxAdapter, OkxAdapterConfig
from .paradex import ParadexAdapter, ParadexAdapterConfig
from .market_data import MockMarketDataAdapter, ScannerConfig
from .models import ExecutionLabel, ScoredOpportunity
from .payload import build_batch_payload
from .scanner import RealtimeScanner
from .ws_adapters import (
    BinanceWebsocketAdapter,
    BybitWebsocketAdapter,
    GrvtWebsocketAdapter,
    HyperliquidWebsocketAdapter,
    OkxWebsocketAdapter,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="perp-arb", description="Realtime multi-perp arbitrage scanner.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_parser = subparsers.add_parser("scan", help="Run the realtime scanner.")
    _add_runtime_arguments(scan_parser)
    scan_parser.add_argument(
        "--format",
        choices=["text", "json", "dashboard"],
        default="text",
        help="Output format.",
    )
    _add_source_arguments(scan_parser)

    backtest_parser = subparsers.add_parser("backtest", help="Inspect persisted opportunity history.")
    _add_backtest_arguments(backtest_parser)
    backtest_parser.add_argument("--top", type=int, default=20, help="Maximum number of candidate series to print.")
    backtest_parser.add_argument("--format", choices=["text", "json", "dashboard"], default="text", help="Output format.")

    dashboard_parser = subparsers.add_parser("dashboard", help="Serve a lightweight realtime dashboard.")
    _add_runtime_arguments(dashboard_parser, iterations_default=0)
    _add_source_arguments(dashboard_parser)
    _add_dashboard_persistence_arguments(dashboard_parser)
    dashboard_parser.add_argument("--host", default="127.0.0.1", help="Dashboard bind host.")
    dashboard_parser.add_argument("--port", type=int, default=8765, help="Dashboard bind port.")
    dashboard_parser.add_argument(
        "--refresh",
        type=float,
        default=2.0,
        help="Client polling interval in seconds.",
    )
    dashboard_parser.add_argument(
        "--tg-token",
        default=None,
        help="Telegram Bot token (also reads TG_BOT_TOKEN env var).",
    )
    dashboard_parser.add_argument(
        "--tg-chat-id",
        default=None,
        help="Telegram chat ID (also reads TG_CHAT_ID env var).",
    )
    return parser


def _add_runtime_arguments(parser: argparse.ArgumentParser, iterations_default: int = 1) -> None:
    parser.add_argument("--iterations", type=int, default=iterations_default, help="Number of scan cycles to run.")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between scan cycles.")
    parser.add_argument("--top", type=int, default=10, help="Maximum number of opportunities to print.")
    parser.add_argument(
        "--min-label",
        choices=[label.value for label in ExecutionLabel],
        default=ExecutionLabel.WATCH.value,
        help="Lowest label to display.",
    )


def _add_source_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--source",
        choices=[
            "mock", "hyperliquid", "grvt", "paradex", "lighter", "nado", "ondo",
            "binance", "okx", "bybit", "bitget", "gate", "kraken", "aster",
            "hyperliquid_grvt", "hyperliquid_paradex", "hyperliquid_lighter",
            "cex", "dex", "all_live",
        ],
        default="mock",
        help="Market data source to scan.",
    )
    parser.add_argument(
        "--transport",
        choices=["rest", "ws"],
        default="rest",
        help="Transport mode for live sources. Websocket mode seeds from REST then streams updates.",
    )
    parser.add_argument(
        "--venues",
        nargs="+",
        default=["hyperliquid", "drift", "aevo"],
        help="Mock venues to simulate.",
    )
    parser.add_argument(
        "--assets",
        nargs="*",
        default=None,
        help="Assets to scan. Omit to scan all available pairs.",
    )
    parser.add_argument(
        "--hyperliquid-dex",
        default="",
        help="Optional Hyperliquid perp dex name.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="HTTP timeout for live Hyperliquid requests.",
    )
    parser.add_argument(
        "--top-book-markets",
        type=int,
        default=10,
        help="Fetch full orderbooks only for the top N markets by volume/OI.",
    )
    parser.add_argument(
        "--lighter-book-request-workers",
        type=int,
        default=4,
        help="Max concurrent Lighter orderbook requests.",
    )
    parser.add_argument(
        "--lighter-top-book-markets",
        type=int,
        default=None,
        help="Top N markets for Lighter orderbook fetching (default: same as --top-book-markets).",
    )
    parser.add_argument(
        "--grvt-base-url",
        default="https://market-data.grvt.io",
        help="GRVT market data base URL.",
    )
    parser.add_argument(
        "--grvt-quotes",
        nargs="+",
        default=["USDT", "USDC"],
        help="GRVT quotes to include when discovering perp instruments.",
    )
    parser.add_argument(
        "--grvt-top-book-markets",
        type=int,
        default=None,
        help="Override --top-book-markets for GRVT only (default: 100, since GRVT has ~95 perps).",
    )


def _add_backtest_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--db-path", default="arb_state.db", help="SQLite state database path.")
    parser.add_argument("--max-points", type=int, default=3600, help="Max history points to load per series.")
    parser.add_argument("--min-samples", type=int, default=30, help="Minimum samples required per opportunity series.")
    parser.add_argument("--signal-zscore", type=float, default=2.0, help="Absolute z-score threshold for candidate signals.")
    parser.add_argument("--min-signal-spread-bps", type=float, default=8.0, help="Alternative signal threshold: absolute spread deviation from history mean in bps.")
    parser.add_argument("--forward-points", type=int, default=5, help="Number of future points to inspect for mean reversion hits.")
    parser.add_argument("--reversion-ratio", type=float, default=0.5, help="Hit if absolute spread compresses to this ratio of the signal spread.")
    parser.add_argument("--lookback-hours", type=float, default=12.0, help="Only analyze history points within the last N hours.")


def _add_dashboard_persistence_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--db-path", default="arb_state.db", help="SQLite state database path.")
    parser.add_argument(
        "--archive-retention-hours",
        type=float,
        default=24.0,
        help="Hours of opportunity archive history to keep in SQLite.",
    )
    parser.add_argument(
        "--asset-blacklist-path",
        default="asset_blacklist.json",
        help="JSON file used by the dashboard and Telegram alerts to hide blacklisted assets.",
    )
    # Backward-compatible no-op options for existing service units and scripts
    # that still pass the removed dashboard history-panel settings.
    parser.add_argument("--max-points", type=int, default=3600, help=argparse.SUPPRESS)
    parser.add_argument("--min-samples", type=int, default=30, help=argparse.SUPPRESS)
    parser.add_argument("--signal-zscore", type=float, default=2.0, help=argparse.SUPPRESS)
    parser.add_argument("--min-signal-spread-bps", type=float, default=8.0, help=argparse.SUPPRESS)
    parser.add_argument("--forward-points", type=int, default=5, help=argparse.SUPPRESS)
    parser.add_argument("--reversion-ratio", type=float, default=0.5, help=argparse.SUPPRESS)
    parser.add_argument("--lookback-hours", type=float, default=12.0, help=argparse.SUPPRESS)


def main(argv: Optional[Sequence[str]] = None, out: Optional[TextIO] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    output = out or sys.stdout
    if args.command == "scan":
        return run_scan(args, output)
    if args.command == "backtest":
        return run_backtest(args, output)
    if args.command == "dashboard":
        from .dashboard import serve_dashboard

        return serve_dashboard(args, output)
    parser.error(f"unsupported command: {args.command}")
    return 2


def run_scan(args: argparse.Namespace, out: TextIO) -> int:
    adapters = build_adapters(args)
    scanner = RealtimeScanner(
        adapters=adapters,
        scanner_config=ScannerConfig(
            top_n=args.top,
            min_label=ExecutionLabel(args.min_label),
            scan_interval_seconds=args.interval,
        ),
    )
    try:
        scanner.run(iterations=args.iterations, sleep_seconds=args.interval, on_batch=lambda batch: emit_batch(batch, args.format, out))
    finally:
        scanner.close()
        for adapter in adapters:
            stop = getattr(adapter, "stop", None)
            if callable(stop):
                stop()
    return 0


def run_backtest(args: argparse.Namespace, out: TextIO) -> int:
    summary = summarize_state_store(
        db_path=args.db_path,
        max_points=args.max_points,
        top_n=args.top,
        min_samples=args.min_samples,
        signal_zscore=args.signal_zscore,
        min_signal_spread_bps=args.min_signal_spread_bps,
        forward_points=args.forward_points,
        reversion_ratio=args.reversion_ratio,
        lookback_hours=args.lookback_hours,
    )
    if args.format in {"json", "dashboard"}:
        out.write(json.dumps(build_backtest_payload(summary)) + "\n")
    else:
        out.write(format_backtest_text(summary) + "\n")
    out.flush()
    return 0


def _grvt_top_book(args: argparse.Namespace) -> int:
    """Return the effective top_book_markets for GRVT (default 100)."""
    if getattr(args, "grvt_top_book_markets", None) is not None:
        return args.grvt_top_book_markets
    return 100


def _lighter_top_book(args: argparse.Namespace) -> int:
    """Return the effective top_book_markets for Lighter (default 15)."""
    if getattr(args, "lighter_top_book_markets", None) is not None:
        return args.lighter_top_book_markets
    return 15


def build_adapters(args: argparse.Namespace):
    if args.source == "hyperliquid":
        if args.transport == "ws":
            return [
                HyperliquidWebsocketAdapter(
                    HyperliquidAdapterConfig(
                        assets=args.assets,
                        dex=args.hyperliquid_dex,
                        timeout_seconds=args.timeout,
                        top_book_markets=args.top_book_markets,
                    )
                )
            ]
        adapters = [
            HyperliquidAdapter(
                HyperliquidAdapterConfig(
                    assets=args.assets,
                    dex=args.hyperliquid_dex,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                )
            )
        ]
        return adapters
    if args.source == "grvt":
        if args.transport == "ws":
            return [
                GrvtWebsocketAdapter(
                    GrvtAdapterConfig(
                        assets=args.assets,
                        quotes=args.grvt_quotes,
                        base_url=args.grvt_base_url,
                        timeout_seconds=args.timeout,
                        top_book_markets=_grvt_top_book(args),
                    )
                )
            ]
        return [
            GrvtAdapter(
                GrvtAdapterConfig(
                    assets=args.assets,
                    quotes=args.grvt_quotes,
                    base_url=args.grvt_base_url,
                    timeout_seconds=args.timeout,
                    top_book_markets=_grvt_top_book(args),
                )
            )
        ]
    if args.source == "binance":
        return [
            BinanceAdapter(
                BinanceAdapterConfig(
                    assets=args.assets,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                )
            )
        ]
    if args.source == "okx":
        return [
            OkxAdapter(
                OkxAdapterConfig(
                    assets=args.assets,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                )
            )
        ]
    if args.source == "bybit":
        return [
            BybitAdapter(
                BybitAdapterConfig(
                    assets=args.assets,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                )
            )
        ]
    if args.source == "bitget":
        return [
            BitgetAdapter(
                BitgetAdapterConfig(
                    assets=args.assets,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                )
            )
        ]
    if args.source == "gate":
        return [
            GateAdapter(
                GateAdapterConfig(
                    assets=args.assets,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                )
            )
        ]
    if args.source == "kraken":
        return [
            KrakenAdapter(
                KrakenAdapterConfig(
                    assets=args.assets,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                )
            )
        ]
    if args.source == "aster":
        return [
            AsterAdapter(
                AsterAdapterConfig(
                    assets=args.assets,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                )
            )
        ]
    if args.source == "paradex":
        return [
            ParadexAdapter(
                ParadexAdapterConfig(
                    assets=args.assets,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                )
            )
        ]
    if args.source == "lighter":
        return [
            LighterAdapter(
                LighterAdapterConfig(
                    assets=args.assets,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                    book_request_workers=args.lighter_book_request_workers,
                )
            )
        ]
    if args.source == "nado":
        return [
            NadoAdapter(
                NadoAdapterConfig(
                    assets=args.assets,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                )
            )
        ]
    if args.source == "ondo":
        return [
            OndoAdapter(
                OndoAdapterConfig(
                    assets=args.assets,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                )
            )
        ]
    if args.source == "hyperliquid_grvt":
        if args.transport == "ws":
            return [
                HyperliquidWebsocketAdapter(
                    HyperliquidAdapterConfig(
                        assets=args.assets,
                        dex=args.hyperliquid_dex,
                        timeout_seconds=args.timeout,
                        top_book_markets=args.top_book_markets,
                    )
                ),
                GrvtWebsocketAdapter(
                    GrvtAdapterConfig(
                        assets=args.assets,
                        quotes=args.grvt_quotes,
                        base_url=args.grvt_base_url,
                        timeout_seconds=args.timeout,
                        top_book_markets=_grvt_top_book(args),
                    )
                ),
            ]
        return [
            HyperliquidAdapter(
                HyperliquidAdapterConfig(
                    assets=args.assets,
                    dex=args.hyperliquid_dex,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                )
            ),
            GrvtAdapter(
                GrvtAdapterConfig(
                    assets=args.assets,
                    quotes=args.grvt_quotes,
                    base_url=args.grvt_base_url,
                    timeout_seconds=args.timeout,
                    top_book_markets=_grvt_top_book(args),
                )
            ),
        ]
    if args.source == "hyperliquid_paradex":
        return [
            HyperliquidAdapter(
                HyperliquidAdapterConfig(
                    assets=args.assets,
                    dex=args.hyperliquid_dex,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                )
            ),
            ParadexAdapter(
                ParadexAdapterConfig(
                    assets=args.assets,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                )
            ),
        ]
    if args.source == "hyperliquid_lighter":
        return [
            HyperliquidAdapter(
                HyperliquidAdapterConfig(
                    assets=args.assets,
                    dex=args.hyperliquid_dex,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                )
            ),
            LighterAdapter(
                LighterAdapterConfig(
                    assets=args.assets,
                    timeout_seconds=args.timeout,
                    top_book_markets=args.top_book_markets,
                    book_request_workers=args.lighter_book_request_workers,
                )
            ),
        ]
    if args.source == "cex":
        return _build_cex_adapters(args)
    if args.source == "dex":
        return _build_dex_adapters(args)
    if args.source == "all_live":
        return _build_cex_adapters(args) + _build_dex_adapters(args)
    mock_assets = args.assets or ["BTC", "ETH", "SOL"]
    return [
        MockMarketDataAdapter(name=f"mock-{venue}", venue=venue, assets=mock_assets, seed=index)
        for index, venue in enumerate(args.venues)
    ]


def _build_cex_adapters(args):
    transport = getattr(args, "transport", "rest")
    if transport == "ws":
        return [
            BinanceWebsocketAdapter(BinanceAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
            OkxWebsocketAdapter(OkxAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
            BybitWebsocketAdapter(BybitAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
            BitgetAdapter(BitgetAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
            GateAdapter(GateAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
            KrakenAdapter(KrakenAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
            AsterAdapter(AsterAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
        ]
    return [
        BinanceAdapter(BinanceAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
        OkxAdapter(OkxAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
        BybitAdapter(BybitAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
        BitgetAdapter(BitgetAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
        GateAdapter(GateAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
        KrakenAdapter(KrakenAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
        AsterAdapter(AsterAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
    ]


def _build_dex_adapters(args):
    transport = getattr(args, "transport", "rest")
    if transport == "ws":
        return [
            HyperliquidWebsocketAdapter(HyperliquidAdapterConfig(assets=args.assets, dex=args.hyperliquid_dex, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
            GrvtWebsocketAdapter(GrvtAdapterConfig(assets=args.assets, quotes=args.grvt_quotes, base_url=args.grvt_base_url, timeout_seconds=args.timeout, top_book_markets=_grvt_top_book(args))),
            ParadexAdapter(ParadexAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
            LighterAdapter(LighterAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=_lighter_top_book(args), book_request_workers=args.lighter_book_request_workers)),
            NadoAdapter(NadoAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
            OndoAdapter(OndoAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
        ]
    return [
        HyperliquidAdapter(HyperliquidAdapterConfig(assets=args.assets, dex=args.hyperliquid_dex, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
        GrvtAdapter(GrvtAdapterConfig(assets=args.assets, quotes=args.grvt_quotes, base_url=args.grvt_base_url, timeout_seconds=args.timeout, top_book_markets=_grvt_top_book(args))),
        ParadexAdapter(ParadexAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
        LighterAdapter(LighterAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=_lighter_top_book(args), book_request_workers=args.lighter_book_request_workers)),
        NadoAdapter(NadoAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
        OndoAdapter(OndoAdapterConfig(assets=args.assets, timeout_seconds=args.timeout, top_book_markets=args.top_book_markets)),
    ]


def emit_batch(batch, output_format: str, out: TextIO) -> None:
    if output_format in {"json", "dashboard"}:
        payload = build_batch_payload(batch)
        if output_format == "dashboard":
            out.write(_format_dashboard_batch(payload))
            out.write("\n")
        else:
            out.write(json.dumps(payload) + "\n")
        out.flush()
        return

    out.write(
        f"[{batch.timestamp.isoformat()}] snapshots={len(batch.snapshots)} "
        f"opportunities={len(batch.opportunities)} shown={len(batch.scored_opportunities)}\n"
    )
    for index, scored in enumerate(batch.scored_opportunities, start=1):
        out.write(_format_opportunity_line(index, scored) + "\n")
    out.flush()


# ---------------------------------------------------------------------------
# CLI text formatting helpers
# ---------------------------------------------------------------------------

def _format_opportunity_line(index: int, scored: ScoredOpportunity) -> str:
    opp = scored.opportunity
    br = scored.breakdown
    max_funding_change = max(abs(opp.leg_a.funding_change_bps), abs(opp.leg_b.funding_change_bps))
    max_oi_change = max(abs(opp.leg_a.oi_change_pct), abs(opp.leg_b.oi_change_pct))
    tag_suffix = f" 风险={','.join(display_many('tag', scored.tags))}" if scored.tags else ""
    policy_suffix = _format_policy_suffix(scored)
    plan_suffix = _format_execution_plan_suffix(scored)
    leg_suffix = _format_execution_legs_suffix(scored)
    return (
        f"{index:02d}. {opp.asset}/{opp.quote} {opp.leg_a.venue}<->{opp.leg_b.venue} "
        f"label={scored.label.value} bucket={br.bucket_type.value} "
        f"score={br.composite_score:.2f} profit_bps={br.expected_profit_bps:.2f} "
        f"roi={br.roi_per_capital:.4f} spread_z={opp.spread_zscore:.2f} "
        f"micro_jump={opp.micro_jump_frequency:.2f} shock_jump={opp.shock_jump_frequency:.2f} "
        f"jump={opp.jump_frequency:.2f} fund_dchg={max_funding_change:.2f} "
        f"oi_dchg={max_oi_change:.2f} direction={br.direction}{tag_suffix}{policy_suffix}{plan_suffix}{leg_suffix}"
    )


def _format_dashboard_batch(payload: dict) -> str:
    summary = payload["dashboard_summary"]
    lines = [
        f"[{payload['timestamp']}] 市场状态={market_regime_display(summary['market_regime'])} "
        f"快照数={payload['snapshot_count']} 机会数={payload['opportunity_count']}",
        (
            "标签分布="
            f"可交易:{summary['label_counts']['tradable']} "
            f"观察:{summary['label_counts']['watch']} "
            f"拦截:{summary['label_counts']['blocked']}"
        ),
    ]
    if summary["top_opportunity"] is not None:
        top = summary["top_opportunity"]
        lines.append(
            f"头号机会={top['asset']} {top['venues'][0]}<->{top['venues'][1]} "
            f"标签={label_display(top['label'])} 评分={top['composite_score']:.2f} 预期收益={top['expected_profit_bps']:.2f}bps"
        )
    for index, opportunity in enumerate(payload["opportunities"], start=1):
        card = opportunity["dashboard_card"]
        lines.append(
            f"{index:02d}. {card['title']} 标签={label_display(opportunity['label'])} 信号强度={card['conviction']} "
            f"观点={card['thesis']}"
        )
        lines.append(
            f"    当前重点={', '.join(card['why_now'])} 监控项={', '.join(card['monitoring_points'])}"
        )
        if card["risks"]:
            lines.append(f"    风险标签={', '.join(card['risks'])}")
    return "\n".join(lines)


def _format_policy_suffix(scored: ScoredOpportunity) -> str:
    policy_parts: List[str] = []
    if not scored.policy.allow_taker and scored.policy.allow_maker:
        policy_parts.append("仅挂单")
    if scored.policy.size_multiplier < 1.0:
        policy_parts.append(f"仓位={scored.policy.size_multiplier:.2f}")
    if not scored.policy.allow_carry:
        policy_parts.append("禁做carry")
    if not policy_parts:
        return ""
    return f" 策略={','.join(policy_parts)}"


def _format_execution_plan_suffix(scored: ScoredOpportunity) -> str:
    plan = scored.execution_plan
    strategy = "/".join(bucket_display(item) for item in plan.allowed_strategies) if plan.allowed_strategies else "无"
    return (
        f" 看板动作={display_term('action', plan.action)}:{display_term('execution_style', plan.execution_style)}:{plan.max_notional_usd:.0f}"
        f":{strategy}"
    )


def _format_execution_legs_suffix(scored: ScoredOpportunity) -> str:
    if not scored.execution_plan.legs:
        return ""
    leg_parts = [
        f"{role_display(leg.role)}:{side_display(leg.side)}@{leg.venue}:{order_type_display(leg.order_type)}"
        for leg in scored.execution_plan.legs
    ]
    return f" 腿计划={';'.join(leg_parts)}"


if __name__ == "__main__":
    raise SystemExit(main())
