from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Optional, TextIO
from urllib import request as url_request
from urllib.parse import urlparse

from .cli import build_adapters
from .payload import build_batch_payload
from .market_data import ScannerConfig
from .models import ExecutionLabel
from .persistence import StateStore
from .practice import PracticeTracker
from .scanner import RealtimeScanner
from .state import StateTrackerConfig

logger = logging.getLogger("perp_arb.dashboard")

# Minimum seconds a signal must persist before triggering a Telegram alert.
TG_ALERT_SECONDS = 60.0
TG_ALERT_MIN_SPREAD_VS_MEAN_PCT = 0.5
TG_ALERT_MIN_SPREAD_ZSCORE = 1.25
TG_ALERT_MIN_LEG_OI_USD = 1_000_000.0
_BLACKLIST_QUOTE_SUFFIX_RE = re.compile(r"[-_](USD|USDT|USDC|USDE|FDUSD|USDT0)(\.P)?$")


def _normalize_blacklist_asset(value) -> str:
    asset = str(value or "").strip().upper()
    if not asset:
        return ""
    asset = re.split(r"[/\s]+", asset, maxsplit=1)[0]
    asset = _BLACKLIST_QUOTE_SUFFIX_RE.sub("", asset)
    asset = re.sub(r"-PERP$", "", asset)
    return asset


def _has_lighter_ticker_only_leg(opportunity) -> bool:
    metadata = getattr(opportunity, "metadata", {}) or {}
    leg_a_lighter = str(getattr(opportunity.leg_a, "venue", "")).lower() == "lighter"
    leg_b_lighter = str(getattr(opportunity.leg_b, "venue", "")).lower() == "lighter"
    return (
        (leg_a_lighter and bool(metadata.get("leg_a_ticker_only")))
        or (leg_b_lighter and bool(metadata.get("leg_b_ticker_only")))
    )


def _opportunity_zscore_window_points(scan_interval_seconds: float) -> int:
    return max(3, int(round(4 * 60 * 60 / max(float(scan_interval_seconds), 1.0))))


def _max_executable_spread_bps(opportunity) -> float:
    leg_a = opportunity.leg_a
    leg_b = opportunity.leg_b
    a_mid = (leg_a.best_bid + leg_a.best_ask) / 2.0
    b_mid = (leg_b.best_bid + leg_b.best_ask) / 2.0
    a_long_b_short = (leg_b.best_bid - leg_a.best_ask) / max(a_mid, 1e-12) * 10_000.0
    b_long_a_short = (leg_a.best_bid - leg_b.best_ask) / max(b_mid, 1e-12) * 10_000.0
    return max(a_long_b_short, b_long_a_short)


def _spread_vs_mean_pct(opportunity) -> float:
    current_bps = max(_max_executable_spread_bps(opportunity), 0.0)
    mean_bps = float(opportunity.spread_mean_bps or 0.0)
    return (current_bps - mean_bps) / 100.0


def _setup_logging() -> None:
    root = logging.getLogger("perp_arb")
    if root.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root.addHandler(handler)
    root.setLevel(logging.INFO)


def serve_dashboard(args, out: Optional[TextIO] = None) -> int:
    _setup_logging()
    adapters = build_adapters(args)
    adapter_names = [getattr(a, "name", type(a).__name__) for a in adapters]
    logger.info("初始化数据源: %s", ", ".join(adapter_names))
    state_store = StateStore(args.db_path, archive_retention_hours=args.archive_retention_hours)
    opportunity_window_points = _opportunity_zscore_window_points(args.interval)

    scanner = RealtimeScanner(
        adapters=adapters,
        scanner_config=ScannerConfig(
            top_n=args.top,
            min_label=ExecutionLabel(args.min_label),
            scan_interval_seconds=args.interval,
        ),
        state_config=StateTrackerConfig(opportunity_max_points=opportunity_window_points),
        state_store=state_store,
        restore_market_state=False,
        restore_opportunity_state=False,
    )
    tg_token = getattr(args, "tg_token", None) or os.environ.get("TG_BOT_TOKEN", "")
    tg_chat_id = getattr(args, "tg_chat_id", None) or os.environ.get("TG_CHAT_ID", "")
    if tg_token and tg_chat_id:
        logger.info("Telegram 通知已启用 (chat_id=%s)", tg_chat_id)
    app = _DashboardApp(scanner=scanner, refresh_seconds=args.refresh,
                        scan_interval=args.interval,
                        tg_token=tg_token, tg_chat_id=tg_chat_id,
                        asset_blacklist_path=args.asset_blacklist_path)
    server = ThreadingHTTPServer((args.host, args.port), app.handler())
    destination = f"http://{args.host}:{args.port}"
    logger.info("面板启动: %s (刷新=%ss, 轮询=%ss, top=%d, 价差Z窗口=%d点≈4h, 归档保留=%.1fh)",
                destination, args.refresh, args.interval, args.top, opportunity_window_points, args.archive_retention_hours)
    if out is not None:
        out.write(f"Serving dashboard at {destination}\n")
        out.flush()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在关闭...")
    finally:
        scanner.close()
        for adapter in scanner.adapters:
            stop = getattr(adapter, "stop", None)
            if callable(stop):
                stop()
        state_store.close()
        server.server_close()
        logger.info("面板已关闭")
    return 0


class _TelegramNotifier:
    """Send Telegram messages via Bot API (fire-and-forget in background thread)."""

    def __init__(self, token: str, chat_id: str) -> None:
        self.token = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)

    def send(self, text: str) -> None:
        if not self.enabled:
            return
        threading.Thread(target=self._send, args=(text,), daemon=True).start()

    def _send(self, text: str) -> None:
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            data = json.dumps({"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}).encode()
            req = url_request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
            with url_request.urlopen(req, timeout=10) as resp:
                resp.read()
            logger.info("Telegram 通知已发送")
        except Exception:
            logger.warning("Telegram 通知发送失败:\n%s", traceback.format_exc())


class _DashboardApp:
    def __init__(self, scanner: RealtimeScanner, refresh_seconds: float,
                 scan_interval: float = 5.0,
                 tg_token: str = "", tg_chat_id: str = "",
                 asset_blacklist_path: str = "") -> None:
        self.scanner = scanner
        self.refresh_seconds = refresh_seconds
        self._scan_count = 0
        self._cached_payload: Optional[dict] = None
        self._lock = threading.Lock()
        self._asset_blacklist_path = asset_blacklist_path
        self._asset_blacklist_lock = threading.Lock()
        self._asset_blacklist: set[str] = set()
        self._load_asset_blacklist()
        # Per-adapter snapshot cache: adapter_name -> (snapshots, source_status)
        self._adapter_cache: Dict[str, tuple] = {}
        # Track when each (asset, venue_a, venue_b) key first met the TG alert condition.
        self._tradable_since: Dict[str, float] = {}
        # Keys that have already triggered an alert (reset when signal disappears)
        self._alerted: set = set()
        self._notifier = _TelegramNotifier(tg_token, tg_chat_id)
        # Watched positions: key -> {asset, venue_a, venue_b, entry_price_a, entry_price_b, entry_spread, target_spread, direction, bucket, ts}
        self._positions: Dict[str, dict] = {}
        self._position_lock = threading.Lock()
        # Completed positions pending frontend notification
        self._completed_positions: list = []
        # Structural spread blacklist: keys with historically stable large spreads.
        # Built from archive data; refreshed periodically.
        self._structural_blacklist: set = set()
        self._structural_blacklist_ts: float = 0.0
        self._practice_tracker = PracticeTracker()

        # Start background scan loop
        self._scan_interval = scan_interval
        self._bg_thread = None
        if self.scanner.adapters:
            self._bg_thread = threading.Thread(target=self._background_scan_loop, daemon=True)
            self._bg_thread.start()

    def _load_asset_blacklist(self) -> None:
        if not self._asset_blacklist_path:
            return
        try:
            with open(self._asset_blacklist_path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except FileNotFoundError:
            raw = []
        except Exception:
            logger.warning("读取币种黑名单失败: %s", self._asset_blacklist_path, exc_info=True)
            raw = []
        if isinstance(raw, dict):
            raw = raw.get("assets", [])
        assets = {_normalize_blacklist_asset(asset) for asset in raw}
        with self._asset_blacklist_lock:
            self._asset_blacklist = {asset for asset in assets if asset}

    def _save_asset_blacklist_locked(self) -> None:
        if not self._asset_blacklist_path:
            return
        directory = os.path.dirname(os.path.abspath(self._asset_blacklist_path))
        if directory:
            os.makedirs(directory, exist_ok=True)
        tmp_path = f"{self._asset_blacklist_path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump({"assets": sorted(self._asset_blacklist)}, fh, ensure_ascii=False, indent=2)
            fh.write("\n")
        os.replace(tmp_path, self._asset_blacklist_path)

    def asset_blacklist(self) -> list[str]:
        with self._asset_blacklist_lock:
            return sorted(self._asset_blacklist)

    def set_asset_blacklist(self, assets) -> list[str]:
        normalized = {_normalize_blacklist_asset(asset) for asset in assets}
        with self._asset_blacklist_lock:
            self._asset_blacklist = {asset for asset in normalized if asset}
            self._save_asset_blacklist_locked()
            return sorted(self._asset_blacklist)

    def _is_asset_blacklisted(self, asset) -> bool:
        normalized = _normalize_blacklist_asset(asset)
        if not normalized:
            return False
        with self._asset_blacklist_lock:
            return normalized in self._asset_blacklist

    def _asset_keys_for_opportunity(self, opportunity) -> set[str]:
        keys = {
            _normalize_blacklist_asset(getattr(opportunity, "asset", "")),
            _normalize_blacklist_asset(getattr(getattr(opportunity, "leg_a", None), "asset", "")),
            _normalize_blacklist_asset(getattr(getattr(opportunity, "leg_b", None), "asset", "")),
        }
        return {key for key in keys if key}

    def _is_opportunity_blacklisted(self, opportunity) -> bool:
        keys = self._asset_keys_for_opportunity(opportunity)
        if not keys:
            return False
        with self._asset_blacklist_lock:
            return bool(keys & self._asset_blacklist)

    def _payload_for_client(self) -> dict:
        payload = dict(self._cached_payload or {})
        blacklist = self.asset_blacklist()
        payload["asset_blacklist"] = blacklist
        if blacklist and "opportunities" in payload:
            blocked = set(blacklist)
            opportunities = [
                item
                for item in payload.get("opportunities", [])
                if not (
                    {
                        _normalize_blacklist_asset(item.get("asset")),
                        _normalize_blacklist_asset(item.get("venue_a_asset")),
                        _normalize_blacklist_asset(item.get("venue_b_asset")),
                    }
                    & blocked
                )
            ]
            payload["opportunities"] = opportunities
            payload["opportunity_count"] = len(opportunities)
        return payload

    def _rebuild_payload(self, scan_duration_ms: float = 0.0, timestamp=None) -> None:
        """Merge all adapter caches, score, and update the cached payload."""
        from datetime import datetime

        all_snapshots = []
        all_statuses = []
        with self._lock:
            for snapshots, status in self._adapter_cache.values():
                all_snapshots.extend(snapshots)
                all_statuses.append(status)

        enriched = self.scanner.market_state.enrich_snapshots(all_snapshots)
        opportunities = self.scanner.builder.build(enriched)
        opportunities = self.scanner.opportunity_state.enrich_opportunities(opportunities)
        scored = self.scanner.scorer.rank(opportunities)
        filtered = self.scanner._filter_scored(scored)
        top = filtered[:self.scanner.config.top_n]

        from .market_data import ScanBatch
        batch = ScanBatch(
            timestamp=timestamp or datetime.utcnow(),
            snapshots=enriched,
            opportunities=opportunities,
            scored_opportunities=top,
            source_statuses=all_statuses,
            scan_duration_ms=scan_duration_ms,
        )
        payload = build_batch_payload(batch)
        payload["practice"] = self._practice_tracker.update(scored)

        alerts = self.check_tradable_alerts(top)
        completed = self._check_positions(all_snapshots)
        payload["alert_sound"] = len(alerts) > 0 or len(completed) > 0
        payload["alert_items"] = [
            {"asset": s.opportunity.asset,
             "venues": f"{s.opportunity.leg_a.venue} ↔ {s.opportunity.leg_b.venue}",
             "score": s.breakdown.composite_score}
            for s in alerts
        ]
        with self._position_lock:
            payload["positions"] = list(self._positions.values())
            payload["completed_positions"] = self._completed_positions.copy()
            self._completed_positions.clear()
        self._cached_payload = payload

    def _update_source_status_payload(self) -> None:
        from datetime import datetime

        with self._lock:
            source_statuses = [
                {
                    "adapter_name": status.adapter_name,
                    "ok": status.ok,
                    "snapshot_count": status.snapshot_count,
                    "poll_duration_ms": status.poll_duration_ms,
                    "display_latency_ms": status.poll_duration_ms,
                    "max_snapshot_latency_ms": status.poll_duration_ms,
                    "error": status.error,
                    "timestamp": status.timestamp.isoformat() if status.timestamp else None,
                }
                for _, status in self._adapter_cache.values()
            ]
            snapshot_count = sum(len(snapshots) for snapshots, _ in self._adapter_cache.values())
            if self._cached_payload is None:
                self._cached_payload = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "snapshot_count": snapshot_count,
                    "opportunity_count": 0,
                    "scan_duration_ms": 0.0,
                    "source_statuses": source_statuses,
                    "dashboard_summary": {
                        "label_counts": {"tradable": 0, "watch": 0, "blocked": 0},
                        "bucket_counts": {"dislocation": 0, "carry": 0},
                        "top_tags": [],
                        "top_opportunity": None,
                        "market_regime": "idle",
                    },
                    "alert_sound": False,
                    "alert_items": [],
                    "practice": self._practice_tracker.update([]),
                    "opportunities": [],
                }
                return

            self._cached_payload["timestamp"] = datetime.utcnow().isoformat()
            self._cached_payload["snapshot_count"] = snapshot_count
            self._cached_payload["source_statuses"] = source_statuses

    def _background_scan_loop(self) -> None:
        """Poll all adapters in parallel; update cache as each completes."""
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from .market_data import SourceStatus
        from datetime import datetime

        while True:
            t0 = time.monotonic()
            try:
                with ThreadPoolExecutor(max_workers=len(self.scanner.adapters)) as executor:
                    futures = {}
                    for adapter in self.scanner.adapters:
                        name = getattr(adapter, "name", type(adapter).__name__)
                        futures[executor.submit(self._poll_one, adapter)] = name

                    completed_names = []
                    try:
                        for future in as_completed(futures, timeout=45.0):
                            name = futures[future]
                            try:
                                adapter_name, snapshots, error, duration_ms = future.result()
                            except Exception as exc:
                                adapter_name = name
                                snapshots = []
                                error = f"{type(exc).__name__}: {exc}"
                                duration_ms = 0.0

                            status = SourceStatus(
                                adapter_name=adapter_name,
                                ok=error is None,
                                snapshot_count=len(snapshots),
                                poll_duration_ms=round(duration_ms, 2),
                                error=error,
                                timestamp=datetime.utcnow(),
                            )
                            with self._lock:
                                self._adapter_cache[adapter_name] = (snapshots, status)
                            completed_names.append(adapter_name)

                            try:
                                self._update_source_status_payload()
                            except Exception:
                                logger.warning("source-status update error: %s", traceback.format_exc())

                            # Only source health is updated incrementally. Full
                            # payload rebuild mutates rolling indicator state,
                            # so it must run once per completed scan.

                    except TimeoutError:
                        for future, name in futures.items():
                            if not future.done():
                                logger.warning("适配器超时: %s", name)
                                future.cancel()

                self._scan_count += 1
                total_ms = (time.monotonic() - t0) * 1000.0

                src_parts = []
                with self._lock:
                    for name, (snaps, st) in self._adapter_cache.items():
                        tag = "OK" if st.ok else f"ERR({st.error})"
                        src_parts.append(f"{name}:{tag}:{st.snapshot_count}条/{st.poll_duration_ms:.0f}ms")

                logger.info(
                    "[scan#%d] 快照=%d 耗时=%.0fms | %s",
                    self._scan_count,
                    sum(len(s) for s, _ in self._adapter_cache.values()),
                    total_ms,
                    " | ".join(src_parts),
                )

                try:
                    self._rebuild_payload(scan_duration_ms=round(total_ms, 2), timestamp=datetime.utcnow())
                except Exception:
                    logger.warning("rebuild error: %s", traceback.format_exc())

                # Mirror RealtimeScanner persistence for offline history analysis
                # while the dashboard loop is running.
                self.scanner._scan_count += 1
                if (
                    self.scanner._state_store is not None
                    and self.scanner._scan_count % self.scanner._flush_interval == 0
                ):
                    with self._lock:
                        statuses = [status for _, status in self._adapter_cache.values()]
                    include_rolling_state = self.scanner._scan_count % self.scanner._rolling_flush_interval == 0
                    self.scanner.request_state_flush(
                        statuses,
                        datetime.utcnow(),
                        include_opportunity_state=include_rolling_state,
                        include_market_state=include_rolling_state,
                    )

            except Exception:
                logger.error("后台扫描异常:\n%s", traceback.format_exc())

            elapsed = time.monotonic() - t0
            remaining = max(self._scan_interval - elapsed, 0.5)
            time.sleep(remaining)

    @staticmethod
    def _poll_one(adapter):
        name = getattr(adapter, "name", type(adapter).__name__)
        start = time.monotonic()
        try:
            snapshots = list(adapter.poll())
            duration_ms = (time.monotonic() - start) * 1000.0
            return name, snapshots, None, duration_ms
        except Exception as exc:
            duration_ms = (time.monotonic() - start) * 1000.0
            return name, [], f"{type(exc).__name__}: {exc}", duration_ms

    def _refresh_structural_blacklist(self) -> None:
        """Identify pairs with structurally stable large spreads from archive data."""
        now = time.time()
        if now - self._structural_blacklist_ts < 300:  # refresh every 5 min
            return
        self._structural_blacklist_ts = now
        store = getattr(self.scanner, "_state_store", None)
        if store is None:
            return
        try:
            import sqlite3
            rows = store._conn.execute("""
                SELECT asset, venue_a, venue_b,
                    COUNT(*) as n,
                    AVG(executable_spread_bps) as avg_sp,
                    MIN(executable_spread_bps) as min_sp,
                    MAX(executable_spread_bps) as max_sp
                FROM opportunity_history_archive
                GROUP BY asset, venue_a, venue_b
                HAVING n >= 10
                    AND avg_sp > 20
                    AND (max_sp - min_sp) < avg_sp * 0.5
            """).fetchall()
            blacklist = set()
            for row in rows:
                asset, va, vb = row[0], row[1], row[2]
                # Add both directions
                blacklist.add(f"{asset}|{va}|{vb}")
                blacklist.add(f"{asset}|{vb}|{va}")
            self._structural_blacklist = blacklist
        except Exception:
            pass

    def check_tradable_alerts(self, scored_opportunities) -> list:
        """Return opportunities whose spread-vs-4H-mean signal persisted long enough."""
        self._refresh_structural_blacklist()
        now = time.monotonic()
        current_tradable_keys: set = set()
        newly_alerting = []

        for scored in scored_opportunities:
            opp = scored.opportunity
            key = f"{opp.asset}|{opp.leg_a.venue}|{opp.leg_b.venue}"

            if self._is_opportunity_blacklisted(opp):
                continue

            # Skip structurally stable large spreads (won't revert)
            if key in self._structural_blacklist:
                continue
            if _has_lighter_ticker_only_leg(opp):
                continue

            spread_vs_mean_pct = _spread_vs_mean_pct(opp)
            if spread_vs_mean_pct <= TG_ALERT_MIN_SPREAD_VS_MEAN_PCT:
                continue
            if float(opp.spread_zscore or 0.0) < TG_ALERT_MIN_SPREAD_ZSCORE:
                continue
            min_oi_usd = min(float(opp.leg_a.oi_usd or 0.0), float(opp.leg_b.oi_usd or 0.0))
            if min_oi_usd < TG_ALERT_MIN_LEG_OI_USD:
                continue

            current_tradable_keys.add(key)

            if key not in self._tradable_since:
                self._tradable_since[key] = now

            elapsed = now - self._tradable_since[key]
            if elapsed >= TG_ALERT_SECONDS and key not in self._alerted:
                self._alerted.add(key)
                newly_alerting.append(scored)

        # Clean up keys that are no longer tradable
        stale = [k for k in self._tradable_since if k not in current_tradable_keys]
        for k in stale:
            del self._tradable_since[k]
            self._alerted.discard(k)

        # Send Telegram notifications
        for scored in newly_alerting:
            opp = scored.opportunity
            br = scored.breakdown
            spread_vs_mean_pct = _spread_vs_mean_pct(opp)
            min_oi_usd = min(float(opp.leg_a.oi_usd or 0.0), float(opp.leg_b.oi_usd or 0.0))
            msg = (
                f"🔔 <b>价差异常信号 (持续≥60s)</b>\n"
                f"<b>{opp.asset}/{opp.quote}</b> · {opp.leg_a.venue} ↔ {opp.leg_b.venue}\n"
                f"价差-4H均值: {spread_vs_mean_pct:+.3f}% | 4H价差Z: {opp.spread_zscore:+.2f}\n"
                f"最小未平仓额: ${min_oi_usd/1_000_000:.2f}M\n"
                f"阈值: >{TG_ALERT_MIN_SPREAD_VS_MEAN_PCT:.1f}% 且 Z≥{TG_ALERT_MIN_SPREAD_ZSCORE:.2f} 且 OI≥$1M\n"
                f"收益: {br.expected_profit_bps:+.2f} bps (${br.expected_profit_usd:+.2f})\n"
                f"类型: {br.bucket_type.value}"
            )
            self._notifier.send(msg)
            logger.info("触发价差异常警报: %s %s↔%s spread_vs_mean=%.3f%% z=%.2f (持续%.0fs)",
                        opp.asset, opp.leg_a.venue, opp.leg_b.venue,
                        spread_vs_mean_pct,
                        opp.spread_zscore,
                        now - self._tradable_since.get(f"{opp.asset}|{opp.leg_a.venue}|{opp.leg_b.venue}", now))

        return newly_alerting

    def add_position(self, pos: dict) -> dict:
        """Add a watched position. Returns the stored position."""
        key = f"{pos['asset']}|{pos['venue_a']}|{pos['venue_b']}"
        entry_a = float(pos.get("entry_price_a", 0))
        entry_b = float(pos.get("entry_price_b", 0))
        mid = (entry_a + entry_b) / 2.0 if (entry_a + entry_b) > 0 else 1.0
        entry_spread_bps = (entry_a - entry_b) / mid * 10000.0
        record = {
            "key": key,
            "asset": pos["asset"],
            "quote": pos.get("quote", "USD"),
            "venue_a": pos["venue_a"],
            "venue_b": pos["venue_b"],
            "direction": pos.get("direction", ""),
            "bucket": pos.get("bucket", "dislocation"),
            "entry_price_a": entry_a,
            "entry_price_b": entry_b,
            "entry_spread_bps": round(entry_spread_bps, 2),
            "status": "watching",
            "ts": time.time(),
            "current_price_a": entry_a,
            "current_price_b": entry_b,
            "current_spread_bps": round(entry_spread_bps, 2),
            "pnl_bps": 0.0,
        }
        with self._position_lock:
            self._positions[key] = record
        logger.info("新增监控仓位: %s %s↔%s entry_spread=%.2fbps",
                    pos["asset"], pos["venue_a"], pos["venue_b"], entry_spread_bps)
        return record

    def remove_position(self, key: str) -> bool:
        with self._position_lock:
            return self._positions.pop(key, None) is not None

    def _check_positions(self, snapshots) -> list:
        """Check watched positions against current prices. Return completed ones."""
        if not self._positions:
            return []
        # Build price lookup: (venue, canonical_asset) -> mark_price
        # Use the same asset alias mapping as OpportunityBuilder so that e.g.
        # XAUT on bybit is findable under the canonical name "PAXG".
        from .market_data import OpportunityBuilder
        price_map = {}
        for snap in snapshots:
            canonical = OpportunityBuilder.ASSET_ALIASES.get(snap.asset.upper(), snap.asset.upper())
            price_map[(snap.venue, canonical)] = snap.mark_price

        completed = []
        with self._position_lock:
            for key, pos in list(self._positions.items()):
                pa = price_map.get((pos["venue_a"], pos["asset"]))
                pb = price_map.get((pos["venue_b"], pos["asset"]))
                if pa is None or pb is None:
                    continue
                pos["current_price_a"] = pa
                pos["current_price_b"] = pb
                mid = (pa + pb) / 2.0 if (pa + pb) > 0 else 1.0
                current_spread_bps = (pa - pb) / mid * 10000.0
                pos["current_spread_bps"] = round(current_spread_bps, 2)
                # PnL = entry spread converged toward zero
                # If we entered long_a/short_b: profit when spread narrows
                pos["pnl_bps"] = round(pos["entry_spread_bps"] - current_spread_bps, 2)

                # Completion: spread crossed zero AND position alive > 30s
                alive = time.time() - pos["ts"]
                if alive < 30:
                    continue
                entry_s = pos["entry_spread_bps"]
                if entry_s > 0 and current_spread_bps <= 0:
                    pos["status"] = "completed"
                    completed.append(pos.copy())
                    del self._positions[key]
                elif entry_s < 0 and current_spread_bps >= 0:
                    pos["status"] = "completed"
                    completed.append(pos.copy())
                    del self._positions[key]

        # Notify on completions
        for pos in completed:
            if self._is_asset_blacklisted(pos.get("asset")):
                logger.info("跳过已拉黑币种仓位完成通知: %s", pos.get("asset"))
                continue
            self._completed_positions.append(pos)
            msg = (
                f"✅ <b>套利完成</b>\n"
                f"<b>{pos['asset']}/{pos['quote']}</b> · {pos['venue_a']} ↔ {pos['venue_b']}\n"
                f"入场价差: {pos['entry_spread_bps']:+.2f} bps\n"
                f"当前价差: {pos['current_spread_bps']:+.2f} bps\n"
                f"盈利: {pos['pnl_bps']:+.2f} bps"
            )
            self._notifier.send(msg)
            logger.info("仓位套利完成: %s PnL=%.2fbps", pos["key"], pos["pnl_bps"])

        return completed

    def handler(self):
        app = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                if parsed.path == "/":
                    self._respond_html(build_dashboard_html(app.refresh_seconds))
                    return
                if parsed.path == "/api/dashboard":
                    if app._cached_payload is not None:
                        self._respond_json(app._payload_for_client())
                    else:
                        self._respond_json({"error": "scan pending", "snapshot_count": 0,
                                            "opportunity_count": 0, "opportunities": [],
                                            "source_statuses": [], "scan_duration_ms": 0,
                                            "dashboard_summary": {"label_counts": {"tradable": 0, "watch": 0, "blocked": 0},
                                                                   "bucket_counts": {"dislocation": 0, "carry": 0},
                                                                   "top_tags": [], "top_opportunity": None,
                                                                   "market_regime": "idle"},
                                            "alert_sound": False, "alert_items": [],
                                            "practice": app._practice_tracker.update([]),
                                            "asset_blacklist": app.asset_blacklist()})
                    return
                if parsed.path == "/api/asset-blacklist":
                    self._respond_json({"assets": app.asset_blacklist()})
                    return
                if parsed.path == "/health":
                    self._respond_json({"ok": True})
                    return
                logger.warning("404: %s", self.path)
                self.send_error(404, "Not found")

            def do_POST(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                try:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length)) if length > 0 else {}
                except Exception:
                    self.send_error(400, "Bad JSON")
                    return
                if parsed.path == "/api/position":
                    action = body.get("action", "add")
                    if action == "add":
                        pos = app.add_position(body)
                        self._respond_json({"ok": True, "position": pos})
                    elif action == "remove":
                        key = body.get("key", "")
                        removed = app.remove_position(key)
                        self._respond_json({"ok": removed})
                    else:
                        self._respond_json({"ok": False, "error": "unknown action"})
                    return
                if parsed.path == "/api/asset-blacklist":
                    assets = body.get("assets", [])
                    if not isinstance(assets, list):
                        self.send_error(400, "assets must be a list")
                        return
                    self._respond_json({"ok": True, "assets": app.set_asset_blacklist(assets)})
                    return
                self.send_error(404, "Not found")

            def log_message(self, format: str, *args) -> None:  # noqa: A003
                return

            def _respond_html(self, body: str) -> None:
                payload = body.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def _respond_json(self, payload_obj: dict) -> None:
                payload = json.dumps(payload_obj).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        return Handler


def _build_legacy_dashboard_html(refresh_seconds: float) -> str:
    refresh_ms = max(int(refresh_seconds * 1000), 500)
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PERP ARB · 合约套利终端</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&family=Inter:wght@300;400;500;600;700&display=swap');
:root{{
  --bg:#0a0e17;--bg2:#0f1420;--bg3:#141926;
  --border:rgba(56,189,248,0.08);--border-h:rgba(56,189,248,0.2);
  --text:#e2e8f0;--dim:#64748b;--dim2:#475569;
  --cyan:#38bdf8;--green:#34d399;--amber:#fbbf24;--red:#f87171;--violet:#a78bfa;
  --glow-c:rgba(56,189,248,0.15);--glow-g:rgba(52,211,153,0.12);
}}
*{{box-sizing:border-box;margin:0;padding:0}}
html{{background:var(--bg);color:var(--text)}}
body{{font-family:'Inter',system-ui,sans-serif;font-size:13px;line-height:1.5;min-height:100vh;overflow-x:hidden}}
body::before{{content:"";position:fixed;inset:0;background:
  radial-gradient(ellipse 80% 60% at 10% 0%,var(--glow-c),transparent),
  radial-gradient(ellipse 60% 50% at 90% 10%,rgba(167,139,250,0.06),transparent),
  radial-gradient(ellipse 70% 40% at 50% 100%,var(--glow-g),transparent);
  pointer-events:none;z-index:0}}
.mono{{font-family:'JetBrains Mono',monospace}}

/* ── top bar ── */
.topbar{{position:sticky;top:0;z-index:100;display:flex;align-items:center;justify-content:space-between;
  padding:0 24px;height:44px;background:rgba(10,14,23,0.88);backdrop-filter:blur(20px)saturate(1.4);
  border-bottom:1px solid var(--border)}}
.topbar-left{{display:flex;align-items:center;gap:16px}}
.logo{{font-family:'JetBrains Mono',monospace;font-weight:700;font-size:14px;color:var(--cyan);letter-spacing:0.06em}}
.logo span{{color:var(--dim)}}
.scan-dot{{width:7px;height:7px;border-radius:50%;background:var(--green);box-shadow:0 0 8px var(--green);animation:pulse 2s infinite}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
.topbar-right{{display:flex;align-items:center;gap:20px;color:var(--dim);font-size:12px}}
.topbar-right .mono{{color:var(--text)}}

/* ── layout ── */
.wrap{{position:relative;z-index:1;max-width:1440px;margin:0 auto;padding:16px 20px 40px}}

/* ── source strip ── */
.src-strip{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px}}
.src-chip{{display:flex;align-items:center;gap:6px;padding:5px 12px;border-radius:6px;font-size:11px;
  background:var(--bg2);border:1px solid var(--border);font-family:'JetBrains Mono',monospace;letter-spacing:0.02em;
  transition:border-color .2s}}
.src-chip:hover{{border-color:var(--border-h)}}
.src-dot{{width:5px;height:5px;border-radius:50%;flex-shrink:0}}

/* ── stat grid ── */
.stats{{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:16px}}
.stat{{padding:14px 16px;border-radius:10px;background:var(--bg2);border:1px solid var(--border);
  transition:border-color .25s,box-shadow .25s}}
.stat:hover{{border-color:var(--border-h);box-shadow:0 0 20px var(--glow-c)}}
.stat-label{{font-size:10px;text-transform:uppercase;letter-spacing:0.12em;color:var(--dim);margin-bottom:6px}}
.stat-val{{font-family:'JetBrains Mono',monospace;font-size:22px;font-weight:600;letter-spacing:-0.02em}}
.stat-sub{{font-size:11px;color:var(--dim);margin-top:4px}}

/* ── panels row ── */
.panels{{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px}}
.pnl{{padding:16px 18px;border-radius:10px;background:var(--bg2);border:1px solid var(--border)}}
.pnl-title{{font-size:10px;text-transform:uppercase;letter-spacing:0.12em;color:var(--dim);margin-bottom:10px;
  display:flex;align-items:center;gap:6px}}
.pnl-title::before{{content:"";width:3px;height:12px;border-radius:2px;background:var(--cyan)}}
.tag-list{{display:flex;flex-wrap:wrap;gap:6px}}
.tag{{padding:4px 10px;border-radius:4px;font-size:11px;background:rgba(56,189,248,0.06);
  border:1px solid rgba(56,189,248,0.1);color:var(--cyan);font-family:'JetBrains Mono',monospace}}
.tag .cnt{{color:var(--dim);margin-left:4px}}
.sort-list{{display:flex;flex-direction:column;gap:6px;font-size:12px;color:var(--dim2)}}
.sort-list span{{color:var(--text);font-weight:500}}

/* ── table ── */
.tbl-wrap{{border-radius:10px;background:var(--bg2);border:1px solid var(--border)}}
.tbl{{width:100%;border-collapse:collapse;table-layout:fixed}}
.tbl th{{background:var(--bg3);padding:10px 12px;font-size:10px;
  text-transform:uppercase;letter-spacing:0.1em;color:var(--dim);text-align:left;font-weight:500;
  border-bottom:1px solid var(--border);white-space:nowrap}}
.tbl td{{padding:10px 12px;border-bottom:1px solid rgba(255,255,255,0.03);font-size:12px;
  vertical-align:middle;white-space:nowrap}}
.tbl tr{{transition:background .15s}}
.tbl tbody tr:hover{{background:rgba(56,189,248,0.04)}}
.tbl .num{{font-family:'JetBrains Mono',monospace;text-align:right}}

/* row expand */
.row-detail{{display:none}}
.row-expand+.row-detail{{display:table-row}}
.detail-cell{{padding:12px 16px 16px;background:rgba(15,20,32,0.6);border-bottom:1px solid var(--border)}}
.detail-grid{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px}}
.detail-section{{font-size:11px;color:var(--dim)}}
.detail-section strong{{display:block;color:var(--text);font-weight:500;margin-bottom:4px}}
.detail-section div{{margin-top:3px}}

/* labels */
.lbl{{display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;letter-spacing:0.02em}}
.lbl-tradable{{color:var(--green);background:rgba(52,211,153,0.1);border:1px solid rgba(52,211,153,0.2)}}
.lbl-watch{{color:var(--amber);background:rgba(251,191,36,0.1);border:1px solid rgba(251,191,36,0.2)}}
.lbl-blocked{{color:var(--red);background:rgba(248,113,113,0.08);border:1px solid rgba(248,113,113,0.15)}}

/* bucket */
.bkt{{font-size:11px;padding:2px 7px;border-radius:3px;font-weight:500}}
.bkt-d{{color:var(--cyan);background:rgba(56,189,248,0.08)}}
.bkt-c{{color:var(--violet);background:rgba(167,139,250,0.08)}}

/* score bar */
.sbar{{position:relative;height:4px;width:100%;min-width:60px;border-radius:2px;background:rgba(255,255,255,0.05)}}
.sbar>span{{position:absolute;left:0;top:0;height:100%;border-radius:2px;
  background:linear-gradient(90deg,var(--cyan),var(--green));transition:width .6s ease}}

/* row highlight for tradable / watch */
.tbl tbody tr.row-tradable>td{{background:rgba(52,211,153,0.05)}}
.tbl tbody tr.row-tradable>td:first-child{{border-left:2px solid var(--green)}}
.tbl tbody tr.row-watch>td{{background:rgba(251,191,36,0.04)}}
.tbl tbody tr.row-watch>td:first-child{{border-left:2px solid var(--amber)}}

/* position entry panel */
.pos-panel{{margin-top:10px;padding:10px 12px;background:rgba(56,189,248,0.04);border:1px solid var(--border-h);border-radius:6px}}
.pos-panel label{{font-size:11px;color:var(--dim);display:block;margin-bottom:2px}}
.pos-panel input{{background:var(--bg);border:1px solid var(--border-h);color:var(--text);padding:4px 8px;border-radius:4px;font-family:'JetBrains Mono',monospace;font-size:12px;width:140px}}
.pos-panel input:focus{{outline:none;border-color:var(--cyan)}}
.pos-row{{display:flex;align-items:center;gap:12px;flex-wrap:wrap}}
.pos-btn{{padding:5px 16px;border:none;border-radius:4px;font-size:11px;font-weight:600;cursor:pointer;transition:all .15s}}
.pos-btn-go{{background:var(--green);color:#0a0e17}}.pos-btn-go:hover{{filter:brightness(1.15)}}
.pos-btn-rm{{background:rgba(248,113,113,0.15);color:var(--red);border:1px solid rgba(248,113,113,0.2)}}.pos-btn-rm:hover{{background:rgba(248,113,113,0.25)}}
.pos-active{{background:rgba(52,211,153,0.06)!important;border-left:2px solid var(--green)!important}}

/* completion flash */
@keyframes completionFlash{{
  0%{{background:rgba(52,211,153,0.3)}}
  50%{{background:rgba(52,211,153,0.08)}}
  100%{{background:rgba(52,211,153,0.3)}}
}}
.pos-complete-flash{{animation:completionFlash 0.5s ease 4}}

/* position strip */
.pos-strip-inner{{display:flex;flex-direction:column;gap:6px}}
.pos-chip{{display:flex;align-items:center;gap:8px;padding:6px 10px;background:rgba(56,189,248,0.04);border:1px solid var(--border);border-radius:6px;font-size:12px;line-height:1.4}}
.pos-chip .pnl-pos{{color:var(--green);font-weight:600}}.pos-chip .pnl-neg{{color:var(--red);font-weight:600}}
.pos-chip .pos-prices{{color:var(--dim);font-size:10px}}

/* empty */
.empty-msg{{padding:48px 24px;text-align:center;color:var(--dim)}}

/* regime dot */
.regime{{display:inline-flex;align-items:center;gap:5px}}
.regime-dot{{width:6px;height:6px;border-radius:50%}}

/* mobile */
@media(max-width:1024px){{
  .stats{{grid-template-columns:repeat(3,1fr)}}
  .panels{{grid-template-columns:1fr}}
  .tbl-wrap{{overflow-x:auto}}
}}
@media(max-width:640px){{
  .stats{{grid-template-columns:repeat(2,1fr)}}
  .wrap{{padding:12px 10px 30px}}
  .topbar{{padding:0 12px}}
}}
</style>
</head>
<body>

<!-- top bar -->
<nav class="topbar">
  <div class="topbar-left">
    <div class="logo">PERP<span>/</span>ARB</div>
    <div class="scan-dot" id="scan-dot"></div>
    <span style="color:var(--dim);font-size:11px" id="status-text">启动中...</span>
  </div>
  <div class="topbar-right">
    <span id="clock" class="mono">--:--:--</span>
    <span>刷新 {refresh_seconds:.0f}s</span>
  </div>
</nav>

<div class="wrap">

  <!-- source health strip -->
  <div class="src-strip" id="src-strip"></div>

  <!-- stat cards -->
  <div class="stats" id="stat-grid">
    <div class="stat"><div class="stat-label">市场状态</div><div class="stat-val" id="regime">--</div></div>
    <div class="stat"><div class="stat-label">可交易</div><div class="stat-val" id="s-tradable">0</div></div>
    <div class="stat"><div class="stat-label">观察</div><div class="stat-val" id="s-watch">0</div></div>
    <div class="stat"><div class="stat-label">错位回归</div><div class="stat-val" id="s-dis">0</div></div>
    <div class="stat"><div class="stat-label">资金费 Carry</div><div class="stat-val" id="s-carry">0</div></div>
    <div class="stat"><div class="stat-label">头号机会</div><div class="stat-val" id="s-top" style="font-size:14px">--</div></div>
  </div>

  <!-- risk tags + sort logic -->
  <div class="panels">
    <div class="pnl">
      <div class="pnl-title">高频风险标签</div>
      <div class="tag-list" id="tag-list"><span class="tag">加载中</span></div>
    </div>
    <div class="pnl" id="pos-panel-wrap">
      <div class="pnl-title">持仓监控</div>
      <div id="pos-strip" class="pos-strip-inner"><span style="color:var(--dim);font-size:12px">暂无持仓</span></div>
    </div>
  </div>

  <div class="panels" id="practice-panel">
    <div class="pnl">
      <div class="pnl-title">实战演练合格线</div>
      <div id="practice-summary" class="sort-list"><span>等待扫描...</span></div>
    </div>
    <div class="pnl">
      <div class="pnl-title">纸面交易账本</div>
      <div id="paper-ledger" class="pos-strip-inner"><span style="color:var(--dim);font-size:12px">暂无纸面交易</span></div>
    </div>
  </div>

  <!-- search bar -->
  <div style="display:flex;align-items:center;gap:10px;margin:10px 0 6px">
    <input id="search-input" type="text" placeholder="搜索币对 / 交易所 (如 BTC, bybit, ETH paradex ...)"
      style="flex:1;max-width:420px;padding:6px 12px;border-radius:6px;border:1px solid var(--dim2);background:var(--bg2);color:var(--text);font-size:13px;outline:none"
      oninput="applySearch()" />
    <span id="search-count" style="color:var(--dim);font-size:12px"></span>
  </div>

  <!-- opportunity table -->
  <div class="tbl-wrap">
    <table class="tbl">
      <thead>
        <tr>
          <th style="width:36px">#</th>
          <th style="width:90px">标的</th>
          <th style="width:180px">场所</th>
          <th style="width:72px">标签</th>
          <th style="width:80px">类型</th>
          <th style="width:210px">详情</th>
          <th style="width:180px">方向</th>
          <th style="width:120px">评分</th>
          <th style="width:110px" class="num">预估盈利</th>
          <th style="width:72px" class="num">ROI</th>
          <th style="width:68px" class="num">Z-Score</th>
          <th style="width:72px">信号</th>
        </tr>
      </thead>
      <tbody id="tbl-body">
        <tr><td colspan="12" class="empty-msg">正在连接数据源...</td></tr>
      </tbody>
    </table>
  </div>

</div>

<script>
const REFRESH_MS={refresh_ms};
const $=id=>document.getElementById(id);
const esc=v=>String(v??"").replace(/[&<>"]/g,c=>({{
  "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}})[c]);

const LBL={{tradable:"可交易",watch:"观察",blocked:"拦截"}};
const LBL_CLS={{tradable:"lbl-tradable",watch:"lbl-watch",blocked:"lbl-blocked"}};
const BKT={{dislocation:"错位回归",carry:"Carry"}};
const BKT_CLS={{dislocation:"bkt-d",carry:"bkt-c"}};
function dirLabel(d,va,vb){{if(d==="long_a_short_b")return"L "+va+" / S "+vb;if(d==="long_b_short_a")return"L "+vb+" / S "+va;return d||""}}

function feeTag(role,fee){{const c=role==="maker"?"var(--green)":"var(--amber)";const l=role==="maker"?"M":"T";return `<span style="color:`+c+`;font-size:10px;font-weight:600">`+l+`</span><span style="color:var(--dim);font-size:10px"> `+fee.toFixed(1)+`</span>`;}}
function detailCol(item){{
  const ra=item.fee_role_a||"";const rb=item.fee_role_b||"";
  const fa=item.fee_a_bps||0;const fb=item.fee_b_bps||0;const ft=item.fee_total_bps||0;
  const feeLine=`<span style="color:var(--dim);font-size:10px">`+esc(item.venue_a)+`</span>`+feeTag(ra,fa)+` <span style="color:var(--dim);font-size:10px">+ `+esc(item.venue_b)+`</span>`+feeTag(rb,fb)+` <span style="color:var(--dim);font-size:10px">= `+ft.toFixed(1)+`bps</span>`;
  if(item.bucket==="dislocation"){{
    const pxDigits=(item.price_a||0)>=1?2:(item.price_a||0)>=0.01?4:6;
    const pa=(item.price_a||0).toFixed(pxDigits);
    const pb=(item.price_b||0).toFixed(pxDigits);
    const edge=item.entry_edge_bps||0;
    const mean=item.spread_mean_bps||0;
    const zs=item.spread_zscore||0;
    const excess=mean>0?Math.max(0,edge-mean):edge;
    const absZ=Math.abs(zs);
    const revFrac=mean>0?(absZ>=1?Math.min(Math.max(0,1-1/absZ),0.75):0):1.0;
    const revertable=excess*revFrac;
    const clr=revertable>3?"var(--green)":revertable>0?"var(--text)":"var(--dim)";
    const meanLbl=mean>0?` <span style="color:var(--dim);font-size:10px">(均值 `+mean.toFixed(2)+`, z=`+zs.toFixed(1)+`)</span>`:"";
    return `<span style="color:var(--dim);font-size:10px">`+esc(item.venue_a)+` `+pa+` / `+esc(item.venue_b)+` `+pb+`</span><br><span style="color:`+clr+`">可执行 `+edge.toFixed(2)+` bps → 回归 `+revertable.toFixed(2)+` bps</span>`+meanLbl+`<br>`+feeLine;
  }}
  if(item.bucket==="carry"){{
    const fa2=(item.funding_a_bps||0);
    const fb2=(item.funding_b_bps||0);
    const ihA=item.funding_a_interval_h||8;
    const ihB=item.funding_b_interval_h||8;
    const dailyA=fa2*(24/ihA);const dailyB=fb2*(24/ihB);
    const dailyDiff=Math.abs(dailyA-dailyB);
    const annual=(dailyDiff*365/100).toFixed(2);
    const lblA=ihA===8?"8h":ihA===4?"4h":ihA===1?"1h":ihA+"h";
    const lblB=ihB===8?"8h":ihB===4?"4h":ihB===1?"1h":ihB+"h";
    const clr=parseFloat(annual)>10?"var(--green)":"var(--text)";
    return `<span style="color:var(--dim)">`+esc(item.venue_a)+` `+fa2.toFixed(3)+`/`+lblA+` / `+esc(item.venue_b)+` `+fb2.toFixed(3)+`/`+lblB+`</span><br><span style="color:`+clr+`">年化差 `+annual+`%</span><br>`+feeLine;
  }}
  return feeLine;
}}
function finiteNum(value, fallback=0){{
  const num=Number(value);
  return Number.isFinite(num)?num:fallback;
}}
function formatUsd(value){{
  const amount=finiteNum(value,0);
  const sign=amount>0?"+":amount<0?"-":"";
  const abs=Math.abs(amount);
  const digits=abs>=1?2:4;
  return sign+"$"+abs.toFixed(digits);
}}
function estimationNotional(item){{
  const ep=item.execution_plan||{{}};
  return finiteNum(item.notional_usd ?? ep.max_notional_usd ?? ep.target_notional_usd,0);
}}
function profitDetail(item){{
  const ep=item.execution_plan||{{}};
  const notional=estimationNotional(item);
  const targetNotional=finiteNum(ep.target_notional_usd,0);
  const profitBps=finiteNum(item.expected_profit_bps,0);
  const profitUsd=Number.isFinite(Number(item.expected_profit_usd))?Number(item.expected_profit_usd):notional*profitBps/10000;
  const entryEdge=finiteNum(item.entry_edge_bps,0);
  const carryEdge=finiteNum(item.carry_edge_bps,0);
  const feeTot=finiteNum(item.fee_total_bps,0);
  const clr=profitUsd>0?"var(--green)":profitUsd<0?"var(--red)":"var(--text)";
  let lines=[];
  lines.push(`<div style="display:flex;justify-content:space-between"><span>估算名义金额</span><span class="mono">$${{notional.toLocaleString(undefined,{{maximumFractionDigits:0}})}}</span></div>`);
  if(targetNotional>0 && Math.abs(targetNotional-notional)>0.01){{
    lines.push(`<div style="display:flex;justify-content:space-between"><span>执行目标</span><span class="mono">$${{targetNotional.toLocaleString(undefined,{{maximumFractionDigits:0}})}}</span></div>`);
  }}
  if(item.bucket==="carry"){{
    const fa=finiteNum(item.funding_a_bps,0);const fb=finiteNum(item.funding_b_bps,0);
    const ihA=finiteNum(item.funding_a_interval_h,8)||8;const ihB=finiteNum(item.funding_b_interval_h,8)||8;
    const dailyA=fa*(24/ihA);const dailyB=fb*(24/ihB);
    const dailyDiff=Math.abs(dailyA-dailyB);
    const dailyUsd=notional*dailyDiff/10000;
    const annualPct=dailyDiff*365/100;
    const annualUsd=dailyUsd*365;
    const lblA=ihA===8?"8h":ihA===4?"4h":ihA===1?"1h":ihA+"h";
    const lblB=ihB===8?"8h":ihB===4?"4h":ihB===1?"1h":ihB+"h";
    lines.push(`<div style="display:flex;justify-content:space-between"><span>资金费 `+esc(item.venue_a)+`</span><span class="mono">${{fa.toFixed(3)}} bps/`+lblA+`</span></div>`);
    lines.push(`<div style="display:flex;justify-content:space-between"><span>资金费 `+esc(item.venue_b)+`</span><span class="mono">${{fb.toFixed(3)}} bps/`+lblB+`</span></div>`);
    lines.push(`<div style="display:flex;justify-content:space-between"><span>日收益</span><span class="mono" style="color:var(--green)">${{dailyDiff.toFixed(3)}} bps · $${{dailyUsd.toFixed(2)}}</span></div>`);
    lines.push(`<div style="display:flex;justify-content:space-between"><span>年化收益</span><span class="mono" style="color:var(--green)">${{annualPct.toFixed(2)}}% · $${{annualUsd>=1000?(annualUsd/1000).toFixed(1)+"k":annualUsd.toFixed(0)}}</span></div>`);
    lines.push(`<div style="display:flex;justify-content:space-between"><span>Carry 边际</span><span class="mono">${{carryEdge.toFixed(2)}} bps</span></div>`);
  }}else{{
    const meanSpread=item.spread_mean_bps||0;
    const zs=Math.abs(item.spread_zscore||0);
    const excess=meanSpread>0?Math.max(0,entryEdge-meanSpread):entryEdge;
    const revFrac=meanSpread>0?(zs>=1?Math.min(Math.max(0,1-1/zs),0.75):0):1.0;
    const revertable=excess*revFrac;
    lines.push(`<div style="display:flex;justify-content:space-between"><span>入场价差</span><span class="mono">${{entryEdge.toFixed(2)}} bps</span></div>`);
    if(meanSpread>0){{
      lines.push(`<div style="display:flex;justify-content:space-between"><span>均值价差</span><span class="mono" style="color:var(--dim)">${{meanSpread.toFixed(2)}} bps</span></div>`);
      lines.push(`<div style="display:flex;justify-content:space-between"><span>超额部分</span><span class="mono">${{excess.toFixed(2)}} bps</span></div>`);
      lines.push(`<div style="display:flex;justify-content:space-between"><span>回归系数 (z=${{zs.toFixed(1)}})</span><span class="mono">${{(revFrac*100).toFixed(0)}}%</span></div>`);
      lines.push(`<div style="display:flex;justify-content:space-between"><span>可获利部分</span><span class="mono" style="color:${{revertable>0?'var(--green)':'var(--dim)'}}">${{revertable.toFixed(2)}} bps</span></div>`);
    }}
  }}
  lines.push(`<div style="display:flex;justify-content:space-between"><span>手续费合计</span><span class="mono" style="color:var(--red)">-${{feeTot.toFixed(1)}} bps</span></div>`);
  lines.push(`<div style="display:flex;justify-content:space-between;border-top:1px solid var(--dim2);padding-top:4px;margin-top:4px"><span><strong>净收益</strong></span><span class="mono" style="color:${{clr}};font-weight:600">${{profitBps>=0?"+":""}}`+profitBps.toFixed(2)+` bps · ${{formatUsd(profitUsd)}}</span></div>`);
  return `<div style="font-size:11px;line-height:1.8;font-family:'JetBrains Mono',monospace">`+lines.join("")+`</div>`;
}}
const REG={{stable:["稳定","var(--green)"],microstructure_noisy:["噪声偏高","var(--amber)"],
  shock_risk:["冲击风险","var(--red)"],idle:["待机","var(--dim)"]}};

function renderSources(list){{
  if(!list||!list.length){{$("src-strip").innerHTML="";return}}
  $("src-strip").innerHTML=list.map(s=>{{
    const c=s.ok?"var(--green)":"var(--red)";
    const rawMs=s.display_latency_ms!=null?s.display_latency_ms:s.poll_duration_ms;
    const ms=rawMs!=null?rawMs.toFixed(0):"?";
    const err=s.error?` · <span style="color:var(--red)">${{esc(s.error).slice(0,40)}}</span>`:"";
    return `<div class="src-chip"><span class="src-dot" style="background:${{c}}"></span>${{esc(s.adapter_name)}}<span style="color:var(--dim)">·${{s.snapshot_count}}条·${{ms}}ms</span>${{err}}</div>`;
  }}).join("");
}}

function renderSummary(sm){{
  const lc=sm.label_counts||{{}};
  const bc=sm.bucket_counts||{{}};
  $("s-tradable").textContent=lc.tradable||0;
  $("s-watch").textContent=lc.watch||0;
  $("s-dis").textContent=bc.dislocation||0;
  $("s-carry").textContent=bc.carry||0;

  const rg=REG[sm.market_regime]||REG.idle;
  $("regime").innerHTML=`<span class="regime"><span class="regime-dot" style="background:${{rg[1]}}"></span>${{rg[0]}}</span>`;

  if(sm.top_opportunity){{
    const t=sm.top_opportunity;
    $("s-top").innerHTML=`${{esc(t.asset)}} <span style="color:var(--dim);font-weight:400">${{esc(t.venues.join(" ↔ "))}}</span>`;
  }}else{{$("s-top").textContent="暂无"}}

  const tags=sm.top_tags||[];
  $("tag-list").innerHTML=tags.length
    ?tags.map(t=>`<span class="tag">${{esc(t.tag)}}<span class="cnt">×${{t.count}}</span></span>`).join("")
    :`<span class="tag" style="color:var(--green)">✓ 无风险标签</span>`;
}}

let _expandedRows=new Set();
let _inputValues={{}};
let _searchQuery="";
function applySearch(){{
  _searchQuery=($("search-input").value||"").trim().toLowerCase();
  if(_expandedRows.size>0)return;
  renderTable(_lastOpps);
}}
function _matchSearch(item){{
  if(!_searchQuery)return true;
  const terms=_searchQuery.split(/\s+/);
  const hay=(item.asset+" "+item.quote+" "+item.venue_a+" "+item.venue_b+" "+(item.venue_a_asset||"")+" "+(item.venue_b_asset||"")).toLowerCase();
  return terms.every(t=>hay.includes(t));
}}
function renderTable(opps){{
  const tb=$("tbl-body");
  if(!opps||!opps.length){{
    tb.innerHTML=`<tr><td colspan="12" class="empty-msg">当前筛选条件下暂无机会</td></tr>`;
    $("search-count").textContent="";
    return;
  }}
  // If any row is expanded, skip full re-render to keep DOM stable
  if(_expandedRows.size>0)return;
  const filtered=opps.filter(_matchSearch);
  $("search-count").textContent=_searchQuery?`${{filtered.length}} / ${{opps.length}} 条`:"";
  if(!filtered.length){{
    tb.innerHTML=`<tr><td colspan="12" class="empty-msg">无匹配结果</td></tr>`;
    return;
  }}
  tb.innerHTML=filtered.map((item,fi)=>{{
    const i=opps.indexOf(item);
    const c=item.dashboard_card||{{}};
    const sw=Math.max(0,Math.min(item.composite_score,100));
    const lblCls=LBL_CLS[item.label]||"lbl-blocked";
    const bktCls=BKT_CLS[item.bucket]||"bkt-d";
    const dir=dirLabel(item.direction,item.venue_a,item.venue_b);
    const profitColor=item.expected_profit_bps>0?"var(--green)":item.expected_profit_bps<0?"var(--red)":"var(--text)";
    const profitUsd=finiteNum(item.expected_profit_usd,0);
    const risks=(c.risks||[]).map(r=>`<span class="tag" style="font-size:10px">${{esc(r)}}</span>`).join(" ")||"";
    const whyNow=(c.why_now||[]).map(p=>`<div>• ${{esc(p)}}</div>`).join("");
    const monitoring=(c.monitoring_points||[]).map(p=>`<div>• ${{esc(p)}}</div>`).join("");
    const rowCls=item.label==="tradable"?"row-tradable":item.label==="watch"?"row-watch":"";
    const expanded=_expandedRows.has(String(i))?"row-expand":"";
    return `
      <tr class="${{rowCls}} ${{expanded}}" data-idx="${{i}}" onclick="this.classList.toggle('row-expand');if(this.classList.contains('row-expand'))_expandedRows.add(String(${{i}}));else _expandedRows.delete(String(${{i}}))" style="cursor:pointer">
        <td style="color:var(--dim)">${{i+1}}</td>
        <td><strong>${{esc(item.asset)}}</strong><span style="color:var(--dim)">/${{esc(item.quote||"")}}</span></td>
        <td style="font-size:11px">${{esc(item.venue_a)}}<span style="color:var(--amber)">[`+esc(item.venue_a_asset||item.asset)+`]</span> <span style="color:var(--dim)">↔</span> ${{esc(item.venue_b)}}<span style="color:var(--amber)">[`+esc(item.venue_b_asset||item.asset)+`]</span></td>
        <td><span class="lbl ${{lblCls}}">${{esc(LBL[item.label]||item.label)}}</span></td>
        <td><span class="bkt ${{bktCls}}">${{esc(BKT[item.bucket]||item.bucket)}}</span></td>
        <td style="font-size:11px;font-family:'JetBrains Mono',monospace">${{detailCol(item)}}</td>
        <td style="font-size:11px;color:var(--dim);font-family:'JetBrains Mono',monospace">${{esc(dir)}}</td>
        <td><div style="display:flex;align-items:center;gap:8px">
          <span class="mono" style="font-size:12px;min-width:36px">${{item.composite_score.toFixed(1)}}</span>
          <div class="sbar"><span style="width:${{sw}}%"></span></div>
        </div></td>
        <td class="num" style="color:${{profitColor}};line-height:1.4">${{item.expected_profit_bps>=0?"+":""}}\
${{item.expected_profit_bps.toFixed(2)}} <span style="font-size:9px;color:var(--dim)">bps</span><br>\
<span style="font-size:11px">${{formatUsd(profitUsd)}}</span></td>
        <td class="num mono">${{item.roi_per_capital.toFixed(4)}}</td>
        <td class="num mono">${{item.spread_zscore.toFixed(2)}}</td>
        <td style="font-size:11px">${{esc(c.conviction||"")}}</td>
      </tr>
      <tr class="row-detail"><td colspan="12" class="detail-cell">
        <div style="margin-bottom:8px;font-size:12px;color:var(--text);line-height:1.6">${{esc(c.thesis||"")}}</div>
        <div class="detail-grid">
          <div class="detail-section"><strong>盈利预估</strong>${{profitDetail(item)}}</div>
          <div class="detail-section"><strong>当前重点</strong>${{whyNow}}</div>
          <div class="detail-section"><strong>监控指标</strong>${{monitoring}}</div>
          <div class="detail-section"><strong>风险标签</strong><div style="display:flex;flex-wrap:wrap;gap:4px;margin-top:4px">${{risks||'<span style="color:var(--green)">无</span>'}}</div></div>
        </div>
        <div class="pos-panel" id="pos-${{i}}">
          <div style="font-size:11px;font-weight:600;color:var(--cyan);margin-bottom:6px">建立监控仓位</div>
          <div class="pos-row">
            <div><label>${{esc(item.venue_a)}} 入场价</label><input type="number" step="any" id="pa-${{i}}" value="${{(item.price_a||0).toFixed(6)}}" onclick="event.stopPropagation()"></div>
            <div><label>${{esc(item.venue_b)}} 入场价</label><input type="number" step="any" id="pb-${{i}}" value="${{(item.price_b||0).toFixed(6)}}" onclick="event.stopPropagation()"></div>
            <button class="pos-btn pos-btn-go" onclick="event.stopPropagation();addPosition(${{i}})">确认建仓</button>
          </div>
        </div>
      </td></tr>`;
  }}).join("");
}}

function renderPractice(payload){{
  const practice=payload||{{}};
  const summary=practice.summary||{{}};
  const policy=practice.policy||{{}};
  const candidates=practice.candidates||[];
  const active=practice.active_trades||[];
  const completed=practice.recent_completed||[];
  const best=candidates.find(c=>c.gate_passed)||candidates[0];
  const rules=[
    `净收益 ≥ ${{(policy.min_expected_profit_bps??3).toFixed(1)}} bps`,
    `Z ≥ ${{(policy.min_dislocation_zscore??2).toFixed(1)}}`,
    `连续 ${{policy.required_consecutive_scans||3}} 轮确认`,
    `资产: ${{(policy.allowed_assets||[]).join(", ")||"-"}}`,
    `场所: ${{(policy.allowed_venues||[]).join(", ")||"-"}}`
  ];
  let candidateLine=`暂无通过合格线的候选`;
  if(best){{
    const status=best.ready?"已进入纸面账本":best.gate_passed?"等待连续确认":"未通过";
    const reasons=(best.reasons||[]).slice(0,2).join(", ");
    candidateLine=`${{esc(best.asset)}} ${{esc(best.venue_a)}}↔${{esc(best.venue_b)}} · ${{status}} · `+
      `${{best.expected_profit_bps.toFixed(2)}}bps · z=${{best.spread_zscore.toFixed(2)}}`+
      (reasons?` · ${{esc(reasons)}}`:"");
  }}
  $("practice-summary").innerHTML=
    `<div><span>合格候选</span> ${{summary.eligible_count||0}} · <span>就绪</span> ${{summary.ready_count||0}} · <span>纸面持仓</span> ${{summary.active_count||0}}</div>`+
    `<div style="color:var(--dim)">${{rules.map(esc).join(" / ")}}</div>`+
    `<div style="margin-top:6px">${{candidateLine}}</div>`;

  let ledger="";
  for(const t of active){{
    const cls=(t.net_pnl_bps||0)>=0?"pnl-pos":"pnl-neg";
    ledger+=`<div class="pos-chip">
      <strong>${{esc(t.asset)}}</strong>
      <span style="color:var(--dim)">${{esc(t.venue_a)}} ↔ ${{esc(t.venue_b)}}</span>
      <span class="pos-prices">入场 ${{t.entry_edge_bps.toFixed(2)}}bps / 当前 ${{t.current_edge_bps.toFixed(2)}}bps</span>
      <span class="${{cls}}">${{t.net_pnl_bps>=0?"+":""}}${{t.net_pnl_bps.toFixed(2)}} bps</span>
    </div>`;
  }}
  for(const t of completed.slice(-3).reverse()){{
    const cls=(t.net_pnl_bps||0)>=0?"pnl-pos":"pnl-neg";
    ledger+=`<div class="pos-chip" style="opacity:.75">
      <strong>${{esc(t.asset)}}</strong>
      <span style="color:var(--dim)">已结束 · ${{esc(t.close_reason||"closed")}}</span>
      <span class="${{cls}}">${{t.net_pnl_bps>=0?"+":""}}${{t.net_pnl_bps.toFixed(2)}} bps</span>
    </div>`;
  }}
  $("paper-ledger").innerHTML=ledger||`<span style="color:var(--dim);font-size:12px">暂无纸面交易</span>`;
}}

/* ── position management ── */
let _lastOpps=[];
async function addPosition(idx){{
  const item=_lastOpps[idx];if(!item)return;
  const pa=parseFloat(document.getElementById("pa-"+idx).value)||item.price_a;
  const pb=parseFloat(document.getElementById("pb-"+idx).value)||item.price_b;
  try{{
    const resp=await fetch("/api/position",{{method:"POST",headers:{{"Content-Type":"application/json"}},
      body:JSON.stringify({{action:"add",asset:item.asset,quote:item.quote||"USD",
        venue_a:item.venue_a,venue_b:item.venue_b,direction:item.direction,
        bucket:item.bucket,entry_price_a:pa,entry_price_b:pb}})
    }});
    const d=await resp.json();
    if(d.ok){{
      const btn=document.querySelector("#pos-"+idx+" .pos-btn-go");
      if(btn){{btn.textContent="已建仓 ✓";btn.style.background="var(--dim2)";btn.disabled=true;}}
    }}
  }}catch(e){{console.warn("addPosition error",e)}}
}}
async function removePosition(key){{
  try{{
    await fetch("/api/position",{{method:"POST",headers:{{"Content-Type":"application/json"}},
      body:JSON.stringify({{action:"remove",key}})
    }});
  }}catch(e){{console.warn("removePosition error",e)}}
}}
function renderPositions(positions,completed){{
  const strip=$("pos-strip");
  if(!positions||!positions.length){{strip.innerHTML=`<span style="color:var(--dim);font-size:12px">暂无持仓</span>`;return;}}
  let html="";
  for(const p of positions){{
    const pnlCls=p.pnl_bps>=0?"pnl-pos":"pnl-neg";
    const spreadAbs=Math.abs(p.current_price_a-p.current_price_b);
    const digits=spreadAbs>=1?2:spreadAbs>=0.01?4:6;
    const entryAbs=Math.abs(p.entry_price_a-p.entry_price_b);
    const edigits=entryAbs>=1?2:entryAbs>=0.01?4:6;
    html+=`<div class="pos-chip">
      <strong style="min-width:50px">${{esc(p.asset)}}</strong>
      <span style="color:var(--dim)">${{esc(p.venue_a)}} ↔ ${{esc(p.venue_b)}}</span>
      <span class="pos-prices">入: ${{p.entry_price_a.toFixed(2)}} / ${{p.entry_price_b.toFixed(2)}} (${{entryAbs.toFixed(edigits)}})</span>
      <span class="pos-prices">现: ${{p.current_price_a.toFixed(2)}} / ${{p.current_price_b.toFixed(2)}} (${{spreadAbs.toFixed(digits)}})</span>
      <span class="${{pnlCls}}" style="min-width:60px;text-align:right">${{p.pnl_bps>=0?"+":""}}${{p.pnl_bps.toFixed(2)}} bps</span>
      <button class="pos-btn pos-btn-rm" onclick="removePosition('${{esc(p.key)}}')">✕</button>
    </div>`;
  }}
  strip.innerHTML=html;
}}

function playCompletionSound(){{
  try{{
    if(!audioCtx)audioCtx=new(window.AudioContext||window.webkitAudioContext)();
    const now=audioCtx.currentTime;
    // Victory fanfare: C-E-G-C ascending
    [523,659,784,1047].forEach((freq,i)=>{{
      const osc=audioCtx.createOscillator();
      const gain=audioCtx.createGain();
      osc.type="triangle";
      osc.frequency.value=freq;
      gain.gain.setValueAtTime(0.25,now+i*0.18);
      gain.gain.exponentialRampToValueAtTime(0.001,now+i*0.18+0.5);
      osc.connect(gain);gain.connect(audioCtx.destination);
      osc.start(now+i*0.18);osc.stop(now+i*0.18+0.5);
    }});
  }}catch(e){{console.warn("completion audio error",e)}}
}}

function showCompletionEffect(completed){{
  if(!completed||!completed.length)return;
  // Flash overlay
  const overlay=document.createElement("div");
  overlay.style.cssText="position:fixed;inset:0;background:rgba(52,211,153,0.12);z-index:9999;pointer-events:none;transition:opacity 2s";
  document.body.appendChild(overlay);
  setTimeout(()=>{{overlay.style.opacity="0"}},200);
  setTimeout(()=>overlay.remove(),2200);
  // Toast notifications
  for(const p of completed){{
    const toast=document.createElement("div");
    toast.style.cssText="position:fixed;top:60px;right:20px;background:var(--bg3);border:1px solid var(--green);border-radius:8px;padding:12px 20px;z-index:10000;font-size:13px;color:var(--text);box-shadow:0 4px 24px rgba(52,211,153,0.2);transition:opacity 1s;max-width:360px";
    toast.innerHTML=`<div style="color:var(--green);font-weight:700;margin-bottom:4px">套利完成 ✓</div>
      <div><strong>${{esc(p.asset)}}</strong> ${{esc(p.venue_a)}} ↔ ${{esc(p.venue_b)}}</div>
      <div style="color:var(--dim);font-size:11px;margin-top:2px">入场 ${{p.entry_spread_bps.toFixed(1)}}bps → 当前 ${{p.current_spread_bps.toFixed(1)}}bps · PnL <span style="color:var(--green)">${{p.pnl_bps>=0?"+":""}}${{p.pnl_bps.toFixed(1)}}bps</span></div>`;
    document.body.appendChild(toast);
    setTimeout(()=>{{toast.style.opacity="0"}},5000);
    setTimeout(()=>toast.remove(),6000);
  }}
}}

/* ── alert sound via Web Audio API ── */
let audioCtx=null;
function playAlertSound(){{
  try{{
    if(!audioCtx)audioCtx=new(window.AudioContext||window.webkitAudioContext)();
    const now=audioCtx.currentTime;
    // 3-tone ascending chime
    [520,660,880].forEach((freq,i)=>{{
      const osc=audioCtx.createOscillator();
      const gain=audioCtx.createGain();
      osc.type="sine";
      osc.frequency.value=freq;
      gain.gain.setValueAtTime(0.18,now+i*0.15);
      gain.gain.exponentialRampToValueAtTime(0.001,now+i*0.15+0.4);
      osc.connect(gain);gain.connect(audioCtx.destination);
      osc.start(now+i*0.15);osc.stop(now+i*0.15+0.4);
    }});
  }}catch(e){{console.warn("audio error",e)}}
}}

async function refresh(){{
  const dot=$("scan-dot");
  const stxt=$("status-text");
  dot.style.background="var(--cyan)";
  try{{
    const dashboardResp=await fetch("/api/dashboard",{{cache:"no-store"}});
    if(!dashboardResp.ok)throw new Error(`HTTP ${{dashboardResp.status}}`);
    const d=await dashboardResp.json();
    renderSources(d.source_statuses||[]);
    renderSummary(d.dashboard_summary||{{}});
    renderPractice(d.practice||{{}});
    if(_expandedRows.size===0)_lastOpps=d.opportunities||[];
    renderTable(_lastOpps);
    renderPositions(d.positions||[],d.completed_positions||[]);
    const ms=d.scan_duration_ms!=null?d.scan_duration_ms.toFixed(0):"?";
    $("clock").textContent=new Date(d.timestamp).toLocaleTimeString("zh-CN",{{hour12:false}});
    const posCnt=(d.positions||[]).length;
    const posInfo=posCnt>0?` · ${{posCnt}} 持仓`:"";
    stxt.textContent=`${{d.snapshot_count||0}} 快照 · ${{d.opportunity_count||0}} 机会 · ${{ms}}ms${{posInfo}}`;
    dot.style.background="var(--green)";

    // Completion alerts
    const completed=d.completed_positions||[];
    if(completed.length>0){{
      playCompletionSound();
      showCompletionEffect(completed);
    }}

    // Play alert sound when backend signals sustained tradable
    if(d.alert_sound&&completed.length===0){{
      playAlertSound();
      const items=(d.alert_items||[]).map(a=>`${{a.asset}} ${{a.venues}}`).join(", ");
      console.log("[ALERT] 可交易信号持续≥60s:",items);
    }}
  }}catch(e){{
    stxt.textContent=`错误: ${{e.message}}`;
    dot.style.background="var(--red)";
  }}
}}

refresh();
setInterval(refresh,REFRESH_MS);
</script>
</body>
</html>
"""


def build_dashboard_html(refresh_seconds: float) -> str:
    refresh_ms = int(refresh_seconds * 1000)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PERP ARB · Funding Terminal</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&display=swap');
:root{{
  --bg:#000;--text:#d8d8df;--muted:#7b7b86;--faint:#353541;
  --line:#2b2b34;--blue:#1683ff;--purple:#8b5cf6;
  --green:#35d07f;--red:#ff5e6c;--amber:#f5b84b;--white:#f4f4f7;
}}
*{{box-sizing:border-box}}
html,body{{margin:0;background:var(--bg);color:var(--text);font-family:'JetBrains Mono',ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:16px;line-height:1.15}}
body{{min-height:100vh;overflow:hidden}}
.screen{{height:100vh;max-width:1720px;margin:0 auto;padding:0 18px 14px;display:flex;flex-direction:column}}
.controls{{display:grid;grid-template-columns:1fr auto;gap:14px;align-items:start;padding:0 0 8px;border-bottom:1px dashed var(--line)}}
.filters{{display:flex;flex-wrap:wrap;gap:8px 18px;align-items:center}}
.slider{{display:flex;align-items:center;gap:9px;white-space:nowrap;color:var(--white);font-weight:700}}
.slider input{{width:130px;accent-color:var(--blue)}}
.checks{{display:flex;flex-wrap:wrap;gap:9px 13px;grid-column:1/-1;color:var(--white)}}
.checks label{{display:inline-flex;align-items:center;gap:4px;white-space:nowrap}}
.checks input{{width:16px;height:16px;margin:0;accent-color:var(--white)}}
.checks label.disabled{{color:#52525c}}
.checks label.disabled input{{accent-color:#555}}
.actions{{display:flex;align-items:center;gap:14px;justify-content:flex-end;color:var(--muted);font-size:13px;white-space:nowrap}}
.status{{color:var(--muted)}}
.search{{background:#030303;border:1px solid #1c1c24;color:var(--text);border-radius:3px;padding:3px 8px;font:inherit;font-size:13px;width:210px;outline:none}}
.search:focus{{border-color:#444}}
.blacklist-input{{width:118px}}
.mini-btn{{background:#050508;border:1px solid #2b2b34;color:var(--text);border-radius:3px;padding:3px 8px;font:inherit;font-size:13px;cursor:pointer}}
.mini-btn:hover{{border-color:#555;color:var(--white)}}
.blacklist-tags{{display:flex;flex-wrap:wrap;gap:6px;grid-column:1/-1;color:var(--muted);font-size:12px}}
.blacklist-tags:empty{{display:none}}
.blacklist-tag{{background:#050508;border:1px solid #2b2b34;color:#d6d6dc;border-radius:3px;padding:2px 6px;font:inherit;font-size:12px;cursor:pointer}}
.blacklist-tag:hover{{border-color:var(--red);color:var(--red)}}
.table-wrap{{flex:1;min-height:0;overflow:auto;padding-top:12px}}
table{{width:100%;border-collapse:collapse;table-layout:fixed}}
thead th{{position:sticky;top:0;background:#000;color:#e5e5e8;font-weight:700;text-align:left;padding:4px 8px 7px;border-bottom:0;font-size:16px;white-space:nowrap}}
tbody td{{padding:1px 8px;color:#9d9da7;font-size:16px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;vertical-align:top}}
tbody tr{{height:22px}}
tbody tr:hover td{{background:#08080d;color:#d6d6dc}}
.num{{text-align:right;font-variant-numeric:tabular-nums}}
.venue{{width:132px}}
.symbol{{width:164px;color:#c8c8d0}}
.spread{{width:164px}}
.stdspread{{width:126px}}
.funddir{{width:96px}}
.feesum{{width:92px}}
.counterparty{{width:88px}}
.oi{{width:126px}}
.vol{{width:126px}}
.apr{{width:124px}}
.fund{{width:156px}}
.limit{{width:126px}}
.sortable{{cursor:pointer;user-select:none}}
.sortable:hover{{color:var(--blue)}}
.symbol-btn{{cursor:pointer;color:#dcdce4}}
.symbol-btn:hover{{color:var(--blue)}}
.dim{{color:var(--muted)}}
.positive{{color:var(--green)}}
.negative{{color:var(--red)}}
.neutral{{color:#b8b8c0}}
.empty{{padding:48px;text-align:center;color:var(--muted)}}
.small{{font-size:13px;color:var(--muted)}}
.row-blocked td{{color:#696973}}
.row-watch td{{color:#b6a56e}}
.row-tradable td{{color:#d6d6dc}}
@media(max-width:900px){{
  body{{overflow:auto}}
  .screen{{height:auto;min-height:100vh;padding:0 10px 12px}}
  .controls{{grid-template-columns:1fr}}
  .actions{{justify-content:flex-start;flex-wrap:wrap}}
  .table-wrap{{overflow-x:auto}}
  table{{min-width:1680px}}
}}
</style>
</head>
<body>
<div class="screen">
  <div class="controls">
    <div class="filters">
      <label class="slider">未平仓额 ≥ <span id="oi-label">$1M</span><input id="oi-slider" type="range" min="0" max="100" value="0"></label>
      <label class="slider">日成交额 ≥ <span id="vol-label">$1M</span><input id="vol-slider" type="range" min="0" max="100" value="0"></label>
      <label class="slider">间隔≤<span id="interval-label">*H</span><input id="interval-slider" type="range" min="0" max="100" value="100"></label>
    </div>
    <div class="actions">
      <input id="search-input" class="search" placeholder="搜索币种/交易所" oninput="applyFilters()">
      <input id="blacklist-input" class="search blacklist-input" placeholder="拉黑币种" onkeydown="if(event.key==='Enter')addBlacklistAsset()">
      <button type="button" class="mini-btn" onclick="addBlacklistAsset()">拉黑</button>
      <span class="status" id="status-text">启动中...</span>
      <span class="status" id="clock">--:--:--</span>
    </div>
    <div class="checks" id="venue-checks"></div>
    <div class="blacklist-tags" id="blacklist-tags"></div>
  </div>
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th class="venue">交易所</th>
          <th class="counterparty">对手所</th>
          <th class="symbol">币种</th>
          <th class="spread num sortable" id="spread-sort" onclick="toggleSpreadSort()">最大价差/百分比 ↕</th>
          <th class="stdspread num sortable" id="spread-mean-sort" onclick="toggleSpreadMeanSort()">价差-4H均值 ↕</th>
          <th class="limit num">4H价差Z</th>
          <th class="feesum num">手续费</th>
          <th class="funddir num">资费方向</th>
          <th class="oi num">未平仓额</th>
          <th class="vol num">日成交额</th>
          <th class="apr num sortable" id="annual-sort" onclick="toggleAnnualSort()">1Y 资金费率 ↓</th>
          <th class="fund num">下一次资金费率</th>
        </tr>
      </thead>
      <tbody id="tbl-body"><tr><td colspan="12" class="empty">正在连接数据源...</td></tr></tbody>
    </table>
  </div>
</div>
<script>
const REFRESH_MS={refresh_ms};
const $=id=>document.getElementById(id);
const ALL_VENUES=["Binance","Bybit","OKX","Bitget","Gate","Kraken","Aster","Hyperliquid","Lighter","Grvt","Paradex","Nado","Ondo"];
const ACTIVE_HINTS=new Set(ALL_VENUES);
const DEFAULT_VISIBLE_ROWS=20;
const DISPLAY_MIN_OI_USD=25000;
const DISPLAY_MIN_VOLUME_USD=50000;
const BLACKLIST_STORAGE_KEY="perpArb.assetBlacklist.v1";
let selectedVenues=new Set();
let blacklistedAssets=new Set();
let lastRows=[];
let activeSort="annual";
let sortAnnualDir=-1;
let sortSpreadDir=-1;
let sortSpreadMeanDir=-1;

function esc(v){{return String(v??"").replace(/[&<>"]/g,c=>({{"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}})[c])}}
function normVenue(v){{const s=String(v||"").replace(/-ws$/,"").toLowerCase();return s.charAt(0).toUpperCase()+s.slice(1)}}
function normAsset(v){{
  let s=String(v||"").trim().toUpperCase();
  if(!s)return "";
  s=s.split(/[\\/\\s]+/)[0];
  s=s.replace(/[-_](USD|USDT|USDC|USDE|FDUSD|USDT0)(\\.P)?$/,"");
  s=s.replace(/-PERP$/,"");
  return s;
}}
function assetKeys(item){{return [item.asset,item.venue_a_asset,item.venue_b_asset].map(normAsset).filter(Boolean)}}
function isBlacklistedAsset(item){{return assetKeys(item).some(asset=>blacklistedAssets.has(asset))}}
function localBlacklist(){{
  try{{
    const raw=JSON.parse(localStorage.getItem(BLACKLIST_STORAGE_KEY)||"[]");
    return new Set((Array.isArray(raw)?raw:[]).map(normAsset).filter(Boolean));
  }}catch(_){{return new Set()}}
}}
function persistLocalBlacklist(){{localStorage.setItem(BLACKLIST_STORAGE_KEY,JSON.stringify([...blacklistedAssets].sort()))}}
async function syncBlacklist(){{
  persistLocalBlacklist();
  try{{
    const resp=await fetch("/api/asset-blacklist",{{method:"POST",headers:{{"Content-Type":"application/json"}},body:JSON.stringify({{assets:[...blacklistedAssets]}})}});
    if(resp.ok){{
      const d=await resp.json();
      blacklistedAssets=new Set((d.assets||[]).map(normAsset).filter(Boolean));
      persistLocalBlacklist();
      renderBlacklistTags();
      applyFilters();
    }}
  }}catch(err){{console.warn("asset blacklist sync failed",err)}}
}}
async function loadBlacklist(){{
  blacklistedAssets=localBlacklist();
  try{{
    const resp=await fetch("/api/asset-blacklist",{{cache:"no-store"}});
    if(resp.ok){{
      const d=await resp.json();
      const serverAssets=new Set((d.assets||[]).map(normAsset).filter(Boolean));
      const merged=new Set([...serverAssets,...blacklistedAssets]);
      blacklistedAssets=merged;
      persistLocalBlacklist();
      if(merged.size!==serverAssets.size)syncBlacklist();
    }}
  }}catch(err){{console.warn("asset blacklist load failed",err)}}
  renderBlacklistTags();
  applyFilters();
}}
function renderBlacklistTags(){{
  const box=$("blacklist-tags");
  if(!box)return;
  box.innerHTML=[...blacklistedAssets].sort().map(asset=>`<button type="button" class="blacklist-tag" data-asset="${{esc(asset)}}" onclick="removeBlacklistAsset(this.dataset.asset)" title="取消拉黑 ${{esc(asset)}}">${{esc(asset)}} ×</button>`).join("");
}}
async function addBlacklistAsset(asset){{
  const input=$("blacklist-input");
  const value=normAsset(asset??(input?input.value:""));
  if(!value)return;
  blacklistedAssets.add(value);
  if(input)input.value="";
  persistLocalBlacklist();
  renderBlacklistTags();
  applyFilters();
  await syncBlacklist();
}}
async function removeBlacklistAsset(asset){{
  blacklistedAssets.delete(normAsset(asset));
  persistLocalBlacklist();
  renderBlacklistTags();
  applyFilters();
  await syncBlacklist();
}}
function fmtMoney(v){{v=Number(v)||0;if(v<=0)return"--";const a=Math.abs(v);if(a>=1e9)return"$"+(v/1e9).toFixed(1)+"B";if(a>=1e6)return"$"+(v/1e6).toFixed(1)+"M";if(a>=1e3)return"$"+(v/1e3).toFixed(1)+"K";return"$"+v.toFixed(0)}}
function fmtSignedPct(v,d=1){{v=Number(v)||0;return (v>0?"+":"")+v.toFixed(d)+"%"}}
function fmtSignedBps(v,d=4){{v=Number(v)||0;return (v>0?"+":"")+v.toFixed(d)+"%"}}
function fmtFeeBps(v){{v=Number(v);if(!Number.isFinite(v))return"--";return v.toFixed(1)+"bps"}}
function fmtZScore(v){{v=Number(v);if(!Number.isFinite(v))return"--";return (v>0?"+":"")+v.toFixed(2)}}
function zScoreClass(v){{v=Number(v);if(!Number.isFinite(v))return"dim";if(v>=2)return"positive";if(v<=-2)return"negative";return Math.abs(v)>=1?"neutral":"dim"}}
function fmtSpreadVsMean(currentPct,meanBps){{
  const meanPct=Number(meanBps)/100;
  currentPct=Number(currentPct);
  if(!Number.isFinite(currentPct)||!Number.isFinite(meanPct))return"--";
  const diff=currentPct-meanPct;
  return (diff>0?"+":"")+diff.toFixed(3)+"%";
}}
function fmtHours(h){{h=Number(h)||0;if(h<=0)return"--";if(h>=24&&h%24===0)return (h/24).toFixed(0)+"D";return h.toFixed(h>=10?0:1).replace(/\\.0$/,"")+"H"}}
function hoursToNext(ts){{if(!ts)return null;const t=Date.parse(ts);if(!Number.isFinite(t))return null;return Math.max(0,(t-Date.now())/3600000)}}
function maxSpread(item){{
  const bidA=Number(item.best_bid_a)||0,askA=Number(item.best_ask_a)||0;
  const bidB=Number(item.best_bid_b)||0,askB=Number(item.best_ask_b)||0;
  const midA=Number(item.price_a)||0,midB=Number(item.price_b)||0;
  if(bidA>0&&askA>0&&bidB>0&&askB>0){{
    const aLongBShort=bidB-askA;
    const bLongAShort=bidA-askB;
    const edge=Math.max(aLongBShort,bLongAShort);
    if(edge>0){{
      const ref=aLongBShort>=bLongAShort?((bidA+askA)/2):((bidB+askB)/2);
      return {{abs:edge,pct:edge/Math.max(ref,1e-12)*100,longIsA:aLongBShort>=bLongAShort}};
    }}
    const midAbs=Math.abs(((bidA+askA)/2)-((bidB+askB)/2));
    return {{abs:midAbs,pct:0,longIsA:((bidA+askA)/2)<=((bidB+askB)/2)}};
  }}
  if(midA<=0||midB<=0)return {{abs:0,pct:0,longIsA:true}};
  const abs=Math.abs(midA-midB);
  return {{abs,pct:abs/Math.min(midA,midB)*100,longIsA:midA<=midB}};
}}
function priceDigits(v){{v=Number(v)||0;if(v>=100)return 2;if(v>=1)return 4;if(v>=0.01)return 5;return 7}}
function rowMetrics(item){{
  const spread=maxSpread(item);
  const ia=Number(item.funding_a_interval_h)||8;
  const ib=Number(item.funding_b_interval_h)||8;
  const fa=Number(item.funding_a_bps)||0;
  const fb=Number(item.funding_b_bps)||0;
  const dailyA=fa*(24/ia);
  const dailyB=fb*(24/ib);
  const nextA=hoursToNext(item.next_funding_a);
  const nextB=hoursToNext(item.next_funding_b);
  const venueA=normVenue(item.venue_a);
  const venueB=normVenue(item.venue_b);
  const primaryIsA=Math.abs(dailyA)>=Math.abs(dailyB);
  const annual=(primaryIsA?(dailyA-dailyB):(dailyB-dailyA))*365/100;
  const spreadVsMean=spread.pct-(Number(item.spread_mean_bps)||0)/100;
  const longIsA=spread.longIsA!==false;
  const tickerOnlyA=!!item.venue_a_ticker_only;
  const tickerOnlyB=!!item.venue_b_ticker_only;
  const primaryVenueBase=primaryIsA?venueA:venueB;
  const counterpartyVenueBase=primaryIsA?venueB:venueA;
  const primaryTickerOnly=primaryIsA?tickerOnlyA:tickerOnlyB;
  const counterpartyTickerOnly=primaryIsA?tickerOnlyB:tickerOnlyA;
  const primaryVenue=primaryVenueBase+(primaryTickerOnly?"*":"");
  const counterpartyVenue=counterpartyVenueBase+(counterpartyTickerOnly?"*":"");
  const primaryVenueTitle=primaryTickerOnly?`${{primaryVenueBase}} ticker-only，价格可信度较低`:"";
  const counterpartyVenueTitle=counterpartyTickerOnly?`${{counterpartyVenueBase}} ticker-only，价格可信度较低`:"";
  const longVenue=longIsA?venueA:venueB;
  const shortVenue=longIsA?venueB:venueA;
  const fundingCarry=longIsA?(-dailyA+dailyB):(-dailyB+dailyA);
  const fundingAligned=fundingCarry>=0;
  const fundingDirectionTitle=`收敛方向: 多 ${{longVenue}} / 空 ${{shortVenue}} · 净资金费 ${{fmtSignedBps(fundingCarry/100,4)}}/日`;
  const makerA=Number(item.maker_fee_a_bps)||0;
  const takerA=Number(item.taker_fee_a_bps)||0;
  const makerB=Number(item.maker_fee_b_bps)||0;
  const takerB=Number(item.taker_fee_b_bps)||0;
  const feeAMakerBTaker=makerA+takerB;
  const feeATakerBMaker=takerA+makerB;
  const aMakerCheaper=feeAMakerBTaker<=feeATakerBMaker;
  const feeSum=aMakerCheaper?feeAMakerBTaker:feeATakerBMaker;
  const feeTitle=aMakerCheaper?`${{venueA}} maker ${{fmtFeeBps(makerA)}} + ${{venueB}} taker ${{fmtFeeBps(takerB)}}`:`${{venueA}} taker ${{fmtFeeBps(takerA)}} + ${{venueB}} maker ${{fmtFeeBps(makerB)}}`;
  const oiA=Number(item.oi_a_usd)||0,oiB=Number(item.oi_b_usd)||0;
  const volA=Number(item.volume_a_24h_usd)||0,volB=Number(item.volume_b_24h_usd)||0;
  const oi=primaryIsA?oiA:oiB;
  const vol=primaryIsA?volA:volB;
  const minOi=Math.min(oiA,oiB);
  const minVol=Math.min(volA,volB);
  const funding=primaryIsA?fa:fb;
  const next=primaryIsA?nextA:nextB;
  const interval=primaryIsA?ia:ib;
  return {{spread,spreadVsMean,fundingAligned,fundingCarry,fundingDirectionTitle,feeSum,feeTitle,oi,vol,minOi,minVol,annual,next,interval,funding,primaryVenue,counterpartyVenue,primaryVenueTitle,counterpartyVenueTitle}};
}}
function threshold(slider,min,max,zeroValue=0){{
  const x=(Number(slider.value)||0)/100;
  if(x<=0)return zeroValue;
  return min*Math.pow(max/min,x);
}}
function intervalThreshold(slider){{
  const x=(Number(slider.value)||0)/100;
  if(x>=0.99)return Infinity;
  return 1+x*23;
}}
function updateSliderLabels(){{
  $("oi-label").textContent=fmtMoney(threshold($("oi-slider"),1e6,1e9,1e6));
  $("vol-label").textContent=fmtMoney(threshold($("vol-slider"),1e6,1e9));
  const h=intervalThreshold($("interval-slider"));
  $("interval-label").textContent=Number.isFinite(h)?fmtHours(h):"*H";
}}
function renderVenueChecks(){{
  const box=$("venue-checks");
  box.innerHTML=ALL_VENUES.map(v=>{{
    const checked=selectedVenues.has(v)?"checked":"";
    const disabled=ACTIVE_HINTS.has(v)?"":"disabled";
    return `<label class="${{disabled}}"><input type="checkbox" value="${{esc(v)}}" ${{checked}} onchange="toggleVenue(this)"> ${{esc(v)}}</label>`;
  }}).join("");
}}
function toggleVenue(el){{if(el.checked)selectedVenues.add(el.value);else selectedVenues.delete(el.value);applyFilters()}}
function initControls(){{
  loadBlacklist();
  for(const v of ACTIVE_HINTS)selectedVenues.add(v);
  renderVenueChecks();
  renderBlacklistTags();
  for(const id of ["oi-slider","vol-slider","interval-slider"])$(id).addEventListener("input",()=>{{updateSliderLabels();applyFilters()}});
  updateSliderLabels();
}}
function matches(item,m){{
  if(isBlacklistedAsset(item))return false;
  const q=($("search-input").value||"").trim().toLowerCase();
  if(q){{
    const hay=[item.asset,item.quote,item.venue_a,item.venue_b,item.venue_a_asset,item.venue_b_asset].join(" ").toLowerCase();
    if(!q.split(/\\s+/).every(t=>hay.includes(t)))return false;
    return true;
  }}
  const va=normVenue(item.venue_a), vb=normVenue(item.venue_b);
  if(selectedVenues.size&&!(selectedVenues.has(va)||selectedVenues.has(vb)))return false;
  if(m.minOi<threshold($("oi-slider"),1e6,1e9,1e6))return false;
  if(m.minVol<threshold($("vol-slider"),1e6,1e9))return false;
  if(m.interval>intervalThreshold($("interval-slider")))return false;
  return true;
}}
function displayQuality(item){{
  const oiA=Number(item.oi_a_usd)||0, oiB=Number(item.oi_b_usd)||0;
  const volA=Number(item.volume_a_24h_usd)||0, volB=Number(item.volume_b_24h_usd)||0;
  return Math.min(oiA,oiB)>=DISPLAY_MIN_OI_USD && Math.min(volA,volB)>=DISPLAY_MIN_VOLUME_USD;
}}
function rowClass(item){{return item.label==="tradable"?"row-tradable":item.label==="watch"?"row-watch":"row-blocked"}}
function searchSymbol(asset){{
  $("search-input").value=asset||"";
  applyFilters();
}}
function toggleAnnualSort(){{
  sortAnnualDir*=-1;
  activeSort="annual";
  updateSortHeaders();
  renderTable(lastRows);
}}
function toggleSpreadSort(){{
  sortSpreadDir*=-1;
  activeSort="spread";
  updateSortHeaders();
  renderTable(lastRows);
}}
function toggleSpreadMeanSort(){{
  sortSpreadMeanDir*=-1;
  activeSort="spreadMean";
  updateSortHeaders();
  renderTable(lastRows);
}}
function updateSortHeaders(){{
  $("spread-sort").textContent="最大价差/百分比 "+(activeSort==="spread"?(sortSpreadDir<0?"↓":"↑"):"↕");
  $("spread-mean-sort").textContent="价差-4H均值 "+(activeSort==="spreadMean"?(sortSpreadMeanDir<0?"↓":"↑"):"↕");
  $("annual-sort").textContent="1Y 资金费率 "+(activeSort==="annual"?(sortAnnualDir<0?"↓":"↑"):"↕");
}}
function renderTable(rows){{
  const tb=$("tbl-body");
  const query=($("search-input").value||"").trim();
  const matched=rows.map(item=>({{item,m:rowMetrics(item)}})).filter(x=>matches(x.item,x.m));
  const displayPool=query?matched:matched.filter(x=>displayQuality(x.item));
  const sorted=displayPool.sort((a,b)=>{{
    if(activeSort==="spread")return sortSpreadDir*(a.m.spread.pct-b.m.spread.pct);
    if(activeSort==="spreadMean")return sortSpreadMeanDir*(a.m.spreadVsMean-b.m.spreadVsMean);
    return sortAnnualDir*(a.m.annual-b.m.annual);
  }});
  const filtered=sorted.slice(0,DEFAULT_VISIBLE_ROWS);
  if(!filtered.length){{tb.innerHTML=`<tr><td colspan="12" class="empty">无匹配结果</td></tr>`;return}}
  tb.innerHTML=filtered.map(({{item,m}})=>{{
    const spreadDigits=priceDigits(m.spread.abs);
    const spreadClr=m.spread.pct>=1?"positive":m.spread.pct>=0.25?"neutral":"dim";
    const annualCls=m.annual>=0?"positive":"negative";
    const fundCls=m.funding>=0?"positive":"negative";
    const nextText=m.next==null?"--":(m.next>=1?Math.floor(m.next)+"h "+Math.round((m.next%1)*60)+"m":Math.round(m.next*60)+"m");
    return `<tr class="${{rowClass(item)}}" title="${{esc(item.direction_display||item.direction)}} · 净收益 ${{Number(item.expected_profit_bps||0).toFixed(2)}}bps">
      <td class="venue" title="${{esc(m.primaryVenueTitle)}}">${{esc(m.primaryVenue)}}</td>
      <td class="counterparty" title="${{esc(m.counterpartyVenueTitle)}}">${{esc(m.counterpartyVenue)}}</td>
      <td class="symbol"><span class="symbol-btn" data-asset="${{esc(item.asset)}}" onclick="searchSymbol(this.dataset.asset)">${{esc(item.venue_a_asset||item.asset)}}/${{esc(item.quote||"USD")}}</span></td>
      <td class="spread num"><span class="${{spreadClr}}">${{m.spread.abs.toFixed(spreadDigits)}} / ${{m.spread.pct.toFixed(3)}}%</span></td>
      <td class="stdspread num"><span class="${{m.spreadVsMean>=0?"positive":"negative"}}">${{fmtSpreadVsMean(m.spread.pct,item.spread_mean_bps)}}</span></td>
      <td class="limit num"><span class="${{zScoreClass(item.spread_zscore)}}">${{fmtZScore(item.spread_zscore)}}</span></td>
      <td class="feesum num"><span class="neutral" title="${{esc(m.feeTitle)}}">${{fmtFeeBps(m.feeSum)}}</span></td>
      <td class="funddir num"><span class="${{m.fundingAligned?"positive":"negative"}}" title="${{esc(m.fundingDirectionTitle)}}">${{m.fundingAligned?"符合":"不符"}}</span></td>
      <td class="oi num">${{fmtMoney(m.oi)}}</td>
      <td class="vol num">${{fmtMoney(m.vol)}}</td>
      <td class="apr num"><span class="${{annualCls}}">${{fmtSignedPct(m.annual,1)}}</span></td>
      <td class="fund num"><span class="${{fundCls}}">${{nextText}} ${{fmtSignedBps(m.funding/100,4)}}</span></td>
    </tr>`;
  }}).join("");
}}
function applyFilters(){{
  updateSliderLabels();
  renderTable(lastRows);
}}
async function refresh(){{
  try{{
    const resp=await fetch("/api/dashboard",{{cache:"no-store"}});
    if(!resp.ok)throw new Error(`HTTP ${{resp.status}}`);
    const d=await resp.json();
    if(Array.isArray(d.asset_blacklist)){{
      blacklistedAssets=new Set(d.asset_blacklist.map(normAsset).filter(Boolean));
      persistLocalBlacklist();
      renderBlacklistTags();
    }}
    lastRows=d.opportunities||[];
    renderTable(lastRows);
    const ms=d.scan_duration_ms!=null?Number(d.scan_duration_ms).toFixed(0):"?";
    $("status-text").textContent=`${{d.snapshot_count||0}} 快照 · ${{d.opportunity_count||0}} 机会 · ${{ms}}ms`;
    $("clock").textContent=new Date(d.timestamp).toLocaleTimeString("zh-CN",{{hour12:false}});
  }}catch(err){{
    $("status-text").textContent="错误: "+err.message;
  }}
}}
initControls();
refresh();
setInterval(refresh,REFRESH_MS);
</script>
</body>
</html>"""
