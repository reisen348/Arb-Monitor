from __future__ import annotations

import threading
import unittest
from collections import deque
from datetime import datetime

from perp_arb.market_data import MarketSnapshot, SourceStatus
from perp_arb.scanner import RealtimeScanner


class _Adapter:
    name = "mock"

    def poll(self):
        return [
            MarketSnapshot(
                venue="mock",
                market_type="perp",
                asset="BTC",
                quote="USD",
                best_bid=1.0,
                best_ask=1.1,
                mark_price=1.05,
                oracle_price=1.05,
                index_price=1.05,
                taker_fee_bps=1.0,
            )
        ]


class _StateStore:
    def __init__(self, block_market_flush=True):
        self.market_flushes = 0
        self.opportunity_flushes = 0
        self.archive_appends = 0
        self.timestamps = 0
        self.started = threading.Event()
        self.release = threading.Event()
        self.flush_history = []
        self.block_market_flush = block_market_flush
        self.snapshot_loads = 0
        self.opportunity_loads = 0

    def load_snapshot_history(self, max_points=120):
        self.snapshot_loads += 1
        return {}

    def load_opportunity_history(self, max_points=120):
        self.opportunity_loads += 1
        return {}

    def flush_market_state(self, history):
        self.market_flushes += 1
        self.flush_history.append(sorted(history.keys()))
        self.started.set()
        if self.block_market_flush:
            self.release.wait(timeout=1.0)

    def flush_opportunity_state(self, history):
        self.opportunity_flushes += 1

    def append_opportunity_state_archive(self, history):
        self.archive_appends += 1

    def save_source_timestamp(self, adapter_name, timestamp):
        self.timestamps += 1


class ScannerFlushTest(unittest.TestCase):
    def test_request_state_flush_keeps_latest_pending_snapshot(self) -> None:
        store = _StateStore()
        scanner = RealtimeScanner([_Adapter()], state_store=store)
        scanner.scan_once()
        scanner.request_state_flush(
            [SourceStatus(adapter_name="mock", ok=True, snapshot_count=1)],
            timestamp := datetime.utcnow(),
        )
        self.assertTrue(store.started.wait(timeout=1.0))

        scanner.market_state._history[("extra", "BTC", "USD")] = deque(maxlen=600)
        scanner.request_state_flush(
            [SourceStatus(adapter_name="mock", ok=True, snapshot_count=1)],
            timestamp,
        )

        store.release.set()
        scanner.close()

        self.assertEqual(store.market_flushes, 2)
        self.assertEqual(store.opportunity_flushes, 2)
        self.assertIn(("extra", "BTC", "USD"), store.flush_history[-1])

    def test_scan_flush_skips_rolling_state_until_rolling_interval(self) -> None:
        store = _StateStore(block_market_flush=False)
        scanner = RealtimeScanner([_Adapter()], state_store=store)
        scanner._flush_executor.shutdown(wait=False, cancel_futures=True)
        scanner._flush_executor = None

        for _ in range(scanner._flush_interval):
            scanner.scan_once()
        scanner.close()

        self.assertEqual(store.archive_appends, 1)
        self.assertEqual(store.opportunity_flushes, 0)
        self.assertEqual(store.market_flushes, 0)

    def test_scan_flush_includes_rolling_state_on_rolling_interval(self) -> None:
        store = _StateStore(block_market_flush=False)
        scanner = RealtimeScanner([_Adapter()], state_store=store)
        scanner._flush_executor.shutdown(wait=False, cancel_futures=True)
        scanner._flush_executor = None

        for _ in range(scanner._rolling_flush_interval):
            scanner.scan_once()
        scanner.close()

        self.assertEqual(store.archive_appends, scanner._rolling_flush_interval // scanner._flush_interval)
        self.assertEqual(store.opportunity_flushes, 1)
        self.assertEqual(store.market_flushes, 1)

    def test_can_skip_state_restore_on_startup(self) -> None:
        store = _StateStore(block_market_flush=False)
        scanner = RealtimeScanner(
            [_Adapter()],
            state_store=store,
            restore_market_state=False,
            restore_opportunity_state=False,
        )
        scanner.close()

        self.assertEqual(store.snapshot_loads, 0)
        self.assertEqual(store.opportunity_loads, 0)


if __name__ == "__main__":
    unittest.main()
