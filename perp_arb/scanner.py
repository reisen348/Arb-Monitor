from __future__ import annotations

import time
import traceback
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
import threading
from typing import Callable, Iterable, List, Optional, Sequence, Tuple

from .market_data import (
    MarketDataAdapter,
    MarketSnapshot,
    OpportunityBuilder,
    PairingConfig,
    ScanBatch,
    ScannerConfig,
    SourceStatus,
)
from .models import ExecutionLabel, ScoredOpportunity
from .persistence import StateStore
from .scoring import ArbitrageScorer, ScoringConfig
from .state import MarketStateTracker, OpportunityStateTracker, StateTrackerConfig


def _poll_adapter(adapter: MarketDataAdapter, timeout_seconds: float) -> Tuple[str, List[MarketSnapshot], Optional[str], float]:
    """Poll a single adapter, returning (name, snapshots, error, duration_ms)."""
    name = getattr(adapter, "name", type(adapter).__name__)
    start = time.monotonic()
    try:
        snapshots = list(adapter.poll())
        duration_ms = (time.monotonic() - start) * 1000.0
        return name, snapshots, None, duration_ms
    except Exception as exc:
        duration_ms = (time.monotonic() - start) * 1000.0
        return name, [], f"{type(exc).__name__}: {exc}", duration_ms


class RealtimeScanner:
    def __init__(
        self,
        adapters: Sequence[MarketDataAdapter],
        scoring_config: ScoringConfig | None = None,
        pairing_config: PairingConfig | None = None,
        scanner_config: ScannerConfig | None = None,
        state_config: StateTrackerConfig | None = None,
        poll_timeout_seconds: float = 10.0,
        state_store: Optional[StateStore] = None,
        restore_market_state: bool = True,
        restore_opportunity_state: bool = True,
    ) -> None:
        self.adapters = list(adapters)
        self.scorer = ArbitrageScorer(scoring_config)
        self.builder = OpportunityBuilder(pairing_config)
        self.config = scanner_config or ScannerConfig()
        self.market_state = MarketStateTracker(state_config)
        self.opportunity_state = OpportunityStateTracker(state_config)
        self.poll_timeout_seconds = poll_timeout_seconds
        self._state_store = state_store
        self._scan_count = 0
        self._flush_interval = 5  # flush every N scans
        self._rolling_flush_interval = 60  # rolling-state tables are large; rewrite them much less often
        self._poll_executor: ThreadPoolExecutor | None = None
        self._flush_executor: ThreadPoolExecutor | None = None
        self._flush_future: Future | None = None
        self._flush_lock = threading.Lock()
        self._pending_flush_payload: tuple[dict, dict, list[SourceStatus], datetime, bool, bool] | None = None
        if len(self.adapters) > 1:
            self._poll_executor = ThreadPoolExecutor(max_workers=len(self.adapters), thread_name_prefix="adapter-poll")
        if self._state_store is not None:
            self._flush_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="state-flush")

        # Restore state from persistence if available
        if self._state_store is not None:
            try:
                cfg = state_config or StateTrackerConfig()
                if restore_market_state:
                    snapshot_history = self._state_store.load_snapshot_history(cfg.max_points)
                    self.market_state.restore_history(snapshot_history)
                if restore_opportunity_state:
                    opportunity_history = self._state_store.load_opportunity_history(cfg.opportunity_points)
                    self.opportunity_state.restore_history(opportunity_history)
            except Exception:
                pass  # Graceful degradation: start fresh if DB is corrupted

    def scan_once(self) -> ScanBatch:
        scan_start = time.monotonic()
        timestamp = datetime.utcnow()
        snapshots: List[MarketSnapshot] = []
        source_statuses: List[SourceStatus] = []

        if len(self.adapters) <= 1:
            # Single adapter: no thread overhead
            for adapter in self.adapters:
                name, adapter_snapshots, error, duration_ms = _poll_adapter(adapter, self.poll_timeout_seconds)
                snapshots.extend(adapter_snapshots)
                source_statuses.append(SourceStatus(
                    adapter_name=name,
                    ok=error is None,
                    snapshot_count=len(adapter_snapshots),
                    poll_duration_ms=round(duration_ms, 2),
                    error=error,
                    timestamp=timestamp,
                ))
        else:
            # Multiple adapters: poll in parallel
            executor = self._poll_executor or ThreadPoolExecutor(max_workers=len(self.adapters))
            futures = {
                executor.submit(_poll_adapter, adapter, self.poll_timeout_seconds): adapter
                for adapter in self.adapters
            }
            try:
                done_iter = as_completed(futures, timeout=self.poll_timeout_seconds + 5.0)
                for future in done_iter:
                    try:
                        name, adapter_snapshots, error, duration_ms = future.result()
                    except Exception as exc:
                        adapter = futures[future]
                        name = getattr(adapter, "name", type(adapter).__name__)
                        adapter_snapshots = []
                        error = f"{type(exc).__name__}: {exc}"
                        duration_ms = 0.0
                    snapshots.extend(adapter_snapshots)
                    source_statuses.append(SourceStatus(
                        adapter_name=name,
                        ok=error is None,
                        snapshot_count=len(adapter_snapshots),
                        poll_duration_ms=round(duration_ms, 2),
                        error=error,
                        timestamp=timestamp,
                    ))
            except TimeoutError:
                # Some adapters didn't finish in time — record them as timed out
                for future, adapter in futures.items():
                    if not future.done():
                        name = getattr(adapter, "name", type(adapter).__name__)
                        source_statuses.append(SourceStatus(
                            adapter_name=name,
                            ok=False,
                            snapshot_count=0,
                            poll_duration_ms=round((self.poll_timeout_seconds + 5.0) * 1000, 2),
                            error="timeout",
                            timestamp=timestamp,
                        ))
                        future.cancel()

        enriched_snapshots = self.market_state.enrich_snapshots(snapshots)
        opportunities = self.builder.build(enriched_snapshots)
        opportunities = self.opportunity_state.enrich_opportunities(opportunities)
        scored = self.scorer.rank(opportunities)
        filtered = self._filter_scored(scored)

        # Periodic state persistence
        self._scan_count += 1
        if self._state_store is not None and self._scan_count % self._flush_interval == 0:
            include_rolling_state = self._scan_count % self._rolling_flush_interval == 0
            self.request_state_flush(
                source_statuses,
                timestamp,
                include_opportunity_state=include_rolling_state,
                include_market_state=include_rolling_state,
            )

        scan_duration_ms = round((time.monotonic() - scan_start) * 1000.0, 2)

        top_scored = filtered[: self.config.top_n]

        return ScanBatch(
            timestamp=timestamp,
            snapshots=enriched_snapshots,
            opportunities=opportunities,
            scored_opportunities=top_scored,
            source_statuses=source_statuses,
            scan_duration_ms=scan_duration_ms,
        )

    def run(
        self,
        iterations: Optional[int] = None,
        sleep_seconds: Optional[float] = None,
        on_batch: Optional[Callable[[ScanBatch], None]] = None,
    ) -> List[ScanBatch]:
        batches: List[ScanBatch] = []
        remaining = iterations
        interval = self.config.scan_interval_seconds if sleep_seconds is None else sleep_seconds
        while remaining is None or remaining > 0:
            batch = self.scan_once()
            batches.append(batch)
            if on_batch is not None:
                on_batch(batch)
            if remaining is not None:
                remaining -= 1
                if remaining <= 0:
                    break
            time.sleep(interval)
        return batches

    def _flush_state(
        self,
        source_statuses: List[SourceStatus],
        timestamp: datetime,
        include_opportunity_state: bool = True,
        include_market_state: bool = True,
    ) -> None:
        """Persist rolling state to SQLite."""
        try:
            opportunity_history = self._opportunity_state_history()
            append_archive = getattr(self._state_store, "append_opportunity_state_archive", None)
            if callable(append_archive):
                append_archive(opportunity_history)
            for status in source_statuses:
                if status.ok and status.timestamp:
                    self._state_store.save_source_timestamp(status.adapter_name, status.timestamp)
            if include_opportunity_state:
                self._state_store.flush_opportunity_state(opportunity_history)
            if include_market_state:
                self._state_store.flush_market_state(self._market_state_history())
        except Exception:
            traceback.print_exc()  # Best-effort persistence, but log failures for recovery/debugging

    def request_state_flush(
        self,
        source_statuses: List[SourceStatus],
        timestamp: datetime,
        include_opportunity_state: bool = True,
        include_market_state: bool = True,
    ) -> None:
        if self._state_store is None:
            return
        if self._flush_executor is None:
            self._flush_state(
                source_statuses,
                timestamp,
                include_opportunity_state=include_opportunity_state,
                include_market_state=include_market_state,
            )
            return

        market_history = {
            key: list(points)
            for key, points in self._market_state_history().items()
        }
        opportunity_history = {
            key: list(points)
            for key, points in self._opportunity_state_history().items()
        }
        statuses = list(source_statuses)
        with self._flush_lock:
            if self._flush_future is not None and not self._flush_future.done():
                pending_include_opportunity = include_opportunity_state
                pending_include_market = include_market_state
                if self._pending_flush_payload is not None:
                    pending_include_opportunity = self._pending_flush_payload[4] or include_opportunity_state
                    pending_include_market = self._pending_flush_payload[5] or include_market_state
                self._pending_flush_payload = (
                    market_history,
                    opportunity_history,
                    statuses,
                    timestamp,
                    pending_include_opportunity,
                    pending_include_market,
                )
                return
            self._flush_future = self._flush_executor.submit(
                self._run_flush_loop,
                market_history,
                opportunity_history,
                statuses,
                timestamp,
                include_opportunity_state,
                include_market_state,
            )

    def close(self) -> None:
        if self._poll_executor is not None:
            self._poll_executor.shutdown(wait=False, cancel_futures=False)
        if self._flush_future is not None:
            try:
                self._flush_future.result(timeout=5.0)
            except Exception:
                pass
        if self._flush_executor is not None:
            self._flush_executor.shutdown(wait=False, cancel_futures=False)

    def _run_flush_loop(
        self,
        market_history: dict,
        opportunity_history: dict,
        source_statuses: list[SourceStatus],
        timestamp: datetime,
        include_opportunity_state: bool,
        include_market_state: bool,
    ) -> None:
        current = (
            market_history,
            opportunity_history,
            source_statuses,
            timestamp,
            include_opportunity_state,
            include_market_state,
        )
        while current is not None:
            _flush_state_snapshot(self._state_store, *current)
            with self._flush_lock:
                current = self._pending_flush_payload
                self._pending_flush_payload = None

    def _market_state_history(self) -> dict:
        return dict(self.market_state._history)

    def _opportunity_state_history(self) -> dict:
        return dict(self.opportunity_state._history)

    def _filter_scored(self, scored: Sequence[ScoredOpportunity]) -> List[ScoredOpportunity]:
        min_rank = _label_rank(self.config.min_label)
        return [item for item in scored if _label_rank(item.label) >= min_rank]


def _label_rank(label: ExecutionLabel) -> int:
    if label == ExecutionLabel.TRADABLE:
        return 2
    if label == ExecutionLabel.WATCH:
        return 1
    return 0


def _flush_state_snapshot(
    state_store: StateStore,
    market_history: dict,
    opportunity_history: dict,
    source_statuses: Sequence[SourceStatus],
    timestamp: datetime,
    include_opportunity_state: bool = True,
    include_market_state: bool = True,
) -> None:
    try:
        # Archive and source timestamps are small and drive /api/backtest.
        # Persist them before the heavier rolling-state table rewrites so a
        # slow snapshot flush cannot leave the dashboard history stale.
        append_archive = getattr(state_store, "append_opportunity_state_archive", None)
        if callable(append_archive):
            append_archive(opportunity_history)
        for status in source_statuses:
            if status.ok and status.timestamp:
                state_store.save_source_timestamp(status.adapter_name, status.timestamp)
        if include_opportunity_state:
            state_store.flush_opportunity_state(opportunity_history)
        if include_market_state:
            state_store.flush_market_state(market_history)
    except Exception:
        traceback.print_exc()
