from __future__ import annotations

import asyncio
import json
import threading
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Deque, DefaultDict, Dict, List, Optional, Sequence

import websockets

from .market_data import MarketDataAdapter, MarketSnapshot


@dataclass(frozen=True)
class MicroPricePoint:
    timestamp: datetime
    mid_price: float


class BaseWebsocketAdapter(MarketDataAdapter, ABC):
    def __init__(
        self,
        name: str,
        reconnect_seconds: float = 1.0,
        jump_threshold_bps: float = 3.0,
        micro_jump_window_seconds: float = 5.0,
        shock_jump_window_seconds: float = 60.0,
        max_micro_points: int = 2048,
    ) -> None:
        self.name = name
        self.reconnect_seconds = reconnect_seconds
        self.jump_threshold_bps = jump_threshold_bps
        self.micro_jump_window_seconds = micro_jump_window_seconds
        self.shock_jump_window_seconds = shock_jump_window_seconds
        self._snapshots: Dict[str, MarketSnapshot] = {}
        self._lock = threading.RLock()
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._started = False
        self._micro_prices: DefaultDict[str, Deque[MicroPricePoint]] = defaultdict(
            lambda: deque(maxlen=max_micro_points)
        )

    def poll(self) -> Sequence[MarketSnapshot]:
        self.start()
        with self._lock:
            return [self._snapshots[key] for key in sorted(self._snapshots)]

    def start(self) -> None:
        with self._lock:
            if not self._snapshots:
                self._seed_locked()
            if self._started:
                return
            self._started = True
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=2.0)
        self._thread = None
        self._started = False

    def _seed_locked(self) -> None:
        for snapshot in self._seed_snapshots():
            key = self._snapshot_key(snapshot)
            self._snapshots[key] = snapshot
            self._record_micro_price(key, self._mid_price(snapshot), snapshot.timestamp)

    def _run_loop(self) -> None:
        asyncio.run(self._stream_forever())

    async def _stream_forever(self) -> None:
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(self._websocket_url(), ping_interval=20, ping_timeout=20) as websocket:
                    for message in self._subscription_messages():
                        await websocket.send(json.dumps(message))
                    while not self._stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            continue
                        self._handle_payload(json.loads(raw))
            except Exception:
                if self._stop_event.is_set():
                    break
                await asyncio.sleep(self.reconnect_seconds)

    def _handle_payload(self, payload: object) -> None:
        updates = self._message_updates(payload)
        if not updates:
            return
        with self._lock:
            for snapshot in updates:
                key = self._snapshot_key(snapshot)
                enriched = self._annotate_microstructure(key, snapshot)
                self._snapshots[key] = enriched

    def _get_snapshot(self, key: str) -> Optional[MarketSnapshot]:
        with self._lock:
            return self._snapshots.get(key)

    @staticmethod
    def _with_timestamp(snapshot: MarketSnapshot, timestamp: Optional[datetime]) -> MarketSnapshot:
        ts = timestamp or datetime.now(timezone.utc)
        staleness_ms = max(0.0, datetime.now(timezone.utc).timestamp() * 1000.0 - ts.timestamp() * 1000.0)
        return replace(snapshot, timestamp=ts, staleness_ms=staleness_ms)

    def _annotate_microstructure(self, key: str, snapshot: MarketSnapshot) -> MarketSnapshot:
        timestamp = snapshot.timestamp or datetime.now(timezone.utc)
        mid_price = self._mid_price(snapshot)
        self._record_micro_price(key, mid_price, timestamp)
        points = list(self._micro_prices[key])
        if len(points) < 2:
            return snapshot
        micro_jump_frequency = self._windowed_jump_frequency(points, self.micro_jump_window_seconds)
        shock_jump_frequency = self._windowed_jump_frequency(points, self.shock_jump_window_seconds)
        return replace(
            snapshot,
            micro_jump_frequency=micro_jump_frequency,
            shock_jump_frequency=shock_jump_frequency,
            jump_frequency=max(micro_jump_frequency, shock_jump_frequency),
        )

    def _record_micro_price(self, key: str, mid_price: float, timestamp: Optional[datetime]) -> None:
        ts = timestamp or datetime.now(timezone.utc)
        history = self._micro_prices[key]
        history.append(MicroPricePoint(timestamp=ts, mid_price=mid_price))
        cutoff = ts.timestamp() - max(self.micro_jump_window_seconds, self.shock_jump_window_seconds)
        while history and history[0].timestamp.timestamp() < cutoff:
            history.popleft()

    def _windowed_jump_frequency(self, points: Sequence[MicroPricePoint], window_seconds: float) -> float:
        if len(points) < 2:
            return 0.0
        latest = points[-1].timestamp.timestamp()
        filtered = [point for point in points if point.timestamp.timestamp() >= latest - window_seconds]
        if len(filtered) < 2:
            return 0.0
        moves = []
        for previous, current in zip(filtered, filtered[1:]):
            if previous.mid_price <= 0 or current.mid_price <= 0:
                continue
            move_bps = abs((current.mid_price - previous.mid_price) / previous.mid_price * 10_000.0)
            moves.append(move_bps)
        if not moves:
            return 0.0
        jumps = sum(1 for move_bps in moves if move_bps >= self.jump_threshold_bps)
        return jumps / len(moves) * 100.0

    @staticmethod
    def _mid_price(snapshot: MarketSnapshot) -> float:
        if snapshot.best_bid > 0 and snapshot.best_ask > 0:
            return (snapshot.best_bid + snapshot.best_ask) / 2.0
        return snapshot.mark_price

    @abstractmethod
    def _seed_snapshots(self) -> Sequence[MarketSnapshot]:
        raise NotImplementedError

    @abstractmethod
    def _snapshot_key(self, snapshot: MarketSnapshot) -> str:
        raise NotImplementedError

    @abstractmethod
    def _websocket_url(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def _subscription_messages(self) -> Sequence[dict]:
        raise NotImplementedError

    @abstractmethod
    def _message_updates(self, payload: object) -> Sequence[MarketSnapshot]:
        raise NotImplementedError
