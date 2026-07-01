"""SQLite persistence layer for rolling state.

Stores snapshot and opportunity history so that indicators survive process restarts.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import DefaultDict, Deque, Dict, List, Optional, Sequence, Tuple

from collections import defaultdict, deque

from .state import SnapshotStatePoint, OpportunityStatePoint, StateTrackerConfig


_SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshot_history (
    venue TEXT NOT NULL,
    asset TEXT NOT NULL,
    quote TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    mark_price REAL NOT NULL,
    funding_rate_bps REAL NOT NULL,
    oi_usd REAL NOT NULL,
    premium_bps REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_snapshot_key
    ON snapshot_history(venue, asset, quote);

CREATE INDEX IF NOT EXISTS idx_snapshot_key_time
    ON snapshot_history(venue, asset, quote, timestamp DESC);

CREATE TABLE IF NOT EXISTS opportunity_history (
    asset TEXT NOT NULL,
    quote TEXT NOT NULL,
    venue_a TEXT NOT NULL,
    venue_b TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    executable_spread_bps REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_opportunity_key
    ON opportunity_history(asset, quote, venue_a, venue_b);

CREATE TABLE IF NOT EXISTS opportunity_history_archive (
    asset TEXT NOT NULL,
    quote TEXT NOT NULL,
    venue_a TEXT NOT NULL,
    venue_b TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    executable_spread_bps REAL NOT NULL,
    PRIMARY KEY (asset, quote, venue_a, venue_b, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_opportunity_archive_time
    ON opportunity_history_archive(timestamp);

CREATE INDEX IF NOT EXISTS idx_opportunity_archive_key
    ON opportunity_history_archive(asset, quote, venue_a, venue_b);

CREATE TABLE IF NOT EXISTS source_timestamps (
    adapter_name TEXT PRIMARY KEY,
    last_seen TEXT NOT NULL
);
"""


class StateStore:
    """SQLite-backed store for rolling indicator state."""

    def __init__(self, db_path: str | Path = "arb_state.db", archive_retention_hours: float = 12.0) -> None:
        self._db_path = str(db_path)
        self._archive_retention_hours = archive_retention_hours
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = threading.RLock()

    def _recover_corrupted_db(self) -> None:
        db_path = Path(self._db_path)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        suffixes = ["", "-wal", "-shm"]
        for suffix in suffixes:
            source = Path(f"{self._db_path}{suffix}")
            if not source.exists():
                continue
            target = db_path.with_name(f"{db_path.name}.corrupt.{timestamp}{suffix}")
            source.rename(target)

    def _connect(self) -> sqlite3.Connection:
        with self._lock:
            if self._conn is None:
                try:
                    self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
                    self._conn.execute("PRAGMA journal_mode=WAL")
                    self._conn.execute("PRAGMA synchronous=NORMAL")
                    self._conn.execute("PRAGMA busy_timeout=5000")
                    self._conn.execute("PRAGMA schema_version")
                    self._conn.executescript(_SCHEMA)
                except sqlite3.DatabaseError as exc:
                    if self._conn is not None:
                        self._conn.close()
                        self._conn = None
                    if "malformed" not in str(exc).lower():
                        raise
                    self._recover_corrupted_db()
                    self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
                    self._conn.execute("PRAGMA journal_mode=WAL")
                    self._conn.execute("PRAGMA synchronous=NORMAL")
                    self._conn.execute("PRAGMA busy_timeout=5000")
                    self._conn.executescript(_SCHEMA)
            return self._conn

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    # -- snapshot history --

    def save_snapshot_points(
        self,
        key: Tuple[str, str, str],
        points: Sequence[SnapshotStatePoint],
    ) -> None:
        conn = self._connect()
        venue, asset, quote = key
        with self._lock:
            conn.execute(
                "DELETE FROM snapshot_history WHERE venue=? AND asset=? AND quote=?",
                (venue, asset, quote),
            )
            conn.executemany(
                "INSERT INTO snapshot_history (venue, asset, quote, timestamp, mark_price, funding_rate_bps, oi_usd, premium_bps) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    (venue, asset, quote, p.timestamp.isoformat(), p.mark_price, p.funding_rate_bps, p.oi_usd, p.premium_bps)
                    for p in points
                ],
            )
            conn.commit()

    def load_snapshot_history(
        self, max_points: int = 120
    ) -> DefaultDict[Tuple[str, str, str], Deque[SnapshotStatePoint]]:
        conn = self._connect()
        result: DefaultDict[Tuple[str, str, str], Deque[SnapshotStatePoint]] = defaultdict(
            lambda: deque(maxlen=max_points)
        )
        if max_points <= 0:
            return result
        cursor = conn.execute(
            """
            SELECT venue, asset, quote, timestamp, mark_price, funding_rate_bps, oi_usd, premium_bps
            FROM (
                SELECT
                    venue,
                    asset,
                    quote,
                    timestamp,
                    mark_price,
                    funding_rate_bps,
                    oi_usd,
                    premium_bps,
                    ROW_NUMBER() OVER (
                        PARTITION BY venue, asset, quote
                        ORDER BY timestamp DESC
                    ) AS row_number
                FROM snapshot_history
            )
            WHERE row_number <= ?
            ORDER BY venue, asset, quote, timestamp
            """,
            (max_points,),
        )
        for row in cursor:
            key = (row[0], row[1], row[2])
            point = SnapshotStatePoint(
                timestamp=datetime.fromisoformat(row[3]),
                mark_price=row[4],
                funding_rate_bps=row[5],
                oi_usd=row[6],
                premium_bps=row[7],
            )
            result[key].append(point)
        return result

    # -- opportunity history --

    def save_opportunity_points(
        self,
        key: Tuple[str, str, str, str],
        points: Sequence[OpportunityStatePoint],
    ) -> None:
        conn = self._connect()
        asset, quote, venue_a, venue_b = key
        with self._lock:
            conn.execute(
                "DELETE FROM opportunity_history WHERE asset=? AND quote=? AND venue_a=? AND venue_b=?",
                (asset, quote, venue_a, venue_b),
            )
            conn.executemany(
                "INSERT INTO opportunity_history (asset, quote, venue_a, venue_b, timestamp, executable_spread_bps) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (asset, quote, venue_a, venue_b, p.timestamp.isoformat(), p.executable_spread_bps)
                    for p in points
                ],
            )
            conn.commit()

    def load_opportunity_history(
        self, max_points: int = 120
    ) -> DefaultDict[Tuple[str, str, str, str], Deque[OpportunityStatePoint]]:
        conn = self._connect()
        result: DefaultDict[Tuple[str, str, str, str], Deque[OpportunityStatePoint]] = defaultdict(
            lambda: deque(maxlen=max_points)
        )
        cursor = conn.execute(
            "SELECT asset, quote, venue_a, venue_b, timestamp, executable_spread_bps "
            "FROM opportunity_history ORDER BY timestamp"
        )
        for row in cursor:
            key = (row[0], row[1], row[2], row[3])
            point = OpportunityStatePoint(
                timestamp=datetime.fromisoformat(row[4]),
                executable_spread_bps=row[5],
            )
            result[key].append(point)
        for key in result:
            while len(result[key]) > max_points:
                result[key].popleft()
        return result

    def load_opportunity_history_archive(
        self,
        max_points: int = 3600,
        lookback_hours: float | None = None,
    ) -> DefaultDict[Tuple[str, str, str, str], Deque[OpportunityStatePoint]]:
        conn = self._connect()
        result: DefaultDict[Tuple[str, str, str, str], Deque[OpportunityStatePoint]] = defaultdict(
            lambda: deque(maxlen=max_points)
        )
        params: tuple = ()
        query = (
            "SELECT asset, quote, venue_a, venue_b, timestamp, executable_spread_bps "
            "FROM opportunity_history_archive"
        )
        if lookback_hours is not None and lookback_hours > 0:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
            query += " WHERE timestamp >= ?"
            params = (cutoff,)
        query += " ORDER BY timestamp"
        cursor = conn.execute(query, params)
        for row in cursor:
            key = (row[0], row[1], row[2], row[3])
            point = OpportunityStatePoint(
                timestamp=datetime.fromisoformat(row[4]),
                executable_spread_bps=row[5],
            )
            result[key].append(point)
        return result

    # -- source timestamps --

    def save_source_timestamp(self, adapter_name: str, timestamp: datetime) -> None:
        conn = self._connect()
        with self._lock:
            conn.execute(
                "INSERT OR REPLACE INTO source_timestamps (adapter_name, last_seen) VALUES (?, ?)",
                (adapter_name, timestamp.isoformat()),
            )
            conn.commit()

    def load_source_timestamps(self) -> Dict[str, datetime]:
        conn = self._connect()
        cursor = conn.execute("SELECT adapter_name, last_seen FROM source_timestamps")
        return {row[0]: datetime.fromisoformat(row[1]) for row in cursor}

    def count_snapshot_series(self, lookback_hours: float | None = None) -> int:
        conn = self._connect()
        params: tuple = ()
        query = "SELECT COUNT(*) FROM (SELECT 1 FROM snapshot_history"
        if lookback_hours is not None and lookback_hours > 0:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
            query += " WHERE timestamp >= ?"
            params = (cutoff,)
        query += " GROUP BY venue, asset, quote)"
        cursor = conn.execute(query, params)
        row = cursor.fetchone()
        return int(row[0]) if row else 0

    # -- bulk flush from trackers --

    def flush_market_state(self, history: dict) -> None:
        """Save all snapshot history from MarketStateTracker._history."""
        conn = self._connect()
        rows = []
        for (venue, asset, quote), points in history.items():
            rows.extend(
                (
                    venue,
                    asset,
                    quote,
                    point.timestamp.isoformat(),
                    point.mark_price,
                    point.funding_rate_bps,
                    point.oi_usd,
                    point.premium_bps,
                )
                for point in points
            )
        with self._lock:
            conn.execute("DELETE FROM snapshot_history")
            if rows:
                conn.executemany(
                    "INSERT INTO snapshot_history (venue, asset, quote, timestamp, mark_price, funding_rate_bps, oi_usd, premium_bps) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    rows,
                )
            conn.commit()

    def flush_opportunity_state(self, history: dict) -> None:
        """Save all opportunity history from OpportunityStateTracker._history."""
        conn = self._connect()
        rows = []
        for (asset, quote, venue_a, venue_b), points in history.items():
            rows.extend(
                (
                    asset,
                    quote,
                    venue_a,
                    venue_b,
                    point.timestamp.isoformat(),
                    point.executable_spread_bps,
                )
                for point in points
            )
        with self._lock:
            conn.execute("DELETE FROM opportunity_history")
            if rows:
                conn.executemany(
                    "INSERT INTO opportunity_history (asset, quote, venue_a, venue_b, timestamp, executable_spread_bps) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    rows,
                )
            conn.commit()

    def append_opportunity_state_archive(self, history: dict) -> None:
        conn = self._connect()
        rows = []
        for (asset, quote, venue_a, venue_b), points in history.items():
            if not points:
                continue
            point = points[-1]
            rows.append(
                (
                    asset,
                    quote,
                    venue_a,
                    venue_b,
                    point.timestamp.isoformat(),
                    point.executable_spread_bps,
                )
            )
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=self._archive_retention_hours)).isoformat()
        with self._lock:
            if rows:
                conn.executemany(
                    "INSERT OR REPLACE INTO opportunity_history_archive (asset, quote, venue_a, venue_b, timestamp, executable_spread_bps) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    rows,
                )
            conn.execute("DELETE FROM opportunity_history_archive WHERE timestamp < ?", (cutoff,))
            conn.commit()
