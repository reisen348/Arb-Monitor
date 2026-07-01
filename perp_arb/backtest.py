from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import mean, pstdev
from typing import Iterable, List, Sequence

from .persistence import StateStore
from .state import OpportunityStatePoint


@dataclass(frozen=True)
class OpportunityHistorySummary:
    asset: str
    quote: str
    venue_a: str
    venue_b: str
    samples: int
    latest_timestamp: datetime
    latest_spread_bps: float
    mean_spread_bps: float
    spread_pstdev_bps: float
    latest_zscore: float | None
    signal_count: int = 0
    hit_count: int = 0
    hit_rate: float = 0.0
    conservative_hit_count: int = 0
    conservative_hit_rate: float = 0.0


@dataclass(frozen=True)
class BacktestSummary:
    db_path: str
    snapshot_series_count: int
    opportunity_series_count: int
    source_count: int
    candidate_count: int
    total_signal_count: int
    total_hit_count: int
    total_hit_rate: float
    total_conservative_hit_count: int
    total_conservative_hit_rate: float
    asset_rankings: Sequence[AggregateHistorySummary]
    venue_pair_rankings: Sequence[AggregateHistorySummary]
    candidates: Sequence[OpportunityHistorySummary]


@dataclass(frozen=True)
class AggregateHistorySummary:
    key: str
    series_count: int
    candidate_count: int
    signal_count: int
    hit_count: int
    hit_rate: float
    conservative_hit_count: int
    conservative_hit_rate: float
    max_abs_latest_zscore: float


def summarize_state_store(
    db_path: str,
    max_points: int = 600,
    top_n: int = 20,
    min_samples: int = 30,
    signal_zscore: float = 2.0,
    min_signal_spread_bps: float = 8.0,
    forward_points: int = 5,
    reversion_ratio: float = 0.5,
    lookback_hours: float | None = None,
) -> BacktestSummary:
    store = StateStore(db_path)
    try:
        snapshot_series_count = store.count_snapshot_series(lookback_hours=lookback_hours)
        loaded_from_archive = True
        opportunity_history = store.load_opportunity_history_archive(
            max_points=max_points,
            lookback_hours=lookback_hours,
        )
        if not opportunity_history:
            loaded_from_archive = False
            opportunity_history = store.load_opportunity_history(max_points=max_points)
        source_timestamps = store.load_source_timestamps()
    finally:
        store.close()

    if lookback_hours is not None and lookback_hours > 0 and not loaded_from_archive:
        opportunity_history = _filter_opportunity_history_by_lookback(opportunity_history, lookback_hours)

    candidates: List[OpportunityHistorySummary] = []
    total_signal_count = 0
    total_hit_count = 0
    total_conservative_hit_count = 0
    for key, points in opportunity_history.items():
        summary = _summarize_opportunity_points(
            key,
            points,
            min_samples=min_samples,
            signal_zscore=signal_zscore,
            min_signal_spread_bps=min_signal_spread_bps,
            forward_points=forward_points,
            reversion_ratio=reversion_ratio,
        )
        if summary is None:
            continue
        total_signal_count += summary.signal_count
        total_hit_count += summary.hit_count
        total_conservative_hit_count += summary.conservative_hit_count
        if (
            summary.samples >= min_samples
            and summary.latest_zscore is not None
            and _is_signal(
                latest_spread=summary.latest_spread_bps,
                mean_spread=summary.mean_spread_bps,
                latest_zscore=summary.latest_zscore,
                signal_zscore=signal_zscore,
                min_signal_spread_bps=min_signal_spread_bps,
            )
        ):
            candidates.append(summary)

    candidates.sort(
        key=lambda item: (
            -(abs(item.latest_zscore or 0.0)),
            -abs(item.latest_spread_bps),
            -item.samples,
        )
    )
    asset_rankings = _aggregate_rankings(candidates, lambda item: f"{item.asset}/{item.quote}")
    venue_pair_rankings = _aggregate_rankings(candidates, lambda item: f"{item.venue_a}<->{item.venue_b}")

    return BacktestSummary(
        db_path=db_path,
        snapshot_series_count=snapshot_series_count,
        opportunity_series_count=len(opportunity_history),
        source_count=len(source_timestamps),
        candidate_count=len(candidates),
        total_signal_count=total_signal_count,
        total_hit_count=total_hit_count,
        total_hit_rate=(total_hit_count / total_signal_count) if total_signal_count else 0.0,
        total_conservative_hit_count=total_conservative_hit_count,
        total_conservative_hit_rate=(total_conservative_hit_count / total_signal_count) if total_signal_count else 0.0,
        asset_rankings=asset_rankings[:top_n],
        venue_pair_rankings=venue_pair_rankings[:top_n],
        candidates=candidates[:top_n],
    )


def _aggregate_rankings(
    candidates: Sequence[OpportunityHistorySummary],
    key_fn,
) -> List[AggregateHistorySummary]:
    groups = defaultdict(list)
    for item in candidates:
        groups[key_fn(item)].append(item)

    ranked: List[AggregateHistorySummary] = []
    for key, items in groups.items():
        signal_count = sum(item.signal_count for item in items)
        hit_count = sum(item.hit_count for item in items)
        conservative_hit_count = sum(item.conservative_hit_count for item in items)
        ranked.append(
            AggregateHistorySummary(
                key=key,
                series_count=len(items),
                candidate_count=len(items),
                signal_count=signal_count,
                hit_count=hit_count,
                hit_rate=(hit_count / signal_count) if signal_count else 0.0,
                conservative_hit_count=conservative_hit_count,
                conservative_hit_rate=(conservative_hit_count / signal_count) if signal_count else 0.0,
                max_abs_latest_zscore=max(abs(item.latest_zscore or 0.0) for item in items),
            )
        )
    ranked.sort(
        key=lambda item: (
            -item.candidate_count,
            -item.signal_count,
            -item.hit_rate,
            -item.max_abs_latest_zscore,
        )
    )
    return ranked


def _filter_snapshot_history_by_lookback(history: dict, lookback_hours: float) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    filtered = {}
    for key, points in history.items():
        kept = [point for point in points if point.timestamp >= cutoff]
        if kept:
            filtered[key] = kept
    return filtered


def _filter_opportunity_history_by_lookback(history: dict, lookback_hours: float) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    filtered = {}
    for key, points in history.items():
        kept = [point for point in points if point.timestamp >= cutoff]
        if kept:
            filtered[key] = kept
    return filtered


def _summarize_opportunity_points(
    key: tuple[str, str, str, str],
    points: Sequence[OpportunityStatePoint],
    min_samples: int,
    signal_zscore: float,
    min_signal_spread_bps: float,
    forward_points: int,
    reversion_ratio: float,
) -> OpportunityHistorySummary | None:
    if not points:
        return None
    spreads = [point.executable_spread_bps for point in points]
    latest = spreads[-1]
    history = spreads[:-1]
    latest_zscore = None
    if len(history) >= 2:
        baseline_mean = mean(history)
        sigma = pstdev(history)
        if sigma <= 1e-9:
            latest_zscore = 0.0 if abs(latest - baseline_mean) <= 1e-9 else float("inf")
        else:
            latest_zscore = (latest - baseline_mean) / sigma
    asset, quote, venue_a, venue_b = key
    signal_count, hit_count, conservative_hit_count = _evaluate_signal_hits(
        spreads,
        min_samples=min_samples,
        signal_zscore=signal_zscore,
        min_signal_spread_bps=min_signal_spread_bps,
        forward_points=forward_points,
        reversion_ratio=reversion_ratio,
    )
    return OpportunityHistorySummary(
        asset=asset,
        quote=quote,
        venue_a=venue_a,
        venue_b=venue_b,
        samples=len(points),
        latest_timestamp=points[-1].timestamp,
        latest_spread_bps=latest,
        mean_spread_bps=mean(spreads),
        spread_pstdev_bps=pstdev(spreads) if len(spreads) >= 2 else 0.0,
        latest_zscore=latest_zscore,
        signal_count=signal_count,
        hit_count=hit_count,
        hit_rate=(hit_count / signal_count) if signal_count else 0.0,
        conservative_hit_count=conservative_hit_count,
        conservative_hit_rate=(conservative_hit_count / signal_count) if signal_count else 0.0,
    )


def _evaluate_signal_hits(
    spreads: Sequence[float],
    min_samples: int,
    signal_zscore: float,
    min_signal_spread_bps: float,
    forward_points: int,
    reversion_ratio: float,
) -> tuple[int, int, int]:
    signal_count = 0
    hit_count = 0
    conservative_hit_count = 0
    if len(spreads) < min_samples + 1:
        return signal_count, hit_count, conservative_hit_count
    index = min_samples
    while index < len(spreads) - 1:
        baseline = list(spreads[:index])
        current = spreads[index]
        zscore = _zscore_against_history(current, baseline)
        baseline_mean = mean(baseline) if baseline else 0.0
        if zscore is None or not _is_signal(
            latest_spread=current,
            mean_spread=baseline_mean,
            latest_zscore=zscore,
            signal_zscore=signal_zscore,
            min_signal_spread_bps=min_signal_spread_bps,
        ):
            index += 1
            continue
        signal_count += 1
        signal_deviation = abs(current - baseline_mean)
        target_abs_deviation = signal_deviation * reversion_ratio
        target_abs_spread = abs(current) * reversion_ratio
        future_window = spreads[index + 1:index + 1 + max(forward_points, 1)]
        if future_window and min(abs(value - baseline_mean) for value in future_window) <= target_abs_deviation:
            hit_count += 1
        if future_window and min(abs(value) for value in future_window) <= target_abs_spread:
            conservative_hit_count += 1
        index += max(forward_points, 1)
    return signal_count, hit_count, conservative_hit_count


def _zscore_against_history(value: float, history: Sequence[float]) -> float | None:
    if len(history) < 2:
        return None
    baseline_mean = mean(history)
    sigma = pstdev(history)
    if sigma <= 1e-9:
        return 0.0 if abs(value - baseline_mean) <= 1e-9 else float("inf")
    return (value - baseline_mean) / sigma


def _is_signal(
    latest_spread: float,
    mean_spread: float,
    latest_zscore: float | None,
    signal_zscore: float,
    min_signal_spread_bps: float,
) -> bool:
    if latest_zscore is not None and abs(latest_zscore) >= signal_zscore:
        return True
    return abs(latest_spread - mean_spread) >= min_signal_spread_bps


def build_backtest_payload(summary: BacktestSummary) -> dict:
    return {
        "db_path": summary.db_path,
        "snapshot_series_count": summary.snapshot_series_count,
        "opportunity_series_count": summary.opportunity_series_count,
        "source_count": summary.source_count,
        "candidate_count": summary.candidate_count,
        "total_signal_count": summary.total_signal_count,
        "total_hit_count": summary.total_hit_count,
        "total_hit_rate": round(summary.total_hit_rate, 4),
        "total_conservative_hit_count": summary.total_conservative_hit_count,
        "total_conservative_hit_rate": round(summary.total_conservative_hit_rate, 4),
        "dashboard_summary": _build_backtest_dashboard_summary(summary),
        "metric_cards": _build_backtest_metric_cards(summary),
        "asset_rankings": [
            {
                "key": item.key,
                "series_count": item.series_count,
                "candidate_count": item.candidate_count,
                "signal_count": item.signal_count,
                "hit_count": item.hit_count,
                "hit_rate": round(item.hit_rate, 4),
                "conservative_hit_count": item.conservative_hit_count,
                "conservative_hit_rate": round(item.conservative_hit_rate, 4),
                "max_abs_latest_zscore": round(item.max_abs_latest_zscore, 4),
            }
            for item in summary.asset_rankings
        ],
        "venue_pair_rankings": [
            {
                "key": item.key,
                "series_count": item.series_count,
                "candidate_count": item.candidate_count,
                "signal_count": item.signal_count,
                "hit_count": item.hit_count,
                "hit_rate": round(item.hit_rate, 4),
                "conservative_hit_count": item.conservative_hit_count,
                "conservative_hit_rate": round(item.conservative_hit_rate, 4),
                "max_abs_latest_zscore": round(item.max_abs_latest_zscore, 4),
            }
            for item in summary.venue_pair_rankings
        ],
        "candidate_cards": [
            _build_candidate_card(item)
            for item in summary.candidates
        ],
        "candidates": [
            {
                "asset": item.asset,
                "quote": item.quote,
                "venue_a": item.venue_a,
                "venue_b": item.venue_b,
                "samples": item.samples,
                "latest_timestamp": item.latest_timestamp.isoformat(),
                "latest_spread_bps": round(item.latest_spread_bps, 4),
                "mean_spread_bps": round(item.mean_spread_bps, 4),
                "spread_pstdev_bps": round(item.spread_pstdev_bps, 4),
                "latest_zscore": round(item.latest_zscore, 4) if item.latest_zscore is not None else None,
                "signal_count": item.signal_count,
                "hit_count": item.hit_count,
                "hit_rate": round(item.hit_rate, 4),
                "conservative_hit_count": item.conservative_hit_count,
                "conservative_hit_rate": round(item.conservative_hit_rate, 4),
            }
            for item in summary.candidates
        ],
    }


def _build_backtest_dashboard_summary(summary: BacktestSummary) -> dict:
    top_candidate = summary.candidates[0] if summary.candidates else None
    top_asset = summary.asset_rankings[0] if summary.asset_rankings else None
    top_venue_pair = summary.venue_pair_rankings[0] if summary.venue_pair_rankings else None
    return {
        "top_candidate": (
            {
                "asset": top_candidate.asset,
                "quote": top_candidate.quote,
                "venues": [top_candidate.venue_a, top_candidate.venue_b],
                "latest_zscore": round(top_candidate.latest_zscore, 4) if top_candidate.latest_zscore is not None else None,
                "latest_spread_bps": round(top_candidate.latest_spread_bps, 4),
                "hit_rate": round(top_candidate.hit_rate, 4),
                "conservative_hit_rate": round(top_candidate.conservative_hit_rate, 4),
            }
            if top_candidate
            else None
        ),
        "top_asset": (
            {
                "key": top_asset.key,
                "candidate_count": top_asset.candidate_count,
                "signal_count": top_asset.signal_count,
                "hit_rate": round(top_asset.hit_rate, 4),
                "conservative_hit_rate": round(top_asset.conservative_hit_rate, 4),
            }
            if top_asset
            else None
        ),
        "top_venue_pair": (
            {
                "key": top_venue_pair.key,
                "candidate_count": top_venue_pair.candidate_count,
                "signal_count": top_venue_pair.signal_count,
                "hit_rate": round(top_venue_pair.hit_rate, 4),
                "conservative_hit_rate": round(top_venue_pair.conservative_hit_rate, 4),
            }
            if top_venue_pair
            else None
        ),
    }


def _build_backtest_metric_cards(summary: BacktestSummary) -> List[dict]:
    return [
        {"label": "候选机会", "value": summary.candidate_count},
        {"label": "历史信号", "value": summary.total_signal_count},
        {"label": "历史命中", "value": summary.total_hit_count},
        {"label": "历史命中率", "value": round(summary.total_hit_rate, 4)},
        {"label": "保守命中率", "value": round(summary.total_conservative_hit_rate, 4)},
        {"label": "热点资产数", "value": len(summary.asset_rankings)},
        {"label": "热点配对数", "value": len(summary.venue_pair_rankings)},
    ]


def _build_candidate_card(item: OpportunityHistorySummary) -> dict:
    return {
        "title": f"{item.asset}/{item.quote} · {item.venue_a} vs {item.venue_b}",
        "thesis": (
            f"当前 spread {item.latest_spread_bps:.2f} bps，"
            f"相对历史均值 {item.mean_spread_bps:.2f} bps 的偏离 z-score 为 {item.latest_zscore:.2f}。"
            if item.latest_zscore is not None
            else f"当前 spread {item.latest_spread_bps:.2f} bps，历史样本不足以形成可靠 z-score。"
        ),
        "stats": [
            f"样本 {item.samples}",
            f"信号 {item.signal_count}",
            f"命中率 {item.hit_rate:.2%}",
            f"保守命中率 {item.conservative_hit_rate:.2%}",
            f"sigma {item.spread_pstdev_bps:.2f}",
        ],
        "monitoring_points": [
            f"当前 spread {item.latest_spread_bps:.2f} bps",
            f"历史均值 {item.mean_spread_bps:.2f} bps",
            f"最新 z-score {(item.latest_zscore or 0.0):.2f}",
            f"最新时间 {item.latest_timestamp.isoformat()}",
        ],
    }


def format_backtest_text(summary: BacktestSummary) -> str:
    lines = [
        f"db={summary.db_path}",
        f"snapshot_series={summary.snapshot_series_count} opportunity_series={summary.opportunity_series_count} sources={summary.source_count}",
        f"candidate_count={summary.candidate_count}",
        f"signals={summary.total_signal_count} hits={summary.total_hit_count} hit_rate={summary.total_hit_rate:.2%} conservative_hit_rate={summary.total_conservative_hit_rate:.2%}",
    ]
    if summary.asset_rankings:
        lines.append("asset_rankings:")
        for item in summary.asset_rankings:
            lines.append(
                f"  {item.key} candidates={item.candidate_count} signals={item.signal_count} "
                f"hit_rate={item.hit_rate:.2%} conservative_hit_rate={item.conservative_hit_rate:.2%} max_z={item.max_abs_latest_zscore:.2f}"
            )
    if summary.venue_pair_rankings:
        lines.append("venue_pair_rankings:")
        for item in summary.venue_pair_rankings:
            lines.append(
                f"  {item.key} candidates={item.candidate_count} signals={item.signal_count} "
                f"hit_rate={item.hit_rate:.2%} conservative_hit_rate={item.conservative_hit_rate:.2%} max_z={item.max_abs_latest_zscore:.2f}"
            )
    for index, item in enumerate(summary.candidates, start=1):
        lines.append(
            f"{index:02d}. {item.asset}/{item.quote} {item.venue_a}<->{item.venue_b} "
            f"samples={item.samples} latest={item.latest_spread_bps:.2f}bps "
            f"mean={item.mean_spread_bps:.2f}bps sigma={item.spread_pstdev_bps:.2f} "
            f"z={item.latest_zscore:.2f} signals={item.signal_count} hit_rate={item.hit_rate:.2%} conservative_hit_rate={item.conservative_hit_rate:.2%}"
        )
    return "\n".join(lines)
