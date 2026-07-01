from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Iterable

from .models import ExecutionLabel, OpportunityBucket, ScoredOpportunity


@dataclass(frozen=True)
class PracticeGateConfig:
    min_expected_profit_bps: float = 3.0
    min_dislocation_zscore: float = 2.0
    min_carry_edge_bps: float = 3.0
    max_carry_entry_edge_bps: float = 5.0
    required_consecutive_scans: int = 3
    paper_notional_usd: float = 100.0
    max_active_trades: int = 5
    max_hold_seconds: float = 15.0 * 60.0
    stop_loss_bps: float = -5.0
    allowed_assets: frozenset[str] = frozenset({"ETH"})
    allowed_venues: frozenset[str] = frozenset({"bybit", "lighter", "nado", "okx"})
    forbidden_tags: frozenset[str] = frozenset({
        "freshness_risk",
        "regime_unstable",
        "liquidation_risk",
        "micro_execution_risk",
        "carry_squeeze_risk",
    })


@dataclass
class _SignalStreak:
    count: int = 0
    last_seen: float = 0.0


@dataclass
class _PaperTrade:
    key: str
    asset: str
    quote: str
    venue_a: str
    venue_b: str
    direction: str
    bucket: str
    opened_at: float
    entry_edge_bps: float
    target_edge_bps: float
    entry_cost_bps: float
    notional_usd: float
    consecutive_scans: int
    current_edge_bps: float
    gross_pnl_bps: float = 0.0
    net_pnl_bps: float = 0.0
    net_pnl_usd: float = 0.0
    status: str = "open"
    close_reason: str = ""
    closed_at: float | None = None

    def as_dict(self) -> dict:
        return {
            "key": self.key,
            "asset": self.asset,
            "quote": self.quote,
            "venue_a": self.venue_a,
            "venue_b": self.venue_b,
            "direction": self.direction,
            "bucket": self.bucket,
            "opened_at": self.opened_at,
            "age_seconds": round((self.closed_at or time.time()) - self.opened_at, 1),
            "entry_edge_bps": round(self.entry_edge_bps, 4),
            "target_edge_bps": round(self.target_edge_bps, 4),
            "current_edge_bps": round(self.current_edge_bps, 4),
            "entry_cost_bps": round(self.entry_cost_bps, 4),
            "gross_pnl_bps": round(self.gross_pnl_bps, 4),
            "net_pnl_bps": round(self.net_pnl_bps, 4),
            "net_pnl_usd": round(self.net_pnl_usd, 4),
            "notional_usd": round(self.notional_usd, 2),
            "consecutive_scans": self.consecutive_scans,
            "status": self.status,
            "close_reason": self.close_reason,
            "closed_at": self.closed_at,
        }


class PracticeTracker:
    """Convert scored opportunities into a conservative paper-trade ledger."""

    def __init__(self, config: PracticeGateConfig | None = None) -> None:
        self.config = config or PracticeGateConfig()
        self._streaks: dict[str, _SignalStreak] = {}
        self._active: dict[str, _PaperTrade] = {}
        self._completed: list[_PaperTrade] = []

    def update(self, scored_opportunities: Iterable[ScoredOpportunity], now: float | None = None) -> dict:
        now = now if now is not None else time.time()
        scored_list = list(scored_opportunities)
        candidates = []
        seen_keys: set[str] = set()

        for scored in scored_list:
            key = self._signal_key(scored)
            seen_keys.add(key)
            reasons = self._gate_reasons(scored)
            passed = not reasons
            streak = self._streaks.setdefault(key, _SignalStreak())
            if passed:
                streak.count += 1
                streak.last_seen = now
            else:
                streak.count = 0
                streak.last_seen = now

            ready = passed and streak.count >= self.config.required_consecutive_scans
            candidate = self._candidate_dict(scored, key, reasons, streak.count, ready)
            candidates.append(candidate)
            if ready:
                self._open_if_needed(scored, key, streak.count, now)

        stale_keys = [key for key in self._streaks if key not in seen_keys]
        for key in stale_keys:
            self._streaks.pop(key, None)

        current_by_key = {self._signal_key(scored): scored for scored in scored_list}
        self._update_active_trades(current_by_key, now)
        eligible = [item for item in candidates if item["gate_passed"]]
        return {
            "mode": "paper",
            "policy": {
                "min_expected_profit_bps": self.config.min_expected_profit_bps,
                "min_dislocation_zscore": self.config.min_dislocation_zscore,
                "required_consecutive_scans": self.config.required_consecutive_scans,
                "paper_notional_usd": self.config.paper_notional_usd,
                "allowed_assets": sorted(self.config.allowed_assets),
                "allowed_venues": sorted(self.config.allowed_venues),
            },
            "summary": {
                "eligible_count": len(eligible),
                "ready_count": sum(1 for item in candidates if item["ready"]),
                "active_count": len(self._active),
                "completed_count": len(self._completed),
            },
            "candidates": candidates[:10],
            "active_trades": [trade.as_dict() for trade in self._active.values()],
            "recent_completed": [trade.as_dict() for trade in self._completed[-10:]],
        }

    def _gate_reasons(self, scored: ScoredOpportunity) -> list[str]:
        opp = scored.opportunity
        br = scored.breakdown
        reasons: list[str] = []
        if scored.label not in {ExecutionLabel.WATCH, ExecutionLabel.TRADABLE}:
            reasons.append("label_not_actionable")
        if scored.block_reasons:
            reasons.append("blocked_by_score")
        if opp.asset.upper() not in self.config.allowed_assets:
            reasons.append("asset_not_whitelisted")
        venues = {opp.leg_a.venue, opp.leg_b.venue}
        if not venues.issubset(self.config.allowed_venues):
            reasons.append("venue_pair_not_whitelisted")
        if br.expected_profit_bps < self.config.min_expected_profit_bps:
            reasons.append("profit_below_practice_gate")
        forbidden = sorted(set(scored.tags).intersection(self.config.forbidden_tags))
        if forbidden:
            reasons.append("forbidden_risk_tag:" + ",".join(forbidden))
        if scored.risk_flags.stale_data:
            reasons.append("stale_data")
        if scored.risk_flags.jump_risk:
            reasons.append("jump_risk")
        if scored.risk_flags.exit_risk:
            reasons.append("exit_risk")
        if br.bucket_type == OpportunityBucket.DISLOCATION:
            if abs(opp.spread_zscore) < self.config.min_dislocation_zscore:
                reasons.append("zscore_below_practice_gate")
        else:
            if br.carry_edge_bps < self.config.min_carry_edge_bps:
                reasons.append("carry_edge_below_practice_gate")
            if abs(br.entry_edge_bps) > self.config.max_carry_entry_edge_bps:
                reasons.append("carry_entry_spread_too_wide")
        return reasons

    def _open_if_needed(self, scored: ScoredOpportunity, key: str, consecutive_scans: int, now: float) -> None:
        if key in self._active:
            return
        if len(self._active) >= self.config.max_active_trades:
            return
        br = scored.breakdown
        opp = scored.opportunity
        notional = min(max(self.config.paper_notional_usd, 1.0), max(opp.notional_usd, 1.0))
        entry_cost_bps = max(
            0.0,
            br.fee_bps + opp.slippage_bps + br.impact_buffer_bps + br.exit_cost_buffer_bps + br.oracle_risk_buffer_bps,
        )
        target_edge_bps = max(0.0, br.entry_edge_bps * 0.5)
        self._active[key] = _PaperTrade(
            key=key,
            asset=opp.asset,
            quote=opp.quote,
            venue_a=opp.leg_a.venue,
            venue_b=opp.leg_b.venue,
            direction=br.direction,
            bucket=br.bucket_type.value,
            opened_at=now,
            entry_edge_bps=br.entry_edge_bps,
            target_edge_bps=target_edge_bps,
            entry_cost_bps=entry_cost_bps,
            notional_usd=notional,
            consecutive_scans=consecutive_scans,
            current_edge_bps=br.entry_edge_bps,
        )

    def _update_active_trades(self, current_by_key: dict[str, ScoredOpportunity], now: float) -> None:
        for key, trade in list(self._active.items()):
            scored = current_by_key.get(key)
            if scored is not None:
                trade.current_edge_bps = scored.breakdown.entry_edge_bps
                trade.gross_pnl_bps = trade.entry_edge_bps - trade.current_edge_bps
                trade.net_pnl_bps = trade.gross_pnl_bps - trade.entry_cost_bps
                trade.net_pnl_usd = trade.notional_usd * trade.net_pnl_bps / 10_000.0
            age = now - trade.opened_at
            close_reason = ""
            if trade.current_edge_bps <= trade.target_edge_bps:
                close_reason = "target_reversion"
            elif trade.gross_pnl_bps <= self.config.stop_loss_bps:
                close_reason = "paper_stop_loss"
            elif age >= self.config.max_hold_seconds:
                close_reason = "timeout"
            if close_reason:
                trade.status = "closed"
                trade.close_reason = close_reason
                trade.closed_at = now
                self._completed.append(trade)
                self._active.pop(key, None)

    def _candidate_dict(
        self,
        scored: ScoredOpportunity,
        key: str,
        reasons: list[str],
        consecutive_scans: int,
        ready: bool,
    ) -> dict:
        opp = scored.opportunity
        br = scored.breakdown
        return {
            "key": key,
            "asset": opp.asset,
            "quote": opp.quote,
            "venue_a": opp.leg_a.venue,
            "venue_b": opp.leg_b.venue,
            "label": scored.label.value,
            "bucket": br.bucket_type.value,
            "direction": br.direction,
            "expected_profit_bps": br.expected_profit_bps,
            "entry_edge_bps": br.entry_edge_bps,
            "carry_edge_bps": br.carry_edge_bps,
            "spread_zscore": opp.spread_zscore,
            "consecutive_scans": consecutive_scans,
            "required_consecutive_scans": self.config.required_consecutive_scans,
            "gate_passed": not reasons,
            "ready": ready,
            "reasons": reasons,
        }

    @staticmethod
    def _signal_key(scored: ScoredOpportunity) -> str:
        opp = scored.opportunity
        br = scored.breakdown
        return "|".join([
            opp.asset.upper(),
            opp.quote.upper(),
            opp.leg_a.venue,
            opp.leg_b.venue,
            br.bucket_type.value,
            br.direction,
        ])
