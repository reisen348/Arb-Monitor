"""JSON payload builders for scan batches, opportunities, and dashboard cards.

This module produces the structured dicts consumed by the web dashboard,
JSON CLI output, and any downstream API consumers.
"""
from __future__ import annotations

from typing import List, Sequence

from .display import (
    bucket_display,
    conviction_label,
    direction_display,
    display_many,
    display_notes,
    display_risks,
    display_term,
    label_display,
    label_rank,
    market_regime_display,
    order_type_display,
    role_display,
    side_display,
    time_in_force_display,
)
from .models import ExecutionLabel, ScoredOpportunity
from .scoring import ScoringConfig, linear_score


def build_batch_payload(batch) -> dict:
    config = ScoringConfig()
    latency_by_source = _aggregate_source_latency(batch)
    source_statuses = []
    for status in getattr(batch, "source_statuses", []):
        source_key = _normalized_source_name(status.adapter_name)
        source_latency = latency_by_source.get(source_key, {})
        source_statuses.append({
            "adapter_name": status.adapter_name,
            "ok": status.ok,
            "snapshot_count": status.snapshot_count,
            "poll_duration_ms": status.poll_duration_ms,
            "display_latency_ms": source_latency.get("typical_ms", status.poll_duration_ms),
            "max_snapshot_latency_ms": source_latency.get("max_ms", status.poll_duration_ms),
            "error": status.error,
            "timestamp": status.timestamp.isoformat() if status.timestamp else None,
        })
    return {
        "timestamp": batch.timestamp.isoformat(),
        "snapshot_count": len(batch.snapshots),
        "opportunity_count": len(batch.opportunities),
        "scan_duration_ms": getattr(batch, "scan_duration_ms", 0.0),
        "source_statuses": source_statuses,
        "dashboard_summary": _build_dashboard_summary(batch.scored_opportunities),
        "sort_policy": {
            "primary": "标签优先级",
            "secondary": "综合评分",
            "tertiary": "资金效率",
            "jump_penalties": {
                "micro_jump_frequency": "主要压制短线执行层排序",
                "shock_jump_frequency": "主要压制慢层状态和爆仓风险排序",
            },
        },
        "opportunities": [_serialize_opportunity(item, config) for item in batch.scored_opportunities],
    }


def _aggregate_source_latency(batch) -> dict[str, dict[str, float]]:
    values_by_source: dict[str, list[float]] = {}
    for snapshot in getattr(batch, "snapshots", []):
        key = _normalized_source_name(getattr(snapshot, "venue", ""))
        display_latency_ms = max(
            float(getattr(snapshot, "staleness_ms", 0.0) or 0.0),
            float(getattr(snapshot, "latency_ms", 0.0) or 0.0),
        )
        values_by_source.setdefault(key, []).append(display_latency_ms)
    return {
        key: {
            "typical_ms": _lower_median(values),
            "max_ms": max(values),
        }
        for key, values in values_by_source.items()
        if values
    }


def _lower_median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    return ordered[(len(ordered) - 1) // 2]


def _normalized_source_name(name: str) -> str:
    return (name or "").removesuffix("-ws")


def _fee_roles(opp, bucket: str):
    """Determine maker/taker role and fee for each leg."""
    if bucket == "carry":
        return ("maker", opp.leg_a.maker_fee_bps, "maker", opp.leg_b.maker_fee_bps)
    if opp.leg_a.depth_10k_usd >= opp.leg_b.depth_10k_usd:
        return ("maker", opp.leg_a.maker_fee_bps, "taker", opp.leg_b.taker_fee_bps)
    return ("taker", opp.leg_a.taker_fee_bps, "maker", opp.leg_b.maker_fee_bps)


def _serialize_opportunity(scored: ScoredOpportunity, config: ScoringConfig) -> dict:
    opp = scored.opportunity
    br = scored.breakdown
    max_funding_change = max(abs(opp.leg_a.funding_change_bps), abs(opp.leg_b.funding_change_bps))
    max_oi_change = max(abs(opp.leg_a.oi_change_pct), abs(opp.leg_b.oi_change_pct))
    role_a, fee_a, role_b, fee_b = _fee_roles(opp, br.bucket_type.value)
    return {
        "asset": opp.asset,
        "quote": opp.quote,
        "venue_a": opp.leg_a.venue,
        "venue_b": opp.leg_b.venue,
        "venue_a_asset": opp.leg_a.asset or opp.asset,
        "venue_b_asset": opp.leg_b.asset or opp.asset,
        "venue_a_ticker_only": bool(opp.metadata.get("leg_a_ticker_only")),
        "venue_b_ticker_only": bool(opp.metadata.get("leg_b_ticker_only")),
        "label": scored.label.value,
        "label_display": label_display(scored.label.value),
        "bucket": br.bucket_type.value,
        "bucket_display": bucket_display(br.bucket_type.value),
        "direction": br.direction,
        "direction_display": direction_display(br.direction),
        "composite_score": br.composite_score,
        "notional_usd": round(opp.notional_usd, 4),
        "capital_used_usd": round(opp.capital_used_usd, 4),
        "expected_profit_bps": br.expected_profit_bps,
        "expected_profit_usd": br.expected_profit_usd,
        "roi_per_capital": br.roi_per_capital,
        "entry_edge_bps": br.entry_edge_bps,
        "carry_edge_bps": br.carry_edge_bps,
        "funding_a_bps": opp.leg_a.funding_rate_bps,
        "funding_b_bps": opp.leg_b.funding_rate_bps,
        "funding_a_interval_h": opp.leg_a.funding_interval_hours,
        "funding_b_interval_h": opp.leg_b.funding_interval_hours,
        "next_funding_a": opp.leg_a.next_funding_time.isoformat() if opp.leg_a.next_funding_time else None,
        "next_funding_b": opp.leg_b.next_funding_time.isoformat() if opp.leg_b.next_funding_time else None,
        "maker_fee_a_bps": round(opp.leg_a.maker_fee_bps, 4),
        "taker_fee_a_bps": round(opp.leg_a.taker_fee_bps, 4),
        "maker_fee_b_bps": round(opp.leg_b.maker_fee_bps, 4),
        "taker_fee_b_bps": round(opp.leg_b.taker_fee_bps, 4),
        "price_a": opp.leg_a.mark_price,
        "price_b": opp.leg_b.mark_price,
        "best_bid_a": opp.leg_a.best_bid,
        "best_ask_a": opp.leg_a.best_ask,
        "best_bid_b": opp.leg_b.best_bid,
        "best_ask_b": opp.leg_b.best_ask,
        "oi_a_usd": round(opp.leg_a.oi_usd, 4),
        "oi_b_usd": round(opp.leg_b.oi_usd, 4),
        "volume_a_24h_usd": round(opp.leg_a.volume_24h_usd, 4),
        "volume_b_24h_usd": round(opp.leg_b.volume_24h_usd, 4),
        "impact_cost_50k_bps": round(opp.impact_cost_50k_bps, 4),
        "slippage_bps": round(opp.slippage_bps, 4),
        "latency_ms": round(opp.latency_ms, 4),
        "staleness_ms": round(opp.staleness_ms, 4),
        "fee_role_a": role_a,
        "fee_a_bps": round(fee_a, 2),
        "fee_role_b": role_b,
        "fee_b_bps": round(fee_b, 2),
        "fee_total_bps": round(fee_a + fee_b, 2),
        "spread_zscore": opp.spread_zscore,
        "spread_mean_bps": round(opp.spread_mean_bps, 2),
        "micro_jump_frequency": opp.micro_jump_frequency,
        "shock_jump_frequency": opp.shock_jump_frequency,
        "jump_frequency": opp.jump_frequency,
        "trend_vs_mean_reversion": opp.trend_vs_mean_reversion,
        "funding_change_bps_max": max_funding_change,
        "oi_change_pct_max": max_oi_change,
        "sort_basis": _serialize_sort_basis(scored, config),
        "alerts": _build_alerts(scored, config),
        "tags": display_many("tag", scored.tags),
        "raw_tags": list(scored.tags),
        "advisories": display_many("advisory", scored.advisories),
        "raw_advisories": list(scored.advisories),
        "dashboard_card": _build_dashboard_card(scored),
        "policy": {
            "allow_taker": scored.policy.allow_taker,
            "allow_maker": scored.policy.allow_maker,
            "size_multiplier": scored.policy.size_multiplier,
            "allow_carry": scored.policy.allow_carry,
            "allow_dislocation": scored.policy.allow_dislocation,
            "notes": display_notes(scored.policy.notes),
            "raw_notes": list(scored.policy.notes),
        },
        "execution_plan": {
            "action": scored.execution_plan.action,
            "action_display": display_term("action", scored.execution_plan.action),
            "execution_style": scored.execution_plan.execution_style,
            "execution_style_display": display_term("execution_style", scored.execution_plan.execution_style),
            "target_notional_usd": scored.execution_plan.target_notional_usd,
            "max_notional_usd": scored.execution_plan.max_notional_usd,
            "allowed_strategies": [bucket_display(item) for item in scored.execution_plan.allowed_strategies],
            "raw_allowed_strategies": list(scored.execution_plan.allowed_strategies),
            "notes": display_notes(scored.execution_plan.notes),
            "raw_notes": list(scored.execution_plan.notes),
            "legs": [
                {
                    "venue": leg.venue,
                    "side": side_display(leg.side),
                    "raw_side": leg.side,
                    "role": role_display(leg.role),
                    "raw_role": leg.role,
                    "order_type": leg.order_type,
                    "order_type_display": order_type_display(leg.order_type),
                    "time_in_force": leg.time_in_force,
                    "time_in_force_display": time_in_force_display(leg.time_in_force),
                    "reference_price": leg.reference_price,
                    "limit_price": leg.limit_price,
                    "notional_usd": leg.notional_usd,
                    "estimated_quantity": leg.estimated_quantity,
                    "post_only": leg.post_only,
                    "reduce_only": leg.reduce_only,
                    "enabled": leg.enabled,
                    "notes": display_many("execution_note", leg.notes),
                    "raw_notes": list(leg.notes),
                }
                for leg in scored.execution_plan.legs
            ],
        },
        "risk_flags": {
            "oracle_deviation": scored.risk_flags.oracle_deviation,
            "mark_index_divergence": scored.risk_flags.mark_index_divergence,
            "funding_spike": scored.risk_flags.funding_spike,
            "oi_spike": scored.risk_flags.oi_spike,
            "thin_book": scored.risk_flags.thin_book,
            "stale_data": scored.risk_flags.stale_data,
            "jump_risk": scored.risk_flags.jump_risk,
            "exit_risk": scored.risk_flags.exit_risk,
        },
        "block_reasons": display_many("block_reason", scored.block_reasons),
        "raw_block_reasons": list(scored.block_reasons),
    }


def _build_dashboard_summary(scored_opportunities: Sequence[ScoredOpportunity]) -> dict:
    label_counts = {label.value: 0 for label in ExecutionLabel}
    bucket_counts = {"dislocation": 0, "carry": 0}
    top_tags: dict[str, int] = {}
    if not scored_opportunities:
        return {
            "label_counts": label_counts,
            "bucket_counts": bucket_counts,
            "top_tags": [],
            "top_opportunity": None,
            "market_regime": "idle",
        }

    for scored in scored_opportunities:
        label_counts[scored.label.value] += 1
        bucket_counts[scored.breakdown.bucket_type.value] += 1
        for tag in scored.tags:
            top_tags[tag] = top_tags.get(tag, 0) + 1

    top_opportunity = max(
        scored_opportunities,
        key=lambda item: (item.breakdown.composite_score, item.breakdown.roi_per_capital),
    )
    avg_shock_jump = sum(item.opportunity.shock_jump_frequency for item in scored_opportunities) / len(scored_opportunities)
    avg_micro_jump = sum(item.opportunity.micro_jump_frequency for item in scored_opportunities) / len(scored_opportunities)
    market_regime = "stable"
    if avg_shock_jump >= 18.0:
        market_regime = "shock_risk"
    elif avg_micro_jump >= 10.0:
        market_regime = "microstructure_noisy"

    return {
        "label_counts": label_counts,
        "bucket_counts": bucket_counts,
        "top_tags": [
            {"tag": display_term("tag", tag), "raw_tag": tag, "count": count}
            for tag, count in sorted(top_tags.items(), key=lambda item: (-item[1], item[0]))[:5]
        ],
        "top_opportunity": {
            "asset": top_opportunity.opportunity.asset,
            "venues": [
                top_opportunity.opportunity.leg_a.venue,
                top_opportunity.opportunity.leg_b.venue,
            ],
            "label": top_opportunity.label.value,
            "composite_score": top_opportunity.breakdown.composite_score,
            "expected_profit_bps": top_opportunity.breakdown.expected_profit_bps,
        },
        "market_regime": market_regime,
        "market_regime_display": market_regime_display(market_regime),
    }


def _build_dashboard_card(scored: ScoredOpportunity) -> dict:
    opp = scored.opportunity
    br = scored.breakdown
    thesis = (
        f"{opp.asset} 在 {opp.leg_a.venue} 与 {opp.leg_b.venue} 之间出现"
        f"{bucket_display(br.bucket_type.value)}机会，当前预期净收益 "
        f"{br.expected_profit_bps:.2f} bps，资金效率 {br.roi_per_capital:.4f}。"
    )
    if scored.label == ExecutionLabel.BLOCKED:
        thesis = (
            f"{opp.asset} 当前被拦截。虽然表面存在价差，但扣除成本并经过风险过滤后，"
            f"净边际还不足以进入观察优先级。"
        )
    elif scored.label == ExecutionLabel.WATCH:
        thesis = (
            f"{opp.asset} 值得继续观察。当前边际存在，但执行条件或市场状态还不够干净，"
            f"暂时更适合看板追踪。"
        )

    monitoring_points = [
        f"微跳价频率 {opp.micro_jump_frequency:.2f}",
        f"冲击跳价频率 {opp.shock_jump_frequency:.2f}",
        f"价差 Z-Score {opp.spread_zscore:.2f}",
        f"资金费差 {br.carry_edge_bps:.2f} bps",
    ]
    return {
        "title": _build_card_title(opp),
        "thesis": thesis,
        "conviction": conviction_label(scored),
        "why_now": _build_why_now(scored),
        "risks": display_risks(scored),
        "monitoring_points": monitoring_points,
    }


def _build_card_title(opp) -> str:
    """Build card title showing original asset per venue."""
    asset_a = opp.leg_a.asset or opp.asset
    asset_b = opp.leg_b.asset or opp.asset
    return (
        f"{opp.asset} · {opp.leg_a.venue}({asset_a}) vs {opp.leg_b.venue}({asset_b})"
    )


def _build_why_now(scored: ScoredOpportunity) -> List[str]:
    opp = scored.opportunity
    br = scored.breakdown
    points = [
        f"净边际 {br.expected_profit_bps:.2f} bps",
        f"综合评分 {br.composite_score:.2f}",
        f"资金效率 {br.roi_per_capital:.4f}",
    ]
    if br.bucket_type.value == "carry":
        points.append(f"资金费 carry {br.carry_edge_bps:.2f} bps")
    else:
        points.append(f"入场价差 {br.entry_edge_bps:.2f} bps")
    return points


def _serialize_sort_basis(scored: ScoredOpportunity, config: ScoringConfig) -> dict:
    opp = scored.opportunity
    br = scored.breakdown
    jump_threshold = _jump_threshold_for_opportunity(opp, config)
    micro_jump_score = linear_score(
        opp.micro_jump_frequency,
        2.0,
        jump_threshold,
        inverse=True,
    )
    shock_jump_score = linear_score(
        opp.shock_jump_frequency,
        2.0,
        jump_threshold,
        inverse=True,
    )
    return {
        "label_rank": label_rank(scored.label),
        "composite_score": br.composite_score,
        "roi_per_capital": br.roi_per_capital,
        "expected_profit_bps": br.expected_profit_bps,
        "micro_jump_frequency": opp.micro_jump_frequency,
        "micro_jump_penalty": round(100.0 - micro_jump_score, 2),
        "shock_jump_frequency": opp.shock_jump_frequency,
        "shock_jump_penalty": round(100.0 - shock_jump_score, 2),
        "ranking_order": [
            "标签优先级",
            "综合评分",
            "资金效率",
        ],
    }


def _build_alerts(scored: ScoredOpportunity, config: ScoringConfig) -> List[dict]:
    opp = scored.opportunity
    alerts: List[dict] = []
    jump_threshold = _jump_threshold_for_opportunity(opp, config)
    micro_warn = jump_threshold * 0.6
    shock_warn = jump_threshold * 0.6

    if opp.micro_jump_frequency >= jump_threshold:
        alerts.append(
            {
                "code": "micro_jump_risk",
                "severity": "高",
                "message": "微跳价频率过高，短线成交质量会明显变差。",
                "value": round(opp.micro_jump_frequency, 2),
                "threshold": jump_threshold,
                "action": "将可交易机会降为观察",
            }
        )
    elif opp.micro_jump_frequency >= micro_warn:
        alerts.append(
            {
                "code": "micro_jump_watch",
                "severity": "中",
                "message": "微跳价频率正在上升，短周期成交可能变差。",
                "value": round(opp.micro_jump_frequency, 2),
                "threshold": round(micro_warn, 2),
                "action": "标记为短线成交噪声高",
            }
        )

    if opp.shock_jump_frequency >= jump_threshold:
        alerts.append(
            {
                "code": "shock_jump_risk",
                "severity": "高",
                "message": "冲击跳价频率过高，市场状态和爆仓风险都在恶化。",
                "value": round(opp.shock_jump_frequency, 2),
                "threshold": jump_threshold,
                "action": "标记为市场状态不稳和爆仓风险升高",
            }
        )
    elif opp.shock_jump_frequency >= shock_warn:
        alerts.append(
            {
                "code": "shock_jump_watch",
                "severity": "中",
                "message": "冲击跳价频率正在上升，市场状态开始变得不稳定。",
                "value": round(opp.shock_jump_frequency, 2),
                "threshold": round(shock_warn, 2),
                "action": "标记为市场状态不稳定",
            }
        )

    if scored.risk_flags.jump_risk and not any(item["code"] == "shock_jump_risk" for item in alerts):
        alerts.append(
            {
                "code": "jump_risk",
                "severity": "中",
                "message": "整体跳价风险偏高，需要提高警惕。",
                "value": round(opp.jump_frequency, 2),
                "threshold": jump_threshold,
                "action": "提示整体跳价风险",
            }
        )

    return alerts


def _jump_threshold_for_opportunity(opp, config: ScoringConfig) -> float:
    if "dex" in opp.leg_a.market_type or "dex" in opp.leg_b.market_type:
        return config.dex_jump_risk_threshold
    return config.jump_risk_threshold
