"""Chinese display mapping layer.

All human-readable term translations, label/bucket/direction display helpers,
and risk/notes rendering live here. If you add new tags, block reasons, or
advisories in the scoring engine, update TERM_DISPLAY accordingly.
"""
from __future__ import annotations

from typing import List, Sequence

from .models import ExecutionLabel, ScoredOpportunity


TERM_DISPLAY = {
    "tag": {
        "freshness_risk": "行情新鲜度偏低",
        "micro_execution_risk": "短线成交噪声高",
        "regime_unstable": "市场状态不稳定",
        "liquidation_risk": "爆仓风险升高",
    },
    "advisory": {
        "freshness_watch": "行情新鲜度偏低，先观察",
        "tradable_downgraded_by_freshness": "因行情新鲜度不足，从可交易降为观察",
        "micro_jump_watch": "微跳价频率偏高，先观察",
        "tradable_downgraded_by_micro_jump": "因短线跳价过多，从可交易降为观察",
        "shock_jump_watch": "冲击跳价偏高，市场状态需谨慎",
        "shock_jump_liquidation_risk": "冲击跳价过高，爆仓风险上升",
        "carry_blocked_by_liquidation_risk": "因爆仓风险，禁止资金费 carry",
    },
    "block_reason": {
        "expected_profit_non_positive": "扣完成本后没有净收益",
        "insufficient_depth_10k": "1 万美元档位深度不足",
        "insufficient_depth_50k": "5 万美元档位深度不足",
        "impact_cost_50k_too_high": "大额冲击成本过高",
        "slippage_too_high": "滑点过高",
        "staleness_too_high": "行情更新太慢",
        "latency_too_high": "链路延迟过高",
        "oracle_offset_too_high": "Oracle 偏移过大",
        "mark_index_divergence_too_high": "标记价和指数价偏离过大",
        "oi_and_funding_spike": "持仓量和资金费同时异常",
        "exit_liquidity_too_low": "退出流动性不足",
        "funding_instability": "资金费延续性不足",
        "funding_window_uncertain": "临近结算，资金费方向不稳",
        "carry_squeeze_risk": "高资金费叠加高波动，挤仓风险高",
        "carry_blocked_by_liquidation_risk": "爆仓风险过高，禁止做 carry",
    },
    "policy_note": {
        "maker_only": "只适合挂单观察",
        "reduce_size_half": "建议先缩到半仓观察",
        "carry_blocked": "不建议做资金费 carry",
        "dislocation_only_short_horizon": "仅适合超短线错位回归",
        "carry_not_allowed_under_liquidation_risk": "爆仓风险阶段禁止做 carry",
    },
    "execution_note": {
        "draft_only": "仅作看板草案",
        "hedge_leg": "对冲腿",
    },
    "action": {
        "execute": "可执行",
        "observe": "继续观察",
        "skip": "跳过",
    },
    "execution_style": {
        "taker_or_maker": "吃单或挂单均可",
        "maker_only": "仅适合挂单",
        "taker_only": "仅适合吃单",
        "disabled": "暂不操作",
    },
    "severity": {
        "high": "高",
        "medium": "中",
        "low": "低",
    },
}


def label_display(label: str) -> str:
    return {
        "tradable": "可交易",
        "watch": "观察",
        "blocked": "拦截",
    }.get(label, label)


def bucket_display(bucket: str) -> str:
    return {
        "dislocation": "错位回归",
        "carry": "资金费 carry",
    }.get(bucket, bucket)


def market_regime_display(regime: str) -> str:
    return {
        "stable": "稳定",
        "microstructure_noisy": "微观结构噪声偏高",
        "shock_risk": "冲击风险偏高",
        "idle": "暂无机会",
    }.get(regime, regime)


def direction_display(direction: str) -> str:
    return {
        "long_a_short_b": "做多 A / 做空 B",
        "long_b_short_a": "做多 B / 做空 A",
    }.get(direction, direction)


def display_term(category: str, code: str) -> str:
    return TERM_DISPLAY.get(category, {}).get(code, code.replace("_", " "))


def display_many(category: str, values: Sequence[str]) -> List[str]:
    return [display_term(category, value) for value in values]


def display_notes(values: Sequence[str]) -> List[str]:
    display_values: List[str] = []
    for value in values:
        if value.startswith("blocked:"):
            reason = value.split(":", 1)[1]
            display_values.append(f"拦截原因：{display_term('block_reason', reason)}")
            continue
        if value.endswith("_strategy_disabled"):
            strategy = value[: -len("_strategy_disabled")]
            display_values.append(f"{bucket_display(strategy)}模式已禁用")
            continue
        display_values.append(
            TERM_DISPLAY["policy_note"].get(
                value,
                TERM_DISPLAY["execution_note"].get(value, value.replace("_", " ")),
            )
        )
    return display_values


def display_risks(scored: ScoredOpportunity) -> List[str]:
    combined = list(dict.fromkeys(list(scored.tags) + list(scored.block_reasons)))[:6]
    result: List[str] = []
    for value in combined:
        category = "tag" if value in TERM_DISPLAY["tag"] else "block_reason"
        result.append(display_term(category, value))
    return result


def side_display(side: str) -> str:
    return {"buy": "买入", "sell": "卖出"}.get(side, side)


def role_display(role: str) -> str:
    return {"primary": "主腿", "hedge": "对冲腿"}.get(role, role)


def order_type_display(order_type: str) -> str:
    return {
        "marketable_limit": "可成交限价",
        "limit": "限价",
        "none": "不下单",
    }.get(order_type, order_type)


def time_in_force_display(tif: str) -> str:
    return {
        "ioc": "立即成交否则取消",
        "gtc": "持续有效",
        "none": "无",
    }.get(tif, tif)


def conviction_label(scored: ScoredOpportunity) -> str:
    if scored.label == ExecutionLabel.TRADABLE and scored.breakdown.composite_score >= 80:
        return "高"
    if scored.label in {ExecutionLabel.TRADABLE, ExecutionLabel.WATCH}:
        return "中"
    return "低"


def label_rank(label: ExecutionLabel) -> int:
    if label == ExecutionLabel.TRADABLE:
        return 2
    if label == ExecutionLabel.WATCH:
        return 1
    return 0
