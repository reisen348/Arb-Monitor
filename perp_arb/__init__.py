"""Perp arbitrage scoring engine."""

from .market_data import (
    MarketDataAdapter,
    MarketSnapshot,
    MockMarketDataAdapter,
    OpportunityBuilder,
    PairingConfig,
    ScanBatch,
    ScannerConfig,
)
from .hyperliquid import HyperliquidAdapter, HyperliquidAdapterConfig, HyperliquidApiError
from .grvt import GrvtAdapter, GrvtAdapterConfig, GrvtApiError
from .bitget import BitgetAdapter, BitgetAdapterConfig, BitgetApiError
from .gate import GateAdapter, GateAdapterConfig, GateApiError
from .kraken import KrakenAdapter, KrakenAdapterConfig, KrakenApiError
from .aster import AsterAdapter, AsterAdapterConfig, AsterApiError
from .ondo import OndoAdapter, OndoAdapterConfig, OndoApiError
from .models import (
    ExecutionLegPlan,
    ExecutionPlan,
    ExecutionPolicy,
    ExecutionLabel,
    OpportunityBucket,
    PerpArbOpportunity,
    PerpLegSnapshot,
    RiskFlags,
    ScoreBreakdown,
    ScoredOpportunity,
)
from .scoring import ScoringConfig, score_opportunities
from .scanner import RealtimeScanner
from .state import MarketStateTracker, OpportunityStateTracker, StateTrackerConfig
from .ws_adapters import GrvtWebsocketAdapter, HyperliquidWebsocketAdapter

__all__ = [
    "ExecutionLegPlan",
    "ExecutionPolicy",
    "ExecutionPlan",
    "ExecutionLabel",
    "AsterAdapter",
    "AsterAdapterConfig",
    "AsterApiError",
    "BitgetAdapter",
    "BitgetAdapterConfig",
    "BitgetApiError",
    "GateAdapter",
    "GateAdapterConfig",
    "GateApiError",
    "GrvtAdapter",
    "GrvtAdapterConfig",
    "GrvtApiError",
    "GrvtWebsocketAdapter",
    "HyperliquidAdapter",
    "HyperliquidAdapterConfig",
    "HyperliquidApiError",
    "HyperliquidWebsocketAdapter",
    "KrakenAdapter",
    "KrakenAdapterConfig",
    "KrakenApiError",
    "MarketDataAdapter",
    "MarketSnapshot",
    "MarketStateTracker",
    "MockMarketDataAdapter",
    "OpportunityBucket",
    "OpportunityStateTracker",
    "OpportunityBuilder",
    "OndoAdapter",
    "OndoAdapterConfig",
    "OndoApiError",
    "PairingConfig",
    "PerpArbOpportunity",
    "PerpLegSnapshot",
    "RiskFlags",
    "RealtimeScanner",
    "ScanBatch",
    "ScoreBreakdown",
    "ScoredOpportunity",
    "ScoringConfig",
    "ScannerConfig",
    "StateTrackerConfig",
    "score_opportunities",
]
