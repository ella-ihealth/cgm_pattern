"""CGM pattern detection library."""

from .models import (
    CGMDay,
    DailyCGMSummary,
    ExcursionEvent,
    ExcursionTrendSummary,
    PatternContext,
    PatternDetection,
    PatternStatus,
    PatternInputBundle,
    RollingStatsSnapshot,
    RollingWindowSummary,
)
from .registry import register_rule, registry
from .rule_base import PatternRule

__all__ = [
    "CGMDay",
    "DailyCGMSummary",
    "ExcursionEvent",
    "ExcursionTrendSummary",
    "PatternContext",
    "PatternDetection",
    "PatternStatus",
    "PatternInputBundle",
    "RollingWindowSummary",
    "RollingStatsSnapshot",
    "PatternRule",
    "register_rule",
    "registry",
]
