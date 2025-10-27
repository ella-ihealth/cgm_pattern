"""Detect single-day extended hyperglycemia events (>250 mg/dL for 4h)."""
from __future__ import annotations

from dataclasses import dataclass

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import prepare_day, total_minutes


@dataclass
class LongHighResult:
    service_date: str
    minutes_above_250: float


@register_rule
class SingleLongHighRule(PatternRule):
    id = "single_long_high"
    pattern_id = 31
    description = "Any day with ≥240 minutes above 250 mg/dL in 30-day window"
    version = "1.0.0"
    metadata = PATTERN_METADATA[31]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        minimum_days = int(self.resolved_threshold(context, "long_high_minimum_days", 14))
        validation_window_days = int(self.resolved_threshold(context, "validation_window_days", 14))
        duration_threshold = float(self.resolved_threshold(context, "long_high_duration_threshold", 240.0))
        glucose_threshold = float(self.resolved_threshold(context, "long_high_glucose_threshold", 250.0))

        required_validation_days = max(validation_window_days, minimum_days)
        validation_days, insufficient_validation = self.ensure_validation_window(
            window,
            context,
            coverage_threshold,
            required_validation_days,
            use_raw_days=True,
        )
        if insufficient_validation:
            return insufficient_validation

        days = validation_days[-minimum_days:]

        detections: list[LongHighResult] = []
        for day in days:
            prepared = prepare_day(day)
            frame = prepared.frame
            if frame.empty:
                continue
            minutes = total_minutes(frame.loc[frame["glucose_mg_dL"] >= glucose_threshold])
            if minutes >= duration_threshold:
                detections.append(
                    LongHighResult(
                        service_date=day.service_date.isoformat(),
                        minutes_above_250=minutes,
                    )
                )

        status = PatternStatus.DETECTED if detections else PatternStatus.NOT_DETECTED
        metrics = {
            "validation_days_considered": len(days),
            "long_high_days": len(detections),
        }
        evidence = {
            "examples": [det.__dict__ for det in detections[:3]],
        }
        confidence = 1.0 if detections else 0.0
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=confidence,
            version=self.version,
        )
