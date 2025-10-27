"""Detect single-day severe hypoglycemia events."""
from __future__ import annotations

from dataclasses import dataclass

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import prepare_day, total_minutes


@dataclass
class SevereLowResult:
    service_date: str
    minutes_below_54: float


@register_rule
class SingleDayLowRule(PatternRule):
    id = "single_day_low"
    pattern_id = 27
    description = "Any day with ≥15 minutes below 54 mg/dL within 14-day window"
    version = "1.0.0"
    metadata = PATTERN_METADATA[27]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        minimum_days = int(self.resolved_threshold(context, "low_minimum_days", 14))
        validation_window_days = int(self.resolved_threshold(context, "validation_window_days", 14))
        duration_threshold = float(self.resolved_threshold(context, "low_duration_threshold", 15.0))
        glucose_threshold = float(self.resolved_threshold(context, "low_glucose_threshold", 54.0))

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

        detections: list[SevereLowResult] = []
        for day in days:
            prepared = prepare_day(day)
            frame = prepared.frame
            if frame.empty:
                continue
            minutes = total_minutes(frame.loc[frame["glucose_mg_dL"] < glucose_threshold])
            if minutes >= duration_threshold:
                detections.append(
                    SevereLowResult(
                        service_date=day.service_date.isoformat(),
                        minutes_below_54=minutes,
                    )
                )

        status = PatternStatus.DETECTED if detections else PatternStatus.NOT_DETECTED
        metrics = {
            "validation_days_considered": len(days),
            "severe_low_days": len(detections),
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
