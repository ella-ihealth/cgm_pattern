"""Detect single-day extreme hyperglycemia events."""
from __future__ import annotations

from dataclasses import dataclass

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import prepare_day, total_minutes


@dataclass
class SpikeResult:
    service_date: str
    max_glucose: float
    minutes_above_250: float


@register_rule
class SingleDayHighSpikeRule(PatternRule):
    id = "single_day_high_spike"
    pattern_id = 26
    description = "Any day with max glucose >300 mg/dL and <2h above 250 mg/dL"
    version = "1.0.0"
    metadata = PATTERN_METADATA[26]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 7))
        spike_threshold = float(self.resolved_threshold(context, "spike_max_threshold", 300.0))
        duration_threshold = float(self.resolved_threshold(context, "spike_duration_threshold", 120.0))

        analysis_window_days = int(self.resolved_threshold(context, "analysis_window_days", 7))
        validation_window_days = int(self.resolved_threshold(context, "validation_window_days", 14))

        _, insufficient_validation = self.ensure_validation_window(
            window,
            context,
            coverage_threshold,
            validation_window_days,
            use_raw_days=True,
        )
        if insufficient_validation:
            return insufficient_validation

        eligible_days = [day for day in window.analysis_days if day.coverage_ratio() >= coverage_threshold]
        eligible_days = eligible_days[-analysis_window_days:]
        if len(eligible_days) < required_days:
            return PatternDetection(
                pattern_id=self.id,
                effective_date=context.analysis_date,
                status=PatternStatus.INSUFFICIENT_DATA,
                evidence={
                    "eligible_days": len(eligible_days),
                    "required_analysis_days": required_days,
                },
                metrics={},
                version=self.version,
            )

        detections: list[SpikeResult] = []
        for day in eligible_days:
            prepared = prepare_day(day)
            frame = prepared.frame
            if frame.empty:
                continue
            max_glucose = float(frame["glucose_mg_dL"].max())
            if max_glucose <= spike_threshold:
                continue
            minutes_above = total_minutes(frame.loc[frame["glucose_mg_dL"] >= 250.0])
            if minutes_above < duration_threshold:
                detections.append(
                    SpikeResult(
                        service_date=day.service_date.isoformat(),
                        max_glucose=max_glucose,
                        minutes_above_250=minutes_above,
                    )
                )

        status = PatternStatus.DETECTED if detections else PatternStatus.NOT_DETECTED
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "spike_days": len(detections),
        }
        evidence = {
            "spike_examples": [det.__dict__ for det in detections[:3]],
        }
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=1.0 if detections else 0.0,
            version=self.version,
        )
