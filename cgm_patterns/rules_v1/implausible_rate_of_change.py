"""Detect physiologically implausible rate-of-change events."""
from __future__ import annotations

from dataclasses import dataclass

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import consecutive_durations, prepare_day, rate_of_change


@dataclass
class RocResult:
    service_date: str
    max_rate: float
    duration_minutes: float


@register_rule
class ImplausibleRateOfChangeRule(PatternRule):
    id = "implausible_rate_of_change"
    pattern_id = 33
    description = "|Δ| >5 mg/dL/min sustained for ≥10 minutes"
    version = "1.0.0"
    metadata = PATTERN_METADATA[33]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        validation_window_days = int(self.resolved_threshold(context, "validation_window_days", 14))
        rate_threshold = float(self.resolved_threshold(context, "roc_threshold", 5.0))
        duration_required = float(self.resolved_threshold(context, "roc_duration_required", 10.0))

        validation_days, insufficient_validation = self.ensure_validation_window(
            window,
            context,
            coverage_threshold,
            validation_window_days,
            use_raw_days=True,
        )
        if insufficient_validation:
            return insufficient_validation

        detections: list[RocResult] = []
        for day in validation_days:
            prepared = prepare_day(day)
            frame = prepared.frame
            if frame.empty:
                continue
            series = frame.set_index("timestamp")["glucose_mg_dL"]
            rates = rate_of_change(series).abs()
            rate_frame = frame.copy()
            rate_frame["rate"] = rates.reindex(frame["timestamp"]).to_numpy()
            mask = rate_frame["rate"].fillna(0) > rate_threshold
            durations = list(consecutive_durations(mask, rate_frame["minutes"]))
            if not durations:
                continue
            max_duration = max(duration for _, duration in durations)
            if max_duration >= duration_required:
                detections.append(
                    RocResult(
                        service_date=day.service_date.isoformat(),
                        max_rate=float(rate_frame["rate"].max(skipna=True)),
                        duration_minutes=max_duration,
                    )
                )

        status = PatternStatus.DETECTED if detections else PatternStatus.NOT_DETECTED
        evidence = {
            "examples": [det.__dict__ for det in detections[:3]],
        }
        metrics = {
            "detections": len(detections),
            "rate_threshold": rate_threshold,
            "duration_required": duration_required,
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
