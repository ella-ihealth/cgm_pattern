"""Detect noisy sensor days for data quality triage."""
from __future__ import annotations

from dataclasses import dataclass

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import prepare_day


@dataclass
class NoiseResult:
    service_date: str
    noise_index: float


@register_rule
class NoisySensorDayRule(PatternRule):
    id = "noisy_sensor_day"
    pattern_id = 35
    description = "Detects days where intra-day noise exceeds threshold"
    version = "1.0.0"
    metadata = PATTERN_METADATA[35]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        validation_window_days = int(self.resolved_threshold(context, "validation_window_days", 14))
        noise_threshold = float(self.resolved_threshold(context, "noise_index_threshold", 30.0))

        validation_days, insufficient_validation = self.ensure_validation_window(
            window,
            context,
            coverage_threshold,
            validation_window_days,
            use_raw_days=True,
        )
        if insufficient_validation:
            return insufficient_validation

        detections: list[NoiseResult] = []
        for day in validation_days:
            prepared = prepare_day(day)
            frame = prepared.frame
            if len(frame) < 6:
                continue
            diffs = frame["glucose_mg_dL"].diff().abs().dropna()
            if diffs.empty:
                continue
            noise_index = float(diffs.quantile(0.95))
            if noise_index > noise_threshold:
                detections.append(
                    NoiseResult(
                        service_date=day.service_date.isoformat(),
                        noise_index=noise_index,
                    )
                )

        status = PatternStatus.DETECTED if detections else PatternStatus.NOT_DETECTED
        evidence = {
            "examples": [det.__dict__ for det in detections[:3]],
        }
        metrics = {
            "detections": len(detections),
            "noise_threshold": noise_threshold,
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
