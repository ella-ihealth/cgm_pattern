"""Detect abrupt baseline shifts indicative of sensor swaps."""
from __future__ import annotations

from dataclasses import dataclass

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from ..rules_v1.utils import prepare_day


@dataclass
class StepChangeResult:
    service_date: str
    timestamp: str
    delta_mg_dl: float


@register_rule
class SensorSwapStepChangeRule(PatternRule):
    id = "sensor_swap_step_change"
    pattern_id = 34
    description = "Baseline shift ≥25 mg/dL within ±2h window"
    version = "1.0.0"
    metadata = PATTERN_METADATA[34]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        delta_threshold = float(self.resolved_threshold(context, "step_change_threshold", 25.0))
        window_minutes = float(self.resolved_threshold(context, "step_change_window_minutes", 60.0))

        detections: list[StepChangeResult] = []
        for day in window.validation_days:
            prepared = prepare_day(day)
            frame = prepared.frame
            if len(frame) < 6:
                continue
            series = frame.set_index("timestamp")["glucose_mg_dL"].sort_index()
            before = series.rolling(f"{int(window_minutes)}min", closed="left", min_periods=3).mean()
            after = series[::-1].rolling(f"{int(window_minutes)}min", closed="left", min_periods=3).mean()[::-1]
            deltas = (after - before).abs()
            candidate = deltas.dropna()
            if candidate.empty:
                continue
            exceed = candidate[candidate >= delta_threshold]
            if exceed.empty:
                continue
            ts = exceed.index[0]
            detections.append(
                StepChangeResult(
                    service_date=day.service_date.isoformat(),
                    timestamp=ts.isoformat(),
                    delta_mg_dl=float(exceed.iloc[0]),
                )
            )

        status = PatternStatus.DETECTED if detections else PatternStatus.NOT_DETECTED
        evidence = {
            "examples": [det.__dict__ for det in detections[:3]],
        }
        metrics = {
            "detections": len(detections),
            "delta_threshold": delta_threshold,
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
