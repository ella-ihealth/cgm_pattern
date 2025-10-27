"""Detect rapid glucose rises (>80 mg/dL within 15 minutes)."""
from __future__ import annotations

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule
from .utils import prepare_day, rolling_delta


@register_rule
class RapidRiseRule(PatternRule):
    id = "rapid_rise"
    pattern_id = 29
    description = "≥3 days with >80 mg/dL rise within 15 minutes"
    version = "1.0.0"
    metadata = PATTERN_METADATA[29]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        rise_threshold = float(self.resolved_threshold(context, "rapid_rise_threshold", 80.0))
        days_required = int(self.resolved_threshold(context, "rapid_rise_days_required", 3))

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

        qualifying = []
        for day in eligible_days:
            prepared = prepare_day(day)
            frame = prepared.frame
            if frame.empty:
                continue
            series = frame.set_index("timestamp")["glucose_mg_dL"]
            deltas = rolling_delta(series, "15min")
            if deltas.ge(rise_threshold).any():
                idx = deltas[deltas.ge(rise_threshold)].index[0]
                qualifying.append(
                    {
                        "service_date": day.service_date.isoformat(),
                        "timestamp": idx.isoformat(),
                        "rise_mg_dl": float(deltas.loc[idx]),
                    }
                )

        status = PatternStatus.DETECTED if len(qualifying) >= days_required else PatternStatus.NOT_DETECTED
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "rapid_rise_days": len(qualifying),
            "rise_threshold": rise_threshold,
        }
        evidence = {
            "rise_examples": qualifying[:days_required],
            "required_days": days_required,
        }
        confidence = min(1.0, len(qualifying) / max(1, days_required))
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=confidence,
            version=self.version,
        )
