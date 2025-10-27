"""Detect likely compression lows during overnight hours."""
from __future__ import annotations

from ..models import PatternContext, PatternDetection, PatternStatus, PatternInputBundle
from ..pattern_metadata import PATTERN_METADATA
from ..registry import register_rule
from ..rule_base import PatternRule


@register_rule
class OvernightCompressionLowRule(PatternRule):
    id = "overnight_compression_low"
    pattern_id = 5
    description = "Overnight glucose <70 mg/dL <15 min with flanking ≥80 mg/dL and >10 mg/dL/5 min drop & recovery"
    version = "1.0.0"
    metadata = PATTERN_METADATA[5]

    def detect(self, window: PatternInputBundle, context: PatternContext) -> PatternDetection:
        coverage_threshold = float(self.resolved_threshold(context, "minimum_day_coverage", 0.7))
        required_days = int(self.resolved_threshold(context, "analysis_days_required", 5))
        low_threshold = float(self.resolved_threshold(context, "compression_low_threshold", 70.0))
        surrounding_threshold = float(self.resolved_threshold(context, "compression_surrounding_threshold", 80.0))
        drop_rate_threshold = float(self.resolved_threshold(context, "compression_drop_rate_threshold", 10.0))
        recovery_rate_threshold = float(self.resolved_threshold(context, "compression_recovery_rate_threshold", 10.0))
        window_start = float(self.resolved_threshold(context, "compression_window_start", 0.0))
        window_end = float(self.resolved_threshold(context, "compression_window_end", 6.0))
        max_duration_minutes = float(self.resolved_threshold(context, "compression_max_minutes", 15.0))

        analysis_window_days = int(self.resolved_threshold(context, "analysis_window_days", 7))

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
            overnight = window.time_window(day, window_start, window_end)
            frame = overnight
            if frame.empty:
                continue

            glucose = frame["glucose_mg_dL"].to_numpy()
            minutes = frame["minutes"].to_numpy()
            times = frame["local_time"].to_numpy()

            below_mask = glucose < low_threshold
            if not below_mask.any():
                continue

            indices = below_mask.nonzero()[0]

            start_idx = indices[0]
            end_idx = indices[-1]
            duration = float(minutes[start_idx:end_idx + 1].sum())
            if duration >= max_duration_minutes:
                continue

            if start_idx == 0 or end_idx == len(glucose) - 1:
                continue

            before_glucose = float(glucose[start_idx - 1])
            after_glucose = float(glucose[end_idx + 1])
            if before_glucose < surrounding_threshold or after_glucose < surrounding_threshold:
                continue

            # Approximate rate per 5 minutes based on immediate neighboring points.
            drop_delta = before_glucose - float(glucose[start_idx])
            recovery_delta = float(glucose[end_idx]) - after_glucose

            drop_rate = drop_delta / max(minutes[start_idx], 1e-6) * 5.0
            recovery_rate = recovery_delta / max(minutes[end_idx], 1e-6) * 5.0

            if drop_rate < drop_rate_threshold or abs(recovery_rate) < recovery_rate_threshold:
                continue

            qualifying.append(
                {
                    "service_date": day.service_date.isoformat(),
                    "duration_minutes": duration,
                    "lowest_glucose": float(glucose[start_idx:end_idx + 1].min()),
                    "before_glucose": before_glucose,
                    "after_glucose": after_glucose,
                    "drop_rate_mg_dl_per_5min": drop_rate,
                    "recovery_rate_mg_dl_per_5min": abs(recovery_rate),
                }
            )

        qualifying_count = len(qualifying)
        status = PatternStatus.DETECTED if qualifying_count > 0 else PatternStatus.NOT_DETECTED
        metrics = {
            "analysis_days_considered": len(eligible_days),
            "compression_low_events": qualifying_count,
            "low_threshold": low_threshold,
            "max_duration_minutes": max_duration_minutes,
            "surrounding_threshold": surrounding_threshold,
            "drop_rate_threshold": drop_rate_threshold,
            "recovery_rate_threshold": recovery_rate_threshold,
            "window_start_hour": window_start,
            "window_end_hour": window_end,
        }
        evidence = {
            "examples": qualifying[:3],
        }
        return PatternDetection(
            pattern_id=self.id,
            effective_date=context.analysis_date,
            status=status,
            evidence=evidence,
            metrics=metrics,
            confidence=min(1.0, qualifying_count / max(1, required_days)),
            version=self.version,
        )
