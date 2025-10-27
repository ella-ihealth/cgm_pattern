"""Rule package that ensures registration on import."""
from __future__ import annotations

from importlib import import_module

_MODULES = [
    "predominant_hyperglycemia",
    "predominant_hypoglycemia",
    "high_glycemic_variability",
    "stable_near_target_control",
    "overnight_hypoglycemia",
    "somogyi_effect",
    "dawn_phenomenon",
    "weekday_weekend_instability",
    "evening_variability_spike",
    "single_day_high_spike",
    "single_day_low",
    "rapid_rise",
    "rapid_fall",
    "single_long_high",
    "day_to_day_instability",
    "implausible_rate_of_change",
    "noisy_sensor_day",
    "recurrent_post_meal_spike",
]

# Import selected rules to trigger registration side-effects.
for _module in _MODULES:
    import_module(f"{__name__}.{_module}")

__all__ = list(_MODULES)
