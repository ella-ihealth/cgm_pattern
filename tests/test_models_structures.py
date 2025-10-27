from datetime import date

import pandas as pd

from cgm_patterns.models import (
    CGMDay,
    ExcursionEvent,
    ExcursionTrendSummary,
    RollingStatsSnapshot,
    RollingWindowSummary,
)


def test_rolling_window_summary_creation():
    summary = RollingWindowSummary(
        patient_id="p",
        start_date=date(2024, 9, 15),
        end_date=date(2024, 9, 28),
        days_worn=12,
        percent_time_active=0.82,
        mean_glucose=165.0,
        gmi=7.2,
        gv=0.15,
        time_percentages={">250": 0.01, "70-179": 0.68},
        extra_metrics={"time_above_180": 0.31},
        window_valid=True,
    )
    assert summary.patient_id == "p"
    assert summary.window_valid is True


def test_excursion_trend_summary_creation():
    excursions = [
        ExcursionEvent(
            start_local="02:45",
            end_local="05:15",
            duration_minutes=150,
            min_mg_dl=45,
            max_mg_dl=65,
            mean_mg_dl=54,
            direction="hypo",
        )
    ]
    trend = ExcursionTrendSummary(
        patient_id="12345",
        start_date=date(2024, 12, 15),
        end_date=date(2025, 1, 13),
        template_coverage_days=7,
        lookback_days=30,
        excursions=excursions,
    )
    assert trend.excursions[0].direction == "hypo"


def test_rolling_stats_snapshot_holds_windows():
    window = RollingWindowSummary(
        patient_id="p",
        start_date=None,
        end_date=date(2025, 1, 6),
        days_worn=13,
        percent_time_active=0.81,
        mean_glucose=170.0,
        gmi=7.4,
        gv=0.148,
    )
    snapshot = RollingStatsSnapshot(
        patient_id="p",
        window_type="ROLLING_14_DAYS",
        generated_for_date=date(2025, 1, 6),
        windows=(window,),
        metadata={"source": "api"},
    )
    assert snapshot.windows[0].end_date == date(2025, 1, 6)


def test_cgmday_readings_local_converts_timezone():
    timestamps = pd.date_range("2024-01-01", periods=2, freq="5min", tz="UTC")
    readings = pd.DataFrame({"timestamp": timestamps, "glucose_mg_dL": [100, 110]})
    day = CGMDay(
        patient_id="p",
        service_date=date(2024, 1, 1),
        readings=readings,
        local_timezone="America/Los_Angeles",
    )
    local = day.readings_local()
    assert str(local.loc[0, "timestamp"].tzinfo) == "America/Los_Angeles"
