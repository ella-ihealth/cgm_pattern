import asyncio
from datetime import date

import pytest

from api_clients.cgm_client import (
    convert_excursion_trend_result,
    convert_rolling_stats_response,
    fetch_excursion_summary,
    fetch_rolling_snapshot,
)
from cgm_patterns.models import ExcursionTrendSummary, RollingStatsSnapshot
from models.cgm_models import (
    CgmExcursionBlock,
    CgmExcursionTrendResult,
    CgmRollingStatsResponse,
    CgmRollingWindow,
    CgmRollingWindowTimeRangePercentage,
)


def _sample_rolling_response() -> CgmRollingStatsResponse:
    window = CgmRollingWindow(
        startDate="2025-09-22",
        endDate="2025-10-06",
        daysWorn=13,
        percentTimeActive=81.5,
        averageGlucose=170.0,
        gmi=7.4,
        gv=0.148,
        windowValid=True,
        timeRangePercentage=[
            CgmRollingWindowTimeRangePercentage(metric=None, percentage=None),
            CgmRollingWindowTimeRangePercentage(metric=None, percentage=None),
        ],
    )
    return CgmRollingStatsResponse(
        patientId="p",
        windowType=None,
        wearThresholdPercent=70.0,
        windows=[window],
    )


def _sample_excursion_result() -> CgmExcursionTrendResult:
    excursion = CgmExcursionBlock(
        startLocal="02:45",
        endLocal="05:15",
        durationMin=150,
        minMgDl=45,
        maxMgDl=65,
        meanMgDl=54,
        direction="hypo",
    )
    return CgmExcursionTrendResult(
        patientId="p",
        startDate="2024-12-15",
        endDate="2025-01-13",
        templateCoverageDays=7,
        lookBackDays=30,
        excursions=[excursion],
    )


def test_convert_rolling_stats_response():
    snapshot = convert_rolling_stats_response(_sample_rolling_response())
    assert isinstance(snapshot, RollingStatsSnapshot)
    assert snapshot.patient_id == "p"
    assert snapshot.windows[0].end_date == date(2025, 10, 6)


def test_convert_excursion_trend_result():
    summary = convert_excursion_trend_result(_sample_excursion_result())
    assert isinstance(summary, ExcursionTrendSummary)
    assert summary.excursions[0].duration_minutes == 150


class _StubClient:
    async def get_cgm_rolling_stats(self, patient_id: str):  # pragma: no cover - simple stub
        return _sample_rolling_response()

    async def get_cgm_excursion_trend(self, patient_id: str):  # pragma: no cover - simple stub
        return _sample_excursion_result()


@pytest.mark.asyncio
async def test_fetch_helpers_use_client():
    client = _StubClient()
    snapshot = await fetch_rolling_snapshot("p", client=client)
    excursion = await fetch_excursion_summary("p", client=client)
    assert snapshot.patient_id == "p"
    assert excursion.template_coverage_days == 7
