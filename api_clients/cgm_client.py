"""
CGM (Continuous Glucose Monitoring) API client for making calls to the CGM service.
"""
import logging
from datetime import date
from typing import Optional, Sequence

from api_clients.uc_backend_client import UCBackendClient
from models.cgm_models import (
    CgmExcursionTrendRequest,
    CgmExcursionTrendResult,
    CgmRollingStatsRequest,
    CgmRollingStatsResponse,
)

from cgm_patterns.models import (
    ExcursionEvent,
    ExcursionTrendSummary,
    RollingStatsSnapshot,
    RollingWindowSummary,
)

CGM_EXCURSION_TREND_ENDPOINT = "/cgm/agp/excursion-trend"
CGM_ROLLING_STATS_ENDPOINT = "/cgm/rolling-stats"


class CGMClient:
    """
    Client for making API calls to the CGM service via UCBackendClient.
    """

    def __init__(self, client: Optional[UCBackendClient] = None):
        self.client = client or UCBackendClient()

    async def get_cgm_excursion_trend(self, patient_id: str) -> CgmExcursionTrendResult:
        """Get CGM excursion trend with strict validation and logging."""
        request_data = CgmExcursionTrendRequest(patientId=patient_id)
        data = await self.client._make_request(
            method="POST",
            endpoint=CGM_EXCURSION_TREND_ENDPOINT,
            json_data=request_data.model_dump(),
        )

        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected non-JSON response from {CGM_EXCURSION_TREND_ENDPOINT}: {data!r}")
        code = data.get("code")
        payload = data.get("data")
        if code not in (0, 200) or payload in (None, {}, []):
            logging.error(
                f"CGM excursion trend failed: code={code}, endpoint={CGM_EXCURSION_TREND_ENDPOINT}, body={data!r}"
            )
            raise RuntimeError(
                f"CGM excursion trend API error (code={code})"
            )
        # Accept either wrapped payload or flat
        payload = payload if payload is not None else data
        return CgmExcursionTrendResult(**payload)

    async def get_cgm_rolling_stats(self, patient_id: str) -> CgmRollingStatsResponse:
        """Get CGM rolling stats with strict validation and logging."""
        request_data = CgmRollingStatsRequest(patientId=patient_id)
        data = await self.client._make_request(
            method="POST",
            endpoint=CGM_ROLLING_STATS_ENDPOINT,
            json_data=request_data.model_dump(),
        )

        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected non-JSON response from {CGM_ROLLING_STATS_ENDPOINT}: {data!r}")
        code = data.get("code")
        payload = data.get("data")
        if code not in (0, 200) or payload in (None, {}, []):
            logging.error(
                f"CGM rolling stats failed: code={code}, endpoint={CGM_ROLLING_STATS_ENDPOINT}, body={data!r}"
            )
            raise RuntimeError(
                f"CGM rolling stats API error (code={code})"
            )
        payload = payload if payload is not None else data
        return CgmRollingStatsResponse(**payload)

    async def get_cgm_data(self, patient_id: str):
        """Convenience wrapper to fetch both excursion trend and rolling stats."""
        excursion_result = await self.get_cgm_excursion_trend(patient_id)
        rolling_stats_result = await self.get_cgm_rolling_stats(patient_id)
        return excursion_result, rolling_stats_result



_client: CGMClient | None = None


def _get_client() -> CGMClient:
    global _client
    if _client is None:
        _client = CGMClient()
    return _client

async def get_cgm_excursion_trend(patient_id: str) -> CgmExcursionTrendResult:
    return await _get_client().get_cgm_excursion_trend(patient_id)

async def get_cgm_rolling_stats(patient_id: str) -> CgmRollingStatsResponse:
    return await _get_client().get_cgm_rolling_stats(patient_id)

async def get_cgm_data(patient_id: str):
    return await _get_client().get_cgm_data(patient_id)


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def convert_rolling_stats_response(response: CgmRollingStatsResponse) -> RollingStatsSnapshot:
    windows: list[RollingWindowSummary] = []
    for window in response.windows or []:
        percentages: dict[str, float] = {}
        if window.timeRangePercentage:
            for entry in window.timeRangePercentage:
                metric_key = entry.metric.value if entry.metric else None
                if metric_key and entry.percentage is not None:
                    percentages[metric_key] = entry.percentage
        extra: dict[str, float] = {}
        summary = RollingWindowSummary(
            patient_id=response.patientId or "",
            start_date=_parse_date(window.startDate),
            end_date=_parse_date(window.endDate) or date.today(),
            days_worn=window.daysWorn,
            percent_time_active=window.percentTimeActive,
            mean_glucose=window.averageGlucose,
            gmi=window.gmi,
            gv=window.gv,
            time_percentages=percentages,
            extra_metrics=extra,
            window_valid=window.windowValid,
        )
        windows.append(summary)

    metadata: dict[str, float] = {}
    if response.wearThresholdPercent is not None:
        metadata["wear_threshold_percent"] = response.wearThresholdPercent

    return RollingStatsSnapshot(
        patient_id=response.patientId or "",
        window_type=response.windowType.value if response.windowType else None,
        generated_for_date=None,
        windows=tuple(windows),
        metadata=metadata,
    )


def convert_excursion_trend_result(result: CgmExcursionTrendResult) -> ExcursionTrendSummary:
    excursions: list[ExcursionEvent] = []
    for block in result.excursions or []:
        excursions.append(
            ExcursionEvent(
                start_local=block.startLocal or "",
                end_local=block.endLocal or "",
                duration_minutes=float(block.durationMin or 0),
                min_mg_dl=float(block.minMgDl or 0),
                max_mg_dl=float(block.maxMgDl or 0),
                mean_mg_dl=float(block.meanMgDl) if block.meanMgDl is not None else None,
                direction=block.direction,
            )
        )

    return ExcursionTrendSummary(
        patient_id=result.patientId or "",
        start_date=_parse_date(result.startDate) or date.today(),
        end_date=_parse_date(result.endDate) or date.today(),
        template_coverage_days=result.templateCoverageDays,
        lookback_days=result.lookBackDays,
        excursions=tuple(excursions),
    )


async def fetch_rolling_snapshot(patient_id: str, client: Optional[CGMClient] = None) -> RollingStatsSnapshot:
    cgm_client = client or _get_client()
    response = await cgm_client.get_cgm_rolling_stats(patient_id)
    return convert_rolling_stats_response(response)


async def fetch_excursion_summary(patient_id: str, client: Optional[CGMClient] = None) -> ExcursionTrendSummary:
    cgm_client = client or _get_client()
    result = await cgm_client.get_cgm_excursion_trend(patient_id)
    return convert_excursion_trend_result(result)
