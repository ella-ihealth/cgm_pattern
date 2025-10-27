from pathlib import Path
import sys

import httpx
import pytest
import respx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cgm_patterns.agent_service_client import (
    DEFAULT_AGENT_SERVICE_CGM_URL,
    fetch_cgm_info,
    fetch_cgm_info_sync,
)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_cgm_info_returns_response_text():
    expected = "CGM tables"
    route = respx.post(DEFAULT_AGENT_SERVICE_CGM_URL).mock(return_value=httpx.Response(200, text=expected))

    result = await fetch_cgm_info("2", "patient-1")

    assert route.called
    assert result == expected


@pytest.mark.asyncio
@respx.mock
async def test_fetch_cgm_info_raises_for_http_error():
    route = respx.post(DEFAULT_AGENT_SERVICE_CGM_URL).mock(return_value=httpx.Response(500, text="boom"))

    with pytest.raises(httpx.HTTPStatusError):
        await fetch_cgm_info("2", "patient-1")

    assert route.called


@respx.mock
def test_fetch_cgm_info_sync_returns_response_text():
    expected = "CGM tables"
    route = respx.post(DEFAULT_AGENT_SERVICE_CGM_URL).mock(return_value=httpx.Response(200, text=expected))

    result = fetch_cgm_info_sync("2", "patient-1")

    assert route.called
    assert result == expected


@respx.mock
def test_fetch_cgm_info_sync_raises_for_http_error():
    route = respx.post(DEFAULT_AGENT_SERVICE_CGM_URL).mock(return_value=httpx.Response(404, text="missing"))

    with pytest.raises(httpx.HTTPStatusError):
        fetch_cgm_info_sync("2", "patient-1")

    assert route.called
