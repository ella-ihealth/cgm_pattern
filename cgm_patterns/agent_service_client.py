"""Client helpers for interacting with the agent-service CGM endpoint."""
from __future__ import annotations

from typing import Any

import httpx

DEFAULT_AGENT_SERVICE_CGM_URL = (
    "http://agent-service-svc.ai.svc.cluster.local/draft-chat-response/cgm_info"
)


async def fetch_cgm_info(
    user_id: str,
    patient_id: str,
    *,
    base_url: str = DEFAULT_AGENT_SERVICE_CGM_URL,
    client: httpx.AsyncClient | None = None,
    timeout: float | httpx.Timeout = 10.0,
    extra_headers: dict[str, str] | None = None,
) -> str:
    """Fetch CGM information from the agent-service endpoint.

    Parameters
    ----------
    user_id:
        Identifier for the requesting user (required by the endpoint).
    patient_id:
        Identifier for the patient whose CGM data should be returned.
    base_url:
        Fully qualified URL to the `draft-chat-response/cgm_info` endpoint.
    client:
        Optional shared ``httpx.AsyncClient``. If not provided, a new client is
        created for the request and closed before returning.
    timeout:
        Timeout passed to ``httpx.AsyncClient`` when an internal client is created.
    extra_headers:
        Optional additional headers to include in the request.

    Returns
    -------
    str
        The textual response body from the agent-service endpoint. The current
        service implementation returns a plain-text CGM report.
    """

    payload: dict[str, Any] = {
        "user_id": user_id,
        "patient_id": patient_id,
    }
    headers = {"Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=timeout)
        close_client = True

    try:
        response = await client.post(base_url, json=payload, headers=headers)
        response.raise_for_status()
        return response.text
    finally:
        if close_client:
            await client.aclose()


def fetch_cgm_info_sync(
    user_id: str,
    patient_id: str,
    *,
    base_url: str = DEFAULT_AGENT_SERVICE_CGM_URL,
    client: httpx.Client | None = None,
    timeout: float | httpx.Timeout = 10.0,
    extra_headers: dict[str, str] | None = None,
) -> str:
    """Synchronous wrapper around :func:`fetch_cgm_info`.

    Useful for scripts or environments where ``async`` is not convenient.
    """

    payload: dict[str, Any] = {
        "user_id": user_id,
        "patient_id": patient_id,
    }
    headers = {"Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)

    close_client = False
    if client is None:
        client = httpx.Client(timeout=timeout)
        close_client = True

    try:
        response = client.post(base_url, json=payload, headers=headers)
        response.raise_for_status()
        return response.text
    finally:
        if close_client:
            client.close()


__all__ = [
    "DEFAULT_AGENT_SERVICE_CGM_URL",
    "fetch_cgm_info",
    "fetch_cgm_info_sync",
]
