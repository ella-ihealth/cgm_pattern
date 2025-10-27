"""
UC Backend API client for making calls to the Unified Care backend service.
"""
import os
import logging
import httpx

# get UC backend environment variables
UC_BACKEND_API_BASE_URL = os.getenv("AI_RAG_UC_BACKEND_API_BASE_URL")
UC_BACKEND_SESSION_TOKEN = os.getenv("AI_RAG_UC_BACKEND_SESSION_TOKEN")
UC_BACKEND_ENV = os.getenv("AI_RAG_ENVIRONMENT", "dev")

class UcBackendService:
    def __init__(self):
        self.url = UC_BACKEND_API_BASE_URL
        self.session_token = UC_BACKEND_SESSION_TOKEN
        if not self.url or not self.session_token:
            raise ValueError(f"###### [UC backend] base URL/session token not set for environment: {UC_BACKEND_ENV}")

class UCBackendClient:
    def __init__(self):
        self.base_url = UC_BACKEND_API_BASE_URL
        self.session_token = UC_BACKEND_SESSION_TOKEN
        if not self.base_url or not self.session_token:
            raise ValueError(f"###### [UC backend] base URL/session token not set for environment: {UC_BACKEND_ENV}")

        self.headers = {
            "content-type": "application/json",
            "x-session-token": self.session_token
        }

    async def _make_request(self, method: str, endpoint: str, params=None, json_data=None):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        timeout = httpx.Timeout(30.0, connect=10.0)

        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
                response = await client.request(
                    method=method.upper(),
                    url=url,
                    params=params,
                    json=json_data,
                    headers=self.headers
                )
                logging.info(f"Request {url} completed with status: {response.status_code}")
                response.raise_for_status()
                return response.json() if response.text else {}
        except httpx.TimeoutException as e:
            logging.error(f"Timeout error calling UC Backend API {method} {url}: {e}")
            raise
        except httpx.RequestError as e:
            logging.error(f"Request error calling UC Backend API {method} {url}: {e}")
            raise
        except httpx.HTTPStatusError as e:
            logging.error(f"HTTP error calling UC Backend API {method} {url}: {e}")
            logging.error(f"Response status: {e.response.status_code}")
            logging.error(f"Response text: {e.response.text}")
            raise

    async def get_care_notes(self, member_id: str) -> dict:
        """
        Get care notes for a specific member.
        
        Args:
            member_id: The member ID to search care notes for
            
        Returns:
            dict: The care notes response from the API
        """
        endpoint = "care-note/search"


        # updatedAt desc with time range filter
        sorts = [{"direction": "DESC", "property": "updatedAt"}]
        json_data = {
            "filter": {
                "memberId": member_id,
                # "updatedAt": {
                #     "$gte": "2025-07-01T00:00:00.000Z",
                #     "$lte": "2025-09-01T23:59:59.999Z"
                # }
            },
            "pageInfo": {
                "pagination": False,
                # "page": 1,
                # "size": 3,
                "sort": sorts,
            }
        }
        
        logging.info(f"Fetching care notes for member {member_id}")
        return await self._make_request("POST", endpoint, json_data=json_data)


    async def get_billable_monthly_time(self, member_id: str, month_of_year: int) -> dict:
        """
        Get billable monthly time for a specific member.
        
        Args:
            member_id: The member ID to get billing info for
            month_of_year: The month and year in format YYYYMM (e.g., 202509)
            
        Returns:
            dict: The billable monthly time response from the API
        """
        endpoint = "billable-monthly-time/current"
        json_data = {
            "memberId": member_id,
            "monthOfYear": month_of_year
        }
        
        logging.info(f"Fetching billable monthly time for member {member_id} for month {month_of_year}")
        return await self._make_request("POST", endpoint, json_data=json_data)
