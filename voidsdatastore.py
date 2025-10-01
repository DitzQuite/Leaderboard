"""
Voids Datastore Python Client Library (Simplified Polling)

Supports:
- Get key values from datastore.
- Update key values.
- Uses environment variables VOIDS_DATASTORE_API_KEY or API_KEY if no key provided.
- Default base_url = https://voidsdatastore.net/api/v1/

Polling includes a short delay (default 5s) to prevent spam requests.
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional, Union

import requests


# -----------------------------
# Exceptions
# -----------------------------
class DatastoreError(Exception):
    """Base exception for datastore errors."""


class AuthenticationError(DatastoreError):
    """Raised when API key is missing or invalid."""


# -----------------------------
# Constants
# -----------------------------
DEFAULT_BASE_URL = "https://voidsdatastore.net/api/v1/"
# Minimum poll interval to prevent accidental spam requests
MIN_POLL_INTERVAL = 5.0


# -----------------------------
# Client
# -----------------------------
class DatastoreClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        request_timeout: float = 10.0,
    ) -> None:
        self.api_key = api_key or os.getenv("VOIDS_DATASTORE_API_KEY") or os.getenv("API_KEY")
        if not self.api_key:
            raise AuthenticationError(
                "API key required. Set VOIDS_DATASTORE_API_KEY (or API_KEY) or pass api_key explicitly."
            )

        base_url = base_url or DEFAULT_BASE_URL
        if not base_url.endswith("/"):
            base_url += "/"
        self.base_url = base_url

        self.request_timeout = float(request_timeout)
        self._session = requests.Session()
        self._session.headers.update({"Authorization": str(self.api_key)})

    def _build_url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"

    def _poll_status(
        self,
        request_id: str,
    ) -> Any:
        """
        Poll /status/{request_id} repeatedly until data is ready.
        Includes a MIN_POLL_INTERVAL delay to prevent spam.
        """
        url = self._build_url(f"status/{request_id}")

        while True:
            try:
                resp = self._session.get(url, timeout=self.request_timeout)
            except requests.RequestException as e:
                raise DatastoreError(f"Network error during polling: {e}") from e

            print(resp.text)
            # Check for a successful response (200 OK)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                except Exception:
                    data = {"status": "error", "message": resp.text}

                # Check if the status is still 'pending'
                if isinstance(data, dict) and data.get("status") == "pending":
                    # Critical: Wait for a short time to prevent spam requests
                    print(f"Request {request_id} pending. Waiting {MIN_POLL_INTERVAL}s...")
                    time.sleep(MIN_POLL_INTERVAL)
                    continue  # Poll again

                # If not pending, return the result
                return data

            # Handle unexpected status codes
            raise DatastoreError(f"Unexpected status {resp.status_code} during polling: {resp.text}")

    def get_key(
        self,
        game_id: str,
        key: str,
    ) -> Any:
        """
        Get a key value from the datastore.
        """
        url = self._build_url(f"key/{game_id}/{key}")
        try:
            resp = self._session.get(url, timeout=self.request_timeout)
        except requests.RequestException as e:
            raise DatastoreError(f"Network error on GET key: {e}") from e

        if resp.status_code == 200:
            try:
                return resp.json()
            except Exception:
                return resp.text

        if resp.status_code == 202:
            try:
                data = resp.json()
                request_id = data.get("requestId")
            except Exception:
                raise DatastoreError("202 response missing valid JSON with requestId")

            if not request_id:
                raise DatastoreError("202 response missing 'requestId' field")

            # Start polling
            return self._poll_status(request_id)

        raise DatastoreError(f"Unexpected status {resp.status_code}: {resp.text}")

    def update_key(
        self,
        game_id: str,
        key: str,
        value: Union[Dict[str, Any], str, int, float, list, None],
    ) -> Any:
        """
        Update a key value in the datastore.
        """
        url = self._build_url(f"key/{game_id}/{key}")

        headers = {}
        json_payload = None
        data_payload = None

        if isinstance(value, (dict, list)):
            json_payload = value
        elif value is None:
            json_payload = None
        else:
            headers["Content-Type"] = "text/plain"
            data_payload = str(value)

        try:
            if json_payload is not None:
                resp = self._session.post(url, json=json_payload, headers=headers, timeout=self.request_timeout)
            else:
                resp = self._session.post(url, data=data_payload, headers=headers, timeout=self.request_timeout)
        except requests.RequestException as e:
            raise DatastoreError(f"Network error on POST key: {e}") from e

        if resp.status_code == 200:
            try:
                return resp.json()
            except Exception:
                return resp.text

        if resp.status_code == 202:
            try:
                data = resp.json()
                request_id = data.get("requestId")
            except Exception:
                raise DatastoreError("202 response missing valid JSON with requestId")

            if not request_id:
                raise DatastoreError("202 response missing 'requestId' field")

            # Start polling
            return self._poll_status(request_id)

        raise DatastoreError(f"Unexpected status {resp.status_code}: {resp.text}")


# -----------------------------
# Convenience functions
# -----------------------------
def get_value(
    game_id: str,
    key: str,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
) -> Any:
    """Convenience function to get a value using a short-lived client."""
    client = DatastoreClient(api_key=api_key, base_url=base_url)
    return client.get_key(game_id, key)


def update_value(
    game_id: str,
    key: str,
    value: Union[Dict[str, Any], str, int, float, list, None],
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
) -> Any:
    """Convenience function to update a value using a short-lived client."""
    client = DatastoreClient(api_key=api_key, base_url=base_url)
    return client.update_key(game_id, key, value)