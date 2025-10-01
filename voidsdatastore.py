"""
Voids Datastore Python Client Library

Supports:
- Get key values from datastore (with async polling until ready).
- Update key values (with async polling until complete).
- Uses environment variables VOIDS_DATASTORE_API_KEY or API_KEY if no key provided.
- Default base_url = https://voidsdatastore.net/api/v1/

Polling is designed to respect rate limits:
- Default poll interval = 5s
- Exponential backoff up to 30s if "pending" repeats
- Honors Retry-After header if present
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


class PollTimeoutError(DatastoreError):
    """Raised when polling timed out before data was ready."""


# -----------------------------
# Constants
# -----------------------------
DEFAULT_BASE_URL = "https://voidsdatastore.net/api/v1/"
DEFAULT_POLL_INTERVAL = 5.0
MAX_BACKOFF_INTERVAL = 30.0


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
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        poll_timeout: float = 60.0,
    ) -> Any:
        """
        Poll /status/{request_id} until the data is ready or timeout.
        Uses exponential backoff if still pending.
        """
        url = self._build_url(f"status/{request_id}")
        deadline = time.monotonic() + float(poll_timeout)
        interval = poll_interval

        while True:
            if time.monotonic() > deadline:
                raise PollTimeoutError(f"Polling for request {request_id} timed out after {poll_timeout}s")

            try:
                resp = self._session.get(url, timeout=self.request_timeout)
            except requests.RequestException as e:
                raise DatastoreError(f"Network error during polling: {e}") from e

            # Always expect 200 with JSON
            try:
                data = resp.json()
            except Exception:
                raise DatastoreError(f"Status endpoint returned non-JSON: {resp.text}")

            # If still pending, wait and retry
            if isinstance(data, dict) and data.get("status") == "pending":
                # Honor Retry-After if present
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait_time = float(retry_after)
                    except ValueError:
                        wait_time = interval
                else:
                    wait_time = interval

                time.sleep(wait_time)

                # Exponential backoff up to max
                interval = min(interval * 2, MAX_BACKOFF_INTERVAL)
                continue

            # Otherwise return the data
            return data

    def get_key(
        self,
        game_id: str,
        key: str,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        poll_timeout: float = 60.0,
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

            return self._poll_status(request_id, poll_interval, poll_timeout)

        raise DatastoreError(f"Unexpected status {resp.status_code}: {resp.text}")

    def update_key(
        self,
        game_id: str,
        key: str,
        value: Union[Dict[str, Any], str, int, float, list, None],
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        poll_timeout: float = 60.0,
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

            return self._poll_status(request_id, poll_interval, poll_timeout)

        raise DatastoreError(f"Unexpected status {resp.status_code}: {resp.text}")


# -----------------------------
# Convenience functions
# -----------------------------
def get_value(
    game_id: str,
    key: str,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    poll_timeout: float = 60.0,
) -> Any:
    client = DatastoreClient(api_key=api_key, base_url=base_url)
    return client.get_key(game_id, key, poll_interval, poll_timeout)


def update_value(
    game_id: str,
    key: str,
    value: Union[Dict[str, Any], str, int, float, list, None],
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    poll_timeout: float = 60.0,
) -> Any:
    client = DatastoreClient(api_key=api_key, base_url=base_url)
    return client.update_key(game_id, key, value, poll_interval, poll_timeout)


# -----------------------------
# Example CLI
# -----------------------------
if __name__ == "__main__":
    import argparse, json

    parser = argparse.ArgumentParser(description="Voids Datastore CLI")
    parser.add_argument("--api-key", help="API key (default: env VOIDS_DATASTORE_API_KEY or API_KEY)", default=None)
    parser.add_argument("--base-url", help="Custom API base URL", default=None)
    sub = parser.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("get", help="Get a key value")
    g.add_argument("game_id")
    g.add_argument("key")

    u = sub.add_parser("set", help="Set a key value")
    u.add_argument("game_id")
    u.add_argument("key")
    u.add_argument("value")

    args = parser.parse_args()
    try:
        if args.cmd == "get":
            res = get_value(args.game_id, args.key, api_key=args.api_key, base_url=args.base_url)
            print(json.dumps(res, indent=2) if isinstance(res, (dict, list)) else res)
        elif args.cmd == "set":
            try:
                val = json.loads(args.value)
            except Exception:
                val = args.value
            res = update_value(args.game_id, args.key, val, api_key=args.api_key, base_url=args.base_url)
            print(json.dumps(res, indent=2) if isinstance(res, (dict, list)) else res)
    except DatastoreError as e:
        print(f"Error: {e}")