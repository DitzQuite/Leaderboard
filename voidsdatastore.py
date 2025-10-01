"""
voidsdatastore.py

Small client library for interacting with the Voids Datastore API.

Features:
- DatastoreClient class (session, auth header, base_url)
- get_key / update_key methods that handle 202 + polling /status/{request_id}
- Convenience functions get_value and update_value using env var for API key
- Configurable polling interval and timeout
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional, Union

import requests

# Public API
__all__ = [
    "DatastoreClient",
    "get_value",
    "update_value",
    "DatastoreError",
    "AuthenticationError",
    "PollTimeoutError",
]


DEFAULT_BASE_URL = "https://voidsdatastore.net/api/v1/"


# Exceptions
class DatastoreError(Exception):
    """Base exception for Voids Datastore client errors."""


class AuthenticationError(DatastoreError):
    """Raised when an API key is missing or invalid."""


class PollTimeoutError(DatastoreError):
    """Raised when polling the status endpoint times out."""


class DatastoreClient:
    """
    Client for Voids Datastore.

    Args:
        api_key: API key value to send in the `Authorization` header. If None,
            the client will try to read env var VOIDS_DATASTORE_API_KEY then API_KEY.
        base_url: Base URL for the API (default "https://voidsdatastore.net/api/v1/").
        request_timeout: Timeout (seconds) for individual HTTP requests (default 10).
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        request_timeout: float = 10.0,
    ) -> None:
        self.api_key = api_key or os.getenv("VOIDS_DATASTORE_API_KEY") or os.getenv("API_KEY")
        if not self.api_key:
            raise AuthenticationError(
                "API key not provided. Set VOIDS_DATASTORE_API_KEY (or API_KEY) "
                "or pass api_key to DatastoreClient()."
            )

        base_url = base_url or DEFAULT_BASE_URL
        # normalize base_url to always end with a single slash
        if not base_url.endswith("/"):
            base_url = base_url + "/"
        self.base_url = base_url

        self.request_timeout = float(request_timeout)
        self._session = requests.Session()
        # Header uses the raw API key as Authorization value (server expects Authorization header)
        self._session.headers.update({"Authorization": str(self.api_key)})

    def _build_url(self, path: str) -> str:
        # path may begin with or without leading slash
        return f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"

    def _raise_for_status(self, resp: requests.Response) -> None:
        if resp.status_code >= 400:
            # try to include any JSON message if present
            msg = None
            try:
                msg = resp.json()
            except Exception:
                msg = resp.text or None
            raise DatastoreError(f"HTTP {resp.status_code}: {msg}")

    def _poll_status(self, request_id: str, interval: float = 3.0, timeout: float = 60.0) -> Any:
        """
        Poll /status/{request_id} every `interval` seconds until the server returns a
        non-pending response (status 200 with data) or until `timeout` seconds have passed.

        Returns:
            The JSON response body when finished (likely the key data or a final status).
        Raises:
            PollTimeoutError on timeout, DatastoreError on HTTP errors.
        """
        status_url = self._build_url(f"status/{request_id}")
        deadline = time.monotonic() + float(timeout)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PollTimeoutError(f"Polling status for request_id={request_id} timed out after {timeout} seconds.")

            try:
                resp = self._session.get(status_url, timeout=min(self.request_timeout, max(0.1, remaining)))
            except requests.RequestException as e:
                # network issue; wrap in DatastoreError
                raise DatastoreError(f"Network error while polling status: {e}") from e

            # If server returns 200, assume final data is in resp.json()
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError:
                    # Not JSON; return raw text
                    return resp.text

            # If server returns 202 or other 2xx indicating pending, check JSON for {"status": "pending"}
            if resp.status_code == 202:
                # Sleep then continue polling
                # If response explicitly says pending, we honor polling interval unchanged.
                try:
                    data = resp.json()
                    # if server uses {"status": "pending"} or similar, continue
                except Exception:
                    # no json, just sleep and continue
                    pass

                time.sleep(float(interval))
                continue

            # Any other (>=400) -> raise
            if resp.status_code >= 400:
                self._raise_for_status(resp)

            # Fallback: sleep and continue
            time.sleep(float(interval))

    def get_key(
        self,
        game_id: str,
        key: str,
        poll_interval: float = 3.0,
        poll_timeout: float = 60.0,
    ) -> Any:
        """
        Retrieve a key value from datastore for the given game_id and key.

        Behavior:
            - Calls GET /key/{game_id}/{key}
            - If response is 202 with {"requestId": "<id>"}: polls /status/{requestId} every poll_interval seconds until done or poll_timeout.
            - If immediate 200: returns JSON/text directly.

        Returns:
            The final JSON decoded value or raw text returned by the status endpoint.

        Raises:
            AuthenticationError, PollTimeoutError, DatastoreError
        """
        url = self._build_url(f"key/{game_id}/{key}")
        try:
            resp = self._session.get(url, timeout=self.request_timeout)
        except requests.RequestException as e:
            raise DatastoreError(f"Network error while requesting key: {e}") from e

        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError:
                return resp.text

        if resp.status_code == 202:
            # Expecting JSON with requestId
            try:
                data = resp.json()
            except ValueError:
                raise DatastoreError("202 response did not contain JSON with a requestId.")

            request_id = data.get("requestId") or data.get("request_id") or data.get("requestId".lower())
            if not request_id:
                raise DatastoreError("202 response missing 'requestId' field.")

            return self._poll_status(request_id, interval=poll_interval, timeout=poll_timeout)

        # Other statuses -> raise
        self._raise_for_status(resp)

    def update_key(
        self,
        game_id: str,
        key: str,
        value: Union[Dict[str, Any], str, int, float, list, None],
        poll_interval: float = 3.0,
        poll_timeout: float = 60.0,
    ) -> Any:
        """
        Update a key by posting data to /key/{game_id}/{key}.

        Args:
            value: Data to send in the POST body. If it's a dict/list, sends as JSON.
                   If it's a primitive (str/int/float), sends as raw text body.

        Returns:
            The final JSON decoded value or raw text returned by the status endpoint.

        Raises:
            AuthenticationError, PollTimeoutError, DatastoreError
        """
        url = self._build_url(f"key/{game_id}/{key}")

        # Choose appropriate content-type and payload
        headers = {}
        payload = None
        json_payload = None

        if isinstance(value, (dict, list)):
            json_payload = value
        elif value is None:
            # send empty JSON null
            json_payload = None
        else:
            # primitives: send as text/plain
            headers["Content-Type"] = "text/plain"
            payload = str(value)

        try:
            if json_payload is not None:
                resp = self._session.post(url, json=json_payload, headers=headers, timeout=self.request_timeout)
            else:
                resp = self._session.post(url, data=payload, headers=headers, timeout=self.request_timeout)
        except requests.RequestException as e:
            raise DatastoreError(f"Network error while updating key: {e}") from e

        if resp.status_code in (200,):
            try:
                return resp.json()
            except ValueError:
                return resp.text

        if resp.status_code == 202:
            # Expecting {"requestId": "..."}
            try:
                data = resp.json()
            except ValueError:
                raise DatastoreError("202 response did not contain JSON with a requestId.")

            request_id = data.get("requestId") or data.get("request_id")
            if not request_id:
                raise DatastoreError("202 response missing 'requestId' field.")

            return self._poll_status(request_id, interval=poll_interval, timeout=poll_timeout)

        # Other statuses -> raise
        self._raise_for_status(resp)


# Convenience functions
def _build_client(api_key: Optional[str], base_url: Optional[str], request_timeout: float) -> DatastoreClient:
    return DatastoreClient(api_key=api_key, base_url=base_url, request_timeout=request_timeout)


def get_value(
    game_id: str,
    key: str,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    poll_interval: float = 3.0,
    poll_timeout: float = 60.0,
    request_timeout: float = 10.0,
) -> Any:
    """
    Convenience function to get a datastore key without manually creating a client.

    Reads VOIDS_DATASTORE_API_KEY or API_KEY from environment if api_key is None.
    """
    client = _build_client(api_key=api_key, base_url=base_url, request_timeout=request_timeout)
    return client.get_key(game_id=game_id, key=key, poll_interval=poll_interval, poll_timeout=poll_timeout)


def update_value(
    game_id: str,
    key: str,
    value: Union[Dict[str, Any], str, int, float, list, None],
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    *,
    poll_interval: float = 3.0,
    poll_timeout: float = 60.0,
    request_timeout: float = 10.0,
) -> Any:
    """
    Convenience function to update a datastore key without manually creating a client.

    Reads VOIDS_DATASTORE_API_KEY or API_KEY from environment if api_key is None.
    """
    client = _build_client(api_key=api_key, base_url=base_url, request_timeout=request_timeout)
    return client.update_key(game_id=game_id, key=key, value=value, poll_interval=poll_interval, poll_timeout=poll_timeout)


# Simple CLI usage example when run directly
if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="Voids Datastore CLI example")
    parser.add_argument("--base-url", help="Base URL for the API", default=None)
    parser.add_argument("--key", help="The API key (optional; env VOIDS_DATASTORE_API_KEY is used if omitted)", default=None)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_get = sub.add_parser("get", help="Get a datastore key")
    p_get.add_argument("game_id")
    p_get.add_argument("key")

    p_set = sub.add_parser("set", help="Set a datastore key")
    p_set.add_argument("game_id")
    p_set.add_argument("key")
    p_set.add_argument("value", help="Value to set (if JSON-like, provide valid JSON)")

    args = parser.parse_args()

    try:
        if args.cmd == "get":
            result = get_value(args.game_id, args.key, api_key=args.key, base_url=args.base_url)
            print(json.dumps(result, indent=2) if isinstance(result, (dict, list)) else result)
        elif args.cmd == "set":
            # try to parse value as JSON; fallback to plain string
            val = None
            try:
                val = json.loads(args.value)
            except Exception:
                val = args.value
            result = update_value(args.game_id, args.key, val, api_key=args.key, base_url=args.base_url)
            print(json.dumps(result, indent=2) if isinstance(result, (dict, list)) else result)
    except AuthenticationError as e:
        print(f"Authentication error: {e}")
    except PollTimeoutError as e:
        print(f"Polling timed out: {e}")
    except DatastoreError as e:
        print(f"Datastore error: {e}")