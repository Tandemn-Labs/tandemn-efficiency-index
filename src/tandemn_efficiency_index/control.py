"""HTTP client for the TEI observability control plane."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Protocol, cast
from urllib import error, request


class ControlPlaneError(RuntimeError):
    """Raised when the TEI control plane cannot complete a request."""

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload


class ControlTransport(Protocol):
    """HTTP operation used by the control-plane client."""

    def send(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes: ...


class UrllibControlTransport:
    """Control-plane transport backed by the Python standard library."""

    def send(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes:
        request_body = b"" if method == "POST" else None
        http_request = request.Request(
            url,
            data=request_body,
            headers=dict(headers),
            method=method,
        )
        try:
            with request.urlopen(http_request, timeout=timeout_seconds) as response:
                return cast(bytes, response.read())
        except error.HTTPError as exc:
            body = exc.read()
            response_payload = _json_object(body)
            message = (
                str(response_payload.get("error"))
                if response_payload and response_payload.get("error")
                else str(exc)
            )
            raise ControlPlaneError(message, exc.code, response_payload) from exc
        except error.URLError as exc:
            raise ControlPlaneError(f"Unable to reach TEI at {url}: {exc.reason}") from exc


class TeiControlClient:
    """Call TEI read and observation-lifecycle endpoints."""

    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        timeout_seconds: float = 10.0,
        transport: ControlTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._token = token
        self._timeout_seconds = timeout_seconds
        self._transport = transport or UrllibControlTransport()

    def health(self) -> dict[str, Any]:
        """Return process liveness state."""
        return self._request_allowing_unavailable("/healthz")

    def readiness(self) -> dict[str, Any]:
        """Return dependency and collection readiness state."""
        return self._request_allowing_unavailable("/readyz")

    def status(self) -> dict[str, Any]:
        """Return the complete control-plane status document."""
        return self._request("GET", "/api/v1/status")

    def resources(self) -> dict[str, Any]:
        """Return the available Kubernetes workload resource map."""
        return self._request("GET", "/api/v1/resources")

    def snapshot(self, window_seconds: int, max_points: int) -> dict[str, Any]:
        """Return a bounded observability snapshot."""
        path = f"/api/v1/snapshot?window_seconds={window_seconds}&max_points={max_points}"
        return self._request("GET", path)

    def start_observation(self) -> dict[str, Any]:
        """Start or resume periodic observation collection."""
        return self._request("POST", "/api/v1/observation/start")

    def stop_observation(self) -> dict[str, Any]:
        """Stop periodic observation collection while leaving the API available."""
        return self._request("POST", "/api/v1/observation/stop")

    def restart_observation(self) -> dict[str, Any]:
        """Restart periodic observation collection."""
        return self._request("POST", "/api/v1/observation/restart")

    def _request(self, method: str, path: str) -> dict[str, Any]:
        headers = {"Accept": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        body = self._transport.send(
            method,
            f"{self.base_url}{path}",
            headers,
            self._timeout_seconds,
        )
        try:
            payload = json.loads(body)
        except (TypeError, ValueError) as exc:
            raise ControlPlaneError("TEI returned an invalid JSON response") from exc
        if not isinstance(payload, dict):
            raise ControlPlaneError("TEI returned an invalid control-plane response")
        return payload

    def _request_allowing_unavailable(self, path: str) -> dict[str, Any]:
        try:
            return self._request("GET", path)
        except ControlPlaneError as exc:
            if exc.status_code == 503 and exc.payload is not None:
                return exc.payload
            raise


def _json_object(body: bytes) -> dict[str, Any] | None:
    try:
        payload = json.loads(body)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload
