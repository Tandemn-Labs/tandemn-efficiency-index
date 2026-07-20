import json
from collections.abc import Mapping

import pytest

from tandemn_efficiency_index.control import ControlPlaneError, TeiControlClient


class FakeTransport:
    def __init__(self, responses: list[dict[str, object]]) -> None:
        self.responses = responses
        self.requests: list[tuple[str, str, Mapping[str, str], float]] = []

    def send(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes:
        self.requests.append((method, url, headers, timeout_seconds))
        return json.dumps(self.responses.pop(0)).encode()


def test_control_client_maps_read_and_lifecycle_endpoints() -> None:
    transport = FakeTransport(
        [
            {"lifecycle": "running"},
            {"resources": {}},
            {"report_type": "prometheus_range_report"},
            {"lifecycle": "stopped"},
        ]
    )
    client = TeiControlClient(
        "http://tei.example/",
        token="secret",
        timeout_seconds=3,
        transport=transport,
    )

    assert client.status()["lifecycle"] == "running"
    assert client.resources() == {"resources": {}}
    assert client.snapshot(3600, 180)["report_type"] == "prometheus_range_report"
    assert client.stop_observation()["lifecycle"] == "stopped"

    assert transport.requests == [
        (
            "GET",
            "http://tei.example/api/v1/status",
            {"Accept": "application/json", "Authorization": "Bearer secret"},
            3,
        ),
        (
            "GET",
            "http://tei.example/api/v1/resources",
            {"Accept": "application/json", "Authorization": "Bearer secret"},
            3,
        ),
        (
            "GET",
            "http://tei.example/api/v1/snapshot?window_seconds=3600&max_points=180",
            {"Accept": "application/json", "Authorization": "Bearer secret"},
            3,
        ),
        (
            "POST",
            "http://tei.example/api/v1/observation/stop",
            {"Accept": "application/json", "Authorization": "Bearer secret"},
            3,
        ),
    ]


def test_control_client_rejects_non_object_response() -> None:
    transport = FakeTransport([])

    def invalid_send(
        method: str,
        url: str,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes:
        return b"[]"

    transport.send = invalid_send  # type: ignore[method-assign]
    client = TeiControlClient("http://tei.example", transport=transport)

    with pytest.raises(ControlPlaneError, match="invalid control-plane response"):
        client.status()


def test_control_client_returns_unavailable_readiness_document() -> None:
    class UnavailableTransport:
        def send(
            self,
            method: str,
            url: str,
            headers: Mapping[str, str],
            timeout_seconds: float,
        ) -> bytes:
            raise ControlPlaneError(
                "Service Unavailable",
                status_code=503,
                payload={"ready": False, "lifecycle": "running"},
            )

    client = TeiControlClient("http://tei.example", transport=UnavailableTransport())

    assert client.readiness() == {"ready": False, "lifecycle": "running"}
    with pytest.raises(ControlPlaneError):
        client.status()
