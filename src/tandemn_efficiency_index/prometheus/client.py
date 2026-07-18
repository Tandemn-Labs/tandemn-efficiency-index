"""Minimal Prometheus HTTP API client for range-vector collection."""

from __future__ import annotations

import json
import math
import ssl
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, cast
from urllib import parse, request


@dataclass(frozen=True)
class PrometheusSample:
    """One unnormalized sample returned by Prometheus."""

    timestamp: datetime
    value: float


@dataclass
class PrometheusSeries:
    """One range-vector series returned by Prometheus."""

    labels: dict[str, str]
    samples: list[PrometheusSample]


class PrometheusTransport(Protocol):
    """HTTP operation required by the Prometheus client."""

    def post(self, url: str, data: bytes, timeout_seconds: float) -> bytes: ...


class UrllibPrometheusTransport:
    """Prometheus transport backed by the Python standard library."""

    def __init__(
        self,
        headers: Mapping[str, str] | None = None,
        ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        self._headers = dict(headers or {})
        self._ssl_context = ssl_context

    def post(self, url: str, data: bytes, timeout_seconds: float) -> bytes:
        headers = {"Content-Type": "application/x-www-form-urlencoded", **self._headers}
        http_request = request.Request(
            url,
            data=data,
            headers=headers,
            method="POST",
        )
        with request.urlopen(
            http_request,
            timeout=timeout_seconds,
            context=self._ssl_context,
        ) as response:
            return cast(bytes, response.read())


class PrometheusQueryError(RuntimeError):
    """Raised when Prometheus rejects or returns an invalid query."""


class PrometheusClient:
    """Query an existing in-cluster Prometheus server."""

    def __init__(
        self,
        base_url: str,
        timeout_seconds: float = 10.0,
        transport: PrometheusTransport | None = None,
        bearer_token: str | None = None,
        ca_file: str | None = None,
        insecure_skip_verify: bool = False,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        headers = {"Authorization": f"Bearer {bearer_token}"} if bearer_token else None
        ssl_context = _ssl_context(ca_file, insecure_skip_verify)
        self._transport = transport or UrllibPrometheusTransport(headers, ssl_context)

    def query_range(
        self,
        query: str,
        start: datetime,
        end: datetime,
        step_seconds: int,
    ) -> list[PrometheusSeries]:
        """Query a PromQL expression over an inclusive time range."""
        payload = parse.urlencode(
            {
                "query": query,
                "start": start.timestamp(),
                "end": end.timestamp(),
                "step": step_seconds,
            }
        ).encode()
        url = f"{self._base_url}/api/v1/query_range"
        try:
            body = self._transport.post(url, payload, self._timeout_seconds)
            response = json.loads(body)
        except (OSError, ValueError) as exc:
            raise PrometheusQueryError(f"Prometheus query failed: {exc}") from exc
        return _parse_response(response)


def _ssl_context(ca_file: str | None, insecure_skip_verify: bool) -> ssl.SSLContext | None:
    if ca_file is None and not insecure_skip_verify:
        return None
    context = ssl.create_default_context(cafile=ca_file)
    if insecure_skip_verify:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context


def _parse_response(response: Any) -> list[PrometheusSeries]:
    if not isinstance(response, Mapping) or response.get("status") != "success":
        error = response.get("error") if isinstance(response, Mapping) else "invalid response"
        raise PrometheusQueryError(f"Prometheus query failed: {error}")

    data = response.get("data")
    if not isinstance(data, Mapping) or data.get("resultType") != "matrix":
        raise PrometheusQueryError("Prometheus query did not return a matrix")
    result = data.get("result")
    if not isinstance(result, list):
        raise PrometheusQueryError("Prometheus matrix result is invalid")

    series: list[PrometheusSeries] = []
    for item in result:
        if not isinstance(item, Mapping):
            raise PrometheusQueryError("Prometheus series is invalid")
        metric = item.get("metric")
        values = item.get("values")
        if not isinstance(metric, Mapping) or not isinstance(values, list):
            raise PrometheusQueryError("Prometheus series is invalid")
        labels = {str(name): str(value) for name, value in metric.items()}
        samples = _parse_samples(values)
        if samples:
            series.append(PrometheusSeries(labels=labels, samples=samples))
    return series


def _parse_samples(values: list[Any]) -> list[PrometheusSample]:
    samples: list[PrometheusSample] = []
    for raw_sample in values:
        if not isinstance(raw_sample, list) or len(raw_sample) < 2:
            continue
        try:
            timestamp = float(raw_sample[0])
            value = float(raw_sample[1])
        except (TypeError, ValueError):
            continue
        if not math.isfinite(timestamp) or not math.isfinite(value):
            continue
        samples.append(
            PrometheusSample(
                timestamp=datetime.fromtimestamp(timestamp, UTC),
                value=value,
            )
        )
    return samples
