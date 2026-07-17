"""Serve a live TEI observability snapshot and its static dashboard."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from typing import Any, Protocol
from urllib.parse import parse_qs, unquote, urlparse

from tandemn_efficiency_index.models.cluster_snapshot import ClusterRecord, JobRecord, WorkloadPod
from tandemn_efficiency_index.models.telemetry import MetricSample, MetricSeries, WorkloadTelemetry
from tandemn_efficiency_index.prometheus.dcgm import DCGM_METRICS

LOGGER = logging.getLogger(__name__)
STATIC_PACKAGE = "tandemn_efficiency_index.ui"
STATIC_FILES = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/styles.css": ("styles.css", "text/css; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
}


class ClusterRecordObserver(Protocol):
    """Observer operations required by the observability runtime."""

    @property
    def record(self) -> ClusterRecord: ...

    def collect_tick(self, collected_at: datetime | None = None) -> ClusterRecord: ...


def cluster_record_to_dict(
    record: ClusterRecord,
    window_seconds: int | None = 3600,
    max_points: int = 180,
) -> dict[str, Any]:
    """Return a bounded JSON-ready view of the rolling cluster record."""
    if window_seconds is not None and window_seconds <= 0:
        raise ValueError("window_seconds must be greater than zero")
    if max_points < 2:
        raise ValueError("max_points must be at least two")

    requested_start = (
        record.updated_at - timedelta(seconds=window_seconds)
        if window_seconds is not None
        else record.window_start
    )
    sample_start = max(record.window_start, requested_start)
    jobs = [
        _job_to_dict(
            job,
            sample_start,
            max_points,
            record.updated_at,
            record.sample_interval_seconds,
        )
        for job in sorted(record.jobs.values(), key=lambda item: item.workload_id)
    ]
    unattributed = _telemetry_to_dict(
        record.unattributed_telemetry,
        sample_start,
        max_points,
    )
    all_series = [
        series for job in record.jobs.values() for series in job.telemetry.series.values()
    ] + list(record.unattributed_telemetry.series.values())

    return {
        "started_at": record.started_at.isoformat(),
        "updated_at": record.updated_at.isoformat(),
        "window_start": record.window_start.isoformat(),
        "sample_start": sample_start.isoformat(),
        "sample_interval_seconds": record.sample_interval_seconds,
        "summary": _cluster_summary(record, all_series, sample_start),
        "jobs": jobs,
        "unattributed_telemetry": unattributed,
        "attribution": _attribution_summary(record.unattributed_telemetry, sample_start),
        "missing_metrics": sorted(record.missing_metrics),
    }


def _cluster_summary(
    record: ClusterRecord,
    all_series: Sequence[MetricSeries],
    sample_start: datetime,
) -> dict[str, int]:
    reporting_series = [
        series
        for series in all_series
        if any(sample.timestamp >= sample_start for sample in series.samples)
    ]
    workers = {pod.uid for job in record.jobs.values() for pod in job.workers.values()}
    gpu_devices = {
        (
            series.scope.gpu_uuid,
            series.scope.node_name,
            series.scope.gpu_index,
            series.scope.gpu_instance_id,
        )
        for series in reporting_series
        if series.scope.gpu_uuid is not None or series.scope.gpu_index is not None
    }
    metric_names = {series.metric_name for series in reporting_series}
    return {
        "workload_count": len(record.jobs),
        "worker_count": len(workers),
        "gpu_count": len(gpu_devices),
        "metric_count": len(metric_names),
        "series_count": len(reporting_series),
        "unattributed_series_count": sum(
            series in record.unattributed_telemetry.series.values() for series in reporting_series
        ),
    }


def _job_to_dict(
    job: JobRecord,
    sample_start: datetime,
    max_points: int,
    updated_at: datetime,
    sample_interval_seconds: int,
) -> dict[str, Any]:
    return {
        "workload_id": job.workload_id,
        "workload": job.workload.to_dict(),
        "workers": [
            _worker_to_dict(worker)
            for worker in sorted(job.workers.values(), key=lambda item: item.name)
        ],
        "telemetry": _telemetry_to_dict(job.telemetry, sample_start, max_points),
        "coverage": _coverage_to_dict(
            job,
            sample_start,
            updated_at,
            sample_interval_seconds,
        ),
    }


def _coverage_to_dict(
    job: JobRecord,
    sample_start: datetime,
    updated_at: datetime,
    sample_interval_seconds: int,
) -> dict[str, Any]:
    metric_names = set(DCGM_METRICS)
    observed_devices: set[tuple[str | None, str | None, str | None, str | None]] = set()
    reporting_by_metric: dict[
        str,
        set[tuple[str | None, str | None, str | None, str | None]],
    ] = {}
    series_count: dict[str, int] = {}
    sample_count: dict[str, int] = {}
    latest_sample_at: dict[str, datetime] = {}

    fresh_after = updated_at - timedelta(seconds=sample_interval_seconds * 3)
    for series in job.telemetry.series.values():
        if series.metric_name not in metric_names:
            continue
        samples = [sample for sample in series.samples if sample.timestamp >= sample_start]
        if not samples:
            continue
        device = _device_identity(series)
        if device is not None and samples[-1].timestamp >= fresh_after:
            observed_devices.add(device)
            reporting_by_metric.setdefault(series.metric_name, set()).add(device)
        series_count[series.metric_name] = series_count.get(series.metric_name, 0) + 1
        sample_count[series.metric_name] = sample_count.get(series.metric_name, 0) + len(samples)
        latest = samples[-1].timestamp
        previous_latest = latest_sample_at.get(series.metric_name)
        if previous_latest is None or latest > previous_latest:
            latest_sample_at[series.metric_name] = latest

    configured_gpu_count = max(0, int(job.workload.total_gpus))
    expected_gpu_count = max(configured_gpu_count, len(observed_devices))
    metrics = []
    for metric_name in sorted(metric_names):
        reporting_gpu_count = len(reporting_by_metric.get(metric_name, set()))
        missing_gpu_count = max(0, expected_gpu_count - reporting_gpu_count)
        if reporting_gpu_count == 0:
            status = "missing"
        elif missing_gpu_count:
            status = "partial"
        else:
            status = "complete"
        metric_latest = latest_sample_at.get(metric_name)
        metrics.append(
            {
                "metric_name": metric_name,
                "status": status,
                "expected_gpu_count": expected_gpu_count,
                "reporting_gpu_count": reporting_gpu_count,
                "missing_gpu_count": missing_gpu_count,
                "series_count": series_count.get(metric_name, 0),
                "sample_count": sample_count.get(metric_name, 0),
                "latest_sample_at": metric_latest.isoformat()
                if metric_latest is not None
                else None,
            }
        )

    statuses = {metric["status"] for metric in metrics}
    if "missing" in statuses:
        status = "missing"
    elif "partial" in statuses:
        status = "partial"
    else:
        status = "complete"
    return {
        "status": status,
        "expected_gpu_count": expected_gpu_count,
        "observed_gpu_count": len(observed_devices),
        "metrics": metrics,
    }


def _device_identity(
    series: MetricSeries,
) -> tuple[str | None, str | None, str | None, str | None] | None:
    scope = series.scope
    if scope.gpu_uuid is None and scope.gpu_index is None:
        return None
    return (
        scope.gpu_uuid,
        scope.node_name,
        scope.gpu_index,
        scope.gpu_instance_id,
    )


def _attribution_summary(
    telemetry: WorkloadTelemetry,
    sample_start: datetime,
) -> dict[str, Any]:
    reasons: dict[str, int] = {}
    reporting_series_count = 0
    for series in telemetry.series.values():
        if not any(sample.timestamp >= sample_start for sample in series.samples):
            continue
        reporting_series_count += 1
        reason = series.scope.attribution_method or "unattributed_unknown"
        reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "unattributed_series_count": reporting_series_count,
        "reasons": dict(sorted(reasons.items())),
    }


def _worker_to_dict(worker: WorkloadPod) -> dict[str, Any]:
    result = asdict(worker)
    result["first_seen_at"] = worker.first_seen_at.isoformat()
    result["last_seen_at"] = worker.last_seen_at.isoformat()
    return result


def _telemetry_to_dict(
    telemetry: WorkloadTelemetry,
    sample_start: datetime,
    max_points: int,
) -> dict[str, Any]:
    return {
        "workload_id": telemetry.workload_id,
        "last_sample_at": (
            telemetry.last_sample_at.isoformat() if telemetry.last_sample_at is not None else None
        ),
        "series": [
            _series_to_dict(series, sample_start, max_points)
            for series in sorted(
                telemetry.series.values(),
                key=lambda item: (
                    item.metric_name,
                    item.scope.runtime_role or "",
                    item.scope.pod_uid or "",
                    item.scope.gpu_index or "",
                ),
            )
        ],
    }


def _series_to_dict(
    series: MetricSeries,
    sample_start: datetime,
    max_points: int,
) -> dict[str, Any]:
    samples = [sample for sample in series.samples if sample.timestamp >= sample_start]
    bounded_samples = _downsample(samples, max_points)
    return {
        "series_id": series.series_id,
        "metric_name": series.metric_name,
        "scope": asdict(series.scope),
        "labels": dict(series.labels),
        "samples": [
            {"timestamp": sample.timestamp.isoformat(), "value": sample.value}
            for sample in bounded_samples
        ],
    }


def _downsample(samples: Sequence[MetricSample], max_points: int) -> list[MetricSample]:
    if len(samples) <= max_points:
        return list(samples)

    if max_points == 2:
        return [samples[0], samples[-1]]

    internal = samples[1:-1]
    internal_slots = max_points - 2
    if internal_slots == 1:
        midpoint = (samples[0].value + samples[-1].value) / 2
        extreme = max(internal, key=lambda sample: abs(sample.value - midpoint))
        return [samples[0], extreme, samples[-1]]

    bucket_count = internal_slots // 2
    selected = [samples[0]]
    for bucket_index in range(bucket_count):
        start = bucket_index * len(internal) // bucket_count
        end = (bucket_index + 1) * len(internal) // bucket_count
        bucket = internal[start:end]
        minimum_index = min(range(len(bucket)), key=lambda index: bucket[index].value)
        maximum_index = max(range(len(bucket)), key=lambda index: bucket[index].value)
        for index in sorted({minimum_index, maximum_index}):
            selected.append(bucket[index])
    selected.append(samples[-1])
    return selected


class ObservabilityRuntime:
    """Collect telemetry in the background and serialize it under one lock."""

    def __init__(self, observer: ClusterRecordObserver) -> None:
        self._observer = observer
        self._record = observer.record
        self._lock = threading.Lock()
        self._stopped = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start periodic collection if it is not already running."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stopped.clear()
        self._thread = threading.Thread(
            target=self._collect_loop,
            name="tei-observability-collector",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop periodic collection and wait briefly for the worker."""
        self._stopped.set()
        if self._thread is not None:
            self._thread.join(timeout=2)

    def collect_once(self) -> ClusterRecord:
        """Collect and retain one observation tick."""
        with self._lock:
            self._record = self._observer.collect_tick()
            return self._record

    def snapshot(
        self,
        window_seconds: int | None = 3600,
        max_points: int = 180,
    ) -> dict[str, Any]:
        """Return a consistent bounded snapshot for an API response."""
        with self._lock:
            return cluster_record_to_dict(
                self._record,
                window_seconds=window_seconds,
                max_points=max_points,
            )

    def _collect_loop(self) -> None:
        while not self._stopped.is_set():
            started = time.monotonic()
            try:
                record = self.collect_once()
                interval_seconds = record.sample_interval_seconds
            except Exception:
                LOGGER.exception("TEI telemetry collection tick failed")
                interval_seconds = self._record.sample_interval_seconds
            elapsed = time.monotonic() - started
            self._stopped.wait(max(0.0, interval_seconds - elapsed))


class ObservabilityServer:
    """Own the TEI collection runtime and its local HTTP server."""

    def __init__(
        self,
        observer: ClusterRecordObserver,
        host: str = "127.0.0.1",
        port: int = 8000,
    ) -> None:
        self.runtime = ObservabilityRuntime(observer)
        handler = _handler_for(self.runtime)
        self.httpd = ThreadingHTTPServer((host, port), handler)

    @property
    def address(self) -> tuple[str, int]:
        """Return the bound host and port."""
        host, port = self.httpd.server_address[:2]
        return str(host), int(port)

    def serve_forever(self) -> None:
        """Collect telemetry and serve the dashboard until shutdown."""
        self.runtime.start()
        try:
            self.httpd.serve_forever()
        finally:
            self.runtime.stop()
            self.httpd.server_close()

    def shutdown(self) -> None:
        """Stop the HTTP server and telemetry collection."""
        self.httpd.shutdown()
        self.runtime.stop()


def _handler_for(runtime: ObservabilityRuntime) -> type[BaseHTTPRequestHandler]:
    class ObservabilityRequestHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/api/v1/snapshot":
                self._serve_snapshot(parse_qs(parsed.query))
                return
            static_file = STATIC_FILES.get(parsed.path)
            if static_file is None:
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            self._serve_static(*static_file)

        def _serve_snapshot(self, query: Mapping[str, list[str]]) -> None:
            try:
                window_value = _query_integer(query, "window_seconds", 3600)
                window_seconds: int | None = window_value
                max_points = _query_integer(query, "max_points", 180)
                if window_value == 0:
                    window_seconds = None
                payload = runtime.snapshot(window_seconds, max_points)
            except ValueError as exc:
                self._send_json(
                    {"error": str(exc)},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            self._send_json(payload)

        def _serve_static(self, filename: str, content_type: str) -> None:
            body = resources.files(STATIC_PACKAGE).joinpath(filename).read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def _send_json(
            self,
            payload: Mapping[str, Any],
            status: HTTPStatus = HTTPStatus.OK,
        ) -> None:
            body = json.dumps(payload, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:
            LOGGER.info("TEI observability request: " + format, *args)

    return ObservabilityRequestHandler


def _query_integer(
    query: Mapping[str, list[str]],
    name: str,
    default: int,
) -> int:
    raw_value = query.get(name, [str(default)])[0]
    try:
        return int(unquote(raw_value))
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
