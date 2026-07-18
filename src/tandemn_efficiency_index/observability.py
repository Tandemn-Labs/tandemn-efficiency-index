"""Serve a live TEI observability snapshot and its static dashboard."""

from __future__ import annotations

import json
import logging
import secrets
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

from tandemn_efficiency_index.models.cluster_snapshot import (
    ClusterRecord,
    JobRecord,
    WorkloadPod,
)
from tandemn_efficiency_index.models.observation import ObservationState, WorkloadRevision
from tandemn_efficiency_index.models.telemetry import MetricSample, MetricSeries, WorkloadTelemetry
from tandemn_efficiency_index.prometheus.generic import (
    DCGM_REQUIRED_METRICS,
    NORMALIZED_INFERENCE_METRICS,
)

LOGGER = logging.getLogger(__name__)
STATIC_PACKAGE = "tandemn_efficiency_index.ui"
STATIC_FILES = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/styles.css": ("styles.css", "text/css; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
}
MAX_WINDOW_SECONDS = 24 * 60 * 60
MAX_POINTS = 2_000


class ClusterRecordObserver(Protocol):
    """Observer operations required by the observability runtime."""

    @property
    def record(self) -> ClusterRecord: ...

    def collect_tick(
        self,
        collected_at: datetime | None = None,
    ) -> ClusterRecord | ObservationState: ...


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
    expected_dcgm_metrics = set(DCGM_REQUIRED_METRICS)
    jobs = [
        _job_to_dict(
            job,
            record.workload_revisions.get(job.workload_id, []),
            sample_start,
            max_points,
            record.updated_at,
            record.sample_interval_seconds,
            expected_dcgm_metrics,
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
        "report_type": "prometheus_range_report",
        "observation_id": record.observation_id,
        "observation_ends_at": (
            record.observation_ends_at.isoformat()
            if record.observation_ends_at is not None
            else None
        ),
        "started_at": record.started_at.isoformat(),
        "updated_at": record.updated_at.isoformat(),
        "window_start": record.window_start.isoformat(),
        "sample_start": sample_start.isoformat(),
        "sample_interval_seconds": record.sample_interval_seconds,
        "summary": _cluster_summary(record, all_series, sample_start),
        "jobs": jobs,
        "runtime_job_keys": [
            {
                "runtime": job_key.runtime,
                "namespace": job_key.namespace,
                "key": job_key.key,
                "workload_id": job_key.workload_id,
                "runtime_state": job_key.runtime_state,
                "valid_from": job_key.valid_from.isoformat(),
                "valid_to": (
                    job_key.valid_to.isoformat() if job_key.valid_to is not None else None
                ),
            }
            for job_key in record.runtime_job_keys
        ],
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
        "workload_count": sum(job.active for job in record.jobs.values()),
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
    job_revisions: Sequence[WorkloadRevision],
    sample_start: datetime,
    max_points: int,
    updated_at: datetime,
    sample_interval_seconds: int,
    expected_dcgm_metrics: set[str],
) -> dict[str, Any]:
    return {
        "workload_id": job.workload_id,
        "active": job.active,
        "removed_at": job.removed_at.isoformat() if job.removed_at is not None else None,
        "workload": job.workload.to_dict(),
        "workload_revisions": [
            {
                "revision_id": revision.revision_id,
                "valid_from": revision.valid_from.isoformat(),
                "valid_to": revision.valid_to.isoformat()
                if revision.valid_to is not None
                else None,
                "configuration": revision.configuration,
            }
            for revision in job_revisions
        ],
        "workers": [
            _worker_to_dict(worker)
            for worker in sorted(job.workers.values(), key=lambda item: item.name)
        ],
        "telemetry": _telemetry_to_dict(
            job.telemetry,
            sample_start,
            max_points,
            job_revisions,
        ),
        "coverage": _coverage_to_dict(
            job,
            sample_start,
            updated_at,
            sample_interval_seconds,
            expected_dcgm_metrics,
        ),
        "inference_coverage": _inference_coverage_to_dict(
            job,
            sample_start,
            updated_at,
            sample_interval_seconds,
        ),
        "intent_evaluation": _intent_evaluation(job, sample_start),
    }


def _intent_evaluation(job: JobRecord, sample_start: datetime) -> dict[str, Any] | None:
    intent = job.workload.declared_intent
    if intent is None or not intent.slo:
        return None
    metric_by_slo = {
        "ttft_ms": "p99_ttft_ms",
        "itl_ms": "p99_tpot_ms",
    }
    results = []
    for slo_name, target_value in sorted(intent.slo.items()):
        metric_name = metric_by_slo.get(slo_name)
        observed_values = [
            series.samples[-1].value
            for series in job.telemetry.series.values()
            if series.metric_name == metric_name
            and series.samples
            and series.samples[-1].timestamp >= sample_start
        ]
        observed = max(observed_values) if observed_values else None
        target = _numeric(target_value)
        if metric_name is None or target is None or observed is None:
            status = "unavailable"
        else:
            status = "met" if observed <= target else "exceeded"
        results.append(
            {
                "slo": slo_name,
                "metric_name": metric_name,
                "target": target,
                "observed": observed,
                "status": status,
            }
        )
    return {"status": _intent_status(results), "objectives": results}


def _intent_status(results: list[dict[str, Any]]) -> str:
    statuses = {str(result["status"]) for result in results}
    if "exceeded" in statuses:
        return "exceeded"
    if statuses == {"met"}:
        return "met"
    return "unavailable"


def _numeric(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coverage_to_dict(
    job: JobRecord,
    sample_start: datetime,
    updated_at: datetime,
    sample_interval_seconds: int,
    metric_names: set[str],
) -> dict[str, Any]:
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


def _inference_coverage_to_dict(
    job: JobRecord,
    sample_start: datetime,
    updated_at: datetime,
    sample_interval_seconds: int,
) -> dict[str, Any]:
    """Report whether every normalized inference signal is present and fresh."""
    fresh_after = updated_at - timedelta(seconds=sample_interval_seconds * 3)
    latest_by_metric: dict[str, datetime] = {}
    for series in job.telemetry.series.values():
        if series.metric_name not in NORMALIZED_INFERENCE_METRICS:
            continue
        samples = [sample for sample in series.samples if sample.timestamp >= sample_start]
        if not samples:
            continue
        latest = samples[-1].timestamp
        previous = latest_by_metric.get(series.metric_name)
        if previous is None or latest > previous:
            latest_by_metric[series.metric_name] = latest

    metrics = []
    for metric_name in NORMALIZED_INFERENCE_METRICS:
        metric_latest = latest_by_metric.get(metric_name)
        status = (
            "complete" if metric_latest is not None and metric_latest >= fresh_after else "missing"
        )
        metrics.append(
            {
                "metric_name": metric_name,
                "status": status,
                "latest_sample_at": (
                    metric_latest.isoformat() if metric_latest is not None else None
                ),
            }
        )
    return {
        "status": "complete"
        if all(metric["status"] == "complete" for metric in metrics)
        else "missing",
        "metrics": metrics,
    }


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
    revisions: Sequence[WorkloadRevision] = (),
) -> dict[str, Any]:
    return {
        "workload_id": telemetry.workload_id,
        "last_sample_at": (
            telemetry.last_sample_at.isoformat() if telemetry.last_sample_at is not None else None
        ),
        "series": [
            _series_to_dict(series, sample_start, max_points, revisions)
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
    revisions: Sequence[WorkloadRevision],
) -> dict[str, Any]:
    samples = [sample for sample in series.samples if sample.timestamp >= sample_start]
    bounded_samples = _downsample(samples, max_points)
    return {
        "series_id": series.series_id,
        "metric_name": series.metric_name,
        "scope": asdict(series.scope),
        "labels": dict(series.labels),
        "samples": [
            {
                "timestamp": sample.timestamp.isoformat(),
                "value": sample.value,
                "workload_revision_id": _revision_at(sample.timestamp, revisions),
            }
            for sample in bounded_samples
        ],
    }


def _revision_at(timestamp: datetime, revisions: Sequence[WorkloadRevision]) -> int | None:
    for revision in revisions:
        if revision.valid_from <= timestamp and (
            revision.valid_to is None or timestamp <= revision.valid_to
        ):
            return revision.revision_id
    return None


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
        self._lifecycle_lock = threading.RLock()
        self._stopped = threading.Event()
        self._thread: threading.Thread | None = None
        self._closed = False
        self._desired_state = "stopped"
        self._last_transition_at = datetime.now().astimezone()
        self._last_transition_error: str | None = None
        self._last_tick_started_at: datetime | None = None
        self._last_tick_completed_at: datetime | None = None
        self._last_tick_error: str | None = None

    def start(self) -> None:
        """Start periodic collection if it is not already running."""
        with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("TEI observability runtime is closed")
            if self._thread is not None and self._thread.is_alive():
                self._desired_state = "running"
                return
            self._desired_state = "running"
            self._stopped.clear()
            self._thread = threading.Thread(
                target=self._collect_loop,
                name="tei-observability-collector",
                daemon=True,
            )
            try:
                self._thread.start()
            except Exception as exc:
                self._desired_state = "stopped"
                self._last_transition_error = str(exc)
                self._last_transition_at = datetime.now().astimezone()
                raise
            self._last_transition_error = None
            self._last_transition_at = datetime.now().astimezone()

    def pause(self) -> None:
        """Stop periodic collection while keeping the API and observer available."""
        with self._lifecycle_lock:
            self._desired_state = "stopped"
            self._stopped.set()
            thread = self._thread
            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout=30)
                if thread.is_alive():
                    message = "TEI collection did not stop before the shutdown deadline"
                    self._last_transition_error = message
                    self._last_transition_at = datetime.now().astimezone()
                    LOGGER.warning(message)
                    raise RuntimeError(message)
            self._thread = None
            self._last_transition_error = None
            self._last_transition_at = datetime.now().astimezone()

    def restart(self) -> None:
        """Restart periodic collection without closing durable resources."""
        self.pause()
        self.start()

    def stop(self) -> None:
        """Permanently stop collection and close the observer."""
        with self._lifecycle_lock:
            self.pause()
            if not self._closed:
                close = getattr(self._observer, "close", None)
                if callable(close):
                    close()
                self._closed = True
                self._last_transition_at = datetime.now().astimezone()

    def collect_once(self) -> ClusterRecord | ObservationState:
        """Collect and retain one observation tick."""
        with self._lock:
            self._last_tick_started_at = datetime.now().astimezone()
            try:
                collected = self._observer.collect_tick()
            except Exception as exc:
                self._last_tick_error = str(exc)
                raise
            self._last_tick_completed_at = datetime.now().astimezone()
            self._last_tick_error = None
            if isinstance(collected, ClusterRecord):
                self._record = collected
            else:
                self._record = self._observer.record
            return collected

    def snapshot(
        self,
        window_seconds: int | None = 3600,
        max_points: int = 180,
    ) -> dict[str, Any]:
        """Return a consistent bounded snapshot for an API response."""
        with self._lock:
            live_record = getattr(self._observer, "live_record", None)
            if callable(live_record):
                record = live_record(window_seconds)
                return cluster_record_to_dict(
                    record,
                    window_seconds=None,
                    max_points=max_points,
                )
            return cluster_record_to_dict(
                self._record,
                window_seconds=window_seconds,
                max_points=max_points,
            )

    def status(self) -> dict[str, Any]:
        """Return process and collector status for health endpoints."""
        process = self.process_status()
        observer_status: dict[str, Any] = {}
        status = getattr(self._observer, "status", None)
        if callable(status):
            try:
                observer_status = status()
            except Exception as exc:
                LOGGER.warning("Collector status check failed: %s", exc)
                observer_status = {"ready": False, "error": str(exc)}
        ready = bool(observer_status.get("ready", self._last_tick_completed_at is not None))
        return {
            **process,
            "ready": bool(process["healthy"]) and ready,
            "collector": observer_status,
        }

    def process_status(self) -> dict[str, Any]:
        """Return liveness state without calling external dependencies."""
        running = self._thread is not None and self._thread.is_alive()
        if self._closed:
            lifecycle = "closed"
        elif running:
            lifecycle = "running"
        elif self._desired_state == "running":
            lifecycle = "failed"
        else:
            lifecycle = "stopped"
        healthy = not self._closed and lifecycle != "failed"
        return {
            "running": running,
            "healthy": healthy,
            "lifecycle": lifecycle,
            "desired_state": self._desired_state,
            "last_transition_at": self._last_transition_at.isoformat(),
            "last_transition_error": self._last_transition_error,
            "last_tick_started_at": (
                self._last_tick_started_at.isoformat()
                if self._last_tick_started_at is not None
                else None
            ),
            "last_tick_completed_at": (
                self._last_tick_completed_at.isoformat()
                if self._last_tick_completed_at is not None
                else None
            ),
            "last_tick_error": self._last_tick_error,
        }

    def _collect_loop(self) -> None:
        while not self._stopped.is_set():
            started = time.monotonic()
            try:
                collected = self.collect_once()
                if isinstance(collected, ObservationState):
                    interval_seconds = collected.state_interval_seconds
                else:
                    interval_seconds = collected.sample_interval_seconds
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
        api_bearer_token: str | None = None,
    ) -> None:
        self.runtime = ObservabilityRuntime(observer)
        handler = _handler_for(self.runtime, api_bearer_token)
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


def _handler_for(
    runtime: ObservabilityRuntime,
    api_bearer_token: str | None = None,
) -> type[BaseHTTPRequestHandler]:
    class ObservabilityRequestHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/healthz":
                status = runtime.process_status()
                self._send_json(
                    status,
                    status=HTTPStatus.OK if status["healthy"] else HTTPStatus.SERVICE_UNAVAILABLE,
                )
                return
            if parsed.path.startswith("/api/") and not self._api_authorized():
                self._send_json(
                    {"error": "A valid bearer token is required"},
                    status=HTTPStatus.UNAUTHORIZED,
                    authenticate=True,
                )
                return
            if parsed.path == "/readyz":
                status = runtime.status()
                self._send_json(
                    status,
                    status=HTTPStatus.OK if status["ready"] else HTTPStatus.SERVICE_UNAVAILABLE,
                )
                return
            if parsed.path == "/api/v1/status":
                self._send_json(runtime.status())
                return
            if parsed.path == "/api/v1/snapshot":
                self._serve_snapshot(parse_qs(parsed.query))
                return
            static_file = STATIC_FILES.get(parsed.path)
            if static_file is None:
                self.send_error(HTTPStatus.NOT_FOUND)
                return
            self._serve_static(*static_file)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api/") and not self._api_authorized():
                self._send_json(
                    {"error": "A valid bearer token is required"},
                    status=HTTPStatus.UNAUTHORIZED,
                    authenticate=True,
                )
                return
            actions = {
                "/api/v1/observation/start": runtime.start,
                "/api/v1/observation/stop": runtime.pause,
                "/api/v1/observation/restart": runtime.restart,
            }
            action = actions.get(parsed.path)
            if action is None:
                self._send_json(
                    {"error": "Control endpoint not found"},
                    status=HTTPStatus.NOT_FOUND,
                )
                return
            try:
                action()
            except Exception as exc:
                LOGGER.exception("Observation control action failed")
                self._send_json(
                    {"error": f"Observation control action failed: {exc}"},
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                )
                return
            self._send_json(runtime.status())

        def _serve_snapshot(self, query: Mapping[str, list[str]]) -> None:
            try:
                window_seconds, max_points = _snapshot_parameters(query)
                payload = runtime.snapshot(window_seconds, max_points)
            except ValueError as exc:
                self._send_json(
                    {"error": str(exc)},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            except Exception as exc:
                LOGGER.exception("Snapshot request failed")
                self._send_json(
                    {"error": f"Snapshot is unavailable: {exc}"},
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                )
                return
            self._send_json(payload)

        def _api_authorized(self) -> bool:
            supplied = self.headers.get("Authorization", "")
            return _bearer_token_matches(supplied, api_bearer_token)

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
            authenticate: bool = False,
        ) -> None:
            body = json.dumps(payload, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            if authenticate:
                self.send_header("WWW-Authenticate", 'Bearer realm="tei"')
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


def _snapshot_parameters(
    query: Mapping[str, list[str]],
) -> tuple[int | None, int]:
    """Validate and bound snapshot range and serialization parameters."""
    window_value = _query_integer(query, "window_seconds", 3600)
    max_points = _query_integer(query, "max_points", 180)
    if window_value < 0:
        raise ValueError("window_seconds cannot be negative")
    if window_value > MAX_WINDOW_SECONDS:
        raise ValueError(f"window_seconds must be at most {MAX_WINDOW_SECONDS}")
    if max_points < 2:
        raise ValueError("max_points must be at least two")
    if max_points > MAX_POINTS:
        raise ValueError(f"max_points must be at most {MAX_POINTS}")
    return (None if window_value == 0 else window_value), max_points


def _bearer_token_matches(supplied: str, configured: str | None) -> bool:
    """Return whether an Authorization header satisfies optional API auth."""
    if configured is None:
        return True
    return secrets.compare_digest(supplied, f"Bearer {configured}")
