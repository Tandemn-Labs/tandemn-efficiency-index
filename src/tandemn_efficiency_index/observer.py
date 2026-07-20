"""Track Kubernetes workload state and query Prometheus on demand."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from tandemn_efficiency_index.models.cluster_snapshot import (
    ClusterRecord,
    JobRecord,
    WorkloadPod,
)
from tandemn_efficiency_index.models.observation import (
    Observation,
    ObservationState,
    ObservedWorkload,
    RuntimeJobKey,
    WorkloadRevision,
)
from tandemn_efficiency_index.models.report import ObservabilityReport
from tandemn_efficiency_index.models.telemetry import (
    DEFAULT_SAMPLE_INTERVAL_SECONDS,
    ClusterTelemetry,
)
from tandemn_efficiency_index.models.workload import Workload
from tandemn_efficiency_index.pod_attribution import WorkloadPodCollector
from tandemn_efficiency_index.prometheus.client import PrometheusClient
from tandemn_efficiency_index.prometheus.generic import PrometheusMetricsCollector

LOGGER = logging.getLogger(__name__)


class WorkloadDiscoverer(Protocol):
    """Dynamic workload discovery operation required by the observer."""

    def discover(self) -> dict[str, Workload]: ...

    def available_resource_map(self) -> dict[str, dict[str, Any]]: ...


class PodCollector(Protocol):
    """Worker Pod collection operation required by the observer."""

    def collect(
        self,
        workloads: Mapping[str, Workload],
        observed_at: datetime,
        known_pods: Mapping[str, WorkloadPod] | None = None,
    ) -> dict[str, WorkloadPod]: ...


class TelemetryCollector(Protocol):
    """Prometheus collection operation required by the observer."""

    def collect(
        self,
        start: datetime,
        end: datetime,
        pods: Mapping[str, WorkloadPod],
    ) -> ClusterTelemetry: ...

    def check_ready(self, at: datetime) -> None: ...


class ObservationStore(Protocol):
    """Durable storage operations required by the observer."""

    def load_state(
        self,
        state_interval_seconds: int,
        now: datetime | None = None,
    ) -> ObservationState: ...

    def ensure_observation(self, now: datetime | None = None) -> Observation: ...

    def save_state(
        self,
        state: ObservationState,
        started_at: datetime,
        completed_at: datetime,
        status: str,
        error: str | None = None,
    ) -> None: ...

    def record_failure(
        self,
        observation_id: str,
        started_at: datetime,
        completed_at: datetime,
        error: str,
    ) -> None: ...

    def status(self) -> dict[str, Any]: ...

    def load_revisions(
        self,
        observation_id: str,
        start: datetime,
        end: datetime,
    ) -> dict[str, list[WorkloadRevision]]: ...

    def load_runtime_job_keys(
        self,
        observation_id: str,
        start: datetime,
        end: datetime,
    ) -> list[RuntimeJobKey]: ...

    def close(self) -> None: ...


class ClusterObserver:
    """Persist Kubernetes state and join Prometheus only for requested windows."""

    def __init__(
        self,
        workloads: Mapping[str, Workload],
        pod_collector: PodCollector,
        prometheus_collector: TelemetryCollector,
        retention: timedelta = timedelta(hours=24),
        sample_interval_seconds: int = DEFAULT_SAMPLE_INTERVAL_SECONDS,
        started_at: datetime | None = None,
        workload_discovery: WorkloadDiscoverer | None = None,
        discovery_interval: timedelta = timedelta(seconds=10),
        store: ObservationStore | None = None,
    ) -> None:
        initial_time = started_at or datetime.now(UTC)
        self._workloads = dict(workloads)
        self._pod_collector = pod_collector
        self._prometheus_collector = prometheus_collector
        self._sample_interval_seconds = sample_interval_seconds
        self._workload_discovery = workload_discovery
        self._discovery_interval = discovery_interval
        self._store = store
        self._last_discovered_at: datetime | None = None
        self._last_successful_collection_at: datetime | None = None
        self._last_collection_error: str | None = None
        self._last_discovery_error: str | None = None
        self._last_prometheus_check_at: datetime | None = None
        self._state = ObservationState(
            observation=Observation(
                observation_id="in-memory",
                started_at=initial_time,
                ends_at=initial_time + retention,
            ),
            updated_at=initial_time,
            state_interval_seconds=sample_interval_seconds,
            jobs={
                workload_id: ObservedWorkload(workload=workload)
                for workload_id, workload in self._workloads.items()
            },
        )
        if store is not None:
            restored = store.load_state(sample_interval_seconds)
            self._state = restored
            restored_workloads = {
                workload_id: job.workload
                for workload_id, job in restored.jobs.items()
                if job.active
            }
            self._workloads.update(restored_workloads)

    @classmethod
    def from_in_cluster(
        cls,
        prometheus_url: str,
        workloads: Mapping[str, Workload] | None = None,
        workload_discovery: WorkloadDiscoverer | None = None,
        discovery_interval: timedelta = timedelta(seconds=10),
        retention: timedelta = timedelta(hours=24),
        sample_interval_seconds: int = DEFAULT_SAMPLE_INTERVAL_SECONDS,
        store: ObservationStore | None = None,
        prometheus_timeout_seconds: float = 5.0,
        prometheus_bearer_token: str | None = None,
        prometheus_ca_file: str | None = None,
        prometheus_insecure_skip_verify: bool = False,
        prometheus_step_seconds: int = DEFAULT_SAMPLE_INTERVAL_SECONDS,
    ) -> ClusterObserver:
        """Create an observer for fixed workloads and an existing Prometheus service."""
        prometheus = PrometheusClient(
            prometheus_url,
            timeout_seconds=prometheus_timeout_seconds,
            bearer_token=prometheus_bearer_token,
            ca_file=prometheus_ca_file,
            insecure_skip_verify=prometheus_insecure_skip_verify,
        )
        return cls(
            workloads=workloads or {},
            pod_collector=WorkloadPodCollector.from_in_cluster(),
            prometheus_collector=PrometheusMetricsCollector(
                prometheus,
                step_seconds=prometheus_step_seconds,
            ),
            workload_discovery=workload_discovery,
            discovery_interval=discovery_interval,
            retention=retention,
            sample_interval_seconds=sample_interval_seconds,
            store=store,
        )

    def collect_tick(self, collected_at: datetime | None = None) -> ObservationState:
        """Reconcile Kubernetes state and verify Prometheus is reachable."""
        started_at = datetime.now(UTC)
        try:
            state = self._collect_state_tick(collected_at)
            self._prometheus_collector.check_ready(state.updated_at)
            self._last_prometheus_check_at = state.updated_at
            completed_at = datetime.now(UTC)
            self._last_successful_collection_at = completed_at
            self._last_collection_error = None
            if self._store is not None:
                status = "partial" if state.missing_state or self._last_discovery_error else "ok"
                self._store.save_state(
                    state,
                    started_at,
                    completed_at,
                    status,
                    self._last_discovery_error,
                )
            return state
        except Exception as exc:
            completed_at = datetime.now(UTC)
            self._last_collection_error = str(exc)
            observation_id = self._state.observation_id
            if self._store is not None:
                self._store.record_failure(
                    observation_id,
                    started_at,
                    completed_at,
                    str(exc),
                )
            raise

    def _collect_state_tick(self, collected_at: datetime | None = None) -> ObservationState:
        """Collect one observation checkpoint from the Kubernetes API."""
        end = collected_at or datetime.now(UTC)
        self._rotate_observation(end)

        self._refresh_workloads(end)
        known_pods = self._known_pods()
        observed_pods = self._pod_collector.collect(
            self._workloads,
            end,
            known_pods,
        )
        self._record_workers(observed_pods)
        self._state.updated_at = end
        self._state.missing_state = []
        return self._state

    def live_record(
        self,
        window_seconds: int | None,
        queried_at: datetime | None = None,
    ) -> ObservabilityReport:
        """Query Prometheus for one live window and join it to observed Pod ownership."""
        end = queried_at or datetime.now(UTC)
        end = min(end, self._state.ends_at)
        if window_seconds is None:
            start = self._state.started_at
        else:
            start = max(
                self._state.started_at,
                end - timedelta(seconds=window_seconds),
            )
        live = self._report_from_state(start, end)
        if self._store is not None:
            live.workload_revisions = self._store.load_revisions(
                self._state.observation_id,
                start,
                end,
            )
            live.runtime_job_keys = self._store.load_runtime_job_keys(
                self._state.observation_id,
                start,
                end,
            )
        relevant_pods = {
            pod.uid: pod
            for job in live.jobs.values()
            for pod in job.workers.values()
            if pod.first_seen_at <= end and pod.last_seen_at >= start
        }
        collected = self._prometheus_collector.collect(start, end, relevant_pods)
        self._merge_telemetry(live, collected)
        live.missing_metrics = sorted(collected.missing_metrics)
        return live

    def status(self) -> dict[str, Any]:
        """Return collector, discovery, and durable-storage health."""
        storage: dict[str, Any] | None = None
        storage_error: str | None = None
        if self._store is not None:
            try:
                storage = self._store.status()
            except Exception as exc:
                storage_error = str(exc)
                LOGGER.warning("Durable storage health check failed: %s", exc)
        discovery_ready = self._workload_discovery is None or self._last_discovered_at is not None
        storage_ready = self._store is None or (
            storage_error is None and storage is not None and bool(storage.get("writable"))
        )
        prometheus_ready = self._last_prometheus_check_at is not None
        return {
            "ready": self._last_successful_collection_at is not None
            and self._last_collection_error is None
            and discovery_ready
            and storage_ready
            and prometheus_ready,
            "last_successful_collection_at": (
                self._last_successful_collection_at.isoformat()
                if self._last_successful_collection_at is not None
                else None
            ),
            "last_collection_error": self._last_collection_error,
            "last_discovery_error": self._last_discovery_error,
            "last_prometheus_check_at": (
                self._last_prometheus_check_at.isoformat()
                if self._last_prometheus_check_at is not None
                else None
            ),
            "storage": storage,
            "storage_error": storage_error,
        }

    def available_resource_map(self) -> dict[str, dict[str, Any]]:
        """Return supported Kubernetes resources visible to workload discovery."""
        if self._workload_discovery is None:
            return {}
        return self._workload_discovery.available_resource_map()

    def close(self) -> None:
        """Close durable resources after collection has stopped."""
        if self._store is not None:
            self._store.close()

    def reconcile_workloads(
        self,
        workloads: Mapping[str, Workload],
        discovered_at: datetime | None = None,
    ) -> None:
        """Apply discovered workloads while preserving retained job history."""
        observed_at = discovered_at or datetime.now(UTC)
        incoming = dict(workloads)
        for workload_id, existing_job in self._state.jobs.items():
            if workload_id not in incoming and existing_job.active:
                existing_job.active = False
                existing_job.removed_at = observed_at

        for workload_id, workload in incoming.items():
            matched_job = self._state.jobs.get(workload_id)
            if matched_job is None:
                self._state.jobs[workload_id] = ObservedWorkload(workload=workload)
                continue
            matched_job.workload = workload
            matched_job.active = True
            matched_job.removed_at = None

        self._workloads = incoming

    @property
    def record(self) -> ClusterRecord:
        """Return a report shell without querying Prometheus."""
        return self._report_from_state(self._state.started_at, self._state.updated_at)

    @property
    def state(self) -> ObservationState:
        """Return the persisted Kubernetes observation state."""
        return self._state

    def _known_pods(self, active_only: bool = False) -> dict[str, WorkloadPod]:
        return {
            pod_uid: pod
            for job in self._state.jobs.values()
            if job.active or not active_only
            for pod_uid, pod in job.workers.items()
        }

    def _record_workers(self, pods: Mapping[str, WorkloadPod]) -> None:
        for pod_uid, pod in pods.items():
            job = self._state.jobs.get(pod.workload_id)
            if job is not None:
                job.workers[pod_uid] = pod

    def _rotate_observation(self, observed_at: datetime) -> None:
        if self._store is None:
            return
        observation = self._store.ensure_observation(observed_at)
        if observation.observation_id == self._state.observation_id:
            return
        self._state = ObservationState(
            observation=observation,
            updated_at=observed_at,
            state_interval_seconds=self._sample_interval_seconds,
            jobs={
                workload_id: ObservedWorkload(workload=workload)
                for workload_id, workload in self._workloads.items()
            },
        )
        self._last_discovered_at = None

    def _refresh_workloads(self, observed_at: datetime) -> None:
        if self._workload_discovery is None:
            return
        if (
            self._last_discovered_at is not None
            and observed_at - self._last_discovered_at < self._discovery_interval
        ):
            return
        try:
            workloads = self._workload_discovery.discover()
        except Exception as exc:
            self._last_discovery_error = str(exc)
            LOGGER.exception("Kubernetes workload discovery refresh failed")
            return
        self.reconcile_workloads(workloads, observed_at)
        self._last_discovered_at = observed_at
        self._last_discovery_error = None

    @staticmethod
    def _merge_telemetry(record: ClusterRecord, collected: ClusterTelemetry) -> None:
        for workload_id, telemetry in collected.jobs.items():
            job = record.jobs.get(workload_id)
            if job is not None:
                job.telemetry.merge(telemetry)
        record.unattributed_telemetry.merge(collected.unattributed)

    def _report_from_state(self, start: datetime, end: datetime) -> ObservabilityReport:
        jobs: dict[str, JobRecord] = {}
        for workload_id, observed in self._state.jobs.items():
            job = JobRecord(
                workload=deepcopy(observed.workload),
                active=observed.active,
                removed_at=observed.removed_at,
            )
            job.workers = deepcopy(observed.workers)
            jobs[workload_id] = job
        return ObservabilityReport(
            started_at=self._state.started_at,
            updated_at=end,
            window_start=start,
            observation_id=self._state.observation_id,
            observation_ends_at=self._state.ends_at,
            sample_interval_seconds=self._sample_interval_seconds,
            jobs=jobs,
            runtime_job_keys=deepcopy(self._state.runtime_job_keys),
            workload_revisions=deepcopy(self._state.workload_revisions),
            missing_metrics=[],
        )
