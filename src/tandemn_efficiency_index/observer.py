"""Append Prometheus telemetry to a long-lived job-scoped cluster record."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

from tandemn_efficiency_index.models.cluster_snapshot import (
    ClusterRecord,
    JobRecord,
    WorkloadPod,
)
from tandemn_efficiency_index.models.telemetry import (
    DEFAULT_SAMPLE_INTERVAL_SECONDS,
    WorkloadTelemetry,
)
from tandemn_efficiency_index.models.workload import Workload
from tandemn_efficiency_index.pod_attribution import WorkloadPodCollector
from tandemn_efficiency_index.prometheus.client import PrometheusClient
from tandemn_efficiency_index.prometheus.dcgm import DcgmCollector
from tandemn_efficiency_index.prometheus.vllm import VllmCollector


class ClusterObserver:
    """Append new samples to fixed workload records at ten-second intervals."""

    def __init__(
        self,
        workloads: Mapping[str, Workload],
        pod_collector: WorkloadPodCollector,
        dcgm_collector: DcgmCollector,
        retention: timedelta = timedelta(hours=24),
        sample_interval_seconds: int = DEFAULT_SAMPLE_INTERVAL_SECONDS,
        started_at: datetime | None = None,
        vllm_collector: VllmCollector | None = None,
    ) -> None:
        initial_time = started_at or datetime.now(UTC)
        self._workloads = dict(workloads)
        self._pod_collector = pod_collector
        self._dcgm_collector = dcgm_collector
        self._vllm_collector = vllm_collector
        self._retention = retention
        self._sample_interval_seconds = sample_interval_seconds
        self._last_collected_at: datetime | None = None
        self._record = ClusterRecord(
            started_at=initial_time,
            updated_at=initial_time,
            window_start=initial_time - retention,
            sample_interval_seconds=sample_interval_seconds,
            jobs={
                workload_id: JobRecord(workload=workload)
                for workload_id, workload in self._workloads.items()
            },
        )

    @classmethod
    def from_in_cluster(
        cls,
        prometheus_url: str,
        workloads: Mapping[str, Workload],
    ) -> ClusterObserver:
        """Create an observer for fixed workloads and an existing Prometheus service."""
        prometheus = PrometheusClient(prometheus_url)
        return cls(
            workloads=workloads,
            pod_collector=WorkloadPodCollector.from_in_cluster(),
            dcgm_collector=DcgmCollector(prometheus),
            vllm_collector=VllmCollector(prometheus),
        )

    def collect_tick(self, collected_at: datetime | None = None) -> ClusterRecord:
        """Append one interval of new samples to the existing job records."""
        end = collected_at or datetime.now(UTC)
        interval = timedelta(seconds=self._sample_interval_seconds)
        start = (self._last_collected_at or end) - interval
        window_start = end - self._retention

        known_pods = self._known_pods()
        observed_pods = self._pod_collector.collect(
            self._workloads,
            end,
            known_pods,
        )
        self._record_workers(observed_pods)
        attribution_pods = self._known_pods()
        collected = self._dcgm_collector.collect(start, end, attribution_pods)
        self._append_telemetry(collected.jobs)
        self._record.unattributed_telemetry.merge(collected.unattributed)
        missing_metrics = set(collected.missing_metrics)
        if self._vllm_collector is not None:
            inference = self._vllm_collector.collect(
                start,
                end,
                observed_pods,
                self._workloads,
            )
            self._append_telemetry(inference.jobs)
            missing_metrics.update(inference.missing_metrics)
        self._prune(window_start)

        self._record.updated_at = end
        self._record.window_start = window_start
        self._record.missing_metrics = sorted(missing_metrics)
        self._last_collected_at = end
        return self._record

    @property
    def record(self) -> ClusterRecord:
        """Return the current rolling cluster record without collecting a new tick."""
        return self._record

    def _known_pods(self) -> dict[str, WorkloadPod]:
        return {
            pod_uid: pod
            for job in self._record.jobs.values()
            for pod_uid, pod in job.workers.items()
        }

    def _record_workers(self, pods: Mapping[str, WorkloadPod]) -> None:
        for pod_uid, pod in pods.items():
            job = self._record.jobs.get(pod.workload_id)
            if job is not None:
                job.workers[pod_uid] = pod

    def _append_telemetry(self, collected: Mapping[str, WorkloadTelemetry]) -> None:
        for workload_id, telemetry in collected.items():
            job = self._record.jobs.get(workload_id)
            if job is not None:
                job.telemetry.merge(telemetry)

    def _prune(self, window_start: datetime) -> None:
        for job in self._record.jobs.values():
            job.telemetry.prune(window_start)
        self._record.unattributed_telemetry.prune(window_start)
