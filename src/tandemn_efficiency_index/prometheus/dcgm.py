"""Collect DCGM time series from Prometheus and attribute them to workers."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Protocol

from tandemn_efficiency_index.models.cluster_snapshot import WorkloadPod
from tandemn_efficiency_index.models.telemetry import (
    DEFAULT_SAMPLE_INTERVAL_SECONDS,
    ClusterTelemetry,
    MetricSample,
    MetricScope,
    MetricSeries,
    WorkloadTelemetry,
)
from tandemn_efficiency_index.prometheus.client import PrometheusQueryError, PrometheusSeries

LOGGER = logging.getLogger(__name__)

DCGM_METRICS = (
    "DCGM_FI_DEV_GPU_UTIL",
    "DCGM_FI_DEV_MEM_COPY_UTIL",
    "DCGM_FI_DEV_FB_USED",
    "DCGM_FI_DEV_FB_FREE",
    "DCGM_FI_DEV_FB_RESERVED",
    "DCGM_FI_DEV_POWER_USAGE",
    "DCGM_FI_DEV_GPU_TEMP",
    "DCGM_FI_DEV_SM_CLOCK",
    "DCGM_FI_DEV_MEM_CLOCK",
    "DCGM_FI_DEV_XID_ERRORS",
    "DCGM_FI_PROF_GR_ENGINE_ACTIVE",
    "DCGM_FI_PROF_SM_ACTIVE",
    "DCGM_FI_PROF_SM_OCCUPANCY",
    "DCGM_FI_PROF_PIPE_TENSOR_ACTIVE",
    "DCGM_FI_PROF_DRAM_ACTIVE",
    "DCGM_FI_PROF_PCIE_TX_BYTES",
    "DCGM_FI_PROF_PCIE_RX_BYTES",
    "DCGM_FI_PROF_NVLINK_TX_BYTES",
    "DCGM_FI_PROF_NVLINK_RX_BYTES",
)

SCOPE_LABELS = frozenset(
    {
        "__name__",
        "UUID",
        "gpu",
        "GPU_I_ID",
        "container",
        "namespace",
        "pod",
        "pod_uid",
        "exported_pod",
        "exported_namespace",
        "exported_container",
        "Hostname",
        "node",
    }
)


class RangeQueryClient(Protocol):
    """Prometheus operation required by the DCGM collector."""

    def query_range(
        self,
        query: str,
        start: datetime,
        end: datetime,
        step_seconds: int,
    ) -> list[PrometheusSeries]: ...


class DcgmCollector:
    """Collect selected DCGM metrics and attach canonical workload ownership."""

    def __init__(
        self,
        prometheus: RangeQueryClient,
        metric_names: Sequence[str] = DCGM_METRICS,
        step_seconds: int = DEFAULT_SAMPLE_INTERVAL_SECONDS,
    ) -> None:
        self._prometheus = prometheus
        self._metric_names = tuple(metric_names)
        self._step_seconds = step_seconds

    def collect(
        self,
        start: datetime,
        end: datetime,
        pods: Mapping[str, WorkloadPod],
    ) -> ClusterTelemetry:
        """Collect one bounded interval of workload-attributed DCGM telemetry."""
        pods_by_qualified_name = {(pod.namespace, pod.name): pod for pod in pods.values()}
        pods_by_name = _unique_pods_by_name(pods)
        telemetry = ClusterTelemetry()
        local_ranks: dict[str, str] = {}
        for metric_name in self._metric_names:
            try:
                results = self._prometheus.query_range(
                    metric_name,
                    start,
                    end,
                    self._step_seconds,
                )
            except PrometheusQueryError as exc:
                LOGGER.warning("Prometheus query failed for %s: %s", metric_name, exc)
                telemetry.missing_metrics.append(metric_name)
                continue
            if not results:
                telemetry.missing_metrics.append(metric_name)
                continue
            if metric_name == "DCGM_FI_DEV_GPU_UTIL":
                local_ranks = _local_ranks(results, pods_by_qualified_name, pods_by_name)
            for result in results:
                _append_series(
                    telemetry,
                    metric_name,
                    result,
                    pods,
                    pods_by_qualified_name,
                    pods_by_name,
                    local_ranks,
                )
        return telemetry


def _append_series(
    telemetry: ClusterTelemetry,
    metric_name: str,
    result: PrometheusSeries,
    pods_by_uid: Mapping[str, WorkloadPod],
    pods_by_qualified_name: Mapping[tuple[str, str], WorkloadPod],
    pods_by_name: Mapping[str, WorkloadPod],
    local_ranks: Mapping[str, str],
) -> None:
    labels = result.labels
    source_pod_uid = labels.get("pod_uid") or None
    exported_namespace = labels.get("exported_namespace") or None
    exported_pod_name = labels.get("exported_pod") or None
    namespace = exported_namespace or labels.get("namespace") or None
    pod_name = exported_pod_name or labels.get("pod") or None
    pod = pods_by_uid.get(source_pod_uid) if source_pod_uid else None
    attribution_method = "pod_uid" if pod is not None else None
    if pod is None and exported_namespace and exported_pod_name:
        pod = pods_by_qualified_name.get((exported_namespace, exported_pod_name))
        if pod is not None:
            attribution_method = "exported_namespace_pod"
    if pod is None and exported_pod_name:
        pod = pods_by_name.get(exported_pod_name)
        if pod is not None:
            attribution_method = "exported_pod"
    if pod is None and exported_pod_name is None:
        pod = pods_by_qualified_name.get((namespace or "", pod_name or ""))
        if pod is not None:
            attribution_method = "namespace_pod"
    pod_uid: str | None
    if pod is not None:
        pod_uid = pod.uid
    else:
        pod_uid = source_pod_uid
        if source_pod_uid or (namespace and pod_name):
            attribution_method = "unattributed_pod_not_found"
        else:
            attribution_method = "unattributed_missing_pod_identity"

    scope = MetricScope(
        workload_id=pod.workload_id if pod else None,
        pod_uid=pod_uid,
        container_name=labels.get("exported_container") or labels.get("container") or None,
        node_name=pod.node_name if pod else labels.get("node") or labels.get("Hostname"),
        gpu_uuid=labels.get("UUID") or None,
        gpu_index=labels.get("gpu") or None,
        local_rank=local_ranks.get(_gpu_key(labels, pod)),
        gpu_instance_id=labels.get("GPU_I_ID") or None,
        runtime_instance=pod.runtime_instance if pod else None,
        runtime_role=pod.runtime_role if pod else None,
        pod_namespace=namespace,
        pod_name=pod_name,
        attribution_method=attribution_method,
    )
    metric_labels = {name: value for name, value in labels.items() if name not in SCOPE_LABELS}
    series = MetricSeries.create(metric_name, scope, metric_labels)
    series.append(
        MetricSample(timestamp=sample.timestamp, value=sample.value) for sample in result.samples
    )
    if scope.workload_id is None:
        destination = telemetry.unattributed
    else:
        destination = telemetry.jobs.setdefault(
            scope.workload_id,
            WorkloadTelemetry(workload_id=scope.workload_id),
        )
    existing = destination.series.get(series.series_id)
    if existing is None:
        destination.series[series.series_id] = series
    else:
        existing.append(series.samples)


def _unique_pods_by_name(pods: Mapping[str, WorkloadPod]) -> dict[str, WorkloadPod]:
    unique: dict[str, WorkloadPod] = {}
    duplicates: set[str] = set()
    for pod in pods.values():
        if pod.name in unique:
            duplicates.add(pod.name)
        else:
            unique[pod.name] = pod
    for pod_name in duplicates:
        del unique[pod_name]
    return unique


def _local_ranks(
    results: Sequence[PrometheusSeries],
    pods_by_qualified_name: Mapping[tuple[str, str], WorkloadPod],
    pods_by_name: Mapping[str, WorkloadPod],
) -> dict[str, str]:
    owned: dict[str, list[tuple[str, str]]] = {}
    for result in results:
        labels = result.labels
        owner_name = labels.get("exported_pod", "")
        owner_namespace = labels.get("exported_namespace", "")
        pod = pods_by_qualified_name.get((owner_namespace, owner_name))
        if pod is None and owner_name:
            pod = pods_by_name.get(owner_name)
        if pod is None:
            continue
        owned.setdefault(pod.uid, []).append((labels.get("gpu", ""), _gpu_key(labels, pod)))

    ranks: dict[str, str] = {}
    for targets in owned.values():
        targets.sort(key=lambda target: _gpu_index_sort_key(target[0]))
        for local_rank, (_, gpu_key) in enumerate(targets):
            ranks[gpu_key] = str(local_rank)
    return ranks


def _gpu_key(labels: Mapping[str, str], pod: WorkloadPod | None) -> str:
    gpu_uuid = labels.get("UUID")
    if gpu_uuid:
        return gpu_uuid
    pod_uid = pod.uid if pod is not None else "unattributed"
    return f"{pod_uid}:{labels.get('gpu', '')}"


def _gpu_index_sort_key(gpu_index: str) -> tuple[int, int | str]:
    try:
        return 0, int(gpu_index)
    except ValueError:
        return 1, gpu_index
