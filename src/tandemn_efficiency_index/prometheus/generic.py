"""Collect Prometheus time series and attribute them to observed worker Pods."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from tandemn_efficiency_index.models.cluster_snapshot import WorkloadPod
from tandemn_efficiency_index.models.telemetry import (
    ClusterTelemetry,
    MetricSample,
    MetricScope,
    MetricSeries,
    WorkloadTelemetry,
)
from tandemn_efficiency_index.prometheus.client import PrometheusQueryError, PrometheusSeries

LOGGER = logging.getLogger(__name__)

ALL_DCGM_QUERY = '{__name__=~"DCGM_FI_.*"}'
DCGM_REQUIRED_METRICS = (
    "DCGM_FI_DEV_SM_CLOCK",
    "DCGM_FI_DEV_MEM_CLOCK",
    "DCGM_FI_DEV_GPU_TEMP",
    "DCGM_FI_DEV_POWER_USAGE",
    "DCGM_FI_DEV_GPU_UTIL",
    "DCGM_FI_DEV_MEM_COPY_UTIL",
    "DCGM_FI_DEV_XID_ERRORS",
    "DCGM_FI_DEV_FB_FREE",
    "DCGM_FI_DEV_FB_USED",
    "DCGM_FI_PROF_GR_ENGINE_ACTIVE",
    "DCGM_FI_PROF_PIPE_TENSOR_ACTIVE",
    "DCGM_FI_PROF_DRAM_ACTIVE",
    "DCGM_FI_PROF_PCIE_TX_BYTES",
    "DCGM_FI_PROF_PCIE_RX_BYTES",
)
NORMALIZED_INFERENCE_METRICS = (
    "p99_ttft_ms",
    "p99_tpot_ms",
    "request_throughput_rps",
    "input_token_throughput_tps",
    "output_token_throughput_tps",
    "num_requests_running",
    "num_requests_waiting",
    "kv_cache_usage_ratio",
)
RAW_INFERENCE_METRIC_PATTERN = "(vllm:.*|sglang:.*|dynamo_.*|ray_serve_.*)"
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
    """Prometheus operation required by the generic collector."""

    def query_range(
        self,
        query: str,
        start: datetime,
        end: datetime,
        step_seconds: int,
    ) -> list[PrometheusSeries]: ...


@dataclass(frozen=True)
class NormalizedQuery:
    """One backend-independent metric and its PromQL expression."""

    metric_name: str
    query: str


class PrometheusMetricsCollector:
    """Collect all DCGM and observed-worker series for a requested range."""

    def __init__(
        self,
        prometheus: RangeQueryClient,
        step_seconds: int,
    ) -> None:
        self._prometheus = prometheus
        self._step_seconds = step_seconds

    def collect(
        self,
        start: datetime,
        end: datetime,
        pods: Mapping[str, WorkloadPod],
    ) -> ClusterTelemetry:
        """Collect all DCGM and observed-worker series for the requested range."""
        try:
            results = self._prometheus.query_range(
                _job_metrics_query(pods),
                start,
                end,
                self._step_seconds,
            )
        except PrometheusQueryError as exc:
            LOGGER.warning("Prometheus query failed for all metrics: %s", exc)
            return ClusterTelemetry(missing_metrics=["*"])

        pods_by_qualified_name = {(pod.namespace, pod.name): pod for pod in pods.values()}
        pods_by_name = _unique_pods_by_name(pods)
        dcgm_util_results = [
            result for result in results if result.labels.get("__name__") == "DCGM_FI_DEV_GPU_UTIL"
        ]
        local_ranks = _local_ranks(dcgm_util_results, pods_by_qualified_name, pods_by_name)
        telemetry = ClusterTelemetry()
        observed_metric_names: set[str] = set()
        for result in results:
            metric_name = result.labels.get("__name__")
            if not metric_name:
                continue
            observed_metric_names.add(metric_name)
            _append_series(
                telemetry,
                metric_name,
                result,
                pods,
                pods_by_qualified_name,
                pods_by_name,
                local_ranks,
            )
        telemetry.missing_metrics.extend(
            metric_name
            for metric_name in DCGM_REQUIRED_METRICS
            if metric_name not in observed_metric_names
        )
        if pods:
            for normalized_query in _normalized_queries(pods):
                try:
                    normalized_results = self._prometheus.query_range(
                        normalized_query.query,
                        start,
                        end,
                        self._step_seconds,
                    )
                except PrometheusQueryError as exc:
                    LOGGER.warning(
                        "Prometheus normalization query failed for %s: %s",
                        normalized_query.metric_name,
                        exc,
                    )
                    telemetry.missing_metrics.append(normalized_query.metric_name)
                    continue
                if not normalized_results:
                    telemetry.missing_metrics.append(normalized_query.metric_name)
                    continue
                for result in normalized_results:
                    normalized_result = PrometheusSeries(
                        labels={**result.labels, "__name__": normalized_query.metric_name},
                        samples=result.samples,
                    )
                    _append_series(
                        telemetry,
                        normalized_query.metric_name,
                        normalized_result,
                        pods,
                        pods_by_qualified_name,
                        pods_by_name,
                        local_ranks,
                    )
        telemetry.missing_metrics = sorted(set(telemetry.missing_metrics))
        return telemetry

    def check_ready(self, at: datetime) -> None:
        """Raise when Prometheus cannot execute a lightweight range query."""
        self._prometheus.query_range(
            "up",
            at,
            at,
            self._step_seconds,
        )


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

    pod_uid = pod.uid if pod is not None else source_pod_uid
    if pod is None:
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
        runtime_job_key=pod.runtime_job_key if pod else None,
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
    destination = (
        telemetry.jobs.setdefault(
            scope.workload_id,
            WorkloadTelemetry(workload_id=scope.workload_id),
        )
        if scope.workload_id is not None
        else telemetry.unattributed
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
    results: list[PrometheusSeries],
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


def _job_metrics_query(pods: Mapping[str, WorkloadPod]) -> str:
    if not pods:
        return ALL_DCGM_QUERY
    namespaces = "|".join(sorted({_escape_regex_literal(pod.namespace) for pod in pods.values()}))
    pod_names = "|".join(sorted({_escape_regex_literal(pod.name) for pod in pods.values()}))
    worker_query = (
        f'{{__name__=~"{RAW_INFERENCE_METRIC_PATTERN}",'
        f'namespace=~"^(?:{_escape_label_value(namespaces)})$",'
        f'pod=~"^(?:{_escape_label_value(pod_names)})$"}}'
    )
    return f"({ALL_DCGM_QUERY}) or ({worker_query})"


def _normalized_queries(pods: Mapping[str, WorkloadPod]) -> tuple[NormalizedQuery, ...]:
    namespaces = "|".join(sorted({_escape_regex_literal(pod.namespace) for pod in pods.values()}))
    pod_names = "|".join(sorted({_escape_regex_literal(pod.name) for pod in pods.values()}))
    scope = (
        f'namespace=~"^(?:{_escape_label_value(namespaces)})$",'
        f'pod=~"^(?:{_escape_label_value(pod_names)})$"'
    )
    group = "namespace, pod, pod_uid, container"

    def selector(metric_pattern: str, extra_matchers: str = "") -> str:
        return f'{{__name__=~"{metric_pattern}",{scope}{extra_matchers}}}'

    def rate_sum(metric_pattern: str) -> str:
        return f"sum by ({group}) (rate({selector(metric_pattern)}[5m]))"

    def gauge_sum(metric_pattern: str) -> str:
        return f"sum by ({group}) ({selector(metric_pattern)})"

    def p99(metric_pattern: str) -> str:
        buckets = f"sum by (le, {group}) (rate({selector(metric_pattern)}[5m]))"
        return f"histogram_quantile(0.99, {buckets}) * 1000"

    return (
        NormalizedQuery(
            "p99_ttft_ms",
            p99(
                "(vllm:time_to_first_token_seconds_bucket|"
                "sglang:time_to_first_token_seconds_bucket|"
                "dynamo_frontend_time_to_first_token_seconds_bucket)"
            ),
        ),
        NormalizedQuery(
            "p99_tpot_ms",
            p99(
                "(vllm:inter_token_latency_seconds_bucket|"
                "vllm:time_per_output_token_seconds_bucket|"
                "sglang:time_per_output_token_seconds_bucket|"
                "dynamo_frontend_inter_token_latency_seconds_bucket)"
            ),
        ),
        NormalizedQuery(
            "request_throughput_rps",
            rate_sum(
                "(vllm:request_success_total|sglang:e2e_request_latency_seconds_count|"
                "dynamo_frontend_requests_total|ray_serve_num_http_requests_total|"
                "ray_serve_num_router_requests_total)"
            ),
        ),
        NormalizedQuery(
            "input_token_throughput_tps",
            rate_sum(
                "(vllm:prompt_tokens_total|sglang:prompt_tokens_total|"
                "dynamo_frontend_input_sequence_tokens_sum)"
            ),
        ),
        NormalizedQuery(
            "output_token_throughput_tps",
            rate_sum(
                "(vllm:generation_tokens_total|sglang:generation_tokens_total|"
                "dynamo_frontend_output_tokens_total)"
            ),
        ),
        NormalizedQuery(
            "num_requests_running",
            gauge_sum(
                "(vllm:num_requests_running|sglang:num_running_reqs|"
                "dynamo_component_inflight_requests|dynamo_frontend_active_requests|"
                "dynamo_frontend_inflight_requests|ray_serve_num_ongoing_http_requests|"
                "ray_serve_num_ongoing_requests_at_replicas)"
            ),
        ),
        NormalizedQuery(
            "num_requests_waiting",
            gauge_sum(
                "(vllm:num_requests_waiting|sglang:num_queue_reqs|"
                "dynamo_frontend_queued_requests|ray_serve_deployment_queued_queries|"
                "ray_serve_request_router_queue_len)"
            )
            + " or "
            + f"sum by ({group}) ("
            + selector(
                "dynamo_frontend_stage_requests",
                ',stage=~"preprocess|route|dispatch"',
            )
            + ")",
        ),
        NormalizedQuery(
            "kv_cache_usage_ratio",
            f"max by ({group}) (" + selector("(vllm:kv_cache_usage_perc|sglang:token_usage)") + ")",
        ),
    )


def _escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _escape_regex_literal(value: str) -> str:
    special = frozenset(r"\.^$|?*+()[]{}")
    return "".join(f"\\{character}" if character in special else character for character in value)
