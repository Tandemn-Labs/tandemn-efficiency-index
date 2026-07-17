"""Collect worker-scoped vLLM time series from Prometheus."""

from __future__ import annotations

import logging
from collections.abc import Mapping
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
from tandemn_efficiency_index.models.workload import Workload
from tandemn_efficiency_index.prometheus.client import PrometheusQueryError, PrometheusSeries

LOGGER = logging.getLogger(__name__)

VLLM_QUERIES = {
    "p99_ttft_ms": (
        "histogram_quantile(0.99, sum(rate("
        "vllm:time_to_first_token_seconds_bucket{{{worker}}}[5m])) by (le)) * 1000"
    ),
    "p99_tpot_ms": (
        "histogram_quantile(0.99, sum(rate("
        "vllm:request_time_per_output_token_seconds_bucket{{{worker}}}[5m])) by (le)) * 1000"
    ),
    "throughput_token_per_sec": "sum(rate(vllm:generation_tokens_total{{{worker}}}[1m]))",
    "live_batch_size": "sum(vllm:num_requests_running{{{worker}}})",
    "depth_req_q": "sum(vllm:num_requests_waiting{{{worker}}})",
    "kv_cache_util": "max(vllm:kv_cache_usage_perc{{{worker}}})",
    "kvcache_hit_rate": (
        "sum(rate(vllm:prefix_cache_hits_total{{{worker}}}[5m])) / "
        "sum(rate(vllm:prefix_cache_queries_total{{{worker}}}[5m]))"
    ),
    "input_length_observed": (
        "sum(rate(vllm:request_prompt_tokens_sum{{{worker}}}[5m])) / "
        "sum(rate(vllm:request_prompt_tokens_count{{{worker}}}[5m]))"
    ),
    "output_length_observed": (
        "sum(rate(vllm:request_generation_tokens_sum{{{worker}}}[5m])) / "
        "sum(rate(vllm:request_generation_tokens_count{{{worker}}}[5m]))"
    ),
    "prefill_iteration_counts_per_second": (
        "sum(rate(vllm:request_prefill_time_seconds_count{{{worker}}}[1m]))"
    ),
    "decode_itr_counts_per_second": (
        "sum(rate(vllm:request_decode_time_seconds_count{{{worker}}}[1m]))"
    ),
    "kv_pressure_score": (
        "max(vllm:kv_cache_usage_perc{{{worker}}}) + "
        "sum(vllm:num_requests_waiting{{{worker}}}) + "
        "sum(rate(vllm:num_preemptions_total{{{worker}}}[5m]))"
    ),
    "pd_inbalance": (
        "sum(rate(vllm:request_prefill_time_seconds_sum{{{worker}}}[5m])) / "
        "sum(rate(vllm:request_decode_time_seconds_sum{{{worker}}}[5m]))"
    ),
}

WORKER_SCOPE_LABELS = frozenset(
    {
        "__name__",
        "container",
        "instance",
        "job",
        "namespace",
        "pod",
    }
)


class RangeQueryClient(Protocol):
    """Prometheus operation required by the vLLM collector."""

    def query_range(
        self,
        query: str,
        start: datetime,
        end: datetime,
        step_seconds: int,
    ) -> list[PrometheusSeries]: ...


class VllmCollector:
    """Collect normalized inference metrics once per vLLM worker Pod."""

    def __init__(
        self,
        prometheus: RangeQueryClient,
        queries: Mapping[str, str] = VLLM_QUERIES,
        step_seconds: int = DEFAULT_SAMPLE_INTERVAL_SECONDS,
    ) -> None:
        self._prometheus = prometheus
        self._queries = dict(queries)
        self._step_seconds = step_seconds

    def collect(
        self,
        start: datetime,
        end: datetime,
        pods: Mapping[str, WorkloadPod],
        workloads: Mapping[str, Workload],
    ) -> ClusterTelemetry:
        """Collect each vLLM query per worker and retain job ownership."""
        vllm_workloads = {
            workload_id for workload_id, workload in workloads.items() if workload.backend == "vllm"
        }
        workers = [pod for pod in pods.values() if pod.workload_id in vllm_workloads]
        telemetry = ClusterTelemetry()
        if not workers:
            return telemetry
        observed_metrics: set[str] = set()

        for worker in workers:
            selector = f'pod="{_escape_label_value(worker.name)}"'
            for metric_name, query_template in self._queries.items():
                query = query_template.format(worker=selector)
                try:
                    results = self._prometheus.query_range(
                        query,
                        start,
                        end,
                        self._step_seconds,
                    )
                except PrometheusQueryError as exc:
                    LOGGER.warning(
                        "Prometheus vLLM query failed for %s/%s: %s",
                        worker.namespace,
                        worker.name,
                        exc,
                    )
                    continue
                if results:
                    observed_metrics.add(metric_name)
                for result in results:
                    _append_worker_series(telemetry, metric_name, result, worker)

        telemetry.missing_metrics = sorted(set(self._queries) - observed_metrics)
        return telemetry


def _append_worker_series(
    telemetry: ClusterTelemetry,
    metric_name: str,
    result: PrometheusSeries,
    worker: WorkloadPod,
) -> None:
    scope = MetricScope(
        workload_id=worker.workload_id,
        pod_uid=worker.uid,
        container_name=result.labels.get("container") or None,
        node_name=worker.node_name,
        runtime_instance=worker.runtime_instance,
        runtime_role=worker.runtime_role,
        pod_namespace=worker.namespace,
        pod_name=worker.name,
        attribution_method="worker_pod_query",
    )
    labels = {
        name: value for name, value in result.labels.items() if name not in WORKER_SCOPE_LABELS
    }
    series = MetricSeries.create(metric_name, scope, labels)
    series.append(
        MetricSample(timestamp=sample.timestamp, value=sample.value) for sample in result.samples
    )
    destination = telemetry.jobs.setdefault(
        worker.workload_id,
        WorkloadTelemetry(workload_id=worker.workload_id),
    )
    existing = destination.series.get(series.series_id)
    if existing is None:
        destination.series[series.series_id] = series
    else:
        existing.append(series.samples)


def _escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')
