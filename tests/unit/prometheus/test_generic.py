from datetime import UTC, datetime

from tandemn_efficiency_index.models.cluster_snapshot import WorkloadPod
from tandemn_efficiency_index.prometheus.client import PrometheusSample, PrometheusSeries
from tandemn_efficiency_index.prometheus.generic import (
    ALL_DCGM_QUERY,
    PrometheusMetricsCollector,
)


class FakePrometheusClient:
    def __init__(
        self,
        series: list[PrometheusSeries],
        normalized_series: list[PrometheusSeries] | None = None,
    ) -> None:
        self.series = series
        self.normalized_series = normalized_series or []
        self.queries: list[str] = []

    def query_range(
        self,
        query: str,
        start: datetime,
        end: datetime,
        step_seconds: int,
    ) -> list[PrometheusSeries]:
        self.queries.append(query)
        if ALL_DCGM_QUERY in query:
            return self.series
        return self.normalized_series


def test_collects_raw_series_and_runs_normalized_queries() -> None:
    timestamp = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    worker = WorkloadPod(
        workload_id="ray:inference/qwen-production",
        namespace="inference",
        name="qwen-worker-abc12",
        uid="pod-uid",
        node_name="gpu-node-1",
        container_names=["ray-worker"],
        runtime_instance="qwen-raycluster-active",
        runtime_state="active",
        runtime_role="gpu-workers",
        first_seen_at=timestamp,
        last_seen_at=timestamp,
        runtime_job_key="qwen-raycluster-active",
    )
    prometheus = FakePrometheusClient(
        [
            PrometheusSeries(
                labels={
                    "__name__": "DCGM_FI_DEV_GPU_UTIL",
                    "exported_namespace": "inference",
                    "exported_pod": worker.name,
                    "UUID": "GPU-abc",
                    "gpu": "0",
                    "job": "dcgm-exporter",
                },
                samples=[PrometheusSample(timestamp, 82.0)],
            ),
            PrometheusSeries(
                labels={
                    "__name__": "sglang:request_latency_seconds",
                    "namespace": worker.namespace,
                    "pod": worker.name,
                    "job": "sglang",
                },
                samples=[PrometheusSample(timestamp, 0.12)],
            ),
        ]
    )

    telemetry = PrometheusMetricsCollector(prometheus, step_seconds=10).collect(
        timestamp,
        timestamp,
        {worker.uid: worker},
    )

    assert len(prometheus.queries) == 9
    assert ALL_DCGM_QUERY in prometheus.queries[0]
    assert 'namespace=~"^(?:inference)$"' in prometheus.queries[0]
    assert 'pod=~"^(?:qwen-worker-abc12)$"' in prometheus.queries[0]
    workload_series = telemetry.jobs[worker.workload_id].series
    assert {series.metric_name for series in workload_series.values()} == {
        "DCGM_FI_DEV_GPU_UTIL",
        "sglang:request_latency_seconds",
    }
    dcgm = next(
        series for series in workload_series.values() if series.metric_name.startswith("DCGM")
    )
    assert dcgm.scope.gpu_uuid == "GPU-abc"
    assert dcgm.scope.local_rank == "0"
    assert dcgm.scope.attribution_method == "exported_namespace_pod"
    assert dcgm.scope.runtime_job_key == "qwen-raycluster-active"
    assert "p99_ttft_ms" in telemetry.missing_metrics


def test_retains_series_without_pod_identity_as_unattributed() -> None:
    timestamp = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    prometheus = FakePrometheusClient(
        [
            PrometheusSeries(
                labels={"__name__": "DCGM_FI_DEV_POWER_USAGE", "job": "dcgm-exporter"},
                samples=[PrometheusSample(timestamp, 1.0)],
            )
        ]
    )

    telemetry = PrometheusMetricsCollector(prometheus, step_seconds=10).collect(
        timestamp,
        timestamp,
        {},
    )

    series = next(iter(telemetry.unattributed.series.values()))
    assert prometheus.queries == [ALL_DCGM_QUERY]
    assert series.metric_name == "DCGM_FI_DEV_POWER_USAGE"
    assert series.scope.attribution_method == "unattributed_missing_pod_identity"


def test_emits_normalized_backend_metrics_with_workload_attribution() -> None:
    timestamp = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    worker = WorkloadPod(
        workload_id="dynamo:inference/qwen",
        namespace="inference",
        name="qwen-worker",
        uid="pod-uid",
        node_name="gpu-node-1",
        container_names=["main"],
        runtime_instance="qwen",
        runtime_state="active",
        runtime_role="worker",
        first_seen_at=timestamp,
        last_seen_at=timestamp,
    )
    normalized = PrometheusSeries(
        labels={"namespace": worker.namespace, "pod": worker.name},
        samples=[PrometheusSample(timestamp, 12.5)],
    )
    prometheus = FakePrometheusClient([], [normalized])

    telemetry = PrometheusMetricsCollector(prometheus, step_seconds=10).collect(
        timestamp,
        timestamp,
        {worker.uid: worker},
    )

    metric_names = {
        series.metric_name for series in telemetry.jobs[worker.workload_id].series.values()
    }
    assert metric_names == {
        "p99_ttft_ms",
        "p99_tpot_ms",
        "request_throughput_rps",
        "input_token_throughput_tps",
        "output_token_throughput_tps",
        "num_requests_running",
        "num_requests_waiting",
        "kv_cache_usage_ratio",
    }
    assert not metric_names.intersection(telemetry.missing_metrics)
