from datetime import UTC, datetime

from tandemn_efficiency_index.models.cluster_snapshot import WorkloadPod
from tandemn_efficiency_index.prometheus.client import (
    PrometheusQueryError,
    PrometheusSample,
    PrometheusSeries,
)
from tandemn_efficiency_index.prometheus.dcgm import DcgmCollector


class FakePrometheusClient:
    def __init__(self, series: list[PrometheusSeries]) -> None:
        self.series = series
        self.queries: list[tuple[str, datetime, datetime, int]] = []

    def query_range(
        self,
        query: str,
        start: datetime,
        end: datetime,
        step_seconds: int,
    ) -> list[PrometheusSeries]:
        self.queries.append((query, start, end, step_seconds))
        return self.series


def test_attributes_dcgm_series_to_worker_and_runtime_ids() -> None:
    timestamp = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    worker = WorkloadPod(
        workload_id="ray:inference/qwen-production",
        namespace="inference",
        name="qwen-raycluster-gpu-workers-abc12",
        uid="pod-uid",
        node_name="gpu-node-1",
        container_names=["ray-worker"],
        runtime_instance="qwen-production-raycluster-active",
        runtime_state="active",
        runtime_role="gpu-workers",
        first_seen_at=timestamp,
        last_seen_at=timestamp,
    )
    prometheus = FakePrometheusClient(
        [
            PrometheusSeries(
                labels={
                    "__name__": "DCGM_FI_DEV_GPU_UTIL",
                    "namespace": "monitoring",
                    "pod": "dcgm-exporter-abc12",
                    "container": "dcgm-exporter",
                    "exported_namespace": worker.namespace,
                    "exported_pod": worker.name,
                    "exported_container": "ray-worker",
                    "Hostname": "gpu-node-1",
                    "UUID": "GPU-abc",
                    "gpu": "2",
                    "job": "dcgm-exporter",
                },
                samples=[PrometheusSample(timestamp=timestamp, value=82.0)],
            )
        ]
    )
    collector = DcgmCollector(prometheus, metric_names=["DCGM_FI_DEV_GPU_UTIL"])

    telemetry = collector.collect(timestamp, timestamp, {worker.uid: worker})

    workload_telemetry = telemetry.jobs[worker.workload_id]
    series = next(iter(workload_telemetry.series.values()))
    assert series.scope.workload_id == "ray:inference/qwen-production"
    assert series.scope.pod_uid == "pod-uid"
    assert series.scope.runtime_instance == "qwen-production-raycluster-active"
    assert series.scope.runtime_role == "gpu-workers"
    assert series.scope.gpu_uuid == "GPU-abc"
    assert series.scope.local_rank == "0"
    assert series.scope.pod_namespace == "inference"
    assert series.scope.pod_name == worker.name
    assert series.scope.container_name == "ray-worker"
    assert series.scope.attribution_method == "exported_namespace_pod"
    assert series.labels == {"job": "dcgm-exporter"}
    assert series.samples[0].value == 82.0
    assert prometheus.queries[0][3] == 10


def test_preserves_unattributed_identity_and_merges_duplicate_chunks() -> None:
    timestamp = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    labels = {
        "namespace": "inference",
        "pod": "deleted-worker",
        "pod_uid": "deleted-uid",
        "Hostname": "gpu-node-2",
        "UUID": "GPU-orphan",
        "gpu": "1",
        "GPU_I_ID": "3",
        "job": "dcgm-exporter",
    }
    prometheus = FakePrometheusClient(
        [
            PrometheusSeries(
                labels=labels,
                samples=[PrometheusSample(timestamp=timestamp, value=20.0)],
            ),
            PrometheusSeries(
                labels=labels,
                samples=[
                    PrometheusSample(
                        timestamp=timestamp.replace(second=10),
                        value=30.0,
                    )
                ],
            ),
        ]
    )
    collector = DcgmCollector(prometheus, metric_names=["DCGM_FI_DEV_GPU_UTIL"])

    telemetry = collector.collect(timestamp, timestamp, {})

    assert len(telemetry.unattributed.series) == 1
    series = next(iter(telemetry.unattributed.series.values()))
    assert [sample.value for sample in series.samples] == [20.0, 30.0]
    assert series.scope.pod_uid == "deleted-uid"
    assert series.scope.pod_namespace == "inference"
    assert series.scope.pod_name == "deleted-worker"
    assert series.scope.gpu_instance_id == "3"
    assert series.scope.attribution_method == "unattributed_pod_not_found"
    assert series.labels == {"job": "dcgm-exporter"}


def test_one_failed_metric_does_not_cancel_the_collection_tick() -> None:
    timestamp = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)

    class PartiallyFailingPrometheusClient:
        def query_range(
            self,
            query: str,
            start: datetime,
            end: datetime,
            step_seconds: int,
        ) -> list[PrometheusSeries]:
            if query == "unavailable_metric":
                raise PrometheusQueryError("query unavailable")
            return [
                PrometheusSeries(
                    labels={"UUID": "GPU-idle", "gpu": "0"},
                    samples=[PrometheusSample(timestamp=timestamp, value=10.0)],
                )
            ]

    collector = DcgmCollector(
        PartiallyFailingPrometheusClient(),
        metric_names=["unavailable_metric", "DCGM_FI_DEV_GPU_UTIL"],
    )

    telemetry = collector.collect(timestamp, timestamp, {})

    assert telemetry.missing_metrics == ["unavailable_metric"]
    assert len(telemetry.unattributed.series) == 1
