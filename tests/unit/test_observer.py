from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

from tandemn_efficiency_index.models.cluster_snapshot import WorkloadPod
from tandemn_efficiency_index.models.telemetry import (
    ClusterTelemetry,
    MetricSample,
    MetricScope,
    MetricSeries,
    WorkloadTelemetry,
)
from tandemn_efficiency_index.models.workload import Workload, WorkloadRuntime
from tandemn_efficiency_index.observer import ClusterObserver


class FakePodCollector:
    def collect(
        self,
        workloads: Mapping[str, Workload],
        observed_at: datetime,
        known_pods: Mapping[str, WorkloadPod] | None = None,
    ) -> dict[str, WorkloadPod]:
        return dict(known_pods or {})


class FakeDcgmCollector:
    def __init__(self, series: MetricSeries, timestamp: datetime) -> None:
        self.series = series
        self.timestamp = timestamp
        self.emit = True

    def collect(
        self,
        start: datetime,
        end: datetime,
        pods: Mapping[str, WorkloadPod],
    ) -> ClusterTelemetry:
        if not self.emit:
            return ClusterTelemetry()
        batch_series = MetricSeries(
            series_id=self.series.series_id,
            metric_name=self.series.metric_name,
            scope=self.series.scope,
            labels=dict(self.series.labels),
        )
        batch_series.append([MetricSample(self.timestamp, 42.0)])
        workload_id = self.series.scope.workload_id
        assert workload_id is not None
        return ClusterTelemetry(
            jobs={
                workload_id: WorkloadTelemetry(
                    workload_id=workload_id,
                    series={batch_series.series_id: batch_series},
                )
            }
        )


def test_observer_deduplicates_overlapping_ten_second_queries() -> None:
    now = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    workload = Workload(
        runtime=WorkloadRuntime.DYNAMO,
        namespace="inference",
        name="qwen",
        uid="dgd-uid",
        api_version="nvidia.com/v1beta1",
        model_id="qwen",
        backend="vllm",
        disaggregated=False,
        total_gpus=1,
        components=[],
    )
    series = MetricSeries.create(
        "DCGM_FI_DEV_GPU_UTIL",
        MetricScope(
            workload_id=workload.workload_id,
            pod_uid="pod-uid",
            container_name="main",
            node_name="gpu-node-1",
            gpu_uuid="GPU-abc",
            gpu_index="0",
        ),
    )
    dcgm_collector = FakeDcgmCollector(series, now)
    observer = ClusterObserver(
        workloads={workload.workload_id: workload},
        pod_collector=FakePodCollector(),
        dcgm_collector=dcgm_collector,
        started_at=now,
    )

    first_record = observer.collect_tick(now)
    record = observer.collect_tick(now + timedelta(seconds=10))
    dcgm_collector.timestamp = now + timedelta(seconds=20)
    observer.collect_tick(now + timedelta(seconds=20))
    dcgm_collector.emit = False
    observer.collect_tick(now + timedelta(seconds=30))
    stored_series = record.jobs[workload.workload_id].telemetry.series[series.series_id]

    assert record is first_record
    assert record.sample_interval_seconds == 10
    assert len(stored_series.samples) == 2
    assert stored_series.samples[0].value == 42.0
    assert stored_series.samples[1].timestamp == now + timedelta(seconds=20)
