from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

import pytest

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
from tandemn_efficiency_index.prometheus.client import PrometheusQueryError


class FakePodCollector:
    def collect(
        self,
        workloads: Mapping[str, Workload],
        observed_at: datetime,
        known_pods: Mapping[str, WorkloadPod] | None = None,
    ) -> dict[str, WorkloadPod]:
        return dict(known_pods or {})


class FakePrometheusCollector:
    def __init__(self, series: MetricSeries, timestamp: datetime) -> None:
        self.series = series
        self.timestamp = timestamp
        self.emit = True
        self.calls = 0
        self.ready_checks = 0

    def collect(
        self,
        start: datetime,
        end: datetime,
        pods: Mapping[str, WorkloadPod],
    ) -> ClusterTelemetry:
        self.calls += 1
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

    def check_ready(self, at: datetime) -> None:
        self.ready_checks += 1


class EmptyPrometheusCollector:
    def check_ready(self, at: datetime) -> None:
        pass

    def collect(
        self,
        start: datetime,
        end: datetime,
        pods: Mapping[str, WorkloadPod],
    ) -> ClusterTelemetry:
        return ClusterTelemetry()


class UnavailablePrometheusCollector(EmptyPrometheusCollector):
    def check_ready(self, at: datetime) -> None:
        raise PrometheusQueryError("connection refused")


class FakeWorkloadDiscovery:
    def __init__(self, workloads: dict[str, Workload]) -> None:
        self.workloads = workloads
        self.calls = 0

    def discover(self) -> dict[str, Workload]:
        self.calls += 1
        return dict(self.workloads)


def test_observer_queries_prometheus_only_for_live_snapshot_windows() -> None:
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
    prometheus_collector = FakePrometheusCollector(series, now)
    observer = ClusterObserver(
        workloads={workload.workload_id: workload},
        pod_collector=FakePodCollector(),
        prometheus_collector=prometheus_collector,
        started_at=now,
    )

    state_record = observer.collect_tick(now)
    observer.collect_tick(now + timedelta(seconds=10))
    prometheus_collector.timestamp = now + timedelta(seconds=20)
    observer.collect_tick(now + timedelta(seconds=20))
    observer.collect_tick(now + timedelta(seconds=30))
    assert prometheus_collector.calls == 0
    assert not hasattr(state_record.jobs[workload.workload_id], "telemetry")

    live_record = observer.live_record(30, now + timedelta(seconds=30))
    stored_series = live_record.jobs[workload.workload_id].telemetry.series[series.series_id]

    assert prometheus_collector.calls == 1
    assert live_record.sample_interval_seconds == 10
    assert [sample.value for sample in stored_series.samples] == [42.0]
    assert stored_series.samples[0].timestamp == now + timedelta(seconds=20)
    assert not hasattr(state_record.jobs[workload.workload_id], "telemetry")


def test_observer_reconciles_discovered_workloads_on_refresh_interval() -> None:
    now = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    workload = _workload("qwen", "dgd-uid")
    discovery = FakeWorkloadDiscovery({workload.workload_id: workload})
    observer = ClusterObserver(
        workloads={},
        pod_collector=FakePodCollector(),
        prometheus_collector=EmptyPrometheusCollector(),
        workload_discovery=discovery,
        discovery_interval=timedelta(seconds=60),
        started_at=now,
    )

    record = observer.collect_tick(now)
    observer.collect_tick(now + timedelta(seconds=10))

    assert discovery.calls == 1
    assert record.jobs[workload.workload_id].active is True

    discovery.workloads = {}
    observer.collect_tick(now + timedelta(seconds=60))

    assert discovery.calls == 2
    assert record.jobs[workload.workload_id].active is False
    assert record.jobs[workload.workload_id].removed_at == now + timedelta(seconds=60)


def test_observer_is_not_ready_when_prometheus_check_fails() -> None:
    now = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    workload = _workload("qwen", "dgd-uid")
    observer = ClusterObserver(
        workloads={workload.workload_id: workload},
        pod_collector=FakePodCollector(),
        prometheus_collector=UnavailablePrometheusCollector(),
        started_at=now,
    )

    with pytest.raises(PrometheusQueryError, match="connection refused"):
        observer.collect_tick(now)

    status = observer.status()
    assert status["ready"] is False
    assert status["last_prometheus_check_at"] is None
    assert status["last_collection_error"] == "connection refused"


def _workload(name: str, uid: str) -> Workload:
    return Workload(
        runtime=WorkloadRuntime.DYNAMO,
        namespace="inference",
        name=name,
        uid=uid,
        api_version="nvidia.com/v1beta1",
        model_id=name,
        backend="vllm",
        disaggregated=False,
        total_gpus=1,
        components=[],
    )
