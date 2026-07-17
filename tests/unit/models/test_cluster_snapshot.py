from datetime import UTC, datetime, timedelta

from tandemn_efficiency_index.models.cluster_snapshot import (
    ClusterRecord,
    JobRecord,
    WorkloadPod,
)
from tandemn_efficiency_index.models.telemetry import (
    ClusterTelemetry,
    MetricSample,
    MetricScope,
    MetricSeries,
    WorkloadTelemetry,
)
from tandemn_efficiency_index.models.workload import Workload, WorkloadRuntime


def _workload(runtime: WorkloadRuntime) -> Workload:
    return Workload(
        runtime=runtime,
        namespace="inference",
        name="qwen-production",
        uid=None,
        api_version="example/v1",
        model_id="qwen",
        backend="vllm",
        disaggregated=False,
        total_gpus=2,
        components=[],
    )


def test_cluster_record_scopes_workers_and_telemetry_to_each_job() -> None:
    now = datetime.now(UTC)
    dynamo = _workload(WorkloadRuntime.DYNAMO)
    ray = _workload(WorkloadRuntime.RAY)
    pod = WorkloadPod(
        workload_id=ray.workload_id,
        namespace="inference",
        name="qwen-production-worker-abc12",
        uid="pod-uid",
        node_name="gpu-node-1",
        container_names=["ray-worker"],
        runtime_instance="qwen-production-raycluster-abc12",
        runtime_state="active",
        runtime_role="gpu-workers",
        first_seen_at=now - timedelta(minutes=5),
        last_seen_at=now,
    )
    series = MetricSeries.create(
        metric_name="DCGM_FI_DEV_GPU_UTIL",
        scope=MetricScope(
            workload_id=ray.workload_id,
            pod_uid=pod.uid,
            container_name="ray-worker",
            node_name=pod.node_name,
            gpu_uuid="GPU-abc",
            gpu_index="0",
        ),
        labels={"Hostname": "gpu-node-1"},
    )
    series.append([MetricSample(timestamp=now, value=82.0)])
    record = ClusterRecord(
        started_at=now - timedelta(minutes=5),
        updated_at=now,
        window_start=now - timedelta(hours=24),
        jobs={
            dynamo.workload_id: JobRecord(workload=dynamo),
            ray.workload_id: JobRecord(workload=ray),
        },
    )
    record.jobs[ray.workload_id].workers[pod.uid] = pod
    record.jobs[ray.workload_id].telemetry.merge(
        ClusterTelemetry(
            jobs={
                ray.workload_id: _workload_telemetry(ray.workload_id, series),
            }
        ).for_workload(ray.workload_id)
    )

    assert set(record.jobs) == {
        "dynamo:inference/qwen-production",
        "ray:inference/qwen-production",
    }
    ray_job = record.jobs[ray.workload_id]
    ray_telemetry = ray_job.telemetry
    assert ray_telemetry.series[series.series_id].samples[0].value == 82.0
    assert ray_telemetry.series[series.series_id].scope.pod_uid == pod.uid
    assert ray_job.workers[pod.uid].name == pod.name
    assert record.jobs[dynamo.workload_id].telemetry.series == {}
    assert record.sample_interval_seconds == 10


def test_metric_series_identity_is_stable_across_label_order() -> None:
    scope = MetricScope(
        workload_id="dynamo:inference/qwen-production",
        pod_uid="pod-uid",
        container_name="main",
        node_name="gpu-node-1",
        gpu_uuid="GPU-abc",
        gpu_index="0",
    )

    first = MetricSeries.create(
        "DCGM_FI_DEV_GPU_UTIL",
        scope,
        {"pod": "worker-1", "namespace": "inference"},
    )
    second = MetricSeries.create(
        "DCGM_FI_DEV_GPU_UTIL",
        scope,
        {"namespace": "inference", "pod": "worker-1"},
    )

    assert first.series_id == second.series_id


def test_cluster_telemetry_preserves_unattributed_series() -> None:
    now = datetime.now(UTC)
    unattributed = MetricSeries.create(
        "DCGM_FI_DEV_GPU_UTIL",
        MetricScope(
            workload_id=None,
            pod_uid=None,
            container_name=None,
            node_name="gpu-node-2",
            gpu_uuid="GPU-idle",
            gpu_index="1",
        ),
    )
    unattributed.append([MetricSample(now, 0.0)])
    telemetry = ClusterTelemetry()
    telemetry.unattributed.series[unattributed.series_id] = unattributed

    workload_view = telemetry.for_workload("ray:inference/qwen-production")

    assert telemetry.unattributed.series[unattributed.series_id].scope.workload_id is None
    assert workload_view.series == {}


def _workload_telemetry(workload_id: str, series: MetricSeries) -> WorkloadTelemetry:
    return WorkloadTelemetry(workload_id=workload_id, series={series.series_id: series})
