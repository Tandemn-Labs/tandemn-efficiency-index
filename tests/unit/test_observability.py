import threading
from datetime import UTC, datetime, timedelta
from importlib import resources

import pytest

from tandemn_efficiency_index.models.cluster_snapshot import (
    ClusterRecord,
    JobRecord,
    WorkloadPod,
)
from tandemn_efficiency_index.models.telemetry import MetricSample, MetricScope, MetricSeries
from tandemn_efficiency_index.models.workload import Workload, WorkloadRuntime
from tandemn_efficiency_index.observability import (
    ObservabilityRuntime,
    _bearer_token_matches,
    _downsample,
    _snapshot_parameters,
    cluster_record_to_dict,
)


class FakeObserver:
    def __init__(self, record: ClusterRecord) -> None:
        self._record = record
        self.collected = threading.Event()

    @property
    def record(self) -> ClusterRecord:
        return self._record

    def collect_tick(self, collected_at: datetime | None = None) -> ClusterRecord:
        self.collected.set()
        return self._record


class FailingStatusObserver(FakeObserver):
    def status(self) -> dict[str, object]:
        raise RuntimeError("database unavailable")


def test_cluster_record_to_dict_bounds_samples_and_preserves_scope() -> None:
    now = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    record = _cluster_record(now)

    payload = cluster_record_to_dict(record, window_seconds=25, max_points=2)

    assert payload["report_type"] == "prometheus_range_report"
    assert payload["summary"] == {
        "workload_count": 1,
        "worker_count": 1,
        "gpu_count": 1,
        "metric_count": 1,
        "series_count": 1,
        "unattributed_series_count": 0,
    }
    job = payload["jobs"][0]
    assert job["workload_id"] == "dynamo:inference/qwen"
    assert job["workers"][0]["node_name"] == "gpu-node-1"
    samples = job["telemetry"]["series"][0]["samples"]
    assert [sample["value"] for sample in samples] == [60.0, 80.0]
    assert all(sample["workload_revision_id"] is None for sample in samples)
    assert job["telemetry"]["series"][0]["scope"]["gpu_uuid"] == "GPU-abc"
    assert job["telemetry"]["series"][0]["scope"]["runtime_job_key"] == "qwen"
    gpu_util_coverage = next(
        metric
        for metric in job["coverage"]["metrics"]
        if metric["metric_name"] == "DCGM_FI_DEV_GPU_UTIL"
    )
    assert gpu_util_coverage["status"] == "complete"
    assert gpu_util_coverage["reporting_gpu_count"] == 1


def test_observability_runtime_and_dashboard_assets_are_available() -> None:
    runtime = ObservabilityRuntime(FakeObserver(_cluster_record(datetime.now(UTC))))

    runtime.collect_once()
    payload = runtime.snapshot(window_seconds=3600, max_points=20)
    html = resources.files("tandemn_efficiency_index.ui").joinpath("index.html").read_text()
    javascript = resources.files("tandemn_efficiency_index.ui").joinpath("app.js").read_text()

    assert payload["summary"]["workload_count"] == 1
    assert payload["jobs"][0]["workload"]["model_id"] == "Qwen/Qwen2.5-7B-Instruct"
    assert "Tandemn Efficiency Index" in html
    assert 'id="runContext"' in html
    assert 'id="metricGrid"' in html
    assert 'id="healthGrid"' in html
    assert 'id="workerList"' in html
    assert 'id="diagnosticList"' in html
    assert 'id="scopeDetails"' in html
    assert 'id="jobJsonDetails"' in html
    assert 'class="sidebar"' not in html
    assert 'class="baseline-note"' in html
    assert "function evaluateBenchmark" in javascript
    assert "function renderBenchmarkOverlay" in javascript


def test_runtime_reports_health_and_readiness_after_collection() -> None:
    observer = FakeObserver(_cluster_record(datetime.now(UTC)))
    runtime = ObservabilityRuntime(observer)
    runtime.start()
    assert observer.collected.wait(timeout=1)

    status = runtime.status()

    assert status["running"] is True
    assert status["ready"] is True
    assert status["last_tick_completed_at"] is not None
    runtime.stop()


def test_runtime_liveness_does_not_call_dependency_status() -> None:
    runtime = ObservabilityRuntime(FailingStatusObserver(_cluster_record(datetime.now(UTC))))

    process = runtime.process_status()
    status = runtime.status()

    assert process["running"] is False
    assert status["ready"] is False
    assert status["collector"]["error"] == "database unavailable"


def test_downsample_preserves_window_extrema() -> None:
    now = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    values = [10.0] * 40
    values[11] = 95.0
    values[28] = -20.0
    samples = [
        MetricSample(now + timedelta(seconds=index), value=value)
        for index, value in enumerate(values)
    ]

    downsampled = _downsample(samples, max_points=10)

    assert len(downsampled) <= 10
    assert downsampled[0] == samples[0]
    assert downsampled[-1] == samples[-1]
    assert 95.0 in {sample.value for sample in downsampled}
    assert -20.0 in {sample.value for sample in downsampled}


def test_coverage_marks_stale_series_missing_without_discarding_history() -> None:
    now = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    record = _cluster_record(now)
    record.updated_at = now + timedelta(seconds=31)

    payload = cluster_record_to_dict(record, window_seconds=3600, max_points=180)

    coverage = next(
        metric
        for metric in payload["jobs"][0]["coverage"]["metrics"]
        if metric["metric_name"] == "DCGM_FI_DEV_GPU_UTIL"
    )
    assert coverage["status"] == "missing"
    assert coverage["reporting_gpu_count"] == 0
    assert coverage["series_count"] == 1
    assert coverage["sample_count"] == 5


def test_coverage_reports_required_metrics_when_no_dcgm_data_exists() -> None:
    now = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    record = _cluster_record(now)
    record.jobs[next(iter(record.jobs))].telemetry.series.clear()

    payload = cluster_record_to_dict(record)

    coverage = payload["jobs"][0]["coverage"]
    assert coverage["status"] == "missing"
    assert coverage["expected_gpu_count"] == 1
    assert coverage["metrics"]
    assert all(metric["status"] == "missing" for metric in coverage["metrics"])


def test_snapshot_parameters_and_bearer_auth_are_bounded() -> None:
    assert _snapshot_parameters({"window_seconds": ["0"], "max_points": ["2000"]}) == (
        None,
        2000,
    )
    with pytest.raises(ValueError, match="window_seconds must be at most"):
        _snapshot_parameters({"window_seconds": ["86401"]})
    with pytest.raises(ValueError, match="max_points must be at most"):
        _snapshot_parameters({"max_points": ["2001"]})
    assert _bearer_token_matches("", None) is True
    assert _bearer_token_matches("Bearer secret", "secret") is True
    assert _bearer_token_matches("Bearer wrong", "secret") is False


def _cluster_record(now: datetime) -> ClusterRecord:
    workload = Workload(
        runtime=WorkloadRuntime.DYNAMO,
        namespace="inference",
        name="qwen",
        uid="dgd-uid",
        api_version="nvidia.com/v1beta1",
        model_id="Qwen/Qwen2.5-7B-Instruct",
        backend="vllm",
        disaggregated=False,
        total_gpus=1,
        components=[],
    )
    pod = WorkloadPod(
        workload_id=workload.workload_id,
        namespace=workload.namespace,
        name="qwen-worker-0",
        uid="pod-uid",
        node_name="gpu-node-1",
        container_names=["worker"],
        runtime_instance="qwen",
        runtime_state="active",
        runtime_role="decode",
        first_seen_at=now - timedelta(minutes=5),
        last_seen_at=now,
        runtime_job_key="qwen",
    )
    series = MetricSeries.create(
        "DCGM_FI_DEV_GPU_UTIL",
        MetricScope(
            workload_id=workload.workload_id,
            pod_uid=pod.uid,
            container_name="worker",
            node_name=pod.node_name,
            gpu_uuid="GPU-abc",
            gpu_index="0",
            runtime_instance=pod.runtime_instance,
            runtime_job_key=pod.runtime_job_key,
            runtime_role=pod.runtime_role,
        ),
    )
    series.append(
        MetricSample(now - timedelta(seconds=offset), value=value)
        for offset, value in ((40, 40.0), (30, 50.0), (20, 60.0), (10, 70.0), (0, 80.0))
    )
    job = JobRecord(workload=workload)
    job.workers[pod.uid] = pod
    job.telemetry.series[series.series_id] = series
    job.telemetry.last_sample_at = now
    return ClusterRecord(
        started_at=now - timedelta(minutes=5),
        updated_at=now,
        window_start=now - timedelta(hours=24),
        jobs={workload.workload_id: job},
    )
