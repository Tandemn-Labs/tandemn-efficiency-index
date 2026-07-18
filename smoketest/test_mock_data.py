"""Contract tests for the isolated dashboard mock data."""

import json
from datetime import timedelta

from smoketest.mock_data import (
    JOB_SPECS,
    RUN_DURATION,
    SAMPLE_INTERVAL_SECONDS,
    SMOKE_DCGM_METRICS,
    build_mock_cluster_record,
    build_mock_snapshot,
)
from smoketest.run_dashboard import MockObserver
from tandemn_efficiency_index.models.workload import WorkloadRuntime
from tandemn_efficiency_index.observability import cluster_record_to_dict


def test_record_has_expected_customer_scenario() -> None:
    record = build_mock_cluster_record()

    assert record.updated_at - record.started_at == timedelta(minutes=23, seconds=17)
    assert RUN_DURATION == timedelta(minutes=23, seconds=17)
    assert record.sample_interval_seconds == SAMPLE_INTERVAL_SECONDS == 10
    assert {job.workload.name for job in record.jobs.values()} == {
        "qwen-chat",
        "glm-chat",
        "deepseek-chat",
    }
    assert all(job.workload.runtime is WorkloadRuntime.DYNAMO for job in record.jobs.values())
    assert all(job.workload.disaggregated for job in record.jobs.values())


def test_mock_exercises_complete_partial_and_missing_gpu_coverage() -> None:
    record = build_mock_cluster_record()
    expected_metrics = set(SMOKE_DCGM_METRICS)
    expected_sample_count = 140

    for job in record.jobs.values():
        workers = set(job.workers)
        gpu_metrics: dict[str, set[str]] = {}
        for series in job.telemetry.series.values():
            assert series.scope.workload_id == job.workload_id
            assert series.scope.pod_uid in workers
            assert len(series.samples) == expected_sample_count
            assert list(series.samples) == sorted(
                series.samples,
                key=lambda sample: sample.timestamp,
            )
            assert series.scope.gpu_uuid is not None
            gpu_metrics.setdefault(series.scope.gpu_uuid, set()).add(series.metric_name)

        assert len(gpu_metrics) == job.workload.total_gpus
        if job.workload.name != "deepseek-chat":
            assert all(metric_names == expected_metrics for metric_names in gpu_metrics.values())
            continue

        for gpu_uuid, metric_names in gpu_metrics.items():
            expected = expected_metrics - {"DCGM_FI_DEV_MEM_CLOCK"}
            if gpu_uuid.endswith("-03"):
                expected -= {"DCGM_FI_DEV_MEM_COPY_UTIL"}
            assert metric_names == expected

    snapshot = build_mock_snapshot()
    coverage = {job["workload"]["name"]: job["coverage"] for job in snapshot["jobs"]}
    assert coverage["qwen-chat"]["status"] == "complete"
    assert coverage["glm-chat"]["status"] == "complete"
    assert coverage["deepseek-chat"]["status"] == "missing"
    deepseek_metrics = {
        metric["metric_name"]: metric for metric in coverage["deepseek-chat"]["metrics"]
    }
    assert deepseek_metrics["DCGM_FI_DEV_MEM_COPY_UTIL"]["status"] == "partial"
    assert deepseek_metrics["DCGM_FI_DEV_MEM_COPY_UTIL"]["reporting_gpu_count"] == 3
    assert deepseek_metrics["DCGM_FI_DEV_MEM_CLOCK"]["status"] == "missing"


def test_snapshot_matches_dashboard_contract_and_is_json_serializable() -> None:
    snapshot = build_mock_snapshot()

    assert snapshot["summary"] == {
        "workload_count": 3,
        "worker_count": 6,
        "gpu_count": 9,
        "metric_count": len(SMOKE_DCGM_METRICS),
        "series_count": 148,
        "unattributed_series_count": 1,
    }
    assert snapshot["missing_metrics"] == []
    assert snapshot["attribution"] == {
        "unattributed_series_count": 1,
        "reasons": {"unattributed_pod_not_found": 1},
    }
    unattributed = snapshot["unattributed_telemetry"]["series"][0]
    assert unattributed["scope"]["gpu_instance_id"] == "3"
    assert unattributed["scope"]["pod_name"] == "deleted-worker-0"
    assert len(snapshot["jobs"]) == len(JOB_SPECS)
    assert json.loads(json.dumps(snapshot)) == snapshot


def test_mock_performance_signals_are_non_flat_and_error_free() -> None:
    record = build_mock_cluster_record()

    for job in record.jobs.values():
        gpu_series = [
            series
            for series in job.telemetry.series.values()
            if series.metric_name == "DCGM_FI_DEV_GPU_UTIL"
        ]
        xid_series = [
            series
            for series in job.telemetry.series.values()
            if series.metric_name == "DCGM_FI_DEV_XID_ERRORS"
        ]
        assert all(len({sample.value for sample in series.samples}) > 20 for series in gpu_series)
        assert all(sample.value == 0.0 for series in xid_series for sample in series.samples)


def test_mock_observer_regenerates_fresh_samples_for_each_tick() -> None:
    observer = MockObserver()
    collected_at = observer.record.updated_at + timedelta(minutes=16)

    record = observer.collect_tick(collected_at)
    snapshot = cluster_record_to_dict(record, window_seconds=900, max_points=180)

    assert record.updated_at == collected_at
    assert all(
        worker.last_seen_at == collected_at
        for job in record.jobs.values()
        for worker in job.workers.values()
    )
    assert all(
        series["samples"] for job in snapshot["jobs"] for series in job["telemetry"]["series"]
    )
