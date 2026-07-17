from datetime import UTC, datetime

from tandemn_efficiency_index.models.cluster_snapshot import WorkloadPod
from tandemn_efficiency_index.models.workload import Workload, WorkloadRuntime
from tandemn_efficiency_index.prometheus.client import PrometheusSample, PrometheusSeries
from tandemn_efficiency_index.prometheus.vllm import VLLM_QUERIES, VllmCollector


class FakePrometheusClient:
    def __init__(self, timestamp: datetime) -> None:
        self.timestamp = timestamp
        self.queries: list[str] = []

    def query_range(
        self,
        query: str,
        start: datetime,
        end: datetime,
        step_seconds: int,
    ) -> list[PrometheusSeries]:
        self.queries.append(query)
        return [
            PrometheusSeries(
                labels={},
                samples=[PrometheusSample(timestamp=self.timestamp, value=42.0)],
            )
        ]


def test_collects_ground_truth_vllm_queries_once_per_worker() -> None:
    timestamp = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    workload = Workload(
        runtime=WorkloadRuntime.DYNAMO,
        namespace="inference",
        name="qwen-production",
        uid="dgd-uid",
        api_version="nvidia.com/v1beta1",
        model_id="Qwen/Qwen3-32B",
        backend="vllm",
        disaggregated=True,
        total_gpus=2,
        components=[],
    )
    worker = WorkloadPod(
        workload_id=workload.workload_id,
        namespace=workload.namespace,
        name="qwen-production-worker-abc12",
        uid="pod-uid",
        node_name="gpu-node-1",
        container_names=["main"],
        runtime_instance=workload.name,
        runtime_state="active",
        runtime_role="decode",
        first_seen_at=timestamp,
        last_seen_at=timestamp,
    )
    prometheus = FakePrometheusClient(timestamp)
    collector = VllmCollector(prometheus)

    telemetry = collector.collect(
        timestamp,
        timestamp,
        {worker.uid: worker},
        {workload.workload_id: workload},
    )

    workload_telemetry = telemetry.jobs[workload.workload_id]
    assert {series.metric_name for series in workload_telemetry.series.values()} == set(
        VLLM_QUERIES
    )
    assert all(
        series.scope.pod_uid == worker.uid and series.scope.attribution_method == "worker_pod_query"
        for series in workload_telemetry.series.values()
    )
    assert (
        'sum(rate(vllm:generation_tokens_total{pod="qwen-production-worker-abc12"}[1m]))'
        in prometheus.queries
    )
    assert len(prometheus.queries) == len(VLLM_QUERIES)
    assert telemetry.missing_metrics == []
