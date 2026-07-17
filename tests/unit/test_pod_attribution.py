from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

from tandemn_efficiency_index.models.cluster_snapshot import WorkloadPod
from tandemn_efficiency_index.models.workload import (
    Workload,
    WorkloadPodSelector,
    WorkloadRuntime,
)
from tandemn_efficiency_index.pod_attribution import WorkloadPodCollector


class FakeCoreV1Api:
    def __init__(self, pods: list[Any]) -> None:
        self.pods = pods
        self.requests: list[tuple[str, str]] = []

    def list_namespaced_pod(self, namespace: str, label_selector: str) -> Any:
        self.requests.append((namespace, label_selector))
        return SimpleNamespace(items=self.pods)


def _workload() -> Workload:
    return Workload(
        runtime=WorkloadRuntime.DYNAMO,
        namespace="inference",
        name="qwen-production",
        uid="dgd-uid",
        api_version="nvidia.com/v1beta1",
        model_id="Qwen/Qwen3-32B",
        backend="vllm",
        disaggregated=False,
        total_gpus=1,
        components=[],
        pod_selectors=[
            WorkloadPodSelector(
                runtime_instance="qwen-production",
                runtime_state="active",
                match_labels={
                    "nvidia.com/dynamo-graph-deployment-name": "qwen-production",
                    "nvidia.com/dynamo-component-type": "worker",
                },
                role_label="nvidia.com/dynamo-sub-component-type",
            )
        ],
    )


def test_collects_worker_identity_and_preserves_first_seen_time() -> None:
    pod = SimpleNamespace(
        metadata=SimpleNamespace(
            uid="pod-uid",
            namespace="inference",
            name="qwen-worker-abc12",
            labels={
                "nvidia.com/dynamo-component-type": "worker",
                "nvidia.com/dynamo-sub-component-type": "decode",
            },
        ),
        spec=SimpleNamespace(
            node_name="gpu-node-1",
            containers=[SimpleNamespace(name="main")],
        ),
    )
    api = FakeCoreV1Api([pod])
    collector = WorkloadPodCollector(api)
    workload = _workload()
    first_tick = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    existing = WorkloadPod(
        workload_id=workload.workload_id,
        namespace="inference",
        name="qwen-worker-abc12",
        uid="pod-uid",
        node_name="gpu-node-1",
        container_names=["main"],
        runtime_instance="qwen-production",
        runtime_state="active",
        runtime_role="decode",
        first_seen_at=first_tick,
        last_seen_at=first_tick,
    )

    pods = collector.collect(
        {workload.workload_id: workload},
        first_tick + timedelta(seconds=10),
        {existing.uid: existing},
    )

    worker = pods["pod-uid"]
    assert worker.workload_id == "dynamo:inference/qwen-production"
    assert worker.runtime_instance == "qwen-production"
    assert worker.runtime_role == "decode"
    assert worker.first_seen_at == first_tick
    assert worker.last_seen_at == first_tick + timedelta(seconds=10)
    assert api.requests == [
        (
            "inference",
            "nvidia.com/dynamo-component-type=worker,"
            "nvidia.com/dynamo-graph-deployment-name=qwen-production",
        )
    ]


def test_does_not_return_a_known_worker_after_it_disappears() -> None:
    workload = _workload()
    observed_at = datetime(2026, 7, 16, 12, 0, tzinfo=UTC)
    known = WorkloadPod(
        workload_id=workload.workload_id,
        namespace=workload.namespace,
        name="terminated-worker",
        uid="terminated-pod-uid",
        node_name="gpu-node-1",
        container_names=["main"],
        runtime_instance=workload.name,
        runtime_state="active",
        runtime_role="decode",
        first_seen_at=observed_at - timedelta(minutes=5),
        last_seen_at=observed_at - timedelta(seconds=10),
    )
    collector = WorkloadPodCollector(FakeCoreV1Api([]))

    pods = collector.collect(
        {workload.workload_id: workload},
        observed_at,
        {known.uid: known},
    )

    assert pods == {}
