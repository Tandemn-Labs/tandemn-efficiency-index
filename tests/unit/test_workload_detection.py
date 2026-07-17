from typing import Any

from tandemn_efficiency_index.models.workload import WorkloadRuntime
from tandemn_efficiency_index.workload_detection import (
    ClusterWorkloadDetector,
    WorkloadTarget,
)


class ApiError(Exception):
    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(f"API status {status}")


class FakeCustomObjectsApi:
    def __init__(self, resources: list[dict[str, Any]]) -> None:
        self.resources = resources

    def get_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str
    ) -> dict[str, Any]:
        for resource in self.resources:
            metadata = resource["metadata"]
            if (
                resource["apiVersion"] == f"{group}/{version}"
                and metadata["namespace"] == namespace
                and metadata["name"] == name
            ):
                return resource
        raise ApiError(404)


def test_aggregates_dynamo_and_ray_into_one_workload_shape() -> None:
    dynamo = {
        "apiVersion": "nvidia.com/v1beta1",
        "kind": "DynamoGraphDeployment",
        "metadata": {"namespace": "inference", "name": "dynamo-qwen"},
        "spec": {
            "backendFramework": "vllm",
            "components": [
                {
                    "name": "DecodeWorker",
                    "type": "decode",
                    "replicas": 1,
                    "podTemplate": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "main",
                                    "image": "dynamo-vllm:1.2.1",
                                    "args": ["--model", "Qwen/Qwen3-8B"],
                                    "resources": {"limits": {"nvidia.com/gpu": "1"}},
                                }
                            ]
                        }
                    },
                }
            ],
        },
    }
    ray = {
        "apiVersion": "ray.io/v1",
        "kind": "RayService",
        "metadata": {"namespace": "inference", "name": "ray-qwen"},
        "spec": {
            "serveConfigV2": """
applications:
  - name: qwen
    args:
      llm_configs:
        - model_loading_config:
            model_id: Qwen/Qwen3-8B
          engine_kwargs:
            tensor_parallel_size: 1
"""
        },
    }
    detector = ClusterWorkloadDetector(FakeCustomObjectsApi([dynamo, ray]))

    workloads = detector.detect(
        [
            WorkloadTarget(WorkloadRuntime.DYNAMO, "inference", "dynamo-qwen"),
            WorkloadTarget(WorkloadRuntime.RAY, "inference", "ray-qwen"),
        ]
    )

    assert set(workloads) == {
        "dynamo:inference/dynamo-qwen",
        "ray:inference/ray-qwen",
    }
    output_shapes = {frozenset(workload.to_dict()) for workload in workloads.values()}
    assert len(output_shapes) == 1
    assert {workload.model_id for workload in workloads.values()} == {"Qwen/Qwen3-8B"}
