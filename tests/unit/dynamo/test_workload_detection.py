from typing import Any

import pytest

from tandemn_efficiency_index.dynamo.workload_detection import (
    DynamoDiscoveryError,
    DynamoWorkloadDetector,
    DynamoWorkloadTarget,
)


class ApiError(Exception):
    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(f"API status {status}")


class FakeCustomObjectsApi:
    def __init__(self, resources: list[dict[str, Any]]) -> None:
        self.resources = resources
        self.requested_versions: list[str] = []

    def get_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str
    ) -> dict[str, Any]:
        self.requested_versions.append(version)
        for resource in self.resources:
            metadata = resource["metadata"]
            if (
                resource["apiVersion"] == f"{group}/{version}"
                and metadata["namespace"] == namespace
                and metadata["name"] == name
            ):
                return resource
        raise ApiError(404)


@pytest.fixture
def dgd() -> dict[str, Any]:
    return {
        "apiVersion": "nvidia.com/v1alpha1",
        "kind": "DynamoGraphDeployment",
        "metadata": {
            "name": "qwen-production",
            "namespace": "inference",
            "uid": "dgd-uid",
            "generation": 3,
        },
        "spec": {
            "backendFramework": "vllm",
            "services": {
                "Frontend": {
                    "componentType": "frontend",
                    "replicas": 1,
                    "extraPodSpec": {
                        "mainContainer": {"image": "nvcr.io/nvidia/ai-dynamo/frontend:1.2.0"}
                    },
                },
                "Worker": {
                    "componentType": "worker",
                    "replicas": 2,
                    "modelRef": {"name": "Qwen/Qwen3-32B"},
                    "extraPodSpec": {
                        "mainContainer": {
                            "image": "nvcr.io/nvidia/ai-dynamo/vllm-runtime:1.2.0",
                            "args": [
                                "--model",
                                "Qwen/Qwen3-32B",
                                "--tensor-parallel-size",
                                "2",
                                "--max-num-seqs=128",
                                "--gpu-memory-utilization",
                                "0.9",
                                "--enable-prefix-caching",
                            ],
                            "resources": {"limits": {"nvidia.com/gpu": "2"}},
                        },
                        "nodeSelector": {"gpu.product": "H100"},
                    },
                },
            },
        },
    }


def test_fetches_target_and_parses_configuration(dgd: dict[str, Any]) -> None:
    api = FakeCustomObjectsApi([dgd])
    detector = DynamoWorkloadDetector(api)

    workloads = detector.detect([DynamoWorkloadTarget("inference", "qwen-production")])

    workload = workloads["dynamo:inference/qwen-production"]
    assert workload.model_id == "Qwen/Qwen3-32B"
    assert workload.backend == "vllm"
    assert workload.total_gpus == 4
    worker = next(component for component in workload.components if component.name == "Worker")
    assert worker.x["tp"] == 2
    assert worker.x["max_num_seq"] == 128
    assert worker.x["gpu_mem_util"] == 0.9
    assert worker.x["prefix_cache_enabled"] is True
    assert worker.gpus_per_replica == 2
    assert worker.total_gpus == 4
    selector = workload.pod_selectors[0]
    assert selector.runtime_instance == "qwen-production"
    assert selector.runtime_job_key == "qwen-production"
    assert selector.runtime_state == "active"
    assert selector.match_labels == {
        "nvidia.com/dynamo-graph-deployment-name": "qwen-production",
    }
    assert selector.role_label == "nvidia.com/dynamo-sub-component-type"
    assert api.requested_versions == ["v1beta1", "v1alpha1"]


def test_returns_json_serializable_workload_map(dgd: dict[str, Any]) -> None:
    api = FakeCustomObjectsApi([dgd])
    detector = DynamoWorkloadDetector(api)

    workload = detector.detect(
        [DynamoWorkloadTarget(namespace="inference", name="qwen-production")]
    )["dynamo:inference/qwen-production"]
    result = workload.to_dict()

    assert workload.workload_id == "dynamo:inference/qwen-production"
    assert result["runtime"] == "dynamo"
    assert result["model_id"] == "Qwen/Qwen3-32B"
    assert result["components"][1]["total_gpus"] == 4
    assert result["pod_selectors"][0]["runtime_instance"] == "qwen-production"


def test_parses_current_v1beta1_component_list() -> None:
    dgd = {
        "apiVersion": "nvidia.com/v1beta1",
        "kind": "DynamoGraphDeployment",
        "metadata": {
            "name": "qwen-disaggregated",
            "namespace": "inference",
            "uid": "dgd-beta-uid",
        },
        "spec": {
            "components": [
                {
                    "name": "VllmPrefillWorker",
                    "type": "prefill",
                    "replicas": 1,
                    "podTemplate": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "main",
                                    "image": "nvcr.io/nvidia/ai-dynamo/vllm-runtime:1.2.1",
                                    "command": ["python3", "-m", "dynamo.vllm"],
                                    "args": ["--model", "Qwen/Qwen3-32B", "--tp", "2"],
                                    "resources": {"limits": {"nvidia.com/gpu": "2"}},
                                }
                            ]
                        }
                    },
                },
                {
                    "name": "VllmDecodeWorker",
                    "type": "decode",
                    "replicas": 2,
                    "podTemplate": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "main",
                                    "image": "nvcr.io/nvidia/ai-dynamo/vllm-runtime:1.2.1",
                                    "command": ["python3", "-m", "dynamo.vllm"],
                                    "args": ["--model", "Qwen/Qwen3-32B"],
                                    "resources": {"limits": {"nvidia.com/gpu": "2"}},
                                }
                            ]
                        }
                    },
                },
            ]
        },
    }
    api = FakeCustomObjectsApi([dgd])
    detector = DynamoWorkloadDetector(api)

    workload = detector.detect([DynamoWorkloadTarget("inference", "qwen-disaggregated")])[
        "dynamo:inference/qwen-disaggregated"
    ]

    assert workload.api_version == "nvidia.com/v1beta1"
    assert workload.backend == "vllm"
    assert workload.disaggregated is True
    assert workload.total_gpus == 6
    assert [component.component_type for component in workload.components] == [
        "prefill",
        "decode",
    ]
    assert api.requested_versions == ["v1beta1"]


def test_rejects_dgd_without_exactly_one_model(dgd: dict[str, Any]) -> None:
    dgd["spec"]["services"]["Worker"]["modelRef"] = {}
    dgd["spec"]["services"]["Worker"]["extraPodSpec"]["mainContainer"]["args"] = []
    detector = DynamoWorkloadDetector(FakeCustomObjectsApi([dgd]))

    with pytest.raises(ValueError, match="exactly one model"):
        detector.detect([DynamoWorkloadTarget(namespace="inference", name="qwen-production")])


def test_requires_at_least_one_target(dgd: dict[str, Any]) -> None:
    detector = DynamoWorkloadDetector(FakeCustomObjectsApi([dgd]))

    with pytest.raises(ValueError, match="At least one DGD"):
        detector.detect([])


def test_raises_clear_error_when_no_api_version_exists() -> None:
    detector = DynamoWorkloadDetector(FakeCustomObjectsApi([]))

    with pytest.raises(DynamoDiscoveryError, match="was not found"):
        detector.detect([DynamoWorkloadTarget("inference", "missing")])
