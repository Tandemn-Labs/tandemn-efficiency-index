from typing import Any

import pytest

from tandemn_efficiency_index.ray.workload_detection import (
    RayDiscoveryError,
    RayWorkloadDetector,
    RayWorkloadTarget,
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
def ray_service() -> dict[str, Any]:
    return {
        "apiVersion": "ray.io/v1",
        "kind": "RayService",
        "metadata": {
            "name": "qwen-production",
            "namespace": "inference",
            "uid": "rayservice-uid",
        },
        "spec": {
            "serveConfigV2": """
applications:
  - name: llm-app
    import_path: ray.serve.llm:build_openai_app
    args:
      llm_configs:
        - model_loading_config:
            model_id: qwen
            model_source: Qwen/Qwen2.5-7B-Instruct
          accelerator_type: L4
          engine_kwargs:
            tensor_parallel_size: 2
            max_model_len: 8192
            max_num_seqs: 128
            gpu_memory_utilization: 0.9
            enable_prefix_caching: true
          deployment_config:
            max_ongoing_requests: 12
            max_queued_requests: 50
            autoscaling_config:
              min_replicas: 1
              max_replicas: 4
              target_ongoing_requests: 8
""",
            "rayClusterConfig": {
                "workerGroupSpecs": [
                    {
                        "groupName": "gpu-workers",
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "name": "ray-worker",
                                        "image": "rayproject/ray-llm:2.55.1-py312-gpu",
                                    }
                                ]
                            }
                        },
                    }
                ]
            },
        },
        "status": {
            "activeServiceStatus": {"rayClusterName": "qwen-production-raycluster-active"},
            "pendingServiceStatus": {"rayClusterName": "qwen-production-raycluster-pending"},
        },
    }


def test_fetches_target_and_parses_llm_configuration(ray_service: dict[str, Any]) -> None:
    api = FakeCustomObjectsApi([ray_service])
    detector = RayWorkloadDetector(api)

    workloads = detector.detect([RayWorkloadTarget("inference", "qwen-production")])

    workload = workloads["ray:inference/qwen-production"]
    assert workload.model_id == "qwen"
    assert workload.backend == "vllm"
    assert workload.total_gpus == 2
    component = workload.components[0]
    assert component.name == "llm-app"
    assert component.replicas == 1
    assert component.gpus_per_replica == 2
    assert component.x["model_source"] == "Qwen/Qwen2.5-7B-Instruct"
    assert component.x["tp"] == 2
    assert component.x["max_model_len"] == 8192
    assert component.x["max_num_seq"] == 128
    assert component.x["gpu_mem_util"] == 0.9
    assert component.x["prefix_cache_enabled"] is True
    assert component.x["max_replicas"] == 4
    assert workload.declared_intent is not None
    assert workload.declared_intent.model_id == "qwen"
    assert workload.declared_intent.backend == "vllm"
    assert workload.declared_intent.slo == {}
    assert workload.declared_intent.components == [
        {
            "name": "llm-app",
            "component_type": "llm",
            "replicas": 1,
            "max_ongoing_requests": 12,
            "max_queued_requests": 50,
            "autoscaling": {
                "min_replicas": 1,
                "max_replicas": 4,
                "target_ongoing_requests": 8,
            },
        }
    ]
    assert component.placement["accelerator_type"] == "L4"
    assert component.image == "rayproject/ray-llm:2.55.1-py312-gpu"
    active_head, active, pending_head, pending = workload.pod_selectors
    assert active_head.match_labels["ray.io/node-type"] == "head"
    assert active.runtime_instance == "qwen-production-raycluster-active"
    assert active.runtime_job_key == "qwen-production-raycluster-active"
    assert active.runtime_state == "active"
    assert active.match_labels == {
        "ray.io/cluster": "qwen-production-raycluster-active",
        "ray.io/node-type": "worker",
    }
    assert active.role_label == "ray.io/group"
    assert pending_head.match_labels["ray.io/node-type"] == "head"
    assert pending.runtime_instance == "qwen-production-raycluster-pending"
    assert pending.runtime_job_key == "qwen-production-raycluster-pending"
    assert pending.runtime_state == "pending"
    assert api.requested_versions == ["v1"]


def test_returns_same_json_shape_as_dynamo(ray_service: dict[str, Any]) -> None:
    detector = RayWorkloadDetector(FakeCustomObjectsApi([ray_service]))

    workload = detector.detect([RayWorkloadTarget("inference", "qwen-production")])[
        "ray:inference/qwen-production"
    ]
    result = workload.to_dict()

    assert workload.workload_id == "ray:inference/qwen-production"
    assert set(result) == {
        "runtime",
        "namespace",
        "name",
        "uid",
        "api_version",
        "model_id",
        "backend",
        "disaggregated",
        "total_gpus",
        "components",
        "pod_selectors",
        "declared_intent",
        "source_generation",
        "source_resource_version",
    }


def test_falls_back_to_v1alpha1(ray_service: dict[str, Any]) -> None:
    ray_service["apiVersion"] = "ray.io/v1alpha1"
    api = FakeCustomObjectsApi([ray_service])
    detector = RayWorkloadDetector(api)

    workload = detector.detect([RayWorkloadTarget("inference", "qwen-production")])[
        "ray:inference/qwen-production"
    ]

    assert workload.api_version == "ray.io/v1alpha1"
    assert api.requested_versions == ["v1", "v1alpha1"]


def test_retains_multiple_models_in_one_ray_service(ray_service: dict[str, Any]) -> None:
    ray_service["spec"]["serveConfigV2"] = """
applications:
  - name: two-models
    args:
      llm_configs:
        - model_loading_config: {model_id: model-a}
        - model_loading_config: {model_id: model-b}
"""
    detector = RayWorkloadDetector(FakeCustomObjectsApi([ray_service]))

    workload = detector.detect([RayWorkloadTarget("inference", "qwen-production")])[
        "ray:inference/qwen-production"
    ]

    assert workload.model_id == "model-a, model-b"
    assert len(workload.components) == 2


def test_parses_custom_serve_wrapper_with_explicit_model_settings(
    ray_service: dict[str, Any],
) -> None:
    ray_service["spec"]["serveConfigV2"] = """
applications:
  - name: custom-vllm
    import_path: company.serving.vllm:app
    deployments:
      - name: ModelDeployment
        num_replicas: 2
        ray_actor_options: {num_gpus: 2}
        user_config:
          model_id: Qwen/Qwen3-32B
          backend: vllm
          engine_kwargs: {tensor_parallel_size: 2}
"""
    detector = RayWorkloadDetector(FakeCustomObjectsApi([ray_service]))

    workload = detector.detect([RayWorkloadTarget("inference", "qwen-production")])[
        "ray:inference/qwen-production"
    ]

    assert workload.model_id == "Qwen/Qwen3-32B"
    assert workload.backend == "vllm"
    assert workload.total_gpus == 4
    assert workload.components[0].replicas == 2
    assert workload.components[0].gpus_per_replica == 2


def test_parses_prefill_decode_as_two_components(ray_service: dict[str, Any]) -> None:
    ray_service["spec"]["serveConfigV2"] = """
applications:
  - name: pd-app
    import_path: ray.serve.llm:build_pd_openai_app
    args:
      prefill_config:
        model_loading_config: {model_id: qwen}
        engine_kwargs:
          tensor_parallel_size: 2
          kv_transfer_config: {kv_connector: NixlConnector, kv_role: kv_producer}
      decode_config:
        model_loading_config: {model_id: qwen}
        engine_kwargs:
          tensor_parallel_size: 2
          kv_transfer_config: {kv_connector: NixlConnector, kv_role: kv_consumer}
"""
    detector = RayWorkloadDetector(FakeCustomObjectsApi([ray_service]))

    workload = detector.detect([RayWorkloadTarget("inference", "qwen-production")])[
        "ray:inference/qwen-production"
    ]

    assert workload.disaggregated is True
    assert workload.total_gpus == 4
    assert [component.component_type for component in workload.components] == [
        "prefill",
        "decode",
    ]
    assert workload.components[0].x["kv_transfer_method"] == "NixlConnector"


def test_requires_at_least_one_target(ray_service: dict[str, Any]) -> None:
    detector = RayWorkloadDetector(FakeCustomObjectsApi([ray_service]))

    with pytest.raises(ValueError, match="At least one RayService"):
        detector.detect([])


def test_raises_clear_error_when_no_api_version_exists() -> None:
    detector = RayWorkloadDetector(FakeCustomObjectsApi([]))

    with pytest.raises(RayDiscoveryError, match="was not found"):
        detector.detect([RayWorkloadTarget("inference", "missing")])
