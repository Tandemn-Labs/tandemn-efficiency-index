from types import SimpleNamespace
from typing import Any

import pytest

from tandemn_efficiency_index.kubernetes_discovery import (
    KubernetesWorkloadDiscovery,
    WorkloadDiscoveryError,
)


class FakeApiextensionsApi:
    def __init__(self, crds: list[Any], error: Exception | None = None) -> None:
        self.crds = crds
        self.error = error
        self.calls = 0

    def list_custom_resource_definition(self) -> Any:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return SimpleNamespace(items=self.crds)


class FakeCustomObjectsApi:
    def __init__(self, resources: list[dict[str, Any]]) -> None:
        self.resources = resources
        self.cluster_requests: list[tuple[str, str, str]] = []
        self.namespace_requests: list[tuple[str, str, str, str]] = []

    def get_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def list_cluster_custom_object(
        self,
        group: str,
        version: str,
        plural: str,
    ) -> dict[str, Any]:
        self.cluster_requests.append((group, version, plural))
        return {"items": self._matching(group, version, plural)}

    def list_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
    ) -> dict[str, Any]:
        self.namespace_requests.append((group, version, namespace, plural))
        return {
            "items": [
                resource
                for resource in self._matching(group, version, plural)
                if resource["metadata"]["namespace"] == namespace
            ]
        }

    def _matching(self, group: str, version: str, plural: str) -> list[dict[str, Any]]:
        kinds = {
            "dynamographdeployments": "DynamoGraphDeployment",
            "dynamographdeploymentrequests": "DynamoGraphDeploymentRequest",
            "rayservices": "RayService",
        }
        return [
            resource
            for resource in self.resources
            if resource["apiVersion"] == f"{group}/{version}" and resource["kind"] == kinds[plural]
        ]


def test_discovers_workloads_and_correlates_dgdr_intent_cluster_wide() -> None:
    api = FakeCustomObjectsApi(
        [
            _dgd("inference", "qwen", dgdr_name="qwen-request"),
            _dgd("research", "llama"),
            _dgdr("inference", "qwen-request"),
            _rayservice("serving", "mistral"),
        ]
    )
    discovery = KubernetesWorkloadDiscovery(
        FakeApiextensionsApi(
            [
                _crd("example.com", "Unrelated", "unrelateds", "v1"),
                _crd(
                    "nvidia.com",
                    "DynamoGraphDeployment",
                    "dynamographdeployments",
                    "v1beta1",
                ),
                _crd(
                    "nvidia.com",
                    "DynamoGraphDeploymentRequest",
                    "dynamographdeploymentrequests",
                    "v1beta1",
                ),
                _crd("ray.io", "RayService", "rayservices", "v1"),
            ]
        ),
        api,
    )

    workloads = discovery.discover()

    assert set(workloads) == {
        "dynamo:inference/qwen",
        "dynamo:research/llama",
        "ray:serving/mistral",
    }
    assert api.cluster_requests == [
        ("nvidia.com", "v1beta1", "dynamographdeploymentrequests"),
        ("nvidia.com", "v1beta1", "dynamographdeployments"),
        ("ray.io", "v1", "rayservices"),
    ]
    qwen = workloads["dynamo:inference/qwen"]
    assert qwen.declared_intent is not None
    assert qwen.declared_intent.source_name == "qwen-request"
    assert qwen.declared_intent.workload["concurrency"] == 100
    assert qwen.declared_intent.slo["ttft_ms"] == 200
    assert workloads["dynamo:research/llama"].declared_intent is None


def test_limits_custom_resource_lists_to_configured_namespaces() -> None:
    api = FakeCustomObjectsApi([_dgd("inference", "qwen"), _dgd("research", "llama")])
    discovery = KubernetesWorkloadDiscovery(
        FakeApiextensionsApi(
            [
                _crd(
                    "nvidia.com",
                    "DynamoGraphDeployment",
                    "dynamographdeployments",
                    "v1beta1",
                )
            ]
        ),
        api,
        namespaces=["inference"],
    )

    workloads = discovery.discover()

    assert set(workloads) == {"dynamo:inference/qwen"}
    assert api.cluster_requests == []
    assert api.namespace_requests == [
        ("nvidia.com", "v1beta1", "inference", "dynamographdeployments")
    ]


def test_reports_crd_rbac_failure() -> None:
    discovery = KubernetesWorkloadDiscovery(
        FakeApiextensionsApi([], error=PermissionError("403 Forbidden")),
        FakeCustomObjectsApi([]),
    )

    with pytest.raises(WorkloadDiscoveryError, match="Unable to list Kubernetes CRDs"):
        discovery.discover()


def test_caches_supported_crds_between_workload_reconciliations() -> None:
    extensions = FakeApiextensionsApi(
        [
            _crd(
                "nvidia.com",
                "DynamoGraphDeployment",
                "dynamographdeployments",
                "v1beta1",
            )
        ]
    )
    discovery = KubernetesWorkloadDiscovery(
        extensions,
        FakeCustomObjectsApi([_dgd("inference", "qwen")]),
    )

    discovery.discover()
    discovery.discover()

    assert extensions.calls == 1


def test_available_resource_map_reports_installed_supported_crds() -> None:
    discovery = KubernetesWorkloadDiscovery(
        FakeApiextensionsApi(
            [
                _crd(
                    "nvidia.com",
                    "DynamoGraphDeployment",
                    "dynamographdeployments",
                    "v1beta1",
                ),
                _crd("example.com", "Unrelated", "unrelateds", "v1"),
            ]
        ),
        FakeCustomObjectsApi([]),
    )

    resource_map = discovery.available_resource_map()

    assert resource_map == {
        "dynamographdeployments.nvidia.com": {
            "group": "nvidia.com",
            "kind": "DynamoGraphDeployment",
            "plural": "dynamographdeployments",
            "served_versions": ["v1beta1"],
            "selected_version": "v1beta1",
        }
    }


def _crd(group: str, kind: str, plural: str, version: str) -> Any:
    return SimpleNamespace(
        spec=SimpleNamespace(
            group=group,
            names=SimpleNamespace(kind=kind, plural=plural),
            versions=[SimpleNamespace(name=version, served=True)],
        )
    )


def _dgd(namespace: str, name: str, dgdr_name: str | None = None) -> dict[str, Any]:
    labels = {}
    if dgdr_name is not None:
        labels = {
            "dgdr.nvidia.com/name": dgdr_name,
            "dgdr.nvidia.com/namespace": namespace,
        }
    return {
        "apiVersion": "nvidia.com/v1beta1",
        "kind": "DynamoGraphDeployment",
        "metadata": {
            "namespace": namespace,
            "name": name,
            "uid": f"{name}-uid",
            "labels": labels,
        },
        "spec": {
            "backendFramework": "vllm",
            "components": [
                {
                    "name": "Worker",
                    "componentType": "worker",
                    "modelRef": {"name": f"models/{name}"},
                    "extraPodSpec": {
                        "mainContainer": {
                            "image": "dynamo-vllm:test",
                            "resources": {"limits": {"nvidia.com/gpu": "1"}},
                        }
                    },
                }
            ],
        },
    }


def _dgdr(namespace: str, name: str) -> dict[str, Any]:
    return {
        "apiVersion": "nvidia.com/v1beta1",
        "kind": "DynamoGraphDeploymentRequest",
        "metadata": {"namespace": namespace, "name": name, "uid": f"{name}-uid"},
        "spec": {
            "model": "models/qwen",
            "backend": "vllm",
            "workload": {"concurrency": 100, "requestRate": 25},
            "sla": {"ttft": 200, "itl": 20},
            "hardware": {"gpuSku": "h200_sxm", "totalGpus": 8},
        },
    }


def _rayservice(namespace: str, name: str) -> dict[str, Any]:
    return {
        "apiVersion": "ray.io/v1",
        "kind": "RayService",
        "metadata": {"namespace": namespace, "name": name, "uid": f"{name}-uid"},
        "spec": {
            "serveConfigV2": f"""
applications:
  - name: {name}
    args:
      llm_configs:
        - model_loading_config:
            model_id: models/{name}
          engine_kwargs:
            tensor_parallel_size: 1
"""
        },
    }
