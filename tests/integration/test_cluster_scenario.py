"""End-to-end tests for a deterministic three-workload Kubernetes cluster."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from tandemn_efficiency_index.kubernetes_discovery import (
    CustomObjectsApi,
    KubernetesWorkloadDiscovery,
)
from tandemn_efficiency_index.observer import ClusterObserver
from tandemn_efficiency_index.pod_attribution import WorkloadPodCollector
from tandemn_efficiency_index.prometheus.client import PrometheusSample, PrometheusSeries
from tandemn_efficiency_index.prometheus.generic import PrometheusMetricsCollector


class FakeCrdApi:
    def __init__(self) -> None:
        self.calls = 0

    def list_custom_resource_definition(self) -> Any:
        self.calls += 1
        return SimpleNamespace(
            items=[
                _crd("nvidia.com", "DynamoGraphDeployment", "dynamographdeployments", "v1beta1"),
                _crd("ray.io", "RayService", "rayservices", "v1"),
            ]
        )


class FakeCustomObjectsApi(CustomObjectsApi):
    def __init__(self, resources: list[dict[str, Any]]) -> None:
        self.resources = resources

    def list_cluster_custom_object(
        self,
        group: str,
        version: str,
        plural: str,
    ) -> dict[str, Any]:
        kinds = {
            "dynamographdeployments": "DynamoGraphDeployment",
            "rayservices": "RayService",
        }
        kind = kinds[plural]
        return {
            "items": [
                resource
                for resource in self.resources
                if resource["apiVersion"] == f"{group}/{version}" and resource["kind"] == kind
            ]
        }

    def get_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
    ) -> dict[str, Any]:
        raise NotImplementedError

    def list_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
    ) -> dict[str, Any]:
        response = self.list_cluster_custom_object(group, version, plural)
        return {
            "items": [
                resource
                for resource in response["items"]
                if resource["metadata"]["namespace"] == namespace
            ]
        }


class FakeCoreApi:
    def __init__(self, pods: list[Any]) -> None:
        self.pods = pods

    def list_namespaced_pod(self, namespace: str) -> Any:
        return SimpleNamespace(
            items=[pod for pod in self.pods if pod.metadata.namespace == namespace]
        )


class FakePrometheusApi:
    def __init__(self, series: list[PrometheusSeries]) -> None:
        self.series = series
        self.queries: list[tuple[str, int]] = []

    def query_range(
        self,
        query: str,
        start: datetime,
        end: datetime,
        step_seconds: int,
    ) -> list[PrometheusSeries]:
        self.queries.append((query, step_seconds))
        if "DCGM_FI_.*" in query:
            return self.series
        return []


def test_realistic_cluster_discovers_attributes_and_reports_three_workloads() -> None:
    resources = _resources()
    discovery = KubernetesWorkloadDiscovery(
        FakeCrdApi(),
        FakeCustomObjectsApi(resources),
    )
    workloads = discovery.discover()
    pods = _pods(workloads)
    prometheus = FakePrometheusApi(_prometheus_series(pods))
    observer = ClusterObserver(
        workloads={},
        pod_collector=WorkloadPodCollector(FakeCoreApi(pods)),
        prometheus_collector=PrometheusMetricsCollector(prometheus, step_seconds=10),
        started_at=TIMESTAMP,
        workload_discovery=discovery,
    )

    observer.collect_tick(TIMESTAMP)
    report = observer.live_record(60, TIMESTAMP)

    assert set(report.jobs) == {
        "dynamo:inference/qwen-vllm",
        "dynamo:serving/llama-sglang",
        "ray:research/mixtral-sglang",
    }
    assert sum(len(job.workers) for job in report.jobs.values()) == 3
    assert prometheus.queries[0][1] == 10
    assert all(job.telemetry.series for job in report.jobs.values())

    qwen = report.jobs["dynamo:inference/qwen-vllm"]
    qwen_dcgm = next(
        series
        for series in qwen.telemetry.series.values()
        if series.metric_name == "DCGM_FI_DEV_GPU_UTIL"
    )
    assert qwen_dcgm.scope.gpu_uuid == "GPU-qwen-vllm-0"
    assert qwen_dcgm.scope.runtime_job_key == "qwen-vllm"
    assert qwen_dcgm.scope.attribution_method == "exported_namespace_pod"

    sglang_metrics = {
        series.metric_name
        for job in report.jobs.values()
        for series in job.telemetry.series.values()
        if series.metric_name.startswith("sglang:")
    }
    assert sglang_metrics == {
        "sglang:request_latency_seconds",
        "sglang:tokens_total",
    }
    assert len(report.unattributed_telemetry.series) == 1
    orphan = next(iter(report.unattributed_telemetry.series.values()))
    assert orphan.scope.attribution_method == "unattributed_pod_not_found"


def test_cluster_retains_pod_history_after_worker_disappears() -> None:
    discovery = KubernetesWorkloadDiscovery(FakeCrdApi(), FakeCustomObjectsApi(_resources()))
    workloads = discovery.discover()
    pods = _pods(workloads)
    prometheus = FakePrometheusApi(_prometheus_series(pods))
    core_api = FakeCoreApi(pods)
    observer = ClusterObserver(
        workloads=workloads,
        pod_collector=WorkloadPodCollector(core_api),
        prometheus_collector=PrometheusMetricsCollector(prometheus, step_seconds=10),
        started_at=TIMESTAMP,
    )

    observer.collect_tick(TIMESTAMP)
    disappeared = pods.pop()
    observer.collect_tick(TIMESTAMP.replace(second=10))
    report = observer.live_record(30, TIMESTAMP.replace(second=10))

    assert disappeared.metadata.uid in {
        pod.uid for job in report.jobs.values() for pod in job.workers.values()
    }
    assert disappeared.metadata.name not in {
        pod.name
        for job in report.jobs.values()
        for pod in job.workers.values()
        if pod.last_seen_at == TIMESTAMP.replace(second=10)
    }


def test_pod_metadata_and_resources_are_captured_from_kubernetes() -> None:
    discovery = KubernetesWorkloadDiscovery(FakeCrdApi(), FakeCustomObjectsApi(_resources()))
    workloads = discovery.discover()
    pods = _pods(workloads)
    observed = WorkloadPodCollector(FakeCoreApi(pods)).collect(workloads, TIMESTAMP)

    for pod in observed.values():
        assert pod.phase == "Running"
        assert pod.ready is True
        assert pod.restart_count == 1
        assert pod.resource_requests["worker"]["nvidia.com/gpu"] == "1"
        assert pod.resource_limits["worker"]["memory"] == "80Gi"


TIMESTAMP = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)


def _crd(group: str, kind: str, plural: str, version: str) -> Any:
    return SimpleNamespace(
        spec=SimpleNamespace(
            group=group,
            names=SimpleNamespace(kind=kind, plural=plural),
            versions=[SimpleNamespace(name=version, served=True)],
        )
    )


def _resources() -> list[dict[str, Any]]:
    return [
        _dgd("inference", "qwen-vllm", "vllm"),
        _dgd("serving", "llama-sglang", "sglang"),
        {
            "apiVersion": "ray.io/v1",
            "kind": "RayService",
            "metadata": {
                "namespace": "research",
                "name": "mixtral-sglang",
                "uid": "ray-mixtral-sglang",
            },
            "spec": {
                "serveConfigV2": """
applications:
  - name: mixtral
    import_path: ray.serve.llm:build_openai_app
    args:
      llm_configs:
        - model_loading_config:
            model_id: mistralai/Mixtral-8x7B-Instruct-v0.1
          server_cls: sglang:build_app
          engine_kwargs:
            tensor_parallel_size: 1
""",
            },
            "rayClusterConfig": {
                "workerGroupSpecs": [
                    {
                        "groupName": "gpu-workers",
                        "template": {
                            "spec": {"containers": [{"name": "ray-worker", "image": "ray:gpu"}]}
                        },
                    }
                ]
            },
            "status": {"activeServiceStatus": {"rayClusterName": "mixtral-sglang-active"}},
        },
    ]


def _dgd(namespace: str, name: str, backend: str) -> dict[str, Any]:
    return {
        "apiVersion": "nvidia.com/v1beta1",
        "kind": "DynamoGraphDeployment",
        "metadata": {"namespace": namespace, "name": name, "uid": f"{name}-uid"},
        "spec": {
            "backendFramework": backend,
            "components": [
                {
                    "name": "Worker",
                    "componentType": "worker",
                    "modelRef": {"name": f"models/{name}"},
                    "extraPodSpec": {
                        "mainContainer": {
                            "image": f"{backend}:latest",
                            "resources": {
                                "requests": {"nvidia.com/gpu": "1", "memory": "64Gi"},
                                "limits": {"nvidia.com/gpu": "1", "memory": "80Gi"},
                            },
                        }
                    },
                }
            ],
        },
    }


def _pods(workloads: dict[str, Any]) -> list[Any]:
    pods: list[Any] = []
    for index, workload in enumerate(workloads.values()):
        selector = workload.pod_selectors[0]
        labels = dict(selector.match_labels)
        if selector.role_label:
            labels[selector.role_label] = "decode"
        pod_name = f"{workload.name}-worker-0"
        pods.append(
            SimpleNamespace(
                metadata=SimpleNamespace(
                    namespace=workload.namespace,
                    name=pod_name,
                    uid=f"pod-{index}",
                    labels=labels,
                ),
                spec=SimpleNamespace(
                    node_name=f"gpu-node-{index}",
                    containers=[
                        SimpleNamespace(
                            name="worker",
                            resources=SimpleNamespace(
                                requests={"nvidia.com/gpu": "1", "memory": "64Gi"},
                                limits={"nvidia.com/gpu": "1", "memory": "80Gi"},
                            ),
                        )
                    ],
                ),
                status=SimpleNamespace(
                    phase="Running",
                    conditions=[SimpleNamespace(type="Ready", status="True")],
                    container_statuses=[SimpleNamespace(restart_count=1)],
                ),
            )
        )
    return pods


def _prometheus_series(pods: list[Any]) -> list[PrometheusSeries]:
    series: list[PrometheusSeries] = []
    for index, pod in enumerate(pods):
        namespace = pod.metadata.namespace
        pod_name = pod.metadata.name
        series.extend(
            [
                PrometheusSeries(
                    labels={
                        "__name__": "DCGM_FI_DEV_GPU_UTIL",
                        "exported_namespace": namespace,
                        "exported_pod": pod_name,
                        "UUID": f"GPU-{pod.metadata.name.split('-worker')[0]}-0",
                        "gpu": "0",
                        "job": "dcgm-exporter",
                    },
                    samples=[PrometheusSample(TIMESTAMP, 70.0 + index)],
                ),
                PrometheusSeries(
                    labels={
                        "__name__": "sglang:request_latency_seconds",
                        "namespace": namespace,
                        "pod": pod_name,
                        "job": "application",
                    },
                    samples=[PrometheusSample(TIMESTAMP, 0.15 + index / 100)],
                ),
                PrometheusSeries(
                    labels={
                        "__name__": "sglang:tokens_total",
                        "namespace": namespace,
                        "pod": pod_name,
                        "job": "application",
                    },
                    samples=[PrometheusSample(TIMESTAMP, 1000.0 + index)],
                ),
            ]
        )
    series.append(
        PrometheusSeries(
            labels={
                "__name__": "DCGM_FI_DEV_GPU_UTIL",
                "namespace": "serving",
                "pod": "terminated-gpu-worker",
                "UUID": "GPU-orphan-0",
                "gpu": "0",
                "job": "dcgm-exporter",
            },
            samples=[PrometheusSample(TIMESTAMP, 12.0)],
        )
    )
    return series
