"""Discover Kubernetes worker pods and retain their workload ownership."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Protocol

from tandemn_efficiency_index.models.cluster_snapshot import WorkloadPod
from tandemn_efficiency_index.models.workload import Workload, WorkloadPodSelector


class CoreV1Api(Protocol):
    """Kubernetes API operation required for worker pod discovery."""

    def list_namespaced_pod(self, namespace: str) -> Any: ...


class WorkloadPodCollector:
    """Collect worker pods selected by normalized Dynamo and Ray workloads."""

    def __init__(self, core_api: CoreV1Api) -> None:
        self._api = core_api

    @classmethod
    def from_in_cluster(cls) -> WorkloadPodCollector:
        """Create a collector using the TEI Pod service account."""
        from kubernetes import client, config

        config.load_incluster_config()
        return cls(client.CoreV1Api())

    def collect(
        self,
        workloads: Mapping[str, Workload],
        observed_at: datetime,
        known_pods: Mapping[str, WorkloadPod] | None = None,
    ) -> dict[str, WorkloadPod]:
        """Return currently observed worker Pods keyed by Pod UID."""
        known = known_pods or {}
        pods: dict[str, WorkloadPod] = {}
        workloads_by_namespace: dict[str, list[tuple[Workload, WorkloadPodSelector]]] = {}
        for workload in workloads.values():
            for selector in workload.pod_selectors:
                workloads_by_namespace.setdefault(workload.namespace, []).append(
                    (workload, selector)
                )
        for namespace, candidates in workloads_by_namespace.items():
            response = self._api.list_namespaced_pod(namespace=namespace)
            for pod in response.items:
                labels = pod.metadata.labels or {}
                for workload, selector in candidates:
                    if not _matches_selector(labels, selector):
                        continue
                    observed = _workload_pod(workload, selector, pod, observed_at, known)
                    existing = pods.get(observed.uid) or known.get(observed.uid)
                    if existing and existing.workload_id != observed.workload_id:
                        raise ValueError(
                            f"Pod {observed.namespace}/{observed.name} matches multiple workloads"
                        )
                    pods[observed.uid] = observed

        return pods


def _matches_selector(labels: Mapping[str, str], selector: WorkloadPodSelector) -> bool:
    return all(labels.get(name) == value for name, value in selector.match_labels.items())


def _workload_pod(
    workload: Workload,
    selector: WorkloadPodSelector,
    pod: Any,
    observed_at: datetime,
    known_pods: Mapping[str, WorkloadPod],
) -> WorkloadPod:
    metadata = pod.metadata
    pod_uid = str(metadata.uid)
    existing = known_pods.get(pod_uid)
    labels = metadata.labels or {}
    containers = [container.name for container in pod.spec.containers]
    status = getattr(pod, "status", None)
    container_statuses = list(getattr(status, "container_statuses", None) or [])
    ready_condition = next(
        (
            condition
            for condition in list(getattr(status, "conditions", None) or [])
            if getattr(condition, "type", None) == "Ready"
        ),
        None,
    )
    return WorkloadPod(
        workload_id=workload.workload_id,
        namespace=str(metadata.namespace or workload.namespace),
        name=str(metadata.name),
        uid=pod_uid,
        node_name=pod.spec.node_name,
        container_names=containers,
        runtime_instance=selector.runtime_instance,
        runtime_job_key=selector.runtime_job_key,
        runtime_state=selector.runtime_state,
        runtime_role=labels.get(selector.role_label) if selector.role_label else None,
        first_seen_at=existing.first_seen_at if existing else observed_at,
        last_seen_at=observed_at,
        phase=getattr(status, "phase", None),
        ready=(getattr(ready_condition, "status", None) == "True")
        if ready_condition is not None
        else None,
        restart_count=sum(
            int(getattr(item, "restart_count", 0) or 0) for item in container_statuses
        ),
        resource_requests=_container_resources(pod.spec.containers, "requests"),
        resource_limits=_container_resources(pod.spec.containers, "limits"),
    )


def _container_resources(containers: list[Any], field_name: str) -> dict[str, Any]:
    resources: dict[str, Any] = {}
    for container in containers:
        configured = getattr(getattr(container, "resources", None), field_name, None) or {}
        resources[str(container.name)] = {
            str(name): str(value) for name, value in configured.items()
        }
    return resources
