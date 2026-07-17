"""Discover Kubernetes worker pods and retain their workload ownership."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any, Protocol

from tandemn_efficiency_index.models.cluster_snapshot import WorkloadPod
from tandemn_efficiency_index.models.workload import Workload, WorkloadPodSelector


class CoreV1Api(Protocol):
    """Kubernetes API operation required for worker pod discovery."""

    def list_namespaced_pod(self, namespace: str, label_selector: str) -> Any: ...


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
        for workload in workloads.values():
            for selector in workload.pod_selectors:
                response = self._api.list_namespaced_pod(
                    namespace=workload.namespace,
                    label_selector=_label_selector(selector),
                )
                for pod in response.items:
                    observed = _workload_pod(workload, selector, pod, observed_at, known)
                    existing = pods.get(observed.uid) or known.get(observed.uid)
                    if existing and existing.workload_id != observed.workload_id:
                        raise ValueError(
                            f"Pod {observed.namespace}/{observed.name} matches multiple workloads"
                        )
                    pods[observed.uid] = observed
        return pods


def _label_selector(selector: WorkloadPodSelector) -> str:
    return ",".join(f"{name}={value}" for name, value in sorted(selector.match_labels.items()))


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
    return WorkloadPod(
        workload_id=workload.workload_id,
        namespace=str(metadata.namespace or workload.namespace),
        name=str(metadata.name),
        uid=pod_uid,
        node_name=pod.spec.node_name,
        container_names=containers,
        runtime_instance=selector.runtime_instance,
        runtime_state=selector.runtime_state,
        runtime_role=labels.get(selector.role_label) if selector.role_label else None,
        first_seen_at=existing.first_seen_at if existing else observed_at,
        last_seen_at=observed_at,
    )
