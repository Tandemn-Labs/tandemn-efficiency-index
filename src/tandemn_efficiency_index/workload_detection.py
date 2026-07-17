"""Dispatch workload discovery to the configured Kubernetes runtime."""

from __future__ import annotations

import logging
from collections.abc import Collection
from dataclasses import dataclass

from tandemn_efficiency_index.dynamo.workload_detection import (
    CustomObjectsApi,
    DynamoWorkloadDetector,
    DynamoWorkloadTarget,
)
from tandemn_efficiency_index.models.workload import Workload, WorkloadRuntime
from tandemn_efficiency_index.ray.workload_detection import (
    RayWorkloadDetector,
    RayWorkloadTarget,
)

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkloadTarget:
    """User-provided runtime and Kubernetes identity for one workload."""

    runtime: WorkloadRuntime
    namespace: str
    name: str

    def __post_init__(self) -> None:
        if not self.namespace.strip() or not self.name.strip():
            raise ValueError("Workload namespace and name are required")


class ClusterWorkloadDetector:
    """Collect normalized workloads from all configured runtimes."""

    def __init__(self, custom_objects_api: CustomObjectsApi) -> None:
        self._dynamo = DynamoWorkloadDetector(custom_objects_api)
        self._ray = RayWorkloadDetector(custom_objects_api)

    @classmethod
    def from_in_cluster(cls) -> ClusterWorkloadDetector:
        """Create a detector using the TEI Pod service account."""
        from kubernetes import client, config

        config.load_incluster_config()
        return cls(client.CustomObjectsApi())

    def detect(self, targets: Collection[WorkloadTarget]) -> dict[str, Workload]:
        """Dispatch targets by runtime and merge their normalized workloads."""
        if not targets:
            raise ValueError("At least one workload target is required")

        dynamo_targets: list[DynamoWorkloadTarget] = []
        ray_targets: list[RayWorkloadTarget] = []
        for target in targets:
            if target.runtime is WorkloadRuntime.DYNAMO:
                dynamo_targets.append(DynamoWorkloadTarget(target.namespace, target.name))
            elif target.runtime is WorkloadRuntime.RAY:
                ray_targets.append(RayWorkloadTarget(target.namespace, target.name))
            else:
                raise ValueError(f"Unsupported workload runtime: {target.runtime}")

        workloads: dict[str, Workload] = {}
        if dynamo_targets:
            workloads.update(self._dynamo.detect(dynamo_targets))
        if ray_targets:
            workloads.update(self._ray.detect(ray_targets))

        LOGGER.info("Loaded %d workloads across configured runtimes", len(workloads))
        return workloads
