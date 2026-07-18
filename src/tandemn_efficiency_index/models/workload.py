"""Canonical workload configuration shared by all runtime detectors."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class WorkloadRuntime(StrEnum):
    """Runtime that owns the Kubernetes workload definition."""

    DYNAMO = "dynamo"
    RAY = "ray"


@dataclass
class WorkloadPodSelector:
    """Kubernetes labels that identify pods owned by one runtime instance."""

    runtime_instance: str
    runtime_state: str
    match_labels: dict[str, str]
    role_label: str | None = None

    @property
    def runtime_job_key(self) -> str:
        """Return the job key emitted by the owning runtime on worker Pods."""
        return self.runtime_instance


@dataclass
class WorkloadComponent:
    """One independently configured component of a model workload."""

    name: str
    component_type: str
    replicas: int
    image: str | None
    gpus_per_replica: int | float
    total_gpus: int | float
    placement: dict[str, Any]
    x: dict[str, Any]


@dataclass
class WorkloadIntent:
    """User-declared workload objectives associated with a deployed workload."""

    source_kind: str
    source_namespace: str
    source_name: str
    source_uid: str | None
    api_version: str
    model_id: str | None = None
    backend: str | None = None
    workload: dict[str, Any] = field(default_factory=dict)
    slo: dict[str, Any] = field(default_factory=dict)
    hardware: dict[str, Any] = field(default_factory=dict)
    components: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class Workload:
    """Normalized configuration for one deployed model workload."""

    runtime: WorkloadRuntime
    namespace: str
    name: str
    uid: str | None
    api_version: str
    model_id: str
    backend: str
    disaggregated: bool
    total_gpus: int | float
    components: list[WorkloadComponent]
    pod_selectors: list[WorkloadPodSelector] = field(default_factory=list)
    declared_intent: WorkloadIntent | None = None
    source_generation: int | None = None
    source_resource_version: str | None = None

    @property
    def workload_id(self) -> str:
        """Return an identity that cannot collide across runtimes."""
        return f"{self.runtime}:{self.namespace}/{self.name}"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable workload record."""
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Workload:
        """Restore a normalized workload from its persisted representation."""
        intent_value = value.get("declared_intent")
        intent = WorkloadIntent(**intent_value) if isinstance(intent_value, dict) else None
        return cls(
            runtime=WorkloadRuntime(value["runtime"]),
            namespace=str(value["namespace"]),
            name=str(value["name"]),
            uid=str(value["uid"]) if value.get("uid") is not None else None,
            api_version=str(value["api_version"]),
            model_id=str(value["model_id"]),
            backend=str(value["backend"]),
            disaggregated=bool(value["disaggregated"]),
            total_gpus=value["total_gpus"],
            components=[WorkloadComponent(**component) for component in value["components"]],
            pod_selectors=[
                WorkloadPodSelector(**selector) for selector in value.get("pod_selectors", [])
            ],
            declared_intent=intent,
            source_generation=(
                int(value["source_generation"])
                if value.get("source_generation") is not None
                else None
            ),
            source_resource_version=(
                str(value["source_resource_version"])
                if value.get("source_resource_version") is not None
                else None
            ),
        )
