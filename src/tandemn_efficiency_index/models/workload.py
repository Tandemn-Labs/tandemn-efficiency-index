"""Canonical workload configuration shared by all runtime detectors."""

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

    @property
    def workload_id(self) -> str:
        """Return an identity that cannot collide across runtimes."""
        return f"{self.runtime}:{self.namespace}/{self.name}"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable workload record."""
        return asdict(self)
