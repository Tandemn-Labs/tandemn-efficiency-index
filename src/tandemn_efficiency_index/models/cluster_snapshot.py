"""Long-lived cluster observation record grouped by job."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

from tandemn_efficiency_index.models.telemetry import (
    DEFAULT_SAMPLE_INTERVAL_SECONDS,
    WorkloadTelemetry,
)
from tandemn_efficiency_index.models.workload import Workload

if TYPE_CHECKING:
    from tandemn_efficiency_index.models.observation import RuntimeJobKey, WorkloadRevision


@dataclass
class WorkloadPod:
    """One observed Kubernetes pod attributed to a normalized workload."""

    workload_id: str
    namespace: str
    name: str
    uid: str
    node_name: str | None
    container_names: list[str]
    runtime_instance: str
    runtime_state: str
    runtime_role: str | None
    first_seen_at: datetime
    last_seen_at: datetime
    runtime_job_key: str | None = None
    phase: str | None = None
    ready: bool | None = None
    restart_count: int = 0
    resource_requests: dict[str, Any] = field(default_factory=dict)
    resource_limits: dict[str, Any] = field(default_factory=dict)


@dataclass
class JobRecord:
    """Configuration, workers, and rolling telemetry for one job."""

    workload: Workload
    active: bool = True
    removed_at: datetime | None = None
    workers: dict[str, WorkloadPod] = field(default_factory=dict)
    telemetry: WorkloadTelemetry = field(init=False)

    def __post_init__(self) -> None:
        self.telemetry = WorkloadTelemetry(workload_id=self.workload.workload_id)

    @property
    def workload_id(self) -> str:
        """Return the canonical job identity."""
        return self.workload.workload_id


@dataclass
class ClusterRecord:
    """Generated report joining observation state to Prometheus time series."""

    started_at: datetime
    updated_at: datetime
    window_start: datetime
    observation_id: str | None = None
    observation_ends_at: datetime | None = None
    sample_interval_seconds: int = DEFAULT_SAMPLE_INTERVAL_SECONDS
    jobs: dict[str, JobRecord] = field(default_factory=dict)
    runtime_job_keys: list[RuntimeJobKey] = field(default_factory=list)
    workload_revisions: dict[str, list[WorkloadRevision]] = field(default_factory=dict)
    unattributed_telemetry: WorkloadTelemetry = field(
        default_factory=lambda: WorkloadTelemetry(workload_id=None)
    )
    missing_metrics: list[str] = field(default_factory=list)
