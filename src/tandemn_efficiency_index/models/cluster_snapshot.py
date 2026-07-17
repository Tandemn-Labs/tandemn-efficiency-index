"""Long-lived cluster observation record grouped by job."""

from dataclasses import dataclass, field
from datetime import datetime

from tandemn_efficiency_index.models.telemetry import (
    DEFAULT_SAMPLE_INTERVAL_SECONDS,
    WorkloadTelemetry,
)
from tandemn_efficiency_index.models.workload import Workload


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


@dataclass
class JobRecord:
    """Configuration, workers, and rolling telemetry for one job."""

    workload: Workload
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
    """Overall rolling observation record with telemetry scoped per job."""

    started_at: datetime
    updated_at: datetime
    window_start: datetime
    sample_interval_seconds: int = DEFAULT_SAMPLE_INTERVAL_SECONDS
    jobs: dict[str, JobRecord] = field(default_factory=dict)
    unattributed_telemetry: WorkloadTelemetry = field(
        default_factory=lambda: WorkloadTelemetry(workload_id=None)
    )
    missing_metrics: list[str] = field(default_factory=list)
