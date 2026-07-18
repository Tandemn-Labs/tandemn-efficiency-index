"""Persisted Kubernetes observation state without Prometheus samples."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from tandemn_efficiency_index.models.cluster_snapshot import WorkloadPod
from tandemn_efficiency_index.models.telemetry import DEFAULT_SAMPLE_INTERVAL_SECONDS
from tandemn_efficiency_index.models.workload import Workload, WorkloadRuntime


@dataclass(frozen=True)
class Observation:
    """One bounded interval used to correlate state and Prometheus time series."""

    observation_id: str
    started_at: datetime
    ends_at: datetime


@dataclass
class ObservedWorkload:
    """One workload and its Kubernetes worker ownership during an observation."""

    workload: Workload
    active: bool = True
    removed_at: datetime | None = None
    workers: dict[str, WorkloadPod] = field(default_factory=dict)

    @property
    def workload_id(self) -> str:
        """Return the canonical workload identity."""
        return self.workload.workload_id


@dataclass(frozen=True)
class RuntimeJobKey:
    """A runtime-generated key that joins a workload to its worker Pods."""

    runtime: WorkloadRuntime
    namespace: str
    key: str
    workload_id: str
    runtime_state: str
    valid_from: datetime
    valid_to: datetime | None


@dataclass(frozen=True)
class WorkloadRevision:
    """One normalized workload configuration active during a bounded interval."""

    revision_id: int
    workload_id: str
    valid_from: datetime
    valid_to: datetime | None
    configuration: dict[str, Any]


@dataclass
class ObservationState:
    """Kubernetes state persisted independently from generated metric reports."""

    observation: Observation
    updated_at: datetime
    state_interval_seconds: int = DEFAULT_SAMPLE_INTERVAL_SECONDS
    jobs: dict[str, ObservedWorkload] = field(default_factory=dict)
    runtime_job_keys: list[RuntimeJobKey] = field(default_factory=list)
    workload_revisions: dict[str, list[WorkloadRevision]] = field(default_factory=dict)
    missing_state: list[str] = field(default_factory=list)

    @property
    def observation_id(self) -> str:
        """Return the current observation identity."""
        return self.observation.observation_id

    @property
    def started_at(self) -> datetime:
        """Return when the current observation started."""
        return self.observation.started_at

    @property
    def ends_at(self) -> datetime:
        """Return when the current observation is scheduled to end."""
        return self.observation.ends_at
