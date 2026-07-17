"""Job-scoped rolling time-series telemetry."""

from __future__ import annotations

import hashlib
import json
from collections import deque
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

DEFAULT_SAMPLE_INTERVAL_SECONDS = 10


@dataclass(frozen=True)
class MetricScope:
    """Workload, worker, and device ownership for one metric series."""

    workload_id: str | None
    pod_uid: str | None
    container_name: str | None
    node_name: str | None
    gpu_uuid: str | None = None
    gpu_index: str | None = None
    local_rank: str | None = None
    gpu_instance_id: str | None = None
    runtime_instance: str | None = None
    runtime_role: str | None = None
    pod_namespace: str | None = None
    pod_name: str | None = None
    attribution_method: str | None = None


@dataclass(frozen=True)
class MetricSample:
    """One timestamped value in a metric series."""

    timestamp: datetime
    value: float


@dataclass
class MetricSeries:
    """One append-only time series for a job, worker, and metric."""

    series_id: str
    metric_name: str
    scope: MetricScope
    labels: dict[str, str] = field(default_factory=dict)
    samples: deque[MetricSample] = field(default_factory=deque)

    @classmethod
    def create(
        cls,
        metric_name: str,
        scope: MetricScope,
        labels: dict[str, str] | None = None,
    ) -> MetricSeries:
        """Create a stable series identity from its metric dimensions."""
        normalized_labels = dict(sorted((labels or {}).items()))
        identity: dict[str, Any] = {
            "metric_name": metric_name,
            "scope": asdict(scope),
            "labels": normalized_labels,
        }
        encoded = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
        series_id = hashlib.sha256(encoded).hexdigest()
        return cls(
            series_id=series_id,
            metric_name=metric_name,
            scope=scope,
            labels=normalized_labels,
        )

    def append(self, samples: Iterable[MetricSample]) -> None:
        """Append samples newer than the latest stored timestamp."""
        latest = self.samples[-1].timestamp if self.samples else None
        for sample in samples:
            if latest is not None and sample.timestamp <= latest:
                continue
            self.samples.append(sample)
            latest = sample.timestamp

    def prune(self, before: datetime) -> None:
        """Remove samples older than the rolling retention boundary."""
        while self.samples and self.samples[0].timestamp < before:
            self.samples.popleft()


@dataclass
class WorkloadTelemetry:
    """Rolling metric series owned by one normalized workload."""

    workload_id: str | None
    series: dict[str, MetricSeries] = field(default_factory=dict)
    last_sample_at: datetime | None = None

    def merge(self, collected: WorkloadTelemetry) -> None:
        """Append a collected batch without rebuilding existing series."""
        for series_id, incoming in collected.series.items():
            existing = self.series.get(series_id)
            if existing is None:
                existing = MetricSeries(
                    series_id=incoming.series_id,
                    metric_name=incoming.metric_name,
                    scope=incoming.scope,
                    labels=dict(incoming.labels),
                )
                self.series[series_id] = existing
            existing.append(incoming.samples)
            if existing.samples:
                timestamp = existing.samples[-1].timestamp
                if self.last_sample_at is None or timestamp > self.last_sample_at:
                    self.last_sample_at = timestamp

    def prune(self, before: datetime) -> None:
        """Prune expired samples without removing the job or series identity."""
        for series in self.series.values():
            series.prune(before)


@dataclass
class ClusterTelemetry:
    """One collected batch grouped by job, plus unattributed metrics."""

    jobs: dict[str, WorkloadTelemetry] = field(default_factory=dict)
    unattributed: WorkloadTelemetry = field(
        default_factory=lambda: WorkloadTelemetry(workload_id=None)
    )
    missing_metrics: list[str] = field(default_factory=list)

    def for_workload(self, workload_id: str) -> WorkloadTelemetry:
        """Return the collected telemetry for one workload."""
        return self.jobs.get(workload_id, WorkloadTelemetry(workload_id=workload_id))
