"""Generated observability report types."""

from dataclasses import dataclass

from tandemn_efficiency_index.models.cluster_snapshot import ClusterRecord


@dataclass
class ObservabilityReport(ClusterRecord):
    """A point-in-time join of observation state and Prometheus range results."""
