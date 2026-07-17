"""Run the TEI dashboard against the isolated smoke-test fixture."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from smoketest.mock_data import build_mock_cluster_record
from tandemn_efficiency_index.models.cluster_snapshot import ClusterRecord
from tandemn_efficiency_index.observability import ObservabilityServer

LOGGER = logging.getLogger(__name__)


class MockObserver:
    """Expose a mock cluster record through the production dashboard server."""

    def __init__(self) -> None:
        self._record = build_mock_cluster_record(datetime.now(UTC))

    @property
    def record(self) -> ClusterRecord:
        """Return the mock record used by the snapshot endpoint."""
        return self._record

    def collect_tick(self, collected_at: datetime | None = None) -> ClusterRecord:
        """Regenerate the deterministic history at the current collection time."""
        self._record = build_mock_cluster_record(collected_at or datetime.now(UTC))
        return self._record


def main() -> None:
    """Serve the production dashboard with the three-job mock record."""
    logging.basicConfig(level=logging.INFO)
    server = ObservabilityServer(MockObserver(), host="127.0.0.1", port=8000)
    LOGGER.info("Mock TEI dashboard available at http://127.0.0.1:8000")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LOGGER.info("Mock TEI dashboard stopped")


if __name__ == "__main__":
    main()
