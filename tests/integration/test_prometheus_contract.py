"""Validate generated PromQL against a real local Prometheus process."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest

from tandemn_efficiency_index.models.cluster_snapshot import WorkloadPod
from tandemn_efficiency_index.prometheus.client import PrometheusClient
from tandemn_efficiency_index.prometheus.generic import (
    NORMALIZED_INFERENCE_METRICS,
    PrometheusMetricsCollector,
)

PROMETHEUS_URL = os.environ.get("TEI_TEST_PROMETHEUS_URL")
pytestmark = pytest.mark.skipif(
    not PROMETHEUS_URL,
    reason="TEI_TEST_PROMETHEUS_URL is required for the Prometheus contract test",
)


def test_generated_queries_are_accepted_by_prometheus() -> None:
    now = datetime.now(UTC)
    pod = WorkloadPod(
        workload_id="ray:inference/qwen",
        namespace="inference-test",
        name="qwen-worker-0",
        uid="pod-uid",
        node_name="gpu-node-1",
        container_names=["ray-worker"],
        runtime_instance="qwen-cluster",
        runtime_state="active",
        runtime_role="gpu-workers",
        first_seen_at=now,
        last_seen_at=now,
    )
    client = PrometheusClient(str(PROMETHEUS_URL))
    collector = PrometheusMetricsCollector(client, step_seconds=10)

    collector.check_ready(now)
    telemetry = collector.collect(
        now - timedelta(minutes=5),
        now,
        {pod.uid: pod},
    )

    assert set(NORMALIZED_INFERENCE_METRICS).issubset(telemetry.missing_metrics)
