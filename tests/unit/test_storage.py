import os
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from tandemn_efficiency_index.models.cluster_snapshot import WorkloadPod
from tandemn_efficiency_index.models.observation import ObservedWorkload
from tandemn_efficiency_index.models.workload import (
    Workload,
    WorkloadPodSelector,
    WorkloadRuntime,
)
from tandemn_efficiency_index.storage import PostgresObservationStore

POSTGRES_DSN = os.environ.get("TEI_TEST_POSTGRES_DSN")
pytestmark = pytest.mark.skipif(
    POSTGRES_DSN is None,
    reason="TEI_TEST_POSTGRES_DSN is required for PostgreSQL storage tests",
)


@pytest.fixture(autouse=True)
def clean_database() -> None:
    if POSTGRES_DSN is None:
        return
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as connection:
        connection.execute("DROP SCHEMA public CASCADE")
        connection.execute("CREATE SCHEMA public")


def test_store_restores_workload_revisions_job_keys_and_pods() -> None:
    assert POSTGRES_DSN is not None
    now = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
    store = PostgresObservationStore(POSTGRES_DSN, timedelta(hours=24))
    state = store.load_state(10, now)
    workload = _workload(total_gpus=1)
    state.jobs[workload.workload_id] = _job(workload, now)

    store.save_state(state, now, now + timedelta(seconds=1), "ok")
    state.jobs[workload.workload_id].workload = _workload(total_gpus=2)
    store.save_state(
        state,
        now + timedelta(seconds=10),
        now + timedelta(seconds=11),
        "ok",
    )
    store.close()

    with psycopg.connect(POSTGRES_DSN) as connection:
        assert _scalar(connection, "SELECT COUNT(*) FROM workload_revisions") == 2
        assert _scalar(connection, "SELECT COUNT(*) FROM runtime_job_keys") == 1
        assert _scalar(connection, "SELECT COUNT(*) FROM pod_assignments") == 1
        assert _scalar(connection, "SELECT COUNT(*) FROM state_collection_runs") == 2
        metric_table = _scalar(connection, "SELECT to_regclass('public.metric_samples')")
        assert metric_table is None

    restored_store = PostgresObservationStore(POSTGRES_DSN, timedelta(hours=24))
    restored = restored_store.load_state(10, now + timedelta(seconds=20))
    job = restored.jobs[workload.workload_id]
    assert restored.observation_id == state.observation_id
    assert job.workload.total_gpus == 2
    assert job.workers["pod-uid"].node_name == "gpu-node-1"
    assert restored.runtime_job_keys[0].key == "qwen"
    revisions = restored.workload_revisions[workload.workload_id]
    assert len(revisions) == 2
    assert revisions[0].valid_to is not None
    assert revisions[1].configuration["total_gpus"] == 2
    restored_store.close()


def test_store_rotates_bounded_observations() -> None:
    assert POSTGRES_DSN is not None
    now = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
    store = PostgresObservationStore(POSTGRES_DSN, timedelta(hours=1))
    first = store.load_state(10, now)
    workload = _workload(total_gpus=1)
    first.jobs[workload.workload_id] = _job(workload, now)
    store.save_state(first, now, now, "ok")

    second_observation = store.ensure_observation(now + timedelta(hours=1, seconds=1))
    second = store.load_state(10, now + timedelta(hours=1, seconds=1))

    assert second_observation.observation_id != first.observation_id
    assert second.observation_id == second_observation.observation_id
    assert second.jobs == {}
    with psycopg.connect(POSTGRES_DSN) as connection:
        statuses = connection.execute(
            "SELECT status FROM observations ORDER BY started_at"
        ).fetchall()
    assert statuses == [("completed",), ("active",)]
    store.close()


def _workload(total_gpus: int) -> Workload:
    return Workload(
        runtime=WorkloadRuntime.DYNAMO,
        namespace="inference",
        name="qwen",
        uid="dgd-uid",
        api_version="nvidia.com/v1beta1",
        model_id="Qwen/Qwen3-32B",
        backend="vllm",
        disaggregated=False,
        total_gpus=total_gpus,
        components=[],
        pod_selectors=[
            WorkloadPodSelector(
                runtime_instance="qwen",
                runtime_state="active",
                match_labels={"nvidia.com/dynamo-graph-deployment-name": "qwen"},
            )
        ],
    )


def _job(workload: Workload, observed_at: datetime) -> ObservedWorkload:
    job = ObservedWorkload(workload=workload)
    job.workers["pod-uid"] = WorkloadPod(
        workload_id=workload.workload_id,
        namespace=workload.namespace,
        name="qwen-worker",
        uid="pod-uid",
        node_name="gpu-node-1",
        container_names=["main"],
        runtime_instance=workload.name,
        runtime_state="active",
        runtime_role="decode",
        first_seen_at=observed_at,
        last_seen_at=observed_at,
        runtime_job_key="qwen",
    )
    return job


def _scalar(connection: psycopg.Connection[tuple[object, ...]], query: str) -> object:
    row = connection.execute(query).fetchone()
    assert row is not None
    return row[0]
