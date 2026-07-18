"""PostgreSQL persistence for Kubernetes observation state."""

from __future__ import annotations

import hashlib
import json
import threading
import uuid
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from tandemn_efficiency_index.models.cluster_snapshot import WorkloadPod
from tandemn_efficiency_index.models.observation import (
    Observation,
    ObservationState,
    ObservedWorkload,
    RuntimeJobKey,
    WorkloadRevision,
)
from tandemn_efficiency_index.models.workload import Workload, WorkloadRuntime

SCHEMA_VERSION = 1
OBSERVATION_LOCK_NAME = "tei-active-observation"


class PostgresObservationStore:
    """Persist workload state while Prometheus remains the metric time-series store."""

    def __init__(
        self,
        dsn: str,
        observation_duration: timedelta,
        history_retention: timedelta | None = None,
        cleanup_interval: timedelta = timedelta(minutes=15),
    ) -> None:
        self._observation_duration = observation_duration
        self._history_retention = history_retention or observation_duration
        self._cleanup_interval = cleanup_interval
        self._last_cleanup_at: datetime | None = None
        self._lock = threading.RLock()
        self._connection: Any = psycopg.connect(
            dsn,
            autocommit=True,
            row_factory=dict_row,
            application_name="tandemn-efficiency-index",
        )
        self._migrate()

    def ensure_observation(self, now: datetime | None = None) -> Observation:
        """Return the active observation, rotating it after its bounded duration."""
        observed_at = now or datetime.now(UTC)
        with self._lock, self._connection.transaction():
            self._connection.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (OBSERVATION_LOCK_NAME,),
            )
            active = self._connection.execute(
                """
                SELECT id, started_at, ends_at
                FROM observations
                WHERE status = 'active'
                ORDER BY started_at DESC
                LIMIT 1
                FOR UPDATE
                """
            ).fetchone()
            if active is not None and observed_at < active["ends_at"]:
                return _observation(active)
            if active is not None:
                self._connection.execute(
                    """
                    UPDATE observations
                    SET status = 'completed', ended_at = ends_at
                    WHERE id = %s
                    """,
                    (active["id"],),
                )
            observation = Observation(
                observation_id=str(uuid.uuid4()),
                started_at=observed_at,
                ends_at=observed_at + self._observation_duration,
            )
            self._connection.execute(
                """
                INSERT INTO observations (id, started_at, ends_at, status)
                VALUES (%s, %s, %s, 'active')
                """,
                (
                    observation.observation_id,
                    observation.started_at,
                    observation.ends_at,
                ),
            )
            return observation

    def load_state(
        self,
        state_interval_seconds: int,
        now: datetime | None = None,
    ) -> ObservationState:
        """Restore the active observation's workload and Pod ownership state."""
        observed_at = now or datetime.now(UTC)
        observation = self.ensure_observation(observed_at)
        with self._lock:
            latest_run = self._connection.execute(
                """
                SELECT completed_at, missing_state
                FROM state_collection_runs
                WHERE observation_id = %s AND status != 'failed'
                ORDER BY completed_at DESC
                LIMIT 1
                """,
                (observation.observation_id,),
            ).fetchone()
            state = ObservationState(
                observation=observation,
                updated_at=latest_run["completed_at"] if latest_run else observed_at,
                state_interval_seconds=state_interval_seconds,
                missing_state=list(latest_run["missing_state"]) if latest_run else [],
            )
            rows = self._connection.execute(
                """
                SELECT workloads.*, revision.workload_json
                FROM workloads
                JOIN LATERAL (
                    SELECT workload_json
                    FROM workload_revisions
                    WHERE observation_id = workloads.observation_id
                      AND workload_id = workloads.workload_id
                    ORDER BY valid_from DESC
                    LIMIT 1
                ) AS revision ON TRUE
                WHERE workloads.observation_id = %s
                """,
                (observation.observation_id,),
            ).fetchall()
            for row in rows:
                workload = Workload.from_dict(dict(row["workload_json"]))
                state.jobs[workload.workload_id] = ObservedWorkload(
                    workload=workload,
                    active=bool(row["active"]),
                    removed_at=row["removed_at"],
                )
            self._restore_pods(state)
            state.runtime_job_keys = self.load_runtime_job_keys(
                observation.observation_id,
                observation.started_at,
                observation.ends_at,
            )
            state.workload_revisions = self.load_revisions(
                observation.observation_id,
                observation.started_at,
                observation.ends_at,
            )
            return state

    def save_state(
        self,
        state: ObservationState,
        started_at: datetime,
        completed_at: datetime,
        status: str,
        error: str | None = None,
    ) -> None:
        """Persist one Kubernetes state reconciliation atomically."""
        with self._lock, self._connection.transaction():
            for job in state.jobs.values():
                self._upsert_workload(state.observation_id, job, completed_at)
                self._sync_runtime_job_keys(state.observation_id, job, completed_at)
                for pod in job.workers.values():
                    self._upsert_pod(state.observation_id, pod)
            self._connection.execute(
                """
                INSERT INTO state_collection_runs (
                    observation_id, started_at, completed_at, status, duration_ms,
                    workload_count, pod_count, missing_state, error
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    state.observation_id,
                    started_at,
                    completed_at,
                    status,
                    max(0, int((completed_at - started_at).total_seconds() * 1000)),
                    sum(job.active for job in state.jobs.values()),
                    sum(len(job.workers) for job in state.jobs.values()),
                    Jsonb(sorted(state.missing_state)),
                    error,
                ),
            )
            if self._cleanup_due(completed_at):
                self._cleanup(completed_at)
                self._last_cleanup_at = completed_at

    def record_failure(
        self,
        observation_id: str,
        started_at: datetime,
        completed_at: datetime,
        error: str,
    ) -> None:
        """Record a failed state reconciliation without changing good state."""
        with self._lock, self._connection.transaction():
            self._connection.execute(
                """
                INSERT INTO state_collection_runs (
                    observation_id, started_at, completed_at, status, duration_ms,
                    workload_count, pod_count, missing_state, error
                ) VALUES (%s, %s, %s, 'failed', %s, 0, 0, %s, %s)
                """,
                (
                    observation_id,
                    started_at,
                    completed_at,
                    max(0, int((completed_at - started_at).total_seconds() * 1000)),
                    Jsonb([]),
                    error,
                ),
            )

    def load_revisions(
        self,
        observation_id: str,
        start: datetime,
        end: datetime,
    ) -> dict[str, list[WorkloadRevision]]:
        """Load workload revisions whose validity overlaps a report window."""
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT id, workload_id, valid_from, valid_to, workload_json
                FROM workload_revisions
                WHERE observation_id = %s
                  AND valid_from <= %s
                  AND (valid_to IS NULL OR valid_to >= %s)
                ORDER BY workload_id, valid_from
                """,
                (observation_id, end, start),
            ).fetchall()
        revisions: dict[str, list[WorkloadRevision]] = {}
        for row in rows:
            workload_id = str(row["workload_id"])
            revisions.setdefault(workload_id, []).append(
                WorkloadRevision(
                    revision_id=int(row["id"]),
                    workload_id=workload_id,
                    valid_from=row["valid_from"],
                    valid_to=row["valid_to"],
                    configuration=dict(row["workload_json"]),
                )
            )
        return revisions

    def load_runtime_job_keys(
        self,
        observation_id: str,
        start: datetime,
        end: datetime,
    ) -> list[RuntimeJobKey]:
        """Load runtime-generated keys active during a report window."""
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT runtime, namespace, job_key, workload_id, runtime_state,
                       valid_from, valid_to
                FROM runtime_job_keys
                WHERE observation_id = %s
                  AND valid_from <= %s
                  AND (valid_to IS NULL OR valid_to >= %s)
                ORDER BY workload_id, valid_from, job_key
                """,
                (observation_id, end, start),
            ).fetchall()
        return [
            RuntimeJobKey(
                runtime=WorkloadRuntime(row["runtime"]),
                namespace=str(row["namespace"]),
                key=str(row["job_key"]),
                workload_id=str(row["workload_id"]),
                runtime_state=str(row["runtime_state"]),
                valid_from=row["valid_from"],
                valid_to=row["valid_to"],
            )
            for row in rows
        ]

    def status(self) -> dict[str, Any]:
        """Return PostgreSQL and active-observation health."""
        with self._lock:
            self._connection.execute("SELECT 1").fetchone()
            observation = self._connection.execute(
                """
                SELECT id, started_at, ends_at
                FROM observations
                WHERE status = 'active'
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
            latest = self._connection.execute(
                """
                SELECT completed_at, status, duration_ms, workload_count, pod_count, error
                FROM state_collection_runs
                ORDER BY completed_at DESC
                LIMIT 1
                """
            ).fetchone()
        return {
            "backend": "postgresql",
            "writable": True,
            "active_observation": (
                {
                    "observation_id": str(observation["id"]),
                    "started_at": observation["started_at"].isoformat(),
                    "ends_at": observation["ends_at"].isoformat(),
                }
                if observation
                else None
            ),
            "latest_state_collection": (
                {
                    "completed_at": latest["completed_at"].isoformat(),
                    "status": latest["status"],
                    "duration_ms": latest["duration_ms"],
                    "workload_count": latest["workload_count"],
                    "pod_count": latest["pod_count"],
                    "error": latest["error"],
                }
                if latest
                else None
            ),
        }

    def close(self) -> None:
        """Close the PostgreSQL connection."""
        with self._lock:
            self._connection.close()

    def _migrate(self) -> None:
        with self._lock, self._connection.transaction():
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TIMESTAMPTZ NOT NULL
                );

                CREATE TABLE IF NOT EXISTS observations (
                    id TEXT PRIMARY KEY,
                    started_at TIMESTAMPTZ NOT NULL,
                    ends_at TIMESTAMPTZ NOT NULL,
                    ended_at TIMESTAMPTZ,
                    status TEXT NOT NULL CHECK (status IN ('active', 'completed'))
                );
                CREATE UNIQUE INDEX IF NOT EXISTS observations_one_active
                    ON observations(status) WHERE status = 'active';

                CREATE TABLE IF NOT EXISTS workloads (
                    observation_id TEXT NOT NULL REFERENCES observations(id) ON DELETE CASCADE,
                    workload_id TEXT NOT NULL,
                    runtime TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    name TEXT NOT NULL,
                    uid TEXT,
                    active BOOLEAN NOT NULL,
                    first_seen_at TIMESTAMPTZ NOT NULL,
                    last_seen_at TIMESTAMPTZ NOT NULL,
                    removed_at TIMESTAMPTZ,
                    PRIMARY KEY (observation_id, workload_id)
                );

                CREATE TABLE IF NOT EXISTS workload_revisions (
                    id BIGSERIAL PRIMARY KEY,
                    observation_id TEXT NOT NULL,
                    workload_id TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    valid_from TIMESTAMPTZ NOT NULL,
                    valid_to TIMESTAMPTZ,
                    workload_json JSONB NOT NULL,
                    FOREIGN KEY (observation_id, workload_id)
                        REFERENCES workloads(observation_id, workload_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS workload_revisions_window
                    ON workload_revisions(observation_id, workload_id, valid_from DESC);

                CREATE TABLE IF NOT EXISTS runtime_job_keys (
                    observation_id TEXT NOT NULL,
                    runtime TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    job_key TEXT NOT NULL,
                    workload_id TEXT NOT NULL,
                    runtime_state TEXT NOT NULL,
                    valid_from TIMESTAMPTZ NOT NULL,
                    valid_to TIMESTAMPTZ,
                    PRIMARY KEY (observation_id, runtime, namespace, job_key, valid_from),
                    FOREIGN KEY (observation_id, workload_id)
                        REFERENCES workloads(observation_id, workload_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS runtime_job_keys_window
                    ON runtime_job_keys(observation_id, runtime, namespace, job_key, valid_from);

                CREATE TABLE IF NOT EXISTS pod_assignments (
                    observation_id TEXT NOT NULL,
                    pod_uid TEXT NOT NULL,
                    workload_id TEXT NOT NULL,
                    runtime_job_key TEXT,
                    namespace TEXT NOT NULL,
                    pod_name TEXT NOT NULL,
                    first_seen_at TIMESTAMPTZ NOT NULL,
                    last_seen_at TIMESTAMPTZ NOT NULL,
                    pod_json JSONB NOT NULL,
                    PRIMARY KEY (observation_id, pod_uid),
                    FOREIGN KEY (observation_id, workload_id)
                        REFERENCES workloads(observation_id, workload_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS pod_assignments_workload
                    ON pod_assignments(observation_id, workload_id, runtime_job_key);
                CREATE INDEX IF NOT EXISTS pod_assignments_name
                    ON pod_assignments(observation_id, namespace, pod_name);

                CREATE TABLE IF NOT EXISTS state_collection_runs (
                    id BIGSERIAL PRIMARY KEY,
                    observation_id TEXT NOT NULL REFERENCES observations(id) ON DELETE CASCADE,
                    started_at TIMESTAMPTZ NOT NULL,
                    completed_at TIMESTAMPTZ NOT NULL,
                    status TEXT NOT NULL,
                    duration_ms INTEGER NOT NULL,
                    workload_count INTEGER NOT NULL,
                    pod_count INTEGER NOT NULL,
                    missing_state JSONB NOT NULL,
                    error TEXT
                );
                """
            )
            self._connection.execute(
                """
                INSERT INTO schema_migrations (version, applied_at)
                VALUES (%s, %s)
                ON CONFLICT (version) DO NOTHING
                """,
                (SCHEMA_VERSION, datetime.now(UTC)),
            )

    def _upsert_workload(
        self,
        observation_id: str,
        job: ObservedWorkload,
        observed_at: datetime,
    ) -> None:
        workload = job.workload
        last_seen_at = job.removed_at or observed_at
        self._connection.execute(
            """
            INSERT INTO workloads (
                observation_id, workload_id, runtime, namespace, name, uid, active,
                first_seen_at, last_seen_at, removed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (observation_id, workload_id) DO UPDATE SET
                runtime = EXCLUDED.runtime,
                namespace = EXCLUDED.namespace,
                name = EXCLUDED.name,
                uid = EXCLUDED.uid,
                active = EXCLUDED.active,
                last_seen_at = EXCLUDED.last_seen_at,
                removed_at = EXCLUDED.removed_at
            """,
            (
                observation_id,
                workload.workload_id,
                workload.runtime,
                workload.namespace,
                workload.name,
                workload.uid,
                job.active,
                observed_at,
                last_seen_at,
                job.removed_at,
            ),
        )
        payload = workload.to_dict()
        content_hash = hashlib.sha256(_canonical_json(payload).encode()).hexdigest()
        current = self._connection.execute(
            """
            SELECT id, content_hash
            FROM workload_revisions
            WHERE observation_id = %s AND workload_id = %s AND valid_to IS NULL
            ORDER BY valid_from DESC
            LIMIT 1
            FOR UPDATE
            """,
            (observation_id, workload.workload_id),
        ).fetchone()
        if not job.active:
            if current:
                self._connection.execute(
                    "UPDATE workload_revisions SET valid_to = %s WHERE id = %s",
                    (last_seen_at, current["id"]),
                )
            return
        if current and current["content_hash"] == content_hash:
            return
        if current:
            self._connection.execute(
                "UPDATE workload_revisions SET valid_to = %s WHERE id = %s",
                (observed_at, current["id"]),
            )
        self._connection.execute(
            """
            INSERT INTO workload_revisions (
                observation_id, workload_id, content_hash, valid_from, workload_json
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            (
                observation_id,
                workload.workload_id,
                content_hash,
                observed_at,
                Jsonb(payload),
            ),
        )

    def _sync_runtime_job_keys(
        self,
        observation_id: str,
        job: ObservedWorkload,
        observed_at: datetime,
    ) -> None:
        workload = job.workload
        desired = {
            selector.runtime_job_key: selector.runtime_state
            for selector in workload.pod_selectors
            if job.active
        }
        current_rows = self._connection.execute(
            """
            SELECT job_key, runtime_state
            FROM runtime_job_keys
            WHERE observation_id = %s AND workload_id = %s AND valid_to IS NULL
            FOR UPDATE
            """,
            (observation_id, workload.workload_id),
        ).fetchall()
        current = {str(row["job_key"]): str(row["runtime_state"]) for row in current_rows}
        for job_key, runtime_state in current.items():
            if desired.get(job_key) == runtime_state:
                continue
            self._connection.execute(
                """
                UPDATE runtime_job_keys
                SET valid_to = %s
                WHERE observation_id = %s AND workload_id = %s
                  AND job_key = %s AND valid_to IS NULL
                """,
                (observed_at, observation_id, workload.workload_id, job_key),
            )
        for job_key, runtime_state in desired.items():
            if current.get(job_key) == runtime_state:
                continue
            self._connection.execute(
                """
                INSERT INTO runtime_job_keys (
                    observation_id, runtime, namespace, job_key, workload_id,
                    runtime_state, valid_from
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    observation_id,
                    workload.runtime,
                    workload.namespace,
                    job_key,
                    workload.workload_id,
                    runtime_state,
                    observed_at,
                ),
            )

    def _upsert_pod(self, observation_id: str, pod: WorkloadPod) -> None:
        payload = asdict(pod)
        payload["first_seen_at"] = pod.first_seen_at.isoformat()
        payload["last_seen_at"] = pod.last_seen_at.isoformat()
        self._connection.execute(
            """
            INSERT INTO pod_assignments (
                observation_id, pod_uid, workload_id, runtime_job_key, namespace,
                pod_name, first_seen_at, last_seen_at, pod_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (observation_id, pod_uid) DO UPDATE SET
                workload_id = EXCLUDED.workload_id,
                runtime_job_key = EXCLUDED.runtime_job_key,
                namespace = EXCLUDED.namespace,
                pod_name = EXCLUDED.pod_name,
                last_seen_at = EXCLUDED.last_seen_at,
                pod_json = EXCLUDED.pod_json
            """,
            (
                observation_id,
                pod.uid,
                pod.workload_id,
                pod.runtime_job_key,
                pod.namespace,
                pod.name,
                pod.first_seen_at,
                pod.last_seen_at,
                Jsonb(payload),
            ),
        )

    def _restore_pods(self, state: ObservationState) -> None:
        rows = self._connection.execute(
            "SELECT pod_json FROM pod_assignments WHERE observation_id = %s",
            (state.observation_id,),
        ).fetchall()
        for row in rows:
            value = dict(row["pod_json"])
            job = state.jobs.get(str(value["workload_id"]))
            if job is None:
                continue
            value["first_seen_at"] = datetime.fromisoformat(value["first_seen_at"])
            value["last_seen_at"] = datetime.fromisoformat(value["last_seen_at"])
            pod = WorkloadPod(**value)
            job.workers[pod.uid] = pod

    def _cleanup_due(self, observed_at: datetime) -> bool:
        return (
            self._last_cleanup_at is None
            or observed_at - self._last_cleanup_at >= self._cleanup_interval
        )

    def _cleanup(self, observed_at: datetime) -> None:
        self._connection.execute(
            """
            DELETE FROM observations
            WHERE status = 'completed' AND ended_at < %s
            """,
            (observed_at - self._history_retention,),
        )


def _observation(row: dict[str, Any]) -> Observation:
    return Observation(
        observation_id=str(row["id"]),
        started_at=row["started_at"],
        ends_at=row["ends_at"],
    )


def _canonical_json(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))
