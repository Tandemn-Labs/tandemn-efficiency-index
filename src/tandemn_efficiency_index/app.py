"""Run TEI workload discovery, telemetry collection, and dashboard serving."""

from __future__ import annotations

import logging
import os
import signal
import threading
from dataclasses import dataclass
from datetime import timedelta

from tandemn_efficiency_index.kubernetes_discovery import KubernetesWorkloadDiscovery
from tandemn_efficiency_index.observability import ObservabilityServer
from tandemn_efficiency_index.observer import ClusterObserver
from tandemn_efficiency_index.storage import PostgresObservationStore


@dataclass(frozen=True)
class Settings:
    """Environment-backed settings supplied by the TEI deployment."""

    prometheus_url: str
    discovery_namespaces: tuple[str, ...]
    discovery_interval_seconds: int
    state_interval_seconds: int
    observation_duration_hours: int
    prometheus_step_seconds: int
    database_dsn: str
    cleanup_interval_minutes: int
    prometheus_timeout_seconds: float
    prometheus_bearer_token: str | None
    prometheus_ca_file: str | None
    prometheus_insecure_skip_verify: bool
    api_bearer_token: str | None
    host: str
    port: int

    @classmethod
    def from_environment(cls) -> Settings:
        """Load and validate settings from environment variables."""
        prometheus_url = _required_environment("TEI_PROMETHEUS_URL")
        discovery_mode = os.environ.get("TEI_DISCOVERY_MODE", "cluster").strip().lower()
        if discovery_mode not in {"cluster", "namespaces"}:
            raise ValueError("TEI_DISCOVERY_MODE must be cluster or namespaces")

        configured_namespaces = tuple(
            namespace.strip()
            for namespace in os.environ.get("TEI_DISCOVERY_NAMESPACES", "").split(",")
            if namespace.strip()
        )
        if discovery_mode == "namespaces" and not configured_namespaces:
            raise ValueError(
                "TEI_DISCOVERY_NAMESPACES is required when TEI_DISCOVERY_MODE=namespaces"
            )
        namespaces = configured_namespaces if discovery_mode == "namespaces" else ()

        return cls(
            prometheus_url=prometheus_url,
            discovery_namespaces=namespaces,
            discovery_interval_seconds=_positive_integer(
                "TEI_DISCOVERY_INTERVAL_SECONDS",
                default=10,
            ),
            state_interval_seconds=_positive_integer("TEI_STATE_INTERVAL_SECONDS", default=10),
            observation_duration_hours=_positive_integer(
                "TEI_OBSERVATION_DURATION_HOURS", default=24
            ),
            prometheus_step_seconds=_positive_integer("TEI_PROMETHEUS_STEP_SECONDS", default=10),
            database_dsn=_required_environment("TEI_DATABASE_DSN"),
            cleanup_interval_minutes=_positive_integer("TEI_CLEANUP_INTERVAL_MINUTES", default=15),
            prometheus_timeout_seconds=_positive_float(
                "TEI_PROMETHEUS_TIMEOUT_SECONDS", default=5.0
            ),
            prometheus_bearer_token=_optional_environment("TEI_PROMETHEUS_BEARER_TOKEN"),
            prometheus_ca_file=_optional_environment("TEI_PROMETHEUS_CA_FILE"),
            prometheus_insecure_skip_verify=_boolean_environment(
                "TEI_PROMETHEUS_INSECURE_SKIP_VERIFY", default=False
            ),
            api_bearer_token=_optional_environment("TEI_API_BEARER_TOKEN"),
            host=os.environ.get("TEI_HOST", "0.0.0.0"),
            port=_positive_integer("TEI_PORT", default=8000),
        )


def main() -> None:
    """Start the in-cluster TEI service."""
    logging.basicConfig(
        level=os.environ.get("TEI_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = Settings.from_environment()
    observation_duration = timedelta(hours=settings.observation_duration_hours)
    store = PostgresObservationStore(
        settings.database_dsn,
        observation_duration=observation_duration,
        cleanup_interval=timedelta(minutes=settings.cleanup_interval_minutes),
    )
    discovery = KubernetesWorkloadDiscovery.from_in_cluster(
        namespaces=settings.discovery_namespaces
    )
    observer = ClusterObserver.from_in_cluster(
        prometheus_url=settings.prometheus_url,
        workload_discovery=discovery,
        discovery_interval=timedelta(seconds=settings.discovery_interval_seconds),
        retention=observation_duration,
        sample_interval_seconds=settings.state_interval_seconds,
        prometheus_step_seconds=settings.prometheus_step_seconds,
        store=store,
        prometheus_timeout_seconds=settings.prometheus_timeout_seconds,
        prometheus_bearer_token=settings.prometheus_bearer_token,
        prometheus_ca_file=settings.prometheus_ca_file,
        prometheus_insecure_skip_verify=settings.prometheus_insecure_skip_verify,
    )
    server = ObservabilityServer(
        observer,
        host=settings.host,
        port=settings.port,
        api_bearer_token=settings.api_bearer_token,
    )
    _install_signal_handlers(server)
    server.serve_forever()


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _optional_environment(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def _positive_integer(name: str, default: int) -> int:
    raw_value = os.environ.get(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _positive_float(name: str, default: float) -> float:
    raw_value = os.environ.get(name, str(default))
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _boolean_environment(name: str, default: bool) -> bool:
    raw_value = os.environ.get(name, str(default)).strip().lower()
    if raw_value in {"true", "1", "yes"}:
        return True
    if raw_value in {"false", "0", "no"}:
        return False
    raise ValueError(f"{name} must be true or false")


def _install_signal_handlers(server: ObservabilityServer) -> None:
    def shutdown(signum: int, frame: object) -> None:
        logging.getLogger(__name__).info("Received signal %d; shutting down", signum)
        threading.Thread(target=server.shutdown, name="tei-shutdown", daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
