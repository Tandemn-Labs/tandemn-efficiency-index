import pytest

from tandemn_efficiency_index.app import Settings


def test_settings_load_cluster_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEI_PROMETHEUS_URL", "http://prometheus:9090")
    monkeypatch.setenv("TEI_DATABASE_DSN", "postgresql://tei:password@postgres/tei")
    monkeypatch.setenv("TEI_DISCOVERY_MODE", "cluster")
    monkeypatch.setenv("TEI_DISCOVERY_NAMESPACES", "ignored")
    monkeypatch.setenv("TEI_API_BEARER_TOKEN", "secret-token")

    settings = Settings.from_environment()

    assert settings.discovery_namespaces == ()
    assert settings.discovery_interval_seconds == 10
    assert settings.state_interval_seconds == 10
    assert settings.observation_duration_hours == 24
    assert settings.prometheus_step_seconds == 10
    assert settings.database_dsn == "postgresql://tei:password@postgres/tei"
    assert settings.api_bearer_token == "secret-token"


def test_settings_require_namespaces_for_scoped_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TEI_PROMETHEUS_URL", "http://prometheus:9090")
    monkeypatch.setenv("TEI_DATABASE_DSN", "postgresql://tei:password@postgres/tei")
    monkeypatch.setenv("TEI_DISCOVERY_MODE", "namespaces")

    with pytest.raises(ValueError, match="TEI_DISCOVERY_NAMESPACES is required"):
        Settings.from_environment()
