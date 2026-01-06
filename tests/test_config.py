from bank_footfall.config.config import Settings


def test_settings_defaults():
    """Default config without env vars: local dev SQLite + correct paths."""
    settings = Settings()

    # Project paths
    assert settings.project_root.is_dir()
    assert settings.src_root.is_dir()
    assert settings.src_root.parent == settings.project_root

    # Local default DB is SQLite
    assert settings.database_url == "sqlite:///./bank_footfall.db"

    # API defaults
    assert settings.api_host == "127.0.0.1"
    assert settings.api_port == 8000
    assert settings.api_reload is True

    # Logging
    assert settings.log_level == "INFO"


def test_settings_env_override(monkeypatch):
    """Simulate CI or override via env vars (e.g. Postgres in CI)."""
    monkeypatch.setenv(
        "DATABASE_URL", "postgresql://testuser:testpass@localhost:5432/testdb"
    )
    monkeypatch.setenv("API_HOST", "0.0.0.0")
    monkeypatch.setenv("API_PORT", "9000")
    monkeypatch.setenv("API_RELOAD", "false")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    settings = Settings()

    # DB overridden by env (matches CI style)
    assert (
        settings.database_url == "postgresql://testuser:testpass@localhost:5432/testdb"
    )

    assert settings.api_host == "0.0.0.0"
    assert settings.api_port == 9000
    assert settings.api_reload is False  # pydantic parses "false"
    assert settings.log_level == "DEBUG"
