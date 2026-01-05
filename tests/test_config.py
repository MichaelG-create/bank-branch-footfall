from bank_footfall.config.config import Settings


def test_settings_defaults():
    settings = Settings()

    # Project paths
    assert settings.project_root.is_dir()
    # Accept either repo root or src dir to avoid overspecifying
    assert str(settings.project_root).endswith("bank-branch-footfall/src")

    # Database: expect Postgres URL (matches CI DATABASE_URL)
    assert (
        settings.database_url == "postgresql://testuser:testpass@localhost:5432/testdb"
    )

    # API defaults
    assert settings.api_host == "127.0.0.1"
    assert settings.api_port == 8000
    assert settings.api_reload is True

    # Logging
    assert settings.log_level == "INFO"


def test_settings_env_override(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///./test.db")
    monkeypatch.setenv("API_HOST", "0.0.0.0")
    monkeypatch.setenv("API_PORT", "9000")
    monkeypatch.setenv("API_RELOAD", "false")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    settings = Settings()

    assert settings.database_url == "sqlite:///./test.db"
    assert settings.api_host == "0.0.0.0"
    assert settings.api_port == 9000
    # pydantic-settings parses bools from env strings
    assert settings.api_reload is False
    assert settings.log_level == "DEBUG"
