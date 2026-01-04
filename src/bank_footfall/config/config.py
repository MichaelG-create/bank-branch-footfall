"""Configuration management for the bank footfall application."""

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Project paths
    project_root: Path = Field(
        default_factory=lambda: Path(__file__).parent.parent.parent
    )
    pythonpath: str = Field(
        default_factory=lambda: str(Path(__file__).parent.parent.parent)
    )

    # Database
    database_url: str = Field(
        default="sqlite:///./bank_footfall.db", env="DATABASE_URL"
    )

    # API
    api_host: str = Field(default="127.0.0.1", env="API_HOST")
    api_port: int = Field(default=8000, env="API_PORT")
    api_reload: bool = Field(default=True, env="API_RELOAD")

    # Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")

    # Airflow
    airflow_home: Optional[str] = Field(default=None, env="AIRFLOW_HOME")

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "allow"

    def setup_python_path(self) -> None:
        """Add project root to Python path."""
        import sys

        if str(self.project_root) not in sys.path:
            sys.path.insert(0, str(self.project_root))


# Global settings instance
settings = Settings()

# Setup Python path automatically
settings.setup_python_path()
