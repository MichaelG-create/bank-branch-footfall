"""Configuration management for the bank footfall application."""
# src/bank_footfall/config/config.py
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# ---- Path helpers ----
CONFIG_DIR = Path(__file__).resolve().parent  # .../src/bank_footfall/config
PACKAGE_ROOT = CONFIG_DIR.parent  # .../src/bank_footfall
SRC_ROOT = PACKAGE_ROOT.parent  # .../src
PROJECT_ROOT = SRC_ROOT.parent  # .../bank-branch-footfall


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="allow",
    )

    # Project paths
    project_root: Path = Field(default=PROJECT_ROOT)  # real repo root
    src_root: Path = Field(default=SRC_ROOT)  # .../src
    pythonpath: str = Field(default=str(SRC_ROOT))  # for convenience

    # Database
    database_url: str = Field(default="sqlite:///./bank_footfall.db")

    # API
    api_host: str = Field(default="127.0.0.1")
    api_port: int = Field(default=8000)
    api_reload: bool = Field(default=True)

    # Logging
    log_level: str = Field(default="INFO")

    # Airflow
    airflow_home: Optional[str] = None


settings = Settings()
