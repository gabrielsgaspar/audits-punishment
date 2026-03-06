"""Application configuration loader backed by environment variables."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, Field

from audits_punishment.paths import clean_dir, docs_dir, interim_dir, project_data_dir, raw_dir, repo_root


class Settings(BaseModel):
    """Runtime settings for pipeline modules."""

    openai_api_key: str | None = None
    openai_model: str = "gpt-4.1-mini"
    log_level: str = "INFO"
    user_agent: str = "audits-punishment-research"
    project_data_dir: Path = Field(default_factory=project_data_dir)
    raw_dir: Path = Field(default_factory=raw_dir)
    interim_dir: Path = Field(default_factory=interim_dir)
    clean_dir: Path = Field(default_factory=clean_dir)
    docs_dir: Path = Field(default_factory=docs_dir)

    @classmethod
    def from_env(cls) -> "Settings":
        env_path = repo_root() / ".env"
        load_dotenv(env_path, override=False)
        return cls(
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            openai_model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            user_agent=os.getenv("USER_AGENT", "audits-punishment-research"),
            project_data_dir=project_data_dir(),
            raw_dir=raw_dir(),
            interim_dir=interim_dir(),
            clean_dir=clean_dir(),
            docs_dir=docs_dir(),
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Load and cache settings once per process."""
    return Settings.from_env()
