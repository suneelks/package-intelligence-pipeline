from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = Field(
        default="postgresql://postgres:postgres@localhost:5432/nuget_pipeline",
        description="Postgres DSN for the application database",
    )

    nuget_catalog_index_url: str = "https://api.nuget.org/v3/catalog0/index.json"
    nuget_user_agent: str = (
        "nuget-pipeline/0.1 (+https://github.com/suneelks/package-intelligence-pipeline)"
    )

    nuget_concurrency: int = 20
    nuget_process_batch_size: int = 100
    nuget_max_pages: int | None = None

    spdx_licenses_url: str = (
        "https://raw.githubusercontent.com/spdx/license-list-data/main/json/licenses.json"
    )

    oss_classifier_batch_size: int = 1000

    http_request_timeout_s: float = 60.0
    http_max_retries: int = 5

    log_level: str = "INFO"
    log_json: bool = True


settings = Settings()
