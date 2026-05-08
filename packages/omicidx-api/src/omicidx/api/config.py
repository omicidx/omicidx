from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="OMICIDX_API_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgresql://omicidx@localhost:5432/omicidx"

    @property
    def async_database_url(self) -> str:
        """Convert standard postgresql:// URI to asyncpg driver URI."""
        return self.database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    db_pool_size: int = 10
    db_max_overflow: int = 20

    # ClickHouse usage logging (issue #43)
    clickhouse_url: str | None = None
    clickhouse_user: str = "default"
    clickhouse_password: SecretStr | None = None
    clickhouse_database: str = "omicidx"

    # Rate limiting
    rate_limit: str = "1000/minute"

    # Pagination defaults
    default_page_size: int = 25
    max_page_size: int = 500


settings = Settings()
