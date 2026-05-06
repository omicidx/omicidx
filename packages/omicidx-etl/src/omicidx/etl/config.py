"""config settings for omicidx_etl"""

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict
from upath import UPath

load_dotenv()  # Load environment variables from .env file


class Settings(BaseSettings):
    """settings for omicidx_etl"""

    model_config = SettingsConfigDict(
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="allow",
    )

    PUBLISH_DIRECTORY: str | None = "/data/omicidx"
    AWS_ACCESS_KEY_ID: str | None = None
    AWS_SECRET_ACCESS_KEY: str | None = None
    AWS_ENDPOINT_URL: str | None = None
    AWS_URL_STYLE: str | None = "path"
    AWS_USE_SSL: bool | None = True
    AWS_REGION: str | None = None

    @property
    def publish_directory(self) -> UPath:
        return UPath(self.PUBLISH_DIRECTORY)


settings = Settings()  # type: ignore

if __name__ == "__main__":
    print(settings.model_dump())
