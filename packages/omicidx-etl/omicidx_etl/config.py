"""config settings for omicidx_etl"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
from dotenv import load_dotenv
from upath import UPath

load_dotenv()  # Load environment variables from .env file

class Settings(BaseSettings):
    """settings for omicidx_etl"""

    model_config = SettingsConfigDict(
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="allow",
    )

    PUBLISH_DIRECTORY: Optional[str] = '/data/omicidx'
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_ENDPOINT_URL: Optional[str] = None
    AWS_URL_STYLE: Optional[str] = 'path'
    AWS_USE_SSL: Optional[bool] = True
    AWS_REGION: Optional[str] = None

    @property
    def publish_directory(self) -> UPath:
        return UPath(self.PUBLISH_DIRECTORY)

settings = Settings()  # type: ignore

if __name__ == "__main__":
    print(settings.model_dump())
