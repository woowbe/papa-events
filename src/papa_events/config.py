from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    max_jobs: int = 20
    retries: int = 5
    model_config = SettingsConfigDict(env_prefix="papa_events_")


settings: Settings = Settings()
