from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    app_env: str = 'local'
    app_debug: bool = True
    app_name: str = "weather_guard"

    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "weather_guard"
    db_user: str = "postgres"
    #db_password: str = Field(..., env="DB_PASSWORD") # type: ignore[arg-type]
    db_password: str = "A101325b!A255075B!!e"
    secret_key: str = "636"
    #secret_key: str = Field(..., env="SECRET_KEY") # type: ignore[arg-type]


    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

settings = Settings() # type: ignore[arg-type]

print("DB_USER:", repr(settings.db_user))
print("DB_PASSWORD:", repr(settings.db_password))
print("DB_HOST:", repr(settings.db_host))
print("DB_NAME:", repr(settings.db_name))