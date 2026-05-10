
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine

# from pydantic_settings import BaseSettings, SettingsConfigDict
#
# class Settings(BaseSettings):
#     app_env: str = 'local'
#     app_debug: bool = True
#     app_name: str = "weather_guard"
#
#     db_host: str = "postgres"
#     db_port: int = 5432
#     db_name: str = "weather_guard"
#     db_user: str = "airflow"
#     #db_password: str = Field(..., env="DB_PASSWORD") # type: ignore[arg-type]
#     db_password: str = "airflow"
#     secret_key: str = "636"
#     #secret_key: str = Field(..., env="SECRET_KEY") # type: ignore[arg-type]
#
#
#     model_config = SettingsConfigDict(
#         env_file=".env",
#         env_file_encoding="utf-8",
#         extra="ignore",
#     )
#
# settings = Settings() # type: ignore[arg-type]
#
# print("DB_USER:", repr(settings.db_user))
# print("DB_PASSWORD:", repr(settings.db_password))
# print("DB_HOST:", repr(settings.db_host))
# print("DB_NAME:", repr(settings.db_name))

#SQLALCHEMY_DATABASE_URL = "sqlite:///./sql_app.db"
# SQLALCHEMY_DATABASE_URL = f"postgresql+psycopg://{settings.db_user}:{settings.db_password}@{settings.db_host}:{settings.db_port}/{settings.db_name}"
#SQLALCHEMY_DATABASE_URL = f"postgresql+psycopg2://airflow:airflow@postgres:5432/weather_guard"
import os
from logger import logger

SQLALCHEMY_DATABASE_URL = os.getenv(
    "DATABASE_URL",
    # "postgresql+psycopg2://weather_user:weather_pass@localhost:5433/weather_guard"
"postgresql+psycopg2://weather_user:weather_pass@weather_db:5432/weather_guard"
)
# SQLALCHEMY_DATABASE_URL = "postgresql+psycopg2://weather_user:weather_pass@weather_db:5432/weather_guard"

engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=False, pool_pre_ping=True)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

print("DATABASE_URL:", engine.url)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_session():
    """Возвращает сессию для использования в скриптах"""
    return SessionLocal()

from services.db.models import *
def recreate_db():
    try:
        logger.info("Удаляем старые таблицы")
        Base.metadata.drop_all(bind=engine)
        logger.info("Создаем таблицы в PostgreSQL...")
        print("Таблицы в моделях:", list(Base.metadata.tables.keys()))
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Все таблицы созданы успешно!")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации БД: {e}")


if __name__ == '__main__':
    recreate_db()
