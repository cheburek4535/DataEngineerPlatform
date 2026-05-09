
from sqlalchemy.orm import declarative_base, sessionmaker, Mapped, relationship
from sqlalchemy import Integer, Column, DateTime, ForeignKey, Float, create_engine
from sqlalchemy.sql import func
from typing import List
from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB
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
SQLALCHEMY_DATABASE_URL = "postgresql+psycopg2://weather_user:weather_pass@weather_db:5432/weather_guard"

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





class RawWeather(Base):
    __tablename__ = 'raw_weather'
    id: Mapped[int] = Column(Integer, primary_key=True, index=True)
    lat: Mapped[float] = Column(Float, index=True, nullable=False)
    lon: Mapped[float] = Column(Float, index=True, nullable=False)
    data_json: Mapped[dict] = Column(JSONB)
    collected_at: Mapped[datetime] = Column(DateTime(timezone=True), server_default=func.now())

class Weather(Base):
    __tablename__ = 'weather'
    id: Mapped[int] = Column(Integer, primary_key=True, index=True)
    lat: Mapped[float] = Column(Float, index=True, nullable=False)
    lon: Mapped[float] = Column(Float, index=True, nullable=False)
    timestamp: Mapped[datetime] = Column(DateTime(timezone=True))
    temperature: Mapped[float] = Column(Float, index=True, nullable=False)
    pressure: Mapped[float] = Column(Float, index=True, nullable=False)
    humidity: Mapped[float] = Column(Float, index=True, nullable=False)
    wind_speed: Mapped[float] = Column(Float, index=True, nullable=False)
    collected_at: Mapped[datetime] = Column(DateTime(timezone=True), server_default=func.now())


class LocationToTrack(Base):
    __tablename__ = 'locations_to_track'
    id : Mapped[int] = Column(Integer, primary_key=True, index=True)
    lat: Mapped[float] = Column(Float, index=True, nullable=False)
    lon: Mapped[float] = Column(Float, index=True, nullable=False)
    check_interval: Mapped[int] = Column(Integer, index=True, nullable=False, default=900)
    last_checked_at: Mapped[datetime] = Column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = Column(DateTime(timezone=True), server_default=func.now())

    anomalies: Mapped[List["Anomaly"]] = relationship(
        "Anomaly",
        back_populates="location",
        cascade="all, delete-orphan"
    )


class Anomaly(Base):
    __tablename__ = 'anomalies'
    id : Mapped[int] = Column(Integer, primary_key=True, index=True)
    location_id: Mapped[int] = Column(Integer, ForeignKey('locations_to_track.id'), nullable=False)
    anomaly_temperature: Mapped[float] = Column(Float, index=True, nullable=True)
    anomaly_humidity: Mapped[float] = Column(Float, index=True, nullable=True)
    anomaly_wind_speed: Mapped[float] = Column(Float, index=True, nullable=True)
    anomaly_pressure: Mapped[float] = Column(Float, index=True, nullable=True)
    additional_data: Mapped[dict] = Column(JSONB)
    found_at: Mapped[datetime] = Column(DateTime(timezone=True), server_default=func.now())

    location: Mapped["LocationToTrack"] = relationship(
        "LocationToTrack",
        back_populates="anomalies"
    )