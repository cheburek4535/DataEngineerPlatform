
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv
from config import settings

load_dotenv()


#SQLALCHEMY_DATABASE_URL = "sqlite:///./sql_app.db"
SQLALCHEMY_DATABASE_URL = f"postgresql+psycopg://{settings.db_user}:{settings.db_password}@{settings.db_host}:{settings.db_port}/{settings.db_name}"

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


from sqlalchemy import Integer, Column, DateTime, ForeignKey, Float
from sqlalchemy.sql import func
from sqlalchemy.orm import Mapped, relationship
from typing import List
from collector.db import Base
from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB


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
        back_populates="anomalies"
    )