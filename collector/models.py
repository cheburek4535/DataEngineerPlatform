from sqlalchemy import UniqueConstraint, String, Index, Boolean, Integer, Column, DateTime, ForeignKey, Text, CheckConstraint, BigInteger, Date, Float, Time
from sqlalchemy.sql import func
from sqlalchemy.orm import Mapped, relationship, validates, object_session, mapped_column
from typing import Optional, List, Dict, Any
from collector.db import Base
from datetime import datetime, timezone
import time
from datetime import date
from sqlalchemy.dialects.postgresql import JSONB

from enum import Enum
from sqlalchemy.dialects.postgresql import ENUM as PGEnum


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