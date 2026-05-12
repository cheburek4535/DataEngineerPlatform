from sqlalchemy.orm import Mapped, relationship
from sqlalchemy import Integer, Column, DateTime, ForeignKey, Float, String, Numeric
from sqlalchemy.sql import func
from typing import List, Optional
from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB
from services.db.db import Base
import decimal


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
    #updated_at: Mapped[datetime] = Column(DateTime(timezone=True), onupdate=func.now())

    location: Mapped["LocationToTrack"] = relationship(
        "LocationToTrack",
        back_populates="anomalies"
    )


class RawCurrency(Base):
    __tablename__ = 'raw_currencies'
    id : Mapped[int] = Column(Integer, primary_key=True, index=True)
    json_data: Mapped[dict] = Column(JSONB)
    collected_at: Mapped[datetime] = Column(DateTime(timezone=True), server_default=func.now())

class CurrencyHistory(Base):
    __tablename__ = 'currency_history'
    id: Mapped[int] = Column(Integer, primary_key=True, index=True)
    name: Mapped[str] = Column(String(64), index=True, nullable=False)
    code: Mapped[str] = Column(String(3), nullable=False)
    value_in_rubles: Mapped[decimal.Decimal] = Column(Numeric(precision=16, scale=6), index=True, nullable=False)
    timestamp: Mapped[datetime] = Column(DateTime(timezone=True), server_default=func.now())

class Currency(Base):
    __tablename__ = 'currencies'
    id : Mapped[int] = Column(Integer, primary_key=True, index=True)
    name: Mapped[str] = Column(String(64), index=True, nullable=False)
    code: Mapped[str] = Column(String(3), nullable=False)
    value_in_rubles: Mapped[decimal.Decimal] = Column(Numeric(precision=16, scale=6), index=True, nullable=False)
    created_at: Mapped[datetime] = Column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = Column(DateTime(timezone=True), nullable=True, onupdate=func.now())

    sharp_changes: Mapped[List["CurrencySharpChange"]] = relationship("CurrencySharpChange", back_populates="currency", cascade="all, delete-orphan")

class CurrencySharpChange(Base):
    __tablename__ = 'currency_sharp_changes'
    id : Mapped[int] = Column(Integer, primary_key=True, index=True)
    change_percents: Mapped[float] = Column(Float, index=True, nullable=False)
    value_in_rubles: Mapped[decimal.Decimal] = Column(Numeric(precision=16, scale=6), index=True, nullable=False)
    previous_value: Mapped[decimal.Decimal] = Column(Numeric(precision=16, scale=6), nullable=True)
    currency_id: Mapped[int] = Column(Integer, ForeignKey('currencies.id'), nullable=False)
    #additional_data: Mapped[Optional[dict]] = Column(JSONB, nullable=True)
    found_at: Mapped[datetime] = Column(DateTime(timezone=True), nullable=True, server_default=func.now())

    currency: Mapped["Currency"] = relationship("Currency", back_populates="sharp_changes")
