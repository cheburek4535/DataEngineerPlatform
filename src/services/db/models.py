from sqlalchemy.orm import Mapped, relationship
from sqlalchemy import Integer, Column, DateTime, ForeignKey, Float, String, Numeric, Boolean
from sqlalchemy.sql import func
from typing import List, Optional
from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB
from services.db.db import Base
import decimal
from sqlalchemy.ext.hybrid import hybrid_property


class RawWeather(Base):
    __tablename__ = 'raw_weather'
    id: Mapped[int] = Column(Integer, primary_key=True, index=True)
    # lat: Mapped[float] = Column(Float, index=True, nullable=False)
    # lon: Mapped[float] = Column(Float, index=True, nullable=False)
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
    last_checked_at: Mapped[datetime] = Column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = Column(DateTime(timezone=True), server_default=func.now())
    is_on_land: Mapped[bool] = Column(Boolean, index=True, nullable=True)

    anomalies: Mapped[List["Anomaly"]] = relationship(
        "Anomaly",
        back_populates="location",
        cascade="all, delete-orphan"
    )
    air_quality: Mapped[List["AirQuality"]] = relationship(
        "AirQuality",
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


class RawAirQuality(Base):
    __tablename__ = 'raw_air_quality'
    id: Mapped[int] = Column(Integer, primary_key=True, index=True)
    # lat: Mapped[float] = Column(Float, index=True, nullable=False)
    # lon: Mapped[float] = Column(Float, index=True, nullable=False)
    json_data: Mapped[dict] = Column(JSONB)
    collected_at: Mapped[datetime] = Column(DateTime(timezone=True))
    created_at: Mapped[datetime] = Column(DateTime(timezone=True), server_default=func.now())

class AirQuality(Base):
    __tablename__ = 'air_quality'
    id: Mapped[int] = Column(Integer, primary_key=True, index=True)
    location_id: Mapped[int] = Column(Integer, ForeignKey('locations_to_track.id'), nullable=False)
    pm25: Mapped[Optional[float]] = Column(Float, nullable=True)  # µg/m³
    pm10: Mapped[Optional[float]] = Column(Float, nullable=True)  # µg/m³
    no2: Mapped[Optional[float]] = Column(Float, nullable=True)  # ppm → пересчитаем в ppb
    o3: Mapped[Optional[float]] = Column(Float, nullable=True)  # ppm → пересчитаем в ppb
    so2: Mapped[Optional[float]] = Column(Float, nullable=True)  # ppm → пересчитаем в ppb
    co: Mapped[Optional[float]] = Column(Float, nullable=True)  # ppm
    collected_at: Mapped[datetime] = Column(DateTime(timezone=True), server_default=func.now())

    location: Mapped["LocationToTrack"] = relationship(
        "LocationToTrack",
        back_populates="air_quality"
    )
    # Уровни по стандартам ВОЗ (среднечасовые, кроме PM — среднесуточные ориентиры)
    # Формат: (хорошо, удовлетворительно, вредно для чувствительных, вредно, очень вредно)
    _PM25_THRESHOLDS = (12, 35, 55, 150, 250)  # µg/m³
    _PM10_THRESHOLDS = (20, 50, 100, 200, 350)  # µg/m³
    _NO2_THRESHOLDS = (0.02, 0.05, 0.1, 0.2, 0.5)  # ppm (часовые)
    _O3_THRESHOLDS = (0.05, 0.07, 0.1, 0.15, 0.2)  # ppm (8-часовые)
    _SO2_THRESHOLDS = (0.02, 0.05, 0.1, 0.2, 0.5)  # ppm (часовые)
    _CO_THRESHOLDS = (4.0, 9.0, 15.0, 30.0, 50.0)  # ppm (8-часовые)

    @hybrid_property
    def air_quality_level(self) -> Optional[str]:
        """
        Возвращает уровень качества воздуха по худшему из параметров.
        Уровни: 'good', 'moderate', 'unhealthy_sensitive', 'unhealthy', 'very_unhealthy', 'hazardous'
        None — если нет ни одного измерения.
        """
        levels = []

        for param_value, thresholds in [
            (self.pm25, self._PM25_THRESHOLDS),
            (self.pm10, self._PM10_THRESHOLDS),
            (self.no2, self._NO2_THRESHOLDS),
            (self.o3, self._O3_THRESHOLDS),
            (self.so2, self._SO2_THRESHOLDS),
            (self.co, self._CO_THRESHOLDS),
        ]:
            if param_value is not None:
                levels.append(self._level_for_value(param_value, thresholds))

        if not levels:
            return None

        # Возвращаем худший уровень
        LEVEL_ORDER = ['good', 'moderate', 'unhealthy_sensitive', 'unhealthy', 'very_unhealthy', 'hazardous']
        return max(levels, key=lambda x: LEVEL_ORDER.index(x))

    @staticmethod
    def _level_for_value(value: float, thresholds: tuple) -> str:
        if value <= thresholds[0]:
            return 'good'
        elif value <= thresholds[1]:
            return 'moderate'
        elif value <= thresholds[2]:
            return 'unhealthy_sensitive'
        elif value <= thresholds[3]:
            return 'unhealthy'
        elif value <= thresholds[4]:
            return 'very_unhealthy'
        else:
            return 'hazardous'

    @air_quality_level.expression
    def air_quality_level(cls):
        """
        SQL-выражение для фильтрации на уровне БД.
        Чтобы работало: session.query(AirQuality).filter(AirQuality.air_quality_level == 'hazardous')
        """
        from sqlalchemy import case

        def case_for_param(param_col, thresholds):
            return case(
                (param_col == None, None),
                (param_col <= thresholds[0], 0),
                (param_col <= thresholds[1], 1),
                (param_col <= thresholds[2], 2),
                (param_col <= thresholds[3], 3),
                (param_col <= thresholds[4], 4),
                else_=5
            )

        pm25_level = case_for_param(cls.pm25, cls._PM25_THRESHOLDS)
        pm10_level = case_for_param(cls.pm10, cls._PM10_THRESHOLDS)
        no2_level = case_for_param(cls.no2, cls._NO2_THRESHOLDS)
        o3_level = case_for_param(cls.o3, cls._O3_THRESHOLDS)
        so2_level = case_for_param(cls.so2, cls._SO2_THRESHOLDS)
        co_level = case_for_param(cls.co, cls._CO_THRESHOLDS)

        # GREATEST среди всех уровней (худший)
        return func.greatest(pm25_level, pm10_level, no2_level, o3_level, so2_level, co_level)

    @hybrid_property
    def is_good(self) -> Optional[bool]:
        """True — воздух хороший, False — есть превышения, None — нет данных."""
        level = self.air_quality_level
        if level is None:
            return None
        return level == 'good'

    @is_good.expression
    def is_good(cls):
        return cls.air_quality_level == 0


class ApiPollProgress(Base):
    __tablename__ = "api_poll_progress"
    id: Mapped[int] = Column(Integer, primary_key=True)
    batch_id: Mapped[str] = Column(String, nullable=False)  # напр. "daily_2026-05-15"
    last_location_id: Mapped[int] = Column(Integer, nullable=False)
    updated_at: Mapped[datetime] = Column(DateTime(timezone=True),
                                          default=func.now(), onupdate=func.now())


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

