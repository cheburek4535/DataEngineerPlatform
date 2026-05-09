from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional

class LocationBase(BaseModel):
    lat: float
    lon: float
    check_interval: int

class LocationCreate(LocationBase):
    pass

class Location(LocationBase):
    id: int
    last_checked_at: Optional[datetime]
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)

class Anomaly(BaseModel):
    id: int
    location_id: int
    anomaly_temperature: Optional[float]
    anomaly_humidity: Optional[float]
    anomaly_wind_speed: Optional[float]
    anomaly_pressure: Optional[float]
    additional_data: Optional[dict]
    found_at: datetime

    location: Location

    model_config = ConfigDict(from_attributes=True)

class RawWeather(BaseModel):
    id: int
    lat: float
    lon: float
    data_json: dict
    collected_at: datetime

    model_config = ConfigDict(from_attributes=True)

class Weather(BaseModel):
    id: int
    lat: float
    lon: float
    timestamp: datetime
    temperature: float
    pressure: float
    humidity: float
    wind_speed: float
    collected_at: datetime

    model_config = ConfigDict(from_attributes=True)





