from typing import Optional
from sqlalchemy.orm import Session
from db import RawWeather, Weather
from extract import collect_raw_weather, get_weather


def save_structured_weather(db: Session, raw_weather: RawWeather) -> dict:
    db_weather = Weather(
            lat=raw_weather.lat,
            lon=raw_weather.lon,
            timestamp=raw_weather.collected_at,
            temperature=raw_weather.data_json['temp'],
            pressure=raw_weather.data_json['pressure'],
            humidity=raw_weather.data_json['humidity'],
            wind_speed=raw_weather.data_json['wind_speed'],
        )
    print(db_weather)
    db.add(db_weather)
    db.commit()
    return db_weather

def collect_weather(db: Session, lat: float, lon: float) -> Optional[dict]:
    weather = get_weather(lon=lon, lat=lat)
    if weather:
        raw_weather = collect_raw_weather(db, weather)
        structured_weather = save_structured_weather(db, raw_weather)
        return structured_weather
    return None