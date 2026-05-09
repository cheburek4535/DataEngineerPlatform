from typing import Optional
from sqlalchemy.orm import Session
from db import RawWeather
import openmeteo_requests
import json

import requests_cache
from retry_requests import retry


cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)
def get_weather(lon: float, lat: float) -> Optional[dict]:
    url = "https://api.open-meteo.com/v1/forecast"
    if lon is not None and lat is not None:
        params = {
            'latitude': lat,
            'longitude': lon,
            'current': 'temperature_2m,wind_speed_10m,relative_humidity_2m,pressure_msl'
        }

    else:
        return None

    try:
        responses = openmeteo.weather_api(url, params=params)

        response = responses[0]
        current = response.Current()
        current_temperature = current.Variables(0).Value()
        current_wind_speed = current.Variables(1).Value()
        current_relative_humidity = current.Variables(2).Value()
        current_pressure = current.Variables(3).Value()

        latitude = response.Latitude()
        longitude = response.Longitude()

        data = {
            'temp': float(current_temperature),  # Преобразуем в обычный float
            'wind_speed': float(current_wind_speed),
            'humidity': float(current_relative_humidity),
            'pressure': float(current_pressure),
            'latitude': float(latitude),
            'longitude': float(longitude),
            'timestamp': response.Current().Time(),  # Время текущих данных
            'raw_json': json.loads(json.dumps({
                'latitude': response.Latitude(),
                'longitude': response.Longitude(),
                'elevation': response.Elevation(),
                'timezone': response.Timezone(),
                'timezone_abbreviation': response.TimezoneAbbreviation(),
                'utc_offset_seconds': response.UtcOffsetSeconds(),
                'current': {
                    'time': response.Current().Time(),
                    'interval': response.Current().Interval(),
                    'temperature_2m': float(current_temperature),
                    'wind_speed_10m': float(current_wind_speed),
                    'relative_humidity_2m': float(current_relative_humidity),
                    'pressure_msl': float(current_pressure)
                }
            }))
        }

        return data
    except Exception as e:
        print(f"Ошибка запроса погоды: {e}")
        return None


def collect_raw_weather(db: Session, weather: dict) -> dict:
        print(weather)
        db_weather = RawWeather(
            lat=weather['latitude'],
            lon=weather['longitude'],
            data_json=weather
        )
        db.add(db_weather)
        db.commit()
        db.refresh(db_weather)
        return db_weather