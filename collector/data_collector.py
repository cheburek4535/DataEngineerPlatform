from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from collector.models import RawWeather, Weather, Anomaly, LocationToTrack
from db import get_session
import openmeteo_requests
import json

import requests_cache
from retry_requests import retry

db = get_session()

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


def compare_weather(db: Session, structured_weather: Weather) -> Optional[dict]:
    lat = structured_weather.lat
    lon = structured_weather.lon
    temperature = structured_weather.temperature
    pressure = structured_weather.pressure
    humidity = structured_weather.humidity
    wind_speed = structured_weather.wind_speed

    similar_places = []
    if lon is not None and lat is not None:
        print("Ищем похожие места")
        radius = 1.0
        similar_places = db.query(Weather).filter(
            and_(  # ← 4 УСЛОВИЯ and_, НЕ or_!
                Weather.lat >= lat - radius,
                Weather.lat <= lat + radius,
                Weather.lon >= lon - radius,
                Weather.lon <= lon + radius
            )
        ).all()
        print(f"Найдено похожих: {len(similar_places)} в радиусе {radius}° от ({lat}, {lon})")
    if len(similar_places) < 2:
        print("Похожие места не найдены или их не хватает")
        return None

    temps = [place.temperature for place in similar_places if place.temperature is not None]
    pressures = [place.pressure for place in similar_places if place.pressure is not None]
    humidities = [place.humidity for place in similar_places if place.humidity is not None]
    winds = [place.wind_speed for place in similar_places if place.wind_speed is not None]

    if not temps:
        return None

    avg_temp = sum(temps) / len(temps)
    avg_pressure = sum(pressures) / len(pressures)
    avg_humidity = sum(humidities) / len(humidities)
    avg_wind = sum(winds) / len(winds)

    threshold = 0.3
    anomaly = {
            'temperature': abs(temperature - avg_temp) > threshold * abs(avg_temp) if temperature else False,
            'pressure': abs(pressure - avg_pressure) > threshold * abs(avg_pressure) if pressure else False,
            'humidity': abs(humidity - avg_humidity) > threshold * avg_humidity if humidity else False,
            'wind_speed': abs(wind_speed - avg_wind) > threshold * avg_wind if wind_speed else False,
            'averages': {'temperature': avg_temp, 'pressure': avg_pressure, 'humidity': avg_humidity, 'wind_speed': avg_wind}
    }
    return anomaly


def check_anomalies(db: Session, lat: float, lon: float, loc: LocationToTrack) -> Optional[Anomaly]:
    weather = collect_weather(db, lat=lat, lon=lon)
    if weather:
        print("Проверка аномалий")
        anomalies = compare_weather(db, weather)
        if anomalies:

            # Собираем все аномалии для этой локации
            anomalies_to_save = {}
            anomalies_data = {}

            for key, is_anomaly in anomalies.items():
                if is_anomaly and key != 'averages':
                    anomaly_value = getattr(weather, key)
                    print(f"Аномалия: {key}: {anomaly_value}! Среднее: {anomalies['averages'][key]}")

                    # Определяем правильное название поля для БД
                    if key == "temperature":
                        anomalies_to_save['anomaly_temperature'] = anomaly_value
                    elif key == "pressure":
                        anomalies_to_save['anomaly_pressure'] = anomaly_value
                    elif key == "humidity":
                        anomalies_to_save['anomaly_humidity'] = anomaly_value
                    elif key == "wind_speed":
                        anomalies_to_save['anomaly_wind_speed'] = anomaly_value

                    # Сохраняем данные для additional_data
                    anomalies_data[key] = {
                        "value": anomaly_value,
                        "avg": anomalies['averages'][key]
                    }

                else:
                    if key != 'averages':
                        value = getattr(weather, key)
                        print(f"Аномалий в {key} НЕ найдено! Среднее: {anomalies['averages'][key]}, текущее: {value}")

            if anomalies_to_save:
                saved_anomaly = save_anomaly(db, anomalies_to_save, loc, anomalies_data)
                return saved_anomaly

    return None


def save_anomaly(db: Session, anomalies: dict, loc: LocationToTrack, data: dict) -> Optional[Anomaly]:
    db_anomaly = Anomaly(
        location=loc,
        additional_data=data,
        **anomalies  # Распаковываем все аномалии в конструктор
    )
    db.add(db_anomaly)
    db.commit()
    db.refresh(db_anomaly)
    return db_anomaly
# check_anomalies(db, 57, 38)
# print(check_anomalies(db, lat=47.48, lon=97.98))
