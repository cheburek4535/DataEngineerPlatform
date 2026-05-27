
import requests
from services.db.db import get_session
from services.db.models import RawWeather, Weather, Anomaly
from typing import Optional, Dict
from sqlalchemy.orm import Session
from sqlalchemy import and_
import json
from logger import logger
from datetime import datetime, timedelta, timezone
import traceback

def process_raw_weather_message(message_value: dict) -> bool:
    db = get_session()

    try:
        loc_id = message_value.get('location_id')
        if loc_id is None:
            return False
        logger.info(f"ОБРАБОТКА ЛОКАЦИИ {loc_id}")
        raw_weather = save_raw_weather(db, message_value)

        weather = save_structured_weather(db, raw_weather)

        # anomalies = check_anomalies(db, weather, loc_id)


        db.commit()
        logger.info(f"Successfully processed weather data")
        return True
    except Exception as e:
        db.rollback()
        logger.info(f"Failed to process weather data: {e}")
        return False
    finally:
        db.close()


def process_raw_weather_batch(messages_batch: list) -> bool:
    """Обрабатывает батч сообщений: сохраняет данные и проверяет аномалии через Go"""
    db = get_session()

    try:
        # Сохраняем все записи
        weather_records = []
        loc_ids = []  # Сохраняем loc_id для каждого weather

        for message_value in messages_batch:
            loc_id = message_value.get('location_id')
            if loc_id is None:
                continue

            raw_weather = save_raw_weather(db, message_value)
            weather = save_structured_weather(db, raw_weather)
            weather_records.append(weather)
            loc_ids.append(loc_id)

        # Формируем данные для отправки в Go
        go_batch = []
        for i, weather in enumerate(weather_records):
            # Убедимся что все значения корректные
            temperature = float(weather.temperature) if weather.temperature is not None else None
            pressure = float(weather.pressure) if weather.pressure is not None else None
            humidity = float(weather.humidity) if weather.humidity is not None else None
            wind_speed = float(weather.wind_speed) if weather.wind_speed is not None else None

            go_item = {
                "id": weather.id,
                "loc_id": loc_ids[i],  # Используем location_id из исходного сообщения
                "lat": float(weather.lat),
                "lon": float(weather.lon),
                "temperature": temperature,
                "pressure": pressure,
                "humidity": humidity,
                "wind_speed": wind_speed,
                "collected_at": weather.timestamp.isoformat() if weather.timestamp else datetime.now(
                    timezone.utc).isoformat()
            }
            go_batch.append(go_item)

        # Отправляем батч в Go и обрабатываем аномалии
        if go_batch:
            check_anomalies_go(db, go_batch)

        db.commit()
        logger.info(f"Successfully processed batch of {len(messages_batch)} weather data")
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"Failed to process weather batch: {e}")
        logger.error(f"Error details: {traceback.format_exc()}")
        return False
    finally:
        db.close()
def save_raw_weather(db: Session, weather: dict) -> RawWeather:
    db_weather = RawWeather(
        # lat=weather['latitude'],
        # lon=weather['longitude'],
        data_json=weather,
        collected_at=datetime.fromtimestamp(weather.get('timestamp')),
    )
    db.add(db_weather)
    db.flush()
    db.refresh(db_weather)
    return db_weather


def save_structured_weather(db: Session, raw_weather: RawWeather) -> Weather:
    db_weather = Weather(
            lat=raw_weather.data_json['latitude'],
            lon=raw_weather.data_json['longitude'],
            timestamp=raw_weather.collected_at,
            temperature=raw_weather.data_json['temp'],
            pressure=raw_weather.data_json['pressure'],
            humidity=raw_weather.data_json['humidity'],
            wind_speed=raw_weather.data_json['wind_speed'],
        )
    db.add(db_weather)
    db.flush()
    return db_weather






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
        radius = 0.3
        similar_places = db.query(Weather).filter(
            and_(
                Weather.lat >= lat - radius,
                Weather.lat <= lat + radius,
                Weather.lon >= lon - radius,
                Weather.lon <= lon + radius,
                Weather.collected_at > datetime.now(timezone.utc) - timedelta(days=31),
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

    threshold = 0.65
    anomaly = {
            'temperature': abs(temperature - avg_temp) > threshold * abs(avg_temp) if temperature else False,
            'pressure': abs(pressure - avg_pressure) > threshold * abs(avg_pressure) if pressure else False,
            'humidity': abs(humidity - avg_humidity) > threshold * avg_humidity if humidity else False,
            'wind_speed': abs(wind_speed - avg_wind) > threshold * avg_wind if wind_speed else False,
            'averages': {'temperature': avg_temp, 'pressure': avg_pressure, 'humidity': avg_humidity, 'wind_speed': avg_wind}
    }
    return anomaly


def check_anomalies(db: Session, weather: Weather, loc_id: int) -> Optional[Anomaly]:
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
                    logger.info(f"Аномалия: {key}: {anomaly_value}! Среднее: {anomalies['averages'][key]}")

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
                        logger.info(f"Аномалий в {key} НЕ найдено! Среднее: {anomalies['averages'][key]}, текущее: {value}")

            if anomalies_to_save:
                saved_anomaly = save_anomaly(db, anomalies_to_save, loc_id, anomalies_data)
                if not saved_anomaly:
                    logger.info(f"Аномалия для локации {loc_id} не сохранена (уже существует)")
                return saved_anomaly

    return None


def check_anomalies_go(db: Session, batch: list) -> Optional[list]:
    """Отправляет батч в Go и сохраняет найденные аномалии"""
    if not batch:
        return None

    print("Проверка аномалий с Go для батча")

    logger.info(f"Sending batch of {len(batch)} items")

    response = requests.post("http://golang:8000/weather/batch", json=batch)

    if response.status_code != 200:
        logger.error(f"Go service returned error: {response.status_code}")
        logger.error(f"Response text: {response.text}")
        return None

    result = response.json()
    anomalies = result.get('result', [])

    if anomalies:
        for anomaly in anomalies:
            anomalies_to_save = anomaly.get('anomalies_to_save', {})
            loc_id = anomaly.get('loc_id')
            anomalies_data = anomaly.get('anomalies_data', {})

            if anomalies_to_save and loc_id:
                saved_anomaly = save_anomaly(db, anomalies_to_save, loc_id, anomalies_data)
                if not saved_anomaly:
                    logger.info(f"Аномалия для локации {loc_id} не сохранена (уже существует)")

        return anomalies

    return None

def save_anomaly(db: Session, anomalies: Dict[str, float], loc_id: int, data: dict) -> Optional[Anomaly]:
    exists_anomaly = db.query(Anomaly).filter_by(location_id=loc_id).order_by(Anomaly.found_at.desc()).first()

    create_new = False
    if not exists_anomaly:
        create_new = True
    else:
        # Проверяем изменения в параметрах
        new_anomalies_count = 0
        for k, value in anomalies.items():
            if getattr(exists_anomaly, k) != value:
                new_anomalies_count += 1

        if new_anomalies_count > 0 or (datetime.now(timezone.utc) - exists_anomaly.found_at) > timedelta(days=1):
            logger.info(
                f"Обновляем аномалию для локации {loc_id}:  ({new_anomalies_count} новых параметров или прошло более суток)")
            create_new = True

    if create_new:
        if exists_anomaly:
            # Обновляем существующую
            for k, value in anomalies.items():
                setattr(exists_anomaly, k, value)
            exists_anomaly.additional_data = data  # Обновляем доп. данные
            exists_anomaly.found_at = datetime.now(timezone.utc)  # Обновляем дату
            db.commit()
            db.refresh(exists_anomaly)
        else:
            # Создаем новую
            db_anomaly = Anomaly(
                location_id=loc_id,
                additional_data=data,
                **anomalies
            )
            db.add(db_anomaly)
            db.commit()
            db.refresh(db_anomaly)
            exists_anomaly = db_anomaly

        return exists_anomaly

    return None




