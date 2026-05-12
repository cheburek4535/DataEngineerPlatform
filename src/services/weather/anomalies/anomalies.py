from typing import Optional, Dict
from sqlalchemy.orm import Session
from sqlalchemy import and_
from services.db.models import Weather, Anomaly, LocationToTrack
from services.weather.transform import collect_weather
from logger import logger
from datetime import datetime, timedelta, timezone


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

    threshold = 0.5
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
                saved_anomaly = save_anomaly(db, anomalies_to_save, loc, anomalies_data)
                if not saved_anomaly:
                    logger.info(f"Аномалия для локации {loc.id} не сохранена (уже существует)")
                return saved_anomaly

    return None

def check_anomalies_for_batch(db: Session, loc: LocationToTrack, weather: Weather) -> Optional[Anomaly]:
    # Функция принимает готовый объект weather, собранный ранее пакетным запросом
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
                saved_anomaly = save_anomaly(db, anomalies_to_save, loc, anomalies_data)
                if not saved_anomaly:
                    logger.info(f"Аномалия для локации {loc.id} не сохранена (уже существует)")
                return saved_anomaly

    return None


def save_anomaly(db: Session, anomalies: Dict[str, float], loc: LocationToTrack, data: dict) -> Optional[Anomaly]:
    exists_anomaly = db.query(Anomaly).filter_by(location_id=loc.id).order_by(Anomaly.found_at.desc()).first()

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
                f"Обновляем аномалию для локации {loc.id}:  ({new_anomalies_count} новых параметров или прошло более суток)")
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
                location=loc,
                additional_data=data,
                **anomalies
            )
            db.add(db_anomaly)
            db.commit()
            db.refresh(db_anomaly)
            exists_anomaly = db_anomaly

        return exists_anomaly

    return None