#A101325b!$ABab
from typing import Optional, List, Dict
import requests
from logger import logger
import urllib.parse

OPENAQ_API_KEY = "522fb004826777c1888ce980d9517b32bceeac75fa7db3c02c563a99e1525177"
OPENAQ_BASE_URL = "https://api.openaq.org/v3"

HEADERS = {
    "X-API-Key": OPENAQ_API_KEY,
    "Accept": "application/json"
}


def find_nearby_locations(lat: float, lon: float, radius_meters: int = 10000) -> List[Dict]:
    """
    Находит станции мониторинга в радиусе radius_meters от точки.
    Возвращает список локаций с их сенсорами.

    API: GET /v3/locations?coordinates=lon,lat&radius=radius_meters
    Документация: "Finding locations near a point"
    """
    # Собираем URL вручную, чтобы не было дублирования параметров
    params = {
        "coordinates": f"{lon},{lat}",  # OpenAQ ожидает lon,lat (!)
        "radius": str(radius_meters)
    }

    # Кодируем параметры сами
    query_string = urllib.parse.urlencode(params)
    url = f"{OPENAQ_BASE_URL}/locations?{query_string}&limit=15"

    logger.info(f"Запрашиваем OpenAQ: {url}")

    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        data = response.json()

        locations = data.get("results", [])
        logger.info(f"OpenAQ: найдено {len(locations)} станций в радиусе {radius_meters}м от ({lat}, {lon})")
        return locations

    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса OpenAQ locations: {e}")
        return []


def get_latest_measurements(sensor_id: int) -> Optional[Dict]:
    """
    Получает последнее измерение для конкретного сенсора.

    API: GET /v3/sensors/{sensor_id}/measurements?limit=1
    Документация: Get latest measurement for a sensor
    """
    # Правильный эндпоинт для последних измерений сенсора
    url = f"{OPENAQ_BASE_URL}/sensors/{sensor_id}/measurements?limit=1"

    logger.debug(f"Запрашиваем измерения сенсора {sensor_id}: {url}")

    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        return results[0] if results else None

    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса измерений сенсора {sensor_id}: {e}")
        return None


def extract_air_quality(lat: float, lon: float, radius_meters: int = 10000) -> Optional[Dict]:
    """
    Основная функция: по координатам получает сводку качества воздуха.

    Возвращает словарь:
    {
        "lat": float,
        "lon": float,
        "timestamp": str (UTC),
        "measurements": [
            {
                "parameter": "pm25",
                "value": 42.5,
                "unit": "µg/m³",
                "location_name": "New Delhi",
                "sensor_id": 23534
            },
            ...
        ]
    }
    """
    locations = find_nearby_locations(lat, lon, radius_meters)

    if not locations:
        logger.warning(f"Нет станций OpenAQ вблизи ({lat}, {lon})")
        return None

    all_measurements = []
    latest_timestamp = None

    for location in locations:
        sensors = location.get("sensors", [])
        location_name = location.get("name", "Unknown")

        for sensor in sensors:
            sensor_id = sensor.get("id")
            parameter = sensor.get("parameter", {})

            # Запрашиваем последнее измерение для этого сенсора
            measurement = get_latest_measurements(sensor_id)

            if measurement:
                all_measurements.append({
                    "parameter": parameter.get("name"),
                    "value": measurement.get("value"),
                    "unit": parameter.get("units"),
                    "location_name": location_name,
                    "sensor_id": sensor_id,
                    "location_id": location.get("id")
                })

                # Берём самую свежую дату среди всех измерений
                m_time = measurement.get("period", {}).get("datetimeFrom", {}).get("utc")
                if m_time and (latest_timestamp is None or m_time > latest_timestamp):
                    latest_timestamp = m_time

    if not all_measurements:
        return None

    return {
        "lat": lat,
        "lon": lon,
        "timestamp": latest_timestamp,
        "measurements": all_measurements,
        "raw_json": {
            "locations": locations,
            "measurements": all_measurements
        }
    }

print(extract_air_quality(47.38, 8.54))