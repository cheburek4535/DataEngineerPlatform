from datetime import timezone, datetime
from typing import Optional, List, Dict
import requests
from logger import logger
import urllib.parse
import time
from services.api_limiter import ApiLimiter, RateLimitExceeded
from services.minio.storage import save_raw_json
from services.kafka.producer_confluent import send_message

OPENAQ_API_KEY = "522fb004826777c1888ce980d9517b32bceeac75fa7db3c02c563a99e1525177"
OPENAQ_BASE_URL = "https://api.openaq.org/v3"

HEADERS = {
    "X-API-Key": OPENAQ_API_KEY,
    "Accept": "application/json"
}


def _get_with_rate_limit(url: str, limiter: ApiLimiter, timeout: int = 15) -> requests.Response:
    limiter.acquire_or_fail_if_hour_exceeded()

    response = requests.get(url, headers=HEADERS, timeout=timeout)

    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        wait_time = limiter.handle_429(retry_after)
        logger.warning(f"OpenAQ вернул 429, спим {wait_time} сек")
        if wait_time > 0:
            time.sleep(wait_time)

        limiter.acquire_or_fail_if_hour_exceeded()
        response = requests.get(url, headers=HEADERS, timeout=timeout)

    response.raise_for_status()
    return response

def find_nearby_locations(lat: float, lon: float, limiter: ApiLimiter, radius_meters: int = 10000) -> List[Dict]:
    params = {
        "coordinates": f"{lat},{lon}",
        "radius": str(radius_meters),
        "limit": "15"
    }
    query_string = urllib.parse.urlencode(params)
    url = f"{OPENAQ_BASE_URL}/locations?{query_string}"

    logger.info(f"Запрашиваем OpenAQ: {url}")

    try:
        response = _get_with_rate_limit(url, limiter, timeout=15)
        data = response.json()


        locations = data.get("results", [])
        if len(locations) > 0:
            save_raw_json(bucket="raw-data", prefix="air_quality/locations", data=data)
        logger.info(f"OpenAQ: найдено {len(locations)} станций в радиусе {radius_meters}м от ({lat}, {lon})")
        return locations
    except RateLimitExceeded:
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса OpenAQ locations: {e}")
        return []


def get_latest_measurements(sensor_id: int, limiter: ApiLimiter) -> Optional[Dict]:
    url = f"{OPENAQ_BASE_URL}/sensors/{sensor_id}/measurements?limit=1"
    logger.debug(f"Запрашиваем измерения сенсора {sensor_id}: {url}")

    try:
        response = _get_with_rate_limit(url, limiter, timeout=15)
        data = response.json()
        save_raw_json(bucket="raw-data", prefix="air_quality/measurements", data=data)

        results = data.get("results", [])
        return results[0] if results else None
    except RateLimitExceeded:
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса измерений сенсора {sensor_id}: {e}")
        return None

def extract_air_quality(loc_id: int, lat: float, lon: float, limiter: ApiLimiter, radius_meters: int = 25000)  -> Optional[Dict]:
    locations = find_nearby_locations(lat, lon, limiter, radius_meters)

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

            if not sensor_id:
                continue

            measurement = get_latest_measurements(sensor_id, limiter)

            if measurement:
                all_measurements.append({
                    "parameter": parameter.get("name"),
                    "value": measurement.get("value"),
                    "unit": parameter.get("units"),
                    "location_name": location_name,
                    "sensor_id": sensor_id,
                    "location_id": location.get("id")
                })

                m_time = measurement.get("period", {}).get("datetimeFrom", {}).get("utc")
                if m_time and (latest_timestamp is None or m_time > latest_timestamp):
                    latest_timestamp = m_time

    if not all_measurements:
        return None

    data = {
        "location_id": loc_id,
        "lat": lat,
        "lon": lon,
        "collected_at": datetime.now(timezone.utc),
        "measurements": all_measurements,
        "raw_json": {
            "locations": locations,
            "measurements": all_measurements
        }
    }
    logger.info(f"Отправляем в kafka")
    success = send_message("air_quality.raw", data.get("collected_at").isoformat(), data)
    if not success:
        raise Exception("Failed to send data to Kafka")

    logger.info(f"Successfully sent weather data to Kafka for datetime: {data.get('collected_at')}")

    return data



