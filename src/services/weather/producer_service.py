from typing import Optional
import openmeteo_requests
import json
from services.minio.storage import save_raw_json
import requests_cache
from retry_requests import retry
from logger import logger
from services.kafka.producer_confluent import send_message


cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)
def get_weather(lat: float, lon: float, loc_id: int) -> Optional[dict]:
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
            'location_id': loc_id,
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

        save_raw_json(bucket="raw-data", prefix="weather", data=data)

        # Отправляем в Kafka
        logger.info("Sending raw weather data to Kafka...")
        success = send_message(
            topic='weather.raw',
            key=data.get('timestamp', 'unknown'),
            value=data
        )

        if not success:
            raise Exception("Failed to send data to Kafka")

        logger.info(f"Successfully sent weather data to Kafka for datetime: {data.get('timestamp')}")

        return data
    except Exception as e:
        msg = str(e)

        if "Daily API request limit exceeded" in msg:
            logger.warning("Open-Meteo daily limit exceeded, skipping request")
            return None

        logger.error(f"Ошибка запроса погоды: {e}")
        raise

