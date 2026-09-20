from typing import Optional
import openmeteo_requests
import requests_cache
from retry_requests import retry
from logger import logger
from services.minio.storage import save_raw_json
from services.kafka.producer_confluent import send_message
import numpy as np
from datetime import datetime, timezone


class HistoricalWeatherProducer:
    def __init__(self):
        cache_session = requests_cache.CachedSession('.cache', expire_after=86400)  # кэш на сутки
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        self.openmeteo = openmeteo_requests.Client(session=retry_session)

    def fetch_and_send(self, lat: float, lon: float, loc_id: int, year: int = 2025) -> Optional[dict]:
        """
        Получает погоду за год и отправляет в Kafka
        Возвращает статистику для дальнейшего использования
        """
        url = "https://archive-api.open-meteo.com/v1/archive"

        params = {
            'latitude': lat,
            'longitude': lon,
            'start_date': f"{year}-01-01",
            'end_date': f"{year}-12-31",
            'hourly': 'temperature_2m,relative_humidity_2m,pressure_msl,wind_speed_10m',
            'timezone': 'auto'
        }

        try:
            responses = self.openmeteo.weather_api(url, params=params)
            response = responses[0]

            hourly = response.Hourly()

            # Получаем массивы данных
            temperatures = hourly.Variables(0).ValuesAsNumpy()
            humidities = hourly.Variables(1).ValuesAsNumpy()
            pressures = hourly.Variables(2).ValuesAsNumpy()
            wind_speeds = hourly.Variables(3).ValuesAsNumpy()

            # Убираем None/null значения
            temps_clean = [float(t) for t in temperatures if not np.isnan(t)]
            hums_clean = [float(h) for h in humidities if not np.isnan(h)]
            press_clean = [float(p) for p in pressures if not np.isnan(p)]
            winds_clean = [float(w) for w in wind_speeds if not np.isnan(w)]

            # Рассчитываем статистику для аномалий (сэмплируем разумно)
            # Берем каждые 6 часов (4 замера в день) -> ~1460 точек за год
            sample_step = 12
            sampled_temps = temps_clean[::sample_step] if len(temps_clean) > sample_step else temps_clean

            data = {
                'location_id': loc_id,
                'lat': float(response.Latitude()),
                'lon': float(response.Longitude()),
                'year': year,
                'collected_at': datetime.now(timezone.utc).isoformat(),
                'statistics': {
                    'temperature': {
                        'mean': float(np.mean(temps_clean)),
                        'std': float(np.std(temps_clean)),
                        'min': float(np.min(temps_clean)),
                        'max': float(np.max(temps_clean)),
                        'samples': sampled_temps  # сэмплированные данные для аномалий
                    },
                    'humidity': {
                        'mean': float(np.mean(hums_clean)),
                        'std': float(np.std(hums_clean)),
                        'min': float(np.min(hums_clean)),
                        'max': float(np.max(hums_clean)),
                        'samples': hums_clean[::sample_step] if len(hums_clean) > sample_step else hums_clean
                    },
                    'pressure': {
                        'mean': float(np.mean(press_clean)),
                        'std': float(np.std(press_clean)),
                        'min': float(np.min(press_clean)),
                        'max': float(np.max(press_clean)),
                    },
                    'wind_speed': {
                        'mean': float(np.mean(winds_clean)),
                        'std': float(np.std(winds_clean)),
                        'min': float(np.min(winds_clean)),
                        'max': float(np.max(winds_clean)),
                    }
                },
                'raw_json': {
                    'latitude': response.Latitude(),
                    'longitude': response.Longitude(),
                    'elevation': response.Elevation(),
                    'timezone': response.Timezone(),
                    'hourly': {
                        'time': hourly.Time(),
                        'temperature_2m': temperatures.tolist(),
                        'relative_humidity_2m': humidities.tolist(),
                        'pressure_msl': pressures.tolist(),
                        'wind_speed_10m': wind_speeds.tolist()
                    }
                }
            }

            # Сохраняем в MinIO
            save_raw_json(bucket="raw-data", prefix="historical_weather", data=data)

            # Отправляем в Kafka
            success = send_message(
                topic='weather.historical',
                key=str(loc_id),
                value=data
            )

            if success:
                logger.info(f"Historical weather for location {loc_id} sent to Kafka")
                return data
            else:
                raise Exception("Failed to send to Kafka")

        except Exception as e:
            logger.error(f"Error fetching historical weather for {loc_id}: {e}")
            return None