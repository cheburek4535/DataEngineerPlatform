from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
from logger import logger
from services.db.db import get_session
from services.db.models import LocationToTrack
from services.weather.producer_service import get_weather
from sqlalchemy.sql import asc
import time


# def collect_weather_for_locations(**context):
#     db = get_session()
#     try:
#         locs = db.query(LocationToTrack).order_by(asc(LocationToTrack.id)).all()
#         for loc in locs:
#             get_weather(lat=loc.lat, lon=loc.lon, loc_id=loc.id)
#             logger.info(f"Сбор данных о локации {loc.id}")
#     except Exception as e:
#         logger.error(e)
#     finally:
#         db.close()
#
def collect_weather_for_locations(**context):
    db = get_session()
    try:
        locations = db.query(LocationToTrack).order_by(asc(LocationToTrack.id)).all()
        # Счетчик для контроля лимита в минуту
        location_counter = 0
        for location in locations:
            try:

                    logger.info(f"Сбор данных о локации {location.id} (координаты {location.lat}, {location.lon})")
                    get_weather(lat=location.lat, lon=location.lon, loc_id=location.id)

                    location.last_checked_at = datetime.now(timezone.utc)
                    db.commit()
                    db.expire_all()

                    # Увеличиваем счетчик после успешной обработки локации
                    location_counter += 1
                    if location_counter >= 5000:
                        logger.warning("Достигнут лимит 5000 локаций")
                        break

                    # ХИТРОСТЬ: Если обработали 500 локаций и это ЕЩЕ НЕ конец списка
                    if location_counter % 500 == 0 and location_counter < len(locations):
                        logger.info(
                            f"Обработано {location_counter} локаций. Спим 60 секунд для сброса минутного лимита API...")
                        time.sleep(60)
                    else:
                        # Микро-пауза в 0.01 сек между обычными запросами.
                        # Она нужна, чтобы база данных успевала отдыхать и не было микро-спама к API.
                        time.sleep(0.01)
            except Exception as e:
                logger.error(f"Ошибка при проверке локации {location.id}: {e}")
                db.rollback()
                continue

    except Exception as e:
        logger.error(f"Критическая ошибка при проверке локаций: {e}")
        db.rollback()
    finally:
        db.close()  # Закрываем сессию


default_args = {
    'owner': 'Cheburek',
    'start_date': datetime(2026, 5, 21),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kafka_weather_producer',
    default_args=default_args,
    description='Fetch weather and publish to Kafka',
    schedule_interval=None,
    catchup=False,
    tags=['weather', 'kafka', 'producer'],
max_active_runs=1,
) as dag:

    fetch_weather = PythonOperator(
            task_id='fetch_and_publish_weather',
        python_callable=collect_weather_for_locations,
    )
