from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone

from services.air_quality.producer_service import extract_air_quality
from services.db.db import get_session
from services.db.models import LocationToTrack, ApiPollProgress
from logger import logger
from sqlalchemy.orm import Session
from sqlalchemy.sql import asc
from services.api_limiter import ApiLimiter, RateLimitExceeded




def init_api_poll_progress(db: Session, batch_id: str) -> ApiPollProgress:
    progress = db.query(ApiPollProgress).filter(ApiPollProgress.batch_id == batch_id).one_or_none()
    if not progress:
        progress = ApiPollProgress(batch_id=batch_id, last_location_id=0)
        db.add(progress)
        db.commit()
        db.refresh(progress)
    return progress


def check_all_locations() -> None:
    limiter = ApiLimiter(per_min=60, per_hour=2000)
    db = get_session()
    max_locations_per_run = None
    batch_id = "daily_" + datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Старт batch_id={batch_id}")

    try:
        progress = init_api_poll_progress(db, batch_id)
        last_id = progress.last_location_id

        # known_coords = (
        #     db.query(
        #         RawAirQuality.lat,
        #         RawAirQuality.lon,
        #     )
        #     .distinct()
        #     .all()
        # )
        # known_set = {(lat, lon) for lat, lon in known_coords}

        locations = (
            db.query(LocationToTrack)
            .filter(LocationToTrack.id > last_id)
            .order_by(asc(LocationToTrack.id))
            .all()
        )

        processed = 0

        for location in locations:
            # if (location.lat, location.lon) not in known_set:
            #     # Пропускаем, API почти наверняка не вернёт данные
            #     logger.debug(f"Пропускаем локацию {location.id} — нет AirQuality данных")
            #     db.commit()
            #     continue
            try:
                logger.info(
                    f"Проверка локации {location.id} (координаты {location.lat}, {location.lon})"
                )

                extract_air_quality(loc_id=location.id, lat=location.lat, lon=location.lon, limiter=limiter)



                location.last_checked_at = datetime.now(timezone.utc)
                progress.last_location_id = location.id
                processed += 1

                db.commit()
                db.expire_all()

                if max_locations_per_run is not None and processed >= max_locations_per_run:
                    logger.info(f"Достигнут лимит {max_locations_per_run} локаций за прогон")
                    break

            except RateLimitExceeded as e:
                db.commit()
                logger.error(e.message)
                logger.error(
                    f"DAG остановлен из-за часового лимита. Следующий запуск возможен через {e.wait_seconds} секунд."
                )
                raise

            except Exception as e:
                logger.error(f"Ошибка при проверке локации {location.id}: {e}")
                db.rollback()
                continue

    except RateLimitExceeded as e:
        logger.error(e.message)
        raise

    except Exception as e:
        logger.error(f"Критическая ошибка при проверке локаций: {e}")
        db.rollback()
        raise
    finally:
        db.close()


default_args = {
    'owner': 'Cheburek',
    'start_date': datetime(2026, 5, 21),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kafka_air_quality_producer',
    default_args=default_args,
    description='Fetch aq and publish to Kafka',
    schedule_interval=None,
    catchup=False,
    tags=['air_quality', 'kafka', 'producer'],
max_active_runs=1,
) as dag:

    fetch_aq = PythonOperator(
            task_id='fetch_and_publish_aq',
        python_callable=check_all_locations
    )
