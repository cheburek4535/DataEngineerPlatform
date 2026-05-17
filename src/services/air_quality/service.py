
from services.db.models import LocationToTrack, ApiPollProgress, AirQuality, RawAirQuality
from datetime import datetime, timezone
from logger import logger
from sqlalchemy.orm import Session
from sqlalchemy.sql import asc, func

from typing import Optional
from services.api_limiter import ApiLimiter, RateLimitExceeded
from services.air_quality.transform import collect_aq



def init_api_poll_progress(db: Session, batch_id: str) -> ApiPollProgress:
    progress = db.query(ApiPollProgress).filter(ApiPollProgress.batch_id == batch_id).one_or_none()
    if not progress:
        progress = ApiPollProgress(batch_id=batch_id, last_location_id=0)
        db.add(progress)
        db.commit()
        db.refresh(progress)
    return progress


def check_all_locations(db: Session, batch_id: str, max_locations_per_run: Optional[int] = None) -> None:
    limiter = ApiLimiter(per_min=60, per_hour=2000)

    try:
        progress = init_api_poll_progress(db, batch_id)
        last_id = progress.last_location_id

        # known_coords = (
        #     db.query(func.distinct(RawAirQuality.lat, RawAirQuality.lon))
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

                structured_aq = collect_aq(db, location, limiter)

                if structured_aq:
                    logger.info(
                        f"Найдены данные о качестве воздуха в локации {location.id} "
                        f"(координаты {location.lat}, {location.lon})"
                    )
                else:
                    logger.info(
                        f"Информации о качестве воздуха в локации {location.id} не обнаружено"
                    )

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

    except RateLimitExceeded:
        raise

    except Exception as e:
        logger.error(f"Критическая ошибка при проверке локаций: {e}")
        db.rollback()
        raise
    finally:
        db.close()
