
from services.air_quality.transform import collect_aq
from services.db.models import LocationToTrack, RawAirQuality
from services.db.db import get_session
from datetime import datetime, timezone
from logger import logger
import time
from sqlalchemy.orm import Session
from sqlalchemy.sql import desc, asc, func
import random
from typing import List, Optional

def check_all_locations(db: Session):
    try:
        locations = db.query(LocationToTrack).order_by(asc(LocationToTrack.id)).all()
        # Счетчик для контроля лимита в минуту
        location_counter = 0

        known_coords = (
            db.query(func.distinct(RawAirQuality.lat, RawAirQuality.lon))
            .all()
        )
        known_set = {(lat, lon) for lat, lon in known_coords}
        for location in locations:
            try:
                for location in locations:
                    if (location.lat, location.lon) not in known_set:
                        # Пропускаем, API почти наверняка не вернёт данные
                        logger.info(f"Пропускаем локацию {location.id} — нет AirQuality данных")
                        db.commit()
                        continue

                    logger.info(f"Проверка локации {location.id} (координаты {location.lat}, {location.lon})")
                    check = collect_aq(db, location)

                    if check:
                        logger.info(
                            f"НАйдены данные о качестве воздуха в локации {location.id} "
                            f"(координаты {location.lat}, {location.lon}). "
                            f"Дополнительные данные: {check.__dict__}"
                        )
                    else:
                        logger.info(f"Информации о качестве воздуха в локации {location.id} не обнаружено")

                    location.last_checked_at = datetime.now(timezone.utc)
                    db.commit()
                    db.expire_all()

                    # Увеличиваем счетчик после успешной обработки локации
                    location_counter += 1
                    if location_counter >= 2000:
                        logger.warning("Достигнут лимит 2000 локаций")
                        break

                    # ХИТРОСТЬ: Если обработали 500 локаций и это ЕЩЕ НЕ конец списка
                    if location_counter % 60 == 0 and location_counter < len(locations):
                        logger.info(
                            f"Обработано {location_counter} локаций. Спим 60 секунд для сброса минутного лимита API...")
                        time.sleep(60)
                    if location_counter % 2000 == 0 and location_counter < len(locations):
                        logger.info(f"Обработано {location_counter} локаций. Спим 1 час для сброса минутного лимита API...")
                        time.sleep(3600)
            except Exception as e:
                logger.error(f"Ошибка при проверке локации {location.id}: {e}")
                db.rollback()
                continue

    except Exception as e:
        logger.error(f"Критическая ошибка при проверке локаций: {e}")
        db.rollback()
    finally:
        db.close()  # Закрываем сессию
