import time

from sqlalchemy.orm import Session
from sqlalchemy.sql import asc
from logger import logger
from services.db.db import get_session
from services.utils.is_on_water import is_on_water
from services.db.models import LocationToTrack


def check_locations_land_water():
    db = get_session()
    try:
        locations = db.query(LocationToTrack).filter(LocationToTrack.is_on_land == None).order_by(asc(LocationToTrack.id)).all()
        if not locations:
            logger.warning("Локаций для проверки нет")
            return None
        logger.info(f"Найдено {len(locations)} для проверки")
        for location in locations:
            is_water = is_on_water(lat=location.lat, lon=location.lon)
            if is_water is None:
                logger.warning(f"Ошибка при проверке локации {location.id} - пропускаем")
                continue
            elif is_water is False:
                location.is_on_land = True
                logger.info(f"Локация {location.id} находится на суше")
            else:
                location.is_on_land = False
                logger.info(f"Локация {location.id} на воде")
            db.add(location)
            db.commit()
            time.sleep(0.01)
        logger.info(f"Все локации проверены")
    except Exception as e:
        logger.error(f"Критическая ошибка при проверке локаций на воду {e}")
        raise
    finally:
        db.close()

check_locations_land_water()